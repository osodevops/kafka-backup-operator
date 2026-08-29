//! Lease-based leader election (`coordination.k8s.io/v1`).
//!
//! A self-contained port of the semantics of client-go's `LeaderElector`:
//!
//! * a candidate may take a lease only when it has observed **no change** to
//!   the lease record for `lease_duration`, measured on its own clock — the
//!   holder's `renewTime` is never compared to our clock, so clock skew between
//!   replicas cannot cause a premature takeover;
//! * the holder renews every `retry_period` and must step down (here: exit the
//!   process) when it has not managed to renew for `renew_deadline`;
//! * every write is a compare-and-swap through `replace` with the
//!   `resourceVersion` from the preceding read — never a server-side apply,
//!   which merges rather than locks;
//! * `leaderTransitions` is incremented only when the holder identity changes;
//! * on shutdown the holder releases the lease (empty holder, 1s duration) so
//!   its successor acquires within one `retry_period` instead of waiting for
//!   the full `lease_duration`.
//!
//! Without a lease, two operator pods (for instance during a Deployment
//! rollout, a node drain or an eviction) run the same scheduled backups
//! concurrently — issue #79.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use hyper::StatusCode;
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use kube::api::{Api, PostParams};
use kube::Client;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::error::{Error, Result};

pub const ENABLED_ENV: &str = "LEADER_ELECTION_ENABLED";
pub const LEASE_DURATION_ENV: &str = "LEADER_ELECTION_LEASE_DURATION";
pub const RENEW_DEADLINE_ENV: &str = "LEADER_ELECTION_RENEW_DEADLINE";
pub const RETRY_PERIOD_ENV: &str = "LEADER_ELECTION_RETRY_PERIOD";
pub const LEASE_NAME_ENV: &str = "LEADER_ELECTION_LEASE_NAME";
pub const OPERATOR_NAMESPACE_ENV: &str = "OPERATOR_NAMESPACE";

pub const DEFAULT_LEASE_NAME: &str = "kafka-backup-operator-leader";
pub const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(15);
pub const DEFAULT_RENEW_DEADLINE: Duration = Duration::from_secs(10);
pub const DEFAULT_RETRY_PERIOD: Duration = Duration::from_secs(2);

/// Leader election settings, normally read from the environment the Helm
/// chart renders (`LEADER_ELECTION_*`, `OPERATOR_NAMESPACE`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderElectionConfig {
    pub lease_name: String,
    pub namespace: String,
    /// How long a candidate waits, without observing a change to the lease,
    /// before it may take over.
    pub lease_duration: Duration,
    /// How long the holder keeps trying to renew before it gives up.
    pub renew_deadline: Duration,
    /// Interval between acquire/renew attempts.
    pub retry_period: Duration,
}

impl LeaderElectionConfig {
    /// Upper bound for one acquire/renew attempt: half the renew deadline, so
    /// a request that hangs (a black-holed connection, a stalled API server)
    /// cannot keep a leader believing it leads past the deadline.
    pub fn step_timeout(&self) -> Duration {
        (self.renew_deadline / 2).max(Duration::from_millis(500))
    }

    /// `Ok(None)` unless `LEADER_ELECTION_ENABLED` is set to a true value.
    /// `fallback_namespace` is used when `OPERATOR_NAMESPACE` is unset.
    pub fn from_env(fallback_namespace: &str) -> Result<Option<Self>> {
        Self::from_lookup(|key| std::env::var(key).ok(), fallback_namespace)
    }

    /// Like [`from_env`](Self::from_env) but with an injectable variable
    /// lookup, so tests do not have to mutate the process environment.
    pub fn from_lookup(
        lookup: impl Fn(&str) -> Option<String>,
        fallback_namespace: &str,
    ) -> Result<Option<Self>> {
        let get = |key: &str| lookup(key).map(|v| v.trim().to_string());

        if !parse_enabled(get(ENABLED_ENV).as_deref())? {
            return Ok(None);
        }

        let duration = |key: &str, default: Duration| -> Result<Duration> {
            match get(key).filter(|v| !v.is_empty()) {
                Some(raw) => parse_duration(&raw).map_err(|e| Error::Config(format!("{key}: {e}"))),
                None => Ok(default),
            }
        };

        let config = Self {
            lease_name: get(LEASE_NAME_ENV)
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| DEFAULT_LEASE_NAME.to_string()),
            namespace: get(OPERATOR_NAMESPACE_ENV)
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| fallback_namespace.to_string()),
            lease_duration: duration(LEASE_DURATION_ENV, DEFAULT_LEASE_DURATION)?,
            renew_deadline: duration(RENEW_DEADLINE_ENV, DEFAULT_RENEW_DEADLINE)?,
            retry_period: duration(RETRY_PERIOD_ENV, DEFAULT_RETRY_PERIOD)?,
        };
        config.validate()?;
        Ok(Some(config))
    }

    /// client-go's invariants: `lease_duration > renew_deadline > retry_period > 0`.
    pub fn validate(&self) -> Result<()> {
        if self.retry_period.is_zero() {
            return Err(Error::Config(format!(
                "{RETRY_PERIOD_ENV} must be greater than zero"
            )));
        }
        if self.renew_deadline <= self.retry_period {
            return Err(Error::Config(format!(
                "{RENEW_DEADLINE_ENV} ({:?}) must be greater than {RETRY_PERIOD_ENV} ({:?})",
                self.renew_deadline, self.retry_period
            )));
        }
        if self.lease_duration <= self.renew_deadline {
            return Err(Error::Config(format!(
                "{LEASE_DURATION_ENV} ({:?}) must be greater than {RENEW_DEADLINE_ENV} ({:?})",
                self.lease_duration, self.renew_deadline
            )));
        }
        if self.lease_name.is_empty() || self.namespace.is_empty() {
            return Err(Error::Config(
                "leader election lease name and namespace must not be empty".to_string(),
            ));
        }
        Ok(())
    }
}

fn parse_enabled(raw: Option<&str>) -> Result<bool> {
    match raw.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
        None | Some("") | Some("false") | Some("0") | Some("no") | Some("off") => Ok(false),
        Some("true") | Some("1") | Some("yes") | Some("on") => Ok(true),
        Some(other) => Err(Error::Config(format!(
            "{ENABLED_ENV} must be true or false, got '{other}'"
        ))),
    }
}

/// Parse a Go-style duration: `15s`, `1500ms`, `1m`, `1m30s`, `2h`; a bare
/// number is seconds.
pub fn parse_duration(raw: &str) -> Result<Duration> {
    let input = raw.trim();
    let invalid = || Error::Config(format!("invalid duration '{raw}'"));
    if input.is_empty() {
        return Err(invalid());
    }
    if let Ok(secs) = input.parse::<u64>() {
        return Ok(Duration::from_secs(secs));
    }

    let mut total = Duration::ZERO;
    let mut rest = input;
    while !rest.is_empty() {
        let digits_end = rest
            .find(|c: char| !c.is_ascii_digit())
            .ok_or_else(invalid)?;
        if digits_end == 0 {
            return Err(invalid());
        }
        let value: u64 = rest[..digits_end].parse().map_err(|_| invalid())?;
        let unit_end = rest[digits_end..]
            .find(|c: char| c.is_ascii_digit())
            .map(|i| digits_end + i)
            .unwrap_or(rest.len());
        let unit = &rest[digits_end..unit_end];
        let part = match unit {
            "ms" => Duration::from_millis(value),
            "s" => Duration::from_secs(value),
            "m" => Duration::from_secs(value * 60),
            "h" => Duration::from_secs(value * 3600),
            _ => return Err(invalid()),
        };
        total = total.checked_add(part).ok_or_else(invalid)?;
        rest = &rest[unit_end..];
    }
    Ok(total)
}

/// This replica's identity in the lease: the pod name (`HOSTNAME`, set by the
/// kubelet), else the host name, else a process-unique fallback.
pub fn identity() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .map(|h| h.trim().to_string())
        .filter(|h| !h.is_empty())
        .or_else(|| {
            std::fs::read_to_string("/etc/hostname")
                .ok()
                .map(|h| h.trim().to_string())
                .filter(|h| !h.is_empty())
        })
        .unwrap_or_else(|| format!("kafka-backup-operator-{}", std::process::id()))
}

/// What this replica knows about its leadership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaderState {
    /// The lease has not been observed yet (start-up, or the API/RBAC is
    /// refusing us). Not ready.
    Unknown,
    /// Another replica holds the lease; this one is a warm standby.
    Follower,
    /// This replica holds the lease (or election is disabled).
    Leader,
}

/// Readiness for `/readyz`. Standbys are ready — a rolling update must be
/// able to complete while the outgoing pod still leads — but a replica that
/// has never observed the lease is not, so a misconfigured install (missing
/// leases RBAC, unreachable API) fails loudly instead of running silently.
pub fn readiness(state: LeaderState) -> (StatusCode, &'static str) {
    match state {
        LeaderState::Unknown => (StatusCode::SERVICE_UNAVAILABLE, "leader election pending"),
        LeaderState::Follower => (StatusCode::OK, "standby"),
        LeaderState::Leader => (StatusCode::OK, "leader"),
    }
}

/// Result of a single acquire-or-renew attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepOutcome {
    /// We hold the lease after this step.
    Leading,
    /// Someone else holds an unexpired lease; nothing was written.
    Standby,
    /// Our compare-and-swap lost against a concurrent writer; state unchanged,
    /// the next step re-reads.
    Conflict,
}

#[derive(Debug, thiserror::Error)]
pub enum LeaderError {
    #[error("leadership lost: lease '{lease}' is now held by {holder:?}")]
    Lost {
        lease: String,
        holder: Option<String>,
    },
    #[error("failed to renew lease '{lease}' within the renew deadline ({deadline:?}); last successful renewal at {last_ok:?}")]
    RenewDeadlineExceeded {
        lease: String,
        deadline: Duration,
        last_ok: Option<DateTime<Utc>>,
    },
}

/// Source of wall-clock time, injectable for tests.
pub type Clock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

pub struct LeaderElector {
    api: Api<Lease>,
    identity: String,
    cfg: LeaderElectionConfig,
    now: Clock,
    /// The lease record as last seen, and when *we* first saw it in that
    /// form. Expiry is judged on `observed_time`, never on the record's own
    /// `renewTime`.
    observed: Option<LeaseSpec>,
    observed_time: DateTime<Utc>,
    last_renew_ok: Option<DateTime<Utc>>,
    state_tx: watch::Sender<LeaderState>,
}

impl LeaderElector {
    pub fn new(
        client: Client,
        cfg: LeaderElectionConfig,
        identity: String,
        state_tx: watch::Sender<LeaderState>,
    ) -> Self {
        let api = Api::<Lease>::namespaced(client, &cfg.namespace);
        Self {
            api,
            identity,
            cfg,
            now: Arc::new(Utc::now),
            observed: None,
            observed_time: Utc::now(),
            last_renew_ok: None,
            state_tx,
        }
    }

    pub fn with_clock(mut self, now: Clock) -> Self {
        self.observed_time = now();
        self.now = now;
        self
    }

    pub fn identity(&self) -> &str {
        &self.identity
    }

    pub fn state(&self) -> LeaderState {
        *self.state_tx.borrow()
    }

    /// The lease record as last observed by this replica.
    pub fn observed(&self) -> Option<&LeaseSpec> {
        self.observed.as_ref()
    }

    fn publish(&self, state: LeaderState) {
        if self.state() != state {
            info!(identity = %self.identity, lease = %self.cfg.lease_name, ?state, "Leader election state changed");
        }
        self.state_tx.send_replace(state);
    }

    fn observe(&mut self, spec: Option<LeaseSpec>, now: DateTime<Utc>) {
        let spec = spec.unwrap_or_default();
        if self.observed.as_ref() != Some(&spec) {
            self.observed = Some(spec);
            self.observed_time = now;
        }
    }

    fn lease_duration_secs(&self) -> i32 {
        i32::try_from(self.cfg.lease_duration.as_secs()).unwrap_or(i32::MAX)
    }

    /// One client-go `tryAcquireOrRenew` step. Never sleeps.
    pub async fn try_acquire_or_renew(&mut self) -> std::result::Result<StepOutcome, kube::Error> {
        let now = (self.now)();
        let name = self.cfg.lease_name.clone();

        let Some(mut lease) = self.api.get_opt(&name).await? else {
            let lease = Lease {
                metadata: ObjectMeta {
                    name: Some(name.clone()),
                    namespace: Some(self.cfg.namespace.clone()),
                    ..Default::default()
                },
                spec: Some(LeaseSpec {
                    holder_identity: Some(self.identity.clone()),
                    lease_duration_seconds: Some(self.lease_duration_secs()),
                    acquire_time: Some(MicroTime(now)),
                    renew_time: Some(MicroTime(now)),
                    lease_transitions: Some(0),
                }),
            };
            return match self.api.create(&PostParams::default(), &lease).await {
                Ok(created) => {
                    self.became_leader(created.spec, now);
                    Ok(StepOutcome::Leading)
                }
                Err(kube::Error::Api(e)) if e.code == 409 => {
                    debug!(lease = %name, "Lease was created concurrently; re-reading on the next attempt");
                    if self.state() == LeaderState::Unknown {
                        self.publish(LeaderState::Follower);
                    }
                    Ok(StepOutcome::Conflict)
                }
                Err(e) => Err(e),
            };
        };

        let current = lease.spec.clone().unwrap_or_default();
        self.observe(Some(current.clone()), now);

        let holder = current.holder_identity.as_deref().filter(|h| !h.is_empty());
        let held_by_me = holder == Some(self.identity.as_str());
        let held_by_other = holder.is_some() && !held_by_me;
        let expired = self.observed_time + to_delta(self.cfg.lease_duration) <= now;

        if held_by_other && !expired {
            self.publish(LeaderState::Follower);
            return Ok(StepOutcome::Standby);
        }

        let (acquire_time, lease_transitions) = if held_by_me {
            (
                current.acquire_time.clone().or(Some(MicroTime(now))),
                current.lease_transitions.or(Some(0)),
            )
        } else {
            (
                Some(MicroTime(now)),
                Some(current.lease_transitions.unwrap_or(0).saturating_add(1)),
            )
        };
        lease.spec = Some(LeaseSpec {
            holder_identity: Some(self.identity.clone()),
            lease_duration_seconds: Some(self.lease_duration_secs()),
            acquire_time,
            renew_time: Some(MicroTime(now)),
            lease_transitions,
        });

        match self
            .api
            .replace(&name, &PostParams::default(), &lease)
            .await
        {
            Ok(updated) => {
                if !held_by_me {
                    info!(
                        identity = %self.identity,
                        lease = %name,
                        previous_holder = holder.unwrap_or("<none>"),
                        "Acquired leader lease"
                    );
                }
                self.became_leader(updated.spec, now);
                Ok(StepOutcome::Leading)
            }
            Err(kube::Error::Api(e)) if e.code == 409 => {
                debug!(lease = %name, "Lease changed concurrently; re-reading on the next attempt");
                if self.state() == LeaderState::Unknown {
                    self.publish(LeaderState::Follower);
                }
                Ok(StepOutcome::Conflict)
            }
            Err(e) => Err(e),
        }
    }

    fn became_leader(&mut self, spec: Option<LeaseSpec>, now: DateTime<Utc>) {
        self.observe(spec, now);
        self.last_renew_ok = Some(now);
        self.publish(LeaderState::Leader);
    }

    /// True when we lead but have not renewed successfully for `renew_deadline`.
    pub fn renew_deadline_exceeded(&self) -> bool {
        if self.state() != LeaderState::Leader {
            return false;
        }
        match self.last_renew_ok {
            Some(last_ok) => (self.now)() - last_ok > to_delta(self.cfg.renew_deadline),
            None => true,
        }
    }

    /// Give the lease up if we hold it, so a successor acquires immediately.
    /// One attempt; failures are logged, the lease then simply expires.
    pub async fn release(&mut self) -> std::result::Result<(), kube::Error> {
        let holds = self
            .observed
            .as_ref()
            .and_then(|s| s.holder_identity.as_deref())
            == Some(self.identity.as_str());
        if !holds {
            return Ok(());
        }
        let name = self.cfg.lease_name.clone();
        let Some(mut lease) = self.api.get_opt(&name).await? else {
            return Ok(());
        };
        let current = lease.spec.clone().unwrap_or_default();
        if current.holder_identity.as_deref() != Some(self.identity.as_str()) {
            return Ok(());
        }
        let now = (self.now)();
        lease.spec = Some(LeaseSpec {
            holder_identity: Some(String::new()),
            lease_duration_seconds: Some(1),
            acquire_time: Some(MicroTime(now)),
            renew_time: Some(MicroTime(now)),
            lease_transitions: current.lease_transitions,
        });
        let updated = self
            .api
            .replace(&name, &PostParams::default(), &lease)
            .await?;
        self.observe(updated.spec, now);
        info!(identity = %self.identity, lease = %name, "Released leader lease");
        Ok(())
    }

    async fn step_down(mut self) -> std::result::Result<(), LeaderError> {
        match tokio::time::timeout(self.cfg.step_timeout(), self.release()).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                warn!(error = %e, lease = %self.cfg.lease_name, "Failed to release leader lease; it will expire")
            }
            Err(_) => {
                warn!(lease = %self.cfg.lease_name, "Releasing the leader lease timed out; it will expire")
            }
        }
        self.publish(LeaderState::Follower);
        Ok(())
    }

    /// Run the election loop until `release_rx` turns `true` (or its sender is
    /// dropped), returning `Ok(())` after releasing the lease. Returns an error
    /// when leadership was lost or could not be renewed within the deadline —
    /// the caller must stop reconciling immediately.
    pub async fn run(
        mut self,
        mut release_rx: watch::Receiver<bool>,
    ) -> std::result::Result<(), LeaderError> {
        loop {
            if *release_rx.borrow() {
                return self.step_down().await;
            }

            let was_leader = self.state() == LeaderState::Leader;
            let step = tokio::time::timeout(self.cfg.step_timeout(), self.try_acquire_or_renew())
                .await
                .unwrap_or_else(|_| {
                    Err(kube::Error::Service(
                        format!(
                            "lease request did not complete within {:?}",
                            self.cfg.step_timeout()
                        )
                        .into(),
                    ))
                });
            match step {
                Ok(StepOutcome::Standby) if was_leader => {
                    let holder = self
                        .observed
                        .as_ref()
                        .and_then(|s| s.holder_identity.clone());
                    self.publish(LeaderState::Follower);
                    return Err(LeaderError::Lost {
                        lease: self.cfg.lease_name.clone(),
                        holder,
                    });
                }
                Ok(_) => {}
                Err(e) => warn!(
                    error = %e,
                    lease = %self.cfg.lease_name,
                    leading = was_leader,
                    "Leader election attempt failed"
                ),
            }

            if self.renew_deadline_exceeded() {
                let err = LeaderError::RenewDeadlineExceeded {
                    lease: self.cfg.lease_name.clone(),
                    deadline: self.cfg.renew_deadline,
                    last_ok: self.last_renew_ok,
                };
                self.publish(LeaderState::Follower);
                return Err(err);
            }

            tokio::select! {
                _ = tokio::time::sleep(self.cfg.retry_period) => {}
                changed = release_rx.changed() => {
                    if changed.is_err() || *release_rx.borrow() {
                        return self.step_down().await;
                    }
                }
            }
        }
    }
}

fn to_delta(d: Duration) -> TimeDelta {
    TimeDelta::from_std(d).unwrap_or(TimeDelta::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn lookup(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |key: &str| map.get(key).cloned()
    }

    #[test]
    fn parse_duration_accepts_go_style_and_bare_seconds() {
        assert_eq!(parse_duration("15s").unwrap(), Duration::from_secs(15));
        assert_eq!(parse_duration("10s").unwrap(), Duration::from_secs(10));
        assert_eq!(parse_duration("2s").unwrap(), Duration::from_secs(2));
        assert_eq!(parse_duration("15").unwrap(), Duration::from_secs(15));
        assert_eq!(
            parse_duration("1500ms").unwrap(),
            Duration::from_millis(1500)
        );
        assert_eq!(parse_duration("1m").unwrap(), Duration::from_secs(60));
        assert_eq!(parse_duration("1m30s").unwrap(), Duration::from_secs(90));
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7200));
        assert_eq!(parse_duration(" 5s ").unwrap(), Duration::from_secs(5));
    }

    #[test]
    fn parse_duration_rejects_garbage() {
        for raw in ["", "abc", "-1s", "1x", "s", "1.5s", "1s2"] {
            assert!(parse_duration(raw).is_err(), "{raw:?} must be rejected");
        }
    }

    #[test]
    fn disabled_unless_enabled_is_true() {
        for value in ["false", "0", "no", "off", "", "  "] {
            let cfg =
                LeaderElectionConfig::from_lookup(lookup(&[(ENABLED_ENV, value)]), "ns").unwrap();
            assert!(cfg.is_none(), "{value:?} must disable leader election");
        }
        assert!(LeaderElectionConfig::from_lookup(lookup(&[]), "ns")
            .unwrap()
            .is_none());
    }

    #[test]
    fn enabled_values_use_documented_defaults() {
        for value in ["true", "TRUE", "1", "yes", "on"] {
            let cfg = LeaderElectionConfig::from_lookup(lookup(&[(ENABLED_ENV, value)]), "sbo")
                .unwrap()
                .unwrap_or_else(|| panic!("{value:?} must enable leader election"));
            assert_eq!(
                cfg,
                LeaderElectionConfig {
                    lease_name: DEFAULT_LEASE_NAME.to_string(),
                    namespace: "sbo".to_string(),
                    lease_duration: Duration::from_secs(15),
                    renew_deadline: Duration::from_secs(10),
                    retry_period: Duration::from_secs(2),
                }
            );
        }
    }

    #[test]
    fn unparseable_enabled_value_is_an_error_naming_the_variable() {
        let err =
            LeaderElectionConfig::from_lookup(lookup(&[(ENABLED_ENV, "maybe")]), "ns").unwrap_err();
        assert!(err.to_string().contains(ENABLED_ENV), "{err}");
    }

    #[test]
    fn env_overrides_timings_lease_name_and_namespace() {
        let cfg = LeaderElectionConfig::from_lookup(
            lookup(&[
                (ENABLED_ENV, "true"),
                (LEASE_DURATION_ENV, "30s"),
                (RENEW_DEADLINE_ENV, "20s"),
                (RETRY_PERIOD_ENV, "5s"),
                (LEASE_NAME_ENV, "my-release-leader"),
                (OPERATOR_NAMESPACE_ENV, "backup-system"),
            ]),
            "ignored",
        )
        .unwrap()
        .unwrap();
        assert_eq!(cfg.lease_duration, Duration::from_secs(30));
        assert_eq!(cfg.renew_deadline, Duration::from_secs(20));
        assert_eq!(cfg.retry_period, Duration::from_secs(5));
        assert_eq!(cfg.lease_name, "my-release-leader");
        assert_eq!(cfg.namespace, "backup-system");
    }

    #[test]
    fn empty_namespace_env_falls_back_to_the_client_namespace() {
        let cfg = LeaderElectionConfig::from_lookup(
            lookup(&[(ENABLED_ENV, "true"), (OPERATOR_NAMESPACE_ENV, "  ")]),
            "fallback",
        )
        .unwrap()
        .unwrap();
        assert_eq!(cfg.namespace, "fallback");
    }

    #[test]
    fn bad_duration_error_names_the_variable() {
        let err = LeaderElectionConfig::from_lookup(
            lookup(&[(ENABLED_ENV, "true"), (LEASE_DURATION_ENV, "abc")]),
            "ns",
        )
        .unwrap_err();
        assert!(err.to_string().contains(LEASE_DURATION_ENV), "{err}");
    }

    #[test]
    fn validate_enforces_client_go_ordering() {
        let base = LeaderElectionConfig {
            lease_name: "l".into(),
            namespace: "n".into(),
            lease_duration: Duration::from_secs(15),
            renew_deadline: Duration::from_secs(10),
            retry_period: Duration::from_secs(2),
        };
        assert!(base.validate().is_ok());

        let renew_not_below_lease = LeaderElectionConfig {
            renew_deadline: Duration::from_secs(15),
            ..base.clone()
        };
        assert!(renew_not_below_lease.validate().is_err());

        let retry_not_below_renew = LeaderElectionConfig {
            retry_period: Duration::from_secs(10),
            ..base.clone()
        };
        assert!(retry_not_below_renew.validate().is_err());

        let zero_retry = LeaderElectionConfig {
            retry_period: Duration::ZERO,
            ..base.clone()
        };
        assert!(zero_retry.validate().is_err());

        let empty_name = LeaderElectionConfig {
            lease_name: String::new(),
            ..base
        };
        assert!(empty_name.validate().is_err());
    }

    #[test]
    fn env_with_inverted_timings_is_rejected() {
        let err = LeaderElectionConfig::from_lookup(
            lookup(&[
                (ENABLED_ENV, "true"),
                (LEASE_DURATION_ENV, "5s"),
                (RENEW_DEADLINE_ENV, "10s"),
            ]),
            "ns",
        )
        .unwrap_err();
        assert!(err.to_string().contains(LEASE_DURATION_ENV), "{err}");
    }

    #[test]
    fn step_timeout_is_half_the_renew_deadline_with_a_floor() {
        let mut cfg = LeaderElectionConfig {
            lease_name: "l".into(),
            namespace: "n".into(),
            lease_duration: Duration::from_secs(15),
            renew_deadline: Duration::from_secs(10),
            retry_period: Duration::from_secs(2),
        };
        assert_eq!(cfg.step_timeout(), Duration::from_secs(5));
        cfg.renew_deadline = Duration::from_millis(600);
        assert_eq!(cfg.step_timeout(), Duration::from_millis(500));
    }

    #[test]
    fn readiness_is_ready_for_leader_and_standby_but_not_before_observation() {
        assert_eq!(
            readiness(LeaderState::Unknown),
            (StatusCode::SERVICE_UNAVAILABLE, "leader election pending")
        );
        assert_eq!(
            readiness(LeaderState::Follower),
            (StatusCode::OK, "standby")
        );
        assert_eq!(readiness(LeaderState::Leader), (StatusCode::OK, "leader"));
    }
}
