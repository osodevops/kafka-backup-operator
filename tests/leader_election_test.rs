//! Leader election against a mock API server (issue #79).
//!
//! The mock keeps one Lease in memory with real `resourceVersion` semantics:
//! `PUT` succeeds only when the body carries the current version, otherwise it
//! answers `409 Conflict` — so every test also proves the elector's writes are
//! compare-and-swap.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use http::{Request, Response};
use http_body_util::BodyExt;
use kafka_backup_operator::leader::{
    Clock, LeaderElectionConfig, LeaderElector, LeaderError, LeaderState, StepOutcome,
};
use kube::client::Body;
use kube::Client;
use serde_json::{json, Value};
use tokio::sync::watch;
use tower_test::mock;

const NS: &str = "kbo";
const LEASE: &str = "kafka-backup-operator-leader";
const ME: &str = "kbo-operator-abc";
const OTHER: &str = "kbo-operator-xyz";

#[derive(Debug, Clone)]
struct Recorded {
    method: String,
    path: String,
    body: Value,
}

#[derive(Default)]
struct LeaseStore {
    lease: Option<Value>,
    next_rv: u64,
    fail_put_with: Option<u16>,
    hang_requests: bool,
    conflict_next_put: bool,
    conflict_next_post: bool,
    recorded: Vec<Recorded>,
}

#[derive(Clone)]
struct MockLeaseServer(Arc<Mutex<LeaseStore>>);

fn status(code: u16, reason: &str, message: &str) -> Value {
    json!({
        "kind": "Status", "apiVersion": "v1", "metadata": {},
        "status": "Failure", "message": message, "reason": reason, "code": code
    })
}

impl MockLeaseServer {
    fn start() -> (Self, Client) {
        let (service, mut handle) = mock::pair::<Request<Body>, Response<Body>>();
        let server = MockLeaseServer(Arc::new(Mutex::new(LeaseStore {
            next_rv: 1,
            ..Default::default()
        })));
        let store = Arc::clone(&server.0);
        tokio::spawn(async move {
            while let Some((request, send)) = handle.next_request().await {
                let method = request.method().to_string();
                let path = request.uri().path().to_string();
                let bytes = request.into_body().collect().await.unwrap().to_bytes();
                let body: Value = if bytes.is_empty() {
                    Value::Null
                } else {
                    serde_json::from_slice(&bytes).unwrap()
                };
                if store.lock().unwrap().hang_requests {
                    // Never answer: the caller's request must be bounded by its own timeout.
                    std::mem::forget(send);
                    continue;
                }
                let (code, response) = {
                    let mut s = store.lock().unwrap();
                    s.recorded.push(Recorded {
                        method: method.clone(),
                        path: path.clone(),
                        body: body.clone(),
                    });
                    let base = format!("/apis/coordination.k8s.io/v1/namespaces/{NS}/leases");
                    let one = format!("{base}/{LEASE}");
                    match (method.as_str(), path.as_str()) {
                        ("GET", p) if p == one => match &s.lease {
                            Some(l) => (200, l.clone()),
                            None => (404, status(404, "NotFound", "lease not found")),
                        },
                        ("POST", p) if p == base => {
                            if s.lease.is_some() || std::mem::take(&mut s.conflict_next_post) {
                                if s.lease.is_none() {
                                    // Simulate a racing creator: the lease now exists, held by OTHER.
                                    let mut l = body.clone();
                                    l["spec"]["holderIdentity"] = json!(OTHER);
                                    l["metadata"]["resourceVersion"] = json!(s.next_rv.to_string());
                                    s.next_rv += 1;
                                    s.lease = Some(l);
                                }
                                (409, status(409, "AlreadyExists", "lease already exists"))
                            } else {
                                let mut l = body.clone();
                                l["metadata"]["resourceVersion"] = json!(s.next_rv.to_string());
                                s.next_rv += 1;
                                s.lease = Some(l.clone());
                                (201, l)
                            }
                        }
                        ("PUT", p) if p == one => {
                            if let Some(code) = s.fail_put_with {
                                (code, status(code, "InternalError", "injected failure"))
                            } else if std::mem::take(&mut s.conflict_next_put) {
                                (409, status(409, "Conflict", "injected conflict"))
                            } else {
                                let current_rv = s.lease.as_ref().and_then(|l| {
                                    l["metadata"]["resourceVersion"]
                                        .as_str()
                                        .map(str::to_string)
                                });
                                let sent_rv = body["metadata"]["resourceVersion"]
                                    .as_str()
                                    .map(str::to_string);
                                if s.lease.is_none() {
                                    (404, status(404, "NotFound", "lease not found"))
                                } else if sent_rv != current_rv {
                                    (409, status(409, "Conflict", "resourceVersion mismatch"))
                                } else {
                                    let mut l = body.clone();
                                    l["metadata"]["resourceVersion"] = json!(s.next_rv.to_string());
                                    s.next_rv += 1;
                                    s.lease = Some(l.clone());
                                    (200, l)
                                }
                            }
                        }
                        _ => (404, status(404, "NotFound", "unhandled route")),
                    }
                };
                let response = Response::builder()
                    .status(code)
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&response).unwrap()))
                    .unwrap();
                send.send_response(response);
            }
        });
        let client = Client::new(service, NS);
        (server, client)
    }

    fn seed(&self, holder: &str, acquire: DateTime<Utc>, renew: DateTime<Utc>, transitions: i32) {
        let mut s = self.0.lock().unwrap();
        s.lease = Some(json!({
            "apiVersion": "coordination.k8s.io/v1", "kind": "Lease",
            "metadata": {"name": LEASE, "namespace": NS, "resourceVersion": s.next_rv.to_string()},
            "spec": {
                "holderIdentity": holder,
                "leaseDurationSeconds": 15,
                "acquireTime": acquire.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                "renewTime": renew.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                "leaseTransitions": transitions
            }
        }));
        s.next_rv += 1;
    }
    fn lease(&self) -> Value {
        self.0.lock().unwrap().lease.clone().expect("lease present")
    }
    fn fail_puts(&self, code: Option<u16>) {
        self.0.lock().unwrap().fail_put_with = code;
    }
    fn hang_requests(&self, hang: bool) {
        self.0.lock().unwrap().hang_requests = hang;
    }
    fn conflict_next_put(&self) {
        self.0.lock().unwrap().conflict_next_put = true;
    }
    fn conflict_next_post(&self) {
        self.0.lock().unwrap().conflict_next_post = true;
    }
    fn requests(&self) -> Vec<Recorded> {
        self.0.lock().unwrap().recorded.clone()
    }
    fn writes(&self) -> Vec<Recorded> {
        self.requests()
            .into_iter()
            .filter(|r| r.method == "POST" || r.method == "PUT")
            .collect()
    }
    fn count(&self, method: &str) -> usize {
        self.requests()
            .iter()
            .filter(|r| r.method == method)
            .count()
    }
}

fn config() -> LeaderElectionConfig {
    LeaderElectionConfig {
        lease_name: LEASE.to_string(),
        namespace: NS.to_string(),
        lease_duration: Duration::from_secs(15),
        renew_deadline: Duration::from_secs(10),
        retry_period: Duration::from_secs(2),
    }
}

struct TestClock(Arc<Mutex<DateTime<Utc>>>);
impl TestClock {
    fn at(t: DateTime<Utc>) -> (Self, Clock) {
        let shared = Arc::new(Mutex::new(t));
        let c = Arc::clone(&shared);
        (Self(shared), Arc::new(move || *c.lock().unwrap()))
    }
    fn advance(&self, d: Duration) {
        *self.0.lock().unwrap() += TimeDelta::from_std(d).unwrap();
    }
}

fn t0() -> DateTime<Utc> {
    "2026-08-29T12:00:00Z".parse().unwrap()
}

fn elector(client: Client, clock: Clock) -> (LeaderElector, watch::Receiver<LeaderState>) {
    let (tx, rx) = watch::channel(LeaderState::Unknown);
    (
        LeaderElector::new(client, config(), ME.to_string(), tx).with_clock(clock),
        rx,
    )
}

fn micro(t: DateTime<Utc>) -> Value {
    json!(t.to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
}

#[tokio::test]
async fn acquire_creates_the_lease_when_absent() {
    let (server, client) = MockLeaseServer::start();
    let (_clock, now) = TestClock::at(t0());
    let (mut e, rx) = elector(client, now);

    assert_eq!(
        e.try_acquire_or_renew().await.unwrap(),
        StepOutcome::Leading
    );
    assert_eq!(*rx.borrow(), LeaderState::Leader);

    let writes = server.writes();
    assert_eq!(writes.len(), 1);
    let post = &writes[0];
    assert_eq!(post.method, "POST");
    assert_eq!(
        post.path,
        format!("/apis/coordination.k8s.io/v1/namespaces/{NS}/leases")
    );
    assert_eq!(post.body["apiVersion"], json!("coordination.k8s.io/v1"));
    assert_eq!(post.body["kind"], json!("Lease"));
    assert_eq!(post.body["metadata"]["name"], json!(LEASE));
    assert_eq!(post.body["metadata"]["namespace"], json!(NS));
    assert_eq!(post.body["spec"]["holderIdentity"], json!(ME));
    assert_eq!(post.body["spec"]["leaseDurationSeconds"], json!(15));
    assert_eq!(post.body["spec"]["leaseTransitions"], json!(0));
    assert_eq!(post.body["spec"]["acquireTime"], micro(t0()));
    assert_eq!(post.body["spec"]["renewTime"], micro(t0()));
}

#[tokio::test]
async fn stands_by_without_writing_while_another_holder_is_fresh() {
    let (server, client) = MockLeaseServer::start();
    // The record's own renewTime is an hour old: irrelevant, expiry is judged
    // on *our* observation time, and we have only just observed it.
    server.seed(
        OTHER,
        t0() - TimeDelta::hours(2),
        t0() - TimeDelta::hours(1),
        3,
    );
    let (clock, now) = TestClock::at(t0());
    let (mut e, rx) = elector(client, now);

    assert_eq!(
        e.try_acquire_or_renew().await.unwrap(),
        StepOutcome::Standby
    );
    assert_eq!(*rx.borrow(), LeaderState::Follower);
    clock.advance(Duration::from_secs(14));
    assert_eq!(
        e.try_acquire_or_renew().await.unwrap(),
        StepOutcome::Standby
    );

    assert!(server.writes().is_empty(), "a standby must never write");
    assert_eq!(server.count("GET"), 2);
    assert!(
        !e.renew_deadline_exceeded(),
        "only leaders have a renew deadline"
    );
}

#[tokio::test]
async fn takes_over_an_expired_lease_with_a_cas_write() {
    let (server, client) = MockLeaseServer::start();
    server.seed(
        OTHER,
        t0() - TimeDelta::minutes(5),
        t0() - TimeDelta::seconds(1),
        3,
    );
    let (clock, now) = TestClock::at(t0());
    let (mut e, rx) = elector(client, now);

    assert_eq!(
        e.try_acquire_or_renew().await.unwrap(),
        StepOutcome::Standby
    );
    let seeded_rv = server.lease()["metadata"]["resourceVersion"].clone();

    clock.advance(Duration::from_secs(16));
    assert_eq!(
        e.try_acquire_or_renew().await.unwrap(),
        StepOutcome::Leading
    );
    assert_eq!(*rx.borrow(), LeaderState::Leader);

    let writes = server.writes();
    assert_eq!(writes.len(), 1);
    let put = &writes[0];
    assert_eq!(put.method, "PUT");
    assert_eq!(put.body["metadata"]["resourceVersion"], seeded_rv);
    assert_eq!(put.body["spec"]["holderIdentity"], json!(ME));
    assert_eq!(put.body["spec"]["leaseTransitions"], json!(4));
    assert_eq!(
        put.body["spec"]["acquireTime"],
        micro(t0() + TimeDelta::seconds(16))
    );
    assert_eq!(
        put.body["spec"]["renewTime"],
        micro(t0() + TimeDelta::seconds(16))
    );
    assert_eq!(put.body["spec"]["leaseDurationSeconds"], json!(15));
}

#[tokio::test]
async fn a_change_by_the_holder_restarts_the_expiry_clock() {
    let (server, client) = MockLeaseServer::start();
    server.seed(OTHER, t0(), t0(), 0);
    let (clock, now) = TestClock::at(t0());
    let (mut e, _rx) = elector(client, now);

    assert_eq!(
        e.try_acquire_or_renew().await.unwrap(),
        StepOutcome::Standby
    );
    clock.advance(Duration::from_secs(10));
    // The holder renews (new renewTime): our observation restarts.
    server.seed(OTHER, t0(), t0() + TimeDelta::seconds(10), 0);
    assert_eq!(
        e.try_acquire_or_renew().await.unwrap(),
        StepOutcome::Standby
    );
    clock.advance(Duration::from_secs(10));
    assert_eq!(
        e.try_acquire_or_renew().await.unwrap(),
        StepOutcome::Standby,
        "20s after first sight but only 10s after the last change: not expired"
    );
    assert!(server.writes().is_empty());
}

#[tokio::test]
async fn renewing_own_lease_keeps_acquire_time_and_transitions() {
    let (server, client) = MockLeaseServer::start();
    let acquired = t0() - TimeDelta::seconds(30);
    server.seed(ME, acquired, t0() - TimeDelta::seconds(2), 2);
    let (_clock, now) = TestClock::at(t0());
    let (mut e, rx) = elector(client, now);

    assert_eq!(
        e.try_acquire_or_renew().await.unwrap(),
        StepOutcome::Leading
    );
    assert_eq!(*rx.borrow(), LeaderState::Leader);

    let put = &server.writes()[0];
    assert_eq!(put.method, "PUT");
    assert_eq!(put.body["spec"]["holderIdentity"], json!(ME));
    assert_eq!(put.body["spec"]["acquireTime"], micro(acquired));
    assert_eq!(put.body["spec"]["leaseTransitions"], json!(2));
    assert_eq!(put.body["spec"]["renewTime"], micro(t0()));
}

#[tokio::test]
async fn an_empty_holder_is_acquired_immediately() {
    let (server, client) = MockLeaseServer::start();
    server.seed(
        "",
        t0() - TimeDelta::seconds(1),
        t0() - TimeDelta::seconds(1),
        7,
    );
    let (_clock, now) = TestClock::at(t0());
    let (mut e, _rx) = elector(client, now);

    assert_eq!(
        e.try_acquire_or_renew().await.unwrap(),
        StepOutcome::Leading
    );
    let put = &server.writes()[0];
    assert_eq!(put.body["spec"]["holderIdentity"], json!(ME));
    assert_eq!(put.body["spec"]["leaseTransitions"], json!(8));
    assert_eq!(put.body["spec"]["acquireTime"], micro(t0()));
}

#[tokio::test]
async fn put_conflict_is_retried_from_a_fresh_read() {
    let (server, client) = MockLeaseServer::start();
    server.seed("", t0(), t0(), 0);
    let (_clock, now) = TestClock::at(t0());
    let (mut e, rx) = elector(client, now);

    server.conflict_next_put();
    assert_eq!(
        e.try_acquire_or_renew().await.unwrap(),
        StepOutcome::Conflict
    );
    assert_ne!(*rx.borrow(), LeaderState::Leader);

    assert_eq!(
        e.try_acquire_or_renew().await.unwrap(),
        StepOutcome::Leading
    );
    let methods: Vec<String> = server.requests().into_iter().map(|r| r.method).collect();
    assert_eq!(methods, vec!["GET", "PUT", "GET", "PUT"]);
}

#[tokio::test]
async fn stale_resource_version_is_rejected_by_the_server_and_not_retried_blindly() {
    let (server, client) = MockLeaseServer::start();
    server.seed(ME, t0(), t0(), 1);
    let (_clock, now) = TestClock::at(t0());
    let (mut e, _rx) = elector(client, now);
    assert_eq!(
        e.try_acquire_or_renew().await.unwrap(),
        StepOutcome::Leading
    );

    // Someone else bumps the version behind our back (fresh holder).
    server.seed(OTHER, t0(), t0(), 2);
    assert_eq!(
        e.try_acquire_or_renew().await.unwrap(),
        StepOutcome::Standby,
        "the fresh read shows a new holder, so no write is attempted"
    );
    assert_eq!(server.count("PUT"), 1);
}

#[tokio::test]
async fn losing_the_create_race_is_a_conflict_then_standby() {
    let (server, client) = MockLeaseServer::start();
    let (_clock, now) = TestClock::at(t0());
    let (mut e, rx) = elector(client, now);

    server.conflict_next_post();
    assert_eq!(
        e.try_acquire_or_renew().await.unwrap(),
        StepOutcome::Conflict
    );
    assert_eq!(
        e.try_acquire_or_renew().await.unwrap(),
        StepOutcome::Standby
    );
    assert_eq!(*rx.borrow(), LeaderState::Follower);
    assert_eq!(
        server.count("PUT"),
        0,
        "never PUT with a guessed resourceVersion"
    );
}

#[tokio::test]
async fn renew_deadline_is_exceeded_when_renewals_keep_failing() {
    let (server, client) = MockLeaseServer::start();
    let (clock, now) = TestClock::at(t0());
    let (mut e, rx) = elector(client, now);
    assert_eq!(
        e.try_acquire_or_renew().await.unwrap(),
        StepOutcome::Leading
    );

    server.fail_puts(Some(500));
    clock.advance(Duration::from_secs(2));
    assert!(e.try_acquire_or_renew().await.is_err());
    assert_eq!(
        *rx.borrow(),
        LeaderState::Leader,
        "still leading within the deadline"
    );
    assert!(!e.renew_deadline_exceeded());

    clock.advance(Duration::from_secs(9));
    assert!(e.try_acquire_or_renew().await.is_err());
    assert!(
        e.renew_deadline_exceeded(),
        "11s without a successful renewal"
    );
}

#[tokio::test]
async fn release_clears_the_holder_only_when_we_hold_it() {
    let (server, client) = MockLeaseServer::start();
    let (_clock, now) = TestClock::at(t0());
    let (mut e, _rx) = elector(client, now);

    // Not holding: nothing written.
    server.seed(OTHER, t0(), t0(), 1);
    assert_eq!(
        e.try_acquire_or_renew().await.unwrap(),
        StepOutcome::Standby
    );
    e.release().await.unwrap();
    assert!(server.writes().is_empty());

    // Holding: holder cleared and duration shortened, transitions kept.
    server.seed(ME, t0(), t0(), 5);
    assert_eq!(
        e.try_acquire_or_renew().await.unwrap(),
        StepOutcome::Leading
    );
    e.release().await.unwrap();
    let put = server.writes().last().unwrap().clone();
    assert_eq!(put.method, "PUT");
    assert_eq!(put.body["spec"]["holderIdentity"], json!(""));
    assert_eq!(put.body["spec"]["leaseDurationSeconds"], json!(1));
    assert_eq!(put.body["spec"]["leaseTransitions"], json!(5));
    assert_eq!(
        server.lease()["spec"]["holderIdentity"],
        json!(""),
        "the mock store reflects the release"
    );
}

/// A clock that follows tokio's (pausable) time, for `run()` tests.
fn tokio_clock() -> Clock {
    let base_utc = t0();
    let base = tokio::time::Instant::now();
    Arc::new(move || base_utc + TimeDelta::from_std(base.elapsed()).unwrap())
}

#[tokio::test(start_paused = true)]
async fn run_steps_down_with_an_error_when_the_renew_deadline_passes() {
    let (server, client) = MockLeaseServer::start();
    let (tx, mut rx) = watch::channel(LeaderState::Unknown);
    let e = LeaderElector::new(client, config(), ME.to_string(), tx).with_clock(tokio_clock());
    let (_release_tx, release_rx) = watch::channel(false);

    server.fail_puts(Some(500)); // the initial POST succeeds, every renewal fails
    let started = tokio::time::Instant::now();
    let outcome = e.run(release_rx).await;

    let elapsed = started.elapsed();
    assert!(
        matches!(outcome, Err(LeaderError::RenewDeadlineExceeded { .. })),
        "{outcome:?}"
    );
    assert!(
        elapsed >= Duration::from_secs(10) && elapsed <= Duration::from_secs(13),
        "stepped down after {elapsed:?}, expected ~renew_deadline"
    );
    let puts = server.count("PUT");
    assert!(
        (4..=6).contains(&puts),
        "one renewal attempt per retry period, got {puts}"
    );
    assert_eq!(
        *rx.borrow_and_update(),
        LeaderState::Follower,
        "a leader that could not renew must publish that it no longer leads"
    );
}

#[tokio::test(start_paused = true)]
async fn run_survives_renew_failures_that_clear_before_the_deadline() {
    let (server, client) = MockLeaseServer::start();
    let (tx, rx) = watch::channel(LeaderState::Unknown);
    let e = LeaderElector::new(client, config(), ME.to_string(), tx).with_clock(tokio_clock());
    let (release_tx, release_rx) = watch::channel(false);

    let task = tokio::spawn(e.run(release_rx));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(*rx.borrow(), LeaderState::Leader);

    server.fail_puts(Some(503));
    tokio::time::sleep(Duration::from_secs(6)).await;
    server.fail_puts(None);
    tokio::time::sleep(Duration::from_secs(24)).await;

    assert!(
        !task.is_finished(),
        "must not step down: a renewal succeeded within the deadline"
    );
    assert_eq!(*rx.borrow(), LeaderState::Leader);

    release_tx.send(true).unwrap();
    assert!(matches!(task.await.unwrap(), Ok(())));
    assert_eq!(*rx.borrow(), LeaderState::Follower);
    assert_eq!(server.lease()["spec"]["holderIdentity"], json!(""));
}

#[tokio::test(start_paused = true)]
async fn run_promotes_a_standby_once_the_holder_stops_renewing() {
    let (server, client) = MockLeaseServer::start();
    server.seed(OTHER, t0(), t0(), 3);
    let (tx, mut rx) = watch::channel(LeaderState::Unknown);
    let e = LeaderElector::new(client, config(), ME.to_string(), tx).with_clock(tokio_clock());
    let (release_tx, release_rx) = watch::channel(false);

    let started = tokio::time::Instant::now();
    let task = tokio::spawn(e.run(release_rx));

    rx.wait_for(|s| *s == LeaderState::Follower).await.unwrap();
    assert!(server.writes().is_empty());
    rx.wait_for(|s| *s == LeaderState::Leader).await.unwrap();
    let promoted_after = started.elapsed();
    assert!(
        promoted_after >= Duration::from_secs(15) && promoted_after <= Duration::from_secs(18),
        "promoted after {promoted_after:?}: expected lease_duration + at most one retry period"
    );
    assert_eq!(server.lease()["spec"]["holderIdentity"], json!(ME));
    assert_eq!(server.lease()["spec"]["leaseTransitions"], json!(4));

    release_tx.send(true).unwrap();
    assert!(matches!(task.await.unwrap(), Ok(())));
}

#[tokio::test(start_paused = true)]
async fn run_reports_lost_leadership_when_another_replica_took_the_lease() {
    let (server, client) = MockLeaseServer::start();
    let (tx, rx) = watch::channel(LeaderState::Unknown);
    let e = LeaderElector::new(client, config(), ME.to_string(), tx).with_clock(tokio_clock());
    let (_release_tx, release_rx) = watch::channel(false);

    let task = tokio::spawn(e.run(release_rx));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(*rx.borrow(), LeaderState::Leader);

    // Out of band, someone else now holds a fresh lease (e.g. after we were
    // partitioned for longer than lease_duration).
    server.seed(OTHER, t0(), t0(), 9);
    let outcome = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(outcome, Err(LeaderError::Lost { .. })),
        "{outcome:?}"
    );
    assert_eq!(*rx.borrow(), LeaderState::Follower);
}

#[tokio::test(start_paused = true)]
async fn dropping_the_release_sender_also_releases() {
    let (server, client) = MockLeaseServer::start();
    let (tx, rx) = watch::channel(LeaderState::Unknown);
    let e = LeaderElector::new(client, config(), ME.to_string(), tx).with_clock(tokio_clock());
    let (release_tx, release_rx) = watch::channel(false);

    let task = tokio::spawn(e.run(release_rx));
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(release_tx);
    assert!(matches!(task.await.unwrap(), Ok(())));
    assert_eq!(*rx.borrow(), LeaderState::Follower);
    assert_eq!(server.lease()["spec"]["holderIdentity"], json!(""));
}

/// A lease request that never answers (black-holed connection, stalled API
/// server) must not let the leader believe it still leads: each step is
/// bounded, so the renew deadline is detected and the elector steps down.
#[tokio::test(start_paused = true)]
async fn run_steps_down_when_lease_requests_hang() {
    let (server, client) = MockLeaseServer::start();
    let (tx, rx) = watch::channel(LeaderState::Unknown);
    let e = LeaderElector::new(client, config(), ME.to_string(), tx).with_clock(tokio_clock());
    let (_release_tx, release_rx) = watch::channel(false);

    let task = tokio::spawn(e.run(release_rx));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(*rx.borrow(), LeaderState::Leader);

    let frozen_at = tokio::time::Instant::now();
    server.hang_requests(true);
    let outcome = tokio::time::timeout(Duration::from_secs(60), task)
        .await
        .expect("a hung request must not stall the elector past the deadline")
        .unwrap();
    let elapsed = frozen_at.elapsed();
    assert!(
        matches!(outcome, Err(LeaderError::RenewDeadlineExceeded { .. })),
        "{outcome:?}"
    );
    assert!(
        elapsed >= Duration::from_secs(10) && elapsed <= Duration::from_secs(18),
        "stepped down after {elapsed:?}: expected renew_deadline plus at most one bounded step"
    );
    assert_eq!(*rx.borrow(), LeaderState::Follower);
}
