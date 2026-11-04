//! Runs started by this operator process (issue #98).
//!
//! Backups, restores, validations and offset resets/rollbacks run inside the
//! operator process, in the reconcile that started them, after patching
//! `phase: Running`. When the process dies mid-run (OOMKilled, evicted,
//! rolled), nothing in the new process runs it, yet the resource keeps
//! `Running`.
//!
//! Reconciles of one object are serialised, so a `Running` status written by
//! a run this process started is only the reflector cache lagging behind that
//! run's final patch, while one this process did not start was interrupted.
//! Interrupted runs are failed, not resumed: restores are not checkpointed
//! (running one again would produce its records again), and the interruption
//! may have been an OOM kill caused by the run itself, which an automatic
//! resume would turn into a crash loop.

use std::collections::HashSet;
use std::sync::{Mutex, OnceLock};

use chrono::Utc;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{Patch, PatchParams};
use kube::Api;
use serde_json::json;
use tracing::warn;

/// Whether a `Running` status belongs to a run of this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunningState {
    /// This process started the run (it may have finished since).
    Owned,
    /// Another process started the run and died before finishing it.
    Interrupted,
}

/// Kind, uid and generation: a recreated resource has a new uid, a changed
/// spec a new generation, and the kind keeps resources of different kinds
/// apart.
type RunKey = (&'static str, String, i64);

fn started() -> &'static Mutex<HashSet<RunKey>> {
    static STARTED: OnceLock<Mutex<HashSet<RunKey>>> = OnceLock::new();
    STARTED.get_or_init(|| Mutex::new(HashSet::new()))
}

fn key(kind: &'static str, meta: &ObjectMeta) -> RunKey {
    let uid = meta.uid.clone().unwrap_or_else(|| {
        // Every stored object has a uid; fall back to the name for objects
        // built in memory.
        format!(
            "{}/{}",
            meta.namespace.as_deref().unwrap_or_default(),
            meta.name.as_deref().unwrap_or_default()
        )
    });
    (kind, uid, meta.generation.unwrap_or(0))
}

/// Record that this process is starting a run of the resource's current
/// generation. Call before patching `phase: Running`.
pub fn record_start(kind: &'static str, meta: &ObjectMeta) {
    started().lock().unwrap().insert(key(kind, meta));
}

/// Classify a `Running` status of the resource's current generation.
pub fn running_state(kind: &'static str, meta: &ObjectMeta) -> RunningState {
    if started().lock().unwrap().contains(&key(kind, meta)) {
        RunningState::Owned
    } else {
        RunningState::Interrupted
    }
}

/// Status patch failing an interrupted run of a `what` ("restore",
/// "backup", ...), with reason `Interrupted`.
pub fn interrupted_status(what: &str, generation: Option<i64>) -> serde_json::Value {
    let message = format!(
        "Interrupted: the operator restarted while this {what} was running, and it was not \
         resumed. Anything it wrote before the interruption is kept. To run it again, change \
         the spec (a metadata-only change such as an annotation does not start a new run)."
    );
    json!({
        "status": {
            "phase": "Failed",
            "message": message,
            "observedGeneration": generation,
            "conditions": [{
                "type": "Ready",
                "status": "False",
                "lastTransitionTime": Utc::now(),
                "reason": "Interrupted",
                "message": message,
            }]
        }
    })
}

/// Fail an interrupted run of `what` ("restore", "backup", ...).
pub async fn mark_interrupted<K>(
    api: &Api<K>,
    name: &str,
    what: &str,
    generation: Option<i64>,
) -> crate::error::Result<()>
where
    K: Clone + serde::de::DeserializeOwned + std::fmt::Debug,
{
    warn!(
        name = %name,
        "The {what} was Running when the operator restarted; marking it Failed (Interrupted)"
    );
    api.patch_status(
        name,
        &PatchParams::apply("kafka-backup-operator"),
        &Patch::Merge(interrupted_status(what, generation)),
    )
    .await?;
    Ok(())
}
