//! Issue #98: a KafkaRestore stayed Running for ever after an operator restart.
//!
//! Restores (like backups, validations and offset resets/rollbacks) run inside
//! the operator process, in the reconcile that started them, after patching
//! `phase: Running`. When the process dies mid-run (OOMKilled, evicted,
//! rolled), nothing in the new process is running it, but the resource keeps
//! `Running`: the restore controller only requeued it every 5 s ("Monitoring
//! restore progress"), so nothing retried it and nothing reported a failure.
//! Reproduced in minikube: SIGKILL during a restore, then `Running` / "Restore
//! in progress" for as long as we watched.
//!
//! Reconciles of one object are serialised, so a `Running` status written by
//! a run this process started is only the reflector cache lagging behind the
//! run's final patch; one this process did not start was interrupted.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kafka_backup_operator::crd::{KafkaBackup, KafkaRestore};
use kafka_backup_operator::reconcilers::backup::{one_shot_backup_decision, OneShotBackup};
use kafka_backup_operator::reconcilers::runs::{
    interrupted_status, record_start, running_state, RunningState,
};
use serde_json::json;

fn meta(name: &str, uid: &str, generation: i64) -> ObjectMeta {
    ObjectMeta {
        name: Some(name.to_string()),
        namespace: Some("kbo".to_string()),
        uid: Some(uid.to_string()),
        generation: Some(generation),
        ..ObjectMeta::default()
    }
}

#[test]
fn a_running_status_left_by_another_process_is_interrupted() {
    let restore = meta("slow-restore", "uid-left-behind", 1);
    assert_eq!(
        running_state("KafkaRestore", &restore),
        RunningState::Interrupted
    );
}

/// The reflector cache can still show `Running` right after this process
/// finished the run; that must not be mistaken for an interruption.
#[test]
fn a_running_status_this_process_wrote_is_its_own_run() {
    let restore = meta("own-restore", "uid-own", 3);
    record_start("KafkaRestore", &restore);
    assert_eq!(running_state("KafkaRestore", &restore), RunningState::Owned);
}

/// A run of an earlier generation does not make a later one ours: the spec
/// changed and the new run was started by a process that has since died.
#[test]
fn ownership_is_per_generation() {
    record_start("KafkaRestore", &meta("regen", "uid-regen", 1));
    assert_eq!(
        running_state("KafkaRestore", &meta("regen", "uid-regen", 2)),
        RunningState::Interrupted
    );
}

#[test]
fn ownership_is_per_kind() {
    record_start("KafkaBackup", &meta("same", "uid-same", 1));
    assert_eq!(
        running_state("KafkaRestore", &meta("same", "uid-same", 1)),
        RunningState::Interrupted
    );
}

#[test]
fn an_interrupted_run_is_failed_with_reason_interrupted() {
    let patch = interrupted_status("restore", Some(4));
    let status = &patch["status"];
    assert_eq!(status["phase"], "Failed");
    assert_eq!(status["observedGeneration"], 4);
    let message = status["message"].as_str().unwrap();
    assert!(message.starts_with("Interrupted"), "{message}");
    assert!(message.contains("restore"), "{message}");
    assert!(message.contains("change the spec"), "{message}");
    let condition = &status["conditions"][0];
    assert_eq!(condition["type"], "Ready");
    assert_eq!(condition["status"], "False");
    assert_eq!(condition["reason"], "Interrupted");
}

fn backup(name: &str, uid: &str, phase: Option<&str>, spec: serde_json::Value) -> KafkaBackup {
    let mut spec_json = json!({
        "topics": ["orders"],
        "kafkaCluster": {"bootstrapServers": ["kafka:9092"]},
        "storage": {"storageType": "pvc", "pvc": {"claimName": "backups"}},
    });
    spec_json
        .as_object_mut()
        .unwrap()
        .extend(spec.as_object().unwrap().clone());
    let mut backup = KafkaBackup::new(name, serde_json::from_value(spec_json).unwrap());
    backup.metadata = meta(name, uid, 1);
    if let Some(phase) = phase {
        backup.status = Some(serde_json::from_value(json!({"phase": phase})).unwrap());
    }
    backup
}

/// One-shot (and continuous) backups were never run again once `Running`,
/// so an interrupted one also stayed `Running` for ever.
#[test]
fn an_interrupted_one_shot_backup_is_reported() {
    let fresh = backup("fresh", "uid-b-fresh", None, json!({}));
    assert_eq!(one_shot_backup_decision(&fresh), OneShotBackup::Run);

    let orphan = backup("orphan", "uid-b-orphan", Some("Running"), json!({}));
    assert_eq!(
        one_shot_backup_decision(&orphan),
        OneShotBackup::Interrupted
    );

    let continuous = backup(
        "continuous",
        "uid-b-cont",
        Some("Running"),
        json!({"continuous": true}),
    );
    assert_eq!(
        one_shot_backup_decision(&continuous),
        OneShotBackup::Interrupted
    );

    let own = backup("own", "uid-b-own", Some("Running"), json!({}));
    record_start("KafkaBackup", &own.metadata);
    assert_eq!(one_shot_backup_decision(&own), OneShotBackup::Wait);

    for phase in ["Completed", "Failed"] {
        let done = backup("done", "uid-b-done", Some(phase), json!({}));
        assert_eq!(one_shot_backup_decision(&done), OneShotBackup::Wait);
    }
}

#[test]
fn restore_resources_carry_the_metadata_the_registry_needs() {
    // Guards the key: name alone is not enough (a recreated resource has a
    // new uid; another namespace can reuse the name).
    let spec = json!({
        "backupRef": {"name": "orders-backup"},
        "kafkaCluster": {"bootstrapServers": ["kafka:9092"]},
    });
    let mut restore = KafkaRestore::new("r", serde_json::from_value(spec).unwrap());
    restore.metadata = meta("r", "uid-r-1", 1);
    record_start("KafkaRestore", &restore.metadata);

    let mut recreated = restore.clone();
    recreated.metadata.uid = Some("uid-r-2".to_string());
    assert_eq!(
        running_state("KafkaRestore", &recreated.metadata),
        RunningState::Interrupted
    );
}
