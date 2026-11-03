//! Issue #94: incremental scheduled backups never resumed.
//!
//! The documented incremental configuration (`checkpoint.enabled: true` with
//! `stopAtCurrentOffsets: true` on a schedule) promises that each run loads
//! the previous run's checkpoint and backs up only new records. The engine
//! resumes from `{backup_id}/offsets.db` and `{backup_id}/manifest.json`, so
//! that only holds when every run of the resource uses the same backup ID.
//! The operator generated `{name}-{YYYYMMDD-HHMMSS}` on every run, so no run
//! could find the previous checkpoint and each one was a full backup.
//!
//! These tests pin the ID that reaches the core config, through the same
//! adapters the reconciler uses.

use chrono::{TimeZone, Utc};
use kafka_backup_operator::adapters::{build_backup_config, to_core_backup_config};
use kafka_backup_operator::crd::KafkaBackup;
use kafka_backup_operator::reconcilers::backup::{backup_id_for, is_incremental};
use serde_json::json;

fn test_client() -> kube::Client {
    kafka_backup_operator::install_crypto_provider();
    kube::Client::try_from(kube::Config::new(
        "http://127.0.0.1".parse().expect("valid URL"),
    ))
    .expect("client can be built without contacting a cluster")
}

fn scheduled_backup(spec_overrides: serde_json::Value) -> KafkaBackup {
    let mut spec = json!({
        "schedule": "0 0 * * * * *",
        "stopAtCurrentOffsets": true,
        "kafkaCluster": {"bootstrapServers": ["kafka:9092"]},
        "topics": ["orders", "payments"],
        "storage": {
            "storageType": "pvc",
            "pvc": {"claimName": "backups"}
        },
    });
    spec.as_object_mut()
        .unwrap()
        .extend(spec_overrides.as_object().unwrap().clone());
    let mut backup = KafkaBackup::new("incremental-hourly", serde_json::from_value(spec).unwrap());
    backup.metadata.namespace = Some("kafka-backup".to_string());
    backup
}

/// The core config of every run of an incremental backup names the same
/// backup set, and its offset database path is derived from that set.
#[tokio::test]
async fn incremental_runs_share_one_backup_set_in_the_core_config() {
    let backup = scheduled_backup(json!({
        "checkpoint": {"enabled": true, "intervalSecs": 15},
    }));
    let resolved = build_backup_config(&backup, &test_client(), "kafka-backup")
        .await
        .expect("resolved config");

    let first = Utc.with_ymd_and_hms(2026, 11, 2, 4, 0, 0).unwrap();
    let second = Utc.with_ymd_and_hms(2026, 11, 2, 5, 0, 0).unwrap();

    assert!(is_incremental(&backup));
    let first_id = backup_id_for(&backup, first);
    let second_id = backup_id_for(&backup, second);
    assert_eq!(first_id, "incremental-hourly");
    assert_eq!(first_id, second_id);

    let config = to_core_backup_config(&resolved, &second_id, None).expect("core config");
    assert_eq!(config.backup_id, "incremental-hourly");
    let offset_storage = config
        .offset_storage
        .expect("offset storage is always configured so the engine can checkpoint");
    assert_eq!(
        offset_storage
            .db_path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap(),
        "incremental-hourly-offsets.db"
    );
    assert!(
        !config.backup.unwrap().continuous,
        "incremental runs stay one-shot: the engine resumes from the stored checkpoint"
    );
}

/// Without checkpointing each run is a standalone full backup in its own
/// timestamped set, exactly as before.
#[tokio::test]
async fn full_scheduled_runs_keep_a_timestamped_backup_set_per_run() {
    let backup = scheduled_backup(json!({}));
    let resolved = build_backup_config(&backup, &test_client(), "kafka-backup")
        .await
        .expect("resolved config");

    let first = Utc.with_ymd_and_hms(2026, 11, 2, 4, 0, 0).unwrap();
    let second = Utc.with_ymd_and_hms(2026, 11, 2, 5, 0, 0).unwrap();

    assert!(!is_incremental(&backup));
    assert_eq!(
        backup_id_for(&backup, first),
        "incremental-hourly-20261102-040000"
    );
    assert_eq!(
        backup_id_for(&backup, second),
        "incremental-hourly-20261102-050000"
    );

    let config = to_core_backup_config(&resolved, &backup_id_for(&backup, second), None)
        .expect("core config");
    assert_eq!(config.backup_id, "incremental-hourly-20261102-050000");
}
