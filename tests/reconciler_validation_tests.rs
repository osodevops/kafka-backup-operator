//! Integration tests for reconciler validation logic
//!
//! These tests verify that the validation functions for each CRD type
//! correctly accept valid specs and reject invalid ones.

use std::collections::HashMap;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kafka_backup_operator::crd::{
    BackupRef, KafkaBackup, KafkaBackupSpec, KafkaClusterSpec, KafkaOffsetReset,
    KafkaOffsetResetSpec, KafkaRestore, KafkaRestoreSpec, OffsetMappingRef, OffsetResetStrategy,
    PitrSpec, PvcStorageSpec, StorageSpec,
};
use kafka_backup_operator::reconcilers::{backup, offset_reset, restore};

// ============================================================================
// Test Helpers
// ============================================================================

fn valid_kafka_cluster() -> KafkaClusterSpec {
    KafkaClusterSpec {
        bootstrap_servers: vec!["kafka:9092".to_string()],
        security_protocol: "PLAINTEXT".to_string(),
        tls_secret: None,
        sasl_secret: None,
    }
}

fn valid_pvc_storage() -> StorageSpec {
    StorageSpec {
        storage_type: "pvc".to_string(),
        pvc: Some(PvcStorageSpec {
            claim_name: "backup-pvc".to_string(),
            sub_path: None,
            create: None,
        }),
        s3: None,
        azure: None,
        gcs: None,
    }
}

fn default_metadata(name: &str) -> ObjectMeta {
    ObjectMeta {
        name: Some(name.to_string()),
        namespace: Some("default".to_string()),
        ..Default::default()
    }
}

// ============================================================================
// Backup Validation Tests
// ============================================================================

fn valid_backup_spec() -> KafkaBackupSpec {
    KafkaBackupSpec {
        kafka_cluster: valid_kafka_cluster(),
        topics: vec!["test-topic".to_string()],
        storage: valid_pvc_storage(),
        compression: "zstd".to_string(),
        compression_level: 3,
        // cron crate uses 7-field format: sec min hour day_of_month month day_of_week year
        schedule: Some("0 0 0 * * * *".to_string()),
        checkpoint: None,
        rate_limiting: None,
        circuit_breaker: None,
        suspend: false,
    }
}

fn create_backup(spec: KafkaBackupSpec) -> KafkaBackup {
    KafkaBackup {
        metadata: default_metadata("test-backup"),
        spec,
        status: None,
    }
}

#[test]
fn backup_valid_spec_passes_validation() {
    let backup = create_backup(valid_backup_spec());
    let result = backup::validate(&backup);
    if let Err(e) = &result {
        panic!("Validation failed unexpectedly: {:?}", e);
    }
    assert!(result.is_ok());
}

#[test]
fn backup_empty_topics_fails_validation() {
    let mut spec = valid_backup_spec();
    spec.topics = vec![];

    let backup = create_backup(spec);
    let result = backup::validate(&backup);

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("topic"));
}

#[test]
fn backup_empty_bootstrap_servers_fails_validation() {
    let mut spec = valid_backup_spec();
    spec.kafka_cluster.bootstrap_servers = vec![];

    let backup = create_backup(spec);
    let result = backup::validate(&backup);

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("bootstrap server"));
}

#[test]
fn backup_invalid_storage_type_fails_validation() {
    let mut spec = valid_backup_spec();
    spec.storage.storage_type = "invalid".to_string();

    let backup = create_backup(spec);
    let result = backup::validate(&backup);

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("storage type"));
}

#[test]
fn backup_pvc_storage_without_config_fails_validation() {
    let mut spec = valid_backup_spec();
    spec.storage.storage_type = "pvc".to_string();
    spec.storage.pvc = None;

    let backup = create_backup(spec);
    let result = backup::validate(&backup);

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .to_lowercase()
        .contains("pvc"));
}

#[test]
fn backup_s3_storage_without_config_fails_validation() {
    let mut spec = valid_backup_spec();
    spec.storage.storage_type = "s3".to_string();
    spec.storage.pvc = None;
    spec.storage.s3 = None;

    let backup = create_backup(spec);
    let result = backup::validate(&backup);

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .to_lowercase()
        .contains("s3"));
}

#[test]
fn backup_invalid_cron_schedule_fails_validation() {
    let mut spec = valid_backup_spec();
    spec.schedule = Some("not-a-cron".to_string());

    let backup = create_backup(spec);
    let result = backup::validate(&backup);

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string().to_lowercase();
    assert!(err_msg.contains("cron") || err_msg.contains("schedule"));
}

#[test]
fn backup_valid_cron_schedules_pass_validation() {
    // cron crate uses 7-field format: sec min hour day_of_month month day_of_week year
    // Day of week: SUN=1, MON=2, ..., SAT=7 (or use names)
    let valid_schedules = vec![
        "0 0 0 * * * *",   // Every day at midnight
        "0 */5 * * * * *", // Every 5 minutes
        "0 0 */2 * * * *", // Every 2 hours
        "0 0 0 * * SUN *", // Every Sunday (using name)
    ];

    for schedule in valid_schedules {
        let mut spec = valid_backup_spec();
        spec.schedule = Some(schedule.to_string());

        let backup = create_backup(spec);
        let result = backup::validate(&backup);
        assert!(
            result.is_ok(),
            "Schedule '{}' should be valid, got error: {:?}",
            schedule,
            result.err()
        );
    }
}

#[test]
fn backup_invalid_compression_fails_validation() {
    let mut spec = valid_backup_spec();
    spec.compression = "gzip".to_string(); // Not supported

    let backup = create_backup(spec);
    let result = backup::validate(&backup);

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .to_lowercase()
        .contains("compression"));
}

#[test]
fn backup_valid_compressions_pass_validation() {
    let valid_compressions = vec!["none", "lz4", "zstd"];

    for compression in valid_compressions {
        let mut spec = valid_backup_spec();
        spec.compression = compression.to_string();

        let backup = create_backup(spec);
        assert!(
            backup::validate(&backup).is_ok(),
            "Compression '{}' should be valid",
            compression
        );
    }
}

#[test]
fn backup_invalid_zstd_compression_level_fails_validation() {
    let invalid_levels = vec![0, 23, -1, 100];

    for level in invalid_levels {
        let mut spec = valid_backup_spec();
        spec.compression = "zstd".to_string();
        spec.compression_level = level;

        let backup = create_backup(spec);
        let result = backup::validate(&backup);

        assert!(result.is_err(), "Level {} should fail validation", level);
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("compression level"));
    }
}

#[test]
fn backup_valid_zstd_compression_levels_pass_validation() {
    for level in 1..=22 {
        let mut spec = valid_backup_spec();
        spec.compression = "zstd".to_string();
        spec.compression_level = level;

        let backup = create_backup(spec);
        assert!(
            backup::validate(&backup).is_ok(),
            "Level {} should be valid",
            level
        );
    }
}

#[test]
fn backup_no_schedule_passes_validation() {
    let mut spec = valid_backup_spec();
    spec.schedule = None; // One-shot backup

    let backup = create_backup(spec);
    assert!(backup::validate(&backup).is_ok());
}

// ============================================================================
// Restore Validation Tests
// ============================================================================

fn valid_restore_spec() -> KafkaRestoreSpec {
    KafkaRestoreSpec {
        backup_ref: BackupRef {
            name: "my-backup".to_string(),
            namespace: None,
            backup_id: None,
            storage: None,
        },
        kafka_cluster: valid_kafka_cluster(),
        topics: vec![],
        pitr: None,
        topic_mapping: HashMap::new(),
        partition_mapping: HashMap::new(),
        offset_reset: None,
        rollback: None,
        rate_limiting: None,
        circuit_breaker: None,
        dry_run: false,
        create_topics: false,
        default_replication_factor: None,
    }
}

fn create_restore(spec: KafkaRestoreSpec) -> KafkaRestore {
    KafkaRestore {
        metadata: default_metadata("test-restore"),
        spec,
        status: None,
    }
}

#[test]
fn restore_valid_spec_passes_validation() {
    let restore = create_restore(valid_restore_spec());
    assert!(restore::validate(&restore).is_ok());
}

#[test]
fn restore_empty_backup_ref_name_and_no_storage_fails_validation() {
    let mut spec = valid_restore_spec();
    spec.backup_ref.name = String::new();
    spec.backup_ref.storage = None;

    let restore = create_restore(spec);
    let result = restore::validate(&restore);

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string().to_lowercase();
    assert!(err_msg.contains("backup") || err_msg.contains("storage"));
}

#[test]
fn restore_empty_bootstrap_servers_fails_validation() {
    let mut spec = valid_restore_spec();
    spec.kafka_cluster.bootstrap_servers = vec![];

    let restore = create_restore(spec);
    let result = restore::validate(&restore);

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("bootstrap server"));
}

#[test]
fn restore_pitr_start_after_end_fails_validation() {
    let mut spec = valid_restore_spec();
    spec.pitr = Some(PitrSpec {
        start_timestamp: Some(2000), // After end
        end_timestamp: Some(1000),
        start_time: None,
        end_time: None,
    });

    let restore = create_restore(spec);
    let result = restore::validate(&restore);

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string().to_lowercase();
    assert!(err_msg.contains("timestamp") || err_msg.contains("before"));
}

#[test]
fn restore_pitr_valid_timestamps_passes_validation() {
    let mut spec = valid_restore_spec();
    spec.pitr = Some(PitrSpec {
        start_timestamp: Some(1000),
        end_timestamp: Some(2000),
        start_time: None,
        end_time: None,
    });

    let restore = create_restore(spec);
    assert!(restore::validate(&restore).is_ok());
}

#[test]
fn restore_pitr_with_only_start_timestamp_passes_validation() {
    let mut spec = valid_restore_spec();
    spec.pitr = Some(PitrSpec {
        start_timestamp: Some(1000),
        end_timestamp: None,
        start_time: None,
        end_time: None,
    });

    let restore = create_restore(spec);
    assert!(restore::validate(&restore).is_ok());
}

#[test]
fn restore_pitr_with_only_end_timestamp_passes_validation() {
    let mut spec = valid_restore_spec();
    spec.pitr = Some(PitrSpec {
        start_timestamp: None,
        end_timestamp: Some(2000),
        start_time: None,
        end_time: None,
    });

    let restore = create_restore(spec);
    assert!(restore::validate(&restore).is_ok());
}

#[test]
fn restore_empty_topics_allowed_for_restore_all() {
    let mut spec = valid_restore_spec();
    spec.topics = vec![]; // Means "restore all"

    let restore = create_restore(spec);
    assert!(restore::validate(&restore).is_ok());
}

#[test]
fn restore_specific_topics_passes_validation() {
    let mut spec = valid_restore_spec();
    spec.topics = vec!["topic-a".to_string(), "topic-b".to_string()];

    let restore = create_restore(spec);
    assert!(restore::validate(&restore).is_ok());
}

#[test]
fn restore_topic_mapping_passes_validation() {
    let mut spec = valid_restore_spec();
    spec.topic_mapping = HashMap::from([("source-topic".to_string(), "target-topic".to_string())]);

    let restore = create_restore(spec);
    assert!(restore::validate(&restore).is_ok());
}

#[test]
fn restore_dry_run_mode_passes_validation() {
    let mut spec = valid_restore_spec();
    spec.dry_run = true;

    let restore = create_restore(spec);
    assert!(restore::validate(&restore).is_ok());
}

// ============================================================================
// Offset Reset Validation Tests
// ============================================================================

fn valid_offset_reset_spec() -> KafkaOffsetResetSpec {
    KafkaOffsetResetSpec {
        kafka_cluster: valid_kafka_cluster(),
        consumer_groups: vec!["test-group".to_string()],
        reset_strategy: OffsetResetStrategy::ToEarliest,
        reset_timestamp: None,
        reset_offset: None,
        topics: vec![],
        parallelism: 50,
        dry_run: false,
        continue_on_error: false,
        offset_mapping_ref: None,
        snapshot_before_reset: true,
    }
}

fn create_offset_reset(spec: KafkaOffsetResetSpec) -> KafkaOffsetReset {
    KafkaOffsetReset {
        metadata: default_metadata("test-offset-reset"),
        spec,
        status: None,
    }
}

#[test]
fn offset_reset_valid_spec_passes_validation() {
    let reset = create_offset_reset(valid_offset_reset_spec());
    assert!(offset_reset::validate(&reset).is_ok());
}

#[test]
fn offset_reset_empty_bootstrap_servers_fails_validation() {
    let mut spec = valid_offset_reset_spec();
    spec.kafka_cluster.bootstrap_servers = vec![];

    let reset = create_offset_reset(spec);
    let result = offset_reset::validate(&reset);

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("bootstrap server"));
}

#[test]
fn offset_reset_empty_consumer_groups_fails_validation() {
    let mut spec = valid_offset_reset_spec();
    spec.consumer_groups = vec![];

    let reset = create_offset_reset(spec);
    let result = offset_reset::validate(&reset);

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("consumer group"));
}

#[test]
fn offset_reset_to_timestamp_without_timestamp_fails_validation() {
    let mut spec = valid_offset_reset_spec();
    spec.reset_strategy = OffsetResetStrategy::ToTimestamp;
    spec.reset_timestamp = None;

    let reset = create_offset_reset(spec);
    let result = offset_reset::validate(&reset);

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .to_lowercase()
        .contains("timestamp"));
}

#[test]
fn offset_reset_to_timestamp_with_timestamp_passes_validation() {
    let mut spec = valid_offset_reset_spec();
    spec.reset_strategy = OffsetResetStrategy::ToTimestamp;
    spec.reset_timestamp = Some(1699900000000); // Valid epoch ms

    let reset = create_offset_reset(spec);
    assert!(offset_reset::validate(&reset).is_ok());
}

#[test]
fn offset_reset_to_offset_without_offset_fails_validation() {
    let mut spec = valid_offset_reset_spec();
    spec.reset_strategy = OffsetResetStrategy::ToOffset;
    spec.reset_offset = None;

    let reset = create_offset_reset(spec);
    let result = offset_reset::validate(&reset);

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .to_lowercase()
        .contains("offset"));
}

#[test]
fn offset_reset_to_offset_with_offset_passes_validation() {
    let mut spec = valid_offset_reset_spec();
    spec.reset_strategy = OffsetResetStrategy::ToOffset;
    spec.reset_offset = Some(1000);

    let reset = create_offset_reset(spec);
    assert!(offset_reset::validate(&reset).is_ok());
}

#[test]
fn offset_reset_from_mapping_without_mapping_ref_fails_validation() {
    let mut spec = valid_offset_reset_spec();
    spec.reset_strategy = OffsetResetStrategy::FromMapping;
    spec.offset_mapping_ref = None;

    let reset = create_offset_reset(spec);
    let result = offset_reset::validate(&reset);

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .to_lowercase()
        .contains("mapping"));
}

#[test]
fn offset_reset_from_mapping_with_mapping_ref_passes_validation() {
    let mut spec = valid_offset_reset_spec();
    spec.reset_strategy = OffsetResetStrategy::FromMapping;
    spec.offset_mapping_ref = Some(OffsetMappingRef {
        restore_name: Some("my-restore".to_string()),
        path: None,
        pvc_name: None,
    });

    let reset = create_offset_reset(spec);
    assert!(offset_reset::validate(&reset).is_ok());
}

#[test]
fn offset_reset_to_earliest_strategy_passes_validation() {
    let mut spec = valid_offset_reset_spec();
    spec.reset_strategy = OffsetResetStrategy::ToEarliest;

    let reset = create_offset_reset(spec);
    assert!(offset_reset::validate(&reset).is_ok());
}

#[test]
fn offset_reset_to_latest_strategy_passes_validation() {
    let mut spec = valid_offset_reset_spec();
    spec.reset_strategy = OffsetResetStrategy::ToLatest;

    let reset = create_offset_reset(spec);
    assert!(offset_reset::validate(&reset).is_ok());
}

#[test]
fn offset_reset_zero_parallelism_fails_validation() {
    let mut spec = valid_offset_reset_spec();
    spec.parallelism = 0;

    let reset = create_offset_reset(spec);
    let result = offset_reset::validate(&reset);

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("parallelism"));
}

#[test]
fn offset_reset_valid_parallelism_passes_validation() {
    let valid_parallelisms = vec![1, 10, 50, 100, 1000];

    for parallelism in valid_parallelisms {
        let mut spec = valid_offset_reset_spec();
        spec.parallelism = parallelism;

        let reset = create_offset_reset(spec);
        assert!(
            offset_reset::validate(&reset).is_ok(),
            "Parallelism {} should be valid",
            parallelism
        );
    }
}

#[test]
fn offset_reset_multiple_consumer_groups_passes_validation() {
    let mut spec = valid_offset_reset_spec();
    spec.consumer_groups = vec![
        "group-1".to_string(),
        "group-2".to_string(),
        "group-3".to_string(),
    ];

    let reset = create_offset_reset(spec);
    assert!(offset_reset::validate(&reset).is_ok());
}

#[test]
fn offset_reset_dry_run_mode_passes_validation() {
    let mut spec = valid_offset_reset_spec();
    spec.dry_run = true;

    let reset = create_offset_reset(spec);
    assert!(offset_reset::validate(&reset).is_ok());
}

#[test]
fn offset_reset_continue_on_error_passes_validation() {
    let mut spec = valid_offset_reset_spec();
    spec.continue_on_error = true;

    let reset = create_offset_reset(spec);
    assert!(offset_reset::validate(&reset).is_ok());
}

#[test]
fn offset_reset_snapshot_disabled_passes_validation() {
    let mut spec = valid_offset_reset_spec();
    spec.snapshot_before_reset = false;

    let reset = create_offset_reset(spec);
    assert!(offset_reset::validate(&reset).is_ok());
}
