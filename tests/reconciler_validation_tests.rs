//! Integration tests for reconciler validation logic
//!
//! These tests verify that the validation functions for each CRD type
//! correctly accept valid specs and reject invalid ones.

use std::collections::HashMap;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kafka_backup_operator::crd::{
    BackupRef, BackupValidationRef, CaSecretRef, ConsumerGroupCheckSpec, EvidenceSpec, KafkaBackup,
    KafkaBackupSpec, KafkaBackupValidation, KafkaBackupValidationSpec, KafkaClusterSpec,
    KafkaOffsetReset, KafkaOffsetResetSpec, KafkaRestore, KafkaRestoreSpec, MessageCountCheckSpec,
    OffsetMappingRef, OffsetRangeCheckSpec, OffsetResetStrategy, PitrSpec, PvcStorageSpec,
    SigningKeyRef, SigningSpec, StorageSpec, TlsSecretRef, TopicRepartitioningSpec,
    ValidationChecksSpec, WebhookCheckSpec,
};
use kafka_backup_operator::reconcilers::{backup, offset_reset, restore, validation};

// ============================================================================
// Test Helpers
// ============================================================================

fn valid_kafka_cluster() -> KafkaClusterSpec {
    KafkaClusterSpec {
        bootstrap_servers: vec!["kafka:9092".to_string()],
        security_protocol: "PLAINTEXT".to_string(),
        tls_secret: None,
        ca_secret: None,
        sasl_secret: None,
        connection: None,
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
        segment_max_bytes: 128 * 1024 * 1024,
        segment_max_interval_ms: 60_000,
        continuous: false,
        stop_at_current_offsets: false,
        include_offset_headers: true,
        source_cluster_id: None,
        poll_interval_ms: 100,
        consumer_group_snapshot: false,
        // cron crate uses 7-field format: sec min hour day_of_month month day_of_week year
        schedule: Some("0 0 0 * * * *".to_string()),
        checkpoint: None,
        rate_limiting: None,
        circuit_breaker: None,
        suspend: false,
        metrics: None,
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

#[test]
fn backup_continuous_with_snapshot_mode_fails_validation() {
    let mut spec = valid_backup_spec();
    spec.continuous = true;
    spec.stop_at_current_offsets = true;

    let backup = create_backup(spec);
    let result = backup::validate(&backup);

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("continuous"));
}

// ============================================================================
// TLS Secret Validation Tests
// ============================================================================

#[test]
fn backup_ssl_without_any_tls_config_fails_validation() {
    let mut spec = valid_backup_spec();
    spec.kafka_cluster.security_protocol = "SSL".to_string();
    spec.kafka_cluster.tls_secret = None;
    spec.kafka_cluster.ca_secret = None;

    let backup = create_backup(spec);
    let result = backup::validate(&backup);

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("tlsSecret or caSecret"));
}

#[test]
fn backup_sasl_ssl_without_any_tls_config_fails_validation() {
    let mut spec = valid_backup_spec();
    spec.kafka_cluster.security_protocol = "SASL_SSL".to_string();
    spec.kafka_cluster.tls_secret = None;
    spec.kafka_cluster.ca_secret = None;

    let backup = create_backup(spec);
    let result = backup::validate(&backup);

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("tlsSecret or caSecret"));
}

#[test]
fn backup_ssl_with_tls_secret_only_passes_validation() {
    let mut spec = valid_backup_spec();
    spec.kafka_cluster.security_protocol = "SSL".to_string();
    spec.kafka_cluster.tls_secret = Some(TlsSecretRef {
        name: "kafka-tls".to_string(),
        ca_key: "ca.crt".to_string(),
        cert_key: Some("tls.crt".to_string()),
        key_key: Some("tls.key".to_string()),
    });

    let backup = create_backup(spec);
    assert!(backup::validate(&backup).is_ok());
}

#[test]
fn backup_ssl_with_ca_secret_only_passes_validation() {
    let mut spec = valid_backup_spec();
    spec.kafka_cluster.security_protocol = "SSL".to_string();
    spec.kafka_cluster.ca_secret = Some(CaSecretRef {
        name: "cluster-ca-cert".to_string(),
        ca_key: "ca.crt".to_string(),
    });

    let backup = create_backup(spec);
    assert!(backup::validate(&backup).is_ok());
}

#[test]
fn backup_ssl_with_both_secrets_passes_validation() {
    let mut spec = valid_backup_spec();
    spec.kafka_cluster.security_protocol = "SSL".to_string();
    spec.kafka_cluster.ca_secret = Some(CaSecretRef {
        name: "cluster-ca-cert".to_string(),
        ca_key: "ca.crt".to_string(),
    });
    spec.kafka_cluster.tls_secret = Some(TlsSecretRef {
        name: "my-kafka-user".to_string(),
        ca_key: "ca.crt".to_string(),
        cert_key: Some("user.crt".to_string()),
        key_key: Some("user.key".to_string()),
    });

    let backup = create_backup(spec);
    assert!(backup::validate(&backup).is_ok());
}

#[test]
fn backup_sasl_ssl_with_ca_secret_only_passes_validation() {
    let mut spec = valid_backup_spec();
    spec.kafka_cluster.security_protocol = "SASL_SSL".to_string();
    spec.kafka_cluster.ca_secret = Some(CaSecretRef {
        name: "cluster-ca-cert".to_string(),
        ca_key: "ca.crt".to_string(),
    });

    let backup = create_backup(spec);
    assert!(backup::validate(&backup).is_ok());
}

#[test]
fn backup_plaintext_with_ca_secret_passes_validation() {
    let mut spec = valid_backup_spec();
    spec.kafka_cluster.security_protocol = "PLAINTEXT".to_string();
    spec.kafka_cluster.ca_secret = Some(CaSecretRef {
        name: "cluster-ca-cert".to_string(),
        ca_key: "ca.crt".to_string(),
    });

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
        repartitioning: HashMap::new(),
        offset_reset: None,
        rollback: None,
        rate_limiting: None,
        circuit_breaker: None,
        dry_run: false,
        produce_batch_size: 1000,
        produce_acks: -1,
        produce_timeout_ms: 30_000,
        purge_topics: false,
        auto_consumer_groups: false,
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
fn restore_invalid_produce_acks_fails_validation() {
    let mut spec = valid_restore_spec();
    spec.produce_acks = 2;

    let restore = create_restore(spec);
    let result = restore::validate(&restore);

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("produceAcks"));
}

#[test]
fn restore_valid_repartitioning_passes_validation() {
    let mut spec = valid_restore_spec();
    spec.repartitioning.insert(
        "orders-restored".to_string(),
        TopicRepartitioningSpec {
            strategy: "murmur2".to_string(),
            target_partitions: 6,
        },
    );

    let restore = create_restore(spec);
    assert!(restore::validate(&restore).is_ok());
}

#[test]
fn restore_invalid_repartitioning_strategy_fails_validation() {
    let mut spec = valid_restore_spec();
    spec.repartitioning.insert(
        "orders-restored".to_string(),
        TopicRepartitioningSpec {
            strategy: "random".to_string(),
            target_partitions: 6,
        },
    );

    let restore = create_restore(spec);
    let result = restore::validate(&restore);

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("repartitioning strategy"));
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

// ============================================================================
// KafkaBackupValidation Tests
// ============================================================================

fn valid_validation_checks() -> ValidationChecksSpec {
    ValidationChecksSpec {
        message_count: Some(MessageCountCheckSpec {
            enabled: true,
            fail_threshold: None,
            topics: vec![],
        }),
        offset_range: None,
        consumer_group_offsets: None,
        custom_webhooks: vec![],
    }
}

fn valid_validation_spec() -> KafkaBackupValidationSpec {
    KafkaBackupValidationSpec {
        backup_ref: BackupValidationRef {
            name: "my-backup".to_string(),
            namespace: None,
            backup_id: None,
            storage: None,
        },
        kafka_cluster: None,
        checks: valid_validation_checks(),
        evidence: None,
        notifications: None,
        schedule: None,
        suspend: false,
    }
}

fn create_validation(spec: KafkaBackupValidationSpec) -> KafkaBackupValidation {
    KafkaBackupValidation {
        metadata: default_metadata("test-validation"),
        spec,
        status: None,
    }
}

#[test]
fn validation_valid_spec_passes() {
    let v = create_validation(valid_validation_spec());
    assert!(validation::validate(&v).is_ok());
}

#[test]
fn validation_no_backup_ref_fails() {
    let mut spec = valid_validation_spec();
    spec.backup_ref.name = String::new();
    let v = create_validation(spec);
    assert!(validation::validate(&v).is_err());
}

#[test]
fn validation_no_checks_enabled_fails() {
    let mut spec = valid_validation_spec();
    spec.checks = ValidationChecksSpec {
        message_count: Some(MessageCountCheckSpec {
            enabled: false,
            fail_threshold: None,
            topics: vec![],
        }),
        offset_range: None,
        consumer_group_offsets: None,
        custom_webhooks: vec![],
    };
    let v = create_validation(spec);
    let result = validation::validate(&v);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("At least one"));
}

#[test]
fn validation_consumer_group_check_without_kafka_cluster_fails() {
    let mut spec = valid_validation_spec();
    spec.checks.consumer_group_offsets = Some(ConsumerGroupCheckSpec {
        enabled: true,
        consumer_groups: vec!["my-group".to_string()],
    });
    spec.kafka_cluster = None;
    let v = create_validation(spec);
    let result = validation::validate(&v);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("kafkaCluster"));
}

#[test]
fn validation_consumer_group_check_with_kafka_cluster_passes() {
    let mut spec = valid_validation_spec();
    spec.checks.consumer_group_offsets = Some(ConsumerGroupCheckSpec {
        enabled: true,
        consumer_groups: vec!["my-group".to_string()],
    });
    spec.kafka_cluster = Some(valid_kafka_cluster());
    let v = create_validation(spec);
    assert!(validation::validate(&v).is_ok());
}

#[test]
fn validation_webhook_without_url_fails() {
    let mut spec = valid_validation_spec();
    spec.checks.custom_webhooks = vec![WebhookCheckSpec {
        name: "empty-webhook".to_string(),
        url: String::new(),
        timeout_secs: 30,
        expected_status_code: 200,
    }];
    let v = create_validation(spec);
    let result = validation::validate(&v);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("URL"));
}

#[test]
fn validation_webhook_with_url_passes() {
    let mut spec = valid_validation_spec();
    spec.checks.custom_webhooks = vec![WebhookCheckSpec {
        name: "my-webhook".to_string(),
        url: "https://example.com/validate".to_string(),
        timeout_secs: 30,
        expected_status_code: 200,
    }];
    spec.checks.message_count = None;
    let v = create_validation(spec);
    assert!(validation::validate(&v).is_ok());
}

#[test]
fn validation_invalid_schedule_fails() {
    let mut spec = valid_validation_spec();
    spec.schedule = Some("not a valid cron".to_string());
    let v = create_validation(spec);
    let result = validation::validate(&v);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("cron"));
}

#[test]
fn validation_valid_schedule_passes() {
    let mut spec = valid_validation_spec();
    spec.schedule = Some("0 0 */6 * * *".to_string());
    let v = create_validation(spec);
    assert!(validation::validate(&v).is_ok());
}

#[test]
fn validation_evidence_signing_without_key_fails() {
    let mut spec = valid_validation_spec();
    spec.evidence = Some(EvidenceSpec {
        formats: vec!["json".to_string()],
        signing: Some(SigningSpec {
            enabled: true,
            key_secret: SigningKeyRef {
                name: String::new(),
                private_key_key: "key.pem".to_string(),
                public_key_key: "pub.pem".to_string(),
            },
        }),
        storage: None,
        retention_days: 90,
    });
    let v = create_validation(spec);
    let result = validation::validate(&v);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("signing"));
}

#[test]
fn validation_suspend_passes() {
    let mut spec = valid_validation_spec();
    spec.suspend = true;
    let v = create_validation(spec);
    assert!(validation::validate(&v).is_ok());
}

#[test]
fn validation_direct_storage_ref_passes() {
    let mut spec = valid_validation_spec();
    spec.backup_ref.name = String::new();
    spec.backup_ref.storage = Some(valid_pvc_storage());
    let v = create_validation(spec);
    assert!(validation::validate(&v).is_ok());
}

#[test]
fn validation_offset_range_only_passes() {
    let mut spec = valid_validation_spec();
    spec.checks.message_count = None;
    spec.checks.offset_range = Some(OffsetRangeCheckSpec { enabled: true });
    let v = create_validation(spec);
    assert!(validation::validate(&v).is_ok());
}

#[test]
fn validation_all_checks_enabled_passes() {
    let mut spec = valid_validation_spec();
    spec.checks.message_count = Some(MessageCountCheckSpec {
        enabled: true,
        fail_threshold: Some(10),
        topics: vec!["topic-a".to_string()],
    });
    spec.checks.offset_range = Some(OffsetRangeCheckSpec { enabled: true });
    spec.checks.consumer_group_offsets = Some(ConsumerGroupCheckSpec {
        enabled: true,
        consumer_groups: vec!["group-1".to_string()],
    });
    spec.checks.custom_webhooks = vec![WebhookCheckSpec {
        name: "my-hook".to_string(),
        url: "https://example.com".to_string(),
        timeout_secs: 120,
        expected_status_code: 200,
    }];
    spec.kafka_cluster = Some(valid_kafka_cluster());
    let v = create_validation(spec);
    assert!(validation::validate(&v).is_ok());
}
