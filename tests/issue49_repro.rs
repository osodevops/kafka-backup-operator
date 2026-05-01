use std::collections::HashMap;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kafka_backup_core::config::OffsetStrategy;
use kafka_backup_operator::adapters::{
    build_restore_config, to_core_restore_config, ResolvedBackupSource,
};
use kafka_backup_operator::crd::{
    BackupRef, KafkaClusterSpec, KafkaRestore, KafkaRestoreSpec, OffsetResetSpec, PvcStorageSpec,
    StorageSpec,
};

fn test_client() -> kube::Client {
    kube::Client::try_from(kube::Config::new(
        "http://127.0.0.1".parse().expect("valid URL"),
    ))
    .expect("client can be built without contacting a cluster")
}

fn storage_ref() -> StorageSpec {
    StorageSpec {
        storage_type: "pvc".to_string(),
        pvc: Some(PvcStorageSpec {
            claim_name: "backup-pvc".to_string(),
            sub_path: Some("backups".to_string()),
            create: None,
        }),
        s3: None,
        azure: None,
        gcs: None,
    }
}

fn restore_with_storage_ref() -> KafkaRestore {
    KafkaRestore {
        metadata: ObjectMeta {
            name: Some("restore-issue49".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        spec: KafkaRestoreSpec {
            backup_ref: BackupRef {
                name: String::new(),
                namespace: None,
                backup_id: Some("backup-issue49".to_string()),
                storage: Some(storage_ref()),
            },
            kafka_cluster: KafkaClusterSpec {
                bootstrap_servers: vec!["kafka:9092".to_string()],
                security_protocol: "PLAINTEXT".to_string(),
                tls_secret: None,
                ca_secret: None,
                sasl_secret: None,
                connection: None,
            },
            topics: vec!["orders".to_string()],
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
        },
        status: None,
    }
}

#[tokio::test]
async fn restore_offset_reset_auto_is_passed_to_core_config() {
    let mut restore = restore_with_storage_ref();
    restore.spec.offset_reset = Some(OffsetResetSpec {
        enabled: true,
        consumer_groups: vec!["test.consumergroup.v1".to_string()],
        strategy: "auto".to_string(),
    });

    let resolved = build_restore_config(&restore, &test_client(), "default")
        .await
        .expect("restore config resolves locally");
    let storage = match &resolved.backup_source {
        ResolvedBackupSource::Storage { storage, .. } => storage,
        ResolvedBackupSource::BackupResource { .. } => panic!("expected direct storage ref"),
    };
    let core = to_core_restore_config(&resolved, "backup-issue49", storage, None)
        .expect("core config builds");
    let options = core.restore.expect("restore options");

    assert!(options.reset_consumer_offsets);
    assert_eq!(
        options.consumer_groups,
        vec!["test.consumergroup.v1".to_string()]
    );
    assert_eq!(options.consumer_group_strategy, OffsetStrategy::HeaderBased);
    assert!(options.offset_report.is_some());
}

#[tokio::test]
async fn restore_auto_consumer_groups_requests_mapping_report() {
    let mut restore = restore_with_storage_ref();
    restore.spec.auto_consumer_groups = true;

    let resolved = build_restore_config(&restore, &test_client(), "default")
        .await
        .expect("restore config resolves locally");
    let storage = match &resolved.backup_source {
        ResolvedBackupSource::Storage { storage, .. } => storage,
        ResolvedBackupSource::BackupResource { .. } => panic!("expected direct storage ref"),
    };
    let core = to_core_restore_config(&resolved, "backup-issue49", storage, None)
        .expect("core config builds");
    let options = core.restore.expect("restore options");

    assert!(options.auto_consumer_groups);
    assert_eq!(options.consumer_group_strategy, OffsetStrategy::HeaderBased);
    assert!(options.offset_report.is_some());
}
