//! `KafkaRestore.spec.includeOriginalOffsetHeader` / `stripOffsetHeaders`
//! (1.3.0) must reach `RestoreOptions` — previously the adapter hardcoded
//! `include_original_offset_header: true` and there was no way to strip the
//! headers kafka-backup adds at backup time (kafka-backup#154).

use std::collections::HashMap;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kafka_backup_operator::adapters::{
    build_restore_config, to_core_restore_config, ResolvedBackupSource,
};
use kafka_backup_operator::crd::{
    BackupRef, KafkaClusterSpec, KafkaRestore, KafkaRestoreSpec, PvcStorageSpec, StorageSpec,
};

fn test_client() -> kube::Client {
    kube::Client::try_from(kube::Config::new(
        "http://127.0.0.1".parse().expect("valid URL"),
    ))
    .expect("client can be built without contacting a cluster")
}

fn restore(spec_json: serde_json::Value) -> KafkaRestore {
    // Go through serde so the CRD defaults (not struct literals) are exercised.
    let mut base = serde_json::json!({
        "backupRef": {"name": "", "backupId": "backup-hdr", "storage": {
            "storageType": "pvc", "pvc": {"claimName": "backup-pvc"}
        }},
        "kafkaCluster": {"bootstrapServers": ["kafka:9092"]},
        "topics": ["orders"],
    });
    base.as_object_mut()
        .unwrap()
        .extend(spec_json.as_object().unwrap().clone());
    let spec: KafkaRestoreSpec = serde_json::from_value(base).expect("spec deserialises");
    KafkaRestore {
        metadata: ObjectMeta {
            name: Some("restore-hdr".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        spec,
        status: None,
    }
}

async fn core_options(restore: &KafkaRestore) -> kafka_backup_core::config::RestoreOptions {
    let resolved = build_restore_config(restore, &test_client(), "default")
        .await
        .expect("restore config resolves locally");
    let storage = match &resolved.backup_source {
        ResolvedBackupSource::Storage { storage, .. } => storage,
        ResolvedBackupSource::BackupResource { .. } => panic!("expected direct storage ref"),
    };
    to_core_restore_config(&resolved, "backup-hdr", storage, None)
        .expect("core config builds")
        .restore
        .expect("restore options")
}

#[test]
fn crd_defaults_keep_previous_behaviour() {
    let r = restore(serde_json::json!({}));
    assert!(
        r.spec.include_original_offset_header,
        "default true (unchanged behaviour)"
    );
    assert!(!r.spec.strip_offset_headers, "default false");
}

#[tokio::test]
async fn defaults_reach_core_options() {
    let options = core_options(&restore(serde_json::json!({}))).await;
    assert!(options.include_original_offset_header);
    assert!(!options.strip_offset_headers);
}

#[tokio::test]
async fn verbatim_restore_settings_reach_core_options() {
    let options = core_options(&restore(serde_json::json!({
        "includeOriginalOffsetHeader": false,
        "stripOffsetHeaders": true
    })))
    .await;
    assert!(!options.include_original_offset_header);
    assert!(options.strip_offset_headers);
}

#[test]
fn struct_literal_fields_are_present() {
    // Guards the field names used by other test fixtures.
    let spec = KafkaRestoreSpec {
        backup_ref: BackupRef {
            name: String::new(),
            namespace: None,
            backup_id: Some("b".to_string()),
            storage: Some(StorageSpec {
                storage_type: "pvc".to_string(),
                pvc: Some(PvcStorageSpec {
                    claim_name: "backup-pvc".to_string(),
                    sub_path: None,
                    create: None,
                }),
                s3: None,
                azure: None,
                gcs: None,
            }),
        },
        kafka_cluster: KafkaClusterSpec {
            bootstrap_servers: vec!["kafka:9092".to_string()],
            security_protocol: "PLAINTEXT".to_string(),
            tls_secret: None,
            ca_secret: None,
            sasl_secret: None,
            connection: None,
        },
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
        include_original_offset_header: false,
        strip_offset_headers: true,
    };
    assert!(spec.strip_offset_headers && !spec.include_original_offset_header);
}
