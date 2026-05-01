use std::collections::HashMap;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kafka_backup_operator::crd::{BackupRef, KafkaClusterSpec, KafkaRestore, KafkaRestoreSpec};
use kafka_backup_operator::reconcilers::restore;

fn restore_spec() -> KafkaRestoreSpec {
    KafkaRestoreSpec {
        backup_ref: BackupRef {
            name: "backup-issue48".to_string(),
            namespace: None,
            backup_id: Some("backup-issue48-20260501-120000".to_string()),
            storage: None,
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
    }
}

fn restore(spec: KafkaRestoreSpec) -> KafkaRestore {
    KafkaRestore {
        metadata: ObjectMeta {
            name: Some("restore-issue48".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        spec,
        status: None,
    }
}

#[test]
fn same_topic_restore_without_purge_is_rejected() {
    let result = restore::validate(&restore(restore_spec()));

    let error = result.expect_err("same-topic restore must require an explicit safety choice");
    assert!(error.to_string().contains("purgeTopics"));
}

#[test]
fn restore_all_without_purge_is_rejected() {
    let mut spec = restore_spec();
    spec.topics = vec![];

    let error = restore::validate(&restore(spec))
        .expect_err("restore-all defaults to original topic names");
    assert!(error.to_string().contains("spec.topics empty"));
}

#[test]
fn same_topic_restore_with_purge_is_allowed() {
    let mut spec = restore_spec();
    spec.purge_topics = true;

    restore::validate(&restore(spec)).expect("explicit purge makes same-topic restore intentional");
}

#[test]
fn mapped_topic_restore_without_purge_is_allowed() {
    let mut spec = restore_spec();
    spec.topic_mapping = HashMap::from([("orders".to_string(), "orders-restored".to_string())]);

    restore::validate(&restore(spec)).expect("mapped restore does not append to the source topic");
}

#[test]
fn partial_topic_mapping_without_purge_is_rejected() {
    let mut spec = restore_spec();
    spec.topics = vec!["orders".to_string(), "payments".to_string()];
    spec.topic_mapping = HashMap::from([("orders".to_string(), "orders-restored".to_string())]);

    let error = restore::validate(&restore(spec))
        .expect_err("unmapped explicit topics still restore into the same topic name");
    assert!(error.to_string().contains("payments"));
}
