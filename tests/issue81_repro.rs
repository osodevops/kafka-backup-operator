//! Issue #81: `KafkaBackup.spec.rateLimiting.recordsPerSec` / `bytesPerSec`
//! had no effect on backups.
//!
//! `to_core_backup_config` mapped only `maxConcurrentPartitions`: core's
//! backup engine had no rate limit to map the other two to (only restores
//! had one). kafka-backup-core 0.27.0 adds `BackupOptions.rate_limit_*`
//! (one token bucket per backup, shared by every partition). Reproduced in
//! minikube on 1.6.3: 50 000 records with `recordsPerSec: 500` backed up in
//! about 3 s instead of about 100 s.

use kafka_backup_operator::adapters::{build_backup_config, to_core_backup_config};
use kafka_backup_operator::crd::KafkaBackup;
use serde_json::json;

fn test_client() -> kube::Client {
    kafka_backup_operator::install_crypto_provider();
    kube::Client::try_from(kube::Config::new(
        "http://127.0.0.1".parse().expect("valid URL"),
    ))
    .expect("client can be built without contacting a cluster")
}

fn backup(rate_limiting: Option<serde_json::Value>) -> KafkaBackup {
    let mut spec = json!({
        "kafkaCluster": {"bootstrapServers": ["kafka:9092"]},
        "topics": ["orders"],
        "storage": {"storageType": "pvc", "pvc": {"claimName": "backups"}},
    });
    if let Some(rl) = rate_limiting {
        spec["rateLimiting"] = rl;
    }
    let mut backup = KafkaBackup::new("rate-limited", serde_json::from_value(spec).unwrap());
    backup.metadata.namespace = Some("kbo".to_string());
    backup
}

async fn core_backup_options(backup: &KafkaBackup) -> kafka_backup_core::config::BackupOptions {
    let resolved = build_backup_config(backup, &test_client(), "kbo")
        .await
        .expect("resolved config");
    let config = to_core_backup_config(&resolved, "rate-limited-1", None).expect("core config");
    config.validate().expect("core accepts the config");
    config.backup.expect("backup options")
}

#[tokio::test]
async fn backup_rate_limits_reach_the_core_config() {
    let options = core_backup_options(&backup(Some(json!({
        "recordsPerSec": 500,
        "bytesPerSec": 1048576,
        "maxConcurrentPartitions": 3,
    }))))
    .await;

    assert_eq!(options.rate_limit_records_per_sec, Some(500));
    assert_eq!(options.rate_limit_bytes_per_sec, Some(1_048_576));
    assert_eq!(options.max_concurrent_partitions, 3);
}

/// 0 means unlimited in the CRD; core rejects 0 and means unlimited by
/// "unset".
#[tokio::test]
async fn zero_means_unlimited() {
    let options = core_backup_options(&backup(Some(json!({
        "recordsPerSec": 0,
        "bytesPerSec": 0,
    }))))
    .await;
    assert_eq!(options.rate_limit_records_per_sec, None);
    assert_eq!(options.rate_limit_bytes_per_sec, None);

    let only_bytes = core_backup_options(&backup(Some(json!({"bytesPerSec": 2048})))).await;
    assert_eq!(only_bytes.rate_limit_records_per_sec, None);
    assert_eq!(only_bytes.rate_limit_bytes_per_sec, Some(2048));
}

#[tokio::test]
async fn no_rate_limiting_block_means_unlimited() {
    let options = core_backup_options(&backup(None)).await;
    assert_eq!(options.rate_limit_records_per_sec, None);
    assert_eq!(options.rate_limit_bytes_per_sec, None);
}
