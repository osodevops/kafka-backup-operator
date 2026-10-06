//! Issue #84: building against kafka-backup-core 0.20–0.23.
//!
//! The core bump is a compile-time break (new struct-literal fields), but the
//! behavioural requirements are what these tests pin:
//!
//! - no core deprecation warning on every backup (`offset_storage.s3_key`,
//!   `backup.checkpoint_interval_secs` are ignored since core 0.22);
//! - the offset database keeps the location and sync cadence it had under
//!   0.19.2 (core always used `{backup_id}/offsets.db` and
//!   `backup.sync_interval_secs`; 0.22 starts honouring
//!   `offset_storage.sync_interval_secs`, so a hardcoded value would change it);
//! - `KafkaBackup.spec.onMissingTopic` reaches `BackupOptions::on_missing_topic`;
//! - the existing `spec.circuitBreaker` (a no-op before core 0.23) reaches
//!   `BackupOptions` / `RestoreOptions::circuit_breaker`;
//! - `KafkaRestore.spec.headerPreflight` reaches `RestoreOptions`, the escape
//!   hatch core's 0.20 preflight error points users at;
//! - a Kubernetes client can still be built: core 0.23 pulls in object_store
//!   0.14 -> reqwest 0.13, which compiles rustls' aws-lc-rs backend next to the
//!   ring backend kube and core use, so rustls can no longer pick a
//!   process-default provider and panics unless the operator installs one.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kafka_backup_core::config::{
    BackupOptions, CircuitBreakerSettings, Config, HeaderPreflightMode, OnMissingTopic,
};
use kafka_backup_operator::adapters::{
    build_backup_config, build_restore_config, to_core_backup_config, to_core_restore_config,
    AzureAuthMethod, AzureStorageConfig, GcsStorageConfig, ResolvedBackupConfig,
    ResolvedBackupSource, ResolvedStorage, S3StorageConfig,
};
use kafka_backup_operator::crd::{KafkaBackup, KafkaBackupSpec, KafkaRestore, KafkaRestoreSpec};
use serde_json::json;

const BACKUP_ID: &str = "backup-84";

fn test_client() -> kube::Client {
    kafka_backup_operator::install_crypto_provider();
    kube::Client::try_from(kube::Config::new(
        "http://127.0.0.1".parse().expect("valid URL"),
    ))
    .expect("client can be built without contacting a cluster")
}

#[tokio::test]
async fn kube_client_builds_once_the_crypto_provider_is_installed() {
    kafka_backup_operator::install_crypto_provider();
    // Idempotent: the operator and every test helper may call it.
    kafka_backup_operator::install_crypto_provider();
    assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    kube::Client::try_from(kube::Config::new(
        "https://127.0.0.1:6443".parse().expect("valid URL"),
    ))
    .expect("TLS client config builds");
}

fn merge(mut base: serde_json::Value, overrides: serde_json::Value) -> serde_json::Value {
    base.as_object_mut()
        .unwrap()
        .extend(overrides.as_object().unwrap().clone());
    base
}

fn backup_spec(overrides: serde_json::Value) -> serde_json::Result<KafkaBackupSpec> {
    // Go through serde so the CRD defaults (not struct literals) are exercised.
    serde_json::from_value(merge(
        json!({
            "kafkaCluster": {"bootstrapServers": ["kafka:9092"]},
            "topics": ["orders"],
            "storage": {"storageType": "pvc", "pvc": {"claimName": "backup-pvc"}},
        }),
        overrides,
    ))
}

async fn resolved_backup(overrides: serde_json::Value) -> ResolvedBackupConfig {
    let backup = KafkaBackup {
        metadata: ObjectMeta {
            name: Some("backup-84".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        spec: backup_spec(overrides).expect("spec deserialises"),
        status: None,
    };
    build_backup_config(&backup, &test_client(), "default")
        .await
        .expect("backup config resolves locally")
}

fn core_backup(resolved: &ResolvedBackupConfig) -> Config {
    to_core_backup_config(resolved, BACKUP_ID, None).expect("core config builds")
}

fn s3(prefix: Option<&str>) -> ResolvedStorage {
    ResolvedStorage::S3(S3StorageConfig {
        bucket: "backups".to_string(),
        region: "eu-west-2".to_string(),
        endpoint: Some("http://minio.minio:9000".to_string()),
        path_style: true,
        allow_http: true,
        prefix: prefix.map(str::to_string),
        access_key_id: "key".to_string(),
        secret_access_key: "secret".to_string(),
    })
}

/// Every storage backend the operator can hand to the backup engine.
async fn storages() -> Vec<(&'static str, ResolvedStorage)> {
    let pvc = resolved_backup(json!({})).await.storage;
    vec![
        ("pvc", pvc),
        ("s3 with prefix", s3(Some("prod/kafka"))),
        ("s3 without prefix", s3(None)),
        (
            "azure",
            ResolvedStorage::Azure(AzureStorageConfig {
                container: "backups".to_string(),
                account_name: "account".to_string(),
                auth: AzureAuthMethod::AccountKey("key".to_string()),
                prefix: Some("prod".to_string()),
                endpoint: None,
            }),
        ),
        (
            "gcs",
            ResolvedStorage::Gcs(GcsStorageConfig {
                bucket: "backups".to_string(),
                prefix: Some("prod".to_string()),
                service_account_json: "/tmp/sa.json".to_string(),
            }),
        ),
    ]
}

#[tokio::test]
async fn backup_config_triggers_no_core_deprecation_warning() {
    for checkpoint in [json!({}), json!({"checkpoint": {"intervalSecs": 45}})] {
        for (label, storage) in storages().await {
            let mut resolved = resolved_backup(checkpoint.clone()).await;
            resolved.storage = storage;
            let warnings = core_backup(&resolved).deprecation_warnings();
            assert!(
                warnings.is_empty(),
                "{label} with {checkpoint}: core would log on every backup: {warnings:?}"
            );
        }
    }
}

#[tokio::test]
async fn offset_database_location_and_sync_cadence_are_left_to_core() {
    for (label, storage) in storages().await {
        let mut resolved = resolved_backup(json!({})).await;
        resolved.storage = storage;
        let offset_storage = core_backup(&resolved)
            .offset_storage
            .expect("operator always configures the offset database");
        // Core stores it at `{backup_id}/offsets.db` under the storage prefix,
        // which is also where 0.19.2 put it — so resumable backups survive the upgrade.
        assert_eq!(offset_storage.s3_key, None, "{label}");
        // `None` keeps the 0.19.2 cadence: the offset database syncs on
        // `backup.sync_interval_secs`, which the CRD checkpoint interval drives.
        assert_eq!(offset_storage.sync_interval_secs, None, "{label}");
    }
}

#[tokio::test]
async fn checkpoint_interval_drives_sync_interval_not_the_deprecated_field() {
    let config = core_backup(
        &resolved_backup(json!({"checkpoint": {"enabled": true, "intervalSecs": 45}})).await,
    );
    let backup = config.backup.expect("backup options");
    assert_eq!(
        backup.checkpoint_interval_secs,
        BackupOptions::default().checkpoint_interval_secs,
        "checkpoint_interval_secs has no effect since core 0.22; leave it at the default"
    );
    assert_eq!(
        backup.sync_interval_secs, 90,
        "unchanged: 2x the checkpoint interval"
    );

    let defaults = core_backup(&resolved_backup(json!({})).await)
        .backup
        .expect("backup options");
    assert_eq!(defaults.sync_interval_secs, 30, "unchanged default");
}

#[tokio::test]
async fn on_missing_topic_defaults_to_fail() {
    let backup = core_backup(&resolved_backup(json!({})).await)
        .backup
        .expect("backup options");
    assert_eq!(backup.on_missing_topic, OnMissingTopic::Fail);
}

#[tokio::test]
async fn on_missing_topic_warn_reaches_core() {
    let backup = core_backup(&resolved_backup(json!({"onMissingTopic": "warn"})).await)
        .backup
        .expect("backup options");
    assert_eq!(backup.on_missing_topic, OnMissingTopic::Warn);
}

#[test]
fn on_missing_topic_rejects_unknown_values() {
    assert!(backup_spec(json!({"onMissingTopic": "ignore"})).is_err());
}

#[tokio::test]
async fn operator_retention_is_not_doubled_by_core_retention() {
    // The operator prunes whole backup sets itself (spec.retention); core 0.21's
    // in-backup segment pruning must stay off.
    let backup =
        core_backup(&resolved_backup(json!({"retention": {"enabled": true, "keepLast": 3}})).await)
            .backup
            .expect("backup options");
    assert!(backup.retention.is_none());
}

fn assert_breaker(actual: &CircuitBreakerSettings, expected: (bool, u32, u64, u32)) {
    assert_eq!(
        (
            actual.enabled,
            actual.failure_threshold,
            actual.reset_timeout_ms,
            actual.success_threshold
        ),
        expected
    );
}

#[tokio::test]
async fn backup_circuit_breaker_spec_reaches_core() {
    let backup = core_backup(
        &resolved_backup(json!({"circuitBreaker": {
            "enabled": false,
            "failureThreshold": 7,
            "resetTimeoutSecs": 12,
            "successThreshold": 4
        }}))
        .await,
    )
    .backup
    .expect("backup options");
    assert_breaker(&backup.circuit_breaker, (false, 7, 12_000, 4));
}

#[tokio::test]
async fn backup_without_circuit_breaker_spec_uses_core_defaults() {
    let backup = core_backup(&resolved_backup(json!({})).await)
        .backup
        .expect("backup options");
    let core = CircuitBreakerSettings::default();
    assert_breaker(
        &backup.circuit_breaker,
        (
            core.enabled,
            core.failure_threshold,
            core.reset_timeout_ms,
            core.success_threshold,
        ),
    );
}

fn restore_spec(overrides: serde_json::Value) -> serde_json::Result<KafkaRestoreSpec> {
    serde_json::from_value(merge(
        json!({
            "backupRef": {"name": "", "backupId": BACKUP_ID, "storage": {
                "storageType": "pvc", "pvc": {"claimName": "backup-pvc"}
            }},
            "kafkaCluster": {"bootstrapServers": ["kafka:9092"]},
            "topics": ["orders"],
        }),
        overrides,
    ))
}

async fn core_restore(overrides: serde_json::Value) -> kafka_backup_core::config::RestoreOptions {
    let restore = KafkaRestore {
        metadata: ObjectMeta {
            name: Some("restore-84".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        spec: restore_spec(overrides).expect("spec deserialises"),
        status: None,
    };
    let resolved = build_restore_config(&restore, &test_client(), "default")
        .await
        .expect("restore config resolves locally");
    let storage = match &resolved.backup_source {
        ResolvedBackupSource::Storage { storage, .. } => storage,
        ResolvedBackupSource::BackupResource { .. } => panic!("expected direct storage ref"),
    };
    to_core_restore_config(&resolved, BACKUP_ID, storage, None)
        .expect("core config builds")
        .restore
        .expect("restore options")
}

#[tokio::test]
async fn restore_circuit_breaker_spec_reaches_core() {
    let restore = core_restore(json!({"circuitBreaker": {
        "failureThreshold": 9,
        "resetTimeoutSecs": 5,
        "successThreshold": 1
    }}))
    .await;
    assert_breaker(&restore.circuit_breaker, (true, 9, 5_000, 1));
}

#[tokio::test]
async fn header_preflight_defaults_to_auto() {
    assert_eq!(
        core_restore(json!({})).await.header_preflight,
        HeaderPreflightMode::Auto
    );
}

#[tokio::test]
async fn header_preflight_skip_reaches_core() {
    for (value, expected) in [
        ("auto", HeaderPreflightMode::Auto),
        ("full", HeaderPreflightMode::Full),
        ("skip", HeaderPreflightMode::Skip),
    ] {
        assert_eq!(
            core_restore(json!({"headerPreflight": value}))
                .await
                .header_preflight,
            expected,
            "{value}"
        );
    }
}

#[test]
fn header_preflight_rejects_unknown_values() {
    assert!(restore_spec(json!({"headerPreflight": "off"})).is_err());
}
