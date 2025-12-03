//! Restore configuration adapter
//!
//! Converts KafkaRestore CRD spec to kafka-backup-core configuration.

use std::collections::HashMap;

use kube::Client;

use crate::crd::{
    BackupRef, KafkaRestore, PitrSpec, RollbackSpec,
};
use crate::error::Result;

use super::backup_config::{
    build_kafka_config, ResolvedCircuitBreakerConfig, ResolvedKafkaConfig,
    ResolvedRateLimitingConfig,
};
use super::storage_config::{build_storage_config, ResolvedStorage};

/// Fully resolved restore configuration
#[derive(Debug, Clone)]
pub struct ResolvedRestoreConfig {
    /// Source backup location
    pub backup_source: ResolvedBackupSource,
    /// Target Kafka cluster
    pub kafka: ResolvedKafkaConfig,
    /// Topics to restore
    pub topics: Vec<String>,
    /// Topic remapping
    pub topic_mapping: HashMap<String, String>,
    /// Partition remapping
    pub partition_mapping: HashMap<i32, i32>,
    /// PITR configuration
    pub pitr: Option<ResolvedPitrConfig>,
    /// Rollback configuration
    pub rollback: Option<ResolvedRollbackConfig>,
    /// Rate limiting
    pub rate_limiting: Option<ResolvedRateLimitingConfig>,
    /// Circuit breaker
    pub circuit_breaker: Option<ResolvedCircuitBreakerConfig>,
    /// Dry run mode
    pub dry_run: bool,
}

/// Resolved backup source
#[derive(Debug, Clone)]
pub enum ResolvedBackupSource {
    /// Reference to a KafkaBackup resource
    BackupResource {
        name: String,
        namespace: String,
        backup_id: Option<String>,
    },
    /// Direct storage reference
    Storage(ResolvedStorage),
}

/// Resolved PITR configuration
#[derive(Debug, Clone)]
pub struct ResolvedPitrConfig {
    /// Start timestamp in epoch milliseconds
    pub start_timestamp_ms: Option<i64>,
    /// End timestamp in epoch milliseconds
    pub end_timestamp_ms: Option<i64>,
}

/// Resolved rollback configuration
#[derive(Debug, Clone)]
pub struct ResolvedRollbackConfig {
    pub snapshot_before_restore: bool,
    pub snapshot_retention_hours: u32,
    pub auto_rollback_on_failure: bool,
    pub snapshot_path: Option<String>,
}

/// Build fully resolved restore configuration from CRD
pub async fn build_restore_config(
    restore: &KafkaRestore,
    client: &Client,
    namespace: &str,
) -> Result<ResolvedRestoreConfig> {
    // Resolve backup source
    let backup_source = resolve_backup_ref(&restore.spec.backup_ref, client, namespace).await?;

    // Resolve Kafka configuration
    let kafka = build_kafka_config(&restore.spec.kafka_cluster, client, namespace).await?;

    // Build PITR config
    let pitr = restore.spec.pitr.as_ref().map(|p| build_pitr_config(p));

    // Build rollback config
    let rollback = restore
        .spec
        .rollback
        .as_ref()
        .map(|r| build_rollback_config(r));

    // Build rate limiting
    let rate_limiting = restore.spec.rate_limiting.as_ref().map(|r| {
        ResolvedRateLimitingConfig {
            records_per_sec: r.records_per_sec,
            bytes_per_sec: r.bytes_per_sec,
            max_concurrent_partitions: r.max_concurrent_partitions,
        }
    });

    // Build circuit breaker
    let circuit_breaker = restore.spec.circuit_breaker.as_ref().map(|c| {
        ResolvedCircuitBreakerConfig {
            enabled: c.enabled,
            failure_threshold: c.failure_threshold,
            reset_timeout_secs: c.reset_timeout_secs,
            success_threshold: c.success_threshold,
            operation_timeout_ms: c.operation_timeout_ms,
        }
    });

    Ok(ResolvedRestoreConfig {
        backup_source,
        kafka,
        topics: restore.spec.topics.clone(),
        topic_mapping: restore.spec.topic_mapping.clone(),
        partition_mapping: restore.spec.partition_mapping.clone(),
        pitr,
        rollback,
        rate_limiting,
        circuit_breaker,
        dry_run: restore.spec.dry_run,
    })
}

/// Resolve backup reference to a concrete source
async fn resolve_backup_ref(
    backup_ref: &BackupRef,
    client: &Client,
    namespace: &str,
) -> Result<ResolvedBackupSource> {
    // If direct storage is specified, use that
    if let Some(storage) = &backup_ref.storage {
        let resolved = build_storage_config(storage, client, namespace).await?;
        return Ok(ResolvedBackupSource::Storage(resolved));
    }

    // Otherwise, reference the KafkaBackup resource
    let backup_namespace = backup_ref.namespace.clone().unwrap_or_else(|| namespace.to_string());

    Ok(ResolvedBackupSource::BackupResource {
        name: backup_ref.name.clone(),
        namespace: backup_namespace,
        backup_id: backup_ref.backup_id.clone(),
    })
}

fn build_pitr_config(pitr: &PitrSpec) -> ResolvedPitrConfig {
    // Prefer explicit timestamps, fall back to DateTime conversion
    let start_timestamp_ms = pitr.start_timestamp.or_else(|| {
        pitr.start_time.map(|dt| dt.timestamp_millis())
    });

    let end_timestamp_ms = pitr.end_timestamp.or_else(|| {
        pitr.end_time.map(|dt| dt.timestamp_millis())
    });

    ResolvedPitrConfig {
        start_timestamp_ms,
        end_timestamp_ms,
    }
}

fn build_rollback_config(rollback: &RollbackSpec) -> ResolvedRollbackConfig {
    let snapshot_path = rollback.snapshot_storage.as_ref().map(|s| {
        let base = format!("/snapshots/{}", s.pvc_name);
        match &s.sub_path {
            Some(sub) => format!("{}/{}", base, sub),
            None => base,
        }
    });

    ResolvedRollbackConfig {
        snapshot_before_restore: rollback.snapshot_before_restore,
        snapshot_retention_hours: rollback.snapshot_retention_hours,
        auto_rollback_on_failure: rollback.auto_rollback_on_failure,
        snapshot_path,
    }
}
