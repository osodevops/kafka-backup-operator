//! KafkaBackup reconciler
//!
//! Handles the business logic for backup operations including:
//! - Spec validation
//! - Schedule checking
//! - Backup execution
//! - Status updates

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use cron::Schedule;
use kafka_backup_core::backup::BackupEngine;
use kube::{
    api::{Patch, PatchParams},
    runtime::controller::Action,
    Api, Client, ResourceExt,
};
use serde_json::json;
use std::str::FromStr;
use tracing::{debug, error, info};

use crate::adapters::{
    build_backup_config, default_tls_dir, to_core_backup_config, ResolvedStorage, TlsFileManager,
};
use crate::crd::KafkaBackup;
use crate::error::{Error, Result};
use crate::metrics;

/// Process-local guard recording the most recent wall-clock time at which
/// this operator fired a scheduled backup for each `{namespace}/{name}`.
///
/// Purpose: the `status.lastScheduleTime` field written by `execute_backup`
/// is the durable, cross-restart anchor, but it lives in the reflector cache
/// and is subject to watch-stream propagation lag (observed ~30-50ms in a
/// local minikube). For backups that complete faster than that window (e.g.
/// incremental / no-op snapshot passes), a second reconcile can fire before
/// the cache sees the tentative marker and re-enter `execute_backup`. This
/// in-memory map is consulted as a strictly-narrower filter on top of the
/// CRD-anchored decision in [`should_run_backup`] and closes that residual
/// race without adding a direct API round-trip to every reconcile.
fn scheduler_guard() -> &'static Mutex<HashMap<String, DateTime<Utc>>> {
    static GUARD: OnceLock<Mutex<HashMap<String, DateTime<Utc>>>> = OnceLock::new();
    GUARD.get_or_init(|| Mutex::new(HashMap::new()))
}

fn guard_key(namespace: &str, name: &str) -> String {
    format!("{}/{}", namespace, name)
}

/// Validate the KafkaBackup spec
pub fn validate(backup: &KafkaBackup) -> Result<()> {
    // Validate topics
    if backup.spec.topics.is_empty() {
        return Err(Error::validation("At least one topic must be specified"));
    }

    // Validate kafka cluster
    if backup.spec.kafka_cluster.bootstrap_servers.is_empty() {
        return Err(Error::validation(
            "At least one bootstrap server must be specified",
        ));
    }

    // Validate storage configuration
    validate_storage(&backup.spec.storage)?;

    // Validate schedule if provided
    if let Some(schedule) = &backup.spec.schedule {
        Schedule::from_str(schedule).map_err(|e| {
            Error::validation(format!("Invalid cron schedule '{}': {}", schedule, e))
        })?;
    }

    // Validate compression
    match backup.spec.compression.as_str() {
        "none" | "lz4" | "zstd" => {}
        other => {
            return Err(Error::validation(format!(
                "Invalid compression '{}': must be one of: none, lz4, zstd",
                other
            )));
        }
    }

    // Validate compression level for zstd
    if backup.spec.compression == "zstd"
        && (backup.spec.compression_level < 1 || backup.spec.compression_level > 22)
    {
        return Err(Error::validation(format!(
            "Invalid zstd compression level {}: must be between 1 and 22",
            backup.spec.compression_level
        )));
    }

    if backup.spec.segment_max_bytes == 0 {
        return Err(Error::validation("segmentMaxBytes must be greater than 0"));
    }

    if backup.spec.segment_max_interval_ms == 0 {
        return Err(Error::validation(
            "segmentMaxIntervalMs must be greater than 0",
        ));
    }

    if backup.spec.continuous && backup.spec.stop_at_current_offsets {
        return Err(Error::validation(
            "continuous and stopAtCurrentOffsets cannot both be true",
        ));
    }

    if let Some(rate_limiting) = &backup.spec.rate_limiting {
        if rate_limiting.max_concurrent_partitions == 0 {
            return Err(Error::validation(
                "rateLimiting.maxConcurrentPartitions must be greater than 0",
            ));
        }
    }

    // Validate TLS configuration: SSL/SASL_SSL requires at least one TLS secret
    let protocol = backup.spec.kafka_cluster.security_protocol.to_uppercase();
    if (protocol == "SSL" || protocol == "SASL_SSL")
        && backup.spec.kafka_cluster.tls_secret.is_none()
        && backup.spec.kafka_cluster.ca_secret.is_none()
    {
        return Err(Error::validation(
            "securityProtocol SSL/SASL_SSL requires either tlsSecret or caSecret to be configured",
        ));
    }

    if let Some(connection) = &backup.spec.kafka_cluster.connection {
        if connection.connections_per_broker == 0 {
            return Err(Error::validation(
                "kafkaCluster.connection.connectionsPerBroker must be greater than 0",
            ));
        }
    }

    Ok(())
}

/// Validate storage configuration
fn validate_storage(storage: &crate::crd::StorageSpec) -> Result<()> {
    match storage.storage_type.as_str() {
        "pvc" => {
            if storage.pvc.is_none() {
                return Err(Error::validation(
                    "PVC storage selected but pvc configuration is missing",
                ));
            }
        }
        "s3" => {
            if storage.s3.is_none() {
                return Err(Error::validation(
                    "S3 storage selected but s3 configuration is missing",
                ));
            }
        }
        "azure" => {
            let azure = storage.azure.as_ref().ok_or_else(|| {
                Error::validation("Azure storage selected but azure configuration is missing")
            })?;
            // The adapter supports workload identity, service principal, SAS token,
            // account key, and default credential fallback.
            if azure.credentials_secret.is_some()
                && (azure.sas_token_secret.is_some()
                    || azure.service_principal_secret.is_some()
                    || azure.use_workload_identity)
            {
                return Err(Error::validation(
                    "Azure storage authentication methods are mutually exclusive",
                ));
            }
            if azure.sas_token_secret.is_some()
                && (azure.service_principal_secret.is_some() || azure.use_workload_identity)
            {
                return Err(Error::validation(
                    "Azure storage authentication methods are mutually exclusive",
                ));
            }
            if azure.service_principal_secret.is_some() && azure.use_workload_identity {
                return Err(Error::validation(
                    "Azure storage authentication methods are mutually exclusive",
                ));
            }
        }
        "gcs" => {
            if storage.gcs.is_none() {
                return Err(Error::validation(
                    "GCS storage selected but gcs configuration is missing",
                ));
            }
        }
        other => {
            return Err(Error::validation(format!(
                "Invalid storage type '{}': must be one of: pvc, s3, azure, gcs",
                other
            )));
        }
    }
    Ok(())
}

/// Check if a backup should run based on the schedule
pub async fn check_schedule(
    backup: &KafkaBackup,
    client: &Client,
    namespace: &str,
) -> Result<Action> {
    let name = backup.name_any();

    // If no schedule, this is a one-shot backup - check if already completed
    let Some(schedule_str) = &backup.spec.schedule else {
        if let Some(status) = &backup.status {
            if status.phase.as_deref() == Some("Completed") {
                return Ok(Action::await_change());
            }
        }
        // One-shot backup that hasn't run - execute now
        return execute_backup(backup, client, namespace).await;
    };

    // Parse schedule
    let schedule = Schedule::from_str(schedule_str)
        .map_err(|e| Error::validation(format!("Invalid cron schedule: {}", e)))?;

    let now = Utc::now();

    // Primary decision: CRD `status.lastScheduleTime` (survives restart).
    let mut should_run = should_run_backup(backup, &schedule, now);

    // Defence in depth: process-local guard overrides the cache-based
    // decision when the current cron tick was already fired in-process.
    // Required because the reflector cache can lag the `Running` status
    // patch by tens of milliseconds — long enough for a very fast backup to
    // complete and re-enter reconcile before the cache catches up.
    let key = guard_key(namespace, &name);
    if should_run {
        if let Some(last_fired) = scheduler_guard().lock().unwrap().get(&key).copied() {
            if schedule
                .after(&last_fired)
                .next()
                .map(|next_tick| next_tick > now)
                .unwrap_or(true)
            {
                debug!(
                    name = %name,
                    last_fired = %last_fired,
                    "In-memory guard indicates current tick already fired; deferring"
                );
                should_run = false;
            }
        }
    }

    if should_run {
        // Record the fire BEFORE calling execute_backup so a racing
        // reconcile triggered by our own Running patch is correctly filtered
        // above even if it reads a stale cache.
        scheduler_guard().lock().unwrap().insert(key, now);
        info!(name = %name, "Scheduled backup time reached, executing backup");
        return execute_backup(backup, client, namespace).await;
    }

    // Calculate next run time
    let next_run = schedule
        .upcoming(Utc)
        .next()
        .unwrap_or_else(|| now + chrono::Duration::hours(1));

    // Requeue for next scheduled backup
    let duration_until_next = (next_run - now).to_std().unwrap_or(Duration::from_secs(60));
    let requeue_duration = duration_until_next.min(Duration::from_secs(300)); // Max 5 minutes

    Ok(Action::requeue(requeue_duration))
}

/// Determine if a scheduled backup tick is due.
///
/// Uses `status.lastScheduleTime` as the monotonic anchor — this is written
/// tentatively in the `Running` status patch before the engine runs, so it is
/// present in the reflector cache even when a subsequent reconcile fires
/// before the terminal `Completed`/`Failed` patch has propagated. This closes
/// the read-your-own-writes race that caused issue #93.
///
/// Fallback chain for the anchor:
/// 1. `status.lastScheduleTime` — authoritative once a tick has started.
/// 2. `metadata.creationTimestamp` — cold-start anchor; ensures the resource
///    does not fire immediately on creation regardless of where the clock sits
///    inside the current cron interval.
/// 3. `now` — safety net; returns `false` for degenerate schedules.
fn should_run_backup(backup: &KafkaBackup, schedule: &Schedule, now: DateTime<Utc>) -> bool {
    let anchor = backup
        .status
        .as_ref()
        .and_then(|s| s.last_schedule_time)
        .or_else(|| backup.metadata.creation_timestamp.as_ref().map(|t| t.0))
        .unwrap_or(now);

    schedule
        .after(&anchor)
        .next()
        .map(|next_tick| next_tick <= now)
        .unwrap_or(false)
}

/// Execute a backup operation
async fn execute_backup(backup: &KafkaBackup, client: &Client, namespace: &str) -> Result<Action> {
    let name = backup.name_any();
    let api: Api<KafkaBackup> = Api::namespaced(client.clone(), namespace);

    info!(name = %name, "Starting backup execution");

    // Tentative anchor: write `lastScheduleTime` BEFORE the engine runs so the
    // reflector cache always has something to scheduler-anchor against on the
    // next reconcile. Without this, back-to-back reconciles driven by the
    // `Running` and `Completed` status events race the cache and re-execute
    // the same tick (issue #93).
    let running_status = json!({
        "status": {
            "phase": "Running",
            "message": "Backup in progress",
            "lastScheduleTime": Utc::now(),
            "observedGeneration": backup.metadata.generation,
        }
    });
    api.patch_status(
        &name,
        &PatchParams::apply("kafka-backup-operator"),
        &Patch::Merge(running_status),
    )
    .await?;

    // TODO: Execute actual backup using kafka-backup-core
    // For now, simulate a successful backup
    let backup_result = execute_backup_internal(backup, client, namespace).await;

    match backup_result {
        Ok(result) => {
            info!(name = %name, records = result.records_processed, bytes = result.bytes_processed, "Backup completed successfully");

            // Update metrics
            metrics::BACKUPS_TOTAL
                .with_label_values(&["success", namespace, &name])
                .inc();
            metrics::BACKUP_SIZE_BYTES
                .with_label_values(&[namespace, &name])
                .set(result.bytes_processed as f64);
            metrics::BACKUP_RECORDS
                .with_label_values(&[namespace, &name])
                .set(result.records_processed as f64);

            // Calculate next scheduled backup
            let next_backup = backup.spec.schedule.as_ref().and_then(|s| {
                Schedule::from_str(s)
                    .ok()
                    .and_then(|sched| sched.upcoming(Utc).next())
            });

            let completed_status = json!({
                "status": {
                    "phase": "Completed",
                    "message": "Backup completed successfully",
                    "lastBackupTime": Utc::now(),
                    "nextScheduledBackup": next_backup,
                    "recordsProcessed": result.records_processed,
                    "bytesProcessed": result.bytes_processed,
                    "segmentsCompleted": result.segments_completed,
                    "backupId": result.backup_id,
                    "observedGeneration": backup.metadata.generation,
                    "conditions": [{
                        "type": "Ready",
                        "status": "True",
                        "lastTransitionTime": Utc::now(),
                        "reason": "BackupSucceeded",
                        "message": "Backup completed successfully"
                    }]
                }
            });
            api.patch_status(
                &name,
                &PatchParams::apply("kafka-backup-operator"),
                &Patch::Merge(completed_status),
            )
            .await?;

            // Requeue for next scheduled backup
            if backup.spec.schedule.is_some() {
                Ok(Action::requeue(Duration::from_secs(60)))
            } else {
                Ok(Action::await_change())
            }
        }
        Err(e) => {
            error!(name = %name, error = %e, "Backup failed");

            metrics::BACKUPS_TOTAL
                .with_label_values(&["failure", namespace, &name])
                .inc();

            let failed_status = json!({
                "status": {
                    "phase": "Failed",
                    "message": format!("Backup failed: {}", e),
                    "observedGeneration": backup.metadata.generation,
                    "conditions": [{
                        "type": "Ready",
                        "status": "False",
                        "lastTransitionTime": Utc::now(),
                        "reason": "BackupFailed",
                        "message": e.to_string()
                    }]
                }
            });
            api.patch_status(
                &name,
                &PatchParams::apply("kafka-backup-operator"),
                &Patch::Merge(failed_status),
            )
            .await?;

            // Retry after delay
            Ok(Action::requeue(Duration::from_secs(300)))
        }
    }
}

/// Internal backup execution result
struct BackupResult {
    backup_id: String,
    records_processed: u64,
    bytes_processed: u64,
    segments_completed: u64,
}

/// Execute the actual backup using kafka-backup-core library
async fn execute_backup_internal(
    backup: &KafkaBackup,
    client: &Client,
    namespace: &str,
) -> Result<BackupResult> {
    let name = backup.name_any();

    // Generate unique backup ID
    let backup_id = format!("{}-{}", name, Utc::now().format("%Y%m%d-%H%M%S"));

    info!(name = %name, backup_id = %backup_id, "Building backup configuration");

    // 1. Build resolved configuration from CRD spec using adapters
    let resolved_config = build_backup_config(backup, client, namespace).await?;

    // 2. Ensure storage directory exists before creating the backup engine
    ensure_storage_directories(&resolved_config.storage)?;

    // 2b. Create TLS file manager if TLS is configured
    let tls_manager = if let Some(tls) = &resolved_config.kafka.tls {
        let tls_dir = default_tls_dir(&name);
        Some(TlsFileManager::new(tls, &tls_dir)?)
    } else {
        None
    };

    // 3. Convert to kafka-backup-core Config
    let core_config = to_core_backup_config(&resolved_config, &backup_id, tls_manager.as_ref())
        .map_err(|e| Error::Core(format!("Failed to build core config: {}", e)))?;

    info!(
        name = %name,
        backup_id = %backup_id,
        topics = ?resolved_config.topics,
        "Starting backup engine"
    );

    // 4. Change working directory to storage path before creating engine
    // The kafka-backup-core library creates SQLite offset database using relative path
    // ./backup_id-offsets.db, so we need to ensure current directory is writable
    let working_dir = get_storage_working_directory(&resolved_config.storage);
    let original_dir = std::env::current_dir().ok();
    if let Err(e) = std::env::set_current_dir(&working_dir) {
        return Err(Error::Storage(format!(
            "Failed to change working directory to '{}': {}",
            working_dir.display(),
            e
        )));
    }
    debug!(working_dir = %working_dir.display(), "Changed working directory for backup engine");

    // 5. Create the backup engine (async constructor)
    let engine = BackupEngine::new(core_config)
        .await
        .map_err(|e| Error::Core(format!("Failed to create backup engine: {}", e)))?;

    // 6. Get metrics handle for tracking progress
    let metrics_handle = engine.metrics();

    // 7. Run the backup (must run in the same working directory as engine was created)
    let run_result = engine.run().await;

    // Restore original working directory after backup completes
    if let Some(ref orig) = original_dir {
        let _ = std::env::set_current_dir(orig);
    }

    run_result.map_err(|e| Error::Core(format!("Backup execution failed: {}", e)))?;

    // 8. Extract final metrics
    let metrics_report = metrics_handle.report();

    info!(
        name = %name,
        backup_id = %backup_id,
        records = metrics_report.records_processed,
        bytes = metrics_report.bytes_written,
        "Backup completed successfully"
    );

    Ok(BackupResult {
        backup_id,
        records_processed: metrics_report.records_processed,
        bytes_processed: metrics_report.bytes_written,
        segments_completed: metrics_report.segments_written,
    })
}

/// Update status to Ready
pub async fn update_status_ready(
    backup: &KafkaBackup,
    client: &Client,
    namespace: &str,
) -> Result<()> {
    let name = backup.name_any();
    let api: Api<KafkaBackup> = Api::namespaced(client.clone(), namespace);

    // Calculate next scheduled backup
    let next_backup = backup.spec.schedule.as_ref().and_then(|s| {
        Schedule::from_str(s)
            .ok()
            .and_then(|sched| sched.upcoming(Utc).next())
    });

    let status = json!({
        "status": {
            "phase": "Ready",
            "message": "Backup schedule is active",
            "nextScheduledBackup": next_backup,
            "observedGeneration": backup.metadata.generation,
            "checkpointEnabled": backup.spec.checkpoint.as_ref().map(|c| c.enabled).unwrap_or(true),
            "conditions": [{
                "type": "Ready",
                "status": "True",
                "lastTransitionTime": Utc::now(),
                "reason": "ScheduleActive",
                "message": "Backup schedule is configured and active"
            }]
        }
    });

    api.patch_status(
        &name,
        &PatchParams::apply("kafka-backup-operator"),
        &Patch::Merge(status),
    )
    .await?;

    Ok(())
}

/// Update status to Failed
pub async fn update_status_failed(
    backup: &KafkaBackup,
    client: &Client,
    namespace: &str,
    error_message: &str,
) -> Result<()> {
    let name = backup.name_any();
    let api: Api<KafkaBackup> = Api::namespaced(client.clone(), namespace);

    let status = json!({
        "status": {
            "phase": "Failed",
            "message": error_message,
            "observedGeneration": backup.metadata.generation,
            "conditions": [{
                "type": "Ready",
                "status": "False",
                "lastTransitionTime": Utc::now(),
                "reason": "ValidationFailed",
                "message": error_message
            }]
        }
    });

    api.patch_status(
        &name,
        &PatchParams::apply("kafka-backup-operator"),
        &Patch::Merge(status),
    )
    .await?;

    Ok(())
}

/// Ensure storage directories exist before backup execution
/// This prevents "unable to open database file" errors from the core library
fn ensure_storage_directories(storage: &ResolvedStorage) -> Result<()> {
    match storage {
        ResolvedStorage::Local(local) => {
            let path = Path::new(&local.path);
            if !path.exists() {
                debug!(path = %local.path, "Creating storage directory");
                std::fs::create_dir_all(path).map_err(|e| {
                    Error::Storage(format!(
                        "Failed to create storage directory '{}': {}",
                        local.path, e
                    ))
                })?;
            }
            // Also create subdirectories that kafka-backup-core expects
            let metadata_path = path.join("metadata");
            let segments_path = path.join("segments");

            if !metadata_path.exists() {
                debug!(path = ?metadata_path, "Creating metadata subdirectory");
                std::fs::create_dir_all(&metadata_path).map_err(|e| {
                    Error::Storage(format!("Failed to create metadata directory: {}", e))
                })?;
            }

            if !segments_path.exists() {
                debug!(path = ?segments_path, "Creating segments subdirectory");
                std::fs::create_dir_all(&segments_path).map_err(|e| {
                    Error::Storage(format!("Failed to create segments directory: {}", e))
                })?;
            }

            info!(path = %local.path, "Storage directories ready");
            Ok(())
        }
        // Cloud storage backends handle directory creation automatically
        ResolvedStorage::S3(_) | ResolvedStorage::Azure(_) | ResolvedStorage::Gcs(_) => Ok(()),
    }
}

/// Get the working directory path for the storage backend
/// This is used to ensure the SQLite offset database is created in a writable location
fn get_storage_working_directory(storage: &ResolvedStorage) -> std::path::PathBuf {
    match storage {
        ResolvedStorage::Local(local) => std::path::PathBuf::from(&local.path),
        // For cloud storage, use /tmp as the working directory
        ResolvedStorage::S3(_) | ResolvedStorage::Azure(_) | ResolvedStorage::Gcs(_) => {
            std::path::PathBuf::from("/tmp")
        }
    }
}

#[cfg(test)]
mod should_run_backup_tests {
    use super::*;
    use crate::crd::{KafkaBackup, KafkaBackupStatus};
    use chrono::TimeZone;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
    use serde_json::json;

    fn make_backup(creation: DateTime<Utc>, status: Option<KafkaBackupStatus>) -> KafkaBackup {
        let spec = serde_json::from_value(json!({
            "kafkaCluster": {"bootstrapServers": ["localhost:9092"]},
            "topics": ["t"],
            "storage": {
                "storageType": "pvc",
                "pvc": {"claimName": "c"}
            },
        }))
        .unwrap();
        let mut b = KafkaBackup::new("test", spec);
        b.metadata.creation_timestamp = Some(Time(creation));
        b.status = status;
        b
    }

    fn every_ten_seconds() -> Schedule {
        // cron crate 0.12 expects 7 fields: sec min hour dom month dow year
        Schedule::from_str("*/10 * * * * * *").unwrap()
    }

    fn at(h: u32, m: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 4, 21, h, m, s).unwrap()
    }

    /// Cold start, creation timestamp well before now → a tick has elapsed
    /// between creation and now, so backup fires.
    #[test]
    fn cold_start_past_first_tick_fires() {
        let creation = at(10, 0, 0);
        let now = at(10, 0, 30);
        let backup = make_backup(creation, None);
        assert!(should_run_backup(&backup, &every_ten_seconds(), now));
    }

    /// Cold start inside the first interval (less than one cron interval has
    /// elapsed since creation) → not due yet.
    #[test]
    fn cold_start_inside_first_interval_skips() {
        let creation = at(10, 0, 1); // 1 second past the :00 tick
        let now = at(10, 0, 5); // still before the :10 tick
        let backup = make_backup(creation, None);
        assert!(!should_run_backup(&backup, &every_ten_seconds(), now));
    }

    /// A scheduled tick has passed since lastScheduleTime → fires.
    #[test]
    fn overdue_tick_fires() {
        let now = at(10, 0, 25);
        let status = KafkaBackupStatus {
            last_schedule_time: Some(at(10, 0, 14)), // next tick after is :20
            ..Default::default()
        };
        let backup = make_backup(at(9, 0, 0), Some(status));
        assert!(should_run_backup(&backup, &every_ten_seconds(), now));
    }

    /// Mid-interval between ticks → does not fire. Regression guard for the
    /// strict `>` inequality (bug 2 in issue #93).
    #[test]
    fn mid_interval_does_not_fire() {
        let now = at(10, 0, 23);
        let status = KafkaBackupStatus {
            last_schedule_time: Some(at(10, 0, 20)), // next tick after is :30, still in the future
            ..Default::default()
        };
        let backup = make_backup(at(9, 0, 0), Some(status));
        assert!(!should_run_backup(&backup, &every_ten_seconds(), now));
    }

    /// Reflector-cache race: a second reconcile fires immediately after the
    /// first one wrote `Running(lastScheduleTime=now)`; the cache has the
    /// tentative anchor but not yet the `Completed` patch. Must NOT re-fire.
    /// Regression guard for bug 1 in issue #93 (the reported symptom).
    #[test]
    fn cache_lag_after_running_patch_does_not_double_fire() {
        let now = at(10, 0, 20);
        let status = KafkaBackupStatus {
            phase: Some("Running".into()),
            last_schedule_time: Some(at(10, 0, 20)), // just written by predecessor reconcile
            last_backup_time: None,                  // Completed patch not yet absorbed
            ..Default::default()
        };
        let backup = make_backup(at(9, 0, 0), Some(status));
        assert!(!should_run_backup(&backup, &every_ten_seconds(), now));
    }

    /// `Failed` phase mid-interval: merge-patch preserved `lastScheduleTime`,
    /// so the next reconcile triggered by the Failed status patch must skip
    /// until the next scheduled tick. Regression guard for bug 3 in issue #93
    /// (the Failed→reconcile→re-execute CPU loop).
    #[test]
    fn failed_phase_mid_interval_does_not_retry_immediately() {
        let now = at(10, 0, 22);
        let status = KafkaBackupStatus {
            phase: Some("Failed".into()),
            last_schedule_time: Some(at(10, 0, 20)), // next tick after is :30
            ..Default::default()
        };
        let backup = make_backup(at(9, 0, 0), Some(status));
        assert!(!should_run_backup(&backup, &every_ten_seconds(), now));
    }
}
