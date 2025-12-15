//! KafkaBackup reconciler
//!
//! Handles the business logic for backup operations including:
//! - Spec validation
//! - Schedule checking
//! - Backup execution
//! - Status updates

use std::path::Path;
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

use crate::adapters::{build_backup_config, to_core_backup_config, ResolvedStorage};
use crate::crd::KafkaBackup;
use crate::error::{Error, Result};
use crate::metrics;

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
            // Validate that either workload identity or credentials_secret is provided
            if !azure.use_workload_identity && azure.credentials_secret.is_none() {
                return Err(Error::validation(
                    "Azure storage requires either use_workload_identity: true or credentials_secret to be configured"
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

    // Check if we should run now
    let should_run = should_run_backup(backup, &schedule, now);

    if should_run {
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

/// Determine if a backup should run now
fn should_run_backup(backup: &KafkaBackup, schedule: &Schedule, now: DateTime<Utc>) -> bool {
    let last_backup = backup.status.as_ref().and_then(|s| s.last_backup_time);

    match last_backup {
        None => true, // Never backed up
        Some(last) => {
            // Get the most recent scheduled time before now
            let mut _prev_scheduled = None;
            for scheduled in schedule.upcoming(Utc).take(10) {
                if scheduled > now {
                    break;
                }
                _prev_scheduled = Some(scheduled);
            }

            // Check using after() iterator for past times
            if let Some(_next) = schedule.upcoming(Utc).next() {
                // If next scheduled time is in the future, check if we missed one
                let interval = schedule.upcoming(Utc).take(2).collect::<Vec<_>>();

                if interval.len() >= 2 {
                    let typical_interval = interval[1] - interval[0];
                    let since_last = now - last;

                    // If more than one interval has passed since last backup, run now
                    return since_last > typical_interval;
                }
            }

            false
        }
    }
}

/// Execute a backup operation
async fn execute_backup(backup: &KafkaBackup, client: &Client, namespace: &str) -> Result<Action> {
    let name = backup.name_any();
    let api: Api<KafkaBackup> = Api::namespaced(client.clone(), namespace);

    info!(name = %name, "Starting backup execution");

    // Update status to Running
    let running_status = json!({
        "status": {
            "phase": "Running",
            "message": "Backup in progress",
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

    // 3. Convert to kafka-backup-core Config
    let core_config = to_core_backup_config(&resolved_config, &backup_id)
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
