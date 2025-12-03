//! KafkaBackup reconciler
//!
//! Handles the business logic for backup operations including:
//! - Spec validation
//! - Schedule checking
//! - Backup execution
//! - Status updates

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use cron::Schedule;
use kube::{
    api::{Patch, PatchParams},
    runtime::controller::Action,
    Api, Client, ResourceExt,
};
use serde_json::json;
use std::str::FromStr;
use tracing::{error, info, warn};

use crate::crd::{Condition, KafkaBackup, KafkaBackupStatus};
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
        return Err(Error::validation("At least one bootstrap server must be specified"));
    }

    // Validate storage configuration
    validate_storage(&backup.spec.storage)?;

    // Validate schedule if provided
    if let Some(schedule) = &backup.spec.schedule {
        Schedule::from_str(schedule)
            .map_err(|e| Error::validation(format!("Invalid cron schedule '{}': {}", schedule, e)))?;
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
    if backup.spec.compression == "zstd" {
        if backup.spec.compression_level < 1 || backup.spec.compression_level > 22 {
            return Err(Error::validation(format!(
                "Invalid zstd compression level {}: must be between 1 and 22",
                backup.spec.compression_level
            )));
        }
    }

    Ok(())
}

/// Validate storage configuration
fn validate_storage(storage: &crate::crd::StorageSpec) -> Result<()> {
    match storage.storage_type.as_str() {
        "pvc" => {
            if storage.pvc.is_none() {
                return Err(Error::validation("PVC storage selected but pvc configuration is missing"));
            }
        }
        "s3" => {
            if storage.s3.is_none() {
                return Err(Error::validation("S3 storage selected but s3 configuration is missing"));
            }
        }
        "azure" => {
            if storage.azure.is_none() {
                return Err(Error::validation("Azure storage selected but azure configuration is missing"));
            }
        }
        "gcs" => {
            if storage.gcs.is_none() {
                return Err(Error::validation("GCS storage selected but gcs configuration is missing"));
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
    let last_backup = backup
        .status
        .as_ref()
        .and_then(|s| s.last_backup_time);

    match last_backup {
        None => true, // Never backed up
        Some(last) => {
            // Get the most recent scheduled time before now
            let mut prev_scheduled = None;
            for scheduled in schedule.upcoming(Utc).take(10) {
                if scheduled > now {
                    break;
                }
                prev_scheduled = Some(scheduled);
            }

            // Check using after() iterator for past times
            if let Some(next) = schedule.upcoming(Utc).next() {
                // If next scheduled time is in the future, check if we missed one
                let interval = schedule
                    .upcoming(Utc)
                    .take(2)
                    .collect::<Vec<_>>();

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
async fn execute_backup(
    backup: &KafkaBackup,
    client: &Client,
    namespace: &str,
) -> Result<Action> {
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
    api.patch_status(&name, &PatchParams::apply("kafka-backup-operator"), &Patch::Merge(running_status))
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
            api.patch_status(&name, &PatchParams::apply("kafka-backup-operator"), &Patch::Merge(completed_status))
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
            api.patch_status(&name, &PatchParams::apply("kafka-backup-operator"), &Patch::Merge(failed_status))
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

/// Execute the actual backup (placeholder for kafka-backup-core integration)
async fn execute_backup_internal(
    backup: &KafkaBackup,
    client: &Client,
    namespace: &str,
) -> Result<BackupResult> {
    // TODO: Implement actual backup using kafka-backup-core library
    // This is a placeholder that demonstrates the expected interface

    // 1. Build configuration from CRD spec using adapters
    // let config = crate::adapters::build_backup_config(backup, client, namespace).await?;

    // 2. Create and run the backup engine
    // let engine = kafka_backup_core::BackupEngine::new(config).await?;
    // let result = engine.run().await?;

    // For now, return a placeholder result
    let backup_id = format!("backup-{}", Utc::now().timestamp());

    Ok(BackupResult {
        backup_id,
        records_processed: 0,
        bytes_processed: 0,
        segments_completed: 0,
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

    api.patch_status(&name, &PatchParams::apply("kafka-backup-operator"), &Patch::Merge(status))
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

    api.patch_status(&name, &PatchParams::apply("kafka-backup-operator"), &Patch::Merge(status))
        .await?;

    Ok(())
}
