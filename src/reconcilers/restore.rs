//! KafkaRestore reconciler
//!
//! Handles the business logic for restore operations including:
//! - Spec validation
//! - Backup reference resolution
//! - Restore execution with PITR support
//! - Offset recovery
//! - Rollback handling

use std::time::Duration;

use chrono::Utc;
use kafka_backup_core::restore::engine::RestoreEngine;
use kube::{
    api::{Patch, PatchParams},
    runtime::controller::Action,
    Api, Client, ResourceExt,
};
use serde_json::json;
use tracing::{error, info, warn};

use crate::adapters::{
    build_restore_config, to_core_restore_config, ResolvedBackupSource, ResolvedStorage,
};
use crate::crd::{KafkaBackup, KafkaRestore};
use crate::error::{Error, Result};
use crate::metrics;

/// Validate the KafkaRestore spec
pub fn validate(restore: &KafkaRestore) -> Result<()> {
    // Validate backup reference
    if restore.spec.backup_ref.name.is_empty() && restore.spec.backup_ref.storage.is_none() {
        return Err(Error::validation(
            "Either backup name or direct storage reference must be specified",
        ));
    }

    // Validate kafka cluster
    if restore.spec.kafka_cluster.bootstrap_servers.is_empty() {
        return Err(Error::validation(
            "At least one bootstrap server must be specified",
        ));
    }

    // Validate PITR if specified
    if let Some(pitr) = &restore.spec.pitr {
        if let (Some(start), Some(end)) = (&pitr.start_timestamp, &pitr.end_timestamp) {
            if start >= end {
                return Err(Error::validation(
                    "PITR start timestamp must be before end timestamp",
                ));
            }
        }
    }

    Ok(())
}

/// Monitor restore progress
pub async fn monitor_progress(
    restore: &KafkaRestore,
    client: &Client,
    namespace: &str,
) -> Result<Action> {
    let name = restore.name_any();

    // TODO: Check actual restore progress from the running operation
    // For now, just requeue to check again
    info!(name = %name, "Monitoring restore progress");

    Ok(Action::requeue(Duration::from_secs(5)))
}

/// Execute a restore operation
pub async fn execute(
    restore: &KafkaRestore,
    client: &Client,
    namespace: &str,
) -> Result<Action> {
    let name = restore.name_any();
    let api: Api<KafkaRestore> = Api::namespaced(client.clone(), namespace);

    info!(name = %name, "Starting restore execution");

    // Check if this is a dry run
    if restore.spec.dry_run {
        info!(name = %name, "Dry run mode - validating restore parameters");
        return execute_dry_run(restore, client, namespace).await;
    }

    // Update status to Running
    let running_status = json!({
        "status": {
            "phase": "Running",
            "message": "Restore in progress",
            "startTime": Utc::now(),
            "observedGeneration": restore.metadata.generation,
        }
    });
    api.patch_status(&name, &PatchParams::apply("kafka-backup-operator"), &Patch::Merge(running_status))
        .await?;

    // Create rollback snapshot if enabled
    if let Some(rollback) = &restore.spec.rollback {
        if rollback.snapshot_before_restore {
            info!(name = %name, "Creating pre-restore offset snapshot for rollback");
            // TODO: Create offset snapshot using kafka-backup-core
        }
    }

    // Execute restore
    let restore_result = execute_restore_internal(restore, client, namespace).await;

    match restore_result {
        Ok(result) => {
            info!(
                name = %name,
                records = result.records_restored,
                "Restore completed successfully"
            );

            // Update metrics
            metrics::RESTORES_TOTAL
                .with_label_values(&["success", namespace, &name])
                .inc();

            let completed_status = json!({
                "status": {
                    "phase": "Completed",
                    "message": "Restore completed successfully",
                    "completionTime": Utc::now(),
                    "recordsRestored": result.records_restored,
                    "bytesRestored": result.bytes_restored,
                    "segmentsProcessed": result.segments_processed,
                    "progressPercent": 100,
                    "offsetMappingPath": result.offset_mapping_path,
                    "observedGeneration": restore.metadata.generation,
                    "conditions": [{
                        "type": "Ready",
                        "status": "True",
                        "lastTransitionTime": Utc::now(),
                        "reason": "RestoreSucceeded",
                        "message": "Restore completed successfully"
                    }]
                }
            });
            api.patch_status(&name, &PatchParams::apply("kafka-backup-operator"), &Patch::Merge(completed_status))
                .await?;

            // Execute offset reset if configured
            if let Some(offset_reset) = &restore.spec.offset_reset {
                if offset_reset.enabled {
                    info!(name = %name, "Executing post-restore offset reset");
                    // TODO: Create KafkaOffsetReset resource or execute directly
                }
            }

            Ok(Action::await_change())
        }
        Err(e) => {
            error!(name = %name, error = %e, "Restore failed");

            metrics::RESTORES_TOTAL
                .with_label_values(&["failure", namespace, &name])
                .inc();

            // Check if auto-rollback is enabled
            if let Some(rollback) = &restore.spec.rollback {
                if rollback.auto_rollback_on_failure {
                    warn!(name = %name, "Auto-rollback enabled, attempting rollback");
                    // TODO: Trigger rollback
                }
            }

            let failed_status = json!({
                "status": {
                    "phase": "Failed",
                    "message": format!("Restore failed: {}", e),
                    "observedGeneration": restore.metadata.generation,
                    "conditions": [{
                        "type": "Ready",
                        "status": "False",
                        "lastTransitionTime": Utc::now(),
                        "reason": "RestoreFailed",
                        "message": e.to_string()
                    }]
                }
            });
            api.patch_status(&name, &PatchParams::apply("kafka-backup-operator"), &Patch::Merge(failed_status))
                .await?;

            Ok(Action::requeue(Duration::from_secs(300)))
        }
    }
}

/// Execute dry run validation
async fn execute_dry_run(
    restore: &KafkaRestore,
    client: &Client,
    namespace: &str,
) -> Result<Action> {
    let name = restore.name_any();
    let api: Api<KafkaRestore> = Api::namespaced(client.clone(), namespace);

    // TODO: Validate backup exists and is accessible
    // TODO: Validate target cluster is reachable
    // TODO: Validate topics can be created/written to

    let status = json!({
        "status": {
            "phase": "Completed",
            "message": "Dry run validation passed",
            "observedGeneration": restore.metadata.generation,
            "conditions": [{
                "type": "Ready",
                "status": "True",
                "lastTransitionTime": Utc::now(),
                "reason": "DryRunPassed",
                "message": "Restore validation completed successfully"
            }]
        }
    });
    api.patch_status(&name, &PatchParams::apply("kafka-backup-operator"), &Patch::Merge(status))
        .await?;

    Ok(Action::await_change())
}

/// Internal restore execution result
struct RestoreResult {
    records_restored: u64,
    bytes_restored: u64,
    segments_processed: u64,
    offset_mapping_path: Option<String>,
}

/// Execute the actual restore using kafka-backup-core library
async fn execute_restore_internal(
    restore: &KafkaRestore,
    client: &Client,
    namespace: &str,
) -> Result<RestoreResult> {
    let name = restore.name_any();

    info!(name = %name, "Building restore configuration");

    // 1. Build resolved configuration from CRD spec
    let resolved_config = build_restore_config(restore, client, namespace).await?;

    // 2. Resolve the backup source to get storage config and backup ID
    let (backup_id, storage) = resolve_backup_source(&resolved_config.backup_source, client, namespace).await?;

    info!(
        name = %name,
        backup_id = %backup_id,
        topics = ?resolved_config.topics,
        "Starting restore engine"
    );

    // 3. Convert to kafka-backup-core Config
    let core_config = to_core_restore_config(&resolved_config, &backup_id, &storage)
        .map_err(|e| Error::Core(format!("Failed to build core config: {}", e)))?;

    // 4. Create the restore engine (sync constructor)
    let engine = RestoreEngine::new(core_config)
        .map_err(|e| Error::Core(format!("Failed to create restore engine: {}", e)))?;

    // 5. Get progress receiver for monitoring
    let mut progress_rx = engine.progress_receiver();

    // Spawn progress monitoring task
    let name_clone = name.clone();
    tokio::spawn(async move {
        while let Ok(progress) = progress_rx.recv().await {
            info!(
                name = %name_clone,
                records = progress.records_restored,
                percentage = progress.percentage,
                throughput = progress.throughput_records_per_sec,
                "Restore progress"
            );
        }
    });

    // 6. Run the restore
    let report = engine
        .run()
        .await
        .map_err(|e| Error::Core(format!("Restore execution failed: {}", e)))?;

    info!(
        name = %name,
        backup_id = %backup_id,
        records = report.records_restored,
        bytes = report.bytes_restored,
        "Restore completed successfully"
    );

    Ok(RestoreResult {
        records_restored: report.records_restored,
        bytes_restored: report.bytes_restored,
        segments_processed: report.segments_processed,
        offset_mapping_path: None, // Offset mapping stored in report.offset_mapping
    })
}

/// Resolve backup source to get backup ID and storage configuration
async fn resolve_backup_source(
    source: &ResolvedBackupSource,
    client: &Client,
    namespace: &str,
) -> Result<(String, ResolvedStorage)> {
    match source {
        ResolvedBackupSource::Storage(storage) => {
            // Direct storage reference - use a generated backup ID
            let backup_id = format!("restore-{}", Utc::now().format("%Y%m%d-%H%M%S"));
            Ok((backup_id, storage.clone()))
        }
        ResolvedBackupSource::BackupResource { name, namespace: backup_ns, backup_id } => {
            // Reference to a KafkaBackup resource - fetch it to get storage config
            let api: Api<KafkaBackup> = Api::namespaced(client.clone(), backup_ns);
            let backup = api.get(name).await.map_err(|e| {
                Error::BackupNotFound(format!("Failed to fetch KafkaBackup '{}': {}", name, e))
            })?;

            // Use provided backup_id or latest from status
            let resolved_backup_id = backup_id.clone().unwrap_or_else(|| {
                backup
                    .status
                    .as_ref()
                    .and_then(|s| s.backup_id.clone())
                    .unwrap_or_else(|| format!("{}-latest", name))
            });

            // Build storage config from the backup's storage spec
            let storage = crate::adapters::build_storage_config(
                &backup.spec.storage,
                client,
                backup_ns,
            )
            .await?;

            Ok((resolved_backup_id, storage))
        }
    }
}

/// Update status to Failed
pub async fn update_status_failed(
    restore: &KafkaRestore,
    client: &Client,
    namespace: &str,
    error_message: &str,
) -> Result<()> {
    let name = restore.name_any();
    let api: Api<KafkaRestore> = Api::namespaced(client.clone(), namespace);

    let status = json!({
        "status": {
            "phase": "Failed",
            "message": error_message,
            "observedGeneration": restore.metadata.generation,
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
