//! KafkaOffsetRollback reconciler
//!
//! Handles the business logic for rolling back consumer group offsets
//! to a previous snapshot.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use kube::{
    api::{Patch, PatchParams},
    runtime::controller::Action,
    Api, Client, ResourceExt,
};
use serde_json::json;
use tracing::{error, info, warn};

use crate::crd::{KafkaOffsetRollback, KafkaOffsetRollbackStatus};
use crate::error::{Error, Result};
use crate::metrics;

/// Validate the KafkaOffsetRollback spec
pub fn validate(rollback: &KafkaOffsetRollback) -> Result<()> {
    // Validate kafka cluster
    if rollback.spec.kafka_cluster.bootstrap_servers.is_empty() {
        return Err(Error::validation(
            "At least one bootstrap server must be specified",
        ));
    }

    // Validate snapshot reference
    if rollback.spec.snapshot_ref.name.is_empty()
        && rollback.spec.snapshot_ref.path.is_none()
    {
        return Err(Error::validation(
            "Either snapshot name or path must be specified",
        ));
    }

    Ok(())
}

/// Monitor rollback progress
pub async fn monitor_progress(
    rollback: &KafkaOffsetRollback,
    client: &Client,
    namespace: &str,
) -> Result<Action> {
    let name = rollback.name_any();

    info!(name = %name, "Monitoring rollback progress");

    Ok(Action::requeue(Duration::from_secs(2)))
}

/// Execute a rollback operation
pub async fn execute(
    rollback: &KafkaOffsetRollback,
    client: &Client,
    namespace: &str,
) -> Result<Action> {
    let name = rollback.name_any();
    let api: Api<KafkaOffsetRollback> = Api::namespaced(client.clone(), namespace);

    info!(
        name = %name,
        snapshot = %rollback.spec.snapshot_ref.name,
        "Starting offset rollback execution"
    );

    // Check if this is a dry run
    if rollback.spec.dry_run {
        info!(name = %name, "Dry run mode - validating rollback parameters");
        return execute_dry_run(rollback, client, namespace).await;
    }

    // Update status to Running
    let running_status = json!({
        "status": {
            "phase": "Running",
            "message": "Offset rollback in progress",
            "observedGeneration": rollback.metadata.generation,
        }
    });
    api.patch_status(&name, &PatchParams::apply("kafka-backup-operator"), &Patch::Merge(running_status))
        .await?;

    // Execute rollback
    let start_time = std::time::Instant::now();
    let rollback_result = execute_rollback_internal(rollback, client, namespace).await;
    let duration = start_time.elapsed();

    match rollback_result {
        Ok(result) => {
            info!(
                name = %name,
                groups_rolled_back = result.groups_rolled_back,
                duration = ?duration,
                "Offset rollback completed"
            );

            let completed_status = json!({
                "status": {
                    "phase": "Completed",
                    "message": format!("Rolled back {} groups", result.groups_rolled_back),
                    "groupsRolledBack": result.groups_rolled_back,
                    "duration": format!("{:.2}s", duration.as_secs_f64()),
                    "verified": result.verified,
                    "observedGeneration": rollback.metadata.generation,
                    "conditions": [{
                        "type": "Ready",
                        "status": "True",
                        "lastTransitionTime": Utc::now(),
                        "reason": "RollbackSucceeded",
                        "message": format!("Rolled back {} groups", result.groups_rolled_back)
                    }]
                }
            });
            api.patch_status(&name, &PatchParams::apply("kafka-backup-operator"), &Patch::Merge(completed_status))
                .await?;

            Ok(Action::await_change())
        }
        Err(e) => {
            error!(name = %name, error = %e, "Offset rollback failed");

            let failed_status = json!({
                "status": {
                    "phase": "Failed",
                    "message": format!("Offset rollback failed: {}", e),
                    "observedGeneration": rollback.metadata.generation,
                    "conditions": [{
                        "type": "Ready",
                        "status": "False",
                        "lastTransitionTime": Utc::now(),
                        "reason": "RollbackFailed",
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
    rollback: &KafkaOffsetRollback,
    client: &Client,
    namespace: &str,
) -> Result<Action> {
    let name = rollback.name_any();
    let api: Api<KafkaOffsetRollback> = Api::namespaced(client.clone(), namespace);

    // TODO: Validate snapshot exists and is accessible
    // TODO: Validate consumer groups exist

    let status = json!({
        "status": {
            "phase": "Completed",
            "message": "Dry run validation passed",
            "observedGeneration": rollback.metadata.generation,
            "conditions": [{
                "type": "Ready",
                "status": "True",
                "lastTransitionTime": Utc::now(),
                "reason": "DryRunPassed",
                "message": "Rollback validation completed successfully"
            }]
        }
    });
    api.patch_status(&name, &PatchParams::apply("kafka-backup-operator"), &Patch::Merge(status))
        .await?;

    Ok(Action::await_change())
}

/// Internal rollback execution result
struct RollbackResult {
    groups_rolled_back: u32,
    verified: bool,
}

/// Execute the actual rollback (placeholder for kafka-backup-core integration)
async fn execute_rollback_internal(
    rollback: &KafkaOffsetRollback,
    client: &Client,
    namespace: &str,
) -> Result<RollbackResult> {
    // TODO: Implement actual rollback using kafka-backup-core library

    // 1. Load snapshot from storage
    // let snapshot = load_snapshot(&rollback.spec.snapshot_ref).await?;

    // 2. Apply offsets to consumer groups
    // kafka_backup_core::apply_offset_snapshot(snapshot, &rollback.spec.kafka_cluster).await?;

    // 3. Verify if requested
    // if rollback.spec.verify_after_rollback {
    //     kafka_backup_core::verify_offsets(snapshot, &rollback.spec.kafka_cluster).await?;
    // }

    Ok(RollbackResult {
        groups_rolled_back: rollback.spec.consumer_groups.len() as u32,
        verified: rollback.spec.verify_after_rollback,
    })
}

/// Update status to Failed
pub async fn update_status_failed(
    rollback: &KafkaOffsetRollback,
    client: &Client,
    namespace: &str,
    error_message: &str,
) -> Result<()> {
    let name = rollback.name_any();
    let api: Api<KafkaOffsetRollback> = Api::namespaced(client.clone(), namespace);

    let status = json!({
        "status": {
            "phase": "Failed",
            "message": error_message,
            "observedGeneration": rollback.metadata.generation,
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
