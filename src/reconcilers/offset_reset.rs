//! KafkaOffsetReset reconciler
//!
//! Handles the business logic for consumer group offset reset operations.

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

use crate::crd::{KafkaOffsetReset, KafkaOffsetResetStatus, OffsetResetStrategy};
use crate::error::{Error, Result};
use crate::metrics;

/// Validate the KafkaOffsetReset spec
pub fn validate(reset: &KafkaOffsetReset) -> Result<()> {
    // Validate kafka cluster
    if reset.spec.kafka_cluster.bootstrap_servers.is_empty() {
        return Err(Error::validation(
            "At least one bootstrap server must be specified",
        ));
    }

    // Validate consumer groups
    if reset.spec.consumer_groups.is_empty() {
        return Err(Error::validation(
            "At least one consumer group must be specified",
        ));
    }

    // Validate strategy-specific requirements
    match &reset.spec.reset_strategy {
        OffsetResetStrategy::ToTimestamp => {
            if reset.spec.reset_timestamp.is_none() {
                return Err(Error::validation(
                    "reset_timestamp is required when using to-timestamp strategy",
                ));
            }
        }
        OffsetResetStrategy::ToOffset => {
            if reset.spec.reset_offset.is_none() {
                return Err(Error::validation(
                    "reset_offset is required when using to-offset strategy",
                ));
            }
        }
        OffsetResetStrategy::FromMapping => {
            if reset.spec.offset_mapping_ref.is_none() {
                return Err(Error::validation(
                    "offset_mapping_ref is required when using from-mapping strategy",
                ));
            }
        }
        _ => {}
    }

    // Validate parallelism
    if reset.spec.parallelism == 0 {
        return Err(Error::validation("parallelism must be greater than 0"));
    }

    Ok(())
}

/// Monitor offset reset progress
pub async fn monitor_progress(
    reset: &KafkaOffsetReset,
    client: &Client,
    namespace: &str,
) -> Result<Action> {
    let name = reset.name_any();

    // TODO: Check actual progress from running operation
    info!(name = %name, "Monitoring offset reset progress");

    Ok(Action::requeue(Duration::from_secs(2)))
}

/// Execute an offset reset operation
pub async fn execute(
    reset: &KafkaOffsetReset,
    client: &Client,
    namespace: &str,
) -> Result<Action> {
    let name = reset.name_any();
    let api: Api<KafkaOffsetReset> = Api::namespaced(client.clone(), namespace);

    info!(
        name = %name,
        groups = reset.spec.consumer_groups.len(),
        strategy = ?reset.spec.reset_strategy,
        "Starting offset reset execution"
    );

    // Check if this is a dry run
    if reset.spec.dry_run {
        info!(name = %name, "Dry run mode - validating reset parameters");
        return execute_dry_run(reset, client, namespace).await;
    }

    // Update status to Running
    let running_status = json!({
        "status": {
            "phase": "Running",
            "message": "Offset reset in progress",
            "groupsTotal": reset.spec.consumer_groups.len(),
            "groupsReset": 0,
            "groupsFailed": 0,
            "observedGeneration": reset.metadata.generation,
        }
    });
    api.patch_status(&name, &PatchParams::apply("kafka-backup-operator"), &Patch::Merge(running_status))
        .await?;

    // Create snapshot if enabled
    if reset.spec.snapshot_before_reset {
        info!(name = %name, "Creating pre-reset offset snapshot");
        // TODO: Create offset snapshot
    }

    // Execute offset reset
    let start_time = std::time::Instant::now();
    let reset_result = execute_reset_internal(reset, client, namespace).await;
    let duration = start_time.elapsed();

    match reset_result {
        Ok(result) => {
            let phase = if result.groups_failed > 0 && !reset.spec.continue_on_error {
                "PartiallyCompleted"
            } else if result.groups_failed > 0 {
                "PartiallyCompleted"
            } else {
                "Completed"
            };

            info!(
                name = %name,
                groups_reset = result.groups_reset,
                groups_failed = result.groups_failed,
                duration = ?duration,
                "Offset reset completed"
            );

            // Update metrics
            metrics::OFFSET_RESETS_TOTAL
                .with_label_values(&[phase.to_lowercase().as_str(), namespace])
                .inc();
            metrics::OFFSET_RESET_DURATION
                .with_label_values(&[namespace])
                .observe(duration.as_secs_f64());

            let completed_status = json!({
                "status": {
                    "phase": phase,
                    "message": format!("Reset {} groups, {} failed", result.groups_reset, result.groups_failed),
                    "groupsTotal": reset.spec.consumer_groups.len(),
                    "groupsReset": result.groups_reset,
                    "groupsFailed": result.groups_failed,
                    "duration": format!("{:.2}s", duration.as_secs_f64()),
                    "snapshotId": result.snapshot_id,
                    "snapshotPath": result.snapshot_path,
                    "groupResults": result.group_results,
                    "observedGeneration": reset.metadata.generation,
                    "conditions": [{
                        "type": "Ready",
                        "status": if result.groups_failed == 0 { "True" } else { "False" },
                        "lastTransitionTime": Utc::now(),
                        "reason": if result.groups_failed == 0 { "ResetSucceeded" } else { "PartialFailure" },
                        "message": format!("Reset {} groups, {} failed", result.groups_reset, result.groups_failed)
                    }]
                }
            });
            api.patch_status(&name, &PatchParams::apply("kafka-backup-operator"), &Patch::Merge(completed_status))
                .await?;

            Ok(Action::await_change())
        }
        Err(e) => {
            error!(name = %name, error = %e, "Offset reset failed");

            metrics::OFFSET_RESETS_TOTAL
                .with_label_values(&["failure", namespace])
                .inc();

            let failed_status = json!({
                "status": {
                    "phase": "Failed",
                    "message": format!("Offset reset failed: {}", e),
                    "observedGeneration": reset.metadata.generation,
                    "conditions": [{
                        "type": "Ready",
                        "status": "False",
                        "lastTransitionTime": Utc::now(),
                        "reason": "ResetFailed",
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
    reset: &KafkaOffsetReset,
    client: &Client,
    namespace: &str,
) -> Result<Action> {
    let name = reset.name_any();
    let api: Api<KafkaOffsetReset> = Api::namespaced(client.clone(), namespace);

    // TODO: Validate consumer groups exist
    // TODO: Validate target offsets are valid

    let status = json!({
        "status": {
            "phase": "Completed",
            "message": "Dry run validation passed",
            "groupsTotal": reset.spec.consumer_groups.len(),
            "observedGeneration": reset.metadata.generation,
            "conditions": [{
                "type": "Ready",
                "status": "True",
                "lastTransitionTime": Utc::now(),
                "reason": "DryRunPassed",
                "message": "Offset reset validation completed successfully"
            }]
        }
    });
    api.patch_status(&name, &PatchParams::apply("kafka-backup-operator"), &Patch::Merge(status))
        .await?;

    Ok(Action::await_change())
}

/// Internal reset execution result
struct ResetResult {
    groups_reset: u32,
    groups_failed: u32,
    snapshot_id: Option<String>,
    snapshot_path: Option<String>,
    group_results: Vec<serde_json::Value>,
}

/// Execute the actual offset reset (placeholder for kafka-backup-core integration)
async fn execute_reset_internal(
    reset: &KafkaOffsetReset,
    client: &Client,
    namespace: &str,
) -> Result<ResetResult> {
    // TODO: Implement actual offset reset using kafka-backup-core library

    // 1. Build configuration from CRD spec
    // let config = crate::adapters::build_offset_reset_config(reset, client, namespace).await?;

    // 2. Execute offset reset for each group in parallel (with parallelism limit)
    // let results = kafka_backup_core::reset_offsets(config).await?;

    Ok(ResetResult {
        groups_reset: reset.spec.consumer_groups.len() as u32,
        groups_failed: 0,
        snapshot_id: None,
        snapshot_path: None,
        group_results: vec![],
    })
}

/// Update status to Failed
pub async fn update_status_failed(
    reset: &KafkaOffsetReset,
    client: &Client,
    namespace: &str,
    error_message: &str,
) -> Result<()> {
    let name = reset.name_any();
    let api: Api<KafkaOffsetReset> = Api::namespaced(client.clone(), namespace);

    let status = json!({
        "status": {
            "phase": "Failed",
            "message": error_message,
            "observedGeneration": reset.metadata.generation,
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
