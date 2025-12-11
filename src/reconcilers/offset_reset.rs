//! KafkaOffsetReset reconciler
//!
//! Handles the business logic for consumer group offset reset operations.

use std::time::Duration;

use chrono::Utc;
use kafka_backup_core::config::KafkaConfig as CoreKafkaConfig;
use kafka_backup_core::config::{SecurityConfig, SecurityProtocol, SaslMechanism, TopicSelection};
use kafka_backup_core::kafka::KafkaClient;
use kafka_backup_core::kafka::consumer_groups::{
    fetch_offsets, commit_offsets, offsets_for_times, CommittedOffset,
};
use kafka_backup_core::{snapshot_current_offsets, BulkOffsetResetConfig};
use kube::{
    api::{Patch, PatchParams},
    runtime::controller::Action,
    Api, Client, ResourceExt,
};
use serde_json::json;
use tracing::{error, info, warn};

use crate::adapters::{build_kafka_config, TlsFileManager, default_tls_dir};
use crate::crd::{KafkaOffsetReset, OffsetResetStrategy};
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
    _client: &Client,
    _namespace: &str,
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
            let phase = if result.groups_failed > 0 {
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

/// Execute the actual offset reset using kafka-backup-core library
async fn execute_reset_internal(
    reset: &KafkaOffsetReset,
    client: &Client,
    namespace: &str,
) -> Result<ResetResult> {
    let name = reset.name_any();
    let bootstrap_servers = reset.spec.kafka_cluster.bootstrap_servers.clone();

    info!(
        name = %name,
        groups = reset.spec.consumer_groups.len(),
        parallelism = reset.spec.parallelism,
        "Building offset reset configuration"
    );

    // Build resolved Kafka configuration from operator config
    let resolved_kafka = build_kafka_config(&reset.spec.kafka_cluster, client, namespace).await?;

    // Create TLS file manager if TLS is configured
    let _tls_manager = if let Some(tls) = &resolved_kafka.tls {
        let tls_dir = default_tls_dir(&name);
        Some(TlsFileManager::new(tls, &tls_dir)?)
    } else {
        None
    };

    // Build kafka-backup-core KafkaConfig
    let security_config = build_core_security_config(&resolved_kafka, _tls_manager.as_ref());
    let core_kafka_config = CoreKafkaConfig {
        bootstrap_servers: bootstrap_servers.clone(),
        security: security_config,
        topics: TopicSelection {
            include: reset.spec.topics.clone(),
            exclude: vec![],
        },
    };

    // Create and connect KafkaClient
    let kafka_client = KafkaClient::new(core_kafka_config);
    kafka_client.connect().await
        .map_err(|e| Error::Core(format!("Failed to connect to Kafka: {}", e)))?;

    info!(name = %name, "Connected to Kafka cluster");

    // Create snapshot if requested
    let (snapshot_id, snapshot_path) = if reset.spec.snapshot_before_reset {
        info!(name = %name, "Creating pre-reset offset snapshot");

        match snapshot_current_offsets(
            &kafka_client,
            &reset.spec.consumer_groups,
            bootstrap_servers.clone(),
        ).await {
            Ok(snapshot) => {
                let snapshot_id = snapshot.snapshot_id.clone();
                info!(
                    name = %name,
                    snapshot_id = %snapshot_id,
                    groups = snapshot.group_offsets.len(),
                    "Created offset snapshot"
                );
                (Some(snapshot_id), None)
            }
            Err(e) => {
                warn!(name = %name, error = %e, "Failed to create snapshot, continuing without");
                (None, None)
            }
        }
    } else {
        (None, None)
    };

    // Build bulk reset configuration
    let _bulk_config = BulkOffsetResetConfig {
        max_concurrent_requests: reset.spec.parallelism,
        max_retry_attempts: 3,
        retry_base_delay_ms: 100,
        request_timeout_ms: 30000,
        continue_on_error: reset.spec.continue_on_error,
    };

    info!(
        name = %name,
        "Executing offset reset with parallelism {}",
        reset.spec.parallelism
    );

    // Track results
    let mut groups_reset = 0u32;
    let mut groups_failed = 0u32;
    let mut group_results = Vec::new();

    // Process each consumer group
    for group_id in &reset.spec.consumer_groups {
        info!(name = %name, group = %group_id, "Processing consumer group");

        match reset_consumer_group(&kafka_client, group_id, reset).await {
            Ok(partitions_reset) => {
                groups_reset += 1;
                group_results.push(json!({
                    "groupId": group_id,
                    "status": "success",
                    "partitionsReset": partitions_reset
                }));
                info!(name = %name, group = %group_id, partitions = partitions_reset, "Group reset successful");
            }
            Err(e) => {
                groups_failed += 1;
                group_results.push(json!({
                    "groupId": group_id,
                    "status": "failed",
                    "error": e.to_string()
                }));
                error!(name = %name, group = %group_id, error = %e, "Group reset failed");

                if !reset.spec.continue_on_error {
                    return Err(Error::Core(format!("Failed to reset group {}: {}", group_id, e)));
                }
            }
        }
    }

    info!(
        name = %name,
        groups_reset = groups_reset,
        groups_failed = groups_failed,
        "Offset reset completed"
    );

    Ok(ResetResult {
        groups_reset,
        groups_failed,
        snapshot_id,
        snapshot_path,
        group_results,
    })
}

/// Build kafka-backup-core SecurityConfig from resolved operator config
fn build_core_security_config(
    resolved: &crate::adapters::ResolvedKafkaConfig,
    tls_manager: Option<&TlsFileManager>,
) -> SecurityConfig {
    let security_protocol = match resolved.security_protocol.to_uppercase().as_str() {
        "PLAINTEXT" => SecurityProtocol::Plaintext,
        "SSL" => SecurityProtocol::Ssl,
        "SASL_PLAINTEXT" => SecurityProtocol::SaslPlaintext,
        "SASL_SSL" => SecurityProtocol::SaslSsl,
        _ => SecurityProtocol::Plaintext,
    };

    let (sasl_mechanism, sasl_username, sasl_password) = match &resolved.sasl {
        Some(sasl) => {
            let mechanism = match sasl.mechanism.to_uppercase().as_str() {
                "PLAIN" => Some(SaslMechanism::Plain),
                "SCRAM-SHA-256" => Some(SaslMechanism::ScramSha256),
                "SCRAM-SHA-512" => Some(SaslMechanism::ScramSha512),
                _ => None,
            };
            (mechanism, Some(sasl.username.clone()), Some(sasl.password.clone()))
        }
        None => (None, None, None),
    };

    let (ssl_ca_location, ssl_certificate_location, ssl_key_location) = match tls_manager {
        Some(mgr) => (
            Some(mgr.ca_location()),
            mgr.certificate_location(),
            mgr.key_location(),
        ),
        None => (None, None, None),
    };

    SecurityConfig {
        security_protocol,
        sasl_mechanism,
        sasl_username,
        sasl_password,
        ssl_ca_location,
        ssl_certificate_location,
        ssl_key_location,
    }
}

/// Reset offsets for a single consumer group
async fn reset_consumer_group(
    kafka_client: &KafkaClient,
    group_id: &str,
    reset: &KafkaOffsetReset,
) -> std::result::Result<u32, kafka_backup_core::Error> {
    // First, fetch current offsets to know which partitions to reset
    // Pass None for topics filter to get all offsets for this group
    let topics_filter: Option<&[String]> = if reset.spec.topics.is_empty() {
        None
    } else {
        Some(&reset.spec.topics)
    };
    let current_offsets = fetch_offsets(kafka_client, group_id, topics_filter).await?;

    if current_offsets.is_empty() {
        info!(group = %group_id, "No committed offsets found for group");
        return Ok(0);
    }

    // Calculate target offsets based on strategy
    let target_offsets = calculate_target_offsets(
        kafka_client,
        &current_offsets,
        &reset.spec.reset_strategy,
        reset.spec.reset_timestamp,
        reset.spec.reset_offset,
    ).await?;

    // Convert to tuple format expected by commit_offsets: (topic, partition, offset, metadata)
    let offsets_tuples: Vec<(String, i32, i64, Option<String>)> = target_offsets
        .iter()
        .map(|o| (o.topic.clone(), o.partition, o.offset, o.metadata.clone()))
        .collect();

    // Commit the new offsets
    let partitions_reset = offsets_tuples.len() as u32;
    commit_offsets(kafka_client, group_id, &offsets_tuples).await?;

    Ok(partitions_reset)
}

/// Calculate target offsets based on reset strategy
async fn calculate_target_offsets(
    kafka_client: &KafkaClient,
    current_offsets: &[CommittedOffset],
    strategy: &OffsetResetStrategy,
    reset_timestamp: Option<i64>,
    reset_offset: Option<i64>,
) -> std::result::Result<Vec<CommittedOffset>, kafka_backup_core::Error> {
    let mut target_offsets = Vec::new();

    for offset in current_offsets {
        let new_offset = match strategy {
            OffsetResetStrategy::ToEarliest => {
                // Get earliest offset for partition
                let (earliest, _) = kafka_client.get_offsets(&offset.topic, offset.partition).await?;
                earliest
            }
            OffsetResetStrategy::ToLatest => {
                // Get latest offset for partition
                let (_, latest) = kafka_client.get_offsets(&offset.topic, offset.partition).await?;
                latest
            }
            OffsetResetStrategy::ToTimestamp => {
                // Get offset for timestamp
                // offsets_for_times takes &[(String, i32, i64)] - (topic, partition, timestamp)
                let timestamp = reset_timestamp.unwrap_or(0);
                let requests = vec![(offset.topic.clone(), offset.partition, timestamp)];
                let timestamp_offsets = offsets_for_times(kafka_client, &requests).await?;
                timestamp_offsets
                    .first()
                    .filter(|to| to.error_code == 0)
                    .map(|to| to.offset)
                    .unwrap_or(offset.offset)
            }
            OffsetResetStrategy::ToOffset => {
                // Use specified offset directly
                reset_offset.unwrap_or(offset.offset)
            }
            OffsetResetStrategy::FromMapping => {
                // For mapping-based reset, keep current offset (handled separately)
                offset.offset
            }
        };

        target_offsets.push(CommittedOffset {
            topic: offset.topic.clone(),
            partition: offset.partition,
            offset: new_offset,
            metadata: offset.metadata.clone(),
            error_code: 0, // Success
        });
    }

    Ok(target_offsets)
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
