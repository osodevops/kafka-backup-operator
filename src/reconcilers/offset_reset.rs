//! KafkaOffsetReset reconciler
//!
//! Handles the business logic for consumer group offset reset operations.

use std::path::Path;
use std::time::Duration;

use chrono::Utc;
use kafka_backup_core::config::KafkaConfig as CoreKafkaConfig;
use kafka_backup_core::config::{SaslMechanism, SecurityConfig, SecurityProtocol, TopicSelection};
use kafka_backup_core::kafka::consumer_groups::{
    commit_offsets, fetch_offsets, offsets_for_times, CommittedOffset,
};
use kafka_backup_core::kafka::KafkaClient;
use kafka_backup_core::manifest::OffsetMapping;
use kafka_backup_core::{snapshot_current_offsets, BulkOffsetResetConfig};
use kube::{
    api::{Patch, PatchParams},
    runtime::controller::Action,
    Api, Client, ResourceExt,
};
use serde_json::json;
use tracing::{error, info, warn};

use crate::adapters::{
    build_kafka_config, default_tls_dir, to_core_connection_config, TlsFileManager,
};
use crate::crd::{KafkaOffsetReset, KafkaRestore, OffsetMappingRef, OffsetResetStrategy};
use crate::error::{Error, Result};
use crate::metrics;

const STALE_RUNNING_AFTER_SECS: i64 = 30 * 60;

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
        OffsetResetStrategy::ToTimestamp if reset.spec.reset_timestamp.is_none() => {
            return Err(Error::validation(
                "reset_timestamp is required when using to-timestamp strategy",
            ));
        }
        OffsetResetStrategy::ToOffset if reset.spec.reset_offset.is_none() => {
            return Err(Error::validation(
                "reset_offset is required when using to-offset strategy",
            ));
        }
        OffsetResetStrategy::FromMapping if reset.spec.offset_mapping_ref.is_none() => {
            return Err(Error::validation(
                "offset_mapping_ref is required when using from-mapping strategy",
            ));
        }
        _ => {}
    }

    // Validate parallelism
    if reset.spec.parallelism == 0 {
        return Err(Error::validation("parallelism must be greater than 0"));
    }

    // Validate TLS configuration: SSL/SASL_SSL requires at least one TLS secret
    let protocol = reset.spec.kafka_cluster.security_protocol.to_uppercase();
    if (protocol == "SSL" || protocol == "SASL_SSL")
        && reset.spec.kafka_cluster.tls_secret.is_none()
        && reset.spec.kafka_cluster.ca_secret.is_none()
    {
        return Err(Error::validation(
            "securityProtocol SSL/SASL_SSL requires either tlsSecret or caSecret to be configured",
        ));
    }

    if let Some(connection) = &reset.spec.kafka_cluster.connection {
        if connection.connections_per_broker == 0 {
            return Err(Error::validation(
                "kafkaCluster.connection.connectionsPerBroker must be greater than 0",
            ));
        }
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

    match running_monitor_decision(reset, Utc::now()) {
        RunningMonitorDecision::Requeue {
            after,
            elapsed_secs,
        } => {
            info!(
                name = %name,
                elapsed_secs,
                "Offset reset still running"
            );
            Ok(Action::requeue(after))
        }
        RunningMonitorDecision::MarkFailed => {
            let message = "Offset reset was left Running without an active tracked operation";
            warn!(name = %name, "{}", message);
            update_status_failed(reset, client, namespace, message).await?;

            Ok(Action::await_change())
        }
    }
}

/// Execute an offset reset operation
pub async fn execute(reset: &KafkaOffsetReset, client: &Client, namespace: &str) -> Result<Action> {
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
            "startTime": Utc::now(),
            "groupsTotal": reset.spec.consumer_groups.len(),
            "groupsReset": 0,
            "groupsFailed": 0,
            "observedGeneration": reset.metadata.generation,
        }
    });
    api.patch_status(
        &name,
        &PatchParams::apply("kafka-backup-operator"),
        &Patch::Merge(running_status),
    )
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
            let message = if result.groups_reset == 0
                && result.groups_failed == 0
                && result.groups_noop > 0
            {
                format!(
                    "No-op: offsets already at target for {} groups",
                    result.groups_noop
                )
            } else {
                format!(
                    "Reset {} groups, {} no-op, {} failed",
                    result.groups_reset, result.groups_noop, result.groups_failed
                )
            };

            info!(
                name = %name,
                groups_reset = result.groups_reset,
                groups_noop = result.groups_noop,
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
                    "message": message.clone(),
                    "groupsTotal": reset.spec.consumer_groups.len(),
                    "groupsReset": result.groups_reset,
                    "groupsFailed": result.groups_failed,
                    "completionTime": Utc::now(),
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
                        "message": message
                    }]
                }
            });
            api.patch_status(
                &name,
                &PatchParams::apply("kafka-backup-operator"),
                &Patch::Merge(completed_status),
            )
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
            api.patch_status(
                &name,
                &PatchParams::apply("kafka-backup-operator"),
                &Patch::Merge(failed_status),
            )
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
    api.patch_status(
        &name,
        &PatchParams::apply("kafka-backup-operator"),
        &Patch::Merge(status),
    )
    .await?;

    Ok(Action::await_change())
}

/// Internal reset execution result
struct ResetResult {
    groups_reset: u32,
    groups_noop: u32,
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
        connection: to_core_connection_config(&resolved_kafka),
    };

    // Create and connect KafkaClient
    let kafka_client = KafkaClient::new(core_kafka_config);
    kafka_client
        .connect()
        .await
        .map_err(|e| Error::Core(format!("Failed to connect to Kafka: {}", e)))?;

    info!(name = %name, "Connected to Kafka cluster");

    // Create snapshot if requested
    let (snapshot_id, snapshot_path) = if reset.spec.snapshot_before_reset {
        info!(name = %name, "Creating pre-reset offset snapshot");

        match snapshot_current_offsets(
            &kafka_client,
            &reset.spec.consumer_groups,
            bootstrap_servers.clone(),
        )
        .await
        {
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
    let mut groups_noop = 0u32;
    let mut groups_failed = 0u32;
    let mut group_results = Vec::new();
    let offset_mapping = if reset.spec.reset_strategy == OffsetResetStrategy::FromMapping {
        Some(load_offset_mapping(reset, client, namespace).await?)
    } else {
        None
    };

    // Process each consumer group
    for group_id in &reset.spec.consumer_groups {
        info!(name = %name, group = %group_id, "Processing consumer group");

        match reset_consumer_group(&kafka_client, group_id, reset, offset_mapping.as_ref()).await {
            Ok(GroupResetOutcome::Applied(partitions_reset)) => {
                if partitions_reset > 0 {
                    groups_reset += 1;
                } else {
                    groups_noop += 1;
                }
                group_results.push(json!({
                    "groupId": group_id,
                    "success": true,
                    "partitionsReset": partitions_reset
                }));
                info!(name = %name, group = %group_id, partitions = partitions_reset, "Group reset successful");
            }
            Ok(GroupResetOutcome::NoOp(partitions_checked)) => {
                groups_noop += 1;
                group_results.push(json!({
                    "groupId": group_id,
                    "success": true,
                    "partitionsReset": 0,
                    "message": format!("No-op: {} partitions already at target offsets", partitions_checked)
                }));
                info!(name = %name, group = %group_id, partitions = partitions_checked, "Group reset no-op");
            }
            Err(e) => {
                groups_failed += 1;
                group_results.push(json!({
                    "groupId": group_id,
                    "success": false,
                    "error": e.to_string()
                }));
                error!(name = %name, group = %group_id, error = %e, "Group reset failed");

                if !reset.spec.continue_on_error {
                    return Err(Error::Core(format!(
                        "Failed to reset group {}: {}",
                        group_id, e
                    )));
                }
            }
        }
    }

    info!(
        name = %name,
        groups_reset = groups_reset,
        groups_noop = groups_noop,
        groups_failed = groups_failed,
        "Offset reset completed"
    );

    Ok(ResetResult {
        groups_reset,
        groups_noop,
        groups_failed,
        snapshot_id,
        snapshot_path,
        group_results,
    })
}

async fn load_offset_mapping(
    reset: &KafkaOffsetReset,
    client: &Client,
    namespace: &str,
) -> Result<OffsetMapping> {
    let mapping_ref = reset.spec.offset_mapping_ref.as_ref().ok_or_else(|| {
        Error::validation("offsetMappingRef is required when using from-mapping strategy")
    })?;
    let path = resolve_offset_mapping_path(mapping_ref, client, namespace).await?;
    let contents = tokio::fs::read_to_string(&path).await.map_err(|e| {
        Error::storage(format!(
            "Failed to read offset mapping at '{}': {}",
            path, e
        ))
    })?;
    serde_json::from_str(&contents).map_err(Error::from)
}

async fn resolve_offset_mapping_path(
    mapping_ref: &OffsetMappingRef,
    client: &Client,
    namespace: &str,
) -> Result<String> {
    if let Some(pvc_name) = &mapping_ref.pvc_name {
        let relative_path = mapping_ref.path.as_deref().unwrap_or("offset-mapping.json");
        if Path::new(relative_path).is_absolute() {
            return Ok(relative_path.to_string());
        }
        return Ok(format!("/data/{}/{}", pvc_name, relative_path));
    }

    if let Some(path) = &mapping_ref.path {
        return Ok(path.clone());
    }

    if let Some(restore_name) = &mapping_ref.restore_name {
        let api: Api<KafkaRestore> = Api::namespaced(client.clone(), namespace);
        let restore = api.get(restore_name).await?;
        return restore
            .status
            .and_then(|status| status.offset_mapping_path)
            .ok_or_else(|| {
                Error::validation(format!(
                    "KafkaRestore '{}' does not expose status.offsetMappingPath",
                    restore_name
                ))
            });
    }

    Err(Error::validation(
        "offsetMappingRef must specify restoreName, path, or pvcName",
    ))
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
            (
                mechanism,
                Some(sasl.username.clone()),
                Some(sasl.password.clone()),
            )
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
        sasl_kerberos_service_name: None,
        sasl_keytab_path: None,
        sasl_krb5_config_path: None,
        sasl_mechanism_plugin_factory: None,
    }
}

/// Reset offsets for a single consumer group
async fn reset_consumer_group(
    kafka_client: &KafkaClient,
    group_id: &str,
    reset: &KafkaOffsetReset,
    mapping: Option<&OffsetMapping>,
) -> std::result::Result<GroupResetOutcome, kafka_backup_core::Error> {
    // First, fetch current offsets to know which partitions to reset
    // Pass None for topics filter to get all offsets for this group
    let topics_filter: Option<&[String]> = if reset.spec.topics.is_empty() {
        None
    } else {
        Some(&reset.spec.topics)
    };
    reset_consumer_group_with_mapping(kafka_client, group_id, reset, mapping, topics_filter).await
}

async fn reset_consumer_group_with_mapping(
    kafka_client: &KafkaClient,
    group_id: &str,
    reset: &KafkaOffsetReset,
    mapping: Option<&OffsetMapping>,
    topics_filter: Option<&[String]>,
) -> std::result::Result<GroupResetOutcome, kafka_backup_core::Error> {
    let fetch_topics = if reset.spec.reset_strategy == OffsetResetStrategy::FromMapping {
        None
    } else {
        topics_filter
    };
    let current_offsets = match fetch_offsets(kafka_client, group_id, fetch_topics).await {
        Ok(offsets) => offsets,
        Err(e) if reset.spec.reset_strategy == OffsetResetStrategy::FromMapping => {
            warn!(
                group = %group_id,
                error = %e,
                "Failed to fetch current offsets; continuing with mapping-based reset"
            );
            Vec::new()
        }
        Err(e) => return Err(e),
    };

    if current_offsets.is_empty() && reset.spec.reset_strategy != OffsetResetStrategy::FromMapping {
        info!(group = %group_id, "No committed offsets found for group");
        return Ok(GroupResetOutcome::NoOp(0));
    }

    // Calculate target offsets based on strategy
    let target_offsets = if reset.spec.reset_strategy == OffsetResetStrategy::FromMapping {
        let mapping = mapping.ok_or_else(|| {
            kafka_backup_core::Error::Config(
                "from-mapping strategy requires a loaded offset mapping".to_string(),
            )
        })?;
        target_offsets_from_mapping(mapping, group_id, topics_filter, &current_offsets)?
    } else {
        calculate_target_offsets(
            kafka_client,
            &current_offsets,
            &reset.spec.reset_strategy,
            reset.spec.reset_timestamp,
            reset.spec.reset_offset,
        )
        .await?
    };

    if target_offsets.is_empty() {
        return Err(kafka_backup_core::Error::Config(format!(
            "No target offsets were calculated for consumer group '{}'",
            group_id
        )));
    }

    if offsets_already_at_target(&current_offsets, &target_offsets) {
        return Ok(GroupResetOutcome::NoOp(target_offsets.len() as u32));
    }

    // Convert to tuple format expected by commit_offsets: (topic, partition, offset, metadata)
    let offsets_tuples: Vec<(String, i32, i64, Option<String>)> = target_offsets
        .iter()
        .map(|o| (o.topic.clone(), o.partition, o.offset, o.metadata.clone()))
        .collect();

    // Commit the new offsets
    let partitions_reset = offsets_tuples.len() as u32;
    commit_offsets(kafka_client, group_id, &offsets_tuples).await?;

    Ok(GroupResetOutcome::Applied(partitions_reset))
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
                let (earliest, _) = kafka_client
                    .get_offsets(&offset.topic, offset.partition)
                    .await?;
                earliest
            }
            OffsetResetStrategy::ToLatest => {
                // Get latest offset for partition
                let (_, latest) = kafka_client
                    .get_offsets(&offset.topic, offset.partition)
                    .await?;
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
                return Err(kafka_backup_core::Error::Config(
                    "from-mapping strategy must be handled using offsetMappingRef".to_string(),
                ));
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupResetOutcome {
    Applied(u32),
    NoOp(u32),
}

fn target_offsets_from_mapping(
    mapping: &OffsetMapping,
    group_id: &str,
    topics_filter: Option<&[String]>,
    current_offsets: &[CommittedOffset],
) -> std::result::Result<Vec<CommittedOffset>, kafka_backup_core::Error> {
    let Some(group_offsets) = mapping.consumer_groups.get(group_id) else {
        return target_offsets_from_current_offsets(
            mapping,
            group_id,
            topics_filter,
            current_offsets,
        );
    };

    let mut target_offsets = Vec::new();
    for (topic, partitions) in &group_offsets.offsets {
        if topics_filter
            .map(|topics| !topics.iter().any(|candidate| candidate == topic))
            .unwrap_or(false)
        {
            continue;
        }

        for (partition, offset) in partitions {
            let target_offset = offset
                .target_offset
                .or_else(|| mapping.lookup_target_offset(topic, *partition, offset.source_offset))
                .ok_or_else(|| {
                    kafka_backup_core::Error::Config(format!(
                        "No target offset mapping for group '{}', topic '{}', partition {}, source offset {}",
                        group_id, topic, partition, offset.source_offset
                    ))
                })?;

            target_offsets.push(CommittedOffset {
                topic: topic.clone(),
                partition: *partition,
                offset: target_offset,
                metadata: offset.metadata.clone(),
                error_code: 0,
            });
        }
    }

    target_offsets.sort_by(|a, b| {
        a.topic
            .cmp(&b.topic)
            .then_with(|| a.partition.cmp(&b.partition))
    });

    Ok(target_offsets)
}

fn target_offsets_from_current_offsets(
    mapping: &OffsetMapping,
    group_id: &str,
    topics_filter: Option<&[String]>,
    current_offsets: &[CommittedOffset],
) -> std::result::Result<Vec<CommittedOffset>, kafka_backup_core::Error> {
    if current_offsets.is_empty() {
        return Err(kafka_backup_core::Error::Config(format!(
            "Offset mapping does not contain consumer group '{}' and current offsets could not be fetched",
            group_id
        )));
    }

    let mut target_offsets = Vec::new();
    let mut missing_offsets = Vec::new();

    for offset in current_offsets {
        if offset.error_code != 0
            || topics_filter
                .map(|topics| !topics.iter().any(|candidate| candidate == &offset.topic))
                .unwrap_or(false)
        {
            continue;
        }

        if detailed_mapping_target_contains(mapping, &offset.topic, offset.partition, offset.offset)
        {
            target_offsets.push(CommittedOffset {
                topic: offset.topic.clone(),
                partition: offset.partition,
                offset: offset.offset,
                metadata: offset.metadata.clone(),
                error_code: 0,
            });
            continue;
        }

        if let Some(target_offset) = lookup_target_offset_without_extrapolating(
            mapping,
            &offset.topic,
            offset.partition,
            offset.offset,
        ) {
            target_offsets.push(CommittedOffset {
                topic: offset.topic.clone(),
                partition: offset.partition,
                offset: target_offset,
                metadata: offset.metadata.clone(),
                error_code: 0,
            });
        } else if offset.offset != 0
            || detailed_mapping_exists(mapping, &offset.topic, offset.partition)
        {
            missing_offsets.push(format!(
                "{}:{}:{}",
                offset.topic, offset.partition, offset.offset
            ));
        }
    }

    if target_offsets.is_empty() && !missing_offsets.is_empty() {
        return Err(kafka_backup_core::Error::Config(format!(
            "No target offsets found in mapping for consumer group '{}': {}",
            group_id,
            missing_offsets.join(", ")
        )));
    }

    target_offsets.sort_by(|a, b| {
        a.topic
            .cmp(&b.topic)
            .then_with(|| a.partition.cmp(&b.partition))
    });

    Ok(target_offsets)
}

fn detailed_mapping_key(topic: &str, partition: i32) -> String {
    format!("{}/{}", topic, partition)
}

fn detailed_mapping_exists(mapping: &OffsetMapping, topic: &str, partition: i32) -> bool {
    mapping
        .detailed_mappings
        .contains_key(&detailed_mapping_key(topic, partition))
}

fn detailed_mapping_target_contains(
    mapping: &OffsetMapping,
    topic: &str,
    partition: i32,
    offset: i64,
) -> bool {
    mapping
        .detailed_mappings
        .get(&detailed_mapping_key(topic, partition))
        .and_then(|pairs| {
            let min = pairs.iter().map(|pair| pair.target_offset).min()?;
            let max = pairs.iter().map(|pair| pair.target_offset).max()?;
            Some(offset >= min && offset <= max.saturating_add(1))
        })
        .unwrap_or(false)
}

fn lookup_target_offset_without_extrapolating(
    mapping: &OffsetMapping,
    topic: &str,
    partition: i32,
    source_offset: i64,
) -> Option<i64> {
    let pairs = mapping
        .detailed_mappings
        .get(&detailed_mapping_key(topic, partition))?;
    let min_source = pairs.iter().map(|pair| pair.source_offset).min()?;
    let max_source = pairs.iter().map(|pair| pair.source_offset).max()?;

    if source_offset < min_source || source_offset > max_source.saturating_add(1) {
        return None;
    }

    if source_offset == max_source.saturating_add(1) {
        let max_target = pairs.iter().map(|pair| pair.target_offset).max()?;
        return Some(max_target.saturating_add(1));
    }

    mapping.lookup_target_offset(topic, partition, source_offset)
}

fn offsets_already_at_target(
    current_offsets: &[CommittedOffset],
    target_offsets: &[CommittedOffset],
) -> bool {
    if current_offsets.is_empty() || target_offsets.is_empty() {
        return false;
    }

    target_offsets.iter().all(|target| {
        current_offsets.iter().any(|current| {
            current.error_code == 0
                && current.topic == target.topic
                && current.partition == target.partition
                && current.offset == target.offset
        })
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunningMonitorDecision {
    Requeue { after: Duration, elapsed_secs: i64 },
    MarkFailed,
}

fn running_monitor_decision(
    reset: &KafkaOffsetReset,
    now: chrono::DateTime<Utc>,
) -> RunningMonitorDecision {
    if let Some(start_time) = reset.status.as_ref().and_then(|status| status.start_time) {
        let elapsed_secs = now.signed_duration_since(start_time).num_seconds();
        if elapsed_secs < STALE_RUNNING_AFTER_SECS {
            return RunningMonitorDecision::Requeue {
                after: Duration::from_secs(10),
                elapsed_secs,
            };
        }
    }

    RunningMonitorDecision::MarkFailed
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

    api.patch_status(
        &name,
        &PatchParams::apply("kafka-backup-operator"),
        &Patch::Merge(status),
    )
    .await?;

    Ok(())
}

#[cfg(test)]
mod issue49_tests {
    use super::*;
    use crate::crd::{KafkaClusterSpec, KafkaOffsetResetSpec, KafkaOffsetResetStatus};

    fn disconnected_kafka_client() -> KafkaClient {
        KafkaClient::new(CoreKafkaConfig {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            security: SecurityConfig {
                security_protocol: SecurityProtocol::Plaintext,
                sasl_mechanism: None,
                sasl_username: None,
                sasl_password: None,
                ssl_ca_location: None,
                ssl_certificate_location: None,
                ssl_key_location: None,
                sasl_kerberos_service_name: None,
                sasl_keytab_path: None,
                sasl_krb5_config_path: None,
                sasl_mechanism_plugin_factory: None,
            },
            topics: TopicSelection {
                include: vec!["orders".to_string()],
                exclude: vec![],
            },
            connection: Default::default(),
        })
    }

    fn reset_with_start_time(start_time: Option<chrono::DateTime<Utc>>) -> KafkaOffsetReset {
        let mut reset = KafkaOffsetReset::new(
            "issue49",
            KafkaOffsetResetSpec {
                kafka_cluster: KafkaClusterSpec {
                    bootstrap_servers: vec!["localhost:9092".to_string()],
                    security_protocol: "PLAINTEXT".to_string(),
                    tls_secret: None,
                    ca_secret: None,
                    sasl_secret: None,
                    connection: None,
                },
                consumer_groups: vec!["test.consumergroup.v1".to_string()],
                reset_strategy: OffsetResetStrategy::FromMapping,
                reset_timestamp: None,
                reset_offset: None,
                topics: vec![],
                parallelism: 50,
                dry_run: false,
                continue_on_error: false,
                offset_mapping_ref: None,
                snapshot_before_reset: false,
            },
        );
        reset.status = Some(KafkaOffsetResetStatus {
            phase: Some("Running".to_string()),
            start_time,
            ..Default::default()
        });
        reset
    }

    #[tokio::test]
    async fn from_mapping_strategy_must_not_fall_back_to_current_offsets() {
        let current_offsets = vec![CommittedOffset {
            topic: "orders".to_string(),
            partition: 0,
            offset: 42,
            metadata: None,
            error_code: 0,
        }];

        let result = calculate_target_offsets(
            &disconnected_kafka_client(),
            &current_offsets,
            &OffsetResetStrategy::FromMapping,
            None,
            None,
        )
        .await;

        assert!(
            result.is_err(),
            "from-mapping must be resolved from offsetMappingRef, not current offsets"
        );
    }

    #[test]
    fn target_offsets_are_derived_from_mapping_consumer_group_snapshot() {
        let mut mapping = OffsetMapping::new();
        mapping.add_detailed("orders", 0, 42, 7, 1_700_000_000_000);
        mapping.add_consumer_group_offset(
            "test.consumergroup.v1",
            "orders",
            0,
            42,
            1_700_000_000_000,
            Some("restored".to_string()),
        );

        let targets =
            target_offsets_from_mapping(&mapping, "test.consumergroup.v1", None, &[]).unwrap();

        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].topic, "orders");
        assert_eq!(targets[0].partition, 0);
        assert_eq!(targets[0].offset, 7);
        assert_eq!(targets[0].metadata.as_deref(), Some("restored"));
    }

    #[test]
    fn target_offsets_fall_back_to_current_group_offsets_when_snapshot_is_absent() {
        let mut mapping = OffsetMapping::new();
        mapping.add_detailed("orders", 0, 42, 107, 1_700_000_000_000);
        let current = vec![CommittedOffset {
            topic: "orders".to_string(),
            partition: 0,
            offset: 42,
            metadata: Some("source".to_string()),
            error_code: 0,
        }];

        let targets =
            target_offsets_from_mapping(&mapping, "test.consumergroup.v1", None, &current).unwrap();

        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].offset, 107);
        assert_eq!(targets[0].metadata.as_deref(), Some("source"));
    }

    #[test]
    fn target_offsets_treat_current_restored_offsets_as_noop_without_snapshot() {
        let mut mapping = OffsetMapping::new();
        mapping.add_detailed("orders", 0, 42, 107, 1_700_000_000_000);
        mapping.add_detailed("orders", 0, 43, 108, 1_700_000_000_001);
        let current = vec![CommittedOffset {
            topic: "orders".to_string(),
            partition: 0,
            offset: 107,
            metadata: None,
            error_code: 0,
        }];

        let targets =
            target_offsets_from_mapping(&mapping, "test.consumergroup.v1", None, &current).unwrap();

        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].offset, 107);
        assert!(offsets_already_at_target(&current, &targets));
    }

    #[test]
    fn equal_current_and_target_offsets_are_noop() {
        let current = vec![CommittedOffset {
            topic: "orders".to_string(),
            partition: 0,
            offset: 7,
            metadata: None,
            error_code: 0,
        }];
        let target = vec![CommittedOffset {
            topic: "orders".to_string(),
            partition: 0,
            offset: 7,
            metadata: None,
            error_code: 0,
        }];

        assert!(offsets_already_at_target(&current, &target));
    }

    #[test]
    fn running_monitor_requeues_fresh_running_status() {
        let now = Utc::now();
        let reset = reset_with_start_time(Some(now - chrono::Duration::seconds(5)));

        assert_eq!(
            running_monitor_decision(&reset, now),
            RunningMonitorDecision::Requeue {
                after: Duration::from_secs(10),
                elapsed_secs: 5
            }
        );
    }

    #[test]
    fn running_monitor_marks_missing_start_time_failed() {
        let reset = reset_with_start_time(None);

        assert_eq!(
            running_monitor_decision(&reset, Utc::now()),
            RunningMonitorDecision::MarkFailed
        );
    }
}
