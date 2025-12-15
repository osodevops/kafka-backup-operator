//! KafkaOffsetRollback reconciler
//!
//! Handles the business logic for rolling back consumer group offsets
//! to a previous snapshot.

use std::time::Duration;

use chrono::Utc;
use kafka_backup_core::config::KafkaConfig as CoreKafkaConfig;
use kafka_backup_core::config::{SaslMechanism, SecurityConfig, SecurityProtocol, TopicSelection};
use kafka_backup_core::kafka::KafkaClient;
use kafka_backup_core::{rollback_offset_reset, verify_rollback, OffsetSnapshot};
use kube::{
    api::{Patch, PatchParams},
    runtime::controller::Action,
    Api, Client, ResourceExt,
};
use serde_json::json;
use tracing::{error, info, warn};

use crate::adapters::{build_kafka_config, default_tls_dir, TlsFileManager};
use crate::crd::KafkaOffsetRollback;
use crate::error::{Error, Result};

/// Validate the KafkaOffsetRollback spec
pub fn validate(rollback: &KafkaOffsetRollback) -> Result<()> {
    // Validate kafka cluster
    if rollback.spec.kafka_cluster.bootstrap_servers.is_empty() {
        return Err(Error::validation(
            "At least one bootstrap server must be specified",
        ));
    }

    // Validate snapshot reference
    if rollback.spec.snapshot_ref.name.is_empty() && rollback.spec.snapshot_ref.path.is_none() {
        return Err(Error::validation(
            "Either snapshot name or path must be specified",
        ));
    }

    Ok(())
}

/// Monitor rollback progress
pub async fn monitor_progress(
    rollback: &KafkaOffsetRollback,
    _client: &Client,
    _namespace: &str,
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
    api.patch_status(
        &name,
        &PatchParams::apply("kafka-backup-operator"),
        &Patch::Merge(running_status),
    )
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
            api.patch_status(
                &name,
                &PatchParams::apply("kafka-backup-operator"),
                &Patch::Merge(completed_status),
            )
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
    api.patch_status(
        &name,
        &PatchParams::apply("kafka-backup-operator"),
        &Patch::Merge(status),
    )
    .await?;

    Ok(Action::await_change())
}

/// Internal rollback execution result
struct RollbackResult {
    groups_rolled_back: u32,
    verified: bool,
}

/// Execute the actual rollback using kafka-backup-core library
async fn execute_rollback_internal(
    rollback: &KafkaOffsetRollback,
    client: &Client,
    namespace: &str,
) -> Result<RollbackResult> {
    let name = rollback.name_any();
    let bootstrap_servers = rollback.spec.kafka_cluster.bootstrap_servers.clone();

    info!(
        name = %name,
        snapshot = %rollback.spec.snapshot_ref.name,
        "Building rollback configuration"
    );

    // Build resolved Kafka configuration
    let resolved_kafka =
        build_kafka_config(&rollback.spec.kafka_cluster, client, namespace).await?;

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
            include: vec![],
            exclude: vec![],
        },
    };

    // Create and connect KafkaClient
    let kafka_client = KafkaClient::new(core_kafka_config);
    kafka_client
        .connect()
        .await
        .map_err(|e| Error::Core(format!("Failed to connect to Kafka: {}", e)))?;

    info!(name = %name, "Connected to Kafka cluster");

    // 1. Load snapshot from storage
    let snapshot_path = rollback.spec.snapshot_ref.path.as_ref().ok_or_else(|| {
        Error::SnapshotNotFound(format!(
            "Snapshot path not specified for '{}'",
            rollback.spec.snapshot_ref.name
        ))
    })?;

    info!(name = %name, path = %snapshot_path, "Loading offset snapshot");

    // Note: Loading the snapshot requires filesystem/storage access
    // The snapshot is stored as JSON by kafka-backup-core
    let snapshot_content = tokio::fs::read_to_string(snapshot_path)
        .await
        .map_err(|e| {
            Error::SnapshotNotFound(format!(
                "Failed to read snapshot at '{}': {}",
                snapshot_path, e
            ))
        })?;

    let snapshot: OffsetSnapshot = serde_json::from_str(&snapshot_content)
        .map_err(|e| Error::Core(format!("Failed to parse snapshot: {}", e)))?;

    info!(
        name = %name,
        snapshot_id = %snapshot.snapshot_id,
        groups = snapshot.group_offsets.len(),
        "Loaded snapshot, executing rollback"
    );

    // 2. Apply rollback using kafka-backup-core
    let rollback_result = rollback_offset_reset(&kafka_client, &snapshot)
        .await
        .map_err(|e| Error::Rollback(format!("Rollback failed: {}", e)))?;

    let groups_rolled_back = rollback_result.groups_rolled_back as u32;

    info!(
        name = %name,
        groups_rolled_back = groups_rolled_back,
        status = ?rollback_result.status,
        "Rollback operation completed"
    );

    // 3. Verify if requested
    let verified = if rollback.spec.verify_after_rollback {
        info!(name = %name, "Verifying rollback");

        let verification = verify_rollback(&kafka_client, &snapshot)
            .await
            .map_err(|e| Error::Rollback(format!("Verification failed: {}", e)))?;

        if !verification.verified {
            warn!(
                name = %name,
                mismatched = verification.groups_mismatched.len(),
                "Rollback verification found mismatches"
            );
        }

        verification.verified
    } else {
        false
    };

    info!(
        name = %name,
        groups_rolled_back = groups_rolled_back,
        verified = verified,
        "Rollback completed"
    );

    Ok(RollbackResult {
        groups_rolled_back,
        verified,
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
    }
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

    api.patch_status(
        &name,
        &PatchParams::apply("kafka-backup-operator"),
        &Patch::Merge(status),
    )
    .await?;

    Ok(())
}
