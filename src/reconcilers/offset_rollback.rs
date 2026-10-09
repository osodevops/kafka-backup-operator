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

use crate::adapters::{
    build_kafka_config, default_tls_dir, to_core_connection_config, TlsFileManager,
};
use crate::crd::{KafkaOffsetRollback, VerificationResult};
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

    // Validate TLS configuration: SSL/SASL_SSL requires at least one TLS secret
    let protocol = rollback.spec.kafka_cluster.security_protocol.to_uppercase();
    if (protocol == "SSL" || protocol == "SASL_SSL")
        && rollback.spec.kafka_cluster.tls_secret.is_none()
        && rollback.spec.kafka_cluster.ca_secret.is_none()
    {
        return Err(Error::validation(
            "securityProtocol SSL/SASL_SSL requires either tlsSecret or caSecret to be configured",
        ));
    }

    if let Some(connection) = &rollback.spec.kafka_cluster.connection {
        if connection.connections_per_broker == 0 {
            return Err(Error::validation(
                "kafkaCluster.connection.connectionsPerBroker must be greater than 0",
            ));
        }
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
            "startTime": Utc::now(),
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

            let finished = finished_status(&result, rollback.metadata.generation);
            api.patch_status(
                &name,
                &PatchParams::apply("kafka-backup-operator"),
                &Patch::Merge(finished),
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

/// Status patch for a rollback that ran to the end: `Completed`, or `Failed`
/// when verification read back offsets that don't match the snapshot.
///
/// Only fields in the CRD's status schema: the API server prunes anything
/// else, which is how `verified` and `duration` used to vanish.
fn finished_status(result: &RollbackResult, generation: Option<i64>) -> serde_json::Value {
    let groups = result.groups_rolled_back;
    let (phase, ready, reason, message) = match &result.verification {
        None => (
            "Completed",
            "True",
            "RollbackSucceeded",
            format!("Rolled back {groups} groups (not verified: verifyAfterRollback is false)"),
        ),
        Some(v) if v.verified => (
            "Completed",
            "True",
            "RollbackSucceeded",
            format!("Rolled back {groups} groups; offsets verified against the snapshot"),
        ),
        Some(v) => (
            "Failed",
            "False",
            "VerificationFailed",
            format!(
                "Rolled back {groups} groups, but {} don't match the snapshot after the \
                 rollback (a consumer may still be committing): {}",
                v.groups_mismatched.len(),
                v.groups_mismatched.join(", ")
            ),
        ),
    };
    let verification = result.verification.as_ref().map(|v| VerificationResult {
        all_matched: v.verified,
        total_groups: v.groups_verified.len() + v.groups_mismatched.len(),
        matched_groups: v.groups_verified.len(),
        mismatched_groups: v.groups_mismatched.clone(),
    });

    json!({
        "status": {
            "phase": phase,
            "message": message,
            "groupsRolledBack": groups,
            "completionTime": Utc::now(),
            // Explicit null when skipped clears a result from an earlier run.
            "verification": verification,
            "observedGeneration": generation,
            "conditions": [{
                "type": "Ready",
                "status": ready,
                "lastTransitionTime": Utc::now(),
                "reason": reason,
                "message": message
            }]
        }
    })
}

/// Internal rollback execution result
struct RollbackResult {
    groups_rolled_back: u32,
    /// `None` when `verifyAfterRollback` is false.
    verification: Option<kafka_backup_core::VerificationResult>,
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
        connection: to_core_connection_config(&resolved_kafka),
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
    ensure_rollback_succeeded(&rollback_result)?;

    // 3. Verify if requested
    let verification = if rollback.spec.verify_after_rollback {
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

        Some(verification)
    } else {
        None
    };

    info!(
        name = %name,
        groups_rolled_back = groups_rolled_back,
        verified = ?verification.as_ref().map(|v| v.verified),
        "Rollback completed"
    );

    Ok(RollbackResult {
        groups_rolled_back,
        verification,
    })
}

/// Fail unless every group in the snapshot was rolled back.
///
/// `rollback_offset_reset` reports per-group commit failures in its result
/// rather than as an error, so the result has to be checked (#224).
fn ensure_rollback_succeeded(result: &kafka_backup_core::RollbackResult) -> Result<()> {
    if result.status == kafka_backup_core::RollbackStatus::Success {
        return Ok(());
    }
    Err(Error::Rollback(format!(
        "{} of {} groups failed to roll back: {}",
        result.groups_failed,
        result.groups_rolled_back + result.groups_failed,
        result.errors.join("; ")
    )))
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

#[cfg(test)]
mod issue224_tests {
    use super::*;
    use kafka_backup_core::{RollbackResult as CoreRollbackResult, RollbackStatus};

    fn core_result(
        status: RollbackStatus,
        rolled_back: usize,
        failed: usize,
    ) -> CoreRollbackResult {
        CoreRollbackResult {
            status,
            snapshot_id: "snap".to_string(),
            groups_rolled_back: rolled_back,
            groups_failed: failed,
            offsets_restored: rolled_back as u64,
            errors: (0..failed)
                .map(|i| format!("g{i}:orders:0 - error code 16"))
                .collect(),
            duration_ms: 1,
        }
    }

    #[test]
    fn successful_rollback_is_accepted() {
        assert!(ensure_rollback_succeeded(&core_result(RollbackStatus::Success, 3, 0)).is_ok());
    }

    #[test]
    fn failed_rollback_fails_the_resource() {
        // Before #224 every commit hit NOT_COORDINATOR, core reported Failed,
        // and the resource still went to Completed ("Rolled back 0 groups").
        let err = ensure_rollback_succeeded(&core_result(RollbackStatus::Failed, 0, 9))
            .expect_err("a failed rollback must not complete");
        let message = err.to_string();
        assert!(message.contains("9"), "{message}");
        assert!(message.contains("error code 16"), "{message}");
    }

    #[test]
    fn partial_rollback_fails_the_resource() {
        let err = ensure_rollback_succeeded(&core_result(RollbackStatus::PartialSuccess, 2, 1))
            .expect_err("a partial rollback must not complete");
        assert!(err.to_string().contains("1 of 3"), "{err}");
    }
}

#[cfg(test)]
mod verification_status_tests {
    use super::*;
    use kafka_backup_core::VerificationResult as CoreVerification;
    use kube::CustomResourceExt;
    use serde_json::Value;

    fn verification(matched: &[&str], mismatched: &[&str]) -> CoreVerification {
        CoreVerification {
            verified: mismatched.is_empty(),
            groups_verified: matched.iter().map(|g| g.to_string()).collect(),
            groups_mismatched: mismatched.iter().map(|g| g.to_string()).collect(),
            mismatches: vec![],
        }
    }

    fn result(groups: u32, verification: Option<CoreVerification>) -> RollbackResult {
        RollbackResult {
            groups_rolled_back: groups,
            verification,
        }
    }

    fn status(result: &RollbackResult) -> Value {
        finished_status(result, Some(1))["status"].clone()
    }

    fn outcomes() -> Vec<(&'static str, RollbackResult)> {
        vec![
            ("verified", result(2, Some(verification(&["a", "b"], &[])))),
            ("mismatch", result(2, Some(verification(&["a"], &["b"])))),
            ("skipped", result(2, None)),
        ]
    }

    /// Every field in `value` must exist in `schema`: the API server prunes
    /// unknown status fields, so a field missing from the CRD never reaches
    /// the user (`verified` and `duration` were dropped this way).
    fn assert_in_schema(value: &Value, schema: &Value, path: &str) {
        match value {
            Value::Object(fields) => {
                let Some(props) = schema.get("properties").and_then(Value::as_object) else {
                    return;
                };
                for (key, v) in fields {
                    let field = format!("{path}.{key}");
                    let sub = props
                        .get(key)
                        .unwrap_or_else(|| panic!("{field} is not in the CRD schema"));
                    assert_in_schema(v, sub, &field);
                }
            }
            Value::Array(items) => {
                if let Some(item_schema) = schema.get("items") {
                    for item in items {
                        assert_in_schema(item, item_schema, &format!("{path}[]"));
                    }
                }
            }
            _ => {}
        }
    }

    #[test]
    fn status_only_uses_fields_in_the_crd_schema() {
        let crd = serde_json::to_value(KafkaOffsetRollback::crd()).unwrap();
        let schema =
            &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["status"];
        for (outcome, result) in outcomes() {
            assert_in_schema(&status(&result), schema, &format!("{outcome}: status"));
        }
    }

    #[test]
    fn verified_rollback_reports_the_verification() {
        let status = status(&result(2, Some(verification(&["a", "b"], &[]))));
        assert_eq!(status["phase"], "Completed");
        assert_eq!(status["conditions"][0]["status"], "True");
        assert_eq!(status["verification"]["allMatched"], true);
        assert_eq!(status["verification"]["totalGroups"], 2);
        assert_eq!(status["verification"]["matchedGroups"], 2);
    }

    #[test]
    fn verification_mismatch_fails_the_resource() {
        // The commit succeeded but the offsets read back don't match the
        // snapshot (e.g. a live consumer committed over the rollback). This
        // used to be "Completed" / Ready=True with the result pruned away.
        let status = status(&result(2, Some(verification(&["a"], &["b"]))));
        assert_eq!(status["phase"], "Failed");
        assert_eq!(status["conditions"][0]["status"], "False");
        assert_eq!(status["conditions"][0]["reason"], "VerificationFailed");
        assert_eq!(status["verification"]["allMatched"], false);
        assert_eq!(status["verification"]["matchedGroups"], 1);
        assert_eq!(
            status["verification"]["mismatchedGroups"],
            serde_json::json!(["b"])
        );
        let message = status["message"].as_str().unwrap();
        assert!(message.contains("b"), "{message}");
    }

    #[test]
    fn skipped_verification_clears_the_verification_block() {
        let status = status(&result(2, None));
        assert_eq!(status["phase"], "Completed");
        assert_eq!(status["conditions"][0]["status"], "True");
        // Explicit null: a merge patch removes a result left by an earlier run.
        assert_eq!(status.get("verification"), Some(&Value::Null));
        let message = status["message"].as_str().unwrap();
        assert!(message.contains("not verified"), "{message}");
    }
}
