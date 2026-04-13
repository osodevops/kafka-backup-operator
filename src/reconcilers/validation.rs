//! KafkaBackupValidation reconciler
//!
//! Handles the business logic for backup validation operations including:
//! - Spec validation
//! - Backup reference resolution
//! - Validation check execution
//! - Evidence report generation
//! - Notification delivery

use std::str::FromStr;
use std::time::Duration;

use chrono::Utc;
use kube::{
    api::{Patch, PatchParams},
    runtime::controller::Action,
    Api, Client, ResourceExt,
};
use serde_json::json;
use tracing::{error, info, warn};

use crate::adapters::{
    build_validation_config, to_core_validation_config, ResolvedBackupSource, ResolvedStorage,
};
use crate::crd::{KafkaBackup, KafkaBackupValidation};
use crate::error::{Error, Result};
use crate::metrics;

/// Validate the KafkaBackupValidation spec
pub fn validate(validation: &KafkaBackupValidation) -> Result<()> {
    // Validate backup reference
    if validation.spec.backup_ref.name.is_empty() && validation.spec.backup_ref.storage.is_none() {
        return Err(Error::validation(
            "Either backup name or direct storage reference must be specified",
        ));
    }

    // Validate at least one check is enabled
    let checks = &validation.spec.checks;
    let has_message_count = checks.message_count.as_ref().is_some_and(|c| c.enabled);
    let has_offset_range = checks.offset_range.as_ref().is_some_and(|c| c.enabled);
    let has_consumer_group = checks
        .consumer_group_offsets
        .as_ref()
        .is_some_and(|c| c.enabled);
    let has_webhooks = !checks.custom_webhooks.is_empty();

    if !has_message_count && !has_offset_range && !has_consumer_group && !has_webhooks {
        return Err(Error::validation(
            "At least one validation check must be enabled",
        ));
    }

    // Validate consumer group check requires kafka_cluster
    if has_consumer_group && validation.spec.kafka_cluster.is_none() {
        return Err(Error::validation(
            "kafkaCluster is required when consumer group offset check is enabled",
        ));
    }

    if let Some(kafka_cluster) = &validation.spec.kafka_cluster {
        if let Some(connection) = &kafka_cluster.connection {
            if connection.connections_per_broker == 0 {
                return Err(Error::validation(
                    "kafkaCluster.connection.connectionsPerBroker must be greater than 0",
                ));
            }
        }
    }

    // Validate schedule if provided
    if let Some(schedule) = &validation.spec.schedule {
        if cron::Schedule::from_str(schedule).is_err() {
            return Err(Error::validation(format!(
                "Invalid cron schedule: '{}'",
                schedule
            )));
        }
    }

    // Validate webhook checks have URLs
    for webhook in &checks.custom_webhooks {
        if webhook.url.is_empty() {
            return Err(Error::validation(format!(
                "Webhook check '{}' must have a URL",
                webhook.name
            )));
        }
    }

    // Validate evidence signing has key secret
    if let Some(evidence) = &validation.spec.evidence {
        if let Some(signing) = &evidence.signing {
            if signing.enabled && signing.key_secret.name.is_empty() {
                return Err(Error::validation(
                    "Evidence signing requires a key secret reference",
                ));
            }
        }
    }

    Ok(())
}

/// Check schedule for recurring validations
pub async fn check_schedule(
    validation: &KafkaBackupValidation,
    _client: &Client,
    _namespace: &str,
) -> Result<Action> {
    let name = validation.name_any();

    if validation.spec.suspend {
        info!(name = %name, "Validation is suspended");
        return Ok(Action::requeue(Duration::from_secs(60)));
    }

    match &validation.spec.schedule {
        Some(schedule) => match cron::Schedule::from_str(schedule) {
            Ok(cron_schedule) => {
                let now = Utc::now();
                if let Some(next) = cron_schedule.upcoming(Utc).next() {
                    let duration_until = (next - now).to_std().unwrap_or(Duration::from_secs(60));
                    info!(
                        name = %name,
                        next = %next,
                        "Next scheduled validation"
                    );
                    Ok(Action::requeue(duration_until))
                } else {
                    Ok(Action::requeue(Duration::from_secs(3600)))
                }
            }
            Err(e) => {
                warn!(name = %name, error = %e, "Invalid cron schedule");
                Ok(Action::requeue(Duration::from_secs(300)))
            }
        },
        None => {
            // One-shot validation, no rescheduling
            Ok(Action::await_change())
        }
    }
}

/// Execute a validation operation
pub async fn execute(
    validation: &KafkaBackupValidation,
    client: &Client,
    namespace: &str,
) -> Result<Action> {
    let name = validation.name_any();
    let api: Api<KafkaBackupValidation> = Api::namespaced(client.clone(), namespace);

    info!(name = %name, "Starting validation execution");

    // Update status to Running
    let running_status = json!({
        "status": {
            "phase": "Running",
            "message": "Validation in progress",
            "startTime": Utc::now(),
            "observedGeneration": validation.metadata.generation,
        }
    });
    api.patch_status(
        &name,
        &PatchParams::apply("kafka-backup-operator"),
        &Patch::Merge(running_status),
    )
    .await?;

    // Execute validation
    let validation_result = execute_validation_internal(validation, client, namespace).await;

    match validation_result {
        Ok(result) => {
            info!(
                name = %name,
                result = %result.overall_result,
                checks_passed = result.checks_passed,
                checks_failed = result.checks_failed,
                "Validation completed"
            );

            metrics::VALIDATIONS_TOTAL
                .with_label_values(&[&result.overall_result, namespace, &name])
                .inc();

            let check_results: Vec<serde_json::Value> = result
                .check_results
                .iter()
                .map(|cr| {
                    json!({
                        "checkName": cr.check_name,
                        "outcome": cr.outcome,
                        "message": cr.message,
                        "durationMs": cr.duration_ms,
                    })
                })
                .collect();

            let completed_status = json!({
                "status": {
                    "phase": "Completed",
                    "message": format!("Validation completed: {}", result.overall_result),
                    "validationResult": result.overall_result,
                    "completionTime": Utc::now(),
                    "checksTotal": result.checks_total,
                    "checksCompleted": result.checks_total,
                    "checksPassed": result.checks_passed,
                    "checksFailed": result.checks_failed,
                    "checksWarned": result.checks_warned,
                    "checkResults": check_results,
                    "evidenceReportPath": result.evidence_report_path,
                    "evidenceReportSigned": result.evidence_report_signed,
                    "lastValidationTime": Utc::now(),
                    "observedGeneration": validation.metadata.generation,
                    "conditions": [{
                        "type": "Ready",
                        "status": "True",
                        "lastTransitionTime": Utc::now(),
                        "reason": "ValidationCompleted",
                        "message": format!("Validation {}", result.overall_result)
                    }]
                }
            });
            api.patch_status(
                &name,
                &PatchParams::apply("kafka-backup-operator"),
                &Patch::Merge(completed_status),
            )
            .await?;

            // Schedule next validation if recurring
            check_schedule(validation, client, namespace).await
        }
        Err(e) => {
            error!(name = %name, error = %e, "Validation failed");

            metrics::VALIDATIONS_TOTAL
                .with_label_values(&["error", namespace, &name])
                .inc();

            let failed_status = json!({
                "status": {
                    "phase": "Failed",
                    "message": format!("Validation failed: {}", e),
                    "observedGeneration": validation.metadata.generation,
                    "conditions": [{
                        "type": "Ready",
                        "status": "False",
                        "lastTransitionTime": Utc::now(),
                        "reason": "ValidationFailed",
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

/// Internal validation result
struct ValidationInternalResult {
    overall_result: String,
    checks_total: u32,
    checks_passed: u32,
    checks_failed: u32,
    checks_warned: u32,
    check_results: Vec<CheckResult>,
    evidence_report_path: Option<String>,
    evidence_report_signed: Option<bool>,
}

struct CheckResult {
    check_name: String,
    outcome: String,
    message: Option<String>,
    duration_ms: Option<u64>,
}

/// Execute the actual validation using kafka-backup-core library
async fn execute_validation_internal(
    validation: &KafkaBackupValidation,
    client: &Client,
    namespace: &str,
) -> Result<ValidationInternalResult> {
    let name = validation.name_any();

    info!(name = %name, "Building validation configuration");

    // 1. Build resolved configuration from CRD spec
    let resolved_config = build_validation_config(validation, client, namespace).await?;

    // 2. Resolve the backup source to get storage config and backup ID
    let (backup_id, storage) =
        resolve_backup_source(&resolved_config.backup_source, client, namespace).await?;

    info!(
        name = %name,
        backup_id = %backup_id,
        "Starting validation checks"
    );

    // 3. Convert to kafka-backup-core ValidationConfig
    let core_config = to_core_validation_config(&resolved_config, &backup_id, &storage)
        .map_err(|e| Error::Core(format!("Failed to build core validation config: {}", e)))?;

    // 4. Create ValidationRunner from core library
    let runner = kafka_backup_core::validation::ValidationRunner::from_config(&core_config.checks);

    // 5. Build the validation context
    let core_storage_config = crate::adapters::to_core_storage_config_for_validation(&storage);
    let storage_backend = kafka_backup_core::storage::create_backend(&core_storage_config)
        .map_err(|e| Error::Storage(format!("Failed to create storage backend: {}", e)))?;

    // Load backup manifest from storage
    let manifest_key = format!("{}/manifest.json", backup_id);
    let manifest_data = storage_backend.get(&manifest_key).await.map_err(|e| {
        Error::Core(format!(
            "Failed to load backup manifest for '{}': {}",
            backup_id, e
        ))
    })?;
    let manifest: kafka_backup_core::manifest::BackupManifest =
        serde_json::from_slice(&manifest_data)
            .map_err(|e| Error::Core(format!("Failed to parse backup manifest: {}", e)))?;

    let target_bootstrap_servers = resolved_config
        .kafka
        .as_ref()
        .map(|k| k.bootstrap_servers.clone())
        .unwrap_or_default();

    let kafka_client = if !target_bootstrap_servers.is_empty() {
        let kafka_config = resolved_config
            .kafka
            .as_ref()
            .map(crate::adapters::to_core_kafka_config_for_validation)
            .unwrap();
        let kc = kafka_backup_core::kafka::KafkaClient::new(kafka_config);
        kc.connect()
            .await
            .map_err(|e| Error::Core(format!("Failed to connect to Kafka: {}", e)))?;
        kc
    } else {
        // No Kafka cluster configured; create a dummy client
        let kafka_config = kafka_backup_core::config::KafkaConfig {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            security: kafka_backup_core::config::SecurityConfig::default(),
            topics: kafka_backup_core::config::TopicSelection::default(),
            connection: kafka_backup_core::config::ConnectionConfig::default(),
        };
        kafka_backup_core::kafka::KafkaClient::new(kafka_config)
    };

    let ctx = kafka_backup_core::validation::ValidationContext {
        backup_id: backup_id.clone(),
        backup_manifest: manifest,
        target_client: std::sync::Arc::new(kafka_client),
        storage: storage_backend,
        pitr_timestamp: None,
        http_client: reqwest::Client::new(),
        target_bootstrap_servers,
    };

    // 6. Run all validation checks
    let summary = runner
        .run_all(&ctx)
        .await
        .map_err(|e| Error::Core(format!("Validation execution failed: {}", e)))?;

    // 7. Map results
    let overall_result = match summary.overall_result {
        kafka_backup_core::validation::CheckOutcome::Passed => "Pass",
        kafka_backup_core::validation::CheckOutcome::Failed => "Fail",
        kafka_backup_core::validation::CheckOutcome::Warning => "Warn",
        kafka_backup_core::validation::CheckOutcome::Skipped => "Skip",
    }
    .to_string();

    let check_results: Vec<CheckResult> = summary
        .results
        .iter()
        .map(|r| {
            let outcome = match r.outcome {
                kafka_backup_core::validation::CheckOutcome::Passed => "Passed",
                kafka_backup_core::validation::CheckOutcome::Failed => "Failed",
                kafka_backup_core::validation::CheckOutcome::Warning => "Warning",
                kafka_backup_core::validation::CheckOutcome::Skipped => "Skipped",
            };
            CheckResult {
                check_name: r.check_name.clone(),
                outcome: outcome.to_string(),
                message: Some(r.detail.clone()),
                duration_ms: Some(r.duration_ms),
            }
        })
        .collect();

    // 8. Generate evidence report if configured
    let (evidence_report_path, evidence_report_signed) = if resolved_config.evidence.is_some() {
        // Evidence generation will be handled by the core library
        // For now, report that evidence was generated
        (None, None)
    } else {
        (None, None)
    };

    Ok(ValidationInternalResult {
        overall_result,
        checks_total: summary.checks_total as u32,
        checks_passed: summary.checks_passed as u32,
        checks_failed: summary.checks_failed as u32,
        checks_warned: summary.checks_warned as u32,
        check_results,
        evidence_report_path,
        evidence_report_signed,
    })
}

/// Resolve backup source to get backup ID and storage configuration
async fn resolve_backup_source(
    source: &ResolvedBackupSource,
    client: &Client,
    _namespace: &str,
) -> Result<(String, ResolvedStorage)> {
    match source {
        ResolvedBackupSource::Storage { storage, backup_id } => {
            let resolved_id = backup_id
                .clone()
                .unwrap_or_else(|| format!("validation-{}", Utc::now().format("%Y%m%d-%H%M%S")));
            Ok((resolved_id, storage.clone()))
        }
        ResolvedBackupSource::BackupResource {
            name,
            namespace: backup_ns,
            backup_id,
        } => {
            let api: Api<KafkaBackup> = Api::namespaced(client.clone(), backup_ns);
            let backup = api.get(name).await.map_err(|e| {
                Error::BackupNotFound(format!("Failed to fetch KafkaBackup '{}': {}", name, e))
            })?;

            let resolved_backup_id = backup_id.clone().unwrap_or_else(|| {
                backup
                    .status
                    .as_ref()
                    .and_then(|s| s.backup_id.clone())
                    .unwrap_or_else(|| format!("{}-latest", name))
            });

            let storage =
                crate::adapters::build_storage_config(&backup.spec.storage, client, backup_ns)
                    .await?;

            Ok((resolved_backup_id, storage))
        }
    }
}

/// Update status to Failed
pub async fn update_status_failed(
    validation: &KafkaBackupValidation,
    client: &Client,
    namespace: &str,
    error_message: &str,
) -> Result<()> {
    let name = validation.name_any();
    let api: Api<KafkaBackupValidation> = Api::namespaced(client.clone(), namespace);

    let status = json!({
        "status": {
            "phase": "Failed",
            "message": error_message,
            "observedGeneration": validation.metadata.generation,
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
