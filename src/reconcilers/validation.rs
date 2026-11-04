//! KafkaBackupValidation reconciler
//!
//! Handles the business logic for backup validation operations including:
//! - Spec validation
//! - Backup reference resolution
//! - Validation check execution
//! - Evidence report generation
//! - Notification delivery

use std::str::FromStr;
use std::time::{Duration, Instant};

use chrono::Utc;
use kube::{
    api::{Patch, PatchParams},
    runtime::controller::Action,
    Api, Client, ResourceExt,
};
use serde_json::json;
use tracing::{error, info, warn};

use crate::adapters::{
    build_validation_config, build_validation_report, emit_validation_evidence, evidence_report_id,
    evidence_storage, send_validation_notifications, to_core_storage_config_for_validation,
    to_core_validation_config, EvidenceOutcome, ResolvedBackupSource, ResolvedStorage,
    ValidationReportInputs,
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
        // Validate TLS configuration: SSL/SASL_SSL requires at least one TLS secret
        let protocol = kafka_cluster.security_protocol.to_uppercase();
        if (protocol == "SSL" || protocol == "SASL_SSL")
            && kafka_cluster.tls_secret.is_none()
            && kafka_cluster.ca_secret.is_none()
        {
            return Err(Error::validation(
                "securityProtocol SSL/SASL_SSL requires either tlsSecret or caSecret to be configured",
            ));
        }

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

    // Validate evidence formats and signing key secret
    if let Some(evidence) = &validation.spec.evidence {
        validate_evidence_formats(&evidence.formats)?;
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

/// Evidence formats kafka-backup-core can produce.
const EVIDENCE_FORMATS: [&str; 2] = ["json", "pdf"];

fn validate_evidence_formats(formats: &[String]) -> Result<()> {
    if formats.is_empty() {
        return Err(Error::validation(
            "evidence.formats must list at least one of: json, pdf",
        ));
    }
    for format in formats {
        if !EVIDENCE_FORMATS.contains(&format.to_lowercase().as_str()) {
            return Err(Error::validation(format!(
                "Unsupported evidence format '{}'; supported formats: json, pdf",
                format
            )));
        }
    }
    Ok(())
}

/// Status fields describing the evidence of the last completed validation.
///
/// Every field is always present: absent artifacts are `null`, so the status
/// merge patch clears paths left by an earlier run.
pub fn evidence_status_fields(
    outcome: Option<&EvidenceOutcome>,
) -> serde_json::Map<String, serde_json::Value> {
    let fields = json!({
        "evidenceReportId": outcome.map(|o| &o.report_id),
        "evidenceReportPath": outcome.and_then(|o| o.json_path.as_ref()),
        "evidencePdfPath": outcome.and_then(|o| o.pdf_path.as_ref()),
        "evidenceSignaturePath": outcome.and_then(|o| o.signature_path.as_ref()),
        "evidenceReportSha256": outcome.map(|o| &o.report_sha256),
        "evidenceReportSigned": outcome.map(|o| o.signed),
    });
    match fields {
        serde_json::Value::Object(map) => map,
        _ => unreachable!("json! object literal"),
    }
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
    let started_at = Instant::now();
    let validation_result = execute_validation_internal(validation, client, namespace).await;
    let elapsed = started_at.elapsed();

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
            metrics::VALIDATION_DURATION
                .with_label_values(&[namespace, &name])
                .observe(elapsed.as_secs_f64());

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

            let mut message = format!("Validation completed: {}", result.overall_result);
            if !result.warnings.is_empty() {
                message.push_str(&format!(" (warnings: {})", result.warnings.join("; ")));
            }
            let mut completed_status = json!({
                "status": {
                    "phase": "Completed",
                    "message": message,
                    "validationResult": result.overall_result,
                    "completionTime": Utc::now(),
                    "checksTotal": result.checks_total,
                    "checksCompleted": result.checks_total,
                    "checksPassed": result.checks_passed,
                    "checksFailed": result.checks_failed,
                    "checksWarned": result.checks_warned,
                    "checkResults": check_results,
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
            if let Some(status) = completed_status["status"].as_object_mut() {
                status.extend(evidence_status_fields(result.evidence.as_ref()));
            }
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

            // The checks may have run, but requested evidence that was not
            // stored makes the run unusable as compliance evidence.
            let reason = match &e {
                Error::Evidence(_) => "EvidenceEmissionFailed",
                _ => "ValidationFailed",
            };
            let failed_status = json!({
                "status": {
                    "phase": "Failed",
                    "message": format!("Validation failed: {}", e),
                    "observedGeneration": validation.metadata.generation,
                    "conditions": [{
                        "type": "Ready",
                        "status": "False",
                        "lastTransitionTime": Utc::now(),
                        "reason": reason,
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
    /// Evidence written for this run, when `spec.evidence` is set
    evidence: Option<EvidenceOutcome>,
    /// Non-fatal findings (PDF generation, notification delivery)
    warnings: Vec<String>,
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

    if target_bootstrap_servers.is_empty() {
        return Err(Error::validation(
            "Validation requires a target Kafka cluster; spec.kafka.bootstrapServers must be set",
        ));
    }
    let kafka_config = resolved_config
        .kafka
        .as_ref()
        .map(crate::adapters::to_core_kafka_config_for_validation)
        .unwrap();
    let target_router = kafka_backup_core::kafka::PartitionLeaderRouter::new(kafka_config)
        .await
        .map_err(|e| Error::Core(format!("Failed to connect to target Kafka cluster: {}", e)))?;

    let ctx = kafka_backup_core::validation::ValidationContext {
        backup_id: backup_id.clone(),
        backup_manifest: manifest.clone(),
        target_client: std::sync::Arc::new(target_router),
        storage: storage_backend,
        pitr_timestamp: None,
        http_client: reqwest::Client::new(),
        target_bootstrap_servers: target_bootstrap_servers.clone(),
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

    // 8. Evidence report and notifications (issue #93)
    let mut warnings = Vec::new();
    let evidence = if resolved_config.evidence.is_some() || resolved_config.notifications.is_some()
    {
        let report_id = evidence_report_id(&name, validation.metadata.uid.as_deref(), Utc::now());
        let report = build_validation_report(ValidationReportInputs {
            report_id: &report_id,
            backup_id: &backup_id,
            manifest: &manifest,
            manifest_bytes: manifest_data.as_ref(),
            backup_storage: &storage,
            target_bootstrap_servers,
            summary: summary.clone(),
            evidence: resolved_config.evidence.as_ref(),
            triggered_by: format!("kafka-backup-operator:{}/{}", namespace, name),
        });

        let outcome = match &resolved_config.evidence {
            Some(evidence_config) => {
                let destination = evidence_storage(evidence_config, &storage);
                let backend = kafka_backup_core::storage::create_backend(
                    &to_core_storage_config_for_validation(destination),
                )
                .map_err(|e| {
                    Error::evidence(format!("Failed to create evidence storage backend: {}", e))
                })?;
                let outcome =
                    emit_validation_evidence(backend.as_ref(), &report, evidence_config).await?;
                info!(
                    name = %name,
                    report_id = %outcome.report_id,
                    json = outcome.json_path.as_deref().unwrap_or("-"),
                    pdf = outcome.pdf_path.as_deref().unwrap_or("-"),
                    signature = outcome.signature_path.as_deref().unwrap_or("-"),
                    sha256 = %outcome.report_sha256,
                    "Evidence report stored"
                );
                warnings.extend(outcome.warnings.iter().cloned());
                Some(outcome)
            }
            None => None,
        };

        if let Some(notifications) = &resolved_config.notifications {
            let evidence_url = outcome
                .as_ref()
                .and_then(|o| o.json_path.clone().or_else(|| o.pdf_path.clone()))
                .unwrap_or_default();
            warnings
                .extend(send_validation_notifications(notifications, &report, &evidence_url).await);
        }

        outcome
    } else {
        None
    };
    for warning in &warnings {
        warn!(name = %name, warning = %warning, "Validation completed with a warning");
    }

    Ok(ValidationInternalResult {
        overall_result,
        checks_total: summary.checks_total as u32,
        checks_passed: summary.checks_passed as u32,
        checks_failed: summary.checks_failed as u32,
        checks_warned: summary.checks_warned as u32,
        check_results,
        evidence,
        warnings,
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
