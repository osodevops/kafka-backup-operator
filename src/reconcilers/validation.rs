//! KafkaBackupValidation reconciler
//!
//! Handles the business logic for backup validation operations including:
//! - Spec validation
//! - Backup reference resolution
//! - Validation check execution
//! - Evidence report generation
//! - Notification delivery

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use cron::Schedule;
use kube::{
    api::{Patch, PatchParams},
    runtime::controller::Action,
    Api, Client, ResourceExt,
};
use serde_json::json;
use tracing::{debug, error, info, warn};

use crate::adapters::{
    build_validation_config, build_validation_report, emit_validation_evidence, evidence_report_id,
    evidence_storage, send_validation_notifications, to_core_storage_config_for_validation,
    to_core_validation_config, EvidenceOutcome, ResolvedBackupSource, ResolvedStorage,
    ValidationReportInputs,
};
use crate::crd::{KafkaBackup, KafkaBackupValidation};
use crate::error::{Error, Result};
use crate::metrics;
use crate::reconcilers::runs;

const KIND: &str = "KafkaBackupValidation";

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

/// Longest a scheduled validation waits before re-checking its schedule, so
/// a lost requeue is recovered.
const MAX_SCHEDULE_REQUEUE: Duration = Duration::from_secs(300);

/// Process-local record of when this operator last started a scheduled run
/// of each `{namespace}/{name}`.
///
/// `status.startTime` (written in the `Running` patch before the checks run)
/// is the durable anchor, but it is read from the reflector cache, which can
/// lag our own patches by tens of milliseconds. A fast validation can finish
/// and be reconciled again while the cache still shows the previous run; this
/// guard stops that reconcile from firing the same tick twice. Same approach
/// as the KafkaBackup scheduler.
fn scheduler_guard() -> &'static Mutex<HashMap<String, DateTime<Utc>>> {
    static GUARD: OnceLock<Mutex<HashMap<String, DateTime<Utc>>>> = OnceLock::new();
    GUARD.get_or_init(|| Mutex::new(HashMap::new()))
}

fn guard_key(validation: &KafkaBackupValidation) -> String {
    format!(
        "{}/{}",
        validation.namespace().unwrap_or_default(),
        validation.name_any()
    )
}

fn parsed_schedule(validation: &KafkaBackupValidation) -> Option<Schedule> {
    validation
        .spec
        .schedule
        .as_deref()
        .and_then(|s| Schedule::from_str(s).ok())
}

/// Whether a cron tick has passed since `anchor`.
fn tick_passed_since(schedule: &Schedule, anchor: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    schedule
        .after(&anchor)
        .next()
        .is_some_and(|next_tick| next_tick <= now)
}

/// Whether a scheduled validation that has already run is due to run again.
///
/// Due when a cron tick has passed since the last run started
/// (`status.startTime`, falling back to the creation time), unless this
/// process already fired that tick (see [`scheduler_guard`]). Never due for
/// one-shot, suspended, or unparseable schedules.
pub fn scheduled_validation_due(validation: &KafkaBackupValidation, now: DateTime<Utc>) -> bool {
    if validation.spec.suspend {
        return false;
    }
    let Some(schedule) = parsed_schedule(validation) else {
        return false;
    };

    let anchor = validation
        .status
        .as_ref()
        .and_then(|s| s.start_time)
        .or_else(|| validation.metadata.creation_timestamp.as_ref().map(|t| t.0))
        .unwrap_or(now);
    if !tick_passed_since(&schedule, anchor, now) {
        return false;
    }

    let fired = scheduler_guard()
        .lock()
        .unwrap()
        .get(&guard_key(validation))
        .copied();
    match fired {
        Some(fired_at) => tick_passed_since(&schedule, fired_at, now),
        None => true,
    }
}

/// Record that this process started a scheduled run of `validation`.
pub fn record_scheduled_run(validation: &KafkaBackupValidation, now: DateTime<Utc>) {
    scheduler_guard()
        .lock()
        .unwrap()
        .insert(guard_key(validation), now);
}

/// The next cron tick after `now`, for scheduled validations.
pub fn next_validation_time(
    validation: &KafkaBackupValidation,
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    parsed_schedule(validation).and_then(|schedule| schedule.after(&now).next())
}

/// Requeue until the next cron tick (capped), or wait for a spec change for
/// one-shot validations.
pub fn requeue_until_next_run(validation: &KafkaBackupValidation, now: DateTime<Utc>) -> Action {
    if validation.spec.schedule.is_none() {
        return Action::await_change();
    }
    match next_validation_time(validation, now) {
        Some(next) => Action::requeue(
            (next - now)
                .to_std()
                .unwrap_or(Duration::from_secs(1))
                .min(MAX_SCHEDULE_REQUEUE),
        ),
        None => Action::requeue(MAX_SCHEDULE_REQUEUE),
    }
}

/// Handle a validation whose current spec has already run: run it again
/// when its schedule is due, otherwise requeue for the next tick.
pub async fn check_schedule(
    validation: &KafkaBackupValidation,
    client: &Client,
    namespace: &str,
) -> Result<Action> {
    let name = validation.name_any();

    if validation.spec.suspend {
        info!(name = %name, "Validation is suspended");
        return Ok(Action::requeue(Duration::from_secs(60)));
    }

    let Some(schedule) = &validation.spec.schedule else {
        // One-shot validation, no rescheduling
        return Ok(Action::await_change());
    };
    if Schedule::from_str(schedule).is_err() {
        warn!(name = %name, schedule = %schedule, "Invalid cron schedule");
        return Ok(Action::requeue(MAX_SCHEDULE_REQUEUE));
    }

    let now = Utc::now();
    if scheduled_validation_due(validation, now) {
        // Record before executing so a reconcile triggered by our own
        // Running patch, reading a stale cache, does not fire this tick again.
        record_scheduled_run(validation, now);
        info!(name = %name, "Scheduled validation time reached, running validation");
        return execute(validation, client, namespace).await;
    }

    debug!(
        name = %name,
        next = ?next_validation_time(validation, now),
        "Next scheduled validation"
    );
    Ok(requeue_until_next_run(validation, now))
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

    runs::record_start(KIND, &validation.metadata);

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
                    "nextScheduledValidation": next_validation_time(validation, Utc::now()),
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

            // Wait for the next tick. Not check_schedule(): `validation` is the
            // pre-run object, whose old startTime would make the tick look due.
            Ok(requeue_until_next_run(validation, Utc::now()))
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
                    "nextScheduledValidation": next_validation_time(validation, Utc::now()),
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

/// Whether a new spec generation runs at once. A suspended validation does
/// not run: setting `suspend: true` is itself a spec change.
pub fn runs_on_spec_change(validation: &KafkaBackupValidation) -> bool {
    !validation.spec.suspend
}

/// Record a new generation of a suspended validation without running it.
pub async fn record_suspended(
    validation: &KafkaBackupValidation,
    client: &Client,
    namespace: &str,
) -> Result<()> {
    let api: Api<KafkaBackupValidation> = Api::namespaced(client.clone(), namespace);
    let status = json!({
        "status": {
            "message": "Validation is suspended",
            "nextScheduledValidation": null,
            "observedGeneration": validation.metadata.generation,
        }
    });
    api.patch_status(
        &validation.name_any(),
        &PatchParams::apply("kafka-backup-operator"),
        &Patch::Merge(status),
    )
    .await?;
    Ok(())
}
