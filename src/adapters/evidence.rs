//! Validation evidence and notifications
//!
//! Builds the evidence report for a KafkaBackupValidation run and writes it
//! through kafka-backup-core's shared evidence pipeline
//! (`kafka_backup_core::evidence::emit_evidence`), the same one the OSS CLI
//! uses, so the stored JSON, its SHA-256, the signature envelope and the PDF
//! follow one byte-level contract. Also delivers `spec.notifications`.

use std::io::Write;

use chrono::{DateTime, Utc};
use kafka_backup_core::evidence::{
    build_evidence_report, emit_evidence, sha256_hex, EvidenceReport, EvidenceReportParams,
};
use kafka_backup_core::manifest::BackupManifest;
use kafka_backup_core::notification::pagerduty::PagerDutyNotifier;
use kafka_backup_core::notification::slack::SlackNotifier;
use kafka_backup_core::notification::NotificationSender;
use kafka_backup_core::storage::StorageBackend;
use kafka_backup_core::validation::{CheckOutcome, ValidationSummary};

use crate::error::{Error, Result};

use super::core_integration::to_core_evidence_config;
use super::storage_config::ResolvedStorage;
use super::validation_config::{ResolvedEvidenceConfig, ResolvedNotificationsConfig};

/// Retention recorded in the report's compliance mappings when no
/// `spec.evidence` is configured (the CRD's `retentionDays` default).
const DEFAULT_RETENTION_DAYS: u32 = 90;

/// What a validation run wrote to evidence storage.
///
/// Paths are object keys relative to the evidence storage (and its prefix),
/// exactly as kafka-backup-core returns them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EvidenceOutcome {
    /// Report ID; also the directory and file stem of every artifact.
    pub report_id: String,
    /// Key of the JSON report, when stored.
    pub json_path: Option<String>,
    /// Key of the PDF report, when stored.
    pub pdf_path: Option<String>,
    /// Key of the detached signature envelope, when signed.
    pub signature_path: Option<String>,
    /// Lowercase hex SHA-256 of the exact stored JSON bytes.
    pub report_sha256: String,
    /// Whether a signature envelope was written.
    pub signed: bool,
    /// Non-fatal findings from the core pipeline (e.g. PDF generation).
    pub warnings: Vec<String>,
}

/// Inputs for [`build_validation_report`].
pub struct ValidationReportInputs<'a> {
    pub report_id: &'a str,
    pub backup_id: &'a str,
    pub manifest: &'a BackupManifest,
    /// Manifest bytes as loaded from storage (hashed into the report).
    pub manifest_bytes: &'a [u8],
    /// Storage holding the validated backup.
    pub backup_storage: &'a ResolvedStorage,
    pub target_bootstrap_servers: Vec<String>,
    pub summary: ValidationSummary,
    pub evidence: Option<&'a ResolvedEvidenceConfig>,
    pub triggered_by: String,
}

/// Report ID for one validation run: `{name}-{YYYYMMDD-HHMMSS}-{uid[..8]}`.
///
/// The UID fragment keeps same-named validations in different namespaces
/// that share an evidence bucket from writing to the same keys.
pub fn evidence_report_id(name: &str, uid: Option<&str>, now: DateTime<Utc>) -> String {
    let stamp = now.format("%Y%m%d-%H%M%S");
    match uid.map(|u| u.chars().take(8).collect::<String>()) {
        Some(short) if !short.is_empty() => format!("{name}-{stamp}-{short}"),
        _ => format!("{name}-{stamp}"),
    }
}

/// Credential-free description of a storage location for the report.
///
/// The CLI records `format!("{:?}", storage_config)`, which embeds account
/// keys and secrets in the (signed) report. Only the location is recorded
/// here.
pub fn describe_storage(storage: &ResolvedStorage) -> String {
    fn with_prefix(base: String, prefix: Option<&str>) -> String {
        match prefix
            .map(|p| p.trim_matches('/'))
            .filter(|p| !p.is_empty())
        {
            Some(p) => format!("{base}/{p}"),
            None => base,
        }
    }
    fn with_endpoint(base: String, endpoint: Option<&str>) -> String {
        match endpoint {
            Some(e) => format!("{base} (endpoint {e})"),
            None => base,
        }
    }

    match storage {
        ResolvedStorage::Local(local) => format!("file://{}", local.path),
        ResolvedStorage::S3(s3) => with_endpoint(
            with_prefix(format!("s3://{}", s3.bucket), s3.prefix.as_deref()),
            s3.endpoint.as_deref(),
        ),
        ResolvedStorage::Azure(azure) => with_endpoint(
            with_prefix(
                format!("azure://{}/{}", azure.account_name, azure.container),
                azure.prefix.as_deref(),
            ),
            azure.endpoint.as_deref(),
        ),
        ResolvedStorage::Gcs(gcs) => {
            with_prefix(format!("gs://{}", gcs.bucket), gcs.prefix.as_deref())
        }
    }
}

/// Where evidence is written: `spec.evidence.storage`, or the backup's own
/// storage when unset (as the CLI does).
pub fn evidence_storage<'a>(
    evidence: &'a ResolvedEvidenceConfig,
    backup_storage: &'a ResolvedStorage,
) -> &'a ResolvedStorage {
    evidence.storage.as_ref().unwrap_or(backup_storage)
}

/// Build the evidence report for a validation run.
pub fn build_validation_report(inputs: ValidationReportInputs<'_>) -> EvidenceReport {
    let mut report = build_evidence_report(EvidenceReportParams {
        run_id: inputs.report_id,
        tool_version: env!("CARGO_PKG_VERSION"),
        backup_id: inputs.backup_id,
        manifest: inputs.manifest,
        manifest_sha256: sha256_hex(inputs.manifest_bytes),
        storage_backend: describe_storage(inputs.backup_storage),
        pitr_timestamp: None,
        target_bootstrap_servers: inputs.target_bootstrap_servers,
        summary: inputs.summary,
        retention_days: inputs
            .evidence
            .map_or(DEFAULT_RETENTION_DAYS, |e| e.retention_days),
        signing_enabled: inputs
            .evidence
            .is_some_and(|e| e.signing_private_key_pem.is_some()),
        triggered_by: Some(inputs.triggered_by),
    });
    // Core names the tool "kafka-backup"; this report was emitted by the
    // operator, at the operator's version.
    report.tool.name = "kafka-backup-operator".to_string();
    report
}

/// Serialize, hash, sign and upload the report through kafka-backup-core.
///
/// Core reads the signing key from a file, so the PEM from the Secret is
/// written to a private temporary file (created exclusively, mode 0600) that
/// is removed when this function returns, on success or error.
pub async fn emit_validation_evidence(
    storage: &dyn StorageBackend,
    report: &EvidenceReport,
    evidence: &ResolvedEvidenceConfig,
) -> Result<EvidenceOutcome> {
    let key_file = match &evidence.signing_private_key_pem {
        Some(pem) => Some(write_signing_key(pem)?),
        None => None,
    };
    let key_path = key_file
        .as_ref()
        .map(|f| f.path().to_string_lossy().into_owned());

    let config = to_core_evidence_config(evidence, key_path);
    let emission = emit_evidence(storage, report, &config)
        .await
        .map_err(|e| Error::evidence(format!("Failed to emit evidence report: {e}")))?;
    drop(key_file);

    Ok(EvidenceOutcome {
        report_id: report.report_id.clone(),
        json_path: emission.json_key,
        pdf_path: emission.pdf_key,
        signature_path: emission.signature_key,
        report_sha256: emission.report_sha256,
        signed: emission.signed,
        warnings: emission.warnings,
    })
}

fn write_signing_key(pem: &str) -> Result<tempfile::NamedTempFile> {
    let mut file = tempfile::Builder::new()
        .prefix("evidence-signing-")
        .suffix(".pem")
        .tempfile()
        .map_err(|e| Error::evidence(format!("Failed to create signing key file: {e}")))?;
    file.write_all(pem.as_bytes())
        .and_then(|()| file.flush())
        .map_err(|e| Error::evidence(format!("Failed to write signing key file: {e}")))?;
    Ok(file)
}

/// Check that a signing key is a PEM PKCS#8 private key, the only form
/// kafka-backup-core parses. Never echoes key material.
pub fn check_signing_key_pem(pem: &str) -> Result<()> {
    if pem.contains("-----BEGIN EC PRIVATE KEY-----") {
        return Err(Error::validation(
            "evidence signing key is a SEC1 'EC PRIVATE KEY'; a PKCS#8 'PRIVATE KEY' is \
             required. Convert it with: openssl pkcs8 -topk8 -nocrypt -in key.pem -out key.p8.pem",
        ));
    }
    if !pem.contains("-----BEGIN PRIVATE KEY-----") {
        return Err(Error::validation(
            "evidence signing key must be a PEM-encoded PKCS#8 ECDSA P-256 private key \
             ('-----BEGIN PRIVATE KEY-----')",
        ));
    }
    Ok(())
}

/// Send the validation result to the configured Slack/PagerDuty targets.
///
/// Returns one message per failed delivery; a failed notification does not
/// fail a validation whose checks and evidence already succeeded.
pub async fn send_validation_notifications(
    notifications: &ResolvedNotificationsConfig,
    report: &EvidenceReport,
    evidence_url: &str,
) -> Vec<String> {
    let mut senders: Vec<(&str, Box<dyn NotificationSender>)> = Vec::new();
    if let Some(url) = &notifications.slack_webhook_url {
        senders.push(("Slack", Box::new(SlackNotifier::new(url.clone()))));
    }
    if let Some(key) = &notifications.pagerduty_routing_key {
        let severity = notifications
            .pagerduty_severity
            .clone()
            .unwrap_or_else(|| "critical".to_string());
        senders.push((
            "PagerDuty",
            Box::new(PagerDutyNotifier::new(key.clone(), severity)),
        ));
    }

    let passed = report.validation.overall_result == CheckOutcome::Passed;
    let mut failures = Vec::new();
    for (name, sender) in &senders {
        let result = if passed {
            sender.send_success(report, evidence_url).await
        } else {
            sender.send_failure(report, evidence_url).await
        };
        if let Err(e) = result {
            failures.push(format!("{name} notification failed: {e}"));
        }
    }
    failures
}
