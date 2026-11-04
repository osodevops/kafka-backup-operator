//! Issue #93: KafkaBackupValidation accepted `spec.evidence` (JSON/PDF
//! formats, signing, a separate storage destination) but never produced
//! evidence. The reconciler's evidence step was a placeholder that always
//! returned `(None, None)`, so a run reported `Completed` with no report in
//! storage and no path in status. `spec.notifications` was resolved and
//! dropped the same way.
//!
//! These tests drive the evidence path the reconciler now uses: the report is
//! built and emitted through kafka-backup-core's shared pipeline (the one the
//! OSS CLI uses), so the stored bytes, digest and signature envelope follow
//! the same contract.

use std::path::Path;
use std::sync::Arc;

use chrono::{TimeZone, Utc};
use kafka_backup_core::evidence::{verify_evidence, EvidenceReport};
use kafka_backup_core::manifest::BackupManifest;
use kafka_backup_core::storage::{create_backend, StorageBackend};
use kafka_backup_core::validation::{CheckOutcome, ValidationResult, ValidationSummary};
use kafka_backup_operator::adapters::{
    build_validation_report, check_signing_key_pem, describe_storage, emit_validation_evidence,
    evidence_report_id, evidence_storage, send_validation_notifications, to_core_storage_config,
    AzureAuthMethod, AzureStorageConfig, LocalStorageConfig, ResolvedEvidenceConfig,
    ResolvedNotificationsConfig, ResolvedStorage, S3StorageConfig, ValidationReportInputs,
};
use kafka_backup_operator::crd::KafkaBackupValidation;
use kafka_backup_operator::reconcilers::validation::{evidence_status_fields, validate};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Test-only P-256 key pair (PKCS#8), generated for these tests.
const TEST_PRIVATE_KEY_PKCS8: &str = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgVF1f/v4cMMxjORPD
QxdwuM41FZrg1upEBjcEHlAUKkKhRANCAAQ4+cgIUYpMJbJB19tXfhycCvnadmb9
yJQDwM2k9V814HOpdgRl/Y06A3J8XTBNV8MGLK6vpJu7IeISr9ljm9IZ
-----END PRIVATE KEY-----
";

const TEST_PUBLIC_KEY: &str = "-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEOPnICFGKTCWyQdfbV34cnAr52nZm
/ciUA8DNpPVfNeBzqXYEZf2NOgNyfF0wTVfDBiyur6SbuyHiEq/ZY5vSGQ==
-----END PUBLIC KEY-----
";

const AZURE_ACCOUNT_KEY: &str = "c2VjcmV0LWFjY291bnQta2V5LW5vdC1mb3ItcmVwb3J0cw==";

fn manifest_bytes() -> Vec<u8> {
    serde_json::to_vec(&json!({
        "backup_id": "orders-backup-20261104-064939",
        "created_at": 1_793_000_000_000i64,
        "source_brokers": ["redpanda.kbo.svc.cluster.local:9092"],
        "topics": [],
    }))
    .unwrap()
}

fn summary(outcome: CheckOutcome) -> ValidationSummary {
    ValidationSummary {
        checks_total: 1,
        checks_passed: usize::from(outcome == CheckOutcome::Passed),
        checks_failed: usize::from(outcome == CheckOutcome::Failed),
        checks_skipped: 0,
        checks_warned: 0,
        overall_result: outcome,
        results: vec![ValidationResult {
            check_name: "MessageCountCheck".to_string(),
            outcome,
            detail: "1 topics; 500 messages expected, 500 restored; 0 discrepancies".to_string(),
            data: json!({}),
            duration_ms: 4,
        }],
        total_duration_ms: 4,
    }
}

fn azure_backup_storage() -> ResolvedStorage {
    ResolvedStorage::Azure(AzureStorageConfig {
        container: "backups".to_string(),
        account_name: "kafkabackups".to_string(),
        auth: AzureAuthMethod::AccountKey(AZURE_ACCOUNT_KEY.to_string()),
        prefix: Some("prod".to_string()),
        endpoint: None,
    })
}

fn local_storage(dir: &Path) -> ResolvedStorage {
    ResolvedStorage::Local(LocalStorageConfig {
        path: dir.to_str().unwrap().to_string(),
    })
}

fn backend(storage: &ResolvedStorage) -> Arc<dyn StorageBackend> {
    create_backend(&to_core_storage_config(storage)).expect("storage backend")
}

fn evidence_config(formats: &[&str], signing_key: Option<&str>) -> ResolvedEvidenceConfig {
    ResolvedEvidenceConfig {
        formats: formats.iter().map(|f| f.to_string()).collect(),
        signing_private_key_pem: signing_key.map(str::to_string),
        signing_public_key_pem: signing_key.map(|_| TEST_PUBLIC_KEY.to_string()),
        storage: None,
        retention_days: 90,
    }
}

fn report(
    manifest_bytes: &[u8],
    backup_storage: &ResolvedStorage,
    evidence: Option<&ResolvedEvidenceConfig>,
    outcome: CheckOutcome,
) -> EvidenceReport {
    let manifest: BackupManifest = serde_json::from_slice(manifest_bytes).unwrap();
    build_validation_report(ValidationReportInputs {
        report_id: "orders-validation-20261104-065000-3f2a9c1e",
        backup_id: "orders-backup-20261104-064939",
        manifest: &manifest,
        manifest_bytes,
        backup_storage,
        target_bootstrap_servers: vec!["redpanda.kbo.svc.cluster.local:9092".to_string()],
        summary: summary(outcome),
        evidence,
        triggered_by: "kafka-backup-operator:kbo/orders-validation".to_string(),
    })
}

fn validation(evidence: serde_json::Value) -> KafkaBackupValidation {
    let spec = json!({
        "backupRef": {"name": "orders-backup"},
        "checks": {"messageCount": {"enabled": true}},
        "evidence": evidence,
    });
    KafkaBackupValidation::new("orders-validation", serde_json::from_value(spec).unwrap())
}

/// The reporter's configuration: JSON and PDF requested. Both land in
/// storage, and the outcome names them and the digest of the stored JSON.
#[tokio::test]
async fn requested_json_and_pdf_reports_are_written_to_evidence_storage() {
    let dir = tempfile::tempdir().unwrap();
    let storage = local_storage(dir.path());
    let evidence = evidence_config(&["json", "pdf"], None);
    let manifest = manifest_bytes();
    let report = report(&manifest, &storage, Some(&evidence), CheckOutcome::Passed);

    let outcome = emit_validation_evidence(backend(&storage).as_ref(), &report, &evidence)
        .await
        .expect("evidence emitted");

    assert_eq!(
        outcome.report_id,
        "orders-validation-20261104-065000-3f2a9c1e"
    );
    let json_path = outcome.json_path.as_deref().expect("JSON report path");
    let pdf_path = outcome.pdf_path.as_deref().expect("PDF report path");
    assert!(json_path.starts_with("evidence-reports/orders-validation-"));
    assert!(json_path.ends_with(".json"));
    assert!(pdf_path.ends_with(".pdf"));
    assert_eq!(outcome.signature_path, None);
    assert!(!outcome.signed);

    let stored_json = std::fs::read(dir.path().join(json_path)).expect("JSON report on disk");
    let stored_pdf = std::fs::read(dir.path().join(pdf_path)).expect("PDF report on disk");
    assert!(stored_pdf.starts_with(b"%PDF"));
    assert_eq!(
        outcome.report_sha256,
        kafka_backup_core::evidence::sha256_hex(&stored_json),
        "status digest must cover the exact stored bytes"
    );
    let stored: serde_json::Value = serde_json::from_slice(&stored_json).unwrap();
    assert_eq!(stored["backup"]["id"], "orders-backup-20261104-064939");
    assert_eq!(stored["validation"]["overall_result"], "PASSED");
}

/// A failed validation is evidence too: the report is still written.
#[tokio::test]
async fn failed_validation_still_produces_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let storage = local_storage(dir.path());
    let evidence = evidence_config(&["json"], None);
    let manifest = manifest_bytes();
    let report = report(&manifest, &storage, Some(&evidence), CheckOutcome::Failed);

    let outcome = emit_validation_evidence(backend(&storage).as_ref(), &report, &evidence)
        .await
        .expect("evidence emitted");

    let stored = std::fs::read(dir.path().join(outcome.json_path.unwrap())).unwrap();
    let stored: serde_json::Value = serde_json::from_slice(&stored).unwrap();
    assert_eq!(stored["validation"]["overall_result"], "FAILED");
}

/// Signing used to hand the PEM *content* to core as a key *path*. The key
/// from the Secret now signs the report, and the envelope verifies against
/// the public key over the exact stored bytes.
#[tokio::test]
async fn signed_evidence_verifies_against_the_public_key() {
    let dir = tempfile::tempdir().unwrap();
    let storage = local_storage(dir.path());
    let evidence = evidence_config(&["json", "pdf"], Some(TEST_PRIVATE_KEY_PKCS8));
    let manifest = manifest_bytes();
    let report = report(&manifest, &storage, Some(&evidence), CheckOutcome::Passed);

    let outcome = emit_validation_evidence(backend(&storage).as_ref(), &report, &evidence)
        .await
        .expect("signed evidence emitted");

    assert!(outcome.signed);
    let sig_path = outcome.signature_path.as_deref().expect("signature path");
    assert!(sig_path.ends_with(".sig"));
    let stored_json = std::fs::read(dir.path().join(outcome.json_path.unwrap())).unwrap();
    let envelope = std::fs::read_to_string(dir.path().join(sig_path)).unwrap();
    let verification =
        verify_evidence(&stored_json, &envelope, Some(TEST_PUBLIC_KEY)).expect("verify");
    assert!(verification.verified(), "{:?}", verification.messages);
    assert_eq!(verification.actual_sha256, outcome.report_sha256);

    let stored: serde_json::Value = serde_json::from_slice(&stored_json).unwrap();
    assert_eq!(
        stored["integrity"]["signature_algorithm"],
        "ECDSA-P256-SHA256"
    );
}

/// The signing key is written to a private temporary file only for the
/// duration of signing; nothing is left behind afterwards.
#[tokio::test]
async fn signing_key_temp_file_is_removed_after_emission() {
    let dir = tempfile::tempdir().unwrap();
    let storage = local_storage(dir.path());
    let evidence = evidence_config(&["json"], Some(TEST_PRIVATE_KEY_PKCS8));
    let manifest = manifest_bytes();
    let report = report(&manifest, &storage, Some(&evidence), CheckOutcome::Passed);

    let key_dir = std::env::temp_dir();
    let leftover = |dir: &Path| {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            // Regular, small files only: $TMPDIR can hold sockets and FIFOs.
            .filter(|e| {
                e.metadata()
                    .map(|m| m.is_file() && m.len() < 4096)
                    .unwrap_or(false)
            })
            .filter(|e| {
                std::fs::read_to_string(e.path())
                    .map(|c| c.contains("MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgVF1f"))
                    .unwrap_or(false)
            })
            .count()
    };
    let before = leftover(&key_dir);
    emit_validation_evidence(backend(&storage).as_ref(), &report, &evidence)
        .await
        .expect("signed evidence emitted");
    assert_eq!(leftover(&key_dir), before, "signing key left on disk");
}

/// The CLI records `format!("{:?}", storage_config)` as the storage backend,
/// which embeds account keys and secrets in the signed report. The operator
/// records a credential-free description instead.
#[test]
fn report_describes_storage_without_credentials() {
    let storage = azure_backup_storage();
    let evidence = evidence_config(&["json"], None);
    let manifest = manifest_bytes();
    let report = report(&manifest, &storage, Some(&evidence), CheckOutcome::Passed);

    assert_eq!(
        report.backup.storage_backend,
        "azure://kafkabackups/backups/prod"
    );
    let serialized = String::from_utf8(report.to_pretty_json().unwrap()).unwrap();
    assert!(!serialized.contains(AZURE_ACCOUNT_KEY));

    let s3 = ResolvedStorage::S3(S3StorageConfig {
        bucket: "kafka-backups".to_string(),
        region: "eu-west-2".to_string(),
        endpoint: Some("http://minio:9000".to_string()),
        path_style: true,
        allow_http: true,
        prefix: None,
        access_key_id: "AKIAEXAMPLE".to_string(),
        secret_access_key: "s3-secret-not-for-reports".to_string(),
    });
    let described = describe_storage(&s3);
    assert_eq!(described, "s3://kafka-backups (endpoint http://minio:9000)");
    assert!(!described.contains("s3-secret-not-for-reports"));
    assert!(!described.contains("AKIAEXAMPLE"));
}

/// The report carries provenance: the manifest digest, who triggered it, and
/// the operator as the emitting tool.
#[test]
fn report_records_manifest_digest_and_provenance() {
    let storage = azure_backup_storage();
    let evidence = evidence_config(&["json"], None);
    let manifest = manifest_bytes();
    let report = report(&manifest, &storage, Some(&evidence), CheckOutcome::Passed);

    assert_eq!(
        report.integrity.backup_manifest_sha256,
        kafka_backup_core::evidence::sha256_hex(&manifest)
    );
    assert_eq!(report.tool.name, "kafka-backup-operator");
    assert_eq!(report.tool.version, env!("CARGO_PKG_VERSION"));
    assert_eq!(
        report.triggered_by.as_deref(),
        Some("kafka-backup-operator:kbo/orders-validation")
    );
}

/// `spec.evidence.storage` is where evidence goes; without it, evidence is
/// written next to the backup, as the CLI does.
#[test]
fn evidence_goes_to_its_own_storage_when_configured() {
    let backup_storage = azure_backup_storage();
    let mut evidence = evidence_config(&["json"], None);
    assert!(matches!(
        evidence_storage(&evidence, &backup_storage),
        ResolvedStorage::Azure(a) if a.container == "backups"
    ));

    evidence.storage = Some(ResolvedStorage::Azure(AzureStorageConfig {
        container: "evidence".to_string(),
        account_name: "kafkaevidence".to_string(),
        auth: AzureAuthMethod::WorkloadIdentity,
        prefix: Some("compliance/kafka".to_string()),
        endpoint: None,
    }));
    assert!(matches!(
        evidence_storage(&evidence, &backup_storage),
        ResolvedStorage::Azure(a) if a.container == "evidence"
    ));
}

/// Report IDs name the resource and run, and include part of the object UID
/// so same-named validations in different namespaces sharing one bucket
/// cannot overwrite each other's evidence.
#[test]
fn report_id_names_the_resource_run_and_object() {
    let now = Utc.with_ymd_and_hms(2026, 11, 4, 6, 50, 0).unwrap();
    assert_eq!(
        evidence_report_id(
            "orders-validation",
            Some("3f2a9c1e-77b0-4a8e-9a43-5e0c1d2b3a4f"),
            now
        ),
        "orders-validation-20261104-065000-3f2a9c1e"
    );
    assert_ne!(
        evidence_report_id("nightly", Some("aaaaaaaa-0000"), now),
        evidence_report_id("nightly", Some("bbbbbbbb-0000"), now)
    );
    assert_eq!(
        evidence_report_id("orders-validation", None, now),
        "orders-validation-20261104-065000"
    );
}

/// Status reports every artifact and the digest; absent artifacts are
/// written as null so a merge patch clears paths left by an earlier run.
#[test]
fn status_reports_every_evidence_artifact() {
    let outcome = kafka_backup_operator::adapters::EvidenceOutcome {
        report_id: "orders-validation-20261104-065000-3f2a9c1e".to_string(),
        json_path: Some("evidence-reports/r/2026/11/r.json".to_string()),
        pdf_path: Some("evidence-reports/r/2026/11/r.pdf".to_string()),
        signature_path: None,
        report_sha256: "ab".repeat(32),
        signed: false,
        warnings: vec![],
    };

    let fields = evidence_status_fields(Some(&outcome));
    assert_eq!(
        serde_json::Value::Object(fields),
        json!({
            "evidenceReportId": "orders-validation-20261104-065000-3f2a9c1e",
            "evidenceReportPath": "evidence-reports/r/2026/11/r.json",
            "evidencePdfPath": "evidence-reports/r/2026/11/r.pdf",
            "evidenceSignaturePath": null,
            "evidenceReportSha256": "ab".repeat(32),
            "evidenceReportSigned": false,
        })
    );

    let cleared = evidence_status_fields(None);
    assert_eq!(cleared.len(), 6);
    assert!(cleared.values().all(serde_json::Value::is_null));
}

/// Formats the operator cannot produce are rejected at spec validation
/// instead of being dropped silently.
#[test]
fn unknown_or_empty_evidence_formats_are_rejected() {
    assert!(validate(&validation(json!({"formats": ["json", "pdf"]}))).is_ok());
    assert!(validate(&validation(json!({"formats": ["JSON", "Pdf"]}))).is_ok());
    assert!(validate(&validation(json!({}))).is_ok(), "default is json");

    let err = validate(&validation(json!({"formats": ["json", "html"]})))
        .expect_err("html is not a supported format");
    assert!(err.to_string().contains("html"), "{err}");

    let err = validate(&validation(json!({"formats": []})))
        .expect_err("an empty format list produces nothing");
    assert!(err.to_string().contains("evidence.formats"), "{err}");
}

/// Core parses PKCS#8 only. A SEC1 key (`openssl ecparam -genkey`) is
/// rejected with the conversion command rather than a parser error.
#[test]
fn sec1_signing_keys_are_rejected_with_a_conversion_hint() {
    assert!(check_signing_key_pem(TEST_PRIVATE_KEY_PKCS8).is_ok());

    let sec1 = "-----BEGIN EC PRIVATE KEY-----\nMHcCAQEE\n-----END EC PRIVATE KEY-----\n";
    let err = check_signing_key_pem(sec1).expect_err("SEC1 is not PKCS#8");
    let msg = err.to_string();
    assert!(msg.contains("PKCS#8"), "{msg}");
    assert!(msg.contains("openssl pkcs8 -topk8 -nocrypt"), "{msg}");
    assert!(
        !msg.contains("MHcCAQEE"),
        "error must not echo key material"
    );

    assert!(check_signing_key_pem("not a key").is_err());
}

/// `spec.notifications` was resolved and never sent. Slack now receives the
/// validation result with the evidence location.
#[tokio::test]
async fn slack_notification_is_sent_with_the_result() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/hook", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) =
            tokio::time::timeout(std::time::Duration::from_secs(10), listener.accept())
                .await
                .expect("no Slack webhook request within 10s")
                .unwrap();
        let mut request = Vec::new();
        let mut buf = [0u8; 4096];
        loop {
            let n = socket.read(&mut buf).await.unwrap();
            request.extend_from_slice(&buf[..n]);
            let text = String::from_utf8_lossy(&request);
            if let Some(header_end) = text.find("\r\n\r\n") {
                let content_length = text[..header_end]
                    .lines()
                    .find_map(|l| {
                        l.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|v| v.trim().parse::<usize>().unwrap())
                    })
                    .unwrap_or(0);
                if request.len() >= header_end + 4 + content_length {
                    break;
                }
            }
            if n == 0 {
                break;
            }
        }
        socket
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok")
            .await
            .unwrap();
        String::from_utf8(request).unwrap()
    });

    let storage = azure_backup_storage();
    let manifest = manifest_bytes();
    let report = report(&manifest, &storage, None, CheckOutcome::Passed);
    let notifications = ResolvedNotificationsConfig {
        slack_webhook_url: Some(url),
        pagerduty_routing_key: None,
        pagerduty_severity: None,
    };

    let failures =
        send_validation_notifications(&notifications, &report, "evidence-reports/r/2026/11/r.json")
            .await;
    assert!(failures.is_empty(), "{failures:?}");

    let request = server.await.unwrap();
    assert!(request.starts_with("POST /hook"), "{request}");
    assert!(request.contains("PASSED"), "{request}");
    assert!(
        request.contains("evidence-reports/r/2026/11/r.json"),
        "{request}"
    );
}

/// A notification failure is reported back, not raised: the validation and
/// its evidence already succeeded.
#[tokio::test]
async fn notification_failures_are_returned_as_warnings() {
    let storage = azure_backup_storage();
    let manifest = manifest_bytes();
    let report = report(&manifest, &storage, None, CheckOutcome::Failed);
    let notifications = ResolvedNotificationsConfig {
        // Nothing listens on the discard port.
        slack_webhook_url: Some("http://127.0.0.1:9/hook".to_string()),
        pagerduty_routing_key: None,
        pagerduty_severity: None,
    };

    let failures = send_validation_notifications(&notifications, &report, "").await;
    assert_eq!(failures.len(), 1);
    assert!(failures[0].contains("Slack"), "{failures:?}");
}
