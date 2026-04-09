//! KafkaBackupValidation Custom Resource Definition

use chrono::{DateTime, Utc};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{Condition, KafkaClusterSpec, StorageSpec};

/// KafkaBackupValidation resource specification
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "kafka.oso.sh",
    version = "v1alpha1",
    kind = "KafkaBackupValidation",
    plural = "kafkabackupvalidations",
    singular = "kafkabackupvalidation",
    shortname = "kbv",
    namespaced,
    status = "KafkaBackupValidationStatus",
    printcolumn = r#"{"name": "Phase", "type": "string", "jsonPath": ".status.phase"}"#,
    printcolumn = r#"{"name": "Result", "type": "string", "jsonPath": ".status.validationResult"}"#,
    printcolumn = r#"{"name": "Checks", "type": "integer", "jsonPath": ".status.checksCompleted"}"#,
    printcolumn = r#"{"name": "Age", "type": "date", "jsonPath": ".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct KafkaBackupValidationSpec {
    /// Reference to the backup to validate
    pub backup_ref: BackupValidationRef,

    /// Target Kafka cluster (required for consumer group offset checks)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kafka_cluster: Option<KafkaClusterSpec>,

    /// Validation checks to run
    pub checks: ValidationChecksSpec,

    /// Evidence report configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evidence: Option<EvidenceSpec>,

    /// Notification channels
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notifications: Option<NotificationsSpec>,

    /// Cron schedule for recurring validation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schedule: Option<String>,

    /// Suspend validations
    #[serde(default)]
    pub suspend: bool,
}

/// Reference to backup for validation
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackupValidationRef {
    /// KafkaBackup resource name
    pub name: String,

    /// Namespace (defaults to same namespace)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,

    /// Specific backup ID to validate
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backup_id: Option<String>,

    /// Direct storage reference (alternative to KafkaBackup resource lookup)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage: Option<StorageSpec>,
}

/// Validation checks configuration
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ValidationChecksSpec {
    /// Message count check (compare record counts against backup manifest)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_count: Option<MessageCountCheckSpec>,

    /// Offset range check (verify backup covers expected offset ranges)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset_range: Option<OffsetRangeCheckSpec>,

    /// Consumer group offset check (verify consumer group positions are backed up)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer_group_offsets: Option<ConsumerGroupCheckSpec>,

    /// Custom webhook checks (call external validation endpoints)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub custom_webhooks: Vec<WebhookCheckSpec>,
}

/// Message count check specification
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MessageCountCheckSpec {
    /// Enable this check
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Acceptable difference threshold (number of records)
    #[serde(default)]
    pub fail_threshold: Option<u64>,

    /// Topics to check (empty = all topics from backup)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub topics: Vec<String>,
}

/// Offset range check specification
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OffsetRangeCheckSpec {
    /// Enable this check
    #[serde(default = "default_true")]
    pub enabled: bool,
}

/// Consumer group offset check specification
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerGroupCheckSpec {
    /// Enable this check
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Consumer groups to check (empty = all groups)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub consumer_groups: Vec<String>,
}

/// Custom webhook check specification
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WebhookCheckSpec {
    /// Display name for this check
    pub name: String,

    /// URL to POST validation payload to
    pub url: String,

    /// Timeout in seconds
    #[serde(default = "default_webhook_timeout")]
    pub timeout_secs: u64,

    /// Expected HTTP status code
    #[serde(default = "default_expected_status")]
    pub expected_status_code: u16,
}

fn default_webhook_timeout() -> u64 {
    120
}

fn default_expected_status() -> u16 {
    200
}

/// Evidence report configuration
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EvidenceSpec {
    /// Output formats (json, pdf)
    #[serde(default = "default_evidence_formats")]
    pub formats: Vec<String>,

    /// Cryptographic signing configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signing: Option<SigningSpec>,

    /// Storage location for evidence reports
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage: Option<StorageSpec>,

    /// Retention period in days
    #[serde(default = "default_retention_days")]
    pub retention_days: u32,
}

fn default_evidence_formats() -> Vec<String> {
    vec!["json".to_string()]
}

fn default_retention_days() -> u32 {
    90
}

/// Evidence signing configuration
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SigningSpec {
    /// Enable ECDSA-P256-SHA256 signing
    #[serde(default)]
    pub enabled: bool,

    /// Secret containing the ECDSA private key (PEM-encoded)
    pub key_secret: SigningKeyRef,
}

/// Reference to signing key secret
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SigningKeyRef {
    /// Secret name
    pub name: String,

    /// Key within secret containing the private key PEM
    #[serde(default = "default_private_key_key")]
    pub private_key_key: String,

    /// Key within secret containing the public key PEM
    #[serde(default = "default_public_key_key")]
    pub public_key_key: String,
}

fn default_private_key_key() -> String {
    "signing-key.pem".to_string()
}

fn default_public_key_key() -> String {
    "signing-key-pub.pem".to_string()
}

/// Notifications configuration
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NotificationsSpec {
    /// Slack notification
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slack: Option<SlackNotificationSpec>,

    /// PagerDuty notification
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagerduty: Option<PagerDutyNotificationSpec>,
}

/// Slack notification specification
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SlackNotificationSpec {
    /// Secret containing the webhook URL
    pub webhook_secret: SecretKeyRef,
}

/// PagerDuty notification specification
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PagerDutyNotificationSpec {
    /// Secret containing the integration/routing key
    pub routing_key_secret: SecretKeyRef,

    /// Alert severity (critical, error, warning, info)
    #[serde(default = "default_pagerduty_severity")]
    pub severity: String,
}

fn default_pagerduty_severity() -> String {
    "critical".to_string()
}

/// Generic secret key reference
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SecretKeyRef {
    /// Secret name
    pub name: String,

    /// Key within the secret
    pub key: String,
}

fn default_true() -> bool {
    true
}

/// KafkaBackupValidation status
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct KafkaBackupValidationStatus {
    /// Current phase (Pending, Running, Completed, Failed)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,

    /// Human-readable message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// Overall validation result (Pass, Fail, Warn)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validation_result: Option<String>,

    /// Validation start time
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time: Option<DateTime<Utc>>,

    /// Validation completion time
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion_time: Option<DateTime<Utc>>,

    /// Total number of checks configured
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checks_total: Option<u32>,

    /// Number of checks completed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checks_completed: Option<u32>,

    /// Number of checks passed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checks_passed: Option<u32>,

    /// Number of checks failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checks_failed: Option<u32>,

    /// Number of checks warned
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checks_warned: Option<u32>,

    /// Per-check results
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub check_results: Vec<CheckResultStatus>,

    /// Path to evidence report in storage
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evidence_report_path: Option<String>,

    /// Whether the evidence report is cryptographically signed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evidence_report_signed: Option<bool>,

    /// Last successful validation time
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_validation_time: Option<DateTime<Utc>>,

    /// Next scheduled validation time
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_scheduled_validation: Option<DateTime<Utc>>,

    /// Observed generation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Status conditions
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// Per-check result in status
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CheckResultStatus {
    /// Check name
    pub check_name: String,

    /// Outcome (Passed, Failed, Skipped, Warning)
    pub outcome: String,

    /// Human-readable detail
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// Duration in milliseconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
}
