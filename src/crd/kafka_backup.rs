//! KafkaBackup Custom Resource Definition

use chrono::{DateTime, Utc};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// KafkaBackup resource specification
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "kafka.oso.sh",
    version = "v1alpha1",
    kind = "KafkaBackup",
    plural = "kafkabackups",
    singular = "kafkabackup",
    shortname = "kb",
    namespaced,
    status = "KafkaBackupStatus",
    printcolumn = r#"{"name": "Phase", "type": "string", "jsonPath": ".status.phase"}"#,
    printcolumn = r#"{"name": "Last Backup", "type": "string", "jsonPath": ".status.lastBackupTime"}"#,
    printcolumn = r#"{"name": "Records", "type": "integer", "jsonPath": ".status.recordsProcessed"}"#,
    printcolumn = r#"{"name": "Resumable", "type": "boolean", "jsonPath": ".status.resumable"}"#,
    printcolumn = r#"{"name": "Age", "type": "date", "jsonPath": ".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct KafkaBackupSpec {
    /// Kafka cluster connection configuration
    pub kafka_cluster: KafkaClusterSpec,

    /// Topics to backup
    pub topics: Vec<String>,

    /// Storage configuration
    pub storage: StorageSpec,

    /// Compression algorithm (none, lz4, zstd)
    #[serde(default = "default_compression")]
    pub compression: String,

    /// Compression level (1-22 for zstd)
    #[serde(default = "default_compression_level")]
    pub compression_level: i32,

    /// Cron schedule for automated backups
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schedule: Option<String>,

    /// Checkpoint configuration for resumable backups
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<CheckpointSpec>,

    /// Rate limiting configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limiting: Option<RateLimitingSpec>,

    /// Circuit breaker configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub circuit_breaker: Option<CircuitBreakerSpec>,

    /// Suspend backups (useful for maintenance)
    #[serde(default)]
    pub suspend: bool,

    /// Metrics configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics: Option<MetricsSpec>,
}

fn default_compression() -> String {
    "zstd".to_string()
}

fn default_compression_level() -> i32 {
    3
}

/// Kafka cluster connection specification
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct KafkaClusterSpec {
    /// Bootstrap servers
    pub bootstrap_servers: Vec<String>,

    /// Security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)
    #[serde(default = "default_security_protocol")]
    pub security_protocol: String,

    /// TLS configuration secret reference
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls_secret: Option<TlsSecretRef>,

    /// SASL configuration secret reference
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sasl_secret: Option<SaslSecretRef>,
}

fn default_security_protocol() -> String {
    "PLAINTEXT".to_string()
}

/// TLS secret reference
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TlsSecretRef {
    /// Secret name
    pub name: String,
    /// CA certificate key in secret
    #[serde(default = "default_ca_key")]
    pub ca_key: String,
    /// Client certificate key in secret
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cert_key: Option<String>,
    /// Client key key in secret
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_key: Option<String>,
}

fn default_ca_key() -> String {
    "ca.crt".to_string()
}

/// SASL secret reference
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SaslSecretRef {
    /// Secret name
    pub name: String,
    /// SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
    pub mechanism: String,
    /// Username key in secret
    #[serde(default = "default_username_key")]
    pub username_key: String,
    /// Password key in secret
    #[serde(default = "default_password_key")]
    pub password_key: String,
}

fn default_username_key() -> String {
    "username".to_string()
}

fn default_password_key() -> String {
    "password".to_string()
}

/// Storage specification
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StorageSpec {
    /// Storage type (pvc, s3, azure, gcs)
    #[serde(default = "default_storage_type")]
    pub storage_type: String,

    /// PVC storage configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pvc: Option<PvcStorageSpec>,

    /// S3 storage configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub s3: Option<S3StorageSpec>,

    /// Azure Blob storage configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub azure: Option<AzureStorageSpec>,

    /// GCS storage configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gcs: Option<GcsStorageSpec>,
}

fn default_storage_type() -> String {
    "pvc".to_string()
}

/// PVC storage specification
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PvcStorageSpec {
    /// PVC claim name
    pub claim_name: String,

    /// Sub-path within the PVC
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_path: Option<String>,

    /// Auto-create PVC if not exists
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create: Option<PvcCreateSpec>,
}

/// PVC auto-creation specification
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PvcCreateSpec {
    /// Enable auto-creation
    #[serde(default)]
    pub enabled: bool,

    /// Storage class name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_class_name: Option<String>,

    /// Storage size (e.g., "100Gi")
    #[serde(default = "default_pvc_size")]
    pub size: String,

    /// Access modes
    #[serde(default = "default_access_modes")]
    pub access_modes: Vec<String>,
}

fn default_pvc_size() -> String {
    "100Gi".to_string()
}

fn default_access_modes() -> Vec<String> {
    vec!["ReadWriteOnce".to_string()]
}

/// S3 storage specification
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct S3StorageSpec {
    /// S3 bucket name
    pub bucket: String,

    /// AWS region
    pub region: String,

    /// Custom endpoint (for MinIO, Ceph, etc.)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,

    /// Path prefix within bucket
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,

    /// Credentials secret reference
    pub credentials_secret: S3CredentialsRef,
}

/// S3 credentials secret reference
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct S3CredentialsRef {
    /// Secret name
    pub name: String,

    /// Access key ID key in secret
    #[serde(default = "default_aws_access_key_id")]
    pub access_key_id_key: String,

    /// Secret access key key in secret
    #[serde(default = "default_aws_secret_access_key")]
    pub secret_access_key_key: String,
}

fn default_aws_access_key_id() -> String {
    "AWS_ACCESS_KEY_ID".to_string()
}

fn default_aws_secret_access_key() -> String {
    "AWS_SECRET_ACCESS_KEY".to_string()
}

/// Azure Blob storage specification
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AzureStorageSpec {
    /// Container name
    pub container: String,

    /// Storage account name
    pub account_name: String,

    /// Path prefix within container
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,

    /// Custom endpoint URL (for Azure Government, China, or private endpoints)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,

    /// Use Azure Workload Identity for authentication
    /// When true, the operator uses the pod's federated identity token
    /// to authenticate with Azure Blob Storage (requires AKS with Workload Identity enabled)
    /// This is auto-detected if AZURE_FEDERATED_TOKEN_FILE environment variable is present
    #[serde(default)]
    pub use_workload_identity: bool,

    /// Credentials secret reference for account key authentication
    /// Optional when using Workload Identity or Service Principal
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials_secret: Option<AzureCredentialsRef>,

    /// SAS token secret reference for time-limited access
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sas_token_secret: Option<AzureSasTokenRef>,

    /// Service Principal credentials for CI/CD pipelines
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_principal_secret: Option<AzureServicePrincipalRef>,
}

/// Azure credentials secret reference (account key)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AzureCredentialsRef {
    /// Secret name
    pub name: String,

    /// Account key key in secret
    #[serde(default = "default_azure_account_key")]
    pub account_key_key: String,
}

fn default_azure_account_key() -> String {
    "AZURE_STORAGE_KEY".to_string()
}

/// Azure SAS token secret reference
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AzureSasTokenRef {
    /// Secret name
    pub name: String,

    /// SAS token key in secret
    #[serde(default = "default_azure_sas_token")]
    pub sas_token_key: String,
}

fn default_azure_sas_token() -> String {
    "AZURE_SAS_TOKEN".to_string()
}

/// Azure Service Principal secret reference
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AzureServicePrincipalRef {
    /// Secret name
    pub name: String,

    /// Client ID key in secret
    #[serde(default = "default_azure_client_id")]
    pub client_id_key: String,

    /// Tenant ID key in secret
    #[serde(default = "default_azure_tenant_id")]
    pub tenant_id_key: String,

    /// Client secret key in secret
    #[serde(default = "default_azure_client_secret")]
    pub client_secret_key: String,
}

fn default_azure_client_id() -> String {
    "AZURE_CLIENT_ID".to_string()
}

fn default_azure_tenant_id() -> String {
    "AZURE_TENANT_ID".to_string()
}

fn default_azure_client_secret() -> String {
    "AZURE_CLIENT_SECRET".to_string()
}

/// GCS storage specification
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GcsStorageSpec {
    /// GCS bucket name
    pub bucket: String,

    /// Path prefix within bucket
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,

    /// Credentials secret reference
    pub credentials_secret: GcsCredentialsRef,
}

/// GCS credentials secret reference
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GcsCredentialsRef {
    /// Secret name
    pub name: String,

    /// Service account JSON key in secret
    #[serde(default = "default_gcs_service_account")]
    pub service_account_json_key: String,
}

fn default_gcs_service_account() -> String {
    "SERVICE_ACCOUNT_JSON".to_string()
}

/// Checkpoint configuration for resumable backups
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointSpec {
    /// Enable checkpointing
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Checkpoint interval in seconds
    #[serde(default = "default_checkpoint_interval")]
    pub interval_secs: u64,

    /// Separate checkpoint storage (defaults to backup storage)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage: Option<CheckpointStorageSpec>,
}

fn default_true() -> bool {
    true
}

fn default_checkpoint_interval() -> u64 {
    30
}

/// Checkpoint storage specification
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointStorageSpec {
    /// PVC name for checkpoints
    pub pvc_name: String,

    /// Sub-path within PVC
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_path: Option<String>,
}

/// Rate limiting configuration
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RateLimitingSpec {
    /// Maximum records per second (0 = unlimited)
    #[serde(default)]
    pub records_per_sec: u64,

    /// Maximum bytes per second (0 = unlimited)
    #[serde(default)]
    pub bytes_per_sec: u64,

    /// Maximum concurrent partitions
    #[serde(default = "default_max_concurrent_partitions")]
    pub max_concurrent_partitions: usize,
}

fn default_max_concurrent_partitions() -> usize {
    4
}

/// Circuit breaker configuration
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CircuitBreakerSpec {
    /// Enable circuit breaker
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Failure threshold before opening circuit
    #[serde(default = "default_failure_threshold")]
    pub failure_threshold: u32,

    /// Time to wait before attempting recovery (seconds)
    #[serde(default = "default_reset_timeout")]
    pub reset_timeout_secs: u64,

    /// Success threshold to close circuit
    #[serde(default = "default_success_threshold")]
    pub success_threshold: u32,

    /// Operation timeout (milliseconds)
    #[serde(default = "default_operation_timeout")]
    pub operation_timeout_ms: u64,
}

fn default_failure_threshold() -> u32 {
    5
}

fn default_reset_timeout() -> u64 {
    60
}

fn default_success_threshold() -> u32 {
    3
}

fn default_operation_timeout() -> u64 {
    30000
}

/// Metrics configuration for Prometheus metrics server
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MetricsSpec {
    /// Enable metrics collection (default: true)
    #[serde(default = "default_metrics_enabled")]
    pub enabled: bool,

    /// Port for metrics HTTP server (default: 9090)
    #[serde(default = "default_metrics_port")]
    pub port: u16,

    /// Bind address for metrics server (default: "0.0.0.0")
    #[serde(default = "default_metrics_bind_address")]
    pub bind_address: String,

    /// Metrics endpoint path (default: "/metrics")
    #[serde(default = "default_metrics_path")]
    pub path: String,

    /// Metrics update interval in milliseconds (default: 500)
    #[serde(default = "default_metrics_update_interval")]
    pub update_interval_ms: u64,

    /// Maximum partition labels to prevent cardinality explosion (default: 100)
    #[serde(default = "default_max_partition_labels")]
    pub max_partition_labels: usize,
}

fn default_metrics_enabled() -> bool {
    true
}

fn default_metrics_port() -> u16 {
    9090
}

fn default_metrics_bind_address() -> String {
    "0.0.0.0".to_string()
}

fn default_metrics_path() -> String {
    "/metrics".to_string()
}

fn default_metrics_update_interval() -> u64 {
    500
}

fn default_max_partition_labels() -> usize {
    100
}

/// KafkaBackup status
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct KafkaBackupStatus {
    /// Current phase (Pending, Running, Completed, Failed)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,

    /// Human-readable message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// Last backup timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_backup_time: Option<DateTime<Utc>>,

    /// Next scheduled backup timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_scheduled_backup: Option<DateTime<Utc>>,

    /// Records processed in current/last backup
    #[serde(skip_serializing_if = "Option::is_none")]
    pub records_processed: Option<u64>,

    /// Bytes processed in current/last backup
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_processed: Option<u64>,

    /// Segments completed in current/last backup
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segments_completed: Option<u64>,

    /// Whether checkpointing is enabled
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint_enabled: Option<bool>,

    /// Last checkpoint timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_checkpoint_time: Option<DateTime<Utc>>,

    /// Whether the backup can be resumed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resumable: Option<bool>,

    /// Throughput (records per second)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throughput_records_per_sec: Option<f64>,

    /// Throughput (bytes per second)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throughput_bytes_per_sec: Option<f64>,

    /// Current backup ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backup_id: Option<String>,

    /// Observed generation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Status conditions
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// Status condition
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    /// Condition type
    pub type_: String,

    /// Status (True, False, Unknown)
    pub status: String,

    /// Last transition time
    pub last_transition_time: DateTime<Utc>,

    /// Reason for the condition
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    /// Human-readable message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}
