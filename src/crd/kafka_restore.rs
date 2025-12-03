//! KafkaRestore Custom Resource Definition

use chrono::{DateTime, Utc};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    CircuitBreakerSpec, Condition, KafkaClusterSpec, RateLimitingSpec, StorageSpec,
};

/// KafkaRestore resource specification
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "kafka.oso.sh",
    version = "v1alpha1",
    kind = "KafkaRestore",
    plural = "kafkarestores",
    singular = "kafkarestore",
    shortname = "kr",
    namespaced,
    status = "KafkaRestoreStatus",
    printcolumn = r#"{"name": "Phase", "type": "string", "jsonPath": ".status.phase"}"#,
    printcolumn = r#"{"name": "Progress", "type": "string", "jsonPath": ".status.progressPercent"}"#,
    printcolumn = r#"{"name": "Records", "type": "integer", "jsonPath": ".status.recordsRestored"}"#,
    printcolumn = r#"{"name": "Age", "type": "date", "jsonPath": ".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct KafkaRestoreSpec {
    /// Reference to backup to restore from
    pub backup_ref: BackupRef,

    /// Target Kafka cluster (can differ from backup source)
    pub kafka_cluster: KafkaClusterSpec,

    /// Topics to restore (empty = all topics from backup)
    #[serde(default)]
    pub topics: Vec<String>,

    /// Point-in-time recovery configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pitr: Option<PitrSpec>,

    /// Topic remapping (source -> target)
    #[serde(default)]
    pub topic_mapping: std::collections::HashMap<String, String>,

    /// Partition remapping (source -> target)
    #[serde(default)]
    pub partition_mapping: std::collections::HashMap<i32, i32>,

    /// Consumer offset reset configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset_reset: Option<OffsetResetSpec>,

    /// Rollback safety configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rollback: Option<RollbackSpec>,

    /// Rate limiting configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limiting: Option<RateLimitingSpec>,

    /// Circuit breaker configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub circuit_breaker: Option<CircuitBreakerSpec>,

    /// Dry run mode (validate without executing)
    #[serde(default)]
    pub dry_run: bool,
}

/// Backup reference for restore
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackupRef {
    /// KafkaBackup resource name
    pub name: String,

    /// Namespace (defaults to same namespace)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,

    /// Specific backup ID (if multiple backups exist)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backup_id: Option<String>,

    /// Alternative: Direct storage reference (for external backups)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage: Option<StorageSpec>,
}

/// Point-in-time recovery specification
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PitrSpec {
    /// Start timestamp (epoch milliseconds)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_timestamp: Option<i64>,

    /// End timestamp (epoch milliseconds)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_timestamp: Option<i64>,

    /// Alternative: Start time as ISO 8601 string
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time: Option<DateTime<Utc>>,

    /// Alternative: End time as ISO 8601 string
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_time: Option<DateTime<Utc>>,
}

/// Offset reset specification for restore
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OffsetResetSpec {
    /// Enable offset reset as part of restore
    #[serde(default)]
    pub enabled: bool,

    /// Consumer groups to reset
    pub consumer_groups: Vec<String>,

    /// Reset strategy (manual, auto, dry_run)
    #[serde(default = "default_offset_strategy")]
    pub strategy: String,
}

fn default_offset_strategy() -> String {
    "manual".to_string()
}

/// Rollback safety specification
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RollbackSpec {
    /// Create snapshot before restore
    #[serde(default = "default_true")]
    pub snapshot_before_restore: bool,

    /// Snapshot retention (hours)
    #[serde(default = "default_retention_hours")]
    pub snapshot_retention_hours: u32,

    /// Auto-rollback on failure
    #[serde(default)]
    pub auto_rollback_on_failure: bool,

    /// Snapshot storage (defaults to operator storage)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_storage: Option<SnapshotStorageSpec>,
}

fn default_true() -> bool {
    true
}

fn default_retention_hours() -> u32 {
    24
}

/// Snapshot storage specification
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotStorageSpec {
    /// PVC name for snapshots
    pub pvc_name: String,

    /// Sub-path within PVC
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_path: Option<String>,
}

/// KafkaRestore status
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct KafkaRestoreStatus {
    /// Current phase (Pending, Running, Completed, Failed, RolledBack)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,

    /// Human-readable message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// Start time
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time: Option<DateTime<Utc>>,

    /// Completion time
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion_time: Option<DateTime<Utc>>,

    /// Progress percentage (0-100)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress_percent: Option<f64>,

    /// Records restored
    #[serde(skip_serializing_if = "Option::is_none")]
    pub records_restored: Option<u64>,

    /// Bytes restored
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_restored: Option<u64>,

    /// Segments processed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segments_processed: Option<u64>,

    /// Current topic being restored
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_topic: Option<String>,

    /// Throughput (records per second)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throughput_records_per_sec: Option<f64>,

    /// ETA (milliseconds)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eta_ms: Option<u64>,

    /// Rollback status
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rollback: Option<RollbackStatus>,

    /// Offset mapping path (for post-restore offset reset)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset_mapping_path: Option<String>,

    /// Observed generation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Status conditions
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// Rollback status information
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RollbackStatus {
    /// Snapshot ID
    pub snapshot_id: String,

    /// Snapshot time
    pub snapshot_time: DateTime<Utc>,

    /// Snapshot storage path
    pub snapshot_path: String,

    /// Whether rollback is available
    pub rollback_available: bool,

    /// Snapshot expiry time
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
}
