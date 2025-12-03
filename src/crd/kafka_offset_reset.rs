//! KafkaOffsetReset Custom Resource Definition

use chrono::{DateTime, Utc};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{Condition, KafkaClusterSpec};

/// KafkaOffsetReset resource specification
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "kafka.oso.sh",
    version = "v1alpha1",
    kind = "KafkaOffsetReset",
    plural = "kafkaoffsetresets",
    singular = "kafkaoffsetreset",
    shortname = "kor",
    namespaced,
    status = "KafkaOffsetResetStatus",
    printcolumn = r#"{"name": "Phase", "type": "string", "jsonPath": ".status.phase"}"#,
    printcolumn = r#"{"name": "Groups", "type": "integer", "jsonPath": ".status.groupsReset"}"#,
    printcolumn = r#"{"name": "Failed", "type": "integer", "jsonPath": ".status.groupsFailed"}"#,
    printcolumn = r#"{"name": "Age", "type": "date", "jsonPath": ".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct KafkaOffsetResetSpec {
    /// Target Kafka cluster
    pub kafka_cluster: KafkaClusterSpec,

    /// Consumer groups to reset
    pub consumer_groups: Vec<String>,

    /// Reset strategy
    pub reset_strategy: OffsetResetStrategy,

    /// Target timestamp for to-timestamp strategy (epoch ms)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reset_timestamp: Option<i64>,

    /// Target offset for to-offset strategy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reset_offset: Option<i64>,

    /// Topics to reset (empty = all topics for the group)
    #[serde(default)]
    pub topics: Vec<String>,

    /// Parallelism for bulk reset
    #[serde(default = "default_parallelism")]
    pub parallelism: usize,

    /// Dry run mode
    #[serde(default)]
    pub dry_run: bool,

    /// Continue on error (don't fail entire operation if one group fails)
    #[serde(default)]
    pub continue_on_error: bool,

    /// Reference to offset mapping from a restore operation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset_mapping_ref: Option<OffsetMappingRef>,

    /// Snapshot before reset for rollback
    #[serde(default = "default_true")]
    pub snapshot_before_reset: bool,
}

fn default_parallelism() -> usize {
    50
}

fn default_true() -> bool {
    true
}

/// Offset reset strategy
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum OffsetResetStrategy {
    /// Reset to earliest offset
    ToEarliest,
    /// Reset to latest offset
    ToLatest,
    /// Reset to specific timestamp
    ToTimestamp,
    /// Reset to specific offset
    ToOffset,
    /// Reset using offset mapping from restore
    FromMapping,
}

/// Reference to offset mapping from restore operation
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OffsetMappingRef {
    /// KafkaRestore resource name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restore_name: Option<String>,

    /// Direct path to offset mapping file
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,

    /// PVC containing the mapping
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pvc_name: Option<String>,
}

/// KafkaOffsetReset status
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct KafkaOffsetResetStatus {
    /// Current phase (Pending, Running, Completed, Failed, PartiallyCompleted)
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

    /// Total groups to reset
    #[serde(skip_serializing_if = "Option::is_none")]
    pub groups_total: Option<usize>,

    /// Groups successfully reset
    #[serde(skip_serializing_if = "Option::is_none")]
    pub groups_reset: Option<usize>,

    /// Groups that failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub groups_failed: Option<usize>,

    /// Duration (human-readable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<String>,

    /// Snapshot ID for rollback
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<String>,

    /// Snapshot path
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_path: Option<String>,

    /// Per-group results
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_results: Vec<GroupResetResult>,

    /// Observed generation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Status conditions
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// Per-group reset result
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GroupResetResult {
    /// Consumer group ID
    pub group_id: String,

    /// Success status
    pub success: bool,

    /// Error message if failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// Number of partitions reset
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partitions_reset: Option<usize>,
}
