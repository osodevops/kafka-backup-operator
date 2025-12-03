//! KafkaOffsetRollback Custom Resource Definition

use chrono::{DateTime, Utc};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{Condition, KafkaClusterSpec};

/// KafkaOffsetRollback resource specification
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "kafka.oso.sh",
    version = "v1alpha1",
    kind = "KafkaOffsetRollback",
    plural = "kafkaoffsetrollbacks",
    singular = "kafkaoffsetrollback",
    shortname = "korb",
    namespaced,
    status = "KafkaOffsetRollbackStatus",
    printcolumn = r#"{"name": "Phase", "type": "string", "jsonPath": ".status.phase"}"#,
    printcolumn = r#"{"name": "Groups", "type": "integer", "jsonPath": ".status.groupsRolledBack"}"#,
    printcolumn = r#"{"name": "Age", "type": "date", "jsonPath": ".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct KafkaOffsetRollbackSpec {
    /// Reference to snapshot to restore from
    pub snapshot_ref: SnapshotRef,

    /// Target Kafka cluster
    pub kafka_cluster: KafkaClusterSpec,

    /// Consumer groups to rollback (empty = all groups in snapshot)
    #[serde(default)]
    pub consumer_groups: Vec<String>,

    /// Dry run mode
    #[serde(default)]
    pub dry_run: bool,

    /// Verify after rollback
    #[serde(default = "default_true")]
    pub verify_after_rollback: bool,
}

fn default_true() -> bool {
    true
}

/// Snapshot reference for rollback
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotRef {
    /// Snapshot name/ID
    pub name: String,

    /// PVC containing the snapshot
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pvc_name: Option<String>,

    /// Path to snapshot file within PVC
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,

    /// Reference to KafkaRestore that created the snapshot
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restore_ref: Option<String>,

    /// Reference to KafkaOffsetReset that created the snapshot
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset_reset_ref: Option<String>,
}

/// KafkaOffsetRollback status
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct KafkaOffsetRollbackStatus {
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

    /// Groups successfully rolled back
    #[serde(skip_serializing_if = "Option::is_none")]
    pub groups_rolled_back: Option<usize>,

    /// Groups that failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub groups_failed: Option<usize>,

    /// Verification result
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification: Option<VerificationResult>,

    /// Observed generation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Status conditions
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// Verification result after rollback
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct VerificationResult {
    /// Whether all offsets matched expected values
    pub all_matched: bool,

    /// Total groups verified
    pub total_groups: usize,

    /// Groups that matched
    pub matched_groups: usize,

    /// Groups with mismatches
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mismatched_groups: Vec<String>,
}
