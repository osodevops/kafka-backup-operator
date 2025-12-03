//! Custom Resource Definitions for Kafka Backup Operator

mod kafka_backup;
mod kafka_offset_reset;
mod kafka_offset_rollback;
mod kafka_restore;

pub use kafka_backup::*;
pub use kafka_offset_reset::*;
pub use kafka_offset_rollback::*;
pub use kafka_restore::*;

use kube::CustomResourceExt;

/// Generate all CRD YAML manifests
pub fn generate_crds() -> Vec<String> {
    vec![
        serde_yaml::to_string(&KafkaBackup::crd()).unwrap(),
        serde_yaml::to_string(&KafkaRestore::crd()).unwrap(),
        serde_yaml::to_string(&KafkaOffsetReset::crd()).unwrap(),
        serde_yaml::to_string(&KafkaOffsetRollback::crd()).unwrap(),
    ]
}
