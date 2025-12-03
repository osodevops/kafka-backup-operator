//! Adapters for converting CRD specs to kafka-backup-core configuration types

mod backup_config;
mod restore_config;
mod secrets;
mod storage_config;

pub use backup_config::*;
pub use restore_config::*;
pub use secrets::*;
pub use storage_config::*;
