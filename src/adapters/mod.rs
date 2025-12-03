//! Adapters for converting CRD specs to kafka-backup-core configuration types

mod backup_config;
mod core_integration;
mod restore_config;
mod secrets;
mod storage_config;

pub use backup_config::*;
pub use core_integration::*;
pub use restore_config::*;
pub use secrets::*;
pub use storage_config::*;
