//! Kubernetes controllers for Kafka Backup CRDs
//!
//! This module contains the controller implementations that watch for CRD changes
//! and trigger reconciliation.

mod backup_controller;
mod offset_reset_controller;
mod offset_rollback_controller;
mod restore_controller;

pub use backup_controller::run as run_backup_controller;
pub use offset_reset_controller::run as run_offset_reset_controller;
pub use offset_rollback_controller::run as run_offset_rollback_controller;
pub use restore_controller::run as run_restore_controller;

use kube::Client;

/// Shared context for all controllers
pub struct Context {
    /// Kubernetes client
    pub client: Client,
}

impl Context {
    /// Create a new context
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}
