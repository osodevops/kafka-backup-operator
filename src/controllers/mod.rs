//! Kubernetes controllers for Kafka Backup CRDs
//!
//! This module contains the controller implementations that watch for CRD changes
//! and trigger reconciliation.

mod backup_controller;
mod offset_reset_controller;
mod offset_rollback_controller;
mod restore_controller;
mod validation_controller;

pub use backup_controller::run as run_backup_controller;
pub use offset_reset_controller::run as run_offset_reset_controller;
pub use offset_rollback_controller::run as run_offset_rollback_controller;
pub use restore_controller::run as run_restore_controller;
pub use validation_controller::run as run_validation_controller;

use std::fmt::Debug;
use std::hash::Hash;
use std::time::Duration;

use kube::runtime::reflector::Store;
use kube::{Client, Resource};

use crate::metrics;

/// How often the managed-resource gauges are refreshed from the watch caches.
const MANAGED_RESOURCE_REFRESH: Duration = Duration::from_secs(30);

/// Keep `kafka_backup_operator_managed_resources` in step with a controller's
/// reflector cache.
///
/// Reading the cache rather than listing the API server keeps this free: the
/// watch already holds every object the controller manages.
pub(crate) fn track_managed_resources<K>(store: Store<K>, kind: &'static str)
where
    K: Resource + Clone + Send + Sync + 'static,
    K::DynamicType: Eq + Hash + Clone + Default + Debug,
{
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(MANAGED_RESOURCE_REFRESH);
        loop {
            ticker.tick().await;
            metrics::MANAGED_RESOURCES
                .with_label_values(&[kind])
                .set(store.state().len() as f64);
        }
    });
}

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
