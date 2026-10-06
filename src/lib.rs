//! OSO Kafka Backup Kubernetes Operator
//!
//! This operator manages Kafka backup and restore operations in Kubernetes
//! using Custom Resource Definitions (CRDs).

pub mod adapters;
pub mod controllers;
pub mod crd;
pub mod error;
pub mod leader;
pub mod metrics;
pub mod reconcilers;
pub mod shutdown;

pub use error::{Error, Result};

/// Install rustls' ring backend as the process-default crypto provider.
///
/// kafka-backup-core >= 0.23 links object_store 0.14 -> reqwest 0.13, which
/// compiles rustls' aws-lc-rs backend next to the ring backend kube and core
/// use. With two backends rustls cannot pick a default, and the first TLS
/// config built without an explicit provider (kube's client) panics. Call this
/// before creating a Kubernetes client; repeated calls are no-ops.
pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}
