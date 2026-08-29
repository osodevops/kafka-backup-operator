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
