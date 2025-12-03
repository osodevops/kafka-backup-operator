//! Error types for the Kafka Backup Operator

use thiserror::Error;

/// Result type alias using the operator's Error type
pub type Result<T> = std::result::Result<T, Error>;

/// Operator error types
#[derive(Error, Debug)]
pub enum Error {
    /// Kubernetes API error
    #[error("Kubernetes API error: {0}")]
    Kube(#[from] kube::Error),

    /// Core library error
    #[error("Kafka backup core error: {0}")]
    Core(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),

    /// Storage error
    #[error("Storage error: {0}")]
    Storage(String),

    /// Validation error
    #[error("Validation error: {0}")]
    Validation(String),

    /// Secret not found
    #[error("Secret not found: {0}")]
    SecretNotFound(String),

    /// Secret key not found
    #[error("Secret key '{key}' not found in secret '{secret}'")]
    SecretKeyNotFound { secret: String, key: String },

    /// PVC not found
    #[error("PVC not found: {0}")]
    PvcNotFound(String),

    /// Backup not found
    #[error("Backup not found: {0}")]
    BackupNotFound(String),

    /// Snapshot not found
    #[error("Snapshot not found: {0}")]
    SnapshotNotFound(String),

    /// Rollback error
    #[error("Rollback error: {0}")]
    Rollback(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Finalizer error
    #[error("Finalizer error: {0}")]
    Finalizer(#[source] Box<kube::runtime::finalizer::Error<Error>>),
}

impl Error {
    /// Create a configuration error
    pub fn config(msg: impl Into<String>) -> Self {
        Error::Config(msg.into())
    }

    /// Create a validation error
    pub fn validation(msg: impl Into<String>) -> Self {
        Error::Validation(msg.into())
    }

    /// Create a storage error
    pub fn storage(msg: impl Into<String>) -> Self {
        Error::Storage(msg.into())
    }

    /// Create a core library error
    pub fn core(msg: impl Into<String>) -> Self {
        Error::Core(msg.into())
    }
}
