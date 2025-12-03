//! Backup configuration adapter
//!
//! Converts KafkaBackup CRD spec to kafka-backup-core configuration.

use kube::Client;

use crate::crd::{
    CheckpointSpec, CircuitBreakerSpec, KafkaBackup, KafkaClusterSpec, RateLimitingSpec,
};
use crate::error::{Error, Result};

use super::secrets::{get_sasl_credentials, get_tls_credentials, TlsCredentials};
use super::storage_config::{build_storage_config, ResolvedStorage};

/// Fully resolved backup configuration
#[derive(Debug, Clone)]
pub struct ResolvedBackupConfig {
    /// Kafka cluster configuration
    pub kafka: ResolvedKafkaConfig,
    /// Topics to backup
    pub topics: Vec<String>,
    /// Storage configuration
    pub storage: ResolvedStorage,
    /// Compression settings
    pub compression: CompressionConfig,
    /// Checkpoint settings
    pub checkpoint: Option<ResolvedCheckpointConfig>,
    /// Rate limiting settings
    pub rate_limiting: Option<ResolvedRateLimitingConfig>,
    /// Circuit breaker settings
    pub circuit_breaker: Option<ResolvedCircuitBreakerConfig>,
}

/// Resolved Kafka cluster configuration with credentials
#[derive(Debug, Clone)]
pub struct ResolvedKafkaConfig {
    pub bootstrap_servers: Vec<String>,
    pub security_protocol: String,
    pub tls: Option<TlsCredentials>,
    pub sasl: Option<SaslCredentials>,
}

/// SASL credentials
#[derive(Debug, Clone)]
pub struct SaslCredentials {
    pub mechanism: String,
    pub username: String,
    pub password: String,
}

/// Compression configuration
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    pub algorithm: String,
    pub level: i32,
}

/// Resolved checkpoint configuration
#[derive(Debug, Clone)]
pub struct ResolvedCheckpointConfig {
    pub enabled: bool,
    pub interval_secs: u64,
    pub storage_path: Option<String>,
}

/// Resolved rate limiting configuration
#[derive(Debug, Clone)]
pub struct ResolvedRateLimitingConfig {
    pub records_per_sec: u64,
    pub bytes_per_sec: u64,
    pub max_concurrent_partitions: usize,
}

/// Resolved circuit breaker configuration
#[derive(Debug, Clone)]
pub struct ResolvedCircuitBreakerConfig {
    pub enabled: bool,
    pub failure_threshold: u32,
    pub reset_timeout_secs: u64,
    pub success_threshold: u32,
    pub operation_timeout_ms: u64,
}

/// Build fully resolved backup configuration from CRD
pub async fn build_backup_config(
    backup: &KafkaBackup,
    client: &Client,
    namespace: &str,
) -> Result<ResolvedBackupConfig> {
    // Resolve Kafka configuration
    let kafka = build_kafka_config(&backup.spec.kafka_cluster, client, namespace).await?;

    // Resolve storage configuration
    let storage = build_storage_config(&backup.spec.storage, client, namespace).await?;

    // Build compression config
    let compression = CompressionConfig {
        algorithm: backup.spec.compression.clone(),
        level: backup.spec.compression_level,
    };

    // Build checkpoint config
    let checkpoint = backup
        .spec
        .checkpoint
        .as_ref()
        .map(|c| build_checkpoint_config(c));

    // Build rate limiting config
    let rate_limiting = backup
        .spec
        .rate_limiting
        .as_ref()
        .map(|r| build_rate_limiting_config(r));

    // Build circuit breaker config
    let circuit_breaker = backup
        .spec
        .circuit_breaker
        .as_ref()
        .map(|c| build_circuit_breaker_config(c));

    Ok(ResolvedBackupConfig {
        kafka,
        topics: backup.spec.topics.clone(),
        storage,
        compression,
        checkpoint,
        rate_limiting,
        circuit_breaker,
    })
}

/// Build Kafka cluster configuration with resolved credentials
pub async fn build_kafka_config(
    kafka: &KafkaClusterSpec,
    client: &Client,
    namespace: &str,
) -> Result<ResolvedKafkaConfig> {
    // Resolve TLS credentials if configured
    let tls = if let Some(tls_ref) = &kafka.tls_secret {
        Some(
            get_tls_credentials(
                client,
                namespace,
                &tls_ref.name,
                &tls_ref.ca_key,
                tls_ref.cert_key.as_deref(),
                tls_ref.key_key.as_deref(),
            )
            .await?,
        )
    } else {
        None
    };

    // Resolve SASL credentials if configured
    let sasl = if let Some(sasl_ref) = &kafka.sasl_secret {
        let (username, password) = get_sasl_credentials(
            client,
            namespace,
            &sasl_ref.name,
            &sasl_ref.username_key,
            &sasl_ref.password_key,
        )
        .await?;

        Some(SaslCredentials {
            mechanism: sasl_ref.mechanism.clone(),
            username,
            password,
        })
    } else {
        None
    };

    Ok(ResolvedKafkaConfig {
        bootstrap_servers: kafka.bootstrap_servers.clone(),
        security_protocol: kafka.security_protocol.clone(),
        tls,
        sasl,
    })
}

fn build_checkpoint_config(checkpoint: &CheckpointSpec) -> ResolvedCheckpointConfig {
    let storage_path = checkpoint.storage.as_ref().map(|s| {
        let base = format!("/checkpoints/{}", s.pvc_name);
        match &s.sub_path {
            Some(sub) => format!("{}/{}", base, sub),
            None => base,
        }
    });

    ResolvedCheckpointConfig {
        enabled: checkpoint.enabled,
        interval_secs: checkpoint.interval_secs,
        storage_path,
    }
}

fn build_rate_limiting_config(rate_limiting: &RateLimitingSpec) -> ResolvedRateLimitingConfig {
    ResolvedRateLimitingConfig {
        records_per_sec: rate_limiting.records_per_sec,
        bytes_per_sec: rate_limiting.bytes_per_sec,
        max_concurrent_partitions: rate_limiting.max_concurrent_partitions,
    }
}

fn build_circuit_breaker_config(circuit_breaker: &CircuitBreakerSpec) -> ResolvedCircuitBreakerConfig {
    ResolvedCircuitBreakerConfig {
        enabled: circuit_breaker.enabled,
        failure_threshold: circuit_breaker.failure_threshold,
        reset_timeout_secs: circuit_breaker.reset_timeout_secs,
        success_threshold: circuit_breaker.success_threshold,
        operation_timeout_ms: circuit_breaker.operation_timeout_ms,
    }
}
