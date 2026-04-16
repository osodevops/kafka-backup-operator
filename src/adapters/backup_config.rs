//! Backup configuration adapter
//!
//! Converts KafkaBackup CRD spec to kafka-backup-core configuration.

use kube::Client;

use crate::crd::{
    CheckpointSpec, CircuitBreakerSpec, KafkaBackup, KafkaClusterSpec, KafkaConnectionSpec,
    MetricsSpec, RateLimitingSpec,
};
use crate::error::Result;

use super::secrets::{get_sasl_credentials, get_tls_credentials_split, TlsCredentials};
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
    /// Metrics settings
    pub metrics: Option<ResolvedMetricsConfig>,
    /// Backup segment and mode settings
    pub backup_options: ResolvedBackupOptionsConfig,
}

/// Resolved Kafka cluster configuration with credentials
#[derive(Debug, Clone)]
pub struct ResolvedKafkaConfig {
    pub bootstrap_servers: Vec<String>,
    pub security_protocol: String,
    pub tls: Option<TlsCredentials>,
    pub sasl: Option<SaslCredentials>,
    pub connection: ResolvedKafkaConnectionConfig,
}

/// Resolved Kafka connection tuning
#[derive(Debug, Clone)]
pub struct ResolvedKafkaConnectionConfig {
    pub tcp_keepalive: bool,
    pub keepalive_time_secs: u64,
    pub keepalive_interval_secs: u64,
    pub tcp_nodelay: bool,
    pub connections_per_broker: usize,
}

impl Default for ResolvedKafkaConnectionConfig {
    fn default() -> Self {
        Self {
            tcp_keepalive: true,
            keepalive_time_secs: 60,
            keepalive_interval_secs: 20,
            tcp_nodelay: true,
            connections_per_broker: 4,
        }
    }
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

/// Resolved metrics configuration
#[derive(Debug, Clone)]
pub struct ResolvedMetricsConfig {
    pub enabled: bool,
    pub port: u16,
    pub bind_address: String,
    pub path: String,
    pub update_interval_ms: u64,
    pub max_partition_labels: usize,
}

/// Resolved backup behavior and tuning options
#[derive(Debug, Clone)]
pub struct ResolvedBackupOptionsConfig {
    pub segment_max_bytes: u64,
    pub segment_max_interval_ms: u64,
    pub continuous: bool,
    pub stop_at_current_offsets: bool,
    pub include_offset_headers: bool,
    pub source_cluster_id: Option<String>,
    pub poll_interval_ms: u64,
    pub consumer_group_snapshot: bool,
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
    let checkpoint = backup.spec.checkpoint.as_ref().map(build_checkpoint_config);

    // Build rate limiting config
    let rate_limiting = backup
        .spec
        .rate_limiting
        .as_ref()
        .map(build_rate_limiting_config);

    // Build circuit breaker config
    let circuit_breaker = backup
        .spec
        .circuit_breaker
        .as_ref()
        .map(build_circuit_breaker_config);

    // Build metrics config
    let metrics = backup.spec.metrics.as_ref().map(build_metrics_config);

    let backup_options = ResolvedBackupOptionsConfig {
        segment_max_bytes: backup.spec.segment_max_bytes,
        segment_max_interval_ms: backup.spec.segment_max_interval_ms,
        continuous: backup.spec.continuous,
        stop_at_current_offsets: backup.spec.stop_at_current_offsets,
        include_offset_headers: backup.spec.include_offset_headers,
        source_cluster_id: backup.spec.source_cluster_id.clone(),
        poll_interval_ms: backup.spec.poll_interval_ms,
        consumer_group_snapshot: backup.spec.consumer_group_snapshot,
    };

    Ok(ResolvedBackupConfig {
        kafka,
        topics: backup.spec.topics.clone(),
        storage,
        compression,
        checkpoint,
        rate_limiting,
        circuit_breaker,
        metrics,
        backup_options,
    })
}

/// Build Kafka cluster configuration with resolved credentials
pub async fn build_kafka_config(
    kafka: &KafkaClusterSpec,
    client: &Client,
    namespace: &str,
) -> Result<ResolvedKafkaConfig> {
    // Resolve TLS credentials if configured
    let has_tls = kafka.tls_secret.is_some();
    let has_ca = kafka.ca_secret.is_some();
    let tls = if has_tls || has_ca {
        Some(
            get_tls_credentials_split(
                client,
                namespace,
                kafka.tls_secret.as_ref().map(|t| t.name.as_str()),
                kafka.tls_secret.as_ref().map(|t| t.ca_key.as_str()).unwrap_or("ca.crt"),
                kafka.tls_secret.as_ref().and_then(|t| t.cert_key.as_deref()),
                kafka.tls_secret.as_ref().and_then(|t| t.key_key.as_deref()),
                kafka.ca_secret.as_ref().map(|c| c.name.as_str()),
                kafka.ca_secret.as_ref().map(|c| c.ca_key.as_str()),
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
        connection: kafka
            .connection
            .as_ref()
            .map(build_connection_config)
            .unwrap_or_default(),
    })
}

fn build_connection_config(connection: &KafkaConnectionSpec) -> ResolvedKafkaConnectionConfig {
    ResolvedKafkaConnectionConfig {
        tcp_keepalive: connection.tcp_keepalive,
        keepalive_time_secs: connection.keepalive_time_secs,
        keepalive_interval_secs: connection.keepalive_interval_secs,
        tcp_nodelay: connection.tcp_nodelay,
        connections_per_broker: connection.connections_per_broker,
    }
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

fn build_circuit_breaker_config(
    circuit_breaker: &CircuitBreakerSpec,
) -> ResolvedCircuitBreakerConfig {
    ResolvedCircuitBreakerConfig {
        enabled: circuit_breaker.enabled,
        failure_threshold: circuit_breaker.failure_threshold,
        reset_timeout_secs: circuit_breaker.reset_timeout_secs,
        success_threshold: circuit_breaker.success_threshold,
        operation_timeout_ms: circuit_breaker.operation_timeout_ms,
    }
}

fn build_metrics_config(metrics: &MetricsSpec) -> ResolvedMetricsConfig {
    ResolvedMetricsConfig {
        enabled: metrics.enabled,
        port: metrics.port,
        bind_address: metrics.bind_address.clone(),
        path: metrics.path.clone(),
        update_interval_ms: metrics.update_interval_ms,
        max_partition_labels: metrics.max_partition_labels,
    }
}
