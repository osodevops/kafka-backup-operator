//! Core library integration adapter
//!
//! Converts operator's resolved configuration types to kafka-backup-core types.

use std::path::PathBuf;

use kafka_backup_core::config::{
    BackupOptions, CompressionType, Config, KafkaConfig, Mode, OffsetStrategy, RestoreOptions,
    SaslMechanism, SecurityConfig, SecurityProtocol, StorageBackendType, StorageConfig,
    TopicSelection,
};

use super::backup_config::{ResolvedBackupConfig, ResolvedKafkaConfig};
use super::restore_config::ResolvedRestoreConfig;
use super::storage_config::ResolvedStorage;

/// Convert resolved backup configuration to kafka-backup-core Config
pub fn to_core_backup_config(
    resolved: &ResolvedBackupConfig,
    backup_id: &str,
) -> kafka_backup_core::Result<Config> {
    let kafka_config = to_core_kafka_config(&resolved.kafka, &resolved.topics);
    let storage_config = to_core_storage_config(&resolved.storage);
    let backup_options = to_core_backup_options(resolved);

    let config = Config {
        mode: Mode::Backup,
        backup_id: backup_id.to_string(),
        source: Some(kafka_config),
        target: None,
        storage: storage_config,
        backup: Some(backup_options),
        restore: None,
        offset_storage: None,
    };

    config.validate()?;
    Ok(config)
}

/// Convert resolved restore configuration to kafka-backup-core Config
pub fn to_core_restore_config(
    resolved: &ResolvedRestoreConfig,
    backup_id: &str,
    storage: &ResolvedStorage,
) -> kafka_backup_core::Result<Config> {
    let kafka_config = to_core_kafka_config(&resolved.kafka, &resolved.topics);
    let storage_config = to_core_storage_config(storage);
    let restore_options = to_core_restore_options(resolved);

    let config = Config {
        mode: Mode::Restore,
        backup_id: backup_id.to_string(),
        source: None,
        target: Some(kafka_config),
        storage: storage_config,
        backup: None,
        restore: Some(restore_options),
        offset_storage: None,
    };

    config.validate()?;
    Ok(config)
}

/// Convert resolved Kafka configuration to kafka-backup-core KafkaConfig
fn to_core_kafka_config(resolved: &ResolvedKafkaConfig, topics: &[String]) -> KafkaConfig {
    let security = to_core_security_config(resolved);

    KafkaConfig {
        bootstrap_servers: resolved.bootstrap_servers.clone(),
        security,
        topics: TopicSelection {
            include: topics.to_vec(),
            exclude: vec![],
        },
    }
}

/// Convert security configuration
fn to_core_security_config(resolved: &ResolvedKafkaConfig) -> SecurityConfig {
    let security_protocol = match resolved.security_protocol.to_uppercase().as_str() {
        "PLAINTEXT" => SecurityProtocol::Plaintext,
        "SSL" => SecurityProtocol::Ssl,
        "SASL_PLAINTEXT" => SecurityProtocol::SaslPlaintext,
        "SASL_SSL" => SecurityProtocol::SaslSsl,
        _ => SecurityProtocol::Plaintext,
    };

    let (sasl_mechanism, sasl_username, sasl_password) = match &resolved.sasl {
        Some(sasl) => {
            let mechanism = match sasl.mechanism.to_uppercase().as_str() {
                "PLAIN" => Some(SaslMechanism::Plain),
                "SCRAM-SHA-256" => Some(SaslMechanism::ScramSha256),
                "SCRAM-SHA-512" => Some(SaslMechanism::ScramSha512),
                _ => None,
            };
            (mechanism, Some(sasl.username.clone()), Some(sasl.password.clone()))
        }
        None => (None, None, None),
    };

    // For TLS, we'd need to write certs to temp files - for now, use paths if available
    // In production, these would be written to a temp directory and paths provided
    let (ssl_ca_location, ssl_certificate_location, ssl_key_location) = match &resolved.tls {
        Some(_tls) => {
            // TODO: Write TLS credentials to temp files and return paths
            // For now, assume TLS certs are mounted at standard locations
            (None, None, None)
        }
        None => (None, None, None),
    };

    SecurityConfig {
        security_protocol,
        sasl_mechanism,
        sasl_username,
        sasl_password,
        ssl_ca_location,
        ssl_certificate_location,
        ssl_key_location,
    }
}

/// Convert resolved storage configuration to kafka-backup-core StorageConfig
fn to_core_storage_config(resolved: &ResolvedStorage) -> StorageConfig {
    match resolved {
        ResolvedStorage::Local(local) => StorageConfig {
            backend: StorageBackendType::Filesystem,
            path: Some(PathBuf::from(&local.path)),
            endpoint: None,
            bucket: None,
            access_key: None,
            secret_key: None,
            prefix: None,
            region: None,
        },
        ResolvedStorage::S3(s3) => StorageConfig {
            backend: StorageBackendType::S3,
            path: None,
            endpoint: s3.endpoint.clone(),
            bucket: Some(s3.bucket.clone()),
            access_key: Some(s3.access_key_id.clone()),
            secret_key: Some(s3.secret_access_key.clone()),
            prefix: s3.prefix.clone(),
            region: Some(s3.region.clone()),
        },
        // Azure and GCS would need to be mapped to S3-compatible endpoints or
        // the core library would need to be extended to support them directly
        ResolvedStorage::Azure(_azure) => {
            // TODO: Map Azure to S3-compatible or extend core library
            StorageConfig {
                backend: StorageBackendType::S3,
                path: None,
                endpoint: None,
                bucket: None,
                access_key: None,
                secret_key: None,
                prefix: None,
                region: None,
            }
        }
        ResolvedStorage::Gcs(_gcs) => {
            // TODO: Map GCS to S3-compatible or extend core library
            StorageConfig {
                backend: StorageBackendType::S3,
                path: None,
                endpoint: None,
                bucket: None,
                access_key: None,
                secret_key: None,
                prefix: None,
                region: None,
            }
        }
    }
}

/// Convert backup options
fn to_core_backup_options(resolved: &ResolvedBackupConfig) -> BackupOptions {
    let compression = match resolved.compression.algorithm.to_lowercase().as_str() {
        "none" => CompressionType::None,
        "lz4" => CompressionType::Lz4,
        "zstd" => CompressionType::Zstd,
        _ => CompressionType::Zstd,
    };

    let (checkpoint_interval_secs, sync_interval_secs, continuous) = match &resolved.checkpoint {
        Some(cp) if cp.enabled => (cp.interval_secs, cp.interval_secs * 2, true),
        _ => (5, 30, false),
    };

    BackupOptions {
        segment_max_bytes: 128 * 1024 * 1024, // 128MB default
        segment_max_interval_ms: 60_000,       // 60s default
        compression,
        compression_level: resolved.compression.level,
        start_offset: kafka_backup_core::config::StartOffset::Earliest,
        continuous,
        include_internal_topics: false,
        internal_topics: vec![],
        checkpoint_interval_secs,
        sync_interval_secs,
        include_offset_headers: true, // Enable for three-phase restore support
        source_cluster_id: None,
    }
}

/// Convert restore options
fn to_core_restore_options(resolved: &ResolvedRestoreConfig) -> RestoreOptions {
    let consumer_group_strategy = if resolved.rollback.is_some() {
        OffsetStrategy::HeaderBased
    } else {
        OffsetStrategy::Skip
    };

    let (rate_limit_records_per_sec, rate_limit_bytes_per_sec, max_concurrent_partitions) =
        match &resolved.rate_limiting {
            Some(rl) => (
                if rl.records_per_sec > 0 { Some(rl.records_per_sec) } else { None },
                if rl.bytes_per_sec > 0 { Some(rl.bytes_per_sec) } else { None },
                rl.max_concurrent_partitions,
            ),
            None => (None, None, 4),
        };

    let (time_window_start, time_window_end) = match &resolved.pitr {
        Some(pitr) => (pitr.start_timestamp_ms, pitr.end_timestamp_ms),
        None => (None, None),
    };

    RestoreOptions {
        time_window_start,
        time_window_end,
        source_partitions: None,
        partition_mapping: resolved.partition_mapping.clone(),
        topic_mapping: resolved.topic_mapping.clone(),
        consumer_group_strategy,
        dry_run: resolved.dry_run,
        include_original_offset_header: true,
        rate_limit_records_per_sec,
        rate_limit_bytes_per_sec,
        max_concurrent_partitions,
        produce_batch_size: 1000,
        checkpoint_state: None,
        checkpoint_interval_secs: 60,
        consumer_groups: vec![],
        reset_consumer_offsets: false,
        offset_report: None,
    }
}

/// Get storage config for snapshot operations
pub fn get_snapshot_storage_path(rollback_path: Option<&str>) -> PathBuf {
    rollback_path
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/data/snapshots"))
}

/// Build bootstrap servers string from resolved kafka config
pub fn get_bootstrap_servers(kafka: &ResolvedKafkaConfig) -> Vec<String> {
    kafka.bootstrap_servers.clone()
}
