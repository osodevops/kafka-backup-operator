//! Core library integration adapter
//!
//! Converts operator's resolved configuration types to kafka-backup-core types.

use std::path::PathBuf;

use kafka_backup_core::config::{
    BackupOptions, CompressionType, Config, KafkaConfig, Mode, OffsetStorageBackend,
    OffsetStorageConfig, OffsetStrategy, RestoreOptions, SaslMechanism, SecurityConfig,
    SecurityProtocol, TopicSelection,
};
use kafka_backup_core::storage::StorageBackendConfig;

use super::backup_config::{ResolvedBackupConfig, ResolvedKafkaConfig};
use super::restore_config::ResolvedRestoreConfig;
use super::storage_config::ResolvedStorage;
use super::tls_files::TlsFileManager;

/// Convert resolved backup configuration to kafka-backup-core Config
pub fn to_core_backup_config(
    resolved: &ResolvedBackupConfig,
    backup_id: &str,
) -> kafka_backup_core::Result<Config> {
    let kafka_config = to_core_kafka_config(&resolved.kafka, &resolved.topics);
    let storage_config = to_core_storage_config(&resolved.storage);
    let backup_options = to_core_backup_options(resolved);

    // Build offset storage config with proper path inside the backup storage directory
    let offset_storage = build_offset_storage_config(&resolved.storage, backup_id);

    let config = Config {
        mode: Mode::Backup,
        backup_id: backup_id.to_string(),
        source: Some(kafka_config),
        target: None,
        storage: storage_config,
        backup: Some(backup_options),
        restore: None,
        offset_storage,
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
    to_core_security_config_with_tls(resolved, None)
}

/// Convert security configuration with optional pre-created TLS files
pub fn to_core_security_config_with_tls(
    resolved: &ResolvedKafkaConfig,
    tls_manager: Option<&TlsFileManager>,
) -> SecurityConfig {
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
            (
                mechanism,
                Some(sasl.username.clone()),
                Some(sasl.password.clone()),
            )
        }
        None => (None, None, None),
    };

    // Use TLS file manager if provided, otherwise no TLS
    let (ssl_ca_location, ssl_certificate_location, ssl_key_location) = match tls_manager {
        Some(mgr) => (
            Some(mgr.ca_location()),
            mgr.certificate_location(),
            mgr.key_location(),
        ),
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

/// Convert resolved storage configuration to kafka-backup-core StorageBackendConfig
fn to_core_storage_config(resolved: &ResolvedStorage) -> StorageBackendConfig {
    match resolved {
        ResolvedStorage::Local(local) => StorageBackendConfig::Filesystem {
            path: PathBuf::from(&local.path),
        },
        ResolvedStorage::S3(s3) => StorageBackendConfig::S3 {
            bucket: s3.bucket.clone(),
            region: Some(s3.region.clone()),
            endpoint: s3.endpoint.clone(),
            access_key: Some(s3.access_key_id.clone()),
            secret_key: Some(s3.secret_access_key.clone()),
            prefix: s3.prefix.clone(),
            path_style: false,
            allow_http: false,
        },
        ResolvedStorage::Azure(azure) => {
            // Determine authentication method based on resolved auth
            let (
                account_key,
                use_workload_identity,
                client_id,
                tenant_id,
                client_secret,
                sas_token,
            ) = match &azure.auth {
                super::storage_config::AzureAuthMethod::AccountKey(key) => {
                    (Some(key.clone()), None, None, None, None, None)
                }
                super::storage_config::AzureAuthMethod::SasToken(token) => {
                    (None, None, None, None, None, Some(token.clone()))
                }
                super::storage_config::AzureAuthMethod::ServicePrincipal {
                    client_id,
                    tenant_id,
                    client_secret,
                } => (
                    None,
                    None,
                    Some(client_id.clone()),
                    Some(tenant_id.clone()),
                    Some(client_secret.clone()),
                    None,
                ),
                super::storage_config::AzureAuthMethod::WorkloadIdentity => {
                    (None, Some(true), None, None, None, None)
                }
                super::storage_config::AzureAuthMethod::DefaultCredential => {
                    // DefaultCredential uses Azure SDK's default credential chain
                    // No explicit auth fields needed - the SDK will auto-detect
                    (None, None, None, None, None, None)
                }
            };

            StorageBackendConfig::Azure {
                account_name: azure.account_name.clone(),
                container_name: azure.container.clone(),
                account_key,
                prefix: azure.prefix.clone(),
                endpoint: azure.endpoint.clone(),
                use_workload_identity,
                client_id,
                tenant_id,
                client_secret,
                sas_token,
            }
        }
        ResolvedStorage::Gcs(gcs) => StorageBackendConfig::Gcs {
            bucket: gcs.bucket.clone(),
            service_account_path: Some(gcs.service_account_json.clone()),
            prefix: gcs.prefix.clone(),
        },
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
        segment_max_interval_ms: 60_000,      // 60s default
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
                if rl.records_per_sec > 0 {
                    Some(rl.records_per_sec)
                } else {
                    None
                },
                if rl.bytes_per_sec > 0 {
                    Some(rl.bytes_per_sec)
                } else {
                    None
                },
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

/// Build offset storage configuration for tracking backup progress
/// This ensures the SQLite database is created in a writable location within the backup storage
fn build_offset_storage_config(
    storage: &ResolvedStorage,
    backup_id: &str,
) -> Option<OffsetStorageConfig> {
    match storage {
        ResolvedStorage::Local(local) => {
            // Create offset database path inside the backup storage directory
            let db_path = PathBuf::from(&local.path).join(format!("{}-offsets.db", backup_id));
            Some(OffsetStorageConfig {
                backend: OffsetStorageBackend::Sqlite,
                db_path,
                s3_key: None,
                sync_interval_secs: 60,
            })
        }
        ResolvedStorage::S3(s3) => {
            // For S3, use a local temp path but sync to S3
            let db_path = PathBuf::from("/tmp").join(format!("{}-offsets.db", backup_id));
            let s3_key = s3
                .prefix
                .as_ref()
                .map(|p| format!("{}/{}/offsets.db", p, backup_id))
                .or_else(|| Some(format!("{}/offsets.db", backup_id)));
            Some(OffsetStorageConfig {
                backend: OffsetStorageBackend::Sqlite,
                db_path,
                s3_key,
                sync_interval_secs: 60,
            })
        }
        // Azure and GCS would need similar handling
        ResolvedStorage::Azure(_) | ResolvedStorage::Gcs(_) => {
            // Use local temp path for now
            let db_path = PathBuf::from("/tmp").join(format!("{}-offsets.db", backup_id));
            Some(OffsetStorageConfig {
                backend: OffsetStorageBackend::Sqlite,
                db_path,
                s3_key: None,
                sync_interval_secs: 60,
            })
        }
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
