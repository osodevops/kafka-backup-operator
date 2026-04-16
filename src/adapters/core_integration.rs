//! Core library integration adapter
//!
//! Converts operator's resolved configuration types to kafka-backup-core types.

use std::path::PathBuf;

use kafka_backup_core::config::{
    BackupOptions, CompressionType, Config, ConnectionConfig, KafkaConfig, MetricsConfig, Mode,
    OffsetStorageBackend, OffsetStorageConfig, OffsetStrategy, RepartitioningStrategy,
    RestoreOptions, SaslMechanism, SecurityConfig, SecurityProtocol, TopicRepartitioning,
    TopicSelection,
};
use kafka_backup_core::storage::StorageBackendConfig;
use kafka_backup_core::validation::{
    ChecksConfig as CoreChecksConfig, ConsumerGroupConfig as CoreConsumerGroupConfig,
    EvidenceConfig as CoreEvidenceConfig, EvidenceFormat, EvidenceStorageConfig,
    MessageCountConfig as CoreMessageCountConfig, NotificationsConfig as CoreNotificationsConfig,
    OffsetRangeConfig as CoreOffsetRangeConfig, PagerDutyConfig as CorePagerDutyConfig,
    SigningConfig as CoreSigningConfig, SlackConfig as CoreSlackConfig,
    ValidationConfig as CoreValidationConfig, WebhookConfig as CoreWebhookConfig,
};

use super::backup_config::{ResolvedBackupConfig, ResolvedKafkaConfig, ResolvedMetricsConfig};
use super::restore_config::ResolvedRestoreConfig;
use super::storage_config::ResolvedStorage;
use super::tls_files::TlsFileManager;
use super::validation_config::{ResolvedEvidenceConfig, ResolvedValidationConfig};

/// Convert resolved backup configuration to kafka-backup-core Config
pub fn to_core_backup_config(
    resolved: &ResolvedBackupConfig,
    backup_id: &str,
    tls_manager: Option<&TlsFileManager>,
) -> kafka_backup_core::Result<Config> {
    let kafka_config = to_core_kafka_config_with_tls(&resolved.kafka, &resolved.topics, tls_manager);
    let storage_config = to_core_storage_config(&resolved.storage);
    let backup_options = to_core_backup_options(resolved);

    // Build offset storage config with proper path inside the backup storage directory
    let offset_storage = build_offset_storage_config(&resolved.storage, backup_id);

    // Build metrics config
    let metrics = resolved.metrics.as_ref().map(to_core_metrics_config);

    let config = Config {
        mode: Mode::Backup,
        backup_id: backup_id.to_string(),
        source: Some(kafka_config),
        target: None,
        storage: storage_config,
        backup: Some(backup_options),
        restore: None,
        offset_storage,
        metrics,
    };

    config.validate()?;
    Ok(config)
}

/// Convert resolved restore configuration to kafka-backup-core Config
pub fn to_core_restore_config(
    resolved: &ResolvedRestoreConfig,
    backup_id: &str,
    storage: &ResolvedStorage,
    tls_manager: Option<&TlsFileManager>,
) -> kafka_backup_core::Result<Config> {
    let kafka_config = to_core_kafka_config_with_tls(&resolved.kafka, &resolved.topics, tls_manager);
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
        metrics: None,
    };

    config.validate()?;
    Ok(config)
}

/// Convert resolved Kafka configuration to kafka-backup-core KafkaConfig
fn to_core_kafka_config(resolved: &ResolvedKafkaConfig, topics: &[String]) -> KafkaConfig {
    to_core_kafka_config_with_tls(resolved, topics, None)
}

/// Convert resolved Kafka configuration to kafka-backup-core KafkaConfig with TLS
fn to_core_kafka_config_with_tls(resolved: &ResolvedKafkaConfig, topics: &[String], tls_manager: Option<&TlsFileManager>) -> KafkaConfig {
    let security = to_core_security_config_with_tls(resolved, tls_manager);

    KafkaConfig {
        bootstrap_servers: resolved.bootstrap_servers.clone(),
        security,
        topics: TopicSelection {
            include: topics.to_vec(),
            exclude: vec![],
        },
        connection: to_core_connection_config(resolved),
    }
}

pub fn to_core_connection_config(resolved: &ResolvedKafkaConfig) -> ConnectionConfig {
    ConnectionConfig {
        tcp_keepalive: resolved.connection.tcp_keepalive,
        keepalive_time_secs: resolved.connection.keepalive_time_secs,
        keepalive_interval_secs: resolved.connection.keepalive_interval_secs,
        tcp_nodelay: resolved.connection.tcp_nodelay,
        connections_per_broker: resolved.connection.connections_per_broker,
    }
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
pub fn to_core_storage_config_for_validation(resolved: &ResolvedStorage) -> StorageBackendConfig {
    to_core_storage_config(resolved)
}

/// Convert resolved Kafka configuration to kafka-backup-core KafkaConfig (for validation)
pub fn to_core_kafka_config_for_validation(resolved: &ResolvedKafkaConfig) -> KafkaConfig {
    to_core_kafka_config(resolved, &[])
}

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
            path_style: s3.path_style,
            allow_http: s3.allow_http,
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
                    // Workload Identity: Do NOT pass client_id/tenant_id to core library.
                    // object_store reads AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_FEDERATED_TOKEN_FILE
                    // from environment automatically. Passing client_id triggers MSI auth instead.
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

/// Convert resolved metrics configuration to kafka-backup-core MetricsConfig
fn to_core_metrics_config(resolved: &ResolvedMetricsConfig) -> MetricsConfig {
    let mut config = MetricsConfig::default();
    config.enabled = resolved.enabled;
    config.port = resolved.port;
    config.bind_address = resolved.bind_address.clone();
    config.path = resolved.path.clone();
    config.update_interval_ms = resolved.update_interval_ms;
    config.max_partition_labels = resolved.max_partition_labels;
    config
}

/// Convert backup options
fn to_core_backup_options(resolved: &ResolvedBackupConfig) -> BackupOptions {
    let compression = match resolved.compression.algorithm.to_lowercase().as_str() {
        "none" => CompressionType::None,
        "lz4" => CompressionType::Lz4,
        "zstd" => CompressionType::Zstd,
        _ => CompressionType::Zstd,
    };

    let (checkpoint_interval_secs, sync_interval_secs) = match &resolved.checkpoint {
        Some(cp) if cp.enabled => (cp.interval_secs, cp.interval_secs * 2),
        _ => (5, 30),
    };

    let max_concurrent_partitions = resolved
        .rate_limiting
        .as_ref()
        .map(|rl| rl.max_concurrent_partitions)
        .unwrap_or(8);

    BackupOptions {
        segment_max_bytes: resolved.backup_options.segment_max_bytes,
        segment_max_interval_ms: resolved.backup_options.segment_max_interval_ms,
        compression,
        compression_level: resolved.compression.level,
        start_offset: kafka_backup_core::config::StartOffset::Earliest,
        continuous: resolved.backup_options.continuous,
        include_internal_topics: false,
        internal_topics: vec![],
        checkpoint_interval_secs,
        sync_interval_secs,
        include_offset_headers: resolved.backup_options.include_offset_headers,
        source_cluster_id: resolved.backup_options.source_cluster_id.clone(),
        stop_at_current_offsets: resolved.backup_options.stop_at_current_offsets,
        max_concurrent_partitions,
        poll_interval_ms: resolved.backup_options.poll_interval_ms,
        consumer_group_snapshot: resolved.backup_options.consumer_group_snapshot,
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

    let repartitioning = resolved
        .repartitioning
        .iter()
        .map(|(topic, repartitioning)| {
            let strategy = match repartitioning.strategy.to_lowercase().as_str() {
                "automatic" => RepartitioningStrategy::Automatic,
                _ => RepartitioningStrategy::Murmur2,
            };
            (
                topic.clone(),
                TopicRepartitioning {
                    strategy,
                    target_partitions: repartitioning.target_partitions,
                },
            )
        })
        .collect();

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
        produce_batch_size: resolved.produce_batch_size,
        produce_acks: resolved.produce_acks,
        produce_timeout_ms: resolved.produce_timeout_ms,
        checkpoint_state: None,
        checkpoint_interval_secs: 60,
        consumer_groups: vec![],
        reset_consumer_offsets: false,
        offset_report: None,
        create_topics: resolved.create_topics,
        default_replication_factor: resolved.default_replication_factor,
        repartitioning,
        purge_topics: resolved.purge_topics,
        auto_consumer_groups: resolved.auto_consumer_groups,
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

/// Convert resolved validation configuration to kafka-backup-core ValidationConfig
pub fn to_core_validation_config(
    resolved: &ResolvedValidationConfig,
    backup_id: &str,
    storage: &ResolvedStorage,
) -> kafka_backup_core::Result<CoreValidationConfig> {
    let storage_config = to_core_storage_config(storage);

    let kafka_config = match &resolved.kafka {
        Some(kafka) => to_core_kafka_config(kafka, &[]),
        None => KafkaConfig {
            bootstrap_servers: vec![],
            security: SecurityConfig::default(),
            topics: TopicSelection::default(),
            connection: ConnectionConfig::default(),
        },
    };

    let checks = to_core_checks_config(&resolved.checks);

    let evidence = match &resolved.evidence {
        Some(ev) => to_core_evidence_config(ev, storage),
        None => CoreEvidenceConfig::default(),
    };

    let notifications = resolved
        .notifications
        .as_ref()
        .map(to_core_notifications_config);

    Ok(CoreValidationConfig {
        backup_id: backup_id.to_string(),
        storage: storage_config,
        target: kafka_config,
        checks,
        evidence,
        notifications,
        pitr_timestamp: None,
        triggered_by: Some("kafka-backup-operator".to_string()),
    })
}

/// Convert CRD checks spec to core ChecksConfig
fn to_core_checks_config(checks: &crate::crd::ValidationChecksSpec) -> CoreChecksConfig {
    let message_count = match &checks.message_count {
        Some(mc) => CoreMessageCountConfig {
            enabled: mc.enabled,
            mode: Default::default(),
            sample_percentage: 100,
            topics: mc.topics.clone(),
            fail_threshold: mc.fail_threshold.unwrap_or(0),
        },
        None => CoreMessageCountConfig {
            enabled: false,
            ..Default::default()
        },
    };

    let offset_range = match &checks.offset_range {
        Some(or) => CoreOffsetRangeConfig {
            enabled: or.enabled,
            verify_high_watermark: true,
            verify_low_watermark: true,
        },
        None => CoreOffsetRangeConfig {
            enabled: false,
            ..Default::default()
        },
    };

    let consumer_group_offsets = match &checks.consumer_group_offsets {
        Some(cg) => CoreConsumerGroupConfig {
            enabled: cg.enabled,
            verify_all_groups: cg.consumer_groups.is_empty(),
            groups: cg.consumer_groups.clone(),
        },
        None => CoreConsumerGroupConfig {
            enabled: false,
            ..Default::default()
        },
    };

    let custom_webhooks: Vec<CoreWebhookConfig> = checks
        .custom_webhooks
        .iter()
        .map(|wh| CoreWebhookConfig {
            name: wh.name.clone(),
            url: wh.url.clone(),
            timeout_seconds: wh.timeout_secs,
            expected_status_code: wh.expected_status_code,
            fail_on_timeout: true,
        })
        .collect();

    CoreChecksConfig {
        message_count,
        offset_range,
        consumer_group_offsets,
        custom_webhooks,
    }
}

/// Convert resolved evidence config to core EvidenceConfig
fn to_core_evidence_config(
    resolved: &ResolvedEvidenceConfig,
    _backup_storage: &ResolvedStorage,
) -> CoreEvidenceConfig {
    let formats = resolved
        .formats
        .iter()
        .filter_map(|f| match f.to_lowercase().as_str() {
            "json" => Some(EvidenceFormat::Json),
            "pdf" => Some(EvidenceFormat::Pdf),
            _ => None,
        })
        .collect();

    let signing = CoreSigningConfig {
        enabled: resolved.signing_private_key_pem.is_some(),
        private_key_path: resolved.signing_private_key_pem.clone(),
        public_key_path: resolved.signing_public_key_pem.clone(),
    };

    CoreEvidenceConfig {
        formats,
        signing,
        storage: EvidenceStorageConfig {
            prefix: "evidence-reports/".to_string(),
            retention_days: resolved.retention_days,
        },
    }
}

/// Convert resolved notifications config to core NotificationsConfig
fn to_core_notifications_config(
    resolved: &super::validation_config::ResolvedNotificationsConfig,
) -> CoreNotificationsConfig {
    CoreNotificationsConfig {
        slack: resolved
            .slack_webhook_url
            .as_ref()
            .map(|url| CoreSlackConfig {
                webhook_url: url.clone(),
            }),
        pagerduty: resolved
            .pagerduty_routing_key
            .as_ref()
            .map(|key| CorePagerDutyConfig {
                integration_key: key.clone(),
                severity: resolved
                    .pagerduty_severity
                    .clone()
                    .unwrap_or_else(|| "critical".to_string()),
            }),
    }
}
