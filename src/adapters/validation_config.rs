//! Validation configuration adapter
//!
//! Converts KafkaBackupValidation CRD spec to kafka-backup-core validation configuration.

use kube::Client;

use crate::crd::{BackupValidationRef, KafkaBackupValidation, NotificationsSpec, SigningSpec};
use crate::error::Result;

use super::backup_config::{build_kafka_config, ResolvedKafkaConfig};
use super::restore_config::ResolvedBackupSource;
use super::secrets::{get_secret, get_secret_string};
use super::storage_config::{build_storage_config, ResolvedStorage};

/// Fully resolved validation configuration
#[derive(Debug, Clone)]
pub struct ResolvedValidationConfig {
    /// Source backup location
    pub backup_source: ResolvedBackupSource,
    /// Target Kafka cluster (for consumer group checks)
    pub kafka: Option<ResolvedKafkaConfig>,
    /// Resolved checks configuration (passed through from CRD)
    pub checks: crate::crd::ValidationChecksSpec,
    /// Resolved evidence configuration
    pub evidence: Option<ResolvedEvidenceConfig>,
    /// Resolved notifications configuration
    pub notifications: Option<ResolvedNotificationsConfig>,
}

/// Resolved evidence configuration with secrets fetched
#[derive(Debug, Clone)]
pub struct ResolvedEvidenceConfig {
    /// Output formats
    pub formats: Vec<String>,
    /// Resolved signing key (PEM content)
    pub signing_private_key_pem: Option<String>,
    /// Resolved signing public key (PEM content)
    pub signing_public_key_pem: Option<String>,
    /// Evidence storage location
    pub storage: Option<ResolvedStorage>,
    /// Retention days
    pub retention_days: u32,
}

/// Resolved notifications configuration with secrets fetched
#[derive(Debug, Clone)]
pub struct ResolvedNotificationsConfig {
    /// Slack webhook URL (resolved from secret)
    pub slack_webhook_url: Option<String>,
    /// PagerDuty routing key (resolved from secret)
    pub pagerduty_routing_key: Option<String>,
    /// PagerDuty severity
    pub pagerduty_severity: Option<String>,
}

/// Build fully resolved validation configuration from CRD
pub async fn build_validation_config(
    validation: &KafkaBackupValidation,
    client: &Client,
    namespace: &str,
) -> Result<ResolvedValidationConfig> {
    let backup_source =
        resolve_validation_backup_ref(&validation.spec.backup_ref, client, namespace).await?;

    let kafka = match &validation.spec.kafka_cluster {
        Some(kafka_spec) => Some(build_kafka_config(kafka_spec, client, namespace).await?),
        None => None,
    };

    let evidence = match &validation.spec.evidence {
        Some(ev) => Some(resolve_evidence_config(ev, client, namespace).await?),
        None => None,
    };

    let notifications = match &validation.spec.notifications {
        Some(notif) => Some(resolve_notifications_config(notif, client, namespace).await?),
        None => None,
    };

    Ok(ResolvedValidationConfig {
        backup_source,
        kafka,
        checks: validation.spec.checks.clone(),
        evidence,
        notifications,
    })
}

/// Resolve backup reference for validation (same logic as restore)
async fn resolve_validation_backup_ref(
    backup_ref: &BackupValidationRef,
    client: &Client,
    namespace: &str,
) -> Result<ResolvedBackupSource> {
    if let Some(storage) = &backup_ref.storage {
        let resolved = build_storage_config(storage, client, namespace).await?;
        return Ok(ResolvedBackupSource::Storage {
            storage: resolved,
            backup_id: backup_ref.backup_id.clone(),
        });
    }

    let backup_namespace = backup_ref
        .namespace
        .clone()
        .unwrap_or_else(|| namespace.to_string());

    Ok(ResolvedBackupSource::BackupResource {
        name: backup_ref.name.clone(),
        namespace: backup_namespace,
        backup_id: backup_ref.backup_id.clone(),
    })
}

/// Resolve evidence configuration, fetching signing keys from secrets
async fn resolve_evidence_config(
    evidence: &crate::crd::EvidenceSpec,
    client: &Client,
    namespace: &str,
) -> Result<ResolvedEvidenceConfig> {
    let (signing_private_key_pem, signing_public_key_pem) = match &evidence.signing {
        Some(signing) if signing.enabled => {
            resolve_signing_keys(signing, client, namespace).await?
        }
        _ => (None, None),
    };

    let storage = match &evidence.storage {
        Some(s) => Some(build_storage_config(s, client, namespace).await?),
        None => None,
    };

    Ok(ResolvedEvidenceConfig {
        formats: evidence.formats.clone(),
        signing_private_key_pem,
        signing_public_key_pem,
        storage,
        retention_days: evidence.retention_days,
    })
}

/// Resolve signing keys from Kubernetes secret
async fn resolve_signing_keys(
    signing: &SigningSpec,
    client: &Client,
    namespace: &str,
) -> Result<(Option<String>, Option<String>)> {
    let secret = get_secret(client, &signing.key_secret.name, namespace).await?;
    let private_key = get_secret_string(&secret, &signing.key_secret.private_key_key)?;
    let public_key = get_secret_string(&secret, &signing.key_secret.public_key_key).ok();
    Ok((Some(private_key), public_key))
}

/// Resolve notifications configuration, fetching webhook URLs from secrets
async fn resolve_notifications_config(
    notifications: &NotificationsSpec,
    client: &Client,
    namespace: &str,
) -> Result<ResolvedNotificationsConfig> {
    let slack_webhook_url = match &notifications.slack {
        Some(slack) => {
            let secret = get_secret(client, &slack.webhook_secret.name, namespace).await?;
            Some(get_secret_string(&secret, &slack.webhook_secret.key)?)
        }
        None => None,
    };

    let (pagerduty_routing_key, pagerduty_severity) = match &notifications.pagerduty {
        Some(pd) => {
            let secret = get_secret(client, &pd.routing_key_secret.name, namespace).await?;
            let key = get_secret_string(&secret, &pd.routing_key_secret.key)?;
            (Some(key), Some(pd.severity.clone()))
        }
        None => (None, None),
    };

    Ok(ResolvedNotificationsConfig {
        slack_webhook_url,
        pagerduty_routing_key,
        pagerduty_severity,
    })
}
