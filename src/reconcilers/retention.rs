//! Backup retention planning and pruning.
//!
//! Retention operates on complete backup sets. It never deletes individual
//! segments from a manifest because that can create PITR gaps that are hard to
//! detect during restore.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use kafka_backup_core::manifest::BackupManifest;
use kafka_backup_core::storage::{
    create_backend_from_config, StorageBackend, StorageBackendConfig,
};
use tracing::{debug, info, warn};

use crate::adapters::{AzureAuthMethod, ResolvedStorage};
use crate::crd::RetentionSpec;
use crate::error::{Error, Result};

const MILLIS_PER_DAY: i64 = 86_400_000;

/// A backup set discovered in storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackupSet {
    /// Backup ID from the manifest.
    pub backup_id: String,
    /// Manifest creation timestamp in epoch milliseconds.
    pub created_at: i64,
    /// Storage keys that belong to this backup set.
    pub keys: Vec<String>,
    /// Total bytes occupied by known keys in this backup set.
    pub bytes: u64,
}

/// Result from a retention pruning run.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RetentionReport {
    /// Number of backup sets considered for this KafkaBackup resource.
    pub inspected_backups: u64,
    /// Number of backup sets selected by policy for pruning.
    pub eligible_backups: u64,
    /// Number of backup sets actually deleted.
    pub deleted_backups: u64,
    /// Number of backup sets retained by policy.
    pub retained_backups: u64,
    /// Bytes reclaimed by deleting backup data.
    pub reclaimed_bytes: u64,
    /// Whether the run only reported what would be deleted.
    pub dry_run: bool,
}

/// Validate a retention spec. Disabled retention is always valid.
pub fn validate_retention(retention: Option<&RetentionSpec>) -> Result<()> {
    let Some(retention) = retention else {
        return Ok(());
    };

    if !retention.enabled {
        return Ok(());
    }

    if retention.max_age_days.is_none() && retention.keep_last.is_none() {
        return Err(Error::validation(
            "retention requires at least one of maxAgeDays or keepLast when enabled",
        ));
    }

    if retention.max_age_days == Some(0) {
        return Err(Error::validation(
            "retention maxAgeDays must be greater than 0",
        ));
    }

    if retention.keep_last == Some(0) {
        return Err(Error::validation(
            "retention keepLast must be greater than 0",
        ));
    }

    Ok(())
}

/// Apply a retention policy to storage. Returns `None` when retention is absent
/// or disabled.
pub async fn apply_retention(
    backup_name: &str,
    retention: Option<&RetentionSpec>,
    storage: &ResolvedStorage,
    current_backup_id: Option<&str>,
) -> Result<Option<RetentionReport>> {
    let Some(retention) = retention else {
        return Ok(None);
    };

    if !retention.enabled {
        return Ok(None);
    }

    validate_retention(Some(retention))?;

    let backend = create_backend(storage)?;
    let local_root = local_storage_root(storage);
    let now_ms = chrono::Utc::now().timestamp_millis();
    let report = apply_retention_with_backend(
        backend.as_ref(),
        backup_name,
        retention,
        current_backup_id,
        now_ms,
        local_root.as_deref(),
    )
    .await?;

    Ok(Some(report))
}

async fn apply_retention_with_backend(
    backend: &dyn StorageBackend,
    backup_name: &str,
    retention: &RetentionSpec,
    current_backup_id: Option<&str>,
    now_ms: i64,
    local_root: Option<&Path>,
) -> Result<RetentionReport> {
    let backup_sets = discover_backup_sets(backend, backup_name).await?;
    let delete_ids = plan_deletions(&backup_sets, retention, current_backup_id, now_ms);
    let eligible_backups = delete_ids.len() as u64;
    let dry_run = retention.dry_run;

    let mut report = RetentionReport {
        inspected_backups: backup_sets.len() as u64,
        eligible_backups,
        retained_backups: backup_sets.len().saturating_sub(delete_ids.len()) as u64,
        dry_run,
        ..Default::default()
    };

    let delete_set: HashSet<&str> = delete_ids.iter().map(String::as_str).collect();
    for backup_set in backup_sets
        .iter()
        .filter(|backup_set| delete_set.contains(backup_set.backup_id.as_str()))
    {
        if dry_run {
            info!(
                backup_id = %backup_set.backup_id,
                bytes = backup_set.bytes,
                "Retention dry run would delete backup set"
            );
            report.reclaimed_bytes += backup_set.bytes;
            continue;
        }

        info!(
            backup_id = %backup_set.backup_id,
            keys = backup_set.keys.len(),
            bytes = backup_set.bytes,
            "Deleting backup set due to retention policy"
        );

        for key in &backup_set.keys {
            backend
                .delete(key)
                .await
                .map_err(|e| Error::storage(format!("Failed to delete '{}': {}", key, e)))?;
        }

        cleanup_local_backup_dir(local_root, &backup_set.backup_id)?;

        report.deleted_backups += 1;
        report.reclaimed_bytes += backup_set.bytes;
    }

    Ok(report)
}

fn local_storage_root(storage: &ResolvedStorage) -> Option<PathBuf> {
    match storage {
        ResolvedStorage::Local(local) => Some(PathBuf::from(&local.path)),
        _ => None,
    }
}

fn cleanup_local_backup_dir(local_root: Option<&Path>, backup_id: &str) -> Result<()> {
    let Some(local_root) = local_root else {
        return Ok(());
    };

    if backup_id.contains('/') || backup_id.contains('\\') {
        warn!(
            backup_id = %backup_id,
            "Skipping local backup directory cleanup for backup ID with path separators"
        );
        return Ok(());
    }

    let backup_dir = local_root.join(backup_id);
    match std::fs::remove_dir_all(&backup_dir) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(Error::storage(format!(
            "Failed to remove local backup directory '{}': {}",
            backup_dir.display(),
            e
        ))),
    }
}

/// Plan which backup IDs should be deleted for a retention policy.
pub fn plan_deletions(
    backup_sets: &[BackupSet],
    retention: &RetentionSpec,
    current_backup_id: Option<&str>,
    now_ms: i64,
) -> Vec<String> {
    if !retention.enabled {
        return Vec::new();
    }

    let mut ordered = backup_sets.to_vec();
    ordered.sort_by(|a, b| {
        b.created_at
            .cmp(&a.created_at)
            .then_with(|| b.backup_id.cmp(&a.backup_id))
    });

    let keep_last = retention.keep_last.unwrap_or(1) as usize;
    let retained_ids: HashSet<&str> = ordered
        .iter()
        .take(keep_last)
        .map(|backup_set| backup_set.backup_id.as_str())
        .chain(current_backup_id)
        .collect();

    let cutoff_ms = retention
        .max_age_days
        .map(|days| now_ms.saturating_sub((days as i64).saturating_mul(MILLIS_PER_DAY)));

    ordered
        .iter()
        .enumerate()
        .filter_map(|(index, backup_set)| {
            if retained_ids.contains(backup_set.backup_id.as_str()) {
                return None;
            }

            let delete_by_count = retention.keep_last.is_some() && index >= keep_last;
            let delete_by_age = cutoff_ms
                .map(|cutoff| backup_set.created_at < cutoff)
                .unwrap_or(false);

            if delete_by_count || delete_by_age {
                Some(backup_set.backup_id.clone())
            } else {
                None
            }
        })
        .collect()
}

async fn discover_backup_sets(
    backend: &dyn StorageBackend,
    backup_name: &str,
) -> Result<Vec<BackupSet>> {
    let keys = backend
        .list("")
        .await
        .map_err(|e| Error::storage(format!("Failed to list backup storage: {}", e)))?;

    let mut backup_sets = Vec::new();

    for manifest_key in keys.iter().filter(|key| key.ends_with("/manifest.json")) {
        let data = match backend.get(manifest_key).await {
            Ok(data) => data,
            Err(e) => {
                warn!(key = %manifest_key, error = %e, "Skipping unreadable backup manifest");
                continue;
            }
        };

        let manifest = match serde_json::from_slice::<BackupManifest>(&data) {
            Ok(manifest) => manifest,
            Err(e) => {
                warn!(key = %manifest_key, error = %e, "Skipping invalid backup manifest");
                continue;
            }
        };

        if !is_owned_backup_id(backup_name, &manifest.backup_id) {
            debug!(
                backup_id = %manifest.backup_id,
                backup_name = %backup_name,
                "Skipping backup set owned by another KafkaBackup resource"
            );
            continue;
        }

        let prefix = format!("{}/", manifest.backup_id);
        let offset_db = format!("{}-offsets.db", manifest.backup_id);
        let set_keys: Vec<String> = keys
            .iter()
            .filter(|key| key.starts_with(&prefix) || *key == &offset_db)
            .cloned()
            .collect();

        let mut bytes = 0;
        for key in &set_keys {
            match backend.size(key).await {
                Ok(size) => bytes += size,
                Err(e) => warn!(
                    key = %key,
                    error = %e,
                    "Could not determine object size during retention scan"
                ),
            }
        }

        backup_sets.push(BackupSet {
            backup_id: manifest.backup_id,
            created_at: manifest.created_at,
            keys: set_keys,
            bytes,
        });
    }

    backup_sets.sort_by_key(|backup_set| backup_set.created_at);
    Ok(backup_sets)
}

fn is_owned_backup_id(backup_name: &str, backup_id: &str) -> bool {
    if backup_id == backup_name {
        return true;
    }

    let Some(suffix) = backup_id.strip_prefix(&format!("{}-", backup_name)) else {
        return false;
    };

    is_generated_backup_suffix(suffix)
}

fn is_generated_backup_suffix(suffix: &str) -> bool {
    let bytes = suffix.as_bytes();

    bytes.len() == "YYYYMMDD-HHMMSS".len()
        && bytes[8] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| index == 8 || byte.is_ascii_digit())
}

fn create_backend(storage: &ResolvedStorage) -> Result<Arc<dyn StorageBackend>> {
    let config = match storage {
        ResolvedStorage::Local(local) => StorageBackendConfig::Filesystem {
            path: local.path.clone().into(),
        },
        ResolvedStorage::S3(s3) => StorageBackendConfig::S3 {
            bucket: s3.bucket.clone(),
            region: Some(s3.region.clone()),
            endpoint: s3.endpoint.clone(),
            access_key: Some(s3.access_key_id.clone()),
            secret_key: Some(s3.secret_access_key.clone()),
            prefix: s3.prefix.clone(),
            path_style: false,
            allow_http: s3
                .endpoint
                .as_ref()
                .map(|endpoint| endpoint.starts_with("http://"))
                .unwrap_or(false),
        },
        ResolvedStorage::Azure(azure) => {
            let (
                account_key,
                use_workload_identity,
                client_id,
                tenant_id,
                client_secret,
                sas_token,
            ) = match &azure.auth {
                AzureAuthMethod::AccountKey(key) => {
                    (Some(key.clone()), None, None, None, None, None)
                }
                AzureAuthMethod::SasToken(token) => {
                    (None, None, None, None, None, Some(token.clone()))
                }
                AzureAuthMethod::ServicePrincipal {
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
                AzureAuthMethod::WorkloadIdentity => (None, Some(true), None, None, None, None),
                AzureAuthMethod::DefaultCredential => (None, None, None, None, None, None),
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
        ResolvedStorage::Gcs(_) => {
            return Err(Error::storage(
                "Operator-managed retention for gcs storage is not supported yet; use a GCS lifecycle policy",
            ));
        }
    };

    create_backend_from_config(&config)
        .map_err(|e| Error::storage(format!("Failed to create retention storage backend: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::{LocalStorageConfig, ResolvedStorage};
    use kafka_backup_core::manifest::TopicBackup;
    use std::fs;
    use tempfile::tempdir;

    fn retention(max_age_days: Option<u32>, keep_last: Option<u32>) -> RetentionSpec {
        RetentionSpec {
            enabled: true,
            max_age_days,
            keep_last,
            dry_run: false,
        }
    }

    fn backup_set(backup_id: &str, created_at: i64) -> BackupSet {
        BackupSet {
            backup_id: backup_id.to_string(),
            created_at,
            keys: Vec::new(),
            bytes: 0,
        }
    }

    fn local_storage(path: &std::path::Path) -> ResolvedStorage {
        ResolvedStorage::Local(LocalStorageConfig {
            path: path.to_string_lossy().to_string(),
        })
    }

    fn write_backup_set(base: &std::path::Path, backup_id: &str, created_at: i64) {
        let backup_dir = base.join(backup_id);
        fs::create_dir_all(backup_dir.join("topics/test-topic/partition=0")).unwrap();

        let mut manifest = BackupManifest::new(backup_id.to_string());
        manifest.created_at = created_at;
        manifest.topics.push(TopicBackup {
            name: "test-topic".to_string(),
            original_partition_count: Some(1),
            partitions: Vec::new(),
        });

        fs::write(
            backup_dir.join("manifest.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        fs::write(
            backup_dir.join("topics/test-topic/partition=0/segment-000001.bin.zst"),
            b"segment-data",
        )
        .unwrap();
        fs::write(base.join(format!("{}-offsets.db", backup_id)), b"offsets").unwrap();
    }

    #[test]
    fn ownership_matcher_rejects_prefix_collisions() {
        assert!(is_owned_backup_id("demo", "demo"));
        assert!(is_owned_backup_id("demo", "demo-20260101-000000"));
        assert!(is_owned_backup_id(
            "demo-archive",
            "demo-archive-20260101-000000"
        ));

        assert!(!is_owned_backup_id("demo", "demo-archive-20260101-000000"));
        assert!(!is_owned_backup_id("demo", "demo-20260101"));
        assert!(!is_owned_backup_id("demo", "demo-20260101-000000-extra"));
        assert!(!is_owned_backup_id("demo", "demo-2026010x-000000"));
    }

    #[test]
    fn validation_allows_missing_or_disabled_retention() {
        assert!(validate_retention(None).is_ok());

        let spec = RetentionSpec {
            enabled: false,
            max_age_days: None,
            keep_last: None,
            dry_run: false,
        };

        assert!(validate_retention(Some(&spec)).is_ok());
    }

    #[test]
    fn validation_requires_policy_when_enabled() {
        let spec = RetentionSpec {
            enabled: true,
            max_age_days: None,
            keep_last: None,
            dry_run: false,
        };

        let err = validate_retention(Some(&spec)).unwrap_err().to_string();
        assert!(err.contains("maxAgeDays") || err.contains("keepLast"));
    }

    #[test]
    fn validation_rejects_zero_values() {
        let age_err = validate_retention(Some(&retention(Some(0), None)))
            .unwrap_err()
            .to_string();
        assert!(age_err.contains("maxAgeDays"));

        let keep_err = validate_retention(Some(&retention(None, Some(0))))
            .unwrap_err()
            .to_string();
        assert!(keep_err.contains("keepLast"));
    }

    #[test]
    fn planner_does_nothing_when_disabled() {
        let mut spec = retention(Some(30), None);
        spec.enabled = false;

        let backups = vec![
            backup_set("backup-20260101-000000", 0),
            backup_set("backup-20260102-000000", MILLIS_PER_DAY),
        ];

        assert!(plan_deletions(&backups, &spec, None, 40 * MILLIS_PER_DAY).is_empty());
    }

    #[test]
    fn planner_deletes_old_backups_by_age_but_keeps_newest() {
        let backups = vec![
            backup_set("backup-20260101-000000", 0),
            backup_set("backup-20260102-000000", MILLIS_PER_DAY),
            backup_set("backup-20260210-000000", 40 * MILLIS_PER_DAY),
        ];

        let planned = plan_deletions(
            &backups,
            &retention(Some(30), None),
            None,
            45 * MILLIS_PER_DAY,
        );

        assert_eq!(
            planned,
            vec![
                "backup-20260102-000000".to_string(),
                "backup-20260101-000000".to_string()
            ]
        );
    }

    #[test]
    fn planner_deletes_by_keep_last_count() {
        let backups = vec![
            backup_set("backup-1", 1),
            backup_set("backup-2", 2),
            backup_set("backup-3", 3),
            backup_set("backup-4", 4),
        ];

        let planned = plan_deletions(&backups, &retention(None, Some(2)), None, 100);

        assert_eq!(
            planned,
            vec!["backup-2".to_string(), "backup-1".to_string()]
        );
    }

    #[test]
    fn planner_never_deletes_current_backup_id() {
        let backups = vec![
            backup_set("backup-1", 1),
            backup_set("backup-2", 2),
            backup_set("backup-3", 3),
        ];

        let planned = plan_deletions(&backups, &retention(None, Some(1)), Some("backup-1"), 100);

        assert_eq!(planned, vec!["backup-2".to_string()]);
    }

    #[test]
    fn planner_applies_keep_last_as_guard_for_age_policy() {
        let backups = vec![
            backup_set("backup-1", 1),
            backup_set("backup-2", 2),
            backup_set("backup-3", 3),
        ];

        let planned = plan_deletions(
            &backups,
            &retention(Some(1), Some(2)),
            None,
            10 * MILLIS_PER_DAY,
        );

        assert_eq!(planned, vec!["backup-1".to_string()]);
    }

    #[tokio::test]
    async fn local_retention_deletes_complete_backup_sets() {
        let dir = tempdir().unwrap();
        write_backup_set(dir.path(), "demo-20260101-000000", 1);
        write_backup_set(dir.path(), "demo-20260102-000000", 2);
        write_backup_set(dir.path(), "demo-20260103-000000", 3);

        let report = apply_retention(
            "demo",
            Some(&retention(None, Some(1))),
            &local_storage(dir.path()),
            Some("demo-20260103-000000"),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(report.inspected_backups, 3);
        assert_eq!(report.eligible_backups, 2);
        assert_eq!(report.deleted_backups, 2);
        assert!(report.reclaimed_bytes > 0);

        assert!(!dir.path().join("demo-20260101-000000").exists());
        assert!(!dir.path().join("demo-20260102-000000").exists());
        assert!(!dir.path().join("demo-20260101-000000-offsets.db").exists());
        assert!(dir
            .path()
            .join("demo-20260103-000000/manifest.json")
            .exists());
    }

    #[tokio::test]
    async fn local_retention_dry_run_reports_without_deleting() {
        let dir = tempdir().unwrap();
        write_backup_set(dir.path(), "demo-20260101-000000", 1);
        write_backup_set(dir.path(), "demo-20260102-000000", 2);

        let mut spec = retention(None, Some(1));
        spec.dry_run = true;

        let report = apply_retention("demo", Some(&spec), &local_storage(dir.path()), None)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(report.eligible_backups, 1);
        assert_eq!(report.deleted_backups, 0);
        assert!(report.dry_run);
        assert!(report.reclaimed_bytes > 0);
        assert!(dir
            .path()
            .join("demo-20260101-000000/manifest.json")
            .exists());
        assert!(dir
            .path()
            .join("demo-20260102-000000/manifest.json")
            .exists());
    }

    #[tokio::test]
    async fn local_retention_only_prunes_matching_backup_name() {
        let dir = tempdir().unwrap();
        write_backup_set(dir.path(), "demo-20260101-000000", 1);
        write_backup_set(dir.path(), "other-20260101-000000", 1);
        write_backup_set(dir.path(), "demo-archive-20260101-000000", 1);
        write_backup_set(dir.path(), "demo-20260102-000000", 2);

        let report = apply_retention(
            "demo",
            Some(&retention(None, Some(1))),
            &local_storage(dir.path()),
            None,
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(report.inspected_backups, 2);
        assert_eq!(report.deleted_backups, 1);
        assert!(!dir.path().join("demo-20260101-000000").exists());
        assert!(dir
            .path()
            .join("other-20260101-000000/manifest.json")
            .exists());
        assert!(dir
            .path()
            .join("demo-archive-20260101-000000/manifest.json")
            .exists());
    }

    #[tokio::test]
    async fn local_retention_skips_invalid_manifests() {
        let dir = tempdir().unwrap();
        write_backup_set(dir.path(), "demo-20260102-000000", 2);

        let invalid_dir = dir.path().join("demo-20260101-000000");
        fs::create_dir_all(&invalid_dir).unwrap();
        fs::write(invalid_dir.join("manifest.json"), b"not-json").unwrap();

        let report = apply_retention(
            "demo",
            Some(&retention(None, Some(1))),
            &local_storage(dir.path()),
            None,
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(report.inspected_backups, 1);
        assert_eq!(report.deleted_backups, 0);
        assert!(invalid_dir.join("manifest.json").exists());
    }
}
