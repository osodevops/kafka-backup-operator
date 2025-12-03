//! Storage configuration adapter
//!
//! Converts CRD StorageSpec to kafka-backup-core storage configuration.

use kube::Client;

use crate::crd::{
    AzureStorageSpec, GcsStorageSpec, PvcStorageSpec, S3StorageSpec, StorageSpec,
};
use crate::error::{Error, Result};

use super::secrets::{get_azure_credentials, get_gcs_credentials, get_s3_credentials};

/// Resolved storage configuration ready for use with kafka-backup-core
#[derive(Debug, Clone)]
pub enum ResolvedStorage {
    /// Local/PVC storage
    Local(LocalStorageConfig),
    /// S3 storage
    S3(S3StorageConfig),
    /// Azure Blob storage
    Azure(AzureStorageConfig),
    /// GCS storage
    Gcs(GcsStorageConfig),
}

/// Local/PVC storage configuration
#[derive(Debug, Clone)]
pub struct LocalStorageConfig {
    pub path: String,
}

/// S3 storage configuration with resolved credentials
#[derive(Debug, Clone)]
pub struct S3StorageConfig {
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub prefix: Option<String>,
    pub access_key_id: String,
    pub secret_access_key: String,
}

/// Azure Blob storage configuration with resolved credentials
#[derive(Debug, Clone)]
pub struct AzureStorageConfig {
    pub container: String,
    pub account_name: String,
    pub account_key: String,
    pub prefix: Option<String>,
}

/// GCS storage configuration with resolved credentials
#[derive(Debug, Clone)]
pub struct GcsStorageConfig {
    pub bucket: String,
    pub prefix: Option<String>,
    pub service_account_json: String,
}

/// Build resolved storage configuration from CRD spec
pub async fn build_storage_config(
    storage: &StorageSpec,
    client: &Client,
    namespace: &str,
) -> Result<ResolvedStorage> {
    match storage.storage_type.as_str() {
        "pvc" => build_pvc_storage(storage.pvc.as_ref()).await,
        "s3" => build_s3_storage(storage.s3.as_ref(), client, namespace).await,
        "azure" => build_azure_storage(storage.azure.as_ref(), client, namespace).await,
        "gcs" => build_gcs_storage(storage.gcs.as_ref(), client, namespace).await,
        other => Err(Error::config(format!("Unsupported storage type: {}", other))),
    }
}

/// Build PVC/local storage configuration
async fn build_pvc_storage(pvc: Option<&PvcStorageSpec>) -> Result<ResolvedStorage> {
    let pvc = pvc.ok_or_else(|| Error::config("PVC configuration is required for pvc storage type"))?;

    // Build the path from claim name and optional sub-path
    let base_path = format!("/data/{}", pvc.claim_name);
    let path = match &pvc.sub_path {
        Some(sub) => format!("{}/{}", base_path, sub),
        None => base_path,
    };

    Ok(ResolvedStorage::Local(LocalStorageConfig { path }))
}

/// Build S3 storage configuration with resolved credentials
async fn build_s3_storage(
    s3: Option<&S3StorageSpec>,
    client: &Client,
    namespace: &str,
) -> Result<ResolvedStorage> {
    let s3 = s3.ok_or_else(|| Error::config("S3 configuration is required for s3 storage type"))?;

    // Fetch credentials from Kubernetes secret
    let (access_key_id, secret_access_key) = get_s3_credentials(
        client,
        namespace,
        &s3.credentials_secret.name,
        &s3.credentials_secret.access_key_id_key,
        &s3.credentials_secret.secret_access_key_key,
    )
    .await?;

    Ok(ResolvedStorage::S3(S3StorageConfig {
        bucket: s3.bucket.clone(),
        region: s3.region.clone(),
        endpoint: s3.endpoint.clone(),
        prefix: s3.prefix.clone(),
        access_key_id,
        secret_access_key,
    }))
}

/// Build Azure Blob storage configuration with resolved credentials
async fn build_azure_storage(
    azure: Option<&AzureStorageSpec>,
    client: &Client,
    namespace: &str,
) -> Result<ResolvedStorage> {
    let azure = azure.ok_or_else(|| Error::config("Azure configuration is required for azure storage type"))?;

    // Fetch credentials from Kubernetes secret
    let account_key = get_azure_credentials(
        client,
        namespace,
        &azure.credentials_secret.name,
        &azure.credentials_secret.account_key_key,
    )
    .await?;

    Ok(ResolvedStorage::Azure(AzureStorageConfig {
        container: azure.container.clone(),
        account_name: azure.account_name.clone(),
        account_key,
        prefix: azure.prefix.clone(),
    }))
}

/// Build GCS storage configuration with resolved credentials
async fn build_gcs_storage(
    gcs: Option<&GcsStorageSpec>,
    client: &Client,
    namespace: &str,
) -> Result<ResolvedStorage> {
    let gcs = gcs.ok_or_else(|| Error::config("GCS configuration is required for gcs storage type"))?;

    // Fetch credentials from Kubernetes secret
    let service_account_json = get_gcs_credentials(
        client,
        namespace,
        &gcs.credentials_secret.name,
        &gcs.credentials_secret.service_account_json_key,
    )
    .await?;

    Ok(ResolvedStorage::Gcs(GcsStorageConfig {
        bucket: gcs.bucket.clone(),
        prefix: gcs.prefix.clone(),
        service_account_json,
    }))
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn test_pvc_path_construction() {
        // This would require mocking - placeholder for future tests
    }
}
