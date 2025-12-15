//! Storage configuration adapter
//!
//! Converts CRD StorageSpec to kafka-backup-core storage configuration.

use kube::Client;

use crate::crd::{
    AzureStorageSpec, GcsStorageSpec, PvcStorageSpec, S3StorageSpec, StorageSpec,
};
use crate::error::{Error, Result};

use super::secrets::{
    get_azure_credentials, get_azure_sas_token, get_azure_service_principal_credentials,
    get_gcs_credentials, get_s3_credentials,
};

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

/// Azure authentication method
#[derive(Debug, Clone)]
pub enum AzureAuthMethod {
    /// Account key authentication (from Kubernetes secret)
    AccountKey(String),
    /// SAS token authentication (time-limited access)
    SasToken(String),
    /// Service Principal authentication (for CI/CD pipelines)
    ServicePrincipal {
        client_id: String,
        tenant_id: String,
        client_secret: String,
    },
    /// Workload Identity authentication (uses pod's federated identity token)
    /// Auto-detected via AZURE_FEDERATED_TOKEN_FILE environment variable
    WorkloadIdentity,
    /// DefaultAzureCredential - uses Azure SDK's credential chain
    /// Falls back through: environment variables, managed identity, CLI, etc.
    DefaultCredential,
}

/// Azure Blob storage configuration with resolved credentials
#[derive(Debug, Clone)]
pub struct AzureStorageConfig {
    pub container: String,
    pub account_name: String,
    pub auth: AzureAuthMethod,
    pub prefix: Option<String>,
    /// Custom endpoint URL (for Azure Government, China, or private endpoints)
    pub endpoint: Option<String>,
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
///
/// Authentication method priority (first match wins):
/// 1. Explicit `use_workload_identity: true` flag
/// 2. Service Principal credentials (if secret provided)
/// 3. SAS token (if secret provided)
/// 4. Account key (if secret provided)
/// 5. Auto-detect Workload Identity via AZURE_FEDERATED_TOKEN_FILE env var
/// 6. DefaultAzureCredential fallback (Azure SDK credential chain)
async fn build_azure_storage(
    azure: Option<&AzureStorageSpec>,
    client: &Client,
    namespace: &str,
) -> Result<ResolvedStorage> {
    let azure = azure.ok_or_else(|| Error::config("Azure configuration is required for azure storage type"))?;

    // Determine authentication method based on priority
    let auth = if azure.use_workload_identity {
        // 1. Explicit Workload Identity flag
        tracing::info!(
            account_name = %azure.account_name,
            container = %azure.container,
            "Using Azure Workload Identity for authentication (explicit flag)"
        );
        AzureAuthMethod::WorkloadIdentity
    } else if let Some(sp_secret) = &azure.service_principal_secret {
        // 2. Service Principal credentials
        let sp_creds = get_azure_service_principal_credentials(
            client,
            namespace,
            &sp_secret.name,
            &sp_secret.client_id_key,
            &sp_secret.tenant_id_key,
            &sp_secret.client_secret_key,
        )
        .await?;

        tracing::info!(
            account_name = %azure.account_name,
            container = %azure.container,
            client_id = %sp_creds.client_id,
            "Using Azure Service Principal for authentication"
        );

        AzureAuthMethod::ServicePrincipal {
            client_id: sp_creds.client_id,
            tenant_id: sp_creds.tenant_id,
            client_secret: sp_creds.client_secret,
        }
    } else if let Some(sas_secret) = &azure.sas_token_secret {
        // 3. SAS token
        let sas_token = get_azure_sas_token(
            client,
            namespace,
            &sas_secret.name,
            &sas_secret.sas_token_key,
        )
        .await?;

        tracing::info!(
            account_name = %azure.account_name,
            container = %azure.container,
            "Using Azure SAS token for authentication"
        );

        AzureAuthMethod::SasToken(sas_token)
    } else if let Some(creds) = &azure.credentials_secret {
        // 4. Account key
        let account_key = get_azure_credentials(
            client,
            namespace,
            &creds.name,
            &creds.account_key_key,
        )
        .await?;

        tracing::info!(
            account_name = %azure.account_name,
            container = %azure.container,
            "Using Azure account key for authentication"
        );

        AzureAuthMethod::AccountKey(account_key)
    } else if std::env::var("AZURE_FEDERATED_TOKEN_FILE").is_ok() {
        // 5. Auto-detect Workload Identity via environment variable
        tracing::info!(
            account_name = %azure.account_name,
            container = %azure.container,
            "Using Azure Workload Identity for authentication (auto-detected via AZURE_FEDERATED_TOKEN_FILE)"
        );
        AzureAuthMethod::WorkloadIdentity
    } else {
        // 6. DefaultAzureCredential fallback
        tracing::info!(
            account_name = %azure.account_name,
            container = %azure.container,
            "Using Azure DefaultCredential chain for authentication"
        );
        AzureAuthMethod::DefaultCredential
    };

    Ok(ResolvedStorage::Azure(AzureStorageConfig {
        container: azure.container.clone(),
        account_name: azure.account_name.clone(),
        auth,
        prefix: azure.prefix.clone(),
        endpoint: azure.endpoint.clone(),
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
