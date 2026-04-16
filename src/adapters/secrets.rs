//! Secret fetching utilities for Kubernetes secrets

use k8s_openapi::api::core::v1::Secret;
use kube::{Api, Client};

use crate::error::{Error, Result};

/// Fetch a secret from Kubernetes
pub async fn get_secret(client: &Client, name: &str, namespace: &str) -> Result<Secret> {
    let secrets: Api<Secret> = Api::namespaced(client.clone(), namespace);
    secrets.get(name).await.map_err(|e| match e {
        kube::Error::Api(api_err) if api_err.code == 404 => {
            Error::SecretNotFound(format!("{}/{}", namespace, name))
        }
        other => Error::Kube(other),
    })
}

/// Get a string value from a secret
pub fn get_secret_string(secret: &Secret, key: &str) -> Result<String> {
    let data = secret
        .data
        .as_ref()
        .ok_or_else(|| Error::SecretKeyNotFound {
            secret: secret.metadata.name.clone().unwrap_or_default(),
            key: key.to_string(),
        })?;

    let bytes = data.get(key).ok_or_else(|| Error::SecretKeyNotFound {
        secret: secret.metadata.name.clone().unwrap_or_default(),
        key: key.to_string(),
    })?;

    String::from_utf8(bytes.0.clone())
        .map_err(|e| Error::Config(format!("Invalid UTF-8 in secret key '{}': {}", key, e)))
}

/// Fetch S3 credentials from a Kubernetes secret
pub async fn get_s3_credentials(
    client: &Client,
    namespace: &str,
    secret_name: &str,
    access_key_id_key: &str,
    secret_access_key_key: &str,
) -> Result<(String, String)> {
    let secret = get_secret(client, secret_name, namespace).await?;
    let access_key_id = get_secret_string(&secret, access_key_id_key)?;
    let secret_access_key = get_secret_string(&secret, secret_access_key_key)?;
    Ok((access_key_id, secret_access_key))
}

/// Fetch Azure account key credentials from a Kubernetes secret
pub async fn get_azure_credentials(
    client: &Client,
    namespace: &str,
    secret_name: &str,
    account_key_key: &str,
) -> Result<String> {
    let secret = get_secret(client, secret_name, namespace).await?;
    get_secret_string(&secret, account_key_key)
}

/// Fetch Azure SAS token from a Kubernetes secret
pub async fn get_azure_sas_token(
    client: &Client,
    namespace: &str,
    secret_name: &str,
    sas_token_key: &str,
) -> Result<String> {
    let secret = get_secret(client, secret_name, namespace).await?;
    get_secret_string(&secret, sas_token_key)
}

/// Azure Service Principal credentials
#[derive(Debug, Clone)]
pub struct AzureServicePrincipalCredentials {
    pub client_id: String,
    pub tenant_id: String,
    pub client_secret: String,
}

/// Fetch Azure Service Principal credentials from a Kubernetes secret
pub async fn get_azure_service_principal_credentials(
    client: &Client,
    namespace: &str,
    secret_name: &str,
    client_id_key: &str,
    tenant_id_key: &str,
    client_secret_key: &str,
) -> Result<AzureServicePrincipalCredentials> {
    let secret = get_secret(client, secret_name, namespace).await?;
    let client_id = get_secret_string(&secret, client_id_key)?;
    let tenant_id = get_secret_string(&secret, tenant_id_key)?;
    let client_secret = get_secret_string(&secret, client_secret_key)?;
    Ok(AzureServicePrincipalCredentials {
        client_id,
        tenant_id,
        client_secret,
    })
}

/// Fetch GCS credentials from a Kubernetes secret
pub async fn get_gcs_credentials(
    client: &Client,
    namespace: &str,
    secret_name: &str,
    service_account_json_key: &str,
) -> Result<String> {
    let secret = get_secret(client, secret_name, namespace).await?;
    get_secret_string(&secret, service_account_json_key)
}

/// Fetch TLS credentials from a Kubernetes secret
pub async fn get_tls_credentials(
    client: &Client,
    namespace: &str,
    secret_name: &str,
    ca_key: &str,
    cert_key: Option<&str>,
    key_key: Option<&str>,
) -> Result<TlsCredentials> {
    let secret = get_secret(client, secret_name, namespace).await?;

    let ca_cert = get_secret_string(&secret, ca_key)?;
    let client_cert = cert_key
        .map(|k| get_secret_string(&secret, k))
        .transpose()?;
    let client_key = key_key.map(|k| get_secret_string(&secret, k)).transpose()?;

    Ok(TlsCredentials {
        ca_cert,
        client_cert,
        client_key,
    })
}

/// TLS credentials structure
#[derive(Debug, Clone)]
pub struct TlsCredentials {
    pub ca_cert: String,
    pub client_cert: Option<String>,
    pub client_key: Option<String>,
}

/// Fetch a CA certificate from a dedicated Kubernetes secret
pub async fn get_ca_certificate(
    client: &Client,
    namespace: &str,
    secret_name: &str,
    ca_key: &str,
) -> Result<String> {
    let secret = get_secret(client, secret_name, namespace).await?;
    get_secret_string(&secret, ca_key)
}

/// Fetch TLS credentials, optionally sourcing the CA cert from a separate secret.
///
/// When `ca_secret_name` / `ca_secret_key` are provided, the CA certificate is
/// read from that secret instead of from `tls_secret_name`. This supports Strimzi
/// deployments where the cluster CA and client certificates are in separate secrets.
#[allow(clippy::too_many_arguments)]
pub async fn get_tls_credentials_split(
    client: &Client,
    namespace: &str,
    tls_secret_name: Option<&str>,
    ca_key_in_tls: &str,
    cert_key: Option<&str>,
    key_key: Option<&str>,
    ca_secret_name: Option<&str>,
    ca_secret_key: Option<&str>,
) -> Result<TlsCredentials> {
    // Determine CA cert source: prefer dedicated CA secret, fall back to TLS secret
    let ca_cert = if let (Some(ca_name), Some(ca_key)) = (ca_secret_name, ca_secret_key) {
        get_ca_certificate(client, namespace, ca_name, ca_key).await?
    } else if let Some(tls_name) = tls_secret_name {
        let secret = get_secret(client, tls_name, namespace).await?;
        get_secret_string(&secret, ca_key_in_tls)?
    } else {
        return Err(Error::Config(
            "Either tlsSecret or caSecret must be configured for TLS".to_string(),
        ));
    };

    // Get client cert/key from TLS secret if present
    let (client_cert, client_key) = if let Some(tls_name) = tls_secret_name {
        let secret = get_secret(client, tls_name, namespace).await?;
        let cert = cert_key
            .map(|k| get_secret_string(&secret, k))
            .transpose()?;
        let key = key_key.map(|k| get_secret_string(&secret, k)).transpose()?;
        (cert, key)
    } else {
        (None, None)
    };

    Ok(TlsCredentials {
        ca_cert,
        client_cert,
        client_key,
    })
}

/// Fetch SASL credentials from a Kubernetes secret
pub async fn get_sasl_credentials(
    client: &Client,
    namespace: &str,
    secret_name: &str,
    username_key: &str,
    password_key: &str,
) -> Result<(String, String)> {
    let secret = get_secret(client, secret_name, namespace).await?;
    let username = get_secret_string(&secret, username_key)?;
    let password = get_secret_string(&secret, password_key)?;
    Ok((username, password))
}
