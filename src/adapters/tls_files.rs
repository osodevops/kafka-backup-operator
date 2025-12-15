//! TLS credential file management
//!
//! Handles writing TLS credentials to temporary files for use by kafka-backup-core.
//! Files are automatically cleaned up when the TlsFileManager is dropped.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

use super::secrets::TlsCredentials;

/// Manages TLS credential files
///
/// Writes TLS certificates and keys to temporary files and provides paths
/// for kafka-backup-core. Files are automatically cleaned up on drop.
pub struct TlsFileManager {
    /// Directory containing the credential files
    base_dir: PathBuf,
    /// Path to CA certificate file
    pub ca_cert_path: PathBuf,
    /// Path to client certificate file (if present)
    pub client_cert_path: Option<PathBuf>,
    /// Path to client key file (if present)
    pub client_key_path: Option<PathBuf>,
    /// Whether to delete files on drop
    cleanup_on_drop: bool,
}

impl TlsFileManager {
    /// Create TLS files from credentials
    ///
    /// # Arguments
    /// * `credentials` - TLS credentials to write
    /// * `base_dir` - Directory to create files in (will be created if needed)
    ///
    /// # Returns
    /// A TlsFileManager that manages the created files
    pub fn new(credentials: &TlsCredentials, base_dir: &Path) -> Result<Self> {
        // Create the base directory if it doesn't exist
        fs::create_dir_all(base_dir).map_err(|e| {
            Error::Core(format!(
                "Failed to create TLS directory {:?}: {}",
                base_dir, e
            ))
        })?;

        // Write CA certificate
        let ca_cert_path = base_dir.join("ca.crt");
        write_file(&ca_cert_path, &credentials.ca_cert)?;

        // Write client certificate if present
        let client_cert_path = if let Some(cert) = &credentials.client_cert {
            let path = base_dir.join("client.crt");
            write_file(&path, cert)?;
            Some(path)
        } else {
            None
        };

        // Write client key if present
        let client_key_path = if let Some(key) = &credentials.client_key {
            let path = base_dir.join("client.key");
            write_file(&path, key)?;
            // Set restrictive permissions on key file (owner read only)
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mut perms = fs::metadata(&path)
                    .map_err(|e| Error::Core(format!("Failed to get key file metadata: {}", e)))?
                    .permissions();
                perms.set_mode(0o400);
                fs::set_permissions(&path, perms).map_err(|e| {
                    Error::Core(format!("Failed to set key file permissions: {}", e))
                })?;
            }
            Some(path)
        } else {
            None
        };

        Ok(Self {
            base_dir: base_dir.to_path_buf(),
            ca_cert_path,
            client_cert_path,
            client_key_path,
            cleanup_on_drop: true,
        })
    }

    /// Create a TlsFileManager from existing file paths (no file creation)
    ///
    /// Useful when certificates are mounted as volumes
    pub fn from_paths(
        ca_cert_path: PathBuf,
        client_cert_path: Option<PathBuf>,
        client_key_path: Option<PathBuf>,
    ) -> Self {
        Self {
            base_dir: ca_cert_path
                .parent()
                .unwrap_or(Path::new("/"))
                .to_path_buf(),
            ca_cert_path,
            client_cert_path,
            client_key_path,
            cleanup_on_drop: false, // Don't delete mounted files
        }
    }

    /// Disable cleanup on drop (useful for debugging)
    pub fn keep_files(mut self) -> Self {
        self.cleanup_on_drop = false;
        self
    }

    /// Get CA certificate path for kafka-backup-core
    pub fn ca_location(&self) -> PathBuf {
        self.ca_cert_path.clone()
    }

    /// Get client certificate path for kafka-backup-core
    pub fn certificate_location(&self) -> Option<PathBuf> {
        self.client_cert_path.clone()
    }

    /// Get client key path for kafka-backup-core
    pub fn key_location(&self) -> Option<PathBuf> {
        self.client_key_path.clone()
    }
}

impl Drop for TlsFileManager {
    fn drop(&mut self) {
        if self.cleanup_on_drop {
            // Clean up files
            let _ = fs::remove_file(&self.ca_cert_path);
            if let Some(path) = &self.client_cert_path {
                let _ = fs::remove_file(path);
            }
            if let Some(path) = &self.client_key_path {
                let _ = fs::remove_file(path);
            }
            // Try to remove the directory if empty
            let _ = fs::remove_dir(&self.base_dir);
        }
    }
}

/// Write content to a file
fn write_file(path: &Path, content: &str) -> Result<()> {
    let mut file = File::create(path)
        .map_err(|e| Error::Core(format!("Failed to create file {:?}: {}", path, e)))?;
    file.write_all(content.as_bytes())
        .map_err(|e| Error::Core(format!("Failed to write file {:?}: {}", path, e)))?;
    file.flush()
        .map_err(|e| Error::Core(format!("Failed to flush file {:?}: {}", path, e)))?;
    Ok(())
}

/// Get the default TLS directory for an operation
pub fn default_tls_dir(operation_id: &str) -> PathBuf {
    PathBuf::from(format!("/tmp/kafka-backup-tls/{}", operation_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_tls_file_manager_creates_files() {
        let dir = tempdir().unwrap();
        let creds = TlsCredentials {
            ca_cert: "-----BEGIN CERTIFICATE-----\ntest-ca\n-----END CERTIFICATE-----".to_string(),
            client_cert: Some(
                "-----BEGIN CERTIFICATE-----\ntest-client\n-----END CERTIFICATE-----".to_string(),
            ),
            client_key: Some(
                "-----BEGIN PRIVATE KEY-----\ntest-key\n-----END PRIVATE KEY-----".to_string(),
            ),
        };

        let manager = TlsFileManager::new(&creds, dir.path()).unwrap();

        assert!(manager.ca_cert_path.exists());
        assert!(manager.client_cert_path.as_ref().unwrap().exists());
        assert!(manager.client_key_path.as_ref().unwrap().exists());

        // Verify file contents
        let ca_content = fs::read_to_string(&manager.ca_cert_path).unwrap();
        assert!(ca_content.contains("test-ca"));
    }

    #[test]
    fn test_tls_file_manager_cleans_up_on_drop() {
        let dir = tempdir().unwrap();
        let base = dir.path().join("tls-test");

        let creds = TlsCredentials {
            ca_cert: "test-ca".to_string(),
            client_cert: None,
            client_key: None,
        };

        let ca_path;
        {
            let manager = TlsFileManager::new(&creds, &base).unwrap();
            ca_path = manager.ca_cert_path.clone();
            assert!(ca_path.exists());
        }
        // After drop, file should be cleaned up
        assert!(!ca_path.exists());
    }

    #[test]
    fn test_tls_file_manager_keep_files() {
        let dir = tempdir().unwrap();
        let base = dir.path().join("tls-keep");

        let creds = TlsCredentials {
            ca_cert: "test-ca".to_string(),
            client_cert: None,
            client_key: None,
        };

        let ca_path;
        {
            let manager = TlsFileManager::new(&creds, &base).unwrap().keep_files();
            ca_path = manager.ca_cert_path.clone();
            assert!(ca_path.exists());
        }
        // After drop, file should still exist because we called keep_files()
        assert!(ca_path.exists());
    }
}
