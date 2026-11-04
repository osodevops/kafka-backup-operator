//! TLS credential file management
//!
//! Handles writing TLS credentials to temporary files for use by kafka-backup-core.
//! Files are automatically cleaned up when the TlsFileManager is dropped.
//!
//! A run killed mid-way (OOMKilled) never drops its manager, and /tmp is an
//! emptyDir that survives a container restart, so a run must cope with files
//! left by an earlier one (issue #99): every file is removed before it is
//! written, each resource has its own directory, and the operator clears the
//! TLS root when it starts.

use std::fs::{self, OpenOptions};
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
        create_private_dir(base_dir)?;

        // Write CA certificate
        let ca_cert_path = base_dir.join("ca.crt");
        write_file(&ca_cert_path, &credentials.ca_cert, 0o644)?;

        // Write client certificate if present
        let client_cert_path = if let Some(cert) = &credentials.client_cert {
            let path = base_dir.join("client.crt");
            write_file(&path, cert, 0o644)?;
            Some(path)
        } else {
            None
        };

        // Write client key if present: owner read-only from creation, so it
        // is never readable by others, not even between write and chmod.
        let client_key_path = if let Some(key) = &credentials.client_key {
            let path = base_dir.join("client.key");
            write_file(&path, key, 0o400)?;
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

/// Create `dir` (and its parents) and make it private to the operator.
fn create_private_dir(dir: &Path) -> Result<()> {
    fs::create_dir_all(dir)
        .map_err(|e| Error::Core(format!("Failed to create TLS directory {:?}: {}", dir, e)))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).map_err(|e| {
            Error::Core(format!(
                "Failed to set TLS directory permissions {:?}: {}",
                dir, e
            ))
        })?;
    }
    Ok(())
}

/// Write `content` to a new file at `path` created with `mode`.
///
/// An existing file is removed first: a run killed before its manager was
/// dropped leaves its files behind, and a read-only (0400) client.key cannot
/// be opened for writing again.
fn write_file(path: &Path, content: &str, mode: u32) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(Error::Core(format!(
                "Failed to remove stale file {:?}: {}",
                path, e
            )))
        }
    }
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(mode);
    }
    #[cfg(not(unix))]
    let _ = mode;
    let mut file = options
        .open(path)
        .map_err(|e| Error::Core(format!("Failed to create file {:?}: {}", path, e)))?;
    file.write_all(content.as_bytes())
        .map_err(|e| Error::Core(format!("Failed to write file {:?}: {}", path, e)))?;
    file.flush()
        .map_err(|e| Error::Core(format!("Failed to flush file {:?}: {}", path, e)))?;
    Ok(())
}

/// Root under which every run writes its TLS files.
pub const TLS_ROOT: &str = "/tmp/kafka-backup-tls";

/// TLS directory of one resource: `/tmp/kafka-backup-tls/<kind>/<namespace>/<name>`.
///
/// Kind and namespace keep resources with the same name (a KafkaBackup and a
/// KafkaRestore, or two namespaces) from sharing files. Runs of one resource
/// are serialised by the controller, so they can share its directory.
pub fn default_tls_dir(kind: &str, namespace: &str, name: &str) -> PathBuf {
    tls_dir_in(Path::new(TLS_ROOT), kind, namespace, name)
}

/// [`default_tls_dir`] under another root.
pub fn tls_dir_in(root: &Path, kind: &str, namespace: &str, name: &str) -> PathBuf {
    root.join(kind).join(namespace).join(name)
}

/// Remove everything under `root`. Called when the operator starts: no run is
/// in progress then, so anything there was left by a killed process, possibly
/// for a resource that has since been deleted. A missing root is fine.
pub fn clear_tls_root(root: &Path) -> std::io::Result<()> {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let path = entry?.path();
        if path.is_dir() {
            fs::remove_dir_all(&path)?;
        } else {
            fs::remove_file(&path)?;
        }
    }
    Ok(())
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
