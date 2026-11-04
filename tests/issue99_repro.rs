//! Issue #99: a leftover read-only client.key made every later run fail.
//!
//! TLS credentials are written to a directory under /tmp and removed when the
//! run's `TlsFileManager` is dropped. When the operator container is killed
//! mid-run (OOMKilled), the drop never runs; /tmp is an emptyDir that survives
//! the container restart, and `File::create` on the leftover 0400 client.key
//! failed with "Permission denied" on every later run (reproduced in minikube:
//! SIGKILL, exit 137, then `Failed to create file
//! "/tmp/kafka-backup-tls/mtls-backup/client.key": Permission denied`).
//! The directory was named after the resource only, so resources of
//! different kinds or namespaces with the same name shared it.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use kafka_backup_operator::adapters::{
    clear_tls_root, default_tls_dir, tls_dir_in, TlsCredentials, TlsFileManager,
};

fn creds(tag: &str) -> TlsCredentials {
    TlsCredentials {
        ca_cert: format!("-----BEGIN CERTIFICATE-----\n{tag}-ca\n-----END CERTIFICATE-----"),
        client_cert: Some(format!(
            "-----BEGIN CERTIFICATE-----\n{tag}-cert\n-----END CERTIFICATE-----"
        )),
        client_key: Some(format!(
            "-----BEGIN PRIVATE KEY-----\n{tag}-key\n-----END PRIVATE KEY-----"
        )),
    }
}

fn mode(path: &Path) -> u32 {
    fs::metadata(path).unwrap().permissions().mode() & 0o777
}

/// What a killed run leaves behind: the files, with client.key read-only.
fn leave_crash_leftovers(dir: &Path) {
    fs::create_dir_all(dir).unwrap();
    for file in ["ca.crt", "client.crt", "client.key"] {
        fs::write(dir.join(file), "stale").unwrap();
    }
    fs::set_permissions(dir.join("client.key"), fs::Permissions::from_mode(0o400)).unwrap();
}

#[test]
fn a_read_only_key_left_by_a_killed_run_is_replaced() {
    let root = tempfile::tempdir().unwrap();
    let dir = tls_dir_in(root.path(), "KafkaBackup", "kbo", "mtls-backup");
    leave_crash_leftovers(&dir);

    let manager = TlsFileManager::new(&creds("fresh"), &dir)
        .expect("a leftover read-only client.key must not block the next run");

    let key = manager.client_key_path.clone().unwrap();
    assert!(fs::read_to_string(&key).unwrap().contains("fresh-key"));
    assert!(fs::read_to_string(&manager.ca_cert_path)
        .unwrap()
        .contains("fresh-ca"));
    assert!(
        fs::read_to_string(manager.client_cert_path.as_ref().unwrap())
            .unwrap()
            .contains("fresh-cert")
    );
}

#[test]
fn the_key_and_its_directory_are_private_from_creation() {
    let root = tempfile::tempdir().unwrap();
    let dir = tls_dir_in(root.path(), "KafkaRestore", "kbo", "restore");
    let manager = TlsFileManager::new(&creds("p"), &dir).unwrap();

    assert_eq!(mode(manager.client_key_path.as_ref().unwrap()), 0o400);
    assert_eq!(mode(&dir), 0o700);
}

#[test]
fn resources_of_another_kind_or_namespace_never_share_a_directory() {
    let a = default_tls_dir("KafkaBackup", "team-a", "nightly");
    for other in [
        default_tls_dir("KafkaRestore", "team-a", "nightly"),
        default_tls_dir("KafkaBackup", "team-b", "nightly"),
        default_tls_dir("KafkaOffsetReset", "team-a", "nightly"),
    ] {
        assert_ne!(a, other);
    }
    assert_eq!(
        a,
        Path::new("/tmp/kafka-backup-tls/KafkaBackup/team-a/nightly")
    );
}

/// Before: a KafkaRestore finishing first deleted the client.key of a
/// running KafkaBackup with the same name.
#[test]
fn dropping_one_resource_leaves_another_same_named_resource_alone() {
    let root = tempfile::tempdir().unwrap();
    let backup = TlsFileManager::new(
        &creds("backup"),
        &tls_dir_in(root.path(), "KafkaBackup", "kbo", "same"),
    )
    .unwrap();
    {
        let _restore = TlsFileManager::new(
            &creds("restore"),
            &tls_dir_in(root.path(), "KafkaRestore", "kbo", "same"),
        )
        .unwrap();
    }
    let key = backup.client_key_path.clone().unwrap();
    assert!(fs::read_to_string(key).unwrap().contains("backup-key"));
}

/// Nothing runs when the operator starts, so whatever is under the TLS root
/// was left by a killed process (possibly for a resource since deleted).
#[test]
fn startup_clears_what_killed_runs_left() {
    let root = tempfile::tempdir().unwrap();
    let dir = tls_dir_in(root.path(), "KafkaBackup", "kbo", "gone");
    leave_crash_leftovers(&dir);

    clear_tls_root(root.path()).unwrap();
    assert!(!dir.exists());
    assert!(fs::read_dir(root.path()).unwrap().next().is_none());

    // A missing root is not an error (first start).
    clear_tls_root(&root.path().join("never-created")).unwrap();
}
