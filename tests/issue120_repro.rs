//! Issue #120: GCS must opt in to ADC without reading a JSON-key Secret.

use http::{Request, Response};
use kafka_backup_core::storage::StorageBackendConfig;
use kafka_backup_operator::adapters::{
    build_backup_config, build_restore_config, build_storage_config, to_core_storage_config,
    ResolvedBackupSource,
};
use kafka_backup_operator::crd::{KafkaBackup, KafkaRestore, StorageSpec};
use kafka_backup_operator::reconcilers::backup::validate;
use kube::{client::Body, Client, CustomResourceExt};
use serde_json::{json, Value};
use tower_test::mock;

fn storage(gcs: Value) -> Value {
    json!({"storageType": "gcs", "gcs": gcs})
}

fn backup(storage: Value) -> KafkaBackup {
    serde_json::from_value(json!({
        "apiVersion": "kafka.oso.sh/v1alpha1", "kind": "KafkaBackup",
        "metadata": {"name": "gcs-backup", "namespace": "backups"},
        "spec": {
            "kafkaCluster": {"bootstrapServers": ["kafka:9092"]},
            "topics": ["orders"], "storage": storage
        }
    }))
    .expect("GCS backup spec must deserialize")
}

fn assert_gcs(config: StorageBackendConfig, credentials: Option<&str>) {
    match config {
        StorageBackendConfig::Gcs {
            bucket,
            prefix,
            service_account_path,
        } => {
            assert_eq!(bucket, "backups");
            assert_eq!(prefix.as_deref(), Some("prod/kafka"));
            assert_eq!(service_account_path.as_deref(), credentials);
        }
        other => panic!("expected GCS, got {other:?}"),
    }
}

#[tokio::test]
async fn workload_identity_resolves_backup_and_restore_without_any_secret_requests() {
    let (service, _handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "backups");
    let storage = storage(json!({
        "bucket": "backups", "prefix": "prod/kafka", "useWorkloadIdentity": true
    }));
    let backup = backup(storage.clone());
    validate(&backup).unwrap();
    // No mock requests are answered: an accidental Secret GET fails this test.
    let resolved = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        build_backup_config(&backup, &client, "backups"),
    )
    .await
    .expect("must not read a Secret")
    .unwrap();
    assert_gcs(to_core_storage_config(&resolved.storage), None);

    let restore: KafkaRestore = serde_json::from_value(json!({
        "apiVersion": "kafka.oso.sh/v1alpha1", "kind": "KafkaRestore",
        "metadata": {"name": "gcs-restore", "namespace": "backups"},
        "spec": {
            "kafkaCluster": {"bootstrapServers": ["kafka:9092"]},
            "backupRef": {"name": "external", "storage": storage, "backupId": "gcs-backup"}
        }
    }))
    .unwrap();
    let resolved = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        build_restore_config(&restore, &client, "backups"),
    )
    .await
    .expect("must not read a Secret")
    .unwrap();
    match resolved.backup_source {
        ResolvedBackupSource::Storage { storage, .. } => {
            assert_gcs(to_core_storage_config(&storage), None);
        }
        _ => panic!("expected direct GCS storage"),
    }
}

#[tokio::test]
async fn invalid_auth_choices_are_rejected_before_any_secret_requests() {
    let (service, _handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "backups");
    for gcs in [
        json!({"bucket": "backups"}),
        json!({"bucket": "backups", "useWorkloadIdentity": false}),
        json!({"bucket": "backups", "useWorkloadIdentity": true,
               "credentialsSecret": {"name": "key"}}),
    ] {
        let backup = backup(storage(gcs));
        let error = validate(&backup).expect_err("must reject missing/conflicting credentials");
        assert!(error.to_string().contains("GCS"));
        // Restore and evidence paths use the adapter without backup validation.
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            build_storage_config(&backup.spec.storage, &client, "backups"),
        )
        .await
        .expect("must validate before reading a Secret")
        .unwrap_err();
        assert!(error.to_string().contains("GCS"));
    }
}

#[tokio::test]
async fn legacy_secret_auth_keeps_default_and_custom_secret_keys() {
    for key in [None, Some("custom.json")] {
        let mut gcs = json!({"bucket": "backups", "prefix": "prod/kafka",
                             "credentialsSecret": {"name": "gcs-key"}});
        if let Some(key) = key {
            gcs["credentialsSecret"]["serviceAccountJsonKey"] = json!(key);
            gcs["useWorkloadIdentity"] = json!(false);
        }
        let spec: StorageSpec = serde_json::from_value(storage(gcs)).unwrap();
        let (service, mut handle) = mock::pair::<Request<Body>, Response<Body>>();
        let client = Client::new(service, "backups");
        let server = tokio::spawn(async move {
            let (request, send) = handle.next_request().await.unwrap();
            assert_eq!(request.method(), "GET");
            assert_eq!(
                request.uri().path(),
                "/api/v1/namespaces/backups/secrets/gcs-key"
            );
            // Synthetic RSA key, never used outside this local test. Build the
            // real core client to catch JSON-content-versus-filepath mistakes.
            let secret = k8s_openapi::api::core::v1::Secret {
                data: Some(std::collections::BTreeMap::from([(
                    key.unwrap_or("SERVICE_ACCOUNT_JSON").to_string(),
                    k8s_openapi::ByteString(
                        include_bytes!("fixtures/gcs-service-account.json").to_vec(),
                    ),
                )])),
                ..Default::default()
            };
            send.send_response(
                Response::builder()
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&secret).unwrap()))
                    .unwrap(),
            );
        });
        let resolved = build_storage_config(&spec, &client, "backups")
            .await
            .unwrap();
        let config = to_core_storage_config(&resolved);
        let path = match &config {
            StorageBackendConfig::Gcs {
                service_account_path: Some(path),
                ..
            } => path.clone(),
            _ => panic!("expected explicit credentials"),
        };
        assert_gcs(config.clone(), Some(&path));
        assert!(!format!("{resolved:?}").contains("BEGIN PRIVATE KEY"));
        kafka_backup_operator::install_crypto_provider();
        kafka_backup_core::storage::create_backend(&config)
            .expect("Secret JSON must reach core as a readable credential file");
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            include_str!("fixtures/gcs-service-account.json")
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        let cloned = resolved.clone();
        drop(resolved);
        assert!(
            std::path::Path::new(&path).exists(),
            "a clone still needs the credentials"
        );
        drop(cloned);
        assert!(
            !std::path::Path::new(&path).exists(),
            "the last owner must remove the credentials"
        );
        server.await.unwrap();
    }
}

#[test]
fn backup_and_restore_crds_allow_optional_secret_with_explicit_auth_validation() {
    for (crd, pointer) in [
        (KafkaBackup::crd(), "/properties/spec/properties/storage"),
        (
            KafkaRestore::crd(),
            "/properties/spec/properties/backupRef/properties/storage",
        ),
    ] {
        let schema = serde_json::to_value(
            crd.spec.versions[0]
                .schema
                .as_ref()
                .unwrap()
                .open_api_v3_schema
                .as_ref()
                .unwrap(),
        )
        .unwrap();
        let storage = schema.pointer(pointer).unwrap();
        let gcs = &storage["properties"]["gcs"];
        assert!(!gcs["required"]
            .as_array()
            .unwrap()
            .contains(&json!("credentialsSecret")));
        assert_eq!(gcs["properties"]["useWorkloadIdentity"]["default"], false);
        assert_eq!(gcs["nullable"], true);
        assert_eq!(gcs["x-kubernetes-validations"].as_array().unwrap().len(), 1);
        assert!(!storage["required"]
            .as_array()
            .unwrap_or(&vec![])
            .contains(&json!("gcs")));
    }
}
