//! kafka-backup#256: retry settings must survive CRD admission and both adapters.
use kafka_backup_operator::adapters::{build_backup_config, to_core_backup_config};
use kafka_backup_operator::crd::KafkaBackup;
use kube::CustomResourceExt;
use serde_json::{json, Value};

async fn config(retry: Option<u64>, wait: Option<i32>) -> kafka_backup_core::Result<Value> {
    kafka_backup_operator::install_crypto_provider();
    let client =
        kube::Client::try_from(kube::Config::new("http://127.0.0.1".parse().unwrap())).unwrap();
    let mut spec = json!({
        "kafkaCluster": {"bootstrapServers": ["kafka:9092"], "connection": {}},
        "topics": ["cold"],
        "storage": {"storageType": "pvc", "pvc": {"claimName": "backups"}}
    });
    if let Some(retry) = retry {
        spec["emptyFetchRetrySecs"] = json!(retry);
    }
    if let Some(wait) = wait {
        spec["kafkaCluster"]["connection"]["fetchMaxWaitMs"] = json!(wait);
    }
    let backup = KafkaBackup::new("cold", serde_json::from_value(spec).unwrap());
    let resolved = build_backup_config(&backup, &client, "default")
        .await
        .unwrap();
    Ok(serde_json::to_value(to_core_backup_config(&resolved, "cold", None)?).unwrap())
}

#[tokio::test]
async fn defaults_enable_bounded_retries() {
    let c = config(None, None).await.unwrap();
    assert_eq!(c["backup"]["empty_fetch_retry_secs"], 30);
    assert_eq!(c["source"]["connection"]["fetch_max_wait_ms"], 500);
}

#[tokio::test]
async fn overrides_and_zero_budget_reach_core() {
    for retry in [0, 60, 3600] {
        let c = config(Some(retry), Some(5000)).await.unwrap();
        assert_eq!(c["backup"]["empty_fetch_retry_secs"], retry);
        assert_eq!(c["source"]["connection"]["fetch_max_wait_ms"], 5000);
    }
}

#[tokio::test]
async fn core_rejects_out_of_range_values_before_networking() {
    for (retry, wait) in [(3601, 500), (30, -1), (30, 30001)] {
        assert!(config(Some(retry), Some(wait)).await.is_err());
    }
}

#[test]
fn admission_schema_preserves_defaults_and_bounds() {
    let crd = serde_json::to_value(KafkaBackup::crd()).unwrap();
    let props = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
        ["properties"];
    let retry = &props["emptyFetchRetrySecs"];
    assert_eq!(retry["default"], 30);
    assert_eq!(retry["minimum"], 0.0);
    assert_eq!(retry["maximum"], 3600.0);
    let wait = &props["kafkaCluster"]["properties"]["connection"]["properties"]["fetchMaxWaitMs"];
    assert_eq!(wait["default"], 500);
    assert_eq!(wait["minimum"], 0.0);
    assert_eq!(wait["maximum"], 30000.0);
}
