//! #117: per-topic diagnostics must survive the result mapper and CRD admission.
use kafka_backup_core::validation::{CheckOutcome, ValidationResult};
use kafka_backup_operator::crd::{CheckResultStatus, KafkaBackupValidation};
use kafka_backup_operator::reconcilers::validation::validation_check_status;
use kube::CustomResourceExt;
use serde_json::json;

fn result() -> ValidationResult {
    ValidationResult {
        check_name: "MessageCountCheck".into(),
        outcome: CheckOutcome::Failed,
        detail: "2 topics; 200 messages expected, 195 restored; 1 discrepancies".into(),
        data: json!({"counts_complete": true, "count_method": "read_committed_record_scan",
        "topic_counts": [
            {"topic": "orders", "expected": 100, "observed": 95, "delta": -5,
             "delta_percentage": -5.0, "outcome": "FAILED", "scan_complete": true,
             "partitions": [{"partition": 0, "expected": 100, "observed": 95,
                "target_start_offset": 0, "target_end_offset": 100}]},
            {"topic": "control", "expected": 100, "observed": 100, "delta": 0,
             "delta_percentage": 0.0, "outcome": "PASSED", "scan_complete": true,
             "partitions": []}
        ]}),
        duration_ms: 200,
    }
}

#[test]
fn status_keeps_all_topic_results_and_readable_failure_details() {
    let core = result();
    let status = serde_json::to_value(validation_check_status(&core)).unwrap();
    assert_eq!(
        status["data"], core.data,
        "structured details must not be discarded"
    );
    let message = status["message"].as_str().unwrap();
    assert!(message.contains("orders [FAILED]"));
    assert!(message.contains("Observed: 95"));
    assert!(message.contains("control [PASSED]"));
    let roundtrip: CheckResultStatus = serde_json::from_value(status.clone()).unwrap();
    assert_eq!(serde_json::to_value(roundtrip).unwrap(), status);
    if let Ok(path) = std::env::var("ISSUE117_STATUS_PATCH") {
        std::fs::write(
            path,
            serde_json::to_vec(&json!({"status": {"checkResults": [status]}})).unwrap(),
        )
        .unwrap();
    }
}

#[test]
fn crd_keeps_nested_diagnostics_in_both_distributed_bundles() {
    let crd = serde_json::to_value(KafkaBackupValidation::crd()).unwrap();
    let item = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["status"]
        ["properties"]["checkResults"]["items"];
    assert_eq!(item["properties"]["data"]["type"], "object");
    assert_eq!(
        item["properties"]["data"]["x-kubernetes-preserve-unknown-fields"],
        true
    );
    for bundle in [
        "deploy/crds/all.yaml",
        "deploy/helm/kafka-backup-operator/crds/all.yaml",
    ] {
        let text = std::fs::read_to_string(bundle).unwrap();
        assert!(
            text.contains("x-kubernetes-preserve-unknown-fields: true"),
            "{bundle} would prune diagnostic rows"
        );
    }
}

#[test]
fn incomplete_scan_remains_unknown_and_legacy_status_remains_readable() {
    let mut core = result();
    core.data["counts_complete"] = json!(false);
    core.data["topic_counts"][0]["observed"] = json!(null);
    core.data["topic_counts"][0]["delta"] = json!(null);
    core.data["topic_counts"][0]["delta_percentage"] = json!(null);
    let status = serde_json::to_value(validation_check_status(&core)).unwrap();
    assert_eq!(status["data"]["counts_complete"], false);
    assert!(status["data"]["topic_counts"][0]["observed"].is_null());
    let legacy: CheckResultStatus =
        serde_json::from_value(json!({"checkName": "MessageCountCheck", "outcome": "Passed"}))
            .unwrap();
    assert_eq!(legacy.outcome, "Passed");
}
