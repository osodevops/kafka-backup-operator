//! Status patches, and the one log record written for each failure.
//!
//! Every reconciler sends its status through [`patch_status`]. A patch that
//! fails a resource (`phase: Failed`) is logged once at ERROR with the kind,
//! namespace, name and the `Ready=False` reason and message, so a failure can
//! be diagnosed from the operator log without reading each resource's status.
//! Callers must not log the same failure again.
//!
//! The record is written after the API server answers: "status set to Failed"
//! is only claimed for a patch that was accepted. When the patch is rejected
//! the failure is still logged, under a different message, because the status
//! does not hold it.

use kube::{
    api::{Patch, PatchParams},
    Api, Resource,
};
use serde_json::Value;
use tracing::{error, warn};

/// Message of the record for a failure the resource status now holds.
pub(crate) const FAILED_RECORDED: &str = "Resource status set to Failed";
/// Message of the record for a failure the API server refused to store.
pub(crate) const FAILED_NOT_RECORDED: &str =
    "Resource failed, and its Failed status could not be recorded";
/// Message of the record for a run that finished with some of its work failed.
pub(crate) const PARTIALLY_COMPLETED: &str = "Resource status set to PartiallyCompleted";

/// What a status patch says went wrong, read from the patch itself: a merge
/// patch that does not set the phase can leave an older failure in the
/// status, and that one was logged when it was written.
struct Failure<'a> {
    partial: bool,
    reason: Option<&'a str>,
    message: Option<&'a str>,
}

impl<'a> Failure<'a> {
    fn of(patch: &'a Value) -> Option<Self> {
        let status = &patch["status"];
        let partial = match status["phase"].as_str() {
            Some("Failed") => false,
            Some("PartiallyCompleted") => true,
            _ => return None,
        };
        let not_ready = status["conditions"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|c| c["type"].as_str() == Some("Ready") && c["status"].as_str() == Some("False"));
        Some(Self {
            partial,
            reason: not_ready.and_then(|c| c["reason"].as_str()),
            // Reconcilers that fail without a condition still set a message.
            message: not_ready
                .and_then(|c| c["message"].as_str())
                .or_else(|| status["message"].as_str()),
        })
    }
}

/// Merge-patch the status of `name`, logging the failure it records, if any.
pub(super) async fn patch_status<K>(
    api: &Api<K>,
    name: &str,
    patch: Value,
) -> crate::error::Result<()>
where
    K: Resource<DynamicType = ()> + Clone + serde::de::DeserializeOwned + std::fmt::Debug,
{
    let result = api
        .patch_status(
            name,
            &PatchParams::apply("kafka-backup-operator"),
            &Patch::Merge(&patch),
        )
        .await;

    if let Some(failure) = Failure::of(&patch) {
        let kind = K::kind(&());
        let namespace = api.namespace();
        let Failure {
            partial,
            reason,
            message,
        } = failure;
        match (&result, partial) {
            (Ok(_), false) => error!(
                kind = %kind,
                namespace,
                name,
                reason,
                condition_message = message,
                "{FAILED_RECORDED}"
            ),
            (Ok(_), true) => warn!(
                kind = %kind,
                namespace,
                name,
                reason,
                condition_message = message,
                "{PARTIALLY_COMPLETED}"
            ),
            (Err(e), _) => error!(
                kind = %kind,
                namespace,
                name,
                reason,
                condition_message = message,
                error = %e,
                "{FAILED_NOT_RECORDED}"
            ),
        }
    }

    result?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};

    use http::{Request, Response, StatusCode};
    use kube::{client::Body, Client};
    use serde_json::json;
    use tower_test::mock;

    use super::*;
    use crate::{crd::KafkaBackup, reconcilers::runs::interrupted_status};

    struct LogWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for LogWriter {
        fn write(&mut self, data: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().write(data)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// Send `patch` to a mock API server that answers `code`, and return
    /// whether it was accepted and the log records written, as JSON.
    async fn patch_and_capture(patch: Value, code: StatusCode) -> (bool, Vec<Value>) {
        let output = Arc::new(Mutex::new(Vec::new()));
        let captured = output.clone();
        let _subscriber = tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .json()
                .with_writer(move || LogWriter(captured.clone()))
                .finish(),
        );

        let (service, mut handle) = mock::pair::<Request<Body>, Response<Body>>();
        let api = Api::<KafkaBackup>::namespaced(Client::new(service, "backups"), "backups");
        let respond = async {
            let (request, send) = handle.next_request().await.unwrap();
            assert_eq!(request.method(), http::Method::PATCH);
            assert!(request.uri().path().ends_with("/orders/status"));
            assert!(
                output.lock().unwrap().is_empty(),
                "nothing is logged before the API server answers"
            );
            let body = if code == StatusCode::OK {
                json!({
                    "apiVersion": "kafka.oso.sh/v1alpha1", "kind": "KafkaBackup",
                    "metadata": {"name": "orders", "namespace": "backups"},
                    "spec": {
                        "kafkaCluster": {"bootstrapServers": ["kafka:9092"]},
                        "topics": ["orders"],
                        "storage": {"storageType": "pvc", "pvc": {"claimName": "backups"}}
                    },
                    // The stored status always holds an old failure: only the
                    // patch decides whether a record is written.
                    "status": interrupted_status("backup", Some(1))["status"]
                })
            } else {
                json!({"status": "Failure", "reason": "Forbidden", "message": "denied", "code": 403})
            };
            send.send_response(
                Response::builder()
                    .status(code)
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            );
        };
        let (result, ()) = tokio::join!(patch_status(&api, "orders", patch), respond);

        let log = output.lock().unwrap();
        let records = String::from_utf8_lossy(&log)
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        (result.is_ok(), records)
    }

    #[tokio::test]
    async fn an_accepted_failure_is_logged_once_with_its_reason() {
        let failed = interrupted_status("backup", Some(1));
        let (accepted, records) = patch_and_capture(failed.clone(), StatusCode::OK).await;

        assert!(accepted);
        assert_eq!(records.len(), 1, "{records:?}");
        assert_eq!(records[0]["level"], "ERROR");
        assert_eq!(
            records[0]["fields"],
            json!({
                "message": FAILED_RECORDED,
                "kind": "KafkaBackup",
                "namespace": "backups",
                "name": "orders",
                "reason": "Interrupted",
                "condition_message": failed["status"]["conditions"][0]["message"]
            })
        );
    }

    #[tokio::test]
    async fn a_rejected_failure_is_still_logged_but_not_as_recorded() {
        let failed = interrupted_status("backup", Some(1));
        let (accepted, records) = patch_and_capture(failed, StatusCode::FORBIDDEN).await;

        assert!(!accepted, "the caller still sees the rejected patch");
        assert_eq!(records.len(), 1, "{records:?}");
        assert_eq!(records[0]["level"], "ERROR");
        let fields = &records[0]["fields"];
        assert_eq!(fields["message"], FAILED_NOT_RECORDED);
        assert_eq!(fields["kind"], "KafkaBackup");
        assert_eq!(fields["namespace"], "backups");
        assert_eq!(fields["name"], "orders");
        assert_eq!(fields["reason"], "Interrupted");
        assert!(fields["error"].as_str().unwrap().contains("denied"));
    }

    #[tokio::test]
    async fn a_failure_without_a_condition_uses_the_status_message() {
        let patch = json!({"status": {"phase": "Failed", "message": "Backup failed: boom"}});
        let (_, records) = patch_and_capture(patch, StatusCode::OK).await;

        assert_eq!(records.len(), 1, "{records:?}");
        assert_eq!(records[0]["fields"]["message"], FAILED_RECORDED);
        assert_eq!(
            records[0]["fields"]["condition_message"],
            "Backup failed: boom"
        );
        assert!(records[0]["fields"].get("reason").is_none());
    }

    #[tokio::test]
    async fn a_partial_completion_is_a_warning() {
        let patch = json!({"status": {
            "phase": "PartiallyCompleted",
            "conditions": [{
                "type": "Ready", "status": "False",
                "reason": "PartialFailure", "message": "Reset 2 groups, 0 no-op, 1 failed"
            }]
        }});
        let (_, records) = patch_and_capture(patch, StatusCode::OK).await;

        assert_eq!(records.len(), 1, "{records:?}");
        assert_eq!(records[0]["level"], "WARN");
        assert_eq!(records[0]["fields"]["message"], PARTIALLY_COMPLETED);
        assert_eq!(records[0]["fields"]["reason"], "PartialFailure");
    }

    #[tokio::test]
    async fn patches_that_do_not_fail_the_resource_log_nothing() {
        for patch in [
            json!({"status": {"phase": "Completed"}}),
            json!({"status": {"phase": "Running", "message": "Backup in progress"}}),
            // Leaves the stored failure in place without writing a new one.
            json!({"status": {"observedGeneration": 2}}),
        ] {
            for code in [StatusCode::OK, StatusCode::FORBIDDEN] {
                let (accepted, records) = patch_and_capture(patch.clone(), code).await;
                assert_eq!(accepted, code == StatusCode::OK);
                assert!(records.is_empty(), "{patch}: {records:?}");
            }
        }
    }
}
