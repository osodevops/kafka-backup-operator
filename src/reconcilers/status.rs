use kube::{
    api::{Patch, PatchParams},
    Api, Resource,
};
use serde_json::Value;
use tracing::error;

pub(super) async fn patch_status<K>(
    api: &Api<K>,
    name: &str,
    patch: Value,
) -> crate::error::Result<()>
where
    K: Resource<DynamicType = ()> + Clone + serde::de::DeserializeOwned + std::fmt::Debug,
{
    let resource = api
        .patch_status(
            name,
            &PatchParams::apply("kafka-backup-operator"),
            &Patch::Merge(&patch),
        )
        .await?;

    // Inspect the submitted patch, not an old failure that a merge may leave intact.
    let status = &patch["status"];
    if status["phase"].as_str() == Some("Failed") {
        for condition in status["conditions"].as_array().into_iter().flatten() {
            if condition["type"].as_str() == Some("Ready")
                && condition["status"].as_str() == Some("False")
            {
                error!(
                    kind = %K::kind(&()),
                    namespace = resource.meta().namespace.as_deref(),
                    name,
                    reason = condition["reason"].as_str(),
                    condition_message = condition["message"].as_str(),
                    "Resource status set to Failed"
                );
            }
        }
    }

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

    #[tokio::test]
    async fn logs_only_accepted_failure_writes() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let captured = output.clone();
        let _subscriber = tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .json()
                .with_writer(move || LogWriter(captured.clone()))
                .finish(),
        );
        let failed = interrupted_status("backup", Some(1));
        let cases = [
            (failed.clone(), StatusCode::OK),
            (failed.clone(), StatusCode::FORBIDDEN),
            (json!({"status": {"phase": "Completed"}}), StatusCode::OK),
            (json!({"status": {"observedGeneration": 2}}), StatusCode::OK),
        ];

        for (index, (patch, code)) in cases.into_iter().enumerate() {
            output.lock().unwrap().clear();
            let (service, mut handle) = mock::pair::<Request<Body>, Response<Body>>();
            let api = Api::<KafkaBackup>::namespaced(Client::new(service, "backups"), "backups");
            let respond = async {
                let (request, send) = handle.next_request().await.unwrap();
                assert_eq!(request.method(), http::Method::PATCH);
                assert!(request.uri().path().ends_with("/orders/status"));
                assert!(output.lock().unwrap().is_empty());
                let body = if code == StatusCode::OK {
                    json!({
                        "apiVersion": "kafka.oso.sh/v1alpha1", "kind": "KafkaBackup",
                        "metadata": {"name": "orders", "namespace": "backups"},
                        "spec": {
                            "kafkaCluster": {"bootstrapServers": ["kafka:9092"]},
                            "topics": ["orders"],
                            "storage": {"storageType": "local", "local": {"path": "/backups"}}
                        },
                        "status": if patch["status"]["phase"] == "Completed" {
                            &patch["status"]
                        } else {
                            &failed["status"]
                        }
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
            let (result, ()) = tokio::join!(patch_status(&api, "orders", patch.clone()), respond);
            assert_eq!(result.is_ok(), code == StatusCode::OK);
            let log = output.lock().unwrap();
            if index == 0 {
                let event: Value = serde_json::from_slice(&log).unwrap();
                assert_eq!(event["level"], "ERROR");
                assert_eq!(
                    event["fields"],
                    json!({
                        "message": "Resource status set to Failed",
                        "kind": "KafkaBackup",
                        "namespace": "backups",
                        "name": "orders",
                        "reason": "Interrupted",
                        "condition_message": failed["status"]["conditions"][0]["message"]
                    })
                );
            } else {
                assert!(log.is_empty(), "unexpected failure log for case {index}");
            }
        }
    }
}
