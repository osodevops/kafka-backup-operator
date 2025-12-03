//! KafkaOffsetReset controller
//!
//! Watches KafkaOffsetReset resources and triggers reconciliation.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use kube::{
    api::ListParams,
    runtime::{
        controller::{Action, Controller},
        finalizer::{finalizer, Event as FinalizerEvent},
        watcher::Config as WatcherConfig,
    },
    Api, Client, ResourceExt,
};
use tracing::{error, info, instrument, warn};

use crate::controllers::Context;
use crate::crd::KafkaOffsetReset;
use crate::error::{Error, Result};
use crate::metrics;
use crate::reconcilers::offset_reset as offset_reset_reconciler;

/// Finalizer name for KafkaOffsetReset resources
const FINALIZER_NAME: &str = "kafka.oso.sh/offset-reset-finalizer";

/// Run the KafkaOffsetReset controller
pub async fn run(client: Client, context: Arc<Context>) {
    let api: Api<KafkaOffsetReset> = Api::all(client.clone());

    // Verify CRD is installed
    if let Err(e) = api.list(&ListParams::default().limit(1)).await {
        error!("KafkaOffsetReset CRD not installed: {}", e);
        return;
    }

    info!("Starting KafkaOffsetReset controller");

    Controller::new(api, WatcherConfig::default())
        .shutdown_on_signal()
        .run(reconcile, error_policy, context)
        .for_each(|result| async move {
            match result {
                Ok((obj, _action)) => {
                    info!(
                        name = %obj.name,
                        namespace = obj.namespace.as_deref().unwrap_or("default"),
                        "Reconciled KafkaOffsetReset"
                    );
                }
                Err(e) => {
                    error!(error = %e, "Reconciliation error");
                    metrics::RECONCILIATION_ERRORS.with_label_values(&["KafkaOffsetReset"]).inc();
                }
            }
        })
        .await;
}

/// Main reconciliation function
#[instrument(skip(ctx), fields(name = %obj.name_any(), namespace = obj.namespace()))]
async fn reconcile(obj: Arc<KafkaOffsetReset>, ctx: Arc<Context>) -> Result<Action> {
    let _timer = metrics::RECONCILE_DURATION
        .with_label_values(&["KafkaOffsetReset"])
        .start_timer();
    metrics::RECONCILIATIONS.with_label_values(&["KafkaOffsetReset"]).inc();

    let namespace = obj.namespace().unwrap_or_else(|| "default".to_string());
    let api: Api<KafkaOffsetReset> = Api::namespaced(ctx.client.clone(), &namespace);

    finalizer(&api, FINALIZER_NAME, obj, |event| async {
        match event {
            FinalizerEvent::Apply(reset) => apply(reset, ctx.clone()).await,
            FinalizerEvent::Cleanup(reset) => cleanup(reset, ctx.clone()).await,
        }
    })
    .await
    .map_err(|e| Error::Finalizer(Box::new(e)))
}

/// Apply reconciliation (create/update)
async fn apply(reset: Arc<KafkaOffsetReset>, ctx: Arc<Context>) -> Result<Action> {
    let name = reset.name_any();
    let namespace = reset.namespace().unwrap_or_else(|| "default".to_string());
    let generation = reset.metadata.generation.unwrap_or(0);

    info!(
        name = %name,
        namespace = %namespace,
        generation = generation,
        "Reconciling KafkaOffsetReset"
    );

    // Check if we've already processed this generation
    if let Some(status) = &reset.status {
        if status.observed_generation == Some(generation) {
            match status.phase.as_deref() {
                Some("Completed") | Some("Failed") | Some("PartiallyCompleted") => {
                    return Ok(Action::await_change());
                }
                Some("Running") => {
                    return offset_reset_reconciler::monitor_progress(&reset, &ctx.client, &namespace)
                        .await;
                }
                _ => {}
            }
        }
    }

    // Validate the spec
    if let Err(e) = offset_reset_reconciler::validate(&reset) {
        warn!(error = %e, "Validation failed");
        offset_reset_reconciler::update_status_failed(&reset, &ctx.client, &namespace, &e.to_string())
            .await?;
        return Ok(Action::requeue(Duration::from_secs(300)));
    }

    // Execute offset reset operation
    offset_reset_reconciler::execute(&reset, &ctx.client, &namespace).await
}

/// Cleanup when resource is being deleted
async fn cleanup(reset: Arc<KafkaOffsetReset>, _ctx: Arc<Context>) -> Result<Action> {
    let name = reset.name_any();
    info!(name = %name, "Cleaning up KafkaOffsetReset");

    metrics::CLEANUPS.with_label_values(&["KafkaOffsetReset"]).inc();

    Ok(Action::await_change())
}

/// Error policy for the controller
fn error_policy(obj: Arc<KafkaOffsetReset>, error: &Error, _ctx: Arc<Context>) -> Action {
    let name = obj.name_any();
    error!(
        name = %name,
        error = %error,
        "Reconciliation failed, scheduling retry"
    );

    let requeue_duration = match error {
        Error::Kube(_) => Duration::from_secs(30),
        Error::Config(_) | Error::Validation(_) => Duration::from_secs(300),
        _ => Duration::from_secs(30),
    };

    Action::requeue(requeue_duration)
}
