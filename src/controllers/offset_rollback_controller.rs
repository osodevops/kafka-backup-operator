//! KafkaOffsetRollback controller
//!
//! Watches KafkaOffsetRollback resources and triggers reconciliation.

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
use crate::crd::KafkaOffsetRollback;
use crate::error::{Error, Result};
use crate::metrics;
use crate::reconcilers::offset_rollback as offset_rollback_reconciler;

/// Finalizer name for KafkaOffsetRollback resources
const FINALIZER_NAME: &str = "kafka.oso.sh/offset-rollback-finalizer";

/// Run the KafkaOffsetRollback controller
pub async fn run(client: Client, context: Arc<Context>) {
    let api: Api<KafkaOffsetRollback> = Api::all(client.clone());

    // Verify CRD is installed
    if let Err(e) = api.list(&ListParams::default().limit(1)).await {
        error!("KafkaOffsetRollback CRD not installed: {}", e);
        return;
    }

    info!("Starting KafkaOffsetRollback controller");

    Controller::new(api, WatcherConfig::default())
        .shutdown_on_signal()
        .run(reconcile, error_policy, context)
        .for_each(|result| async move {
            match result {
                Ok((obj, _action)) => {
                    info!(
                        name = %obj.name,
                        namespace = obj.namespace.as_deref().unwrap_or("default"),
                        "Reconciled KafkaOffsetRollback"
                    );
                }
                Err(e) => {
                    error!(error = %e, "Reconciliation error");
                    metrics::RECONCILIATION_ERRORS.with_label_values(&["KafkaOffsetRollback"]).inc();
                }
            }
        })
        .await;
}

/// Main reconciliation function
#[instrument(skip(ctx, obj), fields(name = %obj.name_any(), namespace = obj.namespace()))]
async fn reconcile(obj: Arc<KafkaOffsetRollback>, ctx: Arc<Context>) -> Result<Action> {
    let _timer = metrics::RECONCILE_DURATION
        .with_label_values(&["KafkaOffsetRollback"])
        .start_timer();
    metrics::RECONCILIATIONS.with_label_values(&["KafkaOffsetRollback"]).inc();

    let namespace = obj.namespace().unwrap_or_else(|| "default".to_string());
    let api: Api<KafkaOffsetRollback> = Api::namespaced(ctx.client.clone(), &namespace);

    finalizer(&api, FINALIZER_NAME, obj, |event| async {
        match event {
            FinalizerEvent::Apply(rollback) => apply(rollback, ctx.clone()).await,
            FinalizerEvent::Cleanup(rollback) => cleanup(rollback, ctx.clone()).await,
        }
    })
    .await
    .map_err(|e| Error::Finalizer(Box::new(e)))
}

/// Apply reconciliation (create/update)
async fn apply(rollback: Arc<KafkaOffsetRollback>, ctx: Arc<Context>) -> Result<Action> {
    let name = rollback.name_any();
    let namespace = rollback.namespace().unwrap_or_else(|| "default".to_string());
    let generation = rollback.metadata.generation.unwrap_or(0);

    info!(
        name = %name,
        namespace = %namespace,
        generation = generation,
        "Reconciling KafkaOffsetRollback"
    );

    // Check if we've already processed this generation
    if let Some(status) = &rollback.status {
        if status.observed_generation == Some(generation) {
            match status.phase.as_deref() {
                Some("Completed") | Some("Failed") => {
                    return Ok(Action::await_change());
                }
                Some("Running") => {
                    return offset_rollback_reconciler::monitor_progress(
                        &rollback,
                        &ctx.client,
                        &namespace,
                    )
                    .await;
                }
                _ => {}
            }
        }
    }

    // Validate the spec
    if let Err(e) = offset_rollback_reconciler::validate(&rollback) {
        warn!(error = %e, "Validation failed");
        offset_rollback_reconciler::update_status_failed(
            &rollback,
            &ctx.client,
            &namespace,
            &e.to_string(),
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(300)));
    }

    // Execute rollback operation
    offset_rollback_reconciler::execute(&rollback, &ctx.client, &namespace).await
}

/// Cleanup when resource is being deleted
async fn cleanup(rollback: Arc<KafkaOffsetRollback>, _ctx: Arc<Context>) -> Result<Action> {
    let name = rollback.name_any();
    info!(name = %name, "Cleaning up KafkaOffsetRollback");

    metrics::CLEANUPS.with_label_values(&["KafkaOffsetRollback"]).inc();

    Ok(Action::await_change())
}

/// Error policy for the controller
fn error_policy(obj: Arc<KafkaOffsetRollback>, error: &Error, _ctx: Arc<Context>) -> Action {
    let name = obj.name_any();
    error!(
        name = %name,
        error = %error,
        "Reconciliation failed, scheduling retry"
    );

    let requeue_duration = match error {
        Error::Kube(_) => Duration::from_secs(30),
        Error::Config(_) | Error::Validation(_) => Duration::from_secs(300),
        Error::SnapshotNotFound(_) => Duration::from_secs(60),
        _ => Duration::from_secs(30),
    };

    Action::requeue(requeue_duration)
}
