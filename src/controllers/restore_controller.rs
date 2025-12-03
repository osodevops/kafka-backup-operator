//! KafkaRestore controller
//!
//! Watches KafkaRestore resources and triggers reconciliation.

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
use crate::crd::KafkaRestore;
use crate::error::{Error, Result};
use crate::metrics;
use crate::reconcilers::restore as restore_reconciler;

/// Finalizer name for KafkaRestore resources
const FINALIZER_NAME: &str = "kafka.oso.sh/restore-finalizer";

/// Run the KafkaRestore controller
pub async fn run(client: Client, context: Arc<Context>) {
    let api: Api<KafkaRestore> = Api::all(client.clone());

    // Verify CRD is installed
    if let Err(e) = api.list(&ListParams::default().limit(1)).await {
        error!("KafkaRestore CRD not installed: {}", e);
        return;
    }

    info!("Starting KafkaRestore controller");

    Controller::new(api, WatcherConfig::default())
        .shutdown_on_signal()
        .run(reconcile, error_policy, context)
        .for_each(|result| async move {
            match result {
                Ok((obj, _action)) => {
                    info!(
                        name = %obj.name,
                        namespace = obj.namespace.as_deref().unwrap_or("default"),
                        "Reconciled KafkaRestore"
                    );
                }
                Err(e) => {
                    error!(error = %e, "Reconciliation error");
                    metrics::RECONCILIATION_ERRORS.with_label_values(&["KafkaRestore"]).inc();
                }
            }
        })
        .await;
}

/// Main reconciliation function
#[instrument(skip(ctx), fields(name = %obj.name_any(), namespace = obj.namespace()))]
async fn reconcile(obj: Arc<KafkaRestore>, ctx: Arc<Context>) -> Result<Action> {
    let _timer = metrics::RECONCILE_DURATION
        .with_label_values(&["KafkaRestore"])
        .start_timer();
    metrics::RECONCILIATIONS.with_label_values(&["KafkaRestore"]).inc();

    let namespace = obj.namespace().unwrap_or_else(|| "default".to_string());
    let api: Api<KafkaRestore> = Api::namespaced(ctx.client.clone(), &namespace);

    finalizer(&api, FINALIZER_NAME, obj, |event| async {
        match event {
            FinalizerEvent::Apply(restore) => apply(restore, ctx.clone()).await,
            FinalizerEvent::Cleanup(restore) => cleanup(restore, ctx.clone()).await,
        }
    })
    .await
    .map_err(|e| Error::Finalizer(Box::new(e)))
}

/// Apply reconciliation (create/update)
async fn apply(restore: Arc<KafkaRestore>, ctx: Arc<Context>) -> Result<Action> {
    let name = restore.name_any();
    let namespace = restore.namespace().unwrap_or_else(|| "default".to_string());
    let generation = restore.metadata.generation.unwrap_or(0);

    info!(
        name = %name,
        namespace = %namespace,
        generation = generation,
        "Reconciling KafkaRestore"
    );

    // Check if we've already processed this generation
    if let Some(status) = &restore.status {
        if status.observed_generation == Some(generation) {
            // Check current phase
            match status.phase.as_deref() {
                Some("Completed") | Some("Failed") | Some("RolledBack") => {
                    // Terminal states - no action needed
                    return Ok(Action::await_change());
                }
                Some("Running") => {
                    // Monitor progress
                    return restore_reconciler::monitor_progress(&restore, &ctx.client, &namespace)
                        .await;
                }
                _ => {}
            }
        }
    }

    // Validate the spec
    if let Err(e) = restore_reconciler::validate(&restore) {
        warn!(error = %e, "Validation failed");
        restore_reconciler::update_status_failed(&restore, &ctx.client, &namespace, &e.to_string())
            .await?;
        return Ok(Action::requeue(Duration::from_secs(300)));
    }

    // Execute restore operation
    restore_reconciler::execute(&restore, &ctx.client, &namespace).await
}

/// Cleanup when resource is being deleted
async fn cleanup(restore: Arc<KafkaRestore>, _ctx: Arc<Context>) -> Result<Action> {
    let name = restore.name_any();
    info!(name = %name, "Cleaning up KafkaRestore");

    // Cancel any running restore operations
    // Clean up rollback snapshots if no longer needed

    metrics::CLEANUPS.with_label_values(&["KafkaRestore"]).inc();

    Ok(Action::await_change())
}

/// Error policy for the controller
fn error_policy(obj: Arc<KafkaRestore>, error: &Error, _ctx: Arc<Context>) -> Action {
    let name = obj.name_any();
    error!(
        name = %name,
        error = %error,
        "Reconciliation failed, scheduling retry"
    );

    let requeue_duration = match error {
        Error::Kube(_) => Duration::from_secs(30),
        Error::Config(_) | Error::Validation(_) => Duration::from_secs(300),
        Error::Storage(_) | Error::BackupNotFound(_) => Duration::from_secs(60),
        Error::Rollback(_) => Duration::from_secs(30),
        _ => Duration::from_secs(30),
    };

    Action::requeue(requeue_duration)
}
