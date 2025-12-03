//! KafkaBackup controller
//!
//! Watches KafkaBackup resources and triggers reconciliation.

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
use crate::crd::KafkaBackup;
use crate::error::{Error, Result};
use crate::metrics;
use crate::reconcilers::backup as backup_reconciler;

/// Finalizer name for KafkaBackup resources
const FINALIZER_NAME: &str = "kafka.oso.sh/backup-finalizer";

/// Run the KafkaBackup controller
pub async fn run(client: Client, context: Arc<Context>) {
    let api: Api<KafkaBackup> = Api::all(client.clone());

    // Verify CRD is installed
    if let Err(e) = api.list(&ListParams::default().limit(1)).await {
        error!("KafkaBackup CRD not installed: {}", e);
        return;
    }

    info!("Starting KafkaBackup controller");

    Controller::new(api, WatcherConfig::default())
        .shutdown_on_signal()
        .run(reconcile, error_policy, context)
        .for_each(|result| async move {
            match result {
                Ok((obj, _action)) => {
                    info!(
                        name = %obj.name,
                        namespace = obj.namespace.as_deref().unwrap_or("default"),
                        "Reconciled KafkaBackup"
                    );
                }
                Err(e) => {
                    error!(error = %e, "Reconciliation error");
                    metrics::RECONCILIATION_ERRORS.with_label_values(&["KafkaBackup"]).inc();
                }
            }
        })
        .await;
}

/// Main reconciliation function
#[instrument(skip(ctx), fields(name = %obj.name_any(), namespace = obj.namespace()))]
async fn reconcile(obj: Arc<KafkaBackup>, ctx: Arc<Context>) -> Result<Action> {
    let _timer = metrics::RECONCILE_DURATION
        .with_label_values(&["KafkaBackup"])
        .start_timer();
    metrics::RECONCILIATIONS.with_label_values(&["KafkaBackup"]).inc();

    let namespace = obj.namespace().unwrap_or_else(|| "default".to_string());
    let api: Api<KafkaBackup> = Api::namespaced(ctx.client.clone(), &namespace);

    // Use finalizer for proper cleanup handling
    finalizer(&api, FINALIZER_NAME, obj, |event| async {
        match event {
            FinalizerEvent::Apply(backup) => apply(backup, ctx.clone()).await,
            FinalizerEvent::Cleanup(backup) => cleanup(backup, ctx.clone()).await,
        }
    })
    .await
    .map_err(|e| Error::Finalizer(Box::new(e)))
}

/// Apply reconciliation (create/update)
async fn apply(backup: Arc<KafkaBackup>, ctx: Arc<Context>) -> Result<Action> {
    let name = backup.name_any();
    let namespace = backup.namespace().unwrap_or_else(|| "default".to_string());
    let generation = backup.metadata.generation.unwrap_or(0);

    info!(
        name = %name,
        namespace = %namespace,
        generation = generation,
        "Reconciling KafkaBackup"
    );

    // Check if we've already processed this generation
    if let Some(status) = &backup.status {
        if status.observed_generation == Some(generation) {
            // Generation unchanged, check if backup should run based on schedule
            if backup.spec.suspend {
                info!("Backup is suspended, skipping");
                return Ok(Action::requeue(Duration::from_secs(60)));
            }

            // Check schedule for next backup
            return backup_reconciler::check_schedule(&backup, &ctx.client, &namespace).await;
        }
    }

    // Validate the spec
    if let Err(e) = backup_reconciler::validate(&backup) {
        warn!(error = %e, "Validation failed");
        backup_reconciler::update_status_failed(&backup, &ctx.client, &namespace, &e.to_string())
            .await?;
        return Ok(Action::requeue(Duration::from_secs(300)));
    }

    // Update status to Ready and calculate next scheduled backup
    backup_reconciler::update_status_ready(&backup, &ctx.client, &namespace).await?;

    // Check if backup should run now
    backup_reconciler::check_schedule(&backup, &ctx.client, &namespace).await
}

/// Cleanup when resource is being deleted
async fn cleanup(backup: Arc<KafkaBackup>, ctx: Arc<Context>) -> Result<Action> {
    let name = backup.name_any();
    info!(name = %name, "Cleaning up KafkaBackup");

    // Cancel any running backup operations
    // Note: The actual backup data in storage is NOT deleted (retention policy applies)

    metrics::CLEANUPS.with_label_values(&["KafkaBackup"]).inc();

    Ok(Action::await_change())
}

/// Error policy for the controller
fn error_policy(obj: Arc<KafkaBackup>, error: &Error, _ctx: Arc<Context>) -> Action {
    let name = obj.name_any();
    error!(
        name = %name,
        error = %error,
        "Reconciliation failed, scheduling retry"
    );

    // Exponential backoff based on error type
    let requeue_duration = match error {
        Error::Kube(_) => Duration::from_secs(30),
        Error::Config(_) | Error::Validation(_) => Duration::from_secs(300),
        Error::Storage(_) => Duration::from_secs(60),
        _ => Duration::from_secs(30),
    };

    Action::requeue(requeue_duration)
}
