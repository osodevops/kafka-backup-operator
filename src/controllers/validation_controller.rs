//! KafkaBackupValidation controller
//!
//! Watches KafkaBackupValidation resources and triggers reconciliation.

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
use crate::crd::KafkaBackupValidation;
use crate::error::{Error, Result};
use crate::metrics;
use crate::reconcilers::validation as validation_reconciler;

/// Finalizer name for KafkaBackupValidation resources
const FINALIZER_NAME: &str = "kafka.oso.sh/validation-finalizer";

/// Run the KafkaBackupValidation controller
pub async fn run(client: Client, context: Arc<Context>) {
    let api: Api<KafkaBackupValidation> = Api::all(client.clone());

    // Verify CRD is installed
    if let Err(e) = api.list(&ListParams::default().limit(1)).await {
        error!("KafkaBackupValidation CRD not installed: {}", e);
        return;
    }

    info!("Starting KafkaBackupValidation controller");

    Controller::new(api, WatcherConfig::default())
        .shutdown_on_signal()
        .run(reconcile, error_policy, context)
        .for_each(|result| async move {
            match result {
                Ok((obj, _action)) => {
                    info!(
                        name = %obj.name,
                        namespace = obj.namespace.as_deref().unwrap_or("default"),
                        "Reconciled KafkaBackupValidation"
                    );
                }
                Err(e) => {
                    error!(error = %e, "Reconciliation error");
                    metrics::RECONCILIATION_ERRORS
                        .with_label_values(&["KafkaBackupValidation"])
                        .inc();
                }
            }
        })
        .await;
}

/// Main reconciliation function
#[instrument(skip(ctx, obj), fields(name = %obj.name_any(), namespace = obj.namespace()))]
async fn reconcile(obj: Arc<KafkaBackupValidation>, ctx: Arc<Context>) -> Result<Action> {
    let _timer = metrics::RECONCILE_DURATION
        .with_label_values(&["KafkaBackupValidation"])
        .start_timer();
    metrics::RECONCILIATIONS
        .with_label_values(&["KafkaBackupValidation"])
        .inc();

    let namespace = obj.namespace().unwrap_or_else(|| "default".to_string());
    let api: Api<KafkaBackupValidation> = Api::namespaced(ctx.client.clone(), &namespace);

    finalizer(&api, FINALIZER_NAME, obj, |event| async {
        match event {
            FinalizerEvent::Apply(validation) => apply(validation, ctx.clone()).await,
            FinalizerEvent::Cleanup(validation) => cleanup(validation, ctx.clone()).await,
        }
    })
    .await
    .map_err(|e| Error::Finalizer(Box::new(e)))
}

/// Apply reconciliation (create/update)
async fn apply(validation: Arc<KafkaBackupValidation>, ctx: Arc<Context>) -> Result<Action> {
    let name = validation.name_any();
    let namespace = validation
        .namespace()
        .unwrap_or_else(|| "default".to_string());
    let generation = validation.metadata.generation.unwrap_or(0);

    info!(
        name = %name,
        namespace = %namespace,
        generation = generation,
        "Reconciling KafkaBackupValidation"
    );

    // Check if we've already processed this generation
    if let Some(status) = &validation.status {
        if status.observed_generation == Some(generation) {
            if validation.spec.suspend {
                info!("Validation is suspended, skipping");
                return Ok(Action::requeue(Duration::from_secs(60)));
            }

            // Check if completed (one-shot) or needs scheduling
            if let Some(phase) = &status.phase {
                if phase == "Completed" || phase == "Failed" {
                    return validation_reconciler::check_schedule(
                        &validation,
                        &ctx.client,
                        &namespace,
                    )
                    .await;
                }
            }

            return Ok(Action::requeue(Duration::from_secs(30)));
        }
    }

    // Validate the spec
    if let Err(e) = validation_reconciler::validate(&validation) {
        warn!(error = %e, "Validation spec check failed");
        validation_reconciler::update_status_failed(
            &validation,
            &ctx.client,
            &namespace,
            &e.to_string(),
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(300)));
    }

    // Execute validation
    validation_reconciler::execute(&validation, &ctx.client, &namespace).await
}

/// Cleanup when resource is being deleted
async fn cleanup(validation: Arc<KafkaBackupValidation>, _ctx: Arc<Context>) -> Result<Action> {
    let name = validation.name_any();
    info!(name = %name, "Cleaning up KafkaBackupValidation");

    metrics::CLEANUPS
        .with_label_values(&["KafkaBackupValidation"])
        .inc();

    Ok(Action::await_change())
}

/// Error policy for the controller
fn error_policy(obj: Arc<KafkaBackupValidation>, error: &Error, _ctx: Arc<Context>) -> Action {
    let name = obj.name_any();
    error!(
        name = %name,
        error = %error,
        "Reconciliation failed, scheduling retry"
    );

    let requeue_duration = match error {
        Error::Kube(_) => Duration::from_secs(30),
        Error::Config(_) | Error::Validation(_) => Duration::from_secs(300),
        Error::Storage(_) => Duration::from_secs(60),
        Error::Evidence(_) => Duration::from_secs(60),
        Error::Notification(_) => Duration::from_secs(30),
        _ => Duration::from_secs(30),
    };

    Action::requeue(requeue_duration)
}
