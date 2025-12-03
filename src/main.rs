//! OSO Kafka Backup Kubernetes Operator
//!
//! Main entry point for the operator. Sets up the Kubernetes client,
//! registers CRD controllers, and runs the reconciliation loops.

use std::sync::Arc;

use kube::Client;
use tokio::signal;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use kafka_backup_operator::{
    controllers::{self, Context},
    metrics,
};

/// Default metrics port
const METRICS_PORT: u16 = 8080;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    init_tracing();

    info!("Starting OSO Kafka Backup Operator");

    // Create Kubernetes client
    let client = Client::try_default().await?;
    info!("Connected to Kubernetes API server");

    // Create shared context
    let context = Arc::new(Context::new(client.clone()));

    // Start metrics server
    let metrics_handle = tokio::spawn(metrics::serve(METRICS_PORT));
    info!("Metrics server starting on port {}", METRICS_PORT);

    // Run all controllers concurrently
    let backup_controller = controllers::run_backup_controller(client.clone(), context.clone());
    let restore_controller = controllers::run_restore_controller(client.clone(), context.clone());
    let offset_reset_controller =
        controllers::run_offset_reset_controller(client.clone(), context.clone());
    let offset_rollback_controller =
        controllers::run_offset_rollback_controller(client.clone(), context.clone());

    // Handle graceful shutdown
    tokio::select! {
        _ = backup_controller => {
            error!("Backup controller exited unexpectedly");
        }
        _ = restore_controller => {
            error!("Restore controller exited unexpectedly");
        }
        _ = offset_reset_controller => {
            error!("Offset reset controller exited unexpectedly");
        }
        _ = offset_rollback_controller => {
            error!("Offset rollback controller exited unexpectedly");
        }
        _ = metrics_handle => {
            error!("Metrics server exited unexpectedly");
        }
        _ = shutdown_signal() => {
            info!("Received shutdown signal, stopping operator");
        }
    }

    info!("OSO Kafka Backup Operator stopped");
    Ok(())
}

/// Initialize tracing subscriber
fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,kube=warn,hyper=warn"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer().json())
        .init();
}

/// Wait for shutdown signal (SIGTERM or SIGINT)
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install CTRL+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("Received CTRL+C signal");
        }
        _ = terminate => {
            info!("Received SIGTERM signal");
        }
    }
}
