//! OSO Kafka Backup Kubernetes Operator
//!
//! Main entry point for the operator. Sets up the Kubernetes client, elects a
//! leader (only the lease holder runs the controllers), registers the CRD
//! controllers, and runs the reconciliation loops.

use std::sync::Arc;

use futures::future::join5;
use kube::Client;
use tokio::sync::watch;
use tokio::task::JoinError;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use kafka_backup_operator::leader::{
    self, LeaderElectionConfig, LeaderElector, LeaderError, LeaderState,
};
use kafka_backup_operator::{
    controllers::{self, Context},
    metrics, shutdown,
};

/// Default metrics port
const METRICS_PORT: u16 = 8080;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Logs go through a lossy non-blocking writer: with a CPU limit the tokio
    // runtime may have a single worker, and a blocking write to a backed-up
    // container stdout would freeze timers, probes, lease renewals and the
    // running backup alike. Dropping log lines under back-pressure is the
    // lesser evil.
    let (stdout, _log_guard) = tracing_appender::non_blocking(std::io::stdout());
    init_tracing(stdout);

    info!("Starting OSO Kafka Backup Operator");

    // Create Kubernetes client
    let client = Client::try_default().await?;
    info!("Connected to Kubernetes API server");

    let shutdown = shutdown::listen();

    let namespace = std::env::var(leader::OPERATOR_NAMESPACE_ENV)
        .ok()
        .map(|ns| ns.trim().to_string())
        .filter(|ns| !ns.is_empty())
        .unwrap_or_else(|| client.default_namespace().to_string());
    let leader_config = LeaderElectionConfig::from_env(&namespace)?;
    let identity = leader::identity();

    // Until this replica has observed the lease it is neither ready nor
    // allowed to reconcile. Without leader election it leads from the start.
    let initial_state = if leader_config.is_some() {
        LeaderState::Unknown
    } else {
        LeaderState::Leader
    };
    let (state_tx, state_rx) = watch::channel(initial_state);
    metrics::set_leader_state(&identity, initial_state);

    // Mirror the state into /readyz and the leader gauge.
    let state_task = {
        let mut rx = state_rx.clone();
        let identity = identity.clone();
        tokio::spawn(async move {
            while rx.changed().await.is_ok() {
                metrics::set_leader_state(&identity, *rx.borrow_and_update());
            }
        })
    };

    // Start metrics server
    let metrics_handle = tokio::spawn(metrics::serve(METRICS_PORT));
    info!("Metrics server starting on port {}", METRICS_PORT);

    // Create shared context
    let context = Arc::new(Context::new(client.clone()));

    match leader_config {
        None => {
            info!("Leader election disabled; starting controllers");
            run_controllers(&client, &context, &shutdown).await;
        }
        Some(config) => {
            info!(
                %identity,
                lease = %config.lease_name,
                namespace = %config.namespace,
                lease_duration = ?config.lease_duration,
                renew_deadline = ?config.renew_deadline,
                retry_period = ?config.retry_period,
                "Leader election enabled; waiting for the leader lease"
            );
            let (release_tx, release_rx) = watch::channel(false);
            let elector = LeaderElector::new(client.clone(), config, identity, state_tx);
            let mut elector_task = tokio::spawn(elector.run(release_rx));

            // Standby: block until we lead, we are told to stop, or the elector dies.
            let mut wait_rx = state_rx.clone();
            tokio::select! {
                waited = wait_rx.wait_for(|s| *s == LeaderState::Leader) => {
                    // Err means the elector dropped its sender, i.e. it exited
                    // before we ever led: never start the controllers then.
                    if waited.is_err() {
                        fatal_exit(elector_task.await);
                    }
                }
                _ = shutdown.clone() => {
                    info!("Shutdown requested while standing by");
                    let _ = release_tx.send(true);
                    let _ = elector_task.await;
                    return Ok(());
                }
                outcome = &mut elector_task => fatal_exit(outcome),
            }
            info!("Acquired leadership; starting controllers");

            tokio::select! {
                _ = run_controllers(&client, &context, &shutdown) => {
                    // Shutdown path: the controllers have drained — a running
                    // backup was asked to stop and has finalized. Only now hand
                    // the lease over, so our successor cannot start the same
                    // schedule while we may still be writing (issue #79).
                    info!("Controllers drained; releasing the leader lease");
                    let _ = release_tx.send(true);
                    let _ = elector_task.await;
                }
                outcome = &mut elector_task => fatal_exit(outcome),
            }
        }
    }

    drop(state_rx);
    state_task.abort();
    metrics_handle.abort();
    info!("OSO Kafka Backup Operator stopped");
    // Everything orderly is done: the controllers have drained, the engine has
    // finalized and the lease is released. A stopped engine's abandoned
    // partition task can still be compressing on the runtime worker, and
    // waiting for it (runtime teardown, or even libc `exit` handlers) used to
    // exhaust the termination grace period and get the pod SIGKILLed. Flush
    // the logs and leave immediately.
    drop(_log_guard);
    // SAFETY: `_exit` terminates the process without running destructors or
    // exit handlers; nothing below needs them and all durable work is done.
    unsafe { libc::_exit(0) }
}

/// Run all controllers until the shared shutdown future resolves and every
/// in-flight reconciliation has finished.
async fn run_controllers(client: &Client, context: &Arc<Context>, shutdown: &shutdown::Shutdown) {
    info!("Controllers started");
    join5(
        controllers::run_backup_controller(client.clone(), context.clone(), shutdown.clone()),
        controllers::run_restore_controller(client.clone(), context.clone(), shutdown.clone()),
        controllers::run_offset_reset_controller(client.clone(), context.clone(), shutdown.clone()),
        controllers::run_offset_rollback_controller(
            client.clone(),
            context.clone(),
            shutdown.clone(),
        ),
        controllers::run_validation_controller(client.clone(), context.clone(), shutdown.clone()),
    )
    .await;
    info!("Controllers stopped");
}

/// The elector stopped on its own: leadership was lost or the lease could not
/// be renewed in time. A running backup cannot be handed over safely, so the
/// only safe reaction is to exit and let the kubelet restart the process,
/// which then rejoins the election as a candidate.
fn fatal_exit(outcome: Result<Result<(), LeaderError>, JoinError>) -> ! {
    match outcome {
        Ok(Ok(())) => error!("Leader elector stopped unexpectedly; exiting"),
        Ok(Err(e)) => {
            error!(error = %e, "Leader election failed; exiting so the pod restarts as a candidate")
        }
        Err(e) => error!(error = %e, "Leader elector task panicked; exiting"),
    }
    std::process::exit(1);
}

/// Initialize tracing subscriber
fn init_tracing(writer: tracing_appender::non_blocking::NonBlocking) {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,kube=warn,hyper=warn"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer().json().with_writer(writer))
        .init();
}
