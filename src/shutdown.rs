//! Process-wide shutdown signal.
//!
//! The operator runs several long-lived tasks (two controllers, the
//! health/metrics server and, when enabled, the leader elector). Each of them
//! used to install its own SIGTERM/SIGINT handler, so the order in which they
//! noticed a shutdown was undefined. A single shared future gives every task
//! the same notification and lets `main` sequence the teardown: drain the
//! controllers first, then release the leader lease (issue #62).

use std::sync::OnceLock;

use futures::future::{BoxFuture, FutureExt, Shared};
use tracing::info;

static CURRENT: OnceLock<Shutdown> = OnceLock::new();

/// Resolves once, on the first SIGTERM or SIGINT. Clone freely; every clone
/// resolves.
pub type Shutdown = Shared<BoxFuture<'static, ()>>;

/// Install the signal handlers and return the shared shutdown future. Also
/// records it as the process-wide handle for [`current`].
pub fn listen() -> Shutdown {
    let shutdown = wait_for_signal().boxed().shared();
    let _ = CURRENT.set(shutdown.clone());
    shutdown
}

/// The process-wide shutdown future, if [`listen`] has been called. Used by
/// long-running reconciles (the in-process backup engine) to stop gracefully
/// when the operator is asked to exit.
pub fn current() -> Option<Shutdown> {
    CURRENT.get().cloned()
}

async fn wait_for_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("Received Ctrl+C, shutting down"),
        _ = terminate => info!("Received SIGTERM, shutting down"),
    }
}
