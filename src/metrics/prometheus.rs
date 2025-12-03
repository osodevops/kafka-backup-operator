//! Prometheus metrics definitions and HTTP server

use std::net::SocketAddr;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use prometheus::{
    register_counter_vec, register_gauge_vec, register_histogram_vec, CounterVec, Encoder,
    GaugeVec, HistogramVec, TextEncoder,
};
use tokio::net::TcpListener;
use tracing::{error, info};

lazy_static::lazy_static! {
    /// Total number of reconciliations
    pub static ref RECONCILIATIONS: CounterVec = register_counter_vec!(
        "kafka_backup_operator_reconciliations_total",
        "Total number of reconciliations",
        &["kind"]
    ).unwrap();

    /// Total number of reconciliation errors
    pub static ref RECONCILIATION_ERRORS: CounterVec = register_counter_vec!(
        "kafka_backup_operator_reconciliation_errors_total",
        "Total number of reconciliation errors",
        &["kind"]
    ).unwrap();

    /// Reconciliation duration histogram
    pub static ref RECONCILE_DURATION: HistogramVec = register_histogram_vec!(
        "kafka_backup_operator_reconcile_duration_seconds",
        "Duration of reconciliations in seconds",
        &["kind"],
        vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
    ).unwrap();

    /// Total number of successful backups
    pub static ref BACKUPS_TOTAL: CounterVec = register_counter_vec!(
        "kafka_backup_operator_backups_total",
        "Total number of backups by outcome",
        &["outcome", "namespace", "name"]
    ).unwrap();

    /// Backup size in bytes
    pub static ref BACKUP_SIZE_BYTES: GaugeVec = register_gauge_vec!(
        "kafka_backup_operator_backup_size_bytes",
        "Size of last backup in bytes",
        &["namespace", "name"]
    ).unwrap();

    /// Backup duration histogram
    pub static ref BACKUP_DURATION: HistogramVec = register_histogram_vec!(
        "kafka_backup_operator_backup_duration_seconds",
        "Duration of backup operations",
        &["namespace", "name"],
        vec![1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0]
    ).unwrap();

    /// Records processed in backups
    pub static ref BACKUP_RECORDS: GaugeVec = register_gauge_vec!(
        "kafka_backup_operator_backup_records_total",
        "Records processed in last backup",
        &["namespace", "name"]
    ).unwrap();

    /// Total number of restores
    pub static ref RESTORES_TOTAL: CounterVec = register_counter_vec!(
        "kafka_backup_operator_restores_total",
        "Total number of restores by outcome",
        &["outcome", "namespace", "name"]
    ).unwrap();

    /// Restore duration histogram
    pub static ref RESTORE_DURATION: HistogramVec = register_histogram_vec!(
        "kafka_backup_operator_restore_duration_seconds",
        "Duration of restore operations",
        &["namespace", "name"],
        vec![1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0]
    ).unwrap();

    /// Total number of offset resets
    pub static ref OFFSET_RESETS_TOTAL: CounterVec = register_counter_vec!(
        "kafka_backup_operator_offset_resets_total",
        "Total number of offset resets by outcome",
        &["outcome", "namespace"]
    ).unwrap();

    /// Offset reset duration histogram
    pub static ref OFFSET_RESET_DURATION: HistogramVec = register_histogram_vec!(
        "kafka_backup_operator_offset_reset_duration_seconds",
        "Duration of offset reset operations",
        &["namespace"],
        vec![0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0]
    ).unwrap();

    /// Total number of cleanup operations
    pub static ref CLEANUPS: CounterVec = register_counter_vec!(
        "kafka_backup_operator_cleanups_total",
        "Total number of cleanup operations",
        &["kind"]
    ).unwrap();

    /// Currently managed resources
    pub static ref MANAGED_RESOURCES: GaugeVec = register_gauge_vec!(
        "kafka_backup_operator_managed_resources",
        "Number of managed resources by kind",
        &["kind"]
    ).unwrap();

    /// Operator health (1 = healthy, 0 = unhealthy)
    pub static ref OPERATOR_HEALTH: prometheus::Gauge = prometheus::register_gauge!(
        "kafka_backup_operator_health",
        "Operator health status (1 = healthy, 0 = unhealthy)"
    ).unwrap();
}

/// Start the metrics HTTP server
pub async fn serve(port: u16) -> anyhow::Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;
    info!("Metrics server listening on {}", addr);

    // Set initial health
    OPERATOR_HEALTH.set(1.0);

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);

        tokio::spawn(async move {
            if let Err(e) = http1::Builder::new()
                .serve_connection(io, service_fn(handle_request))
                .await
            {
                error!("Error serving connection: {}", e);
            }
        });
    }
}

/// Handle HTTP requests
async fn handle_request(
    req: Request<hyper::body::Incoming>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let response = match req.uri().path() {
        "/metrics" => metrics_response(),
        "/healthz" | "/health" => health_response(),
        "/readyz" | "/ready" => ready_response(),
        _ => not_found_response(),
    };

    Ok(response)
}

/// Generate metrics response
fn metrics_response() -> Response<Full<Bytes>> {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();

    if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
        error!("Failed to encode metrics: {}", e);
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Full::new(Bytes::from("Failed to encode metrics")))
            .unwrap();
    }

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", encoder.format_type())
        .body(Full::new(Bytes::from(buffer)))
        .unwrap()
}

/// Health check response
fn health_response() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::OK)
        .body(Full::new(Bytes::from("ok")))
        .unwrap()
}

/// Readiness check response
fn ready_response() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::OK)
        .body(Full::new(Bytes::from("ok")))
        .unwrap()
}

/// Not found response
fn not_found_response() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Full::new(Bytes::from("Not Found")))
        .unwrap()
}
