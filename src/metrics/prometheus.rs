//! Prometheus metrics definitions and HTTP server
//!
//! The operator serves a single `/metrics` endpoint that splices together two
//! exposition bodies:
//!
//! * operator metrics (`kafka_backup_operator_*`), held in the default registry
//!   of the `prometheus` crate; and
//! * kafka-backup-core runtime metrics (`kafka_backup_*`) — lag, throughput,
//!   storage IO and snapshot progress — held in a process-wide
//!   [`PrometheusMetrics`] registry from the `prometheus-client` crate.
//!
//! Backups run in-process here, so there is no separate job pod to scrape and
//! no reason to start core's own metrics server: one endpoint, one Service, one
//! ServiceMonitor. `spec.metrics.enabled` decides whether a given KafkaBackup
//! feeds the shared core registry.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};

use crate::leader::{readiness, LeaderState};

/// Leader-election state mirrored for `/readyz` (0 unknown, 1 follower, 2 leader).
static LEADER_STATE: AtomicU8 = AtomicU8::new(2);

/// Publish this replica's leader-election state for readiness and the gauge.
pub fn set_leader_state(identity: &str, state: LeaderState) {
    let code = match state {
        LeaderState::Unknown => 0,
        LeaderState::Follower => 1,
        LeaderState::Leader => 2,
    };
    LEADER_STATE.store(code, Ordering::Relaxed);
    LEADER
        .with_label_values(&[identity])
        .set(if state == LeaderState::Leader {
            1.0
        } else {
            0.0
        });
}

/// The state last published with [`set_leader_state`]. Starts as `Leader`, the
/// behaviour with leader election disabled.
pub fn leader_state() -> LeaderState {
    match LEADER_STATE.load(Ordering::Relaxed) {
        0 => LeaderState::Unknown,
        1 => LeaderState::Follower,
        _ => LeaderState::Leader,
    }
}

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use kafka_backup_core::metrics::PrometheusMetrics;
use prometheus::{
    register_counter_vec, register_gauge_vec, register_histogram_vec, CounterVec, Encoder,
    GaugeVec, HistogramVec, TextEncoder,
};
use tokio::net::TcpListener;
use tracing::{error, info, warn};

/// Environment variable capping the number of distinct partition label sets the
/// shared core registry will admit, guarding the endpoint against cardinality
/// blow-ups when many topics are backed up.
const MAX_PARTITION_LABELS_ENV: &str = "METRICS_MAX_PARTITION_LABELS";

/// Default cardinality cap, matching kafka-backup-core's own default.
const DEFAULT_MAX_PARTITION_LABELS: usize = 100;

/// Process-wide registry for kafka-backup-core runtime metrics.
///
/// One shared instance rather than one per backup: core's families are keyed by
/// `backup_id`, and two registries would emit duplicate `# HELP`/`# TYPE` lines
/// for the same family, which Prometheus rejects outright.
static CORE_METRICS: OnceLock<Arc<PrometheusMetrics>> = OnceLock::new();

/// Handle to the shared kafka-backup-core metrics registry.
pub fn core_metrics() -> Arc<PrometheusMetrics> {
    CORE_METRICS
        .get_or_init(|| {
            let max_partition_labels = std::env::var(MAX_PARTITION_LABELS_ENV)
                .ok()
                .and_then(|raw| match raw.trim().parse::<usize>() {
                    Ok(parsed) => Some(parsed),
                    Err(_) => {
                        warn!(
                            value = %raw,
                            "Ignoring invalid {MAX_PARTITION_LABELS_ENV}, using default"
                        );
                        None
                    }
                })
                .unwrap_or(DEFAULT_MAX_PARTITION_LABELS);

            info!(
                max_partition_labels,
                "Initialised kafka-backup-core metrics registry"
            );
            Arc::new(PrometheusMetrics::with_max_labels(max_partition_labels))
        })
        .clone()
}

lazy_static::lazy_static! {
    /// Build information, always present so a healthy idle operator never
    /// serves an empty `/metrics` response.
    pub static ref BUILD_INFO: GaugeVec = register_gauge_vec!(
        "kafka_backup_operator_build_info",
        "Build information for the Kafka Backup Operator",
        &["version"]
    ).unwrap();

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
        &["outcome", "resource_namespace", "name"]
    ).unwrap();

    /// Backup size in bytes
    pub static ref BACKUP_SIZE_BYTES: GaugeVec = register_gauge_vec!(
        "kafka_backup_operator_backup_size_bytes",
        "Size of last backup in bytes",
        &["resource_namespace", "name"]
    ).unwrap();

    /// Backup duration histogram
    pub static ref BACKUP_DURATION: HistogramVec = register_histogram_vec!(
        "kafka_backup_operator_backup_duration_seconds",
        "Duration of backup operations",
        &["resource_namespace", "name"],
        vec![1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0]
    ).unwrap();

    /// Records processed in backups
    pub static ref BACKUP_RECORDS: GaugeVec = register_gauge_vec!(
        "kafka_backup_operator_backup_records_total",
        "Records processed in last backup",
        &["resource_namespace", "name"]
    ).unwrap();

    /// Total number of restores
    pub static ref RESTORES_TOTAL: CounterVec = register_counter_vec!(
        "kafka_backup_operator_restores_total",
        "Total number of restores by outcome",
        &["outcome", "resource_namespace", "name"]
    ).unwrap();

    /// Restore duration histogram
    pub static ref RESTORE_DURATION: HistogramVec = register_histogram_vec!(
        "kafka_backup_operator_restore_duration_seconds",
        "Duration of restore operations",
        &["resource_namespace", "name"],
        vec![1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0]
    ).unwrap();

    /// Total number of offset resets
    pub static ref OFFSET_RESETS_TOTAL: CounterVec = register_counter_vec!(
        "kafka_backup_operator_offset_resets_total",
        "Total number of offset resets by outcome",
        &["outcome", "resource_namespace"]
    ).unwrap();

    /// Offset reset duration histogram
    pub static ref OFFSET_RESET_DURATION: HistogramVec = register_histogram_vec!(
        "kafka_backup_operator_offset_reset_duration_seconds",
        "Duration of offset reset operations",
        &["resource_namespace"],
        vec![0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0]
    ).unwrap();

    /// Total number of validations
    pub static ref VALIDATIONS_TOTAL: CounterVec = register_counter_vec!(
        "kafka_backup_operator_validations_total",
        "Total number of validations by outcome",
        &["outcome", "resource_namespace", "name"]
    ).unwrap();

    /// Validation duration histogram
    pub static ref VALIDATION_DURATION: HistogramVec = register_histogram_vec!(
        "kafka_backup_operator_validation_duration_seconds",
        "Duration of validation operations",
        &["resource_namespace", "name"],
        vec![1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0]
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
    pub static ref LEADER: GaugeVec = register_gauge_vec!(
        "kafka_backup_operator_leader",
        "1 when this operator replica holds the leader lease (or leader election is disabled), 0 while it is a standby",
        &["identity"]
    )
    .unwrap();

    pub static ref OPERATOR_HEALTH: prometheus::Gauge = prometheus::register_gauge!(
        "kafka_backup_operator_health",
        "Operator health status (1 = healthy, 0 = unhealthy)"
    ).unwrap();
}

/// Publish metrics that are constant for the lifetime of the process.
///
/// Called before the server starts so the very first scrape already carries
/// build information and health, rather than an empty body.
pub fn init() {
    BUILD_INFO
        .with_label_values(&[env!("CARGO_PKG_VERSION")])
        .set(1.0);
    OPERATOR_HEALTH.set(1.0);
}

/// Start the metrics HTTP server
pub async fn serve(port: u16) -> anyhow::Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;
    info!("Metrics server listening on {}", addr);

    init();

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

/// Strip the OpenMetrics `# EOF` terminator from a core exposition body.
///
/// Core encodes with `prometheus-client`, which terminates its output with
/// `# EOF`. The combined response is served as Prometheus text format 0.0.4,
/// where a stray `# EOF` would merely be an ignored comment — but leaving a
/// terminator mid-body is misleading, and removing it keeps the splice honest.
fn strip_openmetrics_eof(body: &str) -> &str {
    body.strip_suffix("# EOF\n")
        .or_else(|| body.strip_suffix("# EOF"))
        .unwrap_or(body)
}

/// Join operator and core exposition bodies into one text-format response.
///
/// The two halves never share a metric family: operator families are prefixed
/// `kafka_backup_operator_`, core families `kafka_backup_`.
fn combined_exposition(operator: &str, core: &str) -> String {
    let core = strip_openmetrics_eof(core).trim_end_matches('\n');
    if core.is_empty() {
        return operator.to_string();
    }

    let mut body = String::with_capacity(operator.len() + core.len() + 2);
    body.push_str(operator);
    if !body.is_empty() && !body.ends_with('\n') {
        body.push('\n');
    }
    body.push_str(core);
    body.push('\n');
    body
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

    let operator_body = match String::from_utf8(buffer) {
        Ok(body) => body,
        Err(e) => {
            error!("Operator metrics were not valid UTF-8: {}", e);
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Full::new(Bytes::from("Failed to encode metrics")))
                .unwrap();
        }
    };

    let body = combined_exposition(&operator_body, &core_metrics().encode());

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", encoder.format_type())
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

/// Health check response
fn health_response() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::OK)
        .body(Full::new(Bytes::from("ok")))
        .unwrap()
}

/// Readiness check response: `503` until this replica has observed the
/// leader lease, then `standby` / `leader` (see `leader::readiness`).
pub fn ready_response() -> Response<Full<Bytes>> {
    let (status, body) = readiness(leader_state());
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

/// Not found response
fn not_found_response() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Full::new(Bytes::from("Not Found")))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_openmetrics_terminator() {
        assert_eq!(strip_openmetrics_eof("a\n# EOF\n"), "a\n");
        assert_eq!(strip_openmetrics_eof("a\n# EOF"), "a\n");
        assert_eq!(strip_openmetrics_eof("a\n"), "a\n");
    }

    #[test]
    fn combines_both_expositions_without_terminator() {
        let combined = combined_exposition(
            "# TYPE kafka_backup_operator_health gauge\nkafka_backup_operator_health 1\n",
            "# TYPE kafka_backup_records counter\nkafka_backup_records_total{backup_id=\"b\"} 5\n# EOF\n",
        );

        assert!(combined.contains("kafka_backup_operator_health 1"));
        assert!(combined.contains("kafka_backup_records_total{backup_id=\"b\"} 5"));
        assert!(!combined.contains("# EOF"));
        assert!(combined.ends_with('\n'));
    }

    #[test]
    fn empty_core_body_leaves_operator_metrics_untouched() {
        let operator = "kafka_backup_operator_health 1\n";
        assert_eq!(combined_exposition(operator, "# EOF\n"), operator);
        assert_eq!(combined_exposition(operator, ""), operator);
    }

    #[test]
    fn build_info_is_published_by_init() {
        init();

        let families = prometheus::gather();
        let build_info = families
            .iter()
            .find(|f| f.get_name() == "kafka_backup_operator_build_info")
            .expect("build_info registered");

        let metric = &build_info.get_metric()[0];
        assert_eq!(metric.get_gauge().get_value(), 1.0);
        assert_eq!(metric.get_label()[0].get_value(), env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn core_registry_handle_is_shared() {
        assert!(Arc::ptr_eq(&core_metrics(), &core_metrics()));
    }

    #[test]
    fn operator_metrics_avoid_reserved_target_label_names() {
        // A `namespace` label would be rewritten to `exported_namespace` by a
        // ServiceMonitor scrape, hiding these series from the obvious query.
        BACKUPS_TOTAL
            .with_label_values(&["success", "ns", "backup"])
            .inc();

        let families = prometheus::gather();
        for family in &families {
            if !family.get_name().starts_with("kafka_backup_operator_") {
                continue;
            }
            for metric in family.get_metric() {
                for label in metric.get_label() {
                    assert_ne!(
                        label.get_name(),
                        "namespace",
                        "{} must not expose a `namespace` label",
                        family.get_name()
                    );
                }
            }
        }
    }
}
