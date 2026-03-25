// Project:   dfe-transform-vector
// File:      src/metrics.rs
// Purpose:   Prometheus metrics endpoint (/metrics)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Prometheus metrics endpoint (`/metrics`).
//!
//! Uses rustlib `HttpServer` (axum) for the HTTP transport with a custom
//! `/metrics` route that renders wrapper metrics AND proxies Vector's
//! internal prometheus_exporter output from `:9598`.
//!
//! Wrapper metrics use `dfe_transform_vector_` prefix for service-specific
//! counters. The `dfe_pipeline_ready` gauge follows the DFE platform
//! standard. Transport-level metrics (`dfe_transport_*`, `dfe_records_*`)
//! come from Vector's internal prometheus_exporter via the metrics proxy.

use std::sync::Arc;
use std::time::Instant;

use hyperi_rustlib::http_server::{
    HttpServer, HttpServerConfig, IntoResponse, Response, Router, State as AxumState, get,
};
use hyperi_rustlib::metrics::MetricsManager;
use hyperi_rustlib::metrics::dfe::DfeMetrics;
use hyperi_rustlib::metrics::dfe_groups::AppMetrics;
use metrics::{Counter, Gauge};
use tracing::{debug, info};

use crate::Result;
use crate::vector::Lifecycle;
use crate::vector::lifecycle::State;

/// Wrapper metrics registered via `MetricsManager`.
///
/// Service-specific metrics use `dfe_transform_vector_` prefix (applied by
/// MetricsManager namespace). The `dfe_pipeline_ready` gauge follows the
/// DFE platform standard via `DfeMetrics`.
pub struct WrapperMetrics {
    pub manager: MetricsManager,
    pub crashes_total: Counter,
    pub restarts_total: Counter,
    pub config_validation_errors_total: Counter,
    pub uptime_seconds: Gauge,
    pub app: AppMetrics,
    pub dfe: DfeMetrics,
}

impl WrapperMetrics {
    /// Create and register all wrapper metrics.
    ///
    /// Installs the global `metrics` recorder via `MetricsManager::new()`.
    /// Must be called once, before any `metrics::counter!` / `metrics::gauge!` usage.
    pub fn new(commit: &str) -> Self {
        let manager = MetricsManager::new("dfe_transform_vector");

        let crashes_total =
            manager.counter("crashes_total", "Total number of Vector subprocess crashes");
        let restarts_total = manager.counter(
            "restarts_total",
            "Total number of Vector subprocess restarts",
        );
        let config_validation_errors_total = manager.counter(
            "config_validation_errors_total",
            "Total config validation errors",
        );
        let uptime_seconds = manager.gauge("uptime_seconds", "Vector subprocess uptime in seconds");

        // Describe the labelled metrics (recorded via macros in set_lifecycle_state / reload)
        metrics::describe_gauge!(
            "dfe_transform_vector_lifecycle_state",
            "Current lifecycle state (1=active)"
        );
        metrics::describe_counter!(
            "dfe_transform_vector_config_reloads_total",
            "Total config reloads by result"
        );

        let app = AppMetrics::new(&manager, env!("CARGO_PKG_VERSION"), commit);
        let dfe = DfeMetrics::register();

        Self {
            manager,
            crashes_total,
            restarts_total,
            config_validation_errors_total,
            uptime_seconds,
            app,
            dfe,
        }
    }

    /// Update lifecycle state gauge (set current state to 1, all others to 0).
    ///
    /// Emits `dfe_pipeline_ready` via `DfeMetrics` and sets the labelled
    /// `dfe_transform_vector_lifecycle_state` gauge.
    pub fn set_lifecycle_state(&self, state: State) {
        let all_states = [
            "initialising",
            "validating",
            "starting",
            "running",
            "reloading",
            "shutting_down",
            "crashed",
        ];
        for s in &all_states {
            let val = if *s == state.as_str() { 1.0 } else { 0.0 };
            metrics::gauge!("dfe_transform_vector_lifecycle_state", "state" => s.to_string())
                .set(val);
        }

        self.dfe.pipeline_ready(state.is_ready());
    }

    /// Record a config reload result.
    pub fn record_config_reload(&self, result: &str) {
        metrics::counter!(
            "dfe_transform_vector_config_reloads_total",
            "result" => result.to_string()
        )
        .increment(1);

        // Also emit via AppMetrics
        self.app.record_config_reload(result == "success");
    }

    /// Record a config validation error.
    pub fn record_config_validation_error(&self) {
        self.config_validation_errors_total.increment(1);
    }

    /// Render all metrics in Prometheus text format.
    pub fn render(&self) -> String {
        self.manager.render()
    }
}

/// Shared state for the metrics axum handler.
#[derive(Clone)]
struct MetricsState {
    metrics: Arc<WrapperMetrics>,
    lifecycle: Lifecycle,
    started_at: Instant,
    vector_metrics_address: String,
}

/// Start the metrics HTTP server.
///
/// Serves `/metrics` with wrapper metrics in Prometheus text format,
/// plus proxied Vector internal metrics from its prometheus_exporter sink.
pub async fn serve_metrics(
    address: &str,
    metrics: Arc<WrapperMetrics>,
    lifecycle: Lifecycle,
    started_at: Instant,
    vector_metrics_address: String,
) -> Result<()> {
    let state = MetricsState {
        metrics,
        lifecycle,
        started_at,
        vector_metrics_address,
    };

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(state);

    let config = HttpServerConfig {
        bind_address: address.to_string(),
        enable_health_endpoints: false,
        enable_metrics_endpoint: false,
        enable_config_endpoint: false,
        ..Default::default()
    };

    info!(address, "metrics server listening");

    let server = HttpServer::new(config);
    server
        .serve(app)
        .await
        .map_err(|e| crate::Error::Config(format!("metrics server error: {e}")))
}

/// Handle GET /metrics — render wrapper metrics + proxy Vector metrics.
async fn metrics_handler(AxumState(state): AxumState<MetricsState>) -> impl IntoResponse {
    // Update dynamic metrics before rendering
    let lifecycle_state = state.lifecycle.state();
    state.metrics.set_lifecycle_state(lifecycle_state);
    state
        .metrics
        .uptime_seconds
        .set(state.started_at.elapsed().as_secs_f64());

    // Render wrapper metrics via MetricsManager
    let mut output = state.metrics.render();

    // Proxy Vector's prometheus_exporter metrics (best-effort)
    if lifecycle_state.is_ready()
        && let Some(vector_metrics) = fetch_vector_metrics(&state.vector_metrics_address).await
    {
        output.push('\n');
        output.push_str(&vector_metrics);
    }

    Response::builder()
        .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
        .body(output)
        .unwrap_or_else(|_| Response::new("internal error".to_string()))
}

/// Fetch metrics from Vector's prometheus_exporter sink (best-effort).
///
/// Returns `None` if Vector isn't running or the fetch fails.
/// Uses a short timeout to avoid blocking the metrics response.
async fn fetch_vector_metrics(address: &str) -> Option<String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let stream = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        TcpStream::connect(address),
    )
    .await
    .ok()?
    .ok()?;

    let request = format!("GET /metrics HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n");

    let mut stream = stream;
    stream.write_all(request.as_bytes()).await.ok()?;

    let mut response = String::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        stream.read_to_string(&mut response),
    )
    .await
    .ok()?
    .ok()?;

    // Extract body from HTTP response
    let body = response.split("\r\n\r\n").nth(1)?;

    // Verify we got a 200 response
    let status_line = response.lines().next()?;
    if !status_line.contains("200") {
        debug!(
            status = status_line,
            "Vector metrics proxy got non-200 response"
        );
        return None;
    }

    Some(body.to_string())
}
