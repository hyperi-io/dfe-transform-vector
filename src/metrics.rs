// Project:   dfe-transform-vector
// File:      src/metrics.rs
// Purpose:   Prometheus metrics endpoint (/metrics)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Prometheus metrics endpoint (`/metrics`).
//!
//! HTTP server exposing wrapper metrics and (when available) proxied
//! Vector internal metrics from the prometheus_exporter sink on :9598.
//!
//! Uses `MetricsManager` from hyperi-rustlib for the Prometheus recorder
//! and `metrics` crate macros for recording. Service-specific metrics use
//! `dfe_transform_vector_` prefix. Transport-level metrics come from
//! Vector's internal prometheus_exporter via the metrics proxy.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use metrics::{Counter, Gauge};
use tokio::net::TcpListener;
use tracing::{debug, error, info};

use hyperi_rustlib::metrics::MetricsManager;
use hyperi_rustlib::metrics::dfe::DfeMetrics;
use hyperi_rustlib::metrics::dfe_groups::AppMetrics;

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
    let addr: SocketAddr = address
        .parse()
        .map_err(|e| crate::Error::Config(format!("invalid metrics address '{address}': {e}")))?;

    let listener = TcpListener::bind(addr).await.map_err(|e| {
        crate::Error::Config(format!("failed to bind metrics server on {addr}: {e}"))
    })?;

    info!(address = %addr, "metrics server listening");

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                error!(error = %e, "metrics server accept error");
                continue;
            }
        };

        let m = metrics.clone();
        let lc = lifecycle.clone();
        let start = started_at;
        let vec_addr = vector_metrics_address.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = service_fn(move |req| {
                let m = m.clone();
                let lc = lc.clone();
                let vec_addr = vec_addr.clone();
                async move { handle_metrics(req, &m, &lc, start, &vec_addr).await }
            });
            if let Err(e) = http1::Builder::new().serve_connection(io, svc).await {
                debug!(error = %e, "metrics connection error");
            }
        });
    }
}

/// Handle a metrics request.
async fn handle_metrics(
    req: Request<hyper::body::Incoming>,
    metrics: &WrapperMetrics,
    lifecycle: &Lifecycle,
    started_at: Instant,
    vector_metrics_address: &str,
) -> std::result::Result<Response<Full<Bytes>>, Infallible> {
    if req.uri().path() != "/metrics" {
        return Ok(not_found_response());
    }

    // Update dynamic metrics before rendering
    let state = lifecycle.state();
    metrics.set_lifecycle_state(state);
    metrics
        .uptime_seconds
        .set(started_at.elapsed().as_secs_f64());

    // Render wrapper metrics via MetricsManager
    let mut output = metrics.render();

    // Proxy Vector's prometheus_exporter metrics (best-effort)
    if state.is_ready()
        && let Some(vector_metrics) = fetch_vector_metrics(vector_metrics_address).await
    {
        output.push('\n');
        output.push_str(&vector_metrics);
    }

    Ok(prometheus_response(output.as_bytes()))
}

/// Build a Prometheus text response.
fn prometheus_response(body: &[u8]) -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
        .body(Full::new(Bytes::from(body.to_vec())))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::from("internal error"))))
}

/// Build a 404 response.
fn not_found_response() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Full::new(Bytes::from("not found")))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::from("not found"))))
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
