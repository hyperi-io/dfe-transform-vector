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
//! Wrapper metrics use `dfe_transform_vector_` prefix for service-specific
//! counters. The `dfe_pipeline_ready` gauge follows the DFE platform
//! standard for cross-service dashboards. Transport-level metrics
//! (`dfe_transport_*`, `dfe_records_*`) come from Vector's internal
//! prometheus_exporter via the metrics proxy — the wrapper doesn't
//! handle Kafka I/O directly.

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
use prometheus::{
    Encoder, GaugeVec, IntCounter, IntCounterVec, IntGauge, Registry, TextEncoder, opts,
};
use tokio::net::TcpListener;
use tracing::{debug, error, info};

use hyperi_rustlib::metrics::DfeMetrics;

use crate::Result;
use crate::vector::Lifecycle;
use crate::vector::lifecycle::State;

/// Wrapper metrics registered with Prometheus.
///
/// Service-specific metrics use `dfe_transform_vector_` prefix.
/// The `dfe_pipeline_ready` gauge follows the DFE platform standard.
pub struct WrapperMetrics {
    pub registry: Registry,
    pub pipeline_ready: IntGauge,
    pub crashes_total: IntCounter,
    pub restarts_total: IntCounter,
    pub config_reloads_total: IntCounterVec,
    pub config_validation_errors_total: IntCounter,
    pub lifecycle_state: GaugeVec,
    pub uptime_seconds: prometheus::Gauge,
    /// Standard DFE metrics (dual-emit alongside service-specific metrics).
    pub dfe: Option<DfeMetrics>,
}

impl WrapperMetrics {
    /// Create and register all wrapper metrics.
    #[allow(clippy::unwrap_used, clippy::expect_used)]
    pub fn new() -> Self {
        let registry = Registry::new();

        let pipeline_ready = IntGauge::new(
            "dfe_pipeline_ready",
            "Pipeline readiness (1=ready, 0=backpressured/stalled)",
        )
        .expect("dfe_pipeline_ready metric");

        let crashes_total = IntCounter::new(
            "dfe_transform_vector_crashes_total",
            "Total number of Vector subprocess crashes",
        )
        .expect("crashes_total metric");

        let restarts_total = IntCounter::new(
            "dfe_transform_vector_restarts_total",
            "Total number of Vector subprocess restarts",
        )
        .expect("restarts_total metric");

        let config_reloads_total = IntCounterVec::new(
            opts!(
                "dfe_transform_vector_config_reloads_total",
                "Total config reloads by result"
            ),
            &["result"],
        )
        .expect("config_reloads_total metric");

        let config_validation_errors_total = IntCounter::new(
            "dfe_transform_vector_config_validation_errors_total",
            "Total config validation errors",
        )
        .expect("config_validation_errors_total metric");

        let lifecycle_state = GaugeVec::new(
            opts!(
                "dfe_transform_vector_lifecycle_state",
                "Current lifecycle state (1=active)"
            ),
            &["state"],
        )
        .expect("lifecycle_state metric");

        let uptime_seconds = prometheus::Gauge::new(
            "dfe_transform_vector_uptime_seconds",
            "Vector subprocess uptime in seconds",
        )
        .expect("uptime_seconds metric");

        registry
            .register(Box::new(pipeline_ready.clone()))
            .expect("register pipeline_ready");
        registry
            .register(Box::new(crashes_total.clone()))
            .expect("register crashes_total");
        registry
            .register(Box::new(restarts_total.clone()))
            .expect("register restarts_total");
        registry
            .register(Box::new(config_reloads_total.clone()))
            .expect("register config_reloads_total");
        registry
            .register(Box::new(config_validation_errors_total.clone()))
            .expect("register config_validation_errors_total");
        registry
            .register(Box::new(lifecycle_state.clone()))
            .expect("register lifecycle_state");
        registry
            .register(Box::new(uptime_seconds.clone()))
            .expect("register uptime_seconds");

        Self {
            registry,
            pipeline_ready,
            crashes_total,
            restarts_total,
            config_reloads_total,
            config_validation_errors_total,
            lifecycle_state,
            uptime_seconds,
            dfe: None,
        }
    }

    /// Create metrics with DfeMetrics dual-emit enabled.
    ///
    /// Calls `DfeMetrics::register()` to describe all `dfe_*` metric names
    /// with the global recorder. Must be called **after** `MetricsManager::new()`
    /// installs the Prometheus recorder.
    pub fn with_dfe_metrics(mut self) -> Self {
        self.dfe = Some(DfeMetrics::register());
        self
    }

    /// Update lifecycle state gauge (set current state to 1, all others to 0).
    ///
    /// Dual-emits `dfe_pipeline_ready` via `DfeMetrics` when registered.
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
            self.lifecycle_state.with_label_values(&[s]).set(val);
        }

        // Dual-emit via DfeMetrics
        if let Some(ref dfe) = self.dfe {
            dfe.pipeline_ready(state.is_ready());
        }
    }
}

impl Default for WrapperMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Start the metrics HTTP server.
///
/// Serves `/metrics` with wrapper metrics in Prometheus text format,
/// plus proxied Vector internal metrics from its prometheus_exporter sink.
// TODO: migrate to hyperi-rustlib `http-server` feature
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

    // Update dynamic metrics before encoding
    let state = lifecycle.state();
    metrics.set_lifecycle_state(state);
    metrics.pipeline_ready.set(i64::from(state.is_ready()));
    metrics
        .uptime_seconds
        .set(started_at.elapsed().as_secs_f64());

    // Encode wrapper metrics
    let encoder = TextEncoder::new();
    let metric_families = metrics.registry.gather();
    let mut buffer = Vec::new();
    #[allow(clippy::unwrap_used)]
    encoder.encode(&metric_families, &mut buffer).unwrap();

    // Proxy Vector's prometheus_exporter metrics (best-effort)
    if state.is_ready()
        && let Some(vector_metrics) = fetch_vector_metrics(vector_metrics_address).await
    {
        buffer.push(b'\n');
        buffer.extend_from_slice(vector_metrics.as_bytes());
    }

    Ok(prometheus_response(&buffer, encoder.format_type()))
}

/// Build a Prometheus text response.
fn prometheus_response(body: &[u8], content_type: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", content_type)
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
