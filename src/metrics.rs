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

use crate::Result;
use crate::vector::Lifecycle;
use crate::vector::lifecycle::State;

/// Wrapper metrics registered with Prometheus.
pub struct WrapperMetrics {
    pub registry: Registry,
    pub up: IntGauge,
    pub crashes_total: IntCounter,
    pub restarts_total: IntCounter,
    pub config_reloads_total: IntCounterVec,
    pub config_validation_errors_total: IntCounter,
    pub lifecycle_state: GaugeVec,
    pub uptime_seconds: prometheus::Gauge,
}

impl WrapperMetrics {
    /// Create and register all wrapper metrics.
    pub fn new() -> Self {
        let registry = Registry::new();

        let up = IntGauge::new(
            "dfe_transform_vector_up",
            "Whether Vector subprocess is running (1=up, 0=down)",
        )
        .unwrap();

        let crashes_total = IntCounter::new(
            "dfe_transform_vector_crashes_total",
            "Total number of Vector subprocess crashes",
        )
        .unwrap();

        let restarts_total = IntCounter::new(
            "dfe_transform_vector_restarts_total",
            "Total number of Vector subprocess restarts",
        )
        .unwrap();

        let config_reloads_total = IntCounterVec::new(
            opts!(
                "dfe_transform_vector_config_reloads_total",
                "Total config reloads by result"
            ),
            &["result"],
        )
        .unwrap();

        let config_validation_errors_total = IntCounter::new(
            "dfe_transform_vector_config_validation_errors_total",
            "Total config validation errors",
        )
        .unwrap();

        let lifecycle_state = GaugeVec::new(
            opts!(
                "dfe_transform_vector_lifecycle_state",
                "Current lifecycle state (1=active)"
            ),
            &["state"],
        )
        .unwrap();

        let uptime_seconds = prometheus::Gauge::new(
            "dfe_transform_vector_uptime_seconds",
            "Vector subprocess uptime in seconds",
        )
        .unwrap();

        registry.register(Box::new(up.clone())).unwrap();
        registry.register(Box::new(crashes_total.clone())).unwrap();
        registry.register(Box::new(restarts_total.clone())).unwrap();
        registry
            .register(Box::new(config_reloads_total.clone()))
            .unwrap();
        registry
            .register(Box::new(config_validation_errors_total.clone()))
            .unwrap();
        registry
            .register(Box::new(lifecycle_state.clone()))
            .unwrap();
        registry.register(Box::new(uptime_seconds.clone())).unwrap();

        Self {
            registry,
            up,
            crashes_total,
            restarts_total,
            config_reloads_total,
            config_validation_errors_total,
            lifecycle_state,
            uptime_seconds,
        }
    }

    /// Update lifecycle state gauge (set current state to 1, all others to 0).
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
    }
}

impl Default for WrapperMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Start the metrics HTTP server.
///
/// Serves `/metrics` with wrapper metrics in Prometheus text format.
pub async fn serve_metrics(
    address: &str,
    metrics: Arc<WrapperMetrics>,
    lifecycle: Lifecycle,
    started_at: Instant,
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
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = service_fn(move |req| {
                let m = m.clone();
                let lc = lc.clone();
                async move { handle_metrics(req, &m, &lc, start) }
            });
            if let Err(e) = http1::Builder::new().serve_connection(io, svc).await {
                debug!(error = %e, "metrics connection error");
            }
        });
    }
}

/// Handle a metrics request.
fn handle_metrics(
    req: Request<hyper::body::Incoming>,
    metrics: &WrapperMetrics,
    lifecycle: &Lifecycle,
    started_at: Instant,
) -> std::result::Result<Response<Full<Bytes>>, Infallible> {
    if req.uri().path() != "/metrics" {
        return Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(Bytes::from("not found")))
            .unwrap());
    }

    // Update dynamic metrics before encoding
    let state = lifecycle.state();
    metrics.set_lifecycle_state(state);
    metrics.up.set(if state.is_ready() { 1 } else { 0 });
    metrics
        .uptime_seconds
        .set(started_at.elapsed().as_secs_f64());

    // Encode wrapper metrics
    let encoder = TextEncoder::new();
    let metric_families = metrics.registry.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", encoder.format_type())
        .body(Full::new(Bytes::from(buffer)))
        .unwrap())
}
