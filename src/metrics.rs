// Project:   dfe-transform-vector
// File:      src/metrics.rs
// Purpose:   Wrapper-specific Prometheus metrics (registered on the runtime's MetricsManager)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Wrapper-specific Prometheus metrics.
//!
//! All metrics are registered on the `MetricsManager` owned by
//! `hyperi_rustlib::cli::ServiceRuntime`. The runtime serves `/metrics`,
//! `/healthz`, and `/readyz` on `args.metrics_addr` — this module no
//! longer runs its own HTTP server, eliminating the previous double-bind
//! against the same port.
//!
//! Vector's own metrics (transport, records, internal counters) are
//! exposed by Vector's `prometheus_exporter` sink on
//! `config.metrics.vector_metrics_address` (default `127.0.0.1:9598`).
//! Prometheus scrapes that endpoint directly as a separate target — the
//! wrapper does NOT proxy-merge it any more. The chart's `extraPorts`
//! plus a PodMonitor selector handles the second target.

use std::time::{Duration, Instant};

use hyperi_rustlib::metrics::MetricsManager;
use hyperi_rustlib::metrics::dfe::DfeMetrics;
use hyperi_rustlib::metrics::dfe_groups::AppMetrics;
use metrics::{Counter, Gauge};
use tokio::sync::watch;
use tracing::debug;

use crate::vector::Lifecycle;
use crate::vector::lifecycle::State;

/// Wrapper-specific metrics, registered on a shared `MetricsManager`.
///
/// Service-specific metrics use the `dfe_transform_vector_` prefix
/// (applied by the manager's namespace). The `dfe_pipeline_ready` gauge
/// follows the DFE platform standard via `DfeMetrics`.
///
/// This struct does NOT own a `MetricsManager` — it borrows the one
/// created by `hyperi_rustlib::cli::ServiceRuntime` and registers its
/// counters/gauges against it. The runtime owns the `/metrics` HTTP
/// server; this module only feeds metric values into it.
pub struct WrapperMetrics {
    pub crashes_total: Counter,
    pub restarts_total: Counter,
    pub config_validation_errors_total: Counter,
    pub uptime_seconds: Gauge,
    pub app: AppMetrics,
    pub dfe: DfeMetrics,
}

impl WrapperMetrics {
    /// Register wrapper metrics against the runtime's `MetricsManager`.
    ///
    /// The runtime has already installed the global `metrics` recorder
    /// and registered platform metrics (`DfeMetrics`, `AppMetrics`).
    /// This call adds wrapper-specific counters/gauges on the SAME
    /// manager so all metrics appear under a single `/metrics` endpoint
    /// served by the runtime.
    pub fn register(manager: &MetricsManager, commit: &str) -> Self {
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

        // Describe the labelled metrics (recorded via macros in
        // set_lifecycle_state / record_config_reload).
        metrics::describe_gauge!(
            "dfe_transform_vector_lifecycle_state",
            "Current lifecycle state (1=active)"
        );
        metrics::describe_counter!(
            "dfe_transform_vector_config_reloads_total",
            "Total config reloads by result"
        );

        let app = AppMetrics::new(manager, env!("CARGO_PKG_VERSION"), commit);
        let dfe = DfeMetrics::register(manager);

        Self {
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
}

/// Drive the lifecycle-state gauge from lifecycle transitions.
///
/// Subscribes to the lifecycle watch channel and pushes a gauge update
/// every time the state changes. Replaces the previous per-scrape update
/// that ran inside the (now-deleted) metrics HTTP handler.
///
/// Spawns a background task and returns immediately. The task exits when
/// the lifecycle drops all senders.
pub fn spawn_lifecycle_gauge_task(
    metrics: std::sync::Arc<WrapperMetrics>,
    lifecycle: &Lifecycle,
) -> tokio::task::JoinHandle<()> {
    let mut rx: watch::Receiver<State> = lifecycle.subscribe();
    tokio::spawn(async move {
        // Emit the initial state immediately
        metrics.set_lifecycle_state(*rx.borrow());
        while rx.changed().await.is_ok() {
            let state = *rx.borrow();
            debug!(?state, "lifecycle state changed");
            metrics.set_lifecycle_state(state);
        }
    })
}

/// Drive the uptime gauge from a periodic tick.
///
/// The runtime's `/metrics` endpoint exposes the gauge value at scrape
/// time. We tick every `interval` to keep the value fresh between
/// scrapes (Prometheus default scrape is 15-30s, ticking at 5s is fine).
pub fn spawn_uptime_tick_task(
    metrics: std::sync::Arc<WrapperMetrics>,
    started_at: Instant,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            metrics
                .uptime_seconds
                .set(started_at.elapsed().as_secs_f64());
        }
    })
}
