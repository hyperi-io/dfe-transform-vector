// Project:   dfe-transform-vector
// File:      src/metrics.rs
// Purpose:   Wrapper-specific Prometheus metrics (registered on the runtime's MetricsManager)
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Wrapper-specific Prometheus metrics.
//!
//! All metrics are registered on the `MetricsManager` owned by
//! `scalo::cli::ServiceRuntime`. The runtime serves `/metrics`,
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

use metrics::{Counter, Gauge};
use scalo::metrics::MetricsManager;
use scalo::metrics::groups::AppMetrics;
use scalo::metrics::service::ServiceMetrics;
use tokio::sync::watch;
use tracing::debug;

use crate::vector::Lifecycle;
use crate::vector::lifecycle::State;

/// Wrapper-specific metrics, registered on a shared `MetricsManager`.
///
/// Metric names are emitted BARE here; the manager's namespace prepends
/// the app prefix once (so `transform_vector_lifecycle_state` is exported
/// as `<namespace>_transform_vector_lifecycle_state`). The
/// `pipeline_ready` gauge follows the DFE platform standard via
/// `ServiceMetrics`.
///
/// This struct does NOT own a `MetricsManager` — it borrows the one
/// created by `scalo::cli::ServiceRuntime` and registers its
/// counters/gauges against it. The runtime owns the `/metrics` HTTP
/// server; this module only feeds metric values into it.
pub struct WrapperMetrics {
    pub crashes_total: Counter,
    pub restarts_total: Counter,
    pub config_validation_errors_total: Counter,
    pub uptime_seconds: Gauge,
    pub app: AppMetrics,
    pub service: ServiceMetrics,
}

impl WrapperMetrics {
    /// Register wrapper metrics against the runtime's `MetricsManager`.
    ///
    /// The runtime has already installed the global `metrics` recorder
    /// and registered platform metrics (`ServiceMetrics`, `AppMetrics`).
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
        // set_lifecycle_state / record_config_reload). Names are BARE --
        // the manager's namespace adds the app prefix once.
        metrics::describe_gauge!(
            "transform_vector_lifecycle_state",
            "Current lifecycle state (1=active)"
        );
        metrics::describe_counter!(
            "transform_vector_config_reloads_total",
            "Total config reloads by result"
        );

        let app = AppMetrics::new(manager, env!("CARGO_PKG_VERSION"), commit);
        let service = ServiceMetrics::register(manager);

        Self {
            crashes_total,
            restarts_total,
            config_validation_errors_total,
            uptime_seconds,
            app,
            service,
        }
    }

    /// Update lifecycle state gauge (set current state to 1, all others to 0).
    ///
    /// Emits `pipeline_ready` via `ServiceMetrics` and sets the labelled
    /// `transform_vector_lifecycle_state` gauge (namespace-prefixed on emit).
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
            metrics::gauge!("transform_vector_lifecycle_state", "state" => s.to_string()).set(val);
        }

        self.service.pipeline_ready(state.is_ready());
    }

    /// Record a config reload result.
    pub fn record_config_reload(&self, result: &str) {
        metrics::counter!(
            "transform_vector_config_reloads_total",
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

/// Drive scalo's `ScalingPressure` circuit gate from Vector subprocess
/// lifecycle transitions.
///
/// This is a Vector SUPERVISOR -- Vector owns the Kafka consumer, not this
/// process -- so scalo's scaling engine inbound transport is `other`
/// and the smart default is CPU-driven. The one local scale signal the
/// supervisor genuinely owns is Vector subprocess health: when the
/// subprocess is NOT serving traffic (crashed / starting / shutting down)
/// the circuit is OPENED, which pins `scaling_pressure` to 0 -- more pods
/// cannot help a down subprocess. Healthy (running / reloading) closes the
/// circuit and lets CPU drive scale-out.
///
/// Circuit state is idempotent (only the latest lifecycle state matters),
/// so the watch channel's value-coalescing is harmless here. The
/// authoritative crash/restart COUNTS are incremented at the source inside
/// [`crate::vector::run_lifecycle`], not inferred from this channel (which
/// would miss a fast Crashed->Starting->Running transition).
///
/// `pressure` is the runtime's `Option<Arc<ScalingPressure>>`: `None` when
/// the scaling engine is disabled/absent. In that case there is no circuit
/// to gate, so this returns a completed no-op handle and spawns nothing.
///
/// Spawns a background task and returns immediately. The task exits when
/// the lifecycle drops all senders.
pub fn spawn_circuit_gate_task(
    lifecycle: &Lifecycle,
    pressure: Option<std::sync::Arc<scalo::ScalingPressure>>,
) -> tokio::task::JoinHandle<()> {
    let Some(pressure) = pressure else {
        // Scaling engine disabled -- nothing to gate. Return a handle that is
        // already complete so the call site stays uniform.
        return tokio::spawn(async {});
    };

    let mut rx: watch::Receiver<State> = lifecycle.subscribe();
    tokio::spawn(async move {
        // Seed the circuit from the initial state (not ready at startup).
        pressure.set_circuit_open(!rx.borrow().is_ready());

        while rx.changed().await.is_ok() {
            let state = *rx.borrow();
            let open = !state.is_ready();
            pressure.set_circuit_open(open);
            debug!(?state, circuit_open = open, "scaling circuit gate updated");
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
