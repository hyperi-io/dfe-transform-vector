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
//! `/livez`, and `/readyz` on `args.metrics_addr` — this module no
//! longer runs its own HTTP server, eliminating the previous double-bind
//! against the same port.
//!
//! Vector's own metrics (transport, records, internal counters) are exposed
//! by Vector's `prometheus_exporter` sink on
//! `config.metrics.vector_metrics_address` (default `127.0.0.1:9598`). That
//! port is a LOOPBACK DEBUG surface, not a scrape target: [`scrape`] pulls it
//! on scalo's metrics interval and merges every sample into this registry, so
//! `/metrics` on the ops port and the OTLP push both carry `vector_*`
//! alongside the wrapper's own metrics. Nothing outside the pod needs 9598,
//! and no second Prometheus target or PodMonitor selector is involved.

pub mod scrape;

use std::time::{Duration, Instant};

use metrics::{Counter, Gauge};
use scalo::metrics::MetricsManager;
use scalo::metrics::groups::AppMetrics;
use scalo::metrics::service::ServiceMetrics;
use scalo::transport::DeadLetterReason;
use tokio::sync::watch;
use tracing::debug;

use crate::vector::Lifecycle;
use crate::vector::lifecycle::State;

/// scalo's counter of records dropped with nowhere to go, by `reason`.
pub const DEAD_LETTERS_DROPPED: &str = "pipeline_dead_letters_dropped_total";

/// The `reason` scalo counts a record over a size ceiling under.
pub const TOO_LARGE: &str = DeadLetterReason::TooLarge { bytes: 0, limit: 0 }.as_str();

/// Count `records` dropped with nowhere to go under `reason`, in the series
/// scalo counts its own drops in.
pub fn count_dropped_dead_letters(reason: &'static str, records: u64) {
    if records > 0 {
        metrics::counter!(DEAD_LETTERS_DROPPED, "reason" => reason).increment(records);
    }
}

/// What [`DEAD_LETTERS_DROPPED`] gained under `reason` since `snapshotter`'s
/// last snapshot.
#[cfg(test)]
pub(crate) fn dropped_since(
    snapshotter: &metrics_util::debugging::Snapshotter,
    reason: &str,
) -> u64 {
    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, ..)| {
            key.key().name() == DEAD_LETTERS_DROPPED
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "reason" && l.value() == reason)
        })
        .map(|(.., value)| match value {
            metrics_util::debugging::DebugValue::Counter(n) => n,
            _ => 0,
        })
        .sum()
}

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
    pub scrape_failures_total: Counter,
    /// Vector's own error count on the generated sink.
    pub sink_errors_total: Counter,
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
    ///
    /// Pass the runtime's manager, never a second one. `MetricsManager::new`
    /// installs the global recorder via `metrics::set_global_recorder`, which
    /// succeeds at most once per process — a later manager keeps a Prometheus
    /// handle over a registry nothing writes to, so its `render()` is empty
    /// and `/metrics` silently serves nothing.
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
        // App-segment name: the manager's namespace prepends the platform
        // prefix, giving `dfe_transform_vector_scrape_failures_total`.
        let scrape_failures_total = manager.counter(
            "transform_vector_scrape_failures_total",
            "Failed scrapes of Vector's prometheus_exporter",
        );
        let sink_errors_total = manager.counter(
            "transform_vector_sink_errors_total",
            "Errors Vector reported on the generated sink",
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
            scrape_failures_total,
            sink_errors_total,
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

#[cfg(test)]
mod tests {
    use scalo::metrics::{MetricsConfig, MetricsManager};

    use super::WrapperMetrics;

    /// `metrics-manifest` and `generate-artefacts` build their own manager and
    /// call `ServiceApp::register_metrics`, so the catalogue carries only what
    /// registration puts through that manager.
    #[test]
    fn registering_the_wrapper_metrics_fills_the_manifest_catalogue() {
        // The manager the subcommand builds, minus the app-name namespace, so
        // the names read as the service emits them.
        let manager = MetricsManager::with_config(MetricsConfig::offline(""));
        assert!(
            manager.registry().manifest().metrics.is_empty(),
            "a fresh manager starts with no catalogue"
        );

        let _metrics = WrapperMetrics::register(&manager, "test-commit");
        let names: Vec<String> = manager
            .registry()
            .manifest()
            .metrics
            .into_iter()
            .map(|d| d.name)
            .collect();

        for expected in [
            "crashes_total",
            "restarts_total",
            "config_validation_errors_total",
            "transform_vector_scrape_failures_total",
            "transform_vector_sink_errors_total",
            "records_error_total",
            "uptime_seconds",
            "pipeline_ready",
            super::DEAD_LETTERS_DROPPED,
        ] {
            assert!(
                names.iter().any(|n| n == expected),
                "{expected} is missing from the catalogue: {names:?}"
            );
        }
    }

    /// Size-cap and broker refusals are counted in this series by `reason`, so
    /// the catalogue has to carry the label an alert selects on.
    #[test]
    fn the_dead_letter_drop_counter_is_catalogued_with_its_reason_label() {
        let manager = MetricsManager::with_config(MetricsConfig::offline(""));
        let _metrics = WrapperMetrics::register(&manager, "test-commit");

        let descriptor = manager
            .registry()
            .manifest()
            .metrics
            .into_iter()
            .find(|d| d.name == super::DEAD_LETTERS_DROPPED)
            .expect("the dead-letter drop counter is catalogued");
        assert!(
            descriptor.labels.iter().any(|l| l == "reason"),
            "no reason label: {:?}",
            descriptor.labels
        );
    }
}
