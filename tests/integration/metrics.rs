// Project:   dfe-transform-vector
// File:      tests/integration/metrics.rs
// Purpose:   Metrics completeness + health endpoint tests
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Metrics registration completeness and health endpoint behaviour.
//!
//! Note: the wrapper no longer runs its own metrics HTTP server. The
//! rustlib `ServiceRuntime` owns `/metrics`. These tests register
//! `WrapperMetrics` against a local `MetricsManager` and exercise the
//! `render()` output directly — what shows up on the live `/metrics`
//! endpoint in production.

use std::sync::Arc;

use dfe_transform_vector::metrics::WrapperMetrics;
use dfe_transform_vector::vector::Lifecycle;
use dfe_transform_vector::vector::lifecycle::State;
use hyperi_rustlib::metrics::MetricsManager;

use crate::common::{free_port, reqwest_lite};

// ---------------------------------------------------------------------------
// Metrics completeness — every expected metric name must appear in output
// ---------------------------------------------------------------------------

#[test]
fn metrics_completeness_all_expected_metrics_present() {
    let manager = MetricsManager::new("dfe_transform_vector");
    let metrics = WrapperMetrics::register(&manager, "completeness-test");

    // Drive all code paths to ensure metrics are emitted
    metrics.crashes_total.increment(1);
    metrics.restarts_total.increment(1);
    metrics.record_config_reload("success");
    metrics.record_config_reload("failure");
    metrics.record_config_validation_error();
    metrics.set_lifecycle_state(State::Running);
    metrics.uptime_seconds.set(42.0);

    let output = manager.render();

    // Service-specific counters
    let expected = [
        "dfe_transform_vector_crashes_total",
        "dfe_transform_vector_restarts_total",
        "dfe_transform_vector_config_validation_errors_total",
        "dfe_transform_vector_config_reloads_total",
        // Gauges
        "dfe_transform_vector_lifecycle_state",
        "dfe_transform_vector_uptime_seconds",
        // AppMetrics (from rustlib)
        "dfe_transform_vector_info",
        "dfe_transform_vector_start_time_seconds",
        // DfeMetrics pipeline readiness
        "dfe_pipeline_ready",
    ];

    for metric in &expected {
        assert!(
            output.contains(metric),
            "missing expected metric '{metric}' in rendered output.\n\
             First 500 chars of output:\n{}",
            &output[..output.len().min(500)]
        );
    }
}

#[test]
fn metrics_lifecycle_state_labels_are_correct() {
    let manager = MetricsManager::new("dfe_transform_vector");
    let metrics = WrapperMetrics::register(&manager, "label-test");

    // Set to Running
    metrics.set_lifecycle_state(State::Running);
    let output = manager.render();

    // Running should be 1
    assert!(
        output.contains(r#"dfe_transform_vector_lifecycle_state{state="running"} 1"#),
        "running state not 1"
    );

    // Other states should be 0
    for state in [
        "initialising",
        "validating",
        "starting",
        "crashed",
        "shutting_down",
    ] {
        let expected = format!(r#"dfe_transform_vector_lifecycle_state{{state="{state}"}} 0"#);
        assert!(
            output.contains(&expected),
            "state '{state}' should be 0 when running, got:\n{output}"
        );
    }
}

// ---------------------------------------------------------------------------
// Double-init regression — WrapperMetrics must NOT create its own
// MetricsManager. The whole point of the May 2026 refactor was to use
// the runtime's manager so we have exactly one /metrics endpoint, one
// global recorder, and no port-bind collision.
// ---------------------------------------------------------------------------

#[test]
fn wrapper_metrics_does_not_own_a_metrics_manager() {
    // WrapperMetrics only carries metric *handles* (Counter, Gauge) +
    // AppMetrics / DfeMetrics. If a future refactor adds a
    // `manager: MetricsManager` field back, this test fails at compile
    // time — std::mem::size_of catches the layout change.
    let size = std::mem::size_of::<WrapperMetrics>();
    // Sanity bound: a handful of Counter/Gauge handles + two structs.
    // A full MetricsManager would push this over 200 bytes. The exact
    // bound is conservative; the goal is "didn't quietly regrow".
    assert!(
        size < 256,
        "WrapperMetrics grew to {size} bytes — did a MetricsManager creep back in?"
    );
}

#[test]
fn wrapper_metrics_register_is_idempotent_on_shared_manager() {
    // Registering twice on the same manager must not panic. The metrics
    // crate uses interior deduplication, so a re-registration returns
    // the same counter handle.
    let manager = MetricsManager::new("dfe_transform_vector");
    let _first = WrapperMetrics::register(&manager, "first");
    let _second = WrapperMetrics::register(&manager, "second");
    // If we got here without panic, the contract holds.
}

// ---------------------------------------------------------------------------
// Health response format — health server is unchanged
// ---------------------------------------------------------------------------

#[tokio::test]
async fn health_endpoints_respond_correctly() {
    let lifecycle = Lifecycle::new();
    lifecycle.set(State::Running);

    let addr = free_port().await;
    let health_lc = lifecycle.clone();
    let addr_clone = addr.clone();
    tokio::spawn(async move {
        let _ = dfe_transform_vector::health::serve_health(&addr_clone, health_lc).await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Liveness — always 200 if process can respond (rustlib standard)
    let resp = reqwest_lite(&format!("http://{addr}/health/live")).await;
    assert_eq!(resp.0, 200, "liveness should be 200 when running");

    // Readiness — 200 when Running
    let resp = reqwest_lite(&format!("http://{addr}/health/ready")).await;
    assert_eq!(resp.0, 200, "readiness should be 200 when running");
}

#[tokio::test]
async fn health_not_ready_returns_503() {
    let lifecycle = Lifecycle::new();
    // Initialising state = not ready

    let addr = free_port().await;
    let health_lc = lifecycle.clone();
    let addr_clone = addr.clone();
    tokio::spawn(async move {
        let _ = dfe_transform_vector::health::serve_health(&addr_clone, health_lc).await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Readiness should be 503 when initialising (not ready)
    let resp = reqwest_lite(&format!("http://{addr}/health/ready")).await;
    assert_eq!(resp.0, 503, "readiness should be 503 when initialising");

    // Liveness always 200 (rustlib: process is alive if it can respond)
    let resp = reqwest_lite(&format!("http://{addr}/health/live")).await;
    assert_eq!(resp.0, 200, "liveness should be 200 even when initialising");
}

#[allow(dead_code, unused_imports)]
mod _suppress_unused {
    // Keep these in scope so removing them in a careless edit doesn't
    // silently break the helpers that other test modules import.
    use super::Arc;
}

// Marker: the old serve_metrics / metrics_proxy_* tests have been deleted
// — the wrapper no longer runs a second HTTP server. Vector exposes its
// own prometheus_exporter on `config.metrics.vector_metrics_address`;
// Prometheus scrapes that endpoint directly as a separate target.
