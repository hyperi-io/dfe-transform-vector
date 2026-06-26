// Project:   dfe-transform-vector
// File:      tests/integration/lifecycle.rs
// Purpose:   Integration tests for lifecycle state machine + health server
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Integration tests for the lifecycle state machine and the health
//! HTTP server.
//!
//! Metrics endpoint tests live in `tests/integration/metrics.rs` and
//! exercise `WrapperMetrics::register()` against a local
//! `MetricsManager`. The wrapper no longer runs its own metrics HTTP
//! server — the scalo `ServiceRuntime` owns `/metrics`.

use dfe_transform_vector::metrics::WrapperMetrics;
use dfe_transform_vector::vector::Lifecycle;
use dfe_transform_vector::vector::lifecycle::State;
use scalo::metrics::MetricsManager;

use crate::common::{free_port, reqwest_lite};

#[test]
fn lifecycle_state_drives_readiness() {
    let lc = Lifecycle::new();

    // Initial state: not ready
    assert!(!lc.state().is_ready());
    assert!(lc.state().is_alive());

    // Running: ready
    lc.set(State::Running);
    assert!(lc.state().is_ready());
    assert!(lc.state().is_alive());

    // Crashed: not ready, not alive
    lc.set(State::Crashed);
    assert!(!lc.state().is_ready());
    assert!(!lc.state().is_alive());

    // Reloading: still ready
    lc.set(State::Reloading);
    assert!(lc.state().is_ready());
}

#[test]
fn lifecycle_subscriber_gets_updates() {
    let lc = Lifecycle::new();
    let mut rx = lc.subscribe();

    assert_eq!(*rx.borrow(), State::Initialising);

    lc.set(State::Running);
    // The value should be visible immediately (no async needed)
    assert_eq!(*rx.borrow_and_update(), State::Running);

    lc.set(State::Crashed);
    assert_eq!(*rx.borrow_and_update(), State::Crashed);
}

#[test]
fn wrapper_metrics_register_and_render() {
    let manager = MetricsManager::new("dfe");
    let metrics = WrapperMetrics::register(&manager, "test-commit");

    // Increment some counters
    metrics.crashes_total.increment(2);
    metrics.restarts_total.increment(1);
    metrics.record_config_reload("success");
    metrics.record_config_validation_error();

    // Render via the shared MetricsManager (the runtime would render via
    // the same path in production).
    let output = manager.render();

    // Verify metric names appear in output. Bare platform counters render
    // `dfe_<name>`; the app-segment counter keeps `transform_vector_`.
    assert!(
        output.contains("dfe_crashes_total"),
        "missing crashes_total in:\n{output}"
    );
    assert!(
        output.contains("dfe_restarts_total"),
        "missing restarts_total in:\n{output}"
    );
    assert!(
        output.contains("dfe_config_validation_errors_total"),
        "missing config_validation_errors_total in:\n{output}"
    );
    assert!(
        output.contains("dfe_transform_vector_config_reloads_total"),
        "missing config_reloads_total in:\n{output}"
    );
}

#[test]
fn wrapper_metrics_lifecycle_state_gauge() {
    let manager = MetricsManager::new("dfe");
    let metrics = WrapperMetrics::register(&manager, "test-commit");

    metrics.set_lifecycle_state(State::Running);

    let output = manager.render();

    // Running should be 1, others should be 0
    assert!(
        output.contains(r#"state="running"} 1"#),
        "running state not 1 in:\n{output}"
    );
}

#[tokio::test]
async fn health_server_responds() {
    let lc = Lifecycle::new();
    lc.set(State::Running);

    let addr = free_port().await;
    let health_lc = lc.clone();
    let addr_str = addr.clone();
    tokio::spawn(async move {
        use dfe_transform_vector::health::serve_health;
        let _ = serve_health(&addr_str, health_lc).await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Test liveness (scalo HttpServer returns "OK")
    let resp = reqwest_lite(&format!("http://{addr}/health/live")).await;
    assert_eq!(resp.0, 200);
    assert!(resp.1.contains("OK"), "liveness body: {}", resp.1);

    // Test readiness (scalo HttpServer returns "OK" when ready)
    let resp = reqwest_lite(&format!("http://{addr}/health/ready")).await;
    assert_eq!(resp.0, 200);
    assert!(resp.1.contains("OK"), "readiness body: {}", resp.1);
}

#[tokio::test]
async fn health_server_reports_not_ready_when_initialising() {
    let lc = Lifecycle::new(); // State::Initialising

    let addr = free_port().await;
    let health_lc = lc.clone();
    let addr_str = addr.clone();
    tokio::spawn(async move {
        use dfe_transform_vector::health::serve_health;
        let _ = serve_health(&addr_str, health_lc).await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Liveness: alive (even when initialising)
    let resp = reqwest_lite(&format!("http://{addr}/health/live")).await;
    assert_eq!(resp.0, 200);

    // Readiness: not ready (scalo HttpServer returns "NOT READY" with 503)
    let resp = reqwest_lite(&format!("http://{addr}/health/ready")).await;
    assert_eq!(resp.0, 503);
    assert!(resp.1.contains("NOT READY"), "not-ready body: {}", resp.1);
}
