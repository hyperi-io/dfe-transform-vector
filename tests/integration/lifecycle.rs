// Project:   dfe-transform-vector
// File:      tests/integration/lifecycle.rs
// Purpose:   Integration tests for lifecycle state machine + readiness reporting
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Integration tests for the lifecycle state machine and the readiness it
//! publishes.
//!
//! Metrics endpoint tests live in `tests/integration/metrics.rs` and
//! exercise `WrapperMetrics::register()` against the process-wide
//! `MetricsManager` from `common::metrics_fixture`. The wrapper runs no HTTP
//! server of its own — the scalo `ServiceRuntime` owns the one listener that
//! serves `/metrics`, `/livez` and `/readyz`.

use scalo::metrics::MetricsManager;

use dfe_transform_vector::health::register_readiness;
use dfe_transform_vector::metrics::WrapperMetrics;
use dfe_transform_vector::vector::Lifecycle;
use dfe_transform_vector::vector::lifecycle::State;

use crate::common::metrics_fixture::metrics_manager;
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
    let manager = metrics_manager();
    let metrics = WrapperMetrics::register(manager, "test-commit");

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
    let manager = metrics_manager();
    let metrics = WrapperMetrics::register(manager, "test-commit");

    metrics.set_lifecycle_state(State::Running);

    let output = manager.render();

    // Running should be 1, others should be 0
    assert!(
        output.contains(r#"state="running"} 1"#),
        "running state not 1 in:\n{output}"
    );
}

/// The probe surface a deployed pod actually answers on: scalo's metrics
/// listener, which the generated liveness, readiness and startup probes all
/// target, reading the lifecycle the wrapper publishes into scalo's health
/// registry.
///
/// Registration is process-global and cannot be undone, so this is the only
/// test that registers, and it leaves its lifecycle `Running`.
#[tokio::test]
async fn the_probed_port_reports_the_real_lifecycle_state() {
    // Take the shared manager's recorder install first; the throwaway manager
    // below must lose that race, not the fixture every other test renders from.
    let _ = metrics_manager();

    let lc = Lifecycle::new();
    register_readiness(&lc);

    let addr = free_port().await;
    let mut server = MetricsManager::new("dfe");
    server
        .start_server(&addr)
        .await
        .expect("metrics listener binds");
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Initialising: the wrapper is up, the subprocess is not carrying traffic.
    assert_eq!(reqwest_lite(&format!("http://{addr}/livez")).await.0, 200);
    assert_eq!(reqwest_lite(&format!("http://{addr}/readyz")).await.0, 503);

    // The defect this guards: a crashing subprocess under a live supervisor
    // must take the pod out of the Service instead of advertising healthy.
    lc.set(State::Crashed);
    assert_eq!(
        reqwest_lite(&format!("http://{addr}/readyz")).await.0,
        503,
        "a crashed Vector subprocess must not report ready"
    );

    // Liveness stays 200: the supervisor is alive and owns the backoff.
    assert_eq!(reqwest_lite(&format!("http://{addr}/livez")).await.0, 200);

    lc.set(State::Running);
    assert_eq!(reqwest_lite(&format!("http://{addr}/readyz")).await.0, 200);
}

/// Write an executable that exits straight away, standing in for the Vector
/// binary that exits 2 on a bad argv.
fn write_instant_exit_binary(dir: &std::path::Path) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt;

    let path = dir.join("vector-that-exits");
    std::fs::write(&path, "#!/bin/sh\nexit 2\n").expect("write the fake binary");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
        .expect("make the fake binary executable");
    path
}

/// A spawn that returns `Ok` is not a running subprocess: a binary that exits
/// immediately must never reach `Running`, or every crash-on-start reads as a
/// healthy start.
#[tokio::test]
async fn a_subprocess_that_exits_immediately_never_reports_running() {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use dfe_transform_vector::config::VectorConfig;
    use dfe_transform_vector::vector::{BackoffConfig, run_lifecycle};

    let work = tempfile::TempDir::new().expect("work dir");
    let binary = write_instant_exit_binary(work.path());

    let vector_config = VectorConfig {
        binary: binary.to_string_lossy().into_owned(),
        ..VectorConfig::default()
    };
    let backoff = BackoffConfig {
        initial: Duration::from_millis(15),
        max: Duration::from_millis(15),
        multiplier: 1.0,
        reset_after: Duration::from_secs(300),
    };

    let lifecycle = Lifecycle::new();

    // Record every transition rather than sampling, so a brief `Running` in the
    // middle of a crash loop cannot slip past.
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&seen);
    let mut rx = lifecycle.subscribe();
    let watcher = tokio::spawn(async move {
        while rx.changed().await.is_ok() {
            let state = *rx.borrow_and_update();
            recorded.lock().expect("state log").push(state);
        }
    });

    let metrics = WrapperMetrics::register(metrics_manager(), "settle-test");
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let lc = lifecycle.clone();
    let dir = work.path().to_path_buf();
    let task = tokio::spawn(async move {
        let _ = run_lifecycle(
            &vector_config,
            &dir,
            &lc,
            &backoff,
            Arc::new(Mutex::new(None)),
            metrics.crashes_total.clone(),
            metrics.restarts_total.clone(),
            shutdown_rx,
        )
        .await;
    });

    // Long enough for two spawn -> settle -> crash cycles.
    tokio::time::sleep(Duration::from_millis(1_400)).await;
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
    watcher.abort();

    let states = seen.lock().expect("state log").clone();
    assert!(
        !states.contains(&State::Running),
        "a subprocess that exits on spawn must never report Running, saw: {states:?}"
    );
    assert!(
        states.contains(&State::Crashed),
        "the crash must still be reported, saw: {states:?}"
    );
}
