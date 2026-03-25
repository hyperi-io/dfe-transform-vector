// Project:   dfe-transform-vector
// File:      tests/integration/metrics.rs
// Purpose:   Metrics completeness, proxy, and health/metrics server tests
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Metrics completeness checks, proxy integration, and health response format.

use std::sync::Arc;
use std::time::Instant;

use dfe_transform_vector::metrics::WrapperMetrics;
use dfe_transform_vector::vector::Lifecycle;
use dfe_transform_vector::vector::lifecycle::State;

use crate::common::{free_port, reqwest_lite};

// ---------------------------------------------------------------------------
// Metrics completeness — every expected metric name must appear in output
// ---------------------------------------------------------------------------

#[test]
fn metrics_completeness_all_expected_metrics_present() {
    let metrics = WrapperMetrics::new("completeness-test");

    // Drive all code paths to ensure metrics are emitted
    metrics.crashes_total.increment(1);
    metrics.restarts_total.increment(1);
    metrics.record_config_reload("success");
    metrics.record_config_reload("failure");
    metrics.record_config_validation_error();
    metrics.set_lifecycle_state(State::Running);
    metrics.uptime_seconds.set(42.0);

    let output = metrics.render();

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
        "dfe_transform_vector_config_reloads_total",
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
    let metrics = WrapperMetrics::new("label-test");

    // Set to Running
    metrics.set_lifecycle_state(State::Running);
    let output = metrics.render();

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
// Metrics proxy — mock Vector prometheus endpoint
// ---------------------------------------------------------------------------

#[tokio::test]
async fn metrics_proxy_merges_vector_metrics() {
    let lifecycle = Lifecycle::new();
    lifecycle.set(State::Running);

    let metrics = Arc::new(WrapperMetrics::new("proxy-test"));
    let started_at = Instant::now();

    // Start a mock "Vector" prometheus endpoint
    let mock_addr = free_port().await;
    let mock_addr_clone = mock_addr.clone();
    tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind(&mock_addr_clone).await.unwrap();
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf).await;

            let body = "# HELP vector_events_in_total Total events received\n\
                         # TYPE vector_events_in_total counter\n\
                         vector_events_in_total{component_id=\"dfe_source\"} 42\n";
            let response = format!(
                "HTTP/1.1 200 OK\r\n\
                 Content-Type: text/plain\r\n\
                 Content-Length: {}\r\n\
                 Connection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes()).await;
        }
    });

    // Start metrics server pointing to mock Vector
    let metrics_addr = free_port().await;
    let metrics_lc = lifecycle.clone();
    let metrics_clone = metrics.clone();
    let metrics_addr_clone = metrics_addr.clone();
    tokio::spawn(async move {
        let _ = dfe_transform_vector::metrics::serve_metrics(
            &metrics_addr_clone,
            metrics_clone,
            metrics_lc,
            started_at,
            mock_addr,
        )
        .await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let resp = reqwest_lite(&format!("http://{metrics_addr}/metrics")).await;
    assert_eq!(resp.0, 200);

    // Wrapper metrics should be present
    assert!(
        resp.1.contains("dfe_transform_vector_uptime_seconds"),
        "missing wrapper metric in proxied response"
    );

    // Proxied Vector metrics should be merged in
    assert!(
        resp.1.contains("vector_events_in_total"),
        "missing proxied Vector metric in response.\nBody:\n{}",
        resp.1
    );
}

#[tokio::test]
async fn metrics_proxy_graceful_when_vector_down() {
    let lifecycle = Lifecycle::new();
    lifecycle.set(State::Running);

    let metrics = Arc::new(WrapperMetrics::new("proxy-down-test"));
    let started_at = Instant::now();

    // Metrics server with unreachable Vector address
    let metrics_addr = free_port().await;
    let metrics_lc = lifecycle.clone();
    let metrics_clone = metrics.clone();
    let metrics_addr_clone = metrics_addr.clone();
    tokio::spawn(async move {
        let _ = dfe_transform_vector::metrics::serve_metrics(
            &metrics_addr_clone,
            metrics_clone,
            metrics_lc,
            started_at,
            "127.0.0.1:1".to_string(), // unreachable
        )
        .await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let resp = reqwest_lite(&format!("http://{metrics_addr}/metrics")).await;
    assert_eq!(
        resp.0, 200,
        "metrics should still respond when Vector is down"
    );
    assert!(
        resp.1.contains("dfe_transform_vector_uptime_seconds"),
        "wrapper metrics should still render when Vector proxy fails"
    );
}

// ---------------------------------------------------------------------------
// Health/metrics server on concurrent ports — regression for port conflicts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn health_and_metrics_on_separate_ports() {
    let lifecycle = Lifecycle::new();
    lifecycle.set(State::Running);

    let metrics = Arc::new(WrapperMetrics::new("port-test"));
    let started_at = Instant::now();

    // Bind both servers to free ports
    let health_addr = free_port().await;
    let metrics_addr = free_port().await;

    // Spawn health server
    let health_lc = lifecycle.clone();
    let health_addr_clone = health_addr.clone();
    tokio::spawn(async move {
        let _ = dfe_transform_vector::health::serve_health(&health_addr_clone, health_lc).await;
    });

    // Spawn metrics server
    let metrics_lc = lifecycle.clone();
    let metrics_clone = metrics.clone();
    let metrics_addr_clone = metrics_addr.clone();
    tokio::spawn(async move {
        let _ = dfe_transform_vector::metrics::serve_metrics(
            &metrics_addr_clone,
            metrics_clone,
            metrics_lc,
            started_at,
            "127.0.0.1:0".to_string(),
        )
        .await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Both should respond
    let health = reqwest_lite(&format!("http://{health_addr}/health/live")).await;
    assert_eq!(health.0, 200, "health server not responding");

    let metrics_resp = reqwest_lite(&format!("http://{metrics_addr}/metrics")).await;
    assert_eq!(metrics_resp.0, 200, "metrics server not responding");

    // Metrics should NOT respond on health port
    let cross = reqwest_lite(&format!("http://{health_addr}/metrics")).await;
    assert_eq!(cross.0, 404, "health server should return 404 for /metrics");

    // Health should NOT respond on metrics port
    let cross2 = reqwest_lite(&format!("http://{metrics_addr}/health/live")).await;
    assert_eq!(
        cross2.0, 404,
        "metrics server should return 404 for /health/live"
    );
}

// ---------------------------------------------------------------------------
// Health response format — verify JSON structure
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
    // Note: the readiness flag syncs async via watch channel, so we need
    // a small delay after server start for the flag to propagate.

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
