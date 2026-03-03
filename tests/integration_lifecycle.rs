// Project:   dfe-transform-vector
// File:      tests/integration_lifecycle.rs
// Purpose:   Integration tests for lifecycle and health/metrics
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Integration tests for lifecycle state machine and observability.

use std::sync::Arc;
use std::time::Instant;

use dfe_transform_vector::metrics::WrapperMetrics;
use dfe_transform_vector::vector::Lifecycle;
use dfe_transform_vector::vector::lifecycle::State;
use prometheus::Encoder;

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
fn wrapper_metrics_register_and_encode() {
    let metrics = WrapperMetrics::new();

    // Increment some counters
    metrics.crashes_total.inc();
    metrics.crashes_total.inc();
    metrics.restarts_total.inc();
    metrics
        .config_reloads_total
        .with_label_values(&["success"])
        .inc();
    metrics.config_validation_errors_total.inc();
    metrics.up.set(1);

    // Encode to Prometheus text format
    let encoder = prometheus::TextEncoder::new();
    let families = metrics.registry.gather();
    let mut buffer = Vec::new();
    encoder.encode(&families, &mut buffer).unwrap();
    let output = String::from_utf8(buffer).unwrap();

    // Verify metric names appear in output
    assert!(output.contains("dfe_transform_vector_crashes_total 2"));
    assert!(output.contains("dfe_transform_vector_restarts_total 1"));
    assert!(output.contains("dfe_transform_vector_up 1"));
    assert!(output.contains("dfe_transform_vector_config_validation_errors_total 1"));
    assert!(output.contains("dfe_transform_vector_config_reloads_total"));
}

#[test]
fn wrapper_metrics_lifecycle_state_gauge() {
    let metrics = WrapperMetrics::new();

    metrics.set_lifecycle_state(State::Running);

    let encoder = prometheus::TextEncoder::new();
    let families = metrics.registry.gather();
    let mut buffer = Vec::new();
    encoder.encode(&families, &mut buffer).unwrap();
    let output = String::from_utf8(buffer).unwrap();

    // Running should be 1, others should be 0
    assert!(output.contains(r#"state="running"} 1"#));
    assert!(output.contains(r#"state="crashed"} 0"#));
    assert!(output.contains(r#"state="initialising"} 0"#));
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

    // Test liveness
    let resp = reqwest_lite(&format!("http://{addr}/health/live")).await;
    assert_eq!(resp.0, 200);
    assert!(resp.1.contains("alive"));

    // Test readiness
    let resp = reqwest_lite(&format!("http://{addr}/health/ready")).await;
    assert_eq!(resp.0, 200);
    assert!(resp.1.contains("ready"));
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

    // Readiness: not ready
    let resp = reqwest_lite(&format!("http://{addr}/health/ready")).await;
    assert_eq!(resp.0, 503);
    assert!(resp.1.contains("not_ready"));
}

#[tokio::test]
async fn metrics_server_responds() {
    let lc = Lifecycle::new();
    lc.set(State::Running);

    let metrics = Arc::new(WrapperMetrics::new());
    metrics.crashes_total.inc();

    let metrics_lc = lc.clone();
    let metrics_clone = metrics.clone();
    let started_at = Instant::now();

    let addr = free_port().await;
    let addr_str = addr.clone();
    tokio::spawn(async move {
        use dfe_transform_vector::metrics::serve_metrics;
        let _ = serve_metrics(&addr_str, metrics_clone, metrics_lc, started_at).await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let resp = reqwest_lite(&format!("http://{addr}/metrics")).await;
    assert_eq!(resp.0, 200);
    assert!(resp.1.contains("dfe_transform_vector_up"));
    assert!(resp.1.contains("dfe_transform_vector_crashes_total 1"));
    assert!(resp.1.contains("dfe_transform_vector_uptime_seconds"));
}

/// Minimal HTTP GET — avoids pulling in reqwest as a dep.
async fn reqwest_lite(url: &str) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let url = url.strip_prefix("http://").unwrap();
    let (host_port, path) = url.split_once('/').unwrap_or((url, ""));
    let path = format!("/{path}");

    let mut stream = TcpStream::connect(host_port).await.unwrap();
    let request = format!("GET {path} HTTP/1.1\r\nHost: {host_port}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();

    let status_line = response.lines().next().unwrap_or("");
    let status_code: u16 = status_line
        .split_whitespace()
        .nth(1)
        .unwrap_or("0")
        .parse()
        .unwrap_or(0);

    let body = response.split("\r\n\r\n").nth(1).unwrap_or("").to_string();

    (status_code, body)
}

/// Get a free port by binding to :0, extracting the address, then dropping the listener.
async fn free_port() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr.to_string()
}
