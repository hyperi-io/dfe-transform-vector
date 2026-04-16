// Project:   dfe-transform-vector
// File:      tests/e2e/metrics_proxy.rs
// Purpose:   E2E test for metrics proxy against a real Vector subprocess
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Metrics proxy e2e.
//!
//! Spawns Vector with a minimal `internal_metrics` → `prometheus_exporter`
//! pipeline, then verifies our wrapper's `/metrics` endpoint correctly
//! proxies Vector's real output. Exercises the HTTP status parsing fix
//! and error-path logging introduced in v1.0.8.
//!
//! Requires Vector binary (via `scripts/fetch-vector.sh` or system PATH).
//! Does NOT require Kafka — Vector's internal_metrics produces real
//! Prometheus text without any Kafka connection.
//!
//! Run with: `cargo nextest run --test e2e --run-ignored all`

use std::fs;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dfe_transform_vector::metrics::WrapperMetrics;
use dfe_transform_vector::vector::Lifecycle;
use dfe_transform_vector::vector::lifecycle::State;
use tempfile::TempDir;
use tokio::process::Command;
use tokio::time::sleep;

use crate::common::{free_port, reqwest_lite, vector_binary_path};

/// Poll an HTTP endpoint until it responds with 200, or return false after timeout.
async fn wait_for_http(addr: &str, timeout: Duration) -> bool {
    let url = format!("http://{addr}/metrics");
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok((code, _)) =
            tokio::time::timeout(Duration::from_millis(500), reqwest_lite(&url)).await
            && code == 200
        {
            return true;
        }
        sleep(Duration::from_millis(200)).await;
    }
    false
}

/// Write a minimal Vector config to the given path.
///
/// Only uses `internal_metrics` source and `prometheus_exporter` sink —
/// no external dependencies, no Kafka, no disk writes.
fn write_minimal_vector_config(path: &std::path::Path, prom_addr: &str, data_dir: &str) {
    let yaml = format!(
        r#"
data_dir: {data_dir}
api:
  enabled: false
sources:
  internal:
    type: internal_metrics
    scrape_interval_secs: 1
sinks:
  prom:
    type: prometheus_exporter
    inputs: ["internal"]
    address: {prom_addr}
"#
    );
    fs::write(path, yaml).expect("write vector config");
}

#[tokio::test]
#[ignore] // requires Vector binary
async fn e2e_metrics_proxy_fetches_real_vector_metrics() {
    crate::common::skip_if_no_vector!();

    let vector_bin = vector_binary_path().expect("Vector binary should be available");

    let work = TempDir::new().expect("tmp dir");
    let data_dir = work.path().join("data");
    fs::create_dir_all(&data_dir).expect("data dir");

    // Find 2 free ports: wrapper metrics server, Vector prometheus_exporter
    let wrapper_addr = free_port().await;
    let vector_prom_addr = free_port().await;

    let config_path = work.path().join("vector.yaml");
    write_minimal_vector_config(&config_path, &vector_prom_addr, &data_dir.to_string_lossy());

    // Spawn Vector — kill_on_drop(true) ensures process is killed even on panic
    let mut _child = Command::new(vector_bin)
        .arg("--config")
        .arg(&config_path)
        .env("VECTOR_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn vector");

    // Wait for Vector's prometheus_exporter to come up (real service = real polling)
    assert!(
        wait_for_http(&vector_prom_addr, Duration::from_secs(20)).await,
        "Vector prometheus_exporter did not come up on {vector_prom_addr} within 20s"
    );

    // Start wrapper metrics server pointing to real Vector
    let lifecycle = Lifecycle::new();
    lifecycle.set(State::Running); // Proxy only fires when lifecycle.is_ready()

    let metrics = Arc::new(WrapperMetrics::new("e2e-proxy"));
    let started_at = Instant::now();
    let lc_clone = lifecycle.clone();
    let wrapper_clone = wrapper_addr.clone();
    let prom_clone = vector_prom_addr.clone();
    let mc = metrics.clone();
    tokio::spawn(async move {
        let _ = dfe_transform_vector::metrics::serve_metrics(
            &wrapper_clone,
            mc,
            lc_clone,
            started_at,
            prom_clone,
        )
        .await;
    });

    // Give wrapper a moment to bind
    assert!(
        wait_for_http(&wrapper_addr, Duration::from_secs(5)).await,
        "wrapper metrics server did not bind on {wrapper_addr} within 5s"
    );

    // Scrape the wrapper — this is the proxy code path under test
    let (code, body) = reqwest_lite(&format!("http://{wrapper_addr}/metrics")).await;
    assert_eq!(code, 200, "wrapper should return 200");

    // Wrapper-owned metrics must be present
    assert!(
        body.contains("dfe_transform_vector_uptime_seconds"),
        "missing wrapper uptime metric.\nBody (first 800 chars):\n{}",
        &body[..body.len().min(800)]
    );
    assert!(
        body.contains("dfe_pipeline_ready"),
        "missing dfe_pipeline_ready gauge"
    );

    // Vector-emitted metric names should be merged in. Vector always emits
    // at least one `vector_*` metric from internal_metrics (component_events,
    // vector_started_total, vector_uptime_seconds depending on version).
    assert!(
        body.contains("vector_"),
        "no proxied Vector metrics found — proxy failed.\n\
         Body (first 1200 chars):\n{}",
        &body[..body.len().min(1200)]
    );

    // Validate that proxied body is valid Prometheus text format —
    // must contain at least one # HELP line from Vector
    assert!(
        body.contains("# HELP vector_") || body.contains("# TYPE vector_"),
        "proxied Vector metrics missing # HELP/# TYPE lines — status parse or body extraction may be broken"
    );
}

#[tokio::test]
#[ignore] // requires Vector binary
async fn e2e_metrics_proxy_handles_vector_restart() {
    // Regression test: if Vector goes down briefly, the proxy should not
    // 500. It should log the failure and return wrapper-only metrics.
    crate::common::skip_if_no_vector!();

    let lifecycle = Lifecycle::new();
    lifecycle.set(State::Running);
    let metrics = Arc::new(WrapperMetrics::new("e2e-restart"));
    let started_at = Instant::now();

    // Start wrapper pointing at a port where no Vector is running
    let wrapper_addr = free_port().await;
    let unreachable = free_port().await; // Bind briefly to reserve, then let it go

    let lc_clone = lifecycle.clone();
    let wrapper_clone = wrapper_addr.clone();
    let mc = metrics.clone();
    tokio::spawn(async move {
        let _ = dfe_transform_vector::metrics::serve_metrics(
            &wrapper_clone,
            mc,
            lc_clone,
            started_at,
            unreachable,
        )
        .await;
    });

    assert!(
        wait_for_http(&wrapper_addr, Duration::from_secs(5)).await,
        "wrapper metrics server did not bind"
    );

    let (code, body) = reqwest_lite(&format!("http://{wrapper_addr}/metrics")).await;
    assert_eq!(
        code, 200,
        "wrapper must return 200 even when Vector is down (graceful degradation)"
    );
    assert!(
        body.contains("dfe_transform_vector_uptime_seconds"),
        "wrapper metrics must still render when proxy fails"
    );
}
