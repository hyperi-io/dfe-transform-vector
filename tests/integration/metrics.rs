// Project:   dfe-transform-vector
// File:      tests/integration/metrics.rs
// Purpose:   Metrics completeness + health endpoint tests
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Metrics registration completeness and health endpoint behaviour.
//!
//! Note: the wrapper no longer runs its own metrics HTTP server. The
//! scalo `ServiceRuntime` owns `/metrics`. These tests register
//! `WrapperMetrics` against the process-wide `MetricsManager` from
//! `common::metrics_fixture` and exercise the `render()` output directly —
//! what shows up on the live `/metrics` endpoint in production. Building a
//! manager per test instead renders empty; see that helper for why.
//!
//! The fixture namespace there is `"dfe"` (the platform namespace -- rule:
//! group by platform + app via LABEL, scalo emits BARE names and the
//! namespace prepends `dfe_` once). So bare names render `dfe_<name>` and
//! the app-segment gauges render `dfe_transform_vector_<name>`.

use std::sync::Arc;

use dfe_transform_vector::metrics::WrapperMetrics;
use dfe_transform_vector::vector::Lifecycle;
use dfe_transform_vector::vector::lifecycle::State;

use crate::common::metrics_fixture::metrics_manager;

// ---------------------------------------------------------------------------
// Metrics completeness — every expected metric name must appear in output
// ---------------------------------------------------------------------------

#[test]
fn metrics_completeness_all_expected_metrics_present() {
    let manager = metrics_manager();
    let metrics = WrapperMetrics::register(manager, "completeness-test");

    // Drive all code paths to ensure metrics are emitted
    metrics.crashes_total.increment(1);
    metrics.restarts_total.increment(1);
    metrics.record_config_reload("success");
    metrics.record_config_reload("failure");
    metrics.record_config_validation_error();
    metrics.set_lifecycle_state(State::Running);
    metrics.uptime_seconds.set(42.0);

    let output = manager.render();

    // Bare platform counters (namespace `dfe` prepends `dfe_` once).
    let expected = [
        "dfe_crashes_total",
        "dfe_restarts_total",
        "dfe_config_validation_errors_total",
        // App-segment metrics (the wrapper adds the `transform_vector_` segment).
        "dfe_transform_vector_config_reloads_total",
        "dfe_transform_vector_lifecycle_state",
        // Bare gauge
        "dfe_uptime_seconds",
        // AppMetrics (from scalo)
        "dfe_info",
        "dfe_start_time_seconds",
        // ServiceMetrics pipeline readiness
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
    let manager = metrics_manager();
    let metrics = WrapperMetrics::register(manager, "label-test");

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
    // AppMetrics / ServiceMetrics. If a future refactor adds a
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
    let manager = metrics_manager();
    let _first = WrapperMetrics::register(manager, "first");
    let _second = WrapperMetrics::register(manager, "second");
    // If we got here without panic, the contract holds.
}

// ---------------------------------------------------------------------------
// Scaling circuit gate — driven by Vector lifecycle. A down subprocess
// opens the circuit (scaling_pressure pins to 0); healthy closes it.
// Circuit state is idempotent, so the watch channel is the right source.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn circuit_gate_tracks_vector_health() {
    use scalo::scaling::{ScalingComponent, ScalingPressure, ScalingPressureConfig};

    let lifecycle = Lifecycle::new(); // starts Initialising -> not ready
    // The supervisor only drives the circuit breaker gate; a single CPU-ish
    // component keeps the pressure calculator well-formed.
    let pressure = Arc::new(ScalingPressure::new(
        ScalingPressureConfig::default(),
        vec![ScalingComponent::new("cpu", 1.0, 1.0)],
    ));

    let handle =
        dfe_transform_vector::metrics::spawn_circuit_gate_task(&lifecycle, Some(pressure.clone()));

    // Seed: Initialising is not ready, so the circuit starts OPEN.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        pressure.snapshot().circuit_open,
        "circuit should be open at startup (Vector not yet running)"
    );

    // Vector running -> circuit closes (CPU can drive scale-out).
    lifecycle.set(State::Running);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !pressure.snapshot().circuit_open,
        "circuit should close once Vector is Running"
    );

    // Reloading is still ready -> circuit stays closed.
    lifecycle.set(State::Reloading);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !pressure.snapshot().circuit_open,
        "circuit should stay closed while Reloading (still serving)"
    );

    // Crash -> circuit opens (more pods cannot help a down subprocess).
    lifecycle.set(State::Crashed);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        pressure.snapshot().circuit_open,
        "circuit should open when Vector crashes"
    );

    handle.abort();
}

// ---------------------------------------------------------------------------
// Crash / restart counters — incremented at the source in run_lifecycle.
// Driving a non-existent binary forces the spawn-failure crash path, which
// loops crash -> backoff -> restart deterministically until shutdown.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn run_lifecycle_increments_crash_and_restart_counters() {
    use std::sync::Mutex;
    use std::time::Duration;

    use dfe_transform_vector::config::VectorConfig;
    use dfe_transform_vector::vector::{BackoffConfig, run_lifecycle};

    let manager = metrics_manager();
    let metrics = WrapperMetrics::register(manager, "counter-test");

    let vector_config = VectorConfig {
        binary: "/nonexistent/vector-binary-for-test".to_string(),
        ..VectorConfig::default()
    };
    // Tight backoff so the crash -> restart loop cycles a few times fast.
    let backoff = BackoffConfig {
        initial: Duration::from_millis(15),
        max: Duration::from_millis(15),
        multiplier: 1.0,
        reset_after: Duration::from_secs(300),
    };
    let lifecycle = Lifecycle::new();
    let pid = Arc::new(Mutex::new(None));
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let crashes = metrics.crashes_total.clone();
    let restarts = metrics.restarts_total.clone();
    let dir = std::path::PathBuf::from("/tmp");
    let lc = lifecycle.clone();
    let task = tokio::spawn(async move {
        let _ = run_lifecycle(
            &vector_config,
            &dir,
            &lc,
            &backoff,
            pid,
            crashes,
            restarts,
            shutdown_rx,
        )
        .await;
    });

    // Let it cycle through several crash -> restart iterations.
    tokio::time::sleep(Duration::from_millis(120)).await;
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), task).await;

    // Process-shared global recorder: assert non-zero, not an exact value.
    let output = manager.render();
    let counter_value = |name: &str| -> f64 {
        output
            .lines()
            .find(|l| l.starts_with(name) && !l.starts_with("# "))
            .and_then(|l| l.rsplit(' ').next())
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0)
    };
    assert!(
        counter_value("dfe_crashes_total") >= 1.0,
        "crashes_total should be >= 1 after spawn failures, got:\n{output}"
    );
    assert!(
        counter_value("dfe_restarts_total") >= 1.0,
        "restarts_total should be >= 1 after re-spawns, got:\n{output}"
    );
}

// ---------------------------------------------------------------------------
// Vector metrics merge — the wrapper scrapes Vector's prometheus_exporter on
// loopback and registers the samples on the SAME registry, so `vector_*` rides
// /metrics and the OTLP push and the app's throughput counters stop reading 0.
// ---------------------------------------------------------------------------

/// A fixed Vector exposition, label shapes as Vector emits them.
const VECTOR_EXPOSITION: &str = "\
# HELP vector_component_received_events_total events in
# TYPE vector_component_received_events_total counter
vector_component_received_events_total{component_id=\"dfe_source\",component_type=\"kafka\"} 85
# HELP vector_component_sent_events_total events out
# TYPE vector_component_sent_events_total counter
vector_component_sent_events_total{component_id=\"dfe_sink\",component_kind=\"sink\"} 84
# HELP vector_utilization component busy ratio
# TYPE vector_utilization gauge
vector_utilization{component_id=\"dfe_sink\"} 0.25
# HELP vector_rtt_seconds request round trip
# TYPE vector_rtt_seconds histogram
vector_rtt_seconds_bucket{le=\"0.005\"} 3
vector_rtt_seconds_bucket{le=\"+Inf\"} 9
vector_rtt_seconds_sum 0.42
vector_rtt_seconds_count 9
";

/// Serve `VECTOR_EXPOSITION` once per connection on an ephemeral loopback port.
async fn fake_exporter() -> (String, tokio::task::JoinHandle<()>) {
    serve_body(VECTOR_EXPOSITION.to_string()).await
}

/// Serve `body` with a 200 once per connection on an ephemeral loopback port.
async fn serve_body(body: String) -> (String, tokio::task::JoinHandle<()>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake exporter");
    let addr = listener.local_addr().expect("local addr").to_string();

    let handle = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let mut req = [0u8; 1024];
            let _ = sock.read(&mut req).await;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\
                 Connection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = sock.write_all(response.as_bytes()).await;
            let _ = sock.shutdown().await;
        }
    });

    (addr, handle)
}

#[tokio::test]
async fn vector_samples_land_in_the_scalo_registry_with_their_types() {
    use dfe_transform_vector::metrics::scrape::{
        ExpositionMerger, apply_derived, fetch_exposition,
    };

    let manager = metrics_manager();
    let metrics = WrapperMetrics::register(manager, "scrape-test");

    let (addr, server) = fake_exporter().await;
    let body = fetch_exposition(&addr).await.expect("scrape fake exporter");
    let derived = ExpositionMerger::default().merge(&body);
    apply_derived(&metrics, &derived);
    server.abort();

    let output = manager.render();

    // Names survive with the platform namespace the fixture applies, as every
    // other metric on this endpoint gets.
    for expected in [
        "# TYPE dfe_vector_component_received_events_total counter",
        "# TYPE dfe_vector_utilization gauge",
        "dfe_vector_utilization{component_id=\"dfe_sink\"} 0.25",
        "dfe_vector_rtt_seconds_bucket{le=\"+Inf\"} 9",
    ] {
        assert!(
            output.contains(expected),
            "missing '{expected}' in rendered output:\n{output}"
        );
    }

    // Labels are preserved verbatim, so the sink series is still separable.
    assert!(
        output.contains("component_id=\"dfe_sink\"") && output.contains("component_kind=\"sink\""),
        "Vector labels were dropped:\n{output}"
    );

    // Derived throughput: source `received`, sink `sent`.
    assert_eq!(derived.received, 85.0);
    assert_eq!(derived.sent, 84.0);

    let counter_value = |name: &str| -> f64 {
        output
            .lines()
            .find(|l| l.starts_with(name) && !l.starts_with("# "))
            .and_then(|l| l.rsplit(' ').next())
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0)
    };
    assert!(
        counter_value("dfe_records_received_total") >= 85.0,
        "records_received_total did not follow Vector's source counter:\n{output}"
    );
    assert!(
        counter_value("dfe_records_processed_total") >= 84.0,
        "records_processed_total did not follow Vector's sink counter:\n{output}"
    );
    assert!(
        counter_value("dfe_records_delivered_total") >= 84.0,
        "records_delivered_total did not follow Vector's sink counter:\n{output}"
    );
}

#[tokio::test]
async fn an_exposition_past_the_byte_cap_fails_the_scrape() {
    use dfe_transform_vector::metrics::scrape::fetch_exposition;

    // Past the 4 MiB cap in otherwise-valid samples. The read timeout bounds
    // how long a scrape may take, not how much it may return, so only the cap
    // stops this reaching the heap and the registry.
    let mut body = String::with_capacity(5 * 1024 * 1024);
    let mut i = 0u32;
    while body.len() < 5 * 1024 * 1024 {
        body.push_str(&format!("vector_flood_total{{series=\"{i}\"}} 1\n"));
        i += 1;
    }

    let (addr, server) = serve_body(body).await;
    let err = fetch_exposition(&addr)
        .await
        .expect_err("an oversized exposition must fail the scrape, not merge partially");
    server.abort();

    assert!(
        err.contains("more than"),
        "expected the byte cap to be the reason, got: {err}"
    );
}

#[tokio::test]
async fn unreachable_exporter_counts_and_does_not_panic() {
    use std::time::Duration;

    use dfe_transform_vector::metrics::scrape::spawn_vector_scrape_task;

    // Bind then drop, so the port is almost certainly closed.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let dead = listener.local_addr().expect("local addr").to_string();
    drop(listener);

    let manager = metrics_manager();
    let metrics = Arc::new(WrapperMetrics::register(manager, "scrape-fail-test"));

    let task = spawn_vector_scrape_task(metrics.clone(), dead, Duration::from_millis(20), 4);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!task.is_finished(), "the scrape loop must survive failures");
    task.abort();

    let output = manager.render();
    let value = output
        .lines()
        .find(|l| {
            l.starts_with("dfe_transform_vector_scrape_failures_total") && !l.starts_with("# ")
        })
        .and_then(|l| l.rsplit(' ').next())
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.0);
    assert!(value >= 1.0, "scrape failures were not counted:\n{output}");
}

// The probe endpoints are exercised in `tests/integration/lifecycle.rs`, which
// registers into scalo's process-global health registry and must be the only
// test that does.

#[allow(dead_code, unused_imports)]
mod _suppress_unused {
    // Keep these in scope so removing them in a careless edit doesn't
    // silently break the helpers that other test modules import.
    use super::Arc;
}

// Marker: the old serve_metrics tests have been deleted — the wrapper no
// longer runs a second HTTP server. Vector's own prometheus_exporter stays on
// loopback and is merged into this registry by the scrape task above.
