#![allow(clippy::unwrap_used, clippy::expect_used)]
// Project:   dfe-transform-vector
// File:      tests/smoke.rs
// Purpose:   Startup smoke tests and CLI binary tests — fast, no servers
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Smoke tests that catch init panics and CLI regressions before they
//! reach production. These run on every push and require no external
//! dependencies (no servers, no network).

use dfe_transform_vector::config::Config;
use dfe_transform_vector::metrics::WrapperMetrics;
use dfe_transform_vector::vector::lifecycle::State;
use dfe_transform_vector::vector::{BackoffConfig, Lifecycle};

// ---------------------------------------------------------------------------
// Startup smoke tests — catch init panics
// ---------------------------------------------------------------------------

#[test]
fn smoke_metrics_initialisation_does_not_panic() {
    // WrapperMetrics::new() installs a global recorder and registers all
    // metrics. If any metric name/description is invalid or the recorder
    // setup panics, this test catches it.
    let _metrics = WrapperMetrics::new("smoke-test-commit");
}

#[test]
fn smoke_lifecycle_initialisation_does_not_panic() {
    let lifecycle = Lifecycle::new();
    assert_eq!(lifecycle.state(), State::Initialising);

    // Walk through all state transitions
    for state in [
        State::Validating,
        State::Starting,
        State::Running,
        State::Reloading,
        State::Running,
        State::ShuttingDown,
        State::Crashed,
    ] {
        lifecycle.set(state);
        assert_eq!(lifecycle.state(), state);
    }
}

#[test]
fn smoke_config_load_from_fixture_does_not_panic() {
    let config = Config::load(Some("tests/fixtures/configs/minimal.yaml"))
        .expect("fixture minimal.yaml should load");
    config.validate().expect("fixture should pass validation");
}

#[test]
fn smoke_backoff_config_defaults_are_sane() {
    let backoff = BackoffConfig::default();
    assert!(
        backoff.initial.as_millis() > 0,
        "initial backoff must be positive"
    );
    assert!(
        backoff.max >= backoff.initial,
        "max backoff must be >= initial"
    );
}

#[test]
fn smoke_metrics_render_after_state_transitions() {
    let metrics = WrapperMetrics::new("smoke-render");

    // Simulate a full lifecycle
    metrics.crashes_total.increment(1);
    metrics.restarts_total.increment(2);
    metrics.record_config_reload("success");
    metrics.record_config_reload("failure");
    metrics.record_config_validation_error();
    metrics.set_lifecycle_state(State::Running);
    metrics.uptime_seconds.set(123.456);

    // Render must not panic
    let output = metrics.render();

    // Must produce non-empty output
    assert!(
        !output.is_empty(),
        "metrics render produced empty output after state transitions"
    );
}

// ---------------------------------------------------------------------------
// Config validation error paths — meaningful errors, not panics
// ---------------------------------------------------------------------------

#[test]
fn config_load_missing_file_returns_defaults() {
    // Config::load silently falls back to defaults when file doesn't exist
    let config = Config::load(Some("/nonexistent/config.yaml"));
    assert!(config.is_ok(), "missing file should fall back to defaults");
}

#[test]
fn config_validate_empty_pipeline_name_fails() {
    let yaml = r#"
pipeline:
  name: ""
source:
  brokers:
    - "localhost:9092"
  topics:
    - "test"
  group_id: "test-group"
sink:
  brokers:
    - "localhost:9092"
  topic: "output"
"#;
    let tmpdir = tempfile::tempdir().unwrap();
    let path = tmpdir.path().join("config.yaml");
    std::fs::write(&path, yaml).unwrap();

    let config = Config::load(Some(path.to_str().unwrap()));
    // Either load fails or validate fails — both are acceptable
    if let Ok(config) = config {
        let result = config.validate();
        assert!(
            result.is_err(),
            "empty pipeline name should fail validation"
        );
    }
}

#[test]
fn config_validate_missing_sink_topic_fails() {
    let yaml = r#"
pipeline:
  name: "test"
source:
  brokers:
    - "localhost:9092"
  topics:
    - "test"
  group_id: "test-group"
sink:
  brokers:
    - "localhost:9092"
  topic: ""
"#;
    let tmpdir = tempfile::tempdir().unwrap();
    let path = tmpdir.path().join("config.yaml");
    std::fs::write(&path, yaml).unwrap();

    let config = Config::load(Some(path.to_str().unwrap())).unwrap();
    let result = config.validate();
    assert!(result.is_err(), "empty sink topic should fail validation");
}

// ---------------------------------------------------------------------------
// CLI binary invocation — test subcommands via cargo
// ---------------------------------------------------------------------------

#[test]
fn cli_version_prints_version() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_dfe-transform-vector"))
        .arg("version")
        .output()
        .expect("failed to run binary");

    assert!(output.status.success(), "version command failed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(env!("CARGO_PKG_VERSION")),
        "version output should contain package version, got: {stdout}"
    );
}

#[test]
fn cli_config_check_with_valid_fixture() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_dfe-transform-vector"))
        .args([
            "--config",
            "tests/fixtures/configs/minimal.yaml",
            "config-check",
        ])
        .output()
        .expect("failed to run binary");

    assert!(
        output.status.success(),
        "config-check should pass for valid fixture. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn cli_emit_dockerfile_produces_output() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_dfe-transform-vector"))
        .arg("emit-dockerfile")
        .output()
        .expect("failed to run binary");

    assert!(output.status.success(), "emit-dockerfile failed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("FROM ubuntu:24.04"),
        "emit-dockerfile should produce a Dockerfile"
    );
}

#[test]
fn cli_emit_contract_produces_json() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_dfe-transform-vector"))
        .arg("emit-contract")
        .output()
        .expect("failed to run binary");

    assert!(output.status.success(), "emit-contract failed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("dfe-transform-vector"),
        "emit-contract should produce JSON with app name"
    );
}

#[test]
fn cli_emit_compose_produces_output() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_dfe-transform-vector"))
        .arg("emit-compose")
        .output()
        .expect("failed to run binary");

    assert!(output.status.success(), "emit-compose failed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("dfe-transform-vector"),
        "emit-compose should produce compose fragment"
    );
}

#[test]
fn cli_emit_chart_produces_chart_dir() {
    let dir = tempfile::tempdir().unwrap();
    let chart_dir = dir.path().join("chart");

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_dfe-transform-vector"))
        .args(["emit-chart", chart_dir.to_str().unwrap()])
        .output()
        .expect("failed to run binary");

    assert!(
        output.status.success(),
        "emit-chart failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(chart_dir.join("Chart.yaml").exists(), "Chart.yaml missing");
    assert!(
        chart_dir.join("values.yaml").exists(),
        "values.yaml missing"
    );
}
