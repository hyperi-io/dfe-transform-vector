// Project:   dfe-transform-vector
// File:      tests/integration/scaling.rs
// Purpose:   The running supervisor exports scalo's scaling pressure gauge
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The scaling pressure a KEDA pressure trigger reads, off a running binary.
//!
//! scalo's runtime writes the pressure `/scaling/pressure` serves into the
//! `scaling_pressure` gauge once a second, so the scrape and the OTLP push both
//! carry it. This starts the shipped binary on the bus transport with a Vector
//! stand-in and reads both off the ops port.

use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, Instant};

use dfe_transform_vector::metrics::scrape::fetch_exposition;
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use tempfile::TempDir;
use tokio::process::{Child, Command};

use crate::common::{free_port, reqwest_lite};

/// How long the binary has to bind its ops port and write the gauge.
const STARTUP: Duration = Duration::from_secs(30);

/// The gauge's name with no `metrics.namespace` set, as scalo registers it.
const GAUGE: &str = "scaling_pressure";

/// A Vector stand-in: answers `--version` and `validate`, then idles until
/// TERM. It exits on its own after two minutes, so a run killed before it can
/// forward TERM leaves nothing behind.
const VECTOR_STAND_IN: &str = r#"#!/usr/bin/env bash
case "${1:-}" in
    --version) echo "vector 0.0.0 (dfe-transform-vector scaling test stand-in)"; exit 0 ;;
    validate)  exit 0 ;;
esac
trap 'exit 0' TERM INT
for _ in $(seq 1 120); do
    sleep 1
done
"#;

/// Write the stand-in, a no-op transform and a bus-transport config into `dir`.
fn write_run_dir(dir: &Path, metrics_addr: &str, exporter_addr: &str) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt;

    let stand_in = dir.join("vector-stand-in.sh");
    std::fs::write(&stand_in, VECTOR_STAND_IN).expect("write the Vector stand-in");
    std::fs::set_permissions(&stand_in, std::fs::Permissions::from_mode(0o755))
        .expect("make the stand-in executable");

    let transforms = dir.join("transforms");
    std::fs::create_dir_all(&transforms).expect("transforms dir");
    std::fs::write(
        transforms.join("00_no_op.yaml"),
        "transforms:\n  dfe_transform:\n    type: remap\n    inputs:\n      - dfe_source\n    source: \".\"\n",
    )
    .expect("write the transform");

    let config = dir.join("config.yaml");
    std::fs::write(
        &config,
        format!(
            r#"pipeline:
  name: "scaling-gauge"
sink:
  topic: "scaling_load"
transforms:
  dir: "{transforms}"
vector:
  binary: "{stand_in}"
  data_dir: "{data}"
  config_dir: "{vector_config}"
  version: ""
  version_check: "disabled"
metrics:
  address: "{metrics_addr}"
  vector_metrics_address: "{exporter_addr}"
logging:
  level: "warn"
  format: "json"
reload:
  enabled: false
"#,
            transforms = transforms.display(),
            stand_in = stand_in.display(),
            data = dir.join("data").display(),
            vector_config = dir.join("vector-config").display(),
        ),
    )
    .expect("write the config");
    config
}

/// Stop the supervisor with TERM, so it stops the stand-in in turn, and only
/// then fall back to KILL.
async fn stop(mut child: Child) {
    if let Some(pid) = child.id().and_then(|id| i32::try_from(id).ok()) {
        let _ = kill(Pid::from_raw(pid), Signal::SIGTERM);
    }
    if tokio::time::timeout(Duration::from_secs(15), child.wait())
        .await
        .is_err()
    {
        let _ = child.start_kill();
        let _ = child.wait().await;
    }
}

/// The value of the `name` sample in a Prometheus exposition, with or without
/// labels.
fn sample(exposition: &str, name: &str) -> Option<f64> {
    exposition
        .lines()
        .filter(|line| {
            line.strip_prefix(name)
                .is_some_and(|rest| rest.starts_with(' ') || rest.starts_with('{'))
        })
        .find_map(|line| line.rsplit(' ').next()?.parse().ok())
}

#[tokio::test]
async fn the_running_supervisor_exports_the_scaling_pressure_gauge() {
    let dir = TempDir::new().expect("run dir");
    let metrics_addr = free_port().await;
    // Nothing listens here: the merge of Vector's exporter fails and is counted,
    // which leaves the runtime's own metrics untouched.
    let exporter_addr = free_port().await;
    let config = write_run_dir(dir.path(), &metrics_addr, &exporter_addr);

    let child = Command::new(env!("CARGO_BIN_EXE_dfe-transform-vector"))
        .arg("--config")
        .arg(&config)
        .arg("--metrics-addr")
        .arg(&metrics_addr)
        .arg("run")
        // No phone-home from a test run, and no .env from the checkout.
        .env("DFE_TRANSFORM_VERSION_CHECK__ENABLED", "false")
        .current_dir(dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .expect("start dfe-transform-vector");

    let deadline = Instant::now() + STARTUP;
    let mut gauge = None;
    let mut last = String::new();
    while gauge.is_none() && Instant::now() < deadline {
        match fetch_exposition(&metrics_addr).await {
            Ok(body) => {
                gauge = sample(&body, GAUGE);
                last = body;
            }
            Err(e) => last = e,
        }
        if gauge.is_none() {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    let served = if gauge.is_some() {
        Some(reqwest_lite(&format!("http://{metrics_addr}/scaling/pressure")).await)
    } else {
        None
    };
    stop(child).await;

    let gauge = gauge.unwrap_or_else(|| {
        panic!("no `{GAUGE}` sample on /metrics within {STARTUP:?}; last read:\n{last}")
    });
    assert!(
        last.contains(&format!("# TYPE {GAUGE} gauge")),
        "`{GAUGE}` is not exported as a gauge:\n{last}"
    );
    assert!(
        (0.0..=100.0).contains(&gauge),
        "{GAUGE} {gauge} is off scale"
    );

    let (status, body) = served.expect("read /scaling/pressure");
    assert_eq!(status, 200, "/scaling/pressure answered {status}: {body}");
    let pressure: f64 = body
        .trim()
        .parse()
        .unwrap_or_else(|_| panic!("/scaling/pressure is not a number: {body:?}"));
    assert!(
        (0.0..=100.0).contains(&pressure),
        "/scaling/pressure {pressure} is off scale"
    );
}
