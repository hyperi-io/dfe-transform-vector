// Project:   dfe-transform-vector
// File:      tests/integration/signals.rs
// Purpose:   The running supervisor answers SIGTERM whatever SIGHUPs came first
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Signal handling on the shipped binary, with a Vector stand-in.

use std::process::Stdio;
use std::time::{Duration, Instant};

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use tempfile::TempDir;
use tokio::process::Command;

use super::scaling::write_run_dir;
use crate::common::{free_port, reqwest_lite};

/// How long the binary has to come up and report the subprocess running.
const STARTUP: Duration = Duration::from_secs(30);

/// How long SIGTERM may take to end a supervisor whose stand-in exits on TERM.
const STOP: Duration = Duration::from_secs(10);

/// The run dir turns hot reload off, so nothing reads the reload channel: a
/// burst of SIGHUPs past its capacity must still leave SIGTERM answered.
#[tokio::test]
async fn a_burst_of_sighups_never_holds_up_sigterm() {
    let dir = TempDir::new().expect("run dir");
    let metrics_addr = free_port().await;
    let exporter_addr = free_port().await;
    let config = write_run_dir(dir.path(), &metrics_addr, &exporter_addr);

    let mut child = Command::new(env!("CARGO_BIN_EXE_dfe-transform-vector"))
        .arg("--config")
        .arg(&config)
        .arg("--metrics-addr")
        .arg(&metrics_addr)
        .arg("run")
        .env("DFE_TRANSFORM_VERSION_CHECK__ENABLED", "false")
        .current_dir(dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .expect("start dfe-transform-vector");
    let pid = Pid::from_raw(
        child
            .id()
            .and_then(|id| i32::try_from(id).ok())
            .expect("child pid"),
    );

    // Ready means the subprocess runs, and the signal loop started before it.
    let deadline = Instant::now() + STARTUP;
    let mut ready = false;
    while !ready && Instant::now() < deadline {
        if tokio::net::TcpStream::connect(&metrics_addr).await.is_ok() {
            ready = reqwest_lite(&format!("http://{metrics_addr}/readyz"))
                .await
                .0
                == 200;
        }
        if !ready {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    assert!(
        ready,
        "the supervisor never reported ready within {STARTUP:?}"
    );

    for _ in 0..8 {
        kill(pid, Signal::SIGHUP).expect("send SIGHUP");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    kill(pid, Signal::SIGTERM).expect("send SIGTERM");

    let stopped = tokio::time::timeout(STOP, child.wait()).await;
    if stopped.is_err() {
        let _ = child.start_kill();
        let _ = child.wait().await;
    }
    assert!(
        matches!(stopped, Ok(Ok(status)) if status.success()),
        "SIGTERM after 8 SIGHUPs did not stop the supervisor cleanly within {STOP:?}: {stopped:?}"
    );
}
