// Project:   dfe-transform-vector
// File:      src/vector/process.rs
// Purpose:   Vector subprocess spawning and management
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Vector subprocess spawning and management.
//!
//! Spawns Vector as a child process with `--config-dir`, inherits
//! stdout/stderr for log pass-through, and provides signal forwarding
//! and crash recovery with exponential backoff.

use std::path::Path;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use tokio::process::{Child, Command};
use tracing::{debug, error, info, warn};

use super::lifecycle::{Lifecycle, State};
use crate::Result;
use crate::config::VectorConfig;

/// Backoff configuration for crash recovery.
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    /// Initial backoff duration.
    pub initial: Duration,
    /// Maximum backoff duration.
    pub max: Duration,
    /// Multiplier applied on each consecutive crash.
    pub multiplier: f64,
    /// Duration of healthy running that resets the backoff.
    pub reset_after: Duration,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(60),
            multiplier: 2.0,
            reset_after: Duration::from_secs(300),
        }
    }
}

/// Spawn Vector as a child process.
///
/// Runs: `<binary> --config-dir <config_dir> --watch-config poll`
/// with stdout/stderr inherited (Vector logs pass through to pod stdout).
pub fn spawn_vector(vector_config: &VectorConfig, config_dir: &Path) -> Result<Child> {
    info!(
        binary = %vector_config.binary,
        config_dir = %config_dir.display(),
        log_level = %vector_config.log_level,
        "spawning Vector subprocess"
    );

    let mut cmd = Command::new(&vector_config.binary);
    cmd.arg("--config-dir")
        .arg(config_dir)
        .arg("--watch-config")
        .arg("poll");

    // Pass through Vector-specific log level
    if !vector_config.log_level.is_empty() {
        cmd.env("VECTOR_LOG", &vector_config.log_level);
    }

    // Data directory
    if !vector_config.data_dir.is_empty() {
        cmd.arg("--data-dir").arg(&vector_config.data_dir);
    }

    // Vector API address
    if !vector_config.api_address.is_empty() {
        cmd.env("VECTOR_API_ADDRESS", &vector_config.api_address);
        cmd.env("VECTOR_API_ENABLED", "true");
    }

    cmd.stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);

    let child = cmd
        .spawn()
        .map_err(|e| crate::Error::Vector(format!("failed to spawn Vector: {e}")))?;

    debug!(pid = child.id().unwrap_or(0), "Vector process spawned");
    Ok(child)
}

/// Send a signal to the Vector child process.
pub fn send_signal(child: &Child, sig: Signal) -> Result<()> {
    let pid = child
        .id()
        .ok_or_else(|| crate::Error::Vector("Vector process has no PID".into()))?;

    signal::kill(Pid::from_raw(pid as i32), sig)
        .map_err(|e| crate::Error::Vector(format!("failed to send {sig} to Vector: {e}")))?;

    debug!(pid, signal = %sig, "sent signal to Vector");
    Ok(())
}

/// Send SIGHUP to Vector for config reload.
pub fn reload_vector(child: &Child) -> Result<()> {
    send_signal(child, Signal::SIGHUP)
}

/// Run the Vector subprocess lifecycle loop.
///
/// Spawns Vector, monitors for crashes, and restarts with exponential
/// backoff. Exits when shutdown is requested via the cancellation token.
///
/// The `vector_pid` holder is updated with the current Vector PID whenever
/// a new child is spawned, and cleared when the child exits. The reload
/// loop uses this to send SIGHUP for config hot-reload.
pub async fn run_lifecycle(
    vector_config: &VectorConfig,
    config_dir: &Path,
    lifecycle: &Lifecycle,
    backoff: &BackoffConfig,
    vector_pid: Arc<Mutex<Option<u32>>>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let mut current_backoff = backoff.initial;
    let mut crash_count: u64 = 0;

    loop {
        // Check for shutdown before (re)starting
        if *shutdown.borrow() {
            lifecycle.set(State::ShuttingDown);
            info!("shutdown requested, not restarting Vector");
            return Ok(());
        }

        lifecycle.set(State::Starting);
        let mut child = match spawn_vector(vector_config, config_dir) {
            Ok(c) => c,
            Err(e) => {
                error!(error = %e, "failed to spawn Vector");
                lifecycle.set(State::Crashed);
                tokio::time::sleep(current_backoff).await;
                current_backoff = next_backoff(current_backoff, backoff);
                crash_count += 1;
                continue;
            }
        };

        // Publish PID for the reload loop
        *vector_pid.lock().unwrap() = child.id();

        lifecycle.set(State::Running);
        let started_at = Instant::now();

        // Wait for either: child exit or shutdown signal
        let exit_status = tokio::select! {
            status = child.wait() => {
                *vector_pid.lock().unwrap() = None;
                match status {
                    Ok(s) => s,
                    Err(e) => {
                        error!(error = %e, "error waiting for Vector process");
                        lifecycle.set(State::Crashed);
                        crash_count += 1;
                        tokio::time::sleep(current_backoff).await;
                        current_backoff = next_backoff(current_backoff, backoff);
                        continue;
                    }
                }
            }
            _ = shutdown.changed() => {
                // Shutdown requested — forward SIGTERM to Vector
                *vector_pid.lock().unwrap() = None;
                lifecycle.set(State::ShuttingDown);
                info!("shutdown requested, sending SIGTERM to Vector");
                let _ = send_signal(&child, Signal::SIGTERM);

                // Wait for child to exit with timeout
                let timeout = Duration::from_secs(55);
                match tokio::time::timeout(timeout, child.wait()).await {
                    Ok(Ok(status)) => {
                        info!(exit_code = status.code().unwrap_or(-1), "Vector exited after SIGTERM");
                    }
                    Ok(Err(e)) => {
                        error!(error = %e, "error waiting for Vector shutdown");
                    }
                    Err(_) => {
                        warn!("Vector did not exit within {timeout:?}, sending SIGKILL");
                        let _ = child.kill().await;
                    }
                }
                return Ok(());
            }
        };

        // Vector exited unexpectedly
        let uptime = started_at.elapsed();
        let exit_code = exit_status.code().unwrap_or(-1);
        crash_count += 1;

        error!(
            exit_code,
            uptime_secs = uptime.as_secs(),
            crash_count,
            "Vector exited unexpectedly"
        );

        lifecycle.set(State::Crashed);

        // Reset backoff if Vector ran long enough
        if uptime >= backoff.reset_after {
            current_backoff = backoff.initial;
            debug!("backoff reset after sustained healthy run");
        }

        info!(
            backoff_secs = current_backoff.as_secs_f64(),
            crash_count, "restarting Vector after backoff"
        );
        tokio::time::sleep(current_backoff).await;
        current_backoff = next_backoff(current_backoff, backoff);
    }
}

/// Calculate the next backoff duration.
fn next_backoff(current: Duration, config: &BackoffConfig) -> Duration {
    let next = current.mul_f64(config.multiplier);
    next.min(config.max)
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles() {
        let config = BackoffConfig::default();
        let next = next_backoff(Duration::from_secs(1), &config);
        assert_eq!(next, Duration::from_secs(2));
    }

    #[test]
    fn backoff_caps_at_max() {
        let config = BackoffConfig {
            max: Duration::from_secs(10),
            ..Default::default()
        };
        let next = next_backoff(Duration::from_secs(8), &config);
        assert_eq!(next, Duration::from_secs(10));
    }

    #[test]
    fn backoff_already_at_max() {
        let config = BackoffConfig {
            max: Duration::from_secs(60),
            ..Default::default()
        };
        let next = next_backoff(Duration::from_secs(60), &config);
        assert_eq!(next, Duration::from_secs(60));
    }
}
