// Project:   dfe-transform-vector
// File:      src/vector/process.rs
// Purpose:   Vector subprocess spawning and management
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Vector subprocess spawning and management.
//!
//! Spawns Vector as a child process with `--config-dir`, inherits
//! stdout/stderr for log pass-through, and provides signal forwarding
//! and crash recovery with exponential backoff.

use std::path::Path;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use metrics::Counter;
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use scalo::logger::security::{SecurityEvent, SecurityOutcome};
use scalo::logger::{log_debounced, log_state_change};
use tokio::process::{Child, Command};
use tracing::{debug, error, info, warn};

/// Log spam protection: only log crash events at most once per 10 seconds.
static CRASH_LOG_DEBOUNCE: AtomicU64 = AtomicU64::new(0);

/// Log spam protection: only log state transitions, not every check cycle.
static VECTOR_RUNNING: AtomicBool = AtomicBool::new(false);

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

    // Data directory (Vector has no --data-dir CLI flag; env var only)
    if !vector_config.data_dir.is_empty() {
        cmd.env("VECTOR_DATA_DIR", &vector_config.data_dir);
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
///
/// `crashes_total` / `restarts_total` are the authoritative Prometheus
/// counters for Vector subprocess health: incremented HERE, at the source,
/// every time the subprocess crashes and every time it is re-spawned. They
/// are counted here rather than off the lifecycle watch channel because a
/// fast Crashed->Starting->Running transition would be coalesced and lose a
/// restart. (The scaling circuit gate, which only cares about the LATEST
/// state, drives off the watch channel -- see `spawn_circuit_gate_task`.)
///
/// Uses `std::sync::Mutex` intentionally — the lock is held for sub-microsecond
/// reads/writes of a `u32` and is never held across an `.await` point.
/// `tokio::sync::Mutex` is unnecessary overhead for this pattern.
#[allow(clippy::too_many_arguments)]
pub async fn run_lifecycle(
    vector_config: &VectorConfig,
    config_dir: &Path,
    lifecycle: &Lifecycle,
    backoff: &BackoffConfig,
    vector_pid: Arc<Mutex<Option<u32>>>,
    crashes_total: Counter,
    restarts_total: Counter,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let mut current_backoff = backoff.initial;
    let mut crash_count: u64 = 0;
    // True once Vector has been spawned at least once, so the FIRST spawn is
    // the initial launch and each subsequent spawn is a restart.
    let mut spawned_once = false;

    loop {
        // Check for shutdown before (re)starting
        if *shutdown.borrow() {
            lifecycle.set(State::ShuttingDown);
            info!("shutdown requested, not restarting Vector");
            return Ok(());
        }

        // A (re)spawn after the first launch is a restart.
        if spawned_once {
            restarts_total.increment(1);
        }
        spawned_once = true;

        lifecycle.set(State::Starting);
        let mut child = match spawn_vector(vector_config, config_dir) {
            Ok(c) => c,
            Err(e) => {
                error!(error = %e, "failed to spawn Vector");
                lifecycle.set(State::Crashed);
                crashes_total.increment(1);
                tokio::time::sleep(current_backoff).await;
                current_backoff = next_backoff(current_backoff, backoff);
                crash_count += 1;
                continue;
            }
        };

        // Publish PID for the reload loop
        *vector_pid.lock().unwrap_or_else(|p| p.into_inner()) = child.id();

        lifecycle.set(State::Running);
        if log_state_change(&VECTOR_RUNNING, true) {
            info!("Vector subprocess is running");
        }
        let started_at = Instant::now();

        // Wait for either: child exit or shutdown signal
        let exit_status = tokio::select! {
            status = child.wait() => {
                *vector_pid.lock().unwrap_or_else(|p| p.into_inner()) = None;
                match status {
                    Ok(s) => s,
                    Err(e) => {
                        error!(error = %e, "error waiting for Vector process");
                        lifecycle.set(State::Crashed);
                        crashes_total.increment(1);
                        crash_count += 1;
                        tokio::time::sleep(current_backoff).await;
                        current_backoff = next_backoff(current_backoff, backoff);
                        continue;
                    }
                }
            }
            _ = shutdown.changed() => {
                // Shutdown requested — forward SIGTERM to Vector
                *vector_pid.lock().unwrap_or_else(|p| p.into_inner()) = None;
                lifecycle.set(State::ShuttingDown);
                log_state_change(&VECTOR_RUNNING, false);
                info!("shutdown requested, sending SIGTERM to Vector");
                SecurityEvent::new("process.shutdown", "SIGTERM_forward", SecurityOutcome::Success)
                    .actor("system")
                    .detail("graceful shutdown initiated")
                    .emit();
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
        crashes_total.increment(1);
        crash_count += 1;

        // Log spam protection: debounce crash logs in tight restart loops
        if log_debounced(&CRASH_LOG_DEBOUNCE, 10_000) {
            error!(
                exit_code,
                uptime_secs = uptime.as_secs(),
                crash_count,
                "Vector exited unexpectedly"
            );
        }

        // Security audit trail: deliberately NOT debounced — every crash
        // is a security-relevant event for the audit log
        SecurityEvent::new("process.crash", "vector_subprocess", SecurityOutcome::Error)
            .reason(&format!("exit_code={exit_code}"))
            .detail(&format!(
                "crash_count={crash_count}, uptime_secs={}",
                uptime.as_secs()
            ))
            .emit();

        log_state_change(&VECTOR_RUNNING, false);
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
