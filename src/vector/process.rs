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

/// How long a freshly spawned Vector must survive before it counts as running.
///
/// A successful spawn only means fork/exec worked; a bad argv or an unreadable
/// config exits within milliseconds, and without this window every such crash
/// is advertised as a healthy start.
pub const SPAWN_SETTLE: Duration = Duration::from_millis(500);

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

/// The argv Vector is started with.
///
/// No `--watch-config`: the reload loop is the only thing that changes what
/// Vector runs. It validates a staged directory, swaps it in whole and sends
/// SIGHUP, and a watcher of Vector's own would load the directory on its own
/// schedule, ahead of that validation.
///
/// Never `--dangerously-allow-env-var-interpolation`: it would expand `${VAR}`
/// in operator transforms from the pod environment, credentials included.
#[must_use]
pub fn vector_args(config_dir: &Path) -> Vec<std::ffi::OsString> {
    vec!["--config-dir".into(), config_dir.as_os_str().to_owned()]
}

/// A command for the Vector binary at `binary`, without this process's own
/// `DFE_TRANSFORM_*` variables.
///
/// Those carry the Kafka credentials, and VRL's `get_env_var` hands Vector's
/// environment to any transform. Vector reads the credentials from its secret
/// backend instead.
#[must_use]
pub fn vector_command(binary: &str) -> Command {
    let own_prefix = format!("{}_", crate::config::loader::ENV_PREFIX);
    let mut cmd = Command::new(binary);
    for (name, _) in std::env::vars_os() {
        if name.as_encoded_bytes().starts_with(own_prefix.as_bytes()) {
            cmd.env_remove(name);
        }
    }
    cmd
}

/// Sleep for `wait`, cut short by a shutdown request, which the caller's next
/// shutdown check then sees.
async fn sleep_unless_shutdown(wait: Duration, shutdown: &mut tokio::sync::watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    tokio::select! {
        () = tokio::time::sleep(wait) => {}
        _ = shutdown.changed() => {}
    }
}

/// Spawn Vector as a child process.
///
/// stdout/stderr are inherited, so Vector's logs pass through to pod stdout.
///
/// # Errors
///
/// Returns [`crate::Error::Vector`] when the process cannot be spawned.
pub fn spawn_vector(vector_config: &VectorConfig, config_dir: &Path) -> Result<Child> {
    info!(
        binary = %vector_config.binary,
        config_dir = %config_dir.display(),
        log_level = %vector_config.log_level,
        "spawning Vector subprocess"
    );

    let mut cmd = vector_command(&vector_config.binary);
    cmd.args(vector_args(config_dir));

    // Pass through Vector-specific log level
    if !vector_config.log_level.is_empty() {
        cmd.env("VECTOR_LOG", &vector_config.log_level);
    }

    // Data directory (Vector has no --data-dir CLI flag; env var only)
    if !vector_config.data_dir.is_empty() {
        cmd.env("VECTOR_DATA_DIR", &vector_config.data_dir);
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
///
/// # Errors
///
/// Returns [`crate::Error::Vector`] when the child has no PID or the signal cannot be delivered.
pub fn send_signal(child: &Child, sig: Signal) -> Result<()> {
    let pid = child
        .id()
        .ok_or_else(|| crate::Error::Vector("Vector process has no PID".into()))?;

    let raw = i32::try_from(pid).map_err(|_| {
        crate::Error::Vector(format!("Vector PID {pid} does not fit a signal target"))
    })?;
    signal::kill(Pid::from_raw(raw), sig)
        .map_err(|e| crate::Error::Vector(format!("failed to send {sig} to Vector: {e}")))?;

    debug!(pid, signal = %sig, "sent signal to Vector");
    Ok(())
}

/// Send SIGHUP to Vector for config reload.
///
/// # Errors
///
/// Returns [`crate::Error::Vector`] when the signal cannot be delivered.
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
///
/// # Errors
///
/// None in practice: a spawn failure is logged and retried after backoff, and the loop returns `Ok(())` once shutdown is requested.
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
                sleep_unless_shutdown(current_backoff, &mut shutdown).await;
                current_backoff = next_backoff(current_backoff, backoff);
                crash_count += 1;
                continue;
            }
        };

        // Publish PID for the reload loop
        *vector_pid
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = child.id();

        let started_at = Instant::now();

        // Promote to Running only once the child has survived the settle
        // window; a child that is already gone stays Starting and falls
        // through to the single crash path below.
        tokio::time::sleep(SPAWN_SETTLE).await;
        match child.try_wait() {
            // Still there: the only case that earns `Running`.
            Ok(None) => {
                lifecycle.set(State::Running);
                if log_state_change(&VECTOR_RUNNING, true) {
                    info!("Vector subprocess is running");
                }
            }
            // Already gone -- a crash-on-start, handled by the crash path below.
            Ok(Some(status)) => {
                warn!(
                    settle_ms = SPAWN_SETTLE.as_millis(),
                    exit_code = status.code().unwrap_or(-1),
                    "Vector did not survive the settle window, not reporting running"
                );
            }
            // Whether the child is alive is unknown, so it does not earn
            // `Running`; the `child.wait()` below decides.
            Err(e) => {
                error!(
                    error = %e,
                    settle_ms = SPAWN_SETTLE.as_millis(),
                    "could not tell whether Vector survived the settle window"
                );
            }
        }

        // Wait for either: child exit or shutdown signal
        let exit_status = tokio::select! {
            status = child.wait() => {
                *vector_pid.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = None;
                match status {
                    Ok(s) => s,
                    Err(e) => {
                        error!(error = %e, "error waiting for Vector process");
                        lifecycle.set(State::Crashed);
                        crashes_total.increment(1);
                        crash_count += 1;
                        sleep_unless_shutdown(current_backoff, &mut shutdown).await;
                        current_backoff = next_backoff(current_backoff, backoff);
                        continue;
                    }
                }
            }
            _ = shutdown.changed() => {
                // Shutdown requested — forward SIGTERM to Vector
                *vector_pid.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = None;
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
        sleep_unless_shutdown(current_backoff, &mut shutdown).await;
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

    /// A watcher in Vector reloads the directory whenever it changes, ahead of
    /// the reload loop's validation and without a SIGHUP the loop can answer for.
    #[test]
    fn vector_watches_no_config_of_its_own() {
        let args: Vec<String> = vector_args(Path::new("/tmp/assembled"))
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert_eq!(args, ["--config-dir", "/tmp/assembled"], "{args:?}");
    }

    /// VRL's `get_env_var` reads Vector's environment, so the supervisor's own
    /// variables, the Kafka credentials among them, reach no Vector process:
    /// not the `--version` check, not `validate`, not the running one.
    #[tokio::test]
    #[allow(unsafe_code)]
    async fn no_vector_process_sees_the_supervisors_own_variables() {
        use std::os::unix::fs::PermissionsExt;

        // SAFETY: nextest runs each test in its own process, and nothing else
        // in this one touches the environment.
        unsafe {
            std::env::set_var("DFE_TRANSFORM_SINK_SASL_PASSWORD", "probe-credential");
            std::env::set_var("DFE_TRANSFORM_PROBE_OTHER", "probe-other");
            std::env::set_var("PROBE_OPERATOR_VARIABLE", "kept");
        }
        let dir = tempfile::tempdir().unwrap();
        let config_dir = dir.path().join("config");
        std::fs::create_dir(&config_dir).unwrap();
        let script = dir.path().join("vector");
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\nenv > \"{}/env-$1.txt\"\necho 'vector 0.0.0 (stand-in)'\n",
                dir.path().display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let config = VectorConfig {
            binary: script.to_string_lossy().into_owned(),
            version: "0.0.0".into(),
            version_check: "strict".into(),
            ..VectorConfig::default()
        };

        crate::config::validate::check_vector_version(&config)
            .await
            .unwrap();
        crate::config::validate::vector_validate(&config, &config_dir)
            .await
            .unwrap();
        spawn_vector(&config, &config_dir)
            .unwrap()
            .wait()
            .await
            .unwrap();

        for argv0 in ["--version", "validate", "--config-dir"] {
            let seen = std::fs::read_to_string(dir.path().join(format!("env-{argv0}.txt")))
                .unwrap_or_else(|e| panic!("the {argv0} run wrote no environment: {e}"));
            assert!(
                !seen.contains("probe-credential") && !seen.contains("probe-other"),
                "the {argv0} run saw a DFE_TRANSFORM_ variable"
            );
            assert!(
                seen.contains("PROBE_OPERATOR_VARIABLE=kept"),
                "the {argv0} run lost the rest of the environment"
            );
        }
    }

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
