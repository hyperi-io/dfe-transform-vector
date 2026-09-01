// Project:   dfe-transform-vector
// File:      src/config/reload.rs
// Purpose:   Hot-reload: poll-based file watcher, change classification, reload workflow
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Hot-reload support for transform config changes.
//!
//! Polls transform files and big-dial config at a configurable interval.
//! When changes are detected, classifies them as safe (transforms only)
//! or unsafe (source/sink changes), then triggers the appropriate reload.
//!
//! Uses polling instead of inotify because S3-backed mounts (s3fs, goofys,
//! Mountpoint for S3) do not generate filesystem notification events.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use scalo::logger::security;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use super::assembler;
use super::loader::Config;
use super::validate::vector_validate;
use crate::vector::lifecycle::{Lifecycle, State};

/// Result of classifying what changed between two configs.
#[derive(Debug, PartialEq)]
pub enum ChangeKind {
    /// No changes detected.
    None,
    /// Only transform files changed — safe for hot-reload via SIGHUP.
    TransformsOnly,
    /// Source, sink, or other infrastructure changed — requires full restart.
    Unsafe,
}

/// Classify the difference between old and new config.
///
/// Uses an **allowlist** pattern: only `transforms` changes are safe for
/// hot-reload. All other config fields require a pod restart because their
/// values are consumed at startup and not re-read at runtime.
///
/// This is intentionally conservative — any new config fields added in the
/// future will default to "requires restart" until explicitly allowed here.
pub fn classify_change(old: &Config, new: &Config) -> ChangeKind {
    if old == new {
        return ChangeKind::None;
    }

    // Allowlist: only transform changes can be hot-reloaded.
    // Everything else is bound at startup (servers, connections, labels).
    let only_transforms_changed = old.source == new.source
        && old.sink == new.sink
        && old.pipeline == new.pipeline
        && old.vector == new.vector
        && old.metrics == new.metrics
        && old.logging == new.logging
        && old.scaling == new.scaling
        && old.reload == new.reload;

    if only_transforms_changed {
        ChangeKind::TransformsOnly
    } else {
        ChangeKind::Unsafe
    }
}

/// Snapshot of file modification times for change detection.
#[derive(Debug, Clone)]
struct FileSnapshot {
    mtimes: HashMap<PathBuf, SystemTime>,
}

impl FileSnapshot {
    /// Take a snapshot of modification times for all relevant files.
    fn capture(config: &Config) -> Self {
        let mut mtimes = HashMap::new();

        // Snapshot transform files
        if let Some(dir) = &config.transforms.dir {
            let dir_path = Path::new(dir);
            if dir_path.is_dir()
                && let Ok(entries) = std::fs::read_dir(dir_path)
            {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().is_some_and(|e| e == "yaml" || e == "yml")
                        && let Ok(meta) = path.metadata()
                        && let Ok(mtime) = meta.modified()
                    {
                        mtimes.insert(path, mtime);
                    }
                }
            }
        }

        if let Some(files) = &config.transforms.files {
            for file in files {
                let path = PathBuf::from(file);
                if let Ok(meta) = path.metadata()
                    && let Ok(mtime) = meta.modified()
                {
                    mtimes.insert(path, mtime);
                }
            }
        }

        Self { mtimes }
    }

    /// Check if any files have changed since this snapshot.
    fn has_changed(&self, other: &FileSnapshot) -> bool {
        self.mtimes != other.mtimes
    }
}

/// Reload trigger — either from the file watcher or a manual SIGHUP.
#[derive(Debug)]
pub enum ReloadTrigger {
    /// File changes detected by the poll-based watcher.
    FileChange,
    /// Manual SIGHUP received.
    Manual,
}

/// Send SIGHUP to Vector by PID for config reload.
fn send_sighup(pid: u32) -> crate::Result<()> {
    signal::kill(Pid::from_raw(pid as i32), Signal::SIGHUP).map_err(|e| {
        crate::Error::Vector(format!("failed to send SIGHUP to Vector PID {pid}: {e}"))
    })?;
    debug!(pid, "sent SIGHUP to Vector for config reload");
    Ok(())
}

/// Run the config reload loop.
///
/// Polls for file changes at the configured interval and listens for
/// manual reload triggers via the channel. When changes are detected:
///
/// 1. Re-load config (if big-dial config changed)
/// 2. Classify the change (safe vs unsafe)
/// 3. If safe: re-assemble, re-validate, write config dir, SIGHUP Vector
/// 4. If unsafe: log warning (full restart requires pod recreation)
#[allow(clippy::too_many_arguments)]
pub async fn run_reload_loop(
    config: Config,
    config_path: Option<String>,
    config_dir: PathBuf,
    lifecycle: Lifecycle,
    metrics: Arc<crate::metrics::WrapperMetrics>,
    vector_pid: Arc<Mutex<Option<u32>>>,
    mut reload_rx: mpsc::Receiver<ReloadTrigger>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let poll_interval = Duration::from_secs(config.reload.poll_interval_secs);
    let mut current_config = config;
    let mut snapshot = FileSnapshot::capture(&current_config);

    info!(
        poll_interval_secs = current_config.reload.poll_interval_secs,
        "config reload loop started"
    );

    loop {
        // Wait for either: poll interval, manual trigger, or shutdown
        let trigger = tokio::select! {
            _ = tokio::time::sleep(poll_interval) => {
                // Check for file changes
                let new_snapshot = FileSnapshot::capture(&current_config);
                if !snapshot.has_changed(&new_snapshot) {
                    continue;
                }
                snapshot = new_snapshot;
                ReloadTrigger::FileChange
            }
            Some(trigger) = reload_rx.recv() => trigger,
            _ = shutdown.changed() => {
                info!("reload loop shutting down");
                return;
            }
        };

        info!(trigger = ?trigger, "reload triggered");

        // Re-load config from file
        let new_config = match Config::load(config_path.as_deref()) {
            Ok(c) => c,
            Err(e) => {
                error!(error = %e, "failed to re-load config during reload");
                metrics.record_config_reload("error");
                metrics.record_config_validation_error();
                continue;
            }
        };

        // Validate new config
        if let Err(e) = new_config.validate() {
            error!(error = %e, "new config validation failed during reload");
            metrics.record_config_reload("error");
            metrics.record_config_validation_error();
            security::input_validation_failure("config_reload", &e.to_string(), None);
            continue;
        }

        // Classify the change
        let change = classify_change(&current_config, &new_config);
        match change {
            ChangeKind::None => {
                debug!("config reload triggered but no effective changes detected");
                continue;
            }
            ChangeKind::Unsafe => {
                warn!(
                    "non-transform config changed — requires pod restart. \
                     Only transform YAML file changes can be hot-reloaded."
                );
                metrics.record_config_reload("rejected");
                continue;
            }
            ChangeKind::TransformsOnly => {
                info!("transform-only change detected, proceeding with hot-reload");
            }
        }

        // Set lifecycle to Reloading
        lifecycle.set(State::Reloading);

        // Re-assemble config directory
        if let Err(e) = assembler::assemble(&new_config, &config_dir) {
            error!(error = %e, "failed to re-assemble config during reload");
            metrics.record_config_reload("error");
            lifecycle.set(State::Running);
            continue;
        }

        // Re-validate with vector validate
        if let Err(e) = vector_validate(&new_config.vector, &config_dir).await {
            error!(error = %e, "vector validate failed during reload");
            metrics.record_config_reload("error");
            metrics.record_config_validation_error();
            // Re-assemble with old config to restore
            let _ = assembler::assemble(&current_config, &config_dir);
            lifecycle.set(State::Running);
            continue;
        }

        // Send SIGHUP to Vector to pick up the new config
        let pid = vector_pid
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .copied();
        match pid {
            Some(pid) => {
                if let Err(e) = send_sighup(pid) {
                    error!(error = %e, "failed to send SIGHUP to Vector");
                    metrics.record_config_reload("error");
                    lifecycle.set(State::Running);
                    continue;
                }
            }
            None => {
                warn!("Vector process not running, skipping SIGHUP");
                lifecycle.set(State::Running);
                continue;
            }
        }

        // Update state
        current_config = new_config;
        snapshot = FileSnapshot::capture(&current_config);
        lifecycle.set(State::Running);

        info!("config hot-reload completed successfully");
        metrics.record_config_reload("success");
        security::config_changed(
            "config_reload",
            "system",
            "transform config reloaded via SIGHUP",
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::loader::*;

    fn base_config() -> Config {
        Config {
            pipeline: PipelineConfig {
                name: "test".into(),
            },
            source: SourceConfig {
                brokers: vec!["kafka:9092".into()],
                topics: vec!["input".into()],
                group_id: "test-group".into(),
                ..Default::default()
            },
            sink: SinkConfig {
                brokers: vec!["kafka:9092".into()],
                topic: "output".into(),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn classify_no_change() {
        let config = base_config();
        assert_eq!(classify_change(&config, &config.clone()), ChangeKind::None);
    }

    #[test]
    fn classify_transforms_only() {
        let old = base_config();
        let mut new = old.clone();
        new.transforms.dir = Some("/new/transforms".into());
        assert_eq!(classify_change(&old, &new), ChangeKind::TransformsOnly);
    }

    #[test]
    fn classify_source_change_is_unsafe() {
        let old = base_config();
        let mut new = old.clone();
        new.source.brokers = vec!["different-kafka:9092".into()];
        assert_eq!(classify_change(&old, &new), ChangeKind::Unsafe);
    }

    #[test]
    fn classify_sink_change_is_unsafe() {
        let old = base_config();
        let mut new = old.clone();
        new.sink.topic = "different-topic".into();
        assert_eq!(classify_change(&old, &new), ChangeKind::Unsafe);
    }

    #[test]
    fn classify_pipeline_change_is_unsafe() {
        let old = base_config();
        let mut new = old.clone();
        new.pipeline.name = "different-pipeline".into();
        assert_eq!(classify_change(&old, &new), ChangeKind::Unsafe);
    }

    #[test]
    fn classify_logging_change_requires_restart() {
        let old = base_config();
        let mut new = old.clone();
        new.logging.level = "debug".into();
        assert_eq!(classify_change(&old, &new), ChangeKind::Unsafe);
    }

    #[test]
    fn classify_metrics_change_requires_restart() {
        let old = base_config();
        let mut new = old.clone();
        new.metrics.address = "0.0.0.0:9999".into();
        assert_eq!(classify_change(&old, &new), ChangeKind::Unsafe);
    }

    #[test]
    fn classify_scaling_change_requires_restart() {
        let old = base_config();
        let mut new = old.clone();
        new.scaling.pressure_threshold = 0.5;
        assert_eq!(classify_change(&old, &new), ChangeKind::Unsafe);
    }

    #[test]
    fn classify_reload_change_requires_restart() {
        let old = base_config();
        let mut new = old.clone();
        new.reload.poll_interval_secs = 60;
        assert_eq!(classify_change(&old, &new), ChangeKind::Unsafe);
    }

    #[test]
    fn classify_vector_change_requires_restart() {
        let old = base_config();
        let mut new = old.clone();
        new.vector.log_level = "debug".into();
        assert_eq!(classify_change(&old, &new), ChangeKind::Unsafe);
    }

    #[test]
    fn file_snapshot_detects_no_change() {
        let config = base_config();
        let s1 = FileSnapshot::capture(&config);
        let s2 = FileSnapshot::capture(&config);
        assert!(!s1.has_changed(&s2));
    }

    #[test]
    fn file_snapshot_detects_new_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut config = base_config();
        config.transforms.dir = Some(dir.path().to_string_lossy().into_owned());

        let s1 = FileSnapshot::capture(&config);

        // Add a file
        std::fs::write(dir.path().join("new.yaml"), "test").unwrap();
        let s2 = FileSnapshot::capture(&config);

        assert!(s1.has_changed(&s2));
    }
}
