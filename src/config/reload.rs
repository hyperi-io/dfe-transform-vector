// Project:   dfe-transform-vector
// File:      src/config/reload.rs
// Purpose:   Hot-reload: poll-based file watcher, change classification, reload workflow
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Hot-reload support for transform config changes.
//!
//! Polls the main config file and every transform file at a configurable
//! interval.
//! When changes are detected, classifies them as safe (transforms only)
//! or unsafe (source/sink changes), then triggers the appropriate reload.
//!
//! Uses polling instead of inotify because S3-backed mounts (s3fs, goofys,
//! Mountpoint for S3) do not generate filesystem notification events.
//!
//! A change is assembled and validated beside the directory Vector reads, and
//! swapped in only once it passes. See [`super::staging`].
//!
//! Vector refuses a reload it cannot apply and carries on with the config it
//! was running, so the SIGHUP succeeding says nothing about the outcome. The
//! outcome is read from Vector's own reload counters, and a refused reload
//! swaps back the directory Vector is running.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use scalo::logger::security;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use super::loader::Config;
use super::staging::{Published, Staged};
use super::validate::vector_validate;
use crate::metrics::scrape::{ReloadCounts, fetch_exposition};
use crate::vector::lifecycle::{Lifecycle, State};

/// How long Vector has to report a reload: it rebuilds and health-checks every
/// changed component before counting it.
const RELOAD_CONFIRM_TIMEOUT: Duration = Duration::from_secs(60);

/// How often Vector's exporter is read while a reload is pending.
const RELOAD_CONFIRM_POLL: Duration = Duration::from_secs(1);

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
/// Only `transforms` changes are safe for hot-reload. Everything else is bound
/// at startup (servers, connections, labels) and needs a pod restart.
///
/// Asks "is this the old config with a new `transforms` block?" rather than
/// listing the siblings that must match. The list was the bug: it named the
/// eight sibling structs and never mentioned `dfe_source`, so a `dfe_source`
/// edit passed as transforms-only and the loop logged "config hot-reload
/// completed successfully" for a change it had not applied. A field added to
/// [`Config`] later joins the comparison for free, which is what the old
/// comment claimed and the old code did the opposite of.
pub fn classify_change(old: &Config, new: &Config) -> ChangeKind {
    if old == new {
        return ChangeKind::None;
    }

    let mut without_transforms = new.clone();
    without_transforms.transforms = old.transforms.clone();

    if *old == without_transforms {
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
    /// Take a snapshot of modification times for the main config files and
    /// every transform file.
    fn capture(config: &Config, main: &[PathBuf]) -> Self {
        let mut mtimes = HashMap::new();

        for path in main {
            if let Ok(mtime) = path.metadata().and_then(|meta| meta.modified()) {
                mtimes.insert(path.clone(), mtime);
            }
        }

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

/// The main config files a reload re-reads: `config_path`, or the
/// `config.yaml` and `config.yml` [`Config::load`] looks for without one.
fn main_config_files(config_path: Option<&str>) -> Vec<PathBuf> {
    config_path.map_or_else(
        || vec![PathBuf::from("config.yaml"), PathBuf::from("config.yml")],
        |path| vec![PathBuf::from(path)],
    )
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
    let raw = i32::try_from(pid).map_err(|_| {
        crate::Error::Vector(format!("Vector PID {pid} does not fit a signal target"))
    })?;
    signal::kill(Pid::from_raw(raw), Signal::SIGHUP).map_err(|e| {
        crate::Error::Vector(format!("failed to send SIGHUP to Vector PID {pid}: {e}"))
    })?;
    debug!(pid, "sent SIGHUP to Vector for config reload");
    Ok(())
}

/// Hand `Running` back at the end of a reload, but only while this loop still
/// owns the `Reloading` it set.
///
/// `owns_reloading` is false when the reload never entered `Reloading` at all
/// (the subprocess was not running when the change landed), and the
/// compare-and-set fails when the supervisor has recorded a crash mid-reload.
/// Either way the state is left as the supervisor last reported it.
fn end_reload(lifecycle: &Lifecycle, owns_reloading: bool) {
    if owns_reloading {
        lifecycle.set_if(State::Reloading, State::Running);
    }
}

/// How Vector answered a reload it was signalled for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VectorReload {
    /// Vector counted the reload as applied.
    Applied,
    /// Vector did not apply it and kept its running config, for these reasons.
    Refused(Vec<String>),
    /// Vector reported neither before the deadline, for this reason.
    Unconfirmed(String),
}

/// Read Vector's reload counters from its exporter at `address`.
///
/// # Errors
///
/// The exporter could not be read, or its exposition does not parse.
pub async fn read_reload_counts(address: &str) -> Result<ReloadCounts, String> {
    let body = fetch_exposition(address).await?;
    ReloadCounts::parse(&body)
        .ok_or_else(|| format!("{address} returned an exposition that does not parse"))
}

/// Wait for Vector to report the reload it was signalled for, reading its
/// exporter every `poll` until `timeout`.
///
/// `before` is the reading taken before the signal. A refusal counted in the
/// same reading as an applied reload wins: putting back the running config is
/// the side of the tie that cannot leave the directory ahead of Vector.
pub async fn await_vector_reload(
    address: &str,
    before: &ReloadCounts,
    timeout: Duration,
    poll: Duration,
) -> VectorReload {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let unread = match read_reload_counts(address).await {
            Ok(now) => {
                let refused = now.refused_since(before);
                if !refused.is_empty() {
                    return VectorReload::Refused(refused);
                }
                if now.applied > before.applied {
                    return VectorReload::Applied;
                }
                None
            }
            Err(e) => Some(e),
        };
        if tokio::time::Instant::now() >= deadline {
            return VectorReload::Unconfirmed(unread.unwrap_or_else(|| {
                format!("Vector counted no reload applied or refused within {timeout:?}")
            }));
        }
        tokio::time::sleep(poll).await;
    }
}

/// Swap back the config directory Vector is running.
fn roll_back(published: Published, config_dir: &Path) {
    if let Err(e) = published.roll_back() {
        error!(
            error = %e,
            dir = %config_dir.display(),
            "could not put back the config directory Vector is running"
        );
    }
}

/// Run the config reload loop.
///
/// Polls for file changes at the configured interval and listens for
/// manual reload triggers via the channel. When changes are detected:
///
/// 1. Re-load config (if big-dial config changed)
/// 2. Classify the change (safe vs unsafe)
/// 3. If safe: assemble and validate beside the config dir, swap it in, SIGHUP
///    Vector, and read Vector's reload counters for whether it applied the
///    change
/// 4. If unsafe: log warning (full restart requires pod recreation)
///
/// A change that fails assembly or validation never reaches the config
/// directory, and one Vector refuses is swapped back out.
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
    let main = main_config_files(config_path.as_deref());
    let mut current_config = config;
    let mut snapshot = FileSnapshot::capture(&current_config, &main);

    info!(
        poll_interval_secs = current_config.reload.poll_interval_secs,
        "config reload loop started"
    );

    loop {
        // Wait for either: poll interval, manual trigger, or shutdown
        let trigger = tokio::select! {
            () = tokio::time::sleep(poll_interval) => {
                // Check for file changes
                let new_snapshot = FileSnapshot::capture(&current_config, &main);
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

        // Transform files are read at assembly, not held in `Config`, so an
        // unchanged config still re-assembles: the file edit is the change.
        match classify_change(&current_config, &new_config) {
            ChangeKind::Unsafe => {
                warn!(
                    "non-transform config changed — requires pod restart. \
                     Only transform YAML file changes can be hot-reloaded."
                );
                metrics.record_config_reload("rejected");
                continue;
            }
            change @ (ChangeKind::None | ChangeKind::TransformsOnly) => {
                info!(
                    ?change,
                    "transform change detected, proceeding with hot-reload"
                );
            }
        }

        // `Reloading` counts as ready, so it is only ever entered from
        // `Running`. A config change landing mid crash-backoff must not flip
        // /readyz to 200 for a subprocess that is not there. Every exit path
        // below hands the state back through `end_reload`.
        let owns_reloading = lifecycle.set_if(State::Running, State::Reloading);

        let staged = match Staged::assemble(&new_config, &config_dir) {
            Ok(staged) => staged,
            Err(e) => {
                error!(error = %e, "failed to re-assemble config during reload");
                metrics.record_config_reload("error");
                end_reload(&lifecycle, owns_reloading);
                continue;
            }
        };

        if let Err(e) = vector_validate(&new_config.vector, staged.dir()).await {
            error!(error = %e, "vector validate failed during reload");
            metrics.record_config_reload("error");
            metrics.record_config_validation_error();
            staged.discard();
            end_reload(&lifecycle, owns_reloading);
            continue;
        }

        let published = match staged.publish() {
            Ok(published) => published,
            Err(e) => {
                error!(error = %e, "failed to publish the validated config during reload");
                metrics.record_config_reload("error");
                end_reload(&lifecycle, owns_reloading);
                continue;
            }
        };

        // A Vector started from here on reads the new config, so it stays
        // published whether or not this one can be signalled.
        let pid = vector_pid
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .copied();
        let Some(pid) = pid else {
            warn!("Vector process not running, skipping SIGHUP");
            metrics.record_config_reload("error");
            published.commit();
            end_reload(&lifecycle, owns_reloading);
            continue;
        };
        let exporter = &current_config.metrics.vector_metrics_address;
        let before = read_reload_counts(exporter).await;
        if let Err(e) = send_sighup(pid) {
            error!(error = %e, "failed to send SIGHUP to Vector");
            metrics.record_config_reload("error");
            published.commit();
            end_reload(&lifecycle, owns_reloading);
            continue;
        }
        let outcome = match before {
            Ok(before) => {
                await_vector_reload(
                    exporter,
                    &before,
                    RELOAD_CONFIRM_TIMEOUT,
                    RELOAD_CONFIRM_POLL,
                )
                .await
            }
            Err(e) => VectorReload::Unconfirmed(format!("no reading before the SIGHUP: {e}")),
        };

        match outcome {
            VectorReload::Applied => {
                info!("config hot-reload completed successfully");
                metrics.record_config_reload("success");
                security::config_changed(
                    "config_reload",
                    "system",
                    "transform config reloaded via SIGHUP",
                );
            }
            VectorReload::Refused(reasons) => {
                error!(
                    ?reasons,
                    "Vector refused the reload and kept the config it was running; \
                     putting that config back"
                );
                metrics.record_config_reload("error");
                roll_back(published, &config_dir);
                end_reload(&lifecycle, owns_reloading);
                continue;
            }
            VectorReload::Unconfirmed(reason) => {
                warn!(
                    %reason,
                    "Vector did not report the reload; whether it runs the new \
                     transforms is unknown"
                );
                metrics.record_config_reload("unconfirmed");
            }
        }

        // Vector runs the new config, or may: the directory now holds it.
        published.commit();
        current_config = new_config;
        snapshot = FileSnapshot::capture(&current_config, &main);
        end_reload(&lifecycle, owns_reloading);
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

    /// `dfe_source` is a top-level field, which the old sibling-by-sibling
    /// allowlist never compared. It derives the topics and the consumer group,
    /// all of which Vector binds at startup.
    #[test]
    fn classify_dfe_source_change_requires_restart() {
        let old = base_config();
        let mut new = old.clone();
        new.dfe_source = Some("syslog".into());
        assert_eq!(classify_change(&old, &new), ChangeKind::Unsafe);
    }

    #[test]
    fn file_snapshot_detects_no_change() {
        let config = base_config();
        let s1 = FileSnapshot::capture(&config, &[]);
        let s2 = FileSnapshot::capture(&config, &[]);
        assert!(!s1.has_changed(&s2));
    }

    #[test]
    fn file_snapshot_detects_new_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut config = base_config();
        config.transforms.dir = Some(dir.path().to_string_lossy().into_owned());

        let s1 = FileSnapshot::capture(&config, &[]);

        // Add a file
        std::fs::write(dir.path().join("new.yaml"), "test").unwrap();
        let s2 = FileSnapshot::capture(&config, &[]);

        assert!(s1.has_changed(&s2));
    }

    /// `transforms.dir` and `transforms.files` live in the main config file,
    /// so an edit to it alone is a change to reload.
    #[test]
    fn file_snapshot_detects_a_main_config_edit() {
        let dir = tempfile::TempDir::new().unwrap();
        let main = dir.path().join("config.yaml");
        std::fs::write(&main, "pipeline:\n  name: a\n").unwrap();
        let config = base_config();

        let before = FileSnapshot::capture(&config, std::slice::from_ref(&main));
        let mtime = std::fs::metadata(&main).unwrap().modified().unwrap();
        std::fs::write(&main, "pipeline:\n  name: b\n").unwrap();
        std::fs::File::options()
            .write(true)
            .open(&main)
            .unwrap()
            .set_modified(mtime + Duration::from_secs(1))
            .unwrap();
        let after = FileSnapshot::capture(&config, std::slice::from_ref(&main));

        assert!(before.has_changed(&after), "a main config edit went unseen");
    }

    /// Serve `bodies` in turn as Vector's exporter, one per connection, the
    /// last repeated once they run out.
    async fn exporter(bodies: Vec<String>) -> (String, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let handle = tokio::spawn(async move {
            let mut served = 0;
            while let Ok((mut sock, _)) = listener.accept().await {
                let body = &bodies[served.min(bodies.len() - 1)];
                served += 1;
                let mut req = [0u8; 1024];
                let _ = sock.read(&mut req).await;
                let response = format!("HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n{body}");
                let _ = sock.write_all(response.as_bytes()).await;
                let _ = sock.shutdown().await;
            }
        });
        (addr, handle)
    }

    const NOT_RELOADED: &str = "# TYPE vector_started_total counter\nvector_started_total 1\n";

    const APPLIED: &str = "# TYPE vector_reloaded_total counter\nvector_reloaded_total 1\n";

    const REFUSED: &str = "# TYPE vector_component_errors_total counter\n\
        vector_component_errors_total{error_code=\"reload\",error_type=\"configuration_failed\",\
        reason=\"topology_build_failed\",stage=\"processing\"} 1\n";

    const FAST: Duration = Duration::from_millis(10);

    #[tokio::test]
    async fn an_applied_reload_is_reported_applied() {
        let (addr, server) = exporter(vec![NOT_RELOADED.into(), APPLIED.into()]).await;
        let before = read_reload_counts(&addr).await.unwrap();
        let outcome = await_vector_reload(&addr, &before, Duration::from_secs(5), FAST).await;
        server.abort();
        assert_eq!(outcome, VectorReload::Applied);
    }

    #[tokio::test]
    async fn a_refused_reload_is_reported_with_vectors_reason() {
        let (addr, server) = exporter(vec![
            NOT_RELOADED.into(),
            NOT_RELOADED.into(),
            REFUSED.into(),
        ])
        .await;
        let before = read_reload_counts(&addr).await.unwrap();
        let outcome = await_vector_reload(&addr, &before, Duration::from_secs(5), FAST).await;
        server.abort();
        assert_eq!(
            outcome,
            VectorReload::Refused(vec!["reload/topology_build_failed".into()])
        );
    }

    /// Both in one reading: putting the running config back is the side that
    /// cannot leave the directory ahead of Vector.
    #[tokio::test]
    async fn a_refusal_counted_beside_an_applied_reload_wins() {
        let (addr, server) =
            exporter(vec![NOT_RELOADED.into(), format!("{APPLIED}{REFUSED}")]).await;
        let before = read_reload_counts(&addr).await.unwrap();
        let outcome = await_vector_reload(&addr, &before, Duration::from_secs(5), FAST).await;
        server.abort();
        assert!(matches!(outcome, VectorReload::Refused(_)), "{outcome:?}");
    }

    #[tokio::test]
    async fn a_vector_that_reports_nothing_leaves_the_reload_unconfirmed() {
        let (addr, server) = exporter(vec![NOT_RELOADED.into()]).await;
        let before = read_reload_counts(&addr).await.unwrap();
        let outcome = await_vector_reload(&addr, &before, Duration::from_millis(100), FAST).await;
        server.abort();
        assert!(
            matches!(&outcome, VectorReload::Unconfirmed(why) if why.contains("no reload")),
            "{outcome:?}"
        );
    }

    #[tokio::test]
    async fn an_exporter_that_cannot_be_read_leaves_the_reload_unconfirmed() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead = listener.local_addr().unwrap().to_string();
        drop(listener);
        let outcome = await_vector_reload(
            &dead,
            &ReloadCounts::default(),
            Duration::from_millis(100),
            FAST,
        )
        .await;
        assert!(
            matches!(&outcome, VectorReload::Unconfirmed(why) if why.contains(&dead)),
            "{outcome:?}"
        );
    }
}
