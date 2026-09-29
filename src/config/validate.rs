// Project:   dfe-transform-vector
// File:      src/config/validate.rs
// Purpose:   Vector validate command integration
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Vector validate integration.
//!
//! Shells out to `vector validate --no-environment --config-dir <path>` and
//! interprets the exit code:
//! - 0: valid configuration
//! - 78: configuration error (YAML syntax, VRL type mismatch, etc.)
//! - Other: system error (binary not found, permissions, etc.)
//!
//! `--no-environment` compiles VRL against stub enrichment tables and never
//! opens their files, so [`vector_validate`] checks those files itself first.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use scalo::logger::security;
use tracing::{debug, error};

use super::loader::VectorConfig;
use crate::Result;

/// Exit code for Vector configuration errors.
const VECTOR_CONFIG_ERROR_EXIT: i32 = 78;

/// The argv `vector validate` is run with.
///
/// `--no-environment`: validate loads the config without its secret backends,
/// so a `SECRET[...]` credential would reach the broker as the literal
/// reference. The running Vector resolves the secrets and runs the health
/// checks itself.
#[must_use]
pub fn validate_args(config_dir: &Path) -> Vec<OsString> {
    vec![
        "validate".into(),
        "--no-environment".into(),
        "--config-dir".into(),
        config_dir.as_os_str().to_owned(),
    ]
}

/// Run `vector validate` against the assembled config directory.
///
/// Checks the config itself, the VRL of every transform included, that every
/// enrichment table file can be read, and nothing that needs the broker -- see
/// [`validate_args`].
///
/// Returns `Ok(())` if validation passes, or an error naming each unreadable
/// table file, or with details from Vector's stderr output.
pub async fn vector_validate(vector_config: &VectorConfig, config_dir: &Path) -> Result<()> {
    check_enrichment_tables(config_dir).await?;

    let binary = &vector_config.binary;

    debug!(
        binary = %binary,
        config_dir = %config_dir.display(),
        "running vector validate"
    );

    let mut cmd = tokio::process::Command::new(binary);
    cmd.args(validate_args(config_dir))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    // Vector needs a writable data_dir even for validate
    if !vector_config.data_dir.is_empty() {
        cmd.env("VECTOR_DATA_DIR", &vector_config.data_dir);
    }

    let output = cmd
        .output()
        .await
        .map_err(|e| crate::Error::Vector(format!("failed to run '{binary} validate': {e}")))?;

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    if output.status.success() {
        debug!("vector validate passed");
        return Ok(());
    }

    let exit_code = output.status.code().unwrap_or(-1);

    if exit_code == VECTOR_CONFIG_ERROR_EXIT {
        error!(exit_code, stderr = %stderr, "vector config validation failed");
        return Err(crate::Error::Validation(format!(
            "Vector config validation failed (exit {exit_code}): {stderr}"
        )));
    }

    // Unexpected exit code
    error!(
        exit_code,
        stderr = %stderr,
        stdout = %stdout,
        "vector validate exited with unexpected code"
    );
    Err(crate::Error::Vector(format!(
        "vector validate exited with code {exit_code}: {stderr}"
    )))
}

/// Check the Vector binary version against the configured pin.
///
/// Runs `vector --version` and compares with `vector_config.version`.
/// Behaviour depends on `vector_config.version_check`:
/// - `strict`: error if versions don't match
/// - `warn`: log warning if versions don't match
/// - `disabled`: skip check entirely
pub async fn check_vector_version(vector_config: &VectorConfig) -> Result<String> {
    if vector_config.version_check == "disabled" {
        debug!("vector version check disabled");
        return Ok(String::new());
    }

    let binary = &vector_config.binary;
    let output = tokio::process::Command::new(binary)
        .arg("--version")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|e| crate::Error::Vector(format!("failed to run '{binary} --version': {e}")))?;

    if !output.status.success() {
        return Err(crate::Error::Vector(format!(
            "'{binary} --version' failed with exit code {}",
            output.status.code().unwrap_or(-1)
        )));
    }

    // Parse version from output like "vector 0.48.0 (aarch64-unknown-linux-gnu ...)"
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let version = parse_vector_version(&stdout);

    debug!(detected = %version, "vector binary version");

    // Compare with config pin if set
    if !vector_config.version.is_empty() && version != vector_config.version {
        let msg = format!(
            "Vector version mismatch: expected '{}', got '{}'",
            vector_config.version, version
        );
        security::data_quality_alert("version_check", &msg);
        match vector_config.version_check.as_str() {
            "strict" => {
                error!("{}", msg);
                return Err(crate::Error::Vector(msg));
            }
            "warn" => {
                tracing::warn!("{}", msg);
            }
            _ => {}
        }
    }

    Ok(version)
}

/// Parse the version string from `vector --version` output.
///
/// Expects format like: `vector 0.48.0 (aarch64-unknown-linux-gnu ...)`
fn parse_vector_version(output: &str) -> String {
    output
        .split_whitespace()
        .nth(1)
        .unwrap_or("unknown")
        .to_string()
}

/// What the data directory is actually sitting on, as far as we can tell from
/// inside the container.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataDirBacking {
    /// Part of the container's own writable layer, or an explicit emptyDir.
    /// Either way the contents do not survive the container.
    Ephemeral,
    /// A separate mount from outside the container -- a k8s volume, a docker
    /// `-v` bind, a named volume. We cannot tell WHICH from in here, and do not
    /// need to: what matters is that something outside owns the lifetime.
    ExternalMount,
    /// No `/proc/self/mountinfo` to read -- not Linux, or a sandbox that hides
    /// it. Reported rather than assumed either way.
    Unknown,
}

/// Classify `data_dir` from `/proc/self/mountinfo`.
///
/// Only what is visible from WITHIN the container: whether the path falls on a
/// mount of its own, or on the container's root filesystem. A k8s volume and a
/// docker bind both appear as their own mount point; the writable layer does
/// not. That distinction is the whole question, and it does not require knowing
/// anything about the orchestrator.
///
/// `mountinfo` is taken as text so the parsing is testable without a container.
#[must_use]
pub fn classify_data_dir(mountinfo: Option<&str>, data_dir: &Path) -> DataDirBacking {
    let Some(mountinfo) = mountinfo else {
        return DataDirBacking::Unknown;
    };

    // Field 4 is the mount source path within the filesystem, field 5 the mount
    // point. Longest matching mount point wins -- /var/lib/vector beats /.
    let mut best: Option<(usize, &str, &str)> = None;
    for line in mountinfo.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 5 {
            continue;
        }
        let root = fields[3];
        let point = fields[4];
        if data_dir.starts_with(point) && best.is_none_or(|(len, _, _)| point.len() > len) {
            best = Some((point.len(), point, root));
        }
    }

    match best {
        // Only the rootfs covers it, so it is the writable layer.
        Some((_, "/", _)) => DataDirBacking::Ephemeral,
        Some((_, _, root)) => {
            // kubelet exposes an emptyDir by its source path. It IS its own
            // mount, so the mount-point test alone would call it external --
            // but its lifetime is the pod's, which is exactly what a disk
            // buffer must outlive.
            if root.contains("kubernetes.io~empty-dir") {
                DataDirBacking::Ephemeral
            } else {
                DataDirBacking::ExternalMount
            }
        }
        None => DataDirBacking::Ephemeral,
    }
}

/// True when any assembled Vector config declares a disk buffer.
///
/// Parses the YAML rather than grepping for `disk`: a transform named
/// `disk_usage`, or a comment, would otherwise trigger a warning that then gets
/// ignored -- and an alarm people learn to ignore is worse than none.
///
/// Scans the whole assembled directory, not just our own sink fragment, because
/// an operator-supplied transform or sink can declare its own buffer and the
/// Helm-time guard never sees those.
#[must_use]
pub fn assembled_config_uses_disk_buffer(config_dir: &Path) -> bool {
    assembled_documents(config_dir)
        .into_iter()
        .find(|(_, value)| yaml_declares_disk_buffer(value))
        .inspect(|(path, _)| debug!(path = %path.display(), "config declares a disk buffer"))
        .is_some()
}

/// Every YAML document in the assembled directory, with the file it came from.
///
/// A file that cannot be read or parsed is left out: `vector validate` reports it.
fn assembled_documents(config_dir: &Path) -> Vec<(PathBuf, serde_yaml_ng::Value)> {
    let Ok(entries) = std::fs::read_dir(config_dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|e| e == "yaml" || e == "yml"))
        .filter_map(|path| {
            let text = std::fs::read_to_string(&path).ok()?;
            let value = serde_yaml_ng::from_str::<serde_yaml_ng::Value>(&text).ok()?;
            Some((path, value))
        })
        .collect()
}

/// An enrichment table whose file Vector cannot load.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnreadableTable {
    /// The table's name under `enrichment_tables`.
    pub name: String,
    /// The file the table loads.
    pub path: PathBuf,
    /// Why the file cannot be read.
    pub reason: String,
}

impl std::fmt::Display for UnreadableTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "enrichment table '{}' cannot load {}: {}",
            self.name,
            self.path.display(),
            self.reason
        )
    }
}

/// Every enrichment table in the assembled config whose file cannot be read.
///
/// Parsed rather than grepped, and across every assembled file, because an
/// operator's transform file declares its own tables. A `file` table loads
/// `file.path`; `geoip` and `mmdb` load `path`.
#[must_use]
pub fn unreadable_enrichment_tables(config_dir: &Path) -> Vec<UnreadableTable> {
    let mut unreadable = Vec::new();
    for (_, document) in assembled_documents(config_dir) {
        let Some(tables) = document
            .get("enrichment_tables")
            .and_then(serde_yaml_ng::Value::as_mapping)
        else {
            continue;
        };
        for (name, table) in tables {
            let Some(path) = enrichment_table_file(table) else {
                continue;
            };
            if let Some(reason) = unreadable_file(Path::new(path)) {
                unreadable.push(UnreadableTable {
                    name: name.as_str().unwrap_or_default().to_string(),
                    path: PathBuf::from(path),
                    reason,
                });
            }
        }
    }
    unreadable
}

/// The file an enrichment table loads, for the table types that load one.
fn enrichment_table_file(table: &serde_yaml_ng::Value) -> Option<&str> {
    use serde_yaml_ng::Value;

    match table.get("type").and_then(Value::as_str)? {
        "file" => table.get("file")?.get("path")?.as_str(),
        "geoip" | "mmdb" => table.get("path")?.as_str(),
        _ => None,
    }
}

/// Why `path` cannot be read as a file, or `None` when it can.
fn unreadable_file(path: &Path) -> Option<String> {
    match std::fs::File::open(path).and_then(|file| file.metadata()) {
        Ok(meta) if meta.is_file() => None,
        Ok(_) => Some("not a regular file".to_string()),
        Err(e) => Some(e.to_string()),
    }
}

/// Fail when an enrichment table file in the assembled config cannot be read.
///
/// Vector fails on such a table only when it loads it: a crash-loop at startup,
/// and on a reload a rollback the reload never reports.
async fn check_enrichment_tables(config_dir: &Path) -> Result<()> {
    let dir = config_dir.to_path_buf();
    let unreadable = tokio::task::spawn_blocking(move || unreadable_enrichment_tables(&dir))
        .await
        .map_err(|e| crate::Error::Vector(format!("enrichment table check did not finish: {e}")))?;
    if unreadable.is_empty() {
        return Ok(());
    }
    let tables: Vec<String> = unreadable.iter().map(ToString::to_string).collect();
    Err(crate::Error::Validation(tables.join("; ")))
}

/// Recursively look for a `buffer` whose `type` is `disk`.
///
/// Vector accepts `buffer` as either a mapping or a list of them (buffer
/// stages), so both shapes are checked.
fn yaml_declares_disk_buffer(value: &serde_yaml_ng::Value) -> bool {
    use serde_yaml_ng::Value;

    let is_disk = |v: &Value| {
        v.get("type")
            .and_then(Value::as_str)
            .is_some_and(|t| t == "disk")
    };

    match value {
        Value::Mapping(map) => {
            for (k, v) in map {
                if k.as_str() == Some("buffer") {
                    match v {
                        Value::Sequence(stages) => {
                            if stages.iter().any(is_disk) {
                                return true;
                            }
                        }
                        other => {
                            if is_disk(other) {
                                return true;
                            }
                        }
                    }
                }
                if yaml_declares_disk_buffer(v) {
                    return true;
                }
            }
            false
        }
        Value::Sequence(seq) => seq.iter().any(yaml_declares_disk_buffer),
        _ => false,
    }
}

/// Warn LOUDLY when a disk buffer has nowhere durable to live.
///
/// A disk buffer asks Vector to hold events across a restart. On the container's
/// writable layer or an emptyDir every write still succeeds and nothing errors,
/// so the failure is invisible until events are actually lost -- a guarantee
/// that looks present and is not.
///
/// The chart refuses to template our own `sink.buffer.type: disk` without a
/// volume, but an operator-supplied transform or sink config can declare a disk
/// buffer that the chart never sees. This is the backstop for that, and it runs
/// against what the process can observe rather than what the manifest claimed.
///
/// Warns rather than aborts: the process CAN run, the operator may have accepted
/// the trade, and refusing to start a data-plane pod over a durability
/// preference is the wrong call to make on their behalf.
pub fn warn_if_disk_buffer_is_ephemeral(config_dir: &Path, vector_config: &VectorConfig) {
    if !assembled_config_uses_disk_buffer(config_dir) {
        return;
    }

    let data_dir = Path::new(&vector_config.data_dir);
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").ok();

    match classify_data_dir(mountinfo.as_deref(), data_dir) {
        DataDirBacking::ExternalMount => {
            debug!(
                data_dir = %data_dir.display(),
                "disk buffer is on an external mount"
            );
        }
        DataDirBacking::Ephemeral => {
            let msg = format!(
                "DISK BUFFER HAS NO DURABLE STORAGE. The Vector config declares \
                 buffer.type=disk, but {} is on the container's own filesystem \
                 (writable layer or emptyDir). Buffered events will be LOST on \
                 restart or reschedule -- the durability a disk buffer exists to \
                 provide is absent, and nothing will error when it happens. \
                 Mount external storage at that path (Helm: persistence.enabled=true; \
                 docker: -v), or switch to buffer.type=memory so the trade is explicit.",
                data_dir.display()
            );
            error!("{msg}");
            // Routed to the security/data-quality channel as well, so it lands
            // wherever data-loss risks are actually watched rather than only in
            // a log nobody greps.
            security::data_quality_alert("disk_buffer_ephemeral", &msg);
        }
        DataDirBacking::Unknown => {
            error!(
                data_dir = %data_dir.display(),
                "config declares buffer.type=disk but /proc/self/mountinfo is \
                 unreadable, so whether {} survives a restart could NOT be \
                 determined. Verify the mount by hand.",
                data_dir.display()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Without `--no-environment`, validate hands a SASL broker the literal
    /// `SECRET[...]` reference and startup fails on "invalid credentials".
    #[test]
    fn startup_validation_stays_off_the_broker() {
        let args: Vec<String> = validate_args(Path::new("/var/run/vector/config"))
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            args,
            [
                "validate",
                "--no-environment",
                "--config-dir",
                "/var/run/vector/config"
            ]
        );
    }

    #[test]
    fn parse_version_standard() {
        let output = "vector 0.48.0 (x86_64-unknown-linux-gnu 2024-01-01)";
        assert_eq!(parse_vector_version(output), "0.48.0");
    }

    #[test]
    fn parse_version_no_extra() {
        let output = "vector 0.48.0";
        assert_eq!(parse_vector_version(output), "0.48.0");
    }

    #[test]
    fn parse_version_empty() {
        assert_eq!(parse_vector_version(""), "unknown");
    }

    // Real mountinfo lines, trimmed to the fields the parser uses.
    const ROOTFS_ONLY: &str = "\
571 570 0:118 / / rw,relatime - overlay overlay rw,lowerdir=/x,upperdir=/y
572 571 0:121 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw";

    const EMPTY_DIR: &str = "\
571 570 0:118 / / rw,relatime - overlay overlay rw
600 571 259:1 /var/lib/kubelet/pods/abc-123/volumes/kubernetes.io~empty-dir/vector-data /var/lib/vector rw,relatime - ext4 /dev/nvme0n1p1 rw";

    const CSI_VOLUME: &str = "\
571 570 0:118 / / rw,relatime - overlay overlay rw
601 571 259:1 /var/lib/kubelet/pods/abc-123/volumes/kubernetes.io~csi/pvc-9f8e/mount /var/lib/vector rw,relatime - ext4 /dev/nvme1n1 rw";

    const DOCKER_BIND: &str = "\
571 570 0:118 / / rw,relatime - overlay overlay rw
602 571 0:99 /host/vector-buffer /var/lib/vector rw,relatime - ext4 /dev/sda1 rw";

    #[test]
    fn writable_layer_is_ephemeral() {
        // Nothing but the rootfs covers the path, so the buffer is in the
        // container's own filesystem.
        assert_eq!(
            classify_data_dir(Some(ROOTFS_ONLY), Path::new("/var/lib/vector")),
            DataDirBacking::Ephemeral
        );
    }

    #[test]
    fn empty_dir_is_ephemeral_despite_being_its_own_mount() {
        // An emptyDir is its own mount, so a mount-point test alone reads it as
        // durable. Its lifetime is the pod's, which a disk buffer must outlive.
        assert_eq!(
            classify_data_dir(Some(EMPTY_DIR), Path::new("/var/lib/vector")),
            DataDirBacking::Ephemeral
        );
    }

    #[test]
    fn a_csi_volume_counts_as_external() {
        assert_eq!(
            classify_data_dir(Some(CSI_VOLUME), Path::new("/var/lib/vector")),
            DataDirBacking::ExternalMount
        );
    }

    #[test]
    fn a_docker_bind_counts_as_external() {
        // We cannot tell k8s from docker in here, and do not need to -- what
        // matters is that something outside the container owns the lifetime.
        assert_eq!(
            classify_data_dir(Some(DOCKER_BIND), Path::new("/var/lib/vector")),
            DataDirBacking::ExternalMount
        );
    }

    #[test]
    fn longest_matching_mount_point_wins() {
        // `/` also prefixes the path; the more specific mount must win or every
        // external mount would read as the writable layer.
        assert_eq!(
            classify_data_dir(
                Some(CSI_VOLUME),
                Path::new("/var/lib/vector/buffer/dfe_sink")
            ),
            DataDirBacking::ExternalMount
        );
    }

    #[test]
    fn unreadable_mountinfo_is_unknown_not_assumed_fine() {
        // Silently treating "cannot tell" as durable would be the exact bug
        // this whole check exists to catch.
        assert_eq!(
            classify_data_dir(None, Path::new("/var/lib/vector")),
            DataDirBacking::Unknown
        );
    }

    fn write_yaml_file(dir: &Path, name: &str, body: &str) {
        std::fs::write(dir.join(name), body).expect("write fixture");
    }

    #[test]
    fn detects_a_disk_buffer_in_our_own_sink_fragment() {
        let tmp = tempfile::tempdir().expect("tempdir");
        write_yaml_file(
            tmp.path(),
            "90_sink.yaml",
            "sinks:\n  dfe_sink:\n    type: kafka\n    buffer:\n      type: disk\n      max_size: 1073741824\n",
        );
        assert!(assembled_config_uses_disk_buffer(tmp.path()));
    }

    #[test]
    fn detects_a_disk_buffer_an_operator_added_to_a_transform() {
        // The reason this check exists at runtime: the Helm guard only sees our
        // own sink.buffer, never an operator-supplied fragment.
        let tmp = tempfile::tempdir().expect("tempdir");
        write_yaml_file(
            tmp.path(),
            "50_custom.yaml",
            "sinks:\n  extra:\n    type: file\n    buffer:\n      type: disk\n      max_size: 268435488\n",
        );
        assert!(assembled_config_uses_disk_buffer(tmp.path()));
    }

    #[test]
    fn detects_a_disk_buffer_in_a_buffer_stage_list() {
        // Vector also accepts `buffer` as a list of stages.
        let tmp = tempfile::tempdir().expect("tempdir");
        write_yaml_file(
            tmp.path(),
            "90_sink.yaml",
            "sinks:\n  dfe_sink:\n    buffer:\n      - type: memory\n        max_events: 500\n      - type: disk\n        max_size: 268435488\n",
        );
        assert!(assembled_config_uses_disk_buffer(tmp.path()));
    }

    #[test]
    fn a_memory_buffer_does_not_trigger_the_warning() {
        let tmp = tempfile::tempdir().expect("tempdir");
        write_yaml_file(
            tmp.path(),
            "90_sink.yaml",
            "sinks:\n  dfe_sink:\n    type: kafka\n    buffer:\n      type: memory\n      max_events: 500\n",
        );
        assert!(!assembled_config_uses_disk_buffer(tmp.path()));
    }

    #[test]
    fn the_word_disk_elsewhere_does_not_trigger_the_warning() {
        // Parsed, not grepped. A false alarm here teaches people to ignore the
        // real one.
        let tmp = tempfile::tempdir().expect("tempdir");
        write_yaml_file(
            tmp.path(),
            "50_disk_usage.yaml",
            "transforms:\n  disk_usage:\n    type: remap\n    source: |\n      .note = \"buffer type disk is not set here\"\n",
        );
        assert!(!assembled_config_uses_disk_buffer(tmp.path()));
    }

    #[test]
    fn a_missing_config_dir_is_not_a_disk_buffer() {
        assert!(!assembled_config_uses_disk_buffer(Path::new(
            "/nonexistent/dfe-transform-vector/config"
        )));
    }

    /// A transform file declaring a `file` table at `path`, the shape the
    /// bundled filebeat pipeline ships.
    fn file_table(path: &Path) -> String {
        format!(
            "enrichment_tables:\n  timezones:\n    type: file\n    file:\n      path: {}\n      \
             encoding:\n        type: csv\n    schema:\n      abbreviation: string\n",
            path.display()
        )
    }

    #[test]
    fn a_missing_table_file_is_named_with_its_path() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let missing = tmp.path().join("data/timezones.csv");
        write_yaml_file(tmp.path(), "50_000_filebeat.yaml", &file_table(&missing));

        let unreadable = unreadable_enrichment_tables(tmp.path());
        assert_eq!(unreadable.len(), 1, "{unreadable:?}");
        assert_eq!(unreadable[0].name, "timezones");
        assert_eq!(unreadable[0].path, missing);
        assert!(
            unreadable[0]
                .to_string()
                .contains(&missing.display().to_string()),
            "the error must carry the path: {}",
            unreadable[0]
        );
    }

    #[test]
    fn a_readable_table_file_passes() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let table = tmp.path().join("timezones.csv");
        std::fs::write(&table, "abbreviation\nUTC\n").expect("write table");
        write_yaml_file(tmp.path(), "50_000_filebeat.yaml", &file_table(&table));

        assert!(unreadable_enrichment_tables(tmp.path()).is_empty());
    }

    #[test]
    fn a_directory_where_the_table_file_should_be_is_unreadable() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path().join("timezones.csv");
        std::fs::create_dir(&dir).expect("create dir");
        write_yaml_file(tmp.path(), "50_000_filebeat.yaml", &file_table(&dir));

        let unreadable = unreadable_enrichment_tables(tmp.path());
        assert_eq!(unreadable.len(), 1, "{unreadable:?}");
        assert_eq!(unreadable[0].reason, "not a regular file");
    }

    #[test]
    fn geoip_and_mmdb_tables_load_a_file_too() {
        let tmp = tempfile::tempdir().expect("tempdir");
        write_yaml_file(
            tmp.path(),
            "50_000_geo.yaml",
            "enrichment_tables:\n  city:\n    type: geoip\n    path: /nonexistent/GeoLite2-City.mmdb\n  \
             asn:\n    type: mmdb\n    path: /nonexistent/asn.mmdb\n",
        );

        let mut names: Vec<String> = unreadable_enrichment_tables(tmp.path())
            .into_iter()
            .map(|t| t.name)
            .collect();
        names.sort();
        assert_eq!(names, ["asn", "city"]);
    }

    #[test]
    fn a_table_that_loads_no_file_is_not_checked() {
        let tmp = tempfile::tempdir().expect("tempdir");
        write_yaml_file(
            tmp.path(),
            "50_000_memory.yaml",
            "enrichment_tables:\n  cache:\n    type: memory\n    ttl: 60\n",
        );

        assert!(unreadable_enrichment_tables(tmp.path()).is_empty());
    }

    /// A stand-in for a Vector binary whose `validate` passes, as the real one
    /// does with `--no-environment` whatever the table files are.
    fn always_valid_binary(dir: &Path) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let path = dir.join("vector-that-validates");
        std::fs::write(&path, "#!/bin/sh\nexit 0\n").expect("write the fake binary");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
            .expect("make the fake binary executable");
        path
    }

    /// Startup and reload both validate through `vector_validate`, so a table
    /// Vector would fail to load must fail here, before Vector is asked.
    #[tokio::test]
    async fn vector_validate_refuses_a_table_file_it_cannot_read() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let config_dir = tmp.path().join("config");
        std::fs::create_dir(&config_dir).expect("config dir");
        let missing = tmp.path().join("timezones.csv");
        write_yaml_file(&config_dir, "50_000_filebeat.yaml", &file_table(&missing));

        let vector_config = VectorConfig {
            binary: always_valid_binary(tmp.path())
                .to_string_lossy()
                .into_owned(),
            data_dir: String::new(),
            ..VectorConfig::default()
        };

        let err = vector_validate(&vector_config, &config_dir)
            .await
            .expect_err("a table file that cannot be read must fail validation");
        assert!(
            matches!(err, crate::Error::Validation(_))
                && err.to_string().contains(&missing.display().to_string()),
            "the error must be a validation failure naming the file: {err}"
        );

        std::fs::write(&missing, "abbreviation\nUTC\n").expect("write table");
        vector_validate(&vector_config, &config_dir)
            .await
            .expect("the same config validates once the file is there");
    }
}
