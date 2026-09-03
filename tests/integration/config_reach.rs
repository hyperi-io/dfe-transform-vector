// Project:   dfe-transform-vector
// File:      tests/integration/config_reach.rs
// Purpose:   Assert every config key a deployment can set reaches what it names
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Does a setting a deployment writes actually reach the thing it names?
//!
//! The failures this file guards against all look identical from outside: the
//! YAML parses, `validate()` passes, the pod starts, and the setting does
//! nothing. Every case below was a live defect -- `metrics.address` while the
//! listener bound `0.0.0.0:9090`, `logging.*` while the subscriber read scalo's
//! own cascade, `metrics.vector_metrics_address` while the exporter address was
//! hard-coded, and a `config.example.yaml` that could not start at all.

use std::path::{Path, PathBuf};

use scalo::cli::CommonArgs;

use crate::integration::config_env::load_config;
use dfe_transform_vector::config::{Config, assembler};

/// `CommonArgs` with nothing supplied on the command line or in the
/// environment -- the shape a container gets, which passes only `--config`.
fn bare_args() -> CommonArgs {
    CommonArgs {
        config: None,
        log_level: None,
        log_format: None,
        metrics_addr: None,
        verbose: false,
        quiet: false,
    }
}

// ---------------------------------------------------------------------------
// The app's own config keys must reach scalo's arg resolvers
// ---------------------------------------------------------------------------

/// `metrics.address` is where `/metrics`, `/livez` and `/readyz` are served.
/// Left unreached, an operator who moves the port loses every probe and the
/// pod fails readiness for a reason nothing reports.
#[test]
fn metrics_address_reaches_the_listener_bind() {
    let config = Config {
        metrics: dfe_transform_vector::config::MetricsConfig {
            address: "0.0.0.0:19099".into(),
            ..Default::default()
        },
        ..Default::default()
    };
    let mut args = bare_args();
    config.fill_common_args(&mut args);
    assert_eq!(
        args.effective_metrics_addr(),
        "0.0.0.0:19099",
        "metrics.address must reach the address ServiceRuntime binds"
    );
}

#[test]
fn logging_level_and_format_reach_the_logger() {
    let config = Config {
        logging: dfe_transform_vector::config::LoggingConfig {
            level: "trace".into(),
            format: "json".into(),
        },
        ..Default::default()
    };
    let mut args = bare_args();
    config.fill_common_args(&mut args);
    assert_eq!(args.effective_log_level(), "trace");
    assert_eq!(args.effective_log_format(), "json");
}

/// The documented precedence is flag, then environment, then config file.
/// clap has already folded `LOG_LEVEL`/`LOG_FORMAT`/`METRICS_ADDR` into these
/// fields by the time the config is read, so a filled slot is never touched.
#[test]
fn cli_and_env_outrank_the_config_file() {
    let config = Config {
        metrics: dfe_transform_vector::config::MetricsConfig {
            address: "0.0.0.0:19099".into(),
            ..Default::default()
        },
        logging: dfe_transform_vector::config::LoggingConfig {
            level: "trace".into(),
            format: "json".into(),
        },
        ..Default::default()
    };
    let mut args = CommonArgs {
        log_level: Some("warn".into()),
        log_format: Some("text".into()),
        metrics_addr: Some("127.0.0.1:19191".into()),
        ..bare_args()
    };
    config.fill_common_args(&mut args);

    assert_eq!(args.effective_metrics_addr(), "127.0.0.1:19191");
    assert_eq!(args.effective_log_level(), "warn");
    assert_eq!(args.effective_log_format(), "text");
}

#[test]
fn verbose_outranks_the_config_level() {
    let config = Config {
        logging: dfe_transform_vector::config::LoggingConfig {
            level: "error".into(),
            format: "auto".into(),
        },
        ..Default::default()
    };
    let mut args = CommonArgs {
        verbose: true,
        ..bare_args()
    };
    config.fill_common_args(&mut args);
    assert_eq!(args.effective_log_level(), "debug");
}

// ---------------------------------------------------------------------------
// Config keys that reach the Vector subprocess
// ---------------------------------------------------------------------------

/// Vector's `prometheus_exporter` binds whatever the assembled config says.
/// Asserting through `assemble()` rather than the generator alone covers the
/// wiring as well as the emitter.
#[test]
fn the_assembled_exporter_binds_the_configured_address() {
    let out = tempfile::tempdir().expect("tempdir");
    let config = Config {
        source: dfe_transform_vector::config::SourceConfig {
            brokers: vec!["kafka:9092".into()],
            topics: vec!["input".into()],
            group_id: "grp".into(),
            ..Default::default()
        },
        sink: dfe_transform_vector::config::SinkConfig {
            brokers: vec!["kafka:9092".into()],
            topic: "output".into(),
            ..Default::default()
        },
        metrics: dfe_transform_vector::config::MetricsConfig {
            vector_metrics_address: "127.0.0.1:19598".into(),
            ..Default::default()
        },
        ..Default::default()
    };
    assembler::assemble(&config, out.path()).expect("assemble");

    let observability =
        std::fs::read_to_string(out.path().join("99_observability.yaml")).expect("read");
    let parsed: serde_yaml_ng::Value = serde_yaml_ng::from_str(&observability).expect("parse");
    let address = parsed
        .get("sinks")
        .and_then(|s| s.get("prometheus_exporter"))
        .and_then(|e| e.get("address"))
        .and_then(serde_yaml_ng::Value::as_str);
    assert_eq!(
        address,
        Some("127.0.0.1:19598"),
        "metrics.vector_metrics_address must reach the assembled exporter"
    );
}

// ---------------------------------------------------------------------------
// Config the runtime cannot honour must be refused, not absorbed
// ---------------------------------------------------------------------------

/// Nothing on the run path resolves a Vector version or downloads a binary --
/// `spawn_vector` runs `vector.binary`. Accepting `latest` would hand back the
/// pre-shipped binary while the config claimed otherwise.
#[test]
fn a_version_source_the_runtime_cannot_honour_is_refused() {
    for source in ["latest", "stable", "0.56", "0.56.0"] {
        let config = Config {
            source: dfe_transform_vector::config::SourceConfig {
                brokers: vec!["kafka:9092".into()],
                topics: vec!["input".into()],
                group_id: "grp".into(),
                ..Default::default()
            },
            sink: dfe_transform_vector::config::SinkConfig {
                brokers: vec!["kafka:9092".into()],
                topic: "output".into(),
                ..Default::default()
            },
            vector: dfe_transform_vector::config::VectorConfig {
                version_source: source.into(),
                ..Default::default()
            },
            ..Default::default()
        };
        let err = config
            .validate()
            .expect_err("a version_source nothing acts on must be refused");
        assert!(
            err.to_string().contains("vector.version_source"),
            "error should name the key, got: {err}"
        );
    }
}

/// `logging.level` and `logging.format` are enum-shaped strings that now reach
/// scalo's parsers. A typo has to be named as a config error, not surface as
/// `invalid log level: infp` from somewhere with no config key in it.
#[test]
fn a_misspelt_logging_value_is_named_as_a_config_error() {
    for (level, format, expected) in [
        ("infp", "auto", "logging.level"),
        ("info", "jsonn", "logging.format"),
    ] {
        let config = Config {
            source: dfe_transform_vector::config::SourceConfig {
                brokers: vec!["kafka:9092".into()],
                topics: vec!["input".into()],
                group_id: "grp".into(),
                ..Default::default()
            },
            sink: dfe_transform_vector::config::SinkConfig {
                brokers: vec!["kafka:9092".into()],
                topic: "output".into(),
                ..Default::default()
            },
            logging: dfe_transform_vector::config::LoggingConfig {
                level: level.into(),
                format: format.into(),
            },
            ..Default::default()
        };
        let err = config.validate().expect_err("a bad enum must be refused");
        assert!(
            err.to_string().contains(expected),
            "error should name {expected}, got: {err}"
        );
    }
}

#[test]
fn preshipped_is_the_version_source_that_validates() {
    let config = Config {
        source: dfe_transform_vector::config::SourceConfig {
            brokers: vec!["kafka:9092".into()],
            topics: vec!["input".into()],
            group_id: "grp".into(),
            ..Default::default()
        },
        sink: dfe_transform_vector::config::SinkConfig {
            brokers: vec!["kafka:9092".into()],
            topic: "output".into(),
            ..Default::default()
        },
        ..Default::default()
    };
    assert_eq!(config.vector.version_source, "preshipped");
    config.validate().expect("the default must validate");
}

// ---------------------------------------------------------------------------
// Standing checks -- these walk, so a file or a secret added later is covered
// ---------------------------------------------------------------------------

/// Every committed config file must load AND validate.
///
/// Walks rather than naming files: `config.example.yaml` shipped for months
/// unable to start (`sink.topic must not be empty`) because only the fixtures
/// were exercised, and a fixture added tomorrow would have the same gap.
#[test]
fn every_committed_config_file_loads_and_validates() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files: Vec<PathBuf> = Vec::new();

    for dir in [root.to_path_buf(), root.join("tests/fixtures/configs")] {
        let entries = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
            .flatten();
        for entry in entries {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if !name.ends_with(".yaml") && !name.ends_with(".yml") {
                continue;
            }
            // The repo root holds one config file and a pile of unrelated
            // YAML; the fixtures directory is all config.
            if dir == root && !name.starts_with("config") {
                continue;
            }
            files.push(path);
        }
    }

    assert!(
        files.iter().any(|p| p.ends_with("config.example.yaml")),
        "the example config must be among the files checked"
    );

    for path in files {
        let display = path.display().to_string();
        let config = load_config(Some(&display)).unwrap_or_else(|e| panic!("{display}: {e}"));
        config
            .validate()
            .unwrap_or_else(|e| panic!("{display} does not validate: {e}"));
    }
}
