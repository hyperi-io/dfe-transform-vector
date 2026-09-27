// Project:   dfe-transform-vector
// File:      tests/integration/vector_validate.rs
// Purpose:   Integration tests that run vector validate against assembled configs
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Vector validate integration tests.
//!
//! Assembles full configs with Kafka source, transforms, and Kafka sink,
//! then runs `vector validate --config-dir` to verify Vector accepts them.
//!
//! Uses `scripts/fetch-vector.sh` to auto-download the Vector binary (cached
//! in `.tmp/vector/`). Falls back to system PATH. Skips gracefully if unavailable.
//! Run with: `cargo nextest run -E 'test(vector_validate)' --run-ignored all`

use std::fs;
use std::path::Path;
use std::process::Command;

use dfe_transform_vector::config::assembler;
use dfe_transform_vector::config::loader::{
    BufferConfig, Config, DecodingConfig, PipelineConfig, SaslConfig, SinkConfig, SourceConfig,
    TlsConfig, TransformConfig, VectorConfig,
};
use tempfile::TempDir;

use crate::common;

const FIXTURE_TRANSFORMS: &str = "tests/fixtures/transforms";

fn build_config(buffer: BufferConfig) -> (Config, TempDir) {
    let work_dir = TempDir::new().expect("failed to create work dir");
    let data_dir = work_dir.path().join("data");
    fs::create_dir_all(&data_dir).expect("failed to create data dir");

    let config = Config {
        pipeline: PipelineConfig {
            name: "validate-test".to_string(),
        },
        source: SourceConfig {
            brokers: vec!["localhost:9092".to_string()],
            topics: vec!["raw.input".to_string()],
            group_id: "validate-test-group".to_string(),
            decoding: DecodingConfig {
                codec: "json".to_string(),
            },
            ..Default::default()
        },
        sink: SinkConfig {
            brokers: vec!["localhost:9092".to_string()],
            topic: "enriched.output".to_string(),
            key_field: String::new(),
            encoding: "json".to_string(),
            compression: "none".to_string(),
            buffer,
            ..Default::default()
        },
        transforms: TransformConfig {
            dir: Some(FIXTURE_TRANSFORMS.to_string()),
            files: None,
        },
        vector: VectorConfig {
            binary: "vector".to_string(),
            data_dir: data_dir.to_string_lossy().to_string(),
            api_address: "0.0.0.0:0".to_string(),
            log_level: "info".to_string(),
            version: String::new(),
            version_check: "disabled".to_string(),
            ..Default::default()
        },
        ..Default::default()
    };

    (config, work_dir)
}

fn assemble_and_validate(config: &Config, work_dir: &Path) -> std::process::Output {
    let vector_bin = common::vector_binary_path()
        .expect("Vector binary should be available (checked by caller)");

    let config_dir = work_dir.join("config");
    assembler::assemble(config, &config_dir).expect("assembly should succeed");

    // Verify all expected files exist
    assert!(config_dir.join("00_global.yaml").exists(), "global missing");
    assert!(config_dir.join("00_source.yaml").exists(), "source missing");
    assert!(config_dir.join("90_sink.yaml").exists(), "sink missing");
    assert!(
        config_dir.join("99_observability.yaml").exists(),
        "observability missing"
    );

    // Count transform files
    let transform_count = fs::read_dir(&config_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("50_"))
        .count();
    assert_eq!(transform_count, 5, "expected 5 transform files");

    // Run vector validate (--no-environment skips health checks since Kafka isn't running)
    Command::new(vector_bin)
        .arg("validate")
        .arg("--no-environment")
        .arg("--config-dir")
        .arg(&config_dir)
        .env("VECTOR_DATA_DIR", &config.vector.data_dir)
        .output()
        .expect("failed to run vector validate")
}

#[test]
#[ignore] // requires Vector binary — run with: cargo nextest run -E 'test(vector_validate)' --run-ignored all
fn vector_validate_full_chain_memory_buffer() {
    common::skip_if_no_vector!();

    let buffer = BufferConfig {
        buffer_type: "memory".to_string(),
        max_events: Some(500),
        max_size: None,
        when_full: "block".to_string(),
    };

    let (config, work_dir) = build_config(buffer);
    let output = assemble_and_validate(&config, work_dir.path());

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "vector validate failed with memory buffer:\nstdout: {stdout}\nstderr: {stderr}"
    );
}

#[test]
#[ignore] // requires Vector binary
fn vector_validate_full_chain_disk_buffer() {
    common::skip_if_no_vector!();

    let buffer = BufferConfig {
        buffer_type: "disk".to_string(),
        max_events: None,
        max_size: Some(268_435_488),
        when_full: "block".to_string(),
    };

    let (config, work_dir) = build_config(buffer);
    let output = assemble_and_validate(&config, work_dir.path());

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "vector validate failed with disk buffer:\nstdout: {stdout}\nstderr: {stderr}"
    );
}

#[test]
#[ignore] // requires Vector binary
fn vector_validate_full_chain_drop_newest() {
    common::skip_if_no_vector!();

    let buffer = BufferConfig {
        buffer_type: "memory".to_string(),
        max_events: Some(1000),
        max_size: None,
        when_full: "drop_newest".to_string(),
    };

    let (config, work_dir) = build_config(buffer);
    let output = assemble_and_validate(&config, work_dir.path());

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "vector validate failed with drop_newest:\nstdout: {stdout}\nstderr: {stderr}"
    );
}

#[test]
#[ignore] // requires Vector binary
fn vector_validate_full_chain_default_buffer() {
    common::skip_if_no_vector!();

    let (config, work_dir) = build_config(BufferConfig::default());
    let output = assemble_and_validate(&config, work_dir.path());

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "vector validate failed with default buffer:\nstdout: {stdout}\nstderr: {stderr}"
    );
}

#[test]
#[ignore] // requires Vector binary
fn vector_validate_full_chain_sasl_tls() {
    common::skip_if_no_vector!();

    let work_dir = TempDir::new().expect("failed to create work dir");
    let data_dir = work_dir.path().join("data");
    fs::create_dir_all(&data_dir).expect("failed to create data dir");

    let sasl = SaslConfig {
        enabled: true,
        mechanism: "scram_sha_512".into(),
        username: "test-user".into(),
        password: "test-pass".into(),
        secret_dir: None,
    };
    let tls = TlsConfig {
        enabled: true,
        ..Default::default()
    };

    let config = Config {
        pipeline: PipelineConfig {
            name: "sasl-tls-test".to_string(),
        },
        source: SourceConfig {
            brokers: vec!["kafka:9093".to_string()],
            topics: vec!["raw.secure".to_string()],
            group_id: "validate-sasl-group".to_string(),
            decoding: DecodingConfig {
                codec: "json".to_string(),
            },
            sasl: sasl.clone(),
            tls: tls.clone(),
            ..Default::default()
        },
        sink: SinkConfig {
            brokers: vec!["kafka:9093".to_string()],
            topic: "enriched.secure".to_string(),
            key_field: ".org_id".into(),
            encoding: "json".to_string(),
            compression: "zstd".to_string(),
            sasl,
            tls,
            ..Default::default()
        },
        transforms: TransformConfig {
            dir: Some(FIXTURE_TRANSFORMS.to_string()),
            files: None,
        },
        vector: VectorConfig {
            binary: "vector".to_string(),
            data_dir: data_dir.to_string_lossy().to_string(),
            api_address: "0.0.0.0:0".to_string(),
            log_level: "info".to_string(),
            version: String::new(),
            version_check: "disabled".to_string(),
            ..Default::default()
        },
        ..Default::default()
    };

    let config_dir = work_dir.path().join("config");
    assembler::assemble(&config, &config_dir).expect("assembly should succeed");

    // Verify SASL and TLS config appear in generated source YAML
    let source_yaml = fs::read_to_string(config_dir.join("00_source.yaml")).unwrap();
    assert!(
        source_yaml.contains("SCRAM-SHA-512"),
        "SASL mechanism missing from source"
    );
    assert!(
        source_yaml.contains("security.protocol: SASL_SSL"),
        "security.protocol missing"
    );

    // Verify SASL and TLS config appear in generated sink YAML
    let sink_yaml = fs::read_to_string(config_dir.join("90_sink.yaml")).unwrap();
    assert!(
        sink_yaml.contains("SCRAM-SHA-512"),
        "SASL mechanism missing from sink"
    );
    assert!(
        sink_yaml.contains("security.protocol: SASL_SSL"),
        "security.protocol missing from sink"
    );
    assert!(
        sink_yaml.contains("SECRET[dfe_credentials.sink_sasl_password]")
            && !sink_yaml.contains("test-pass"),
        "the sink must name its password by secret reference: {sink_yaml}"
    );

    let vector_bin = common::vector_binary_path()
        .expect("Vector binary should be available (checked by caller)");
    let output = Command::new(vector_bin)
        .arg("validate")
        .arg("--no-environment")
        .arg("--config-dir")
        .arg(&config_dir)
        .env("VECTOR_DATA_DIR", &config.vector.data_dir)
        .output()
        .expect("failed to run vector validate");

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "vector validate failed with SASL+TLS:\nstdout: {stdout}\nstderr: {stderr}"
    );
}

/// `vector validate` does not resolve `SECRET[...]` references -- a missing key
/// validates clean -- so only a running Vector proves the assembled backend,
/// the files the assembler writes and the names the components use line up.
///
/// Runs Vector on the assembled global file beside a stdin -> console pipeline
/// that prints both credentials, and reads them back.
#[test]
fn a_running_vector_reads_the_credentials_the_assembler_hands_it() {
    let Some(vector_bin) = common::vector_binary_path() else {
        common::require_service_in_ci("Vector binary", "scripts/fetch-vector.sh found nothing");
        eprintln!("Skipping: Vector binary not available (run scripts/fetch-vector.sh)");
        return;
    };

    let work = TempDir::new().expect("work dir");
    let data_dir = work.path().join("data");
    fs::create_dir_all(&data_dir).expect("data dir");
    let (mut config, _unused) = build_config(BufferConfig::default());
    config.vector.data_dir = data_dir.to_string_lossy().into_owned();
    config.transforms = TransformConfig::default();
    config.source.sasl = SaslConfig {
        enabled: true,
        username: "probe-user".into(),
        password: "probe-pass".into(),
        ..SaslConfig::default()
    };
    config.sink.sasl = config.source.sasl.clone();

    let assembled = work.path().join("config");
    assembler::assemble(&config, &assembled).expect("assembly should succeed");

    // The assembled global file, with the secret backend, plus a pipeline that
    // prints what the source's references resolve to.
    let probe = work.path().join("probe");
    fs::create_dir_all(&probe).expect("probe dir");
    fs::copy(
        assembled.join("00_global.yaml"),
        probe.join("00_global.yaml"),
    )
    .expect("global");
    let source_yaml = fs::read_to_string(assembled.join("00_source.yaml")).unwrap();
    let parsed: serde_yaml_ng::Value = serde_yaml_ng::from_str(&source_yaml).unwrap();
    let sasl = &parsed["sources"]["dfe_source"]["sasl"];
    let username_ref = sasl["username"].as_str().expect("username reference");
    let password_ref = sasl["password"].as_str().expect("password reference");
    fs::write(
        probe.join("50_probe.yaml"),
        format!(
            "sources:\n  in:\n    type: stdin\ntransforms:\n  show:\n    type: remap\n    \
             inputs: [in]\n    source: |\n      .user = \"{username_ref}\"\n      \
             .pass = \"{password_ref}\"\nsinks:\n  out:\n    type: console\n    \
             inputs: [show]\n    encoding:\n      codec: json\n"
        ),
    )
    .expect("probe pipeline");

    let mut child = Command::new(vector_bin)
        .arg("--quiet")
        .arg("--config-dir")
        .arg(&probe)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("run vector");
    {
        use std::io::Write as _;
        let mut stdin = child.stdin.take().expect("stdin");
        stdin.write_all(b"hello\n").expect("write event");
    }
    let output = child
        .wait_with_output()
        .expect("vector exits at end of input");
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "vector failed on the assembled secret backend: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let event: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap_or_else(|e| {
        panic!("vector printed no event ({e}): {stdout}");
    });
    assert_eq!(event["user"], "probe-user");
    assert_eq!(event["pass"], "probe-pass");
}

#[test]
#[ignore] // requires Vector binary
fn vector_validate_full_chain_sasl_tls_skip_verify() {
    common::skip_if_no_vector!();

    let work_dir = TempDir::new().expect("failed to create work dir");
    let data_dir = work_dir.path().join("data");
    fs::create_dir_all(&data_dir).expect("failed to create data dir");

    let sasl = SaslConfig {
        enabled: true,
        mechanism: "scram_sha_512".into(),
        username: "test-user".into(),
        password: "test-pass".into(),
        secret_dir: None,
    };
    let tls = TlsConfig {
        enabled: true,
        skip_verify: true,
        ..Default::default()
    };

    let config = Config {
        pipeline: PipelineConfig {
            name: "skip-verify-test".to_string(),
        },
        source: SourceConfig {
            brokers: vec!["kafka:9093".to_string()],
            topics: vec!["raw.dev".to_string()],
            group_id: "validate-skip-verify-group".to_string(),
            decoding: DecodingConfig {
                codec: "json".to_string(),
            },
            sasl: sasl.clone(),
            tls: tls.clone(),
            ..Default::default()
        },
        sink: SinkConfig {
            brokers: vec!["kafka:9093".to_string()],
            topic: "enriched.dev".to_string(),
            key_field: String::new(),
            encoding: "json".to_string(),
            compression: "none".to_string(),
            sasl,
            tls,
            ..Default::default()
        },
        transforms: TransformConfig {
            dir: Some(FIXTURE_TRANSFORMS.to_string()),
            files: None,
        },
        vector: VectorConfig {
            binary: "vector".to_string(),
            data_dir: data_dir.to_string_lossy().to_string(),
            api_address: "0.0.0.0:0".to_string(),
            log_level: "info".to_string(),
            version: String::new(),
            version_check: "disabled".to_string(),
            ..Default::default()
        },
        ..Default::default()
    };

    let config_dir = work_dir.path().join("config");
    assembler::assemble(&config, &config_dir).expect("assembly should succeed");

    // Verify skip_verify generates correct Vector TLS and librdkafka options
    let source_yaml = fs::read_to_string(config_dir.join("00_source.yaml")).unwrap();
    assert!(
        source_yaml.contains("verify_certificate: false"),
        "verify_certificate missing from source"
    );
    assert!(
        source_yaml.contains("verify_hostname: false"),
        "verify_hostname missing from source"
    );
    assert!(
        source_yaml.contains("enable.ssl.certificate.verification: 'false'"),
        "librdkafka ssl cert verification not disabled in source"
    );

    let sink_yaml = fs::read_to_string(config_dir.join("90_sink.yaml")).unwrap();
    assert!(
        sink_yaml.contains("verify_certificate: false"),
        "verify_certificate missing from sink"
    );
    assert!(
        sink_yaml.contains("enable.ssl.certificate.verification: 'false'"),
        "librdkafka ssl cert verification not disabled in sink"
    );

    let vector_bin = common::vector_binary_path()
        .expect("Vector binary should be available (checked by caller)");
    let output = Command::new(vector_bin)
        .arg("validate")
        .arg("--no-environment")
        .arg("--config-dir")
        .arg(&config_dir)
        .env("VECTOR_DATA_DIR", &config.vector.data_dir)
        .output()
        .expect("failed to run vector validate");

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "vector validate failed with SASL+TLS+skip_verify:\nstdout: {stdout}\nstderr: {stderr}"
    );
}
