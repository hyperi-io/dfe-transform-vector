// Project:   dfe-transform-vector
// File:      tests/integration_vector_validate.rs
// Purpose:   Integration tests that run vector validate against assembled configs
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Vector validate integration tests.
//!
//! Assembles full configs with Kafka source, transforms, and Kafka sink,
//! then runs `vector validate --config-dir` to verify Vector accepts them.
//!
//! Requires the `vector` binary on PATH.
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

const FIXTURE_TRANSFORMS: &str = "tests/fixtures/transforms";

fn vector_available() -> bool {
    Command::new("vector").arg("--version").output().is_ok()
}

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
        },
        ..Default::default()
    };

    (config, work_dir)
}

fn assemble_and_validate(config: &Config, work_dir: &Path) -> std::process::Output {
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
    Command::new("vector")
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
    if !vector_available() {
        eprintln!("skipping: Vector binary not available");
        return;
    }

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
    if !vector_available() {
        eprintln!("skipping: Vector binary not available");
        return;
    }

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
    if !vector_available() {
        eprintln!("skipping: Vector binary not available");
        return;
    }

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
    if !vector_available() {
        eprintln!("skipping: Vector binary not available");
        return;
    }

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
    if !vector_available() {
        eprintln!("skipping: Vector binary not available");
        return;
    }

    let work_dir = TempDir::new().expect("failed to create work dir");
    let data_dir = work_dir.path().join("data");
    fs::create_dir_all(&data_dir).expect("failed to create data dir");

    let sasl = SaslConfig {
        enabled: true,
        mechanism: "scram_sha_512".into(),
        username: "${KAFKA_SASL_USERNAME}".into(),
        password: "${KAFKA_SASL_PASSWORD}".into(),
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

    let output = Command::new("vector")
        .arg("validate")
        .arg("--no-environment")
        .arg("--config-dir")
        .arg(&config_dir)
        .env("VECTOR_DATA_DIR", &config.vector.data_dir)
        .env("KAFKA_SASL_USERNAME", "test-user")
        .env("KAFKA_SASL_PASSWORD", "test-pass")
        .output()
        .expect("failed to run vector validate");

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "vector validate failed with SASL+TLS:\nstdout: {stdout}\nstderr: {stderr}"
    );
}

#[test]
#[ignore] // requires Vector binary
fn vector_validate_full_chain_sasl_tls_skip_verify() {
    if !vector_available() {
        eprintln!("skipping: Vector binary not available");
        return;
    }

    let work_dir = TempDir::new().expect("failed to create work dir");
    let data_dir = work_dir.path().join("data");
    fs::create_dir_all(&data_dir).expect("failed to create data dir");

    let sasl = SaslConfig {
        enabled: true,
        mechanism: "scram_sha_512".into(),
        username: "${KAFKA_SASL_USERNAME}".into(),
        password: "${KAFKA_SASL_PASSWORD}".into(),
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

    let output = Command::new("vector")
        .arg("validate")
        .arg("--no-environment")
        .arg("--config-dir")
        .arg(&config_dir)
        .env("VECTOR_DATA_DIR", &config.vector.data_dir)
        .env("KAFKA_SASL_USERNAME", "test-user")
        .env("KAFKA_SASL_PASSWORD", "test-pass")
        .output()
        .expect("failed to run vector validate");

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "vector validate failed with SASL+TLS+skip_verify:\nstdout: {stdout}\nstderr: {stderr}"
    );
}
