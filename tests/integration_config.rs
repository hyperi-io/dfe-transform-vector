#![allow(unsafe_code, clippy::unwrap_used, clippy::expect_used)]
// Project:   dfe-transform-vector
// File:      tests/integration_config.rs
// Purpose:   Integration tests for config assembly pipeline
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Integration tests for the full config assembly pipeline.
//!
//! Tests the end-to-end flow: big-dial config → YAML generation →
//! transform loading → DAG wiring → config directory assembly.

use std::fs;

use dfe_transform_vector::config::assembler;
use dfe_transform_vector::config::loader::*;
use tempfile::TempDir;

/// Build a complete test config with realistic values.
fn full_config(transforms_dir: Option<String>) -> Config {
    Config {
        pipeline: PipelineConfig {
            name: "syslog-enrichment".into(),
        },
        source: SourceConfig {
            brokers: vec!["kafka-1:9092".into(), "kafka-2:9092".into()],
            topics: vec!["raw_syslog_land".into()],
            group_id: "dfe-transform-vector-syslog-enrichment".into(),
            decoding: DecodingConfig {
                codec: "json".into(),
            },
            sasl: SaslConfig {
                enabled: true,
                mechanism: "scram_sha_512".into(),
                username: "${KAFKA_SASL_USERNAME}".into(),
                password: "${KAFKA_SASL_PASSWORD}".into(),
            },
            tls: TlsConfig {
                enabled: true,
                ..Default::default()
            },
            ..Default::default()
        },
        sink: SinkConfig {
            brokers: vec!["kafka-1:9092".into(), "kafka-2:9092".into()],
            topic: "enriched_syslog_land".into(),
            key_field: ".org_id".into(),
            encoding: "json".into(),
            compression: "zstd".into(),
            sasl: SaslConfig {
                enabled: true,
                mechanism: "scram_sha_512".into(),
                username: "${KAFKA_SASL_USERNAME}".into(),
                password: "${KAFKA_SASL_PASSWORD}".into(),
            },
            tls: TlsConfig {
                enabled: true,
                ..Default::default()
            },
            ..Default::default()
        },
        transforms: TransformConfig {
            dir: transforms_dir,
            files: None,
        },
        vector: VectorConfig::default(),
        health: HealthConfig::default(),
        metrics: MetricsConfig::default(),
        logging: LoggingConfig::default(),
        scaling: ScalingConfig::default(),
        reload: ReloadConfig::default(),
    }
}

/// Create sample transform YAML files in a temp directory.
fn create_transform_files(dir: &std::path::Path) {
    fs::write(
        dir.join("01_parse.yaml"),
        r#"transforms:
  parse:
    type: remap
    inputs:
      - dfe_source
    source: |
      . = parse_json!(.message)
"#,
    )
    .unwrap();

    fs::write(
        dir.join("02_enrich.yaml"),
        r#"transforms:
  enrich:
    type: remap
    inputs:
      - parse
    source: |
      .environment = "production"
      .timestamp = now()
"#,
    )
    .unwrap();

    fs::write(
        dir.join("03_filter.yaml"),
        r#"transforms:
  filter:
    type: filter
    inputs:
      - enrich
    condition: '.level != "debug"'
"#,
    )
    .unwrap();
}

#[test]
fn end_to_end_assembly_with_transforms() {
    let transforms_dir = TempDir::new().unwrap();
    create_transform_files(transforms_dir.path());

    let output_dir = TempDir::new().unwrap();
    let config = full_config(Some(transforms_dir.path().to_string_lossy().into_owned()));

    // Assemble should succeed
    assembler::assemble(&config, output_dir.path()).unwrap();

    // Verify directory structure
    assert!(output_dir.path().join("00_source.yaml").exists());
    assert!(output_dir.path().join("90_sink.yaml").exists());
    assert!(output_dir.path().join("99_observability.yaml").exists());

    // Verify source YAML content
    let source = fs::read_to_string(output_dir.path().join("00_source.yaml")).unwrap();
    assert!(source.contains("dfe_source"));
    assert!(source.contains("type: kafka"));
    assert!(source.contains("kafka-1:9092,kafka-2:9092"));
    assert!(source.contains("raw_syslog_land"));
    assert!(source.contains("SCRAM-SHA-512"));
    assert!(source.contains("${KAFKA_SASL_USERNAME}"));
    assert!(source.contains("cooperative-sticky"));

    // Verify sink YAML content — should wire to "filter" (last transform)
    let sink = fs::read_to_string(output_dir.path().join("90_sink.yaml")).unwrap();
    assert!(sink.contains("dfe_sink"));
    assert!(sink.contains("filter")); // Auto-wired to last transform
    assert!(sink.contains("enriched_syslog_land"));
    assert!(sink.contains(".org_id"));
    assert!(sink.contains("compression: zstd"));
    assert!(sink.contains("acknowledgements"));

    // Verify observability YAML
    let obs = fs::read_to_string(output_dir.path().join("99_observability.yaml")).unwrap();
    assert!(obs.contains("internal_metrics"));
    assert!(obs.contains("prometheus_exporter"));
    assert!(obs.contains("0.0.0.0:9598"));

    // Verify transform files were written flat (3 files, prefixed with 50_)
    let mut files: Vec<String> = fs::read_dir(output_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("50_"))
        .collect();
    files.sort();
    assert_eq!(files.len(), 3);
    assert!(files[0].contains("01_parse"));
    assert!(files[1].contains("02_enrich"));
    assert!(files[2].contains("03_filter"));
}

#[test]
fn end_to_end_assembly_no_transforms_wires_source_to_sink() {
    let output_dir = TempDir::new().unwrap();
    let config = full_config(None);

    assembler::assemble(&config, output_dir.path()).unwrap();

    // Sink should wire directly to dfe_source
    let sink = fs::read_to_string(output_dir.path().join("90_sink.yaml")).unwrap();
    assert!(sink.contains("dfe_source"));

    // No transform files
    let transform_files: Vec<_> = fs::read_dir(output_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("50_"))
        .collect();
    assert!(transform_files.is_empty());
}

#[test]
fn assembly_rejects_broken_dag() {
    let transforms_dir = TempDir::new().unwrap();

    // Create a transform that references a nonexistent input
    fs::write(
        transforms_dir.path().join("01_broken.yaml"),
        r#"transforms:
  broken:
    type: remap
    inputs:
      - nonexistent_component
    source: |
      .x = 1
"#,
    )
    .unwrap();

    let output_dir = TempDir::new().unwrap();
    let config = full_config(Some(transforms_dir.path().to_string_lossy().into_owned()));

    let err = assembler::assemble(&config, output_dir.path()).unwrap_err();
    assert!(err.to_string().contains("undefined input"));
}

#[test]
fn assembly_rejects_cyclic_dag() {
    let transforms_dir = TempDir::new().unwrap();

    // a depends on dfe_source + b, b depends on a → cycle
    fs::write(
        transforms_dir.path().join("01_cycle.yaml"),
        r#"transforms:
  a:
    type: remap
    inputs:
      - dfe_source
      - b
    source: |
      .x = 1
  b:
    type: remap
    inputs:
      - a
    source: |
      .y = 1
"#,
    )
    .unwrap();

    let output_dir = TempDir::new().unwrap();
    let config = full_config(Some(transforms_dir.path().to_string_lossy().into_owned()));

    let err = assembler::assemble(&config, output_dir.path()).unwrap_err();
    assert!(err.to_string().contains("cycle"));
}

#[test]
fn config_load_from_yaml_file() {
    let dir = TempDir::new().unwrap();
    let config_path = dir.path().join("config.yaml");

    fs::write(
        &config_path,
        r#"
pipeline:
  name: yaml-loaded-pipeline
source:
  brokers:
    - kafka:9092
  topics:
    - test-topic
  group_id: test-group
sink:
  brokers:
    - kafka:9092
  topic: output-topic
"#,
    )
    .unwrap();

    let config = Config::load(Some(config_path.to_str().unwrap())).unwrap();
    // Check fields not affected by env var leakage from other tests
    assert_eq!(config.source.topics, vec!["test-topic"]);
    assert_eq!(config.sink.topic, "output-topic");
    assert_eq!(config.source.group_id, "test-group");
}

#[test]
fn config_validation_catches_missing_sink_topic() {
    let config = Config::default();
    let err = config.validate().unwrap_err();
    assert!(err.to_string().contains("sink.topic"));
}

#[test]
fn config_validation_catches_invalid_sasl_mechanism() {
    let mut config = full_config(None);
    config.source.sasl.mechanism = "invalid_mech".into();
    let err = config.validate().unwrap_err();
    assert!(err.to_string().contains("mechanism"));
}

#[test]
fn config_validation_catches_invalid_version_check() {
    let mut config = full_config(None);
    config.vector.version_check = "invalid_mode".into();
    let err = config.validate().unwrap_err();
    assert!(err.to_string().contains("version_check"));
}

#[test]
fn config_validation_catches_invalid_buffer_type() {
    let mut config = full_config(None);
    config.sink.buffer.buffer_type = "invalid".into();
    let err = config.validate().unwrap_err();
    assert!(err.to_string().contains("sink.buffer.type"));
}

#[test]
fn config_validation_catches_invalid_when_full() {
    let mut config = full_config(None);
    config.sink.buffer.when_full = "ignore".into();
    let err = config.validate().unwrap_err();
    assert!(err.to_string().contains("sink.buffer.when_full"));
}

#[test]
fn config_validation_catches_disk_buffer_missing_max_size() {
    let mut config = full_config(None);
    config.sink.buffer.buffer_type = "disk".into();
    config.sink.buffer.max_size = None;
    let err = config.validate().unwrap_err();
    assert!(err.to_string().contains("max_size is required"));
}

#[test]
fn config_validation_catches_disk_buffer_too_small() {
    let mut config = full_config(None);
    config.sink.buffer.buffer_type = "disk".into();
    config.sink.buffer.max_size = Some(1000);
    let err = config.validate().unwrap_err();
    assert!(err.to_string().contains("at least 268435488"));
}

#[test]
fn config_validation_accepts_memory_buffer() {
    let mut config = full_config(None);
    config.sink.buffer = BufferConfig {
        buffer_type: "memory".into(),
        max_events: Some(1000),
        max_size: None,
        when_full: "block".into(),
    };
    config.validate().expect("memory buffer should be valid");
}

#[test]
fn config_validation_accepts_disk_buffer() {
    let mut config = full_config(None);
    config.sink.buffer = BufferConfig {
        buffer_type: "disk".into(),
        max_events: None,
        max_size: Some(268_435_488),
        when_full: "drop_newest".into(),
    };
    config.validate().expect("disk buffer should be valid");
}

#[test]
fn config_env_override_flat() {
    // Set env var, load config, verify override
    // SAFETY: test is single-threaded for this env var, no concurrent access
    unsafe { std::env::set_var("DFE_TRANSFORM_PIPELINE_NAME", "env-override-test") };
    let config = Config::load(None).unwrap();
    assert_eq!(config.pipeline.name, "env-override-test");
    unsafe { std::env::remove_var("DFE_TRANSFORM_PIPELINE_NAME") };
}
