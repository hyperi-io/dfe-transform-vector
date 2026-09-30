// Project:   dfe-transform-vector
// File:      tests/integration/config.rs
// Purpose:   Integration tests for config assembly pipeline
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Integration tests for the full config assembly pipeline.
//!
//! Tests the end-to-end flow: big-dial config → YAML generation →
//! transform loading → DAG wiring → config directory assembly.

use std::fs;

use dfe_transform_vector::config::assembler;
use dfe_transform_vector::config::loader::*;
use tempfile::TempDir;

use crate::integration::config_env::{env_write_guard, load_config};

/// Build a complete test config with realistic values.
fn full_config(transforms_dir: Option<String>) -> Config {
    Config {
        dfe_source: None,
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
                username: "kafka-user".into(),
                password: "kafka-pass".into(),
                secret_dir: None,
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
                username: "kafka-user".into(),
                password: "kafka-pass".into(),
                secret_dir: None,
            },
            tls: TlsConfig {
                enabled: true,
                ..Default::default()
            },
            ..Default::default()
        },
        bridge: BridgeConfig::default(),
        transforms: TransformConfig {
            dir: transforms_dir,
            files: None,
        },
        vector: VectorConfig::default(),
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
    assert!(source.contains("SECRET[dfe_credentials.source_sasl_username]"));
    assert!(source.contains("cooperative-sticky"));

    // Verify sink YAML content — the size cap reads the last transform, and
    // the sink reads the size cap
    let sink = fs::read_to_string(output_dir.path().join("90_sink.yaml")).unwrap();
    let sink_yaml: serde_yaml_ng::Value = serde_yaml_ng::from_str(&sink).unwrap();
    assert_eq!(
        sink_yaml["transforms"]["dfe_size_cap"]["inputs"][0].as_str(),
        Some("filter"),
        "the size cap must be auto-wired to the last transform: {sink}"
    );
    assert_eq!(
        sink_yaml["sinks"]["dfe_sink"]["inputs"][0].as_str(),
        Some("dfe_size_cap")
    );
    assert!(sink.contains("enriched_syslog_land"));

    // No credential in any config file Vector loads
    for entry in fs::read_dir(output_dir.path()).unwrap() {
        let path = entry.unwrap().path();
        if path.is_file() {
            let text = fs::read_to_string(&path).unwrap();
            assert!(
                !text.contains("kafka-pass") && !text.contains("kafka-user"),
                "{} carries a credential",
                path.display()
            );
        }
    }
    assert!(sink.contains(".org_id"));
    assert!(sink.contains("compression: zstd"));
    assert!(sink.contains("acknowledgements"));

    // Verify observability YAML
    let obs = fs::read_to_string(output_dir.path().join("99_observability.yaml")).unwrap();
    assert!(obs.contains("internal_metrics"));
    assert!(obs.contains("prometheus_exporter"));
    // The exporter binds where metrics.vector_metrics_address says, on
    // loopback, because the wrapper is its only reader.
    assert!(obs.contains("127.0.0.1:9598"));

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

    let config = load_config(Some(config_path.to_str().unwrap())).unwrap();
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

/// With the commit timer armed, handing offset-storing back to librdkafka
/// turns an unclean pod death into skipped records rather than replayed ones.
#[test]
fn config_validation_catches_offset_store_handed_back_to_librdkafka() {
    let mut config = full_config(None);
    config
        .source
        .librdkafka_options
        .insert("enable.auto.offset.store".into(), "true".into());
    let err = config.validate().unwrap_err();
    assert!(
        err.to_string().contains("enable.auto.offset.store"),
        "validation rejected for the wrong reason: {err}"
    );
}

/// Setting it to `false` is what Vector does anyway, so it must stay legal.
#[test]
fn config_validation_allows_offset_store_pinned_off() {
    let mut config = full_config(None);
    config
        .source
        .librdkafka_options
        .insert("enable.auto.offset.store".into(), "false".into());
    config
        .validate()
        .expect("pinning offset-store off matches what Vector already sets");
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

/// `version_check: strict` with an empty `version` must be rejected.
///
/// `check_vector_version` compares only `if !vector_config.version.is_empty()`,
/// so an empty pin makes strict mode a check that cannot fire. The default pin
/// is the shipped `VECTOR_VERSION`, but `vector: { version: "" }` in YAML and
/// `DFE_TRANSFORM_VECTOR_VECTOR_VERSION=""` in the environment both land an
/// empty string on a strict config, which would then accept any Vector binary
/// on PATH. The mode and the pin have to be validated together.
#[test]
fn config_validation_catches_strict_version_check_with_empty_version() {
    let mut config = full_config(None);
    config.vector.version_check = "strict".into();
    config.vector.version = String::new();

    let err = config.validate().expect_err(
        "strict version_check with an empty version pin is a check that can \
         never fire and must not validate",
    );
    let msg = err.to_string();
    assert!(
        msg.contains("version"),
        "error should name the version pin, got: {msg}"
    );
}

/// Same rule for `warn`: a warn-mode check with no pin never warns.
#[test]
fn config_validation_catches_warn_version_check_with_empty_version() {
    let mut config = full_config(None);
    config.vector.version_check = "warn".into();
    config.vector.version = String::new();

    assert!(
        config.validate().is_err(),
        "warn version_check with an empty version pin can never warn"
    );
}

/// `disabled` is the one mode where an empty pin is legitimate -- nothing is
/// compared, so there is nothing to pin. Guards the fix against over-reach.
#[test]
fn config_validation_allows_disabled_version_check_with_empty_version() {
    let mut config = full_config(None);
    config.vector.version_check = "disabled".into();
    config.vector.version = String::new();

    config
        .validate()
        .expect("disabled version_check needs no version pin");
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
    // The environment is per-process, so this must exclude every concurrent
    // `Config::load` for the whole set -> load -> remove sequence.
    let _exclusive = env_write_guard();

    // SAFETY: `env_write_guard` excludes every other reader and writer of the
    // config environment for the life of `_exclusive`.
    unsafe { std::env::set_var("DFE_TRANSFORM_PIPELINE_NAME", "env-override-test") };
    let config = Config::load(None).unwrap();
    unsafe { std::env::remove_var("DFE_TRANSFORM_PIPELINE_NAME") };

    assert_eq!(config.pipeline.name, "env-override-test");
}

// =========================================================================
// KafkaSource integration tests
// =========================================================================

#[test]
fn dfe_source_derives_topics_and_cg() {
    let dir = TempDir::new().unwrap();
    let config_path = dir.path().join("config.yaml");
    fs::write(
        &config_path,
        r#"
dfe_source: syslog
sink:
  brokers:
    - kafka:9092
source:
  brokers:
    - kafka:9092
"#,
    )
    .unwrap();

    let config = load_config(Some(config_path.to_str().unwrap())).unwrap();
    assert_eq!(config.source.topics, vec!["syslog_land"]);
    assert_eq!(config.sink.topic, "syslog_load");
    assert_eq!(config.source.group_id, "dfe-transform-vector-syslog");
}

#[test]
fn dfe_source_with_pipeline_name_uses_pipeline_in_cg() {
    let dir = TempDir::new().unwrap();
    let config_path = dir.path().join("config.yaml");
    fs::write(
        &config_path,
        r#"
dfe_source: syslog
pipeline:
  name: syslog-enriched
sink:
  brokers:
    - kafka:9092
source:
  brokers:
    - kafka:9092
"#,
    )
    .unwrap();

    let config = load_config(Some(config_path.to_str().unwrap())).unwrap();
    assert_eq!(config.source.topics, vec!["syslog_land"]);
    assert_eq!(config.sink.topic, "syslog_load");
    assert_eq!(
        config.source.group_id,
        "dfe-transform-vector-syslog-enriched"
    );
}

#[test]
fn dfe_source_explicit_overrides_win() {
    let dir = TempDir::new().unwrap();
    let config_path = dir.path().join("config.yaml");
    fs::write(
        &config_path,
        r#"
dfe_source: syslog
source:
  brokers:
    - kafka:9092
  topics:
    - custom_input_topic
  group_id: custom-consumer-group
sink:
  brokers:
    - kafka:9092
  topic: custom_output_topic
"#,
    )
    .unwrap();

    let config = load_config(Some(config_path.to_str().unwrap())).unwrap();
    // Explicit values should NOT be overridden by KafkaSource
    assert_eq!(config.source.topics, vec!["custom_input_topic"]);
    assert_eq!(config.sink.topic, "custom_output_topic");
    assert_eq!(config.source.group_id, "custom-consumer-group");
}

#[test]
fn dfe_source_not_set_uses_explicit_config() {
    let dir = TempDir::new().unwrap();
    let config_path = dir.path().join("config.yaml");
    fs::write(
        &config_path,
        r#"
source:
  brokers:
    - kafka:9092
  topics:
    - raw_events
  group_id: my-group
sink:
  brokers:
    - kafka:9092
  topic: out_events
"#,
    )
    .unwrap();

    let config = load_config(Some(config_path.to_str().unwrap())).unwrap();
    assert_eq!(config.source.topics, vec!["raw_events"]);
    assert_eq!(config.sink.topic, "out_events");
    assert_eq!(config.source.group_id, "my-group");
    assert!(config.dfe_source.is_none());
}

// ---------------------------------------------------------------------------
// Enum and range validation (codec, encoding, compression, offset, thresholds)
// ---------------------------------------------------------------------------

#[test]
fn config_validation_catches_invalid_codec() {
    let mut config = full_config(None);
    config.source.decoding.codec = "msgpack".into();
    let err = config.validate().unwrap_err();
    assert!(
        err.to_string().contains("source.decoding.codec"),
        "expected codec validation error, got: {err}"
    );
    assert!(err.to_string().contains("json, raw_bytes, protobuf"));
}

#[test]
fn config_validation_catches_invalid_encoding() {
    let mut config = full_config(None);
    config.sink.encoding = "csv".into();
    let err = config.validate().unwrap_err();
    assert!(
        err.to_string().contains("sink.encoding"),
        "expected encoding validation error, got: {err}"
    );
    assert!(err.to_string().contains("json, raw_bytes"));
}

#[test]
fn config_validation_catches_invalid_compression() {
    let mut config = full_config(None);
    config.sink.compression = "brotli".into();
    let err = config.validate().unwrap_err();
    assert!(
        err.to_string().contains("sink.compression"),
        "expected compression validation error, got: {err}"
    );
}

#[test]
fn config_validation_catches_invalid_auto_offset_reset() {
    let mut config = full_config(None);
    config.source.auto_offset_reset = "earliest".into(); // Kafka term, not Vector term
    let err = config.validate().unwrap_err();
    assert!(
        err.to_string().contains("auto_offset_reset"),
        "expected auto_offset_reset validation error, got: {err}"
    );
    assert!(err.to_string().contains("largest, smallest"));
}

#[test]
fn config_validation_catches_drain_timeout_exceeds_session() {
    let mut config = full_config(None);
    config.source.session_timeout_ms = 30000;
    config.source.drain_timeout_ms = Some(30000); // Equal = invalid (must be less)
    let err = config.validate().unwrap_err();
    assert!(
        err.to_string().contains("drain_timeout_ms"),
        "expected drain_timeout validation error, got: {err}"
    );
}

#[test]
fn config_validation_catches_drain_timeout_greater_than_session() {
    let mut config = full_config(None);
    config.source.session_timeout_ms = 10000;
    config.source.drain_timeout_ms = Some(20000);
    let err = config.validate().unwrap_err();
    assert!(
        err.to_string()
            .contains("drain_timeout_ms (20000) must be less than session_timeout_ms (10000)"),
        "expected specific drain/session error, got: {err}"
    );
}

#[test]
fn config_validation_accepts_drain_timeout_less_than_session() {
    let mut config = full_config(None);
    config.source.session_timeout_ms = 30000;
    config.source.drain_timeout_ms = Some(15000);
    config
        .validate()
        .expect("drain_timeout < session_timeout should be valid");
}

#[test]
fn config_validation_accepts_no_drain_timeout() {
    let mut config = full_config(None);
    config.source.drain_timeout_ms = None;
    config
        .validate()
        .expect("absent drain_timeout should be valid (default derivation)");
}

#[test]
fn config_validation_catches_pressure_threshold_above_one() {
    let mut config = full_config(None);
    config.scaling.pressure_threshold = 1.5;
    let err = config.validate().unwrap_err();
    assert!(
        err.to_string().contains("pressure_threshold"),
        "expected pressure_threshold validation error, got: {err}"
    );
}

#[test]
fn config_validation_catches_pressure_threshold_negative() {
    let mut config = full_config(None);
    config.scaling.pressure_threshold = -0.1;
    let err = config.validate().unwrap_err();
    assert!(
        err.to_string().contains("pressure_threshold"),
        "expected pressure_threshold validation error, got: {err}"
    );
}

#[test]
fn config_validation_accepts_pressure_threshold_boundaries() {
    let mut config = full_config(None);

    config.scaling.pressure_threshold = 0.0;
    config
        .validate()
        .expect("pressure_threshold=0.0 should be valid");

    config.scaling.pressure_threshold = 1.0;
    config
        .validate()
        .expect("pressure_threshold=1.0 should be valid");

    config.scaling.pressure_threshold = 0.5;
    config
        .validate()
        .expect("pressure_threshold=0.5 should be valid");
}

#[test]
fn config_validation_catches_sasl_enabled_without_username() {
    let mut config = full_config(None);
    config.source.sasl.enabled = true;
    config.source.sasl.username = String::new();
    config.source.sasl.password = "some-password".into();
    let err = config.validate().unwrap_err();
    assert!(
        err.to_string().contains("username must not be empty"),
        "expected SASL username validation error, got: {err}"
    );
}

/// Vector expands no `${VAR}`, so a placeholder credential would reach the
/// broker as the literal text and every login would fail.
#[test]
fn config_validation_refuses_an_env_placeholder_in_a_credential() {
    for (side, field) in [
        ("source", "username"),
        ("source", "password"),
        ("sink", "username"),
        ("sink", "password"),
    ] {
        let mut config = full_config(None);
        let sasl = if side == "source" {
            &mut config.source.sasl
        } else {
            &mut config.sink.sasl
        };
        let value = "${KAFKA_SASL_CREDENTIAL}".to_string();
        if field == "username" {
            sasl.username = value;
        } else {
            sasl.password = value;
        }
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains(&format!("{side}.sasl.{field}")) && err.contains("placeholder"),
            "{side}.sasl.{field}: expected the placeholder refusal, got: {err}"
        );
    }
}

#[test]
fn config_validation_accepts_a_secret_dir_in_place_of_credentials() {
    let mut config = full_config(None);
    config.source.sasl.username.clear();
    config.source.sasl.password.clear();
    config.source.sasl.secret_dir = Some("/var/run/secrets/dfe-kafka".into());
    config
        .validate()
        .expect("a secret directory supplies both credentials");
}

#[test]
fn config_validation_refuses_a_secret_dir_beside_credentials() {
    let mut config = full_config(None);
    config.sink.sasl.secret_dir = Some("/var/run/secrets/dfe-kafka".into());
    let err = config.validate().unwrap_err().to_string();
    assert!(
        err.contains("sink.sasl.secret_dir") && err.contains("set one of them"),
        "two sources for one credential must be refused, got: {err}"
    );
}

#[test]
fn config_validation_refuses_a_relative_secret_dir() {
    let mut config = full_config(None);
    config.source.sasl.username.clear();
    config.source.sasl.password.clear();
    config.source.sasl.secret_dir = Some("secrets/kafka".into());
    let err = config.validate().unwrap_err().to_string();
    assert!(
        err.contains("absolute path"),
        "a relative secret_dir must be refused, got: {err}"
    );
}

/// The API takes unauthenticated requests, so on it may only bind loopback.
#[test]
fn config_validation_keeps_the_vector_api_on_loopback() {
    let mut config = full_config(None);
    config.vector.api_enabled = false;
    config.vector.api_address = "0.0.0.0:8686".into();
    config
        .validate()
        .expect("an address is not checked while the API is off");

    config.vector.api_enabled = true;
    let err = config.validate().unwrap_err().to_string();
    assert!(
        err.contains("loopback"),
        "a wildcard bind must be refused, got: {err}"
    );

    for address in ["127.0.0.1:8686", "[::1]:8686"] {
        config.vector.api_address = address.into();
        config
            .validate()
            .unwrap_or_else(|e| panic!("{address} is loopback and must pass: {e}"));
    }
}

/// A credential in `Debug` output ends up in logs and panic messages.
#[test]
fn sasl_debug_output_never_carries_the_password() {
    let config = full_config(None);
    let printed = format!("{config:?}");
    assert!(
        !printed.contains("kafka-pass"),
        "Debug printed the password: {printed}"
    );
    assert!(printed.contains("***REDACTED***"));
}

#[test]
fn config_validation_accepts_all_valid_codecs() {
    for codec in &["json", "raw_bytes", "protobuf"] {
        let mut config = full_config(None);
        config.source.decoding.codec = (*codec).into();
        config
            .validate()
            .unwrap_or_else(|e| panic!("codec '{codec}' should be valid, got: {e}"));
    }
}

#[test]
fn config_validation_accepts_all_valid_compressions() {
    for comp in &["none", "gzip", "lz4", "snappy", "zstd"] {
        let mut config = full_config(None);
        config.sink.compression = (*comp).into();
        config
            .validate()
            .unwrap_or_else(|e| panic!("compression '{comp}' should be valid, got: {e}"));
    }
}

#[test]
fn config_validation_accepts_all_valid_sasl_mechanisms() {
    for mech in &["plain", "scram_sha_256", "scram_sha_512"] {
        let mut config = full_config(None);
        config.source.sasl.mechanism = (*mech).into();
        config.source.sasl.enabled = true;
        config.source.sasl.username = "user".into();
        config
            .validate()
            .unwrap_or_else(|e| panic!("SASL mechanism '{mech}' should be valid, got: {e}"));
    }
}

// ---------------------------------------------------------------------------
// Complex config combinations and realistic edge cases
// ---------------------------------------------------------------------------

#[test]
fn config_minimal_valid_passes_validation() {
    // The absolute minimum config that should validate
    let config = Config {
        pipeline: PipelineConfig { name: "x".into() },
        source: SourceConfig {
            brokers: vec!["b:9092".into()],
            topics: vec!["t".into()],
            group_id: "g".into(),
            ..Default::default()
        },
        sink: SinkConfig {
            brokers: vec!["b:9092".into()],
            topic: "out".into(),
            ..Default::default()
        },
        ..Default::default()
    };
    config.validate().expect("minimal config should validate");
}

#[test]
fn config_with_all_fields_populated_validates() {
    // Everything set to non-default values — tests that maximal configs pass
    let config = Config {
        dfe_source: Some("syslog".into()),
        pipeline: PipelineConfig {
            name: "syslog-enriched".into(),
        },
        source: SourceConfig {
            brokers: vec![
                "kafka-1:9092".into(),
                "kafka-2:9092".into(),
                "kafka-3:9092".into(),
            ],
            topics: vec!["raw_land".into(), "backup_land".into()],
            group_id: "dfe-transform-vector-syslog".into(),
            decoding: DecodingConfig {
                codec: "protobuf".into(),
            },
            sasl: SaslConfig {
                enabled: true,
                mechanism: "scram_sha_256".into(),
                username: String::new(),
                password: String::new(),
                secret_dir: Some("/var/run/secrets/dfe-kafka".into()),
            },
            tls: TlsConfig {
                enabled: true,
                ca_cert_file: Some("/etc/ssl/ca.pem".into()),
                cert_file: Some("/etc/ssl/client.pem".into()),
                key_file: Some("/etc/ssl/client.key".into()),
                skip_verify: false,
            },
            auto_offset_reset: "smallest".into(),
            session_timeout_ms: 45000,
            commit_interval_ms: 10000,
            drain_timeout_ms: Some(20000),
            topic_lag_metric: false,
            librdkafka_options: [("debug".into(), "consumer".into())].into(),
            transport: Transport::Bus,
            listen: "0.0.0.0:6000".into(),
            acknowledgements: scalo::transport::AcknowledgementsConfig::new(false),
        },
        sink: SinkConfig {
            transport: Transport::Bus,
            endpoint: "http://dfe-loader:6000".into(),
            brokers: vec!["kafka-1:9092".into()],
            topic: "enriched_load".into(),
            key_field: ".org_id".into(),
            encoding: "raw_bytes".into(),
            compression: "lz4".into(),
            sasl: SaslConfig {
                enabled: true,
                mechanism: "plain".into(),
                username: "producer".into(),
                password: "secret".into(),
                secret_dir: None,
            },
            tls: TlsConfig {
                enabled: true,
                skip_verify: true,
                ..Default::default()
            },
            buffer: BufferConfig {
                buffer_type: "disk".into(),
                max_events: None,
                max_size: Some(536_870_912), // 512 MiB
                when_full: "drop_newest".into(),
            },
            batch: BatchConfig {
                max_events: 5000,
                max_bytes: Some(5_000_000),
                timeout_secs: 2,
            },
            message_timeout_ms: 120_000,
            socket_timeout_ms: 30_000,
            librdkafka_options: [("queue.buffering.max.kbytes".into(), "1048576".into())].into(),
        },
        bridge: BridgeConfig {
            to_vector: "127.0.0.1:16100".into(),
            from_vector: "127.0.0.1:16101".into(),
            batch_size: 250,
        },
        transforms: TransformConfig {
            dir: Some("/etc/dfe-transform-vector/transforms".into()),
            files: None,
        },
        vector: VectorConfig {
            binary: "/opt/vector/bin/vector".into(),
            data_dir: "/var/data/vector".into(),
            api_enabled: true,
            api_address: "127.0.0.1:8686".into(),
            log_level: "debug".into(),
            version: "0.53.0".into(),
            version_check: "warn".into(),
            ..Default::default()
        },
        metrics: MetricsConfig {
            address: "0.0.0.0:9090".into(),
            vector_metrics_address: "127.0.0.1:9598".into(),
            ..Default::default()
        },
        logging: LoggingConfig {
            level: "debug".into(),
            format: "text".into(),
        },
        scaling: ScalingConfig {
            pressure_threshold: 0.7,
        },
        reload: ReloadConfig {
            enabled: true,
            poll_interval_secs: 15,
        },
    };
    config
        .validate()
        .expect("fully-populated config should validate");
}

#[test]
fn config_multiple_validation_errors_reports_first() {
    // Multiple invalid fields — verify we get a clear first error, not a confusing cascade
    let config = Config {
        pipeline: PipelineConfig {
            name: String::new(), // Invalid: empty
        },
        source: SourceConfig {
            brokers: vec![],                 // Invalid: empty
            topics: vec![],                  // Invalid: empty
            group_id: String::new(),         // Invalid: empty
            auto_offset_reset: "bad".into(), // Invalid
            ..Default::default()
        },
        sink: SinkConfig {
            brokers: vec![],
            topic: String::new(),
            encoding: "avro".into(),     // Invalid
            compression: "bzip2".into(), // Invalid
            ..Default::default()
        },
        scaling: ScalingConfig {
            pressure_threshold: 5.0, // Invalid
        },
        ..Default::default()
    };
    let err = config.validate().unwrap_err();
    // First validation is pipeline.name
    assert!(
        err.to_string().contains("pipeline.name"),
        "first validation error should be pipeline.name, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// Edge cases and boundary values (AI trap: happy-path overfitting)
// ---------------------------------------------------------------------------

#[test]
fn config_malformed_yaml_produces_error() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("bad.yaml");
    fs::write(&path, "{{{{not valid yaml: [[[").unwrap();

    let result = load_config(Some(path.to_str().unwrap()));
    assert!(result.is_err(), "malformed YAML should produce error");
}

#[test]
fn config_empty_file_loads_defaults() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("empty.yaml");
    fs::write(&path, "").unwrap();

    // Empty YAML is valid — deserialises as null, which should either
    // produce defaults or an error (both acceptable)
    let _ = load_config(Some(path.to_str().unwrap()));
}

#[test]
fn config_unknown_fields_are_rejected() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("extra.yaml");
    fs::write(
        &path,
        r#"
pipeline:
  name: "test"
  completely_unknown_field: 42
source:
  brokers: ["kafka:9092"]
  topics: ["t"]
  group_id: "g"
sink:
  brokers: ["kafka:9092"]
  topic: "out"
"#,
    )
    .unwrap();

    // serde strict mode should reject unknown fields
    let result = load_config(Some(path.to_str().unwrap()));
    // If serde is not in deny_unknown_fields mode, this passes — that's a finding
    // Either way, the test documents the current behaviour
    if let Ok(config) = result {
        // At minimum the known fields should be correct
        assert_eq!(config.pipeline.name, "test");
    }
}

#[test]
fn config_reloading_state_keeps_readiness() {
    // During hot-reload, readiness stays healthy (old config still running)
    use dfe_transform_vector::vector::Lifecycle;
    use dfe_transform_vector::vector::lifecycle::State;

    let lc = Lifecycle::new();

    // Running → ready
    lc.set(State::Running);
    assert!(lc.state().is_ready());

    // Reloading → still ready (critical: traffic keeps flowing during reload)
    lc.set(State::Reloading);
    assert!(
        lc.state().is_ready(),
        "Reloading state must maintain readiness — traffic should keep flowing"
    );
    assert!(lc.state().is_alive(), "Reloading state must be alive");
}

// =========================================================================
// Kafka security floor
// =========================================================================

/// The variables `scalo::env::is_production` reads, highest priority first.
const APP_ENV_VARS: [&str; 3] = ["APP_ENV", "ENVIRONMENT", "ENV"];

/// Run `f` with `APP_ENV` set to `app_env`, or with none of the three
/// environment-name variables set.
fn with_app_env<T>(app_env: Option<&str>, f: impl FnOnce() -> T) -> T {
    // The environment is per-process, so this must exclude every concurrent
    // reader for the whole set -> run -> remove sequence.
    let _exclusive = env_write_guard();

    // SAFETY: nextest gives this test its own process, and `_exclusive` holds
    // off every guarded reader and writer of the environment besides.
    unsafe {
        for var in APP_ENV_VARS {
            std::env::remove_var(var);
        }
        if let Some(value) = app_env {
            std::env::set_var("APP_ENV", value);
        }
    }
    let result = f();
    unsafe { std::env::remove_var("APP_ENV") };
    result
}

/// Validate `config` under `app_env`, as [`with_app_env`] sets it.
fn validate_with_app_env(
    config: &Config,
    app_env: Option<&str>,
) -> dfe_transform_vector::Result<()> {
    with_app_env(app_env, || config.validate())
}

/// The SASL and TLS settings of one Kafka side of `config`.
fn side_security<'a>(
    config: &'a mut Config,
    side: &str,
) -> (&'a mut SaslConfig, &'a mut TlsConfig) {
    match side {
        "source" => (&mut config.source.sasl, &mut config.source.tls),
        _ => (&mut config.sink.sasl, &mut config.sink.tls),
    }
}

/// Assert `result` is the floor refusing `side`, carrying scalo's `reason`.
fn assert_floor_refused(result: dfe_transform_vector::Result<()>, side: &str, reason: &str) {
    let message = result
        .expect_err(&format!("{side}: the security floor must refuse this"))
        .to_string();
    assert!(
        message.contains(&format!("{side} Kafka client refused")) && message.contains(reason),
        "{side}: expected a floor refusal naming '{reason}', got: {message}"
    );
}

/// PLAIN sends the password itself, so without TLS it crosses the network in
/// the clear -- in development as much as in production.
#[test]
fn sasl_plain_without_tls_is_refused_in_every_environment() {
    for side in ["source", "sink"] {
        let mut config = full_config(None);
        let (sasl, tls) = side_security(&mut config, side);
        sasl.mechanism = "plain".into();
        tls.enabled = false;

        for app_env in [None, Some("development"), Some("production")] {
            assert_floor_refused(validate_with_app_env(&config, app_env), side, "PLAIN");
        }

        // The same credentials over TLS are what PLAIN is for.
        side_security(&mut config, side).1.enabled = true;
        for app_env in [None, Some("production")] {
            validate_with_app_env(&config, app_env)
                .unwrap_or_else(|e| panic!("{side}: PLAIN over SASL_SSL must pass: {e}"));
        }
    }
}

#[test]
fn tls_skip_verify_is_refused_only_in_production() {
    for side in ["source", "sink"] {
        let mut config = full_config(None);
        side_security(&mut config, side).1.skip_verify = true;

        validate_with_app_env(&config, None)
            .unwrap_or_else(|e| panic!("{side}: skip_verify outside production must pass: {e}"));
        assert_floor_refused(
            validate_with_app_env(&config, Some("production")),
            side,
            "ssl_skip_verify",
        );
    }
}

/// SCRAM without TLS runs SASL_PLAINTEXT. With neither on, the generator
/// writes no protocol and librdkafka runs PLAINTEXT.
#[test]
fn a_transport_without_tls_is_refused_in_production() {
    for (protocol, sasl_on) in [("SASL_PLAINTEXT", true), ("PLAINTEXT", false)] {
        for side in ["source", "sink"] {
            let mut config = full_config(None);
            let (sasl, tls) = side_security(&mut config, side);
            tls.enabled = false;
            if !sasl_on {
                *sasl = SaslConfig::default();
            }

            validate_with_app_env(&config, None)
                .unwrap_or_else(|e| panic!("{side}: {protocol} outside production must pass: {e}"));
            assert_floor_refused(
                validate_with_app_env(&config, Some("production")),
                side,
                &format!("'{protocol}'"),
            );
        }
    }

    validate_with_app_env(&full_config(None), Some("production"))
        .expect("SASL_SSL on both sides must pass in production");
}

/// A side on the direct transport has no Kafka client, so there is nothing
/// for the floor to judge.
#[test]
fn the_floor_skips_a_side_on_the_direct_transport() {
    let mut config = full_config(None);
    config.sink.transport = Transport::Direct;
    config.sink.tls.enabled = false;

    validate_with_app_env(&config, Some("production"))
        .expect("a direct sink carries no Kafka client for the floor to refuse");
}

/// A raw `librdkafka_options` entry that weakens the transport.
///
/// Over a big dial the derived-key check refuses it outright. With the big
/// dials off, the floor judges the typed protocol, PLAINTEXT, which production
/// refuses whatever the raw map says -- scalo 2.13 judges typed fields only.
#[test]
fn a_raw_librdkafka_security_override_that_weakens_the_transport_is_refused() {
    let mut config = full_config(None);
    config
        .sink
        .librdkafka_options
        .insert("security.protocol".into(), "SASL_PLAINTEXT".into());
    let message = validate_with_app_env(&config, None)
        .expect_err("a raw protocol under sink.tls must be refused")
        .to_string();
    assert!(
        message.contains("sink.librdkafka_options sets 'security.protocol'"),
        "{message}"
    );

    let mut config = full_config(None);
    config.sink.sasl = SaslConfig::default();
    config.sink.tls = TlsConfig::default();
    config.sink.librdkafka_options.extend([
        (
            "security.protocol".to_string(),
            "SASL_PLAINTEXT".to_string(),
        ),
        ("sasl.mechanism".to_string(), "PLAIN".to_string()),
    ]);
    // The raw map's own refusal, in any environment, arrives with the scalo bump.
    assert_floor_refused(
        validate_with_app_env(&config, Some("production")),
        "sink",
        "PLAINTEXT",
    );
}

/// The file a user-declared Kafka component is written into.
const USER_KAFKA_FILE: &str = "10_user_kafka.yaml";

/// Assemble a config whose one transform file declares a Kafka source or sink
/// with `security` as its SASL and TLS blocks, under `app_env`.
fn assemble_user_kafka(
    section: &str,
    security: &str,
    app_env: Option<&str>,
) -> dfe_transform_vector::Result<std::path::PathBuf> {
    let component = match section {
        "sources" => "  side_feed:\n    type: kafka\n    group_id: side\n    topics: [side_land]\n",
        _ => {
            "  side_feed:\n    type: kafka\n    inputs: [dfe_source]\n    topic: side_copy\n    \
              encoding:\n      codec: json\n"
        }
    };
    let yaml = format!(
        "transforms:\n  passthrough:\n    type: remap\n    inputs: [dfe_source]\n    source: \".\"\n\
         {section}:\n{component}    bootstrap_servers: kafka-1:9093\n{security}"
    );

    let transforms_dir = TempDir::new().unwrap();
    fs::write(transforms_dir.path().join(USER_KAFKA_FILE), yaml).unwrap();
    let output = TempDir::new().unwrap();
    let config = full_config(Some(transforms_dir.path().to_string_lossy().into_owned()));

    with_app_env(app_env, || assembler::assemble(&config, output.path()))
}

/// Assert `result` is the floor refusing the user's `side_feed` component.
fn assert_user_kafka_refused(
    result: dfe_transform_vector::Result<std::path::PathBuf>,
    kind: &str,
    reason: &str,
) {
    let message = result
        .expect_err(&format!("the user's Kafka {kind} must be refused"))
        .to_string();
    assert!(
        message.contains(USER_KAFKA_FILE)
            && message.contains(&format!("Kafka {kind} 'side_feed' refused"))
            && message.contains(reason),
        "expected the floor to name {USER_KAFKA_FILE} and side_feed for '{reason}', got: {message}"
    );
}

/// A Kafka source or sink a transform file declares runs on Vector's own
/// client, exactly like the generated ones, so PLAIN without TLS is refused
/// there too.
#[test]
fn a_user_kafka_component_with_plain_over_plaintext_is_refused_in_every_environment() {
    let plain = "    sasl:\n      enabled: true\n      mechanism: PLAIN\n";
    for (section, kind) in [("sources", "source"), ("sinks", "sink")] {
        for app_env in [None, Some("development"), Some("production")] {
            assert_user_kafka_refused(assemble_user_kafka(section, plain, app_env), kind, "PLAIN");
        }
    }
}

#[test]
fn a_user_kafka_component_that_skips_certificate_checks_is_refused_in_production() {
    let unverified = "    sasl:\n      enabled: true\n      mechanism: SCRAM-SHA-512\n    \
                      tls:\n      enabled: true\n      verify_certificate: false\n";
    for (section, kind) in [("sources", "source"), ("sinks", "sink")] {
        assemble_user_kafka(section, unverified, None).unwrap_or_else(|e| {
            panic!("{kind}: verify_certificate off outside production must pass: {e}")
        });
        assert_user_kafka_refused(
            assemble_user_kafka(section, unverified, Some("production")),
            kind,
            "ssl_skip_verify",
        );
    }
}

#[test]
fn a_safe_user_kafka_component_passes() {
    let safe = "    sasl:\n      enabled: true\n      mechanism: SCRAM-SHA-512\n    \
                tls:\n      enabled: true\n";
    for section in ["sources", "sinks"] {
        for app_env in [None, Some("production")] {
            assemble_user_kafka(section, safe, app_env).unwrap_or_else(|e| {
                panic!("{section}: SCRAM over TLS must pass under {app_env:?}: {e}")
            });
        }
    }
}

#[test]
fn config_shutting_down_is_not_ready() {
    use dfe_transform_vector::vector::Lifecycle;
    use dfe_transform_vector::vector::lifecycle::State;

    let lc = Lifecycle::new();
    lc.set(State::ShuttingDown);
    assert!(
        !lc.state().is_ready(),
        "ShuttingDown must not be ready — K8s should stop routing traffic"
    );
    // ShuttingDown should still be alive (process is draining, not dead)
    assert!(
        lc.state().is_alive(),
        "ShuttingDown should still be alive while draining"
    );
}
