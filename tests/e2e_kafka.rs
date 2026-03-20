#![allow(clippy::unwrap_used, clippy::expect_used)]
// Project:   dfe-transform-vector
// File:      tests/e2e_kafka.rs
// Purpose:   End-to-end Kafka pipeline test (dual-mode: docker/remote)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! End-to-end test: produce -> transform -> consume via a real Kafka broker.
//!
//! Assembles a Vector config with a remap transform, runs Vector as a
//! subprocess, produces a message to the source topic, and verifies the
//! transformed message appears on the sink topic.
//!
//! Test mode (set `TEST_MODE` in `.env`):
//! - `docker` — dfe-docker infra profile (`localhost:19092`, PLAINTEXT)
//! - `remote` — devex cluster from env vars (SASL_SSL)
//!
//! Also requires the `vector` binary on PATH.

mod common;

use std::fs;
use std::process::Command;
use std::time::Duration;

use hyperi_rustlib::transport::Transport;
use hyperi_rustlib::transport::kafka::{KafkaConfig, KafkaTransport};

use tempfile::TempDir;

use dfe_transform_vector::config::assembler;
use dfe_transform_vector::config::loader::{
    Config, DecodingConfig, PipelineConfig, SinkConfig, SourceConfig, TransformConfig, VectorConfig,
};

use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;

const SOURCE_TOPIC: &str = "e2e-transform-source";
const SINK_TOPIC: &str = "e2e-transform-sink";
const GROUP_ID: &str = "e2e-transform-group";
const CONSUME_TIMEOUT: Duration = Duration::from_secs(30);

fn vector_available() -> bool {
    Command::new("vector").arg("--version").output().is_ok()
}

fn build_test_config(kf: &KafkaConfig, transforms_dir: &str) -> Config {
    let has_sasl = kf.sasl_mechanism.is_some() && kf.sasl_username.is_some();
    let has_tls = kf.security_protocol.contains("SSL");
    let brokers: Vec<String> = kf.brokers.clone();

    let sasl = if has_sasl {
        dfe_transform_vector::config::loader::SaslConfig {
            enabled: true,
            mechanism: kf.sasl_mechanism.clone().unwrap_or_default(),
            username: kf
                .sasl_username
                .as_deref()
                .map(|u| format!("${{KAFKA_SASL_USER:-{u}}}"))
                .unwrap_or_default(),
            password: kf
                .sasl_password
                .as_deref()
                .map(|p| format!("${{KAFKA_SASL_PASSWORD:-{p}}}"))
                .unwrap_or_default(),
        }
    } else {
        Default::default()
    };

    let tls = if has_tls {
        dfe_transform_vector::config::loader::TlsConfig {
            enabled: true,
            ..Default::default()
        }
    } else {
        Default::default()
    };

    Config {
        dfe_source: None,
        pipeline: PipelineConfig {
            name: "e2e-test".to_string(),
        },
        source: SourceConfig {
            brokers: brokers.clone(),
            topics: vec![SOURCE_TOPIC.to_string()],
            group_id: GROUP_ID.to_string(),
            decoding: DecodingConfig {
                codec: "json".to_string(),
            },
            sasl: sasl.clone(),
            tls: tls.clone(),
            ..Default::default()
        },
        sink: SinkConfig {
            brokers,
            topic: SINK_TOPIC.to_string(),
            key_field: String::new(),
            encoding: "json".to_string(),
            compression: "none".to_string(),
            sasl,
            tls,
            ..Default::default()
        },
        transforms: TransformConfig {
            dir: Some(transforms_dir.to_string()),
            files: None,
        },
        vector: VectorConfig {
            binary: "vector".to_string(),
            data_dir: String::new(),
            api_address: "0.0.0.0:0".to_string(),
            log_level: "info".to_string(),
            version: String::new(),
            version_check: "disabled".to_string(),
        },
        ..Default::default()
    }
}

#[tokio::test]
async fn e2e_kafka_produce_transform_consume() {
    common::skip_if_no_kafka!();

    if !vector_available() {
        eprintln!("skipping: Vector binary not available");
        return;
    }

    let kf = common::kafka_test_config();
    let mode = common::TestMode::detect();
    eprintln!("TEST_MODE={mode:?}, brokers={:?}", kf.brokers);

    // Create topics via rustlib KafkaAdmin (skip if ACLs prevent it)
    let admin = common::kafka_admin(&kf);
    if let Err(e) = admin
        .create_topics(&[(SOURCE_TOPIC, 1, 1), (SINK_TOPIC, 1, 1)])
        .await
    {
        eprintln!("skipping: topic creation failed (ACL or connectivity): {e}");
        return;
    }

    // Write a simple remap transform that adds a field
    let transforms_dir = TempDir::new().expect("failed to create transforms dir");
    fs::write(
        transforms_dir.path().join("01_add_field.yaml"),
        r#"transforms:
  add_field:
    type: remap
    source: |
      .e2e_transformed = true
      .pipeline = "e2e-test"
"#,
    )
    .expect("failed to write transform");

    // Build config and assemble Vector config dir
    let work_dir = TempDir::new().expect("failed to create work dir");
    let config_dir = work_dir.path().join("config");
    let data_dir = work_dir.path().join("data");
    fs::create_dir_all(&data_dir).expect("failed to create data dir");

    let mut config = build_test_config(&kf, transforms_dir.path().to_str().expect("non-utf8 path"));
    config.vector.data_dir = data_dir.to_string_lossy().to_string();

    assembler::assemble(&config, &config_dir).expect("assembly failed");

    // Pass SASL credentials as env vars for Vector's ${} interpolation
    let mut vector_cmd = Command::new("vector");
    vector_cmd
        .arg("--config-dir")
        .arg(&config_dir)
        .env("VECTOR_DATA_DIR", &data_dir)
        .env("VECTOR_LOG", "info");

    if let Some(ref user) = kf.sasl_username {
        vector_cmd.env("KAFKA_SASL_USER", user);
    }
    if let Some(ref password) = kf.sasl_password {
        vector_cmd.env("KAFKA_SASL_PASSWORD", password);
    }

    let mut vector_proc = vector_cmd.spawn().expect("failed to start Vector");

    // Give Vector time to start and connect to Kafka
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Produce a test message via rustlib KafkaTransport
    let mut producer_config = kf.clone();
    producer_config.topics = vec![SOURCE_TOPIC.to_string()];
    let producer = KafkaTransport::new(&producer_config)
        .await
        .expect("failed to create producer transport");

    let test_payload = br#"{"event_type":"test","value":42}"#;
    let result = producer.send(SOURCE_TOPIC, test_payload).await;
    assert!(
        matches!(result, hyperi_rustlib::transport::SendResult::Ok),
        "failed to produce message: {result:?}"
    );
    eprintln!("produced message to {SOURCE_TOPIC}");

    // Consume from sink topic via rustlib KafkaTransport
    let mut consumer_config = kf.clone();
    consumer_config.topics = vec![SINK_TOPIC.to_string()];
    consumer_config.group = "e2e-consumer".to_string();
    let consumer = KafkaTransport::new(&consumer_config)
        .await
        .expect("failed to create consumer transport");

    let deadline = std::time::Instant::now() + CONSUME_TIMEOUT;
    let mut received_payload: Option<String> = None;

    while std::time::Instant::now() < deadline {
        let messages = consumer.recv(10).await.expect("recv failed");
        if let Some(msg) = messages.first() {
            received_payload = Some(String::from_utf8_lossy(&msg.payload).to_string());
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Shut down Vector
    let _ = signal::kill(Pid::from_raw(vector_proc.id() as i32), Signal::SIGTERM);
    let _ = vector_proc.wait();

    // Verify the transformed message
    let received = received_payload.expect("no message received on sink topic within timeout");
    eprintln!("received: {received}");

    let parsed: serde_json::Value =
        serde_json::from_str(&received).expect("received message is not valid JSON");

    assert_eq!(
        parsed.get("e2e_transformed"),
        Some(&serde_json::Value::Bool(true)),
        "transform did not add e2e_transformed field"
    );
    assert_eq!(
        parsed.get("pipeline"),
        Some(&serde_json::Value::String("e2e-test".to_string())),
        "transform did not add pipeline field"
    );
    assert_eq!(
        parsed.get("value"),
        Some(&serde_json::json!(42)),
        "original value field should be preserved"
    );
}
