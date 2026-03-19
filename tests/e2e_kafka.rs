#![allow(clippy::unwrap_used, clippy::expect_used)]
// Project:   dfe-transform-vector
// File:      tests/e2e_kafka.rs
// Purpose:   End-to-end Kafka pipeline test via testcontainers
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! End-to-end test: produce → transform → consume via a real Kafka broker.
//!
//! Spins up a Kafka container via testcontainers, assembles a Vector config
//! with a simple remap transform, runs Vector as a subprocess, produces a
//! message to the source topic, and verifies the transformed message appears
//! on the sink topic.
//!
//! Requires Docker and the `vector` binary on PATH.
//! Skipped automatically if either is unavailable.

use std::fs;
use std::process::Command;
use std::time::Duration;

use rdkafka::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use tempfile::TempDir;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::Kafka;

use dfe_transform_vector::config::assembler;
use dfe_transform_vector::config::loader::{
    Config, DecodingConfig, PipelineConfig, SinkConfig, SourceConfig, TransformConfig, VectorConfig,
};

use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;

const SOURCE_TOPIC: &str = "e2e-source";
const SINK_TOPIC: &str = "e2e-sink";
const GROUP_ID: &str = "e2e-test-group";
const CONSUME_TIMEOUT: Duration = Duration::from_secs(30);

fn vector_available() -> bool {
    Command::new("vector").arg("--version").output().is_ok()
}

fn docker_available() -> bool {
    Command::new("docker").arg("info").output().is_ok()
}

async fn create_topics(bootstrap_servers: &str, topics: &[&str]) {
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()
        .expect("failed to create admin client");

    let new_topics: Vec<NewTopic> = topics
        .iter()
        .map(|t| NewTopic::new(t, 1, TopicReplication::Fixed(1)))
        .collect();

    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));

    admin
        .create_topics(&new_topics, &opts)
        .await
        .expect("failed to create topics");
}

fn produce_message(bootstrap_servers: &str, topic: &str, key: &str, payload: &str) {
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("message.timeout.ms", "10000")
        .create()
        .expect("failed to create producer");

    producer
        .send(BaseRecord::to(topic).key(key).payload(payload))
        .expect("failed to send message");

    producer
        .flush(Duration::from_secs(10))
        .expect("flush failed");
}

fn consume_one(bootstrap_servers: &str, topic: &str, timeout: Duration) -> Option<String> {
    consume_one_from_group(bootstrap_servers, topic, "e2e-consumer", timeout)
}

fn consume_one_from_group(
    bootstrap_servers: &str,
    topic: &str,
    group_id: &str,
    timeout: Duration,
) -> Option<String> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("failed to create consumer");

    consumer.subscribe(&[topic]).expect("failed to subscribe");

    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if let Some(result) = consumer.poll(Duration::from_millis(500)) {
            match result {
                Ok(msg) => {
                    return msg
                        .payload_view::<str>()
                        .and_then(|r| r.ok())
                        .map(String::from);
                }
                Err(e) => {
                    eprintln!("consume error: {e}");
                }
            }
        }
    }
    None
}

fn build_test_config(bootstrap_servers: &str, transforms_dir: &str) -> Config {
    Config {
        pipeline: PipelineConfig {
            name: "e2e-test".to_string(),
        },
        source: SourceConfig {
            brokers: vec![bootstrap_servers.to_string()],
            topics: vec![SOURCE_TOPIC.to_string()],
            group_id: GROUP_ID.to_string(),
            decoding: DecodingConfig {
                codec: "json".to_string(),
            },
            ..Default::default()
        },
        sink: SinkConfig {
            brokers: vec![bootstrap_servers.to_string()],
            topic: SINK_TOPIC.to_string(),
            key_field: String::new(),
            encoding: "json".to_string(),
            compression: "none".to_string(),
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
#[ignore] // requires Docker + Vector — run with: cargo nextest run -E 'test(e2e)' --run-ignored all
async fn e2e_kafka_produce_transform_consume() {
    if !docker_available() {
        eprintln!("skipping: Docker not available");
        return;
    }
    if !vector_available() {
        eprintln!("skipping: Vector binary not available");
        return;
    }

    // Start Kafka container (apache/kafka-native for fast startup)
    let kafka_node = Kafka::default()
        .start()
        .await
        .expect("failed to start Kafka container");

    let host = kafka_node.get_host().await.expect("failed to get host");
    let port = kafka_node
        .get_host_port_ipv4(testcontainers_modules::kafka::apache::KAFKA_PORT)
        .await
        .expect("failed to get Kafka port");
    let bootstrap_servers = format!("{host}:{port}");
    eprintln!("Kafka bootstrap: {bootstrap_servers}");

    // Create source and sink topics
    create_topics(&bootstrap_servers, &[SOURCE_TOPIC, SINK_TOPIC]).await;

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

    let mut config = build_test_config(
        &bootstrap_servers,
        transforms_dir.path().to_str().expect("non-utf8 path"),
    );
    config.vector.data_dir = data_dir.to_string_lossy().to_string();

    assembler::assemble(&config, &config_dir).expect("assembly failed");

    assert!(
        config_dir.join("00_source.yaml").exists(),
        "source config missing"
    );
    assert!(
        config_dir.join("90_sink.yaml").exists(),
        "sink config missing"
    );

    // Start Vector subprocess
    let mut vector_proc = Command::new("vector")
        .arg("--config-dir")
        .arg(&config_dir)
        .env("VECTOR_DATA_DIR", &data_dir)
        .env("VECTOR_LOG", "info")
        .spawn()
        .expect("failed to start Vector");

    // Give Vector time to start and connect to Kafka
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Produce a test message
    let test_payload = r#"{"event_type":"test","value":42}"#;
    produce_message(&bootstrap_servers, SOURCE_TOPIC, "test-key", test_payload);
    eprintln!("produced message to {SOURCE_TOPIC}");

    // Consume from sink topic (blocking poll in a spawn_blocking to avoid blocking tokio)
    let bs = bootstrap_servers.clone();
    let received =
        tokio::task::spawn_blocking(move || consume_one(&bs, SINK_TOPIC, CONSUME_TIMEOUT))
            .await
            .expect("consume task panicked");

    // Shut down Vector
    let _ = signal::kill(Pid::from_raw(vector_proc.id() as i32), Signal::SIGTERM);
    let _ = vector_proc.wait();

    // Verify the transformed message
    let received = received.expect("no message received on sink topic within timeout");
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

const RELOAD_SOURCE_TOPIC: &str = "e2e-reload-source";
const RELOAD_SINK_TOPIC: &str = "e2e-reload-sink";

#[tokio::test]
#[ignore] // requires Docker + Vector — run with: cargo nextest run -E 'test(e2e)' --run-ignored all
async fn e2e_kafka_hot_reload_transform_change() {
    if !docker_available() {
        eprintln!("skipping: Docker not available");
        return;
    }
    if !vector_available() {
        eprintln!("skipping: Vector binary not available");
        return;
    }

    // Start Kafka
    let kafka_node = Kafka::default()
        .start()
        .await
        .expect("failed to start Kafka container");

    let host = kafka_node.get_host().await.expect("failed to get host");
    let port = kafka_node
        .get_host_port_ipv4(testcontainers_modules::kafka::apache::KAFKA_PORT)
        .await
        .expect("failed to get Kafka port");
    let bootstrap_servers = format!("{host}:{port}");
    eprintln!("Kafka bootstrap (reload test): {bootstrap_servers}");

    create_topics(
        &bootstrap_servers,
        &[RELOAD_SOURCE_TOPIC, RELOAD_SINK_TOPIC],
    )
    .await;

    // Phase 1: original transform adds phase="original"
    let transforms_dir = TempDir::new().expect("failed to create transforms dir");
    fs::write(
        transforms_dir.path().join("01_phase.yaml"),
        r#"transforms:
  set_phase:
    type: remap
    source: |
      .phase = "original"
"#,
    )
    .expect("failed to write original transform");

    let work_dir = TempDir::new().expect("failed to create work dir");
    let config_dir = work_dir.path().join("config");
    let data_dir = work_dir.path().join("data");
    fs::create_dir_all(&data_dir).expect("failed to create data dir");

    let config = Config {
        pipeline: PipelineConfig {
            name: "reload-test".to_string(),
        },
        source: SourceConfig {
            brokers: vec![bootstrap_servers.clone()],
            topics: vec![RELOAD_SOURCE_TOPIC.to_string()],
            group_id: "e2e-reload-group".to_string(),
            decoding: DecodingConfig {
                codec: "json".to_string(),
            },
            ..Default::default()
        },
        sink: SinkConfig {
            brokers: vec![bootstrap_servers.clone()],
            topic: RELOAD_SINK_TOPIC.to_string(),
            key_field: String::new(),
            encoding: "json".to_string(),
            compression: "none".to_string(),
            ..Default::default()
        },
        transforms: TransformConfig {
            dir: Some(transforms_dir.path().to_string_lossy().to_string()),
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

    assembler::assemble(&config, &config_dir).expect("assembly failed");

    // Start Vector with --watch-config poll (so it picks up SIGHUP)
    let mut vector_proc = Command::new("vector")
        .arg("--config-dir")
        .arg(&config_dir)
        .env("VECTOR_DATA_DIR", &data_dir)
        .env("VECTOR_LOG", "info")
        .spawn()
        .expect("failed to start Vector");

    tokio::time::sleep(Duration::from_secs(8)).await;

    // Produce message and verify phase="original"
    produce_message(
        &bootstrap_servers,
        RELOAD_SOURCE_TOPIC,
        "k1",
        r#"{"msg":"before reload"}"#,
    );
    eprintln!("produced pre-reload message");

    let bs1 = bootstrap_servers.clone();
    let received = tokio::task::spawn_blocking(move || {
        consume_one_from_group(
            &bs1,
            RELOAD_SINK_TOPIC,
            "e2e-reload-consumer-1",
            CONSUME_TIMEOUT,
        )
    })
    .await
    .expect("consume task panicked");

    let received = received.expect("no pre-reload message received");
    eprintln!("pre-reload received: {received}");
    let parsed: serde_json::Value = serde_json::from_str(&received).unwrap();
    assert_eq!(
        parsed.get("phase"),
        Some(&serde_json::Value::String("original".into())),
        "pre-reload message should have phase=original"
    );

    // Phase 2: update transform to set phase="reloaded"
    fs::write(
        transforms_dir.path().join("01_phase.yaml"),
        r#"transforms:
  set_phase:
    type: remap
    source: |
      .phase = "reloaded"
"#,
    )
    .expect("failed to write updated transform");

    // Re-assemble config directory with new transform
    assembler::assemble(&config, &config_dir).expect("re-assembly failed");

    // Send SIGHUP to Vector to trigger config reload
    let vector_pid = vector_proc.id();
    signal::kill(Pid::from_raw(vector_pid as i32), Signal::SIGHUP)
        .expect("failed to send SIGHUP to Vector");
    eprintln!("sent SIGHUP to Vector (PID {vector_pid})");

    // Wait for Vector to reload config
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Produce post-reload message
    produce_message(
        &bootstrap_servers,
        RELOAD_SOURCE_TOPIC,
        "k2",
        r#"{"msg":"after reload"}"#,
    );
    eprintln!("produced post-reload message");

    let bs2 = bootstrap_servers.clone();
    let received = tokio::task::spawn_blocking(move || {
        consume_one_from_group(
            &bs2,
            RELOAD_SINK_TOPIC,
            "e2e-reload-consumer-2",
            CONSUME_TIMEOUT,
        )
    })
    .await
    .expect("consume task panicked");

    // Shut down Vector
    let _ = signal::kill(Pid::from_raw(vector_proc.id() as i32), Signal::SIGTERM);
    let _ = vector_proc.wait();

    // The post-reload consumer may see the pre-reload message first (it reads from earliest).
    // We need to find the message with msg="after reload" and verify it has phase="reloaded".
    let received = received.expect("no post-reload message received");
    eprintln!("post-reload received: {received}");
    let parsed: serde_json::Value = serde_json::from_str(&received).unwrap();

    // If this is the pre-reload message (consumer-2 reads from earliest), it will have
    // phase="original". The second message should have phase="reloaded".
    // To handle this, check if we got the right one; if not, the test assertion will tell us.
    if parsed.get("msg") == Some(&serde_json::Value::String("before reload".into())) {
        // Got the pre-reload message first — consume one more
        let bs3 = bootstrap_servers.clone();
        let received2 = tokio::task::spawn_blocking(move || {
            consume_one_from_group(
                &bs3,
                RELOAD_SINK_TOPIC,
                "e2e-reload-consumer-2",
                CONSUME_TIMEOUT,
            )
        })
        .await
        .expect("consume task panicked");

        let received2 = received2.expect("no second post-reload message received");
        eprintln!("post-reload received (2nd): {received2}");
        let parsed2: serde_json::Value = serde_json::from_str(&received2).unwrap();
        assert_eq!(
            parsed2.get("phase"),
            Some(&serde_json::Value::String("reloaded".into())),
            "post-reload message should have phase=reloaded"
        );
    } else {
        assert_eq!(
            parsed.get("phase"),
            Some(&serde_json::Value::String("reloaded".into())),
            "post-reload message should have phase=reloaded"
        );
    }
}
