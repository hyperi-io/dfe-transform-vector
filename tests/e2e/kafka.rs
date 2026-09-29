// Project:   dfe-transform-vector
// File:      tests/e2e/kafka.rs
// Purpose:   E2E Kafka pipeline test (dual-mode: docker or remote)
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! End-to-end Kafka pipeline test.
//!
//! Produces a message, lets Vector process it through our assembled config
//! with a remap transform, consumes from the sink topic, and verifies the
//! transform was applied. Validates the full generated Vector YAML against
//! a real Kafka broker — including librdkafka options, security protocol
//! auto-injection, and DAG wiring.
//!
//! **Backend selection** (automatic — see `tests/common/mod.rs`):
//! 1. Live cluster from `.env` (preferred — realistic multi-broker setup, faster).
//! 2. Testcontainers Apache Kafka (fallback when live is unavailable).
//! 3. Skipped entirely if neither is available.
//!
//! Requires Vector binary. Skips gracefully if not found.
//!
//! **Cleanup:** Vector subprocess uses `kill_on_drop(true)` — guaranteed
//! termination on any exit path. Kafka containers stop on fixture drop.
//! Topics have unique nanosecond suffixes so residuals do not affect repeat
//! runs; explicit delete runs on the happy path.
//!
//! Run with: `cargo nextest run --test e2e --run-ignored all`

use std::fs;
use std::process::Stdio;
use std::time::{Duration, Instant};

use dfe_transform_vector::config::assembler;
use dfe_transform_vector::config::loader::{
    Config, DecodingConfig, MetricsConfig, PipelineConfig, SaslConfig, SinkConfig, SourceConfig,
    TlsConfig, TransformConfig, VectorConfig,
};
use dfe_transform_vector::config::validate::vector_validate;
use dfe_transform_vector::metrics::scrape::DeadLetterDrops;
use dfe_transform_vector::metrics::{DEAD_LETTERS_DROPPED, TOO_LARGE};
use scalo::transport::kafka::{KafkaAdmin, KafkaConfig, KafkaTransport};
use scalo::transport::{TransportBase, TransportReceiver, TransportSender};
use tempfile::TempDir;
use tokio::process::Command;
use tokio::time::sleep;

use crate::common::{KafkaFixture, vector_binary_path};

/// Generate a unique topic suffix per test run to avoid collisions.
fn unique_suffix() -> String {
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{nanos:x}")
}

/// Write a simple remap transform that adds an `enriched` field.
fn write_enrich_transform(dir: &std::path::Path) {
    fs::create_dir_all(dir).expect("create transforms dir");
    fs::write(
        dir.join("01_enrich.yaml"),
        r#"transforms:
  enrich:
    type: remap
    inputs:
      - dfe_source
    source: |
      .enriched = true
      .processed_at = to_string(now())
"#,
    )
    .expect("write enrich transform");
}

/// Build a `dfe-transform-vector` Config that mirrors a scalo `KafkaConfig`.
/// Claim an ephemeral loopback address for Vector's `prometheus_exporter`.
///
/// The default 9598 collides with anything else running on the host, and the
/// wrapper now drives the exporter address from config.
fn free_loopback_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    format!("127.0.0.1:{}", addr.port())
}

fn config_from_kafka_test_config(
    kf: &KafkaConfig,
    source_topic: &str,
    sink_topic: &str,
    group_id: &str,
    transforms_dir: &str,
    data_dir: &str,
    vector_metrics_address: &str,
) -> Config {
    let protocol = kf.security_protocol.to_lowercase();
    let sasl_enabled = protocol.starts_with("sasl");
    let tls_enabled = protocol.contains("ssl");

    let sasl = if sasl_enabled {
        SaslConfig {
            enabled: true,
            // scalo uses "SCRAM-SHA-512" style, ours uses lowercase_underscore
            mechanism: kf
                .sasl_mechanism
                .clone()
                .unwrap_or_else(|| "scram_sha_512".into())
                .to_lowercase()
                .replace('-', "_"),
            username: kf.sasl_username.clone().unwrap_or_default(),
            password: kf
                .sasl_password
                .as_ref()
                .map(|s| s.expose().to_string())
                .unwrap_or_default(),
            secret_dir: None,
        }
    } else {
        SaslConfig::default()
    };

    let tls = TlsConfig {
        enabled: tls_enabled,
        skip_verify: kf.ssl_skip_verify,
        ..Default::default()
    };

    Config {
        pipeline: PipelineConfig {
            name: "e2e-pipeline".into(),
        },
        source: SourceConfig {
            brokers: kf.brokers.clone(),
            topics: vec![source_topic.into()],
            group_id: group_id.into(),
            decoding: DecodingConfig {
                codec: "json".into(),
            },
            sasl: sasl.clone(),
            tls: tls.clone(),
            auto_offset_reset: "smallest".into(),
            ..Default::default()
        },
        sink: SinkConfig {
            brokers: kf.brokers.clone(),
            topic: sink_topic.into(),
            encoding: "json".into(),
            compression: "none".into(),
            sasl,
            tls,
            ..Default::default()
        },
        transforms: TransformConfig {
            dir: Some(transforms_dir.into()),
            files: None,
        },
        vector: VectorConfig {
            binary: "vector".into(),
            data_dir: data_dir.into(),
            api_address: "127.0.0.1:0".into(),
            log_level: "warn".into(),
            version: String::new(),
            version_check: "disabled".into(),
            ..Default::default()
        },
        metrics: MetricsConfig {
            vector_metrics_address: vector_metrics_address.into(),
            ..Default::default()
        },
        ..Default::default()
    }
}

#[tokio::test]
#[ignore] // requires Vector binary + Kafka (live or Docker)
async fn e2e_kafka_pipeline_produces_consumes_with_transform() {
    crate::common::skip_if_no_vector!();

    let Some(fixture) = KafkaFixture::acquire("kafka-pipeline-produces-consumes").await else {
        crate::common::require_service_in_ci("Kafka", "live unreachable and no Docker");
        eprintln!("Skipping: no Kafka backend available (live unreachable AND no Docker)");
        return;
    };
    let kf_base = fixture.config.clone();

    let vector_bin = vector_binary_path().expect("vector binary");

    // Unique topic names per run — residuals on panic are harmless.
    let suffix = unique_suffix();
    let source_topic = format!("dfe_e2e_src_{suffix}");
    let sink_topic = format!("dfe_e2e_sink_{suffix}");
    let group_id = format!("dfe-e2e-{suffix}");

    let mut admin_cfg = kf_base.clone();
    admin_cfg.group = group_id.clone();
    admin_cfg.topics = vec![source_topic.clone()];
    let admin = KafkaAdmin::new(&admin_cfg).expect("admin");

    admin
        .create_topics(&[(&source_topic, 1, 1), (&sink_topic, 1, 1)])
        .await
        .expect("create topics");

    // Assemble dfe-transform-vector config
    let work = TempDir::new().expect("work dir");
    let data_dir = work.path().join("data");
    fs::create_dir_all(&data_dir).expect("data dir");
    let transforms_dir = work.path().join("transforms");
    write_enrich_transform(&transforms_dir);

    let config_dir = work.path().join("cfg");
    let exporter_addr = free_loopback_addr();
    let dfe_config = config_from_kafka_test_config(
        &kf_base,
        &source_topic,
        &sink_topic,
        &group_id,
        &transforms_dir.to_string_lossy(),
        &data_dir.to_string_lossy(),
        &exporter_addr,
    );
    dfe_config.validate().expect("config validates");
    assembler::assemble(&dfe_config, &config_dir).expect("assemble config");

    // Spawn Vector against real Kafka. kill_on_drop(true) ensures the
    // subprocess is SIGKILLed on any exit path — panic, assertion failure,
    // or normal return — so we never leak Vector processes.
    let mut vector_cmd = Command::new(vector_bin);
    vector_cmd
        .arg("--config-dir")
        .arg(&config_dir)
        .env("VECTOR_LOG", "warn")
        .env("VECTOR_DATA_DIR", &data_dir)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);

    let mut vector_child = vector_cmd.spawn().expect("spawn vector");

    // Run the test body, capturing the result so we can clean up topics
    // before reporting the failure (Vector cleanup is automatic via
    // kill_on_drop).
    let test_outcome = run_pipeline_assertions(
        &kf_base,
        &source_topic,
        &sink_topic,
        &group_id,
        &suffix,
        &config_dir,
        &exporter_addr,
    )
    .await;

    // Ensure Vector is reaped before topic cleanup — avoids races where
    // Vector is still holding consumer group membership.
    let _ = vector_child.start_kill();
    let _ = tokio::time::timeout(Duration::from_secs(3), vector_child.wait()).await;

    // Explicit topic cleanup on the happy path.
    let _ = admin
        .delete_topics(&[source_topic.as_str(), sink_topic.as_str()])
        .await;

    // Surface test failure AFTER cleanup.
    if let Err(msg) = test_outcome {
        panic!("pipeline test failed: {msg}");
    }
}

/// Against a broker that only accepts SCRAM-SHA-512, as every DFE tier's does,
/// the supervisor's startup `vector validate` passes and the running Vector
/// authenticates from the secret backend and delivers.
///
/// Owns its broker, so it runs by default rather than opt-in.
#[tokio::test]
async fn e2e_a_scram_broker_passes_startup_validation_and_delivers() {
    let Some(vector_bin) = vector_binary_path() else {
        crate::common::require_service_in_ci(
            "Vector binary",
            "scripts/fetch-vector.sh found nothing",
        );
        eprintln!("Skipping: Vector binary not available (run scripts/fetch-vector.sh)");
        return;
    };
    let Some(fixture) = KafkaFixture::scram("kafka-scram-startup-validation").await else {
        crate::common::require_service_in_ci("Docker", "the SCRAM broker container did not start");
        eprintln!("Skipping: no SCRAM broker (Docker unavailable)");
        return;
    };
    let kf_base = fixture.config.clone();

    let suffix = unique_suffix();
    let source_topic = format!("dfe_e2e_scram_src_{suffix}");
    let sink_topic = format!("dfe_e2e_scram_sink_{suffix}");
    let group_id = format!("dfe-e2e-scram-{suffix}");

    // Also the control: the fixture's own credentials authenticate.
    let mut admin_cfg = kf_base.clone();
    admin_cfg.group = group_id.clone();
    let admin = KafkaAdmin::new(&admin_cfg).expect("admin");
    admin
        .create_topics(&[(&source_topic, 1, 1), (&sink_topic, 1, 1)])
        .await
        .expect("create topics over SCRAM");

    let work = TempDir::new().expect("work dir");
    let data_dir = work.path().join("data");
    fs::create_dir_all(&data_dir).expect("data dir");
    let transforms_dir = work.path().join("transforms");
    write_enrich_transform(&transforms_dir);
    let config_dir = work.path().join("cfg");
    let exporter_addr = free_loopback_addr();
    let mut dfe_config = config_from_kafka_test_config(
        &kf_base,
        &source_topic,
        &sink_topic,
        &group_id,
        &transforms_dir.to_string_lossy(),
        &data_dir.to_string_lossy(),
        &exporter_addr,
    );
    dfe_config.vector.binary = vector_bin.to_string_lossy().into_owned();
    assert!(dfe_config.source.sasl.enabled && dfe_config.sink.sasl.enabled);
    dfe_config.validate().expect("config validates");
    assembler::assemble(&dfe_config, &config_dir).expect("assemble config");

    // The gate the supervisor runs before it spawns Vector.
    vector_validate(&dfe_config.vector, &config_dir)
        .await
        .expect("startup vector validate must pass against a SCRAM broker");

    let mut vector_child = Command::new(vector_bin)
        .arg("--config-dir")
        .arg(&config_dir)
        .env("VECTOR_LOG", "warn")
        .env("VECTOR_DATA_DIR", &data_dir)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn vector");

    let outcome = run_pipeline_assertions(
        &kf_base,
        &source_topic,
        &sink_topic,
        &group_id,
        &suffix,
        &config_dir,
        &exporter_addr,
    )
    .await;

    let _ = vector_child.start_kill();
    let _ = tokio::time::timeout(Duration::from_secs(3), vector_child.wait()).await;

    if let Err(msg) = outcome {
        panic!("SCRAM pipeline test failed: {msg}");
    }
}

/// The sink's `message.max.bytes` in the size-cap test: small enough that an
/// oversize record is far under the broker's and the test producer's limits.
const SMALL_MESSAGE_MAX_BYTES: &str = "200000";

/// A record on the bus over the sink's ceiling is dropped by the size cap and
/// counts once in `pipeline_dead_letters_dropped_total{reason="too_large"}`,
/// however many times the exporter is scraped afterwards.
#[tokio::test]
#[ignore] // requires Vector binary + Kafka (live or Docker)
async fn e2e_an_oversize_record_on_the_bus_counts_one_too_large_drop() {
    crate::common::skip_if_no_vector!();

    let Some(fixture) = KafkaFixture::acquire("kafka-oversize-counts-once").await else {
        crate::common::require_service_in_ci("Kafka", "live unreachable and no Docker");
        eprintln!("Skipping: no Kafka backend available (live unreachable AND no Docker)");
        return;
    };
    let kf_base = fixture.config.clone();
    let vector_bin = vector_binary_path().expect("vector binary");

    let suffix = unique_suffix();
    let source_topic = format!("dfe_e2e_big_src_{suffix}");
    let sink_topic = format!("dfe_e2e_big_sink_{suffix}");
    let group_id = format!("dfe-e2e-big-{suffix}");

    let mut admin_cfg = kf_base.clone();
    admin_cfg.group = group_id.clone();
    admin_cfg.topics = vec![source_topic.clone()];
    let admin = KafkaAdmin::new(&admin_cfg).expect("admin");
    admin
        .create_topics(&[(&source_topic, 1, 1), (&sink_topic, 1, 1)])
        .await
        .expect("create topics");

    let work = TempDir::new().expect("work dir");
    let data_dir = work.path().join("data");
    fs::create_dir_all(&data_dir).expect("data dir");
    let transforms_dir = work.path().join("transforms");
    write_enrich_transform(&transforms_dir);
    let config_dir = work.path().join("cfg");
    let exporter_addr = free_loopback_addr();
    let mut dfe_config = config_from_kafka_test_config(
        &kf_base,
        &source_topic,
        &sink_topic,
        &group_id,
        &transforms_dir.to_string_lossy(),
        &data_dir.to_string_lossy(),
        &exporter_addr,
    );
    dfe_config
        .sink
        .librdkafka_options
        .insert("message.max.bytes".into(), SMALL_MESSAGE_MAX_BYTES.into());
    dfe_config.validate().expect("config validates");
    assembler::assemble(&dfe_config, &config_dir).expect("assemble config");

    let mut vector_child = Command::new(vector_bin)
        .arg("--config-dir")
        .arg(&config_dir)
        .env("VECTOR_LOG", "warn")
        .env("VECTOR_DATA_DIR", &data_dir)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn vector");

    let outcome = run_oversize_assertions(
        &kf_base,
        &source_topic,
        &sink_topic,
        &group_id,
        &suffix,
        &exporter_addr,
    )
    .await;

    let _ = vector_child.start_kill();
    let _ = tokio::time::timeout(Duration::from_secs(3), vector_child.wait()).await;
    let _ = admin
        .delete_topics(&[source_topic.as_str(), sink_topic.as_str()])
        .await;

    if let Err(msg) = outcome {
        panic!("oversize test failed: {msg}");
    }
}

/// What `pipeline_dead_letters_dropped_total{reason="too_large"}` gained since
/// `snapshotter`'s last snapshot.
fn too_large_since(snapshotter: &metrics_util::debugging::Snapshotter) -> u64 {
    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, ..)| {
            key.key().name() == DEAD_LETTERS_DROPPED
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "reason" && l.value() == TOO_LARGE)
        })
        .map(|(.., value)| match value {
            metrics_util::debugging::DebugValue::Counter(n) => n,
            _ => 0,
        })
        .sum()
}

/// Send one oversize record then one marker, wait for the marker downstream,
/// then check the size cap's drop reaches scalo's counter exactly once.
async fn run_oversize_assertions(
    kf_base: &KafkaConfig,
    source_topic: &str,
    sink_topic: &str,
    group_id: &str,
    suffix: &str,
    exporter_addr: &str,
) -> Result<(), String> {
    sleep(Duration::from_secs(3)).await;

    let mut producer_cfg = kf_base.clone();
    producer_cfg.group = format!("{group_id}-producer");
    producer_cfg.topics = vec![source_topic.to_string()];
    let producer = KafkaTransport::new(&producer_cfg)
        .await
        .map_err(|e| format!("producer transport: {e}"))?;
    // One partition, so the marker reaches the size cap after the oversize one.
    let oversize = format!(
        r#"{{"id":"{suffix}-big","blob":"{}"}}"#,
        "x".repeat(300_000)
    );
    let marker = format!(r#"{{"id":"{suffix}-marker"}}"#);
    for payload in [oversize, marker] {
        let sent = producer
            .send(source_topic, bytes::Bytes::from(payload.into_bytes()))
            .await;
        if !matches!(
            sent,
            scalo::transport::SendResult::Ok | scalo::transport::SendResult::Backpressured
        ) {
            let _ = producer.close().await;
            return Err(format!("producer send failed: {sent:?}"));
        }
    }
    let _ = producer.close().await;

    let mut consumer_cfg = kf_base.clone();
    consumer_cfg.group = format!("{group_id}-consumer");
    consumer_cfg.topics = vec![sink_topic.to_string()];
    consumer_cfg.auto_offset_reset = "earliest".into();
    let consumer = KafkaTransport::new(&consumer_cfg)
        .await
        .map_err(|e| format!("consumer transport: {e}"))?;
    let deadline = Instant::now() + Duration::from_secs(30);
    let (mut marker_seen, mut oversize_seen) = (false, false);
    while !marker_seen && Instant::now() < deadline {
        match consumer.recv(10).await {
            Ok(batch) => {
                for record in &batch.records {
                    let body = String::from_utf8_lossy(&record.payload);
                    marker_seen |= body.contains(&format!("{suffix}-marker"));
                    oversize_seen |= body.contains(&format!("{suffix}-big"));
                }
            }
            Err(_) => sleep(Duration::from_millis(500)).await,
        }
    }
    let _ = consumer.close().await;
    if !marker_seen {
        return Err("the marker did not reach the sink topic within 30s".into());
    }
    if oversize_seen {
        return Err("the oversize record reached the sink topic past the size cap".into());
    }

    // Vector publishes internal metrics on its own flush interval.
    let mut merger = dfe_transform_vector::metrics::scrape::ExpositionMerger::default();
    let mut derived = dfe_transform_vector::metrics::scrape::Derived::default();
    let deadline = Instant::now() + Duration::from_secs(20);
    while derived.oversize < 1.0 && Instant::now() < deadline {
        if let Ok(body) =
            dfe_transform_vector::metrics::scrape::fetch_exposition(exporter_addr).await
        {
            derived = merger.merge(&body);
        }
        if derived.oversize < 1.0 {
            sleep(Duration::from_millis(500)).await;
        }
    }
    if derived.oversize != 1.0 {
        return Err(format!(
            "the size cap should have dropped exactly one record: {derived:?}"
        ));
    }

    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let mut drops = DeadLetterDrops::too_large();
    metrics::with_local_recorder(&recorder, || drops.record(&derived));
    let first = too_large_since(&snapshotter);

    // A second scrape of the same total must add nothing.
    let body = dfe_transform_vector::metrics::scrape::fetch_exposition(exporter_addr)
        .await
        .map_err(|e| format!("second scrape: {e}"))?;
    let again = merger.merge(&body);
    metrics::with_local_recorder(&recorder, || drops.record(&again));
    let second = too_large_since(&snapshotter);

    if (first, second) != (1, 0) {
        return Err(format!(
            "too_large must rise by exactly one and not again on a rescrape: \
             first scrape {first}, second {second}"
        ));
    }
    Ok(())
}

/// Returns `Ok(())` if the pipeline delivered the transformed message.
/// Returns `Err(msg)` with context on any assertion failure — this is so
/// the caller can run cleanup before panicking.
async fn run_pipeline_assertions(
    kf_base: &KafkaConfig,
    source_topic: &str,
    sink_topic: &str,
    group_id: &str,
    suffix: &str,
    config_dir: &std::path::Path,
    exporter_addr: &str,
) -> Result<(), String> {
    // Give Vector time to wire up consumer group / join partitions.
    sleep(Duration::from_secs(3)).await;

    // Producer: send a test message to the source topic.
    let mut producer_cfg = kf_base.clone();
    producer_cfg.group = format!("{group_id}-producer");
    producer_cfg.topics = vec![source_topic.to_string()];
    let producer = KafkaTransport::new(&producer_cfg)
        .await
        .map_err(|e| format!("producer transport: {e}"))?;

    // The first argument is the destination topic, not a record key.
    let test_payload = format!(r#"{{"id":"{suffix}","value":"hello"}}"#);
    let send_result = producer
        .send(source_topic, bytes::Bytes::from(test_payload.into_bytes()))
        .await;
    let send_ok = matches!(
        send_result,
        scalo::transport::SendResult::Ok | scalo::transport::SendResult::Backpressured
    );
    let _ = producer.close().await;
    if !send_ok {
        return Err(format!("producer send failed: {send_result:?}"));
    }

    // Consumer: read from sink topic until we see the enriched message.
    let mut consumer_cfg = kf_base.clone();
    consumer_cfg.group = format!("{group_id}-consumer");
    consumer_cfg.topics = vec![sink_topic.to_string()];
    consumer_cfg.auto_offset_reset = "earliest".into();
    let consumer = KafkaTransport::new(&consumer_cfg)
        .await
        .map_err(|e| format!("consumer transport: {e}"))?;

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut seen_payload: Option<String> = None;
    while Instant::now() < deadline {
        let batch = match consumer.recv(10).await {
            Ok(b) => b,
            Err(_) => {
                sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        for record in &batch.records {
            let body = String::from_utf8_lossy(&record.payload).to_string();
            if body.contains(suffix) {
                seen_payload = Some(body);
                break;
            }
        }
        if seen_payload.is_some() {
            // Commit tokens live on the batch, not per-record (WorkBatch spine).
            let _ = consumer.commit(&batch.commit_tokens).await;
            break;
        }
        sleep(Duration::from_millis(500)).await;
    }
    let _ = consumer.close().await;

    let payload = seen_payload.ok_or_else(|| {
        "did not receive enriched message on sink topic within 30s \
         — pipeline did not deliver"
            .to_string()
    })?;

    // Verify the transform was actually applied.
    if !payload.contains("\"enriched\":true") && !payload.contains("enriched\":true") {
        return Err(format!(
            "transform did not apply — payload missing `enriched`: {payload}"
        ));
    }
    if !payload.contains("processed_at") {
        return Err(format!(
            "transform did not apply `processed_at` field: {payload}"
        ));
    }
    if !payload.contains(suffix) {
        return Err(format!(
            "payload id does not match — received wrong message: {payload}"
        ));
    }

    // Vector publishes internal metrics on its own flush interval, so poll
    // until the exposition carries the delivered events.
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut derived = dfe_transform_vector::metrics::scrape::Derived::default();
    let mut merger = dfe_transform_vector::metrics::scrape::ExpositionMerger::default();
    let mut last_err = String::new();
    while Instant::now() < deadline {
        match dfe_transform_vector::metrics::scrape::fetch_exposition(exporter_addr).await {
            Ok(body) => {
                derived = merger.merge(&body);
                if derived.received >= 1.0 && derived.sent >= 1.0 {
                    break;
                }
                last_err = format!("exposition carried {derived:?}");
            }
            Err(e) => last_err = e,
        }
        sleep(Duration::from_millis(500)).await;
    }
    if derived.received < 1.0 || derived.sent < 1.0 {
        return Err(format!(
            "Vector metrics did not merge into throughput counters ({derived:?}): {last_err}"
        ));
    }

    // In SASL mode, verify the security protocol was injected into source YAML,
    // and that Vector authenticated from the secret backend alone.
    if let Some(ref username) = kf_base.sasl_username {
        let source_yaml = fs::read_to_string(config_dir.join("00_source.yaml"))
            .map_err(|e| format!("read source YAML: {e}"))?;
        if !source_yaml.contains("security.protocol:") {
            return Err("SASL mode but generated source YAML missing security.protocol".into());
        }
        if source_yaml.contains(username.as_str()) {
            return Err("SASL mode but the username is written into the source YAML".into());
        }
    }

    Ok(())
}
