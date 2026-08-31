// Project:   dfe-transform-vector
// File:      tests/e2e/filebeat_kafka.rs
// Purpose:   WS21 parity: the filebeat corpus through the app, Vector and a broker
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The transform hop of the WS21 chain, with Vector doing the transforming.
//!
//! ```text
//! filebeat corpus -> filebeat_land -> dfe-transform-vector -> filebeat_load
//! ```
//!
//! This is the parity half of the acceptance case: the same corpus, the same
//! bundled filebeat VRL, the same golden events, and the same assertions as
//! `dfe-transform-vrl/tests/e2e/filebeat_kafka.rs`. Only the transform app
//! differs, which is what makes WS21 a test of the transform LAYER rather
//! than of one app.
//!
//! What it exercises that a config test cannot:
//!
//! - The instance is bound to its source by NAME. `dfe_source: filebeat` is
//!   the only topic setting in the config; `filebeat_land`, `filebeat_load`
//!   and the consumer group are all derived from it.
//! - The VRL arrives as an authored transform file, wrapped in a `remap`,
//!   with its lookup table declared alongside it -- so a top-level
//!   `enrichment_tables:` reaching Vector through the assembler is proved,
//!   not assumed.
//! - The app runs `vector validate` before starting, so a pipeline Vector's
//!   VRL cannot compile fails here rather than silently passing nothing on.
//!
//! The corpus, the pipeline and the divergence list belong to
//! dfe-transform-vrl. Without that checkout there is nothing to run against
//! and the test says so.
//!
//! NOT covered here, because it needs a cluster: the receiver hop that picks
//! the source, and the loader hop that lands rows in `ClickHouse`.

use std::time::{Duration, Instant};

use scalo::transport::kafka::{KafkaAdmin, KafkaConfig, KafkaTransport};
use scalo::transport::{TransportBase, TransportReceiver, TransportSender};

use crate::common::{self, KafkaFixture};
use crate::filebeat_corpus as fb;

/// The source this instance is bound to, and the topics that follow from it.
const SOURCE: &str = "filebeat";
const LAND_TOPIC: &str = "filebeat_land";
const LOAD_TOPIC: &str = "filebeat_load";

/// A line no filebeat module claims, carried through to prove the
/// shape-unmatched catch-all still passes an event on rather than dropping it.
const UNMATCHED_PROBE: &str = "ws21 probe: a line no filebeat module claims";

/// How long the app gets to assemble the config, run `vector validate` over
/// the 5.5K-line pipeline, and start Vector.
const READY_TIMEOUT: Duration = Duration::from_secs(240);

/// How long the corpus gets to arrive on the sink topic once the app is ready.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(180);

/// One golden event, and the corpus line it grades.
struct Golden {
    log: &'static str,
    idx: usize,
    event: serde_json::Value,
}

/// Every corpus line as Vector will read it off `filebeat_land`, paired with
/// the goldens they must produce.
fn corpus_workload() -> (Vec<serde_json::Value>, Vec<Golden>) {
    let mut inputs = Vec::new();
    let mut goldens = Vec::new();

    for log in fb::all_logs() {
        let lines = fb::corpus_lines(log);
        let expected = fb::corpus_expected(&format!("{log}-expected.json"));
        assert_eq!(
            lines.len(),
            expected.len(),
            "{log}: {} input lines but {} golden events",
            lines.len(),
            expected.len()
        );
        let conf = fb::corpus_conf(log);
        for (idx, (line, event)) in lines.iter().zip(expected).enumerate() {
            inputs.push(fb::input_event(line, conf.as_ref()));
            goldens.push(Golden { log, idx, event });
        }
    }

    inputs.push(fb::input_event(UNMATCHED_PROBE, None));
    (inputs, goldens)
}

fn client_config(base: &KafkaConfig, topics: &[&str], group: &str) -> KafkaConfig {
    KafkaConfig {
        group: group.to_string(),
        topics: topics.iter().map(|t| (*t).to_string()).collect(),
        auto_offset_reset: "earliest".to_string(),
        ..base.clone()
    }
}

/// A port the OS has just confirmed free.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    let port = listener
        .local_addr()
        .expect("read the bound address")
        .port();
    drop(listener);
    port
}

/// The bundled filebeat VRL as a Vector `remap` transform, with the lookup
/// table it needs declared in the same file.
///
/// The top-level `enrichment_tables` key is the point: the assembler writes
/// this file into Vector's config dir verbatim, so Vector merges the
/// declaration and the VRL's `get_enrichment_table_record` calls resolve.
fn write_transform(dir: &std::path::Path) {
    let vrl = std::fs::read_to_string(fb::pipeline_vrl().expect("the bundled pipeline"))
        .expect("read the bundled filebeat.vrl");
    let timezones = fb::timezones_csv().expect("the bundled timezones table");

    let doc = serde_json::json!({
        "enrichment_tables": {
            "timezones": {
                "type": "file",
                "file": {
                    "path": timezones.to_string_lossy(),
                    "encoding": { "type": "csv" },
                },
                "schema": { "abbreviation": "string" },
            },
        },
        "transforms": {
            "filebeat": {
                "type": "remap",
                "inputs": ["dfe_source"],
                "source": vrl,
            },
        },
    });

    std::fs::create_dir_all(dir).expect("create the transforms dir");
    std::fs::write(
        dir.join("01_filebeat.yaml"),
        serde_yaml_ng::to_string(&doc).expect("transform serialises"),
    )
    .expect("write the transform file");
}

/// The config file the app runs from.
///
/// `dfe_source` is the ONLY topic setting: leaving `source.topics`,
/// `sink.topic` and `source.group_id` at their defaults is what lets the app
/// derive them, which is the binding under test.
fn write_config(
    dir: &std::path::Path,
    brokers: &[String],
    transforms_dir: &std::path::Path,
    data_dir: &std::path::Path,
    assembled_dir: &std::path::Path,
    vector_binary: &std::path::Path,
    health_port: u16,
) -> std::path::PathBuf {
    // JSON is valid YAML 1.2, so serialising sidesteps quoting the paths.
    let config = serde_json::json!({
        "dfe_source": SOURCE,
        "pipeline": { "name": SOURCE },
        "source": {
            "brokers": brokers,
            "decoding": { "codec": "json" },
            "auto_offset_reset": "smallest",
        },
        "sink": {
            "brokers": brokers,
            "encoding": "json",
            "compression": "none",
        },
        "transforms": { "dir": transforms_dir.to_string_lossy() },
        "vector": {
            "binary": vector_binary.to_string_lossy(),
            "data_dir": data_dir.to_string_lossy(),
            // The default is the container's /var/run path, which no test host
            // can write to.
            "config_dir": assembled_dir.to_string_lossy(),
            "api_address": "127.0.0.1:0",
            "log_level": "warn",
            // The binary under test is whatever the fetch script cached or the
            // host provides, which need not be the pinned one.
            "version_check": "disabled",
        },
        "health": { "address": format!("127.0.0.1:{health_port}") },
        "logging": { "level": "info", "format": "text" },
        "scaling": { "enabled": false },
    });
    let path = dir.join("config.yaml");
    std::fs::write(
        &path,
        serde_json::to_string_pretty(&config).expect("config serialises"),
    )
    .expect("write config");
    path
}

/// GET a URL and return its status, or `None` if the request did not answer
/// within a couple of seconds.
async fn http_status(host_port: &str, path: &str) -> Option<u16> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let request = async {
        let mut stream = tokio::net::TcpStream::connect(host_port).await.ok()?;
        let head = format!("GET {path} HTTP/1.1\r\nHost: {host_port}\r\nConnection: close\r\n\r\n");
        stream.write_all(head.as_bytes()).await.ok()?;
        let mut response = String::new();
        stream.read_to_string(&mut response).await.ok()?;
        response
            .lines()
            .next()?
            .split_whitespace()
            .nth(1)?
            .parse()
            .ok()
    };
    tokio::time::timeout(Duration::from_secs(3), request)
        .await
        .ok()
        .flatten()
}

/// Stop the app the way Kubernetes does, so it reaps Vector on the way out.
///
/// SIGKILL would orphan the Vector grandchild, which keeps this test's stderr
/// pipe open and shows up as a leaked process.
async fn stop(app: &mut tokio::process::Child) {
    let _ =
        dfe_transform_vector::vector::process::send_signal(app, nix::sys::signal::Signal::SIGTERM);
    if tokio::time::timeout(Duration::from_secs(15), app.wait())
        .await
        .is_err()
    {
        let _ = app.start_kill();
        let _ = app.wait().await;
    }
}

/// Read up to `want` JSON events off `topic`, stopping early once they arrive.
async fn drain(
    base: &KafkaConfig,
    topic: &str,
    group: &str,
    want: usize,
    timeout: Duration,
) -> Vec<serde_json::Value> {
    let consumer = KafkaTransport::new(&client_config(base, &[topic], group))
        .await
        .unwrap_or_else(|e| panic!("consumer on {topic}: {e}"));
    let mut out = Vec::with_capacity(want);
    let deadline = Instant::now() + timeout;
    while out.len() < want && Instant::now() < deadline {
        match consumer.recv(want).await {
            Ok(batch) => {
                for record in &batch.records {
                    match serde_json::from_slice(&record.payload) {
                        Ok(value) => out.push(value),
                        Err(e) => panic!("payload on {topic} is not JSON: {e}"),
                    }
                }
                let _ = consumer.commit(&batch.commit_tokens).await;
            }
            Err(e) => panic!("recv on {topic} failed: {e}"),
        }
    }
    let _ = consumer.close().await;
    out
}

/// Match every golden to an output event, consuming each output once.
///
/// The sink topic has one partition, so the output usually arrives in the
/// order it was produced; that position is tried first and the scan is the
/// fallback. Returns the goldens that nothing produced, each with the
/// closest candidate's problems.
fn match_goldens(goldens: &[Golden], outputs: &[serde_json::Value]) -> Vec<String> {
    let mut used = vec![false; outputs.len()];
    let mut unmatched = Vec::new();

    for (position, golden) in goldens.iter().enumerate() {
        let mut order: Vec<usize> = Vec::with_capacity(outputs.len());
        if position < outputs.len() {
            order.push(position);
        }
        order.extend((0..outputs.len()).filter(|i| *i != position));

        let mut closest: Option<Vec<String>> = None;
        let mut matched = false;
        for candidate in order {
            if used[candidate] {
                continue;
            }
            let problems = fb::problems_against_golden(
                golden.log,
                golden.idx,
                &golden.event,
                &outputs[candidate],
            );
            if problems.is_empty() {
                used[candidate] = true;
                matched = true;
                break;
            }
            if closest.as_ref().is_none_or(|c| problems.len() < c.len()) {
                closest = Some(problems);
            }
        }

        if !matched {
            let detail = closest.map_or_else(
                || "no output event was left to compare against".to_string(),
                |problems| problems.join("; "),
            );
            unmatched.push(format!("{}[{}]: {detail}", golden.log, golden.idx));
        }
    }
    unmatched
}

#[tokio::test]
async fn filebeat_corpus_round_trips_through_vector() {
    if fb::vrl_repo().is_none() {
        eprintln!(
            "SKIP: the corpus and the bundled pipeline live in dfe-transform-vrl. \
             Point DFE_TRANSFORM_VRL_DIR at a checkout to run this."
        );
        return;
    }
    let Some(vector_binary) = common::vector_binary_path() else {
        eprintln!("SKIP: no Vector binary (run scripts/fetch-vector.sh)");
        return;
    };
    let Some(fixture) = KafkaFixture::hermetic("filebeat-corpus-round-trip").await else {
        common::require_service_in_ci("Kafka", "no Docker to start a broker container");
        eprintln!("SKIP: no Docker, so this test cannot own a broker.");
        return;
    };
    let base = fixture.config.clone();

    let (inputs, goldens) = corpus_workload();
    eprintln!(
        "WS21: replaying {} corpus lines from {} logs through {LAND_TOPIC} -> {LOAD_TOPIC}",
        inputs.len(),
        fb::all_logs().len()
    );

    let admin =
        KafkaAdmin::new(&client_config(&base, &[LAND_TOPIC], "ws21-admin")).expect("admin client");
    admin
        .create_topics(&[(LAND_TOPIC, 1, 1), (LOAD_TOPIC, 1, 1)])
        .await
        .expect("create the source-bound topics");

    let producer = KafkaTransport::new(&client_config(&base, &[LAND_TOPIC], "ws21-seed"))
        .await
        .expect("seed producer");
    // `send`'s first argument is the DESTINATION, which for Kafka is the topic
    // name rather than a partition key.
    for (i, event) in inputs.iter().enumerate() {
        let payload = serde_json::to_vec(event).expect("event serialises");
        let sent = producer.send(LAND_TOPIC, bytes::Bytes::from(payload)).await;
        assert!(
            matches!(
                sent,
                scalo::transport::SendResult::Ok | scalo::transport::SendResult::Backpressured
            ),
            "seeding {LAND_TOPIC} failed at event {i}: {sent:?}"
        );
    }
    let _ = producer.close().await;

    // The seed has to be on the topic before the app is asked to read it, or a
    // later empty sink says nothing about the transform.
    let seeded = drain(
        &base,
        LAND_TOPIC,
        "ws21-seed-check",
        inputs.len(),
        Duration::from_secs(60),
    )
    .await;
    assert_eq!(
        seeded.len(),
        inputs.len(),
        "seeding {LAND_TOPIC} put {} of {} events on the topic",
        seeded.len(),
        inputs.len()
    );

    let work = tempfile::TempDir::new().expect("work dir");
    let transforms_dir = work.path().join("transforms");
    write_transform(&transforms_dir);
    let data_dir = work.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("create the vector data dir");
    let health_port = free_port();
    let config_path = write_config(
        work.path(),
        &base.brokers,
        &transforms_dir,
        &data_dir,
        &work.path().join("assembled"),
        vector_binary,
        health_port,
    );

    // kill_on_drop is the backstop for an assertion failure; the happy path
    // stops the app with SIGTERM instead, because SIGKILL leaves the Vector
    // grandchild orphaned and still holding this test's stderr.
    let mut app = tokio::process::Command::new(env!("CARGO_BIN_EXE_dfe-transform-vector"))
        .arg("--config")
        .arg(&config_path)
        .arg("--metrics-addr")
        .arg(format!("127.0.0.1:{}", free_port()))
        .arg("run")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn dfe-transform-vector");

    let health = format!("127.0.0.1:{health_port}");
    let deadline = Instant::now() + READY_TIMEOUT;
    let mut ready = false;
    while Instant::now() < deadline {
        if let Ok(Some(status)) = app.try_wait() {
            panic!("dfe-transform-vector exited before becoming ready: {status}");
        }
        if http_status(&health, "/readyz").await == Some(200) {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        ready,
        "dfe-transform-vector did not report ready on {health} within {READY_TIMEOUT:?}"
    );

    let outputs = drain(
        &base,
        LOAD_TOPIC,
        "ws21-verify",
        inputs.len(),
        DRAIN_TIMEOUT,
    )
    .await;
    stop(&mut app).await;

    // Assertion 1 -- everything that went in came out.
    assert_eq!(
        outputs.len(),
        inputs.len(),
        "seeded {} events on {LAND_TOPIC} but {LOAD_TOPIC} carried {}",
        inputs.len(),
        outputs.len()
    );

    // Assertion 2 -- the shape-unmatched catch-all passes an event through
    // tagged, rather than dropping it.
    let probe_at = outputs
        .iter()
        .position(|e| e.get("message").and_then(serde_json::Value::as_str) == Some(UNMATCHED_PROBE))
        .expect("the unmatched probe must arrive on the sink topic");
    assert!(
        fb::is_unmatched(&outputs[probe_at]),
        "an unclaimed line must carry filebeat_unmatched: {}",
        outputs[probe_at]
    );

    // Assertion 3 -- every corpus line agrees with its elastic golden, after
    // the port's documented divergences.
    let mut corpus_outputs = outputs;
    corpus_outputs.remove(probe_at);
    let unmatched = match_goldens(&goldens, &corpus_outputs);
    assert!(
        unmatched.is_empty(),
        "{} of {} corpus events did not match their golden:\n{}",
        unmatched.len(),
        goldens.len(),
        unmatched.join("\n")
    );

    // Assertion 4 -- all three module branches actually ran.
    let branch_ran = |module: &str, probe: &dyn Fn(&serde_json::Value) -> bool| {
        assert!(
            corpus_outputs.iter().any(probe),
            "no output event came from the {module} branch"
        );
    };
    let product_is = |event: &serde_json::Value, want: &str| {
        event
            .get("observer")
            .and_then(|o| o.get("product"))
            .and_then(serde_json::Value::as_str)
            == Some(want)
    };
    branch_ran("umbrella", &|e| product_is(e, "Umbrella"));
    branch_ran("ios", &|e| product_is(e, "IOS"));
    branch_ran("meraki", &|e| e.get("cisco_meraki").is_some());
}
