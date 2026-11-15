// Project:   dfe-transform-vector
// File:      tests/e2e/app_kafka.rs
// Purpose:   The app binary over a real broker with an authored transform file
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The shipped binary reading `e2e_syslog_land`, transforming with Vector and
//! writing `e2e_syslog_load`.
//!
//! The instance is bound to its source by NAME: `dfe_source` is the only
//! topic setting in the config, and both topics and the consumer group derive
//! from it. The app also runs `vector validate` before it starts Vector, so a
//! transform file Vector cannot compile fails here.
//!
//! The receiver hop that picks the source and the loader hop that lands rows
//! in `ClickHouse` need a cluster and are not covered.

use std::time::{Duration, Instant};

use scalo::transport::kafka::{KafkaAdmin, KafkaConfig, KafkaTransport};
use scalo::transport::{TransportBase, TransportReceiver, TransportSender};

use crate::common::{self, KafkaFixture};

const SOURCE: &str = "e2e_syslog";
const LAND_TOPIC: &str = "e2e_syslog_land";
const LOAD_TOPIC: &str = "e2e_syslog_load";

/// A synthetic line and the fields the transform must set, or `None` where it
/// must tag the event `unparsed` and pass it on.
struct Case {
    name: &'static str,
    line: &'static str,
    expect: Option<(&'static str, &'static str)>,
}

const CASES: &[Case] = &[
    Case {
        name: "denied",
        line: "denied src=203.0.113.7",
        expect: Some(("203.0.113.7", "denied")),
    },
    Case {
        name: "allowed",
        line: "allowed src=198.51.100.23",
        expect: Some(("198.51.100.23", "allowed")),
    },
    Case {
        name: "reset",
        line: "conn_reset src=192.0.2.1",
        expect: Some(("192.0.2.1", "conn_reset")),
    },
    Case {
        name: "no_source",
        line: "a line with no key value pairs",
        expect: None,
    },
    Case {
        name: "bad_ip",
        line: "denied src=not-an-address",
        expect: None,
    },
];

/// How long the app gets to assemble the config, validate and start Vector.
const READY_TIMEOUT: Duration = Duration::from_secs(120);

/// How long the cases get to arrive on the sink topic once the app is ready.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(120);

const TRANSFORM: &str = r#"transforms:
  parse_line:
    type: remap
    inputs:
      - dfe_source
    source: |
      parsed, err = parse_regex(.message, r'^(?P<action>[a-z_]+) src=(?P<ip>[0-9.]+)$')
      if err == null {
        .source.ip = parsed.ip
        .event.action = parsed.action
      } else {
        .tags = ["unparsed"]
      }
"#;

fn workload() -> Vec<serde_json::Value> {
    CASES
        .iter()
        .map(|c| serde_json::json!({ "message": c.line, "_case": c.name }))
        .collect()
}

fn write_transform(dir: &std::path::Path) {
    std::fs::create_dir_all(dir).expect("create the transforms dir");
    std::fs::write(dir.join("01_parse_line.yaml"), TRANSFORM).expect("write the transform file");
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

fn write_config(
    dir: &std::path::Path,
    brokers: &[String],
    transforms_dir: &std::path::Path,
    data_dir: &std::path::Path,
    assembled_dir: &std::path::Path,
    vector_binary: &std::path::Path,
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
    let mut last_err = None;
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
            // A broker the client is still reconnecting to answers with a
            // transport error, which the deadline is already there to bound.
            // Failing on the first one turns a loaded host into a red suite.
            Err(e) => {
                last_err = Some(e.to_string());
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
    assert!(
        out.len() >= want || last_err.is_none(),
        "recv on {topic} never succeeded within {timeout:?}: {}",
        last_err.unwrap_or_default()
    );
    let _ = consumer.close().await;
    out
}

#[tokio::test]
async fn syslog_lines_round_trip_through_vector() {
    let Some(vector_binary) = common::vector_binary_path() else {
        common::require_service_in_ci("Vector binary", "scripts/fetch-vector.sh found nothing");
        eprintln!("SKIP: no Vector binary (run scripts/fetch-vector.sh)");
        return;
    };
    let Some(fixture) = KafkaFixture::hermetic("syslog-round-trip").await else {
        common::require_service_in_ci("Kafka", "no Docker to start a broker container");
        eprintln!("SKIP: no Docker, so this test cannot own a broker.");
        return;
    };
    let base = fixture.config.clone();

    let inputs = workload();
    eprintln!(
        "replaying {} events through {LAND_TOPIC} -> {LOAD_TOPIC}",
        inputs.len()
    );

    let admin =
        KafkaAdmin::new(&client_config(&base, &[LAND_TOPIC], "admin")).expect("admin client");
    admin
        .create_topics(&[(LAND_TOPIC, 1, 1), (LOAD_TOPIC, 1, 1)])
        .await
        .expect("create the source-bound topics");

    let producer = KafkaTransport::new(&client_config(&base, &[LAND_TOPIC], "seed"))
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
        "seed-check",
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
    let config_path = write_config(
        work.path(),
        &base.brokers,
        &transforms_dir,
        &data_dir,
        &work.path().join("assembled"),
        vector_binary,
    );

    // The port the generated probes target: /metrics, /livez and /readyz are
    // all served here, and it is the only health surface the app has.
    let probe_port = free_port();

    // kill_on_drop is the backstop for an assertion failure; the happy path
    // stops the app with SIGTERM instead, because SIGKILL leaves the Vector
    // grandchild orphaned and still holding this test's stderr.
    let mut app = tokio::process::Command::new(env!("CARGO_BIN_EXE_dfe-transform-vector"))
        .arg("--config")
        .arg(&config_path)
        .arg("--metrics-addr")
        .arg(format!("127.0.0.1:{probe_port}"))
        .arg("run")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn dfe-transform-vector");

    let probes = format!("127.0.0.1:{probe_port}");
    let deadline = Instant::now() + READY_TIMEOUT;
    let mut ready = false;
    while Instant::now() < deadline {
        if let Ok(Some(status)) = app.try_wait() {
            panic!("dfe-transform-vector exited before becoming ready: {status}");
        }
        if http_status(&probes, "/readyz").await == Some(200) {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        ready,
        "dfe-transform-vector did not report ready on {probes} within {READY_TIMEOUT:?}"
    );

    let outputs = drain(&base, LOAD_TOPIC, "verify", inputs.len(), DRAIN_TIMEOUT).await;
    stop(&mut app).await;

    // Assertion 1 -- everything that went in came out.
    assert_eq!(
        outputs.len(),
        inputs.len(),
        "seeded {} events on {LAND_TOPIC} but {LOAD_TOPIC} carried {}",
        inputs.len(),
        outputs.len()
    );

    // Assertion 2 -- each line carries the fields its case names.
    let mut failures = Vec::new();
    for case in CASES {
        let Some(event) = outputs
            .iter()
            .find(|e| e.get("_case").and_then(serde_json::Value::as_str) == Some(case.name))
        else {
            failures.push(format!("{}: no output event carried its name", case.name));
            continue;
        };
        let ip = event
            .pointer("/source/ip")
            .and_then(serde_json::Value::as_str);
        let action = event
            .pointer("/event/action")
            .and_then(serde_json::Value::as_str);
        let unparsed = event
            .pointer("/tags")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|t| t.iter().any(|v| v == "unparsed"));
        match case.expect {
            Some((want_ip, want_action)) => {
                if ip != Some(want_ip) || action != Some(want_action) || unparsed {
                    failures.push(format!("{}: got {event}", case.name));
                }
            }
            None => {
                if !unparsed || ip.is_some() || action.is_some() {
                    failures.push(format!("{}: expected unparsed, got {event}", case.name));
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} cases did not match:\n{}",
        failures.len(),
        CASES.len(),
        failures.join("\n")
    );
}
