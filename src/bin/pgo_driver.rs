// Project:   dfe-transform-vector
// File:      src/bin/pgo_driver.rs
// Purpose:   PGO workload driver -- drives the direct-transport bridge and stands in for Vector
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! PGO workload driver for `dfe-transform-vector`.
//!
//! The supervisor's own hot path is the DIRECT transport: every record crosses
//! this process twice -- in on `source.listen` and out to Vector, then back
//! from Vector and on to the next stage (`src/bridge.rs`). On the bus Vector
//! talks to Kafka itself and the per-record CPU is inside the upstream binary
//! the image downloads, which no PGO or BOLT build of ours can reach. So this
//! driver closes the direct loop around a running supervisor and keeps it busy:
//!
//! ```text
//!   driver --Push--> supervisor --PushEvents--> driver (Vector stand-in)
//!                                                   |
//!   driver (next stage) <--Push-- supervisor <--PushEvents--
//! ```
//!
//! It also serves a Vector-shaped Prometheus exposition on the address the
//! supervisor scrapes, so the metrics merge (`src/metrics/scrape.rs`) runs for
//! real rather than failing every tick. Everything is loopback -- no broker, no
//! Vector binary, no network.
//!
//! `scripts/pgo-workload.sh` owns the supervisor process, its config and the
//! Vector stand-in; this binary owns the traffic.
//!
//! Built only with `--features pgo-driver`; the shipped binary is unaffected.
//!
//! Configuration via environment variables:
//! - `PGO_DRIVER_DURATION_SECS` (default 300) -- how long to drive load
//! - `PGO_DRIVER_PUSH_ENDPOINT` (default `http://127.0.0.1:6000`) -- source.listen
//! - `PGO_DRIVER_TO_VECTOR` (default `127.0.0.1:6100`) -- bridge.to_vector
//! - `PGO_DRIVER_FROM_VECTOR` (default `127.0.0.1:6101`) -- bridge.from_vector
//! - `PGO_DRIVER_SINK_LISTEN` (default `127.0.0.1:6200`) -- the next stage
//! - `PGO_DRIVER_VECTOR_METRICS` (default `127.0.0.1:9598`) -- the exposition
//! - `PGO_DRIVER_RPS` (default 5000) -- records per second
//! - `PGO_DRIVER_BATCH` (default 100) -- records per push
//! - `PGO_DRIVER_DESTINATION` (default `pgo_land`) -- routing key on the push
//!
//! Exit codes:
//! - 0: the loop carried records for the full duration
//! - 1: a listener or client could not be built, or the loop did not carry

#![allow(clippy::expect_used)]
// Load-generator maths on monotonically-increasing counters: precision loss and
// truncation are irrelevant here and unreachable on the targets we build.
#![allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]

use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use scalo::transport::grpc::{GrpcConfig, GrpcTransport};
use scalo::transport::vector_compat::VectorCompatClient;
use scalo::transport::{
    PayloadFormat, Record, RecordMeta, SendResult, TransportReceiver, TransportSender,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio::time::{MissedTickBehavior, interval};

/// How long the loop keeps draining after the pushers stop, so records still in
/// flight are counted as delivered rather than as loss.
const DRAIN: Duration = Duration::from_secs(5);

/// Longest a receive loop waits before it re-checks its deadline.
const RECV_POLL: Duration = Duration::from_millis(200);

/// Records pulled per receive.
const RECV_MAX: usize = 500;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let cfg = Config::from_env();
    println!("pgo-driver starting: {cfg:#?}");

    let stats = Arc::new(Stats::new());
    let load_until = Instant::now() + Duration::from_secs(cfg.duration_secs);
    let drain_until = load_until + DRAIN;

    // Vector's `vector` source, as far as the supervisor can tell.
    let to_vector =
        match GrpcTransport::new(&GrpcConfig::server(&cfg.to_vector).with_vector_compat()).await {
            Ok(t) => Arc::new(t),
            Err(e) => fail(&format!(
                "cannot bind the Vector stand-in on {}: {e}",
                cfg.to_vector
            )),
        };

    // Vector's `vector` sink, dialling the supervisor back.
    let from_vector = match VectorCompatClient::connect_lazy(&format!("http://{}", cfg.from_vector))
    {
        Ok(c) => Arc::new(c),
        Err(e) => fail(&format!("cannot dial {} back: {e}", cfg.from_vector)),
    };

    // The next stage -- a loader, on a real deployment.
    let next_stage = match GrpcTransport::new(&GrpcConfig::server(&cfg.sink_listen)).await {
        Ok(t) => Arc::new(t),
        Err(e) => fail(&format!(
            "cannot bind the next stage on {}: {e}",
            cfg.sink_listen
        )),
    };

    // Whoever is upstream -- a receiver or a fetcher, on a real deployment.
    let upstream = match GrpcTransport::new(&GrpcConfig::client(&cfg.push_endpoint)).await {
        Ok(t) => Arc::new(t),
        Err(e) => fail(&format!(
            "cannot dial the supervisor on {}: {e}",
            cfg.push_endpoint
        )),
    };

    let exposition = match TcpListener::bind(&cfg.vector_metrics).await {
        Ok(l) => l,
        Err(e) => fail(&format!(
            "cannot bind the metrics exposition on {}: {e}",
            cfg.vector_metrics
        )),
    };

    let mut tasks = JoinSet::new();
    tasks.spawn(vector_leg(
        to_vector,
        from_vector,
        stats.clone(),
        drain_until,
    ));
    tasks.spawn(next_stage_leg(next_stage, stats.clone(), drain_until));
    tasks.spawn(serve_exposition(exposition, stats.clone(), drain_until));

    // Two pushers, because a deployment has more than one upstream sender and
    // one stream would leave the server's per-connection paths cold.
    let per_pusher = (cfg.rps / 2).max(1);
    for _ in 0..2 {
        tasks.spawn(push_loop(
            upstream.clone(),
            cfg.clone(),
            stats.clone(),
            load_until,
            per_pusher,
        ));
    }

    let reporter_stats = stats.clone();
    tasks.spawn(async move {
        let mut tick = interval(Duration::from_secs(15));
        tick.tick().await;
        while Instant::now() < drain_until {
            tick.tick().await;
            reporter_stats.report();
        }
    });

    while let Some(result) = tasks.join_next().await {
        if let Err(e) = result {
            eprintln!("pgo-driver: task error: {e}");
        }
    }

    stats.report();

    let pushed = stats.pushed.load(Ordering::Relaxed);
    let delivered = stats.delivered.load(Ordering::Relaxed);
    if pushed == 0 {
        eprintln!(
            "pgo-driver: the supervisor accepted no records -- profile would be startup only"
        );
        std::process::exit(1);
    }
    // Nothing is dropped by design, so a loop that lost half its traffic is a
    // broken workload, not a slow one -- and a profile off it is worse than none.
    if delivered * 2 < pushed {
        eprintln!(
            "pgo-driver: only {delivered} of {pushed} records completed the loop -- the bridge did not carry"
        );
        std::process::exit(1);
    }
    println!("pgo-driver: complete");
}

/// Report why the driver cannot start and stop.
fn fail(what: &str) -> ! {
    eprintln!("pgo-driver: {what}");
    std::process::exit(1);
}

// ===========================================================================
// Config
// ===========================================================================

#[derive(Clone, Debug)]
struct Config {
    duration_secs: u64,
    push_endpoint: String,
    to_vector: String,
    from_vector: String,
    sink_listen: String,
    vector_metrics: String,
    rps: u32,
    batch: usize,
    destination: Arc<str>,
}

impl Config {
    fn from_env() -> Self {
        Self {
            duration_secs: env_u64("PGO_DRIVER_DURATION_SECS", 300),
            push_endpoint: env_str("PGO_DRIVER_PUSH_ENDPOINT", "http://127.0.0.1:6000"),
            to_vector: env_str("PGO_DRIVER_TO_VECTOR", "127.0.0.1:6100"),
            from_vector: env_str("PGO_DRIVER_FROM_VECTOR", "127.0.0.1:6101"),
            sink_listen: env_str("PGO_DRIVER_SINK_LISTEN", "127.0.0.1:6200"),
            vector_metrics: env_str("PGO_DRIVER_VECTOR_METRICS", "127.0.0.1:9598"),
            rps: env_u32("PGO_DRIVER_RPS", 5000),
            batch: env_u32("PGO_DRIVER_BATCH", 100).max(1) as usize,
            destination: Arc::from(env_str("PGO_DRIVER_DESTINATION", "pgo_land").as_str()),
        }
    }
}

fn env_str(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u32(key: &str, default: u32) -> u32 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ===========================================================================
// Stats
// ===========================================================================

struct Stats {
    pushed: AtomicU64,
    through_vector: AtomicU64,
    delivered: AtomicU64,
    backpressured: AtomicU64,
    scrapes: AtomicU64,
    start: Instant,
}

impl Stats {
    fn new() -> Self {
        Self {
            pushed: AtomicU64::new(0),
            through_vector: AtomicU64::new(0),
            delivered: AtomicU64::new(0),
            backpressured: AtomicU64::new(0),
            scrapes: AtomicU64::new(0),
            start: Instant::now(),
        }
    }

    fn report(&self) {
        let elapsed = self.start.elapsed().as_secs_f64();
        let pushed = self.pushed.load(Ordering::Relaxed);
        println!(
            "pgo-driver [{elapsed:>6.1}s] pushed={pushed:>9} through_vector={:>9} delivered={:>9} backpressured={} scrapes={} rate={:.0}/s",
            self.through_vector.load(Ordering::Relaxed),
            self.delivered.load(Ordering::Relaxed),
            self.backpressured.load(Ordering::Relaxed),
            self.scrapes.load(Ordering::Relaxed),
            (pushed as f64) / elapsed.max(1.0)
        );
    }
}

// ===========================================================================
// The loop around the supervisor
// ===========================================================================

/// Push records at the supervisor's own listener, as an upstream stage does.
async fn push_loop(
    upstream: Arc<GrpcTransport>,
    cfg: Config,
    stats: Arc<Stats>,
    until: Instant,
    rps: u32,
) {
    let corpus = corpus();
    let mut tick = pacer(rps, cfg.batch);
    let mut idx: usize = 0;

    while Instant::now() < until {
        tick.tick().await;

        let mut records = Vec::with_capacity(cfg.batch);
        for _ in 0..cfg.batch {
            records.push(Record {
                payload: corpus[idx % corpus.len()].clone(),
                key: Some(Arc::clone(&cfg.destination)),
                headers: Vec::new(),
                metadata: RecordMeta {
                    timestamp_ms: None,
                    format: PayloadFormat::Json,
                },
            });
            idx = idx.wrapping_add(1);
        }

        match upstream.send_batch(&records).await {
            SendResult::Ok => {
                stats
                    .pushed
                    .fetch_add(records.len() as u64, Ordering::Relaxed);
            }
            SendResult::Backpressured => {
                stats.backpressured.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(RECV_POLL).await;
            }
            other => {
                eprintln!("pgo-driver: the supervisor refused a batch: {other:?}");
                tokio::time::sleep(RECV_POLL).await;
            }
        }
    }
}

/// Stand in for Vector: take what the supervisor hands over and give it back.
///
/// Vector's own transforms are not modelled -- they run in a separate binary
/// and no build of ours compiles them. What matters here is that both bridge
/// legs stay busy.
async fn vector_leg(
    to_vector: Arc<GrpcTransport>,
    from_vector: Arc<VectorCompatClient>,
    stats: Arc<Stats>,
    until: Instant,
) {
    while Instant::now() < until {
        let Some(records) = poll(&to_vector).await else {
            continue;
        };
        if records.is_empty() {
            continue;
        }

        let events: Vec<serde_json::Value> = records.iter().map(|r| as_event(&r.payload)).collect();
        for attempt in 0..3 {
            match from_vector.send_events(&events).await {
                Ok(()) => {
                    stats
                        .through_vector
                        .fetch_add(events.len() as u64, Ordering::Relaxed);
                    break;
                }
                Err(e) => {
                    if attempt == 2 {
                        eprintln!("pgo-driver: the supervisor would not take the batch back: {e}");
                    }
                    tokio::time::sleep(RECV_POLL).await;
                }
            }
        }
    }
}

/// Stand in for the next stage, which on a real deployment is the loader.
async fn next_stage_leg(next_stage: Arc<GrpcTransport>, stats: Arc<Stats>, until: Instant) {
    while Instant::now() < until {
        let Some(records) = poll(&next_stage).await else {
            continue;
        };
        if !records.is_empty() {
            stats
                .delivered
                .fetch_add(records.len() as u64, Ordering::Relaxed);
        }
    }
}

/// One bounded receive, so a quiet listener still lets the loop check its
/// deadline. `None` is nothing to do.
async fn poll(transport: &GrpcTransport) -> Option<Vec<Record>> {
    match tokio::time::timeout(RECV_POLL, transport.recv(RECV_MAX)).await {
        Ok(Ok(batch)) => Some(batch.records),
        Ok(Err(_)) | Err(_) => None,
    }
}

/// A payload as the JSON event Vector's protocol carries, matching what the
/// supervisor does with one that is not a JSON object.
fn as_event(payload: &[u8]) -> serde_json::Value {
    match serde_json::from_slice::<serde_json::Value>(payload) {
        Ok(value) if value.is_object() => value,
        _ => serde_json::json!({ "message": String::from_utf8_lossy(payload) }),
    }
}

/// The interval one pusher waits between batches to hold its share of the rate.
fn pacer(rps: u32, batch: usize) -> tokio::time::Interval {
    let per_batch_nanos = 1_000_000_000u64 * batch as u64 / u64::from(rps.max(1));
    let mut tick = interval(Duration::from_nanos(per_batch_nanos.max(1)));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tick
}

// ===========================================================================
// Vector's metrics exporter, as the supervisor scrapes it
// ===========================================================================

/// Serve a Vector-shaped exposition on the address the supervisor scrapes.
///
/// The supervisor reads the body to EOF under `Connection: close`
/// (`src/metrics/scrape.rs`), so each response closes its own connection.
async fn serve_exposition(listener: TcpListener, stats: Arc<Stats>, until: Instant) {
    while Instant::now() < until {
        let Ok(Ok((mut socket, _))) = tokio::time::timeout(RECV_POLL, listener.accept()).await
        else {
            continue;
        };

        let mut request = [0u8; 1024];
        let _ = tokio::time::timeout(RECV_POLL, socket.read(&mut request)).await;

        let served = stats.scrapes.fetch_add(1, Ordering::Relaxed) + 1;
        let body = exposition(
            served,
            stats.pushed.load(Ordering::Relaxed),
            stats.delivered.load(Ordering::Relaxed),
        );
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = socket.write_all(response.as_bytes()).await;
        let _ = socket.shutdown().await;
    }
}

/// How many component groups the exposition carries.
///
/// A Vector pipeline reports per component, per metric, and the merge walks
/// every sample -- so a handful of series would profile the parse loop as if
/// it never ran.
const EXPOSITION_COMPONENTS: u64 = 120;

/// A Vector-shaped exposition: the two counters the merge derives its
/// throughput totals from, plus per-component counters, gauges and histograms.
fn exposition(served: u64, received: u64, sent: u64) -> String {
    let mut text = String::with_capacity(96 * 1024);

    text.push_str("# TYPE vector_component_received_events_total counter\n");
    text.push_str(&format!(
        "vector_component_received_events_total{{component_id=\"dfe_source\",component_kind=\"source\",component_type=\"vector\"}} {received}\n"
    ));
    text.push_str("# TYPE vector_component_sent_events_total counter\n");
    text.push_str(&format!(
        "vector_component_sent_events_total{{component_id=\"dfe_sink\",component_kind=\"sink\",component_type=\"vector\"}} {sent}\n"
    ));
    text.push_str("# TYPE vector_started_total counter\n");
    text.push_str("vector_started_total 1\n");

    for i in 0..EXPOSITION_COMPONENTS {
        let labels = format!(
            "component_id=\"transform_{i}\",component_kind=\"transform\",component_type=\"remap\""
        );
        text.push_str(&format!(
            "# TYPE vector_component_received_event_bytes_total_{i} counter\n"
        ));
        text.push_str(&format!(
            "vector_component_received_event_bytes_total_{i}{{{labels}}} {}\n",
            served * (i + 1) * 977
        ));
        text.push_str(&format!("# TYPE vector_buffer_events_{i} gauge\n"));
        text.push_str(&format!(
            "vector_buffer_events_{i}{{{labels}}} {}\n",
            (i % 17) as f64 * 1.5
        ));
        text.push_str(&format!(
            "# TYPE vector_component_send_duration_seconds_{i} histogram\n"
        ));
        for (n, le) in ["0.001", "0.005", "0.05", "0.5", "5", "+Inf"]
            .iter()
            .enumerate()
        {
            text.push_str(&format!(
                "vector_component_send_duration_seconds_{i}_bucket{{{labels},le=\"{le}\"}} {}\n",
                served * (n as u64 + 1)
            ));
        }
        text.push_str(&format!(
            "vector_component_send_duration_seconds_{i}_sum{{{labels}}} {}\n",
            served as f64 * 0.42
        ));
        text.push_str(&format!(
            "vector_component_send_duration_seconds_{i}_count{{{labels}}} {}\n",
            served * 6
        ));
    }

    text
}

// ===========================================================================
// Record corpus -- the mix a transform stage actually carries
// ===========================================================================

/// The payloads one pusher cycles through.
///
/// Repetition sets the mix: flat JSON dominates a DFE pipeline, nested and
/// embedded-JSON events are common, a large batched record and a line that is
/// not JSON at all are the tails -- and the last one is the only path that
/// reaches the supervisor's non-object fallback.
fn corpus() -> Vec<Bytes> {
    let mut payloads = Vec::with_capacity(20);
    for _ in 0..9 {
        payloads.push(Bytes::from_static(FLAT));
    }
    for _ in 0..4 {
        payloads.push(Bytes::from_static(NESTED));
    }
    for _ in 0..4 {
        payloads.push(Bytes::from_static(EMBEDDED));
    }
    for _ in 0..2 {
        payloads.push(Bytes::from(batched()));
    }
    payloads.push(Bytes::from_static(NOT_JSON));
    payloads
}

/// A flat event -- what a syslog or JSON-lines source lands.
const FLAT: &[u8] = br#"{"_timestamp":"2026-09-16T01:02:03Z","org_id":"acme","_source":"auth","host":"web-07","level":"info","user":"alice","action":"login","status":"success","duration_ms":42,"src_ip":"198.51.100.7"}"#;

/// A nested event -- objects and arrays, as an API audit log carries.
const NESTED: &[u8] = br#"{"_timestamp":"2026-09-16T01:02:04Z","org_id":"bigcorp","_source":"api","event":{"category":"authentication","outcome":"failure","reason":"mfa_timeout"},"actor":{"id":"u-7781","type":"service_account","groups":["platform","oncall"]},"network":{"src_ip":"203.0.113.24","asn":13335,"country":"AU","tls":{"version":"1.3","cipher":"TLS_AES_128_GCM_SHA256"}},"labels":{"env":"prod","team":"data"}}"#;

/// An event whose `message` is itself JSON -- the shape a transform unpacks.
const EMBEDDED: &[u8] = br#"{"_timestamp":"2026-09-16T01:02:05Z","org_id":"contoso","_source":"orders","level":"warn","message":"{\"event\":\"order_placed\",\"order_id\":\"o-7821\",\"items\":[{\"sku\":\"sku-1\",\"qty\":2},{\"sku\":\"sku-9\",\"qty\":1}],\"total_cents\":12750,\"customer\":{\"id\":\"u-7\",\"tier\":\"gold\"}}"}"#;

/// A line that is not JSON -- the supervisor carries it as `{"message": ...}`
/// rather than dropping it.
const NOT_JSON: &[u8] =
    b"<134>1 2026-09-16T01:02:06Z web-07 sshd 4021 - - Accepted publickey for derek from 198.51.100.7 port 54122 ssh2";

/// A batched event -- one payload carrying fifty sub-records, so the serialise
/// path sees a record that is kilobytes rather than hundreds of bytes.
fn batched() -> Vec<u8> {
    let mut buf =
        br#"{"_timestamp":"2026-09-16T01:02:07Z","org_id":"globex","_source":"http","batch_id":"b-001","events":["#
            .to_vec();
    for i in 0..50 {
        if i > 0 {
            buf.push(b',');
        }
        buf.extend_from_slice(
            format!(
                r#"{{"id":{i},"type":"http.request","path":"/api/v1/users/{i}","method":"GET","status":200,"duration_ms":{ms},"client_ip":"10.0.{a}.{b}"}}"#,
                ms = 5 + (i % 30),
                a = i % 255,
                b = (i * 7) % 255,
            )
            .as_bytes(),
        );
    }
    buf.extend_from_slice(b"]}");
    buf
}
