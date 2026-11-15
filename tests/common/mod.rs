#![allow(dead_code, clippy::unwrap_used)]
// Project:   dfe-transform-vector
// File:      tests/common/mod.rs
// Purpose:   Shared test infrastructure: live-first Kafka, else scalo's shared broker
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Shared test helpers for integration and e2e tests.
//!
//! ## Kafka test fixture -- live first, then the run's shared broker
//!
//! Priority order (automatic -- no TEST_MODE env needed):
//! 1. **Live cluster** -- if `.env` credentials authenticate successfully.
//!    Faster and more realistic (cluster, not single-node).
//! 2. **Shared broker** -- scalo's `shared_service` broker, one PLAINTEXT
//!    container for the whole test run, joined by every nextest process.
//! 3. **Skip** -- if neither is available (no env creds AND no Docker).
//!
//! scalo names and labels every container and removes it when the run ends.
//! A SCRAM broker is a test's own, removed when its fixture drops.
//!
//! Credentials come from the project `.env` (never hard-coded), loaded from
//! the repo root at the first call.

use std::env;
use std::net::ToSocketAddrs;
use std::time::Duration;

use scalo::SensitiveString;
use scalo::deployment::test_support::shared_service::{
    OwnedContainer, Ready, ServiceSpec, SharedContainer, Suite, container_name, start_alone,
};
use scalo::transport::kafka::test_support::{
    KAFKA_IMAGE, KAFKA_PORT, docker_answers, scram_password, shared_kafka,
};
use scalo::transport::kafka::{KafkaAdmin, KafkaConfig, KafkaProfile};

// Kept in its own file so `tests/smoke.rs` can pull it in with `#[path]`
// without dragging the Kafka helpers into that binary. Left
// as a module rather than re-exported, because a `pub use` that a given test
// binary never touches trips `unused_imports` under `-D warnings`.
pub mod metrics_fixture;

/// Which backend the current test fixture resolved to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FixtureMode {
    /// Live cluster from `.env` / environment variables (preferred).
    Live,
    /// A broker container scalo started (fallback).
    Container,
}

/// Test fixture that provides a working Kafka cluster.
pub struct KafkaFixture {
    pub config: KafkaConfig,
    pub mode: FixtureMode,
    broker: Option<Broker>,
}

/// The broker container behind a fixture.
pub enum Broker {
    /// The run's shared PLAINTEXT broker, removed when the run ends.
    Shared(SharedContainer),
    /// The SASL/SCRAM-SHA-512 broker [`KafkaFixture::scram`] starts, removed
    /// when the fixture drops.
    Owned(OwnedContainer),
}

impl KafkaFixture {
    /// The broker container behind this fixture, `None` for a live cluster.
    pub fn broker(&self) -> Option<&SharedContainer> {
        match &self.broker {
            Some(Broker::Shared(container)) => Some(container),
            Some(Broker::Owned(owned)) => Some(&owned.container),
            None => None,
        }
    }

    /// Acquire a Kafka fixture: the live cluster, else the run's shared broker.
    ///
    /// Returns `None` if neither live nor Docker are available — callers
    /// should skip the test in that case.
    pub async fn acquire(_test: &str) -> Option<Self> {
        load_dotenv();

        // 1. Try live cluster (real authenticated connection probe)
        if let Some(fixture) = try_live_cluster().await {
            eprintln!(
                "kafka fixture: using LIVE cluster at {:?}",
                fixture.config.brokers
            );
            return Some(fixture);
        }

        // 2. Fall back to the run's shared broker
        match shared_broker().await {
            Ok(fixture) => {
                eprintln!(
                    "kafka fixture: using the SHARED BROKER at {:?} \
                     (live cluster unavailable -- check .env or OpenBao creds)",
                    fixture.config.brokers
                );
                Some(fixture)
            }
            Err(e) => {
                eprintln!(
                    "kafka fixture: UNAVAILABLE -- live cluster unreachable \
                     AND no broker container: {e}"
                );
                None
            }
        }
    }

    /// Acquire the run's shared broker container, never the live cluster.
    ///
    /// `acquire` prefers the live cluster, which is wrong for a test that uses
    /// the real DFE topic names: `filebeat_land` on a shared live broker is
    /// somebody else's data. Returns `None` when no container can be started.
    pub async fn hermetic(_test: &str) -> Option<Self> {
        match shared_broker().await {
            Ok(fixture) => {
                eprintln!(
                    "kafka fixture: using the SHARED BROKER at {:?}",
                    fixture.config.brokers
                );
                Some(fixture)
            }
            Err(e) => {
                eprintln!("kafka fixture: UNAVAILABLE -- no broker container: {e}");
                None
            }
        }
    }

    /// Acquire a broker this test OWNS that only accepts SASL/SCRAM-SHA-512,
    /// the way every DFE tier's broker does.
    ///
    /// Every other fixture is PLAINTEXT, so nothing else here exercises the
    /// credential path. Returns `None` when no container can be started.
    pub async fn scram(test: &str) -> Option<Self> {
        match scram_broker(test).await {
            Ok(fixture) => {
                eprintln!(
                    "kafka fixture: using an OWNED SCRAM BROKER at {:?}",
                    fixture.config.brokers
                );
                Some(fixture)
            }
            Err(e) => {
                eprintln!("kafka fixture: UNAVAILABLE -- SCRAM broker failed: {e}");
                None
            }
        }
    }
}

/// Load this repo's own `.env`: `dotenvy::dotenv()` would load the first `.env` in any parent.
pub fn load_dotenv() {
    let _ = dotenvy::from_path(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(".env"));
}

/// Build a `KafkaConfig` from environment variables (live-cluster mode).
///
/// Returns `None` if required broker list is not set.
fn live_config_from_env() -> Option<KafkaConfig> {
    let brokers_str = env::var("KAFKA_BROKERS").ok()?;
    if brokers_str.is_empty() {
        return None;
    }
    Some(KafkaConfig {
        profile: KafkaProfile::DevTest,
        brokers: brokers_str.split(',').map(String::from).collect(),
        group: "dfe-transform-vector-tests".into(),
        security_protocol: env::var("KAFKA_SECURITY_PROTOCOL")
            .unwrap_or_else(|_| "SASL_SSL".into()),
        sasl_mechanism: env::var("KAFKA_SASL_MECHANISM").ok(),
        sasl_username: env::var("KAFKA_SASL_USER").ok(),
        sasl_password: env::var("KAFKA_SASL_PASSWORD")
            .ok()
            .map(SensitiveString::new),
        ssl_skip_verify: env::var("KAFKA_SSL_SKIP_VERIFY")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(true),
        ..Default::default()
    })
}

/// Probe the live cluster: TCP + authenticated metadata fetch.
///
/// Returns `Some(fixture)` only if the full handshake succeeds -- stale
/// SASL creds count as a failure, falling back to the shared broker.
async fn try_live_cluster() -> Option<KafkaFixture> {
    let cfg = live_config_from_env()?;
    if !tcp_reachable(&cfg.brokers).await {
        return None;
    }
    // Authenticated probe -- if SASL creds are stale, this fails and we
    // fall through to the shared broker.
    if !authenticated_probe(&cfg).await {
        return None;
    }
    Some(KafkaFixture {
        config: cfg,
        mode: FixtureMode::Live,
        broker: None,
    })
}

/// TCP-level reachability check on the first broker.
async fn tcp_reachable(brokers: &[String]) -> bool {
    let Some(first) = brokers.first() else {
        return false;
    };
    let addr_owned = first.clone();
    tokio::task::spawn_blocking(move || {
        addr_owned
            .to_socket_addrs()
            .ok()
            .and_then(|mut addrs| addrs.next())
            .map(|a| std::net::TcpStream::connect_timeout(&a, Duration::from_secs(3)).is_ok())
            .unwrap_or(false)
    })
    .await
    .unwrap_or(false)
}

/// Attempt an authenticated metadata fetch to verify credentials work.
///
/// Uses a short timeout; if the broker accepts the creds, we're live.
async fn authenticated_probe(cfg: &KafkaConfig) -> bool {
    let cfg = cfg.clone();
    tokio::task::spawn_blocking(move || {
        let admin = match KafkaAdmin::new(&cfg) {
            Ok(a) => a,
            Err(_) => return false,
        };
        admin.list_topics().is_ok()
    })
    .await
    .unwrap_or(false)
}

/// Panic if a backing service is missing while running in CI.
///
/// Skipping is right on a developer machine, where the daemon may simply be
/// down. In CI it makes the test pass VACUOUSLY: the suite reports green while
/// exercising none of the integration surface. A gate that disappears along
/// with its environment is not a gate.
pub fn require_service_in_ci(what: &str, detail: &str) {
    assert!(
        std::env::var_os("CI").is_none(),
        "{what} unreachable in CI ({detail}) -- integration tests must RUN here, \
         not skip. Skipping would report green while testing nothing."
    );
}

// =============================================================================
// Broker containers
// =============================================================================
//
// scalo's `shared_service` starts, names and labels every container: one
// PLAINTEXT broker per test run, shared by every nextest process, and a SCRAM
// broker a test holds alone. Names start `dfe-transform-vector-test-integration-`
// (or `HYPERI_TEST_CONTAINER_PREFIX`), the `io.hyperi.test.suite` label is
// `dfe-transform-vector-integration`, and a watcher removes them once the run
// ends.

/// This suite, for container names, labels and the `HYPERI_TEST_*` lookup.
pub const SUITE: Suite = Suite {
    repo: "dfe-transform-vector",
    manifest_dir: env!("CARGO_MANIFEST_DIR"),
    tmp_dir: option_env!("CARGO_TARGET_TMPDIR"),
};

/// A JVM broker takes 5-12 s to become ready, longer on a busy runner, so 60 s is too tight.
const KAFKA_STARTUP_TIMEOUT: Duration = Duration::from_secs(180);

/// Username the SCRAM broker bootstraps at format time. Distinct enough that a
/// test can tell it apart from anything else in a generated config.
const SCRAM_USERNAME: &str = "dfe-test-user";

/// The run's shared PLAINTEXT broker, sufficient for validating pipeline wiring.
async fn shared_broker() -> Result<KafkaFixture, String> {
    if !docker_answers() {
        return Err("no Docker daemon answers".into());
    }
    let container = shared_kafka(SUITE).await;
    let config = KafkaConfig {
        profile: KafkaProfile::DevTest,
        brokers: vec![container.addr(KAFKA_PORT)],
        group: "dfe-transform-vector-tests".into(),
        security_protocol: "PLAINTEXT".into(),
        ..Default::default()
    };
    Ok(KafkaFixture {
        config,
        mode: FixtureMode::Container,
        broker: Some(Broker::Shared(container)),
    })
}

/// One KRaft node that serves SASL_PLAINTEXT with SCRAM-SHA-512 only, its user
/// written in at format time -- the shape of the DFE single tier's broker.
const SCRAM_BROKER_SCRIPT: &str = r#"set -e
CONF=/tmp/server.properties
cat > "$CONF" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=SASL_PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=SASL_PLAINTEXT://${ADVERTISED_ADDRESS}
listener.security.protocol.map=CONTROLLER:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=SASL_PLAINTEXT
sasl.enabled.mechanisms=SCRAM-SHA-512
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512
listener.name.sasl_plaintext.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="${SCRAM_USERNAME}" password="${SCRAM_PASSWORD}";
log.dirs=/tmp/kafka-data
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
EOF
/opt/kafka/bin/kafka-storage.sh format -t "$(/opt/kafka/bin/kafka-storage.sh random-uuid)" -c "$CONF" \
  --add-scram "SCRAM-SHA-512=[name=${SCRAM_USERNAME},password=${SCRAM_PASSWORD}]"
exec /opt/kafka/bin/kafka-server-start.sh "$CONF"
"#;

/// Start an Apache Kafka container this test holds alone, accepting only
/// SASL/SCRAM-SHA-512, and a fixture config that authenticates to it.
///
/// `test` names the calling test and goes into the container name, so
/// concurrent tests do not collide on it.
async fn scram_broker(test: &str) -> Result<KafkaFixture, String> {
    if !docker_answers() {
        return Err("no Docker daemon answers".into());
    }
    // The broker tells clients to reconnect to the address it advertises, so
    // the host port is chosen before it starts rather than read back after.
    let address = free_port().await;
    let host_port: u16 = address
        .rsplit(':')
        .next()
        .and_then(|p| p.parse().ok())
        .ok_or_else(|| format!("no port in {address}"))?;

    let name = container_name(&SUITE, Some(test), "kafka-scram");
    let spec = ServiceSpec {
        pinned_ports: vec![(KAFKA_PORT, host_port)],
        env: vec![
            ("ADVERTISED_ADDRESS".into(), address.clone()),
            ("SCRAM_USERNAME".into(), SCRAM_USERNAME.into()),
            ("SCRAM_PASSWORD".into(), scram_password().into()),
        ],
        entrypoint: Some("/bin/sh".into()),
        args: vec!["-c".into(), SCRAM_BROKER_SCRIPT.into()],
        ready: Ready::Log("Kafka Server started"),
        ..ServiceSpec::new("kafka-scram", KAFKA_IMAGE, KAFKA_STARTUP_TIMEOUT)
    };
    let owned = tokio::task::spawn_blocking(move || start_alone(&SUITE, &spec, &name))
        .await
        .map_err(|e| format!("start the SCRAM broker: {e}"))?;

    let config = KafkaConfig {
        profile: KafkaProfile::DevTest,
        brokers: vec![address],
        group: "dfe-transform-vector-tests".into(),
        security_protocol: "SASL_PLAINTEXT".into(),
        sasl_mechanism: Some("SCRAM-SHA-512".into()),
        sasl_username: Some(SCRAM_USERNAME.into()),
        sasl_password: Some(SensitiveString::new(scram_password())),
        ..Default::default()
    };

    Ok(KafkaFixture {
        config,
        mode: FixtureMode::Container,
        broker: Some(Broker::Owned(owned)),
    })
}

// =============================================================================
// Legacy sync helpers -- kept for existing integration tests that only
// build configs (don't actually connect). New tests should use
// `KafkaFixture::acquire()` instead.
// =============================================================================

/// Build a scalo `KafkaConfig` from env (sync — for tests that only
/// construct YAML and don't connect). Does NOT verify authentication.
pub fn kafka_test_config() -> KafkaConfig {
    load_dotenv();
    live_config_from_env().unwrap_or_else(|| KafkaConfig {
        profile: KafkaProfile::DevTest,
        brokers: vec!["localhost:9092".into()],
        group: "dfe-transform-vector-tests".into(),
        security_protocol: "PLAINTEXT".into(),
        ..Default::default()
    })
}

/// Sync TCP reachability probe — used by legacy tests.
pub fn is_kafka_reachable(config: &KafkaConfig) -> bool {
    let first = config
        .brokers
        .first()
        .map(String::as_str)
        .unwrap_or("localhost:9092");
    first
        .to_socket_addrs()
        .ok()
        .and_then(|mut addrs| addrs.next())
        .map(|a| std::net::TcpStream::connect_timeout(&a, Duration::from_secs(3)).is_ok())
        .unwrap_or(false)
}

/// Create a `KafkaAdmin` from config — thin convenience for legacy tests.
pub fn kafka_admin(config: &KafkaConfig) -> KafkaAdmin {
    KafkaAdmin::new(config).expect("failed to create KafkaAdmin")
}

// =============================================================================
// Vector binary discovery + skip macros
// =============================================================================

/// Resolve the Vector binary path via fetch script or system PATH.
///
/// Tries `scripts/fetch-vector.sh` first (downloads and caches in `.tmp/`),
/// falls back to `vector` on system PATH. Returns `None` if unavailable.
/// Uses `OnceLock` so the fetch runs at most once per test binary.
pub fn vector_binary_path() -> Option<&'static std::path::PathBuf> {
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::OnceLock;

    static BIN: OnceLock<Option<PathBuf>> = OnceLock::new();
    BIN.get_or_init(|| {
        let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/fetch-vector.sh");
        if script.exists()
            && let Ok(output) = Command::new("bash").arg(&script).output()
            && output.status.success()
        {
            let stdout = String::from_utf8_lossy(&output.stdout);
            if let Some(last_line) = stdout.trim().lines().last() {
                let p = PathBuf::from(last_line);
                if p.exists() {
                    return Some(p);
                }
            }
        }
        Command::new("vector")
            .arg("--version")
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|_| PathBuf::from("vector"))
    })
    .as_ref()
}

/// Skip test if Vector binary is not available.
#[allow(unused_macros)]
macro_rules! skip_if_no_vector {
    () => {
        if $crate::common::vector_binary_path().is_none() {
            eprintln!("Skipping: Vector binary not available (run scripts/fetch-vector.sh)");
            return;
        }
    };
}

#[allow(unused_imports)]
pub(crate) use skip_if_no_vector;

// =============================================================================
// HTTP + port helpers
// =============================================================================

/// Minimal HTTP GET — avoids pulling in reqwest as a dep.
pub async fn reqwest_lite(url: &str) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let url = url.strip_prefix("http://").unwrap();
    let (host_port, path) = url.split_once('/').unwrap_or((url, ""));
    let path = format!("/{path}");

    let mut stream = TcpStream::connect(host_port).await.unwrap();
    let request = format!("GET {path} HTTP/1.1\r\nHost: {host_port}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();

    let status_line = response.lines().next().unwrap_or("");
    let status_code: u16 = status_line
        .split_whitespace()
        .nth(1)
        .unwrap_or("0")
        .parse()
        .unwrap_or(0);

    let body = response.split("\r\n\r\n").nth(1).unwrap_or("").to_string();

    (status_code, body)
}

/// Stand in for Vector's `prometheus_exporter`: answer each connection with the
/// exposition `body` builds from how many connections came before it.
pub async fn serve_expositions(
    body: impl Fn(u64) -> String + Send + Sync + 'static,
) -> (String, tokio::task::JoinHandle<()>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let handle = tokio::spawn(async move {
        let mut served = 0u64;
        while let Ok((mut sock, _)) = listener.accept().await {
            let body = body(served);
            served += 1;
            let mut req = [0u8; 1024];
            let _ = sock.read(&mut req).await;
            let response = format!("HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n{body}");
            let _ = sock.write_all(response.as_bytes()).await;
            let _ = sock.shutdown().await;
        }
    });
    (addr, handle)
}

/// Get a free port by binding to :0, extracting the address, then dropping.
pub async fn free_port() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr.to_string()
}
