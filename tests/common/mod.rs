#![allow(dead_code, clippy::unwrap_used)]
// Project:   dfe-transform-vector
// File:      tests/common/mod.rs
// Purpose:   Shared test infrastructure — live-first Kafka with testcontainers fallback
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Shared test helpers for integration and e2e tests.
//!
//! ## Kafka test fixture — live first, testcontainers fallback
//!
//! Priority order (automatic — no TEST_MODE env needed):
//! 1. **Live cluster** — if `.env` credentials authenticate successfully.
//!    Faster and more realistic (cluster, not single-node).
//! 2. **Testcontainers** — ephemeral single-node Apache Kafka spawned via
//!    Docker. Used only when the live cluster is unreachable or creds are
//!    stale.
//! 3. **Skip** — if neither is available (no env creds AND no Docker).
//!
//! The fixture owns the container (if any) and shuts it down on drop.
//!
//! Credentials come from the project `.env` (never hard-coded). `.env` is
//! loaded via `dotenvy` at the first call.

use std::env;
use std::net::ToSocketAddrs;
use std::time::Duration;

use scalo::SensitiveString;
use scalo::transport::kafka::{KafkaAdmin, KafkaConfig, KafkaProfile};

// Kept in its own file so `tests/smoke.rs` can pull it in with `#[path]`
// without dragging the Kafka/testcontainers helpers into that binary. Left
// as a module rather than re-exported, because a `pub use` that a given test
// binary never touches trips `unused_imports` under `-D warnings`.
pub mod metrics_fixture;

/// Which backend the current test fixture resolved to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FixtureMode {
    /// Live cluster from `.env` / environment variables (preferred).
    Live,
    /// Testcontainers-managed Apache Kafka (fallback).
    Testcontainer,
}

/// Test fixture that provides a working Kafka cluster and handles cleanup.
///
/// Holds a container handle when in `Testcontainer` mode — the container
/// stops on drop, preventing test leaks.
pub struct KafkaFixture {
    pub config: KafkaConfig,
    pub mode: FixtureMode,
    #[allow(dead_code)]
    container: Option<KafkaContainerHandle>,
}

/// Opaque wrapper around the testcontainers Kafka container.
/// Drop = stop container (handled by testcontainers-rs).
enum KafkaContainerHandle {
    /// The PLAINTEXT broker from the testcontainers module.
    Plaintext(testcontainers::ContainerAsync<testcontainers_modules::kafka::apache::Kafka>),
    /// The SASL/SCRAM-SHA-512 broker [`KafkaFixture::scram`] starts.
    Scram(testcontainers::ContainerAsync<testcontainers::GenericImage>),
}

impl KafkaFixture {
    /// Acquire a Kafka fixture using live-first, testcontainers-fallback order.
    ///
    /// `test` names the calling test and goes into the container name, so
    /// concurrent tests do not collide on it.
    ///
    /// Returns `None` if neither live nor Docker are available — callers
    /// should skip the test in that case.
    pub async fn acquire(test: &str) -> Option<Self> {
        load_dotenv();

        // 1. Try live cluster (real authenticated connection probe)
        if let Some(fixture) = try_live_cluster().await {
            eprintln!(
                "kafka fixture: using LIVE cluster at {:?}",
                fixture.config.brokers
            );
            return Some(fixture);
        }

        // 2. Fall back to testcontainers
        match try_testcontainer(test).await {
            Ok(fixture) => {
                eprintln!(
                    "kafka fixture: using TESTCONTAINER at {:?} \
                     (live cluster unavailable — check .env or OpenBao creds)",
                    fixture.config.brokers
                );
                Some(fixture)
            }
            Err(e) => {
                eprintln!(
                    "kafka fixture: UNAVAILABLE — live cluster unreachable \
                     AND testcontainers failed: {e}"
                );
                None
            }
        }
    }

    /// Acquire a Kafka fixture this test OWNS — always a fresh container,
    /// never a broker that was already there.
    ///
    /// `acquire` prefers the live cluster, which is wrong for a test that uses
    /// the real DFE topic names: `filebeat_land` on a shared broker is
    /// somebody else's data. Returns `None` when no container can be started.
    pub async fn hermetic(test: &str) -> Option<Self> {
        match try_testcontainer(test).await {
            Ok(fixture) => {
                eprintln!(
                    "kafka fixture: using an OWNED TESTCONTAINER at {:?}",
                    fixture.config.brokers
                );
                Some(fixture)
            }
            Err(e) => {
                eprintln!("kafka fixture: UNAVAILABLE — testcontainers failed: {e}");
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
        match try_scram_testcontainer(test).await {
            Ok(fixture) => {
                eprintln!(
                    "kafka fixture: using an OWNED SCRAM TESTCONTAINER at {:?}",
                    fixture.config.brokers
                );
                Some(fixture)
            }
            Err(e) => {
                eprintln!("kafka fixture: UNAVAILABLE — SCRAM testcontainer failed: {e}");
                None
            }
        }
    }
}

pub fn load_dotenv() {
    let _ = dotenvy::dotenv();
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
/// Returns `Some(fixture)` only if the full handshake succeeds — stale
/// SASL creds count as a failure, triggering testcontainers fallback.
async fn try_live_cluster() -> Option<KafkaFixture> {
    let cfg = live_config_from_env()?;
    if !tcp_reachable(&cfg.brokers).await {
        return None;
    }
    // Authenticated probe — if SASL creds are stale, this fails and we
    // fall through to testcontainers.
    if !authenticated_probe(&cfg).await {
        return None;
    }
    Some(KafkaFixture {
        config: cfg,
        mode: FixtureMode::Live,
        container: None,
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
// Container naming and cleanup
// =============================================================================
//
// Every container this suite starts carries a name that says which repo, which
// suite and which backing service it is, so an operator looking at `docker ps`
// can tell what left it behind. testcontainers' default is a random hex name,
// which is untraceable the moment one survives.
//
// Naming: `dfe-transform-vector-test-integration-<test>-<service>`, because
// every container here is owned by exactly ONE test. nextest runs each test in
// its own process, so nothing is shared even when it looks like it should be --
// two tests calling `KafkaFixture::acquire` start two brokers. That was already
// true with testcontainers' random names; the only thing a single shared name
// would add is a collision, where the first test wins and the rest fail with
// "name is already in use" and skip. `container_name` still takes `None` for a
// container started once for a whole binary, but no suite does that today.
//
// Cleanup is belt AND braces, because `Drop` alone is not enough:
//
//   - Normal completion and a panic both unwind, so `Drop` stops the container.
//   - A SIGKILL, an abort, or Ctrl-C on the test run does NOT. `Drop` never
//     runs and the container survives.
//
// testcontainers-rs 0.27 has no resource reaper (no Ryuk), so the second case
// is the one that leaves crap behind. A deterministic name would then make it
// WORSE than a random one -- the leaked container holds the name and every
// later run fails with "name already in use". `reap_stale` closes that: remove
// any container already holding the name before starting, so a leak costs the
// next run nothing and self-heals.
//
// The label goes on as well, so a sweep can find these regardless of name:
//   docker rm -f $(docker ps -aq --filter label=io.hyperi.test.suite=dfe-transform-vector-integration)

/// Label marking every container this suite starts, for bulk cleanup.
pub const TEST_SUITE_LABEL: (&str, &str) =
    ("io.hyperi.test.suite", "dfe-transform-vector-integration");

/// Labels for a container this suite starts: what it is, and whose run owns it.
///
/// The name says what and why; these say WHO, which is what you need when
/// several runs share a machine and one has left something behind. The pid is
/// the owning test process -- `ps -p <pid>` answers "is that run still alive, or
/// is this rubbish I can remove?".
fn test_labels(service: &str) -> Vec<(String, String)> {
    vec![
        (
            TEST_SUITE_LABEL.0.to_string(),
            TEST_SUITE_LABEL.1.to_string(),
        ),
        (
            "io.hyperi.test.repo".to_string(),
            "dfe-transform-vector".to_string(),
        ),
        ("io.hyperi.test.service".to_string(), service.to_string()),
        (
            "io.hyperi.test.owner-pid".to_string(),
            std::process::id().to_string(),
        ),
    ]
}

/// Container name for a backing service in this suite.
///
/// Pass `Some(test)` -- the owning test -- for anything a test starts for itself,
/// which is everything here. `None` is for a container started once for a whole
/// test binary; nothing does that today, and using it from several tests would
/// make them collide on the name rather than share the container.
///
/// Names are lowercased and non-alphanumerics collapse to `-`, because Docker
/// only accepts `[a-zA-Z0-9][a-zA-Z0-9_.-]*`, and a Rust test path
/// (`kafka::test_roundtrip`) has colons in it.
#[must_use]
pub fn container_name(test: Option<&str>, service: &str) -> String {
    let slug = |s: &str| {
        s.chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() {
                    c.to_ascii_lowercase()
                } else {
                    '-'
                }
            })
            .collect::<String>()
    };
    test.map_or_else(
        || format!("dfe-transform-vector-test-integration-{}", slug(service)),
        |t| {
            format!(
                "dfe-transform-vector-test-integration-{}-{}",
                slug(t),
                slug(service)
            )
        },
    )
}

/// Remove a DEAD container holding `name`, so a leak from a killed run cannot
/// block this one.
///
/// Never touches a RUNNING container. Two concurrent runs of this suite on one
/// machine share these names, and force-removing a live one would sabotage the
/// other run -- a confusing mid-test failure in a process that did nothing
/// wrong. Leaving it means the start below fails with "name is already in use",
/// which says what actually happened.
///
/// Best-effort otherwise: no Docker, nothing to remove, or an already-gone
/// container are all fine. A failure here must not fail the test -- the start
/// that follows reports the real problem.
pub fn reap_stale(name: &str) {
    let running = std::process::Command::new("docker")
        .args(["ps", "--quiet", "--filter", &format!("name=^{name}$")])
        .output();
    // Non-empty stdout means a container by this name is up. Leave it alone.
    if let Ok(out) = &running
        && !out.stdout.is_empty()
    {
        return;
    }
    let _ = std::process::Command::new("docker")
        .args(["rm", "--force", "--volumes", name])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
}

/// Spawn an ephemeral Apache Kafka container (testcontainers).
///
/// The container runs in PLAINTEXT mode (no SASL/TLS) — sufficient for
/// validating pipeline wiring. The returned fixture owns the container;
/// it stops on drop.
///
/// `test` names the calling test and goes into the container name, so
/// concurrent tests do not collide on it.
async fn try_testcontainer(test: &str) -> Result<KafkaFixture, String> {
    use testcontainers::ImageExt;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::kafka::apache;

    // Pinned here, not left to the module default of 3.8.0. A tag baked into a
    // dependency's source is invisible to dependency review: Renovate reads
    // Cargo.toml, correctly reports the crate current, and never sees the image.
    // renovate: datasource=docker depName=apache/kafka-native
    const KAFKA_TAG: &str = "4.3.1";

    let name = container_name(Some(test), "kafka");
    reap_stale(&name);
    let container = apache::Kafka::default()
        .with_tag(KAFKA_TAG)
        .with_container_name(&name)
        .with_labels(test_labels("kafka"))
        .start()
        .await
        .map_err(|e| format!("start kafka container: {e}"))?;

    let host = container
        .get_host()
        .await
        .map_err(|e| format!("get host: {e}"))?;
    let port = container
        .get_host_port_ipv4(apache::KAFKA_PORT)
        .await
        .map_err(|e| format!("get mapped port: {e}"))?;

    let bootstrap = format!("{host}:{port}");

    let config = KafkaConfig {
        profile: KafkaProfile::DevTest,
        brokers: vec![bootstrap],
        group: "dfe-transform-vector-tests".into(),
        security_protocol: "PLAINTEXT".into(),
        ..Default::default()
    };

    Ok(KafkaFixture {
        config,
        mode: FixtureMode::Testcontainer,
        container: Some(KafkaContainerHandle::Plaintext(container)),
    })
}

/// Username the SCRAM broker bootstraps at format time.
const SCRAM_USERNAME: &str = "dfe-test-user";

/// Password of [`SCRAM_USERNAME`]; a fixture's, never a deployment's.
const SCRAM_PASSWORD: &str = "scram-test-password";

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

/// Spawn an ephemeral Apache Kafka container that only accepts
/// SASL/SCRAM-SHA-512, and a fixture config that authenticates to it.
///
/// `test` names the calling test and goes into the container name, so
/// concurrent tests do not collide on it.
async fn try_scram_testcontainer(test: &str) -> Result<KafkaFixture, String> {
    use testcontainers::core::{IntoContainerPort, WaitFor};
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{GenericImage, ImageExt};

    // The broker image the single tier runs, digest-pinned.
    // renovate: datasource=docker depName=apache/kafka
    const KAFKA_TAG: &str =
        "4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837";

    // The broker tells clients to reconnect to the address it advertises, so
    // the host port is chosen before it starts rather than read back after.
    let address = free_port().await;
    let host_port: u16 = address
        .rsplit(':')
        .next()
        .and_then(|p| p.parse().ok())
        .ok_or_else(|| format!("no port in {address}"))?;

    let name = container_name(Some(test), "kafka-scram");
    reap_stale(&name);
    let container = GenericImage::new("apache/kafka", KAFKA_TAG)
        .with_entrypoint("/bin/sh")
        .with_exposed_port(9092.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Kafka Server started"))
        .with_cmd(["-c", SCRAM_BROKER_SCRIPT])
        .with_env_var("ADVERTISED_ADDRESS", address.clone())
        .with_env_var("SCRAM_USERNAME", SCRAM_USERNAME)
        .with_env_var("SCRAM_PASSWORD", SCRAM_PASSWORD)
        .with_mapped_port(host_port, 9092.tcp())
        .with_container_name(&name)
        .with_labels(test_labels("kafka-scram"))
        .start()
        .await
        .map_err(|e| format!("start SCRAM kafka container: {e}"))?;

    let config = KafkaConfig {
        profile: KafkaProfile::DevTest,
        brokers: vec![address],
        group: "dfe-transform-vector-tests".into(),
        security_protocol: "SASL_PLAINTEXT".into(),
        sasl_mechanism: Some("SCRAM-SHA-512".into()),
        sasl_username: Some(SCRAM_USERNAME.into()),
        sasl_password: Some(SensitiveString::new(SCRAM_PASSWORD)),
        ..Default::default()
    };

    Ok(KafkaFixture {
        config,
        mode: FixtureMode::Testcontainer,
        container: Some(KafkaContainerHandle::Scram(container)),
    })
}

// =============================================================================
// Legacy sync helpers — kept for existing integration tests that only
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

/// Get a free port by binding to :0, extracting the address, then dropping.
pub async fn free_port() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr.to_string()
}
