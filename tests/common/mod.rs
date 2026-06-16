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

use hyperi_rustlib::SensitiveString;
use hyperi_rustlib::transport::kafka::{KafkaAdmin, KafkaConfig, KafkaProfile};

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
struct KafkaContainerHandle {
    _inner: testcontainers::ContainerAsync<testcontainers_modules::kafka::apache::Kafka>,
}

impl KafkaFixture {
    /// Acquire a Kafka fixture using live-first, testcontainers-fallback order.
    ///
    /// Returns `None` if neither live nor Docker are available — callers
    /// should skip the test in that case.
    pub async fn acquire() -> Option<Self> {
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
        match try_testcontainer().await {
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

/// Spawn an ephemeral Apache Kafka container (testcontainers).
///
/// The container runs in PLAINTEXT mode (no SASL/TLS) — sufficient for
/// validating pipeline wiring. The returned fixture owns the container;
/// it stops on drop.
async fn try_testcontainer() -> Result<KafkaFixture, String> {
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::kafka::apache;

    let container = apache::Kafka::default()
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
        container: Some(KafkaContainerHandle { _inner: container }),
    })
}

// =============================================================================
// Legacy sync helpers — kept for existing integration tests that only
// build configs (don't actually connect). New tests should use
// `KafkaFixture::acquire()` instead.
// =============================================================================

/// Build a rustlib `KafkaConfig` from env (sync — for tests that only
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
