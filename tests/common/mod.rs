#![allow(dead_code, clippy::unwrap_used)]
// Project:   dfe-transform-vector
// File:      tests/common/mod.rs
// Purpose:   Shared test infrastructure — dual-mode Kafka config via rustlib
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Shared test helpers for integration and e2e tests.
//!
//! Supports two backends controlled by `TEST_MODE` in `.env`:
//! - `remote` (default) — devex cluster via env vars
//! - `docker` — dfe-docker infra profile on localhost

use std::env;
use std::net::ToSocketAddrs;
use std::time::Duration;

use hyperi_rustlib::SensitiveString;
use hyperi_rustlib::transport::kafka::{KafkaAdmin, KafkaConfig, KafkaProfile};

/// Test backend mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestMode {
    Remote,
    Docker,
}

impl TestMode {
    pub fn detect() -> Self {
        load_dotenv();
        match env::var("TEST_MODE").unwrap_or_default().as_str() {
            "docker" => Self::Docker,
            _ => Self::Remote,
        }
    }
}

pub fn load_dotenv() {
    let _ = dotenvy::dotenv();
}

/// Build a rustlib `KafkaConfig` for the active test mode.
///
/// Docker mode: `localhost:19092`, PLAINTEXT, devtest profile, no SASL.
/// Remote mode: from env vars (`KAFKA_BROKERS`, `KAFKA_SASL_MECHANISM`, etc.)
pub fn kafka_test_config() -> KafkaConfig {
    load_dotenv();
    match TestMode::detect() {
        TestMode::Docker => KafkaConfig {
            profile: KafkaProfile::DevTest,
            brokers: vec!["localhost:19092".into()],
            group: "e2e-test".into(),
            security_protocol: "PLAINTEXT".into(),
            ..Default::default()
        },
        TestMode::Remote => KafkaConfig {
            profile: KafkaProfile::DevTest,
            brokers: env::var("KAFKA_BROKERS")
                .unwrap_or_else(|_| "localhost:9092".into())
                .split(',')
                .map(String::from)
                .collect(),
            group: "e2e-test".into(),
            security_protocol: env::var("KAFKA_SECURITY_PROTOCOL")
                .unwrap_or_else(|_| "SASL_SSL".into()),
            sasl_mechanism: env::var("KAFKA_SASL_MECHANISM").ok(),
            sasl_username: env::var("KAFKA_SASL_USER").ok(),
            sasl_password: env::var("KAFKA_SASL_PASSWORD")
                .ok()
                .map(SensitiveString::new),
            ssl_skip_verify: true,
            ..Default::default()
        },
    }
}

/// Check if the first broker in the config is reachable via TCP.
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

/// Create a `KafkaAdmin` from the test config.
pub fn kafka_admin(config: &KafkaConfig) -> KafkaAdmin {
    KafkaAdmin::new(config).expect("failed to create KafkaAdmin")
}

/// Skip test if Kafka is not reachable in the current test mode.
#[allow(unused_macros)]
macro_rules! skip_if_no_kafka {
    () => {
        let kf = $crate::common::kafka_test_config();
        if !$crate::common::is_kafka_reachable(&kf) {
            eprintln!(
                "Skipping: Kafka not reachable at {:?} (TEST_MODE={:?})",
                kf.brokers,
                $crate::common::TestMode::detect()
            );
            return;
        }
    };
}

#[allow(unused_imports)]
pub(crate) use skip_if_no_kafka;

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
        // Fallback: system PATH
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

/// Get a free port by binding to :0, extracting the address, then dropping the listener.
pub async fn free_port() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr.to_string()
}
