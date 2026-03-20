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
            sasl_password: env::var("KAFKA_SASL_PASSWORD").ok(),
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

pub(crate) use skip_if_no_kafka;
