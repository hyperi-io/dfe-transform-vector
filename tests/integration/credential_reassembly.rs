// Project:   dfe-transform-vector
// File:      tests/integration/credential_reassembly.rs
// Purpose:   A re-assembly never shows a reader a missing or empty credential
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The `.secrets` directory under a reload.
//!
//! A reload re-assembles the config directory Vector reads its credentials
//! from. Anything that reads those files while it runs, the supervisor
//! restarting Vector mid-reload for one, must find each file whole: the old
//! credential or the new one, never missing and never truncated.

use std::fs;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use dfe_transform_vector::config::assembler;
use dfe_transform_vector::config::loader::{Config, SinkConfig, SourceConfig};
use tempfile::TempDir;

#[test]
fn a_reassembly_never_shows_a_reader_a_missing_or_empty_credential() {
    let output = TempDir::new().unwrap();
    let mut config = Config {
        source: SourceConfig {
            brokers: vec!["kafka:9092".into()],
            topics: vec!["input".into()],
            group_id: "test-group".into(),
            ..Default::default()
        },
        sink: SinkConfig {
            brokers: vec!["kafka:9092".into()],
            topic: "output".into(),
            ..Default::default()
        },
        ..Default::default()
    };
    config.source.sasl.enabled = true;
    config.source.sasl.username = "src-user".into();
    config.source.sasl.password = "src-pass".into();
    assembler::assemble(&config, output.path()).unwrap();

    let password = output.path().join(".secrets").join("source_sasl_password");
    let done = AtomicBool::new(false);
    let missing = AtomicUsize::new(0);
    let empty = AtomicUsize::new(0);
    let reads = AtomicUsize::new(0);

    std::thread::scope(|scope| {
        scope.spawn(|| {
            while !done.load(Ordering::Relaxed) {
                match fs::read_to_string(&password) {
                    Ok(text) if text.is_empty() => {
                        empty.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(_) => {}
                    Err(_) => {
                        missing.fetch_add(1, Ordering::Relaxed);
                    }
                }
                reads.fetch_add(1, Ordering::Relaxed);
            }
        });
        for _ in 0..300 {
            assembler::assemble(&config, output.path()).unwrap();
        }
        done.store(true, Ordering::Relaxed);
    });

    assert!(reads.load(Ordering::Relaxed) > 0);
    assert_eq!(
        (
            missing.load(Ordering::Relaxed),
            empty.load(Ordering::Relaxed)
        ),
        (0, 0),
        "a reader saw the credential missing or empty during re-assembly"
    );
    assert_eq!(fs::read_to_string(&password).unwrap(), "src-pass");
}

/// A credential the new config no longer needs does not linger.
#[test]
fn a_credential_the_new_config_drops_is_removed() {
    let output = TempDir::new().unwrap();
    let mut config = Config {
        source: SourceConfig {
            brokers: vec!["kafka:9092".into()],
            topics: vec!["input".into()],
            group_id: "test-group".into(),
            ..Default::default()
        },
        sink: SinkConfig {
            brokers: vec!["kafka:9092".into()],
            topic: "output".into(),
            ..Default::default()
        },
        ..Default::default()
    };
    config.source.sasl.enabled = true;
    config.source.sasl.username = "src-user".into();
    config.source.sasl.password = "src-pass".into();
    assembler::assemble(&config, output.path()).unwrap();
    assert!(output.path().join(".secrets").is_dir());

    config.source.sasl.enabled = false;
    assembler::assemble(&config, output.path()).unwrap();
    assert!(
        !output.path().join(".secrets").exists(),
        "a credential stayed on disk after the config stopped using it"
    );
}
