// Project:   dfe-transform-vector
// File:      tests/integration/dead_letter_vrl.rs
// Purpose:   The size cap's dead letter remap, run through Vector's own VRL
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The remap that turns a record over the producer's ceiling into scalo's DLQ
//! entry, run through `vector vrl` so the program is the one the assembler
//! writes and the VRL is Vector's.
//!
//! Skips where no Vector binary is available.

use std::fs;
use std::process::Command;

use dfe_transform_vector::config::generate::{
    SIZE_CAP_DLQ_LABEL, generate_sink_yaml, max_record_bytes,
};
use dfe_transform_vector::config::loader::{BridgeConfig, SinkConfig};
use scalo::dlq::DlqConfig;
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::common;

/// The payload and copied header budget the tests give the DLQ.
const CAP: usize = 40;

/// Base64 characters for `bytes` bytes.
fn base64_len(bytes: usize) -> usize {
    bytes.div_ceil(3) * 4
}

/// Run `events` through the remap `sink` generates, one entry per event.
fn run_remap(sink: &SinkConfig, events: &[Value]) -> Vec<Value> {
    let dlq = DlqConfig {
        max_payload_bytes: Some(CAP),
        ..DlqConfig::default()
    };
    let yaml = generate_sink_yaml(sink, &BridgeConfig::default(), &["last".into()], true, &dlq);
    let program = yaml["transforms"][SIZE_CAP_DLQ_LABEL]["source"]
        .as_str()
        .expect("the remap source");

    let dir = TempDir::new().expect("work dir");
    let (program_file, input_file) = (dir.path().join("dlq.vrl"), dir.path().join("in.json"));
    fs::write(&program_file, program).expect("write the program");
    let lines: Vec<String> = events.iter().map(Value::to_string).collect();
    fs::write(&input_file, lines.join("\n")).expect("write the events");

    let vector = common::vector_binary_path().expect("a Vector binary");
    let output = Command::new(vector)
        .args(["vrl", "--quiet", "--print-object", "--input"])
        .arg(&input_file)
        .arg("--program")
        .arg(&program_file)
        .output()
        .expect("run vector vrl");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "vector vrl failed: {}\n{stdout}\n--- program ---\n{program}",
        String::from_utf8_lossy(&output.stderr)
    );
    stdout
        .lines()
        .filter(|line| line.starts_with('{'))
        .map(|line| serde_json::from_str(line).expect("an entry that is JSON"))
        .collect()
}

fn json_sink() -> SinkConfig {
    SinkConfig {
        topic: "syslog_load".into(),
        ..SinkConfig::default()
    }
}

/// The envelope scalo's DLQ writes for a record cut to its first bytes.
fn assert_envelope(entry: &Value, sink: &SinkConfig, original: usize) {
    assert_eq!(entry["service"], "dfe-transform-vector", "{entry}");
    assert_eq!(entry["attempts"], 0, "{entry}");
    assert_eq!(entry["truncated"], true, "{entry}");
    assert_eq!(entry["original_bytes"], original, "{entry}");
    assert_eq!(entry["destination"], sink.topic.as_str(), "{entry}");
    assert_eq!(entry["_raw_encoding"], "base64", "{entry}");
    assert_eq!(
        entry["reason"],
        format!(
            "record of {original} bytes is over the sink's {}-byte ceiling",
            max_record_bytes(sink)
        ),
        "{entry}"
    );
    assert!(
        entry["timestamp"]
            .as_i64()
            .is_some_and(|t| t > 1_700_000_000_000),
        "timestamp should be epoch milliseconds: {entry}"
    );
}

/// The header fields that fit are copied beside the payload and take their
/// bytes from the cap, so the payload's head is what they leave.
#[test]
fn a_dead_letter_copies_its_header_and_cuts_the_rest_to_the_cap() {
    common::skip_if_no_vector!();
    let sink = json_sink();
    let event = json!({"_org_id": "acme", "_source": "syslog", "id": 7, "blob": "x".repeat(100)});
    let original = event.to_string().len();

    let entries = run_remap(&sink, &[event]);

    assert_eq!(entries.len(), 1);
    let entry = &entries[0];
    assert_envelope(entry, &sink, original);
    assert_eq!(entry["_org_id"], "acme");
    assert_eq!(entry["_source"], "syslog");
    // `"acme"` and `"syslog"` are 6 and 8 bytes of the 40.
    assert_eq!(
        entry["_raw"].as_str().map(str::len),
        Some(base64_len(CAP - 14))
    );
    assert!(entry["_raw"].as_str().is_some_and(|r| r.starts_with("eyJ")));
    assert!(entry.get("header_dropped").is_none(), "{entry}");
}

/// A header value that does not fit is left out and named, as scalo's DLQ does.
#[test]
fn a_header_value_over_the_cap_is_named_in_header_dropped() {
    common::skip_if_no_vector!();
    let sink = json_sink();
    let event = json!({"_tags": ["t".repeat(60)], "id": 1});
    let original = event.to_string().len();

    let entries = run_remap(&sink, &[event]);

    let entry = &entries[0];
    assert_envelope(entry, &sink, original);
    assert_eq!(entry["header_dropped"], json!(["_tags"]));
    assert!(entry.get("_tags").is_none(), "{entry}");
    assert_eq!(entry["_raw"].as_str().map(str::len), Some(base64_len(CAP)));
}

/// The cut counts bytes, so a multibyte character split by it still leaves
/// exactly the cap's worth, and a record with no header keeps all of it.
#[test]
fn the_payload_is_cut_by_bytes_not_characters() {
    common::skip_if_no_vector!();
    let sink = json_sink();
    let event = json!({"blob": "\u{e9}".repeat(60)});
    let original = event.to_string().len();
    assert!(original > CAP * 2, "the record has to be over the cap");

    let entries = run_remap(&sink, &[event]);

    assert_envelope(&entries[0], &sink, original);
    assert_eq!(
        entries[0]["_raw"].as_str().map(str::len),
        Some(base64_len(CAP))
    );
}

/// With `raw_bytes` the record is the `message` field, and the dead letter
/// measures and cuts that, as the size cap does.
#[test]
fn a_raw_bytes_record_is_the_message_field() {
    common::skip_if_no_vector!();
    let sink = SinkConfig {
        encoding: "raw_bytes".into(),
        ..json_sink()
    };
    let event = json!({"_org_id": "acme", "message": "m".repeat(100)});

    let entries = run_remap(&sink, &[event]);

    let entry = &entries[0];
    assert_envelope(entry, &sink, 100);
    assert_eq!(entry["_org_id"], "acme");
    assert_eq!(
        entry["_raw"].as_str().map(str::len),
        Some(base64_len(CAP - 6))
    );
    assert!(
        entry["_raw"]
            .as_str()
            .is_some_and(|r| r.starts_with("bW1t"))
    );
}
