// A missing or malformed case file is unrecoverable and has no caller to
// report it to, so the loader panics rather than threading a Result through
// every test.
#![allow(dead_code, clippy::panic)]
// Project:   dfe-transform-vector
// File:      tests/common/filebeat.rs
// Purpose:   The filebeat test cases and their comparison, read from dfe-transform-vrl
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The synthetic filebeat cases in dfe-transform-vrl's
//! `tests/fixtures/filebeat/cases.yaml`, and what counts as agreeing with one.
//!
//! Both transform apps run the SAME bundled pipeline over the SAME cases, and a
//! parity test only means something if both grade on the same curve.
//! dfe-transform-vrl owns the cases and the bundled VRL, and this is the reader
//! for them. It mirrors `dfe-transform-vrl/tests/common/filebeat.rs`.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use serde::Deserialize;

/// The dfe-transform-vrl checkout that owns the cases and the pipeline.
///
/// `DFE_TRANSFORM_VRL_DIR` first, then a `dfe-transform-vrl` sitting beside
/// any ancestor of this checkout -- which finds it from a plain clone and from
/// a `.worktrees/<branch>` worktree alike, without naming anybody's workspace
/// layout. `None` means the acceptance case cannot run here, and the test says
/// so rather than asserting on nothing.
pub fn vrl_repo() -> Option<&'static Path> {
    static DIR: OnceLock<Option<PathBuf>> = OnceLock::new();
    DIR.get_or_init(|| {
        let has_pipeline = |dir: &Path| dir.join("pipelines/filebeat/filebeat.vrl").is_file();
        if let Some(explicit) = std::env::var_os("DFE_TRANSFORM_VRL_DIR") {
            let dir = PathBuf::from(explicit);
            return has_pipeline(&dir).then_some(dir);
        }
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .map(|a| a.join("dfe-transform-vrl"))
            .find(|dir| has_pipeline(dir))
    })
    .as_deref()
}

/// The bundled filebeat pipeline, or `None` without a dfe-transform-vrl checkout.
pub fn pipeline_vrl() -> Option<PathBuf> {
    vrl_repo().map(|dir| dir.join("pipelines/filebeat/filebeat.vrl"))
}

/// The timezones lookup the ios and meraki branches need.
pub fn timezones_csv() -> Option<PathBuf> {
    vrl_repo().map(|dir| dir.join("pipelines/filebeat/timezones.csv"))
}

// =========================================================================
// The cases
// =========================================================================

/// The filebeat module a case's line belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Module {
    Meraki,
    Ios,
    Umbrella,
}

/// One input line and what the pipeline must make of it.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Case {
    /// Unique across the file. The e2e suite carries it through the broker to
    /// pair each output with its case.
    pub name: String,
    pub module: Module,
    /// The `_conf` object the DFE 2.1 deployment injected (`tz_offset`).
    #[serde(default)]
    pub conf: Option<serde_json::Value>,
    /// The raw log line, as the `message` field.
    pub line: String,
    /// A subset of the transformed event: every leaf named here must be
    /// present and equal in the output. Fields the case does not name are not
    /// checked.
    pub expect: serde_json::Value,
}

/// Every case in the file, read once. Callers check [`vrl_repo`] first.
pub fn cases() -> &'static [Case] {
    static CASES: OnceLock<Vec<Case>> = OnceLock::new();
    CASES.get_or_init(|| {
        let path = vrl_repo()
            .expect("dfe-transform-vrl checkout must be present")
            .join("tests/fixtures/filebeat/cases.yaml");
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let cases: Vec<Case> =
            scalo::yaml::from_str(&text, "cases.yaml", scalo::yaml::Profile::Data)
                .unwrap_or_else(|e| panic!("parse cases.yaml: {e}"));
        let mut seen = HashSet::new();
        for case in &cases {
            assert!(
                seen.insert(case.name.as_str()),
                "duplicate case name {}",
                case.name
            );
        }
        cases
    })
}

/// A DFE 2.1-shaped input event: `{message, tags, timestamp}` plus the
/// `_conf` object the 2.1 deployment injected (`tz_offset`, `tz_map`).
pub fn input_event(message: &str, conf: Option<&serde_json::Value>) -> serde_json::Value {
    let mut obj = serde_json::json!({
        "message": message,
        "tags": [],
        "timestamp": "2026-01-01T00:00:00Z",
    });
    if let Some(conf) = conf {
        obj["_conf"] = conf.clone();
    }
    obj
}

/// The input event for one case.
pub fn case_event(case: &Case) -> serde_json::Value {
    input_event(&case.line, case.conf.as_ref())
}

// =========================================================================
// What counts as agreeing with a case
// =========================================================================

/// Compare one transformed event against a case's `expect`: every leaf in
/// `expected` must be present and equal in `ours`. Extra fields on our side
/// (DFE 2.1 event carriage like `_conf`, `timestamp`, and the fields Vector's
/// kafka source adds) are fine. The root `tags` array compares as a set,
/// because the 2.1 template seeds tags in a fixed but arbitrary order.
pub fn diff_expected(
    path: &str,
    expected: &serde_json::Value,
    ours: &serde_json::Value,
    problems: &mut Vec<String>,
) {
    match expected {
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                let child = if path.is_empty() {
                    k.clone()
                } else {
                    format!("{path}.{k}")
                };
                match ours.get(k) {
                    Some(o) => diff_expected(&child, v, o, problems),
                    None => problems.push(format!("{child}: missing (expected {v})")),
                }
            }
        }
        serde_json::Value::Array(items) if path == "tags" => {
            let have = ours.as_array().map(Vec::as_slice).unwrap_or_default();
            for item in items {
                if !have.contains(item) {
                    problems.push(format!("tags: missing {item}"));
                }
            }
        }
        serde_json::Value::Array(items) => match ours.as_array() {
            Some(have) if have.len() == items.len() => {
                for (i, (e, o)) in items.iter().zip(have).enumerate() {
                    diff_expected(&format!("{path}[{i}]"), e, o, problems);
                }
            }
            _ => problems.push(format!("{path}: expected array {expected}, got {ours}")),
        },
        leaf => {
            if ours != leaf {
                problems.push(format!("{path}: expected {leaf}, got {ours}"));
            }
        }
    }
}

/// Everything wrong with `ours` as the transform of `case`. Empty means it
/// agrees.
pub fn problems_for(case: &Case, ours: &serde_json::Value) -> Vec<String> {
    let mut problems = Vec::new();
    diff_expected("", &case.expect, ours, &mut problems);
    problems
}

/// Whether the pipeline routed this event to the shape-unmatched catch-all.
/// Every case belongs to a known module, so this must never fire on one.
pub fn is_unmatched(event: &serde_json::Value) -> bool {
    event
        .get("tags")
        .and_then(|t| t.as_array())
        .is_some_and(|t| t.contains(&serde_json::json!("filebeat_unmatched")))
}
