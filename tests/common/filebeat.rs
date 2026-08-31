#![allow(dead_code, clippy::panic)]
// Project:   dfe-transform-vector
// File:      tests/common/filebeat.rs
// Purpose:   The filebeat corpus and its golden comparison, read from dfe-transform-vrl
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The elastic/integrations filebeat corpus, and what counts as agreeing
//! with it.
//!
//! Both transform apps run the SAME bundled pipeline over the SAME data in
//! WS21, and a parity test only means something if both grade on the same
//! curve. dfe-transform-vrl owns the corpus archive, the bundled VRL and the
//! list of documented divergences from elastic's goldens; this is the reader
//! for them, and it mirrors `dfe-transform-vrl/tests/common/filebeat.rs`.
//!
//! The archive is read into memory and NEVER unpacked to the working tree:
//! it carries Elastic-licensed data whose terms travel with it.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// The dfe-transform-vrl checkout that owns the corpus and the pipeline.
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

/// The corpus archive.
pub fn corpus_archive() -> Option<PathBuf> {
    vrl_repo().map(|dir| dir.join("tests/fixtures/filebeat/filebeat-testdata.tar.gz"))
}

// =========================================================================
// The corpus
// =========================================================================

/// Every umbrella log in the corpus, in the order the suites replay them.
pub const UMBRELLA_LOGS: &[&str] = &[
    "cisco_umbrella/log/test-umbrella-auditlogs.log",
    "cisco_umbrella/log/test-umbrella-cloudfirewalllogs.log",
    "cisco_umbrella/log/test-umbrella-dlplogs.log",
    "cisco_umbrella/log/test-umbrella-dnslogs.log",
    "cisco_umbrella/log/test-umbrella-intrusionlogs.log",
    "cisco_umbrella/log/test-umbrella-iplogs.log",
    "cisco_umbrella/log/test-umbrella-proxylogs.log",
];

/// Every `cisco_ios` log in the corpus.
pub const IOS_LOGS: &[&str] = &[
    "cisco_ios/log/test-asr920.log",
    "cisco_ios/log/test-badauth.log",
    "cisco_ios/log/test-cisco-ios.log",
    "cisco_ios/log/test-date-format-tzoffset.log",
    "cisco_ios/log/test-date-format.log",
    "cisco_ios/log/test-syslog-header.log",
    "cisco_ios/log/test-syslog.log",
    "cisco_ios/log/test-tzoffset.log",
];

/// Every `cisco_meraki` log in the corpus.
pub const MERAKI_LOGS: &[&str] = &[
    "cisco_meraki/log/test-airmarshal-events.log",
    "cisco_meraki/log/test-events.log",
    "cisco_meraki/log/test-flows.log",
    "cisco_meraki/log/test-ip-flow.log",
    "cisco_meraki/log/test-security-events.log",
    "cisco_meraki/log/test-urls.log",
];

/// Every log file the corpus carries, all three modules.
pub fn all_logs() -> Vec<&'static str> {
    let mut out = Vec::with_capacity(UMBRELLA_LOGS.len() + IOS_LOGS.len() + MERAKI_LOGS.len());
    out.extend_from_slice(UMBRELLA_LOGS);
    out.extend_from_slice(IOS_LOGS);
    out.extend_from_slice(MERAKI_LOGS);
    out
}

/// The corpus archive, read once and held in memory.
pub fn corpus() -> &'static BTreeMap<String, Vec<u8>> {
    static DATA: OnceLock<BTreeMap<String, Vec<u8>>> = OnceLock::new();
    DATA.get_or_init(|| {
        let path = corpus_archive().expect("dfe-transform-vrl checkout must be present");
        let file = std::fs::File::open(&path).expect("corpus archive must be readable");
        let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(file));
        let mut map = BTreeMap::new();
        for entry in archive.entries().expect("read corpus tar") {
            let mut entry = entry.expect("corpus tar entry");
            if !entry.header().entry_type().is_file() {
                continue;
            }
            let name = entry
                .path()
                .expect("entry path")
                .to_string_lossy()
                .trim_start_matches("./")
                .to_string();
            let mut buf = Vec::new();
            std::io::Read::read_to_end(&mut entry, &mut buf).expect("read entry");
            map.insert(name, buf);
        }
        assert!(!map.is_empty(), "corpus archive is empty");
        map
    })
}

/// One corpus member as text.
pub fn corpus_text(name: &str) -> String {
    let bytes = corpus()
        .get(name)
        .unwrap_or_else(|| panic!("fixture {name} missing from archive"));
    String::from_utf8_lossy(bytes).into_owned()
}

/// The non-blank lines of one corpus log.
pub fn corpus_lines(name: &str) -> Vec<String> {
    corpus_text(name)
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(str::to_string)
        .collect()
}

/// The golden events for one corpus log.
pub fn corpus_expected(name: &str) -> Vec<serde_json::Value> {
    let doc: serde_json::Value =
        serde_json::from_str(&corpus_text(name)).expect("expected.json parses");
    doc.get("expected")
        .and_then(|e| e.as_array())
        .expect("expected array")
        .clone()
}

/// The `fields._conf` object for a corpus file (`tz_offset`, `tz_map`, ...):
/// `<file>-config.yml` wins, else the module's `test-common-config.yml`
/// (mirrors the elastic test harness).
pub fn corpus_conf(log_name: &str) -> Option<serde_json::Value> {
    let per_file = format!("{log_name}-config.yml");
    let dir = log_name.rsplit_once('/').map_or("", |(d, _)| d);
    let common = format!("{dir}/test-common-config.yml");
    for candidate in [per_file, common] {
        if let Some(bytes) = corpus().get(&candidate) {
            let doc: serde_json::Value =
                serde_yaml_ng::from_slice(bytes).expect("config yml parses");
            if let Some(conf) = doc.get("fields").and_then(|f| f.get("_conf")) {
                return Some(conf.clone());
            }
        }
    }
    None
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

// =========================================================================
// What counts as agreeing with the goldens
// =========================================================================

/// Paths where the port deliberately diverges from the elastic goldens.
/// geoip enrichment is stripped (dfe-loader owns geoip), event.ingested
/// is elastic-harness-dynamic, log.file.path carries our routing seed.
pub const IGNORED_PATHS: &[&str] = &[
    "source.geo",
    "source.as",
    "destination.geo",
    "destination.as",
    "client.geo",
    "client.as",
    "event.ingested",
    "log.file.path",
    // VRL's parse_user_agent (woothee/ua-parser) classifies differently
    // from elastic's user_agent processor ("Mac OSX" vs "Mac OS X",
    // device "pc" vs "Mac"). Engine-library parity, not a port defect;
    // user_agent.original stays asserted.
    "user_agent.name",
    "user_agent.os",
    "user_agent.device",
    "user_agent.version",
    "user_agent.major",
    "user_agent.minor",
    "user_agent.patch",
];

fn ignored(path: &str) -> bool {
    IGNORED_PATHS
        .iter()
        .any(|p| path == *p || path.starts_with(&format!("{p}.")))
}

/// Narrow per-line divergences from the goldens, each with a cause that is
/// library parity or an elastic-integration feature the 2.1 templates
/// never had (see the pipeline's own README in dfe-transform-vrl).
pub const ACCEPTED_MISMATCHES: &[(&str, usize, &str)] = &[
    // VRL's public-suffix data includes the PSL private section
    // (*.servicebus.windows.net); elastic's registered_domain processor
    // uses the ICANN section only.
    (
        "cisco_umbrella/log/test-umbrella-dnslogs.log",
        6,
        "dns.question.registered_domain",
    ),
    (
        "cisco_umbrella/log/test-umbrella-dnslogs.log",
        6,
        "dns.question.subdomain",
    ),
    (
        "cisco_umbrella/log/test-umbrella-dnslogs.log",
        6,
        "dns.question.top_level_domain",
    ),
    (
        "cisco_umbrella/log/test-umbrella-dnslogs.log",
        9,
        "dns.question.registered_domain",
    ),
    (
        "cisco_umbrella/log/test-umbrella-dnslogs.log",
        9,
        "dns.question.subdomain",
    ),
    (
        "cisco_umbrella/log/test-umbrella-dnslogs.log",
        9,
        "dns.question.top_level_domain",
    ),
    (
        "cisco_umbrella/log/test-umbrella-dnslogs.log",
        10,
        "dns.question.registered_domain",
    ),
    (
        "cisco_umbrella/log/test-umbrella-dnslogs.log",
        10,
        "dns.question.subdomain",
    ),
    (
        "cisco_umbrella/log/test-umbrella-dnslogs.log",
        10,
        "dns.question.top_level_domain",
    ),
    // VRL parse_url normalises an empty path to "/"; elastic keeps "".
    (
        "cisco_umbrella/log/test-umbrella-dlplogs.log",
        0,
        "url.path",
    ),
    // VRL is_nullish(" ") is true, so the template skips user_agent
    // entirely for whitespace-only UA strings; elastic still emits
    // user_agent.original " " with name "Other". Library semantics.
    (
        "cisco_umbrella/log/test-umbrella-proxylogs.log",
        7,
        "user_agent",
    ),
    (
        "cisco_umbrella/log/test-umbrella-proxylogs.log",
        8,
        "user_agent",
    ),
    (
        "cisco_umbrella/log/test-umbrella-proxylogs.log",
        9,
        "user_agent",
    ),
    (
        "cisco_umbrella/log/test-umbrella-proxylogs.log",
        10,
        "user_agent",
    ),
    (
        "cisco_umbrella/log/test-umbrella-proxylogs.log",
        11,
        "user_agent",
    ),
];

fn accepted_mismatch(file: &str, idx: usize, path: &str) -> bool {
    ACCEPTED_MISMATCHES
        .iter()
        .any(|(f, i, p)| *f == file && *i == idx && *p == path)
}

/// Whether a corpus file's timestamps may differ in the year alone.
///
/// `cisco_ios` only: its classic "Feb  8 04:00:48" format carries no year, so
/// the parser infers the CURRENT year while the goldens embed their
/// generation year.
pub fn allows_year_swap(log_name: &str) -> bool {
    log_name.starts_with("cisco_ios/")
}

/// Timestamps match at millisecond precision, with the year-swap tolerance
/// applied when the corpus file has no year to parse.
fn timestamps_match(ours: &str, exp: &str, allow_year_swap: bool) -> bool {
    use chrono::{DateTime, Datelike, SubsecRound};
    let (Ok(a), Ok(b)) = (
        DateTime::parse_from_rfc3339(ours),
        DateTime::parse_from_rfc3339(exp),
    ) else {
        return false;
    };
    let a = a.trunc_subsecs(3);
    let b = b.trunc_subsecs(3);
    if a == b {
        return true;
    }
    if !allow_year_swap {
        return false;
    }
    let (Some(a), Some(b)) = (a.with_year(2000), b.with_year(2000)) else {
        return false;
    };
    a == b
}

/// Canonical query-string compare: same key -> multiset of values,
/// independent of parameter order (the 2.1 template rebuilds url.query
/// from a VRL object, which alphabetises; elastic keeps raw order).
fn query_params(q: &str) -> BTreeMap<String, Vec<String>> {
    let mut map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for pair in q.split('&').filter(|s| !s.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        map.entry(k.to_string()).or_default().push(v.to_string());
    }
    for values in map.values_mut() {
        values.sort();
    }
    map
}

/// Compare one transformed event against its elastic golden: every leaf in
/// `expected` (bar the documented ignores) must be present and equal in
/// ours. Extra fields on our side (DFE 2.1 event carriage, and the fields
/// Vector's kafka source adds) are fine. The root `tags` array compares as a
/// set (the 2.1 template seeds tags in a different order).
pub fn diff_expected(
    path: &str,
    exp: &serde_json::Value,
    ours: &serde_json::Value,
    allow_year_swap: bool,
    problems: &mut Vec<String>,
) {
    if ignored(path) {
        return;
    }
    match exp {
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                let child = if path.is_empty() {
                    k.clone()
                } else {
                    format!("{path}.{k}")
                };
                match ours.get(k) {
                    Some(o) => diff_expected(&child, v, o, allow_year_swap, problems),
                    None => {
                        if !ignored(&child) {
                            problems.push(format!("{child}: missing (expected {v})"));
                        }
                    }
                }
            }
        }
        serde_json::Value::Array(items) if path == "tags" => {
            let ours_set: Vec<&serde_json::Value> = ours
                .as_array()
                .map(|a| a.iter().collect())
                .unwrap_or_default();
            for item in items {
                if !ours_set.contains(&item) {
                    problems.push(format!("tags: missing {item}"));
                }
            }
        }
        serde_json::Value::Array(items) => match ours.as_array() {
            Some(ours_arr) if ours_arr.len() == items.len() => {
                for (i, (e, o)) in items.iter().zip(ours_arr).enumerate() {
                    diff_expected(&format!("{path}[{i}]"), e, o, allow_year_swap, problems);
                }
            }
            _ => problems.push(format!("{path}: expected array {exp}, got {ours}")),
        },
        leaf => {
            if path == "@timestamp" || path.ends_with(".@timestamp") {
                let ours_s = ours.as_str().unwrap_or_default();
                let exp_s = leaf.as_str().unwrap_or_default();
                if !timestamps_match(ours_s, exp_s, allow_year_swap) {
                    problems.push(format!("{path}: expected {exp_s}, got {ours_s}"));
                }
            } else if path == "url.query" {
                let ours_s = ours.as_str().unwrap_or_default();
                let exp_s = leaf.as_str().unwrap_or_default();
                if query_params(ours_s) != query_params(exp_s) {
                    problems.push(format!(
                        "{path}: params differ, expected {exp_s}, got {ours_s}"
                    ));
                }
            } else if ours != leaf {
                problems.push(format!("{path}: expected {leaf}, got {ours}"));
            }
        }
    }
}

/// Everything wrong with `ours` as the transform of `log_name[idx]`, after
/// the documented ignores and that line's accepted divergences are removed.
/// Empty means it agrees with the golden.
pub fn problems_against_golden(
    log_name: &str,
    idx: usize,
    expected: &serde_json::Value,
    ours: &serde_json::Value,
) -> Vec<String> {
    let mut problems = Vec::new();
    diff_expected(
        "",
        expected,
        ours,
        allows_year_swap(log_name),
        &mut problems,
    );
    problems
        .into_iter()
        .filter(|p| {
            let path = p.split(':').next().unwrap_or_default();
            !accepted_mismatch(log_name, idx, path)
        })
        .collect()
}

/// Whether the pipeline routed this event to the shape-unmatched catch-all.
pub fn is_unmatched(event: &serde_json::Value) -> bool {
    event
        .get("tags")
        .and_then(|t| t.as_array())
        .is_some_and(|t| t.contains(&serde_json::json!("filebeat_unmatched")))
}
