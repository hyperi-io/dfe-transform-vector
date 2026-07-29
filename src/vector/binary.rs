// Project:   dfe-transform-vector
// File:      src/vector/binary.rs
// Purpose:   Vector binary acquisition -- version resolution + persistent cache
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Which Vector binary to run, and where it comes from.
//!
//! Three things have to hold at once:
//!
//! - **An airgapped deploy works cold.** The image pre-ships a binary, so there
//!   is always something to run without a network.
//! - **A GitHub outage is invisible.** Resolution failures degrade to the
//!   pre-shipped version rather than stopping the service. This is the PRIMARY
//!   case, not an edge case.
//! - **Operators can still choose.** A cache outside the container holds other
//!   versions, so a deployment can run something other than the pre-shipped
//!   binary without rebuilding the image.
//!
//! The asymmetry that falls out of that: **resolution failures degrade,
//! acquisition failures abort.** Not knowing which version is newest is
//! survivable; having no binary at all is not.
//!
//! The version-selection functions here are deliberately PURE -- they take a
//! release list rather than fetching one. The `stable` rules in particular
//! (previous-minor, plus the major-adoption guard) are the part most likely to
//! be got wrong, and they are worth testing without a network in the loop.

use std::path::{Path, PathBuf};

use tracing::{debug, info, warn};

use crate::Error;

/// Where the Vector version comes from.
///
/// Parsed from the `vector.version_source` config value. A bare version string
/// selects itself: three components pin exactly, two follow a minor line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionSource {
    /// The version compiled into the image. Default -- no network, always
    /// present, and the only mode guaranteed to work cold.
    Preshipped,
    /// Newest published release.
    Latest,
    /// Newest patch of the PREVIOUS minor line. See [`select_stable`].
    Stable,
    /// An exact version, e.g. `0.56.0`. Needs no lookup, which is what makes it
    /// the airgap-friendly non-default.
    Exact(String),
    /// A minor line, e.g. `0.56` -- resolved to its newest patch.
    MinorLine(String),
}

impl VersionSource {
    /// Parse a config value.
    ///
    /// # Errors
    /// When the value is neither a known keyword nor a `MAJOR.MINOR[.PATCH]`
    /// version. Rejecting rather than defaulting is deliberate: a typo like
    /// `lastest` silently becoming `preshipped` is the kind of quiet
    /// substitution this module exists to avoid.
    pub fn parse(s: &str) -> Result<Self, Error> {
        let t = s.trim();
        match t {
            "preshipped" => return Ok(Self::Preshipped),
            "latest" => return Ok(Self::Latest),
            "stable" => return Ok(Self::Stable),
            _ => {}
        }

        let parts: Vec<&str> = t.split('.').collect();
        let numeric = parts
            .iter()
            .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()));
        match (parts.len(), numeric) {
            (3, true) => Ok(Self::Exact(t.to_string())),
            (2, true) => Ok(Self::MinorLine(t.to_string())),
            _ => Err(Error::Vector(format!(
                "vector.version_source: expected preshipped, latest, stable, \
                 MAJOR.MINOR or MAJOR.MINOR.PATCH -- got {t:?}"
            ))),
        }
    }

    /// Whether resolving this source needs to reach the network.
    #[must_use]
    pub const fn needs_lookup(&self) -> bool {
        matches!(self, Self::Latest | Self::Stable | Self::MinorLine(_))
    }
}

/// A parsed `MAJOR.MINOR.PATCH`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl Version {
    /// Parse `MAJOR.MINOR.PATCH`, tolerating a leading `v`.
    ///
    /// Returns `None` for anything else, including pre-releases -- an `-rc`
    /// build must never be selected by `latest` or `stable`.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim().strip_prefix('v').unwrap_or(s.trim());
        let mut it = s.split('.');
        let major = it.next()?.parse().ok()?;
        let minor = it.next()?.parse().ok()?;
        let patch = it.next()?.parse().ok()?;
        if it.next().is_some() {
            return None;
        }
        Some(Self {
            major,
            minor,
            patch,
        })
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// Highest version in `releases`.
#[must_use]
pub fn select_latest(releases: &[Version]) -> Option<Version> {
    releases.iter().copied().max()
}

/// Newest patch of the previous minor line -- the `stable` rule.
///
/// Two things are going on:
///
/// **Previous minor, not previous patch.** Vector is 0.x, where the MINOR is
/// the effective breaking unit: 0.57 -> 0.56 is a real step, 0.56.3 -> 0.56.0 is
/// not. So `stable` means "one breaking step back, fully patched".
///
/// **A new major is not eligible until `x.1.0` exists.** A brand-new major gets
/// its worst bugs in `x.0.z`; adopting it as *stable* the day it lands defeats
/// the point of the mode. So while the newest release is `1.0.4`, `stable`
/// stays on the 0.x line. Once `1.1.0` publishes, major 1 becomes eligible and
/// the previous-minor rule then picks the newest `1.0.z`.
///
/// Falls back to the latest release when there is no previous minor to pick
/// (a line with only one minor published), because returning nothing would turn
/// a resolvable request into a failure for no benefit.
#[must_use]
pub fn select_stable(releases: &[Version]) -> Option<Version> {
    let major = highest_eligible_major(releases)?;

    let minors: Vec<u32> = {
        let mut m: Vec<u32> = releases
            .iter()
            .filter(|v| v.major == major)
            .map(|v| v.minor)
            .collect();
        m.sort_unstable();
        m.dedup();
        m
    };

    // Second-highest minor in the eligible major; if there is only one, the
    // latest IS the stable choice.
    let target_minor = match minors.len() {
        0 => return None,
        1 => minors[0],
        n => minors[n - 2],
    };

    releases
        .iter()
        .copied()
        .filter(|v| v.major == major && v.minor == target_minor)
        .max()
}

/// The highest major that has published at least one `x.1.0`.
///
/// Major 0 is always eligible -- it is the pre-1.0 line, and "wait for 0.1.0"
/// would be nonsense for a project already at 0.57.
fn highest_eligible_major(releases: &[Version]) -> Option<u32> {
    let mut majors: Vec<u32> = releases.iter().map(|v| v.major).collect();
    majors.sort_unstable();
    majors.dedup();

    majors
        .into_iter()
        .rev()
        .find(|&m| m == 0 || releases.iter().any(|v| v.major == m && v.minor >= 1))
}

/// Newest patch on a given `MAJOR.MINOR` line.
#[must_use]
pub fn select_minor_line(releases: &[Version], line: &str) -> Option<Version> {
    let mut it = line.split('.');
    let major: u32 = it.next()?.parse().ok()?;
    let minor: u32 = it.next()?.parse().ok()?;

    releases
        .iter()
        .copied()
        .filter(|v| v.major == major && v.minor == minor)
        .max()
}

/// Resolve `source` against a release list, degrading to `preshipped`.
///
/// `releases` is `None` when the lookup could not be performed -- no network,
/// GitHub down, rate-limited. That is NOT an error: the whole point of
/// pre-shipping a binary is that this case keeps working. It warns and returns
/// the pre-shipped version, so the reason appears in the log rather than being
/// inferred later from a surprising version.
///
/// # Errors
/// Only when `source` is a keyword whose selection genuinely cannot be made
/// from a release list that WAS fetched -- e.g. `MinorLine("9.9")` when no such
/// line exists. A wrong request is worth reporting; an unreachable network is
/// not.
pub fn resolve(
    source: &VersionSource,
    preshipped: &str,
    releases: Option<&[Version]>,
) -> Result<String, Error> {
    if let VersionSource::Preshipped = source {
        return Ok(preshipped.to_string());
    }
    if let VersionSource::Exact(v) = source {
        return Ok(v.clone());
    }

    let Some(releases) = releases else {
        warn!(
            source = ?source,
            %preshipped,
            "could not reach the release index; using the pre-shipped Vector. \
             This is expected offline or during a GitHub outage."
        );
        return Ok(preshipped.to_string());
    };

    let picked = match source {
        VersionSource::Latest => select_latest(releases),
        VersionSource::Stable => select_stable(releases),
        VersionSource::MinorLine(line) => select_minor_line(releases, line),
        VersionSource::Preshipped | VersionSource::Exact(_) => unreachable!("handled above"),
    };

    match picked {
        Some(v) => {
            info!(source = ?source, resolved = %v, "resolved Vector version");
            Ok(v.to_string())
        }
        None => Err(Error::Vector(format!(
            "vector.version_source {source:?} matched no published release \
             ({} releases known)",
            releases.len()
        ))),
    }
}

/// Cache filename for a version on a given target.
///
/// Keyed on version AND target triple, not version alone: the release archives
/// are per-arch (`vector-0.56.0-aarch64-unknown-linux-gnu`), so a version-only
/// key would happily serve an x86_64 binary to an arm64 node.
#[must_use]
pub fn cache_path(cache_dir: &Path, version: &str, target: &str) -> PathBuf {
    cache_dir.join(format!("vector-{version}-{target}"))
}

/// Make the pre-shipped binary available in the cache.
///
/// Copies rather than symlinks: the cache outlives the container, and a link
/// into a deleted image layer is worse than no entry at all.
///
/// A failure here is logged, not returned. The pre-shipped binary is still
/// usable in place, so an unwritable cache degrades performance on later starts
/// rather than blocking this one.
pub fn seed_cache(cache_dir: &Path, preshipped_bin: &Path, version: &str, target: &str) {
    let dest = cache_path(cache_dir, version, target);
    if dest.exists() {
        debug!(path = %dest.display(), "cache already holds the pre-shipped Vector");
        return;
    }
    if !preshipped_bin.exists() {
        debug!(path = %preshipped_bin.display(), "no pre-shipped Vector to seed from");
        return;
    }

    if let Err(e) = std::fs::create_dir_all(cache_dir) {
        warn!(dir = %cache_dir.display(), error = %e, "could not create the Vector cache dir");
        return;
    }
    match std::fs::copy(preshipped_bin, &dest) {
        Ok(_) => {
            set_executable(&dest);
            info!(path = %dest.display(), %version, "seeded the Vector cache from the pre-shipped binary");
        }
        Err(e) => warn!(
            path = %dest.display(),
            error = %e,
            "could not seed the Vector cache; the pre-shipped binary is still usable in place"
        ),
    }
}

#[cfg(unix)]
fn set_executable(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    if let Ok(md) = std::fs::metadata(path) {
        let mut perms = md.permissions();
        perms.set_mode(perms.mode() | 0o111);
        if let Err(e) = std::fs::set_permissions(path, perms) {
            warn!(path = %path.display(), error = %e, "could not mark the cached Vector executable");
        }
    }
}

#[cfg(not(unix))]
fn set_executable(_path: &Path) {}

#[cfg(test)]
mod tests {
    use super::*;

    fn vs(list: &[&str]) -> Vec<Version> {
        list.iter().filter_map(|s| Version::parse(s)).collect()
    }

    #[test]
    fn parses_keywords_and_versions() {
        assert_eq!(
            VersionSource::parse("preshipped").unwrap(),
            VersionSource::Preshipped
        );
        assert_eq!(
            VersionSource::parse("latest").unwrap(),
            VersionSource::Latest
        );
        assert_eq!(
            VersionSource::parse("stable").unwrap(),
            VersionSource::Stable
        );
        assert_eq!(
            VersionSource::parse("0.56.0").unwrap(),
            VersionSource::Exact("0.56.0".into())
        );
        assert_eq!(
            VersionSource::parse("0.56").unwrap(),
            VersionSource::MinorLine("0.56".into())
        );
        assert_eq!(
            VersionSource::parse("  stable  ").unwrap(),
            VersionSource::Stable
        );
    }

    #[test]
    fn rejects_a_typo_rather_than_defaulting() {
        // A silent fallback to `preshipped` here would mean a deployment asking
        // for `latest` quietly gets something else.
        assert!(VersionSource::parse("lastest").is_err());
        assert!(VersionSource::parse("").is_err());
        assert!(VersionSource::parse("0").is_err());
        assert!(VersionSource::parse("0.56.0.1").is_err());
        assert!(VersionSource::parse("0.x").is_err());
    }

    #[test]
    fn only_preshipped_and_exact_avoid_a_lookup() {
        assert!(!VersionSource::Preshipped.needs_lookup());
        assert!(!VersionSource::Exact("0.56.0".into()).needs_lookup());
        assert!(VersionSource::Latest.needs_lookup());
        assert!(VersionSource::Stable.needs_lookup());
        assert!(VersionSource::MinorLine("0.56".into()).needs_lookup());
    }

    #[test]
    fn version_parse_rejects_prereleases() {
        assert_eq!(Version::parse("v0.56.0"), Version::parse("0.56.0"));
        assert!(Version::parse("0.57.0-rc1").is_none());
        assert!(Version::parse("0.57").is_none());
        assert!(Version::parse("vdev-v0.3.9").is_none());
    }

    #[test]
    fn stable_picks_the_previous_minors_newest_patch() {
        let r = vs(&["0.57.0", "0.56.0", "0.56.3", "0.55.1"]);
        assert_eq!(select_stable(&r).unwrap().to_string(), "0.56.3");
        assert_eq!(select_latest(&r).unwrap().to_string(), "0.57.0");
    }

    #[test]
    fn stable_on_the_real_release_list_today() {
        // Only 0.56.0 exists on that line, so stable is 0.56.0 -- the version
        // this image now pre-ships.
        let r = vs(&["0.57.0", "0.56.0", "0.55.0", "0.54.0"]);
        assert_eq!(select_stable(&r).unwrap().to_string(), "0.56.0");
    }

    #[test]
    fn a_new_major_is_not_stable_until_it_has_a_dot_one() {
        // 1.0.z published, no 1.1.0 yet -> stay on the 0.x line.
        let r = vs(&["1.0.4", "1.0.0", "0.57.0", "0.56.2"]);
        assert_eq!(select_stable(&r).unwrap().to_string(), "0.56.2");

        // 1.1.0 lands -> major 1 becomes eligible, previous minor is 1.0.
        let r = vs(&["1.1.0", "1.0.4", "1.0.0", "0.57.0", "0.56.2"]);
        assert_eq!(select_stable(&r).unwrap().to_string(), "1.0.4");
    }

    #[test]
    fn stable_falls_back_to_latest_when_there_is_no_previous_minor() {
        let r = vs(&["0.57.0", "0.57.1"]);
        assert_eq!(select_stable(&r).unwrap().to_string(), "0.57.1");
    }

    #[test]
    fn minor_line_takes_the_newest_patch() {
        let r = vs(&["0.57.0", "0.56.0", "0.56.3", "0.56.1"]);
        assert_eq!(select_minor_line(&r, "0.56").unwrap().to_string(), "0.56.3");
        assert!(select_minor_line(&r, "0.99").is_none());
    }

    #[test]
    fn an_unreachable_index_degrades_to_preshipped() {
        // The primary case: offline, or GitHub down. Must not error.
        for src in [
            VersionSource::Latest,
            VersionSource::Stable,
            VersionSource::MinorLine("0.56".into()),
        ] {
            let got = resolve(&src, "0.56.0", None).expect("must degrade, not fail");
            assert_eq!(
                got, "0.56.0",
                "{src:?} should fall back to the pre-shipped version"
            );
        }
    }

    #[test]
    fn preshipped_and_exact_never_consult_the_index() {
        assert_eq!(
            resolve(&VersionSource::Preshipped, "0.56.0", None).unwrap(),
            "0.56.0"
        );
        assert_eq!(
            resolve(&VersionSource::Exact("0.48.0".into()), "0.56.0", None).unwrap(),
            "0.48.0"
        );
    }

    #[test]
    fn a_request_that_cannot_be_met_by_a_fetched_index_is_an_error() {
        // Distinct from the unreachable case: we DID look, and the answer is
        // that the operator asked for something that does not exist.
        let r = vs(&["0.57.0", "0.56.0"]);
        assert!(resolve(&VersionSource::MinorLine("9.9".into()), "0.56.0", Some(&r)).is_err());
    }

    #[test]
    fn cache_key_includes_the_target_triple() {
        let dir = Path::new("/var/cache/vector");
        let arm = cache_path(dir, "0.56.0", "aarch64-unknown-linux-gnu");
        let x86 = cache_path(dir, "0.56.0", "x86_64-unknown-linux-gnu");
        assert_ne!(
            arm, x86,
            "same version on different arches must not collide"
        );
        assert!(arm.to_string_lossy().contains("0.56.0"));
    }

    #[test]
    fn seeding_is_idempotent_and_marks_the_binary_executable() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cache = tmp.path().join("cache");
        let shipped = tmp.path().join("vector");
        std::fs::write(&shipped, b"#!/bin/sh\ntrue\n").expect("write stub");

        seed_cache(&cache, &shipped, "0.56.0", "test-triple");
        let dest = cache_path(&cache, "0.56.0", "test-triple");
        assert!(dest.exists(), "first seed should populate the cache");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&dest).unwrap().permissions().mode();
            assert!(mode & 0o111 != 0, "cached binary must be executable");
        }

        // Re-seeding must not fail or duplicate.
        seed_cache(&cache, &shipped, "0.56.0", "test-triple");
        assert!(dest.exists());
    }

    #[test]
    fn seeding_without_a_preshipped_binary_is_not_fatal() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cache = tmp.path().join("cache");
        seed_cache(&cache, &tmp.path().join("absent"), "0.56.0", "test-triple");
        assert!(!cache_path(&cache, "0.56.0", "test-triple").exists());
    }
}
