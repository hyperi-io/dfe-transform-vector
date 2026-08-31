// Project:   dfe-transform-vector
// File:      tests/common/metrics_fixture.rs
// Purpose:   The one MetricsManager a test process is allowed to build
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! One `MetricsManager` per test process.
//!
//! `MetricsManager::new` builds a fresh Prometheus recorder and installs it
//! with `metrics::set_global_recorder`, which succeeds at most ONCE per
//! process — scalo logs a warning and keeps the existing recorder on every
//! later call. The losing manager still hands back a `PrometheusHandle`, but
//! that handle covers a registry no `metrics::counter!`/`gauge!` ever writes
//! to, so its `render()` comes back an empty string and every metric
//! assertion made through it fails.
//!
//! Production has exactly one manager — the one `scalo::cli::ServiceRuntime`
//! owns and `WrapperMetrics::register` borrows — so one manager per process
//! is also what these tests are supposed to be modelling.
//!
//! `cargo nextest` hides the problem by giving every test its own process.
//! Plain `cargo test` shares one process per test binary and does not, so a
//! per-test manager renders empty for all but whichever test happened to
//! construct its manager first.
//!
//! Sharing the registry is safe for the assertions here: the counters are
//! monotonic and asserted with `>=`, the presence checks only ever gain
//! series, and every test that touches the lifecycle gauge sets it to
//! `Running`, so no two writers disagree on a value.

use std::sync::OnceLock;

use scalo::metrics::MetricsManager;

/// The process-wide `MetricsManager` every test must register and render
/// through.
///
/// The namespace is `dfe` — the platform namespace, matching production —
/// so bare metric names render as `dfe_<name>` and the wrapper's own
/// app-segment names render as `dfe_transform_vector_<name>`.
pub fn metrics_manager() -> &'static MetricsManager {
    static MANAGER: OnceLock<MetricsManager> = OnceLock::new();
    MANAGER.get_or_init(|| MetricsManager::new("dfe"))
}
