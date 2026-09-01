// Project:   dfe-transform-vector
// File:      tests/integration/config_env.rs
// Purpose:   Serialise the process-global config environment across parallel tests
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Access control for the process-global environment `Config::load` reads.
//!
//! `Config::load` merges `DFE_TRANSFORM_*` env vars over the file it is given,
//! and the environment is per-PROCESS. `cargo test` runs the whole integration
//! binary's tests as threads in one process, so a test that sets
//! `DFE_TRANSFORM_PIPELINE_NAME` changes what every concurrently running
//! `Config::load` returns -- three unrelated tests fail with a pipeline name
//! they never asked for.
//!
//! An `RwLock` rather than a `Mutex` so the readers stay parallel: only the
//! handful of tests that MUTATE the environment need exclusive access.
//!
//! Load config through [`load_config`] rather than calling `Config::load`
//! directly, so the guard cannot be forgotten.

use std::sync::{RwLock, RwLockWriteGuard};

use dfe_transform_vector::Result;
use dfe_transform_vector::config::Config;

/// Shared/exclusive access to the `DFE_TRANSFORM_*` environment.
static CONFIG_ENV: RwLock<()> = RwLock::new(());

/// Load config while holding shared access to the environment.
///
/// Poison-tolerant: a panicking test leaves the lock poisoned, and failing
/// every later test on that is noise, not signal.
pub fn load_config(path: Option<&str>) -> Result<Config> {
    let _shared = CONFIG_ENV.read().unwrap_or_else(|e| e.into_inner());
    Config::load(path)
}

/// Take exclusive access before setting or removing a `DFE_TRANSFORM_*` var.
///
/// Hold the returned guard across the whole set -> load -> remove sequence.
pub fn env_write_guard() -> RwLockWriteGuard<'static, ()> {
    CONFIG_ENV.write().unwrap_or_else(|e| e.into_inner())
}
