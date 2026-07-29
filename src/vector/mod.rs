// Project:   dfe-transform-vector
// File:      src/vector/mod.rs
// Purpose:   Vector subprocess management
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Vector subprocess management.
//!
//! Handles spawning, monitoring, signal forwarding, and crash recovery
//! for the Vector child process.

pub mod binary;
pub mod lifecycle;
pub mod process;

pub use binary::{VersionSource, cache_path, resolve, seed_cache};
pub use lifecycle::{Lifecycle, State};
pub use process::{BackoffConfig, run_lifecycle, spawn_vector};
