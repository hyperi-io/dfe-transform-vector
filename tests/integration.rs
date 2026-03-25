#![allow(unsafe_code, clippy::unwrap_used, clippy::expect_used)]
// Project:   dfe-transform-vector
// File:      tests/integration.rs
// Purpose:   Single-binary integration test entry point
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Integration tests — single binary with submodules.
//!
//! Each top-level `tests/*.rs` file compiles as a separate binary (separate
//! link cycle). Consolidating all integration tests into one crate root with
//! submodules gives a 3x compile-time reduction. See Rust Standards, Testing.

mod common;

#[path = "integration/mod.rs"]
mod integration;
