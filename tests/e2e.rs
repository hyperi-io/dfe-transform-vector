#![allow(clippy::unwrap_used, clippy::expect_used)]
// Project:   dfe-transform-vector
// File:      tests/e2e.rs
// Purpose:   Single-binary e2e test entry point
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! End-to-end tests — single binary with submodules.

mod common;

#[path = "e2e/mod.rs"]
mod e2e;
