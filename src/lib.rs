// Project:   dfe-transform-vector
// File:      src/lib.rs
// Purpose:   Library root — module declarations and public API
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! dfe-transform-vector: Rust wrapper for Vector.dev subprocess
//!
//! Manages Vector.dev as a child process for Kafka-to-Kafka transform pipelines,
//! making Vector a first-class DFE platform citizen.

// Lints are configured in Cargo.toml [lints] section.
#![allow(clippy::doc_markdown)]
#![cfg_attr(test, allow(clippy::unwrap_used))]
#![cfg_attr(test, allow(clippy::expect_used))]

pub mod config;
pub mod deployment;
pub mod error;
pub mod health;
pub mod metrics;
pub mod vector;

pub use error::{Error, Result};
