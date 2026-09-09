// Project:   dfe-transform-vector
// File:      src/lib.rs
// Purpose:   Library root — module declarations and public API
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! dfe-transform-vector: Rust wrapper for Vector.dev subprocess
//!
//! Manages Vector.dev as a child process, so a Vector pipeline is a DFE
//! transform stage: it assembles Vector's config, supervises the process, and
//! reports health, metrics and scaling pressure the way every other DFE app
//! does. Records reach Vector over Kafka on the bus transport, or through the
//! [`bridge`] on the direct one.

// Lints are configured in Cargo.toml [lints] section.
#![allow(clippy::doc_markdown)]
#![cfg_attr(test, allow(clippy::unwrap_used))]
#![cfg_attr(test, allow(clippy::expect_used))]

pub mod bridge;
pub mod config;
pub mod deployment;
pub mod error;
pub mod health;
pub mod metrics;
pub mod vector;

pub use error::{Error, Result};
