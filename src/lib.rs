// Project:   dfe-transform-vector
// File:      src/lib.rs
// Purpose:   Library root — module declarations and public API
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! dfe-transform-vector: Rust wrapper for Vector.dev subprocess
//!
//! Manages Vector.dev as a child process for Kafka-to-Kafka transform pipelines,
//! making Vector a first-class DFE platform citizen.

pub mod config;
pub mod error;
pub mod health;
pub mod metrics;
pub mod vector;

pub use error::{Error, Result};
