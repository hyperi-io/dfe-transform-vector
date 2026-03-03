// Project:   dfe-transform-vector
// File:      src/config/mod.rs
// Purpose:   Configuration module — loading and validation
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Configuration loading and validation.
//!
//! Configuration cascade (highest to lowest priority):
//!   1. CLI args (--config, --log-level, etc.)
//!   2. Environment variables (`DFE_TRANSFORM_*`)
//!   3. `.env` file (via dotenvy)
//!   4. Config file specified by `--config`
//!   5. Hard-coded defaults

pub mod loader;

pub use loader::Config;
