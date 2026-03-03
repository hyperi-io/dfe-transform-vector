// Project:   dfe-transform-vector
// File:      src/config/mod.rs
// Purpose:   Configuration module — loading, generation, and assembly
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Configuration loading, YAML generation, and assembly.
//!
//! Configuration cascade (highest to lowest priority):
//!   1. CLI args (--config, --log-level, etc.)
//!   2. Environment variables (`DFE_TRANSFORM_*`)
//!   3. `.env` file (via dotenvy)
//!   4. Config file specified by `--config`
//!   5. Hard-coded defaults

pub mod assembler;
pub mod generate;
pub mod loader;
pub mod transforms;
pub mod validate;
pub mod wiring;

pub use loader::{
    Config, DecodingConfig, HealthConfig, LoggingConfig, MetricsConfig, PipelineConfig, SaslConfig,
    ScalingConfig, SinkConfig, SourceConfig, TlsConfig, TransformConfig, VectorConfig,
};
