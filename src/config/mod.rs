// Project:   dfe-transform-vector
// File:      src/config/mod.rs
// Purpose:   Configuration module — loading, generation, and assembly
// Language:  Rust
//
// License:   BUSL-1.1
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
pub mod kafka_defaults;
pub mod loader;
pub mod reload;
pub mod secrets;
pub mod transforms;
pub mod validate;
pub mod wiring;

pub use loader::{
    BatchConfig, BridgeConfig, BufferConfig, Config, DecodingConfig, LoggingConfig, MetricsConfig,
    PipelineConfig, ReloadConfig, SaslConfig, ScalingConfig, SinkConfig, SourceConfig, TlsConfig,
    TransformConfig, Transport, VectorConfig,
};
