// Project:   dfe-transform-vector
// File:      src/error.rs
// Purpose:   Error types for the application
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Error types for dfe-transform-vector.

use thiserror::Error;

/// Main error type.
#[derive(Error, Debug)]
pub enum Error {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Vector subprocess error: {0}")]
    Vector(String),

    #[error("YAML error: {0}")]
    Yaml(#[from] serde_yaml_ng::Error),

    #[error("validation error: {0}")]
    Validation(String),

    #[error("transport error: {0}")]
    Transport(String),

    #[error("shutdown requested")]
    Shutdown,
}

/// Result type alias using our Error.
pub type Result<T> = std::result::Result<T, Error>;
