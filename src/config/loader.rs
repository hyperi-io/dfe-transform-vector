// Project:   dfe-transform-vector
// File:      src/config/loader.rs
// Purpose:   Configuration structures and loading
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Configuration structures and loading.
//!
//! Scaffold-stage config with pipeline identity and logging.
//! Full sub-configs (source, sink, transforms, vector, health, metrics, scaling)
//! are added in TODO 2.1.

use std::path::Path;

use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::Result;

/// Main configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Pipeline identity.
    pub pipeline: PipelineConfig,
    /// Logging configuration.
    pub logging: LoggingConfig,
}

/// Pipeline identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PipelineConfig {
    /// Pipeline name (used in metrics labels and logging).
    pub name: String,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
        }
    }
}

/// Logging configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    /// Log level (trace, debug, info, warn, error).
    pub level: String,
    /// Log format (json, text).
    pub format: String,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            format: "json".to_string(),
        }
    }
}

impl Config {
    /// Load configuration from optional file path, with env var overrides.
    pub fn load(config_path: Option<&str>) -> Result<Self> {
        // Load .env file if present
        let _ = dotenvy::dotenv();

        // Start with defaults
        let mut config = Config::default();

        // Load YAML config file if provided
        if let Some(path) = config_path {
            if Path::new(path).exists() {
                let content = std::fs::read_to_string(path)
                    .map_err(|e| crate::Error::Config(format!("failed to read {path}: {e}")))?;
                config = serde_yaml_ng::from_str(&content)?;
                debug!(path, "loaded configuration file");
            }
        } else {
            // Try default config paths
            for path in &["config.yaml", "config.yml"] {
                if Path::new(path).exists() {
                    let content = std::fs::read_to_string(path)
                        .map_err(|e| crate::Error::Config(format!("failed to read {path}: {e}")))?;
                    config = serde_yaml_ng::from_str(&content)?;
                    debug!(path, "loaded configuration file");
                    break;
                }
            }
        }

        Ok(config)
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<()> {
        if self.pipeline.name.is_empty() {
            return Err(crate::Error::Validation(
                "pipeline.name must not be empty".into(),
            ));
        }
        Ok(())
    }
}
