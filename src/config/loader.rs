// Project:   dfe-transform-vector
// File:      src/config/loader.rs
// Purpose:   Configuration structures and loading
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Configuration structures and loading.
//!
//! Big-dial config schema for Kafka source/sink, user-supplied transforms,
//! Vector subprocess, health/metrics endpoints, and scaling pressure.

use std::path::Path;

use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::Result;

// =============================================================================
// Shared sub-configs (used by both source and sink)
// =============================================================================

/// SASL authentication for Kafka.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SaslConfig {
    /// Enable SASL authentication.
    pub enabled: bool,
    /// SASL mechanism: plain, scram_sha_256, scram_sha_512.
    pub mechanism: String,
    /// SASL username. Supports Vector env interpolation: `${KAFKA_SASL_USERNAME}`.
    pub username: String,
    /// SASL password. Supports Vector env interpolation: `${KAFKA_SASL_PASSWORD}`.
    pub password: String,
}

impl Default for SaslConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mechanism: "scram_sha_512".to_string(),
            username: String::new(),
            password: String::new(),
        }
    }
}

/// TLS configuration for Kafka.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TlsConfig {
    /// Enable TLS.
    pub enabled: bool,
    /// Path to CA certificate PEM file.
    pub ca_cert_file: Option<String>,
    /// Path to client certificate PEM file (for mTLS).
    pub cert_file: Option<String>,
    /// Path to client key PEM file (for mTLS).
    pub key_file: Option<String>,
    /// Skip hostname verification (dev/test only).
    pub skip_verify: bool,
}

/// Message decoding configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DecodingConfig {
    /// Codec: json, raw_bytes, protobuf.
    pub codec: String,
}

impl Default for DecodingConfig {
    fn default() -> Self {
        Self {
            codec: "json".to_string(),
        }
    }
}

// =============================================================================
// Top-level config and sub-sections
// =============================================================================

/// Main configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Pipeline identity.
    pub pipeline: PipelineConfig,
    /// Kafka source (input).
    pub source: SourceConfig,
    /// Kafka sink (output).
    pub sink: SinkConfig,
    /// User-supplied transform YAML files.
    pub transforms: TransformConfig,
    /// Vector subprocess settings.
    pub vector: VectorConfig,
    /// Health endpoint.
    pub health: HealthConfig,
    /// Metrics endpoint.
    pub metrics: MetricsConfig,
    /// Logging.
    pub logging: LoggingConfig,
    /// KEDA scaling pressure.
    pub scaling: ScalingConfig,
}

/// Pipeline identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PipelineConfig {
    /// Pipeline name (used in metrics labels, Kafka group_id, logging).
    pub name: String,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
        }
    }
}

/// Kafka source configuration (input big dials).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SourceConfig {
    /// Kafka bootstrap servers.
    pub brokers: Vec<String>,
    /// Topics to consume from.
    pub topics: Vec<String>,
    /// Consumer group ID.
    pub group_id: String,
    /// Message decoding.
    pub decoding: DecodingConfig,
    /// SASL authentication.
    pub sasl: SaslConfig,
    /// TLS configuration.
    pub tls: TlsConfig,
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self {
            brokers: vec!["localhost:9092".to_string()],
            topics: vec!["events".to_string()],
            group_id: "dfe-transform-vector".to_string(),
            decoding: DecodingConfig::default(),
            sasl: SaslConfig::default(),
            tls: TlsConfig::default(),
        }
    }
}

/// Kafka sink configuration (output big dials).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SinkConfig {
    /// Kafka bootstrap servers.
    pub brokers: Vec<String>,
    /// Output topic name.
    pub topic: String,
    /// Event field path for Kafka partition key (e.g., ".org_id").
    pub key_field: String,
    /// Encoding codec: json, raw_bytes.
    pub encoding: String,
    /// Compression: none, gzip, lz4, snappy, zstd.
    pub compression: String,
    /// SASL authentication.
    pub sasl: SaslConfig,
    /// TLS configuration.
    pub tls: TlsConfig,
}

impl Default for SinkConfig {
    fn default() -> Self {
        Self {
            brokers: vec!["localhost:9092".to_string()],
            topic: String::new(),
            key_field: String::new(),
            encoding: "json".to_string(),
            compression: "none".to_string(),
            sasl: SaslConfig::default(),
            tls: TlsConfig::default(),
        }
    }
}

/// Transform file loading configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TransformConfig {
    /// Directory to load all YAML transform files from.
    pub dir: Option<String>,
    /// Explicit list of transform YAML file paths (loaded in order).
    pub files: Option<Vec<String>>,
}

/// Vector subprocess configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct VectorConfig {
    /// Path to Vector binary.
    pub binary: String,
    /// Data directory for disk buffers and state.
    pub data_dir: String,
    /// Vector API server address (host:port).
    pub api_address: String,
    /// Vector log level.
    pub log_level: String,
    /// Expected Vector version (semver).
    pub version: String,
    /// Version check mode: strict, warn, disabled.
    pub version_check: String,
}

impl Default for VectorConfig {
    fn default() -> Self {
        Self {
            binary: "/usr/local/bin/vector".to_string(),
            data_dir: "/var/lib/vector".to_string(),
            api_address: "0.0.0.0:8686".to_string(),
            log_level: "info".to_string(),
            version: String::new(),
            version_check: "strict".to_string(),
        }
    }
}

/// Health endpoint configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HealthConfig {
    /// Health server bind address (host:port).
    pub address: String,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            address: "0.0.0.0:9000".to_string(),
        }
    }
}

/// Metrics endpoint configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    /// Metrics server bind address (host:port).
    pub address: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            address: "0.0.0.0:9090".to_string(),
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

/// KEDA scaling pressure configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ScalingConfig {
    /// Scaling pressure threshold (0.0–1.0).
    pub pressure_threshold: f64,
}

impl Default for ScalingConfig {
    fn default() -> Self {
        Self {
            pressure_threshold: 0.8,
        }
    }
}

// =============================================================================
// Config loading and validation
// =============================================================================

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
        // Pipeline
        if self.pipeline.name.is_empty() {
            return Err(crate::Error::Validation(
                "pipeline.name must not be empty".into(),
            ));
        }

        // Source
        if self.source.brokers.is_empty() {
            return Err(crate::Error::Validation(
                "source.brokers must have at least one broker".into(),
            ));
        }
        if self.source.topics.is_empty() {
            return Err(crate::Error::Validation(
                "source.topics must have at least one topic".into(),
            ));
        }
        if self.source.group_id.is_empty() {
            return Err(crate::Error::Validation(
                "source.group_id must not be empty".into(),
            ));
        }
        self.validate_sasl("source.sasl", &self.source.sasl)?;

        // Sink
        if self.sink.brokers.is_empty() {
            return Err(crate::Error::Validation(
                "sink.brokers must have at least one broker".into(),
            ));
        }
        if self.sink.topic.is_empty() {
            return Err(crate::Error::Validation(
                "sink.topic must not be empty".into(),
            ));
        }
        self.validate_sasl("sink.sasl", &self.sink.sasl)?;

        // Vector version check mode
        let valid_modes = ["strict", "warn", "disabled"];
        if !valid_modes.contains(&self.vector.version_check.as_str()) {
            return Err(crate::Error::Validation(format!(
                "vector.version_check must be one of: {}",
                valid_modes.join(", ")
            )));
        }

        Ok(())
    }

    fn validate_sasl(&self, prefix: &str, sasl: &SaslConfig) -> Result<()> {
        if !sasl.enabled {
            return Ok(());
        }
        let valid_mechanisms = ["plain", "scram_sha_256", "scram_sha_512"];
        if !valid_mechanisms.contains(&sasl.mechanism.as_str()) {
            return Err(crate::Error::Validation(format!(
                "{prefix}.mechanism must be one of: {}",
                valid_mechanisms.join(", ")
            )));
        }
        Ok(())
    }
}
