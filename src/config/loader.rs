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

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::Result;

/// SASL authentication for Kafka.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

/// Main configuration.
///
/// ## Hot-reload behaviour
///
/// **Hot-reloaded (takes effect on next poll cycle / SIGHUP):**
/// - Transform YAML file contents (modified/added/removed in watched directory)
/// - `transforms.dir` — watcher switches to new directory after successful reload
/// - `transforms.files` — watcher switches to new file list after successful reload
///
/// **Requires pod restart:**
/// - `source.*` — Kafka consumer connections established at Vector startup
/// - `sink.*` — Kafka producer connections established at Vector startup
/// - `pipeline.name` — used in consumer group_id and metrics labels at startup
/// - `vector.*` — binary path, data_dir, API address, log level set at Vector spawn
/// - `health.address` — HTTP server binds at startup
/// - `metrics.*` — HTTP server binds at startup
/// - `logging.*` — tracing subscriber configured at startup
/// - `scaling.*` — not consumed at runtime
/// - `reload.poll_interval_secs` — captured at reload loop start
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Pipeline identity. **Requires restart.**
    pub pipeline: PipelineConfig,
    /// Kafka source (input). **Requires restart.**
    pub source: SourceConfig,
    /// Kafka sink (output). **Requires restart.**
    pub sink: SinkConfig,
    /// User-supplied transform YAML files. **Hot-reloaded.**
    pub transforms: TransformConfig,
    /// Vector subprocess settings. **Requires restart.**
    pub vector: VectorConfig,
    /// Health endpoint. **Requires restart** (server binds at startup).
    pub health: HealthConfig,
    /// Metrics endpoint. **Requires restart** (server binds at startup).
    pub metrics: MetricsConfig,
    /// Logging. **Requires restart** (subscriber configured at startup).
    pub logging: LoggingConfig,
    /// KEDA scaling pressure. **Requires restart.**
    pub scaling: ScalingConfig,
    /// Hot-reload configuration. **Requires restart** (poll interval captured at start).
    pub reload: ReloadConfig,
}

/// Pipeline identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    /// Auto offset reset: `largest` (default) or `smallest`.
    pub auto_offset_reset: String,
    /// Session timeout (ms). Default: 30000.
    pub session_timeout_ms: u32,
    /// Offset commit interval (ms). Default: 5000.
    pub commit_interval_ms: u32,
    /// Drain timeout (ms) — max wait for pending acks during shutdown/rebalance.
    /// Must be less than session_timeout_ms. Default: half of session_timeout_ms.
    pub drain_timeout_ms: Option<u32>,
    /// Expose per-topic consumer lag metric. Default: true.
    pub topic_lag_metric: bool,
    /// Extra librdkafka options (passed through to Vector).
    pub librdkafka_options: BTreeMap<String, String>,
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
            auto_offset_reset: "largest".to_string(),
            session_timeout_ms: 30000,
            commit_interval_ms: 5000,
            drain_timeout_ms: None,
            topic_lag_metric: true,
            librdkafka_options: BTreeMap::new(),
        }
    }
}

/// Kafka sink configuration (output big dials).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    /// Sink buffer configuration.
    pub buffer: BufferConfig,
    /// Sink batch configuration (Vector-level batching).
    pub batch: BatchConfig,
    /// Local message timeout (ms). Default: 300000 (5 min).
    pub message_timeout_ms: u32,
    /// Network request timeout (ms). Default: 60000 (60s).
    pub socket_timeout_ms: u32,
    /// Extra librdkafka options (passed through to Vector).
    pub librdkafka_options: BTreeMap<String, String>,
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
            buffer: BufferConfig::default(),
            batch: BatchConfig::default(),
            message_timeout_ms: 300_000,
            socket_timeout_ms: 60_000,
            librdkafka_options: BTreeMap::new(),
        }
    }
}

/// Vector-level batch configuration for sinks.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct BatchConfig {
    /// Maximum events per batch before flush. Default: 10000.
    pub max_events: u32,
    /// Maximum uncompressed batch size (bytes) before flush.
    pub max_bytes: Option<u64>,
    /// Max batch age (seconds) before flush. Default: 1.
    pub timeout_secs: u32,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_events: 10_000,
            max_bytes: None,
            timeout_secs: 1,
        }
    }
}

/// Buffer configuration for Vector sinks.
///
/// Supports two modes:
/// - `memory`: in-memory buffer (default), configured by `max_events`
/// - `disk`: persistent disk buffer, configured by `max_size` (bytes, min ~256 MiB)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct BufferConfig {
    /// Buffer type: `memory` or `disk`.
    #[serde(rename = "type")]
    pub buffer_type: String,
    /// Maximum events in memory buffer (only for type=memory). Default: 500.
    pub max_events: Option<u64>,
    /// Maximum buffer size in bytes (only for type=disk). Min ~256 MiB (268435488).
    pub max_size: Option<u64>,
    /// Behaviour when buffer is full: `block` (default) or `drop_newest`.
    pub when_full: String,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            buffer_type: "memory".to_string(),
            max_events: None,
            max_size: None,
            when_full: "block".to_string(),
        }
    }
}

/// Transform file loading configuration.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct TransformConfig {
    /// Directory to load all YAML transform files from.
    pub dir: Option<String>,
    /// Explicit list of transform YAML file paths (loaded in order).
    pub files: Option<Vec<String>>,
}

/// Vector subprocess configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    /// Metrics server bind address (host:port).
    pub address: String,
    /// Vector's prometheus_exporter address to proxy (host:port).
    pub vector_metrics_address: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            address: "0.0.0.0:9090".to_string(),
            vector_metrics_address: "127.0.0.1:9598".to_string(),
        }
    }
}

/// Logging configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

/// Hot-reload configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ReloadConfig {
    /// Enable hot-reload (poll-based file watcher + SIGHUP).
    pub enabled: bool,
    /// Poll interval in seconds for detecting config/transform file changes.
    pub poll_interval_secs: u64,
}

impl Default for ReloadConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            poll_interval_secs: 30,
        }
    }
}

/// KEDA scaling pressure configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

/// Environment variable prefix for all config overrides.
const ENV_PREFIX: &str = "DFE_TRANSFORM";

/// Read a single env var with our prefix.
fn env_var(name: &str) -> Option<String> {
    std::env::var(format!("{ENV_PREFIX}_{name}")).ok()
}

/// Read a comma-separated env var as a list.
fn env_var_list(name: &str) -> Option<Vec<String>> {
    env_var(name).map(|v| v.split(',').map(|s| s.trim().to_string()).collect())
}

/// Read an env var parsed to a specific type.
fn env_var_parsed<T: std::str::FromStr>(name: &str) -> Option<T> {
    env_var(name).and_then(|v| v.parse().ok())
}

/// Apply figment env var cascade (DFE_TRANSFORM_SECTION__FIELD with __ nesting).
fn apply_figment_env(config: &mut Config) -> Result<()> {
    use figment::Figment;
    use figment::providers::{Env, Serialized};

    let figment = Figment::from(Serialized::defaults(&*config))
        .merge(Env::prefixed(&format!("{ENV_PREFIX}_")).split("__"));

    *config = figment
        .extract()
        .map_err(|e| crate::Error::Config(e.to_string()))?;
    Ok(())
}

/// Apply explicit flat env var overrides (highest priority after CLI args).
///
/// These K8s-friendly overrides use single underscores and are explicit
/// per-field rather than relying on figment's automatic nesting.
fn apply_env_overrides(config: &mut Config) {
    // Pipeline
    if let Some(v) = env_var("PIPELINE_NAME") {
        config.pipeline.name = v;
        debug!("override: pipeline.name from env");
    }

    // Source
    if let Some(v) = env_var_list("SOURCE_BROKERS") {
        config.source.brokers = v;
        debug!("override: source.brokers from env");
    }
    if let Some(v) = env_var_list("SOURCE_TOPICS") {
        config.source.topics = v;
        debug!("override: source.topics from env");
    }
    if let Some(v) = env_var("SOURCE_GROUP_ID") {
        config.source.group_id = v;
        debug!("override: source.group_id from env");
    }
    if let Some(v) = env_var("SOURCE_SASL_USERNAME") {
        config.source.sasl.enabled = true;
        config.source.sasl.username = v;
        debug!("override: source.sasl.username from env");
    }
    if let Some(v) = env_var("SOURCE_SASL_PASSWORD") {
        config.source.sasl.enabled = true;
        config.source.sasl.password = v;
        debug!("override: source.sasl.password from env");
    }
    if let Some(v) = env_var("SOURCE_SASL_MECHANISM") {
        config.source.sasl.mechanism = v;
        debug!("override: source.sasl.mechanism from env");
    }

    // Sink
    if let Some(v) = env_var_list("SINK_BROKERS") {
        config.sink.brokers = v;
        debug!("override: sink.brokers from env");
    }
    if let Some(v) = env_var("SINK_TOPIC") {
        config.sink.topic = v;
        debug!("override: sink.topic from env");
    }
    if let Some(v) = env_var("SINK_KEY_FIELD") {
        config.sink.key_field = v;
        debug!("override: sink.key_field from env");
    }
    if let Some(v) = env_var("SINK_ENCODING") {
        config.sink.encoding = v;
        debug!("override: sink.encoding from env");
    }
    if let Some(v) = env_var("SINK_COMPRESSION") {
        config.sink.compression = v;
        debug!("override: sink.compression from env");
    }
    if let Some(v) = env_var("SINK_SASL_USERNAME") {
        config.sink.sasl.enabled = true;
        config.sink.sasl.username = v;
        debug!("override: sink.sasl.username from env");
    }
    if let Some(v) = env_var("SINK_SASL_PASSWORD") {
        config.sink.sasl.enabled = true;
        config.sink.sasl.password = v;
        debug!("override: sink.sasl.password from env");
    }
    if let Some(v) = env_var("SINK_SASL_MECHANISM") {
        config.sink.sasl.mechanism = v;
        debug!("override: sink.sasl.mechanism from env");
    }

    // Transforms
    if let Some(v) = env_var("TRANSFORMS_DIR") {
        config.transforms.dir = Some(v);
        debug!("override: transforms.dir from env");
    }

    // Vector
    if let Some(v) = env_var("VECTOR_BINARY") {
        config.vector.binary = v;
        debug!("override: vector.binary from env");
    }
    if let Some(v) = env_var("VECTOR_DATA_DIR") {
        config.vector.data_dir = v;
        debug!("override: vector.data_dir from env");
    }
    if let Some(v) = env_var("VECTOR_LOG_LEVEL") {
        config.vector.log_level = v;
        debug!("override: vector.log_level from env");
    }
    if let Some(v) = env_var("VECTOR_VERSION") {
        config.vector.version = v;
        debug!("override: vector.version from env");
    }
    if let Some(v) = env_var("VECTOR_VERSION_CHECK") {
        config.vector.version_check = v;
        debug!("override: vector.version_check from env");
    }

    // Health
    if let Some(v) = env_var("HEALTH_ADDRESS") {
        config.health.address = v;
        debug!("override: health.address from env");
    }

    // Metrics
    if let Some(v) = env_var("METRICS_ADDRESS") {
        config.metrics.address = v;
        debug!("override: metrics.address from env");
    }

    // Scaling
    if let Some(v) = env_var_parsed::<f64>("SCALING_PRESSURE_THRESHOLD") {
        config.scaling.pressure_threshold = v;
        debug!("override: scaling.pressure_threshold from env");
    }
}

impl Config {
    /// Load configuration with full cascade.
    ///
    /// Priority (highest to lowest):
    ///   1. CLI args (handled by caller)
    ///   2. Flat env overrides (`DFE_TRANSFORM_SOURCE_BROKERS`, etc.)
    ///   3. Figment env vars with `__` nesting (`DFE_TRANSFORM_SOURCE__BROKERS`)
    ///   4. `.env` file (via dotenvy)
    ///   5. Config YAML file
    ///   6. Hard-coded defaults
    pub fn load(config_path: Option<&str>) -> Result<Self> {
        // Load .env file if present (before any env var reading)
        let _ = dotenvy::dotenv();

        // Start with defaults
        let mut config = Config::default();

        // Load YAML config file (overrides defaults)
        if let Some(path) = config_path {
            if Path::new(path).exists() {
                let content = std::fs::read_to_string(path)
                    .map_err(|e| crate::Error::Config(format!("failed to read {path}: {e}")))?;
                config = serde_yaml_ng::from_str(&content)?;
                debug!(path, "loaded configuration file");
            }
        } else {
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

        // Apply figment env vars (DFE_TRANSFORM_SECTION__FIELD)
        apply_figment_env(&mut config)?;

        // Apply explicit flat env overrides (highest priority)
        apply_env_overrides(&mut config);

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
        self.validate_buffer(&self.sink.buffer)?;

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

    fn validate_buffer(&self, buffer: &BufferConfig) -> Result<()> {
        let valid_types = ["memory", "disk"];
        if !valid_types.contains(&buffer.buffer_type.as_str()) {
            return Err(crate::Error::Validation(format!(
                "sink.buffer.type must be one of: {}",
                valid_types.join(", ")
            )));
        }

        let valid_when_full = ["block", "drop_newest"];
        if !valid_when_full.contains(&buffer.when_full.as_str()) {
            return Err(crate::Error::Validation(format!(
                "sink.buffer.when_full must be one of: {}",
                valid_when_full.join(", ")
            )));
        }

        if buffer.buffer_type == "disk" {
            let Some(max_size) = buffer.max_size else {
                return Err(crate::Error::Validation(
                    "sink.buffer.max_size is required when buffer type is disk".into(),
                ));
            };
            if max_size < 268_435_488 {
                return Err(crate::Error::Validation(format!(
                    "sink.buffer.max_size must be at least 268435488 (256 MiB), got {max_size}"
                )));
            }
        }

        Ok(())
    }
}
