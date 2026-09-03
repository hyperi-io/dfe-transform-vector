// Project:   dfe-transform-vector
// File:      src/config/loader.rs
// Purpose:   Configuration structures and loading
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Configuration structures and loading.
//!
//! Big-dial config schema for Kafka source/sink, user-supplied transforms,
//! Vector subprocess, health/metrics endpoints, and scaling pressure.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};
use tracing::debug;

use scalo::cli::CommonArgs;
use scalo::kafka_config::{KafkaSource, ServiceRole};

use crate::Result;

/// SASL authentication for Kafka.
///
/// NOTE: `password` is `String`, not `SensitiveString`, because the config
/// goes through a figment serialize→merge→deserialize round-trip in
/// `apply_figment_env()`. `SensitiveString` serialises as `***REDACTED***`
/// which destroys the value during the round-trip. Password protection is
/// handled by: (1) `flat_env_string_sensitive` which masks the env var in
/// logs, and (2) the generated Vector YAML using `${KAFKA_SASL_PASSWORD}`
/// env-var interpolation — the actual secret never appears in our config.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
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
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
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
/// - `transforms.dir` -- watcher switches to new directory after successful reload
/// - `transforms.files` -- watcher switches to new file list after successful reload
///
/// **Requires pod restart:**
/// - `source.*` -- Kafka consumer connections established at Vector startup
/// - `sink.*` -- Kafka producer connections established at Vector startup
/// - `pipeline.name` -- used in consumer group_id and metrics labels at startup
/// - `vector.*` -- binary path, data_dir, config_dir, API address, log level
///   set at Vector spawn
/// - `metrics.address` -- the HTTP server binds at startup
/// - `logging.*` -- tracing subscriber configured at startup
/// - `reload.poll_interval_secs` -- captured at reload loop start
///
/// **Read by nothing in this process:**
/// - `scaling.pressure_threshold` -- no reader. Scale-out is driven by the
///   chart's KEDA triggers (consumer-group lag and CPU), gated by the
///   subprocess circuit in [`crate::metrics::spawn_circuit_gate_task`].
///   scalo's own `ScalingEngine` reads a `scaling:` block from ITS cascade,
///   which this app does not initialise, so the engine keys
///   (`enabled`/`interval_secs`/`transport`/`params`) are inert here too.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct Config {
    /// DFE source name (e.g. `"syslog"`, `"netflow"`).
    ///
    /// When set, derives defaults for fields not explicitly configured:
    /// - `source.topics` → `["{dfe_source}_land"]`
    /// - `sink.topic` → `"{dfe_source}_load"`
    /// - `source.group_id` → `"dfe-transform-vector-{dfe_source}"`
    ///
    /// Explicit values always override the derived defaults.
    /// See [`KafkaSource`] for the platform topic naming convention.
    pub dfe_source: Option<String>,
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
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

impl PipelineConfig {
    /// Returns `Some(&name)` if the pipeline name was explicitly set
    /// (differs from the hard-coded `"default"`).
    fn name_if_not_default(&self) -> Option<&str> {
        if self.name == "default" {
            None
        } else {
            Some(&self.name)
        }
    }
}

/// Kafka source configuration (input big dials).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
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
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct TransformConfig {
    /// Directory to load all YAML transform files from.
    pub dir: Option<String>,
    /// Explicit list of transform YAML file paths (loaded in order).
    pub files: Option<Vec<String>>,
}

/// Vector subprocess configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct VectorConfig {
    /// Path to Vector binary.
    pub binary: String,
    /// Data directory for disk buffers and state.
    pub data_dir: String,
    /// Directory the assembled Vector config is written to.
    ///
    /// Defaults to the container path. A deployment that is not the image --
    /// a bare install, or a test driving the binary directly -- has no write
    /// access there and must point this somewhere it owns.
    pub config_dir: String,
    /// Vector API server address (host:port).
    pub api_address: String,
    /// Vector log level.
    pub log_level: String,
    /// Expected Vector version (semver).
    ///
    /// Defaults to the version the image was built with
    /// ([`crate::deployment::VECTOR_VERSION`]) rather than empty:
    /// [`check_vector_version`](crate::config::validate::check_vector_version)
    /// skips the comparison on an empty pin, which would make the default
    /// `version_check: strict` a check that cannot fire. With the default pin,
    /// strict means the binary on PATH must be the one this image shipped.
    ///
    /// Set it explicitly (or set `version_check` to `warn`/`disabled`) when
    /// deliberately running a different Vector to the pre-shipped one.
    pub version: String,
    /// Version check mode: strict, warn, disabled.
    pub version_check: String,
    /// Where the Vector binary comes from. `preshipped` is the only value the
    /// runtime can honour.
    ///
    /// This process never acquires a binary: it runs [`binary`](Self::binary)
    /// and nothing else. The selection rules in [`crate::vector::binary`]
    /// (`latest`, `stable`, a minor line, an exact version) have no caller on
    /// the run path, so any other value here would resolve to the pre-shipped
    /// binary anyway -- silently, and while reporting success. `validate()`
    /// rejects them rather than substituting.
    ///
    /// To run a different Vector, point [`binary`](Self::binary) at it and set
    /// [`version`](Self::version) to match.
    pub version_source: String,
}

impl Default for VectorConfig {
    fn default() -> Self {
        Self {
            binary: "/usr/local/bin/vector".to_string(),
            data_dir: "/var/lib/vector".to_string(),
            config_dir: super::assembler::DEFAULT_CONFIG_DIR.to_string(),
            api_address: "0.0.0.0:8686".to_string(),
            log_level: "info".to_string(),
            version: crate::deployment::VECTOR_VERSION.to_string(),
            version_check: "strict".to_string(),
            version_source: "preshipped".to_string(),
        }
    }
}

/// Metrics endpoint configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct MetricsConfig {
    /// Bind address for `/metrics`, `/livez` and `/readyz` (host:port).
    ///
    /// Reaches the listener through [`Config::fill_common_args`]; `--metrics-addr`
    /// and `METRICS_ADDR` both outrank it.
    pub address: String,
    /// Bind address for Vector's own `prometheus_exporter` sink (host:port).
    ///
    /// Written into the assembled `99_observability.yaml`, so Vector's internal
    /// metrics are exposed here. Prometheus scrapes it as a second target --
    /// the wrapper does not proxy it -- which is why it defaults to `0.0.0.0`
    /// rather than loopback.
    pub vector_metrics_address: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            address: "0.0.0.0:9090".to_string(),
            vector_metrics_address: "0.0.0.0:9598".to_string(),
        }
    }
}

/// Logging configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct LoggingConfig {
    /// Log level (trace, debug, info, warn, error).
    ///
    /// Reaches the subscriber through [`Config::fill_common_args`];
    /// `--log-level`, `LOG_LEVEL`, `--verbose` and `--quiet` all outrank it.
    pub level: String,
    /// Log format (json, text, auto).
    ///
    /// `auto` -- the default -- is text on a terminal and JSON everywhere else,
    /// so a pod ships structured logs and a developer reads them.
    pub format: String,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            format: "auto".to_string(),
        }
    }
}

/// Hot-reload configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
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
///
/// Present for parity with the `scaling:` block every DFE service accepts.
/// Nothing in this process reads it -- see the "read by nothing" note on
/// [`Config`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct ScalingConfig {
    /// Scaling pressure threshold (0.0–1.0). Range-checked, not consumed.
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

use scalo::config::flat_env::{self, ApplyFlatEnv, Normalize};

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

impl ApplyFlatEnv for Config {
    /// Apply explicit flat env var overrides (highest priority after CLI args).
    ///
    /// Env var names are the contract with dfe-engine — do not rename.
    /// Format: `DFE_TRANSFORM_<SUFFIX>` (prefix passed by caller).
    fn apply_flat_env(&mut self, prefix: &str) {
        // DFE source shorthand
        if let Some(v) = flat_env::flat_env_string(prefix, "DFE_SOURCE") {
            self.dfe_source = Some(v);
        }

        // Pipeline
        if let Some(v) = flat_env::flat_env_string(prefix, "PIPELINE_NAME") {
            self.pipeline.name = v;
        }

        // Source
        if let Some(v) = flat_env::flat_env_list(prefix, "SOURCE_BROKERS") {
            self.source.brokers = v;
        }
        if let Some(v) = flat_env::flat_env_list(prefix, "SOURCE_TOPICS") {
            self.source.topics = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "SOURCE_GROUP_ID") {
            self.source.group_id = v;
        }
        if let Some(v) = flat_env::flat_env_string_sensitive(prefix, "SOURCE_SASL_USERNAME") {
            self.source.sasl.username = v;
        }
        if let Some(v) = flat_env::flat_env_string_sensitive(prefix, "SOURCE_SASL_PASSWORD") {
            self.source.sasl.password = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "SOURCE_SASL_MECHANISM") {
            self.source.sasl.mechanism = v;
        }

        // Sink
        if let Some(v) = flat_env::flat_env_list(prefix, "SINK_BROKERS") {
            self.sink.brokers = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "SINK_TOPIC") {
            self.sink.topic = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "SINK_KEY_FIELD") {
            self.sink.key_field = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "SINK_ENCODING") {
            self.sink.encoding = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "SINK_COMPRESSION") {
            self.sink.compression = v;
        }
        if let Some(v) = flat_env::flat_env_string_sensitive(prefix, "SINK_SASL_USERNAME") {
            self.sink.sasl.username = v;
        }
        if let Some(v) = flat_env::flat_env_string_sensitive(prefix, "SINK_SASL_PASSWORD") {
            self.sink.sasl.password = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "SINK_SASL_MECHANISM") {
            self.sink.sasl.mechanism = v;
        }

        // Transforms
        if let Some(v) = flat_env::flat_env_string(prefix, "TRANSFORMS_DIR") {
            self.transforms.dir = Some(v);
        }

        // Vector
        if let Some(v) = flat_env::flat_env_string(prefix, "VECTOR_BINARY") {
            self.vector.binary = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "VECTOR_DATA_DIR") {
            self.vector.data_dir = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "VECTOR_CONFIG_DIR") {
            self.vector.config_dir = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "VECTOR_LOG_LEVEL") {
            self.vector.log_level = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "VECTOR_VERSION") {
            self.vector.version = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "VECTOR_VERSION_CHECK") {
            self.vector.version_check = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "VECTOR_VERSION_SOURCE") {
            self.vector.version_source = v;
        }

        // Metrics
        if let Some(v) = flat_env::flat_env_string(prefix, "METRICS_ADDRESS") {
            self.metrics.address = v;
        }

        // Scaling
        if let Some(v) = flat_env::flat_env_parsed::<f64>(prefix, "SCALING_PRESSURE_THRESHOLD") {
            self.scaling.pressure_threshold = v;
        }
    }
}

impl Normalize for Config {
    /// Apply side-effect normalisations after env overrides.
    ///
    /// Credentials present → enable SASL automatically.
    fn normalize(&mut self) {
        if !self.source.sasl.username.is_empty() || !self.source.sasl.password.is_empty() {
            self.source.sasl.enabled = true;
        }
        if !self.sink.sasl.username.is_empty() || !self.sink.sasl.password.is_empty() {
            self.sink.sasl.enabled = true;
        }
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

        // Load YAML config file (overrides defaults).
        //
        // Explicit `--config <path>` is STRICT: missing file is a hard
        // error, not a silent fallback. Otherwise a typo in the mount
        // path produces a downstream "sink.topic must not be empty"
        // validation error and the user has no clue why.
        //
        // Implicit `config.yaml` / `config.yml` search is LENIENT —
        // those names are optional by design.
        if let Some(path) = config_path {
            if !Path::new(path).exists() {
                return Err(crate::Error::Config(format!(
                    "config file not found: {path}"
                )));
            }
            let content = std::fs::read_to_string(path)
                .map_err(|e| crate::Error::Config(format!("failed to read {path}: {e}")))?;
            config = serde_yaml_ng::from_str(&content)?;
            debug!(path, "loaded configuration file");
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
        config.apply_flat_env(ENV_PREFIX);

        // Normalise: credentials present → enable SASL
        config.normalize();

        // Derive topic/CG defaults from dfe_source (if set)
        config.resolve_source_defaults()?;

        Ok(config)
    }

    /// Derive topic and consumer group defaults from `dfe_source`.
    ///
    /// Only fills fields that still hold their hard-coded defaults — explicit
    /// config always wins. Called after all env/YAML loading is complete.
    fn resolve_source_defaults(&mut self) -> Result<()> {
        let Some(ref source_name) = self.dfe_source else {
            return Ok(());
        };

        let dfe = KafkaSource::new(source_name);
        let defaults = SourceConfig::default();
        let sink_defaults = SinkConfig::default();

        // source.topics — only override if still the hard-coded default
        if self.source.topics == defaults.topics {
            self.source.topics = vec![dfe.input_topic()];
        }

        // sink.topic — only override if still empty (default)
        if self.sink.topic == sink_defaults.topic {
            self.sink.topic = dfe.output_topic();
        }

        // source.group_id — only override if still the hard-coded default
        if self.source.group_id == defaults.group_id {
            let cg = dfe
                .consumer_group(
                    "transform-vector",
                    ServiceRole::Transform,
                    self.pipeline.name_if_not_default(),
                    None,
                )
                .map_err(|e| crate::Error::Config(e.to_string()))?;
            self.source.group_id = cg;
        }

        Ok(())
    }

    /// Hand `metrics.address` and `logging.*` to the resolvers that read them.
    ///
    /// `ServiceRuntime` binds [`CommonArgs::effective_metrics_addr`] and the
    /// logger is built from `effective_log_level`/`effective_log_format`. Those
    /// fall through to scalo's OWN config cascade, which this app never
    /// initialises -- so a value set in this app's config file reaches nothing
    /// unless it is put where the resolvers look.
    ///
    /// Only fills a slot the CLI flag and its environment variable both left
    /// empty, so the documented precedence still holds: flag, then environment,
    /// then this config, then the hard-coded default. `--verbose`/`--quiet` are
    /// checked ahead of the level either way.
    pub fn fill_common_args(&self, args: &mut CommonArgs) {
        if args.metrics_addr.is_none() && !self.metrics.address.is_empty() {
            args.metrics_addr = Some(self.metrics.address.clone());
        }
        if args.log_level.is_none() && !self.logging.level.is_empty() {
            args.log_level = Some(self.logging.level.clone());
        }
        if args.log_format.is_none() && !self.logging.format.is_empty() {
            args.log_format = Some(self.logging.format.clone());
        }
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
        Self::validate_librdkafka_options(
            "source",
            &self.source.librdkafka_options,
            &super::generate::derived_source_options(&self.source),
        )?;

        // Source codec
        let valid_codecs = ["json", "raw_bytes", "protobuf"];
        if !valid_codecs.contains(&self.source.decoding.codec.as_str()) {
            return Err(crate::Error::Validation(format!(
                "source.decoding.codec must be one of: {}",
                valid_codecs.join(", ")
            )));
        }

        // Source auto_offset_reset
        let valid_offsets = ["largest", "smallest"];
        if !valid_offsets.contains(&self.source.auto_offset_reset.as_str()) {
            return Err(crate::Error::Validation(format!(
                "source.auto_offset_reset must be one of: {}",
                valid_offsets.join(", ")
            )));
        }

        // Source drain_timeout_ms must be less than session_timeout_ms
        if let Some(drain) = self.source.drain_timeout_ms
            && drain >= self.source.session_timeout_ms
        {
            return Err(crate::Error::Validation(format!(
                "source.drain_timeout_ms ({drain}) must be less than session_timeout_ms ({})",
                self.source.session_timeout_ms
            )));
        }

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
        Self::validate_librdkafka_options(
            "sink",
            &self.sink.librdkafka_options,
            &super::generate::derived_sink_options(&self.sink),
        )?;

        // Sink encoding
        let valid_encodings = ["json", "raw_bytes"];
        if !valid_encodings.contains(&self.sink.encoding.as_str()) {
            return Err(crate::Error::Validation(format!(
                "sink.encoding must be one of: {}",
                valid_encodings.join(", ")
            )));
        }

        // Sink compression
        let valid_compressions = ["none", "gzip", "lz4", "snappy", "zstd"];
        if !valid_compressions.contains(&self.sink.compression.as_str()) {
            return Err(crate::Error::Validation(format!(
                "sink.compression must be one of: {}",
                valid_compressions.join(", ")
            )));
        }

        // Vector version check mode
        let valid_modes = ["strict", "warn", "disabled"];
        if !valid_modes.contains(&self.vector.version_check.as_str()) {
            return Err(crate::Error::Validation(format!(
                "vector.version_check must be one of: {}",
                valid_modes.join(", ")
            )));
        }

        // A mode without a pin is a check that cannot fire.
        // `check_vector_version` compares only when `version` is non-empty, so
        // `strict`/`warn` with an empty pin accepts whatever Vector is on PATH.
        // The default pin is non-empty, but `vector: { version: "" }` in YAML
        // and `..._VECTOR_VERSION=""` in the environment both reach here.
        if self.vector.version_check != "disabled" && self.vector.version.is_empty() {
            return Err(crate::Error::Validation(format!(
                "vector.version must not be empty when vector.version_check is \
                 '{}' -- an empty pin skips the comparison entirely, so the \
                 check would never fire. Set the expected version, or set \
                 vector.version_check to 'disabled'.",
                self.vector.version_check
            )));
        }

        // Reject a bad version_source HERE rather than at first use. A typo
        // that only surfaces when the binary is acquired means the pod starts,
        // passes config validation, and then fails somewhere less obvious.
        let version_source = crate::vector::VersionSource::parse(&self.vector.version_source)
            .map_err(|e| crate::Error::Validation(e.to_string()))?;

        // Refuse a source the runtime cannot act on. Nothing on the run path
        // resolves a version or downloads a binary -- `spawn_vector` runs
        // `vector.binary` -- so every other value would quietly get the
        // pre-shipped binary while the config says otherwise.
        if version_source != crate::vector::VersionSource::Preshipped {
            return Err(crate::Error::Validation(format!(
                "vector.version_source must be 'preshipped', got '{}'. This \
                 process runs vector.binary and never acquires one, so any \
                 other source would silently resolve to the pre-shipped \
                 binary. Point vector.binary at the Vector you want and set \
                 vector.version to match.",
                self.vector.version_source
            )));
        }

        // Logging level and format reach scalo's logger via
        // `fill_common_args`, which parses them and aborts startup on a bad
        // value. Catch it here so the message names the config key.
        let valid_levels = ["trace", "debug", "info", "warn", "error"];
        if !valid_levels.contains(&self.logging.level.as_str()) {
            return Err(crate::Error::Validation(format!(
                "logging.level must be one of: {}",
                valid_levels.join(", ")
            )));
        }
        let valid_formats = ["json", "text", "auto"];
        if !valid_formats.contains(&self.logging.format.as_str()) {
            return Err(crate::Error::Validation(format!(
                "logging.format must be one of: {}",
                valid_formats.join(", ")
            )));
        }

        // Scaling pressure threshold must be in [0.0, 1.0]
        if !(0.0..=1.0).contains(&self.scaling.pressure_threshold) {
            return Err(crate::Error::Validation(format!(
                "scaling.pressure_threshold must be between 0.0 and 1.0, got {}",
                self.scaling.pressure_threshold
            )));
        }

        Ok(())
    }

    /// Refuse a `librdkafka_options` entry the generator is going to overwrite.
    ///
    /// The generator writes the big-dial-derived options after merging this
    /// map, so `sink.librdkafka_options: {compression.type: lz4}` alongside
    /// `sink.compression: zstd` produced zstd and said nothing -- while the
    /// docs called this map the layer above everything. The derived list lives
    /// in the generator that applies it, so the two cannot drift apart.
    fn validate_librdkafka_options(
        side: &str,
        options: &BTreeMap<String, String>,
        derived: &[(&str, &str)],
    ) -> Result<()> {
        for (key, owner) in derived {
            if options.contains_key(*key) {
                return Err(crate::Error::Validation(format!(
                    "{side}.librdkafka_options sets '{key}', which is derived from \
                     {owner} and overwritten when the Vector config is generated. \
                     Set it through {owner}, or remove the librdkafka_options entry."
                )));
            }
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
        // Username is required when SASL is enabled (password may use env var
        // interpolation like ${KAFKA_SASL_PASSWORD} which appears non-empty).
        if sasl.username.is_empty() {
            return Err(crate::Error::Validation(format!(
                "{prefix}.username must not be empty when SASL is enabled"
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
