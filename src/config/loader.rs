// Project:   dfe-transform-vector
// File:      src/config/loader.rs
// Purpose:   Configuration structures and loading
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Configuration structures and loading.
//!
//! Big-dial config schema for the source/sink on either transport,
//! user-supplied transforms, the Vector subprocess, the metrics endpoint, and
//! scaling pressure.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};
use tracing::debug;

use scalo::cli::CommonArgs;
use scalo::kafka_config::{KafkaSource, ServiceRole};
use scalo::transport::{AcknowledgementsConfig, KafkaConfig};

use crate::Result;

/// SASL authentication for Kafka.
///
/// The credentials reach Vector through its `directory` secret backend, never
/// as text in the Vector config: either a mounted directory holding `username`
/// and `password` files (`secret_dir`), or `username`/`password` here, which
/// the assembler writes to owner-only files beside the Vector config.
///
/// `password` is `String`, not `SensitiveString`, because the config goes
/// through a figment serialize-merge-deserialize round trip in
/// `apply_figment_env()`, and `SensitiveString` serialises as `***REDACTED***`.
/// `Debug` redacts it instead.
#[derive(Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct SaslConfig {
    /// Enable SASL authentication.
    pub enabled: bool,
    /// SASL mechanism: plain, scram_sha_256, scram_sha_512.
    pub mechanism: String,
    /// SASL username. Leave empty when `secret_dir` supplies it.
    pub username: String,
    /// SASL password. Leave empty when `secret_dir` supplies it.
    ///
    /// Taken literally: Vector does not expand `${VAR}` placeholders, so a
    /// value holding one is refused.
    // Typed `String` for the round-trip above, but schema'd as scalo's
    // `SensitiveString` so the emitted config-schema carries `x-dfe-secret` and
    // `writeOnly`, which is what tells the console to mask the field.
    #[schemars(with = "scalo::SensitiveString")]
    pub password: String,
    /// Directory holding the credentials as files named `username` and
    /// `password`, such as a mounted Kubernetes Secret. Vector reads them
    /// itself; replaces `username` and `password`.
    pub secret_dir: Option<String>,
}

impl Default for SaslConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mechanism: "scram_sha_512".to_string(),
            username: String::new(),
            password: String::new(),
            secret_dir: None,
        }
    }
}

impl std::fmt::Debug for SaslConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let password = if self.password.is_empty() {
            ""
        } else {
            "***REDACTED***"
        };
        f.debug_struct("SaslConfig")
            .field("enabled", &self.enabled)
            .field("mechanism", &self.mechanism)
            .field("username", &self.username)
            .field("password", &password)
            .field("secret_dir", &self.secret_dir)
            .finish()
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
/// - `source.*` — the consumer or the Push listener is established at startup
/// - `sink.*` — the producer or the Push client is established at startup
/// - `bridge.*` — the two supervisor-to-Vector legs bind at startup
/// - `pipeline.name` — used in consumer group_id and metrics labels at startup
/// - `vector.*` — binary path, data_dir, config_dir, API switch and address,
///   log level set at Vector spawn
/// - `metrics.address` -- the HTTP server binds at startup
/// - `logging.*` -- tracing subscriber configured at startup
/// - `reload.poll_interval_secs` -- captured at reload loop start
///
/// **Read by nothing in this process:**
/// - `scaling.pressure_threshold` -- no reader. Scale-out is driven by the
///   chart's KEDA triggers (consumer-group lag and CPU), gated by the
///   subprocess circuit in [`crate::metrics::spawn_circuit_gate_task`].
///   scalo's runtime reads its own `scaling.enabled` and
///   `scaling.memory_gate_threshold` from the cascade, not this key.
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
    /// Source (input), on either transport. **Requires restart.**
    pub source: SourceConfig,
    /// Sink (output), on either transport. **Requires restart.**
    pub sink: SinkConfig,
    /// The supervisor-to-Vector legs, direct transport only. **Requires restart.**
    pub bridge: BridgeConfig,
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

/// Which transport a stage uses.
///
/// One deployment runs one of them: `bus` is a broker between the stages,
/// `direct` is gRPC between them and needs no broker at all. The record and the
/// transform are identical either way -- only who hands the record over changes.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum Transport {
    /// Kafka topics.
    #[default]
    Bus,
    /// A scalo Push listener (source) or client (sink), bridged to Vector.
    Direct,
}

impl Transport {
    /// Whether this stage is on the direct transport.
    #[must_use]
    pub const fn is_direct(self) -> bool {
        matches!(self, Self::Direct)
    }
}

/// Reject a bind address the listener would fail on at startup.
///
/// A hostname is not a bind address: it resolves at socket-bind time and the
/// failure surfaces after readiness has already been published.
fn validate_bind(field: &str, value: &str) -> Result<()> {
    if value.parse::<std::net::SocketAddr>().is_ok() {
        return Ok(());
    }
    Err(crate::Error::Validation(format!(
        "{field} must be a host:port bind address on the direct transport (got '{value}')"
    )))
}

/// Parse a transport name from the environment, keeping `current` on anything
/// unrecognised so a typo cannot silently move a deployment off its transport.
fn parse_transport(value: &str, current: Transport) -> Transport {
    match value.trim().to_ascii_lowercase().as_str() {
        "bus" | "kafka" => Transport::Bus,
        "direct" | "grpc" => Transport::Direct,
        other => {
            tracing::warn!(
                value = other,
                "unknown transport, keeping the configured one"
            );
            current
        }
    }
}

/// Source configuration: the bus topics to consume, or the Push listener to
/// accept records on.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct SourceConfig {
    /// `bus` consumes `topics`; `direct` accepts pushes on `listen`.
    pub transport: Transport,
    /// Address the scalo Push listener binds on the direct transport.
    pub listen: String,
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
    /// Hold each record's source acknowledgement until the sink has it.
    ///
    /// On (the default) a Kafka offset is committed, and a push is answered,
    /// only once the record is delivered, so a crash redelivers rather than
    /// loses it. Off, both happen at receipt.
    pub acknowledgements: AcknowledgementsConfig,
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self {
            transport: Transport::default(),
            listen: "0.0.0.0:6000".to_string(),
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
            acknowledgements: AcknowledgementsConfig::default(),
        }
    }
}

/// Sink configuration: the bus topic to produce to, or the Push listener to
/// send the transformed records on to.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct SinkConfig {
    /// `bus` produces to `topic`; `direct` pushes to `endpoint`.
    pub transport: Transport,
    /// Downstream Push listener on the direct transport, e.g. the loader.
    pub endpoint: String,
    /// Kafka bootstrap servers.
    pub brokers: Vec<String>,
    /// Output topic name. On the direct transport it is still the routing key
    /// the downstream stage picks its table by, so a source keeps its name.
    pub topic: String,
    /// Event field path for Kafka partition key (e.g., ".org_id").
    pub key_field: String,
    /// Encoding codec: json, raw_bytes.
    pub encoding: String,
    /// Compression: none, gzip, lz4, snappy, zstd. Default: zstd.
    pub compression: String,
    /// SASL authentication.
    pub sasl: SaslConfig,
    /// TLS configuration.
    pub tls: TlsConfig,
    /// Sink buffer configuration.
    pub buffer: BufferConfig,
    /// Sink batch configuration (Vector-level batching).
    pub batch: BatchConfig,
    /// Local message timeout (ms). Default: 0, no limit.
    ///
    /// librdkafka rejects a record still undelivered at this limit, and the
    /// rejection drops it. With no limit a broker outage holds the record and
    /// back-pressures the source instead.
    pub message_timeout_ms: u32,
    /// Network request timeout (ms). Default: 60000 (60s).
    pub socket_timeout_ms: u32,
    /// Extra librdkafka options (passed through to Vector).
    pub librdkafka_options: BTreeMap<String, String>,
}

impl Default for SinkConfig {
    fn default() -> Self {
        Self {
            transport: Transport::default(),
            endpoint: "http://dfe-loader:6000".to_string(),
            brokers: vec!["localhost:9092".to_string()],
            topic: String::new(),
            key_field: String::new(),
            encoding: "json".to_string(),
            compression: "zstd".to_string(),
            sasl: SaslConfig::default(),
            tls: TlsConfig::default(),
            buffer: BufferConfig::default(),
            batch: BatchConfig::default(),
            message_timeout_ms: 0,
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

/// The two in-pod legs between the supervisor and Vector, on the direct
/// transport only.
///
/// Vector does not speak scalo's `Transport/Push`, so on `direct` the
/// supervisor translates: it accepts records from the rest of DFE on
/// `source.listen` and hands them to Vector over Vector's own protocol, then
/// takes them back and pushes them to `sink.endpoint`. Both legs stay on
/// loopback -- they exist inside one pod and nothing outside it may reach them.
/// On the bus neither address is bound.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct BridgeConfig {
    /// Where Vector accepts records from the supervisor -- Vector's own
    /// `vector` source binds here, and the supervisor dials it.
    pub to_vector: String,
    /// Where the supervisor accepts the transformed records back -- Vector's
    /// `vector` sink dials here, and the supervisor's listener binds it.
    pub from_vector: String,
    /// Records moved per bridge hop. Bounds the batch a retry replays.
    pub batch_size: usize,
}

impl Default for BridgeConfig {
    fn default() -> Self {
        Self {
            to_vector: "127.0.0.1:6100".to_string(),
            from_vector: "127.0.0.1:6101".to_string(),
            batch_size: 500,
        }
    }
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
    /// Run Vector's API (GraphQL, health, `vector top`). Default: off.
    ///
    /// Nothing in this process is a client, and the API accepts unauthenticated
    /// requests, so it stays off unless an operator wants it for debugging.
    pub api_enabled: bool,
    /// Vector API bind address (host:port), used only with `api_enabled`.
    ///
    /// Must be a loopback address: reach it with `kubectl port-forward` or
    /// `docker exec`, never from the network.
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
            api_enabled: false,
            api_address: "127.0.0.1:8686".to_string(),
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
    /// Vector's `prometheus_exporter` bind address (host:port).
    ///
    /// The wrapper writes it into the assembled Vector config AND scrapes it,
    /// merging every `vector_*` sample into the scalo registry, so nothing
    /// outside the pod needs this port. Keep it on loopback.
    pub vector_metrics_address: String,
    /// Scrape ticks a merged `vector_*` gauge may go unseen before it is
    /// zeroed, so a component Vector drops stops reporting its last value.
    pub vector_metrics_expiry_ticks: u32,
    /// Seconds the sink may hold records, or have the broker refuse them,
    /// without delivering any before `/readyz` reports not ready. 0 never does.
    /// Default: 60.
    pub sink_stall_secs: u64,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            address: "0.0.0.0:9090".to_string(),
            vector_metrics_address: "127.0.0.1:9598".to_string(),
            vector_metrics_expiry_ticks: crate::metrics::scrape::DEFAULT_EXPIRY_TICKS,
            sink_stall_secs: 60,
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

/// Set up scalo's cascade with the loaded config file as its settings layer.
///
/// This app reads its own sections from the file directly, but the sections
/// scalo's runtime owns (`version_check`, `metrics` and the rest) resolve from
/// the cascade alone. A reload re-enters here, and the cascade is set once per
/// process.
fn init_cascade(path: Option<&str>) -> Result<()> {
    let opts = scalo::config::ConfigOptions {
        env_prefix: ENV_PREFIX.to_string(),
        config_paths: path.map(std::path::PathBuf::from).into_iter().collect(),
        // `Config::load` has already read `.env` into the process environment.
        load_dotenv: false,
        ..Default::default()
    };
    match scalo::config::setup(opts) {
        Ok(()) | Err(scalo::config::ConfigError::AlreadyInitialised) => Ok(()),
        Err(e) => Err(crate::Error::Config(format!("failed to setup config: {e}"))),
    }
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
        if let Some(v) = flat_env::flat_env_string(prefix, "SOURCE_TRANSPORT") {
            self.source.transport = parse_transport(&v, self.source.transport);
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "SOURCE_LISTEN") {
            self.source.listen = v;
        }
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
        if let Some(v) = flat_env::flat_env_string(prefix, "SINK_TRANSPORT") {
            self.sink.transport = parse_transport(&v, self.sink.transport);
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "SINK_ENDPOINT") {
            self.sink.endpoint = v;
        }
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

        // Bridge
        if let Some(v) = flat_env::flat_env_string(prefix, "BRIDGE_TO_VECTOR") {
            self.bridge.to_vector = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "BRIDGE_FROM_VECTOR") {
            self.bridge.from_vector = v;
        }
        if let Some(v) = flat_env::flat_env_parsed::<usize>(prefix, "BRIDGE_BATCH_SIZE") {
            self.bridge.batch_size = v;
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
    /// Credentials or a credential directory present → enable SASL automatically.
    fn normalize(&mut self) {
        for sasl in [&mut self.source.sasl, &mut self.sink.sasl] {
            if !sasl.username.is_empty() || !sasl.password.is_empty() || sasl.secret_dir.is_some() {
                sasl.enabled = true;
            }
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
        let mut loaded_from = None;

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
            loaded_from = Some(path);
        } else {
            for path in &["config.yaml", "config.yml"] {
                if Path::new(path).exists() {
                    let content = std::fs::read_to_string(path)
                        .map_err(|e| crate::Error::Config(format!("failed to read {path}: {e}")))?;
                    config = serde_yaml_ng::from_str(&content)?;
                    debug!(path, "loaded configuration file");
                    loaded_from = Some(*path);
                    break;
                }
            }
        }

        init_cascade(loaded_from)?;

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
    /// fall through to scalo's config cascade, which reads `logger.*` rather
    /// than `logging.*` and none of the flat `DFE_TRANSFORM_*` overrides -- so
    /// a value set here reaches nothing unless it is put where the resolvers
    /// look.
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

        // The two ends move independently: a stage may consume a topic and push
        // the result onward, or the reverse.
        let direct_in = self.source.transport.is_direct();
        let direct_out = self.sink.transport.is_direct();
        self.validate_bridge(direct_in, direct_out)?;

        // Source. Structural problems refuse; a valid config with no topics is a
        // transform whose source has not been written yet, which idles instead
        // (see `work_state`).
        if direct_in {
            validate_bind("source.listen", &self.source.listen)?;
        } else {
            if self.source.brokers.is_empty() {
                return Err(crate::Error::Validation(
                    "source.brokers must have at least one broker".into(),
                ));
            }
            if self.source.group_id.is_empty() {
                return Err(crate::Error::Validation(
                    "source.group_id must not be empty".into(),
                ));
            }
        }
        self.validate_sasl("source.sasl", &self.source.sasl)?;
        Self::validate_librdkafka_options(
            "source",
            &self.source.librdkafka_options,
            &super::generate::derived_source_options(&self.source),
        )?;
        if !direct_in {
            Self::validate_kafka_floor(
                "source",
                &super::generate::kafka_client_config(
                    &self.source.brokers,
                    &self.source.sasl,
                    &self.source.tls,
                    &self.source.librdkafka_options,
                ),
            )?;
        }

        // Vector stores an offset only once the sink has acknowledged the
        // event, which is what makes the commit timer safe to run. Hand that
        // job back to librdkafka and offsets are stored at fetch time and
        // committed seconds later, so a pod that dies mid-flight skips every
        // record it had read but not yet produced.
        if self
            .source
            .librdkafka_options
            .get("enable.auto.offset.store")
            .is_some_and(|v| v == "true")
        {
            return Err(crate::Error::Validation(
                "source.librdkafka_options.enable.auto.offset.store must not be true: Vector \
                 stores offsets after the sink acknowledges, and letting librdkafka store them \
                 on fetch silently drops records on an unclean shutdown"
                    .into(),
            ));
        }

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
        if direct_out {
            if self.sink.endpoint.is_empty() {
                return Err(crate::Error::Validation(
                    "sink.endpoint must not be empty on the direct transport".into(),
                ));
            }
        } else {
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
        }
        self.validate_sasl("sink.sasl", &self.sink.sasl)?;
        self.validate_buffer(&self.sink.buffer)?;
        Self::validate_librdkafka_options(
            "sink",
            &self.sink.librdkafka_options,
            &super::generate::derived_sink_options(&self.sink),
        )?;
        if !direct_out {
            Self::validate_kafka_floor(
                "sink",
                &super::generate::kafka_client_config(
                    &self.sink.brokers,
                    &self.sink.sasl,
                    &self.sink.tls,
                    &self.sink.librdkafka_options,
                ),
            )?;
        }

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

        self.validate_api()?;

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

    /// Hold one generated Kafka client to scalo's Kafka security floor.
    ///
    /// The floor has rules for every environment and stricter ones for
    /// production, which `scalo::env::is_production` reads from `APP_ENV`,
    /// `ENVIRONMENT` or `ENV`.
    fn validate_kafka_floor(side: &str, client: &KafkaConfig) -> Result<()> {
        super::generate::kafka_floor(client).map_err(|e| {
            crate::Error::Validation(format!(
                "{side} Kafka client refused ({side}.sasl and {side}.tls set its \
                 security_protocol and sasl_mechanism): {e}"
            ))
        })
    }

    /// Does this configuration give the transform work?
    ///
    /// On the bus a transform with no topics has nothing to consume: it starts,
    /// stays Ready, holds no consumer group, and picks up the first config that
    /// names a topic. On the direct transport the listener IS the work.
    #[must_use]
    pub fn work_state(&self) -> scalo::lifecycle::WorkState {
        scalo::lifecycle::WorkState::idle_if(
            !self.source.transport.is_direct() && self.source.topics.is_empty(),
            "no source topics configured",
        )
    }

    /// Check the supervisor-to-Vector legs each direct end actually binds.
    ///
    /// A leg only exists where its end of the pipeline is on the direct
    /// transport, so a bus-in/direct-out stage is judged on `from_vector` alone.
    fn validate_bridge(&self, direct_in: bool, direct_out: bool) -> Result<()> {
        if !direct_in && !direct_out {
            return Ok(());
        }
        if direct_in {
            validate_bind("bridge.to_vector", &self.bridge.to_vector)?;
        }
        if direct_out {
            validate_bind("bridge.from_vector", &self.bridge.from_vector)?;
        }
        if direct_in && direct_out && self.bridge.to_vector == self.bridge.from_vector {
            return Err(crate::Error::Validation(
                "bridge.to_vector and bridge.from_vector must differ -- they are the \
                 two ends of the loop through Vector, and one address cannot be both"
                    .into(),
            ));
        }
        if self.bridge.batch_size == 0 {
            return Err(crate::Error::Validation(
                "bridge.batch_size must be at least 1".into(),
            ));
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
        // Vector expands no `${VAR}`, so a placeholder would reach the broker
        // as the credential and every login would fail.
        for (field, value) in [("username", &sasl.username), ("password", &sasl.password)] {
            if value.contains("${") {
                return Err(crate::Error::Validation(format!(
                    "{prefix}.{field} holds a '${{...}}' placeholder, which nothing expands: \
                     Vector reads it literally. Set the credential itself (the \
                     DFE_TRANSFORM_{side}_SASL_{upper} environment variable does), or point \
                     {prefix}.secret_dir at a directory holding username and password files",
                    side = prefix
                        .split('.')
                        .next()
                        .unwrap_or_default()
                        .to_ascii_uppercase(),
                    upper = field.to_ascii_uppercase(),
                )));
            }
        }
        match &sasl.secret_dir {
            Some(dir) => {
                if !sasl.username.is_empty() || !sasl.password.is_empty() {
                    return Err(crate::Error::Validation(format!(
                        "{prefix}.secret_dir and {prefix}.username/password both name the \
                         credentials -- set one of them"
                    )));
                }
                if !Path::new(dir).is_absolute() {
                    return Err(crate::Error::Validation(format!(
                        "{prefix}.secret_dir must be an absolute path (got '{dir}'): Vector \
                         resolves it from its own working directory"
                    )));
                }
            }
            None => {
                if sasl.username.is_empty() {
                    return Err(crate::Error::Validation(format!(
                        "{prefix}.username must not be empty when SASL is enabled, unless \
                         {prefix}.secret_dir supplies it"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Keep Vector's API off the network.
    ///
    /// It takes unauthenticated GraphQL requests and no part of this process
    /// is a client, so it is opt-in and bound to loopback.
    fn validate_api(&self) -> Result<()> {
        if !self.vector.api_enabled {
            return Ok(());
        }
        let address = &self.vector.api_address;
        match address.parse::<std::net::SocketAddr>() {
            Ok(addr) if addr.ip().is_loopback() => Ok(()),
            Ok(_) => Err(crate::Error::Validation(format!(
                "vector.api_address must be a loopback address when vector.api_enabled is true \
                 (got '{address}'): the API takes unauthenticated requests, so reach it with \
                 port-forward or exec rather than publishing it"
            ))),
            Err(_) => Err(crate::Error::Validation(format!(
                "vector.api_address must be a host:port bind address (got '{address}')"
            ))),
        }
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
