// Project:   dfe-transform-vector
// File:      src/config/generate.rs
// Purpose:   Generate Vector-native YAML from big-dial config
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Vector-native YAML generation from big-dial config.
//!
//! Generates source, sink, and observability YAML files that Vector
//! understands, using the canonical component labels `dfe_source` and
//! `dfe_sink`.
//!
//! The labels and the file layout are the same on both transports, so a
//! transform file authored against `dfe_source` runs unchanged whichever one
//! the deployment is on. Only the component the label names changes: `kafka`
//! on the bus, `vector` on direct -- where the supervisor's bridge is the peer
//! at each end.
//!
//! Production librdkafka defaults come from `scalo::kafka_config`
//! (shared DFE baseline). Service-specific overrides and user-supplied
//! `librdkafka_options` from config YAML are merged on top.

use std::collections::{BTreeMap, HashMap};

use serde_yaml_ng::Value;

use super::kafka_defaults;
use super::loader::{
    BridgeConfig, BufferConfig, SaslConfig, SinkConfig, SourceConfig, TlsConfig, VectorConfig,
};
use super::secrets::{SecretBackend, SecretNames, Side};

/// Canonical label for the generated source component.
pub const SOURCE_LABEL: &str = "dfe_source";

/// Canonical label for the generated sink component.
pub const SINK_LABEL: &str = "dfe_sink";

/// Label of the filter that keeps records over the producer's size ceiling off
/// the Kafka sink.
pub const SIZE_CAP_LABEL: &str = "dfe_size_cap";

/// librdkafka's own `message.max.bytes` default, the ceiling when the sink's
/// `librdkafka_options` sets none.
const LIBRDKAFKA_MESSAGE_MAX_BYTES: u64 = 1_000_000;

/// Room left under `message.max.bytes` for the key, headers and record framing.
const RECORD_FRAMING_BYTES: u64 = 128;

/// Generate Vector global config YAML (timezone, data_dir, API, secret backends).
///
/// `secrets` are the SASL credential sources, each one `directory` entry under
/// `secret`.
#[must_use]
pub fn generate_global_yaml(vector: &VectorConfig, secrets: &[SecretBackend]) -> Value {
    let mut root = serde_yaml_ng::Mapping::new();

    // Vector's default is the HOST's timezone, so a transform that parses a
    // zone-less timestamp would produce a different instant on every pod. DFE
    // events are UTC end to end.
    root.insert(val("timezone"), val("UTC"));

    if !vector.data_dir.is_empty() {
        root.insert(val("data_dir"), val(&vector.data_dir));
    }

    // Written out even when off, so a change of Vector's own default cannot
    // open an unauthenticated listener.
    let mut api = serde_yaml_ng::Mapping::new();
    api.insert(val("enabled"), Value::Bool(vector.api_enabled));
    if vector.api_enabled && !vector.api_address.is_empty() {
        api.insert(val("address"), val(&vector.api_address));
    }
    root.insert(val("api"), Value::Mapping(api));

    let mut backends = serde_yaml_ng::Mapping::new();
    for secret in secrets {
        let mut backend = serde_yaml_ng::Mapping::new();
        backend.insert(val("type"), val("directory"));
        backend.insert(val("path"), val(&secret.dir.to_string_lossy()));
        // A mounted Secret written by hand often ends in a newline, which
        // would otherwise become part of the credential.
        backend.insert(val("remove_trailing_whitespace"), Value::Bool(true));
        backends.insert(val(secret.name), Value::Mapping(backend));
    }
    if !backends.is_empty() {
        root.insert(val("secret"), Value::Mapping(backends));
    }

    Value::Mapping(root)
}

/// Generate the `sources.dfe_source` YAML for this deployment's transport.
///
/// On the bus that is a Kafka consumer with production-tuned librdkafka options
/// derived from DFE 2.1 templates -- fetch sizing, pre-fetch queuing, commit
/// control, cooperative-sticky rebalancing. On direct it is Vector's own
/// `vector` source, which the supervisor's bridge pushes into over loopback.
#[must_use]
pub fn generate_source_yaml(source: &SourceConfig, bridge: &BridgeConfig) -> Value {
    let component = if source.transport.is_direct() {
        direct_source_component(bridge)
    } else {
        kafka_source_component(source)
    };
    wrap("sources", SOURCE_LABEL, component)
}

/// The `vector` source the supervisor's bridge delivers into.
///
/// It carries no `acknowledgements` of its own: Vector turns them on for a
/// source whose sink has them, and deprecates setting them on the source. With
/// them on, the RPC does not return until the record has reached the sink, so
/// the bridge answers its own sender only once Vector has delivered.
fn direct_source_component(bridge: &BridgeConfig) -> serde_yaml_ng::Mapping {
    let mut component = serde_yaml_ng::Mapping::new();
    component.insert(val("type"), val("vector"));
    component.insert(val("address"), val(&bridge.to_vector));
    component
}

/// An `{enabled: <on>}` block.
fn enabled_block(on: bool) -> Value {
    let mut block = serde_yaml_ng::Mapping::new();
    block.insert(val("enabled"), Value::Bool(on));
    Value::Mapping(block)
}

fn kafka_source_component(source: &SourceConfig) -> serde_yaml_ng::Mapping {
    let mut component = serde_yaml_ng::Mapping::new();
    component.insert(val("type"), val("kafka"));
    component.insert(val("bootstrap_servers"), val(&source.brokers.join(",")));
    component.insert(
        val("topics"),
        Value::Sequence(source.topics.iter().map(|t| val(t)).collect()),
    );
    component.insert(val("group_id"), val(&source.group_id));

    // Decoding
    let mut decoding = serde_yaml_ng::Mapping::new();
    decoding.insert(val("codec"), val(&source.decoding.codec));
    component.insert(val("decoding"), Value::Mapping(decoding));

    // Session and commit timing
    component.insert(
        val("session_timeout_ms"),
        Value::Number(source.session_timeout_ms.into()),
    );
    component.insert(
        val("commit_interval_ms"),
        Value::Number(source.commit_interval_ms.into()),
    );
    if let Some(drain_ms) = source.drain_timeout_ms {
        component.insert(val("drain_timeout_ms"), Value::Number(drain_ms.into()));
    }

    // Offset reset
    component.insert(val("auto_offset_reset"), val(&source.auto_offset_reset));

    // Consumer lag metric
    if source.topic_lag_metric {
        let mut metrics = serde_yaml_ng::Mapping::new();
        metrics.insert(val("topic_lag_metric"), Value::Bool(true));
        component.insert(val("metrics"), Value::Mapping(metrics));
    }

    // SASL
    if source.sasl.enabled {
        component.insert(val("sasl"), build_sasl_block(Side::Source, &source.sasl));
    }

    // TLS
    if source.tls.enabled {
        component.insert(val("tls"), build_tls_block(&source.tls));
    }

    // librdkafka options — production defaults + user overrides
    let rdkafka = build_source_librdkafka(source);
    component.insert(val("librdkafka_options"), Value::Mapping(rdkafka));

    component
}

/// Wrap one component under its section and label, the shape every generated
/// file takes.
fn wrap(name: &str, label: &str, component: serde_yaml_ng::Mapping) -> Value {
    Value::Mapping(section(name, label, component))
}

/// `{<name>: {<label>: <component>}}`.
fn section(name: &str, label: &str, component: serde_yaml_ng::Mapping) -> serde_yaml_ng::Mapping {
    let mut components = serde_yaml_ng::Mapping::new();
    components.insert(val(label), Value::Mapping(component));

    let mut root = serde_yaml_ng::Mapping::new();
    root.insert(val(name), Value::Mapping(components));
    root
}

/// Generate the `sinks.dfe_sink` YAML for this deployment's transport.
///
/// On the bus that is a Kafka producer with production-tuned librdkafka
/// options, Vector-level batching, buffer config and acknowledgements, behind
/// a filter that holds back records over the producer's size ceiling. On
/// direct it is Vector's own `vector` sink, dialling the supervisor's bridge,
/// which pushes the records on to `sink.endpoint`.
///
/// `acknowledged` is `source.acknowledgements.enabled`: the sink's end-to-end
/// acknowledgement is what lets the source hold its own.
#[must_use]
pub fn generate_sink_yaml(
    sink: &SinkConfig,
    bridge: &BridgeConfig,
    inputs: &[String],
    acknowledged: bool,
) -> Value {
    if sink.transport.is_direct() {
        return wrap(
            "sinks",
            SINK_LABEL,
            direct_sink_component(sink, bridge, inputs, acknowledged),
        );
    }

    let mut root = section(
        "sinks",
        SINK_LABEL,
        kafka_sink_component(sink, &[SIZE_CAP_LABEL.to_string()], acknowledged),
    );
    root.extend(section(
        "transforms",
        SIZE_CAP_LABEL,
        size_cap_component(sink, inputs),
    ));
    Value::Mapping(root)
}

/// The largest record, in encoded bytes, the Kafka sink is handed.
///
/// librdkafka refuses a record over `message.max.bytes`, and Vector drops a
/// refused record. The key and headers count against the same limit, so a
/// margin is kept below it.
#[must_use]
pub fn max_record_bytes(sink: &SinkConfig) -> u64 {
    sink.librdkafka_options
        .get("message.max.bytes")
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(LIBRDKAFKA_MESSAGE_MAX_BYTES)
        .saturating_sub(RECORD_FRAMING_BYTES)
}

/// The filter in front of the Kafka sink that drops a record over
/// [`max_record_bytes`], counted in `component_discarded_events_total`.
///
/// Measures the record as the sink's codec encodes it: the JSON of the whole
/// event, or the `message` field for `raw_bytes`.
fn size_cap_component(sink: &SinkConfig, inputs: &[String]) -> serde_yaml_ng::Mapping {
    let encoded = match sink.encoding.as_str() {
        "raw_bytes" => r#"string(.message) ?? """#,
        _ => "encode_json(.)",
    };
    let condition = format!("length({encoded}) <= {}", max_record_bytes(sink));

    let mut component = serde_yaml_ng::Mapping::new();
    component.insert(val("type"), val("filter"));
    component.insert(val("inputs"), input_list(inputs));
    component.insert(val("condition"), val(&condition));
    component
}

/// The `vector` sink that hands the transformed records back to the supervisor.
///
/// The buffer is the operator's dial on both transports, so it is carried here
/// too; `acknowledgements` completes the hold-rather-than-drop chain the
/// `vector` source starts.
fn direct_sink_component(
    sink: &SinkConfig,
    bridge: &BridgeConfig,
    inputs: &[String],
    acknowledged: bool,
) -> serde_yaml_ng::Mapping {
    let mut component = serde_yaml_ng::Mapping::new();
    component.insert(val("type"), val("vector"));
    component.insert(val("inputs"), input_list(inputs));
    // A URI, not a bind address: this end dials the supervisor's listener.
    component.insert(
        val("address"),
        val(&format!("http://{}", bridge.from_vector)),
    );
    component.insert(val("acknowledgements"), enabled_block(acknowledged));
    // The bridge is this process's own listener, bound only after `vector
    // validate` runs, and its health is the supervisor's readiness.
    component.insert(val("healthcheck"), enabled_block(false));
    component.insert(val("buffer"), build_buffer_block(&sink.buffer));

    component
}

fn kafka_sink_component(
    sink: &SinkConfig,
    inputs: &[String],
    acknowledged: bool,
) -> serde_yaml_ng::Mapping {
    let mut component = serde_yaml_ng::Mapping::new();
    component.insert(val("type"), val("kafka"));
    component.insert(val("inputs"), input_list(inputs));
    component.insert(val("bootstrap_servers"), val(&sink.brokers.join(",")));
    component.insert(val("topic"), val(&sink.topic));

    // Key field (partition key)
    if !sink.key_field.is_empty() {
        component.insert(val("key_field"), val(&sink.key_field));
    }

    // Encoding
    let mut encoding = serde_yaml_ng::Mapping::new();
    encoding.insert(val("codec"), val(&sink.encoding));
    component.insert(val("encoding"), Value::Mapping(encoding));

    // Compression
    if sink.compression != "none" {
        component.insert(val("compression"), val(&sink.compression));
    }

    // Acknowledgements — on the sink (not source) per Vector 0.53+ guidance
    component.insert(val("acknowledgements"), enabled_block(acknowledged));

    // Healthcheck
    component.insert(val("healthcheck"), enabled_block(true));

    // Timeouts
    component.insert(
        val("message_timeout_ms"),
        Value::Number(sink.message_timeout_ms.into()),
    );
    component.insert(
        val("socket_timeout_ms"),
        Value::Number(sink.socket_timeout_ms.into()),
    );

    // Vector-level batch
    let mut batch = serde_yaml_ng::Mapping::new();
    batch.insert(
        val("max_events"),
        Value::Number(sink.batch.max_events.into()),
    );
    batch.insert(
        val("timeout_secs"),
        Value::Number(sink.batch.timeout_secs.into()),
    );
    if let Some(max_bytes) = sink.batch.max_bytes {
        batch.insert(val("max_bytes"), Value::Number(max_bytes.into()));
    }
    component.insert(val("batch"), Value::Mapping(batch));

    // SASL
    if sink.sasl.enabled {
        component.insert(val("sasl"), build_sasl_block(Side::Sink, &sink.sasl));
    }

    // TLS
    if sink.tls.enabled {
        component.insert(val("tls"), build_tls_block(&sink.tls));
    }

    // Buffer
    component.insert(val("buffer"), build_buffer_block(&sink.buffer));

    // librdkafka options — production defaults + user overrides
    let rdkafka = build_sink_librdkafka(sink);
    component.insert(val("librdkafka_options"), Value::Mapping(rdkafka));

    component
}

/// A component's wired `inputs` list.
fn input_list(inputs: &[String]) -> Value {
    Value::Sequence(inputs.iter().map(|i| val(i)).collect())
}

/// Generate Vector observability YAML (internal_metrics + prometheus_exporter).
///
/// `address` is `metrics.vector_metrics_address`; the wrapper scrapes the same
/// address and merges the samples into the scalo registry, so it stays on
/// loopback rather than being published. Hard-coding it here instead left that
/// config key accepted, validated and inert.
#[must_use]
pub fn generate_observability_yaml(address: &str) -> Value {
    let mut metrics_source = serde_yaml_ng::Mapping::new();
    metrics_source.insert(val("type"), val("internal_metrics"));

    let mut sources = serde_yaml_ng::Mapping::new();
    sources.insert(val("internal_metrics"), Value::Mapping(metrics_source));

    let mut prom_sink = serde_yaml_ng::Mapping::new();
    prom_sink.insert(val("type"), val("prometheus_exporter"));
    prom_sink.insert(
        val("inputs"),
        Value::Sequence(vec![val("internal_metrics")]),
    );
    prom_sink.insert(val("address"), val(address));

    let mut sinks = serde_yaml_ng::Mapping::new();
    sinks.insert(val("prometheus_exporter"), Value::Mapping(prom_sink));

    let mut root = serde_yaml_ng::Mapping::new();
    root.insert(val("sources"), Value::Mapping(sources));
    root.insert(val("sinks"), Value::Mapping(sinks));

    Value::Mapping(root)
}

/// librdkafka options the generator derives from the big dials, with the
/// config path that owns each.
///
/// These are written AFTER `librdkafka_options` is merged in, so a user entry
/// for one of them is overwritten. [`crate::config::Config::validate`] reads
/// this same list and refuses such a config, rather than letting the discard
/// happen in silence -- `librdkafka_options` is documented as the layer above
/// everything, and for these keys it is not.
///
/// Condition-aware on purpose: with SASL off nothing derives `sasl.mechanism`,
/// so setting it by hand is legitimate and stays allowed.
#[must_use]
pub fn derived_source_options(source: &SourceConfig) -> Vec<(&'static str, &'static str)> {
    let mut derived = Vec::new();
    if security_protocol(&source.sasl, &source.tls).is_some() {
        derived.push((
            "security.protocol",
            "source.sasl.enabled and source.tls.enabled",
        ));
    }
    if sasl_mechanism(&source.sasl).is_some() {
        derived.push(("sasl.mechanism", "source.sasl.mechanism"));
    }
    if verification_off(&source.tls) {
        derived.push((
            "enable.ssl.certificate.verification",
            "source.tls.skip_verify",
        ));
    }
    derived
}

/// Sink counterpart of [`derived_source_options`].
///
/// `compression.type` is unconditional: the sink always writes it from
/// `sink.compression`.
#[must_use]
pub fn derived_sink_options(sink: &SinkConfig) -> Vec<(&'static str, &'static str)> {
    let mut derived = vec![("compression.type", "sink.compression")];
    if security_protocol(&sink.sasl, &sink.tls).is_some() {
        derived.push((
            "security.protocol",
            "sink.sasl.enabled and sink.tls.enabled",
        ));
    }
    if sasl_mechanism(&sink.sasl).is_some() {
        derived.push(("sasl.mechanism", "sink.sasl.mechanism"));
    }
    if verification_off(&sink.tls) {
        derived.push((
            "enable.ssl.certificate.verification",
            "sink.tls.skip_verify",
        ));
    }
    derived
}

/// Service-specific consumer librdkafka overrides for transform-vector.
///
/// Vector arms its offset-commit timer only with auto-commit on, and the shared
/// DFE baseline turns it off -- see docs/LIBRDKAFKA.md, Service-Specific
/// Overrides.
const SERVICE_CONSUMER_OVERRIDES: &[(&str, &str)] = &[("enable.auto.commit", "true")];

/// Service-specific producer librdkafka overrides for transform-vector.
///
/// - `queue.buffering.max.kbytes`: caps the local producer queue to 256 MiB
///   (librdkafka default is 1 GiB) because transform-vector pods typically have
///   2-4 GiB memory.
/// - `enable.idempotence`: the sink retries without limit, and a retry on a
///   non-idempotent producer can reorder and duplicate records within a
///   partition. librdkafka then sets `acks=all` and at most 5 requests in
///   flight itself.
const SERVICE_PRODUCER_OVERRIDES: &[(&str, &str)] = &[
    ("queue.buffering.max.kbytes", "262144"),
    ("enable.idempotence", "true"),
];

fn build_source_librdkafka(source: &SourceConfig) -> serde_yaml_ng::Mapping {
    let base = kafka_defaults::consumer_profile("production");
    let user_overrides: HashMap<String, String> = source
        .librdkafka_options
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let merged = kafka_defaults::merge_layers(&base, SERVICE_CONSUMER_OVERRIDES, &user_overrides);

    let mut m = serde_yaml_ng::Mapping::new();
    for (k, v) in &merged {
        m.insert(val(k), val(v));
    }
    insert_security_options(&mut m, &source.sasl, &source.tls);
    m
}

fn build_sink_librdkafka(sink: &SinkConfig) -> serde_yaml_ng::Mapping {
    let base = kafka_defaults::producer_profile("production");
    let user_overrides: HashMap<String, String> = sink
        .librdkafka_options
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let merged = kafka_defaults::merge_layers(&base, SERVICE_PRODUCER_OVERRIDES, &user_overrides);

    let mut m = serde_yaml_ng::Mapping::new();
    for (k, v) in &merged {
        m.insert(val(k), val(v));
    }

    // Compression from big-dial config (overrides scalo default if different)
    let compression = match sink.compression.as_str() {
        "none" => "none",
        "gzip" => "gzip",
        "lz4" => "lz4",
        "snappy" => "snappy",
        _ => "zstd",
    };
    m.insert(val("compression.type"), val(compression));
    insert_security_options(&mut m, &sink.sasl, &sink.tls);
    m
}

/// The `security.protocol` the generator writes for one Kafka side.
///
/// `None` when it writes none, and librdkafka runs its own default,
/// plaintext.
#[must_use]
pub fn security_protocol(sasl: &SaslConfig, tls: &TlsConfig) -> Option<&'static str> {
    match (sasl.enabled, tls.enabled) {
        (true, true) => Some("SASL_SSL"),
        (true, false) => Some("SASL_PLAINTEXT"),
        (false, true) => Some("SSL"),
        (false, false) => None,
    }
}

/// The `sasl.mechanism` the generator writes for one Kafka side, in
/// librdkafka's spelling, or `None` with SASL off.
#[must_use]
pub fn sasl_mechanism(sasl: &SaslConfig) -> Option<String> {
    sasl.enabled
        .then(|| normalise_sasl_mechanism(&sasl.mechanism))
}

/// Whether the generator turns certificate verification off for one Kafka
/// side: only with TLS on, since no TLS block is written otherwise.
fn verification_off(tls: &TlsConfig) -> bool {
    tls.enabled && tls.skip_verify
}

/// Write the security options the big dials derive over the merged layers.
fn insert_security_options(m: &mut serde_yaml_ng::Mapping, sasl: &SaslConfig, tls: &TlsConfig) {
    if let Some(protocol) = security_protocol(sasl, tls) {
        m.insert(val("security.protocol"), val(protocol));
    }
    if let Some(mechanism) = sasl_mechanism(sasl) {
        m.insert(val("sasl.mechanism"), val(&mechanism));
    }
    if verification_off(tls) {
        m.insert(val("enable.ssl.certificate.verification"), val("false"));
    }
}

/// The scalo Kafka client config equivalent to one generated Kafka side.
///
/// Vector runs the generated source and sink on its own librdkafka client, so
/// scalo never builds these clients and never applies its security floor to
/// them. This is the config that floor judges instead. The side's raw
/// `librdkafka_options` go into `librdkafka_overrides`, where every scalo
/// release that judges raw maps finds them.
#[must_use]
pub fn kafka_client_config(
    brokers: &[String],
    sasl: &SaslConfig,
    tls: &TlsConfig,
    librdkafka_options: &BTreeMap<String, String>,
) -> scalo::transport::KafkaConfig {
    scalo::transport::KafkaConfig {
        brokers: brokers.to_vec(),
        security_protocol: security_protocol(sasl, tls)
            .unwrap_or("PLAINTEXT")
            .to_string(),
        sasl_mechanism: sasl_mechanism(sasl),
        ssl_skip_verify: tls.skip_verify,
        librdkafka_overrides: librdkafka_options
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        ..Default::default()
    }
}

/// The component's `sasl` block, naming its credentials by `SECRET[...]`
/// reference so no credential is written into the Vector config.
fn build_sasl_block(side: Side, sasl: &super::loader::SaslConfig) -> Value {
    let secret = SecretNames::of(side, sasl);
    let mut m = serde_yaml_ng::Mapping::new();
    m.insert(val("enabled"), Value::Bool(true));
    m.insert(
        val("mechanism"),
        val(&normalise_sasl_mechanism(&sasl.mechanism)),
    );
    m.insert(val("username"), val(&secret.username_ref()));
    m.insert(val("password"), val(&secret.password_ref()));
    Value::Mapping(m)
}

fn build_tls_block(tls: &super::loader::TlsConfig) -> Value {
    let mut m = serde_yaml_ng::Mapping::new();
    m.insert(val("enabled"), Value::Bool(true));

    if let Some(ref ca) = tls.ca_cert_file {
        m.insert(val("ca_file"), val(ca));
    }
    if let Some(ref cert) = tls.cert_file {
        m.insert(val("crt_file"), val(cert));
    }
    if let Some(ref key) = tls.key_file {
        m.insert(val("key_file"), val(key));
    }
    if tls.skip_verify {
        m.insert(val("verify_certificate"), Value::Bool(false));
        m.insert(val("verify_hostname"), Value::Bool(false));
    }

    Value::Mapping(m)
}

fn build_buffer_block(buffer: &BufferConfig) -> Value {
    let mut m = serde_yaml_ng::Mapping::new();
    m.insert(val("type"), val(&buffer.buffer_type));

    match buffer.buffer_type.as_str() {
        "memory" => {
            if let Some(max_events) = buffer.max_events {
                m.insert(val("max_events"), Value::Number(max_events.into()));
            }
        }
        "disk" => {
            if let Some(max_size) = buffer.max_size {
                m.insert(val("max_size"), Value::Number(max_size.into()));
            }
        }
        _ => {}
    }

    m.insert(val("when_full"), val(&buffer.when_full));
    Value::Mapping(m)
}

/// Normalise SASL mechanism from config format to Vector's expected format.
///
/// Config uses lowercase with underscores (e.g., `scram_sha_512`).
/// Vector expects uppercase with hyphens (e.g., `SCRAM-SHA-512`).
fn normalise_sasl_mechanism(mechanism: &str) -> String {
    match mechanism {
        "plain" => "PLAIN".to_string(),
        "scram_sha_256" => "SCRAM-SHA-256".to_string(),
        "scram_sha_512" => "SCRAM-SHA-512".to_string(),
        other => other.to_uppercase().replace('_', "-"),
    }
}

/// Shorthand for creating a YAML string value.
fn val(s: &str) -> Value {
    Value::String(s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        BatchConfig, BufferConfig, SaslConfig, SinkConfig, SourceConfig, TlsConfig,
    };

    #[test]
    fn global_yaml_pins_utc() {
        // Left unset, Vector parses zone-less timestamps in the host's
        // timezone, so the same event transforms differently per pod.
        let global = generate_global_yaml(&crate::config::VectorConfig::default(), &[]);
        assert_eq!(
            global.get("timezone").and_then(Value::as_str),
            Some("UTC"),
            "the assembled config must pin the timezone"
        );
    }

    /// The API takes unauthenticated requests and nothing here is a client,
    /// so the default config must switch it off in so many words.
    #[test]
    fn global_yaml_turns_the_api_off_by_default() {
        let global = generate_global_yaml(&crate::config::VectorConfig::default(), &[]);
        let api = global.get("api").expect("the api block is always written");
        assert_eq!(api.get("enabled").and_then(Value::as_bool), Some(false));
        assert!(
            api.get("address").is_none(),
            "no bind address while the API is off: {api:?}"
        );
    }

    #[test]
    fn global_yaml_binds_the_configured_api_address_when_enabled() {
        let vector = crate::config::VectorConfig {
            api_enabled: true,
            ..Default::default()
        };
        let global = generate_global_yaml(&vector, &[]);
        let api = global.get("api").expect("api block");
        assert_eq!(api.get("enabled").and_then(Value::as_bool), Some(true));
        assert_eq!(
            api.get("address").and_then(Value::as_str),
            Some("127.0.0.1:8686")
        );
    }

    #[test]
    fn global_yaml_declares_each_secret_backend_as_a_directory() {
        let backends = [SecretBackend {
            name: "dfe_source_sasl",
            dir: std::path::PathBuf::from("/var/run/secrets/dfe-kafka"),
        }];
        let global = generate_global_yaml(&crate::config::VectorConfig::default(), &backends);
        let backend = global
            .get("secret")
            .and_then(|s| s.get("dfe_source_sasl"))
            .expect("secret.dfe_source_sasl");
        assert_eq!(
            backend.get("type").and_then(Value::as_str),
            Some("directory")
        );
        assert_eq!(
            backend.get("path").and_then(Value::as_str),
            Some("/var/run/secrets/dfe-kafka")
        );
        assert_eq!(
            backend
                .get("remove_trailing_whitespace")
                .and_then(Value::as_bool),
            Some(true)
        );
    }

    #[test]
    fn source_yaml_production_defaults() {
        let source = SourceConfig {
            brokers: vec!["kafka-1:9092".into(), "kafka-2:9092".into()],
            topics: vec!["raw_events".into()],
            group_id: "test-group".into(),
            ..Default::default()
        };
        let yaml = generate_source_yaml(&source, &BridgeConfig::default());
        let text = serde_yaml_ng::to_string(&yaml).unwrap();

        assert!(text.contains("dfe_source"));
        assert!(text.contains("type: kafka"));
        assert!(text.contains("kafka-1:9092,kafka-2:9092"));
        assert!(text.contains("raw_events"));
        assert!(text.contains("test-group"));
        assert!(text.contains("codec: json"));
        // Production librdkafka defaults (from scalo kafka_config)
        assert!(text.contains("cooperative-sticky"));
        assert!(text.contains("fetch.min.bytes: '1048576'"));
        assert!(text.contains("fetch.wait.max.ms: '100'"));
        assert!(text.contains("queued.min.messages: '20000'"));
        // Overridden off the shared baseline's `false` -- see
        // SERVICE_CONSUMER_OVERRIDES.
        assert!(text.contains("enable.auto.commit: 'true'"));
        assert!(text.contains("statistics.interval.ms: '1000'"));
        // Removed settings — back to librdkafka defaults
        assert!(!text.contains("queued.max.messages.kbytes"));
        assert!(!text.contains("socket.receive.buffer.bytes"));
        assert!(!text.contains("fetch.max.bytes"));
        // Session/commit timing
        assert!(text.contains("session_timeout_ms: 30000"));
        assert!(text.contains("commit_interval_ms: 5000"));
        assert!(text.contains("auto_offset_reset: largest"));
        // Consumer lag metric enabled
        assert!(text.contains("topic_lag_metric: true"));
        // SASL disabled by default — no security.protocol
        assert!(!text.contains("security.protocol"));
    }

    #[test]
    fn source_yaml_with_sasl_injects_security_protocol() {
        let source = SourceConfig {
            brokers: vec!["kafka:9093".into()],
            topics: vec!["topic1".into()],
            group_id: "grp".into(),
            sasl: SaslConfig {
                enabled: true,
                mechanism: "scram_sha_512".into(),
                username: "kafka-user".into(),
                password: "kafka-pass".into(),
                secret_dir: None,
            },
            tls: TlsConfig {
                enabled: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let yaml = generate_source_yaml(&source, &BridgeConfig::default());
        let text = serde_yaml_ng::to_string(&yaml).unwrap();

        assert!(text.contains("SCRAM-SHA-512"));
        assert!(text.contains("security.protocol: SASL_SSL"));
        assert!(text.contains("sasl.mechanism: SCRAM-SHA-512"));
    }

    /// The credential values must never reach the Vector config text: it is
    /// written to disk, re-read on every reload, and Vector echoes config in
    /// its errors. Only the secret references may appear.
    #[test]
    fn sasl_credentials_reach_vector_as_secret_references_only() {
        let credentials = SaslConfig {
            enabled: true,
            username: "kafka-user".into(),
            password: "kafka-pass".into(),
            ..SaslConfig::default()
        };
        let source = SourceConfig {
            sasl: credentials.clone(),
            ..Default::default()
        };
        let sink = SinkConfig {
            sasl: credentials,
            ..Default::default()
        };
        let text =
            serde_yaml_ng::to_string(&generate_source_yaml(&source, &BridgeConfig::default()))
                .unwrap()
                + &serde_yaml_ng::to_string(&generate_sink_yaml(
                    &sink,
                    &BridgeConfig::default(),
                    &["src".into()],
                    true,
                ))
                .unwrap();

        assert!(!text.contains("kafka-pass"), "a password leaked: {text}");
        assert!(!text.contains("kafka-user"), "a username leaked: {text}");
        for reference in [
            "SECRET[dfe_credentials.source_sasl_username]",
            "SECRET[dfe_credentials.source_sasl_password]",
            "SECRET[dfe_credentials.sink_sasl_username]",
            "SECRET[dfe_credentials.sink_sasl_password]",
        ] {
            assert!(text.contains(reference), "{reference} missing: {text}");
        }
    }

    #[test]
    fn a_secret_dir_is_referenced_by_its_own_backend() {
        let source = SourceConfig {
            sasl: SaslConfig {
                enabled: true,
                secret_dir: Some("/var/run/secrets/dfe-kafka".into()),
                ..SaslConfig::default()
            },
            ..Default::default()
        };
        let text =
            serde_yaml_ng::to_string(&generate_source_yaml(&source, &BridgeConfig::default()))
                .unwrap();
        assert!(text.contains("username: SECRET[dfe_source_sasl.username]"));
        assert!(text.contains("password: SECRET[dfe_source_sasl.password]"));
    }

    #[test]
    fn the_producer_is_idempotent() {
        let text = serde_yaml_ng::to_string(&generate_sink_yaml(
            &SinkConfig::default(),
            &BridgeConfig::default(),
            &["src".into()],
            true,
        ))
        .unwrap();
        assert!(
            text.contains("enable.idempotence: 'true'"),
            "an unlimited retry on a non-idempotent producer reorders and duplicates: {text}"
        );
    }

    #[test]
    fn an_operator_can_still_turn_idempotence_off() {
        let mut sink = SinkConfig::default();
        sink.librdkafka_options
            .insert("enable.idempotence".into(), "false".into());
        let text = serde_yaml_ng::to_string(&generate_sink_yaml(
            &sink,
            &BridgeConfig::default(),
            &["src".into()],
            true,
        ))
        .unwrap();
        assert!(text.contains("enable.idempotence: 'false'"));
    }

    /// The Kafka sink reads from the size cap, and the cap from the wired
    /// inputs, with the ceiling taken from `message.max.bytes`.
    #[test]
    fn the_kafka_sink_sits_behind_the_size_cap() {
        let yaml = generate_sink_yaml(
            &SinkConfig::default(),
            &BridgeConfig::default(),
            &["last".into()],
            true,
        );
        let sink_inputs = yaml
            .get("sinks")
            .and_then(|s| s.get(SINK_LABEL))
            .and_then(|s| s.get("inputs"))
            .and_then(Value::as_sequence)
            .expect("sink inputs");
        assert_eq!(sink_inputs, &vec![val(SIZE_CAP_LABEL)]);

        let cap = yaml
            .get("transforms")
            .and_then(|t| t.get(SIZE_CAP_LABEL))
            .expect("the size cap transform");
        assert_eq!(cap.get("type").and_then(Value::as_str), Some("filter"));
        assert_eq!(
            cap.get("inputs").and_then(Value::as_sequence),
            Some(&vec![val("last")])
        );
        assert_eq!(
            cap.get("condition").and_then(Value::as_str),
            Some("length(encode_json(.)) <= 999872")
        );
    }

    #[test]
    fn the_size_cap_follows_message_max_bytes() {
        let mut sink = SinkConfig {
            encoding: "raw_bytes".into(),
            ..Default::default()
        };
        sink.librdkafka_options
            .insert("message.max.bytes".into(), "2000000".into());
        assert_eq!(max_record_bytes(&sink), 1_999_872);
        let yaml = generate_sink_yaml(&sink, &BridgeConfig::default(), &["src".into()], true);
        assert_eq!(
            yaml.get("transforms")
                .and_then(|t| t.get(SIZE_CAP_LABEL))
                .and_then(|c| c.get("condition"))
                .and_then(Value::as_str),
            Some(r#"length(string(.message) ?? "") <= 1999872"#)
        );
    }

    #[test]
    fn the_direct_sink_has_no_size_cap() {
        let sink = SinkConfig {
            transport: crate::config::Transport::Direct,
            ..Default::default()
        };
        let yaml = generate_sink_yaml(&sink, &BridgeConfig::default(), &["src".into()], true);
        assert!(yaml.get("transforms").is_none());
    }

    /// The direct sink dials the supervisor's own bridge, which is bound only
    /// after `vector validate` runs, so its health check would fail startup.
    /// The Kafka sink's check stays: the broker is someone else's.
    #[test]
    fn only_the_direct_sink_skips_its_health_check() {
        let healthcheck = |transport| {
            let sink = SinkConfig {
                transport,
                ..Default::default()
            };
            generate_sink_yaml(&sink, &BridgeConfig::default(), &["src".into()], true)
                .get("sinks")
                .and_then(|s| s.get(SINK_LABEL))
                .and_then(|s| s.get("healthcheck"))
                .and_then(|h| h.get("enabled"))
                .and_then(Value::as_bool)
        };
        assert_eq!(healthcheck(crate::config::Transport::Direct), Some(false));
        assert_eq!(healthcheck(crate::config::Transport::Bus), Some(true));
    }

    /// Vector deprecates acknowledgements set on a source; the direct source
    /// takes them from its sink.
    #[test]
    fn the_direct_source_takes_acknowledgements_from_its_sink() {
        let source = SourceConfig {
            transport: crate::config::Transport::Direct,
            ..Default::default()
        };
        let yaml = generate_source_yaml(&source, &BridgeConfig::default());
        let component = yaml
            .get("sources")
            .and_then(|s| s.get(SOURCE_LABEL))
            .expect("the direct source");
        assert!(component.get("acknowledgements").is_none(), "{component:?}");
    }

    #[test]
    fn acknowledgements_off_reaches_the_sink() {
        let yaml = generate_sink_yaml(
            &SinkConfig::default(),
            &BridgeConfig::default(),
            &["src".into()],
            false,
        );
        assert_eq!(
            yaml.get("sinks")
                .and_then(|s| s.get(SINK_LABEL))
                .and_then(|s| s.get("acknowledgements"))
                .and_then(|a| a.get("enabled"))
                .and_then(Value::as_bool),
            Some(false)
        );
    }

    #[test]
    fn source_yaml_user_librdkafka_overrides() {
        let mut source = SourceConfig::default();
        source
            .librdkafka_options
            .insert("fetch.max.bytes".into(), "52428800".into());
        source
            .librdkafka_options
            .insert("custom.option".into(), "value".into());

        let yaml = generate_source_yaml(&source, &BridgeConfig::default());
        let text = serde_yaml_ng::to_string(&yaml).unwrap();

        // User override replaces default
        assert!(text.contains("fetch.max.bytes: '52428800'"));
        // Custom option included
        assert!(text.contains("custom.option: value"));
    }

    #[test]
    fn source_commits_offsets_and_says_how_often() {
        let source = SourceConfig::default();
        let text =
            serde_yaml_ng::to_string(&generate_source_yaml(&source, &BridgeConfig::default()))
                .unwrap();

        // A commit interval with auto-commit off arms no timer, so the interval
        // is inert and lag stops moving between rebalances.
        assert!(text.contains("enable.auto.commit: 'true'"));
        assert!(text.contains("commit_interval_ms: 5000"));
        assert!(!text.contains("enable.auto.commit: 'false'"));
    }

    #[test]
    fn a_user_can_still_turn_auto_commit_back_off() {
        let mut source = SourceConfig::default();
        source
            .librdkafka_options
            .insert("enable.auto.commit".into(), "false".into());

        let text =
            serde_yaml_ng::to_string(&generate_source_yaml(&source, &BridgeConfig::default()))
                .unwrap();

        assert!(text.contains("enable.auto.commit: 'false'"));
    }

    #[test]
    fn source_yaml_skip_verify_disables_cert_check() {
        let source = SourceConfig {
            tls: TlsConfig {
                enabled: true,
                skip_verify: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let yaml = generate_source_yaml(&source, &BridgeConfig::default());
        let text = serde_yaml_ng::to_string(&yaml).unwrap();

        // Vector TLS block
        assert!(text.contains("verify_certificate: false"));
        assert!(text.contains("verify_hostname: false"));
        // librdkafka option
        assert!(text.contains("enable.ssl.certificate.verification: 'false'"));
    }

    #[test]
    fn sink_yaml_skip_verify_disables_cert_check() {
        let sink = SinkConfig {
            tls: TlsConfig {
                enabled: true,
                skip_verify: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let yaml = generate_sink_yaml(&sink, &BridgeConfig::default(), &["src".into()], true);
        let text = serde_yaml_ng::to_string(&yaml).unwrap();

        // Vector TLS block
        assert!(text.contains("verify_certificate: false"));
        assert!(text.contains("verify_hostname: false"));
        // librdkafka option
        assert!(text.contains("enable.ssl.certificate.verification: 'false'"));
    }

    #[test]
    fn sink_yaml_production_defaults() {
        let sink = SinkConfig {
            brokers: vec!["kafka:9092".into()],
            topic: "output_topic".into(),
            key_field: ".org_id".into(),
            encoding: "json".into(),
            compression: "zstd".into(),
            ..Default::default()
        };
        let yaml = generate_sink_yaml(
            &sink,
            &BridgeConfig::default(),
            &["last_transform".into()],
            true,
        );
        let text = serde_yaml_ng::to_string(&yaml).unwrap();

        assert!(text.contains("dfe_sink"));
        assert!(text.contains("type: kafka"));
        assert!(text.contains("output_topic"));
        assert!(text.contains(".org_id"));
        assert!(text.contains("last_transform"));
        assert!(text.contains("codec: json"));
        assert!(text.contains("compression: zstd"));
        // Production librdkafka defaults (from scalo kafka_config)
        assert!(text.contains("linger.ms: '100'"));
        assert!(text.contains("compression.type: zstd"));
        assert!(text.contains("socket.nagle.disable: 'true'"));
        assert!(text.contains("statistics.interval.ms: '1000'"));
        // Service-specific override: producer queue cap
        assert!(text.contains("queue.buffering.max.kbytes: '262144'"));
        // Removed settings — back to librdkafka defaults
        assert!(!text.contains("batch.size"));
        assert!(!text.contains("batch.num.messages"));
        assert!(!text.contains("message.max.bytes"));
        // Acknowledgements on sink
        assert!(text.contains("acknowledgements:"));
        // Healthcheck
        assert!(text.contains("healthcheck:"));
        // Timeouts
        // No local limit: a broker outage holds the record rather than
        // rejecting it, and a rejected record is dropped.
        assert!(text.contains("message_timeout_ms: 0"));
        assert!(text.contains("socket_timeout_ms: 60000"));
        // Vector batch
        assert!(text.contains("max_events: 10000"));
        assert!(text.contains("timeout_secs: 1"));
    }

    #[test]
    fn sink_yaml_buffer_memory_with_max_events() {
        let sink = SinkConfig {
            buffer: BufferConfig {
                buffer_type: "memory".into(),
                max_events: Some(1000),
                max_size: None,
                when_full: "drop_newest".into(),
            },
            ..Default::default()
        };
        let yaml = generate_sink_yaml(&sink, &BridgeConfig::default(), &["src".into()], true);
        let text = serde_yaml_ng::to_string(&yaml).unwrap();

        assert!(text.contains("type: memory"));
        assert!(text.contains("max_events: 1000"));
        assert!(text.contains("when_full: drop_newest"));
    }

    #[test]
    fn sink_yaml_buffer_disk() {
        let sink = SinkConfig {
            buffer: BufferConfig {
                buffer_type: "disk".into(),
                max_events: None,
                max_size: Some(268_435_488),
                when_full: "block".into(),
            },
            ..Default::default()
        };
        let yaml = generate_sink_yaml(&sink, &BridgeConfig::default(), &["src".into()], true);
        let text = serde_yaml_ng::to_string(&yaml).unwrap();

        assert!(text.contains("type: disk"));
        assert!(text.contains("max_size: 268435488"));
        assert!(text.contains("when_full: block"));
    }

    #[test]
    fn sink_yaml_custom_batch() {
        let sink = SinkConfig {
            batch: BatchConfig {
                max_events: 5000,
                max_bytes: Some(8_388_608),
                timeout_secs: 2,
            },
            ..Default::default()
        };
        let yaml = generate_sink_yaml(&sink, &BridgeConfig::default(), &["src".into()], true);
        let text = serde_yaml_ng::to_string(&yaml).unwrap();

        assert!(text.contains("max_events: 5000"));
        assert!(text.contains("max_bytes: 8388608"));
        assert!(text.contains("timeout_secs: 2"));
    }

    /// The stack's producer compression is zstd, so a sink that names none
    /// compresses with it rather than sending batches raw.
    #[test]
    fn a_sink_that_names_no_compression_uses_zstd() {
        assert_eq!(SinkConfig::default().compression, "zstd");
        let yaml = generate_sink_yaml(
            &SinkConfig::default(),
            &BridgeConfig::default(),
            &["src".into()],
            true,
        );
        let sink = yaml
            .get("sinks")
            .and_then(|s| s.get(SINK_LABEL))
            .expect("the kafka sink");
        assert_eq!(
            sink.get("compression").and_then(Value::as_str),
            Some("zstd")
        );
        assert_eq!(
            sink.get("librdkafka_options")
                .and_then(|o| o.get("compression.type"))
                .and_then(Value::as_str),
            Some("zstd")
        );
    }

    #[test]
    fn sink_yaml_no_compression_sets_none_in_librdkafka() {
        let sink = SinkConfig {
            compression: "none".into(),
            ..Default::default()
        };
        let yaml = generate_sink_yaml(&sink, &BridgeConfig::default(), &["src".into()], true);
        let text = serde_yaml_ng::to_string(&yaml).unwrap();

        // Vector-level compression omitted
        assert!(!text.contains("\n    compression:"));
        // librdkafka compression.type set to none
        assert!(text.contains("compression.type: none"));
    }

    #[test]
    fn observability_yaml() {
        let yaml = generate_observability_yaml("127.0.0.1:9598");
        let text = serde_yaml_ng::to_string(&yaml).unwrap();

        assert!(text.contains("internal_metrics"));
        assert!(text.contains("prometheus_exporter"));
        assert!(text.contains("127.0.0.1:9598"));
    }

    #[test]
    fn observability_yaml_honours_configured_address() {
        let yaml = generate_observability_yaml("127.0.0.1:19598");
        let text = serde_yaml_ng::to_string(&yaml).unwrap();

        assert!(text.contains("127.0.0.1:19598"));
        assert!(!text.contains(":9598"), "hardcoded default leaked: {text}");
    }

    /// The exporter has to land where the config says, or the wrapper scrapes a
    /// port nothing is listening on and the pod reports no Vector metrics.
    #[test]
    fn observability_yaml_binds_the_configured_address() {
        let yaml = generate_observability_yaml("127.0.0.1:19598");
        let address = yaml
            .get("sinks")
            .and_then(|s| s.get("prometheus_exporter"))
            .and_then(|e| e.get("address"))
            .and_then(Value::as_str);
        assert_eq!(
            address,
            Some("127.0.0.1:19598"),
            "metrics.vector_metrics_address must reach the generated exporter"
        );
    }

    #[test]
    fn sasl_mechanism_normalisation() {
        assert_eq!(normalise_sasl_mechanism("plain"), "PLAIN");
        assert_eq!(normalise_sasl_mechanism("scram_sha_256"), "SCRAM-SHA-256");
        assert_eq!(normalise_sasl_mechanism("scram_sha_512"), "SCRAM-SHA-512");
    }

    /// The security floor judges the client Vector runs, so the protocol and
    /// mechanism it is handed must be the ones written into the Vector config.
    #[test]
    fn the_floor_client_matches_the_generated_security_options() {
        for (sasl_on, tls_on) in [(false, false), (true, false), (false, true), (true, true)] {
            let sink = SinkConfig {
                sasl: SaslConfig {
                    enabled: sasl_on,
                    mechanism: "plain".into(),
                    username: "user".into(),
                    ..SaslConfig::default()
                },
                tls: TlsConfig {
                    enabled: tls_on,
                    ..Default::default()
                },
                ..Default::default()
            };
            let yaml = generate_sink_yaml(&sink, &BridgeConfig::default(), &["src".into()], true);
            let written = yaml
                .get("sinks")
                .and_then(|s| s.get(SINK_LABEL))
                .and_then(|s| s.get("librdkafka_options"))
                .expect("the sink's librdkafka_options");
            let client = kafka_client_config(
                &sink.brokers,
                &sink.sasl,
                &sink.tls,
                &sink.librdkafka_options,
            );

            // No protocol written leaves librdkafka on its plaintext default.
            assert_eq!(
                client.security_protocol,
                written
                    .get("security.protocol")
                    .and_then(Value::as_str)
                    .unwrap_or("PLAINTEXT"),
                "sasl={sasl_on} tls={tls_on}"
            );
            assert_eq!(
                client.sasl_mechanism.as_deref(),
                written.get("sasl.mechanism").and_then(Value::as_str),
                "sasl={sasl_on} tls={tls_on}"
            );
        }
    }

    /// A scalo release that judges raw librdkafka maps reads them from
    /// `librdkafka_overrides`, so every raw option has to arrive there as set.
    #[test]
    fn the_floor_client_carries_the_raw_options_as_overrides() {
        let options: BTreeMap<String, String> = [
            ("ssl.endpoint.identification.algorithm", "none"),
            ("enable.ssl.certificate.verification", "false"),
            ("fetch.max.bytes", "52428800"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let tls = TlsConfig {
            enabled: true,
            skip_verify: true,
            ..Default::default()
        };
        let brokers = vec!["kafka-1:9093".to_string()];

        let client = kafka_client_config(&brokers, &SaslConfig::default(), &tls, &options);

        assert_eq!(client.brokers, brokers);
        assert!(client.ssl_skip_verify);
        assert_eq!(client.librdkafka_overrides.len(), options.len());
        for (key, value) in &options {
            assert_eq!(
                client.librdkafka_overrides.get(key),
                Some(value),
                "{key} did not reach librdkafka_overrides"
            );
        }
    }
}
