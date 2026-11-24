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
//! `librdkafka_options` from config YAML are merged on top. The sink's
//! `message.max.bytes`, where no layer sets it, is scalo's pipeline-wide record
//! ceiling.

use std::collections::{BTreeMap, HashMap};

use scalo::dlq::{DEFAULT_MAX_PAYLOAD_BYTES, DlqConfig};
use scalo::transport::kafka::MESSAGE_MAX_BYTES;
use serde_json::{Map, Number, Value};
use tracing::warn;

/// A YAML mapping in the generated Vector config.
type Mapping = Map<String, Value>;

use super::kafka_defaults;
use super::loader::{
    BatchConfig, BridgeConfig, BufferConfig, SaslConfig, SinkConfig, SourceConfig, TlsConfig,
    VectorConfig,
};
use super::secrets::{SecretBackend, SecretNames, Side};

/// Canonical label for the generated source component.
pub const SOURCE_LABEL: &str = "dfe_source";

/// Canonical label for the generated sink component.
pub const SINK_LABEL: &str = "dfe_sink";

/// Label of the transform that keeps records over the producer's size ceiling
/// off the Kafka sink: a `route` whose unmatched output is dead-lettered, or a
/// `filter` that drops them while `dlq.enabled` is false.
pub const SIZE_CAP_LABEL: &str = "dfe_size_cap";

/// Label of the remap that turns a record over the ceiling into a DLQ entry.
pub const SIZE_CAP_DLQ_LABEL: &str = "dfe_size_cap_dlq";

/// Label of the Kafka sink that writes those entries to `sink.dlq_topic`.
pub const DLQ_SINK_LABEL: &str = "dfe_dlq_sink";

/// The size cap route's output for a record that fits.
const WITHIN_CAP_OUTPUT: &str = "within_cap";

/// Vector's output of a `route` for the events no route matched.
const UNMATCHED_OUTPUT: &str = "_unmatched";

/// `service` in a dead-letter entry, as scalo's DLQ names its producer.
const DLQ_SERVICE: &str = env!("CARGO_PKG_NAME");

/// The common-header fields scalo's DLQ copies from the original event beside
/// its payload.
const DLQ_HEADER_FIELDS: [&str; 4] = ["_org_id", "_source", "_timestamp_received", "_tags"];

/// Bytes kept back from a dead letter's Kafka ceiling for its envelope, the
/// key and the record framing.
const DLQ_ENVELOPE_BYTES: u64 = 8192;

/// Events the dead letter sink's memory buffer holds before it blocks.
const DLQ_BUFFER_EVENTS: u64 = 500;

/// Vector's `type` for a Kafka source or sink.
pub const KAFKA_COMPONENT_TYPE: &str = "kafka";

/// librdkafka's broker hostname check, off when set to `none`.
const HOSTNAME_VERIFICATION_KEY: &str = "ssl.endpoint.identification.algorithm";

/// librdkafka's producer key for the largest record it puts on the wire.
const MESSAGE_MAX_BYTES_KEY: &str = "message.max.bytes";

/// Room left under `message.max.bytes` for the key, headers and record framing.
const RECORD_FRAMING_BYTES: u64 = 128;

/// The direct sink's batch byte ceiling where `sink.batch.max_bytes` sets none:
/// half the 16 MiB the return listener decodes, so a push of many records still
/// fits once encoded.
pub const DIRECT_BATCH_MAX_BYTES: u64 = 8 * 1024 * 1024;

/// Generate Vector global config YAML (timezone, data_dir, API, secret backends).
///
/// `secrets` are the SASL credential sources, each one `directory` entry under
/// `secret`.
#[must_use]
pub fn generate_global_yaml(vector: &VectorConfig, secrets: &[SecretBackend]) -> Value {
    let mut root = Mapping::new();

    // Vector's default is the HOST's timezone, so a transform that parses a
    // zone-less timestamp would produce a different instant on every pod. DFE
    // events are UTC end to end.
    root.insert(key("timezone"), val("UTC"));

    if !vector.data_dir.is_empty() {
        root.insert(key("data_dir"), val(&vector.data_dir));
    }

    // Written out even when off, so a change of Vector's own default cannot
    // open an unauthenticated listener.
    let mut api = Mapping::new();
    api.insert(key("enabled"), Value::Bool(vector.api_enabled));
    if vector.api_enabled && !vector.api_address.is_empty() {
        api.insert(key("address"), val(&vector.api_address));
    }
    root.insert(key("api"), Value::Object(api));

    let mut backends = Mapping::new();
    for secret in secrets {
        let mut backend = Mapping::new();
        backend.insert(key("type"), val("directory"));
        backend.insert(key("path"), val(&secret.dir.to_string_lossy()));
        // A mounted Secret written by hand often ends in a newline, which
        // would otherwise become part of the credential.
        backend.insert(key("remove_trailing_whitespace"), Value::Bool(true));
        backends.insert(key(secret.name), Value::Object(backend));
    }
    if !backends.is_empty() {
        root.insert(key("secret"), Value::Object(backends));
    }

    Value::Object(root)
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
fn direct_source_component(bridge: &BridgeConfig) -> Mapping {
    let mut component = Mapping::new();
    component.insert(key("type"), val("vector"));
    component.insert(key("address"), val(&bridge.to_vector));
    component
}

/// An `{enabled: <on>}` block.
fn enabled_block(on: bool) -> Value {
    let mut block = Mapping::new();
    block.insert(key("enabled"), Value::Bool(on));
    Value::Object(block)
}

fn kafka_source_component(source: &SourceConfig) -> Mapping {
    let mut component = Mapping::new();
    component.insert(key("type"), val(KAFKA_COMPONENT_TYPE));
    component.insert(key("bootstrap_servers"), val(&source.brokers.join(",")));
    component.insert(
        key("topics"),
        Value::Array(source.topics.iter().map(|t| val(t)).collect()),
    );
    component.insert(key("group_id"), val(&source.group_id));

    // Decoding
    let mut decoding = Mapping::new();
    decoding.insert(key("codec"), val(&source.decoding.codec));
    component.insert(key("decoding"), Value::Object(decoding));

    // Session and commit timing
    component.insert(
        key("session_timeout_ms"),
        Value::Number(source.session_timeout_ms.into()),
    );
    component.insert(
        key("commit_interval_ms"),
        Value::Number(source.commit_interval_ms.into()),
    );
    if let Some(drain_ms) = source.drain_timeout_ms {
        component.insert(key("drain_timeout_ms"), Value::Number(drain_ms.into()));
    }

    // Offset reset
    component.insert(key("auto_offset_reset"), val(&source.auto_offset_reset));

    // Consumer lag metric
    if source.topic_lag_metric {
        let mut metrics = Mapping::new();
        metrics.insert(key("topic_lag_metric"), Value::Bool(true));
        component.insert(key("metrics"), Value::Object(metrics));
    }

    // SASL
    if source.sasl.enabled {
        component.insert(key("sasl"), build_sasl_block(Side::Source, &source.sasl));
    }

    // TLS
    if source.tls.enabled {
        component.insert(key("tls"), build_tls_block(&source.tls));
    }

    // librdkafka options — production defaults + user overrides
    let rdkafka = build_source_librdkafka(source);
    component.insert(key("librdkafka_options"), Value::Object(rdkafka));

    component
}

/// Wrap one component under its section and label, the shape every generated
/// file takes.
fn wrap(name: &str, label: &str, component: Mapping) -> Value {
    Value::Object(section(name, label, component))
}

/// `{<name>: {<label>: <component>}}`.
fn section(name: &str, label: &str, component: Mapping) -> Mapping {
    let mut components = Mapping::new();
    components.insert(key(label), Value::Object(component));

    let mut root = Mapping::new();
    root.insert(key(name), Value::Object(components));
    root
}

/// Generate the `sinks.dfe_sink` YAML for this deployment's transport.
///
/// On the bus that is a Kafka producer with production-tuned librdkafka
/// options, Vector-level batching, buffer config and acknowledgements, behind
/// a size cap. A record over the producer's ceiling goes through a remap into
/// a DLQ entry and a second Kafka sink on `sink.dlq_topic`, or is dropped while
/// `dlq.enabled` is false. On direct it is Vector's own `vector` sink,
/// dialling the supervisor's bridge, which pushes the records on to
/// `sink.endpoint`.
///
/// `acknowledged` is `source.acknowledgements.enabled`: the sink's end-to-end
/// acknowledgement is what lets the source hold its own. `dlq` is the app's
/// `dlq` block, whose `enabled` and `max_payload_bytes` the size cap reads.
#[must_use]
pub fn generate_sink_yaml(
    sink: &SinkConfig,
    bridge: &BridgeConfig,
    inputs: &[String],
    acknowledged: bool,
    dlq: &DlqConfig,
) -> Value {
    if sink.transport.is_direct() {
        return wrap(
            "sinks",
            SINK_LABEL,
            direct_sink_component(sink, bridge, inputs, acknowledged),
        );
    }

    let dead_lettering = dlq.enabled;
    let within_cap = if dead_lettering {
        format!("{SIZE_CAP_LABEL}.{WITHIN_CAP_OUTPUT}")
    } else {
        SIZE_CAP_LABEL.to_string()
    };

    let mut sinks = Mapping::new();
    sinks.insert(
        key(SINK_LABEL),
        Value::Object(kafka_sink_component(sink, &[within_cap], acknowledged)),
    );
    let mut transforms = Mapping::new();
    if dead_lettering {
        transforms.insert(
            key(SIZE_CAP_LABEL),
            Value::Object(size_cap_route_component(sink, inputs)),
        );
        transforms.insert(
            key(SIZE_CAP_DLQ_LABEL),
            Value::Object(dead_letter_remap_component(sink, dlq)),
        );
        sinks.insert(
            key(DLQ_SINK_LABEL),
            Value::Object(dead_letter_sink_component(sink, acknowledged)),
        );
    } else {
        transforms.insert(
            key(SIZE_CAP_LABEL),
            Value::Object(size_cap_filter_component(sink, inputs)),
        );
    }

    let mut root = Mapping::new();
    root.insert(key("sinks"), Value::Object(sinks));
    root.insert(key("transforms"), Value::Object(transforms));
    Value::Object(root)
}

/// The largest record, in encoded bytes, the Kafka sink is handed.
///
/// librdkafka refuses a record over `message.max.bytes`, and Vector drops a
/// refused record. The key and headers count against the same limit, so a
/// margin is kept below it. The ceiling is read from the options the sink is
/// generated with, so the size cap and the producer cannot disagree.
#[must_use]
pub fn max_record_bytes(sink: &SinkConfig) -> u64 {
    sink_librdkafka_layers(sink)
        .get(MESSAGE_MAX_BYTES_KEY)
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or_else(pipeline_message_max_bytes)
        .saturating_sub(RECORD_FRAMING_BYTES)
}

/// scalo's pipeline-wide record ceiling, which every other stage's producer
/// and every topic scalo creates carry.
fn pipeline_message_max_bytes() -> u64 {
    u64::from(MESSAGE_MAX_BYTES.unsigned_abs())
}

/// The VRL expression for a record as the sink's codec encodes it: the JSON of
/// the whole event, or the `message` field for `raw_bytes`.
fn encoded_record(sink: &SinkConfig) -> &'static str {
    match sink.encoding.as_str() {
        "raw_bytes" => r#"string(.message) ?? """#,
        _ => "encode_json(.)",
    }
}

/// The VRL condition that holds for a record within [`max_record_bytes`].
fn within_cap_condition(sink: &SinkConfig) -> String {
    format!(
        "length({}) <= {}",
        encoded_record(sink),
        max_record_bytes(sink)
    )
}

/// The route in front of the Kafka sink. A record over [`max_record_bytes`]
/// leaves on `_unmatched`, which [`dead_letter_remap_component`] reads.
fn size_cap_route_component(sink: &SinkConfig, inputs: &[String]) -> Mapping {
    let mut routes = Mapping::new();
    routes.insert(key(WITHIN_CAP_OUTPUT), val(&within_cap_condition(sink)));

    let mut component = Mapping::new();
    component.insert(key("type"), val("route"));
    component.insert(key("inputs"), input_list(inputs));
    component.insert(key("route"), Value::Object(routes));
    component
}

/// The filter in front of the Kafka sink while `dlq.enabled` is false: it drops
/// a record over [`max_record_bytes`], counted in
/// `component_discarded_events_total`.
fn size_cap_filter_component(sink: &SinkConfig, inputs: &[String]) -> Mapping {
    let mut component = Mapping::new();
    component.insert(key("type"), val("filter"));
    component.insert(key("inputs"), input_list(inputs));
    component.insert(key("condition"), val(&within_cap_condition(sink)));
    component
}

/// The most bytes of a dead letter's payload and copied header: `dlq`'s
/// `max_payload_bytes`, held to what the sink's Kafka ceiling takes once the
/// payload is base64.
#[must_use]
pub fn dead_letter_payload_cap(sink: &SinkConfig, dlq: &DlqConfig) -> u64 {
    let configured = dlq
        .max_payload_bytes
        .map_or(DEFAULT_MAX_PAYLOAD_BYTES as u64, |n| n as u64);
    let kafka_fit = max_record_bytes(sink).saturating_sub(DLQ_ENVELOPE_BYTES) / 2;
    configured.min(kafka_fit)
}

/// A VRL string literal for `text`.
fn vrl_string(text: &str) -> String {
    let mut literal = String::with_capacity(text.len() + 2);
    literal.push('"');
    for c in text.chars() {
        match c {
            '\\' => literal.push_str("\\\\"),
            '"' => literal.push_str("\\\""),
            '\n' => literal.push_str("\\n"),
            '\r' => literal.push_str("\\r"),
            '\t' => literal.push_str("\\t"),
            other => literal.push(other),
        }
    }
    literal.push('"');
    literal
}

/// The VRL that builds a scalo DLQ entry from a record over the ceiling.
///
/// The entry is `truncated`: the record's first bytes in `_raw` as base64, its
/// length in `original_bytes`, and the header fields scalo copies beside it. A
/// header value that does not fit in the cap with the others is left out and
/// named in `header_dropped`.
fn dead_letter_source(sink: &SinkConfig, dlq: &DlqConfig) -> String {
    let cap = dead_letter_payload_cap(sink, dlq);
    let mut program = format!(
        "body = {}\noriginal = length(body)\nheader = {{}}\nheader_bytes = 0\nheader_dropped = []\n",
        encoded_record(sink),
    );
    for field in DLQ_HEADER_FIELDS {
        program.push_str(&format!(
            "if !is_null(.{field}) {{\n  size = length(encode_json(.{field}))\n  \
             if header_bytes + size <= {cap} {{\n    header.{field} = .{field}\n    \
             header_bytes = header_bytes + size\n  }} else {{\n    \
             header_dropped = push(header_dropped, \"{field}\")\n  }}\n}}\n"
        ));
    }
    program.push_str(&format!(
        "head = slice!(body, 0, {cap} - header_bytes)\n\
         . = header\n\
         .timestamp = to_unix_timestamp(now(), unit: \"milliseconds\")\n\
         .service = {service}\n\
         .reason = \"record of \" + to_string(original) + \" bytes is over the sink's {ceiling}-byte ceiling\"\n\
         .attempts = 0\n\
         .truncated = true\n\
         .original_bytes = original\n\
         .destination = {destination}\n\
         ._raw = encode_base64(head)\n\
         ._raw_encoding = \"base64\"\n\
         if length(header_dropped) > 0 {{\n  .header_dropped = header_dropped\n}}\n",
        service = vrl_string(DLQ_SERVICE),
        ceiling = max_record_bytes(sink),
        destination = vrl_string(&sink.topic),
    ));
    program
}

/// The remap that turns what the size cap route left unmatched into a dead
/// letter entry.
fn dead_letter_remap_component(sink: &SinkConfig, dlq: &DlqConfig) -> Mapping {
    let mut component = Mapping::new();
    component.insert(key("type"), val("remap"));
    component.insert(
        key("inputs"),
        input_list(&[format!("{SIZE_CAP_LABEL}.{UNMATCHED_OUTPUT}")]),
    );
    component.insert(key("source"), val(&dead_letter_source(sink, dlq)));
    component
}

/// The Kafka sink for dead letter entries.
///
/// It takes the main sink's brokers, credentials, compression and librdkafka
/// options, and writes `sink.dlq_topic` as JSON with no partition key, as
/// scalo's DLQ does. Its buffer holds in memory and blocks when full: an entry
/// the broker cannot take yet back-pressures the source rather than drops.
fn dead_letter_sink_component(sink: &SinkConfig, acknowledged: bool) -> Mapping {
    let dead_letters = SinkConfig {
        topic: sink.dlq_topic.clone(),
        key_field: String::new(),
        encoding: "json".to_string(),
        buffer: BufferConfig {
            buffer_type: "memory".to_string(),
            max_events: Some(DLQ_BUFFER_EVENTS),
            max_size: None,
            when_full: "block".to_string(),
        },
        ..sink.clone()
    };
    kafka_sink_component(
        &dead_letters,
        &[SIZE_CAP_DLQ_LABEL.to_string()],
        acknowledged,
    )
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
) -> Mapping {
    let mut component = Mapping::new();
    component.insert(key("type"), val("vector"));
    component.insert(key("inputs"), input_list(inputs));
    // A URI, not a bind address: this end dials the supervisor's listener.
    component.insert(
        key("address"),
        val(&format!("http://{}", bridge.from_vector)),
    );
    component.insert(key("acknowledgements"), enabled_block(acknowledged));
    // The bridge is this process's own listener, bound only after `vector
    // validate` runs, and its health is the supervisor's readiness.
    component.insert(key("healthcheck"), enabled_block(false));
    component.insert(key("buffer"), build_buffer_block(&sink.buffer));
    component.insert(
        key("batch"),
        build_batch_block(
            &sink.batch,
            Some(sink.batch.max_bytes.unwrap_or(DIRECT_BATCH_MAX_BYTES)),
            sink.batch.timeout_secs.into(),
        ),
    );

    component
}

fn kafka_sink_component(sink: &SinkConfig, inputs: &[String], acknowledged: bool) -> Mapping {
    let mut component = Mapping::new();
    component.insert(key("type"), val(KAFKA_COMPONENT_TYPE));
    component.insert(key("inputs"), input_list(inputs));
    component.insert(key("bootstrap_servers"), val(&sink.brokers.join(",")));
    component.insert(key("topic"), val(&sink.topic));

    // Key field (partition key)
    if !sink.key_field.is_empty() {
        component.insert(key("key_field"), val(&sink.key_field));
    }

    // Encoding
    let mut encoding = Mapping::new();
    encoding.insert(key("codec"), val(&sink.encoding));
    component.insert(key("encoding"), Value::Object(encoding));

    // Compression. Vector writes this as `compression.codec`, the only
    // statement of it: librdkafka reads `compression.type` as the same setting,
    // so `build_sink_librdkafka` keeps that spelling out.
    component.insert(key("compression"), val(&sink.compression));

    // Acknowledgements — on the sink (not source) per Vector 0.53+ guidance
    component.insert(key("acknowledgements"), enabled_block(acknowledged));

    // Healthcheck
    component.insert(key("healthcheck"), enabled_block(true));

    // Timeouts
    component.insert(
        key("message_timeout_ms"),
        Value::Number(sink.message_timeout_ms.into()),
    );
    component.insert(
        key("socket_timeout_ms"),
        Value::Number(sink.socket_timeout_ms.into()),
    );

    // librdkafka options — production defaults + user overrides
    let mut rdkafka = build_sink_librdkafka(sink);

    // Vector always writes `batch.timeout_secs` as `queue.buffering.max.ms`,
    // which librdkafka reads as `linger.ms`. A layer that names the delay
    // moves into that field, so the sink states it once.
    let timeout_secs = match take_batching_delay(&mut rdkafka) {
        Some(layered) => {
            if sink.batch.timeout_secs != BatchConfig::default().timeout_secs {
                warn!(
                    sink_batch_timeout_secs = sink.batch.timeout_secs,
                    layered_delay_secs = %layered,
                    "sink.batch.timeout_secs is ignored: a librdkafka layer sets the batching delay"
                );
            }
            layered
        }
        None => sink.batch.timeout_secs.into(),
    };
    component.insert(
        key("batch"),
        build_batch_block(&sink.batch, sink.batch.max_bytes, timeout_secs),
    );

    // SASL
    if sink.sasl.enabled {
        component.insert(key("sasl"), build_sasl_block(Side::Sink, &sink.sasl));
    }

    // TLS
    if sink.tls.enabled {
        component.insert(key("tls"), build_tls_block(&sink.tls));
    }

    // Buffer
    component.insert(key("buffer"), build_buffer_block(&sink.buffer));

    component.insert(key("librdkafka_options"), Value::Object(rdkafka));

    component
}

/// Remove the batching delay from a sink's librdkafka options and return it in
/// seconds, the unit of Vector's `batch.timeout_secs`.
///
/// `None`, with the options untouched, when no layer names it or the value is
/// not a number of milliseconds. librdkafka refuses such a value at startup.
fn take_batching_delay(rdkafka: &mut Mapping) -> Option<Number> {
    let name = ["linger.ms", "queue.buffering.max.ms"]
        .into_iter()
        .find(|name| rdkafka.contains_key(*name))?;
    let millis: f64 = rdkafka.get(name)?.as_str()?.trim().parse().ok()?;
    if !millis.is_finite() || millis < 0.0 {
        return None;
    }
    let seconds = Number::from_f64(millis / 1000.0)?;
    rdkafka.remove("linger.ms");
    rdkafka.remove("queue.buffering.max.ms");
    Some(seconds)
}

/// A sink's Vector-level `batch` block, with `max_bytes` written when given.
fn build_batch_block(batch: &BatchConfig, max_bytes: Option<u64>, timeout_secs: Number) -> Value {
    let mut block = Mapping::new();
    block.insert(key("max_events"), Value::Number(batch.max_events.into()));
    block.insert(key("timeout_secs"), Value::Number(timeout_secs));
    if let Some(max_bytes) = max_bytes {
        block.insert(key("max_bytes"), Value::Number(max_bytes.into()));
    }
    Value::Object(block)
}

/// A component's wired `inputs` list.
fn input_list(inputs: &[String]) -> Value {
    Value::Array(inputs.iter().map(|i| val(i)).collect())
}

/// Generate Vector observability YAML (internal_metrics + prometheus_exporter).
///
/// `address` is `metrics.vector_metrics_address`; the wrapper scrapes the same
/// address and merges the samples into the scalo registry, so it stays on
/// loopback rather than being published. Hard-coding it here instead left that
/// config key accepted, validated and inert.
#[must_use]
pub fn generate_observability_yaml(address: &str) -> Value {
    let mut metrics_source = Mapping::new();
    metrics_source.insert(key("type"), val("internal_metrics"));

    let mut sources = Mapping::new();
    sources.insert(key("internal_metrics"), Value::Object(metrics_source));

    let mut prom_sink = Mapping::new();
    prom_sink.insert(key("type"), val("prometheus_exporter"));
    prom_sink.insert(key("inputs"), Value::Array(vec![val("internal_metrics")]));
    prom_sink.insert(key("address"), val(address));

    let mut sinks = Mapping::new();
    sinks.insert(key("prometheus_exporter"), Value::Object(prom_sink));

    let mut root = Mapping::new();
    root.insert(key("sources"), Value::Object(sources));
    root.insert(key("sinks"), Value::Object(sinks));

    Value::Object(root)
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
/// `compression.type` and `compression.codec`, librdkafka's two spellings of
/// one setting, are unconditional: the sink always writes the codec from
/// `sink.compression`.
#[must_use]
pub fn derived_sink_options(sink: &SinkConfig) -> Vec<(&'static str, &'static str)> {
    let mut derived = vec![
        ("compression.type", "sink.compression"),
        ("compression.codec", "sink.compression"),
    ];
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
/// DFE baseline turns it off -- see docs/librdkafka.md, Service-Specific
/// Overrides.
const SERVICE_CONSUMER_OVERRIDES: &[(&str, &str)] = &[("enable.auto.commit", "true")];

/// Service-specific producer librdkafka overrides for transform-vector.
///
/// - `queue.buffering.max.kbytes`: caps the local producer queue to 256 MiB
///   (librdkafka default is 1 GiB), which is too large for a transform-vector
///   pod.
/// - `enable.idempotence`: the sink retries without limit, and a retry on a
///   non-idempotent producer can reorder and duplicate records within a
///   partition. librdkafka then sets `acks=all` and at most 5 requests in
///   flight itself.
const SERVICE_PRODUCER_OVERRIDES: &[(&str, &str)] = &[
    ("queue.buffering.max.kbytes", "262144"),
    ("enable.idempotence", "true"),
];

/// The Kafka source's librdkafka options from every layer: the baseline
/// profile, the service overrides, then the source's `librdkafka_options`.
///
/// The security options written after these are derived from `source.sasl`
/// and `source.tls`, which [`crate::config::Config::validate`] keeps out of
/// `librdkafka_options`.
#[must_use]
pub fn source_librdkafka_layers(source: &SourceConfig) -> HashMap<String, String> {
    let base = kafka_defaults::consumer_profile("production");
    let user_overrides: HashMap<String, String> = source
        .librdkafka_options
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    kafka_defaults::merge_layers(&base, SERVICE_CONSUMER_OVERRIDES, &user_overrides)
}

fn build_source_librdkafka(source: &SourceConfig) -> Mapping {
    let mut m = Mapping::new();
    for (k, v) in &source_librdkafka_layers(source) {
        m.insert(key(k), val(v));
    }
    insert_security_options(&mut m, &source.sasl, &source.tls);
    m
}

/// The Kafka sink's librdkafka options from every layer: the baseline profile,
/// the service overrides, then the sink's `librdkafka_options`.
///
/// `message.max.bytes` takes scalo's pipeline-wide ceiling where no layer sets
/// it: librdkafka's own default of 1,000,000 bytes refuses a record every other
/// stage carries.
fn sink_librdkafka_layers(sink: &SinkConfig) -> HashMap<String, String> {
    let base = kafka_defaults::producer_profile("production");
    let user_overrides: HashMap<String, String> = sink
        .librdkafka_options
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let mut merged =
        kafka_defaults::merge_layers(&base, SERVICE_PRODUCER_OVERRIDES, &user_overrides);
    merged
        .entry(MESSAGE_MAX_BYTES_KEY.to_string())
        .or_insert_with(|| pipeline_message_max_bytes().to_string());
    merged
}

fn build_sink_librdkafka(sink: &SinkConfig) -> Mapping {
    let mut m = Mapping::new();
    for (k, v) in &sink_librdkafka_layers(sink) {
        m.insert(key(k), val(v));
    }

    // The codec comes from `sink.compression` through Vector's own field, so a
    // profile's spelling of it is dropped.
    m.remove("compression.type");
    m.remove("compression.codec");
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
fn insert_security_options(m: &mut Mapping, sasl: &SaslConfig, tls: &TlsConfig) {
    if let Some(protocol) = security_protocol(sasl, tls) {
        m.insert(key("security.protocol"), val(protocol));
    }
    if let Some(mechanism) = sasl_mechanism(sasl) {
        m.insert(key("sasl.mechanism"), val(&mechanism));
    }
    if verification_off(tls) {
        m.insert(key("enable.ssl.certificate.verification"), val("false"));
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

/// The scalo Kafka client config equivalent to a Kafka source or sink written
/// in Vector's own shape, as a user transform file may declare one.
///
/// Reads the fields the generator writes for its own Kafka components:
/// `bootstrap_servers`, `sasl.enabled`, `sasl.mechanism`, `tls.enabled`,
/// `tls.verify_certificate`, `tls.verify_hostname` and `librdkafka_options`.
/// An absent flag reads as off, as Vector reads it.
#[must_use]
pub fn vector_kafka_client_config(component: &Value) -> scalo::transport::KafkaConfig {
    let sasl_block = component.get("sasl");
    let tls_block = component.get("tls");
    let flag =
        |block: Option<&Value>, key: &str| block.and_then(|b| b.get(key)).and_then(Value::as_bool);

    let brokers: Vec<String> = component
        .get("bootstrap_servers")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|b| !b.is_empty())
        .map(String::from)
        .collect();
    let sasl = SaslConfig {
        enabled: flag(sasl_block, "enabled").unwrap_or(false),
        mechanism: sasl_block
            .and_then(|s| s.get("mechanism"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        ..SaslConfig::default()
    };
    let tls = TlsConfig {
        enabled: flag(tls_block, "enabled").unwrap_or(false),
        skip_verify: flag(tls_block, "verify_certificate") == Some(false),
        ..TlsConfig::default()
    };
    let options: BTreeMap<String, String> = component
        .get("librdkafka_options")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
        .filter_map(|(k, v)| Some((k.clone(), scalar_text(v)?)))
        .collect();

    let mut client = kafka_client_config(&brokers, &sasl, &tls, &options);
    // scalo judges hostname verification from the raw map alone.
    if flag(tls_block, "verify_hostname") == Some(false) {
        client
            .librdkafka_overrides
            .insert(HOSTNAME_VERIFICATION_KEY.to_string(), "none".to_string());
    }
    client
}

/// Judge one Kafka client against scalo's Kafka security floor, with
/// production read by `scalo::env::is_production`.
///
/// # Errors
///
/// scalo's reason, when the floor refuses the client.
pub fn kafka_floor(client: &scalo::transport::KafkaConfig) -> Result<(), String> {
    client.validate(scalo::env::is_production())
}

/// A YAML scalar as the text librdkafka reads, or `None` for anything else.
fn scalar_text(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// The component's `sasl` block, naming its credentials by `SECRET[...]`
/// reference so no credential is written into the Vector config.
fn build_sasl_block(side: Side, sasl: &super::loader::SaslConfig) -> Value {
    let secret = SecretNames::of(side, sasl);
    let mut m = Mapping::new();
    m.insert(key("enabled"), Value::Bool(true));
    m.insert(
        key("mechanism"),
        val(&normalise_sasl_mechanism(&sasl.mechanism)),
    );
    m.insert(key("username"), val(&secret.username_ref()));
    m.insert(key("password"), val(&secret.password_ref()));
    Value::Object(m)
}

fn build_tls_block(tls: &super::loader::TlsConfig) -> Value {
    let mut m = Mapping::new();
    m.insert(key("enabled"), Value::Bool(true));

    if let Some(ref ca) = tls.ca_cert_file {
        m.insert(key("ca_file"), val(ca));
    }
    if let Some(ref cert) = tls.cert_file {
        m.insert(key("crt_file"), val(cert));
    }
    if let Some(ref key_file) = tls.key_file {
        m.insert(key("key_file"), val(key_file));
    }
    if tls.skip_verify {
        m.insert(key("verify_certificate"), Value::Bool(false));
        m.insert(key("verify_hostname"), Value::Bool(false));
    }

    Value::Object(m)
}

fn build_buffer_block(buffer: &BufferConfig) -> Value {
    let mut m = Mapping::new();
    m.insert(key("type"), val(&buffer.buffer_type));

    match buffer.buffer_type.as_str() {
        "memory" => {
            if let Some(max_events) = buffer.max_events {
                m.insert(key("max_events"), Value::Number(max_events.into()));
            }
        }
        "disk" => {
            if let Some(max_size) = buffer.max_size {
                m.insert(key("max_size"), Value::Number(max_size.into()));
            }
        }
        _ => {}
    }

    m.insert(key("when_full"), val(&buffer.when_full));
    Value::Object(m)
}

/// Normalise SASL mechanism from config format to Vector's expected format.
///
/// Config takes `scram_sha_512` or librdkafka's `SCRAM-SHA-512`, and Vector
/// expects the latter. A name this app does not authenticate with, as a user's
/// own Vector component may carry, is upper-cased with hyphens.
fn normalise_sasl_mechanism(mechanism: &str) -> String {
    super::loader::SaslMechanism::parse(mechanism).map_or_else(
        || mechanism.to_uppercase().replace('_', "-"),
        |known| known.as_librdkafka().to_string(),
    )
}

/// Shorthand for creating a YAML string value.
fn val(s: &str) -> Value {
    Value::String(s.to_string())
}

/// Shorthand for a mapping key.
fn key(s: &str) -> String {
    s.to_string()
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
        let text = scalo::yaml::to_string(&yaml).unwrap();

        assert!(text.contains("dfe_source"));
        assert!(text.contains("type: kafka"));
        assert!(text.contains("kafka-1:9092,kafka-2:9092"));
        assert!(text.contains("raw_events"));
        assert!(text.contains("test-group"));
        assert!(text.contains("codec: json"));
        // Production librdkafka defaults (from scalo kafka_config)
        assert!(text.contains("cooperative-sticky"));
        assert!(text.contains("fetch.min.bytes: \"1048576\""));
        assert!(text.contains("fetch.wait.max.ms: \"100\""));
        assert!(text.contains("queued.min.messages: \"20000\""));
        // Overridden off the shared baseline's `false` -- see
        // SERVICE_CONSUMER_OVERRIDES.
        assert!(text.contains("enable.auto.commit: \"true\""));
        assert!(text.contains("statistics.interval.ms: \"1000\""));
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
        let text = scalo::yaml::to_string(&yaml).unwrap();

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
        let text = scalo::yaml::to_string(&generate_source_yaml(&source, &BridgeConfig::default()))
            .unwrap()
            + &scalo::yaml::to_string(&generate_sink_yaml(
                &sink,
                &BridgeConfig::default(),
                &["src".into()],
                true,
                &scalo::dlq::DlqConfig::default(),
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
        let text = scalo::yaml::to_string(&generate_source_yaml(&source, &BridgeConfig::default()))
            .unwrap();
        assert!(text.contains("username: SECRET[dfe_source_sasl.username]"));
        assert!(text.contains("password: SECRET[dfe_source_sasl.password]"));
    }

    #[test]
    fn the_producer_is_idempotent() {
        let text = scalo::yaml::to_string(&generate_sink_yaml(
            &SinkConfig::default(),
            &BridgeConfig::default(),
            &["src".into()],
            true,
            &scalo::dlq::DlqConfig::default(),
        ))
        .unwrap();
        assert!(
            text.contains("enable.idempotence: \"true\""),
            "an unlimited retry on a non-idempotent producer reorders and duplicates: {text}"
        );
    }

    #[test]
    fn an_operator_can_still_turn_idempotence_off() {
        let mut sink = SinkConfig::default();
        sink.librdkafka_options
            .insert("enable.idempotence".into(), "false".into());
        let text = scalo::yaml::to_string(&generate_sink_yaml(
            &sink,
            &BridgeConfig::default(),
            &["src".into()],
            true,
            &scalo::dlq::DlqConfig::default(),
        ))
        .unwrap();
        assert!(text.contains("enable.idempotence: \"false\""));
    }

    /// scalo's pipeline-wide record ceiling, which every other stage's producer
    /// and every topic scalo creates already accept.
    fn pipeline_ceiling() -> u64 {
        u64::from(scalo::transport::kafka::MESSAGE_MAX_BYTES.unsigned_abs())
    }

    /// The `message.max.bytes` written into the generated Kafka sink.
    fn written_message_max_bytes(yaml: &Value) -> Option<&str> {
        yaml.get("sinks")
            .and_then(|s| s.get(SINK_LABEL))
            .and_then(|s| s.get("librdkafka_options"))
            .and_then(|o| o.get("message.max.bytes"))
            .and_then(Value::as_str)
    }

    /// The size cap's condition, on its route or its filter.
    fn size_cap_condition(yaml: &Value) -> Option<&str> {
        let cap = yaml.get("transforms").and_then(|t| t.get(SIZE_CAP_LABEL))?;
        cap.get("route")
            .and_then(|r| r.get(WITHIN_CAP_OUTPUT))
            .or_else(|| cap.get("condition"))
            .and_then(Value::as_str)
    }

    /// A generated sink's `inputs`, as the labels they name.
    fn inputs_of<'a>(yaml: &'a Value, section: &str, label: &str) -> Vec<&'a str> {
        yaml.get(section)
            .and_then(|s| s.get(label))
            .and_then(|c| c.get("inputs"))
            .and_then(Value::as_array)
            .map(|inputs| inputs.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default()
    }

    fn bus_yaml(sink: &SinkConfig, dlq: &DlqConfig) -> Value {
        generate_sink_yaml(sink, &BridgeConfig::default(), &["last".into()], true, dlq)
    }

    /// The Kafka sink reads the size cap's `within_cap` output, and the cap
    /// reads the wired inputs.
    #[test]
    fn the_kafka_sink_sits_behind_the_size_cap() {
        let yaml = bus_yaml(&SinkConfig::default(), &DlqConfig::default());
        assert_eq!(
            inputs_of(&yaml, "sinks", SINK_LABEL),
            vec!["dfe_size_cap.within_cap"]
        );
        assert_eq!(inputs_of(&yaml, "transforms", SIZE_CAP_LABEL), vec!["last"]);
        let cap = yaml
            .get("transforms")
            .and_then(|t| t.get(SIZE_CAP_LABEL))
            .expect("the size cap transform");
        assert_eq!(cap.get("type").and_then(Value::as_str), Some("route"));
    }

    /// A record over the ceiling leaves the route on `_unmatched`, becomes an
    /// entry in the remap and is written to `sink.dlq_topic` as JSON with no
    /// key, whichever encoding the main sink uses.
    #[test]
    fn an_oversize_record_is_dead_lettered_whatever_the_encoding() {
        for (encoding, encoded) in [
            ("json", "body = encode_json(.)"),
            ("raw_bytes", r#"body = string(.message) ?? """#),
        ] {
            let sink = SinkConfig {
                encoding: encoding.into(),
                key_field: ".org_id".into(),
                topic: "syslog_load".into(),
                ..Default::default()
            };
            let yaml = bus_yaml(&sink, &DlqConfig::default());

            assert_eq!(
                inputs_of(&yaml, "transforms", SIZE_CAP_DLQ_LABEL),
                vec!["dfe_size_cap._unmatched"],
                "{encoding}"
            );
            let source = yaml["transforms"][SIZE_CAP_DLQ_LABEL]["source"]
                .as_str()
                .expect("the remap source");
            assert!(source.starts_with(encoded), "{encoding}: {source}");
            assert!(
                source.contains(r#"._raw_encoding = "base64""#),
                "{encoding}: {source}"
            );
            assert!(
                source.contains(r#".destination = "syslog_load""#),
                "{encoding}: {source}"
            );

            let dlq = &yaml["sinks"][DLQ_SINK_LABEL];
            assert_eq!(
                inputs_of(&yaml, "sinks", DLQ_SINK_LABEL),
                vec![SIZE_CAP_DLQ_LABEL]
            );
            assert_eq!(dlq["type"], "kafka", "{encoding}");
            assert_eq!(dlq["topic"], "dfe_transform_dlq", "{encoding}");
            assert_eq!(dlq["encoding"]["codec"], "json", "{encoding}");
            assert!(dlq.get("key_field").is_none(), "{encoding}: {dlq:?}");
            assert_eq!(dlq["buffer"]["type"], "memory", "{encoding}");
            assert_eq!(dlq["buffer"]["when_full"], "block", "{encoding}");
            assert_eq!(
                yaml["sinks"][SINK_LABEL]["key_field"], ".org_id",
                "the main sink keeps its key"
            );
        }
    }

    /// The dead letter sink reaches the broker as the main sink does: the same
    /// credentials by reference, TLS, and `message.max.bytes`.
    #[test]
    fn the_dead_letter_sink_shares_the_main_sinks_broker_settings() {
        let mut sink = SinkConfig {
            sasl: SaslConfig {
                enabled: true,
                username: "kafka-user".into(),
                password: "kafka-pass".into(),
                ..SaslConfig::default()
            },
            tls: TlsConfig {
                enabled: true,
                ..Default::default()
            },
            brokers: vec!["kafka-1:9093".into()],
            ..Default::default()
        };
        sink.librdkafka_options
            .insert("message.max.bytes".into(), "2000000".into());
        let yaml = bus_yaml(&sink, &DlqConfig::default());
        let (main, dead) = (&yaml["sinks"][SINK_LABEL], &yaml["sinks"][DLQ_SINK_LABEL]);
        for field in [
            "bootstrap_servers",
            "sasl",
            "tls",
            "compression",
            "librdkafka_options",
        ] {
            assert_eq!(main[field], dead[field], "{field}");
        }
        assert_eq!(
            dead["sasl"]["username"],
            "SECRET[dfe_credentials.sink_sasl_username]"
        );
        assert!(!yaml.to_string().contains("kafka-pass"));
    }

    /// With `dlq.enabled` false the cap is the filter that drops, and no remap
    /// or second sink is written.
    #[test]
    fn a_disabled_dlq_leaves_the_size_cap_dropping() {
        let dlq = DlqConfig {
            enabled: false,
            ..DlqConfig::default()
        };
        let yaml = bus_yaml(&SinkConfig::default(), &dlq);
        assert_eq!(inputs_of(&yaml, "sinks", SINK_LABEL), vec![SIZE_CAP_LABEL]);
        assert_eq!(yaml["transforms"][SIZE_CAP_LABEL]["type"], "filter");
        assert!(yaml["transforms"].get(SIZE_CAP_DLQ_LABEL).is_none());
        assert!(yaml["sinks"].get(DLQ_SINK_LABEL).is_none());
    }

    /// The dead letter keeps `dlq.max_payload_bytes`, held to what the sink's
    /// Kafka ceiling takes once the payload is base64.
    #[test]
    fn the_dead_letter_payload_cap_follows_the_dlq_and_the_ceiling() {
        // (message.max.bytes, dlq.max_payload_bytes, cap)
        for (ceiling, configured, cap) in [
            (None, Some(DEFAULT_MAX_PAYLOAD_BYTES), 524_288_u64),
            (None, Some(1024), 1024),
            (None, None, 524_288),
            (Some("200000"), Some(DEFAULT_MAX_PAYLOAD_BYTES), 95_840),
            (Some("8192"), Some(DEFAULT_MAX_PAYLOAD_BYTES), 0),
        ] {
            let mut sink = SinkConfig::default();
            if let Some(ceiling) = ceiling {
                sink.librdkafka_options
                    .insert("message.max.bytes".into(), ceiling.into());
            }
            let dlq = DlqConfig {
                max_payload_bytes: configured,
                ..DlqConfig::default()
            };
            assert_eq!(
                dead_letter_payload_cap(&sink, &dlq),
                cap,
                "{ceiling:?} {configured:?}"
            );
        }
    }

    /// A topic name is written as a VRL string, so a quote or backslash in it
    /// cannot end the literal.
    #[test]
    fn vrl_strings_escape_quotes_and_backslashes() {
        assert_eq!(vrl_string("a\"b\\c"), "\"a\\\"b\\\\c\"");
    }

    /// A sink no deployment has tuned carries a 2 MiB record. librdkafka's own
    /// `message.max.bytes` default is 1,000,000, so both the producer and the
    /// size cap have to sit at the ceiling the rest of the chain uses.
    #[test]
    fn a_default_sink_carries_a_2_mib_record() {
        const TWO_MIB: u64 = 2 * 1024 * 1024;
        let sink = SinkConfig::default();
        let yaml = generate_sink_yaml(
            &sink,
            &BridgeConfig::default(),
            &["src".into()],
            true,
            &scalo::dlq::DlqConfig::default(),
        );

        assert_eq!(
            written_message_max_bytes(&yaml),
            Some(pipeline_ceiling().to_string().as_str()),
            "librdkafka refuses a record over its own 1,000,000-byte default"
        );
        let ceiling = pipeline_ceiling() - RECORD_FRAMING_BYTES;
        assert_eq!(max_record_bytes(&sink), ceiling);
        assert_eq!(
            size_cap_condition(&yaml),
            Some(format!("length(encode_json(.)) <= {ceiling}").as_str())
        );
        assert!(
            max_record_bytes(&sink) > TWO_MIB,
            "a 2 MiB record must pass the default size cap"
        );
    }

    /// A deployment's `message.max.bytes` -- a managed broker's lower limit, or
    /// a larger one -- is the ceiling for both librdkafka and the size cap.
    #[test]
    fn the_size_cap_follows_message_max_bytes() {
        for (configured, cap) in [("2000000", 1_999_872_u64), ("8388608", 8_388_480)] {
            let mut sink = SinkConfig {
                encoding: "raw_bytes".into(),
                ..Default::default()
            };
            sink.librdkafka_options
                .insert("message.max.bytes".into(), configured.into());
            assert_eq!(max_record_bytes(&sink), cap, "{configured}");
            let yaml = generate_sink_yaml(
                &sink,
                &BridgeConfig::default(),
                &["src".into()],
                true,
                &scalo::dlq::DlqConfig::default(),
            );
            assert_eq!(
                written_message_max_bytes(&yaml),
                Some(configured),
                "the deployment's ceiling reaches librdkafka as set"
            );
            assert_eq!(
                size_cap_condition(&yaml),
                Some(format!(r#"length(string(.message) ?? "") <= {cap}"#).as_str())
            );
        }
    }

    #[test]
    fn the_direct_sink_has_no_size_cap() {
        let sink = SinkConfig {
            transport: crate::config::Transport::Direct,
            ..Default::default()
        };
        let yaml = generate_sink_yaml(
            &sink,
            &BridgeConfig::default(),
            &["src".into()],
            true,
            &scalo::dlq::DlqConfig::default(),
        );
        assert!(yaml.get("transforms").is_none());
    }

    /// `sink.batch` tunes the direct sink as it tunes the Kafka one, and an
    /// unset byte ceiling still keeps each push well under what the return
    /// listener decodes.
    #[test]
    fn the_direct_sink_batches_as_sink_batch_says() {
        let batch_of = |batch: BatchConfig| {
            let sink = SinkConfig {
                transport: crate::config::Transport::Direct,
                batch,
                ..Default::default()
            };
            generate_sink_yaml(
                &sink,
                &BridgeConfig::default(),
                &["src".into()],
                true,
                &scalo::dlq::DlqConfig::default(),
            )
            .get("sinks")
            .and_then(|s| s.get(SINK_LABEL))
            .and_then(|s| s.get("batch"))
            .cloned()
        };
        assert_eq!(
            batch_of(BatchConfig {
                max_events: 250,
                max_bytes: Some(1_048_576),
                timeout_secs: 1,
            }),
            Some(serde_json::json!({"max_events": 250, "max_bytes": 1_048_576, "timeout_secs": 1}))
        );
        assert_eq!(
            batch_of(BatchConfig::default()),
            Some(serde_json::json!({
                "max_events": 10_000,
                "max_bytes": DIRECT_BATCH_MAX_BYTES,
                "timeout_secs": 1
            }))
        );
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
            generate_sink_yaml(
                &sink,
                &BridgeConfig::default(),
                &["src".into()],
                true,
                &scalo::dlq::DlqConfig::default(),
            )
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
            &scalo::dlq::DlqConfig::default(),
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
        let text = scalo::yaml::to_string(&yaml).unwrap();

        // User override replaces default
        assert!(text.contains("fetch.max.bytes: \"52428800\""));
        // Custom option included
        assert!(text.contains("custom.option: value"));
    }

    #[test]
    fn source_commits_offsets_and_says_how_often() {
        let source = SourceConfig::default();
        let text = scalo::yaml::to_string(&generate_source_yaml(&source, &BridgeConfig::default()))
            .unwrap();

        // A commit interval with auto-commit off arms no timer, so the interval
        // is inert and lag stops moving between rebalances.
        assert!(text.contains("enable.auto.commit: \"true\""));
        assert!(text.contains("commit_interval_ms: 5000"));
        assert!(!text.contains("enable.auto.commit: \"false\""));
    }

    #[test]
    fn a_user_can_still_turn_auto_commit_back_off() {
        let mut source = SourceConfig::default();
        source
            .librdkafka_options
            .insert("enable.auto.commit".into(), "false".into());

        let text = scalo::yaml::to_string(&generate_source_yaml(&source, &BridgeConfig::default()))
            .unwrap();

        assert!(text.contains("enable.auto.commit: \"false\""));
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
        let text = scalo::yaml::to_string(&yaml).unwrap();

        // Vector TLS block
        assert!(text.contains("verify_certificate: false"));
        assert!(text.contains("verify_hostname: false"));
        // librdkafka option
        assert!(text.contains("enable.ssl.certificate.verification: \"false\""));
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
        let yaml = generate_sink_yaml(
            &sink,
            &BridgeConfig::default(),
            &["src".into()],
            true,
            &scalo::dlq::DlqConfig::default(),
        );
        let text = scalo::yaml::to_string(&yaml).unwrap();

        // Vector TLS block
        assert!(text.contains("verify_certificate: false"));
        assert!(text.contains("verify_hostname: false"));
        // librdkafka option
        assert!(text.contains("enable.ssl.certificate.verification: \"false\""));
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
            &scalo::dlq::DlqConfig::default(),
        );
        let text = scalo::yaml::to_string(&yaml).unwrap();

        assert!(text.contains("dfe_sink"));
        assert!(text.contains("type: kafka"));
        assert!(text.contains("output_topic"));
        assert!(text.contains(".org_id"));
        assert!(text.contains("last_transform"));
        assert!(text.contains("codec: json"));
        assert!(text.contains("compression: zstd"));
        // Production librdkafka defaults (from scalo kafka_config)
        assert!(!text.contains("compression.type"));
        assert!(text.contains("socket.nagle.disable: \"true\""));
        assert!(text.contains("statistics.interval.ms: \"1000\""));
        // Service-specific override: producer queue cap
        assert!(text.contains("queue.buffering.max.kbytes: \"262144\""));
        // Removed settings — back to librdkafka defaults
        assert!(!text.contains("batch.size"));
        assert!(!text.contains("batch.num.messages"));
        // The pipeline's record ceiling, not librdkafka's 1,000,000 default.
        assert!(text.contains(&format!("message.max.bytes: \"{}\"", pipeline_ceiling())));
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
        // The production profile's `linger.ms` is the batching delay, in
        // Vector's field.
        assert!(text.contains("timeout_secs: 0.02"));
        assert!(!text.contains("linger.ms"));
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
        let yaml = generate_sink_yaml(
            &sink,
            &BridgeConfig::default(),
            &["src".into()],
            true,
            &scalo::dlq::DlqConfig::default(),
        );
        let text = scalo::yaml::to_string(&yaml).unwrap();

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
        let yaml = generate_sink_yaml(
            &sink,
            &BridgeConfig::default(),
            &["src".into()],
            true,
            &scalo::dlq::DlqConfig::default(),
        );
        let text = scalo::yaml::to_string(&yaml).unwrap();

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
        let yaml = generate_sink_yaml(
            &sink,
            &BridgeConfig::default(),
            &["src".into()],
            true,
            &scalo::dlq::DlqConfig::default(),
        );
        let text = scalo::yaml::to_string(&yaml).unwrap();

        assert!(text.contains("max_events: 5000"));
        assert!(text.contains("max_bytes: 8388608"));
        // The production profile's `linger.ms` replaces `timeout_secs` on the bus.
        assert!(text.contains("timeout_secs: 0.02"));
    }

    /// A Kafka sink states its batching delay once, as `batch.timeout_secs`.
    /// Vector writes that as `queue.buffering.max.ms`, which librdkafka reads
    /// as `linger.ms`, so a second statement leaves the producer with whichever
    /// one the client applied last.
    #[test]
    fn a_kafka_sink_states_its_batching_delay_once() {
        let with_option = |k: &str, v: &str| SinkConfig {
            librdkafka_options: [(k.to_string(), v.to_string())].into(),
            ..Default::default()
        };

        for (case, sink, seconds) in [
            ("profile only", SinkConfig::default(), 0.02),
            ("user linger.ms", with_option("linger.ms", "20"), 0.02),
            (
                "user queue.buffering.max.ms",
                with_option("queue.buffering.max.ms", "5"),
                0.005,
            ),
        ] {
            let component = kafka_sink_component(&sink, &["src".into()], true);
            let options = component["librdkafka_options"].as_object().unwrap();
            assert!(!options.contains_key("linger.ms"), "{case}");
            assert!(!options.contains_key("queue.buffering.max.ms"), "{case}");
            assert_eq!(component["batch"]["timeout_secs"], seconds, "{case}");
        }
    }

    /// The delay leaves the librdkafka options for Vector's own field, and a
    /// value that is not a number of milliseconds stays where librdkafka
    /// refuses it.
    #[test]
    fn the_batching_delay_moves_into_the_batch_timeout() {
        let options = |pairs: &[(&str, &str)]| -> Mapping {
            pairs.iter().map(|(k, v)| (k.to_string(), val(v))).collect()
        };

        for (case, pairs, seconds, left) in [
            ("none", &[("fetch.min.bytes", "1")][..], None, 1),
            ("linger", &[("linger.ms", "100")][..], Some(0.1), 0),
            (
                "alias",
                &[("queue.buffering.max.ms", "1000")][..],
                Some(1.0),
                0,
            ),
            ("zero", &[("linger.ms", "0")][..], Some(0.0), 0),
            ("not a number", &[("linger.ms", "soon")][..], None, 1),
            ("negative", &[("linger.ms", "-5")][..], None, 1),
        ] {
            let mut rdkafka = options(pairs);
            let delay = take_batching_delay(&mut rdkafka).map(|n| n.as_f64().unwrap());
            assert_eq!(delay, seconds, "{case}");
            assert_eq!(rdkafka.len(), left, "{case}");
        }
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
            &scalo::dlq::DlqConfig::default(),
        );
        let sink = yaml
            .get("sinks")
            .and_then(|s| s.get(SINK_LABEL))
            .expect("the kafka sink");
        assert_eq!(
            sink.get("compression").and_then(Value::as_str),
            Some("zstd")
        );
        let options = sink
            .get("librdkafka_options")
            .and_then(Value::as_object)
            .expect("the librdkafka options");
        assert!(!options.contains_key("compression.type"));
        assert!(!options.contains_key("compression.codec"));
    }

    #[test]
    fn sink_yaml_no_compression_sets_none_on_the_vector_field() {
        let sink = SinkConfig {
            compression: "none".into(),
            ..Default::default()
        };
        let yaml = generate_sink_yaml(
            &sink,
            &BridgeConfig::default(),
            &["src".into()],
            true,
            &scalo::dlq::DlqConfig::default(),
        );
        let text = scalo::yaml::to_string(&yaml).unwrap();

        assert!(text.contains("\n    compression: none"));
        assert!(!text.contains("compression.type"));
    }

    /// A sink states its compression codec once, as Vector's `compression`
    /// field. Vector writes that as `compression.codec`, which librdkafka reads
    /// as `compression.type`, so a second statement leaves the producer with
    /// whichever one the client applied last.
    #[test]
    fn a_kafka_sink_states_its_compression_codec_once() {
        for codec in ["none", "gzip", "lz4", "snappy", "zstd"] {
            let sink = SinkConfig {
                compression: codec.into(),
                ..Default::default()
            };
            let component = kafka_sink_component(&sink, &["src".into()], true);
            let options = component["librdkafka_options"].as_object().unwrap();
            assert_eq!(component["compression"], codec);
            assert!(!options.contains_key("compression.type"), "{codec}");
            assert!(!options.contains_key("compression.codec"), "{codec}");
        }
    }

    /// Either spelling in `sink.librdkafka_options` is refused, with the dial
    /// that owns the codec named.
    #[test]
    fn both_spellings_of_the_compression_codec_are_derived_options() {
        let derived = derived_sink_options(&SinkConfig::default());
        for spelling in ["compression.type", "compression.codec"] {
            assert!(
                derived.contains(&(spelling, "sink.compression")),
                "{spelling}"
            );
        }
    }

    #[test]
    fn observability_yaml() {
        let yaml = generate_observability_yaml("127.0.0.1:9598");
        let text = scalo::yaml::to_string(&yaml).unwrap();

        assert!(text.contains("internal_metrics"));
        assert!(text.contains("prometheus_exporter"));
        assert!(text.contains("127.0.0.1:9598"));
    }

    #[test]
    fn observability_yaml_honours_configured_address() {
        let yaml = generate_observability_yaml("127.0.0.1:19598");
        let text = scalo::yaml::to_string(&yaml).unwrap();

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

    /// The Kafka Secret's spelling reaches Vector as librdkafka's own name,
    /// padded or in either case.
    #[test]
    fn librdkafka_mechanism_spellings_are_written_as_librdkafka_names() {
        for (given, written) in [
            ("PLAIN", "PLAIN"),
            ("SCRAM-SHA-256", "SCRAM-SHA-256"),
            ("SCRAM-SHA-512", "SCRAM-SHA-512"),
            ("scram-sha-512", "SCRAM-SHA-512"),
            (" PLAIN ", "PLAIN"),
        ] {
            assert_eq!(normalise_sasl_mechanism(given), written, "{given:?}");
        }
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
            let yaml = generate_sink_yaml(
                &sink,
                &BridgeConfig::default(),
                &["src".into()],
                true,
                &scalo::dlq::DlqConfig::default(),
            );
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

    /// Every security field of a Kafka component in Vector's shape reaches the
    /// floor client, including the hostname check scalo reads from raw maps.
    #[test]
    fn a_vector_kafka_component_maps_onto_the_floor_client() {
        let component: Value = scalo::yaml::from_str(
            r#"
type: kafka
bootstrap_servers: "kafka-1:9093, kafka-2:9093"
sasl:
  enabled: true
  mechanism: PLAIN
tls:
  enabled: true
  verify_certificate: false
  verify_hostname: false
librdkafka_options:
  fetch.max.bytes: "52428800"
  socket.keepalive.enable: true
  queue.buffering.max.ms: 5
"#,
            "generated.yaml",
            scalo::yaml::Profile::Trusted,
        )
        .unwrap();

        let client = vector_kafka_client_config(&component);

        assert_eq!(client.brokers, vec!["kafka-1:9093", "kafka-2:9093"]);
        assert_eq!(client.security_protocol, "SASL_SSL");
        assert_eq!(client.sasl_mechanism.as_deref(), Some("PLAIN"));
        assert!(client.ssl_skip_verify);
        for (key, value) in [
            ("fetch.max.bytes", "52428800"),
            ("socket.keepalive.enable", "true"),
            ("queue.buffering.max.ms", "5"),
            (HOSTNAME_VERIFICATION_KEY, "none"),
        ] {
            assert_eq!(
                client.librdkafka_overrides.get(key).map(String::as_str),
                Some(value),
                "{key}"
            );
        }
    }

    /// A component with no security blocks is what librdkafka runs by default.
    #[test]
    fn a_bare_vector_kafka_component_is_plaintext() {
        let component: Value = scalo::yaml::from_str(
            "type: kafka\nbootstrap_servers: kafka:9092\n",
            "generated.yaml",
            scalo::yaml::Profile::Trusted,
        )
        .unwrap();

        let client = vector_kafka_client_config(&component);

        assert_eq!(client.security_protocol, "PLAINTEXT");
        assert_eq!(client.sasl_mechanism, None);
        assert!(!client.ssl_skip_verify);
        assert!(client.librdkafka_overrides.is_empty());
    }

    /// Reading back what the generator writes gives the client it was built
    /// from, so the reverse mapping reads the generator's own field names.
    #[test]
    fn a_generated_kafka_sink_reads_back_as_the_client_it_came_from() {
        for (sasl_on, tls_on, skip_verify) in [
            (false, false, false),
            (true, false, false),
            (false, true, true),
            (true, true, false),
            (true, true, true),
        ] {
            let sink = SinkConfig {
                brokers: vec!["kafka-1:9093".into(), "kafka-2:9093".into()],
                sasl: SaslConfig {
                    enabled: sasl_on,
                    mechanism: "scram_sha_256".into(),
                    username: "user".into(),
                    ..SaslConfig::default()
                },
                tls: TlsConfig {
                    enabled: tls_on,
                    skip_verify,
                    ..Default::default()
                },
                ..Default::default()
            };
            let yaml = generate_sink_yaml(
                &sink,
                &BridgeConfig::default(),
                &["src".into()],
                true,
                &scalo::dlq::DlqConfig::default(),
            );
            let component = yaml
                .get("sinks")
                .and_then(|s| s.get(SINK_LABEL))
                .expect("the generated kafka sink");

            let read_back = vector_kafka_client_config(component);
            let built = kafka_client_config(
                &sink.brokers,
                &sink.sasl,
                &sink.tls,
                &sink.librdkafka_options,
            );
            let case = format!("sasl={sasl_on} tls={tls_on} skip_verify={skip_verify}");

            assert_eq!(read_back.brokers, built.brokers, "{case}");
            assert_eq!(
                read_back.security_protocol, built.security_protocol,
                "{case}"
            );
            assert_eq!(read_back.sasl_mechanism, built.sasl_mechanism, "{case}");
            assert_eq!(read_back.ssl_skip_verify, built.ssl_skip_verify, "{case}");
        }
    }
}
