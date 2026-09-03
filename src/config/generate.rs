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
//! Production librdkafka defaults come from `scalo::kafka_config`
//! (shared DFE baseline). Service-specific overrides and user-supplied
//! `librdkafka_options` from config YAML are merged on top.

use std::collections::HashMap;

use serde_yaml_ng::Value;

use super::kafka_defaults;
use super::loader::{BufferConfig, SinkConfig, SourceConfig, VectorConfig};

/// Canonical label for the generated Kafka source component.
pub const SOURCE_LABEL: &str = "dfe_source";

/// Canonical label for the generated Kafka sink component.
pub const SINK_LABEL: &str = "dfe_sink";

/// Generate Vector global config YAML (timezone, data_dir, api settings).
///
/// Produces the top-level settings Vector needs at the global scope.
#[must_use]
pub fn generate_global_yaml(vector: &VectorConfig) -> Value {
    let mut root = serde_yaml_ng::Mapping::new();

    // Vector's default is the HOST's timezone, so a transform that parses a
    // zone-less timestamp would produce a different instant on every pod. DFE
    // events are UTC end to end.
    root.insert(val("timezone"), val("UTC"));

    if !vector.data_dir.is_empty() {
        root.insert(val("data_dir"), val(&vector.data_dir));
    }

    let mut api = serde_yaml_ng::Mapping::new();
    api.insert(val("enabled"), Value::Bool(true));
    if !vector.api_address.is_empty() {
        api.insert(val("address"), val(&vector.api_address));
    }
    root.insert(val("api"), Value::Mapping(api));

    Value::Mapping(root)
}

/// Generate Vector Kafka source YAML from big-dial config.
///
/// Produces a `sources.dfe_source` block with production-tuned librdkafka
/// options derived from DFE 2.1 templates. Includes fetch sizing, pre-fetch
/// queuing, commit control, and cooperative-sticky rebalancing.
#[must_use]
pub fn generate_source_yaml(source: &SourceConfig) -> Value {
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
        component.insert(val("sasl"), build_sasl_block(&source.sasl));
    }

    // TLS
    if source.tls.enabled {
        component.insert(val("tls"), build_tls_block(&source.tls));
    }

    // librdkafka options — production defaults + user overrides
    let rdkafka = build_source_librdkafka(source);
    component.insert(val("librdkafka_options"), Value::Mapping(rdkafka));

    // Wrap in sources.dfe_source
    let mut sources = serde_yaml_ng::Mapping::new();
    sources.insert(val(SOURCE_LABEL), Value::Mapping(component));

    let mut root = serde_yaml_ng::Mapping::new();
    root.insert(val("sources"), Value::Mapping(sources));

    Value::Mapping(root)
}

/// Generate Vector Kafka sink YAML from big-dial config.
///
/// Produces a `sinks.dfe_sink` block with production-tuned librdkafka
/// options, Vector-level batching, buffer config, and acknowledgements.
#[must_use]
pub fn generate_sink_yaml(sink: &SinkConfig, inputs: &[String]) -> Value {
    let mut component = serde_yaml_ng::Mapping::new();
    component.insert(val("type"), val("kafka"));
    component.insert(
        val("inputs"),
        Value::Sequence(inputs.iter().map(|i| val(i)).collect()),
    );
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
    let mut acks = serde_yaml_ng::Mapping::new();
    acks.insert(val("enabled"), Value::Bool(true));
    component.insert(val("acknowledgements"), Value::Mapping(acks));

    // Healthcheck
    let mut healthcheck = serde_yaml_ng::Mapping::new();
    healthcheck.insert(val("enabled"), Value::Bool(true));
    component.insert(val("healthcheck"), Value::Mapping(healthcheck));

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
        component.insert(val("sasl"), build_sasl_block(&sink.sasl));
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

    // Wrap in sinks.dfe_sink
    let mut sinks = serde_yaml_ng::Mapping::new();
    sinks.insert(val(SINK_LABEL), Value::Mapping(component));

    let mut root = serde_yaml_ng::Mapping::new();
    root.insert(val("sinks"), Value::Mapping(sinks));

    Value::Mapping(root)
}

/// Generate Vector observability YAML (internal_metrics + prometheus_exporter).
#[must_use]
pub fn generate_observability_yaml() -> Value {
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
    prom_sink.insert(val("address"), val("0.0.0.0:9598"));

    let mut sinks = serde_yaml_ng::Mapping::new();
    sinks.insert(val("prometheus_exporter"), Value::Mapping(prom_sink));

    let mut root = serde_yaml_ng::Mapping::new();
    root.insert(val("sources"), Value::Mapping(sources));
    root.insert(val("sinks"), Value::Mapping(sinks));

    Value::Mapping(root)
}

/// Service-specific consumer librdkafka overrides for transform-vector.
///
/// Vector arms its offset-commit timer only with auto-commit on, and the shared
/// DFE baseline turns it off -- see docs/LIBRDKAFKA.md, Service-Specific
/// Overrides.
const SERVICE_CONSUMER_OVERRIDES: &[(&str, &str)] = &[("enable.auto.commit", "true")];

/// Service-specific producer librdkafka overrides for transform-vector.
///
/// Caps the local producer queue to 256 MiB (librdkafka default is 1 GiB)
/// because transform-vector pods typically have 2-4 GiB memory.
const SERVICE_PRODUCER_OVERRIDES: &[(&str, &str)] = &[("queue.buffering.max.kbytes", "262144")];

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

    // SASL security protocol (auto-inject based on config)
    if source.sasl.enabled && source.tls.enabled {
        m.insert(val("security.protocol"), val("SASL_SSL"));
    } else if source.sasl.enabled {
        m.insert(val("security.protocol"), val("SASL_PLAINTEXT"));
    } else if source.tls.enabled {
        m.insert(val("security.protocol"), val("SSL"));
    }
    if source.sasl.enabled {
        m.insert(
            val("sasl.mechanism"),
            val(&normalise_sasl_mechanism(&source.sasl.mechanism)),
        );
    }

    if source.tls.enabled && source.tls.skip_verify {
        m.insert(val("enable.ssl.certificate.verification"), val("false"));
    }

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

    // SASL security protocol (auto-inject based on config)
    if sink.sasl.enabled && sink.tls.enabled {
        m.insert(val("security.protocol"), val("SASL_SSL"));
    } else if sink.sasl.enabled {
        m.insert(val("security.protocol"), val("SASL_PLAINTEXT"));
    } else if sink.tls.enabled {
        m.insert(val("security.protocol"), val("SSL"));
    }
    if sink.sasl.enabled {
        m.insert(
            val("sasl.mechanism"),
            val(&normalise_sasl_mechanism(&sink.sasl.mechanism)),
        );
    }

    if sink.tls.enabled && sink.tls.skip_verify {
        m.insert(val("enable.ssl.certificate.verification"), val("false"));
    }

    m
}

fn build_sasl_block(sasl: &super::loader::SaslConfig) -> Value {
    let mut m = serde_yaml_ng::Mapping::new();
    m.insert(val("enabled"), Value::Bool(true));
    m.insert(
        val("mechanism"),
        val(&normalise_sasl_mechanism(&sasl.mechanism)),
    );
    m.insert(val("username"), val(&sasl.username));
    m.insert(val("password"), val(&sasl.password));
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
        let global = generate_global_yaml(&crate::config::VectorConfig::default());
        assert_eq!(
            global.get("timezone").and_then(Value::as_str),
            Some("UTC"),
            "the assembled config must pin the timezone"
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
        let yaml = generate_source_yaml(&source);
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
                username: "${KAFKA_USER}".into(),
                password: "${KAFKA_PASS}".into(),
            },
            tls: TlsConfig {
                enabled: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let yaml = generate_source_yaml(&source);
        let text = serde_yaml_ng::to_string(&yaml).unwrap();

        assert!(text.contains("SCRAM-SHA-512"));
        assert!(text.contains("${KAFKA_USER}"));
        assert!(text.contains("security.protocol: SASL_SSL"));
        assert!(text.contains("sasl.mechanism: SCRAM-SHA-512"));
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

        let yaml = generate_source_yaml(&source);
        let text = serde_yaml_ng::to_string(&yaml).unwrap();

        // User override replaces default
        assert!(text.contains("fetch.max.bytes: '52428800'"));
        // Custom option included
        assert!(text.contains("custom.option: value"));
    }

    #[test]
    fn source_commits_offsets_and_says_how_often() {
        let source = SourceConfig::default();
        let text = serde_yaml_ng::to_string(&generate_source_yaml(&source)).unwrap();

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

        let text = serde_yaml_ng::to_string(&generate_source_yaml(&source)).unwrap();

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
        let yaml = generate_source_yaml(&source);
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
        let yaml = generate_sink_yaml(&sink, &["src".into()]);
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
        let yaml = generate_sink_yaml(&sink, &["last_transform".into()]);
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
        assert!(text.contains("message_timeout_ms: 300000"));
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
        let yaml = generate_sink_yaml(&sink, &["src".into()]);
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
        let yaml = generate_sink_yaml(&sink, &["src".into()]);
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
        let yaml = generate_sink_yaml(&sink, &["src".into()]);
        let text = serde_yaml_ng::to_string(&yaml).unwrap();

        assert!(text.contains("max_events: 5000"));
        assert!(text.contains("max_bytes: 8388608"));
        assert!(text.contains("timeout_secs: 2"));
    }

    #[test]
    fn sink_yaml_no_compression_sets_none_in_librdkafka() {
        let sink = SinkConfig {
            compression: "none".into(),
            ..Default::default()
        };
        let yaml = generate_sink_yaml(&sink, &["src".into()]);
        let text = serde_yaml_ng::to_string(&yaml).unwrap();

        // Vector-level compression omitted
        assert!(!text.contains("\n    compression:"));
        // librdkafka compression.type set to none
        assert!(text.contains("compression.type: none"));
    }

    #[test]
    fn observability_yaml() {
        let yaml = generate_observability_yaml();
        let text = serde_yaml_ng::to_string(&yaml).unwrap();

        assert!(text.contains("internal_metrics"));
        assert!(text.contains("prometheus_exporter"));
        assert!(text.contains("0.0.0.0:9598"));
    }

    #[test]
    fn sasl_mechanism_normalisation() {
        assert_eq!(normalise_sasl_mechanism("plain"), "PLAIN");
        assert_eq!(normalise_sasl_mechanism("scram_sha_256"), "SCRAM-SHA-256");
        assert_eq!(normalise_sasl_mechanism("scram_sha_512"), "SCRAM-SHA-512");
    }
}
