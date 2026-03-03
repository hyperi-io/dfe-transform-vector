// Project:   dfe-transform-vector
// File:      src/config/generate.rs
// Purpose:   Generate Vector-native YAML from big-dial config
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Vector-native YAML generation from big-dial config.
//!
//! Generates source, sink, and observability YAML files that Vector
//! understands, using the canonical component labels `dfe_source` and
//! `dfe_sink`.

use serde_yaml_ng::Value;

use super::loader::{SinkConfig, SourceConfig};

/// Canonical label for the generated Kafka source component.
pub const SOURCE_LABEL: &str = "dfe_source";

/// Canonical label for the generated Kafka sink component.
pub const SINK_LABEL: &str = "dfe_sink";

/// Generate Vector Kafka source YAML from big-dial config.
///
/// Produces a `sources.dfe_source` block with type `kafka`, including
/// SASL/TLS when enabled, acknowledgements, and cooperative-sticky
/// rebalancing.
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

    // Acknowledgements — always enabled for at-least-once delivery
    let mut acks = serde_yaml_ng::Mapping::new();
    acks.insert(val("enabled"), Value::Bool(true));
    component.insert(val("acknowledgements"), Value::Mapping(acks));

    // SASL
    if source.sasl.enabled {
        component.insert(val("sasl"), build_sasl_block(&source.sasl));
    }

    // TLS
    if source.tls.enabled {
        component.insert(val("tls"), build_tls_block(&source.tls));
    }

    // librdkafka cooperative-sticky rebalancing
    let mut rdkafka = serde_yaml_ng::Mapping::new();
    rdkafka.insert(
        val("partition.assignment.strategy"),
        val("cooperative-sticky"),
    );
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
/// Produces a `sinks.dfe_sink` block with type `kafka`. The `inputs`
/// field is set to the provided list (typically the last transform label,
/// wired by the DAG assembler).
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

    // SASL
    if sink.sasl.enabled {
        component.insert(val("sasl"), build_sasl_block(&sink.sasl));
    }

    // TLS
    if sink.tls.enabled {
        component.insert(val("tls"), build_tls_block(&sink.tls));
    }

    // Wrap in sinks.dfe_sink
    let mut sinks = serde_yaml_ng::Mapping::new();
    sinks.insert(val(SINK_LABEL), Value::Mapping(component));

    let mut root = serde_yaml_ng::Mapping::new();
    root.insert(val("sinks"), Value::Mapping(sinks));

    Value::Mapping(root)
}

/// Generate Vector observability YAML (internal_metrics + prometheus_exporter).
pub fn generate_observability_yaml() -> Value {
    // internal_metrics source
    let mut metrics_source = serde_yaml_ng::Mapping::new();
    metrics_source.insert(val("type"), val("internal_metrics"));

    let mut sources = serde_yaml_ng::Mapping::new();
    sources.insert(val("internal_metrics"), Value::Mapping(metrics_source));

    // prometheus_exporter sink
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

// =========================================================================
// Helpers
// =========================================================================

/// Build a SASL mapping block for Vector config.
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

/// Build a TLS mapping block for Vector config.
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

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DecodingConfig, SaslConfig, SinkConfig, SourceConfig, TlsConfig};

    #[test]
    fn source_yaml_basic() {
        let source = SourceConfig {
            brokers: vec!["kafka-1:9092".into(), "kafka-2:9092".into()],
            topics: vec!["raw_events".into()],
            group_id: "test-group".into(),
            decoding: DecodingConfig::default(),
            sasl: SaslConfig::default(),
            tls: TlsConfig::default(),
        };
        let yaml = generate_source_yaml(&source);
        let text = serde_yaml_ng::to_string(&yaml).unwrap();

        assert!(text.contains("dfe_source"));
        assert!(text.contains("type: kafka"));
        assert!(text.contains("kafka-1:9092,kafka-2:9092"));
        assert!(text.contains("raw_events"));
        assert!(text.contains("test-group"));
        assert!(text.contains("codec: json"));
        assert!(text.contains("cooperative-sticky"));
        // SASL disabled by default — should not appear
        assert!(!text.contains("sasl"));
    }

    #[test]
    fn source_yaml_with_sasl_and_tls() {
        let source = SourceConfig {
            brokers: vec!["kafka:9093".into()],
            topics: vec!["topic1".into()],
            group_id: "grp".into(),
            decoding: DecodingConfig::default(),
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
        };
        let yaml = generate_source_yaml(&source);
        let text = serde_yaml_ng::to_string(&yaml).unwrap();

        assert!(text.contains("SCRAM-SHA-512"));
        assert!(text.contains("${KAFKA_USER}"));
        assert!(text.contains("${KAFKA_PASS}"));
        assert!(text.contains("tls:"));
    }

    #[test]
    fn sink_yaml_basic() {
        let sink = SinkConfig {
            brokers: vec!["kafka:9092".into()],
            topic: "output_topic".into(),
            key_field: ".org_id".into(),
            encoding: "json".into(),
            compression: "zstd".into(),
            sasl: SaslConfig::default(),
            tls: TlsConfig::default(),
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
    }

    #[test]
    fn sink_yaml_no_compression_when_none() {
        let sink = SinkConfig {
            compression: "none".into(),
            ..Default::default()
        };
        let yaml = generate_sink_yaml(&sink, &["src".into()]);
        let text = serde_yaml_ng::to_string(&yaml).unwrap();
        assert!(!text.contains("compression"));
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
