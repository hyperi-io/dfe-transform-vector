// Project:   dfe-transform-vector
// File:      tests/integration/deployment.rs
// Purpose:   Deployment contract tests — catch broken CI artefacts
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Deployment contract tests verify that generated Dockerfiles, Helm charts,
//! compose fragments, and contract JSON match the expected structure.

use dfe_transform_vector::deployment;

#[test]
fn contract_produces_valid_structure() {
    let c = deployment::contract();

    assert_eq!(c.app_name, "dfe-transform-vector");
    assert_eq!(c.binary_name, "dfe-transform-vector");
    assert_eq!(c.env_prefix, "DFE_TRANSFORM");
    assert_eq!(c.metric_prefix, "transform_vector");
    assert_eq!(c.metrics_port, 9090);
    assert_eq!(c.base_image, "ubuntu:24.04");

    // Health paths match the DFE contract
    assert_eq!(c.health.liveness_path, "/health/live");
    assert_eq!(c.health.readiness_path, "/health/ready");
    assert_eq!(c.health.metrics_path, "/metrics");

    // Required ports: health (9000), vector-api (8686)
    let port_names: Vec<&str> = c.extra_ports.iter().map(|p| p.name.as_str()).collect();
    assert!(port_names.contains(&"health"), "missing health port");
    assert!(
        port_names.contains(&"vector-api"),
        "missing vector-api port"
    );

    let health_port = c.extra_ports.iter().find(|p| p.name == "health").unwrap();
    assert_eq!(health_port.port, 9000);

    // KEDA config present
    assert!(c.keda.is_some(), "KEDA contract must be defined");
    let keda = c.keda.as_ref().unwrap();
    assert!(keda.min_replicas >= 1);
    assert!(keda.max_replicas > keda.min_replicas);

    // Kafka secrets defined
    assert!(!c.secrets.is_empty(), "Kafka secrets must be defined");
    let env_vars: Vec<&str> = c.secrets[0]
        .env_vars
        .iter()
        .map(|s| s.env_var.as_str())
        .collect();
    assert!(
        env_vars.contains(&"KAFKA_SASL_USERNAME"),
        "missing KAFKA_SASL_USERNAME secret"
    );
    assert!(
        env_vars.contains(&"KAFKA_SASL_PASSWORD"),
        "missing KAFKA_SASL_PASSWORD secret"
    );

    // Default config present and parseable
    assert!(
        c.default_config.is_some(),
        "default config must be embedded in contract"
    );
}

#[test]
fn emit_dockerfile_produces_valid_output() {
    let dockerfile = deployment::emit_dockerfile();

    // Must contain base image
    assert!(
        dockerfile.contains("FROM ubuntu:24.04"),
        "missing base image in Dockerfile"
    );

    // Must download Vector binary inside the build (multi-arch, latest-or-pinned).
    // Replaces the old `COPY vector /usr/local/bin/vector` which required CI-side
    // staging — now the image is self-contained.
    assert!(
        dockerfile.contains("ARG VECTOR_VERSION"),
        "missing VECTOR_VERSION build arg"
    );
    assert!(
        dockerfile.contains("ARG TARGETARCH"),
        "missing TARGETARCH build arg (multi-arch support)"
    );
    assert!(
        dockerfile.contains("packages.timber.io/vector"),
        "missing Vector tarball download from packages.timber.io"
    );
    assert!(
        dockerfile.contains("/usr/local/bin/vector"),
        "missing /usr/local/bin/vector install path"
    );

    // Must contain wrapper binary COPY
    assert!(
        dockerfile.contains("dfe-transform-vector"),
        "missing wrapper binary in Dockerfile"
    );

    // Must contain USER directive (non-root)
    assert!(
        dockerfile.contains("USER "),
        "missing USER directive in Dockerfile"
    );

    // Must contain data directories
    assert!(
        dockerfile.contains("/var/lib/vector"),
        "missing Vector data directory"
    );
    assert!(
        dockerfile.contains("/var/run/vector/config"),
        "missing Vector config directory"
    );
}

#[test]
fn emit_chart_generates_without_panic() {
    let contract = deployment::contract();
    let dir = tempfile::tempdir().expect("create temp dir");
    let dir_path = dir.path().to_str().unwrap();

    let result = hyperi_rustlib::deployment::generate_chart(&contract, dir_path);
    assert!(
        result.is_ok(),
        "chart generation failed: {:?}",
        result.err()
    );

    // Chart.yaml must exist
    let chart_yaml = dir.path().join("Chart.yaml");
    assert!(chart_yaml.exists(), "Chart.yaml not generated");

    // values.yaml must exist
    let values_yaml = dir.path().join("values.yaml");
    assert!(values_yaml.exists(), "values.yaml not generated");

    // templates/ directory must exist
    let templates = dir.path().join("templates");
    assert!(templates.is_dir(), "templates/ not generated");
}

#[test]
fn emit_compose_generates_without_panic() {
    let contract = deployment::contract();
    let compose = hyperi_rustlib::deployment::generate_compose_fragment(&contract);

    assert!(!compose.is_empty(), "compose fragment is empty");
    assert!(
        compose.contains("dfe-transform-vector"),
        "compose missing service name"
    );
}

#[test]
fn contract_json_roundtrip() {
    let contract = deployment::contract();
    let json = contract.to_json();

    // Must be valid JSON
    assert!(!json.is_empty(), "contract JSON is empty");
    assert!(
        json.starts_with('{'),
        "contract JSON doesn't start with open brace"
    );

    // Must contain key fields
    assert!(json.contains("dfe-transform-vector"));
    assert!(json.contains("DFE_TRANSFORM"));
}
