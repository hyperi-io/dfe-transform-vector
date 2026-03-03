// Project:   dfe-transform-vector
// File:      src/deployment.rs
// Purpose:   Deployment contract — Dockerfile, Helm chart, Compose generation
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Deployment contract for dfe-transform-vector.
//!
//! Defines the single source of truth for all deployment artefact generation:
//! Dockerfile, Helm chart, and Docker Compose fragment. The contract captures
//! health paths, ports, secrets, KEDA scaling, and default config.

use hyperi_rustlib::deployment::{
    DeploymentContract, HealthContract, KedaContract, PortContract, SecretEnvContract,
    SecretGroupContract,
};

/// Build the deployment contract for dfe-transform-vector.
///
/// This contract drives generation of:
/// - Dockerfile (runtime image with appuser, health check, ports)
/// - Helm chart (Deployment, Service, ConfigMap, Secret, KEDA, HPA)
/// - Docker Compose fragment (local dev)
pub fn contract() -> DeploymentContract {
    DeploymentContract {
        app_name: "dfe-transform-vector".into(),
        binary_name: "dfe-transform-vector".into(),
        description: "Vector.dev subprocess wrapper for Kafka-to-Kafka transform pipelines".into(),
        metrics_port: 9090,
        health: HealthContract {
            liveness_path: "/health/live".into(),
            readiness_path: "/health/ready".into(),
            metrics_path: "/metrics".into(),
        },
        env_prefix: "DFE_TRANSFORM".into(),
        metric_prefix: "transform_vector".into(),
        config_mount_path: "/etc/dfe/config.yaml".into(),
        image_registry: "ghcr.io/hyperi-io".into(),
        extra_ports: vec![
            PortContract {
                name: "health".into(),
                port: 9000,
                protocol: "TCP".into(),
            },
            PortContract {
                name: "vector-api".into(),
                port: 8686,
                protocol: "TCP".into(),
            },
        ],
        entrypoint_args: vec!["--config".into(), "/etc/dfe/config.yaml".into()],
        secrets: vec![SecretGroupContract {
            group_name: "kafka".into(),
            env_vars: vec![
                SecretEnvContract {
                    env_var: "KAFKA_SASL_USERNAME".into(),
                    key_name: "username".into(),
                    secret_key: "kafka-username".into(),
                },
                SecretEnvContract {
                    env_var: "KAFKA_SASL_PASSWORD".into(),
                    key_name: "password".into(),
                    secret_key: "kafka-password".into(),
                },
            ],
        }],
        default_config: Some(serde_json::json!({
            "pipeline": {
                "name": "default"
            },
            "source": {
                "brokers": ["kafka:9092"],
                "topics": ["raw_events"],
                "group_id": "dfe-transform-vector-default",
                "decoding": { "codec": "json" },
                "sasl": { "enabled": true, "mechanism": "scram_sha_512" },
                "tls": { "enabled": false }
            },
            "sink": {
                "brokers": ["kafka:9092"],
                "topic": "enriched_events",
                "key_field": ".org_id",
                "encoding": "json",
                "compression": "zstd",
                "sasl": { "enabled": true, "mechanism": "scram_sha_512" },
                "tls": { "enabled": false }
            },
            "transforms": {
                "dir": "/etc/dfe/transforms"
            },
            "vector": {
                "binary": "/usr/local/bin/vector",
                "data_dir": "/var/lib/vector",
                "api_address": "0.0.0.0:8686",
                "log_level": "info",
                "version": "0.48.0",
                "version_check": "warn"
            },
            "health": {
                "address": "0.0.0.0:9000"
            },
            "metrics": {
                "address": "0.0.0.0:9090",
                "vector_metrics_address": "127.0.0.1:9598"
            },
            "logging": {
                "level": "info",
                "format": "json"
            }
        })),
        depends_on: vec!["kafka".into()],
        keda: Some(KedaContract {
            min_replicas: 1,
            max_replicas: 10,
            polling_interval: 15,
            cooldown_period: 300,
            kafka_lag_threshold: 1000,
            activation_lag_threshold: 0,
            cpu_enabled: true,
            cpu_threshold: 80,
        }),
    }
}

/// Generate a Dockerfile with Vector binary support.
///
/// Calls the standard `generate_dockerfile()` and inserts Vector-specific
/// lines (binary COPY, data directories) before the USER directive.
pub fn emit_dockerfile() -> String {
    let contract = contract();
    let base = hyperi_rustlib::deployment::generate_dockerfile(&contract);

    // Insert Vector binary and directories before the USER line
    let vector_lines = "\
# Vector binary (built/downloaded externally by CI)
COPY vector /usr/local/bin/vector

# Vector data and config directories
RUN mkdir -p /var/lib/vector /var/run/vector/config /etc/dfe/transforms \\
    && chown -R appuser:appuser /var/lib/vector /var/run/vector /etc/dfe
";

    if let Some(pos) = base.find("\nUSER ") {
        let (before, after) = base.split_at(pos);
        format!("{before}\n\n{vector_lines}{after}")
    } else {
        // Fallback: append at end
        format!("{base}\n{vector_lines}")
    }
}
