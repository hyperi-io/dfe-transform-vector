// Project:   dfe-transform-vector
// File:      src/deployment.rs
// Purpose:   Deployment contract — Dockerfile, Helm chart, Compose generation
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Deployment contract for dfe-transform-vector.
//!
//! Defines the single source of truth for all deployment artefact generation:
//! Dockerfile, Helm chart, and Docker Compose fragment. The contract captures
//! health paths, ports, secrets, KEDA scaling, and default config.

use scalo::deployment::{
    DeploymentContract, HealthContract, ImageProfile, KedaConfig, KedaContract, NativeDepsContract,
    PortContract, SecretEnvContract, SecretGroupContract, base_image_from_cascade,
};

/// Build the deployment contract for dfe-transform-vector.
///
/// This contract drives generation of:
/// - Dockerfile (runtime image with appuser, health check, ports)
/// - Helm chart (Deployment, Service, ConfigMap, Secret, KEDA, HPA)
/// - Docker Compose fragment (local dev)
#[must_use]
pub fn contract() -> DeploymentContract {
    DeploymentContract {
        app_name: "dfe-transform-vector".into(),
        binary_name: "dfe-transform-vector".into(),
        description: "Vector.dev subprocess wrapper for Kafka-to-Kafka transform pipelines".into(),
        metrics_port: 9090,
        health: HealthContract {
            liveness_path: "/livez".into(),
            readiness_path: "/readyz".into(),
            metrics_path: "/metrics".into(),
        },
        env_prefix: "DFE_TRANSFORM".into(),
        metric_prefix: "transform_vector".into(),
        config_mount_path: "/etc/dfe-transform-vector/config.yaml".into(),
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
        entrypoint_args: vec![
            "--config".into(),
            "/etc/dfe-transform-vector/config.yaml".into(),
        ],
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
                "dir": "/etc/dfe-transform-vector/transforms"
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
        // `KedaContract` is `#[non_exhaustive]` (scalo 2.8.13) so it can no
        // longer be built via a struct literal. Build the app's real KEDA
        // values into a `KedaConfig` and convert; the new `scaling_pressure_*`
        // trigger fields stay at their defaults (OFF -- the Prometheus
        // serverAddress is cluster-specific and must be set before enabling).
        keda: Some(KedaContract::from_config(&KedaConfig {
            min_replicas: 1,
            max_replicas: 10,
            polling_interval: 15,
            cooldown_period: 300,
            kafka_lag_threshold: 1000,
            activation_lag_threshold: 0,
            cpu_enabled: true,
            cpu_threshold: 80,
            ..Default::default()
        })),
        // Org-wide base image via the scalo cascade (deployment.base_image),
        // defaulting to debian:trixie-slim. NOT pinned per-app -- the ubuntu
        // pin was a stale pre-trixie-cutover leftover.
        base_image: base_image_from_cascade(),
        native_deps: NativeDepsContract::default(),
        image_profile: ImageProfile::default(),
        schema_version: 3,
        oci_labels: scalo::deployment::OciLabels {
            licenses: "BUSL-1.1".into(),
            ..Default::default()
        },
        // Reflectable config (scalo-rs#6): derived JSON Schema of the wrapper
        // Config + a minimal catalog. This wrapper is the Vector-owns-routing
        // anomaly -- Vector's own YAML defines transforms/routing, so the
        // catalog describes only the wrapper capability (Kafka in/out + the
        // managed Vector transform), not per-transform knobs.
        config_schema: Some(scalo::deployment::config_schema_json::<crate::config::Config>()),
        capabilities: capabilities(),
    }
}

/// Capability catalog for dfe-transform-vector: the Vector-subprocess wrapper.
/// Deliberately thin -- Vector's own YAML config owns the transform/routing
/// surface (the documented anomaly), so this describes the wrapper only.
fn capabilities() -> Vec<scalo::deployment::Capability> {
    use scalo::deployment::{Capability, FieldSpec};
    vec![
        Capability::new("transform", "vector")
            .description(
                "Vector (vector.dev) subprocess wrapper: Kafka source -> user transform YAML -> \
                 Kafka sink. Vector's own YAML config defines the transforms + routing; this \
                 wrapper manages the process lifecycle and Kafka wiring.",
            )
            .maturity("stable")
            .field(FieldSpec::string("dfe_source").description(
                "DFE source name; derives topic/group defaults ({src}_land -> {src}_load).",
            ))
            .field(
                FieldSpec::string("transforms.dir")
                    .description("Directory of Vector transform YAML files (hot-reloaded)."),
            )
            .field(
                FieldSpec::list("transforms.files")
                    .description("Explicit ordered list of Vector transform YAML files."),
            ),
    ]
}

/// Vector.dev version bundled into the published image.
///
/// Pinned. Update deliberately — Vector minor versions can change CLI
/// flags and config schema. Keep in sync with the `vector.version`
/// default in [`crate::config::loader::VectorConfig`].
pub const VECTOR_VERSION: &str = "0.48.0";

/// Generate the Dockerfile from the contract.
///
/// Wraps `scalo::deployment::generate_dockerfile` and inserts
/// the Vector binary download + data-directory setup before `USER appuser`.
///
/// dfe-transform-vector ships TWO binaries in its runtime image — its own
/// Rust wrapper (autobuilt by cargo, handled by scalo's contract) AND
/// the upstream `vector` binary (downloaded inside the build at the
/// version pinned by [`VECTOR_VERSION`]). The other five DFE Rust apps
/// are single-binary images; this consumer-side override exists because
/// scalo's deployment contract has no slot for an add-on native binary.
///
/// When `hyperi-ci`'s overlay framework lands (see
/// `docs/superpowers/specs/2026-05-15-vector-binary-overlay-spec.md`)
/// this override moves into `.hyperi-ci.yaml`.
#[must_use]
pub fn emit_dockerfile() -> String {
    let base = scalo::deployment::generate_dockerfile(&contract(), None);

    // Vector install + data dirs go BEFORE the `USER` directive so root
    // can still chown the directories it creates.
    let vector_layer = format!(
        "# Vector binary — downloaded inside the build for portability.\n\
         # Pinned to {VECTOR_VERSION}; bump deliberately (CLI flags + config\n\
         # schema can shift between minor versions). Multi-arch via $TARGETARCH.\n\
         ARG VECTOR_VERSION={VECTOR_VERSION}\n\
         ARG TARGETARCH\n\
         RUN set -eu \\\n \
         && case \"${{TARGETARCH:-amd64}}\" in \\\n     \
                 amd64) ARCH=x86_64 ;; \\\n     \
                 arm64) ARCH=aarch64 ;; \\\n     \
                 *) echo \"unsupported TARGETARCH: ${{TARGETARCH}}\" >&2; exit 1 ;; \\\n \
            esac \\\n \
         && curl -fsSL \"https://packages.timber.io/vector/${{VECTOR_VERSION}}/vector-${{VECTOR_VERSION}}-${{ARCH}}-unknown-linux-gnu.tar.gz\" \\\n         \
                 -o /tmp/vector.tar.gz \\\n \
         && tar xz -C /tmp -f /tmp/vector.tar.gz \\\n \
         && mv \"/tmp/vector-${{ARCH}}-unknown-linux-gnu/bin/vector\" /usr/local/bin/vector \\\n \
         && chmod +x /usr/local/bin/vector \\\n \
         && rm -rf /tmp/vector.tar.gz \"/tmp/vector-${{ARCH}}-unknown-linux-gnu\" \\\n \
         && /usr/local/bin/vector --version\n\
         \n\
         # Vector data and config directories\n\
         RUN mkdir -p /var/lib/vector /var/run/vector/config /etc/dfe-transform-vector/transforms \\\n     \
             && chown -R appuser:appuser /var/lib/vector /var/run/vector /etc/dfe-transform-vector\n\
         \n\
         LABEL io.hyperi.vector.version=\"{VECTOR_VERSION}\"\n\
         \n"
    );

    // Splice in before the `USER ` line. Fail loudly if the anchor isn't
    // found — that means scalo's generator changed shape and this
    // override needs reviewing.
    if let Some(idx) = base.find("USER ") {
        let (before, after) = base.split_at(idx);
        format!("{before}{vector_layer}{after}")
    } else {
        panic!(
            "scalo generate_dockerfile() output missing `USER ` directive — \
             cannot splice Vector binary install. Review scalo output shape."
        );
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_contract_carries_reflectable_config() {
        let c = contract();
        assert_eq!(c.schema_version, 3);
        assert!(c.config_schema.is_some());
        assert!(c.capabilities.iter().any(|cap| cap.name == "vector"));
    }

    /// The committed reflectable artefacts under docs/ must not drift from a
    /// fresh regen. Regenerate with `dfe-transform-vector config-schema --dir docs`.
    #[test]
    fn test_config_artifacts_do_not_drift() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("docs");
        scalo::deployment::assert_no_config_artifact_drift(&contract(), dir);
    }
}
