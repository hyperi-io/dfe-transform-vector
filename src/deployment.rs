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
    DeploymentContract, HealthContract, ImageProfile, KedaContract, NativeDepsContract,
    PortContract, SecretEnvContract, SecretGroupContract,
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
        base_image: "ubuntu:24.04".into(),
        native_deps: NativeDepsContract::default(),
        image_profile: ImageProfile::default(),
        schema_version: 2,
        oci_labels: hyperi_rustlib::deployment::OciLabels::default(),
    }
}

/// Generate a Dockerfile with Vector binary support.
///
/// Calls the standard `generate_dockerfile()` and inserts a self-contained
/// Vector-download step + data directories before the USER directive.
/// The download runs inside the image build (no build-context staging,
/// no CI-side preparation step), so `docker build .` works anywhere.
///
/// Vector version selection: defaults to **latest release** at build
/// time (queries the GitHub releases API). Pass `--build-arg
/// VECTOR_VERSION=0.48.0` to pin a specific version for reproducible
/// builds. Multi-arch via `$TARGETARCH` (buildx auto-supplies amd64 /
/// arm64, mapped to Vector's x86_64 / aarch64 tarball naming).
///
/// **Temporary override**: this per-consumer `emit_dockerfile` is an
/// exception to the standard `hyperi_rustlib::deployment::generate_dockerfile()`
/// contract. The base rustlib emitter targets a single Rust binary;
/// dfe-transform-vector ships an additional native binary (Vector.dev)
/// not built by cargo. The proper long-term fix is an anchor+overlay
/// mechanism in rustlib + hyperi-ci so consumers declare add-on
/// binaries via config rather than overriding the generator in Rust.
/// Until that lands, this override stays — restoring the pattern from
/// commit 2345818 that was eroded during the rustlib v2.x deployment
/// refactor.
#[must_use]
pub fn emit_dockerfile() -> String {
    let contract = contract();
    let base = hyperi_rustlib::deployment::generate_dockerfile(&contract);

    // Insert Vector download + directories before the USER line.
    // curl is already installed by the base image (runtime healthcheck uses it).
    let vector_lines = "\
# Vector binary — downloaded inside the build for portability.
#
# By default, fetch the LATEST Vector release at build time. Override with
# `--build-arg VECTOR_VERSION=X.Y.Z` to pin a specific version (reproducible).
#
# Multi-arch: $TARGETARCH is amd64/arm64 (set by buildx); we map to
# Vector's tarball arch (x86_64/aarch64).
ARG VECTOR_VERSION=
ARG TARGETARCH
RUN set -eu \\
 && case \"${TARGETARCH}\" in \\
        amd64) ARCH=x86_64 ;; \\
        arm64) ARCH=aarch64 ;; \\
        *) echo \"unsupported TARGETARCH: ${TARGETARCH}\" >&2; exit 1 ;; \\
    esac \\
 && if [ -z \"${VECTOR_VERSION}\" ]; then \\
        VECTOR_VERSION=$(curl -fsSL https://api.github.com/repos/vectordotdev/vector/releases/latest \\
            | grep '\"tag_name\"' \\
            | head -1 \\
            | sed 's/.*\"v\\([^\"]*\\)\".*/\\1/'); \\
        if [ -z \"${VECTOR_VERSION}\" ]; then \\
            echo \"failed to resolve latest Vector version from GitHub API\" >&2; \\
            exit 1; \\
        fi; \\
        echo \"Resolved latest Vector version: ${VECTOR_VERSION}\"; \\
    fi \\
 && curl -fsSL \"https://packages.timber.io/vector/${VECTOR_VERSION}/vector-${VECTOR_VERSION}-${ARCH}-unknown-linux-gnu.tar.gz\" \\
        -o /tmp/vector.tar.gz \\
 && tar xz -C /tmp -f /tmp/vector.tar.gz \\
 && mv \"/tmp/vector-${ARCH}-unknown-linux-gnu/bin/vector\" /usr/local/bin/vector \\
 && chmod +x /usr/local/bin/vector \\
 && rm -rf /tmp/vector.tar.gz \"/tmp/vector-${ARCH}-unknown-linux-gnu\"

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
