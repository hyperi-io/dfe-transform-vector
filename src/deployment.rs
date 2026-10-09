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
    CONTRACT_SCHEMA_VERSION, DeploymentContract, HealthContract, ImageProfile, KafkaLagTrigger,
    KedaConfig, KedaContract, NativeDepsContract, PortContract, ResourceList, ResourcesContract,
    SecretEnvContract, SecretGroupContract, SecurityContract, WritablePath,
    base_image_from_cascade,
};

/// The chart description and the OCI image description, kept as one string so
/// the two cannot drift.
const DESCRIPTION: &str = "Vector subprocess wrapper for Kafka-to-Kafka transform pipelines";

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
        description: DESCRIPTION.into(),
        metrics_port: 9090,
        health: HealthContract {
            startup_budget_seconds: 120,
            ..HealthContract::default()
        },
        env_prefix: "DFE_TRANSFORM".into(),
        metric_prefix: "transform_vector".into(),
        config_mount_path: "/etc/dfe-transform-vector/config.yaml".into(),
        image_registry: "ghcr.io/hyperi-io".into(),
        // No separate health port: `metrics_port` carries /livez, /readyz and
        // /metrics, and is the only port the generated probes target. `push` is
        // the scalo Push listener the direct transport receives records on --
        // 6000 is the platform convention every DFE stage's listener uses.
        // Vector's API gets no port: it binds loopback, when it runs at all.
        extra_ports: vec![
            PortContract::tcp("push", 6000)
                .when_equals("config.source.transport", "direct")
                .bound_from("source.listen")
                .app_protocol("kubernetes.io/h2c"),
        ],
        unbound_listen_paths: vec![],
        entrypoint_args: vec![
            "--config".into(),
            "/etc/dfe-transform-vector/config.yaml".into(),
        ],
        secrets: secrets(),
        default_config: Some(serde_json::json!({
            "pipeline": {
                "name": "default"
            },
            "source": {
                "transport": "bus",
                "listen": "0.0.0.0:6000",
                "brokers": ["kafka:9092"],
                "topics": ["raw_events"],
                "group_id": "dfe-transform-vector-default",
                "decoding": { "codec": "json" },
                // The username and password arrive through the env vars `secrets()` declares, never the ConfigMap.
                "sasl": { "enabled": true, "mechanism": "scram_sha_512" },
                "tls": { "enabled": false },
                "acknowledgements": { "enabled": true }
            },
            "sink": {
                "transport": "bus",
                "endpoint": "http://dfe-loader:6000",
                "brokers": ["kafka:9092"],
                "topic": "enriched_events",
                "key_field": ".org_id",
                "encoding": "json",
                "compression": "zstd",
                "sasl": { "enabled": true, "mechanism": "scram_sha_512" },
                "tls": { "enabled": false }
            },
            // The two loopback legs between the supervisor and Vector, used on
            // the direct transport only.
            "bridge": {
                "to_vector": "127.0.0.1:6100",
                "from_vector": "127.0.0.1:6101",
                "batch_size": 500
            },
            "transforms": {
                "dir": "/etc/dfe-transform-vector/transforms"
            },
            "vector": {
                "binary": "/usr/local/bin/vector",
                "data_dir": "/var/lib/vector",
                "api_enabled": false,
                "api_address": "127.0.0.1:8686",
                "log_level": "info",
                "version": VECTOR_VERSION,
                "version_check": "warn"
            },
            // vector_metrics_address is where Vector's own prometheus_exporter
            // binds. The wrapper scrapes it and merges vector_* into the 9090
            // registry, so it stays on loopback and needs no container port.
            "metrics": {
                "address": "0.0.0.0:9090",
                "vector_metrics_address": "127.0.0.1:9598",
                "vector_metrics_expiry_ticks": 4,
                "sink_stall_secs": 60
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
        keda: Some(
            KedaContract::from_config(&KedaConfig {
                min_replicas: 1,
                max_replicas: 10,
                polling_interval: 15,
                cooldown_period: 300,
                kafka_lag_threshold: 1000,
                activation_lag_threshold: 0,
                cpu_enabled: true,
                cpu_threshold: 80,
                ..Default::default()
            })
            // Raw consumer-group lag rises when a downstream stage breaks, so it never scales this app.
            .with_kafka_trigger(KafkaLagTrigger::disabled()),
        ),
        // Org-wide base image via the scalo cascade (deployment.base_image),
        // defaulting to debian:trixie-slim. NOT pinned per-app -- the ubuntu
        // pin was a stale pre-trixie-cutover leftover.
        base_image: base_image_from_cascade(),
        native_deps: NativeDepsContract::default(),
        image_profile: ImageProfile::default(),
        schema_version: CONTRACT_SCHEMA_VERSION,
        // scalo writes no vendor, licence or copyright of its own, so the labels
        // and the generated Dockerfile header carry exactly these.
        oci_labels: scalo::deployment::OciLabels {
            title: "dfe-transform-vector".into(),
            description: DESCRIPTION.into(),
            vendor: "HYPERI PTY LIMITED".into(),
            label_namespace: "io.hyperi".into(),
            licenses: "BUSL-1.1".into(),
            copyright: "(c) 2026 HYPERI PTY LIMITED".into(),
        },
        // Reflectable config (scalo-rs#6): derived JSON Schema of the wrapper
        // Config + a minimal catalog. This wrapper is the Vector-owns-routing
        // anomaly -- Vector's own YAML defines transforms/routing, so the
        // catalog describes only the wrapper capability (Kafka in/out + the
        // managed Vector transform), not per-transform knobs.
        config_schema: Some(scalo::deployment::config_schema_json::<crate::config::Config>()),
        capabilities: capabilities(),
        // Vector's data directory, and the assembled config with its credential files, under a read-only root.
        writable_paths: vec![
            WritablePath::new("data", "/var/lib/vector"),
            WritablePath::new("run", "/var/run/vector"),
        ],
        termination_grace_seconds: 90,
        resources: ResourcesContract {
            requests: ResourceList {
                cpu: "100m".into(),
                memory: "128Mi".into(),
            },
            limits: ResourceList {
                cpu: "500m".into(),
                memory: "512Mi".into(),
            },
        },
        security: SecurityContract::default(),
        singleton: false,
    }
}

/// The Kafka Secret the chart mounts into each endpoint's SASL env vars.
///
/// `Config::apply_flat_env` reads these per-endpoint names. Each `key_name` is
/// distinct because the chart renders it as a values key.
fn secrets() -> Vec<SecretGroupContract> {
    let env = |env_var: &str, key_name: &str, secret_key: &str| SecretEnvContract {
        env_var: env_var.into(),
        key_name: key_name.into(),
        secret_key: secret_key.into(),
    };
    vec![SecretGroupContract::new(
        "kafka",
        vec![
            env(
                "DFE_TRANSFORM_SOURCE_SASL_USERNAME",
                "source-username",
                "username",
            ),
            env(
                "DFE_TRANSFORM_SOURCE_SASL_PASSWORD",
                "source-password",
                "password",
            ),
            env(
                "DFE_TRANSFORM_SINK_SASL_USERNAME",
                "sink-username",
                "username",
            ),
            env(
                "DFE_TRANSFORM_SINK_SASL_PASSWORD",
                "sink-password",
                "password",
            ),
        ],
    )]
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
/// flags and config schema. The `vector.version` default in
/// [`crate::config::loader::VectorConfig`] and the contract's own
/// `default_config` both read this constant, so a deployment's expected
/// version cannot drift from the shipped binary.
// renovate: datasource=github-releases depName=vectordotdev/vector
pub const VECTOR_VERSION: &str = "0.58.0";

/// SHA256 of `vector-0.58.0-x86_64-unknown-linux-gnu.tar.gz`, read from
/// Vector's published `vector-0.58.0-SHA256SUMS` and cross-checked against
/// the identically-named asset on the GitHub release -- the two agree, so the
/// digest does not rest on a single host.
const VECTOR_SHA256_AMD64: &str =
    "a4634bea859a7ad7064ff3dd6f6ad7eb0e8dd4493cc41657d84da8dd66f09d09";

/// SHA256 of `vector-0.58.0-aarch64-unknown-linux-gnu.tar.gz`, sourced and
/// cross-checked the same way as [`VECTOR_SHA256_AMD64`].
const VECTOR_SHA256_ARM64: &str =
    "06d9f9768feb0cb5c7cdfc12e0b737b22f1220967f5455f391a395361b5799e5";

/// The Vector install layer, spliced into the generated Dockerfile.
///
/// A real Dockerfile so hadolint and shellcheck can lint it; see
/// [`emit_dockerfile`].
const VECTOR_LAYER_TEMPLATE: &str = include_str!("vector-layer.dockerfile");

/// Substituted with [`VECTOR_VERSION`] in [`VECTOR_LAYER_TEMPLATE`].
const VERSION_PLACEHOLDER: &str = "@VECTOR_VERSION@";

/// Substituted with [`VECTOR_SHA256_AMD64`] in [`VECTOR_LAYER_TEMPLATE`].
const SHA256_AMD64_PLACEHOLDER: &str = "@VECTOR_SHA256_AMD64@";

/// Substituted with [`VECTOR_SHA256_ARM64`] in [`VECTOR_LAYER_TEMPLATE`].
const SHA256_ARM64_PLACEHOLDER: &str = "@VECTOR_SHA256_ARM64@";

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
/// When `hyperi-ci` grows an overlay framework this override moves into
/// `.hyperi-ci.yaml`.
#[must_use]
pub fn emit_dockerfile() -> String {
    let base = scalo::deployment::generate_dockerfile(&contract(), None);

    // The fragment is a REAL Dockerfile file, not a Rust string literal, so
    // hadolint and shellcheck can read it. Shell embedded in `format!` is
    // validated by nothing until the image build runs in CI, and the escaping
    // (`\\\n`, doubled braces) hides typos from review.
    //
    // include_str! means it is still compile-time -- no runtime IO, no build
    // script, and the file ships inside the published crate.
    let vector_layer = VECTOR_LAYER_TEMPLATE
        .replace(VERSION_PLACEHOLDER, VECTOR_VERSION)
        .replace(SHA256_AMD64_PLACEHOLDER, VECTOR_SHA256_AMD64)
        .replace(SHA256_ARM64_PLACEHOLDER, VECTOR_SHA256_ARM64);

    debug_assert!(
        !vector_layer.contains(VERSION_PLACEHOLDER)
            && !vector_layer.contains(SHA256_AMD64_PLACEHOLDER)
            && !vector_layer.contains(SHA256_ARM64_PLACEHOLDER),
        "unsubstituted placeholder left in the Vector layer"
    );

    // Vector install + data dirs go BEFORE the `USER` directive so root can
    // still chown the directories it creates.
    //
    // Anchoring on scalo's output text is the weak point of this override: it
    // breaks if the generator's shape changes. Panicking is deliberate -- the
    // alternative is emitting a Dockerfile that silently drops the Vector
    // install and produces an image whose second binary is simply absent.
    let Some(user) = base.find("\nUSER ") else {
        panic!(
            "scalo generate_dockerfile() output has no `USER ` directive to \
             splice the Vector install before. scalo's generator shape changed; \
             review this override rather than working around it."
        );
    };

    // The comment lines directly above `USER` describe it, so they stay with it.
    let mut idx = user;
    while let Some(prev) = base[..idx].rfind('\n') {
        if !base[prev + 1..idx].starts_with('#') {
            break;
        }
        idx = prev;
    }

    // +1 to keep the newline with `before`, so the splice starts on its own line.
    let (before, after) = base.split_at(idx + 1);
    format!("{before}{vector_layer}\n{after}")
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// The OCI title and description feed the image labels and the registry
    /// package page, and scalo leaves both empty unless the app sets them.
    #[test]
    fn test_oci_title_and_description_are_set() {
        let c = contract();
        assert_eq!(c.oci_labels.title, c.app_name);
        assert_eq!(c.oci_labels.description, c.description);
        assert_ne!(c.oci_labels.description, "");
    }

    #[test]
    fn test_contract_carries_reflectable_config() {
        let c = contract();
        assert_eq!(c.schema_version, CONTRACT_SCHEMA_VERSION);
        assert!(c.config_schema.is_some());
        assert!(c.capabilities.iter().any(|cap| cap.name == "vector"));
    }

    /// The version the chart hands a deployment must be the version in the
    /// image, or `version_check: strict` refuses to start every pod.
    #[test]
    fn test_default_config_expects_the_shipped_vector() {
        let c = contract();
        let default_config = c.default_config.expect("contract carries default config");
        assert_eq!(default_config["vector"]["version"], VECTOR_VERSION);
    }

    /// One health surface: every probe path is served on `metrics_port`, so no
    /// extra port may claim to answer them.
    #[test]
    fn test_contract_advertises_no_second_health_port() {
        let c = contract();
        assert!(
            !c.extra_ports.iter().any(|p| p.name == "health"),
            "a second health port is a second answer to readiness"
        );
    }

    /// The direct transport needs a listener the chart can put a Service in
    /// front of, and the engine reads the port from apps.yaml `endpoints.push`.
    /// Both must be 6000, the convention every DFE stage's listener follows.
    #[test]
    fn test_contract_advertises_the_push_listener() {
        let c = contract();
        let push = c
            .extra_ports
            .iter()
            .find(|p| p.name == "push")
            .expect("the direct transport's Push listener must be a declared port");
        assert_eq!(push.port, 6000);
        // The Push listener is cleartext gRPC, so a proxy in front of it must speak h2c.
        assert_eq!(push.app_protocol, "kubernetes.io/h2c");
    }

    /// KEDA stays on and scales on CPU alone.
    #[test]
    fn test_keda_scales_on_cpu_without_a_lag_trigger() {
        let c = contract();
        let keda = c.keda.as_ref().expect("keda present");
        assert!(keda.enabled);
        assert!(keda.cpu_enabled);
        assert!(
            !keda.kafka_trigger.enabled,
            "raw consumer-group lag scales out when a downstream stage is broken"
        );
    }

    /// `generate-artefacts` and the generators write nothing for a contract
    /// that fails these checks, and the library refuses to render a values
    /// path the contract leaves unset -- as it did the lag trigger's
    /// `config.kafka.*`, which this app has no block for.
    #[test]
    fn test_contract_passes_the_generator_checks() {
        let c = contract();
        c.validate()
            .expect("every generator must accept the contract");
        scalo::deployment::assert_listeners_declared(&c);
        let unresolved = c.unresolved_values_paths();
        assert!(
            unresolved.is_empty(),
            "the chart reads values default_config never sets: {unresolved:?}"
        );
    }

    /// The root filesystem is read-only, so every directory the app writes by
    /// default sits under an ungated writable path or the pod cannot start.
    #[test]
    fn test_every_default_write_directory_is_a_writable_path() {
        let c = contract();
        assert!(c.security.read_only_root_filesystem);
        let vector = crate::config::Config::default().vector;
        for dir in [&vector.data_dir, &vector.config_dir] {
            assert!(
                c.writable_paths
                    .iter()
                    .any(|writable| writable.when.is_none()
                        && std::path::Path::new(dir).starts_with(&writable.path)),
                "no writable path covers {dir}: {:?}",
                c.writable_paths
            );
        }
    }

    /// The committed reflectable artefacts under docs/ must not drift from a
    /// fresh regen. Regenerate with `dfe-transform-vector config-schema --dir docs`.
    #[test]
    fn test_config_artifacts_do_not_drift() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("docs");
        scalo::deployment::assert_no_config_artifact_drift(&contract(), dir);
    }
}
