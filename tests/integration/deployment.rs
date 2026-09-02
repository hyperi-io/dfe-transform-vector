// Project:   dfe-transform-vector
// File:      tests/integration/deployment.rs
// Purpose:   Deployment contract tests — catch broken CI artefacts
// Language:  Rust
//
// License:   BUSL-1.1
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
    // base_image is resolved via the scalo cascade (deployment.base_image),
    // defaulting to debian:trixie-slim. Don't pin a distro here -- assert it is
    // non-empty and carries an explicit tag so the test survives an org-wide
    // base-image change.
    assert!(!c.base_image.is_empty(), "base_image must not be empty");
    assert!(
        c.base_image.contains(':'),
        "base_image must include an explicit tag: {}",
        c.base_image
    );

    // Health paths match the DFE contract
    assert_eq!(c.health.liveness_path, "/livez");
    assert_eq!(c.health.readiness_path, "/readyz");
    assert_eq!(c.health.metrics_path, "/metrics");

    // The probe paths above are all served on metrics_port; the only extra
    // port is Vector's own API.
    let port_names: Vec<&str> = c.extra_ports.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(port_names, vec!["vector-api"]);

    let vector_api = c
        .extra_ports
        .iter()
        .find(|p| p.name == "vector-api")
        .unwrap();
    assert_eq!(vector_api.port, 8686);

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

    // Base image -- cascade-resolved (debian:trixie-slim by default), so just
    // assert a FROM line with an explicitly tagged image, not a specific distro.
    assert!(
        dockerfile.contains(&format!("FROM {}", deployment::contract().base_image)),
        "missing/incorrect base image FROM line in Dockerfile"
    );

    // Wrapper binary COPY
    assert!(
        dockerfile.contains("COPY dfe-transform-vector /usr/local/bin/dfe-transform-vector"),
        "missing wrapper binary COPY in Dockerfile"
    );

    // Vector binary install (Issue #13 — published image was missing this).
    assert!(
        dockerfile.contains("ARG VECTOR_VERSION"),
        "Dockerfile missing Vector version ARG — Vector binary install absent"
    );
    assert!(
        dockerfile.contains("packages.timber.io/vector"),
        "Dockerfile missing Vector tarball download from packages.timber.io"
    );
    assert!(
        dockerfile.contains("/usr/local/bin/vector --version"),
        "Dockerfile missing build-time `vector --version` smoke check — \
         a broken/missing binary would not be caught at image build time"
    );
    assert!(
        dockerfile.contains(&format!(
            "ARG VECTOR_VERSION={}",
            deployment::VECTOR_VERSION
        )),
        "Vector version must be pinned to deployment::VECTOR_VERSION, not 'latest'"
    );
    assert!(
        dockerfile.contains(&format!(
            "io.hyperi.vector.version=\"{}\"",
            deployment::VECTOR_VERSION
        )),
        "Dockerfile missing OCI label io.hyperi.vector.version for audit"
    );

    // Vector data directories created before USER switch.
    assert!(
        dockerfile.contains("/var/lib/vector"),
        "Dockerfile missing /var/lib/vector data dir"
    );
    assert!(
        dockerfile.contains("/etc/dfe-transform-vector/transforms"),
        "Dockerfile missing /etc/dfe-transform-vector/transforms transform dir"
    );

    // Splice order: Vector install must land BEFORE `USER appuser` so
    // root can chown the directories.
    let vector_idx = dockerfile
        .find("ARG VECTOR_VERSION")
        .expect("vector install present");
    let user_idx = dockerfile.find("USER ").expect("USER directive present");
    assert!(
        vector_idx < user_idx,
        "Vector install must be spliced BEFORE USER directive"
    );

    // Config mount path matches sibling DFE components (Issue #11).
    assert!(
        dockerfile.contains("/etc/dfe-transform-vector/config.yaml"),
        "Dockerfile CMD must reference /etc/dfe-transform-vector/config.yaml \
         (matches sibling DFE components like dfe-loader)"
    );
    assert!(
        !dockerfile.contains("/etc/dfe/config.yaml"),
        "Dockerfile must NOT reference legacy /etc/dfe/config.yaml — \
         that path is inconsistent with sibling components"
    );
}

#[test]
fn checked_in_dockerfile_matches_emit_dockerfile() {
    // The checked-in Dockerfile is autogenerated from emit_dockerfile().
    // If they drift, CI publishes from a stale Dockerfile and bugs like
    // issue #13 (missing Vector binary) sneak through.
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("Dockerfile");
    let on_disk = std::fs::read_to_string(&path).expect("read Dockerfile");
    let emitted = deployment::emit_dockerfile();
    assert_eq!(
        on_disk.trim(),
        emitted.trim(),
        "Dockerfile on disk does not match emit_dockerfile() output — \
         regenerate with: `cargo run -- emit-dockerfile > Dockerfile`"
    );
}

#[test]
fn contract_uses_standard_mount_path() {
    // Sibling DFE components (loader, receiver, fetcher, archiver) all
    // mount config at /etc/<component-name>/config.yaml. Don't be the
    // outlier that forces compose authors to special-case this service.
    // Issue #11.
    let c = deployment::contract();
    assert_eq!(
        c.config_mount_path, "/etc/dfe-transform-vector/config.yaml",
        "config_mount_path must follow /etc/<component-name>/ convention"
    );
    assert!(
        c.entrypoint_args
            .contains(&"/etc/dfe-transform-vector/config.yaml".to_string()),
        "entrypoint --config arg must match config_mount_path"
    );
}

#[test]
fn emit_chart_generates_without_panic() {
    let contract = deployment::contract();
    let dir = tempfile::tempdir().expect("create temp dir");
    let dir_path = dir.path().to_str().unwrap();

    let result = scalo::deployment::generate_chart(&contract, dir_path, None);
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

/// The committed KEDA ScaledObject is a deliberate hand-edit of the generated
/// one, and a fresh `emit-chart` over `chart/` silently clobbers it.
///
/// The generator addresses `.Values.config.kafka.*`, which this app has no
/// block for, and hardcodes the broker's SASL mechanism and TLS mode. A
/// regenerated ScaledObject therefore renders empty `bootstrapServers`,
/// `consumerGroup` and `topic`, and describes an auth posture the app may not
/// be using — all valid YAML that KEDA accepts and then never scales on, with
/// nothing logged.
#[test]
fn checked_in_keda_scaledobject_survives_emit_chart() {
    let committed_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("chart/templates/keda-scaledobject.yaml");
    let committed = std::fs::read_to_string(&committed_path).expect("read committed ScaledObject");

    // Assert on the trigger metadata itself, not on a substring anywhere in the
    // file, so the explanatory note at the top cannot satisfy or trip it.
    let directive = |key: &str| {
        committed
            .lines()
            .map(str::trim)
            .find(|l| l.starts_with(&format!("{key}:")))
            .unwrap_or_else(|| panic!("committed ScaledObject has no `{key}:` line"))
            .to_string()
    };

    for (key, expected) in [
        ("bootstrapServers", ".Values.config.source.brokers"),
        ("consumerGroup", ".Values.config.source.group_id"),
        ("sasl", ".Values.config.source.sasl"),
        ("tls", ".Values.config.source.tls"),
        ("unsafeSsl", ".Values.config.source.tls"),
    ] {
        let line = directive(key);
        assert!(
            line.contains(expected),
            "chart/templates/keda-scaledobject.yaml has been overwritten by `emit-chart`: \
             `{line}` does not read {expected}. The generator addresses config.kafka.*, which \
             this chart does not define, and hardcodes the mechanism and TLS mode, so KEDA \
             would get an empty or wrong value here and stop scaling with nothing logged."
        );
    }
    assert!(
        committed.contains("index .Values.config.source.topics 0"),
        "committed ScaledObject no longer derives the topic from config.source.topics"
    );

    let values = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("chart/values.yaml"),
    )
    .expect("read chart values");
    let values: serde_yaml_ng::Value =
        serde_yaml_ng::from_str(&values).expect("parse chart values");
    let source = values
        .get("config")
        .and_then(|c| c.get("source"))
        .expect("chart values define config.source");
    for key in ["brokers", "group_id", "topics"] {
        assert!(
            source.get(key).is_some(),
            "chart values.yaml has no config.source.{key} for the ScaledObject to read"
        );
    }
    // The trigger dereferences these unguarded, so a missing one is a Helm
    // render error rather than a bad value.
    for (block, key) in [
        ("sasl", "enabled"),
        ("sasl", "mechanism"),
        ("tls", "enabled"),
    ] {
        assert!(
            source.get(block).and_then(|b| b.get(key)).is_some(),
            "chart values.yaml has no config.source.{block}.{key}; the ScaledObject trigger \
             dereferences it and Helm fails to render the chart at all"
        );
    }

    // The divergence itself: if scalo's generator ever converges on this shape,
    // the hand-edit and its note in the template are stale.
    let dir = tempfile::tempdir().expect("create temp dir");
    scalo::deployment::generate_chart(&deployment::contract(), dir.path().to_str().unwrap(), None)
        .expect("chart generation");
    let generated = std::fs::read_to_string(dir.path().join("templates/keda-scaledobject.yaml"))
        .expect("read generated ScaledObject");
    assert_ne!(
        generated.trim(),
        committed.trim(),
        "the generator now emits the committed ScaledObject verbatim — drop the hand-edit \
         note from chart/templates/keda-scaledobject.yaml and this divergence assertion"
    );
}

/// The committed KEDA TriggerAuthentication is a deliberate hand-edit of the
/// generated one, and a fresh `emit-chart` over `chart/` silently clobbers it.
///
/// The generator binds the username to the `sasl` parameter. KEDA's kafka
/// scaler reads `sasl` as the mechanism enum and takes the principal in
/// `username`, so a regenerated TriggerAuthentication supplies no username and
/// the scaler answers "no username given" instead of a lag metric.
#[test]
fn checked_in_keda_triggerauth_survives_emit_chart() {
    let committed_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("chart/templates/keda-triggerauth.yaml");
    let committed =
        std::fs::read_to_string(&committed_path).expect("read committed TriggerAuthentication");

    // Assert on the secretTargetRef entries, not on a substring anywhere in the
    // file, so the explanatory note at the top cannot satisfy or trip it.
    let parameters: Vec<String> = committed
        .lines()
        .map(str::trim)
        .filter_map(|l| l.strip_prefix("- parameter: "))
        .map(str::to_string)
        .collect();

    assert!(
        parameters.iter().any(|p| p == "username"),
        "chart/templates/keda-triggerauth.yaml has been overwritten by `emit-chart`: its \
         secretTargetRef binds {parameters:?} and not `username`, so KEDA's kafka scaler \
         answers \"no username given\" and returns no lag metric."
    );
    assert!(
        !parameters.iter().any(|p| p == "sasl"),
        "chart/templates/keda-triggerauth.yaml binds a secret to `sasl`, which KEDA parses as \
         the mechanism enum (none, plaintext, scram_sha256, scram_sha512, oauthbearer, \
         gssapi) and rejects. The mechanism belongs in the ScaledObject trigger metadata."
    );

    // The divergence itself: if scalo's generator ever converges on this shape,
    // the hand-edit and its note in the template are stale.
    let dir = tempfile::tempdir().expect("create temp dir");
    scalo::deployment::generate_chart(&deployment::contract(), dir.path().to_str().unwrap(), None)
        .expect("chart generation");
    let generated = std::fs::read_to_string(dir.path().join("templates/keda-triggerauth.yaml"))
        .expect("read generated TriggerAuthentication");
    assert_ne!(
        generated.trim(),
        committed.trim(),
        "the generator now emits the committed TriggerAuthentication verbatim — drop the \
         hand-edit note from chart/templates/keda-triggerauth.yaml and this divergence \
         assertion"
    );
}

/// The chart's expected Vector version must match the one the Dockerfile bakes.
///
/// They drifted by nine minor versions once already -- the chart said 0.48.0
/// against a 0.57.0 binary -- and only `version_check: warn` kept pods
/// starting. Under `strict` that combination refuses to start at all.
#[test]
fn chart_expects_the_vector_version_the_image_ships() {
    let values = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("chart/values.yaml"),
    )
    .expect("read chart values");
    let values: serde_yaml_ng::Value =
        serde_yaml_ng::from_str(&values).expect("parse chart values");

    let charted = values
        .get("config")
        .and_then(|c| c.get("vector"))
        .and_then(|v| v.get("version"))
        .and_then(serde_yaml_ng::Value::as_str)
        .expect("chart values define config.vector.version");

    assert_eq!(
        charted,
        deployment::VECTOR_VERSION,
        "chart/values.yaml expects Vector {charted}, the image ships {}",
        deployment::VECTOR_VERSION
    );
}

#[test]
fn emit_compose_generates_without_panic() {
    let contract = deployment::contract();
    let compose = scalo::deployment::generate_compose_fragment(&contract);

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
