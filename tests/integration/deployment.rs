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
use scalo::config::flat_env::{ApplyFlatEnv, Normalize};

use crate::integration::config_env::with_vars;

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

    // The probe paths above are all served on metrics_port; the one extra port
    // is the direct transport's Push listener, and it listens only there.
    let port_names: Vec<&str> = c.extra_ports.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(port_names, vec!["push"]);
    let push = &c.extra_ports[0];
    assert_eq!(
        push.when.as_ref().map(ToString::to_string).as_deref(),
        Some("config.source.transport is \"direct\""),
        "the Push port must render only on the direct transport"
    );
    assert!(
        c.undeclared_listeners().is_empty(),
        "every listener in default_config needs a port: {:?}",
        c.undeclared_listeners()
    );

    // KEDA config present
    assert!(c.keda.is_some(), "KEDA contract must be defined");
    let keda = c.keda.as_ref().unwrap();
    assert!(keda.min_replicas >= 1);
    assert!(keda.max_replicas > keda.min_replicas);

    // One required Kafka Secret, mounted into both endpoints' SASL names.
    assert_eq!(c.secrets.len(), 1, "one Kafka secret group");
    assert_eq!(c.secrets[0].group_name, "kafka");
    assert!(!c.secrets[0].optional);
    let env_vars: Vec<&str> = c.secrets[0]
        .env_vars
        .iter()
        .map(|s| s.env_var.as_str())
        .collect();
    assert_eq!(
        env_vars,
        [
            "DFE_TRANSFORM_SOURCE_SASL_USERNAME",
            "DFE_TRANSFORM_SOURCE_SASL_PASSWORD",
            "DFE_TRANSFORM_SINK_SASL_USERNAME",
            "DFE_TRANSFORM_SINK_SASL_PASSWORD",
        ]
    );

    // Default config present and parseable
    assert!(
        c.default_config.is_some(),
        "default config must be embedded in contract"
    );
}

/// The chart mounts a Secret under every declared name, so a name the config
/// never reads leaves the credential silently unused.
///
/// Each name spells the field it fills -- `DFE_TRANSFORM_SINK_SASL_PASSWORD` is
/// `sink.sasl.password` -- so a name read into the other endpoint fails here too.
#[test]
fn every_contract_secret_env_var_reaches_the_config() {
    use dfe_transform_vector::config::loader::Config;

    let c = deployment::contract();
    let prefix = c.env_prefix.clone();

    for group in &c.secrets {
        for env in &group.env_vars {
            let field = env
                .env_var
                .strip_prefix(&format!("{prefix}_"))
                .unwrap_or_else(|| panic!("{} lacks the {prefix} prefix", env.env_var));
            let pointer = format!("/{}", field.to_ascii_lowercase().replace('_', "/"));

            let sentinel = format!("sentinel-{}", env.key_name);
            let config = with_vars(&[(env.env_var.as_str(), Some(sentinel.as_str()))], || {
                let mut config = Config::default();
                config.apply_flat_env(&prefix);
                config
            });

            let applied = serde_json::to_value(&config).expect("config serialises");
            let reached = applied
                .pointer(&pointer)
                .and_then(serde_json::Value::as_str)
                == Some(sentinel.as_str());
            // The message carries the env var and group names only, never a value.
            assert!(
                reached,
                "{} ({}) was set and the config field its name spells did not read it",
                env.env_var, group.group_name
            );
        }
    }
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

/// The contract's default config, with the Kafka Secret mounted into the env
/// vars the contract declares, must be a config the app accepts.
///
/// `emit-chart` writes `default_config` into its values, so a chart generated
/// from the contract is only as good as this, and anything `validate()` rejects
/// is a crash-loop before Vector starts.
#[test]
fn the_contract_default_config_is_one_the_app_accepts() {
    use dfe_transform_vector::config::loader::Config;

    let c = deployment::contract();
    let default_config = c
        .default_config
        .clone()
        .expect("contract carries a default config");
    let mut config: Config = serde_json::from_value(default_config)
        .expect("the contract's default config must deserialise into the app's Config");

    let mounted: Vec<(&str, Option<&str>)> = c
        .secrets
        .iter()
        .flat_map(|group| &group.env_vars)
        .map(|env| (env.env_var.as_str(), Some("from-the-secret")))
        .collect();
    with_vars(&mounted, || {
        config.apply_flat_env(&c.env_prefix);
        config.normalize();
    });

    config
        .validate()
        .expect("deployment::contract()'s default_config does not validate");
}

/// Vector's API takes unauthenticated requests and nothing here is a client,
/// so no shipped artefact may publish its port.
#[test]
fn no_artefact_publishes_the_vector_api() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let dockerfile = std::fs::read_to_string(root.join("Dockerfile")).expect("read Dockerfile");
    assert!(
        !dockerfile.contains("8686"),
        "Dockerfile still publishes Vector's API port"
    );
    let c = deployment::contract();
    assert!(
        c.extra_ports.iter().all(|p| p.port != 8686),
        "the contract declares Vector's API port: {:?}",
        c.extra_ports
    );
    let default_config = c.default_config.expect("contract carries default config");
    assert_eq!(default_config["vector"]["api_enabled"], false);
}

/// Every config the product ships, as the app reads it.
fn shipped_configs() -> Vec<(String, dfe_transform_vector::config::loader::Config)> {
    use dfe_transform_vector::config::loader::Config;

    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut out = Vec::new();

    out.push((
        "deployment::contract().default_config".to_string(),
        serde_json::from_value::<Config>(
            deployment::contract()
                .default_config
                .expect("contract default config"),
        )
        .expect("contract config"),
    ));

    for entry in std::fs::read_dir(root.join("tests/fixtures/configs")).expect("fixture configs") {
        let path = entry.expect("fixture entry").path();
        let text = std::fs::read_to_string(&path).expect("read fixture");
        out.push((
            path.display().to_string(),
            serde_yaml_ng::from_str::<Config>(&text).expect("fixture config"),
        ));
    }
    out
}

/// Vector 0.57+ expands no `${VAR}` unless started with
/// `--dangerously-allow-env-var-interpolation`, which the supervisor never
/// passes. A placeholder that survives assembly therefore reaches Vector as
/// literal text: a SASL credential logs in to the broker as the string
/// `${KAFKA_SASL_PASSWORD}`.
#[test]
fn no_env_placeholder_survives_assembly_of_a_shipped_config() {
    let flag = "--dangerously-allow-env-var-interpolation";
    assert!(
        !dfe_transform_vector::vector::vector_args(std::path::Path::new("/cfg"))
            .iter()
            .any(|a| a == flag),
        "the supervisor must not start Vector with {flag}"
    );

    for (label, mut config) in shipped_configs() {
        let work = tempfile::tempdir().expect("work dir");
        // A config that names a credential directory gets one, so assembly can
        // check the files are there.
        let mounted = work.path().join("secret");
        std::fs::create_dir_all(&mounted).expect("secret dir");
        std::fs::write(mounted.join("username"), "u").expect("username file");
        std::fs::write(mounted.join("password"), "p").expect("password file");
        for sasl in [&mut config.source.sasl, &mut config.sink.sasl] {
            if sasl.secret_dir.is_some() {
                sasl.secret_dir = Some(mounted.to_string_lossy().into_owned());
            }
        }
        // A container path is read inside the directories the image creates,
        // so a config naming one the image lacks fails assembly here.
        let image = work.path().join("image");
        image_filesystem(&image);
        if let Some(dir) = config
            .transforms
            .dir
            .clone()
            .filter(|d| std::path::Path::new(d).is_absolute())
        {
            config.transforms.dir = Some(inside(&image, &dir).to_string_lossy().into_owned());
        }

        let assembled = work.path().join("config");
        dfe_transform_vector::config::assembler::assemble(&config, &assembled)
            .unwrap_or_else(|e| panic!("{label} does not assemble: {e}"));
        // The credential files Vector reads count as much as the YAML does.
        let mut pending = vec![assembled];
        while let Some(dir) = pending.pop() {
            for entry in std::fs::read_dir(&dir).expect("assembled dir") {
                let path = entry.expect("assembled entry").path();
                if path.is_dir() {
                    pending.push(path);
                    continue;
                }
                let text = std::fs::read_to_string(&path).expect("assembled file");
                assert!(
                    !text.contains("${"),
                    "{label}: {} carries a ${{...}} placeholder Vector will read literally",
                    path.display()
                );
            }
        }
    }
}

/// `path`, a container path, inside the filesystem stood up at `root`.
fn inside(root: &std::path::Path, path: &str) -> std::path::PathBuf {
    root.join(path.trim_start_matches('/'))
}

/// Stand up under `root` the directories the image creates.
fn image_filesystem(root: &std::path::Path) {
    let dockerfile = deployment::emit_dockerfile();
    let created = dockerfile
        .lines()
        .find_map(|l| l.trim().strip_prefix("RUN mkdir -p "))
        .expect("the image creates its directories with one mkdir");
    for dir in created
        .split_whitespace()
        .take_while(|t| t.starts_with('/'))
    {
        std::fs::create_dir_all(inside(root, dir)).expect("image directory");
    }
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
