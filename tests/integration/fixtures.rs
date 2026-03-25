// Project:   dfe-transform-vector
// File:      tests/integration/fixtures.rs
// Purpose:   Tests that exercise the fixture config and transform library
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Fixture library tests.
//!
//! Verifies that all fixtures in `tests/fixtures/` are valid and can be
//! assembled through the full config pipeline. These tests serve as both
//! correctness checks and living documentation for common transform patterns.

use std::fs;
use std::path::Path;

use dfe_transform_vector::config::assembler;
use dfe_transform_vector::config::loader::Config;
use dfe_transform_vector::config::transforms::load_transforms;
use dfe_transform_vector::config::wiring::{auto_wire, extract_components, validate_dag};
use tempfile::TempDir;

const FIXTURE_TRANSFORMS: &str = "tests/fixtures/transforms";
const FIXTURE_CONFIGS: &str = "tests/fixtures/configs";

// ---------------------------------------------------------------------------
// Config fixture tests
// ---------------------------------------------------------------------------

#[test]
fn fixture_minimal_config_loads_and_validates() {
    let path = Path::new(FIXTURE_CONFIGS).join("minimal.yaml");
    let config = Config::load(Some(path.to_str().unwrap())).expect("minimal.yaml should load");
    config.validate().expect("minimal.yaml should be valid");

    assert_eq!(config.pipeline.name, "test-pipeline");
    assert_eq!(config.source.topics, vec!["raw.input"]);
    assert_eq!(config.sink.topic, "enriched.output");
    assert_eq!(config.vector.version_check, "disabled");
}

#[test]
fn fixture_sasl_config_loads_and_validates() {
    let path = Path::new(FIXTURE_CONFIGS).join("with_sasl.yaml");
    let config = Config::load(Some(path.to_str().unwrap())).expect("with_sasl.yaml should load");
    config.validate().expect("with_sasl.yaml should be valid");

    assert!(config.source.sasl.enabled);
    assert!(config.source.tls.enabled);
    assert!(config.sink.sasl.enabled);
    assert!(config.sink.tls.enabled);
    // Credentials stay as env-var references — Vector interpolates at runtime
    assert!(
        config
            .source
            .sasl
            .username
            .contains("${KAFKA_SASL_USERNAME}")
    );
    assert!(
        config
            .source
            .sasl
            .password
            .contains("${KAFKA_SASL_PASSWORD}")
    );
    assert_eq!(config.sink.compression, "zstd");
}

#[test]
fn fixture_with_transforms_config_assembles_full_chain() {
    let config_path = Path::new(FIXTURE_CONFIGS).join("with_transforms.yaml");
    let config = Config::load(Some(config_path.to_str().unwrap()))
        .expect("with_transforms.yaml should load");
    config
        .validate()
        .expect("with_transforms.yaml should be valid");

    let output_dir = TempDir::new().unwrap();
    assembler::assemble(&config, output_dir.path())
        .expect("assembly should succeed with fixture transforms");

    // Source, sink, and observability should exist
    assert!(output_dir.path().join("00_source.yaml").exists());
    assert!(output_dir.path().join("90_sink.yaml").exists());
    assert!(output_dir.path().join("99_observability.yaml").exists());

    // All 5 transform fixtures should be in the config dir (flat, prefixed 50_)
    let mut names: Vec<String> = fs::read_dir(output_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("50_"))
        .collect();
    names.sort();
    assert_eq!(
        names.len(),
        5,
        "expected 5 transform fixtures, got {names:?}"
    );
}

// ---------------------------------------------------------------------------
// Individual transform fixture tests
// ---------------------------------------------------------------------------

/// Load and parse a single transform fixture, returning its loaded YAML.
fn load_fixture_transform(
    filename: &str,
) -> Vec<dfe_transform_vector::config::transforms::LoadedTransform> {
    let path = Path::new(FIXTURE_TRANSFORMS).join(filename);
    load_transforms(&dfe_transform_vector::config::loader::TransformConfig {
        dir: None,
        files: Some(vec![path.to_string_lossy().into_owned()]),
    })
    .unwrap_or_else(|e| panic!("failed to load {filename}: {e}"))
}

#[test]
fn fixture_parse_message_is_valid_transform() {
    let transforms = load_fixture_transform("01_parse_message.yaml");
    assert_eq!(transforms.len(), 1);

    let components = extract_components(&transforms).unwrap();
    assert!(
        components
            .iter()
            .any(|c| c.label == "parse_message" && c.kind == "transform"),
        "expected parse_message transform component"
    );
}

#[test]
fn fixture_enrich_metadata_is_valid_transform() {
    let transforms = load_fixture_transform("02_enrich_metadata.yaml");
    assert_eq!(transforms.len(), 1);

    let components = extract_components(&transforms).unwrap();
    assert!(
        components
            .iter()
            .any(|c| c.label == "enrich_metadata" && c.kind == "transform"),
        "expected enrich_metadata transform component"
    );
}

#[test]
fn fixture_filter_noise_is_valid_transform() {
    let transforms = load_fixture_transform("03_filter_noise.yaml");
    assert_eq!(transforms.len(), 1);

    let components = extract_components(&transforms).unwrap();
    assert!(
        components
            .iter()
            .any(|c| c.label == "filter_noise" && c.kind == "transform"),
        "expected filter_noise transform component"
    );
}

#[test]
fn fixture_redact_pii_is_valid_transform() {
    let transforms = load_fixture_transform("04_redact_pii.yaml");
    assert_eq!(transforms.len(), 1);

    let components = extract_components(&transforms).unwrap();
    assert!(
        components
            .iter()
            .any(|c| c.label == "redact_pii" && c.kind == "transform"),
        "expected redact_pii transform component"
    );
}

#[test]
fn fixture_reduce_aggregate_is_valid_transform() {
    let transforms = load_fixture_transform("05_reduce_aggregate.yaml");
    assert_eq!(transforms.len(), 1);

    let components = extract_components(&transforms).unwrap();
    assert!(
        components
            .iter()
            .any(|c| c.label == "reduce_aggregate" && c.kind == "transform"),
        "expected reduce_aggregate transform component"
    );
}

// ---------------------------------------------------------------------------
// DAG wiring tests over fixture chain
// ---------------------------------------------------------------------------

#[test]
fn fixture_full_chain_wires_without_errors() {
    // Load all 5 fixtures as a chain and verify DAG wires cleanly
    let files: Vec<String> = (1..=5)
        .map(|i| {
            let dir = Path::new(FIXTURE_TRANSFORMS);
            fs::read_dir(dir)
                .unwrap()
                .filter_map(|e| {
                    let e = e.unwrap();
                    let name = e.file_name().to_string_lossy().into_owned();
                    if name.starts_with(&format!("0{i}_")) {
                        Some(dir.join(&name).to_string_lossy().into_owned())
                    } else {
                        None
                    }
                })
                .next()
                .unwrap_or_else(|| panic!("fixture 0{i}_*.yaml not found"))
        })
        .collect();

    let transforms = load_transforms(&dfe_transform_vector::config::loader::TransformConfig {
        dir: None,
        files: Some(files),
    })
    .expect("all fixtures should load");

    let components = extract_components(&transforms).expect("components should extract");
    let wiring = auto_wire(components).expect("auto-wire should succeed");
    validate_dag(&wiring).expect("DAG should be valid");

    // The chain has 5 transforms — sink inputs should be the last one
    assert_eq!(wiring.sink_inputs.len(), 1);
    assert_eq!(wiring.sink_inputs[0], "reduce_aggregate");
}
