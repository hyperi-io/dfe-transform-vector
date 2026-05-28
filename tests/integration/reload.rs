// Project:   dfe-transform-vector
// File:      tests/integration/reload.rs
// Purpose:   Hot-reload integration tests — file change detection and classification
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Reload integration — file change detection triggers re-assembly.

use dfe_transform_vector::config::Config;

#[test]
fn reload_file_change_triggers_reassembly() {
    use dfe_transform_vector::config::assembler;
    use dfe_transform_vector::config::reload::{ChangeKind, classify_change};

    let dir = tempfile::tempdir().unwrap();
    let transforms_dir = dir.path().join("transforms");
    std::fs::create_dir_all(&transforms_dir).unwrap();

    // Write a transform file
    let transform_yaml = r#"
transforms:
  parse:
    type: remap
    inputs: ["dfe_source"]
    source: |
      .parsed = true
"#;
    std::fs::write(transforms_dir.join("01_parse.yaml"), transform_yaml).unwrap();

    // Build config pointing to this transforms dir
    let config_yaml = format!(
        r#"
pipeline:
  name: "reload-test"
source:
  brokers: ["localhost:9092"]
  topics: ["raw_input"]
  group_id: "reload-test-group"
sink:
  brokers: ["localhost:9092"]
  topic: "enriched_output"
  encoding: "json"
transforms:
  dir: "{}"
vector:
  binary: "/usr/local/bin/vector"
  data_dir: "{}"
  version_check: "disabled"
"#,
        transforms_dir.to_string_lossy(),
        dir.path().join("data").to_string_lossy()
    );

    let config_path = dir.path().join("config.yaml");
    std::fs::write(&config_path, &config_yaml).unwrap();

    let config = Config::load(Some(config_path.to_str().unwrap())).unwrap();
    config.validate().unwrap();

    // Assemble config directory
    let config_dir = dir.path().join("vector-config");
    assembler::assemble(&config, &config_dir).unwrap();

    // Verify assembled files exist
    assert!(config_dir.join("00_global.yaml").exists());
    assert!(config_dir.join("00_source.yaml").exists());
    assert!(config_dir.join("90_sink.yaml").exists());
    assert!(config_dir.join("99_observability.yaml").exists());

    // Verify transform was assembled
    let assembled: Vec<_> = std::fs::read_dir(&config_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("50_"))
        .collect();
    assert_eq!(assembled.len(), 1, "expected 1 transform file");

    // Now change the transform file (add a second transform)
    let transform2_yaml = r#"
transforms:
  enrich:
    type: remap
    inputs: ["parse"]
    source: |
      .enriched = true
"#;
    std::fs::write(transforms_dir.join("02_enrich.yaml"), transform2_yaml).unwrap();

    // Classify this as a transform-only change (re-load config to pick up new file)
    let new_config = Config::load(Some(config_path.to_str().unwrap())).unwrap();
    // The Config structs are identical (same YAML), but the transforms dir has new files
    // classify_change compares Config fields, not filesystem — so it returns None
    let change = classify_change(&config, &new_config);
    assert_eq!(change, ChangeKind::None, "config struct unchanged");

    // Re-assemble should pick up the new transform file
    assembler::assemble(&new_config, &config_dir).unwrap();
    let assembled_after: Vec<_> = std::fs::read_dir(&config_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("50_"))
        .collect();
    assert_eq!(
        assembled_after.len(),
        2,
        "expected 2 transform files after adding enrich"
    );
}

#[test]
fn reload_classify_multi_field_change_is_unsafe() {
    use dfe_transform_vector::config::reload::{ChangeKind, classify_change};

    let old = Config::load(Some("tests/fixtures/configs/minimal.yaml")).unwrap();
    let mut new = old.clone();

    // Change both transforms AND source — should be unsafe (not transforms-only)
    new.transforms.dir = Some("/new/transforms".into());
    new.source.brokers = vec!["different:9092".into()];

    assert_eq!(classify_change(&old, &new), ChangeKind::Unsafe);
}
