// Project:   dfe-transform-vector
// File:      tests/integration/reload.rs
// Purpose:   Hot-reload integration tests — file change detection and classification
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Reload integration — file change detection triggers re-assembly, and a
//! reload never manufactures readiness the subprocess has not earned.

use crate::integration::config_env::load_config;

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

    let config = load_config(Some(config_path.to_str().unwrap())).unwrap();
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
    let new_config = load_config(Some(config_path.to_str().unwrap())).unwrap();
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

/// Write an executable that succeeds whatever it is asked to do, standing in
/// for a Vector binary whose `validate` passes.
fn write_always_ok_binary(dir: &std::path::Path) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt;

    let path = dir.join("vector-that-validates");
    std::fs::write(&path, "#!/bin/sh\nexit 0\n").expect("write the fake binary");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
        .expect("make the fake binary executable");
    path
}

/// A transform change landing while Vector is dead must not report ready.
///
/// The failure it guards: Vector crash-loops with the backoff grown to 60s, a
/// ConfigMap edit lands during the backoff, and the reload marks the lifecycle
/// `Reloading` (which counts as ready) and then `Running` — so `/readyz`
/// answers 200 for the rest of the backoff with no subprocess at all, and KEDA
/// and Argo both read the pod as healthy.
#[tokio::test]
async fn a_reload_while_the_subprocess_is_dead_never_reports_ready() {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use dfe_transform_vector::config::reload::{ReloadTrigger, run_reload_loop};
    use dfe_transform_vector::metrics::WrapperMetrics;
    use dfe_transform_vector::vector::Lifecycle;
    use dfe_transform_vector::vector::lifecycle::State;

    use crate::common::metrics_fixture::metrics_manager;

    let dir = tempfile::tempdir().unwrap();
    let transforms_dir = dir.path().join("transforms");
    std::fs::create_dir_all(&transforms_dir).unwrap();
    std::fs::write(
        transforms_dir.join("01_parse.yaml"),
        "transforms:\n  parse:\n    type: remap\n    inputs: [\"dfe_source\"]\n    source: |\n      .parsed = true\n",
    )
    .unwrap();

    let binary = write_always_ok_binary(dir.path());
    let config_path = dir.path().join("config.yaml");
    std::fs::write(
        &config_path,
        format!(
            r#"
pipeline:
  name: "dead-vector-reload"
source:
  brokers: ["localhost:9092"]
  topics: ["raw_input"]
  group_id: "dead-vector-reload-group"
sink:
  brokers: ["localhost:9092"]
  topic: "enriched_output"
  encoding: "json"
transforms:
  dir: "{}"
vector:
  binary: "{}"
  data_dir: "{}"
  version_check: "disabled"
"#,
            transforms_dir.to_string_lossy(),
            binary.to_string_lossy(),
            dir.path().join("data").to_string_lossy()
        ),
    )
    .unwrap();

    // The loop's starting config differs from the file in `transforms` only, so
    // the reload classifies as TransformsOnly and runs the hot-reload body.
    let on_disk = load_config(Some(config_path.to_str().unwrap())).unwrap();
    let mut starting = on_disk.clone();
    starting.transforms.dir = None;

    // The state a crash-looping pod is actually in when the edit lands, and no
    // PID to SIGHUP.
    let lifecycle = Lifecycle::new();
    lifecycle.set(State::Crashed);
    let vector_pid = Arc::new(Mutex::new(None));

    // Record every transition rather than sampling, so a momentary ready state
    // in the middle of the reload cannot slip past.
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&seen);
    let mut rx = lifecycle.subscribe();
    let watcher = tokio::spawn(async move {
        while rx.changed().await.is_ok() {
            recorded
                .lock()
                .expect("state log")
                .push(*rx.borrow_and_update());
        }
    });

    let config_dir = dir.path().join("vector-config");
    let metrics = Arc::new(WrapperMetrics::register(metrics_manager(), "reload-test"));
    let (reload_tx, reload_rx) = tokio::sync::mpsc::channel(1);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let loop_config_dir = config_dir.clone();
    let loop_lifecycle = lifecycle.clone();
    let loop_config_path = config_path.to_string_lossy().into_owned();
    let task = tokio::spawn(async move {
        run_reload_loop(
            starting,
            Some(loop_config_path),
            loop_config_dir,
            loop_lifecycle,
            metrics,
            vector_pid,
            reload_rx,
            shutdown_rx,
        )
        .await;
    });

    reload_tx.send(ReloadTrigger::Manual).await.unwrap();

    // The assembled config dir appearing proves the reload body ran rather than
    // bailing out at classification. Poll to a deadline instead of sizing a
    // sleep to the success path.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !config_dir.join("00_source.yaml").exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "the reload never reached re-assembly, so this proves nothing"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Let the loop finish the reload past the SIGHUP branch.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
    watcher.abort();

    let states = seen.lock().expect("state log").clone();
    assert!(
        !states.iter().any(State::is_ready),
        "a reload arriving while the subprocess is dead must never report ready, saw: {states:?}"
    );
    assert_eq!(
        lifecycle.state(),
        State::Crashed,
        "a reload must leave the crashed state the supervisor reported"
    );
}

#[test]
fn reload_classify_multi_field_change_is_unsafe() {
    use dfe_transform_vector::config::reload::{ChangeKind, classify_change};

    let old = load_config(Some("tests/fixtures/configs/minimal.yaml")).unwrap();
    let mut new = old.clone();

    // Change both transforms AND source — should be unsafe (not transforms-only)
    new.transforms.dir = Some("/new/transforms".into());
    new.source.brokers = vec!["different:9092".into()];

    assert_eq!(classify_change(&old, &new), ChangeKind::Unsafe);
}
