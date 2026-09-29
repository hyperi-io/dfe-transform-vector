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

/// A stand-in for the running Vector: alive, with a PID to signal, and deaf to
/// SIGHUP so the reload's signal does not end it.
fn vector_stand_in() -> std::process::Child {
    std::process::Command::new("sh")
        .args(["-c", "trap '' HUP; exec sleep 120"])
        .spawn()
        .expect("spawn the stand-in")
}

/// The first rendered series of `name` carrying `label`, 0 when absent.
fn counted(name: &str, label: &str) -> f64 {
    let output = crate::common::metrics_fixture::metrics_manager().render();
    output
        .lines()
        .find(|l| l.starts_with(name) && l.contains(label))
        .and_then(|l| l.rsplit(' ').next())
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.0)
}

/// What `transform_vector_config_reloads_total{result}` reads, 0 when absent.
fn reloads_counted(result: &str) -> f64 {
    counted(
        "dfe_transform_vector_config_reloads_total",
        &format!("result=\"{result}\""),
    )
}

/// Reloads the supervisor's own validation refused, which a refusal by Vector
/// must not be mistaken for.
fn validation_failures_counted() -> f64 {
    counted("dfe_config_validation_errors_total", "")
}

/// Poll `done` until it holds or `limit` passes; `true` if it held.
async fn eventually(limit: std::time::Duration, done: impl Fn() -> bool) -> bool {
    let deadline = std::time::Instant::now() + limit;
    while !done() {
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    true
}

/// A transform file and nothing else to hot-reload, the shape of every
/// operator edit.
fn remap_transform(field: &str) -> String {
    format!(
        "transforms:\n  parse:\n    type: remap\n    inputs: [\"dfe_source\"]\n    source: |\n      .{field} = true\n"
    )
}

/// The assembled copy of the one transform file.
fn assembled_parse(config_dir: &std::path::Path) -> String {
    std::fs::read_to_string(config_dir.join("50_000_01_parse.yaml")).unwrap_or_default()
}

/// Everything a reload loop test needs, torn down in `finish`.
struct ReloadRig {
    config_dir: std::path::PathBuf,
    stand_in: Option<std::process::Child>,
    reload_tx: tokio::sync::mpsc::Sender<dfe_transform_vector::config::reload::ReloadTrigger>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl ReloadRig {
    /// Assemble `starting` as startup does, then run the reload loop over it
    /// against a stand-in Vector.
    fn start(
        starting: dfe_transform_vector::config::Config,
        config_path: &std::path::Path,
        config_dir: std::path::PathBuf,
    ) -> Self {
        dfe_transform_vector::config::assembler::assemble(&starting, &config_dir)
            .expect("startup assembly");
        let stand_in = vector_stand_in();
        Self::run(
            starting,
            config_path,
            config_dir,
            stand_in.id(),
            Some(stand_in),
        )
    }

    /// Run the reload loop over the assembled `config_dir`, signalling `pid`,
    /// with `config_path` as the file it re-reads.
    fn run(
        starting: dfe_transform_vector::config::Config,
        config_path: &std::path::Path,
        config_dir: std::path::PathBuf,
        pid: u32,
        stand_in: Option<std::process::Child>,
    ) -> Self {
        use std::sync::{Arc, Mutex};

        use dfe_transform_vector::config::reload::run_reload_loop;
        use dfe_transform_vector::metrics::WrapperMetrics;
        use dfe_transform_vector::vector::Lifecycle;
        use dfe_transform_vector::vector::lifecycle::State;

        let lifecycle = Lifecycle::new();
        lifecycle.set(State::Running);
        let metrics = Arc::new(WrapperMetrics::register(
            crate::common::metrics_fixture::metrics_manager(),
            "reload-rig",
        ));
        let (reload_tx, reload_rx) = tokio::sync::mpsc::channel(1);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(run_reload_loop(
            starting,
            Some(config_path.to_string_lossy().into_owned()),
            config_dir.clone(),
            lifecycle,
            metrics,
            Arc::new(Mutex::new(Some(pid))),
            reload_rx,
            shutdown_rx,
        ));
        Self {
            config_dir,
            stand_in,
            reload_tx,
            shutdown_tx,
            task,
        }
    }

    async fn finish(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), self.task).await;
        if let Some(mut stand_in) = self.stand_in {
            let _ = stand_in.kill();
            let _ = stand_in.wait();
        }
    }
}

/// Editing a transform file is the hot reload operators actually do, and it
/// leaves `Config` untouched -- the files are read at assembly. The reload
/// must still re-assemble and hand Vector the edit.
#[tokio::test]
async fn a_transform_edit_alone_is_reassembled_and_applied() {
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let transforms_dir = dir.path().join("transforms");
    std::fs::create_dir_all(&transforms_dir).unwrap();
    std::fs::write(
        transforms_dir.join("01_parse.yaml"),
        remap_transform("before"),
    )
    .unwrap();

    // Vector's counters before the SIGHUP, then one applied reload after it.
    let (exporter, server) = crate::common::serve_expositions(|n| {
        if n == 0 {
            String::new()
        } else {
            "# TYPE vector_reloaded_total counter\nvector_reloaded_total 1\n".to_string()
        }
    })
    .await;

    let config_path = dir.path().join("config.yaml");
    std::fs::write(
        &config_path,
        format!(
            "pipeline:\n  name: \"edit-reload\"\nsource:\n  brokers: [\"localhost:9092\"]\n  \
             topics: [\"raw_input\"]\n  group_id: \"edit-reload-group\"\nsink:\n  \
             brokers: [\"localhost:9092\"]\n  topic: \"out\"\ntransforms:\n  dir: \"{}\"\n\
             vector:\n  binary: \"{}\"\n  data_dir: \"{}\"\n  version_check: \"disabled\"\n\
             metrics:\n  vector_metrics_address: \"{exporter}\"\nreload:\n  poll_interval_secs: 1\n",
            transforms_dir.display(),
            write_always_ok_binary(dir.path()).display(),
            dir.path().join("data").display(),
        ),
    )
    .unwrap();

    let starting = load_config(Some(config_path.to_str().unwrap())).unwrap();
    let rig = ReloadRig::start(starting, &config_path, dir.path().join("vector-config"));
    assert!(assembled_parse(&rig.config_dir).contains(".before = true"));
    // The loop takes its file snapshot when it first runs; the edit must come after.
    tokio::time::sleep(Duration::from_millis(100)).await;

    std::fs::write(
        transforms_dir.join("01_parse.yaml"),
        remap_transform("after"),
    )
    .unwrap();

    let config_dir = rig.config_dir.clone();
    let reassembled = eventually(Duration::from_secs(10), || {
        assembled_parse(&config_dir).contains(".after = true")
    })
    .await;
    let applied = eventually(Duration::from_secs(10), || {
        reloads_counted("success") >= 1.0
    })
    .await;
    rig.finish().await;
    server.abort();

    assert!(
        reassembled,
        "the edit never reached the assembled config, so Vector could not load it"
    );
    assert!(
        applied,
        "Vector reported the reload applied, and it was not counted"
    );
}

/// Vector refuses a reload it cannot apply and keeps the config it was
/// running, with the SIGHUP itself succeeding. The refusal is on Vector's own
/// counters, and the assembled directory has to go back to what Vector runs.
#[tokio::test]
async fn a_reload_vector_refuses_is_reported_and_the_running_config_put_back() {
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let running_dir = dir.path().join("transforms-running");
    let refused_dir = dir.path().join("transforms-refused");
    for (at, field) in [(&running_dir, "running"), (&refused_dir, "refused")] {
        std::fs::create_dir_all(at).unwrap();
        std::fs::write(at.join("01_parse.yaml"), remap_transform(field)).unwrap();
    }

    let (exporter, server) = crate::common::serve_expositions(|n| {
        if n == 0 {
            String::new()
        } else {
            "# TYPE vector_component_errors_total counter\n\
             vector_component_errors_total{error_code=\"reload\",error_type=\"configuration_failed\",\
             reason=\"topology_build_failed\",stage=\"processing\"} 1\n"
                .to_string()
        }
    })
    .await;

    let config_path = dir.path().join("config.yaml");
    std::fs::write(
        &config_path,
        format!(
            "pipeline:\n  name: \"refused-reload\"\nsource:\n  brokers: [\"localhost:9092\"]\n  \
             topics: [\"raw_input\"]\n  group_id: \"refused-reload-group\"\nsink:\n  \
             brokers: [\"localhost:9092\"]\n  topic: \"out\"\ntransforms:\n  dir: \"{}\"\n\
             vector:\n  binary: \"{}\"\n  data_dir: \"{}\"\n  version_check: \"disabled\"\n\
             metrics:\n  vector_metrics_address: \"{exporter}\"\nreload:\n  poll_interval_secs: 3600\n",
            refused_dir.display(),
            write_always_ok_binary(dir.path()).display(),
            dir.path().join("data").display(),
        ),
    )
    .unwrap();

    // Running the other directory, so the file's `transforms.dir` is the change.
    let mut starting = load_config(Some(config_path.to_str().unwrap())).unwrap();
    starting.transforms.dir = Some(running_dir.to_string_lossy().into_owned());
    let rig = ReloadRig::start(starting, &config_path, dir.path().join("vector-config"));
    rig.reload_tx
        .send(dfe_transform_vector::config::reload::ReloadTrigger::Manual)
        .await
        .unwrap();

    let refused = eventually(Duration::from_secs(10), || reloads_counted("error") >= 1.0).await;
    let config_dir = rig.config_dir.clone();
    let succeeded = reloads_counted("success");
    let invalid = validation_failures_counted();
    rig.finish().await;
    server.abort();

    assert!(
        refused,
        "Vector refused the reload, and it was not counted as failed"
    );
    assert_eq!(succeeded, 0.0, "a refused reload was counted as a success");
    assert_eq!(invalid, 0.0, "the failure came from validation, not Vector");
    let assembled = assembled_parse(&config_dir);
    assert!(
        assembled.contains(".running = true") && !assembled.contains(".refused = true"),
        "the assembled config must be the one Vector kept running, got:\n{assembled}"
    );
}

/// The same refusal from a real Vector, which is what pins the counter names
/// and labels the supervisor reads.
///
/// A transform file that sets a global option validates, and a running Vector
/// refuses to reload it: global options change only on a restart.
#[tokio::test]
async fn a_reload_a_real_vector_refuses_is_reported_and_put_back() {
    use std::time::Duration;

    use dfe_transform_vector::config::assembler;
    use dfe_transform_vector::metrics::scrape::fetch_exposition;

    let Some(vector_bin) = crate::common::vector_binary_path() else {
        crate::common::require_service_in_ci(
            "Vector binary",
            "scripts/fetch-vector.sh found nothing",
        );
        eprintln!("Skipping: Vector binary not available (run scripts/fetch-vector.sh)");
        return;
    };

    let dir = tempfile::tempdir().unwrap();
    let running_dir = dir.path().join("transforms-running");
    let refused_dir = dir.path().join("transforms-refused");
    for at in [&running_dir, &refused_dir] {
        std::fs::create_dir_all(at).unwrap();
        std::fs::write(at.join("01_parse.yaml"), remap_transform("parsed")).unwrap();
    }
    std::fs::write(
        refused_dir.join("02_global.yaml"),
        "expire_metrics_secs: 30\n",
    )
    .unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let port = |addr: String| addr.rsplit(':').next().unwrap().to_string();
    let exporter = crate::common::free_port().await;
    let listen = port(crate::common::free_port().await);
    let to_vector = port(crate::common::free_port().await);
    let from_vector = port(crate::common::free_port().await);

    // Direct at both ends, so no broker is involved.
    let config_path = dir.path().join("config.yaml");
    std::fs::write(
        &config_path,
        format!(
            "pipeline:\n  name: \"real-refused-reload\"\nsource:\n  transport: direct\n  \
             listen: \"127.0.0.1:{listen}\"\nsink:\n  transport: direct\n  \
             endpoint: \"http://127.0.0.1:{from_vector}\"\n  topic: \"out\"\nbridge:\n  \
             to_vector: \"127.0.0.1:{to_vector}\"\n  from_vector: \"127.0.0.1:{from_vector}\"\n\
             transforms:\n  dir: \"{}\"\nvector:\n  binary: \"{}\"\n  data_dir: \"{}\"\n  \
             version_check: \"disabled\"\nmetrics:\n  vector_metrics_address: \"{exporter}\"\n\
             reload:\n  poll_interval_secs: 3600\n",
            refused_dir.display(),
            vector_bin.display(),
            data_dir.display(),
        ),
    )
    .unwrap();

    let mut starting = load_config(Some(config_path.to_str().unwrap())).unwrap();
    starting.validate().expect("the test config validates");
    starting.transforms.dir = Some(running_dir.to_string_lossy().into_owned());
    let config_dir = dir.path().join("vector-config");
    assembler::assemble(&starting, &config_dir).unwrap();

    let mut vector = tokio::process::Command::new(vector_bin)
        .args(dfe_transform_vector::vector::vector_args(&config_dir))
        .env("VECTOR_DATA_DIR", &data_dir)
        .env("VECTOR_LOG", "warn")
        .stdout(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn vector");
    let exporter_up = {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            if fetch_exposition(&exporter).await.is_ok() {
                break true;
            }
            if std::time::Instant::now() >= deadline {
                break false;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    };
    if !exporter_up {
        let _ = vector.start_kill();
        panic!("Vector's exporter never answered on {exporter}");
    }

    let rig = ReloadRig::run(
        starting,
        &config_path,
        config_dir.clone(),
        vector.id().expect("Vector has a pid"),
        None,
    );
    rig.reload_tx
        .send(dfe_transform_vector::config::reload::ReloadTrigger::Manual)
        .await
        .unwrap();

    let refused = eventually(Duration::from_secs(30), || reloads_counted("error") >= 1.0).await;
    let succeeded = reloads_counted("success");
    let invalid = validation_failures_counted();
    let still_running = matches!(vector.try_wait(), Ok(None));
    rig.finish().await;
    let _ = vector.start_kill();
    let _ = vector.wait().await;

    assert!(
        refused,
        "Vector refused the reload, and it was not counted as failed"
    );
    assert_eq!(succeeded, 0.0, "a refused reload was counted as a success");
    assert_eq!(
        invalid, 0.0,
        "the change failed validation, so this proves nothing about Vector's refusal"
    );
    assert!(still_running, "a refused reload must leave Vector running");
    let put_back = std::fs::read_dir(&config_dir).unwrap().flatten().all(|e| {
        !std::fs::read_to_string(e.path())
            .unwrap_or_default()
            .contains("expire_metrics_secs")
    });
    assert!(
        put_back,
        "the assembled config still carries the change Vector refused"
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
