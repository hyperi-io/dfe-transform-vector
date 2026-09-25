// Project:   dfe-transform-vector
// File:      tests/integration/direct_transport.rs
// Purpose:   Push listener in, Vector in the middle, gRPC sink out, no broker anywhere
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The transform on the direct transport.
//!
//! A record pushed at the supervisor's Push listener comes out the other side
//! transformed, with no Kafka in the path. A second scalo Push listener stands
//! in for the loader.
//!
//! This runs the real Vector binary, because the whole point of the direct
//! transport is that the record makes it THROUGH Vector: the two loopback legs
//! either speak Vector's protocol or they do not, and only Vector can say.

use std::time::Duration;

use bytes::Bytes;
use dfe_transform_vector::bridge::{Bridge, BridgeRuntime, MAX_HOLD, SEND_TIMEOUT_MS};
use dfe_transform_vector::config::assembler;
use dfe_transform_vector::config::loader::{
    BridgeConfig, Config, PipelineConfig, SinkConfig, SourceConfig, TransformConfig, Transport,
    VectorConfig,
};
use dfe_transform_vector::vector::spawn_vector;
use scalo::transport::grpc::{GrpcConfig, GrpcTransport};
use scalo::transport::{TransportReceiver, TransportSender};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

use crate::common;

/// The transform this test runs: a Vector no-op that stamps a field, so the
/// assertion can tell a record that went THROUGH Vector from one that did not.
const NO_OP_TRANSFORM: &str = "transforms:\n  \
     dfe_transform:\n    \
     type: remap\n    \
     inputs:\n      - dfe_source\n    \
     source: |\n      \
     .transformed = true\n";

/// Allocate a free loopback port.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

/// Poll until the port accepts a TCP connection, or fail after 30s.
async fn wait_for_port(addr: &str) {
    for _ in 0..600 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("nothing listening on {addr} within 30s");
}

/// A config on the direct transport, with every address on a free loopback port.
fn direct_config(work: &TempDir, vector_binary: &str) -> Config {
    let transforms = work.path().join("transforms");
    std::fs::create_dir_all(&transforms).expect("transforms dir");
    std::fs::write(transforms.join("00_no_op.yaml"), NO_OP_TRANSFORM).expect("transform file");

    Config {
        pipeline: PipelineConfig {
            name: "direct-transport-test".into(),
        },
        source: SourceConfig {
            transport: Transport::Direct,
            listen: format!("127.0.0.1:{}", free_port()),
            ..Default::default()
        },
        sink: SinkConfig {
            transport: Transport::Direct,
            // Filled in by the caller once the stand-in loader is listening.
            endpoint: String::new(),
            topic: "orders_load".into(),
            ..Default::default()
        },
        bridge: BridgeConfig {
            to_vector: format!("127.0.0.1:{}", free_port()),
            from_vector: format!("127.0.0.1:{}", free_port()),
            batch_size: 10,
        },
        transforms: TransformConfig {
            dir: Some(transforms.to_string_lossy().into_owned()),
            files: None,
        },
        vector: VectorConfig {
            binary: vector_binary.into(),
            data_dir: work.path().join("data").to_string_lossy().into_owned(),
            config_dir: work.path().join("config").to_string_lossy().into_owned(),
            // Vector's own API adds nothing here and would claim a fixed port.
            api_address: String::new(),
            version: String::new(),
            version_check: "disabled".into(),
            ..Default::default()
        },
        ..Default::default()
    }
}

#[tokio::test]
async fn a_record_pushed_at_the_listener_comes_out_the_sink_transformed() {
    let Some(vector_binary) = common::vector_binary_path() else {
        common::require_service_in_ci("Vector binary", "scripts/fetch-vector.sh found nothing");
        eprintln!("Skipping: Vector binary not available (run scripts/fetch-vector.sh)");
        return;
    };

    let work = TempDir::new().expect("work dir");
    std::fs::create_dir_all(work.path().join("data")).expect("data dir");
    let mut config = direct_config(&work, &vector_binary.to_string_lossy());

    // The next stage, which on a real deployment is the loader.
    let loader_port = free_port();
    let loader = GrpcTransport::new(&GrpcConfig::server(&format!("127.0.0.1:{loader_port}")))
        .await
        .expect("stand-in loader listener");
    config.sink.endpoint = format!("http://127.0.0.1:{loader_port}");

    config.validate().expect("the direct config must validate");

    // Assemble and start Vector on the generated config.
    let config_dir = std::path::PathBuf::from(&config.vector.config_dir);
    assembler::assemble(&config, &config_dir).expect("assemble");
    let mut vector = spawn_vector(&config.vector, &config_dir).expect("spawn Vector");

    // The bridge binds both listeners; Vector binds the middle one.
    let bridge = Bridge::build(&config, BridgeRuntime::default())
        .await
        .expect("bridge");
    let stop = CancellationToken::new();
    let bridge_task = tokio::spawn(bridge.run(stop.clone(), stop.clone()));

    wait_for_port(&config.bridge.to_vector).await;
    wait_for_port(&config.source.listen).await;

    // Push at the transform's own listener, exactly as the receiver would.
    let pusher = GrpcTransport::new(&GrpcConfig::client(&format!(
        "http://{}",
        config.source.listen
    )))
    .await
    .expect("push client");
    for id in 0..3 {
        let result = pusher
            .send(
                "orders_land",
                Bytes::from(format!(r#"{{"id":"e{id}","level":"info"}}"#)),
            )
            .await;
        assert!(result.is_ok(), "push {id} failed: {result:?}");
    }

    // Collect what reached the stand-in loader.
    let mut received = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while received.len() < 3 && tokio::time::Instant::now() < deadline {
        if let Ok(batch) = loader.recv(10).await {
            received.extend(batch.records);
        }
    }

    stop.cancel();
    let _ = vector.kill().await;
    let _ = bridge_task.await;

    assert_eq!(
        received.len(),
        3,
        "every pushed record must reach the next stage"
    );
    for record in &received {
        let value: serde_json::Value =
            serde_json::from_slice(&record.payload).expect("valid JSON out of the sink");
        assert_eq!(value["transformed"], true, "the Vector transform ran");
        assert_eq!(value["level"], "info", "the original fields survived");
        assert_eq!(
            record.key.as_deref(),
            Some("orders_load"),
            "the next stage routes on sink.topic, so the bridge must stamp it"
        );
    }
}

/// The bridge is what the direct transport IS, so a config that names it must
/// bind both legs and neither may collide with the Push listener.
#[tokio::test]
async fn the_bridge_binds_both_loopback_legs() {
    let work = TempDir::new().expect("work dir");
    let mut config = direct_config(&work, "/nonexistent/vector");
    config.sink.endpoint = "http://127.0.0.1:1".into();

    let bridge = Bridge::build(&config, BridgeRuntime::default())
        .await
        .expect("bridge binds");
    let stop = CancellationToken::new();
    let task = tokio::spawn(bridge.run(stop.clone(), stop.clone()));

    wait_for_port(&config.source.listen).await;
    wait_for_port(&config.bridge.from_vector).await;

    stop.cancel();
    let _ = task.await;
}

/// A stage can bridge one end only -- a topic in, a push out -- and then the
/// other leg must not be bound. A leg with no Vector component at the far end
/// would retry forever against nothing.
#[tokio::test]
async fn only_the_direct_end_binds_a_leg() {
    let work = TempDir::new().expect("work dir");
    let mut config = direct_config(&work, "/nonexistent/vector");
    config.sink.endpoint = "http://127.0.0.1:1".into();
    // Consume a topic, push the result onward.
    config.source.transport = Transport::Bus;
    config.source.topics = vec!["orders_land".into()];

    config
        .validate()
        .expect("a bus source with a direct sink is a legitimate stage");

    let listen = config.source.listen.clone();
    let bridge = Bridge::build(&config, BridgeRuntime::default())
        .await
        .expect("bridge binds");
    let stop = CancellationToken::new();
    let task = tokio::spawn(bridge.run(stop.clone(), stop.clone()));

    wait_for_port(&config.bridge.from_vector).await;
    assert!(
        tokio::net::TcpStream::connect(&listen).await.is_err(),
        "the Push listener must not be bound when the source is on the bus"
    );

    stop.cancel();
    let _ = task.await;
}

/// A push client that gives up after `timeout_ms`.
async fn pusher_with_timeout(listen: &str, timeout_ms: u64) -> GrpcTransport {
    let mut config = GrpcConfig::client(&format!("http://{listen}"));
    config.send_timeout_ms = timeout_ms;
    GrpcTransport::new(&config).await.expect("push client")
}

/// The inbound listener is built armed: a push is answered only once Vector
/// has taken its records. With nothing listening where Vector should be, the
/// sender must never be told OK, since an OK here is a record lost on a kill.
#[tokio::test]
async fn a_push_is_not_answered_ok_before_vector_takes_it() {
    let work = TempDir::new().expect("work dir");
    let mut config = direct_config(&work, "/nonexistent/vector");
    // Inbound only: the far end of the push is Vector, which is not running.
    config.sink.transport = Transport::Bus;
    config.sink.topic = "orders_load".into();

    let bridge = Bridge::build(&config, BridgeRuntime::default())
        .await
        .expect("bridge binds");
    let stop = CancellationToken::new();
    let task = tokio::spawn(bridge.run(stop.clone(), stop.clone()));
    wait_for_port(&config.source.listen).await;

    let pusher = pusher_with_timeout(&config.source.listen, 3_000).await;
    let result = pusher
        .send("orders_land", Bytes::from_static(br#"{"id":"held"}"#))
        .await;

    stop.cancel();
    let _ = task.await;
    assert!(
        !result.is_ok(),
        "a push Vector never took must not be acknowledged: {result:?}"
    );
}

/// Armed from the moment it binds, not from when the pipeline first runs: a
/// push that lands between the two is held too, where an unarmed listener
/// would answer it at enqueue with nothing yet reading its queue.
#[tokio::test]
async fn a_push_before_the_bridge_runs_is_held_too() {
    let work = TempDir::new().expect("work dir");
    let mut config = direct_config(&work, "/nonexistent/vector");
    config.sink.transport = Transport::Bus;
    config.sink.topic = "orders_load".into();

    // Bound, and deliberately never run.
    let bridge = Bridge::build(&config, BridgeRuntime::default())
        .await
        .expect("bridge binds");

    let pusher = pusher_with_timeout(&config.source.listen, 3_000).await;
    let result = pusher
        .send("orders_land", Bytes::from_static(br#"{"id":"early"}"#))
        .await;
    drop(bridge);

    assert!(
        !result.is_ok(),
        "a push that arrived before the pipeline ran must not be acknowledged: {result:?}"
    );
}

/// Shutdown answers what the bridge still holds instead of dropping it: the
/// sender hears back (and retries) rather than hanging on a push nobody will
/// deliver, and the bridge itself stops.
#[tokio::test]
async fn shutdown_answers_a_held_push_rather_than_dropping_it() {
    let work = TempDir::new().expect("work dir");
    let mut config = direct_config(&work, "/nonexistent/vector");
    config.sink.transport = Transport::Bus;
    config.sink.topic = "orders_load".into();

    let bridge = Bridge::build(&config, BridgeRuntime::default())
        .await
        .expect("bridge binds");
    let stop = CancellationToken::new();
    let task = tokio::spawn(bridge.run(stop.clone(), stop.clone()));
    wait_for_port(&config.source.listen).await;

    // Long enough that only the shutdown can answer it.
    let pusher = pusher_with_timeout(&config.source.listen, 60_000).await;
    let push = tokio::spawn(async move {
        pusher
            .send("orders_land", Bytes::from_static(br#"{"id":"in-flight"}"#))
            .await
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.cancel();

    let answered = tokio::time::timeout(Duration::from_secs(40), push)
        .await
        .expect("the held push must be answered once the bridge shuts down")
        .expect("push task");
    assert!(
        !answered.is_ok(),
        "a push nothing delivered must not be acknowledged at shutdown: {answered:?}"
    );
    tokio::time::timeout(Duration::from_secs(40), task)
        .await
        .expect("the bridge stops after draining")
        .expect("bridge task");
}

/// The outbound listener is built armed too: Vector's `PushEvents` is answered
/// only once the next stage has the records, so Vector's own source is never
/// released for a record the loader did not get.
#[tokio::test]
async fn vectors_push_is_not_answered_ok_before_the_next_stage_takes_it() {
    let work = TempDir::new().expect("work dir");
    let mut config = direct_config(&work, "/nonexistent/vector");
    // Outbound only, towards a next stage that is not there.
    config.source.transport = Transport::Bus;
    config.source.topics = vec!["orders_land".into()];
    config.sink.endpoint = format!("http://127.0.0.1:{}", free_port());

    let bridge = Bridge::build(&config, BridgeRuntime::default())
        .await
        .expect("bridge binds");
    let stop = CancellationToken::new();
    let task = tokio::spawn(bridge.run(stop.clone(), stop.clone()));
    wait_for_port(&config.bridge.from_vector).await;

    // What Vector's `vector` sink does: PushEvents at the from_vector leg.
    let vector_sink = scalo::transport::vector_compat::VectorCompatClient::connect_lazy(&format!(
        "http://{}",
        config.bridge.from_vector
    ))
    .expect("vector-protocol client");
    let sent = tokio::time::timeout(
        Duration::from_secs(60),
        vector_sink.send_events(&[serde_json::json!({"id": "held"})]),
    )
    .await
    .expect("the hold budget answers the push");

    stop.cancel();
    let _ = task.await;
    assert!(
        sent.is_err(),
        "PushEvents must not be answered OK while the next stage is down"
    );
}

/// The outbound leg keeps the stage's deadlines: Vector's push is given up at
/// `MAX_HOLD`, and inside that hold a next stage that never answers is
/// abandoned at `SEND_TIMEOUT_MS` and tried again. On scalo's defaults, a 25 s
/// hold and a 30 s send, the push is held to 25 s and the next stage is tried
/// once.
#[tokio::test]
async fn the_outbound_leg_gives_a_hung_next_stage_up_inside_the_hold() {
    let work = TempDir::new().expect("work dir");
    let mut config = direct_config(&work, "/nonexistent/vector");
    config.source.transport = Transport::Bus;
    config.source.topics = vec!["orders_land".into()];

    // Takes every push and never releases its records, so each is answered
    // only when its hold runs out.
    let loader_port = free_port();
    let loader = GrpcTransport::builder(&GrpcConfig::server(&format!("127.0.0.1:{loader_port}")))
        .armed(true)
        .start()
        .await
        .expect("stand-in loader listener");
    config.sink.endpoint = format!("http://127.0.0.1:{loader_port}");

    let bridge = Bridge::build(&config, BridgeRuntime::default())
        .await
        .expect("bridge binds");
    let stop = CancellationToken::new();
    let task = tokio::spawn(bridge.run(stop.clone(), stop.clone()));
    wait_for_port(&config.bridge.from_vector).await;

    let vector_sink = scalo::transport::vector_compat::VectorCompatClient::connect_lazy(&format!(
        "http://{}",
        config.bridge.from_vector
    ))
    .expect("vector-protocol client");
    let events = [serde_json::json!({"id": "hung"})];
    let started = tokio::time::Instant::now();
    let push = vector_sink.send_events(&events);
    tokio::pin!(push);

    // Kept unreleased until the end, so the loader never answers one early.
    let mut taken = Vec::new();
    let sent = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            tokio::select! {
                sent = &mut push => break sent,
                batch = loader.recv(10) => {
                    if let Ok(batch) = batch
                        && !batch.records.is_empty()
                    {
                        taken.push(batch);
                    }
                }
            }
        }
    })
    .await
    .expect("the hold answers the push");
    let held_for = started.elapsed();
    let attempts: usize = taken.iter().map(|b| b.records.len()).sum();

    stop.cancel();
    let _ = task.await;
    drop(taken);

    assert!(
        sent.is_err(),
        "PushEvents must not be answered OK while the next stage never answers"
    );
    assert!(
        held_for >= MAX_HOLD - Duration::from_secs(1)
            && held_for < MAX_HOLD + Duration::from_secs(3),
        "Vector's push must be held for the {MAX_HOLD:?} hold, not scalo's 25 s default; it was \
         held {held_for:?}"
    );
    assert!(
        attempts >= 2,
        "a next stage that never answers must be given up after {SEND_TIMEOUT_MS} ms and tried \
         again inside the hold; it saw {attempts} attempt(s)"
    );
}

/// One address cannot be both ends of the loop through Vector.
#[test]
fn the_two_bridge_legs_must_differ() {
    let work = TempDir::new().expect("work dir");
    let mut config = direct_config(&work, "/nonexistent/vector");
    config.sink.endpoint = "http://127.0.0.1:1".into();
    config.bridge.from_vector = config.bridge.to_vector.clone();

    let err = config
        .validate()
        .expect_err("identical legs must be refused");
    assert!(
        err.to_string().contains("must differ"),
        "unexpected error: {err}"
    );
}

/// A hostname is not a bind address: it would fail at socket-bind time, after
/// readiness had already been published.
#[test]
fn a_listen_address_that_is_not_bindable_is_refused_up_front() {
    let work = TempDir::new().expect("work dir");
    let mut config = direct_config(&work, "/nonexistent/vector");
    config.sink.endpoint = "http://127.0.0.1:1".into();
    config.source.listen = "dfe-transform-vector:6000".into();

    let err = config.validate().expect_err("a hostname must be refused");
    assert!(
        err.to_string().contains("source.listen"),
        "unexpected error: {err}"
    );
}

/// The bus keeps its own rules: brokers and a group are still required, and the
/// direct-only addresses are not consulted.
#[test]
fn the_bus_still_demands_brokers_and_a_group() {
    let mut config = Config {
        sink: SinkConfig {
            topic: "out".into(),
            ..Default::default()
        },
        ..Default::default()
    };
    config.validate().expect("the default bus config validates");

    config.source.brokers.clear();
    let err = config.validate().expect_err("no brokers must be refused");
    assert!(
        err.to_string().contains("source.brokers"),
        "unexpected error: {err}"
    );
}

/// An empty topic list is a source nobody has written yet, not a broken config:
/// the app starts, stays Ready and waits.
#[test]
fn a_bus_transform_with_no_topics_idles_rather_than_refusing() {
    let mut config = Config {
        sink: SinkConfig {
            topic: "out".into(),
            ..Default::default()
        },
        ..Default::default()
    };
    config.source.topics.clear();

    config
        .validate()
        .expect("no topics is valid -- it is empty of work, not malformed");
    assert!(config.work_state().is_idle());

    config.source.topics = vec!["orders_land".into()];
    assert!(!config.work_state().is_idle());
}

/// On direct the listener IS the work, so an empty topic list means nothing.
#[test]
fn a_direct_transform_is_never_idle() {
    let work = TempDir::new().expect("work dir");
    let mut config = direct_config(&work, "/nonexistent/vector");
    config.sink.endpoint = "http://127.0.0.1:1".into();
    config.source.topics.clear();

    assert!(!config.work_state().is_idle());
}

/// A transform file authored against `dfe_source` must wire to the same labels
/// on both transports, or moving a deployment between them rewrites every file.
#[test]
fn both_transports_assemble_the_same_component_labels() {
    let work = TempDir::new().expect("work dir");
    let mut direct = direct_config(&work, "/nonexistent/vector");
    direct.sink.endpoint = "http://127.0.0.1:1".into();

    let direct_dir = work.path().join("direct-config");
    assembler::assemble(&direct, &direct_dir).expect("assemble direct");
    let direct_source = std::fs::read_to_string(direct_dir.join("00_source.yaml")).unwrap();
    let direct_sink = std::fs::read_to_string(direct_dir.join("90_sink.yaml")).unwrap();

    let mut bus = direct.clone();
    bus.source.transport = Transport::Bus;
    bus.sink.transport = Transport::Bus;
    bus.sink.topic = "orders_load".into();
    let bus_dir = work.path().join("bus-config");
    assembler::assemble(&bus, &bus_dir).expect("assemble bus");
    let bus_source = std::fs::read_to_string(bus_dir.join("00_source.yaml")).unwrap();
    let bus_sink = std::fs::read_to_string(bus_dir.join("90_sink.yaml")).unwrap();

    for text in [&direct_source, &bus_source] {
        assert!(text.contains("dfe_source"), "source label changed: {text}");
    }
    for text in [&direct_sink, &bus_sink] {
        assert!(text.contains("dfe_sink"), "sink label changed: {text}");
        assert!(
            text.contains("dfe_transform"),
            "the wired transform is missing: {text}"
        );
    }

    assert!(direct_source.contains("type: vector"));
    assert!(bus_source.contains("type: kafka"));
    assert!(
        direct_sink.contains(&format!("http://{}", direct.bridge.from_vector)),
        "the direct sink must dial the supervisor: {direct_sink}"
    );
}

/// The generated components must be the ones Vector actually accepts, not the
/// ones we believe it accepts.
#[tokio::test]
async fn the_assembled_direct_config_passes_vector_validate() {
    let Some(vector_binary) = common::vector_binary_path() else {
        common::require_service_in_ci("Vector binary", "scripts/fetch-vector.sh found nothing");
        eprintln!("Skipping: Vector binary not available (run scripts/fetch-vector.sh)");
        return;
    };

    let work = TempDir::new().expect("work dir");
    std::fs::create_dir_all(work.path().join("data")).expect("data dir");
    let mut config = direct_config(&work, &vector_binary.to_string_lossy());
    config.sink.endpoint = "http://127.0.0.1:1".into();

    let config_dir = std::path::PathBuf::from(&config.vector.config_dir);
    assembler::assemble(&config, &config_dir).expect("assemble");

    let output = std::process::Command::new(vector_binary)
        .arg("validate")
        .arg("--no-environment")
        .arg("--config-dir")
        .arg(&config_dir)
        .env("VECTOR_DATA_DIR", &config.vector.data_dir)
        .output()
        .expect("run vector validate");

    assert!(
        output.status.success(),
        "vector validate rejected the direct config:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The shipped templates are what an operator copies, so a template Vector will
/// not load is worse than none.
#[test]
fn the_shipped_templates_pass_vector_validate() {
    let Some(vector_binary) = common::vector_binary_path() else {
        common::require_service_in_ci("Vector binary", "scripts/fetch-vector.sh found nothing");
        eprintln!("Skipping: Vector binary not available (run scripts/fetch-vector.sh)");
        return;
    };

    let data_dir = TempDir::new().expect("data dir");
    let templates = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("templates");

    for name in ["bus.yaml", "direct.yaml"] {
        let path = templates.join(name);
        assert!(path.is_file(), "{name} is missing from templates/");

        let output = std::process::Command::new(vector_binary)
            .arg("validate")
            .arg("--no-environment")
            .arg(&path)
            .env("VECTOR_DATA_DIR", data_dir.path())
            .output()
            .expect("run vector validate");

        assert!(
            output.status.success(),
            "vector validate rejected templates/{name}:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

/// The templates document the topology the supervisor generates, so a template
/// that names different components than the generator is a lie in the docs.
#[test]
fn the_templates_name_the_components_the_generator_emits() {
    let templates = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("templates");
    let bus = std::fs::read_to_string(templates.join("bus.yaml")).expect("bus.yaml");
    let direct = std::fs::read_to_string(templates.join("direct.yaml")).expect("direct.yaml");

    for text in [&bus, &direct] {
        assert!(text.contains("dfe_source:"), "source label missing");
        assert!(text.contains("dfe_sink:"), "sink label missing");
        assert!(text.contains("timezone: UTC"), "the UTC pin is missing");
    }

    assert!(
        bus.contains("{source}_land"),
        "bus template lost the input topic"
    );
    assert!(
        bus.contains("{source}_load"),
        "bus template lost the output topic"
    );
    assert!(
        direct.contains(&BridgeConfig::default().to_vector),
        "direct template must use the default bridge.to_vector address"
    );
    assert!(
        direct.contains(&format!("http://{}", BridgeConfig::default().from_vector)),
        "direct template must dial the default bridge.from_vector address"
    );
}
