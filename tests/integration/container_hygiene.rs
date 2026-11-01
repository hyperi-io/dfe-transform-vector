// Project:   dfe-transform-vector
// File:      tests/integration/container_hygiene.rs
// Purpose:   Pin the test-container naming and cleanup convention
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The containers this suite starts say which repo started them.
//!
//! Several runs share a developer machine, so `docker ps` has to show what each
//! container is and whose run owns it. scalo's `shared_service` names and labels
//! them, and this checks the suite is wired to it.

use crate::common;

/// A REAL container carries the suite's name prefix and label, and the SCRAM
/// broker a test holds alone is gone once its fixture drops.
///
/// The SCRAM broker, not the shared one: there is something to watch go away
/// only when this test owns the container.
#[tokio::test]
async fn a_started_container_carries_the_suite_name_and_label_then_goes_away() {
    let Some(fixture) = common::KafkaFixture::scram("container-hygiene-inspects-a-real-one").await
    else {
        common::require_service_in_ci("Kafka", "no Docker to start a broker container");
        eprintln!("skipping: no Docker, so this test cannot own a container");
        return;
    };
    let name = fixture
        .broker()
        .expect("a SCRAM fixture is always a container")
        .name
        .clone();
    let inspect = |field: &str| {
        std::process::Command::new("docker")
            .args(["inspect", &name, "--format", field])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
    };

    assert!(
        name.starts_with(&format!("{}-integration-", common::SUITE.prefix())),
        "{name} does not carry the suite prefix"
    );
    let suite = inspect(r#"{{index .Config.Labels "io.hyperi.test.suite"}}"#)
        .expect("docker inspect must report labels");
    assert_eq!(suite, common::SUITE.label(), "suite label missing or wrong");
    assert_eq!(suite, "dfe-transform-vector-integration");

    drop(fixture);
    for _ in 0..50 {
        if inspect("{{.Name}}").is_none() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("{name} still exists after its fixture was dropped -- the suite leaks containers");
}
