// Project:   dfe-transform-vector
// File:      tests/integration/pgo_workload.rs
// Purpose:   The PGO workload must carry records for its full duration and exit clean
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The release build's PGO workload, run for real.
//!
//! `scripts/pgo-workload.sh` only runs on a release-channel build, where a
//! workload that starts the supervisor and then carries nothing still looks
//! like a success: hyperi-ci takes the exit code, and a profile collected off
//! an idle process makes PGO NEGATIVE rather than merely useless. This runs the
//! script the way CI does -- one argument, the binary -- so a workload that has
//! stopped driving the direct path fails here instead.

use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

/// Seconds of load to ask for: the script's own floor, which is the shortest
/// run it accepts.
const DURATION_SECS: u64 = 60;

/// What the script may spend on top of the load itself -- building the driver,
/// waiting for readiness, draining, cleaning up. hyperi-ci allows the workload
/// `duration + 600s` before it kills it, so this stays well inside that.
const OVERHEAD: Duration = Duration::from_secs(300);

#[test]
fn the_pgo_workload_carries_records_and_exits_clean() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let script = root.join("scripts/pgo-workload.sh");
    assert!(script.is_file(), "scripts/pgo-workload.sh is missing");

    let started = Instant::now();
    let run = Command::new("bash")
        .arg(&script)
        .arg(env!("CARGO_BIN_EXE_dfe-transform-vector"))
        .env("PGO_WORKLOAD_DURATION_SECS", DURATION_SECS.to_string())
        // The driver only generates load, so build it in the profile this test
        // run has already warmed rather than paying for a release build.
        .env("PGO_DRIVER_PROFILE", "debug")
        // The supervisor under test here is unoptimised, so drive it at a rate
        // it can carry rather than the release-build rate CI uses.
        .env("PGO_DRIVER_RPS", "1000")
        .current_dir(&root)
        .output()
        .expect("run scripts/pgo-workload.sh");
    let elapsed = started.elapsed();

    let stdout = String::from_utf8_lossy(&run.stdout);
    let stderr = String::from_utf8_lossy(&run.stderr);

    assert!(
        run.status.success(),
        "the workload exited {:?}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}",
        run.status.code()
    );
    assert!(
        stdout.contains("pgo-driver: complete"),
        "the driver did not report a loop that carried\n--- stdout ---\n{stdout}"
    );
    assert!(
        elapsed < Duration::from_secs(DURATION_SECS) + OVERHEAD,
        "the workload took {elapsed:?} for a {DURATION_SECS}s run -- it must self-terminate"
    );
}
