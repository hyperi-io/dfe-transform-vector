// Project:   dfe-transform-vector
// File:      src/health.rs
// Purpose:   Publish the Vector subprocess lifecycle as the service readiness signal
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Readiness reporting for the Vector subprocess.
//!
//! The service has ONE health surface: `/livez`, `/readyz` and `/metrics` on
//! the metrics port, served by scalo's `MetricsManager`. That is the port the
//! generated deployment contract points every probe at.
//!
//! `/readyz` there answers 200 unless something reports the service not ready,
//! so the wrapper must publish the subprocess state or a dead Vector advertises
//! healthy to Kubernetes and KEDA. It publishes the sink's delivery progress
//! too: a live Vector can hold records it cannot deliver indefinitely.
//!
//! `/readyz` reads the `MetricsManager` readiness callback AND the
//! `HealthRegistry` on every request, so either would answer the probe -- the
//! registry is the one used because only it carries a component name, which puts
//! Vector on the detailed health output as the reason the pod left the Service.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use scalo::health::{HealthRegistry, HealthStatus};
use tracing::debug;

use crate::vector::Lifecycle;
use crate::vector::lifecycle::State;

/// Name the Vector subprocess reports under in scalo's health registry.
const COMPONENT: &str = "vector_subprocess";

/// Name the sink's delivery progress reports under.
const SINK_COMPONENT: &str = "vector_sink";

/// Publish the Vector subprocess lifecycle as the service readiness signal.
///
/// Call once during startup. The closure is evaluated per probe request, so it
/// always reads the live lifecycle state.
pub fn register_readiness(lifecycle: &Lifecycle) {
    let lifecycle = lifecycle.clone();
    HealthRegistry::register(COMPONENT, move || readiness_status(lifecycle.state()));
    debug!(component = COMPONENT, "readiness published to /readyz");
}

/// Publish the sink's delivery progress as a second readiness signal.
///
/// A live Vector whose sink has delivered nothing it holds for the stall window
/// is not serving, so the pod leaves the Service rather than sit Ready over a
/// partition with no leader.
pub fn register_sink_progress(stalled: Arc<AtomicBool>) {
    HealthRegistry::register(SINK_COMPONENT, move || {
        sink_status(stalled.load(Ordering::Relaxed))
    });
    debug!(
        component = SINK_COMPONENT,
        "sink progress published to /readyz"
    );
}

/// Map the stall flag to the status `/readyz` reads.
#[must_use]
pub fn sink_status(stalled: bool) -> HealthStatus {
    if stalled {
        HealthStatus::Unhealthy
    } else {
        HealthStatus::Healthy
    }
}

/// Map a lifecycle state to the status `/readyz` reads.
///
/// Two-valued deliberately: the registry counts `Degraded` as ready, and no
/// Vector state earns that -- either the subprocess is carrying traffic or the
/// pod must leave the Service.
#[must_use]
pub fn readiness_status(state: State) -> HealthStatus {
    if state.is_ready() {
        HealthStatus::Healthy
    } else {
        HealthStatus::Unhealthy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stalled_sink_is_not_ready() {
        assert_eq!(sink_status(true), HealthStatus::Unhealthy);
        assert_eq!(sink_status(false), HealthStatus::Healthy);
    }

    #[test]
    fn a_crashed_subprocess_is_not_ready() {
        assert_eq!(readiness_status(State::Crashed), HealthStatus::Unhealthy);
    }

    #[test]
    fn a_starting_subprocess_is_not_ready() {
        assert_eq!(readiness_status(State::Starting), HealthStatus::Unhealthy);
    }

    #[test]
    fn only_a_live_subprocess_is_ready() {
        assert_eq!(readiness_status(State::Running), HealthStatus::Healthy);
        assert_eq!(readiness_status(State::Reloading), HealthStatus::Healthy);
        assert_eq!(
            readiness_status(State::Initialising),
            HealthStatus::Unhealthy
        );
        assert_eq!(readiness_status(State::Validating), HealthStatus::Unhealthy);
        assert_eq!(
            readiness_status(State::ShuttingDown),
            HealthStatus::Unhealthy
        );
    }

    /// `Degraded` would read as ready, which no Vector state should.
    #[test]
    fn no_state_maps_to_degraded() {
        for state in [
            State::Initialising,
            State::Validating,
            State::Starting,
            State::Running,
            State::Reloading,
            State::ShuttingDown,
            State::Crashed,
        ] {
            assert_ne!(readiness_status(state), HealthStatus::Degraded);
        }
    }
}
