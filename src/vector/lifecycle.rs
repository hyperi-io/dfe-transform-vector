// Project:   dfe-transform-vector
// File:      src/vector/lifecycle.rs
// Purpose:   Lifecycle state machine for Vector subprocess
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Vector subprocess lifecycle states.
//!
//! Tracks the current state of the Vector child process through its
//! lifecycle: initialising → validating → starting → running →
//! reloading → shutting down → crashed.

use std::sync::Arc;

use tokio::sync::watch;

/// Lifecycle states for the Vector subprocess.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Wrapper is loading config and preparing.
    Initialising,
    /// Running `vector validate` on assembled config.
    Validating,
    /// Vector process is starting up.
    Starting,
    /// Vector is running and healthy.
    Running,
    /// Config reload in progress (re-validate then SIGHUP).
    Reloading,
    /// Graceful shutdown in progress (SIGTERM sent to Vector).
    ShuttingDown,
    /// Vector crashed or exited unexpectedly.
    Crashed,
}

impl State {
    /// Whether the wrapper is ready to accept traffic (for readiness probe).
    pub fn is_ready(&self) -> bool {
        matches!(self, State::Running | State::Reloading)
    }

    /// Whether the wrapper is alive (for liveness probe).
    pub fn is_alive(&self) -> bool {
        !matches!(self, State::Crashed)
    }

    /// String representation for metrics labels.
    pub fn as_str(&self) -> &'static str {
        match self {
            State::Initialising => "initialising",
            State::Validating => "validating",
            State::Starting => "starting",
            State::Running => "running",
            State::Reloading => "reloading",
            State::ShuttingDown => "shutting_down",
            State::Crashed => "crashed",
        }
    }
}

impl std::fmt::Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Shared lifecycle state with change notification.
///
/// Uses a `watch` channel so health/metrics servers can observe
/// state transitions without polling.
#[derive(Clone)]
pub struct Lifecycle {
    tx: Arc<watch::Sender<State>>,
    rx: watch::Receiver<State>,
}

impl Default for Lifecycle {
    fn default() -> Self {
        Self::new()
    }
}

impl Lifecycle {
    /// Create a new lifecycle starting in `Initialising` state.
    pub fn new() -> Self {
        let (tx, rx) = watch::channel(State::Initialising);
        Self {
            tx: Arc::new(tx),
            rx,
        }
    }

    /// Get the current state.
    pub fn state(&self) -> State {
        *self.rx.borrow()
    }

    /// Transition to a new state.
    ///
    /// Unconditional, so it belongs to the task that OWNS the subprocess --
    /// `run_lifecycle`. Anything else annotating the state wants `set_if`.
    pub fn set(&self, state: State) {
        let _ = self.tx.send(state);
        tracing::debug!(state = %state, "lifecycle transition");
    }

    /// Transition to `to` only if the state is still `from`. Returns whether
    /// it moved.
    ///
    /// Two tasks write this state: `run_lifecycle`, which owns the subprocess
    /// and is authoritative, and the reload loop, which only annotates a reload
    /// in progress. An unconditional write from the reload loop can resurrect a
    /// state the supervisor has already moved past -- reporting `Running` over a
    /// `Crashed` recorded microseconds earlier, which puts a dead pod back in
    /// the Service. Going through here instead drops a transition this caller no
    /// longer owns.
    ///
    /// The compare and the write happen under the watch channel's own lock, so
    /// there is no window between the two for the other writer to slip through.
    pub fn set_if(&self, from: State, to: State) -> bool {
        let moved = self.tx.send_if_modified(|current| {
            if *current == from {
                *current = to;
                true
            } else {
                false
            }
        });
        if moved {
            tracing::debug!(from = %from, to = %to, "lifecycle transition");
        }
        moved
    }

    /// Get a receiver that can watch for state changes.
    pub fn subscribe(&self) -> watch::Receiver<State> {
        self.rx.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_initial_state() {
        let lc = Lifecycle::new();
        assert_eq!(lc.state(), State::Initialising);
    }

    #[test]
    fn lifecycle_transitions() {
        let lc = Lifecycle::new();
        lc.set(State::Validating);
        assert_eq!(lc.state(), State::Validating);
        lc.set(State::Running);
        assert_eq!(lc.state(), State::Running);
    }

    #[test]
    fn set_if_moves_only_from_the_expected_state() {
        let lc = Lifecycle::new();
        lc.set(State::Running);

        assert!(lc.set_if(State::Running, State::Reloading));
        assert_eq!(lc.state(), State::Reloading);

        // The supervisor has since recorded a crash; the reload loop's
        // hand-back must not resurrect it.
        lc.set(State::Crashed);
        assert!(!lc.set_if(State::Reloading, State::Running));
        assert_eq!(lc.state(), State::Crashed);
    }

    #[test]
    fn state_readiness() {
        assert!(!State::Initialising.is_ready());
        assert!(!State::Validating.is_ready());
        assert!(!State::Starting.is_ready());
        assert!(State::Running.is_ready());
        assert!(State::Reloading.is_ready());
        assert!(!State::ShuttingDown.is_ready());
        assert!(!State::Crashed.is_ready());
    }

    #[test]
    fn state_liveness() {
        assert!(State::Initialising.is_alive());
        assert!(State::Running.is_alive());
        assert!(State::ShuttingDown.is_alive());
        assert!(!State::Crashed.is_alive());
    }
}
