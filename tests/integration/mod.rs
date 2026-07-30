// Project:   dfe-transform-vector
// File:      tests/integration/mod.rs
// Purpose:   Integration test submodule declarations
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

mod config;
// A leak check has to fail the test when the container is still there, and the
// poll loop it sits after cannot express that as an assert.
#[allow(clippy::panic)]
mod container_hygiene;
mod deployment;
mod fixtures;
mod lifecycle;
mod metrics;
mod reload;
mod vector_validate;
