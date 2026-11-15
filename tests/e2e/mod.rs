// Project:   dfe-transform-vector
// File:      tests/e2e/mod.rs
// Purpose:   E2E test submodule declarations
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

mod filebeat_kafka;
mod kafka;
// The Vector metrics merge is asserted inside `kafka` against a real Vector,
// and against a fake exporter in `tests/integration/metrics.rs`.
