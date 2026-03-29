// Project:   dfe-transform-vector
// File:      tests/e2e/kafka.rs
// Purpose:   End-to-end Kafka pipeline test (placeholder — needs rustlib transport API update)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! E2E Kafka pipeline test.
//!
//! Requires Docker (testcontainers) or remote Kafka + Vector binary.
//! Run with: `cargo nextest run --test e2e --run-ignored all`
//!
//! Temporarily disabled: the KafkaTransport API changed in rustlib v1.20.
//! Needs updating to use the new transport trait split.
