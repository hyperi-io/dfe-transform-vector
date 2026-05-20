// Project:   dfe-transform-vector
// File:      tests/e2e/mod.rs
// Purpose:   E2E test submodule declarations
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

mod kafka;
// metrics_proxy module removed — wrapper no longer proxies Vector's
// /metrics. Vector exposes its own prometheus_exporter on
// `config.metrics.vector_metrics_address`; Prometheus scrapes that
// endpoint directly as a separate target.
