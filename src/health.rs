// Project:   dfe-transform-vector
// File:      src/health.rs
// Purpose:   Health endpoints (/health/live, /health/ready)
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Health endpoints (`/health/live`, `/health/ready`).
//!
//! Uses rustlib `HttpServer` (axum) for the HTTP transport. The readiness
//! state is driven by the Vector subprocess lifecycle — ready only when
//! Vector is running and healthy.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use hyperi_rustlib::http_server::{HttpServer, HttpServerConfig};
use tracing::{info, trace, warn};

use crate::Result;
use crate::vector::Lifecycle;

/// Start the health HTTP server.
///
/// Serves:
/// - `/health/live` — 200 if wrapper process is alive (always, handled by rustlib)
/// - `/health/ready` — 200 if Vector is running and healthy, 503 otherwise
pub async fn serve_health(address: &str, lifecycle: Lifecycle) -> Result<()> {
    let config = HttpServerConfig {
        bind_address: address.to_string(),
        enable_health_endpoints: true,
        enable_metrics_endpoint: false,
        enable_config_endpoint: false,
        ..Default::default()
    };

    let server = HttpServer::new(config);

    // Set initial readiness from current lifecycle state
    server.set_ready(lifecycle.state().is_ready());

    // Spawn a task that keeps the readiness flag in sync with lifecycle changes
    let ready_flag = server.ready_flag();
    let lc = lifecycle.clone();
    tokio::spawn(async move {
        sync_readiness(lc, ready_flag).await;
    });

    info!(address, "health server listening");

    server
        .serve(hyperi_rustlib::http_server::Router::new())
        .await
        .map_err(|e| crate::Error::Health(e.to_string()))
}

/// Keep the HttpServer readiness flag in sync with the Vector lifecycle.
async fn sync_readiness(lifecycle: Lifecycle, ready_flag: Arc<AtomicBool>) {
    let mut rx = lifecycle.subscribe();
    loop {
        if rx.changed().await.is_err() {
            warn!("lifecycle channel closed, health readiness sync stopped");
            break;
        }
        let state = lifecycle.state();
        let ready = state.is_ready();
        trace!(state = %state, ready, "health readiness poll");
        ready_flag.store(ready, Ordering::SeqCst);
    }
}
