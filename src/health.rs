// Project:   dfe-transform-vector
// File:      src/health.rs
// Purpose:   Health endpoints (/health/live, /health/ready)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Health endpoints (`/health/live`, `/health/ready`).
//!
//! HTTP server providing composite health status of the wrapper and
//! Vector child process. Conforms to the dfe-engine health contract.

use std::convert::Infallible;
use std::net::SocketAddr;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::Serialize;
use tokio::net::TcpListener;
use tracing::{debug, error, info};

use crate::Result;
use crate::vector::Lifecycle;

/// Health response body.
#[derive(Serialize)]
struct HealthBody {
    status: &'static str,
    state: String,
}

/// Start the health HTTP server.
///
/// Serves:
/// - `/health/live` — 200 if wrapper process is alive
/// - `/health/ready` — 200 if Vector is running and healthy, 503 otherwise
// TODO: migrate to hyperi-rustlib `http-server` feature
pub async fn serve_health(address: &str, lifecycle: Lifecycle) -> Result<()> {
    let addr: SocketAddr = address
        .parse()
        .map_err(|e| crate::Error::Health(format!("invalid health address '{address}': {e}")))?;

    let listener = TcpListener::bind(addr).await.map_err(|e| {
        crate::Error::Health(format!("failed to bind health server on {addr}: {e}"))
    })?;

    info!(address = %addr, "health server listening");

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                error!(error = %e, "health server accept error");
                continue;
            }
        };

        let lc = lifecycle.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = service_fn(move |req| {
                let lc = lc.clone();
                async move { handle_health(req, &lc) }
            });
            if let Err(e) = http1::Builder::new().serve_connection(io, svc).await {
                debug!(error = %e, "health connection error");
            }
        });
    }
}

/// Handle a health request.
fn handle_health(
    req: Request<hyper::body::Incoming>,
    lifecycle: &Lifecycle,
) -> std::result::Result<Response<Full<Bytes>>, Infallible> {
    let state = lifecycle.state();

    match req.uri().path() {
        "/health/live" => {
            let (status_code, status_text) = if state.is_alive() {
                (StatusCode::OK, "alive")
            } else {
                (StatusCode::SERVICE_UNAVAILABLE, "dead")
            };
            Ok(json_response(status_code, status_text, &state.to_string()))
        }
        "/health/ready" => {
            let (status_code, status_text) = if state.is_ready() {
                (StatusCode::OK, "ready")
            } else {
                (StatusCode::SERVICE_UNAVAILABLE, "not_ready")
            };
            Ok(json_response(status_code, status_text, &state.to_string()))
        }
        _ => Ok(not_found_response()),
    }
}

/// Build a JSON health response using serde_json.
fn json_response(
    status: StatusCode,
    status_text: &'static str,
    state: &str,
) -> Response<Full<Bytes>> {
    let body = HealthBody {
        status: status_text,
        state: state.to_string(),
    };
    let json = serde_json::to_string(&body).unwrap_or_else(|_| r#"{"status":"error"}"#.into());

    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(json)))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::from(r#"{"status":"error"}"#))))
}

/// Build a 404 JSON response.
fn not_found_response() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(r#"{"error":"not found"}"#)))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::from(r#"{"error":"not found"}"#))))
}
