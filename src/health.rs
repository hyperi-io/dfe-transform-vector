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
use tokio::net::TcpListener;
use tracing::{debug, error, info};

use crate::Result;
use crate::vector::Lifecycle;

/// Start the health HTTP server.
///
/// Serves:
/// - `/health/live` — 200 if wrapper process is alive
/// - `/health/ready` — 200 if Vector is running and healthy, 503 otherwise
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
            let (status, body) = if state.is_alive() {
                (
                    StatusCode::OK,
                    format!(r#"{{"status":"alive","state":"{}"}}"#, state),
                )
            } else {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!(r#"{{"status":"dead","state":"{}"}}"#, state),
                )
            };
            Ok(json_response(status, &body))
        }
        "/health/ready" => {
            let (status, body) = if state.is_ready() {
                (
                    StatusCode::OK,
                    format!(r#"{{"status":"ready","state":"{}"}}"#, state),
                )
            } else {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!(r#"{{"status":"not_ready","state":"{}"}}"#, state),
                )
            };
            Ok(json_response(status, &body))
        }
        _ => Ok(json_response(
            StatusCode::NOT_FOUND,
            r#"{"error":"not found"}"#,
        )),
    }
}

/// Build a JSON HTTP response.
fn json_response(status: StatusCode, body: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap()
}
