// Project:   dfe-transform-vector
// File:      src/bridge.rs
// Purpose:   Carry records between DFE's Push transport and Vector on the direct transport
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The direct-transport bridge.
//!
//! On the bus, Vector talks to Kafka itself and this module does nothing. On
//! `direct` there is no broker, and the two protocols do not meet: the rest of
//! DFE speaks scalo's `Transport/Push`, Vector speaks `vector.Vector/PushEvents`.
//! The supervisor translates, so a record crosses four hops inside the pod:
//!
//! ```text
//!   receiver/fetcher --Push--> [listener] --PushEvents--> Vector `vector` source
//!                                                              |
//!                                                          transforms
//!                                                              |
//!   loader/next stage <--Push-- [listener] <--PushEvents-- Vector `vector` sink
//! ```
//!
//! Both inner hops stay on loopback: they exist inside one pod and nothing
//! outside it may reach them.
//!
//! The two halves are independent. A stage that consumes a topic and pushes
//! onward runs the lower leg alone; a stage that receives pushes and produces to
//! a topic runs the upper one. Vector's own component follows the same split --
//! `kafka` on the bus end, `vector` on the direct one.
//!
//! ## Nothing is dropped
//!
//! Every hop holds rather than discards. A batch that cannot move on is
//! retried, so the supervisor stops draining the listener behind it, the
//! listener's channel fills, and scalo answers the upstream caller
//! `Backpressured` -- the same signal a full Kafka producer queue gives, all
//! the way back to whoever is sending. A record leaves this process only by
//! being accepted downstream.

use std::sync::Arc;
use std::time::Duration;

use scalo::transport::grpc::{GrpcConfig, GrpcTransport};
use scalo::transport::vector_compat::VectorCompatClient;
use scalo::transport::{Record, SendResult, TransportReceiver, TransportSender};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::{Error, Result};

/// First wait after a hop refuses a batch.
const RETRY_MIN: Duration = Duration::from_millis(100);

/// Ceiling on that wait. Long enough not to hammer a restarting peer, short
/// enough that recovery is not held back by the backoff itself.
const RETRY_MAX: Duration = Duration::from_secs(5);

/// How long an idle `recv` waits before looping, so shutdown is noticed
/// promptly on a quiet pipeline.
const RECV_IDLE: Duration = Duration::from_millis(200);

/// The half that carries records INTO Vector.
struct Inbound {
    /// Accepts `Transport/Push` from the rest of DFE.
    upstream: Arc<GrpcTransport>,
    /// Hands them to Vector's `vector` source.
    to_vector: Arc<VectorCompatClient>,
}

/// The half that carries records OUT of Vector.
struct Outbound {
    /// Accepts `PushEvents` back from Vector's `vector` sink.
    from_vector: Arc<GrpcTransport>,
    /// Pushes them to the next stage.
    downstream: Arc<GrpcTransport>,
    /// Routing key stamped on every outgoing record -- the `<source>_load`
    /// label the next stage picks its table by.
    destination: Arc<str>,
}

/// The listeners and clients that make up the bridge.
///
/// Each half is built only when its end of the pipeline is on the direct
/// transport, so a stage that consumes a topic and pushes onward (or the
/// reverse) binds one leg rather than two -- and a leg with no Vector component
/// at the other end is never started.
///
/// Built before Vector is spawned, so a port that cannot be bound fails startup
/// rather than surfacing after readiness has been published.
pub struct Bridge {
    inbound: Option<Inbound>,
    outbound: Option<Outbound>,
    /// Records moved per hop.
    batch_size: usize,
}

impl Bridge {
    /// Bind the listener and open the client each direct end needs.
    ///
    /// # Errors
    ///
    /// Fails when an address cannot be bound or an endpoint URI is unusable.
    pub async fn build(config: &Config) -> Result<Self> {
        let bridge = &config.bridge;

        let inbound = if config.source.transport.is_direct() {
            let upstream = server(&config.source.listen, "upstream Push listener").await?;
            let to_vector =
                VectorCompatClient::connect_lazy(&format!("http://{}", bridge.to_vector)).map_err(
                    |e| {
                        Error::Transport(format!(
                            "bridge.to_vector '{}' is not a usable endpoint: {e}",
                            bridge.to_vector
                        ))
                    },
                )?;
            info!(
                listen = %config.source.listen,
                to_vector = %bridge.to_vector,
                "bridge inbound ready"
            );
            Some(Inbound {
                upstream: Arc::new(upstream),
                to_vector: Arc::new(to_vector),
            })
        } else {
            None
        };

        let outbound = if config.sink.transport.is_direct() {
            // Vector's sink is a Vector-protocol client, so this end must accept
            // that protocol as well as the native one.
            let from_vector = server_with_vector_compat(&bridge.from_vector).await?;
            let downstream = GrpcTransport::new(&GrpcConfig::client(&config.sink.endpoint))
                .await
                .map_err(|e| {
                    Error::Transport(format!(
                        "sink.endpoint '{}' is not a usable endpoint: {e}",
                        config.sink.endpoint
                    ))
                })?;
            info!(
                from_vector = %bridge.from_vector,
                endpoint = %config.sink.endpoint,
                "bridge outbound ready"
            );
            Some(Outbound {
                from_vector: Arc::new(from_vector),
                downstream: Arc::new(downstream),
                destination: Arc::from(config.sink.topic.as_str()),
            })
        } else {
            None
        };

        Ok(Self {
            inbound,
            outbound,
            batch_size: bridge.batch_size,
        })
    }

    /// Run whichever directions this deployment has, until shutdown.
    pub async fn run(self, shutdown: watch::Receiver<bool>) {
        let batch_size = self.batch_size;
        let inbound = self.inbound.map(|half| {
            tokio::spawn(run_inbound(
                half.upstream,
                half.to_vector,
                batch_size,
                shutdown.clone(),
            ))
        });
        let outbound = self.outbound.map(|half| {
            tokio::spawn(run_outbound(
                half.from_vector,
                half.downstream,
                half.destination,
                batch_size,
                shutdown.clone(),
            ))
        });

        if let Some(task) = inbound {
            let _ = task.await;
        }
        if let Some(task) = outbound {
            let _ = task.await;
        }
        debug!("direct-transport bridge stopped");
    }
}

/// Bind a native Push listener.
async fn server(listen: &str, what: &str) -> Result<GrpcTransport> {
    GrpcTransport::new(&GrpcConfig::server(listen))
        .await
        .map_err(|e| Error::Transport(format!("{what} could not bind '{listen}': {e}")))
}

/// Bind a listener that accepts the Vector protocol as well as the native one.
async fn server_with_vector_compat(listen: &str) -> Result<GrpcTransport> {
    GrpcTransport::new(&GrpcConfig::server(listen).with_vector_compat())
        .await
        .map_err(|e| {
            Error::Transport(format!(
                "bridge.from_vector listener could not bind '{listen}': {e}"
            ))
        })
}

/// Upstream Push listener to Vector's `vector` source.
async fn run_inbound(
    upstream: Arc<GrpcTransport>,
    to_vector: Arc<VectorCompatClient>,
    batch_size: usize,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow() {
            return;
        }

        let batch = match recv(&upstream, batch_size, &mut shutdown).await {
            Some(batch) => batch,
            None => return,
        };
        if batch.is_empty() {
            continue;
        }

        let events: Vec<serde_json::Value> = batch.iter().map(|r| as_event(&r.payload)).collect();

        let mut wait = RETRY_MIN;
        loop {
            match to_vector.send_events(&events).await {
                Ok(()) => break,
                Err(e) => {
                    warn!(
                        error = %e,
                        records = events.len(),
                        "Vector would not take the batch -- holding it and retrying"
                    );
                    if !sleep_unless_shutdown(wait, &mut shutdown).await {
                        return;
                    }
                    wait = (wait * 2).min(RETRY_MAX);
                }
            }
        }
    }
}

/// Vector's `vector` sink to the next stage's Push listener.
async fn run_outbound(
    from_vector: Arc<GrpcTransport>,
    downstream: Arc<GrpcTransport>,
    destination: Arc<str>,
    batch_size: usize,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow() {
            return;
        }

        let mut batch = match recv(&from_vector, batch_size, &mut shutdown).await {
            Some(batch) => batch,
            None => return,
        };
        if batch.is_empty() {
            continue;
        }

        // The next stage routes on the record key, and a record that came back
        // through Vector carries none.
        for record in &mut batch {
            record.key = Some(Arc::clone(&destination));
        }

        let mut wait = RETRY_MIN;
        loop {
            match downstream.send_batch(&batch).await {
                SendResult::Ok => break,
                SendResult::Backpressured => {
                    debug!(
                        records = batch.len(),
                        "next stage is backpressured -- holding the batch"
                    );
                }
                other => {
                    error!(
                        result = ?other,
                        records = batch.len(),
                        "next stage refused the batch -- holding it and retrying"
                    );
                }
            }
            if !sleep_unless_shutdown(wait, &mut shutdown).await {
                return;
            }
            wait = (wait * 2).min(RETRY_MAX);
        }
    }
}

/// One `recv`, or `None` once shutdown is requested.
///
/// A closed transport ends the loop; any other receive error is transient and
/// costs one idle interval.
async fn recv(
    transport: &GrpcTransport,
    max: usize,
    shutdown: &mut watch::Receiver<bool>,
) -> Option<Vec<Record>> {
    tokio::select! {
        result = transport.recv(max) => match result {
            Ok(batch) => Some(batch.records),
            Err(e) if is_closed(&e) => {
                debug!(error = %e, "bridge listener closed");
                None
            }
            Err(e) => {
                warn!(error = %e, "bridge receive failed");
                if sleep_unless_shutdown(RECV_IDLE, shutdown).await {
                    Some(Vec::new())
                } else {
                    None
                }
            }
        },
        _ = shutdown.changed() => None,
    }
}

fn is_closed(error: &scalo::transport::TransportError) -> bool {
    matches!(error, scalo::transport::TransportError::Closed)
}

/// Sleep, unless shutdown arrives first. `false` means stop.
async fn sleep_unless_shutdown(wait: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        () = tokio::time::sleep(wait) => !*shutdown.borrow(),
        _ = shutdown.changed() => false,
    }
}

/// A payload as the JSON event Vector's protocol carries.
///
/// A DFE record is JSON, but the bridge may not drop one that is not: anything
/// that will not parse as a JSON object becomes `{"message": <text>}`, which is
/// where Vector's own decoders put bytes they cannot read.
fn as_event(payload: &[u8]) -> serde_json::Value {
    match serde_json::from_slice::<serde_json::Value>(payload) {
        Ok(value) if value.is_object() => value,
        _ => serde_json::json!({ "message": String::from_utf8_lossy(payload) }),
    }
}

/// Whether this configuration runs the bridge at all.
///
/// Either end is enough: a stage that consumes a topic and pushes onward needs
/// the outbound leg alone, and the reverse needs the inbound one. [`Bridge`]
/// builds whichever halves apply.
#[must_use]
pub fn is_enabled(config: &Config) -> bool {
    config.source.transport.is_direct() || config.sink.transport.is_direct()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_json_object_passes_through_unchanged() {
        let event = as_event(br#"{"id":"e1","level":"info"}"#);
        assert_eq!(event["id"], "e1");
        assert_eq!(event["level"], "info");
    }

    /// Dropping is not an option, so a payload Vector's protocol cannot carry
    /// as an object still arrives -- as the text it was.
    #[test]
    fn a_payload_that_is_not_a_json_object_arrives_as_a_message() {
        assert_eq!(as_event(b"not json at all")["message"], "not json at all");
        assert_eq!(as_event(b"[1,2,3]")["message"], "[1,2,3]");
        assert_eq!(as_event(b"")["message"], "");
    }

    #[test]
    fn invalid_utf8_is_carried_lossily_rather_than_dropped() {
        let event = as_event(&[0xff, 0xfe, b'h', b'i']);
        assert!(
            event["message"].as_str().is_some_and(|s| s.ends_with("hi")),
            "the readable bytes must survive: {event}"
        );
    }

    #[test]
    fn either_end_on_direct_enables_the_bridge() {
        let mut config = Config::default();
        assert!(!is_enabled(&config));

        config.source.transport = crate::config::Transport::Direct;
        assert!(is_enabled(&config));

        config.source.transport = crate::config::Transport::Bus;
        config.sink.transport = crate::config::Transport::Direct;
        assert!(is_enabled(&config));
    }
}
