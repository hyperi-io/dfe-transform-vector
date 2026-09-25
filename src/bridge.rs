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
//! ## A push is answered once it is delivered
//!
//! Each leg is a scalo `BatchEngine` pipeline over a listener built armed, so
//! with `source.acknowledgements` on (the default) no push is answered until
//! the hop after it has taken the records:
//!
//! - inbound: the upstream sender's push is answered once `PushEvents` returns,
//!   and Vector's `vector` source, with acknowledgements on, returns only once
//!   Vector's sink has delivered.
//! - outbound: Vector's `PushEvents` is answered once the next stage accepted
//!   the records, so Vector's own source is released only then.
//!
//! A hop that refuses is retried until the push's hold deadline, then the push
//! is answered `Unavailable` and its sender retries: duplicates are possible,
//! loss is not. At shutdown each listener closes to new pushes and drains what
//! it already holds.

use std::sync::Arc;

use bytes::Bytes;
use scalo::governor::UnifiedPressure;
use scalo::transport::grpc::{GrpcConfig, GrpcToken, GrpcTransport};
use scalo::transport::vector_compat::VectorCompatClient;
use scalo::transport::{
    AcknowledgementsConfig, Record, SendResult, SinkConfirmation, TransportError, TransportSender,
    WorkBatch,
};
use scalo::worker::engine::BatchProcessingConfig;
use scalo::worker::{AdaptiveWorkerPool, BatchEngine, EngineError};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::{Error, Result};

/// What the service runtime lends the bridge.
#[derive(Clone, Default)]
pub struct BridgeRuntime {
    /// The runtime's worker pool, reused rather than building a second one.
    pub pool: Option<Arc<AdaptiveWorkerPool>>,
    /// Pressure both listeners shed pushes on while it holds intake.
    pub pressure: Option<Arc<UnifiedPressure>>,
}

/// The half that carries records INTO Vector.
struct Inbound {
    /// Accepts `Transport/Push` from the rest of DFE.
    upstream: GrpcTransport,
    /// Hands them to Vector's `vector` source.
    to_vector: Arc<VectorCompatClient>,
}

/// The half that carries records OUT of Vector.
struct Outbound {
    /// Accepts `PushEvents` back from Vector's `vector` sink.
    from_vector: GrpcTransport,
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
    /// Runs both legs; its `max_chunk_size` is the records moved per hop.
    engine: BatchEngine,
}

impl Bridge {
    /// Bind the listener and open the client each direct end needs.
    ///
    /// # Errors
    ///
    /// Fails when an address cannot be bound or an endpoint URI is unusable.
    pub async fn build(config: &Config, runtime: BridgeRuntime) -> Result<Self> {
        let bridge = &config.bridge;
        let acknowledgements = config.source.acknowledgements;

        let inbound = if config.source.transport.is_direct() {
            let upstream = listener(
                &GrpcConfig::server(&config.source.listen),
                acknowledgements,
                runtime.pressure.clone(),
                "upstream Push listener",
            )
            .await?;
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
                acknowledgements = acknowledgements.enabled,
                "bridge inbound ready"
            );
            Some(Inbound {
                upstream,
                to_vector: Arc::new(to_vector),
            })
        } else {
            None
        };

        let outbound = if config.sink.transport.is_direct() {
            // Vector's sink is a Vector-protocol client, so this end must accept
            // that protocol as well as the native one.
            let from_vector = listener(
                &GrpcConfig::server(&bridge.from_vector).with_vector_compat(),
                acknowledgements,
                runtime.pressure.clone(),
                "bridge.from_vector listener",
            )
            .await?;
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
                acknowledgements = acknowledgements.enabled,
                "bridge outbound ready"
            );
            Some(Outbound {
                from_vector,
                downstream: Arc::new(downstream),
                destination: Arc::from(config.sink.topic.as_str()),
            })
        } else {
            None
        };

        let engine_config = BatchProcessingConfig {
            max_chunk_size: bridge.batch_size,
            ..BatchProcessingConfig::default()
        };
        let engine = match runtime.pool {
            Some(pool) => BatchEngine::with_pool(pool, engine_config),
            None => BatchEngine::new(engine_config),
        };

        Ok(Self {
            inbound,
            outbound,
            engine,
        })
    }

    /// Run whichever directions this deployment has, until shutdown.
    ///
    /// Each leg stops taking pushes once its token is cancelled and drains what
    /// it already holds. The outbound token is separate so the leg Vector's
    /// sink flushes into can stay up until Vector has exited.
    pub async fn run(
        self,
        inbound_shutdown: CancellationToken,
        outbound_shutdown: CancellationToken,
    ) {
        let Self {
            inbound,
            outbound,
            engine,
        } = self;
        let engine = &engine;

        let inbound = async {
            if let Some(half) = inbound
                && let Err(e) = run_inbound(engine, &half, inbound_shutdown).await
            {
                error!(error = %e, "bridge inbound stopped");
            }
        };
        let outbound = async {
            if let Some(half) = outbound
                && let Err(e) = run_outbound(engine, &half, outbound_shutdown).await
            {
                error!(error = %e, "bridge outbound stopped");
            }
        };
        tokio::join!(inbound, outbound);
        debug!("direct-transport bridge stopped");
    }
}

/// Bind a receive server built armed, so no push is answered before the
/// pipeline releases its records.
async fn listener(
    config: &GrpcConfig,
    acknowledgements: AcknowledgementsConfig,
    pressure: Option<Arc<UnifiedPressure>>,
    what: &str,
) -> Result<GrpcTransport> {
    let builder = GrpcTransport::builder(config)
        .acknowledgements(acknowledgements)
        .armed(true);
    let builder = match pressure {
        Some(pressure) => builder.pressure(pressure),
        None => builder,
    };
    let listen = config.listen.clone().unwrap_or_default();
    builder
        .start()
        .await
        .map_err(|e| Error::Transport(format!("{what} could not bind '{listen}': {e}")))
}

/// A refusal the pipeline retries until the push's hold deadline.
fn retry_later() -> EngineError {
    EngineError::Transport(TransportError::Backpressure)
}

/// Upstream Push listener to Vector's `vector` source.
async fn run_inbound(
    engine: &BatchEngine,
    half: &Inbound,
    shutdown: CancellationToken,
) -> std::result::Result<(), EngineError> {
    let to_vector = &half.to_vector;
    engine
        .pipeline(&half.upstream)
        .shutdown(shutdown)
        // Vector's `vector` source with acknowledgements on answers only once
        // its sink has delivered.
        .sink_confirms(SinkConfirmation::Remote)
        .run(Ok, |batch: &WorkBatch<GrpcToken>| {
            let events: Vec<serde_json::Value> =
                batch.records.iter().map(|r| as_event(&r.payload)).collect();
            let to_vector = Arc::clone(to_vector);
            async move {
                to_vector.send_events(&events).await.map_err(|e| {
                    warn!(
                        error = %e,
                        records = events.len(),
                        "Vector would not take the batch -- holding it and retrying"
                    );
                    retry_later()
                })
            }
        })
        .await
}

/// Vector's `vector` sink to the next stage's Push listener.
async fn run_outbound(
    engine: &BatchEngine,
    half: &Outbound,
    shutdown: CancellationToken,
) -> std::result::Result<(), EngineError> {
    let destination = &half.destination;
    let downstream = &half.downstream;
    engine
        .pipeline(&half.from_vector)
        .shutdown(shutdown)
        // Screens out what the next stage's listener would refuse outright, so
        // such a record is counted rather than sent and silently left out.
        .sender(&**downstream)
        .run(
            |mut batch: WorkBatch<GrpcToken>| {
                // The next stage routes on the record key, and a record that
                // came back through Vector carries none.
                for record in &mut batch.records {
                    record.key = Some(Arc::clone(destination));
                }
                Ok(batch)
            },
            |batch: &WorkBatch<GrpcToken>| {
                let records: Vec<Record> = batch.records.clone();
                let downstream = Arc::clone(downstream);
                async move {
                    match downstream.send_batch(&records).await {
                        // The screen has already taken out what the sender
                        // would dead-letter, so what it names here it counted.
                        SendResult::Ok | SendResult::FilteredDlq => Ok(()),
                        SendResult::Backpressured => {
                            debug!(
                                records = records.len(),
                                "next stage is backpressured -- holding the batch"
                            );
                            Err(retry_later())
                        }
                        SendResult::Fatal(e) => {
                            error!(
                                error = %e,
                                records = records.len(),
                                "next stage refused the batch -- holding it and retrying"
                            );
                            Err(retry_later())
                        }
                    }
                }
            },
        )
        .await
}

/// A payload as the JSON event Vector's protocol carries.
///
/// A DFE record is JSON, but the bridge may not drop one that is not: anything
/// that will not parse as a JSON object becomes `{"message": <text>}`, which is
/// where Vector's own decoders put bytes they cannot read.
fn as_event(payload: &Bytes) -> serde_json::Value {
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
        let event = as_event(&Bytes::from_static(br#"{"id":"e1","level":"info"}"#));
        assert_eq!(event["id"], "e1");
        assert_eq!(event["level"], "info");
    }

    /// Dropping is not an option, so a payload Vector's protocol cannot carry
    /// as an object still arrives -- as the text it was.
    #[test]
    fn a_payload_that_is_not_a_json_object_arrives_as_a_message() {
        assert_eq!(
            as_event(&Bytes::from_static(b"not json at all"))["message"],
            "not json at all"
        );
        assert_eq!(
            as_event(&Bytes::from_static(b"[1,2,3]"))["message"],
            "[1,2,3]"
        );
        assert_eq!(as_event(&Bytes::new())["message"], "");
    }

    #[test]
    fn invalid_utf8_is_carried_lossily_rather_than_dropped() {
        let event = as_event(&Bytes::from_static(&[0xff, 0xfe, b'h', b'i']));
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

    /// A refused hop is waited out to the push's deadline, never taken as a
    /// reason to stop the leg.
    #[test]
    fn a_refused_hop_is_retried_not_fatal() {
        assert!(matches!(
            retry_later(),
            EngineError::Transport(ref e) if e.is_recoverable()
        ));
    }
}
