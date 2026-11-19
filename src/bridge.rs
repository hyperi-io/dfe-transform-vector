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
//!
//! A record leaves without being delivered only when no resend could deliver
//! it: one over the next stage's message-size ceiling, or one Vector refused for
//! good (a sink it feeds rejected it, or it could not use the request). The
//! first goes to the DLQ, and its push is answered once the DLQ holds it. The
//! second, refused only once it is sent, is released dropped and counted in
//! `pipeline_dead_letters_dropped_total`, so it is not resent forever.
//!
//! A leg whose pipeline fails stops the service, so the pod restarts rather
//! than run on with a listener nobody reads.
//!
//! ## Deadlines
//!
//! The deadlines nest, outermost first, so an inner hop is always answered
//! while the hop around it can still answer its own sender:
//!
//! - [`intake_hold`] (18 s, the hold of the ladder's depth-1 rung): the longest the intake listener holds a push,
//!   under the deadline of the stage pushing in.
//! - [`return_hold`] (16.5 s): the longest the listener Vector's sink pushes
//!   into holds a push. On direct to direct that hold sits inside the intake
//!   one, with room for Vector's own sink batching.
//! - [`send_timeout_ms`] (15 s, that rung's next-hop deadline): each send to the next stage, and each dial to
//!   Vector, gives up after this, so a hung hop is retried inside the hold.
//!
//! ## Pressure and memory
//!
//! Only the intake listener sheds pushes under the governor's pressure. The
//! return listener is the only drain for everything the intake holds, so
//! shedding it would stall the stage until the holds expired into duplicates.
//! Both lease what they hold on the runtime's memory guard.

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use scalo::dlq::Dlq;
use scalo::governor::UnifiedPressure;
use scalo::memory::MemoryGuard;
use scalo::transport::grpc::{GrpcConfig, GrpcToken, GrpcTransport};
use scalo::transport::hold::{HoldLadder, Rung};
use scalo::transport::vector_compat::VectorCompatClient;
use scalo::transport::{
    AcknowledgementsConfig, DEAD_LETTER_REJECTED, DeliveryStatus, Record, SendResult,
    SinkConfirmation, TransportError, TransportSender, WorkBatch,
};
use scalo::worker::engine::{BatchProcessingConfig, BlockPieces, Refusals};
use scalo::worker::{AdaptiveWorkerPool, BatchEngine, EngineError};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::metrics::count_dropped_dead_letters;
use crate::{Error, Result};

/// The room the return hold leaves inside the intake hold for Vector's
/// one-second sink batching.
pub const SINK_BATCHING_MARGIN: Duration = Duration::from_millis(1_500);

/// A transform's rung on scalo's hold ladder: one hop behind the receiver.
#[allow(clippy::expect_used)]
fn rung() -> Rung {
    HoldLadder::DEFAULT
        .rung(1)
        .expect("the default hold ladder has a depth-1 rung")
}

/// The longest the intake listener holds a push: under the upstream stage's
/// send deadline, over [`return_hold`].
#[must_use]
pub fn intake_hold() -> Duration {
    rung().hold
}

/// The longest the listener Vector's sink pushes into holds a push: inside
/// [`intake_hold`] by [`SINK_BATCHING_MARGIN`], over [`send_timeout_ms`].
#[must_use]
pub fn return_hold() -> Duration {
    intake_hold().saturating_sub(SINK_BATCHING_MARGIN)
}

/// Each send to the next stage, and each dial to Vector, gives up after this,
/// inside [`return_hold`], so the hold can still retry it.
#[must_use]
pub fn send_timeout_ms() -> u64 {
    u64::try_from(rung().next_hop_deadline.as_millis()).unwrap_or(u64::MAX)
}

/// Payload bytes one `PushEvents` to Vector carries at most, bar a single
/// record over it, which goes alone.
///
/// Vector's `vector` source refuses a request that decodes past 100 MiB, and a
/// batch it refuses would be retried until its hold ran out, on every resend.
/// The intake listener bounds one record at 16 MiB, so a request this size
/// stays under that cap at several times its payload once encoded.
pub const VECTOR_PUSH_BYTES: usize = 16 * 1024 * 1024;

/// What the service runtime lends the bridge.
#[derive(Clone, Default)]
pub struct BridgeRuntime {
    /// The runtime's worker pool, reused rather than building a second one.
    pub pool: Option<Arc<AdaptiveWorkerPool>>,
    /// Pressure the intake listener sheds pushes on while it holds intake.
    pub pressure: Option<Arc<UnifiedPressure>>,
    /// The guard both listeners lease held bytes on, which also sizes their
    /// held-byte ceilings.
    pub memory_guard: Option<Arc<MemoryGuard>>,
    /// Where both legs send a record the hop after them would refuse for
    /// good. Without one such a record is dropped and counted.
    pub dlq: Option<Arc<Dlq>>,
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
                Hold {
                    max: intake_hold(),
                    pressure: runtime.pressure.clone(),
                    memory_guard: runtime.memory_guard.clone(),
                },
                "upstream Push listener",
            )
            .await?;
            let to_vector = VectorCompatClient::connect_lazy_within(
                &format!("http://{}", bridge.to_vector),
                send_timeout_ms(),
            )
            .map_err(|e| {
                Error::Transport(format!(
                    "bridge.to_vector '{}' is not a usable endpoint: {e}",
                    bridge.to_vector
                ))
            })?;
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
            // No pressure here: this leg drains what the intake holds.
            let from_vector = listener(
                &GrpcConfig::server(&bridge.from_vector).with_vector_compat(),
                acknowledgements,
                Hold {
                    max: return_hold(),
                    pressure: None,
                    memory_guard: runtime.memory_guard.clone(),
                },
                "bridge.from_vector listener",
            )
            .await?;
            let downstream = GrpcTransport::new(&downstream_config(&config.sink.endpoint))
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
        let engine = match runtime.dlq {
            Some(dlq) => engine.with_dlq(dlq),
            None => engine,
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
    ///
    /// A leg whose pipeline fails cancels `failed` at once, so the service can
    /// stop Vector while the other leg drains.
    ///
    /// # Errors
    ///
    /// Returns the first leg's failure, once both legs have stopped.
    pub async fn run(
        self,
        inbound_shutdown: CancellationToken,
        outbound_shutdown: CancellationToken,
        failed: CancellationToken,
    ) -> Result<()> {
        let Self {
            inbound,
            outbound,
            engine,
        } = self;
        let engine = &engine;
        let failed = &failed;

        let inbound = async {
            match inbound {
                Some(half) => leg_ended(
                    "inbound",
                    run_inbound(engine, &half, inbound_shutdown).await,
                    failed,
                ),
                None => Ok(()),
            }
        };
        let outbound = async {
            match outbound {
                Some(half) => leg_ended(
                    "outbound",
                    run_outbound(engine, &half, outbound_shutdown).await,
                    failed,
                ),
                None => Ok(()),
            }
        };
        let (inbound, outbound) = tokio::join!(inbound, outbound);
        debug!("direct-transport bridge stopped");
        inbound.and(outbound)
    }
}

/// What Vector's `vector` source answering proves: with acknowledgements on it
/// returns only once its sink has delivered.
const VECTOR_CONFIRMS: SinkConfirmation = SinkConfirmation::Remote;

/// A leg's end: a failure also cancels `failed`, so the service stops rather
/// than run on with a listener nothing reads.
fn leg_ended(
    leg: &'static str,
    ended: std::result::Result<(), EngineError>,
    failed: &CancellationToken,
) -> Result<()> {
    ended.map_err(|e| {
        error!(leg, error = %e, "bridge leg stopped -- stopping the service");
        failed.cancel();
        Error::Transport(format!("bridge {leg} leg stopped: {e}"))
    })
}

/// The client for the next stage's Push listener.
fn downstream_config(endpoint: &str) -> GrpcConfig {
    let mut config = GrpcConfig::client(endpoint);
    config.send_timeout_ms = send_timeout_ms();
    config
}

/// How a listener holds the pushes it takes.
struct Hold {
    /// The longest a push is held.
    max: Duration,
    /// Pressure the listener sheds pushes on, for the intake only.
    pressure: Option<Arc<UnifiedPressure>>,
    /// The guard held bytes are leased on.
    memory_guard: Option<Arc<MemoryGuard>>,
}

/// Bind a receive server built armed, so no push is answered before the
/// pipeline releases its records.
async fn listener(
    config: &GrpcConfig,
    acknowledgements: AcknowledgementsConfig,
    hold: Hold,
    what: &str,
) -> Result<GrpcTransport> {
    let mut builder = GrpcTransport::builder(config)
        .acknowledgements(acknowledgements)
        .armed(true)
        .max_hold(hold.max);
    if let Some(pressure) = hold.pressure {
        builder = builder.pressure(pressure);
    }
    if let Some(guard) = hold.memory_guard {
        builder = builder.memory_guard(guard);
    }
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
        .listener("inbound")
        .shutdown(shutdown)
        .sink_confirms(VECTOR_CONFIRMS)
        .run_with_pieces(
            Ok,
            |batch: &WorkBatch<GrpcToken>, pieces: &BlockPieces<'_>| {
                let sizes: Vec<usize> = batch.records.iter().map(|r| r.payload.len()).collect();
                let events: Vec<serde_json::Value> =
                    batch.records.iter().map(|r| as_event(&r.payload)).collect();
                let to_vector = Arc::clone(to_vector);
                // Carries only what Vector rejected for good; the pipeline's
                // own piece carries whether the batch was delivered.
                let rejected_piece = pieces.piece();
                async move {
                    let mut rejected = 0_usize;
                    // A push that can clear retries the whole batch, so a run
                    // Vector already took is sent again: duplicated, never lost.
                    for run in runs_within(&sizes, VECTOR_PUSH_BYTES) {
                        let run = &events[run];
                        match to_vector.send_events_status(run).await {
                            Ok(()) => {}
                            Err(status) if VectorCompatClient::is_permanent_rejection(&status) => {
                                warn!(
                                    code = ?status.code(),
                                    message = status.message(),
                                    records = run.len(),
                                    "Vector rejected the records for good -- releasing them \
                                     as dropped"
                                );
                                rejected += run.len();
                            }
                            Err(status) => {
                                warn!(
                                    error = %status,
                                    records = events.len(),
                                    "Vector would not take the batch -- holding it and retrying"
                                );
                                // Delivered is the merge's identity, so a
                                // failed attempt adds nothing to the block.
                                rejected_piece.report(DeliveryStatus::Delivered);
                                return Err(retry_later());
                            }
                        }
                    }
                    rejected_piece.report(dropped_if_rejected(rejected));
                    Ok(())
                }
            },
        )
        .await
}

/// What an attempt that ended with `rejected` records refused for good adds to
/// its block: `Dropped`, counted, when there were any.
fn dropped_if_rejected(rejected: usize) -> DeliveryStatus {
    if rejected == 0 {
        return DeliveryStatus::Delivered;
    }
    count_dropped_dead_letters(DEAD_LETTER_REJECTED, rejected as u64);
    DeliveryStatus::Dropped
}

/// Consecutive runs of `sizes` whose totals stay within `budget`. A size over
/// the budget runs alone.
fn runs_within(sizes: &[usize], budget: usize) -> Vec<Range<usize>> {
    let mut runs = Vec::new();
    let mut start = 0;
    let mut total = 0_usize;
    for (index, &size) in sizes.iter().enumerate() {
        if index > start && total.saturating_add(size) > budget {
            runs.push(start..index);
            start = index;
            total = 0;
        }
        total = total.saturating_add(size);
    }
    if start < sizes.len() {
        runs.push(start..sizes.len());
    }
    runs
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
        .listener("outbound")
        .shutdown(shutdown)
        // Screens out what the next stage's listener would refuse outright, so
        // such a record is counted rather than sent and silently left out.
        .sender(&**downstream)
        .run_with_pieces(
            |mut batch: WorkBatch<GrpcToken>| {
                // The next stage routes on the record key, and a record that
                // came back through Vector carries none.
                for record in &mut batch.records {
                    record.key = Some(Arc::clone(destination));
                }
                Ok(batch)
            },
            |batch: &WorkBatch<GrpcToken>, pieces: &BlockPieces<'_>| {
                let records: Vec<Record> = batch.records.clone();
                let downstream = Arc::clone(downstream);
                let refusals = pieces.refusals();
                async move { send_to_next_stage(&downstream, &records, &refusals).await }
            },
        )
        .await
}

/// Send `records` to the next stage, or return the refusal the pipeline
/// retries.
///
/// A `FilteredDlq` answer to a batch names no record, so the batch is sent
/// again one record at a time. A record the stage dead-letters goes back to
/// the pipeline through `refusals`, which writes it to the DLQ before the
/// source is released. Records already sent go out twice: a duplicate, never
/// a loss.
async fn send_to_next_stage(
    downstream: &GrpcTransport,
    records: &[Record],
    refusals: &Refusals,
) -> std::result::Result<(), EngineError> {
    match downstream.send_batch(records).await {
        SendResult::Ok => return Ok(()),
        SendResult::FilteredDlq => {}
        refused => return Err(refusal(refused, records.len())),
    }
    for record in records {
        let one = if records.len() == 1 {
            SendResult::FilteredDlq
        } else {
            downstream.send_batch(std::slice::from_ref(record)).await
        };
        match one {
            SendResult::Ok => {}
            SendResult::FilteredDlq => {
                warn!("next stage dead-lettered a record -- handing it to the DLQ");
                refusals.dead_letter(record, "the next stage dead-lettered the record");
            }
            refused => return Err(refusal(refused, 1)),
        }
    }
    Ok(())
}

/// The retry for a send to the next stage that was not taken.
fn refusal(result: SendResult, records: usize) -> EngineError {
    match result {
        SendResult::Fatal(e) => error!(
            error = %e,
            records,
            "next stage refused the batch -- holding it and retrying"
        ),
        _ => debug!(records, "next stage is backpressured -- holding the batch"),
    }
    retry_later()
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

    /// A send the next stage did not take is held and retried, never dropped.
    #[test]
    fn a_send_not_taken_is_retried() {
        assert!(matches!(
            refusal(SendResult::Backpressured, 3),
            EngineError::Transport(ref e) if e.is_recoverable()
        ));
        assert!(matches!(
            refusal(SendResult::Fatal(TransportError::Send("refused".into())), 3),
            EngineError::Transport(ref e) if e.is_recoverable()
        ));
    }

    /// The deadlines nest strictly, outermost first: the intake hold, then the
    /// return hold with room for Vector's one-second sink batching, then each
    /// send. An inner hop that outlived the one around it would be cut off
    /// rather than answered.
    #[test]
    fn the_deadlines_nest_intake_over_return_over_send() {
        let send = Duration::from_millis(downstream_config("http://127.0.0.1:1").send_timeout_ms);
        let (intake, ret) = (intake_hold(), return_hold());
        assert_eq!(send, Duration::from_millis(send_timeout_ms()));
        assert_eq!(
            (intake, ret, send),
            (
                Duration::from_secs(18),
                Duration::from_millis(16_500),
                Duration::from_secs(15)
            )
        );
        assert!(
            intake > ret + Duration::from_secs(1),
            "the {ret:?} return hold leaves Vector no room inside the {intake:?} intake hold"
        );
        assert!(
            ret > send,
            "a {send:?} send outlives the {ret:?} return hold"
        );
    }

    /// A PushEvents is split so none outgrows what Vector's `vector` source
    /// decodes, and one record the intake admitted always fits on its own.
    #[test]
    fn no_push_to_vector_outgrows_what_its_source_decodes() {
        // Vector's `vector` source decode cap, its global decompressed-size cap.
        const VECTOR_DECODE_LIMIT: usize = 100 * 1024 * 1024;
        assert!(
            GrpcConfig::default().max_message_size <= VECTOR_PUSH_BYTES,
            "one record the intake admits must fit a push on its own"
        );
        const {
            assert!(
                VECTOR_PUSH_BYTES * 3 < VECTOR_DECODE_LIMIT,
                "a push at the budget must stay under Vector's cap at three times its payload"
            );
        }
    }

    #[test]
    fn runs_stay_within_the_budget_and_an_oversize_record_runs_alone() {
        assert_eq!(runs_within(&[], 10), Vec::<Range<usize>>::new());
        assert_eq!(runs_within(&[3, 3, 3], 100), vec![0..3]);
        assert_eq!(runs_within(&[5, 5, 5], 10), vec![0..2, 2..3]);
        assert_eq!(runs_within(&[4, 20, 4], 10), vec![0..1, 1..2, 2..3]);
        assert_eq!(runs_within(&[20], 10), vec![0..1]);
    }

    /// The `listener` and `guarantee` of every `pipeline_delivery_guarantee`
    /// series `snapshotter` saw, `(none)` for a series with no listener.
    fn guarantees(snapshotter: &metrics_util::debugging::Snapshotter) -> Vec<(String, String)> {
        let mut published: Vec<(String, String)> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, ..)| key.key().name() == "pipeline_delivery_guarantee")
            .map(|(key, ..)| {
                let label = |name: &str| {
                    key.key()
                        .labels()
                        .find(|l| l.key() == name)
                        .map_or_else(|| "(none)".to_owned(), |l| l.value().to_owned())
                };
                (label("listener"), label("guarantee"))
            })
            .collect();
        published.sort();
        published
    }

    /// Each leg's pipeline publishes its own guarantee series, told apart by
    /// `listener`, and nothing writes the unlabelled series.
    #[tokio::test(flavor = "current_thread")]
    async fn each_leg_publishes_its_own_guarantee() {
        let mut config = Config::default();
        config.source.transport = crate::config::Transport::Direct;
        config.source.listen = "127.0.0.1:0".into();
        config.sink.transport = crate::config::Transport::Direct;
        config.sink.endpoint = "http://127.0.0.1:1".into();
        config.bridge.from_vector = "127.0.0.1:0".into();
        let bridge = Bridge::build(&config, BridgeRuntime::default())
            .await
            .expect("bridge binds");

        // One thread, so every task the bridge spawns records here.
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _recording = metrics::set_default_local_recorder(&recorder);
        let stop = CancellationToken::new();
        let task = tokio::spawn(bridge.run(stop.clone(), stop.clone(), CancellationToken::new()));
        tokio::time::sleep(Duration::from_millis(200)).await;
        stop.cancel();
        task.await
            .expect("bridge task")
            .expect("a shutdown is not a leg failure");

        assert_eq!(
            guarantees(&snapshotter),
            vec![
                ("inbound".to_owned(), "at_least_once".to_owned()),
                ("outbound".to_owned(), "at_least_once".to_owned()),
            ]
        );
    }

    /// Records Vector rejected for good release their block `Dropped` and are
    /// counted; an attempt with none adds nothing.
    #[test]
    fn records_vector_rejected_are_dropped_and_counted() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let (none, some) = metrics::with_local_recorder(&recorder, || {
            (dropped_if_rejected(0), dropped_if_rejected(3))
        });
        assert_eq!(none, DeliveryStatus::Delivered);
        assert_eq!(some, DeliveryStatus::Dropped);
        assert_eq!(
            crate::metrics::dropped_since(&snapshotter, DEAD_LETTER_REJECTED),
            3
        );
    }

    /// A leg that stops on its own takes the service down with it; one that
    /// stops cleanly does not.
    #[test]
    fn a_failed_leg_signals_the_service_and_returns_its_error() {
        let failed = CancellationToken::new();
        assert!(leg_ended("inbound", Ok(()), &failed).is_ok());
        assert!(!failed.is_cancelled());

        let ended = leg_ended("outbound", Err(EngineError::Sink("gone".into())), &failed);
        assert!(
            failed.is_cancelled(),
            "a failed leg must signal the service"
        );
        let message = ended.expect_err("a failed leg is an error").to_string();
        assert!(
            message.contains("outbound") && message.contains("gone"),
            "{message}"
        );
    }
}
