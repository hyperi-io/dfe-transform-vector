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
//! The one record that leaves without being delivered is one the next stage
//! could never take, over its message-size ceiling. Its push is released
//! dropped and scalo counts it, so it is not resent forever.
//!
//! A leg whose pipeline fails stops the service, so the pod restarts rather
//! than run on with a listener nobody reads.
//!
//! ## Deadlines
//!
//! The deadlines nest, outermost first, so an inner hop is always answered
//! while the hop around it can still answer its own sender:
//!
//! - [`INTAKE_HOLD`] (18 s): the longest the intake listener holds a push,
//!   under the deadline of the stage pushing in.
//! - [`RETURN_HOLD`] (16.5 s): the longest the listener Vector's sink pushes
//!   into holds a push. On direct to direct that hold sits inside the intake
//!   one, with room for Vector's own sink batching.
//! - [`SEND_TIMEOUT_MS`] (15 s): each send to the next stage, and each dial to
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
use scalo::governor::UnifiedPressure;
use scalo::memory::MemoryGuard;
use scalo::transport::ack::EffectiveGuarantee;
use scalo::transport::grpc::{GrpcConfig, GrpcToken, GrpcTransport};
use scalo::transport::vector_compat::VectorCompatClient;
use scalo::transport::{
    AcknowledgementsConfig, DeliveryStatus, Record, SendResult, SinkConfirmation, TransportError,
    TransportReceiver, TransportSender, WorkBatch,
};
use scalo::worker::engine::{BatchProcessingConfig, BlockPieces};
use scalo::worker::{AdaptiveWorkerPool, BatchEngine, EngineError};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::{Error, Result};

/// The longest the intake listener holds a push: under the upstream stage's
/// send deadline, over [`RETURN_HOLD`].
pub const INTAKE_HOLD: Duration = Duration::from_secs(18);

/// The longest the listener Vector's sink pushes into holds a push: inside
/// [`INTAKE_HOLD`] by more than Vector's one-second sink batching, over
/// [`SEND_TIMEOUT_MS`].
pub const RETURN_HOLD: Duration = Duration::from_millis(16_500);

/// Each send to the next stage, and each dial to Vector, gives up after this,
/// inside [`RETURN_HOLD`], so the hold can still retry it.
pub const SEND_TIMEOUT_MS: u64 = 15_000;

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
                    max: INTAKE_HOLD,
                    pressure: runtime.pressure.clone(),
                    memory_guard: runtime.memory_guard.clone(),
                },
                "upstream Push listener",
            )
            .await?;
            let to_vector = VectorCompatClient::connect_lazy_within(
                &format!("http://{}", bridge.to_vector),
                SEND_TIMEOUT_MS,
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
                    max: RETURN_HOLD,
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
        self.publish_guarantees();
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

    /// Publish each leg's delivery guarantee as its own
    /// `pipeline_delivery_guarantee` series, labelled by leg.
    fn publish_guarantees(&self) {
        if let Some(half) = &self.inbound {
            EffectiveGuarantee::of(half.upstream.ack_control(), VECTOR_CONFIRMS)
                .publish_for("inbound");
        }
        if let Some(half) = &self.outbound {
            EffectiveGuarantee::of(
                half.from_vector.ack_control(),
                half.downstream.confirms_delivery(),
            )
            .publish_for("outbound");
        }
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
    config.send_timeout_ms = SEND_TIMEOUT_MS;
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
        .shutdown(shutdown)
        .sink_confirms(VECTOR_CONFIRMS)
        .run(Ok, |batch: &WorkBatch<GrpcToken>| {
            let sizes: Vec<usize> = batch.records.iter().map(|r| r.payload.len()).collect();
            let events: Vec<serde_json::Value> =
                batch.records.iter().map(|r| as_event(&r.payload)).collect();
            let to_vector = Arc::clone(to_vector);
            async move {
                // A refused push retries the whole batch, so a run Vector
                // already took is sent again: duplicated, never lost.
                for run in runs_within(&sizes, VECTOR_PUSH_BYTES) {
                    to_vector.send_events(&events[run]).await.map_err(|e| {
                        warn!(
                            error = %e,
                            records = events.len(),
                            "Vector would not take the batch -- holding it and retrying"
                        );
                        retry_later()
                    })?;
                }
                Ok(())
            }
        })
        .await
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
                // Carries only what the send left out; the pipeline's own piece
                // carries whether the batch was delivered.
                let left_out = pieces.piece();
                async move {
                    match sent(downstream.send_batch(&records).await, records.len()) {
                        Ok(status) => {
                            left_out.report(status);
                            Ok(())
                        }
                        Err(e) => {
                            // Delivered is the merge's identity, so a failed
                            // attempt adds nothing to the block's status.
                            left_out.report(DeliveryStatus::Delivered);
                            Err(e)
                        }
                    }
                }
            },
        )
        .await
}

/// What one send to the next stage adds to its block's status, or the refusal
/// the pipeline retries.
fn sent(result: SendResult, records: usize) -> std::result::Result<DeliveryStatus, EngineError> {
    match result {
        SendResult::Ok => Ok(DeliveryStatus::Delivered),
        // Every record was over the next stage's size ceiling, which the screen
        // should have caught: released dropped, never delivered, never resent.
        SendResult::FilteredDlq => {
            warn!(
                records,
                "next stage could take none of the batch -- releasing it as dropped"
            );
            Ok(DeliveryStatus::Dropped)
        }
        SendResult::Backpressured => {
            debug!(records, "next stage is backpressured -- holding the batch");
            Err(retry_later())
        }
        SendResult::Fatal(e) => {
            error!(
                error = %e,
                records,
                "next stage refused the batch -- holding it and retrying"
            );
            Err(retry_later())
        }
    }
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

    /// A batch the next stage took none of is released dropped, so its source
    /// is answered and it is counted, but never as delivered.
    #[test]
    fn a_send_that_left_every_record_out_is_dropped_never_delivered() {
        assert!(matches!(
            sent(SendResult::FilteredDlq, 3),
            Ok(DeliveryStatus::Dropped)
        ));
        assert!(matches!(
            sent(SendResult::Ok, 3),
            Ok(DeliveryStatus::Delivered)
        ));
        assert!(matches!(
            sent(SendResult::Backpressured, 3),
            Err(EngineError::Transport(ref e)) if e.is_recoverable()
        ));
        assert!(matches!(
            sent(SendResult::Fatal(TransportError::Send("refused".into())), 3),
            Err(EngineError::Transport(ref e)) if e.is_recoverable()
        ));
    }

    /// The deadlines nest strictly, outermost first: the intake hold, then the
    /// return hold with room for Vector's one-second sink batching, then each
    /// send. An inner hop that outlived the one around it would be cut off
    /// rather than answered.
    #[test]
    fn the_deadlines_nest_intake_over_return_over_send() {
        let send = Duration::from_millis(downstream_config("http://127.0.0.1:1").send_timeout_ms);
        assert_eq!(send, Duration::from_millis(SEND_TIMEOUT_MS));
        assert!(
            INTAKE_HOLD > RETURN_HOLD + Duration::from_secs(1),
            "the {RETURN_HOLD:?} return hold leaves Vector no room inside the {INTAKE_HOLD:?} \
             intake hold"
        );
        assert!(
            RETURN_HOLD > send,
            "a {send:?} send outlives the {RETURN_HOLD:?} return hold"
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

    /// Records the gauges registered through it, and nothing else.
    #[derive(Default)]
    struct GaugeKeys(std::sync::Mutex<Vec<metrics::Key>>);

    impl metrics::Recorder for GaugeKeys {
        fn describe_counter(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn describe_gauge(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn describe_histogram(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn register_counter(
            &self,
            _: &metrics::Key,
            _: &metrics::Metadata<'_>,
        ) -> metrics::Counter {
            metrics::Counter::noop()
        }
        fn register_gauge(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(key.clone());
            metrics::Gauge::noop()
        }
        fn register_histogram(
            &self,
            _: &metrics::Key,
            _: &metrics::Metadata<'_>,
        ) -> metrics::Histogram {
            metrics::Histogram::noop()
        }
    }

    /// Each leg publishes its own guarantee series, told apart by `listener`,
    /// rather than both writing the one unlabelled series.
    #[tokio::test]
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

        let keys = GaugeKeys::default();
        metrics::with_local_recorder(&keys, || bridge.publish_guarantees());

        let published: Vec<(String, String)> = keys
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|key| key.name() == "pipeline_delivery_guarantee")
            .map(|key| {
                let label = |name: &str| {
                    key.labels()
                        .find(|l| l.key() == name)
                        .map(|l| l.value().to_owned())
                        .unwrap_or_default()
                };
                (label("listener"), label("guarantee"))
            })
            .collect();
        assert_eq!(
            published,
            vec![
                ("inbound".to_owned(), "at_least_once".to_owned()),
                ("outbound".to_owned(), "at_least_once".to_owned()),
            ]
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
