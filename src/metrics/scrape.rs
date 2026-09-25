// Project:   dfe-transform-vector
// File:      src/metrics/scrape.rs
// Purpose:   Scrape Vector's prometheus_exporter and merge it into the scalo registry
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Merge Vector's `internal_metrics` into the scalo registry.
//!
//! Vector exports its own counters through a `prometheus_exporter` sink on
//! `metrics.vector_metrics_address`. Nothing scraped that address, so
//! `vector_*` reached neither `/metrics` nor the OTLP push, and the wrapper's
//! own throughput counters read 0 while events flowed.
//!
//! This module closes that: a background task GETs the exporter on scalo's
//! metrics interval, parses the text exposition, and re-registers every sample
//! on the global `metrics` recorder that scalo's `MetricsManager` owns. From
//! there `vector_*` rides the normal app path -- the `/metrics` scrape and the
//! OTLP push both carry it, with the platform namespace and labels scalo
//! applies to every other metric.
//!
//! A component Vector drops stops being scraped, so its gauges are zeroed once
//! they have gone unseen for `metrics.vector_metrics_expiry_ticks` ticks.

use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use metrics::Label;
use prometheus_parse::{Scrape, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, info, warn};

use super::WrapperMetrics;
use crate::config::generate::{SINK_LABEL, SIZE_CAP_LABEL, SOURCE_LABEL};

/// Vector's per-component counter of events read in.
const VECTOR_RECEIVED: &str = "vector_component_received_events_total";

/// Vector's per-component counter of events emitted.
const VECTOR_SENT: &str = "vector_component_sent_events_total";

/// Vector's per-component counter of events dropped, on purpose or not.
const VECTOR_DISCARDED: &str = "vector_component_discarded_events_total";

/// Vector's per-component counter of errors.
const VECTOR_ERRORS: &str = "vector_component_errors_total";

/// Vector label naming the pipeline component a sample belongs to.
const COMPONENT_ID: &str = "component_id";

/// Connect timeout for one scrape.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// Read timeout for one scrape.
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Cap on one exposition, headers included.
///
/// `READ_TIMEOUT` bounds how long a scrape may take, not how much it may
/// return. Vector's own exposition is well under a megabyte; anything past
/// this cap is a runaway exporter, not metrics.
const MAX_EXPOSITION_BYTES: u64 = 4 * 1024 * 1024;

/// Ticks a merged gauge may go unseen before it is zeroed.
pub const DEFAULT_EXPIRY_TICKS: u32 = 4;

/// Minimum gap between unreachable-exporter WARN lines.
///
/// A crash-looping Vector is unreachable on every tick, so the first failure
/// warns and the rest only bump the counter until this elapses.
const WARN_EVERY: Duration = Duration::from_secs(300);

/// Throughput totals lifted out of one exposition.
#[derive(Debug, Default, PartialEq)]
pub struct Derived {
    /// `vector_component_received_events_total` on the wrapper's source.
    pub received: f64,
    /// `vector_component_sent_events_total` on the wrapper's sink.
    pub sent: f64,
    /// `vector_component_received_events_total` on the wrapper's sink: what it
    /// has taken on, delivered or not.
    pub sink_received: f64,
    /// `vector_component_discarded_events_total` on the wrapper's sink: records
    /// it rejected and dropped.
    pub sink_discarded: f64,
    /// `vector_component_errors_total` on the wrapper's sink.
    pub sink_errors: f64,
    /// `vector_component_discarded_events_total` on the size cap: records over
    /// the producer's ceiling, held back from the sink.
    pub oversize: f64,
}

impl Derived {
    /// Records the pipeline lost after taking them in: rejected by the sink,
    /// or held back by the size cap.
    #[must_use]
    pub fn rejected(&self) -> f64 {
        self.sink_discarded + self.oversize
    }

    /// Records the sink has taken on and neither delivered nor dropped.
    #[must_use]
    pub fn outstanding(&self) -> f64 {
        (self.sink_received - self.sent - self.sink_discarded).max(0.0)
    }
}

/// Fetch the exposition from Vector's `prometheus_exporter`.
///
/// Raw HTTP/1.1 over TCP: the request is one line, the response is read to EOF
/// under `Connection: close`, and pulling in an HTTP client crate for a
/// loopback GET is not worth the build cost.
///
/// # Errors
///
/// Returns the reason the exporter could not be read -- connect refused,
/// timeout, a non-200 status, or a body past the 4 MiB cap.
pub async fn fetch_exposition(address: &str) -> Result<String, String> {
    let mut stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(address))
        .await
        .map_err(|_| format!("connect to {address} timed out"))?
        .map_err(|e| format!("connect to {address} failed: {e}"))?;

    let request = format!("GET /metrics HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|e| format!("write to {address} failed: {e}"))?;

    // One byte past the cap, so a body that fills it is still distinguishable
    // from one that overran it.
    let mut capped = (&mut stream).take(MAX_EXPOSITION_BYTES + 1);
    let mut raw = Vec::new();
    tokio::time::timeout(READ_TIMEOUT, capped.read_to_end(&mut raw))
        .await
        .map_err(|_| format!("read from {address} timed out"))?
        .map_err(|e| format!("read from {address} failed: {e}"))?;

    if raw.len() as u64 > MAX_EXPOSITION_BYTES {
        // Merging a truncated exposition would publish a partial registry as if
        // it were the whole one, so the scrape fails instead.
        return Err(format!(
            "{address} returned more than {MAX_EXPOSITION_BYTES} bytes"
        ));
    }

    let response =
        String::from_utf8(raw).map_err(|_| format!("{address} returned a non-UTF-8 exposition"))?;

    let status = response.lines().next().unwrap_or_default();
    if !status.contains(" 200") {
        return Err(format!("{address} answered {status}"));
    }

    response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .ok_or_else(|| format!("{address} returned a response with no body"))
}

/// The handle a merged series was registered on.
enum Handle {
    Counter(metrics::Counter),
    Gauge(metrics::Gauge),
}

/// One merged series: its handle, the key it was registered under, and how many
/// ticks since Vector last reported it.
struct Tracked {
    name: String,
    labels: Vec<Label>,
    handle: Handle,
    unseen: u32,
}

impl Tracked {
    /// Confirm the entry really is this series, so a hash collision cannot hand
    /// one series another's handle.
    fn matches(&self, name: &str, pairs: &[(&str, &str)]) -> bool {
        self.name == name
            && self.labels.len() == pairs.len()
            && self
                .labels
                .iter()
                .zip(pairs)
                .all(|(l, (k, v))| l.key() == *k && l.value() == *v)
    }
}

/// Merges expositions onto the global recorder, tick after tick.
///
/// Carries the state one scrape cannot: the line buffer the parser drains, and
/// the series already registered. A cached series costs a hash and an atomic
/// store per tick -- the name and labels are built once, at first sight.
pub struct ExpositionMerger {
    /// Reused across ticks. Only the spine survives -- prometheus-parse takes
    /// owned lines and consumes them.
    lines: Vec<std::io::Result<String>>,
    /// Merged series by hash of name plus sorted labels.
    series: HashMap<u64, Tracked>,
    expiry_ticks: u32,
}

impl Default for ExpositionMerger {
    fn default() -> Self {
        Self::new(DEFAULT_EXPIRY_TICKS)
    }
}

impl ExpositionMerger {
    /// A merger that zeroes a gauge after `expiry_ticks` ticks without it.
    #[must_use]
    pub fn new(expiry_ticks: u32) -> Self {
        Self {
            lines: Vec::new(),
            series: HashMap::new(),
            expiry_ticks,
        }
    }

    /// Register every sample in `text` on the global recorder.
    ///
    /// Returns the throughput totals the derived app counters are built from.
    /// An exposition that does not parse merges nothing, ages nothing, and logs
    /// at debug.
    pub fn merge(&mut self, text: &str) -> Derived {
        self.lines.extend(text.lines().map(|l| Ok(l.to_string())));
        let scrape = match Scrape::parse(self.lines.drain(..)) {
            Ok(s) => s,
            Err(e) => {
                debug!(error = %e, "Vector exposition did not parse");
                return Derived::default();
            }
        };

        self.tick();

        let mut derived = Derived::default();
        // Label refs, reused across every sample in this exposition.
        let mut pairs: Vec<(&str, &str)> = Vec::new();

        for sample in &scrape.samples {
            if let Some(v) = counter_value(&sample.value) {
                match (sample.metric.as_str(), sample.labels.get(COMPONENT_ID)) {
                    (VECTOR_RECEIVED, Some(SOURCE_LABEL)) => derived.received += v,
                    (VECTOR_SENT, Some(SINK_LABEL)) => derived.sent += v,
                    (VECTOR_RECEIVED, Some(SINK_LABEL)) => derived.sink_received += v,
                    (VECTOR_DISCARDED, Some(SINK_LABEL)) => derived.sink_discarded += v,
                    (VECTOR_ERRORS, Some(SINK_LABEL)) => derived.sink_errors += v,
                    (VECTOR_DISCARDED, Some(SIZE_CAP_LABEL)) => derived.oversize += v,
                    _ => {}
                }
            }

            pairs.clear();
            pairs.extend(sample.labels.iter().map(|(k, v)| (k.as_str(), v.as_str())));
            // Sorted so a series keeps one registry key across ticks --
            // HashMap iteration order differs between map instances.
            pairs.sort_unstable();

            match &sample.value {
                Value::Counter(v) => self.counter(&sample.metric, &pairs, to_u64(*v)),
                Value::Gauge(v) => self.gauge(&sample.metric, &pairs, *v),
                // A histogram arrives pre-aggregated, and the metrics crate's
                // Histogram takes observations, not buckets -- replaying them
                // would re-bucket the data through scalo's own summary and
                // change the numbers. Republishing each cumulative bucket as a
                // counter round-trips the exposition unchanged instead.
                Value::Histogram(buckets) => {
                    let name = format!("{}_bucket", sample.metric);
                    for bucket in buckets {
                        let le = format_le(bucket.less_than);
                        let mut with_le = pairs.clone();
                        with_le.push(("le", &le));
                        self.counter(&name, &with_le, to_u64(bucket.count));
                    }
                }
                Value::Summary(quantiles) => {
                    for q in quantiles {
                        let quantile = q.quantile.to_string();
                        let mut with_q = pairs.clone();
                        with_q.push(("quantile", &quantile));
                        self.gauge(&sample.metric, &with_q, q.count);
                    }
                }
                // No TYPE line, which is also where a histogram's _sum and
                // _count land. Integral totals stay counters; anything else is
                // a gauge so a fractional _sum keeps its precision.
                Value::Untyped(v) => {
                    if sample.metric.ends_with("_total") || sample.metric.ends_with("_count") {
                        self.counter(&sample.metric, &pairs, to_u64(*v));
                    } else {
                        self.gauge(&sample.metric, &pairs, *v);
                    }
                }
            }
        }

        self.expire();
        derived
    }

    /// Age every series by one tick with no exposition to match it, so a scrape
    /// that never lands expires its gauges the same way a dropped component does.
    pub fn age(&mut self) {
        self.tick();
        self.expire();
    }

    /// One tick older, before this exposition marks what it carries as seen.
    fn tick(&mut self) {
        for tracked in self.series.values_mut() {
            tracked.unseen += 1;
        }
    }

    /// Hash of one series: its name and its sorted labels.
    fn series_hash(&self, name: &str, pairs: &[(&str, &str)]) -> u64 {
        let mut hasher = self.series.hasher().build_hasher();
        name.hash(&mut hasher);
        pairs.hash(&mut hasher);
        hasher.finish()
    }

    /// Set a counter to `value`, registering the series on first sight.
    fn counter(&mut self, name: &str, pairs: &[(&str, &str)], value: u64) {
        let series = self.series_hash(name, pairs);
        if let Some(tracked) = self.series.get_mut(&series)
            && tracked.matches(name, pairs)
            && let Handle::Counter(c) = &tracked.handle
        {
            c.absolute(value);
            tracked.unseen = 0;
            return;
        }

        let labels = to_labels(pairs);
        let handle = metrics::counter!(name.to_string(), labels.clone());
        handle.absolute(value);
        self.track(series, name, labels, Handle::Counter(handle));
    }

    /// Set a gauge to `value`, registering the series on first sight.
    fn gauge(&mut self, name: &str, pairs: &[(&str, &str)], value: f64) {
        let series = self.series_hash(name, pairs);
        if let Some(tracked) = self.series.get_mut(&series)
            && tracked.matches(name, pairs)
            && let Handle::Gauge(g) = &tracked.handle
        {
            g.set(value);
            tracked.unseen = 0;
            return;
        }

        let labels = to_labels(pairs);
        let handle = metrics::gauge!(name.to_string(), labels.clone());
        handle.set(value);
        self.track(series, name, labels, Handle::Gauge(handle));
    }

    fn track(&mut self, series: u64, name: &str, labels: Vec<Label>, handle: Handle) {
        self.series.insert(
            series,
            Tracked {
                name: name.to_string(),
                labels,
                handle,
                unseen: 0,
            },
        );
    }

    /// Drop every series unseen for `expiry_ticks`, zeroing the gauges.
    ///
    /// Zeroed rather than unregistered: the facade hands out handles, not a
    /// registry, and a utilisation gauge frozen at its last value reads as a
    /// component that is still busy. A counter is left flat instead --
    /// `absolute` is monotonic, and a flat counter already rates to zero.
    fn expire(&mut self) {
        let limit = self.expiry_ticks;
        self.series.retain(|_, tracked| {
            if tracked.unseen < limit {
                return true;
            }
            if let Handle::Gauge(g) = &tracked.handle {
                g.set(0.0);
            }
            false
        });
    }
}

/// Own the label pairs a series is registered under.
fn to_labels(pairs: &[(&str, &str)]) -> Vec<Label> {
    pairs
        .iter()
        .map(|(k, v)| Label::new((*k).to_string(), (*v).to_string()))
        .collect()
}

/// Point the app's own throughput counters at Vector's.
///
/// `Counter::absolute` keeps the running maximum, so a Vector restart holds
/// the totals where they were instead of reporting a reset the pod did not
/// have.
pub fn apply_derived(metrics: &WrapperMetrics, derived: &Derived) {
    metrics
        .app
        .records_received
        .absolute(to_u64(derived.received));
    metrics.app.records_processed.absolute(to_u64(derived.sent));
    // The platform's sink-side counter is the same number by another name and
    // reads 0 without this. ServiceMetrics only exposes an increment.
    metrics::counter!("records_delivered_total").absolute(to_u64(derived.sent));
    // A record Vector rejected or the size cap held back is gone, so it counts
    // where every other app counts a lost record.
    metrics
        .app
        .records_error
        .absolute(to_u64(derived.rejected()));
    metrics
        .sink_errors_total
        .absolute(to_u64(derived.sink_errors));
}

/// Whether the sink is still moving the records it has taken on.
///
/// Vector with an unlimited producer timeout holds a record it cannot deliver
/// -- a partition with no leader, a broker that is down -- for as long as that
/// lasts, while the process stays healthy. Stalled means records are
/// outstanding and none has been delivered or dropped for `stall_after`.
#[derive(Debug)]
pub struct SinkProgress {
    stall_after: Duration,
    /// Delivered plus dropped at the last advance.
    settled: f64,
    last_advance: Instant,
}

impl SinkProgress {
    /// A tracker that calls the sink stalled after `stall_after` without
    /// progress. A zero `stall_after` never does.
    #[must_use]
    pub fn new(stall_after: Duration, now: Instant) -> Self {
        Self {
            stall_after,
            settled: 0.0,
            last_advance: now,
        }
    }

    /// Record one exposition's totals; `true` when the sink is stalled.
    pub fn observe(&mut self, derived: &Derived, now: Instant) -> bool {
        let settled = derived.sent + derived.sink_discarded;
        // A restarted Vector starts its counters again from zero.
        if settled != self.settled || derived.outstanding() == 0.0 {
            self.settled = settled;
            self.last_advance = now;
        }
        !self.stall_after.is_zero()
            && derived.outstanding() > 0.0
            && now.saturating_duration_since(self.last_advance) >= self.stall_after
    }
}

/// Scrape Vector's exporter on `interval` and merge each exposition.
///
/// Spawns a background task and returns immediately. A scrape that fails bumps
/// `transform_vector_scrape_failures_total` and warns at most once every five
/// minutes; it never aborts the loop and leaves `stalled` as it was. A gauge
/// unseen for `expiry_ticks` ticks is zeroed.
///
/// `stalled` is set while the sink has records outstanding and has made no
/// progress for `stall_after`, and cleared when it moves again.
pub fn spawn_vector_scrape_task(
    metrics: Arc<WrapperMetrics>,
    address: String,
    interval: Duration,
    expiry_ticks: u32,
    stall_after: Duration,
    stalled: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_warn: Option<Instant> = None;
        let mut merger = ExpositionMerger::new(expiry_ticks);
        let mut progress = SinkProgress::new(stall_after, Instant::now());

        loop {
            ticker.tick().await;
            match fetch_exposition(&address).await {
                Ok(body) => {
                    let derived = merger.merge(&body);
                    apply_derived(&metrics, &derived);
                    let now_stalled = progress.observe(&derived, Instant::now());
                    if stalled.swap(now_stalled, Ordering::Relaxed) != now_stalled {
                        if now_stalled {
                            warn!(
                                outstanding = derived.outstanding(),
                                stall_secs = stall_after.as_secs(),
                                "Vector's sink has delivered nothing it holds; reporting not ready"
                            );
                        } else {
                            info!("Vector's sink is delivering again; reporting ready");
                        }
                    }
                    debug!(
                        received = derived.received,
                        sent = derived.sent,
                        outstanding = derived.outstanding(),
                        rejected = derived.rejected(),
                        "merged Vector metrics into the scalo registry"
                    );
                }
                Err(reason) => {
                    merger.age();
                    metrics.scrape_failures_total.increment(1);
                    let due = last_warn.is_none_or(|t| t.elapsed() >= WARN_EVERY);
                    if due {
                        warn!(
                            error = %reason,
                            "Vector metrics exporter unreachable; vector_* will be stale"
                        );
                        last_warn = Some(Instant::now());
                    }
                }
            }
        }
    })
}

/// Serialise a bucket bound the way Prometheus spells it.
fn format_le(bound: f64) -> String {
    if bound.is_infinite() {
        "+Inf".to_string()
    } else {
        bound.to_string()
    }
}

/// Clamp an exposition float into the counter's integer domain.
fn to_u64(v: f64) -> u64 {
    if v.is_finite() && v > 0.0 {
        // `as` saturates at u64::MAX for a finite float since Rust 1.45.
        v as u64
    } else {
        0
    }
}

/// The scalar behind a counter-shaped sample, if it has one.
fn counter_value(value: &Value) -> Option<f64> {
    match value {
        Value::Counter(v) | Value::Untyped(v) => Some(*v),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXPOSITION: &str = "\
# HELP vector_component_received_events_total events in
# TYPE vector_component_received_events_total counter
vector_component_received_events_total{component_id=\"dfe_source\",component_type=\"kafka\"} 85
# HELP vector_component_sent_events_total events out
# TYPE vector_component_sent_events_total counter
vector_component_sent_events_total{component_id=\"dfe_sink\",component_kind=\"sink\"} 84
vector_component_sent_events_total{component_id=\"dfe_source\",component_kind=\"source\"} 85
# HELP vector_utilization component busy ratio
# TYPE vector_utilization gauge
vector_utilization{component_id=\"dfe_sink\"} 0.25
# HELP vector_buffer_events buffered events
# TYPE vector_buffer_events gauge
vector_buffer_events 7
";

    #[test]
    fn derived_totals_come_from_the_wrapper_components() {
        let derived = ExpositionMerger::default().merge(EXPOSITION);
        assert_eq!(
            derived,
            Derived {
                received: 85.0,
                sent: 84.0,
                ..Derived::default()
            },
            "sent must come from the sink, not the source"
        );
    }

    /// A record Vector rejected, or the size cap held back, is counted as
    /// lost; the samples are the shapes Vector 0.58 exports.
    #[test]
    fn rejected_records_come_from_the_sink_and_the_size_cap() {
        let text = "\
# TYPE vector_component_received_events_total counter
vector_component_received_events_total{component_id=\"dfe_sink\",component_kind=\"sink\"} 100
# TYPE vector_component_sent_events_total counter
vector_component_sent_events_total{component_id=\"dfe_sink\",component_kind=\"sink\"} 90
# TYPE vector_component_discarded_events_total counter
vector_component_discarded_events_total{component_id=\"dfe_sink\",intentional=\"false\"} 4
vector_component_discarded_events_total{component_id=\"dfe_size_cap\",intentional=\"true\"} 3
vector_component_discarded_events_total{component_id=\"user_filter\",intentional=\"true\"} 50
# TYPE vector_component_errors_total counter
vector_component_errors_total{component_id=\"dfe_sink\",error_type=\"request_failed\",stage=\"sending\"} 2
";
        let derived = ExpositionMerger::default().merge(text);
        assert_eq!(derived.sink_received, 100.0);
        assert_eq!(derived.sink_discarded, 4.0);
        assert_eq!(derived.oversize, 3.0);
        assert_eq!(derived.sink_errors, 2.0);
        assert_eq!(
            derived.rejected(),
            7.0,
            "an operator's own filter is not a loss"
        );
        assert_eq!(derived.outstanding(), 6.0);
    }

    fn holding(sink_received: f64, sent: f64) -> Derived {
        Derived {
            sink_received,
            sent,
            ..Derived::default()
        }
    }

    #[test]
    fn a_sink_that_holds_records_without_delivering_is_stalled_after_the_window() {
        let start = Instant::now();
        let window = Duration::from_secs(60);
        let mut progress = SinkProgress::new(window, start);

        assert!(!progress.observe(&holding(100.0, 90.0), start));
        assert!(
            !progress.observe(&holding(120.0, 90.0), start + Duration::from_secs(59)),
            "not yet stalled inside the window"
        );
        assert!(
            progress.observe(&holding(140.0, 90.0), start + window),
            "records outstanding and nothing delivered for the window"
        );
        assert!(
            !progress.observe(
                &holding(140.0, 95.0),
                start + window + Duration::from_secs(1)
            ),
            "one delivery clears it"
        );
    }

    #[test]
    fn an_idle_sink_is_never_stalled() {
        let start = Instant::now();
        let mut progress = SinkProgress::new(Duration::from_secs(1), start);
        assert!(!progress.observe(&holding(50.0, 50.0), start + Duration::from_secs(600)));
    }

    #[test]
    fn a_restarted_vector_resets_the_window_rather_than_reading_as_stalled() {
        let start = Instant::now();
        let mut progress = SinkProgress::new(Duration::from_secs(10), start);
        assert!(!progress.observe(&holding(100.0, 90.0), start));
        // Counters start from zero again after a restart.
        assert!(!progress.observe(&holding(5.0, 1.0), start + Duration::from_secs(11)));
    }

    #[test]
    fn a_zero_window_never_stalls() {
        let start = Instant::now();
        let mut progress = SinkProgress::new(Duration::ZERO, start);
        assert!(!progress.observe(&holding(100.0, 0.0), start + Duration::from_secs(3600)));
    }

    #[test]
    fn histogram_buckets_keep_the_le_label() {
        let text = "\
# TYPE vector_rtt_seconds histogram
vector_rtt_seconds_bucket{le=\"0.005\"} 3
vector_rtt_seconds_bucket{le=\"+Inf\"} 9
vector_rtt_seconds_sum 0.42
vector_rtt_seconds_count 9
";
        // Parses clean and yields no throughput -- the derived counters must
        // not pick up a histogram's _count.
        assert_eq!(ExpositionMerger::default().merge(text), Derived::default());
    }

    #[test]
    fn a_gauge_that_stops_being_reported_is_zeroed_after_the_expiry_ticks() {
        let tracks = |m: &ExpositionMerger, name: &str| m.series.values().any(|t| t.name == name);

        let mut merger = ExpositionMerger::new(2);
        merger
            .merge("# TYPE vector_utilization gauge\nvector_utilization{component_id=\"a\"} 0.9\n");
        assert!(tracks(&merger, "vector_utilization"), "gauge not tracked");

        // Vector drops the component: the series ages one tick per merge, and
        // a scrape that never lands ages it the same way.
        merger.merge("# TYPE vector_other gauge\nvector_other 1\n");
        assert!(
            tracks(&merger, "vector_utilization"),
            "one tick unseen is not yet expired"
        );
        merger.age();
        assert!(
            !tracks(&merger, "vector_utilization"),
            "the dropped gauge was not expired"
        );
    }

    #[test]
    fn a_series_seen_again_is_not_re_registered() {
        let mut merger = ExpositionMerger::default();
        merger.merge(EXPOSITION);
        let registered = merger.series.len();
        merger.merge(EXPOSITION);
        assert_eq!(
            merger.series.len(),
            registered,
            "the second merge registered the same series twice"
        );
        assert!(
            merger.series.values().all(|t| t.unseen == 0),
            "a series carried by the exposition must not be ageing"
        );
    }

    #[test]
    fn the_line_buffer_is_reused_across_merges() {
        let mut merger = ExpositionMerger::default();
        merger.merge(EXPOSITION);
        let capacity = merger.lines.capacity();
        merger.merge(EXPOSITION);
        assert!(
            merger.lines.is_empty(),
            "the buffer is drained by the parser"
        );
        assert_eq!(
            merger.lines.capacity(),
            capacity,
            "the second merge reallocated the line buffer"
        );
    }

    #[test]
    fn infinite_bucket_bound_renders_as_prometheus_spells_it() {
        assert_eq!(format_le(f64::INFINITY), "+Inf");
        assert_eq!(format_le(0.005), "0.005");
    }

    #[test]
    fn negative_and_non_finite_counters_clamp_to_zero() {
        assert_eq!(to_u64(-1.0), 0);
        assert_eq!(to_u64(f64::NAN), 0);
        assert_eq!(to_u64(12.9), 12);
    }

    /// A counter, a gauge and a 6-bucket histogram per group, ~3000 lines.
    fn large_exposition() -> String {
        let mut text = String::new();
        for i in 0..231 {
            let labels = format!("component_id=\"c{i}\",component_kind=\"sink\",pipeline=\"p{i}\"");
            text.push_str(&format!("# TYPE vector_bytes_{i}_total counter\n"));
            text.push_str(&format!("vector_bytes_{i}_total{{{labels}}} {i}\n"));
            text.push_str(&format!("# TYPE vector_util_{i} gauge\n"));
            text.push_str(&format!("vector_util_{i}{{{labels}}} 0.25\n"));
            text.push_str(&format!("# TYPE vector_rtt_{i}_seconds histogram\n"));
            for (n, le) in ["0.005", "0.01", "0.05", "0.1", "1", "+Inf"]
                .iter()
                .enumerate()
            {
                text.push_str(&format!(
                    "vector_rtt_{i}_seconds_bucket{{{labels},le=\"{le}\"}} {n}\n"
                ));
            }
            text.push_str(&format!("vector_rtt_{i}_seconds_sum{{{labels}}} 0.42\n"));
            text.push_str(&format!("vector_rtt_{i}_seconds_count{{{labels}}} 6\n"));
        }
        text
    }

    /// Merge cost over a large exposition -- printed, never asserted, because a
    /// timing assertion is a flake. No recorder is installed, so this measures
    /// parse plus key/label construction, not the registry lookup.
    ///
    /// `cargo test --release --lib -- --ignored --nocapture merge_cost`
    #[test]
    #[ignore = "timing measurement, not pass/fail"]
    fn merge_cost_over_a_large_exposition() {
        let text = large_exposition();
        let rounds = 100;
        let mut merger = ExpositionMerger::default();
        let start = std::time::Instant::now();
        for _ in 0..rounds {
            let _ = merger.merge(&text);
        }
        let elapsed = start.elapsed();

        // Without the series cache: a fresh merger re-registers every series.
        let uncached_start = std::time::Instant::now();
        for _ in 0..rounds {
            let _ = ExpositionMerger::default().merge(&text);
        }
        let uncached = uncached_start.elapsed();

        // The floor: what prometheus-parse alone costs on the same input.
        let mut lines: Vec<std::io::Result<String>> = Vec::new();
        let parse_start = std::time::Instant::now();
        for _ in 0..rounds {
            lines.extend(text.lines().map(|l| Ok(l.to_string())));
            let _ = Scrape::parse(lines.drain(..));
        }
        let parse = parse_start.elapsed();

        println!(
            "lines={} rounds={rounds} per_merge={:?} uncached={:?} parse_floor={:?}",
            text.lines().count(),
            elapsed / rounds,
            uncached / rounds,
            parse / rounds
        );
    }
}
