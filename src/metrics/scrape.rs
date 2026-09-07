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
//! Merged series are never removed: a component Vector drops keeps its last
//! value until the pod restarts.

use std::sync::Arc;
use std::time::{Duration, Instant};

use prometheus_parse::{Scrape, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, warn};

use super::WrapperMetrics;
use crate::config::generate::{SINK_LABEL, SOURCE_LABEL};

/// Vector's per-component counter of events read in.
const VECTOR_RECEIVED: &str = "vector_component_received_events_total";

/// Vector's per-component counter of events emitted.
const VECTOR_SENT: &str = "vector_component_sent_events_total";

/// Vector label naming the pipeline component a sample belongs to.
const COMPONENT_ID: &str = "component_id";

/// Connect timeout for one scrape.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// Read timeout for one scrape.
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Minimum gap between unreachable-exporter WARN lines.
///
/// A crash-looping Vector is unreachable on every tick, so the first failure
/// warns and the rest only bump the counter until this elapses.
const WARN_EVERY: Duration = Duration::from_secs(300);

/// Throughput totals lifted out of one exposition.
#[derive(Debug, Default, PartialEq)]
pub struct Derived {
    /// `vector_component_received_events_total` on the wrapper's Kafka source.
    pub received: f64,
    /// `vector_component_sent_events_total` on the wrapper's Kafka sink.
    pub sent: f64,
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
/// timeout, or a non-200 status.
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

    let mut response = String::new();
    tokio::time::timeout(READ_TIMEOUT, stream.read_to_string(&mut response))
        .await
        .map_err(|_| format!("read from {address} timed out"))?
        .map_err(|e| format!("read from {address} failed: {e}"))?;

    let status = response.lines().next().unwrap_or_default();
    if !status.contains(" 200") {
        return Err(format!("{address} answered {status}"));
    }

    response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .ok_or_else(|| format!("{address} returned a response with no body"))
}

/// Register every sample in `text` on the global recorder.
///
/// Returns the throughput totals the derived app counters are built from.
/// An exposition that does not parse merges nothing and logs at debug.
#[must_use]
pub fn merge_exposition(text: &str) -> Derived {
    let scrape = match Scrape::parse(text.lines().map(|l| Ok(l.to_string()))) {
        Ok(s) => s,
        Err(e) => {
            debug!(error = %e, "Vector exposition did not parse");
            return Derived::default();
        }
    };

    let mut derived = Derived::default();

    for sample in &scrape.samples {
        let labels: Vec<metrics::Label> = sample
            .labels
            .iter()
            .map(|(k, v)| metrics::Label::new(k.clone(), v.clone()))
            .collect();

        match &sample.value {
            Value::Counter(v) => {
                metrics::counter!(sample.metric.clone(), labels).absolute(to_u64(*v));
            }
            Value::Gauge(v) => {
                metrics::gauge!(sample.metric.clone(), labels).set(*v);
            }
            // A histogram arrives pre-aggregated, and the metrics crate's
            // Histogram takes observations, not buckets -- replaying them
            // would re-bucket the data through scalo's own summary and
            // change the numbers. Republishing each cumulative bucket as a
            // counter round-trips the exposition unchanged instead.
            Value::Histogram(buckets) => {
                let name = format!("{}_bucket", sample.metric);
                for bucket in buckets {
                    let mut with_le = labels.clone();
                    with_le.push(metrics::Label::new("le", format_le(bucket.less_than)));
                    metrics::counter!(name.clone(), with_le).absolute(to_u64(bucket.count));
                }
            }
            Value::Summary(quantiles) => {
                for q in quantiles {
                    let mut with_q = labels.clone();
                    with_q.push(metrics::Label::new("quantile", q.quantile.to_string()));
                    metrics::gauge!(sample.metric.clone(), with_q).set(q.count);
                }
            }
            // No TYPE line, which is also where a histogram's _sum and _count
            // land. Integral totals stay counters; anything else is a gauge so
            // a fractional _sum keeps its precision.
            Value::Untyped(v) => {
                if sample.metric.ends_with("_total") || sample.metric.ends_with("_count") {
                    metrics::counter!(sample.metric.clone(), labels).absolute(to_u64(*v));
                } else {
                    metrics::gauge!(sample.metric.clone(), labels).set(*v);
                }
            }
        }

        let component = sample.labels.get(COMPONENT_ID);
        if let Some(v) = counter_value(&sample.value) {
            match (sample.metric.as_str(), component) {
                (VECTOR_RECEIVED, Some(SOURCE_LABEL)) => derived.received += v,
                (VECTOR_SENT, Some(SINK_LABEL)) => derived.sent += v,
                _ => {}
            }
        }
    }

    derived
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
}

/// Scrape Vector's exporter on `interval` and merge each exposition.
///
/// Spawns a background task and returns immediately. A scrape that fails bumps
/// `transform_vector_scrape_failures_total` and warns at most once every five
/// minutes; it never flips readiness and never aborts the loop.
pub fn spawn_vector_scrape_task(
    metrics: Arc<WrapperMetrics>,
    address: String,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_warn: Option<Instant> = None;

        loop {
            ticker.tick().await;
            match fetch_exposition(&address).await {
                Ok(body) => {
                    let derived = merge_exposition(&body);
                    apply_derived(&metrics, &derived);
                    debug!(
                        received = derived.received,
                        sent = derived.sent,
                        "merged Vector metrics into the scalo registry"
                    );
                }
                Err(reason) => {
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
        let derived = merge_exposition(EXPOSITION);
        assert_eq!(
            derived,
            Derived {
                received: 85.0,
                sent: 84.0
            },
            "sent must come from the sink, not the source"
        );
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
        assert_eq!(merge_exposition(text), Derived::default());
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
}
