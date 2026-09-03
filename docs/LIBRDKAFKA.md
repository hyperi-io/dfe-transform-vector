# librdkafka Configuration Reference

DFE services use librdkafka (via Vector's Kafka source/sink or via rdkafka crate
directly). This document covers the shared baseline defaults, per-service
overrides, and how to customise settings through the config cascade.

## Design Philosophy

**Only set values that differ from librdkafka defaults.** Every non-default
setting must have clear justification. Fewer overrides means fewer surprises,
easier debugging, and automatic benefit from upstream librdkafka improvements.

## Configuration Cascade

THREE layers merge in priority order (highest wins):

| Layer | Source | Who Manages |
|-------|--------|-------------|
| 3. User config YAML | `librdkafka_options:` in service config | Platform operator |
| 2. Service-specific | Hardcoded in each service's generator | Service developer |
| 1. Baseline | Central `librdkafka.yaml` if present, else `scalo::kafka_config` constants | Platform team / shared library |

The baseline is ONE layer, not two. A profile in the central `librdkafka.yaml`
**replaces** the scalo constant for that profile outright (see [Central Config
File](#central-config-file)); it does not merge with it and it does not sit
above the service-specific constants. So a central value for a key a service
also hardcodes is overwritten by the service.

That last part matters when tuning: `queue.buffering.max.kbytes` is a
transform-vector service override, and setting it centrally will NOT take
effect. Set it per-instance in `sink.librdkafka_options` instead -- layer 3 is
above everything.

### Example

A transform-vector pod producing to Kafka:

1. Baseline: scalo `PRODUCER_PRODUCTION` sets `linger.ms=100`,
   `compression.type=zstd`, `socket.nagle.disable=true`,
   `statistics.interval.ms=1000` -- or, if `librdkafka.yaml` defines
   `producer.production`, exactly what that profile lists and nothing else
2. Service override adds `queue.buffering.max.kbytes=262144` (256 MiB cap)
3. User config YAML sets `compression.type=lz4` for a specific pipeline

Result: `linger.ms=100`, `compression.type=lz4`,
`queue.buffering.max.kbytes=262144`, `socket.nagle.disable=true`,
`statistics.interval.ms=1000`

## Consumer Profiles

### Production (`CONSUMER_PRODUCTION`)

High-throughput consumer baseline. Used by dfe-loader, dfe-transform-vector,
and any service consuming from Kafka in production.

| Setting | Value | librdkafka Default | Justification |
|---------|-------|--------------------|---------------|
| `partition.assignment.strategy` | `cooperative-sticky` | `range,roundrobin` | KIP-429: incremental rebalances avoid stop-the-world pauses. Partitions stay assigned during rebalance — only moving partitions are revoked. |
| `fetch.min.bytes` | `1048576` (1 MiB) | `1` | Batch fetches for throughput. Broker waits until 1 MiB of data is available before responding, reducing fetch round-trips. |
| `fetch.wait.max.ms` | `100` | `500` | Upper bound on fetch latency when `fetch.min.bytes` threshold isn't met. Prevents 500 ms stalls on low-volume topics. |
| `queued.min.messages` | `20000` | `100000` | Pre-fetch queue depth. 10-20K is the efficiency sweet spot — large enough for batching, small enough to avoid excessive memory and rebalance lag. |
| `enable.auto.commit` | `false` | `true` | DFE services manage offset commits explicitly after processing (at-least-once guarantee). Auto-commit risks data loss on crash. |
| `statistics.interval.ms` | `1000` | `0` (disabled) | Enable librdkafka internal metrics at 1-second granularity for Prometheus scraping. |

**Settings intentionally left at default:**

| Setting | Default | Why Not Changed |
|---------|---------|-----------------|
| `queued.max.messages.kbytes` | `65536` (64 MiB) | Adequate for DFE message sizes |
| `socket.receive.buffer.bytes` | `0` (OS default) | OS auto-tuning is better than static values |
| `auto.offset.reset` | `latest` | Controlled per-service via big-dial config, not librdkafka |
| `session.timeout.ms` | `45000` | Default is sensible for K8s environments |
| `heartbeat.interval.ms` | `3000` | Default (session.timeout / 15) is correct |
| `max.poll.interval.ms` | `300000` | Default 5 min is generous enough |
| `fetch.max.bytes` | `52428800` (50 MiB) | Default is adequate |

### Development/Test (`CONSUMER_DEVTEST`)

Fast iteration, low memory footprint. For local dev and CI.

| Setting | Value | librdkafka Default | Justification |
|---------|-------|--------------------|---------------|
| `partition.assignment.strategy` | `cooperative-sticky` | `range,roundrobin` | Consistent with production |
| `queued.min.messages` | `1000` | `100000` | Lower memory on dev machines |
| `enable.auto.commit` | `false` | `true` | Consistent with production |
| `reconnect.backoff.ms` | `10` | `100` | Fast reconnect for quick iteration |
| `reconnect.backoff.max.ms` | `100` | `10000` | Cap quickly — don't wait 10s in dev |
| `log.connection.close` | `true` | `false` | Debug-friendly connection logging |
| `statistics.interval.ms` | `1000` | `0` | Metrics available even in dev |

### Low-Latency (`CONSUMER_LOW_LATENCY`)

Minimal fetch delay for latency-sensitive pipelines.

| Setting | Value | librdkafka Default | Justification |
|---------|-------|--------------------|---------------|
| `partition.assignment.strategy` | `cooperative-sticky` | `range,roundrobin` | Consistent across environments |
| `fetch.wait.max.ms` | `10` | `500` | Return from fetch after 10 ms regardless of data available |
| `queued.min.messages` | `1000` | `100000` | Smaller pre-fetch queue — process sooner |
| `enable.auto.commit` | `false` | `true` | DFE manages commits |
| `reconnect.backoff.ms` | `10` | `100` | Fast reconnect |
| `reconnect.backoff.max.ms` | `100` | `10000` | Cap quickly |
| `statistics.interval.ms` | `1000` | `0` | Enable metrics |

## Producer Profiles

### Production (`PRODUCER_PRODUCTION`)

High-throughput producer. Default for all DFE services writing to Kafka.

| Setting | Value | librdkafka Default | Justification |
|---------|-------|--------------------|---------------|
| `linger.ms` | `100` | `5` | Accumulate larger batches — 100 ms latency trade-off for significantly higher throughput. Most DFE pipelines are throughput-optimised. |
| `compression.type` | `zstd` | `none` | Best compression ratio with acceptable CPU cost. Reduces network I/O and broker storage. |
| `socket.nagle.disable` | `true` | `false` | Disable TCP Nagle's algorithm. Kafka already batches at the application level (`linger.ms`), so Nagle just adds latency. |
| `statistics.interval.ms` | `1000` | `0` | Enable librdkafka metrics for Prometheus. |

**Settings intentionally left at default:**

| Setting | Default | Why Not Changed |
|---------|---------|-----------------|
| `acks` | `all` (-1) | Default is already the safest — all ISR replicas must acknowledge. |
| `batch.size` | `1000000` (1 MiB) | Adequate for most workloads |
| `batch.num.messages` | `10000` | Default is reasonable |
| `queue.buffering.max.kbytes` | `1048576` (1 GiB) | Default is fine for most services. transform-vector overrides to 256 MiB due to lower pod memory. |
| `message.max.bytes` | `1000000` (1 MiB) | DFE messages are typically <100 KB. Services needing larger messages override individually. |
| `retries` | `2147483647` (max) | Default retry-forever with idempotence is correct. |
| `delivery.timeout.ms` | `300000` (5 min) | Generous timeout for transient broker issues |

### Exactly-Once (`PRODUCER_EXACTLY_ONCE`)

Idempotent producer with ordering guarantees.

| Setting | Value | librdkafka Default | Justification |
|---------|-------|--------------------|---------------|
| `enable.idempotence` | `true` | `false` | Exactly-once semantics within a partition. Broker deduplicates by producer ID + sequence number. |
| `acks` | `all` | `all` (-1) | Already default but stated explicitly — invariant for EOS. |
| `max.in.flight.requests.per.connection` | `5` | `1000000` | Maximum value that preserves ordering with idempotent producer (librdkafka requirement). |
| `linger.ms` | `20` | `5` | Moderate batching — lower than production to reduce latency for EOS workloads. |
| `compression.type` | `zstd` | `none` | Consistent with production |
| `socket.nagle.disable` | `true` | `false` | Consistent with production |
| `statistics.interval.ms` | `1000` | `0` | Enable metrics |

### Low-Latency (`PRODUCER_LOW_LATENCY`)

Minimal delay, leader-ack only. Trades durability for speed.

| Setting | Value | librdkafka Default | Justification |
|---------|-------|--------------------|---------------|
| `acks` | `1` | `all` (-1) | Leader acknowledgement only — skip ISR replication wait. Risk: data loss if leader crashes before replication. |
| `linger.ms` | `0` | `5` | Send immediately, no batching delay. |
| `compression.type` | `lz4` | `none` | LZ4 is the fastest compression codec — adds minimal CPU for significant bandwidth reduction. |
| `socket.nagle.disable` | `true` | `false` | No TCP coalescing |
| `statistics.interval.ms` | `1000` | `0` | Enable metrics |

### Development/Test (`PRODUCER_DEVTEST`)

Fast acks, no compression overhead. For local dev and CI.

| Setting | Value | librdkafka Default | Justification |
|---------|-------|--------------------|---------------|
| `acks` | `1` | `all` (-1) | Faster for dev — don't wait for full ISR |
| `socket.nagle.disable` | `true` | `false` | Consistent across profiles |
| `statistics.interval.ms` | `1000` | `0` | Enable metrics even in dev |

## Service-Specific Overrides

Each DFE service may add overrides on top of the shared baseline. These are
hardcoded in the service and applied at layer 2 (below user config, above
baseline).

### dfe-transform-vector

| Setting | Value | Reason |
|---------|-------|--------|
| `queue.buffering.max.kbytes` | `262144` (256 MiB) | Pods typically have 2-4 GiB memory. Default 1 GiB producer queue is too large. |

### dfe-loader

No service-specific overrides currently. Uses production baseline as-is.

### dfe-receiver

No service-specific overrides currently. Uses production baseline as-is.

## Central Config File

A single `librdkafka.yaml` in the git-managed config repo (`dfe-devex`)
is the management point for the platform-wide baseline. All DFE services
read this file at startup and fall back to scalo coded-in constants if
it isn't present. It is layer 1, so service-specific constants (layer 2)
still override any key it sets.

**Location:** `dfe-devex/shared/librdkafka.yaml`

**Discovery:** Via `DFE_CONFIG_DIR` environment variable. The service reads
`$DFE_CONFIG_DIR/shared/librdkafka.yaml`. This follows the same pattern
dfe-engine uses for `services/`, `deployment/`, etc.

**Activation:**

```bash
cd /path/to/dfe-devex
cp shared/librdkafka.yaml.example shared/librdkafka.yaml
# Edit as needed, then commit
```

**Caching:** Profiles are loaded once on first access via `OnceLock` and
cached for the process lifetime. Changes require a service restart (or
future hot-reload support).

**Format:**

```yaml
consumer:
  production:
    partition.assignment.strategy: cooperative-sticky
    fetch.min.bytes: "1048576"
    # ... add/remove/change any librdkafka setting

  devtest:
    # ...

  low_latency:
    # ...

producer:
  production:
    linger.ms: "100"
    compression.type: zstd
    # ...

  exactly_once:
    # ...

  low_latency:
    # ...

  devtest:
    # ...
```

Profile names match the scalo constant names (`production`, `devtest`,
`low_latency`, `exactly_once`). If a profile exists in the file, it
completely replaces the scalo constant for that profile. If a profile
is missing from the file, the scalo constant is used.

## User Config Overrides

Any setting can be overridden per-instance via the service's config YAML:

```yaml
source:
  librdkafka_options:
    fetch.min.bytes: "2097152"    # 2 MiB instead of 1 MiB
    custom.vendor.setting: "value"

sink:
  librdkafka_options:
    compression.type: "lz4"       # LZ4 instead of zstd
    queue.buffering.max.kbytes: "524288"  # 512 MiB
```

These are layer 3 (highest priority) and override everything below, including
the service-specific constants.

## Source Code References

- **Shared baseline constants:** `scalo/src/kafka_config.rs`
- **Central config loader:** `src/config/kafka_defaults.rs` (YAML loading, `OnceLock` cache, fallback)
- **Profile accessors:** `kafka_defaults::consumer_profile()`, `kafka_defaults::producer_profile()`
- **3-layer merge:** `kafka_defaults::merge_layers()` (base + service overrides + user overrides)
- **YAML generator:** `src/config/generate.rs` (`build_source_librdkafka`, `build_sink_librdkafka`)
- **Config schema:** `src/config/loader.rs` (`SourceConfig.librdkafka_options`, `SinkConfig.librdkafka_options`)
- **Central config file:** `dfe-devex/shared/librdkafka.yaml.example`

## Reference

- [librdkafka CONFIGURATION.md](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) — full property reference with defaults
- [KIP-429](https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol) — cooperative-sticky rebalance protocol
