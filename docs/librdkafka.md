# librdkafka settings

On the bus transport Vector owns the Kafka clients, and the supervisor writes their librdkafka settings into the generated source and sink as `librdkafka_options`. This page covers where those settings come from and how to change them.

## Layers

Three layers merge into `librdkafka_options`, highest last:

| Layer | Source | Who manages it |
|-------|--------|----------------|
| 1. Baseline profile | `consumer.production` or `producer.production` in the central file when it defines that profile, else scalo's `CONSUMER_PRODUCTION` or `PRODUCER_PRODUCTION` | Platform operator, or scalo |
| 2. Service overrides | Hardcoded in `src/config/generate.rs` | This repo |
| 3. Per-instance options | `source.librdkafka_options`, `sink.librdkafka_options` | Whoever writes the config |

Layer 1 is one layer, not two. A profile in the central file REPLACES the matching scalo constant set outright, so a central `producer.production` that names only `linger.ms` ships only `linger.ms`, and every other baseline setting reverts to what Vector writes itself, below, or else librdkafka's default. Layer 2 sits on whichever source won, so a central setting cannot undo a service override: `queue.buffering.max.kbytes` set centrally takes no effect here.

Vector applies `librdkafka_options` over the settings it writes itself from the source and sink fields, so every layer above outranks those:

| librdkafka key | Written by Vector from |
|---|---|
| `session.timeout.ms`, `auto.commit.interval.ms`, `auto.offset.reset` | `source.session_timeout_ms` (30000), `source.commit_interval_ms` (5000), `source.auto_offset_reset` (`largest`) |
| `enable.auto.commit`, `enable.auto.offset.store`, `statistics.interval.ms` | Vector's kafka source code, as `true`, `false` and `1000` |
| `message.timeout.ms`, `socket.timeout.ms` | `sink.message_timeout_ms` (0, no limit), `sink.socket_timeout_ms` (60000) |
| `queue.buffering.max.ms`, `batch.num.messages`, `batch.size` | `sink.batch.timeout_secs`, `.max_events` and, where set, `.max_bytes` |

`linger.ms` is librdkafka's other name for `queue.buffering.max.ms`, and a client built from both takes whichever its hash map applied last. Vector always writes `sink.batch.timeout_secs` as `queue.buffering.max.ms`, so the supervisor moves a batching delay that a layer names into `batch.timeout_secs` and leaves both spellings out of `librdkafka_options`. A layer that names either spelling replaces the other spelling from every layer below it, and within one layer the spelling that sorts last wins. A layer therefore outranks `sink.batch.timeout_secs`. The production profile names `linger.ms`, so on the bus transport `timeout_secs` has no effect until a central profile drops that key, and the supervisor logs a warning when `timeout_secs` is not 1 and a layer overrides it.

A few keys are derived from the big dials and written after the merge, so `librdkafka_options` cannot reach them:

| Key | Owned by | Written when |
|---|---|---|
| `compression.type`, `compression.codec` | `sink.compression` | always, on the sink, as Vector's `compression` field |
| `security.protocol` | `{source,sink}.sasl.enabled` + `.tls.enabled` | either is on |
| `sasl.mechanism` | `{source,sink}.sasl.mechanism` | SASL is on |
| `enable.ssl.certificate.verification` | `{source,sink}.tls.skip_verify` | TLS is on and verification is skipped |

`config-check` rejects a config that sets one of these in `librdkafka_options`, and names the big dial that owns the key. With SASL off nothing derives `sasl.mechanism`, so setting it by hand there is allowed. Any other key can be set per instance, bar two checks. `config-check` rejects `enable.auto.offset.store: true` in any layer of the source consumer's options, the central profile included, because librdkafka would then store offsets at fetch, before the sink acknowledges. The client config built from each endpoint's brokers, SASL, TLS and `librdkafka_options` must pass scalo's Kafka security floor.

```yaml
source:
  librdkafka_options:
    fetch.min.bytes: "2097152"    # 2 MiB instead of 1 MiB

sink:
  librdkafka_options:
    queue.buffering.max.kbytes: "524288"  # 512 MiB
```

For example, for a producer with no central file, `sink.compression: lz4` and `sink.librdkafka_options: {linger.ms: "20"}`, the supervisor writes `batch.timeout_secs: 0.02` and `compression: lz4` on the sink and, in `librdkafka_options`, `queue.buffering.max.kbytes=262144`, `enable.idempotence=true`, `socket.nagle.disable=true`, `statistics.interval.ms=1000` and `message.max.bytes=16777216`, the last because no layer set it.

Add a central `producer.production` naming only `linger.ms: "50"` and `socket.nagle.disable` and `statistics.interval.ms` drop out of `librdkafka_options`, because the central profile replaced the constants rather than merging over them. Vector still writes `statistics.interval.ms=1000` itself. The delay stays 20 ms: the per-instance option outranks the central file. Drop that option and it becomes 50 ms.

`dfe-transform-vector --config <file> assemble <dir>` writes `90_sink.yaml` and `00_source.yaml`, so you can read the result for any config.

## The production profiles

This app reads `CONSUMER_PRODUCTION` for the source and `PRODUCER_PRODUCTION` for the sink, and no other profile. The librdkafka defaults below are those of the librdkafka release Vector bundles.

| Consumer setting | Value | librdkafka default | Justification |
|---------|-------|--------------------|---------------|
| `partition.assignment.strategy` | `cooperative-sticky` | `range,roundrobin` | KIP-429: incremental rebalances avoid stop-the-world pauses. Partitions stay assigned during rebalance -- only moving partitions are revoked. |
| `fetch.min.bytes` | `1048576` (1 MiB) | `1` | Batch fetches for throughput. Broker waits until 1 MiB of data is available before responding, reducing fetch round-trips. |
| `fetch.wait.max.ms` | `100` | `500` | Upper bound on fetch latency when the `fetch.min.bytes` threshold isn't met. Prevents 500 ms stalls on low-volume topics. |
| `queued.min.messages` | `20000` | `100000` | Pre-fetch queue depth, large enough for batching and small enough to bound memory and rebalance lag. |
| `enable.auto.commit` | `false` | `true` | Suits a service that commits by hand after processing. This app overrides it back to `true` -- see Service overrides. |
| `statistics.interval.ms` | `1000` | `0` (disabled) | librdkafka internal metrics at 1-second granularity. |

| Producer setting | Value | librdkafka default | Justification |
|---------|-------|--------------------|---------------|
| `linger.ms` | `100` | `5` | Larger batches -- 100 ms of latency for higher throughput. This is the delay the sink uses, in place of `sink.batch.timeout_secs`. See Layers. |
| `compression.type` | `zstd` | `none` | Best compression ratio with acceptable CPU cost. `sink.compression` replaces it on every sink. |
| `socket.nagle.disable` | `true` | `true` | librdkafka's default as well. The profile states it. |
| `statistics.interval.ms` | `1000` | `0` | librdkafka metrics. |

Left at librdkafka's default, unless a layer or a Vector field above writes them:

| Setting | Default |
|---------|---------|
| `queued.max.messages.kbytes` | `65536` (64 MiB) |
| `socket.receive.buffer.bytes` | `0` (OS default) |
| `heartbeat.interval.ms` | `3000` |
| `max.poll.interval.ms` | `300000` |
| `fetch.max.bytes` | `52428800` (50 MiB) |
| `acks` | `all` (-1), which idempotence needs anyway |
| `retries` | `2147483647` |

## Service overrides

| Setting | Side | Value | Reason |
|---------|------|-------|--------|
| `queue.buffering.max.kbytes` | producer | `262144` (256 MiB) | librdkafka's default 1 GiB producer queue is too large for a transform-vector pod. |
| `enable.idempotence` | producer | `true` | The sink retries without limit, and a retry on a non-idempotent producer can reorder and duplicate records within a partition. |
| `enable.auto.commit` | consumer | `true` | Vector's kafka source stores offsets on delivery and leaves the periodic flush to librdkafka's commit timer, which the baseline's `false` never arms. Vector still commits on rebalance and clean shutdown, so the baseline left reported lag stuck between those points rather than losing offsets. |
| `message.max.bytes` | producer | `16777216` (16 MiB), scalo's `MESSAGE_MAX_BYTES` | The record ceiling every other stage's producer and every topic scalo creates accept, where librdkafka's default of `1000000` rejects a larger record. Written only where neither layer 1 nor `sink.librdkafka_options` sets the key. The size cap in front of the sink reads the same value. |

## Central config file

The central file is `$DFE_CONFIG_DIR/shared/librdkafka.yaml`, read only when `DFE_CONFIG_DIR` is set. This app reads it if it is there and falls back to scalo's constants if it is not, and no other DFE app reads it. It is layer 1, so the service overrides still win over any key it sets.

The profiles load once, on first access, and are cached for the process lifetime, so a change to the file needs a restart: a hot reload keeps the cached profiles.

```yaml
consumer:
  production:
    partition.assignment.strategy: cooperative-sticky
    fetch.min.bytes: "1048576"
    # ... add/remove/change any librdkafka setting

producer:
  production:
    linger.ms: "100"
    compression.type: zstd
    # ...
```

Only the `production` profile of each side is read. A profile in the file replaces the scalo constant set of that name, and a missing one leaves the constants in force. So does an empty one, or one holding no string, number or boolean value: it is skipped.

The code and the references behind this page:

- Profile constants: [scalo-rs src/kafka_config.rs](https://github.com/hyperi-io/scalo-rs/blob/main/src/kafka_config.rs)
- Central file loader: `src/config/kafka_defaults.rs` (`consumer_profile()`, `producer_profile()`, the `OnceLock` cache, `merge_layers()`)
- Layer merge and derived keys: `src/config/generate.rs` (`build_source_librdkafka`, `build_sink_librdkafka`, `SERVICE_CONSUMER_OVERRIDES`, `SERVICE_PRODUCER_OVERRIDES`)
- Config fields: `src/config/loader.rs` (`SourceConfig.librdkafka_options`, `SinkConfig.librdkafka_options`)
- [librdkafka CONFIGURATION.md](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) -- every property, with its default
- [KIP-429](https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol) -- the cooperative-sticky rebalance protocol
