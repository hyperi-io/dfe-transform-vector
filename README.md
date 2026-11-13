# dfe-transform-vector

Runs a Vector pipeline as a supervised transform stage in a DFE deployment.

[![Build Status](https://github.com/hyperi-io/dfe-transform-vector/actions/workflows/ci.yml/badge.svg)](https://github.com/hyperi-io/dfe-transform-vector/actions)
[![License](https://img.shields.io/badge/license-BUSL--1.1-blue)](LICENSE)

## What it does

- Vector does the transform, and the supervisor supplies the rest a stage needs: config, credentials, health, metrics and restarts. Reach for it when you need one of Vector's own transforms beyond `remap`. For VRL alone, [dfe-transform-vrl](https://github.com/hyperi-io/dfe-transform-vrl) is smaller and in-process.
- One small YAML (pipeline name, brokers, topics, SASL and TLS) generates Vector's source, sink and observability config, so you write only the `transforms:` and `enrichment_tables:` blocks.
- Kafka in and out (`bus`), or a direct gRPC push with no broker (`direct`). The transform you author is the same on both.
- A crashed Vector is restarted with backoff from 1 s to 60 s, and readiness follows it.
- With reload enabled, a transform change is validated, sent to Vector with a SIGHUP, and read back from Vector's own counters as applied, refused (the running config is put back) or unconfirmed. One that fails validation is logged and never sent.
- Vector's `vector_*` metrics are merged into the supervisor's, so one endpoint and one OTLP push carry both.

## Quick start

```bash
cargo build --release
./target/release/dfe-transform-vector run --config config.example.yaml
```

[config.example.yaml](config.example.yaml) is the annotated reference. [templates/bus.yaml](templates/bus.yaml) and [templates/direct.yaml](templates/direct.yaml) are the whole Vector topology for each transport, commented per field and runnable as they stand: copy one, replace the `dfe_transform` step, and you have a working pipeline. In a deployment the supervisor generates `sources:` and `sinks:`, and refuses any other top-level key in a transform file.

| | `bus` | `direct` |
|---|---|---|
| In | Kafka `<source>_land` | a scalo Push listener on `source.listen` (port 6000 by default) |
| Out | Kafka `<source>_load` | a scalo Push client to `sink.endpoint` |
| Needs a broker | yes | no |

One port, 9090 by default, carries the ops surface: `/livez`, `/readyz` (200 only while Vector is up and its sink is not stalled) and `/metrics`. Vector's own exporter defaults to `127.0.0.1:9598`, and `metrics.vector_metrics_address` stays on loopback.

## Configuration

Settings load through a cascade: CLI, then `DFE_TRANSFORM_VECTOR_*` environment variables, then the working directory's `.env`, then the config file, then defaults. The transport dials take a single underscore (`DFE_TRANSFORM_VECTOR_SOURCE_TRANSPORT`, `_SOURCE_LISTEN`, `_SINK_ENDPOINT`, the SASL pairs), and a key inside a section takes two (`DFE_TRANSFORM_VECTOR_SOURCE__BROKERS`, `_SINK__TOPIC`, `_TRANSFORMS__DIR`). [docs/config-schema.yaml](docs/config-schema.yaml) lists every key with its type and default.

A transform file change hot-reloads when reload is enabled, after it validates. A change to `source.*`, `sink.*`, `bridge.*`, `pipeline.name`, `vector.*`, `metrics.*` or `logging.*` takes a pod restart, and the watcher logs it as such. The reload path, the readiness rules, the metrics merge and the Kafka tuning are in [docs/internals.md](docs/internals.md) and [docs/librdkafka.md](docs/librdkafka.md).

## Documentation

- [docs/architecture.md](docs/architecture.md): the problem, the ownership boundary and the invariants.
- [docs/internals.md](docs/internals.md): the config engine, the subprocess manager, health and metrics, the Vector version pin, the chart.
- [docs/migration.md](docs/migration.md): moving from the official Vector chart.
- [docs/librdkafka.md](docs/librdkafka.md): Kafka client tuning.
- [tests/TESTING.md](tests/TESTING.md): the test tiers, and the e2e cases that need Docker and a Vector binary.

The suite's published docs are at <https://dfe.docs.hyperi.io>.

## Help and contributing

How to work in this repo, the commands that prove a change and what tends to bite are in [CONTRIBUTING.md](CONTRIBUTING.md). A question that is not a bug goes in a new issue.

Open bugs: [this repo](https://github.com/hyperi-io/dfe-transform-vector/issues?q=is%3Aopen+is%3Aissue+type%3ABug+-label%3Abacklog+-label%3Anext-minor) | [whole DFE suite](https://github.com/search?type=issues&q=is%3Aopen+is%3Aissue+type%3ABug+-label%3Abacklog+-label%3Anext-minor+repo%3Ahyperi-io%2Fdfe-infra+repo%3Ahyperi-io%2Fdfe-docker+repo%3Ahyperi-io%2Fdfe-deploy+repo%3Ahyperi-io%2Fdfe-engine+repo%3Ahyperi-io%2Fdfe-loader+repo%3Ahyperi-io%2Fdfe-receiver+repo%3Ahyperi-io%2Fdfe-fetcher+repo%3Ahyperi-io%2Fdfe-archiver+repo%3Ahyperi-io%2Fdfe-transform-vrl+repo%3Ahyperi-io%2Fdfe-transform-vector+repo%3Ahyperi-io%2Fdfe-transform-elastic+repo%3Ahyperi-io%2Fdfe-ui+repo%3Ahyperi-io%2Fdfe-hyperdx+repo%3Ahyperi-io%2Fscalo-rs+repo%3Ahyperi-io%2Fscalo-py+repo%3Ahyperi-io%2Flogreducer+repo%3Ahyperi-io%2Fdfe-schemas+repo%3Ahyperi-io%2Fvector-vrl+repo%3Ahyperi-io%2Fculvert+repo%3Ahyperi-io%2Fdfe-clickhouse-rs+repo%3Ahyperi-io%2Ffactbook-rs)

## License

BUSL-1.1, see [LICENSE](LICENSE).
