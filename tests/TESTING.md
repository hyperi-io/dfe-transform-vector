# Testing

Which tier runs what, and how the tests find a broker and a Vector binary.

## Tiers

- Core runs on every push and PR: `cargo nextest run --all-features`. Unit, integration, smoke, and the e2e cases that are not `#[ignore]`.
- Full runs nightly and before every release (`test.full.required_for_release` in `.hyperi-ci.yaml`): core plus every `#[ignore]` case, `cargo nextest run --all-features --run-ignored all --ignore-default-filter`. Those are the `vector_validate` cases, the opt-in `e2e::kafka` pipelines and one timing measurement in `src/metrics/scrape.rs`.

```bash
# Unit tests -- no infrastructure
cargo nextest run --lib

# Config assembly, wiring, fixtures, lifecycle, metrics
cargo nextest run --test integration

# CLI surface
cargo nextest run --test smoke

# The e2e tests -- need Docker or a live cluster, plus a Vector binary
cargo nextest run --test e2e

# Everything, opt-in cases included
cargo nextest run --run-ignored all
```

Opt-in (`#[ignore]`) cases are the full tier: `vector_validate` runs Vector's validator over each buffer, SASL and TLS variant of an assembled config, and two `e2e::kafka` pipelines drive a bare Vector process against a broker.

## Brokers and the Vector binary

There is no mode switch. `KafkaFixture::acquire` resolves a broker by itself:

1. A live cluster, if `KAFKA_BROKERS` (and any SASL settings) in `.env` authenticate against it. Faster, and a multi-broker cluster.
2. Otherwise the run's shared broker: one single-node Apache Kafka, scalo's `KAFKA_IMAGE`, that scalo's `shared_service` starts for the whole run and every test process joins. The first test that needs it starts it on the local Docker daemon, locally and in CI alike, and a watcher removes it when the run's last test process exits.
3. A skip, when neither is available. In CI that is an assertion failure instead: a test that skips when its environment disappears is not a gate.

`KafkaFixture::hermetic` skips step 1 outright and takes the shared broker.
Anything using the production DFE topic names takes that path, because
`filebeat_land` on a shared live broker is somebody else's data.
`KafkaFixture::scram` starts a SCRAM-only broker the test holds alone.

scalo names every container under the suite prefix, labels it with the repo,
the suite, the service and the owner, and removes it when the run ends. Sweep
leftovers from a killed run with:

```bash
docker rm -f $(docker ps -aq --filter label=io.hyperi.test.suite=dfe-transform-vector-integration)
```

Tests that spawn Vector resolve it through `scripts/fetch-vector.sh`, which downloads the pinned build once into `.tmp/`, and fall back to `vector` on `PATH`. Without either they skip locally, and the cases that run by default fail under CI.

## The filebeat acceptance case

`e2e::filebeat_kafka` runs in core, on the shared broker, which no live cluster's data shares.
It seeds the synthetic filebeat cases onto `filebeat_land`, runs the shipped
binary over them, and grades `filebeat_load` against the fields each case names.

The cases (`tests/fixtures/filebeat/cases.yaml`) and the bundled pipeline belong
to dfe-transform-vrl, so the case needs that repo checked out beside this one,
or `DFE_TRANSFORM_VRL_DIR` pointing at it. Without one it skips locally and fails under CI, which checks the repo out beside this one.
