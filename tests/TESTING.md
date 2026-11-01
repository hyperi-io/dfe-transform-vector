# Testing

The house rules are hyperi-ai's testing standard, `standards/universal/testing.md`.

## Tiers

- **Core** runs on every push and PR: `cargo nextest run --all-features`. Unit, integration, smoke, and the e2e cases that are not `#[ignore]`.
- **Full** runs nightly and before every release (`test.full.required_for_release` in `.hyperi-ci.yaml`): core plus every `#[ignore]` case, `cargo nextest run --all-features --run-ignored all`. Those are the `vector_validate` cases and the opt-in `e2e::kafka` pipelines.

## Backends

There is no mode switch. `KafkaFixture::acquire` resolves a broker on its own:

1. **A live cluster**, if `KAFKA_BROKERS` (and any SASL settings) in `.env`
   authenticate against it. Faster, and a real multi-broker cluster.
2. **The run's shared broker**, otherwise -- one single-node Apache Kafka that
   scalo's `shared_service` starts for the whole run and every test process
   joins. The image is scalo's `KAFKA_IMAGE`,
   `apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`
   as of scalo 2.14.25. The first test that needs it starts it on the local
   Docker daemon, locally and in CI alike, and a watcher removes it when the
   run's last test process exits.
3. **Skip**, when neither is available. In CI that is an assertion failure
   instead: a test that skips when its environment disappears is not a gate.

`KafkaFixture::hermetic` skips step 1 outright and takes the shared broker.
Anything using the real DFE topic names takes that path, because
`filebeat_land` on a shared live broker is somebody else's data.
`KafkaFixture::scram` starts a SCRAM-only broker the test holds alone.

scalo names every container under the suite prefix, labels it with the repo,
the suite, the service and the owner, and removes it when the run ends. Sweep
leftovers from a killed run with:

```bash
docker rm -f $(docker ps -aq --filter label=io.hyperi.test.suite=dfe-transform-vector-integration)
```

## The Vector binary

Tests that spawn Vector resolve it through `scripts/fetch-vector.sh`, which
downloads the pinned build once into `.tmp/`, and fall back to `vector` on
`PATH`. Without either they skip.

## Suites

```bash
# Unit tests -- no infrastructure
cargo nextest run --lib

# Config assembly, wiring, fixtures, lifecycle, metrics
cargo nextest run --test integration

# CLI surface
cargo nextest run --test smoke

# End to end -- needs Docker or a live cluster, plus a Vector binary
cargo nextest run --test e2e

# Everything, opt-in cases included
cargo nextest run --run-ignored all
```

Opt-in (`#[ignore]`) cases are the full tier: `vector_validate` runs the real
validator over each assembled config, and two `e2e::kafka` pipelines drive a
bare Vector process against a broker.

## The WS21 acceptance case

`e2e::filebeat_kafka` runs in core, on the shared broker, which no live cluster's data shares.
It seeds the elastic/integrations filebeat corpus onto `filebeat_land`, runs the
shipped binary over it, and grades `filebeat_load` against elastic's golden
events.

The corpus, the bundled pipeline and the list of documented divergences belong
to dfe-transform-vrl, so the case needs that repo checked out beside this one,
or `DFE_TRANSFORM_VRL_DIR` pointing at it. Without one it prints a skip.
