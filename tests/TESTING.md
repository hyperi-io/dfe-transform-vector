# Testing

## Backends

There is no mode switch. `KafkaFixture::acquire` resolves a broker on its own:

1. **A live cluster**, if `KAFKA_BROKERS` (and any SASL settings) in `.env`
   authenticate against it. Faster, and a real multi-broker cluster.
2. **Testcontainers**, otherwise -- an ephemeral single-node Apache Kafka the
   fixture starts and stops.
3. **Skip**, when neither is available. In CI that is an assertion failure
   instead: a test that skips when its environment disappears is not a gate.

`KafkaFixture::hermetic` skips step 1 outright and always starts its own
container. Anything using the real DFE topic names takes that path, because
`filebeat_land` on a shared broker is somebody else's data.

Every container carries the repo, the suite, the service and the owning pid as
labels, and a name derived from the test. Sweep leftovers with:

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

Opt-in (`#[ignore]`) cases are the ones that need a Vector binary to prove
something about Vector itself: `vector_validate` runs the real validator over
each assembled config, and `e2e::kafka` drives a bare Vector process against a
broker.

## The WS21 acceptance case

`e2e::filebeat_kafka` runs by default -- it owns its broker, so it is safe to.
It seeds the elastic/integrations filebeat corpus onto `filebeat_land`, runs the
shipped binary over it, and grades `filebeat_load` against elastic's golden
events.

The corpus, the bundled pipeline and the list of documented divergences belong
to dfe-transform-vrl, so the case needs that repo checked out beside this one,
or `DFE_TRANSFORM_VRL_DIR` pointing at it. Without one it prints a skip.
