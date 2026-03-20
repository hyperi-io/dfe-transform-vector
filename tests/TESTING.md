# Testing

## Test Modes

Tests support two backends, controlled by `TEST_MODE` in `.env`:

### Remote (default)

Uses the devex cluster Kafka endpoint from `.env`. Tests skip if the endpoint
is unreachable.

```bash
TEST_MODE=remote cargo nextest run
```

### Docker-local

Uses `dfe-docker` infra profile (Kafka on `localhost:19092`, PLAINTEXT, no auth).

```bash
# Start infrastructure (once, stays running)
cd /projects/dfe-docker
docker compose --profile infra up -d

# Run tests
TEST_MODE=docker cargo nextest run

# Tear down (when done)
docker compose --profile infra down
```

## Test Categories

### Unit tests (always run, no infrastructure)

```bash
cargo nextest run --lib
```

### Integration tests (config assembly, wiring, fixtures)

```bash
cargo nextest run --test integration_config --test integration_fixtures --test integration_lifecycle
```

### Vector validate tests (require `vector` binary on PATH)

```bash
cargo nextest run --test integration_vector_validate --run-ignored all
```

### E2E tests (require Kafka + `vector` binary on PATH)

```bash
cargo nextest run --test e2e_kafka
```

Skips automatically if Kafka is unreachable or Vector is not installed.

## Infrastructure Endpoints

| Service | Docker-local | Remote (devex) |
|---------|-------------|----------------|
| Kafka | `localhost:19092` (PLAINTEXT) | `kafka.devex.hyperi.io:32089` (SASL_SSL) |
