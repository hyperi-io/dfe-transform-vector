# dfe-transform-vector

Rust wrapper that manages Vector.dev as a subprocess for Kafka-to-Kafka
transform pipelines in the HyperI DFE (Data Fusion Engine) platform. The wrapper
is built on the [scalo](https://github.com/hyperi-io/scalo-rs) data-plane runtime
(config cascade, logging, metrics, health probes, CLI, deployment contract).

## Architecture

```mermaid
flowchart TB
    SRC[("Kafka source topic(s)")]
    SINK[("Kafka sink topic")]
    subgraph W["dfe-transform-vector (Rust, PID 1)"]
        CE["Config engine<br/>big-dial YAML -> Vector config dir"]
        PM["Process manager<br/>spawn / signal / crash recovery"]
        VEC["Vector.dev<br/>subprocess (--config-dir)"]
        OPS["Health :9000 + Metrics :9090<br/>wrapper + proxied Vector"]
        CE --> VEC
        PM --> VEC
    end
    SRC --> VEC --> SINK
```

## Features

- **Big-dial config**: Simple YAML (pipeline name, brokers, topics, SASL/TLS)
  generates full Vector-native source, sink, and observability YAML
- **DAG auto-wiring**: User-supplied transform YAMLs are loaded, validated, and
  wired between `dfe_source` and `dfe_sink` automatically
- **Crash recovery**: Exponential backoff restarts (1s-60s), K8s-aware readiness
- **Hot-reload**: Poll-based file watcher detects transform changes, validates,
  then SIGHUPs Vector (source/sink changes require pod restart)
- **Version pinning**: Config-level Vector version check (strict/warn/disabled)
- **Production Kafka tuning**: librdkafka defaults baked in with 4-layer cascade
- **Metrics**: Wrapper metrics + proxied Vector internal metrics on single endpoint
- **Helm chart**: Generated Deployment-based chart with KEDA, HPA, ConfigMap

## Quick Start

```bash
# Build
cargo build --release

# Run with config file
./target/release/dfe-transform-vector run --config config.example.yaml

# Generate deployment artifacts
./target/release/dfe-transform-vector emit-dockerfile
./target/release/dfe-transform-vector emit-chart
./target/release/dfe-transform-vector emit-compose
```

## Configuration

See [config.example.yaml](config.example.yaml) for a full annotated configuration.

Key environment variable overrides (prefix `DFE_TRANSFORM_`):

| Variable | Description |
|----------|-------------|
| `DFE_TRANSFORM_SOURCE__BROKERS` | Kafka source broker addresses |
| `DFE_TRANSFORM_SOURCE__TOPICS` | Source topic list |
| `DFE_TRANSFORM_SOURCE__GROUP_ID` | Consumer group ID |
| `DFE_TRANSFORM_SINK__BROKERS` | Kafka sink broker addresses |
| `DFE_TRANSFORM_SINK__TOPIC` | Sink output topic |
| `DFE_TRANSFORM_PIPELINE__NAME` | Pipeline name |
| `DFE_TRANSFORM_TRANSFORMS__DIR` | Transform YAML directory |
| `DFE_TRANSFORM_LOGGING__LEVEL` | Log level (trace/debug/info/warn/error) |

## Hot-Reload

**Hot-reloaded (takes effect via SIGHUP to Vector):**
- Transform YAML file contents (modified/added/removed in watched directory)
- `transforms.dir` / `transforms.files` path changes

**Requires pod restart:**
- `source.*` -- Kafka consumer config baked into Vector at startup
- `sink.*` -- Kafka producer config baked into Vector at startup
- `pipeline.name` -- consumer group_id and metrics labels set at startup
- `vector.*` -- binary path, data_dir, API address set at spawn
- `health.address` / `metrics.*` -- HTTP servers bound at startup
- `logging.*` -- tracing subscriber configured at startup

## Endpoints

| Endpoint | Port | Purpose |
|----------|------|---------|
| `GET /health/live` | 9000 | K8s liveness probe |
| `GET /health/ready` | 9000 | K8s readiness probe (200 only when Vector is healthy) |
| `GET /metrics` | 9090 | Prometheus scrape (wrapper + proxied Vector metrics) |

## Development

```bash
# Run checks (fmt + clippy + test + deny)
make check

# Run tests
cargo nextest run

# Run e2e test (requires Docker + Vector binary on PATH)
cargo nextest run --test e2e_kafka --run-ignored all

# Run Vector validate tests (requires Vector binary on PATH)
cargo nextest run --test integration_vector_validate --run-ignored all
```

## Documentation

- [docs/DESIGN.md](docs/DESIGN.md) -- Full architecture and design
- [docs/MIGRATION.md](docs/MIGRATION.md) -- Migration from official Vector chart
- [docs/LIBRDKAFKA.md](docs/LIBRDKAFKA.md) -- Kafka tuning reference
- [RESEARCH.md](RESEARCH.md) -- Research findings and option analysis

## License

This project is licensed under the Business Source License 1.1
(BUSL-1.1). See [LICENSE](LICENSE) for details.

Copyright (c) 2026 HYPERI PTY LIMITED

For commercial licensing options, see [COMMERCIAL.md](COMMERCIAL.md).
