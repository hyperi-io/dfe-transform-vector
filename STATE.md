# Project Context

**Project:** dfe-transform-vector
**Purpose:** Rust wrapper that manages Vector.dev as a subprocess for Kafka→transform→Kafka pipelines, making Vector a first-class DFE platform citizen alongside dfe-loader and dfe-receiver.

> **Note:** The `ai/` submodule provides standards and configuration - not code
> to import. Your project never imports or links to it.

---

## DO NOT ADD TO THIS FILE

**The following belong elsewhere:**

| Data | Correct Location |
|------|------------------|
| Version numbers | `VERSION` file, `git describe --tags` |
| Tasks/Progress | `TODO.md` |
| Session history | Git log (`git log --oneline -10`) |
| Changelog | `CHANGELOG.md` (semantic-release) |
| Dates | Git commit timestamps |

**This file is for static project context only.**

---

## Project Overview

### Architecture

A Rust binary (`dfe-transform-vector`) runs as PID 1 in a K8s pod. It:
1. Loads "big dial" config (pipeline name, Kafka source/sink settings, transform file paths)
2. Generates Vector-native source and sink YAML from the big dials
3. Loads user-supplied transform YAMLs, auto-wires the DAG (source → transforms → sink)
4. Validates the assembled config via `vector validate`
5. Spawns Vector as a child subprocess with `--config-dir`
6. Provides composite health (`/health/live`, `/health/ready`) and metrics (`/metrics`) endpoints
7. Monitors the Vector child process, restarts on crash with exponential backoff
8. Supports hot-reload: re-validates config before sending SIGHUP to Vector

### Key Components

1. **Config Engine** (`src/config/`) - Big-dial config loading (7-layer cascade via hyperi-rustlib), source/sink YAML generation, transform loading, DAG wiring and validation
2. **Process Manager** (`src/vector/`) - Vector subprocess lifecycle: spawn, signal forwarding, crash recovery with backoff, health polling
3. **Observability Server** (`src/health.rs`, `src/metrics.rs`) - HTTP server exposing `/health/live`, `/health/ready`, `/metrics` (wrapper + proxied Vector metrics), KEDA scaling pressure signal
4. **Helm Chart** (`chart/`) - Purpose-built chart (not the official Vector chart), generates StatefulSet, Service, ConfigMaps, PodMonitor, KEDA ScaledObject
5. **dfe-engine Plugin** - Python ServicePlugin (ServiceDescriptor, Pydantic config model, deployment config) registered via entry point for unified management

### Tech Stack

- **Language:** Rust (edition 2024)
- **Async runtime:** tokio
- **Config:** figment (cascade), serde_yaml_ng
- **HTTP:** hyper/axum (health + metrics)
- **Metrics:** prometheus crate
- **Subprocess:** tokio::process
- **Shared lib:** hyperi-rustlib (config, transport, metrics patterns)
- **Data engine:** Vector.dev (subprocess, not compiled crate)
- **Deployment:** Helm + Argo CD (via dfe-engine HelmValuesCompiler)
- **Orchestration:** dfe-engine ServicePlugin (Python/Pydantic)

---

## Key Decisions

### Vector as Subprocess (not compiled crate)

**Decision:** Run Vector as a child process via `tokio::process::Command`
**Rationale:** Vector is ~500k lines of Rust with 400+ deps and no stable library API. Subprocess gives version decoupling (swap binary without Rust changes), fault isolation (crash doesn't kill wrapper), fast builds, and simple testing.
**Alternatives considered:** Compiling Vector as a Rust crate dependency — rejected due to build complexity, maintenance burden, and tight version coupling.

### Drop the Official Vector Helm Chart

**Decision:** Write our own Helm chart instead of using timberio/vector
**Rationale:** The chart adds almost no value for our use case — we always deploy as StatefulSet, generate our own config, handle our own probes. Its defaults are empty (no probes, no init containers). Our chart matches dfe-loader/dfe-receiver patterns exactly.
**Alternatives considered:** Continuing with official chart + heavy overrides — rejected because overrides already replicate 90% of the chart.

### Same Management Interface as dfe-loader/dfe-receiver

**Decision:** Implement the full dfe-engine ServicePlugin contract (ServiceDescriptor, Pydantic config model, deployment config, validation callback)
**Rationale:** Vector must not be an outlier. dfe-engine must manage it identically — same config registry CRUD, same Helm compilation, same KEDA wiring, same multi-instance support.
**Alternatives considered:** Standalone management — rejected because it would require duplicate tooling.

### Health Endpoint Paths

**Decision:** `/health/live` and `/health/ready` (not `/healthz` and `/readyz`)
**Rationale:** Matches dfe-loader and dfe-receiver contract that dfe-engine expects.

### Vector Version Pinning

**Decision:** Config-level version pin with strict/warn/disabled modes, checked at startup
**Rationale:** Prevents silent version drift between config expectations and actual binary. Updates are a container image concern (change Dockerfile ARG), not a wrapper code concern.

---

## External Dependencies

- **Vector.dev** - Data processing engine (subprocess binary, not library)
- **hyperi-rustlib** - Shared Rust library (config cascade, transport abstraction, metrics patterns)
- **dfe-engine** - Python orchestrator (ServicePlugin registration, config registry, Helm compilation)
- **Apache Kafka** - Source and sink for all pipelines (via Vector's librdkafka)
- **Argo CD** - GitOps deployment (Application CRDs generated by dfe-engine)
- **KEDA** - Autoscaling based on Kafka consumer lag + scaling pressure metric
- **Prometheus** - Metrics scraping via PodMonitor

---

## Resources

**Documentation:**

- [docs/DESIGN.md](docs/DESIGN.md) - Full architecture and design
- [RESEARCH.md](RESEARCH.md) - Research findings and option analysis
- [TODO.md](TODO.md) - Work breakdown structure

**External Resources:**

- [Vector.dev Transforms Reference](https://vector.dev/docs/reference/configuration/transforms/)
- [Vector Kafka Source](https://vector.dev/docs/reference/configuration/sources/kafka/)
- [Vector Kafka Sink](https://vector.dev/docs/reference/configuration/sinks/kafka/)
- [Vector Multi-file Config](https://vector.dev/guides/level-up/managing-complex-configs/)

**Sibling Projects:**

- `/projects/dfe-loader` - Kafka → ClickHouse loader (Rust, same big-dial pattern)
- `/projects/dfe-receiver` - Inbound data receiver (Rust, same management interface)
- `/projects/dfe-engine` - Python orchestrator (manages all DFE services)
- `/projects/dfe-core` - Infrastructure-as-code (Terraform, ArgoCD, current Vector deployment)

---

## Notes for AI Assistants

This file contains **static project context only**.

**DO NOT add:**

- Version numbers (use `git describe --tags`)
- Progress/tasks (use `TODO.md`)
- Dates or session history (use `git log`)
- "Current Session" or "Last Session" sections

**DO add:**

- Architecture decisions and rationale
- Key component descriptions
- External dependencies
- How things work (not what's happening)

When in doubt, ask: "Will this be true next week?" If no, it doesn't belong here.
