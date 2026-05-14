## CI

CI is live via `hyperi-ci`. Run `hyperi-ci check` (or `make check`) locally before pushing.

---

# Project Context

**Project:** dfe-transform-vector
**Purpose:** Rust wrapper that manages Vector.dev as a subprocess for Kafka→transform→Kafka pipelines, making Vector a first-class DFE platform citizen alongside dfe-loader and dfe-receiver.

> **Note:** The `hyperi-ai/` submodule provides standards and configuration - not code
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
4. **Helm Chart** (`chart/`) - Purpose-built chart (not the official Vector chart), generates Deployment, Service, ConfigMap, KEDA ScaledObject
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
**Rationale:** The chart adds almost no value for our use case — we generate our own config and handle our own probes. Its defaults are empty (no probes, no init containers). Our chart matches dfe-loader/dfe-receiver patterns exactly.
**Alternatives considered:** Continuing with official chart + heavy overrides — rejected because overrides already replicate 90% of the chart.

### Same Management Interface as dfe-loader/dfe-receiver

**Decision:** Implement the full dfe-engine ServicePlugin contract (ServiceDescriptor, Pydantic config model, deployment config, validation callback)
**Rationale:** Vector must not be an outlier. dfe-engine must manage it identically — same config registry CRUD, same Helm compilation, same KEDA wiring, same multi-instance support.
**Alternatives considered:** Standalone management — rejected because it would require duplicate tooling.

### Forward MSRV (Latest Stable, Not a Cap)

**Decision:** `rust-version` is set to the latest stable (currently 1.94) and bumped
freely. It is a build requirement floor, not a compatibility cap.
**Rationale:** Pre-OSS internal project — we control the build environment and always
use latest stable. The field ensures cargo resolver picks deps compatible with our
toolchain. When the project is open-sourced, freeze MSRV and follow a more conservative
bump policy for downstream consumers.

### Health Endpoint Paths

**Decision:** `/health/live` and `/health/ready` (not `/healthz` and `/readyz`)
**Rationale:** Matches dfe-loader and dfe-receiver contract that dfe-engine expects.

### Vector Version Pinning

**Decision:** Config-level version pin with strict/warn/disabled modes, checked at startup
**Rationale:** Prevents silent version drift between config expectations and actual binary. Updates are a container image concern (change Dockerfile ARG), not a wrapper code concern.

### Config File Watcher Uses Polling (not inotify)

**Decision:** The wrapper's config file watcher (TODO 5.1) must use poll-based watching, not filesystem change notifications.
**Rationale:** Transform YAML files may be delivered via S3-backed mounts (e.g. s3fs, goofys, Mountpoint for S3) which do not generate inotify events. Vector's native `--watch-config` flag only supports inotify/kqueue — it will not detect changes on S3-mounted volumes. The dfe platform already handles this in other components by polling at a configurable interval (default 30s). The wrapper must implement its own polling watcher rather than relying on Vector's built-in file watching for config reload triggers.

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

- `/projects/dfe-transform-vrl` - Embedded VRL transform engine (Rust, for VRL-only pipelines with native msgpack)
- `/projects/dfe-loader` - Kafka → ClickHouse loader (Rust, same big-dial pattern)
- `/projects/dfe-receiver` - Inbound data receiver (Rust, same management interface)
- `/projects/dfe-engine` - Python orchestrator (manages all DFE services)
- `/projects/dfe-core` - Infrastructure-as-code (Terraform, ArgoCD, current Vector deployment)

---

## Notes for AI Assistants

This file contains **static project context only**.

**NEVER use path dependencies to hyperi-rustlib.** The `Cargo.toml` must always
reference hyperi-rustlib from crates.io (`version = ">=X.Y"`). Never add
`[patch.crates-io]` or `path = "/projects/hyperi-rustlib"` overrides. The local
checkout at `/projects/hyperi-rustlib` is for reading source code only — never
for build-time linking. This rule must not be removed.

**NEVER kill cargo processes** to free the lock. Multiple projects build concurrently
on this host. Wait for the lock to release, or ask the user — never `kill`, `pkill`,
or `rm` the cargo lock file.

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

---

## Rust Release-Track Optimisation Readiness

**Tier 1 (jemalloc allocator at every channel + fat LTO on beta+):**
✅ **WIRED**

`Cargo.toml` declares `jemalloc = ["dep:tikv-jemallocator"]` (default
features empty) and `src/main.rs` wires `#[global_allocator]` under
`#[cfg(feature = "jemalloc")]`. Release build verified:
`strings target/release/dfe-transform-vector | grep -ciE
'jemalloc|je_mallctl'` returns 39. CI opts in per channel via
`--features jemalloc` and overrides `lto = "fat"` at beta+.

Coverage scope: the wrapper hot path (lifecycle, metrics endpoint,
config reload). Vector's own throughput loop is unaffected — Vector
is a separate binary with its own build pipeline.

**Tier 2 (PGO + BOLT on release):** ⚠️ **NOT RECOMMENDED**

This is a supervisor/lifecycle wrapper around Vector — the data-plane
hot path lives inside the Vector subprocess, not in this code. PGO
would profile rarely-hit wrapper code (metrics scrapes, hot-reload
events, lifecycle state transitions) and provide near-zero runtime
benefit on a 30-60 min build penalty. Stick with Tier 1.

---

## POLICY UPDATE 2026-04-17 — jemalloc-only

DFE allocator policy standardised on jemalloc. Source:
`hyperi-ai/standards/languages/RUST.md` → *Allocator Policy*.

Tier 1 allocator wiring landed 2026-04-29 — jemalloc only, no mimalloc
feature, no fallback `#[cfg]`. See `Cargo.toml` `[features]` and
`src/main.rs` for the canonical wiring.

---

## CI workflow contract (post dfe-loader Canary 2)

`.github/workflows/ci.yml` MUST satisfy these for release-channel
publishing (and for Tier 2 PGO/BOLT to run if ever enabled). Loader
v1.17.4 publish *succeeded* but silently shipped spike-channel
(Tier 1 thin LTO instead of fat) because of these:

| Setting | Required | Current | Why |
|---|---|---|---|
| `uses: hyperi-io/hyperi-ci/.github/workflows/rust-ci.yml@<ref>` | `ba03ff0` (v1.12.1+) or `@main` | ✅ `@main` | Older pins predate `HYPERCI_CHANNEL` resolver — tagged dispatch falls back to `channel=spike` |
| `with: publish-target` | `both` | ✅ `both` | `internal` resolves to spike channel (thin LTO). `both` = release channel (fat LTO) |

**Action:** flipped `publish-target` to `both` 2026-04-29 — single-line
otherwise even Tier 1 fat LTO doesn't apply to the wrapper binary.

Reference implementation: dfe-loader v1.17.5 — full Tier 2 verified
live on R2, build log signature
`channel=release, allocator=jemalloc, lto=fat, pgo=on, bolt=on`.
This wrapper would target `pgo=off, bolt=off` but otherwise the same.


---

## DFE Pipeline Context

**This app:** dfe-transform-vector — Rust wrapper around the Vector.dev subprocess — Kafka → Kafka. Anomaly: routing config is owned by Vector itself, not by rustlib.
**Criticality:** 6/6 (1 = highest)
**Rustlib rebuild wave:** 2

### Data flow

```text
                 ┌────────────────────────────────────────────┐
                 │                INGRESS                      │
                 │  ┌──────────────┐    ┌──────────────┐      │
                 │  │ dfe-receiver │    │ dfe-fetcher  │      │
                 │  │ (push: HTTP/ │    │ (pull: AWS / │      │
                 │  │  syslog/gRPC)│    │ Azure / M365)│      │
                 │  └──────┬───────┘    └──────┬───────┘      │
                 └─────────┼───────────────────┼──────────────┘
                           │                   │
                           └─────────┬─────────┘
                                     ▼
                         ┌─────────────────────┐
                         │  Kafka — ingress    │
                         └──────────┬──────────┘
                                    ▼
                         ┌──────────────────────┐
                         │      dfe-loader      │
                         │ (route, enrich,      │
                         │  parse, fan-out)     │
                         └──────────┬──────────┘
                                    ▼
                         ┌─────────────────────┐
                         │ Kafka — transform   │
                         └──────────┬──────────┘
                           │                  │
                           ▼                  ▼
                  ┌────────────────┐ ┌──────────────────────┐
                  │ dfe-transform- │ │ dfe-transform-vector │
                  │      vrl       │ │ (Vector.dev wrapper) │
                  └────────┬───────┘ └──────────┬──────────┘
                           │                    │
                           └─────────┬──────────┘
                                     ▼
                         ┌─────────────────────┐
                         │  Kafka — archive    │
                         └──────────┬──────────┘
                                    ▼
                         ┌─────────────────────┐
                         │    dfe-archiver     │
                         │ (S3 / Azure / GCS / │
                         │      MinIO)         │
                         └─────────────────────┘
```

### Siblings — the core six DFE Rust apps

| # | App | Role | Wave |
|---|-----|------|------|
| 1 | dfe-loader | Mid-tier routing, enrichment, parsing — most complex, best canary | 1 |
| 2 | dfe-receiver | Push ingress (HTTP / syslog / gRPC) — pipeline entry point | 1 |
| 3 | dfe-fetcher | Pull ingress (AWS / Azure / M365 / GCP) | 2 |
| 4 | dfe-archiver | Sink to object store — bookend of the pipeline | 1 |
| 5 | dfe-transform-vrl | Embedded VRL transform engine | 2 |
| 6 | dfe-transform-vector | Vector.dev subprocess wrapper (owns its own routing config) | 2 |

### Rustlib rebuild waves (ARC = 3 concurrent Rust CI builds)

- **Wave 1 — bookends + ingress:** dfe-loader, dfe-receiver, dfe-archiver.
  Covers the full data path (ingress → mid-tier → sink). If wave 1 is green,
  the pipeline structure is sound.
- **Wave 2 — remaining:** dfe-fetcher, dfe-transform-vrl, dfe-transform-vector.
  Pull ingress + transform layer.

Waves run sequentially; consumers within a wave run in parallel, capped at
the ARC runner's concurrent Rust CI capacity (3).

### Automation — `/rebuild-consumers` (driven from rustlib)

When `hyperi-rustlib` changes and the change needs to flow downstream,
**drive the rebuild from the `hyperi-rustlib` repo, not from this app**.
The rebuild-consumers skill in rustlib owns the wave plan, target version,
ARC capacity, and consumer scope:

```bash
# from the rustlib repo (sibling of this app):
python3 scripts/rebuild_consumers.py check        # surface breakage
python3 scripts/rebuild_consumers.py apply --wave 1
python3 scripts/rebuild_consumers.py apply --wave 2
```

Reference (relative to this app — adjust if your workspace layout differs):

- `../hyperi-rustlib/.claude/skills/rebuild-consumers/SKILL.md`
- `../hyperi-rustlib/.claude/consumers.toml`
- `../hyperi-rustlib/scripts/rebuild_consumers.py`
- `../hyperi-rustlib/STATE.md` → *Core DFE Apps*

### Backburner — not auto-rebuilt

`dfe-transform-elastic` and `dfe-transform-splack` have drifted significantly
against accumulated rustlib changes and need manual remediation before
re-joining the lockstep set. The automation **excludes** them by design.
Promotion requires a deliberate `tier = "core"` flip in
`../hyperi-rustlib/.claude/consumers.toml`.
