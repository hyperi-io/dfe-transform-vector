# DFE Transform Vector — Research & Architecture Discussion

## Problem Statement

We need to orchestrate **Kafka → Vector transforms → Kafka** pipelines deployed as K8s pods. Each deployment has:

- A **pre-defined Kafka source** (labelled, YAML)
- A **pre-defined Kafka sink** (labelled, YAML)
- **N user-supplied Vector YAML files** in between (98% transforms, occasionally extra sources/sinks)

The wiring, credentials, scaling, and lifecycle must be abstracted behind "big dials" — high-level configuration knobs that hide infrastructure complexity.

---

## Current Architecture (dfe-core)

### How it works today

dfe-core uses a **GitOps + Helm + Artifactory** approach:

1. **ArgoCD ApplicationSets** with matrix generators deploy N pipelines across M clusters from a single declarative config
2. **Official Vector Helm chart** (timberio/vector v0.42.1) deployed as StatefulSet
3. **Vector configs fetched dynamically** at pod init via init container from Artifactory (zip archive)
4. **Secrets** (Kafka SASL, ClickHouse creds, Artifactory creds) managed through AWS Secrets Manager → ExternalSecrets → K8s Secrets → env vars, 15-minute refresh
5. **KEDA autoscaling** based on Kafka consumer lag
6. **Multi-source Helm values**: common values from dfe-core + tenant-specific values from pipelines repo + kustomize patches for secrets

### Config fetching workflow (init container)
```bash
curl -u ${ARTIFACTORY_USERNAME}:${ARTIFACTORY_PASSWORD} \
  -L -o /tmp/download.zip \
  ${ARTIFACTORY_VECTOR_TEMPLATES}/vector-artifacts/artifacts-${VERSION}.zip
unzip /tmp/download.zip -d /etc/vector_config
```

### Key infrastructure
- Dedicated Karpenter node pools (ARM64 c8gn compute-optimized for pipelines, r7g memory-optimized for receiver)
- Persistent storage (20Gi for pipelines, 128Gi for receiver)
- Separate Vector Receiver (NLB-exposed, mTLS + token auth) from Vector Pipelines (internal transform layer)

### Strengths
- Battle-tested GitOps pattern, well-understood by infra team
- ArgoCD handles multi-tenant, multi-cluster deployment
- Secrets rotation is automatic
- KEDA scaling works well with Kafka lag

### Weaknesses
- **No config validation before deployment** — bad YAML goes live and crashes the pod
- **Opaque config assembly** — zip from Artifactory means no visibility into what Vector will actually run
- **No composition guarantees** — nothing validates that the source label matches transform inputs or that the sink inputs are wired correctly
- **Upgrade friction** — Vector version bumps (currently 0.48.0) can break VRL transforms silently
- **No dry-run** — can't test a pipeline config without deploying it
- **Init container dependency** — pod startup blocked on Artifactory availability

---

## Reference: dfe-loader Architecture

dfe-loader is the sibling Rust project (Kafka → ClickHouse) that established the "big dials" pattern:

### Key big dials exposed
| Category | Dials |
|---|---|
| **Buffer** | `flush_rows`, `flush_bytes`, `flush_age_secs` |
| **Routing** | `db_fields`, `table_fields`, `default_db/table`, `routed_orgs`, `route_all_by_org` |
| **Metadata** | `metadata.enabled`, `include_raw`, `tags_fields`, `drop_tags` |
| **Timestamp DQ** | `enabled`, `max_future_seconds`, `invalid_action` |
| **Scaling** | `memory.limit_bytes`, `pressure_threshold`, `max_concurrent_inserts` |
| **Field mapping** | `builtin` (ecs/cim/beats), `default_action`, external mapping files |
| **DLQ** | `enabled`, `mode` (per_table/common), `topic_suffix` |

### What makes it work
- **Config cascade**: CLI args → env vars → .env → YAML/TOML → defaults
- **Hot-reload**: file polling + SIGHUP for safe components (routing, metadata, sanitization)
- **Metrics**: Prometheus exposition with KEDA-compatible scaling pressure
- **Health**: `/healthz` endpoint with readiness checks
- **Circuit breaker**: per-destination failure isolation

---

## Vector.dev Configuration Patterns

### How Vector wires components
Vector pipelines are **directed acyclic graphs (DAGs)**. Each component declares its upstream via the `inputs` field:

```yaml
sources:
  kafka_in:
    type: kafka
    bootstrap_servers: "broker:9092"
    topics: ["events"]
    group_id: "vector"

transforms:
  parse:
    type: remap
    inputs: ["kafka_in"]
    source: |
      . = parse_json!(.message)

  enrich:
    type: remap
    inputs: ["parse"]
    source: |
      .environment = "production"

sinks:
  kafka_out:
    type: kafka
    inputs: ["enrich"]
    bootstrap_servers: "broker:9092"
    topic: "processed"
    encoding:
      codec: json
```

### Multi-file config support
Vector natively supports composable configs:
```bash
vector --config /etc/vector/source.yaml --config /etc/vector/transforms/*.yaml --config /etc/vector/sink.yaml
# or
vector --config-dir /etc/vector/config.d/
```

### Validation tools
- `vector validate` — pre-flight config validation (checks topology, VRL syntax, type compatibility)
- `vector graph` — outputs pipeline topology as DOT graph
- `vector tap` — inspect inputs/outputs of components in real time
- Unit tests built into Vector config format

---

## Option 1: Pure Helm Values Approach (Enhanced dfe-core)

### How it would work
1. Keep the existing Helm + ArgoCD + Artifactory pattern
2. Add a **validation CI step** that runs `vector validate` on assembled configs before publishing to Artifactory
3. Add a **config assembler script** (bash/python) that:
   - Takes the pre-defined source YAML, user transform YAMLs, and pre-defined sink YAML
   - Validates `inputs` wiring (source label → first transform → ... → sink)
   - Merges into a single config or directory
   - Runs `vector validate`
   - Packages and uploads to Artifactory
4. Expose "big dials" as top-level Helm values that template into the source/sink YAML

### Big dials via Helm values
```yaml
# values.yaml — the user-facing "big dials"
pipeline:
  name: "syslog-enrichment"
  version: "1.2.0"

source:
  kafka:
    topics: ["raw-syslog"]
    group_id: "vector-syslog"
    # consumer_group_prefix auto-derived from pipeline.name

sink:
  kafka:
    topic: "enriched-syslog"
    key_field: ".tenant_id"

scaling:
  minReplicas: 1
  maxReplicas: 10
  kafkaLagThreshold: 1000

resources:
  cpu: "2"
  memory: "4Gi"
```

### Pros
- **Minimal new code** — extends what already works
- **Team familiarity** — Helm + ArgoCD is well-understood
- **Vector upgrades are simple** — just bump chart version + image tag
- **No new runtime dependency** — Vector is the only binary in the pod

### Cons
- **Validation is CI-time only** — runtime composition errors still possible
- **No runtime DAG awareness** — can't detect broken wiring after deployment
- **Helm templating is fragile** — complex `inputs` wiring via Go templates gets ugly fast
- **No hot-reload coordination** — config changes require pod restart
- **Limited programmatic control** — can't dynamically adjust pipeline topology

---

## Option 2: Rust Integration Layer (this project)

### How it would work
A Rust binary (`dfe-transform-vector`) that:

1. **Owns the Vector config lifecycle**: assembles, validates, and manages Vector configuration
2. **Runs Vector as a managed subprocess** (or embeds it if feasible)
3. **Exposes big dials** via its own config (matching dfe-loader's cascade pattern)
4. **Provides health, metrics, and scaling signals** (like dfe-loader does)
5. **Still uses Helm** for K8s deployment — this is the binary that runs in the pod instead of bare Vector

### Architecture
```
┌─────────────────────────────────────────────────────┐
│  K8s Pod                                             │
│                                                      │
│  ┌───────────────────────────────────────────────┐  │
│  │  dfe-transform-vector (Rust)                  │  │
│  │                                                │  │
│  │  Config Engine:                                │  │
│  │  - Reads big-dial config (YAML/env/CLI)       │  │
│  │  - Generates source.yaml (Kafka source)       │  │
│  │  - Loads user transform YAMLs                 │  │
│  │  - Generates sink.yaml (Kafka sink)           │  │
│  │  - Validates DAG wiring (inputs chain)        │  │
│  │  - Runs `vector validate` on assembled config │  │
│  │                                                │  │
│  │  Process Manager:                              │  │
│  │  - Spawns `vector` as child process            │  │
│  │  - Monitors health (Vector API on :8686)      │  │
│  │  - Handles graceful shutdown (SIGTERM chain)   │  │
│  │  - Restarts on crash with backoff             │  │
│  │                                                │  │
│  │  Observability:                                │  │
│  │  - /healthz (composite: self + vector)        │  │
│  │  - /metrics (proxy vector + own metrics)      │  │
│  │  - KEDA scaling pressure signals              │  │
│  │                                                │  │
│  │  Hot-reload:                                   │  │
│  │  - Watches config files / SIGHUP              │  │
│  │  - Re-validates before applying               │  │
│  │  - Sends SIGHUP to Vector or restarts it      │  │
│  │                                                │  │
│  └───────────────────────────────────────────────┘  │
│                                                      │
│  ┌───────────────────────────────────────────────┐  │
│  │  vector (child process)                        │  │
│  │  - Runs with assembled config                  │  │
│  │  - Kafka source → transforms → Kafka sink     │  │
│  └───────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
```

### Big dials
```yaml
# dfe-transform-vector config
pipeline:
  name: "syslog-enrichment"

source:
  type: kafka
  brokers: "${KAFKA_BROKERS}"    # env var interpolation
  topics: ["raw-syslog"]
  group_id: "vector-${pipeline.name}"
  sasl:
    mechanism: "SCRAM-SHA-512"
    username: "${KAFKA_SASL_USERNAME}"
    password: "${KAFKA_SASL_PASSWORD}"

sink:
  type: kafka
  brokers: "${KAFKA_BROKERS}"
  topic: "enriched-syslog"
  key_field: ".tenant_id"
  encoding: json

transforms:
  # paths to user-supplied Vector YAML files
  files:
    - /etc/vector_config/transforms/parse.yaml
    - /etc/vector_config/transforms/enrich.yaml
    - /etc/vector_config/transforms/filter.yaml
  # OR inline dir
  dir: /etc/vector_config/transforms/

vector:
  binary: /usr/bin/vector
  data_dir: /var/lib/vector
  api_port: 8686

health:
  port: 9000

metrics:
  port: 9090

scaling:
  pressure_threshold: 0.8
```

### Config assembly logic (Rust)
1. Read big-dial config
2. Generate source YAML with canonical label (e.g., `kafka_source`)
3. Load user transform YAMLs, validate each has valid `inputs` references
4. Auto-wire: if first transform has `inputs: ["source"]`, rewrite to `inputs: ["kafka_source"]`
5. Generate sink YAML with `inputs` pointing to last transform's label
6. Merge all into Vector config directory
7. Run `vector validate --config-dir /tmp/assembled/`
8. If valid, start Vector with `--config-dir`

### Pros
- **Runtime validation** — config is validated before Vector starts, bad configs never run
- **DAG awareness** — Rust layer understands the pipeline topology, can detect broken wiring
- **Unified observability** — single health/metrics endpoint that aggregates Vector + wrapper state
- **Hot-reload with validation** — re-validates config before sending SIGHUP to Vector
- **Auto-wiring** — users supply transforms, the system wires source/sink automatically
- **Consistent with dfe-loader** — same config cascade, same big-dial pattern, team familiarity
- **Crash resilience** — wrapper restarts Vector on crash, reports to metrics
- **Future extensibility** — can add config templating, VRL validation, transform library management

### Cons
- **New binary to maintain** — more code, more builds, more testing
- **Vector subprocess management** — signal forwarding, stdout/stderr capture, PID management
- **Version coupling risk** — must track Vector releases for config schema compatibility
- **Startup latency** — wrapper adds milliseconds (negligible in practice)
- **Two processes in one pod** — slightly more complex resource management

---

## External Ecosystem Research

### Existing Kubernetes operators for Vector

| Operator | Language | CRDs | Maturity |
|---|---|---|---|
| [kaasops/vector-operator](https://github.com/kaasops/vector-operator) | Go | `Vector`, `VectorPipeline`, `ClusterVectorPipeline` | Active, handles config merging + validation |
| [zcentric/vector-operator](https://github.com/zcentric/vector-operator) | Go | `Vector`, `VectorAggregator`, `VectorPipeline` | Active, multi-version support |

These operators are the closest existing "big dial" pattern — you declare high-level pipeline CRDs and the operator generates, validates, and deploys Vector configs. However:
- Both are Go-based, not Rust
- They add a cluster-level operator dependency
- They may conflict with existing ArgoCD patterns
- They don't provide the same config cascade / hot-reload model as dfe-loader

### Programmatic config management tools

| Tool | Approach |
|---|---|
| `vector validate` | Built-in pre-flight validation (topology, VRL syntax, types) |
| `vector generate` | Scaffolding for linear chains |
| [vector_jsonnet](https://github.com/xunleii/vector_jsonnet) | Jsonnet library for composable Vector configs |
| CUE | Strong typing + validation over Vector YAML |
| Datadog Observability Pipelines | Commercial "big dial" UI built on Vector (Terraform support) |

### Key Vector capabilities we leverage

- **Multi-file config**: `--config-dir` loads all YAML from a directory
- **DAG topology**: `inputs` field wiring, validated by `vector validate`
- **Environment variable interpolation**: `${VAR}` and `${VAR:-default}` in config
- **Config reload**: `--watch-config` or SIGHUP
- **GraphQL API**: observation and limited control at runtime (port 8686)
- **Kafka source/sink**: librdkafka-backed, SASL/SCRAM, consumer groups, regex topics, acknowledgements

### Rust Kubernetes operator ecosystem

For future consideration if we want to go full operator:
- **[kube-rs](https://github.com/kube-rs/kube)** — CNCF Sandbox, provides Controller builder, CRD derive macros, async reconcilers
- One production case study: zero crashes, 68% less resource consumption vs Go operator
- Not needed for Option 2 (subprocess model) but relevant if we later want CRD-based pipeline management

---

## Decision Matrix

| Criterion | Option 1 (Helm-only) | Option 2 (Rust wrapper) |
|---|---|---|
| **Robustness** | CI-time validation only | Runtime validation + auto-wiring + crash recovery |
| **Config safety** | Bad YAML can reach production | Bad YAML caught before Vector starts |
| **Big dials** | Helm values (Go templates) | Native config cascade (like dfe-loader) |
| **Vector upgrades** | Bump chart + image tag | Bump image tag, validate config schema compat |
| **Hot-reload** | Pod restart only | Validated reload via SIGHUP |
| **Observability** | Vector's built-in metrics | Composite metrics (wrapper + Vector) |
| **Team familiarity** | High (existing pattern) | Medium (new binary, but mirrors dfe-loader) |
| **Maintenance cost** | Low | Medium (new Rust codebase) |
| **Time to MVP** | Days | Weeks |
| **Auto-wiring** | Manual `inputs` management | Automatic source/sink wiring |
| **Extensibility** | Limited by Helm templating | Full programmatic control |
| **External deps** | Helm, ArgoCD, Artifactory | Same + Rust binary |
| **Scaling** | KEDA (existing) | KEDA + pressure signals (like dfe-loader) |
| **Crash handling** | K8s restartPolicy | Wrapper restart with backoff + K8s fallback |

---

## Vector Helm Chart: What It Actually Provides

### The honest answer: not much magic

The official Vector Helm chart generates standard K8s resources (StatefulSet/Deployment/DaemonSet, Service, ServiceAccount, ConfigMap, optional PodMonitor/HPA/PDB). Its **actual value-adds** are:

| Feature | Value | Hard to replicate? |
|---|---|---|
| **ConfigMap checksum annotation** | Auto-restarts pods when config changes | No — one line, or use stakater/Reloader |
| **Port auto-generation from config** | Parses Vector YAML to generate container/service ports | Medium — convenience, prevents port drift |
| **Role-based workload switching** | Single chart deploys as Agent/Aggregator/Stateless | No — we always use StatefulSet (Aggregator) |
| **HAProxy sub-chart** | TCP load balancer sidecar | Not needed for transform pipelines |
| **`customConfig` with `tpl`** | Helm template expressions inside Vector config | Nice but we already use env var interpolation |
| **PodMonitor generation** | Small Prometheus Operator manifest | Trivial to write ourselves |

### What it does NOT provide
- **No probes configured by default** — liveness/readiness are empty `{}`, you must opt in
- **No preStop hook** — empty `{}` by default
- **No init containers** — dfe-core adds its own (Artifactory fetch)
- **No sidecars** — dfe-core adds nothing here either
- **No config validation** — just renders YAML into a ConfigMap
- **API binds to 127.0.0.1** — probes can't reach it without overriding to 0.0.0.0

### What Vector itself handles (no chart needed)
| Feature | Vector native | Notes |
|---|---|---|
| Graceful shutdown | SIGTERM → stop sources → flush buffers → exit | Default 60s timeout, configurable |
| Config hot-reload | `--watch-config poll` or SIGHUP | Works with K8s ConfigMap volume mounts |
| Health endpoint | `/health` on API port (8686) | Returns `{"ok":true}` |
| Internal metrics | `internal_metrics` source | Always available |
| Prometheus exposition | `prometheus_exporter` sink | Config, not chart feature |
| Disk buffer persistence | `data_dir` with WAL files | Just needs a volume mount |
| Exit code 78 | Invalid configuration | Useful for init container validation |

### Recommendation: Drop the Vector Helm chart

For the Rust wrapper approach, the chart adds friction and almost no value:
- We're always deploying as StatefulSet (one role)
- We generate our own Vector config (the whole point of the wrapper)
- We handle health/metrics through the wrapper
- We already customize probes, init containers, volumes, IRSA, topology spread — all via dfe-core overrides

**Replace with**: Our own Helm chart (or plain manifests + kustomize) in this repo that deploys the `dfe-transform-vector` container. The container image includes both the Rust wrapper binary and the Vector binary. Simpler, no upstream chart version to track, full control.

The dfe-core ArgoCD ApplicationSet pattern stays — it just points to our chart instead of `helm.vector.dev`.

---

## Subprocess vs Compiled Crate: How to Run Vector

### Option A: Vector as subprocess (recommended)

```
dfe-transform-vector (PID 1) → spawns → vector (child process)
```

The Rust binary assembles config, validates it, then calls `vector --config-dir /tmp/assembled/` as a child process via `tokio::process::Command`.

**Why this is the right call:**

| Factor | Subprocess | Compiled crate |
|---|---|---|
| **Vector upgrades** | Swap binary, zero Rust changes | Rebuild entire crate against new Vector version |
| **Build complexity** | Rust binary + Vector binary in Docker image | Must compile Vector from source as a Rust dependency — Vector is ~500k lines of Rust with 400+ deps |
| **Version decoupling** | Wrapper and Vector version independently | Tight coupling, must coordinate releases |
| **Fault isolation** | Vector crash doesn't kill wrapper; wrapper can restart it, report metrics, log the crash | Vector panic = whole process dies |
| **Memory/resource visibility** | Two processes, separate memory tracking in K8s | Single process, simpler but less granular |
| **Signal handling** | Forward SIGTERM to child, wait for graceful drain | Native — single process handles it |
| **Development speed** | Don't need to understand Vector internals | Must understand Vector's internal APIs (unstable, undocumented) |
| **Testing** | Mock the subprocess, test config assembly independently | Integration-heavy, hard to unit test |
| **Maintenance burden** | Low — treat Vector as a black box | High — every Vector release may break internal API assumptions |

**The "subprocess is brittle" concern:**

This is the key question. But consider:
- The current Helm deployment is *already* "Vector as a process managed by something else" — that something is kubelet + the container runtime
- Our wrapper adds a layer *between* kubelet and Vector that can: validate before starting, restart with backoff on crash, report crash metrics, hold the health endpoint unhealthy during restart
- This is strictly more robust than bare Vector in a pod
- The pattern is well-established: nginx unit, envoy + pilot, istio sidecar, containerd + shims

**Subprocess management is simple in Rust:**

```rust
// Simplified — the actual implementation would use tokio::process
let mut child = Command::new("/usr/bin/vector")
    .args(["--config-dir", "/tmp/assembled/", "--watch-config", "poll"])
    .stdout(Stdio::inherit())  // Vector logs go to pod stdout
    .stderr(Stdio::inherit())
    .spawn()?;

// Forward SIGTERM
tokio::signal::unix::signal(SignalKind::terminate())?.recv().await;
child.signal(Signal::SIGTERM)?;

// Wait for graceful shutdown (with timeout)
tokio::time::timeout(Duration::from_secs(55), child.wait()).await?;
```

**Key design decisions for subprocess approach:**
1. **stdout/stderr: inherit** — Vector logs go directly to pod stdout, no buffering/reparsing
2. **PID 1: the wrapper** — receives SIGTERM from kubelet, forwards to Vector
3. **Health: composite** — wrapper checks its own state + polls Vector's `/health` API
4. **Restart policy**: exponential backoff (1s, 2s, 4s, 8s... capped at 60s), with metrics counter
5. **Config reload**: wrapper re-assembles + re-validates, then either SIGHUP to Vector or restart it

### Option B: Vector as compiled crate (not recommended)

This would mean adding `vector` as a Rust dependency and calling its internal APIs to build and run a pipeline programmatically.

**Why not:**
- Vector is **not designed as a library**. There is no stable public API for embedding it. The `vector` crate's internals are undocumented and change between releases.
- The build would pull in **~400 transitive dependencies** including `rdkafka` (C FFI), `lua`, `vrl` compiler, `aws-sdk-*`, and many more. Build times would be enormous.
- **Every Vector release** would require recompilation and potentially code changes to accommodate internal API shifts.
- You'd be taking on the **maintenance burden of Vector's internals** rather than treating it as a stable, well-tested binary.
- The one advantage (single process, native signal handling) doesn't justify the cost. K8s already manages process lifecycle — adding a thin wrapper on top is negligible overhead.

### Verdict: Subprocess, decisively

The subprocess model gives us:
- Clean version decoupling (upgrade Vector independently)
- Fault isolation (crash → restart, not crash → pod death)
- Simple development (config assembly + process management, not Vector internals)
- Fast builds (small Rust binary, Vector is just a binary in the Docker image)

The "brittleness" concern is a non-issue because:
1. The alternative (bare Vector in a pod) is the *same thing* with less control
2. Process supervision is a solved problem (systemd, supervisord, tini, etc.)
3. Our wrapper is purpose-built and thin — it's not a generic process manager

---

## Discussion Points

### 1. Do we need runtime validation?
The biggest differentiator. If bad configs reaching production is rare and acceptable (fix-forward with GitOps), Option 1 is fine. If it causes outages or data loss, Option 2 pays for itself.

### 2. How complex are the transform chains?
If most deployments are 2-3 simple VRL transforms, Helm templating handles it. If we have 10+ transforms with branching routes, auto-wiring becomes essential.

### 3. Vector upgrade strategy
Option 1 is simpler for upgrades (just bump versions). Option 2 adds a layer that must be tested against each Vector release. However, Option 2 can also **pin and validate** Vector versions more precisely.

### 4. Should we embed Helm or not?
**Recommendation: Keep Helm for K8s deployment, regardless of option.** The Rust binary (if chosen) is what runs *inside* the pod. Helm + ArgoCD still handles the K8s deployment lifecycle. The init container pattern can still fetch transform YAMLs from Artifactory.

### 5. Hybrid approach?
Start with Option 1 (enhanced validation in CI), build the Rust wrapper incrementally:
- Phase 1: Config assembler CLI tool (validates + assembles, runs in CI and init container)
- Phase 2: Runtime wrapper (subprocess management, health, metrics)
- Phase 3: Hot-reload + scaling pressure signals

This gives immediate value (validation) while building toward the full vision.

### 6. Do we adopt an existing operator?
The kaasops/vector-operator is interesting but:
- Adds a cluster-wide dependency
- Conflicts with ArgoCD ownership model
- Doesn't match our config cascade pattern
- Go-based, doesn't fit our Rust ecosystem

**Recommendation: Don't adopt an external operator.** Our needs are specific enough that a purpose-built solution (either option) is better.

---

## References

- [Vector Transforms Reference](https://vector.dev/docs/reference/configuration/transforms/)
- [Vector Managing Complex Configs](https://vector.dev/guides/level-up/managing-complex-configs/)
- [Vector Kafka Source](https://vector.dev/docs/reference/configuration/sources/kafka/)
- [Vector Kafka Sink](https://vector.dev/docs/reference/configuration/sinks/kafka/)
- [Vector Helm Chart](https://github.com/vectordotdev/helm-charts)
- [Vector K8s Deployment](https://vector.dev/docs/setup/installation/platforms/kubernetes/)
- [Vector Environment Variables](https://vector.dev/docs/reference/environment_variables/)
- [Vector Unit Testing](https://vector.dev/docs/reference/configuration/unit-tests/)
- [kaasops/vector-operator](https://github.com/kaasops/vector-operator)
- [zcentric/vector-operator](https://github.com/zcentric/vector-operator)
- [vector_jsonnet](https://github.com/xunleii/vector_jsonnet)
- [kube-rs](https://github.com/kube-rs/kube)
- [Vector Config API Issue #24020](https://github.com/vectordotdev/vector/issues/24020)
- [Vector CRD Issue #3018](https://github.com/timberio/vector/issues/3018)
