# DFE Transform Vector — Work Breakdown Structure

## Decisions Locked

- **Approach**: Option 2 — Rust integration layer (subprocess model)
- **Vector execution**: Subprocess (`tokio::process::Command`), NOT compiled crate
- **Helm chart**: Own chart in this repo, drop official `timberio/vector` chart
- **ArgoCD**: Keep existing ApplicationSet pattern from dfe-core, re-point to our chart
- **Config pattern**: Match dfe-loader's cascade (CLI → env → .env → YAML → defaults)

---

## 1. Project Scaffold

- [ ] 1.1 `cargo init` with workspace layout matching dfe-loader conventions
- [ ] 1.2 `Cargo.toml` — edition 2024, MSRV, common deps (`tokio`, `figment`, `clap`, `serde`, `serde_yaml_ng`, `tracing`, `hyper`, `prometheus`)
- [ ] 1.3 Shared lib dependency on `hyperi-rustlib` (transport abstraction, config, metrics patterns)
- [ ] 1.4 `.cargo/config.toml` — SIMD flags, cross-compile targets (amd64 + arm64)
- [ ] 1.5 Basic `main.rs` — CLI args, config load, tracing init, tokio runtime
- [ ] 1.6 `config.example.yaml` — annotated example with all big dials
- [ ] 1.7 `.gitignore`, `LICENSE`, `CLAUDE.md`

## 2. Config Engine

- [ ] 2.1 **Big-dial config schema** (`src/config/`)
  - [ ] 2.1.1 Define `PipelineConfig` struct — pipeline name, version
  - [ ] 2.1.2 Define `SourceConfig` struct — Kafka source big dials (brokers, topics, group_id, SASL)
  - [ ] 2.1.3 Define `SinkConfig` struct — Kafka sink big dials (brokers, topic, key_field, encoding)
  - [ ] 2.1.4 Define `TransformConfig` struct — file paths / directory for user-supplied YAML
  - [ ] 2.1.5 Define `VectorConfig` struct — binary path, data_dir, api_port, log level
  - [ ] 2.1.6 Define `HealthConfig`, `MetricsConfig`, `ScalingConfig`
- [ ] 2.2 **Config cascade** via figment (CLI → env `DFE_TRANSFORM_*` → .env → YAML → defaults)
- [ ] 2.3 **Source YAML generator** — takes `SourceConfig`, emits Vector Kafka source YAML with canonical label (`dfe_source`)
- [ ] 2.4 **Sink YAML generator** — takes `SinkConfig`, emits Vector Kafka sink YAML with canonical label (`dfe_sink`)
- [ ] 2.5 **Transform loader** — reads user-supplied YAML files from path list or directory, preserves order
- [ ] 2.6 **DAG wiring & validation** (`src/config/wiring.rs`)
  - [ ] 2.6.1 Parse all component labels and `inputs` references from loaded YAMLs
  - [ ] 2.6.2 Auto-wire: first transform gets `inputs: ["dfe_source"]` if not explicitly set
  - [ ] 2.6.3 Auto-wire: sink gets `inputs` pointing to last transform's label if not explicitly set
  - [ ] 2.6.4 Validate DAG — all `inputs` references resolve, no orphans, no cycles
  - [ ] 2.6.5 Handle the 2% case: user-supplied extra sources/sinks (pass through, validate refs)
- [ ] 2.7 **Config assembler** — merge source + transforms + sink + internal_metrics + prometheus_exporter into config directory
- [ ] 2.8 **Vector validate integration** — shell out to `vector validate --config-dir <path>`, parse exit code (0 = ok, 78 = invalid config), capture stderr for error messages
- [ ] 2.9 **Environment variable interpolation** — pass through to Vector's native `${VAR}` / `${VAR:-default}` support in generated YAML

## 3. Vector Subprocess Manager

- [ ] 3.1 **Process spawner** (`src/vector/process.rs`)
  - [ ] 3.1.1 `tokio::process::Command` with `--config-dir`, `--watch-config poll`
  - [ ] 3.1.2 `stdout(Stdio::inherit())`, `stderr(Stdio::inherit())` — Vector logs pass through to pod stdout
  - [ ] 3.1.3 Configurable Vector CLI args pass-through (log level, etc.)
- [ ] 3.2 **Signal forwarding**
  - [ ] 3.2.1 Wrapper is PID 1, traps SIGTERM + SIGINT
  - [ ] 3.2.2 Forward SIGTERM to Vector child process
  - [ ] 3.2.3 Wait for child exit with timeout (default 55s, leaves 5s for wrapper cleanup before K8s SIGKILL at 60s)
  - [ ] 3.2.4 If timeout expires, SIGKILL child and exit non-zero
- [ ] 3.3 **Crash recovery**
  - [ ] 3.3.1 Monitor child process exit status
  - [ ] 3.3.2 On unexpected exit: log, increment crash counter metric, restart with backoff
  - [ ] 3.3.3 Exponential backoff: 1s → 2s → 4s → 8s → ... → 60s cap
  - [ ] 3.3.4 Reset backoff on sustained healthy run (configurable, e.g., 5 minutes)
  - [ ] 3.3.5 Max crash count before giving up (configurable, default: unlimited — let K8s handle pod-level restart)
- [ ] 3.4 **Lifecycle states**: `Initializing` → `Validating` → `Starting` → `Running` → `Reloading` → `ShuttingDown` → `Crashed`

## 4. Observability

- [ ] 4.1 **Health server** (`src/health.rs`)
  - [ ] 4.1.1 HTTP server on configurable port (default 9000)
  - [ ] 4.1.2 `/healthz` — composite: wrapper state + poll Vector `/health` on :8686
  - [ ] 4.1.3 `/readyz` — ready only when Vector is running and healthy
  - [ ] 4.1.4 Return 200/503 with JSON body (`{"wrapper": "ok", "vector": "ok"}`)
- [ ] 4.2 **Metrics** (`src/metrics.rs`)
  - [ ] 4.2.1 Prometheus endpoint on configurable port (default 9090)
  - [ ] 4.2.2 Proxy Vector's `/metrics` from prometheus_exporter sink
  - [ ] 4.2.3 Add wrapper metrics:
    - `dfe_transform_vector_up` (gauge: 1 = running, 0 = down)
    - `dfe_transform_vector_crashes_total` (counter)
    - `dfe_transform_vector_restarts_total` (counter)
    - `dfe_transform_vector_config_reloads_total` (counter, label: success/failure)
    - `dfe_transform_vector_config_validation_errors_total` (counter)
    - `dfe_transform_vector_lifecycle_state` (gauge with state label)
    - `dfe_transform_vector_uptime_seconds` (gauge — Vector child process uptime)
- [ ] 4.3 **Structured logging** via `tracing` + `tracing-subscriber` (JSON format for K8s log aggregation)

## 5. Hot-Reload

- [ ] 5.1 **Config file watcher** — poll-based (configurable interval, default 30s) on transform YAML directory and big-dial config
- [ ] 5.2 **SIGHUP handler** — manual reload trigger
- [ ] 5.3 **Reload workflow**:
  - [ ] 5.3.1 Detect change (file watcher or SIGHUP)
  - [ ] 5.3.2 Re-read big-dial config (safe components only — source/sink changes require restart)
  - [ ] 5.3.3 Re-load transform YAMLs
  - [ ] 5.3.4 Re-run DAG validation + `vector validate`
  - [ ] 5.3.5 If valid: write new config dir, send SIGHUP to Vector child (native hot-reload)
  - [ ] 5.3.6 If invalid: log error, increment metric, keep running with old config
- [ ] 5.4 **Classify config changes**: safe (transforms only) vs unsafe (source/sink/brokers change → requires full restart)

## 6. Docker Image

- [ ] 6.1 Multi-stage Dockerfile:
  - Stage 1: Rust build (cargo build --release, cross-compile amd64 + arm64)
  - Stage 2: Fetch Vector binary (from official release or our registry)
  - Stage 3: Runtime — `debian:bookworm-slim`, copy both binaries, non-root user
- [ ] 6.2 Vector binary sourced from official release tarball (pinned version in Dockerfile ARG)
- [ ] 6.3 Entrypoint: `/usr/local/bin/dfe-transform-vector`
- [ ] 6.4 Vector at: `/usr/local/bin/vector`
- [ ] 6.5 Health check: `HEALTHCHECK CMD curl -f http://localhost:9000/healthz || exit 1`
- [ ] 6.6 Multi-arch build (amd64 + arm64) matching dfe-loader CI pattern

## 7. Helm Chart

- [ ] 7.1 Chart scaffold in `chart/` directory
- [ ] 7.2 **StatefulSet** template (always StatefulSet, no role switching needed)
  - [ ] 7.2.1 Container: `dfe-transform-vector` image
  - [ ] 7.2.2 Ports: health (9000), metrics (9090), Vector API (8686)
  - [ ] 7.2.3 Liveness probe: `/healthz` on health port
  - [ ] 7.2.4 Readiness probe: `/readyz` on health port
  - [ ] 7.2.5 `terminationGracePeriodSeconds: 65` (55s Vector drain + 5s wrapper cleanup + 5s buffer)
  - [ ] 7.2.6 Volume mounts: data_dir (PVC), config (ConfigMap), transform YAMLs (ConfigMap or emptyDir from init container)
  - [ ] 7.2.7 Env vars from Secrets (Kafka SASL, etc.) via `envFrom` / `env.valueFrom`
  - [ ] 7.2.8 Resource requests/limits (configurable via values)
  - [ ] 7.2.9 Topology spread constraints, node affinity, tolerations (configurable)
- [ ] 7.3 **ConfigMap** for big-dial config YAML
- [ ] 7.4 **Service** (ClusterIP — health + metrics ports)
- [ ] 7.5 **ServiceAccount** with IRSA annotation (configurable)
- [ ] 7.6 **PodMonitor** for Prometheus Operator
- [ ] 7.7 **PodDisruptionBudget** (optional)
- [ ] 7.8 **values.yaml** — big dials at top level, sensible defaults
- [ ] 7.9 ConfigMap checksum annotation on pod template (auto-restart on config change)

## 8. dfe-core Integration

- [ ] 8.1 New ArgoCD ApplicationSet pointing to this repo's chart (replaces `helm.vector.dev` source)
- [ ] 8.2 Migrate existing pipeline values from dfe-core format to big-dial format
- [ ] 8.3 ExternalSecrets stay as-is (Kafka SASL, Artifactory creds)
- [ ] 8.4 KEDA TriggerAuthentication stays as-is
- [ ] 8.5 Karpenter node pools stay as-is (vector_node role)
- [ ] 8.6 Init container: either keep Artifactory fetch (for transform YAMLs) or migrate to ConfigMap-based delivery
- [ ] 8.7 Cutover plan: deploy alongside existing Vector pipelines, validate, switch traffic

## 9. Testing

- [ ] 9.1 **Unit tests**
  - [ ] 9.1.1 Config parsing — all cascade levels, env var override, defaults
  - [ ] 9.1.2 Source/sink YAML generation — verify output matches Vector schema
  - [ ] 9.1.3 DAG wiring — auto-wire, explicit wire, broken refs, cycles, orphans
  - [ ] 9.1.4 Config assembler — multi-file merge, ordering
- [ ] 9.2 **Integration tests**
  - [ ] 9.2.1 `vector validate` on generated configs (requires Vector binary in test env)
  - [ ] 9.2.2 Subprocess lifecycle — start, health check, SIGTERM shutdown, crash restart
  - [ ] 9.2.3 End-to-end: config → assemble → validate → start Vector → health ok
- [ ] 9.3 **Testcontainers** (Kafka) — full pipeline test: produce → transform → consume
- [ ] 9.4 **Config fixture library** — example transform YAMLs for common patterns (remap, filter, route, reduce)

## 10. CI/CD

- [ ] 10.1 CI pipeline matching dfe-loader pattern (format, lint, test, build, publish)
- [ ] 10.2 Cross-compile amd64 + arm64
- [ ] 10.3 Docker image publish to Harbor
- [ ] 10.4 Helm chart publish to Harbor ChartMuseum (or OCI)
- [ ] 10.5 semantic-release versioning from conventional commits

---

## Implementation Order

**Phase 1 — MVP (config assembly + subprocess, no hot-reload)**
1.1–1.7, 2.1–2.9, 3.1–3.4, 4.1–4.3, 6.1–6.6, 9.1–9.2

**Phase 2 — Helm + deployment**
7.1–7.9, 8.1–8.7, 10.1–10.5

**Phase 3 — Hot-reload + hardening**
5.1–5.4, 9.3–9.4

**Phase 4 — Production cutover**
8.7 (parallel deploy, validate, switch)
