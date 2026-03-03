# DFE Transform Vector — Work Breakdown Structure

## Decisions Locked

- **Approach**: Option 2 — Rust integration layer (subprocess model)
- **Vector execution**: Subprocess (`tokio::process::Command`), NOT compiled crate
- **Helm chart**: Own chart in this repo, drop official `timberio/vector` chart
- **ArgoCD**: Keep existing ApplicationSet pattern from dfe-core, re-point to our chart
- **Config pattern**: Match dfe-loader's cascade (CLI → env → .env → YAML → defaults)

---

## 1. Project Scaffold

- [x] 1.1 `cargo init` with workspace layout matching dfe-loader conventions
- [x] 1.2 `Cargo.toml` — edition 2024, MSRV, common deps (`tokio`, `figment`, `clap`, `serde`, `serde_yaml_ng`, `tracing`, `hyper`, `prometheus`)
- [x] 1.3 Shared lib dependency on `hyperi-rustlib` (commented out — Artifactory token expired)
- [x] 1.4 `.cargo/config.toml` — SIMD flags, cross-compile targets (amd64 + arm64)
- [x] 1.5 Basic `main.rs` — CLI args, config load, tracing init, tokio runtime
- [x] 1.6 `config.example.yaml` — annotated example with all big dials
- [x] 1.7 `.gitignore`, `LICENSE`, `CLAUDE.md`

## 2. Config Engine

- [x] 2.1 **Big-dial config schema** (`src/config/loader.rs`)
  - [x] 2.1.1 `PipelineConfig` — pipeline name
  - [x] 2.1.2 `SourceConfig` — Kafka source (brokers, topics, group_id, SASL, TLS, decoding)
  - [x] 2.1.3 `SinkConfig` — Kafka sink (brokers, topic, key_field, encoding, compression, SASL, TLS)
  - [x] 2.1.4 `TransformConfig` — dir or file list for user-supplied YAML
  - [x] 2.1.5 `VectorConfig` — binary, data_dir, api_address, log_level, version, version_check
  - [x] 2.1.6 `HealthConfig`, `MetricsConfig`, `LoggingConfig`, `ScalingConfig`
- [x] 2.2 **Config cascade** via figment (`DFE_TRANSFORM_*` env vars with `__` nesting + flat overrides)
- [x] 2.3 **Source YAML generator** (`src/config/generate.rs`) — emits `sources.dfe_source` with Kafka, SASL, TLS, acknowledgements, cooperative-sticky
- [x] 2.4 **Sink YAML generator** (`src/config/generate.rs`) — emits `sinks.dfe_sink` with Kafka, key_field, compression, SASL, TLS
- [x] 2.5 **Transform loader** (`src/config/transforms.rs`) — reads YAML from dir (sorted) or file list (ordered)
- [x] 2.6 **DAG wiring & validation** (`src/config/wiring.rs`)
  - [x] 2.6.1 Parse component labels and `inputs` from loaded YAMLs
  - [x] 2.6.2 Auto-wire: first transform gets `inputs: ["dfe_source"]` if not set
  - [x] 2.6.3 Auto-wire: discover terminal transforms, wire as sink inputs
  - [x] 2.6.4 Validate: undefined input refs, orphans, cycles (DFS colouring)
  - [x] 2.6.5 Extra sources/sinks pass through (validated but not auto-wired)
- [x] 2.7 **Config assembler** (`src/config/assembler.rs`) — writes `00_source.yaml`, `50_transforms/`, `90_sink.yaml`, `99_observability.yaml`
- [x] 2.8 **Vector validate integration** (`src/config/validate.rs`) — `vector validate --config-dir`, version check (strict/warn/disabled)
- [x] 2.9 **Environment variable interpolation** — `${VAR}` syntax preserved in generated YAML, Vector interpolates at runtime

## 3. Vector Subprocess Manager

- [x] 3.1 **Process spawner** (`src/vector/process.rs`)
  - [x] 3.1.1 `tokio::process::Command` with `--config-dir`, `--watch-config poll`
  - [x] 3.1.2 `stdout(Stdio::inherit())`, `stderr(Stdio::inherit())`
  - [x] 3.1.3 `VECTOR_LOG`, `VECTOR_API_ADDRESS`, `--data-dir` pass-through
- [x] 3.2 **Signal forwarding**
  - [x] 3.2.1 SIGTERM + SIGINT handler in main.rs
  - [x] 3.2.2 Forward SIGTERM to Vector child
  - [x] 3.2.3 Wait for exit with 55s timeout
  - [x] 3.2.4 SIGKILL on timeout
- [x] 3.3 **Crash recovery**
  - [x] 3.3.1 Monitor child exit status in lifecycle loop
  - [x] 3.3.2 Log + restart on unexpected exit
  - [x] 3.3.3 Exponential backoff: 1s → 2s → 4s → ... → 60s cap
  - [x] 3.3.4 Reset backoff after 5 minutes sustained healthy run
  - [x] 3.3.5 Unlimited restarts (K8s handles pod-level restart)
- [x] 3.4 **Lifecycle states** (`src/vector/lifecycle.rs`): `Initialising` → `Validating` → `Starting` → `Running` → `Reloading` → `ShuttingDown` → `Crashed`

## 4. Observability

- [x] 4.1 **Health server** (`src/health.rs`)
  - [x] 4.1.1 HTTP server on configurable port (default 9000)
  - [x] 4.1.2 `/health/live` — 200 if alive, 503 if crashed
  - [x] 4.1.3 `/health/ready` — 200 if Vector running, 503 otherwise
  - [x] 4.1.4 JSON body with status and lifecycle state
- [x] 4.2 **Metrics** (`src/metrics.rs`)
  - [x] 4.2.1 Prometheus endpoint on configurable port (default 9090)
  - [x] 4.2.2 Wrapper metrics: `up`, `crashes_total`, `restarts_total`, `config_reloads_total`, `config_validation_errors_total`, `lifecycle_state`, `uptime_seconds`
  - [x] 4.2.3 Proxy Vector's `/metrics` from prometheus_exporter sink on :9598 (best-effort, 2s/5s timeout)
- [x] 4.3 **Structured logging** — JSON/text via tracing-subscriber, configurable level and format

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

- [x] 6.1 Multi-stage Dockerfile:
  - Stage 1: Rust build (cargo build --release)
  - Stage 2: Fetch Vector binary (from official release tarball)
  - Stage 3: Runtime — `debian:bookworm-slim`, copy both binaries, non-root user
- [x] 6.2 Vector binary sourced from official release tarball (pinned version in Dockerfile ARG, v0.48.0)
- [x] 6.3 Entrypoint: `/usr/local/bin/dfe-transform-vector`
- [x] 6.4 Vector at: `/usr/local/bin/vector`
- [x] 6.5 Health check: `HEALTHCHECK CMD curl -f http://localhost:9000/health/live || exit 1`
- [x] 6.6 Multi-arch build support via TARGETARCH → Vector arch mapping

## 7. Helm Chart

- [ ] 7.1 Chart scaffold in `chart/` directory
- [ ] 7.2 **StatefulSet** template (always StatefulSet, no role switching needed)
  - [ ] 7.2.1 Container: `dfe-transform-vector` image
  - [ ] 7.2.2 Ports: health (9000), metrics (9090), Vector API (8686)
  - [ ] 7.2.3 Liveness probe: `/health/live` on health port
  - [ ] 7.2.4 Readiness probe: `/health/ready` on health port
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

- [x] 9.1 **Unit tests** (32 tests passing)
  - [x] 9.1.1 Config parsing — cascade, env var override, defaults
  - [x] 9.1.2 Source/sink YAML generation — SASL, TLS, compression, encoding
  - [x] 9.1.3 DAG wiring — auto-wire, explicit wire, broken refs, cycles, orphans
  - [x] 9.1.4 Config assembler — with/without transforms, directory cleanup
  - [x] 9.1.5 Lifecycle state — transitions, readiness, liveness
  - [x] 9.1.6 Backoff — doubling, cap, reset
  - [x] 9.1.7 Version parsing
- [x] 9.2 **Integration tests** (16 tests passing)
  - [x] 9.2.1 Config assembly — end-to-end with/without transforms, broken DAG, cyclic DAG
  - [x] 9.2.2 Config loading — YAML file, validation (missing topic, invalid SASL, invalid version_check), env overrides
  - [x] 9.2.3 Lifecycle — state drives readiness, subscriber updates
  - [x] 9.2.4 Metrics — register/encode, lifecycle state gauge
  - [x] 9.2.5 Health server — responds 200 when running, 503 when initialising
  - [x] 9.2.6 Metrics server — responds with Prometheus text format
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
