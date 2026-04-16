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

- [x] 5.1 **Config file watcher** — poll-based (configurable interval, default 30s) on transform YAML directory and big-dial config
- [x] 5.2 **SIGHUP handler** — manual reload trigger
- [x] 5.3 **Reload workflow**:
  - [x] 5.3.1 Detect change (file watcher or SIGHUP)
  - [x] 5.3.2 Re-read big-dial config (safe components only — source/sink changes require restart)
  - [x] 5.3.3 Re-load transform YAMLs
  - [x] 5.3.4 Re-run DAG validation + `vector validate`
  - [x] 5.3.5 If valid: write new config dir, send SIGHUP to Vector child (native hot-reload)
  - [x] 5.3.6 If invalid: log error, increment metric, keep running with old config
- [x] 5.4 **Classify config changes**: safe (transforms only) vs unsafe (source/sink/brokers change → requires full restart)

## 6. Docker Image

- [x] 6.1 Dockerfile generated from `DeploymentContract` via `emit-dockerfile` subcommand
  - Runtime: `ubuntu:24.04`, single-stage (CI builds binary externally)
  - Vector binary COPY + data directories inserted via post-processing
- [x] 6.2 Vector binary sourced from official release tarball (pinned version, v0.48.0)
- [x] 6.3 Entrypoint: `/usr/local/bin/dfe-transform-vector`
- [x] 6.4 Vector at: `/usr/local/bin/vector`
- [x] 6.5 Health check: `HEALTHCHECK CMD curl -f http://localhost:9000/health/live || exit 1`
- [x] 6.6 Multi-arch build support via TARGETARCH → Vector arch mapping

## 7. Helm Chart

- [x] 7.1 Chart generated from `DeploymentContract` via `emit-chart` subcommand
- [x] 7.2 **Deployment** template (switched from StatefulSet — Vector data dir loss just re-reads from Kafka)
  - [x] 7.2.1 Container: `dfe-transform-vector` image
  - [x] 7.2.2 Ports: health (9000), metrics (9090), Vector API (8686)
  - [x] 7.2.3 Liveness/readiness probes: `/health/live`, `/health/ready`
  - [x] 7.2.4 Env vars from Secrets (KAFKA_SASL_USERNAME/PASSWORD via secretKeyRef)
- [x] 7.3 **ConfigMap** for big-dial config YAML
- [x] 7.4 **Service** (ClusterIP — health + metrics ports)
- [x] 7.5 **ServiceAccount**
- [x] 7.6 **KEDA ScaledObject** + TriggerAuthentication (Kafka lag + CPU)
- [x] 7.7 **HPA** (fallback when KEDA not available)
- [x] 7.8 **values.yaml** — sensible defaults from DeploymentContract
- [x] 7.9 **docker-compose.yaml** generated via `emit-compose` subcommand

## 8. dfe-core Integration

- [x] 8.1 New ArgoCD ApplicationSet pointing to this repo's chart (replaces `helm.vector.dev` source)
  - `dfe-core/gitOps/addons/argo_apps/dfe-transform-vector.yaml` — matrix generator (clusters × pipeline git files)
  - Scans for `pipelines/*/dfe-transform-vector-*.yaml` (coexists with old `vector-*.yaml`)
  - Sources: chart from `dfe-transform-vector.git`, common values + ExternalSecrets from dfe-core, pipeline values from pipelines repo
- [x] 8.2 Migrate existing pipeline values from dfe-core format to big-dial format
  - `dfe-core/gitOps/addons/helm/dfe-transform-vector/common.yaml` — shared values (image, resources, KEDA, init container, SASL/TLS defaults)
  - Migration guide: `docs/MIGRATION.md` — before/after format, what changes, what stays
  - Pipeline-specific values: just pipeline name, topics, group_id, sink topic, KEDA overrides
- [x] 8.3 ExternalSecrets stay as-is (Kafka SASL, Artifactory creds)
  - Copied to `dfe-core/gitOps/addons/helm/dfe-transform-vector/` — same format, same AWS Secrets Manager keys
  - Kustomize patches in ApplicationSet replace tenancy-specific secret keys
- [x] 8.4 KEDA TriggerAuthentication stays as-is
  - `keda-trigger-auth-kafka-credential.yaml` — same secret references
  - ScaledObject now references `config.source.brokers/topics/group_id` (was `config.kafka.*`)
- [x] 8.5 Karpenter node pools stay as-is (vector_node role)
  - Same `dedicated: vector` taint toleration in ApplicationSet inline values
  - Same `role: vector_node` node affinity
- [x] 8.6 Init container: keep Artifactory fetch pattern for transform YAMLs
  - Init container defined in `common.yaml` with same curl+unzip pattern
  - Fetches from `${ARTIFACTORY_VECTOR_TEMPLATES}/vector-artifacts/artifacts-${VERSION}.zip`
  - Mounts to `/etc/dfe/transforms` via `extraVolumes`/`extraVolumeMounts`
- [x] 8.7 Cutover plan: deploy alongside existing Vector pipelines, validate, switch traffic
  - Documented in `docs/MIGRATION.md` — parallel deployment, different consumer groups, validate, switch

## 9. Testing

- [x] 9.1 **Unit tests** (38 tests passing)
  - [x] 9.1.1 Config parsing — cascade, env var override, defaults
  - [x] 9.1.2 Source/sink YAML generation — SASL, TLS, compression, encoding
  - [x] 9.1.3 DAG wiring — auto-wire, explicit wire, broken refs, cycles, orphans
  - [x] 9.1.4 Config assembler — with/without transforms, directory cleanup
  - [x] 9.1.5 Lifecycle state — transitions, readiness, liveness
  - [x] 9.1.6 Backoff — doubling, cap, reset
  - [x] 9.1.7 Version parsing
- [x] 9.2 **Integration tests** (35 tests passing)
  - [x] 9.2.1 Config assembly — end-to-end with/without transforms, broken DAG, cyclic DAG
  - [x] 9.2.2 Config loading — YAML file, validation (missing topic, invalid SASL, invalid version_check), env overrides
  - [x] 9.2.3 Lifecycle — state drives readiness, subscriber updates
  - [x] 9.2.4 Metrics — register/encode, lifecycle state gauge
  - [x] 9.2.5 Health server — responds 200 when running, 503 when initialising
  - [x] 9.2.6 Metrics server — responds with Prometheus text format
- [x] 9.3 **Testcontainers** (Kafka) — full pipeline test: produce → transform → consume
  - `tests/e2e_kafka.rs` — starts Kafka via testcontainers (apache/kafka-native), assembles config, runs Vector subprocess, verifies transform applied
  - Run with: `cargo nextest run --test e2e_kafka --run-ignored all`
  - Requires Docker + Vector binary on PATH
- [x] 9.4 **Config fixture library** (9 tests passing)
  - Fixtures: `tests/fixtures/transforms/01-05`, `tests/fixtures/configs/minimal|with_sasl|with_transforms`
  - Tests: `tests/integration_fixtures.rs` — config loading, individual transform extraction, full chain DAG wiring
  - Fixed: test assertions used `c.kind == "transforms"` (plural) but `extract_components` trims to singular `"transform"`; added explicit `inputs:` to chain fixtures 02–05; fixed SASL mechanism format to `scram_sha_512`

## 9b. Buffer Configuration & Vector Validate Tests

- [x] 9b.1 **BufferConfig** — `src/config/loader.rs`: memory (max_events) and disk (max_size, min 256 MiB) modes, `when_full` (block/drop_newest)
- [x] 9b.2 **Buffer YAML generation** — `src/config/generate.rs`: `build_buffer_block()` emits buffer config in sink YAML
- [x] 9b.3 **Buffer validation** — `src/config/loader.rs`: type, when_full, disk requires max_size >= 268435488
- [x] 9b.4 **Global config generation** — `src/config/generate.rs`: `generate_global_yaml()` emits `data_dir` + `api` settings; assembler writes `00_global.yaml`
- [x] 9b.5 **VRL fixture fixes** — fixed all 5 transform fixtures for Vector 0.53.0 VRL compliance (dynamic paths, fallible ops, coalescing)
- [x] 9b.6 **Vector validate integration tests** — `tests/integration_vector_validate.rs`: 4 tests (memory, disk, drop_newest, default) assemble full 5-fixture chain + run `vector validate --no-environment`
- [x] 9b.7 **Buffer validation unit tests** — `tests/integration_config.rs`: 6 tests (invalid type, invalid when_full, missing max_size, too small, valid memory, valid disk)
- [x] 9b.8 **Chart + dfe-core buffer defaults** — `chart/values.yaml` and `dfe-core/common.yaml` updated with memory buffer defaults
- [x] 9b.9 **process.rs data_dir fix** — changed `--data-dir` CLI flag (doesn't exist) to `VECTOR_DATA_DIR` env var
- [x] 9b.10 **validate.rs data_dir fix** — added `VECTOR_DATA_DIR` env var to `vector validate` command

## 9c. Production Kafka Tuning

- [x] 9c.1 **SourceConfig production fields** — `auto_offset_reset`, `session_timeout_ms`, `commit_interval_ms`, `drain_timeout_ms`, `topic_lag_metric`, `librdkafka_options` (BTreeMap)
- [x] 9c.2 **SinkConfig production fields** — `BatchConfig` (max_events=10K, max_bytes, timeout_secs=1), `message_timeout_ms`, `socket_timeout_ms`, `librdkafka_options` (BTreeMap)
- [x] 9c.3 **Source YAML generator rewrite** — production librdkafka defaults baked in (cooperative-sticky, 10 MiB fetch, 100K pre-fetch queue, auto.commit false, 1 MiB socket buffer); auto-inject security.protocol from SASL/TLS config; user librdkafka_options override defaults
- [x] 9c.4 **Sink YAML generator rewrite** — production librdkafka defaults (8 MiB batch.size, 20ms linger, 10K batch.num.messages, acks=all, 1 GiB queue, zstd, nagle disabled); acknowledgements moved from source to sink (Vector 0.53+ deprecation); Vector-level batch config; healthcheck enabled
- [x] 9c.5 **Test fixes** — struct literals updated with `..Default::default()`, acknowledgements assertion moved to sink, unused imports cleaned
- [x] 9c.6 **Helm + config updates** — chart/values.yaml, deploy/helm/common.yaml, config.example.yaml updated with new fields
- [x] 9c.7 **dfe-core files relocated** — ArgoCD ApplicationSet and Helm values copied to deploy/ directory (dfe-core changes reverted)

## 9d. Central librdkafka Defaults (Git-Managed Config)

- [x] 9d.1 **Central config file in dfe-devex** — `shared/librdkafka.yaml.example` in dfe-devex git-managed config repo
  - Consumer profiles: production, devtest, low_latency
  - Producer profiles: production, exactly_once, low_latency, devtest
  - Follows same activation pattern as other dfe-devex configs (copy .example → .yaml)
- [x] 9d.2 **Rustlib fallback** — `src/config/kafka_defaults.rs` loads `$DFE_CONFIG_DIR/shared/librdkafka.yaml`, falls back to `hyperi_rustlib::kafka_config` constants
  - `OnceLock`-cached, loaded once on first access
  - `merge_layers()` handles 3-layer merge: base → service overrides → user `librdkafka_options`
- [x] 9d.3 **Transform-vector integration** — `generate.rs` uses `kafka_defaults::consumer_profile("production")` and `kafka_defaults::producer_profile("production")` + `merge_layers()`
- [x] 9d.4 **Document in /docs/LIBRDKAFKA.md** — full reference: every setting, justification, cascade, central config file, activation, code references

## 10. CI/CD

- [x] 10.1 CI pipeline matching dfe-loader pattern (ci.yml, publish.yml, semantic-release.yml via ci submodule)
- [x] 10.2 Cross-compile amd64 + arm64 (targets in .hyperi-ci.yaml)
- [x] 10.3 Docker image publish to JFrog (linux/amd64 + linux/arm64)
- [x] 10.4 Helm chart publish to JFrog
- [x] 10.5 semantic-release versioning from conventional commits (.releaserc.json, package.json)

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

---

## Completed (Metrics Migration)

- [x] Migrate metrics from `prometheus` crate to `metrics` crate + `MetricsManager`
- [x] Add `AppMetrics` (info, start_time, config_reloads) from dfe_groups
- [x] Add `DfeMetrics` dual-emit (`dfe_pipeline_ready` gauge)
- [x] Bump rustlib to >=1.18 with `metrics-dfe` feature
- [x] Rustlib capability review — bespoke metrics replaced with standard patterns

## Completed (Test Restructuring)

- [x] Restructured tests to HyperI testing standards (single-binary integration pattern)
- [x] Created `scripts/fetch-vector.sh` (auto-download Vector binary for tests)
- [x] Added `vector_binary_path()` OnceLock helper + `skip_if_no_vector!` macro
- [x] Added 28 new tests: smoke (14), deployment (5), metrics (9), reload (2), CLI (6), health JSON (2), edge cases (5)
- [x] AI trap audit: added malformed YAML, empty file, unknown fields, Reloading readiness, ShuttingDown state tests
- [x] DFE Metrics Survey (`~/DFE-METRICS-SURVEY.md`) — all 7 Rust apps inventoried

## Completed (rustlib v1.19 Migration)

- [x] Bumped rustlib to >=1.19 with `config-reload`, `http-server`, `version-check` features
- [x] Migrated health.rs from bespoke hyper to rustlib `HttpServer` (axum)
- [x] Migrated metrics.rs `serve_metrics` from bespoke hyper to rustlib `HttpServer` (axum)
- [x] Removed hyper/hyper-util/http-body-util direct deps (axum comes via rustlib)
- [x] Wired `VersionCheck::check_on_startup()` in `run_transform_service()`
- [x] SensitiveString — incompatible with figment serialize round-trip, documented as upstream fix needed

## Planned: Composite Scaling Pressure

Weighted scaling signal combining consumer lag + Vector buffer pressure + error rate.
Feature-gated under `scaling` — opt-in, KEDA direct Kafka lag trigger remains primary.

### Data Sources

1. **Consumer lag** (weight 0.5) — rdkafka admin client
   - Query committed offsets vs high watermark for `config.source.group_id`
   - Brokers, SASL/TLS from existing config (no new credentials)
   - rdkafka promoted from dev-dep to full dep (feature-gated)

2. **Buffer pressure** (weight 0.3) — Vector GraphQL API `:8686`
   - `vector_buffer_byte_size / buffer.max_size` for `dfe_sink` component
   - HTTP POST to `http://localhost:8686/graphql` with component query
   - Graceful fail: if Vector API unavailable, skip this component

3. **Error rate** (weight 0.2) — Vector GraphQL API `:8686`
   - `component_errors_total / events_in_total` over sliding window
   - Same GraphQL endpoint, same graceful-fail behaviour
   - Only `dfe_source` and `dfe_sink` components (not internal metrics)

### Implementation

- [ ] `src/scaling.rs` — `ScalingPoller` struct with configurable poll interval (default 30s)
- [ ] rdkafka admin client for consumer lag (reuse source config for brokers/SASL/TLS)
- [ ] Vector GraphQL client (raw HTTP POST, no graphql crate — just serde_json)
- [ ] `weighted_composite()` — normalise each source to 0-100, apply weights
- [ ] Emit `dfe_scaling_pressure` gauge (already registered via DfeMetrics)
- [ ] Feature gate: `scaling = ["dep:rdkafka"]` in Cargo.toml
- [ ] Graceful degradation: each source fails independently, remaining sources re-weight
- [ ] Tests: mock GraphQL responses, mock Kafka admin responses

### Metrics Emitted

| Metric | Type | Description |
|--------|------|-------------|
| `dfe_scaling_pressure` | gauge | Composite 0-100 (already registered via DfeMetrics) |
| `dfe_transform_vector_consumer_lag` | gauge | Sum of partition lags |
| `dfe_transform_vector_buffer_pressure` | gauge | 0.0-1.0 ratio |
| `dfe_transform_vector_error_rate` | gauge | Errors/events ratio over window |

### Risks

- Vector GraphQL schema is unversioned — field names may change between releases
- rdkafka as full dep adds ~2MB binary size and librdkafka runtime requirement
- Feature-gated mitigates both: disabled by default, KEDA direct lag still works

## Completed (rustlib v2.5 + Release Config)

- [x] Bumped hyperi-rustlib from >=2.4.6 to >=2.5.4 (no API changes, clean build)
- [x] Added missing `breaking: true → major` rule to `.releaserc.yaml` (matches reference configs)
- [x] Cleaned up stale remote branches (chore/merge-to-release, fix/merge-to-release, fix/merge-to-release-ga)
- [x] Code review + security review — no blockers

## Completed (Bug Fixes + Validation Hardening)

- [x] Config validation: codec, encoding, compression, auto_offset_reset enum validation
- [x] Config validation: drain_timeout_ms < session_timeout_ms relational check
- [x] Config validation: scaling.pressure_threshold bounded to [0.0, 1.0]
- [x] Config validation: SASL username required when SASL enabled
- [x] Metrics proxy: fixed HTTP status parsing (`.contains("200")` → explicit status code parse)
- [x] Metrics proxy: added trace logging to all error paths (connect, write, read, timeout, UTF-8)
- [x] Health server: added warn log when lifecycle channel closes
- [x] deny.toml: migrated to cargo-deny 0.19 format (removed deprecated fields)
- [x] Dependencies: updated all to latest (aws-lc-rs, clap, etc.)
- [x] Added 18 new validation tests (143 total, up from 125)

## Completed (E2E Test Coverage)

- [x] Metrics proxy e2e — spawns real Vector with `internal_metrics` → `prometheus_exporter`,
      verifies wrapper /metrics proxies actual Vector output. Validates the HTTP status parse
      fix and error-path logging against real responses.
- [x] Metrics proxy graceful-degradation e2e — verifies wrapper returns 200 with wrapper-only
      metrics when Vector is unreachable (regression guard for proxy fallback behaviour).
- [x] Kafka pipeline e2e — live-first with testcontainers fallback. Full pipeline: produce →
      Vector transform → consume, verifies remap transform actually applied end-to-end.
      In SASL mode, also asserts security.protocol injection in generated YAML.
- [x] Test cleanup: Vector uses `kill_on_drop(true)` (guaranteed SIGKILL on any exit path),
      topics have unique nanosecond suffixes, explicit delete on happy path. Testcontainers
      Kafka stops on fixture drop (RAII).

## Completed (Testcontainers Integration)

- [x] Added `testcontainers` + `testcontainers-modules` (kafka feature) as dev-deps
- [x] `KafkaFixture::acquire()` — live-first with automatic testcontainers fallback
- [x] Authenticated probe against live cluster (stale creds → testcontainers fallback)
- [x] `.env.example` updated — no more TEST_MODE; auto-detection explained

## Active

### Performance Review

Audit applicable optimisations from [dfe-loader/docs/PERFORMANCE.md](/projects/dfe-loader/docs/PERFORMANCE.md).
Note: Vector runs as subprocess — most knobs apply to the Rust integration layer (config gen, metrics proxy, supervision), not the Vector binary itself.

- [ ] Allocator: enable `jemalloc` or `mimalloc` feature, benchmark vs system glibc on representative workload
- [ ] Build profile: confirm `lto = "thin"`, `codegen-units = 1`, `panic = "abort"`, `strip = true` in release
- [ ] Profile under load (perf, flamegraph, jeprof) — record baseline for regression detection
- [ ] PGO + BOLT: evaluate ROI for production binary (10-20% + 5-15% gain)
- [ ] Batch tuning: validate buffer/flush thresholds align with rustlib Kafka transport (10K recv / 20K prefetch)

---

## Backlog

- [ ] Documentation review (use `/doco` skill)
- [ ] Re-build and re-test with updated hyperi-ci (prod/test change separation)
- [x] Add logging to `fetch_vector_metrics()` error paths (metrics.rs:246-289)
- [x] Fix HTTP status parsing in metrics proxy (`.contains("200")` → explicit parse)
- [x] Add enum validation for codec, encoding, compression, auto_offset_reset (config/loader.rs)
- [x] Validate drain_timeout_ms < session_timeout_ms (config/loader.rs:193)
- [x] Fix deny.toml `unmaintained` field (invalid value for cargo-deny)
- [x] Migrate health/metrics HTTP servers to rustlib `http-server` feature
- [x] Dual-mode e2e test infrastructure (docker + remote Kafka) — already implemented
