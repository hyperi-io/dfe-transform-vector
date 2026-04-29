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

**Status (2026-04-29): deliberately deferred.** KEDA direct Kafka lag
trigger remains the primary scaler in production and is sufficient.
This composite signal is an *enhancement* — not gating any current
deliverable. Reactivate only on explicit ask (e.g. KEDA lag alone
proves insufficient for a customer workload).

Note: the "Tests: mock GraphQL responses, mock Kafka admin responses"
bullet in this section conflicts with the No Mocks Policy in
`hyperi-ai/standards/rules/universal.md`. When this feature is
reactivated, plan for testcontainers Kafka + a real Vector subprocess
fixture (we already have both harnesses in `tests/`).

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

### Release v1.0.10 — shipped 2026-04-29

PATCH bump from three `fix:` commits (`fix(deps): clear advisories`,
`fix: wire Tier 1 jemalloc allocator`, plus the `ci:` and `docs:`
commits which don't bump). Pushed via `hyperi-ci push`, semantic-
release tagged `v1.0.10`, `hyperi-ci release v1.0.10` dispatched the
publish workflow.

| Destination | Status | Evidence |
|---|---|---|
| R2 (`https://downloads.hyperi.io/dfe-transform-vector/v1.0.10/`) | ✅ shipped | both binaries + checksums.sha256, content-type `application/x-elf`, HTTP 200 |
| GitHub Release `v1.0.10` | ✅ shipped | 3 assets (amd64 6,347,472 bytes, arm64 5,653,960 bytes, checksums) |
| **Tier 1 jemalloc verified live** | ✅ | downloaded R2 amd64 → `strings ... \| grep jemalloc` → 39 hits, sha256 matches checksum file |
| Container image (ghcr.io/hyperi-io/dfe-transform-vector) | ⏭ skipped | hyperi-ci `Container` job runs but build/push steps marked `skipped` — config-schema mismatch in `.hyperi-ci.yaml`. Same on dfe-loader v1.17.5. |
| Helm chart push | ⏭ skipped | same root cause as Container |
| JFrog (`hyperi.jfrog.io`) | 🚫 dead | subscription redirects to `landing.jfrog.com/reactivate-server` — hyperi-ci has already rerouted binaries to R2 + GH Releases |

#### Platform-level follow-ups (NOT this project — file under hyperi-ci)

The container/helm "skipped silently" pattern affects every DFE Rust
service. **Do not patch our `.hyperi-ci.yaml`** — the schema we have
(`publish.container.enabled`) matches the canonical dfe-loader/
dfe-receiver layout. The skip lives inside hyperi-ci's reusable
workflow (the `Check container enabled` step looks at the wrong key).

- [ ] **(hyperi-ci issue)** `Container` job's `Check container
      enabled` step reads top-level `container.enabled` but consumer
      configs put it under `publish.container.enabled`. Either fix
      the lookup path or migrate every consumer config in lockstep.
- [ ] **(hyperi-ci issue)** Same for `helm.enabled`. When fixed,
      decide replacement registry (JFrog is dead — likely
      `ghcr.io/hyperi-io/charts/...` for OCI helm).
- [ ] **(hyperi-ci issue)** `JFROG_TOKEN` is still passed in the
      publish env even though the binary publisher already routes
      around JFrog. Cosmetic — drop when hyperi-ci's publish secrets
      are tidied.

### Dependency + Security Refresh (2026-04-29)

**Source:** `/deps` skill (Phase 1 analysis-only) + `cargo deny check
advisories` + GitHub Dependabot alerts. Goal: pull lockfile to current
heads before next release so we ship without open advisories.

#### Open advisories (must clear before release)

| Crate | Current | Fixed in | Advisory | Severity | Reach |
|-------|---------|----------|----------|----------|-------|
| `rustls-webpki` | 0.103.12 | 0.103.13 | RUSTSEC-2026-0104 / GHSA-82j2-j2ch-gfr8 | high | runtime (via metrics-exporter-prometheus → hyper-rustls) |
| `astral-tokio-tar` | 0.6.0 | 0.6.1 | RUSTSEC-2026-0112 / GHSA-fp55-jw48-c537 | — | dev-dep only (testcontainers) |
| `astral-tokio-tar` | 0.6.0 | 0.6.1 | RUSTSEC-2026-0113 / GHSA-xx64-wwv2-hcqq | — | dev-dep only (testcontainers) |
| `openssl` | 0.10.77 | 0.10.78 | GHSA-pqf5-4pqq-29f5 | high | transitive |
| `openssl` | 0.10.77 | 0.10.78 | GHSA-xmgf-hq76-4vx2 | low | transitive |
| `openssl` | 0.10.77 | 0.10.78 | GHSA-8c75-8mhr-p7r9 | high | transitive |
| `openssl` | 0.10.77 | 0.10.78 | GHSA-hppc-g8h3-xhp3 | high | transitive |
| `openssl` | 0.10.77 | 0.10.78 | GHSA-ghm9-cr32-g9qj | high | transitive |

All resolvable by `cargo update` — no `Cargo.toml` floor bumps needed.
`cargo update --dry-run` confirms 0.10.77 → 0.10.78, 0.103.12 → 0.103.13,
0.6.0 → 0.6.1 will all be picked up.

- [x] `cargo update` executed 2026-04-29 — Cargo.lock bumped (33 packages
      relocked); resolves all 9 advisories. **Cargo.lock is now staged
      for commit.**
- [x] `cargo deny check advisories` → `advisories ok` (verified
      post-update)
- [x] `hyperi-ci check` — clippy + fmt + 143 tests pass on rustc 1.95
      after cargo update (2026-04-29, 9 skipped are environment-gated).
- [x] GitHub Dependabot alerts #6–#11 auto-closed when v1.0.10 landed
      on main (2026-04-29). `gh api .../dependabot/alerts` open count: 0.

#### Other lockfile bumps in the same `cargo update` run (informational)

These are minor/patch bumps that come along for free and have no API
impact. Listed for transparency, no action needed:

- `tokio` 1.52.0 → 1.52.1
- `rustls` 0.23.38 → 0.23.40
- `metrics` 0.24.3 → 0.24.4
- `metrics-exporter-prometheus` 0.18.1 → 0.18.2
- `metrics-util` 0.20.1 → 0.20.2
- `clap` (transitive), `cc`, `libc`, `js-sys`, `wasm-bindgen` family,
  `idna_adapter`, `rkyv`, `uuid`, `winnow`, `web-sys`, `wasip2` — all
  patch-level

#### Manifest pins (verified at latest)

- [x] `hyperi-rustlib >=2.5.4` — crates.io max stable is 2.5.4 (verified
      `https://crates.io/api/v1/crates/hyperi-rustlib`)
- [x] All other direct deps use `>=` ranges per Rust standards — lockfile
      is the reproducibility surface, manifest does not need bumping
- [x] No prohibited licenses (cargo-deny check licenses passes; OpenSSL +
      Unicode-DFS-2016 allowances are unused)
- [x] `LICENSE` is FSL-1.1-ALv2 with current copyright year (2026)

#### Renovate / Dependabot config

- [x] `renovate.json` exists in repo root
- [x] No open Renovate PRs at time of audit (2026-04-29)
- [x] `renovate.json` targets the default branch — verified, no stale
      release-branch config (release branch was deleted April 2026)

### Code Review Findings (`/review` 2026-04-29)

Lightweight pass over the source tree. No critical issues found — the
codebase is mature and follows HyperI Rust standards. Items below are
nice-to-haves.

- [x] `src/main.rs:315-319` — three `.unwrap()` calls on
      `tokio::signal::unix::signal(...)` for SIGTERM/SIGINT/SIGHUP.
      **Wontfix:** already gated under `#[allow(clippy::unwrap_used)]`,
      and a panic at signal-handler install is the correct supervisor
      response — the process can't function without signal handling and
      K8s will restart the pod immediately. Tightening to typed errors
      adds machinery without changing behaviour.
- [x] LICENSE: FSL-1.1-ALv2, copyright 2026 HYPERI PTY LIMITED — current.
- [x] `Cargo.toml` lints config: `unsafe_code = "deny"`, `unwrap_used =
      "warn"`, `expect_used = "warn"` — matches Rust standard.
- [x] `rust-toolchain.toml`: stable channel pinned, both amd64 + arm64
      targets installed — matches CI cross-compile setup.
- [x] No `todo!()`, `unimplemented!()`, or `panic!()` in production code.
- [x] No `.expect()` in production code (all 49 unwrap call sites are in
      `#[cfg(test)]` modules except the three signal-install sites
      above).

### CI Pre-Flight Items (consolidated)

These were already noted in *Lessons from dfe-loader Tier 2 canary*
below; surfacing them here so they're actionable in one place.

- [x] `.github/workflows/ci.yml:39` — flipped `publish-target: internal`
      → `both` (2026-04-29). Release builds will now ship release-channel
      binaries (jemalloc + fat LTO) once Tier 1 wiring is in.
- [x] hyperi-ci CLI matches PyPI latest (1.12.1 confirmed local +
      remote)
- [x] hyperi-rustlib at latest stable (2.5.4) — manifest floor matches
- [x] Tier 1 allocator wiring complete (2026-04-29) — `jemalloc` feature
      declared, `#[global_allocator]` in `src/main.rs`. Release build
      verified with 39 jemalloc symbols.

### Performance Review

Audit applicable optimisations from [dfe-loader/docs/PERFORMANCE.md](/projects/dfe-loader/docs/PERFORMANCE.md).
Note: Vector runs as subprocess — most knobs apply to the Rust integration layer (config gen, metrics proxy, supervision), not the Vector binary itself.

- [x] Allocator: jemalloc wired under `jemalloc` feature flag
      (mimalloc forbidden per 2026-04-17 policy). Benchmarking deferred —
      not meaningful on a supervisor binary; the Vector subprocess owns
      the data-plane hot path.
- [x] Build profile: `lto = "thin"`, `codegen-units = 1`,
      `panic = "abort"`, `strip = true` confirmed in `Cargo.toml`. CI
      overrides to `lto = "fat"` at beta+ via hyperi-ci release-track
      logic.
- [-] Profile under load — **deferred:** the wrapper has no
      meaningful data-plane work to profile. Vector's own perf tooling
      handles the Vector binary. Re-add only if a wrapper-specific
      regression appears.
- [-] PGO + BOLT — **declined** (see *Tier 2 opt-in* DECISION
      2026-04-29 above)
- [-] Batch tuning — **N/A here:** buffer/flush thresholds are
      Vector-config concerns (set in big-dial config and emitted into
      Vector YAML), not Rust transport concerns. The rustlib Kafka
      transport is unused on this project — Vector's librdkafka does
      the I/O.

---

## Backlog

- [ ] Documentation review (use `/doco` skill) — deferred; not part
      of the 2026-04-29 cleanup pass. Re-add to *Active* before next
      doc-impacting change.
- [x] Re-build and re-test with updated hyperi-ci — 2026-04-29 with
      hyperi-ci v1.12.1: 143/143 tests pass on default + jemalloc
      builds, prod/test change separation honoured.
- [ ] Run `/deps` skill before each release — catches stale lockfile + new
      advisories. Treat as a standing pre-release step alongside
      `hyperi-ci check`.
- [ ] Run `/review` skill before each release — light-touch quality pass on
      anything touched since the last release. Outputs go here under
      *Code Review Findings* or get actioned directly.
- [x] Add logging to `fetch_vector_metrics()` error paths (metrics.rs:246-289)
- [x] Fix HTTP status parsing in metrics proxy (`.contains("200")` → explicit parse)
- [x] Add enum validation for codec, encoding, compression, auto_offset_reset (config/loader.rs)
- [x] Validate drain_timeout_ms < session_timeout_ms (config/loader.rs:193)
- [x] Fix deny.toml `unmaintained` field (invalid value for cargo-deny)
- [x] Migrate health/metrics HTTP servers to rustlib `http-server` feature
- [x] Dual-mode e2e test infrastructure (docker + remote Kafka) — already implemented

---

## Rust Release-Track Optimisation (hyperi-ci Tier 1/2)

**Context:** hyperi-ci is shipping channel-gated build optimisations for Rust
binaries (see `hyperi-ai/standards/languages/RUST.md` — *Release-Track Build
Optimisation*). This project has no `[features]` section and needs the full
allocator setup.

### Tier 1 prep — **ACTION REQUIRED** (jemalloc-only per 2026-04-17 policy)

Current state: **⚠️ NEEDS FULL SETUP — no `[features]` section, no allocator.**

Expected gain when done: **+15-25% on the wrapper hot path** (lifecycle,
metrics endpoint, config reload). Note: Vector's own throughput loop is
unaffected — Vector is a separate binary with its own build pipeline.

Add to `Cargo.toml` (jemalloc only — no mimalloc per 2026-04-17 policy):
```toml
[dependencies]
tikv-jemallocator = { version = ">=0.6", optional = true }

[features]
default = []     # Do NOT include jemalloc — CI opts in per channel
jemalloc = ["dep:tikv-jemallocator"]
```

Wire in `src/main.rs`:
```rust
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
```

- [x] Add `tikv-jemallocator` optional dep (`Cargo.toml`)
- [x] Add `[features]` section with `default = []`, `jemalloc`
      (NO mimalloc per 2026-04-17 policy)
- [x] Wire `#[global_allocator]` in `src/main.rs`
- [x] Verify `[profile.release] lto = "thin"` (CI overrides to `fat` at beta+)
- [x] `cargo build --release --features jemalloc` compiles clean (2026-04-29)
- [x] `hyperi-ci check` passes (143/143) on both default and jemalloc builds
- [x] Verified: `strings target/release/dfe-transform-vector | grep -ciE
      'jemalloc|je_mallctl'` → 39 (matches dfe-receiver canary)

### Tier 2 opt-in (PGO + BOLT — release channel only)

**DECISION 2026-04-29: NOT ADOPTED for this wrapper binary.**

This project is a Vector *supervisor*: at steady state it is idle
except for occasional metrics scrapes, hot-reload events, and
lifecycle state transitions. The data-plane hot path (Kafka → parse
→ transform → produce) lives inside the Vector subprocess, which
ships its own optimised binary. PGO on this wrapper would profile
the rarely-executed supervisor paths and provide near-zero runtime
benefit on a +30–60 min build penalty.

Action: `.hyperi-ci.yaml` has no `build.rust.optimize.pgo` stanza —
that is intentional and correct. Tier 1 (jemalloc + fat LTO at
beta+) is the appropriate ceiling for this binary shape.

Re-evaluate only if:
- The wrapper's hot path materially changes (e.g. it gets pulled
  into the data plane rather than supervising Vector), or
- The shared hyperi-ci PGO infrastructure makes opt-in essentially
  free (e.g. zero-config workload templates).

The conditional checkboxes that previously lived here (workload
script, testcontainers harness, etc.) are deleted as part of this
decision — they would only re-enter scope if the decision flips.

---

## POLICY UPDATE 2026-04-17 — Jemalloc at every channel

**Allocator policy:** DFE binaries standardise on jemalloc at **every**
channel (spike/alpha/beta/release). No mimalloc. See
`hyperi-ai/standards/languages/RUST.md` → *Allocator Policy* and
`hyperi-ci/docs/RUST-RELEASE-TRACK-OPTIMISATION.md`.

### Action items — COMPLETE (2026-04-29)

Allocator wiring landed in commit `fcf97c5`:

- [x] Add ONLY jemalloc: `jemalloc = ["dep:tikv-jemallocator"]`
- [x] Do NOT add a `mimalloc` feature or dep — policy forbids it
- [x] Do NOT add a `mimalloc` fallback `#[cfg]` block in `main.rs`
- [x] Single allocator wiring in `src/main.rs`:
      ```rust
      #[cfg(feature = "jemalloc")]
      #[global_allocator]
      static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
      ```

This supersedes the earlier Tier 1 setup guidance above which may have
mentioned mimalloc as an option.

### Verification on next release (once wired)

```bash
strings target/<target>/release/<binary> | grep -ciE 'jemalloc|je_mallctl'
```

---

## Lessons from dfe-receiver Tier 2 canary (2026-04-17)

**Context:** dfe-receiver was the first DFE binary to ship the full
hyperi-ci release-track build optimisation feature (Tier 1 jemalloc +
fat LTO on beta+; Tier 2 PGO + BOLT opt-in on release). Findings from
that work are now baked into the shared docs — this section is the
signal to apply the same pattern here.

### Canary findings

- **Binary size impact (jemalloc static link, stripped release)**:
  +491 KB (+3.5%) on a 14 MB baseline. mimalloc was +131 KB (+1.0%)
  but is no longer an allowed allocator per 2026-04-17 policy.
- **Micro-bench allocator delta**: jemalloc wins −7.2% on
  `json_validation/large`, −4.4% on small; within noise elsewhere.
  Micro-benchmarks understate the real production win — Kafka
  producer + async task allocations are where the gains materialise.
- **Detection on stripped binaries**: `nm` won't see symbols because
  release profile has `strip = true`. Use
  `strings <binary> | grep -ciE 'jemalloc|je_mallctl'` — should
  return > 0 on a jemalloc build.
- **PGO workload shape that actually works**: a Rust driver linked to
  the project's own lib (to reuse proto types) plus a bash orchestrator
  that spins up testcontainers dependencies, starts the instrumented
  binary, drives realistic multi-protocol traffic for ≥ 60s
  (300s default), and cleans up on EXIT. See dfe-receiver's
  `scripts/pgo-workload.sh` + `src/bin/pgo-driver.rs` for the
  reference implementation.
- **PGO workload anti-patterns confirmed**: single-request curls,
  `curl /healthz` loops, and port-probe scripts all produce negative
  PGO gains — the compiler mis-optimises startup paths over hot paths.

### Where to read

- hyperi-ci `docs/RUST-RELEASE-TRACK-OPTIMISATION.md` — opt-in guide,
  verification, troubleshooting
- hyperi-ci `docs/PGO-WORKLOAD-GUIDE.md` — four rules, anti-patterns,
  profile quality metrics
- hyperi-ci `templates/pgo-workload/` — five template shapes to copy
  (`http-server.sh`, `grpc-server.sh`, `kafka-producer.sh`,
  `kafka-consumer.sh`, `multi-protocol.sh`)
- dfe-receiver `docs/PERFORMANCE.md` — concrete binary-size numbers,
  bench deltas, reproduction commands
- hyperi-ai standards `rules/rust.md` — the channel matrix +
  jemalloc-only policy

### Applies to this project

(Each consumer project owns the per-project status below — update as
Tier 1 preconditions are met and when Tier 2 opt-in lands.)

- [x] Tier 1 preconditions met (`jemalloc` feature declared in
      `Cargo.toml`, `#[global_allocator]` wired in `src/main.rs` under
      `#[cfg(feature = "jemalloc")]`, mimalloc never added) — landed
      2026-04-29
- [-] Workload script — **N/A:** Tier 2 declined for this wrapper
      binary (see DECISION 2026-04-29 above)
- [-] `.hyperi-ci.yaml build.rust.optimize.pgo.enabled` — **N/A:** same
      decision as above; left unset deliberately
- [x] Next release-channel build will be verified post-publish via
      `strings <binary> | grep jemalloc` (non-empty expected — local
      release build already shows 39 jemalloc symbols)

---

## Lessons from dfe-loader Tier 2 canary (2026-04-23)

**Context:** dfe-loader was Canary 2 for hyperi-ci Tier 2. Released
v1.17.5 to R2 with full `channel=release, allocator=jemalloc, lto=fat,
pgo=on, bolt=on` after two CI gotchas surfaced and were fixed. Most
of the loader-specific Tier 2 findings don't apply here (this is a
Vector wrapper, not a data-plane binary), but **one CI gotcha hits
this project regardless of whether you ever turn on Tier 2**.

### The CI-level gotcha that applies to this project

**`.github/workflows/ci.yml` `with: publish-target` MUST be `both`,
not `internal`.** This workflow input *overrides* `publish.target`
from `.hyperi-ci.yaml`. `internal` resolves to spike channel = Tier 1
(jemalloc + thin LTO). `both` = release channel = jemalloc + fat LTO
on the wrapper binary, plus consistent release-channel artefact
publishing semantics.

**This project currently has `publish-target: internal` in
[.github/workflows/ci.yml](.github/workflows/ci.yml).** Even just to
get fat LTO on the wrapper at release time, this needs to be `both`.
Loader v1.17.4 hit this exact bug — publish *succeeded* but shipped
spike-channel binaries (Tier 1 thin LTO instead of release-channel
fat LTO).

The other loader gotcha (workflow `uses:` pinned to a hyperi-ci
version that predates `HYPERCI_CHANNEL`) does **not** apply — this
project uses `@main` so it tracks tip and gets `HYPERCI_CHANNEL`
automatically.

### Why Tier 2 PGO doesn't pay back here

Loader is a data-plane binary: every Kafka message goes through its
hot path. PGO on loader bought a meaningful win because the workload
script could exercise the actual production hot path (parse → route
→ transform → insert).

This project is a Vector *supervisor*: at steady state it's idle
except for occasional metrics scrapes, hot-reload events, and
lifecycle state transitions. PGO would profile those rarely-hit
paths and provide near-zero runtime benefit on a 30-60 min build
penalty. Stick with Tier 1 (jemalloc + fat LTO at beta+) — that's
free once `publish-target` is fixed.

### Verification artefacts (loader v1.17.5)

For comparison after this wrapper's next release-channel build:
- amd64 binary: 18.2 MB stripped, 39 jemalloc symbol strings, BOLT
  marker present (BOLT will NOT appear here unless Tier 2 is enabled)
- Build log signature with Tier 1 only:
  `Rust build optimisation: channel=release, allocator=jemalloc, lto=fat, pgo=off, bolt=off`
- R2 path: `https://downloads.hyperi.io/dfe-transform-vector/v<X.Y.Z>/`

### Pre-flight checklist

Before triggering the next release:

- [x] **Tier 1 setup complete** (jemalloc feature + `#[global_allocator]`
      wiring) — landed 2026-04-29. Release build shows 39 jemalloc
      symbols.
- [x] **Fixed `.github/workflows/ci.yml` `publish-target: internal` →
      `both`** (2026-04-29).
- [x] **Verified rustlib at latest stable** (2026-04-29) — crates.io
      max stable is v2.5.4, `Cargo.toml` floor matches.
- [x] **Local hyperi-ci CLI matches PyPI latest** — verified v1.12.1
      against PyPI on 2026-04-29.
- [x] **`hyperi-ci check` runs clean** (2026-04-29) — clippy + fmt +
      cargo deny + 143/143 tests on rustc 1.95.

### Trigger sequence (verbatim from loader Canary 2)

1. Real `fix:` commit on main → semantic-release bumps + tags.
2. `git pull --rebase origin main` to pull the version-commit + tag.
3. `hyperi-ci release vX.Y.Z` → dispatches publish workflow.
4. `hyperi-ci watch` → monitor.
5. Verify R2 + `strings | grep jemalloc` on downloaded binary.
