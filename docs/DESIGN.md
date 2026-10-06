# DFE Transform Vector — Design Document

## 1. Purpose

dfe-transform-vector wraps Vector.dev as a managed subprocess to provide **Kafka → transform → Kafka** pipelines that are first-class citizens in the DFE platform — indistinguishable from dfe-loader and dfe-receiver from dfe-engine's management, GitOps, and observability perspective.

Vector is powerful but opaque. This wrapper makes it behave like every other DFE service: same config registry, same health contract, same metrics shape, same scaling signals, same Helm compilation, same Argo CD lifecycle.

---

## 2. Architecture

```text
┌──────────────────────────────────────────────────────────────────┐
│  dfe-engine (Python)                                             │
│                                                                  │
│  ServicePlugin("transform-vector")                               │
│  ├─ ServiceDescriptor      (ports, health paths, kafka role)     │
│  ├─ TransformVectorConfig  (Pydantic — big dials)                │
│  ├─ DeploymentConfig       (Pydantic — K8s sizing)               │
│  ├─ validate_config()      (cross-field validation)              │
│  └─ HelmValuesCompiler     (generates values.yaml)               │
│                                                                  │
│  Config Registry: config_directory/transform-vector-*.yaml       │
│  Deploy Registry: deployment_configs/transform-vector-*.yaml     │
└──────────────────────┬───────────────────────────────────────────┘
                       │ compiled Helm values + Argo CD Application
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│  K8s Pod (Deployment)                                            │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  dfe-transform-vector (Rust, PID 1)                        │  │
│  │                                                            │  │
│  │  ┌─────────────┐  ┌──────────────┐  ┌──────────────────┐  │  │
│  │  │ Config      │  │ Process      │  │ Observability    │  │  │
│  │  │ Engine      │  │ Manager      │  │ Server           │  │  │
│  │  │             │  │              │  │                  │  │  │
│  │  │ • Load big  │  │ • Spawn      │  │ • /livez         │  │  │
│  │  │   dials     │  │   vector     │  │ • /readyz        │  │  │
│  │  │ • Generate  │  │ • Signal     │  │ • /metrics       │  │  │
│  │  │   source/   │  │   forwarding │  │ • scaling_       │  │  │
│  │  │   sink YAML │  │ • Crash      │  │   pressure       │  │  │
│  │  │ • Load user │  │   recovery   │  │                  │  │  │
│  │  │   transforms│  │ • Health     │  │                  │  │  │
│  │  │ • Wire DAG  │  │   polling    │  │                  │  │  │
│  │  │ • Validate  │  │              │  │                  │  │  │
│  │  │ • Assemble  │  │              │  │                  │  │  │
│  │  └──────┬──────┘  └──────┬───────┘  └──────────────────┘  │  │
│  │         │                │                                 │  │
│  │         │  config dir    │  child process                  │  │
│  │         ▼                ▼                                 │  │
│  │  ┌────────────────────────────────────────────────────┐    │  │
│  │  │  vector (subprocess)                                │    │  │
│  │  │  --config-dir /var/run/vector/config/               │    │  │
│  │  │  --watch-config --watch-config-method poll          │    │  │
│  │  │                                                     │    │  │
│  │  │  Kafka source ──► transforms ──► Kafka sink         │    │  │
│  │  │            ──► internal_metrics ──► prometheus_exp   │    │  │
│  │  └────────────────────────────────────────────────────┘    │  │
│  └────────────────────────────────────────────────────────────┘  │
│                                                                  │
│  Volumes:                                                        │
│  • /etc/dfe-transform-vector/config.yaml (ConfigMap, big dials)  │
│  • /etc/dfe-transform-vector/transforms/ (empty dir from image)  │
│  • /var/run/vector/config/ (image dir — assembled Vector config) │
└──────────────────────────────────────────────────────────────────┘
```

---

## 3. dfe-engine Integration

### 3.1 ServiceDescriptor

```python
descriptor = ServiceDescriptor(
    name="transform-vector",
    display_name="DFE Transform - Vector",
    image="ghcr.io/hyperi-io/dfe-transform-vector",
    default_port=9090,
    metrics_port=9090,
    kafka_role=KafkaRole.BOTH,
    consumer_group="dfe-transform-vector",
    liveness_paths=("/livez",),
    readiness_paths=("/readyz",),
    description="Kafka-to-Kafka transform pipelines powered by Vector.dev",
)
```

This is identical in shape to dfe-loader and dfe-receiver descriptors. dfe-engine discovers it via entry point and treats it as any other service.

### 3.2 Config Model (Pydantic — dfe-engine side)

```python
class TransformVectorConfig(BaseServiceConfig):
    """Runtime configuration — the 'big dials'."""

    model_config = ConfigDict(extra="forbid")

    # Universal (inherited pattern)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    # Pipeline identity
    pipeline: PipelineConfig          # name, version

    # Kafka source (input)
    source: SourceKafkaConfig         # brokers, topics, group_id, sasl, tls

    # Kafka sink (output)
    sink: SinkKafkaConfig             # brokers, topic, key_field, encoding

    # Transform files
    transforms: TransformFilesConfig  # file list or directory path

    # Vector subprocess
    vector: VectorProcessConfig       # binary path, data_dir, api_port, log_level

    # Scaling
    scaling: ScalingConfig            # pressure_threshold
```

Shared models (`MetricsConfig`, `LoggingConfig`, `SaslConfig`, `KafkaTlsConfig`) are reused from `dfe_engine.services.models.common` — same as dfe-loader and dfe-receiver.

There is no DLQ and no `dlq` config. A record no retry would get through is dropped and counted in `pipeline_dead_letters_dropped_total`: `reason="too_large"` for one over the next stage's message-size ceiling, `reason="rejected"` for one the sink refused for good.

### 3.3 ServicePlugin

```python
plugin = ServicePlugin(
    descriptor=descriptor,
    config_class=TransformVectorConfig,
    deployment_class=TransformVectorDeploymentConfig,
    validate_config=validate_transform_vector,
    sizing_overrides={
        "xs":     {"vector": {"memory_limit": "512Mi"}},
        "small":  {"vector": {"memory_limit": "1Gi"}},
        "medium": {"vector": {"memory_limit": "2Gi"}},
        "large":  {"vector": {"memory_limit": "4Gi"}},
        "xlarge": {"vector": {"memory_limit": "8Gi"}},
    },
    keda_defaults={
        "min_replicas": 2,
        "max_replicas": 8,
        "kafka_trigger": {
            "consumer_group": "dfe-transform-vector",
            "lag_threshold": 1000,
        },
    },
    config_template_overrides={
        "production": {
            "source": {"sasl": {"enabled": True, "mechanism": "scram_sha_512"},
                        "tls": {"enabled": True}},
            "sink":   {"sasl": {"enabled": True, "mechanism": "scram_sha_512"},
                        "tls": {"enabled": True}},
        },
        "k8s": {
            "source": {"brokers": ["kafka-bootstrap.kafka.svc.cluster.local:9092"]},
            "sink":   {"brokers": ["kafka-bootstrap.kafka.svc.cluster.local:9092"]},
        },
    },
)
```

### 3.4 Entry Point

```toml
# In dfe-engine's pyproject.toml or a separate plugin package
[project.entry-points."dfe_engine.services"]
transform-vector = "dfe_transform_vector_plugin:plugin"
```

### 3.5 What This Gives Us

Once registered, dfe-engine automatically provides:

| Capability | How |
|---|---|
| Config CRUD | `registry.get_config("transform-vector", "production")` |
| Config validation | `registry.validate("transform-vector", {...})` |
| Git-aware history | `registry.get_config_history(...)` |
| Helm values compilation | `compiler.compile_all()` includes transform-vector |
| Argo CD Application CRDs | Generated alongside loader and receiver |
| KEDA ScaledObject | Generated from keda_defaults + deployment config |
| T-shirt sizing | xs/small/medium/large/xlarge resource presets |
| Environment profiles | dev/staging/production overrides |
| Multi-instance | transform-vector-syslog, transform-vector-netflow, etc. |

---

## 4. Health & Observability Contract

### 4.1 Endpoints

All DFE services expose the same three endpoints, on the one ops port, so a pod
has a single answer to "are you ready". dfe-transform-vector is no exception.

| Endpoint | Port | Purpose | Response |
|---|---|---|---|
| `GET /livez` | 9090 | K8s liveness + startup probes | `200 OK` if the supervisor process is running |
| `GET /readyz` | 9090 | K8s readiness probe | `200 OK` only while the Vector child process is up |
| `GET /metrics` | 9090 | Prometheus scrape | Wrapper metrics + proxied Vector metrics |

### 4.2 Liveness vs Readiness

```text
/livez  → always 200 if the Rust process is running (fast, no deps)
/readyz → 200 only when the lifecycle is Running or Reloading, which means:
                 1. Config loaded, assembled and `vector validate`-clean
                 2. The Vector child was spawned and still existed 500ms later
                 3. Not in crash-recovery backoff
```

The supervisor publishes its lifecycle into scalo's health registry, which the
ops listener consults per request -- so `/readyz` tracks the subprocess rather
than the supervisor that outlives it.

A spawn returning success is not a running subprocess. The lifecycle only
reaches `Running` once the child has survived a short settle window
(`SPAWN_SETTLE`, 500ms), so a crash-on-start (bad argv, unreadable config)
reports 503 instead of a healthy start.

**What the settle window does NOT prove.** The check is `try_wait()` returning
"still running" at t+500ms -- the process EXISTS. Vector's own startup routinely
takes longer than that, so `/readyz` can answer 200 while Vector is still
wiring up its topology and consuming nothing. The signal for "carrying
traffic" is the sink's progress in Vector's own metrics: `/readyz` also reports
not ready while the sink holds records and has delivered none of them for
`metrics.sink_stall_secs` (60 by default). Vector's API is off by default and,
when turned on, binds loopback only.

During config reload, readiness stays healthy -- but only if it was healthy
already. `Reloading` counts as ready, so it is entered by compare-and-set from
`Running`: a transform change landing mid crash-backoff leaves the `Crashed`
state alone rather than advertising a subprocess that is not there. During crash
recovery, readiness returns 503 until Vector restarts successfully.

### 4.3 Metrics

**Wrapper metrics** (emitted by the Rust binary):

```text
# DFE platform standard
dfe_pipeline_ready 1                          # 1=ready, 0=not ready

# Process lifecycle
dfe_transform_vector_lifecycle_state{state="running"} 1
dfe_transform_vector_uptime_seconds 3600

# Subprocess health
dfe_transform_vector_crashes_total 0
dfe_transform_vector_restarts_total 1

# Config management
dfe_transform_vector_config_reloads_total{result="success"} 5
dfe_transform_vector_config_reloads_total{result="failure"} 0
dfe_transform_vector_config_validation_errors_total 0
```

Vector, not the wrapper, owns the Kafka client, so the record counters
(`records_received_total`, `records_processed_total`, `records_delivered_total`)
are derived from Vector's own component counters by the scrape below.

**Merged Vector metrics** (scraped from Vector's `prometheus_exporter` on
loopback `metrics.vector_metrics_address`, registered on the same registry
:9090 serves and scalo pushes over OTLP):

```text
# Names and labels as Vector emits them
vector_component_received_events_total{component_id="dfe_source",component_type="kafka"} 150000
vector_component_sent_events_total{component_id="dfe_sink",component_kind="sink"} 149950
vector_component_errors_total{component_id="parse"} 50
vector_buffer_byte_size{component_id="dfe_sink"} 1048576
# ... every other Vector internal metric
```

Counters register as counters via `absolute`, so a Vector restart holds the
totals rather than reporting a reset the pod did not have. A histogram arrives
pre-aggregated, so each cumulative `_bucket` re-registers as a counter carrying
its `le` label: the metrics crate's `Histogram` takes observations, and
replaying buckets through it would change the numbers.

One scrape of :9090 therefore carries wrapper state and Vector pipeline state,
and 9598 stays a loopback debug surface with no reader outside the pod.

### 4.4 Scaling

KEDA scales based on Kafka consumer lag (ScaledObject targets the consumer group
directly). The wrapper exposes `dfe_pipeline_ready` as a basic readiness signal.
Composite scaling pressure (weighted Kafka lag + memory + error rate) is planned
for a future release via the scalo `scaling` feature.

---

## 5. Config Engine

### 5.1 Big-Dial Config (Rust side)

The Rust binary loads its own config via the standard scalo 7-layer cascade:

```text
1. CLI args                              (highest priority)
2. Env vars (DFE_TRANSFORM_VECTOR_*)
3. .env file
4. settings.{env}.yaml
5. settings.yaml
6. defaults.yaml
7. Hard-coded defaults                   (lowest priority)
```

```yaml
# /etc/dfe-transform-vector/config.yaml — the big dials
pipeline:
  name: syslog-enrichment

source:
  brokers: "${KAFKA_BROKERS}"
  topics:
    - raw_syslog_land
  group_id: "dfe-transform-vector-${PIPELINE_NAME}"
  sasl:
    mechanism: scram_sha_512
    # A mounted Secret holding `username` and `password` files. Vector reads
    # them through its directory secret backend; it expands no ${VAR}.
    secret_dir: /var/run/secrets/dfe-kafka
  tls:
    enabled: true

sink:
  brokers: "${KAFKA_BROKERS}"
  topic: enriched_syslog_land
  key_field: ".org_id"
  encoding: json
  compression: zstd

transforms:
  dir: /etc/dfe-transform-vector/transforms

vector:
  binary: /usr/local/bin/vector
  data_dir: /var/lib/vector
  api_enabled: false          # loopback only when turned on
  log_level: info

metrics:
  address: "0.0.0.0:9090"   # also serves /livez and /readyz

scaling:
  pressure_threshold: 0.8
```

### 5.2 Config Assembly

The config engine takes the big dials and produces a Vector config directory:

```text
/var/run/vector/config/
  00_global.yaml          # data_dir, api switch, SASL secret backends
  00_source.yaml          # Generated from source big dials
  50_000_parse.yaml       # User transforms, flat (Vector --config-dir doesn't recurse)
  50_001_enrich.yaml      # Prefixed with 50_NNN_ for stable sort order
  50_002_filter.yaml
  90_sink.yaml            # Generated from sink big dials
  99_observability.yaml   # Generated: internal_metrics + prometheus_exporter
```

**Generated source (`00_source.yaml`):**

```yaml
sources:
  dfe_source:
    type: kafka
    bootstrap_servers: "kafka-1:9092,kafka-2:9092"
    topics:
      - raw_syslog_land
    group_id: dfe-transform-vector-syslog-enrichment
    decoding:
      codec: json
    sasl:
      enabled: true
      mechanism: SCRAM-SHA-512
      username: SECRET[dfe_source_sasl.username]
      password: SECRET[dfe_source_sasl.password]
    tls:
      enabled: true
    librdkafka_options:
      partition.assignment.strategy: cooperative-sticky
```

**Generated sink (`90_sink.yaml`):**

```yaml
transforms:
  dfe_size_cap:                         # drops a record over message.max.bytes
    type: filter
    inputs: ["<last_transform_label>"]  # Auto-wired
    condition: length(encode_json(.)) <= 999872
sinks:
  dfe_sink:
    type: kafka
    inputs: ["dfe_size_cap"]
    bootstrap_servers: "kafka-1:9092,kafka-2:9092"
    topic: enriched_syslog_land
    key_field: ".org_id"
    encoding:
      codec: json
    compression: zstd
    message_timeout_ms: 0               # an outage holds rather than rejects
    sasl:
      enabled: true
      mechanism: SCRAM-SHA-512
      username: SECRET[dfe_sink_sasl.username]
      password: SECRET[dfe_sink_sasl.password]
    tls:
      enabled: true
    librdkafka_options:
      enable.idempotence: "true"
```

**Generated observability (`99_observability.yaml`):**

```yaml
sources:
  internal_metrics:
    type: internal_metrics

sinks:
  prometheus_exporter:
    type: prometheus_exporter
    inputs:
      - internal_metrics
    address: "127.0.0.1:9598"   # metrics.vector_metrics_address
```

### 5.3 DAG Auto-Wiring

The wrapper understands the pipeline topology and wires components automatically:

1. **Source label is always `dfe_source`** — canonical, predictable
2. **Sink label is always `dfe_sink`** — canonical, predictable
3. **First transform**: if `inputs` contains `"source"` or `"dfe_source"`, keep it; otherwise inject `inputs: ["dfe_source"]`
4. **Intermediate transforms**: left as authored (user controls the chain)
5. **Last transform**: its label is discovered and injected as `dfe_sink.inputs`
6. **Extra sources/sinks** (the 2% case): detected and passed through unchanged, references validated

**Validation rules:**

- All `inputs` references must resolve to a defined component
- No orphaned components (every component must be reachable from a source)
- No cycles in the DAG
- At least one path from `dfe_source` to `dfe_sink`
- `vector validate --config-dir` as final gate (catches VRL syntax, type mismatches)

### 5.4 Hot-Reload

```text
Config change detected (file watcher or SIGHUP)
  │
  ├─ Transform file change (safe)
  │   ├─ Re-read transform YAMLs
  │   ├─ Re-run DAG wiring + validation
  │   ├─ Check every enrichment table file can be read, then `vector validate`
  │   ├─ If valid: write new config dir → SIGHUP to Vector child
  │   ├─ Read vector_reloaded_total / component_errors_total{error_code} for the outcome
  │   ├─ If invalid or refused: log error, increment metric, put back the running config dir
  │   └─ Emit config_reloads_total{result=success|error|unconfirmed}
  │
  └─ Any other config change (unsafe — requires pod restart)
      ├─ Log warning: "config change requires pod restart"
      ├─ Emit config_reloads_total{result="restart_required"}
      └─ Keep old config running — operator must restart the pod
```

---

## 6. Subprocess Manager

### 6.1 Lifecycle States

```text
Initialising ──► Validating ──► Starting ──► Running ──► ShuttingDown
                                                │
                                                ├──► Reloading ──► Running
                                                │
                                                ▼
                                             Crashed
                                           (wrapper restarts Vector with backoff;
                                            K8s handles pod-level restart)
```

### 6.2 Signal Handling

```text
Wrapper (PID 1)                     Vector (child)
     │                                    │
     │◄──── SIGTERM (from kubelet) ──────│
     │                                    │
     │──── SIGTERM ──────────────────────►│
     │                                    │
     │     (Vector drains: stops sources, │
     │      flushes buffers, exits)       │
     │                                    │
     │◄──── exit(0) ─────────────────────│
     │                                    │
     │  cleanup (metrics flush, etc.)     │
     │  exit(0)                           │
```

**Timeouts:**

- `terminationGracePeriodSeconds` in K8s: **65s**
- Wrapper sends SIGTERM to Vector, waits: **55s**
- If Vector doesn't exit in 55s: SIGKILL child, then exit(1)
- Remaining 10s: wrapper cleanup + K8s buffer

### 6.3 Crash Recovery

```rust
// Pseudocode
loop {
    let child = spawn_vector(&config_dir)?;
    let start_time = Instant::now();

    match child.wait().await {
        Ok(status) if status.success() => break,  // clean exit (SIGTERM)

        Ok(status) => {
            metrics.crashes_total.inc();
            warn!("Vector exited with {status}, restarting in {backoff}");

            if start_time.elapsed() > HEALTHY_THRESHOLD {
                backoff.reset();  // was stable, reset backoff
            }

            sleep(backoff.next()).await;
            metrics.restarts_total.inc();
        }

        Err(e) => {
            error!("Vector process error: {e}");
            sleep(backoff.next()).await;
        }
    }
}
```

**Backoff schedule:** 1s → 2s → 4s → 8s → 16s → 32s → 60s (cap)
**Reset after:** 5 minutes of healthy running
**Max crashes:** unlimited by default (K8s pod restart handles the outer loop)

---

## 7. Vector Binary Version Management

A key robustness concern: the Vector binary in the container must be tracked, pinned, and updatable without rebuilding the wrapper.

### 7.1 Version Pinning

The wrapper's config declares a **pinned Vector version**:

```yaml
vector:
  version: "0.48.0"              # Expected version
  version_check: strict          # strict | warn | disabled
  binary: /usr/local/bin/vector
```

**At startup**, the wrapper runs `vector --version`, parses the output, and compares:

| `version_check` | Mismatch behavior |
|---|---|
| `strict` | Refuse to start. Exit with error. Config or image is wrong. |
| `warn` | Log warning, emit metric, continue. For dev/staging. |
| `disabled` | Skip check entirely. |

Version check failures are logged at startup. No runtime version metrics are
currently emitted — this is deferred until the scalo `version-check` feature
is adopted.

### 7.2 Version Update Strategy

Vector upgrades are a **container image concern**, not a wrapper code concern. This is the key advantage of the subprocess model.

```text
                    Wrapper Image (Rust)           Vector Binary
                    ─────────────────────          ─────────────
Release cadence:    When wrapper changes           When Vector releases
Build trigger:      Code change in this repo       Upstream release / security fix
Update path:        New wrapper image tag           New base image layer
Coupling:           None — wrapper is version-      Binary in /usr/local/bin/
                    agnostic (validates via          pinned via Dockerfile ARG
                    config pin)
```

**Dockerfile pattern:**

The Dockerfile is generated via `emit-dockerfile` from the `DeploymentContract`.
CI builds the Rust binary externally (cross-compile amd64 + arm64), then the
Dockerfile copies it into the runtime image alongside the Vector binary.

```dockerfile
# Runtime image — CI builds the wrapper binary externally
FROM debian:trixie-slim
ARG VECTOR_VERSION=0.58.0
ARG TARGETARCH

# Wrapper binary (built by CI, copied in)
COPY dfe-transform-vector /usr/local/bin/dfe-transform-vector

# Vector binary downloaded inside the build, per $TARGETARCH, with its
# LICENSE / NOTICE / licenses tree copied from the same release archive
RUN curl -fsSL "https://packages.timber.io/vector/${VECTOR_VERSION}/..."

RUN mkdir -p /var/lib/vector /var/run/vector/config
USER appuser
ENTRYPOINT ["dfe-transform-vector"]
```

**To update Vector without touching wrapper code:**

1. Change `VECTOR_VERSION` in `src/deployment.rs` — that const is the ONLY
   pin. Do NOT edit the Dockerfile: it is autogenerated, and
   `checked_in_dockerfile_matches_emit_dockerfile` fails if it drifts.
2. Regenerate: `cargo run -- emit-dockerfile > Dockerfile`
3. Rebuild image — only the vector and runtime stages rebuild (layer cache)
4. CI runs integration tests against the new Vector version
5. Deploy

The default config's `vector.version` needs no separate edit — the loader
reads `deployment::VECTOR_VERSION`, so it follows step 1 on its own.

### 7.3 Compatibility Testing

Each CI build validates the wrapper against the pinned Vector version:

```yaml
# CI step
- name: Vector compatibility test
  run: |
    vector --version  # Verify binary exists
    # Generate a test config from fixture big dials
    dfe-transform-vector assemble --config test/fixtures/basic.yaml --output /tmp/test-config/
    # Validate with the actual Vector binary
    vector validate --config-dir /tmp/test-config/
    # Start Vector, wait for health, stop
    dfe-transform-vector --config test/fixtures/basic.yaml &
    sleep 5
    curl -f http://localhost:9090/readyz
    kill %1
```

### 7.4 Version Drift Detection

Version drift is detected at startup via the `version_check` config setting. The
wrapper runs `vector --version`, parses the output, and compares against the
pinned version. In `strict` mode, a mismatch prevents startup. In `warn` mode,
it logs and continues. Periodic release checking is not implemented — version
tracking is a container image pipeline concern.

---

## 8. Robustness

### 8.1 Failure Modes & Mitigations

| Failure Mode | Detection | Mitigation |
|---|---|---|
| **Bad config YAML** | `vector validate` returns exit 78 | Refuse to start. Clear error in logs + metric. Old config kept on reload. |
| **Broken DAG wiring** | Wrapper DAG analysis before validate | Detailed error: "transform 'enrich' references unknown input 'parser' (did you mean 'parse'?)" |
| **Vector crash** | Child process exits non-zero | Restart with exponential backoff. Readiness goes unhealthy. Metrics track crash count. |
| **Vector hang** | Health poll timeout (Vector API unresponsive) | After N consecutive failures (configurable, default 5): SIGTERM → wait → SIGKILL → restart |
| **OOM** | Child killed by cgroup (exit 137) | Restart. Metric emitted. Scaling pressure increases. Triggers KEDA scale-up. |
| **Kafka unreachable** | Vector consumer lag stops advancing | Scaling pressure stays low (no lag movement). Vector's own retry handles reconnection. |
| **Config reload: new config invalid** | Enrichment table files readable, then `vector validate` on new config | Keep old config running, and put its config dir back. Log error. Emit `config_reloads_total{result="error"}`. |
| **Config reload: Vector rejects SIGHUP** | Vector keeps old config and counts `component_errors_total{error_code="reload"}` | Wrapper reads the counter, emits `config_reloads_total{result="error"}` and puts back the config dir Vector runs. |
| **Version mismatch** | Startup version check | `strict` mode: refuse to start. `warn` mode: log + metric. |
| **Disk buffer corruption** | Vector exits on startup | Wrapper restarts Vector. If persistent, Vector's WAL recovery handles it. |

### 8.2 Graceful Degradation

The wrapper prioritizes **availability over correctness of configuration updates**:

1. **Config reload fails** → old config keeps running (no disruption)
2. **Vector crashes** → readiness unhealthy, K8s stops routing traffic, wrapper restarts Vector
3. **Wrapper crashes** → K8s restartPolicy restarts the pod (outer safety net)
4. **Both crash** → K8s handles it — same as any other pod failure

The two-layer restart model (wrapper restarts Vector, K8s restarts wrapper) provides defense in depth that bare Vector-in-a-pod does not have.

### 8.3 Startup Sequence

```text
1. Load config (cascade)
2. Validate config (Pydantic-equivalent in Rust)
3. Check Vector binary exists at configured path
4. Check Vector version matches pin (if strict)
5. Generate source/sink YAML from big dials
6. Load user transform YAMLs
7. Run DAG wiring + validation
8. Assemble config directory
9. Run `vector validate --no-environment --config-dir` (the broker is Vector's to reach, at runtime)
10. Publish the lifecycle into scalo's health registry (readiness: NOT READY)
11. Spawn Vector child process
12. Wait out the settle window (`SPAWN_SETTLE`, 500ms)
13. Child still alive → lifecycle Running, so readiness reads READY
14. Enter steady state (monitor child, watch config files)
```

The ops listener is scalo's, started by `ServiceRuntime` before step 1; the
wrapper never binds a port of its own. See §4.2 for what the settle window
does and does not prove.

If any step 1–9 fails, the wrapper exits immediately with a clear error. Steps 1–9 are **pre-flight checks** — nothing runs until they all pass. This means a bad config never reaches Vector.

### 8.4 Data Safety

Vector's Kafka source with `acknowledgements: true` only commits offsets after downstream sinks confirm delivery. This means:

- **At-least-once delivery** is maintained through crashes and restarts
- Disk buffers (WAL in `data_dir`) survive pod restarts via PVC
- No data loss on graceful shutdown — Vector drains buffers before exiting
- On crash: some messages may be reprocessed (Kafka re-delivers from last committed offset)

The wrapper does not touch the data path — it only manages config and process lifecycle.

### 8.5 Memory Backpressure (MemoryGuard)

**Not applicable to this project.** The `scalo` `MemoryGuard` provides
cgroup-aware memory backpressure for services that buffer data in-process
(dfe-receiver, dfe-loader, dfe-transform-vrl, dfe-fetcher, dfe-archiver).

dfe-transform-vector does not buffer data — Vector manages all Kafka I/O,
buffering, and memory allocation as an independent subprocess. The Rust
wrapper's own memory footprint is constant and negligible (config structs,
HTTP servers, prometheus registry). Vector's memory is controlled via its
own `buffer` config (`max_events`, `max_size`) and container resource limits.

If the wrapper ever adds its own data buffering, `MemoryGuard` should be
adopted at that point using pattern D (transport backpressure) from the
DFE remediation guide.

---

## 9. Helm Chart

### 9.1 Chart Structure

```text
chart/
  Chart.yaml
  values.yaml
  templates/
    _helpers.tpl
    deployment.yaml
    service.yaml
    configmap.yaml               # Big-dial config
    secret.yaml                  # Kafka credentials (when existingSecret empty)
    serviceaccount.yaml
    hpa.yaml                     # HPA fallback (when KEDA not available)
    keda-scaledobject.yaml       # KEDA ScaledObject (Kafka lag + CPU)
    keda-triggerauth.yaml        # KEDA TriggerAuthentication
    NOTES.txt
```

No dependency on the official Vector Helm chart. This is a purpose-built chart for dfe-transform-vector that follows the same patterns as dfe-loader and dfe-receiver charts.

### 9.2 Values Shape

`chart/values.yaml`, abridged to the keys an operator normally sets:

```yaml
replicaCount: 1                  # ignored while KEDA or the HPA fallback owns the count

image:
  repository: ghcr.io/hyperi-io/dfe-transform-vector
  tag: ""                        # defaults to the chart appVersion
  pullPolicy: IfNotPresent

config:                          # mounted as /etc/dfe-transform-vector/config.yaml
  pipeline:
    name: default
  source:
    transport: bus               # bus consumes topics, direct accepts pushes on listen
    brokers: ["kafka:9092"]
    topics: ["raw_events"]
    group_id: dfe-transform-vector-default
    sasl:
      enabled: true
      mechanism: scram_sha_512
      secret_dir: /var/run/secrets/dfe-kafka
    tls:
      enabled: false
  sink:
    transport: bus               # bus produces to topic, direct pushes to endpoint
    brokers: ["kafka:9092"]
    topic: enriched_events
    key_field: .org_id
    encoding: json
    compression: zstd
    sasl:
      enabled: true
      mechanism: scram_sha_512
      secret_dir: /var/run/secrets/dfe-kafka
    tls:
      enabled: false
  transforms:
    dir: /etc/dfe-transform-vector/transforms
  vector:
    version: 0.58.0
    version_check: warn
  metrics:
    address: 0.0.0.0:9090

kafka:                           # credentials, mounted as files at sasl.secret_dir
  existingSecret: ""
  secretKeys:
    username: kafka-username
    password: kafka-password

resources:
  requests:
    cpu: 250m
    memory: 256Mi
  limits:
    cpu: "2"
    memory: 1Gi

keda:
  enabled: true
  minReplicaCount: 1
  maxReplicaCount: 10
  kafka:
    lagThreshold: "1000"
  cpu:
    enabled: true
    threshold: "80"

autoscaling:                     # HPA fallback, exclusive with keda.enabled
  enabled: false

serviceAccount:
  create: true
  annotations: {}

nodeSelector: {}
tolerations: []
affinity: {}
```

The chart has no `transforms` value. Transform YAML comes from the image, at `config.transforms.dir`, or from the paths in `config.transforms.files`.

### 9.3 Parity With dfe-loader/dfe-receiver Charts

| Feature | dfe-loader | dfe-receiver | dfe-transform-vector |
|---|---|---|---|
| Workload type | StatefulSet | StatefulSet | Deployment |
| Health probes | /livez, /readyz | /livez, /readyz | /livez, /readyz |
| Metrics port | 9090 | 9090 | 9090 |
| ConfigMap | config.yaml | config.yaml | config.yaml |
| PVC | data dir | data dir | None (Deployment) |
| KEDA ScaledObject | Kafka lag | Kafka lag | Kafka lag |
| PodMonitor | Yes | Yes | Yes |
| ServiceAccount + IRSA | Yes | Yes | Yes |
| Reloader annotation | Yes | Yes | Yes |

---

## 10. Multi-Instance Deployment

Like dfe-loader, multiple instances of dfe-transform-vector can run simultaneously for different pipelines:

```text
transform-vector-syslog      # Kafka(raw_syslog) → parse+enrich → Kafka(enriched_syslog)
transform-vector-netflow      # Kafka(raw_netflow) → decode+geoip → Kafka(enriched_netflow)
transform-vector-dns          # Kafka(raw_dns) → parse+filter  → Kafka(enriched_dns)
```

Each instance has its own:

- Config file in dfe-engine registry: `transform-vector-syslog.yaml`
- Deployment config: `deployment_configs/transform-vector-syslog.yaml`
- Kafka consumer group: `dfe-transform-vector-syslog`
- Helm release: separate Deployment
- KEDA ScaledObject: independent scaling

This is the standard dfe-engine multi-instance pattern — no special handling needed.

---

## 11. Component Boundary Summary

| Concern | Owner | Notes |
|---|---|---|
| Config schema + validation | dfe-engine (Pydantic) + wrapper (Rust) | Both validate, dfe-engine at compile/save time, wrapper at runtime |
| Helm values compilation | dfe-engine | HelmValuesCompiler |
| Argo CD Applications | dfe-engine | Generated from deployment registry |
| K8s manifests | This repo's Helm chart | Deployment, Service, ConfigMap, etc. |
| Config assembly (big dials → Vector YAML) | Wrapper (Rust) | The core novel work |
| DAG wiring + validation | Wrapper (Rust) | Auto-wire source/sink, validate refs |
| Vector process lifecycle | Wrapper (Rust) | Spawn, monitor, restart, shutdown |
| Data processing | Vector (subprocess) | Kafka consume → transform → Kafka produce |
| Health/metrics aggregation | Wrapper (Rust) | Composite health, proxied + own metrics |
| Scaling signals | Wrapper (Rust) | Compute scaling_pressure from Vector metrics |
| Vector binary version | Dockerfile ARG + config pin | Decoupled from wrapper releases |

---

## 12. What's NOT In Scope

- **VRL authoring tools** — users write their own transform VRL, we just validate it
- **Transform library/marketplace** — future consideration, not MVP
- **Multi-Vector-process** — one Vector child per wrapper instance (scale via K8s replicas)
- **Kafka topic creation** — handled by dfe-engine's source registry
- **ClickHouse DDL** — not applicable (this is Kafka→Kafka, not Kafka→ClickHouse)
- **Kubernetes operator / CRDs** — we use Helm + Argo CD, not a custom operator
