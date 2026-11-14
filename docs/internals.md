# dfe-transform-vector internals

## 1. Purpose

dfe-transform-vector wraps Vector.dev as a managed subprocess to provide **Kafka -> transform -> Kafka** pipelines that the DFE platform manages, deploys and observes the same way as dfe-loader and dfe-receiver.

Vector is powerful but opaque. This wrapper makes it behave like every other DFE service: the same health contract, metric names and scaling signal, a chart generated from its deployment contract, and the same Argo CD lifecycle. How the app is deployed is declared in dfe-infra's `apps.yaml`, not coded into dfe-engine.

---

## 2. Architecture

The pod runs the chart generated from `contract()` (section 9), with the config the deployment mounts.

```text
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
│  │  │  (no watcher of its own: reloads arrive as SIGHUP)  │    │  │
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

## 3. Dead Letters

On `direct`, the supervisor's bridge sends a record the next stage refuses for good to the DLQ `dlq` configures: scalo's `DlqConfig`, by default NDJSON files under `/var/spool/scalo/dlq`. With `dlq.enabled: false` such a record is dropped and counted in `pipeline_dead_letters_dropped_total`: `reason="too_large"` for one over the next stage's message-size ceiling, `reason="rejected"` for one the sink refused for good. On `bus`, Vector's own Kafka sink delivers and the bridge DLQ is not used.

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

**Wrapper metrics** (emitted by the Rust binary). Names are bare unless
`metrics.namespace` is set, and none is by default:

```text
# DFE platform standard
pipeline_ready 1                          # 1=ready, 0=not ready

# Process lifecycle
transform_vector_lifecycle_state{state="running"} 1
uptime_seconds 3600

# Subprocess health
crashes_total 0
restarts_total 1

# Config management
transform_vector_config_reloads_total{result="success"} 5
transform_vector_config_reloads_total{result="error"} 0
transform_vector_config_reloads_total{result="rejected"} 0
transform_vector_config_reloads_total{result="unconfirmed"} 0
config_validation_errors_total 0
```

`success` means Vector reported the reload. `error` (a config that does not
load or validate, a failed assembly or publish, a SIGHUP that cannot be sent,
or a reload Vector refused) and `rejected` (a change outside the transform
files, which needs a restart) mean the running Vector did not take the change.
`unconfirmed` means the change is published but Vector did not report whether
it runs it.

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

Counters register as counters via `absolute`, carried across Vector restarts:
a total that falls is a restarted Vector, and the count it had reached becomes
a base the new count adds to. A histogram arrives
pre-aggregated, so each cumulative `_bucket` re-registers as a counter carrying
its `le` label: the metrics crate's `Histogram` takes observations, and
replaying buckets through it would change the numbers.

One scrape of :9090 therefore carries wrapper state and Vector pipeline state,
and 9598 stays a loopback debug surface with no reader outside the pod.

### 4.4 Scaling

The ScaledObject scales on CPU alone. `contract()` sets `KafkaLagTrigger::disabled()`,
because consumer-group lag rises when a downstream stage breaks and more replicas
cannot fix that. A deployment adds a trigger with the library chart's
`keda.extraTriggers` value. `spawn_circuit_gate_task` pins scalo's scaling
pressure to 0 while the Vector subprocess is down.

In a DFE deployment, dfe-infra's values set the CPU target to 70% and add no
pressure trigger in any profile, so this app scales on CPU alone. The engine's
[KEDA scaling page](https://github.com/hyperi-io/dfe-engine/blob/main/docs/deployment/keda-scaling.md)
covers the triggers for every app.

scalo's runtime serves the pressure at `/scaling/pressure` and writes it into the
`scaling_pressure` gauge. With no pressure components and no memory reading fed
to it, the value is 0 while Vector runs too.

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
  brokers: ["kafka:9092"]     # or DFE_TRANSFORM_VECTOR_SOURCE_BROKERS
  topics:
    - raw_syslog_land
  group_id: dfe-transform-vector-syslog-enrichment
  sasl:
    mechanism: scram_sha_512
    # A mounted Secret holding `username` and `password` files. Vector reads
    # them through its directory secret backend; it expands no ${VAR}.
    secret_dir: /var/run/secrets/dfe-kafka
  tls:
    enabled: true

sink:
  brokers: ["kafka:9092"]     # or DFE_TRANSFORM_VECTOR_SINK_BROKERS
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
    condition: length(encode_json(.)) <= 16777088
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
      message.max.bytes: "16777216"     # scalo's record ceiling unless a layer sets one
```

The size cap and `message.max.bytes` read one value: scalo's pipeline-wide record ceiling of 16 MiB, the one every other stage's producer and every topic scalo creates carry, unless the central profile or `sink.librdkafka_options` sets `message.max.bytes`. The cap keeps 128 bytes under it for the key, headers and framing. librdkafka's own default of 1,000,000 bytes would refuse a record the rest of the chain carries.

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
3. **First transform**: a transform with no or empty `inputs` reads `dfe_source`, and an input written `source` is rewritten to `dfe_source` in the file Vector reads
4. **Intermediate transforms**: left as authored (user controls the chain). An input names a component (`parse`) or one of its named outputs (`split.accepted`)
5. **Last transforms**: every output nothing reads is injected as `dfe_sink.inputs`. A `route` or `exclusive_route` has named outputs only, so each unread route and its `_unmatched` reach the sink
6. **Extra sources/sinks**: refused, with every other top-level key bar `transforms` and `enrichment_tables`

**Validation rules:**

- All `inputs` references must resolve to a defined component, and to an output that component has
- No orphaned components (every component must be reachable from a source)
- No cycles in the DAG
- At least one path from `dfe_source` to `dfe_sink`
- `vector validate --config-dir` as final gate (catches VRL syntax, type mismatches)

### 5.4 Hot-Reload

```text
Config change detected (file watcher or SIGHUP)
  │
  ├─ Transform file change (safe)
  │   ├─ Re-read transform YAMLs, re-run DAG wiring + validation into <config_dir>.next
  │   ├─ Check every enrichment table file can be read, then `vector validate` <config_dir>.next
  │   ├─ If invalid: log error, increment metric, drop <config_dir>.next (Vector's dir untouched)
  │   ├─ If valid: swap <config_dir>.next and <config_dir> in one rename → SIGHUP to Vector child
  │   ├─ Read vector_reloaded_total / component_errors_total{error_code} for the outcome
  │   ├─ If refused: log error, increment metric, swap the running config dir back
  │   └─ Emit config_reloads_total{result=success|error|unconfirmed}
  │
  └─ Any other config change (unsafe — requires pod restart)
      ├─ Log warning: "config change requires pod restart"
      ├─ Emit config_reloads_total{result="rejected"}
      └─ Keep old config running — operator must restart the pod
```

The swap is one `renameat2(RENAME_EXCHANGE)`, so a Vector restarted mid-reload reads the old directory or the new one, never a mix. A filesystem that cannot swap directories, such as an overlay image layer with no volume mounted at `/var/run/vector`, gets the validated files copied in place instead, which is not atomic.

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
  version: "<vector version>"    # Expected version, default deployment::VECTOR_VERSION
  version_check: warn            # strict | warn | disabled
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
ARG VECTOR_VERSION=<vector version>
ARG TARGETARCH

# Wrapper binary (built by CI, copied in)
COPY dfe-transform-vector /usr/local/bin/dfe-transform-vector

# Vector binary downloaded inside the build, per $TARGETARCH, with its
# LICENSE / NOTICE / licenses tree copied from the same release archive
RUN curl -fsSL "https://github.com/vectordotdev/vector/releases/download/v${VECTOR_VERSION}/..."

RUN mkdir -p /var/lib/vector /var/run/vector/config /etc/dfe-transform-vector/transforms
USER appuser
ENTRYPOINT ["dfe-transform-vector"]
```

**To update Vector without touching wrapper code:**

1. Change `VECTOR_VERSION` in `src/deployment.rs` — that const is the ONLY
   pin. Do NOT edit the Dockerfile: it is autogenerated, and
   `checked_in_dockerfile_matches_emit_dockerfile` fails if it drifts.
2. Regenerate: `cargo run -- emit-dockerfile > Dockerfile`
   Then move `services.vector` in dfe-infra's `versions.yaml` to the same
   release (its tag is `<version>-alpine`). dfe-fetcher's container Vector
   extractor runs that image, so the two must name one release.
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
| **Vector hang** | Only through the sink: records held and none delivered for `metrics.sink_stall_secs` | Readiness goes unhealthy. Nothing restarts a hung Vector, and liveness stays 200. |
| **OOM** | Child killed by cgroup (exit 137) | Restart with backoff, counted as a crash. The scaling circuit opens, pinning scaling pressure to 0 until Vector runs again. |
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

### 9.1 How the Chart Is Built

No chart is committed. At release, hyperi-ci runs the binary's `generate-artefacts` for the deployment contract and assembles a thin chart from it on the scalo-service library chart, at the version `release.helm.library` names in `.hyperi-ci.yaml`. The library renders the Deployment, Service, ConfigMap, ServiceAccount and KEDA ScaledObject, and its README documents the values.

### 9.2 What the Contract Fixes

- Probes on 9090 (`/livez`, `/readyz`) with a 120 s startup budget.
- The Push port 6000, `appProtocol: kubernetes.io/h2c`, only while `config.source.transport` is `direct` or `grpc`.
- The Kafka Secret's `username` and `password` in `DFE_TRANSFORM_VECTOR_{SOURCE,SINK}_SASL_{USERNAME,PASSWORD}`. The assembler writes them to owner-only files under `vector.config_dir`, which Vector reads through its `directory` secret backend.
- Writable paths `data` at `/var/lib/vector` and `run` at `/var/run/vector`, beside the library's `/tmp`, under a read-only root filesystem.
- Requests 100m/128Mi, limits 500m/512Mi, a 90 s grace period, uid and gid 1000.
- KEDA on CPU at 80 %, 1 to 10 replicas, no Kafka lag trigger.

The mounted config is the chart's `config` with `configOverrides` merged over it. The contract's `default_config` is not merged in, so the app's own defaults apply to anything a deployment leaves unset. The library mounts the ConfigMap over the config file's whole directory unless `configMount.subPath` is set, and that hides the image's `/etc/dfe-transform-vector/transforms`.

### 9.3 Parity With dfe-loader/dfe-receiver Charts

All three are generated on the same scalo-service library chart, so they share the same features:

| Feature | dfe-loader | dfe-receiver | dfe-transform-vector |
|---|---|---|---|
| Workload type | Deployment | Deployment | Deployment |
| Health probes | /livez, /readyz | /livez, /readyz | /livez, /readyz |
| Metrics port | 9090 | 9090 | 9090 |
| ConfigMap | config.yaml | config.yaml | config.yaml |
| KEDA ScaledObject (contract) | CPU, no Kafka lag trigger | CPU, no Kafka lag trigger | CPU, no Kafka lag trigger |

---

## 10. Multi-Instance Deployment

Like dfe-loader, multiple instances of dfe-transform-vector can run simultaneously for different pipelines:

```text
transform-vector-syslog      # Kafka(raw_syslog) → parse+enrich → Kafka(enriched_syslog)
transform-vector-netflow      # Kafka(raw_netflow) → decode+geoip → Kafka(enriched_netflow)
transform-vector-dns          # Kafka(raw_dns) → parse+filter  → Kafka(enriched_dns)
```

dfe-infra's `apps.yaml` declares the app `multiplicity: per_config`, so each source config gets its own:

- Config file: `transform-vector-syslog.yaml`
- Kafka consumer group: `dfe-transform-vector-syslog`
- Helm release: separate Deployment
- KEDA ScaledObject: independent scaling

The app is in the default composition of `docker-single` only. Elsewhere an operator adds an instance per source.

---

## 11. Component Boundary Summary

| Concern | Owner | Notes |
|---|---|---|
| App deployment | dfe-infra `apps.yaml` | Multiplicity, scaling, transports, source binding |
| Config schema + validation | Wrapper (Rust) | `docs/config-schema.json` is generated from the config structs. dfe-engine judges a stored config against that mounted contract |
| K8s manifests | Chart generated from `contract()` | On the scalo-service library chart (section 9) |
| Config assembly (big dials → Vector YAML) | Wrapper (Rust) | The core novel work |
| DAG wiring + validation | Wrapper (Rust) | Auto-wire source/sink, validate refs |
| Vector process lifecycle | Wrapper (Rust) | Spawn, monitor, restart, shutdown |
| Data processing | Vector (subprocess) | Kafka consume → transform → Kafka produce |
| Health/metrics aggregation | Wrapper (Rust) | Composite health, proxied + own metrics |
| Scaling signals | Wrapper (Rust) | `scaling_pressure` is 0 while Vector runs and pinned to 0 while it is down. KEDA scales on CPU |
| Vector binary version | Dockerfile ARG + config pin | Decoupled from wrapper releases |

---

## 12. What's NOT In Scope

- **VRL authoring tools** — users write their own transform VRL, we just validate it
- **Transform library/marketplace** — future consideration, not MVP
- **Multi-Vector-process** — one Vector child per wrapper instance (scale via K8s replicas)
- **Kafka topic creation** — handled by dfe-engine's source registry
- **ClickHouse DDL** — not applicable (this is Kafka→Kafka, not Kafka→ClickHouse)
- **Kubernetes operator / CRDs** — we use Helm + Argo CD, not a custom operator
