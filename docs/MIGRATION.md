# Migration Guide: Vector → dfe-transform-vector

This guide covers migrating existing Vector pipelines from the official
`timberio/vector` Helm chart to the `dfe-transform-vector` wrapper.

## Pipeline Values Migration

Existing pipeline values files in the `pipelines` repo follow the official
Vector chart format. The new format uses the "big-dial" config pattern.

### File Naming

Old: `pipelines/<namespace>/vector-<pipeline>.yaml`
New: `pipelines/<namespace>/dfe-transform-vector-<pipeline>.yaml`

The ArgoCD ApplicationSet scans for `dfe-transform-vector-*.yaml` files.
Both old and new pipelines can coexist during migration.

### Values Format

**Before** (official Vector chart — `vector-syslog.yaml`):

```yaml
replicas: 3
image:
  tag: "0.48.0-debian"
persistence:
  size: 20Gi
resources:
  requests:
    cpu: "2"
    memory: 2Gi
  limits:
    cpu: "4"
    memory: 4Gi
customConfig:
  sources:
    kafka_in:
      type: kafka
      bootstrap_servers: "${KAFKA_BROKERS_SASL_SCRAM}"
      group_id: dfe-vector-syslog
      topics: [raw_syslog_land]
      sasl:
        enabled: true
        mechanism: SCRAM-SHA-512
        username: "${KAFKA_SASL_USERNAME}"
        password: "${KAFKA_SASL_PASSWORD}"
      tls:
        enabled: true
  transforms:
    # ... 500+ lines of inline VRL ...
  sinks:
    kafka_out:
      type: kafka
      bootstrap_servers: "${KAFKA_BROKERS_SASL_SCRAM}"
      topic: enriched_syslog_land
      # ...
```

**After** (dfe-transform-vector — `dfe-transform-vector-syslog.yaml`):

```yaml
replicaCount: 3

resources:
  requests:
    cpu: "2"
    memory: 2Gi
  limits:
    cpu: "4"
    memory: 4Gi

config:
  pipeline:
    name: syslog-enrichment
  source:
    topics:
      - raw_syslog_land
    group_id: dfe-transform-vector-syslog
  sink:
    topic: enriched_syslog_land
    key_field: .org_id
    compression: zstd

keda:
  kafka:
    lagThreshold: "500"
  maxReplicaCount: 20
```

Key differences:
- **No inline VRL** — transforms are fetched by the init container from Artifactory
- **No persistence** — Deployment (not StatefulSet), data dir loss just re-reads from Kafka
- **SASL/TLS** inherited from `common.yaml` — only override pipeline-specific values
- **KEDA** configured declaratively, inherits defaults from common.yaml
- **Kafka brokers** come from `DFE_TRANSFORM_SOURCE_BROKERS` and
  `DFE_TRANSFORM_SINK_BROKERS`. A `${VAR}` in the config is not expanded:
  neither the wrapper nor Vector 0.57+ interpolates environment variables
- **SASL credentials** are files in the mounted secret at `sasl.secret_dir`,
  read by Vector's directory secret backend

### What Stays the Same

- **ExternalSecrets**: Same `kafka-sasl-secret` and `tenant-artifactory` — no changes
- **KEDA TriggerAuthentication**: Same secret references — no changes
- **Karpenter NodePools**: Same `vector_node` role and `dedicated: vector` taint — no changes
- **Transform YAML delivery**: Same Artifactory ZIP download pattern via init container

### What Changes

| Element | Before | After |
|---------|--------|-------|
| Chart source | `helm.vector.dev` v0.42.1 | `dfe-transform-vector.git` `/chart` |
| Image | `timberio/vector:0.48.0-debian` | `ghcr.io/hyperi-io/dfe-transform-vector` |
| Workload type | StatefulSet | Deployment |
| Health endpoint | `/health` on :8686 | `/livez`, `/readyz` on :9090, beside `/metrics` |
| Metrics | Vector native on :9090 | Wrapper + merged Vector on :9090 |
| Config format | Full Vector YAML (customConfig) | Big-dial config (source/sink/transforms) |
| DAG wiring | Manual (inline in values) | Auto-wired by wrapper |
| Hot-reload | Not supported | Poll-based file watcher + SIGHUP |
| Crash recovery | K8s pod restart only | Wrapper restarts Vector with backoff |

## Cutover Procedure

See the cutover plan section below for step-by-step parallel deployment
and traffic switching procedure.

### Step 1: Prepare Pipeline Values

For each existing `vector-<name>.yaml`, create a corresponding
`dfe-transform-vector-<name>.yaml` with big-dial config format.

### Step 2: Deploy in Parallel

Both ApplicationSets run simultaneously. The new pipeline uses a
different consumer group ID, so it reads from the same source topic
independently without affecting the existing pipeline.

### Step 3: Validate

- Compare output topic records between old and new pipelines
- Monitor metrics: throughput, latency, error rates
- Check health endpoints respond correctly
- Verify KEDA scaling behaviour

### Step 4: Switch Traffic

Once validated:
1. Scale down old pipeline replicas to 0
2. Update downstream consumers to read from new output topic (if topic name changed)
3. Monitor for 24-48 hours
4. Remove old pipeline values file from pipelines repo

### Step 5: Cleanup

After all pipelines migrated:
1. Remove old `vector.yaml` ApplicationSet from dfe-core
2. Remove old `gitOps/addons/helm/vector/` directory
3. Remove `vector_project.yaml` ArgoCD project (or update to cover new apps)
