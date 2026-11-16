# Moving off the Vector chart

A values file for the official Vector Helm chart, served from `helm.vector.dev`, holds the whole Vector config. dfe-transform-vector's chart takes the big-dial config instead, one values file per pipeline, and both releases can run side by side during the move.

Before, the official Vector chart's `vector-syslog.yaml`:

```yaml
replicas: 3
image:
  tag: "<vector release>-debian"
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

After, dfe-transform-vector's `dfe-transform-vector-syslog.yaml`:

```yaml
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
    brokers: ["kafka:9092"]
    topics:
      - raw_syslog_land
    group_id: dfe-transform-vector-syslog
    tls:
      enabled: true
  sink:
    brokers: ["kafka:9092"]
    topic: enriched_syslog_land
    key_field: .org_id
    compression: zstd
    tls:
      enabled: true
  transforms:
    dir: /etc/dfe-transform-vector-transforms

fileSets:
  transforms:
    mountPath: /etc/dfe-transform-vector-transforms
    files:
      - name: enrich.yaml
        content: |
          transforms:
            enrich:
              type: remap
              source: .env = "prod"

secrets:
  kafka:
    existingSecret: kafka-sasl   # holds username, password and sasl.mechanism

keda:
  minReplicaCount: 3
  maxReplicaCount: 20
```

Key differences:

- No inline VRL. Transforms are YAML files the app loads from `config.transforms.dir`, or lists in `config.transforms.files`. The chart's ConfigMap mounts over `/etc/dfe-transform-vector/` and hides the image's `transforms` directory there. Deliver transforms with a `fileSets` entry, bake them into an image outside `/etc/dfe-transform-vector/` and point `transforms.dir` there, or set `configMount.subPath`.
- No persistence by default. The workload is a Deployment, and `vector.data_dir` is an emptyDir unless `writablePaths.data.persistence.enabled` gives it a claim, so records not yet acknowledged are read again from Kafka after a pod restart.
- SASL and TLS can sit in one values file every pipeline layers over, so a pipeline file holds the values that differ.
- A `${VAR}` in the config is not expanded: neither the app nor Vector 0.57+ interpolates environment variables. Brokers and topics are big dials, and in a DFE deployment the `DFE_TRANSFORM_VECTOR_SOURCE_BROKERS` and `DFE_TRANSFORM_VECTOR_SINK_BROKERS` variables set the brokers.
- SASL credentials come from the Kafka Secret through `DFE_TRANSFORM_VECTOR_{SOURCE,SINK}_SASL_{USERNAME,PASSWORD}`, and the app hands them to Vector's directory secret backend as owner-only files.
- `replicaCount` applies when nothing autoscales the workload. KEDA scales on CPU, between `keda.minReplicaCount` and `keda.maxReplicaCount`, with no Kafka lag trigger and no TriggerAuthentication.

The chart creates no Secret, so the Kafka SASL secret comes from wherever your deployment supplies it. Node placement moves across through the chart's `nodeSelector` and `tolerations`.

| Element | Before | After |
|---------|--------|-------|
| Chart source | `helm.vector.dev` | The thin chart each `dfe-transform-vector` release publishes, on scalo-service |
| Image | `timberio/vector` | `ghcr.io/hyperi-io/dfe-transform-vector` |
| Workload type | StatefulSet, the `Aggregator` role | Deployment |
| Health endpoint | `/health` on Vector's API, 8686 by default | `/livez`, `/readyz` on 9090, beside `/metrics` |
| Metrics | Vector's exporter | The app's and Vector's, merged on 9090 |
| Config format | Full Vector YAML (`customConfig`) | Big-dial config (source/sink/transforms) |
| Transform delivery | Inline VRL in `customConfig` | YAML files in `transforms.dir`, from a `fileSets` entry or an image path outside the config directory |
| DAG wiring | Manual (inline in values) | Auto-wired by the app |
| Reload | Vector's `--watch-config`, where the args set it | A transform file changed in place reloads. A chart change rolls the pods |
| Crash recovery | K8s pod restart only | The app restarts Vector with backoff, inside the pod |

## Cutover procedure

1. For each existing Vector values file, write a dfe-transform-vector values file in the big-dial format above.
2. Run both releases at once, the new one with a separate `sink.topic`. The new pipeline uses a different consumer group ID, so it reads the same source topic independently of the existing pipeline, and its separate output topic keeps the two from writing every record twice into one topic. A group with no committed offset starts at the newest one, because `source.auto_offset_reset` defaults to `largest`, so its output holds only what arrives after it starts. Set `smallest` to have it read the topic's backlog as well.
3. Validate: compare the output topics of the old and new pipelines, watch throughput, latency and error rates, check the health endpoints, and check KEDA scales.
4. Scale the old pipeline to 0 replicas and let downstream consumers drain its output topic, then point them at the new output topic. Records both pipelines carried during the overlap are in both topics, so a consumer that starts the new topic at its beginning reads those twice. Start it past the last record it took from the old topic, or deduplicate.
5. Monitor for 24-48 hours, then remove the old Vector release and its values file.
