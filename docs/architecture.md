# dfe-transform-vector architecture

dfe-transform-vector runs a Vector pipeline as a DFE transform stage, for the transforms beyond VRL's `remap`. A Rust supervisor is PID 1 and gives Vector what every DFE app has: one config, one ops port, health, metrics and restarts.

```mermaid
---
config:
  theme: base
  look: neo
  fontFamily: Inter, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif
  flowchart:
    curve: linear
    nodeSpacing: 40
    rankSpacing: 64
    padding: 18
    wrappingWidth: 280
    htmlLabels: true
  elk:
    preset: modelOrder
  themeVariables:
    fontSize: 15px
    lineColor: "#64748B"
    primaryTextColor: "#0F172A"
    nodeBorder: "#334155"
    clusterBorder: "#0072B2"
    edgeLabelBackground: "#FFFFFF"
  themeCSS: >-
    p{margin:0}
    .node[data-look] rect,.node[data-look] path,.node[data-look] polygon,.node[data-look] .outer-path{filter:drop-shadow(0 1px 2px rgba(15,23,42,.10)) drop-shadow(0 6px 14px rgba(15,23,42,.10))}
    .node rect{rx:10px;ry:10px}
    .cluster rect{rx:16px;ry:16px}
    .node .label div{min-width:150px}
    .node.decision .label div{min-width:0}
    .cluster[data-look] rect{filter:none}
    .nodeLabel{line-height:1.35}
    .nodeLabel b{font-weight:600;font-size:15px;letter-spacing:-0.01em}
    .nodeLabel span{font-size:12.5px;font-weight:400;color:#334155}
    .cluster-label span.nodeLabel{display:inline-block;background:#FFFFFF;color:#0B2A3F;border:1.5px solid #0072B2;border-radius:999px;padding:2px 12px;font-size:11.5px;font-weight:700;letter-spacing:.09em;text-transform:uppercase}
    .flowchart-link{stroke-width:1.75px}
    .labelBkg{background:transparent!important}
    span.edgeLabel{display:inline-block;background:#FFFFFF!important;color:#1E293B;border:1px solid #94A3B8;border-radius:999px;padding:2px 10px;font-size:12px;font-weight:500}
    span.edgeLabel p{background:transparent!important}
    .label-icon{width:20px;height:20px;vertical-align:-4px;margin-right:6px}
    .icon-shape path[stroke-width="0"][fill="none"]{fill:#FFFFFF;stroke:#FFFFFF;stroke-width:12px;stroke-linejoin:round}
    .icon-shape p{background:transparent!important;color:#0F172A}
    .ingest .label-icon{color:#0E8A7D}
    .xform .label-icon{color:#7B4FB0}
    .load .label-icon{color:#0072B2}
    .bus .label-icon{color:#A8517F}
    .store .label-icon{color:#00785A}
    .ui .label-icon{color:#D9480F}
    .ext .label-icon{color:#64748B}
---
flowchart TB
    accTitle: L3 Components -- dfe-transform-vector
    accDescr: The previous stage sends records into the pod and the config and transform files configure it. Inside, the config engine writes the Vector config that the process manager starts Vector on. On the direct transport the bridge intake hands records to Vector and the bridge return takes them back. Vector's metrics are merged into the supervisor's. Records leave for the next stage and metrics go to the collector.

    subgraph PREV["Previous stage"]
        direction LR
        prev("<b>Records in</b><br/><span>*_land topic or Push</span>")
    end

    subgraph CFGIN["Config"]
        direction LR
        files[("<b>Config files</b><br/><span>config and transforms</span>")]
    end

    subgraph POD["dfe-transform-vector"]
        direction TB
        subgraph SUP["Supervisor"]
            direction LR
            cfg("<b>Config engine</b><br/><span>writes Vector's config</span>")
            proc("<b>Process manager</b><br/><span>spawns, restarts</span>")
            merge("<b>Metrics merge</b><br/><span>vector_* into ours</span>")
            cfg --> proc
        end
        subgraph DATA["Records"]
            direction LR
            intake("<b>Bridge intake</b><br/><span>direct only</span>")
            vec("<b>Vector</b><br/><span>the transform</span>")
            ret("<b>Bridge return</b><br/><span>direct only</span>")
            intake -.-> vec -.-> ret
        end
        SUP -->|starts, scrapes| DATA
    end

    subgraph NEXT["Next stage"]
        direction LR
        next("<b>Records out</b><br/><span>*_load topic or Push</span>")
    end

    subgraph COL["Telemetry"]
        direction LR
        collector("<b>Metrics collector</b><br/><span>scrape or OTLP</span>")
    end

    PREV -->|records| POD
    CFGIN -->|config| POD
    POD -->|records| NEXT
    POD -->|metrics| COL
  class prev,next bus
  class collector ext
  class files store
  class cfg,proc,intake,vec,ret,merge part
  classDef ext fill:#F4F6F8,stroke:#94A3B8,stroke-width:1.5px,stroke-dasharray:5 4,color:#0F172A
  classDef ingest fill:#FFFFFF,stroke:#0E8A7D,stroke-width:3px,color:#0F172A
  classDef xform fill:#FFFFFF,stroke:#7B4FB0,stroke-width:3px,color:#0F172A
  classDef load fill:#FFFFFF,stroke:#0072B2,stroke-width:3px,color:#0F172A
  classDef bus fill:#FFFFFF,stroke:#A8517F,stroke-width:3px,color:#0F172A
  classDef store fill:#FFFFFF,stroke:#00785A,stroke-width:3px,color:#0F172A
  classDef ui fill:#FFFFFF,stroke:#D9480F,stroke-width:3px,color:#0F172A
  classDef decision fill:#FFFFFF,stroke:#9A6700,stroke-width:3px,color:#0F172A
  classDef error fill:#FFFFFF,stroke:#C2410C,stroke-width:3px,color:#0F172A
  classDef control fill:#FFFFFF,stroke:#334155,stroke-width:3px,color:#0F172A
  classDef part fill:#FFFFFF,stroke:#94A3B8,stroke-width:1.5px,color:#0F172A
  style PREV fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style CFGIN fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style POD fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style SUP fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style DATA fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style NEXT fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style COL fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
```

*Figure 1. L3 Components -- dfe-transform-vector. The arrows between Bridge intake, Vector and Bridge return are the direct transport. On the bus Vector reads and writes Kafka itself.* Up: [L2 Services -- DFE deployment](https://github.com/hyperi-io/dfe-infra/blob/main/docs/architecture.md). Down: [L4 Detail -- Config engine](internals.md), [L4 Detail -- Vector](vector.md), [L4 Detail -- Direct transport bridge](direct-transport.md#how-it-works).

## How data flows

1. The **Config engine** turns the big-dial config and your transform files into a Vector config directory, wires the graph and runs `vector validate`.
2. The **Process manager** spawns **Vector** on that directory, and restarts it after a crash.
3. On the bus, Vector's `kafka` source reads `<source>_land`. On the direct transport, the **Bridge intake** takes Push calls on port 6000 and hands records to Vector over loopback.
4. Vector runs the transforms.
5. On the bus, Vector's `kafka` sink writes `<source>_load`. On direct, the **Bridge return** takes Vector's output and pushes it to the next stage.
6. The **Metrics merge** scrapes Vector's exporter and serves its `vector_*` series beside the supervisor's, on one port and one OTLP push.

## Components

| Part | Role | Page |
|---|---|---|
| Config engine | Builds and validates Vector's config, and reloads transforms | [internals.md](internals.md) |
| Process manager | Spawns Vector, forwards SIGTERM, restarts it with backoff | [vector.md](vector.md#how-it-works) |
| Metrics merge | Serves Vector's metrics with the supervisor's | [vector.md](vector.md#interfaces) |
| Bridge intake | Translates scalo `Transport/Push` into Vector's `PushEvents` | [direct-transport.md](direct-transport.md) |
| Vector | The upstream binary, at the version `VECTOR_VERSION` pins | [vector.md](vector.md) |
| Bridge return | Takes Vector's output and pushes it onward | [direct-transport.md](direct-transport.md) |

You write the `transforms:` and `enrichment_tables:` blocks. The supervisor generates the source, the sink and the observability config.

## External dependencies

| Dependency | Needed for | Swappable |
|---|---|---|
| Kafka | The bus transport, read and written by Vector | Any broker `source.brokers` and `sink.brokers` reach. Direct needs none |
| A DFE service over gRPC | The direct sink, `http://dfe-loader:6000` by default | Any stage running a Push listener |
| Dead-letter queue | Records over the next hop's size ceiling | `dfe_transform_dlq` on the bus, a file spool on direct |
| dfe-engine | Writing a deployment per source config | dfe-infra's `apps.yaml` declares the app, so the engine has no code for it |

## Deployment variants

| Variant | What runs |
|---|---|
| Docker `single` | On by default. One instance per source that binds a Vector transform |
| Kubernetes, every profile | When a deployment asks for it. One deployment per source config, KEDA sets replicas |

One Vector runs per pod, and replicas scale out. The released chart scales on CPU. dfe-infra's values add a pressure trigger in `single`, `scale` and `mesh`, but the gauge it reads stays 0 when Vector runs, so that trigger never scales a pod out and CPU does the scaling. Feeding it is tracked in <https://github.com/hyperi-io/dfe-transform-vector/issues/117>. Lag isn't a trigger, because it rises when a downstream stage breaks and more pods can't fix that.

The chart mounts the config and transform files, and an edit to either rolls the pods. Outside a chart, a transform change hot-reloads. [reference/configuration.md](reference/configuration.md) has the config layers and what the chart fixes.

## Design considerations

**Security.** Vector runs whatever a transform file declares, with read access to the Kafka credentials. So a transform file may declare `transforms` (never `lua`) and `enrichment_tables` alone, and a table must resolve inside `transforms.data_dir` and outside every credential path. A transform can't read a file, reach the network or inflate a record: no transform may set `file` or `files`, and the VRL host, network, file and decompression functions are refused before Vector sees them. A failed validate is logged as the components and error codes alone, never the source Vector quotes. Credentials reach Vector as `SECRET[...]` file references, never as values in its config. Vector runs without the `DFE_TRANSFORM_VECTOR_*` variables.

**Reliability.** Delivery is at least once on both transports. A crashed Vector restarts on an exponential backoff capped at a minute, with no crash limit. A failed bridge leg stops the process, because its senders would otherwise retry forever against a live pod.

**Operations.** `/readyz` means Vector survived 500 ms after spawn and its sink isn't stalled. A pod with an unreachable broker and nothing held stays Ready. Vector's exporter on 9598 is a loopback debug surface, and 9090 is the scrape target.

**Performance.** Vector's buffers have no memory cap of their own, so the pod's limit has to cover Vector's worst case. For VRL alone, [dfe-transform-vrl](https://github.com/hyperi-io/dfe-transform-vrl) runs in-process with bounded buffers.

## Why it's built this way

**A subprocess, not a library.** Vector has no stable API for embedding. Running the upstream release binary makes a Vector upgrade a version bump and an image rebuild, not a rebuild against every Vector release.

**One pin.** `VECTOR_VERSION` in `src/deployment.rs` feeds the Dockerfile, the default `vector.version` and the contract. `vector.version_check: strict` stops a pod whose config and binary disagree.

**Polling for reload.** S3-backed mounts (s3fs, goofys, Mountpoint for S3) raise no filesystem events, so inotify would work on a laptop and silently stop reloading in production.

**Wiring from labels.** The config engine reads each transform's `inputs` and joins `dfe_source` to `dfe_sink` through them, so a transform author never hand-wires the topology.

## Diagram map

- [Figure 1. L3 Components -- dfe-transform-vector](architecture.md)
  - [L4 Detail -- Config engine](internals.md)
  - [L4 Detail -- Vector lifecycle](vector.md)
  - [L4 Detail -- Direct transport bridge](direct-transport.md)

## What's next

- [config.example.yaml](../config.example.yaml), the annotated config, and [config-schema.yaml](config-schema.yaml), every key
- [reference/configuration.md](reference/configuration.md), the config layers and the chart
- [librdkafka.md](librdkafka.md), the Kafka client settings, and [migration.md](migration.md), moving off the Vector chart
- [CONTRIBUTING.md Context](../CONTRIBUTING.md#context), where things live and the commands that test a change
