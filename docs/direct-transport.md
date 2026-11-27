# Direct transport

The direct transport runs [dfe-transform-vector](architecture.md) with no broker. The supervisor's bridge translates between scalo's `Transport/Push`, which the rest of DFE speaks, and Vector's `vector.Vector/PushEvents`.

| | |
|---|---|
| Repo | [hyperi-io/dfe-transform-vector](https://github.com/hyperi-io/dfe-transform-vector), `src/bridge.rs` |
| Image and chart | `ghcr.io/hyperi-io/dfe-transform-vector`. The chart opens the Push port when `config.source.transport` is `direct` or `grpc` |
| Config reference | `source.transport`, `source.listen`, `sink.transport`, `sink.endpoint` and `bridge` in [config.example.yaml](../config.example.yaml). [templates/direct.yaml](../templates/direct.yaml) is the whole topology |
| Endpoints | Push listener on `source.listen` (`0.0.0.0:6000`). Loopback legs on `bridge.to_vector` (`127.0.0.1:6100`) and `bridge.from_vector` (`127.0.0.1:6101`) |
| Metrics | `pipeline_dead_letters_dropped_total{reason}` on `:9090/metrics` |

`source.transport` and `sink.transport` each pick `bus` or `direct`, so a stage can take Kafka in and push out, or the reverse. A stage with one end on `direct` runs that half of the bridge alone, and the transform you write is the same either way.

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
    accTitle: L4 Detail -- Direct transport bridge
    accDescr: The previous stage pushes records to the intake listener, which hands them to Vector over loopback. Vector's output comes back to the return listener, which pushes it to the next stage, or sends a record over the size ceiling to the DLQ.

    subgraph IN["Previous stage"]
        direction LR
        prev("<b>Push client</b><br/><span>the stage in front</span>")
    end

    subgraph BR["dfe-transform-vector"]
        direction LR
        intake("<b>Intake listener</b><br/><span>source.listen</span>")
        vec("<b>Vector</b><br/><span>vector source and sink</span>")
        ret("<b>Return listener</b><br/><span>bridge.from_vector</span>")
        intake --> vec --> ret
    end

    subgraph OUT["Next stage"]
        direction LR
        next("<b>Push listener</b><br/><span>sink.endpoint</span>")
    end

    subgraph DL["Dead letters"]
        direction LR
        dlq[("<b>DLQ</b><br/><span>file spool</span>")]
    end

    IN -->|Push| BR
    BR -->|Push| OUT
    BR -.->|too large| DL
  class prev,next ext
  class intake,vec,ret part
  class dlq bus
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
  style IN fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style BR fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style OUT fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style DL fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
```

*Figure 1. L4 Detail -- Direct transport bridge.* Up: [L3 Components -- dfe-transform-vector](architecture.md).

## How it works

1. The previous stage pushes a batch to the intake listener.
2. The intake hands the records to Vector's `vector` source on `bridge.to_vector`, over loopback.
3. Vector runs the transforms, and its `vector` sink dials the return listener on `bridge.from_vector`.
4. The return listener pushes the records to `sink.endpoint`, or sends one over the next stage's size ceiling to the DLQ.
5. The intake answers its sender once the next hop has the records.

## Interfaces

In a DFE deployment the supervisor generates the `sources:` and `sinks:` blocks, so a whole template mounted as a transform file is refused. `templates/bus.yaml` and `templates/direct.yaml` both pass `vector validate` as they stand. Copy a template's `transforms:` block, not the file.

## Scaling and failure

Both listeners are built armed, so a push is held until the pipeline releases its records. A hop that rejects is retried until the hold runs out, then the push is answered `Unavailable` and its sender retries. The holds nest: 18 s at the intake, 16.5 s on the leg back from Vector and 15 s per send onward. An inner hop times out first, so the outer one can still answer its sender.

Only the intake sheds under memory pressure. The return leg drains everything the intake holds, so shedding it would stall the stage until the holds expired into duplicates. Both listeners lease held bytes on the runtime's memory guard.

A record over the next stage's size ceiling goes to the DLQ, or is dropped and counted with the DLQ off. A record Vector refused for good is released dropped and counted. A failed bridge leg stops the process with a non-zero exit, because its senders would otherwise retry forever against a live pod.

## Key metrics

| Metric | Healthy when |
|---|---|
| `pipeline_dead_letters_dropped_total{reason="too_large"}` | Flat. A rise is records over the next stage's ceiling with no DLQ |
| `pipeline_dead_letters_dropped_total{reason="rejected"}` | Flat. A rise is Vector refusing records for good |
| `records_delivered_total` | Tracks `records_received_total` |

## Related

- [Vector](vector.md), the process between the two legs
- [Config engine](internals.md), which writes the `vector` source and sink
