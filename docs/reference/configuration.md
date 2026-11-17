# Configuration

How dfe-transform-vector reads its config, and what the released chart fixes. Every key is in [config-schema.yaml](../config-schema.yaml).

## Config layers

The app's config layers in this order, each over the one before:

1. The defaults, which [config-schema.yaml](../config-schema.yaml) lists.
2. The config file: `--config`, else `config.yaml` or `config.yml` in the working directory.
3. `DFE_TRANSFORM_VECTOR_<SECTION>__<KEY>`, which reaches any key. A list takes brackets: `DFE_TRANSFORM_VECTOR_SOURCE__BROKERS='[a:9092,b:9092]'`.
4. The flat names, such as `DFE_TRANSFORM_VECTOR_SOURCE_BROKERS=a:9092,b:9092`, `_SINK_TOPIC`, `_TRANSFORMS_DIR`, `_VECTOR_BINARY` and the SASL pairs.

The working directory's `.env` is read into the environment before the env layers, and a variable set before it wins. `--log-level`, `--log-format` and `--metrics-addr`, or `LOG_LEVEL`, `LOG_FORMAT` and `METRICS_ADDR`, outrank `logging.*` and `metrics.address`. Nothing reads `DFE_TRANSFORM_VECTOR_CONFIG`: name the file with `--config`, before any subcommand.

Two flat names cover both endpoints. A `DFE_TRANSFORM_VECTOR_KAFKA_SECURITY_PROTOCOL` naming SSL, such as `SASL_SSL`, turns on `source.tls.enabled` and `sink.tls.enabled`, and any other value leaves them as they were. `DFE_TRANSFORM_VECTOR_KAFKA_SASL_MECHANISM` sets both SASL mechanisms, and the per-endpoint `_SOURCE_SASL_MECHANISM` and `_SINK_SASL_MECHANISM` beat it. `source.transport` and `sink.transport` take `kafka` for `bus` and `grpc` for `direct`, the names the other DFE stages use.

The same file seeds scalo's cascade as well, so scalo's sections in it apply: `version_check`, `scaling.enabled`, `scaling.memory_gate_threshold` and `metrics.namespace`. `dfe_source` fills in `<source>_land`, `<source>_load` and the consumer group, `dfe-transform-vector-<pipeline.name>` or `dfe-transform-vector-<source>` without a pipeline name, wherever the config leaves them at their defaults.

```yaml
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
    # them through its directory secret backend, and expands no ${VAR}.
    secret_dir: /var/run/secrets/dfe-kafka
  tls:
    enabled: true
sink:
  brokers: ["kafka:9092"]     # or DFE_TRANSFORM_VECTOR_SINK_BROKERS
  topic: enriched_syslog_land
  key_field: ".org_id"
  compression: zstd
  sasl:                       # each endpoint has its own SASL and TLS
    mechanism: scram_sha_512
    secret_dir: /var/run/secrets/dfe-kafka
  tls:
    enabled: true
transforms:
  dir: /etc/dfe-transform-vector-transforms   # a file set's mountPath, see The chart
```

## The chart

No chart is committed. At release hyperi-ci runs `generate-artefacts` for the deployment contract and assembles a thin chart on the scalo-service library chart, at the version `release.helm.library` names in `.hyperi-ci.yaml`. The library renders the Deployment, Service, ConfigMap, ServiceAccount and KEDA ScaledObject, and its README documents the values. To see the chart a release would ship, build the binary and run `hyperi-ci chart assemble --binary target/debug/dfe-transform-vector --image ghcr.io/hyperi-io/dfe-transform-vector:<tag>@sha256:<digest>`, which prints the directory it wrote.

`dfe-transform-vector emit-contract` prints what the contract fixes: the probes on the metrics port and their startup budget, the Push port when `config.source.transport` is `direct` or `grpc`, the Kafka Secret in `DFE_TRANSFORM_VECTOR_{SOURCE,SINK}_SASL_{USERNAME,PASSWORD}` and `DFE_TRANSFORM_VECTOR_KAFKA_SASL_MECHANISM`, the writable paths `data`, `run` and `dlq` beside the library's `/tmp` under a read-only root, the requests and limits, the grace period, and KEDA on CPU with no Kafka lag trigger.

The mounted config is the chart's `config` with `configOverrides` merged over it. The contract's `default_config` is never merged in, so the app's defaults apply to anything a deployment leaves unset, `vector.version_check: strict` included. The ConfigMap mounts over the config file's whole directory unless `configMount.subPath` is set, which hides the image's `/etc/dfe-transform-vector/transforms`: deliver transform files with a `fileSets` entry and point `transforms.dir` at its `mountPath`.
