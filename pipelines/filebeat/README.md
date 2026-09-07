<!--
Project:   dfe-transform-vector
File:      pipelines/filebeat/README.md
Purpose:   How the bundled filebeat pipeline is used and where it comes from
Language:  Markdown

License:   BUSL-1.1
Copyright: (c) 2026 HYPERI PTY LIMITED
-->

# Bundled pipeline: filebeat-compat (INTERIM)

A pre-canned transform file that keeps DFE 2.1 filebeat feeds working on
dfe-transform-vector: the same VRL dfe-transform-vrl runs, wrapped in a Vector
`remap` and carrying the enrichment table it needs. Point `transforms.dir` at
this directory and the pipeline runs -- there is nothing to author.

> **INTERIM.** Elastic compatibility is being replaced by
> `dfe-transform-elastic` (Rust-native, currently in beta). New integrations
> should NOT build on this pipeline.

## Using it

```yaml
# config.yaml
dfe_source: "filebeat"                     # derives filebeat_land -> filebeat_load
transforms:
  dir: "/etc/dfe-transform-vector/transforms"
```

Mount `filebeat.yaml` into that directory and `timezones.csv` at the path the
pipeline's `enrichment_tables` entry names
(`/etc/dfe-transform-vector/data/timezones.csv`), or edit that one path to
wherever the table lives. Both files are here.

The wrapper writes a transform file into Vector's config dir verbatim, which is
what lets a transform file declare a top-level `enrichment_tables` key: Vector
merges the declaration and the VRL's timezone lookups resolve. `vector validate`
runs before Vector starts, so a pipeline that cannot compile stops the app
rather than passing nothing on.

Events are the DFE 2.1 Kafka shape (`{message, tags, timestamp}`), and the
optional `._conf.tz_offset` / `._conf.tz_map` fields drive the ios and meraki
timezone handling.

## What it covers, and how it routes

The modules, the routing rules, the departures from DFE 2.1 and the known
divergences from the elastic goldens are documented once, in dfe-transform-vrl
`pipelines/filebeat/README.md`. They apply here unchanged: this is the same VRL.

## Provenance

`filebeat.yaml` embeds dfe-transform-vrl `pipelines/filebeat/filebeat.vrl`
@ `e0b7087`, byte-identical, and `timezones.csv` is that repo's table copied.
Refresh by copying both again rather than editing here -- the two transform
apps grade against the same corpus (`tests/e2e/filebeat_kafka.rs` in each), and
that only means something while the program is the same one.
