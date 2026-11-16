<!--
Project:   dfe-transform-vector
File:      pipelines/filebeat/README.md
Purpose:   How the bundled filebeat pipeline is used and where it comes from
Language:  Markdown

License:   BUSL-1.1
Copyright: (c) 2026 HYPERI PTY LIMITED
-->

# Bundled pipeline: filebeat-compat (LEGACY)

A pre-canned transform file that keeps DFE/XDR 2.0 and 2.1 filebeat feeds working on dfe-transform-vector: the same VRL dfe-transform-vrl runs, wrapped in a Vector `remap` and carrying the enrichment table it needs. Point `transforms.dir` at this directory, mount the table, and the pipeline runs -- there is nothing to author. The modules, the routing rules, the departures from DFE 2.1 and the known limitations are documented once, in dfe-transform-vrl `pipelines/filebeat/README.md`, and apply here unchanged.

> **LEGACY.** Backwards compatibility for DFE/XDR 2.0 and 2.1 filebeat feeds, frozen at the Cisco IOS, Meraki and Umbrella modules. It takes no further modules and fetches nothing from Elastic at build, test or run time. Other feeds use `dfe-transform-elastic`, and 2.0 and 2.1 feeds should plan to move there.

```yaml
# config.yaml
dfe_source: "filebeat"                     # derives filebeat_land -> filebeat_load
transforms:
  dir: "/etc/dfe-transform-vector/transforms"
```

Mount `filebeat.yaml` into that directory and `timezones.csv` at the path the pipeline's `enrichment_tables` entry names (`/etc/dfe-transform-vector/data/timezones.csv`), or edit that one path to wherever the table lives inside `transforms.data_dir`. Both files are here.

The wrapper writes each transform file into Vector's config dir with its wired `inputs` added, and a transform file may declare a top-level `enrichment_tables` key, so Vector merges the declaration and the VRL's timezone lookups resolve. `vector validate` runs before Vector starts, so a pipeline that cannot compile stops the app rather than passing nothing on.

Events hold the DFE 2.1 Kafka fields (`{message, tags, timestamp}`), and the optional `._conf.tz_offset` / `._conf.tz_map` fields drive the ios and meraki timezone handling.

`filebeat.yaml` embeds dfe-transform-vrl `pipelines/filebeat/filebeat.vrl` @ `e0b7087`, the same program bar two lines of its header comment, and `timezones.csv` is that repo's table copied. Refresh by copying both again rather than editing here -- the two transform apps grade against the same cases (`tests/e2e/filebeat_kafka.rs` in each), and that means something as long as the program is the same one.
