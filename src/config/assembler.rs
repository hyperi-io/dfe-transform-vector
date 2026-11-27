// Project:   dfe-transform-vector
// File:      src/config/assembler.rs
// Purpose:   Assemble Vector config directory from generated + user YAML
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Config assembler.
//!
//! Merges generated source/sink/observability YAML with user-supplied
//! transform files into a Vector `--config-dir` directory:
//!
//! ```text
//! <output_dir>/
//!   00_global.yaml
//!   00_source.yaml
//!   50_000_parse.yaml
//!   50_001_enrich.yaml
//!   90_sink.yaml
//!   99_observability.yaml
//!   .secrets/           SASL credentials given as text, owner-only
//! ```

use std::path::{Path, PathBuf};

use tracing::{debug, info};

use super::generate::{
    generate_global_yaml, generate_observability_yaml, generate_sink_yaml, generate_source_yaml,
};
use super::loader::Config;
use super::secrets;
use super::transforms::{LoadedTransform, load_transforms};
use super::validate::enrichment_table_file;
use super::wiring::{WiringResult, auto_wire, extract_components, validate_dag};
use crate::Result;

/// Default output directory for assembled Vector config.
pub const DEFAULT_CONFIG_DIR: &str = "/var/run/vector/config";

/// Assemble the full Vector config directory from big-dial config.
///
/// Returns the path to the assembled config directory.
///
/// # Errors
///
/// Returns an error when the output directory cannot be cleaned or written, the secret files cannot be materialised, a transform file fails to load, or the wired topology is invalid.
pub fn assemble(config: &Config, output_dir: &Path) -> Result<PathBuf> {
    assemble_for(config, output_dir, output_dir)?;
    Ok(output_dir.to_path_buf())
}

/// Assemble into `output_dir` a config that Vector will read from `published`.
///
/// The secret backends name `published`'s credential directory, so a
/// directory assembled aside and then moved to `published` reads its own
/// credentials there.
///
/// # Errors
///
/// As [`assemble`].
pub fn assemble_for(config: &Config, output_dir: &Path, published: &Path) -> Result<()> {
    // Clean and create output directory. The credential directory stays: a
    // process that restarts Vector mid-reload reads it, and a directory removed
    // and rebuilt is missing for a moment. `secrets::materialise` replaces its
    // files whole and drops the ones no longer needed.
    if output_dir.exists() {
        clean_dir(output_dir).map_err(|e| {
            crate::Error::Io(std::io::Error::new(
                e.kind(),
                format!("failed to clean config dir {}: {e}", output_dir.display()),
            ))
        })?;
    }
    std::fs::create_dir_all(output_dir)?;

    // Credentials go where the secret backends read them, never into the YAML.
    secrets::materialise(config, output_dir)?;

    // Generate global config (data_dir, API, secret backends)
    let global_yaml = generate_global_yaml(&config.vector, &secrets::backends(config, published));
    write_yaml(output_dir, "00_global.yaml", &global_yaml)?;

    // Generate source YAML
    let source_yaml = generate_source_yaml(&config.source, &config.bridge);
    write_yaml(output_dir, "00_source.yaml", &source_yaml)?;

    // Load user transform files
    let transforms = load_transforms(&config.transforms)?;
    let data_dir = Path::new(&config.transforms.data_dir);
    let credentials = credential_paths(config, published, output_dir);
    for lt in &transforms {
        refuse_out_of_scope_keys(lt, data_dir, &credentials)?;
    }

    // Extract components for DAG wiring
    let components = extract_components(&transforms)?;
    let wiring = auto_wire(components)?;

    validate_dag(&wiring)?;
    debug!("DAG validation passed");

    // Write transform files to subdirectory (with auto-wired inputs injected)
    write_transforms(output_dir, &transforms, &wiring)?;

    // Generate sink YAML with wired inputs
    let sink_yaml = generate_sink_yaml(
        &config.sink,
        &config.bridge,
        &wiring.sink_inputs,
        config.source.acknowledgements.enabled,
        &config.dlq.0,
    );
    write_yaml(output_dir, "90_sink.yaml", &sink_yaml)?;

    // Generate observability YAML
    let obs_yaml = generate_observability_yaml(&config.metrics.vector_metrics_address);
    write_yaml(output_dir, "99_observability.yaml", &obs_yaml)?;

    info!(dir = %output_dir.display(), "assembled Vector config directory");
    Ok(())
}

/// Remove everything in `dir` except the credential directory.
fn clean_dir(dir: &Path) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_name() == secrets::PRIVATE_DIR {
            continue;
        }
        if entry.file_type()?.is_dir() {
            std::fs::remove_dir_all(entry.path())?;
        } else {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

/// Write a YAML value to a file in the output directory.
fn write_yaml(dir: &Path, filename: &str, value: &serde_json::Value) -> Result<()> {
    let path = dir.join(filename);
    let content = scalo::yaml::to_string(value)?;
    std::fs::write(&path, content)?;
    debug!(path = %path.display(), "wrote config file");
    Ok(())
}

/// Write transform files into the config directory.
///
/// Files are written flat (not in a subdirectory) because Vector's
/// `--config-dir` does not recurse into subdirectories. Each file is
/// prefixed with `50_` to sort after source (00_) and before sink (90_).
///
/// Writes every transform's wired `inputs` into it. Vector requires every
/// transform to declare its inputs.
fn write_transforms(
    dir: &Path,
    transforms: &[LoadedTransform],
    wiring: &WiringResult,
) -> Result<()> {
    if transforms.is_empty() {
        return Ok(());
    }

    // Build a lookup from component label -> wired inputs
    let mut wired_inputs: std::collections::HashMap<&str, &[String]> =
        std::collections::HashMap::new();
    for c in &wiring.components {
        if !c.inputs.is_empty() {
            wired_inputs.insert(&c.label, &c.inputs);
        }
    }

    for (i, lt) in transforms.iter().enumerate() {
        let mut yaml = lt.yaml.clone();
        inject_wired_inputs(&mut yaml, &wired_inputs);

        let original_name = lt
            .path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("transform.yaml");

        let filename = format!("50_{i:03}_{original_name}");
        let content = scalo::yaml::to_string(&yaml)?;
        refuse_reserved_secret_refs(&lt.path, &content)?;
        let path = dir.join(&filename);
        std::fs::write(&path, &content)?;
        debug!(path = %path.display(), "wrote transform file");
    }

    Ok(())
}

/// Refuse a transform that names one of the assembler's own secret backends.
///
/// Vector replaces a `SECRET[<backend>.<key>]` anywhere in the file it reads,
/// VRL included, so the check reads the text written, with the YAML escapes in
/// the operator's file already decoded.
fn refuse_reserved_secret_refs(source: &Path, written: &str) -> Result<()> {
    for backend in secrets::RESERVED_BACKENDS {
        if written.contains(&format!("SECRET[{backend}.")) {
            return Err(crate::Error::Validation(format!(
                "transform file {} references the secret backend '{backend}', which holds the \
                 Kafka credentials and is not for transforms",
                source.display()
            )));
        }
    }
    Ok(())
}

/// Every path holding a Kafka credential Vector can read: both config
/// directories, each secret backend's directory and each TLS key.
fn credential_paths(config: &Config, published: &Path, output_dir: &Path) -> Vec<PathBuf> {
    let mut paths = vec![published.to_path_buf(), output_dir.to_path_buf()];
    paths.extend(
        secrets::backends(config, published)
            .into_iter()
            .map(|backend| backend.dir),
    );
    paths.extend(
        [&config.source.tls, &config.sink.tls]
            .into_iter()
            .filter(|tls| tls.enabled)
            .filter_map(|tls| tls.key_file.as_ref().map(PathBuf::from)),
    );
    paths
}

/// What a refused VRL function that reads a file does.
const READS_A_FILE: &str = "reads the file the program names";

/// What a refused VRL decompression function does.
const INFLATES: &str = "inflates a record with no bound on its size";

/// What a refused VRL network function does.
const REACHES_THE_NETWORK: &str = "reaches the network";

/// VRL functions a transform may not call, with what each does that the pod
/// cannot allow.
///
/// The functions dfe-transform-vrl's compiler leaves out: its `REFUSED_FUNCTIONS`
/// and the network functions it builds without.
const REFUSED_VRL_CALLS: [(&str, &str); 12] = [
    ("get_hostname", "reads the host's name"),
    ("http_request", REACHES_THE_NETWORK),
    ("dns_lookup", REACHES_THE_NETWORK),
    ("reverse_dns", REACHES_THE_NETWORK),
    ("parse_proto", READS_A_FILE),
    ("encode_proto", READS_A_FILE),
    ("validate_json_schema", READS_A_FILE),
    ("decode_gzip", INFLATES),
    ("decode_zlib", INFLATES),
    ("decode_zstd", INFLATES),
    ("decode_lz4", INFLATES),
    ("decode_snappy", INFLATES),
];

/// VRL arguments that name a file to read, each with the function taking it.
const REFUSED_VRL_ARGUMENTS: [(&str, &str); 2] =
    [("parse_etld", "psl"), ("parse_groks", "alias_sources")];

/// Refuse a transform file that declares more than transforms and enrichment
/// tables loading from `data_dir`, or a transform that reads a file or
/// inflates a record without a bound.
///
/// Vector runs whatever the file declares with its own reach, so a source,
/// sink, secret backend, out-of-dir table or file-reading transform could read
/// the Kafka credentials under any of `credentials`, which may sit inside
/// `data_dir` too.
fn refuse_out_of_scope_keys(
    lt: &LoadedTransform,
    data_dir: &Path,
    credentials: &[PathBuf],
) -> Result<()> {
    let refuse = |what: String| {
        Err(crate::Error::Validation(format!(
            "transform file {} {what}",
            lt.path.display()
        )))
    };
    let Some(document) = lt.yaml.as_object() else {
        return Ok(());
    };
    for (key, value) in document {
        match key.as_str() {
            "transforms" => {
                for (label, component) in value.as_object().into_iter().flatten() {
                    if let Some(why) = refused_transform(component) {
                        return refuse(format!("declares transform '{label}' {why}"));
                    }
                }
            }
            "enrichment_tables" => {
                let Some(tables) = value.as_object() else {
                    return refuse("declares enrichment_tables that are not a mapping".into());
                };
                if !data_dir.is_absolute() {
                    return refuse(format!(
                        "declares enrichment tables, and transforms.data_dir '{}' is not an \
                         absolute path",
                        data_dir.display()
                    ));
                }
                let allowed = resolve(data_dir);
                let secrets: Vec<PathBuf> = credentials.iter().map(|d| resolve(d)).collect();
                for (name, table) in tables {
                    if table.get("type").and_then(serde_json::Value::as_str) == Some("memory") {
                        continue;
                    }
                    let Some(file) = enrichment_table_file(table) else {
                        return refuse(format!(
                            "declares enrichment table '{name}' of a type that names no file \
                             this check can place"
                        ));
                    };
                    let file = Path::new(file);
                    let resolved = resolve(file);
                    if file.is_absolute() && secrets.iter().any(|s| resolved.starts_with(s)) {
                        return refuse(format!(
                            "declares enrichment table '{name}' loading {}, which holds a Kafka \
                             credential",
                            file.display()
                        ));
                    }
                    if !file.is_absolute() || !resolved.starts_with(&allowed) {
                        return refuse(format!(
                            "declares enrichment table '{name}' loading {}, which is outside \
                             transforms.data_dir {}",
                            file.display(),
                            data_dir.display()
                        ));
                    }
                }
            }
            other => {
                return refuse(format!(
                    "declares top-level key '{other}', but a transform file may declare only \
                     transforms and enrichment_tables"
                ));
            }
        }
    }
    Ok(())
}

/// Why Vector may not run `component`, or `None` when it may.
fn refused_transform(component: &serde_json::Value) -> Option<String> {
    // Lua is the one transform type with file and process access.
    if component.get("type").and_then(serde_json::Value::as_str) == Some("lua") {
        return Some("of type 'lua', which can read any file Vector can".into());
    }
    // A `remap` loads its VRL from these, and Vector quotes a file it cannot
    // compile in its validate report.
    if let Some(key) = ["file", "files"]
        .into_iter()
        .find(|key| component.get(key).is_some())
    {
        return Some(format!(
            "that sets '{key}', which loads a file Vector can read, credentials included"
        ));
    }
    // VRL can sit in any string a transform holds: a `source`, a `condition`, a route.
    let mut texts = Vec::new();
    collect_strings(component, &mut texts);
    texts.into_iter().find_map(refused_vrl)
}

/// Push every string in `value`, at any depth, onto `out`.
fn collect_strings<'a>(value: &'a serde_json::Value, out: &mut Vec<&'a str>) {
    match value {
        serde_json::Value::String(text) => out.push(text),
        serde_json::Value::Array(items) => {
            for item in items {
                collect_strings(item, out);
            }
        }
        serde_json::Value::Object(fields) => {
            for field in fields.values() {
                collect_strings(field, out);
            }
        }
        _ => {}
    }
}

/// Why `vrl` may not run: a refused call or a file argument, or `None`.
fn refused_vrl(vrl: &str) -> Option<String> {
    if let Some((function, does)) = REFUSED_VRL_CALLS
        .iter()
        .find(|(function, _)| calls(vrl, function))
    {
        return Some(format!(
            "that calls the VRL function '{function}', which {does}"
        ));
    }
    REFUSED_VRL_ARGUMENTS
        .iter()
        .find(|(function, argument)| calls(vrl, function) && passes(vrl, argument))
        .map(|(function, argument)| {
            format!(
                "that passes '{argument}' to the VRL function '{function}', which reads the \
                 file it names"
            )
        })
}

/// Whether `vrl` calls `function`: its name, an optional `!`, then `(`.
///
/// VRL builds no call from a string, so the text is the program. A match in a
/// comment, or with a space VRL rejects before the `(`, refuses needlessly
/// rather than miss a call.
fn calls(vrl: &str, function: &str) -> bool {
    identifier_followed_by(vrl, function, |rest| {
        let rest = rest.trim_start();
        rest.strip_prefix('!')
            .unwrap_or(rest)
            .trim_start()
            .starts_with('(')
    })
}

/// Whether `vrl` passes the named argument `argument`: its name, then `:`.
fn passes(vrl: &str, argument: &str) -> bool {
    identifier_followed_by(vrl, argument, |rest| rest.trim_start().starts_with(':'))
}

/// Whether `name` stands in `vrl` as a whole identifier with `rest_matches`
/// true of the text after it.
fn identifier_followed_by(vrl: &str, name: &str, rest_matches: impl Fn(&str) -> bool) -> bool {
    let identifier = |c: char| c.is_ascii_alphanumeric() || c == '_';
    vrl.match_indices(name).any(|(at, _)| {
        !vrl[..at].chars().next_back().is_some_and(identifier)
            && rest_matches(&vrl[at + name.len()..])
    })
}

/// `path` as the OS resolves it, `..` and symlinks included, or with `.` and
/// `..` folded when it does not exist yet.
fn resolve(path: &Path) -> PathBuf {
    if let Ok(real) = path.canonicalize() {
        return real;
    }
    let mut folded = PathBuf::new();
    for part in path.components() {
        match part {
            std::path::Component::ParentDir => {
                folded.pop();
            }
            std::path::Component::CurDir => {}
            other => folded.push(other),
        }
    }
    folded
}

/// Write each transform's wired `inputs` over whatever the file declared, so
/// Vector reads the topology the DAG check passed: `source` as `dfe_source`,
/// and an empty or missing list wired from the source.
fn inject_wired_inputs(
    yaml: &mut serde_json::Value,
    wired_inputs: &std::collections::HashMap<&str, &[String]>,
) {
    use serde_json::Value;

    const INPUTS_KEY: &str = "inputs";

    if let Some(Value::Object(entries)) = yaml.get_mut("transforms") {
        for (label, component_val) in entries.iter_mut() {
            if let Some(inputs) = wired_inputs.get(label.as_str())
                && let Value::Object(component_map) = component_val
            {
                let inputs_val =
                    Value::Array(inputs.iter().map(|s| Value::String(s.clone())).collect());
                component_map.insert(INPUTS_KEY.to_string(), inputs_val);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::loader::{SinkConfig, SourceConfig, TransformConfig};
    use std::fs;
    use tempfile::TempDir;

    fn test_config(transforms_dir: Option<String>) -> Config {
        Config {
            source: SourceConfig {
                brokers: vec!["kafka:9092".into()],
                topics: vec!["input".into()],
                group_id: "test-group".into(),
                ..Default::default()
            },
            sink: SinkConfig {
                brokers: vec!["kafka:9092".into()],
                topic: "output".into(),
                key_field: ".id".into(),
                encoding: "json".into(),
                compression: "none".into(),
                ..Default::default()
            },
            transforms: TransformConfig {
                dir: transforms_dir,
                files: None,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn assemble_no_transforms() {
        let output = TempDir::new().unwrap();
        let config = test_config(None);

        let result = assemble(&config, output.path()).unwrap();
        assert_eq!(result, output.path());

        // Check files exist
        assert!(output.path().join("00_source.yaml").exists());
        assert!(output.path().join("90_sink.yaml").exists());
        assert!(output.path().join("99_observability.yaml").exists());
        // No transform files when empty
        let transform_files: Vec<_> = fs::read_dir(output.path())
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().starts_with("50_"))
            .collect();
        assert!(transform_files.is_empty());

        // Source should reference dfe_source
        let source_content = fs::read_to_string(output.path().join("00_source.yaml")).unwrap();
        assert!(source_content.contains("dfe_source"));

        // Sink inputs should be dfe_source (direct wire)
        let sink_content = fs::read_to_string(output.path().join("90_sink.yaml")).unwrap();
        assert!(sink_content.contains("dfe_source"));
    }

    #[test]
    fn assemble_with_transforms() {
        let transforms_dir = TempDir::new().unwrap();
        fs::write(
            transforms_dir.path().join("01_parse.yaml"),
            "transforms:\n  parse:\n    type: remap\n    inputs:\n      - dfe_source\n    source: |\n      . = parse_json!(.message)\n",
        )
        .unwrap();
        fs::write(
            transforms_dir.path().join("02_filter.yaml"),
            "transforms:\n  filter:\n    type: filter\n    inputs:\n      - parse\n    condition: 'true'\n",
        )
        .unwrap();

        let output = TempDir::new().unwrap();
        let config = test_config(Some(transforms_dir.path().to_string_lossy().into_owned()));

        assemble(&config, output.path()).unwrap();

        // Transform files should exist in the config dir (flat, prefixed with 50_)
        let transform_files: Vec<_> = fs::read_dir(output.path())
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().starts_with("50_"))
            .collect();
        assert_eq!(transform_files.len(), 2);

        // Sink should wire to "filter" (terminal transform)
        let sink_content = fs::read_to_string(output.path().join("90_sink.yaml")).unwrap();
        assert!(sink_content.contains("filter"));
    }

    /// Vector runs a transform file with its own reach and substitutes
    /// `SECRET[...]` in VRL text too, so any route to the Kafka credentials in
    /// the config dir is refused, YAML escapes and symlinks included. So is VRL,
    /// in any transform, that reads a file or inflates a record.
    #[test]
    fn a_transform_file_reaching_the_credentials_is_refused() {
        const REMAP: &str =
            "transforms:\n  leak:\n    type: remap\n    inputs: [dfe_source]\n    source: ";
        let remap = |source: &str| format!("{REMAP}{source}\n");
        let table = |path: &str| {
            format!(
                "enrichment_tables:\n  t:\n    type: file\n    file:\n      path: {path}\n      \
                 encoding:\n        type: csv\n"
            )
        };
        for (document, refused) in [
            (
                remap(r#"'.x = "SECRET[dfe_credentials.sink_sasl_password]"'"#),
                Some("dfe_credentials"),
            ),
            (
                remap(r#"'.x = "SECRET[dfe_source_sasl.password]"'"#),
                Some("dfe_source_sasl"),
            ),
            (
                remap(r#"'.x = "SECRET[dfe_sink_sasl.username]"'"#),
                Some("dfe_sink_sasl"),
            ),
            (
                remap(r#"".x = \"\x53ECRET[dfe_credentials.sink_sasl_password]\"""#),
                Some("dfe_credentials"),
            ),
            (
                remap(r#"".x = \"\u0053ECRET[dfe_sink_sasl.password]\"""#),
                Some("dfe_sink_sasl"),
            ),
            (remap(r#"'.x = "SECRET[operator_vault.api_key]"'"#), None),
            (remap(r#"'.x = "dfe_credentials"'"#), None),
            (
                "sources:\n  s:\n    type: file\n    include: ['{config}/.secrets/*']\n".into(),
                Some("'sources'"),
            ),
            (
                "sinks:\n  s:\n    type: console\n    inputs: [dfe_source]\n".into(),
                Some("'sinks'"),
            ),
            (
                "secret:\n  b:\n    type: directory\n    path: '{config}/.secrets'\n".into(),
                Some("'secret'"),
            ),
            ("api:\n  enabled: true\n".into(), Some("'api'")),
            (
                "transforms:\n  l:\n    type: lua\n    version: '2'\n    inputs: [dfe_source]\n    \
                 hooks:\n      process: process\n"
                    .into(),
                Some("type 'lua'"),
            ),
            (
                "transforms:\n  f:\n    type: remap\n    inputs: [dfe_source]\n    \
                 file: '{config}/.secrets/sink_sasl_password'\n"
                    .into(),
                Some("sets 'file'"),
            ),
            (
                "transforms:\n  f:\n    type: remap\n    inputs: [dfe_source]\n    \
                 files: ['{config}/.secrets/sink_sasl_password']\n"
                    .into(),
                Some("sets 'files'"),
            ),
            (
                "transforms:\n  f:\n    type: reduce\n    inputs: [dfe_source]\n    \
                 file: '{config}/.secrets/sink_sasl_password'\n"
                    .into(),
                Some("sets 'file'"),
            ),
            (remap(r#"'.h = get_hostname!()'"#), Some("'get_hostname'")),
            (
                remap(r#"'.r = http_request("http://x")'"#),
                Some("'http_request'"),
            ),
            (remap(r#"'.a = dns_lookup!(.h)'"#), Some("'dns_lookup'")),
            (remap(r#"'.h = reverse_dns!(.ip)'"#), Some("'reverse_dns'")),
            (
                remap(r#"'. = parse_proto!(.m, "/x.desc", "a.B")'"#),
                Some("'parse_proto'"),
            ),
            (
                remap(r#"'.m = encode_proto!(., "/x.desc", "a.B")'"#),
                Some("'encode_proto'"),
            ),
            (
                remap(r#"'.ok = validate_json_schema!(.m, "/x.json")'"#),
                Some("'validate_json_schema'"),
            ),
            (remap(r#"'.m = decode_gzip!(.m)'"#), Some("'decode_gzip'")),
            (remap(r#"'.m = decode_zlib(.m) ?? .m'"#), Some("'decode_zlib'")),
            (remap(r#"'.m = decode_zstd! (.m)'"#), Some("'decode_zstd'")),
            (remap(r#"'.m = decode_lz4!(.m)'"#), Some("'decode_lz4'")),
            (remap(r#"'.m = decode_snappy!(.m)'"#), Some("'decode_snappy'")),
            (
                "transforms:\n  f:\n    type: filter\n    inputs: [dfe_source]\n    \
                 condition: 'length(decode_gzip!(.m)) > 0'\n"
                    .into(),
                Some("'decode_gzip'"),
            ),
            (
                remap(r#"'.d = parse_etld!(.host, psl: "/x.dat")'"#),
                Some("'psl'"),
            ),
            (
                remap(r#"'. = parse_groks!(.m, patterns: ["%{x}"], alias_sources: ["/x"])'"#),
                Some("'alias_sources'"),
            ),
            (remap(r#"'.d = parse_etld!(.host)'"#), None),
            (remap(r#"'.note = "decode_gzip"'"#), None),
            (remap(r#"'.x = encode_gzip(.m)'"#), None),
            (remap(r#"'.p = get_env_var("DFE_PIPELINE_NAME") ?? "x"'"#), None),
            (
                table("{data}/../config/.secrets/sink_sasl_password"),
                Some("holds a Kafka credential"),
            ),
            (
                table("{data}/link.csv"),
                Some("outside transforms.data_dir"),
            ),
            (table("{data}/zones.csv"), None),
        ] {
            let work = TempDir::new().unwrap();
            let data = work.path().join("data");
            let config_dir = work.path().join("config");
            let transforms = work.path().join("transforms");
            for dir in [&data, &config_dir, &transforms] {
                fs::create_dir_all(dir).unwrap();
            }
            fs::write(data.join("zones.csv"), "abbreviation\nAEST\n").unwrap();
            fs::write(work.path().join("outside.csv"), "abbreviation\nAEST\n").unwrap();
            std::os::unix::fs::symlink(work.path().join("outside.csv"), data.join("link.csv"))
                .unwrap();
            let document = document
                .replace("{data}", &data.to_string_lossy())
                .replace("{config}", &config_dir.to_string_lossy());
            fs::write(transforms.join("leak.yaml"), &document).unwrap();
            let mut config = test_config(Some(transforms.to_string_lossy().into_owned()));
            config.transforms.data_dir = data.to_string_lossy().into_owned();
            match (assemble(&config, &config_dir), refused) {
                (Ok(_), None) => {}
                (Err(e), Some(why)) => {
                    let e = e.to_string();
                    assert!(
                        e.contains("leak.yaml") && e.contains(why),
                        "{document}: {e}"
                    );
                }
                // Names the outcome only: the assembled config carries credentials.
                (Ok(_), Some(why)) => panic!("{document}: assembled, expected a {why} refusal"),
                (Err(e), None) => panic!("{document}: refused: {e}"),
            }
        }
    }

    /// A mounted SASL directory or a TLS key inside `transforms.data_dir` is a
    /// credential, not a table, wherever it sits.
    #[test]
    fn a_table_loading_a_credential_inside_the_data_dir_is_refused() {
        let work = TempDir::new().unwrap();
        let data = work.path().join("data");
        let kafka = data.join("kafka");
        let transforms = work.path().join("transforms");
        for dir in [&kafka, &transforms] {
            fs::create_dir_all(dir).unwrap();
        }
        for file in ["username", "password"] {
            fs::write(kafka.join(file), "x\n").unwrap();
        }
        fs::write(data.join("client.key"), "key\n").unwrap();
        fs::write(data.join("zones.csv"), "abbreviation\nAEST\n").unwrap();

        for (table, side, refused) in [
            ("kafka/password", "source", true),
            ("kafka/username", "sink", true),
            ("client.key", "source", true),
            ("client.key", "sink", true),
            ("zones.csv", "source", false),
        ] {
            fs::write(
                transforms.join("t.yaml"),
                format!(
                    "enrichment_tables:\n  t:\n    type: file\n    file:\n      path: {}\n      \
                     encoding:\n        type: csv\n",
                    data.join(table).display()
                ),
            )
            .unwrap();
            let mut config = test_config(Some(transforms.to_string_lossy().into_owned()));
            config.transforms.data_dir = data.to_string_lossy().into_owned();
            let (sasl, tls) = if side == "source" {
                (&mut config.source.sasl, &mut config.source.tls)
            } else {
                (&mut config.sink.sasl, &mut config.sink.tls)
            };
            sasl.enabled = true;
            sasl.secret_dir = Some(kafka.to_string_lossy().into_owned());
            tls.enabled = true;
            tls.key_file = Some(data.join("client.key").to_string_lossy().into_owned());

            let out = work.path().join("out");
            match (assemble(&config, &out), refused) {
                (Err(e), true) => assert!(
                    e.to_string().contains("credential"),
                    "{side} {table}: refused for the wrong reason: {e}"
                ),
                (Ok(_), false) => {}
                (Ok(_), true) => {
                    panic!("{side} {table}: a table reads a credential, and it passed")
                }
                (Err(e), false) => panic!("{side} {table}: refused: {e}"),
            }
        }
    }

    /// Vector reads the file the assembler writes, so the wiring the DAG check
    /// passed is what that file carries: `source` as `dfe_source`, and an empty
    /// or missing list wired from the source.
    #[test]
    fn the_written_transform_carries_the_wired_inputs() {
        for (inputs, want) in [
            ("inputs: [source]\n    ", vec!["dfe_source"]),
            (
                "inputs: [source, dfe_source]\n    ",
                vec!["dfe_source", "dfe_source"],
            ),
            ("inputs: []\n    ", vec!["dfe_source"]),
            ("", vec!["dfe_source"]),
            ("inputs: dfe_source\n    ", vec!["dfe_source"]),
        ] {
            let transforms_dir = TempDir::new().unwrap();
            fs::write(
                transforms_dir.path().join("01_parse.yaml"),
                format!("transforms:\n  parse:\n    type: remap\n    {inputs}source: '.x = 1'\n"),
            )
            .unwrap();
            let output = TempDir::new().unwrap();
            let config = test_config(Some(transforms_dir.path().to_string_lossy().into_owned()));
            assemble(&config, output.path()).unwrap();

            let written = fs::read_to_string(output.path().join("50_000_01_parse.yaml")).unwrap();
            let yaml: serde_json::Value =
                scalo::yaml::from_str(&written, "t.yaml", scalo::yaml::Profile::Config).unwrap();
            let got: Vec<&str> = yaml["transforms"]["parse"]["inputs"]
                .as_array()
                .unwrap_or_else(|| panic!("{inputs:?}: no inputs list in {written}"))
                .iter()
                .filter_map(serde_json::Value::as_str)
                .collect();
            assert_eq!(got, want, "{inputs:?}: {written}");
        }
    }

    #[test]
    fn assemble_cleans_existing_dir() {
        let output = TempDir::new().unwrap();
        // Create a stale file
        fs::write(output.path().join("stale.yaml"), "stale").unwrap();

        let config = test_config(None);
        assemble(&config, output.path()).unwrap();

        // Stale file should be gone
        assert!(!output.path().join("stale.yaml").exists());
    }
}
