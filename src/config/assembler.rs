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
    let global_yaml = generate_global_yaml(&config.vector, &secrets::backends(config, output_dir));
    write_yaml(output_dir, "00_global.yaml", &global_yaml)?;

    // Generate source YAML
    let source_yaml = generate_source_yaml(&config.source, &config.bridge);
    write_yaml(output_dir, "00_source.yaml", &source_yaml)?;

    // Load user transform files
    let transforms = load_transforms(&config.transforms)?;
    for lt in &transforms {
        refuse_out_of_scope_keys(lt, Path::new(&config.transforms.data_dir), output_dir)?;
    }

    // Extract components for DAG wiring
    let components = extract_components(&transforms)?;
    let wiring = auto_wire(components)?;

    // Validate the DAG
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
    );
    write_yaml(output_dir, "90_sink.yaml", &sink_yaml)?;

    // Generate observability YAML
    let obs_yaml = generate_observability_yaml(&config.metrics.vector_metrics_address);
    write_yaml(output_dir, "99_observability.yaml", &obs_yaml)?;

    info!(dir = %output_dir.display(), "assembled Vector config directory");
    Ok(output_dir.to_path_buf())
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
/// Injects auto-wired `inputs` into transform components that don't have
/// explicit inputs. Vector requires every transform to declare its inputs.
fn write_transforms(
    dir: &Path,
    transforms: &[LoadedTransform],
    wiring: &WiringResult,
) -> Result<()> {
    if transforms.is_empty() {
        return Ok(());
    }

    // Build a lookup from component label → wired inputs
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

/// Refuse a transform file that declares more than transforms and enrichment
/// tables loading from `data_dir`.
///
/// Vector runs whatever the file declares with its own reach, so a source,
/// sink, secret backend or out-of-dir table could read the Kafka credentials in
/// `config_dir`.
fn refuse_out_of_scope_keys(
    lt: &LoadedTransform,
    data_dir: &Path,
    config_dir: &Path,
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
                // Lua is the one transform type with file and process access.
                let lua = value
                    .as_object()
                    .into_iter()
                    .flatten()
                    .find(|(_, component)| {
                        component.get("type").and_then(serde_json::Value::as_str) == Some("lua")
                    });
                if let Some((label, _)) = lua {
                    return refuse(format!(
                        "declares transform '{label}' of type 'lua', which can read any file \
                         Vector can"
                    ));
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
                let secrets = resolve(config_dir);
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
                    if !file.is_absolute()
                        || !resolved.starts_with(&allowed)
                        || resolved.starts_with(&secrets)
                    {
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

/// Inject auto-wired `inputs` into transform components that lack them.
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
                && !component_map.contains_key(INPUTS_KEY)
            {
                let inputs_val =
                    Value::Array(inputs.iter().map(|s| Value::String(s.clone())).collect());
                component_map.insert(INPUTS_KEY.to_string(), inputs_val);
                debug!(label = %label, "injected auto-wired inputs into transform");
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
    /// the config dir is refused, YAML escapes and symlinks included.
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
                table("{data}/../config/.secrets/sink_sasl_password"),
                Some("outside transforms.data_dir"),
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
                (outcome, _) => panic!("{document}: {outcome:?}"),
            }
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
