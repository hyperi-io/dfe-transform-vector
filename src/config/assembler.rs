// Project:   dfe-transform-vector
// File:      src/config/assembler.rs
// Purpose:   Assemble Vector config directory from generated + user YAML
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Config assembler.
//!
//! Merges generated source/sink/observability YAML with user-supplied
//! transform files into a Vector `--config-dir` directory:
//!
//! ```text
//! <output_dir>/
//!   00_source.yaml
//!   50_transforms/
//!     01-parse.yaml
//!     02-enrich.yaml
//!   90_sink.yaml
//!   99_observability.yaml
//! ```

use std::path::{Path, PathBuf};

use tracing::{debug, info};

use super::generate::{generate_observability_yaml, generate_sink_yaml, generate_source_yaml};
use super::loader::Config;
use super::transforms::{LoadedTransform, load_transforms};
use super::wiring::{auto_wire, extract_components, validate_dag};
use crate::Result;

/// Default output directory for assembled Vector config.
pub const DEFAULT_CONFIG_DIR: &str = "/var/run/vector/config";

/// Assemble the full Vector config directory from big-dial config.
///
/// Returns the path to the assembled config directory.
pub fn assemble(config: &Config, output_dir: &Path) -> Result<PathBuf> {
    // Clean and create output directory
    if output_dir.exists() {
        std::fs::remove_dir_all(output_dir).map_err(|e| {
            crate::Error::Io(std::io::Error::new(
                e.kind(),
                format!("failed to clean config dir {}: {e}", output_dir.display()),
            ))
        })?;
    }
    std::fs::create_dir_all(output_dir)?;

    // Generate source YAML
    let source_yaml = generate_source_yaml(&config.source);
    write_yaml(output_dir, "00_source.yaml", &source_yaml)?;

    // Load user transform files
    let transforms = load_transforms(&config.transforms)?;

    // Extract components for DAG wiring
    let components = extract_components(&transforms)?;
    let wiring = auto_wire(components)?;

    // Validate the DAG
    validate_dag(&wiring)?;
    debug!("DAG validation passed");

    // Write transform files to subdirectory
    write_transforms(output_dir, &transforms)?;

    // Generate sink YAML with wired inputs
    let sink_yaml = generate_sink_yaml(&config.sink, &wiring.sink_inputs);
    write_yaml(output_dir, "90_sink.yaml", &sink_yaml)?;

    // Generate observability YAML
    let obs_yaml = generate_observability_yaml();
    write_yaml(output_dir, "99_observability.yaml", &obs_yaml)?;

    info!(dir = %output_dir.display(), "assembled Vector config directory");
    Ok(output_dir.to_path_buf())
}

/// Write a YAML value to a file in the output directory.
fn write_yaml(dir: &Path, filename: &str, value: &serde_yaml_ng::Value) -> Result<()> {
    let path = dir.join(filename);
    let content = serde_yaml_ng::to_string(value)?;
    std::fs::write(&path, content)?;
    debug!(path = %path.display(), "wrote config file");
    Ok(())
}

/// Write transform files into a `50_transforms/` subdirectory.
///
/// Preserves original filenames (prefixed with index for ordering
/// if the original names don't sort correctly).
fn write_transforms(dir: &Path, transforms: &[LoadedTransform]) -> Result<()> {
    if transforms.is_empty() {
        return Ok(());
    }

    let transforms_dir = dir.join("50_transforms");
    std::fs::create_dir_all(&transforms_dir)?;

    for (i, lt) in transforms.iter().enumerate() {
        let original_name = lt
            .path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("transform.yaml");

        // Prefix with zero-padded index to guarantee ordering
        let filename = format!("{:03}_{original_name}", i);
        let content = serde_yaml_ng::to_string(&lt.yaml)?;
        let path = transforms_dir.join(&filename);
        std::fs::write(&path, &content)?;
        debug!(path = %path.display(), "wrote transform file");
    }

    Ok(())
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::loader::{
        DecodingConfig, SaslConfig, SinkConfig, SourceConfig, TlsConfig, TransformConfig,
    };
    use std::fs;
    use tempfile::TempDir;

    fn test_config(transforms_dir: Option<String>) -> Config {
        Config {
            source: SourceConfig {
                brokers: vec!["kafka:9092".into()],
                topics: vec!["input".into()],
                group_id: "test-group".into(),
                decoding: DecodingConfig::default(),
                sasl: SaslConfig::default(),
                tls: TlsConfig::default(),
            },
            sink: SinkConfig {
                brokers: vec!["kafka:9092".into()],
                topic: "output".into(),
                key_field: ".id".into(),
                encoding: "json".into(),
                compression: "none".into(),
                sasl: SaslConfig::default(),
                tls: TlsConfig::default(),
            },
            transforms: TransformConfig {
                dir: transforms_dir,
                files: None,
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
        // No transforms dir when empty
        assert!(!output.path().join("50_transforms").exists());

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

        // Transforms dir should exist with 2 files
        let t_dir = output.path().join("50_transforms");
        assert!(t_dir.is_dir());
        let entries: Vec<_> = fs::read_dir(&t_dir).unwrap().collect();
        assert_eq!(entries.len(), 2);

        // Sink should wire to "filter" (terminal transform)
        let sink_content = fs::read_to_string(output.path().join("90_sink.yaml")).unwrap();
        assert!(sink_content.contains("filter"));
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
