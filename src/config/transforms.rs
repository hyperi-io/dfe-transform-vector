// Project:   dfe-transform-vector
// File:      src/config/transforms.rs
// Purpose:   Load user-supplied transform YAML files
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Transform YAML loading.
//!
//! Loads user-supplied Vector transform files from a directory or an
//! explicit file list, preserving ordering for DAG wiring.

use std::path::{Path, PathBuf};

use tracing::debug;

use super::loader::TransformConfig;
use crate::Result;

/// A loaded transform file with its parsed YAML content.
#[derive(Debug, Clone)]
pub struct LoadedTransform {
    /// Original file path (for diagnostics).
    pub path: PathBuf,
    /// Parsed YAML value.
    pub yaml: serde_yaml_ng::Value,
}

/// Load transform YAML files according to the config.
///
/// If `config.dir` is set, globs `*.yaml` and `*.yml` from the directory
/// (sorted by filename for deterministic ordering).
/// If `config.files` is set, loads files in declared order.
/// If neither is set, returns an empty list.
pub fn load_transforms(config: &TransformConfig) -> Result<Vec<LoadedTransform>> {
    if let Some(ref dir) = config.dir {
        load_from_directory(dir)
    } else if let Some(ref files) = config.files {
        load_from_file_list(files)
    } else {
        debug!("no transforms configured");
        Ok(vec![])
    }
}

/// Load all YAML files from a directory, sorted by filename.
fn load_from_directory(dir: &str) -> Result<Vec<LoadedTransform>> {
    let dir_path = Path::new(dir);
    if !dir_path.is_dir() {
        return Err(crate::Error::Config(format!(
            "transforms.dir does not exist or is not a directory: {dir}"
        )));
    }

    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir_path)
        .map_err(|e| crate::Error::Config(format!("failed to read transforms dir {dir}: {e}")))?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            match path.extension().and_then(|e| e.to_str()) {
                Some("yaml" | "yml") => Some(path),
                _ => None,
            }
        })
        .collect();

    // Sort by filename for deterministic ordering
    paths.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

    debug!(
        dir,
        count = paths.len(),
        "loading transforms from directory"
    );
    load_files(&paths)
}

/// Load transform files from an explicit path list.
fn load_from_file_list(files: &[String]) -> Result<Vec<LoadedTransform>> {
    let paths: Vec<PathBuf> = files.iter().map(PathBuf::from).collect();
    debug!(count = paths.len(), "loading transforms from file list");
    load_files(&paths)
}

/// Read and parse a list of YAML files.
fn load_files(paths: &[PathBuf]) -> Result<Vec<LoadedTransform>> {
    let mut transforms = Vec::with_capacity(paths.len());
    for path in paths {
        let content = std::fs::read_to_string(path).map_err(|e| {
            crate::Error::Config(format!(
                "failed to read transform file {}: {e}",
                path.display()
            ))
        })?;
        let yaml: serde_yaml_ng::Value = serde_yaml_ng::from_str(&content)?;
        debug!(path = %path.display(), "loaded transform file");
        transforms.push(LoadedTransform {
            path: path.clone(),
            yaml,
        });
    }
    Ok(transforms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn load_from_dir_sorts_by_name() {
        let dir = TempDir::new().unwrap();
        fs::write(
            dir.path().join("02_enrich.yaml"),
            "transforms:\n  enrich:\n    type: remap\n    inputs: [\"parse\"]\n    source: |\n      .x = 1\n",
        )
        .unwrap();
        fs::write(
            dir.path().join("01_parse.yaml"),
            "transforms:\n  parse:\n    type: remap\n    inputs: [\"dfe_source\"]\n    source: |\n      . = parse_json!(.message)\n",
        )
        .unwrap();
        // Non-yaml file should be ignored
        fs::write(dir.path().join("README.md"), "ignore me").unwrap();

        let config = TransformConfig {
            dir: Some(dir.path().to_string_lossy().into_owned()),
            files: None,
        };

        let loaded = load_transforms(&config).unwrap();
        assert_eq!(loaded.len(), 2);
        assert!(loaded[0].path.to_string_lossy().contains("01_parse"));
        assert!(loaded[1].path.to_string_lossy().contains("02_enrich"));
    }

    #[test]
    fn load_from_file_list_preserves_order() {
        let dir = TempDir::new().unwrap();
        let file_a = dir.path().join("a.yaml");
        let file_b = dir.path().join("b.yaml");
        fs::write(&file_a, "transforms:\n  a:\n    type: remap\n    inputs: [\"x\"]\n    source: |\n      .a = 1\n").unwrap();
        fs::write(&file_b, "transforms:\n  b:\n    type: remap\n    inputs: [\"a\"]\n    source: |\n      .b = 1\n").unwrap();

        let config = TransformConfig {
            dir: None,
            files: Some(vec![
                file_b.to_string_lossy().into_owned(),
                file_a.to_string_lossy().into_owned(),
            ]),
        };

        let loaded = load_transforms(&config).unwrap();
        assert_eq!(loaded.len(), 2);
        // Order should match file list, not alphabetical
        assert!(loaded[0].path.to_string_lossy().contains("b.yaml"));
        assert!(loaded[1].path.to_string_lossy().contains("a.yaml"));
    }

    #[test]
    fn load_empty_config_returns_empty() {
        let config = TransformConfig::default();
        let loaded = load_transforms(&config).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn load_missing_dir_errors() {
        let config = TransformConfig {
            dir: Some("/nonexistent/path".into()),
            files: None,
        };
        let err = load_transforms(&config).unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }
}
