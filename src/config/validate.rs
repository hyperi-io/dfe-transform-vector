// Project:   dfe-transform-vector
// File:      src/config/validate.rs
// Purpose:   Vector validate command integration
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Vector validate integration.
//!
//! Shells out to `vector validate --config-dir <path>` and interprets
//! the exit code:
//! - 0: valid configuration
//! - 78: configuration error (YAML syntax, VRL type mismatch, etc.)
//! - Other: system error (binary not found, permissions, etc.)

use std::path::Path;
use std::process::Stdio;

use hyperi_rustlib::logger::security;
use tracing::{debug, error};

use super::loader::VectorConfig;
use crate::Result;

/// Exit code for Vector configuration errors.
const VECTOR_CONFIG_ERROR_EXIT: i32 = 78;

/// Run `vector validate` against the assembled config directory.
///
/// Returns `Ok(())` if validation passes, or an error with details
/// from Vector's stderr output.
pub async fn vector_validate(vector_config: &VectorConfig, config_dir: &Path) -> Result<()> {
    let binary = &vector_config.binary;

    debug!(
        binary = %binary,
        config_dir = %config_dir.display(),
        "running vector validate"
    );

    let mut cmd = tokio::process::Command::new(binary);
    cmd.arg("validate")
        .arg("--config-dir")
        .arg(config_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    // Vector needs a writable data_dir even for validate
    if !vector_config.data_dir.is_empty() {
        cmd.env("VECTOR_DATA_DIR", &vector_config.data_dir);
    }

    let output = cmd
        .output()
        .await
        .map_err(|e| crate::Error::Vector(format!("failed to run '{binary} validate': {e}")))?;

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    if output.status.success() {
        debug!("vector validate passed");
        return Ok(());
    }

    let exit_code = output.status.code().unwrap_or(-1);

    if exit_code == VECTOR_CONFIG_ERROR_EXIT {
        error!(exit_code, stderr = %stderr, "vector config validation failed");
        return Err(crate::Error::Validation(format!(
            "Vector config validation failed (exit {exit_code}): {stderr}"
        )));
    }

    // Unexpected exit code
    error!(
        exit_code,
        stderr = %stderr,
        stdout = %stdout,
        "vector validate exited with unexpected code"
    );
    Err(crate::Error::Vector(format!(
        "vector validate exited with code {exit_code}: {stderr}"
    )))
}

/// Check the Vector binary version against the configured pin.
///
/// Runs `vector --version` and compares with `vector_config.version`.
/// Behaviour depends on `vector_config.version_check`:
/// - `strict`: error if versions don't match
/// - `warn`: log warning if versions don't match
/// - `disabled`: skip check entirely
pub async fn check_vector_version(vector_config: &VectorConfig) -> Result<String> {
    if vector_config.version_check == "disabled" {
        debug!("vector version check disabled");
        return Ok(String::new());
    }

    let binary = &vector_config.binary;
    let output = tokio::process::Command::new(binary)
        .arg("--version")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|e| crate::Error::Vector(format!("failed to run '{binary} --version': {e}")))?;

    if !output.status.success() {
        return Err(crate::Error::Vector(format!(
            "'{binary} --version' failed with exit code {}",
            output.status.code().unwrap_or(-1)
        )));
    }

    // Parse version from output like "vector 0.48.0 (aarch64-unknown-linux-gnu ...)"
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let version = parse_vector_version(&stdout);

    debug!(detected = %version, "vector binary version");

    // Compare with config pin if set
    if !vector_config.version.is_empty() && version != vector_config.version {
        let msg = format!(
            "Vector version mismatch: expected '{}', got '{}'",
            vector_config.version, version
        );
        security::data_quality_alert("version_check", &msg);
        match vector_config.version_check.as_str() {
            "strict" => {
                error!("{}", msg);
                return Err(crate::Error::Vector(msg));
            }
            "warn" => {
                tracing::warn!("{}", msg);
            }
            _ => {}
        }
    }

    Ok(version)
}

/// Parse the version string from `vector --version` output.
///
/// Expects format like: `vector 0.48.0 (aarch64-unknown-linux-gnu ...)`
fn parse_vector_version(output: &str) -> String {
    output
        .split_whitespace()
        .nth(1)
        .unwrap_or("unknown")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_version_standard() {
        let output = "vector 0.48.0 (x86_64-unknown-linux-gnu 2024-01-01)";
        assert_eq!(parse_vector_version(output), "0.48.0");
    }

    #[test]
    fn parse_version_no_extra() {
        let output = "vector 0.48.0";
        assert_eq!(parse_vector_version(output), "0.48.0");
    }

    #[test]
    fn parse_version_empty() {
        assert_eq!(parse_vector_version(""), "unknown");
    }
}
