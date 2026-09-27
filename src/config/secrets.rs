// Project:   dfe-transform-vector
// File:      src/config/secrets.rs
// Purpose:   Hand SASL credentials to Vector through its directory secret backend
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! SASL credentials for Vector's Kafka components.
//!
//! No credential is written into the assembled Vector config. Each component
//! names its username and password as `SECRET[<backend>.<key>]`, and Vector
//! reads them through a `directory` secret backend, one file per key:
//!
//! - `sasl.secret_dir` set: the backend is that directory -- a mounted
//!   Kubernetes Secret, say -- holding `username` and `password` files.
//! - otherwise: the assembler writes `username`/`password` from the config into
//!   owner-only files under `<config_dir>/.secrets`, which Vector's
//!   `--config-dir` does not load as config.
//!
//! Vector 0.57+ expands no `${VAR}` placeholder, so this is the only way a
//! credential reaches it without landing in the config text.

use std::io::Write;
use std::path::{Path, PathBuf};

use super::loader::{Config, SaslConfig};
use crate::Result;

/// Directory under the Vector config dir that holds credentials the config
/// carried as text.
pub const PRIVATE_DIR: &str = ".secrets";

/// Backend for credentials written by the assembler.
const PRIVATE_BACKEND: &str = "dfe_credentials";

/// Which Kafka component a credential belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    /// The generated Kafka source.
    Source,
    /// The generated Kafka sink.
    Sink,
}

impl Side {
    /// The config section name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Sink => "sink",
        }
    }
}

/// The secret backend and keys one component's credentials are read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SecretNames {
    /// Vector secret backend name.
    pub backend: &'static str,
    /// Key, and file name, of the username.
    pub username: &'static str,
    /// Key, and file name, of the password.
    pub password: &'static str,
}

impl SecretNames {
    /// The names `side`'s SASL credentials are read under.
    #[must_use]
    pub fn of(side: Side, sasl: &SaslConfig) -> Self {
        match (side, sasl.secret_dir.is_some()) {
            (Side::Source, true) => Self {
                backend: "dfe_source_sasl",
                username: "username",
                password: "password",
            },
            (Side::Sink, true) => Self {
                backend: "dfe_sink_sasl",
                username: "username",
                password: "password",
            },
            (Side::Source, false) => Self {
                backend: PRIVATE_BACKEND,
                username: "source_sasl_username",
                password: "source_sasl_password",
            },
            (Side::Sink, false) => Self {
                backend: PRIVATE_BACKEND,
                username: "sink_sasl_username",
                password: "sink_sasl_password",
            },
        }
    }

    /// `SECRET[<backend>.<username key>]`.
    #[must_use]
    pub fn username_ref(&self) -> String {
        format!("SECRET[{}.{}]", self.backend, self.username)
    }

    /// `SECRET[<backend>.<password key>]`.
    #[must_use]
    pub fn password_ref(&self) -> String {
        format!("SECRET[{}.{}]", self.backend, self.password)
    }
}

/// One `directory` secret backend in the assembled global config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretBackend {
    /// Backend name.
    pub name: &'static str,
    /// Directory it reads, one file per key.
    pub dir: PathBuf,
}

/// The Kafka components that authenticate: those on the bus with SASL on.
fn authenticating(config: &Config) -> impl Iterator<Item = (Side, &SaslConfig)> {
    [
        (
            Side::Source,
            &config.source.sasl,
            config.source.transport.is_direct(),
        ),
        (
            Side::Sink,
            &config.sink.sasl,
            config.sink.transport.is_direct(),
        ),
    ]
    .into_iter()
    .filter(|(_, sasl, direct)| sasl.enabled && !direct)
    .map(|(side, sasl, _)| (side, sasl))
}

/// The backends the assembled config needs, each once.
#[must_use]
pub fn backends(config: &Config, config_dir: &Path) -> Vec<SecretBackend> {
    let mut out: Vec<SecretBackend> = Vec::new();
    for (side, sasl) in authenticating(config) {
        let names = SecretNames::of(side, sasl);
        let dir = sasl
            .secret_dir
            .as_ref()
            .map_or_else(|| config_dir.join(PRIVATE_DIR), PathBuf::from);
        if !out.iter().any(|b| b.name == names.backend) {
            out.push(SecretBackend {
                name: names.backend,
                dir,
            });
        }
    }
    out
}

/// Put every credential where its backend reads it.
///
/// Credentials given as text go to owner-only files under
/// `<config_dir>/.secrets`. A `secret_dir` is only checked: both files must be
/// readable, since Vector fails on a missing one only once it runs, where
/// `vector validate` passes it.
///
/// # Errors
///
/// A file that cannot be written, or a `secret_dir` file that cannot be read.
pub fn materialise(config: &Config, config_dir: &Path) -> Result<()> {
    for (side, sasl) in authenticating(config) {
        let names = SecretNames::of(side, sasl);
        match &sasl.secret_dir {
            Some(dir) => {
                for key in [names.username, names.password] {
                    let path = Path::new(dir).join(key);
                    std::fs::File::open(&path).map_err(|e| {
                        crate::Error::Config(format!(
                            "{side}.sasl.secret_dir: cannot read {path}: {e}",
                            side = side.as_str(),
                            path = path.display()
                        ))
                    })?;
                }
            }
            None => {
                let dir = config_dir.join(PRIVATE_DIR);
                create_private_dir(&dir)?;
                write_private(&dir.join(names.username), &sasl.username)?;
                write_private(&dir.join(names.password), &sasl.password)?;
            }
        }
    }
    Ok(())
}

/// Create `dir` readable by this user alone.
fn create_private_dir(dir: &Path) -> Result<()> {
    use std::os::unix::fs::DirBuilderExt;

    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)?;
    Ok(())
}

/// Write `value` to `path`, readable by this user alone.
fn write_private(path: &Path, value: &str) -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt;

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(value.as_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;
    use crate::config::Transport;

    fn sasl(username: &str, password: &str, secret_dir: Option<&str>) -> SaslConfig {
        SaslConfig {
            enabled: true,
            username: username.into(),
            password: password.into(),
            secret_dir: secret_dir.map(Into::into),
            ..SaslConfig::default()
        }
    }

    #[test]
    fn credentials_given_as_text_land_in_owner_only_files() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.source.sasl = sasl("src-user", "src-pass", None);
        config.sink.sasl = sasl("sink-user", "sink-pass", None);

        materialise(&config, dir.path()).unwrap();

        let private = dir.path().join(PRIVATE_DIR);
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&private), 0o700);
        for (file, want) in [
            ("source_sasl_username", "src-user"),
            ("source_sasl_password", "src-pass"),
            ("sink_sasl_username", "sink-user"),
            ("sink_sasl_password", "sink-pass"),
        ] {
            let path = private.join(file);
            assert_eq!(std::fs::read_to_string(&path).unwrap(), want);
            assert_eq!(mode(&path), 0o600, "{file} is readable beyond its owner");
        }

        let backends = backends(&config, dir.path());
        assert_eq!(
            backends,
            vec![SecretBackend {
                name: PRIVATE_BACKEND,
                dir: private,
            }],
            "both sides share the one written backend"
        );
    }

    #[test]
    fn a_secret_dir_is_read_where_it_is_mounted() {
        let mounted = tempfile::tempdir().unwrap();
        std::fs::write(mounted.path().join("username"), "u\n").unwrap();
        std::fs::write(mounted.path().join("password"), "p\n").unwrap();
        let config_dir = tempfile::tempdir().unwrap();

        let mut config = Config::default();
        config.source.sasl = sasl("", "", mounted.path().to_str());

        materialise(&config, config_dir.path()).unwrap();
        assert!(
            !config_dir.path().join(PRIVATE_DIR).exists(),
            "nothing is copied out of a mounted secret"
        );
        assert_eq!(
            backends(&config, config_dir.path()),
            vec![SecretBackend {
                name: "dfe_source_sasl",
                dir: mounted.path().to_path_buf(),
            }]
        );
        let names = SecretNames::of(Side::Source, &config.source.sasl);
        assert_eq!(names.password_ref(), "SECRET[dfe_source_sasl.password]");
    }

    #[test]
    fn a_secret_dir_missing_a_file_fails_assembly_rather_than_the_vector_start() {
        let mounted = tempfile::tempdir().unwrap();
        std::fs::write(mounted.path().join("username"), "u").unwrap();
        let config_dir = tempfile::tempdir().unwrap();

        let mut config = Config::default();
        config.sink.sasl = sasl("", "", mounted.path().to_str());

        let err = materialise(&config, config_dir.path()).unwrap_err();
        assert!(
            err.to_string().contains("sink.sasl.secret_dir")
                && err.to_string().contains("password"),
            "the error must name the setting and the file: {err}"
        );
    }

    #[test]
    fn a_direct_end_or_sasl_off_needs_no_backend() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.source.transport = Transport::Direct;
        config.source.sasl = sasl("u", "p", None);
        assert!(backends(&config, dir.path()).is_empty());

        materialise(&config, dir.path()).unwrap();
        assert!(!dir.path().join(PRIVATE_DIR).exists());
    }
}
