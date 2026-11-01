// Project:   dfe-transform-vector
// File:      src/config/staging.rs
// Purpose:   Assemble a reload beside the live Vector config and publish it whole
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Staged publication of the assembled Vector config for a reload.
//!
//! A change is assembled and validated in a sibling of the directory Vector
//! reads, so that directory only ever holds a config that passed. Publishing
//! swaps the two in one `renameat2(RENAME_EXCHANGE)`, so a Vector restarted
//! mid-reload lists the old directory or the new one, never a mix. Where the
//! filesystem cannot swap directories, such as a directory from an overlay
//! image layer, the staged files are copied over in place instead.

use std::io::Write as _;
use std::path::{Path, PathBuf};

use tracing::{debug, warn};

use super::assembler;
use super::loader::Config;
use crate::Result;

/// A change assembled beside the directory Vector reads, not yet published.
#[derive(Debug)]
pub struct Staged {
    staging: PathBuf,
    published: PathBuf,
}

impl Staged {
    /// Assemble `config` beside `published`, the directory Vector reads.
    ///
    /// # Errors
    ///
    /// As [`assembler::assemble`], or `published` has no parent to stage in.
    /// Nothing is left beside `published` on failure.
    pub fn assemble(config: &Config, published: &Path) -> Result<Self> {
        let staged = Self {
            staging: staging_dir(published)?,
            published: published.to_path_buf(),
        };
        remove_if_present(&staged.staging)?;
        match assembler::assemble_for(config, &staged.staging, published) {
            Ok(()) => Ok(staged),
            Err(e) => {
                staged.discard();
                Err(e)
            }
        }
    }

    /// The directory the change was assembled in.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.staging
    }

    /// Drop the change, leaving the directory Vector reads as it is.
    pub fn discard(self) {
        remove_logged(&self.staging);
    }

    /// Make the change the directory Vector reads.
    ///
    /// # Errors
    ///
    /// The directories could not be swapped, and neither could the staged
    /// files be copied in place. A failed copy puts the previous files back.
    pub fn publish(self) -> Result<Published> {
        if !self.published.exists() {
            return match std::fs::rename(&self.staging, &self.published) {
                Ok(()) => Ok(Published {
                    published: self.published,
                    previous: Previous::Absent,
                }),
                Err(e) => {
                    self.discard();
                    Err(e.into())
                }
            };
        }
        match exchange(&self.staging, &self.published) {
            Ok(()) => {
                debug!(dir = %self.published.display(), "swapped in the staged config");
                Ok(Published {
                    published: self.published,
                    previous: Previous::Swapped(self.staging),
                })
            }
            Err(e) if cannot_swap(&e) => {
                debug!(error = %e, "cannot swap directories here, copying the staged config in place");
                self.copy_in_place()
            }
            Err(e) => {
                self.discard();
                Err(e.into())
            }
        }
    }

    fn copy_in_place(self) -> Result<Published> {
        let captured = Snapshot::capture(&self.published)
            .and_then(|previous| Snapshot::capture(&self.staging).map(|staged| (previous, staged)));
        let (previous, staged) = match captured {
            Ok(both) => both,
            Err(e) => {
                self.discard();
                return Err(e.into());
            }
        };
        remove_logged(&self.staging);
        if let Err(e) = staged.restore(&self.published) {
            if let Err(back) = previous.restore(&self.published) {
                warn!(error = %back, dir = %self.published.display(), "could not put the previous config back");
            }
            return Err(e.into());
        }
        Ok(Published {
            published: self.published,
            previous: Previous::Copied(previous),
        })
    }
}

/// A change Vector reads now, with the config it replaced kept until the
/// outcome is known.
#[derive(Debug)]
pub struct Published {
    published: PathBuf,
    previous: Previous,
}

/// Where the replaced config is kept.
#[derive(Debug)]
enum Previous {
    /// The staging directory, which the swap left holding it.
    Swapped(PathBuf),
    /// In memory, from before the copy.
    Copied(Snapshot),
    /// Nowhere: there was no config directory before.
    Absent,
}

impl Published {
    /// Keep the change and drop the config it replaced.
    ///
    /// A reader still listing the replaced directory sees it emptied. Call it
    /// once Vector has answered the SIGHUP, or when no Vector is running.
    pub fn commit(self) {
        match &self.previous {
            Previous::Swapped(old) => remove_logged(old),
            Previous::Copied(_) | Previous::Absent => {}
        }
    }

    /// Put back the config the change replaced.
    ///
    /// # Errors
    ///
    /// The directories could not be swapped back, or the copy could not be
    /// written.
    pub fn roll_back(self) -> Result<()> {
        match self.previous {
            Previous::Swapped(old) => {
                exchange(&old, &self.published)?;
                remove_logged(&old);
                Ok(())
            }
            Previous::Copied(snapshot) => Ok(snapshot.restore(&self.published)?),
            Previous::Absent => Ok(remove_if_present(&self.published)?),
        }
    }
}

/// `<published>.next`, on the same filesystem so the two can be swapped.
fn staging_dir(published: &Path) -> Result<PathBuf> {
    let name = published.file_name().ok_or_else(|| {
        crate::Error::Config(format!(
            "vector.config_dir {} names no directory to stage a reload beside",
            published.display()
        ))
    })?;
    let mut staged = name.to_os_string();
    staged.push(".next");
    Ok(published.with_file_name(staged))
}

/// Swap the directories at `a` and `b` in one step.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn exchange(a: &Path, b: &Path) -> std::io::Result<()> {
    use nix::fcntl::{AT_FDCWD, RenameFlags, renameat2};

    renameat2(AT_FDCWD, a, AT_FDCWD, b, RenameFlags::RENAME_EXCHANGE).map_err(Into::into)
}

/// Swap the directories at `a` and `b` in one step.
#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn exchange(_: &Path, _: &Path) -> std::io::Result<()> {
    Err(std::io::ErrorKind::Unsupported.into())
}

/// Whether `e` says this filesystem cannot swap the two directories, rather
/// than that something is wrong with them.
fn cannot_swap(e: &std::io::Error) -> bool {
    use nix::errno::Errno;

    e.kind() == std::io::ErrorKind::Unsupported
        || e.raw_os_error().is_some_and(|code| {
            matches!(
                Errno::from_raw(code),
                Errno::EXDEV | Errno::EINVAL | Errno::ENOSYS | Errno::EOPNOTSUPP | Errno::EBUSY
            )
        })
}

fn remove_if_present(dir: &Path) -> std::io::Result<()> {
    match std::fs::remove_dir_all(dir) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
        Ok(()) | Err(_) => Ok(()),
    }
}

fn remove_logged(dir: &Path) {
    if let Err(e) = remove_if_present(dir) {
        warn!(error = %e, dir = %dir.display(), "could not remove a staged config");
    }
}

/// An assembled config directory read into memory, file by file.
#[derive(Debug)]
struct Snapshot {
    /// Relative path, contents (`None` for a directory) and mode, parents first.
    entries: Vec<(PathBuf, Option<Vec<u8>>, u32)>,
}

impl Snapshot {
    /// Read every file and directory under `dir`.
    fn capture(dir: &Path) -> std::io::Result<Self> {
        use std::os::unix::fs::PermissionsExt;

        let mut entries = Vec::new();
        let mut pending = vec![PathBuf::new()];
        while let Some(rel) = pending.pop() {
            for entry in std::fs::read_dir(dir.join(&rel))? {
                let entry = entry?;
                let path = rel.join(entry.file_name());
                let meta = entry.metadata()?;
                let mode = meta.permissions().mode();
                if meta.is_dir() {
                    entries.push((path.clone(), None, mode));
                    pending.push(path);
                } else {
                    entries.push((path, Some(std::fs::read(entry.path())?), mode));
                }
            }
        }
        Ok(Self { entries })
    }

    /// Replace `dir` with the captured files, modes included -- the credentials
    /// under `.secrets` are owner-only.
    fn restore(&self, dir: &Path) -> std::io::Result<()> {
        use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};

        remove_if_present(dir)?;
        std::fs::create_dir_all(dir)?;
        for (rel, contents, mode) in &self.entries {
            let path = dir.join(rel);
            match contents {
                None => std::fs::DirBuilder::new().mode(*mode).create(&path)?,
                Some(bytes) => std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(*mode)
                    .open(&path)?
                    .write_all(bytes)?,
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;
    use crate::config::loader::{SinkConfig, SourceConfig, TransformConfig};

    /// A config whose one transform, `<file>` in `dir`, sets `.<field> = true`.
    fn config_with(dir: &Path, file: &str, field: &str) -> Config {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(
            dir.join(file),
            format!(
                "transforms:\n  parse:\n    type: remap\n    inputs: [dfe_source]\n    \
                 source: '.{field} = true'\n"
            ),
        )
        .unwrap();
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
                ..Default::default()
            },
            transforms: TransformConfig {
                dir: Some(dir.to_string_lossy().into_owned()),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn with_sink_credential(mut config: Config) -> Config {
        config.sink.sasl.enabled = true;
        config.sink.sasl.username = "u".into();
        config.sink.sasl.password = "p".into();
        config
    }

    fn transform_in(dir: &Path) -> String {
        std::fs::read_to_string(dir.join("50_000_01_parse.yaml")).unwrap_or_default()
    }

    fn assemble_and_commit(config: &Config, live: &Path) {
        Staged::assemble(config, live)
            .unwrap()
            .publish()
            .unwrap()
            .commit();
    }

    /// Roll-back, commit and a failed assembly, judged on the directory Vector
    /// reads and on what is left beside it.
    #[test]
    fn a_staged_change_reaches_the_live_directory_only_when_published() {
        let work = tempfile::tempdir().unwrap();
        let live = work.path().join("config");
        let next = work.path().join("config.next");
        let old = config_with(&work.path().join("old"), "01_parse.yaml", "old");
        let new = config_with(&work.path().join("new"), "01_parse.yaml", "new");
        let mut broken = new.clone();
        let bad = work.path().join("bad");
        std::fs::create_dir_all(&bad).unwrap();
        std::fs::write(bad.join("01_parse.yaml"), "transforms: [").unwrap();
        broken.transforms.dir = Some(bad.to_string_lossy().into_owned());

        // No directory yet: the change is moved into place, and a roll-back
        // leaves none again.
        Staged::assemble(&old, &live)
            .unwrap()
            .publish()
            .unwrap()
            .roll_back()
            .unwrap();
        assert!(
            !live.exists() && !next.exists(),
            "a first roll-back left a directory"
        );
        assemble_and_commit(&old, &live);

        let staged = Staged::assemble(&new, &live).unwrap();
        assert!(
            transform_in(&live).contains(".old"),
            "staging touched the live directory"
        );
        assert!(transform_in(staged.dir()).contains(".new"));
        staged.publish().unwrap().roll_back().unwrap();
        assert!(
            transform_in(&live).contains(".old"),
            "a roll-back kept the change"
        );
        assert!(!next.exists(), "a roll-back left {} behind", next.display());

        assemble_and_commit(&new, &live);
        assert!(
            transform_in(&live).contains(".new"),
            "a commit lost the change"
        );
        assert!(!next.exists(), "a commit left {} behind", next.display());

        assert!(Staged::assemble(&broken, &live).is_err());
        assert!(
            transform_in(&live).contains(".new"),
            "a failed assembly reached the live directory"
        );
        assert!(
            !next.exists(),
            "a failed assembly left {} behind",
            next.display()
        );
    }

    /// The staged secret backend names the live directory's credentials, which
    /// is where Vector reads them once the swap is done.
    #[test]
    fn a_staged_config_reads_its_credentials_where_it_is_published() {
        let work = tempfile::tempdir().unwrap();
        let live = work.path().join("config");
        let config =
            with_sink_credential(config_with(&work.path().join("t"), "01_parse.yaml", "x"));
        assembler::assemble(&config, &live).unwrap();

        let staged = Staged::assemble(&config, &live).unwrap();
        let global = std::fs::read_to_string(staged.dir().join("00_global.yaml")).unwrap();
        let live_secrets = live.join(".secrets").to_string_lossy().into_owned();
        assert!(global.contains(&live_secrets), "{global}");
        staged.publish().unwrap().commit();
        assert_eq!(
            std::fs::read_to_string(live.join(".secrets/sink_sasl_password")).unwrap(),
            "p"
        );
    }

    /// Every listing of the live directory taken while changes are published
    /// is one whole config: every generated file, and one generation's
    /// transform.
    #[test]
    fn a_reader_never_lists_a_partial_or_mixed_config() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        const GENERATED: [&str; 4] = [
            "00_global.yaml",
            "00_source.yaml",
            "90_sink.yaml",
            "99_observability.yaml",
        ];
        let work = tempfile::tempdir().unwrap();
        let live = work.path().join("config");
        let a = config_with(&work.path().join("a"), "01_parse.yaml", "a");
        let b = config_with(&work.path().join("b"), "02_parse.yaml", "b");
        assembler::assemble(&a, &live).unwrap();

        let done = AtomicBool::new(false);
        let torn = AtomicUsize::new(0);
        let listings = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                while !done.load(Ordering::Relaxed) {
                    let names: Vec<String> = std::fs::read_dir(&live)
                        .map(|entries| {
                            entries
                                .flatten()
                                .map(|e| e.file_name().to_string_lossy().into_owned())
                                .collect()
                        })
                        .unwrap_or_default();
                    let has = |n: &str| names.iter().any(|x| x == n);
                    let whole = GENERATED.iter().all(|n| has(n))
                        && has("50_000_01_parse.yaml") != has("50_000_02_parse.yaml");
                    if !whole {
                        torn.fetch_add(1, Ordering::Relaxed);
                    }
                    listings.fetch_add(1, Ordering::Relaxed);
                }
            });
            for round in 0..200 {
                let config = if round % 2 == 0 { &b } else { &a };
                let published = Staged::assemble(config, &live).unwrap().publish().unwrap();
                // The reload loop commits once Vector answers, long after any
                // listing of the old directory has finished. Two more
                // listings here means the one that may have opened it is done.
                let seen = listings.load(Ordering::Relaxed);
                while listings.load(Ordering::Relaxed) < seen + 2 {
                    std::thread::yield_now();
                }
                published.commit();
            }
            done.store(true, Ordering::Relaxed);
        });

        assert!(listings.load(Ordering::Relaxed) > 0);
        assert_eq!(
            torn.load(Ordering::Relaxed),
            0,
            "a reader listed a partial or mixed config while changes were published"
        );
    }

    /// The fallback for a filesystem that cannot swap directories copies the
    /// staged files in, and puts the previous ones back the same way.
    #[test]
    fn a_copied_change_rolls_back_byte_for_byte_with_its_modes() {
        let work = tempfile::tempdir().unwrap();
        let live = work.path().join("config");
        let old = with_sink_credential(config_with(
            &work.path().join("old"),
            "01_parse.yaml",
            "old",
        ));
        let new = config_with(&work.path().join("new"), "01_parse.yaml", "new");
        assembler::assemble(&old, &live).unwrap();

        let published = Staged::assemble(&new, &live)
            .unwrap()
            .copy_in_place()
            .unwrap();
        assert!(transform_in(&live).contains(".new"));
        assert!(
            !live.join(".secrets").exists(),
            "the copy kept a credential the change drops"
        );
        assert!(!work.path().join("config.next").exists());

        published.roll_back().unwrap();
        assert!(transform_in(&live).contains(".old"));
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&live.join(".secrets")), 0o700);
        assert_eq!(mode(&live.join(".secrets/sink_sasl_password")), 0o600);
        assert_eq!(
            std::fs::read_to_string(live.join(".secrets/sink_sasl_password")).unwrap(),
            "p"
        );
    }

    #[test]
    fn only_errors_that_say_the_filesystem_cannot_swap_fall_back_to_a_copy() {
        use nix::errno::Errno;

        for (code, falls_back) in [
            (Errno::EXDEV, true),
            (Errno::EINVAL, true),
            (Errno::EBUSY, true),
            (Errno::EOPNOTSUPP, true),
            (Errno::ENOENT, false),
            (Errno::EACCES, false),
        ] {
            let e = std::io::Error::from_raw_os_error(code as i32);
            assert_eq!(cannot_swap(&e), falls_back, "{code}");
        }
        assert!(cannot_swap(&std::io::ErrorKind::Unsupported.into()));
    }
}
