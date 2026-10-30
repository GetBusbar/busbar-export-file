// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The **request-log FILE sink** (`module: request-log-file`, PUSH): each request-log batch the host
//! builds for this instance is appended to a JSONL file.
//!
//! A `kind: export` plugin on the export kind's memory ABI (`busbar_contract::abi::export`): ONE door
//! ([`door`], `plugin_door!`), linked in as a compiled-in row or exported by the sibling
//! `busbar-export-file-plugin` cdylib. Its manifest carries [`DECLARES`] (`busbar-plugin-pack
//! --declares-file declares.json`): one series, the five catalogue codes and the one destination.
//!
//! **The sink never opens a path.** Its manifest declares `path` a DESTINATION: the host binds the
//! operator's configured path, and every append and rotation is a host act on the host's bounded
//! disk lane (`disk.append`). A delivery hands the batch, unchanged, to `disk.append` under the key
//! `path`, and the host appends it whole, rotating first when the file already holds `rotate_mb`
//! MiB. The manifest declares `keep: 9` for `path`, so the host rotates BY RENAME, keeping
//! `<path>.1` .. `<path>.9` and dropping only the oldest; an operator's `keep` sets N, and
//! `keep: 0` truncates in place. An append the host could not make drops the batch with the
//! open-failed ([`OPEN_FAILED`]) or append-failed ([`APPEND_FAILED`]) line, and each failed step of a
//! rotation has its own declared code.
//!
//! Settings are validated by the sink with the same serde shape ([`config::FileSettings`]) the host
//! validates the configuration with, so the words match.

#![forbid(unsafe_code)]

pub mod config;
pub mod door;

use std::sync::{Arc, PoisonError, RwLock};
use std::task::Poll;

use busbar_contract::abi::export;
use busbar_contract::abi::host::service::{
    DiskWritten, DISK_OPEN_FAILED, DISK_RENAME_FAILED, DISK_RETENTION_FAILED, DISK_ROTATED,
    DISK_SHIFT_FAILED,
};
use busbar_contract::abi::sdk::conn::{ConnFailure, DiskFailure, Host};
use busbar_contract::abi::sdk::life::{settings_as, Life, Refreshed, Refusal};
use config::FileSettings;

/// The plugin's canonical name.
pub const NAME: &str = "busbar-export-file";

/// The `module:` an operator writes.
pub const ALIAS: &str = "request-log-file";

/// What this sink DECLARES to the host (the manifest's `declares` section): its one series, its
/// five catalogue codes and its one destination. The packer embeds it (`--declares-file`).
pub const DECLARES: &str = include_str!("../declares.json");

/// How many rotated archives the host keeps (`<path>.1` .. `<path>.{ROTATE_ARCHIVE_LIMIT}`) when it
/// rotates a destination by rename and the operator sets no `keep`: the default this sink declares
/// (`declares.json` `keep.path`), named in the retention-failed line.
pub const ROTATE_ARCHIVE_LIMIT: u32 = busbar_contract::services::DISK_KEEP;

/// The most appends one instance holds in flight; past it the host sheds the line and counts the
/// declared shed counter.
pub const MAX_INFLIGHT: u32 = 64;

/// The declared destination: the settings key naming the file.
pub const DESTINATION: &str = "path";

/// The catalogue code a dropped line is raised under: writing the opened file failed.
pub const APPEND_FAILED: &str = "BUSBAR-7073";
/// The catalogue code a dropped line is raised under: the file could not be opened for the append.
pub const OPEN_FAILED: &str = "BUSBAR-7074";
/// The catalogue code a rotation raises when dropping the oldest archive failed.
pub const RETENTION_FAILED: &str = "BUSBAR-7075";
/// The catalogue code a rotation raises when shifting an archive up one slot failed.
pub const SHIFT_FAILED: &str = "BUSBAR-7076";
/// The catalogue code a rotation raises when renaming the live file to its first archive failed.
pub const ROTATE_RENAME_FAILED: &str = "BUSBAR-7077";

/// The settings, parsed as the configuration grammar parses them (empty is `{}`).
fn parse(settings: &[u8]) -> Result<FileSettings, Refusal> {
    let parsed: FileSettings = settings_as(settings)?;
    if parsed.path.is_empty() || parsed.path.contains('\0') {
        return Err(Refusal::failed("settings: `path` must name a file"));
    }
    if parsed.rotate_mb == Some(0) {
        return Err(Refusal::failed("settings: `rotate_mb` must be at least 1"));
    }
    if parsed.keep.is_some_and(|k| k > MAX_KEEP) {
        return Err(Refusal::failed(format!(
            "settings: `keep` is at most {MAX_KEEP} archives"
        )));
    }
    Ok(parsed)
}

/// The most archives the host keeps for one destination: a larger `keep` is refused here rather
/// than cut short by the host.
pub const MAX_KEEP: u32 = 64;

/// One opened instance.
#[derive(Debug)]
pub struct FileSink {
    /// The configured path (the only setting the sink itself reads) beside the effective `keep`
    /// (the operator's, else the declared [`ROTATE_ARCHIVE_LIMIT`]): ONE lock, written once per
    /// settings, so a reader never pairs one reload's path with another's keep. `open` and
    /// `refresh` refuse settings that do not parse, so an instance never holds unparsed ones.
    issued: RwLock<(Arc<str>, u32)>,
}

impl Life for FileSink {
    const CANCEL: u32 = export::cancel::ABORTED;

    /// The settings' shape, in the configuration grammar's own serde words.
    fn validate(settings: &[u8]) -> Result<(), Refusal> {
        parse(settings).map(|_| ())
    }

    /// Refuses settings that do not parse, in the same words as [`Life::validate`].
    fn open(settings: &[u8], _: &[&[u8]], _: u64) -> Result<Self, Refusal> {
        parse(settings).map(|s| Self {
            issued: RwLock::new((s.path.into(), s.keep.unwrap_or(ROTATE_ARCHIVE_LIMIT))),
        })
    }

    /// A reload's settings replace the instance's; settings that do not parse are refused and the
    /// old ones stay.
    fn refresh(&self, settings: &[u8], _: &[&[u8]], _: u64) -> Result<Refreshed, Refusal> {
        let parsed = parse(settings)?;
        *self.issued.write().unwrap_or_else(PoisonError::into_inner) = (
            parsed.path.into(),
            parsed.keep.unwrap_or(ROTATE_ARCHIVE_LIMIT),
        );
        Ok(Refreshed::default())
    }
}

impl FileSink {
    /// The configured path.
    pub fn path(&self) -> Arc<str> {
        self.current().0
    }

    /// The configured (path, `keep`), read together under one lock.
    pub fn current(&self) -> (Arc<str>, u32) {
        let g = self.issued.read().unwrap_or_else(PoisonError::into_inner);
        (Arc::clone(&g.0), g.1)
    }

    /// The effective `keep`: how many archives the host keeps beside the path.
    pub fn keep(&self) -> u32 {
        self.current().1
    }

    /// Have the host append `batch` (JSON lines, unchanged) to the destination on `host`'s disk
    /// lane, under the op running on `ticket`: what [`Connector::disk_append`] answered. An empty
    /// batch appends nothing and answers at once.
    ///
    /// [`Connector::disk_append`]: busbar_contract::abi::sdk::conn::Connector::disk_append
    pub fn append(
        &self,
        host: Option<&Host>,
        ticket: busbar_contract::abi::mechanism::ticket::Ticket,
        batch: &[u8],
    ) -> Poll<Option<Result<DiskWritten, DiskFailure>>> {
        if batch.is_empty() {
            return Poll::Ready(None);
        }
        let Some(host) = host else {
            return Poll::Ready(Some(Err(DiskFailure {
                step: 0,
                rotation: no_rotation(),
                why: ConnFailure::Unarmed,
            })));
        };
        host.connector(ticket)
            .disk_append(DESTINATION, batch)
            .map(Some)
    }

    /// Raise what the host reported for one append, in the order it happened: each failed step of
    /// the rotation that ran before it, the rotation itself, then — when the batch did not land —
    /// the line that says it was dropped.
    pub fn settle(&self, answer: &Result<DiskWritten, DiskFailure>) {
        let (path, keep) = self.current();
        self.settle_at(&path, keep, answer);
    }

    /// [`FileSink::settle`] naming `path`: the file the append was issued against, which a reload
    /// that landed while the append was pending may have replaced in the settings, and the `keep`
    /// that was in force with it.
    pub fn settle_at(&self, path: &str, keep: u32, answer: &Result<DiskWritten, DiskFailure>) {
        let rotation = match answer {
            Ok(w) => *w,
            Err(f) => f.rotation,
        };
        if rotation.faults & DISK_RETENTION_FAILED != 0 {
            tracing::warn!(
                diag = %RETENTION_FAILED,
                archive = %format!("{path}.{keep}"),
                "request-log archive retention cleanup failed; the archive series may exceed its `keep` limit"
            );
        }
        if rotation.faults & DISK_SHIFT_FAILED != 0 {
            tracing::warn!(
                diag = %SHIFT_FAILED,
                path = %path,
                "request-log archive shift failed; older archive left in place rather than lost"
            );
        }
        if rotation.faults & DISK_RENAME_FAILED != 0 {
            tracing::warn!(
                diag = %ROTATE_RENAME_FAILED,
                path = %path,
                "request-log file rotation rename failed; continuing to APPEND to the current file \
                 rather than truncate it, so no recorded data is lost — the file will exceed rotate_mb \
                 until this is resolved"
            );
        }
        if rotation.rotated == DISK_ROTATED {
            tracing::info!(path = %path, archive = %format!("{path}.1"), "request-log file rotated by rename");
        }
        match answer {
            Ok(_) => {}
            // The host could not open the file for the append: the file it could not open, a
            // destination it never opened (one this sink was not granted), or its disk lane full.
            Err(f) if f.step == DISK_OPEN_FAILED => {
                tracing::warn!(diag = %OPEN_FAILED, path = %path, error = %f.why, "request-log file open failed; this log was dropped");
            }
            // Every other append that did not land is the append-failed line, in the host's words:
            // the write itself failed, or the append was never run (no ticket, no disk service, a
            // fault), which is no file-open problem, so not BUSBAR-7074 and its disk remedy. A
            // plugin speaks only over the ABI: every dropped batch carries a declared code.
            Err(f) => {
                tracing::warn!(diag = %APPEND_FAILED, path = %path, error = %f.why, "request-log file append failed; this log was dropped");
            }
        }
    }
}

/// A report of no rotation.
const fn no_rotation() -> DiskWritten {
    DiskWritten::ZERO
}

pub use door::{door, Check, Deliver, MACRO_DOOR, NO_INSTANCE, STATEMENT, UNKNOWN_PHASE};

#[cfg(test)]
#[path = "tests/lib_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/door_tests.rs"]
mod door_tests;
