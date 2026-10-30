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

use std::sync::{Arc, PoisonError, RwLock};
use std::task::Poll;

use busbar_contract::abi::export::{self, CheckIn, CheckOut, DeliverIn, ExportStream, Tail};
use busbar_contract::abi::host::service::{
    DiskWritten, DISK_OPEN_FAILED, DISK_RENAME_FAILED, DISK_RETENTION_FAILED, DISK_ROTATED,
    DISK_SHIFT_FAILED,
};
use busbar_contract::abi::mechanism::call::{OutHead, Outcome};
use busbar_contract::abi::mechanism::door::{KindTailHead, Rewrite, Statement};
use busbar_contract::abi::sdk::conn::{ConnFailure, DiskFailure, Host};
use busbar_contract::abi::sdk::door::{alias, statement};
use busbar_contract::abi::sdk::export::{tail, NoScrape, NoServe, Quiet};
use busbar_contract::abi::sdk::life::{settings_as, Held, Life, Refreshed, Refusal};
use busbar_contract::abi::sdk::{Instance, Lent, Out, Safe, SafeSlot};
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

/// The streams this sink carries: the request log.
const STREAMS: &[u8] = &[ExportStream::Logs as u8];

/// The export kind's Statement tail: `logs`, and no route (a push-only sink).
const TAIL: Tail = tail(STREAMS, &[]);

/// The module name an operator writes, as the alias the registry holds beside [`NAME`].
const REWRITES: &[Rewrite] = &[alias(ALIAS)];

/// This plugin's Statement: its name, version, in-flight bound, alias and stream.
pub const STATEMENT: Statement = Statement {
    kind_tail: (&TAIL as *const Tail).cast::<KindTailHead>(),
    rewrites: REWRITES.as_ptr(),
    rewrites_len: REWRITES.len(),
    ..statement(NAME, env!("CARGO_PKG_VERSION"), MAX_INFLIGHT)
};

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
    /// The configured path (the only setting the sink itself reads). `open` and `refresh` refuse
    /// settings that do not parse, so an instance never holds unparsed ones.
    path: RwLock<Arc<str>>,
    /// The effective `keep`: the operator's, else the declared [`ROTATE_ARCHIVE_LIMIT`]. Set with
    /// `path`, from the same parsed settings.
    keep: RwLock<u32>,
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
            keep: RwLock::new(s.keep.unwrap_or(ROTATE_ARCHIVE_LIMIT)),
            path: RwLock::new(s.path.into()),
        })
    }

    /// A reload's settings replace the instance's; settings that do not parse are refused and the
    /// old ones stay.
    fn refresh(&self, settings: &[u8], _: &[&[u8]], _: u64) -> Result<Refreshed, Refusal> {
        let parsed = parse(settings)?;
        *self.keep.write().unwrap_or_else(PoisonError::into_inner) =
            parsed.keep.unwrap_or(ROTATE_ARCHIVE_LIMIT);
        *self.path.write().unwrap_or_else(PoisonError::into_inner) = parsed.path.into();
        Ok(Refreshed::default())
    }
}

impl FileSink {
    /// The configured path.
    pub fn path(&self) -> Arc<str> {
        Arc::clone(&self.path.read().unwrap_or_else(PoisonError::into_inner))
    }

    /// The effective `keep`: how many archives the host keeps beside the path.
    pub fn keep(&self) -> u32 {
        *self.keep.read().unwrap_or_else(PoisonError::into_inner)
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
        self.settle_at(&self.path(), self.keep(), answer);
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
    DiskWritten {
        size: 0,
        rotated: 0,
        faults: 0,
        _reserved: [0; 2],
        written: 0,
    }
}

/// The instance state every slot reads.
type State = Held<FileSink>;

/// The error texts of the door's REFUSED answers: one per cause, so the host's line names why
/// instead of a bare outcome.
pub const NO_INSTANCE: &str = "no opened request-log-file instance";

/// The (path, `keep`) a delivery's lines name: the pair parked when the append was issued if this is
/// a resume, else the instance's current pair, so a reload that landed in between is not followed.
fn issued_at(parked: Option<Box<(Arc<str>, u32)>>, sink: &FileSink) -> (Arc<str>, u32) {
    parked.map_or_else(|| (sink.path(), sink.keep()), |p| *p)
}

/// `deliver`: the batch appended through the host's disk lane ([`FileSink::append`]), pending
/// while the host appends; fire-and-forget, so READY whatever became of the line once the host
/// answered ([`FileSink::settle`] raises what it reported).
pub struct Deliver;

impl SafeSlot for Deliver {
    type In = DeliverIn;
    type Out = OutHead;
    type State = State;
    fn call(
        instance: Instance<'_, State>,
        input: Lent<'_, DeliverIn>,
        mut out: Out<'_, OutHead>,
    ) -> Outcome {
        let Some(h) = instance.get() else {
            return out.fail(Refusal::refused(NO_INSTANCE));
        };
        // A resumed delivery re-issues the append it parked (the host answers its stored result);
        // a fresh one appends the batch it was lent.
        // The batch is the lent `in`, which a resume re-presents from continuation-owned memory, so
        // it is never copied. What is parked is the path (and its `keep`) the append was issued
        // against, so the lines a late answer raises name that file, not a reload's new one.
        let issued_at = issued_at(instance.resume::<(Arc<str>, u32)>(), h.life());
        let batch: &[u8] = input.field(|i| &i.batch).bytes();
        match h.life().append(h.host(), instance.ticket(), batch) {
            Poll::Pending => {
                instance.park(issued_at);
                Outcome::Pending
            }
            Poll::Ready(Some(answer)) => {
                h.life().settle_at(&issued_at.0, issued_at.1, &answer);
                Outcome::Ready
            }
            Poll::Ready(None) => Outcome::Ready,
        }
    }
}

/// `check`: no check across instances; each instance's settings are `validate`'s.
pub struct Check;

impl SafeSlot for Check {
    type In = CheckIn;
    type Out = CheckOut;
    type State = State;
    fn call(_: Instance<'_, State>, _: Lent<'_, CheckIn>, _: Out<'_, CheckOut>) -> Outcome {
        Outcome::Ready
    }
}

// `scrape` and `serve` are REFUSED, saying why (a push sink carries no `metrics` stream and
// declares no route); `status` has nothing to report.
mod table {
    use super::{Check, Deliver, FileSink, NoScrape, NoServe, Quiet, Safe};

    busbar_contract::plugin_door! {
        ops: busbar_contract::abi::export::Ops,
        statement: super::STATEMENT,
        lifecycle: life(FileSink),
        kind_ops: {
            deliver: Safe<Deliver>, scrape: Safe<NoScrape<FileSink>>,
            status: Safe<Quiet<FileSink>>, check: Safe<Check>, serve: Safe<NoServe<FileSink>>,
        },
    }
}

/// This plugin's door: the one a compiled-in build links and the dropped-in image exports.
pub use table::{door, MACRO_DOOR};

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
