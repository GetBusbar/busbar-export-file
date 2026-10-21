// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The **request-log FILE sink** (`module: request-log-file`, PUSH): each request-log batch the host
//! builds for this instance is appended to a JSONL file.
//!
//! A `kind: export` plugin on the export kind's memory ABI (`busbar_contract::abi::export`, THE
//! DESIGN §11): ONE door ([`door`], `plugin_door!`), linked into the binary as a compiled-in row or
//! exported by the sibling `busbar-export-file-plugin` cdylib as its one symbol. Its manifest still
//! carries `DECLARES` (`busbar-plugin-pack --declares-file declares.json`): its one series (1.5.5's
//! shed counter), the two catalogue codes and the one destination with its retention.
//!
//! **The sink never opens a path.** Its manifest declares `path` a DESTINATION that keeps no
//! archives (`keep` 0): the host binds the operator's configured path, and every append and every
//! rollover is a host act on the host's bounded disk lane (THE DESIGN §11.11 R4, the `disk.append`
//! service of §11.12). A delivery hands the batch's lines to `disk.append` under the key `path` ONE
//! LINE AT A TIME, in order and unchanged, as 1.5.5's sink appended each line on its own
//! (v1.5.5 `crates/busbar/src/export/file.rs`); before each append the host truncates the file IN
//! PLACE when it already holds `rotate_mb` MiB — 1.5.5's "roll over by truncation rather than a
//! rename": no archive, no other file. What the host reports back is raised in the words the sink
//! always raised: a line the host could not append is dropped, alone, with the open-failed
//! (`OPEN_FAILED`) or append-failed (`APPEND_FAILED`) line, and the lines after it still go.
//!
//! Settings are validated by the sink itself while the host validates the configuration, with the
//! same serde shape (`config::FileSettings`) and so the same words.

#![forbid(unsafe_code)]

mod config;

use std::sync::{PoisonError, RwLock};
use std::task::Poll;

use busbar_contract::abi::export::{
    self, CheckIn, CheckOut, DeliverIn, ExportStream, ScrapeIn, ScrapeOut, ServeIn, ServeOut,
    StatusOut, Tail,
};
use busbar_contract::abi::host::service::{DiskWritten, DISK_OPEN_FAILED};
use busbar_contract::abi::mechanism::call::{AbiStr, InHead, OutHead, Outcome};
use busbar_contract::abi::mechanism::door::{Rewrite, Statement, REWRITE_ALIAS};
use busbar_contract::abi::sdk::conn::{ConnFailure, DiskFailure};
use busbar_contract::abi::sdk::door::{abi_str, statement};
use busbar_contract::abi::sdk::life::{Held, Life, Refreshed, Refusal};
use busbar_contract::abi::sdk::{Instance, Lent, Out, Safe, SafeSlot};
use config::FileSettings;

/// The plugin's canonical name.
const NAME: &str = "busbar-export-file";

/// The `module:` an operator writes — the frozen 1.5.x module name.
const ALIAS: &str = "request-log-file";

busbar_contract::statement_extensions!(
    /// The Statement's extensions: this plugin's whole `declares` document (BUSBAR-1.6.0.md l.246): its
    /// one series, its two catalogue codes and its one destination with its retention. The packer
    /// embeds the same file (`--declares-file`).
    EXTENSIONS: [(
        busbar_contract::abi::mechanism::extensions::EXT_DECLARES,
        include_bytes!("../declares.json")
    )]
);

/// The most appends one instance holds in flight (1.5.5's `MAX_INFLIGHT_FILE_APPENDS`); past it the
/// host sheds the line and counts the declared shed counter.
const MAX_INFLIGHT: u32 = 64;

/// The declared destination: the settings key naming the file.
const DESTINATION: &str = "path";

/// The catalogue code a dropped line is raised under: writing the opened file failed.
const APPEND_FAILED: &str = "BUSBAR-7073";
/// The catalogue code a dropped line is raised under: the file could not be opened for the append.
const OPEN_FAILED: &str = "BUSBAR-7074";

/// The streams this sink carries: the request log.
const STREAMS: &[u8] = &[ExportStream::Logs as u8];

/// The export kind's Statement tail: `logs`, and no route (a push-only sink).
const TAIL: Tail = Tail::EMPTY.with_streams(STREAMS);

/// The module name an operator writes, as the alias the registry holds beside [`NAME`].
const REWRITES: &[Rewrite] = &[Rewrite {
    class: REWRITE_ALIAS,
    _reserved: 0,
    from: abi_str(ALIAS),
    to: AbiStr::ABSENT,
}];

/// This plugin's Statement: its name, version, in-flight bound, alias and stream.
const STATEMENT: Statement = {
    let mut s = statement(NAME, env!("CARGO_PKG_VERSION"), MAX_INFLIGHT)
        .with_rewrites(REWRITES)
        .with_kind_tail(&TAIL);
    s.extensions = EXTENSIONS;
    s
};

/// The settings, parsed as the configuration grammar parses them (empty is `{}`).
fn parse(settings: &[u8]) -> Result<FileSettings, Refusal> {
    let bytes = if settings.is_empty() { b"{}" } else { settings };
    serde_json::from_slice::<serde_json::Value>(bytes)
        .and_then(serde_json::from_value)
        .map_err(|e| Refusal::failed(format!("settings: {e}")))
}

/// One opened instance.
#[derive(Debug)]
struct FileSink {
    /// The instance's settings; `None` when they do not parse (validation already refused them, so
    /// an instance that got this far is never configured that way — it simply writes nothing).
    settings: RwLock<Option<FileSettings>>,
}

impl Life for FileSink {
    const CANCEL: u32 = export::cancel::ABORTED;

    /// The settings' shape, in the configuration grammar's own serde words.
    fn validate(settings: &[u8]) -> Result<(), Refusal> {
        parse(settings).map(|_| ())
    }

    /// Never refuses: the settings were validated with the configuration, and an instance whose
    /// settings do not parse writes nothing.
    fn open(settings: &[u8], _: &[&[u8]], _: u64) -> Result<Self, Refusal> {
        Ok(Self {
            settings: RwLock::new(parse(settings).ok()),
        })
    }

    /// A reload's settings replace the instance's.
    fn refresh(&self, settings: &[u8], _: &[&[u8]], _: u64) -> Result<Refreshed, Refusal> {
        *self
            .settings
            .write()
            .unwrap_or_else(PoisonError::into_inner) = parse(settings).ok();
        Ok(Refreshed::default())
    }
}

impl FileSink {
    /// The configured path; `None` when the settings did not parse (the instance writes nothing).
    pub fn path(&self) -> Option<String> {
        self.settings
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|s| s.path.clone())
    }

    /// Hand the lines of `batch` to `append` ONE AT A TIME, in order and unchanged, from the line
    /// `at` names on, raising what the host answered for each as it answers (1.5.5 appended, and
    /// dropped, one line at a time). PENDING while the host appends a line: `at` then names that
    /// line, so a resumed delivery re-issues it and goes on from there, never raising a line twice.
    /// An instance whose settings did not parse appends nothing.
    fn deliver(
        &self,
        batch: &[u8],
        at: &mut Progress,
        mut append: impl FnMut(&[u8]) -> Poll<Result<DiskWritten, DiskFailure>>,
    ) -> Poll<()> {
        if self.path().is_none() {
            return Poll::Ready(());
        }
        for line in lines(batch).skip(at.line) {
            match append(&line) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(answer) => self.settle_at(&at.path, &answer),
            }
            at.line += 1;
        }
        Poll::Ready(())
    }

    /// Raise what the host reported for one line's append: nothing when it landed, else the line
    /// that says it was dropped.
    #[cfg(test)]
    pub fn settle(&self, answer: &Result<DiskWritten, DiskFailure>) {
        self.settle_at(&self.path().unwrap_or_default(), answer);
    }

    /// `FileSink::settle` naming `path`: the file the append was issued against, which a reload
    /// that landed while the append was pending may have replaced in the settings.
    pub fn settle_at(&self, path: &str, answer: &Result<DiskWritten, DiskFailure>) {
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
            // plugin speaks only over the ABI (Part 2 #2): every dropped line carries a declared code.
            Err(f) => {
                tracing::warn!(diag = %APPEND_FAILED, path = %path, error = %f.why, "request-log file append failed; this log was dropped");
            }
        }
    }
}

/// The JSON lines of `batch`, each ending in `\n` as 1.5.5's `writeln!` wrote it, the final line
/// included when the batch does not end in one.
fn lines(batch: &[u8]) -> impl Iterator<Item = std::borrow::Cow<'_, [u8]>> {
    batch
        .split_inclusive(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| {
            if line.ends_with(b"\n") {
                std::borrow::Cow::Borrowed(line)
            } else {
                let mut owned = line.to_vec();
                owned.push(b'\n');
                std::borrow::Cow::Owned(owned)
            }
        })
}

/// Where a delivery stands, parked while the host appends one of its lines: the path the delivery
/// was issued against (the lines it raises name that file, not a reload's new one), the line whose
/// append is pending, and how many host services the delivery had issued before it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Progress {
    path: String,
    line: usize,
    issued: u32,
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

/// `deliver`: the batch's lines appended through the host's disk lane, one at a time
/// ([`FileSink::deliver`]), pending while the host appends one; fire-and-forget, so READY whatever
/// became of each line once the host answered (`FileSink::settle_at` raises what it reported).
struct Deliver;

impl SafeSlot for Deliver {
    type In = DeliverIn;
    type Out = OutHead;
    type State = State;
    fn call(
        instance: Instance<'_, State>,
        input: Lent<'_, DeliverIn>,
        _: Out<'_, OutHead>,
    ) -> Outcome {
        let Some(h) = instance.get() else {
            return Outcome::Refused;
        };
        // A resumed delivery goes on from the line it parked on, re-issuing that line's append
        // under the same handle (the host answers its stored result); a fresh one starts at its
        // first line. The batch is the lent `in`, which a resume re-presents from
        // continuation-owned memory (spec A.5), so it is never copied.
        let mut at: Progress = instance.resume::<Progress>().map_or_else(
            || Progress {
                path: h.life().path().unwrap_or_default(),
                line: 0,
                issued: 0,
            },
            |p| *p,
        );
        let batch: &[u8] = input.batch().bytes();
        let ticket = instance.ticket();
        let host = h.host();
        let mut conn = host.map(|host| host.connector_from(ticket, at.issued));
        let mut before = at.issued;
        let answered = h
            .life()
            .deliver(batch, &mut at, |line| match conn.as_mut() {
                Some(conn) => {
                    before = conn.issued();
                    conn.disk_append(DESTINATION, line)
                }
                None => Poll::Ready(Err(DiskFailure {
                    step: 0,
                    rotation: no_rotation(),
                    why: ConnFailure::Unarmed,
                })),
            });
        match answered {
            Poll::Pending => {
                at.issued = before;
                instance.park(at);
                Outcome::Pending
            }
            Poll::Ready(()) => Outcome::Ready,
        }
    }
}

/// `scrape`: a push sink renders no exposition.
struct Scrape;

impl SafeSlot for Scrape {
    type In = ScrapeIn;
    type Out = ScrapeOut;
    type State = State;
    fn call(_: Instance<'_, State>, _: Lent<'_, ScrapeIn>, _: Out<'_, ScrapeOut>) -> Outcome {
        Outcome::Ready
    }
}

/// `status`: nothing to report.
struct Status;

impl SafeSlot for Status {
    type In = InHead;
    type Out = StatusOut;
    type State = State;
    fn call(_: Instance<'_, State>, _: Lent<'_, InHead>, _: Out<'_, StatusOut>) -> Outcome {
        Outcome::Ready
    }
}

/// `check`: no check across instances; each instance's settings are `validate`'s.
struct Check;

impl SafeSlot for Check {
    type In = CheckIn;
    type Out = CheckOut;
    type State = State;
    fn call(_: Instance<'_, State>, _: Lent<'_, CheckIn>, _: Out<'_, CheckOut>) -> Outcome {
        Outcome::Ready
    }
}

/// `serve`: no route, so any request is `404`.
struct Serve;

impl SafeSlot for Serve {
    type In = ServeIn;
    type Out = ServeOut;
    type State = State;
    fn call(_: Instance<'_, State>, _: Lent<'_, ServeIn>, mut out: Out<'_, ServeOut>) -> Outcome {
        out.set(|o| &o.status_code, 404_u16);
        Outcome::Ready
    }
}

mod table {
    use super::{Check, Deliver, FileSink, Safe, Scrape, Serve, Status};

    busbar_contract::plugin_door! {
        ops: busbar_contract::abi::export::Ops,
        statement: super::STATEMENT,
        lifecycle: life(FileSink),
        kind_ops: {
            deliver: Safe<Deliver>, scrape: Safe<Scrape>, status: Safe<Status>,
            check: Safe<Check>, serve: Safe<Serve>,
        },
    }
}

/// THE DOOR: this plugin's one public item (BUSBAR-1.6.0.md l.3567, Crate shape); the one a compiled-in
/// build links and the dropped-in image exports.
pub use table::door;

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
