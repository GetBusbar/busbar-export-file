// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! THE DOOR: the file sink on the export kind's table (`busbar_contract::abi::export`), its
//! lifecycle the SDK's generic one over [`FileSink`], every kind op a [`SafeSlot`]. The Statement
//! states the name, version, in-flight bound, the module name as its alias and the `logs` stream.

use std::sync::Arc;
use std::task::Poll;

use busbar_contract::abi::export::{self, CheckIn, CheckOut, DeliverIn, ExportStream, Tail};
use busbar_contract::abi::mechanism::call::{Blob, OutHead, Outcome};
use busbar_contract::abi::mechanism::door::{KindTailHead, Rewrite, Statement};
use busbar_contract::abi::sdk::door::{alias, statement};
use busbar_contract::abi::sdk::export::{tail, NoScrape, NoServe, Quiet};
use busbar_contract::abi::sdk::life::{Held, Refusal};
use busbar_contract::abi::sdk::{Instance, Lent, Out, Safe, SafeSlot};

use crate::{FileSink, ALIAS, MAX_INFLIGHT, NAME};

/// The streams this sink carries: the request log.
pub(crate) const STREAMS: &[u8] = &[ExportStream::Logs as u8];

/// The export kind's Statement tail: `logs`, and no route (a push-only sink).
pub(crate) const TAIL: Tail = tail(STREAMS, &[]);

/// The module name an operator writes, as the alias the registry holds beside [`NAME`].
pub(crate) const REWRITES: &[Rewrite] = &[alias(ALIAS)];

/// This plugin's Statement: its name, version, in-flight bound, alias and stream.
pub const STATEMENT: Statement = Statement {
    settings_schema: Blob::json(include_bytes!("../settings.schema.json")),
    kind_tail: (&TAIL as *const Tail).cast::<KindTailHead>(),
    rewrites: REWRITES.as_ptr(),
    rewrites_len: REWRITES.len(),
    ..statement(NAME, env!("CARGO_PKG_VERSION"), MAX_INFLIGHT)
};

/// The instance state every slot reads.
type State = Held<FileSink>;

/// The error texts of the door's REFUSED answers: one per cause, so the host's line names why
/// instead of a bare outcome.
pub const NO_INSTANCE: &str = "no opened request-log-file instance";
/// `check` at a phase this sink does not know.
pub const UNKNOWN_PHASE: &str = "not a check phase this sink knows (limits or instances)";

/// The (path, `keep`) a delivery's lines name: the pair parked when the append was issued if this is
/// a resume, else the instance's current pair, so a reload that landed in between is not followed.
pub(crate) fn issued_at(parked: Option<(Arc<str>, u32)>, sink: &FileSink) -> (Arc<str>, u32) {
    parked.unwrap_or_else(|| sink.current())
}

/// `deliver`: the batch appended through the host's disk lane ([`FileSink::append`]), pending
/// while the host appends; fire-and-forget, so READY whatever became of the line once the host
/// answered ([`FileSink::settle_at`] raises what it reported, against the issued-at path).
pub(crate) struct Deliver;

impl SafeSlot for Deliver {
    type Kind = busbar_contract::plugin::markers::ExportKind;
    type In = DeliverIn;
    type Out = OutHead;
    type State = State;
    fn call(
        instance: Instance<'_, State, busbar_contract::plugin::markers::ExportKind>,
        input: Lent<'_, DeliverIn>,
        mut out: Out<'_, OutHead>,
    ) -> Outcome {
        let Some(h) = instance.get() else {
            return out.fail(Refusal::refused(NO_INSTANCE));
        };

        let issued_at = issued_at(instance.resume_value::<(Arc<str>, u32)>(), h.life());
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
pub(crate) struct Check;

impl SafeSlot for Check {
    type Kind = busbar_contract::plugin::markers::ExportKind;
    type In = CheckIn;
    type Out = CheckOut;
    type State = State;
    fn call(
        instance: Instance<'_, State, busbar_contract::plugin::markers::ExportKind>,
        input: Lent<'_, CheckIn>,
        mut out: Out<'_, CheckOut>,
    ) -> Outcome {
        if instance.get().is_none() {
            return out.fail(Refusal::refused(NO_INSTANCE));
        }
        match input.phase {
            export::CHECK_PHASE_LIMITS | export::CHECK_PHASE_INSTANCES => Outcome::Ready,

            _ => out.fail(Refusal::refused(UNKNOWN_PHASE)),
        }
    }
}

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
