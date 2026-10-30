// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The file sink's door: what its Statement states and what each kind op answers through the door
//! over a fake host (deliver's append, the refusals, `check`). The door over the real loader,
//! linked and dropped in, is the plugin crate's `tests/conformance.rs`.

use super::*;
use crate::door::{REWRITES, STREAMS, TAIL};
use crate::tests::{report, TICKET};
use busbar_contract::abi::export::{DeliverIn, ExportStream, Tail};
use busbar_contract::abi::mechanism::call::InHead;
use busbar_contract::abi::mechanism::call::{OutHead, Outcome};
use busbar_contract::abi::mechanism::door::REWRITE_ALIAS;

/// The Statement: the plugin's name and version, 1.5.5's in-flight bound, the module name as its
/// alias, the `logs` stream and no route (a push-only sink).
#[test]
fn the_statement_states_the_sink() {
    assert_eq!(DoorView::of(MACRO_DOOR).name(), NAME);
    assert_eq!(NAME, "busbar-export-file");
    assert_eq!(STATEMENT.version.len, env!("CARGO_PKG_VERSION").len());
    // The kit has no safe reader for the version's bytes (`DoorView::name` reads the name only), and
    // this crate forbids unsafe: the version is pinned by presence and length.
    assert!(!STATEMENT.version.is_absent());
    assert_eq!(STATEMENT.max_inflight, 64);
    assert_eq!(STATEMENT.rewrites_len, 1);
    assert_eq!(REWRITES[0].class, REWRITE_ALIAS);
    assert_eq!(REWRITES[0].from.len, ALIAS.len());
    assert_eq!(STREAMS, &[ExportStream::Logs as u8]);
    assert_eq!(TAIL.streams_len, 1);
    assert_eq!(TAIL.routes_len, 0);
    assert!(TAIL.routes.is_null());
    assert_eq!(TAIL.head.size as usize, size_of::<Tail>());
}

// ── THE DELIVER OP, THROUGH THE DOOR (C4) ───────────────────────────────────────────────────────

use busbar_contract::abi::host::service::{DiskAppendIn, DISK_APPEND_FAILED};
use busbar_contract::abi::mechanism::call::{BLOB_JSON, FLAG_RESUME};
use busbar_contract::abi::mechanism::lifecycle::{slot, OpenIn, OpenOut, RefreshIn};
use busbar_contract::abi::sdk::test_support::fake::{self, FakeHost, FakeService, ServiceCall};
use busbar_contract::abi::sdk::test_support::reply;
use busbar_contract::abi::sdk::test_support::test_in::TestIn;
use busbar_contract::abi::sdk::test_support::{zeroed, DoorView};
use std::cell::RefCell;

thread_local! {
    /// Every `disk.append` the fake host received on this thread: (destination key, bytes).
    static APPENDS: RefCell<Vec<(String, Vec<u8>)>> = const { RefCell::new(Vec::new()) };
    /// What the fake host answers the NEXT append with.
    static NEXT: RefCell<Next> = const { RefCell::new(Next::Land) };
}

#[derive(Clone, Copy)]
enum Next {
    Land,
    Pend,
    Fail,
}

/// The host's `disk.append`, recording what it was asked and answering as `NEXT` says.
struct Disk;

impl FakeService for Disk {
    type In = DiskAppendIn;
    fn serve(call: &mut ServiceCall<DiskAppendIn>) -> Outcome {
        let key = call.text(|i| &i.dest_key).to_string();
        let bytes = call.blob(|i| &i.bytes).to_vec();
        let len = bytes.len() as u64;
        APPENDS.with(|a| a.borrow_mut().push((key, bytes)));
        match NEXT.with(|n| *n.borrow()) {
            Next::Pend => Outcome::Pending,
            Next::Land => {
                call.put(|i| &i.result, report(0, 0, len));
                Outcome::Ready
            }
            Next::Fail => {
                call.put(|i| &i.result, report(0, 0, 0));
                call.set_value(DISK_APPEND_FAILED)
                    .set_error("No space left on device (os error 28)");
                Outcome::Failed
            }
        }
    }
}

fn appends() -> Vec<(String, Vec<u8>)> {
    APPENDS.with(|a| a.borrow().clone())
}

fn next(n: Next) {
    NEXT.with(|c| *c.borrow_mut() = n);
}

const BATCH: &[u8] = b"{\"a\":1}\n{\"b\":2}\n";

/// Drive the deliver op through the door over a fake host serving `disk.append`: the host receives
/// the batch unchanged under the key `path` (C4 i); and an append that pends, then a reload to a new
/// path, then a resumed delivery the host fails, runs to READY with the append re-issued (C4 ii, as
/// far as the harness can observe it: see below).
/// RED before: `Deliver` passed another destination key, or altered the batch.
#[test]
fn the_deliver_op_appends_the_batch_under_the_path_key_through_the_door() {
    APPENDS.with(|a| a.borrow_mut().clear());
    let host = FakeHost::new().serve(fake::host::DISK_APPEND, Disk);
    let view = DoorView::of(MACRO_DOOR);
    let ops = |pick: fn(
        &'static export::Ops,
    ) -> &'static Option<busbar_contract::abi::mechanism::call::Op>| {
        view.op::<export::Ops>(pick).expect("the slot is stated")
    };
    let open = ops(|t| &t.head.open);
    let refresh = ops(|t| &t.head.refresh);
    let deliver = ops(|t| &t.deliver);
    let close = ops(|t| &t.head.close);

    let mut opened = {
        let lent = host.lend_in(
            TestIn::<OpenIn>::new()
                .set(|i| &i.head.op, slot::OPEN)
                .settings(br#"{"path":"/old.jsonl"}"#),
        );
        reply::call_hosted_in(open, None, &lent, zeroed::<OpenOut>())
    };
    assert_eq!(opened.outcome, Outcome::Ready);
    let instance = opened.take_instance().expect("open answered an instance");

    let deliver_in = |flags: u32| {
        TestIn::<DeliverIn>::new()
            .set(|i| &i.head.op, export::slot::DELIVER)
            .set(|i| &i.head.flags, flags)
            .set(|i| &i.head.ticket, TICKET)
            .blob(|i| &i.batch, BATCH, BLOB_JSON)
    };
    let out = || {
        let mut h: OutHead = zeroed();
        h.size = size_of::<OutHead>() as u32;
        h
    };

    // (i) a landed append: the key is `path`, the bytes are the batch, unchanged.
    next(Next::Land);
    let c = reply::call_in(deliver, Some(&instance), &deliver_in(0), out());
    assert_eq!(c.outcome, Outcome::Ready);
    assert_eq!(appends(), [("path".to_string(), BATCH.to_vec())]);

    // (ii) an append that pends, a reload to a new path, the resumed delivery failing.
    next(Next::Pend);
    let c = reply::call_in(deliver, Some(&instance), &deliver_in(0), out());
    assert_eq!(c.outcome, Outcome::Pending);
    let c = reply::call_in(
        refresh,
        Some(&instance),
        &TestIn::<RefreshIn>::new()
            .set(|i| &i.head.op, slot::REFRESH)
            .settings(br#"{"path":"/new.jsonl"}"#),
        out(),
    );
    assert_eq!(c.outcome, Outcome::Ready);
    next(Next::Fail);
    // The resumed delivery re-issues the parked append and the host fails it: the op is still
    // fire-and-forget (READY). The line it raises is folded into the reply's envelope under the
    // door's call capture, which `reply::call_in` clears before returning (`reply.rs`
    // `clear_head`, l.403-413): the harness exposes no way to read it, so the path named by that
    // line is pinned by `a_settled_append_names_the_path_it_was_issued_against` alone.
    let c = reply::call_in(deliver, Some(&instance), &deliver_in(FLAG_RESUME), out());
    assert_eq!(c.outcome, Outcome::Ready);
    assert!(appends()
        .iter()
        .all(|(k, b)| k == DESTINATION && b == BATCH));
    assert_eq!(appends().len(), 3);

    let closed = reply::close_in(
        close,
        instance,
        &TestIn::<InHead>::new().set(|i| &i.op, slot::CLOSE),
        out(),
    );
    assert_eq!(closed.outcome, Outcome::Ready);
    drop(host);
}

// ── EVERY REFUSAL NAMES ITS CAUSE (sibling review, export kind) ─────────────────────────────────

/// RED before: `serve` answered READY `404` for a sink that declares no route, a silent "nothing".
/// It is now REFUSED, saying why, in the words every export sibling uses (`scrape`: the plugin
/// crate's `tests/refusals.rs`).
#[test]
fn every_refusal_names_its_cause_through_the_door() {
    use busbar_contract::abi::export::{Ops, ServeIn, ServeOut};
    let view = DoorView::of(MACRO_DOOR);
    let open = view.op::<Ops>(|t| &t.head.open).expect("open");
    let close = view.op::<Ops>(|t| &t.head.close).expect("close");
    let serve = view.op::<Ops>(|t| &t.serve).expect("serve");
    let out = || {
        let mut h: OutHead = zeroed();
        h.size = size_of::<OutHead>() as u32;
        h
    };

    // `deliver` with no instance answers NO_INSTANCE; neither the host nor the test kit enters a
    // kind op on no instance (`reply::call_in` asserts it), so that arm is not driven.
    let mut opened = reply::call_in(
        open,
        None,
        &TestIn::<OpenIn>::new()
            .set(|i| &i.head.op, slot::OPEN)
            .settings(br#"{"path":"/x.jsonl"}"#),
        zeroed::<OpenOut>(),
    );
    assert_eq!(opened.outcome, Outcome::Ready);
    let instance = opened.take_instance().expect("an instance");

    // `scrape` is driven over the loader (`tests/refusals.rs`): the test kit copies no `ScrapeOut`.

    let c = reply::call_in(
        serve,
        Some(&instance),
        &TestIn::<ServeIn>::new().set(|i| &i.head.op, export::slot::SERVE),
        zeroed::<ServeOut>(),
    );
    assert_eq!(c.outcome, Outcome::Refused);
    assert_eq!(
        c.error.as_deref(),
        Some(busbar_contract::abi::sdk::export::NO_ROUTE)
    );

    let closed = reply::close_in(
        close,
        instance,
        &TestIn::<InHead>::new().set(|i| &i.op, slot::CLOSE),
        out(),
    );
    assert_eq!(closed.outcome, Outcome::Ready);
}

/// `check` is the sink's own: at the limits and instances phases it has nothing to say (READY, no
/// findings), and a phase it does not know is REFUSED saying why, never run as another (the words
/// every export sibling uses). A `check` on no instance answers `NO_INSTANCE`; the test kit never
/// enters a kind op on no instance, so that arm is not driven here.
/// RED before: an unknown phase answered READY.
#[test]
fn check_refuses_a_phase_it_does_not_know_through_the_door() {
    use busbar_contract::abi::export::{
        CheckIn, CheckOut, Ops, CHECK_PHASE_INSTANCES, CHECK_PHASE_LIMITS,
    };
    let view = DoorView::of(MACRO_DOOR);
    let open = view.op::<Ops>(|t| &t.head.open).expect("open");
    let close = view.op::<Ops>(|t| &t.head.close).expect("close");
    let check_op = view.op::<Ops>(|t| &t.check).expect("check");
    let mut opened = reply::call_in(
        open,
        None,
        &TestIn::<OpenIn>::new()
            .set(|i| &i.head.op, slot::OPEN)
            .settings(br#"{"path":"/x.jsonl"}"#),
        zeroed::<OpenOut>(),
    );
    assert_eq!(opened.outcome, Outcome::Ready);
    let instance = opened.take_instance().expect("an instance");
    let check = |phase: u32| {
        reply::call_in(
            check_op,
            Some(&instance),
            &TestIn::<CheckIn>::new()
                .set(|i| &i.head.op, check_op.index())
                .set(|i| &i.phase, phase),
            zeroed::<CheckOut>(),
        )
    };
    for phase in [CHECK_PHASE_LIMITS, CHECK_PHASE_INSTANCES] {
        let known = check(phase);
        assert_eq!(known.outcome, Outcome::Ready);
        assert!(known.owned.is_empty(), "{:?}", known.owned);
    }
    let unknown = check(7);
    assert_eq!(unknown.outcome, Outcome::Refused);
    assert_eq!(unknown.error.as_deref(), Some(UNKNOWN_PHASE));

    let closed = reply::close_in(
        close,
        instance,
        &TestIn::<InHead>::new().set(|i| &i.op, slot::CLOSE),
        {
            let mut h: OutHead = zeroed();
            h.size = size_of::<OutHead>() as u32;
            h
        },
    );
    assert_eq!(closed.outcome, Outcome::Ready);
}
