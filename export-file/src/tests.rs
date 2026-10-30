// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The file sink's own contract: the words its settings errors carry, what a delivery raises, and
//! what its Statement states. The door over the real loader, linked and dropped in, is the plugin
//! crate's `tests/conformance.rs`.

use super::*;
use busbar_contract::abi::mechanism::call::InHead;
use busbar_contract::abi::mechanism::door::REWRITE_ALIAS;
use std::sync::{Arc, Mutex};

fn sink(settings: serde_json::Value) -> FileSink {
    FileSink::open(settings.to_string().as_bytes(), &[], 1)
        .expect("the sink opens on settings that parse")
}

fn refused(settings: &str) -> String {
    let r = FileSink::validate(settings.as_bytes()).expect_err("refused");
    assert_eq!(r.outcome(), Outcome::Failed);
    r.text().expect("a refusal says why").to_string()
}

/// The settings error is the configuration grammar's own serde line; good settings report nothing.
#[test]
fn settings_are_validated_in_the_configurations_words() {
    assert!(FileSink::validate(br#"{"path":"/x","rotate_mb":1}"#).is_ok());
    assert_eq!(refused("{}"), "settings: missing field `path`");
    assert_eq!(refused(""), "settings: missing field `path`");
    assert_eq!(
        refused(r#"{"path":"/x","rotate":1}"#),
        "settings: unknown field `rotate`, expected one of `path`, `rotate_mb`, `keep`"
    );
}

/// KEEP N, DEFAULT 9 (owner signed 2026-10-27): the sink declares `keep: 9` for its destination, so
/// the host rotates by rename keeping nine archives; an operator's `keep` (0 = truncate in place,
/// as 1.5.5) is accepted up to the host's bound and refused past it.
#[test]
fn the_sink_keeps_nine_archives_unless_the_operator_says_otherwise() {
    let d: serde_json::Value = serde_json::from_str(DECLARES).unwrap();
    assert_eq!(d["keep"], serde_json::json!({ "path": 9 }));
    for keep in [0, 1, 9, MAX_KEEP] {
        let s = format!(r#"{{"path":"/x","rotate_mb":1,"keep":{keep}}}"#);
        assert!(FileSink::validate(s.as_bytes()).is_ok(), "keep {keep}");
    }
    assert_eq!(
        refused(r#"{"path":"/x","keep":65}"#),
        "settings: `keep` is at most 64 archives"
    );
    assert_eq!(
        refused(r#"{"path":"/x","keep":-1}"#),
        "settings: invalid value: integer `-1`, expected u32"
    );
}

/// A path the host can never bind (empty, or holding a NUL) is refused at settings time, not
/// left to drop every batch at runtime.
#[test]
fn a_path_the_host_cannot_bind_is_refused() {
    let want = "settings: `path` must name a file";
    assert_eq!(refused(r#"{"path":""}"#), want);
    assert_eq!(refused(r#"{"path":"a\u0000b"}"#), want);
}

/// `rotate_mb: 0` would rotate before every append; core's own grammar refuses it too.
#[test]
fn a_zero_rotate_mb_is_refused() {
    assert_eq!(
        refused(r#"{"path":"x","rotate_mb":0}"#),
        "settings: `rotate_mb` must be at least 1"
    );
    assert!(FileSink::validate(br#"{"path":"x","rotate_mb":1}"#).is_ok());
}

/// EFILE-7: a wrong-typed `rotate_mb` is refused in the configuration's own serde words.
#[test]
fn a_wrong_typed_rotate_mb_is_refused_in_serde_words() {
    assert_eq!(
        refused(r#"{"path":"/x","rotate_mb":-1}"#),
        "settings: invalid value: integer `-1`, expected u64"
    );
    assert_eq!(
        refused(r#"{"path":"/x","rotate_mb":"one"}"#),
        "settings: invalid type: string \"one\", expected u64"
    );
}

/// One logged event: every field it carried, by name, and its message.
type Event = Vec<(String, String)>;

/// The `tracing` events captured, in order.
#[derive(Default, Clone)]
struct Capture(Arc<Mutex<Vec<Event>>>);

struct Fields(Vec<(String, String)>);

impl tracing::field::Visit for Fields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0
            .push((field.name().to_string(), format!("{value:?}")));
    }
}

impl tracing::Subscriber for Capture {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        let mut v = Fields(Vec::new());
        event.record(&mut v);
        v.0.push(("level".to_string(), event.metadata().level().to_string()));
        self.0.lock().unwrap().push(v.0);
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

/// Run `f` and return every event it logged, in order.
fn logged(f: impl FnOnce()) -> Vec<Event> {
    let cap = Capture::default();
    tracing::subscriber::with_default(cap.clone(), f);
    let got = cap.0.lock().unwrap().clone();
    got
}

fn field(fields: &[(String, String)], name: &str) -> String {
    fields
        .iter()
        .find(|(n, _)| n == name)
        .map(|(_, v)| v.clone())
        .unwrap_or_else(|| panic!("no `{name}` field in {fields:?}"))
}

const TICKET: busbar_contract::abi::mechanism::ticket::Ticket =
    busbar_contract::abi::mechanism::ticket::Ticket {
        slot: 1,
        generation: 1,
    };

fn report(rotated: u8, faults: u8, written: u64) -> DiskWritten {
    DiskWritten {
        size: size_of::<DiskWritten>() as u32,
        rotated,
        faults,
        _reserved: [0; 2],
        written,
    }
}

fn failed(step: u64, why: &str) -> Result<DiskWritten, DiskFailure> {
    Err(DiskFailure {
        step,
        rotation: report(0, 0, 0),
        why: ConnFailure::Failed(why.to_string()),
    })
}

/// A batch lands through the host's disk lane: an empty batch asks the host nothing; one with no
/// host tables answers that it was handed none. Settings that do not parse never open an instance.
#[test]
fn a_delivery_asks_the_host_only_with_a_path_and_a_batch() {
    let s = sink(serde_json::json!({"path": "/var/log/x.jsonl", "rotate_mb": 2}));
    assert_eq!(s.append(None, TICKET, b""), Poll::Ready(None));
    let r = FileSink::open(br#"{"rotate_mb":2}"#, &[], 1).expect_err("unparsed settings refuse");
    assert_eq!(r.outcome(), Outcome::Failed);
    assert_eq!(r.text().unwrap(), "settings: missing field `path`");
    match s.append(None, TICKET, b"{}\n") {
        Poll::Ready(Some(Err(f))) => assert_eq!(f.why, ConnFailure::Unarmed),
        other => panic!("{other:?}"),
    }
}

/// An append the host could not open for is ONE open-failed line, naming the path and the host's
/// words; one whose write failed, or one the host never ran, is ONE append-failed line; a landed
/// batch raises nothing.
#[test]
fn a_dropped_batch_is_one_line_naming_the_step_the_host_failed() {
    let s = sink(serde_json::json!({"path": "/var/log/x.jsonl", "rotate_mb": 2}));
    let events = logged(|| {
        s.settle(&failed(
            busbar_contract::abi::host::service::DISK_OPEN_FAILED,
            "No such file or directory (os error 2)",
        ))
    });
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(field(&events[0], "diag"), OPEN_FAILED);
    assert_eq!(field(&events[0], "level"), "WARN");
    assert_eq!(field(&events[0], "path"), "/var/log/x.jsonl");
    assert_eq!(
        field(&events[0], "error"),
        "No such file or directory (os error 2)"
    );
    assert_eq!(
        field(&events[0], "message"),
        "request-log file open failed; this log was dropped"
    );
    let events = logged(|| {
        s.settle(&failed(
            busbar_contract::abi::host::service::DISK_APPEND_FAILED,
            "No space left on device (os error 28)",
        ));
    });
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(field(&events[0], "diag"), APPEND_FAILED);
    assert_eq!(field(&events[0], "level"), "WARN");
    assert_eq!(
        field(&events[0], "message"),
        "request-log file append failed; this log was dropped"
    );
    // An append the host never ran (no ticket: step 0) is no open failure, and still a declared
    // line: the append-failed one, never an uncoded warning (a plugin speaks only over the ABI,
    // Part 2 #2). RED before: an uncoded "was not run by the host" warning with no `diag`.
    let events = logged(|| {
        s.settle(&Err(DiskFailure {
            step: 0,
            rotation: report(0, 0, 0),
            why: ConnFailure::NoTicket,
        }));
    });
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(field(&events[0], "diag"), APPEND_FAILED);
    assert_eq!(
        field(&events[0], "message"),
        "request-log file append failed; this log was dropped"
    );
    assert!(logged(|| s.settle(&Ok(report(0, 0, 3)))).is_empty());
}

/// A rotation the host ran before the append is raised as it happened: each failed step under its
/// declared code (retention, shift, rename), then the rotation itself when the file was renamed.
#[test]
fn a_rotation_is_raised_step_by_step() {
    let s = sink(serde_json::json!({"path": "/var/log/x.jsonl", "rotate_mb": 1}));
    let events = logged(|| s.settle(&Ok(report(DISK_ROTATED, 0, 3))));
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(field(&events[0], "archive"), "/var/log/x.jsonl.1");
    assert_eq!(field(&events[0], "level"), "INFO");
    assert_eq!(
        field(&events[0], "message"),
        "request-log file rotated by rename"
    );
    let all = DISK_RETENTION_FAILED | DISK_SHIFT_FAILED | DISK_RENAME_FAILED;
    let events = logged(|| s.settle(&Ok(report(0, all, 3))));
    let codes: Vec<String> = events.iter().map(|e| field(e, "diag")).collect();
    assert_eq!(
        codes,
        [RETENTION_FAILED, SHIFT_FAILED, ROTATE_RENAME_FAILED]
    );
    assert!(events.iter().all(|e| field(e, "level") == "WARN"));
    // C8: each line names what it is about.
    assert_eq!(ROTATE_ARCHIVE_LIMIT, 9);
    assert_eq!(
        field(&events[0], "archive"),
        format!("/var/log/x.jsonl.{ROTATE_ARCHIVE_LIMIT}")
    );
    assert_eq!(field(&events[1], "path"), "/var/log/x.jsonl");
    assert_eq!(field(&events[2], "path"), "/var/log/x.jsonl");
}

/// The retention line names the archive the host drops: `<path>.<keep>` with the operator's `keep`.
#[test]
fn the_retention_line_names_the_effective_keep() {
    let retention = Ok(report(0, DISK_RETENTION_FAILED, 3));
    let s = sink(serde_json::json!({"path": "/var/log/x.jsonl", "rotate_mb": 1, "keep": 3}));
    let events = logged(|| s.settle(&retention));
    assert_eq!(field(&events[0], "archive"), "/var/log/x.jsonl.3");
    assert_eq!(
        field(&events[0], "message"),
        "request-log archive retention cleanup failed; the archive series may exceed its `keep` limit"
    );
    // A reload between issue and settle: the issued-at pair is what is named.
    let events = logged(|| s.settle_at("/old.jsonl", 5, &retention));
    assert_eq!(field(&events[0], "archive"), "/old.jsonl.5");
}

/// C5: each rotation fault bit alone raises exactly its own code, and nothing else.
#[test]
fn each_rotation_fault_bit_raises_exactly_its_own_code() {
    let s = sink(serde_json::json!({"path": "/var/log/x.jsonl", "rotate_mb": 1}));
    for (bit, code) in [
        (DISK_RETENTION_FAILED, RETENTION_FAILED),
        (DISK_SHIFT_FAILED, SHIFT_FAILED),
        (DISK_RENAME_FAILED, ROTATE_RENAME_FAILED),
    ] {
        let events = logged(|| s.settle(&Ok(report(0, bit, 3))));
        assert_eq!(events.len(), 1, "bit {bit}: {events:?}");
        assert_eq!(field(&events[0], "diag"), code, "bit {bit}");
        assert_eq!(field(&events[0], "level"), "WARN", "bit {bit}");
    }
    assert_eq!(RETENTION_FAILED, "BUSBAR-7075");
    assert_eq!(SHIFT_FAILED, "BUSBAR-7076");
    assert_eq!(ROTATE_RENAME_FAILED, "BUSBAR-7077");
}

/// C6: an append that failed still reports the rotation that ran before it: the rotation's lines
/// come first, the dropped line last.
#[test]
fn a_failed_append_raises_its_rotation_before_the_dropped_line() {
    let s = sink(serde_json::json!({"path": "/var/log/x.jsonl", "rotate_mb": 1}));
    let answer = Err(DiskFailure {
        step: busbar_contract::abi::host::service::DISK_APPEND_FAILED,
        rotation: report(DISK_ROTATED, DISK_SHIFT_FAILED, 0),
        why: ConnFailure::Failed("No space left on device (os error 28)".to_string()),
    });
    let events = logged(|| s.settle(&answer));
    let codes: Vec<Option<String>> = events
        .iter()
        .map(|e| e.iter().find(|(n, _)| n == "diag").map(|(_, v)| v.clone()))
        .collect();
    assert_eq!(
        codes,
        [
            Some(SHIFT_FAILED.to_string()),
            None,
            Some(APPEND_FAILED.to_string())
        ],
        "{events:?}"
    );
    assert_eq!(field(&events[1], "archive"), "/var/log/x.jsonl.1");
    assert_eq!(field(&events[1], "level"), "INFO");
    assert_eq!(
        field(&events[2], "message"),
        "request-log file append failed; this log was dropped"
    );
    assert_eq!(field(&events[2], "level"), "WARN");
}

/// A reload's settings replace the instance's; a reload whose settings do not parse is refused and
/// the old settings stay, so deliveries continue at the old path.
#[test]
fn a_refused_refresh_keeps_the_settings() {
    let s = sink(serde_json::json!({"path": "/a"}));
    let r = s
        .refresh(b"{}", &[], 2)
        .expect_err("unparsed settings refuse");
    assert_eq!(r.outcome(), Outcome::Failed);
    assert_eq!(r.text().unwrap(), "settings: missing field `path`");
    assert_eq!(&*s.path(), "/a");
    match s.append(None, TICKET, b"{}\n") {
        Poll::Ready(Some(Err(f))) => assert_eq!(f.why, ConnFailure::Unarmed),
        other => panic!("a refused reload must not stop the deliveries: {other:?}"),
    }
    s.refresh(br#"{"path":"/b"}"#, &[], 3)
        .expect("a refresh applies");
    assert_eq!(&*s.path(), "/b");
}

/// The code a delivery raises and the destination it names are the ones the manifest DECLARES,
/// and the dropped counter is the declared SHED counter.
#[test]
fn what_the_sink_raises_is_declared() {
    let d: serde_json::Value = serde_json::from_str(DECLARES).expect("declares.json parses");
    let codes: Vec<String> = d["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| format!("BUSBAR-{}", x["code"]))
        .collect();
    for code in [
        APPEND_FAILED,
        OPEN_FAILED,
        RETENTION_FAILED,
        SHIFT_FAILED,
        ROTATE_RENAME_FAILED,
    ] {
        assert!(codes.contains(&code.to_string()), "{code}");
    }
    let shed: Vec<&str> = d["metrics"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|m| m["shed"].as_bool().unwrap_or(false))
        .map(|m| m["name"].as_str().unwrap())
        .collect();
    assert_eq!(shed, vec!["busbar_file_logs_dropped_total"]);
    assert_eq!(d["destinations"], serde_json::json!([DESTINATION]));
}

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

/// THE LINES NAME THE FILE THE APPEND WAS ISSUED AGAINST (A2 r1 F1): a reload that replaced the
/// path while the append was pending must not point the operator at the new file.
/// RED before the fix: `settle_at` read the settings' path when the answer arrived.
#[test]
fn a_settled_append_names_the_path_it_was_issued_against() {
    let s = sink(serde_json::json!({"path": "/old.jsonl"}));
    s.refresh(br#"{"path":"/new.jsonl"}"#, &[], 2)
        .expect("a refresh applies");
    let events = logged(|| {
        s.settle_at(
            "/old.jsonl",
            9,
            &failed(
                busbar_contract::abi::host::service::DISK_APPEND_FAILED,
                "No space left on device (os error 28)",
            ),
        )
    });
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(field(&events[0], "path"), "/old.jsonl");
    let events = logged(|| s.settle_at("/old.jsonl", 9, &Ok(report(DISK_ROTATED, 0, 3))));
    assert_eq!(field(&events[0], "path"), "/old.jsonl");
    assert_eq!(field(&events[0], "archive"), "/old.jsonl.1");
}

/// A delivery resumed after a reload settles against the (path, keep) its append was issued at, not
/// the reloaded settings. The test kit cannot show this through the door: the lines a delivery
/// raises are folded into the reply envelope, which `reply::call_in` clears before returning, so
/// the unit that holds the decision (`issued_at`, the pair `Deliver` parks) is tested directly.
#[test]
fn a_resumed_delivery_settles_against_the_pair_it_parked() {
    let s = sink(serde_json::json!({"path": "/a.jsonl", "keep": 3}));
    let parked = issued_at(None, &s);
    assert_eq!((&*parked.0, parked.1), ("/a.jsonl", 3));
    s.refresh(br#"{"path":"/b.jsonl"}"#, &[], 2)
        .expect("a refresh applies");
    let resumed = issued_at(Some(Box::new(parked)), &s);
    assert_eq!((&*resumed.0, resumed.1), ("/a.jsonl", 3));
    let fresh = issued_at(None, &s);
    assert_eq!((&*fresh.0, fresh.1), ("/b.jsonl", ROTATE_ARCHIVE_LIMIT));
    let events = logged(|| {
        s.settle_at(&resumed.0, resumed.1, &failed(DISK_OPEN_FAILED, "gone"));
    });
    assert_eq!(field(&events[0], "path"), "/a.jsonl");
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
