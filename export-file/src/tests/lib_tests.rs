// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The file sink's own contract: the words its settings errors carry, what a delivery raises, and
//! what its Statement states. The door over the real loader, linked and dropped in, is the plugin
//! crate's `tests/conformance.rs`.

use super::*;
use crate::door::issued_at;
use busbar_contract::abi::mechanism::call::Outcome;
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

pub(crate) const TICKET: busbar_contract::abi::mechanism::ticket::Ticket =
    busbar_contract::abi::mechanism::ticket::Ticket {
        slot: 1,
        generation: 1,
    };

pub(crate) fn report(rotated: u8, faults: u8, written: u64) -> DiskWritten {
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
