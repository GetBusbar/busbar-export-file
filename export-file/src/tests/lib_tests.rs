// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The file sink's own contract: the words its settings errors carry, what a delivery raises, and
//! what its Statement states. The door over the real loader, linked and dropped in, is the plugin
//! crate's `tests/conformance.rs`.

#[path = "../../../testing/settings/integer_token.rs"]
mod settings_profile;

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

#[test]
fn the_sink_keeps_nine_archives_unless_the_operator_says_otherwise() {
    let d: serde_json::Value = serde_json::from_str(DECLARES).unwrap();
    assert_eq!(d["keep"], serde_json::json!({ "path": 9 }));
    // The keep the host applies (declared) is the keep the sink's lines name when none is set.
    assert_eq!(d["keep"]["path"], ROTATE_ARCHIVE_LIMIT);
    for keep in [0, 1, 9, MAX_KEEP] {
        let s = format!(r#"{{"path":"/x","rotate_mb":1,"keep":{keep}}}"#);
        assert!(FileSink::validate(s.as_bytes()).is_ok(), "keep {keep}");
    }
    assert_eq!(
        refused(r#"{"path":"/x","keep":65}"#),
        "settings: `keep` is at most 64 archives"
    );
    // The SDK reads through an arbitrary-precision Value before the unsigned field.
    for token in ["-1", "4294967296"] {
        let settings = format!(r#"{{"path":"/x","keep":{token}}}"#);
        assert_eq!(refused(&settings), "settings: invalid number", "{token}");
    }
}

/// A path the host can never bind (empty, or holding a NUL) is refused at settings time, each in
/// its own words, not left to drop every batch at runtime (README "Differences from 1.5.5").
#[test]
fn a_path_the_host_cannot_bind_is_refused() {
    assert_eq!(
        refused(r#"{"path":""}"#),
        "settings: `path` must name a file"
    );
    assert_eq!(
        refused(r#"{"path":"a\u0000b"}"#),
        "settings: `path` must not contain a NUL character"
    );
}

/// `open` refuses exactly what `validate` refuses, in the same words: an instance never opens on
/// a path the host cannot bind or on more archives than the host keeps.
#[test]
fn open_refuses_what_validate_refuses() {
    for settings in [
        r#"{"path":""}"#,
        r#"{"path":"a\u0000b"}"#,
        r#"{"path":"/x","keep":65}"#,
    ] {
        let r = FileSink::open(settings.as_bytes(), &[], 1).expect_err(settings);
        assert_eq!(r.outcome(), Outcome::Failed, "{settings}");
        assert_eq!(r.text(), Some(refused(settings).as_str()), "{settings}");
    }
}

/// `rotate_mb: 0` is accepted as 1.5.5 accepted it (it rotates before every append), and an
/// instance opened or refreshed on it says so in its log.
#[test]
fn a_zero_rotate_mb_is_accepted_and_says_so() {
    assert!(FileSink::validate(br#"{"path":"x","rotate_mb":0}"#).is_ok());
    let want = "request-log file `rotate_mb: 0` rotates the file before every append";
    let mut opened = None;
    let events = logged(|| {
        opened = FileSink::open(br#"{"path":"/x","rotate_mb":0,"keep":0}"#, &[], 1).ok();
    });
    let s = opened.expect("rotate_mb 0 opens");
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(field(&events[0], "level"), "WARN");
    assert_eq!(field(&events[0], "path"), "/x");
    assert_eq!(field(&events[0], "message"), want);
    assert_eq!(s.keep(), 0);
    let events = logged(|| {
        s.refresh(br#"{"path":"/y","rotate_mb":0}"#, &[], 2)
            .expect("rotate_mb 0 refreshes");
    });
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(field(&events[0], "message"), want);
    assert_eq!(&*s.path(), "/y");
    let events = logged(|| {
        s.refresh(br#"{"path":"/z","rotate_mb":1}"#, &[], 3)
            .expect("rotate_mb 1 refreshes");
    });
    assert!(events.is_empty(), "{events:?}");
}

#[test]
fn a_wrong_typed_rotate_mb_is_refused_without_echoing_values() {
    // Negative and overflowing numbers remain refusals with the SDK's exact diagnostic.
    for token in ["-1", "18446744073709551616"] {
        let settings = format!(r#"{{"path":"/x","rotate_mb":{token}}}"#);
        assert_eq!(refused(&settings), "settings: invalid number", "{token}");
    }
    assert_eq!(
        refused(r#"{"path":"/x","rotate_mb":"one"}"#),
        "settings: invalid type: string \"***\", expected u64"
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
fn an_empty_batch_asks_nothing_and_no_host_tables_answer_unarmed() {
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

    let events = logged(|| s.settle_at("/old.jsonl", 5, &retention));
    assert_eq!(field(&events[0], "archive"), "/old.jsonl.5");
}

/// With `keep: 0` (truncate in place, no archives), the retention line names the path rather than a
/// nonexistent archive, and a rotation says "truncation" rather than "rename".
#[test]
fn keep_zero_diagnostics_do_not_name_a_nonexistent_archive() {
    let s = sink(serde_json::json!({"path": "/var/log/x.jsonl", "rotate_mb": 1, "keep": 0}));

    // Retention with keep=0: must not name `<path>.0`.
    let events = logged(|| s.settle(&Ok(report(0, DISK_RETENTION_FAILED, 0))));
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(field(&events[0], "diag"), RETENTION_FAILED);
    assert_eq!(field(&events[0], "path"), "/var/log/x.jsonl");
    assert!(
        events[0].iter().all(|(n, _)| n != "archive"),
        "keep=0 retention must not name an archive: {events:?}"
    );
    assert_eq!(
        field(&events[0], "message"),
        "request-log retention cleanup failed during in-place truncation"
    );

    // Rotation with keep=0: truncation, not rename.
    let events = logged(|| s.settle(&Ok(report(DISK_ROTATED, 0, 3))));
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(field(&events[0], "level"), "INFO");
    assert_eq!(field(&events[0], "path"), "/var/log/x.jsonl");
    assert!(
        events[0].iter().all(|(n, _)| n != "archive"),
        "keep=0 rotation must not name an archive: {events:?}"
    );
    assert_eq!(
        field(&events[0], "message"),
        "request-log file rotated by truncation"
    );
}

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
    // The semantic checks hold through refresh too, in `parse`'s exact words.
    for (settings, text) in [
        (&br#"{"path":""}"#[..], "settings: `path` must name a file"),
        (
            &br#"{"path":"a\u0000b"}"#[..],
            "settings: `path` must not contain a NUL character",
        ),
        (
            &br#"{"path":"/x","keep":65}"#[..],
            "settings: `keep` is at most 64 archives",
        ),
    ] {
        let r = s
            .refresh(settings, &[], 2)
            .expect_err("semantically bad settings refuse");
        assert_eq!(r.outcome(), Outcome::Failed);
        assert_eq!(r.text().unwrap(), text);
        assert_eq!(&*s.path(), "/a");
    }
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
/// the reloaded settings: the unit that holds the decision (`issued_at`, the pair `Deliver` parks).
#[test]
fn a_resumed_delivery_settles_against_the_pair_it_parked() {
    let s = sink(serde_json::json!({"path": "/a.jsonl", "keep": 3}));
    let parked = issued_at(None, &s);
    assert_eq!((&*parked.0, parked.1), ("/a.jsonl", 3));
    s.refresh(br#"{"path":"/b.jsonl"}"#, &[], 2)
        .expect("a refresh applies");
    let resumed = issued_at(Some(parked), &s);
    assert_eq!((&*resumed.0, resumed.1), ("/a.jsonl", 3));
    let fresh = issued_at(None, &s);
    assert_eq!((&*fresh.0, fresh.1), ("/b.jsonl", ROTATE_ARCHIVE_LIMIT));
    let events = logged(|| {
        s.settle_at(&resumed.0, resumed.1, &failed(DISK_OPEN_FAILED, "gone"));
    });
    assert_eq!(field(&events[0], "path"), "/a.jsonl");
}

#[test]
fn a_reload_replaces_an_explicit_keep_and_can_restore_the_default() {
    let s = sink(serde_json::json!({"path": "/a.jsonl"}));
    assert_eq!(s.keep(), 9);
    for (generation, keep) in [(2, 3), (3, 5), (4, 0)] {
        let settings = format!(r#"{{"path":"/a.jsonl","keep":{keep}}}"#);
        s.refresh(settings.as_bytes(), &[], generation)
            .expect("an explicit keep applies");
        assert_eq!(s.keep(), keep);
        let events = logged(|| s.settle(&Ok(report(0, DISK_RETENTION_FAILED, 0))));
        if keep != 0 {
            assert_eq!(field(&events[0], "archive"), format!("/a.jsonl.{keep}"));
        } else {
            assert_eq!(field(&events[0], "path"), "/a.jsonl");
            assert!(
                events[0].iter().all(|(n, _)| n != "archive"),
                "keep=0 retention must not name an archive"
            );
        }
    }
    s.refresh(br#"{"path":"/a.jsonl"}"#, &[], 5)
        .expect("omitting keep restores the default");
    assert_eq!(s.keep(), 9);
}

/// The linked door advertises the settings accepted by the production lifecycle, including
/// nullable options, the u64 rotation limit and the sink's path/retention checks.
#[test]
#[allow(unsafe_code)]
fn settings_schema_matches_lifecycle() {
    use busbar_contract::abi::mechanism::{call::BLOB_JSON, door::Door};
    // SAFETY: the linked door and its Statement/schema are static program data.
    let statement = unsafe { &*Door::read_parts(crate::door::door()).statement };
    let blob = &statement.settings_schema;
    assert_eq!(blob.fmt, BLOB_JSON);
    assert_eq!(blob.flags, 0);
    assert!(!blob.ptr.is_null());
    // SAFETY: this is the immutable schema published by the linked door above.
    let bytes = unsafe { std::slice::from_raw_parts(blob.ptr, blob.len) };
    assert_eq!(bytes, include_bytes!("../../settings.schema.json"));
    let schema: serde_json::Value = serde_json::from_slice(bytes).expect("schema JSON");
    let validator = settings_profile::validator(&schema).expect("valid JSON Schema");

    use serde_json::json;
    for (settings, accepted) in [
        (json!({"path":"/logs/requests.jsonl"}), true),
        (json!({"path":"x", "rotate_mb":null, "keep":null}), true),
        (json!({"path":"x", "rotate_mb":0, "keep":0}), true),
        (
            json!({"path":"x", "rotate_mb":u64::MAX, "keep":MAX_KEEP}),
            true,
        ),
        (json!({}), false),
        (json!({"path":""}), false),
        (json!({"path":"a\u{0}b"}), false),
        (json!({"path":null}), false),
        (json!({"path":"x", "keep":MAX_KEEP + 1}), false),
        (json!({"path":"x", "rotate_mb":-1}), false),
        (json!({"path":"x", "rotate_mb":"1"}), false),
        (json!({"path":"x", "extra":true}), false),
    ] {
        assert_eq!(
            validator.is_valid(&settings),
            accepted,
            "schema: {settings}"
        );
        assert_eq!(
            FileSink::validate(settings.to_string().as_bytes()).is_ok(),
            accepted,
            "lifecycle: {settings}"
        );
    }
    let defaults = FileSink::open(br#"{"path":"x"}"#, &[], 1).expect("defaults open");
    assert_eq!(schema["properties"]["keep"]["default"], defaults.keep());
}

#[test]
fn settings_schema_integer_tokens_match_native_parser() {
    let schema: serde_json::Value =
        serde_json::from_slice(include_bytes!("../../settings.schema.json")).unwrap();
    let profile = settings_profile::validator(&schema).unwrap();
    let standard = jsonschema::validator_for(&schema).unwrap();
    assert_eq!(schema["x-busbar-settings-profile"], "integer-token-v1");
    for (pointer, template, integer, kind) in [
        (
            r#"/properties/rotate_mb"#,
            r#"{"path":"x","rotate_mb":TOKEN}"#,
            "1",
            "unsigned",
        ),
        (
            r#"/properties/keep"#,
            r#"{"path":"x","keep":TOKEN}"#,
            "1",
            "unsigned",
        ),
    ] {
        assert_eq!(
            schema.pointer(pointer).unwrap()["x-busbar-integer-token"],
            kind
        );
        for (token, accepted) in [
            (integer.to_string(), true),
            (format!("{integer}.0"), false),
            (format!("{integer}e0"), false),
            (format!("{integer}E+0"), false),
        ] {
            let text = template.replace("TOKEN", &token);
            let value: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert!(
                standard.is_valid(&value),
                "standard mathematical integer: {text}"
            );
            assert_eq!(profile.is_valid(&value), accepted, "profile: {text}");
            assert_eq!(
                FileSink::validate(text.as_bytes()).is_ok(),
                accepted,
                "native: {text}"
            );
        }
        // Do not infer signed negative-zero behavior from a regular expression:
        // the profile must agree with the actual lifecycle parser.
        for token in [
            "null",
            "0",
            "-0",
            "-0.0",
            "0e0",
            "-1",
            "18446744073709551616",
        ] {
            let text = template.replace("TOKEN", token);
            let value: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert_eq!(
                profile.is_valid(&value),
                FileSink::validate(text.as_bytes()).is_ok(),
                "native sign/range: {text}"
            );
        }
    }
}
