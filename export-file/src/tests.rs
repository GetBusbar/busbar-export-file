// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The file sink's own contract: the words its settings errors carry, what a delivery raises, and
//! what its Statement states. The door over the real loader, linked and dropped in, is the plugin
//! crate's `tests/conformance.rs`.

use super::*;
use std::sync::{Arc, Mutex};

fn sink(settings: serde_json::Value) -> FileSink {
    FileSink::open(settings.to_string().as_bytes(), &[], 1).expect("the sink never refuses to open")
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
        "settings: unknown field `rotate`, expected `path` or `rotate_mb`"
    );
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

/// A batch lands through the host's disk lane: an instance whose settings did not parse, or an
/// empty batch, asks the host nothing; one with no host tables answers that it was handed none.
#[test]
fn a_delivery_asks_the_host_only_with_a_path_and_a_batch() {
    let s = sink(serde_json::json!({"path": "/var/log/x.jsonl", "rotate_mb": 2}));
    assert_eq!(s.append(None, TICKET, b""), Poll::Ready(None));
    let unparsed = sink(serde_json::json!({"rotate_mb": 2}));
    assert_eq!(unparsed.append(None, TICKET, b"{}\n"), Poll::Ready(None));
    match s.append(None, TICKET, b"{}\n") {
        Poll::Ready(Some(Err(f))) => assert_eq!(f.why, ConnFailure::Unarmed),
        other => panic!("{other:?}"),
    }
}

/// An append the host could not open for is ONE open-failed line, naming the path and the host's
/// words; one whose write failed is ONE append-failed line; a landed batch raises nothing.
#[test]
fn a_dropped_batch_is_one_line_naming_the_step_the_host_failed() {
    let s = sink(serde_json::json!({"path": "/var/log/x.jsonl", "rotate_mb": 2}));
    let events = logged(|| s.settle(&failed(0, "No such file or directory (os error 2)")));
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(field(&events[0], "diag"), OPEN_FAILED);
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
}

/// A reload's settings replace the instance's: unparsed settings stop the deliveries, good ones
/// resume them at the new path.
#[test]
fn a_refresh_replaces_the_settings() {
    let s = sink(serde_json::json!({"path": "/a"}));
    s.refresh(b"{}", &[], 2).expect("a refresh applies");
    assert_eq!(s.path(), None);
    assert_eq!(s.append(None, TICKET, b"{}\n"), Poll::Ready(None));
    s.refresh(br#"{"path":"/b"}"#, &[], 3)
        .expect("a refresh applies");
    assert_eq!(s.path().as_deref(), Some("/b"));
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
    assert_eq!(STATEMENT.name.len, NAME.len());
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
