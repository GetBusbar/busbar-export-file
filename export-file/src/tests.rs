// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The file sink's own contract: the words its settings errors carry, what a delivery raises, and
//! what its Statement states. The door over the real loader, linked and dropped in, is the plugin
//! crate's `tests/conformance.rs`.

use super::*;
use std::mem::size_of;
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

/// A fresh delivery's progress: at its first line, against `path`.
fn fresh(path: &str) -> Progress {
    Progress {
        path: path.to_string(),
        line: 0,
        issued: 0,
    }
}

/// The lines a delivery hands the host, in order, each answered by `answer(index)`.
fn handed(
    s: &FileSink,
    batch: &[u8],
    at: &mut Progress,
    answer: impl Fn(usize) -> Poll<Result<DiskWritten, DiskFailure>>,
) -> (Poll<()>, Vec<Vec<u8>>) {
    let mut lines = Vec::new();
    let polled = s.deliver(batch, at, |line| {
        lines.push(line.to_vec());
        answer(lines.len() - 1)
    });
    (polled, lines)
}

/// A batch goes to the host's disk lane ONE LINE PER APPEND, each unchanged and in order, as
/// 1.5.5's sink appended each line on its own; an instance whose settings did not parse, or an
/// empty batch, asks the host nothing.
#[test]
fn a_delivery_hands_the_host_one_line_per_append() {
    let s = sink(serde_json::json!({"path": "/var/log/x.jsonl", "rotate_mb": 2}));
    let landed = |_| Poll::Ready(Ok(report(0, 0, 1)));
    let (polled, lines) = handed(&s, b"", &mut fresh("/var/log/x.jsonl"), landed);
    assert_eq!((polled, lines.len()), (Poll::Ready(()), 0));
    let unparsed = sink(serde_json::json!({"rotate_mb": 2}));
    let (polled, lines) = handed(&unparsed, b"{}\n", &mut fresh(""), landed);
    assert_eq!((polled, lines.len()), (Poll::Ready(()), 0));
    let mut at = fresh("/var/log/x.jsonl");
    let (polled, lines) = handed(&s, b"{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n", &mut at, landed);
    assert_eq!(polled, Poll::Ready(()));
    assert_eq!(lines, [&b"{\"a\":1}\n"[..], b"{\"b\":2}\n", b"{\"c\":3}\n"]);
    assert_eq!(at.line, 3);
}

/// A FAILED APPEND DROPS ITS OWN LINE, AS 1.5.5'S DID (one open and one write per line, one warning
/// per dropped line): the line the host could not append is raised once, and the lines after it
/// still go to the host and land. RED before: the whole batch was one append, so one failure
/// dropped every line of it under a single warning.
#[test]
fn a_failed_append_drops_only_its_own_line() {
    let s = sink(serde_json::json!({"path": "/var/log/x.jsonl", "rotate_mb": 2}));
    let mut at = fresh("/var/log/x.jsonl");
    let mut lines = Vec::new();
    let events = logged(|| {
        let (polled, got) = handed(&s, b"{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n", &mut at, |i| {
            if i == 1 {
                failed(
                    busbar_contract::abi::host::service::DISK_APPEND_FAILED,
                    "No space left on device (os error 28)",
                )
                .into()
            } else {
                Poll::Ready(Ok(report(0, 0, 8)))
            }
        });
        assert_eq!(polled, Poll::Ready(()));
        lines = got;
    });
    assert_eq!(lines.len(), 3, "every line still goes to the host");
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(field(&events[0], "diag"), APPEND_FAILED);
    assert_eq!(
        field(&events[0], "message"),
        "request-log file append failed; this log was dropped"
    );
}

/// A line the host is still appending parks the delivery AT THAT LINE: the resumed delivery
/// re-issues it and goes on, and the lines already answered are neither handed again nor raised
/// again.
#[test]
fn a_pending_line_parks_the_delivery_at_that_line() {
    let s = sink(serde_json::json!({"path": "/var/log/x.jsonl"}));
    let batch = b"{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n";
    let mut at = fresh("/var/log/x.jsonl");
    let events = logged(|| {
        let (polled, lines) = handed(&s, batch, &mut at, |i| {
            if i == 0 {
                failed(
                    busbar_contract::abi::host::service::DISK_OPEN_FAILED,
                    "denied",
                )
                .into()
            } else {
                Poll::Pending
            }
        });
        assert_eq!((polled, lines.len()), (Poll::Pending, 2));
    });
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(at.line, 1, "parked at the pending second line");
    let events = logged(|| {
        let (polled, lines) = handed(&s, batch, &mut at, |_| Poll::Ready(Ok(report(0, 0, 8))));
        assert_eq!(polled, Poll::Ready(()));
        assert_eq!(lines, [&b"{\"b\":2}\n"[..], b"{\"c\":3}\n"]);
    });
    assert!(events.is_empty(), "{events:?}");
}

/// A line the host could not open the file for is ONE open-failed warning, naming the path and the
/// host's words; one whose write failed, or one the host never ran, is ONE append-failed warning;
/// a landed line raises nothing.
#[test]
fn a_dropped_line_is_one_warning_naming_the_step_the_host_failed() {
    let s = sink(serde_json::json!({"path": "/var/log/x.jsonl", "rotate_mb": 2}));
    let events = logged(|| {
        s.settle(&failed(
            busbar_contract::abi::host::service::DISK_OPEN_FAILED,
            "No such file or directory (os error 2)",
        ))
    });
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

/// The destination keeps no archives, so the sink raises nothing about a rollover: whatever
/// rotation facts a host reports beside a landed line, the line is all there is (1.5.5 truncated in
/// place and said nothing).
#[test]
fn a_landed_line_raises_nothing_whatever_rotation_is_reported() {
    let s = sink(serde_json::json!({"path": "/var/log/x.jsonl", "rotate_mb": 1}));
    assert!(logged(|| s.settle(&Ok(report(1, 0b111, 3)))).is_empty());
}

/// 1.5.5's WRITE, verbatim (v1.5.5 `crates/busbar/src/export/file.rs` 96-108): when the file
/// already holds `rotate_bytes`, open it truncated, else for append; then `writeln!` the line.
fn append_1_5_5(path: &std::path::Path, rotate_bytes: Option<u64>, line: &str) {
    use std::io::Write as _;
    let truncate = rotate_bytes
        .and_then(|limit| std::fs::metadata(path).ok().map(|m| m.len() >= limit))
        .unwrap_or(false);
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(!truncate)
        .write(true)
        .truncate(truncate)
        .open(path)
        .expect("opens");
    writeln!(file, "{line}").expect("writes");
}

/// The host's disk lane for a destination that keeps `keep` archives (busbar
/// `crates/busbar-kernel/src/host_disk.rs`): a due `keep` 0 file is truncated in place, then the
/// bytes are written whole.
fn host_append(path: &std::path::Path, rotate_at: u64, keep: u64, bytes: &[u8]) -> DiskWritten {
    use std::io::Write as _;
    assert_eq!(keep, 0, "this sink's destination keeps no archives");
    let truncate = std::fs::metadata(path).is_ok_and(|m| m.len() >= rotate_at);
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(!truncate)
        .write(true)
        .truncate(truncate)
        .open(path)
        .expect("opens");
    file.write_all(bytes).expect("writes");
    report(0, 0, bytes.len() as u64)
}

/// THE FILE AFTER ROLLOVER IS 1.5.5'S, BYTE FOR BYTE: the same lines in the same batches, under
/// `rotate_mb: 1`, through this sink and the retention its manifest declares, leave the same bytes
/// as 1.5.5's sink left, and no other file — including a rollover that falls inside a batch. RED
/// before: the whole batch was one append, so the file rolled over only at batch boundaries and
/// kept lines 1.5.5 had truncated away (and the host renamed it to archives).
#[test]
fn the_file_after_rollover_is_1_5_5s_byte_for_byte() {
    let d: serde_json::Value =
        serde_json::from_slice(include_bytes!("../declares.json")).expect("declares.json parses");
    let keep = d["keep"][DESTINATION]
        .as_u64()
        .expect("the destination declares its keep");
    let dir = std::env::temp_dir().join(format!(
        "busbar-export-file-rollover-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    let ours = dir.join("ours.jsonl");
    let theirs_dir = dir.join("1.5.5");
    std::fs::create_dir_all(&theirs_dir).expect("scratch dir");
    let theirs = theirs_dir.join("ours.jsonl");
    let path = ours.display().to_string();
    let s = sink(serde_json::json!({"path": path, "rotate_mb": 1}));
    let pad = "x".repeat(400_000);
    let lines: Vec<String> = (0..8)
        .map(|n| serde_json::json!({"n": n, "pad": pad}).to_string())
        .collect();
    for chunk in lines.chunks(4) {
        let batch: String = chunk.iter().map(|l| format!("{l}\n")).collect();
        let polled = s.deliver(batch.as_bytes(), &mut fresh(&path), |line| {
            Poll::Ready(Ok(host_append(&ours, 1024 * 1024, keep, line)))
        });
        assert_eq!(polled, Poll::Ready(()));
        for line in chunk {
            append_1_5_5(&theirs, Some(1024 * 1024), line);
        }
    }
    let got = std::fs::read(&ours).expect("the live file");
    assert!(
        got == std::fs::read(&theirs).expect("1.5.5's file"),
        "the bytes differ from 1.5.5's"
    );
    assert_eq!(
        got,
        format!("{}\n{}\n", lines[6], lines[7]).into_bytes(),
        "the second rollover, inside the second batch, left its last two lines"
    );
    let beside: Vec<_> = std::fs::read_dir(&dir)
        .expect("scratch dir")
        .map(|e| e.expect("an entry").file_name())
        .collect();
    assert_eq!(
        beside.len(),
        2,
        "the live file and 1.5.5's directory, no archive: {beside:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A reload's settings replace the instance's: unparsed settings stop the deliveries, good ones
/// resume them at the new path.
#[test]
fn a_refresh_replaces_the_settings() {
    let s = sink(serde_json::json!({"path": "/a"}));
    s.refresh(b"{}", &[], 2).expect("a refresh applies");
    assert_eq!(s.path(), None);
    let (polled, lines) = handed(&s, b"{}\n", &mut fresh(""), |_| {
        Poll::Ready(Ok(report(0, 0, 3)))
    });
    assert_eq!((polled, lines.len()), (Poll::Ready(()), 0));
    s.refresh(br#"{"path":"/b"}"#, &[], 3)
        .expect("a refresh applies");
    assert_eq!(s.path().as_deref(), Some("/b"));
}

/// The code a delivery raises and the destination it names are the ones the manifest DECLARES,
/// the dropped counter is the declared SHED counter, and the destination declares it keeps NO
/// archives (`keep` 0: the host truncates it in place, 1.5.5's rollover). Nothing else is
/// declared: a sink that keeps no archives has no rotation step to report.
#[test]
fn what_the_sink_raises_is_declared() {
    let d: serde_json::Value =
        serde_json::from_slice(include_bytes!("../declares.json")).expect("declares.json parses");
    let codes: Vec<String> = d["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| format!("BUSBAR-{}", x["code"]))
        .collect();
    assert_eq!(codes, [APPEND_FAILED, OPEN_FAILED]);
    let shed: Vec<&str> = d["metrics"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|m| m["shed"].as_bool().unwrap_or(false))
        .map(|m| m["name"].as_str().unwrap())
        .collect();
    assert_eq!(shed, vec!["busbar_file_logs_dropped_total"]);
    assert_eq!(d["destinations"], serde_json::json!([DESTINATION]));
    assert_eq!(d["keep"], serde_json::json!({ "path": 0 }));
}

/// The Statement: the plugin's name and version, 1.5.5's in-flight bound, the module name as its
/// alias, the `logs` stream and no route (a push-only sink).
#[test]
fn the_statement_states_the_sink() {
    assert_eq!(STATEMENT.name.len(), NAME.len());
    assert_eq!(STATEMENT.max_inflight, 64);
    assert_eq!(STATEMENT.rewrites_len(), 1);
    assert_eq!(REWRITES[0].class, REWRITE_ALIAS);
    assert_eq!(REWRITES[0].from.len(), ALIAS.len());
    assert_eq!(STREAMS, &[ExportStream::Logs as u8]);
    assert_eq!(TAIL.streams_len(), 1);
    assert_eq!(TAIL.routes_len(), 0);
    assert!(TAIL.routes_ptr().is_null());
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
            &failed(
                busbar_contract::abi::host::service::DISK_APPEND_FAILED,
                "No space left on device (os error 28)",
            ),
        )
    });
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(field(&events[0], "path"), "/old.jsonl");
}
