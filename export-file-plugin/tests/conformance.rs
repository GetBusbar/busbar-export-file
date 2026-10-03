// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! **ONE EXPORT SINK, BOTH DOORS, ONE TABLE** — the file sink's linked + dropped-in conformance on
//! the export kind's memory ABI (THE DESIGN §11.4), run against the busbar rev this repo pins
//! (`.busbar-ref`).
//!
//! The sink is held two ways at once: LINKED (the logic crate's `door`, through the loader's
//! `load_linked`) and DROPPED IN (this crate's built cdylib, `dlopen`ed by the loader's
//! `load_dropped`, which resolves `busbar_plugin_door`, validates the door and compares its
//! Statement with the stated one byte for byte). Each is bound to a real dispatcher and driven over
//! the same script through the export kind's table: `validate` over good and refused settings,
//! `open`, `deliver`, `scrape`, `status`, `check`, `serve`, `close`, with every envelope entry the
//! host ingested. The two transcripts must be equal.
//!
//! THE RED ARMS, same file: the door asked for as another kind is refused; a stated Statement that
//! is not the door's is refused; the same door opened over unparsed settings answers a different
//! transcript (so the equality is not vacuous). A missing cdylib PANICS — this test IS the
//! dropped-in door's proof, and never skips.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use busbar_contract::abi::export::{
    slot, CheckIn, CheckOut, DeliverIn, ScrapeIn, ScrapeOut, ServeIn, ServeOut, StatusOut,
    CHECK_PHASE_INSTANCES,
};
use busbar_contract::abi::mechanism::call::{AbiStr, Blob, InHead, OutHead, BLOB_JSON, BLOB_JSONL};
use busbar_contract::abi::mechanism::lifecycle::{slot as lc, OpenIn, OpenOut, ValidateIn};
use busbar_plugin_loader::dispatch::kinds::export::Export;
use busbar_plugin_loader::dispatch::kinds::hook::Hook;
use busbar_plugin_loader::dispatch::{
    in_head, load_dropped, load_linked, out_head, rendering_of_library, Bind, Called, Diagnostic,
    DispatchConfig, Dispatcher, Dropped, EnvelopeSink, Frame, LinkedRow, LoadError, Metric, Plugin,
    NO_BLOB,
};

/// This crate's built cdylib (uplifted or under `deps`, newest wins). A missing artifact is a
/// failure, never a skip.
fn cdylib() -> PathBuf {
    let exe = std::env::current_exe().expect("the test binary has a path");
    let profile = exe
        .parent()
        .and_then(|d| d.parent())
        .expect("target/<profile>");
    let file = busbar_plugin_loader::plugin_library_filename("busbar_export_file_plugin");
    [profile.join(&file), profile.join("deps").join(&file)]
        .into_iter()
        .filter_map(|p| Some((std::fs::metadata(&p).ok()?.modified().ok()?, p)))
        .max()
        .map(|(_, p)| p)
        .unwrap_or_else(|| panic!("the busbar-export-file-plugin cdylib ({file}) is not built"))
}

/// Every envelope entry the host ingested, as text.
#[derive(Default)]
struct Recorder(Mutex<Vec<String>>);

impl EnvelopeSink for Recorder {
    fn metric(&self, m: Metric<'_>) {
        self.0
            .lock()
            .unwrap()
            .push(format!("metric {} {} {}", m.family, m.kind, m.value));
    }
    fn diag(&self, d: Diagnostic<'_>) {
        self.0.lock().unwrap().push(format!(
            "diag {} {} {}",
            String::from_utf8_lossy(d.name),
            d.severity,
            String::from_utf8_lossy(d.text)
        ));
    }
    fn dropped(&self, why: Dropped) {
        self.0.lock().unwrap().push(format!("dropped {why:?}"));
    }
}

fn bind(d: &Dispatcher, sink: Arc<Recorder>) -> Bind {
    Bind {
        instance: Arc::from("tail"),
        max_inflight_cap: 64,
        sink,
        dispatcher: d.adopter(),
        conns: None,
    }
}

/// A blank head stating `I`'s size (the host states the `in` it wrote).
fn head<I>() -> InHead {
    InHead {
        size: std::mem::size_of::<I>() as u32,
        ..in_head()
    }
}

const fn blob(bytes: &'static [u8], fmt: u32) -> Blob {
    Blob {
        ptr: bytes.as_ptr(),
        len: bytes.len(),
        fmt,
        flags: 0,
    }
}

fn answered(c: &Called) -> String {
    let error = c
        .error
        .as_deref()
        .map(String::from_utf8_lossy)
        .unwrap_or_default();
    format!("{:?} {error:?} lease={}", c.outcome, c.lease)
}

/// One door's transcript over `settings`: every op's answer, then every envelope entry.
fn transcript(p: &Plugin<Export>, rec: &Recorder, settings: &'static [u8]) -> Vec<String> {
    let mut t = Vec::new();
    for s in [
        &br#"{"path":"/x","rotate_mb":1}"#[..],
        b"{}",
        br#"{"path":"/x","rotate":1}"#,
        br#"{"path":"/x","rotate_mb":"one"}"#,
    ] {
        let mut err = vec![0_u8; 512];
        let input = ValidateIn {
            head: head::<ValidateIn>(),
            settings: Blob {
                ptr: s.as_ptr(),
                len: s.len(),
                fmt: BLOB_JSON,
                flags: 0,
            },
            err_buf: err.as_mut_ptr(),
            err_cap: err.len(),
        };
        let mut f = Frame::new(input, out_head());
        t.push(format!(
            "validate {}",
            answered(&p.call(lc::VALIDATE, &mut f))
        ));
    }

    let mut err = vec![0_u8; 512];
    let mut f = Frame::new(
        OpenIn {
            head: head::<OpenIn>(),
            host: std::ptr::null(),
            settings: Blob {
                ptr: settings.as_ptr(),
                len: settings.len(),
                fmt: BLOB_JSON,
                flags: 0,
            },
            secrets: std::ptr::null(),
            secrets_len: 0,
            generation: 1,
            err_buf: err.as_mut_ptr(),
            err_cap: err.len(),
        },
        OpenOut {
            head: out_head(),
            instance: std::ptr::null_mut(),
            err_len: 0,
        },
    );
    t.push(format!("open {}", answered(&p.call(lc::OPEN, &mut f))));

    let mut f = Frame::new(
        DeliverIn {
            head: head::<DeliverIn>(),
            op_id: [7; 16],
            stream: 1,
            _reserved: [0; 7],
            batch: blob(b"{\"outcome\":\"ok\",\"ts\":1}\n", BLOB_JSONL),
        },
        out_head(),
    );
    t.push(format!(
        "deliver {}",
        answered(&p.call(slot::DELIVER, &mut f))
    ));

    let mut buf = vec![0_u8; 64];
    let mut f = Frame::new(
        ScrapeIn {
            head: head::<ScrapeIn>(),
            families: std::ptr::null(),
            families_len: 0,
            buf: buf.as_mut_ptr(),
            cap: buf.len(),
        },
        ScrapeOut {
            head: out_head(),
            written: 0,
            needed: 0,
        },
    );
    let c = p.call(slot::SCRAPE, &mut f);
    t.push(format!("scrape {} written={}", answered(&c), f.out.written));

    let mut f = Frame::new(
        in_head(),
        StatusOut {
            head: out_head(),
            status: NO_BLOB,
        },
    );
    let c = p.call(slot::STATUS, &mut f);
    t.push(format!("status {} len={}", answered(&c), f.out.status.len));

    let mut f = Frame::new(
        CheckIn {
            head: head::<CheckIn>(),
            phase: CHECK_PHASE_INSTANCES,
            _reserved: 0,
            instances: std::ptr::null(),
            instances_len: 0,
        },
        CheckOut {
            head: out_head(),
            findings: NO_BLOB,
        },
    );
    let c = p.call(slot::CHECK, &mut f);
    t.push(format!("check {} len={}", answered(&c), f.out.findings.len));

    let method = b"GET";
    let path = b"/exports/tail/x";
    let mut f = Frame::new(
        ServeIn {
            head: head::<ServeIn>(),
            method: AbiStr {
                ptr: method.as_ptr(),
                len: method.len(),
            },
            path: AbiStr {
                ptr: path.as_ptr(),
                len: path.len(),
            },
            query: AbiStr {
                ptr: std::ptr::null(),
                len: 0,
            },
            headers: std::ptr::null(),
            headers_len: 0,
            body: NO_BLOB,
        },
        ServeOut {
            head: out_head(),
            status_code: 0,
            _reserved: [0; 6],
            headers_out: std::ptr::null(),
            headers_out_len: 0,
            body: NO_BLOB,
        },
    );
    let c = p.call(slot::SERVE, &mut f);
    t.push(format!("serve {} code={}", answered(&c), f.out.status_code));

    let mut f: Frame<InHead, OutHead> = Frame::new(in_head(), out_head());
    t.push(format!("close {}", answered(&p.call(lc::CLOSE, &mut f))));

    t.extend(rec.0.lock().unwrap().drain(..));
    t
}

const SETTINGS: &[u8] = br#"{"path":"/var/log/requests.jsonl","rotate_mb":1}"#;

/// The file sink loads as ONE door both ways and answers alike, byte for byte.
#[test]
fn the_linked_and_the_dropped_in_file_sink_are_one_sink() {
    let d = Dispatcher::new(DispatchConfig::default());
    let row = LinkedRow::of(busbar_export_file::door).expect("the door states itself");

    // What the packer signs into the manifest is the linked row's Statement, byte for byte.
    let packed = rendering_of_library(&cdylib()).expect("the cdylib loads");
    assert_eq!(packed.as_deref(), Some(&row.statement[..]));

    let (lr, dr) = (Arc::new(Recorder::default()), Arc::new(Recorder::default()));
    let linked: Plugin<Export> = load_linked(&row, bind(&d, lr.clone())).expect("linked loads");
    let dropped: Plugin<Export> =
        load_dropped(&cdylib(), &row.statement, bind(&d, dr.clone())).expect("dropped loads");
    assert_eq!(linked.name(), "busbar-export-file");
    assert_eq!(dropped.name(), linked.name());

    let a = transcript(&linked, &lr, SETTINGS);
    let b = transcript(&dropped, &dr, SETTINGS);
    assert_eq!(a, b, "the two doors are not one sink");

    // What the script did: the settings refusals in the grammar's words, a delivery on no ticket
    // (it cannot pend on the host's disk lane) dropped under the open-failed code, a push sink's
    // empty answers.
    let joined = a.join("\n");
    for want in [
        "validate Ready \"\"",
        "settings: missing field `path`",
        "settings: unknown field `rotate`, expected `path` or `rotate_mb`",
        "settings: invalid type: string \\\"one\\\", expected u64",
        "open Ready",
        "deliver Ready",
        "scrape Ready \"\" lease=0 written=0",
        "status Ready \"\" lease=0 len=0",
        "check Ready \"\" lease=0 len=0",
        "serve Ready \"\" lease=0 code=404",
        "close Ready",
        "request-log file open failed; this log was dropped",
        "BUSBAR-7074",
    ] {
        assert!(joined.contains(want), "missing {want:?} in:\n{joined}");
    }

    // RED: the same door over settings that do not parse raises nothing on delivery.
    let rr = Arc::new(Recorder::default());
    let other: Plugin<Export> =
        load_dropped(&cdylib(), &row.statement, bind(&d, rr.clone())).expect("dropped loads");
    let c = transcript(&other, &rr, b"{}");
    assert_ne!(
        c, a,
        "an unconfigured sink must not answer as a configured one"
    );
    assert!(!c.join("\n").contains("BUSBAR-7074"));
}

/// The door is refused as another kind, and against a Statement that is not its own.
#[test]
fn the_door_is_refused_as_another_kind_or_statement() {
    let d = Dispatcher::new(DispatchConfig::default());
    let row = LinkedRow::of(busbar_export_file::door).expect("the door states itself");
    let rec = Arc::new(Recorder::default());
    assert!(load_linked::<Hook>(&row, bind(&d, rec.clone())).is_err());
    assert!(load_dropped::<Hook>(&cdylib(), &row.statement, bind(&d, rec.clone())).is_err());

    let mut other = row.statement.clone();
    let last = other.len() - 1;
    other[last] ^= 1;
    let refused = load_dropped::<Export>(&cdylib(), &other, bind(&d, rec));
    assert!(matches!(refused, Err(LoadError::StatementMismatch)));
}
