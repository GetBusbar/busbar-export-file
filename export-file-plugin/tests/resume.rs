// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! **A DELIVERY RESUMED AFTER A RELOAD NAMES THE FILE ITS APPEND WAS ISSUED AGAINST, THROUGH BOTH
//! DOORS.** Driven over the real loader: the host's disk lane holds the first `disk.append` (the
//! delivery PENDS and parks its (path, keep) pair), the instance is refreshed onto another path,
//! then the lane answers the held append as failed and the loader resumes the delivery. The
//! envelope the host ingests carries the drop line; it must name path A, the real park/resume.
//! The sink's own tests cannot see this: the test kit clears the lines a call raises.

use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};

use busbar_contract::abi::export::ExportStream;
use busbar_contract::abi::host::service::DISK_APPEND_FAILED;
use busbar_contract::abi::mechanism::call::{Blob, Outcome, BLOB_JSON};
use busbar_contract::abi::mechanism::lifecycle::{slot as lc, RefreshIn};
use busbar_contract::abi::sdk::door::blank_in;
use busbar_contract::export_calls::{Delivered, ExportCalls};
use busbar_contract::services::double::ServicesDouble;
use busbar_contract::services::{DiskDest, DiskReport, Later, Ran};
use busbar_plugin_loader::dispatch::kinds::export::Export;
use busbar_plugin_loader::dispatch::{
    in_head, load_dropped, load_linked, out_head, Bind, ConnTable, Diagnostic, DispatchConfig,
    Dispatcher, Dropped, EnvelopeSink, Frame, LinkedRow, Metric, Plugin,
};
use busbar_plugin_loader::export_door::ExportInstance;
use busbar_plugin_loader::sign::Declares;

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
        .unwrap_or_else(|| panic!("the busbar_export_file_plugin cdylib ({file}) is not built"))
}

/// The host's disk lane: the append is held (the answer pends) until the test answers it.
#[derive(Default)]
struct Held {
    held: Mutex<Vec<(String, Later)>>,
}

impl ServicesDouble for Held {
    fn disk_append(&self, dest: &DiskDest, _: Vec<u8>, later: Later) -> Ran {
        self.held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((dest.path.clone(), later));
        Ran::Later
    }
}

/// The host's envelope sink, recorded as text.
#[derive(Default)]
struct Tape(Mutex<Vec<String>>);

impl EnvelopeSink for Tape {
    fn metric(&self, _: Metric<'_>) {}
    fn diag(&self, d: Diagnostic<'_>) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(format!("{} {}", d.id, String::from_utf8_lossy(d.text)));
    }
    fn dropped(&self, _: Dropped) {}
}

fn until(what: &str, done: impl Fn() -> bool) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while !done() {
        assert!(std::time::Instant::now() < deadline, "timed out: {what}");
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

fn run(dropped: bool) {
    let lane = Arc::new(Held::default());
    let tape = Arc::new(Tape::default());
    let d = Arc::new(Dispatcher::with_services(
        DispatchConfig::default(),
        lane.clone(),
    ));
    let row = LinkedRow::of(busbar_export_file::door).expect("the linked door renders");
    let bind = Bind {
        instance: Arc::from("export.resume"),
        max_inflight_cap: 64,
        sink: tape.clone(),
        dispatcher: d.adopter(),
        conns: ConnTable::Probe,
    };
    let plugin: Plugin<Export> = if dropped {
        load_dropped::<Export>(&cdylib(), &row.statement, bind).expect("the dropped door loads")
    } else {
        load_linked::<Export>(&row, bind).expect("the linked door loads")
    };
    let declares = Declares::from_declares_json(busbar_export_file::DECLARES.as_bytes())
        .expect("the declaration parses");
    plugin.grant_destinations(&declares);
    let sink = ExportInstance::open(plugin, d, br#"{"path":"/a.jsonl","keep":3}"#)
        .expect("the sink opens");

    // The delivery pends in the host's disk lane, parked at path A.
    let queued = sink.deliver(
        ExportStream::Logs as u8,
        b"{\"n\":1}\n".to_vec(),
        Box::new(()),
    );
    assert!(matches!(queued, Delivered::Queued), "{queued:?}");
    until("the append reached the disk lane", || {
        !lane
            .held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
    });

    // A reload lands while it pends: the settings now name path B.
    let settings = br#"{"path":"/b.jsonl"}"#;
    let mut input: RefreshIn = blank_in();
    input.head = in_head();
    input.generation = 2;
    input.settings = Blob {
        ptr: settings.as_ptr(),
        len: settings.len(),
        fmt: BLOB_JSON,
        flags: 0,
    };
    let out = out_head();
    let called = sink.plugin().call(lc::REFRESH, &mut Frame::new(input, out));
    assert_eq!(called.outcome, Outcome::Ready);

    // The lane answers the held append as failed; the loader resumes the delivery.
    let (_, later) = lane
        .held
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(0);
    later(
        DiskReport {
            step: DISK_APPEND_FAILED,
            rotated: false,
            faults: 0,
            error: "No space left on device (os error 28)",
        }
        .stored(),
    );
    until("the drop line reached the host", || {
        !tape
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
    });
    drop(sink);
    let seen = tape
        .0
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    assert!(
        seen.iter()
            .any(|l| l.contains("request-log file append failed") && l.contains("path=/a.jsonl")),
        "dropped in: {dropped}: the drop line must name path A: {seen:?}"
    );
    assert!(
        seen.iter().all(|l| !l.contains("/b.jsonl")),
        "dropped in: {dropped}: {seen:?}"
    );
}

#[test]
fn a_resumed_delivery_names_the_path_it_was_issued_against_through_both_doors() {
    run(false);
    run(true);
}
