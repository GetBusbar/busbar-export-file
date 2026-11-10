// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! **A DELIVERY RESUMED AFTER A RELOAD NAMES THE FILE ITS APPEND WAS ISSUED AGAINST, THROUGH BOTH
//! DOORS.** Driven over the real loader: the host's disk lane holds the first `disk.append` (the
//! delivery PENDS and parks its (path, keep) pair), the instance is refreshed onto another path,
//! then the lane answers the held append as failed and the loader resumes the delivery. The
//! envelope the host ingests carries the drop line; it must name path A, the real park/resume.

use std::sync::{Arc, Mutex, PoisonError};

use busbar_contract::abi::export::ExportStream;
use busbar_contract::abi::host::service::{DISK_APPEND_FAILED, DISK_RETENTION_FAILED};
use busbar_contract::abi::mechanism::call::{Blob, Outcome, BLOB_JSON};
use busbar_contract::abi::mechanism::lifecycle::{slot as lc, RefreshIn};
use busbar_contract::abi::sdk::door::blank_in;
use busbar_contract::export_calls::{Delivered, ExportCalls};
use busbar_contract::services::{
    Caller, HookAsk, HostServices, NestAsk, Reading, RecordsList, Stored, TrustKeyRef, UNSERVED,
};
use busbar_contract::services::{DiskDest, DiskReport, Later, Ran};
use busbar_plugin_loader::dispatch::kinds::export::Export;
use busbar_plugin_loader::dispatch::{
    in_head, load_dropped, load_linked, out_head, Bind, ConnTable, Diagnostic, DispatchConfig,
    Dispatcher, Dropped, EnvelopeSink, Frame, LinkedRow, Metric, Plugin,
};
use busbar_plugin_loader::export_door::ExportInstance;
use busbar_plugin_loader::sign::Declares;

/// Locates this crate's built cdylib; a missing artifact is a failure, never a skip.
mod support;
use support::cdylib;

/// The host's disk lane: the append is held (the answer pends) until the test answers it.
#[derive(Default)]
struct Held {
    held: Mutex<Vec<(String, Later)>>,
}

impl HostServices for Held {
    fn now(&self) -> Reading {
        Reading {
            wall_ns: 0,
            mono_ns: 0,
        }
    }
    fn dest_judge(&self, _dest: &str, _class: u32, _flags: u32, _later: Option<Later>) -> Ran {
        Ran::Now(Stored::refused(UNSERVED))
    }
    fn records_get(&self, _caller: &Caller, _kind: &str, _key: &[u8], _later: Later) -> Ran {
        Ran::Now(Stored::refused(UNSERVED))
    }
    fn records_list(&self, _caller: &Caller, _list: RecordsList, _later: Later) -> Ran {
        Ran::Now(Stored::refused(UNSERVED))
    }
    fn records_claim(
        &self,
        _caller: &Caller,
        _kind: &str,
        _key: &[u8],
        _ttl_ms: u64,
        _later: Later,
    ) -> Ran {
        Ran::Now(Stored::refused(UNSERVED))
    }
    fn sign(&self, _caller: &Caller, _data: &[u8]) -> Stored {
        Stored::refused(UNSERVED)
    }
    fn trust_sight(
        &self,
        _caller: &Caller,
        _counterparty: &str,
        _hash: &str,
        _later: Later,
    ) -> Ran {
        Ran::Now(Stored::refused(UNSERVED))
    }
    fn trust_unreached(&self, _caller: &Caller, _counterparty: &str) -> Stored {
        Stored::refused(UNSERVED)
    }
    fn trust_sight_item(
        &self,
        _caller: &Caller,
        _counterparty: &str,
        _item: &str,
        _digest: &str,
        _later: Later,
    ) -> Ran {
        Ran::Now(Stored::refused(UNSERVED))
    }
    fn trust_decide(
        &self,
        _caller: &Caller,
        _ticket: busbar_contract::abi::mechanism::ticket::Ticket,
        _key: TrustKeyRef<'_>,
        _expected: Option<&str>,
        _approve: bool,
        _later: Later,
    ) -> Ran {
        Ran::Now(Stored::refused(UNSERVED))
    }
    fn trust_state(&self, _caller: &Caller, _counterparty: &str) -> Stored {
        Stored::refused(UNSERVED)
    }
    fn trust_serves(
        &self,
        _caller: &Caller,
        _counterparty: &str,
        _item: Option<&str>,
        _digest: Option<&str>,
    ) -> Stored {
        Stored::refused(UNSERVED)
    }
    fn trust_due(&self, _caller: &Caller) -> Stored {
        Stored::refused(UNSERVED)
    }
    fn trust_verify(
        &self,
        _caller: &Caller,
        _counterparty: &str,
        _payload: &[u8],
        _signatures: &[u8],
    ) -> Stored {
        Stored::refused(UNSERVED)
    }
    fn entitlement_check(&self, _caller: &Caller, _unit: Option<u64>, _target: &str) -> Stored {
        Stored::refused(UNSERVED)
    }
    fn session_emit(&self, _caller: &Caller, _session: u64, _bytes: &[u8]) -> Stored {
        Stored::refused(UNSERVED)
    }
    fn ticket_mint(
        &self,
        _caller: &Caller,
        _unit: Option<u64>,
        _ttl_secs: u64,
        _record: &[u8],
        _later: Later,
    ) -> Ran {
        Ran::Now(Stored::refused(UNSERVED))
    }
    fn ticket_take(&self, _caller: &Caller, _unit: Option<u64>) -> Stored {
        Stored::refused(UNSERVED)
    }
    fn random_fill(&self, _len: u64) -> Stored {
        Stored::refused(UNSERVED)
    }
    fn records_secret(&self, _kind: &str, _id: &str, _later: Later) -> Ran {
        Ran::Now(Stored::refused(UNSERVED))
    }
    fn unit_nest(&self, _caller: &Caller, _unit: Option<u64>, _ask: NestAsk, _later: Later) -> Ran {
        Ran::Now(Stored::refused(UNSERVED))
    }
    fn work_open(
        &self,
        _caller: &Caller,
        _unit: Option<u64>,
        _kind: &str,
        _record: &[u8],
        _later: Later,
    ) -> Ran {
        Ran::Now(Stored::refused(UNSERVED))
    }
    fn work_find(
        &self,
        _caller: &Caller,
        _unit: Option<u64>,
        _reference: &[u8],
        _later: Later,
    ) -> Ran {
        Ran::Now(Stored::refused(UNSERVED))
    }
    fn work_settle(
        &self,
        _caller: &Caller,
        _unit: Option<u64>,
        _handle: u64,
        _record: &[u8],
        _later: Later,
    ) -> Ran {
        Ran::Now(Stored::refused(UNSERVED))
    }
    fn work_resume(
        &self,
        _caller: &Caller,
        _unit: Option<u64>,
        _handle: u64,
        _later: Later,
    ) -> Ran {
        Ran::Now(Stored::refused(UNSERVED))
    }
    fn verify_lookup(&self, _caller: &Caller, _key: &[u8], _later: Later) -> Ran {
        Ran::Now(Stored::refused(UNSERVED))
    }
    fn verify_store(&self, _caller: &Caller, _key: &[u8], _entry: &[u8], _ttl_ms: u64) -> Stored {
        Stored::refused(UNSERVED)
    }
    fn content_scan(
        &self,
        _caller: &Caller,
        _unit: Option<u64>,
        _content: &[u8],
        _later: Later,
    ) -> Ran {
        Ran::Now(Stored::refused(UNSERVED))
    }
    fn hook_call(&self, _caller: &Caller, _unit: Option<u64>, _ask: HookAsk, _later: Later) -> Ran {
        Ran::Now(Stored::refused(UNSERVED))
    }
    fn snapshot_read(&self, _caller: &Caller, _scope: u32, _later: Later) -> Ran {
        Ran::Now(Stored::refused(UNSERVED))
    }
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
        busbar_contract::services::test_caller_binding(),
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
    let disk = busbar_contract::services::disk_admission::DiskAuthority::issue(
        &busbar_contract::caps::KernelSeal::acquire_for_kernel(),
    );
    plugin.grant_destinations(&disk, &declares);
    let sink = ExportInstance::open(plugin, d, br#"{"path":"/a.jsonl","keep":3}"#)
        .expect("the sink opens");

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

    // The reload took effect through the loader: a delivery issued now targets path B.
    let second = sink.deliver(
        ExportStream::Logs as u8,
        b"{\"n\":2}\n".to_vec(),
        Box::new(()),
    );
    assert!(matches!(second, Delivered::Queued), "{second:?}");
    until("the second append reached the disk lane", || {
        lane.held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
            == 2
    });
    {
        let held = lane.held.lock().unwrap_or_else(PoisonError::into_inner);
        assert_eq!(held[0].0, "/a.jsonl", "dropped in: {dropped}");
        assert_eq!(held[1].0, "/b.jsonl", "dropped in: {dropped}");
    }

    // The lane answers the second append as failed (no rotation faults): its drop line is the one
    // line allowed to name path B, because it was issued after the reload.
    let (_, second_later) = lane
        .held
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(1);
    second_later(
        DiskReport {
            step: DISK_APPEND_FAILED,
            rotated: false,
            faults: 0,
            error: "No space left on device (os error 28)",
        }
        .stored(),
    );
    until("the second drop line reached the host", || {
        tape.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .any(|line| line.contains("path=/b.jsonl"))
    });

    // Then the first, failed with a retention fault; the loader resumes the first delivery.
    let (_, later) = lane
        .held
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(0);
    later(
        DiskReport {
            step: DISK_APPEND_FAILED,
            rotated: false,
            faults: DISK_RETENTION_FAILED,
            error: "No space left on device (os error 28)",
        }
        .stored(),
    );
    until("the first delivery's drop line reached the host", || {
        tape.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .any(|l| l.contains("request-log file append failed") && l.contains("path=/a.jsonl"))
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
    let named_b: Vec<&String> = seen.iter().filter(|l| l.contains("/b.jsonl")).collect();
    assert!(
        named_b.len() == 1 && named_b[0].contains("request-log file append failed"),
        "dropped in: {dropped}: only the post-reload delivery's drop line may name path B: {seen:?}"
    );
    assert!(
        seen.iter()
            .any(|l| l.contains("BUSBAR-7075") && l.contains("archive=/a.jsonl.3")),
        "dropped in: {dropped}: retention must name the issued-at keep: {seen:?}"
    );
}

#[test]
fn a_resumed_delivery_names_the_path_it_was_issued_against_through_both_doors() {
    run(false);
    run(true);
}
