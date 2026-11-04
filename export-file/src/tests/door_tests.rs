// SPDX-License-Identifier: Apache-2.0
//! Concrete ABI crossings of the file sink; no test-kit runtime.
use super::*;
use crate::door::{REWRITES, STREAMS, TAIL};
use crate::tests::{report, TICKET};
use busbar_contract::abi::export::{
    self, CheckIn, CheckOut, DeliverIn, ExportStream, Ops, ServeIn, ServeOut, Tail,
    CHECK_PHASE_INSTANCES, CHECK_PHASE_LIMITS,
};
use busbar_contract::abi::host::service::{
    DiskAppendIn, HostSlots, ServiceOut, DISK_APPEND_FAILED, SERVICES,
};
use busbar_contract::abi::mechanism::call::{
    Blob, InHead, OutHead, Outcome, RawOutcome, BLOB_JSON, FLAG_RESUME,
};
use busbar_contract::abi::mechanism::door::{Door, REWRITE_ALIAS};
use busbar_contract::abi::mechanism::lifecycle::{slot, OpenIn, OpenOut, RefreshIn};
use busbar_contract::abi::mechanism::ticket::{HostCtx, HostTables};
use busbar_contract::abi::sdk::door::{blank_in, blank_out};
use std::cell::RefCell;
use std::ffi::c_void;

/// The Statement: the plugin's name and version, 1.5.5's in-flight bound, the module name as its
/// alias, the `logs` stream and no route (a push-only sink).
#[test]
fn the_statement_states_the_sink() {
    // SAFETY: the statement is static program data published by the linked door.
    let statement = unsafe { &*Door::read_parts(door()).statement };
    assert_eq!(
        unsafe { std::slice::from_raw_parts(statement.name.ptr, statement.name.len) },
        NAME.as_bytes()
    );
    assert_eq!(NAME, "busbar-export-file");
    assert_eq!(STATEMENT.version.len, env!("CARGO_PKG_VERSION").len());

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

thread_local! {
    static APPENDS: RefCell<Vec<(String, Vec<u8>)>> = const { RefCell::new(Vec::new()) };
    static NEXT: RefCell<Next> = const { RefCell::new(Next::Land) };
}
#[derive(Clone, Copy)]
enum Next {
    Land,
    Pend,
    Fail,
}
const BATCH: &[u8] = b"{\"a\":1}\n{\"b\":2}\n";
extern "C" fn append(_: HostCtx, input: *const c_void, output: *mut ServiceOut) -> RawOutcome {
    // SAFETY: the SDK lends this typed input and writable output during the callback.
    let (input, output) = unsafe { (&*input.cast::<DiskAppendIn>(), &mut *output) };
    // SAFETY: these byte loans remain live for the synchronous service callback.
    let (key, bytes) = unsafe {
        (
            std::str::from_utf8(std::slice::from_raw_parts(
                input.dest_key.ptr,
                input.dest_key.len,
            ))
            .unwrap()
            .to_owned(),
            std::slice::from_raw_parts(input.bytes.ptr, input.bytes.len).to_vec(),
        )
    };
    let len = bytes.len() as u64;
    APPENDS.with(|a| a.borrow_mut().push((key, bytes)));
    let outcome = match NEXT.with(|n| *n.borrow()) {
        Next::Pend => Outcome::Pending,
        Next::Land => {
            unsafe {
                input.result.write(report(0, 0, len));
            }
            Outcome::Ready
        }
        Next::Fail => {
            unsafe {
                input.result.write(report(0, 0, 0));
            }
            output.value = DISK_APPEND_FAILED;
            Outcome::Failed
        }
    };
    output.outcome = RawOutcome::of(outcome);
    output.outcome
}
fn ops() -> &'static Ops {
    // SAFETY: the linked door's export table is static program data.
    unsafe { &*Door::read_parts(door()).ops.cast::<Ops>() }
}
fn blob(bytes: &[u8]) -> Blob {
    Blob {
        ptr: bytes.as_ptr(),
        len: bytes.len(),
        fmt: BLOB_JSON,
        flags: 0,
    }
}
fn head() -> OutHead {
    let mut out: OutHead = blank_out();
    out.size = size_of::<OutHead>() as u32;
    out
}
fn open(host: *const HostTables) -> *mut c_void {
    let mut input: OpenIn = blank_in();
    input.head.op = slot::OPEN;
    input.head.size = size_of::<OpenIn>() as u32;
    input.host = host;
    input.settings = blob(br#"{"path":"/old.jsonl"}"#);
    input.generation = 1;
    let mut out: OpenOut = blank_out();
    out.head.size = size_of::<OpenOut>() as u32;
    assert_eq!(
        // SAFETY: the actual door receives live, correctly sized OpenIn/OpenOut records.
        unsafe {
            ops().head.open.unwrap()(
                std::ptr::null_mut(),
                (&input as *const OpenIn).cast(),
                (&mut out as *mut OpenOut).cast(),
            )
        }
        .outcome(),
        Outcome::Ready
    );
    assert!(!out.instance.is_null());
    out.instance
}
fn close(instance: *mut c_void) {
    let mut input: InHead = blank_in();
    input.op = slot::CLOSE;
    input.size = size_of::<InHead>() as u32;
    let mut out = head();
    assert_eq!(
        // SAFETY: this instance came from the actual door; both typed frames remain live.
        unsafe {
            ops().head.close.unwrap()(
                instance,
                (&input as *const InHead).cast(),
                (&mut out as *mut OutHead).cast(),
            )
        }
        .outcome(),
        Outcome::Ready
    );
}
#[test]
fn the_deliver_op_appends_the_batch_under_the_path_key_through_the_door() {
    APPENDS.with(|a| a.borrow_mut().clear());
    // SAFETY: HostSlots consists of scalars and nullable function pointers.
    let mut services: HostSlots = unsafe { std::mem::zeroed() };
    services.size = size_of::<HostSlots>() as u32;
    services.slots = SERVICES;
    services.disk_append = Some(append);
    let host = HostTables {
        size: size_of::<HostTables>() as u32,
        _reserved: 0,
        ctx: HostCtx::NONE,
        wake: None,
        conns: std::ptr::null(),
        services: &services,
        io: std::ptr::null(),
    };
    let instance = open(&host);
    let mut input: DeliverIn = blank_in();
    input.head.op = export::slot::DELIVER;
    input.head.size = size_of::<DeliverIn>() as u32;
    input.head.ticket = TICKET;
    input.batch = blob(BATCH);
    let deliver = |input: &DeliverIn| {
        let mut out = head();
        // SAFETY: the live instance and batch loan outlast this synchronous typed crossing.
        unsafe {
            ops().deliver.unwrap()(
                instance,
                (input as *const DeliverIn).cast(),
                (&mut out as *mut OutHead).cast(),
            )
        }
        .outcome()
    };
    NEXT.with(|n| *n.borrow_mut() = Next::Land);
    assert_eq!(deliver(&input), Outcome::Ready);
    assert_eq!(
        APPENDS.with(|a| a.borrow().clone()),
        [("path".to_string(), BATCH.to_vec())]
    );
    NEXT.with(|n| *n.borrow_mut() = Next::Pend);
    assert_eq!(deliver(&input), Outcome::Pending);
    let mut refresh: RefreshIn = blank_in();
    refresh.head.op = slot::REFRESH;
    refresh.head.size = size_of::<RefreshIn>() as u32;
    refresh.settings = blob(br#"{"path":"/new.jsonl"}"#);
    let mut out = head();
    assert_eq!(
        // SAFETY: the live instance receives a correctly sized refresh and output frame.
        unsafe {
            ops().head.refresh.unwrap()(
                instance,
                (&refresh as *const RefreshIn).cast(),
                (&mut out as *mut OutHead).cast(),
            )
        }
        .outcome(),
        Outcome::Ready
    );
    NEXT.with(|n| *n.borrow_mut() = Next::Fail);
    input.head.flags = FLAG_RESUME;
    assert_eq!(deliver(&input), Outcome::Ready);
    APPENDS.with(|a| {
        let a = a.borrow();
        assert!(a.iter().all(|(k, b)| k == DESTINATION && b == BATCH));
        assert_eq!(a.len(), 3);
    });
    close(instance);
}
#[test]
fn an_unopened_instance_is_refused_naming_its_cause_through_the_door() {
    let mut input: DeliverIn = blank_in();
    input.head.op = export::slot::DELIVER;
    input.head.size = size_of::<DeliverIn>() as u32;
    input.head.ticket = TICKET;
    input.batch = blob(BATCH);
    let mut out = head();
    assert_eq!(
        // SAFETY: no instance was opened; the typed frames and batch loan are live for the call.
        unsafe {
            ops().deliver.unwrap()(
                std::ptr::null_mut(),
                (&input as *const DeliverIn).cast(),
                (&mut out as *mut OutHead).cast(),
            )
        }
        .outcome(),
        Outcome::Refused
    );
    // SAFETY: the error loan is read before the next crossing invalidates call capture.
    let error = unsafe { std::slice::from_raw_parts(out.error.ptr, out.error.len) };
    assert_eq!(error, NO_INSTANCE.as_bytes());

    let mut input: CheckIn = blank_in();
    input.head.op = export::slot::CHECK;
    input.head.size = size_of::<CheckIn>() as u32;
    input.phase = CHECK_PHASE_LIMITS;
    let mut out: CheckOut = blank_out();
    out.head.size = size_of::<CheckOut>() as u32;
    assert_eq!(
        // SAFETY: no instance was opened; correctly sized CheckIn/CheckOut records are live.
        unsafe {
            ops().check.unwrap()(
                std::ptr::null_mut(),
                (&input as *const CheckIn).cast(),
                (&mut out as *mut CheckOut).cast(),
            )
        }
        .outcome(),
        Outcome::Refused
    );
    // SAFETY: the error loan is read before the next crossing invalidates call capture.
    let error = unsafe { std::slice::from_raw_parts(out.head.error.ptr, out.head.error.len) };
    assert_eq!(error, NO_INSTANCE.as_bytes());
}
#[test]
fn serve_is_refused_naming_no_route_through_the_door() {
    let instance = open(std::ptr::null());
    let mut input: ServeIn = blank_in();
    input.head.op = export::slot::SERVE;
    input.head.size = size_of::<ServeIn>() as u32;
    let mut out: ServeOut = blank_out();
    out.head.size = size_of::<ServeOut>() as u32;
    assert_eq!(
        // SAFETY: the live instance receives correctly sized ServeIn/ServeOut records.
        unsafe {
            ops().serve.unwrap()(
                instance,
                (&input as *const ServeIn).cast(),
                (&mut out as *mut ServeOut).cast(),
            )
        }
        .outcome(),
        Outcome::Refused
    );
    // SAFETY: the error loan is read before the next crossing invalidates call capture.
    let error = unsafe { std::slice::from_raw_parts(out.head.error.ptr, out.head.error.len) };
    assert_eq!(
        error,
        busbar_contract::abi::sdk::export::NO_ROUTE.as_bytes()
    );
    close(instance);
}
#[test]
fn check_refuses_a_phase_it_does_not_know_through_the_door() {
    let instance = open(std::ptr::null());
    for phase in [CHECK_PHASE_LIMITS, CHECK_PHASE_INSTANCES, 7] {
        let mut input: CheckIn = blank_in();
        input.head.op = export::slot::CHECK;
        input.head.size = size_of::<CheckIn>() as u32;
        input.phase = phase;
        let mut out: CheckOut = blank_out();
        out.head.size = size_of::<CheckOut>() as u32;
        // SAFETY: the live instance receives correctly sized CheckIn/CheckOut records.
        let outcome = unsafe {
            ops().check.unwrap()(
                instance,
                (&input as *const CheckIn).cast(),
                (&mut out as *mut CheckOut).cast(),
            )
        }
        .outcome();
        if phase == 7 {
            assert_eq!(outcome, Outcome::Refused);
            // SAFETY: error is read before the next crossing.
            assert_eq!(
                unsafe { std::slice::from_raw_parts(out.head.error.ptr, out.head.error.len) },
                UNKNOWN_PHASE.as_bytes()
            );
        } else {
            assert_eq!(outcome, Outcome::Ready);
            assert_eq!(out.findings.len, 0);
        }
    }
    close(instance);
}
