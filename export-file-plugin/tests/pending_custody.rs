// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! Drive the actual linked door across repeated Pending, terminal failure/success, cancellation
//! and instance close. The host owns the lent batch throughout every crossing; the plugin must
//! pass that same initialized range through without materializing a private payload copy. The
//! actual reply diagnostics must name the path captured before a refresh.

use std::cell::Cell;
use std::ffi::c_void;
use std::mem::size_of;

use busbar_contract::abi::export::{self, DeliverIn, Ops};
use busbar_contract::abi::host::service::{
    DiskAppendIn, DiskWritten, HostSlots, ServiceOut, DISK_APPEND_FAILED, DISK_ROTATED, SERVICES,
};
use busbar_contract::abi::mechanism::call::{
    Blob, InHead, OutHead, Outcome, RawOutcome, BLOB_JSON, BLOB_JSONL, FLAG_RESUME,
};
use busbar_contract::abi::mechanism::lifecycle::{
    slot, CancelIn, CancelOut, OpenIn, OpenOut, RefreshIn,
};
use busbar_contract::abi::mechanism::ticket::{HostCtx, HostTables, Ticket};
use busbar_contract::abi::sdk::door::{blank_in, blank_out};

const PAYLOAD: &[u8] = b"private-file-continuation-canary\n";
/// The batch content as text: it is data passed through, never a word of a diagnostic.
const CANARY: &str = "private-file-continuation-canary";
const TICKET: Ticket = Ticket {
    slot: 1,
    generation: 1,
};
thread_local! {
    static CALLS: Cell<u32> = const { Cell::new(0) };
    static FAIL: Cell<bool> = const { Cell::new(false) };
    static BORROWED: Cell<bool> = const { Cell::new(true) };
    static EXPECTED_PTR: Cell<usize> = const { Cell::new(0) };
}

extern "C" fn append(_: HostCtx, input: *const c_void, out: *mut ServiceOut) -> RawOutcome {
    // SAFETY: the SDK lends the checked disk input, initialized byte loan and writable answer.
    let input = unsafe { &*input.cast::<DiskAppendIn>() };
    let out = unsafe { &mut *out };
    let calls = CALLS.with(|n| {
        let next = n.get() + 1;
        n.set(next);
        next
    });
    BORROWED.with(|borrowed| {
        borrowed.set(
            borrowed.get()
                && input.bytes.ptr as usize == EXPECTED_PTR.with(Cell::get)
                && input.bytes.len == PAYLOAD.len(),
        );
    });
    let outcome = if calls <= 2 {
        Outcome::Pending
    } else {
        // SAFETY: the SDK lends a writable result slot for this synchronous callback.
        unsafe {
            input.result.write(DiskWritten {
                size: size_of::<DiskWritten>() as u32,
                rotated: if FAIL.with(Cell::get) {
                    0
                } else {
                    DISK_ROTATED
                },
                faults: 0,
                _reserved: [0; 2],
                written: if FAIL.with(Cell::get) {
                    0
                } else {
                    input.bytes.len as u64
                },
            })
        };
        if FAIL.with(Cell::get) {
            out.value = DISK_APPEND_FAILED;
            Outcome::Failed
        } else {
            Outcome::Ready
        }
    };
    out.outcome = RawOutcome::of(outcome);
    out.outcome
}

fn ops() -> &'static Ops {
    // SAFETY: this linked door and its export op table are static plugin program data.
    unsafe { &*busbar_export_file::door().parts().ops.cast::<Ops>() }
}

fn blob(bytes: &[u8], fmt: u32) -> Blob {
    Blob {
        ptr: bytes.as_ptr(),
        len: bytes.len(),
        fmt,
        flags: 0,
    }
}

/// Copy only the live diagnostic strings lent by this completed crossing.
fn diagnostics(output: &OutHead) -> Vec<String> {
    if output.envelope.diags_len == 0 {
        return Vec::new();
    }
    // SAFETY: Safe's call capture owns these diagnostics through the next crossing on this
    // ticket. The caller consumes them immediately after deliver returns.
    unsafe {
        std::slice::from_raw_parts(output.envelope.diags, output.envelope.diags_len)
            .iter()
            .map(|diag| {
                std::str::from_utf8(std::slice::from_raw_parts(diag.text.ptr, diag.text.len))
                    .expect("UTF-8 diagnostic")
                    .to_owned()
            })
            .collect()
    }
}

#[test]
fn pending_delivery_borrows_its_batch_and_keeps_its_original_destination() {
    for end in [0, 1, 2, 3] {
        CALLS.with(|n| n.set(0));
        FAIL.with(|n| n.set(end == 1));
        BORROWED.with(|n| n.set(true));
        // SAFETY: these host tables consist solely of scalar fields and nullable function pointers.
        let mut services: HostSlots = unsafe { std::mem::zeroed() };
        services.size = size_of::<HostSlots>() as u32;
        services.slots = SERVICES;
        services.disk_append = Some(append);
        let tables = HostTables {
            size: size_of::<HostTables>() as u32,
            _reserved: 0,
            ctx: HostCtx::NONE,
            wake: None,
            conns: std::ptr::null(),
            services: &services,
            io: std::ptr::null(),
        };
        let mut input: OpenIn = blank_in();
        input.head.op = slot::OPEN;
        input.head.size = size_of::<OpenIn>() as u32;
        input.host = &tables;
        input.settings = blob(br#"{"path":"/original"}"#, BLOB_JSON);
        input.generation = 1;
        let mut opened: OpenOut = blank_out();
        opened.head.size = size_of::<OpenOut>() as u32;
        let table = ops();
        assert_eq!(
            // SAFETY: the actual OPEN slot receives live, correctly sized frames and host loans.
            unsafe {
                table.head.open.unwrap()(
                    std::ptr::null_mut(),
                    (&input as *const OpenIn).cast(),
                    (&mut opened as *mut OpenOut).cast(),
                )
            }
            .outcome(),
            Outcome::Ready
        );
        let instance = opened.instance;
        let mut delivery: DeliverIn = blank_in();
        delivery.head.op = export::slot::DELIVER;
        delivery.head.size = size_of::<DeliverIn>() as u32;
        delivery.head.ticket = TICKET;
        delivery.batch = blob(PAYLOAD, BLOB_JSONL);

        EXPECTED_PTR.with(|ptr| ptr.set(delivery.batch.ptr as usize));
        delivery.stream = export::ExportStream::Logs as u8;
        let mut output: OutHead = blank_out();
        output.size = size_of::<OutHead>() as u32;
        assert_eq!(
            // SAFETY: the instance is live and the typed frames and original batch remain lent.
            unsafe {
                table.deliver.unwrap()(
                    instance,
                    (&delivery as *const DeliverIn).cast(),
                    (&mut output as *mut OutHead).cast(),
                )
            }
            .outcome(),
            Outcome::Pending
        );
        delivery.head.flags |= FLAG_RESUME;
        assert_eq!(
            // SAFETY: this resumes the same live ticket with the same still-borrowed frames.
            unsafe {
                table.deliver.unwrap()(
                    instance,
                    (&delivery as *const DeliverIn).cast(),
                    (&mut output as *mut OutHead).cast(),
                )
            }
            .outcome(),
            Outcome::Pending
        );

        let mut refresh: RefreshIn = blank_in();
        refresh.head.op = slot::REFRESH;
        refresh.head.size = size_of::<RefreshIn>() as u32;
        refresh.settings = blob(br#"{"path":"/refreshed"}"#, BLOB_JSON);
        refresh.generation = 2;
        let mut refreshed: OutHead = blank_out();
        refreshed.size = size_of::<OutHead>() as u32;
        assert_eq!(
            // SAFETY: the live instance receives initialized, exact-sized refresh/output frames.
            unsafe {
                table.head.refresh.unwrap()(
                    instance,
                    (&refresh as *const RefreshIn).cast(),
                    (&mut refreshed as *mut OutHead).cast(),
                )
            }
            .outcome(),
            Outcome::Ready
        );
        if end == 2 {
            let mut cancel: CancelIn = blank_in();
            cancel.head.op = slot::CANCEL;
            cancel.head.size = size_of::<CancelIn>() as u32;
            cancel.ticket = TICKET;
            let mut cancelled: CancelOut = blank_out();
            cancelled.head.size = size_of::<CancelOut>() as u32;
            // Not ABORTED (0) until the plugin writes it.
            cancelled.disposition = u32::MAX;
            assert_eq!(
                // SAFETY: the live instance owns this pending ticket; both typed frames are live.
                unsafe {
                    table.head.cancel.unwrap()(
                        instance,
                        (&cancel as *const CancelIn).cast(),
                        (&mut cancelled as *mut CancelOut).cast(),
                    )
                }
                .outcome(),
                Outcome::Ready
            );
            // A cancelled delivery never landed: the host is told it was aborted.
            assert_eq!(cancelled.disposition, export::cancel::ABORTED);
        } else if end != 3 {
            assert_eq!(
                // SAFETY: the pending ticket and original typed loans remain live for completion.
                unsafe {
                    table.deliver.unwrap()(
                        instance,
                        (&delivery as *const DeliverIn).cast(),
                        (&mut output as *mut OutHead).cast(),
                    )
                }
                .outcome(),
                Outcome::Ready
            );

            let old = diagnostics(&output);
            assert!(old.iter().all(|line| !line.contains(CANARY)), "{old:?}");
            assert!(old.iter().any(|line| line.contains("/original")), "{old:?}");
            assert!(
                old.iter().all(|line| !line.contains("/refreshed")),
                "{old:?}"
            );
            if end == 1 {
                assert!(
                    old.iter().any(|line| line.contains("BUSBAR-7073")),
                    "{old:?}"
                );
            } else {
                assert!(old.iter().any(|line| line.contains("rotated")), "{old:?}");
            }

            delivery.head.ticket.generation += 1;
            delivery.head.flags &= !FLAG_RESUME;
            output = blank_out();
            output.size = size_of::<OutHead>() as u32;
            assert_eq!(
                // SAFETY: the same live instance receives a fresh ticket and live typed frames.
                unsafe {
                    table.deliver.unwrap()(
                        instance,
                        (&delivery as *const DeliverIn).cast(),
                        (&mut output as *mut OutHead).cast(),
                    )
                }
                .outcome(),
                Outcome::Ready
            );
            let fresh = diagnostics(&output);
            assert!(fresh.iter().all(|line| !line.contains(CANARY)), "{fresh:?}");
            assert!(
                fresh.iter().any(|line| line.contains("/refreshed")),
                "{fresh:?}"
            );
            assert!(
                fresh.iter().all(|line| !line.contains("/original")),
                "{fresh:?}"
            );
        }
        output = blank_out();
        output.size = size_of::<OutHead>() as u32;
        let mut close: InHead = blank_in();
        close.op = slot::CLOSE;
        close.size = size_of::<InHead>() as u32;
        // SAFETY: this test owns the instance and closes it once before host loans expire.
        unsafe {
            table.head.close.unwrap()(
                instance,
                (&close as *const InHead).cast(),
                (&mut output as *mut OutHead).cast(),
            );
        }
        assert!(
            BORROWED.with(Cell::get),
            "end={end}: disk.append received a private payload copy instead of the host-owned loan"
        );
    }
}
