// SPDX-License-Identifier: Apache-2.0
//! File-specific conformance through the real linked and dropped-in loaders.
use busbar_contract::abi::export::{
    self, CheckIn, CheckOut, DeliverIn, ExportStream, ServeIn, ServeOut, StatusOut,
    CHECK_PHASE_INSTANCES, CHECK_PHASE_LIMITS,
};
use busbar_contract::abi::mechanism::call::{
    AbiStr, Blob, DeadlineClass, InHead, Outcome, BLOB_JSON, BLOB_JSONL,
};
use busbar_contract::abi::mechanism::lifecycle::{
    slot, OpenIn, OpenOut, RefreshIn, ReleaseIn, TickIn, TickOut, ValidateIn,
};
use busbar_contract::abi::sdk::door::{blank_in, blank_out};
use busbar_plugin_loader::dispatch::kinds::export::Export;
use busbar_plugin_loader::dispatch::{
    in_head, load_dropped, load_linked, out_head, Bind, Called, ConnTable, Diagnostic,
    DispatchConfig, Dispatcher, Dropped, EnvelopeSink, Frame, LinkedRow, Metric, Plugin,
};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Tape(Mutex<Vec<String>>);
impl EnvelopeSink for Tape {
    fn metric(&self, m: Metric<'_>) {
        self.0.lock().unwrap().push(format!(
            "metric {} {} {} {:?}",
            m.family, m.kind, m.value, m.labels
        ));
    }
    fn diag(&self, d: Diagnostic<'_>) {
        self.0.lock().unwrap().push(format!(
            "diag {} {} {}",
            String::from_utf8_lossy(d.name),
            d.severity,
            String::from_utf8_lossy(d.text)
        ));
    }
    fn dropped(&self, d: Dropped) {
        self.0.lock().unwrap().push(format!("dropped {d:?}"));
    }
}
mod support;
use support::cdylib as artifact;

fn blob(bytes: &[u8], fmt: u32) -> Blob {
    Blob {
        ptr: bytes.as_ptr(),
        len: bytes.len(),
        fmt,
        flags: 0,
    }
}
fn text(s: &str) -> AbiStr {
    AbiStr {
        ptr: s.as_ptr(),
        len: s.len(),
    }
}
fn answer(c: &Called) -> String {
    format!(
        "{:?} lease={} error={:?}",
        c.outcome,
        c.lease != 0,
        c.error.as_ref().map(|v| String::from_utf8_lossy(v))
    )
}
fn copied(b: Blob) -> Vec<u8> {
    if b.len == 0 {
        vec![]
    } else {
        // SAFETY: dispatcher judged the result and its lease remains held.
        unsafe { std::slice::from_raw_parts(b.ptr, b.len) }.to_vec()
    }
}
fn deliver(p: &Plugin<Export>, d: &Dispatcher, batch: &[u8]) -> Called {
    let owned = Arc::new(batch.to_vec());
    let mut input: DeliverIn = blank_in();
    input.head = in_head();
    input.stream = ExportStream::Logs as u8;
    input.batch = blob(&owned, BLOB_JSONL);
    let out = out_head();
    let ticket = d.mint(0).expect("ticket");
    let done = d
        .submit_watched(
            p,
            ticket,
            export::slot::DELIVER,
            Frame::new(input, out),
            DeadlineClass::WriteBehind,
            0,
            owned,
            std::time::Duration::ZERO,
        )
        .wait_done();
    d.recycle(ticket);
    Called {
        outcome: done.outcome,
        error: done.error.clone(),
        lease: done.lease,
        recall: None,
    }
}
fn run(dropped: bool) -> Vec<(String, String)> {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("conformance.json")).unwrap();
    let settings = serde_json::to_vec(&fixture["settings"]).unwrap();
    let d = Arc::new(Dispatcher::new(DispatchConfig::default()));
    let tape = Arc::new(Tape::default());
    let row = LinkedRow::of(busbar_export_file::door).unwrap();
    let bind = Bind {
        instance: Arc::from("export.conformance"),
        max_inflight_cap: 64,
        sink: tape.clone(),
        dispatcher: d.adopter(),
        conns: ConnTable::Probe,
    };
    let p = if dropped {
        load_dropped::<Export>(&artifact(), &row.statement, bind).unwrap()
    } else {
        load_linked::<Export>(&row, bind).unwrap()
    };
    let mut fold = Vec::new();
    let mut previous =
        p.inner.crossings.load(Ordering::SeqCst) - p.inner.resumes.load(Ordering::SeqCst);
    let mut record = |label: &str, pin: u64, line: String| {
        let now = p.inner.crossings.load(Ordering::SeqCst) - p.inner.resumes.load(Ordering::SeqCst);
        assert_eq!(now - previous, pin, "{label}");
        previous = now;
        let host = std::mem::take(&mut *tape.0.lock().unwrap());
        fold.push((label.to_owned(), format!("{line} host={host:?}")));
    };
    assert_eq!(p.name(), "busbar-export-file");
    assert_eq!(p.max_inflight(), 64);
    record(
        "facts",
        0,
        format!("{:?} {} {}", p.kind(), p.name(), p.max_inflight()),
    );
    for (i, bad) in fixture["export"]["bad_settings"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        let bytes = serde_json::to_vec(&bad["settings"]).unwrap();
        let mut input: ValidateIn = blank_in();
        input.head = in_head();
        input.settings = blob(&bytes, BLOB_JSON);
        let mut err = [0u8; 512];
        input.err_buf = err.as_mut_ptr();
        input.err_cap = err.len();
        let c = p.call(slot::VALIDATE, &mut Frame::new(input, out_head()));
        let route = if dropped { "dropped-in" } else { "linked" };
        let expected = bad["refusal"].as_str().expect("fixture refusal text");
        assert_eq!(
            c.outcome,
            Outcome::Failed,
            "{route} bad settings #{i}: {}",
            answer(&c)
        );
        assert_eq!(c.lease, 0, "{route} bad settings #{i}: {}", answer(&c));
        let actual = String::from_utf8_lossy(c.error.as_ref().unwrap_or_else(|| {
            panic!("{route} bad settings #{i}: missing refusal text; expected {expected:?}")
        }));
        assert!(
            actual.contains(expected),
            "{route} bad settings #{i}: expected refusal substring {expected:?}, actual {actual:?}"
        );
        record(&format!("validate bad {i}"), 1, answer(&c));
    }
    let batch = fixture["export"]["deliver"][0]["batch"]
        .as_str()
        .unwrap()
        .as_bytes();
    let c = deliver(&p, &d, batch);
    assert_ne!(c.outcome, Outcome::Ready);
    record("deliver unopened", 0, answer(&c));
    let mut input: ValidateIn = blank_in();
    input.head = in_head();
    input.settings = blob(&settings, BLOB_JSON);
    let mut err = [0u8; 512];
    input.err_buf = err.as_mut_ptr();
    input.err_cap = err.len();
    let c = p.call(slot::VALIDATE, &mut Frame::new(input, out_head()));
    assert_eq!(c.outcome, Outcome::Ready);
    record("validate", 1, answer(&c));
    let mut input: OpenIn = blank_in();
    input.head = in_head();
    input.settings = blob(&settings, BLOB_JSON);
    input.generation = 1;
    let mut out: OpenOut = blank_out();
    out.head = out_head();
    let c = p.call(slot::OPEN, &mut Frame::new(input, out));
    assert_eq!(c.outcome, Outcome::Ready);
    record("open", 1, answer(&c));
    let has = p.has_ready();
    let ready = p.ready(&d, std::time::Duration::from_secs(30));
    assert!(ready.is_ok(), "{ready:?}");
    record("ready", u64::from(has), format!("{has} {ready:?}"));
    let c = deliver(&p, &d, batch);
    assert_eq!(c.outcome, Outcome::Ready);
    assert_eq!(c.lease, 0);
    for want in fixture["export"]["deliver"][0]["observed"]
        .as_array()
        .unwrap()
    {
        assert!(tape
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|s| s.contains(want.as_str().unwrap())));
    }
    record("deliver", 1, answer(&c));
    let mut leases = Vec::new();
    let input: InHead = in_head();
    let mut out: StatusOut = blank_out();
    out.head = out_head();
    let mut frame = Frame::new(input, out);
    let c = p.call(export::slot::STATUS, &mut frame);
    assert_eq!(c.outcome, Outcome::Ready);
    assert_eq!(
        format!("{:?}", c.outcome),
        fixture["export"]["status"]["outcome"].as_str().unwrap()
    );
    let body = copied(frame.out.status);
    if c.lease != 0 {
        leases.push(c.lease);
    }
    record("status", 1, format!("{} {body:?}", answer(&c)));
    for phase in [CHECK_PHASE_LIMITS, CHECK_PHASE_INSTANCES] {
        let mut input: CheckIn = blank_in();
        input.head = in_head();
        input.phase = phase;
        let instance = busbar_contract::abi::export::CheckInstance {
            name: text("conformance"),
            settings: blob(&settings, BLOB_JSON),
        };
        input.instances = &instance;
        input.instances_len = 1;
        let mut out: CheckOut = blank_out();
        out.head = out_head();
        let mut frame = Frame::new(input, out);
        let c = p.call(export::slot::CHECK, &mut frame);
        assert_eq!(c.outcome, Outcome::Ready);
        assert_eq!(
            format!("{:?}", c.outcome),
            fixture["export"]["check"]["outcome"].as_str().unwrap()
        );
        let findings = copied(frame.out.findings);
        if c.lease != 0 {
            leases.push(c.lease);
        }
        record(
            &format!("check {phase}"),
            1,
            format!("{} {findings:?}", answer(&c)),
        );
    }
    let serve = &fixture["export"]["serve"][0];
    let mut input: ServeIn = blank_in();
    input.head = in_head();
    input.method = text(serve["method"].as_str().unwrap());
    input.path = text(serve["path"].as_str().unwrap());
    let mut out: ServeOut = blank_out();
    out.head = out_head();
    let mut frame = Frame::new(input, out);
    let c = p.call(export::slot::SERVE, &mut frame);
    assert_eq!(c.outcome, Outcome::Refused);
    assert_eq!(
        format!("{:?}", c.outcome),
        serve["outcome"].as_str().unwrap()
    );
    let body = copied(frame.out.body);
    if c.lease != 0 {
        leases.push(c.lease);
    }
    record(
        "serve",
        1,
        format!("{} {} {body:?}", answer(&c), frame.out.status_code),
    );
    let mut unique = leases.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), leases.len());
    record("own leases", 0, "distinct=true".into());
    for lease in &leases {
        let mut input: ReleaseIn = blank_in();
        input.head = in_head();
        input.lease = *lease;
        let c = p.call(slot::RELEASE, &mut Frame::new(input, out_head()));
        assert_eq!(c.outcome, Outcome::Ready);
        record("release", 1, answer(&c));
    }
    if let Some(lease) = leases.first() {
        let mut input: ReleaseIn = blank_in();
        input.head = in_head();
        input.lease = *lease;
        let c = p.call(slot::RELEASE, &mut Frame::new(input, out_head()));
        assert_eq!(c.outcome, Outcome::Refused);
        record("release again", 1, answer(&c));
    }
    let mut input: TickIn = blank_in();
    input.head = in_head();
    input.now_ns = 1;
    let mut out: TickOut = blank_out();
    out.head = out_head();
    let mut frame = Frame::new(input, out);
    let c = p.call(slot::TICK, &mut frame);
    assert_eq!(c.outcome, Outcome::Ready);
    record(
        "tick",
        1,
        format!("{} {}", answer(&c), frame.out.next_tick_ns),
    );
    for (label, bytes) in [
        ("refresh bad", b"{}".as_slice()),
        ("refresh", settings.as_slice()),
    ] {
        let mut input: RefreshIn = blank_in();
        input.head = in_head();
        input.settings = blob(bytes, BLOB_JSON);
        input.generation = 2;
        let c = p.call(slot::REFRESH, &mut Frame::new(input, out_head()));
        if label == "refresh" {
            assert_eq!(c.outcome, Outcome::Ready);
        } else {
            assert_eq!(c.outcome, Outcome::Failed, "{label}");
        }
        record(label, 1, answer(&c));
    }
    let c = deliver(&p, &d, batch);
    assert_eq!(c.outcome, Outcome::Ready);
    for want in fixture["export"]["deliver"][0]["observed"]
        .as_array()
        .unwrap()
    {
        assert!(tape
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|s| s.contains(want.as_str().unwrap())));
    }
    record("deliver after refresh", 1, answer(&c));
    let c = p.call(slot::CLOSE, &mut Frame::new(in_head(), out_head()));
    assert_eq!(c.outcome, Outcome::Ready);
    record("close", 1, answer(&c));
    let c = deliver(&p, &d, batch);
    assert_ne!(c.outcome, Outcome::Ready);
    record("deliver after close", 0, answer(&c));
    fold
}
#[test]
fn the_linked_and_the_dropped_in_door_are_one_plugin() {
    assert_eq!(run(false), run(true));
}
#[test]
fn no_configured_credential_crosses_back() {
    // The original suite returns immediately for a non-secret plugin with no credentials.
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("conformance.json")).unwrap();
    assert!(fixture.get("credentials").is_none());
    assert!(fixture.get("export").is_some());
}

fn red_bind(d: &Dispatcher) -> Bind {
    Bind {
        instance: Arc::from("export.file-red"),
        max_inflight_cap: 64,
        sink: Arc::new(Tape::default()),
        dispatcher: d.adopter(),
        conns: ConnTable::Probe,
    }
}
static RED_DOOR: std::sync::atomic::AtomicPtr<busbar_contract::abi::mechanism::door::Door> =
    std::sync::atomic::AtomicPtr::new(std::ptr::null_mut());
static RED_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
extern "C" fn red_door() -> *const busbar_contract::abi::mechanism::door::Door {
    RED_DOOR.load(std::sync::atomic::Ordering::Acquire)
}
fn restate(parts: busbar_contract::abi::mechanism::door::DoorParts) {
    // SAFETY: every pointee is the original static file door data; scalar changes are rejection probes.
    let door = Box::leak(Box::new(unsafe {
        busbar_contract::abi::mechanism::door::Door::__from_parts(parts)
    }));
    RED_DOOR.store(door, std::sync::atomic::Ordering::Release);
}
#[test]
fn another_kind_abi_or_layout_is_refused() {
    let _guard = RED_LOCK.lock().unwrap();
    use busbar_contract::abi::mechanism::{door::Door, KindCode};
    use busbar_plugin_loader::dispatch::LoadError;
    // SAFETY: real static linked file door.
    let real = unsafe { Door::read_parts(busbar_export_file::door()) };
    let kind = KindCode::Export;
    assert_eq!(real.kind_abi, kind.abi_version());
    assert_eq!(real.layout, kind.layout_fingerprint());
    let d = Dispatcher::new(DispatchConfig::default());
    for abi in [real.kind_abi + 1, real.kind_abi.wrapping_sub(1)] {
        restate(busbar_contract::abi::mechanism::door::DoorParts {
            kind_abi: abi,
            ..real
        });
        let error = LinkedRow::of_raw(red_door).unwrap_err();
        assert_eq!(
            error,
            LoadError::KindAbi {
                kind,
                door: abi,
                host: real.kind_abi
            }
        );
        assert!(error.to_string().contains("rebuild"));
        let mut statement = LinkedRow::of(busbar_export_file::door).unwrap().statement;
        let at = busbar_contract::abi::mechanism::rendering::RENDERING_MAGIC.len() + 8;
        statement[at..at + 4].copy_from_slice(&abi.to_le_bytes());
        assert_eq!(
            load_dropped::<Export>(&artifact(), &statement, red_bind(&d)).unwrap_err(),
            LoadError::ManifestKindAbi {
                stated: abi,
                host: real.kind_abi
            }
        );
    }
    for layout in [real.layout ^ 1, 0] {
        restate(busbar_contract::abi::mechanism::door::DoorParts { layout, ..real });
        let error = LinkedRow::of_raw(red_door).unwrap_err();
        assert!(
            matches!(&error,LoadError::KindLayout {kind: named,stated,host,..} if *named==kind && *stated==layout && *host==real.layout)
        );
        assert!(error
            .to_string()
            .contains("rebuild against this busbar-contract"));
        let row = LinkedRow {
            statement: LinkedRow::of(busbar_export_file::door).unwrap().statement,
            door: red_door,
        };
        assert_eq!(
            load_linked::<Export>(&row, red_bind(&d)).unwrap_err(),
            error
        );
    }
}
#[test]
fn another_statement_is_refused_by_both_loaders() {
    let _guard = RED_LOCK.lock().unwrap();
    use busbar_contract::abi::mechanism::door::{Door, DoorParts, Statement};
    use busbar_plugin_loader::dispatch::LoadError;
    let real = unsafe { Door::read_parts(busbar_export_file::door()) };
    let statement = Box::leak(Box::new(Statement {
        version: text("0.0.0-file-red"),
        ..unsafe { *real.statement }
    }));
    restate(DoorParts { statement, ..real });
    let honest = LinkedRow::of(busbar_export_file::door).unwrap();
    let other = LinkedRow::of_raw(red_door).unwrap();
    assert_ne!(honest.statement, other.statement);
    let d = Dispatcher::new(DispatchConfig::default());
    let row = LinkedRow {
        statement: other.statement.clone(),
        door: honest.door,
    };
    assert_eq!(
        load_linked::<Export>(&row, red_bind(&d)).unwrap_err(),
        LoadError::StatementMismatch
    );
    assert_eq!(
        load_dropped::<Export>(&artifact(), &other.statement, red_bind(&d)).unwrap_err(),
        LoadError::StatementMismatch
    );
    let row = LinkedRow {
        statement: honest.statement,
        door: red_door,
    };
    assert_eq!(
        load_linked::<Export>(&row, red_bind(&d)).unwrap_err(),
        LoadError::StatementMismatch
    );
}
#[test]
fn every_other_kind_refuses_the_file_door() {
    use busbar_plugin_loader::dispatch::{Kind, LoadError};
    fn refuse<K: Kind>() {
        let row = LinkedRow::of(busbar_export_file::door).unwrap();
        let d = Dispatcher::new(DispatchConfig::default());
        let want = LoadError::ManifestKind {
            stated: busbar_contract::abi::mechanism::KindCode::Export,
            want: K::CODE,
        };
        assert_eq!(load_linked::<K>(&row, red_bind(&d)).unwrap_err(), want);
        assert_eq!(
            load_dropped::<K>(&artifact(), &row.statement, red_bind(&d)).unwrap_err(),
            want
        );
    }
    use busbar_plugin_loader::dispatch::kinds::{
        auth::Auth, hook::Hook, plane::Plane, secret::Secret, store::Store, transport::Transport,
    };
    refuse::<Auth>();
    refuse::<Hook>();
    refuse::<Plane>();
    refuse::<Secret>();
    refuse::<Store>();
    refuse::<Transport>();
}
#[test]
fn declared_needs_and_requested_profile_match() {
    let row = LinkedRow::of(busbar_export_file::door).unwrap();
    let statement = busbar_contract::abi::mechanism::rendering::read(&row.statement).unwrap();
    assert!(statement.needs.is_empty());
    let declares: serde_json::Value = serde_json::from_str(busbar_export_file::DECLARES).unwrap();
    assert!(declares
        .get("needs")
        .is_none_or(|v| v.as_array().is_some_and(Vec::is_empty)));
    assert!(!(cfg!(debug_assertions) && std::env::var_os("BUSBAR_EXPECT_RELEASE").is_some()));
}
