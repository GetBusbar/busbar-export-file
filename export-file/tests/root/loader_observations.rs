// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! Product observations removed from the kernel's neutral disk-service witness.
use busbar_contract::export_calls::{Delivered, ExportCalls};
use busbar_plugin_loader::dispatch::{
    kinds::export::Export, load_dropped_bytes, load_linked, rendering_of, Bind, ConnTable,
    Diagnostic, DispatchConfig, Dispatcher, Dropped, EnvelopeSink, LinkedRow, Metric,
};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Default)]
struct Tape(Mutex<Vec<String>>);
impl EnvelopeSink for Tape {
    fn metric(&self, _: Metric<'_>) {}
    fn diag(&self, diagnostic: Diagnostic<'_>) {
        self.0.lock().unwrap().push(format!(
            "log {} {}",
            diagnostic.severity,
            String::from_utf8_lossy(diagnostic.text)
        ));
    }
    fn dropped(&self, why: Dropped) {
        panic!("observation dropped: {why:?}");
    }
}
fn exercise(dropped: bool, path: &Path) -> Vec<String> {
    let dispatcher = Arc::new(Dispatcher::with_services(
        DispatchConfig::default(),
        Arc::new(busbar_kernel::host_services::KernelServices::new()),
        busbar_kernel::host_services::caller_binding(),
    ));
    let tape = Arc::new(Tape::default());
    let bind = Bind {
        instance: format!("file-observation-{}", dropped).into(),
        max_inflight_cap: 64,
        sink: tape.clone(),
        dispatcher: dispatcher.adopter(),
        conns: ConnTable::NoNeeds,
    };
    let stated = rendering_of(busbar_export_file::door).unwrap();
    let plugin = if dropped {
        let image = PathBuf::from(
            std::env::var_os("BUSBAR_TEST_EXPORT_FILE_CDYLIB")
                .expect("the built actual file exporter image"),
        );
        load_dropped_bytes::<Export>(
            &std::fs::read(image).unwrap(),
            "actual-file-observation",
            &stated,
            bind,
        )
        .unwrap()
    } else {
        load_linked::<Export>(&LinkedRow::of(busbar_export_file::door).unwrap(), bind).unwrap()
    };
    let mut declares = busbar_plugin_loader::sign::Declares::from_declares_json(
        busbar_export_file::DECLARES.as_bytes(),
    )
    .unwrap();
    declares.keep.insert("path".into(), 2);
    plugin.grant_destinations(
        &busbar_contract::services::disk_admission::DiskAuthority::issue(
            &busbar_kernel::seal::acquire(),
        ),
        &declares,
    );
    let settings = serde_json::json!({"path":path,"rotate_mb":1});
    let sink = busbar_plugin_loader::export_door::ExportInstance::open(
        plugin,
        dispatcher,
        settings.to_string().as_bytes(),
    )
    .unwrap();
    for n in 1..=3 {
        assert!(matches!(
            sink.deliver(
                busbar_contract::abi::export::ExportStream::Logs as u8,
                serde_json::json!({"n":n,"pad":"x".repeat(1_100_000)})
                    .to_string()
                    .into_bytes(),
                Box::new(())
            ),
            Delivered::Queued
        ));
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while !PathBuf::from(format!("{}.2", path.display())).is_file()
        || tape.0.lock().unwrap().len() != 2
    {
        assert!(
            Instant::now() < deadline,
            "all actual deliveries and observations complete"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    drop(sink);
    let seen = tape.0.lock().unwrap().clone();
    seen
}
#[test]
fn actual_file_linked_and_dropped_report_identical_product_observations() {
    let dir = crate::root::test_plugins::scratch("owner-file-observations-both");
    let path = dir.join("requests.jsonl");
    let linked = exercise(false, &path);
    std::fs::remove_dir_all(&dir).unwrap();
    std::fs::create_dir_all(&dir).unwrap();
    let dropped = exercise(true, &path);
    std::fs::remove_dir_all(dir).unwrap();
    assert_eq!(
        linked, dropped,
        "both product origins emit the same observations"
    );
    assert!(!linked.is_empty(), "equivalence cannot be vacuous");
}
#[test]
fn actual_file_reports_each_rotation_in_its_product_words() {
    let dir = crate::root::test_plugins::scratch("owner-file-observations-words");
    let path = dir.join("requests.jsonl");
    let got = exercise(false, &path);
    let expected = format!(
        "log 0 busbar_export_file: request-log file rotated by rename path={p} archive={p}.1",
        p = path.display()
    );
    assert_eq!(got, vec![expected.clone(), expected]);
    std::fs::remove_dir_all(dir).unwrap();
}
