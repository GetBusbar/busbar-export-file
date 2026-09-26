// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! **ONE EXPORT SINK, BOTH DOORS, ONE ROW** — the file sink's linked + dropped-in conformance, run
//! against the busbar rev this repo pins (`.busbar-ref`).
//!
//! The sink is held two ways at once: LINKED (its `linked::EXPORT` statement and boundary, the row a
//! busbar build that compiles it in registers) and DROPPED IN (this crate's built cdylib, signed
//! first-party under the SAME statement — `declares` included, which is what
//! `busbar-plugin-pack --declares-file` embeds — into a temp `plugins/` directory and found by the
//! loader's scan). Each arm is opened by the one `open_export`, its settings validated by the one
//! `probe_export`, and driven through a rotation scenario against a real file: the two arms must
//! agree byte for byte on the row, the validation lines, the series the host grants and every file
//! the deliveries leave behind — lines, rotation and retention.
//!
//! The RED arm is in the same test: the same cdylib dropped in WITHOUT its declarations is not the
//! same sink — the host binds it no destination, so its deliveries leave no file.
//!
//! Ported from busbar's `crates/busbar/src/root/tests/linked_exports.rs` (K9b), where the sink was
//! proven before it moved to this repo; busbar still runs that test against the pinned sink.

use busbar_plugin_loader::sign::{sign, Manifest, SigningKey, TrustPolicy};
use busbar_plugin_loader::{LinkedPlugin, PluginRegistry};

/// The release key the dropped-in arm is signed with, and the policy's first-party key.
fn release() -> SigningKey {
    SigningKey::from_bytes(&[11u8; 32])
}

/// The version both arms state (a linked row states its binary's version; here, this crate's).
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// This crate's built cdylib (uplifted or under `deps`, newest wins). A missing artifact is a
/// failure, never a skip: this test IS the dropped-in door's proof.
fn cdylib() -> Vec<u8> {
    let exe = std::env::current_exe().expect("the test binary has a path");
    let profile = exe
        .parent()
        .and_then(|d| d.parent())
        .expect("target/<profile>");
    let file = busbar_plugin_loader::plugin_library_filename("busbar_export_file_plugin");
    let found = [profile.join(&file), profile.join("deps").join(&file)]
        .into_iter()
        .filter_map(|p| Some((std::fs::metadata(&p).ok()?.modified().ok()?, p)))
        .max()
        .map(|(_, p)| p)
        .unwrap_or_else(|| panic!("the busbar-export-file-plugin cdylib ({file}) is not built"));
    std::fs::read(found).expect("read the cdylib")
}

/// The LINKED row: exactly what busbar's composition root states for `linked::EXPORT`.
fn linked_row() -> LinkedPlugin {
    let (name, alias, declares, entry) = busbar_export_file::linked::EXPORT;
    let abi = busbar_plugin_loader::supported_abi("export")
        .iter()
        .copied()
        .max()
        .unwrap_or_default();
    let manifest = Manifest {
        name: name.into(),
        alias: alias.into(),
        kind: "export".into(),
        version: VERSION.into(),
        publisher: busbar_plugin_loader::sign::FIRST_PARTY_PUBLISHER.into(),
        abi_version: abi,
        sha256: String::new(),
        signature: String::new(),
        description: String::new(),
        homepage: String::new(),
        license: String::new(),
        needs: Default::default(),
        settings_schema: None,
        schema_derived: false,
        host: None,
        declares: serde_json::from_str(declares).expect("declares.json parses"),
    };
    LinkedPlugin::boundary(manifest, entry)
}

/// THE DROPPED-IN DOOR: `lib` signed first-party under `manifest` into a fresh `plugins/`
/// directory, scanned under a policy holding the release key.
fn dropped(tag: &str, manifest: Manifest, lib: &[u8]) -> PluginRegistry {
    let dir = scratch(&format!("plugins-{tag}"));
    let signed = sign(&release(), manifest, lib);
    let tarball = busbar_plugin_loader::tarball::package(&signed, "libsink.so", lib).unwrap();
    std::fs::write(dir.join("sink.tar.gz"), tarball).unwrap();
    let policy = TrustPolicy {
        first_party_key: Some(release().verifying_key()),
        binary_version: VERSION.into(),
        first_party_floors: Default::default(),
        first_party_high_water: Default::default(),
        publishers: Default::default(),
        allow_unsigned: false,
        allow_third_party: false,
        min_versions: Default::default(),
    };
    busbar_plugin_loader::scan_and_validate(&dir, &policy).expect("the signed sink scans")
}

/// A fresh scratch directory for this process.
fn scratch(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("export-file-conf-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// What one door does with `alias`, as one comparable transcript: the row's statement, whether it
/// is first-party, the validation lines, the series the host granted it, and — after a rotation
/// scenario against a real file under `tag` — every file left behind, by name, with its bytes.
fn transcript(tag: &str, registry: &PluginRegistry, alias: &str) -> serde_json::Value {
    let p = registry.resolve(alias).expect("the alias resolves");
    let stated = Manifest {
        sha256: String::new(),
        signature: String::new(),
        ..p.manifest.clone()
    };
    let validation = [
        serde_json::json!({"path": "/x"}),
        serde_json::json!({}),
        serde_json::json!({"path": "/x", "rotate_mb": "one"}),
        serde_json::json!({"path": "/x", "rotate": 1}),
    ]
    .map(|s| registry.probe_export(alias, "tail", &s));

    // THE ROTATION SCENARIO: a live file already at `rotate_mb`, a full archive series (so the
    // oldest is retired), then three deliveries — the first rotates, the next two append.
    let dir = scratch(&format!("files-{tag}"));
    let path = dir.join("requests.jsonl");
    std::fs::write(&path, vec![b'x'; 1024 * 1024]).unwrap();
    for i in 1..=9 {
        std::fs::write(
            dir.join(format!("requests.jsonl.{i}")),
            format!("archive {i}\n"),
        )
        .unwrap();
    }
    let settings = serde_json::json!({"path": path.display().to_string(), "rotate_mb": 1});
    let sink = registry
        .open_export(alias, &settings.to_string())
        .expect("the sink opens");
    let granted: Vec<String> = stated
        .declares
        .metrics
        .iter()
        .filter(|d| {
            busbar_plugin_loader::observe::first_party_series(&stated.name, &d.name, &d.kind)
        })
        .map(|d| d.name.clone())
        .collect();
    for n in 0..3 {
        let line = serde_json::json!({"ingress_protocol": "k9b", "outcome": "ok", "ts": n});
        sink.deliver(busbar_plugin_loader::ExportStream::Logs, &line)
            .expect("the delivery completes");
    }
    let mut files: Vec<(String, String)> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .map(|f| {
            let bytes = std::fs::read(&f).unwrap();
            let name = f.file_name().unwrap().to_string_lossy().into_owned();
            let text = match bytes.len() > 4096 {
                true => format!("{} bytes", bytes.len()),
                false => String::from_utf8_lossy(&bytes).into_owned(),
            };
            (name, text)
        })
        .collect();
    files.sort();
    let _ = std::fs::remove_dir_all(&dir);
    serde_json::json!({
        "row": stated,
        "first_party": p.first_party(),
        "validation": validation,
        "granted": granted,
        "files": files,
    })
}

/// The file sink registers ONE row and behaves as ONE sink through either door — and the same
/// cdylib without its declarations does not (the RED arm).
#[test]
fn the_linked_and_the_dropped_in_file_sink_are_one_sink() {
    let row = linked_row();
    let (manifest, alias) = (row.manifest.clone(), row.manifest.alias.clone());
    assert_eq!(manifest.name, "busbar-export-file");
    assert_eq!(alias, "request-log-file");
    let lib = cdylib();

    let linked_registry = PluginRegistry::empty().link(vec![row]).unwrap();
    let linked = transcript("linked", &linked_registry, &alias);
    let dropped_registry = dropped("dropped", manifest.clone(), &lib);
    let dropped_in = transcript("dropped", &dropped_registry, &alias);
    assert_eq!(linked, dropped_in, "the two doors are not one sink");

    // The scenario did what the sink is for: the full archive series shifted up and the oldest
    // retired, the live file renamed to `.1`, and the three lines in a fresh file.
    let files = linked["files"].as_array().unwrap();
    assert_eq!(files.len(), 10, "{files:?}");
    assert_eq!(files[0][0], "requests.jsonl");
    assert_eq!(
        files[0][1].as_str().unwrap(),
        "{\"ingress_protocol\":\"k9b\",\"outcome\":\"ok\",\"ts\":0}\n\
         {\"ingress_protocol\":\"k9b\",\"outcome\":\"ok\",\"ts\":1}\n\
         {\"ingress_protocol\":\"k9b\",\"outcome\":\"ok\",\"ts\":2}\n"
    );
    assert_eq!(
        files[1],
        serde_json::json!(["requests.jsonl.1", "1048576 bytes"])
    );
    assert_eq!(
        files[9],
        serde_json::json!(["requests.jsonl.9", "archive 8\n"])
    );
    assert_eq!(linked["first_party"], true);

    // RED ARM: the same bytes, dropped in without their declarations.
    let bare = Manifest {
        declares: Default::default(),
        ..manifest
    };
    let red_registry = dropped("red", bare, &lib);
    let red = transcript("red", &red_registry, &alias);
    assert_ne!(red, linked, "an undeclared sink must not be the same sink");
    let red_files = red["files"].as_array().unwrap();
    assert!(
        !red_files
            .iter()
            .any(|f| f[1].as_str().unwrap().contains("k9b")),
        "with no destination granted, no line may be written: {red_files:?}"
    );
}
