// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

use super::*;
#[test]
fn loader_owned_file_shape_and_stream_contract() {
    assert_eq!(
        door::STREAMS,
        &[busbar_contract::abi::export::ExportStream::Logs as u8]
    );
    assert!(parse(br#"{"path":"opaque-path", "rotate_mb":64}"#).is_ok());
    assert_eq!(
        parse(br#"{"path":7}"#).unwrap_err().text(),
        Some("settings: invalid type: number, expected a string")
    );
}
#[test]
fn loader_owned_file_observation_names_are_declared() {
    let declaration: serde_json::Value = serde_json::from_str(DECLARES).unwrap();
    assert!(declaration["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .any(|d| d["code"] == 7074));
    assert_eq!(OPEN_FAILED, "BUSBAR-7074");
}
