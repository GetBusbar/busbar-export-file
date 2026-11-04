// SPDX-License-Identifier: Apache-2.0
//! The actual file-plugin artifact selected by the build lane.
pub fn cdylib() -> std::path::PathBuf {
    let path = if let Some(path) = std::env::var_os("BUSBAR_PLUGIN_CDYLIB") {
        let path = std::path::PathBuf::from(path);
        assert!(path.is_absolute(), "BUSBAR_PLUGIN_CDYLIB must be absolute");
        path
    } else {
        let exe = std::env::current_exe().expect("test executable path");
        let profile = exe
            .parent()
            .and_then(|p| p.parent())
            .expect("target profile directory");
        profile.join(busbar_plugin_loader::plugin_library_filename(
            "busbar_export_file_plugin",
        ))
    };
    assert!(
        path.is_file(),
        "build the actual file plugin cdylib with dropped-in before testing: {}",
        path.display()
    );
    path
}
