// SPDX-License-Identifier: Apache-2.0
//! The actual file-plugin artifact selected by the build lane.
pub fn cdylib() -> std::path::PathBuf {
    let path = if let Some(path) = std::env::var_os("BUSBAR_PLUGIN_CDYLIB") {
        let path = std::path::PathBuf::from(path);
        assert!(path.is_absolute(), "BUSBAR_PLUGIN_CDYLIB must be absolute");
        path
    } else {
        // The test executable sits in `<profile>/deps/`, beside the cdylib `cargo test` builds for
        // this crate's dropped-in dev-dependency (README); that one is this run's build, so it wins
        // over one an explicit `cargo build` uplifted into `<profile>/` earlier.
        let exe = std::env::current_exe().expect("test executable path");
        let deps = exe.parent().expect("test executable directory");
        let name = busbar_plugin_loader::plugin_library_filename("busbar_export_file_plugin");
        let built = deps.join(&name);
        if built.is_file() {
            built
        } else {
            deps.parent().expect("target profile directory").join(name)
        }
    };
    assert!(
        path.is_file(),
        "build the actual file plugin cdylib with dropped-in before testing: {}",
        path.display()
    );
    path
}
