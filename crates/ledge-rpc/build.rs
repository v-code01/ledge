//! Build script: run the Cap'n Proto compiler (`capnpc`) over the cross-language
//! schema `sdk/schema/ledge.capnp`, generating Rust types into `OUT_DIR`.
//!
//! The build REQUIRES the `capnp` binary to be present. `capnpc` resolves it
//! from `PATH` by default; if that fails we fall back to the Homebrew install
//! path so the build works in shells that have not run `brew shellenv`.

use std::path::Path;

fn main() {
    // The schema lives outside the crate (shared SDK contract); re-run codegen
    // whenever it changes.
    println!("cargo:rerun-if-changed=../../sdk/schema/ledge.capnp");
    println!("cargo:rerun-if-changed=build.rs");

    let mut cmd = capnpc::CompilerCommand::new();
    cmd.file("../../sdk/schema/ledge.capnp")
        // The schema's import root, so `@0x...` ids resolve consistently.
        .src_prefix("../../sdk/schema");

    // If `capnp` is not on PATH, point capnpc at the known Homebrew location.
    if which_capnp().is_none() {
        let brew = Path::new("/opt/homebrew/opt/capnp/bin/capnp");
        if brew.exists() {
            cmd.capnp_executable(brew);
        }
    }

    cmd.run().expect("capnpc codegen failed for ledge.capnp");
}

/// Probe `PATH` for a `capnp` executable. Returns `Some(())` if found.
fn which_capnp() -> Option<()> {
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        let candidate = dir.join("capnp");
        if candidate.is_file() {
            return Some(());
        }
    }
    None
}
