use std::process::Command;

use prost::Message;
use prost_types::FileDescriptorSet;

/// Proto definitions are imported from the buf.build registry at build time
/// (no vendored copies, no protoc), pinned to a release label where the
/// module publishes them. To upgrade, pick a newer ref:
///
/// ```bash
/// buf registry module label list buf.build/streamingfast/substreams --page-size 10
/// ```
///
/// Each entry is `(module, paths)`; `paths` limits the build to those proto
/// files plus their transitive imports.
const BUF_MODULES: &[(&str, &[&str])] = &[
    (
        "buf.build/streamingfast/substreams:v1.18.5",
        &[
            "sf/substreams/rpc/v3/service.proto",
            "sf/substreams/rpc/v2/service.proto",
            "sf/substreams/v1/package.proto",
            "sf/substreams/v1/modules.proto",
            "sf/substreams/v1/clock.proto",
            "sf/substreams/v1/deltas.proto",
        ],
    ),
    (
        // This module publishes no version labels; pinned by BSR commit
        // (`main` as of 2023-06-28 — the proto has been stable since).
        "buf.build/streamingfast/substreams-sink-database-changes:bfa22295f99e4cceb62d1aa8fdce988e",
        &["sf/substreams/sink/database/v1/database.proto"],
    ),
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    for &(module, paths) in BUF_MODULES {
        let mut command = Command::new("buf");
        command.args(["build", module, "--as-file-descriptor-set", "-o", "-"]);
        for path in paths {
            command.args(["--path", path]);
        }
        let output = command.output().map_err(|error| {
            format!(
                "failed to run `buf build {module}`: {error}; the buf CLI is required to \
                 fetch proto definitions from buf.build — see https://buf.build/docs/installation"
            )
        })?;
        if !output.status.success() {
            return Err(format!(
                "`buf build {module}` failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }

        let descriptors = FileDescriptorSet::decode(output.stdout.as_slice())?;
        tonic_prost_build::configure()
            .build_server(false)
            .compile_fds(descriptors)?;
    }

    println!("cargo:rerun-if-changed=build.rs");
    Ok(())
}
