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

    emit_build_metadata();

    println!("cargo:rerun-if-changed=build.rs");
    Ok(())
}

/// Capture git short SHA and commit date for the `/version` endpoint.
///
/// Prefers the `GIT_COMMIT` / `GIT_COMMIT_DATE` build-time env vars when set,
/// then falls back to live `git` for local builds, then `"unknown"` so the
/// build never fails on missing VCS metadata. The env-first path exists because
/// the Docker image build excludes `.git` (see `.dockerignore`) — CI injects
/// the values as build args instead. Mirrors `pinax-network/pinax-api`.
fn emit_build_metadata() {
    let git = |args: &[&str]| -> Option<String> {
        let out = Command::new("git").args(args).output().ok()?;
        if !out.status.success() {
            return None;
        }
        let s = String::from_utf8(out.stdout).ok()?.trim().to_owned();
        (!s.is_empty()).then_some(s)
    };

    // Build-time env var (injected by CI/Docker) wins; otherwise run git.
    let env_var = |name: &str| std::env::var(name).ok().filter(|s| !s.is_empty());
    let env_or_git = |name: &str, args: &[&str]| -> String {
        env_var(name)
            .or_else(|| git(args))
            .unwrap_or_else(|| "unknown".to_owned())
    };

    // Short-SHA the commit so an injected full 40-char SHA matches `--short`.
    let mut commit = env_or_git("GIT_COMMIT", &["rev-parse", "--short", "HEAD"]);
    if commit.len() > 7 && commit.bytes().all(|b| b.is_ascii_hexdigit()) {
        commit.truncate(7);
    }
    let date = env_or_git(
        "GIT_COMMIT_DATE",
        &["log", "-1", "--format=%cd", "--date=short"],
    );

    // OKF `timestamp` for skills/SKILL.md — ISO 8601 datetime of its last
    // meaningful change (its last commit). Injected into the served `/SKILL.md`.
    let skill_ts = env_var("SKILL_MD_TIMESTAMP")
        .or_else(|| git(&["log", "-1", "--format=%cI", "--", "skills/SKILL.md"]))
        .unwrap_or_else(|| "unknown".to_owned());

    println!("cargo:rustc-env=GIT_COMMIT={commit}");
    println!("cargo:rustc-env=GIT_COMMIT_DATE={date}");
    println!("cargo:rustc-env=SKILL_MD_TIMESTAMP={skill_ts}");
    // Rebuild when the checked-out commit or injected metadata changes so the
    // values stay accurate.
    println!("cargo:rerun-if-env-changed=GIT_COMMIT");
    println!("cargo:rerun-if-env-changed=GIT_COMMIT_DATE");
    println!("cargo:rerun-if-env-changed=SKILL_MD_TIMESTAMP");
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs");
}
