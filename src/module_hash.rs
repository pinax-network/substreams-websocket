//! Substreams module hash, ported from `streamingfast/substreams/manifest/signature.go`
//! and cross-checked against `substreams-js/packages/core/src/manifest/signature/create-module-hash.ts`.
//!
//! The output is the SHA-1 (20 bytes) of a deterministic byte concatenation of:
//!   - "initial_block" + 8 bytes LE uint64
//!   - "kind"          + "map" | "store" | "block_index"
//!   - "binary"        + binary.type + binary.content
//!   - "inputs"        + for each input: input_name + input_value
//!   - "ancestors"     + for each ancestor (transitive map/store inputs, by shortest path): nested hash
//!   - "entrypoint"    + module.binary_entrypoint
//!
//! Block-filter dependencies are not yet folded in; they are rare and only used by some
//! advanced Substreams. Add when needed.

use std::collections::{HashMap, HashSet, VecDeque};

use sha1::{Digest, Sha1};

use crate::substreams::pb::sf::substreams::v1::{Module, Modules, module, module::input};

#[derive(Debug, thiserror::Error)]
pub enum ModuleHashError {
    #[error("module {0:?} not found in package")]
    UnknownModule(String),

    #[error("module {module:?} has no kind set")]
    MissingKind { module: String },

    #[error("module {module:?} references unknown binary index {index}")]
    MissingBinary { module: String, index: u32 },

    #[error("module {module:?} input #{index} has no inner input set")]
    MissingInput { module: String, index: usize },
}

/// Compute the canonical Substreams module hash (20 bytes, SHA-1) for `module_name`
/// against the modules and binaries in `modules`.
pub fn compute_module_hash(
    modules: &Modules,
    module_name: &str,
) -> Result<[u8; 20], ModuleHashError> {
    let index = build_index(modules);
    let target = index
        .get(module_name)
        .copied()
        .ok_or_else(|| ModuleHashError::UnknownModule(module_name.to_owned()))?;
    hash_module(modules, target, &index, &mut HashMap::new())
}

/// Hex-encoded module hash, lowercase, no `0x` prefix.
pub fn compute_module_hash_hex(
    modules: &Modules,
    module_name: &str,
) -> Result<String, ModuleHashError> {
    Ok(hex_encode(&compute_module_hash(modules, module_name)?))
}

fn build_index(modules: &Modules) -> HashMap<&str, usize> {
    modules
        .modules
        .iter()
        .enumerate()
        .map(|(i, module)| (module.name.as_str(), i))
        .collect()
}

fn hash_module(
    modules: &Modules,
    module_idx: usize,
    index: &HashMap<&str, usize>,
    cache: &mut HashMap<usize, [u8; 20]>,
) -> Result<[u8; 20], ModuleHashError> {
    if let Some(cached) = cache.get(&module_idx) {
        return Ok(*cached);
    }

    let module = &modules.modules[module_idx];
    let mut buf: Vec<u8> = Vec::new();

    buf.extend_from_slice(b"initial_block");
    buf.extend_from_slice(&module.initial_block.to_le_bytes());

    buf.extend_from_slice(b"kind");
    buf.extend_from_slice(kind_name(module)?.as_bytes());

    buf.extend_from_slice(b"binary");
    let binary = modules
        .binaries
        .get(module.binary_index as usize)
        .ok_or_else(|| ModuleHashError::MissingBinary {
            module: module.name.clone(),
            index: module.binary_index,
        })?;
    buf.extend_from_slice(binary.r#type.as_bytes());
    buf.extend_from_slice(&binary.content);

    buf.extend_from_slice(b"inputs");
    for (i, input) in module.inputs.iter().enumerate() {
        let (name, value) = input_name_value(input).ok_or(ModuleHashError::MissingInput {
            module: module.name.clone(),
            index: i,
        })?;
        buf.extend_from_slice(name.as_bytes());
        buf.extend_from_slice(value.as_bytes());
    }

    buf.extend_from_slice(b"ancestors");
    for ancestor_idx in ancestors_of(modules, module_idx, index) {
        let ancestor_hash = hash_module(modules, ancestor_idx, index, cache)?;
        buf.extend_from_slice(&ancestor_hash);
    }

    buf.extend_from_slice(b"entrypoint");
    buf.extend_from_slice(module.binary_entrypoint.as_bytes());

    let digest: [u8; 20] = Sha1::digest(&buf).into();
    cache.insert(module_idx, digest);
    Ok(digest)
}

fn kind_name(module: &Module) -> Result<&'static str, ModuleHashError> {
    match module.kind.as_ref() {
        Some(module::Kind::KindMap(_)) => Ok("map"),
        Some(module::Kind::KindStore(_)) => Ok("store"),
        Some(module::Kind::KindBlockIndex(_)) => Ok("block_index"),
        None => Err(ModuleHashError::MissingKind {
            module: module.name.clone(),
        }),
    }
}

fn input_name_value(input: &module::Input) -> Option<(&'static str, String)> {
    let inner = input.input.as_ref()?;
    Some(match inner {
        input::Input::Store(_) => ("store", String::new()),
        input::Input::Source(source) => ("source", source.r#type.clone()),
        input::Input::Map(_) => ("map", String::new()),
        input::Input::Params(params) => ("params", params.value.clone()),
        input::Input::FoundationalStore(fs) => ("foundational-store", fs.identifier.clone()),
    })
}

/// Direct module-to-module dependencies (Map/Store inputs only).
fn direct_parents<'a>(module: &'a Module, index: &HashMap<&str, usize>) -> Vec<usize> {
    let mut parents = Vec::new();
    for input in &module.inputs {
        let Some(inner) = input.input.as_ref() else {
            continue;
        };
        let name = match inner {
            input::Input::Map(m) => m.module_name.as_str(),
            input::Input::Store(s) => s.module_name.as_str(),
            _ => continue,
        };
        if let Some(&parent) = index.get(name) {
            parents.push(parent);
        }
    }
    parents
}

/// All transitive ancestors of `module_idx`, returned in deterministic order:
/// breadth-first by shortest path from the target module, ties broken by the
/// order in `Modules.modules`. Matches the JS `ancestorsOf` (shortest-paths +
/// distance >= 1) for any DAG without cycles.
fn ancestors_of(modules: &Modules, module_idx: usize, index: &HashMap<&str, usize>) -> Vec<usize> {
    let mut distances: HashMap<usize, usize> = HashMap::new();
    distances.insert(module_idx, 0);

    let mut queue: VecDeque<usize> = VecDeque::new();
    queue.push_back(module_idx);
    let mut visited: HashSet<usize> = HashSet::new();
    visited.insert(module_idx);

    while let Some(current) = queue.pop_front() {
        let current_distance = distances[&current];
        for parent in direct_parents(&modules.modules[current], index) {
            let new_distance = current_distance + 1;
            distances
                .entry(parent)
                .and_modify(|d| {
                    if new_distance < *d {
                        *d = new_distance;
                    }
                })
                .or_insert(new_distance);
            if visited.insert(parent) {
                queue.push_back(parent);
            }
        }
    }

    let mut ancestors: Vec<usize> = distances
        .into_iter()
        .filter(|(idx, distance)| *idx != module_idx && *distance >= 1)
        .map(|(idx, _)| idx)
        .collect();

    // Deterministic: sort by module definition order to match Go's stable graph traversal.
    ancestors.sort_unstable();
    ancestors
}

pub fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(*byte >> 4) as usize] as char);
        out.push(HEX[(*byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::substreams::pb::sf::substreams::v1::{
        Binary, Module, Modules, module, module::Kind, module::KindMap,
    };

    fn map_module(name: &str, inputs: Vec<module::Input>) -> Module {
        Module {
            name: name.to_owned(),
            kind: Some(Kind::KindMap(KindMap {
                output_type: "T".to_owned(),
            })),
            binary_index: 0,
            binary_entrypoint: format!("entry_{name}"),
            inputs,
            output: None,
            initial_block: 0,
            block_filter: None,
        }
    }

    fn map_input(name: &str) -> module::Input {
        module::Input {
            input: Some(module::input::Input::Map(module::input::Map {
                module_name: name.to_owned(),
            })),
        }
    }

    fn source_input(ty: &str) -> module::Input {
        module::Input {
            input: Some(module::input::Input::Source(module::input::Source {
                r#type: ty.to_owned(),
            })),
        }
    }

    fn modules() -> Modules {
        Modules {
            modules: vec![
                map_module("root", vec![source_input("sf.solana.type.v1.Block")]),
                map_module("leaf", vec![map_input("root")]),
            ],
            binaries: vec![Binary {
                r#type: "wasm/rust-v1".to_owned(),
                content: b"WASM-CONTENT".to_vec(),
            }],
        }
    }

    #[test]
    fn hashes_are_stable_for_same_input() {
        let m = modules();
        let h1 = compute_module_hash(&m, "leaf").unwrap();
        let h2 = compute_module_hash(&m, "leaf").unwrap();
        assert_eq!(h1, h2);
    }

    #[test]
    fn different_modules_produce_different_hashes() {
        let m = modules();
        let root = compute_module_hash(&m, "root").unwrap();
        let leaf = compute_module_hash(&m, "leaf").unwrap();
        assert_ne!(root, leaf);
    }

    #[test]
    fn changing_binary_content_changes_hash() {
        let mut m = modules();
        let before = compute_module_hash(&m, "leaf").unwrap();
        m.binaries[0].content = b"OTHER-WASM".to_vec();
        let after = compute_module_hash(&m, "leaf").unwrap();
        assert_ne!(before, after);
    }

    #[test]
    fn changing_param_value_changes_hash() {
        let mut m = modules();
        m.modules[1].inputs.push(module::Input {
            input: Some(module::input::Input::Params(module::input::Params {
                value: "protocol=raydium".to_owned(),
            })),
        });
        let with_v1 = compute_module_hash(&m, "leaf").unwrap();

        if let Some(module::input::Input::Params(p)) =
            m.modules[1].inputs.last_mut().unwrap().input.as_mut()
        {
            p.value = "protocol=jupiter".to_owned();
        }
        let with_v2 = compute_module_hash(&m, "leaf").unwrap();
        assert_ne!(with_v1, with_v2);
    }

    #[test]
    fn unknown_module_errors() {
        let m = modules();
        let err = compute_module_hash(&m, "ghost").unwrap_err();
        assert!(matches!(err, ModuleHashError::UnknownModule(_)));
    }

    #[test]
    fn hex_is_lowercase_40_chars() {
        let m = modules();
        let hex = compute_module_hash_hex(&m, "leaf").unwrap();
        assert_eq!(hex.len(), 40);
        assert!(
            hex.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }
}
