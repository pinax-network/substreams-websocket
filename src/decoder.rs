use prost::Message;
use serde::Serialize;

pub mod pb {
    pub mod sf {
        pub mod substreams {
            pub mod sink {
                pub mod database {
                    pub mod v1 {
                        tonic::include_proto!("sf.substreams.sink.database.v1");
                    }
                }
            }
        }
    }
}

use pb::sf::substreams::sink::database::v1::DatabaseChanges;

/// Protobuf `type.googleapis.com/...` URL that every supported Substreams
/// module must produce. Anything else is rejected at startup.
pub const SUPPORTED_OUTPUT_TYPE_URL: &str =
    "type.googleapis.com/sf.substreams.sink.database.v1.DatabaseChanges";

/// Bare proto name as it appears in a Substreams manifest's `output.type`.
pub const SUPPORTED_OUTPUT_TYPE: &str = "proto:sf.substreams.sink.database.v1.DatabaseChanges";

/// Per-row keys that duplicate top-level `block.*` fields. Stripped from each
/// event to avoid repeating the same value on every row.
const BLOCK_KEYS_TO_STRIP: &[&str] = &["block_num", "block_hash", "timestamp", "minute"];

/// Per-row keys that exist only as ClickHouse-backfill provenance. They carry
/// no useful information for a live WebSocket consumer. Names taken from the
/// shared `substreams-evm` (`set_template_tx`/`_log`/`_call`) and
/// `substreams-svm` (`set_*_transaction_v2`/`_instruction_v2`) templates.
const EXTRA_KEYS_TO_STRIP: &[&str] = &[
    // EVM tx provenance (set_template_tx)
    "tx_index",
    "tx_nonce",
    "tx_gas_price",
    "tx_gas_limit",
    "tx_gas_used",
    "tx_value",
    // EVM log provenance (set_template_log). `log_ordinal` is retained as
    // the canonical EVM event ordering key; `log_index` is dropped since it
    // duplicates positional information already available from the row order.
    "log_index",
    "log_block_index",
    "log_topics",
    "log_data",
    // EVM call provenance (set_template_call) — full set, debug/trace only
    "call_caller",
    "call_index",
    "call_begin_ordinal",
    "call_end_ordinal",
    "call_address",
    "call_value",
    "call_gas_consumed",
    "call_gas_limit",
    "call_depth",
    "call_parent_index",
    "call_type",
    // SVM transaction provenance (set_*_transaction_v2)
    "compute_units_consumed",
    // SVM instruction provenance (set_*_instruction_v2). `program_id` is
    // intentionally retained — semantic identifier, not provenance.
    "stack_height",
];

/// SVM emits a transaction-level `fee` that is pure provenance. EVM's
/// `swap_fee` table emits a *protocol* `fee` that is real data. We can't
/// tell by table name alone here (decoder is chain-agnostic), so strip
/// `fee` only when the row also carries SVM-specific `compute_units_consumed`,
/// which never appears on EVM rows.
const SVM_FEE_GUARD: &str = "compute_units_consumed";

/// Suffix applied by upstream templates when a list value is joined into a
/// comma string for ClickHouse. On the wire we split it back into a JSON
/// array and drop the suffix (`signers_raw` → `signers: [...]`).
const RAW_LIST_SUFFIX: &str = "_raw";

#[derive(Debug, Clone)]
pub struct BlockContext {
    pub block_num: u64,
    pub block_hash: String,
    pub timestamp: String,
    /// Unix epoch seconds for the block timestamp. Used as the chain-agnostic
    /// resume key (`?from_timestamp=`) and for replay log time-window trim.
    pub timestamp_seconds: i64,
    pub network: String,
    pub module_hash: String,
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("failed to decode sf.substreams.sink.database.v1.DatabaseChanges: {0}")]
    DatabaseChanges(#[source] prost::DecodeError),

    #[error("failed to serialize decoded payload: {0}")]
    Serialize(#[from] serde_json::Error),
}

/// Decoded block, pre-routing. Subscribers receive per-table sub-blocks
/// (one per `@table` group) so the routing happens at the broadcast layer,
/// not at decode. Each event row carries its `@table` key so the server can
/// split the block by table before fan-out.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DatabaseChangesBlockMessage {
    pub network: String,
    pub block_num: u64,
    pub block_hash: String,
    pub timestamp: String,
    /// Unix epoch seconds. Same value as `timestamp` but in a machine-friendly
    /// integer form. Used by `?from_timestamp=` reconnect and the replay log.
    pub timestamp_seconds: i64,
    /// Canonical Substreams module hash of the configured output module.
    /// Subscribers can use it to detect spkg upgrades.
    pub module_hash: String,
    pub events: Vec<serde_json::Map<String, serde_json::Value>>,
}

/// Decode a `sf.substreams.sink.database.v1.DatabaseChanges` payload into a flat
/// block message. Each row in `table_changes` becomes one object containing the
/// `table` name plus every `field.name -> field.value` pair, in the order they
/// appear on the wire. `ordinal`, `operation`, `pk`/`composite_pk`, and
/// `update_op` are intentionally dropped — values pass through as-is. Fields
/// that duplicate `block.*` (block_num, block_hash, timestamp, minute) are
/// stripped from each row.
pub fn decode_database_changes(
    payload: &[u8],
    context: BlockContext,
) -> Result<DatabaseChangesBlockMessage, DecodeError> {
    let changes = DatabaseChanges::decode(payload).map_err(DecodeError::DatabaseChanges)?;
    Ok(normalize_database_changes(changes, context))
}

pub fn normalize_database_changes(
    changes: DatabaseChanges,
    context: BlockContext,
) -> DatabaseChangesBlockMessage {
    let events = changes
        .table_changes
        .into_iter()
        .filter_map(|change| {
            let has_svm_fee_guard = change.fields.iter().any(|f| f.name == SVM_FEE_GUARD);
            let mut row = serde_json::Map::with_capacity(change.fields.len() + 1);
            row.insert("@table".to_owned(), serde_json::Value::String(change.table));
            for field in change.fields {
                if BLOCK_KEYS_TO_STRIP.contains(&field.name.as_str()) {
                    continue;
                }
                if EXTRA_KEYS_TO_STRIP.contains(&field.name.as_str()) {
                    continue;
                }
                if field.name == "fee" && has_svm_fee_guard {
                    continue;
                }
                if let Some(stripped) = field.name.strip_suffix(RAW_LIST_SUFFIX) {
                    let items: Vec<serde_json::Value> = if field.value.is_empty() {
                        Vec::new()
                    } else {
                        field
                            .value
                            .split(',')
                            .map(|s| serde_json::Value::String(s.to_owned()))
                            .collect()
                    };
                    row.insert(stripped.to_owned(), serde_json::Value::Array(items));
                    continue;
                }
                row.insert(field.name, serde_json::Value::String(field.value));
            }
            if row.len() <= 1 {
                return None;
            }
            Some(row)
        })
        .collect();

    DatabaseChangesBlockMessage {
        network: context.network,
        block_num: context.block_num,
        block_hash: context.block_hash,
        timestamp: context.timestamp,
        timestamp_seconds: context.timestamp_seconds,
        module_hash: context.module_hash,
        events,
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;
    use crate::decoder::pb::sf::substreams::sink::database::v1::{
        DatabaseChanges, Field, TableChange,
    };

    fn context() -> BlockContext {
        BlockContext {
            block_num: 350_000_000,
            block_hash: "block-hash".to_owned(),
            timestamp: "2026-05-13 17:00:00".to_owned(),
            timestamp_seconds: 1_778_770_800,
            network: "solana-mainnet".to_owned(),
            module_hash: "deadbeef".to_owned(),
        }
    }

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.to_owned(),
            value: value.to_owned(),
            update_op: 1,
        }
    }

    #[test]
    fn flattens_and_preserves_order() {
        let changes = DatabaseChanges {
            table_changes: vec![
                TableChange {
                    table: "swaps".to_owned(),
                    ordinal: 0,
                    operation: 1,
                    fields: vec![
                        field("input_amount", "1287000000"),
                        field("user", "F2MUEfN1HG5mC5EiUoxhjjc7HpKi4QQnzvipnbGx6Av8"),
                    ],
                    primary_key: None,
                },
                TableChange {
                    table: "swaps".to_owned(),
                    ordinal: 1,
                    operation: 1,
                    fields: vec![field("input_amount", "999")],
                    primary_key: None,
                },
                TableChange {
                    table: "transfers".to_owned(),
                    ordinal: 2,
                    operation: 1,
                    fields: vec![field("amount", "42")],
                    primary_key: None,
                },
            ],
        };

        let mut payload = Vec::new();
        changes.encode(&mut payload).expect("encode");

        let message = decode_database_changes(&payload, context()).expect("decode ok");
        assert_eq!(message.network, "solana-mainnet");
        assert_eq!(message.block_num, 350_000_000);
        assert_eq!(message.block_hash, "block-hash");
        assert_eq!(message.timestamp, "2026-05-13 17:00:00");
        assert_eq!(message.events.len(), 3);
        assert_eq!(message.events[0]["@table"], "swaps");
        assert_eq!(message.events[1]["@table"], "swaps");
        assert_eq!(message.events[2]["@table"], "transfers");
        assert_eq!(message.events[0]["input_amount"], "1287000000");
        assert_eq!(message.events[1]["input_amount"], "999");
        assert_eq!(message.events[2]["amount"], "42");

        // Operation / ordinal / pk / update_op never surface.
        for event in &message.events {
            assert!(event.get("operation").is_none());
            assert!(event.get("ordinal").is_none());
            assert!(event.get("updateOp").is_none());
            assert!(event.get("pk").is_none());
            assert!(event.get("compositePk").is_none());
        }
    }

    #[test]
    fn strips_block_duplicate_fields_from_events() {
        let changes = DatabaseChanges {
            table_changes: vec![TableChange {
                table: "swaps".to_owned(),
                ordinal: 0,
                operation: 1,
                fields: vec![
                    field("block_num", "350000000"),
                    field("block_hash", "should-not-appear"),
                    field("timestamp", "1751210681"),
                    field("minute", "29186844"),
                    field("input_amount", "1287000000"),
                ],
                primary_key: None,
            }],
        };

        let mut payload = Vec::new();
        changes.encode(&mut payload).expect("encode");

        let message = decode_database_changes(&payload, context()).expect("decode ok");
        let event = &message.events[0];
        assert!(event.get("block_num").is_none());
        assert!(event.get("block_hash").is_none());
        assert!(event.get("timestamp").is_none());
        assert!(event.get("minute").is_none());
        assert_eq!(event["input_amount"], "1287000000");
        assert_eq!(event["@table"], "swaps");
    }

    #[test]
    fn drops_rows_that_only_carry_block_header_columns() {
        let changes = DatabaseChanges {
            table_changes: vec![
                // A `blocks` row whose only columns duplicate top-level meta.
                TableChange {
                    table: "blocks".to_owned(),
                    ordinal: 0,
                    operation: 1,
                    fields: vec![
                        field("block_num", "350000000"),
                        field("block_hash", "GsK6..."),
                        field("timestamp", "1751210681"),
                        field("minute", "29186844"),
                    ],
                    primary_key: None,
                },
                // A real row that should still survive.
                TableChange {
                    table: "swaps".to_owned(),
                    ordinal: 1,
                    operation: 1,
                    fields: vec![field("input_amount", "100")],
                    primary_key: None,
                },
            ],
        };
        let mut payload = Vec::new();
        changes.encode(&mut payload).expect("encode");

        let message = decode_database_changes(&payload, context()).expect("decode");
        assert_eq!(
            message.events.len(),
            1,
            "the empty `blocks` row must be dropped"
        );
        assert_eq!(message.events[0]["@table"], "swaps");
    }

    #[test]
    fn serializes_to_expected_top_level_shape() {
        let changes = DatabaseChanges {
            table_changes: vec![TableChange {
                table: "swaps".to_owned(),
                ordinal: 0,
                operation: 1,
                fields: vec![field("input_amount", "100")],
                primary_key: None,
            }],
        };
        let message = normalize_database_changes(changes, context());
        let json = serde_json::to_value(message).expect("serialize");
        assert!(json.get("stream").is_none(), "stream field is removed");
        assert_eq!(json["network"], "solana-mainnet");
        assert_eq!(json["block_num"], 350_000_000);
        assert_eq!(json["block_hash"], "block-hash");
        assert_eq!(json["timestamp"], "2026-05-13 17:00:00");
        assert_eq!(json["module_hash"], "deadbeef");
        assert!(json.get("block").is_none(), "no nested 'block' object");
        assert!(
            json.get("type").is_none(),
            "top-level 'type' must be removed"
        );
        assert!(json.get("changes").is_none(), "renamed to 'events'");
        assert!(json.get("@stream").is_none(), "no @-prefix on meta fields");
        assert!(json["events"].is_array());
        assert!(
            json["events"][0].get("table").is_none(),
            "row-level 'table' must be '@table' (collision guard)"
        );
        assert_eq!(json["events"][0]["@table"], "swaps");
    }

    #[test]
    fn order_preserved_through_to_value_roundtrip() {
        // The server path goes struct -> Value -> String, so this asserts the
        // feature flag actually preserves order through Value::Object too.
        let changes = DatabaseChanges {
            table_changes: vec![TableChange {
                table: "swaps".to_owned(),
                ordinal: 0,
                operation: 1,
                fields: vec![field("zeta", "1"), field("alpha", "2")],
                primary_key: None,
            }],
        };
        let message = normalize_database_changes(changes, context());
        let value = serde_json::to_value(&message).expect("to_value");
        let text = serde_json::to_string(&value).expect("to_string");
        assert!(
            text.find("\"network\"").unwrap() < text.find("\"events\"").unwrap(),
            "events must come after network in {text}"
        );
        assert!(
            text.find("\"@table\"").unwrap() < text.find("\"zeta\"").unwrap(),
            "@table must come first in row: {text}"
        );
    }

    #[test]
    fn fields_serialize_in_struct_declaration_order() {
        let changes = DatabaseChanges {
            table_changes: vec![TableChange {
                table: "swaps".to_owned(),
                ordinal: 0,
                operation: 1,
                fields: vec![field("zeta", "1"), field("alpha", "2")],
                primary_key: None,
            }],
        };
        let message = normalize_database_changes(changes, context());
        let text = serde_json::to_string(&message).expect("serialize");

        // Top-level: network, block_num, block_hash, timestamp, module_hash,
        // events — in that exact order. `events` is last so subscribers see
        // the metadata header before the payload array.
        let order = [
            "\"network\"",
            "\"block_num\"",
            "\"block_hash\"",
            "\"timestamp\"",
            "\"module_hash\"",
            "\"events\"",
        ];
        let mut last = 0;
        for key in order {
            let idx = text
                .find(key)
                .unwrap_or_else(|| panic!("missing key {key} in {text}"));
            assert!(idx >= last, "key {key} appeared out of order in {text}");
            last = idx;
        }

        // Per-row: `@table` first, then field-name insertion order.
        assert!(text.find("\"@table\"").unwrap() < text.find("\"zeta\"").unwrap());
        assert!(text.find("\"zeta\"").unwrap() < text.find("\"alpha\"").unwrap());
    }

    #[test]
    fn strips_evm_extra_metadata_columns() {
        let changes = DatabaseChanges {
            table_changes: vec![TableChange {
                table: "swaps".to_owned(),
                ordinal: 0,
                operation: 1,
                fields: vec![
                    field("tx_hash", "0xabc"),
                    field("tx_from", "0x111"),
                    field("tx_to", "0x222"),
                    field("tx_index", "3"),
                    field("tx_nonce", "7"),
                    field("tx_gas_price", "1000"),
                    field("tx_gas_limit", "21000"),
                    field("tx_gas_used", "21000"),
                    field("tx_value", "0"),
                    field("log_index", "5"),
                    field("log_address", "0xdef"),
                    field("log_block_index", "9"),
                    field("log_ordinal", "100"),
                    field("log_topics", "0x1,0x2"),
                    field("log_data", "0xff"),
                    field("call_caller", "0x999"),
                    field("call_index", "0"),
                    field("call_begin_ordinal", "1"),
                    field("call_end_ordinal", "2"),
                    field("call_address", "0xaaa"),
                    field("call_value", "0"),
                    field("call_gas_consumed", "100"),
                    field("call_gas_limit", "200"),
                    field("call_depth", "1"),
                    field("call_parent_index", "0"),
                    field("call_type", "call"),
                    field("input_amount", "100"),
                ],
                primary_key: None,
            }],
        };
        let message = normalize_database_changes(changes, context());
        let event = &message.events[0];
        // Kept
        assert_eq!(event["tx_hash"], "0xabc");
        assert_eq!(event["tx_from"], "0x111");
        assert_eq!(event["tx_to"], "0x222");
        assert_eq!(event["log_ordinal"], "100");
        assert_eq!(event["log_address"], "0xdef");
        assert_eq!(event["input_amount"], "100");
        // Dropped
        for key in [
            "tx_index",
            "tx_nonce",
            "tx_gas_price",
            "tx_gas_limit",
            "tx_gas_used",
            "tx_value",
            "log_index",
            "log_block_index",
            "log_topics",
            "log_data",
            "call_caller",
            "call_index",
            "call_begin_ordinal",
            "call_end_ordinal",
            "call_address",
            "call_value",
            "call_gas_consumed",
            "call_gas_limit",
            "call_depth",
            "call_parent_index",
            "call_type",
        ] {
            assert!(event.get(key).is_none(), "{key} must be dropped");
        }
    }

    #[test]
    fn strips_svm_extra_metadata_but_keeps_program_id() {
        let changes = DatabaseChanges {
            table_changes: vec![TableChange {
                table: "swaps".to_owned(),
                ordinal: 0,
                operation: 1,
                fields: vec![
                    field("signature", "sig"),
                    field("fee_payer", "payer"),
                    field("fee", "5000"),
                    field("compute_units_consumed", "20000"),
                    field("program_id", "prog"),
                    field("stack_height", "2"),
                    field("input_amount", "1"),
                ],
                primary_key: None,
            }],
        };
        let message = normalize_database_changes(changes, context());
        let event = &message.events[0];
        assert_eq!(event["signature"], "sig");
        assert_eq!(event["fee_payer"], "payer");
        assert_eq!(event["program_id"], "prog");
        assert_eq!(event["input_amount"], "1");
        assert!(event.get("fee").is_none());
        assert!(event.get("compute_units_consumed").is_none());
        assert!(event.get("stack_height").is_none());
    }

    #[test]
    fn keeps_fee_on_evm_swap_fee_table_without_cu_guard() {
        // EVM `swap_fee` rows carry a real `fee` value (protocol fee) and no
        // `compute_units_consumed`. The conditional drop must not touch it.
        let changes = DatabaseChanges {
            table_changes: vec![TableChange {
                table: "swap_fee".to_owned(),
                ordinal: 0,
                operation: 1,
                fields: vec![
                    field("protocol", "uniswap_v3"),
                    field("pool", "0xpool"),
                    field("fee", "3000"),
                ],
                primary_key: None,
            }],
        };
        let message = normalize_database_changes(changes, context());
        assert_eq!(message.events[0]["fee"], "3000");
    }

    #[test]
    fn splits_raw_suffix_into_array_and_renames() {
        let changes = DatabaseChanges {
            table_changes: vec![TableChange {
                table: "swaps".to_owned(),
                ordinal: 0,
                operation: 1,
                fields: vec![
                    field("signers_raw", "alice,bob,carol"),
                    field("input_amount", "1"),
                ],
                primary_key: None,
            }],
        };
        let message = normalize_database_changes(changes, context());
        let event = &message.events[0];
        assert!(event.get("signers_raw").is_none(), "`_raw` key removed");
        let signers = event["signers"].as_array().expect("array");
        assert_eq!(signers.len(), 3);
        assert_eq!(signers[0], "alice");
        assert_eq!(signers[2], "carol");
    }

    #[test]
    fn empty_raw_field_becomes_empty_array() {
        let changes = DatabaseChanges {
            table_changes: vec![TableChange {
                table: "swaps".to_owned(),
                ordinal: 0,
                operation: 1,
                fields: vec![field("signers_raw", ""), field("input_amount", "1")],
                primary_key: None,
            }],
        };
        let message = normalize_database_changes(changes, context());
        let signers = message.events[0]["signers"].as_array().expect("array");
        assert!(signers.is_empty());
    }

    #[test]
    fn rejects_invalid_payload() {
        let error = decode_database_changes(&[0xff, 0xff], context()).expect_err("rejects");
        assert!(matches!(error, DecodeError::DatabaseChanges(_)));
    }
}
