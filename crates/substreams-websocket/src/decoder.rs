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

#[derive(Debug, Clone)]
pub struct BlockContext {
    pub block_num: u64,
    pub block_hash: String,
    pub timestamp: String,
    pub network: String,
    pub cursor: String,
    pub module_hash: String,
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("failed to decode sf.substreams.sink.database.v1.DatabaseChanges: {0}")]
    DatabaseChanges(#[source] prost::DecodeError),

    #[error("failed to serialize decoded payload: {0}")]
    Serialize(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DatabaseChangesBlockMessage {
    /// Stream display name (the TOML `[[streams]].name`). Forms the
    /// `(stream, network)` identity WebSocket subscribers register against.
    pub stream: String,
    pub network: String,
    pub block_num: u64,
    pub block_hash: String,
    pub timestamp: String,
    /// Opaque Substreams cursor for the block being delivered. Subscribers may
    /// persist this and reconnect to a Substreams endpoint directly using it
    /// to resume from this exact position. This server does not replay.
    pub cursor: String,
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
    stream: &str,
    payload: &[u8],
    context: BlockContext,
) -> Result<DatabaseChangesBlockMessage, DecodeError> {
    let changes = DatabaseChanges::decode(payload).map_err(DecodeError::DatabaseChanges)?;
    Ok(normalize_database_changes(stream, changes, context))
}

pub fn normalize_database_changes(
    stream: &str,
    changes: DatabaseChanges,
    context: BlockContext,
) -> DatabaseChangesBlockMessage {
    let events = changes
        .table_changes
        .into_iter()
        .map(|change| {
            let mut row = serde_json::Map::with_capacity(change.fields.len() + 1);
            row.insert("@table".to_owned(), serde_json::Value::String(change.table));
            for field in change.fields {
                if BLOCK_KEYS_TO_STRIP.contains(&field.name.as_str()) {
                    continue;
                }
                row.insert(field.name, serde_json::Value::String(field.value));
            }
            row
        })
        .collect();

    DatabaseChangesBlockMessage {
        stream: stream.to_owned(),
        network: context.network,
        block_num: context.block_num,
        block_hash: context.block_hash,
        timestamp: context.timestamp,
        cursor: context.cursor,
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
            network: "solana-mainnet".to_owned(),
            cursor: "cur-xyz".to_owned(),
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

        let message = decode_database_changes("swaps", &payload, context()).expect("decode ok");
        assert_eq!(message.stream, "swaps");
        assert_eq!(message.network, "solana-mainnet");
        assert_eq!(message.block_num, 350_000_000);
        assert_eq!(message.block_hash, "block-hash");
        assert_eq!(message.timestamp, "2026-05-13 17:00:00");
        assert_eq!(message.cursor, "cur-xyz");
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

        let message = decode_database_changes("swaps", &payload, context()).expect("decode ok");
        let event = &message.events[0];
        assert!(event.get("block_num").is_none());
        assert!(event.get("block_hash").is_none());
        assert!(event.get("timestamp").is_none());
        assert!(event.get("minute").is_none());
        assert_eq!(event["input_amount"], "1287000000");
        assert_eq!(event["@table"], "swaps");
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
        let message = normalize_database_changes("swaps", changes, context());
        let json = serde_json::to_value(message).expect("serialize");
        assert_eq!(json["stream"], "swaps");
        assert_eq!(json["network"], "solana-mainnet");
        assert_eq!(json["block_num"], 350_000_000);
        assert_eq!(json["block_hash"], "block-hash");
        assert_eq!(json["timestamp"], "2026-05-13 17:00:00");
        assert_eq!(json["cursor"], "cur-xyz");
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
        let message = normalize_database_changes("swaps", changes, context());
        let value = serde_json::to_value(&message).expect("to_value");
        let text = serde_json::to_string(&value).expect("to_string");
        assert!(
            text.find("\"stream\"").unwrap() < text.find("\"events\"").unwrap(),
            "events must come after stream in {text}"
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
        let message = normalize_database_changes("swaps", changes, context());
        let text = serde_json::to_string(&message).expect("serialize");

        // Top-level: stream, network, block_num, block_hash, timestamp,
        // cursor, module_hash, events — in that exact order. `events` is last
        // so subscribers see the metadata header before the payload array.
        let order = [
            "\"stream\"",
            "\"network\"",
            "\"block_num\"",
            "\"block_hash\"",
            "\"timestamp\"",
            "\"cursor\"",
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
    fn rejects_invalid_payload() {
        let error = decode_database_changes("x", &[0xff, 0xff], context()).expect_err("rejects");
        assert!(matches!(error, DecodeError::DatabaseChanges(_)));
    }
}
