use prost::Message;
use serde::Serialize;

pub mod pb {
    pub mod dex {
        pub mod swaps {
            pub mod v1 {
                tonic::include_proto!("dex.swaps.v1");
            }
        }
    }

    pub mod solana {
        pub mod spl {
            pub mod token {
                pub mod v1 {
                    tonic::include_proto!("solana.spl.token.v1");
                }
            }
        }
    }

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

use pb::{
    dex::swaps::v1::{Events as SwapEvents, Protocol, Swap, Transaction as SwapTransaction},
    sf::substreams::sink::database::v1::DatabaseChanges,
    solana::spl::token::v1::{
        Events as TransferEvents, Instruction, Transaction as TransferTransaction, Transfer,
        instruction,
    },
};

#[derive(Debug, Clone)]
pub struct BlockContext {
    pub block_num: u64,
    pub block_hash: String,
    pub timestamp: String,
    pub network: String,
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("failed to decode dex.swaps.v1.Events: {0}")]
    SwapEvents(#[source] prost::DecodeError),

    #[error("failed to decode solana.spl.token.v1.Events: {0}")]
    TransferEvents(#[source] prost::DecodeError),

    #[error("failed to decode sf.substreams.sink.database.v1.DatabaseChanges: {0}")]
    DatabaseChanges(#[source] prost::DecodeError),

    #[error("failed to serialize decoded payload: {0}")]
    Serialize(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SwapBlockMessage {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub network: String,
    pub block: BlockRef,
    pub transactions: Vec<TransactionRef>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct BlockRef {
    pub number: u64,
    pub hash: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TransactionRef {
    pub signature: String,
    pub fee_payer: String,
    pub signers: Vec<String>,
    pub fee: u64,
    pub compute_units_consumed: u64,
    pub swaps: Vec<SwapRef>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TransferBlockMessage {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub network: String,
    pub block: BlockRef,
    pub transactions: Vec<TransferTransactionRef>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TransferTransactionRef {
    pub signature: String,
    pub fee_payer: String,
    pub signers: Vec<String>,
    pub fee: u64,
    pub compute_units_consumed: u64,
    pub transfers: Vec<TransferRef>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TransferRef {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub program_id: String,
    pub stack_height: u32,
    pub is_root: bool,
    pub authority: String,
    pub multisig_authority: Vec<String>,
    pub source: String,
    pub destination: String,
    pub amount: u64,
    pub mint: String,
    pub decimals: Option<u32>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SwapRef {
    pub protocol: String,
    pub program_id: String,
    pub stack_height: u32,
    pub amm: String,
    pub amm_pool: String,
    pub user: String,
    pub input_mint: String,
    pub input_amount: u64,
    pub output_mint: String,
    pub output_amount: u64,
}

pub fn decode_swaps(
    payload: &[u8],
    context: BlockContext,
) -> Result<SwapBlockMessage, DecodeError> {
    let events = SwapEvents::decode(payload).map_err(DecodeError::SwapEvents)?;
    Ok(normalize_events(events, context))
}

pub fn normalize_events(events: SwapEvents, context: BlockContext) -> SwapBlockMessage {
    let transactions = events
        .transactions
        .into_iter()
        .map(normalize_transaction)
        .collect();

    SwapBlockMessage {
        kind: "swaps",
        network: context.network,
        block: BlockRef {
            number: context.block_num,
            hash: context.block_hash,
            timestamp: context.timestamp,
        },
        transactions,
    }
}

fn normalize_transaction(transaction: SwapTransaction) -> TransactionRef {
    TransactionRef {
        signature: encode_bytes(&transaction.signature),
        fee_payer: encode_bytes(&transaction.fee_payer),
        signers: transaction
            .signers
            .iter()
            .map(|signer| encode_bytes(signer))
            .collect(),
        fee: transaction.fee,
        compute_units_consumed: transaction.compute_units_consumed,
        swaps: transaction.swaps.into_iter().map(normalize_swap).collect(),
    }
}

pub fn decode_transfers(
    payload: &[u8],
    context: BlockContext,
) -> Result<TransferBlockMessage, DecodeError> {
    let events = TransferEvents::decode(payload).map_err(DecodeError::TransferEvents)?;
    Ok(normalize_transfer_events(events, context))
}

pub fn normalize_transfer_events(
    events: TransferEvents,
    context: BlockContext,
) -> TransferBlockMessage {
    let transactions = events
        .transactions
        .into_iter()
        .map(normalize_transfer_transaction)
        .collect();

    TransferBlockMessage {
        kind: "transfers",
        network: context.network,
        block: BlockRef {
            number: context.block_num,
            hash: context.block_hash,
            timestamp: context.timestamp,
        },
        transactions,
    }
}

fn normalize_transfer_transaction(transaction: TransferTransaction) -> TransferTransactionRef {
    TransferTransactionRef {
        signature: encode_bytes(&transaction.signature),
        fee_payer: encode_bytes(&transaction.fee_payer),
        signers: transaction
            .signers
            .iter()
            .map(|signer| encode_bytes(signer))
            .collect(),
        fee: transaction.fee,
        compute_units_consumed: transaction.compute_units_consumed,
        transfers: transaction
            .instructions
            .into_iter()
            .filter_map(normalize_transfer_instruction)
            .collect(),
    }
}

fn normalize_transfer_instruction(instruction: Instruction) -> Option<TransferRef> {
    let (kind, transfer) = match instruction.instruction? {
        instruction::Instruction::Transfer(transfer) => ("transfer", transfer),
        instruction::Instruction::Mint(transfer) => ("mint", transfer),
        instruction::Instruction::Burn(transfer) => ("burn", transfer),
    };

    Some(normalize_transfer(
        kind,
        transfer,
        instruction.program_id,
        instruction.stack_height,
        instruction.is_root,
    ))
}

fn normalize_transfer(
    kind: &'static str,
    transfer: Transfer,
    program_id: Vec<u8>,
    stack_height: u32,
    is_root: bool,
) -> TransferRef {
    TransferRef {
        kind,
        program_id: encode_bytes(&program_id),
        stack_height,
        is_root,
        authority: encode_bytes(&transfer.authority),
        multisig_authority: transfer
            .multisig_authority
            .iter()
            .map(|authority| encode_bytes(authority))
            .collect(),
        source: encode_bytes(&transfer.source),
        destination: encode_bytes(&transfer.destination),
        amount: transfer.amount,
        mint: encode_bytes(&transfer.mint),
        decimals: transfer.decimals,
    }
}

fn normalize_swap(swap: Swap) -> SwapRef {
    SwapRef {
        protocol: protocol_name(swap.protocol),
        program_id: encode_bytes(&swap.program_id),
        stack_height: swap.stack_height,
        amm: encode_bytes(&swap.amm),
        amm_pool: encode_bytes(&swap.amm_pool),
        user: encode_bytes(&swap.user),
        input_mint: encode_bytes(&swap.input_mint),
        input_amount: swap.input_amount,
        output_mint: encode_bytes(&swap.output_mint),
        output_amount: swap.output_amount,
    }
}

fn protocol_name(protocol: i32) -> String {
    Protocol::try_from(protocol)
        .map(|protocol| {
            protocol
                .as_str_name()
                .trim_start_matches("PROTOCOL_")
                .to_ascii_lowercase()
        })
        .unwrap_or_else(|_| format!("unknown_{protocol}"))
}

fn encode_bytes(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        String::new()
    } else {
        bs58::encode(bytes).into_string()
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DatabaseChangesBlockMessage {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub network: String,
    pub block: BlockRef,
    pub changes: Vec<serde_json::Map<String, serde_json::Value>>,
}

/// Decode a `sf.substreams.sink.database.v1.DatabaseChanges` payload into a flat
/// block message. Each row in `table_changes` becomes one object containing the
/// `table` name plus every `field.name -> field.value` pair. The `ordinal`,
/// `operation`, `pk`/`composite_pk`, and `update_op` fields are intentionally
/// dropped — values are passed through as-is (already strings on the wire).
/// Order of rows mirrors the input `table_changes` order.
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
    let rows = changes
        .table_changes
        .into_iter()
        .map(|change| {
            let mut row = serde_json::Map::with_capacity(change.fields.len() + 1);
            row.insert("table".to_owned(), serde_json::Value::String(change.table));
            for field in change.fields {
                row.insert(field.name, serde_json::Value::String(field.value));
            }
            row
        })
        .collect();

    DatabaseChangesBlockMessage {
        kind: "database_changes",
        network: context.network,
        block: BlockRef {
            number: context.block_num,
            hash: context.block_hash,
            timestamp: context.timestamp,
        },
        changes: rows,
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;
    use crate::decoder::pb::{
        dex::swaps::v1::{Events as SwapEvents, Swap, Transaction},
        solana::spl::token::v1::{
            Events as TransferEvents, Instruction as TransferInstruction,
            Transaction as TokenTransaction, Transfer as TokenTransfer, instruction,
        },
    };

    #[test]
    fn decodes_swap_events_into_one_block_message() {
        let events = SwapEvents {
            transactions: vec![Transaction {
                signature: bytes(1),
                fee_payer: bytes(2),
                signers: vec![bytes(2), bytes(3)],
                fee: 5_000,
                compute_units_consumed: 77_777,
                swaps: vec![
                    Swap {
                        protocol: Protocol::RaydiumAmmV4 as i32,
                        program_id: bytes(4),
                        stack_height: 2,
                        amm: bytes(5),
                        amm_pool: bytes(6),
                        user: bytes(7),
                        input_mint: bytes(8),
                        input_amount: u64::MAX,
                        output_mint: bytes(9),
                        output_amount: 42,
                    },
                    Swap {
                        protocol: Protocol::JupiterV6 as i32,
                        program_id: bytes(10),
                        stack_height: 1,
                        amm: bytes(11),
                        amm_pool: bytes(12),
                        user: bytes(13),
                        input_mint: bytes(14),
                        input_amount: 100,
                        output_mint: bytes(15),
                        output_amount: 200,
                    },
                ],
            }],
        };

        let mut payload = Vec::new();
        events.encode(&mut payload).expect("events encode");

        let message = decode_swaps(&payload, context()).expect("events decode");

        assert_eq!(message.kind, "swaps");
        assert_eq!(message.network, "solana-mainnet");
        assert_eq!(message.block.number, 123);
        assert_eq!(message.block.hash, "block-hash");
        assert_eq!(message.block.timestamp, "2026-05-12 17:00:00");
        assert_eq!(message.transactions.len(), 1);
        assert_eq!(message.transactions[0].swaps.len(), 2);
        assert_eq!(
            message.transactions[0].signature,
            bs58::encode(bytes(1)).into_string()
        );
        assert_eq!(
            message.transactions[0].fee_payer,
            bs58::encode(bytes(2)).into_string()
        );
        assert_eq!(message.transactions[0].swaps[0].protocol, "raydium_amm_v4");
        assert_eq!(
            message.transactions[0].swaps[0].program_id,
            bs58::encode(bytes(4)).into_string()
        );
        assert_eq!(message.transactions[0].swaps[0].stack_height, 2);
        assert_eq!(
            message.transactions[0].swaps[0].amm,
            bs58::encode(bytes(5)).into_string()
        );
        assert_eq!(
            message.transactions[0].swaps[0].amm_pool,
            bs58::encode(bytes(6)).into_string()
        );
        assert_eq!(message.transactions[0].swaps[0].input_amount, u64::MAX);
        assert_eq!(message.transactions[0].swaps[0].output_amount, 42);
        assert_eq!(message.transactions[0].swaps[1].protocol, "jupiter_v6");
    }

    #[test]
    fn serializes_block_message_with_all_swaps() {
        let events = SwapEvents {
            transactions: vec![Transaction {
                swaps: vec![Swap {
                    input_amount: u64::MAX,
                    output_amount: u64::MAX - 1,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let message = normalize_events(events, context());
        let json = serde_json::to_value(message).expect("message serializes");

        assert_eq!(json["type"], "swaps");
        assert_eq!(json["block"]["number"], 123);
        assert_eq!(json["block"]["hash"], "block-hash");
        assert_eq!(json["block"]["timestamp"], "2026-05-12 17:00:00");
        assert_eq!(
            json["transactions"]
                .as_array()
                .expect("transactions array")
                .len(),
            1
        );
        assert_eq!(
            json["transactions"][0]["swaps"][0]["input_amount"],
            u64::MAX
        );
        assert_eq!(
            json["transactions"][0]["swaps"][0]["output_amount"],
            u64::MAX - 1
        );
        assert!(json.get("cursor").is_none());
        assert!(json.get("transaction").is_none());
        assert!(json.get("instruction").is_none());
        assert!(json.get("swap").is_none());
        assert!(json.get("swaps").is_none());
    }

    #[test]
    fn rejects_invalid_events_payload() {
        let error = decode_swaps(&[0xff, 0xff], context()).expect_err("payload rejects");
        assert!(matches!(error, DecodeError::SwapEvents(_)));
    }

    #[test]
    fn decodes_transfer_events_into_one_block_message() {
        let events = TransferEvents {
            transactions: vec![TokenTransaction {
                signature: bytes(1),
                fee_payer: bytes(2),
                signers: vec![bytes(2), bytes(3)],
                fee: 5_000,
                compute_units_consumed: 77_777,
                instructions: vec![
                    token_instruction(
                        bytes(4),
                        1,
                        true,
                        instruction::Instruction::Transfer(token_transfer(5, 100, Some(6))),
                    ),
                    token_instruction(
                        bytes(7),
                        2,
                        false,
                        instruction::Instruction::Mint(token_transfer(8, 200, None)),
                    ),
                    token_instruction(
                        bytes(9),
                        3,
                        false,
                        instruction::Instruction::Burn(token_transfer(10, 300, Some(9))),
                    ),
                    TransferInstruction {
                        program_id: bytes(11),
                        stack_height: 4,
                        is_root: false,
                        instruction: None,
                    },
                ],
            }],
        };

        let mut payload = Vec::new();
        events.encode(&mut payload).expect("events encode");

        let message = decode_transfers(&payload, context()).expect("events decode");

        assert_eq!(message.kind, "transfers");
        assert_eq!(message.network, "solana-mainnet");
        assert_eq!(message.block.number, 123);
        assert_eq!(message.block.hash, "block-hash");
        assert_eq!(message.transactions.len(), 1);
        assert_eq!(message.transactions[0].transfers.len(), 3);
        assert_eq!(
            message.transactions[0].signature,
            bs58::encode(bytes(1)).into_string()
        );
        assert_eq!(message.transactions[0].transfers[0].kind, "transfer");
        assert_eq!(message.transactions[0].transfers[0].is_root, true);
        assert_eq!(
            message.transactions[0].transfers[0].program_id,
            bs58::encode(bytes(4)).into_string()
        );
        assert_eq!(
            message.transactions[0].transfers[0].authority,
            bs58::encode(bytes(5)).into_string()
        );
        assert_eq!(message.transactions[0].transfers[0].amount, 100);
        assert_eq!(message.transactions[0].transfers[0].decimals, Some(6));
        assert_eq!(message.transactions[0].transfers[1].kind, "mint");
        assert_eq!(message.transactions[0].transfers[1].decimals, None);
        assert_eq!(message.transactions[0].transfers[2].kind, "burn");
    }

    #[test]
    fn serializes_transfer_block_message() {
        let events = TransferEvents {
            transactions: vec![TokenTransaction {
                instructions: vec![token_instruction(
                    bytes(1),
                    1,
                    true,
                    instruction::Instruction::Transfer(token_transfer(2, u64::MAX, Some(9))),
                )],
                ..Default::default()
            }],
        };

        let message = normalize_transfer_events(events, context());
        let json = serde_json::to_value(message).expect("message serializes");

        assert_eq!(json["type"], "transfers");
        assert_eq!(json["block"]["number"], 123);
        assert_eq!(json["transactions"].as_array().unwrap().len(), 1);
        assert_eq!(json["transactions"][0]["transfers"][0]["type"], "transfer");
        assert_eq!(json["transactions"][0]["transfers"][0]["amount"], u64::MAX);
        assert_eq!(json["transactions"][0]["transfers"][0]["decimals"], 9);
        assert!(json.get("cursor").is_none());
    }

    #[test]
    fn rejects_invalid_transfer_events_payload() {
        let error = decode_transfers(&[0xff, 0xff], context()).expect_err("payload rejects");
        assert!(matches!(error, DecodeError::TransferEvents(_)));
    }

    #[test]
    fn flattens_database_changes_and_preserves_order() {
        use crate::decoder::pb::sf::substreams::sink::database::v1::{
            DatabaseChanges, Field, TableChange,
        };

        let changes = DatabaseChanges {
            table_changes: vec![
                TableChange {
                    table: "swaps".to_owned(),
                    ordinal: 0,
                    operation: 1,
                    fields: vec![
                        Field {
                            name: "block_num".to_owned(),
                            value: "350000000".to_owned(),
                            update_op: 1,
                        },
                        Field {
                            name: "input_amount".to_owned(),
                            value: "1287000000".to_owned(),
                            update_op: 1,
                        },
                    ],
                    primary_key: None,
                },
                TableChange {
                    table: "swaps".to_owned(),
                    ordinal: 1,
                    operation: 1,
                    fields: vec![Field {
                        name: "block_num".to_owned(),
                        value: "350000000".to_owned(),
                        update_op: 1,
                    }],
                    primary_key: None,
                },
                TableChange {
                    table: "transfers".to_owned(),
                    ordinal: 2,
                    operation: 1,
                    fields: vec![Field {
                        name: "amount".to_owned(),
                        value: "42".to_owned(),
                        update_op: 1,
                    }],
                    primary_key: None,
                },
            ],
        };

        let mut payload = Vec::new();
        changes.encode(&mut payload).expect("changes encode");

        let message = decode_database_changes(&payload, context()).expect("decode ok");
        assert_eq!(message.kind, "database_changes");
        assert_eq!(message.network, "solana-mainnet");
        assert_eq!(message.block.number, 123);
        assert_eq!(message.changes.len(), 3);

        // Order preserved exactly.
        assert_eq!(message.changes[0]["table"], "swaps");
        assert_eq!(message.changes[1]["table"], "swaps");
        assert_eq!(message.changes[2]["table"], "transfers");

        // Flattened name:value, no operation / ordinal / pk / updateOp.
        assert_eq!(message.changes[0]["block_num"], "350000000");
        assert_eq!(message.changes[0]["input_amount"], "1287000000");
        assert!(message.changes[0].get("ordinal").is_none());
        assert!(message.changes[0].get("operation").is_none());
        assert!(message.changes[0].get("updateOp").is_none());
        assert!(message.changes[0].get("pk").is_none());
        assert!(message.changes[0].get("compositePk").is_none());

        assert_eq!(message.changes[2]["amount"], "42");
    }

    #[test]
    fn rejects_invalid_database_changes_payload() {
        let error = decode_database_changes(&[0xff, 0xff], context()).expect_err("rejects");
        assert!(matches!(error, DecodeError::DatabaseChanges(_)));
    }

    fn context() -> BlockContext {
        BlockContext {
            block_num: 123,
            block_hash: "block-hash".to_owned(),
            timestamp: "2026-05-12 17:00:00".to_owned(),
            network: "solana-mainnet".to_owned(),
        }
    }

    fn bytes(seed: u8) -> Vec<u8> {
        vec![seed; 32]
    }

    fn token_instruction(
        program_id: Vec<u8>,
        stack_height: u32,
        is_root: bool,
        instruction: instruction::Instruction,
    ) -> TransferInstruction {
        TransferInstruction {
            program_id,
            stack_height,
            is_root,
            instruction: Some(instruction),
        }
    }

    fn token_transfer(seed: u8, amount: u64, decimals: Option<u32>) -> TokenTransfer {
        TokenTransfer {
            authority: bytes(seed),
            multisig_authority: vec![bytes(seed + 1)],
            source: bytes(seed + 2),
            destination: bytes(seed + 3),
            amount,
            mint: bytes(seed + 4),
            decimals,
        }
    }
}
