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
}

use pb::dex::swaps::v1::{Events, Protocol, Transaction};

#[derive(Debug, Clone)]
pub struct BlockContext {
    pub number: u64,
    pub id: String,
    pub cursor: String,
    pub final_block_height: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("failed to decode dex.swaps.v1.Events: {0}")]
    Events(#[from] prost::DecodeError),
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SwapMessage {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub block: BlockRef,
    pub cursor: String,
    pub transaction: TransactionRef,
    pub swap: SwapRef,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct BlockRef {
    pub number: u64,
    pub id: String,
    pub final_block_height: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TransactionRef {
    pub signature: String,
    pub fee_payer: String,
    pub signers: Vec<String>,
    pub fee: String,
    pub compute_units_consumed: String,
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
    pub input_amount: String,
    pub output_mint: String,
    pub output_amount: String,
}

pub fn decode_swaps(
    payload: &[u8],
    context: BlockContext,
) -> Result<Vec<SwapMessage>, DecodeError> {
    let events = Events::decode(payload)?;
    Ok(normalize_events(events, context))
}

pub fn normalize_events(events: Events, context: BlockContext) -> Vec<SwapMessage> {
    events
        .transactions
        .into_iter()
        .flat_map(|transaction| normalize_transaction(transaction, &context))
        .collect()
}

fn normalize_transaction(transaction: Transaction, context: &BlockContext) -> Vec<SwapMessage> {
    let transaction_ref = TransactionRef {
        signature: encode_bytes(&transaction.signature),
        fee_payer: encode_bytes(&transaction.fee_payer),
        signers: transaction
            .signers
            .iter()
            .map(|signer| encode_bytes(signer))
            .collect(),
        fee: transaction.fee.to_string(),
        compute_units_consumed: transaction.compute_units_consumed.to_string(),
    };

    transaction
        .swaps
        .into_iter()
        .map(|swap| SwapMessage {
            kind: "swap",
            block: BlockRef {
                number: context.number,
                id: context.id.clone(),
                final_block_height: context.final_block_height,
            },
            cursor: context.cursor.clone(),
            transaction: transaction_ref.clone(),
            swap: SwapRef {
                protocol: protocol_name(swap.protocol),
                program_id: encode_bytes(&swap.program_id),
                stack_height: swap.stack_height,
                amm: encode_bytes(&swap.amm),
                amm_pool: encode_bytes(&swap.amm_pool),
                user: encode_bytes(&swap.user),
                input_mint: encode_bytes(&swap.input_mint),
                input_amount: swap.input_amount.to_string(),
                output_mint: encode_bytes(&swap.output_mint),
                output_amount: swap.output_amount.to_string(),
            },
        })
        .collect()
}

fn protocol_name(protocol: i32) -> String {
    Protocol::try_from(protocol)
        .map(|protocol| protocol.as_str_name().to_owned())
        .unwrap_or_else(|_| format!("PROTOCOL_UNKNOWN_{protocol}"))
}

fn encode_bytes(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        String::new()
    } else {
        bs58::encode(bytes).into_string()
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;
    use crate::decoder::pb::dex::swaps::v1::{Swap, Transaction};

    #[test]
    fn decodes_and_flattens_swap_events() {
        let events = Events {
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

        let messages = decode_swaps(&payload, context()).expect("events decode");

        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].kind, "swap");
        assert_eq!(messages[0].block.number, 123);
        assert_eq!(messages[0].cursor, "cursor-123");
        assert_eq!(
            messages[0].transaction.signature,
            bs58::encode(bytes(1)).into_string()
        );
        assert_eq!(
            messages[0].transaction.fee_payer,
            bs58::encode(bytes(2)).into_string()
        );
        assert_eq!(messages[0].swap.protocol, "PROTOCOL_RAYDIUM_AMM_V4");
        assert_eq!(
            messages[0].swap.program_id,
            bs58::encode(bytes(4)).into_string()
        );
        assert_eq!(messages[0].swap.amm, bs58::encode(bytes(5)).into_string());
        assert_eq!(
            messages[0].swap.amm_pool,
            bs58::encode(bytes(6)).into_string()
        );
        assert_eq!(messages[0].swap.input_amount, u64::MAX.to_string());
        assert_eq!(messages[0].swap.output_amount, "42");
        assert_eq!(messages[1].swap.protocol, "PROTOCOL_JUPITER_V6");
    }

    #[test]
    fn serializes_amounts_as_strings() {
        let events = Events {
            transactions: vec![Transaction {
                swaps: vec![Swap {
                    input_amount: u64::MAX,
                    output_amount: u64::MAX - 1,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let message = normalize_events(events, context()).remove(0);
        let json = serde_json::to_value(message).expect("message serializes");

        assert_eq!(json["swap"]["input_amount"], u64::MAX.to_string());
        assert_eq!(json["swap"]["output_amount"], (u64::MAX - 1).to_string());
    }

    #[test]
    fn rejects_invalid_events_payload() {
        let error = decode_swaps(&[0xff, 0xff], context()).expect_err("payload rejects");
        assert!(matches!(error, DecodeError::Events(_)));
    }

    fn context() -> BlockContext {
        BlockContext {
            number: 123,
            id: "block-id".to_owned(),
            cursor: "cursor-123".to_owned(),
            final_block_height: 120,
        }
    }

    fn bytes(seed: u8) -> Vec<u8> {
        vec![seed; 32]
    }
}
