# Event filters

Per-subscription field filters on the `events[]` array. Subscribers only receive events whose columns match their filter; if no events match in a given block, the block is skipped entirely for that subscriber.

## Filter shape

A filter is an **SQE expression string** (StreamingFast Substreams Query Expression — the same language as Firehose `substreams run -t`), not a JSON object.

```
protocol:raydium_cpmm && user:F2MUEfN1HG5mC5EiUoxhjjc7HpKi4QQnzvipnbGx6Av8
```

### Grammar

```
maker:0xW                          field equals value (case-insensitive)
maker:0xW || taker:0xW             OR — wallet as maker OR taker
protocol:clob && maker:0xW         AND (whitespace also means AND: `protocol:clob maker:0xW`)
(maker:0xW || taker:0xW) && !amm:0xdead   grouping + negation
0xWALLET                           bare term: matches when ANY column equals 0xWALLET
"two words"  or  label:'a b'       quote values containing spaces or ( ) | & ' "
```

- **`field:value` — string equality only.** **ASCII-case-insensitive** on that `events[*]` column, so a checksummed or lowercased EVM address both match (supply the exact value for case-significant data like Solana base58 keys). No regex, no range, no substring.
- **bare `value` (no `field:`).** Matches when **any** string column of the event equals it (e.g. `0xWALLET`). Great for "this wallet in any role".
- **Operators:** `||` (OR), `&&` or whitespace (AND), `!` (NOT), `( )` (grouping). `&&` binds tighter than `||`.
- **Quoting.** Use `'…'` or `"…"` around values containing spaces or `( ) | & ' "`.
- **OR across columns works.** `tx_from:0xW || maker:0xW || taker:0xW` (or just bare `0xW`) matches a wallet in any role.
- **Missing fields are a miss.** Filtering on `protocol` against an event that has no `protocol` column drops the event.
- **Schema-agnostic.** The server does not know the swap / transfer schema. Any column name works.
- **Top-level fields are not filterable.** `block_num`, `network`, `module_hash`, etc. always pass through. The filter applies only to columns inside `events[*]`.

## Wire — connect

URL-encoded SQE expression on the WebSocket upgrade. The query param is `?filter=` (alias `?sqe=`):

```
# ?filter=protocol:raydium_cpmm
ws://host/ws/solana-mainnet@swaps?filter=protocol%3Araydium_cpmm
# ?sqe=maker:0xW || taker:0xW
ws://host/stream?streams=solana-mainnet@swaps&sqe=maker%3A0xW%20%7C%7C%20taker%3A0xW
```

The filter applies to **every selector** in the URL, including wildcards: `EventFilterSet` resolves wildcard selectors (`*@*`, `*@swaps`, `solana-mainnet@*`) against each outgoing `(network, table)` at broadcast time, so `/ws/*@*?filter=...` filters every channel.

For different filters per channel on one connection, use the live `SET_FILTER` command.

## Wire — live commands

### `SET_FILTER`

```json
// request
{ "method": "SET_FILTER",
  "params": ["solana-mainnet@swaps", "protocol:raydium_cpmm && user:F2MUE…"],
  "id": 1 }

// reply (accept)
{ "result": null, "id": 1 }
// reply (reject — previous filter left unchanged)
{ "error": "…", "id": 1 }
```

- `params[0]` = `network@table` selector. Wildcards accepted on either side: `*@*`, `<network>@*`, `*@<table>`. Filters with wildcard selectors apply to every matching outgoing `(network, table)`. Multiple stored filters (exact + wildcard) compose with AND semantics — each must pass.
- `params[1]` = SQE expression **string** (not an object).
- **Replaces** any existing filter for the selector — it does not accumulate. To combine conditions, send one expression with `||` rather than multiple `SET_FILTER` calls.

### `CLEAR_FILTER`

```json
// request
{ "method": "CLEAR_FILTER", "params": ["solana-mainnet@swaps"], "id": 2 }

// reply
{ "result": null, "id": 2 }
```

Drops the filter for the listed selectors. Silently ignores selectors without a filter.

### `LIST_FILTERS`

```json
// request
{ "method": "LIST_FILTERS", "id": 3 }

// reply
{ "result": { "solana-mainnet@swaps": "protocol:raydium_cpmm" }, "id": 3 }
```

Returns the current selector → expression-string map. An empty `{}` means no filter is active.

## Bounds

| Variable | Default | Notes |
|----------|---------|-------|
| `SUBSTREAMS_WEBSOCKET_MAX_FILTER_FIELDS` | `16` | Max distinct field names in one expression. |
| `SUBSTREAMS_WEBSOCKET_MAX_FILTER_VALUES` | `512` | Max total number of terms in the expression. |

The values cap is the **total number of terms across the whole expression**, not per field. Over-cap, a parse error, or a non-string `params[1]` returns an `error` reply (e.g. `filter exceeds max terms (total across the expression): 192 > 64`) and **leaves the previous filter in place**; the socket stays open, so always read the reply — an ignored `error` looks exactly like the filter doing nothing.

## Common filter shapes

Examples per table — the server doesn't enforce these, they're just operator-friendly suggestions matching the columns commonly present.

### SVM swaps (`solana-mainnet@swaps`)

```
# Raydium CPMM swaps by a specific user
protocol:raydium_cpmm && user:F2MUEfN1HG5mC5EiUoxhjjc7HpKi4QQnzvipnbGx6Av8

# A wallet in any swap role (user OR fee_payer), or just the bare address
user:F2MUE… || fee_payer:F2MUE…
F2MUEfN1HG5mC5EiUoxhjjc7HpKi4QQnzvipnbGx6Av8

# WSOL on either leg of the trade
input_mint:So11111111111111111111111111111111111111112 || output_mint:So11111111111111111111111111111111111111112
```

### SVM SPL transfers (`solana-mainnet@spl_transfers`)

```
# A wallet as source OR destination of an SPL transfer
source:<wallet> || destination:<wallet>

# A specific mint, restricted to the SPL Token program
mint:<mint> && program_id:TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA
```

### SVM system transfers (`solana-mainnet@system_transfers`)

```
# System-program transfers touching a wallet in any role
program_id:11111111111111111111111111111111 && (source:<wallet> || destination:<wallet> || fee_payer:<wallet>)
```

## Performance

Broadcast cost goes from `O(clients)` to `O(clients × matching_events × expression_terms)`. At 1000 clients × 500 events × 64 terms = 32M comparisons per block — still around a millisecond. The field/term caps keep operator-supplied expressions from blowing the bound.

JSON re-serialization per filtered client is the real cost. v1 re-serializes per client; v2 will group clients by identical filter, serialize once per group.

## What it does not do

- **No cross-event filtering.** Filter operates per-event. You cannot ask "give me the block only if it contains at least one matching event" without also filtering events out — that is what skipping happens for already (zero matches = no broadcast).
- **No numeric / range / regex matching.** String equality only. Operators pre-compute their allowlist.
- **No subtraction.** "Everything except X" is not expressible with a value list, but `!field:X` (negation) is supported.
- **No filter on lifecycle messages.** `started`, `error`, `undo`, `dropped` always pass through regardless of filter.
