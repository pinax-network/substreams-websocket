# Event filters

Per-subscription field filters on the `events[]` array. Subscribers only receive events whose columns match their filter; if no events match in a given block, the block is skipped entirely for that subscriber.

## Filter shape

```json
{
  "protocol": "raydium_cpmm",
  "user": ["addr1", "addr2"]
}
```

- **String equality only.** No regex, no range, no substring.
- **Field-level AND.** Every key in the filter must match.
- **Value-level OR.** A value can be a string (exact match) or an array of strings (any of). Empty array matches nothing.
- **Missing fields are a miss.** Filtering on `protocol` against an event that has no `protocol` column drops the event.
- **Schema-agnostic.** The server does not know the swap / transfer schema. Any column name works.
- **Top-level fields are not filterable.** `block_num`, `network`, `module_hash`, etc. always pass through. Filter applies only to keys inside `events[*]`.

## Wire — connect

URL-encoded JSON on the WebSocket upgrade:

```
ws://host/ws/solana-mainnet@swaps?filter=%7B%22protocol%22%3A%22raydium_cpmm%22%7D
ws://host/stream?streams=solana-mainnet@swaps&filter=%7B%22user%22%3A%5B%22a%22%2C%22b%22%5D%7D
```

The filter applies to **every explicit `network@table` selector** in the URL. Wildcard selectors (`*@swaps`, `solana-mainnet@*`) skip filtering — they always receive every event.

For different filters per channel on one connection, use the live `SET_FILTER` command.

## Wire — live commands

### `SET_FILTER`

```json
// request
{ "method": "SET_FILTER",
  "params": ["solana-mainnet@swaps", { "protocol": "raydium_cpmm", "user": ["a","b"] }],
  "id": 1 }

// reply
{ "result": null, "id": 1 }
```

- `params[0]` = `network@table` selector. Wildcards accepted on either side: `*@*`, `<network>@*`, `*@<table>`. Filters with wildcard selectors apply to every matching outgoing `(network, table)`. Multiple stored filters (exact + wildcard) compose with AND semantics — each must pass.
- `params[1]` = filter object. `{}` is valid and matches every event (effectively no-op).
- Replaces any existing filter for the selector. Idempotent.

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
{ "result": { "solana-mainnet@swaps": { "protocol": "raydium_cpmm" } }, "id": 3 }
```

Returns the current filter map. Keys sorted alphabetically.

## Bounds

| Variable | Default | Notes |
|----------|---------|-------|
| `SUBSTREAMS_WEBSOCKET_MAX_FILTER_FIELDS` | `16` | Max keys in one filter object. |
| `SUBSTREAMS_WEBSOCKET_MAX_FILTER_VALUES` | `64` | Max total string values across all keys. |

Overflow returns `{"error":"filter exceeds max fields/values","id":...}` and leaves the previous filter in place.

## Replay interaction

The replay log stores **unfiltered** block JSON. On `?from_block=<n>` resume, the server applies the client's current filter to each replayed block before sending. This means a client can change filters between disconnect and reconnect and the replay respects the new filter.

Wildcard selectors stay live-only (already documented in [`replay.md`](replay.md)).

## Common filter shapes

Examples per table — the server doesn't enforce these, they're just operator-friendly suggestions matching the columns commonly present.

### SVM swaps (`solana-mainnet@swaps`)

```json
{
  "protocol": "raydium_cpmm",
  "amm": "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
  "amm_pool": "8ZT5BBW3WRpvCwPiadE6jiocQfriMjW7DSfXR2pF6YcT",
  "program_id": "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
  "user": "F2MUEfN1HG5mC5EiUoxhjjc7HpKi4QQnzvipnbGx6Av8",
  "input_mint": "So11111111111111111111111111111111111111112",
  "output_mint": "13muFYDBUvgNpyDQSZ4eQTVHNoWaGhykonvyZWdGbonk",
  "fee_payer": "F2MUEfN1HG5mC5EiUoxhjjc7HpKi4QQnzvipnbGx6Av8"
}
```

### SVM SPL transfers (`solana-mainnet@spl_transfers`)

```json
{
  "source": "<wallet>",
  "destination": "<wallet>",
  "mint": "<mint>",
  "program_id": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
  "fee_payer": "<wallet>"
}
```

### SVM system transfers (`solana-mainnet@system_transfers`)

```json
{
  "program_id": "11111111111111111111111111111111",
  "fee_payer": "<wallet>",
  "source": "<wallet>",
  "destination": "<wallet>"
}
```

## Performance

Broadcast cost goes from `O(clients)` to `O(clients × matching_events × filter_fields)`. At 1000 clients × 500 events × 16 fields = 8M comparisons per block — still under a millisecond. Field/value caps keep operator-supplied filters from blowing the bound.

JSON re-serialization per filtered client is the real cost. v1 re-serializes per client; v2 will group clients by identical filter, serialize once per group.

## What it does not do

- **No cross-event filtering.** Filter operates per-event. You cannot ask "give me the block only if it contains at least one matching event" without also filtering events out — that is what skipping happens for already (zero matches = no broadcast).
- **No numeric / range / regex matching.** String equality only. Operators pre-compute their allowlist.
- **No subtraction.** "Everything except X" is not expressible. List the values you want explicitly.
- **No filter on lifecycle messages.** `started`, `error`, `undo`, `gap` always pass through regardless of filter.
