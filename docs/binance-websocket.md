# Binance WebSocket conventions

This server's URL layout and live SUBSCRIBE protocol are deliberate copies of Binance's market-streams convention. Pinning to a well-known pattern lets WebSocket clients written for Binance reuse their parsing.

Authoritative upstream references:

- Binance Futures market streams: <https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams>
- Live subscribing/unsubscribing: <https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Live-Subscribing-Unsubscribing-to-streams>

## What we mirrored verbatim

- **Stream selector form `<symbol>@<channel>`.** Binance uses `bnbusdt@aggTrade`. We use `<network>@<stream>` (`solana-mainnet@swaps`). The `@` separator and the lowercase convention are identical.
- **Two URL modes.**
  - `/ws/<a>` (single, raw) and `/ws/<a>/<b>/...` (combined, wrapped) match Binance's path layout exactly.
  - `/stream?streams=<a>/<b>/...` matches Binance's query layout exactly, including the `/`-separator inside the query value.
- **Combined-stream envelope.** Binance wraps combined-stream payloads as `{"stream":"<id>","data":<raw>}`. We do the same, byte-for-byte compatible shape.
- **JSON command envelope.** `{ "method": "...", "params": [...], "id": ... }` → `{ "result": ..., "id": ... }` or `{ "error": "...", "id": ... }`. Methods are the same names (`SUBSCRIBE`, `UNSUBSCRIBE`, `LIST_SUBSCRIPTIONS`), case-sensitive.

## What we deliberately diverged on

- **No `id` requirement.** Binance requires `id`; we treat it as optional and echo `null` when absent. Saves clients on quick experimentation.
- **Wildcards.** Binance has no wildcard syntax. We accept `*` on either side of `@` so subscribers can pull "every swaps stream" or "every Solana stream" with one selector. Wildcards survive `LIST_SUBSCRIPTIONS` verbatim — they are not expanded.
- **No public/market/private split.** Binance routes streams under `/public`, `/market`, `/private`. We have one data type (`DatabaseChanges`) and one authorization model (server-wide), so the prefix would be ceremony.
- **No 24-hour forced disconnect.** Binance kills connections at 24 hours. We do not. Operators can layer their own LB-level timeout if needed.
- **No 1024-stream subscription cap.** Binance limits 1024 streams per connection. We have no hard cap.
- **No incoming 10/sec rate cap on the command channel.** Binance throttles command frames. We don't — subscription mutations are O(small) in our hot path.

## What is intentionally Binance-incompatible

- **`undo` lifecycle messages.** Reorg handling is chain-native; Binance has nothing analogous.
- **`cursor` field on every block.** Substreams resume semantics; not present in Binance's order-book feeds.
- **Welcome (`session`) message.** Binance does not send one. We do, so clients can discover available streams and confirm their parsed subscriptions.

## Style chip

The Try-it client tags each WS-protocol message with a small `WS` chip and color-codes inbound vs outbound. Loosely modeled on how Binance's testnet client visualizes the connection — but our implementation is plain CSS, no SDK.
