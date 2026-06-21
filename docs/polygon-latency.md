# Polygon head-latency report — Substreams vs RPC vs dRPC

**Date:** 2026-06-21 · **Network:** Polygon mainnet · **Author:** latency probe (`scripts/polygon-latency.py`, `scripts/rpc-compare.py`)

## Question

Two things were asked:

1. Does the **raw Substreams** stream return delayed blocks relative to `now()`?
2. Is **Substreams behind the RPC** (i.e. does the Substreams pipeline lag a plain RPC node)?

Follow-ups added two more:

3. Is **our Pinax RPC node behind dRPC** on the same samples?
4. How much latency does **this WebSocket fan-out server** itself add (gRPC receive → DatabaseChanges decode → per-client fan-out → WebSocket delivery)?

## TL;DR

- **Substreams is at the chain head.** Median delay of a freshly-streamed block relative to `now()` is **0.08 s**, p95 **0.98 s** — below Polygon's ~1.5 s block time. It is *not* delayed.
- **Substreams is not behind the RPC — it is the opposite.** Across the window our Pinax RPC was **never ahead** of Substreams. It was even ~60 % of the time and **behind by up to 39 blocks (~45–60 s)** the rest of the time.
- **Our Pinax RPC node is behind dRPC.** Our RPC trailed dRPC in **75.6 %** of paired samples and was **never ahead**. dRPC, like Substreams, stays pinned at head.
- **Root cause is the RPC gateway/backend, not the chain, not Substreams, not the network.** Our RPC endpoint periodically rotates onto a stale backend node — its reported head block even jumps *backwards* — while the firehose/Substreams tier and dRPC both track head continuously. Our endpoint is actually the *fastest* by network RTT (75 ms vs dRPC's 87 ms), so this is a freshness/load-balancing issue, not latency on the wire.
- **The WebSocket fan-out server adds ~30 ms (p50, p95 ~50 ms).** Streaming heavy ~0.5–1.1 MB/block Polygon `erc20_transfers` DatabaseChanges, the server's decode + fan-out cost is **negligible against the ~1.5 s block time** — blocks arrive over WebSocket statistically indistinguishable from the raw gRPC stream (both sub-second vs `now()`).

> All RPC URLs in this report are redacted: the `/v1/<key>` (Pinax) and `/polygon/<key>` (dRPC) API-key path segments are stripped to `***` everywhere, including the saved raw data.

---

## Method

Two collectors, both stamping every observation with the local receive time so each source can be compared against `now()` and against each other:

- **`scripts/polygon-latency.py`** — streams a raw Substreams module live from chain head (`stream … --start-block -1 --production-mode`) and, in parallel, polls the Polygon RPC (`eth_getBlockByNumber("latest")`). For every Substreams block and every RPC poll it records `receive_wallclock` and the block's own `timestamp`.
- **`scripts/rpc-compare.py`** — polls two RPC endpoints back-to-back each round, so the two are sampled at the same wall-clock instant.
- **`scripts/ws-latency.js`** (Test 3) — runs two consumers of the *same* stream side-by-side: the raw `stream` subcommand (gRPC direct) and a WebSocket client subscribed to a locally-run `substreams-websocket serve`. For every `block_num` seen on both, the **server-added latency** is `ws_recv − raw_recv`.

Definitions:

- **delay vs `now()`** = `receive_wallclock − block_timestamp`. Captures end-to-end staleness: block production + propagation + (firehose + Substreams tier | RPC node) + network to the probe. Floor is ~one block time.
- **block lag** = `head_block(A) − head_block(B)` aligned by wall-clock. Negative ⇒ A is behind B. This is robust to local clock skew (pure block-number comparison).

Substreams package used: [`polymarket-orderbook-substreams` v0.4.0](https://substreams.dev/packages/polymarket-orderbook-substreams/v0.4.0) (`map_all_order_fills`), endpoint `polygon.substreams.pinax.network:443`. The module choice is immaterial to head latency — at the live edge the firehose delivers a clock for every block regardless of module output; we observed zero block gaps.

> **Gotcha for reproducers:** the `stream` subcommand defaults to `--max-messages 10`, so it exits after ~5 blocks. The script overrides this with `--max-messages 1000000000` and respawns the subprocess if the gRPC stream drops. Without the override a run looks like an immediate Substreams "stall" — it is not.

---

## Test 1 — Substreams vs RPC vs `now()`

**Window:** 2026-06-21 18:59:25 → 19:02:25 UTC (180 s) · 121 Substreams blocks, 61 RPC polls (3 s cadence) · est. block time **1.5 s** · **0 block gaps, 0 reconnects**.

| Measurement | min | p50 | avg | p90 | p95 | max |
|---|---|---|---|---|---|---|
| **Substreams delay vs `now()`** (s) | −1.12 | **0.08** | 0.10 | 0.78 | 0.98 | 1.92 |
| **RPC delay vs `now()`** (s) | 0.53 | 0.57 | 14.78 | 47.54 | 51.54 | 59.54 |
| **RPC − Substreams head lag** (blocks) | −39 | 0 | −9.35 | 0 | 0 | 0 |

Who was ahead (by block number): **RPC ahead 0 %**, even 60 %, **Substreams ahead 40 %**.

Read-out:
- Substreams sits on the head: median 0.08 s, p95 under 1 s.
- The RPC is usually fine (p50 0.57 s) but **bimodal** — it periodically blows out to ~50–60 s. During the run its head block froze and then *regressed* (88908951 → 88908940) before recovering, dropping up to 39 blocks behind Substreams.

A ~13-minute partial run (stopped early) showed the same shape: Substreams delay never exceeded **1.9 s**; the RPC periodically fell ~35 blocks behind.

---

## Test 2 — Our Pinax RPC vs dRPC (same samples)

**Window:** 2026-06-21 19:08:20 → 19:11:18 UTC (180 s) · 90 paired rounds (2 s cadence) · 0 errors either side.

| Measurement | min | p50 | avg | p90 | p95 | max |
|---|---|---|---|---|---|---|
| **ours − dRPC head block** (blocks) | −37 | −1 | −4.98 | 0 | 0 | 0 |
| **ours delay vs `now()`** (s) | −0.08 | 0.93 | 7.15 | 20.93 | 31.18 | 54.92 |
| **dRPC delay vs `now()`** (s) | −1.08 | −0.07 | −0.42 | −0.07 | −0.07 | −0.07 |
| ours RTT (ms) | 54 | 75 | 87 | 90 | 196 | 388 |
| dRPC RTT (ms) | 77 | 87 | 88 | 96 | 99 | 101 |

Who was behind: **ours behind 75.6 %**, even 24.4 %, **dRPC behind 0 %**.

Read-out:
- **dRPC is pinned at head** the entire run (delay ≈ 0, max 0.07 s) — same behaviour as Substreams.
- **Our RPC is behind dRPC 75.6 % of the time and never ahead.** It tracks head most of the time (p50 0.9 s) but repeatedly drops 12–37 blocks (~20–55 s) behind — the same backend-rotation pattern as Test 1.
- Our endpoint has the **lower network RTT** (75 ms vs 87 ms), so the lag is *not* network distance — it is which backend the gateway routes a given request to.

---

## Test 3 — WebSocket fan-out latency (server overhead)

Tests 1 and 2 measure the upstream. This one measures **what this repo's server adds**. A local `substreams-websocket serve` and the raw `stream` subcommand both consume the same Polygon stream; for every block seen on both, the delta is the server's decode + fan-out cost.

**Stream:** `evm-transfers` v0.4.0 `db_out` on `polygon.substreams.pinax.network:443`, subscription `polygon@erc20_transfers`. 120 matched blocks, 0 WS errors. Payloads were heavy — **~0.5–1.1 MB and ~3,000–7,000 transfer rows per block** — a deliberately hard decode/fan-out load.

> **Why not the Polymarket package here?** The WebSocket server only accepts DatabaseChanges (`db_out`). Polymarket's `db_out` is **store-backed** (market / trader / global analytics stores from the v2 start block ~84.9M), so at the live edge it must backprocess store state before it can emit a single block — it did **not** reach head within the benchmark window (0 blocks after 75 s). The stateless `map_all_order_fills` map (used by Tests 1–2) streams at head instantly but isn't DatabaseChanges. The stateless `evm-transfers` `db_out` streams at head immediately and stresses the fan-out path harder, so it's the better — and feasible — choice for measuring *server* overhead. The cold-start behaviour is itself worth noting for anyone wiring a store-heavy package into the live server.

| Measurement | min | p50 | avg | p90 | p95 | max |
|---|---|---|---|---|---|---|
| **server added latency** `ws_recv − raw_recv` (ms) | −2384 | **30** | 12.5 | 43 | 50.1 | 139 |
| WS delay vs `now()` (s) | −1.06 | −0.27 | −0.24 | 0.35 | 0.55 | 1.43 |
| raw delay vs `now()` (s) | −1.09 | −0.29 | −0.23 | 0.36 | 0.36 | 3.53 |

Read-out:
- **Server adds ~30 ms median** (p90 43 ms, p95 50 ms) — negligible vs Polygon's ~1.5 s block time. WS-delivered and raw-gRPC delays vs `now()` are statistically identical (both sub-second).
- **The negative outliers (min −2384 ms) are a measurement artifact, not the server being faster than itself.** The two consumers use *independent* gRPC connections to the tier, so the per-block delta also contains the jitter difference between those two connections; when the server's connection happens to receive a block well before the probe's, the delta goes negative. The p50/p90/p95 are the meaningful figures.
- **The server's own instrumentation confirms it.** Its `block hot-path profile` logs over the run averaged `decode_ms_avg ≈ 17–33` and `broadcast_ms_avg ≈ 8–17` (sum ≈ 30 ms), matching the externally-measured p50 of 30 ms. `max_drift_secs` stayed ≤ 1 s the whole run (server tracked head).

---

## Grafana corroboration

Source: Pinax ops Grafana, VictoriaMetrics datasource `P4169E866C3094E38`. Window ≈ the test hour.

| Series (network=polygon) | p50 | avg | max |
|---|---|---|---|
| `firehose_healthcheck_drift` (polygon.firehose.pinax.network) | 0.79 s | 2.19 s | 79.25 s |
| `head_block_time_drift{service="substreams-tier1"}` (polygon-v6-fs) | 0.24 s | 1.43 s | 83.2 s |
| `rpc_proxy_head_block` archive backends (riv-prod1) | ~7 s | 15.2 s | 97 s |

- `firehose_healthcheck_status{polygon}` was **1 (up) for the whole window**.
- Server-side metrics agree with the live probe: the **firehose and Substreams tier track head sub-second** on average, while the **RPC gateway's archive backends average ~15 s of drift** (max 97 s) — they are the laggy component.
- There was one ~2-minute firehose/tier drift spike to ~80 s at **18:39 UTC** (a transient upstream stall; it recovered on its own). **This was *before* both test windows (18:59 and 19:08), so it did not contaminate the measurements** — and even with it included, the firehose median stays under 1 s.

Dashboards:
- Nodes / WARN-ERROR logs (cluster `ott-monitor1`, network polygon): <https://ops.stats.pinax.network/d/frxzchm/opstrom-nodes>
- Relevant series for ongoing watch: `firehose_healthcheck_drift{network="polygon"}`, `head_block_time_drift{network="polygon",service="substreams-tier1"}`, `time() - rpc_proxy_head_block_time{network="polygon",cluster="riv-prod1"}`.

---

## Conclusion

1. **Raw Substreams is not delayed.** It delivers Polygon blocks at the head with a median 0.08 s / p95 0.98 s lag relative to `now()`.
2. **Substreams is not behind the RPC.** If anything the RPC is behind Substreams — never ahead of it in any sample.
3. **Our Pinax RPC node is behind dRPC** (behind 75.6 % of samples, never ahead). dRPC, like Substreams, stays at head.
4. **This WebSocket server adds ~30 ms** (p50) of decode + fan-out, even under heavy ~1 MB/block payloads — negligible against a ~1.5 s block time.

The latency that exists lives in **our RPC gateway/backend pool**, which periodically serves a stale backend (head block can move backwards). Firehose, the Substreams tier1, and dRPC all track head continuously. Suggested follow-up: tighten the RPC gateway's max-allowed backend drift / health-gating so stale archive nodes are dropped from rotation, and alert on `time() - rpc_proxy_head_block_time{network="polygon"} > ~10s`.

---

## Reproduce

```bash
# Substreams vs RPC vs now()  (3 min)
set -a && . ./.env && set +a
export POLYGON_RPC_URL="https://polygon.rpc.service.pinax.network/v1/<key>/"
python3 scripts/polygon-latency.py --duration 180 --rpc-interval 3 --out-dir runs/3min

# Our RPC vs dRPC  (3 min, same samples)
python3 scripts/rpc-compare.py \
  --a "ours=https://polygon.rpc.service.pinax.network/v1/<key>/" \
  --b "drpc=https://lb.drpc.live/polygon/<key>" \
  --duration 180 --interval 2 --out-dir runs/rpc-3min

# Re-analyze a saved run without re-collecting
python3 scripts/polygon-latency.py --analyze-only runs/3min/events.jsonl

# WebSocket fan-out latency  (3 min) — start the server, then the client
set -a && . ./.env && set +a
SUBSTREAMS_WEBSOCKET_LISTEN=127.0.0.1:8090 \
  SUBSTREAMS_WEBSOCKET_CURSORS_DIR="$(mktemp -d)" SUBSTREAMS_PRODUCTION_MODE=true \
  ./target/debug/substreams-websocket serve --streams scripts/ws-bench-streams.yaml &
# wait until the server is at head, then:
node scripts/ws-latency.js \
  --ws "ws://127.0.0.1:8090/ws/polygon@erc20_transfers" \
  --endpoint "https://polygon.substreams.pinax.network:443" \
  --spkg "https://github.com/pinax-network/substreams-evm/releases/download/evm-transfers-v0.4.0/evm-transfers-v0.4.0.spkg" \
  --module db_out --duration 180 --out-dir runs/ws-bench
```

> The server skips decode/fan-out for streams with **no subscribers**, so its head-block gauge won't advance until `ws-latency.js` connects — that's expected.

Raw data and per-run summaries are written under `runs/` (`events.jsonl` / `rounds.jsonl` + `summary.json`). API keys are redacted in all saved output.
