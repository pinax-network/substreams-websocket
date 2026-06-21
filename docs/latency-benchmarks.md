# WebSocket latency benchmarks

Measured head-latency study of the `substreams-websocket` fan-out server against
the public Pinax deployment (`wss://ws.pinax.network`). The headline question:
**does the WebSocket hop add latency on top of Substreams?** Short answer, across
every test below: **no — the WebSocket mirrors the upstream Substreams stream to
within ~100 ms**, even for heavy chains and with a 100-wallet server-side filter.
The lag operators actually observe lives *upstream* (chain → Firehose →
Substreams) and is bursty for high-throughput chains.

> Provenance: measured 2026-06-21 with Substreams CLI `1.18.5` and a Node 22
> native-WebSocket client, from a cloud container (not co-located with the Pinax
> endpoints). Raw harness scripts are reproduced at the end. These numbers are a
> point-in-time snapshot of a live network — re-run the harness to refresh them.

## The metric: head lag

Every block carries a producer timestamp (the chain's block time). **Head lag**
is `now − block_timestamp` measured at the instant a consumer receives the block:

- **Live Substreams**: `substreams run … -o clock` prints each block's `age`,
  which is exactly `now − block_timestamp` at receipt.
- **WebSocket**: every block message carries `timestamp_seconds`; head lag is
  `now − timestamp_seconds` at receipt. The server also exports this live as the
  Prometheus gauge `substreams_websocket_head_block_time_drift`.

To compare the two feeds we run them **concurrently off one process clock** and
**match blocks by number**. The matched delta `WS_recv − SS_recv` cancels the
common path (chain → Substreams → our vantage) and isolates the *extra* hop the
WebSocket server adds. That is why the absolute head-lag columns can be large
(they include upstream processing) while the matched delta stays near zero.

Negative head lag is normal: block timestamps are integer seconds, so a block
received 0.4 s into its timestamp-second reads as `−0.6 s`, plus minor clock skew
between the producer and our host.

## Benchmark 1 — Polymarket (light payloads)

Polygon, `polymarket@ctfexchange_order_filled`, ~50–83 KB/block. 120 s window.

| feed | mean | p50 | p90 | max |
|---|---|---|---|---|
| Live Substreams | −0.09 s | −0.22 s | 0.46 s | 2.87 s |
| Pinax WS | −0.17 s | −0.27 s | 0.42 s | 0.93 s |
| **WS − SS (matched)** | **−0.04 s** | −0.03 s | 0.00 s | **0.04 s** |

Both feeds sit at the Polygon head; the WebSocket adds nothing measurable
(matched delta within 40 ms, mostly noise from our vantage's RTT).

## Benchmark 2 — Solana SPL transfers (heavy payloads)

`solana@spl_transfer`, avg **304 KB/block**, max **742 KB**, ~2.5 blocks/s,
**65 MB** transferred in 90 s. This is the stress case for throughput.

| feed | mean | p50 | p90 | max |
|---|---|---|---|---|
| Live Substreams | 5.13 s | 2.40 s | **13.78 s** | 18.52 s |
| Pinax WS | 5.02 s | 2.34 s | 13.62 s | 18.47 s |
| **WS − SS (matched)** | **−0.08 s** | −0.03 s | −0.00 s | **0.06 s** |

Two things stand out:

1. The **absolute** head lag is large and **bursty** — p50 ≈ 2.4 s but p90 ≈ 14 s
   in the same 90 s window. This is upstream: producing and serializing
   multi-hundred-KB DatabaseChanges blocks for a high-throughput chain.
2. The WebSocket still **mirrors** Substreams within ~80 ms. It is not the source
   of the lag.

This is the key result for interpreting a report like *"WS 13 s vs Substreams
5 s."* That is one bursty distribution sampled at two different instants — not the
WebSocket adding a fixed offset. Always confirm with a matched, concurrent A/B
before blaming the fan-out layer.

## Benchmark 3 — production vs development mode

A common suspicion is that Substreams *production mode* trades head latency for
backfill throughput. Measured directly: two `substreams run` streams against the
same Solana endpoint, one default (development), one `--production-mode`, matched
by block over 90 s.

| mode | mean | p50 | p90 | max |
|---|---|---|---|---|
| development | 2.12 s | 2.08 s | 2.64 s | 4.08 s |
| production | 2.10 s | 2.05 s | 2.62 s | 4.11 s |
| **prod − dev (matched)** | **−0.02 s** | −0.02 s | −0.00 s | 0.03 s |

**No difference at the head** (~20 ms, within noise). Production mode's parallel
back-processing helps *backfill*; it is not a head-latency lever. (The
`substreams-websocket` server is live-only with no replay, so it leaves
`production_mode=false` to match development-mode semantics regardless.)

Note also that this window's upstream lag (~2 s) was much lower than Benchmark 2's
(~5 s p50 / 14 s p90) on the same chain — direct evidence that upstream head lag
is time-variable, not a fixed property.

## Benchmark 4 — 100-wallet server-side filter

Does evaluating a large server-side filter delay delivery? We collected 100 live
wallet addresses from the Polymarket stream and subscribed a second WebSocket
connection with a 100-term OR filter (`w1 || w2 || … || w100`, a 4,596-character
expression), alongside an unfiltered connection and live Substreams. 150 s window.

Head lag (`now − block_ts`):

| feed | mean | p50 | p90 |
|---|---|---|---|
| Live Substreams | −0.53 s | −0.51 s | −0.12 s |
| WS unfiltered | −0.63 s | −0.58 s | −0.25 s |
| WS + 100-wallet filter | −0.63 s | −0.58 s | −0.25 s |

Matched deltas:

| comparison | mean | p50 | p90 | max |
|---|---|---|---|---|
| **filtered − unfiltered (filter cost)** | **+0.001 s** | +0.001 s | +0.002 s | **+0.002 s** |
| unfiltered − Substreams | −0.076 s | −0.070 s | −0.034 s | 0.000 s |
| filtered − Substreams | −0.101 s | −0.068 s | −0.033 s | 0.000 s |

A 100-wallet filter costs **~1–2 ms** versus no filter. The filter is a linear
per-row scan (`apply_filter_in_place`), so 100 OR-terms is trivial. Notes:

- The public server **accepted ≥100 filter values** (the 100-term expression
  upgraded cleanly).
- Filtered and unfiltered payloads were the same size (~50 KB/block): the 100
  sampled wallets cover essentially all order-fill activity, since Polymarket
  fills are dominated by a handful of maker wallets. That makes this the *worst
  case* for filter cost (every row retained → full re-serialization), and it was
  still ~1–2 ms.

## Takeaways

- **The WebSocket fan-out is not a latency source.** Across light and heavy
  streams, filtered and unfiltered, it tracks live Substreams within ~100 ms.
- **Head lag is upstream and bursty.** For high-throughput chains expect p50 of a
  few seconds and p90 spikes into the teens — driven by block size and chain rate,
  not the delivery layer. Attribute lag with the
  `substreams_websocket_head_block_time_drift` gauge vs a direct consumer before
  suspecting the server.
- **Production vs development mode is a wash at the head** (~20 ms).
- **Server-side filtering is cheap** (~1–2 ms for 100 wallets).
- **Defensive throughput hardening still matters.** The gRPC HTTP/2 window was
  raised to 8 MiB with adaptive flow control (see
  [`substreams.md`](substreams.md)) so the *delivery* layer keeps its near-zero
  overhead even when the WebSocket server is far (network-wise) from the endpoint
  or pulls multi-MiB blocks. Our co-located-ish vantage didn't exercise that
  ceiling, but high-RTT deployments would.

## Reproducing

All tests use the same shape: stream live Substreams and the public WS together,
match by block number, off one clock. Set `SUBSTREAMS_API_TOKEN` and download the
matching `.spkg` first (`substreams info <spkg>` shows the network → pick the
`<chain>.substreams.pinax.network:443` endpoint).

WS vs live Substreams, matched by block (`compare.mjs`):

```js
import { spawn } from "node:child_process";
const ENDPOINT = process.env.ENDPOINT, SPKG = process.env.SPKG, WS_URL = process.env.WS_URL;
const ss = new Map(), ws = new Map();
const re = /BLOCK #([\d,]+).*age=(-?[\d.]+)(ms|s|µs|us)/;
const child = spawn("./substreams", ["run","-e",ENDPOINT,SPKG,"db_out","--start-block","-1","-o","clock"], { env: process.env });
let buf = "";
child.stdout.on("data", d => { buf += d; let i;
  while ((i = buf.indexOf("\n")) >= 0) { const l = buf.slice(0,i); buf = buf.slice(i+1);
    const m = re.exec(l); if (!m) continue; const n = +m[1].replace(/,/g,"");
    let a = +m[2]; if (m[3]==="s") a*=1000; else if (m[3].startsWith("µ")||m[3]==="us") a/=1000;
    if (!ss.has(n)) ss.set(n, { recv: Date.now(), age: a }); } });
const sock = new WebSocket(WS_URL);
sock.addEventListener("message", ev => { const m = JSON.parse(ev.data);
  if (typeof m.block_num === "number" && Array.isArray(m.events) && !ws.has(m.block_num))
    ws.set(m.block_num, { recv: Date.now(), ts: m.timestamp_seconds*1000 }); });
setTimeout(() => { child.kill("SIGTERM"); sock.close();
  const d = []; for (const [n,w] of ws) { const s = ss.get(n); if (s) d.push((w.recv-s.recv)/1000); }
  const mean = a => a.reduce((x,y)=>x+y,0)/a.length;
  console.log("matched WS-SS delta mean", mean(d).toFixed(3)+"s", "n="+d.length);
}, 120000);
```

```bash
# Benchmark 1 (Polymarket)
ENDPOINT=polygon.substreams.pinax.network:443 SPKG=polymarket.spkg \
  WS_URL="wss://ws.pinax.network/ws/polymarket@ctfexchange_order_filled" node compare.mjs
# Benchmark 2 (Solana, heavy)
ENDPOINT=solana.substreams.pinax.network:443 SPKG=svm-transfers.spkg \
  WS_URL="wss://ws.pinax.network/ws/solana@spl_transfer" node compare.mjs
```

For Benchmark 3 run two `substreams run` streams (one with `--production-mode`)
and match by block. For Benchmark 4, collect N wallets from the unfiltered stream,
then open a second WS with `?filter=<w1 || w2 || … || wN>` and compare its matched
receive times to the unfiltered connection.

See [`substreams.md`](substreams.md#head-latency--lag) for the operational summary
and [`filters.md`](filters.md) for the filter expression language.
