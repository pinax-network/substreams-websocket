#!/usr/bin/env node
/*
 * WebSocket fan-out latency benchmark.
 *
 * Measures the latency the substreams-websocket SERVER adds on top of the raw
 * Substreams stream: gRPC receive -> DatabaseChanges decode -> per-client
 * fan-out -> WebSocket delivery.
 *
 * It runs two consumers of the SAME Polygon stream side-by-side:
 *   - raw:  the `stream` subcommand talking gRPC directly to the Substreams tier
 *   - ws:   a WebSocket client subscribed to this server's fan-out
 * For every block_num seen on both, the added latency is ws_recv - raw_recv.
 * It also reports each side's delay relative to now() (receive - block_time).
 *
 * The server must already be running and serving the same stream. Example:
 *   SUBSTREAMS_WEBSOCKET_LISTEN=127.0.0.1:8090 \
 *   SUBSTREAMS_WEBSOCKET_CURSORS_DIR=$(mktemp -d) \
 *   ./target/debug/substreams-websocket serve --streams scripts/ws-bench-streams.yaml
 *
 * Usage:
 *   node scripts/ws-latency.js \
 *     --ws ws://127.0.0.1:8090/ws/polygon@* \
 *     --endpoint https://polygon.substreams.pinax.network:443 \
 *     --spkg https://spkg.io/PaulieB14/polymarket-orderbook-substreams-v0.4.0.spkg \
 *     --module db_out --duration 180 --out-dir runs/ws-bench
 *
 * Node >= 22 (uses the built-in global WebSocket; no npm deps).
 */
"use strict";

const { spawn } = require("node:child_process");
const fs = require("node:fs");
const path = require("node:path");
const readline = require("node:readline");

function arg(name, def) {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 && i + 1 < process.argv.length ? process.argv[i + 1] : def;
}

const WS_URL = arg("ws", "ws://127.0.0.1:8090/ws/polygon@*");
const ENDPOINT = arg("endpoint", "https://polygon.substreams.pinax.network:443");
const SPKG = arg("spkg", "https://spkg.io/PaulieB14/polymarket-orderbook-substreams-v0.4.0.spkg");
const MODULE = arg("module", "db_out");
const DURATION = parseInt(arg("duration", "180"), 10);
const BIN = process.env.SWS_BIN || "./target/debug/substreams-websocket";
const OUT_DIR = arg("out-dir", null);

// block_num -> {recv: epoch_ms, ts: block_unix_secs}
const raw = new Map();
const ws = new Map();
const BLOCK_RE = /block_num=(\d+)\b.*?\btimestamp_seconds=(-?\d+)/;

// --- raw gRPC stream via the `stream` subcommand -------------------------
const proc = spawn(
  BIN,
  ["stream", SPKG, MODULE, "--endpoint", ENDPOINT, "--start-block", "-1",
   "--production-mode", "--max-messages", "1000000000"],
  { stdio: ["ignore", "pipe", "inherit"] }
);
readline.createInterface({ input: proc.stdout }).on("line", (line) => {
  const m = BLOCK_RE.exec(line);
  if (m) {
    const n = Number(m[1]);
    if (!raw.has(n)) raw.set(n, { recv: Date.now(), ts: Number(m[2]) });
  }
});

// --- WebSocket fan-out client --------------------------------------------
let sessionSeen = false;
let wsErrors = 0;
const sock = new WebSocket(WS_URL);
sock.addEventListener("message", (ev) => {
  const now = Date.now();
  let msg;
  try { msg = JSON.parse(ev.data); } catch { return; }
  if (msg.type === "session") {
    sessionSeen = true;
    const s = (msg.streams || []).map((x) => `${x.network}/${x.package_name}@${x.package_version}`).join(", ");
    console.error(`[ws] session: streams=[${s}] subs=${JSON.stringify(msg.subscriptions)} wrap=${msg.wrap_envelope}`);
    return;
  }
  // Block payload may be raw or wrapped under .data (combined-stream mode).
  const blk = msg.block_num !== undefined ? msg : (msg.data && msg.data.block_num !== undefined ? msg.data : null);
  if (!blk) return; // lifecycle / control frame
  const n = Number(blk.block_num);
  if (!ws.has(n)) {
    // timestamp is UTC "YYYY-MM-DD HH:MM:SS"
    const tsSecs = Math.floor(Date.parse(blk.timestamp.replace(" ", "T") + "Z") / 1000);
    ws.set(n, { recv: now, ts: tsSecs });
  }
});
sock.addEventListener("error", () => { wsErrors++; });
sock.addEventListener("open", () => console.error(`[ws] connected ${WS_URL}`));

// --- progress ------------------------------------------------------------
const start = Date.now();
const progress = setInterval(() => {
  const elapsed = Math.round((Date.now() - start) / 1000);
  let matched = 0;
  for (const n of ws.keys()) if (raw.has(n)) matched++;
  console.error(`[${String(elapsed).padStart(4)}s] raw_blocks=${raw.size} ws_blocks=${ws.size} matched=${matched}`);
}, 15000);

// --- finish --------------------------------------------------------------
function pctl(a, p) {
  if (!a.length) return null;
  const s = [...a].sort((x, y) => x - y);
  const k = (s.length - 1) * (p / 100);
  const f = Math.floor(k), c = Math.min(f + 1, s.length - 1);
  return s[f] + (s[c] - s[f]) * (k - f);
}
function stats(a, unit) {
  if (!a.length) return null;
  const r = (x) => Math.round(x * 1000) / 1000;
  return {
    n: a.length, min: r(Math.min(...a)), p50: r(pctl(a, 50)), avg: r(a.reduce((s, x) => s + x, 0) / a.length),
    p90: r(pctl(a, 90)), p95: r(pctl(a, 95)), max: r(Math.max(...a)), unit,
  };
}

function finish() {
  clearInterval(progress);
  try { proc.kill("SIGTERM"); } catch {}
  try { sock.close(); } catch {}

  const matchedKeys = [...ws.keys()].filter((n) => raw.has(n));
  // added latency (ms) = ws_recv - raw_recv, on the same block
  const addedMs = matchedKeys.map((n) => ws.get(n).recv - raw.get(n).recv);
  // delay vs now() (secs) = recv - block_ts
  const wsDelay = [...ws.values()].map((v) => v.recv / 1000 - v.ts);
  const rawDelay = [...raw.values()].map((v) => v.recv / 1000 - v.ts);

  const summary = {
    meta: { ws_url: WS_URL, endpoint: ENDPOINT, spkg: SPKG, module: MODULE, duration: DURATION },
    counts: {
      raw_blocks: raw.size, ws_blocks: ws.size, matched_blocks: matchedKeys.length,
      ws_errors: wsErrors, session_seen: sessionSeen,
    },
    server_added_latency_ms: stats(addedMs, "ms"),
    ws_delay_vs_now: stats(wsDelay, "secs"),
    raw_delay_vs_now: stats(rawDelay, "secs"),
  };

  const row = (label, st) => st
    ? console.log(`${label.padEnd(28)}: min=${st.min} p50=${st.p50} avg=${st.avg} p90=${st.p90} p95=${st.p95} max=${st.max} ${st.unit}`)
    : console.log(`${label.padEnd(28)}: (no data)`);

  console.log("\n" + "=".repeat(64));
  console.log("WEBSOCKET FAN-OUT LATENCY");
  console.log("=".repeat(64));
  console.log(`ws url : ${WS_URL}`);
  console.log(`stream : ${SPKG} / ${MODULE}`);
  console.log(`counts : raw=${raw.size} ws=${ws.size} matched=${matchedKeys.length} ws_err=${wsErrors}`);
  console.log("-".repeat(64));
  row("server added latency (ws-raw)", summary.server_added_latency_ms);
  row("ws delay vs now", summary.ws_delay_vs_now);
  row("raw delay vs now", summary.raw_delay_vs_now);
  console.log("=".repeat(64) + "\n");

  if (OUT_DIR) {
    fs.mkdirSync(OUT_DIR, { recursive: true });
    fs.writeFileSync(path.join(OUT_DIR, "ws-summary.json"), JSON.stringify(summary, null, 2));
    const rows = matchedKeys.sort((a, b) => a - b).map((n) => ({
      block: n, raw_recv: raw.get(n).recv, ws_recv: ws.get(n).recv,
      added_ms: ws.get(n).recv - raw.get(n).recv, block_ts: ws.get(n).ts,
    }));
    fs.writeFileSync(path.join(OUT_DIR, "ws-matched.jsonl"),
      rows.map((r) => JSON.stringify(r)).join("\n") + "\n");
    console.error(`wrote ${OUT_DIR}/ws-summary.json and ws-matched.jsonl`);
  }
  process.exit(0);
}

setTimeout(finish, DURATION * 1000);
process.on("SIGINT", finish);
process.on("SIGTERM", finish);
