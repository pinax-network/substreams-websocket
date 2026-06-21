#!/usr/bin/env python3
"""
Polygon head-latency monitor: raw Substreams vs RPC vs wall-clock now().

Streams a raw Substreams module live from chain head and, in parallel, polls a
Polygon RPC endpoint for its latest block. Every block (Substreams) and every
RPC poll is stamped with the local receive time, so we can answer two questions:

  1. How stale is each source relative to now()?
       delay = receive_wallclock - block_timestamp
  2. Is Substreams behind the RPC (or vice-versa)?
       block_lag = rpc_head_block - substreams_head_block  (aligned by wall-clock)

The Substreams head delay includes block production + propagation + firehose +
the Substreams tier. The RPC head delay includes block production + propagation
+ the RPC node. Comparing the two isolates the cost the Substreams pipeline adds
on top of a plain RPC node.

Usage:
    # collect (reads SUBSTREAMS_* creds + POLYGON_RPC_URL from env / .env)
    ./scripts/polygon-latency.py --duration 180 --out-dir runs/3min

    # re-analyze a previous run without re-collecting
    ./scripts/polygon-latency.py --analyze-only runs/3min/events.jsonl

Env vars (collection):
    SUBSTREAMS_ENDPOINT   default https://polygon.substreams.pinax.network:443
    SUBSTREAMS_API_KEY / SUBSTREAMS_TOKEN / SUBSTREAMS_AUTH_URL  (as in .env)
    POLYGON_RPC_URL       required; the /v1/<key> path is redacted in all output
    SWS_BIN               path to the substreams-websocket binary
                          (default ./target/debug/substreams-websocket)
"""

import argparse
import json
import os
import re
import statistics
import subprocess
import sys
import threading
import time
import urllib.request

DEFAULT_ENDPOINT = "https://polygon.substreams.pinax.network:443"
DEFAULT_SPKG = "https://spkg.io/PaulieB14/polymarket-orderbook-substreams-v0.4.0.spkg"
DEFAULT_MODULE = "map_all_order_fills"
DEFAULT_BIN = "./target/debug/substreams-websocket"

# `block block_num=88908687 block_hash=... timestamp=2026-... timestamp_seconds=1782068026 ...`
BLOCK_RE = re.compile(r"block_num=(\d+)\b.*?\btimestamp_seconds=(-?\d+)")
SESSION_RE = re.compile(r"resolved_start_block=(\d+)\s+chain_head=(\d+)")


def redact_rpc(url: str) -> str:
    """Strip the /v1/<api-key> segment so the key never lands in output."""
    return re.sub(r"(/v1/)[^/]+", r"\1***", url)


def rpc_latest(url: str, timeout: float = 10.0):
    """Return (block_number:int, block_timestamp:int) for the chain head."""
    body = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getBlockByNumber",
            "params": ["latest", False],
        }
    ).encode()
    req = urllib.request.Request(
        url, data=body, headers={"content-type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = json.load(resp)
    result = data["result"]
    return int(result["number"], 16), int(result["timestamp"], 16)


class Collector:
    def __init__(self, args):
        self.args = args
        self.events = []  # list of dicts: {t, kind, ...}
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.proc = None
        self.session = None

    def record(self, ev):
        with self.lock:
            self.events.append(ev)

    def stream_reader(self):
        endpoint = os.environ.get("SUBSTREAMS_ENDPOINT", DEFAULT_ENDPOINT)
        bin_path = os.environ.get("SWS_BIN", DEFAULT_BIN)
        # The `stream` subcommand defaults to --max-messages 10; override with a
        # huge cap so it streams the whole window. Respawn on premature exit so a
        # dropped gRPC connection during a long run doesn't end collection.
        cmd = [
            bin_path,
            "stream",
            self.args.spkg,
            self.args.module,
            "--endpoint",
            endpoint,
            "--start-block",
            "-1",
            "--production-mode",
            "--max-messages",
            str(10**9),
        ]
        while not self.stop.is_set():
            self.proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
            for line in self.proc.stdout:
                now = time.time()
                if self.stop.is_set():
                    break
                m = BLOCK_RE.search(line)
                if m:
                    self.record(
                        {
                            "t": now,
                            "kind": "sub",
                            "block": int(m.group(1)),
                            "block_ts": int(m.group(2)),
                        }
                    )
                    continue
                s = SESSION_RE.search(line)
                if s:
                    self.session = {
                        "resolved_start_block": int(s.group(1)),
                        "chain_head": int(s.group(2)),
                    }
            # stdout closed: process exited.
            if self.stop.is_set():
                break
            err = ""
            try:
                err = (self.proc.stderr.read() or "").strip()[-300:]
            except Exception:  # noqa: BLE001
                pass
            self.record({"t": time.time(), "kind": "sub_reconnect", "stderr": err})
            print(f"[substreams stream exited, respawning] {err}", file=sys.stderr, flush=True)
            self.stop.wait(1.0)

    def rpc_poller(self):
        url = self.args.rpc_url
        interval = self.args.rpc_interval
        # Align polls to a steady cadence regardless of request latency.
        next_t = time.time()
        while not self.stop.is_set():
            t0 = time.time()
            try:
                block, block_ts = rpc_latest(url)
                self.record(
                    {
                        "t": time.time(),
                        "kind": "rpc",
                        "block": block,
                        "block_ts": block_ts,
                        "rtt_ms": round((time.time() - t0) * 1000, 1),
                    }
                )
            except Exception as e:  # noqa: BLE001 - keep polling through transient errors
                self.record({"t": time.time(), "kind": "rpc_err", "error": str(e)})
            next_t += interval
            self.stop.wait(max(0.0, next_t - time.time()))

    def progress_printer(self):
        start = time.time()
        while not self.stop.is_set():
            self.stop.wait(15)
            with self.lock:
                subs = [e for e in self.events if e["kind"] == "sub"]
                rpcs = [e for e in self.events if e["kind"] == "rpc"]
            elapsed = int(time.time() - start)
            last_sub = subs[-1] if subs else None
            last_rpc = rpcs[-1] if rpcs else None
            msg = f"[{elapsed:>4}s] sub_blocks={len(subs)} rpc_polls={len(rpcs)}"
            if last_sub:
                msg += f" sub_head={last_sub['block']} sub_delay={time.time()-last_sub['block_ts']:.1f}s"
            if last_rpc:
                msg += f" rpc_head={last_rpc['block']}"
            if last_sub and last_rpc:
                msg += f" lag={last_rpc['block']-last_sub['block']:+d}blk"
            print(msg, file=sys.stderr, flush=True)

    def run(self):
        threads = [
            threading.Thread(target=self.stream_reader, daemon=True),
            threading.Thread(target=self.rpc_poller, daemon=True),
            threading.Thread(target=self.progress_printer, daemon=True),
        ]
        for t in threads:
            t.start()
        try:
            self.stop.wait(self.args.duration)
        except KeyboardInterrupt:
            print("interrupted, finishing up...", file=sys.stderr)
        self.stop.set()
        if self.proc:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        time.sleep(0.5)


def pctl(values, p):
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(s) - 1)
    return s[f] + (s[c] - s[f]) * (k - f)


def summarize(events, meta):
    subs = [e for e in events if e["kind"] == "sub"]
    rpcs = [e for e in events if e["kind"] == "rpc"]
    rpc_errs = [e for e in events if e["kind"] == "rpc_err"]
    reconnects = [e for e in events if e["kind"] == "sub_reconnect"]

    # Deduplicate Substreams blocks (live edge can re-emit on reconnect); keep
    # the first time we saw each block number.
    seen = {}
    for e in subs:
        if e["block"] not in seen:
            seen[e["block"]] = e
    subs_u = sorted(seen.values(), key=lambda e: e["block"])

    # 1) Source delay relative to now() (receive_wallclock - block_timestamp).
    sub_delays = [e["t"] - e["block_ts"] for e in subs_u]
    rpc_delays = [e["t"] - e["block_ts"] for e in rpcs]

    # 2) RPC vs Substreams block lag, aligned by wall-clock: for each RPC poll,
    #    find the highest Substreams block received at or before the poll time.
    sub_by_time = sorted(subs, key=lambda e: e["t"])
    lags = []
    for r in rpcs:
        best = None
        for e in sub_by_time:
            if e["t"] <= r["t"]:
                if best is None or e["block"] > best:
                    best = e["block"]
            else:
                break
        if best is not None:
            lags.append({"t": r["t"], "rpc": r["block"], "sub": best, "lag": r["block"] - best})

    # Estimate avg block time from Substreams block timestamps.
    block_time = None
    if len(subs_u) >= 2:
        span_blocks = subs_u[-1]["block"] - subs_u[0]["block"]
        span_secs = subs_u[-1]["block_ts"] - subs_u[0]["block_ts"]
        if span_blocks > 0:
            block_time = span_secs / span_blocks

    def stats(vals, unit):
        if not vals:
            return None
        return {
            "n": len(vals),
            "min": round(min(vals), 3),
            "p50": round(pctl(vals, 50), 3),
            "avg": round(statistics.fmean(vals), 3),
            "p90": round(pctl(vals, 90), 3),
            "p95": round(pctl(vals, 95), 3),
            "max": round(max(vals), 3),
            "unit": unit,
        }

    lag_vals = [l["lag"] for l in lags]
    # Detect Substreams blocks that were NOT contiguous (gaps -> stalls).
    gaps = []
    for a, b in zip(subs_u, subs_u[1:]):
        if b["block"] - a["block"] > 1:
            gaps.append({"after": a["block"], "next": b["block"], "missing": b["block"] - a["block"] - 1})

    t0 = min((e["t"] for e in events), default=0)
    t1 = max((e["t"] for e in events), default=0)
    return {
        "meta": meta,
        "window": {
            "wallclock_start": t0,
            "wallclock_end": t1,
            "duration_secs": round(t1 - t0, 1),
        },
        "counts": {
            "substreams_blocks": len(subs_u),
            "substreams_block_events": len(subs),
            "rpc_polls": len(rpcs),
            "rpc_errors": len(rpc_errs),
            "substreams_reconnects": len(reconnects),
            "substreams_block_gaps": len(gaps),
        },
        "block_time_secs_est": round(block_time, 3) if block_time else None,
        "substreams_head_delay": stats(sub_delays, "secs"),
        "rpc_head_delay": stats(rpc_delays, "secs"),
        "rpc_minus_substreams_block_lag": stats(lag_vals, "blocks"),
        "rpc_ahead_share": round(sum(1 for v in lag_vals if v > 0) / len(lag_vals), 3)
        if lag_vals
        else None,
        "substreams_ahead_share": round(sum(1 for v in lag_vals if v < 0) / len(lag_vals), 3)
        if lag_vals
        else None,
        "even_share": round(sum(1 for v in lag_vals if v == 0) / len(lag_vals), 3)
        if lag_vals
        else None,
        "gaps": gaps[:20],
    }


def print_summary(s):
    print("\n" + "=" * 64)
    print("POLYGON HEAD-LATENCY SUMMARY")
    print("=" * 64)
    m = s["meta"]
    print(f"endpoint (substreams) : {m['endpoint']}")
    print(f"endpoint (rpc)        : {m['rpc_url_redacted']}")
    print(f"spkg / module         : {m['spkg']} / {m['module']}")
    print(f"window                : {s['window']['duration_secs']}s")
    c = s["counts"]
    print(
        f"samples               : {c['substreams_blocks']} sub blocks, "
        f"{c['rpc_polls']} rpc polls, {c['rpc_errors']} rpc errors, "
        f"{c['substreams_block_gaps']} block gaps"
    )
    print(f"est. block time       : {s['block_time_secs_est']}s")

    def row(label, st):
        if not st:
            print(f"{label:<26}: (no data)")
            return
        print(
            f"{label:<26}: min={st['min']} p50={st['p50']} avg={st['avg']} "
            f"p90={st['p90']} p95={st['p95']} max={st['max']} {st['unit']}"
        )

    print("-" * 64)
    row("Substreams delay vs now", s["substreams_head_delay"])
    row("RPC delay vs now", s["rpc_head_delay"])
    row("RPC - Substreams lag", s["rpc_minus_substreams_block_lag"])
    print(
        f"{'who is ahead':<26}: rpc_ahead={s['rpc_ahead_share']} "
        f"even={s['even_share']} sub_ahead={s['substreams_ahead_share']}"
    )
    if s["gaps"]:
        print("-" * 64)
        print(f"substreams block gaps (stalls): {s['gaps']}")
    print("=" * 64 + "\n")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--duration", type=int, default=180, help="collection seconds (default 180)")
    ap.add_argument("--rpc-interval", type=float, default=3.0, help="rpc poll interval secs (default 3)")
    ap.add_argument("--rpc-url", default=os.environ.get("POLYGON_RPC_URL"))
    ap.add_argument("--spkg", default=DEFAULT_SPKG)
    ap.add_argument("--module", default=DEFAULT_MODULE)
    ap.add_argument("--out-dir", default=None, help="dir to write events.jsonl + summary.json")
    ap.add_argument("--analyze-only", default=None, help="path to events.jsonl to re-analyze")
    args = ap.parse_args()

    if args.analyze_only:
        events = []
        meta = {"endpoint": "?", "rpc_url_redacted": "?", "spkg": "?", "module": "?"}
        with open(args.analyze_only) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                if obj.get("kind") == "_meta":
                    meta = obj["meta"]
                else:
                    events.append(obj)
        s = summarize(events, meta)
        print_summary(s)
        out = os.path.join(os.path.dirname(args.analyze_only) or ".", "summary.json")
        with open(out, "w") as f:
            json.dump(s, f, indent=2)
        print(f"wrote {out}")
        return

    if not args.rpc_url:
        sys.exit("POLYGON_RPC_URL not set (env or --rpc-url)")

    meta = {
        "endpoint": os.environ.get("SUBSTREAMS_ENDPOINT", DEFAULT_ENDPOINT),
        "rpc_url_redacted": redact_rpc(args.rpc_url),
        "spkg": args.spkg,
        "module": args.module,
        "rpc_interval": args.rpc_interval,
        "requested_duration": args.duration,
    }
    print(f"collecting for {args.duration}s ...", file=sys.stderr)
    print(f"  substreams: {meta['endpoint']}", file=sys.stderr)
    print(f"  rpc:        {meta['rpc_url_redacted']}", file=sys.stderr)

    c = Collector(args)
    c.run()
    meta["session"] = c.session
    s = summarize(c.events, meta)
    print_summary(s)

    if args.out_dir:
        os.makedirs(args.out_dir, exist_ok=True)
        ev_path = os.path.join(args.out_dir, "events.jsonl")
        with open(ev_path, "w") as f:
            f.write(json.dumps({"kind": "_meta", "meta": meta}) + "\n")
            for e in c.events:
                f.write(json.dumps(e) + "\n")
        with open(os.path.join(args.out_dir, "summary.json"), "w") as f:
            json.dump(s, f, indent=2)
        print(f"wrote {ev_path} and summary.json", file=sys.stderr)


if __name__ == "__main__":
    main()
