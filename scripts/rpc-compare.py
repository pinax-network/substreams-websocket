#!/usr/bin/env python3
"""
Head-to-head RPC comparison: poll two Polygon RPC endpoints on the SAME samples
and report which one is behind.

Each round polls both endpoints back-to-back (same wall-clock moment) with
eth_getBlockByNumber("latest"), so block-number and timestamp differences are a
fair side-by-side. Any path segment longer than 20 chars is redacted (`***`) so
API keys never land in output or saved data.

Usage:
    ./scripts/rpc-compare.py \
        --a "ours=https://polygon.rpc.service.pinax.network/v1/<key>/" \
        --b "drpc=https://lb.drpc.live/polygon/<key>" \
        --duration 180 --interval 2 --out-dir runs/rpc-3min
"""

import argparse
import json
import os
import re
import statistics
import sys
import time
import urllib.request


def redact(url: str) -> str:
    return re.sub(r"/[A-Za-z0-9_-]{20,}", "/***", url)


def rpc_latest(url: str, timeout: float = 10.0):
    body = json.dumps(
        {"jsonrpc": "2.0", "id": 1, "method": "eth_getBlockByNumber", "params": ["latest", False]}
    ).encode()
    req = urllib.request.Request(url, data=body, headers={"content-type": "application/json"})
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = json.load(resp)
    result = data["result"]
    return int(result["number"], 16), int(result["timestamp"], 16), round((time.time() - t0) * 1000, 1)


def pctl(values, p):
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(s) - 1)
    return s[f] + (s[c] - s[f]) * (k - f)


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


def parse_named(arg):
    label, _, url = arg.partition("=")
    if not url:
        sys.exit(f"bad --a/--b value (need label=url): {arg}")
    return label, url


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--a", required=True, help="label=url for endpoint A (e.g. ours=https://...)")
    ap.add_argument("--b", required=True, help="label=url for endpoint B (e.g. drpc=https://...)")
    ap.add_argument("--duration", type=int, default=180)
    ap.add_argument("--interval", type=float, default=2.0)
    ap.add_argument("--out-dir", default=None)
    args = ap.parse_args()

    la, ua = parse_named(args.a)
    lb, ub = parse_named(args.b)
    meta = {"a_label": la, "a_url": redact(ua), "b_label": lb, "b_url": redact(ub),
            "interval": args.interval, "duration": args.duration}
    print(f"comparing {la} vs {lb} for {args.duration}s @ {args.interval}s", file=sys.stderr)
    print(f"  {la}: {meta['a_url']}", file=sys.stderr)
    print(f"  {lb}: {meta['b_url']}", file=sys.stderr)

    rounds = []
    start = time.time()
    next_t = start
    last_log = start
    while time.time() - start < args.duration:
        t = time.time()
        rec = {"t": t}
        for key, url in (("a", ua), ("b", ub)):
            try:
                blk, ts, rtt = rpc_latest(url)
                rec[key] = {"block": blk, "block_ts": ts, "rtt_ms": rtt}
            except Exception as e:  # noqa: BLE001
                rec[key] = {"error": str(e)}
        rounds.append(rec)
        if t - last_log >= 15:
            last_log = t
            a, b = rec.get("a", {}), rec.get("b", {})
            if "block" in a and "block" in b:
                diff = a["block"] - b["block"]
                print(f"[{int(t-start):>4}s] {la}={a['block']} {lb}={b['block']} "
                      f"{la}-{lb}={diff:+d}blk  {la}_delay={t-a['block_ts']:.1f}s "
                      f"{lb}_delay={t-b['block_ts']:.1f}s", file=sys.stderr, flush=True)
        next_t += args.interval
        time.sleep(max(0.0, next_t - time.time()))

    # Analysis over rounds where both succeeded.
    both = [r for r in rounds if "block" in r.get("a", {}) and "block" in r.get("b", {})]
    a_minus_b = [r["a"]["block"] - r["b"]["block"] for r in both]  # +ve => A ahead
    a_delay = [r["t"] - r["a"]["block_ts"] for r in both]
    b_delay = [r["t"] - r["b"]["block_ts"] for r in both]
    a_rtt = [r["a"]["rtt_ms"] for r in both]
    b_rtt = [r["b"]["rtt_ms"] for r in both]
    a_err = sum(1 for r in rounds if "error" in r.get("a", {}))
    b_err = sum(1 for r in rounds if "error" in r.get("b", {}))

    summary = {
        "meta": meta,
        "counts": {"rounds": len(rounds), "both_ok": len(both),
                   f"{la}_errors": a_err, f"{lb}_errors": b_err},
        f"{la}_minus_{lb}_block": stats(a_minus_b, "blocks"),
        f"{la}_behind_share": round(sum(1 for d in a_minus_b if d < 0) / len(a_minus_b), 3) if a_minus_b else None,
        f"{lb}_behind_share": round(sum(1 for d in a_minus_b if d > 0) / len(a_minus_b), 3) if a_minus_b else None,
        "even_share": round(sum(1 for d in a_minus_b if d == 0) / len(a_minus_b), 3) if a_minus_b else None,
        f"{la}_delay_vs_now": stats(a_delay, "secs"),
        f"{lb}_delay_vs_now": stats(b_delay, "secs"),
        f"{la}_rtt_ms": stats(a_rtt, "ms"),
        f"{lb}_rtt_ms": stats(b_rtt, "ms"),
    }

    print("\n" + "=" * 64)
    print(f"RPC HEAD COMPARISON: {la} vs {lb}")
    print("=" * 64)
    print(f"{la}: {meta['a_url']}")
    print(f"{lb}: {meta['b_url']}")
    print(f"rounds={len(rounds)} both_ok={len(both)} {la}_err={a_err} {lb}_err={b_err}")
    print("-" * 64)

    def row(label, st):
        if not st:
            print(f"{label:<24}: (no data)"); return
        print(f"{label:<24}: min={st['min']} p50={st['p50']} avg={st['avg']} "
              f"p90={st['p90']} p95={st['p95']} max={st['max']} {st['unit']}")

    row(f"{la}-{lb} block diff", summary[f"{la}_minus_{lb}_block"])
    print(f"{'who is behind':<24}: {la}_behind={summary[f'{la}_behind_share']} "
          f"even={summary['even_share']} {lb}_behind={summary[f'{lb}_behind_share']}")
    row(f"{la} delay vs now", summary[f"{la}_delay_vs_now"])
    row(f"{lb} delay vs now", summary[f"{lb}_delay_vs_now"])
    row(f"{la} rtt", summary[f"{la}_rtt_ms"])
    row(f"{lb} rtt", summary[f"{lb}_rtt_ms"])
    print("=" * 64 + "\n")

    if args.out_dir:
        os.makedirs(args.out_dir, exist_ok=True)
        with open(os.path.join(args.out_dir, "rounds.jsonl"), "w") as f:
            f.write(json.dumps({"kind": "_meta", "meta": meta}) + "\n")
            for r in rounds:
                f.write(json.dumps(r) + "\n")
        with open(os.path.join(args.out_dir, "summary.json"), "w") as f:
            json.dump(summary, f, indent=2)
        print(f"wrote {args.out_dir}/rounds.jsonl and summary.json", file=sys.stderr)


if __name__ == "__main__":
    main()
