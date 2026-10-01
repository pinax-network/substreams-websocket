# Transfer fan-out performance

Tracking: [issue #101](https://github.com/pinax-network/substreams-websocket/issues/101).

## Observed production bottleneck (2026-10-01)

Read-only VictoriaMetrics queries at 20:26 UTC and Flux MCP pod logs between 20:27 and
20:36 UTC showed `svm_transfers@v0.3.0` falling behind on `riv-prod-pinax-api-a`, running
application version 0.6.8. At 20:26, `solana@spl_transfer` and `solana@system_transfer`
both reported 1,050 seconds of drift, versus 3.3 seconds for swaps on the same pod.
Transfers processed 3.54 blocks/s over 15 minutes; swaps processed 3.74 blocks/s.
The backend had 27 connections.

The `block hot-path profile` logs isolate the local processing cost: transfers averaged
7.94 ms to decode and 227.75 ms to group/serialize/filter/fan out at 20:27. Later windows
showed broadcast averages between 238.81 and 323.97 ms, against a roughly 267 ms block
interval. Backlog varied with load; these measurements do not imply a constant lag rate.
The shared tier-1 head drift was 2.6 seconds, but that alone does not qualify the
package-specific upstream output latency.

The server previously cloned the entire block for every filtered client, including rows
that would immediately be discarded. It also linearly scanned every OR-list literal for
each event, and copied serialized JSON text for every unfiltered client. The synthetic
benchmark below reproduces those costs. It does not expose or assume production clients'
actual filter expressions.

## Implementation

- Compile same-field OR literals into ASCII-normalized hash sets after validating the
  original expression's field/term limits. This also covers bare comma lists and nested
  boolean expressions. The original expression remains the `LIST_FILTERS` representation.
- Select borrowed event rows and serialize the filtered view. All matching exact and
  wildcard filters still intersect; zero surviving rows produce no block frame.
- Share unfiltered raw/wrapped text using Axum's reference-counted `Utf8Bytes`.
  Queue bounds, `try_send`, drop accounting, cursors, and disconnect limits retain their
  existing behavior.

## Offline benchmark

Apple M1 Max, release build, Rust 1.98.0; 750 synthetic transfer rows, 27 subscribers,
100 iterations per scenario. Grouping, per-table payload construction, filtering,
serialization, queueing, and queue draining are included. Protobuf decode, cursor writes,
gRPC, and socket transport are excluded. Each row includes transfer addresses, amount,
signature, program, and signers. Clients alternate raw and wrapped envelopes.

Baseline: main commit `8d02c88` with the same benchmark harness. Optimized timings are
from this change on the same machine. Values are mean milliseconds per block, not a
production latency forecast.

| Scenario | Baseline | Optimized | Speedup |
|---|---:|---:|---:|
| Unfiltered | 1.727 | 1.556 | 1.1x |
| One field filter, one matching row per client | 14.008 | 2.973 | 4.7x |
| 119-wallet OR list across three fields, no matching rows | 414.556 | 6.754 | 61.4x |

Run without a live upstream or credentials:

```bash
cargo test --release --locked transfer_fanout_benchmark -- --ignored --nocapture
```

This is an ignored measurement test, with no timing threshold in CI. Regression tests
compare indexed matching with the original expression interpreter across mixed-case,
missing/non-string fields, Unicode, duplicates, quoting, AND/OR/NOT, and empty filters.
Fan-out tests compare raw/wrapped frames byte-for-byte with the previous filtering path,
including wildcard intersections, row/field order, escaping, and no-match suppression.

## Rollout acceptance

Production has not been changed by this investigation. Review and release the application,
then validate the tagged release under subscribed transfer load in stage before promoting
the production image through the owning `k8s-ws` overlay. Dev tracks builds; stage tracks
semver releases; production currently pins its version.

Observe at least two windows ten minutes apart, and include a busier transfer interval:

- `block hot-path profile` broadcast cost stays comfortably below the measured block interval.
- `svm_transfers` block rate reaches chain speed; drift remains low or its existing backlog
  shrinks. Low drift immediately after pod replacement is insufficient evidence.
- `broadcast_dropped_total`, force-closes, reconnects, and cursor-save errors remain acceptable
  relative to the pre-rollout baseline; verify actual client delivery and filter results.

The current `k8s-ws` deployment uses `emptyDir` for cursors. Replacing a pod starts near
live head by default and discards the backlog; clients backfill any missed window through
Substreams. That effect must not be mistaken for proof that throughput recovered.
