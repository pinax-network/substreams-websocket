# docs/

Reference material that is **not** loaded at runtime or compile time. Audience: contributors and AI agents (e.g. Claude Code) making non-trivial changes to this repo. The runtime user docs are at [`README.md`](../README.md) and [`public/SKILL.md`](../public/SKILL.md).

## Index

| File | Subject |
|------|---------|
| [`binance-websocket.md`](binance-websocket.md) | URL conventions + live SUBSCRIBE protocol mirrored from Binance market streams |
| [`substreams.md`](substreams.md) | Substreams concepts, DatabaseChanges proto, module hash algorithm |
| [`auth.md`](auth.md) | Pinax JWT exchange and alternative auth modes |
| [`cursors-and-resume.md`](cursors-and-resume.md) | Cursor persistence semantics and what "exact resume" means here |
| [`replay.md`](replay.md) | Per-spkg JSONL replay log for client reconnects (`?from_timestamp=<n>`) |
| [`filters.md`](filters.md) | Per-subscription event filters (`?filter=`, `SET_FILTER`, `CLEAR_FILTER`, `LIST_FILTERS`) |
| [`graceful-shutdown.md`](graceful-shutdown.md) | SIGTERM drain protocol — clean `Close` to every client before exit |
| [`backpressure.md`](backpressure.md) | Per-client backpressure handling — `try_send`, drop counters, force-close on `Close(1013)` |
| [`metrics.md`](metrics.md) | Prometheus `/metrics` endpoint + full metric catalog |
| [`envoy.md`](envoy.md) | Running behind Envoy (or any reverse proxy) — health-check, idle timeout, buffer sizing |
| [`decisions.md`](decisions.md) | Log of significant design decisions made during dev |
| [`railway.md`](railway.md) | Railway deployment specifics (inline TOML, volumes, GHCR image) |
| [`sample.db_out.json`](sample.db_out.json) | Captured `db_out` response showing raw upstream JSON shape (pre-normalization) |
