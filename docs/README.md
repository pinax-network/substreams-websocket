# docs/

Reference material that is **not** loaded at runtime or compile time. Audience: contributors and AI agents (e.g. Claude Code) making non-trivial changes to this repo. The runtime user docs are at [`README.md`](../README.md) and [`public/SKILL.md`](../public/SKILL.md).

## Index

| File | Subject |
|------|---------|
| [`binance-websocket.md`](binance-websocket.md) | URL conventions + live SUBSCRIBE protocol mirrored from Binance market streams |
| [`substreams.md`](substreams.md) | Substreams concepts, DatabaseChanges proto, module hash algorithm |
| [`auth.md`](auth.md) | Pinax JWT exchange and alternative auth modes |
| [`cursors-and-resume.md`](cursors-and-resume.md) | Cursor persistence semantics and what "exact resume" means here |
| [`replay.md`](replay.md) | Per-stream JSONL replay log for client reconnects (`?from_block=<n>`) |
| [`graceful-shutdown.md`](graceful-shutdown.md) | SIGTERM drain protocol — clean `Close` to every client before exit |
| [`decisions.md`](decisions.md) | Log of significant design decisions made during dev |
| [`railway.md`](railway.md) | Railway deployment specifics (inline TOML, volumes, GHCR image) |
| [`sample.db_out.json`](sample.db_out.json) | Captured `db_out` response showing raw upstream JSON shape (pre-normalization) |
