# Cursors and resume

What the server persists between restarts, and what "exact resume" actually buys you.

## File layout

`SUBSTREAMS_WEBSOCKET_CURSORS_DIR` (default `./cursors`) holds one file per spkg:

```
cursors/<network>-<package_name>@<package_version>-<module_hash>.cursor
```

- `network` comes from the stream's TOML entry.
- `package_name` + `package_version` come from the spkg's `Package.package_meta[0]` (same as `substreams info`).
- `module_hash` is computed locally from the `.spkg` — see [`substreams.md`](substreams.md#module-hash).

Keying on `(network, package_name, package_version, module_hash)` means a spkg upgrade — new version, new module hash, new inputs — writes to a new file. Old cursors stay on disk but are never read again. Safe by construction. Renaming a TOML entry has no effect; identity is anchored in the spkg.

## Write semantics

- Cursor is updated on every `BlockScopedData` and every `BlockUndoSignal`.
- Write is `tmp file + rename` — atomic on POSIX. A crash mid-write leaves either the old cursor or the new one, never a truncated file.
- No fsync. We accept losing the last few seconds of cursor progress on hard power loss; the upstream resume will just re-emit a handful of blocks.

## What "exact resume" means

On restart:

1. Read cursor file → string `C`.
2. Send `Blocks` request with `start_cursor = C`.
3. Upstream replays from the block **after** `C`, not from `C` itself.

So on the WebSocket side, clients connecting *during the gap* see no events; clients connecting *after resume* see the new blocks land in order. There is no in-memory replay buffer — if a client was disconnected when block N was fanned out, block N is lost to that client.

If you need historical replay, point a fresh consumer at the gRPC endpoint directly with `start_block_num`.

## Reorg handling

`BlockUndoSignal` carries `last_valid_cursor`. We persist that cursor and broadcast an `undo` envelope (see [`public/SKILL.md`](../public/SKILL.md)) so subscribed clients can roll back their local state. We never replay the undone blocks ourselves — that is the client's job using the original block deliveries.

## Operational notes

- Wipe a single cursor to force a re-sync of one spkg: `rm cursors/<network>-<package_name>@<package_version>-<module_hash>.cursor`.
- Wipe the directory to re-sync everything.
- On Railway / Fly / Heroku, the cursors dir lives on ephemeral storage. Cold deploys lose cursors and re-sync from the manifest's `initial_block`. For long-running deploys, mount a volume at `SUBSTREAMS_WEBSOCKET_CURSORS_DIR`.
