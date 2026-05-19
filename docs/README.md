# docs/

Reference material that is **not** loaded at runtime or compile time.

## `sample.db_out.json`

A captured `db_out` module response (JSON form) showing the raw upstream
shape of `sf.substreams.sink.database.v1.DatabaseChanges` before the
server normalizes it. Useful for understanding which fields the decoder
strips (`ordinal`, `operation`, `updateOp`, `compositePk`, the
duplicate-of-block-header columns) and which ones survive as
`events[*]`.

The on-wire shape the WebSocket server emits is documented in
[`public/SKILL.md`](../public/SKILL.md) and [`README.md`](../README.md).
