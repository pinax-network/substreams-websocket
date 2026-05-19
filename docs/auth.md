# Substreams auth

How the server gets a credential past Pinax (or any Substreams provider) at the gRPC layer.

## Two inputs, three modes

Inputs:

- `SUBSTREAMS_API_KEY` — long-lived key, e.g. issued by the Pinax dashboard.
- `SUBSTREAMS_TOKEN` — a pre-minted JWT. Skips exchange entirely.

Modes the server picks between at startup:

1. **API key → JWT exchange (default for Pinax).**
   `SUBSTREAMS_API_KEY` set + `SUBSTREAMS_AUTH_URL=https://auth.pinax.network/v1/auth/issue`.
   Server POSTs `{ "api_key": "..." }`, parses `{ "token": "..." }`, and sends `Authorization: Bearer <jwt>` on every gRPC call. Mirrors what `substreams-js` does on the browser side.
2. **Raw bearer.**
   `SUBSTREAMS_TOKEN` set. Sent as `Authorization: Bearer <token>` verbatim. Use this if you mint JWTs out of band or run a non-Pinax provider that issues raw tokens.
3. **Header passthrough (graph-node style).**
   `SUBSTREAMS_API_KEY` set + `SUBSTREAMS_AUTH_URL=none`. No exchange. The key is sent in the header named by `SUBSTREAMS_API_KEY_HEADER` (default `X-Api-Key`). Use this when the provider accepts the API key directly on the gRPC metadata.

## Token lifetime

JWTs from Pinax are short-lived (~hours). The server exchanges once at startup. If the JWT expires mid-stream, the upstream will close the gRPC channel; our retry loop reconnects and re-runs the exchange. No manual rotation needed.

## References

- Pinax auth endpoint: `https://auth.pinax.network/v1/auth/issue`
- substreams-js exchange (reference impl): <https://github.com/substreams-js/substreams-js/blob/main/packages/node/src/connect.ts>

## WebSocket-side auth

This doc covers the server-to-upstream link. The server-to-client (WebSocket) side has no auth today — every client gets every configured stream. Put a reverse proxy in front if you need client auth.
