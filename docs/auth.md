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

## Browser Try It authentication

Try It supports a JWT, no credential, or an API key when the operator configures
`SUBSTREAMS_WEBSOCKET_UI_AUTH_URL` / `--ui-auth-url`. Embedded hosts set
`WebSocketConfig.ui_auth_url`. The value is a trusted absolute HTTPS endpoint,
without userinfo, query parameters or a fragment. There is no default issuer:
configure the issuer matching the gateway's credential environment. This setting
does not change upstream authentication or add authentication to this server.
API-key mode is selected when an issuer is configured; otherwise JWT is selected.
For an unauthenticated server, explicitly choose **None**.

In API-key mode the browser POSTs `{"api_key":"..."}` to that endpoint and expects
`{"token":"<JWT>","expires_at":<Unix seconds>}`. The issuer must allow the page's
origin through CORS, including `POST` and `Content-Type`. Browser cookies and
redirects are disabled. The exchange times out after ten seconds and rejects
responses larger than 32 KiB, malformed tokens, and tokens expiring within thirty
seconds. Reconnecting reuses the in-memory token until that freshness boundary,
then exchanges again. Reconnection is manual.

The browser supplies the JWT to the existing gateway through `?token=`, because
the WebSocket API cannot set `X-Api-Key` or `Authorization` on an upgrade. The
gateway must validate it, enforce access and quotas, and remove the credential
parameter before forwarding or logging. Raw API keys never enter the WebSocket
URL. Authenticated connections must use this page's own host over HTTPS/WSS;
editing the connection URL cannot send a credential to another host.

API keys are masked and automatically saved in this browser's local storage for
the current site and configured issuer. They survive closing the panel, reloading
the page and closing the browser. **Clear** in API-key mode removes the saved key;
clearing this site's browser data also removes it. Separate sites and issuers do
not share saved keys. Browser restrictions such as private browsing can prevent
persistence; the UI reports unavailable storage and keeps the key only in the
current tab. If a storage update fails, a previously saved key may be unchanged.

Manually entered JWTs and exchanged tokens stay in memory and are cleared when
the panel closes or the page leaves. Previously stored UI JWTs are removed from
local storage. Changing the credential or authentication mode cancels an
outstanding exchange and disconnects. Pasted credential query parameters are
removed from the URL. Error messages never include issuer response bodies or
token-bearing URLs.

Run the browser authentication regression checks with
`node --test scripts/tryit-auth.test.cjs` (Node 22 or newer). They use synthetic
credentials and mocked exchange responses; they make no live authenticated calls.
