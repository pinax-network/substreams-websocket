// Embedded into the landing page at build time. No credentials leave this
// object except the configured HTTPS exchange body and the WS token parameter.
function createTryItAuth(issuer, pageUrl) {
  'use strict';
  var page = new URL(pageUrl);
  var cached = null;
  var controller = null;
  var generation = 0;
  var timeoutMs = 10000;

  function failure(message) {
    var error = new Error(message);
    error.uiSafe = true;
    return error;
  }
  function canceled() { return new DOMException('Connection canceled', 'AbortError'); }
  function clear() {
    generation++;
    if (controller) controller.abort();
    controller = null;
    cached = null;
  }
  function cancel() {
    generation++;
    if (controller) controller.abort();
    controller = null;
  }
  function cleanTarget(value) {
    var target;
    try { target = new URL(value); } catch (_) { throw failure('Enter a valid WebSocket URL.'); }
    if (!/^wss?:$/.test(target.protocol) || target.username || target.password || target.hash) {
      throw failure('Use a WebSocket URL without credentials or a fragment.');
    }
    // A pasted URL never supplies credentials. Use the password field instead.
    Array.from(target.searchParams.keys()).forEach(function (key) {
      if (/^(token|api_key|apikey|key|access_token|authorization)$/i.test(key)) target.searchParams.delete(key);
    });
    return target;
  }
  function validateToken(value) {
    if (typeof value !== 'string' || value.length > 16384 || !/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(value)) {
      throw failure('Enter a JWT, or choose API key to exchange a project key.');
    }
    var payload;
    try {
      payload = JSON.parse(atob(value.split('.')[1].replace(/-/g, '+').replace(/_/g, '/')));
    } catch (_) { throw failure('The JWT is malformed.'); }
    if (!payload || typeof payload !== 'object') throw failure('The JWT is malformed.');
    // This is a local freshness check, not signature verification. The gateway
    // remains responsible for validating the token and its permissions.
    if (payload.exp !== undefined && (!Number.isFinite(payload.exp) || payload.exp * 1000 <= Date.now() + 30000)) {
      throw failure('The JWT has expired or expires shortly. Obtain a fresh JWT.');
    }
    return payload.exp;
  }
  async function prepare(value, mode, credential) {
    cancel();
    var current = generation;
    var target = cleanTarget(value);
    if (mode !== 'none' && mode !== 'jwt' && mode !== 'api_key') throw failure('Choose an authentication method.');
    if (mode === 'none') return { url: target.href, display: target.origin + target.pathname, authenticated: false };
    if (page.protocol !== 'https:' || target.protocol !== 'wss:' || target.host !== page.host) {
      throw failure('Credentials can only be used with this site’s secure WebSocket endpoint.');
    }
    credential = credential.trim();
    if (!credential || credential.length > 16384 || /[\x00-\x20\x7f]/.test(credential)) {
      throw failure(mode === 'api_key' ? 'Enter an API key.' : 'Enter a JWT without the Bearer prefix.');
    }
    var token;
    if (mode === 'jwt') {
      validateToken(credential);
      token = credential;
    } else {
      var exchange;
      try { exchange = new URL(issuer); } catch (_) { throw failure('API-key exchange is not configured for this site.'); }
      if (exchange.protocol !== 'https:' || exchange.username || exchange.password || exchange.search || exchange.hash) {
        throw failure('API-key exchange is not configured for this site.');
      }
      if (cached && cached.key === credential && cached.expires * 1000 > Date.now() + 30000) {
        token = cached.token;
      } else {
        controller = new AbortController();
        var active = controller;
        var timer = setTimeout(function () { active.abort(); }, timeoutMs);
        var response;
        var data;
        try {
          response = await fetch(exchange.href, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
            body: JSON.stringify({ api_key: credential }),
            credentials: 'omit', redirect: 'error', cache: 'no-store',
            referrerPolicy: 'no-referrer', signal: active.signal,
          });
          if (current !== generation) throw canceled();
          if (!response.ok) {
            throw failure(response.status === 400 || response.status === 401 || response.status === 403 || response.status === 404
              ? 'API-key exchange rejected the key. Check that it belongs to this environment.'
              : 'API-key exchange is unavailable. Try again later.');
          }
          // Never show the response body or an exception containing it.
          try {
            var reader = response.body.getReader();
            var chunks = [];
            var size = 0;
            while (true) {
              var chunk = await reader.read();
              if (chunk.done) break;
              size += chunk.value.byteLength;
              if (size > 32768) {
                await reader.cancel();
                throw failure('API-key exchange returned an invalid response.');
              }
              chunks.push(chunk.value);
            }
            var bytes = new Uint8Array(size);
            var offset = 0;
            chunks.forEach(function (chunk) { bytes.set(chunk, offset); offset += chunk.byteLength; });
            data = JSON.parse(new TextDecoder().decode(bytes));
          } catch (_) { throw failure('API-key exchange returned an invalid response.'); }
          if (current !== generation) throw canceled();
          var tokenExpiry;
          try { tokenExpiry = validateToken(data && data.token); } catch (_) { throw failure('API-key exchange returned an invalid or expired token.'); }
          if (!Number.isSafeInteger(data.expires_at) || data.expires_at * 1000 <= Date.now() + 30000) {
            throw failure('API-key exchange returned an invalid or expired token.');
          }
          cached = { key: credential, token: data.token, expires: tokenExpiry === undefined ? data.expires_at : Math.min(data.expires_at, tokenExpiry) };
          token = cached.token;
        } catch (error) {
          if (current !== generation) throw canceled();
          if (active.signal.aborted) throw failure('API-key exchange timed out. Try again.');
          // Our own messages are fixed strings. Fetch errors can contain URLs,
          // and upstream bodies must never reach the console or the UI.
          if (error && error.uiSafe) throw error;
          throw failure('Could not reach API-key exchange. Check connectivity and browser access for this site.');
        } finally {
          clearTimeout(timer);
          if (controller === active) controller = null;
        }
      }
    }
    if (current !== generation) throw canceled();
    target.searchParams.set('token', token);
    return { url: target.href, display: target.origin + target.pathname, authenticated: true };
  }
  return { prepare: prepare, clear: clear, cancel: cancel, cleanTarget: cleanTarget };
}
