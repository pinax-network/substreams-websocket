const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

const source = fs.readFileSync(path.join(__dirname, '../public/tryit-auth.js'), 'utf8');
const page = 'https://streams.example/';
const endpoint = 'wss://streams.example/ws/test@transfers';
const issuer = 'https://auth.example/v1/auth/issue';
const apiKey = 'synthetic-project-key';
const now = 1800000000000;
function jwt(expires = now / 1000 + 3600) {
  return 'eyJhbGciOiJIUzI1NiJ9.' + Buffer.from(JSON.stringify({ exp: expires })).toString('base64url') + '.c3ludGhldGlj';
}
function harness(fetchImpl, configuredIssuer = issuer) {
  let clock = now;
  let timeout;
  const calls = [];
  const context = vm.createContext({
    URL, AbortController, DOMException, TextDecoder, Uint8Array, atob,
    Date: { now: () => clock },
    setTimeout: (callback) => { timeout = callback; return 1; },
    clearTimeout: () => { timeout = null; },
    fetch: async (...args) => {
      calls.push(args);
      return fetchImpl ? fetchImpl(...args) : Response.json({ token: jwt(), expires_at: now / 1000 + 3600 });
    },
  });
  vm.runInContext(source, context);
  return {
    auth: context.createTryItAuth(configuredIssuer, page), calls,
    advance: (ms) => { clock += ms; },
    expireTimeout: () => timeout(),
  };
}

test('API key only reaches the configured issuer body; JWT only reaches the upgrade', async () => {
  const { auth, calls } = harness();
  const result = await auth.prepare(endpoint + '?filter=amount%3E0', 'api_key', apiKey);
  assert.equal(calls.length, 1);
  const [url, options] = calls[0];
  assert.equal(url, issuer);
  assert.equal(options.body, JSON.stringify({ api_key: apiKey }));
  assert.equal(options.credentials, 'omit');
  assert.equal(options.redirect, 'error');
  assert.equal(options.referrerPolicy, 'no-referrer');
  assert.equal(options.cache, 'no-store');
  assert.equal(options.headers['Content-Type'], 'application/json');
  assert.equal(new URL(result.url).searchParams.get('token'), jwt());
  assert.equal(new URL(result.url).searchParams.get('filter'), 'amount>0');
  assert.equal(result.display, endpoint.replace('wss:', 'wss:'));
  assert.ok(!result.url.includes(apiKey));
  assert.ok(!result.display.includes(jwt()));
});

test('JWT and unauthenticated modes work without an issuer or an exchange', async () => {
  const { auth, calls } = harness(null, '');
  const result = await auth.prepare(endpoint, 'jwt', jwt());
  assert.equal(new URL(result.url).searchParams.get('token'), jwt());
  const none = await auth.prepare(endpoint + '?token=old&api_key=old&APIKEY=old&filter=ok', 'none', '');
  assert.equal(none.url, endpoint + '?filter=ok');
  assert.equal(none.authenticated, false);
  assert.equal(calls.length, 0);
});

test('a raw key in JWT mode gives an actionable error without echoing it', async () => {
  const { auth, calls } = harness();
  await assert.rejects(auth.prepare(endpoint, 'jwt', apiKey), /choose API key/);
  assert.equal(calls.length, 0);
});

test('authenticated target cannot exfiltrate to another host or insecure socket', async () => {
  const { auth, calls } = harness();
  for (const target of ['wss://evil.example/ws', 'wss://streams.example.evil.example/ws', 'ws://streams.example/ws', 'wss://streams.example:444/ws', 'wss://user:secret@streams.example/ws', 'javascript:alert(1)', endpoint + '#token=secret']) {
    await assert.rejects(auth.prepare(target, 'api_key', apiKey));
  }
  assert.equal(calls.length, 0);
});

test('API-key mode rejects missing/untrusted issuer URLs before sending anything', async () => {
  for (const value of ['', '/exchange', 'http://auth.example/issue', 'https://user:secret@auth.example/issue', issuer + '?key=secret', issuer + '#fragment']) {
    const { auth, calls } = harness(null, value);
    await assert.rejects(auth.prepare(endpoint, 'api_key', apiKey), /not configured/);
    assert.equal(calls.length, 0);
  }
});

test('cached token reused, exchanged again near expiry or after clearing', async () => {
  const { auth, calls, advance } = harness((_url, _options) => Response.json({ token: jwt(now / 1000 + 7200), expires_at: now / 1000 + 3600 }));
  await auth.prepare(endpoint, 'api_key', apiKey);
  await auth.prepare(endpoint, 'api_key', apiKey);
  assert.equal(calls.length, 1);
  advance(3590000);
  await assert.rejects(auth.prepare(endpoint, 'api_key', apiKey), /expired/);
  assert.equal(calls.length, 2);
  auth.clear();
  await assert.rejects(auth.prepare(endpoint, 'api_key', apiKey), /expired/);
  assert.equal(calls.length, 3);
});

test('changing a key cannot reuse the prior key’s token', async () => {
  const { auth, calls } = harness();
  await auth.prepare(endpoint, 'api_key', apiKey);
  await auth.prepare(endpoint, 'api_key', 'another-synthetic-key');
  assert.equal(calls.length, 2);
});

test('issuer rejection and network errors never echo credentials or response bodies', async () => {
  for (const status of [400, 401, 403, 404, 429, 500]) {
    const { auth } = harness(() => new Response(apiKey + jwt(), { status }));
    await assert.rejects(auth.prepare(endpoint, 'api_key', apiKey), (error) => {
      assert.ok(!error.message.includes(apiKey));
      assert.ok(!error.message.includes(jwt()));
      assert.match(error.message, /^API-key exchange/);
      return true;
    });
  }
  const { auth } = harness(() => { throw new Error('network ' + apiKey); });
  await assert.rejects(auth.prepare(endpoint, 'api_key', apiKey), /^Error: Could not reach API-key exchange/);
});

test('malformed, expired, or oversized exchange responses fail closed', async () => {
  for (const response of [
    () => new Response('<html>' + apiKey),
    () => Response.json({ token: apiKey, expires_at: now / 1000 + 3600 }),
    () => Response.json({ token: jwt(), expires_at: now / 1000 - 1 }),
    () => Response.json({ token: jwt(), expires_at: 'tomorrow' }),
    () => Response.json({ token: jwt(now / 1000 - 1), expires_at: now / 1000 + 3600 }),
    () => new Response(' '.repeat(32769)),
  ]) {
    const { auth } = harness(response);
    await assert.rejects(auth.prepare(endpoint, 'api_key', apiKey), /invalid|expired/);
  }
});

test('cancellation prevents a late exchange result from opening a socket', async () => {
  let resolve;
  let signal;
  const { auth } = harness((_url, options) => {
    signal = options.signal;
    return new Promise((done) => { resolve = done; });
  });
  const pending = auth.prepare(endpoint, 'api_key', apiKey);
  auth.clear();
  assert.equal(signal.aborted, true);
  resolve(Response.json({ token: jwt(), expires_at: now / 1000 + 3600 }));
  await assert.rejects(pending, { name: 'AbortError' });
});

test('exchange timeout is bounded and produces a fixed safe message', async () => {
  const { auth, expireTimeout } = harness((_url, options) => new Promise((_resolve, reject) => {
    options.signal.addEventListener('abort', () => reject(new DOMException(apiKey, 'AbortError')));
  }));
  const pending = auth.prepare(endpoint, 'api_key', apiKey);
  expireTimeout();
  await assert.rejects(pending, /API-key exchange timed out/);
});

test('edited URL credentials are stripped case-insensitively before every mode', async () => {
  const { auth } = harness();
  const result = await auth.prepare(endpoint + '?%74oken=secret&token=other&access_token=x&Authorization=y&filter=ok', 'jwt', jwt());
  const target = new URL(result.url);
  assert.deepEqual([...target.searchParams.keys()], ['filter', 'token']);
  assert.equal(target.searchParams.get('token'), jwt());
  assert.ok(!result.display.includes('secret'));
});

test('landing scripts parse, keep credentials masked, and remove legacy saved JWTs', () => {
  const html = fs.readFileSync(path.join(__dirname, '../public/index.html'), 'utf8').replace('__UI_AUTH_SCRIPT__', source);
  for (const [, script] of html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)) new vm.Script(script);
  assert.ok(!/sessionStorage/.test(html));
  assert.ok(!/setItem\(\s*['"]substreams-ws-ui-jwt['"]/.test(html));
  assert.match(html, /localStorage\.removeItem\('substreams-ws-ui-jwt'\)/);
  assert.match(html, /id="jwt-input"\s+type="password"/);
});


function memoryStorage() {
  const values = new Map();
  return {
    values,
    getItem: (key) => values.has(key) ? values.get(key) : null,
    setItem: (key, value) => values.set(key, value),
    removeItem: (key) => values.delete(key),
  };
}
function keyStore(storage, configuredIssuer = issuer, pageUrl = page) {
  const context = vm.createContext({ URL, encodeURIComponent });
  vm.runInContext(source, context);
  return context.createTryItKeyStore(configuredIssuer, pageUrl, storage);
}

test('API key survives a fresh page instance and is trimmed before saving', () => {
  const storage = memoryStorage();
  const original = keyStore(storage);
  assert.equal(original.load(), '');
  assert.equal(original.status(), 'empty');
  assert.equal(original.save('  ' + apiKey + '\n'), apiKey);
  assert.equal(original.status(), 'saved');
  assert.equal(storage.values.size, 1);
  assert.deepEqual([...storage.values.values()], [apiKey]);
  const reopened = keyStore(storage, 'https://AUTH.example:443/v1/auth/issue', page + 'other-page');
  assert.equal(reopened.load(), apiKey);
  assert.equal(reopened.status(), 'saved');
});

test('saved API keys are isolated by page origin and configured issuer', () => {
  const storage = memoryStorage();
  keyStore(storage).save(apiKey);
  for (const [otherIssuer, otherPage] of [
    ['https://other-auth.example/v1/auth/issue', page],
    [issuer + '/other', page],
    [issuer, 'https://other-streams.example/'],
    [issuer, 'https://streams.example:8443/'],
  ]) {
    const other = keyStore(storage, otherIssuer, otherPage);
    assert.equal(other.load(), '');
    other.save('other-synthetic-key');
  }
  assert.equal(keyStore(storage).load(), apiKey);
});

test('Clear forgets the key in memory and for subsequent page instances', () => {
  const storage = memoryStorage();
  const store = keyStore(storage);
  store.save(apiKey);
  assert.equal(store.save(''), '');
  assert.equal(store.status(), 'empty');
  assert.equal(store.load(), '');
  assert.equal(storage.values.size, 0);
  assert.equal(keyStore(storage).load(), '');
});

test('missing or unsafe issuer and insecure pages never access credential storage', () => {
  const storage = new Proxy({}, { get() { assert.fail('disabled scope accessed storage'); } });
  for (const [configuredIssuer, pageUrl] of [
    ['', page], ['/exchange', page], ['http://auth.example/issue', page],
    ['https://user:secret@auth.example/issue', page], [issuer + '?token=x', page],
    [issuer + '#fragment', page], [issuer, 'http://streams.example/'],
    [issuer, 'not a URL'],
  ]) {
    const store = keyStore(storage, configuredIssuer, pageUrl);
    assert.equal(store.load(), '');
    assert.equal(store.save(apiKey), '');
    assert.equal(store.status(), 'disabled');
  }
});

test('blocked storage keeps the current key for this tab without claiming it is saved', () => {
  for (const storage of [null, {
    getItem() { throw new Error('blocked ' + apiKey); },
    setItem() { throw new Error('full ' + apiKey); },
    removeItem() { throw new Error('blocked ' + apiKey); },
  }]) {
    const store = keyStore(storage);
    assert.equal(store.load(), '');
    assert.equal(store.status(), 'unavailable');
    assert.equal(store.save(apiKey), apiKey);
    assert.equal(store.load(), apiKey);
    assert.equal(store.status(), 'unavailable');
    assert.equal(store.save(''), '');
    assert.equal(store.load(), '');
    assert.equal(store.status(), 'unavailable');
  }
});

test('failed updates do not restore a stale saved key into the current tab', () => {
  const storage = memoryStorage();
  keyStore(storage).save('previous-synthetic-key');
  const store = keyStore(storage);
  assert.equal(store.load(), 'previous-synthetic-key');
  storage.setItem = () => { throw new Error('full'); };
  storage.removeItem = () => { throw new Error('blocked'); };
  store.save(apiKey);
  assert.equal(store.load(), apiKey);
  assert.equal(store.status(), 'unavailable');
  store.save('');
  assert.equal(store.load(), '');
  assert.equal(store.status(), 'unavailable');
  // The warning must not promise that failed removal changed persistent storage.
  assert.equal(keyStore(storage).load(), 'previous-synthetic-key');
});

test('malformed or oversized persisted keys are discarded without exposure', () => {
  for (const invalid of ['', ' key-with-padding ', 'key with spaces', 'key\u0000value', 'x'.repeat(16385), 42]) {
    const storage = memoryStorage();
    keyStore(storage).save(apiKey);
    const [name] = storage.values.keys();
    storage.values.set(name, invalid);
    const store = keyStore(storage);
    assert.equal(store.load(), '');
    assert.equal(store.status(), 'empty');
    assert.equal(storage.values.size, 0);
  }
});
