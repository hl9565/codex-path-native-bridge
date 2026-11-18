import assert from 'node:assert/strict';
import { once } from 'node:events';
import http from 'node:http';
import test from 'node:test';
import { createPelicanServer } from '../scripts/serve-pelican.mjs';

async function listen(server, t) {
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => new Promise(resolve => {
    server.closeAllConnections();
    server.close(resolve);
  }));
  return `http://127.0.0.1:${server.address().port}`;
}

async function fixture(t, handler) {
  const upstream = http.createServer(handler);
  const gateway = await listen(upstream, t);
  const app = await listen(createPelicanServer(), t);
  return { app, gateway, headers: { Authorization: 'Bearer fixture-key', 'X-Pelican-Gateway': gateway + '/v1' } };
}

test('serves only the playground with a same-origin proxy marker', async t => {
  const base = await listen(createPelicanServer(), t);
  const response = await fetch(base + '/');
  assert.equal(response.status, 200);
  assert.match(response.headers.get('content-security-policy'), /connect-src 'self'/);
  assert.match(await response.text(), /<meta name="pelican-local-proxy" content="1">/);
  assert.equal((await fetch(base + '/Cargo.toml')).status, 404);
});

test('forwards the chosen API key and model route without requiring gateway CORS', async t => {
  let received;
  const f = await fixture(t, (request, response) => {
    received = { path: request.url, key: request.headers.authorization, cookie: request.headers.cookie };
    response.writeHead(200, { 'Content-Type': 'application/json' });
    response.end(JSON.stringify({ data: [{ id: 'fixture-model' }] }));
  });
  const response = await fetch(f.app + '/api/models', { headers: { ...f.headers, Cookie: 'must-not-forward=1' } });
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), { data: [{ id: 'fixture-model' }] });
  assert.deepEqual(received, { path: '/v1/models', key: 'Bearer fixture-key', cookie: undefined });
  assert.equal(response.headers.get('access-control-allow-origin'), null);
});

test('rejects cross-site origins and unregistered API routes before forwarding', async t => {
  let calls = 0;
  const f = await fixture(t, (_request, response) => { calls++; response.end('{}'); });
  assert.equal((await fetch(f.app + '/api/models', { headers: { ...f.headers, Origin: 'https://other.example' } })).status, 403);
  assert.equal((await fetch(f.app + '/api/admin', { headers: f.headers })).status, 404);
  assert.equal((await fetch(f.app + '/api/models', { headers: { 'X-Pelican-Gateway': f.gateway + '/v1' } })).status, 401);
  assert.equal(calls, 0);
});

test('does not follow gateway redirects or forward an API key to a new destination', async t => {
  const f = await fixture(t, (_request, response) => {
    response.writeHead(302, { Location: 'https://other.example/' });
    response.end();
  });
  const response = await fetch(f.app + '/api/models', { headers: f.headers });
  assert.equal(response.status, 502);
  assert.equal((await response.json()).error.code, 'gateway_redirect');
});

test('streams Responses bytes immediately and preserves the request body', { timeout: 5000 }, async t => {
  let receivedBody = '';
  let finish;
  const f = await fixture(t, async (request, response) => {
    for await (const chunk of request) receivedBody += chunk;
    response.writeHead(200, { 'Content-Type': 'text/event-stream', 'X-Request-Id': 'fixture-request' });
    response.write('data: {"type":"response.created"}\n\n');
    await new Promise(resolve => { finish = resolve; });
    response.end('data: {"type":"response.completed"}\n\n');
  });
  const body = JSON.stringify({ model: 'fixture-model', input: 'fixture prompt', stream: true });
  const response = await fetch(f.app + '/api/responses', {
    method: 'POST', headers: { ...f.headers, 'Content-Type': 'application/json' }, body,
  });
  assert.equal(response.headers.get('x-request-id'), 'fixture-request');
  const reader = response.body.getReader();
  const first = await reader.read();
  assert.match(new TextDecoder().decode(first.value), /response.created/);
  assert.equal(receivedBody, body);
  finish();
  let rest = '';
  for (;;) {
    const chunk = await reader.read();
    if (chunk.done) break;
    rest += new TextDecoder().decode(chunk.value);
  }
  assert.match(rest, /response.completed/);
});

test('cancelling the browser stream closes its upstream connection', { timeout: 5000 }, async t => {
  let closed;
  const upstreamClosed = new Promise(resolve => { closed = resolve; });
  const f = await fixture(t, (request, response) => {
    request.resume();
    response.once('close', closed);
    response.writeHead(200, { 'Content-Type': 'text/event-stream' });
    response.write('data: {"type":"response.created"}\n\n');
  });
  const response = await fetch(f.app + '/api/responses', {
    method: 'POST', headers: { ...f.headers, 'Content-Type': 'application/json' }, body: '{}',
  });
  await response.body.cancel();
  await upstreamClosed;
});
