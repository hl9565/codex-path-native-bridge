import http from 'node:http';
import https from 'node:https';
import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const pageFile = new URL('../web/pelican.html', import.meta.url);
const MAX_BODY = 8 * 1024 * 1024;

function json(response, status, code, message) {
  if (response.destroyed) return;
  if (response.headersSent) { response.destroy(); return; }
  response.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
    'X-Content-Type-Options': 'nosniff',
  });
  response.end(JSON.stringify({ error: { code, message } }));
}

function gatewayUrl(value, suffix) {
  if (typeof value !== 'string' || value.length > 2048) throw new Error('invalid_gateway');
  const base = new URL(value);
  if (!['http:', 'https:'].includes(base.protocol) || base.username || base.password || base.search || base.hash)
    throw new Error('invalid_gateway');
  return new URL(base.href.replace(/\/+$/, '') + suffix);
}

async function proxy(request, response, suffix) {
  const authorization = request.headers.authorization;
  if (typeof authorization !== 'string' || authorization.length > 16384 || !/^Bearer \S+$/i.test(authorization)) {
    json(response, 401, 'missing_api_key', 'An API Client Key is required.');
    return;
  }
  let target;
  try { target = gatewayUrl(request.headers['x-pelican-gateway'], suffix); }
  catch { json(response, 400, 'invalid_gateway', 'Provide a valid gateway API base URL.'); return; }

  const chunks = [];
  let bytes = 0;
  try {
    for await (const chunk of request) {
      bytes += chunk.length;
      if (bytes > MAX_BODY) {
        json(response, 413, 'body_too_large', 'Request body exceeds 8 MiB.');
        return;
      }
      chunks.push(chunk);
    }
  } catch { json(response, 400, 'incomplete_request', 'The request was interrupted.'); return; }
  const body = Buffer.concat(chunks);
  if (request.method === 'POST') {
    if ((request.headers['content-type'] || '').split(';')[0].trim().toLowerCase() !== 'application/json') {
      json(response, 415, 'invalid_content_type', 'Use application/json.');
      return;
    }
    try { JSON.parse(body.toString('utf8')); }
    catch { json(response, 400, 'invalid_json', 'Request body must be valid JSON.'); return; }
  }

  const headers = { Authorization: authorization, Accept: request.headers.accept || 'application/json', 'Accept-Encoding': 'identity' };
  if (request.method === 'POST') headers['Content-Type'] = 'application/json';
  const transport = target.protocol === 'https:' ? https : http;
  let timedOut = false;
  const upstream = transport.request(target, { method: request.method, headers }, incoming => {
    if (incoming.statusCode >= 300 && incoming.statusCode < 400) {
      incoming.destroy();
      json(response, 502, 'gateway_redirect', 'The gateway redirected. Use its final API URL.');
      return;
    }
    const forwarded = { 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff' };
    for (const name of ['content-type', 'content-encoding', 'x-request-id', 'openai-request-id']) {
      if (incoming.headers[name]) forwarded[name] = incoming.headers[name];
    }
    response.writeHead(incoming.statusCode || 502, forwarded);
    response.flushHeaders();
    incoming.once('error', () => response.destroy());
    incoming.pipe(response);
  });
  upstream.setTimeout(suffix === '/models' ? 30_000 : 600_000, () => {
    timedOut = true;
    upstream.destroy();
  });
  upstream.once('error', () => json(response, timedOut ? 504 : 502,
    timedOut ? 'gateway_timeout' : 'gateway_unreachable',
    timedOut ? 'The gateway timed out.' : 'Cannot reach the gateway. Check its URL, network, and TLS certificate.'));
  request.once('aborted', () => upstream.destroy());
  response.once('close', () => { if (!response.writableEnded) upstream.destroy(); });
  if (response.destroyed) upstream.destroy();
  else upstream.end(body);
}

export function createPelicanServer() {
  const server = http.createServer(async (request, response) => {
    const address = server.address();
    const host = `127.0.0.1:${address.port}`;
    const origin = `http://${host}`;
    // No wildcard CORS, cookies, public bind, arbitrary file serving, or credential logging.
    if (request.headers.host !== host || (request.headers.origin && request.headers.origin !== origin)
        || request.headers['sec-fetch-site'] === 'cross-site') {
      json(response, 403, 'origin_rejected', 'Use the loopback page served by this process.');
      return;
    }
    const path = request.url;
    try {
      if (request.method === 'GET' && (path === '/' || path === '/pelican.html')) {
        const html = (await readFile(pageFile, 'utf8')).replace('<head>', '<head>\n  <meta name="pelican-local-proxy" content="1">');
        response.writeHead(200, {
          'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store',
          'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'no-referrer',
          'Content-Security-Policy': "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; frame-src 'self' data: blob:; connect-src 'self'; object-src 'none'; base-uri 'none'; form-action 'self'",
        });
        response.end(html);
      } else if ((path === '/api/models' && request.method === 'GET') || (path === '/api/responses' && request.method === 'POST')) {
        await proxy(request, response, path.slice('/api'.length));
      } else if (path === '/favicon.ico' && request.method === 'GET') {
        response.writeHead(204); response.end();
      } else {
        json(response, 404, 'not_found', 'Unknown playground route.');
      }
    } catch { json(response, 500, 'local_server_error', 'The local playground could not complete this request.'); }
  });
  server.requestTimeout = 600_000;
  return server;
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const portIndex = process.argv.indexOf('--port');
  const port = portIndex < 0 ? 3217 : Number(process.argv[portIndex + 1]);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    console.error('Choose a valid port with --port <1-65535>.');
    process.exitCode = 1;
  } else {
    const server = createPelicanServer();
    server.once('error', error => {
      console.error(error.code === 'EADDRINUSE' ? `Port ${port} is in use. Choose another --port.` : 'Cannot start the loopback playground.');
      process.exitCode = 1;
    });
    server.listen(port, '127.0.0.1', () => console.log(`Pelican playground: http://127.0.0.1:${port}/`));
    for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => server.close());
  }
}
