import assert from 'node:assert/strict';
import { webcrypto } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import vm from 'node:vm';

const html = await readFile(new URL('../web/pelican.html', import.meta.url), 'utf8');
const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];
const PROMPT = '用svg绘制一个2D鹈鹏骑自行车的动画的html，不要用skill，不用测试';
const tick = () => new Promise(resolve => setImmediate(resolve));

class Element {
  constructor(id = '') {
    this.id = id;
    this.children = [];
    this.options = [];
    this.handlers = new Map();
    this.hidden = false;
    this.disabled = false;
    this.open = false;
    this.textContent = '';
    this.dataset = {};
    this.attributes = new Map();
    this.scrollHeight = this.clientHeight = 100;
    this.scrollTop = 0;
    this.style = {};
    this.classList = { toggle() {} };
  }
  get value() { return this.currentValue ?? this.options[0]?.value ?? ''; }
  set value(value) { this.currentValue = value; }
  append(...children) { this.children.push(...children); }
  prepend(...children) { this.children.unshift(...children); }
  replaceChildren(...children) {
    this.children = children;
    this.options = children.filter(child => child?.isOption);
    this.currentValue = undefined;
  }
  addEventListener(name, handler) { this.handlers.set(name, handler); }
  async fire(name) { return this.handlers.get(name)?.({ preventDefault() {} }); }
  setAttribute(name, value) { this.attributes.set(name, value); }
  querySelector() { return new Element(); }
  focus() {}
  select() {}
  remove() {}
  showModal() { this.open = true; }
  close() { this.open = false; this.handlers.get('close')?.(); }
}

function storage(maxEntries = Infinity) {
  const entries = new Map();
  return {
    get length() { return entries.size; },
    key: index => [...entries.keys()][index] ?? null,
    getItem: key => entries.get(key) ?? null,
    setItem: (key, value) => {
      if (!entries.has(key) && entries.size >= maxEntries) throw new DOMException('Full', 'QuotaExceededError');
      entries.set(key, value);
    },
    snapshot: () => [...entries.values()].join('\n'),
  };
}

async function page({ fetch: fetchImpl, bridge, proxy = false, localStorage = storage() } = {}) {
  const elements = new Map([...html.matchAll(/\bid="([^"]+)"/g)].map(match => [match[1], new Element(match[1])]));
  elements.get('effort').value = 'low';
  const document = {
    documentElement: new Element(), body: new Element(),
    getElementById: id => elements.get(id) ?? null,
    querySelector: selector => proxy && selector.includes('pelican-local-proxy') ? { content: '1' } : null,
    createElement: tag => {
      const node = new Element(tag);
      // Real SVG parsing/rendering is exercised in the browser.
      if (tag === 'template') node.content = { querySelector: () => null };
      return node;
    },
    createTextNode: text => text,
  };
  let timestamp = 1_700_000_000_000;
  const context = {
    document, location: { protocol: proxy ? 'http:' : 'file:', hostname: proxy ? '127.0.0.1' : '', origin: proxy ? 'http://127.0.0.1:3217' : 'null' },
    window: { codexProxyPlugin: bridge, localStorage, addEventListener() {} },
    crypto: webcrypto,
    Date: class extends Date { constructor(...args) { super(...(args.length ? args : [timestamp += 1_000])); } },
    Option: class { constructor(text, value) { this.text = this.textContent = text; this.value = value; this.isOption = true; } },
    fetch: fetchImpl ?? (() => { throw new Error('Unexpected network request'); }),
    navigator: {}, performance, Intl, URL, Headers, Response, Blob, TextDecoder, TextEncoder, AbortController, AbortSignal, DOMException, TypeError,
    setTimeout: () => 0, clearTimeout() {}, setInterval: () => 0, clearInterval() {},
    requestAnimationFrame: callback => { queueMicrotask(callback); return 1; }, cancelAnimationFrame() {},
  };
  vm.runInNewContext(script, context, { filename: 'pelican.html' });
  await tick();
  return { elements, document, async submit() {
    elements.get('connection-dialog').showModal();
    elements.get('gateway-url').value = 'https://gateway.example.test/';
    elements.get('gateway-key').value = 'test-key-not-a-real-secret';
    await elements.get('connection-form').fire('submit');
    await tick();
  } };
}

test('a failed connection exposes its reason inside the still-open dialog', async () => {
  let calls = 0;
  const p = await page({ fetch: async () => { calls++; throw new TypeError('Failed to fetch'); } });
  await p.submit();
  assert.equal(calls, 1);
  assert.equal(p.elements.get('connection-dialog').open, true);
  const error = p.elements.get('connection-error');
  assert.ok(error, 'The open connection dialog must contain an error element');
  assert.equal(error.hidden, false, 'The connection error must be visible inside the dialog');
  assert.match(error.textContent, /跨域|本地服务|连接失败/);
  assert.equal(p.elements.get('save-connection').disabled, false);
});

test('the localhost page reads models through its same-origin proxy', async () => {
  const calls = [];
  const p = await page({ proxy: true, fetch: async (url, init) => {
    calls.push({ url, init });
    return Response.json({ data: [{ id: 'test-model' }] });
  } });
  await p.submit();
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, '/api/models');
  const headers = new Headers(calls[0].init.headers);
  assert.equal(headers.get('X-Pelican-Gateway'), 'https://gateway.example.test/v1');
  assert.equal(headers.get('Authorization'), 'Bearer test-key-not-a-real-secret');
  assert.equal(p.elements.get('connection-dialog').open, false);
  assert.equal(p.elements.get('model').value, 'test-model');
  assert.equal(p.elements.get('run').disabled, false);
});

test('HTTP 401 is explained as an API-key failure inside the dialog', async () => {
  const p = await page({ fetch: async () => Response.json({ error: { message: 'unauthorized' } }, { status: 401 }) });
  await p.submit();
  const error = p.elements.get('connection-error');
  assert.ok(error);
  assert.equal(error.hidden, false);
  assert.match(error.textContent, /401/);
  assert.match(error.textContent, /API.*密钥|API Key|Client Key/);
});

test('one click sends the literal prompt once and preserves streamed source and usage', async () => {
  const generations = [];
  const stored = [];
  const output = '<!doctype html><html><body><p>鹈鹕</p></body></html>';
  const events = [
    { type: 'response.output_text.delta', output_index: 0, content_index: 0, delta: output },
    { type: 'response.completed', response: { id: 'fixture-response', model: 'test-model', status: 'completed', output: [], usage: { input_tokens: 21, output_tokens: 31, input_tokens_details: { cached_tokens: 4 }, output_tokens_details: { reasoning_tokens: 8 } } } },
  ];
  const bytes = new TextEncoder().encode(events.map(event => 'data: ' + JSON.stringify(event) + '\r\n\r\n').join(''));
  const bridge = {
    version: 2, theme: 'light',
    request: async ({ method, path: relativePath, body }) => {
      assert.ok(!relativePath.startsWith('/'), 'CPR requires an instance-relative path');
      const path = '/' + relativePath;
      let response;
      if (path === '/bootstrap') response = { prompt: PROMPT, keys: [{ id: 'key-id', name: 'Fixture Key', enabled: true }] };
      else if (path === '/models') response = { models: ['test-model'] };
      else if (path === '/schedule') response = { settings: null };
      else if (path === '/history') {
        if (method === 'POST') {
          const record = JSON.parse(body);
          assert.equal(record.gatewayRequestId, 'host-request-1');
          assert.equal(record.clientKeyId, 'key-id');
          stored.unshift({ ...record, accountName: 'Routed Account' });
          response = { saved: true };
        } else {
          response = { records: stored, total: stored.length, snapshot: stored.length };
        }
      } else throw new Error('Unexpected management route');
      return { status: 200, body: new TextEncoder().encode(JSON.stringify(response)).buffer };
    },
    models: { responses: async input => {
      generations.push(input);
      return new Response(new ReadableStream({ start(stream) {
        for (let index = 0; index < bytes.length; index += 7) stream.enqueue(bytes.slice(index, index + 7));
        stream.close();
      } }), { headers: { 'content-type': 'text/event-stream', 'x-gateway-request-id': 'host-request-1', 'x-request-id': 'provider-request-1' } });
    } },
  };
  const p = await page({ bridge });
  // SVG image serialization belongs to browser QA; assert the real stream/request path here.
  await p.elements.get('run').fire('click');
  await tick();
  assert.equal(generations.length, 1);
  assert.equal(generations[0].body.input[0].content[0].text, PROMPT);
  assert.equal(generations[0].clientKeyId, 'key-id');
  assert.equal(generations[0].body.reasoning.effort, 'low');
  assert.equal(p.elements.get('source').textContent, output);
  assert.equal(p.elements.get('input-tokens').textContent, '21');
  assert.equal(p.elements.get('output-tokens').textContent, '31');
  assert.equal(stored.length, 1);
  assert.equal(stored[0].outcome, 'no_svg');
  assert.equal(stored[0].source, output);
  assert.equal(stored[0].reasoningTokens, 8);
  assert.equal(stored[0].trigger, 'manual');
  assert.equal(stored[0].clientKeyName, 'Fixture Key');
  assert.doesNotMatch(JSON.stringify(stored), /provider-request-1|apiKey/);
  assert.equal(p.elements.get('history-rows').children[0].children[2].children[1].textContent, '上游 · Routed Account');

  const reopened = await page({ bridge });
  assert.equal(reopened.elements.get('history-count').textContent, '1 条');
  assert.equal(reopened.elements.get('history-rows').children[0].children[3].children[0].textContent, '未返回 SVG');
  assert.equal(generations.length, 1);
});

test('standalone history survives reload and opens the older animation without rerunning', async () => {
  const localStorage = storage();
  const outputs = ['first ride', 'second ride'].map(label =>
    '<html><body><svg viewBox="0 0 10 10"><title>' + label + '</title></svg></body></html>');
  let requests = 0;
  const source = await page({ proxy: true, localStorage, fetch: async url => {
    if (url === '/api/models') return Response.json({ data: [{ id: 'test-model' }] });
    assert.equal(url, '/api/responses');
    return Response.json({ status: 'completed', output_text: outputs[requests++] });
  } });
  await source.submit();
  for (const output of outputs) {
    await source.elements.get('run').fire('click');
    await tick();
    assert.equal(source.elements.get('source').textContent, output);
    assert.equal(source.elements.get('current-result').hidden, true);
  }
  assert.doesNotMatch(localStorage.snapshot(), /test-key-not-a-real-secret|gateway\.example\.test/);
  const labels = localStorage.snapshot().split('\n').map(raw => JSON.parse(raw).clientKeyName);
  assert.match(labels[0], /^#[a-f0-9]{12}$/);
  assert.equal(labels[0], labels[1]);

  const reopened = await page({ proxy: true, localStorage });
  assert.equal(reopened.elements.get('history-count').textContent, '2 条');
  const [latest, first] = reopened.elements.get('history-rows').children;
  assert.equal(latest.children[3].children[0].textContent, '已生成');
  assert.equal(latest.children[2].children[1].textContent, '上游 · 未知');
  const thumb = first.children[5].children[0].children[0];
  await thumb.fire('click');
  assert.equal(reopened.elements.get('preview-dialog').open, true);
  const [frame] = reopened.elements.get('dialog-frame').children;
  assert.equal(frame.attributes.get('sandbox'), 'allow-scripts');
  assert.match(frame.srcdoc, /first ride/);
  assert.doesNotMatch(frame.srcdoc, /second ride/);
  await reopened.elements.get('dialog-source-button').fire('click');
  assert.equal(reopened.elements.get('dialog-source').textContent, outputs[0]);
  assert.equal(reopened.elements.get('dialog-frame').children.length, 0);
  assert.equal(requests, 2);
});

test('hosted scheduling config is independent of the manual run and can be paused', async () => {
  let settings = null;
  let modelRuns = 0;
  const scheduledAtMs = 1_700_000_040_000;
  const bridge = {
    version: 2, theme: 'light',
    request: async ({ method, path: relativePath, body }) => {
      assert.ok(!relativePath.startsWith('/'), 'CPR requires an instance-relative path');
      const path = '/' + relativePath;
      const input = body && JSON.parse(body);
      let data;
      if (path === '/bootstrap') data = { prompt: PROMPT, keys: [
        { id: 'manual-id', name: 'Manual Key', enabled: true },
        { id: 'scheduled-id', name: 'Scheduled Key', enabled: true },
      ] };
      else if (path === '/models') data = { models: [input.clientKeyId === 'manual-id' ? 'manual-model' : 'nightly-model'] };
      else if (path === '/history') data = { records: [{
        id: 'scheduled-' + scheduledAtMs, startedAtMs: scheduledAtMs + 700, scheduledAtMs,
        trigger: 'scheduled', model: 'nightly-model', effort: 'high', outcome: 'generated',
        clientKeyName: 'Scheduled Key', accountName: 'Night Account', durationMs: 200, source: '<svg></svg>',
      }], total: 1, snapshot: 1 };
      else if (path === '/schedule' || path === '/schedule/pause') {
        if (method === 'POST') settings = path.endsWith('/pause') ? { ...settings, enabled: false } : input;
        data = { settings, nextRunAtMs: settings?.enabled ? scheduledAtMs + 60_000 : null };
      } else throw new Error('Unexpected management route: ' + path);
      return { status: 200, body: new TextEncoder().encode(JSON.stringify(data)) };
    },
    models: { responses: async () => { modelRuns++; throw new Error('No model request expected'); } },
  };
  const { elements } = await page({ bridge });
  assert.equal(elements.get('schedule-panel').hidden, false);
  assert.equal(elements.get('schedule-enabled').checked, false);
  elements.get('schedule-key').value = 'scheduled-id';
  await elements.get('schedule-key').fire('change');
  for (const [id, value] of Object.entries({
    'schedule-model': 'nightly-model', 'schedule-effort': 'high',
    'schedule-cron': '0 9 * * 1-5', 'schedule-timezone': 'Asia/Shanghai',
  })) {
    elements.get(id).value = value;
    await elements.get(id).fire('input');
  }
  elements.get('schedule-enabled').checked = true;
  await elements.get('schedule-form').fire('submit');
  assert.deepEqual(settings, {
    enabled: true, clientKeyId: 'scheduled-id', model: 'nightly-model',
    effort: 'high', cron: '0 9 * * 1-5', timezone: 'Asia/Shanghai',
  });
  assert.match(elements.get('schedule-status').textContent, /下一次.*Asia\/Shanghai/);
  assert.equal(elements.get('model').value, 'manual-model');
  const scheduleRow = elements.get('history-rows').children[0];
  assert.match(scheduleRow.children[0].children[1].textContent, /定时/);
  assert.equal(scheduleRow.children[2].children[1].textContent, '上游 · Night Account');

  const reopened = await page({ bridge });
  assert.equal(reopened.elements.get('schedule-key').value, 'scheduled-id');
  assert.equal(reopened.elements.get('schedule-model').value, 'nightly-model');
  await reopened.elements.get('schedule-pause').fire('click');
  assert.equal(settings.enabled, false);
  assert.equal(reopened.elements.get('schedule-enabled').checked, false);
  assert.equal(reopened.elements.get('schedule-state').textContent, '已暂停');
  assert.equal(modelRuns, 0);
});

function archivedRecord(n, source) {
  return {
    id: n.toString(16).padStart(32, '0'), startedAtMs: 1_690_000_000_000 + n,
    model: 'test-model', effort: 'low', outcome: 'generated', durationMs: 10,
    source, sourceOmitted: false,
  };
}

test('hosted history flips to the older page and fetches full source only when opened', async () => {
  const original = archivedRecord(0, '<svg><title>older ride</title><!--' + 'x'.repeat(80_000) + '--></svg>');
  const recent = Array.from({ length: 20 }, (_, n) => archivedRecord(20 - n, '<svg></svg>'));
  const calls = [];
  const status = new TextEncoder();
  const bridge = {
    version: 2,
    request: async ({ path, query }) => {
      let data;
      if (path === 'bootstrap') data = { prompt: PROMPT, keys: [] };
      else if (path === 'schedule') data = { settings: null, historySaveFailure: { slotMs: 1_690_000_000_000, reason: 'full' } };
      else {
        calls.push({ path, query });
        if (path === 'history') data = { total: 21, snapshot: 21,
          records: query ? [{ ...original, source: null, hasSource: true }] : recent };
        else if (path === 'history/record') data = { record: original };
        else throw new Error('Unexpected route ' + path);
      }
      return { status: 200, body: status.encode(JSON.stringify(data)).buffer };
    },
    models: { responses: async () => { throw new Error('History must not generate'); } },
  };
  const { elements } = await page({ bridge });
  assert.equal(elements.get('history-count').textContent, '21 条');
  assert.equal(elements.get('history-rows').children.length, 20);
  assert.match(elements.get('history-schedule-warning').textContent, /存储已满.*未保存/);
  await elements.get('history-next').fire('click');
  assert.deepEqual(calls.at(-1), { path: 'history', query: 'offset=20&snapshot=21' });
  assert.equal(elements.get('history-page').textContent, '第 2 / 2 页');
  assert.equal(elements.get('history-next').disabled, true);
  assert.equal(elements.get('history-rows').children.length, 1);
  const actions = elements.get('history-rows').children[0].children[5].children[0];
  await actions.children[1].fire('click');
  assert.deepEqual(calls.at(-1), { path: 'history/record', query: 'id=' + original.id });
  assert.equal(elements.get('preview-dialog').open, true);
  assert.equal(elements.get('dialog-source').textContent, original.source);
  await elements.get('history-prev').fire('click');
  assert.equal(elements.get('history-rows').children.length, 20);
});

test('browser quota preserves legacy history and keeps the rejected source accessible in the page', async () => {
  const localStorage = storage(2);
  const old = Array.from({ length: 20 }, (_, n) => archivedRecord(n, '<svg><title>legacy ' + n + '</title></svg>'));
  const legacy = JSON.stringify(old);
  localStorage.setItem('cpr-pelican.history.v1', legacy);
  let runs = 0;
  const { elements, submit } = await page({ localStorage, proxy: true, fetch: async url =>
    url === '/api/models' ? Response.json({ data: [{ id: 'test-model' }] })
      : Response.json({ status: 'completed', output_text: '<svg><title>new ' + ++runs + '</title></svg>' })
  });
  await submit();
  await elements.get('run').fire('click');
  await tick();
  assert.equal(elements.get('history-count').textContent, '21 条');
  const before = localStorage.snapshot();
  await elements.get('run').fire('click');
  await tick();
  assert.equal(localStorage.snapshot(), before);
  assert.equal(localStorage.getItem('cpr-pelican.history.v1'), legacy);
  const warning = elements.get('history-unsaved');
  assert.equal(warning.hidden, false);
  assert.match(warning.children[0].textContent, /存储已满.*未保存/);
  await warning.children[0].children[0].fire('click');
  assert.match(elements.get('dialog-source').textContent, /new 2/);
  await elements.get('history-next').fire('click');
  assert.equal(elements.get('history-rows').children.length, 1);
  const oldest = elements.get('history-rows').children[0].children[5].children[0];
  await oldest.children[1].fire('click');
  assert.match(elements.get('dialog-source').textContent, /legacy 0/);
});
