#!/usr/bin/env node
// Explicit opt-in native smoke. No daemon lifecycle, auth writes, turns, or
// business tools. Two real tunnel processes use an in-memory MCP fixture.
import assert from 'node:assert/strict';
import { spawn, execFileSync } from 'node:child_process';
import { once } from 'node:events';
import fs from 'node:fs/promises';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

const [binary, socket] = process.argv.slice(2);
assert(binary && socket && path.isAbsolute(binary) && path.isAbsolute(socket), 'usage: node smoke_shared_codex_daemon.mjs /absolute/tunnel-client /absolute/existing/socket');
const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'shared-codex-smoke-'));
const children = [];
const processRows = () => execFileSync('/bin/ps', ['-axo', 'pid=,ppid=,args='], { encoding: 'utf8' }).trim().split('\n').map(line => {
  const [, pid, ppid, args] = line.match(/^\s*(\d+)\s+(\d+)\s+(.*)$/);
  return { pid: Number(pid), ppid: Number(ppid), args };
});
const before = processRows();
const server = http.createServer(async (req, res) => {
  if (req.method !== 'POST' || req.url !== '/mcp') { res.writeHead(404).end(); return; }
  let body = '';
  for await (const chunk of req) body += chunk;
  let rpc;
  try { rpc = JSON.parse(body); } catch { res.writeHead(400).end(); return; }
  if (rpc.id === undefined) { res.writeHead(202).end(); return; }
  let result;
  if (rpc.method === 'initialize') result = { protocolVersion: rpc.params.protocolVersion, capabilities: { tools: {} }, serverInfo: { name: 'read-only-daemon-smoke', version: '1' } };
  else if (rpc.method === 'tools/list') result = { tools: [] };
  else if (rpc.method === 'ping') result = {};
  else { res.writeHead(400).end(); return; }
  res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({ jsonrpc: '2.0', id: rpc.id, result }));
});
server.listen(0, '127.0.0.1');
await once(server, 'listening');
const target = `http://127.0.0.1:${server.address().port}/mcp`;
async function readJSON(url, opts) {
  const r = await fetch(url, { ...opts, signal: AbortSignal.timeout(20000) });
  if (r.status !== 200) throw new Error(`HTTP ${r.status} at ${new URL(url).pathname}: ${await r.text()}`);
  return r.json();
}
async function waitFor(fn, limit = 30000) {
  const deadline = Date.now() + limit;
  let error;
  while (Date.now() < deadline) {
    try { const result = await fn(); if (result) return result; } catch (err) { error = err; }
    await delay(100);
  }
  throw error || new Error('bounded readiness timeout');
}
async function stop(child) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  child.kill('SIGTERM');
  const exited = once(child, 'exit');
  await Promise.race([exited, delay(5000)]);
  if (child.exitCode === null && child.signalCode === null) { child.kill('SIGKILL'); await exited; }
}
async function mcpReadOnly(url) {
  let session;
  async function rpc(method, id, params) {
    const r = await fetch(url, { method: 'POST', headers: { 'content-type': 'application/json', accept: 'application/json, text/event-stream', 'mcp-protocol-version': '2025-06-18', ...(session ? { 'mcp-session-id': session } : {}) }, body: JSON.stringify({ jsonrpc: '2.0', method, ...(id === undefined ? {} : { id }), ...(params ? { params } : {}) }), signal: AbortSignal.timeout(10000) });
    assert(r.ok, `MCP HTTP ${r.status}`);
    session ||= r.headers.get('mcp-session-id');
    if (id === undefined) return;
    const text = await r.text();
    const message = JSON.parse(text.startsWith('event:') || text.startsWith('data:') ? text.split('\n').find(l => l.startsWith('data:')).slice(5).trim() : text);
    assert(!message.error, 'MCP protocol error');
    return message.result;
  }
  const init = await rpc('initialize', 1, { protocolVersion: '2025-06-18', capabilities: {}, clientInfo: { name: 'shared-daemon-smoke', version: '1' } });
  await rpc('notifications/initialized');
  const tools = await rpc('tools/list', 2, {});
  assert.equal(init.serverInfo.name, 'read-only-daemon-smoke');
  assert.equal(tools.tools.length, 0);
  return { initialized: true, toolsList: true, businessCalls: 0 };
}
try {
  for (let i = 0; i < 2; i++) {
    const healthFile = path.join(dir, `health-${i}`), urlFile = path.join(dir, `proxy-${i}.json`);
    const child = spawn(binary, ['dev', 'proxy', '--backend', 'go', '--mcp-server-url', target, '--health-url-file', healthFile, '--url-file', urlFile, '--print-json'], { env: { ...process.env, TUNNEL_CLIENT_CODEX_APP_SERVER_MODE: 'daemon', TUNNEL_CLIENT_CODEX_APP_SERVER_SOCKET: socket, TUNNEL_CLIENT_CODEX_APP_SERVER_CWD: dir }, stdio: ['ignore', 'pipe', 'pipe'] });
    child.tail = '';
    for (const stream of [child.stdout, child.stderr]) stream.on('data', data => { child.tail = (child.tail + data).slice(-2000); });
    children.push(child);
    child.health = await waitFor(async () => { if (child.exitCode !== null) throw new Error(child.tail); return (await fs.readFile(healthFile, 'utf8')).trim(); });
    child.info = await waitFor(async () => JSON.parse(await fs.readFile(urlFile, 'utf8')));
  }
  const snapshots = await Promise.all(children.map(child => waitFor(async () => {
    const s = await readJSON(child.health + '/api/codex/status');
    return s.ready ? s : null;
  })));
  assert(snapshots[0].daemon_pid > 0);
  assert.equal(snapshots[0].daemon_pid, snapshots[1].daemon_pid);
  for (const s of snapshots) { assert.equal(s.mode, 'daemon'); assert.equal(s.pid ?? 0, 0); assert.equal(s.command, ''); }
  const checks = await Promise.all(children.map(async child => {
    for (const endpoint of ['/healthz', '/readyz']) {
      const r = await fetch(child.health + endpoint, { signal: AbortSignal.timeout(5000) });
      assert.equal(r.status, 200);
    }
    return mcpReadOnly(child.info.mcp_url);
  }));
  const threads = await Promise.all(children.map(child => readJSON(child.health + '/api/codex/thread/start', { method: 'POST', headers: { 'content-type': 'application/json', origin: child.health }, body: JSON.stringify({ cwd: dir, model: 'gpt-6-sol', sandbox_type: 'read-only', approval_policy: 'never', inject_context: false }) })));
  assert(threads[0].thread_id && threads[0].thread_id !== threads[1].thread_id);
  for (let i = 0; i < 2; i++) {
    const s = await readJSON(children[i].health + '/api/codex/status');
    assert.equal(s.threads.length, 1);
    assert.equal(s.threads[0].id, threads[i].thread_id);
    const events = await readJSON(children[i].health + '/api/codex/events');
    assert(events.events.every(e => !e.thread_id || e.thread_id === threads[i].thread_id));
  }
  const after = processRows();
  for (const child of children) {
    const descendants = new Set([child.pid]);
    for (let size = -1; size !== descendants.size;) {
      size = descendants.size;
      for (const p of after) if (descendants.has(p.ppid)) descendants.add(p.pid);
    }
    assert(!after.some(p => descendants.has(p.pid) && /\bcodex\b.*\bapp-server\b/.test(p.args)), 'tunnel spawned an app-server');
  }
  assert(before.some(p => p.pid === snapshots[0].daemon_pid), 'daemon was not already running');
  await stop(children[0]);
  assert.equal((await readJSON(children[1].health + '/api/codex/status')).daemon_pid, snapshots[0].daemon_pid);
  await mcpReadOnly(children[1].info.mcp_url);
  assert(processRows().some(p => p.pid === snapshots[0].daemon_pid), 'stopping a tunnel stopped the daemon');
  console.log(JSON.stringify({ ok: true, tunnelPIDs: children.map(c => c.pid), daemonPID: snapshots[0].daemon_pid, socket, healthz: [200, 200], readyz: [200, 200], mcp: checks, independentThreads: true, foreignEvents: 0, appServerChildren: 0, survivingTunnelAfterPeerStop: true, daemonRestarted: false, turnsStarted: 0, authWrites: 0 }, null, 2));
} finally {
  await Promise.all(children.map(stop));
  await new Promise(resolve => server.close(resolve));
  await fs.rm(dir, { recursive: true });
}
