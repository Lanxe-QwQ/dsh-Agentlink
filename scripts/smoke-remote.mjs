import { createServer } from 'node:http';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdir, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { isAbsolute, join } from 'node:path';
import { RemoteDshClient } from '../dist/remote-client.js';
import { BridgeService } from '../dist/bridge-service.js';
import { DshConnectionManager } from '../dist/connection-manager.js';
import { EventLedger } from '../dist/event-ledger.js';
import { TaskStore } from '../dist/task-store.js';
const root = process.env.DSH_SMOKE_ROOT;
if (!root || !isAbsolute(root)) throw new Error('Set DSH_SMOKE_ROOT to an absolute directory for isolated test data');
await mkdir(root, { recursive: true });
const home = await mkdtemp(join(root, 'agentlink-remote-smoke-'));
let calls = 0;
const provider = createServer(async (request, response) => {
  for await (const _ of request) {}
  calls++;
  if (calls === 3) { request.on('close', () => response.end()); return; }
  response.writeHead(200, { 'content-type': 'text/event-stream' });
  const frames = [
    ['message_start', { type: 'message_start', message: { id: 'msg_local', type: 'message', role: 'assistant', model: 'deepseek-v4-pro', content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 10, output_tokens: 0 } } }],
    ['content_block_start', { type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } }],
    ['content_block_delta', { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: 'local-adapter-pong' } }],
    ['content_block_stop', { type: 'content_block_stop', index: 0 }],
    ['message_delta', { type: 'message_delta', delta: { stop_reason: 'end_turn', stop_sequence: null }, usage: { output_tokens: 3 } }],
    ['message_stop', { type: 'message_stop' }],
  ];
  response.end(frames.map(([event, data]) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`).join(''));
});
provider.listen(0, '127.0.0.1'); await once(provider, 'listening');
const providerUrl = `http://127.0.0.1:${provider.address().port}`;
const exe = process.env.DSH_SMOKE_COMMAND;
const commandArgs = JSON.parse(process.env.DSH_SMOKE_ARGS ?? '[]');
if (!exe || !Array.isArray(commandArgs) || !commandArgs.every(arg => typeof arg === 'string')) throw new Error('Set DSH_SMOKE_COMMAND and optional JSON DSH_SMOKE_ARGS for your installed DSH CLI');
const connectionFile = `${home}/connection.json`;
const profile = `${home}/profiles/web`;
await mkdir(profile, { recursive: true });
await writeFile(`${profile}/package.json`, JSON.stringify({ name: 'agentlink-smoke-profile', private: true, dependencies: {}, dsh: { profile: { bundles: ['@deepseek-ai/dsh-base', '@deepseek-ai/dsh-web-app'] } } }));
await writeFile(`${profile}/cordis.yml`, '[]\n');
await writeFile(`${profile}/pnpm-workspace.yaml`, 'packages:\n  - .\n');
await writeFile(`${profile}/cordis.patch.yml`, `- insert:\n    - id: agentlink-desktop-connection\n      name: ${JSON.stringify(fileURLToPath(new URL('../dist/desktop-connection.js', import.meta.url)))}\n      config:\n        connectionFile: ${JSON.stringify(connectionFile)}\n`);
const child = spawn(exe, [...commandArgs, 'web', '--no-open', '--host', '127.0.0.1', '--port', '0'], { windowsHide: true, cwd: home, env: { ...process.env, ...(process.env.DSH_SMOKE_ELECTRON === '1' ? { ELECTRON_RUN_AS_NODE: '1' } : {}), DSH_HOME: home, DEEPSEEK_API_KEY: 'local-stub-only', DEEPSEEK_BASE_URL: providerUrl }, stdio: ['ignore', 'pipe', 'pipe'] });
let output = '', stderr = '', manager, startupError;
child.on('error', error => { startupError = error; });
child.stdout.on('data', data => { output += data; }); child.stderr.on('data', data => { stderr += data; });
const report = { realDshVersion: process.env.DSH_SMOKE_VERSION ?? 'operator-supplied runtime', provider: 'local deterministic stub', externalModelCalls: 0 };
try {
  const deadline = Date.now() + 15000;
  while (!output.match(/dsh web:\s+(https?:\/\/\S+)/) && Date.now() < deadline && child.exitCode === null && !startupError) await new Promise(resolve => setTimeout(resolve, 50));
  if (startupError) throw new Error('Unable to start DSH_SMOKE_COMMAND; verify the launcher path');
  const url = output.match(/dsh web:\s+(https?:\/\/\S+)/)?.[1];
  if (!url) throw new Error('Isolated DSH did not become ready');
  let connection;
  const connectionDeadline = Date.now() + 5000;
  while (!connection && Date.now() < connectionDeadline) {
    try { connection = JSON.parse(await readFile(connectionFile, 'utf8')); } catch { await new Promise(resolve => setTimeout(resolve, 50)); }
  }
  if (!connection) throw new Error('Desktop connection publisher did not create its record');
  report.desktopConnectionPublished = true;
  const api = new RemoteDshClient(connection, 10000, process.env.DSH_SMOKE_VERSION ?? 'operator-supplied runtime');
  const config = { hostUrl: connection.origin, homeDir: `${home}/bridge`, requestTimeoutMs: 10000, allowRemoteHost: false, declaredDshVersion: process.env.DSH_SMOKE_VERSION ?? 'operator-supplied runtime' };
  const tasks = new TaskStore(config.homeDir), ledger = new EventLedger(config.homeDir);
  manager = new DshConnectionManager(config, api, tasks, ledger);
  const service = new BridgeService(config, api, tasks, manager, ledger);
  const workspace = `${home}/workspace`; await mkdir(workspace, { recursive: true });
  manager.start();
  const delegation = await service.delegate({ cwd: workspace, prompt: 'Reply with local-adapter-pong. Do not use tools.', waitSeconds: 0 });
  report.accepted = delegation.accepted; report.detached = delegation.detached;
  const timeout = Date.now() + 20000;
  let snapshot;
  do { await new Promise(resolve => setTimeout(resolve, 100)); snapshot = await service.status(delegation.taskId); } while (snapshot.execution !== 'turn_completed' && Date.now() < timeout);
  report.execution = snapshot.execution;
  report.statusKeys = Object.keys(snapshot);
  report.finalMessage = snapshot.finalMessage;
  const history = await api.sessionHistory(delegation.rootSessionId);
  report.historyTypes = history.events.map(row => row.event.type);
  report.completed = history.events.some(row => row.event.type === 'turn/end' && row.event.data?.reason?.kind === 'completed');
  report.answerReturned = JSON.stringify(history).includes('local-adapter-pong');
  if (!report.completed || !report.answerReturned) throw new Error('Real DSH delegation did not return the expected completed turn');
  const followup = await service.continueTask(delegation.taskId, 'Return local-adapter-pong again. Do not use tools.');
  report.followupAccepted = followup.accepted;
  const followupDeadline = Date.now() + 15000;
  while (Date.now() < followupDeadline) {
    await new Promise(resolve => setTimeout(resolve, 100));
    const updated = await api.sessionHistory(delegation.rootSessionId);
    if (updated.events.filter(row => row.event.type === 'turn/end').length >= 2) { report.followupCompleted = true; break; }
  }
  await service.continueTask(delegation.taskId, 'Wait until cancelled. Do not use tools.');
  const cancelDeadline = Date.now() + 10000;
  while (calls < 3 && Date.now() < cancelDeadline) await new Promise(resolve => setTimeout(resolve, 50));
  const cancellation = await service.cancel(delegation.taskId, 'turn');
  report.cancelAccepted = cancellation.accepted;
  await new Promise(resolve => setTimeout(resolve, 200));
  const cancelledHistory = await api.sessionHistory(delegation.rootSessionId);
  const lastEnd = cancelledHistory.events.filter(row => row.event.type === 'turn/end').at(-1);
  report.cancelReason = lastEnd?.event.data?.reason?.kind;
  if (!report.followupCompleted || !report.cancelAccepted || report.cancelReason !== 'aborted') throw new Error('Follow-up/cancellation evidence incomplete');
} catch (error) {
  report.error = { name: error.name, message: error.message, cause: error.cause?.message };
  if (!calls) report.hostErrorHint = stderr.split('\n').filter(line => /error|failed/i.test(line)).map(line => line.replace(/token=[^\s&]+/g, 'token=REDACTED')).slice(0, 4);
  process.exitCode = 1;
} finally {
  await manager?.stop();
  child.kill();
  provider.closeAllConnections(); provider.close();
  report.localProviderRequests = calls;
  report.artifacts = home;
  await writeFile(`${home}/result.json`, JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report));
}
