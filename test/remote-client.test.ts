import assert from "node:assert/strict";
import { createServer } from "node:http";
import { once } from "node:events";
import { spawn } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test, type TestContext } from "node:test";
import { WebSocketServer } from "ws";
import { RemoteDshClient, connectRemoteHost } from "../src/remote-client.js";
import { getDshUnaryMetadata } from "../src/dsh-types.js";

async function host(context: TestContext, launchStatus = 302, interaction?: unknown) {
  const requests: Array<{ path: string; body: any; cookie?: string }> = [];
  const server = createServer(async (req, res) => {
    if (req.url === "/?token=local-launch") {
      res.writeHead(launchStatus, { location: "./", "set-cookie": "dsh_session=signed-local; Path=/; HttpOnly" }); res.end(); return;
    }
    let text = "";
    for await (const chunk of req) text += chunk;
    const body = text ? JSON.parse(text) : {};
    requests.push({ path: req.url!, body, ...(req.headers.cookie ? { cookie: req.headers.cookie } : {}) });
    if (req.headers.cookie !== "dsh_session=signed-local") { res.writeHead(401); res.end("unauthorized"); return; }
    let value: unknown;
    if (req.url === "/api/session/modelCatalog") value = { default: { provider: "deepseek-official", model: "deepseek-v4-flash" }, routableProviders: ["deepseek-official"], groups: [], failures: [] };
    else if (req.url === "/api/session/list") value = { items: [] };
    else if (req.url === "/api/session/create") value = { sessionId: "owned-session", agentPreset: "code" };
    else if (req.url === "/api/session/prompt") {
      assert.equal(body.payload.args.request.requestId, body.rpcId);
      assert.equal(body.payload.args.request.content[0].text, "Return pong");
      value = { accepted: true };
    } else if (req.url === "/api/session/cancel") value = { accepted: true };
    else if (req.url === "/api/$events/result") value = undefined;
    else { res.writeHead(404); res.end(); return; }
    res.setHeader("content-type", "application/json");
    res.end(JSON.stringify({ type: "server-response", rpcId: body.rpcId, result: { ok: true, value } }));
  });
  const websocket = new WebSocketServer({ server, path: "/api/remote.mux" });
  const opens: string[] = [];
  websocket.on("connection", (socket, request) => {
    assert.equal(request.headers.cookie, "dsh_session=signed-local");
    socket.on("message", data => {
      const message = JSON.parse(data.toString());
      if (message.type !== "open") return;
      opens.push(message.endpoint);
      const item = (value: unknown) => socket.send(JSON.stringify({ type: "item", streamId: message.streamId, value }));
      if (message.endpoint === "$events") {
        item({ type: "ready", clientId: "remote-client", host: { home: "/not-read" } });
        if (interaction) item(interaction);
      }
      if (message.endpoint === "session/control") item({ type: "baseline", value: { projections: {} } });
      if (message.endpoint === "session/follow") {
        assert.deepEqual(message.payload.args.request.address, { kind: "session", sessionId: "owned-session" });
        item({ type: "snapshot", cursor: 2, records: [
          { type: "event", event: { type: "turn/start", seq: 0, time: 1, data: { turn: 1 } } },
          { type: "event", event: { type: "assistant/message", seq: 1, time: 2, data: { turn: 1, step: 1, message: { content: [{ type: "text", text: "pong" }] } } } },
          { type: "event", event: { type: "turn/end", seq: 2, time: 3, data: { turn: 1, reason: { kind: "completed" } } } },
        ], hasMore: false, projections: { asOfSeq: 2, values: { inbox: { "next-turn": [], "next-step": [] } } } });
      }
    });
  });
  server.listen(0, "127.0.0.1"); await once(server, "listening");
  const address = server.address() as { port: number };
  context.after(async () => { for (const socket of websocket.clients) socket.terminate(); websocket.close(); server.closeAllConnections(); await new Promise<void>(resolve => server.close(() => resolve())); });
  return { url: `http://127.0.0.1:${address.port}`, requests, opens };
}

test("remote authentication exchanges a launch URL without following redirects or exposing the token", async context => {
  const h = await host(context);
  const connection = await connectRemoteHost(`${h.url}/?token=local-launch`);
  assert.deepEqual(connection, { origin: h.url, cookie: "dsh_session=signed-local" });
  assert.equal(h.requests.length, 0);
});

test("remote client submits one correlated prompt using slash endpoints and named args", async context => {
  const h = await host(context);
  const api = new RemoteDshClient({ origin: h.url, cookie: "dsh_session=signed-local" });
  const created = await api.sessionCreate({ cwd: "D:\\work\\a project" });
  const receipt = await api.sessionPrompt({ sessionId: created.sessionId, mode: "queue", content: [{ type: "text", text: "Return pong" }] });
  assert.equal(receipt.accepted, true);
  assert.equal(h.requests[0]?.path, "/api/session/create");
  assert.deepEqual(h.requests[0]?.body.payload, { args: { request: { cwd: "D:\\work\\a project" } } });
  assert.equal(getDshUnaryMetadata(receipt).issuedRpcId, h.requests[1]?.body.rpcId);
  assert.ok(!h.requests.some(r => /selectModel/.test(r.path)));
});

test("remote authentication accepts the 303 root exchange used by DSH 0.2.1", async context => {
  const h = await host(context, 303);
  assert.deepEqual(await connectRemoteHost(`${h.url}/?token=local-launch`), { origin: h.url, cookie: "dsh_session=signed-local" });
});

test("remote history reads a real follow snapshot and preserves completion evidence", async context => {
  const h = await host(context);
  const api = new RemoteDshClient({ origin: h.url, cookie: "dsh_session=signed-local" });
  const history = await api.sessionHistory("owned-session", { maxMessages: 3 });
  assert.equal(history.hasMore, false);
  assert.equal(history.events[2]?.event.type, "turn/end");
  assert.deepEqual(h.opens, ["session/follow"]);
});

test("remote mux opens owned session journals and translates snapshots for the existing ledger", async context => {
  const h = await host(context);
  const api = new RemoteDshClient({ origin: h.url, cookie: "dsh_session=signed-local" });
  await api.sessionCreate({ cwd: "D:\\work" });
  const controller = new AbortController();
  const frames = [];
  for await (const frame of api.openMux(controller.signal)) {
    frames.push(frame.payload);
    if (frame.payload.type === "session/event" && frame.payload.event.type === "turn/end") controller.abort();
  }
  assert.ok(frames.some(f => f.type === "session/subscribed" && f.lastSeq === 2));
  assert.equal(frames.filter(f => f.type === "session/event").length, 3);
  assert.ok(h.opens.includes("session/control"));
  assert.ok(h.opens.includes("$events"));
});

test("remote client rejects credential forwarding and malformed origins", async () => {
  await assert.rejects(connectRemoteHost("https://example.com/?token=secret"), /loopback/);
  assert.throws(() => new RemoteDshClient({ origin: "http://user:pass@127.0.0.1:3080", cookie: "x=y" }), /origin/);
  assert.throws(() => new RemoteDshClient({ origin: "http://127.0.0.1:3080", cookie: "x=y\r\nextra: value" }), /cookie/);
});

test("connection CLI reads the launch URL from stdin and writes only a private local connection record", async context => {
  const h = await host(context);
  const home = await mkdtemp(join(tmpdir(), "remote-connect-"));
  context.after(() => rm(home, { recursive: true, force: true, maxRetries: 5, retryDelay: 20 }));
  const out = join(home, "connection.json");
  const child = spawn(process.execPath, ["--import", "tsx", "src/connect.ts", "--out", out], { stdio: ["pipe", "pipe", "pipe"] });
  let output = "";
  child.stdout.on("data", data => { output += data; }); child.stderr.on("data", data => { output += data; });
  child.stdin.end(`${h.url}/?token=local-launch\n`);
  const [code] = await once(child, "exit");
  assert.equal(code, 0, output);
  assert.deepEqual(JSON.parse(await readFile(out, "utf8")), { origin: h.url, cookie: "dsh_session=signed-local" });
  assert.equal(output.includes("local-launch"), false);
  assert.equal(output.includes("signed-local"), false);
});

test("remote approvals require the exact owned request and are never answered automatically or replayed", async context => {
  const h = await host(context, 303, { type: "waterfall", event: "approval/request", eventId: "request-1", agentId: "owned-session", request: { approvalId: "approval-1", toolName: "Bash" } });
  const api = new RemoteDshClient({ origin: h.url, cookie: "dsh_session=signed-local" });
  await api.sessionCreate({ cwd: "D:\\work" });
  const controller = new AbortController();
  for await (const frame of api.openMux(controller.signal)) {
    if (frame.payload.type !== "approval/requested") continue;
    assert.equal(h.requests.filter(r => r.path === "/api/$events/result").length, 0);
    const response = { type: "client-response" as const, rpcId: "request-1", result: { ok: true as const, value: { sessionId: "owned-session", approvalId: "wrong-approval", outcome: "rejected" as const } } };
    assert.deepEqual(await api.respond(response), { accepted: false, reason: "bad-response" });
    response.result.value.approvalId = "approval-1";
    assert.deepEqual(await api.respond(response), { accepted: true });
    assert.deepEqual(await api.respond(response), { accepted: false, reason: "not-pending" });
    assert.deepEqual(h.requests.find(r => r.path === "/api/$events/result")?.body.payload, { args: { clientId: "remote-client", eventId: "request-1", outcome: { kind: "result", value: "rejected" } } });
    controller.abort();
  }
});

test("remote questions return a typed answer on the matching event channel", async context => {
  const h = await host(context, 303, { type: "waterfall", event: "user-questions/request", eventId: "question-1", agentId: "owned-session", request: { questions: [{ id: "q1", question: "Choose", options: [{ label: "A" }] }] } });
  const api = new RemoteDshClient({ origin: h.url, cookie: "dsh_session=signed-local" });
  await api.sessionCreate({ cwd: "D:\\work" });
  const controller = new AbortController();
  for await (const frame of api.openMux(controller.signal)) {
    if (frame.payload.type !== "question/requested") continue;
    await api.respond({ type: "client-response", rpcId: "question-1", result: { ok: true, value: { sessionId: "owned-session", answer: { answers: [{ id: "q1", selected: ["A"] }] } } } });
    assert.deepEqual(h.requests.find(r => r.path === "/api/$events/result")?.body.payload.args.outcome.value, { answers: [{ id: "q1", selected: ["A"] }] });
    controller.abort();
  }
});

test("remote connection resolution follows a replaced desktop connection record", async context => {
  const first = await host(context), second = await host(context);
  let connection = { origin: first.url, cookie: "dsh_session=signed-local" };
  const api = new RemoteDshClient(() => connection);
  await api.sessionList();
  connection = { origin: second.url, cookie: "dsh_session=signed-local" };
  await api.sessionList();
  assert.equal(first.requests.length, 1);
  assert.equal(second.requests.length, 1);
  assert.equal(api.baseUrl, second.url);
});
