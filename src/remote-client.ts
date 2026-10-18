import { randomUUID } from "node:crypto";
import { lstatSync, readFileSync } from "node:fs";
import WebSocket from "ws";
import { z } from "zod";
import { DshRpcError, DshTransportError } from "./dsh-client.js";
import {
  attachDshUnaryMetadata, dshSessionListValueSchema, dshSessionCreateValueSchema,
  dshSessionPromptValueSchema, dshSessionRenameValueSchema, dshSessionCancelValueSchema,
  dshSessionUpdateQueueValueSchema, dshSessionHistorySchema, dshSessionModelsSchema,
  dshHostDescriptionSchema, dshMuxFrameSchema, dshSubagentListValueSchema,
  type DshApi, type DshClientResponse, type DshMuxFrame, type DshServerRequest,
  type DshSubagentAddress, type DshUnaryResult,
} from "./dsh-types.js";

export interface RemoteConnection { origin: string; cookie: string }
type ObjectValue = Record<string, any>;
const object = (value: unknown): ObjectValue => {
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new DshTransportError("invalid DSH Remote object");
  return value as ObjectValue;
};

export function remoteOrigin(raw: string): string {
  let url: URL;
  try { url = new URL(raw); } catch { throw new Error("invalid DSH Remote origin"); }
  if (!["http:", "https:"].includes(url.protocol) || url.username || url.password || url.pathname !== "/" || url.search || url.hash) {
    throw new Error("DSH Remote requires a plain origin without credentials, path or query");
  }
  if (!["127.0.0.1", "localhost", "[::1]"].includes(url.hostname)) throw new Error("DSH Remote requires a literal loopback host");
  return url.origin;
}

export function readRemoteConnection(path: string): RemoteConnection {
  try {
    const info = lstatSync(path);
    if (!info.isFile() || info.isSymbolicLink() || info.size > 65536) throw new Error();
    const record = JSON.parse(readFileSync(path, "utf8"));
    const origin = remoteOrigin(record.origin);
    if (typeof record.cookie !== "string" || !record.cookie || /[\r\n]/.test(record.cookie)) throw new Error();
    return { origin, cookie: record.cookie };
  } catch { throw new Error("invalid DSH_CONNECTION_FILE; reconnect using a fresh launch URL"); }
}

/** Exchange only an operator-supplied launch URL; never read browser cookies or signing keys. */
export async function connectRemoteHost(raw: string): Promise<RemoteConnection> {
  let url: URL;
  try { url = new URL(raw); } catch { throw new Error("invalid DSH launch URL"); }
  const origin = remoteOrigin(url.origin);
  if (url.username || url.password || url.pathname !== "/" || url.hash ||
      url.searchParams.getAll("token").length !== 1 || !url.searchParams.get("token") ||
      [...url.searchParams.keys()].some(key => key !== "token")) throw new Error("expected the DSH root launch URL with one token");
  let response: Response;
  try { response = await fetch(url, { redirect: "manual", signal: AbortSignal.timeout(10_000) }); }
  catch { throw new DshTransportError("DSH launch URL exchange failed; check the running Host"); }
  if (response.status !== 302 && response.status !== 303) throw new DshTransportError(`DSH launch URL exchange returned HTTP ${response.status}`);
  const cookie = response.headers.getSetCookie().map(value => value.split(";", 1)[0]).join("; ");
  if (!cookie) throw new DshTransportError("DSH launch URL exchange did not issue a session cookie");
  return { origin, cookie };
}

interface StreamMessage { type: "item" | "end" | "error"; streamId: string; value?: unknown; error?: { code: string; message: string; details: unknown } }

/** One independently cancellable Remote stream. Socket errors never masquerade as completion. */
async function* remoteStream(connection: RemoteConnection, endpoint: string, args: unknown, signal: AbortSignal, timeoutMs: number): AsyncGenerator<unknown> {
  if (signal.aborted) return;
  const url = new URL("/api/remote.mux", connection.origin);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  const socket = new WebSocket(url, { headers: { cookie: connection.cookie }, handshakeTimeout: timeoutMs, maxPayload: 16 * 1024 * 1024 });
  const streamId = randomUUID();
  const inbox: Array<StreamMessage | Error> = [];
  let wake: (() => void) | undefined;
  let stopped = false;
  const push = (value: StreamMessage | Error) => { inbox.push(value); wake?.(); wake = undefined; };
  socket.on("open", () => socket.send(JSON.stringify({ type: "open", streamId, endpoint, payload: { args } })));
  socket.on("message", bytes => {
    try {
      const frame = object(JSON.parse(bytes.toString()));
      if (frame.streamId !== streamId || !["item", "end", "error"].includes(frame.type)) throw new Error();
      push(frame as StreamMessage);
    } catch { push(new DshTransportError("invalid DSH Remote stream frame")); }
  });
  socket.on("error", () => push(new DshTransportError("DSH Remote stream connection failed; reconnect the Host if its port changed")));
  socket.on("close", () => { if (!stopped && !signal.aborted) push(new DshTransportError("DSH Remote stream closed before cancellation")); wake?.(); });
  const abort = () => { stopped = true; wake?.(); if (socket.readyState === WebSocket.OPEN) socket.close(); else socket.terminate(); };
  signal.addEventListener("abort", abort, { once: true });
  if (signal.aborted) abort();
  try {
    while (!signal.aborted) {
      const frame = inbox.shift();
      if (!frame) { await new Promise<void>(resolve => { wake = resolve; }); continue; }
      if (frame instanceof Error) throw frame;
      if (frame.type === "error") throw new DshRpcError(frame.error?.code ?? "gateway/internal", frame.error?.message ?? "DSH Remote stream failed", frame.error?.details);
      if (frame.type === "end") return;
      yield frame.value;
    }
  } finally {
    stopped = true;
    signal.removeEventListener("abort", abort);
    if (socket.readyState === WebSocket.OPEN) { socket.send(JSON.stringify({ type: "cancel", streamId })); socket.close(); }
    else if (socket.readyState === WebSocket.CONNECTING) socket.terminate();
  }
}

/** Adapt DSH 0.2 Remote contracts to the bridge's existing coordination contract. */
export class RemoteDshClient implements DshApi {
  private readonly readConnection: () => RemoteConnection;
  get baseUrl(): string { return this.connection.origin; }
  private get connection(): RemoteConnection {
    const connection = this.readConnection();
    const origin = remoteOrigin(connection.origin);
    if (!connection.cookie || /[\r\n]/.test(connection.cookie) || connection.cookie.length > 65536) throw new Error("invalid DSH session cookie");
    return { origin, cookie: connection.cookie };
  }
  private readonly watched = new Map<string, ObjectValue>();
  private watchAdded: ((id: string) => void) | undefined;
  private readonly pending = new Map<string, { clientId: string; sessionId: string; kind: "approval" | "question"; approvalId?: string }>();
  constructor(connection: RemoteConnection | (() => RemoteConnection), private readonly timeoutMs = 30_000, private readonly version = "remote-api") {
    this.readConnection = typeof connection === "function" ? connection : () => connection;
    void this.connection;
  }

  private async call(endpoint: string, args: unknown, signal?: AbortSignal, rpcId = randomUUID()): Promise<unknown> {
    const connection = this.connection;
    let response: Response;
    try {
      response = await fetch(new URL(`/api/${endpoint}`, connection.origin), {
        method: "POST", redirect: "error", headers: { "content-type": "application/json", cookie: connection.cookie },
        body: JSON.stringify({ type: "client-request", rpcId, method: endpoint, payload: { args } }),
        signal: signal ? AbortSignal.any([signal, AbortSignal.timeout(this.timeoutMs)]) : AbortSignal.timeout(this.timeoutMs),
      });
    } catch { throw new DshTransportError(`DSH Remote request failed for ${endpoint}`); }
    if (!response.ok) throw new DshTransportError(`DSH Remote ${endpoint}: HTTP ${response.status}${response.status === 401 ? "; reconnect using a fresh launch URL" : ""}`);
    let envelope: ObjectValue;
    try { envelope = object(await response.json()); } catch { throw new DshTransportError("invalid DSH Remote JSON response"); }
    if (envelope.type !== "server-response" || envelope.rpcId !== rpcId) throw new DshTransportError("invalid DSH Remote correlation");
    const result = object(envelope.result);
    if (result.ok === false) throw new DshRpcError(result.error?.code ?? "gateway/internal", result.error?.message ?? "DSH Remote rejected request", result.error?.details);
    if (result.ok !== true) throw new DshTransportError("invalid DSH Remote result");
    return result.value;
  }

  private value<T extends object>(schema: z.ZodType<T>, value: unknown, method: string, id = randomUUID()): DshUnaryResult<T> {
    const parsed = schema.safeParse(value);
    if (!parsed.success) throw new DshTransportError(`incompatible DSH Remote response for ${method}`);
    return attachDshUnaryMetadata(parsed.data, { method, issuedRpcId: id });
  }

  private watch(sessionId: string, address = { kind: "session", sessionId } as ObjectValue) {
    if (this.watched.has(sessionId)) return;
    this.watched.set(sessionId, address); this.watchAdded?.(sessionId);
  }

  async hostDescribe(signal?: AbortSignal) {
    const catalog = object(await this.call("session/modelCatalog", {}, signal));
    const sessions = await this.sessionList(signal);
    return this.value(dshHostDescriptionSchema, { version: this.version, cwd: process.cwd(), attachedSessions: sessions.items.length, canOpenPath: false, ...catalog.default }, "host.describe");
  }
  async sessionList(signal?: AbortSignal) {
    const result = object(await this.call("session/list", { _request: {} }, signal));
    const items = (result.items as ObjectValue[]).map(item => ({ ...item, ...(typeof item.projections?.values?.agentPreset === "string" ? { agentPreset: item.projections.values.agentPreset } : {}) }));
    return this.value(dshSessionListValueSchema, { items }, "session.list");
  }
  async sessionCreate(payload: Parameters<DshApi["sessionCreate"]>[0], signal?: AbortSignal) {
    const result = this.value(dshSessionCreateValueSchema, await this.call("session/create", { request: payload }, signal), "session.create");
    this.watch(result.sessionId); return result;
  }
  async sessionModels(sessionId: string, signal?: AbortSignal) {
    const catalog = object(await this.call("session/modelCatalog", {}, signal));
    const projections = object(await this.call("session/projections", { request: { sessionId } }, signal));
    const selection = projections.values?.modelSelection?.next ?? projections.values?.modelSelection?.lastUsed ?? catalog.default;
    return this.value(dshSessionModelsSchema, { current: selection, routable: Array.isArray(catalog.routableProviders) && catalog.routableProviders.includes(selection?.provider), groups: catalog.groups, failures: catalog.failures }, "session.models");
  }
  async sessionPrompt(payload: Parameters<DshApi["sessionPrompt"]>[0], signal?: AbortSignal) {
    const id = randomUUID();
    return this.value(dshSessionPromptValueSchema, await this.call("session/prompt", { request: { ...payload, requestId: id } }, signal, id), "session.prompt", id);
  }
  async sessionRename(sessionId: string, title: string, signal?: AbortSignal) {
    return this.value(dshSessionRenameValueSchema, await this.call("session/rename", { request: { sessionId, title } }, signal), "session.rename");
  }
  async sessionCancel(sessionId: string, signal?: AbortSignal) {
    return this.value(dshSessionCancelValueSchema, await this.call("session/cancel", { request: { sessionId } }, signal), "session.cancel");
  }
  async sessionUpdateQueue(sessionId: string, itemId: string, action: { kind: "remove" }, signal?: AbortSignal) {
    return this.value(dshSessionUpdateQueueValueSchema, await this.call("session/updateQueue", { request: { sessionId, itemId, action } }, signal), "session.updateQueue");
  }
  async agentPresetList(): ReturnType<DshApi["agentPresetList"]> {
    throw new DshRpcError("unsupported_capability", "Remote mode follows the DSH default preset; explicit roster selection is unavailable", {});
  }
  async subagentList(parentSessionId: string, signal?: AbortSignal) {
    const projections = object(await this.call("session/projections", { request: { sessionId: parentSessionId } }, signal));
    const catalog: ObjectValue[] = projections.values?.subagentCatalog ?? [];
    const sessions = await this.sessionList(signal);
    const entries = catalog.filter(c => c.mode === "one-shot" || c.mode === "continuable").map(c => ({ kind: "child", id: c.id, mode: c.mode, ...(c.label === undefined ? {} : { label: c.label }), activity: sessions.items.find(s => s.sessionId === c.id)?.running ? "running" : "inactive", hasChildren: false }));
    return this.value(dshSubagentListValueSchema, { entries, parentAvailable: true }, "subagent.list");
  }

  private async history(address: ObjectValue, options: { beforeSeq?: number; maxMessages?: number }, signal?: AbortSignal) {
    const lifetime = new AbortController();
    const readSignal = AbortSignal.any([lifetime.signal, AbortSignal.timeout(this.timeoutMs), ...(signal ? [signal] : [])]);
    try {
      for await (const raw of remoteStream(this.connection, "session/follow", { request: { address, maxMessages: options.maxMessages ?? 50 } }, readSignal, this.timeoutMs)) {
        const snapshot = object(raw);
        if (snapshot.type !== "snapshot") throw new DshTransportError("DSH history stream omitted its opening snapshot");
        let page = snapshot;
        if (options.beforeSeq !== undefined) page = object(await this.call("session/page", { request: { address, throughSeq: snapshot.cursor, ...options } }, readSignal));
        return this.value(dshSessionHistorySchema, { events: page.records, hasMore: page.hasMore, ...(snapshot.projections ? { projections: snapshot.projections } : {}) }, "session.history");
      }
      throw new DshTransportError("DSH history ended before its opening snapshot");
    } finally { lifetime.abort(); }
  }
  sessionHistory(sessionId: string, options = {}, signal?: AbortSignal) {
    this.watch(sessionId); return this.history({ kind: "session", sessionId }, options, signal);
  }
  subagentHistory(address: DshSubagentAddress, options = {}, signal?: AbortSignal) {
    const remoteAddress = { kind: "subagent", ...address };
    this.watch(address.childSessionId, remoteAddress); return this.history(remoteAddress, options, signal);
  }

  async respond(message: DshClientResponse, signal?: AbortSignal): ReturnType<DshApi["respond"]> {
    const pending = this.pending.get(message.rpcId);
    const value = message.result.value;
    if (!pending) return { accepted: false, reason: "not-pending" };
    if (value.sessionId !== pending.sessionId || ("approvalId" in value) !== (pending.kind === "approval") ||
        ("approvalId" in value && value.approvalId !== pending.approvalId)) return { accepted: false, reason: "bad-response" };
    const answer = "approvalId" in value ? value.outcome : value.answer;
    // No retry: a lost receipt must not answer the same approval twice.
    await this.call("$events/result", { clientId: pending.clientId, eventId: message.rpcId, outcome: { kind: "result", value: answer } }, signal);
    this.pending.delete(message.rpcId);
    return { accepted: true };
  }

  async *openMux(signal: AbortSignal, onOpen?: () => void): AsyncGenerator<DshServerRequest<DshMuxFrame>> {
    const lifetime = new AbortController();
    const combined = AbortSignal.any([signal, lifetime.signal]);
    const inbox: Array<DshServerRequest<DshMuxFrame> | Error> = [];
    const workers = new Set<Promise<void>>();
    const following = new Set<string>();
    let wake: (() => void) | undefined;
    let clientId: string | undefined;
    const push = (value: DshServerRequest<DshMuxFrame> | Error) => { if (!combined.aborted) inbox.push(value); wake?.(); wake = undefined; };
    const emit = (value: unknown, id = randomUUID()) => { const payload = dshMuxFrameSchema.parse(value); push({ type: "server-request", rpcId: id, method: payload.type, payload }); };
    const launch = (work: () => Promise<void>) => {
      const job = work().catch(error => { if (!combined.aborted) push(error instanceof Error ? error : new DshTransportError("DSH stream failed")); });
      workers.add(job); void job.finally(() => workers.delete(job));
    };
    const projections = (sessionId: string, values: ObjectValue, seq: number) => {
      const inboxValue = values.inbox;
      if (inboxValue) {
        const items = ["next-turn", "next-step"].flatMap(target => (inboxValue[target] ?? []).map((message: ObjectValue) => ({ id: message.id, placement: target === "next-turn" ? "queued" : "steering", message: message.message ?? message })));
        emit({ type: "session/queue", sessionId, items });
      }
      for (const [key, value] of Object.entries(values)) emit({ type: "session/projection", sessionId, key, value, seq });
    };
    const follow = (sessionId: string) => {
      if (following.has(sessionId)) return; following.add(sessionId);
      launch(async () => {
        for await (const raw of remoteStream(this.connection, "session/follow", { request: { address: this.watched.get(sessionId), maxMessages: 50 } }, combined, this.timeoutMs)) {
          const frame = object(raw);
          if (frame.type === "snapshot") {
            emit({ type: "session/subscribed", sessionId, lastSeq: frame.cursor });
            for (const record of frame.records) emit({ type: "session/event", sessionId, event: record.event });
            if (frame.projections) projections(sessionId, frame.projections.values, frame.projections.asOfSeq);
          } else if (frame.type === "event") emit({ type: "session/event", sessionId, event: frame.event });
        }
        if (!combined.aborted) throw new DshTransportError("DSH session follow ended unexpectedly");
      });
    };
    this.watchAdded = follow;
    launch(async () => {
      for await (const raw of remoteStream(this.connection, "$events", {}, combined, this.timeoutMs)) {
        const frame = object(raw);
        if (frame.type === "ready") { clientId = frame.clientId; onOpen?.(); for (const id of this.watched.keys()) follow(id); }
        else if (frame.type === "cancel") {
          const pending = this.pending.get(frame.eventId);
          if (pending) { emit(pending.kind === "approval" ? { type: "approval/resolved", sessionId: pending.sessionId, approvalId: pending.approvalId, outcome: "cancelled" } : { type: "question/resolved", sessionId: pending.sessionId, questionRpcId: frame.eventId, outcome: "cancelled" }, frame.eventId); this.pending.delete(frame.eventId); }
        } else if (frame.type === "waterfall" && clientId) {
          const kind = frame.event === "approval/request" ? "approval" : frame.event === "user-questions/request" ? "question" : undefined;
          if (!kind || !this.watched.has(frame.agentId)) { await this.call("$events/result", { clientId, eventId: frame.eventId, outcome: { kind: "next" } }, combined); continue; }
          const approvalId = frame.request.approvalId ?? frame.request.id ?? frame.eventId;
          this.pending.set(frame.eventId, { clientId, sessionId: frame.agentId, kind, ...(kind === "approval" ? { approvalId } : {}) });
          emit(kind === "approval" ? { ...frame.request, type: "approval/requested", sessionId: frame.agentId, approvalId, toolName: frame.request.toolName ?? frame.request.tool?.name ?? "tool" } : { type: "question/requested", sessionId: frame.agentId, questions: frame.request.questions }, frame.eventId);
        }
      }
      if (!combined.aborted) throw new DshTransportError("DSH event stream ended unexpectedly");
    });
    launch(async () => {
      for await (const raw of remoteStream(this.connection, "session/control", {}, combined, this.timeoutMs)) {
        const frame = object(raw);
        if (frame.type === "baseline") { for (const [id, block] of Object.entries(frame.value.projections)) if (this.watched.has(id)) projections(id, object(block).values, object(block).asOfSeq); }
        else if (frame.type === "projection" && this.watched.has(frame.sessionId)) projections(frame.sessionId, { [frame.key]: frame.value }, frame.seq);
      }
      if (!combined.aborted) throw new DshTransportError("DSH control stream ended unexpectedly");
    });
    const abort = () => { wake?.(); }; combined.addEventListener("abort", abort, { once: true });
    try {
      while (!combined.aborted) {
        const value = inbox.shift();
        if (value instanceof Error) throw value;
        if (value) yield value;
        else await new Promise<void>(resolve => { wake = resolve; });
      }
    } finally {
      this.watchAdded = undefined; lifetime.abort(); combined.removeEventListener("abort", abort);
      await Promise.allSettled(workers); this.pending.clear();
    }
  }
}
