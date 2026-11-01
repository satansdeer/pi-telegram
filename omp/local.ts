/**
 * Private local registration and attachment transport between a live OMP terminal and its Workspace daemon.
 * Zones: filesystem admission, authenticated registration, bounded bidirectional RPC, disconnect ownership.
 * Never starts, resumes, retries, or terminates an OMP process.
 */
import { randomBytes, timingSafeEqual } from "node:crypto";
import { constants } from "node:fs";
import type { Stats } from "node:fs";
import { chmod, lstat, mkdir, open, unlink } from "node:fs/promises";
import { createConnection, createServer } from "node:net";
import type { Socket } from "node:net";
import { homedir } from "node:os";
import { isAbsolute, join, resolve } from "node:path";

export type LocalHandler = (method: string, params: Record<string, unknown>) => Promise<unknown>;
export interface LocalPeer {
  request(method: string, params?: Record<string, unknown>): Promise<unknown>;
  close(): void;
}
export interface Attachment {
  id: string;
  threadId: number;
  name: string;
  groupId: number;
  sessionId: string;
  sessionFile: string;
  cwd: string;
}
export interface WorkspaceServer { close(): Promise<void>; }

type Pending = { method: string; resolve: (value: unknown) => void; reject: (error: Error) => void; timer?: NodeJS.Timeout };
type Paths = { directory: Stats; socket: Stats; token: Stats };
const MAX_FRAME = 8 * 1024 * 1024;
const MAX_BUFFERED_WRITE = MAX_FRAME * 2;
const MAX_PENDING = 64;
const DISCONNECTED = "Workspace connection closed; request outcome may be unknown and was not retried";
const NOT_RUNNING = "Workspace daemon is not running; start it before /telegram-connect";

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function exact(value: Record<string, unknown>, fields: string[]): boolean {
  const actual = Object.keys(value);
  return actual.length === fields.length && actual.every((key) => fields.includes(key));
}
function text(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && !value.includes("\0");
}
function nameValid(value: unknown, empty: boolean): value is string {
  if (typeof value !== "string" || (!empty && !value) || value.includes("\0")) return false;
  let length = 0;
  for (let offset = 0; offset < value.length; offset++) {
    if (++length > 128) return false;
    if (value.codePointAt(offset)! > 0xffff) offset++;
  }
  return true;
}
function methodValid(value: unknown): value is string {
  return typeof value === "string" && value.length <= 128 && /^[a-z][a-zA-Z0-9_-]*$/.test(value);
}
function idValid(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}
export function attachmentValid(value: unknown): value is Attachment {
  return record(value) && exact(value, ["id", "threadId", "name", "groupId", "sessionId", "sessionFile", "cwd"])
    && text(value.id) && idValid(value.threadId) && value.threadId >= 2 && nameValid(value.name, false)
    && typeof value.groupId === "number" && Number.isSafeInteger(value.groupId) && value.groupId < 0
    && text(value.sessionId) && text(value.sessionFile) && isAbsolute(value.sessionFile)
    && text(value.cwd) && isAbsolute(value.cwd);
}
function sameFile(left: Stats, right: Stats): boolean {
  return left.dev === right.dev && left.ino === right.ino;
}
function owned(stats: Stats): boolean {
  return typeof process.getuid === "function" && stats.uid === process.getuid();
}
function privatePath(stats: Stats, kind: "directory" | "socket" | "token"): void {
  const type = kind === "directory" ? stats.isDirectory() : kind === "socket" ? stats.isSocket() : stats.isFile();
  if (!type || stats.isSymbolicLink() || !owned(stats) || (stats.mode & 0o7777) !== (kind === "directory" ? 0o700 : 0o600)
    || (kind === "token" && (stats.nlink !== 1 || stats.size !== 64))) {
    throw new Error(`Unsafe Workspace ${kind} path or permissions; expected private owner-only access`);
  }
}
async function inspectPaths(stateDir: string): Promise<Paths> {
  const directory = await lstat(stateDir);
  privatePath(directory, "directory");
  const socket = await lstat(join(stateDir, "live.sock"));
  privatePath(socket, "socket");
  const token = await lstat(join(stateDir, "live.token"));
  privatePath(token, "token");
  return { directory, socket, token };
}
async function readToken(path: string, expected: Stats): Promise<string> {
  const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const stats = await file.stat();
    privatePath(stats, "token");
    if (!sameFile(stats, expected)) throw new Error("Workspace token changed while connecting");
    const buffer = Buffer.alloc(65);
    const { bytesRead } = await file.read(buffer, 0, buffer.length, 0);
    const token = buffer.toString("utf8", 0, bytesRead);
    if (bytesRead !== 64 || !/^[a-f0-9]{64}$/.test(token)) throw new Error("Invalid Workspace authentication token");
    return token;
  } finally { await file.close(); }
}
async function unlinkOwned(path: string, expected: Stats | undefined): Promise<void> {
  if (!expected) return;
  let current: Stats;
  try { current = await lstat(path); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return; throw error; }
  if (sameFile(current, expected) && !current.isSymbolicLink()) await unlink(path);
}

/** Responses are dispatched immediately, including while registration awaits terminal state. */
function createPeer(socket: Socket, handler: LocalHandler, onClose: () => void, onRegistered?: () => void): LocalPeer {
  const pending = new Map<number, Pending>();
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let chunks: Buffer[] = [];
  let bytes = 0;
  let outgoingId = 0;
  let incomingId = 0;
  let active = 0;
  let closed = false;
  const close = () => {
    if (closed) return;
    closed = true;
    chunks = [];
    for (const request of pending.values()) {
      clearTimeout(request.timer);
      request.reject(new Error(DISCONNECTED));
    }
    pending.clear();
    socket.destroy();
    try { onClose(); } catch { /* A consumer cannot prevent transport teardown. */ }
  };
  const send = (frame: Record<string, unknown>): boolean => {
    if (closed) return false;
    try {
      const body = JSON.stringify(frame);
      const size = Buffer.byteLength(body);
      if (size > MAX_FRAME || socket.writableLength + size + 1 > MAX_BUFFERED_WRITE) { close(); return false; }
      socket.write(`${body}\n`);
      return true;
    } catch { close(); return false; }
  };
  const receive = (buffer: Buffer) => {
    let frame: unknown;
    try { frame = JSON.parse(decoder.decode(buffer)); } catch { close(); return; }
    if (!record(frame) || !idValid(frame.id)) { close(); return; }
    if (frame.type === "response") {
      const success = exact(frame, ["type", "id", "result"]);
      if (!success && (!exact(frame, ["type", "id", "error"]) || !text(frame.error) || frame.error.length > 4096)) { close(); return; }
      const request = pending.get(frame.id);
      if (!request) { close(); return; }
      pending.delete(frame.id);
      clearTimeout(request.timer);
      if (success && request.method === "register" && frame.result === null) onRegistered?.();
      if (success) request.resolve(frame.result);
      else request.reject(new Error(frame.error as string));
      return;
    }
    if (frame.type !== "request" || !exact(frame, ["type", "id", "method", "params"])
      || !methodValid(frame.method) || !record(frame.params) || frame.id <= incomingId || active >= MAX_PENDING) { close(); return; }
    incomingId = frame.id;
    active++;
    const id = frame.id;
    let response: Promise<unknown>;
    try { response = Promise.resolve(handler(frame.method, frame.params)); }
    catch (error) { response = Promise.reject(error); }
    void response.then(
      (result) => { send({ type: "response", id, result: result === undefined ? null : result }); },
      (error: unknown) => {
        const message = error instanceof Error ? error.message : "Workspace request failed";
        send({ type: "response", id, error: message.slice(0, 4096) || "Workspace request failed" });
      },
    ).finally(() => { active--; });
  };
  socket.on("data", (chunk: Buffer) => {
    let offset = 0;
    while (!closed && offset < chunk.length) {
      const newline = chunk.indexOf(10, offset);
      const end = newline < 0 ? chunk.length : newline;
      const part = chunk.subarray(offset, end);
      bytes += part.length;
      if (bytes > MAX_FRAME) { close(); return; }
      if (part.length) chunks.push(part);
      if (newline < 0) return;
      if (!bytes) { close(); return; }
      const frame = chunks.length === 1 ? chunks[0]! : Buffer.concat(chunks, bytes);
      chunks = [];
      bytes = 0;
      receive(frame);
      offset = newline + 1;
    }
  });
  socket.on("error", close);
  socket.on("end", close);
  socket.on("close", close);
  return {
    request(method, params = {}) {
      if (closed) return Promise.reject(new Error(DISCONNECTED));
      if (!methodValid(method) || !record(params)) return Promise.reject(new Error("Invalid Workspace request"));
      if (method === "attach" && (!exact(params, ["name"]) || !nameValid(params.name, true))) {
        return Promise.reject(new Error("Workspace attach requires a name of at most 128 codepoints and no NUL"));
      }
      if (pending.size >= MAX_PENDING || outgoingId >= Number.MAX_SAFE_INTEGER) return Promise.reject(new Error("Too many pending Workspace requests"));
      const { promise, resolve, reject } = Promise.withResolvers<unknown>();
      const id = ++outgoingId;
      const request: Pending = { method, resolve, reject };
      pending.set(id, request);
      if (method !== "prompt") {
        request.timer = setTimeout(() => {
          pending.delete(id);
          reject(new Error(`Workspace ${method} timed out; outcome may be unknown and was not retried`));
          close();
        }, method === "attach" ? 40_000 : 10_000);
      }
      send({ type: "request", id, method, params });
      return method === "attach" ? promise.then(value => {
        if (!attachmentValid(value)) { close(); throw new Error("Invalid Workspace attachment response"); }
        return value;
      }) : promise;
    },
    close,
  };
}

export async function connectWorkspace(handler: LocalHandler, onClose: () => void): Promise<LocalPeer> {
  const stateDir = resolve(process.env.OMP_WORKSPACE_STATE_DIR ?? join(homedir(), ".config", "omp-workspace"));
  let peer: LocalPeer | undefined;
  let socket: Socket | undefined;
  let timer: NodeJS.Timeout | undefined;
  let handshakeStarted = false;
  let registered = false;
  try {
    const before = await inspectPaths(stateDir);
    socket = createConnection(join(stateDir, "live.sock"));
    const connection = socket;
    peer = createPeer(connection, async (method, params) => {
      if (!handshakeStarted) {
        peer?.close();
        throw new Error("Workspace request arrived before authentication");
      }
      if (!registered && method !== "state") throw new Error("Workspace registration has not completed");
      return handler(method, params);
    }, onClose, () => { registered = true; });
    const connected = Promise.withResolvers<void>();
    const onConnect = () => { connection.off("error", onError); connected.resolve(); };
    const onError = (error: Error) => { connection.off("connect", onConnect); connected.reject(error); };
    connection.once("connect", onConnect);
    connection.once("error", onError);
    timer = setTimeout(() => { connection.destroy(); onError(new Error("Workspace daemon connection timed out")); }, 5_000);
    await connected.promise;
    clearTimeout(timer);
    // Do not send the token to a replaced socket, symlink, or newly public path.
    const after = await inspectPaths(stateDir);
    if (!sameFile(before.directory, after.directory) || !sameFile(before.socket, after.socket) || !sameFile(before.token, after.token)) {
      throw new Error("Workspace paths changed while connecting");
    }
    const token = await readToken(join(stateDir, "live.token"), after.token);
    handshakeStarted = true;
    const result = await peer.request("register", { token });
    if (result !== null) throw new Error("Invalid Workspace registration response");
    return peer;
  } catch (error) {
    peer?.close();
    socket?.destroy();
    const code = (error as NodeJS.ErrnoException).code;
    if (code === "ENOENT" || code === "ECONNREFUSED") throw new Error(NOT_RUNNING);
    throw error;
  } finally { clearTimeout(timer); }
}

/** Caller must already hold the daemon lock before stale socket removal. */
export async function startWorkspaceServer(
  stateDir: string,
  register: (peer: LocalPeer) => Promise<void>,
  attach: (peer: LocalPeer, name: string) => Promise<Attachment>,
  detach: (peer: LocalPeer) => void,
): Promise<WorkspaceServer> {
  stateDir = resolve(stateDir);
  await mkdir(stateDir, { recursive: true, mode: 0o700 });
  privatePath(await lstat(stateDir), "directory");
  const socketPath = join(stateDir, "live.sock");
  const tokenPath = join(stateDir, "live.token");
  for (const [path, kind] of [[socketPath, "socket"], [tokenPath, "token"]] as const) {
    let previous: Stats;
    try { previous = await lstat(path); }
    catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") continue; throw error; }
    if (kind === "token") privatePath(previous, "token");
    else if (!previous.isSocket() || previous.isSymbolicLink() || !owned(previous)) throw new Error("Unsafe Workspace socket path; refusing to remove a non-socket");
    await unlink(path);
  }
  const token = randomBytes(32).toString("hex");
  const file = await open(tokenPath, "wx", 0o600);
  const tokenStats = await file.stat();
  try { await file.writeFile(token); await file.chmod(0o600); }
  catch (error) { await unlinkOwned(tokenPath, tokenStats); throw error; }
  finally { await file.close(); }
  const peers = new Set<LocalPeer>();
  let socketStats: Stats | undefined;
  let ready = false;
  let closing: Promise<void> | undefined;
  const server = createServer((socket) => {
    let phase: "new" | "registering" | "registered" | "rejected" = "new";
    let attaching = false;
    const timeout = setTimeout(() => peer.close(), 10_000);
    const peer = createPeer(socket, async (method, params) => {
      if (phase === "new") {
        if (method !== "register" || !exact(params, ["token"])
          || typeof params.token !== "string" || !/^[a-f0-9]{64}$/.test(params.token)) {
          peer.close();
          throw new Error("Invalid Workspace registration request");
        }
        phase = "rejected";
        if (!timingSafeEqual(Buffer.from(params.token, "hex"), Buffer.from(token, "hex"))) throw new Error("Workspace authentication rejected");
        phase = "registering";
        clearTimeout(timeout);
        try { await register(peer); }
        catch (error) { peer.close(); throw error; }
        if (phase !== "registering") throw new Error(DISCONNECTED);
        phase = "registered";
        return null;
      }
      if (phase !== "registered" || method !== "attach" || !exact(params, ["name"]) || !nameValid(params.name, true)) {
        peer.close();
        throw new Error("Invalid Workspace attachment request");
      }
      if (attaching) throw new Error("Workspace attachment is already in progress");
      attaching = true;
      try {
        const attachment = await attach(peer, params.name);
        if (!attachmentValid(attachment)) throw new Error("Invalid Workspace attachment result");
        return attachment;
      } finally { attaching = false; }
    }, () => {
      phase = "rejected";
      clearTimeout(timeout);
      peers.delete(peer);
      detach(peer);
    });
    peers.add(peer);
    if (!ready) peer.close();
  });
  const close = (): Promise<void> => {
    closing ??= (async () => {
      ready = false;
      for (const peer of peers) peer.close();
      try {
        if (server.listening) {
          const stopped = Promise.withResolvers<void>();
          server.close((error) => error ? stopped.reject(error) : stopped.resolve());
          await stopped.promise;
        }
      } finally {
        await unlinkOwned(socketPath, socketStats);
        await unlinkOwned(tokenPath, tokenStats);
      }
    })();
    return closing;
  };
  server.on("error", () => { void close().catch(() => {}); });
  try {
    const listening = Promise.withResolvers<void>();
    server.once("error", listening.reject);
    server.listen(socketPath, () => { server.off("error", listening.reject); listening.resolve(); });
    await listening.promise;
    socketStats = await lstat(socketPath);
    if (!socketStats.isSocket() || socketStats.isSymbolicLink() || !owned(socketStats)) throw new Error("Unsafe Workspace socket path after listening");
    await chmod(socketPath, 0o600);
    privatePath(await lstat(socketPath), "socket");
    ready = true;
    return { close };
  } catch (error) { await close(); throw error; }
}
