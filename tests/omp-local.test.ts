/**
 * Live terminal attachment transport regression tests.
 * Zones: bidirectional admission, authentication, ambiguous disconnects, private filesystem paths.
 */
import assert from "node:assert/strict";
import { chmodSync } from "node:fs";
import { chmod, lstat, mkdtemp, readFile, realpath, rm, stat, symlink, unlink, writeFile } from "node:fs/promises";
import { createConnection, createServer } from "node:net";
import type { Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import type { TestContext } from "node:test";
import { connectWorkspace, startWorkspaceServer } from "../omp/local.ts";
import type { Attachment, LocalPeer, WorkspaceServer } from "../omp/local.ts";

const ATTACHMENT: Attachment = {
  id: "abcdef123456", threadId: 42, name: "Terminal", groupId: -100123,
  sessionId: "same-running-session", sessionFile: "/sessions/live.jsonl", cwd: "/workspace",
};
const STATE = { sessionId: ATTACHMENT.sessionId, sessionFile: ATTACHMENT.sessionFile, cwd: ATTACHMENT.cwd, isStreaming: false };

async function fixture(t: TestContext) {
  const dir = await realpath(await mkdtemp(join(tmpdir(), "ol-")));
  await chmod(dir, 0o700);
  const previous = process.env.OMP_WORKSPACE_STATE_DIR;
  process.env.OMP_WORKSPACE_STATE_DIR = dir;
  const servers: WorkspaceServer[] = [];
  t.after(async () => {
    try { for (const server of servers.reverse()) await server.close(); }
    finally {
      if (previous === undefined) delete process.env.OMP_WORKSPACE_STATE_DIR;
      else process.env.OMP_WORKSPACE_STATE_DIR = previous;
      await rm(dir, { recursive: true, force: true });
    }
  });
  return { dir, servers };
}

async function rawClient(path: string): Promise<Socket> {
  const socket = createConnection(path);
  const connected = Promise.withResolvers<void>();
  socket.once("connect", connected.resolve);
  socket.on("error", connected.reject);
  await connected.promise;
  return socket;
}

function readFrame(socket: Socket): Promise<Record<string, unknown>> {
  const frame = Promise.withResolvers<Record<string, unknown>>();
  let body = "";
  const receive = (chunk: Buffer) => {
    body += chunk.toString("utf8");
    const newline = body.indexOf("\n");
    if (newline < 0) return;
    socket.off("data", receive);
    try { frame.resolve(JSON.parse(body.slice(0, newline)) as Record<string, unknown>); }
    catch (error) { frame.reject(error); }
  };
  socket.on("data", receive);
  socket.once("error", frame.reject);
  socket.once("end", () => frame.reject(new Error("Socket ended before a response")));
  return frame.promise;
}

async function fakeDaemon(dir: string, accepted: (socket: Socket) => void): Promise<WorkspaceServer> {
  await writeFile(join(dir, "live.token"), "a".repeat(64), { mode: 0o600 });
  const sockets = new Set<Socket>();
  const server = createServer((socket) => {
    sockets.add(socket);
    socket.on("error", () => {});
    socket.once("close", () => sockets.delete(socket));
    accepted(socket);
  });
  const listening = Promise.withResolvers<void>();
  server.once("error", listening.reject);
  server.listen(join(dir, "live.sock"), listening.resolve);
  await listening.promise;
  await chmod(join(dir, "live.sock"), 0o600);
  return {
    async close() {
      for (const socket of sockets) socket.destroy();
      if (!server.listening) return;
      const stopped = Promise.withResolvers<void>();
      server.close((error) => error ? stopped.reject(error) : stopped.resolve());
      await stopped.promise;
    },
  };
}

test("Registration requests terminal state without attaching, then repeated attach multiplexes prompt, state and abort", { timeout: 10_000 }, async (t) => {
  const { dir, servers } = await fixture(t);
  let daemonPeer: LocalPeer | undefined;
  let detached = 0;
  let closed = 0;
  let prompts = 0;
  let aborts = 0;
  let attaches = 0;
  const started = Promise.withResolvers<void>();
  const completion = Promise.withResolvers<string>();
  const server = await startWorkspaceServer(dir, async (peer) => {
    daemonPeer = peer;
    assert.deepEqual(await peer.request("state"), STATE);
  }, async (_peer, name) => {
    attaches++;
    assert.equal(name, "Terminal");
    return ATTACHMENT;
  }, () => { detached++; });
  servers.push(server);
  const terminal = await connectWorkspace(async (method, params) => {
    if (method === "state") { assert.deepEqual(params, {}); return STATE; }
    if (method === "prompt") { assert.deepEqual(params, { text: "hello" }); prompts++; started.resolve(); return completion.promise; }
    if (method === "abort") { aborts++; completion.resolve("aborted locally"); return; }
    throw new Error("Interactive answers are unsupported on a terminal attachment");
  }, () => { closed++; });
  assert.equal(attaches, 0, "Discovery registration must not create or attach a Telegram topic");
  assert.deepEqual(await terminal.request("attach", { name: "Terminal" }), ATTACHMENT);
  assert.deepEqual(await terminal.request("attach", { name: "Terminal" }), ATTACHMENT);
  assert.equal(attaches, 2, "Authenticated connections support repeated attachment requests");
  assert.ok(daemonPeer);
  const reply = daemonPeer.request("prompt", { text: "hello" });
  await started.promise;
  assert.deepEqual(await daemonPeer.request("state"), STATE, "A prompt never blocks state or abort responses");
  assert.equal(await daemonPeer.request("abort"), null);
  assert.equal(await reply, "aborted locally");
  await assert.rejects(daemonPeer.request("answer", { id: "dialog", response: {} }), /unsupported/);
  assert.equal(prompts, 1);
  assert.equal(aborts, 1);
  const directory = await stat(dir), socket = await stat(join(dir, "live.sock")), token = await stat(join(dir, "live.token"));
  assert.equal(directory.mode & 0o777, 0o700);
  assert.equal(socket.mode & 0o777, 0o600);
  assert.equal(token.mode & 0o777, 0o600);
  await server.close();
  await server.close();
  terminal.close();
  assert.equal(detached, 1);
  assert.equal(closed, 1);
  assert.equal(aborts, 1, "Transport close must never abort the terminal");
});

test("Authentication rejection never invokes registration or attach, and malformed frames receive no error echo", { timeout: 10_000 }, async (t) => {
  const { dir, servers } = await fixture(t);
  let registrations = 0;
  let attaches = 0;
  const detached = Promise.withResolvers<void>();
  const server = await startWorkspaceServer(dir, async () => { registrations++; }, async () => { attaches++; return ATTACHMENT; }, () => detached.resolve());
  servers.push(server);
  const socket = await rawClient(join(dir, "live.sock"));
  t.after(() => socket.destroy());
  const response = readFrame(socket);
  socket.write(`${JSON.stringify({ type: "request", id: 1, method: "register", params: { token: "0".repeat(64) } })}\n`);
  assert.match(String((await response).error), /authentication rejected/);
  socket.destroy();
  await detached.promise;
  assert.equal(registrations, 0);
  assert.equal(attaches, 0);
  for (const invalid of [
    "not-json\n",
    `${JSON.stringify({ type: "response", id: 999, error: "unmatched" })}\n`,
    `${JSON.stringify({ type: "request", id: 1, method: "register", params: {}, extra: true })}\n`,
    `${JSON.stringify({ type: "request", id: 1, method: "register", params: { token: "0".repeat(64), extra: true } })}\n`,
    `${JSON.stringify({ type: "request", id: 1, method: "attach", params: { token: "0".repeat(64), name: "" } })}\n`,
    `${"x".repeat(8 * 1024 * 1024 + 1)}\n`,
  ]) {
    const invalidSocket = await rawClient(join(dir, "live.sock"));
    const ended = Promise.withResolvers<void>();
    let echoed = 0;
    invalidSocket.on("data", (chunk: Buffer) => { echoed += chunk.length; });
    invalidSocket.once("close", ended.resolve);
    invalidSocket.write(invalid);
    await ended.promise;
    assert.equal(echoed, 0, "Malformed input must not create protocol response loops");
  }
  assert.equal(attaches, 0);
  assert.equal(registrations, 0);
});

test("Disconnect rejects an admitted pending prompt once, without retrying or terminating the terminal", { timeout: 10_000 }, async (t) => {
  const { dir, servers } = await fixture(t);
  let daemonPeer: LocalPeer | undefined;
  let detached = 0;
  let closed = 0;
  let prompts = 0;
  let aborts = 0;
  const admitted = Promise.withResolvers<void>();
  const completion = Promise.withResolvers<string>();
  const terminalClosed = Promise.withResolvers<void>();
  servers.push(await startWorkspaceServer(dir, async (peer) => { daemonPeer = peer; await peer.request("state"); }, async () => ATTACHMENT, () => { detached++; }));
  const terminal = await connectWorkspace(async (method) => {
    if (method === "state") return STATE;
    if (method === "abort") { aborts++; return; }
    prompts++;
    admitted.resolve();
    return completion.promise;
  }, () => { closed++; terminalClosed.resolve(); });
  assert.ok(daemonPeer);
  const pending = assert.rejects(daemonPeer.request("prompt", { text: "one turn" }), /outcome may be unknown.*not retried/);
  await admitted.promise;
  terminal.close();
  terminal.close();
  await Promise.all([pending, terminalClosed.promise]);
  assert.equal(detached, 1);
  assert.equal(closed, 1);
  assert.equal(prompts, 1);
  assert.equal(aborts, 0);
  completion.resolve("the original terminal can still finish its turn");
  await assert.rejects(daemonPeer.request("state"), /not retried/);
  assert.equal(prompts, 1);
});

test("Disconnect also rejects registration while daemon state admission is pending", { timeout: 10_000 }, async (t) => {
  const { dir, servers } = await fixture(t);
  const admitted = Promise.withResolvers<void>();
  const never = Promise.withResolvers<unknown>();
  let detached = 0;
  let closed = 0;
  const server = await startWorkspaceServer(dir, async (peer) => {
    await peer.request("state");
  }, async () => ATTACHMENT, () => { detached++; });
  servers.push(server);
  const attaching = connectWorkspace(async () => { admitted.resolve(); return never.promise; }, () => { closed++; });
  const rejected = assert.rejects(attaching, /outcome may be unknown.*not retried/);
  await admitted.promise;
  await server.close();
  await rejected;
  never.resolve(STATE);
  assert.equal(detached, 1);
  assert.equal(closed, 1);
});

test("Eight-megabyte-class prompt replies are bounded without truncation", { timeout: 10_000 }, async (t) => {
  const { dir, servers } = await fixture(t);
  let daemonPeer: LocalPeer | undefined;
  servers.push(await startWorkspaceServer(dir, async (peer) => { daemonPeer = peer; }, async () => ATTACHMENT, () => {}));
  const body = "x".repeat(8 * 1024 * 1024 - 256);
  const terminal = await connectWorkspace(async () => body, () => {});
  assert.ok(daemonPeer);
  assert.equal(await daemonPeer.request("prompt", { text: "large response" }), body);
  terminal.close();
});

test("Missing daemon reports the actionable not-running diagnostic", async (t) => {
  await fixture(t);
  await assert.rejects(connectWorkspace(async () => STATE, () => {}), /daemon is not running; start it/);
});

for (const unsafe of ["socket-file", "socket-symlink", "token-symlink", "directory-permissions"] as const) {
  test(`Server refuses ${unsafe} without replacing unsafe files`, async (t) => {
    const { dir } = await fixture(t);
    const target = join(dir, "untouched");
    await writeFile(target, "leave me alone", { mode: 0o600 });
    if (unsafe === "socket-file") await writeFile(join(dir, "live.sock"), "not a socket", { mode: 0o600 });
    if (unsafe === "socket-symlink") await symlink(target, join(dir, "live.sock"));
    if (unsafe === "token-symlink") await symlink(target, join(dir, "live.token"));
    if (unsafe === "directory-permissions") await chmod(dir, 0o755);
    await assert.rejects(startWorkspaceServer(dir, async () => {}, async () => ATTACHMENT, () => {}), /Unsafe Workspace/);
    assert.equal(await readFile(target, "utf8"), "leave me alone");
    if (unsafe === "socket-symlink") assert.equal((await lstat(join(dir, "live.sock"))).isSymbolicLink(), true);
    if (unsafe === "token-symlink") assert.equal((await lstat(join(dir, "live.token"))).isSymbolicLink(), true);
  });
}

for (const unsafe of ["socket-symlink", "socket-permissions", "token-symlink", "token-permissions", "directory-permissions", "directory-symlink"] as const) {
  test(`Client refuses ${unsafe} before connecting or disclosing its token`, async (t) => {
    const { dir, servers } = await fixture(t);
    let connections = 0;
    let received = 0;
    servers.push(await fakeDaemon(dir, (socket) => { connections++; socket.on("data", (chunk: Buffer) => { received += chunk.length; }); }));
    if (unsafe === "socket-symlink") {
      await unlink(join(dir, "live.sock"));
      await symlink(join(dir, "target.sock"), join(dir, "live.sock"));
    }
    if (unsafe === "socket-permissions") await chmod(join(dir, "live.sock"), 0o666);
    if (unsafe === "token-symlink") {
      await writeFile(join(dir, "target.token"), "a".repeat(64), { mode: 0o600 });
      await unlink(join(dir, "live.token"));
      await symlink(join(dir, "target.token"), join(dir, "live.token"));
    }
    if (unsafe === "token-permissions") await chmod(join(dir, "live.token"), 0o644);
    if (unsafe === "directory-permissions") await chmod(dir, 0o755);
    if (unsafe === "directory-symlink") {
      await symlink(dir, join(dir, "alias"));
      process.env.OMP_WORKSPACE_STATE_DIR = join(dir, "alias");
    }
    await assert.rejects(connectWorkspace(async () => STATE, () => {}), /Unsafe Workspace/);
    assert.equal(connections, 0);
    assert.equal(received, 0);
  });
}

test("Post-connect permission change is rejected before token disclosure", { timeout: 10_000 }, async (t) => {
  const { dir, servers } = await fixture(t);
  let received = 0;
  let closed = 0;
  servers.push(await fakeDaemon(dir, (socket) => {
    chmodSync(join(dir, "live.sock"), 0o666);
    socket.on("data", (chunk: Buffer) => { received += chunk.length; });
  }));
  await assert.rejects(connectWorkspace(async () => STATE, () => { closed++; }), /Unsafe Workspace/);
  assert.equal(received, 0);
  assert.equal(closed, 1);
});

test("Attachment response validates every required field and rejects unknown fields", { timeout: 10_000 }, async (t) => {
  const { dir, servers } = await fixture(t);
  const responses: unknown[] = [
    { ...ATTACHMENT, extra: true }, { ...ATTACHMENT, threadId: 1 }, { ...ATTACHMENT, groupId: 123 },
    { ...ATTACHMENT, cwd: "relative" }, { ...ATTACHMENT, sessionFile: "relative.jsonl" },
    { ...ATTACHMENT, sessionId: "" }, { ...ATTACHMENT, name: "x".repeat(129) }, {},
  ];
  servers.push(await fakeDaemon(dir, (socket) => {
    void readFrame(socket).then(async (registration) => {
      const attachment = readFrame(socket);
      socket.write(`${JSON.stringify({ type: "response", id: registration.id, result: null })}\n`);
      const request = await attachment;
      socket.write(`${JSON.stringify({ type: "response", id: request.id, result: responses.shift() })}\n`);
    });
  }));
  const count = responses.length;
  for (let index = 0; index < count; index++) {
    const terminal = await connectWorkspace(async () => STATE, () => {});
    await assert.rejects(terminal.request("attach", { name: "" }), /Invalid Workspace attachment response/);
  }
});

test("Attachment names count Unicode codepoints, and token changes between server generations", { timeout: 10_000 }, async (t) => {
  const { dir, servers } = await fixture(t);
  const name = "🚀".repeat(128);
  const first = await startWorkspaceServer(dir, async () => {}, async (_peer, requested) => { assert.equal(requested, name); return { ...ATTACHMENT, name }; }, () => {});
  servers.push(first);
  const token = await readFile(join(dir, "live.token"), "utf8");
  const terminal = await connectWorkspace(async () => STATE, () => {});
  assert.deepEqual(await terminal.request("attach", { name }), { ...ATTACHMENT, name });
  await assert.rejects(terminal.request("attach", { name: `${name}x` }), /128 codepoints/);
  terminal.close();
  await first.close();
  const second = await startWorkspaceServer(dir, async () => {}, async () => ATTACHMENT, () => {});
  servers.push(second);
  assert.notEqual(await readFile(join(dir, "live.token"), "utf8"), token);
});

test("State deadlines disconnect without retry, while prompts have no duration deadline", { timeout: 10_000 }, async (t) => {
  const { dir, servers } = await fixture(t);
  let daemonPeer: LocalPeer | undefined;
  let prompts = 0;
  let states = 0;
  const promptAdmitted = Promise.withResolvers<void>();
  const stateAdmitted = Promise.withResolvers<void>();
  const completion = Promise.withResolvers<unknown>();
  servers.push(await startWorkspaceServer(dir, async (peer) => { daemonPeer = peer; }, async () => ATTACHMENT, () => {}));
  const terminal = await connectWorkspace(async (method) => {
    if (method === "prompt") { prompts++; promptAdmitted.resolve(); }
    else { states++; stateAdmitted.resolve(); }
    return completion.promise;
  }, () => {});
  assert.ok(daemonPeer);
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const prompt = daemonPeer.request("prompt", { text: "a long real turn" });
  const rejectedPrompt = assert.rejects(prompt, /outcome may be unknown.*not retried/);
  let settled = false;
  void prompt.then(() => { settled = true; }, () => { settled = true; });
  await promptAdmitted.promise;
  t.mock.timers.tick(60_000);
  await Promise.resolve();
  assert.equal(settled, false, "Elapsed time alone cannot cancel or detach a prompt");
  const expired = assert.rejects(daemonPeer.request("state"), /state timed out.*outcome may be unknown.*not retried/);
  await stateAdmitted.promise;
  t.mock.timers.tick(10_001);
  await Promise.all([expired, rejectedPrompt]);
  assert.equal(prompts, 1);
  assert.equal(states, 1);
  completion.resolve("the local turn remains alive");
  terminal.close();
  t.mock.timers.reset();
});

test("Registration response is null and authenticated attaches reject overlap without preventing another attachment", { timeout: 10_000 }, async t => {
  const { dir, servers } = await fixture(t);
  const admitted = Promise.withResolvers<void>();
  const completion = Promise.withResolvers<Attachment>();
  let registered = 0;
  let attached = 0;
  servers.push(await startWorkspaceServer(dir, async () => { registered++; }, async () => {
    attached++;
    if (attached === 1) { admitted.resolve(); return completion.promise; }
    if (attached === 2) return { ...ATTACHMENT, unexpected: true };
    return ATTACHMENT;
  }, () => {}));
  const terminal = await connectWorkspace(async () => STATE, () => {});
  assert.equal(registered, 1);
  assert.equal(attached, 0);
  const first = terminal.request("attach", { name: "" });
  await admitted.promise;
  await assert.rejects(terminal.request("attach", { name: "" }), /already in progress/);
  assert.equal(attached, 1);
  completion.resolve(ATTACHMENT);
  assert.deepEqual(await first, ATTACHMENT);
  await assert.rejects(terminal.request("attach", { name: "" }), /Invalid Workspace attachment result/);
  assert.deepEqual(await terminal.request("attach", { name: "" }), ATTACHMENT);
  assert.equal(registered, 1);
  terminal.close();
});

test("Client rejects non-null registration results", { timeout: 10_000 }, async t => {
  const { dir, servers } = await fixture(t);
  servers.push(await fakeDaemon(dir, socket => {
    void readFrame(socket).then(request => {
      assert.equal(request.method, "register");
      assert.deepEqual(Object.keys(request.params as object), ["token"]);
      socket.write(`${JSON.stringify({ type: "response", id: request.id, result: ATTACHMENT })}\n`);
    });
  }));
  await assert.rejects(connectWorkspace(async () => STATE, () => {}), /Invalid Workspace registration response/);
});

test("Attached is admitted only after the registration wire response, including within the same socket chunk", { timeout: 10_000 }, async t => {
  const { dir, servers } = await fixture(t);
  const finished = Promise.withResolvers<void>();
  let attachments = 0;
  servers.push(await fakeDaemon(dir, socket => {
    void readFrame(socket).then(async registration => {
      const premature = readFrame(socket);
      socket.write(`${JSON.stringify({ type: "request", id: 1, method: "attached", params: ATTACHMENT })}\n`);
      assert.match(String((await premature).error), /registration has not completed/);
      const accepted = readFrame(socket);
      socket.write(`${JSON.stringify({ type: "response", id: registration.id, result: null })}\n${JSON.stringify({ type: "request", id: 2, method: "attached", params: ATTACHMENT })}\n`);
      assert.equal((await accepted).result, null);
      finished.resolve();
    }).catch(finished.reject);
  }));
  const terminal = await connectWorkspace(async (method, params) => {
    assert.equal(method, "attached");
    assert.deepEqual(params, ATTACHMENT);
    attachments++;
  }, () => {});
  await finished.promise;
  assert.equal(attachments, 1);
  terminal.close();
});

test("Register uses a ten-second control deadline and attach uses forty seconds", { timeout: 10_000 }, async t => {
  const { dir, servers } = await fixture(t);
  const registration = Promise.withResolvers<Record<string, unknown>>();
  servers.push(await fakeDaemon(dir, socket => { void readFrame(socket).then(registration.resolve); }));
  t.mock.timers.enable({ apis: ["setTimeout"] });
  t.after(() => t.mock.timers.reset());
  const connecting = connectWorkspace(async () => STATE, () => {});
  const rejectedRegistration = assert.rejects(connecting, /register timed out.*not retried/);
  await registration.promise;
  t.mock.timers.tick(9_999);
  await Promise.resolve();
  t.mock.timers.tick(1);
  await rejectedRegistration;
  await servers[0]!.close();
  const admitted = Promise.withResolvers<void>();
  const completion = Promise.withResolvers<Attachment>();
  servers.push(await startWorkspaceServer(dir, async () => {}, async () => { admitted.resolve(); return completion.promise; }, () => {}));
  const terminal = await connectWorkspace(async () => STATE, () => {});
  const attaching = terminal.request("attach", { name: "" });
  const rejectedAttach = assert.rejects(attaching, /attach timed out.*not retried/);
  let settled = false;
  void attaching.then(() => { settled = true; }, () => { settled = true; });
  await admitted.promise;
  t.mock.timers.tick(39_999);
  await Promise.resolve();
  assert.equal(settled, false);
  t.mock.timers.tick(1);
  await rejectedAttach;
  completion.resolve(ATTACHMENT);
  terminal.close();
});
