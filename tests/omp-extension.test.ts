/**
 * Exercises same-process Telegram attachment, terminal admission, and response ownership.
 * Zones: omp extension, terminal admission, live session attachment
 */
import assert from "node:assert/strict";
import test from "node:test";
import telegramExtension from "../omp/extension.ts";
import type { TerminalContext, TerminalHost } from "../omp/extension.ts";
import type { Attachment, LocalHandler, LocalPeer } from "../omp/local.ts";

type Link = {
  handler: LocalHandler; onClose(): void; peer: LocalPeer; closes: number; attachment: Attachment;
  requests: { method: string; params: Record<string, unknown> }[];
};

function harness(options: { deferredConnect?: boolean; subagent?: boolean; unavailable?: boolean } = {}) {
  const listeners = new Map<string, (event: unknown, ctx: TerminalContext) => unknown>();
  const commands = new Map<string, (args: string, ctx: TerminalContext) => Promise<void>>();
  const notifications: { message: string; type?: string }[] = [];
  const sent: string[] = [];
  const links: Link[] = [];
  const deferred = Promise.withResolvers<void>();
  const identity = { sessionId: "terminal-session", sessionFile: "/sessions/terminal.jsonl", cwd: "/project" };
  let idle = true;
  let queued = false;
  let aborts = 0;
  let sendFailure: Error | undefined;
  let unavailable = !!options.unavailable;
  let attempts = 0;
  const ctx: TerminalContext = {
    get cwd() { return identity.cwd; },
    agent: { kind: options.subagent ? "sub" : "main" },
    sessionManager: { getSessionId: () => identity.sessionId, getSessionFile: () => identity.sessionFile },
    isIdle: () => idle,
    hasPendingMessages: () => queued,
    abort() { aborts++; },
    ui: { notify(message, type) { notifications.push({ message, type }); } },
  };
  const host: TerminalHost = {
    registerCommand(name, command) { commands.set(name, command.handler); },
    sendUserMessage(text) { if (sendFailure) throw sendFailure; sent.push(text); },
    on(event, callback) { listeners.set(event, callback as (event: unknown, ctx: TerminalContext) => unknown); },
  };
  telegramExtension(host, async (handler, onClose) => {
    attempts++;
    if (unavailable) throw new Error("Workspace daemon is not running; start it before /telegram-connect");
    const link: Link = {
      handler, onClose, closes: 0, requests: [],
      attachment: { ...identity, id: "external-terminal", name: "terminal", threadId: 42, groupId: -1001 },
      peer: {
        async request(method, params = {}) {
          link.requests.push({ method, params });
          assert.equal(method, "attach");
          link.attachment.name = String(params.name || "terminal");
          await handler("attached", { ...link.attachment });
          return link.attachment;
        },
        close() { link.closes++; onClose(); },
      },
    };
    links.push(link);
    const state = await handler("state", {}) as Record<string, unknown>;
    assert.deepEqual({ sessionId: state.sessionId, sessionFile: state.sessionFile, cwd: state.cwd },
      { sessionId: link.attachment.sessionId, sessionFile: link.attachment.sessionFile, cwd: link.attachment.cwd });
    assert.equal(typeof state.isStreaming, "boolean");
    if (options.deferredConnect) await deferred.promise;
    return link.peer;
  });
  function emit(event: string, data: Record<string, unknown> = {}) {
    return listeners.get(event)?.({ type: event, ...data }, ctx);
  }
  return {
    identity, notifications, sent, links, ctx,
    get aborts() { return aborts; },
    get attempts() { return attempts; },
    setUnavailable(value: boolean) { unavailable = value; },
    setIdle(value: boolean) { idle = value; },
    setQueued(value: boolean) { queued = value; },
    failSend(error?: Error) { sendFailure = error; },
    command(name: string, args = "") { return commands.get(name)!(args, ctx); },
    remote(method: string, params: Record<string, unknown> = {}, link = links.at(-1)!) { return link.handler(method, params); },
    start() { idle = false; emit("agent_start"); },
    message(text: string, stopReason = "stop", errorMessage?: string) {
      emit("turn_end", { turnIndex: 0, toolResults: [], message: { role: "assistant", content: [{ type: "text", text }], stopReason, errorMessage } });
    },
    end(messages: unknown[] = [], willContinue = false) {
      idle = !willContinue;
      emit("agent_end", { messages, willContinue });
    },
    emit,
    finishConnect() { deferred.resolve(); },
    async flush() { for (let step = 0; step < 8; step++) await Promise.resolve(); },
  };
}

test("terminal attachment is idempotent, reserves admission before dispatch, and keeps local controls usable", async () => {
  const terminal = harness();
  await terminal.command("telegram-connect", "work");
  const state = await terminal.remote("state");
  assert.deepEqual(state, { ...terminal.identity, isStreaming: false });
  await terminal.command("telegram-connect", "different name");
  assert.equal(terminal.links.length, 1);
  assert.match(terminal.notifications.at(-1)!.message, /already connected: work \(topic 42\)/);

  terminal.setIdle(false);
  await assert.rejects(terminal.remote("prompt", { text: "busy terminal" }), /busy/);
  terminal.setIdle(true);
  terminal.setQueued(true);
  await assert.rejects(terminal.remote("prompt", { text: "queued terminal" }), /busy/);
  terminal.setQueued(false);
  const response = terminal.remote("prompt", { text: "remote task" });
  assert.deepEqual(terminal.sent, ["remote task"]);
  assert.equal((await terminal.remote("state") as { isStreaming: boolean }).isStreaming, true);
  await assert.rejects(terminal.remote("prompt", { text: "another remote task" }), /busy/);
  assert.deepEqual(terminal.emit("input", { text: "local task", source: "interactive" }), { handled: true });
  assert.deepEqual(terminal.emit("input", { text: "concurrent RPC task", source: "rpc" }), { handled: true });
  assert.match(terminal.notifications.at(-1)!.message, /telegram-disconnect.*\/abort/);
  assert.equal(terminal.emit("input", { text: "/abort", source: "interactive" }), undefined);
  assert.equal(terminal.emit("input", { text: "/telegram-disconnect", source: "interactive" }), undefined);
  assert.equal(terminal.emit("input", { text: "remote task", source: "extension" }), undefined);
  terminal.start();
  terminal.message("remote answer");
  terminal.end();
  assert.equal(await response, "remote answer");
  assert.equal(terminal.emit("input", { text: "local task", source: "interactive" }), undefined);
});

test("only newly observed assistant output belongs to a remote turn and asynchronous continuation does not settle early", async () => {
  const terminal = harness();
  await terminal.command("telegram-connect");
  const old = { role: "assistant", content: [{ type: "text", text: "prior terminal reply" }], stopReason: "stop" };
  terminal.emit("turn_end", { message: old });
  const response = terminal.remote("prompt", { text: "new task" });
  let settled = false;
  void response.then(() => { settled = true; }, () => { settled = true; });
  terminal.end([old]);
  await Promise.resolve();
  assert.equal(settled, false, "a late terminal settle before the remote agent_start cannot finish the reservation");
  terminal.start();
  terminal.message("draft");
  terminal.end([old], true);
  await Promise.resolve();
  assert.equal(settled, false);
  await assert.rejects(terminal.remote("prompt", { text: "too soon" }), /busy/);
  terminal.start();
  terminal.message("final result");
  terminal.end([old]);
  assert.equal(await response, "final result");

  const empty = terminal.remote("prompt", { text: "empty result" });
  terminal.start();
  terminal.end([old]);
  assert.equal(await empty, "", "full history must never supply an earlier response");
});

test("assistant errors and aborts reject instead of returning a misleading successful response", async () => {
  const terminal = harness();
  await terminal.command("telegram-connect");
  const failed = terminal.remote("prompt", { text: "fail" });
  const failedCheck = assert.rejects(failed, /provider unavailable/);
  terminal.start();
  terminal.message("not success", "error", "provider unavailable");
  terminal.end();
  await failedCheck;

  const aborted = terminal.remote("prompt", { text: "abort me" });
  const abortedCheck = assert.rejects(aborted, /aborted/);
  terminal.start();
  await terminal.remote("abort");
  assert.equal(terminal.aborts, 1);
  terminal.message("partial response", "aborted");
  terminal.end();
  await abortedCheck;
  await assert.rejects(terminal.remote("answer", { value: "yes" }), /local OMP terminal/);
  await assert.rejects(terminal.remote("state", { extra: true }), /Unexpected/);
  terminal.failSend(new Error("dispatch failed"));
  await assert.rejects(terminal.remote("prompt", { text: "dispatch failure" }), /dispatch failed/);
  terminal.failSend();
  assert.equal((await terminal.remote("state") as { isStreaming: boolean }).isStreaming, false);
});

test("disconnect rejects the remote waiter without aborting and holds admission until the terminal actually settles", async () => {
  const terminal = harness();
  await terminal.command("telegram-connect");
  const original = terminal.links[0]!;
  const response = terminal.remote("prompt", { text: "keep running" });
  const rejected = assert.rejects(response, /disconnected/);
  await terminal.command("telegram-disconnect");
  await rejected;
  assert.equal(original.closes, 1);
  assert.equal(terminal.aborts, 0);
  assert.deepEqual(terminal.emit("input", { text: "do not mix", source: "interactive" }), { handled: true });
  await terminal.command("telegram-connect");
  original.onClose();
  assert.equal(terminal.links.at(-1)!.closes, 0, "a late close callback must not detach the new connection");
  await assert.rejects(terminal.remote("prompt", { text: "too early" }), /busy/);
  terminal.start();
  terminal.message("the original operation still ran");
  terminal.end();
  assert.equal(terminal.emit("input", { text: "terminal usable", source: "interactive" }), undefined);
  const next = terminal.remote("prompt", { text: "next operation" });
  terminal.start();
  terminal.message("next response");
  terminal.end();
  assert.equal(await next, "next response");
  assert.equal(terminal.aborts, 0);
});

test("identity changes fence old requests and responses while lifecycle transitions detach without aborting", async () => {
  const terminal = harness();
  await terminal.command("telegram-connect");
  const oldLink = terminal.links[0]!;
  const response = terminal.remote("prompt", { text: "old identity" });
  const rejected = assert.rejects(response, /identity changed/);
  terminal.identity.sessionId = "new-session";
  terminal.identity.sessionFile = "/sessions/new.jsonl";
  await assert.rejects(terminal.remote("abort"), /closed/);
  await rejected;
  assert.equal(terminal.aborts, 0, "an old attachment cannot abort the new identity");
  await terminal.command("telegram-connect", "new identity");
  await assert.rejects(terminal.remote("prompt", { text: "wait for old settle" }), /busy/);
  terminal.start();
  terminal.message("old output must not leak");
  terminal.end();
  await assert.rejects(terminal.remote("state", {}, oldLink), /closed/);
  const fresh = terminal.remote("prompt", { text: "fresh task" });
  terminal.start();
  terminal.message("fresh output");
  terminal.end();
  assert.equal(await fresh, "fresh output");
  for (const event of ["session_before_switch", "session_before_branch", "session_before_tree", "session_shutdown"]) {
    if (event !== "session_before_switch") await terminal.command("telegram-connect");
    const link = terminal.links.at(-1)!;
    terminal.emit(event);
    assert.equal(link.closes, 1);
    await assert.rejects(terminal.remote("state", {}, link), /closed/);
  }
  assert.equal(terminal.aborts, 0);
});

test("in-flight connection callbacks are fenced and daemon loss leaves the terminal running", async () => {
  const terminal = harness({ deferredConnect: true });
  const connecting = terminal.command("telegram-connect");
  const duplicate = terminal.command("telegram-connect");
  assert.equal(terminal.links.length, 1);
  await terminal.command("telegram-disconnect");
  terminal.finishConnect();
  await Promise.all([connecting, duplicate]);
  assert.equal(terminal.links[0]!.closes, 1);
  assert.equal(terminal.notifications.some(item => item.message.startsWith("Telegram connected:")), false);
  await terminal.command("telegram-connect");
  const response = terminal.remote("prompt", { text: "daemon disappears" });
  const rejected = assert.rejects(response, /connection closed/);
  terminal.start();
  terminal.links.at(-1)!.onClose();
  await rejected;
  assert.equal(terminal.aborts, 0);
  terminal.message("terminal continues");
  terminal.end();
  assert.equal(terminal.emit("input", { text: "local work", source: "interactive" }), undefined);
  assert.match(terminal.notifications.at(-1)!.message, /terminal is still running/);

  const subagent = harness({ subagent: true });
  await subagent.command("telegram-connect");
  assert.equal(subagent.links.length, 0);
  assert.match(subagent.notifications.at(-1)!.message, /main OMP terminal/);
  terminal.emit("session_shutdown");
});

test("session startup registers silently without a topic and remote attachment authorizes same-process prompts", async () => {
  const terminal = harness();
  terminal.emit("session_start");
  await terminal.flush();
  assert.equal(terminal.links.length, 1);
  assert.deepEqual(terminal.links[0]!.requests, [], "Registration must not send attach");
  assert.equal(terminal.notifications.length, 0);
  assert.deepEqual(terminal.sent, []);
  await assert.rejects(terminal.remote("prompt", { text: "not authorized" }), /not attached/);
  await assert.rejects(terminal.remote("abort"), /not attached/);
  assert.equal(terminal.aborts, 0);
  const attachment = terminal.links[0]!.attachment;
  for (const invalid of [
    { ...attachment, extra: true }, { ...attachment, sessionId: "" },
    { ...attachment, sessionFile: "relative" }, { ...attachment, threadId: 1 },
    { ...attachment, groupId: 1 }, { ...attachment, name: "x".repeat(129) },
  ]) await assert.rejects(terminal.remote("attached", invalid), /Invalid Workspace attachment/);
  await assert.rejects(terminal.remote("attached", { ...attachment, sessionId: "another" }), /different OMP session identity/);
  await assert.rejects(terminal.remote("prompt", { text: "still not authorized" }), /not attached/);
  await terminal.remote("attached", { ...attachment });
  assert.match(terminal.notifications.at(-1)!.message, /Telegram connected/);
  const response = terminal.remote("prompt", { text: "same live process" });
  terminal.start();
  terminal.message("current process response");
  terminal.end();
  assert.equal(await response, "current process response");
  assert.deepEqual(terminal.sent, ["same live process"]);
  terminal.emit("session_shutdown");
});

test("an attach response alone cannot authorize prompts or aborts", async () => {
  const terminal = harness();
  terminal.emit("session_start");
  await terminal.flush();
  const link = terminal.links[0]!;
  link.peer.request = async () => ({ ...link.attachment });
  await terminal.command("telegram-connect");
  assert.match(terminal.notifications.at(-1)!.message, /did not authorize/);
  await assert.rejects(terminal.remote("prompt", { text: "not authorized" }), /not attached/);
  await assert.rejects(terminal.remote("abort"), /not attached/);
  assert.deepEqual(terminal.sent, []);
  assert.equal(terminal.aborts, 0);
  terminal.emit("session_shutdown");
});

test("remote attachment after the wire handshake can precede registration promise completion", async () => {
  const terminal = harness({ deferredConnect: true });
  terminal.emit("session_start");
  await terminal.flush();
  const link = terminal.links[0]!;
  await terminal.remote("attached", { ...link.attachment });
  terminal.finishConnect();
  await terminal.flush();
  assert.deepEqual(link.requests, []);
  const response = terminal.remote("prompt", { text: "early attachment" });
  terminal.start();
  terminal.message("same terminal");
  terminal.end();
  assert.equal(await response, "same terminal");
  assert.equal(terminal.links.length, 1);
  terminal.emit("session_shutdown");
});

test("daemon absence and loss retry only registration with bounded silent delay, and optout clears retries", async t => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const terminal = harness({ unavailable: true });
  t.after(() => { terminal.emit("session_shutdown"); t.mock.timers.reset(); });
  terminal.emit("session_start");
  await terminal.flush();
  assert.equal(terminal.attempts, 1);
  assert.deepEqual(terminal.notifications, []);
  t.mock.timers.tick(999);
  await terminal.flush();
  assert.equal(terminal.attempts, 1);
  t.mock.timers.tick(1);
  await terminal.flush();
  assert.equal(terminal.attempts, 2);
  for (let retry = 0; retry < 8; retry++) {
    t.mock.timers.tick(30_000);
    await terminal.flush();
  }
  const attempts = terminal.attempts;
  terminal.setUnavailable(false);
  t.mock.timers.tick(30_000);
  await terminal.flush();
  assert.equal(terminal.attempts, attempts + 1, "Retry delay remains bounded at thirty seconds");
  assert.deepEqual(terminal.links.at(-1)!.requests, []);
  assert.deepEqual(terminal.notifications, []);
  await terminal.command("telegram-connect", "recover");
  const old = terminal.links.at(-1)!;
  assert.equal(old.requests.length, 1);
  old.onClose();
  t.mock.timers.tick(1_000);
  await terminal.flush();
  const rediscovered = terminal.links.at(-1)!;
  assert.notEqual(rediscovered, old);
  assert.deepEqual(rediscovered.requests, [], "Daemon restart must never replay an attach");
  assert.deepEqual(terminal.sent, []);
  await assert.rejects(terminal.remote("prompt", { text: "requires fresh attach" }), /not attached/);
  await terminal.command("telegram-disconnect");
  const optedOutAttempts = terminal.attempts;
  t.mock.timers.tick(300_000);
  terminal.emit("agent_start");
  terminal.end();
  await terminal.flush();
  assert.equal(terminal.attempts, optedOutAttempts);
  await terminal.command("telegram-connect");
  assert.equal(terminal.attempts, optedOutAttempts + 1, "Explicit connect opts back in");
  terminal.links.at(-1)!.onClose();
  terminal.emit("session_shutdown");
  const stoppedAttempts = terminal.attempts;
  t.mock.timers.tick(300_000);
  await terminal.flush();
  assert.equal(terminal.attempts, stoppedAttempts);
});

test("native lifecycle after-events rediscover new identities and fence unfinished old registration", async () => {
  const terminal = harness({ deferredConnect: true });
  terminal.emit("session_start");
  const old = terminal.links[0]!;
  terminal.emit("session_before_switch");
  terminal.identity.sessionId = "resumed";
  terminal.identity.sessionFile = "/sessions/resumed.jsonl";
  terminal.emit("session_switch", { reason: "resume" });
  assert.equal(terminal.links.length, 2);
  await assert.rejects(terminal.remote("state", {}, old), /closed/);
  terminal.finishConnect();
  await terminal.flush();
  assert.equal(old.closes, 1);
  assert.deepEqual(terminal.links.at(-1)!.requests, []);
  assert.deepEqual(await terminal.remote("state"), { ...terminal.identity, isStreaming: false });
  for (const [before, after] of [
    ["session_before_branch", "session_branch"],
    ["session_before_tree", "session_tree"],
  ]) {
    await terminal.command("telegram-disconnect");
    const count: number = terminal.links.length;
    terminal.emit(before!);
    assert.equal(terminal.links.length, count);
    terminal.identity.sessionId += "-next";
    terminal.identity.sessionFile += ".next";
    terminal.emit(after!);
    await terminal.flush();
    assert.equal(terminal.links.length, count + 1, "New lifecycle resets discovery optout");
    assert.deepEqual(terminal.links.at(-1)!.requests, []);
  }
  assert.equal(terminal.aborts, 0);
  terminal.emit("session_shutdown");
});

test("new unsaved sessions wait silently for persistence, while native subagents and managed children never discover", async t => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const terminal = harness();
  const subagent = harness({ subagent: true });
  const managed = harness();
  const previous = process.env.OMP_WORKSPACE_MANAGED;
  t.after(() => {
    terminal.emit("session_shutdown");
    subagent.emit("session_shutdown");
    managed.emit("session_shutdown");
    if (previous === undefined) delete process.env.OMP_WORKSPACE_MANAGED;
    else process.env.OMP_WORKSPACE_MANAGED = previous;
    t.mock.timers.reset();
  });
  terminal.identity.sessionFile = "";
  terminal.emit("session_start");
  await terminal.flush();
  assert.equal(terminal.attempts, 0);
  assert.deepEqual(terminal.notifications, []);
  terminal.identity.sessionFile = "/sessions/persisted.jsonl";
  t.mock.timers.tick(1_000);
  await terminal.flush();
  assert.equal(terminal.attempts, 1);
  assert.deepEqual(terminal.links[0]!.requests, []);
  subagent.emit("session_start");
  subagent.emit("session_switch");
  t.mock.timers.tick(300_000);
  await subagent.flush();
  assert.equal(subagent.attempts, 0);
  assert.deepEqual(subagent.notifications, []);
  process.env.OMP_WORKSPACE_MANAGED = "1";
  managed.emit("session_start");
  managed.emit("session_switch");
  t.mock.timers.tick(300_000);
  await managed.flush();
  assert.equal(managed.attempts, 0);
  assert.equal(managed.notifications.length, 0);
  await managed.command("telegram-connect");
  assert.equal(managed.attempts, 0);
  assert.match(managed.notifications.at(-1)!.message, /Workspace-managed/);
});
