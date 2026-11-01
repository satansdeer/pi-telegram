/**
 * Standalone Workspace bridge regression tests.
 * Zones: forum/session isolation, owner admission, crash recovery, durable identity.
 */
import assert from "node:assert/strict";
import { copyFile, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { WorkspaceBridge } from "../omp/workspace.ts";
import type { Binding, SessionPort, UiRequest } from "../omp/workspace.ts";

const OWNER = 42;
const BOT = 99;
const GROUP = -100123;
type Sent = { chat_id: number; message_thread_id?: number; text: string };

class FakeSession implements SessionPort {
  readonly binding: Binding;
  readonly prompts: string[] = [];
  readonly answers: Array<{ id: string; response: Record<string, unknown> }> = [];
  aborts = 0;
  stopped = false;
  private finish?: (body: string) => void;
  private readonly admitted: (threadId: number) => void;

  constructor(binding: Binding, admitted: (threadId: number) => void) {
    this.binding = { ...binding, sessionId: binding.sessionId ?? `omp-${binding.id}`, sessionFile: binding.sessionFile ?? `/sessions/${binding.id}/transcript.jsonl` };
    this.admitted = admitted;
  }
  async state(): Promise<Record<string, unknown>> {
    return { sessionId: this.binding.sessionId, sessionFile: this.binding.sessionFile };
  }
  prompt(body: string): Promise<string> {
    this.prompts.push(body);
    const { promise, resolve } = Promise.withResolvers<string>();
    this.finish = resolve;
    this.admitted(this.binding.threadId);
    return promise;
  }
  complete(body: string): void {
    assert.ok(this.finish, "a real turn must be pending");
    const finish = this.finish;
    this.finish = undefined;
    finish(body);
  }
  async abort(): Promise<void> {
    this.aborts++;
    if (this.finish) this.complete(`aborted-${this.binding.threadId}`);
  }
  async answer(id: string, response: Record<string, unknown>): Promise<void> {
    this.answers.push({ id, response });
  }
  async stop(): Promise<void> {
    this.stopped = true;
    if (this.finish) this.complete(`stopped-${this.binding.threadId}`);
  }
}

class Harness {
  readonly bridge: WorkspaceBridge;
  readonly sent: Sent[] = [];
  readonly opened: Binding[] = [];
  readonly sessions = new Map<number, FakeSession>();
  readonly ui = new Map<number, (request: UiRequest) => void>();
  readonly created: Array<Record<string, unknown>> = [];
  private readonly listeners = new Set<() => void>();
  private updateId: number;
  private nextThread = 100;
  failSend?: (message: Sent) => boolean;
  constructor(statePath: string, offset = 0) {
    this.updateId = offset;
    this.bridge = new WorkspaceBridge({ statePath, cwd: "/workspace", ownerId: OWNER, botId: BOT, botUsername: "workspace_bot" }, async (method, params) => {
      if (method === "getChat") return { id: params.chat_id, type: "supergroup", is_forum: true };
      if (method === "getChatMember") return { user: { id: BOT }, status: "administrator", can_manage_topics: true };
      if (method === "createForumTopic") { this.created.push(params); return { message_thread_id: ++this.nextThread, name: params.name }; }
      assert.equal(method, "sendMessage");
      const sent = params as Sent;
      if (this.failSend?.(sent)) throw new Error("test transport unavailable");
      this.sent.push({ ...sent });
      for (const listener of this.listeners) listener();
      return { message_id: this.sent.length };
    }, async (binding, onUi) => {
      this.opened.push({ ...binding });
      this.ui.set(binding.threadId, onUi);
      const session = new FakeSession(binding, () => { for (const listener of this.listeners) listener(); });
      this.sessions.set(binding.threadId, session);
      return session;
    });
  }
  update(body: string, threadId?: number, ownerId = OWNER, groupId = GROUP): Record<string, unknown> {
    return { update_id: this.updateId++, message: { from: { id: ownerId }, chat: { id: groupId, type: "supergroup" }, text: body, ...(threadId === undefined ? {} : { message_thread_id: threadId, is_topic_message: true }) } };
  }
  async dispatch(body: string, threadId?: number, ownerId = OWNER, groupId = GROUP): Promise<void> {
    await this.bridge.handle(this.update(body, threadId, ownerId, groupId));
  }
  async until(predicate: () => boolean): Promise<void> {
    if (predicate()) return;
    const { promise, resolve } = Promise.withResolvers<void>();
    const check = () => {
      if (!predicate()) return;
      this.listeners.delete(check);
      resolve();
    };
    this.listeners.add(check);
    check();
    await promise;
  }
  async reply(contains: string, threadId?: number, after = 0): Promise<Sent> {
    await this.until(() => this.sent.slice(after).some((sent) => sent.message_thread_id === threadId && sent.text.includes(contains)));
    return this.sent.slice(after).find((sent) => sent.message_thread_id === threadId && sent.text.includes(contains))!;
  }
  async bindWorkspace(): Promise<void> {
    await this.bridge.init();
    await this.dispatch("/workspace@workspace_bot");
    await this.reply("Workspace bound");
  }
  async create(name = "Chat"): Promise<number> {
    const threadId = this.nextThread + 1;
    await this.dispatch(`/new ${name}`);
    await this.reply("Chat created", threadId);
    return threadId;
  }
}

test("Workspace isolates two topics, keeps controls responsive, and resumes only the exact designated transcript", { timeout: 15000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "omp-workspace-"));
  const statePath = join(directory, "state.json");
  const h = new Harness(statePath);
  try {
    await h.bindWorkspace();
    const alpha = await h.create("Alpha");
    const beta = await h.create("😀".repeat(200));
    assert.notEqual(alpha, beta);
    assert.equal([...String(h.created[1]!.name)].length, 128, "topic title stays within Telegram's codepoint limit");
    assert.match(String(h.created[0]!.name), /^\[[a-f0-9]{12}\] Alpha$/);
    assert.notEqual(h.opened[0]!.id, h.opened[1]!.id);
    assert.notEqual(h.opened[0]!.id, String(alpha));
    await h.dispatch("alpha input", alpha);
    await h.dispatch("beta input", beta);
    await h.until(() => h.sessions.get(alpha)!.prompts.length === 1 && h.sessions.get(beta)!.prompts.length === 1);
    await h.dispatch("must not queue", alpha);
    await h.reply("This topic is busy", alpha);
    await h.dispatch("/status", alpha);
    await h.reply("Busy", alpha);
    assert.deepEqual(h.sessions.get(alpha)!.prompts, ["alpha input"]);
    assert.deepEqual(h.sessions.get(beta)!.prompts, ["beta input"]);

    h.ui.get(alpha)!({ id: "approval", method: "confirm", title: "Allow operation?" });
    await h.reply("UI confirm request approval", alpha);
    await h.dispatch('/answer approval {"confirmed":true}', beta);
    await h.reply("No such pending UI request in this exact topic", beta);
    await h.dispatch('/answer approval {"confirmed":"yes"}', alpha);
    await h.reply("Invalid confirm answer", alpha);
    assert.equal(h.sessions.get(alpha)!.answers.length, 0);
    await h.dispatch('/answer approval {"confirmed":true}', alpha);
    await h.reply("UI request approval answered", alpha);
    assert.deepEqual(h.sessions.get(alpha)!.answers, [{ id: "approval", response: { confirmed: true } }]);
    assert.equal(h.sessions.get(beta)!.answers.length, 0);

    await h.dispatch("/abort", alpha);
    await h.reply("Abort requested", alpha);
    await h.reply(`aborted-${alpha}`, alpha);
    assert.equal(h.sessions.get(alpha)!.aborts, 1);
    assert.equal(h.sessions.get(beta)!.aborts, 0);
    const longReply = "beta:" + "😀".repeat(4500);
    h.sessions.get(beta)!.complete(longReply);
    await h.until(() => h.sent.filter((sent) => sent.message_thread_id === beta && (sent.text.startsWith("beta:") || sent.text.startsWith("😀"))).map((sent) => sent.text).join("") === longReply);
    assert.ok(h.sent.every((sent) => sent.chat_id === GROUP && sent.text.length <= 4096));
    assert.ok(!h.sent.some((sent) => sent.message_thread_id === alpha && sent.text.startsWith("beta:")));
    await h.bridge.stop();
    const stored = JSON.parse(await readFile(statePath, "utf8"));
    assert.equal((await stat(statePath)).mode & 0o777, 0o600);
    assert.equal((await stat(directory)).mode & 0o777, 0o700);
    assert.equal(stored.admissions.length, 0);
    const designated = stored.bindings.find((binding: Binding) => binding.threadId === alpha) as Binding;
    assert.ok(designated.sessionId && designated.sessionFile);

    const resumed = new Harness(statePath, stored.offset);
    await resumed.bridge.init();
    await resumed.dispatch("/bind do not replace", alpha);
    await resumed.reply("designated session will not be replaced", alpha);
    assert.equal(resumed.opened.length, 0);
    await resumed.dispatch("continued input", alpha);
    await resumed.until(() => resumed.sessions.get(alpha)?.prompts.length === 1);
    assert.deepEqual(resumed.opened, [designated], "restart supplies the exact stored session ID/file, not the latest session in a directory");
    resumed.sessions.get(alpha)!.complete("resumed alpha");
    await resumed.reply("resumed alpha", alpha);
    await resumed.bridge.stop();
  } finally {
    await h.bridge.stop();
    await rm(directory, { recursive: true, force: true });
  }
});

test("Workspace rejects unauthorized/unbound input, ignores other bot commands, and durably deduplicates updates", { timeout: 15000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "omp-workspace-admission-"));
  const h = new Harness(join(directory, "state.json"));
  try {
    await h.bridge.init();
    await h.dispatch("unbound input", 55);
    await h.reply("Workspace is unbound", 55);
    assert.equal(h.opened.length, 0);
    await h.dispatch("/workspace", undefined, OWNER + 1);
    await h.reply("Owner only");
    assert.equal(h.opened.length, 0);
    await h.dispatch("/workspace");
    await h.reply("Workspace bound");
    const createUpdate = h.update("/new One");
    await h.bridge.handle(createUpdate);
    await h.reply("Chat created", 101);
    await h.bridge.handle(createUpdate);
    assert.equal(h.created.length, 1);
    const unauthorized = h.update("intruder", 101, OWNER + 1);
    await h.bridge.handle(unauthorized);
    await h.reply("Owner only", 101);
    await h.bridge.handle(unauthorized);
    assert.equal(h.sent.filter((sent) => sent.message_thread_id === 101 && sent.text.includes("Owner only")).length, 1);
    assert.deepEqual(h.sessions.get(101)!.prompts, []);
    const prompt = h.update("one admitted turn", 101);
    await h.bridge.handle(prompt);
    await h.until(() => h.sessions.get(101)!.prompts.length === 1);
    await h.bridge.handle(prompt);
    const currentOffset = h.bridge.offset;
    await h.bridge.handle({ update_id: 0, message: { from: { id: OWNER }, chat: { id: GROUP }, text: "old input", message_thread_id: 101 } });
    assert.equal(h.bridge.offset, currentOffset);
    await h.dispatch("/new@other_bot ignored", 101);
    await h.dispatch("  /model unsafe", 101);
    await h.reply("Unsupported slash command", 101);
    assert.equal(h.created.length, 1);
    assert.deepEqual(h.sessions.get(101)!.prompts, ["one admitted turn"]);
    await h.bridge.handle({ update_id: currentOffset + 10, message: { from: { id: OWNER }, chat: { id: GROUP }, message_thread_id: 101, photo: [{ file_id: "unsupported" }] } });
    await h.reply("Attachments are unsupported", 101);
    h.sessions.get(101)!.complete("completed once");
    await h.reply("completed once", 101);
    await h.bridge.stop();
    const state = JSON.parse(await readFile(join(directory, "state.json"), "utf8"));
    assert.equal(state.offset, currentOffset + 11);
    assert.equal(state.admissions.length, 0);
  } finally {
    await h.bridge.stop();
    await rm(directory, { recursive: true, force: true });
  }
});

test("Workspace restart reports interrupted admission without replaying a prompt or topic creation", { timeout: 15000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "omp-workspace-crash-"));
  const recoveredDirectory = await mkdtemp(join(tmpdir(), "omp-workspace-recovered-"));
  const h = new Harness(join(directory, "state.json"));
  const recovered = new Harness(join(recoveredDirectory, "state.json"));
  try {
    await h.bindWorkspace();
    const threadId = await h.create();
    const update = h.update("do not replay", threadId);
    await h.bridge.handle(update);
    await h.until(() => h.sessions.get(threadId)!.prompts.length === 1);
    // Snapshot the durable crash boundary, then let the first in-memory process stop.
    await copyFile(join(directory, "state.json"), join(recoveredDirectory, "state.json"));
    await h.bridge.stop();
    await recovered.bridge.init();
    await recovered.reply("Its outcome may be ambiguous", threadId);
    assert.equal(recovered.opened.length, 0);
    assert.equal(recovered.created.length, 0);
    await recovered.bridge.handle(update);
    assert.equal(recovered.opened.length, 0);
    const stored = JSON.parse(await readFile(join(recoveredDirectory, "state.json"), "utf8"));
    assert.equal(stored.admissions.length, 0);
    assert.equal(stored.bindings[0].sessionFile, h.sessions.get(threadId)!.binding.sessionFile);
    await recovered.bridge.stop();
  } finally {
    await h.bridge.stop();
    await recovered.bridge.stop();
    await rm(directory, { recursive: true, force: true });
    await rm(recoveredDirectory, { recursive: true, force: true });
  }
});

test("Workspace corrupt, foreign, and numerically unsafe state fails closed without replacing the file", async () => {
  const directory = await mkdtemp(join(tmpdir(), "omp-workspace-corrupt-"));
  const statePath = join(directory, "state.json");
  const valid = { version: 1, ownerId: OWNER, botId: BOT, offset: 0, bindings: [], admissions: [] };
  try {
    for (const source of ["{not json", JSON.stringify({ ...valid, offset: "12" }), JSON.stringify({ ...valid, offset: Number.MAX_SAFE_INTEGER + 1 }), JSON.stringify({ ...valid, botId: BOT + 1 }), JSON.stringify({ ...valid, ownerId: OWNER + 1 }), JSON.stringify({ ...valid, groupId: -1.5 })]) {
      await writeFile(statePath, source, { mode: 0o600 });
      const h = new Harness(statePath);
      await assert.rejects(h.bridge.init(), /Malformed|Invalid|different bot or owner/);
      assert.equal(await readFile(statePath, "utf8"), source);
      assert.equal(h.created.length, 0);
      assert.equal(h.opened.length, 0);
      assert.equal(h.sent.length, 0);
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("Workspace keeps execution separate from failed reply delivery and never reruns it", { timeout: 15000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "omp-workspace-delivery-"));
  const statePath = join(directory, "state.json");
  const h = new Harness(statePath);
  let resumed: Harness | undefined;
  try {
    await h.bindWorkspace();
    const threadId = await h.create();
    const update = h.update("execute once", threadId);
    await h.bridge.handle(update);
    await h.until(() => h.sessions.get(threadId)!.prompts.length === 1);
    h.failSend = (message) => message.text === "reply could not be delivered";
    h.sessions.get(threadId)!.complete("reply could not be delivered");
    await h.bridge.stop();
    const stored = JSON.parse(await readFile(statePath, "utf8"));
    assert.equal(stored.admissions.length, 1);
    assert.equal(stored.admissions[0].phase, "report");
    assert.match(stored.admissions[0].diagnostic, /execution will not be repeated/);
    assert.deepEqual(h.sessions.get(threadId)!.prompts, ["execute once"]);
    resumed = new Harness(statePath, stored.offset);
    await resumed.bridge.init();
    await resumed.reply("execution will not be repeated", threadId);
    await resumed.bridge.handle(update);
    assert.equal(resumed.opened.length, 0);
    assert.equal(resumed.created.length, 0);
    await resumed.bridge.stop();
  } finally {
    await h.bridge.stop();
    await resumed?.bridge.stop();
    await rm(directory, { recursive: true, force: true });
  }
});

test("Workspace shutdown retains an explicit failure when a designated child may still be running", { timeout: 15000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "omp-workspace-stop-"));
  const h = new Harness(join(directory, "state.json"));
  try {
    await h.bindWorkspace();
    const thread = await h.create();
    h.sessions.get(thread)!.stop = async () => { throw new Error("child still draining"); };
    await assert.rejects(h.bridge.stop(), (error: unknown) => error instanceof AggregateError
      && /ownership must be retained/.test(error.message)
      && error.errors.some((cause) => cause instanceof Error && cause.message === "child still draining"));
  } finally {
    for (const session of h.sessions.values()) session.stop = FakeSession.prototype.stop.bind(session);
    await h.bridge.stop();
    await rm(directory, { recursive: true, force: true });
  }
});
