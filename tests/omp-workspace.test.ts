/**
 * Standalone Workspace bridge regression tests.
 * Zones: forum/session isolation, owner admission, crash recovery, durable identity.
 */
import assert from "node:assert/strict";
import { copyFile, mkdir, mkdtemp, readFile, realpath, rm, stat, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { scheduler } from "node:timers/promises";
import { test } from "node:test";
import { WorkspaceBridge } from "../omp/workspace.ts";
import type { Binding, SessionPort, UiRequest } from "../omp/workspace.ts";

const OWNER = 42;
const BOT = 99;
const GROUP = -100123;
type Sent = { chat_id: number; message_thread_id?: number; text: string; parse_mode?: "HTML" };
type Action = { chat_id: number; message_thread_id?: number; action: "typing" };

class FakeSession implements SessionPort {
  readonly binding: Binding;
  readonly prompts: string[] = [];
  readonly btws: string[] = [];
  readonly steers: string[] = [];
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
  async steer(body: string): Promise<void> { this.steers.push(body); }
  async btw(body: string): Promise<string> {
    this.btws.push(body);
    return `sidecar: ${body}`;
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

class TerminalSession implements SessionPort {
  readonly identity: { sessionId: string; sessionFile: string; cwd: string };
  readonly prompts: string[] = [];
  readonly steers: string[] = [];
  readonly attached: Array<Binding & { groupId: number }> = [];
  streaming = false;
  aborts = 0;
  stopped = false;
  beforeState?: () => Promise<void>;
  beforeAttach?: () => Promise<void>;
  constructor(sessionId: string, cwd = "/terminal/project", sessionFile = `/terminal/${sessionId}.jsonl`) {
    this.identity = { sessionId, cwd, sessionFile };
  }
  async state(): Promise<Record<string, unknown>> {
    const hook = this.beforeState;
    this.beforeState = undefined;
    await hook?.();
    return { ...this.identity, isStreaming: this.streaming };
  }
  readonly onAttach = async (binding: Binding & { groupId: number }): Promise<void> => {
    this.attached.push(binding);
    await this.beforeAttach?.();
  };
  async prompt(body: string): Promise<string> {
    this.prompts.push(body);
    return `${this.identity.sessionId} response`;
  }
  async steer(body: string): Promise<void> { this.steers.push(body); }
  async btw(body: string): Promise<string> { return `sidecar: ${body}`; }
  async abort(): Promise<void> { this.aborts++; }
  async answer(): Promise<void> { throw new Error("Use the terminal for interactive dialogs"); }
  async stop(): Promise<void> { this.stopped = true; }
}

class Harness {
  readonly bridge: WorkspaceBridge;
  readonly sent: Sent[] = [];
  readonly actions: Action[] = [];
  readonly opened: Binding[] = [];
  readonly sessions = new Map<number, FakeSession>();
  readonly ui = new Map<number, (request: UiRequest) => void>();
  readonly created: Array<Record<string, unknown>> = [];
  readonly downloads: Array<{ fileId: string; fileName: string; directory: string; path: string }> = [];
  private readonly listeners = new Set<() => void>();
  private updateId: number;
  private nextThread = 100;
  failSend?: (message: Sent) => boolean;
  beforeCreateTopic?: () => Promise<void>;
  constructor(statePath: string, offset = 0, cwd = "/workspace") {
    this.updateId = offset;
    this.bridge = new WorkspaceBridge({ statePath, cwd, ownerId: OWNER, botId: BOT, botUsername: "workspace_bot" }, async (method, params) => {
      if (method === "getChat") return { id: params.chat_id, type: "supergroup", is_forum: true };
      if (method === "getChatMember") return { user: { id: BOT }, status: "administrator", can_manage_topics: true };
      if (method === "createForumTopic") {
        this.created.push(params);
        await this.beforeCreateTopic?.();
        return { message_thread_id: ++this.nextThread, name: params.name };
      }
      if (method === "sendChatAction") {
        this.actions.push({ ...(params as Action) });
        for (const listener of this.listeners) listener();
        return true;
      }
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
    }, async (fileId, fileName, directory) => {
      const path = join(directory, fileName);
      this.downloads.push({ fileId, fileName, directory, path });
      return path;
    });
  }
  incoming(fields: Record<string, unknown>, threadId?: number, ownerId = OWNER, groupId = GROUP): Record<string, unknown> {
    const updateId = this.updateId++;
    return {
      update_id: updateId,
      message: {
        message_id: updateId + 1,
        from: { id: ownerId },
        chat: { id: groupId, type: "supergroup" },
        ...(threadId === undefined ? {} : { message_thread_id: threadId, is_topic_message: true }),
        ...fields,
      },
    };
  }
  update(body: string, threadId?: number, ownerId = OWNER, groupId = GROUP): Record<string, unknown> {
    return this.incoming({ text: body }, threadId, ownerId, groupId);
  }
  async dispatch(body: string, threadId?: number, ownerId = OWNER, groupId = GROUP): Promise<void> {
    await this.bridge.handle(this.update(body, threadId, ownerId, groupId));
  }
  async command(body: string, contains: string, threadId?: number, ownerId = OWNER, groupId = GROUP): Promise<Sent> {
    const after = this.sent.length;
    await this.dispatch(body, threadId, ownerId, groupId);
    return this.reply(contains, threadId, after);
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
    await h.dispatch("change direction", alpha);
    await h.reply("Steering applied", alpha);
    assert.deepEqual(h.sessions.get(alpha)!.steers, ["change direction"]);
    await h.dispatch("/status", alpha);
    await h.reply("Busy", alpha);
    assert.deepEqual(h.sessions.get(alpha)!.prompts, ["alpha input"]);
    assert.deepEqual(h.sessions.get(beta)!.prompts, ["beta input"]);
    const sidecar = await h.command("/btw status <now> & later", "Sidecar", alpha);
    assert.deepEqual(h.sessions.get(alpha)!.btws, ["status <now> & later"]);
    assert.deepEqual(h.sessions.get(alpha)!.prompts, ["alpha input"], "side questions must stay out of the main transcript");
    assert.equal(sidecar.parse_mode, "HTML");
    assert.equal(sidecar.text, "💭 <b>Sidecar</b>\n<blockquote>sidecar: status &lt;now&gt; &amp; later</blockquote>");

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

test("Workspace refreshes exact-topic typing only while the model is working", async (t) => {
  t.mock.timers.enable({ apis: ["setInterval"] });
  const directory = await mkdtemp(join(tmpdir(), "omp-workspace-typing-"));
  const h = new Harness(join(directory, "state.json"));
  try {
    await h.bindWorkspace();
    const threadId = await h.create("Typing");
    await h.dispatch("long prompt", threadId);
    await h.until(() => h.actions.length === 1);
    assert.deepEqual(h.actions[0], { chat_id: GROUP, message_thread_id: threadId, action: "typing" });

    t.mock.timers.tick(3000);
    await scheduler.yield();
    assert.equal(h.actions.length, 2, "typing is refreshed before Telegram expires it");

    h.sessions.get(threadId)!.complete("finished");
    await h.reply("finished", threadId);
    t.mock.timers.tick(3000);
    await scheduler.yield();
    assert.equal(h.actions.length, 2, "typing stops when model work completes");
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
    await h.bridge.handle({ update_id: currentOffset + 10, message: { from: { id: OWNER }, chat: { id: GROUP }, message_thread_id: 101, video_note: { file_id: "unsupported" } } });
    await h.reply("This attachment type is unsupported", 101);
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

test("Workspace archives supported attachments and recovers files from replied-to messages", { timeout: 15000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "omp-workspace-attachments-"));
  const h = new Harness(join(directory, "state.json"));
  try {
    await h.bindWorkspace();
    const threadId = await h.create("Files");
    await h.bridge.handle(h.incoming({
      caption: "Review this file",
      document: { file_id: "new-file", file_name: "notes.txt", mime_type: "text/plain" },
    }, threadId));
    await h.until(() => h.sessions.get(threadId)?.prompts.length === 1);
    const first = h.sessions.get(threadId)!.prompts[0]!;
    assert.match(first, /^Review this file\n\n\[attachments\] /);
    assert.match(first, /\/notes\.txt\n\n\[chat-attachments\] /);
    assert.equal(h.downloads[0]!.fileId, "new-file");
    assert.match(h.downloads[0]!.directory, new RegExp(`/attachments/${GROUP}/${threadId}/\\d+$`));
    await h.bridge.handle(h.incoming({
      text: "Inspect the older upload",
      reply_to_message: {
        message_id: 77,
        caption: "Earlier report",
        document: { file_id: "old-file", file_name: "report.pdf", mime_type: "application/pdf" },
      },
    }, threadId));
    await h.reply("Steering applied", threadId);
    const recovered = h.sessions.get(threadId)!.steers[0]!;
    assert.match(recovered, /^Inspect the older upload\n\n\[reply\] Earlier report/);
    assert.match(recovered, /\[attachments\] .+\/77\n- \/report\.pdf/);
    assert.match(recovered, /\[chat-attachments\] /);
    assert.deepEqual(h.downloads.map(({ fileId }) => fileId), ["new-file", "old-file"]);
    assert.match(h.downloads[1]!.directory, /\/77$/);
    h.sessions.get(threadId)!.complete("both files reviewed");
    await h.reply("both files reviewed", threadId);
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
    // Preserve the durable crash boundary, then unblock this fixture without mutating the copied snapshot.
    await copyFile(join(directory, "state.json"), join(recoveredDirectory, "state.json"));
    await h.sessions.get(threadId)!.abort();
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

test("Workspace shutdown drains an accepted turn before stopping its session", { timeout: 15000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "omp-workspace-drain-"));
  const statePath = join(directory, "state.json");
  const h = new Harness(statePath);
  let session: FakeSession | undefined;
  try {
    await h.bindWorkspace();
    const thread = await h.create();
    await h.dispatch("finish before restart", thread);
    await h.until(() => h.sessions.get(thread)?.prompts.length === 1);
    session = h.sessions.get(thread)!;

    const stopping = h.bridge.stop();
    await scheduler.yield();
    assert.equal(session.stopped, false, "shutdown must not stop a session with an accepted turn");

    session.complete("finished before restart");
    await stopping;
    assert.equal(session.stopped, true);
    assert.ok(h.sent.some((sent) => sent.message_thread_id === thread && sent.text === "finished before restart"));
    assert.equal(JSON.parse(await readFile(statePath, "utf8")).admissions.length, 0);
  } finally {
    if (session && !session.stopped) await session.abort();
    await h.bridge.stop();
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

test("Workspace validates project directories before creating topics and persists canonical per-chat directories", { timeout: 15000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "omp-workspace-cwd-"));
  const statePath = join(directory, "state.json");
  const project = join(directory, "project with spaces");
  await mkdir(project);
  await symlink(project, join(directory, "alias"));
  const canonical = await realpath(project);
  const h = new Harness(statePath, 0, directory);
  try {
    await h.bindWorkspace();
    const relative = await h.create("--cwd project with spaces");
    const absolute = await h.create(`Named work --cwd ${join(directory, "alias")}`);
    const created = h.created.length;
    const opened = h.opened.length;
    for (const argument of ["--cwd", "--cwd missing-project", "--cwd state.json"]) {
      const replies = h.sent.length;
      await h.dispatch(`/new ${argument}`);
      await h.until(() => h.sent.length > replies);
      assert.equal(h.created.length, created, "invalid directories must not leave orphan Telegram topics");
      assert.equal(h.opened.length, opened, "invalid directories must not start OMP");
    }
    await h.bridge.stop();
    const stored = JSON.parse(await readFile(statePath, "utf8"));
    assert.deepEqual(stored.bindings.map((binding: Binding) => ({
      threadId: binding.threadId, cwd: binding.cwd, name: binding.name,
    })), [
      { threadId: relative, cwd: canonical, name: "project with spaces" },
      { threadId: absolute, cwd: canonical, name: "Named work" },
    ]);
  } finally {
    await h.bridge.stop();
    await rm(directory, { recursive: true, force: true });
  }
});

test("Terminal bindings route to the live port and remain offline after disconnect or daemon restart", { timeout: 15000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "omp-workspace-terminal-"));
  const statePath = join(directory, "state.json");
  const h = new Harness(statePath);
  let restarted: Harness | undefined;
  const identity = { sessionId: "terminal-session", sessionFile: "/terminal/session.jsonl", cwd: "/terminal/project" };
  const prompts: string[] = [];
  let aborts = 0;
  const port: SessionPort = {
    async state() { return identity; },
    async prompt(text) { prompts.push(text); return "same live terminal response"; },
    async steer(text) { prompts.push(`steer:${text}`); },
    async btw(text) { return `sidecar: ${text}`; },
    async abort() { aborts++; },
    async answer() { throw new Error("Use the terminal for interactive dialogs"); },
    async stop() {},
  };
  try {
    await h.bindWorkspace();
    await h.bridge.register(port, async () => {});
    const attached = await h.bridge.attach(port, "Terminal project");
    assert.equal(attached.external, true);
    assert.equal(attached.sessionId, identity.sessionId);
    assert.equal((await h.bridge.attach(port, "Duplicate")).id, attached.id);
    assert.equal(h.created.length, 1, "duplicate live attachments must not create orphan topics");
    await h.dispatch("continue terminal work", attached.threadId);
    await h.reply("same live terminal response", attached.threadId);
    assert.deepEqual(prompts, ["continue terminal work"]);
    assert.equal(h.opened.length, 0, "attaching must never spawn a shadow OMP");
    await h.dispatch("/abort", attached.threadId);
    await h.reply("Abort requested", attached.threadId);
    assert.equal(aborts, 1);
    await h.bridge.detach(port);
    await h.dispatch("offline input", attached.threadId);
    await h.reply("Designated terminal is offline", attached.threadId);
    assert.deepEqual(prompts, ["continue terminal work"]);
    const offlineStatus = h.sent.length;
    await h.dispatch("/status", attached.threadId);
    await h.reply("Terminal: offline", attached.threadId, offlineStatus);
    await h.bridge.register(port, async () => {});
    const reconnected = await h.bridge.attach(port, "Ignored new name");
    assert.equal(reconnected.threadId, attached.threadId);
    assert.equal(h.created.length, 1, "reconnect must preserve the generated chat and topic");
    await h.bridge.detach(port);
    await h.bridge.stop();
    const stored = JSON.parse(await readFile(statePath, "utf8"));
    restarted = new Harness(statePath, stored.offset);
    await restarted.bridge.init();
    await restarted.dispatch("must not resume terminal transcript", attached.threadId);
    await restarted.reply("Designated terminal is offline", attached.threadId);
    assert.equal(restarted.opened.length, 0, "external ownership survives daemon restart");
    const restartedStatus = restarted.sent.length;
    await restarted.dispatch("/status", attached.threadId);
    await restarted.reply("Terminal: offline", attached.threadId, restartedStatus);
    await restarted.command("/running", "No live terminals registered");
    await assert.rejects(restarted.bridge.attach(port, "Not registered"), /not registered/);
    await restarted.bridge.register(port, async () => {});
    await restarted.command("/running", "Telegram topic: not attached");
    const restored = await restarted.bridge.attach(port, "Reconnect after restart");
    assert.equal(restored.id, attached.id);
    assert.equal(restarted.created.length, 0);
  } finally {
    await h.bridge.stop();
    await restarted?.bridge.stop();
    await rm(directory, { recursive: true, force: true });
  }
});

test("Terminal attachment cannot take over a daemon-owned transcript", { timeout: 15000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "omp-workspace-ownership-"));
  const h = new Harness(join(directory, "state.json"));
  try {
    await h.bindWorkspace();
    const thread = await h.create();
    const owned = h.sessions.get(thread)!;
    const port: SessionPort = {
      async state() { return { ...await owned.state(), cwd: "/workspace" }; },
      async prompt() { throw new Error("must not route"); },
      async steer() { throw new Error("must not route"); },
      async btw() { throw new Error("must not route"); },
      async abort() {}, async answer() {}, async stop() {},
    };
    await assert.rejects(h.bridge.register(port, async () => {}), /different session owner/);
    await assert.rejects(h.bridge.attach(port, "Takeover"), /not registered/);
    const ownedIdentity = await port.state();
    for (const override of [{ sessionId: "different-id" }, { sessionFile: "/other/transcript.jsonl" }]) {
      const conflict: SessionPort = { ...port, async state() { return { ...ownedIdentity, ...override }; } };
      await assert.rejects(h.bridge.register(conflict, async () => {}), /different session owner/);
    }
    assert.equal(h.created.length, 1);
    await h.dispatch("owned input", thread);
    await h.until(() => owned.prompts.length === 1);
    owned.complete("owned response");
    await h.reply("owned response", thread);
  } finally {
    await h.bridge.stop();
    await rm(directory, { recursive: true, force: true });
  }
});

test("Telegram discovers live terminals without topics and attaches only the exact chosen session", { timeout: 15000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "omp-workspace-discovery-"));
  const statePath = join(directory, "state.json");
  const h = new Harness(statePath);
  const alpha = new TerminalSession("shared-prefix-alpha-full-id", "/projects/alpha");
  const beta = new TerminalSession("shared-prefix-beta-full-id", "/projects/beta");
  beta.streaming = true;
  try {
    await h.bridge.init();
    const before = await readFile(statePath, "utf8");
    await h.bridge.register(alpha, alpha.onAttach);
    await h.bridge.register(beta, beta.onAttach);
    assert.equal(await readFile(statePath, "utf8"), before, "discovery is not durable topic/session state");
    assert.equal(h.created.length, 0);
    assert.equal(h.sent.length, 0);
    await assert.rejects(h.bridge.attach(alpha, "Before setup"), /Send \/workspace/);
    await h.command("/running", "Workspace is unbound");
    await h.command(`/attach ${beta.identity.sessionId}`, "Workspace is unbound");
    await h.command("/workspace", "Workspace bound");

    const listing = await h.command("/running", alpha.identity.sessionId);
    assert.ok(listing.text.includes(`OMP session: ${alpha.identity.sessionId}\nCwd: /projects/alpha\nIdle`));
    assert.ok(listing.text.includes(`OMP session: ${beta.identity.sessionId}\nCwd: /projects/beta\nBusy`));
    assert.equal(h.created.length, 0, "listing must not create even an unattached terminal's topic");
    assert.equal(h.opened.length, 0);
    assert.equal(alpha.attached.length + beta.attached.length, 0);
    await h.command("/running extra", "Usage: /running");
    await h.command("/attach", "Usage: /attach <full-session-id>");
    await h.command(`/attach ${beta.identity.sessionId} extra`, "Usage: /attach <full-session-id>");
    await h.command("/attach shared-prefix", "No live terminal with that exact full session ID");
    for (const body of ["/running", `/attach ${beta.identity.sessionId}`]) {
      await h.command(body, "Owner only", undefined, OWNER + 1);
      await h.command(body, "only in the bound Workspace group", undefined, OWNER, GROUP - 1);
    }
    assert.equal(h.created.length, 0);

    const attached = await h.command(`/attach ${beta.identity.sessionId}`, "Terminal attached.");
    assert.ok(attached.text.includes("https://t.me/c/123/101"));
    assert.equal(beta.attached.length, 1);
    assert.equal(beta.attached[0]!.groupId, GROUP);
    assert.equal(beta.attached[0]!.sessionId, beta.identity.sessionId);
    assert.equal(alpha.attached.length, 0);
    assert.equal(h.created.length, 1);
    await h.command("chosen live input", `${beta.identity.sessionId} response`, 101);
    assert.deepEqual(beta.prompts, ["chosen live input"]);
    assert.deepEqual(alpha.prompts, []);
    await h.command("/abort", "Abort requested", 101);
    assert.equal(beta.aborts, 1);
    assert.equal(alpha.aborts, 0);
    beta.streaming = false;
    const fresh = await h.command("/running", `${beta.identity.sessionId}\nCwd: /projects/beta\nIdle`);
    assert.ok(fresh.text.includes("Telegram topic: 101"));
    h.bridge.detach(beta);
    const closed = await h.command("/running", alpha.identity.sessionId);
    assert.ok(!closed.text.includes(beta.identity.sessionId));
    await h.command(`/attach ${beta.identity.sessionId}`, "No live terminal with that exact full session ID");
    await h.command("offline input", "Designated terminal is offline", 101);
    assert.deepEqual(beta.prompts, ["chosen live input"]);
  } finally {
    await h.bridge.stop();
    await rm(directory, { recursive: true, force: true });
  }
});

test("Repeated and concurrent Telegram/local attachments create one topic without blocking discovery", { timeout: 15000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "omp-workspace-attach-queue-"));
  const h = new Harness(join(directory, "state.json"));
  const terminal = new TerminalSession("concurrent-full-session-id");
  const started = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const requests: Array<Promise<unknown>> = [];
  try {
    await h.bindWorkspace();
    await h.bridge.register(terminal, terminal.onAttach);
    h.beforeCreateTopic = async () => { started.resolve(); await release.promise; };
    const first = h.bridge.attach(terminal, "Chosen terminal");
    const second = h.bridge.attach(terminal, "Do not rename");
    requests.push(first, second);
    await started.promise;
    const after = h.sent.length;
    await h.dispatch(`/attach ${terminal.identity.sessionId}`);
    await h.dispatch(`/attach ${terminal.identity.sessionId}`);
    await h.command("/running", "Telegram topic: not attached");
    assert.equal(h.created.length, 1);
    release.resolve();
    const [binding, repeated] = await Promise.all([first, second]);
    assert.equal(repeated.id, binding.id);
    await h.reply("Terminal attached.", undefined, after);
    await h.command(`/attach ${terminal.identity.sessionId}`, "Terminal attached.");
    assert.equal((await h.bridge.attach(terminal, "Still the same")).threadId, binding.threadId);
    assert.equal(h.created.length, 1);
    assert.equal(terminal.attached.length, 1, "idempotent attachment does not authorize or notify twice");
    assert.equal(h.opened.length, 0);
    await h.command("routed after authorization", `${terminal.identity.sessionId} response`, binding.threadId);
  } finally {
    release.resolve();
    await Promise.allSettled(requests);
    await h.bridge.stop();
    await rm(directory, { recursive: true, force: true });
  }
});

test("Registration fences exact identity and rejects duplicate session IDs or canonical transcript owners", { timeout: 15000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "omp-workspace-identity-"));
  const h = new Harness(join(directory, "state.json"));
  const file = join(directory, "terminal.jsonl");
  const alias = join(directory, "alias.jsonl");
  await writeFile(file, "");
  await symlink(file, alias);
  const terminal = new TerminalSession("identity-full-id", "/project", file);
  try {
    await h.bindWorkspace();
    for (const invalid of [
      { sessionId: "", sessionFile: file, cwd: "/project" },
      { sessionId: "has whitespace", sessionFile: file, cwd: "/project" },
      { sessionId: "relative-file", sessionFile: "relative.jsonl", cwd: "/project" },
      { sessionId: "relative-cwd", sessionFile: file, cwd: "relative" },
    ]) {
      const port = new TerminalSession("invalid");
      Object.assign(port.identity, invalid);
      await assert.rejects(h.bridge.register(port, port.onAttach), /persistent session identity/);
      await assert.rejects(h.bridge.attach(port, "Invalid"), /not registered/);
    }
    const contenders = [new TerminalSession("concurrent-owner", "/one"), new TerminalSession("concurrent-owner", "/two", "/other/concurrent.jsonl")];
    const registrations = await Promise.allSettled(contenders.map((port) => h.bridge.register(port, port.onAttach)));
    assert.equal(registrations.filter((result) => result.status === "fulfilled").length, 1, "concurrent registration must reserve one live owner");
    assert.equal(registrations.filter((result) => result.status === "rejected").length, 1);
    for (const contender of contenders) h.bridge.detach(contender);
    await h.bridge.register(terminal, terminal.onAttach);
    const duplicateId = new TerminalSession(terminal.identity.sessionId, "/other", join(directory, "other.jsonl"));
    const duplicateFile = new TerminalSession("other-full-id", "/other", alias);
    await assert.rejects(h.bridge.register(duplicateId, duplicateId.onAttach), /live terminal owner/);
    await assert.rejects(h.bridge.register(duplicateFile, duplicateFile.onAttach), /live terminal owner/);
    terminal.identity.sessionId = "changed-full-id";
    await h.command("/running", "No live terminals registered");
    terminal.identity.sessionId = "identity-full-id";
    await assert.rejects(h.bridge.attach(terminal, "Stale"), /not registered/);
    await h.bridge.register(terminal, terminal.onAttach);
    terminal.identity.cwd = "/changed-project";
    await assert.rejects(h.bridge.attach(terminal, "Changed cwd"), /identity changed/);
    terminal.identity.cwd = "/project";
    await h.bridge.register(terminal, terminal.onAttach);
    terminal.identity.sessionFile = alias;
    await assert.rejects(h.bridge.attach(terminal, "Changed exact file"), /identity changed/);
    terminal.identity.sessionFile = file;
    assert.equal(h.created.length, 0, "stale identity cannot create a topic");
    await h.bridge.register(terminal, terminal.onAttach);
    const binding = await h.bridge.attach(terminal, "Identity owner");
    terminal.identity.sessionId = "another-session";
    await h.command("must not cross sessions", "identity changed", binding.threadId);
    assert.deepEqual(terminal.prompts, []);
    await assert.rejects(h.bridge.attach(terminal, "Stale route"), /not registered/);
    await assert.rejects(h.bridge.register(duplicateFile, duplicateFile.onAttach), /different session owner/);
    assert.equal(h.created.length, 1);
  } finally {
    await h.bridge.stop();
    await rm(directory, { recursive: true, force: true });
  }
});

test("Disconnect during registration or attachment cannot resurrect discovery or topic routing", { timeout: 15000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "omp-workspace-attach-close-"));
  const h = new Harness(join(directory, "state.json"));
  const gates: Array<{ resolve(): void }> = [];
  const requests: Array<Promise<unknown>> = [];
  try {
    await h.bindWorkspace();
    for (const phase of ["registration", "state", "topic", "persistence", "authorization"] as const) {
      const terminal = new TerminalSession(`disconnect-${phase}-full-id`);
      const started = Promise.withResolvers<void>();
      const release = Promise.withResolvers<void>();
      gates.push(release);
      const pause = async () => { started.resolve(); await release.promise; };
      let pending: Promise<unknown>;
      if (phase === "registration") {
        terminal.beforeState = pause;
        pending = h.bridge.register(terminal, terminal.onAttach);
      } else {
        await h.bridge.register(terminal, terminal.onAttach);
        if (phase === "state") terminal.beforeState = pause;
        if (phase === "topic") h.beforeCreateTopic = pause;
        if (phase === "persistence") {
          // Hold the persistence boundary without relying on filesystem scheduling.
          const save = Reflect.get(h.bridge, "save");
          Reflect.set(h.bridge, "save", async () => {
            Reflect.set(h.bridge, "save", save);
            const saved = Reflect.apply(save, h.bridge, []);
            await pause();
            await saved;
          });
        }
        if (phase === "authorization") terminal.beforeAttach = pause;
        pending = h.bridge.attach(terminal, phase);
      }
      requests.push(pending);
      const rejected = assert.rejects(pending, /registration is no longer live/);
      await started.promise;
      const threadId = phase === "authorization" ? terminal.attached[0]!.threadId : undefined;
      if (threadId !== undefined) {
        await h.command("not authorized yet", "Designated terminal is offline", threadId);
      }
      h.bridge.detach(terminal);
      release.resolve();
      await rejected;
      h.beforeCreateTopic = undefined;
      await h.command("/running", "No live terminals registered");
      await assert.rejects(h.bridge.attach(terminal, "Do not resurrect"), /not registered/);
      assert.deepEqual(terminal.prompts, []);
      if (phase === "topic" || phase === "persistence" || phase === "authorization") {
        const created = h.created.length;
        terminal.beforeAttach = undefined;
        await h.bridge.register(terminal, terminal.onAttach);
        const restored = await h.bridge.attach(terminal, "Reuse known created topic");
        assert.equal(h.created.length, created);
        h.bridge.detach(terminal);
        await h.command("still offline", "Designated terminal is offline", restored.threadId);
        assert.deepEqual(terminal.prompts, []);
      } else assert.equal(terminal.attached.length, 0);
    }
  } finally {
    for (const gate of gates) gate.resolve();
    await Promise.allSettled(requests);
    await h.bridge.stop();
    await rm(directory, { recursive: true, force: true });
  }
});

test("Uncertain terminal topic creation is latched instead of retried by queued attachment requests", { timeout: 15000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "omp-workspace-uncertain-topic-"));
  const h = new Harness(join(directory, "state.json"));
  const terminal = new TerminalSession("uncertain-full-session-id");
  try {
    await h.bindWorkspace();
    await h.bridge.register(terminal, terminal.onAttach);
    h.beforeCreateTopic = async () => { throw new Error("Telegram response lost after side effect"); };
    const first = h.bridge.attach(terminal, "Unknown result");
    const second = h.bridge.attach(terminal, "Do not retry");
    await Promise.all([
      assert.rejects(first, /Topic creation outcome is uncertain/),
      assert.rejects(second, /Topic creation outcome is uncertain/),
    ]);
    await h.command(`/attach ${terminal.identity.sessionId}`, "Topic creation outcome is uncertain");
    assert.equal(h.created.length, 1);
    assert.equal(terminal.attached.length, 0);
    assert.equal(h.opened.length, 0);
  } finally {
    await h.bridge.stop();
    await rm(directory, { recursive: true, force: true });
  }
});
