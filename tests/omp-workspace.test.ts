/**
 * Standalone Workspace bridge regression tests.
 * Zones: forum/session isolation, owner admission, crash recovery, durable identity.
 */
import assert from "node:assert/strict";
import { copyFile, mkdir, mkdtemp, readFile, realpath, rm, stat, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { scheduler } from "node:timers/promises";
import { test } from "node:test";
import { WorkspaceBridge } from "../omp/workspace.ts";
import type { Binding, SessionPort, UiRequest } from "../omp/workspace.ts";
import { CompletedReplyError, OmpSession, SessionLifecycleError } from "../omp/rpc.ts";

const OWNER = 42;
const BOT = 99;
const GROUP = -100123;
type Sent = { chat_id: number; message_thread_id?: number; text: string; parse_mode?: "HTML" };
type Action = { chat_id: number; message_thread_id?: number; action: "typing" };
type Upload = {
  method: string;
  fields: Record<string, string>;
  fileField: string;
  filePath: string;
  fileName: string;
};

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
  readonly uploads: Upload[] = [];
  readonly uploadContents: string[] = [];
  readonly transcriptions: string[] = [];
  private readonly listeners = new Set<() => void>();
  private updateId: number;
  private nextThread = 100;
  failSend?: (message: Sent) => boolean;
  beforeSend?: (message: Sent) => Promise<void>;
  failUpload?: (upload: Upload) => boolean;
  beforeUpload?: (upload: Upload) => Promise<void>;
  beforeCreateTopic?: () => Promise<void>;
  openSession?: (binding: Binding, onUi: (request: UiRequest) => void) => Promise<SessionPort | undefined>;
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
      assert.ok(method === "sendMessage" || method === "sendRichMessage");
      const sent = params as Sent;
      if (method === "sendRichMessage") {
        const rich = params.rich_message;
        assert.ok(rich && typeof rich === "object" && "markdown" in rich && typeof rich.markdown === "string");
        sent.text = rich.markdown;
      }
      if (this.failSend?.(sent)) throw new Error("test transport unavailable");
      await this.beforeSend?.(sent);
      this.sent.push({ ...sent });
      for (const listener of this.listeners) listener();
      return { message_id: this.sent.length };
    }, async (binding, onUi) => {
      this.opened.push({ ...binding });
      this.ui.set(binding.threadId, onUi);
      const opened = await this.openSession?.(binding, onUi);
      if (opened) return opened;
      const session = new FakeSession(binding, () => { for (const listener of this.listeners) listener(); });
      this.sessions.set(binding.threadId, session);
      return session;
    }, async (fileId, fileName, directory) => {
      const path = join(directory, fileName);
      this.downloads.push({ fileId, fileName, directory, path });
      return path;
    }, async (method, fields, fileField, filePath, fileName) => {
      this.uploads.push({ method, fields, fileField, filePath, fileName });
      await this.beforeUpload?.(this.uploads[this.uploads.length - 1]!);
      this.uploadContents.push(await readFile(filePath, "utf8"));
      if (this.failUpload?.(this.uploads[this.uploads.length - 1]!)) throw new Error("upload acknowledgement lost");
      for (const listener of this.listeners) listener();
      return { message_id: this.uploads.length };
    }, async (filePath) => {
      this.transcriptions.push(filePath);
      return "spoken request";
    });
  }
  notify(): void { for (const listener of this.listeners) listener(); }
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
  async drain(): Promise<void> {
    while (!this.bridge.idle) await scheduler.yield();
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
    await h.dispatch("alpha input", alpha);
    await h.dispatch("beta input", beta);
    await h.until(() => h.sessions.get(alpha)?.prompts.length === 1 && h.sessions.get(beta)?.prompts.length === 1);
    assert.notEqual(h.opened[0]!.id, h.opened[1]!.id);
    assert.notEqual(h.opened[0]!.id, String(alpha));
    await h.dispatch("change direction", alpha);
    await h.reply("Steering applied", alpha);
    assert.deepEqual(h.sessions.get(alpha)!.steers, ["change direction"]);
    await h.dispatch("/status", alpha);
    await h.reply("Busy", alpha);
    assert.deepEqual(h.sessions.get(alpha)!.prompts, ["alpha input"]);
    assert.deepEqual(h.sessions.get(beta)!.prompts, ["beta input"]);
    await h.command("/btw status <now> & later", "Sidecar", alpha);
    assert.deepEqual(h.sessions.get(alpha)!.btws, ["status <now> & later"]);
    assert.deepEqual(h.sessions.get(alpha)!.prompts, ["alpha input"], "side questions must stay out of the main transcript");

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
    assert.ok(h.sent.every((sent) => sent.chat_id === GROUP));
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

test("Workspace repairs a missing managed transcript without replacing existing history or attachments", { timeout: 15000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "omp-workspace-repair-"));
  const statePath = join(directory, "state.json");
  const sessionDirectory = join(directory, "sessions", "abcdef123456");
  const transcript = join(sessionDirectory, "missing.jsonl");
  const attachment = join(directory, "attachments", String(GROUP), "53", "91", "leads.csv");
  await mkdir(sessionDirectory, { recursive: true });
  await mkdir(join(directory, "attachments", String(GROUP), "53", "91"), { recursive: true });
  await writeFile(transcript, "existing history");
  await writeFile(attachment, "lead");
  await writeFile(statePath, JSON.stringify({
    version: 1,
    ownerId: OWNER,
    botId: BOT,
    offset: 0,
    groupId: GROUP,
    bindings: [{
      id: "abcdef123456",
      threadId: 53,
      name: "getsales-setup",
      cwd: "/workspace",
      sessionId: "missing-session",
      sessionFile: transcript,
    }],
    admissions: [],
  }));
  const h = new Harness(statePath);
  try {
    await h.bridge.init();
    await h.dispatch("/repair", 53);
    await h.reply("transcript still exists; repair refused", 53);
    assert.equal(JSON.parse(await readFile(statePath, "utf8")).bindings[0].sessionId, "missing-session");

    await rm(transcript);
    h.openSession = async () => {
      try { await readFile(transcript); }
      catch (error) { throw new SessionLifecycleError(error, true); }
      return undefined;
    };
    await h.command("request before repair", "ENOENT", 53);
    h.openSession = undefined;
    await h.dispatch("/repair", 53);
    await h.reply("Missing transcript released", 53);
    assert.equal(JSON.parse(await readFile(statePath, "utf8")).bindings[0].sessionId, undefined);
    assert.equal(await readFile(attachment, "utf8"), "lead");

    await h.dispatch("configure GetSales", 53);
    await h.until(() => h.sessions.get(53)?.prompts.length === 1);
    assert.equal(h.opened.at(-1)?.sessionFile, undefined, "first use after explicit repair creates a session instead of resuming the missing transcript");
    h.sessions.get(53)!.complete("ready");
    await h.reply("ready", 53);
    assert.ok(JSON.parse(await readFile(statePath, "utf8")).bindings[0].sessionId);
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
    assert.equal(h.sessions.has(101), false);
    const prompt = h.update("one admitted turn", 101);
    await h.bridge.handle(prompt);
    await h.until(() => h.sessions.get(101)?.prompts.length === 1);
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
    const afterVoice = h.sent.length;
    await h.bridge.handle(h.incoming({
      voice: { file_id: "voice-file", duration: 2, mime_type: "audio/ogg" },
    }, threadId));
    await h.reply("Steering applied", threadId, afterVoice);
    assert.match(h.sessions.get(threadId)!.steers[1]!, /\[outputs\]\n- Voice transcript \(voice-\d+\.ogg\): spoken request/);
    assert.deepEqual(h.transcriptions, [h.downloads[2]!.path]);
    h.sessions.get(threadId)!.complete("both files reviewed");
    await h.reply("both files reviewed", threadId);
  } finally {
    await h.bridge.stop();
    await rm(directory, { recursive: true, force: true });
  }
});

test("Workspace uploads linked workspace files and contained local artifacts without leaking outside either root", { timeout: 15000 }, async () => {
  const directory = await realpath(await mkdtemp(join(tmpdir(), "omp-workspace-output-")));
  const statePath = join(directory, "state.json");
  const transcript = join(directory, "sessions", "turn.jsonl");
  const local = join(directory, "sessions", "turn", "local");
  const report = join(local, "reports", "mont.md");
  const secret = join(directory, "sessions", "turn", "secret.txt");
  const cwd = join(directory, "workspace");
  const design = join(cwd, "openspec", "design (final).md");
  const absolute = join(cwd, "summary.txt");
  await mkdir(join(cwd, "openspec"), { recursive: true });
  await writeFile(design, "workspace design\n");
  await writeFile(absolute, "workspace summary\n");
  await writeFile(join(cwd, "example.md"), "code examples must not upload\n");
  await mkdir(join(local, "reports"), { recursive: true });
  await writeFile(report, "safe report\n");
  await writeFile(secret, "must not upload\n");
  await symlink(secret, join(local, "leak.txt"));
  await symlink(secret, join(cwd, "leak.txt"));
  const h = new Harness(statePath, 0, cwd);
  let session: FakeSession | undefined;
  h.openSession = async (binding) => {
    session = new FakeSession({ ...binding, sessionFile: transcript }, () => h.notify());
    h.sessions.set(binding.threadId, session);
    return session;
  };
  try {
    await h.bindWorkspace();
    const threadId = await h.create("Output");
    await h.dispatch("Create the report", threadId);
    await h.until(() => session?.prompts.length === 1);
    session!.complete([
      "Done: [report](local://reports%2Fmont.md)",
      "[outside](local://../secret.txt)",
      "[symlink](local://leak.txt)",
      "[design](<openspec/design (final).md>)",
      `[summary](${absolute} "Summary")`,
      "[duplicate](openspec/design%20%28final%29.md)",
      "[outside](../sessions/turn/secret.txt)",
      `[outside absolute](${secret})`,
      "[symlink](leak.txt)",
      "[web](https://example.com/summary.txt)",
      "[directory](openspec)",
      "[missing](missing.md)",
      "[encoded traversal](%2e%2e/sessions/turn/secret.txt)",
      "`[example](example.md)`",
      "```markdown\n[example](example.md)\n```",
    ].join("\n"));
    await h.reply("Done:", threadId);
    await h.bridge.stop();
    assert.deepEqual(h.uploads.map(({ filePath: _filePath, ...upload }) => upload), [report, design, absolute].map((filePath) => ({
      method: "sendDocument",
      fields: { chat_id: String(GROUP), message_thread_id: String(threadId) },
      fileField: "document",
      fileName: basename(filePath),
    })));
    assert.deepEqual(h.uploadContents, ["safe report\n", "workspace design\n", "workspace summary\n"]);
    for (const upload of h.uploads) await assert.rejects(stat(upload.filePath), { code: "ENOENT" });
  } finally {
    await h.bridge.stop();
    await rm(directory, { recursive: true, force: true });
  }
});

test("Workspace uploads a pinned private snapshot even if the source is retargeted during transport", { timeout: 15000 }, async () => {
  const directory = await realpath(await mkdtemp(join(tmpdir(), "omp-workspace-pinned-")));
  const cwd = join(directory, "project");
  const source = join(cwd, "report.txt");
  const secret = join(directory, "secret.txt");
  await mkdir(cwd);
  await writeFile(source, "approved report");
  await writeFile(secret, "outside secret");
  const h = new Harness(join(directory, "state.json"), 0, cwd);
  try {
    await h.bindWorkspace();
    const threadId = await h.create();
    await h.dispatch("report", threadId);
    await h.until(() => h.sessions.get(threadId)?.prompts.length === 1);
    h.beforeUpload = async (upload) => {
      assert.equal(upload.fileName, "report.txt");
      await rm(source);
      await symlink(secret, source);
    };
    h.sessions.get(threadId)!.complete("Done [report](report.txt)");
    await h.drain();
    assert.deepEqual(h.uploadContents, ["approved report"]);
    assert.equal(h.uploads.length, 1);
    await assert.rejects(stat(h.uploads[0]!.filePath), { code: "ENOENT" });
  } finally {
    await h.bridge.stop();
    await rm(directory, { recursive: true, force: true });
  }
});

test("Workspace durably retries ordinary command replies and sidecars without repeating their commands", { timeout: 15000 }, async () => {
  for (const command of ["/status", "/btw saved side question"]) {
    const directory = await mkdtemp(join(tmpdir(), "omp-workspace-command-reply-"));
    const statePath = join(directory, "state.json");
    const h = new Harness(statePath);
    let resumed: Harness | undefined;
    try {
      await h.bindWorkspace();
      const threadId = await h.create();
      await h.dispatch("initialize", threadId);
      await h.until(() => h.sessions.get(threadId)?.prompts.length === 1);
      h.sessions.get(threadId)!.complete("initialized");
      await h.drain();
      h.failSend = (message) => message.text.includes(command === "/status" ? "Pending UI:" : "sidecar: saved side question");
      await h.dispatch(command, threadId);
      await h.drain();
      const saved = JSON.parse(await readFile(statePath, "utf8"));
      const parts = saved.admissions[0].reply.parts as Array<{ kind: string; body: string }>;
      assert.equal(parts[0]!.kind, command === "/status" ? "text" : "markdown");
      if (command !== "/status") assert.match(parts[0]!.body, /Sidecar/);
      await h.bridge.stop();
      resumed = new Harness(statePath, saved.offset);
      await resumed.bridge.init();
      assert.equal(resumed.sent.length, 2);
      assert.match(resumed.sent[0]!.text, /may duplicate/);
      assert.equal(resumed.sent[1]!.text, parts[0]!.body);
      assert.ok(resumed.sent.every((message) => message.chat_id === GROUP && message.message_thread_id === threadId));
      assert.equal(resumed.opened.length, 0);
      assert.deepEqual(h.sessions.get(threadId)!.btws, command === "/status" ? [] : ["saved side question"]);
      assert.equal(JSON.parse(await readFile(statePath, "utf8")).admissions.length, 0);
    } finally {
      await h.bridge.stop();
      await resumed?.bridge.stop();
      await rm(directory, { recursive: true, force: true });
    }
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
    await h.until(() => h.sessions.get(threadId)?.prompts.length === 1);
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

test("Workspace recovers saved text and attachments after failed delivery without executing or opening OMP", { timeout: 15000 }, async () => {
  const directory = await realpath(await mkdtemp(join(tmpdir(), "omp-workspace-delivery-")));
  const statePath = join(directory, "state.json");
  const attachment = join(directory, "report.txt");
  await writeFile(attachment, "saved report");
  const h = new Harness(statePath, 0, directory);
  let resumed: Harness | undefined;
  try {
    await h.bindWorkspace();
    const threadId = await h.create();
    const update = h.update("execute once", threadId);
    await h.bridge.handle(update);
    await h.until(() => h.sessions.get(threadId)?.prompts.length === 1);
    const body = "reply could not be delivered [report](report.txt)";
    h.failSend = (message) => message.text === body;
    h.sessions.get(threadId)!.complete(body);
    await h.bridge.stop();
    const stored = JSON.parse(await readFile(statePath, "utf8"));
    assert.equal(stored.admissions.length, 1);
    assert.equal(stored.admissions[0].phase, "delivery");
    assert.equal(stored.admissions[0].reply.cursor, 0);
    assert.equal(stored.admissions[0].reply.inFlight, true);
    assert.equal(stored.admissions[0].reply.parts[0].body, body);
    assert.equal(stored.admissions[0].reply.parts[1].path, attachment);
    assert.deepEqual(h.sessions.get(threadId)!.prompts, ["execute once"]);
    resumed = new Harness(statePath, stored.offset, directory);
    await resumed.bridge.init();
    assert.match(resumed.sent[0]!.text, /may duplicate/);
    assert.equal(resumed.sent[1]!.text, body);
    assert.ok(resumed.sent.every((message) => message.chat_id === GROUP && message.message_thread_id === threadId));
    assert.deepEqual(resumed.uploads.map(({ fileName, fields }) => ({ fileName, fields })), [{
      fileName: "report.txt", fields: { chat_id: String(GROUP), message_thread_id: String(threadId) },
    }]);
    assert.deepEqual(resumed.uploadContents, ["saved report"]);
    await resumed.bridge.handle(update);
    assert.equal(resumed.opened.length, 0);
    assert.equal(resumed.created.length, 0);
    assert.equal(JSON.parse(await readFile(statePath, "utf8")).admissions.length, 0);
  } finally {
    await h.bridge.stop();
    await resumed?.bridge.stop();
    await rm(directory, { recursive: true, force: true });
  }
});

test("Workspace recovery preserves acknowledged chunks and files and warns before an uncertain file retry", { timeout: 15000 }, async () => {
  const directory = await realpath(await mkdtemp(join(tmpdir(), "omp-workspace-parts-")));
  const statePath = join(directory, "state.json");
  const first = join(directory, "first.txt");
  const second = join(directory, "second.txt");
  await writeFile(first, "first");
  await writeFile(second, "second");
  const h = new Harness(statePath, 0, directory);
  let resumed: Harness | undefined;
  try {
    await h.bindWorkspace();
    const threadId = await h.create();
    await h.dispatch("create files", threadId);
    await h.until(() => h.sessions.get(threadId)?.prompts.length === 1);
    h.failUpload = ({ fileName }) => fileName === "second.txt";
    h.sessions.get(threadId)!.complete(`${"a".repeat(33_000)}\n\n[first](first.txt) [second](second.txt)`);
    await h.bridge.stop();
    const saved = JSON.parse(await readFile(statePath, "utf8"));
    const reply = saved.admissions[0].reply;
    assert.ok(reply.parts.filter((part: { kind: string }) => part.kind === "markdown").length > 1);
    assert.equal(reply.cursor, reply.parts.length - 1);
    assert.equal(reply.parts[reply.cursor].path, second);
    assert.equal(reply.inFlight, true);
    assert.deepEqual(h.uploads.map(({ fileName }) => fileName), ["first.txt", "second.txt"]);
    resumed = new Harness(statePath, saved.offset, directory);
    await resumed.bridge.init();
    assert.equal(resumed.sent.length, 1, "no acknowledged text chunk is resent");
    assert.match(resumed.sent[0]!.text, /may duplicate/);
    assert.equal(resumed.sent[0]!.message_thread_id, threadId);
    assert.deepEqual(resumed.uploads.map(({ fileName }) => fileName), ["second.txt"], "only the uncertain file is retried");
    assert.deepEqual(resumed.uploadContents, ["second"]);
    assert.equal(resumed.opened.length, 0);
    assert.equal(JSON.parse(await readFile(statePath, "utf8")).admissions.length, 0);
  } finally {
    await h.bridge.stop();
    await resumed?.bridge.stop();
    await rm(directory, { recursive: true, force: true });
  }
});

test("Workspace delivery retries are bounded, nonblocking, exclusive, and drained on shutdown", { timeout: 15000 }, async (t) => {
  t.mock.timers.enable({ apis: ["Date"], now: 100_000 });
  const directory = await mkdtemp(join(tmpdir(), "omp-workspace-retry-"));
  const h = new Harness(join(directory, "state.json"));
  const gate = Promise.withResolvers<void>();
  const entered = Promise.withResolvers<void>();
  try {
    await h.bindWorkspace();
    const threadId = await h.create();
    await h.dispatch("one turn", threadId);
    await h.until(() => h.sessions.get(threadId)?.prompts.length === 1);
    let failures = 0;
    h.failSend = ({ text }) => {
      if (text !== "saved answer") return false;
      failures++;
      return true;
    };
    h.sessions.get(threadId)!.complete("saved answer");
    await h.drain();
    await h.bridge.retryDeliveries();
    await h.drain();
    assert.equal(failures, 1, "normal polling must not hammer a failed delivery");
    t.mock.timers.tick(30_000);
    await h.bridge.retryDeliveries();
    await h.drain();
    assert.equal(failures, 2, "current-process failures become eligible again");
    h.failSend = undefined;
    t.mock.timers.tick(30_000);
    h.beforeSend = async ({ text }) => {
      if (text === "saved answer") { entered.resolve(); await gate.promise; }
    };
    await h.bridge.retryDeliveries();
    await entered.promise;
    await h.bridge.retryDeliveries();
    await h.command("/status", "Idle", threadId);
    assert.equal(h.sent.filter(({ text }) => text === "saved answer").length, 0);
    let stopped = false;
    const stopping = h.bridge.stop().then(() => { stopped = true; });
    await scheduler.yield();
    assert.equal(stopped, false, "shutdown waits for an in-flight delivery");
    gate.resolve();
    await stopping;
    assert.equal(h.sent.filter(({ text }) => text === "saved answer").length, 1);
    assert.deepEqual(h.sessions.get(threadId)!.prompts, ["one turn"]);
    assert.equal(h.opened.length, 1);
    assert.equal(JSON.parse(await readFile(join(directory, "state.json"), "utf8")).admissions.length, 0);
    const count = h.sent.length;
    await h.bridge.retryDeliveries();
    assert.equal(h.sent.length, count);
  } finally {
    gate.resolve();
    await h.bridge.stop();
    await rm(directory, { recursive: true, force: true });
  }
});

test("Workspace preserves a proven completed answer with an explicit late-error warning", { timeout: 15000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "omp-workspace-late-error-"));
  const h = new Harness(join(directory, "state.json"));
  let resumed: Harness | undefined;
  h.openSession = async (binding) => {
    const session = new FakeSession(binding, () => h.notify());
    session.prompt = async (body) => {
      session.prompts.push(body);
      throw new CompletedReplyError("Completed despite disconnect", new Error("read ECONNRESET"));
    };
    h.sessions.set(binding.threadId, session);
    return session;
  };
  try {
    await h.bindWorkspace();
    const threadId = await h.create();
    h.failSend = ({ text }) => text.includes("Completed despite disconnect");
    await h.dispatch("finish once", threadId);
    await h.drain();
    const saved = JSON.parse(await readFile(join(directory, "state.json"), "utf8"));
    const body = saved.admissions[0].reply.parts.map((part: { body: string }) => part.body).join("");
    assert.match(body, /Completed despite disconnect/);
    assert.match(body, /final answer completed.*later OMP lifecycle\/transport step failed: read ECONNRESET/);
    assert.match(body, /Later lifecycle steps are not confirmed/);
    assert.doesNotMatch(body, /Update .* failed:/);
    assert.deepEqual(h.sessions.get(threadId)!.prompts, ["finish once"]);
    await h.bridge.stop();
    resumed = new Harness(join(directory, "state.json"), saved.offset);
    await resumed.bridge.init();
    assert.ok(resumed.sent.some((message) => message.message_thread_id === threadId
      && message.text.includes("Completed despite disconnect") && message.text.includes("Later lifecycle steps are not confirmed")));
    assert.equal(resumed.opened.length, 0);
  } finally {
    await h.bridge.stop();
    await resumed?.bridge.stop();
    await rm(directory, { recursive: true, force: true });
  }
});

test("Workspace rejects invalid saved replies instead of resetting or delivering them", async () => {
  const directory = await mkdtemp(join(tmpdir(), "omp-workspace-invalid-reply-"));
  const statePath = join(directory, "state.json");
  const target = { chatId: GROUP, threadId: 51 };
  const valid = { target, parts: [{ kind: "markdown", body: "saved answer" }], cursor: 0 };
  const invalid = [
    { ...valid, cursor: -1 },
    { ...valid, cursor: 2 },
    { ...valid, cursor: 1, inFlight: true },
    { ...valid, inFlight: false },
    { ...valid, retryAt: "tomorrow" },
    { ...valid, target: { ...target, chatId: GROUP - 1 } },
    { ...valid, target: { ...target, threadId: 52 } },
    { ...valid, parts: [] },
    { ...valid, parts: [{ kind: "markdown", body: "answer", hidden: true }] },
    { ...valid, parts: [{ kind: "text", body: "x".repeat(4097) }] },
    { ...valid, parts: [{ kind: "file", root: directory, path: join(directory, "..", "secret") }] },
    { ...valid, parts: [{ kind: "file", root: directory, path: "relative.txt" }] },
    { ...valid, parts: Array.from({ length: 11 }, (_, index) => ({ kind: "file", root: directory, path: join(directory, `${index}.txt`) })) },
    { ...valid, surprise: "not allowed" },
  ];
  try {
    for (const reply of invalid) {
      const source = JSON.stringify({
        version: 1, ownerId: OWNER, botId: BOT, offset: 2, bindings: [], admissions: [{
          updateId: 1, target, kind: "prompt", phase: "delivery", reply,
        }],
      });
      await writeFile(statePath, source);
      const h = new Harness(statePath);
      await assert.rejects(h.bridge.init(), /Invalid Workspace state/);
      assert.equal(await readFile(statePath, "utf8"), source);
      assert.equal(h.sent.length, 0);
      assert.equal(h.uploads.length, 0);
      assert.equal(h.opened.length, 0);
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("Workspace rechecks pending files after restart and never uploads a symlink retarget outside the allowed root", { timeout: 15000 }, async () => {
  const directory = await realpath(await mkdtemp(join(tmpdir(), "omp-workspace-retarget-")));
  const cwd = join(directory, "project");
  const statePath = join(directory, "state.json");
  const file = join(cwd, "report.txt");
  const secret = join(directory, "secret.txt");
  await mkdir(cwd);
  await writeFile(file, "safe report");
  await writeFile(secret, "private");
  const h = new Harness(statePath, 0, cwd);
  let resumed: Harness | undefined;
  try {
    await h.bindWorkspace();
    const threadId = await h.create();
    await h.dispatch("report", threadId);
    await h.until(() => h.sessions.get(threadId)?.prompts.length === 1);
    h.failUpload = () => true;
    h.sessions.get(threadId)!.complete("Done [report](report.txt)");
    await h.bridge.stop();
    const saved = JSON.parse(await readFile(statePath, "utf8"));
    await rm(file);
    await symlink(secret, file);
    resumed = new Harness(statePath, saved.offset, cwd);
    await resumed.bridge.init();
    assert.equal(resumed.uploads.length, 0);
    assert.equal(resumed.opened.length, 0);
    assert.match(resumed.sent[0]!.text, /may duplicate/);
    const pending = JSON.parse(await readFile(statePath, "utf8")).admissions[0];
    assert.equal(pending.phase, "delivery");
    assert.equal(pending.reply.cursor, 1, "acknowledged text stays acknowledged even if the file becomes unsafe");
    assert.equal(pending.reply.parts[1].path, file);
    assert.match(pending.diagnostic, /canonical allowed path|symbolic links|ELOOP/);
  } finally {
    await h.bridge.stop();
    await resumed?.bridge.stop();
    await rm(directory, { recursive: true, force: true });
  }
});

test("Workspace attachment preparation errors preserve completed text and explain the missing uploads", { timeout: 15000 }, async () => {
  const directory = await realpath(await mkdtemp(join(tmpdir(), "omp-workspace-preparation-")));
  const h = new Harness(join(directory, "state.json"), 0, directory);
  const links = [];
  for (let index = 0; index < 11; index++) {
    await writeFile(join(directory, `${index}.txt`), String(index));
    links.push(`[${index}](${index}.txt)`);
  }
  try {
    await h.bindWorkspace();
    const threadId = await h.create();
    await h.dispatch("many outputs", threadId);
    await h.until(() => h.sessions.get(threadId)?.prompts.length === 1);
    const body = `Completed report.\n${links.join("\n")}`;
    h.sessions.get(threadId)!.complete(body);
    await h.drain();
    assert.ok(h.sent.some((message) => message.message_thread_id === threadId && message.text === body));
    assert.ok(h.sent.some((message) => message.message_thread_id === threadId
      && message.text.includes("answer is complete") && message.text.includes("attachment preparation failed")));
    assert.equal(h.uploads.length, 0);
    assert.equal(JSON.parse(await readFile(join(directory, "state.json"), "utf8")).admissions.length, 0);
  } finally {
    await h.bridge.stop();
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
    await h.dispatch("initialize", thread);
    await h.until(() => h.sessions.get(thread)?.prompts.length === 1);
    h.sessions.get(thread)!.complete("ready");
    await h.reply("ready", thread);
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

test("Workspace latches a missing designated transcript without poisoning other topics or shutdown", { timeout: 15000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "omp-workspace-missing-"));
  const statePath = join(directory, "state.json");
  const binding: Binding = { id: "aaaaaaaaaaaa", threadId: 10, name: "Missing", cwd: directory,
    sessionId: "missing-session", sessionFile: join(directory, "missing", "transcript.jsonl") };
  await writeFile(statePath, JSON.stringify({ version: 1, ownerId: OWNER, botId: BOT, offset: 0,
    groupId: GROUP, bindings: [binding], admissions: [] }));
  const h = new Harness(statePath, 0, directory);
  h.openSession = async (opening, onUi) => {
    if (opening.id !== binding.id) return undefined;
    const session = new OmpSession({ binary: process.execPath, cwd: opening.cwd,
      sessionDir: join(directory, opening.id), resume: opening.sessionFile, onUi });
    await session.ready();
    return session;
  };
  try {
    await h.bridge.init();
    await h.command("first request", "ENOENT", binding.threadId);
    await h.command("second request", "ENOENT", binding.threadId);
    assert.equal(h.opened.filter((opening) => opening.id === binding.id).length, 1, "failed opening must not spawn a replacement");
    assert.equal(h.sessions.has(binding.threadId), false);

    const healthy = await h.create("Healthy");
    await h.dispatch("still usable", healthy);
    await h.until(() => h.sessions.get(healthy)?.prompts.length === 1);
    h.sessions.get(healthy)!.complete("healthy reply");
    await h.reply("healthy reply", healthy);
    await h.bridge.stop();

    const saved = JSON.parse(await readFile(statePath, "utf8"));
    assert.deepEqual(saved.bindings.find((entry: Binding) => entry.id === binding.id), binding);
    assert.deepEqual(saved.admissions, []);
    assert.equal(h.sessions.get(healthy)!.stopped, true);
  } finally {
    await h.bridge.stop();
    await rm(directory, { recursive: true, force: true });
  }
});

for (const classified of [false, true]) {
  test(`Workspace retains ${classified ? "classified" : "unclassified"} startup uncertainty without reopening`, { timeout: 15000 }, async () => {
    const directory = await mkdtemp(join(tmpdir(), "omp-workspace-startup-"));
    const h = new Harness(join(directory, "state.json"));
    h.openSession = async () => {
      const error = new Error("startup child shutdown is uncertain");
      throw classified ? new SessionLifecycleError(error, false) : error;
    };
    try {
      await h.bindWorkspace();
      const thread = await h.create();
      await h.command("first request", "startup child shutdown is uncertain", thread);
      await h.command("second request", "startup child shutdown is uncertain", thread);
      assert.equal(h.opened.length, 1);
      await assert.rejects(h.bridge.stop(), /ownership must be retained/);
    } finally {
      await h.bridge.stop().catch(() => undefined);
      await rm(directory, { recursive: true, force: true });
    }
  });
}

for (const cleanup of ["clean", "exited", "uncertain"] as const) {
  test(`Workspace latches validation failure with ${cleanup} child cleanup`, { timeout: 15000 }, async () => {
    const directory = await mkdtemp(join(tmpdir(), "omp-workspace-validation-"));
    const h = new Harness(join(directory, "state.json"));
    let stops = 0;
    h.openSession = async (binding) => {
      const port = new FakeSession(binding, () => {});
      port.state = async () => ({ sessionId: "invalid", sessionFile: "relative.jsonl" });
      port.stop = async () => {
        stops++;
        if (cleanup === "exited") throw new SessionLifecycleError(new Error("child exited unsuccessfully"), true);
        if (cleanup === "uncertain") throw new Error("child still draining");
      };
      return port;
    };
    try {
      await h.bindWorkspace();
      const thread = await h.create();
      const failure = cleanup === "uncertain" ? "shutdown is uncertain" : "OMP did not return";
      await h.command("first request", failure, thread);
      await h.command("second request", failure, thread);
      assert.equal(h.opened.length, 1);
      assert.equal(stops, 1, "failed opening cleanup must not be retried");
      if (cleanup === "uncertain") await assert.rejects(h.bridge.stop(), /ownership must be retained/);
      else await h.bridge.stop();
    } finally {
      await h.bridge.stop().catch(() => undefined);
      await rm(directory, { recursive: true, force: true });
    }
  });
}

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
    await h.dispatch("initialize", thread);
    await h.until(() => h.sessions.get(thread)?.prompts.length === 1);
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
