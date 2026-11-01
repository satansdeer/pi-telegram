/**
 * Standalone OMP Workspace forum bridge.
 * Zones: owner admission, durable topic/session identity, RPC UI, exact Telegram delivery.
 * Owns no polling or API transport. Topics and transcripts are never deleted or replaced.
 */
import { randomBytes, randomUUID } from "node:crypto";
import { chmod, lstat, mkdir, open, readFile, rename, unlink } from "node:fs/promises";
import { dirname, isAbsolute, resolve } from "node:path";
import { getTelegramTargetThreadParams } from "../lib/target.ts";
import type { TelegramTarget } from "../lib/target.ts";

export interface Binding {
  id: string;
  threadId: number;
  name: string;
  cwd: string;
  sessionId?: string;
  sessionFile?: string;
}
export interface UiRequest {
  id: string;
  method: string;
  [key: string]: unknown;
}
export interface SessionPort {
  prompt(text: string): Promise<string>;
  abort(): Promise<void>;
  state(): Promise<Record<string, unknown>>;
  answer(id: string, response: Record<string, unknown>): Promise<void>;
  stop(): Promise<void>;
}
type Api = (method: string, params: Record<string, unknown>) => Promise<unknown>;
type OpenSession = (binding: Binding, onUi: (request: UiRequest) => void) => Promise<SessionPort>;
type Options = { statePath: string; cwd: string; ownerId: number; botId: number; botUsername: string };
type Admission = {
  updateId: number;
  target: TelegramTarget;
  kind: string;
  phase: "admitted" | "running" | "delivery" | "report";
  diagnostic?: string;
};
type State = {
  version: 1;
  ownerId: number;
  botId: number;
  offset: number;
  groupId?: number;
  bindings: Binding[];
  admissions: Admission[];
};
type Command = { name: string; argument: string; otherBot: boolean };

const HELP = [
  "/workspace — bind this forum supergroup once (bot must manage topics)",
  "/new [name] — create a new topic and designated OMP transcript",
  "/bind [name] — adopt this existing non-General topic",
  "/sessions — list chat IDs, Telegram topics, and OMP sessions",
  "/status — show this topic's session and pending UI requests",
  "/abort — interrupt this topic's active turn",
  "/answer <request-id> <JSON object> — answer this topic's pending UI request",
  "/cancel <request-id> — cancel this topic's pending UI request",
  "/help — show these commands",
  "Only the configured owner can send input. Send plain text inside a bound topic; attachments and other slash commands are not forwarded.",
].join("\n");
const DIALOGS: Record<string, true> = { confirm: true, select: true, input: true, editor: true, ask: true };
const ATTACHMENTS = ["photo", "animation", "audio", "document", "video", "video_note", "voice", "sticker", "contact", "location", "venue", "poll", "dice", "paid_media"];

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function integer(value: unknown, minimum = 1): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= minimum;
}
function chatId(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value !== 0;
}
function text(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && !value.includes("\0");
}
function keys(value: Record<string, unknown>, allowed: string[]): boolean {
  return Object.keys(value).every((key) => allowed.includes(key));
}
function diagnostic(error: unknown): string {
  return (error instanceof Error ? error.message : "Unknown processing error")
    .replace(/\b\d{5,}:[A-Za-z0-9_-]{20,}\b/g, "[redacted bot token]").slice(0, 600);
}
function targetOf(message: Record<string, unknown>): TelegramTarget | undefined {
  if (!record(message.chat) || !chatId(message.chat.id)) return undefined;
  if (message.message_thread_id !== undefined && !integer(message.message_thread_id)) return undefined;
  return message.message_thread_id === undefined
    ? { chatId: message.chat.id }
    : { chatId: message.chat.id, threadId: message.message_thread_id as number };
}
function parseState(value: unknown, options: Options): State {
  const invalid = () => { throw new Error("Invalid Workspace state; refusing to reset or replay it"); };
  if (!record(value) || !keys(value, ["version", "ownerId", "botId", "offset", "groupId", "bindings", "admissions"])) return invalid();
  if (value.version !== 1 || !integer(value.ownerId) || !integer(value.botId) || !integer(value.offset, 0)) return invalid();
  if (value.ownerId !== options.ownerId || value.botId !== options.botId) throw new Error("Workspace state belongs to a different bot or owner");
  if (value.groupId !== undefined && (!chatId(value.groupId) || value.groupId >= 0)) return invalid();
  if (!Array.isArray(value.bindings) || !Array.isArray(value.admissions)) return invalid();
  const ids = new Set<string>();
  const threads = new Set<number>();
  const sessionIds = new Set<string>();
  const sessionFiles = new Set<string>();
  for (const binding of value.bindings) {
    if (!record(binding) || !keys(binding, ["id", "threadId", "name", "cwd", "sessionId", "sessionFile"])) return invalid();
    if (typeof binding.id !== "string" || !/^[a-f0-9]{12}$/.test(binding.id) || !integer(binding.threadId, 2)
      || !text(binding.name) || [...binding.name].length > 128 || !text(binding.cwd) || !isAbsolute(binding.cwd)
      || ids.has(binding.id) || threads.has(binding.threadId)) return invalid();
    if ((binding.sessionId === undefined) !== (binding.sessionFile === undefined)) return invalid();
    if (binding.sessionId !== undefined && (!text(binding.sessionId) || !text(binding.sessionFile) || !isAbsolute(binding.sessionFile))) return invalid();
    if (binding.sessionId !== undefined) {
      if (sessionIds.has(binding.sessionId) || sessionFiles.has(resolve(binding.sessionFile as string))) return invalid();
      sessionIds.add(binding.sessionId);
      sessionFiles.add(resolve(binding.sessionFile as string));
    }
    ids.add(binding.id);
    threads.add(binding.threadId);
  }
  if (value.bindings.length && value.groupId === undefined) return invalid();
  const updates = new Set<number>();
  for (const admission of value.admissions) {
    if (!record(admission) || !keys(admission, ["updateId", "target", "kind", "phase", "diagnostic"])) return invalid();
    if (!integer(admission.updateId, 0) || admission.updateId >= value.offset || updates.has(admission.updateId)
      || !record(admission.target) || !keys(admission.target, ["chatId", "threadId"]) || !chatId(admission.target.chatId)
      || (admission.target.threadId !== undefined && !integer(admission.target.threadId)) || !text(admission.kind)
      || !["admitted", "running", "delivery", "report"].includes(admission.phase as string)
      || (admission.diagnostic !== undefined && !text(admission.diagnostic))) return invalid();
    updates.add(admission.updateId);
  }
  return value as unknown as State;
}

/** Validate the UI protocol, including exact ask/select choices, before crossing RPC. */
function validateAnswer(request: UiRequest, response: unknown): asserts response is Record<string, unknown> {
  if (!record(response)) throw new Error("Answer must be a JSON object");
  if (request.secret === true) throw new Error("Secret input is unsupported over Telegram; cancel this request and use the local terminal");
  const one = Object.keys(response).length === 1;
  if (response.cancelled === true && one) return;
  if (request.method === "confirm" && one && typeof response.confirmed === "boolean") return;
  if (["select", "input", "editor"].includes(request.method) && one && typeof response.value === "string") {
    if (request.method !== "select" || (Array.isArray(request.options) && request.options.includes(response.value))) return;
  }
  if (request.method === "ask" && one && Array.isArray(response.answers) && Array.isArray(request.questions)
    && response.answers.length === request.questions.length) {
    const valid = response.answers.every((answer, index) => {
      const question: unknown = (request.questions as unknown[])[index];
      if (!record(answer) || !keys(answer, ["id", "selectedOptions", "customInput"]) || !record(question)
        || answer.id !== question.id || !Array.isArray(answer.selectedOptions) || !Array.isArray(question.options)
        || (answer.customInput !== undefined && typeof answer.customInput !== "string")) return false;
      const choices = question.options.filter(record).map((option) => option.label);
      if (!answer.selectedOptions.every((option) => typeof option === "string" && choices.includes(option))
        || new Set(answer.selectedOptions).size !== answer.selectedOptions.length) return false;
      return question.multi === true || (answer.selectedOptions.length <= 1
        && !(answer.selectedOptions.length && typeof answer.customInput === "string" && answer.customInput.trim()));
    });
    if (valid) return;
  }
  throw new Error(`Invalid ${request.method} answer; use the request's exact options and protocol fields (value, confirmed, answers, or cancelled:true)`);
}

export class WorkspaceBridge {
  private readonly options: Options;
  private readonly api: Api;
  private readonly openSession: OpenSession;
  private data: State;
  private initialized = false;
  private stopping = false;
  private fatal?: Error;
  private admitting: Promise<void> = Promise.resolve();
  private writing: Promise<void> = Promise.resolve();
  private readonly jobs = new Set<Promise<void>>();
  private readonly sessions = new Map<string, Promise<SessionPort>>();
  private readonly pending = new Map<string, Map<string, UiRequest>>();
  private readonly busy = new Set<number>();
  private groupSetup = false;

  constructor(options: Options, api: Api, openSession: OpenSession) {
    if (!integer(options.ownerId) || !integer(options.botId) || !/^[A-Za-z0-9_]+$/.test(options.botUsername)) throw new Error("Invalid Workspace bot/owner identity");
    this.options = { ...options, statePath: resolve(options.statePath), cwd: resolve(options.cwd) };
    this.api = api;
    this.openSession = openSession;
    this.data = { version: 1, ownerId: options.ownerId, botId: options.botId, offset: 0, bindings: [], admissions: [] };
  }
  get offset(): number { return this.data.offset; }

  async init(): Promise<void> {
    if (this.initialized) throw new Error("Workspace already initialized");
    const directory = dirname(this.options.statePath);
    await mkdir(directory, { recursive: true, mode: 0o700 });
    if (!(await lstat(directory)).isDirectory()) throw new Error("Workspace state directory must be a real directory");
    await chmod(directory, 0o700);
    let source: string | undefined;
    try {
      const info = await lstat(this.options.statePath);
      if (!info.isFile() || info.isSymbolicLink()) throw new Error("Workspace state must be a regular file");
      await chmod(this.options.statePath, 0o600);
      source = await readFile(this.options.statePath, "utf8");
    } catch (error) {
      if (!(record(error) && error.code === "ENOENT")) throw error;
    }
    if (source !== undefined) {
      let parsed: unknown;
      try { parsed = JSON.parse(source); } catch { throw new Error("Malformed Workspace state JSON; refusing to reset or replay it"); }
      this.data = parseState(parsed, this.options);
    } else await this.save();
    // An admission is durable before any external effect. We can never know which
    // side of a crash a Telegram/OMP commit reached: report, never replay it.
    for (const admission of [...this.data.admissions]) {
      const notice = admission.diagnostic ?? `Update ${admission.updateId} was interrupted during ${admission.kind} (${admission.phase}). Its outcome may be ambiguous. It will not be replayed; inspect the topic and OMP transcript before sending again. Telegram, OMP, and local state cannot commit exactly once together.`;
      admission.phase = "report";
      admission.diagnostic = notice;
      await this.save();
      try {
        await this.send(admission.target, notice);
        await this.complete(admission);
      } catch (error) {
        console.error(`Workspace recovery delivery failed for update ${admission.updateId}: ${diagnostic(error)}`);
      }
    }
    this.initialized = true;
  }

  /** Admit durably, then run turns in the background so controls stay responsive. */
  handle(update: unknown): Promise<void> {
    const next = this.admitting.then(() => this.admit(update));
    this.admitting = next.catch(() => {});
    return next;
  }
  private async admit(update: unknown): Promise<void> {
    if (!this.initialized || this.stopping) throw new Error("Workspace is not accepting updates");
    if (this.fatal) throw this.fatal;
    if (!record(update) || !integer(update.update_id, 0) || update.update_id === Number.MAX_SAFE_INTEGER) throw new Error("Invalid Telegram update_id");
    if (update.update_id < this.data.offset) return;
    this.data.offset = update.update_id + 1;
    const message = record(update.message) ? update.message : undefined;
    const target = message && targetOf(message);
    if (!message || !target || !(typeof message.text === "string" || ATTACHMENTS.some((key) => message[key] !== undefined))) {
      await this.save();
      return;
    }
    const input = typeof message.text === "string" ? message.text : "";
    const commandInput = input.trimStart();
    const match = /^\/([A-Za-z0-9_]+)(?:@([A-Za-z0-9_]+))?(?:\s+([\s\S]*))?$/.exec(commandInput);
    const command = commandInput.startsWith("/") ? {
      name: match?.[1]?.toLowerCase() ?? "unknown",
      argument: match?.[3]?.trim() ?? "",
      otherBot: Boolean(match?.[2] && match[2].toLowerCase() !== this.options.botUsername.toLowerCase()),
    } : undefined;
    if (command?.otherBot) { await this.save(); return; }
    const owner = record(message.from) && message.from.id === this.options.ownerId && message.sender_chat === undefined;
    let refusal = owner ? undefined : "Owner only: this bot does not accept input or controls from other group members.";
    let reserved = false;
    let setup = false;
    if (owner && !command && input.trim() && target.chatId === this.data.groupId && target.threadId !== undefined
      && this.data.bindings.some((binding) => binding.threadId === target.threadId)) {
      if (this.busy.has(target.threadId)) refusal = "This topic is busy. Use /status or /abort; input was not queued.";
      else { this.busy.add(target.threadId); reserved = true; }
    }
    if (owner && command?.name === "bind" && target.chatId === this.data.groupId && target.threadId !== undefined && target.threadId > 1
      && !this.data.bindings.some((binding) => binding.threadId === target.threadId)) {
      if (this.busy.has(target.threadId)) refusal = "This topic is busy; its binding is already being initialized.";
      else { this.busy.add(target.threadId); reserved = true; }
    }
    if (owner && command?.name === "workspace" && this.data.groupId === undefined) {
      if (this.groupSetup) refusal = "Workspace binding is already in progress.";
      else { this.groupSetup = true; setup = true; }
    }
    const admission: Admission = { updateId: update.update_id, target, kind: refusal ? "refusal" : command?.name ?? (input ? "prompt" : "attachment"), phase: "admitted" };
    this.data.admissions.push(admission);
    await this.save();
    this.track(this.process(admission, message, command, refusal).finally(() => {
      if (reserved && target.threadId !== undefined) this.busy.delete(target.threadId);
      if (setup) this.groupSetup = false;
    }));
  }

  private async process(admission: Admission, message: Record<string, unknown>, command?: Command, refusal?: string): Promise<void> {
    try {
      if (refusal) await this.reply(admission, refusal);
      else if (command) await this.command(admission, command);
      else if (typeof message.text !== "string") await this.reply(admission, "Attachments are unsupported. Send a plain-text message inside a bound topic.");
      else if (!message.text.trim()) await this.reply(admission, "Send a non-empty plain-text message.");
      else {
        this.requireGroup(admission.target);
        const binding = this.requireBinding(admission.target);
        const session = await this.session(binding);
        await this.assertIdentity(binding, session);
        admission.phase = "running";
        await this.save();
        const output = await session.prompt(message.text);
        await this.reply(admission, output || "OMP completed without a text reply.");
      }
      await this.complete(admission);
    } catch (error) {
      const delivery = admission.phase === "delivery";
      admission.phase = "report";
      admission.diagnostic = delivery
        ? `Reply delivery or completion commit failed for update ${admission.updateId}. Handling/model execution will not be repeated to deliver it. Inspect this topic's designated OMP transcript. ${diagnostic(error)}`
        : `Update ${admission.updateId} (${admission.kind}) failed: ${diagnostic(error)}. It will not be replayed; any started operation may have an ambiguous outcome. Inspect this topic/transcript before sending again.`;
      console.error(`Workspace: ${admission.diagnostic}`);
      try {
        await this.save();
        if (!delivery) {
          await this.send(admission.target, admission.diagnostic);
          await this.complete(admission);
        }
      } catch (reportError) {
        console.error(`Workspace failure report could not be committed/delivered: ${diagnostic(reportError)}`);
      }
    }
  }

  private async command(admission: Admission, command: Command): Promise<void> {
    const target = admission.target;
    if (command.name === "help") { await this.reply(admission, HELP); return; }
    if (command.name === "workspace") {
      if (this.data.groupId !== undefined) {
        if (target.chatId !== this.data.groupId) throw new Error("Workspace is already bound to a different group; rebinding is refused");
        await this.reply(admission, `Workspace is bound to group ${this.data.groupId}. Use /new [name] or /bind in an existing topic.`);
        return;
      }
      const chat = await this.api("getChat", { chat_id: target.chatId });
      if (!record(chat) || chat.id !== target.chatId || chat.type !== "supergroup" || chat.is_forum !== true || target.chatId >= 0) throw new Error("/workspace requires an is_forum supergroup");
      const member = await this.api("getChatMember", { chat_id: target.chatId, user_id: this.options.botId });
      if (!record(member) || !record(member.user) || member.user.id !== this.options.botId
        || member.status !== "administrator" || member.can_manage_topics !== true) throw new Error("Bot must be an administrator with can_manage_topics");
      this.data.groupId = target.chatId;
      await this.save();
      await this.reply(admission, "Workspace bound. Use /new [name] to create a chat topic, or /bind inside an existing non-General topic.");
      return;
    }
    if (!["new", "bind", "sessions", "status", "abort", "answer", "cancel"].includes(command.name)) throw new Error("Unsupported slash command. Use /help; slash commands are never forwarded to OMP");
    this.requireGroup(target);
    if (command.name === "new" || command.name === "bind") {
      if (command.name === "bind") {
        if (!integer(target.threadId, 2)) throw new Error("/bind must be sent inside an existing non-General topic");
        const existing = this.data.bindings.find((binding) => binding.threadId === target.threadId);
        if (existing) { await this.reply(admission, `Already bound; the designated session will not be replaced.\n${this.describe(existing)}`); return; }
      }
      let id: string;
      do { id = randomBytes(6).toString("hex"); } while (this.data.bindings.some((binding) => binding.id === id));
      const prefix = `[${id}] `;
      const name = [...(command.argument.replace(/\s+/g, " ").trim() || "Chat")].slice(0, 128 - prefix.length).join("");
      let threadId = target.threadId;
      if (command.name === "new") {
        admission.phase = "running";
        await this.save();
        // Never retry this non-idempotent method after a transport error.
        const topic = await this.api("createForumTopic", { chat_id: target.chatId, name: `${prefix}${name}` });
        if (!record(topic) || !integer(topic.message_thread_id, 2)) throw new Error("createForumTopic returned an invalid topic ID; inspect the group before retrying");
        threadId = topic.message_thread_id;
        if (this.data.bindings.some((binding) => binding.threadId === threadId)) throw new Error("Created topic is already bound; refusing to replace its session");
      }
      const binding: Binding = { id, threadId: threadId!, name, cwd: this.options.cwd };
      this.data.bindings.push(binding);
      this.busy.add(binding.threadId);
      await this.save();
      try {
        await this.session(binding, true);
        await this.reply(admission, `Chat created.\n${this.describe(binding)}`, { chatId: target.chatId, threadId: binding.threadId });
      } finally {
        this.busy.delete(binding.threadId);
      }
      return;
    }
    if (command.name === "sessions") {
      await this.reply(admission, this.data.bindings.length ? this.data.bindings.map((binding) => this.describe(binding)).join("\n\n") : "No chats yet. Use /new [name] or /bind in a topic.");
      return;
    }
    const binding = this.requireBinding(target);
    if (command.name === "status") {
      const requests = [...(this.pending.get(binding.id)?.keys() ?? [])];
      await this.reply(admission, `${this.describe(binding)}\n${this.busy.has(binding.threadId) ? "Busy" : "Idle"}\nPending UI: ${requests.join(", ") || "none"}`);
      return;
    }
    if (command.name === "abort") {
      const live = this.sessions.get(binding.id);
      if (live) { await (await live).abort(); await this.reply(admission, "Abort requested for this topic only."); }
      else await this.reply(admission, "No live OMP process in this topic.");
      return;
    }
    const match = /^(\S+)(?:\s+([\s\S]+))?$/.exec(command.argument);
    if (!match || (command.name === "answer" ? !match[2] : Boolean(match[2]))) throw new Error(command.name === "answer" ? "Usage: /answer <request-id> <JSON object>" : "Usage: /cancel <request-id>");
    const request = this.pending.get(binding.id)?.get(match[1]!);
    if (!request) throw new Error("No such pending UI request in this exact topic");
    let response: unknown = { cancelled: true };
    if (command.name === "answer") {
      try { response = JSON.parse(match[2]!); } catch { throw new Error("Answer must contain valid JSON"); }
      validateAnswer(request, response);
    }
    const live = this.sessions.get(binding.id);
    if (!live) throw new Error("This topic has no live session for the pending UI request");
    this.pending.get(binding.id)!.delete(request.id);
    admission.phase = "running";
    await this.save();
    await (await live).answer(request.id, response as Record<string, unknown>);
    await this.reply(admission, `UI request ${request.id} ${command.name === "cancel" ? "cancelled" : "answered"}.`);
  }

  private requireGroup(target: TelegramTarget): void {
    if (this.data.groupId === undefined) throw new Error("Workspace is unbound. Send /workspace in your forum supergroup first");
    if (target.chatId !== this.data.groupId) throw new Error("Input is accepted only in the bound Workspace group");
  }
  private requireBinding(target: TelegramTarget): Binding {
    const binding = this.data.bindings.find((candidate) => candidate.threadId === target.threadId);
    if (!binding) throw new Error("This topic is not bound. Use /new [name] or /bind in a non-General topic");
    return binding;
  }
  private describe(binding: Binding): string {
    return `Chat ${binding.id}: ${binding.name}\nTelegram topic: ${binding.threadId}\nOMP session: ${binding.sessionId ?? "uninitialized (automatic replacement refused)"}\nTranscript: ${binding.sessionFile ?? "not recorded"}\nCwd: ${binding.cwd}`;
  }
  private async assertIdentity(binding: Binding, session: SessionPort): Promise<void> {
    const state = await session.state();
    if (state.sessionId !== binding.sessionId || state.sessionFile !== binding.sessionFile) throw new Error("OMP session identity differs from the designated transcript; refusing input");
  }
  private async session(binding: Binding, create = false): Promise<SessionPort> {
    const existing = this.sessions.get(binding.id);
    if (existing) return existing;
    if (this.stopping) throw new Error("Workspace is stopping");
    if (!create && (!binding.sessionId || !binding.sessionFile)) throw new Error("Session initialization was interrupted; no designated transcript was recorded. Automatic replacement is refused");
    const opening = (async () => {
      const port = await this.openSession({ ...binding }, (request) => this.onUi(binding, request));
      try {
        if (this.stopping) throw new Error("Workspace is stopping");
        const state = await port.state();
        if (!text(state.sessionId) || !text(state.sessionFile) || !isAbsolute(state.sessionFile)) throw new Error("OMP did not return a persistent sessionId and absolute sessionFile");
        if (!create && (state.sessionId !== binding.sessionId || state.sessionFile !== binding.sessionFile)) throw new Error("OMP resumed a different transcript; refusing to replace the designated session");
        if (create) {
          if (this.data.bindings.some((other) => other !== binding && (other.sessionId === state.sessionId
            || (other.sessionFile !== undefined && resolve(other.sessionFile) === resolve(state.sessionFile as string))))) {
            throw new Error("OMP returned a transcript already designated to another topic; refusing shared sessions");
          }
          binding.sessionId = state.sessionId;
          binding.sessionFile = state.sessionFile;
          await this.save();
        }
        return port;
      } catch (error) {
        try { await port.stop(); }
        catch (shutdown) { throw new AggregateError([error, shutdown], "OMP initialization failed and child shutdown is uncertain"); }
        throw error;
      }
    })();
    this.sessions.set(binding.id, opening);
    // Keep a failed opening latched: a second command must not silently create a replacement.
    return opening;
  }
  private onUi(binding: Binding, request: UiRequest): void {
    if (!text(request.id) || !text(request.method)) return;
    const target = { chatId: this.data.groupId!, threadId: binding.threadId };
    if (request.method === "cancel") {
      if (typeof request.targetId === "string") this.pending.get(binding.id)?.delete(request.targetId);
      this.track(this.send(target, `UI request ${String(request.targetId ?? request.id)} is no longer pending.`).catch((error) => console.error(`Workspace UI delivery failed: ${diagnostic(error)}`)));
      return;
    }
    if (Object.hasOwn(DIALOGS, request.method)) {
      let pending = this.pending.get(binding.id);
      if (!pending) { pending = new Map(); this.pending.set(binding.id, pending); }
      if (pending.has(request.id)) return;
      pending.set(request.id, request);
      const details = request.secret === true ? "Secret input is unsupported over Telegram; use /cancel and the local terminal."
        : [request.title, request.message, request.options && JSON.stringify(request.options), request.questions && JSON.stringify(request.questions)].filter((part) => typeof part === "string").join("\n");
      this.track(this.send(target, `UI ${request.method} request ${request.id}\n${details}\n/answer ${request.id} <JSON object>\n/cancel ${request.id}`).catch((error) => console.error(`Workspace UI delivery failed: ${diagnostic(error)}`)));
    } else if (request.method === "notify" && typeof request.message === "string") {
      this.track(this.send(target, `OMP: ${request.message}`).catch((error) => console.error(`Workspace UI delivery failed: ${diagnostic(error)}`)));
    }
  }

  private async reply(admission: Admission, body: string, target = admission.target): Promise<void> {
    admission.phase = "delivery";
    await this.save();
    await this.send(target, body);
  }
  private async send(target: TelegramTarget, body: string): Promise<void> {
    // Conservative UTF-16 chunks also fit Telegram's 4096-character ceiling.
    for (let start = 0; start < body.length;) {
      let end = Math.min(start + 4096, body.length);
      if (end < body.length && /[\uD800-\uDBFF]/.test(body[end - 1]!)) end--;
      await this.api("sendMessage", { chat_id: target.chatId, ...getTelegramTargetThreadParams(target), text: body.slice(start, end) });
      start = end;
    }
  }
  private async complete(admission: Admission): Promise<void> {
    this.data.admissions = this.data.admissions.filter((entry) => entry !== admission);
    await this.save();
  }
  private save(): Promise<void> {
    const source = JSON.stringify(this.data, null, 2) + "\n";
    const saved = this.writing.then(async () => {
      const temporary = `${this.options.statePath}.${randomUUID()}.tmp`;
      try {
        const file = await open(temporary, "wx", 0o600);
        try { await file.writeFile(source, "utf8"); await file.sync(); } finally { await file.close(); }
        await rename(temporary, this.options.statePath);
        const directory = await open(dirname(this.options.statePath), "r");
        try { await directory.sync(); } finally { await directory.close(); }
      } catch (error) {
        await unlink(temporary).catch(() => {});
        throw error;
      }
    });
    this.writing = saved;
    return saved.catch((error: unknown) => {
      this.fatal = new Error(`Workspace state persistence failed; refusing further input: ${diagnostic(error)}`);
      throw this.fatal;
    });
  }
  private track(job: Promise<void>): void {
    this.jobs.add(job);
    void job.finally(() => this.jobs.delete(job)).catch((error) => console.error(`Workspace background failure: ${diagnostic(error)}`));
  }
  async stop(): Promise<void> {
    this.stopping = true;
    await this.admitting;
    const opened = await Promise.allSettled([...this.sessions.values()]);
    const shutdowns = await Promise.allSettled(opened.flatMap((result) => result.status === "fulfilled" ? [result.value.stop()] : []));
    const failures = [...opened, ...shutdowns].filter((result) => result.status === "rejected").map((result) => result.reason);
    if (failures.length) throw new AggregateError(failures, "OMP child shutdown is uncertain; supervisor ownership must be retained");
    while (this.jobs.size) await Promise.allSettled([...this.jobs]);
    await this.writing;
  }
}
