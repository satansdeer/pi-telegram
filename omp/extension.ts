/**
 * Registers the current OMP terminal for Telegram discovery without spawning an agent.
 * Zones: native lifecycle, pure registration, authorized attachment, terminal turn ownership.
 */
import { attachmentValid, connectWorkspace } from "./local.ts";
import type { Attachment, LocalHandler, LocalPeer } from "./local.ts";

type Identity = { sessionId: string; sessionFile: string; cwd: string };
type Message = { role?: unknown; content?: unknown; stopReason?: unknown; errorMessage?: unknown };
type Events = {
  agent_start: { type: "agent_start" };
  agent_end: { type: "agent_end"; messages: unknown[]; willContinue?: boolean };
  turn_end: { type: "turn_end"; message: unknown };
  input: { type: "input"; text: string; source: "interactive" | "rpc" | "extension" };
  session_start: unknown;
  session_before_switch: unknown;
  session_switch: unknown;
  session_before_branch: unknown;
  session_branch: unknown;
  session_before_tree: unknown;
  session_tree: unknown;
  session_shutdown: unknown;
};

// The small, structural subset of the native OMP v18.13.3 extension API used here.
export interface TerminalContext {
  cwd: string;
  agent?: { kind: "main" | "sub" };
  sessionManager: { getSessionId(): string; getSessionFile(): string | undefined };
  isIdle(): boolean;
  hasPendingMessages(): boolean;
  abort(): void;
  runEphemeralTurn?(options: { promptText: string }): Promise<{ replyText: string }>;
  ui: { notify(message: string, type?: "info" | "warning" | "error"): void };
}

export interface TerminalHost {
  registerCommand(name: string, options: { description?: string; handler(args: string, ctx: TerminalContext): Promise<void> }): void;
  sendUserMessage(text: string): void;
  on<K extends keyof Events>(event: K, handler: (event: Events[K], ctx: TerminalContext) => void | { handled: true }): void;
}

type Connection = {
  generation: number; identity: Identity; closed: boolean; peer?: LocalPeer;
  attachment?: Attachment; ready?: Promise<Connection>; attaching?: Promise<void>;
};
type Turn = {
  generation: number;
  identity: Identity;
  started: boolean;
  invalidated: boolean;
  message?: Message;
  resolve(text: string): void;
  reject(error: Error): void;
};

function errorOf(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function identityOf(ctx: TerminalContext): Identity {
  if (ctx.agent && ctx.agent.kind !== "main") throw new Error("Telegram can attach only to the main OMP terminal session.");
  const sessionId = ctx.sessionManager.getSessionId();
  const sessionFile = ctx.sessionManager.getSessionFile();
  if (!sessionId || !sessionFile || !ctx.cwd) throw new Error("Telegram requires a persistent OMP session with a session file.");
  return { sessionId, sessionFile, cwd: ctx.cwd };
}

function sameIdentity(left: Identity, right: Identity): boolean {
  return left.sessionId === right.sessionId && left.sessionFile === right.sessionFile && left.cwd === right.cwd;
}

function assistant(value: unknown): Message | undefined {
  return value !== null && typeof value === "object" && (value as Message).role === "assistant" ? value as Message : undefined;
}

function textOf(message: Message | undefined): string {
  if (typeof message?.content === "string") return message.content;
  if (!Array.isArray(message?.content)) return "";
  return message.content.filter((part): part is { type: "text"; text: string } =>
    part !== null && typeof part === "object" && part.type === "text" && typeof part.text === "string")
    .map(part => part.text).join("");
}

function parameters(params: Record<string, unknown>, keys: string[]): void {
  if (Object.keys(params).some(key => !keys.includes(key))) throw new Error("Unexpected Telegram request parameters.");
}

function btwPrompt(question: string): string {
  return `<btw>
Ephemeral side question for current interactive session.
Answer briefly, directly; use conversation context already provided.
NEVER use tools.
NEVER ask follow-up questions.
Question:
${question}
</btw>`;
}

export default function telegramExtension(pi: TerminalHost, connect: typeof connectWorkspace = connectWorkspace): void {
  let context: TerminalContext | undefined;
  let connection: Connection | undefined;
  let pending: Turn | undefined;
  let generation = 0;
  let sidecar = false;
  let activeOperation = false;
  let optedOut = false;
  let suspended = false;
  let stopped = false;
  let retryDelay = 1_000;
  let retryTimer: NodeJS.Timeout | undefined;

  function clearRetry(): void {
    clearTimeout(retryTimer);
    retryTimer = undefined;
  }

  function canDiscover(): boolean {
    return !!context && context.agent?.kind !== "sub" && process.env.OMP_WORKSPACE_MANAGED !== "1"
      && !optedOut && !suspended && !stopped;
  }

  function scheduleRegistration(): void {
    if (!canDiscover() || connection || retryTimer) return;
    retryTimer = setTimeout(() => { retryTimer = undefined; discover(); }, retryDelay);
    retryTimer.unref?.();
    retryDelay = Math.min(retryDelay * 2, 30_000);
  }

  function disconnect(reason: string, notify = false): void {
    const previous = connection;
    if (!previous) return;
    connection = undefined;
    previous.closed = true;
    if (pending?.generation === previous.generation) pending.reject(new Error(reason));
    previous.peer?.close();
    if (notify) context?.ui.notify(reason, "info");
  }

  function observe(ctx: TerminalContext): void {
    context = ctx;
    let identity: Identity;
    try { identity = identityOf(ctx); } catch (error) {
      if (pending) { pending.invalidated = true; pending.reject(errorOf(error)); }
      disconnect(errorOf(error).message);
      return;
    }
    if (pending && !sameIdentity(pending.identity, identity)) {
      pending.invalidated = true;
      pending.reject(new Error("OMP session identity changed during the Telegram turn."));
    }
    if (connection && !sameIdentity(connection.identity, identity)) {
      disconnect("Telegram disconnected because the OMP session identity changed.");
      scheduleRegistration();
    }
  }

  function currentContext(current: Connection): TerminalContext {
    if (!context) throw new Error("OMP terminal context is unavailable.");
    observe(context);
    if (connection !== current || current.closed) throw new Error("Telegram connection is closed; use /telegram-connect again.");
    return context;
  }

  function acceptAttachment(current: Connection, value: unknown): void {
    currentContext(current);
    if (!attachmentValid(value)) throw new Error("Invalid Workspace attachment response");
    if (!sameIdentity(value, current.identity)) throw new Error("Telegram Workspace returned a different OMP session identity.");
    const previous = current.attachment;
    current.attachment = value;
    if (!previous || previous.id !== value.id || previous.threadId !== value.threadId || previous.name !== value.name) {
      context!.ui.notify(`Telegram connected: ${value.name} (topic ${value.threadId}). This terminal remains the active OMP session.`, "info");
    }
  }

  function handler(current: Connection): LocalHandler {
    return async (method, params) => {
      const ctx = currentContext(current);
      if (method === "state") {
        parameters(params, []);
        return { ...identityOf(ctx), isStreaming: !!pending || sidecar || activeOperation || !ctx.isIdle() || ctx.hasPendingMessages() };
      }
      if (method === "attached") { acceptAttachment(current, params); return; }
      if (method === "answer") throw new Error("Telegram /answer is unsupported for attached terminal sessions. Answer interactive dialogs in the local OMP terminal.");
      if (!current.attachment) throw new Error("Telegram terminal is not attached to a topic.");
      if (method === "abort") {
        parameters(params, []);
        ctx.abort();
        return;
      }
      if (method === "btw") {
        parameters(params, ["text"]);
        if (typeof params.text !== "string" || !params.text.trim()) throw new Error("Telegram /btw question must not be empty.");
        if (!ctx.runEphemeralTurn) throw new Error("This OMP terminal does not support ephemeral /btw turns.");
        if (sidecar) throw new Error("An OMP /btw question is already running.");
        sidecar = true;
        try {
          const result = await ctx.runEphemeralTurn({ promptText: btwPrompt(params.text) });
          currentContext(current);
          if (typeof result.replyText !== "string") throw new Error("OMP /btw returned an invalid response.");
          return result.replyText;
        } finally {
          sidecar = false;
        }
      }
      if (method !== "prompt") throw new Error(`Unsupported Telegram terminal method: ${method}`);
      parameters(params, ["text"]);
      if (typeof params.text !== "string" || !params.text.trim()) throw new Error("Telegram prompt text must not be empty.");
      if (pending || activeOperation || !ctx.isIdle() || ctx.hasPendingMessages()) {
        throw new Error("OMP terminal is busy; wait for its current turn and queued messages to finish.");
      }
      const result = Promise.withResolvers<string>();
      const turn: Turn = { ...result, generation: current.generation, identity: current.identity, started: false, invalidated: false };
      // Reserve synchronously: sendUserMessage returns void and may start asynchronously.
      pending = turn;
      try { pi.sendUserMessage(params.text); } catch (error) {
        if (!turn.started && pending === turn) pending = undefined;
        turn.reject(errorOf(error));
      }
      return result.promise;
    };
  }

  function register(): Promise<Connection> {
    if (process.env.OMP_WORKSPACE_MANAGED === "1") throw new Error("Telegram discovery is disabled for Workspace-managed OMP sessions.");
    if (!context) throw new Error("OMP terminal context is unavailable.");
    const identity = identityOf(context);
    if (connection) return connection.ready!;
    clearRetry();
    const current: Connection = { generation: ++generation, identity, closed: false };
    connection = current;
    current.ready = connect(handler(current), () => {
      if (connection !== current) return;
      const attached = !!current.attachment;
      disconnect("Telegram Workspace connection closed. The terminal is still running; only discovery will reconnect automatically.", attached);
      scheduleRegistration();
    }).then(peer => {
      if (context) observe(context);
      if (connection !== current || current.closed) { peer.close(); throw new Error("Telegram connection is closed."); }
      current.peer = peer;
      retryDelay = 1_000;
      return current;
    }).catch(error => {
      if (connection === current) disconnect(`Telegram registration failed: ${errorOf(error).message}`);
      scheduleRegistration();
      throw error;
    });
    return current.ready;
  }

  function discover(): void {
    if (!canDiscover()) return;
    try { void register().catch(() => {}); }
    catch { scheduleRegistration(); }
  }

  pi.registerCommand("telegram-connect", {
    description: "Attach this OMP terminal session to Telegram Workspace: /telegram-connect [name]",
    async handler(name, ctx) {
      observe(ctx);
      optedOut = false;
      suspended = false;
      try {
        if (stopped) throw new Error("OMP terminal is shutting down.");
        const current = await register();
        currentContext(current);
        if (current.attachment) {
          ctx.ui.notify(`Telegram already connected: ${current.attachment.name} (topic ${current.attachment.threadId}).`, "info");
          return;
        }
        current.attaching ??= (async () => {
          const attachment = await current.peer!.request("attach", { name: name.trim() });
          currentContext(current);
          if (!attachmentValid(attachment) || !sameIdentity(attachment, current.identity)) {
            throw new Error("Telegram Workspace returned an invalid attachment or different OMP session identity.");
          }
          const authorized = current.attachment;
          if (!authorized || authorized.id !== attachment.id || authorized.threadId !== attachment.threadId
            || authorized.groupId !== attachment.groupId || authorized.name !== attachment.name) {
            throw new Error("Telegram Workspace did not authorize this terminal attachment.");
          }
        })().finally(() => { current.attaching = undefined; });
        await current.attaching;
      } catch (error) { ctx.ui.notify(`Telegram connection failed: ${errorOf(error).message}`, "error"); }
    },
  });

  pi.registerCommand("telegram-disconnect", {
    description: "Detach Telegram and disable discovery until /telegram-connect or a new session lifecycle",
    async handler(_args, ctx) {
      observe(ctx);
      optedOut = true;
      clearRetry();
      const registered = !!connection;
      disconnect("Telegram disconnected. This OMP terminal session continues running.", true);
      if (!registered) ctx.ui.notify("Telegram is not connected. Discovery is disabled for this session.", "info");
    },
  });

  pi.on("input", (event, ctx) => {
    observe(ctx);
    if (!pending || event.source === "extension" || /^\/(?:telegram-disconnect|abort)(?:\s|$)/.test(event.text.trim())) return;
    ctx.ui.notify("A Telegram turn is pending. Use /telegram-disconnect or /abort before submitting terminal input.", "warning");
    return { handled: true };
  });

  pi.on("agent_start", (_event, ctx) => {
    observe(ctx);
    activeOperation = true;
    if (pending) pending.started = true;
  });

  pi.on("turn_end", (event, ctx) => {
    observe(ctx);
    const message = assistant(event.message);
    if (pending?.started && !pending.invalidated && message) pending.message = message;
  });

  pi.on("agent_end", (event, ctx) => {
    observe(ctx);
    if (event.willContinue) return;
    activeOperation = false;
    const turn = pending;
    if (!turn?.started) return;
    pending = undefined;
    if (turn.invalidated) return;
    // Full history is not response ownership: only fresh turn_end output can settle this turn.
    const message = turn.message;
    if (message?.stopReason === "error" || message?.stopReason === "aborted" ||
      typeof message?.errorMessage === "string" && message.errorMessage.trim()) {
      turn.reject(new Error(typeof message.errorMessage === "string" && message.errorMessage.trim()
        ? message.errorMessage : `OMP turn ${message.stopReason}.`));
    } else turn.resolve(textOf(message));
  });

  for (const event of ["session_before_switch", "session_before_branch", "session_before_tree"] as const) {
    pi.on(event, (_event, ctx) => {
      suspended = true;
      clearRetry();
      observe(ctx);
      disconnect(`Telegram disconnected before ${event.replaceAll("_", " ")}. The terminal operation is not aborted.`);
    });
  }

  for (const event of ["session_start", "session_switch", "session_branch", "session_tree"] as const) {
    pi.on(event, (_event, ctx) => {
      suspended = false;
      optedOut = false;
      retryDelay = 1_000;
      clearRetry();
      observe(ctx);
      discover();
    });
  }

  pi.on("session_shutdown", (_event, ctx) => {
    stopped = true;
    clearRetry();
    observe(ctx);
    disconnect("Telegram disconnected before session shutdown. The terminal operation is not aborted.");
  });
}
