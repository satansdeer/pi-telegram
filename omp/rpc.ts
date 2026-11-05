/**
 * Owns one persistent OMP child and its JSONL request/turn lifecycle.
 * Zones: omp rpc, process lifecycle, session persistence
 */
import { spawn } from "node:child_process";
import type { ChildProcessWithoutNullStreams } from "node:child_process";
import { chmodSync, closeSync, mkdirSync, openSync, readdirSync, readSync, statSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { createInterface } from "node:readline";

type Frame = Record<string, unknown>;
export type UiRequest = { id: string; method: string; [key: string]: unknown };
type Waiter = { resolve: (frame: Frame) => void; reject: (error: Error) => void };
type Turn = {
  id: string;
  resolve: (text: string) => void;
  reject: (error: Error) => void;
  acknowledged: boolean;
  started: boolean;
  terminal: boolean;
  settled: boolean;
  text: string;
  result?: Frame;
  failure?: Error;
};
type Sidecar = {
  id: string;
  recordId?: string;
  resolve: (text: string) => void;
  reject: (error: Error) => void;
  settled: boolean;
};
type Exit = { code: number | null; signal: NodeJS.Signals | null; error?: Error };

class OutcomeUnknownError extends Error {}

/** Carries child-ownership proof without treating a failed operation as success. */
export class SessionLifecycleError extends Error {
  readonly shutdownConfirmed: boolean;

  constructor(cause: unknown, shutdownConfirmed: boolean) {
    super(cause instanceof Error ? cause.message : String(cause), { cause });
    this.shutdownConfirmed = shutdownConfirmed;
  }
}

function record(value: unknown): value is Frame {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function lastAssistant(messages: unknown): Frame | undefined {
  if (!Array.isArray(messages)) return undefined;
  return messages.findLast((message: unknown) => record(message) && message.role === "assistant");
}

function textOf(message: Frame): string {
  if (typeof message.content === "string") return message.content;
  if (!Array.isArray(message.content)) return "";
  return message.content.filter((part: unknown) => record(part) && part.type === "text" && typeof part.text === "string")
    .map((part: Frame) => part.text).join("");
}

function errorOf(value: unknown, fallback: string): Error {
  return new Error(typeof value === "string" ? value : record(value) && typeof value.message === "string" ? value.message : fallback);
}

function deadline<T>(promise: Promise<T>, milliseconds: number, message: string): Promise<T> {
  const timeout = Promise.withResolvers<never>();
  const timer = setTimeout(() => timeout.reject(new OutcomeUnknownError(message)), milliseconds);
  return Promise.race([promise, timeout.promise]).finally(() => clearTimeout(timer));
}

// Read only the identity prefix, not a potentially enormous transcript. OMP may prepend a title entry.
function transcriptId(path: string): string {
  if (!statSync(path).isFile()) throw new Error(`OMP resume path is not a transcript file: ${path}`);
  const file = openSync(path, "r");
  try {
    const bytes = Buffer.alloc(64 * 1024);
    const prefix = bytes.subarray(0, readSync(file, bytes)).toString("utf8");
    for (const line of prefix.split("\n").slice(0, 2)) {
      let entry: unknown;
      try { entry = JSON.parse(line); } catch { break; }
      if (record(entry) && entry.type === "session" && typeof entry.id === "string" && entry.id) return entry.id;
    }
    throw new Error(`OMP resume file has no valid session identity: ${path}`);
  } finally { closeSync(file); }
}

export class OmpSession {
  private readonly child: ChildProcessWithoutNullStreams;
  private readonly pending = new Map<string, Waiter>();
  private readonly exited: Promise<Exit>;
  private readonly recovered: Map<string, string>;
  private readonly directory: string;
  private readonly onUi: (request: UiRequest) => void;
  private turn?: Turn;
  private sidecar?: Sidecar;
  private failure?: Error;
  private exit?: Exit;
  private sequence = 0;
  private streaming = false;
  private stopping = false;

  constructor(options: {
    binary: string; cwd: string; sessionDir: string; resume?: string;
    onUi: (request: UiRequest) => void; args?: string[];
  }) {
    try {
      const directory = resolve(options.cwd, options.sessionDir);
      this.directory = directory;
      mkdirSync(directory, { recursive: true, mode: 0o700 });
      chmodSync(directory, 0o700);
      this.recovered = new Map();
      const resume = options.resume === undefined ? undefined : resolve(options.cwd, options.resume);
      if (resume !== undefined) {
        this.recovered.set(resume, transcriptId(resume));
      } else {
        for (const entry of readdirSync(directory, { withFileTypes: true })) {
          if (entry.isFile() && entry.name.endsWith(".jsonl")) {
            const file = resolve(directory, entry.name);
            this.recovered.set(file, transcriptId(file));
          }
        }
      }
      const env = { ...process.env };
      for (const [key, value] of Object.entries(env)) {
        if (/TELEGRAM.*TOKEN|BOT_?TOKEN|TOKEN.*TELEGRAM/i.test(key) || /^\d+:[\w-]{20,}$/.test(value ?? "")) delete env[key];
      }
      env.OMP_WORKSPACE_MANAGED = "1";
      this.onUi = options.onUi;
      this.child = spawn(options.binary, [
        ...(options.args ?? []), "--mode", "rpc", "--session-dir", directory,
        ...(resume ? ["--resume", resume] : this.recovered.size ? ["--continue"] : []),
      ], { cwd: options.cwd, env, stdio: ["pipe", "pipe", "pipe"] });
    } catch (error) {
      throw new SessionLifecycleError(error, true);
    }
    const exited = Promise.withResolvers<Exit>();
    this.exited = exited.promise;
    this.child.once("error", (error) => this.fail(error));
    this.child.once("close", (code, signal) => {
      this.exit = { code, signal, error: this.failure };
      this.fail(new Error(`OMP RPC process exited (${signal ?? code}); unfinished outcomes are unknown`));
      exited.resolve(this.exit);
    });
    this.child.stdin.on("error", (error) => this.fail(error));
    // Drain stderr without copying model/provider diagnostics or secrets into supervisor logs.
    this.child.stderr.resume();
    this.child.stderr.on("error", (error) => this.fail(error));
    this.child.stdout.on("error", (error) => this.fail(error));
    const lines = createInterface({ input: this.child.stdout, crlfDelay: Infinity });
    lines.on("error", (error) => this.fail(error));
    lines.on("line", (line) => {
      if (!line.trim()) return;
      let frame: unknown;
      try { frame = JSON.parse(line); } catch {
        this.fail(new Error("OMP emitted invalid JSON; unfinished outcomes are unknown"));
        this.child.stdin.end();
        return;
      }
      if (!record(frame) || typeof frame.type !== "string") {
        this.fail(new Error("OMP emitted an invalid RPC frame; unfinished outcomes are unknown"));
        this.child.stdin.end();
        return;
      }
      this.handleFrame(frame);
    });
  }

  get busy(): boolean { return this.turn !== undefined || this.streaming; }

  async ready(): Promise<Record<string, unknown>> {
    try {
      const state = await this.state();
      if (typeof state.sessionId !== "string" || !state.sessionId) throw new Error("OMP did not expose a session identity");
      if (!this.recovered.size && (typeof state.sessionFile !== "string" || dirname(resolve(state.sessionFile)) !== this.directory)) {
        throw new Error("OMP did not allocate a persistent transcript in its dedicated session directory");
      }
      if (this.recovered.size && (typeof state.sessionFile !== "string" || this.recovered.get(resolve(state.sessionFile)) !== state.sessionId)) {
        throw new Error("OMP did not resume the persisted transcript; refusing a fresh replacement session");
      }
      return state;
    } catch (error) {
      try { await this.stop(); } catch (shutdown) {
        if (this.exit) throw new SessionLifecycleError(error, true);
        throw new SessionLifecycleError(new AggregateError([error, shutdown], "OMP startup failed and its child shutdown is uncertain"), false);
      }
      throw new SessionLifecycleError(error, true);
    }
  }

  async state(): Promise<Record<string, unknown>> {
    const response = await this.request("get_state");
    if (!record(response.data)) throw new Error("OMP get_state returned no state");
    return response.data;
  }

  async setModel(provider: string, modelId: string): Promise<void> {
    const response = await this.request("set_model", { provider, modelId });
    if (!record(response.data) || response.data.provider !== provider || response.data.id !== modelId) {
      throw new Error("OMP set_model did not select the requested model");
    }
  }

  async prompt(text: string): Promise<string> {
    if (this.busy) throw new Error("OMP session is already processing a prompt");
    if (this.failure) throw this.failure;
    if (this.stopping) throw new Error("OMP session is stopping");
    const completion = Promise.withResolvers<string>();
    // Exit/UI/result errors can arrive before the command acknowledgement.
    void completion.promise.catch(() => undefined);
    const turn: Turn = {
      id: `workspace-${++this.sequence}`, resolve: completion.resolve, reject: completion.reject,
      acknowledged: false, started: false, terminal: false, settled: false, text: "",
    };
    this.turn = turn; // Admission must happen before the first await.
    try {
      const response = await this.request("prompt", { message: text }, turn.id);
      turn.acknowledged = true;
      if (record(response.data) && response.data.agentInvoked === false) {
        turn.result = { status: "completed", agentInvoked: false };
      }
      this.finishTurn(turn);
    } catch (error) {
      const failure = error instanceof Error ? error : new Error(String(error));
      turn.failure = failure;
      if (!(failure instanceof OutcomeUnknownError) && !turn.started && this.turn === turn) this.turn = undefined;
      this.finishTurn(turn);
    }
    return await completion.promise;
  }

  async steer(text: string): Promise<void> {
    if (this.failure) throw this.failure;
    if (this.stopping) throw new Error("OMP session is stopping");
    if (!this.turn) throw new Error("OMP has no active prompt to steer; send this again as a new prompt");
    await this.request("steer", { message: text });
  }

  async btw(question: string): Promise<string> {
    if (this.sidecar) throw new Error("An OMP /btw question is already running");
    if (this.failure) throw this.failure;
    if (this.stopping) throw new Error("OMP session is stopping");
    const completion = Promise.withResolvers<string>();
    void completion.promise.catch(() => undefined);
    const sidecar: Sidecar = {
      id: `workspace-${++this.sequence}`, resolve: completion.resolve, reject: completion.reject, settled: false,
    };
    this.sidecar = sidecar;
    try {
      const response = await this.request("btw", { question }, sidecar.id);
      if (record(response.data) && record(response.data.record)) this.handleBtwRecord(response.data.record);
    } catch (error) {
      const failure = error instanceof Error ? error : new Error(String(error));
      if (!sidecar.settled) {
        sidecar.settled = true;
        sidecar.reject(failure);
      }
      if (!(failure instanceof OutcomeUnknownError) && this.sidecar === sidecar) this.sidecar = undefined;
    }
    return await completion.promise;
  }

  async abort(): Promise<void> { await this.request("abort"); }

  async answer(id: string, response: Record<string, unknown>): Promise<void> {
    await this.send({ ...response, id, type: "extension_ui_response" });
  }

  async stop(): Promise<void> {
    this.stopping = true;
    if (!this.exit) this.child.stdin.end();
    const result = await deadline(this.exited, 5_000, "OMP is still draining accepted work; shutdown outcome unknown (child was not killed)");
    if (result.error) throw new SessionLifecycleError(result.error, true);
    if (result.code !== 0) throw new SessionLifecycleError(new Error(`OMP shutdown failed (${result.signal ?? result.code}); persistence outcome unknown`), true);
  }

  private async request(type: string, fields: Frame = {}, id = `workspace-${++this.sequence}`): Promise<Frame> {
    if (this.failure) throw this.failure;
    if (this.stopping) throw new Error("OMP session is stopping");
    const waiter = Promise.withResolvers<Frame>();
    this.pending.set(id, waiter);
    void this.send({ ...fields, id, type }).catch((error: Error) => waiter.reject(error));
    try {
      return await deadline(waiter.promise, 30_000, `OMP ${type} acknowledgement timed out; outcome unknown; do not retry automatically`);
    } finally { this.pending.delete(id); }
  }

  private send(frame: Frame): Promise<void> {
    if (this.failure) return Promise.reject(this.failure);
    if (this.stopping) return Promise.reject(new Error("OMP session is stopping"));
    const sent = Promise.withResolvers<void>();
    this.child.stdin.write(`${JSON.stringify(frame)}\n`, (error) => error ? sent.reject(error) : sent.resolve());
    return sent.promise;
  }

  private handleFrame(frame: Frame): void {
    if (frame.type === "response" && typeof frame.id === "string") {
      const waiter = this.pending.get(frame.id);
      this.pending.delete(frame.id);
      if (frame.success === true) {
        waiter?.resolve(frame);
        if (this.turn?.id === frame.id) {
          this.turn.acknowledged = true;
          if (record(frame.data) && frame.data.agentInvoked === false) this.turn.result = { status: "completed", agentInvoked: false };
          this.finishTurn(this.turn);
        }
      }
      else {
        const error = errorOf(frame.error, "OMP RPC request failed");
        waiter?.reject(error);
        if (this.turn?.id === frame.id) {
          this.turn.failure = error;
          this.finishTurn(this.turn);
        }
      }
    }
    if (frame.type === "btw_record" && record(frame.record)) this.handleBtwRecord(frame.record);
    const turn = this.turn;
    if (frame.type === "agent_start") {
      this.streaming = true;
      if (turn) turn.started = true;
    }
    if (frame.type === "message_end" && turn?.started && record(frame.message) && frame.message.role === "assistant") {
      turn.text = textOf(frame.message);
      if (frame.message.stopReason === "error") turn.failure = errorOf(frame.message.errorMessage, "OMP provider request failed");
      if (frame.message.stopReason === "aborted") turn.failure = new Error("OMP prompt was aborted");
    }
    if (frame.type === "agent_end" && frame.isTerminal !== false) {
      this.streaming = false;
      if (turn?.started) {
        turn.terminal = true;
        const last = lastAssistant(frame.messages);
        if (last) {
          turn.text = textOf(last);
          if (last.stopReason === "error") turn.failure = errorOf(last.errorMessage, "OMP provider request failed");
          if (last.stopReason === "aborted") turn.failure = new Error("OMP prompt was aborted");
        }
        this.finishTurn(turn);
      }
    }
    if (frame.type === "prompt_result" && turn && frame.id === turn.id) {
      turn.result = frame;
      if (frame.status !== "completed") turn.failure = errorOf(frame.error, frame.status === "aborted" ? "OMP prompt was aborted" : "OMP prompt failed");
      this.finishTurn(turn);
    }
    if (frame.type === "rpc_frame_error") this.fail(new Error("OMP RPC output exceeded the transport limit; outcome unknown"));
    if (frame.type === "extension_ui_request" && typeof frame.id === "string" && typeof frame.method === "string") {
      const request = frame as UiRequest;
      void Promise.resolve().then(() => this.onUi(request)).catch((error: unknown) => {
        if (this.turn) {
          this.turn.failure = errorOf(error instanceof Error ? error.message : error, "OMP UI delivery failed");
          this.finishTurn(this.turn);
        }
        void this.answer(request.id, { cancelled: true }).catch(() => undefined);
      });
    }
  }

  private handleBtwRecord(record: Frame): void {
    const sidecar = this.sidecar;
    if (!sidecar || typeof record.id !== "string") return;
    if (sidecar.recordId !== undefined && sidecar.recordId !== record.id) return;
    sidecar.recordId = record.id;
    if (record.status === "running") return;
    if (!sidecar.settled) {
      sidecar.settled = true;
      if (record.status === "complete" && typeof record.answer === "string") sidecar.resolve(record.answer);
      else sidecar.reject(errorOf(record.error, `OMP /btw ${String(record.status ?? "failed")}`));
    }
    if (this.sidecar === sidecar) this.sidecar = undefined;
  }

  private finishTurn(turn: Turn): void {
    const local = turn.result?.agentInvoked === false;
    if (turn.failure && !turn.settled) {
      turn.settled = true;
      turn.reject(turn.failure);
    }
    if (!turn.settled && turn.acknowledged && turn.result?.status === "completed" && (local || turn.terminal)) {
      turn.settled = true;
      turn.resolve(local ? "" : turn.text);
    }
    // A timeout rejects the caller but cannot release the slot: the child may have admitted the prompt.
    if (turn.settled && (turn.terminal || local || (turn.result && !turn.started)) && this.turn === turn) this.turn = undefined;
  }

  private fail(error: Error): void {
    this.failure ??= error;
    for (const waiter of this.pending.values()) waiter.reject(this.failure);
    this.pending.clear();
    this.turn?.reject(this.failure);
    this.turn = undefined;
    if (this.sidecar && !this.sidecar.settled) this.sidecar.reject(this.failure);
    this.sidecar = undefined;
    if (this.exit) this.streaming = false;
  }
}
