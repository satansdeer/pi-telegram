/**
 * Owns one persistent OMP child and its JSONL request/turn lifecycle.
 * Zones: omp rpc, process lifecycle, session persistence
 */
import { spawn } from "node:child_process";
import { chmodSync, closeSync, mkdirSync, openSync, readdirSync, readSync, statSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { createInterface } from "node:readline";
class OutcomeUnknownError extends Error {
}
function record(value) {
    return value !== null && typeof value === "object" && !Array.isArray(value);
}
function lastAssistant(messages) {
    if (!Array.isArray(messages))
        return undefined;
    return messages.findLast((message) => record(message) && message.role === "assistant");
}
function textOf(message) {
    if (typeof message.content === "string")
        return message.content;
    if (!Array.isArray(message.content))
        return "";
    return message.content.filter((part) => record(part) && part.type === "text" && typeof part.text === "string")
        .map((part) => part.text).join("");
}
function errorOf(value, fallback) {
    return new Error(typeof value === "string" ? value : record(value) && typeof value.message === "string" ? value.message : fallback);
}
function deadline(promise, milliseconds, message) {
    const timeout = Promise.withResolvers();
    const timer = setTimeout(() => timeout.reject(new OutcomeUnknownError(message)), milliseconds);
    return Promise.race([promise, timeout.promise]).finally(() => clearTimeout(timer));
}
// Read only the identity prefix, not a potentially enormous transcript. OMP may prepend a title entry.
function transcriptId(path) {
    if (!statSync(path).isFile())
        throw new Error(`OMP resume path is not a transcript file: ${path}`);
    const file = openSync(path, "r");
    try {
        const bytes = Buffer.alloc(64 * 1024);
        const prefix = bytes.subarray(0, readSync(file, bytes)).toString("utf8");
        for (const line of prefix.split("\n").slice(0, 2)) {
            let entry;
            try {
                entry = JSON.parse(line);
            }
            catch {
                break;
            }
            if (record(entry) && entry.type === "session" && typeof entry.id === "string" && entry.id)
                return entry.id;
        }
        throw new Error(`OMP resume file has no valid session identity: ${path}`);
    }
    finally {
        closeSync(file);
    }
}
export class OmpSession {
    child;
    pending = new Map();
    exited;
    recovered;
    directory;
    onUi;
    turn;
    failure;
    exit;
    sequence = 0;
    streaming = false;
    stopping = false;
    constructor(options) {
        const directory = resolve(options.cwd, options.sessionDir);
        this.directory = directory;
        mkdirSync(directory, { recursive: true, mode: 0o700 });
        chmodSync(directory, 0o700);
        this.recovered = new Map();
        const resume = options.resume === undefined ? undefined : resolve(options.cwd, options.resume);
        if (resume !== undefined) {
            this.recovered.set(resume, transcriptId(resume));
        }
        else {
            for (const entry of readdirSync(directory, { withFileTypes: true })) {
                if (entry.isFile() && entry.name.endsWith(".jsonl")) {
                    const file = resolve(directory, entry.name);
                    this.recovered.set(file, transcriptId(file));
                }
            }
        }
        const env = { ...process.env };
        for (const [key, value] of Object.entries(env)) {
            if (/TELEGRAM.*TOKEN|BOT_?TOKEN|TOKEN.*TELEGRAM/i.test(key) || /^\d+:[\w-]{20,}$/.test(value ?? ""))
                delete env[key];
        }
        env.OMP_WORKSPACE_MANAGED = "1";
        this.onUi = options.onUi;
        this.child = spawn(options.binary, [
            ...(options.args ?? []), "--mode", "rpc", "--session-dir", directory,
            ...(resume ? ["--resume", resume] : this.recovered.size ? ["--continue"] : []),
        ], { cwd: options.cwd, env, stdio: ["pipe", "pipe", "pipe"] });
        const exited = Promise.withResolvers();
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
            if (!line.trim())
                return;
            let frame;
            try {
                frame = JSON.parse(line);
            }
            catch {
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
    get busy() { return this.turn !== undefined || this.streaming; }
    async ready() {
        try {
            const state = await this.state();
            if (typeof state.sessionId !== "string" || !state.sessionId)
                throw new Error("OMP did not expose a session identity");
            if (!this.recovered.size && (typeof state.sessionFile !== "string" || dirname(resolve(state.sessionFile)) !== this.directory)) {
                throw new Error("OMP did not allocate a persistent transcript in its dedicated session directory");
            }
            if (this.recovered.size && (typeof state.sessionFile !== "string" || this.recovered.get(resolve(state.sessionFile)) !== state.sessionId)) {
                throw new Error("OMP did not resume the persisted transcript; refusing a fresh replacement session");
            }
            return state;
        }
        catch (error) {
            try {
                await this.stop();
            }
            catch (shutdown) {
                if (this.exit)
                    throw error; // A known exit is not an orphaned startup process.
                throw new AggregateError([error, shutdown], "OMP startup failed and its child shutdown is uncertain");
            }
            throw error;
        }
    }
    async state() {
        const response = await this.request("get_state");
        if (!record(response.data))
            throw new Error("OMP get_state returned no state");
        return response.data;
    }
    async prompt(text) {
        if (this.busy)
            throw new Error("OMP session is already processing a prompt");
        if (this.failure)
            throw this.failure;
        if (this.stopping)
            throw new Error("OMP session is stopping");
        const completion = Promise.withResolvers();
        // Exit/UI/result errors can arrive before the command acknowledgement.
        void completion.promise.catch(() => undefined);
        const turn = {
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
        }
        catch (error) {
            const failure = error instanceof Error ? error : new Error(String(error));
            turn.failure = failure;
            if (!(failure instanceof OutcomeUnknownError) && !turn.started && this.turn === turn)
                this.turn = undefined;
            this.finishTurn(turn);
        }
        return await completion.promise;
    }
    async abort() { await this.request("abort"); }
    async answer(id, response) {
        await this.send({ ...response, id, type: "extension_ui_response" });
    }
    async stop() {
        this.stopping = true;
        if (!this.exit)
            this.child.stdin.end();
        const result = await deadline(this.exited, 5_000, "OMP is still draining accepted work; shutdown outcome unknown (child was not killed)");
        if (result.error)
            throw result.error;
        if (result.code !== 0)
            throw new Error(`OMP shutdown failed (${result.signal ?? result.code}); persistence outcome unknown`);
    }
    async request(type, fields = {}, id = `workspace-${++this.sequence}`) {
        if (this.failure)
            throw this.failure;
        if (this.stopping)
            throw new Error("OMP session is stopping");
        const waiter = Promise.withResolvers();
        this.pending.set(id, waiter);
        void this.send({ ...fields, id, type }).catch((error) => waiter.reject(error));
        try {
            return await deadline(waiter.promise, 30_000, `OMP ${type} acknowledgement timed out; outcome unknown; do not retry automatically`);
        }
        finally {
            this.pending.delete(id);
        }
    }
    send(frame) {
        if (this.failure)
            return Promise.reject(this.failure);
        if (this.stopping)
            return Promise.reject(new Error("OMP session is stopping"));
        const sent = Promise.withResolvers();
        this.child.stdin.write(`${JSON.stringify(frame)}\n`, (error) => error ? sent.reject(error) : sent.resolve());
        return sent.promise;
    }
    handleFrame(frame) {
        if (frame.type === "response" && typeof frame.id === "string") {
            const waiter = this.pending.get(frame.id);
            this.pending.delete(frame.id);
            if (frame.success === true) {
                waiter?.resolve(frame);
                if (this.turn?.id === frame.id) {
                    this.turn.acknowledged = true;
                    if (record(frame.data) && frame.data.agentInvoked === false)
                        this.turn.result = { status: "completed", agentInvoked: false };
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
        const turn = this.turn;
        if (frame.type === "agent_start") {
            this.streaming = true;
            if (turn)
                turn.started = true;
        }
        if (frame.type === "message_end" && turn?.started && record(frame.message) && frame.message.role === "assistant") {
            turn.text = textOf(frame.message);
            if (frame.message.stopReason === "error")
                turn.failure = errorOf(frame.message.errorMessage, "OMP provider request failed");
            if (frame.message.stopReason === "aborted")
                turn.failure = new Error("OMP prompt was aborted");
        }
        if (frame.type === "agent_end" && frame.isTerminal !== false) {
            this.streaming = false;
            if (turn?.started) {
                turn.terminal = true;
                const last = lastAssistant(frame.messages);
                if (last) {
                    turn.text = textOf(last);
                    if (last.stopReason === "error")
                        turn.failure = errorOf(last.errorMessage, "OMP provider request failed");
                    if (last.stopReason === "aborted")
                        turn.failure = new Error("OMP prompt was aborted");
                }
                this.finishTurn(turn);
            }
        }
        if (frame.type === "prompt_result" && turn && frame.id === turn.id) {
            turn.result = frame;
            if (frame.status !== "completed")
                turn.failure = errorOf(frame.error, frame.status === "aborted" ? "OMP prompt was aborted" : "OMP prompt failed");
            this.finishTurn(turn);
        }
        if (frame.type === "rpc_frame_error")
            this.fail(new Error("OMP RPC output exceeded the transport limit; outcome unknown"));
        if (frame.type === "extension_ui_request" && typeof frame.id === "string" && typeof frame.method === "string") {
            const request = frame;
            void Promise.resolve().then(() => this.onUi(request)).catch((error) => {
                if (this.turn) {
                    this.turn.failure = errorOf(error instanceof Error ? error.message : error, "OMP UI delivery failed");
                    this.finishTurn(this.turn);
                }
                void this.answer(request.id, { cancelled: true }).catch(() => undefined);
            });
        }
    }
    finishTurn(turn) {
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
        if (turn.settled && (turn.terminal || local || (turn.result && !turn.started)) && this.turn === turn)
            this.turn = undefined;
    }
    fail(error) {
        this.failure ??= error;
        for (const waiter of this.pending.values())
            waiter.reject(this.failure);
        this.pending.clear();
        this.turn?.reject(this.failure);
        this.turn = undefined;
        if (this.exit)
            this.streaming = false;
    }
}
