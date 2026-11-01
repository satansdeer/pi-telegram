/**
 * Standalone OMP Workspace daemon: bot transport, single-process ownership, RPC wiring.
 * Zones: telegram transport, process lifecycle, configuration
 */
import { mkdir, open, readFile, unlink } from "node:fs/promises";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { OmpSession } from "./rpc.js";
import { startWorkspaceServer } from "./local.js";
import { WorkspaceBridge } from "./workspace.js";
const token = process.env.OMP_TELEGRAM_BOT_TOKEN?.trim();
if (!token)
    throw new Error("Set OMP_TELEGRAM_BOT_TOKEN in a private environment file");
const ownerId = Number(process.env.OMP_TELEGRAM_OWNER_ID ?? process.env.OMP_TELEGRAM_USER_IDS);
if (!Number.isSafeInteger(ownerId) || ownerId <= 0) {
    throw new Error("Set OMP_TELEGRAM_OWNER_ID to your numeric Telegram user ID (one owner only)");
}
const stateDir = resolve(process.env.OMP_WORKSPACE_STATE_DIR ?? join(homedir(), ".config", "omp-workspace"));
const cwd = resolve(process.env.OMP_WORKSPACE_CWD ?? process.cwd());
const binary = process.env.OMP_BINARY || "omp";
let stopping = false;
const polling = new AbortController();
class TelegramError extends Error {
    code;
    retryAfter;
    constructor(code, message, retryAfter) {
        super(message);
        this.code = code;
        this.retryAfter = retryAfter;
    }
}
async function api(method, params) {
    let response;
    try {
        response = await fetch(`https://api.telegram.org/bot${token}/${method}`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(params),
            signal: method === "getUpdates"
                ? AbortSignal.any([polling.signal, AbortSignal.timeout(40_000)])
                : AbortSignal.timeout(30_000),
        });
    }
    catch {
        throw new TelegramError(0, `${method}: network request failed; outcome may be unknown`);
    }
    let result;
    try {
        result = await response.json();
    }
    catch {
        throw new TelegramError(response.status, `${method}: invalid Telegram response; outcome may be unknown`);
    }
    if (!response.ok || result.ok !== true) {
        const description = String(result.description ?? response.statusText).replaceAll(token, "[redacted]");
        throw new TelegramError(result.error_code ?? response.status, `${method}: ${description}`, result.parameters?.retry_after);
    }
    return result.result;
}
const me = await api("getMe", {});
if (!Number.isSafeInteger(me.id) || !me.username)
    throw new Error("Invalid getMe response");
const webhook = await api("getWebhookInfo", {});
if (webhook.url)
    throw new Error("This bot has a webhook; remove it before using this polling daemon");
if (process.argv.includes("--check")) {
    console.log(JSON.stringify({ bot: `@${me.username}`, ownerId, cwd, stateDir, webhook: false }));
}
else {
    await mkdir(stateDir, { recursive: true, mode: 0o700 });
    const lockPath = join(stateDir, "daemon.lock");
    let lock;
    try {
        lock = await open(lockPath, "wx", 0o600);
    }
    catch (error) {
        if (error.code !== "EEXIST")
            throw error;
        const pid = Number((await readFile(lockPath, "utf8")).trim());
        if (!Number.isSafeInteger(pid) || pid <= 0)
            throw new Error("Invalid daemon.lock; inspect it before removing it");
        try {
            process.kill(pid, 0);
        }
        catch (probeError) {
            if (probeError.code !== "ESRCH")
                throw probeError;
            throw new Error(`Workspace daemon PID ${pid} is gone; inspect unfinished OMP work, then remove ${lockPath}`);
        }
        throw new Error(`Workspace daemon is already running (PID ${pid})`);
    }
    await lock.writeFile(`${process.pid}\n`);
    await lock.close();
    const bridge = new WorkspaceBridge({
        statePath: join(stateDir, "state.json"), cwd, ownerId, botId: me.id, botUsername: me.username,
    }, api, async (binding, onUi) => {
        const session = new OmpSession({
            binary, cwd: binding.cwd,
            sessionDir: join(stateDir, "sessions", binding.id),
            resume: binding.sessionFile, onUi,
        });
        await session.ready();
        return session;
    });
    const terminals = new Map();
    let local;
    const shutdown = () => { stopping = true; polling.abort(); };
    process.once("SIGTERM", shutdown);
    process.once("SIGINT", shutdown);
    try {
        await bridge.init();
        local = await startWorkspaceServer(stateDir, async (peer) => {
            const port = {
                async state() {
                    const state = await peer.request("state");
                    if (typeof state !== "object" || state === null || Array.isArray(state))
                        throw new Error("Invalid terminal session state");
                    return state;
                },
                async btw(text) {
                    const result = await peer.request("btw", { text });
                    if (typeof result !== "string")
                        throw new Error("Attached OMP terminal returned an invalid /btw response");
                    return result;
                },
                async prompt(text) {
                    const response = await peer.request("prompt", { text });
                    if (typeof response !== "string")
                        throw new Error("Invalid terminal response");
                    return response;
                },
                async abort() { await peer.request("abort"); },
                async answer(id, response) { await peer.request("answer", { id, response }); },
                async stop() { peer.close(); },
            };
            terminals.set(peer, port);
            try {
                await bridge.register(port, async (binding) => {
                    if (terminals.get(peer) !== port)
                        throw new Error("Terminal disconnected before attachment completed");
                    const { id, threadId, name, groupId, sessionId, sessionFile, cwd } = binding;
                    await peer.request("attached", { id, threadId, name, groupId, sessionId, sessionFile, cwd });
                });
            }
            catch (error) {
                terminals.delete(peer);
                await bridge.detach(port);
                throw error;
            }
        }, async (peer, name) => {
            const port = terminals.get(peer);
            if (!port)
                throw new Error("Terminal is not registered");
            const { id, threadId, name: label, groupId, sessionId, sessionFile, cwd: directory } = await bridge.attach(port, name);
            return { id, threadId, name: label, groupId, sessionId, sessionFile, cwd: directory };
        }, (peer) => {
            const port = terminals.get(peer);
            terminals.delete(peer);
            if (port)
                bridge.detach(port);
        });
        console.log(`Workspace ready: @${me.username}; owner ${ownerId}; cwd ${cwd}`);
        while (!stopping) {
            let updates;
            try {
                updates = await api("getUpdates", { offset: bridge.offset, timeout: 25, allowed_updates: ["message"] });
            }
            catch (error) {
                if (stopping)
                    break;
                if (error instanceof TelegramError && [401, 403, 409].includes(error.code))
                    throw error;
                console.error(error instanceof Error ? error.message : "Telegram polling failed");
                await delay(error instanceof TelegramError && error.retryAfter ? error.retryAfter * 1000 : 3_000);
                continue;
            }
            if (!Array.isArray(updates))
                throw new Error("getUpdates did not return an array");
            for (const update of updates) {
                if (stopping)
                    break;
                await bridge.handle(update);
            }
        }
    }
    finally {
        stopping = true;
        polling.abort();
        await bridge.stop();
        await local?.close();
        await unlink(lockPath);
    }
}
