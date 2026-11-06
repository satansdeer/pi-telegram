/**
 * Standalone OMP Workspace forum bridge.
 * Zones: owner admission, durable topic/session identity, RPC UI, exact Telegram delivery.
 * Owns no polling or API transport. Topics and transcripts are never deleted or replaced.
 */
import { randomBytes, randomUUID } from "node:crypto";
import { chmod, lstat, mkdir, open, readFile, readdir, realpath, rename, stat, unlink } from "node:fs/promises";
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { appendTelegramAttachmentSection, appendTelegramListSection, buildTelegramReplyContextBlock, downloadTelegramMessageFiles, extractTelegramMessageText, } from "../lib/media.js";
import { extractMarkdownLinkDestinations } from "../lib/rendering.js";
import { getTelegramTargetThreadParams } from "../lib/target.js";
import { SessionLifecycleError } from "./rpc.js";
const TYPING_REFRESH_MS = 3_000;
const MAX_LOCAL_ARTIFACTS = 10;
const MAX_LOCAL_ARTIFACT_BYTES = 50 * 1024 * 1024;
const LOCAL_ARTIFACT_URI = /\blocal:\/\/([^\s<>"')\]]+)/g;
const HELP = [
    "/workspace — bind this forum supergroup once (bot must manage topics)",
    "/new [name] [--cwd directory] — create a topic/session; relative directories start at the Workspace root",
    "/bind [name] — adopt this existing non-General topic",
    "/sessions — list chat IDs, Telegram topics, and OMP sessions",
    "/running — list live terminals without creating topics",
    "/attach <full-session-id> — attach that exact live terminal to a topic",
    "/status — show this topic's session and pending UI requests",
    "/repair — release a missing managed transcript so this topic can start a fresh OMP session",
    "/btw <question> — ask an ephemeral side question without adding it to the main chat history",
    "/abort — interrupt this topic's active turn",
    "/answer <request-id> <JSON object> — answer this topic's pending UI request",
    "/cancel <request-id> — cancel this topic's pending UI request",
    "/help — show these commands",
    "Only the configured owner can send input. Plain text or supported files in an idle topic start a turn; additional input steers its active turn. Reply to or forward an older file to recover it into the local attachment archive. Other slash commands are not forwarded.",
].join("\n");
const DIALOGS = { confirm: true, select: true, input: true, editor: true, ask: true };
const SUPPORTED_ATTACHMENTS = ["photo", "animation", "audio", "document", "video", "voice", "sticker"];
const ATTACHMENTS = [...SUPPORTED_ATTACHMENTS, "video_note", "contact", "location", "venue", "poll", "dice", "paid_media"];
function record(value) {
    return typeof value === "object" && value !== null && !Array.isArray(value);
}
function integer(value, minimum = 1) {
    return typeof value === "number" && Number.isSafeInteger(value) && value >= minimum;
}
function chatId(value) {
    return typeof value === "number" && Number.isSafeInteger(value) && value !== 0;
}
function text(value) {
    return typeof value === "string" && value.length > 0 && !value.includes("\0");
}
function keys(value, allowed) {
    return Object.keys(value).every((key) => allowed.includes(key));
}
function hasAttachment(message, kinds = ATTACHMENTS) {
    return kinds.some((key) => message[key] !== undefined);
}
function escapeHtml(value) {
    return value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
}
async function canonicalFile(path) {
    try {
        return await realpath(path);
    }
    catch (error) {
        if (record(error) && error.code === "ENOENT")
            return resolve(await canonicalFile(dirname(path)), basename(path));
        throw error;
    }
}
function terminalIdentity(state) {
    if (!record(state) || !text(state.sessionId) || /\s/.test(state.sessionId)
        || !text(state.sessionFile) || !isAbsolute(state.sessionFile)
        || !text(state.cwd) || !isAbsolute(state.cwd)) {
        throw new Error("Terminal must have a persistent session identity and absolute sessionFile and cwd");
    }
    return { sessionId: state.sessionId, sessionFile: state.sessionFile, cwd: state.cwd };
}
function diagnostic(error) {
    return (error instanceof Error ? error.message : "Unknown processing error")
        .replace(/\b\d{5,}:[A-Za-z0-9_-]{20,}\b/g, "[redacted bot token]").slice(0, 600);
}
function targetOf(message) {
    if (!record(message.chat) || !chatId(message.chat.id))
        return undefined;
    if (message.message_thread_id !== undefined && !integer(message.message_thread_id))
        return undefined;
    return message.message_thread_id === undefined
        ? { chatId: message.chat.id }
        : { chatId: message.chat.id, threadId: message.message_thread_id };
}
function parseState(value, options) {
    const invalid = () => { throw new Error("Invalid Workspace state; refusing to reset or replay it"); };
    if (!record(value) || !keys(value, ["version", "ownerId", "botId", "offset", "groupId", "bindings", "admissions"]))
        return invalid();
    if (value.version !== 1 || !integer(value.ownerId) || !integer(value.botId) || !integer(value.offset, 0))
        return invalid();
    if (value.ownerId !== options.ownerId || value.botId !== options.botId)
        throw new Error("Workspace state belongs to a different bot or owner");
    if (value.groupId !== undefined && (!chatId(value.groupId) || value.groupId >= 0))
        return invalid();
    if (!Array.isArray(value.bindings) || !Array.isArray(value.admissions))
        return invalid();
    const ids = new Set();
    const threads = new Set();
    const sessionIds = new Set();
    const sessionFiles = new Set();
    for (const binding of value.bindings) {
        if (!record(binding) || !keys(binding, ["id", "threadId", "name", "cwd", "sessionId", "sessionFile", "external"]))
            return invalid();
        if (typeof binding.id !== "string" || !/^[a-f0-9]{12}$/.test(binding.id) || !integer(binding.threadId, 2)
            || !text(binding.name) || [...binding.name].length > 128 || !text(binding.cwd) || !isAbsolute(binding.cwd)
            || ids.has(binding.id) || threads.has(binding.threadId))
            return invalid();
        if ((binding.sessionId === undefined) !== (binding.sessionFile === undefined))
            return invalid();
        if (binding.sessionId !== undefined && (!text(binding.sessionId) || !text(binding.sessionFile) || !isAbsolute(binding.sessionFile)))
            return invalid();
        if (binding.external !== undefined && (binding.external !== true || binding.sessionId === undefined))
            return invalid();
        if (binding.sessionId !== undefined) {
            if (sessionIds.has(binding.sessionId) || sessionFiles.has(resolve(binding.sessionFile)))
                return invalid();
            sessionIds.add(binding.sessionId);
            sessionFiles.add(resolve(binding.sessionFile));
        }
        ids.add(binding.id);
        threads.add(binding.threadId);
    }
    if (value.bindings.length && value.groupId === undefined)
        return invalid();
    const updates = new Set();
    for (const admission of value.admissions) {
        if (!record(admission) || !keys(admission, ["updateId", "target", "kind", "phase", "diagnostic"]))
            return invalid();
        if (!integer(admission.updateId, 0) || admission.updateId >= value.offset || updates.has(admission.updateId)
            || !record(admission.target) || !keys(admission.target, ["chatId", "threadId"]) || !chatId(admission.target.chatId)
            || (admission.target.threadId !== undefined && !integer(admission.target.threadId)) || !text(admission.kind)
            || !["admitted", "running", "delivery", "report"].includes(admission.phase)
            || (admission.diagnostic !== undefined && !text(admission.diagnostic)))
            return invalid();
        updates.add(admission.updateId);
    }
    return value;
}
/** Validate the UI protocol, including exact ask/select choices, before crossing RPC. */
function validateAnswer(request, response) {
    if (!record(response))
        throw new Error("Answer must be a JSON object");
    if (request.secret === true)
        throw new Error("Secret input is unsupported over Telegram; cancel this request and use the local terminal");
    const one = Object.keys(response).length === 1;
    if (response.cancelled === true && one)
        return;
    if (request.method === "confirm" && one && typeof response.confirmed === "boolean")
        return;
    if (["select", "input", "editor"].includes(request.method) && one && typeof response.value === "string") {
        if (request.method !== "select" || (Array.isArray(request.options) && request.options.includes(response.value)))
            return;
    }
    if (request.method === "ask" && one && Array.isArray(response.answers) && Array.isArray(request.questions)
        && response.answers.length === request.questions.length) {
        const valid = response.answers.every((answer, index) => {
            const question = request.questions[index];
            if (!record(answer) || !keys(answer, ["id", "selectedOptions", "customInput"]) || !record(question)
                || answer.id !== question.id || !Array.isArray(answer.selectedOptions) || !Array.isArray(question.options)
                || (answer.customInput !== undefined && typeof answer.customInput !== "string"))
                return false;
            const choices = question.options.filter(record).map((option) => option.label);
            if (!answer.selectedOptions.every((option) => typeof option === "string" && choices.includes(option))
                || new Set(answer.selectedOptions).size !== answer.selectedOptions.length)
                return false;
            return question.multi === true || (answer.selectedOptions.length <= 1
                && !(answer.selectedOptions.length && typeof answer.customInput === "string" && answer.customInput.trim()));
        });
        if (valid)
            return;
    }
    throw new Error(`Invalid ${request.method} answer; use the request's exact options and protocol fields (value, confirmed, answers, or cancelled:true)`);
}
export class WorkspaceBridge {
    options;
    api;
    openSession;
    downloadFile;
    sendFile;
    transcribeVoice;
    data;
    initialized = false;
    stopping = false;
    fatal;
    admitting = Promise.resolve();
    attaching = Promise.resolve();
    writing = Promise.resolve();
    jobs = new Set();
    sessions = new Map();
    terminals = new Map();
    pending = new Map();
    busy = new Map();
    starts = new Map();
    groupSetup = false;
    constructor(options, api, openSession, downloadFile, sendFile, transcribeVoice) {
        if (!integer(options.ownerId) || !integer(options.botId) || !/^[A-Za-z0-9_]+$/.test(options.botUsername))
            throw new Error("Invalid Workspace bot/owner identity");
        this.options = { ...options, statePath: resolve(options.statePath), cwd: resolve(options.cwd) };
        this.api = api;
        this.openSession = openSession;
        this.downloadFile = downloadFile;
        this.sendFile = sendFile;
        this.transcribeVoice = transcribeVoice;
        this.data = { version: 1, ownerId: options.ownerId, botId: options.botId, offset: 0, bindings: [], admissions: [] };
    }
    get offset() { return this.data.offset; }
    /** Registration is ephemeral: no topic creation, persistence, or implicit reattachment. */
    async register(port, onAttach) {
        if (!this.initialized || this.stopping || this.fatal)
            throw this.fatal ?? new Error("Workspace is not ready");
        if (this.terminals.has(port))
            throw new Error("Terminal is already registered");
        const registration = { onAttach };
        this.terminals.set(port, registration);
        try {
            const identity = terminalIdentity(await port.state());
            const file = await canonicalFile(identity.sessionFile);
            this.requireRegistration(port, registration);
            for (const [other, entry] of this.terminals) {
                if (other !== port && entry.identity && (entry.identity.sessionId === identity.sessionId || entry.canonicalFile === file)) {
                    throw new Error("This transcript already has a live terminal owner");
                }
            }
            registration.identity = identity;
            registration.canonicalFile = file;
            await this.terminalBinding(identity, file);
            this.requireRegistration(port, registration);
            registration.registered = true;
        }
        catch (error) {
            if (this.terminals.get(port) === registration)
                this.detach(port);
            throw error;
        }
    }
    /** A separate queue keeps slow attachments out of Telegram admission/control handling. */
    attach(port, label) {
        const registration = this.terminals.get(port);
        if (!registration?.registered)
            return Promise.reject(new Error("Terminal is not registered; reconnect it before attaching"));
        const attached = this.attaching.then(async () => {
            this.requireRegistration(port, registration);
            if (this.data.groupId === undefined)
                throw new Error("Send /workspace in your Telegram forum group first");
            const identity = terminalIdentity(await this.readTerminal(port, registration));
            let binding = await this.terminalBinding(identity, registration.canonicalFile);
            this.requireRegistration(port, registration);
            if (binding && registration.bindingId === binding.id && this.sessions.has(binding.id))
                return this.attachedBinding(binding);
            if (binding && (this.sessions.has(binding.id) || this.busy.has(binding.threadId))) {
                throw new Error("This transcript already has a live connection or an unsettled turn");
            }
            if (!binding) {
                if (registration.topicFailure)
                    throw registration.topicFailure;
                let id;
                do {
                    id = randomBytes(6).toString("hex");
                } while (this.data.bindings.some((entry) => entry.id === id));
                const prefix = `[${id}] `;
                const name = [...(label.replace(/\s+/g, " ").trim() || basename(identity.cwd))].slice(0, 128 - prefix.length).join("");
                let threadId;
                try {
                    // Latch uncertain outcomes: queued requests must not create a second topic.
                    const topic = await this.api("createForumTopic", { chat_id: this.data.groupId, name: `${prefix}${name}` });
                    if (!record(topic) || !integer(topic.message_thread_id, 2)
                        || this.data.bindings.some((entry) => entry.threadId === topic.message_thread_id))
                        throw new Error("Invalid or already-bound topic");
                    threadId = topic.message_thread_id;
                }
                catch (error) {
                    registration.topicFailure = new Error(`Topic creation outcome is uncertain; inspect Telegram before reconnecting. ${diagnostic(error)}`);
                    throw registration.topicFailure;
                }
                binding = { id, threadId, name, ...identity, external: true };
                // Retain a known created topic even if its terminal disconnected during the call.
                this.data.bindings.push(binding);
                await this.save();
            }
            await this.readTerminal(port, registration);
            const attached = this.attachedBinding(binding);
            await registration.onAttach(attached);
            await this.readTerminal(port, registration);
            this.sessions.set(binding.id, Promise.resolve(port));
            registration.bindingId = binding.id;
            await this.send({ chatId: attached.groupId, threadId: binding.threadId }, `Terminal connected (same live OMP session).\n${this.describe(binding)}\nInteractive dialogs stay in the terminal.`);
            this.requireRegistration(port, registration);
            return attached;
        });
        this.attaching = attached.then(() => { }, () => { });
        return attached;
    }
    detach(port) {
        const registration = this.terminals.get(port);
        if (registration?.bindingId !== undefined)
            this.sessions.delete(registration.bindingId);
        this.terminals.delete(port);
    }
    requireRegistration(port, registration) {
        if (this.stopping || this.fatal || this.terminals.get(port) !== registration) {
            throw this.fatal ?? new Error("Terminal registration is no longer live; reconnect before attaching");
        }
    }
    async readTerminal(port, registration) {
        this.requireRegistration(port, registration);
        try {
            const state = await port.state();
            const identity = terminalIdentity(state);
            const file = await canonicalFile(identity.sessionFile);
            this.requireRegistration(port, registration);
            const expected = registration.identity;
            if (!expected || identity.sessionId !== expected.sessionId || identity.sessionFile !== expected.sessionFile
                || identity.cwd !== expected.cwd || file !== registration.canonicalFile) {
                throw new Error("Terminal identity changed; reconnect the designated session before using /running or /attach");
            }
            return { ...state, ...identity };
        }
        catch (error) {
            if (this.terminals.get(port) === registration)
                this.detach(port);
            throw error;
        }
    }
    async terminalBinding(identity, file) {
        let found;
        for (const binding of this.data.bindings) {
            if (binding.sessionId !== identity.sessionId
                && (binding.sessionFile === undefined || await canonicalFile(binding.sessionFile) !== file))
                continue;
            if (!binding.external || binding.sessionId !== identity.sessionId || binding.sessionFile !== identity.sessionFile || binding.cwd !== identity.cwd) {
                throw new Error("Transcript is already designated to a different session owner");
            }
            found = binding;
        }
        return found;
    }
    attachedBinding(binding) {
        return { ...binding, groupId: this.data.groupId, sessionId: binding.sessionId, sessionFile: binding.sessionFile };
    }
    async init() {
        if (this.initialized)
            throw new Error("Workspace already initialized");
        const directory = dirname(this.options.statePath);
        await mkdir(directory, { recursive: true, mode: 0o700 });
        if (!(await lstat(directory)).isDirectory())
            throw new Error("Workspace state directory must be a real directory");
        await chmod(directory, 0o700);
        let source;
        try {
            const info = await lstat(this.options.statePath);
            if (!info.isFile() || info.isSymbolicLink())
                throw new Error("Workspace state must be a regular file");
            await chmod(this.options.statePath, 0o600);
            source = await readFile(this.options.statePath, "utf8");
        }
        catch (error) {
            if (!(record(error) && error.code === "ENOENT"))
                throw error;
        }
        if (source !== undefined) {
            let parsed;
            try {
                parsed = JSON.parse(source);
            }
            catch {
                throw new Error("Malformed Workspace state JSON; refusing to reset or replay it");
            }
            this.data = parseState(parsed, this.options);
        }
        else
            await this.save();
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
            }
            catch (error) {
                console.error(`Workspace recovery delivery failed for update ${admission.updateId}: ${diagnostic(error)}`);
            }
        }
        this.initialized = true;
    }
    /** Admit durably, then run turns in the background so controls stay responsive. */
    handle(update) {
        const next = this.admitting.then(() => this.admit(update));
        this.admitting = next.catch(() => { });
        return next;
    }
    async admit(update) {
        if (!this.initialized || this.stopping)
            throw new Error("Workspace is not accepting updates");
        if (this.fatal)
            throw this.fatal;
        if (!record(update) || !integer(update.update_id, 0) || update.update_id === Number.MAX_SAFE_INTEGER)
            throw new Error("Invalid Telegram update_id");
        if (update.update_id < this.data.offset)
            return;
        this.data.offset = update.update_id + 1;
        const message = record(update.message) ? update.message : undefined;
        const target = message && targetOf(message);
        if (!message || !target || !(typeof message.text === "string" || hasAttachment(message))) {
            await this.save();
            return;
        }
        const input = typeof message.text === "string" ? message.text : typeof message.caption === "string" ? message.caption : "";
        const hasPrompt = Boolean(input.trim() || hasAttachment(message, SUPPORTED_ATTACHMENTS)
            || (record(message.reply_to_message) && hasAttachment(message.reply_to_message, SUPPORTED_ATTACHMENTS)));
        const commandInput = typeof message.text === "string" ? message.text.trimStart() : "";
        const match = /^\/([A-Za-z0-9_]+)(?:@([A-Za-z0-9_]+))?(?:\s+([\s\S]*))?$/.exec(commandInput);
        const command = commandInput.startsWith("/") ? {
            name: match?.[1]?.toLowerCase() ?? "unknown",
            argument: match?.[3]?.trim() ?? "",
            otherBot: Boolean(match?.[2] && match[2].toLowerCase() !== this.options.botUsername.toLowerCase()),
        } : undefined;
        if (command?.otherBot) {
            await this.save();
            return;
        }
        const owner = record(message.from) && message.from.id === this.options.ownerId && message.sender_chat === undefined;
        let refusal = owner ? undefined : "Owner only: this bot does not accept input or controls from other group members.";
        let reserved = false;
        let setup = false;
        let steeringReady;
        let turnStarted;
        if (owner && !command && hasPrompt && target.chatId === this.data.groupId && target.threadId !== undefined
            && this.data.bindings.some((binding) => binding.threadId === target.threadId)) {
            const count = this.busy.get(target.threadId);
            if (count !== undefined) {
                steeringReady = this.starts.get(target.threadId);
                if (steeringReady) {
                    this.busy.set(target.threadId, count + 1);
                    reserved = true;
                }
                else
                    refusal = "This topic is busy; its binding is still being initialized.";
            }
            else {
                const started = Promise.withResolvers();
                this.busy.set(target.threadId, 1);
                this.starts.set(target.threadId, started.promise);
                turnStarted = started.resolve;
            }
            reserved = true;
        }
        if (owner && command?.name === "bind" && target.chatId === this.data.groupId && target.threadId !== undefined && target.threadId > 1
            && !this.data.bindings.some((binding) => binding.threadId === target.threadId)) {
            if (this.busy.has(target.threadId))
                refusal = "This topic is busy; its binding is already being initialized.";
            else {
                this.busy.set(target.threadId, 1);
                reserved = true;
            }
        }
        if (owner && command?.name === "workspace" && this.data.groupId === undefined) {
            if (this.groupSetup)
                refusal = "Workspace binding is already in progress.";
            else {
                this.groupSetup = true;
                setup = true;
            }
        }
        const admission = {
            updateId: update.update_id, target,
            kind: refusal ? "refusal" : command?.name ?? (steeringReady ? "steer" : hasPrompt ? "prompt" : "attachment"),
            phase: "admitted",
        };
        this.data.admissions.push(admission);
        await this.save();
        this.track(this.process(admission, message, command, refusal, steeringReady, turnStarted).finally(() => {
            turnStarted?.();
            if (reserved && target.threadId !== undefined) {
                const count = this.busy.get(target.threadId) ?? 0;
                if (count <= 1) {
                    this.busy.delete(target.threadId);
                    this.starts.delete(target.threadId);
                }
                else
                    this.busy.set(target.threadId, count - 1);
            }
            if (setup)
                this.groupSetup = false;
        }));
    }
    async process(admission, message, command, refusal, steeringReady, turnStarted) {
        try {
            if (refusal)
                await this.reply(admission, refusal);
            else if (command)
                await this.command(admission, command);
            else if (hasAttachment(message) && !hasAttachment(message, SUPPORTED_ATTACHMENTS)
                && !(record(message.reply_to_message) && hasAttachment(message.reply_to_message, SUPPORTED_ATTACHMENTS))) {
                await this.reply(admission, "This attachment type is unsupported. Send a photo, document, video, audio/voice, animation, or sticker.");
            }
            else {
                const promptText = await this.attachmentPrompt(message, admission.target);
                if (!promptText.trim()) {
                    await this.reply(admission, "Send non-empty text or a supported attachment.");
                }
                else if (admission.kind === "steer") {
                    await steeringReady;
                    this.requireGroup(admission.target);
                    const binding = this.requireBinding(admission.target);
                    const session = await this.session(binding);
                    await this.assertIdentity(binding, session);
                    admission.phase = "running";
                    await this.save();
                    if (binding.external)
                        this.externalRegistration(binding, session);
                    await session.steer(promptText);
                    await this.reply(admission, "↪️ Steering applied to the active turn.");
                }
                else {
                    this.requireGroup(admission.target);
                    const binding = this.requireBinding(admission.target);
                    const session = await this.session(binding, !binding.external && binding.sessionId === undefined);
                    await this.assertIdentity(binding, session);
                    admission.phase = "running";
                    await this.save();
                    if (binding.external)
                        this.externalRegistration(binding, session);
                    const stopTyping = this.startTyping(admission.target);
                    let output;
                    try {
                        const prompt = session.prompt(promptText);
                        turnStarted?.();
                        output = await prompt;
                    }
                    finally {
                        stopTyping();
                    }
                    await this.replyWithArtifacts(admission, binding, output || "OMP completed without a text reply.");
                }
            }
            await this.complete(admission);
        }
        catch (error) {
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
            }
            catch (reportError) {
                console.error(`Workspace failure report could not be committed/delivered: ${diagnostic(reportError)}`);
            }
        }
    }
    attachmentArchive(target) {
        return join(dirname(this.options.statePath), "attachments", String(target.chatId), String(target.threadId ?? 0));
    }
    async attachmentFiles(message, target) {
        return downloadTelegramMessageFiles([message], {
            downloadFile: (fileId, fileName, source) => {
                if (!source || !integer(source.messageId))
                    throw new Error("Telegram attachment has no valid message ID");
                return this.downloadFile(fileId, fileName, join(this.attachmentArchive(target), String(source.messageId)));
            },
        });
    }
    async voiceTranscripts(files, label) {
        const transcripts = [];
        for (const file of files) {
            if (file.kind !== "voice")
                continue;
            try {
                const transcript = (await this.transcribeVoice(file.path)).trim();
                if (transcript)
                    transcripts.push(`${label} (${file.fileName}): ${transcript}`);
            }
            catch (error) {
                console.error(`Voice transcription failed for ${file.path}:`, error);
            }
        }
        return transcripts;
    }
    async attachmentPrompt(message, target) {
        const media = message;
        let files = [];
        if (hasAttachment(message, SUPPORTED_ATTACHMENTS)) {
            if (!integer(message.message_id))
                throw new Error("Telegram attachment has no valid message ID");
            files = await this.attachmentFiles(media, target);
        }
        let replyFiles = [];
        const reply = record(message.reply_to_message) ? message.reply_to_message : undefined;
        if (reply && hasAttachment(reply, SUPPORTED_ATTACHMENTS)) {
            if (!integer(reply.message_id))
                throw new Error("Replied attachment has no valid message ID");
            replyFiles = await this.attachmentFiles({
                ...reply,
                message_id: reply.message_id,
                ...(media.chat ? { chat: media.chat } : {}),
            }, target);
        }
        let prompt = appendTelegramAttachmentSection(extractTelegramMessageText(media), files);
        const replyContext = buildTelegramReplyContextBlock(media, replyFiles);
        if (replyContext)
            prompt = prompt ? `${prompt}\n\n${replyContext}` : `_\n\n${replyContext}`;
        const transcripts = [
            ...await this.voiceTranscripts(files, "Voice transcript"),
            ...await this.voiceTranscripts(replyFiles, "Replied voice transcript"),
        ];
        prompt = appendTelegramListSection(prompt, "outputs", transcripts);
        if (files.length || replyFiles.length)
            prompt += `\n\n[chat-attachments] ${this.attachmentArchive(target)}`;
        return prompt;
    }
    async command(admission, command) {
        const target = admission.target;
        if (command.name === "help") {
            await this.reply(admission, HELP);
            return;
        }
        if (command.name === "workspace") {
            if (this.data.groupId !== undefined) {
                if (target.chatId !== this.data.groupId)
                    throw new Error("Workspace is already bound to a different group; rebinding is refused");
                await this.reply(admission, `Workspace is bound to group ${this.data.groupId}. Use /running and /attach <full-session-id>, /new [name], or /bind in an existing topic.`);
                return;
            }
            const chat = await this.api("getChat", { chat_id: target.chatId });
            if (!record(chat) || chat.id !== target.chatId || chat.type !== "supergroup" || chat.is_forum !== true || target.chatId >= 0)
                throw new Error("/workspace requires an is_forum supergroup");
            const member = await this.api("getChatMember", { chat_id: target.chatId, user_id: this.options.botId });
            if (!record(member) || !record(member.user) || member.user.id !== this.options.botId
                || member.status !== "administrator" || member.can_manage_topics !== true)
                throw new Error("Bot must be an administrator with can_manage_topics");
            this.data.groupId = target.chatId;
            await this.save();
            await this.reply(admission, "Workspace bound. Use /running and /attach <full-session-id> for a live terminal, /new [name] to create a chat topic, or /bind inside an existing non-General topic.");
            return;
        }
        if (!["new", "bind", "running", "attach", "sessions", "status", "repair", "abort", "btw", "answer", "cancel"].includes(command.name))
            throw new Error("Unsupported slash command. Use /help; slash commands are never forwarded to OMP");
        this.requireGroup(target);
        if (command.name === "running") {
            if (command.argument)
                throw new Error("Usage: /running");
            const running = [];
            for (const [port, registration] of this.terminals) {
                if (!registration.registered)
                    continue;
                let state;
                try {
                    state = await this.readTerminal(port, registration);
                }
                catch {
                    continue;
                }
                const binding = this.data.bindings.find((entry) => entry.id === registration.bindingId);
                const active = state.isStreaming === true || (binding !== undefined && this.busy.has(binding.threadId));
                running.push(`OMP session: ${state.sessionId}\nCwd: ${state.cwd}\n${active ? "Busy" : "Idle"}\n${binding ? this.topicLocation(binding) : "Telegram topic: not attached"}`);
            }
            await this.reply(admission, running.length ? running.join("\n\n") : "No live terminals registered. Open OMP in a terminal to make it discoverable, then use /running and /attach <full-session-id>.");
            return;
        }
        if (command.name === "attach") {
            if (!/^\S+$/.test(command.argument))
                throw new Error("Usage: /attach <full-session-id>");
            let entry;
            for (const candidate of this.terminals) {
                if (candidate[1].registered && candidate[1].identity?.sessionId === command.argument) {
                    entry = candidate;
                    break;
                }
            }
            if (!entry)
                throw new Error("No live terminal with that exact full session ID. Use /running; prefix matching and automatic spawn/resume are forbidden");
            admission.phase = "running";
            await this.save();
            const binding = await this.attach(entry[0], basename(entry[1].identity.cwd));
            await this.reply(admission, `Terminal attached.\n${this.describe(binding)}\n${this.topicLocation(binding)}`);
            return;
        }
        if (command.name === "new" || command.name === "bind") {
            if (command.name === "bind") {
                if (!integer(target.threadId, 2))
                    throw new Error("/bind must be sent inside an existing non-General topic");
                const existing = this.data.bindings.find((binding) => binding.threadId === target.threadId);
                if (existing) {
                    await this.reply(admission, `Already bound; the designated session will not be replaced.\n${this.describe(existing)}`);
                    return;
                }
            }
            let argument = command.argument;
            let cwd = this.options.cwd;
            const directoryOption = command.name === "new" ? /(?:^|\s)--cwd(?:\s+|$)/.exec(argument) : null;
            if (directoryOption) {
                const directory = argument.slice(directoryOption.index + directoryOption[0].length).trim();
                if (!directory)
                    throw new Error("/new --cwd requires a directory");
                cwd = await realpath(resolve(this.options.cwd, directory));
                if (!(await stat(cwd)).isDirectory())
                    throw new Error("The selected working directory is not a directory");
                argument = argument.slice(0, directoryOption.index).trim() || basename(cwd);
            }
            let id;
            do {
                id = randomBytes(6).toString("hex");
            } while (this.data.bindings.some((binding) => binding.id === id));
            const prefix = `[${id}] `;
            const name = [...(argument.replace(/\s+/g, " ").trim() || "Chat")].slice(0, 128 - prefix.length).join("");
            let threadId = target.threadId;
            if (command.name === "new") {
                admission.phase = "running";
                await this.save();
                // Never retry this non-idempotent method after a transport error.
                const topic = await this.api("createForumTopic", { chat_id: target.chatId, name: `${prefix}${name}` });
                if (!record(topic) || !integer(topic.message_thread_id, 2))
                    throw new Error("createForumTopic returned an invalid topic ID; inspect the group before retrying");
                threadId = topic.message_thread_id;
                if (this.data.bindings.some((binding) => binding.threadId === threadId))
                    throw new Error("Created topic is already bound; refusing to replace its session");
            }
            const binding = { id, threadId: threadId, name, cwd };
            this.data.bindings.push(binding);
            await this.save();
            await this.reply(admission, `Chat created.\n${this.describe(binding)}`, { chatId: target.chatId, threadId: binding.threadId });
            return;
        }
        if (command.name === "sessions") {
            await this.reply(admission, this.data.bindings.length ? this.data.bindings.map((binding) => this.describe(binding)).join("\n\n") : "No chats yet. Use /new [name] or /bind in a topic.");
            return;
        }
        const binding = this.requireBinding(target);
        if (command.name === "repair") {
            if (command.argument)
                throw new Error("Usage: /repair");
            if (binding.external)
                throw new Error("Terminal-owned topics must be repaired by reopening and attaching their designated OMP session");
            if (!binding.sessionFile) {
                await this.reply(admission, "No transcript is designated. Send your message again to initialize this topic.");
                return;
            }
            try {
                if ((await stat(binding.sessionFile)).isFile())
                    throw new Error("The designated transcript still exists; repair refused to preserve its history");
                throw new Error("The designated transcript path is not a regular file; inspect it manually");
            }
            catch (error) {
                if (!(record(error) && error.code === "ENOENT"))
                    throw error;
            }
            const alternatives = (await readdir(dirname(binding.sessionFile), { withFileTypes: true }))
                .filter((entry) => entry.isFile() && entry.name.endsWith(".jsonl"));
            if (alternatives.length)
                throw new Error("Other transcripts exist in this topic's session directory; inspect them manually before repair");
            const existing = this.sessions.get(binding.id);
            if (existing) {
                try {
                    await existing;
                    throw new Error("This topic still has a live OMP process; repair refused");
                }
                catch (error) {
                    const failure = error instanceof SessionLifecycleError && error.shutdownConfirmed ? error.cause : error;
                    if (this.sessions.get(binding.id) === existing && record(failure) && failure.code === "ENOENT")
                        this.sessions.delete(binding.id);
                    else
                        throw error;
                }
            }
            delete binding.sessionId;
            delete binding.sessionFile;
            await this.save();
            await this.reply(admission, "Missing transcript released. Send your message again to start a fresh OMP session; this topic and its attachment archive are unchanged.");
            return;
        }
        if (command.name === "btw") {
            if (!command.argument)
                throw new Error("Usage: /btw <question>");
            const session = await this.session(binding);
            await this.assertIdentity(binding, session);
            admission.phase = "running";
            await this.save();
            if (binding.external)
                this.externalRegistration(binding, session);
            const stopTyping = this.startTyping(target);
            let output;
            try {
                output = await session.btw(command.argument);
            }
            finally {
                stopTyping();
            }
            await this.replySidecar(admission, output || "OMP completed the side question without a text reply.");
            return;
        }
        if (command.name === "status") {
            const requests = [...(this.pending.get(binding.id)?.keys() ?? [])];
            const live = binding.external ? this.sessions.get(binding.id) : undefined;
            const active = this.busy.has(binding.threadId) || (live !== undefined && (await (await live).state()).isStreaming === true);
            await this.reply(admission, `${this.describe(binding)}\n${active ? "Busy" : "Idle"}\nPending UI: ${binding.external ? "use the local terminal for interactive dialogs" : requests.join(", ") || "none"}`);
            return;
        }
        if (command.name === "abort") {
            const live = this.sessions.get(binding.id);
            if (live) {
                const session = await live;
                await this.assertIdentity(binding, session);
                if (binding.external)
                    this.externalRegistration(binding, session);
                await session.abort();
                await this.reply(admission, "Abort requested for this topic only.");
            }
            else
                await this.reply(admission, "No live OMP process in this topic.");
            return;
        }
        const match = /^(\S+)(?:\s+([\s\S]+))?$/.exec(command.argument);
        if (!match || (command.name === "answer" ? !match[2] : Boolean(match[2])))
            throw new Error(command.name === "answer" ? "Usage: /answer <request-id> <JSON object>" : "Usage: /cancel <request-id>");
        const request = this.pending.get(binding.id)?.get(match[1]);
        if (!request)
            throw new Error("No such pending UI request in this exact topic");
        let response = { cancelled: true };
        if (command.name === "answer") {
            try {
                response = JSON.parse(match[2]);
            }
            catch {
                throw new Error("Answer must contain valid JSON");
            }
            validateAnswer(request, response);
        }
        const live = this.sessions.get(binding.id);
        if (!live)
            throw new Error("This topic has no live session for the pending UI request");
        this.pending.get(binding.id).delete(request.id);
        admission.phase = "running";
        await this.save();
        await (await live).answer(request.id, response);
        await this.reply(admission, `UI request ${request.id} ${command.name === "cancel" ? "cancelled" : "answered"}.`);
    }
    requireGroup(target) {
        if (this.data.groupId === undefined)
            throw new Error("Workspace is unbound. Send /workspace in your forum supergroup first");
        if (target.chatId !== this.data.groupId)
            throw new Error("Input is accepted only in the bound Workspace group");
    }
    requireBinding(target) {
        const binding = this.data.bindings.find((candidate) => candidate.threadId === target.threadId);
        if (!binding)
            throw new Error("This topic is not bound. Use /new [name] or /bind in a non-General topic");
        return binding;
    }
    describe(binding) {
        return `Chat ${binding.id}: ${binding.name}\nTelegram topic: ${binding.threadId}\nOMP session: ${binding.sessionId ?? "uninitialized (created on first message)"}\nTranscript: ${binding.sessionFile ?? "not recorded"}\nCwd: ${binding.cwd}${binding.external ? `\nTerminal: ${this.sessions.has(binding.id) ? "connected" : "offline — reopen the designated terminal, use /running and /attach <full-session-id>, or /telegram-connect there"}` : ""}`;
    }
    topicLocation(binding) {
        const group = String(this.data.groupId);
        return `Telegram topic: ${binding.threadId}${group.startsWith("-100") && group.length > 4 ? `\nhttps://t.me/c/${group.slice(4)}/${binding.threadId}` : `\nGroup: ${group}`}`;
    }
    externalRegistration(binding, session) {
        const registration = this.terminals.get(session);
        if (!registration?.registered || registration.bindingId !== binding.id)
            throw new Error("Designated terminal is offline; use /running and /attach <full-session-id>");
        this.requireRegistration(session, registration);
        return registration;
    }
    async assertIdentity(binding, session) {
        if (binding.external) {
            await this.readTerminal(session, this.externalRegistration(binding, session));
            return;
        }
        const state = await session.state();
        if (state.sessionId !== binding.sessionId || state.sessionFile !== binding.sessionFile)
            throw new Error("OMP session identity differs from the designated transcript; refusing input");
    }
    async session(binding, create = false) {
        const existing = this.sessions.get(binding.id);
        if (existing)
            return existing;
        if (this.stopping)
            throw new Error("Workspace is stopping");
        if (binding.external)
            throw new Error("Designated terminal is offline. Reopen that same session, use /running and /attach <full-session-id>, or /telegram-connect there; automatic spawn/resume is forbidden");
        if (!create && (!binding.sessionId || !binding.sessionFile))
            throw new Error("This topic has no designated transcript; send a normal message to initialize it");
        const opening = (async () => {
            const port = await this.openSession({ ...binding }, (request) => this.onUi(binding, request));
            try {
                if (this.stopping)
                    throw new Error("Workspace is stopping");
                const state = await port.state();
                if (!text(state.sessionId) || !text(state.sessionFile) || !isAbsolute(state.sessionFile))
                    throw new Error("OMP did not return a persistent sessionId and absolute sessionFile");
                if (!create && (state.sessionId !== binding.sessionId || state.sessionFile !== binding.sessionFile))
                    throw new Error("OMP resumed a different transcript; refusing to replace the designated session");
                if (create) {
                    const file = await canonicalFile(state.sessionFile);
                    for (const other of this.data.bindings) {
                        if (other !== binding && (other.sessionId === state.sessionId
                            || (other.sessionFile !== undefined && await canonicalFile(other.sessionFile) === file))) {
                            throw new Error("OMP returned a transcript already designated to another topic; refusing shared sessions");
                        }
                    }
                    for (const registration of this.terminals.values()) {
                        if (registration.identity && (registration.identity.sessionId === state.sessionId || registration.canonicalFile === file)) {
                            throw new Error("OMP returned a transcript owned by a registered live terminal; refusing shared sessions");
                        }
                    }
                    binding.sessionId = state.sessionId;
                    binding.sessionFile = state.sessionFile;
                    await this.save();
                }
                return port;
            }
            catch (error) {
                try {
                    await port.stop();
                }
                catch (shutdown) {
                    if (shutdown instanceof SessionLifecycleError && shutdown.shutdownConfirmed)
                        throw new SessionLifecycleError(error, true);
                    throw new SessionLifecycleError(new AggregateError([error, shutdown], "OMP initialization failed and child shutdown is uncertain"), false);
                }
                throw new SessionLifecycleError(error, true);
            }
        })();
        this.sessions.set(binding.id, opening);
        // Keep a failed opening latched: a second command must not silently create a replacement.
        return opening;
    }
    onUi(binding, request) {
        if (!text(request.id) || !text(request.method))
            return;
        const target = { chatId: this.data.groupId, threadId: binding.threadId };
        if (request.method === "cancel") {
            if (typeof request.targetId === "string")
                this.pending.get(binding.id)?.delete(request.targetId);
            this.track(this.send(target, `UI request ${String(request.targetId ?? request.id)} is no longer pending.`).catch((error) => console.error(`Workspace UI delivery failed: ${diagnostic(error)}`)));
            return;
        }
        if (Object.hasOwn(DIALOGS, request.method)) {
            let pending = this.pending.get(binding.id);
            if (!pending) {
                pending = new Map();
                this.pending.set(binding.id, pending);
            }
            if (pending.has(request.id))
                return;
            pending.set(request.id, request);
            const details = request.secret === true ? "Secret input is unsupported over Telegram; use /cancel and the local terminal."
                : [request.title, request.message, request.options && JSON.stringify(request.options), request.questions && JSON.stringify(request.questions)].filter((part) => typeof part === "string").join("\n");
            this.track(this.send(target, `UI ${request.method} request ${request.id}\n${details}\n/answer ${request.id} <JSON object>\n/cancel ${request.id}`).catch((error) => console.error(`Workspace UI delivery failed: ${diagnostic(error)}`)));
        }
        else if (request.method === "notify" && typeof request.message === "string") {
            this.track(this.send(target, `OMP: ${request.message}`).catch((error) => console.error(`Workspace UI delivery failed: ${diagnostic(error)}`)));
        }
    }
    startTyping(target) {
        let stopped = false;
        let sending = false;
        let failureReported = false;
        const refresh = async () => {
            if (stopped || sending)
                return;
            sending = true;
            try {
                await this.api("sendChatAction", { chat_id: target.chatId, ...getTelegramTargetThreadParams(target), action: "typing" });
                failureReported = false;
            }
            catch (error) {
                if (!stopped && !failureReported) {
                    failureReported = true;
                    console.error(`Workspace typing action failed: ${diagnostic(error)}`);
                }
            }
            finally {
                sending = false;
            }
        };
        void refresh();
        const timer = setInterval(() => void refresh(), TYPING_REFRESH_MS);
        timer.unref();
        return () => {
            stopped = true;
            clearInterval(timer);
        };
    }
    async localArtifacts(binding, body) {
        const candidates = [];
        if (binding.sessionFile) {
            const root = join(dirname(binding.sessionFile), basename(binding.sessionFile, ".jsonl"), "local");
            for (const match of body.matchAll(LOCAL_ARTIFACT_URI)) {
                if (!isAbsolute(match[1]))
                    candidates.push({ root, name: match[1] });
            }
        }
        for (const destination of extractMarkdownLinkDestinations(body)) {
            if (!/^(?:[a-z][a-z\d+.-]*:|\/\/|#)/i.test(destination)) {
                candidates.push({ root: binding.cwd, name: destination });
            }
        }
        const roots = new Map();
        const files = [];
        const seen = new Set();
        for (const candidate of candidates) {
            let artifactName;
            try {
                artifactName = decodeURIComponent(candidate.name);
            }
            catch {
                continue;
            }
            if (!artifactName || artifactName.includes("\0"))
                continue;
            let root;
            let file;
            try {
                root = roots.get(candidate.root) ?? await realpath(candidate.root);
                roots.set(candidate.root, root);
                file = await realpath(resolve(root, artifactName));
            }
            catch (error) {
                if (record(error) && (error.code === "ENOENT" || error.code === "ENOTDIR"))
                    continue;
                throw error;
            }
            const contained = relative(root, file);
            if (!contained || contained === ".." || contained.startsWith(`..${sep}`) || isAbsolute(contained) || seen.has(file))
                continue;
            const info = await stat(file);
            if (!info.isFile())
                continue;
            if (info.size > MAX_LOCAL_ARTIFACT_BYTES) {
                throw new Error(`${basename(file)} exceeds the ${MAX_LOCAL_ARTIFACT_BYTES}-byte Telegram file limit`);
            }
            if (files.length === MAX_LOCAL_ARTIFACTS)
                throw new Error(`OMP reply contains more than ${MAX_LOCAL_ARTIFACTS} local files`);
            seen.add(file);
            files.push(file);
        }
        return files;
    }
    async replyWithArtifacts(admission, binding, body) {
        const files = await this.localArtifacts(binding, body);
        await this.reply(admission, body);
        for (const file of files) {
            await this.sendFile("sendDocument", {
                chat_id: String(admission.target.chatId),
                ...(admission.target.threadId === undefined ? {} : { message_thread_id: String(admission.target.threadId) }),
            }, "document", file, basename(file));
        }
    }
    async reply(admission, body, target = admission.target) {
        admission.phase = "delivery";
        await this.save();
        await this.send(target, body);
    }
    async replySidecar(admission, body) {
        admission.phase = "delivery";
        await this.save();
        await this.sendSidecar(admission.target, body);
    }
    async send(target, body) {
        // Conservative UTF-16 chunks also fit Telegram's 4096-character ceiling.
        for (let start = 0; start < body.length;) {
            let end = Math.min(start + 4096, body.length);
            if (end < body.length && /[\uD800-\uDBFF]/.test(body[end - 1]))
                end--;
            await this.api("sendMessage", { chat_id: target.chatId, ...getTelegramTargetThreadParams(target), text: body.slice(start, end) });
            start = end;
        }
    }
    async sendSidecar(target, body) {
        for (let start = 0; start < body.length;) {
            let end = Math.min(start + 4_000, body.length);
            if (end < body.length && /[\uD800-\uDBFF]/.test(body[end - 1]))
                end--;
            await this.api("sendMessage", {
                chat_id: target.chatId,
                ...getTelegramTargetThreadParams(target),
                text: `💭 <b>Sidecar</b>\n<blockquote>${escapeHtml(body.slice(start, end))}</blockquote>`,
                parse_mode: "HTML",
            });
            start = end;
        }
    }
    async complete(admission) {
        this.data.admissions = this.data.admissions.filter((entry) => entry !== admission);
        await this.save();
    }
    save() {
        const source = JSON.stringify(this.data, null, 2) + "\n";
        const saved = this.writing.then(async () => {
            const temporary = `${this.options.statePath}.${randomUUID()}.tmp`;
            try {
                const file = await open(temporary, "wx", 0o600);
                try {
                    await file.writeFile(source, "utf8");
                    await file.sync();
                }
                finally {
                    await file.close();
                }
                await rename(temporary, this.options.statePath);
                const directory = await open(dirname(this.options.statePath), "r");
                try {
                    await directory.sync();
                }
                finally {
                    await directory.close();
                }
            }
            catch (error) {
                await unlink(temporary).catch(() => { });
                throw error;
            }
        });
        this.writing = saved;
        return saved.catch((error) => {
            this.fatal = new Error(`Workspace state persistence failed; refusing further input: ${diagnostic(error)}`);
            throw this.fatal;
        });
    }
    track(job) {
        this.jobs.add(job);
        void job.finally(() => this.jobs.delete(job)).catch((error) => console.error(`Workspace background failure: ${diagnostic(error)}`));
    }
    async stop() {
        this.stopping = true;
        await this.admitting;
        await this.attaching;
        while (this.jobs.size)
            await Promise.allSettled([...this.jobs]);
        for (const port of this.terminals.keys())
            this.detach(port);
        const opened = await Promise.allSettled([...this.sessions.values()]);
        const shutdowns = await Promise.allSettled(opened.flatMap((result) => result.status === "fulfilled" ? [result.value.stop()] : []));
        // Failed operations are not live-child proof; ignore only explicitly confirmed startup cleanup.
        const failures = [...opened.filter((result) => result.status === "rejected"
                && !(result.reason instanceof SessionLifecycleError && result.reason.shutdownConfirmed)), ...shutdowns]
            .filter((result) => result.status === "rejected").map((result) => result.reason);
        if (failures.length)
            throw new AggregateError(failures, "OMP child shutdown is uncertain; supervisor ownership must be retained");
        await this.writing;
    }
}
