/**
 * Exercises the OMP transport with an isolated, model-free child process.
 * Zones: omp rpc, process lifecycle, session persistence
 */
import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import test from "node:test";
import { OmpSession, SessionLifecycleError } from "../omp/rpc.ts";
import type { UiRequest } from "../omp/rpc.ts";

const fixture = String.raw`
import { readFileSync, readdirSync, writeFileSync } from "node:fs";
import { basename, join } from "node:path";
import { createInterface } from "node:readline";
const args = process.argv.slice(2);
const directory = args[args.indexOf("--session-dir") + 1];
const resume = args.includes("--resume") ? args[args.indexOf("--resume") + 1] : undefined;
const continued = args.includes("--continue");
const sessionFile = resume ?? (continued ? join(directory, readdirSync(directory).find(name => name.endsWith(".jsonl"))) : join(directory, "fixture.jsonl"));
const sessionId = resume || continued ? JSON.parse(readFileSync(sessionFile, "utf8").trim()).id : "session-" + basename(directory);
if (!resume && !continued) writeFileSync(sessionFile, JSON.stringify({type: "session", id: sessionId}) + "\n");
const send = frame => process.stdout.write(JSON.stringify(frame) + "\n");
const reply = (frame, data = {}) => send({type: "response", id: frame.id, command: frame.type, success: true, data});
let model = {provider: "fixture", id: "default"};
let active;
let btw;
let steering = [];
function finish(text, status = "completed") {
  const current = active;
  active = undefined;
  const message = {role: "assistant", content: [{type: "text", text: text ?? ""}], stopReason: status === "completed" ? "stop" : status,
    ...(status === "error" ? {errorMessage: "provider rejected the turn"} : {})};
  if (text !== null || status === "aborted") send({type: "message_end", message});
  send({type: "agent_end", isTerminal: true, messages: text === null ? [] : [message]});
  if (!current.reported) send({type: "prompt_result", id: current.id, agentInvoked: true, status, sessionSettled: true,
    ...(status === "error" ? {error: {message: "provider rejected the turn"}} : {})});
}
const input = createInterface({input: process.stdin, crlfDelay: Infinity});
input.on("line", line => {
  const frame = JSON.parse(line);
  if (frame.type === "get_state") {
    reply(frame, {sessionId: args.includes("--wrong-session") ? "wrong-session" : sessionId, sessionFile, pid: process.pid, isStreaming: !!active, continued, resume: resume ?? null, model,
      tokenPresent: Object.keys(process.env).some(key => /TELEGRAM.*TOKEN|BOT_?TOKEN/i.test(key))});
  } else if (frame.type === "set_model") {
    model = {provider: frame.provider, id: frame.modelId};
    reply(frame, model);
  } else if (frame.type === "prompt") {
    if (frame.message === "exit") process.exit(7);
    reply(frame, {agentInvoked: true});
    active = frame;
    send({type: "agent_start"});
    if (frame.message === "fail") finish(null, "error");
    else if (frame.message === "empty") finish(null);
    else {
      send({type: "message_end", message: {role: "assistant", stopReason: "toolUse", content: [
        {type: "thinking", thinking: "private reasoning"}, {type: "text", text: "draft"}]}});
      send({type: "agent_end", isTerminal: false, messages: []});
      send({type: "prompt_result", id: frame.id, agentInvoked: true, status: "completed", sessionSettled: false});
      active.reported = true;
      send({type: "extension_ui_request", id: "ui-" + frame.id, method: "input", prompt: frame.message});
    }
  } else if (frame.type === "btw") {
    const now = Date.now();
    btw = {id: "btw-" + frame.id, leafId: null, question: frame.question, answer: "", status: "running", createdAt: now, updatedAt: now};
    send({type: "btw_record", record: btw});
    reply(frame, {record: btw});
    btw = frame.question === "fail-sidecar"
      ? {...btw, status: "error", error: "sidecar provider failed", updatedAt: Date.now()}
      : {...btw, status: "complete", answer: "side:" + frame.question, updatedAt: Date.now()};
    send({type: "btw_record", record: btw});
    btw = undefined;
  } else if (frame.type === "steer") {
    steering.push(frame.message);
    reply(frame);
  } else if (frame.type === "extension_ui_response" && active) {
    if (frame.cancelled) finish(null, "aborted");
    else finish("final:" + active.message + (steering.length ? "|steer:" + steering.join(",") : ""));
  } else if (frame.type === "abort") {
    reply(frame);
    if (active) finish(null, "aborted");
  }
});
input.on("close", () => {
  if (active) finish("drained");
  if (args.includes("--failed-exit")) process.exitCode = 7;
});
`;

test("OMP admits one turn, keeps controls responsive, and never reuses old output or hides failures", { timeout: 15_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-rpc-"));
  const script = join(root, "fixture.mjs");
  await writeFile(script, fixture);
  const ui = Promise.withResolvers<UiRequest>();
  const oldToken = process.env.OMP_TELEGRAM_BOT_TOKEN;
  process.env.OMP_TELEGRAM_BOT_TOKEN = "123456:private-test-token";
  const session = new OmpSession({
    binary: process.execPath, args: [script], cwd: root, sessionDir: join(root, "topic"),
    onUi(request) {
      if (request.prompt === "ui-failure") throw new Error("UI delivery failed");
      ui.resolve(request);
    },
  });
  try {
    const state = await session.ready();
    assert.equal(state.tokenPresent, false);
    assert.equal(state.continued, false);
    assert.equal((await stat(join(root, "topic"))).mode & 0o777, 0o700);
    await session.setModel("openai-codex", "gpt-6.1-sol");
    assert.deepEqual((await session.state()).model, { provider: "openai-codex", id: "gpt-6.1-sol" });
    const progress: string[] = [];
    const first = session.prompt("first", (text) => { progress.push(text); });
    assert.equal(session.busy, true);
    await assert.rejects(session.prompt("concurrent"), /already processing/);
    const request = await ui.promise;
    let settled = false;
    void first.then(() => { settled = true; }, () => { settled = true; });
    assert.equal((await session.state()).isStreaming, true);
    assert.equal(settled, false, "a nonterminal agent_end and early result must not release the turn");
    assert.deepEqual(progress, ["draft"], "public progress must arrive while the turn is still running");
    assert.equal(await session.btw("what is happening?"), "side:what is happening?");
    assert.equal(settled, false, "an ephemeral side question must not settle the main turn");
    await assert.rejects(session.btw("fail-sidecar"), /sidecar provider failed/);
    assert.equal(session.busy, true);
    await session.steer("change direction");
    await session.answer(request.id, { value: "finish" });
    assert.equal(await first, "final:first|steer:change direction");
    assert.deepEqual(progress, ["draft"], "the final answer must not be repeated as progress");
    assert.equal(session.busy, false);
    await assert.rejects(session.steer("too late"), /no active prompt/);
    assert.equal(await session.prompt("empty"), "", "an empty turn must not return the previous reply");
    await assert.rejects(session.prompt("fail"), /provider rejected the turn/);
    assert.equal(session.busy, false);
    await assert.rejects(session.prompt("ui-failure"), /UI delivery failed/);
    await session.state(); // An ordered barrier after the automatic cancellation response.
    assert.equal(session.busy, false);
    const aborted = session.prompt("abort-me");
    const abortedCheck = assert.rejects(aborted, /aborted/);
    await session.state();
    await session.abort();
    await abortedCheck;
    const draining = session.prompt("drain-me");
    await session.state();
    await session.stop();
    assert.equal(await draining, "drained", "EOF must drain accepted work rather than kill it");
  } finally {
    if (oldToken === undefined) delete process.env.OMP_TELEGRAM_BOT_TOKEN;
    else process.env.OMP_TELEGRAM_BOT_TOKEN = oldToken;
    try { await session.stop(); } finally { await rm(root, { recursive: true, force: true }); }
  }
});

test("OMP keeps topic transcripts isolated, recovers only existing files, and rejects exit waiters", { timeout: 15_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-rpc-resume-"));
  const script = join(root, "fixture.mjs");
  await writeFile(script, fixture);
  const sessions: OmpSession[] = [];
  function open(name: string, resume?: string, extra: string[] = []): OmpSession {
    const session = new OmpSession({ binary: process.execPath, args: [script, ...extra], cwd: root, sessionDir: join(root, name), resume, onUi() {} });
    sessions.push(session);
    return session;
  }
  try {
    const first = open("one");
    const firstState = await first.ready();
    await first.stop();
    const recovered = open("one");
    const recoveredState = await recovered.ready();
    assert.equal(recoveredState.continued, true);
    assert.equal(recoveredState.sessionId, firstState.sessionId);
    assert.equal(recoveredState.sessionFile, firstState.sessionFile);
    await recovered.stop();
    const resumed = open("one", String(firstState.sessionFile));
    const resumedState = await resumed.ready();
    assert.equal(resumedState.continued, false);
    assert.equal(resumedState.resume, firstState.sessionFile);
    assert.equal(resumedState.sessionId, firstState.sessionId);
    const mismatched = open("one", String(firstState.sessionFile), ["--wrong-session"]);
    await assert.rejects(mismatched.ready(), (error: unknown) => error instanceof SessionLifecycleError
      && error.shutdownConfirmed && /did not resume the persisted transcript/.test(error.message));
    await assert.rejects(mismatched.state(), /exited/, "failed startup must have closed its child");
    const other = open("two");
    const otherState = await other.ready();
    assert.notEqual(otherState.sessionId, firstState.sessionId);
    assert.notEqual(otherState.sessionFile, firstState.sessionFile);
    assert.equal(otherState.continued, false);
    assert.throws(() => open("one", join(root, "missing.jsonl")), (error: unknown) => error instanceof SessionLifecycleError
      && error.shutdownConfirmed && (error.cause as NodeJS.ErrnoException).code === "ENOENT");
    await mkdir(join(root, "invalid"));
    await writeFile(join(root, "invalid", "broken.jsonl"), "not a transcript\n");
    assert.throws(() => open("invalid"), /no valid session identity/);
    assert.equal(JSON.parse((await readFile(String(firstState.sessionFile), "utf8")).trim()).id, firstState.sessionId);
    const exitPrompt = assert.rejects(other.prompt("exit"), /exited|EPIPE/);
    const exitState = assert.rejects(other.state(), /exited|EPIPE/);
    await Promise.all([exitPrompt, exitState]);
    assert.equal(other.busy, false);
    assert.equal(basename(String(resumedState.sessionFile)), "fixture.jsonl");
  } finally {
    await Promise.all(sessions.map(session => session.stop().catch(() => undefined)));
    await rm(root, { recursive: true, force: true });
  }
});

for (const cleanup of ["failed-exit", "uncertain"] as const) {
  test(`OMP startup distinguishes confirmed ${cleanup === "failed-exit" ? "exit" : "uncertain cleanup"}`, { timeout: 15000 }, async () => {
    const root = await mkdtemp(join(tmpdir(), "omp-rpc-startup-"));
    const script = join(root, "fixture.mjs");
    const resume = join(root, "designated.jsonl");
    await writeFile(script, fixture);
    await writeFile(resume, JSON.stringify({ type: "session", id: "designated-session" }) + "\n");
    const session = new OmpSession({ binary: process.execPath, args: [script, "--wrong-session", ...(cleanup === "failed-exit" ? ["--failed-exit"] : [])],
      cwd: root, sessionDir: join(root, "topic"), resume, onUi() {} });
    const stop = session.stop.bind(session);
    let pid: number | undefined;
    try {
      pid = (await session.state()).pid as number;
      if (cleanup === "uncertain") session.stop = async () => { throw new Error("shutdown outcome unknown"); };
      await assert.rejects(session.ready(), (error: unknown) => error instanceof SessionLifecycleError
        && error.shutdownConfirmed === (cleanup === "failed-exit")
        && (cleanup === "failed-exit" ? /did not resume/.test(error.message) : /child shutdown is uncertain/.test(error.message)));
      if (cleanup === "uncertain") assert.doesNotThrow(() => process.kill(pid!, 0), "uncertain startup must not kill the child");
    } finally {
      await stop().catch(() => undefined);
      await rm(root, { recursive: true, force: true });
    }
  });
}
