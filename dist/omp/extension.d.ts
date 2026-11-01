/**
 * Registers the current OMP terminal for Telegram discovery without spawning an agent.
 * Zones: native lifecycle, pure registration, authorized attachment, terminal turn ownership.
 */
import { connectWorkspace } from "./local.ts";
type Events = {
    agent_start: {
        type: "agent_start";
    };
    agent_end: {
        type: "agent_end";
        messages: unknown[];
        willContinue?: boolean;
    };
    turn_end: {
        type: "turn_end";
        message: unknown;
    };
    input: {
        type: "input";
        text: string;
        source: "interactive" | "rpc" | "extension";
    };
    session_start: unknown;
    session_before_switch: unknown;
    session_switch: unknown;
    session_before_branch: unknown;
    session_branch: unknown;
    session_before_tree: unknown;
    session_tree: unknown;
    session_shutdown: unknown;
};
export interface TerminalContext {
    cwd: string;
    agent?: {
        kind: "main" | "sub";
    };
    sessionManager: {
        getSessionId(): string;
        getSessionFile(): string | undefined;
    };
    isIdle(): boolean;
    hasPendingMessages(): boolean;
    abort(): void;
    ui: {
        notify(message: string, type?: "info" | "warning" | "error"): void;
    };
}
export interface TerminalHost {
    registerCommand(name: string, options: {
        description?: string;
        handler(args: string, ctx: TerminalContext): Promise<void>;
    }): void;
    sendUserMessage(text: string): void;
    on<K extends keyof Events>(event: K, handler: (event: Events[K], ctx: TerminalContext) => void | {
        handled: true;
    }): void;
}
export default function telegramExtension(pi: TerminalHost, connect?: typeof connectWorkspace): void;
export {};
