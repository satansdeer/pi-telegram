export type UiRequest = {
    id: string;
    method: string;
    [key: string]: unknown;
};
/** Carries child-ownership proof without treating a failed operation as success. */
export declare class SessionLifecycleError extends Error {
    readonly shutdownConfirmed: boolean;
    constructor(cause: unknown, shutdownConfirmed: boolean);
}
export declare class OmpSession {
    private readonly child;
    private readonly pending;
    private readonly exited;
    private readonly recovered;
    private readonly directory;
    private readonly onUi;
    private turn?;
    private sidecar?;
    private failure?;
    private exit?;
    private sequence;
    private streaming;
    private stopping;
    constructor(options: {
        binary: string;
        cwd: string;
        sessionDir: string;
        resume?: string;
        onUi: (request: UiRequest) => void;
        args?: string[];
    });
    get busy(): boolean;
    ready(): Promise<Record<string, unknown>>;
    state(): Promise<Record<string, unknown>>;
    setModel(provider: string, modelId: string): Promise<void>;
    prompt(text: string, onProgress?: (text: string) => void): Promise<string>;
    steer(text: string): Promise<void>;
    btw(question: string): Promise<string>;
    abort(): Promise<void>;
    answer(id: string, response: Record<string, unknown>): Promise<void>;
    stop(): Promise<void>;
    private request;
    private send;
    private handleFrame;
    private handleBtwRecord;
    private finishTurn;
    private fail;
}
