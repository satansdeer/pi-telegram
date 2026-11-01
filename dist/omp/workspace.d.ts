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
type Options = {
    statePath: string;
    cwd: string;
    ownerId: number;
    botId: number;
    botUsername: string;
};
export declare class WorkspaceBridge {
    private readonly options;
    private readonly api;
    private readonly openSession;
    private data;
    private initialized;
    private stopping;
    private fatal?;
    private admitting;
    private writing;
    private readonly jobs;
    private readonly sessions;
    private readonly pending;
    private readonly busy;
    private groupSetup;
    constructor(options: Options, api: Api, openSession: OpenSession);
    get offset(): number;
    init(): Promise<void>;
    /** Admit durably, then run turns in the background so controls stay responsive. */
    handle(update: unknown): Promise<void>;
    private admit;
    private process;
    private command;
    private requireGroup;
    private requireBinding;
    private describe;
    private assertIdentity;
    private session;
    private onUi;
    private reply;
    private send;
    private complete;
    private save;
    private track;
    stop(): Promise<void>;
}
export {};
