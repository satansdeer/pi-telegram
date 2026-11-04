export interface Binding {
    id: string;
    threadId: number;
    name: string;
    cwd: string;
    sessionId?: string;
    sessionFile?: string;
    /** Terminal-owned process: never spawn or resume it from this daemon. */
    external?: true;
}
export interface UiRequest {
    id: string;
    method: string;
    [key: string]: unknown;
}
export interface SessionPort {
    prompt(text: string): Promise<string>;
    steer(text: string): Promise<void>;
    btw(text: string): Promise<string>;
    abort(): Promise<void>;
    state(): Promise<Record<string, unknown>>;
    answer(id: string, response: Record<string, unknown>): Promise<void>;
    stop(): Promise<void>;
}
type Api = (method: string, params: Record<string, unknown>) => Promise<unknown>;
type SendFile = (method: string, fields: Record<string, string>, fileField: string, filePath: string, fileName: string) => Promise<unknown>;
type OpenSession = (binding: Binding, onUi: (request: UiRequest) => void) => Promise<SessionPort>;
type DownloadFile = (fileId: string, fileName: string, directory: string) => Promise<string>;
type TranscribeVoice = (filePath: string) => Promise<string>;
type Options = {
    statePath: string;
    cwd: string;
    ownerId: number;
    botId: number;
    botUsername: string;
};
type TerminalIdentity = {
    sessionId: string;
    sessionFile: string;
    cwd: string;
};
type TerminalBinding = Binding & TerminalIdentity & {
    groupId: number;
};
export declare class WorkspaceBridge {
    private readonly options;
    private readonly api;
    private readonly openSession;
    private readonly downloadFile;
    private readonly sendFile;
    private readonly transcribeVoice;
    private data;
    private initialized;
    private stopping;
    private fatal?;
    private admitting;
    private attaching;
    private writing;
    private readonly jobs;
    private readonly sessions;
    private readonly terminals;
    private readonly pending;
    private readonly busy;
    private readonly starts;
    private groupSetup;
    constructor(options: Options, api: Api, openSession: OpenSession, downloadFile: DownloadFile, sendFile: SendFile, transcribeVoice: TranscribeVoice);
    get offset(): number;
    /** Registration is ephemeral: no topic creation, persistence, or implicit reattachment. */
    register(port: SessionPort, onAttach: (binding: TerminalBinding) => Promise<void>): Promise<void>;
    /** A separate queue keeps slow attachments out of Telegram admission/control handling. */
    attach(port: SessionPort, label: string): Promise<TerminalBinding>;
    detach(port: SessionPort): void;
    private requireRegistration;
    private readTerminal;
    private terminalBinding;
    private attachedBinding;
    init(): Promise<void>;
    /** Admit durably, then run turns in the background so controls stay responsive. */
    handle(update: unknown): Promise<void>;
    private admit;
    private process;
    private attachmentArchive;
    private attachmentFiles;
    private voiceTranscripts;
    private attachmentPrompt;
    private command;
    private requireGroup;
    private requireBinding;
    private describe;
    private topicLocation;
    private externalRegistration;
    private assertIdentity;
    private session;
    private onUi;
    private startTyping;
    private localArtifacts;
    private replyWithArtifacts;
    private reply;
    private replySidecar;
    private send;
    private sendSidecar;
    private complete;
    private save;
    private track;
    stop(): Promise<void>;
}
export {};
