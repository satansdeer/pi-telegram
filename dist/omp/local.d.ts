export type LocalHandler = (method: string, params: Record<string, unknown>) => Promise<unknown>;
export interface LocalPeer {
    request(method: string, params?: Record<string, unknown>): Promise<unknown>;
    close(): void;
}
export interface Attachment {
    id: string;
    threadId: number;
    name: string;
    groupId: number;
    sessionId: string;
    sessionFile: string;
    cwd: string;
}
export interface WorkspaceServer {
    close(): Promise<void>;
}
export declare function attachmentValid(value: unknown): value is Attachment;
export declare function connectWorkspace(handler: LocalHandler, onClose: () => void): Promise<LocalPeer>;
/** Caller must already hold the daemon lock before stale socket removal. */
export declare function startWorkspaceServer(stateDir: string, register: (peer: LocalPeer) => Promise<void>, attach: (peer: LocalPeer, name: string) => Promise<Attachment>, detach: (peer: LocalPeer) => void): Promise<WorkspaceServer>;
