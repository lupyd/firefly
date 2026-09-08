export * as protos from './protos/message';
export interface RawUserMessage {
    id: number;
    other: string;
    message: Uint8Array;
    sentByOther: boolean;
    from?: string;
    to?: string;
}
export interface RawGroupMessage {
    id: number;
    groupId: number;
    by: string;
    message: Uint8Array;
    channelId: number;
    epoch: number;
    from?: string;
}
export interface RawGroupInfo {
    id: number;
    name: string;
    description: string;
    identifier: Uint8Array;
    groupId?: number;
}
export interface MlsKeyPackageStorage {
    insert(id: Uint8Array, keyPackageData: Uint8Array): Promise<boolean> | boolean;
    delete(id: Uint8Array): Promise<boolean> | boolean;
    get(id: Uint8Array): Promise<Uint8Array | null | undefined> | Uint8Array | null | undefined;
}
export interface MlsGroupStateStorage {
    state(groupId: Uint8Array): Promise<Uint8Array | null | undefined> | Uint8Array | null | undefined;
    epoch(groupId: Uint8Array, epochId: number | bigint): Promise<Uint8Array | null | undefined> | Uint8Array | null | undefined;
    write(groupId: Uint8Array, stateData: Uint8Array, epochInserts: Record<string, Uint8Array> | Map<number | bigint, Uint8Array>, epochUpdates: Record<string, Uint8Array> | Map<number | bigint, Uint8Array>): Promise<boolean> | boolean;
    maxEpochId?(groupId: Uint8Array): Promise<number | bigint | null | undefined> | number | bigint | null | undefined;
}
export interface MlsPreSharedKeyStorage {
    get(id: Uint8Array): Promise<Uint8Array | null | undefined> | Uint8Array | null | undefined;
}
export interface UserMessageStorage {
    add(id: number | bigint, other: string, message: Uint8Array, sentByOther: boolean): Promise<void> | void;
    getLastMessagesOf?(other: string, before: number | bigint, limit: number | bigint): Promise<RawUserMessage[]>;
    get?(other: string, before: number | bigint, limit: number | bigint): Promise<RawUserMessage[]>;
}
export interface GroupMessageStorage {
    add(id: number | bigint, groupId: number | bigint, channelId: number, epoch: number, by: string, message: Uint8Array): Promise<void> | void;
    get(groupId: number | bigint, startBefore: number | bigint, limit: number): Promise<RawGroupMessage[]>;
    getLastMessageOfGroup?(groupId: number | bigint): Promise<RawGroupMessage | null | undefined>;
    deleteByGroupId?(groupId: number | bigint): Promise<void> | void;
    updateCursor?(id: number | bigint, groupId: number | bigint, epoch: number): Promise<void> | void;
}
export interface GroupInfoStorage {
    getAll?(): Promise<RawGroupInfo[]>;
    get(id: number | bigint): Promise<RawGroupInfo | null | undefined>;
    set(id: number | bigint, name: string, description: string, groupStateId: Uint8Array): Promise<void> | void;
    delete(id: number | bigint): Promise<void> | void;
}
export interface KeyValueStorage {
    get(key: string): Promise<string | null | undefined>;
    set(key: string, value: string): Promise<void> | void;
    updateLastReceivedMessageId?(lastReceivedMessageId: number | bigint): Promise<void> | void;
}
export interface StorageProviders {
    mlsKeyPackageStorage?: MlsKeyPackageStorage;
    mlsGroupStateStorage?: MlsGroupStateStorage;
    mlsPreSharedKeyStorage?: MlsPreSharedKeyStorage;
    userMessageStorage?: UserMessageStorage;
    groupMessageStorage?: GroupMessageStorage;
    groupInfoStorage?: GroupInfoStorage;
    keyValueStorage?: KeyValueStorage;
}
export declare const initLogger: (filePath: string) => void;
export declare class FireflyClientNode {
    private inner;
    constructor(inner: any);
    static create(fireflyBaseUrl: string, fireflyBaseWsUrl: string, retryIntervalInMs: number, callbacksObj: any, keyStoresPathname: string, requestTimeoutInMs: number, storage?: StorageProviders): Promise<FireflyClientNode>;
    setAccessToken(token: string): void;
    checkSetup(): Promise<void>;
    initializeWithRetrying(): Promise<void>;
    isInitialized(): boolean;
    getConnectionState(): string;
    dispose(): Promise<void>;
    encryptAndSend(to: string, payload: Uint8Array | number[]): Promise<any>;
    encryptAndSendGroup(groupId: number, payload: Uint8Array | number[]): Promise<number>;
    createGroup(name: string, description?: string, settings?: number): Promise<any>;
    addGroupMember(groupId: number, username: string, roleId?: number): Promise<void>;
    kickGroupMember(groupId: number, username: string): Promise<void>;
    deleteGroup(groupId: number): Promise<void>;
    createJoinLink(groupId: number, expiresInSeconds?: number, maxUses?: number): Promise<string>;
    joinViaLink(token: string): Promise<void>;
    requestToJoin(groupId: number): Promise<void>;
    syncGroupJoinsAndReadds(groupId: number): Promise<void>;
    loadAllGroups(): Promise<void>;
    getGroupInfos(): Promise<any[]>;
    getGroupMessages(groupId: number, startBefore?: number, limit?: number): Promise<any[]>;
    getOnlineStatus(usernames: string[]): Promise<string[]>;
    readUserMessagesUpto(other: string, uptoMessageId: number): Promise<void>;
    uploadFcmToken(token?: string | null): Promise<void>;
    getConversations(token: string): Promise<any[]>;
    getGroupExtension(groupId: number): Promise<Uint8Array>;
    exportGroupMeetingKey(groupId: number): Promise<Uint8Array>;
}
