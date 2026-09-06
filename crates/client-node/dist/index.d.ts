export * as protos from './protos/message';
export interface RawUserMessage {
    id: number;
    other: string;
    message: Uint8Array;
    sentByOther: boolean;
}
export interface RawGroupMessage {
    id: number;
    groupId: number;
    by: string;
    message: Uint8Array;
    channelId: number;
    epoch: number;
}
export interface RawGroupInfo {
    id: number;
    name: string;
    description: string;
    identifier: Uint8Array;
}
export interface UserMessageStorageAdapter {
    add?: (id: number, other: string, message: Uint8Array, sentByOther: boolean) => Promise<void> | void;
    get?: (other: string, startBefore: number, limit: number) => Promise<RawUserMessage[]>;
}
export interface GroupMessageStorageAdapter {
    add?: (id: number, groupId: number, channelId: number, epoch: number, by: string, message: Uint8Array) => Promise<void> | void;
    get?: (groupId: number, startBefore: number, limit: number) => Promise<RawGroupMessage[]>;
    getLastMessageOfGroup?: (groupId: number) => Promise<RawGroupMessage | null>;
    deleteByGroupId?: (groupId: number) => Promise<void> | void;
    updateCursor?: (id: number, groupId: number, epoch: number) => Promise<void> | void;
}
export interface GroupInfoStorageAdapter {
    getAll?: () => Promise<RawGroupInfo[]>;
    get?: (id: number) => Promise<RawGroupInfo | null>;
    set?: (id: number, name: string, description: string, identifier: Uint8Array) => Promise<void> | void;
    delete?: (id: number) => Promise<void> | void;
}
export interface KeyValueStorageAdapter {
    get?: (key: string) => Promise<string | null>;
    set?: (key: string, value: string) => Promise<void> | void;
}
export interface MlsStorageAdapter {
    keyPackageInsert?: (id: Uint8Array, data: Uint8Array) => Promise<boolean> | boolean;
    keyPackageDelete?: (id: Uint8Array) => Promise<boolean> | boolean;
    keyPackageGet?: (id: Uint8Array) => Promise<Uint8Array | null>;
    groupState?: (groupId: Uint8Array) => Promise<Uint8Array | null>;
    groupEpoch?: (groupId: Uint8Array, epochId: number) => Promise<Uint8Array | null>;
    groupWrite?: (groupId: Uint8Array, stateData: Uint8Array, epochInserts: Record<string, Uint8Array>, epochUpdates: Record<string, Uint8Array>) => Promise<boolean> | boolean;
    groupMaxEpochId?: (groupId: Uint8Array) => Promise<number | null>;
    pskGet?: (id: Uint8Array) => Promise<Uint8Array | null>;
}
export interface FireflyStorageAdapter {
    userMessages?: UserMessageStorageAdapter;
    groupMessages?: GroupMessageStorageAdapter;
    groupInfo?: GroupInfoStorageAdapter;
    keyValue?: KeyValueStorageAdapter;
    mls?: MlsStorageAdapter;
}
export declare const initLogger: (filePath: string) => void;
export declare class FireflyClientNode {
    private inner;
    constructor(inner: any);
    static create(fireflyBaseUrl: string, fireflyBaseWsUrl: string, retryIntervalInMs: number, callbacksObj: any, keyStoresPathname: string, requestTimeoutInMs: number, storage?: FireflyStorageAdapter): Promise<FireflyClientNode>;
    setAccessToken(token: string): void;
    set_access_token(token: string): void;
    checkSetup(): Promise<void>;
    check_setup(): Promise<void>;
    initializeWithRetrying(): Promise<void>;
    initialize_with_retrying(): Promise<void>;
    isInitialized(): boolean;
    is_initialized(): boolean;
    getConnectionState(): string;
    get_connection_state(): string;
    dispose(): Promise<void>;
    encryptAndSend(to: string, payload: Uint8Array | number[]): Promise<any>;
    encrypt_and_send(to: string, payload: Uint8Array | number[]): Promise<any>;
    encryptAndSendGroup(groupId: number, payload: Uint8Array | number[]): Promise<number>;
    encrypt_and_send_group(groupId: number, payload: Uint8Array | number[]): Promise<number>;
    createGroup(name: string, description?: string, settings?: number): Promise<any>;
    create_group(name: string, description?: string, settings?: number): Promise<any>;
    addGroupMember(groupId: number, username: string, roleId?: number): Promise<void>;
    add_group_member(groupId: number, username: string, roleId?: number): Promise<void>;
    kickGroupMember(groupId: number, username: string): Promise<void>;
    kick_group_member(groupId: number, username: string): Promise<void>;
    deleteGroup(groupId: number): Promise<void>;
    delete_group(groupId: number): Promise<void>;
    createJoinLink(groupId: number, expiresInSeconds?: number, maxUses?: number): Promise<string>;
    create_join_link(groupId: number, expiresInSeconds?: number, maxUses?: number): Promise<string>;
    joinViaLink(token: string): Promise<void>;
    join_via_link(token: string): Promise<void>;
    requestToJoin(groupId: number): Promise<void>;
    request_to_join(groupId: number): Promise<void>;
    syncGroupJoinsAndReadds(groupId: number): Promise<void>;
    sync_group_joins_and_readds(groupId: number): Promise<void>;
    loadAllGroups(): Promise<void>;
    load_all_groups(): Promise<void>;
    getGroupInfos(): Promise<any[]>;
    get_group_infos(): Promise<any[]>;
    getGroupMessages(groupId: number, startBefore?: number, limit?: number): Promise<any[]>;
    get_group_messages(groupId: number, startBefore?: number, limit?: number): Promise<any[]>;
    getOnlineStatus(usernames: string[]): Promise<string[]>;
    get_online_status(usernames: string[]): Promise<string[]>;
    readUserMessagesUpto(other: string, uptoMessageId: number): Promise<void>;
    read_user_messages_upto(other: string, uptoMessageId: number): Promise<void>;
    uploadFcmToken(token?: string | null): Promise<void>;
    upload_fcm_token(token?: string | null): Promise<void>;
    getConversations(token: string): Promise<any[]>;
    get_conversations(token: string): Promise<any[]>;
    getGroupExtension(groupId: number): Promise<Uint8Array>;
    get_group_extension(groupId: number): Promise<Uint8Array>;
    exportGroupMeetingKey(groupId: number): Promise<Uint8Array>;
    export_group_meeting_key(groupId: number): Promise<Uint8Array>;
}
