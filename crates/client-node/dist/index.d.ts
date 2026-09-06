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
    max_epoch_id?(groupId: Uint8Array): Promise<number | bigint | null | undefined> | number | bigint | null | undefined;
}
export interface MlsPreSharedKeyStorage {
    get(id: Uint8Array): Promise<Uint8Array | null | undefined> | Uint8Array | null | undefined;
}
export interface UserMessageStorage {
    add(id: number | bigint, other: string, message: Uint8Array, sentByOther: boolean): Promise<void> | void;
    getLastMessagesOf?(other: string, before: number | bigint, limit: number | bigint): Promise<RawUserMessage[]>;
    get_last_messages_of?(other: string, before: number | bigint, limit: number | bigint): Promise<RawUserMessage[]>;
    get?(other: string, before: number | bigint, limit: number | bigint): Promise<RawUserMessage[]>;
}
export interface GroupMessageStorage {
    add(id: number | bigint, groupId: number | bigint, channelId: number, epoch: number, by: string, message: Uint8Array): Promise<void> | void;
    get(groupId: number | bigint, startBefore: number | bigint, limit: number): Promise<RawGroupMessage[]>;
    getLastMessageOfGroup?(groupId: number | bigint): Promise<RawGroupMessage | null | undefined>;
    get_last_message_of_group?(groupId: number | bigint): Promise<RawGroupMessage | null | undefined>;
    deleteByGroupId?(groupId: number | bigint): Promise<void> | void;
    delete_by_group_id?(groupId: number | bigint): Promise<void> | void;
    updateCursor?(id: number | bigint, groupId: number | bigint, epoch: number): Promise<void> | void;
    update_cursor?(id: number | bigint, groupId: number | bigint, epoch: number): Promise<void> | void;
}
export interface GroupInfoStorage {
    getAll?(): Promise<RawGroupInfo[]>;
    get_all?(): Promise<RawGroupInfo[]>;
    get(id: number | bigint): Promise<RawGroupInfo | null | undefined>;
    set(id: number | bigint, name: string, description: string, groupStateId: Uint8Array): Promise<void> | void;
    delete(id: number | bigint): Promise<void> | void;
}
export interface KeyValueStorage {
    get(key: string): Promise<string | null | undefined>;
    set(key: string, value: string): Promise<void> | void;
    updateLastReceivedMessageId?(lastReceivedMessageId: number | bigint): Promise<void> | void;
    update_last_received_message_id?(lastReceivedMessageId: number | bigint): Promise<void> | void;
}
export interface StorageProviders {
    mlsKeyPackageStorage?: MlsKeyPackageStorage;
    mlsGroupStateStorage?: MlsGroupStateStorage;
    mlsPreSharedKeyStorage?: MlsPreSharedKeyStorage;
    userMessageStorage?: UserMessageStorage;
    groupMessageStorage?: GroupMessageStorage;
    groupInfoStorage?: GroupInfoStorage;
    keyValueStorage?: KeyValueStorage;
    keyPackageStorage?: MlsKeyPackageStorage;
    groupStateStorage?: MlsGroupStateStorage;
    preSharedKeyStorage?: MlsPreSharedKeyStorage;
    userMessages?: UserMessageStorage;
    groupMessages?: GroupMessageStorage;
    groupInfo?: GroupInfoStorage;
    keyValue?: KeyValueStorage;
    mls?: any;
}
export type FireflyStorageAdapter = StorageProviders;
export type UserMessageStorageAdapter = UserMessageStorage;
export type GroupMessageStorageAdapter = GroupMessageStorage;
export type GroupInfoStorageAdapter = GroupInfoStorage;
export type KeyValueStorageAdapter = KeyValueStorage;
export type MlsStorageAdapter = StorageProviders;
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
