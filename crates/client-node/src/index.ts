// @ts-ignore
import * as wasmPkg from '../wasm/firefly_client_node.js';
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

// ---------------------------------------------------------------------------
// Core MLS Storage Providers (1-to-1 with firefly_core::storage_provider)
// ---------------------------------------------------------------------------

export interface MlsKeyPackageStorage {
  insert(id: Uint8Array, keyPackageData: Uint8Array): Promise<boolean> | boolean;
  delete(id: Uint8Array): Promise<boolean> | boolean;
  get(id: Uint8Array): Promise<Uint8Array | null | undefined> | Uint8Array | null | undefined;
}

export interface MlsGroupStateStorage {
  state(groupId: Uint8Array): Promise<Uint8Array | null | undefined> | Uint8Array | null | undefined;
  epoch(groupId: Uint8Array, epochId: number | bigint): Promise<Uint8Array | null | undefined> | Uint8Array | null | undefined;
  write(
    groupId: Uint8Array,
    stateData: Uint8Array,
    epochInserts: Record<string, Uint8Array> | Map<number | bigint, Uint8Array>,
    epochUpdates: Record<string, Uint8Array> | Map<number | bigint, Uint8Array>
  ): Promise<boolean> | boolean;
  maxEpochId?(groupId: Uint8Array): Promise<number | bigint | null | undefined> | number | bigint | null | undefined;
}

export interface MlsPreSharedKeyStorage {
  get(id: Uint8Array): Promise<Uint8Array | null | undefined> | Uint8Array | null | undefined;
}

// ---------------------------------------------------------------------------
// Application Storage Providers (1-to-1 with firefly_client::storage)
// ---------------------------------------------------------------------------

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

export const initLogger = (filePath: string) => {
  if (typeof (wasmPkg as any).init_logger === 'function') {
    (wasmPkg as any).init_logger(filePath);
  }
};

export class FireflyClientNode {
  private inner: any;

  constructor(inner: any) {
    this.inner = inner;
  }

  static async create(
    fireflyBaseUrl: string,
    fireflyBaseWsUrl: string,
    retryIntervalInMs: number,
    callbacksObj: any,
    keyStoresPathname: string,
    requestTimeoutInMs: number,
    storage?: StorageProviders
  ): Promise<FireflyClientNode> {
    const cb = { ...callbacksObj };
    if (storage) {
      cb.storageProviders = storage;
    }
    const raw = await wasmPkg.FireflyClientNode.create(
      fireflyBaseUrl,
      fireflyBaseWsUrl,
      retryIntervalInMs,
      cb,
      keyStoresPathname,
      requestTimeoutInMs
    );
    return new FireflyClientNode(raw);
  }

  setAccessToken(token: string): void {
    this.inner.setAccessToken(token);
  }

  async checkSetup(): Promise<void> {
    return await this.inner.checkSetup();
  }

  async initializeWithRetrying(): Promise<void> {
    return await this.inner.initializeWithRetrying();
  }

  isInitialized(): boolean {
    return this.inner.isInitialized();
  }

  getConnectionState(): string {
    return this.inner.getConnectionState();
  }

  async dispose(): Promise<void> {
    return await this.inner.dispose();
  }

  async encryptAndSend(to: string, payload: Uint8Array | number[]): Promise<any> {
    const arr = payload instanceof Uint8Array ? payload : new Uint8Array(payload);
    return await this.inner.encryptAndSend(to, arr);
  }

  async encryptAndSendGroup(groupId: number, payload: Uint8Array | number[]): Promise<number> {
    const arr = payload instanceof Uint8Array ? payload : new Uint8Array(payload);
    return await this.inner.encryptAndSendGroup(groupId, arr);
  }

  async createGroup(name: string, description: string = '', settings?: number): Promise<any> {
    return await this.inner.createGroup(name, description, settings ?? null);
  }

  async addGroupMember(groupId: number, username: string, roleId: number = 1): Promise<void> {
    return await this.inner.addGroupMember(groupId, username, roleId);
  }

  async kickGroupMember(groupId: number, username: string): Promise<void> {
    return await this.inner.kickGroupMember(groupId, username);
  }

  async deleteGroup(groupId: number): Promise<void> {
    return await this.inner.deleteGroup(groupId);
  }

  async createJoinLink(groupId: number, expiresInSeconds: number = 86400, maxUses: number = 100): Promise<string> {
    return await this.inner.createJoinLink(groupId, expiresInSeconds, maxUses);
  }

  async joinViaLink(token: string): Promise<void> {
    return await this.inner.joinViaLink(token);
  }

  async requestToJoin(groupId: number): Promise<void> {
    return await this.inner.requestToJoin(groupId);
  }

  async syncGroupJoinsAndReadds(groupId: number): Promise<void> {
    return await this.inner.syncGroupJoinsAndReadds(groupId);
  }

  async loadAllGroups(): Promise<void> {
    return await this.inner.loadAllGroups();
  }

  async getGroupInfos(): Promise<any[]> {
    return await this.inner.getGroupInfos();
  }

  async getGroupMessages(groupId: number, startBefore: number = 0, limit: number = 50): Promise<any[]> {
    return await this.inner.getGroupMessages(groupId, startBefore, limit);
  }

  async getOnlineStatus(usernames: string[]): Promise<string[]> {
    return await this.inner.getOnlineStatus(usernames);
  }

  async readUserMessagesUpto(other: string, uptoMessageId: number): Promise<void> {
    return await this.inner.readUserMessagesUpto(other, uptoMessageId);
  }

  async uploadFcmToken(token?: string | null): Promise<void> {
    return await this.inner.uploadFcmToken(token ?? null);
  }

  async getConversations(token: string): Promise<any[]> {
    return await this.inner.getConversations(token);
  }

  async getGroupExtension(groupId: number): Promise<Uint8Array> {
    return await this.inner.getGroupExtension(groupId);
  }

  async exportGroupMeetingKey(groupId: number): Promise<Uint8Array> {
    return await this.inner.exportGroupMeetingKey(groupId);
  }
}
