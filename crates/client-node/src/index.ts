// @ts-ignore
import * as wasmPkg from '../wasm/firefly_client_node.js';
export * as protos from './protos/message';

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
    requestTimeoutInMs: number
  ): Promise<FireflyClientNode> {
    const raw = await wasmPkg.FireflyClientNode.create(
      fireflyBaseUrl,
      fireflyBaseWsUrl,
      retryIntervalInMs,
      callbacksObj,
      keyStoresPathname,
      requestTimeoutInMs
    );
    return new FireflyClientNode(raw);
  }

  setAccessToken(token: string): void {
    this.inner.set_access_token(token);
  }
  set_access_token(token: string): void {
    this.inner.set_access_token(token);
  }

  async checkSetup(): Promise<void> {
    return await this.inner.check_setup();
  }
  async check_setup(): Promise<void> {
    return await this.inner.check_setup();
  }

  async initializeWithRetrying(): Promise<void> {
    return await this.inner.initialize_with_retrying();
  }
  async initialize_with_retrying(): Promise<void> {
    return await this.inner.initialize_with_retrying();
  }

  isInitialized(): boolean {
    return this.inner.is_initialized();
  }
  is_initialized(): boolean {
    return this.inner.is_initialized();
  }

  getConnectionState(): string {
    return this.inner.get_connection_state();
  }
  get_connection_state(): string {
    return this.inner.get_connection_state();
  }

  async dispose(): Promise<void> {
    return await this.inner.dispose();
  }

  async encryptAndSend(to: string, payload: Uint8Array | number[]): Promise<any> {
    const arr = payload instanceof Uint8Array ? payload : new Uint8Array(payload);
    return await this.inner.encrypt_and_send(to, arr);
  }
  async encrypt_and_send(to: string, payload: Uint8Array | number[]): Promise<any> {
    return this.encryptAndSend(to, payload);
  }

  async encryptAndSendGroup(groupId: number, payload: Uint8Array | number[]): Promise<number> {
    const arr = payload instanceof Uint8Array ? payload : new Uint8Array(payload);
    return await this.inner.encrypt_and_send_group(groupId, arr);
  }
  async encrypt_and_send_group(groupId: number, payload: Uint8Array | number[]): Promise<number> {
    return this.encryptAndSendGroup(groupId, payload);
  }

  async createGroup(name: string, description: string = '', settings?: number): Promise<any> {
    return await this.inner.create_group(name, description, settings ?? null);
  }
  async create_group(name: string, description: string = '', settings?: number): Promise<any> {
    return this.createGroup(name, description, settings);
  }

  async addGroupMember(groupId: number, username: string, roleId: number = 1): Promise<void> {
    return await this.inner.add_group_member(groupId, username, roleId);
  }
  async add_group_member(groupId: number, username: string, roleId: number = 1): Promise<void> {
    return this.addGroupMember(groupId, username, roleId);
  }

  async kickGroupMember(groupId: number, username: string): Promise<void> {
    return await this.inner.kick_group_member(groupId, username);
  }
  async kick_group_member(groupId: number, username: string): Promise<void> {
    return this.kickGroupMember(groupId, username);
  }

  async deleteGroup(groupId: number): Promise<void> {
    return await this.inner.delete_group(groupId);
  }
  async delete_group(groupId: number): Promise<void> {
    return this.deleteGroup(groupId);
  }

  async createJoinLink(groupId: number, expiresInSeconds: number = 86400, maxUses: number = 100): Promise<string> {
    return await this.inner.create_join_link(groupId, expiresInSeconds, maxUses);
  }
  async create_join_link(groupId: number, expiresInSeconds: number = 86400, maxUses: number = 100): Promise<string> {
    return this.createJoinLink(groupId, expiresInSeconds, maxUses);
  }

  async joinViaLink(token: string): Promise<void> {
    return await this.inner.join_via_link(token);
  }
  async join_via_link(token: string): Promise<void> {
    return this.joinViaLink(token);
  }

  async requestToJoin(groupId: number): Promise<void> {
    return await this.inner.request_to_join(groupId);
  }
  async request_to_join(groupId: number): Promise<void> {
    return this.requestToJoin(groupId);
  }

  async syncGroupJoinsAndReadds(groupId: number): Promise<void> {
    return await this.inner.sync_group_joins_and_readds(groupId);
  }
  async sync_group_joins_and_readds(groupId: number): Promise<void> {
    return this.syncGroupJoinsAndReadds(groupId);
  }

  async loadAllGroups(): Promise<void> {
    return await this.inner.load_all_groups();
  }
  async load_all_groups(): Promise<void> {
    return this.loadAllGroups();
  }

  async getGroupInfos(): Promise<any[]> {
    return await this.inner.get_group_infos();
  }
  async get_group_infos(): Promise<any[]> {
    return this.getGroupInfos();
  }

  async getGroupMessages(groupId: number, startBefore: number = 0, limit: number = 50): Promise<any[]> {
    return await this.inner.get_group_messages(groupId, startBefore, limit);
  }
  async get_group_messages(groupId: number, startBefore: number = 0, limit: number = 50): Promise<any[]> {
    return this.getGroupMessages(groupId, startBefore, limit);
  }

  async getOnlineStatus(usernames: string[]): Promise<string[]> {
    return await this.inner.get_online_status(usernames);
  }
  async get_online_status(usernames: string[]): Promise<string[]> {
    return this.getOnlineStatus(usernames);
  }

  async readUserMessagesUpto(other: string, uptoMessageId: number): Promise<void> {
    return await this.inner.read_user_messages_upto(other, uptoMessageId);
  }
  async read_user_messages_upto(other: string, uptoMessageId: number): Promise<void> {
    return this.readUserMessagesUpto(other, uptoMessageId);
  }

  async uploadFcmToken(token?: string | null): Promise<void> {
    return await this.inner.upload_fcm_token(token ?? null);
  }
  async upload_fcm_token(token?: string | null): Promise<void> {
    return this.uploadFcmToken(token);
  }

  async getConversations(token: string): Promise<any[]> {
    return await this.inner.get_conversations(token);
  }
  async get_conversations(token: string): Promise<any[]> {
    return this.getConversations(token);
  }

  async getGroupExtension(groupId: number): Promise<Uint8Array> {
    return await this.inner.get_group_extension(groupId);
  }
  async get_group_extension(groupId: number): Promise<Uint8Array> {
    return this.getGroupExtension(groupId);
  }

  async exportGroupMeetingKey(groupId: number): Promise<Uint8Array> {
    return await this.inner.export_group_meeting_key(groupId);
  }
  async export_group_meeting_key(groupId: number): Promise<Uint8Array> {
    return this.exportGroupMeetingKey(groupId);
  }
}
