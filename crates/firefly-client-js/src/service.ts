// import { crockfordDecode, ULID } from "ulid";
import {
  GroupMessages,
  GroupMessage,
  GroupKeyPackages,
  GroupInvite,
  PreKeyBundles,
  UserMessages,
  ConversationStart,
  Conversations,
  Groups,
  SignedToken,
  GroupKeyPackage,
  Group,
  GroupId,
  PreKeyBundle,
  CreateMeetingRequest,
  CreateMeetingResponse,
  JoinMeetingRequest,
  JoinMeetingResponse,
  LeaveMeetingRequest,
  EndMeetingRequest,
  GetActiveSessionResponse,
} from "firefly-protos-js";

/**
 * Minimal ULID validator (Crockford alphabet, 26 chars)
 */
// function isValidULID(u: string): u is ULID {
//   return /^[0123456789ABCDEFGHJKMNPQRSTVWXYZ]{26}$/i.test(u);
// }

/** join array of numbers/strings into server-expected %2C encoded CSV */
function joinCsv(items: Array<string | number | bigint>) {
  return items.map(String).join("%2C");
}

export class HttpError extends Error {
  constructor(
    public statusCode: number,
    public responseText: string,
  ) {
    super(`HTTP ${statusCode}: ${responseText}`);
    this.name = "HttpError";
  }
}

export class FireflyService {
  baseUrl: string;
  getAuthToken: () => Promise<string>;

  constructor(baseUrl: string, getAuthToken: () => Promise<string>) {
    this.baseUrl = baseUrl.replace(/\/$/, "");
    this.getAuthToken = getAuthToken;
  }

  private async req(path: string, opts: RequestInit = {}) {
    const token = await this.getAuthToken();
    const url = new URL(path, this.baseUrl).toString();
    const res = await fetch(url, {
      ...opts,
      headers: {
        Authorization: `Bearer ${token}`,
        ...(opts.headers || {}),
      },
    });
    if (!res.ok) {
      const responseText = await res.text();
      throw new HttpError(res.status, responseText);
    }
    return res;
  }

  // ---------- GETs ----------
  async getJWKS() {
    const res = await fetch(new URL("/jwks.json", this.baseUrl).toString(), {
      headers: { Authorization: `Bearer ${await this.getAuthToken()}` },
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
  }

  async getGroupMessages(opts: {
    groupId?: number;
    startAfter?: bigint;
    limit?: number;
  }) {
    if (!opts.groupId) throw new Error("groupId required");
    if (opts.startAfter && opts.startAfter < 0n)
      throw new Error("invalid startAfter");

    const url = new URL("/group/messages", this.baseUrl);
    if (opts.groupId != null)
      url.searchParams.set("groupId", String(opts.groupId));
    if (opts.startAfter)
      url.searchParams.set("startAfter", opts.startAfter.toString());
    if (opts.limit != null)
      url.searchParams.set(
        "limit",
        String(Math.max(1, Math.min(1000, Math.floor(opts.limit)))),
      );

    const res = await this.req(url.pathname + url.search);
    return GroupMessages.decode(new Uint8Array(await res.arrayBuffer()));
  }

  async getKeyPackages() {
    const res = await this.req("/group/keyPackages");
    return GroupKeyPackages.decode(new Uint8Array(await res.arrayBuffer()));
  }

  async getKeyPackage(username: string) {
    if (!username) throw new Error("username required");
    const url = new URL("/group/keyPackage", this.baseUrl);
    url.searchParams.set("username", username);
    const res = await this.req(url.pathname + url.search);
    const arr = new Uint8Array(await res.arrayBuffer());
    return GroupKeyPackage.decode(arr);
  }

  async getPreKeyBundles() {
    const res = await this.req("/user/preKeyBundles");
    return PreKeyBundles.decode(new Uint8Array(await res.arrayBuffer()));
  }

  async getUserMessages(opts: {
    conversationId?: number;
    startAfter?: bigint;
    limit?: number;
  }) {
    if (!opts.conversationId && !opts.startAfter)
      throw new Error("conversationId or startAfter required");
    if (opts.startAfter && opts.startAfter < 0n)
      throw new Error("invalid startAfter");

    const url = new URL("/user/messages", this.baseUrl);
    if (opts.conversationId != null)
      url.searchParams.set("conversationId", String(opts.conversationId));
    if (opts.startAfter)
      url.searchParams.set("startAfter", opts.startAfter.toString());
    if (opts.limit != null)
      url.searchParams.set(
        "limit",
        String(Math.max(1, Math.min(1000, Math.floor(opts.limit)))),
      );

    const res = await this.req(url.pathname + url.search);
    return UserMessages.decode(new Uint8Array(await res.arrayBuffer()));
  }

  async getConversations() {
    const res = await this.req("/user/conversations");
    return Conversations.decode(new Uint8Array(await res.arrayBuffer()));
  }

  async getConversation(other: string, preKeyBundleRequired = false) {
    const url = new URL("/user/conversation", this.baseUrl);
    url.searchParams.set("other", other);
    if (preKeyBundleRequired) {
      url.searchParams.set("preKeyBundleRequired", "true");
    }
    const res = await this.req(url.pathname + url.search);
    return ConversationStart.decode(new Uint8Array(await res.arrayBuffer()));
  }

  async getPreKeyBundle(other: string) {
    const url = new URL("/user/preKeyBundle", this.baseUrl);
    url.searchParams.set("other", other);
    const res = await this.req(url.pathname + url.search);
    return PreKeyBundle.decode(new Uint8Array(await res.arrayBuffer()));
  }

  async getGroups() {
    const res = await this.req("/groups");
    return Groups.decode(new Uint8Array(await res.arrayBuffer()));
  }

  // ---------- POSTs ----------
  async sign(credential: Uint8Array): Promise<SignedToken> {
    const res = await this.req("/sign", {
      method: "POST",
      headers: { "Content-Type": "application/octet-stream" },
      body: new Uint8Array(credential),
    });
    return SignedToken.decode(new Uint8Array(await res.arrayBuffer()));
  }

  async createGroup(group: Group): Promise<bigint> {
    const res = await this.req("/group", {
      method: "POST",
      headers: {
        "Content-Type": "application/x-protobuf; proto=firefly.Group",
      },
      body: new Uint8Array(Group.encode(group).finish()),
    });
    const buf = await res.arrayBuffer();
    return GroupId.decode(new Uint8Array(buf)).id;
  }

  async uploadKeyPackages(packages: GroupKeyPackages) {
    const body = GroupKeyPackages.encode(packages).finish();
    const res = await this.req("/group/keyPackages", { method: "POST", body });
    return GroupKeyPackages.decode(new Uint8Array(await res.arrayBuffer()));
  }

  async postGroupMessage(groupId: bigint, message: Uint8Array) {
    const msg = GroupMessage.create({ groupId, message });
    const res = await this.req("/group/message", {
      method: "POST",
      body: GroupMessage.encode(msg).finish(),
    });
    return GroupMessage.decode(new Uint8Array(await res.arrayBuffer()));
  }

  async postCommit(groupId: bigint, message: Uint8Array) {
    const msg = GroupMessage.create({ groupId, message });
    const res = await this.req("/group/commit", {
      method: "POST",
      body: GroupMessage.encode(msg).finish(),
    });
    return GroupMessage.decode(new Uint8Array(await res.arrayBuffer())); // commit id bytes
  }

  async invite(
    groupId: bigint,
    invitee: string,
    welcomeMessage: Uint8Array,
    commitId: bigint,
  ) {
    if (commitId < 0n) throw new Error("invalid commitId ULID");
    const invite = GroupInvite.create({
      groupId,
      invitee,
      welcomeMessage,
      commitId,
    });
    const res = await this.req("/group/invite", {
      method: "POST",
      body: GroupInvite.encode(invite).finish(),
    });

    if (!res.ok) {
      throw new Error(`failed to send invite, [${res.status}], ${await res.text()}`,)
    }
  }

  async requestReAdd(groupId: number) {
    const url = new URL("/group/reAdd", this.baseUrl);
    url.searchParams.set("groupId", String(groupId));
    await this.req(url.pathname + url.search);
  }

  // async uploadPreKeyBundles(bundles: PreKeyBundles) {
  //   const body = PreKeyBundles.encode(bundles).finish();
  //   await this.req("/user/preKeyBundles", { method: "POST", body });
  // }

  // async postUserMessage(
  //   conversationId: bigint,
  //   text: Uint8Array,
  //   type: number,
  // ) {
  //   const msg = UserMessage.create({ text, type });
  //   const res = await this.req("/user/message", {
  //     method: "POST",
  //     body: UserMessage.encode(msg).finish(),
  //   });
  //   return UserMessage.decode(new Uint8Array(await res.arrayBuffer()));
  // }

  // async createConversation(otherUsername: string) {
  //   if (!otherUsername) throw new Error("other username required");
  //   const url = new URL("/user/conversation", this.baseUrl);
  //   url.searchParams.set("other", otherUsername);
  //   const res = await this.req(url.pathname + url.search, { method: "POST" });
  //   const buffer = new Uint8Array(await res.arrayBuffer());

  //   return ConversationStart.decode(buffer);
  // }

  // ---------- DELETEs ----------
  async deleteGroup(id: number) {
    const url = new URL("/group", this.baseUrl);
    url.searchParams.set("id", String(id));
    await this.req(url.pathname + url.search, { method: "DELETE" });
  }

  async deleteGroupMember(uname: string, groupId: number) {
    if (!uname) throw new Error("uname required");
    const url = new URL("/group/member", this.baseUrl);
    url.searchParams.set("uname", uname);
    url.searchParams.set("groupId", String(groupId));
    await this.req(url.pathname + url.search, { method: "DELETE" });
  }

  async deleteGroupInvites(commitIds: bigint[]) {
    if (!commitIds.length) return;
    for (const c of commitIds)
      if (c < 0n) throw new Error("invalid ULID in commitIds");
    const url = new URL("/group/invites", this.baseUrl);
    url.searchParams.set("commitIds", joinCsv(commitIds));
    await this.req(url.pathname + url.search, { method: "DELETE" });
  }

  async deleteKeyPackages(ids: number[]) {
    if (!ids.length) return;
    const url = new URL("/group/keyPackages", this.baseUrl);
    url.searchParams.set("ids", joinCsv(ids));
    await this.req(url.pathname + url.search, { method: "DELETE" });
  }

  async deletePreKeyBundles(ids: number[]) {
    if (!ids.length) return;
    const url = new URL("/user/preKeyBundle", this.baseUrl);
    url.searchParams.set("ids", joinCsv(ids));
    await this.req(url.pathname + url.search, { method: "DELETE" });
  }

  async deleteConversations(ids: number[]) {
    const url = new URL("/user/conversations", this.baseUrl);
    url.searchParams.set("ids", joinCsv(ids));
    await this.req(url.pathname + url.search, { method: "DELETE" });
  }

  // async syncUserMessages(since: bigint, limit: number) {
  //   const url = new URL("/user/sync", this.baseUrl);
  //   url.searchParams.set("since", since.toString());
  //   url.searchParams.set("limit", limit.toString());

  //   const response = await this.req(url.pathname + url.search);

  //   return UserMessages.decode(new Uint8Array(await response.arrayBuffer()));
  // }

  async deleteUserMessages(until: bigint) {
    const url = new URL("/user/messages", this.baseUrl);
    url.searchParams.set("until", until.toString());

    await this.req(url.pathname + url.search, { method: "DELETE" });
  }

  async recreateConversations() {
    await this.req("/user/conversations", { method: "PATCH" });
  }

  async getWebrtcConfig() {
    const response = await this.req("/webrtcConfig");
    const body = await response.text();

    return JSON.parse(body) as RTCConfiguration;
  }

  async deleteUserMessage(convoId: bigint, msgId: bigint) {
    return await this.req(
      `/user/message?conversationId=${convoId}&messageId=${msgId}`,
      { method: "DELETE" },
    );
  }

  async createMeeting(request: CreateMeetingRequest): Promise<CreateMeetingResponse> {
    const res = await this.req("/meeting/create", {
      method: "POST",
      headers: {
        "Content-Type": "application/x-protobuf; proto=firefly.CreateMeetingRequest",
      },
      body: new Uint8Array(CreateMeetingRequest.encode(request).finish()),
    });
    return CreateMeetingResponse.decode(new Uint8Array(await res.arrayBuffer()));
  }

  async joinMeeting(request: JoinMeetingRequest): Promise<JoinMeetingResponse> {
    const res = await this.req("/meeting/join", {
      method: "POST",
      headers: {
        "Content-Type": "application/x-protobuf; proto=firefly.JoinMeetingRequest",
      },
      body: new Uint8Array(JoinMeetingRequest.encode(request).finish()),
    });
    return JoinMeetingResponse.decode(new Uint8Array(await res.arrayBuffer()));
  }

  async leaveMeeting(request: LeaveMeetingRequest): Promise<void> {
    await this.req("/meeting/leave", {
      method: "POST",
      headers: {
        "Content-Type": "application/x-protobuf; proto=firefly.LeaveMeetingRequest",
      },
      body: new Uint8Array(LeaveMeetingRequest.encode(request).finish()),
    });
  }

  async endMeeting(request: EndMeetingRequest): Promise<void> {
    await this.req("/meeting/end", {
      method: "POST",
      headers: {
        "Content-Type": "application/x-protobuf; proto=firefly.EndMeetingRequest",
      },
      body: new Uint8Array(EndMeetingRequest.encode(request).finish()),
    });
  }

  async getActiveSession(groupId: number, channelId: number): Promise<GetActiveSessionResponse> {
    const gId = Number(groupId);
    const cId = Number(channelId);
    if (Number.isNaN(gId) || Number.isNaN(cId) || gId <= 0 || cId <= 0) {
      return { session: undefined };
    }
    const url = new URL("/meeting/active", this.baseUrl);
    url.searchParams.set("groupId", String(gId));
    url.searchParams.set("channelId", String(cId));
    const res = await this.req(url.pathname + url.search);
    return GetActiveSessionResponse.decode(new Uint8Array(await res.arrayBuffer()));
  }
}
