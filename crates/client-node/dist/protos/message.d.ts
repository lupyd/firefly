import { BinaryReader, BinaryWriter } from "@bufbuild/protobuf/wire";
export declare const protobufPackage = "firefly";
export declare enum CallSignalType {
    /** CALL_REQUEST - Call invitation (contains SDP Offer) */
    CALL_REQUEST = 0,
    /** CALL_ANSWER - Call accepted (contains SDP Answer) */
    CALL_ANSWER = 1,
    /** CALL_REJECT - Call declined by receiver */
    CALL_REJECT = 2,
    /** CALL_CANCEL - Call canceled by caller before answer */
    CALL_CANCEL = 3,
    /** CALL_HANGUP - Call ended by either party during or after session */
    CALL_HANGUP = 4,
    /** CALL_DISMISS - Server-sent notification to dismiss call on other devices */
    CALL_DISMISS = 5,
    /** CALL_ICECANDIDATE - Dynamic ICE candidates signaling */
    CALL_ICECANDIDATE = 6,
    UNRECOGNIZED = -1
}
export declare function callSignalTypeFromJSON(object: any): CallSignalType;
export declare function callSignalTypeToJSON(object: CallSignalType): string;
export declare enum CallMessageType {
    none = 0,
    request = 1,
    reject = 2,
    end = 3,
    /** ended - for saving call messages */
    ended = 4,
    rejected = 5,
    /** candidate - webrtc messages */
    candidate = 10,
    answer = 11,
    offer = 12,
    UNRECOGNIZED = -1
}
export declare function callMessageTypeFromJSON(object: any): CallMessageType;
export declare function callMessageTypeToJSON(object: CallMessageType): string;
export declare enum MeetingSessionStatus {
    MEETING_STATUS_ACTIVE = 0,
    MEETING_STATUS_ENDED = 1,
    UNRECOGNIZED = -1
}
export declare function meetingSessionStatusFromJSON(object: any): MeetingSessionStatus;
export declare function meetingSessionStatusToJSON(object: MeetingSessionStatus): string;
export declare enum MeetingSignalType {
    /** MEETING_SIGNAL_STARTED - New meeting created in a channel */
    MEETING_SIGNAL_STARTED = 0,
    /** MEETING_SIGNAL_JOINED - A user joined */
    MEETING_SIGNAL_JOINED = 1,
    /** MEETING_SIGNAL_LEFT - A user left */
    MEETING_SIGNAL_LEFT = 2,
    /** MEETING_SIGNAL_ENDED - Meeting ended (by creator or last participant) */
    MEETING_SIGNAL_ENDED = 3,
    UNRECOGNIZED = -1
}
export declare function meetingSignalTypeFromJSON(object: any): MeetingSignalType;
export declare function meetingSignalTypeToJSON(object: MeetingSignalType): string;
export interface UserMessage {
    id: bigint;
    toId: bigint;
    fromId: bigint;
    text: Buffer;
    type: number;
    /** flags for server to notify or just send or don't send */
    settings: number;
    hashValue: bigint;
    /** optional sends these for decryption purposes */
    fromUsername: string;
    fromDeviceId: number;
}
export interface Group {
    id: bigint;
    name: string;
    description: string;
    state: Buffer;
    settings: number;
    upgraded: boolean;
    pending: boolean;
    owner: string;
}
export interface Groups {
    groups: Group[];
}
export interface UserMessages {
    messages: UserMessage[];
}
export interface GroupInvite {
    groupId: bigint;
    inviter: string;
    invitee: string;
    welcomeMessage: Buffer;
    commitId: bigint;
}
export interface GroupCommitAndWelcome {
    id: bigint;
    groupId: bigint;
    commitMessage: Buffer;
    inviter: string;
    invitee: string;
    welcomeMessages: Buffer[];
    inviteeAddresses: bigint[];
}
export interface GroupInvites {
    invites: GroupInvite[];
}
export interface GroupMessage {
    id: bigint;
    groupId: bigint;
    message: Buffer;
    epoch: number;
}
export interface GroupKeyPackage {
    address: bigint;
    package: Buffer;
    username: string;
    id: number;
}
export interface GroupKeyPackages {
    packages: GroupKeyPackage[];
}
export interface GroupMessages {
    messages: GroupMessage[];
}
export interface GroupSyncRequest {
    groupId: bigint;
    startAfter: bigint;
    until: bigint;
    limit: number;
}
export interface GroupSyncRequests {
    requests: GroupSyncRequest[];
}
export interface GroupMemberUpdate {
    groupId: bigint;
    lastMessageSeen: bigint;
    lastEpoch: number;
}
export interface GroupMemberUpdates {
    updates: GroupMemberUpdate[];
}
export interface GroupCommit {
    id: bigint;
    groupId: bigint;
    commit: Buffer;
    epoch: number;
}
export interface GroupCommits {
    commits: GroupCommit[];
}
export interface GroupCommitSyncRequest {
    groupId: bigint;
    epoch: number;
}
export interface GroupReAddRequest {
    groupId: bigint;
    addressId: bigint;
    username: string;
}
export interface GroupReAddRequests {
    requests: GroupReAddRequest[];
}
export interface Error {
    error: string;
    errorCode: number;
}
export interface Result {
    body: Buffer;
    resultCode: number;
}
export interface Address {
    id: bigint;
    username: string;
    fcmToken: string;
    deviceId: number;
}
export interface Addresses {
    addresses: Address[];
}
export interface UploadUserMessage {
    messages: UserMessage[];
}
export interface MessageIdAndTo {
    id: bigint;
    to: bigint;
    isSelf: boolean;
}
export interface UserMessageUploaded {
    messageIds: MessageIdAndTo[];
}
export interface UserOnlineStatusRequest {
    /** max 32 usernames */
    usernames: string[];
}
export interface UserOnlineStatusResponse {
    /** bit i is 1 if usernames[i] is online */
    onlineBits: number;
}
export interface Request {
    id: number;
    createUserMessage?: UserMessage | undefined;
    uploadUserMessage?: UploadUserMessage | undefined;
    uploadGroupMessage?: GroupMessage | undefined;
    requestGroupReAdds?: RequestGroupReAdds | undefined;
    requestGroupSync?: RequestGroupSync | undefined;
    userOnlineStatus?: UserOnlineStatusRequest | undefined;
    createJoinLink?: CreateJoinLinkRequest | undefined;
    joinViaLink?: JoinViaLinkRequest | undefined;
    createMeeting?: CreateMeetingRequest | undefined;
    joinMeeting?: JoinMeetingRequest | undefined;
    leaveMeeting?: LeaveMeetingRequest | undefined;
    endMeeting?: EndMeetingRequest | undefined;
    getActiveSession?: GetActiveSessionRequest | undefined;
}
export interface Response {
    id: number;
    error: Error | undefined;
    createdUserMessage?: UserMessage | undefined;
    userMessageUploaded?: UserMessageUploaded | undefined;
    groupMessageUploaded?: GroupMessage | undefined;
    groupReAddRequestSuccess?: GroupReAddRequestSuccess | undefined;
    userOnlineStatus?: UserOnlineStatusResponse | undefined;
    createJoinLink?: CreateJoinLinkResponse | undefined;
    joinViaLinkSuccess?: JoinViaLinkSuccess | undefined;
    createMeetingResponse?: CreateMeetingResponse | undefined;
    joinMeetingResponse?: JoinMeetingResponse | undefined;
    getActiveSessionResponse?: GetActiveSessionResponse | undefined;
}
export interface ServerMessage {
    userMessage?: UserMessage | undefined;
    groupMessage?: GroupMessage | undefined;
    userMessages?: UserMessages | undefined;
    groupMessages?: GroupMessages | undefined;
    response?: Response | undefined;
    ping?: Buffer | undefined;
    pong?: Buffer | undefined;
    groupInvite?: GroupInvite | undefined;
    groupCommits?: GroupCommits | undefined;
    groupReAddRequests?: GroupReAddRequests | undefined;
    groupJoinRequests?: GroupJoinRequests | undefined;
    callSignal?: CallSignal | undefined;
    groupMeetingSignal?: GroupMeetingSignal | undefined;
}
export interface ClientMessage {
    userMessage?: UserMessage | undefined;
    groupMessage?: GroupMessage | undefined;
    verifiedUserMessage?: UserMessage | undefined;
    request?: Request | undefined;
    ping?: Buffer | undefined;
    pong?: Buffer | undefined;
    callSignal?: CallSignal | undefined;
    groupMeetingSignal?: GroupMeetingSignal | undefined;
}
export interface CallSignal {
    callId: bigint;
    senderUsername: string;
    receiverUsername: string;
    type: CallSignalType;
    sdp: string;
    candidate: string;
    sdpMLineIndex: number;
    sdpMid: string;
    senderDeviceId: number;
}
export interface GroupId {
    id: bigint;
}
export interface AuthToken {
    username: string;
    validUntil: bigint;
    credential: Buffer;
    addressId: bigint;
    deviceId: number;
}
export interface SignedToken {
    kid: string;
    payload: Buffer;
    signature: Buffer;
}
export interface FireflyIdentity {
    secret: Buffer;
    public: Buffer;
    credential: Buffer;
}
export interface FireflyGroupExtension {
    name: string;
    roles: FireflyGroupRole[];
    channels: FireflyGroupChannel[];
    members: FireflyGroupMember[];
    defaultPermissions: number;
}
export interface FireflyGroupRole {
    id: number;
    name: string;
    permissions: number;
}
export interface FireflyGroupMember {
    username: string;
    role: number;
}
export interface FireflyGroupChannel {
    id: number;
    name: string;
    type: number;
    roles: FireflyGroupRole[];
    defaultPermissions: number;
}
export interface PreKeyBundle {
    registrationId: number;
    deviceId: number;
    preKeyId: number;
    prePublicKey: Buffer;
    signedPreKeyId: number;
    signedPrePublicKey: Buffer;
    signedPreKeySignature: Buffer;
    identityPublicKey: Buffer;
    KEMPreKeyId: number;
    KEMPrePublicKey: Buffer;
    KEMPreKeySignature: Buffer;
}
export interface PreKeyBundleEntry {
    id: number;
    address: bigint;
    bundle: PreKeyBundle | undefined;
    username: string;
    deviceId: number;
}
export interface PreKeyBundleEntries {
    entries: PreKeyBundleEntry[];
}
export interface ConversationStart {
    conversationId: bigint;
    startedBy: string;
    other: string;
    bundle: PreKeyBundle | undefined;
}
export interface PreKeyBundles {
    bundles: PreKeyBundle[];
}
export interface Conversation {
    user1: string;
    user2: string;
    settings: bigint;
}
export interface Conversations {
    conversations: Conversation[];
}
export interface EncryptedFile {
    url: string;
    secretKey: Buffer;
    contentType: number;
    contentLength: number;
}
export interface EncryptedFiles {
    files: EncryptedFile[];
}
export interface MessagePayload {
    text: string;
    replyingTo: bigint;
    files: EncryptedFiles | undefined;
}
export interface CallMessage {
    message: Buffer;
    type: CallMessageType;
    jsonBody: string;
    sessionId: number;
}
export interface SelfUserMessage {
    to: string;
    /** UserMessageInner encrypted */
    inner: Buffer;
}
export interface UserMessageInner {
    plainText?: Buffer | undefined;
    callMessage?: CallMessage | undefined;
    messagePayload?: MessagePayload | undefined;
    selfMessage?: SelfUserMessage | undefined;
    nonce: number;
}
export interface GroupMessageInner {
    channelId: number;
    messagePayload?: MessagePayload | undefined;
}
export interface RequestGroupReAdds {
    groupIds: bigint[];
}
export interface RequestGroupSync {
    groupId: bigint;
    epoch: number;
}
export interface GroupReAddRequestSuccess {
}
export interface CreateJoinLinkRequest {
    groupId: bigint;
    expiresInSeconds: bigint;
    maxUses: number;
}
export interface CreateJoinLinkResponse {
    token: string;
}
export interface JoinViaLinkRequest {
    token: string;
}
export interface JoinViaLinkSuccess {
}
export interface GroupJoinRequest {
    groupId: bigint;
    addressId: bigint;
    username: string;
}
export interface GroupJoinRequests {
    requests: GroupJoinRequest[];
}
export interface GroupMeetingSession {
    /** UUIDv7 timestamp-based ID */
    sessionId: bigint;
    groupId: bigint;
    /** Which voice/text channel this meeting is in */
    channelId: number;
    creatorUsername: string;
    status: MeetingSessionStatus;
    /** Microsecond timestamp */
    createdAt: bigint;
    endedAt: bigint;
    /** Current participant usernames */
    participants: string[];
    /** Cloudflare RealtimeKit meeting ID */
    cfMeetingId: string;
    e2eeEnabled: boolean;
}
export interface CreateMeetingRequest {
    groupId: bigint;
    channelId: number;
    e2eeEnabled: boolean;
}
export interface CreateMeetingResponse {
    session: GroupMeetingSession | undefined;
    /** CF RealtimeKit auth token for creator */
    participantToken: string;
}
export interface JoinMeetingRequest {
    groupId: bigint;
    sessionId: bigint;
}
export interface JoinMeetingResponse {
    session: GroupMeetingSession | undefined;
    /** CF RealtimeKit auth token for joiner */
    participantToken: string;
}
export interface LeaveMeetingRequest {
    groupId: bigint;
    sessionId: bigint;
}
export interface EndMeetingRequest {
    groupId: bigint;
    sessionId: bigint;
}
export interface GetActiveSessionRequest {
    groupId: bigint;
    channelId: number;
}
export interface GetActiveSessionResponse {
    /** null/empty if no active session */
    session: GroupMeetingSession | undefined;
}
/** Signal sent over WebSocket to notify group members of meeting events */
export interface GroupMeetingSignal {
    groupId: bigint;
    channelId: number;
    sessionId: bigint;
    type: MeetingSignalType;
    /** Who triggered the event */
    username: string;
    cfMeetingId: string;
}
export declare const UserMessage: MessageFns<UserMessage>;
export declare const Group: MessageFns<Group>;
export declare const Groups: MessageFns<Groups>;
export declare const UserMessages: MessageFns<UserMessages>;
export declare const GroupInvite: MessageFns<GroupInvite>;
export declare const GroupCommitAndWelcome: MessageFns<GroupCommitAndWelcome>;
export declare const GroupInvites: MessageFns<GroupInvites>;
export declare const GroupMessage: MessageFns<GroupMessage>;
export declare const GroupKeyPackage: MessageFns<GroupKeyPackage>;
export declare const GroupKeyPackages: MessageFns<GroupKeyPackages>;
export declare const GroupMessages: MessageFns<GroupMessages>;
export declare const GroupSyncRequest: MessageFns<GroupSyncRequest>;
export declare const GroupSyncRequests: MessageFns<GroupSyncRequests>;
export declare const GroupMemberUpdate: MessageFns<GroupMemberUpdate>;
export declare const GroupMemberUpdates: MessageFns<GroupMemberUpdates>;
export declare const GroupCommit: MessageFns<GroupCommit>;
export declare const GroupCommits: MessageFns<GroupCommits>;
export declare const GroupCommitSyncRequest: MessageFns<GroupCommitSyncRequest>;
export declare const GroupReAddRequest: MessageFns<GroupReAddRequest>;
export declare const GroupReAddRequests: MessageFns<GroupReAddRequests>;
export declare const Error: MessageFns<Error>;
export declare const Result: MessageFns<Result>;
export declare const Address: MessageFns<Address>;
export declare const Addresses: MessageFns<Addresses>;
export declare const UploadUserMessage: MessageFns<UploadUserMessage>;
export declare const MessageIdAndTo: MessageFns<MessageIdAndTo>;
export declare const UserMessageUploaded: MessageFns<UserMessageUploaded>;
export declare const UserOnlineStatusRequest: MessageFns<UserOnlineStatusRequest>;
export declare const UserOnlineStatusResponse: MessageFns<UserOnlineStatusResponse>;
export declare const Request: MessageFns<Request>;
export declare const Response: MessageFns<Response>;
export declare const ServerMessage: MessageFns<ServerMessage>;
export declare const ClientMessage: MessageFns<ClientMessage>;
export declare const CallSignal: MessageFns<CallSignal>;
export declare const GroupId: MessageFns<GroupId>;
export declare const AuthToken: MessageFns<AuthToken>;
export declare const SignedToken: MessageFns<SignedToken>;
export declare const FireflyIdentity: MessageFns<FireflyIdentity>;
export declare const FireflyGroupExtension: MessageFns<FireflyGroupExtension>;
export declare const FireflyGroupRole: MessageFns<FireflyGroupRole>;
export declare const FireflyGroupMember: MessageFns<FireflyGroupMember>;
export declare const FireflyGroupChannel: MessageFns<FireflyGroupChannel>;
export declare const PreKeyBundle: MessageFns<PreKeyBundle>;
export declare const PreKeyBundleEntry: MessageFns<PreKeyBundleEntry>;
export declare const PreKeyBundleEntries: MessageFns<PreKeyBundleEntries>;
export declare const ConversationStart: MessageFns<ConversationStart>;
export declare const PreKeyBundles: MessageFns<PreKeyBundles>;
export declare const Conversation: MessageFns<Conversation>;
export declare const Conversations: MessageFns<Conversations>;
export declare const EncryptedFile: MessageFns<EncryptedFile>;
export declare const EncryptedFiles: MessageFns<EncryptedFiles>;
export declare const MessagePayload: MessageFns<MessagePayload>;
export declare const CallMessage: MessageFns<CallMessage>;
export declare const SelfUserMessage: MessageFns<SelfUserMessage>;
export declare const UserMessageInner: MessageFns<UserMessageInner>;
export declare const GroupMessageInner: MessageFns<GroupMessageInner>;
export declare const RequestGroupReAdds: MessageFns<RequestGroupReAdds>;
export declare const RequestGroupSync: MessageFns<RequestGroupSync>;
export declare const GroupReAddRequestSuccess: MessageFns<GroupReAddRequestSuccess>;
export declare const CreateJoinLinkRequest: MessageFns<CreateJoinLinkRequest>;
export declare const CreateJoinLinkResponse: MessageFns<CreateJoinLinkResponse>;
export declare const JoinViaLinkRequest: MessageFns<JoinViaLinkRequest>;
export declare const JoinViaLinkSuccess: MessageFns<JoinViaLinkSuccess>;
export declare const GroupJoinRequest: MessageFns<GroupJoinRequest>;
export declare const GroupJoinRequests: MessageFns<GroupJoinRequests>;
export declare const GroupMeetingSession: MessageFns<GroupMeetingSession>;
export declare const CreateMeetingRequest: MessageFns<CreateMeetingRequest>;
export declare const CreateMeetingResponse: MessageFns<CreateMeetingResponse>;
export declare const JoinMeetingRequest: MessageFns<JoinMeetingRequest>;
export declare const JoinMeetingResponse: MessageFns<JoinMeetingResponse>;
export declare const LeaveMeetingRequest: MessageFns<LeaveMeetingRequest>;
export declare const EndMeetingRequest: MessageFns<EndMeetingRequest>;
export declare const GetActiveSessionRequest: MessageFns<GetActiveSessionRequest>;
export declare const GetActiveSessionResponse: MessageFns<GetActiveSessionResponse>;
export declare const GroupMeetingSignal: MessageFns<GroupMeetingSignal>;
type Builtin = Date | Function | Uint8Array | string | number | boolean | bigint | undefined;
export type DeepPartial<T> = T extends bigint ? string | number | bigint : T extends Builtin ? T : T extends globalThis.Array<infer U> ? globalThis.Array<DeepPartial<U>> : T extends ReadonlyArray<infer U> ? ReadonlyArray<DeepPartial<U>> : T extends {} ? {
    [K in keyof T]?: DeepPartial<T[K]>;
} : Partial<T>;
type KeysOfUnion<T> = T extends T ? keyof T : never;
export type Exact<P, I extends P> = P extends Builtin ? P : P & {
    [K in keyof P]: Exact<P[K], I[K]>;
} & {
    [K in Exclude<keyof I, KeysOfUnion<P>>]: never;
};
export interface MessageFns<T> {
    encode(message: T, writer?: BinaryWriter): BinaryWriter;
    decode(input: BinaryReader | Uint8Array, length?: number): T;
    fromJSON(object: any): T;
    toJSON(message: T): unknown;
    create<I extends Exact<DeepPartial<T>, I>>(base?: I): T;
    fromPartial<I extends Exact<DeepPartial<T>, I>>(object: I): T;
}
export {};
