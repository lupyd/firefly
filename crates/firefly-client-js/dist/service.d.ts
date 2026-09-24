import { GroupMessages, GroupMessage, GroupKeyPackages, PreKeyBundles, UserMessages, ConversationStart, Conversations, Groups, SignedToken, GroupKeyPackage, Group, PreKeyBundle, CreateMeetingRequest, CreateMeetingResponse, JoinMeetingRequest, JoinMeetingResponse, LeaveMeetingRequest, EndMeetingRequest, GetActiveSessionResponse } from "firefly-protos-js";
export declare class HttpError extends Error {
    statusCode: number;
    responseText: string;
    constructor(statusCode: number, responseText: string);
}
export declare class FireflyService {
    baseUrl: string;
    getAuthToken: () => Promise<string>;
    constructor(baseUrl: string, getAuthToken: () => Promise<string>);
    private req;
    getJWKS(): Promise<any>;
    getGroupMessages(opts: {
        groupId?: number;
        startAfter?: bigint;
        limit?: number;
    }): Promise<GroupMessages>;
    getKeyPackages(): Promise<GroupKeyPackages>;
    getKeyPackage(username: string): Promise<GroupKeyPackage>;
    getPreKeyBundles(): Promise<PreKeyBundles>;
    getUserMessages(opts: {
        conversationId?: number;
        startAfter?: bigint;
        limit?: number;
    }): Promise<UserMessages>;
    getConversations(): Promise<Conversations>;
    getConversation(other: string, preKeyBundleRequired?: boolean): Promise<ConversationStart>;
    getPreKeyBundle(other: string): Promise<PreKeyBundle>;
    getGroups(): Promise<Groups>;
    sign(credential: Uint8Array): Promise<SignedToken>;
    createGroup(group: Group): Promise<bigint>;
    uploadKeyPackages(packages: GroupKeyPackages): Promise<GroupKeyPackages>;
    postGroupMessage(groupId: bigint, message: Uint8Array): Promise<GroupMessage>;
    postCommit(groupId: bigint, message: Uint8Array): Promise<GroupMessage>;
    invite(groupId: bigint, invitee: string, welcomeMessage: Uint8Array, commitId: bigint): Promise<void>;
    requestReAdd(groupId: number): Promise<void>;
    deleteGroup(id: number): Promise<void>;
    deleteGroupMember(uname: string, groupId: number): Promise<void>;
    deleteGroupInvites(commitIds: bigint[]): Promise<void>;
    deleteKeyPackages(ids: number[]): Promise<void>;
    deletePreKeyBundles(ids: number[]): Promise<void>;
    deleteConversations(ids: number[]): Promise<void>;
    deleteUserMessages(until: bigint): Promise<void>;
    recreateConversations(): Promise<void>;
    getWebrtcConfig(): Promise<RTCConfiguration>;
    deleteUserMessage(convoId: bigint, msgId: bigint): Promise<Response>;
    createMeeting(request: CreateMeetingRequest): Promise<CreateMeetingResponse>;
    joinMeeting(request: JoinMeetingRequest): Promise<JoinMeetingResponse>;
    leaveMeeting(request: LeaveMeetingRequest): Promise<void>;
    endMeeting(request: EndMeetingRequest): Promise<void>;
    getActiveSession(groupId: number, channelId: number): Promise<GetActiveSessionResponse>;
}
