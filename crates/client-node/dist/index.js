"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.FireflyClientNode = exports.initLogger = exports.protos = void 0;
// @ts-ignore
const wasmPkg = __importStar(require("../wasm/firefly_client_node.js"));
exports.protos = __importStar(require("./protos/message"));
const initLogger = (filePath) => {
    if (typeof wasmPkg.init_logger === 'function') {
        wasmPkg.init_logger(filePath);
    }
};
exports.initLogger = initLogger;
class FireflyClientNode {
    inner;
    constructor(inner) {
        this.inner = inner;
    }
    static async create(fireflyBaseUrl, fireflyBaseWsUrl, retryIntervalInMs, callbacksObj, keyStoresPathname, requestTimeoutInMs) {
        const raw = await wasmPkg.FireflyClientNode.create(fireflyBaseUrl, fireflyBaseWsUrl, retryIntervalInMs, callbacksObj, keyStoresPathname, requestTimeoutInMs);
        return new FireflyClientNode(raw);
    }
    setAccessToken(token) {
        this.inner.set_access_token(token);
    }
    set_access_token(token) {
        this.inner.set_access_token(token);
    }
    async checkSetup() {
        return await this.inner.check_setup();
    }
    async check_setup() {
        return await this.inner.check_setup();
    }
    async initializeWithRetrying() {
        return await this.inner.initialize_with_retrying();
    }
    async initialize_with_retrying() {
        return await this.inner.initialize_with_retrying();
    }
    isInitialized() {
        return this.inner.is_initialized();
    }
    is_initialized() {
        return this.inner.is_initialized();
    }
    getConnectionState() {
        return this.inner.get_connection_state();
    }
    get_connection_state() {
        return this.inner.get_connection_state();
    }
    async dispose() {
        return await this.inner.dispose();
    }
    async encryptAndSend(to, payload) {
        const arr = payload instanceof Uint8Array ? payload : new Uint8Array(payload);
        return await this.inner.encrypt_and_send(to, arr);
    }
    async encrypt_and_send(to, payload) {
        return this.encryptAndSend(to, payload);
    }
    async encryptAndSendGroup(groupId, payload) {
        const arr = payload instanceof Uint8Array ? payload : new Uint8Array(payload);
        return await this.inner.encrypt_and_send_group(groupId, arr);
    }
    async encrypt_and_send_group(groupId, payload) {
        return this.encryptAndSendGroup(groupId, payload);
    }
    async createGroup(name, description = '', settings) {
        return await this.inner.create_group(name, description, settings ?? null);
    }
    async create_group(name, description = '', settings) {
        return this.createGroup(name, description, settings);
    }
    async addGroupMember(groupId, username, roleId = 1) {
        return await this.inner.add_group_member(groupId, username, roleId);
    }
    async add_group_member(groupId, username, roleId = 1) {
        return this.addGroupMember(groupId, username, roleId);
    }
    async kickGroupMember(groupId, username) {
        return await this.inner.kick_group_member(groupId, username);
    }
    async kick_group_member(groupId, username) {
        return this.kickGroupMember(groupId, username);
    }
    async deleteGroup(groupId) {
        return await this.inner.delete_group(groupId);
    }
    async delete_group(groupId) {
        return this.deleteGroup(groupId);
    }
    async createJoinLink(groupId, expiresInSeconds = 86400, maxUses = 100) {
        return await this.inner.create_join_link(groupId, expiresInSeconds, maxUses);
    }
    async create_join_link(groupId, expiresInSeconds = 86400, maxUses = 100) {
        return this.createJoinLink(groupId, expiresInSeconds, maxUses);
    }
    async joinViaLink(token) {
        return await this.inner.join_via_link(token);
    }
    async join_via_link(token) {
        return this.joinViaLink(token);
    }
    async requestToJoin(groupId) {
        return await this.inner.request_to_join(groupId);
    }
    async request_to_join(groupId) {
        return this.requestToJoin(groupId);
    }
    async syncGroupJoinsAndReadds(groupId) {
        return await this.inner.sync_group_joins_and_readds(groupId);
    }
    async sync_group_joins_and_readds(groupId) {
        return this.syncGroupJoinsAndReadds(groupId);
    }
    async loadAllGroups() {
        return await this.inner.load_all_groups();
    }
    async load_all_groups() {
        return this.loadAllGroups();
    }
    async getGroupInfos() {
        return await this.inner.get_group_infos();
    }
    async get_group_infos() {
        return this.getGroupInfos();
    }
    async getGroupMessages(groupId, startBefore = 0, limit = 50) {
        return await this.inner.get_group_messages(groupId, startBefore, limit);
    }
    async get_group_messages(groupId, startBefore = 0, limit = 50) {
        return this.getGroupMessages(groupId, startBefore, limit);
    }
    async getOnlineStatus(usernames) {
        return await this.inner.get_online_status(usernames);
    }
    async get_online_status(usernames) {
        return this.getOnlineStatus(usernames);
    }
    async readUserMessagesUpto(other, uptoMessageId) {
        return await this.inner.read_user_messages_upto(other, uptoMessageId);
    }
    async read_user_messages_upto(other, uptoMessageId) {
        return this.readUserMessagesUpto(other, uptoMessageId);
    }
    async uploadFcmToken(token) {
        return await this.inner.upload_fcm_token(token ?? null);
    }
    async upload_fcm_token(token) {
        return this.uploadFcmToken(token);
    }
    async getConversations(token) {
        return await this.inner.get_conversations(token);
    }
    async get_conversations(token) {
        return this.getConversations(token);
    }
    async getGroupExtension(groupId) {
        return await this.inner.get_group_extension(groupId);
    }
    async get_group_extension(groupId) {
        return this.getGroupExtension(groupId);
    }
    async exportGroupMeetingKey(groupId) {
        return await this.inner.export_group_meeting_key(groupId);
    }
    async export_group_meeting_key(groupId) {
        return this.exportGroupMeetingKey(groupId);
    }
}
exports.FireflyClientNode = FireflyClientNode;
