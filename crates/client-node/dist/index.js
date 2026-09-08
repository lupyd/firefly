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
    static async create(fireflyBaseUrl, fireflyBaseWsUrl, retryIntervalInMs, callbacksObj, keyStoresPathname, requestTimeoutInMs, storage) {
        const cb = { ...callbacksObj };
        if (storage) {
            cb.storageProviders = storage;
        }
        const raw = await wasmPkg.FireflyClientNode.create(fireflyBaseUrl, fireflyBaseWsUrl, retryIntervalInMs, cb, keyStoresPathname, requestTimeoutInMs);
        return new FireflyClientNode(raw);
    }
    setAccessToken(token) {
        this.inner.setAccessToken(token);
    }
    async checkSetup() {
        return await this.inner.checkSetup();
    }
    async initializeWithRetrying() {
        return await this.inner.initializeWithRetrying();
    }
    isInitialized() {
        return this.inner.isInitialized();
    }
    getConnectionState() {
        return this.inner.getConnectionState();
    }
    async dispose() {
        return await this.inner.dispose();
    }
    async encryptAndSend(to, payload) {
        const arr = payload instanceof Uint8Array ? payload : new Uint8Array(payload);
        return await this.inner.encryptAndSend(to, arr);
    }
    async encryptAndSendGroup(groupId, payload) {
        const arr = payload instanceof Uint8Array ? payload : new Uint8Array(payload);
        return await this.inner.encryptAndSendGroup(groupId, arr);
    }
    async createGroup(name, description = '', settings) {
        return await this.inner.createGroup(name, description, settings ?? null);
    }
    async addGroupMember(groupId, username, roleId = 1) {
        return await this.inner.addGroupMember(groupId, username, roleId);
    }
    async kickGroupMember(groupId, username) {
        return await this.inner.kickGroupMember(groupId, username);
    }
    async deleteGroup(groupId) {
        return await this.inner.deleteGroup(groupId);
    }
    async createJoinLink(groupId, expiresInSeconds = 86400, maxUses = 100) {
        return await this.inner.createJoinLink(groupId, expiresInSeconds, maxUses);
    }
    async joinViaLink(token) {
        return await this.inner.joinViaLink(token);
    }
    async requestToJoin(groupId) {
        return await this.inner.requestToJoin(groupId);
    }
    async syncGroupJoinsAndReadds(groupId) {
        return await this.inner.syncGroupJoinsAndReadds(groupId);
    }
    async loadAllGroups() {
        return await this.inner.loadAllGroups();
    }
    async getGroupInfos() {
        return await this.inner.getGroupInfos();
    }
    async getGroupMessages(groupId, startBefore = 0, limit = 50) {
        return await this.inner.getGroupMessages(groupId, startBefore, limit);
    }
    async getOnlineStatus(usernames) {
        return await this.inner.getOnlineStatus(usernames);
    }
    async readUserMessagesUpto(other, uptoMessageId) {
        return await this.inner.readUserMessagesUpto(other, uptoMessageId);
    }
    async uploadFcmToken(token) {
        return await this.inner.uploadFcmToken(token ?? null);
    }
    async getConversations(token) {
        return await this.inner.getConversations(token);
    }
    async getGroupExtension(groupId) {
        return await this.inner.getGroupExtension(groupId);
    }
    async exportGroupMeetingKey(groupId) {
        return await this.inner.exportGroupMeetingKey(groupId);
    }
}
exports.FireflyClientNode = FireflyClientNode;
