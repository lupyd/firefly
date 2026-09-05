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
exports.protos = exports.FireflyClientNode = exports.initLogger = exports.FireflyBot = exports.FireflyClient = void 0;
const fs = __importStar(require("fs"));
const path = __importStar(require("path"));
const http = __importStar(require("http"));
const crypto = __importStar(require("crypto"));
const child_process_1 = require("child_process");
const firefly_client_node_1 = require("firefly-client-node");
Object.defineProperty(exports, "FireflyClientNode", { enumerable: true, get: function () { return firefly_client_node_1.FireflyClientNode; } });
Object.defineProperty(exports, "protos", { enumerable: true, get: function () { return firefly_client_node_1.protos; } });
Object.defineProperty(exports, "initLogger", { enumerable: true, get: function () { return firefly_client_node_1.initLogger; } });
const GroupMessageInner = firefly_client_node_1.protos.GroupMessageInner;
const UserMessageInner = firefly_client_node_1.protos.UserMessageInner;
// PKCE utilities
function base64url(buffer) {
    return buffer.toString('base64')
        .replace(/=/g, '')
        .replace(/\+/g, '-')
        .replace(/\//g, '_');
}
function generatePkce() {
    const verifier = base64url(crypto.randomBytes(32));
    const challenge = base64url(crypto.createHash('sha256').update(verifier).digest());
    return { verifier, challenge };
}
function decodeJwt(token) {
    try {
        const parts = token.split('.');
        if (parts.length < 2)
            return null;
        const payload = Buffer.from(parts[1], 'base64').toString('utf8');
        return JSON.parse(payload);
    }
    catch (e) {
        return null;
    }
}
function openBrowser(url) {
    const start = process.platform === 'darwin' ? 'open' :
        process.platform === 'win32' ? 'start' :
            'xdg-open';
    (0, child_process_1.exec)(`${start} "${url}"`, (err) => {
        if (err) {
            console.log(`Failed to open browser automatically. Please open this link manually:\n\n${url}\n`);
        }
    });
}
class FireflyClient {
    port;
    auth0Domain;
    auth0ClientId;
    auth0Audience;
    redirectUri;
    apiBaseUrl;
    wsUrl;
    emulatorMode;
    clientUsername;
    sessionFile;
    dbFile;
    commands;
    messageHandlers;
    groupMessageHandlers;
    client;
    session;
    constructor(options = {}) {
        this.port = options.port || 38295;
        this.auth0Domain = options.auth0Domain || 'https://auth.lupyd.com';
        this.auth0ClientId = options.auth0ClientId || 'GnfEyGY0JdD0Oige2HSpeErcaWLrvObm';
        this.auth0Audience = options.auth0Audience || 'https://lupyd.com';
        this.redirectUri = `http://localhost:${this.port}/callback`;
        this.apiBaseUrl = options.apiBaseUrl || 'https://firefly.lupyd.com';
        this.wsUrl = options.wsUrl || 'wss://firefly.lupyd.com/';
        this.emulatorMode = options.emulatorMode !== undefined ? options.emulatorMode : (process.env.EMULATOR_MODE === 'true');
        this.clientUsername = options.username || process.env.CLIENT_USERNAME || process.env.BOT_USERNAME || 'example_client';
        this.sessionFile = options.sessionFile || path.resolve(process.cwd(), 'client-session.json');
        this.dbFile = options.dbFile || path.resolve(process.cwd(), 'client-store.db');
        this.commands = new Map();
        this.messageHandlers = [];
        this.groupMessageHandlers = [];
        this.client = null;
        this.session = {
            access_token: null,
            refresh_token: null,
            expires_at: 0,
            username: null,
        };
    }
    // Registers a command trigger
    command(name, handler) {
        const trigger = name.startsWith('/') ? name.toLowerCase() : `/${name.toLowerCase()}`;
        this.commands.set(trigger, handler);
    }
    // Registers a general direct message handler
    onMessage(handler) {
        this.messageHandlers.push(handler);
    }
    // Registers a general group message handler
    onGroupMessage(handler) {
        this.groupMessageHandlers.push(handler);
    }
    // Fetch members' online status and last connected timestamp
    async getGroupMembersOnlineStatus(groupId) {
        const token = await this.getOrRenewAccessToken();
        const response = await fetch(`${this.apiBaseUrl}/group/members/status?groupId=${groupId}`, {
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${token}`,
            },
        });
        if (!response.ok) {
            const errText = await response.text();
            throw new Error(`Failed to fetch group members status: ${errText}`);
        }
        const buffer = await response.arrayBuffer();
        return firefly_client_node_1.protos.GroupMembersOnlineStatus.decode(new Uint8Array(buffer));
    }
    // Send read receipt up to a specific message ID
    async readUserMessagesUpto(other, uptoMessageId) {
        if (!this.client) {
            throw new Error('Client not initialized');
        }
        await this.client.readUserMessagesUpto(other, Number(uptoMessageId));
    }
    // Direct 1:1 user message
    async sendUserMessage(to, text) {
        if (!this.client) {
            throw new Error('Client not initialized');
        }
        const payload = {
            messagePayload: {
                text,
                files: undefined,
                replyingTo: 0n,
            },
            nonce: Math.floor(Math.random() * 9_999_999),
        };
        const messageInnerBytes = UserMessageInner.encode(payload).finish();
        return await this.client.encryptAndSend(to, Array.from(messageInnerBytes));
    }
    // Send group message
    async sendGroupMessage(groupId, text, channelId = 0) {
        if (!this.client) {
            throw new Error('Client not initialized');
        }
        const payload = {
            messagePayload: {
                text,
                files: undefined,
                replyingTo: 0n,
            },
            channelId,
        };
        const messageInnerBytes = GroupMessageInner.encode(payload).finish();
        return await this.client.encryptAndSendGroup(groupId, Array.from(messageInnerBytes));
    }
    // Create group
    async createGroup(name, description = '', settings) {
        if (!this.client) {
            throw new Error('Client not initialized');
        }
        return await this.client.createGroup(name, description, settings);
    }
    // Add/invite member to group
    async inviteMember(groupId, username, roleId = 1) {
        if (!this.client) {
            throw new Error('Client not initialized');
        }
        await this.client.addGroupMember(groupId, username, roleId);
    }
    async addGroupMember(groupId, username, roleId = 1) {
        return this.inviteMember(groupId, username, roleId);
    }
    // Kick member from group
    async kickMember(groupId, username) {
        if (!this.client) {
            throw new Error('Client not initialized');
        }
        await this.client.kickGroupMember(groupId, username);
    }
    async kickGroupMember(groupId, username) {
        return this.kickMember(groupId, username);
    }
    // Create join link for group
    async createJoinLink(groupId, expiresInSeconds = 86400, maxUses = 100) {
        if (!this.client) {
            throw new Error('Client not initialized');
        }
        return await this.client.createJoinLink(groupId, expiresInSeconds, maxUses);
    }
    // Join group via link
    async joinViaLink(linkToken) {
        if (!this.client) {
            throw new Error('Client not initialized');
        }
        await this.client.joinViaLink(linkToken);
        await this.client.loadAllGroups();
    }
    // Request to join / re-add to group
    async requestToJoin(groupId) {
        if (!this.client) {
            throw new Error('Client not initialized');
        }
        await this.client.requestToJoin(groupId);
    }
    async syncGroupJoinsAndReadds(groupId) {
        if (!this.client) {
            throw new Error('Client not initialized');
        }
        await this.client.syncGroupJoinsAndReadds(groupId);
    }
    // List group infos
    async getGroups() {
        if (!this.client) {
            throw new Error('Client not initialized');
        }
        return await this.client.getGroupInfos();
    }
    async getGroupInfos() {
        return this.getGroups();
    }
    // Get historical group messages
    async getGroupMessages(groupId, startBefore = 0, limit = 50) {
        if (!this.client) {
            throw new Error('Client not initialized');
        }
        return await this.client.getGroupMessages(groupId, startBefore, limit);
    }
    // Query online status
    async getOnlineStatus(usernames) {
        if (!this.client) {
            throw new Error('Client not initialized');
        }
        return await this.client.getOnlineStatus(usernames);
    }
    // Dispose client
    async dispose() {
        if (this.client) {
            await this.client.dispose();
            this.client = null;
        }
    }
    // Load and save session
    _loadSession() {
        if (fs.existsSync(this.sessionFile)) {
            try {
                const content = fs.readFileSync(this.sessionFile, 'utf8');
                this.session = JSON.parse(content);
            }
            catch (e) {
                console.error('Failed to parse cached session file, starting fresh.');
            }
        }
    }
    _saveSession() {
        fs.writeFileSync(this.sessionFile, JSON.stringify(this.session, null, 2), 'utf8');
    }
    // Exchange Code
    async _exchangeCodeForTokens(code, codeVerifier) {
        const params = new URLSearchParams({
            grant_type: 'authorization_code',
            client_id: this.auth0ClientId,
            code_verifier: codeVerifier,
            code: code,
            redirect_uri: this.redirectUri,
        });
        const response = await fetch(`${this.auth0Domain}/oauth/token`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: params.toString(),
        });
        if (!response.ok) {
            const errText = await response.text();
            throw new Error(`Failed to exchange code for tokens: ${errText}`);
        }
        return response.json();
    }
    // Refresh token
    async _refreshAccessToken(refreshToken) {
        const params = new URLSearchParams({
            grant_type: 'refresh_token',
            client_id: this.auth0ClientId,
            refresh_token: refreshToken,
        });
        const response = await fetch(`${this.auth0Domain}/oauth/token`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: params.toString(),
        });
        if (!response.ok) {
            const errText = await response.text();
            throw new Error(`Failed to refresh access token: ${errText}`);
        }
        return response.json();
    }
    // Start Auth0 Flow
    _startOauthFlow() {
        return new Promise((resolve, reject) => {
            const { verifier, challenge } = generatePkce();
            const state = base64url(crypto.randomBytes(16));
            const server = http.createServer(async (req, res) => {
                const reqUrl = new URL(req.url || '', `http://localhost:${this.port}`);
                if (reqUrl.pathname === '/callback') {
                    const code = reqUrl.searchParams.get('code');
                    const returnedState = reqUrl.searchParams.get('state');
                    if (returnedState !== state) {
                        res.writeHead(400, { 'Content-Type': 'text/plain' });
                        res.end('CSRF state mismatch. Authentication failed.');
                        server.close();
                        reject(new Error('CSRF State mismatch'));
                        return;
                    }
                    if (!code) {
                        res.writeHead(400, { 'Content-Type': 'text/plain' });
                        res.end('Authorization code missing in redirect.');
                        server.close();
                        reject(new Error('Authorization code missing'));
                        return;
                    }
                    res.writeHead(200, { 'Content-Type': 'text/html' });
                    res.end(`
            <html>
              <body style="font-family: Arial, sans-serif; text-align: center; padding-top: 100px; background-color: #121214; color: #e1e1e6;">
                <h1 style="color: #04d361;">Sign In Successful!</h1>
                <p>You can now close this tab and return to your terminal.</p>
              </body>
            </html>
          `);
                    server.close();
                    try {
                        console.log('Exchanging authorization code for tokens...');
                        const tokenResponse = await this._exchangeCodeForTokens(code, verifier);
                        const claims = decodeJwt(tokenResponse.access_token);
                        this.session.access_token = tokenResponse.access_token;
                        this.session.refresh_token = tokenResponse.refresh_token;
                        this.session.expires_at = Date.now() + (tokenResponse.expires_in * 1000);
                        this.session.username = claims ? claims.uname : 'client';
                        this._saveSession();
                        console.log(`Successfully authenticated as user: ${this.session.username}`);
                        resolve();
                    }
                    catch (err) {
                        reject(err);
                    }
                }
            });
            server.listen(this.port, () => {
                const authUrl = `${this.auth0Domain}/authorize?` + new URLSearchParams({
                    client_id: this.auth0ClientId,
                    audience: this.auth0Audience,
                    response_type: 'code',
                    scope: 'openid profile email offline_access',
                    redirect_uri: this.redirectUri,
                    code_challenge_method: 'S256',
                    code_challenge: challenge,
                    state: state,
                }).toString();
                console.log('Opening browser for Auth0 Sign In...');
                openBrowser(authUrl);
            });
        });
    }
    // Token management interface
    async getOrRenewAccessToken() {
        if (this.emulatorMode) {
            if (this.session.access_token && this.client) {
                try {
                    this.client.setAccessToken(this.session.access_token);
                }
                catch (e) { }
            }
            return this.session.access_token;
        }
        if (!this.session.access_token || Date.now() + 120000 >= this.session.expires_at) {
            if (this.session.refresh_token) {
                console.log('Access token expired or expiring soon. Refreshing token...');
                try {
                    const refreshResponse = await this._refreshAccessToken(this.session.refresh_token);
                    const claims = decodeJwt(refreshResponse.access_token);
                    this.session.access_token = refreshResponse.access_token;
                    if (refreshResponse.refresh_token) {
                        this.session.refresh_token = refreshResponse.refresh_token;
                    }
                    this.session.expires_at = Date.now() + (refreshResponse.expires_in * 1000);
                    this.session.username = claims ? claims.uname : 'client';
                    this._saveSession();
                    console.log('Access token successfully refreshed.');
                }
                catch (err) {
                    console.error('Failed to refresh token, starting new login flow:', err.message);
                    await this._startOauthFlow();
                }
            }
            else {
                await this._startOauthFlow();
            }
        }
        return this.session.access_token;
    }
    // Handles parsed message routing to command registry
    async _handleMessage({ text, sender, isGroup, groupId, channelId }) {
        if (!text)
            return;
        let commandName = '';
        let args = [];
        if (text.startsWith('/')) {
            const parts = text.trim().split(/\s+/);
            commandName = parts[0].toLowerCase();
            args = parts.slice(1);
        }
        const ctx = {
            client: this,
            bot: this,
            sender,
            text,
            command: commandName,
            args,
            isGroup,
            groupId,
            channelId,
            reply: async (replyText) => {
                if (isGroup && groupId !== null && channelId !== null) {
                    const payload = {
                        messagePayload: {
                            text: replyText,
                            files: undefined,
                            replyingTo: 0n,
                        },
                        channelId: channelId,
                    };
                    const messageInnerBytes = GroupMessageInner.encode(payload).finish();
                    await this.client.encryptAndSendGroup(groupId, Array.from(messageInnerBytes));
                }
                else {
                    const payload = {
                        messagePayload: {
                            text: replyText,
                            files: undefined,
                            replyingTo: 0n,
                        },
                        nonce: Math.floor(Math.random() * 9_999_999),
                    };
                    const messageInnerBytes = UserMessageInner.encode(payload).finish();
                    await this.client.encryptAndSend(sender, Array.from(messageInnerBytes));
                }
            }
        };
        if (isGroup) {
            for (const handler of this.groupMessageHandlers) {
                try {
                    await handler(ctx);
                }
                catch (err) {
                    console.error('Error executing group message handler:', err);
                }
            }
        }
        else {
            for (const handler of this.messageHandlers) {
                try {
                    await handler(ctx);
                }
                catch (err) {
                    console.error('Error executing message handler:', err);
                }
            }
        }
        if (commandName && this.commands.has(commandName)) {
            const handler = this.commands.get(commandName);
            try {
                await handler(ctx);
            }
            catch (err) {
                console.error(`Error executing command ${commandName}:`, err);
            }
        }
    }
    // Initializes FFI connection
    async start() {
        this._loadSession();
        if (this.emulatorMode) {
            console.log(`[Emulator Mode] Skipping Auth0 login. Direct name: ${this.clientUsername}`);
            this.session.access_token = this.clientUsername;
            this.session.refresh_token = 'dummy_refresh';
            this.session.expires_at = Date.now() + 1000 * 60 * 60 * 24;
            this.session.username = this.clientUsername;
            this._saveSession();
        }
        else {
            await this.getOrRenewAccessToken();
        }
        console.log(`Initializing Firefly MLS Client [user: ${this.session.username}]...`);
        const callbacks = {
            name: this.session.username,
            initialToken: this.session.access_token,
            getAccessToken: () => {
                console.log(`[getAccessToken Callback] requested for ${this.session.username}`);
                const token = this.session.access_token;
                console.log(`[getAccessToken Callback] returning token: ${token}`);
                return token;
            },
            onMessage: async (err, msgJson) => {
                if (err) {
                    console.error("onMessage error:", err);
                    return;
                }
                console.log("[onMessage] trigger check, msgJson:", msgJson);
                if (!msgJson)
                    return;
                try {
                    const msg = JSON.parse(msgJson);
                    const other = msg.other;
                    const sentByOther = msg.sent_by_other;
                    const message = msg.message;
                    console.log(`[onMessage] msg received from: ${other}, sentByOther: ${sentByOther}`);
                    if (!sentByOther)
                        return; // skip outgoing
                    const decoded = UserMessageInner.decode(new Uint8Array(message));
                    console.log('[onMessage] decoded inner message:', JSON.stringify(decoded, (k, v) => typeof v === 'bigint' ? v.toString() : v));
                    const text = decoded.messagePayload?.text;
                    if (!text)
                        return;
                    console.log(`[Direct Message] From: ${other}, Content: "${text}"`);
                    await this._handleMessage({
                        text,
                        sender: other,
                        isGroup: false,
                        groupId: null,
                        channelId: null,
                    });
                }
                catch (err) {
                    console.error('Error handling direct message:', err);
                }
            },
            onGroupMessage: async (err, msgJson) => {
                if (err) {
                    console.error("onGroupMessage error:", err);
                    return;
                }
                console.log("[onGroupMessage] trigger check, msgJson:", msgJson);
                if (!msgJson)
                    return;
                try {
                    const msg = JSON.parse(msgJson);
                    const by = msg.by;
                    const groupId = msg.group_id;
                    const message = msg.message;
                    const channelId = msg.channel_id;
                    console.log(`[onGroupMessage] msg received from: ${by}, group: ${groupId}`);
                    if (by === this.session.username)
                        return; // skip outgoing
                    const decoded = GroupMessageInner.decode(new Uint8Array(message));
                    console.log('[onGroupMessage] decoded inner message:', JSON.stringify(decoded, (k, v) => typeof v === 'bigint' ? v.toString() : v));
                    const text = decoded.messagePayload?.text;
                    if (!text)
                        return;
                    console.log(`[Group Message] Group ID: ${groupId}, By: ${by}, Content: "${text}"`);
                    await this._handleMessage({
                        text,
                        sender: by,
                        isGroup: true,
                        groupId: groupId,
                        channelId: channelId,
                    });
                }
                catch (err) {
                    console.error('Error handling group message:', err);
                }
            },
            onGroupJoined: async (groupId) => {
                console.log(`[Group Joined] Client joined group: ${groupId}`);
                try {
                    await this.client.loadAllGroups();
                }
                catch (err) {
                    console.error('Failed to load groups on join:', err);
                }
            },
            onCallSignal: () => { },
            onGroupMeetingSignal: () => { },
            onReadUserMessagesUpto: () => { },
        };
        this.client = await firefly_client_node_1.FireflyClientNode.create(this.apiBaseUrl, this.wsUrl, 2000, callbacks, this.dbFile, 15000);
        console.log('Connecting to Firefly MLS network...');
        try {
            console.log('Running checkSetup()...');
            await this.client.checkSetup();
            console.log('checkSetup() completed successfully!');
            // Run initializeWithRetrying in the background without awaiting it
            this.client.initializeWithRetrying().catch((err) => {
                console.error('Error in client initialization background loop:', err);
            });
            // Poll isInitialized() until it is true (up to 30 seconds)
            let initialized = false;
            for (let i = 0; i < 60; i++) {
                if (this.client.isInitialized()) {
                    initialized = true;
                    break;
                }
                await new Promise((resolve) => setTimeout(resolve, 500));
            }
            if (!initialized) {
                throw new Error('Client timeout waiting for initialization');
            }
            await this.client.loadAllGroups();
            console.log('Client is fully connected and listening.');
        }
        catch (err) {
            console.error('Error starting connection:', err);
        }
    }
}
exports.FireflyClient = FireflyClient;
const FireflyBot = FireflyClient;
exports.FireflyBot = FireflyBot;
