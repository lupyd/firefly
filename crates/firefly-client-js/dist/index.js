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
var __exportStar = (this && this.__exportStar) || function(m, exports) {
    for (var p in m) if (p !== "default" && !Object.prototype.hasOwnProperty.call(exports, p)) __createBinding(exports, m, p);
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.protos = exports.FireflyClientNode = exports.initLogger = exports.FireflyBot = exports.FireflyClient = exports.SignInRequiredError = exports.EXIT_SIGN_IN_REQUIRED = exports.DEFAULT_GROUP_PERMISSIONS = exports.UserPermission = void 0;
const fs = __importStar(require("fs"));
const path = __importStar(require("path"));
const http = __importStar(require("http"));
const crypto = __importStar(require("crypto"));
const child_process_1 = require("child_process");
const firefly_client_node_1 = require("firefly-client-node");
Object.defineProperty(exports, "FireflyClientNode", { enumerable: true, get: function () { return firefly_client_node_1.FireflyClientNode; } });
Object.defineProperty(exports, "protos", { enumerable: true, get: function () { return firefly_client_node_1.protos; } });
Object.defineProperty(exports, "initLogger", { enumerable: true, get: function () { return firefly_client_node_1.initLogger; } });
var firefly_client_node_2 = require("firefly-client-node");
Object.defineProperty(exports, "UserPermission", { enumerable: true, get: function () { return firefly_client_node_2.UserPermission; } });
Object.defineProperty(exports, "DEFAULT_GROUP_PERMISSIONS", { enumerable: true, get: function () { return firefly_client_node_2.DEFAULT_GROUP_PERMISSIONS; } });
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
    // On Windows `start` takes the first quoted argument as the window title, so pass an empty one
    const start = process.platform === 'darwin' ? 'open' :
        process.platform === 'win32' ? 'start ""' :
            'xdg-open';
    (0, child_process_1.exec)(`${start} "${url}"`, (err) => {
        if (err) {
            console.log(`Failed to open browser automatically. Please open this link manually:\n\n${url}\n`);
        }
    });
}
// Exit code for "the saved sign-in can't be renewed; a person has to sign in again".
// run-bots.ts stops restarting a bot that exits with it. 78 is EX_CONFIG from sysexits.h.
exports.EXIT_SIGN_IN_REQUIRED = 78;
class SignInRequiredError extends Error {
    constructor(message) {
        super(message);
        this.name = 'SignInRequiredError';
    }
}
exports.SignInRequiredError = SignInRequiredError;
// `rejected` means the auth server refused the refresh token, so retrying won't help.
// Otherwise the failure (network, 5xx) is temporary.
class TokenRefreshError extends Error {
    rejected;
    constructor(message, rejected) {
        super(message);
        this.rejected = rejected;
    }
}
// Renew this long before the access token expires (or halfway, for very short-lived tokens)
const REFRESH_LEAD_MS = 5 * 60_000;
const REFRESH_RETRY_MIN_MS = 15_000;
const REFRESH_RETRY_MAX_MS = 5 * 60_000;
// setTimeout fires immediately for delays above 2^31-1 ms
const MAX_TIMER_MS = 2_147_483_647;
// How late the renewal timer may run before a request triggers the renewal itself
const TIMER_LATE_MS = 5_000;
class FireflyClient {
    config;
    port;
    auth0Domain;
    auth0ClientId;
    auth0Audience;
    redirectUri;
    apiBaseUrl;
    wsUrl;
    emulatorMode;
    unattended;
    clientUsername;
    renewing = null;
    renewTimer = null;
    renewDueAt = 0;
    renewRetryMs = REFRESH_RETRY_MIN_MS;
    sessionFile;
    dbFile;
    commands;
    messageHandlers;
    groupMessageHandlers;
    memberJoinHandlers;
    memberLeaveHandlers;
    client;
    session;
    constructor(options = {}) {
        this.config = options;
        this.port = options.port || 38295;
        this.auth0Domain = options.auth0Domain || 'https://auth.lupyd.com';
        this.auth0ClientId = options.auth0ClientId || 'GnfEyGY0JdD0Oige2HSpeErcaWLrvObm';
        this.auth0Audience = options.auth0Audience || 'https://lupyd.com';
        this.redirectUri = `http://localhost:${this.port}/callback`;
        this.apiBaseUrl = options.apiBaseUrl || process.env.FIREFLY_BASE_URL || 'https://firefly.lupyd.com';
        // With only the HTTP URL changed, use the same server for the websocket rather than
        // quietly mixing a local server with production
        const customBase = options.apiBaseUrl || process.env.FIREFLY_BASE_URL;
        this.wsUrl = options.wsUrl || process.env.FIREFLY_BASE_WS_URL ||
            (customBase ? customBase.replace(/^http/, 'ws').replace(/\/?$/, '/') : 'wss://firefly.lupyd.com/');
        this.emulatorMode = options.emulatorMode !== undefined ? options.emulatorMode : (process.env.EMULATOR_MODE === 'true');
        this.unattended = options.unattended !== undefined ? options.unattended : (process.env.FIREFLY_UNATTENDED === 'true');
        this.clientUsername = options.username || process.env.CLIENT_USERNAME || process.env.BOT_USERNAME || 'example_client';
        this.sessionFile = options.sessionFile || path.resolve(process.cwd(), 'client-session.json');
        this.dbFile = options.dbFile || path.resolve(process.cwd(), 'client-store.db');
        this.commands = new Map();
        this.messageHandlers = [];
        this.groupMessageHandlers = [];
        this.memberJoinHandlers = [];
        this.memberLeaveHandlers = [];
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
    // Registers a handler for members joining a group this client is in (never fired for itself)
    onMemberJoin(handler) {
        this.memberJoinHandlers.push(handler);
    }
    // Registers a handler for members leaving or being removed from a group this client is in
    onMemberLeave(handler) {
        this.memberLeaveHandlers.push(handler);
    }
    async _getGroupExtension(groupId) {
        if (!this.client) {
            throw new Error('Client not initialized');
        }
        const bytes = await this.client.getGroupExtension(groupId);
        return firefly_client_node_1.protos.FireflyGroupExtension.decode(new Uint8Array(bytes));
    }
    // Members of a group with their resolved group-level permissions
    async getGroupMembers(groupId) {
        const extension = await this._getGroupExtension(groupId);
        return extension.members.map((member) => ({
            username: member.username,
            roleId: member.role,
            permissions: extension.roles.find((role) => role.id === member.role)?.permissions ?? 0,
        }));
    }
    async getGroupChannels(groupId) {
        const extension = await this._getGroupExtension(groupId);
        return extension.channels.map((channel) => ({ id: channel.id, name: channel.name, type: channel.type }));
    }
    // Whether the user holds the given permission (ManageGroup by default, same as the Lupyd apps' admin check)
    async isGroupAdmin(groupId, username, permission = firefly_client_node_1.UserPermission.ManageGroup) {
        const member = (await this.getGroupMembers(groupId)).find((m) => m.username === username);
        return member !== undefined && (member.permissions & permission) === permission;
    }
    async _emitMemberEvents(groupId, added, removed) {
        const self = this.session.username;
        const emit = async (handlers, username) => {
            const ctx = {
                bot: this,
                groupId,
                username,
                sendToGroup: async (text, channelId) => {
                    const target = channelId ?? (await this.getGroupChannels(groupId))[0]?.id ?? 0;
                    await this.sendGroupMessage(groupId, text, target);
                },
                sendDirect: async (text) => {
                    await this.sendUserMessage(username, text);
                },
            };
            for (const handler of handlers) {
                try {
                    await handler(ctx);
                }
                catch (err) {
                    console.error('Error executing member event handler:', err);
                }
            }
        };
        for (const username of added) {
            if (username !== self)
                await emit(this.memberJoinHandlers, username);
        }
        for (const username of removed) {
            if (username !== self)
                await emit(this.memberLeaveHandlers, username);
        }
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
                messageType: 0,
            },
            nonce: Math.floor(Math.random() * 9_999_999),
            messageType: 0,
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
                messageType: 0,
            },
            channelId,
            messageType: 0,
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
    // Favourite messages APIs
    async addFavourite(favourite) {
        if (!this.client) {
            throw new Error('Client not initialized');
        }
        return await this.client.addFavourite(favourite);
    }
    async addFavorite(favorite) {
        return await this.addFavourite(favorite);
    }
    async removeUserFavourite(other, messageId) {
        if (!this.client) {
            throw new Error('Client not initialized');
        }
        return await this.client.removeUserFavourite(other, messageId);
    }
    async removeUserFavorite(other, messageId) {
        return await this.removeUserFavourite(other, messageId);
    }
    async removeGroupFavourite(groupId, messageId) {
        if (!this.client) {
            throw new Error('Client not initialized');
        }
        return await this.client.removeGroupFavourite(groupId, messageId);
    }
    async removeGroupFavorite(groupId, messageId) {
        return await this.removeGroupFavourite(groupId, messageId);
    }
    async removeFavouriteById(favouriteId) {
        if (!this.client) {
            throw new Error('Client not initialized');
        }
        return await this.client.removeFavouriteById(favouriteId);
    }
    async removeFavoriteById(favoriteId) {
        return await this.removeFavouriteById(favoriteId);
    }
    async isUserFavourite(other, messageId) {
        if (!this.client) {
            throw new Error('Client not initialized');
        }
        return await this.client.isUserFavourite(other, messageId);
    }
    async isUserFavorite(other, messageId) {
        return await this.isUserFavourite(other, messageId);
    }
    async isGroupFavourite(groupId, messageId) {
        if (!this.client) {
            throw new Error('Client not initialized');
        }
        return await this.client.isGroupFavourite(groupId, messageId);
    }
    async isGroupFavorite(groupId, messageId) {
        return await this.isGroupFavourite(groupId, messageId);
    }
    async getFavourites(limit = 50, offset = 0) {
        if (!this.client) {
            throw new Error('Client not initialized');
        }
        return await this.client.getFavourites(limit, offset);
    }
    async getFavorites(limit = 50, offset = 0) {
        return await this.getFavourites(limit, offset);
    }
    async getUserFavourites(other, limit = 50, offset = 0) {
        if (!this.client) {
            throw new Error('Client not initialized');
        }
        return await this.client.getUserFavourites(other, limit, offset);
    }
    async getUserFavorites(other, limit = 50, offset = 0) {
        return await this.getUserFavourites(other, limit, offset);
    }
    async getGroupFavourites(groupId, channelId, limit = 50, offset = 0) {
        if (!this.client) {
            throw new Error('Client not initialized');
        }
        return await this.client.getGroupFavourites(groupId, channelId, limit, offset);
    }
    async getGroupFavorites(groupId, channelId, limit = 50, offset = 0) {
        return await this.getGroupFavourites(groupId, channelId, limit, offset);
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
        // The session holds access/refresh tokens: keep it owner-readable only.
        fs.writeFileSync(this.sessionFile, JSON.stringify(this.session, null, 2), { encoding: 'utf8', mode: 0o600 });
        // `mode` only applies on creation; tighten perms on a pre-existing file too.
        try {
            fs.chmodSync(this.sessionFile, 0o600);
        }
        catch (e) {
            console.error('Failed to restrict permissions on session file.');
        }
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
        let response;
        try {
            response = await fetch(`${this.auth0Domain}/oauth/token`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                body: params.toString(),
            });
        }
        catch (err) {
            throw new TokenRefreshError(`Failed to reach the sign-in server: ${err.message}`, false);
        }
        if (!response.ok) {
            const errText = await response.text().catch(() => '');
            // 400/401/403 are the server refusing the refresh token (invalid_grant: revoked or expired)
            const rejected = response.status === 400 || response.status === 401 || response.status === 403;
            throw new TokenRefreshError(`Failed to refresh access token (${response.status}): ${errText}`, rejected);
        }
        return response.json();
    }
    // Stores a token response from a sign-in or refresh and schedules the next renewal
    _applyTokens(tokenResponse) {
        const claims = decodeJwt(tokenResponse.access_token);
        this.session.access_token = tokenResponse.access_token;
        // Refresh tokens may or may not rotate; keep the old one if none came back
        if (tokenResponse.refresh_token) {
            this.session.refresh_token = tokenResponse.refresh_token;
        }
        this.session.expires_at = Date.now() + (tokenResponse.expires_in * 1000);
        this.session.username = claims?.uname ?? this.session.username ?? 'client';
        this._saveSession();
        this.client?.setAccessToken(tokenResponse.access_token);
        this.renewRetryMs = REFRESH_RETRY_MIN_MS;
        this._scheduleRenewal();
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
                        this._applyTokens(tokenResponse);
                        console.log(`Successfully authenticated as user: ${this.session.username}`);
                        resolve();
                    }
                    catch (err) {
                        reject(err);
                    }
                }
            });
            server.listen(this.port, '127.0.0.1', () => {
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
        if (this.session.access_token && Date.now() + REFRESH_LEAD_MS < this.session.expires_at) {
            if (!this.renewTimer)
                this._scheduleRenewal();
            return this.session.access_token;
        }
        return this._renew();
    }
    // Runs one renewal at a time; concurrent callers share it. Throws TokenRefreshError when the
    // failure is temporary and SignInRequiredError when a person has to sign in (unattended mode).
    _renew() {
        if (!this.renewing) {
            this.renewing = this._doRenew().finally(() => {
                this.renewing = null;
            });
        }
        return this.renewing;
    }
    async _doRenew() {
        if (this.session.refresh_token) {
            try {
                console.log('Access token expired or expiring soon. Refreshing token...');
                this._applyTokens(await this._refreshAccessToken(this.session.refresh_token));
                console.log('Access token successfully refreshed.');
                return this.session.access_token;
            }
            catch (err) {
                if (!(err instanceof TokenRefreshError && err.rejected))
                    throw err;
                console.error(`The saved sign-in can no longer be renewed: ${err.message}`);
            }
        }
        if (this.unattended) {
            throw new SignInRequiredError(`${this.session.username ?? 'This client'} needs to sign in again (no valid refresh token in ${this.sessionFile})`);
        }
        await this._startOauthFlow();
        return this.session.access_token;
    }
    // Renews ahead of expiry so the Rust client, which reads the token synchronously, never gets
    // an expired one. With no delay given, renews REFRESH_LEAD_MS before expiry.
    _scheduleRenewal(delayMs) {
        if (this.emulatorMode)
            return;
        if (this.renewTimer)
            clearTimeout(this.renewTimer);
        const remaining = this.session.expires_at - Date.now();
        const delay = delayMs ?? Math.max(0, remaining - Math.min(REFRESH_LEAD_MS, remaining / 2));
        this.renewDueAt = Date.now() + delay;
        this.renewTimer = setTimeout(() => void this._renewInBackground(), Math.min(delay, MAX_TIMER_MS));
        // Don't keep the process alive just for this timer
        this.renewTimer.unref?.();
    }
    async _renewInBackground() {
        if (this.renewTimer)
            clearTimeout(this.renewTimer);
        this.renewTimer = null;
        try {
            await this._renew();
        }
        catch (err) {
            if (err instanceof SignInRequiredError) {
                this._signInRequired(err);
                return;
            }
            const left = Math.round((this.session.expires_at - Date.now()) / 1000);
            console.error(`Token refresh failed: ${err.message}. Retrying in ${this.renewRetryMs / 1000}s; ` +
                (left > 0 ? `the current token expires in ${left}s.` : 'the current token has expired.'));
            this._scheduleRenewal(this.renewRetryMs);
            this.renewRetryMs = Math.min(this.renewRetryMs * 2, REFRESH_RETRY_MAX_MS);
        }
    }
    // The Rust client calls getAccessToken synchronously before requests, so this can't wait for a
    // refresh. If the renewal timer ran late (the machine slept), start the renewal now so the next
    // request gets a fresh token.
    _accessTokenForClient() {
        if (!this.emulatorMode &&
            !this.renewing &&
            this.renewTimer &&
            Date.now() > this.renewDueAt + TIMER_LATE_MS) {
            void this._renewInBackground();
        }
        return this.session.access_token;
    }
    _signInRequired(err) {
        if (this.config.onSignInRequired) {
            this.config.onSignInRequired(err);
            return;
        }
        console.error(`${err.message}.`);
        console.error('Sign in once by hand (a browser opens): run the bot without FIREFLY_UNATTENDED, e.g. `bun start` in its folder.');
        process.exit(exports.EXIT_SIGN_IN_REQUIRED);
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
                            messageType: 0,
                        },
                        channelId: channelId,
                        messageType: 0,
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
                            messageType: 0,
                        },
                        nonce: Math.floor(Math.random() * 9_999_999),
                        messageType: 0,
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
            try {
                await this.getOrRenewAccessToken();
            }
            catch (err) {
                if (err instanceof SignInRequiredError)
                    this._signInRequired(err);
                throw err;
            }
        }
        console.log(`Initializing Firefly MLS Client [user: ${this.session.username}]...`);
        const callbacks = {
            name: this.session.username,
            initialToken: this.session.access_token,
            getAccessToken: () => this._accessTokenForClient(),
            onMessage: async (err, msgJson) => {
                if (err) {
                    console.error("onMessage error:", err);
                    return;
                }
                if (!msgJson)
                    return;
                try {
                    const msg = JSON.parse(msgJson);
                    const other = msg.other;
                    const sentByOther = msg.sentByOther ?? msg.sent_by_other;
                    const message = msg.message;
                    console.log(`[onMessage] msg ${msg.id} received from: ${other}, sentByOther: ${sentByOther}`);
                    if (!sentByOther)
                        return; // skip outgoing
                    const decoded = UserMessageInner.decode(new Uint8Array(message));
                    const text = decoded.messagePayload?.text;
                    if (!text)
                        return;
                    console.log(`[Direct Message] id: ${msg.id}, From: ${other}, length: ${text.length}`);
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
                if (!msgJson)
                    return;
                try {
                    const msg = JSON.parse(msgJson);
                    const by = msg.by;
                    const groupId = msg.groupId ?? msg.group_id;
                    const message = msg.message;
                    const channelId = msg.channelId ?? msg.channel_id;
                    console.log(`[onGroupMessage] msg ${msg.id} received from: ${by}, group: ${groupId}`);
                    if (by === this.session.username)
                        return; // skip outgoing
                    const decoded = GroupMessageInner.decode(new Uint8Array(message));
                    const text = decoded.messagePayload?.text;
                    if (!text)
                        return;
                    console.log(`[Group Message] id: ${msg.id}, Group ID: ${groupId}, By: ${by}, length: ${text.length}`);
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
            onGroupMembersChanged: async (groupId, added, removed) => {
                console.log(`[Group Members Changed] group: ${groupId}, added: ${added.join(', ')}, removed: ${removed.join(', ')}`);
                await this._emitMemberEvents(groupId, added, removed);
            },
            onCallSignal: () => { },
            onGroupMeetingSignal: () => { },
            onReadUserMessagesUpto: () => { },
        };
        this.client = await firefly_client_node_1.FireflyClientNode.create(this.apiBaseUrl, this.wsUrl, 2000, callbacks, this.dbFile, 15000, this.config.storageProviders || this.config.storage);
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
__exportStar(require("./service"), exports);
