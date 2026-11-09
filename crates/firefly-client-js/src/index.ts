import * as fs from 'fs';
import * as path from 'path';
import * as http from 'http';
import * as crypto from 'crypto';
import { exec } from 'child_process';
import {
  FireflyClientNode,
  protos,
  initLogger,
  UserPermission,
  MlsKeyPackageStorage,
  MlsGroupStateStorage,
  MlsPreSharedKeyStorage,
  UserMessageStorage,
  GroupMessageStorage,
  GroupInfoStorage,
  KeyValueStorage,
  SignalStateStorage,
  StorageProviders,
  RawUserMessage,
  RawGroupMessage,
  RawGroupInfo,
  RawFavouriteMessage,
  RawFavoriteMessage,
  RawSearchResultItem,
} from 'firefly-client-node';

export type {
  MlsKeyPackageStorage,
  MlsGroupStateStorage,
  MlsPreSharedKeyStorage,
  UserMessageStorage,
  GroupMessageStorage,
  GroupInfoStorage,
  KeyValueStorage,
  SignalStateStorage,
  StorageProviders,
  RawUserMessage,
  RawGroupMessage,
  RawGroupInfo,
  RawFavouriteMessage,
  RawFavoriteMessage,
  RawSearchResultItem,
};

export { UserPermission, DEFAULT_GROUP_PERMISSIONS } from 'firefly-client-node';

const GroupMessageInner = protos.GroupMessageInner;
const UserMessageInner = protos.UserMessageInner;

// PKCE utilities
function base64url(buffer: Buffer): string {
  return buffer.toString('base64')
    .replace(/=/g, '')
    .replace(/\+/g, '-')
    .replace(/\//g, '_');
}

function generatePkce() {
  const verifier = base64url(crypto.randomBytes(32));
  const challenge = base64url(
    crypto.createHash('sha256').update(verifier).digest()
  );
  return { verifier, challenge };
}

function decodeJwt(token: string): any {
  try {
    const parts = token.split('.');
    if (parts.length < 2) return null;
    const payload = Buffer.from(parts[1], 'base64').toString('utf8');
    return JSON.parse(payload);
  } catch (e) {
    return null;
  }
}

function openBrowser(url: string): void {
  // On Windows `start` takes the first quoted argument as the window title, so pass an empty one
  const start = process.platform === 'darwin' ? 'open' :
                process.platform === 'win32' ? 'start ""' :
                'xdg-open';
  exec(`${start} "${url}"`, (err: any) => {
    if (err) {
      console.log('Failed to open a browser automatically; open the link above manually.');
    }
  });
}

export interface ClientConfig {
  port?: number;
  auth0Domain?: string;
  auth0ClientId?: string;
  auth0Audience?: string;
  apiBaseUrl?: string;
  wsUrl?: string;
  emulatorMode?: boolean;
  username?: string;
  sessionFile?: string;
  dbFile?: string;
  storage?: StorageProviders;
  storageProviders?: StorageProviders;
  // Never open a browser to sign in: when the saved sign-in can't be renewed, call
  // onSignInRequired instead. Defaults to FIREFLY_UNATTENDED=true in the environment.
  unattended?: boolean;
  // Called in unattended mode when the saved sign-in can no longer be renewed. By default the
  // client logs how to sign in again and exits with EXIT_SIGN_IN_REQUIRED.
  onSignInRequired?: (err: SignInRequiredError) => void;
}

// Exit code for "the saved sign-in can't be renewed; a person has to sign in again".
// run-bots.ts stops restarting a bot that exits with it. 78 is EX_CONFIG from sysexits.h.
export const EXIT_SIGN_IN_REQUIRED = 78;

export class SignInRequiredError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SignInRequiredError';
  }
}

// `rejected` means the auth server refused the refresh token, so retrying won't help.
// Otherwise the failure (network, 5xx) is temporary.
class TokenRefreshError extends Error {
  constructor(message: string, public rejected: boolean) {
    super(message);
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

export type BotConfig = ClientConfig;

export interface ClientContext {
  client: FireflyClient;
  sender: string;
  text: string;
  command: string;
  args: string[];
  isGroup: boolean;
  groupId: number | null;
  channelId: number | null;
  reply: (text: string) => Promise<void>;
}

export interface BotContext {
  bot: FireflyClient;
  sender: string;
  text: string;
  command: string;
  args: string[];
  isGroup: boolean;
  groupId: number | null;
  channelId: number | null;
  reply: (text: string) => Promise<void>;
}

export type CommandHandler = (ctx: ClientContext & BotContext) => Promise<void>;

export interface MemberEventContext {
  bot: FireflyClient;
  groupId: number;
  username: string;
  // Posts in the group; channelId defaults to the group's first channel
  sendToGroup: (text: string, channelId?: number) => Promise<void>;
  // Sends a direct message to the member
  sendDirect: (text: string) => Promise<void>;
}

export type MemberEventHandler = (ctx: MemberEventContext) => Promise<void> | void;

export interface GroupMember {
  username: string;
  roleId: number;
  permissions: number;
}

export interface GroupChannel {
  id: number;
  name: string;
  type: number;
}

export class FireflyClient {
  private config: ClientConfig;
  private port: number;
  private auth0Domain: string;
  private auth0ClientId: string;
  private auth0Audience: string;
  private redirectUri: string;

  private apiBaseUrl: string;
  private wsUrl: string;
  public emulatorMode: boolean;
  public unattended: boolean;
  private clientUsername: string;
  private renewing: Promise<string | null> | null = null;
  private renewTimer: ReturnType<typeof setTimeout> | null = null;
  private renewDueAt = 0;
  private renewRetryMs = REFRESH_RETRY_MIN_MS;

  private sessionFile: string;
  private dbFile: string;

  public commands: Map<string, CommandHandler>;
  public messageHandlers: Array<(ctx: ClientContext & BotContext) => Promise<void> | void>;
  public groupMessageHandlers: Array<(ctx: ClientContext & BotContext) => Promise<void> | void>;
  public memberJoinHandlers: MemberEventHandler[];
  public memberLeaveHandlers: MemberEventHandler[];
  public client: any;
  public session: {
    access_token: string | null;
    refresh_token: string | null;
    expires_at: number;
    username: string | null;
  };

  constructor(options: ClientConfig = {}) {
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
  command(name: string, handler: CommandHandler): void {
    const trigger = name.startsWith('/') ? name.toLowerCase() : `/${name.toLowerCase()}`;
    this.commands.set(trigger, handler);
  }

  // Registers a general direct message handler
  onMessage(handler: (ctx: ClientContext & BotContext) => Promise<void> | void): void {
    this.messageHandlers.push(handler);
  }

  // Registers a general group message handler
  onGroupMessage(handler: (ctx: ClientContext & BotContext) => Promise<void> | void): void {
    this.groupMessageHandlers.push(handler);
  }

  // Registers a handler for members joining a group this client is in (never fired for itself)
  onMemberJoin(handler: MemberEventHandler): void {
    this.memberJoinHandlers.push(handler);
  }

  // Registers a handler for members leaving or being removed from a group this client is in
  onMemberLeave(handler: MemberEventHandler): void {
    this.memberLeaveHandlers.push(handler);
  }

  private async _getGroupExtension(groupId: number): Promise<protos.FireflyGroupExtension> {
    if (!this.client) {
      throw new Error('Client not initialized');
    }
    const bytes = await this.client.getGroupExtension(groupId);
    return protos.FireflyGroupExtension.decode(new Uint8Array(bytes));
  }

  // Members of a group with their resolved group-level permissions
  async getGroupMembers(groupId: number): Promise<GroupMember[]> {
    const extension = await this._getGroupExtension(groupId);
    return extension.members.map((member) => ({
      username: member.username,
      roleId: member.role,
      permissions: extension.roles.find((role) => role.id === member.role)?.permissions ?? 0,
    }));
  }

  async getGroupChannels(groupId: number): Promise<GroupChannel[]> {
    const extension = await this._getGroupExtension(groupId);
    return extension.channels.map((channel) => ({ id: channel.id, name: channel.name, type: channel.type }));
  }

  // Whether the user holds the given permission (ManageGroup by default, same as the Lupyd apps' admin check)
  async isGroupAdmin(groupId: number, username: string, permission: number = UserPermission.ManageGroup): Promise<boolean> {
    const member = (await this.getGroupMembers(groupId)).find((m) => m.username === username);
    return member !== undefined && (member.permissions & permission) === permission;
  }

  private async _emitMemberEvents(groupId: number, added: string[], removed: string[]): Promise<void> {
    const self = this.session.username;
    const emit = async (handlers: MemberEventHandler[], username: string) => {
      const ctx: MemberEventContext = {
        bot: this,
        groupId,
        username,
        sendToGroup: async (text: string, channelId?: number) => {
          const target = channelId ?? (await this.getGroupChannels(groupId))[0]?.id ?? 0;
          await this.sendGroupMessage(groupId, text, target);
        },
        sendDirect: async (text: string) => {
          await this.sendUserMessage(username, text);
        },
      };
      for (const handler of handlers) {
        try {
          await handler(ctx);
        } catch (err) {
          console.error('Error executing member event handler:', err);
        }
      }
    };
    for (const username of added) {
      if (username !== self) await emit(this.memberJoinHandlers, username);
    }
    for (const username of removed) {
      if (username !== self) await emit(this.memberLeaveHandlers, username);
    }
  }

  // Fetch members' online status and last connected timestamp
  async getGroupMembersOnlineStatus(groupId: number): Promise<any> {
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
    return protos.GroupMembersOnlineStatus.decode(new Uint8Array(buffer));
  }

  // Send read receipt up to a specific message ID
  async readUserMessagesUpto(other: string, uptoMessageId: bigint | number): Promise<void> {
    if (!this.client) {
      throw new Error('Client not initialized');
    }
    await this.client.readUserMessagesUpto(other, Number(uptoMessageId));
  }

  // Direct 1:1 user message
  async sendUserMessage(to: string, text: string): Promise<any> {
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
  async sendGroupMessage(groupId: number, text: string, channelId: number = 0): Promise<number> {
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
  async createGroup(name: string, description: string = '', settings?: number): Promise<any> {
    if (!this.client) {
      throw new Error('Client not initialized');
    }
    return await this.client.createGroup(name, description, settings);
  }

  // Add/invite member to group
  async inviteMember(groupId: number, username: string, roleId: number = 1): Promise<void> {
    if (!this.client) {
      throw new Error('Client not initialized');
    }
    await this.client.addGroupMember(groupId, username, roleId);
  }

  async addGroupMember(groupId: number, username: string, roleId: number = 1): Promise<void> {
    return this.inviteMember(groupId, username, roleId);
  }

  // Kick member from group
  async kickMember(groupId: number, username: string): Promise<void> {
    if (!this.client) {
      throw new Error('Client not initialized');
    }
    await this.client.kickGroupMember(groupId, username);
  }

  async kickGroupMember(groupId: number, username: string): Promise<void> {
    return this.kickMember(groupId, username);
  }

  // Create join link for group
  async createJoinLink(groupId: number, expiresInSeconds: number = 86400, maxUses: number = 100): Promise<string> {
    if (!this.client) {
      throw new Error('Client not initialized');
    }
    return await this.client.createJoinLink(groupId, expiresInSeconds, maxUses);
  }

  // Join group via link
  async joinViaLink(linkToken: string): Promise<void> {
    if (!this.client) {
      throw new Error('Client not initialized');
    }
    await this.client.joinViaLink(linkToken);
    await this.client.loadAllGroups();
  }

  // Request to join / re-add to group
  async requestToJoin(groupId: number): Promise<void> {
    if (!this.client) {
      throw new Error('Client not initialized');
    }
    await this.client.requestToJoin(groupId);
  }

  async syncGroupJoinsAndReadds(groupId: number): Promise<void> {
    if (!this.client) {
      throw new Error('Client not initialized');
    }
    await this.client.syncGroupJoinsAndReadds(groupId);
  }

  // List group infos
  async getGroups(): Promise<any[]> {
    if (!this.client) {
      throw new Error('Client not initialized');
    }
    return await this.client.getGroupInfos();
  }

  async getGroupInfos(): Promise<any[]> {
    return this.getGroups();
  }

  // Get historical group messages
  async getGroupMessages(groupId: number, startBefore: number = 0, limit: number = 50): Promise<any[]> {
    if (!this.client) {
      throw new Error('Client not initialized');
    }
    return await this.client.getGroupMessages(groupId, startBefore, limit);
  }

  // Query online status
  async getOnlineStatus(usernames: string[]): Promise<string[]> {
    if (!this.client) {
      throw new Error('Client not initialized');
    }
    return await this.client.getOnlineStatus(usernames);
  }

  // Favourite messages APIs
  async addFavourite(favourite: RawFavouriteMessage): Promise<number> {
    if (!this.client) {
      throw new Error('Client not initialized');
    }
    return await this.client.addFavourite(favourite);
  }

  async addFavorite(favorite: RawFavouriteMessage): Promise<number> {
    return await this.addFavourite(favorite);
  }

  async removeUserFavourite(other: string, messageId: number): Promise<boolean> {
    if (!this.client) {
      throw new Error('Client not initialized');
    }
    return await this.client.removeUserFavourite(other, messageId);
  }

  async removeUserFavorite(other: string, messageId: number): Promise<boolean> {
    return await this.removeUserFavourite(other, messageId);
  }

  async removeGroupFavourite(groupId: number, messageId: number): Promise<boolean> {
    if (!this.client) {
      throw new Error('Client not initialized');
    }
    return await this.client.removeGroupFavourite(groupId, messageId);
  }

  async removeGroupFavorite(groupId: number, messageId: number): Promise<boolean> {
    return await this.removeGroupFavourite(groupId, messageId);
  }

  async removeFavouriteById(favouriteId: number): Promise<boolean> {
    if (!this.client) {
      throw new Error('Client not initialized');
    }
    return await this.client.removeFavouriteById(favouriteId);
  }

  async removeFavoriteById(favoriteId: number): Promise<boolean> {
    return await this.removeFavouriteById(favoriteId);
  }

  async isUserFavourite(other: string, messageId: number): Promise<boolean> {
    if (!this.client) {
      throw new Error('Client not initialized');
    }
    return await this.client.isUserFavourite(other, messageId);
  }

  async isUserFavorite(other: string, messageId: number): Promise<boolean> {
    return await this.isUserFavourite(other, messageId);
  }

  async isGroupFavourite(groupId: number, messageId: number): Promise<boolean> {
    if (!this.client) {
      throw new Error('Client not initialized');
    }
    return await this.client.isGroupFavourite(groupId, messageId);
  }

  async isGroupFavorite(groupId: number, messageId: number): Promise<boolean> {
    return await this.isGroupFavourite(groupId, messageId);
  }

  async getFavourites(limit: number = 50, offset: number = 0): Promise<RawFavouriteMessage[]> {
    if (!this.client) {
      throw new Error('Client not initialized');
    }
    return await this.client.getFavourites(limit, offset);
  }

  async getFavorites(limit: number = 50, offset: number = 0): Promise<RawFavouriteMessage[]> {
    return await this.getFavourites(limit, offset);
  }

  async getUserFavourites(other?: string, limit: number = 50, offset: number = 0): Promise<RawFavouriteMessage[]> {
    if (!this.client) {
      throw new Error('Client not initialized');
    }
    return await this.client.getUserFavourites(other, limit, offset);
  }

  async getUserFavorites(other?: string, limit: number = 50, offset: number = 0): Promise<RawFavouriteMessage[]> {
    return await this.getUserFavourites(other, limit, offset);
  }

  async getGroupFavourites(groupId?: number, channelId?: number, limit: number = 50, offset: number = 0): Promise<RawFavouriteMessage[]> {
    if (!this.client) {
      throw new Error('Client not initialized');
    }
    return await this.client.getGroupFavourites(groupId, channelId, limit, offset);
  }

  async getGroupFavorites(groupId?: number, channelId?: number, limit: number = 50, offset: number = 0): Promise<RawFavouriteMessage[]> {
    return await this.getGroupFavourites(groupId, channelId, limit, offset);
  }

  // Dispose client
  async dispose(): Promise<void> {
    if (this.client) {
      await this.client.dispose();
      this.client = null;
    }
  }

  // Load and save session
  private _loadSession(): void {
    if (fs.existsSync(this.sessionFile)) {
      try {
        const content = fs.readFileSync(this.sessionFile, 'utf8');
        this.session = JSON.parse(content);
      } catch (e) {
        console.error('Failed to parse cached session file, starting fresh.');
      }
    }
  }

  private _saveSession(): void {
    // The session holds access/refresh tokens: keep it owner-readable only.
    fs.writeFileSync(this.sessionFile, JSON.stringify(this.session, null, 2), { encoding: 'utf8', mode: 0o600 });
    // `mode` only applies on creation; tighten perms on a pre-existing file too.
    try {
      fs.chmodSync(this.sessionFile, 0o600);
    } catch (e) {
      console.error('Failed to restrict permissions on session file.');
    }
  }

  // Exchange Code
  private async _exchangeCodeForTokens(code: string, codeVerifier: string): Promise<any> {
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
  private async _refreshAccessToken(refreshToken: string): Promise<any> {
    const params = new URLSearchParams({
      grant_type: 'refresh_token',
      client_id: this.auth0ClientId,
      refresh_token: refreshToken,
    });

    let response: Response;
    try {
      response = await fetch(`${this.auth0Domain}/oauth/token`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: params.toString(),
      });
    } catch (err: any) {
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
  private _applyTokens(tokenResponse: any): void {
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
  private _startOauthFlow(): Promise<void> {
    return new Promise((resolve, reject) => {
      const { verifier, challenge } = generatePkce();
      const state = base64url(crypto.randomBytes(16));

      const server = http.createServer(async (req: any, res: any) => {
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
          } catch (err) {
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

        // Always print the link: on a server the browser may not open, and the person signs in
        // from their own computer through an SSH tunnel to this port
        console.log(`Opening browser for Auth0 Sign In. If it doesn't open, open this link (it must reach localhost:${this.port} on this machine):\n\n${authUrl}\n`);
        openBrowser(authUrl);
      });
    });
  }

  // Token management interface
  public async getOrRenewAccessToken(): Promise<string | null> {
    if (this.emulatorMode) {
    if (this.session.access_token && this.client) {
      try {
        this.client.setAccessToken(this.session.access_token);
      } catch (e) {}
    }
    return this.session.access_token;
    }

    if (this.session.access_token && Date.now() + REFRESH_LEAD_MS < this.session.expires_at) {
      if (!this.renewTimer) this._scheduleRenewal();
      return this.session.access_token;
    }
    return this._renew();
  }

  // Runs one renewal at a time; concurrent callers share it. Throws TokenRefreshError when the
  // failure is temporary and SignInRequiredError when a person has to sign in (unattended mode).
  private _renew(): Promise<string | null> {
    if (!this.renewing) {
      this.renewing = this._doRenew().finally(() => {
        this.renewing = null;
      });
    }
    return this.renewing;
  }

  private async _doRenew(): Promise<string | null> {
    if (this.session.refresh_token) {
      try {
        console.log('Access token expired or expiring soon. Refreshing token...');
        this._applyTokens(await this._refreshAccessToken(this.session.refresh_token));
        console.log('Access token successfully refreshed.');
        return this.session.access_token;
      } catch (err: any) {
        if (!(err instanceof TokenRefreshError && err.rejected)) throw err;
        console.error(`The saved sign-in can no longer be renewed: ${err.message}`);
      }
    }
    if (this.unattended) {
      throw new SignInRequiredError(
        `${this.session.username ?? 'This client'} needs to sign in again (no valid refresh token in ${this.sessionFile})`,
      );
    }
    await this._startOauthFlow();
    return this.session.access_token;
  }

  // Renews ahead of expiry so the Rust client, which reads the token synchronously, never gets
  // an expired one. With no delay given, renews REFRESH_LEAD_MS before expiry.
  private _scheduleRenewal(delayMs?: number): void {
    if (this.emulatorMode) return;
    if (this.renewTimer) clearTimeout(this.renewTimer);
    const remaining = this.session.expires_at - Date.now();
    const delay = delayMs ?? Math.max(0, remaining - Math.min(REFRESH_LEAD_MS, remaining / 2));
    this.renewDueAt = Date.now() + delay;
    this.renewTimer = setTimeout(() => void this._renewInBackground(), Math.min(delay, MAX_TIMER_MS));
    // Don't keep the process alive just for this timer
    (this.renewTimer as any).unref?.();
  }

  private async _renewInBackground(): Promise<void> {
    if (this.renewTimer) clearTimeout(this.renewTimer);
    this.renewTimer = null;
    try {
      await this._renew();
    } catch (err: any) {
      if (err instanceof SignInRequiredError) {
        this._signInRequired(err);
        return;
      }
      const left = Math.round((this.session.expires_at - Date.now()) / 1000);
      console.error(
        `Token refresh failed: ${err.message}. Retrying in ${this.renewRetryMs / 1000}s; ` +
        (left > 0 ? `the current token expires in ${left}s.` : 'the current token has expired.'),
      );
      this._scheduleRenewal(this.renewRetryMs);
      this.renewRetryMs = Math.min(this.renewRetryMs * 2, REFRESH_RETRY_MAX_MS);
    }
  }

  // The Rust client calls getAccessToken synchronously before requests, so this can't wait for a
  // refresh. If the renewal timer ran late (the machine slept), start the renewal now so the next
  // request gets a fresh token.
  public _accessTokenForClient(): string | null {
    if (
      !this.emulatorMode &&
      !this.renewing &&
      this.renewTimer &&
      Date.now() > this.renewDueAt + TIMER_LATE_MS
    ) {
      void this._renewInBackground();
    }
    return this.session.access_token;
  }

  private _signInRequired(err: SignInRequiredError): void {
    if (this.config.onSignInRequired) {
      this.config.onSignInRequired(err);
      return;
    }
    console.error(`${err.message}.`);
    console.error('Sign in once by hand (a browser opens): run the bot without FIREFLY_UNATTENDED, e.g. `bun start` in its folder.');
    process.exit(EXIT_SIGN_IN_REQUIRED);
  }

  // Handles parsed message routing to command registry
  public async _handleMessage({ text, sender, isGroup, groupId, channelId }: {
    text: string;
    sender: string;
    isGroup: boolean;
    groupId: number | null;
    channelId: number | null;
  }): Promise<void> {
    if (!text) return;

    let commandName = '';
    let args: string[] = [];
    if (text.startsWith('/')) {
      const parts = text.trim().split(/\s+/);
      commandName = parts[0].toLowerCase();
      args = parts.slice(1);
    }

    const ctx: ClientContext & BotContext = {
      client: this,
      bot: this,
      sender,
      text,
      command: commandName,
      args,
      isGroup,
      groupId,
      channelId,
      reply: async (replyText: string) => {
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
        } else {
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
        } catch (err) {
          console.error('Error executing group message handler:', err);
        }
      }
    } else {
      for (const handler of this.messageHandlers) {
        try {
          await handler(ctx);
        } catch (err) {
          console.error('Error executing message handler:', err);
        }
      }
    }

    if (commandName && this.commands.has(commandName)) {
      const handler = this.commands.get(commandName)!;
      try {
        await handler(ctx);
      } catch (err) {
        console.error(`Error executing command ${commandName}:`, err);
      }
    }
  }

  // Initializes FFI connection
  async start(): Promise<void> {
    this._loadSession();

    if (this.emulatorMode) {
      console.log(`[Emulator Mode] Skipping Auth0 login. Direct name: ${this.clientUsername}`);
      this.session.access_token = this.clientUsername;
      this.session.refresh_token = 'dummy_refresh';
      this.session.expires_at = Date.now() + 1000 * 60 * 60 * 24;
      this.session.username = this.clientUsername;
      this._saveSession();
    } else {
      try {
        await this.getOrRenewAccessToken();
      } catch (err) {
        if (err instanceof SignInRequiredError) this._signInRequired(err);
        throw err;
      }
    }

    console.log(`Initializing Firefly MLS Client [user: ${this.session.username}]...`);

    const callbacks = {
      name: this.session.username,
      initialToken: this.session.access_token,
      
      getAccessToken: () => this._accessTokenForClient(),

      onMessage: async (err: any, msgJson: string) => {
        if (err) {
          console.error("onMessage error:", err);
          return;
        }
        if (!msgJson) return;
        try {
          const msg = JSON.parse(msgJson);
          const other = msg.other;
          const sentByOther = msg.sentByOther ?? msg.sent_by_other;
          const message = msg.message;

          console.log(`[onMessage] msg ${msg.id} received from: ${other}, sentByOther: ${sentByOther}`);
          if (!sentByOther) return; // skip outgoing

          const decoded = UserMessageInner.decode(new Uint8Array(message));
          const text = decoded.messagePayload?.text;
          if (!text) return;

          console.log(`[Direct Message] id: ${msg.id}, From: ${other}, length: ${text.length}`);
          await this._handleMessage({
            text,
            sender: other,
            isGroup: false,
            groupId: null,
            channelId: null,
          });
        } catch (err) {
          console.error('Error handling direct message:', err);
        }
      },

      onGroupMessage: async (err: any, msgJson: string) => {
        if (err) {
          console.error("onGroupMessage error:", err);
          return;
        }
        if (!msgJson) return;
        try {
          const msg = JSON.parse(msgJson);
          const by = msg.by;
          const groupId = msg.groupId ?? msg.group_id;
          const message = msg.message;
          const channelId = msg.channelId ?? msg.channel_id;

          console.log(`[onGroupMessage] msg ${msg.id} received from: ${by}, group: ${groupId}`);
          if (by === this.session.username) return; // skip outgoing

          const decoded = GroupMessageInner.decode(new Uint8Array(message));
          const text = decoded.messagePayload?.text;
          if (!text) return;

          console.log(`[Group Message] id: ${msg.id}, Group ID: ${groupId}, By: ${by}, length: ${text.length}`);
          await this._handleMessage({
            text,
            sender: by,
            isGroup: true,
            groupId: groupId,
            channelId: channelId,
          });
        } catch (err) {
          console.error('Error handling group message:', err);
        }
      },

      onGroupJoined: async (groupId: number) => {
        console.log(`[Group Joined] Client joined group: ${groupId}`);
        try {
          await this.client.loadAllGroups();
        } catch (err) {
          console.error('Failed to load groups on join:', err);
        }
      },

      onGroupMembersChanged: async (groupId: number, added: string[], removed: string[]) => {
        console.log(`[Group Members Changed] group: ${groupId}, added: ${added.join(', ')}, removed: ${removed.join(', ')}`);
        await this._emitMemberEvents(groupId, added, removed);
      },

      onCallSignal: () => {},
      onGroupMeetingSignal: () => {},
      onReadUserMessagesUpto: () => {},
    };

    this.client = await FireflyClientNode.create(
      this.apiBaseUrl,
      this.wsUrl,
      2000,
      callbacks,
      this.dbFile,
      15000,
      this.config.storageProviders || this.config.storage
    );

    console.log('Connecting to Firefly MLS network...');
    try {
      console.log('Running checkSetup()...');
      await this.client.checkSetup();
      console.log('checkSetup() completed successfully!');

      // Run initializeWithRetrying in the background without awaiting it
      this.client.initializeWithRetrying().catch((err: any) => {
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
    } catch (err) {
      console.error('Error starting connection:', err);
    }
  }
}

const FireflyBot = FireflyClient;
export { FireflyBot, initLogger };
export { FireflyClientNode, protos };
export * from "./service";

