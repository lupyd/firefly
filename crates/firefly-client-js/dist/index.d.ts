import { FireflyClientNode, protos, initLogger } from 'firefly-client-node';
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
}
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
export declare class FireflyClient {
    private port;
    private auth0Domain;
    private auth0ClientId;
    private auth0Audience;
    private redirectUri;
    private apiBaseUrl;
    private wsUrl;
    emulatorMode: boolean;
    private clientUsername;
    private sessionFile;
    private dbFile;
    commands: Map<string, CommandHandler>;
    client: any;
    session: {
        access_token: string | null;
        refresh_token: string | null;
        expires_at: number;
        username: string | null;
    };
    constructor(options?: ClientConfig);
    command(name: string, handler: CommandHandler): void;
    getGroupMembersOnlineStatus(groupId: number): Promise<any>;
    readUserMessagesUpto(other: string, uptoMessageId: bigint | number): Promise<void>;
    private _loadSession;
    private _saveSession;
    private _exchangeCodeForTokens;
    private _refreshAccessToken;
    private _startOauthFlow;
    getOrRenewAccessToken(): Promise<string | null>;
    _handleMessage({ text, sender, isGroup, groupId, channelId }: {
        text: string;
        sender: string;
        isGroup: boolean;
        groupId: number | null;
        channelId: number | null;
    }): Promise<void>;
    start(): Promise<void>;
}
declare const FireflyBot: typeof FireflyClient;
export { FireflyBot, initLogger };
export { FireflyClientNode, protos };
