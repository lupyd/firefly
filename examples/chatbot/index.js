const fs = require('fs');
const path = require('path');
const http = require('http');
const crypto = require('crypto');
const { exec } = require('child_process');
const readline = require('readline');
const protobuf = require('protobufjs');
const { FireflyClientNode } = require('firefly-client-node');

// Load protobuf schemas dynamically
const protoPath = path.join(__dirname, 'message.proto');
const root = protobuf.loadSync(protoPath);
const GroupMessageInner = root.lookupType('firefly.GroupMessageInner');

// Configuration Defaults
const PORT = 38295;
const AUTH0_DOMAIN = process.env.AUTH0_DOMAIN || 'https://auth.lupyd.com';
const AUTH0_CLIENT_ID = process.env.AUTH0_CLIENT_ID || 'GnfEyGY0JdD0Oige2HSpeErcaWLrvObm';
const AUTH0_AUDIENCE = process.env.AUTH0_AUDIENCE || 'https://lupyd.com';
const REDIRECT_URI = `http://localhost:${PORT}/callback`;

const FIREFLY_BASE_URL = process.env.FIREFLY_BASE_URL || 'https://firefly.lupyd.com';
const FIREFLY_BASE_WS_URL = process.env.FIREFLY_BASE_WS_URL || 'wss://firefly.lupyd.com/';
const EMULATOR_MODE = process.env.EMULATOR_MODE === 'true';

const SESSION_FILE = path.join(__dirname, 'bot-session.json');
const DB_FILE = path.join(__dirname, 'bot-store.db');

// Global state holding current tokens
let session = {
  access_token: null,
  refresh_token: null,
  expires_at: 0,
  username: null,
};

// JWT Decoding Utility
function decodeJwt(token) {
  try {
    const parts = token.split('.');
    if (parts.length < 2) return null;
    const payload = Buffer.from(parts[1], 'base64').toString('utf8');
    return JSON.parse(payload);
  } catch (e) {
    return null;
  }
}

// PKCE helper utilities
function base64url(buffer) {
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

function openBrowser(url) {
  const start = process.platform === 'darwin' ? 'open' :
                process.platform === 'win32' ? 'start' :
                'xdg-open';
  exec(`${start} "${url}"`, (err) => {
    if (err) {
      console.log(`Failed to open browser automatically. Please open this link manually:\n\n${url}\n`);
    }
  });
}

// Auth0 Token Exchange
async function exchangeCodeForTokens(code, codeVerifier) {
  const params = new URLSearchParams({
    grant_type: 'authorization_code',
    client_id: AUTH0_CLIENT_ID,
    code_verifier: codeVerifier,
    code: code,
    redirect_uri: REDIRECT_URI,
  });

  const response = await fetch(`${AUTH0_DOMAIN}/oauth/token`, {
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

// Auth0 Token Refresh
async function refreshAccessToken(refreshToken) {
  const params = new URLSearchParams({
    grant_type: 'refresh_token',
    client_id: AUTH0_CLIENT_ID,
    refresh_token: refreshToken,
  });

  const response = await fetch(`${AUTH0_DOMAIN}/oauth/token`, {
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

// Save & load session helper
function saveSession() {
  fs.writeFileSync(SESSION_FILE, JSON.stringify(session, null, 2), 'utf8');
}

function loadSession() {
  if (fs.existsSync(SESSION_FILE)) {
    try {
      const content = fs.readFileSync(SESSION_FILE, 'utf8');
      session = JSON.parse(content);
    } catch (e) {
      console.error('Failed to parse cached session file, starting fresh.');
    }
  }
}

// Interactive CLI/Browser Login Flow
function startOauthFlow() {
  return new Promise((resolve, reject) => {
    const { verifier, challenge } = generatePkce();
    const state = base64url(crypto.randomBytes(16));

    const server = http.createServer(async (req, res) => {
      const reqUrl = new URL(req.url, `http://localhost:${PORT}`);
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
          const tokenResponse = await exchangeCodeForTokens(code, verifier);
          const claims = decodeJwt(tokenResponse.access_token);

          session.access_token = tokenResponse.access_token;
          session.refresh_token = tokenResponse.refresh_token;
          session.expires_at = Date.now() + (tokenResponse.expires_in * 1000);
          session.username = claims ? claims.uname : 'bot';

          saveSession();
          console.log(`Successfully authenticated as user: ${session.username}`);
          resolve();
        } catch (err) {
          reject(err);
        }
      }
    });

    server.listen(PORT, () => {
      const authUrl = `${AUTH0_DOMAIN}/authorize?` + new URLSearchParams({
        client_id: AUTH0_CLIENT_ID,
        audience: AUTH0_AUDIENCE,
        response_type: 'code',
        scope: 'openid profile email offline_access',
        redirect_uri: REDIRECT_URI,
        code_challenge_method: 'S256',
        code_challenge: challenge,
        state: state,
      }).toString();

      console.log('Opening browser for Auth0 Sign In...');
      openBrowser(authUrl);
    });
  });
}

// Bypassed login logic for Emulator Mode
async function runEmulatorLogin() {
  const username = process.env.BOT_USERNAME || 'example_bot';
  console.log(`[Emulator Mode] Skipping Auth0. Logging in directly as: ${username}`);
  
  session.access_token = username;
  session.refresh_token = 'dummy_refresh';
  session.expires_at = Date.now() + 1000 * 60 * 60 * 24; // 24 hours
  session.username = username;
  
  saveSession();
}

// Token validation & renewer loop
async function getOrRenewAccessToken() {
  if (EMULATOR_MODE) {
    return session.access_token;
  }

  // Refresh if expired or expiring within the next 2 minutes
  if (!session.access_token || Date.now() + 120000 >= session.expires_at) {
    if (session.refresh_token) {
      console.log('Access token expired or expiring soon. Refreshing token...');
      try {
        const refreshResponse = await refreshAccessToken(session.refresh_token);
        const claims = decodeJwt(refreshResponse.access_token);

        session.access_token = refreshResponse.access_token;
        if (refreshResponse.refresh_token) {
          session.refresh_token = refreshResponse.refresh_token;
        }
        session.expires_at = Date.now() + (refreshResponse.expires_in * 1000);
        session.username = claims ? claims.uname : 'bot';
        
        saveSession();
        console.log('Access token successfully refreshed.');
      } catch (err) {
        console.error('Failed to refresh token, starting new login flow:', err.message);
        await startOauthFlow();
      }
    } else {
      await startOauthFlow();
    }
  }

  return session.access_token;
}

// Send Group Message helper
async function sendTextToGroup(client, groupId, channelId, text) {
  try {
    const payload = {
      messagePayload: {
        text: text,
        files: null,
        replyingTo: 0,
      },
      channelId: channelId,
    };

    const err = GroupMessageInner.verify(payload);
    if (err) throw new Error(err);

    const messageInnerBytes = GroupMessageInner.encode(GroupMessageInner.create(payload)).finish();

    await client.encryptAndSendGroup(groupId, Array.from(messageInnerBytes));
  } catch (err) {
    console.error(`Failed to send message to group ${groupId}:`, err);
  }
}

// Fetch a joke from public API
async function fetchDadJoke() {
  try {
    const response = await fetch('https://icanhazdadjoke.com/', {
      headers: { 'Accept': 'application/json' }
    });
    if (response.ok) {
      const data = await response.json();
      return data.joke;
    }
  } catch (e) {
    console.error('Error fetching dad joke:', e.message);
  }
  return "I would tell you a joke about UDP, but you might not get it.";
}

// Bot logic loop bootstrap
async function main() {
  loadSession();

  if (EMULATOR_MODE) {
    await runEmulatorLogin();
  } else {
    try {
      await getOrRenewAccessToken();
    } catch (err) {
      console.error('Login failed:', err);
      process.exit(1);
    }
  }

  console.log(`Starting Firefly MLS bot: ${session.username}...`);

  // Create callbacks definition
  const callbacks = {
    name: session.username,
    
    getAccessToken: async () => {
      return getOrRenewAccessToken();
    },

    onMessage: (msg) => {
      console.log(`[Direct Message] From: ${msg.other}, payload size: ${msg.message.length} bytes`);
    },

    onGroupMessage: async (msg) => {
      try {
        const decoded = GroupMessageInner.decode(new Uint8Array(msg.message));
        const text = decoded.messagePayload?.text;
        
        if (!text) return;

        console.log(`[Group Message] Group ID: ${msg.groupId}, By: ${msg.by}, Content: "${text}"`);

        // Skip responding to our own messages to avoid infinite loops
        if (msg.by === session.username) {
          return;
        }

        // Routing commands
        if (text.startsWith('/')) {
          const parts = text.split(' ');
          const command = parts[0].toLowerCase();

          if (command === '/hi') {
            const greeting = `Hello, @${msg.by}! I am the Firefly MLS Example Chatbot. Nice to meet you!`;
            await sendTextToGroup(botClient, msg.groupId, msg.channelId, greeting);
          } else if (command === '/joke') {
            const joke = await fetchDadJoke();
            await sendTextToGroup(botClient, msg.groupId, msg.channelId, joke);
          } else if (command === '/help') {
            const helpText = 
              `Available Commands:\n` +
              `  /hi   - Greet the bot\n` +
              `  /joke - Get a funny dad joke\n` +
              `  /help - Show this message`;
            await sendTextToGroup(botClient, msg.groupId, msg.channelId, helpText);
          }
        }
      } catch (err) {
        console.error('Error handling group message:', err);
      }
    },

    onGroupJoined: async (groupId) => {
      console.log(`[Group Joined] Bot successfully joined group: ${groupId}`);
      try {
        // Automatically load groups to initialize encryption keys
        await botClient.loadAllGroups();
      } catch (err) {
        console.error('Error loading groups upon join:', err);
      }
    },

    onCallSignal: (sig) => {
      console.log(`[Call Signal] Call ID: ${sig.callId}, sender: ${sig.senderUsername}`);
    },

    onGroupMeetingSignal: (sig) => {
      console.log(`[Meeting Signal] Group: ${sig.groupId}, user: ${sig.username}`);
    }
  };

  // Create bot client
  const botClient = FireflyClientNode.create(
    FIREFLY_BASE_URL,
    FIREFLY_BASE_WS_URL,
    2000, // Retry interval: 2 seconds
    callbacks,
    DB_FILE,
    15000 // Request timeout: 15 seconds
  );

  // Initialize and run the socket connection loop
  console.log('Connecting to Firefly MLS network...');
  try {
    await botClient.initializeWithRetrying();
    await botClient.loadAllGroups();
    console.log('Bot is fully connected and listening for messages.');
  } catch (err) {
    console.error('Error initializing client connection:', err);
  }
}

main().catch((err) => {
  console.error('Fatal bot error:', err);
});
