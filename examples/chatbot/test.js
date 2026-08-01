const test = require('node:test');
const assert = require('node:assert');
const crypto = require('crypto');
const path = require('path');
const protobuf = require('protobufjs');

// Load protobuf schemas dynamically
const protoPath = path.join(__dirname, 'message.proto');
const root = protobuf.loadSync(protoPath);
const GroupMessageInner = root.lookupType('firefly.GroupMessageInner');

// 1. Mock functions & utilities from index.js to test directly
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

// Mocking chatbot command routing
function parseCommand(text) {
  if (!text || !text.startsWith('/')) return null;
  const parts = text.split(' ');
  return parts[0].toLowerCase();
}

// ─── Tests ───────────────────────────────────────────────────────────────────

test('decodeJwt - decodes valid JWT payload', () => {
  const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64');
  const claims = { uname: 'test_user', perms: 5, exp: 999999999 };
  const payload = Buffer.from(JSON.stringify(claims)).toString('base64');
  const token = `${header}.${payload}.signature`;

  const decoded = decodeJwt(token);
  assert.notEqual(decoded, null);
  assert.strictEqual(decoded.uname, 'test_user');
  assert.strictEqual(decoded.perms, 5);
});

test('decodeJwt - returns null on invalid format', () => {
  assert.strictEqual(decodeJwt('invalid-token'), null);
});

test('generatePkce - produces valid verifier and challenge base64url', () => {
  const { verifier, challenge } = generatePkce();

  assert.strictEqual(typeof verifier, 'string');
  assert.strictEqual(typeof challenge, 'string');
  assert.match(verifier, /^[a-zA-Z0-9_-]+$/);
  assert.match(challenge, /^[a-zA-Z0-9_-]+$/);
});

test('Protobuf encoding/decoding of group messages', () => {
  const channelId = 5;
  const textMessage = 'Hello Firefly MLS!';

  // Encode using protobufjs
  const payload = {
    messagePayload: {
      text: textMessage,
      files: null,
      replyingTo: 0,
    },
    channelId,
  };

  const messageInnerBytes = GroupMessageInner.encode(GroupMessageInner.create(payload)).finish();

  assert.ok(messageInnerBytes instanceof Uint8Array);
  assert.ok(messageInnerBytes.length > 0);

  // Decode and assert properties
  const decoded = GroupMessageInner.decode(messageInnerBytes);
  assert.strictEqual(decoded.channelId, channelId);
  assert.strictEqual(decoded.messagePayload?.text, textMessage);
});

test('Command parsing and routing', () => {
  assert.strictEqual(parseCommand('/hi guest'), '/hi');
  assert.strictEqual(parseCommand('/JOKE'), '/joke');
  assert.strictEqual(parseCommand('/help'), '/help');
  assert.strictEqual(parseCommand('hello there'), null);
});
