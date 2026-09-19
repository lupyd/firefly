# Group message permissions

| Permission | Bit | Required for |
| --- | --- | --- |
| `SeeMessage` | `1` | Reading/receiving messages and cached history |
| `PinMessage` | `2` | Sending pinned messages, including pinned re-encryption |
| `AddMessage` | `4` (unchanged) | Sending messages |

Sending requires `SeeMessage | AddMessage`; sending a pin additionally requires
`PinMessage`. Reading a pin needs only `SeeMessage`, not `PinMessage`. The pinned
message-type flag (`MESSAGE_TYPE_PINNED = 1`) is separate from permission masks.
Both the outer and nested message-type flags are checked.

New groups created through the native/WASM clients give the default role
`SeeMessage | AddMessage` (`5`). They do **not** grant `PinMessage`. Explicit
permissions passed to the low-level core group constructor remain unchanged.
Existing groups are not migrated or implicitly granted permissions: the new bits
are enforced immediately, including masks of `0` or legacy `AddMessage`-only `4`.
An authorized administrator must explicitly grant the desired bits using the
existing role/channel update APIs. Existing bit values and protobuf/MLS formats
are unchanged; no server/database migration is required.

## Resolution and enforcement

For an existing channel, an explicit role override replaces the channel defaults;
otherwise the channel defaults apply. This matches the existing client channel
rules (group-level management rights do not imply channel read/pin access).
Channel `0`, if not explicitly defined, uses the group role/default permissions
for the legacy group-wide stream. Unknown nonzero channels and undefined roles
fail closed. Default-role members may be absent from the serialized extension;
the authenticated MLS roster establishes membership.

The shared `FireflyMlsRules` helpers run before encryption and after authenticated
MLS decryption in `FireflyMlsGroup`. Receivers validate the actual MLS sender's
permissions independently of the server, as well as the local reader's access.
Unauthorized plaintext is not returned, stored, or delivered through callbacks.
MLS commits continue to be processed while reads are denied. Rejected application
messages still consume/persist their ratchet state; native synchronization advances
its cursor without retaining plaintext.

Client-facing native history stores and WASM history/pinned-history getters
recheck current read permissions, including after revocation. Internal raw storage
providers are trusted persistence primitives, not authorization APIs.
Direct 1:1 messages are unaffected: these are group/channel role permissions.

## Security boundary

This is client policy enforcement, not per-channel cryptographic separation.
An old or modified client possessing the group's MLS keys can bypass its own
read checks; plaintext already seen cannot be recalled. Updated clients reject
unauthorized sends/pins from such peers. Preventing malicious group members from
decrypting a channel requires separate channel encryption groups/keys, which
would be a protocol change beyond these backwards-compatible permission bits.

## Verification

Start PostgreSQL with `docker compose up -d` in `../firefly-mls`. Run that server in
emulator mode on port 39209 (`SUBSCRIPTIONS_AUTHORIZATION=''` enables its built-in
subscription fallback; keep `RESET_DB=false` when preserving an existing DB).

```sh
cargo check
cargo check -p firefly-client-node --target wasm32-unknown-unknown
EMULATOR_MODE=true RUST_LOG=info FIREFLY_BASE_URL=http://127.0.0.1:39209 cargo test
```

`message_permissions.rs` exhaustively tests masks, both pin flags, channels, raw
legacy payloads and numeric/wire compatibility. `group_client.rs` tests unchecked
MLS peers bypassing sender-side checks. `roles_and_permissions_test.rs` exercises
live grants/revocation, callbacks, cached history, default permissions and role
management boundaries against a server without message-level enforcement.

The Node and high-level JS packages export `UserPermission` and
`DEFAULT_GROUP_PERMISSIONS`. The Node message codecs accept an optional
`messageType` (default `0`), preserving older callers while retaining both pin
flags. `getGroupExtension()` returns the current MLS extension bytes for UI rules.

WASM runtime coverage (after building the package):

```sh
cd crates/client-node
wasm-pack build --dev --target nodejs --out-dir wasm
./node_modules/.bin/tsc
FIREFLY_BASE_URL=http://127.0.0.1:39209 bun test ./tests/message-permissions.test.cjs
```
