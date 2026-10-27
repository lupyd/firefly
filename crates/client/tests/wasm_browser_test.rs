//! Runs the shared client storage and Signal paths inside a browser dedicated worker, where the
//! database lives in OPFS. Needs chromedriver and a `wasm-bindgen-test-runner` matching the
//! locked wasm-bindgen version (wasm-pack overrides RUSTFLAGS, dropping `mls_build_async`):
//! `CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
//!  cargo test -p firefly-client --target wasm32-unknown-unknown --test wasm_browser_test`
#![cfg(target_arch = "wasm32")]

use firefly_client::{
    FfiFireflyWsClient,
    callbacks::FireflyWsClientCallback,
    storage::{GroupMessage, UserMessage as StoredUserMessage},
    websocket::ConnectionState,
};
use firefly_client::db::{
    ffi_stores::FfiKeyStores,
    keyvalue::KeyValueStore,
    messages::{MessagesStore, UserMessage},
    search::SearchEngine,
    setup_pool_from_path,
};
use firefly_protos::{
    firefly::{self, MessagePayload, mod_UserMessageInner},
    serialize_proto,
};
use libsignal_protocol::{DeviceId, ProtocolAddress};
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_dedicated_worker);

fn unique(name: &str) -> String {
    format!("{}-{}.db", name, js_sys::Date::now() as u64)
}

fn user_message(id: u64, other: &str, text: &str) -> UserMessage {
    let inner = firefly::UserMessageInner {
        nonce: 0,
        message: mod_UserMessageInner::OneOfmessage::messagePayload(MessagePayload {
            text: text.into(),
            files: None,
            ext: firefly::mod_MessagePayload::OneOfext::None,
            message_type: 0,
        }),
        message_type: 0,
    };
    UserMessage {
        id,
        other: other.to_string(),
        message: serialize_proto(&inner).unwrap().to_vec(),
        sent_by_other: true,
        message_type: 0,
    }
}

#[wasm_bindgen_test]
async fn stores_persist_across_reopen_with_fts_search() {
    let name = unique("persist");
    {
        let pool = setup_pool_from_path(&name, 1).await.unwrap();
        let kv = KeyValueStore::new(pool.clone()).await.unwrap();
        kv.set("greeting", "hello from opfs").await.unwrap();

        let messages = MessagesStore::new(pool.clone()).await.unwrap();
        messages
            .insert_user_message(user_message(7, "alice", "the launch plan is confidential"))
            .await
            .unwrap();
    }

    // A fresh pool reads back what the first one wrote, migrations are idempotent.
    let pool = setup_pool_from_path(&name, 1).await.unwrap();
    let kv = KeyValueStore::new(pool.clone()).await.unwrap();
    assert_eq!(kv.get("greeting").await.unwrap(), "hello from opfs");

    let messages = MessagesStore::new(pool.clone()).await.unwrap();
    let last = messages.get_last_messages_of("alice", i64::MAX, 10).await.unwrap();
    assert_eq!(last.len(), 1);
    assert_eq!(last[0].id, 7);
    assert!(last[0].sent_by_other);

    let hits = SearchEngine::new(pool)
        .search_user("confidential", Some("alice"), 10, 0)
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].message_id, 7);
}

#[wasm_bindgen_test]
async fn signal_sessions_encrypt_and_decrypt() {
    let alice = FfiKeyStores::new(setup_pool_from_path(&unique("alice"), 1).await.unwrap())
        .await
        .unwrap();
    let bob = FfiKeyStores::new(setup_pool_from_path(&unique("bob"), 1).await.unwrap())
        .await
        .unwrap();

    let device = |stores: &FfiKeyStores| {
        let identity = stores.store().identity_store.clone();
        async move { identity.get_full_identity_key_pair().await.unwrap().device_id }
    };
    let bob_address = ProtocolAddress::new("bob".into(), DeviceId::new(device(&bob).await).unwrap());
    let alice_address =
        ProtocolAddress::new("alice".into(), DeviceId::new(device(&alice).await).unwrap());

    let bundle = bob.generate_prekey_bundle().await.unwrap();
    alice.process_pre_key_bundle("bob".into(), bundle).await.unwrap();

    let first = alice.encrypt(bob_address.clone(), b"Hello Bob".to_vec()).await.unwrap();
    let plain = bob.decrypt(alice_address.clone(), first.cipher_text, first.ty).await.unwrap();
    assert_eq!(plain, b"Hello Bob");

    let reply = bob.encrypt(alice_address, b"Hi Alice".to_vec()).await.unwrap();
    let plain = alice.decrypt(bob_address, reply.cipher_text, reply.ty).await.unwrap();
    assert_eq!(plain, b"Hi Alice");
}

struct NoopCallbacks;

#[async_trait::async_trait]
impl FireflyWsClientCallback for NoopCallbacks {
    fn name(&self) -> &str {
        "wasm-test"
    }

    async fn get_access_token(&self) -> Option<String> {
        None
    }

    async fn on_message(&self, _message: StoredUserMessage) {}

    async fn on_group_message(&self, _group_message: GroupMessage) {}
}

#[wasm_bindgen_test]
async fn client_constructs_on_opfs() {
    let client = FfiFireflyWsClient::create_with_endpoints(
        "https://firefly.invalid".into(),
        "wss://firefly.invalid".into(),
        None,
        1000,
        Box::new(NoopCallbacks),
        unique("client"),
        10_000,
    )
    .await
    .unwrap();
    assert!(matches!(client.get_connection_state(), ConnectionState::Disconnected));
    client.dispose().await;
}
