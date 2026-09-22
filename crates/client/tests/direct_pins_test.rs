use firefly_client::{
    db::{messages::{MessagesStore, UserMessage}, setup_pool},
    direct_pins::{apply_records, open, pin_content_fingerprint, seal},
};
use firefly_protos::{
    firefly::{self, mod_UserMessageInner, MessagePayload},
    serialize_proto, MESSAGE_TYPE_NORMAL, MESSAGE_TYPE_PINNED,
};
use rand::RngCore;
use sha2::Digest;

fn make_msg_payload(text: &str, nonce: u32, msg_type: u32) -> Vec<u8> {
    let inner = firefly::UserMessageInner {
        message: mod_UserMessageInner::OneOfmessage::messagePayload(MessagePayload {
            text: text.into(),
            files: None,
            ext: firefly::mod_MessagePayload::OneOfext::None,
            message_type: msg_type,
        }),
        nonce,
        message_type: msg_type,
    };
    serialize_proto(&inner).expect("serialize proto").to_vec()
}

#[test]
fn test_pin_content_fingerprint_invariance() {
    let nonce = 987654321;
    let payload_alice = make_msg_payload("Secret message", nonce, MESSAGE_TYPE_NORMAL);
    let payload_bob = make_msg_payload("Secret message", nonce, MESSAGE_TYPE_PINNED);

    let alice_msg = UserMessage {
        id: 1001,
        other: "bob".into(),
        message: payload_alice,
        sent_by_other: false,
        message_type: MESSAGE_TYPE_NORMAL,
    };

    let bob_msg = UserMessage {
        id: 5555,
        other: "alice".into(),
        message: payload_bob,
        sent_by_other: true,
        message_type: MESSAGE_TYPE_PINNED,
    };

    // Both must yield the exact same fingerprint despite differing IDs, direction, other, and mutable pin flag
    assert_eq!(
        pin_content_fingerprint(&alice_msg.message),
        pin_content_fingerprint(&bob_msg.message)
    );

    // Different message must yield a different fingerprint
    let different_payload = make_msg_payload("Other message", nonce, 0);
    assert_ne!(
        pin_content_fingerprint(&alice_msg.message),
        pin_content_fingerprint(&different_payload)
    );
}

#[test]
fn test_seal_and_open_compression_and_encryption() {
    let mut messages = Vec::new();
    for i in 0..10 {
        messages.push(UserMessage {
            id: 1000 + i,
            other: "bob".into(),
            message: make_msg_payload(&format!("Pinned message {i}"), 100 + i as u32, MESSAGE_TYPE_PINNED),
            sent_by_other: i % 2 == 0,
            message_type: MESSAGE_TYPE_PINNED,
        });
    }

    let mut key = [0u8; 32];
    rand::rng().fill_bytes(&mut key);
    let snapshot_id = "0123456789abcdef0123456789abcdef";

    let ciphertext = seal("bob", snapshot_id, &key, &messages).expect("seal snapshot");
    assert!(ciphertext.len() > 12);

    let secret = firefly::DirectPinSnapshotSecret {
        version: 1,
        other: "bob".into(),
        snapshot_id: snapshot_id.into(),
        key: key.to_vec().into(),
        ciphertext_hash: sha2::Sha256::digest(&ciphertext).to_vec().into(),
        message_count: messages.len() as u32,
    };

    let decrypted = open(&secret, &ciphertext).expect("open snapshot");
    assert_eq!(decrypted.len(), 10);
    for (orig, dec) in messages.iter().zip(decrypted.iter()) {
        assert_eq!(
            pin_content_fingerprint(&orig.message),
            pin_content_fingerprint(&dec.message)
        );
        assert_eq!(dec.message_type & MESSAGE_TYPE_PINNED, MESSAGE_TYPE_PINNED);
    }

    // Tampered ciphertext must fail
    let mut bad_blob = ciphertext.clone();
    bad_blob[15] ^= 0xFF;
    assert!(open(&secret, &bad_blob).is_err());

    // Wrong key must fail
    let mut wrong_key = key;
    wrong_key[0] ^= 1;
    let wrong_secret = firefly::DirectPinSnapshotSecret {
        key: wrong_key.to_vec().into(),
        ..secret.clone()
    };
    assert!(open(&wrong_secret, &ciphertext).is_err());
}

#[tokio::test]
async fn test_direct_pin_sync_between_participants_and_devices() -> anyhow::Result<()> {
    let alice_pool = setup_pool("sqlite::memory:", 1).await?;
    let alice_store = MessagesStore::new(alice_pool).await?;

    let bob_pool = setup_pool("sqlite::memory:", 1).await?;
    let bob_store = MessagesStore::new(bob_pool).await?;

    let alice_device2_pool = setup_pool("sqlite::memory:", 1).await?;
    let alice_device2_store = MessagesStore::new(alice_device2_pool).await?;

    // Seed 3 messages in conversation
    // Msg 1: Alice -> Bob
    let p1 = make_msg_payload("Message 1", 111, 0);
    alice_store.insert_user_message(UserMessage {
        id: 101,
        other: "bob".into(),
        message: p1.clone(),
        sent_by_other: false,
        message_type: 0,
    }).await?;
    bob_store.insert_user_message(UserMessage {
        id: 201,
        other: "alice".into(),
        message: p1.clone(),
        sent_by_other: true,
        message_type: 0,
    }).await?;
    alice_device2_store.insert_user_message(UserMessage {
        id: 301,
        other: "bob".into(),
        message: p1.clone(),
        sent_by_other: false,
        message_type: 0,
    }).await?;

    // Msg 2: Bob -> Alice
    let p2 = make_msg_payload("Message 2", 222, 0);
    alice_store.insert_user_message(UserMessage {
        id: 102,
        other: "bob".into(),
        message: p2.clone(),
        sent_by_other: true,
        message_type: 0,
    }).await?;
    bob_store.insert_user_message(UserMessage {
        id: 202,
        other: "alice".into(),
        message: p2.clone(),
        sent_by_other: false,
        message_type: 0,
    }).await?;
    alice_device2_store.insert_user_message(UserMessage {
        id: 302,
        other: "bob".into(),
        message: p2.clone(),
        sent_by_other: true,
        message_type: 0,
    }).await?;

    // Msg 3: Alice -> Bob
    let p3 = make_msg_payload("Message 3", 333, 0);
    alice_store.insert_user_message(UserMessage {
        id: 103,
        other: "bob".into(),
        message: p3.clone(),
        sent_by_other: false,
        message_type: 0,
    }).await?;
    bob_store.insert_user_message(UserMessage {
        id: 203,
        other: "alice".into(),
        message: p3.clone(),
        sent_by_other: true,
        message_type: 0,
    }).await?;
    alice_device2_store.insert_user_message(UserMessage {
        id: 303,
        other: "bob".into(),
        message: p3.clone(),
        sent_by_other: false,
        message_type: 0,
    }).await?;

    // Alice pins Msg 1 and Msg 2
    alice_store.update_message_type("bob", 101, MESSAGE_TYPE_PINNED).await?;
    alice_store.update_message_type("bob", 102, MESSAGE_TYPE_PINNED).await?;

    let alice_pins = alice_store.get_pinned_messages_of("bob").await?;
    assert_eq!(alice_pins.len(), 2);

    let mut key = [0u8; 32];
    rand::rng().fill_bytes(&mut key);
    let snapshot_id = "11112222333344445555666677778888";

    // Compress and encrypt pinned messages altogether
    let ciphertext = seal("bob", snapshot_id, &key, &alice_pins)?;

    let secret = firefly::DirectPinSnapshotSecret {
        version: 1,
        other: "bob".into(),
        snapshot_id: snapshot_id.into(),
        key: key.to_vec().into(),
        ciphertext_hash: sha2::Sha256::digest(&ciphertext).to_vec().into(),
        message_count: 2,
    };

    // Bob receives only the encryption key capability (secret), opens the snapshot, and applies it
    let bob_decrypted = open(&secret, &ciphertext)?;
    apply_records(&bob_store, "alice", &bob_decrypted).await?;

    // Verify Bob's store now has Msg 1 (id 201) and Msg 2 (id 202) pinned, Msg 3 (id 203) unpinned
    let bob_pins = bob_store.get_pinned_messages_of("alice").await?;
    assert_eq!(bob_pins.len(), 2);
    let bob_pinned_ids: Vec<u64> = bob_pins.iter().map(|m| m.id).collect();
    assert!(bob_pinned_ids.contains(&201));
    assert!(bob_pinned_ids.contains(&202));
    assert!(!bob_pinned_ids.contains(&203));

    // Alice's device 2 also receives the encryption key capability, opens and applies it
    let device2_decrypted = open(&secret, &ciphertext)?;
    apply_records(&alice_device2_store, "bob", &device2_decrypted).await?;

    let dev2_pins = alice_device2_store.get_pinned_messages_of("bob").await?;
    assert_eq!(dev2_pins.len(), 2);
    let dev2_pinned_ids: Vec<u64> = dev2_pins.iter().map(|m| m.id).collect();
    assert!(dev2_pinned_ids.contains(&301));
    assert!(dev2_pinned_ids.contains(&302));
    assert!(!dev2_pinned_ids.contains(&303));

    // Now unpin Msg 1 (leaving only Msg 2 pinned)
    alice_store.update_message_type("bob", 101, 0).await?;
    let updated_pins = alice_store.get_pinned_messages_of("bob").await?;
    assert_eq!(updated_pins.len(), 1);

    let snapshot_id_2 = "99998888777766665555444433332222";
    let ciphertext_2 = seal("bob", snapshot_id_2, &key, &updated_pins)?;
    let secret_2 = firefly::DirectPinSnapshotSecret {
        version: 1,
        other: "bob".into(),
        snapshot_id: snapshot_id_2.into(),
        key: key.to_vec().into(),
        ciphertext_hash: sha2::Sha256::digest(&ciphertext_2).to_vec().into(),
        message_count: 1,
    };

    let bob_decrypted_2 = open(&secret_2, &ciphertext_2)?;
    apply_records(&bob_store, "alice", &bob_decrypted_2).await?;

    let bob_pins_after = bob_store.get_pinned_messages_of("alice").await?;
    assert_eq!(bob_pins_after.len(), 1);
    assert_eq!(bob_pins_after[0].id, 202);

    Ok(())
}
