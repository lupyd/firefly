use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};

use firefly_client::{
    db::{
        favourites::{FavoriteMessagesStore, FavouriteMessagesStore},
        group_messages::GroupMessage,
        messages::UserMessage,
    },
    storage::{
        FavouriteMessage, FavouriteMessageStorage, FavouriteSource, MemoryFavouriteMessageStore,
    },
};
use firefly_protos::{
    firefly::{self, mod_GroupMessageInner, mod_UserMessageInner, MessagePayload},
    serialize_proto,
};

async fn create_memory_pool() -> SqlitePool {
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap()
}

fn create_test_user_payload(text: &str) -> Vec<u8> {
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
    serialize_proto(&inner).unwrap().to_vec()
}

fn create_test_group_payload(channel_id: u32, text: &str) -> Vec<u8> {
    let inner = firefly::GroupMessageInner {
        channelId: channel_id,
        message: mod_GroupMessageInner::OneOfmessage::messagePayload(MessagePayload {
            text: text.into(),
            files: None,
            ext: firefly::mod_MessagePayload::OneOfext::None,
            message_type: 0,
        }),
        message_type: 0,
    };
    serialize_proto(&inner).unwrap().to_vec()
}

#[tokio::test]
async fn test_favourite_store_lifecycle() {
    let pool = create_memory_pool().await;
    let store = FavouriteMessagesStore::new(pool).await.unwrap();

    // 1. Add user message favourite
    let user_msg = UserMessage {
        id: 101,
        other: "alice".to_string(),
        sent_by_other: true,
        message: create_test_user_payload("Important secret key for deployment"),
        message_type: 0,
    };

    let id1 = store.add_user_message(&user_msg, None).await.unwrap();
    assert!(id1 > 0);

    // Verify it is favourite
    assert!(store.is_user_favourite("alice", 101).await.unwrap());
    assert!(!store.is_user_favourite("bob", 101).await.unwrap());
    assert!(!store.is_user_favourite("alice", 999).await.unwrap());

    // 2. Add group message favourite
    let group_msg = GroupMessage {
        id: 202,
        group_id: 500,
        by: "carol".to_string(),
        message: create_test_group_payload(1, "Release schedule confirmed for Friday"),
        channel_id: 1,
        epoch: 2,
        message_type: 0,
    };

    let id2 = store.add_group_message(&group_msg, None).await.unwrap();
    assert!(id2 > 0);

    // Verify group message favourite
    assert!(store.is_group_favourite(500, 202).await.unwrap());
    assert!(!store.is_group_favourite(501, 202).await.unwrap());
    assert!(!store.is_group_favourite(500, 999).await.unwrap());

    // 3. Query all favourites
    let all = store.get_all(10, 0).await.unwrap();
    assert_eq!(all.len(), 2);
    // Descending order of creation
    assert_eq!(all[0].id, id2);
    assert_eq!(all[1].id, id1);

    // 4. Query user favourites
    let alice_favs = store.get_user_favourites(Some("alice"), 10, 0).await.unwrap();
    assert_eq!(alice_favs.len(), 1);
    assert_eq!(alice_favs[0].text, "Important secret key for deployment");
    assert_eq!(alice_favs[0].source, FavouriteSource::User);

    let bob_favs = store.get_user_favourites(Some("bob"), 10, 0).await.unwrap();
    assert!(bob_favs.is_empty());

    let all_user_favs = store.get_user_favourites(None, 10, 0).await.unwrap();
    assert_eq!(all_user_favs.len(), 1);

    // 5. Query group favourites
    let group_favs = store.get_group_favourites(Some(500), None, 10, 0).await.unwrap();
    assert_eq!(group_favs.len(), 1);
    assert_eq!(group_favs[0].text, "Release schedule confirmed for Friday");
    assert_eq!(group_favs[0].channel_id, Some(1));

    let ch1_favs = store.get_group_favourites(Some(500), Some(1), 10, 0).await.unwrap();
    assert_eq!(ch1_favs.len(), 1);

    let ch2_favs = store.get_group_favourites(Some(500), Some(2), 10, 0).await.unwrap();
    assert!(ch2_favs.is_empty());

    // 6. Test idempotent re-add (update)
    let re_added_id = store.add_user_message(&user_msg, Some("Updated note".to_string())).await.unwrap();
    assert_eq!(re_added_id, id1);
    let updated = store.get_by_id(id1).await.unwrap().unwrap();
    assert_eq!(updated.text, "Updated note");

    // 7. Remove favourites
    let removed_user = store.remove_user_favourite("alice", 101).await.unwrap();
    assert!(removed_user);
    assert!(!store.is_user_favourite("alice", 101).await.unwrap());

    let removed_group = store.remove_group_favourite(500, 202).await.unwrap();
    assert!(removed_group);
    assert!(!store.is_group_favourite(500, 202).await.unwrap());

    let empty = store.get_all(10, 0).await.unwrap();
    assert!(empty.is_empty());
}

#[tokio::test]
async fn test_favourite_store_remove_by_id_and_clear() {
    let pool = create_memory_pool().await;
    let store: FavoriteMessagesStore = FavouriteMessagesStore::new(pool).await.unwrap();

    let fav1 = FavouriteMessage {
        id: 0,
        source: FavouriteSource::User,
        message_id: 1,
        other: Some("user_a".to_string()),
        group_id: None,
        channel_id: None,
        by: "user_a".to_string(),
        text: "msg 1".to_string(),
        message: vec![1, 2, 3],
        message_type: 0,
        epoch: None,
        created_at: 1000,
    };

    let fav2 = FavouriteMessage {
        id: 0,
        source: FavouriteSource::Group,
        message_id: 2,
        other: None,
        group_id: Some(10),
        channel_id: Some(1),
        by: "user_b".to_string(),
        text: "msg 2".to_string(),
        message: vec![4, 5, 6],
        message_type: 0,
        epoch: Some(1),
        created_at: 2000,
    };

    let id1 = store.add(fav1).await.unwrap();
    let id2 = store.add(fav2).await.unwrap();

    assert_eq!(store.get_all(10, 0).await.unwrap().len(), 2);

    // Remove one by id
    let removed = store.remove_by_id(id1).await.unwrap();
    assert!(removed);
    assert_eq!(store.get_all(10, 0).await.unwrap().len(), 1);

    // Remove again returns false
    assert!(!store.remove_by_id(id1).await.unwrap());

    // Clear all
    store.clear_all().await.unwrap();
    assert!(store.get_all(10, 0).await.unwrap().is_empty());
    assert!(store.get_by_id(id2).await.unwrap().is_none());
}

#[tokio::test]
async fn test_memory_favourite_store() {
    let store = MemoryFavouriteMessageStore::new();

    let fav_user = FavouriteMessage {
        id: 0,
        source: FavouriteSource::User,
        message_id: 10,
        other: Some("alice".to_string()),
        group_id: None,
        channel_id: None,
        by: "alice".to_string(),
        text: "hello memory".to_string(),
        message: vec![1, 2],
        message_type: 0,
        epoch: None,
        created_at: 100,
    };

    let fav_group = FavouriteMessage {
        id: 0,
        source: FavouriteSource::Group,
        message_id: 20,
        other: None,
        group_id: Some(77),
        channel_id: Some(3),
        by: "bob".to_string(),
        text: "group memory".to_string(),
        message: vec![3, 4],
        message_type: 0,
        epoch: Some(1),
        created_at: 200,
    };

    let id1 = store.add(fav_user).await.unwrap();
    let id2 = store.add(fav_group).await.unwrap();

    assert!(store.is_user_favourite("alice", 10).await.unwrap());
    assert!(!store.is_user_favourite("alice", 11).await.unwrap());
    assert!(store.is_group_favourite(77, 20).await.unwrap());
    assert!(!store.is_group_favourite(77, 21).await.unwrap());

    let all = store.get_all(10, 0).await.unwrap();
    assert_eq!(all.len(), 2);
    // Ordered descending by created_at
    assert_eq!(all[0].id, id2);
    assert_eq!(all[1].id, id1);

    let user_favs = store.get_user_favourites(Some("alice"), 10, 0).await.unwrap();
    assert_eq!(user_favs.len(), 1);
    assert_eq!(user_favs[0].id, id1);

    let group_favs = store.get_group_favourites(Some(77), Some(3), 10, 0).await.unwrap();
    assert_eq!(group_favs.len(), 1);
    assert_eq!(group_favs[0].id, id2);

    let removed = store.remove_user_favourite("alice", 10).await.unwrap();
    assert!(removed);
    assert!(!store.is_user_favourite("alice", 10).await.unwrap());

    let removed_grp = store.remove_group_favourite(77, 20).await.unwrap();
    assert!(removed_grp);
    assert!(!store.is_group_favourite(77, 20).await.unwrap());

    assert!(store.get_all(10, 0).await.unwrap().is_empty());
}
