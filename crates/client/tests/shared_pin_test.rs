use firefly_client::db::{setup_pool,group_messages::GroupMessagesStore};
use firefly_protos::{firefly::{self,mod_GroupMessageInner::OneOfmessage},serialize_proto};

async fn event(store:&GroupMessagesStore,id:u64,channel:u32,pinned:bool)->anyhow::Result<()> {
    let inner=firefly::GroupMessageInner{channelId:channel,message_type:2,message:OneOfmessage::pinUpdate(firefly::GroupPinUpdate{message_id:10,pinned})};
    store.add(id,42,channel,1,"moderator",&serialize_proto(&inner)?,2).await
}
async fn target(store:&GroupMessagesStore)->anyhow::Result<()> {
    let inner=firefly::GroupMessageInner{channelId:1,message_type:8,message:OneOfmessage::messagePayload(firefly::MessagePayload{text:"target".into(),..Default::default()})};
    store.add(10,42,1,1,"alice",&serialize_proto(&inner)?,8).await
}
#[tokio::test]
async fn shared_pin_and_unpin_converge_with_reordering_and_missing_targets()->anyhow::Result<()> {
    let a=GroupMessagesStore::new(setup_pool("sqlite::memory:",1).await?).await?;
    let b=GroupMessagesStore::new(setup_pool("sqlite::memory:",1).await?).await?;
    target(&a).await?;
    event(&a,20,1,true).await?; event(&b,20,1,true).await?;
    target(&b).await?;
    for s in [&a,&b] {assert_eq!(s.get_message(42,10).await?.expect("target").message_type,9);}
    event(&a,30,1,false).await?;
    event(&b,30,1,false).await?;
    event(&b,20,1,true).await?; // stale replay cannot restore the pin
    for s in [&a,&b] {assert_eq!(s.get_message(42,10).await?.expect("target").message_type,8);}
    Ok(())
}
#[tokio::test]
async fn other_channel_cannot_pin_or_poison_future_valid_updates()->anyhow::Result<()> {
    let store=GroupMessagesStore::new(setup_pool("sqlite::memory:",1).await?).await?;
    event(&store,50,2,true).await?;
    target(&store).await?;
    assert_eq!(store.get_message(42,10).await?.expect("target").message_type,8);
    event(&store,30,1,true).await?;
    assert_eq!(store.get_message(42,10).await?.expect("target").message_type,9);
    assert!(store.get_message(43,10).await?.is_none());
    Ok(())
}

// Exhaust all target/pin/unpin/repin delivery orders, not just one happy path.
#[tokio::test]
async fn every_delivery_order_converges_after_duplicates_and_store_reopen() -> anyhow::Result<()> {
    let mut orders = Vec::new();
    for a in 0..4 { for b in 0..4 { for c in 0..4 { for d in 0..4 {
        if a != b && a != c && a != d && b != c && b != d && c != d {
            orders.push([a,b,c,d]);
        }
    }}}}
    assert_eq!(orders.len(), 24);
    for order in orders {
        let pool = setup_pool("sqlite::memory:", 1).await?;
        let store = GroupMessagesStore::new(pool.clone()).await?;
        for operation in order {
            match operation {
                0 => target(&store).await?,
                1 => event(&store, 20, 1, true).await?,
                2 => event(&store, 30, 1, false).await?,
                3 => event(&store, 40, 1, true).await?,
                _ => unreachable!(),
            }
        }
        drop(store);
        let store = GroupMessagesStore::new(pool).await?;
        // Recreating the store must retain ordering evidence in SQL.
        event(&store, 30, 1, false).await?;
        event(&store, 20, 1, true).await?;
        assert_eq!(store.get_message(42, 10).await?.expect("target").message_type, 9, "order {order:?}");
        event(&store, 50, 1, false).await?;
        event(&store, 40, 1, true).await?;
        assert_eq!(store.get_message(42, 10).await?.expect("target").message_type, 8, "order {order:?}");
    }
    Ok(())
}
