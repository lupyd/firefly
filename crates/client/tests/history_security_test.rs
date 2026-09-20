use firefly_client::{db::{setup_pool, group_messages::{GroupMessage, GroupMessagesStore}}, history::{compute_unencrypted_hash, decrypt_and_unpack_chunk, pack_messages_into_chunk, validate_chunk_records, history_chunk_url}};
use firefly_protos::{firefly, serialize_proto};

fn record(id: u64, channel: u32) -> anyhow::Result<GroupMessage> {
    let inner = firefly::GroupMessageInner { channelId: channel, message_type: 0, message: firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(firefly::MessagePayload {text: format!("message {id}").into(), ..Default::default()}) };
    Ok(GroupMessage {id, group_id:42, by:"alice".into(), message:serialize_proto(&inner)?.to_vec(), channel_id:channel, epoch:1, message_type:0})
}

#[test]
fn sender_integrity_and_structural_validation() -> anyhow::Result<()> {
    let messages = vec![record(1,1)?, record(3,2)?];
    let packed = pack_messages_into_chunk(&messages)?;
    let decoded = decrypt_and_unpack_chunk(&packed.blob, &packed.key, &packed.unencrypted_hash)?;
    assert_eq!(decoded[0].by,"alice");
    validate_chunk_records(&decoded,42,1,3,2)?;
    let mut changed = decoded.clone(); changed[0].by="mallory".into();
    assert_ne!(compute_unencrypted_hash(&changed)?, packed.unencrypted_hash);
    changed=decoded.clone(); changed[1].group_id=43;
    assert!(validate_chunk_records(&changed,42,1,3,2).is_err());
    changed=decoded.clone(); changed[1].id=1;
    assert!(validate_chunk_records(&changed,42,1,3,2).is_err());
    assert!(validate_chunk_records(&decoded,42,1,3,1).is_err());
    assert!(validate_chunk_records(&decoded,42,1,4,2).is_err());
    changed=decoded.clone(); changed[0].by.clear();
    assert!(validate_chunk_records(&changed,42,1,3,2).is_err());
    changed=decoded; changed[0].message_type=firefly_protos::MESSAGE_TYPE_HIDDEN;
    assert!(validate_chunk_records(&changed,42,1,3,2).is_err());
    Ok(())
}

#[test]
fn history_urls_reject_credentials_cross_group_and_external_origins() -> anyhow::Result<()> {
    let base="https://cdn.example/api/v1";
    let valid="https://cdn.example/api/v1/group_chunks/42/0123456789abcdef0123456789abcdef";
    assert!(history_chunk_url(base, valid,42).is_ok());
    for bad in [valid.replace("cdn.example","evil.example"), valid.replace("/42/","/43/"),format!("{valid}?token=secret"),format!("{valid}#key"),valid.replace("https://","https://user:pass@"),valid.replace("https:","http:"),valid.replace("0123456789abcdef0123456789abcdef","../other")] {
        assert!(history_chunk_url(base,&bad,42).is_err(),"accepted {bad}");
    }
    Ok(())
}

#[tokio::test]
async fn imports_never_overwrite_originals_or_become_vote_evidence() -> anyhow::Result<()> {
    let pool=setup_pool("sqlite::memory:",1).await?;
    let store=GroupMessagesStore::new(pool.clone()).await?;
    let original=record(1,1)?;
    sqlx::query("INSERT INTO group_messages(id,group_id,by,message,channel_id,epoch,message_type,text) VALUES(1,42,'alice',?,1,1,0,'original')").bind(&original.message).execute(&pool).await?;
    let mut forged=original.clone(); forged.by="mallory".into();
    let imported=record(3,1)?;
    assert_eq!(store.import_verified_history(42,7,&[1;32],&[forged,imported]).await?,1);
    let evidence=store.authenticated_history_ids(42,&[1,3]).await?;
    assert_eq!(evidence.len(),1); assert_eq!(evidence[0].by,"alice");
    assert_eq!(store.get_channel_page(42,1,4,10).await?.len(),2);
    let mut foreign=record(5,1)?; foreign.group_id=43;
    assert!(store.import_verified_history(42,8,&[2;32],&[record(4,1)?,foreign]).await.is_err());
    assert_eq!(store.get_channel_page(42,1,10,10).await?.len(),2,"partial import must roll back");
    Ok(())
}

#[tokio::test]
async fn channel_pagination_filters_before_limit() -> anyhow::Result<()> {
    let pool=setup_pool("sqlite::memory:",1).await?;
    let store=GroupMessagesStore::new(pool).await?;
    let mut messages=vec![record(1,1)?,record(2,1)?];
    for id in 3..=100 { messages.push(record(id,2)?); }
    store.import_verified_history(42,7,&[1;32],&messages).await?;
    let page=store.get_channel_page(42,1,101,1).await?;
    assert_eq!(page[0].id,2);
    let next=store.get_channel_page(42,1,2,1).await?;
    assert_eq!(next[0].id,1);
    Ok(())
}

#[tokio::test]
async fn download_sends_no_credentials_and_never_follows_redirects() -> anyhow::Result<()> {
    use tokio::{net::TcpListener, io::{AsyncReadExt, AsyncWriteExt}};
    for redirect in [false,true] {
        let listener=TcpListener::bind("127.0.0.1:0").await?;
        let target=TcpListener::bind("127.0.0.1:0").await?;
        let target_url=format!("http://{}/stolen",target.local_addr()?);
        let base=format!("http://{}",listener.local_addr()?);
        let url=format!("{base}/group_chunks/42/0123456789abcdef0123456789abcdef");
        let task=tokio::spawn(async move {
            let (mut stream,_)=listener.accept().await?;
            let mut request=Vec::new();
            loop {
                let mut buffer=[0u8;1024]; let n=stream.read(&mut buffer).await?;
                if n==0 { break; } request.extend_from_slice(&buffer[..n]);
                if request.windows(4).any(|w|w==b"\r\n\r\n") { break; }
                anyhow::ensure!(request.len()<8192,"Unbounded request");
            }
            let response=if redirect {format!("HTTP/1.1 302 Found\r\nLocation: {target_url}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")} else {"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\ndata".into()};
            stream.write_all(response.as_bytes()).await?;
            Ok::<String,anyhow::Error>(String::from_utf8(request)?)
        });
        let result=firefly_client::history::download_history_blob(&base,42,&url).await;
        if redirect { assert!(result.is_err()); } else { assert_eq!(result?,b"data"); }
        let request=task.await??.to_lowercase();
        assert!(!request.contains("authorization:"));assert!(!request.contains("cookie:"));assert!(!request.contains("token="));
        assert!(tokio::time::timeout(std::time::Duration::from_millis(50),target.accept()).await.is_err());
    }
    Ok(())
}

#[test]
fn thousand_message_zstd_aes_roundtrip_and_count_limit() -> anyhow::Result<()> {
    use aes_gcm::{aead::{Aead,KeyInit},Aes256Gcm,Nonce};
    let messages=(1..=1000).map(|id|record(id,1)).collect::<anyhow::Result<Vec<_>>>()?;
    let packed=pack_messages_into_chunk(&messages)?;
    let cipher=Aes256Gcm::new_from_slice(&packed.key)?;
    let compressed=cipher.decrypt(Nonce::from_slice(&packed.blob[..12]),&packed.blob[12..]).map_err(|_|anyhow::anyhow!("decrypt"))?;
    assert_eq!(&compressed[..4],&[0x28,0xb5,0x2f,0xfd],"zstd frame magic");
    let decoded=decrypt_and_unpack_chunk(&packed.blob,&packed.key,&packed.unencrypted_hash)?;
    validate_chunk_records(&decoded,42,1,1000,1000)?;
    let mut too_many=messages;too_many.push(record(1001,1)?);
    assert!(pack_messages_into_chunk(&too_many).is_err());
    let mut tampered=packed.blob;tampered[20]^=1;
    assert!(decrypt_and_unpack_chunk(&tampered,&packed.key,&packed.unencrypted_hash).is_err());
    Ok(())
}

#[test]
fn zstd_expansion_is_bounded_before_deserializing() -> anyhow::Result<()> {
    use aes_gcm::{aead::{Aead,KeyInit},Aes256Gcm,Nonce};
    use firefly_client::history::MAX_CHUNK_PLAINTEXT;
    let expanded=vec![0u8;MAX_CHUNK_PLAINTEXT+1];
    let compressed=zstd::stream::encode_all(expanded.as_slice(),3)?;
    let key=[7u8;32];let nonce=[8u8;12];
    let cipher=Aes256Gcm::new_from_slice(&key)?;
    let mut blob=nonce.to_vec();blob.extend(cipher.encrypt(Nonce::from_slice(&nonce),compressed.as_slice()).map_err(|_|anyhow::anyhow!("encrypt"))?);
    let error=decrypt_and_unpack_chunk(&blob,&key,&[0;32]).expect_err("expansion rejected");
    assert!(error.to_string().contains("expansion limit"),"{error}");
    Ok(())
}

#[tokio::test]
async fn replacement_chunks_update_imports_but_live_receipts_and_pin_snapshots_win() -> anyhow::Result<()> {
    let pool=setup_pool("sqlite::memory:",1).await?;
    let store=GroupMessagesStore::new(pool).await?;
    let original=record(10,1)?;
    store.import_verified_history(42,1,&[1;32],&[original.clone()]).await?;
    let mut replacement=original.clone();replacement.by="bob".into();
    assert_eq!(store.import_verified_history(42,2,&[2;32],&[replacement.clone()]).await?,1);
    assert_eq!(store.get_message(42,10).await?.expect("import").by,"bob");
    store.add(10,42,1,1,"alice",&original.message,0).await?;
    assert_eq!(store.authenticated_history_ids(42,&[10]).await?.len(),1);
    assert_eq!(store.import_verified_history(42,3,&[3;32],&[replacement]).await?,0);
    assert_eq!(store.get_message(42,10).await?.expect("live").by,"alice");
    let mut pinned=record(11,1)?;pinned.message_type=1;
    store.import_verified_history(42,0,&[4;32],&[pinned]).await?;
    store.import_verified_history(42,4,&[5;32],&[record(11,1)?]).await?;
    assert_eq!(store.get_message(42,11).await?.expect("snapshot").message_type,1);
    assert!(store.authenticated_history_ids(42,&[11]).await?.is_empty());
    Ok(())
}

#[test]
fn receipt_epoch_is_not_part_of_plaintext_consensus() -> anyhow::Result<()> {
    let a=vec![record(10,1)?];let mut b=a.clone();b[0].epoch=999;
    assert_eq!(compute_unencrypted_hash(&a)?,compute_unencrypted_hash(&b)?);
    let packed=pack_messages_into_chunk(&b)?;
    assert_eq!(packed.unencrypted_hash,compute_unencrypted_hash(&a)?);
    Ok(())
}

#[tokio::test]
async fn three_stores_require_independent_receipts_not_joiner_imports() -> anyhow::Result<()> {
    let publisher = GroupMessagesStore::new(setup_pool("sqlite::memory:",1).await?).await?;
    let reviewer = GroupMessagesStore::new(setup_pool("sqlite::memory:",1).await?).await?;
    let joiner = GroupMessagesStore::new(setup_pool("sqlite::memory:",1).await?).await?;
    let records = (1..=1000).map(|id|record(id,1)).collect::<anyhow::Result<Vec<_>>>()?;
    for r in &records {
        publisher.add(r.id,r.group_id,r.channel_id,1,&r.by,&r.message,0).await?;
        reviewer.add(r.id,r.group_id,r.channel_id,9,&r.by,&r.message,0).await?;
    }
    let ids = records.iter().map(|r|r.id).collect::<Vec<_>>();
    let originals = publisher.authenticated_history_ids(42,&ids).await?;
    let packed = pack_messages_into_chunk(&originals)?;
    let evidence = reviewer.authenticated_history_ids(42,&ids).await?;
    assert_eq!(compute_unencrypted_hash(&evidence)?,packed.unencrypted_hash);
    assert!(decrypt_and_unpack_chunk(&packed.blob,&[0;32],&packed.unencrypted_hash).is_err());
    let decoded = decrypt_and_unpack_chunk(&packed.blob,&packed.key,&packed.unencrypted_hash)?;
    validate_chunk_records(&decoded,42,1,1000,1000)?;
    assert_eq!(joiner.import_verified_history(42,7,&packed.unencrypted_hash,&decoded).await?,1000);
    assert!(joiner.authenticated_history_ids(42,&ids).await?.is_empty());
    // A joiner only becomes an independent witness for actually received IDs.
    let r=&records[0];
    joiner.add(r.id,r.group_id,r.channel_id,10,&r.by,&r.message,0).await?;
    assert_eq!(joiner.authenticated_history_ids(42,&ids).await?.len(),1);
    let mut forged = records.clone();
    forged[500].by = "mallory".into();
    let forged_chunk = pack_messages_into_chunk(&forged)?;
    assert_ne!(compute_unencrypted_hash(&evidence)?,forged_chunk.unencrypted_hash);
    Ok(())
}
