//! Real MLS + production SQLite storage integration. No network or auth emulator.
//! BasicIdentityProvider is test-only: these tests do NOT cover Firefly authentication.
//! Each parent starts fresh processes; READY barriers replace timing sleeps.
use anyhow::Context;
use firefly_client::db::{
    group_stores::GroupStateStore, keyvalue::KeyValueStore, setup_pool_from_path,
};
use firefly_core::storage_provider::FfiGroupStateStorage;
use mls_rs::{
    CipherSuiteProvider, Client, CryptoProvider, MlsMessage,
    client_builder::{BaseConfig, WithCryptoProvider, WithGroupStateStorage, WithIdentityProvider},
    identity::{
        SigningIdentity,
        basic::{BasicCredential, BasicIdentityProvider},
    },
};
use mls_rs_crypto_rustcrypto::RustCryptoProvider;
use serde::{Deserialize, Serialize};
use std::{
    io::{BufRead, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

type Config = WithIdentityProvider<
    BasicIdentityProvider,
    WithCryptoProvider<RustCryptoProvider, WithGroupStateStorage<FfiGroupStateStorage, BaseConfig>>,
>;
const PAYLOAD: &[u8] = b"test-only authenticated chunk capability";
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "firefly-mls-fault-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let mut builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&p).unwrap();
        Self(p)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
#[derive(Serialize, Deserialize)]
struct Identity {
    secret: Vec<u8>,
    public: Vec<u8>,
}
async fn client(root: &Path, name: &str) -> anyhow::Result<(Client<Config>, sqlx::SqlitePool)> {
    let pool = setup_pool_from_path(
        root.join(format!("{name}.sqlite"))
            .to_str()
            .context("path")?,
        1,
    )
    .await?;
    let provider = RustCryptoProvider::default();
    let suite = firefly_core::config::CIPHERSUITE;
    let path = root.join(format!("{name}.identity"));
    let identity: Identity = if path.exists() {
        serde_json::from_slice(&std::fs::read(&path)?)?
    } else {
        let (secret, public) = provider
            .cipher_suite_provider(suite)
            .context("suite")?
            .signature_key_generate()
            .await?;
        let identity = Identity {
            secret: secret.as_bytes().to_vec(),
            public: public.as_bytes().to_vec(),
        };
        std::fs::write(path, serde_json::to_vec(&identity)?)?;
        identity
    };
    let signing = SigningIdentity::new(
        BasicCredential::new(name.as_bytes().to_vec()).into_credential(),
        identity.public.into(),
    );
    let store: Arc<dyn firefly_core::storage_provider::MlsGroupStateStorage> =
        Arc::new(GroupStateStore::new(pool.clone()).await?);
    let client = Client::builder()
        .group_state_storage(FfiGroupStateStorage::new(store))
        .crypto_provider(provider)
        .identity_provider(BasicIdentityProvider::new())
        .signing_identity(signing, identity.secret.into(), suite)
        .build();
    Ok((client, pool))
}
async fn setup(root: &Path) -> anyhow::Result<()> {
    let (alice, ap) = client(root, "alice").await?;
    let (bob, bp) = client(root, "bob").await?;
    let mut group = alice
        .create_group(Default::default(), Default::default(), None)
        .await?;
    let kp = bob
        .generate_key_package_message(Default::default(), Default::default(), None)
        .await?;
    let commit = group.commit_builder().add_member(kp)?.build().await?;
    group.apply_pending_commit().await?;
    let (mut joined, _) = bob
        .join_group(None, &commit.welcome_messages[0], None)
        .await?;
    joined.write_to_storage().await?;
    std::fs::write(root.join("group-id"), joined.group_id())?;
    for (name, data) in [("cipher", PAYLOAD), ("next", b"next message".as_slice())] {
        let message = group
            .encrypt_application_message(data, Default::default())
            .await?;
        std::fs::write(root.join(name), message.to_bytes()?)?;
    }
    ap.close().await;
    bp.close().await;
    Ok(())
}
fn ready_and_block() {
    println!("FAULT_READY");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}
async fn worker(root: &Path, phase: &str) -> anyhow::Result<()> {
    if phase.starts_with("tail_") {
        return tail_worker(root, phase).await;
    }
    if phase == "setup" {
        return setup(root).await;
    }
    let (bob, pool) = client(root, "bob").await?;
    let id = std::fs::read(root.join("group-id"))?;
    let mut group = bob.load_group(&id).await?;
    let cipher = std::fs::read(root.join("cipher"))?;
    if phase == "before_decrypt" {
        ready_and_block();
    }
    if phase == "reverse_order" {
        group
            .process_incoming_message(MlsMessage::from_bytes(&std::fs::read(root.join("next"))?)?)
            .await?;
        group.write_to_storage().await?;
        drop(group);
        let mut group = bob.load_group(&id).await?;
        let m = group
            .process_incoming_message(MlsMessage::from_bytes(&cipher)?)
            .await?;
        anyhow::ensure!(
            matches!(m, mls_rs::group::ReceivedMessage::ApplicationMessage(_)),
            "older in-epoch message lost"
        );
        return Ok(());
    }
    if phase == "tampered_ciphertext" {
        let mut damaged = cipher.clone();
        let last = damaged.len() - 1;
        damaged[last] ^= 1;
        anyhow::ensure!(
            group
                .process_incoming_message(MlsMessage::from_bytes(&damaged)?)
                .await
                .is_err(),
            "tampering accepted"
        );
        // Do not assume an unsuccessful MLS operation preserved in-memory state.
        drop(group);
        let mut group = bob.load_group(&id).await?;
        group
            .process_incoming_message(MlsMessage::from_bytes(&cipher)?)
            .await?;
        return Ok(());
    }
    if phase == "verify_capability" {
        let kv = KeyValueStore::new(pool.clone()).await?;
        anyhow::ensure!(
            kv.get("authenticated-capability").await? == hex::encode(PAYLOAD),
            "capability lost"
        );
        println!("CAPABILITY_OK");
        return Ok(());
    }
    if phase == "wrong_account" {
        let (other, other_pool) = client(root, "carol").await?;
        anyhow::ensure!(
            other.load_group(&id).await.is_err(),
            "cross-account MLS state leaked"
        );
        let kv = KeyValueStore::new(other_pool).await?;
        anyhow::ensure!(
            kv.get("authenticated-capability").await.is_err(),
            "cross-account capability leaked"
        );
        return Ok(());
    }
    if phase == "verify_replay" {
        match group
            .process_incoming_message(MlsMessage::from_bytes(&cipher)?)
            .await
        {
            Ok(mls_rs::group::ReceivedMessage::ApplicationMessage(m)) => {
                anyhow::ensure!(m.data() == PAYLOAD, "incorrect plaintext");
                println!("REPLAY_OK");
            }
            Ok(_) => anyhow::bail!("not application message"),
            Err(_) => println!("REPLAY_LOST"),
        }
        return Ok(());
    }
    let result = group
        .process_incoming_message(MlsMessage::from_bytes(&cipher)?)
        .await?;
    let m = match result {
        mls_rs::group::ReceivedMessage::ApplicationMessage(m) => m,
        _ => anyhow::bail!("wrong message kind"),
    };
    anyhow::ensure!(m.data() == PAYLOAD, "incorrect plaintext");
    if phase == "after_decrypt_before_save" {
        ready_and_block();
    }
    if phase == "write_failure" {
        sqlx::query("CREATE TRIGGER fail_state BEFORE INSERT ON group_states BEGIN SELECT RAISE(ABORT,'injected disk write failure'); END").execute(&pool).await?;
        anyhow::ensure!(
            group.write_to_storage().await.is_err(),
            "fault not triggered"
        );
        anyhow::ensure!(
            group
                .process_incoming_message(MlsMessage::from_bytes(&cipher)?)
                .await
                .is_err(),
            "mutated in-memory ratchet unexpectedly reusable"
        );
        println!("WRITE_FAILED_MEMORY_CONSUMED");
        return Ok(());
    }
    let journal_id = firefly_client::receipt_coordinator::ReceiptCoordinator::stage_receipt(
        &pool,
        group.group_id(),
        group.current_epoch(),
        "authenticated-capability",
        &hex::encode(m.data()),
    ).await?;
    group.write_to_storage().await?;
    if phase == "after_save_before_capability" {
        ready_and_block();
    }
    if phase == "reordered_delivery" {
        // This worker consumed the first message; verify duplicate rejection doesn't
        // prevent the next authentic message. Separate test below uses reverse order.
        anyhow::ensure!(
            group
                .process_incoming_message(MlsMessage::from_bytes(&cipher)?)
                .await
                .is_err(),
            "duplicate accepted"
        );
        group
            .process_incoming_message(MlsMessage::from_bytes(&std::fs::read(root.join("next"))?)?)
            .await?;
        return Ok(());
    }
    firefly_client::receipt_coordinator::ReceiptCoordinator::commit_receipt(
        &pool,
        journal_id,
        "authenticated-capability",
        &hex::encode(m.data()),
    ).await?;
    let _ = KeyValueStore::new(pool.clone()).await?;
    if phase == "after_capability" {
        ready_and_block();
    }
    anyhow::bail!("unknown fault phase")
}
fn command(root: &Path, phase: &str) -> Command {
    let mut c = Command::new(std::env::current_exe().unwrap());
    c.args(["--exact", "fault_worker", "--ignored", "--nocapture"])
        .env("FIREFLY_FAULT_ROOT", root)
        .env("FIREFLY_FAULT_PHASE", phase);
    c
}
fn run(root: &Path, phase: &str) -> String {
    let mut child = command(root, phase)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        use std::io::Read;
        let mut text = String::new();
        let result = stdout.read_to_string(&mut text).map(|_| text);
        let _ = tx.send(result);
    });
    let result = rx.recv_timeout(Duration::from_secs(30));
    if result.is_err() {
        let _ = child.kill();
    }
    let status = child.wait().unwrap();
    reader.join().unwrap();
    let output = result
        .expect("worker timeout/disconnected")
        .expect("worker stdout");
    assert!(status.success(), "worker {phase}: {output}");
    output
}
fn crash(root: &Path, phase: &str) {
    let mut child = command(root, phase)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in std::io::BufReader::new(stdout).lines() {
            match line {
                Ok(line) if line == "FAULT_READY" => {
                    let _ = tx.send(());
                    break;
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });
    let reached = rx.recv_timeout(Duration::from_secs(30));
    let _ = child.kill();
    let status = child.wait().unwrap();
    reader.join().unwrap();
    assert!(reached.is_ok(), "worker failed to reach {phase}");
    assert!(!status.success(), "worker was not killed");
}
#[test]
#[ignore = "subprocess helper, not a standalone scenario"]
fn fault_worker() {
    let root = std::env::var_os("FIREFLY_FAULT_ROOT").expect("parent-only worker");
    let phase = std::env::var("FIREFLY_FAULT_PHASE").unwrap();
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(worker(Path::new(&root), &phase))
        .unwrap();
}
#[test]
fn crash_before_decrypt_replays_from_fresh_process() {
    let dir = Scratch::new();
    run(&dir.0, "setup");
    crash(&dir.0, "before_decrypt");
    assert!(run(&dir.0, "verify_replay").contains("REPLAY_OK"));
}
#[test]
fn crash_after_decrypt_before_save_replays_from_fresh_process() {
    let dir = Scratch::new();
    run(&dir.0, "setup");
    crash(&dir.0, "after_decrypt_before_save");
    assert!(run(&dir.0, "verify_replay").contains("REPLAY_OK"));
}
#[test]
fn crash_after_ratchet_save_must_not_lose_capability() {
    let dir = Scratch::new();
    run(&dir.0, "setup");
    crash(&dir.0, "after_save_before_capability");
    let replay = run(&dir.0, "verify_replay");
    let recovered = tokio::runtime::Runtime::new().unwrap().block_on(async {
        let (_, pool) = client(&dir.0, "bob").await.unwrap();
        let kv = KeyValueStore::new(pool.clone()).await.unwrap();
        let saved = kv.get("authenticated-capability").await.is_ok();
        pool.close().await;
        saved
    });
    assert!(
        recovered || replay.contains("REPLAY_OK"),
        "S10 reproduced: MLS ratchet persisted, capability absent, ciphertext cannot be decrypted after process restart"
    );
}
#[test]
fn failed_storage_requires_reloading_consumed_in_memory_ratchet() {
    let dir = Scratch::new();
    run(&dir.0, "setup");
    assert!(run(&dir.0, "write_failure").contains("WRITE_FAILED_MEMORY_CONSUMED"));
    assert!(run(&dir.0, "verify_replay").contains("REPLAY_OK"));
}
#[test]
fn duplicate_delivery_does_not_block_next_authentic_message() {
    let dir = Scratch::new();
    run(&dir.0, "setup");
    run(&dir.0, "reordered_delivery");
}

#[test]
fn crash_after_capability_save_recovers_without_redecrypting() {
    let dir = Scratch::new();
    run(&dir.0, "setup");
    crash(&dir.0, "after_capability");
    assert!(run(&dir.0, "verify_capability").contains("CAPABILITY_OK"));
    assert!(run(&dir.0, "verify_replay").contains("REPLAY_LOST"));
}
#[test]
fn reverse_delivery_survives_group_reload() {
    let dir = Scratch::new();
    run(&dir.0, "setup");
    run(&dir.0, "reverse_order");
}
#[test]
fn tampered_ciphertext_does_not_prevent_reloaded_authentic_delivery() {
    let dir = Scratch::new();
    run(&dir.0, "setup");
    run(&dir.0, "tampered_ciphertext");
}
#[test]
fn switched_account_cannot_load_other_accounts_group_or_capabilities() {
    let dir = Scratch::new();
    run(&dir.0, "setup");
    crash(&dir.0, "after_capability");
    run(&dir.0, "wrong_account");
}
async fn tail_worker(root: &Path, phase: &str) -> anyhow::Result<()> {
    use firefly_client::{
        db::group_messages::{GroupMessage, GroupMessagesStore},
        history::{decrypt_and_unpack_chunk, pack_messages_into_chunk},
    };
    use firefly_protos::{firefly, serialize_proto};
    let pool = setup_pool_from_path(root.join("tail.sqlite").to_str().context("path")?, 1).await?;
    let kv = KeyValueStore::new(pool.clone()).await?;
    let store = GroupMessagesStore::new(pool.clone()).await?;
    let mut originals = Vec::new();
    for id in [10, 20] {
        let inner = firefly::GroupMessageInner {
            channelId: 1,
            message_type: 0,
            message: firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(
                firefly::MessagePayload {
                    text: format!("original {id}").into(),
                    ..Default::default()
                },
            ),
        };
        originals.push(GroupMessage {
            id,
            group_id: 42,
            channel_id: 1,
            by: "alice".into(),
            epoch: 1,
            message_type: 0,
            message: serialize_proto(&inner)?.to_vec(),
        });
    }
    // Exercise the real zstd/AES codec and SQLite import, not a map-based simulation.
    let packed = pack_messages_into_chunk(&originals)?;
    let records = decrypt_and_unpack_chunk(&packed.blob, &packed.key, &packed.unencrypted_hash)?;
    let inbox = "a".repeat(32);
    if phase == "tail_import_before_ack" {
        kv.enqueue_snapshot_tail(42, &inbox).await?;
        store
            .import_verified_history(42, i64::MAX as u64, &packed.unencrypted_hash, &records)
            .await?;
        ready_and_block();
    }
    if phase == "tail_fail_mid_import" {
        sqlx::query("CREATE TRIGGER reject_second BEFORE INSERT ON group_messages WHEN NEW.id=20 BEGIN SELECT RAISE(ABORT,'injected import write failure'); END").execute(&pool).await?;
        anyhow::ensure!(
            store
                .import_verified_history(42, i64::MAX as u64, &packed.unencrypted_hash, &records)
                .await
                .is_err(),
            "fault not triggered"
        );
        anyhow::ensure!(
            store.get_range(42, 1, 30).await?.is_empty(),
            "partial import escaped transaction"
        );
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM group_history_imports")
            .fetch_one(&pool)
            .await?;
        anyhow::ensure!(count == 0, "provenance committed for rolled-back data");
        sqlx::query("DROP TRIGGER reject_second")
            .execute(&pool)
            .await?;
        kv.enqueue_snapshot_tail(42, &inbox).await?;
    }
    if phase == "tail_ack_failure" {
        kv.enqueue_snapshot_tail(42, &inbox).await?;
        sqlx::query("CREATE TRIGGER reject_ack BEFORE UPDATE ON group_snapshot_inbox BEGIN SELECT RAISE(ABORT,'injected ACK failure'); END").execute(&pool).await?;
        let result = kv
            .drain_snapshot_tails(42, 0, |_| async {
                store
                    .import_verified_history(
                        42,
                        i64::MAX as u64,
                        &packed.unencrypted_hash,
                        &records,
                    )
                    .await?;
                Ok(())
            })
            .await;
        anyhow::ensure!(result.is_err(), "ACK failure not reached");
        anyhow::ensure!(
            kv.has_pending_snapshot_tails(42).await?,
            "failed ACK erased inbox"
        );
        sqlx::query("DROP TRIGGER reject_ack")
            .execute(&pool)
            .await?;
        ready_and_block();
    }
    if phase == "tail_drain" || phase == "tail_fail_mid_import" {
        let completed = kv
            .drain_snapshot_tails(42, 0, |_| async {
                store
                    .import_verified_history(
                        42,
                        i64::MAX as u64,
                        &packed.unencrypted_hash,
                        &records,
                    )
                    .await?;
                Ok(())
            })
            .await?;
        anyhow::ensure!(completed == 1, "expected one pending tail");
        anyhow::ensure!(
            !kv.has_pending_snapshot_tails(42).await?,
            "tail not acknowledged"
        );
        let imported = store.get_range(42, 1, 30).await?;
        anyhow::ensure!(imported.len() == 2, "duplicate or missing records");
        for (actual, expected) in imported.iter().zip(&originals) {
            anyhow::ensure!(
                actual.id == expected.id
                    && actual.by == expected.by
                    && actual.message == expected.message,
                "original metadata lost"
            );
        }
        anyhow::ensure!(
            store
                .authenticated_history_ids(42, &[10, 20])
                .await?
                .is_empty(),
            "adder imports became independent vote evidence"
        );
        kv.enqueue_snapshot_tail(42, &inbox).await?;
        anyhow::ensure!(
            !kv.has_pending_snapshot_tails(42).await?,
            "duplicate handoff requeued completed tail"
        );
        return Ok(());
    }
    anyhow::bail!("unknown tail phase")
}
#[test]
fn kill_after_tail_import_before_ack_replays_without_duplicate_records() {
    let dir = Scratch::new();
    crash(&dir.0, "tail_import_before_ack");
    run(&dir.0, "tail_drain");
}
#[test]
fn failed_tail_ack_then_process_restart_replays_import_safely() {
    let dir = Scratch::new();
    crash(&dir.0, "tail_ack_failure");
    run(&dir.0, "tail_drain");
}
#[test]
fn failure_mid_tail_import_rolls_back_records_and_provenance() {
    let dir = Scratch::new();
    run(&dir.0, "tail_fail_mid_import");
}
