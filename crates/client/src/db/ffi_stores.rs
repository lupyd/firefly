use libsignal_protocol::ProtocolAddress;
use sqlx::SqlitePool;

use crate::{EncryptedMessage, FfiPreKeyBundle, db::stores::KeyStores};

pub struct FfiKeyStores {
    sender: std::sync::mpsc::Sender<Command>,
    #[allow(unused)]
    handler: std::thread::JoinHandle<()>,

    stores: KeyStores,
}

impl Drop for FfiKeyStores {
    fn drop(&mut self) {
        if self.sender.send(Command::Exit).is_err() {
            log::error!("Error sending exit command");
        }
    }
}

pub enum Command {
    Exit,
    Decrypt {
        other: ProtocolAddress,
        cipher_text: Vec<u8>,
        ty: u8,
        reply: tokio::sync::oneshot::Sender<anyhow::Result<Vec<u8>>>,
    },

    Encrypt {
        other: ProtocolAddress,
        plain_text: Vec<u8>,
        reply: tokio::sync::oneshot::Sender<anyhow::Result<EncryptedMessage>>,
    },

    ProcessPreKeyBundle {
        other: String,
        pre_key_bundle: FfiPreKeyBundle,
        reply: tokio::sync::oneshot::Sender<anyhow::Result<()>>,
    },

    GeneratePreKeyBundle {
        reply: tokio::sync::oneshot::Sender<anyhow::Result<FfiPreKeyBundle>>,
    },
}

impl FfiKeyStores {
    pub fn store(&self) -> &KeyStores {
        &self.stores
    }
}

impl FfiKeyStores {
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        let stores = KeyStores::new(pool).await?;

        let (sender, receiver) = std::sync::mpsc::channel::<Command>();
        let s = stores.clone();
        let handler = std::thread::spawn(move || {
            let mut stores = s.clone();
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                while let Ok(cmd) = receiver.recv() {
                    match cmd {
                        Command::Decrypt {
                            other,
                            cipher_text,
                            ty,
                            reply,
                        } => {
                            if let Err(err) = reply.send(stores.decrypt(other, cipher_text, ty).await) {
                                log::error!("Error sending reply: {:?}", err);
                            }
                        }
                        Command::Encrypt {
                            other,
                            plain_text,
                            reply,
                        } => {
                            if let Err(err) = reply.send(stores.encrypt(other, plain_text).await) {
                                log::error!("Error sending reply: {:?}", err);
                            }
                        }

                        Command::ProcessPreKeyBundle {
                            other,
                            pre_key_bundle,
                            reply,
                        } => {
                            if let Err(err) =
                                reply.send(stores.process_pre_key_bundle(other, pre_key_bundle).await)
                            {
                                log::error!("Error sending reply: {:?}", err);
                            }
                        }

                        Command::GeneratePreKeyBundle { reply } => {
                            if let Err(_) = reply.send(stores.generate_prekey_bundle().await) {
                                log::error!("Error sending Generated Pre Key Bundle");
                            }
                        }

                        Command::Exit => {
                            break;
                        }
                    }
                }
            });
        });

        Ok(Self {
            sender,
            handler,
            stores,
        })
    }
}

impl FfiKeyStores {
    pub async fn decrypt(
        &self,
        other: ProtocolAddress,
        cipher_text: Vec<u8>,
        ty: u8,
    ) -> anyhow::Result<Vec<u8>> {
        let (reply, receiver) = tokio::sync::oneshot::channel::<anyhow::Result<Vec<u8>>>();
        self.sender.send(Command::Decrypt {
            other,
            cipher_text,
            ty,
            reply,
        })?;
        let decrypted = receiver.await??;
        Ok(decrypted)
    }

    pub async fn encrypt(
        &self,
        other: ProtocolAddress,
        plain_text: Vec<u8>,
    ) -> anyhow::Result<EncryptedMessage> {
        let (reply, receiver) =
            tokio::sync::oneshot::channel::<anyhow::Result<EncryptedMessage>>();
        self.sender.send(Command::Encrypt {
            other,
            plain_text,
            reply,
        })?;
        let encrypted = receiver.await??;
        Ok(encrypted)
    }

    pub async fn process_pre_key_bundle(
        &self,
        other: String,
        pre_key_bundle: FfiPreKeyBundle,
    ) -> anyhow::Result<()> {
        let (reply, receiver) = tokio::sync::oneshot::channel::<anyhow::Result<()>>();
        self.sender.send(Command::ProcessPreKeyBundle {
            other,
            pre_key_bundle,
            reply,
        })?;
        let processed = receiver.await??;
        Ok(processed)
    }

    pub async fn generate_prekey_bundle(&self) -> anyhow::Result<FfiPreKeyBundle> {
        let (reply, receiver) =
            tokio::sync::oneshot::channel::<anyhow::Result<FfiPreKeyBundle>>();
        self.sender.send(Command::GeneratePreKeyBundle { reply })?;
        let bundle = receiver.await??;
        Ok(bundle)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::setup_pool;
    use libsignal_protocol::{DeviceId, ProtocolAddress};

    const DB_URI: &str = ":memory:";

    async fn test_ffi_encryption(
        user1: &FfiKeyStores,
        user1_name: &str,
        user2: &FfiKeyStores,
        user2_name: &str,
        user2_pre_key_bundle: FfiPreKeyBundle,
    ) -> anyhow::Result<()> {
        let bob_device_id = user2
            .store()
            .identity_store
            .get_full_identity_key_pair()
            .await
            .unwrap()
            .device_id;
        let alice_device_id = user1
            .store()
            .identity_store
            .get_full_identity_key_pair()
            .await
            .unwrap()
            .device_id;

        let bob_address = ProtocolAddress::new(
            user2_name.to_string(),
            DeviceId::new(bob_device_id).unwrap(),
        );
        let alice_address = ProtocolAddress::new(
            user1_name.to_string(),
            DeviceId::new(alice_device_id).unwrap(),
        );

        user1
            .process_pre_key_bundle(user2_name.to_string(), user2_pre_key_bundle)
            .await?;

        let msg1 = user1
            .encrypt(bob_address.clone(), b"Hello Bob".to_vec())
            .await?;
        println!("msg1: len={}, ty={}", msg1.cipher_text.len(), msg1.ty);
        let decrypted1 = user2
            .decrypt(alice_address.clone(), msg1.cipher_text, msg1.ty)
            .await?;
        assert_eq!(decrypted1, b"Hello Bob");

        let msg2 = user2
            .encrypt(alice_address.clone(), b"Hi Alice".to_vec())
            .await?;
        println!("msg2: len={}, ty={}", msg2.cipher_text.len(), msg2.ty);
        let decrypted2 = user1
            .decrypt(bob_address.clone(), msg2.cipher_text, msg2.ty)
            .await?;
        assert_eq!(decrypted2, b"Hi Alice");

        let msg3 = user1
            .encrypt(bob_address.clone(), b"How are you?".to_vec())
            .await?;
        println!("msg3: len={}, ty={}", msg3.cipher_text.len(), msg3.ty);
        let decrypted3 = user2
            .decrypt(alice_address.clone(), msg3.cipher_text, msg3.ty)
            .await?;
        assert_eq!(decrypted3, b"How are you?");

        Ok(())
    }

    #[tokio::test]
    async fn test_ffi_alice_bob_encryption() {
        env_logger::Builder::from_default_env()
            .format(|buf, record| {
                use std::io::Write;
                writeln!(
                    buf,
                    "[{} {}:{}] {}",
                    record.level(),
                    record.file().unwrap_or("?"),
                    record.line().unwrap_or(0),
                    record.args()
                )
            })
            .init();

        let alice_pool = setup_pool(DB_URI, 1).await.unwrap();
        let bob_pool = setup_pool(DB_URI, 1).await.unwrap();
        let charles_pool = setup_pool(DB_URI, 1).await.unwrap();

        let alice = FfiKeyStores::new(alice_pool).await.unwrap();
        let bob = FfiKeyStores::new(bob_pool).await.unwrap();
        let charles = FfiKeyStores::new(charles_pool).await.unwrap();

        let bob_bundle = bob.generate_prekey_bundle().await.unwrap();
        let bob_bundle2 = bob.generate_prekey_bundle().await.unwrap();

        test_ffi_encryption(&charles, "charles", &bob, "bob", bob_bundle)
            .await
            .unwrap();
        test_ffi_encryption(&alice, "alice", &bob, "bob", bob_bundle2)
            .await
            .unwrap();
    }
}
