#[cfg(not(target_arch = "wasm32"))]
use log::LevelFilter;

#[cfg(not(target_arch = "wasm32"))]
use crate::logger::TeeLogger;

use firefly_protos::firefly;

#[cfg(not(target_arch = "wasm32"))]
pub mod callbacks;

#[cfg(not(target_arch = "wasm32"))]
pub mod db;

pub mod error;

#[cfg(not(target_arch = "wasm32"))]
pub mod group;

#[cfg(not(target_arch = "wasm32"))]
pub mod logger;

pub mod schema;
pub mod utils;

#[cfg(not(target_arch = "wasm32"))]
pub mod websocket;

#[derive(Debug)]
pub struct EncryptedMessage {
    pub cipher_text: Vec<u8>,
    pub ty: u8,
}

#[derive(Clone)]
pub struct FfiPreKeyBundle {
    registration_id: u32,
    device_id: u8,
    pre_key_id: u32,
    pre_key: Vec<u8>,
    signed_pre_key_id: u32,
    signed_pre_key_public: Vec<u8>,
    signed_pre_key_signature: Vec<u8>,
    kyber_pre_key_id: u32,
    kyber_pre_key_public: Vec<u8>,
    kyber_pre_key_signature: Vec<u8>,
    identity_key: Vec<u8>,
}

impl From<firefly::PreKeyBundle<'_>> for FfiPreKeyBundle {
    fn from(bundle: firefly::PreKeyBundle<'_>) -> Self {
        Self {
            registration_id: bundle.registrationId as u32,
            device_id: bundle.deviceId as u8,
            pre_key_id: bundle.preKeyId as u32,
            pre_key: bundle.prePublicKey.to_vec(),
            signed_pre_key_id: bundle.signedPreKeyId as u32,
            signed_pre_key_public: bundle.signedPrePublicKey.to_vec(),
            signed_pre_key_signature: bundle.signedPreKeySignature.to_vec(),
            kyber_pre_key_id: bundle.KEMPreKeyId as u32,
            kyber_pre_key_public: bundle.KEMPrePublicKey.to_vec(),
            kyber_pre_key_signature: bundle.KEMPreKeySignature.to_vec(),
            identity_key: bundle.identityPublicKey.to_vec(),
        }
    }
}

impl Into<firefly::PreKeyBundle<'static>> for FfiPreKeyBundle {
    fn into(self) -> firefly::PreKeyBundle<'static> {
        firefly::PreKeyBundle {
            registrationId: self.registration_id,
            deviceId: self.device_id as u32,
            preKeyId: self.pre_key_id,
            prePublicKey: self.pre_key.into(),
            signedPreKeyId: self.signed_pre_key_id,
            signedPrePublicKey: self.signed_pre_key_public.into(),
            signedPreKeySignature: self.signed_pre_key_signature.into(),
            KEMPreKeyId: self.kyber_pre_key_id,
            KEMPrePublicKey: self.kyber_pre_key_public.into(),
            KEMPreKeySignature: self.kyber_pre_key_signature.into(),
            identityPublicKey: self.identity_key.into(),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
static INIT_LOGGING: std::sync::Once = std::sync::Once::new();

#[cfg(not(target_arch = "wasm32"))]
pub fn init_logger(file_path: String) {
    INIT_LOGGING.call_once(|| {
        init_logging(&file_path);

        set_panic_handler();
    });
}

#[cfg(not(target_arch = "wasm32"))]
fn init_logging(file_path: &str) {
    let level = LevelFilter::Info;
    let tee_logger = TeeLogger::new(file_path, level).expect("can't initaite tee logger");

    log::set_boxed_logger(Box::new(tee_logger)).expect("set logger failed");
    log::set_max_level(level);
}

#[cfg(not(target_arch = "wasm32"))]
fn set_panic_handler() {
    std::panic::set_hook(Box::new(|panic_info| {
        let backtrace = std::backtrace::Backtrace::force_capture();
        let message = match panic_info.payload().downcast_ref::<&str>() {
            Some(s) => *s,
            None => match panic_info.payload().downcast_ref::<String>() {
                Some(s) => &s[..],
                None => "Box<Any>",
            },
        };
        let thread = std::thread::current();
        let thread = thread.name().unwrap_or("<unnamed>");
        let msg = format!(
            "thread '{}' panicked at '{}', {}\n{}",
            thread,
            message,
            panic_info.location().unwrap(),
            backtrace
        );
        log::error!("{}", msg);
        std::process::abort();
    }));
}

#[cfg(not(target_arch = "wasm32"))]
pub struct FfiFileServer {
    server: tokio::sync::Mutex<shfs::FileServer>,
}

#[cfg(not(target_arch = "wasm32"))]
impl FfiFileServer {
    pub fn create(base_path: String, token: String) -> Self {
        let server = tokio::sync::Mutex::new(shfs::FileServer::new(base_path, token));

        Self { server }
    }

    pub async fn start_serving(&self, port: Option<u16>) -> anyhow::Result<u16> {
        self.server
            .lock()
            .await
            .serve(port)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    pub async fn token(&self) -> String {
        self.server.lock().await.token().to_string()
    }

    pub async fn port(&self) -> String {
        self.server.lock().await.port().to_string()
    }
}
