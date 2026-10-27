use std::fmt::Display;

use bytes::Bytes;
use rand::{CryptoRng, SeedableRng};

lazy_static::lazy_static! {
    /// Without timeouts a request on a dead network (packets silently dropped) hangs for minutes,
    /// holding up everything waiting on it. The read timeout applies per read, so large
    /// downloads that keep making progress are unaffected.
    pub static ref HTTP_CLIENT: reqwest::Client = {
        #[cfg(not(target_arch = "wasm32"))]
        {
            reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(10))
                .read_timeout(std::time::Duration::from_secs(30))
                .build()
                .unwrap_or_default()
        }
        // fetch exposes no connect/read timeouts; the browser applies its own.
        #[cfg(target_arch = "wasm32")]
        {
            reqwest::Client::new()
        }
    };
}

pub use firefly_core::utils::now_system_time;

/// Client for requests whose credentials must never follow a redirect. Native builds refuse
/// redirects outright. Browsers allow no redirect control from fetch, but already strip
/// `Authorization` on cross-origin redirects, so wasm keeps the defaults.
pub fn no_redirect_client(timeout: Option<std::time::Duration>) -> reqwest::Result<reqwest::Client> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        let mut builder = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none());
        if let Some(timeout) = timeout {
            builder = builder.timeout(timeout);
        }
        builder.build()
    }
    #[cfg(target_arch = "wasm32")]
    {
        let _ = timeout;
        reqwest::Client::builder().build()
    }
}

/// Client whose requests give up after `timeout`. fetch has no client-level timeout, so the
/// browser build relies on the browser's own limits.
pub fn http_client_with_timeout(timeout: std::time::Duration) -> reqwest::Result<reqwest::Client> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        reqwest::Client::builder().timeout(timeout).build()
    }
    #[cfg(target_arch = "wasm32")]
    {
        let _ = timeout;
        reqwest::Client::builder().build()
    }
}

/// Reads a response body, failing with `too_large` once it exceeds `max` bytes. Natively the
/// read stops at the limit; the browser's fetch only exposes the complete body.
pub async fn read_body_limited(
    response: reqwest::Response,
    max: usize,
    too_large: &'static str,
) -> anyhow::Result<Vec<u8>> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        let mut response = response;
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            anyhow::ensure!(bytes.len().saturating_add(chunk.len()) <= max, too_large);
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }
    #[cfg(target_arch = "wasm32")]
    {
        let bytes = response.bytes().await?;
        anyhow::ensure!(bytes.len() <= max, too_large);
        Ok(bytes.to_vec())
    }
}

pub fn get_current_timestamp_millis_since_epoch() -> u64 {
    firefly_core::utils::get_current_timestamp_in_millis()
}

pub fn get_current_timestamp_seconds_since_epoch() -> u64 {
    firefly_core::utils::get_current_timestamp_in_secs()
}

pub fn get_current_timestamp_microseconds_since_epoch() -> u64 {
    firefly_core::utils::get_current_timestamp_in_microsecs() as u64
}

pub fn rng() -> impl CryptoRng {
    rand_chacha::ChaCha20Rng::from_os_rng()
}

pub fn deserialize_proto<'a, T: quick_protobuf::MessageRead<'a> + Sized>(
    bytes: &'a [u8],
) -> Result<T, quick_protobuf::Error> {
    firefly_protos::deserialize_proto(bytes)
}

pub fn serialize_proto<T: quick_protobuf::MessageWrite + Sized>(
    msg: &T,
) -> Result<Bytes, quick_protobuf::Error> {
    firefly_protos::serialize_proto(msg)
}

pub fn write_url_comma_seperated(
    mut w: impl std::fmt::Write,
    mut iter: impl Iterator<Item = impl Display>,
) -> Result<(), std::fmt::Error> {
    let Some(first) = iter.next() else {
        return Ok(());
    };

    write!(w, "{}", first)?;

    for cur in iter {
        write!(w, "%2C{}", cur)?;
    }

    Ok(())
}
