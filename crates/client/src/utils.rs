use std::{
    fmt::Display,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use rand::{CryptoRng, SeedableRng};

lazy_static::lazy_static! {
    pub static ref HTTP_CLIENT: reqwest::Client = reqwest::Client::new();
}

pub fn get_current_timestamp_millis_since_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("TIME WENT BACKWARDS")
        .as_millis() as u64
}

pub fn get_current_timestamp_seconds_since_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("TIME WENT BACKWARDS")
        .as_secs()
}

pub fn get_current_timestamp_microseconds_since_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("TIME WENT BACKWARDS")
        .as_micros() as u64
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
