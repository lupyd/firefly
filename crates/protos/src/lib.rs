mod protos;

use bytes::{BufMut, Bytes, BytesMut};
pub use protos::*;
use quick_protobuf::Writer;

pub const MESSAGE_TYPE_NORMAL: u32 = 0;
pub const MESSAGE_TYPE_PINNED: u32 = 1 << 0;
pub const MESSAGE_TYPE_HIDDEN: u32 = 1 << 1;
pub const MESSAGE_TYPE_HISTORY_KEYS: u32 = 1 << 2;

/// `UserMessage.settings` bits understood by the server.
/// The message targets the sender's own devices.
pub const USER_MESSAGE_SETTINGS_SELF_DEVICE: u32 = 1 << 0;
/// An automatic protocol message (session accept, hidden sync): delivering it must not count as
/// the sender accepting the recipient's message request.
pub const USER_MESSAGE_SETTINGS_PROTOCOL: u32 = 1 << 1;

pub fn deserialize_proto<'a, 'b: 'a, T: quick_protobuf::MessageRead<'a> + Sized>(
    bytes: &'b [u8],
) -> Result<T, quick_protobuf::Error> {
    T::from_reader(&mut quick_protobuf::BytesReader::from_bytes(bytes), &bytes)
}

pub fn serialize_proto<T: quick_protobuf::MessageWrite + Sized>(
    msg: &T,
) -> Result<Bytes, quick_protobuf::Error> {
    let mut writer = BytesMut::new().writer();
    msg.write_message(&mut Writer::new(&mut writer))?;
    Ok(writer.into_inner().freeze())
}

