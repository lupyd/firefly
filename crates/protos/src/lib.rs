mod protos;

use bytes::{BufMut, Bytes, BytesMut};
pub use protos::*;
use quick_protobuf::Writer;

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
