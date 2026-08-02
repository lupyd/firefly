// Automatically generated rust module for 'message.proto' file

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_imports)]
#![allow(unknown_lints)]
#![allow(clippy::all)]
#![cfg_attr(rustfmt, rustfmt_skip)]


use std::borrow::Cow;
use quick_protobuf::{MessageInfo, MessageRead, MessageWrite, BytesReader, Writer, WriterBackend, Result};
use quick_protobuf::sizeofs::*;
use super::*;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CallSignalType {
    CALL_REQUEST = 0,
    CALL_ANSWER = 1,
    CALL_REJECT = 2,
    CALL_CANCEL = 3,
    CALL_HANGUP = 4,
    CALL_DISMISS = 5,
    CALL_ICECANDIDATE = 6,
}

impl Default for CallSignalType {
    fn default() -> Self {
        CallSignalType::CALL_REQUEST
    }
}

impl From<i32> for CallSignalType {
    fn from(i: i32) -> Self {
        match i {
            0 => CallSignalType::CALL_REQUEST,
            1 => CallSignalType::CALL_ANSWER,
            2 => CallSignalType::CALL_REJECT,
            3 => CallSignalType::CALL_CANCEL,
            4 => CallSignalType::CALL_HANGUP,
            5 => CallSignalType::CALL_DISMISS,
            6 => CallSignalType::CALL_ICECANDIDATE,
            _ => Self::default(),
        }
    }
}

impl<'a> From<&'a str> for CallSignalType {
    fn from(s: &'a str) -> Self {
        match s {
            "CALL_REQUEST" => CallSignalType::CALL_REQUEST,
            "CALL_ANSWER" => CallSignalType::CALL_ANSWER,
            "CALL_REJECT" => CallSignalType::CALL_REJECT,
            "CALL_CANCEL" => CallSignalType::CALL_CANCEL,
            "CALL_HANGUP" => CallSignalType::CALL_HANGUP,
            "CALL_DISMISS" => CallSignalType::CALL_DISMISS,
            "CALL_ICECANDIDATE" => CallSignalType::CALL_ICECANDIDATE,
            _ => Self::default(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CallMessageType {
    none = 0,
    request = 1,
    reject = 2,
    end = 3,
    ended = 4,
    rejected = 5,
    candidate = 10,
    answer = 11,
    offer = 12,
}

impl Default for CallMessageType {
    fn default() -> Self {
        CallMessageType::none
    }
}

impl From<i32> for CallMessageType {
    fn from(i: i32) -> Self {
        match i {
            0 => CallMessageType::none,
            1 => CallMessageType::request,
            2 => CallMessageType::reject,
            3 => CallMessageType::end,
            4 => CallMessageType::ended,
            5 => CallMessageType::rejected,
            10 => CallMessageType::candidate,
            11 => CallMessageType::answer,
            12 => CallMessageType::offer,
            _ => Self::default(),
        }
    }
}

impl<'a> From<&'a str> for CallMessageType {
    fn from(s: &'a str) -> Self {
        match s {
            "none" => CallMessageType::none,
            "request" => CallMessageType::request,
            "reject" => CallMessageType::reject,
            "end" => CallMessageType::end,
            "ended" => CallMessageType::ended,
            "rejected" => CallMessageType::rejected,
            "candidate" => CallMessageType::candidate,
            "answer" => CallMessageType::answer,
            "offer" => CallMessageType::offer,
            _ => Self::default(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum MeetingSessionStatus {
    MEETING_STATUS_ACTIVE = 0,
    MEETING_STATUS_ENDED = 1,
}

impl Default for MeetingSessionStatus {
    fn default() -> Self {
        MeetingSessionStatus::MEETING_STATUS_ACTIVE
    }
}

impl From<i32> for MeetingSessionStatus {
    fn from(i: i32) -> Self {
        match i {
            0 => MeetingSessionStatus::MEETING_STATUS_ACTIVE,
            1 => MeetingSessionStatus::MEETING_STATUS_ENDED,
            _ => Self::default(),
        }
    }
}

impl<'a> From<&'a str> for MeetingSessionStatus {
    fn from(s: &'a str) -> Self {
        match s {
            "MEETING_STATUS_ACTIVE" => MeetingSessionStatus::MEETING_STATUS_ACTIVE,
            "MEETING_STATUS_ENDED" => MeetingSessionStatus::MEETING_STATUS_ENDED,
            _ => Self::default(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum MeetingSignalType {
    MEETING_SIGNAL_STARTED = 0,
    MEETING_SIGNAL_JOINED = 1,
    MEETING_SIGNAL_LEFT = 2,
    MEETING_SIGNAL_ENDED = 3,
}

impl Default for MeetingSignalType {
    fn default() -> Self {
        MeetingSignalType::MEETING_SIGNAL_STARTED
    }
}

impl From<i32> for MeetingSignalType {
    fn from(i: i32) -> Self {
        match i {
            0 => MeetingSignalType::MEETING_SIGNAL_STARTED,
            1 => MeetingSignalType::MEETING_SIGNAL_JOINED,
            2 => MeetingSignalType::MEETING_SIGNAL_LEFT,
            3 => MeetingSignalType::MEETING_SIGNAL_ENDED,
            _ => Self::default(),
        }
    }
}

impl<'a> From<&'a str> for MeetingSignalType {
    fn from(s: &'a str) -> Self {
        match s {
            "MEETING_SIGNAL_STARTED" => MeetingSignalType::MEETING_SIGNAL_STARTED,
            "MEETING_SIGNAL_JOINED" => MeetingSignalType::MEETING_SIGNAL_JOINED,
            "MEETING_SIGNAL_LEFT" => MeetingSignalType::MEETING_SIGNAL_LEFT,
            "MEETING_SIGNAL_ENDED" => MeetingSignalType::MEETING_SIGNAL_ENDED,
            _ => Self::default(),
        }
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct UserMessage<'a> {
    pub id: u64,
    pub toId: u64,
    pub fromId: u64,
    pub text: Cow<'a, [u8]>,
    pub type_pb: u32,
    pub settings: u32,
    pub hashValue: u64,
    pub fromUsername: Cow<'a, str>,
    pub fromDeviceId: u32,
}

impl<'a> MessageRead<'a> for UserMessage<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(9) => msg.id = r.read_fixed64(bytes)?,
                Ok(16) => msg.toId = r.read_uint64(bytes)?,
                Ok(24) => msg.fromId = r.read_uint64(bytes)?,
                Ok(34) => msg.text = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(48) => msg.type_pb = r.read_uint32(bytes)?,
                Ok(56) => msg.settings = r.read_uint32(bytes)?,
                Ok(81) => msg.hashValue = r.read_fixed64(bytes)?,
                Ok(66) => msg.fromUsername = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(72) => msg.fromDeviceId = r.read_uint32(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for UserMessage<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.id == 0u64 { 0 } else { 1 + 8 }
        + if self.toId == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.toId) as u64) }
        + if self.fromId == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.fromId) as u64) }
        + if self.text == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.text).len()) }
        + if self.type_pb == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.type_pb) as u64) }
        + if self.settings == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.settings) as u64) }
        + if self.hashValue == 0u64 { 0 } else { 1 + 8 }
        + if self.fromUsername == "" { 0 } else { 1 + sizeof_len((&self.fromUsername).len()) }
        + if self.fromDeviceId == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.fromDeviceId) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.id != 0u64 { w.write_with_tag(9, |w| w.write_fixed64(*&self.id))?; }
        if self.toId != 0u64 { w.write_with_tag(16, |w| w.write_uint64(*&self.toId))?; }
        if self.fromId != 0u64 { w.write_with_tag(24, |w| w.write_uint64(*&self.fromId))?; }
        if self.text != Cow::Borrowed(b"") { w.write_with_tag(34, |w| w.write_bytes(&**&self.text))?; }
        if self.type_pb != 0u32 { w.write_with_tag(48, |w| w.write_uint32(*&self.type_pb))?; }
        if self.settings != 0u32 { w.write_with_tag(56, |w| w.write_uint32(*&self.settings))?; }
        if self.hashValue != 0u64 { w.write_with_tag(81, |w| w.write_fixed64(*&self.hashValue))?; }
        if self.fromUsername != "" { w.write_with_tag(66, |w| w.write_string(&**&self.fromUsername))?; }
        if self.fromDeviceId != 0u32 { w.write_with_tag(72, |w| w.write_uint32(*&self.fromDeviceId))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct Group<'a> {
    pub id: u64,
    pub name: Cow<'a, str>,
    pub description: Cow<'a, str>,
    pub state: Cow<'a, [u8]>,
    pub settings: u32,
    pub upgraded: bool,
    pub pending: bool,
    pub owner: Cow<'a, str>,
}

impl<'a> MessageRead<'a> for Group<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(9) => msg.id = r.read_fixed64(bytes)?,
                Ok(18) => msg.name = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(26) => msg.description = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(42) => msg.state = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(48) => msg.settings = r.read_uint32(bytes)?,
                Ok(56) => msg.upgraded = r.read_bool(bytes)?,
                Ok(64) => msg.pending = r.read_bool(bytes)?,
                Ok(74) => msg.owner = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for Group<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.id == 0u64 { 0 } else { 1 + 8 }
        + if self.name == "" { 0 } else { 1 + sizeof_len((&self.name).len()) }
        + if self.description == "" { 0 } else { 1 + sizeof_len((&self.description).len()) }
        + if self.state == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.state).len()) }
        + if self.settings == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.settings) as u64) }
        + if self.upgraded == false { 0 } else { 1 + sizeof_varint(*(&self.upgraded) as u64) }
        + if self.pending == false { 0 } else { 1 + sizeof_varint(*(&self.pending) as u64) }
        + if self.owner == "" { 0 } else { 1 + sizeof_len((&self.owner).len()) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.id != 0u64 { w.write_with_tag(9, |w| w.write_fixed64(*&self.id))?; }
        if self.name != "" { w.write_with_tag(18, |w| w.write_string(&**&self.name))?; }
        if self.description != "" { w.write_with_tag(26, |w| w.write_string(&**&self.description))?; }
        if self.state != Cow::Borrowed(b"") { w.write_with_tag(42, |w| w.write_bytes(&**&self.state))?; }
        if self.settings != 0u32 { w.write_with_tag(48, |w| w.write_uint32(*&self.settings))?; }
        if self.upgraded != false { w.write_with_tag(56, |w| w.write_bool(*&self.upgraded))?; }
        if self.pending != false { w.write_with_tag(64, |w| w.write_bool(*&self.pending))?; }
        if self.owner != "" { w.write_with_tag(74, |w| w.write_string(&**&self.owner))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct Groups<'a> {
    pub groups: Vec<firefly::Group<'a>>,
}

impl<'a> MessageRead<'a> for Groups<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.groups.push(r.read_message::<firefly::Group>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for Groups<'a> {
    fn get_size(&self) -> usize {
        0
        + self.groups.iter().map(|s| 1 + sizeof_len((s).get_size())).sum::<usize>()
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        for s in &self.groups { w.write_with_tag(10, |w| w.write_message(s))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct UserMessages<'a> {
    pub messages: Vec<firefly::UserMessage<'a>>,
}

impl<'a> MessageRead<'a> for UserMessages<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.messages.push(r.read_message::<firefly::UserMessage>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for UserMessages<'a> {
    fn get_size(&self) -> usize {
        0
        + self.messages.iter().map(|s| 1 + sizeof_len((s).get_size())).sum::<usize>()
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        for s in &self.messages { w.write_with_tag(10, |w| w.write_message(s))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupInvite<'a> {
    pub groupId: u64,
    pub inviter: Cow<'a, str>,
    pub invitee: Cow<'a, str>,
    pub welcomeMessage: Cow<'a, [u8]>,
    pub commitId: u64,
}

impl<'a> MessageRead<'a> for GroupInvite<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.groupId = r.read_uint64(bytes)?,
                Ok(18) => msg.inviter = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(26) => msg.invitee = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(34) => msg.welcomeMessage = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(41) => msg.commitId = r.read_fixed64(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for GroupInvite<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.groupId == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.groupId) as u64) }
        + if self.inviter == "" { 0 } else { 1 + sizeof_len((&self.inviter).len()) }
        + if self.invitee == "" { 0 } else { 1 + sizeof_len((&self.invitee).len()) }
        + if self.welcomeMessage == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.welcomeMessage).len()) }
        + if self.commitId == 0u64 { 0 } else { 1 + 8 }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.groupId != 0u64 { w.write_with_tag(8, |w| w.write_uint64(*&self.groupId))?; }
        if self.inviter != "" { w.write_with_tag(18, |w| w.write_string(&**&self.inviter))?; }
        if self.invitee != "" { w.write_with_tag(26, |w| w.write_string(&**&self.invitee))?; }
        if self.welcomeMessage != Cow::Borrowed(b"") { w.write_with_tag(34, |w| w.write_bytes(&**&self.welcomeMessage))?; }
        if self.commitId != 0u64 { w.write_with_tag(41, |w| w.write_fixed64(*&self.commitId))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupCommitAndWelcome<'a> {
    pub id: u64,
    pub groupId: u64,
    pub commitMessage: Cow<'a, [u8]>,
    pub inviter: Cow<'a, str>,
    pub invitee: Cow<'a, str>,
    pub welcomeMessages: Vec<Cow<'a, [u8]>>,
    pub inviteeAddresses: Vec<u64>,
}

impl<'a> MessageRead<'a> for GroupCommitAndWelcome<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(9) => msg.id = r.read_fixed64(bytes)?,
                Ok(16) => msg.groupId = r.read_uint64(bytes)?,
                Ok(26) => msg.commitMessage = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(34) => msg.inviter = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(42) => msg.invitee = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(50) => msg.welcomeMessages.push(r.read_bytes(bytes).map(Cow::Borrowed)?),
                Ok(58) => msg.inviteeAddresses = r.read_packed(bytes, |r, bytes| Ok(r.read_uint64(bytes)?))?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for GroupCommitAndWelcome<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.id == 0u64 { 0 } else { 1 + 8 }
        + if self.groupId == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.groupId) as u64) }
        + if self.commitMessage == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.commitMessage).len()) }
        + if self.inviter == "" { 0 } else { 1 + sizeof_len((&self.inviter).len()) }
        + if self.invitee == "" { 0 } else { 1 + sizeof_len((&self.invitee).len()) }
        + self.welcomeMessages.iter().map(|s| 1 + sizeof_len((s).len())).sum::<usize>()
        + if self.inviteeAddresses.is_empty() { 0 } else { 1 + sizeof_len(self.inviteeAddresses.iter().map(|s| sizeof_varint(*(s) as u64)).sum::<usize>()) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.id != 0u64 { w.write_with_tag(9, |w| w.write_fixed64(*&self.id))?; }
        if self.groupId != 0u64 { w.write_with_tag(16, |w| w.write_uint64(*&self.groupId))?; }
        if self.commitMessage != Cow::Borrowed(b"") { w.write_with_tag(26, |w| w.write_bytes(&**&self.commitMessage))?; }
        if self.inviter != "" { w.write_with_tag(34, |w| w.write_string(&**&self.inviter))?; }
        if self.invitee != "" { w.write_with_tag(42, |w| w.write_string(&**&self.invitee))?; }
        for s in &self.welcomeMessages { w.write_with_tag(50, |w| w.write_bytes(&**s))?; }
        w.write_packed_with_tag(58, &self.inviteeAddresses, |w, m| w.write_uint64(*m), &|m| sizeof_varint(*(m) as u64))?;
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupInvites<'a> {
    pub invites: Vec<firefly::GroupInvite<'a>>,
}

impl<'a> MessageRead<'a> for GroupInvites<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.invites.push(r.read_message::<firefly::GroupInvite>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for GroupInvites<'a> {
    fn get_size(&self) -> usize {
        0
        + self.invites.iter().map(|s| 1 + sizeof_len((s).get_size())).sum::<usize>()
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        for s in &self.invites { w.write_with_tag(10, |w| w.write_message(s))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupMessage<'a> {
    pub id: u64,
    pub groupId: u64,
    pub message: Cow<'a, [u8]>,
    pub epoch: u32,
}

impl<'a> MessageRead<'a> for GroupMessage<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(9) => msg.id = r.read_fixed64(bytes)?,
                Ok(16) => msg.groupId = r.read_uint64(bytes)?,
                Ok(26) => msg.message = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(32) => msg.epoch = r.read_uint32(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for GroupMessage<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.id == 0u64 { 0 } else { 1 + 8 }
        + if self.groupId == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.groupId) as u64) }
        + if self.message == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.message).len()) }
        + if self.epoch == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.epoch) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.id != 0u64 { w.write_with_tag(9, |w| w.write_fixed64(*&self.id))?; }
        if self.groupId != 0u64 { w.write_with_tag(16, |w| w.write_uint64(*&self.groupId))?; }
        if self.message != Cow::Borrowed(b"") { w.write_with_tag(26, |w| w.write_bytes(&**&self.message))?; }
        if self.epoch != 0u32 { w.write_with_tag(32, |w| w.write_uint32(*&self.epoch))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupKeyPackage<'a> {
    pub address: u64,
    pub package: Cow<'a, [u8]>,
    pub username: Cow<'a, str>,
    pub id: i32,
}

impl<'a> MessageRead<'a> for GroupKeyPackage<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(24) => msg.address = r.read_uint64(bytes)?,
                Ok(18) => msg.package = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(34) => msg.username = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(8) => msg.id = r.read_int32(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for GroupKeyPackage<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.address == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.address) as u64) }
        + if self.package == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.package).len()) }
        + if self.username == "" { 0 } else { 1 + sizeof_len((&self.username).len()) }
        + if self.id == 0i32 { 0 } else { 1 + sizeof_varint(*(&self.id) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.address != 0u64 { w.write_with_tag(24, |w| w.write_uint64(*&self.address))?; }
        if self.package != Cow::Borrowed(b"") { w.write_with_tag(18, |w| w.write_bytes(&**&self.package))?; }
        if self.username != "" { w.write_with_tag(34, |w| w.write_string(&**&self.username))?; }
        if self.id != 0i32 { w.write_with_tag(8, |w| w.write_int32(*&self.id))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupKeyPackages<'a> {
    pub packages: Vec<firefly::GroupKeyPackage<'a>>,
}

impl<'a> MessageRead<'a> for GroupKeyPackages<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.packages.push(r.read_message::<firefly::GroupKeyPackage>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for GroupKeyPackages<'a> {
    fn get_size(&self) -> usize {
        0
        + self.packages.iter().map(|s| 1 + sizeof_len((s).get_size())).sum::<usize>()
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        for s in &self.packages { w.write_with_tag(10, |w| w.write_message(s))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupMessages<'a> {
    pub messages: Vec<firefly::GroupMessage<'a>>,
}

impl<'a> MessageRead<'a> for GroupMessages<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(18) => msg.messages.push(r.read_message::<firefly::GroupMessage>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for GroupMessages<'a> {
    fn get_size(&self) -> usize {
        0
        + self.messages.iter().map(|s| 1 + sizeof_len((s).get_size())).sum::<usize>()
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        for s in &self.messages { w.write_with_tag(18, |w| w.write_message(s))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupSyncRequest {
    pub group_id: u64,
    pub start_after: u64,
    pub until: u64,
    pub limit: u32,
}

impl<'a> MessageRead<'a> for GroupSyncRequest {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.group_id = r.read_uint64(bytes)?,
                Ok(17) => msg.start_after = r.read_fixed64(bytes)?,
                Ok(25) => msg.until = r.read_fixed64(bytes)?,
                Ok(32) => msg.limit = r.read_uint32(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl MessageWrite for GroupSyncRequest {
    fn get_size(&self) -> usize {
        0
        + if self.group_id == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.group_id) as u64) }
        + if self.start_after == 0u64 { 0 } else { 1 + 8 }
        + if self.until == 0u64 { 0 } else { 1 + 8 }
        + if self.limit == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.limit) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.group_id != 0u64 { w.write_with_tag(8, |w| w.write_uint64(*&self.group_id))?; }
        if self.start_after != 0u64 { w.write_with_tag(17, |w| w.write_fixed64(*&self.start_after))?; }
        if self.until != 0u64 { w.write_with_tag(25, |w| w.write_fixed64(*&self.until))?; }
        if self.limit != 0u32 { w.write_with_tag(32, |w| w.write_uint32(*&self.limit))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupSyncRequests {
    pub requests: Vec<firefly::GroupSyncRequest>,
}

impl<'a> MessageRead<'a> for GroupSyncRequests {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.requests.push(r.read_message::<firefly::GroupSyncRequest>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl MessageWrite for GroupSyncRequests {
    fn get_size(&self) -> usize {
        0
        + self.requests.iter().map(|s| 1 + sizeof_len((s).get_size())).sum::<usize>()
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        for s in &self.requests { w.write_with_tag(10, |w| w.write_message(s))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupMemberUpdate {
    pub group_id: u64,
    pub last_message_seen: u64,
    pub last_epoch: u32,
}

impl<'a> MessageRead<'a> for GroupMemberUpdate {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.group_id = r.read_uint64(bytes)?,
                Ok(17) => msg.last_message_seen = r.read_fixed64(bytes)?,
                Ok(24) => msg.last_epoch = r.read_uint32(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl MessageWrite for GroupMemberUpdate {
    fn get_size(&self) -> usize {
        0
        + if self.group_id == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.group_id) as u64) }
        + if self.last_message_seen == 0u64 { 0 } else { 1 + 8 }
        + if self.last_epoch == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.last_epoch) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.group_id != 0u64 { w.write_with_tag(8, |w| w.write_uint64(*&self.group_id))?; }
        if self.last_message_seen != 0u64 { w.write_with_tag(17, |w| w.write_fixed64(*&self.last_message_seen))?; }
        if self.last_epoch != 0u32 { w.write_with_tag(24, |w| w.write_uint32(*&self.last_epoch))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupMemberUpdates {
    pub updates: Vec<firefly::GroupMemberUpdate>,
}

impl<'a> MessageRead<'a> for GroupMemberUpdates {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.updates.push(r.read_message::<firefly::GroupMemberUpdate>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl MessageWrite for GroupMemberUpdates {
    fn get_size(&self) -> usize {
        0
        + self.updates.iter().map(|s| 1 + sizeof_len((s).get_size())).sum::<usize>()
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        for s in &self.updates { w.write_with_tag(10, |w| w.write_message(s))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupCommit<'a> {
    pub id: u64,
    pub groupId: u64,
    pub commit: Cow<'a, [u8]>,
    pub epoch: u32,
}

impl<'a> MessageRead<'a> for GroupCommit<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(9) => msg.id = r.read_fixed64(bytes)?,
                Ok(16) => msg.groupId = r.read_uint64(bytes)?,
                Ok(34) => msg.commit = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(24) => msg.epoch = r.read_uint32(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for GroupCommit<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.id == 0u64 { 0 } else { 1 + 8 }
        + if self.groupId == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.groupId) as u64) }
        + if self.commit == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.commit).len()) }
        + if self.epoch == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.epoch) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.id != 0u64 { w.write_with_tag(9, |w| w.write_fixed64(*&self.id))?; }
        if self.groupId != 0u64 { w.write_with_tag(16, |w| w.write_uint64(*&self.groupId))?; }
        if self.commit != Cow::Borrowed(b"") { w.write_with_tag(34, |w| w.write_bytes(&**&self.commit))?; }
        if self.epoch != 0u32 { w.write_with_tag(24, |w| w.write_uint32(*&self.epoch))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupCommits<'a> {
    pub commits: Vec<firefly::GroupCommit<'a>>,
}

impl<'a> MessageRead<'a> for GroupCommits<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.commits.push(r.read_message::<firefly::GroupCommit>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for GroupCommits<'a> {
    fn get_size(&self) -> usize {
        0
        + self.commits.iter().map(|s| 1 + sizeof_len((s).get_size())).sum::<usize>()
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        for s in &self.commits { w.write_with_tag(10, |w| w.write_message(s))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupCommitSyncRequest {
    pub group_id: u64,
    pub epoch: u32,
}

impl<'a> MessageRead<'a> for GroupCommitSyncRequest {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.group_id = r.read_uint64(bytes)?,
                Ok(16) => msg.epoch = r.read_uint32(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl MessageWrite for GroupCommitSyncRequest {
    fn get_size(&self) -> usize {
        0
        + if self.group_id == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.group_id) as u64) }
        + if self.epoch == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.epoch) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.group_id != 0u64 { w.write_with_tag(8, |w| w.write_uint64(*&self.group_id))?; }
        if self.epoch != 0u32 { w.write_with_tag(16, |w| w.write_uint32(*&self.epoch))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupReAddRequest<'a> {
    pub group_id: u64,
    pub address_id: u64,
    pub username: Cow<'a, str>,
}

impl<'a> MessageRead<'a> for GroupReAddRequest<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.group_id = r.read_uint64(bytes)?,
                Ok(16) => msg.address_id = r.read_uint64(bytes)?,
                Ok(26) => msg.username = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for GroupReAddRequest<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.group_id == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.group_id) as u64) }
        + if self.address_id == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.address_id) as u64) }
        + if self.username == "" { 0 } else { 1 + sizeof_len((&self.username).len()) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.group_id != 0u64 { w.write_with_tag(8, |w| w.write_uint64(*&self.group_id))?; }
        if self.address_id != 0u64 { w.write_with_tag(16, |w| w.write_uint64(*&self.address_id))?; }
        if self.username != "" { w.write_with_tag(26, |w| w.write_string(&**&self.username))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupReAddRequests<'a> {
    pub requests: Vec<firefly::GroupReAddRequest<'a>>,
}

impl<'a> MessageRead<'a> for GroupReAddRequests<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.requests.push(r.read_message::<firefly::GroupReAddRequest>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for GroupReAddRequests<'a> {
    fn get_size(&self) -> usize {
        0
        + self.requests.iter().map(|s| 1 + sizeof_len((s).get_size())).sum::<usize>()
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        for s in &self.requests { w.write_with_tag(10, |w| w.write_message(s))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupMemberOnlineStatus<'a> {
    pub address_id: u64,
    pub username: Cow<'a, str>,
    pub device_id: u32,
    pub last_connected_at: u64,
    pub is_online: bool,
}

impl<'a> MessageRead<'a> for GroupMemberOnlineStatus<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.address_id = r.read_uint64(bytes)?,
                Ok(18) => msg.username = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(24) => msg.device_id = r.read_uint32(bytes)?,
                Ok(32) => msg.last_connected_at = r.read_uint64(bytes)?,
                Ok(40) => msg.is_online = r.read_bool(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for GroupMemberOnlineStatus<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.address_id == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.address_id) as u64) }
        + if self.username == "" { 0 } else { 1 + sizeof_len((&self.username).len()) }
        + if self.device_id == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.device_id) as u64) }
        + if self.last_connected_at == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.last_connected_at) as u64) }
        + if self.is_online == false { 0 } else { 1 + sizeof_varint(*(&self.is_online) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.address_id != 0u64 { w.write_with_tag(8, |w| w.write_uint64(*&self.address_id))?; }
        if self.username != "" { w.write_with_tag(18, |w| w.write_string(&**&self.username))?; }
        if self.device_id != 0u32 { w.write_with_tag(24, |w| w.write_uint32(*&self.device_id))?; }
        if self.last_connected_at != 0u64 { w.write_with_tag(32, |w| w.write_uint64(*&self.last_connected_at))?; }
        if self.is_online != false { w.write_with_tag(40, |w| w.write_bool(*&self.is_online))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupMembersOnlineStatus<'a> {
    pub members: Vec<firefly::GroupMemberOnlineStatus<'a>>,
}

impl<'a> MessageRead<'a> for GroupMembersOnlineStatus<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.members.push(r.read_message::<firefly::GroupMemberOnlineStatus>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for GroupMembersOnlineStatus<'a> {
    fn get_size(&self) -> usize {
        0
        + self.members.iter().map(|s| 1 + sizeof_len((s).get_size())).sum::<usize>()
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        for s in &self.members { w.write_with_tag(10, |w| w.write_message(s))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct Error<'a> {
    pub error: Cow<'a, str>,
    pub errorCode: u32,
}

impl<'a> MessageRead<'a> for Error<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(18) => msg.error = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(8) => msg.errorCode = r.read_uint32(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for Error<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.error == "" { 0 } else { 1 + sizeof_len((&self.error).len()) }
        + if self.errorCode == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.errorCode) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.error != "" { w.write_with_tag(18, |w| w.write_string(&**&self.error))?; }
        if self.errorCode != 0u32 { w.write_with_tag(8, |w| w.write_uint32(*&self.errorCode))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct Result_pb<'a> {
    pub body: Cow<'a, [u8]>,
    pub resultCode: u32,
}

impl<'a> MessageRead<'a> for Result_pb<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(18) => msg.body = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(8) => msg.resultCode = r.read_uint32(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for Result_pb<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.body == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.body).len()) }
        + if self.resultCode == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.resultCode) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.body != Cow::Borrowed(b"") { w.write_with_tag(18, |w| w.write_bytes(&**&self.body))?; }
        if self.resultCode != 0u32 { w.write_with_tag(8, |w| w.write_uint32(*&self.resultCode))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct Address<'a> {
    pub id: u64,
    pub username: Cow<'a, str>,
    pub fcmToken: Cow<'a, str>,
    pub deviceId: u32,
}

impl<'a> MessageRead<'a> for Address<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.id = r.read_uint64(bytes)?,
                Ok(18) => msg.username = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(34) => msg.fcmToken = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(24) => msg.deviceId = r.read_uint32(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for Address<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.id == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.id) as u64) }
        + if self.username == "" { 0 } else { 1 + sizeof_len((&self.username).len()) }
        + if self.fcmToken == "" { 0 } else { 1 + sizeof_len((&self.fcmToken).len()) }
        + if self.deviceId == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.deviceId) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.id != 0u64 { w.write_with_tag(8, |w| w.write_uint64(*&self.id))?; }
        if self.username != "" { w.write_with_tag(18, |w| w.write_string(&**&self.username))?; }
        if self.fcmToken != "" { w.write_with_tag(34, |w| w.write_string(&**&self.fcmToken))?; }
        if self.deviceId != 0u32 { w.write_with_tag(24, |w| w.write_uint32(*&self.deviceId))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct Addresses<'a> {
    pub addresses: Vec<firefly::Address<'a>>,
}

impl<'a> MessageRead<'a> for Addresses<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.addresses.push(r.read_message::<firefly::Address>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for Addresses<'a> {
    fn get_size(&self) -> usize {
        0
        + self.addresses.iter().map(|s| 1 + sizeof_len((s).get_size())).sum::<usize>()
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        for s in &self.addresses { w.write_with_tag(10, |w| w.write_message(s))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct UploadUserMessage<'a> {
    pub messages: Vec<firefly::UserMessage<'a>>,
}

impl<'a> MessageRead<'a> for UploadUserMessage<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.messages.push(r.read_message::<firefly::UserMessage>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for UploadUserMessage<'a> {
    fn get_size(&self) -> usize {
        0
        + self.messages.iter().map(|s| 1 + sizeof_len((s).get_size())).sum::<usize>()
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        for s in &self.messages { w.write_with_tag(10, |w| w.write_message(s))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct MessageIdAndTo {
    pub id: u64,
    pub to: u64,
    pub is_self: bool,
}

impl<'a> MessageRead<'a> for MessageIdAndTo {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(9) => msg.id = r.read_fixed64(bytes)?,
                Ok(16) => msg.to = r.read_uint64(bytes)?,
                Ok(24) => msg.is_self = r.read_bool(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl MessageWrite for MessageIdAndTo {
    fn get_size(&self) -> usize {
        0
        + if self.id == 0u64 { 0 } else { 1 + 8 }
        + if self.to == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.to) as u64) }
        + if self.is_self == false { 0 } else { 1 + sizeof_varint(*(&self.is_self) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.id != 0u64 { w.write_with_tag(9, |w| w.write_fixed64(*&self.id))?; }
        if self.to != 0u64 { w.write_with_tag(16, |w| w.write_uint64(*&self.to))?; }
        if self.is_self != false { w.write_with_tag(24, |w| w.write_bool(*&self.is_self))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct UserMessageUploaded {
    pub messageIds: Vec<firefly::MessageIdAndTo>,
}

impl<'a> MessageRead<'a> for UserMessageUploaded {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.messageIds.push(r.read_message::<firefly::MessageIdAndTo>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl MessageWrite for UserMessageUploaded {
    fn get_size(&self) -> usize {
        0
        + self.messageIds.iter().map(|s| 1 + sizeof_len((s).get_size())).sum::<usize>()
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        for s in &self.messageIds { w.write_with_tag(10, |w| w.write_message(s))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct UserOnlineStatusRequest<'a> {
    pub usernames: Vec<Cow<'a, str>>,
}

impl<'a> MessageRead<'a> for UserOnlineStatusRequest<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.usernames.push(r.read_string(bytes).map(Cow::Borrowed)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for UserOnlineStatusRequest<'a> {
    fn get_size(&self) -> usize {
        0
        + self.usernames.iter().map(|s| 1 + sizeof_len((s).len())).sum::<usize>()
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        for s in &self.usernames { w.write_with_tag(10, |w| w.write_string(&**s))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct UserOnlineStatusResponse {
    pub online_bits: u32,
}

impl<'a> MessageRead<'a> for UserOnlineStatusResponse {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(13) => msg.online_bits = r.read_fixed32(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl MessageWrite for UserOnlineStatusResponse {
    fn get_size(&self) -> usize {
        0
        + if self.online_bits == 0u32 { 0 } else { 1 + 4 }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.online_bits != 0u32 { w.write_with_tag(13, |w| w.write_fixed32(*&self.online_bits))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct Request<'a> {
    pub id: u32,
    pub payload: firefly::mod_Request::OneOfpayload<'a>,
}

impl<'a> MessageRead<'a> for Request<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.id = r.read_uint32(bytes)?,
                Ok(18) => msg.payload = firefly::mod_Request::OneOfpayload::createUserMessage(r.read_message::<firefly::UserMessage>(bytes)?),
                Ok(26) => msg.payload = firefly::mod_Request::OneOfpayload::uploadUserMessage(r.read_message::<firefly::UploadUserMessage>(bytes)?),
                Ok(34) => msg.payload = firefly::mod_Request::OneOfpayload::uploadGroupMessage(r.read_message::<firefly::GroupMessage>(bytes)?),
                Ok(42) => msg.payload = firefly::mod_Request::OneOfpayload::requestGroupReAdds(r.read_message::<firefly::RequestGroupReAdds>(bytes)?),
                Ok(50) => msg.payload = firefly::mod_Request::OneOfpayload::requestGroupSync(r.read_message::<firefly::RequestGroupSync>(bytes)?),
                Ok(58) => msg.payload = firefly::mod_Request::OneOfpayload::userOnlineStatus(r.read_message::<firefly::UserOnlineStatusRequest>(bytes)?),
                Ok(66) => msg.payload = firefly::mod_Request::OneOfpayload::createJoinLink(r.read_message::<firefly::CreateJoinLinkRequest>(bytes)?),
                Ok(74) => msg.payload = firefly::mod_Request::OneOfpayload::joinViaLink(r.read_message::<firefly::JoinViaLinkRequest>(bytes)?),
                Ok(82) => msg.payload = firefly::mod_Request::OneOfpayload::createMeeting(r.read_message::<firefly::CreateMeetingRequest>(bytes)?),
                Ok(90) => msg.payload = firefly::mod_Request::OneOfpayload::joinMeeting(r.read_message::<firefly::JoinMeetingRequest>(bytes)?),
                Ok(98) => msg.payload = firefly::mod_Request::OneOfpayload::leaveMeeting(r.read_message::<firefly::LeaveMeetingRequest>(bytes)?),
                Ok(106) => msg.payload = firefly::mod_Request::OneOfpayload::endMeeting(r.read_message::<firefly::EndMeetingRequest>(bytes)?),
                Ok(114) => msg.payload = firefly::mod_Request::OneOfpayload::getActiveSession(r.read_message::<firefly::GetActiveSessionRequest>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for Request<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.id == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.id) as u64) }
        + match self.payload {
            firefly::mod_Request::OneOfpayload::createUserMessage(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_Request::OneOfpayload::uploadUserMessage(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_Request::OneOfpayload::uploadGroupMessage(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_Request::OneOfpayload::requestGroupReAdds(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_Request::OneOfpayload::requestGroupSync(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_Request::OneOfpayload::userOnlineStatus(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_Request::OneOfpayload::createJoinLink(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_Request::OneOfpayload::joinViaLink(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_Request::OneOfpayload::createMeeting(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_Request::OneOfpayload::joinMeeting(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_Request::OneOfpayload::leaveMeeting(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_Request::OneOfpayload::endMeeting(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_Request::OneOfpayload::getActiveSession(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_Request::OneOfpayload::None => 0,
    }    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.id != 0u32 { w.write_with_tag(8, |w| w.write_uint32(*&self.id))?; }
        match self.payload {            firefly::mod_Request::OneOfpayload::createUserMessage(ref m) => { w.write_with_tag(18, |w| w.write_message(m))? },
            firefly::mod_Request::OneOfpayload::uploadUserMessage(ref m) => { w.write_with_tag(26, |w| w.write_message(m))? },
            firefly::mod_Request::OneOfpayload::uploadGroupMessage(ref m) => { w.write_with_tag(34, |w| w.write_message(m))? },
            firefly::mod_Request::OneOfpayload::requestGroupReAdds(ref m) => { w.write_with_tag(42, |w| w.write_message(m))? },
            firefly::mod_Request::OneOfpayload::requestGroupSync(ref m) => { w.write_with_tag(50, |w| w.write_message(m))? },
            firefly::mod_Request::OneOfpayload::userOnlineStatus(ref m) => { w.write_with_tag(58, |w| w.write_message(m))? },
            firefly::mod_Request::OneOfpayload::createJoinLink(ref m) => { w.write_with_tag(66, |w| w.write_message(m))? },
            firefly::mod_Request::OneOfpayload::joinViaLink(ref m) => { w.write_with_tag(74, |w| w.write_message(m))? },
            firefly::mod_Request::OneOfpayload::createMeeting(ref m) => { w.write_with_tag(82, |w| w.write_message(m))? },
            firefly::mod_Request::OneOfpayload::joinMeeting(ref m) => { w.write_with_tag(90, |w| w.write_message(m))? },
            firefly::mod_Request::OneOfpayload::leaveMeeting(ref m) => { w.write_with_tag(98, |w| w.write_message(m))? },
            firefly::mod_Request::OneOfpayload::endMeeting(ref m) => { w.write_with_tag(106, |w| w.write_message(m))? },
            firefly::mod_Request::OneOfpayload::getActiveSession(ref m) => { w.write_with_tag(114, |w| w.write_message(m))? },
            firefly::mod_Request::OneOfpayload::None => {},
    }        Ok(())
    }
}

pub mod mod_Request {

use super::*;

#[derive(Debug, PartialEq, Clone)]
pub enum OneOfpayload<'a> {
    createUserMessage(firefly::UserMessage<'a>),
    uploadUserMessage(firefly::UploadUserMessage<'a>),
    uploadGroupMessage(firefly::GroupMessage<'a>),
    requestGroupReAdds(firefly::RequestGroupReAdds),
    requestGroupSync(firefly::RequestGroupSync),
    userOnlineStatus(firefly::UserOnlineStatusRequest<'a>),
    createJoinLink(firefly::CreateJoinLinkRequest),
    joinViaLink(firefly::JoinViaLinkRequest<'a>),
    createMeeting(firefly::CreateMeetingRequest),
    joinMeeting(firefly::JoinMeetingRequest),
    leaveMeeting(firefly::LeaveMeetingRequest),
    endMeeting(firefly::EndMeetingRequest),
    getActiveSession(firefly::GetActiveSessionRequest),
    None,
}

impl<'a> Default for OneOfpayload<'a> {
    fn default() -> Self {
        OneOfpayload::None
    }
}

}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct Response<'a> {
    pub id: u32,
    pub error: Option<firefly::Error<'a>>,
    pub body: firefly::mod_Response::OneOfbody<'a>,
}

impl<'a> MessageRead<'a> for Response<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.id = r.read_uint32(bytes)?,
                Ok(18) => msg.error = Some(r.read_message::<firefly::Error>(bytes)?),
                Ok(26) => msg.body = firefly::mod_Response::OneOfbody::createdUserMessage(r.read_message::<firefly::UserMessage>(bytes)?),
                Ok(34) => msg.body = firefly::mod_Response::OneOfbody::userMessageUploaded(r.read_message::<firefly::UserMessageUploaded>(bytes)?),
                Ok(42) => msg.body = firefly::mod_Response::OneOfbody::groupMessageUploaded(r.read_message::<firefly::GroupMessage>(bytes)?),
                Ok(50) => msg.body = firefly::mod_Response::OneOfbody::groupReAddRequestSuccess(r.read_message::<firefly::GroupReAddRequestSuccess>(bytes)?),
                Ok(58) => msg.body = firefly::mod_Response::OneOfbody::userOnlineStatus(r.read_message::<firefly::UserOnlineStatusResponse>(bytes)?),
                Ok(66) => msg.body = firefly::mod_Response::OneOfbody::createJoinLink(r.read_message::<firefly::CreateJoinLinkResponse>(bytes)?),
                Ok(74) => msg.body = firefly::mod_Response::OneOfbody::joinViaLinkSuccess(r.read_message::<firefly::JoinViaLinkSuccess>(bytes)?),
                Ok(82) => msg.body = firefly::mod_Response::OneOfbody::createMeetingResponse(r.read_message::<firefly::CreateMeetingResponse>(bytes)?),
                Ok(90) => msg.body = firefly::mod_Response::OneOfbody::joinMeetingResponse(r.read_message::<firefly::JoinMeetingResponse>(bytes)?),
                Ok(114) => msg.body = firefly::mod_Response::OneOfbody::getActiveSessionResponse(r.read_message::<firefly::GetActiveSessionResponse>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for Response<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.id == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.id) as u64) }
        + self.error.as_ref().map_or(0, |m| 1 + sizeof_len((m).get_size()))
        + match self.body {
            firefly::mod_Response::OneOfbody::createdUserMessage(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_Response::OneOfbody::userMessageUploaded(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_Response::OneOfbody::groupMessageUploaded(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_Response::OneOfbody::groupReAddRequestSuccess(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_Response::OneOfbody::userOnlineStatus(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_Response::OneOfbody::createJoinLink(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_Response::OneOfbody::joinViaLinkSuccess(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_Response::OneOfbody::createMeetingResponse(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_Response::OneOfbody::joinMeetingResponse(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_Response::OneOfbody::getActiveSessionResponse(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_Response::OneOfbody::None => 0,
    }    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.id != 0u32 { w.write_with_tag(8, |w| w.write_uint32(*&self.id))?; }
        if let Some(ref s) = self.error { w.write_with_tag(18, |w| w.write_message(s))?; }
        match self.body {            firefly::mod_Response::OneOfbody::createdUserMessage(ref m) => { w.write_with_tag(26, |w| w.write_message(m))? },
            firefly::mod_Response::OneOfbody::userMessageUploaded(ref m) => { w.write_with_tag(34, |w| w.write_message(m))? },
            firefly::mod_Response::OneOfbody::groupMessageUploaded(ref m) => { w.write_with_tag(42, |w| w.write_message(m))? },
            firefly::mod_Response::OneOfbody::groupReAddRequestSuccess(ref m) => { w.write_with_tag(50, |w| w.write_message(m))? },
            firefly::mod_Response::OneOfbody::userOnlineStatus(ref m) => { w.write_with_tag(58, |w| w.write_message(m))? },
            firefly::mod_Response::OneOfbody::createJoinLink(ref m) => { w.write_with_tag(66, |w| w.write_message(m))? },
            firefly::mod_Response::OneOfbody::joinViaLinkSuccess(ref m) => { w.write_with_tag(74, |w| w.write_message(m))? },
            firefly::mod_Response::OneOfbody::createMeetingResponse(ref m) => { w.write_with_tag(82, |w| w.write_message(m))? },
            firefly::mod_Response::OneOfbody::joinMeetingResponse(ref m) => { w.write_with_tag(90, |w| w.write_message(m))? },
            firefly::mod_Response::OneOfbody::getActiveSessionResponse(ref m) => { w.write_with_tag(114, |w| w.write_message(m))? },
            firefly::mod_Response::OneOfbody::None => {},
    }        Ok(())
    }
}

pub mod mod_Response {

use super::*;

#[derive(Debug, PartialEq, Clone)]
pub enum OneOfbody<'a> {
    createdUserMessage(firefly::UserMessage<'a>),
    userMessageUploaded(firefly::UserMessageUploaded),
    groupMessageUploaded(firefly::GroupMessage<'a>),
    groupReAddRequestSuccess(firefly::GroupReAddRequestSuccess),
    userOnlineStatus(firefly::UserOnlineStatusResponse),
    createJoinLink(firefly::CreateJoinLinkResponse<'a>),
    joinViaLinkSuccess(firefly::JoinViaLinkSuccess),
    createMeetingResponse(firefly::CreateMeetingResponse<'a>),
    joinMeetingResponse(firefly::JoinMeetingResponse<'a>),
    getActiveSessionResponse(firefly::GetActiveSessionResponse<'a>),
    None,
}

impl<'a> Default for OneOfbody<'a> {
    fn default() -> Self {
        OneOfbody::None
    }
}

}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct ServerMessage<'a> {
    pub message: firefly::mod_ServerMessage::OneOfmessage<'a>,
}

impl<'a> MessageRead<'a> for ServerMessage<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.message = firefly::mod_ServerMessage::OneOfmessage::userMessage(r.read_message::<firefly::UserMessage>(bytes)?),
                Ok(18) => msg.message = firefly::mod_ServerMessage::OneOfmessage::groupMessage(r.read_message::<firefly::GroupMessage>(bytes)?),
                Ok(26) => msg.message = firefly::mod_ServerMessage::OneOfmessage::userMessages(r.read_message::<firefly::UserMessages>(bytes)?),
                Ok(34) => msg.message = firefly::mod_ServerMessage::OneOfmessage::groupMessages(r.read_message::<firefly::GroupMessages>(bytes)?),
                Ok(82) => msg.message = firefly::mod_ServerMessage::OneOfmessage::response(r.read_message::<firefly::Response>(bytes)?),
                Ok(90) => msg.message = firefly::mod_ServerMessage::OneOfmessage::ping(r.read_bytes(bytes).map(Cow::Borrowed)?),
                Ok(98) => msg.message = firefly::mod_ServerMessage::OneOfmessage::pong(r.read_bytes(bytes).map(Cow::Borrowed)?),
                Ok(122) => msg.message = firefly::mod_ServerMessage::OneOfmessage::groupInvite(r.read_message::<firefly::GroupInvite>(bytes)?),
                Ok(130) => msg.message = firefly::mod_ServerMessage::OneOfmessage::groupCommits(r.read_message::<firefly::GroupCommits>(bytes)?),
                Ok(138) => msg.message = firefly::mod_ServerMessage::OneOfmessage::groupReAddRequests(r.read_message::<firefly::GroupReAddRequests>(bytes)?),
                Ok(146) => msg.message = firefly::mod_ServerMessage::OneOfmessage::groupJoinRequests(r.read_message::<firefly::GroupJoinRequests>(bytes)?),
                Ok(162) => msg.message = firefly::mod_ServerMessage::OneOfmessage::callSignal(r.read_message::<firefly::CallSignal>(bytes)?),
                Ok(170) => msg.message = firefly::mod_ServerMessage::OneOfmessage::groupMeetingSignal(r.read_message::<firefly::GroupMeetingSignal>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for ServerMessage<'a> {
    fn get_size(&self) -> usize {
        0
        + match self.message {
            firefly::mod_ServerMessage::OneOfmessage::userMessage(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_ServerMessage::OneOfmessage::groupMessage(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_ServerMessage::OneOfmessage::userMessages(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_ServerMessage::OneOfmessage::groupMessages(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_ServerMessage::OneOfmessage::response(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_ServerMessage::OneOfmessage::ping(ref m) => 1 + sizeof_len((m).len()),
            firefly::mod_ServerMessage::OneOfmessage::pong(ref m) => 1 + sizeof_len((m).len()),
            firefly::mod_ServerMessage::OneOfmessage::groupInvite(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_ServerMessage::OneOfmessage::groupCommits(ref m) => 2 + sizeof_len((m).get_size()),
            firefly::mod_ServerMessage::OneOfmessage::groupReAddRequests(ref m) => 2 + sizeof_len((m).get_size()),
            firefly::mod_ServerMessage::OneOfmessage::groupJoinRequests(ref m) => 2 + sizeof_len((m).get_size()),
            firefly::mod_ServerMessage::OneOfmessage::callSignal(ref m) => 2 + sizeof_len((m).get_size()),
            firefly::mod_ServerMessage::OneOfmessage::groupMeetingSignal(ref m) => 2 + sizeof_len((m).get_size()),
            firefly::mod_ServerMessage::OneOfmessage::None => 0,
    }    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        match self.message {            firefly::mod_ServerMessage::OneOfmessage::userMessage(ref m) => { w.write_with_tag(10, |w| w.write_message(m))? },
            firefly::mod_ServerMessage::OneOfmessage::groupMessage(ref m) => { w.write_with_tag(18, |w| w.write_message(m))? },
            firefly::mod_ServerMessage::OneOfmessage::userMessages(ref m) => { w.write_with_tag(26, |w| w.write_message(m))? },
            firefly::mod_ServerMessage::OneOfmessage::groupMessages(ref m) => { w.write_with_tag(34, |w| w.write_message(m))? },
            firefly::mod_ServerMessage::OneOfmessage::response(ref m) => { w.write_with_tag(82, |w| w.write_message(m))? },
            firefly::mod_ServerMessage::OneOfmessage::ping(ref m) => { w.write_with_tag(90, |w| w.write_bytes(&**m))? },
            firefly::mod_ServerMessage::OneOfmessage::pong(ref m) => { w.write_with_tag(98, |w| w.write_bytes(&**m))? },
            firefly::mod_ServerMessage::OneOfmessage::groupInvite(ref m) => { w.write_with_tag(122, |w| w.write_message(m))? },
            firefly::mod_ServerMessage::OneOfmessage::groupCommits(ref m) => { w.write_with_tag(130, |w| w.write_message(m))? },
            firefly::mod_ServerMessage::OneOfmessage::groupReAddRequests(ref m) => { w.write_with_tag(138, |w| w.write_message(m))? },
            firefly::mod_ServerMessage::OneOfmessage::groupJoinRequests(ref m) => { w.write_with_tag(146, |w| w.write_message(m))? },
            firefly::mod_ServerMessage::OneOfmessage::callSignal(ref m) => { w.write_with_tag(162, |w| w.write_message(m))? },
            firefly::mod_ServerMessage::OneOfmessage::groupMeetingSignal(ref m) => { w.write_with_tag(170, |w| w.write_message(m))? },
            firefly::mod_ServerMessage::OneOfmessage::None => {},
    }        Ok(())
    }
}

pub mod mod_ServerMessage {

use super::*;

#[derive(Debug, PartialEq, Clone)]
pub enum OneOfmessage<'a> {
    userMessage(firefly::UserMessage<'a>),
    groupMessage(firefly::GroupMessage<'a>),
    userMessages(firefly::UserMessages<'a>),
    groupMessages(firefly::GroupMessages<'a>),
    response(firefly::Response<'a>),
    ping(Cow<'a, [u8]>),
    pong(Cow<'a, [u8]>),
    groupInvite(firefly::GroupInvite<'a>),
    groupCommits(firefly::GroupCommits<'a>),
    groupReAddRequests(firefly::GroupReAddRequests<'a>),
    groupJoinRequests(firefly::GroupJoinRequests<'a>),
    callSignal(firefly::CallSignal<'a>),
    groupMeetingSignal(firefly::GroupMeetingSignal<'a>),
    None,
}

impl<'a> Default for OneOfmessage<'a> {
    fn default() -> Self {
        OneOfmessage::None
    }
}

}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct ClientMessage<'a> {
    pub message: firefly::mod_ClientMessage::OneOfmessage<'a>,
}

impl<'a> MessageRead<'a> for ClientMessage<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.message = firefly::mod_ClientMessage::OneOfmessage::userMessage(r.read_message::<firefly::UserMessage>(bytes)?),
                Ok(18) => msg.message = firefly::mod_ClientMessage::OneOfmessage::groupMessage(r.read_message::<firefly::GroupMessage>(bytes)?),
                Ok(26) => msg.message = firefly::mod_ClientMessage::OneOfmessage::verifiedUserMessage(r.read_message::<firefly::UserMessage>(bytes)?),
                Ok(82) => msg.message = firefly::mod_ClientMessage::OneOfmessage::request(r.read_message::<firefly::Request>(bytes)?),
                Ok(90) => msg.message = firefly::mod_ClientMessage::OneOfmessage::ping(r.read_bytes(bytes).map(Cow::Borrowed)?),
                Ok(98) => msg.message = firefly::mod_ClientMessage::OneOfmessage::pong(r.read_bytes(bytes).map(Cow::Borrowed)?),
                Ok(162) => msg.message = firefly::mod_ClientMessage::OneOfmessage::callSignal(r.read_message::<firefly::CallSignal>(bytes)?),
                Ok(170) => msg.message = firefly::mod_ClientMessage::OneOfmessage::groupMeetingSignal(r.read_message::<firefly::GroupMeetingSignal>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for ClientMessage<'a> {
    fn get_size(&self) -> usize {
        0
        + match self.message {
            firefly::mod_ClientMessage::OneOfmessage::userMessage(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_ClientMessage::OneOfmessage::groupMessage(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_ClientMessage::OneOfmessage::verifiedUserMessage(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_ClientMessage::OneOfmessage::request(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_ClientMessage::OneOfmessage::ping(ref m) => 1 + sizeof_len((m).len()),
            firefly::mod_ClientMessage::OneOfmessage::pong(ref m) => 1 + sizeof_len((m).len()),
            firefly::mod_ClientMessage::OneOfmessage::callSignal(ref m) => 2 + sizeof_len((m).get_size()),
            firefly::mod_ClientMessage::OneOfmessage::groupMeetingSignal(ref m) => 2 + sizeof_len((m).get_size()),
            firefly::mod_ClientMessage::OneOfmessage::None => 0,
    }    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        match self.message {            firefly::mod_ClientMessage::OneOfmessage::userMessage(ref m) => { w.write_with_tag(10, |w| w.write_message(m))? },
            firefly::mod_ClientMessage::OneOfmessage::groupMessage(ref m) => { w.write_with_tag(18, |w| w.write_message(m))? },
            firefly::mod_ClientMessage::OneOfmessage::verifiedUserMessage(ref m) => { w.write_with_tag(26, |w| w.write_message(m))? },
            firefly::mod_ClientMessage::OneOfmessage::request(ref m) => { w.write_with_tag(82, |w| w.write_message(m))? },
            firefly::mod_ClientMessage::OneOfmessage::ping(ref m) => { w.write_with_tag(90, |w| w.write_bytes(&**m))? },
            firefly::mod_ClientMessage::OneOfmessage::pong(ref m) => { w.write_with_tag(98, |w| w.write_bytes(&**m))? },
            firefly::mod_ClientMessage::OneOfmessage::callSignal(ref m) => { w.write_with_tag(162, |w| w.write_message(m))? },
            firefly::mod_ClientMessage::OneOfmessage::groupMeetingSignal(ref m) => { w.write_with_tag(170, |w| w.write_message(m))? },
            firefly::mod_ClientMessage::OneOfmessage::None => {},
    }        Ok(())
    }
}

pub mod mod_ClientMessage {

use super::*;

#[derive(Debug, PartialEq, Clone)]
pub enum OneOfmessage<'a> {
    userMessage(firefly::UserMessage<'a>),
    groupMessage(firefly::GroupMessage<'a>),
    verifiedUserMessage(firefly::UserMessage<'a>),
    request(firefly::Request<'a>),
    ping(Cow<'a, [u8]>),
    pong(Cow<'a, [u8]>),
    callSignal(firefly::CallSignal<'a>),
    groupMeetingSignal(firefly::GroupMeetingSignal<'a>),
    None,
}

impl<'a> Default for OneOfmessage<'a> {
    fn default() -> Self {
        OneOfmessage::None
    }
}

}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct CallSignal<'a> {
    pub call_id: u64,
    pub sender_username: Cow<'a, str>,
    pub receiver_username: Cow<'a, str>,
    pub type_pb: firefly::CallSignalType,
    pub sdp: Cow<'a, str>,
    pub candidate: Cow<'a, str>,
    pub sdp_m_line_index: i32,
    pub sdp_mid: Cow<'a, str>,
    pub sender_device_id: u32,
}

impl<'a> MessageRead<'a> for CallSignal<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(9) => msg.call_id = r.read_fixed64(bytes)?,
                Ok(18) => msg.sender_username = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(26) => msg.receiver_username = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(32) => msg.type_pb = r.read_enum(bytes)?,
                Ok(42) => msg.sdp = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(50) => msg.candidate = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(56) => msg.sdp_m_line_index = r.read_int32(bytes)?,
                Ok(66) => msg.sdp_mid = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(72) => msg.sender_device_id = r.read_uint32(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for CallSignal<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.call_id == 0u64 { 0 } else { 1 + 8 }
        + if self.sender_username == "" { 0 } else { 1 + sizeof_len((&self.sender_username).len()) }
        + if self.receiver_username == "" { 0 } else { 1 + sizeof_len((&self.receiver_username).len()) }
        + if self.type_pb == firefly::CallSignalType::CALL_REQUEST { 0 } else { 1 + sizeof_varint(*(&self.type_pb) as u64) }
        + if self.sdp == "" { 0 } else { 1 + sizeof_len((&self.sdp).len()) }
        + if self.candidate == "" { 0 } else { 1 + sizeof_len((&self.candidate).len()) }
        + if self.sdp_m_line_index == 0i32 { 0 } else { 1 + sizeof_varint(*(&self.sdp_m_line_index) as u64) }
        + if self.sdp_mid == "" { 0 } else { 1 + sizeof_len((&self.sdp_mid).len()) }
        + if self.sender_device_id == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.sender_device_id) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.call_id != 0u64 { w.write_with_tag(9, |w| w.write_fixed64(*&self.call_id))?; }
        if self.sender_username != "" { w.write_with_tag(18, |w| w.write_string(&**&self.sender_username))?; }
        if self.receiver_username != "" { w.write_with_tag(26, |w| w.write_string(&**&self.receiver_username))?; }
        if self.type_pb != firefly::CallSignalType::CALL_REQUEST { w.write_with_tag(32, |w| w.write_enum(*&self.type_pb as i32))?; }
        if self.sdp != "" { w.write_with_tag(42, |w| w.write_string(&**&self.sdp))?; }
        if self.candidate != "" { w.write_with_tag(50, |w| w.write_string(&**&self.candidate))?; }
        if self.sdp_m_line_index != 0i32 { w.write_with_tag(56, |w| w.write_int32(*&self.sdp_m_line_index))?; }
        if self.sdp_mid != "" { w.write_with_tag(66, |w| w.write_string(&**&self.sdp_mid))?; }
        if self.sender_device_id != 0u32 { w.write_with_tag(72, |w| w.write_uint32(*&self.sender_device_id))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupId {
    pub id: u64,
}

impl<'a> MessageRead<'a> for GroupId {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(16) => msg.id = r.read_uint64(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl MessageWrite for GroupId {
    fn get_size(&self) -> usize {
        0
        + if self.id == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.id) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.id != 0u64 { w.write_with_tag(16, |w| w.write_uint64(*&self.id))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct AuthToken<'a> {
    pub username: Cow<'a, str>,
    pub valid_until: u64,
    pub credential: Cow<'a, [u8]>,
    pub address_id: u64,
    pub device_id: u32,
}

impl<'a> MessageRead<'a> for AuthToken<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.username = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(16) => msg.valid_until = r.read_uint64(bytes)?,
                Ok(34) => msg.credential = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(48) => msg.address_id = r.read_uint64(bytes)?,
                Ok(40) => msg.device_id = r.read_uint32(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for AuthToken<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.username == "" { 0 } else { 1 + sizeof_len((&self.username).len()) }
        + if self.valid_until == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.valid_until) as u64) }
        + if self.credential == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.credential).len()) }
        + if self.address_id == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.address_id) as u64) }
        + if self.device_id == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.device_id) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.username != "" { w.write_with_tag(10, |w| w.write_string(&**&self.username))?; }
        if self.valid_until != 0u64 { w.write_with_tag(16, |w| w.write_uint64(*&self.valid_until))?; }
        if self.credential != Cow::Borrowed(b"") { w.write_with_tag(34, |w| w.write_bytes(&**&self.credential))?; }
        if self.address_id != 0u64 { w.write_with_tag(48, |w| w.write_uint64(*&self.address_id))?; }
        if self.device_id != 0u32 { w.write_with_tag(40, |w| w.write_uint32(*&self.device_id))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct SignedToken<'a> {
    pub kid: Cow<'a, str>,
    pub payload: Cow<'a, [u8]>,
    pub signature: Cow<'a, [u8]>,
}

impl<'a> MessageRead<'a> for SignedToken<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.kid = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(18) => msg.payload = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(26) => msg.signature = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for SignedToken<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.kid == "" { 0 } else { 1 + sizeof_len((&self.kid).len()) }
        + if self.payload == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.payload).len()) }
        + if self.signature == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.signature).len()) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.kid != "" { w.write_with_tag(10, |w| w.write_string(&**&self.kid))?; }
        if self.payload != Cow::Borrowed(b"") { w.write_with_tag(18, |w| w.write_bytes(&**&self.payload))?; }
        if self.signature != Cow::Borrowed(b"") { w.write_with_tag(26, |w| w.write_bytes(&**&self.signature))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct FireflyIdentity<'a> {
    pub secret: Cow<'a, [u8]>,
    pub public: Cow<'a, [u8]>,
    pub credential: Cow<'a, [u8]>,
}

impl<'a> MessageRead<'a> for FireflyIdentity<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(18) => msg.secret = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(26) => msg.public = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(34) => msg.credential = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for FireflyIdentity<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.secret == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.secret).len()) }
        + if self.public == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.public).len()) }
        + if self.credential == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.credential).len()) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.secret != Cow::Borrowed(b"") { w.write_with_tag(18, |w| w.write_bytes(&**&self.secret))?; }
        if self.public != Cow::Borrowed(b"") { w.write_with_tag(26, |w| w.write_bytes(&**&self.public))?; }
        if self.credential != Cow::Borrowed(b"") { w.write_with_tag(34, |w| w.write_bytes(&**&self.credential))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct FireflyGroupExtension<'a> {
    pub name: Cow<'a, str>,
    pub roles: Vec<firefly::FireflyGroupRole<'a>>,
    pub channels: Vec<firefly::FireflyGroupChannel<'a>>,
    pub members: Vec<firefly::FireflyGroupMember<'a>>,
    pub default_permissions: u32,
}

impl<'a> MessageRead<'a> for FireflyGroupExtension<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.name = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(18) => msg.roles.push(r.read_message::<firefly::FireflyGroupRole>(bytes)?),
                Ok(26) => msg.channels.push(r.read_message::<firefly::FireflyGroupChannel>(bytes)?),
                Ok(34) => msg.members.push(r.read_message::<firefly::FireflyGroupMember>(bytes)?),
                Ok(45) => msg.default_permissions = r.read_fixed32(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for FireflyGroupExtension<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.name == "" { 0 } else { 1 + sizeof_len((&self.name).len()) }
        + self.roles.iter().map(|s| 1 + sizeof_len((s).get_size())).sum::<usize>()
        + self.channels.iter().map(|s| 1 + sizeof_len((s).get_size())).sum::<usize>()
        + self.members.iter().map(|s| 1 + sizeof_len((s).get_size())).sum::<usize>()
        + if self.default_permissions == 0u32 { 0 } else { 1 + 4 }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.name != "" { w.write_with_tag(10, |w| w.write_string(&**&self.name))?; }
        for s in &self.roles { w.write_with_tag(18, |w| w.write_message(s))?; }
        for s in &self.channels { w.write_with_tag(26, |w| w.write_message(s))?; }
        for s in &self.members { w.write_with_tag(34, |w| w.write_message(s))?; }
        if self.default_permissions != 0u32 { w.write_with_tag(45, |w| w.write_fixed32(*&self.default_permissions))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct FireflyGroupRole<'a> {
    pub id: u32,
    pub name: Cow<'a, str>,
    pub permissions: u32,
    pub color: u32,
}

impl<'a> MessageRead<'a> for FireflyGroupRole<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.id = r.read_uint32(bytes)?,
                Ok(18) => msg.name = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(29) => msg.permissions = r.read_fixed32(bytes)?,
                Ok(32) => msg.color = r.read_uint32(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for FireflyGroupRole<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.id == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.id) as u64) }
        + if self.name == "" { 0 } else { 1 + sizeof_len((&self.name).len()) }
        + if self.permissions == 0u32 { 0 } else { 1 + 4 }
        + if self.color == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.color) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.id != 0u32 { w.write_with_tag(8, |w| w.write_uint32(*&self.id))?; }
        if self.name != "" { w.write_with_tag(18, |w| w.write_string(&**&self.name))?; }
        if self.permissions != 0u32 { w.write_with_tag(29, |w| w.write_fixed32(*&self.permissions))?; }
        if self.color != 0u32 { w.write_with_tag(32, |w| w.write_uint32(*&self.color))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct FireflyGroupMember<'a> {
    pub username: Cow<'a, str>,
    pub role: u32,
}

impl<'a> MessageRead<'a> for FireflyGroupMember<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.username = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(16) => msg.role = r.read_uint32(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for FireflyGroupMember<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.username == "" { 0 } else { 1 + sizeof_len((&self.username).len()) }
        + if self.role == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.role) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.username != "" { w.write_with_tag(10, |w| w.write_string(&**&self.username))?; }
        if self.role != 0u32 { w.write_with_tag(16, |w| w.write_uint32(*&self.role))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct FireflyGroupChannel<'a> {
    pub id: u32,
    pub name: Cow<'a, str>,
    pub type_pb: u32,
    pub roles: Vec<firefly::FireflyGroupRole<'a>>,
    pub default_permissions: u32,
}

impl<'a> MessageRead<'a> for FireflyGroupChannel<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.id = r.read_uint32(bytes)?,
                Ok(18) => msg.name = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(24) => msg.type_pb = r.read_uint32(bytes)?,
                Ok(34) => msg.roles.push(r.read_message::<firefly::FireflyGroupRole>(bytes)?),
                Ok(45) => msg.default_permissions = r.read_fixed32(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for FireflyGroupChannel<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.id == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.id) as u64) }
        + if self.name == "" { 0 } else { 1 + sizeof_len((&self.name).len()) }
        + if self.type_pb == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.type_pb) as u64) }
        + self.roles.iter().map(|s| 1 + sizeof_len((s).get_size())).sum::<usize>()
        + if self.default_permissions == 0u32 { 0 } else { 1 + 4 }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.id != 0u32 { w.write_with_tag(8, |w| w.write_uint32(*&self.id))?; }
        if self.name != "" { w.write_with_tag(18, |w| w.write_string(&**&self.name))?; }
        if self.type_pb != 0u32 { w.write_with_tag(24, |w| w.write_uint32(*&self.type_pb))?; }
        for s in &self.roles { w.write_with_tag(34, |w| w.write_message(s))?; }
        if self.default_permissions != 0u32 { w.write_with_tag(45, |w| w.write_fixed32(*&self.default_permissions))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct PreKeyBundle<'a> {
    pub registrationId: u32,
    pub deviceId: u32,
    pub preKeyId: u32,
    pub prePublicKey: Cow<'a, [u8]>,
    pub signedPreKeyId: u32,
    pub signedPrePublicKey: Cow<'a, [u8]>,
    pub signedPreKeySignature: Cow<'a, [u8]>,
    pub identityPublicKey: Cow<'a, [u8]>,
    pub KEMPreKeyId: u32,
    pub KEMPrePublicKey: Cow<'a, [u8]>,
    pub KEMPreKeySignature: Cow<'a, [u8]>,
}

impl<'a> MessageRead<'a> for PreKeyBundle<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.registrationId = r.read_uint32(bytes)?,
                Ok(16) => msg.deviceId = r.read_uint32(bytes)?,
                Ok(24) => msg.preKeyId = r.read_uint32(bytes)?,
                Ok(34) => msg.prePublicKey = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(40) => msg.signedPreKeyId = r.read_uint32(bytes)?,
                Ok(50) => msg.signedPrePublicKey = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(58) => msg.signedPreKeySignature = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(66) => msg.identityPublicKey = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(72) => msg.KEMPreKeyId = r.read_uint32(bytes)?,
                Ok(82) => msg.KEMPrePublicKey = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(90) => msg.KEMPreKeySignature = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for PreKeyBundle<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.registrationId == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.registrationId) as u64) }
        + if self.deviceId == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.deviceId) as u64) }
        + if self.preKeyId == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.preKeyId) as u64) }
        + if self.prePublicKey == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.prePublicKey).len()) }
        + if self.signedPreKeyId == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.signedPreKeyId) as u64) }
        + if self.signedPrePublicKey == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.signedPrePublicKey).len()) }
        + if self.signedPreKeySignature == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.signedPreKeySignature).len()) }
        + if self.identityPublicKey == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.identityPublicKey).len()) }
        + if self.KEMPreKeyId == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.KEMPreKeyId) as u64) }
        + if self.KEMPrePublicKey == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.KEMPrePublicKey).len()) }
        + if self.KEMPreKeySignature == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.KEMPreKeySignature).len()) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.registrationId != 0u32 { w.write_with_tag(8, |w| w.write_uint32(*&self.registrationId))?; }
        if self.deviceId != 0u32 { w.write_with_tag(16, |w| w.write_uint32(*&self.deviceId))?; }
        if self.preKeyId != 0u32 { w.write_with_tag(24, |w| w.write_uint32(*&self.preKeyId))?; }
        if self.prePublicKey != Cow::Borrowed(b"") { w.write_with_tag(34, |w| w.write_bytes(&**&self.prePublicKey))?; }
        if self.signedPreKeyId != 0u32 { w.write_with_tag(40, |w| w.write_uint32(*&self.signedPreKeyId))?; }
        if self.signedPrePublicKey != Cow::Borrowed(b"") { w.write_with_tag(50, |w| w.write_bytes(&**&self.signedPrePublicKey))?; }
        if self.signedPreKeySignature != Cow::Borrowed(b"") { w.write_with_tag(58, |w| w.write_bytes(&**&self.signedPreKeySignature))?; }
        if self.identityPublicKey != Cow::Borrowed(b"") { w.write_with_tag(66, |w| w.write_bytes(&**&self.identityPublicKey))?; }
        if self.KEMPreKeyId != 0u32 { w.write_with_tag(72, |w| w.write_uint32(*&self.KEMPreKeyId))?; }
        if self.KEMPrePublicKey != Cow::Borrowed(b"") { w.write_with_tag(82, |w| w.write_bytes(&**&self.KEMPrePublicKey))?; }
        if self.KEMPreKeySignature != Cow::Borrowed(b"") { w.write_with_tag(90, |w| w.write_bytes(&**&self.KEMPreKeySignature))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct PreKeyBundleEntry<'a> {
    pub id: u32,
    pub address: u64,
    pub bundle: Option<firefly::PreKeyBundle<'a>>,
    pub username: Cow<'a, str>,
    pub device_id: u32,
}

impl<'a> MessageRead<'a> for PreKeyBundleEntry<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.id = r.read_uint32(bytes)?,
                Ok(16) => msg.address = r.read_uint64(bytes)?,
                Ok(26) => msg.bundle = Some(r.read_message::<firefly::PreKeyBundle>(bytes)?),
                Ok(34) => msg.username = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(40) => msg.device_id = r.read_uint32(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for PreKeyBundleEntry<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.id == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.id) as u64) }
        + if self.address == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.address) as u64) }
        + self.bundle.as_ref().map_or(0, |m| 1 + sizeof_len((m).get_size()))
        + if self.username == "" { 0 } else { 1 + sizeof_len((&self.username).len()) }
        + if self.device_id == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.device_id) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.id != 0u32 { w.write_with_tag(8, |w| w.write_uint32(*&self.id))?; }
        if self.address != 0u64 { w.write_with_tag(16, |w| w.write_uint64(*&self.address))?; }
        if let Some(ref s) = self.bundle { w.write_with_tag(26, |w| w.write_message(s))?; }
        if self.username != "" { w.write_with_tag(34, |w| w.write_string(&**&self.username))?; }
        if self.device_id != 0u32 { w.write_with_tag(40, |w| w.write_uint32(*&self.device_id))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct PreKeyBundleEntries<'a> {
    pub entries: Vec<firefly::PreKeyBundleEntry<'a>>,
}

impl<'a> MessageRead<'a> for PreKeyBundleEntries<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.entries.push(r.read_message::<firefly::PreKeyBundleEntry>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for PreKeyBundleEntries<'a> {
    fn get_size(&self) -> usize {
        0
        + self.entries.iter().map(|s| 1 + sizeof_len((s).get_size())).sum::<usize>()
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        for s in &self.entries { w.write_with_tag(10, |w| w.write_message(s))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct ConversationStart<'a> {
    pub conversationId: u64,
    pub started_by: Cow<'a, str>,
    pub other: Cow<'a, str>,
    pub bundle: Option<firefly::PreKeyBundle<'a>>,
}

impl<'a> MessageRead<'a> for ConversationStart<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.conversationId = r.read_uint64(bytes)?,
                Ok(18) => msg.started_by = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(26) => msg.other = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(34) => msg.bundle = Some(r.read_message::<firefly::PreKeyBundle>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for ConversationStart<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.conversationId == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.conversationId) as u64) }
        + if self.started_by == "" { 0 } else { 1 + sizeof_len((&self.started_by).len()) }
        + if self.other == "" { 0 } else { 1 + sizeof_len((&self.other).len()) }
        + self.bundle.as_ref().map_or(0, |m| 1 + sizeof_len((m).get_size()))
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.conversationId != 0u64 { w.write_with_tag(8, |w| w.write_uint64(*&self.conversationId))?; }
        if self.started_by != "" { w.write_with_tag(18, |w| w.write_string(&**&self.started_by))?; }
        if self.other != "" { w.write_with_tag(26, |w| w.write_string(&**&self.other))?; }
        if let Some(ref s) = self.bundle { w.write_with_tag(34, |w| w.write_message(s))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct PreKeyBundles<'a> {
    pub bundles: Vec<firefly::PreKeyBundle<'a>>,
}

impl<'a> MessageRead<'a> for PreKeyBundles<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.bundles.push(r.read_message::<firefly::PreKeyBundle>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for PreKeyBundles<'a> {
    fn get_size(&self) -> usize {
        0
        + self.bundles.iter().map(|s| 1 + sizeof_len((s).get_size())).sum::<usize>()
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        for s in &self.bundles { w.write_with_tag(10, |w| w.write_message(s))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct Conversation<'a> {
    pub user1: Cow<'a, str>,
    pub user2: Cow<'a, str>,
    pub settings: u64,
}

impl<'a> MessageRead<'a> for Conversation<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.user1 = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(18) => msg.user2 = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(24) => msg.settings = r.read_uint64(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for Conversation<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.user1 == "" { 0 } else { 1 + sizeof_len((&self.user1).len()) }
        + if self.user2 == "" { 0 } else { 1 + sizeof_len((&self.user2).len()) }
        + if self.settings == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.settings) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.user1 != "" { w.write_with_tag(10, |w| w.write_string(&**&self.user1))?; }
        if self.user2 != "" { w.write_with_tag(18, |w| w.write_string(&**&self.user2))?; }
        if self.settings != 0u64 { w.write_with_tag(24, |w| w.write_uint64(*&self.settings))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct Conversations<'a> {
    pub conversations: Vec<firefly::Conversation<'a>>,
}

impl<'a> MessageRead<'a> for Conversations<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.conversations.push(r.read_message::<firefly::Conversation>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for Conversations<'a> {
    fn get_size(&self) -> usize {
        0
        + self.conversations.iter().map(|s| 1 + sizeof_len((s).get_size())).sum::<usize>()
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        for s in &self.conversations { w.write_with_tag(10, |w| w.write_message(s))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct EncryptedFile<'a> {
    pub url: Cow<'a, str>,
    pub secretKey: Cow<'a, [u8]>,
    pub contentType: u32,
    pub contentLength: u32,
}

impl<'a> MessageRead<'a> for EncryptedFile<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.url = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(26) => msg.secretKey = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(16) => msg.contentType = r.read_uint32(bytes)?,
                Ok(32) => msg.contentLength = r.read_uint32(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for EncryptedFile<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.url == "" { 0 } else { 1 + sizeof_len((&self.url).len()) }
        + if self.secretKey == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.secretKey).len()) }
        + if self.contentType == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.contentType) as u64) }
        + if self.contentLength == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.contentLength) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.url != "" { w.write_with_tag(10, |w| w.write_string(&**&self.url))?; }
        if self.secretKey != Cow::Borrowed(b"") { w.write_with_tag(26, |w| w.write_bytes(&**&self.secretKey))?; }
        if self.contentType != 0u32 { w.write_with_tag(16, |w| w.write_uint32(*&self.contentType))?; }
        if self.contentLength != 0u32 { w.write_with_tag(32, |w| w.write_uint32(*&self.contentLength))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct EncryptedFiles<'a> {
    pub files: Vec<firefly::EncryptedFile<'a>>,
}

impl<'a> MessageRead<'a> for EncryptedFiles<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.files.push(r.read_message::<firefly::EncryptedFile>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for EncryptedFiles<'a> {
    fn get_size(&self) -> usize {
        0
        + self.files.iter().map(|s| 1 + sizeof_len((s).get_size())).sum::<usize>()
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        for s in &self.files { w.write_with_tag(10, |w| w.write_message(s))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct MessagePayload<'a> {
    pub text: Cow<'a, str>,
    pub replyingTo: u64,
    pub files: Option<firefly::EncryptedFiles<'a>>,
}

impl<'a> MessageRead<'a> for MessagePayload<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.text = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(17) => msg.replyingTo = r.read_fixed64(bytes)?,
                Ok(26) => msg.files = Some(r.read_message::<firefly::EncryptedFiles>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for MessagePayload<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.text == "" { 0 } else { 1 + sizeof_len((&self.text).len()) }
        + if self.replyingTo == 0u64 { 0 } else { 1 + 8 }
        + self.files.as_ref().map_or(0, |m| 1 + sizeof_len((m).get_size()))
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.text != "" { w.write_with_tag(10, |w| w.write_string(&**&self.text))?; }
        if self.replyingTo != 0u64 { w.write_with_tag(17, |w| w.write_fixed64(*&self.replyingTo))?; }
        if let Some(ref s) = self.files { w.write_with_tag(26, |w| w.write_message(s))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct CallMessage<'a> {
    pub message: Cow<'a, [u8]>,
    pub type_pb: firefly::CallMessageType,
    pub jsonBody: Cow<'a, str>,
    pub sessionId: u32,
}

impl<'a> MessageRead<'a> for CallMessage<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.message = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(24) => msg.type_pb = r.read_enum(bytes)?,
                Ok(34) => msg.jsonBody = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(16) => msg.sessionId = r.read_uint32(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for CallMessage<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.message == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.message).len()) }
        + if self.type_pb == firefly::CallMessageType::none { 0 } else { 1 + sizeof_varint(*(&self.type_pb) as u64) }
        + if self.jsonBody == "" { 0 } else { 1 + sizeof_len((&self.jsonBody).len()) }
        + if self.sessionId == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.sessionId) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.message != Cow::Borrowed(b"") { w.write_with_tag(10, |w| w.write_bytes(&**&self.message))?; }
        if self.type_pb != firefly::CallMessageType::none { w.write_with_tag(24, |w| w.write_enum(*&self.type_pb as i32))?; }
        if self.jsonBody != "" { w.write_with_tag(34, |w| w.write_string(&**&self.jsonBody))?; }
        if self.sessionId != 0u32 { w.write_with_tag(16, |w| w.write_uint32(*&self.sessionId))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct SelfUserMessage<'a> {
    pub to: Cow<'a, str>,
    pub inner: Cow<'a, [u8]>,
}

impl<'a> MessageRead<'a> for SelfUserMessage<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.to = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(18) => msg.inner = r.read_bytes(bytes).map(Cow::Borrowed)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for SelfUserMessage<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.to == "" { 0 } else { 1 + sizeof_len((&self.to).len()) }
        + if self.inner == Cow::Borrowed(b"") { 0 } else { 1 + sizeof_len((&self.inner).len()) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.to != "" { w.write_with_tag(10, |w| w.write_string(&**&self.to))?; }
        if self.inner != Cow::Borrowed(b"") { w.write_with_tag(18, |w| w.write_bytes(&**&self.inner))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct UserMessageInner<'a> {
    pub nonce: u32,
    pub message: firefly::mod_UserMessageInner::OneOfmessage<'a>,
}

impl<'a> MessageRead<'a> for UserMessageInner<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(85) => msg.nonce = r.read_fixed32(bytes)?,
                Ok(10) => msg.message = firefly::mod_UserMessageInner::OneOfmessage::plainText(r.read_bytes(bytes).map(Cow::Borrowed)?),
                Ok(18) => msg.message = firefly::mod_UserMessageInner::OneOfmessage::callMessage(r.read_message::<firefly::CallMessage>(bytes)?),
                Ok(26) => msg.message = firefly::mod_UserMessageInner::OneOfmessage::messagePayload(r.read_message::<firefly::MessagePayload>(bytes)?),
                Ok(34) => msg.message = firefly::mod_UserMessageInner::OneOfmessage::selfMessage(r.read_message::<firefly::SelfUserMessage>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for UserMessageInner<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.nonce == 0u32 { 0 } else { 1 + 4 }
        + match self.message {
            firefly::mod_UserMessageInner::OneOfmessage::plainText(ref m) => 1 + sizeof_len((m).len()),
            firefly::mod_UserMessageInner::OneOfmessage::callMessage(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_UserMessageInner::OneOfmessage::messagePayload(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_UserMessageInner::OneOfmessage::selfMessage(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_UserMessageInner::OneOfmessage::None => 0,
    }    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.nonce != 0u32 { w.write_with_tag(85, |w| w.write_fixed32(*&self.nonce))?; }
        match self.message {            firefly::mod_UserMessageInner::OneOfmessage::plainText(ref m) => { w.write_with_tag(10, |w| w.write_bytes(&**m))? },
            firefly::mod_UserMessageInner::OneOfmessage::callMessage(ref m) => { w.write_with_tag(18, |w| w.write_message(m))? },
            firefly::mod_UserMessageInner::OneOfmessage::messagePayload(ref m) => { w.write_with_tag(26, |w| w.write_message(m))? },
            firefly::mod_UserMessageInner::OneOfmessage::selfMessage(ref m) => { w.write_with_tag(34, |w| w.write_message(m))? },
            firefly::mod_UserMessageInner::OneOfmessage::None => {},
    }        Ok(())
    }
}

pub mod mod_UserMessageInner {

use super::*;

#[derive(Debug, PartialEq, Clone)]
pub enum OneOfmessage<'a> {
    plainText(Cow<'a, [u8]>),
    callMessage(firefly::CallMessage<'a>),
    messagePayload(firefly::MessagePayload<'a>),
    selfMessage(firefly::SelfUserMessage<'a>),
    None,
}

impl<'a> Default for OneOfmessage<'a> {
    fn default() -> Self {
        OneOfmessage::None
    }
}

}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupMessageInner<'a> {
    pub channelId: u32,
    pub message: firefly::mod_GroupMessageInner::OneOfmessage<'a>,
}

impl<'a> MessageRead<'a> for GroupMessageInner<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.channelId = r.read_uint32(bytes)?,
                Ok(18) => msg.message = firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(r.read_message::<firefly::MessagePayload>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for GroupMessageInner<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.channelId == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.channelId) as u64) }
        + match self.message {
            firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(ref m) => 1 + sizeof_len((m).get_size()),
            firefly::mod_GroupMessageInner::OneOfmessage::None => 0,
    }    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.channelId != 0u32 { w.write_with_tag(8, |w| w.write_uint32(*&self.channelId))?; }
        match self.message {            firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(ref m) => { w.write_with_tag(18, |w| w.write_message(m))? },
            firefly::mod_GroupMessageInner::OneOfmessage::None => {},
    }        Ok(())
    }
}

pub mod mod_GroupMessageInner {

use super::*;

#[derive(Debug, PartialEq, Clone)]
pub enum OneOfmessage<'a> {
    messagePayload(firefly::MessagePayload<'a>),
    None,
}

impl<'a> Default for OneOfmessage<'a> {
    fn default() -> Self {
        OneOfmessage::None
    }
}

}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct RequestGroupReAdds {
    pub group_ids: Vec<u64>,
}

impl<'a> MessageRead<'a> for RequestGroupReAdds {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.group_ids = r.read_packed(bytes, |r, bytes| Ok(r.read_uint64(bytes)?))?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl MessageWrite for RequestGroupReAdds {
    fn get_size(&self) -> usize {
        0
        + if self.group_ids.is_empty() { 0 } else { 1 + sizeof_len(self.group_ids.iter().map(|s| sizeof_varint(*(s) as u64)).sum::<usize>()) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        w.write_packed_with_tag(10, &self.group_ids, |w, m| w.write_uint64(*m), &|m| sizeof_varint(*(m) as u64))?;
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct RequestGroupSync {
    pub group_id: u64,
    pub epoch: u32,
}

impl<'a> MessageRead<'a> for RequestGroupSync {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.group_id = r.read_uint64(bytes)?,
                Ok(16) => msg.epoch = r.read_uint32(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl MessageWrite for RequestGroupSync {
    fn get_size(&self) -> usize {
        0
        + if self.group_id == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.group_id) as u64) }
        + if self.epoch == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.epoch) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.group_id != 0u64 { w.write_with_tag(8, |w| w.write_uint64(*&self.group_id))?; }
        if self.epoch != 0u32 { w.write_with_tag(16, |w| w.write_uint32(*&self.epoch))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupReAddRequestSuccess { }

impl<'a> MessageRead<'a> for GroupReAddRequestSuccess {
    fn from_reader(r: &mut BytesReader, _: &[u8]) -> Result<Self> {
        r.read_to_end();
        Ok(Self::default())
    }
}

impl MessageWrite for GroupReAddRequestSuccess { }

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct CreateJoinLinkRequest {
    pub group_id: u64,
    pub expires_in_seconds: u64,
    pub max_uses: u32,
}

impl<'a> MessageRead<'a> for CreateJoinLinkRequest {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.group_id = r.read_uint64(bytes)?,
                Ok(16) => msg.expires_in_seconds = r.read_uint64(bytes)?,
                Ok(24) => msg.max_uses = r.read_uint32(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl MessageWrite for CreateJoinLinkRequest {
    fn get_size(&self) -> usize {
        0
        + if self.group_id == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.group_id) as u64) }
        + if self.expires_in_seconds == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.expires_in_seconds) as u64) }
        + if self.max_uses == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.max_uses) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.group_id != 0u64 { w.write_with_tag(8, |w| w.write_uint64(*&self.group_id))?; }
        if self.expires_in_seconds != 0u64 { w.write_with_tag(16, |w| w.write_uint64(*&self.expires_in_seconds))?; }
        if self.max_uses != 0u32 { w.write_with_tag(24, |w| w.write_uint32(*&self.max_uses))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct CreateJoinLinkResponse<'a> {
    pub token: Cow<'a, str>,
}

impl<'a> MessageRead<'a> for CreateJoinLinkResponse<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.token = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for CreateJoinLinkResponse<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.token == "" { 0 } else { 1 + sizeof_len((&self.token).len()) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.token != "" { w.write_with_tag(10, |w| w.write_string(&**&self.token))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct JoinViaLinkRequest<'a> {
    pub token: Cow<'a, str>,
}

impl<'a> MessageRead<'a> for JoinViaLinkRequest<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.token = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for JoinViaLinkRequest<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.token == "" { 0 } else { 1 + sizeof_len((&self.token).len()) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.token != "" { w.write_with_tag(10, |w| w.write_string(&**&self.token))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct JoinViaLinkSuccess { }

impl<'a> MessageRead<'a> for JoinViaLinkSuccess {
    fn from_reader(r: &mut BytesReader, _: &[u8]) -> Result<Self> {
        r.read_to_end();
        Ok(Self::default())
    }
}

impl MessageWrite for JoinViaLinkSuccess { }

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupJoinRequest<'a> {
    pub group_id: u64,
    pub address_id: u64,
    pub username: Cow<'a, str>,
}

impl<'a> MessageRead<'a> for GroupJoinRequest<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.group_id = r.read_uint64(bytes)?,
                Ok(16) => msg.address_id = r.read_uint64(bytes)?,
                Ok(26) => msg.username = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for GroupJoinRequest<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.group_id == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.group_id) as u64) }
        + if self.address_id == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.address_id) as u64) }
        + if self.username == "" { 0 } else { 1 + sizeof_len((&self.username).len()) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.group_id != 0u64 { w.write_with_tag(8, |w| w.write_uint64(*&self.group_id))?; }
        if self.address_id != 0u64 { w.write_with_tag(16, |w| w.write_uint64(*&self.address_id))?; }
        if self.username != "" { w.write_with_tag(26, |w| w.write_string(&**&self.username))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupJoinRequests<'a> {
    pub requests: Vec<firefly::GroupJoinRequest<'a>>,
}

impl<'a> MessageRead<'a> for GroupJoinRequests<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.requests.push(r.read_message::<firefly::GroupJoinRequest>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for GroupJoinRequests<'a> {
    fn get_size(&self) -> usize {
        0
        + self.requests.iter().map(|s| 1 + sizeof_len((s).get_size())).sum::<usize>()
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        for s in &self.requests { w.write_with_tag(10, |w| w.write_message(s))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupMeetingSession<'a> {
    pub session_id: u64,
    pub group_id: u64,
    pub channel_id: u32,
    pub creator_username: Cow<'a, str>,
    pub status: firefly::MeetingSessionStatus,
    pub created_at: u64,
    pub ended_at: u64,
    pub participants: Vec<Cow<'a, str>>,
    pub cf_meeting_id: Cow<'a, str>,
    pub e2ee_enabled: bool,
}

impl<'a> MessageRead<'a> for GroupMeetingSession<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(9) => msg.session_id = r.read_fixed64(bytes)?,
                Ok(16) => msg.group_id = r.read_uint64(bytes)?,
                Ok(24) => msg.channel_id = r.read_uint32(bytes)?,
                Ok(34) => msg.creator_username = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(40) => msg.status = r.read_enum(bytes)?,
                Ok(49) => msg.created_at = r.read_fixed64(bytes)?,
                Ok(57) => msg.ended_at = r.read_fixed64(bytes)?,
                Ok(66) => msg.participants.push(r.read_string(bytes).map(Cow::Borrowed)?),
                Ok(74) => msg.cf_meeting_id = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(80) => msg.e2ee_enabled = r.read_bool(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for GroupMeetingSession<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.session_id == 0u64 { 0 } else { 1 + 8 }
        + if self.group_id == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.group_id) as u64) }
        + if self.channel_id == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.channel_id) as u64) }
        + if self.creator_username == "" { 0 } else { 1 + sizeof_len((&self.creator_username).len()) }
        + if self.status == firefly::MeetingSessionStatus::MEETING_STATUS_ACTIVE { 0 } else { 1 + sizeof_varint(*(&self.status) as u64) }
        + if self.created_at == 0u64 { 0 } else { 1 + 8 }
        + if self.ended_at == 0u64 { 0 } else { 1 + 8 }
        + self.participants.iter().map(|s| 1 + sizeof_len((s).len())).sum::<usize>()
        + if self.cf_meeting_id == "" { 0 } else { 1 + sizeof_len((&self.cf_meeting_id).len()) }
        + if self.e2ee_enabled == false { 0 } else { 1 + sizeof_varint(*(&self.e2ee_enabled) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.session_id != 0u64 { w.write_with_tag(9, |w| w.write_fixed64(*&self.session_id))?; }
        if self.group_id != 0u64 { w.write_with_tag(16, |w| w.write_uint64(*&self.group_id))?; }
        if self.channel_id != 0u32 { w.write_with_tag(24, |w| w.write_uint32(*&self.channel_id))?; }
        if self.creator_username != "" { w.write_with_tag(34, |w| w.write_string(&**&self.creator_username))?; }
        if self.status != firefly::MeetingSessionStatus::MEETING_STATUS_ACTIVE { w.write_with_tag(40, |w| w.write_enum(*&self.status as i32))?; }
        if self.created_at != 0u64 { w.write_with_tag(49, |w| w.write_fixed64(*&self.created_at))?; }
        if self.ended_at != 0u64 { w.write_with_tag(57, |w| w.write_fixed64(*&self.ended_at))?; }
        for s in &self.participants { w.write_with_tag(66, |w| w.write_string(&**s))?; }
        if self.cf_meeting_id != "" { w.write_with_tag(74, |w| w.write_string(&**&self.cf_meeting_id))?; }
        if self.e2ee_enabled != false { w.write_with_tag(80, |w| w.write_bool(*&self.e2ee_enabled))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct CreateMeetingRequest {
    pub group_id: u64,
    pub channel_id: u32,
    pub e2ee_enabled: bool,
}

impl<'a> MessageRead<'a> for CreateMeetingRequest {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.group_id = r.read_uint64(bytes)?,
                Ok(16) => msg.channel_id = r.read_uint32(bytes)?,
                Ok(24) => msg.e2ee_enabled = r.read_bool(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl MessageWrite for CreateMeetingRequest {
    fn get_size(&self) -> usize {
        0
        + if self.group_id == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.group_id) as u64) }
        + if self.channel_id == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.channel_id) as u64) }
        + if self.e2ee_enabled == false { 0 } else { 1 + sizeof_varint(*(&self.e2ee_enabled) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.group_id != 0u64 { w.write_with_tag(8, |w| w.write_uint64(*&self.group_id))?; }
        if self.channel_id != 0u32 { w.write_with_tag(16, |w| w.write_uint32(*&self.channel_id))?; }
        if self.e2ee_enabled != false { w.write_with_tag(24, |w| w.write_bool(*&self.e2ee_enabled))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct CreateMeetingResponse<'a> {
    pub session: Option<firefly::GroupMeetingSession<'a>>,
    pub participant_token: Cow<'a, str>,
}

impl<'a> MessageRead<'a> for CreateMeetingResponse<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.session = Some(r.read_message::<firefly::GroupMeetingSession>(bytes)?),
                Ok(18) => msg.participant_token = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for CreateMeetingResponse<'a> {
    fn get_size(&self) -> usize {
        0
        + self.session.as_ref().map_or(0, |m| 1 + sizeof_len((m).get_size()))
        + if self.participant_token == "" { 0 } else { 1 + sizeof_len((&self.participant_token).len()) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if let Some(ref s) = self.session { w.write_with_tag(10, |w| w.write_message(s))?; }
        if self.participant_token != "" { w.write_with_tag(18, |w| w.write_string(&**&self.participant_token))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct JoinMeetingRequest {
    pub group_id: u64,
    pub session_id: u64,
}

impl<'a> MessageRead<'a> for JoinMeetingRequest {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.group_id = r.read_uint64(bytes)?,
                Ok(17) => msg.session_id = r.read_fixed64(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl MessageWrite for JoinMeetingRequest {
    fn get_size(&self) -> usize {
        0
        + if self.group_id == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.group_id) as u64) }
        + if self.session_id == 0u64 { 0 } else { 1 + 8 }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.group_id != 0u64 { w.write_with_tag(8, |w| w.write_uint64(*&self.group_id))?; }
        if self.session_id != 0u64 { w.write_with_tag(17, |w| w.write_fixed64(*&self.session_id))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct JoinMeetingResponse<'a> {
    pub session: Option<firefly::GroupMeetingSession<'a>>,
    pub participant_token: Cow<'a, str>,
}

impl<'a> MessageRead<'a> for JoinMeetingResponse<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.session = Some(r.read_message::<firefly::GroupMeetingSession>(bytes)?),
                Ok(18) => msg.participant_token = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for JoinMeetingResponse<'a> {
    fn get_size(&self) -> usize {
        0
        + self.session.as_ref().map_or(0, |m| 1 + sizeof_len((m).get_size()))
        + if self.participant_token == "" { 0 } else { 1 + sizeof_len((&self.participant_token).len()) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if let Some(ref s) = self.session { w.write_with_tag(10, |w| w.write_message(s))?; }
        if self.participant_token != "" { w.write_with_tag(18, |w| w.write_string(&**&self.participant_token))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct LeaveMeetingRequest {
    pub group_id: u64,
    pub session_id: u64,
}

impl<'a> MessageRead<'a> for LeaveMeetingRequest {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.group_id = r.read_uint64(bytes)?,
                Ok(17) => msg.session_id = r.read_fixed64(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl MessageWrite for LeaveMeetingRequest {
    fn get_size(&self) -> usize {
        0
        + if self.group_id == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.group_id) as u64) }
        + if self.session_id == 0u64 { 0 } else { 1 + 8 }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.group_id != 0u64 { w.write_with_tag(8, |w| w.write_uint64(*&self.group_id))?; }
        if self.session_id != 0u64 { w.write_with_tag(17, |w| w.write_fixed64(*&self.session_id))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct EndMeetingRequest {
    pub group_id: u64,
    pub session_id: u64,
}

impl<'a> MessageRead<'a> for EndMeetingRequest {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.group_id = r.read_uint64(bytes)?,
                Ok(17) => msg.session_id = r.read_fixed64(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl MessageWrite for EndMeetingRequest {
    fn get_size(&self) -> usize {
        0
        + if self.group_id == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.group_id) as u64) }
        + if self.session_id == 0u64 { 0 } else { 1 + 8 }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.group_id != 0u64 { w.write_with_tag(8, |w| w.write_uint64(*&self.group_id))?; }
        if self.session_id != 0u64 { w.write_with_tag(17, |w| w.write_fixed64(*&self.session_id))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct GetActiveSessionRequest {
    pub group_id: u64,
    pub channel_id: u32,
}

impl<'a> MessageRead<'a> for GetActiveSessionRequest {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.group_id = r.read_uint64(bytes)?,
                Ok(16) => msg.channel_id = r.read_uint32(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl MessageWrite for GetActiveSessionRequest {
    fn get_size(&self) -> usize {
        0
        + if self.group_id == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.group_id) as u64) }
        + if self.channel_id == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.channel_id) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.group_id != 0u64 { w.write_with_tag(8, |w| w.write_uint64(*&self.group_id))?; }
        if self.channel_id != 0u32 { w.write_with_tag(16, |w| w.write_uint32(*&self.channel_id))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct GetActiveSessionResponse<'a> {
    pub session: Option<firefly::GroupMeetingSession<'a>>,
}

impl<'a> MessageRead<'a> for GetActiveSessionResponse<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.session = Some(r.read_message::<firefly::GroupMeetingSession>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for GetActiveSessionResponse<'a> {
    fn get_size(&self) -> usize {
        0
        + self.session.as_ref().map_or(0, |m| 1 + sizeof_len((m).get_size()))
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if let Some(ref s) = self.session { w.write_with_tag(10, |w| w.write_message(s))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupMeetingSignal<'a> {
    pub group_id: u64,
    pub channel_id: u32,
    pub session_id: u64,
    pub type_pb: firefly::MeetingSignalType,
    pub username: Cow<'a, str>,
    pub cf_meeting_id: Cow<'a, str>,
}

impl<'a> MessageRead<'a> for GroupMeetingSignal<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.group_id = r.read_uint64(bytes)?,
                Ok(16) => msg.channel_id = r.read_uint32(bytes)?,
                Ok(25) => msg.session_id = r.read_fixed64(bytes)?,
                Ok(32) => msg.type_pb = r.read_enum(bytes)?,
                Ok(42) => msg.username = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(50) => msg.cf_meeting_id = r.read_string(bytes).map(Cow::Borrowed)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl<'a> MessageWrite for GroupMeetingSignal<'a> {
    fn get_size(&self) -> usize {
        0
        + if self.group_id == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.group_id) as u64) }
        + if self.channel_id == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.channel_id) as u64) }
        + if self.session_id == 0u64 { 0 } else { 1 + 8 }
        + if self.type_pb == firefly::MeetingSignalType::MEETING_SIGNAL_STARTED { 0 } else { 1 + sizeof_varint(*(&self.type_pb) as u64) }
        + if self.username == "" { 0 } else { 1 + sizeof_len((&self.username).len()) }
        + if self.cf_meeting_id == "" { 0 } else { 1 + sizeof_len((&self.cf_meeting_id).len()) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.group_id != 0u64 { w.write_with_tag(8, |w| w.write_uint64(*&self.group_id))?; }
        if self.channel_id != 0u32 { w.write_with_tag(16, |w| w.write_uint32(*&self.channel_id))?; }
        if self.session_id != 0u64 { w.write_with_tag(25, |w| w.write_fixed64(*&self.session_id))?; }
        if self.type_pb != firefly::MeetingSignalType::MEETING_SIGNAL_STARTED { w.write_with_tag(32, |w| w.write_enum(*&self.type_pb as i32))?; }
        if self.username != "" { w.write_with_tag(42, |w| w.write_string(&**&self.username))?; }
        if self.cf_meeting_id != "" { w.write_with_tag(50, |w| w.write_string(&**&self.cf_meeting_id))?; }
        Ok(())
    }
}

