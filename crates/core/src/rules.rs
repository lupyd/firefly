use std::collections::{HashMap, HashSet};

use firefly_protos::{
    deserialize_proto,
    firefly::{AuthToken, FireflyGroupChannel, FireflyGroupMember, FireflyGroupRole},
};

use log::info;
use mls_rs::{
    MlsRules,
    group::{
        GroupContext, Member, Roster, Sender,
        proposal::{CustomProposal, MlsCustomProposal, Proposal, RemoveProposal},
    },
    identity::SigningIdentity,
    mls_rules::{
        CommitDirection, CommitOptions, CommitSource, EncryptionOptions, ProposalBundle,
        ProposalInfo, ProposalSource,
    },
    ExtensionList,
};

use crate::{
    config::{
        FireflyCredential, FireflyError, UpdateChannelProposal, UpdateRoleInChannelProposal,
        UpdateRoleProposal, UpdateUserProposal, UserPermission, is_valid_name,
    },
    extension::{FireflyGroupExtension, FireflyGroupExtensionWrapper, OWNER_ROLE_ID},
    utils::{expiry_reference_time_secs, get_current_timestamp_in_secs},
};
pub const MAX_DEVICES_ALLOWED: usize = 5;

#[inline(always)]
pub const fn has_permission(permissions: u32, expected_permission: u32) -> bool {
    permissions & expected_permission == expected_permission
}

#[derive(Debug, thiserror::Error)]
#[error("{permission:?} permission required for channel {channel_id}")]
pub struct MessagePermissionDenied {
    pub permission: UserPermission,
    pub channel_id: u32,
}

impl FireflyMlsRules {
    /// Resolve permissions for an authenticated MLS roster member. Default-role
    /// members are deliberately omitted from the serialized member list.
    /// Channel overrides replace permissions; otherwise channel defaults apply.
    /// Channel zero without an explicit channel is the legacy group-wide stream.
    pub fn message_permissions(
        extension: &FireflyGroupExtensionWrapper<'_>,
        username: &str,
        channel_id: u32,
    ) -> u32 {
        let role = extension.get_role_of_user(username).unwrap_or(0);
        if extension.get_permissions_from_role_id(role).is_none() {
            return 0;
        }
        let group_role_perms = extension.get_permissions_from_role_id(role).unwrap_or(0);
        match extension.get_channel(channel_id) {
            Some(channel) => {
                let raw_perms = if let Some(r) = channel.roles.iter().find(|r| r.id == role) {
                    r.permissions
                } else if role > 0 {
                    group_role_perms
                } else if channel.default_permissions == 0 {
                    extension.inner().default_permissions
                } else {
                    channel.default_permissions
                };
                raw_perms & !(UserPermission::NullPermission as u32)
            }
            None if channel_id == 0 => group_role_perms & !(UserPermission::NullPermission as u32),
            None => 0,
        }
    }

    pub fn require_message_permission(
        extension: &FireflyGroupExtensionWrapper<'_>,
        username: &str,
        channel_id: u32,
        permission: UserPermission,
    ) -> Result<(), MessagePermissionDenied> {
        if has_permission(
            Self::message_permissions(extension, username, channel_id),
            permission as u32,
        ) {
            Ok(())
        } else {
            Err(MessagePermissionDenied {
                permission,
                channel_id,
            })
        }
    }

    /// Raw application bytes remain supported on the legacy group-wide stream.
    /// Both pin flags are authenticated inside the MLS ciphertext, never taken
    /// from unauthenticated relay metadata.
    pub fn message_metadata(data: &[u8]) -> (u32, u32) {
        match deserialize_proto::<firefly_protos::firefly::GroupMessageInner>(data) {
            Ok(inner) => {
                let nested_type = match inner.message {
                    firefly_protos::firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(p) => p.message_type,
                    _ => 0,
                };
                (inner.channelId, inner.message_type | nested_type)
            }
            Err(_) => (0, 0),
        }
    }

    pub fn check_message_sender(
        extension: &FireflyGroupExtensionWrapper<'_>,
        username: &str,
        data: &[u8],
    ) -> Result<(), MessagePermissionDenied> {
        if let Ok(inner) = deserialize_proto::<firefly_protos::firefly::GroupMessageInner>(data) {
            if let firefly_protos::firefly::mod_GroupMessageInner::OneOfmessage::pinSnapshot(snapshot) = inner.message {
                let denied=||MessagePermissionDenied {permission:UserPermission::PinMessage,channel_id:inner.channelId};
                if inner.message_type!=firefly_protos::MESSAGE_TYPE_HIDDEN || snapshot.messages.is_empty() || snapshot.messages.len()>100 || data.len()>4*1024*1024 {return Err(denied());}
                for record in snapshot.messages {
                    let record_inner=deserialize_proto::<firefly_protos::firefly::GroupMessageInner>(&record.message).map_err(|_|denied())?;
                    let (channel,flags)=Self::message_metadata(&record.message);
                    if flags & firefly_protos::MESSAGE_TYPE_HIDDEN!=0 || !matches!(record_inner.message,firefly_protos::firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(_)) {return Err(denied());}
                    Self::require_message_permission(extension,username,channel,UserPermission::SeeMessage)?;
                    Self::require_message_permission(extension,username,channel,UserPermission::PinMessage)?;
                }
                return Ok(());
            }
        }
        if let Ok(inner) = deserialize_proto::<firefly_protos::firefly::GroupMessageInner>(data) {
            if let firefly_protos::firefly::mod_GroupMessageInner::OneOfmessage::syncBundle(bundle) = inner.message {
                Self::require_message_permission(extension, username, 0, UserPermission::SeeMessage)?;
                if inner.message_type != firefly_protos::MESSAGE_TYPE_HIDDEN || inner.channelId != 0
                    || bundle.format_version != 1 || bundle.group_id == 0 || data.len() > 256 * 1024
                    || bundle.keys.len() > 32 || bundle.chunks.len() > 32 {
                    return Err(MessagePermissionDenied { permission: UserPermission::SeeMessage, channel_id: 0 });
                }
                // This transports capabilities only. Snapshot writes are independently
                // authorized by the server, and receivers bind keys to immutable bytes.
                return Ok(());
            }
        }
        let (channel_id, message_type) = Self::message_metadata(data);
        Self::require_message_permission(
            extension,
            username,
            channel_id,
            UserPermission::SeeMessage,
        )?;
        if let Ok(inner) = deserialize_proto::<firefly_protos::firefly::GroupMessageInner>(data) {
            if let firefly_protos::firefly::mod_GroupMessageInner::OneOfmessage::pinUpdate(update) = inner.message {
                // Both pin and unpin are authenticated control events, never chat.
                if update.message_id == 0 || inner.message_type != firefly_protos::MESSAGE_TYPE_HIDDEN {
                    return Err(MessagePermissionDenied { permission: UserPermission::PinMessage, channel_id });
                }
                return Self::require_message_permission(extension, username, channel_id, UserPermission::PinMessage);
            }
        }
        Self::require_message_permission(
            extension,
            username,
            channel_id,
            UserPermission::AddMessage,
        )?;
        if message_type & firefly_protos::MESSAGE_TYPE_PINNED != 0 {
            Self::require_message_permission(
                extension,
                username,
                channel_id,
                UserPermission::PinMessage,
            )?;
        }
        Ok(())
    }
}

pub fn does_signing_identity_has_this_username(username: &str, s: &SigningIdentity) -> bool {
    on_signing_identity(s, |x| x.username == username).unwrap_or(false)
}

pub fn get_username_from_signing_identity(s: &SigningIdentity) -> anyhow::Result<String> {
    on_signing_identity(s, |x| x.username.to_string())
}

fn auth_token_into_owned(x: &AuthToken) -> AuthToken<'static> {
    AuthToken {
        username: x.username.to_string().into(),
        valid_until: x.valid_until,
        credential: x.credential.to_vec().into(),
        address_id: x.address_id,
        device_id: x.device_id,
    }
}

pub fn get_auth_token_from_signing_identity(
    s: &SigningIdentity,
) -> anyhow::Result<AuthToken<'static>> {
    on_signing_identity(s, |x| auth_token_into_owned(&x))
}

// this borrow checker is annoying, find a cleaner way, rename for better understanding
pub fn on_signing_identity<F, T>(identity: &SigningIdentity, f: F) -> anyhow::Result<T>
where
    F: FnOnce(AuthToken) -> T,
{
    let credential = FireflyCredential::from_signing_identity(identity)?;
    let signed_token = credential.signed_token()?;
    let auth_token = deserialize_proto::<AuthToken>(&signed_token.payload)?;
    Ok(f(auth_token))
}

fn get_device_count(roster: &Roster, username: &str) -> usize {
    roster
        .members_iter()
        .filter(|x| does_signing_identity_has_this_username(username, &x.signing_identity))
        .count()
}

fn get_devices_already_in(roster: &Roster, username: &str) -> Vec<(Member, AuthToken<'static>)> {
    roster
        .members_iter()
        .filter_map(|x| {
            let token = on_signing_identity(&x.signing_identity, |z| {
                if z.username != username {
                    return None;
                }

                Some(auth_token_into_owned(&z))
            })
            .ok()??;

            Some((x, token))
        })
        .collect()
}

pub fn deserialize_identities(
    roster: &Roster,
) -> impl Iterator<Item = (Member, AuthToken<'static>)> {
    roster.members_iter().filter_map(|x| {
        let token = on_signing_identity(&x.signing_identity, |z| auth_token_into_owned(&z)).ok()?;
        Some((x, token))
    })
}

#[derive(Clone)]
pub struct FireflyMlsRules;

#[async_trait::async_trait]
impl MlsRules for FireflyMlsRules {
    type Error = FireflyError;

    #[doc = " This is called when preparing or receiving a commit to pre-process the set of committed"]
    #[doc = ""]
    #[doc = " will be presented for validation and filtering. Filter and validate will"]
    #[doc = " present a raw list of proposals. Standard MLS rules are applied internally"]
    #[doc = " on the result of these rules."]
    #[doc = ""]
    #[doc = " Each member of a group MUST apply the same proposal rules in order to"]
    #[doc = " maintain a working group."]
    #[doc = ""]
    #[doc = " Typically, any invalid proposal should result in an error. The exception are invalid"]
    #[doc = " by-reference proposals processed when _preparing_ a commit, which should be filtered"]
    #[doc = " out instead. This is to avoid the deadlock situation when no commit can be generated"]
    #[doc = " after receiving an invalid set of proposal messages."]
    #[doc = ""]
    #[doc = " `ProposalBundle` can be arbitrarily modified. For example, a Remove proposal that"]
    #[doc = " removes a moderator can result in adding a GroupContextExtensions proposal that updates"]
    #[doc = " the moderator list in the group context. The resulting `ProposalBundle` is validated"]
    #[doc = " by the library."]
    async fn filter_proposals(
        &self,
        direction: CommitDirection,
        source: CommitSource,
        current_roster: &Roster,
        current_context: &GroupContext,
        proposals: ProposalBundle,
    ) -> Result<ProposalBundle, Self::Error> {
        // A received commit is held to the rules every deployed client applies, or a member could
        // refuse a commit the others (and the server) took and be stranded at its epoch. The
        // stricter rules ([`StrictRules`]) bind what this client sends and what the server
        // accepts (`FireflyServerMlsRules`), which is where they take effect.
        let strict = direction == CommitDirection::Send;
        filter_proposals_with(direction, source, current_roster, current_context, proposals, strict)
    }

    #[doc = " This is called when preparing a commit to determine various options: whether to enforce an update"]
    #[doc = " path in case it is not mandated by MLS, whether to include the ratchet tree in the welcome"]
    #[doc = " message (if the commit adds members) and whether to generate a single welcome message, or one"]
    #[doc = " welcome message for each added member."]
    #[doc = ""]
    #[doc = " The `new_roster` and `new_extension_list` describe the group state after the commit."]
    fn commit_options(
        &self,
        _new_roster: &Roster,
        _new_context: &GroupContext,
        _proposals: &ProposalBundle,
    ) -> Result<CommitOptions, Self::Error> {
        Ok(CommitOptions::new()
            .with_single_welcome_message(true)
            .with_allow_external_commit(false))
    }

    #[doc = " This is called when sending any packet. For proposals and commits, this determines whether to"]
    #[doc = " encrypt them. For any encrypted packet, this determines the padding mode used."]
    #[doc = ""]
    #[doc = " Note that for commits, the `current_roster` and `current_extension_list` describe the group state"]
    #[doc = " before the commit, unlike in [commit_options](MlsRules::commit_options)."]
    fn encryption_options(
        &self,
        _current_roster: &Roster,
        _current_context: &GroupContext,
    ) -> Result<EncryptionOptions, Self::Error> {
        Ok(EncryptionOptions::new(
            false,
            mls_rs::client_builder::PaddingMode::None,
        ))
    }
}

/// [`MlsRules::filter_proposals`] for both rule sets: `strict` adds [`StrictRules`] on top of
/// the rules every member applies to a received commit.
pub fn filter_proposals_with(
    direction: CommitDirection,
    source: CommitSource,
    current_roster: &Roster,
    current_context: &GroupContext,
    proposals: ProposalBundle,
    strict: bool,
) -> Result<ProposalBundle, FireflyError> {
    let committer_signing_identity = match source {
        CommitSource::ExistingMember(member) => member.signing_identity,
        CommitSource::NewMember(signing_identity) => signing_identity,
    };

    let committer_credential =
        FireflyCredential::from_signing_identity(&committer_signing_identity)?;

    let committer_signed_token = committer_credential.signed_token()?;
    let _committer_auth_token =
        deserialize_proto::<AuthToken>(&committer_signed_token.payload)?;

    let current_extension_original = match current_context
        .extensions()
        .get_as::<FireflyGroupExtension>()?
    {
        Some(ext) => ext,
        None => return Err("No Firefly Group Extension Found".into()),
    };

    let mut members_removed = HashMap::<String, Vec<(u32, AuthToken)>>::new(); // not remove + add, kicked out
    let mut extension_updated = false;
    let mut filtered_proposals = ProposalBundle::default();
    let mut current_extension = current_extension_original.deserialize()?;
    let strict_rules = if strict {
        Some(StrictRules::new(current_roster, &current_extension, &proposals))
    } else {
        None
    };

    for proposal in proposals.into_proposals() {
        let mut implied_removes = Vec::new();
        let result = match &strict_rules {
            Some(rules) => rules.check(&proposal, current_roster, &current_extension),
            None => Ok(()),
        }
        .and_then(|()| check_proposal(
            &proposal,
            current_roster,
            &mut current_extension,
            &mut members_removed,
            &mut implied_removes,
        ));

        match result {
            Ok(updated) => {
                extension_updated |= updated;
                for index in implied_removes {
                    filtered_proposals.add(
                        Proposal::Remove(RemoveProposal::removing(index)?),
                        Sender::Member(0),
                        mls_rs::mls_rules::ProposalSource::Local,
                    );
                }
                filtered_proposals.add(proposal.proposal, proposal.sender, proposal.source);
            }
            Err(err) if should_drop_invalid_proposal(direction, &proposal.source) => {
                // a cached proposal anyone can broadcast must not block every future commit
                log::warn!("dropping invalid by-reference proposal from commit: {}", err);
            }
            Err(err) => return Err(err),
        }
    }

    if !members_removed.is_empty() {
        for (username, members) in members_removed {
            // means we removing every device of the member, hence the user is leaving the group, but if
            if get_device_count(current_roster, &username) == members.len() {
                current_extension.delete_member(&username);
                extension_updated = true;
            }
        }
    }

    if extension_updated {
        let new_extension = FireflyGroupExtension::new(current_extension)?;
        if !new_extension.equal(&current_extension_original) {
            let mut new_extensions = current_context.extensions().clone();
            new_extensions.set_from(new_extension)?;
            let gce_proposal = Proposal::GroupContextExtensions(new_extensions);
            filtered_proposals.add(
                gce_proposal,
                Sender::Member(0),
                mls_rs::mls_rules::ProposalSource::Local,
            );
            info!("Added Group Extension Update");
        }
    }

    return Ok(filtered_proposals);
}

/// A member whose every device has been expired for this long (it has not been online for its
/// credential lifetime and this much more) may have its last device removed even if it is the
/// owner, by a member allowed to manage members. Until then the owner's last device stays, so
/// the owner role (held in the group extension only while the owner has a device) is never lost
/// to a few weeks offline. See [`StrictRules`].
pub const OWNER_LAST_DEVICE_GRACE_SECS: u64 = 30 * 24 * 60 * 60;

/// One leaf of the roster, as [`StrictRules`] sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RosterDevice {
    pub index: u32,
    pub username: String,
    pub valid_until: u64,
}

impl RosterDevice {
    pub fn from_roster(roster: &Roster) -> Vec<RosterDevice> {
        deserialize_identities(roster)
            .map(|(member, token)| RosterDevice {
                index: member.index,
                username: token.username.into_owned(),
                valid_until: token.valid_until,
            })
            .collect()
    }
}

/// Rules on top of those every member applies to a received commit. They bind only commits this
/// client builds and commits the server accepts, never what a member accepts from others:
/// deployed clients apply the older rules, and commits accepted under them are still in every
/// backlog, so refusing one on receipt would strand the member at that epoch. Since the server
/// refuses commits that break them, honest members only ever receive commits that keep them.
///
/// - Only an owner (a holder of `OWNER_ROLE_ID`) may remove a device of an owner whose
///   credential is live, whatever role or permissions the remover holds.
/// - Another member may remove an owner's expired devices, oldest first, but never the owner's
///   last device in the group, unless it holds ManageMember and every one of the owner's
///   devices has been expired for [`OWNER_LAST_DEVICE_GRACE_SECS`]. (Removing a user's last
///   device drops their entry from the group extension, which for the owner deletes the owner
///   role for good: nothing can grant it again.)
/// - Only an owner may give `OWNER_ROLE_ID` to anyone, change an owner's role, or change or
///   delete `OWNER_ROLE_ID` (in the group or in a channel).
///
/// Expiry is judged against the current time: the server's clock for the server, which is what
/// decides; the committer's for its own commits, which the server checks again.
pub struct StrictRules {
    owners: HashSet<String>,
    devices: Vec<RosterDevice>,
    removed: HashSet<u32>,
    now: u64,
}

impl StrictRules {
    pub fn new(
        roster: &Roster,
        extension: &FireflyGroupExtensionWrapper,
        proposals: &ProposalBundle,
    ) -> Self {
        Self::from_parts(
            extension,
            RosterDevice::from_roster(roster),
            proposals.remove_proposals().iter().map(|p| p.proposal.to_remove()),
            get_current_timestamp_in_secs(),
        )
    }

    /// `removed`: every leaf the commit removes explicitly.
    pub fn from_parts(
        extension: &FireflyGroupExtensionWrapper,
        devices: Vec<RosterDevice>,
        removed: impl IntoIterator<Item = u32>,
        now: u64,
    ) -> Self {
        let owners = extension
            .inner()
            .members
            .iter()
            .filter(|m| m.role == OWNER_ROLE_ID)
            .map(|m| m.username.to_string())
            .collect();
        Self { owners, devices, removed: removed.into_iter().collect(), now }
    }

    fn is_owner(&self, username: &str) -> bool {
        self.owners.contains(username)
    }

    /// Checks one proposal of the commit; `extension` is the group extension as updated by the
    /// proposals before it.
    pub fn check(
        &self,
        proposal: &ProposalInfo<Proposal>,
        roster: &Roster,
        extension: &FireflyGroupExtensionWrapper,
    ) -> Result<(), FireflyError> {
        let Sender::Member(sender_index) = proposal.sender else {
            return Err("Sender has to be member".into());
        };
        let sender = get_username_from_signing_identity(
            &roster.member_with_index(sender_index)?.signing_identity,
        )?;
        match &proposal.proposal {
            Proposal::Remove(remove) => {
                let permissions = sender_permissions(extension, &sender)?;
                self.check_remove(&sender, permissions, remove.to_remove())
            }
            Proposal::Custom(custom) => self.check_custom(&sender, custom, extension),
            _ => Ok(()),
        }
    }

    pub fn check_remove(
        &self,
        sender: &str,
        sender_permissions: u32,
        index: u32,
    ) -> Result<(), FireflyError> {
        let Some(target) = self.devices.iter().find(|d| d.index == index) else {
            return Err(format!("no member at leaf {index}").into());
        };
        if target.username == sender || !self.is_owner(&target.username) || self.is_owner(sender) {
            return Ok(());
        }
        if target.valid_until >= self.now {
            return Err(format!(
                "rejected remove proposal, only the owner can remove a live device of the owner {}",
                target.username
            )
            .into());
        }
        let owner_devices = || self.devices.iter().filter(|d| d.username == target.username);
        if owner_devices().any(|d| !self.removed.contains(&d.index)) {
            // expired, and the owner keeps a device in the group
            return Ok(());
        }
        let newest = owner_devices().map(|d| d.valid_until).max().unwrap_or(0);
        if has_permission(sender_permissions, UserPermission::ManageMember as u32)
            && newest.saturating_add(OWNER_LAST_DEVICE_GRACE_SECS) < self.now
        {
            return Ok(());
        }
        Err(format!(
            "rejected remove proposal, the owner {}'s last device stays in the group",
            target.username
        )
        .into())
    }

    pub fn check_custom(
        &self,
        sender: &str,
        proposal: &CustomProposal,
        extension: &FireflyGroupExtensionWrapper,
    ) -> Result<(), FireflyError> {
        if self.is_owner(sender) {
            return Ok(());
        }
        let proposal_type = proposal.proposal_type();
        let touches_owner = if proposal_type == UpdateUserProposal::proposal_type() {
            let update = UpdateUserProposal::from_custom_proposal(proposal)?;
            update.role_id == OWNER_ROLE_ID
                || self.is_owner(&update.username)
                || extension.is_owner(&update.username)
        } else if proposal_type == UpdateRoleProposal::proposal_type() {
            UpdateRoleProposal::from_custom_proposal(proposal)?.role_id == OWNER_ROLE_ID
        } else if proposal_type == UpdateRoleInChannelProposal::proposal_type() {
            UpdateRoleInChannelProposal::from_custom_proposal(proposal)?.role_proposal.role_id
                == OWNER_ROLE_ID
        } else {
            false
        };
        if touches_owner {
            return Err(format!(
                "rejected proposal from {sender}, only the owner can change the owner role or who holds it"
            )
            .into());
        }
        Ok(())
    }
}

/// The server's rules: [`FireflyMlsRules`] plus [`StrictRules`] for every commit it accepts.
#[derive(Clone)]
pub struct FireflyServerMlsRules;

#[async_trait::async_trait]
impl MlsRules for FireflyServerMlsRules {
    type Error = FireflyError;

    async fn filter_proposals(
        &self,
        direction: CommitDirection,
        source: CommitSource,
        current_roster: &Roster,
        current_context: &GroupContext,
        proposals: ProposalBundle,
    ) -> Result<ProposalBundle, Self::Error> {
        filter_proposals_with(direction, source, current_roster, current_context, proposals, true)
    }

    fn commit_options(
        &self,
        new_roster: &Roster,
        new_context: &GroupContext,
        proposals: &ProposalBundle,
    ) -> Result<CommitOptions, Self::Error> {
        FireflyMlsRules.commit_options(new_roster, new_context, proposals)
    }

    fn encryption_options(
        &self,
        current_roster: &Roster,
        current_context: &GroupContext,
    ) -> Result<EncryptionOptions, Self::Error> {
        FireflyMlsRules.encryption_options(current_roster, current_context)
    }
}

/// Only invalid by-reference proposals met while _preparing_ our own commit are dropped;
/// a received commit carrying any invalid proposal is rejected as a whole.
pub fn should_drop_invalid_proposal(direction: CommitDirection, source: &ProposalSource) -> bool {
    direction == CommitDirection::Send && matches!(source, ProposalSource::ByReference(_))
}

/// Validates one proposal against the group rules. Removals implied by an Add are pushed to
/// `implied_removes`, and nothing is recorded or mutated unless the proposal is accepted.
/// Returns whether the firefly extension was updated.
fn check_proposal(
    proposal: &ProposalInfo<Proposal>,
    current_roster: &Roster,
    current_extension: &mut FireflyGroupExtensionWrapper,
    members_removed: &mut HashMap<String, Vec<(u32, AuthToken<'static>)>>,
    implied_removes: &mut Vec<u32>,
) -> Result<bool, FireflyError> {
    let Sender::Member(sender_idx) = proposal.sender else {
        return Err("Sender has to be member".into());
    };

    let sender_member = current_roster.member_with_index(sender_idx)?;

    let sender_username = get_username_from_signing_identity(&sender_member.signing_identity)?;

    let sender_permissions = sender_permissions(current_extension, &sender_username)?;

    match &proposal.proposal {
        Proposal::Add(proposal) => {
            let Ok(addee_identity) =
                get_auth_token_from_signing_identity(proposal.key_package().signing_identity())
            else {
                return Err(("Invalid credential in add proposal").into());
            };

            if addee_identity.username != sender_username
                && !has_permission(sender_permissions, UserPermission::ManageMember as u32)
            {
                return Err(format!(
                    "rejected proposal, adder {} don't have permission to manage user",
                    sender_username,
                )
                .into());
            }
            let devices = get_devices_already_in(current_roster, &addee_identity.username);

            let device_count = devices.len();

            if device_count >= MAX_DEVICES_ALLOWED && devices.len() <= 1 {
                return Err(format!("MAX_DEVICES_ALLOWED {MAX_DEVICES_ALLOWED} reached").into());
            }

            if let Some((member, old_device_with_same_address)) = devices
                .iter()
                .find(|(_, x)| x.address_id == addee_identity.address_id)
            {
                log::info!(
                    "found old device with same address {:?}, removing index: {}",
                    old_device_with_same_address,
                    member.index
                );
                implied_removes.push(member.index);
            }

            if device_count >= MAX_DEVICES_ALLOWED {
                if let Some((old_member, old_token)) =
                    devices.iter().min_by_key(|(_, tok)| tok.valid_until)
                {
                    log::info!(
                        "MAX_DEVICES_ALLOWED reached. Auto-removing oldest device of user {} (index: {}, device_id: {})",
                        addee_identity.username,
                        old_member.index,
                        old_token.device_id
                    );
                    implied_removes.push(old_member.index);
                }
            }
            Ok(false)
        }
        Proposal::Remove(proposal) => {
            let member_to_remove = current_roster.member_with_index(proposal.to_remove())?;

            let auth_token = get_auth_token_from_signing_identity(&member_to_remove.signing_identity)?;

            if auth_token.username == sender_username {
                // User removing their own device (leaving or cleanup) is always allowed
            } else {
                // Check if this is an inactive/stale device that any member can clean up.
                // An inactive device is:
                // 1. A device whose token has expired (valid_until < now), OR
                // 2. An older device when multiple devices exist for the user, leaving the most recent device.
                let target_devices = get_devices_already_in(current_roster, &auth_token.username);
                // judged at the commit's server time when it is known (see utils::with_commit_time)
                let is_inactive_token = auth_token.valid_until < expiry_reference_time_secs();
                let is_stale_device = if target_devices.len() > 1 {
                    let most_recent_valid_until = target_devices
                        .iter()
                        .map(|(_, tok)| tok.valid_until)
                        .max()
                        .unwrap_or(0);
                    auth_token.valid_until < most_recent_valid_until
                } else {
                    false
                };

                if is_inactive_token || is_stale_device {
                    log::info!(
                        "Allowing removal of {}'s inactive/stale device by {}",
                        auth_token.username,
                        sender_username
                    );
                } else {
                    // Removing an active/primary device requires ManageMember permission
                    if !has_permission(sender_permissions, UserPermission::ManageMember as u32) {
                        return Err(format!(
                            "rejected remove proposal, remover {} does not have permission to manage members",
                            sender_username
                        )
                        .into());
                    }

                    // A member cannot remove someone who has permissions that the remover does not have
                    let target_role_id = current_extension
                        .get_role_of_user(&auth_token.username)
                        .unwrap_or_default();

                    if let Some(target_permissions) =
                        current_extension.get_permissions_from_role_id(target_role_id)
                    {
                        if !has_permission(sender_permissions, target_permissions) {
                            return Err(format!(
                                "rejected remove proposal, remover {} does not have permissions to remove {}",
                                sender_username, auth_token.username
                            )
                            .into());
                        }
                    }
                }
            }

            members_removed
                .entry(auth_token.username.to_string())
                .or_default()
                .push((proposal.to_remove(), auth_token));
            Ok(false)
        }
        Proposal::Update(proposal) => {
            if !does_signing_identity_has_this_username(&sender_username, proposal.signing_identity())
            {
                return Err(format!(
                    "update proposal rejected from {} as username don't match",
                    sender_username
                )
                .into());
            }
            Ok(false)
        }
        other => check_group_proposal(other, current_extension, &sender_username),
    }
}

fn sender_permissions(
    current_extension: &FireflyGroupExtensionWrapper,
    sender_username: &str,
) -> Result<u32, FireflyError> {
    let sender_role = current_extension
        .get_role_of_user(sender_username)
        .unwrap_or_default();

    if sender_role == 0 {
        Ok(current_extension.default_permissions())
    } else {
        current_extension
            .get_permissions_from_role_id(sender_role)
            .ok_or(FireflyError::from("No Role with that index exists"))
    }
}

/// Validates the proposals whose rules depend only on the sender and the firefly extension
/// (everything except Add, Remove and Update). Returns whether the extension was updated.
pub fn check_group_proposal(
    proposal: &Proposal,
    current_extension: &mut FireflyGroupExtensionWrapper,
    sender_username: &str,
) -> Result<bool, FireflyError> {
    let sender_permissions = sender_permissions(current_extension, sender_username)?;

    match proposal {
        // Nothing distributes pre-shared keys, so a PSK only the committer holds would leave every
        // other member unable to process the commit while the server (which never needs the PSK)
        // advances the epoch: the group would be bricked.
        Proposal::Psk(_) => Err("PSK proposals are not allowed".into()),
        // nothing handles a reinitialised group, so accepting one would brick the group
        Proposal::ReInit(_) => Err("ReInit proposals are not allowed".into()),
        Proposal::ExternalInit(_) => Err(("External Init not allowed").into()),
        Proposal::GroupContextExtensions(extensions) => {
            validate_group_context_extensions(current_extension, sender_username, extensions)?;
            Ok(false)
        }
        Proposal::Custom(custom) => {
            log::info!("handling custom proposal {:?}", custom);
            match handle_custom_proposal(
                custom,
                current_extension,
                sender_permissions,
                sender_username,
            ) {
                Ok(updated) => {
                    log::info!("extension updated {}", updated);
                    Ok(updated)
                }
                Err(err) => {
                    Err(format!("rejected proposal {:?} because of {}", custom, err).into())
                }
            }
        }
        _ => Err("invalid proposal".into()),
    }
}

/// A GroupContextExtensions proposal replaces the whole rule set at once, bypassing every
/// custom proposal check, so only the owner may send one and the result must stay well formed.
pub fn validate_group_context_extensions(
    current_extension: &FireflyGroupExtensionWrapper,
    sender_username: &str,
    extensions: &ExtensionList,
) -> Result<(), FireflyError> {
    if !current_extension.is_owner(sender_username) {
        return Err(format!(
            "rejected group context extensions from {}, only the owner can replace them",
            sender_username
        )
        .into());
    }

    let Some(new_extension) = extensions.get_as::<FireflyGroupExtension>()? else {
        return Err("group context extensions are missing the Firefly Group Extension".into());
    };

    if !new_extension.deserialize()?.is_valid() {
        return Err("group context extensions carry an invalid Firefly Group Extension".into());
    }

    Ok(())
}

pub fn handle_custom_proposal(
    proposal: &CustomProposal,
    current_extension: &mut FireflyGroupExtensionWrapper,
    sender_permissions: u32,
    sender_username: &str,
) -> anyhow::Result<bool> {
    let sender_is_owner = current_extension.is_owner(sender_username);
    let proposal_type = proposal.proposal_type();
    if proposal_type == UpdateUserProposal::proposal_type() {
        let update_user = UpdateUserProposal::from_custom_proposal(proposal)?;

        let is_existing_member = current_extension.get_role_of_user(&update_user.username).is_some();
        let required_permission = if is_existing_member {
            UserPermission::ManageRole as u32
        } else {
            UserPermission::ManageMember as u32
        };

        if has_permission(sender_permissions, required_permission)
            || has_permission(sender_permissions, UserPermission::ManageRole as u32)
        {
            if update_user.role_id != 0 {
                let Some(permissions) =
                    current_extension.get_permissions_from_role_id(update_user.role_id)
                else {
                    return Err(anyhow::anyhow!("the role does not exist"));
                };

                // member can only give any role that has subset of permissions of themselves to others
                if !has_permission(sender_permissions, permissions) {
                    return Err(anyhow::anyhow!("not enough permissions"));
                }

                // member cannot change the role of someone whose current role has permissions the sender lacks
                if let Some(target_current_role) = current_extension.get_role_of_user(&update_user.username) {
                    if let Some(target_permissions) = current_extension.get_permissions_from_role_id(target_current_role) {
                        if !has_permission(sender_permissions, target_permissions) {
                            return Err(anyhow::anyhow!("not enough permissions to modify target user's role"));
                        }
                    }
                }

                current_extension
                    .update_member(FireflyGroupMember {
                        username: update_user.username.into(),
                        role: update_user.role_id,
                    })
                    .ok_or(anyhow::anyhow!("role doesn't exist"))?;
                return Ok(true);
            } else {
                // default permissions will go to any new member with no questions asked
                return Ok(false);
            }
        } else {
            let req_name = if is_existing_member { "ManageRole" } else { "ManageMember" };
            return Err(anyhow::anyhow!("{} permission required to update member roles", req_name));
        }
    } else if proposal_type == UpdateRoleProposal::proposal_type() {
        let update_role_proposal = UpdateRoleProposal::from_custom_proposal(proposal)?;

        if has_permission(sender_permissions, UserPermission::ManageRole as u32) {
            if update_role_proposal.delete {
                let Some(role_permissions) =
                    current_extension.get_permissions_from_role_id(update_role_proposal.role_id)
                else {
                    return Err(anyhow::anyhow!("role id does not exist"));
                };

                // member can update/delete any role that has subset of permissions of themselves
                if !has_permission(sender_permissions, role_permissions) {
                    return Err(anyhow::anyhow!("not enough permissions"));
                }

                if current_extension
                    .delete_role(update_role_proposal.role_id.into())
                    .is_some()
                {
                    return Ok(true);
                }

                return Err(anyhow::anyhow!("role does not exist"));
            }

            // make sure the sender can have the permissions they can provide
            if !has_permission(sender_permissions, update_role_proposal.permissions) {
                return Err(anyhow::anyhow!("not enough permissions"));
            }

            if let Some(role_permissions) =
                current_extension.get_permissions_from_role_id(update_role_proposal.role_id)
            {
                // if the previous permissions of the role are higher than the sender's permissions, the sender cannot de escalate the role
                if !has_permission(sender_permissions, role_permissions) {
                    return Err(anyhow::anyhow!(
                        "not enough permissions to modify existing role"
                    ));
                }
            };

            if !is_valid_name(&update_role_proposal.name) {
                return Err(anyhow::anyhow!("invalid role name"));
            }

            current_extension.update_role(FireflyGroupRole {
                id: update_role_proposal.role_id.into(),
                name: update_role_proposal.name.into(),
                permissions: update_role_proposal.permissions.into(),
                color: update_role_proposal.color.into(),
            });
            return Ok(true);
        } else {
            return Err(anyhow::anyhow!("not enough permissions"));
        }
    } else if proposal_type == UpdateChannelProposal::proposal_type() {
        let update_channel_proposal = UpdateChannelProposal::from_custom_proposal(proposal)?;

        if let Some(sender_permissions_in_channel) = current_extension
            .get_permissions_of_user_in_channel(sender_username, update_channel_proposal.id)
        {
            if !(has_permission(sender_permissions, UserPermission::ManageChannel as u32)
                || has_permission(
                    sender_permissions_in_channel,
                    UserPermission::ManageChannel as u32,
                ))
            {
                return Err(anyhow::anyhow!("ManageChannel permission required"));
            }

            if update_channel_proposal.delete {
                current_extension
                    .delete_channel(update_channel_proposal.id)
                    .ok_or(anyhow::anyhow!("channel does not exist"))?;
                return Ok(true);
            }

            // channel defaults can never exceed what the sender holds, at root or in the channel
            if sender_is_owner
                || (has_permission(sender_permissions, UserPermission::ManageChannel as u32)
                    && has_permission(sender_permissions, update_channel_proposal.default_permissions))
                || (has_permission(
                    sender_permissions_in_channel,
                    update_channel_proposal.default_permissions,
                ) && has_permission(
                    sender_permissions_in_channel,
                    current_extension
                        .get_channel(update_channel_proposal.id)
                        .ok_or(anyhow::anyhow!("channel does not exist"))?
                        .default_permissions,
                ))
            {
                if !is_valid_name(&update_channel_proposal.name) {
                    return Err(anyhow::anyhow!("invalid channel name"));
                }

                let channel_info = FireflyGroupChannel {
                    id: update_channel_proposal.id,
                    name: update_channel_proposal.name.into(),
                    type_pb: update_channel_proposal.channel_ty as u32,
                    roles: Vec::new(),
                    default_permissions: update_channel_proposal.default_permissions,
                };

                current_extension.update_channel(channel_info);
                return Ok(true);
            } else {
                return Err(anyhow::anyhow!(
                    "default permissions are higher than sender to modify"
                ));
            }
        } else {
            // channel does not exist, so user just have to have ManageMember permission
            if !has_permission(sender_permissions, UserPermission::ManageChannel as u32) {
                return Err(anyhow::anyhow!("ManageChannel permission required"));
            }

            if update_channel_proposal.delete {
                return Err(anyhow::anyhow!("the channel does not exist to delete"));
            }

            if !is_valid_name(&update_channel_proposal.name) {
                return Err(anyhow::anyhow!("invalid channel name"));
            }

            // channel level permissions such as ManageRole would otherwise be minted here and then
            // used to rewrite every role inside the channel, so the creator can only grant what they hold
            if !sender_is_owner
                && !has_permission(sender_permissions, update_channel_proposal.default_permissions)
            {
                return Err(anyhow::anyhow!(
                    "default permissions are higher than sender to create"
                ));
            }

            let channel_info = FireflyGroupChannel {
                id: update_channel_proposal.id,
                name: update_channel_proposal.name.into(),
                type_pb: update_channel_proposal.channel_ty as u32,
                roles: Vec::new(),
                default_permissions: update_channel_proposal.default_permissions,
            };

            current_extension.update_channel(channel_info);
            return Ok(true);
        }
    } else if proposal_type == UpdateRoleInChannelProposal::proposal_type() {
        let update_role_in_channel_proposal =
            UpdateRoleInChannelProposal::from_custom_proposal(proposal)?;
        let role_proposal = update_role_in_channel_proposal.role_proposal;
        let channel_id = update_role_in_channel_proposal.channel_id;
        let Some(sender_permissions_in_channel) =
            current_extension.get_permissions_of_user_in_channel(sender_username, channel_id)
        else {
            return Err(anyhow::anyhow!("channel id does not exist"));
        };

        if role_proposal.delete {
            // sender has root level permission to update channels and everything inside channels
            if has_permission(sender_permissions, UserPermission::ManageChannel as u32)
                ||
                // updater inside the channel has ManageRole permission
                (has_permission(
                    sender_permissions_in_channel,
                    UserPermission::ManageRole as u32,
                ) &&
                // updater has subset of permissions of role, hence can modify/delete it, that is the role < updater
                has_permission(
                    sender_permissions_in_channel,
                    current_extension
                        .get_permissions_from_role_id_in_channel(role_proposal.role_id, channel_id)
                        .ok_or(anyhow::anyhow!("channel id does not exist"))?,
                ))
            {
                current_extension.delete_channel_role(channel_id, role_proposal.role_id);
                return Ok(true);
            } else {
                return Err(anyhow::anyhow!("sender does not have permissions"));
            }
        }

        if let Some(role_permissions_in_channel) = current_extension
            .get_permissions_from_role_id_in_channel(role_proposal.role_id, channel_id)
        {
            // role exists

            // role 0 here is the channel's default, which applies to every default-role member
            if sender_is_owner
                || (
                    // root level manage channel permission or manage role permission in channel
                    (has_permission(sender_permissions, UserPermission::ManageChannel as u32)
                    || has_permission(
                    sender_permissions_in_channel,
                    UserPermission::ManageRole as u32,
                )) &&

                    // proposed permissions are subset of sender permissions in channel
                    has_permission(sender_permissions_in_channel, role_proposal.permissions)
                    &&
                    // the role to be changed is subset of sender permissions in channel and not higher level
                    has_permission(sender_permissions_in_channel, role_permissions_in_channel)
                )
            {
                current_extension.update_channel_role_permissions(
                    channel_id,
                    role_proposal.role_id,
                    role_proposal.permissions,
                );
                return Ok(true);
            } else {
                return Err(anyhow::anyhow!("sender does not have permissions"));
            }
        } else {
            // every group role (and role 0) resolves to permissions in an existing channel, so
            // only roles missing from the group get here; overriding them would leave a dangling
            // override that a later role with the same id silently inherits
            return Err(anyhow::anyhow!("role does not exist"));
        }
    } else {
        return Err(anyhow::anyhow!("invalid custom proposal type"));
    }
}
