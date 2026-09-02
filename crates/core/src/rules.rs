use std::collections::HashMap;

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
    mls_rules::{CommitDirection, CommitOptions, CommitSource, EncryptionOptions, ProposalBundle},
};

use crate::{
    config::{
        FireflyCredential, FireflyError, UpdateChannelProposal, UpdateRoleInChannelProposal,
        UpdateRoleProposal, UpdateUserProposal, UserPermission, is_valid_name,
    },
    extension::{FireflyGroupExtension, FireflyGroupExtensionWrapper},
    utils::get_current_timestamp_in_secs,
};
pub const MAX_DEVICES_ALLOWED: usize = 5;

#[inline(always)]
pub const fn has_permission(permissions: u32, expected_permission: u32) -> bool {
    permissions & expected_permission == expected_permission
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

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
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
        _direction: CommitDirection,
        source: CommitSource,
        current_roster: &Roster,
        current_context: &GroupContext,
        proposals: ProposalBundle,
    ) -> Result<ProposalBundle, Self::Error> {
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

        for proposal in proposals.into_proposals() {
            let Sender::Member(sender_idx) = proposal.sender else {
                return Err("Sender has to be member".into());
            };

            let sender_member = current_roster.member_with_index(sender_idx)?;

            let sender_username =
                get_username_from_signing_identity(&sender_member.signing_identity)?;

            let sender_role = current_extension
                .get_role_of_user(&sender_username)
                .unwrap_or_default();

            let sender_permissions = if sender_role == 0 {
                current_extension.default_permissions()
            } else {
                current_extension
                    .get_permissions_from_role_id(sender_role)
                    .ok_or(FireflyError::from("No Role with that index exists"))?
            };

            match &proposal.proposal {
                Proposal::Add(proposal) => {
                    let Ok(addee_identity) = get_auth_token_from_signing_identity(
                        proposal.key_package().signing_identity(),
                    ) else {
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

                    if let Some((member, old_device_with_same_address)) = devices
                        .iter()
                        .find(|(_, x)| x.address_id == addee_identity.address_id)
                    {
                        log::info!(
                            "found old device with same address {:?}, removing index: {}",
                            old_device_with_same_address,
                            member.index
                        );
                        filtered_proposals.add(
                            Proposal::Remove(RemoveProposal::removing(member.index)?),
                            Sender::Member(0),
                            mls_rs::mls_rules::ProposalSource::Local,
                        );
                    }

                    if device_count >= MAX_DEVICES_ALLOWED && devices.len() > 1 {
                        if let Some((old_member, old_token)) = devices
                            .iter()
                            .min_by_key(|(_, tok)| tok.valid_until)
                        {
                            log::info!(
                                "MAX_DEVICES_ALLOWED reached. Auto-removing oldest device of user {} (index: {}, device_id: {})",
                                addee_identity.username,
                                old_member.index,
                                old_token.device_id
                            );
                            filtered_proposals.add(
                                Proposal::Remove(RemoveProposal::removing(old_member.index)?),
                                Sender::Member(0),
                                mls_rs::mls_rules::ProposalSource::Local,
                            );
                        }
                    } else if device_count >= MAX_DEVICES_ALLOWED {
                        return Err(
                            format!("MAX_DEVICES_ALLOWED {MAX_DEVICES_ALLOWED} reached").into()
                        );
                    }
                }
                Proposal::Remove(proposal) => {
                    let member_to_remove =
                        current_roster.member_with_index(proposal.to_remove())?;

                    let auth_token =
                        get_auth_token_from_signing_identity(&member_to_remove.signing_identity)?;

                    if auth_token.username == sender_username {
                        // User removing their own device (leaving or cleanup) is always allowed
                    } else {
                        // Check if this is an inactive/stale device that any member can clean up.
                        // An inactive device is:
                        // 1. A device whose token has expired (valid_until < now), OR
                        // 2. An older device when multiple devices exist for the user, leaving the most recent device.
                        let target_devices =
                            get_devices_already_in(current_roster, &auth_token.username);
                        let is_inactive_token =
                            auth_token.valid_until < get_current_timestamp_in_secs();
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
                            if !has_permission(
                                sender_permissions,
                                UserPermission::ManageMember as u32,
                            ) {
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

                            if let Some(target_permissions) = current_extension.get_permissions_from_role_id(target_role_id) {
                                if !has_permission(sender_permissions, target_permissions) {
                                    return Err(format!(
                                        "rejected remove proposal, remover {} does not have permissions to remove {}",
                                        sender_username,
                                        auth_token.username
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
                }
                Proposal::Update(proposal) => {
                    if !does_signing_identity_has_this_username(
                        &sender_username,
                        proposal.signing_identity(),
                    ) {
                        return Err(format!(
                            "update proposal rejected from {} as username don't match",
                            sender_username
                        )
                        .into());
                    }
                }
                Proposal::Psk(_) => {
                    info!("WARN: PSK proposal included, as I don't know what it suppose to do");
                }
                Proposal::ReInit(_) => {
                    info!("WARN: allowing REINIT Proposal from {}", sender_username);
                }
                Proposal::ExternalInit(_) => {
                    return Err(("External Init not allowed").into());
                }
                Proposal::GroupContextExtensions(_extensions) => {
                    if !has_permission(sender_permissions, UserPermission::ManageGroup as u32) {
                        return Err(
                            ("Skipping proposal because user doesn't have ManageGroup permission")
                                .into(),
                        );
                    }
                }
                Proposal::Custom(custom) => {
                    log::info!("handling custom proposal {:?}", custom);
                    match handle_custom_proposal(
                        &custom,
                        &current_roster,
                        &mut current_extension,
                        sender_permissions,
                        sender_username.as_str(),
                    ) {
                        Ok(updated) => {
                            log::info!("extension updated {}", updated);
                            if updated {
                                extension_updated = updated;
                            }
                        }
                        Err(err) => {
                            return Err(format!(
                                "rejected proposal {:?} because of {}",
                                custom, err
                            )
                            .into());
                        }
                    }
                }

                _ => {
                    return Err("invalid proposal".into());
                }
            }

            filtered_proposals.add(proposal.proposal, proposal.sender, proposal.source);
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

fn handle_custom_proposal(
    proposal: &CustomProposal,
    _current_roster: &Roster,
    current_extension: &mut FireflyGroupExtensionWrapper,
    sender_permissions: u32,
    sender_username: &str,
) -> anyhow::Result<bool> {
    let proposal_type = proposal.proposal_type();
    if proposal_type == UpdateUserProposal::proposal_type() {
        let update_user = UpdateUserProposal::from_custom_proposal(proposal)?;

        if has_permission(sender_permissions, UserPermission::ManageRole as u32) {
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
            return Err(anyhow::anyhow!("ManageRole permission required to update member roles"));
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

            if has_permission(sender_permissions, UserPermission::ManageChannel as u32)
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

            // since the permissions from here apply to only channel level, we don't need to check if they are subset of creator's permissions

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

            // root level manage channel permission
            if has_permission(sender_permissions, UserPermission::ManageChannel as u32)
                || (
                    // manage role permission in channel
                    has_permission(
                    sender_permissions_in_channel,
                    UserPermission::ManageRole as u32,
                ) &&

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
            // role does not exist, to be created

            // updater has root level ManageChannel permission
            if has_permission(sender_permissions, UserPermission::ManageChannel as u32)
                ||
                // updater has ManageRole permission in channel
                (has_permission(
                    sender_permissions_in_channel,
                    UserPermission::ManageRole as u32,
                )
                // updater's permissions in channel are subset of new role permissions
                && has_permission(sender_permissions_in_channel, role_proposal.permissions))
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
        }
    } else {
        return Err(anyhow::anyhow!("invalid custom proposal type"));
    }
}
