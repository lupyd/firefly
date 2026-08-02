use firefly_protos::{
    deserialize_proto,
    firefly::{FireflyGroupChannel, FireflyGroupRole},
    serialize_proto,
};
use mls_rs::{
    extension::{ExtensionType, MlsCodecExtension},
    mls_rs_codec::{MlsDecode, MlsEncode, MlsSize},
};

use crate::{
    protos::{self},
    sorted_search::SortedSearch,
};

#[derive(MlsSize, MlsDecode, MlsEncode, Default, Debug)]
pub struct FireflyGroupExtension {
    inner: Vec<u8>,
}

impl FireflyGroupExtension {
    pub fn serialize(&self) -> Vec<u8> {
        self.inner.clone()
    }

    pub fn deserialize<'a>(
        &'a self,
    ) -> Result<FireflyGroupExtensionWrapper<'a>, quick_protobuf::Error> {
        Ok(FireflyGroupExtensionWrapper {
            inner: deserialize_proto(&self.inner)?,
        })
    }

    pub fn new(w: FireflyGroupExtensionWrapper) -> Result<Self, quick_protobuf::Error> {
        Ok(Self {
            inner: w.serialize()?,
        })
    }

    pub fn equal(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

#[derive(Debug)]
pub struct FireflyGroupExtensionWrapper<'a> {
    inner: protos::firefly::FireflyGroupExtension<'a>,
}

impl<'a> FireflyGroupExtensionWrapper<'a> {
    pub fn inner(&self) -> &protos::firefly::FireflyGroupExtension<'a> {
        &self.inner
    }

    pub fn new(ext: protos::firefly::FireflyGroupExtension<'a>) -> Self {
        Self { inner: ext }
    }

    pub fn update_group(&mut self, name: String, permissions: u32) {
        self.inner.default_permissions = permissions;
        self.inner.name = name.into();
    }

    pub fn is_valid(&self) -> bool {
        self.check_all_are_sorted() && self.check_roles_are_mapped_well()
    }

    pub fn users(&self) -> impl Iterator<Item = &str> {
        self.inner.members.iter().map(|x| x.username.as_ref())
    }

    pub fn check_all_are_sorted(&self) -> bool {
        self.inner.roles.is_sorted_by_key(|x| x.id)
            && self.inner.members.is_sorted_by_key(|x| &x.username)
            && self.inner.channels.is_sorted_by_key(|x| x.id)
            && self
                .inner
                .channels
                .iter()
                .all(|x| x.roles.is_sorted_by_key(|x| x.id))
    }

    pub fn check_roles_are_mapped_well(&self) -> bool {
        for channel in self.inner.channels.iter() {
            if !channel.roles.iter().all(|x| {
                x.name.is_empty() && self.inner.roles.search_by_key(&x.id, |z| z.id).is_ok()
            }) {
                return false;
            }
        }
        true
    }

    pub fn has_member(&self, username: &str) -> bool {
        self.inner
            .members
            .search_by_key(&username, |x| &&x.username)
            .is_ok()
    }

    #[inline(always)]
    pub const fn default_permissions(&self) -> u32 {
        self.inner.default_permissions
    }

    pub fn get_permissions_from_role_id(&self, role: u32) -> Option<u32> {
        if role == 0 {
            return Some(self.default_permissions());
        }

        let roles = &self.inner.roles;
        let idx = roles.search_by_key(&role, |x| x.id).ok()?;
        Some(roles[idx].permissions)
    }

    pub fn get_permissions_from_role_id_in_channel(
        &self,
        role_id: u32,
        channel_id: u32,
    ) -> Option<u32> {
        let channel = self.get_channel(channel_id)?;

        if role_id == 0 {
            return Some(channel.default_permissions);
        }
        let idx = channel.roles.search_by_key(&role_id, |x| x.id).ok()?;

        Some(channel.roles[idx].permissions)
    }

    pub fn get_role_of_user(&self, username: &str) -> Option<u32> {
        let members = &self.inner.members;

        let member_idx = members.search_by_key(&username, |x| &x.username).ok()?;
        Some(members[member_idx].role)
    }

    pub fn get_permissions_of_user_in_channel(
        &self,
        username: &str,
        channel_id: u32,
    ) -> Option<u32> {
        let member_role = self.get_role_of_user(username)?;

        let channels = &self.inner.channels;
        let channel_idx = channels.search_by_key(&channel_id, |x| x.id).ok()?;

        let channel = &channels[channel_idx];

        let roles = &channel.roles;
        if let Ok(role_idx) = roles.search_by_key(&member_role, |x| x.id) {
            Some(roles[role_idx].permissions);
        }

        return Some(channel.default_permissions);
    }

    pub fn serialize(&self) -> Result<Vec<u8>, quick_protobuf::Error> {
        Ok(serialize_proto(&self.inner)?.to_vec())
    }

    pub fn deserialize(buf: &'a [u8]) -> Result<Self, quick_protobuf::Error> {
        Ok(Self {
            inner: deserialize_proto(buf)?,
        })
    }

    pub fn update_member<'b: 'a>(
        &mut self,
        member: protos::firefly::FireflyGroupMember<'b>,
    ) -> Option<()> {
        // gets default permissions
        if member.role == 0 {
            return Some(());
        }

        self.update_member_even_if_default_role(member)
    }

    pub fn update_member_even_if_default_role<'b: 'a>(
        &mut self,
        member: protos::firefly::FireflyGroupMember<'b>,
    ) -> Option<()> {
        if member.role != 0
            && self
                .inner
                .roles
                .search_by_key(&member.role, |x| x.id)
                .is_err()
        {
            return None;
        }

        let members = &mut self.inner.members;
        {
            match members.search_by_key(&member.username.as_ref(), |x| &x.username) {
                Ok(idx) => {
                    members[idx] = member;
                }
                Err(idx) => {
                    members.insert(idx, member);
                }
            }
        }

        Some(())
    }

    pub fn update_role<'b: 'a>(&mut self, role: protos::firefly::FireflyGroupRole<'b>) {
        if role.id == 0 {
            self.update_default_permissions(role.permissions);
            return;
        }

        let roles = &mut self.inner.roles;
        match roles.search_by_key(&role.id, |x| x.id) {
            Ok(idx) => {
                roles[idx] = role;
            }
            Err(idx) => {
                roles.insert(idx, role);
            }
        }
    }

    pub fn update_channel<'b: 'a>(&mut self, channel: protos::firefly::FireflyGroupChannel<'b>) {
        let channels = &mut self.inner.channels;
        match channels.search_by_key(&channel.id, |x| x.id) {
            Ok(idx) => {
                let mut new_channel = channel;
                std::mem::swap(&mut channels[idx], &mut new_channel);
                let old_channel = new_channel;

                channels[idx].roles = old_channel.roles; // keep the roles unupdated, roles can be updated only via update_channel_role_permissions
            }
            Err(idx) => {
                channels.insert(idx, channel);
            }
        }
    }

    pub fn update_channel_role_permissions(
        &mut self,
        channel_id: u32,
        role_id: u32,
        permissions: u32,
    ) -> Option<()> {
        let channels = &mut self.inner.channels;
        let idx = channels.search_by_key(&channel_id, |x| x.id).ok()?;
        let channel = &mut channels[idx];

        if role_id == 0 {
            self.update_default_permissions(permissions);
            return Some(());
        }

        match channel.roles.search_by_key(&role_id, |x| x.id) {
            Ok(idx) => {
                channel.roles[idx].permissions = permissions;
            }
            Err(idx) => {
                channel.roles.insert(
                    idx,
                    FireflyGroupRole {
                        id: role_id,
                        name: Default::default(),
                        permissions,
                        color: Default::default(),
                    },
                );
            }
        }
        Some(())
    }

    #[inline(always)]
    pub const fn update_default_permissions(&mut self, permissions: u32) {
        self.inner.default_permissions = permissions;
    }

    pub fn update_channel_default_permissions(
        &mut self,
        channel_id: u32,
        permissions: u32,
    ) -> Option<()> {
        let channels = &mut self.inner.channels;
        let idx = channels.search_by_key(&channel_id, |x| x.id).ok()?;
        let channel = &mut channels[idx];
        channel.default_permissions = permissions;
        Some(())
    }

    pub fn delete_channel(&mut self, channel_id: u32) -> Option<()> {
        let channels = &mut self.inner.channels;
        let idx = channels.search_by_key(&channel_id, |x| x.id).ok()?;
        channels.remove(idx);

        Some(())
    }

    pub fn delete_member(&mut self, username: &str) -> Option<()> {
        let members = &mut self.inner.members;
        let idx = members.search_by_key(&username, |x| &x.username).ok()?;
        members.remove(idx);

        Some(())
    }

    pub fn delete_role(&mut self, role_id: u32) -> Option<()> {
        let roles = &mut self.inner.roles;
        let idx = roles.search_by_key(&role_id, |x| x.id).ok()?;
        roles.remove(idx);

        let channels = &mut self.inner.channels;

        for channel in channels.iter_mut() {
            let roles = &mut channel.roles;

            let Ok(idx) = roles.search_by_key(&role_id, |x| x.id) else {
                continue;
            };

            roles.remove(idx);
        }

        let members = &mut self.inner.members;

        for member in members.iter_mut() {
            if member.role == role_id {
                member.role = 0;
            }
        }

        Some(())
    }

    pub fn get_channel(&self, channel_id: u32) -> Option<&FireflyGroupChannel<'a>> {
        let idx = self
            .inner
            .channels
            .search_by_key(&channel_id, |x| x.id)
            .ok()?;

        Some(&self.inner.channels[idx])
    }

    pub fn delete_channel_role(&mut self, channel_id: u32, role_id: u32) -> Option<()> {
        let idx = self
            .inner
            .channels
            .search_by_key(&channel_id, |x| x.id)
            .ok()?;

        let channel = &mut self.inner.channels[idx];

        let idx = channel.roles.search_by_key(&role_id, |x| x.id).ok()?;

        channel.roles.remove(idx);

        Some(())
    }
}

impl MlsCodecExtension for FireflyGroupExtension {
    fn extension_type() -> ExtensionType {
        return ExtensionType::new(65001);
    }
}
