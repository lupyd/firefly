//! Private channels (groups v2). A private channel is a channel of a group (the parent) whose
//! messages are in an MLS group of its own (the child), so only the child's members can read
//! them. The parent's extension keeps the channel, with `PRIVATE_CHANNEL_FLAG` in its type; the
//! server ties the child to it (`groups.parent_group_id`, `parent_channel_id`).
//!
//! Only the parent's owner creates private channels and adds or removes their members (the
//! server enforces it). Members come in two ways, recorded as their role in the child:
//! - the owner adds them (`PRIVATE_CHANNEL_ADDED_ROLE_ID`), and they stay until the owner
//!   removes them or they leave the parent;
//! - SeeMessage granted at the channel level in the parent (see
//!   `FireflyGroupExtensionWrapper::grants_private_channel`) brings them in
//!   (`PRIVATE_CHANNEL_BY_ROLE_ROLE_ID`), and taking it away removes them again.
//!
//! The owner's devices keep the child in line with the parent ([`plan_membership`]): after the
//! owner changes the parent, and whenever they apply a commit of someone else's to it (a member
//! who joined, left, or changed role while the owner was away).

use std::collections::{HashMap, HashSet};

use firefly_core::{
    config::{PRIVATE_CHANNEL_BY_ROLE_ROLE_ID, is_private_channel},
    extension::{FireflyGroupExtensionWrapper, OWNER_ROLE_ID},
};

use crate::db::keyvalue::KeyValueStore;

const LINKS_KEY: &str = "private_channel_links";

/// Where the group behind a private channel belongs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PrivateChannelLink {
    pub parent_group_id: u64,
    pub channel_id: u32,
}

/// Every child group this device knows of, by child group id.
pub async fn links(key_value_store: &KeyValueStore) -> HashMap<u64, PrivateChannelLink> {
    match key_value_store.get(LINKS_KEY).await {
        Ok(json) => serde_json::from_str(&json).unwrap_or_default(),
        Err(_) => HashMap::new(),
    }
}

pub async fn link_of(key_value_store: &KeyValueStore, child_group_id: u64) -> Option<PrivateChannelLink> {
    links(key_value_store).await.get(&child_group_id).copied()
}

/// The private channels of `parent_group_id`: (child group id, channel id).
pub async fn children_of(key_value_store: &KeyValueStore, parent_group_id: u64) -> Vec<(u64, u32)> {
    let mut children: Vec<(u64, u32)> = links(key_value_store)
        .await
        .into_iter()
        .filter(|(_, link)| link.parent_group_id == parent_group_id)
        .map(|(child, link)| (child, link.channel_id))
        .collect();
    children.sort();
    children
}

/// Records links (from the server's group list, an invite, or a channel this device created).
pub async fn record_links(
    key_value_store: &KeyValueStore,
    new_links: impl IntoIterator<Item = (u64, PrivateChannelLink)>,
) -> anyhow::Result<()> {
    let mut all = links(key_value_store).await;
    let mut changed = false;
    for (child, link) in new_links {
        if all.insert(child, link) != Some(link) {
            changed = true;
        }
    }
    if changed {
        key_value_store.set(LINKS_KEY, &serde_json::to_string(&all)?).await?;
    }
    Ok(())
}

pub async fn forget_link(key_value_store: &KeyValueStore, child_group_id: u64) -> anyhow::Result<()> {
    let mut all = links(key_value_store).await;
    if all.remove(&child_group_id).is_some() {
        key_value_store.set(LINKS_KEY, &serde_json::to_string(&all)?).await?;
    }
    Ok(())
}

/// What the owner's device commits to a private channel's group to match its parent.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct MembershipPlan {
    /// Users SeeMessage in the channel brings in, who aren't in it yet.
    pub add: Vec<String>,
    /// Users who left the parent, and users SeeMessage brought in who no longer have it.
    pub remove: Vec<String>,
}

/// `parent_users`/`child_users`: the users with a device in each group. Users the owner added
/// themselves stay while they are in the parent; the owner always stays.
pub fn plan_membership(
    parent: &FireflyGroupExtensionWrapper,
    parent_users: &HashSet<String>,
    channel_id: u32,
    child: &FireflyGroupExtensionWrapper,
    child_users: &HashSet<String>,
    owner: &str,
) -> MembershipPlan {
    let Some(channel) = parent.get_channel(channel_id) else {
        // The channel was deleted: the channel's group goes with it, not member by member.
        return MembershipPlan::default();
    };
    if !is_private_channel(channel.type_pb) {
        return MembershipPlan::default();
    }
    let granted: HashSet<&String> = parent_users
        .iter()
        .filter(|u| u.as_str() != owner && parent.grants_private_channel(u, channel_id))
        .collect();

    let mut add: Vec<String> = granted
        .iter()
        .filter(|u| !child_users.contains(u.as_str()))
        .map(|u| (*u).clone())
        .collect();
    let mut remove: Vec<String> = child_users
        .iter()
        .filter(|u| u.as_str() != owner)
        .filter(|u| {
            let role = child.get_role_of_user(u).unwrap_or(0);
            if role == OWNER_ROLE_ID {
                return false;
            }
            !parent_users.contains(u.as_str())
                || (role == PRIVATE_CHANNEL_BY_ROLE_ROLE_ID && !granted.contains(u))
        })
        .cloned()
        .collect();
    add.sort();
    remove.sort();
    MembershipPlan { add, remove }
}

#[cfg(test)]
mod tests {
    use super::*;
    use firefly_core::config::{PRIVATE_CHANNEL_ADDED_ROLE_ID, PRIVATE_CHANNEL_FLAG, UserPermission};
    use firefly_protos::firefly::{FireflyGroupChannel, FireflyGroupExtension, FireflyGroupMember, FireflyGroupRole};

    const SEE: u32 = UserPermission::SeeMessage as u32;
    const NULL: u32 = UserPermission::NullPermission as u32;

    fn role(id: u32, permissions: u32) -> FireflyGroupRole<'static> {
        FireflyGroupRole { id, name: if permissions == u32::MAX { "owner".into() } else { "".into() }, permissions, color: 0 }
    }

    fn member(username: &str, role: u32) -> FireflyGroupMember<'static> {
        FireflyGroupMember { username: username.to_string().into(), role }
    }

    /// Roles: 1 owner, 2 moderators (SeeMessage everywhere), 3 helpers. Channel 5 is private;
    /// `overrides` are its role overrides, `default` its default permissions.
    fn parent(overrides: Vec<FireflyGroupRole<'static>>, default: u32) -> FireflyGroupExtensionWrapper<'static> {
        let mut members = vec![member("alice", 1), member("bob", 2), member("carol", 3), member("dave", 0)];
        members.sort_by(|a, b| a.username.cmp(&b.username));
        FireflyGroupExtensionWrapper::new(FireflyGroupExtension {
            name: "g".into(),
            roles: vec![role(1, u32::MAX), role(2, SEE | 4), role(3, SEE | 4)],
            channels: vec![FireflyGroupChannel {
                id: 5,
                name: "secret".into(),
                type_pb: 1 | PRIVATE_CHANNEL_FLAG,
                roles: overrides,
                default_permissions: default,
            }],
            members,
            default_permissions: SEE | 4,
        })
    }

    fn child(members: &[(&str, u32)]) -> FireflyGroupExtensionWrapper<'static> {
        let mut members: Vec<_> = members.iter().map(|(u, r)| member(u, *r)).collect();
        members.sort_by(|a, b| a.username.cmp(&b.username));
        FireflyGroupExtensionWrapper::new(FireflyGroupExtension { members, ..Default::default() })
    }

    fn users(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn group_wide_see_message_brings_no_one_in() {
        let plan = plan_membership(
            &parent(vec![], NULL),
            &users(&["alice", "bob", "carol", "dave"]),
            5,
            &child(&[("alice", 1)]),
            &users(&["alice"]),
            "alice",
        );
        assert_eq!(plan, MembershipPlan::default());
    }

    #[test]
    fn granting_a_role_adds_its_members_and_revoking_removes_only_them() {
        let all = users(&["alice", "bob", "carol", "dave"]);
        let granted = parent(vec![role(3, SEE)], NULL);
        let plan = plan_membership(&granted, &all, 5, &child(&[("alice", 1)]), &users(&["alice"]), "alice");
        assert_eq!(plan.add, vec!["carol".to_string()]);
        assert!(plan.remove.is_empty());

        // carol came in by role, dave was added by the owner: revoking removes carol only
        let in_channel = child(&[("alice", 1), ("carol", PRIVATE_CHANNEL_BY_ROLE_ROLE_ID), ("dave", PRIVATE_CHANNEL_ADDED_ROLE_ID)]);
        let revoked = parent(vec![role(3, NULL)], NULL);
        let plan = plan_membership(&revoked, &all, 5, &in_channel, &users(&["alice", "carol", "dave"]), "alice");
        assert!(plan.add.is_empty());
        assert_eq!(plan.remove, vec!["carol".to_string()]);
    }

    #[test]
    fn channel_default_covers_members_without_a_role() {
        let plan = plan_membership(
            &parent(vec![], SEE),
            &users(&["alice", "bob", "carol", "dave"]),
            5,
            &child(&[("alice", 1)]),
            &users(&["alice"]),
            "alice",
        );
        assert_eq!(plan.add, vec!["dave".to_string()]);
    }

    #[test]
    fn users_who_left_the_parent_are_removed_however_they_came_in() {
        let in_channel = child(&[("alice", 1), ("bob", PRIVATE_CHANNEL_ADDED_ROLE_ID), ("carol", PRIVATE_CHANNEL_BY_ROLE_ROLE_ID)]);
        let plan = plan_membership(
            &parent(vec![role(3, SEE)], NULL),
            &users(&["alice", "dave"]),
            5,
            &in_channel,
            &users(&["alice", "bob", "carol"]),
            "alice",
        );
        assert_eq!(plan.remove, vec!["bob".to_string(), "carol".to_string()]);
    }

    #[test]
    fn deleted_or_public_channels_plan_nothing() {
        let all = users(&["alice", "bob"]);
        let in_channel = child(&[("alice", 1), ("bob", PRIVATE_CHANNEL_BY_ROLE_ROLE_ID)]);
        let plan = plan_membership(&parent(vec![], NULL), &all, 9, &in_channel, &users(&["alice", "bob"]), "alice");
        assert_eq!(plan, MembershipPlan::default());

        let mut public = parent(vec![], NULL);
        let channel = public.get_channel(5).cloned().map(|mut c| {
            c.type_pb = 1;
            c
        });
        if let Some(c) = channel {
            public.update_channel(c);
        }
        let plan = plan_membership(&public, &all, 5, &in_channel, &users(&["alice", "bob"]), "alice");
        assert_eq!(plan, MembershipPlan::default());
    }
}
