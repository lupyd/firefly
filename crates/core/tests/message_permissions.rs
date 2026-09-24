use firefly_core::{
    config::{DEFAULT_GROUP_PERMISSIONS, UserPermission},
    extension::FireflyGroupExtensionWrapper,
    rules::{FireflyMlsRules, has_permission},
};
use firefly_protos::{firefly::*, serialize_proto};

const SEE: u32 = UserPermission::SeeMessage as u32;
const PIN: u32 = UserPermission::PinMessage as u32;
const ADD: u32 = UserPermission::AddMessage as u32;

fn extension(default_permissions: u32) -> FireflyGroupExtensionWrapper<'static> {
    FireflyGroupExtensionWrapper::new(FireflyGroupExtension {
        default_permissions,
        ..Default::default()
    })
}

fn payload(channel: u32, outer: u32, nested: u32) -> Vec<u8> {
    serialize_proto(&GroupMessageInner {
        channelId: channel,
        message_type: outer,
        message: mod_GroupMessageInner::OneOfmessage::messagePayload(MessagePayload {
            text: "hello".into(),
            message_type: nested,
            ..Default::default()
        }),
    })
    .unwrap()
    .to_vec()
}

#[test]
fn permission_values_and_new_group_default_are_wire_compatible() {
    assert_eq!((SEE, PIN, ADD), (1, 2, 4));
    assert_eq!(UserPermission::ManageChannel as u32, 8);
    assert_eq!(UserPermission::ManageRole as u32, 16);
    assert_eq!(UserPermission::ManageMember as u32, 32);
    assert_eq!(UserPermission::ManageGroup as u32, 64);
    assert_eq!(DEFAULT_GROUP_PERMISSIONS, SEE | ADD);
    assert!(!has_permission(DEFAULT_GROUP_PERMISSIONS, PIN));
    // Existing masks are not silently upgraded, including old AddMessage-only groups.
    let old = extension(ADD).serialize().unwrap();
    let decoded = FireflyGroupExtensionWrapper::deserialize(&old).unwrap();
    assert_eq!(decoded.default_permissions(), ADD);
    assert!(FireflyMlsRules::check_message_sender(&decoded, "member", &payload(0, 0, 0)).is_err());
}

#[test]
fn exhaustive_send_read_and_pin_bit_combinations() {
    for mask in 0..128 {
        let ext = extension(mask);
        let read = FireflyMlsRules::require_message_permission(
            &ext,
            "member",
            0,
            UserPermission::SeeMessage,
        );
        assert_eq!(read.is_ok(), mask & SEE != 0, "read mask={mask}");
        for (outer, nested) in [(0, 0), (1, 0), (0, 1), (1, 1), (0x80, 0), (0x80, 0x81)] {
            let pinned = (outer | nested) & 1 != 0;
            let required = SEE | ADD | if pinned { PIN } else { 0 };
            assert_eq!(
                FireflyMlsRules::check_message_sender(&ext, "member", &payload(0, outer, nested))
                    .is_ok(),
                mask & required == required,
                "mask={mask}, outer={outer}, nested={nested}",
            );
        }
    }
}

#[test]
fn channel_overrides_replace_defaults_and_missing_channels_fail_closed() {
    let mut ext = extension(SEE | ADD | PIN);
    ext.update_role(FireflyGroupRole {
        id: 1,
        name: "member".into(),
        permissions: SEE | ADD,
        ..Default::default()
    });
    ext.update_member(FireflyGroupMember {
        username: "alice".into(),
        role: 1,
    });
    ext.update_channel(FireflyGroupChannel {
        id: 7,
        default_permissions: SEE,
        ..Default::default()
    });
    // Role permissions are preserved unless explicitly overwritten in channel.
    assert_eq!(FireflyMlsRules::message_permissions(&ext, "alice", 7), SEE | ADD);
    assert_eq!(
        FireflyMlsRules::message_permissions(&ext, "default_member", 7),
        SEE
    );
    assert!(FireflyMlsRules::check_message_sender(&ext, "alice", &payload(7, 0, 0)).is_ok());
    ext.update_channel_role_permissions(7, 1, SEE | ADD | PIN)
        .unwrap();
    assert!(FireflyMlsRules::check_message_sender(&ext, "alice", &payload(7, 0, 1)).is_ok());
    ext.update_channel_role_permissions(7, 1, 0).unwrap();
    assert!(
        FireflyMlsRules::require_message_permission(&ext, "alice", 7, UserPermission::SeeMessage)
            .is_err()
    );
    assert_eq!(FireflyMlsRules::message_permissions(&ext, "alice", 999), 0);
    assert!(FireflyMlsRules::check_message_sender(&ext, "alice", &payload(999, 0, 0)).is_err());
    // Channel with default_permissions == 0 falls back to group-level default_permissions
    ext.update_channel(FireflyGroupChannel {
        id: 8,
        default_permissions: 0,
        ..Default::default()
    });
    assert_eq!(
        FireflyMlsRules::message_permissions(&ext, "default_member", 8),
        SEE | ADD | PIN
    );
    // Channel with NullPermission explicitly overwrites to grant 0 permissions
    ext.update_channel(FireflyGroupChannel {
        id: 9,
        default_permissions: UserPermission::NullPermission as u32,
        ..Default::default()
    });
    assert_eq!(
        FireflyMlsRules::message_permissions(&ext, "default_member", 9),
        0
    );
    // Channel with NullPermission | SEE grants SEE
    ext.update_channel(FireflyGroupChannel {
        id: 10,
        default_permissions: (UserPermission::NullPermission as u32) | SEE,
        ..Default::default()
    });
    assert_eq!(
        FireflyMlsRules::message_permissions(&ext, "default_member", 10),
        SEE
    );
    // Explicit channel zero overrides the group-wide fallback too if role is overwritten
    ext.update_channel(FireflyGroupChannel {
        id: 0,
        ..Default::default()
    });
    ext.update_channel_role_permissions(0, 1, 0).unwrap();
    assert_eq!(FireflyMlsRules::message_permissions(&ext, "alice", 0), 0);
}

#[test]
fn raw_legacy_payloads_use_group_wide_permissions_without_bypassing_them() {
    for data in [b"Hello".as_slice(), b"", &[0xff, 0xff]] {
        assert!(
            FireflyMlsRules::check_message_sender(&extension(SEE | ADD), "member", data).is_ok()
        );
        assert!(FireflyMlsRules::check_message_sender(&extension(ADD), "member", data).is_err());
        assert!(FireflyMlsRules::check_message_sender(&extension(SEE), "member", data).is_err());
    }
}

#[test]
fn undefined_roles_fail_closed_and_default_roster_members_need_no_extension_entry() {
    let ext = FireflyGroupExtensionWrapper::new(FireflyGroupExtension {
        default_permissions: SEE | ADD | PIN,
        members: vec![FireflyGroupMember {
            username: "broken".into(),
            role: 999,
        }],
        ..Default::default()
    });
    assert_eq!(FireflyMlsRules::message_permissions(&ext, "broken", 0), 0);
    assert_eq!(
        FireflyMlsRules::message_permissions(&ext, "authenticated_default_member", 0),
        SEE | ADD | PIN
    );
}

#[test]
fn shared_pin_control_requires_see_and_pin_for_both_actions() {
    for pinned in [false,true] {
        let message=serialize_proto(&GroupMessageInner { channelId:0,message_type:firefly_protos::MESSAGE_TYPE_HIDDEN,message:mod_GroupMessageInner::OneOfmessage::pinUpdate(GroupPinUpdate{message_id:10,pinned}) }).unwrap();
        for mask in 0..128 { assert_eq!(FireflyMlsRules::check_message_sender(&extension(mask),"member",&message).is_ok(),mask&(SEE|PIN)==SEE|PIN,"mask={mask},pinned={pinned}"); }
    }
}
#[test]
fn shared_pin_control_cannot_be_visible_or_target_zero() {
    for (id,flags) in [(0,2),(1,0),(1,3)] {
        let message=serialize_proto(&GroupMessageInner {channelId:0,message_type:flags,message:mod_GroupMessageInner::OneOfmessage::pinUpdate(GroupPinUpdate{message_id:id,pinned:true})}).unwrap();
        assert!(FireflyMlsRules::check_message_sender(&extension(127),"member",&message).is_err());
    }
}

#[test]
fn hidden_pin_snapshots_require_pin_rights_in_every_target_channel() {
    let original=payload(0,0,0);
    let record=GroupHistoryRecord{id:10,group_id:42,sender:"alice".into(),message:original.into(),epoch:1};
    let make=|flags, records:Vec<GroupHistoryRecord>|serialize_proto(&GroupMessageInner{channelId:0,message_type:flags,message:mod_GroupMessageInner::OneOfmessage::pinSnapshot(GroupPinSnapshot{messages:records})}).unwrap();
    let snapshot=make(2,vec![record.clone()]);
    assert!(FireflyMlsRules::check_message_sender(&extension(SEE|PIN),"adder",&snapshot).is_ok());
    assert!(FireflyMlsRules::check_message_sender(&extension(SEE|ADD),"adder",&snapshot).is_err());
    assert!(FireflyMlsRules::check_message_sender(&extension(127),"adder",&make(0,vec![record.clone()])).is_err());
    assert!(FireflyMlsRules::check_message_sender(&extension(127),"adder",&make(2,vec![record.clone();101])).is_err());
    let foreign_channel=GroupHistoryRecord{message:payload(99,0,0).into(),..record};
    assert!(FireflyMlsRules::check_message_sender(&extension(127),"adder",&make(2,vec![foreign_channel])).is_err());
}
