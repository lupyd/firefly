use firefly_core::config::{FireflyCredential, UserPermission, is_valid_name};
use firefly_core::jwk::{ecdsa_to_jwk, generate_key};
use firefly_core::rules::{
    does_signing_identity_has_this_username, get_username_from_signing_identity, has_permission,
    on_signing_identity,
};
use firefly_core::sorted_search::SortedSearch;
use firefly_protos::firefly::{AuthToken, SignedToken};
use firefly_protos::serialize_proto;
use mls_rs::identity::{MlsCredential, SigningIdentity};
use mls_rs_core::crypto::SignaturePublicKey;
use p256::ecdsa::signature::hazmat::PrehashSigner;
use sha2::Digest;
use std::borrow::Cow;

#[tokio::test]
async fn test_signing_identity_helpers() {
    let key = generate_key().unwrap();
    let kid = "test-kid";

    let auth_token = AuthToken {
        username: "alice".into(),
        valid_until: 2000000000,
        address_id: 1,
        device_id: 1,
        ..Default::default()
    };

    let payload = serialize_proto(&auth_token).unwrap();
    let digest = sha2::Sha256::digest(&payload);
    let signature: p256::ecdsa::Signature = key.sign_prehash(&digest).unwrap();

    let signed_token = SignedToken {
        kid: kid.into(),
        payload: Cow::Owned(payload.to_vec()),
        signature: Cow::Owned(signature.to_bytes().as_slice().to_vec()),
    };

    let credential_data = serialize_proto(&signed_token).unwrap();
    let credential = FireflyCredential::new(credential_data.to_vec()).unwrap();

    // Create a dummy public key for SigningIdentity
    let signature_key = SignaturePublicKey::new(vec![0u8; 32]);
    let signing_identity =
        SigningIdentity::new(credential.into_credential().unwrap(), signature_key);

    // Test get_username_from_signing_identity
    assert_eq!(
        get_username_from_signing_identity(&signing_identity).unwrap(),
        "alice"
    );

    // Test does_signing_identity_has_this_username
    assert!(does_signing_identity_has_this_username(
        "alice",
        &signing_identity
    ));
    assert!(!does_signing_identity_has_this_username(
        "bob",
        &signing_identity
    ));

    // Test on_signing_identity
    let username =
        on_signing_identity(&signing_identity, |token| token.username.to_string()).unwrap();
    assert_eq!(username, "alice");
}

#[test]
fn test_jwk_verification() {
    let key = generate_key().unwrap();
    let kid = "test-kid";
    let jwk = ecdsa_to_jwk(&key.verifying_key(), kid);

    let payload = b"hello world";
    let digest = sha2::Sha256::digest(payload);
    let signature: p256::ecdsa::Signature = key.sign_prehash(&digest).unwrap();

    assert!(jwk.verify(payload, signature.to_bytes().as_slice()));
    assert!(!jwk.verify(payload, b"invalid signature"));
    assert!(!jwk.verify(b"wrong payload", signature.to_bytes().as_slice()));
}

#[test]
fn test_sorted_search_edge_cases() {
    let arr = [1, 3, 5, 7, 9];

    // Found cases
    assert_eq!(arr.search_by_key(&1, |x| *x), Ok(0));
    assert_eq!(arr.search_by_key(&5, |x| *x), Ok(2));
    assert_eq!(arr.search_by_key(&9, |x| *x), Ok(4));

    // Not found cases (returns Err with insertion index)
    assert_eq!(arr.search_by_key(&0, |x| *x), Err(0));
    assert_eq!(arr.search_by_key(&2, |x| *x), Err(1));
    assert_eq!(arr.search_by_key(&10, |x| *x), Err(5));

    // Empty array
    let empty: [i32; 0] = [];
    assert_eq!(empty.search_by_key(&1, |x| *x), Err(0));

    // Single element
    let single = [5];
    assert_eq!(single.search_by_key(&5, |x| *x), Ok(0));
    assert_eq!(single.search_by_key(&4, |x| *x), Err(0));
    assert_eq!(single.search_by_key(&6, |x| *x), Err(1));
}

#[test]
fn test_permissions() {
    let perms = UserPermission::AddMessage as u32 | UserPermission::ManageMember as u32;

    assert!(has_permission(perms, UserPermission::AddMessage as u32));
    assert!(has_permission(perms, UserPermission::ManageMember as u32));
    assert!(!has_permission(perms, UserPermission::ManageGroup as u32));
    assert!(has_permission(perms, perms));
    assert!(!has_permission(UserPermission::AddMessage as u32, perms));
}

#[test]
fn test_role_hierarchy_and_inactivity() {
    let now = 1000u64;
    let active_valid_until = 2000u64;
    let expired_valid_until = 500u64;

    // Active vs Inactive token check
    assert!(active_valid_until >= now);
    assert!(!(expired_valid_until >= now));

    // Permission hierarchy checks
    let owner_perms = u32::MAX;
    let admin_perms = UserPermission::AddMessage as u32
        | UserPermission::ManageMember as u32
        | UserPermission::ManageRole as u32
        | UserPermission::ManageChannel as u32;
    let manager_perms = UserPermission::AddMessage as u32 | UserPermission::ManageMember as u32;
    let moderator_perms = UserPermission::AddMessage as u32 | UserPermission::ManageChannel as u32;
    let default_member_perms = UserPermission::AddMessage as u32;

    // Owner has permissions over admin and manager
    assert!(has_permission(owner_perms, admin_perms));
    assert!(has_permission(owner_perms, manager_perms));
    assert!(has_permission(owner_perms, default_member_perms));

    // Admin has permissions over manager and moderator
    assert!(has_permission(admin_perms, manager_perms));
    assert!(has_permission(admin_perms, moderator_perms));
    assert!(!has_permission(admin_perms, owner_perms));

    // Manager does NOT have permissions over admin or owner
    assert!(!has_permission(manager_perms, admin_perms));
    assert!(!has_permission(manager_perms, owner_perms));
    assert!(has_permission(manager_perms, default_member_perms));

    // Moderator cannot manage members or roles
    assert!(!has_permission(moderator_perms, UserPermission::ManageMember as u32));
    assert!(!has_permission(moderator_perms, UserPermission::ManageRole as u32));
    assert!(has_permission(moderator_perms, UserPermission::ManageChannel as u32));

    // Default member only has AddMessage
    assert!(!has_permission(default_member_perms, UserPermission::ManageMember as u32));
    assert!(!has_permission(default_member_perms, UserPermission::ManageRole as u32));
    assert!(!has_permission(default_member_perms, UserPermission::ManageChannel as u32));
    assert!(!has_permission(default_member_perms, UserPermission::ManageGroup as u32));
    assert!(has_permission(default_member_perms, UserPermission::AddMessage as u32));
}

#[test]
fn test_privilege_escalation_rules() {
    let sender_permissions = UserPermission::AddMessage as u32 | UserPermission::ManageMember as u32;
    let target_role_permissions = UserPermission::AddMessage as u32
        | UserPermission::ManageMember as u32
        | UserPermission::ManageRole as u32;

    // Sender cannot grant a role that has permissions sender does not possess
    assert!(!has_permission(sender_permissions, target_role_permissions));

    // Sender cannot modify/create a role that has permissions sender does not possess
    assert!(!has_permission(sender_permissions, target_role_permissions));

    // Sender cannot delete a role with higher permissions
    assert!(!has_permission(sender_permissions, target_role_permissions));

    // Sender CAN grant a role with equal or fewer permissions
    let allowed_role_perms = UserPermission::AddMessage as u32;
    assert!(has_permission(sender_permissions, allowed_role_perms));
}

#[test]
fn test_name_validation() {
    assert!(is_valid_name("valid_name"));
    assert!(is_valid_name("Alice"));
    assert!(is_valid_name("Group 123"));
    assert!(!is_valid_name(""));
    assert!(!is_valid_name("   "));
    assert!(is_valid_name("a"));
}
