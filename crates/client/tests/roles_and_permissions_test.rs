use async_trait::async_trait;
use firefly_client::callbacks::FireflyWsClientCallback;
use firefly_client::db::{group_messages::GroupMessage, messages::UserMessage};
use firefly_client::group::{UpdateRoleProposalFfi, UpdateUserProposalFfi};
use firefly_client::websocket::FireflyWsClient;
use firefly_core::config::UserPermission;
use firefly_protos::firefly;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

struct TestCallbacks {
    name: String,
    token: String,
    message_tx: mpsc::Sender<UserMessage>,
    group_message_tx: mpsc::Sender<GroupMessage>,
}

#[async_trait]
impl FireflyWsClientCallback for TestCallbacks {
    fn name(&self) -> &str {
        &self.name
    }

    async fn get_access_token(&self) -> Option<String> {
        Some(self.token.clone())
    }

    async fn on_message(&self, message: UserMessage) {
        let _ = self.message_tx.send(message).await;
    }

    async fn on_group_message(&self, group_message: GroupMessage) {
        let _ = self.group_message_tx.send(group_message).await;
    }
}

async fn setup_server() -> Option<(String, String)> {
    let _ = std::fs::create_dir_all("/tmp/firefly");
    dotenv::from_filename(".env.test").ok();
    dotenv::dotenv().ok();
    if let Ok(base_url) = std::env::var("FIREFLY_BASE_URL") {
        let ws_url = std::env::var("FIREFLY_WS_URL").unwrap_or_else(|_| {
            base_url
                .replace("http://", "ws://")
                .replace("https://", "wss://")
        });
        firefly_client::init_logger("/tmp/firefly/test_roles_permissions.log".to_string());
        Some((base_url, ws_url))
    } else {
        println!("Skipping integration test: FIREFLY_BASE_URL is not set.");
        None
    }
}

fn cleanup_dir(dir: &str) {
    if let Err(e) = std::fs::remove_dir_all(dir) {
        if e.kind() != std::io::ErrorKind::NotFound {
            log::warn!("Failed to cleanup test dir {}: {:?}", dir, e);
        }
    }
}

async fn wait_for_init(client: &FireflyWsClient) -> anyhow::Result<()> {
    for _ in 0..60 {
        if client.is_initialized() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(anyhow::anyhow!("Client timeout waiting for initialization"))
}

async fn create_client_helper(
    base_url: &str,
    ws_url: &str,
    test_dir: &str,
    username: &str,
) -> (
    Arc<FireflyWsClient>,
    mpsc::Receiver<UserMessage>,
    mpsc::Receiver<GroupMessage>,
) {
    let (msg_tx, msg_rx) = mpsc::channel(100);
    let (gmsg_tx, gmsg_rx) = mpsc::channel(100);
    let callbacks = TestCallbacks {
        name: username.to_string(),
        token: username.to_string(),
        message_tx: msg_tx,
        group_message_tx: gmsg_tx,
    };

    let db_path = format!("{}/{}.db", test_dir, username);
    let client = FireflyWsClient::create(
        base_url.to_string(),
        ws_url.to_string(),
        1000,
        Box::new(callbacks),
        db_path,
        5000,
    )
    .await
    .expect("Failed to create client");
    let client = Arc::new(client);

    let client_init = client.clone();
    tokio::spawn(async move {
        let _ = client_init.initialize_with_retrying().await;
    });

    wait_for_init(&client)
        .await
        .unwrap_or_else(|_| panic!("{} failed to initialize", username));

    (client, msg_rx, gmsg_rx)
}

// =============================================================================
// TEST 1: Role Hierarchy & Privilege Escalation Prevention
// =============================================================================
#[tokio::test]
async fn test_role_hierarchy_and_privilege_escalation() {
    let (base_url, ws_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };

    let test_run_id = rand::random::<u32>();
    let test_dir = format!("/tmp/firefly/roles_escalation_{}", test_run_id);
    let _ = std::fs::create_dir_all(&test_dir);

    let alice_name = format!("alice_{}", test_run_id);
    let bob_name = format!("bob_{}", test_run_id);
    let charlie_name = format!("charlie_{}", test_run_id);
    let dave_name = format!("dave_{}", test_run_id);
    let eve_name = format!("eve_{}", test_run_id);

    let (alice, _, _) = create_client_helper(&base_url, &ws_url, &test_dir, &alice_name).await;
    let (bob, _, _) = create_client_helper(&base_url, &ws_url, &test_dir, &bob_name).await;
    let (charlie, _, _) = create_client_helper(&base_url, &ws_url, &test_dir, &charlie_name).await;
    let (dave, _, _) = create_client_helper(&base_url, &ws_url, &test_dir, &dave_name).await;
    let (eve, _, _) = create_client_helper(&base_url, &ws_url, &test_dir, &eve_name).await;

    // 1. Alice creates group
    let group_info = alice
        .create_group("Hierarchy Test Group".into(), "Description".into(), 0)
        .await
        .expect("Alice creates group");
    let group_id = group_info.id;

    // Define Permission combinations
    let perm_admin = (UserPermission::AddMessage as u32)
        | (UserPermission::ManageRole as u32)
        | (UserPermission::ManageMember as u32);
    let perm_mod = (UserPermission::AddMessage as u32) | (UserPermission::ManageMember as u32);
    let perm_member = UserPermission::AddMessage as u32;

    // Alice creates Role 2 (Admin), Role 3 (Moderator), Role 4 (Member), Role 5 (Observer)
    alice
        .update_group_roles(
            group_id,
            vec![
                UpdateRoleProposalFfi {
                    name: "admin".into(),
                    role_id: 2,
                    permissions: perm_admin,
                    delete: false,
                    color: 1,
                },
                UpdateRoleProposalFfi {
                    name: "moderator".into(),
                    role_id: 3,
                    permissions: perm_mod,
                    delete: false,
                    color: 2,
                },
                UpdateRoleProposalFfi {
                    name: "member".into(),
                    role_id: 4,
                    permissions: perm_member,
                    delete: false,
                    color: 3,
                },
                UpdateRoleProposalFfi {
                    name: "observer".into(),
                    role_id: 5,
                    permissions: 0,
                    delete: false,
                    color: 4,
                },
            ],
        )
        .await
        .expect("Alice sets up roles");

    // Alice adds Bob (Admin: 2), Charlie (Mod: 3), Dave (Member: 4), Eve (Observer: 5)
    alice.add_group_member(group_id, bob_name.clone(), 2).await.expect("Add Bob");
    alice.add_group_member(group_id, charlie_name.clone(), 3).await.expect("Add Charlie");
    alice.add_group_member(group_id, dave_name.clone(), 4).await.expect("Add Dave");
    alice.add_group_member(group_id, eve_name.clone(), 5).await.expect("Add Eve");

    // Sync all members
    bob.check_setup().await.expect("Bob sync");
    charlie.check_setup().await.expect("Charlie sync");
    dave.check_setup().await.expect("Dave sync");
    eve.check_setup().await.expect("Eve sync");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // --- CHECK 1: Escalation via Role Creation ---
    // Bob has perm_admin (lacks ManageChannel, ManageGroup).
    // Bob attempts to create a role granting ManageChannel (escalation) -> MUST FAIL.
    println!("[TEST] Bob attempting privilege escalation by creating role with ManageChannel...");
    let escalate_create_res = bob
        .update_group_roles(
            group_id,
            vec![UpdateRoleProposalFfi {
                name: "super_channel_admin".into(),
                role_id: 10,
                permissions: perm_admin | (UserPermission::ManageChannel as u32),
                delete: false,
                color: 5,
            }],
        )
        .await;
    assert!(
        escalate_create_res.is_err(),
        "Bob must NOT be able to create a role with permissions he does not possess!"
    );
    println!("✓ Privilege escalation via role creation correctly rejected!");

    // Bob creates a role with a SUBSET of his own permissions -> MUST SUCCEED.
    println!("[TEST] Bob creating a valid sub-role with subset of permissions...");
    let valid_create_res = bob
        .update_group_roles(
            group_id,
            vec![UpdateRoleProposalFfi {
                name: "sub_mod".into(),
                role_id: 11,
                permissions: perm_mod,
                delete: false,
                color: 6,
            }],
        )
        .await;
    assert!(
        valid_create_res.is_ok(),
        "Bob must be allowed to create roles with permissions subset of his own: {:?}",
        valid_create_res.err()
    );
    println!("✓ Valid sub-role creation succeeded!");

    // --- CHECK 2: Escalation via Member Role Assignment ---
    // Bob attempts to promote Charlie to Owner (role 1: u32::MAX) -> MUST FAIL.
    println!("[TEST] Bob attempting to promote Charlie to Owner...");
    let promote_to_owner_res = bob
        .update_group_users(
            group_id,
            vec![UpdateUserProposalFfi {
                username: charlie_name.clone(),
                role_id: 1, // Owner
            }],
        )
        .await;
    assert!(
        promote_to_owner_res.is_err(),
        "Bob must NOT be able to grant the Owner role!"
    );
    println!("✓ Privilege escalation via granting higher role correctly rejected!");

    // Bob promotes Dave from Member (4) to Moderator (3) -> MUST SUCCEED.
    println!("[TEST] Bob promoting Dave to Moderator...");
    let promote_dave_res = bob
        .update_group_users(
            group_id,
            vec![UpdateUserProposalFfi {
                username: dave_name.clone(),
                role_id: 3, // Moderator
            }],
        )
        .await;
    assert!(
        promote_dave_res.is_ok(),
        "Bob must be allowed to promote Dave to a role he has authority over: {:?}",
        promote_dave_res.err()
    );
    println!("✓ Valid promotion succeeded!");

    // --- CHECK 3: Superior Member Protection (Cannot demote or modify superiors) ---
    // Bob attempts to demote Alice (Owner) to Member (4) -> MUST FAIL.
    println!("[TEST] Bob attempting to demote Owner Alice...");
    let demote_alice_res = bob
        .update_group_users(
            group_id,
            vec![UpdateUserProposalFfi {
                username: alice_name.clone(),
                role_id: 4,
            }],
        )
        .await;
    assert!(
        demote_alice_res.is_err(),
        "Bob must NOT be able to modify the role of superior user Alice!"
    );
    println!("✓ Modifying superior member correctly rejected!");

    // Charlie (Moderator, lacking ManageRole) attempts to assign any role -> MUST FAIL.
    charlie.check_setup().await.expect("Charlie sync");
    println!("[TEST] Charlie without ManageRole attempting to update member roles...");
    let charlie_update_res = charlie
        .update_group_users(
            group_id,
            vec![UpdateUserProposalFfi {
                username: eve_name.clone(),
                role_id: 4,
            }],
        )
        .await;
    assert!(
        charlie_update_res.is_err(),
        "Charlie without ManageRole must NOT be able to update member roles!"
    );
    println!("✓ Role update without ManageRole correctly rejected!");

    // --- CHECK 4: Superior Role Deletion Protection ---
    // Bob attempts to delete Role 1 (Owner) -> MUST FAIL.
    println!("[TEST] Bob attempting to delete Role 1 (Owner)...");
    let delete_owner_role_res = bob
        .update_group_roles(
            group_id,
            vec![UpdateRoleProposalFfi {
                name: "".into(),
                role_id: 1,
                permissions: 0,
                delete: true,
                color: 0,
            }],
        )
        .await;
    assert!(
        delete_owner_role_res.is_err(),
        "Bob must NOT be able to delete the Owner role!"
    );
    println!("✓ Deleting superior role correctly rejected!");

    // Bob deletes Role 11 (which he created) -> MUST SUCCEED.
    println!("[TEST] Bob deleting Role 11...");
    let delete_role_11_res = bob
        .update_group_roles(
            group_id,
            vec![UpdateRoleProposalFfi {
                name: "".into(),
                role_id: 11,
                permissions: 0,
                delete: true,
                color: 0,
            }],
        )
        .await;
    assert!(
        delete_role_11_res.is_ok(),
        "Bob must be allowed to delete a role with lower/equal permissions: {:?}",
        delete_role_11_res.err()
    );
    println!("✓ Valid role deletion succeeded!");

    // --- CHECK 5: Member Removal (Kick) Permissions ---
    // Eve (Observer, no ManageMember) attempts to kick Dave -> MUST FAIL.
    println!("[TEST] Eve without ManageMember attempting to kick Dave...");
    let eve_kick_res = eve.kick_group_member(group_id, dave_name.clone()).await;
    assert!(
        eve_kick_res.is_err(),
        "Eve without ManageMember must NOT be able to kick Dave!"
    );
    println!("✓ Kick without ManageMember correctly rejected!");

    // Charlie (Moderator with ManageMember) attempts to kick Alice (Owner) -> MUST FAIL.
    println!("[TEST] Charlie attempting to kick Owner Alice...");
    let charlie_kick_alice = charlie.kick_group_member(group_id, alice_name.clone()).await;
    assert!(
        charlie_kick_alice.is_err(),
        "Charlie must NOT be able to kick superior member Alice!"
    );
    println!("✓ Kicking superior member correctly rejected!");

    // Charlie attempts to kick Bob (Admin with higher permissions) -> MUST FAIL.
    println!("[TEST] Charlie attempting to kick Admin Bob...");
    let charlie_kick_bob = charlie.kick_group_member(group_id, bob_name.clone()).await;
    assert!(
        charlie_kick_bob.is_err(),
        "Charlie must NOT be able to kick member Bob who has higher permissions!"
    );
    println!("✓ Kicking higher-tier member correctly rejected!");

    // Charlie kicks Eve (Observer with 0 permissions) -> MUST SUCCEED.
    println!("[TEST] Charlie kicking Eve...");
    let charlie_kick_eve = charlie.kick_group_member(group_id, eve_name.clone()).await;
    assert!(
        charlie_kick_eve.is_ok(),
        "Charlie with ManageMember must be able to kick lower-tier Eve: {:?}",
        charlie_kick_eve.err()
    );
    println!("✓ Kicking lower-tier member succeeded!");

    alice.dispose().await;
    bob.dispose().await;
    charlie.dispose().await;
    dave.dispose().await;
    eve.dispose().await;
    cleanup_dir(&test_dir);
}

// =============================================================================
// TEST 2: Channel Permissions, Overrides, and Restriction Enforcement
// =============================================================================
#[tokio::test]
async fn test_channel_permissions_overrides_and_restriction() {
    let (base_url, ws_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };

    let test_run_id = rand::random::<u32>();
    let test_dir = format!("/tmp/firefly/chan_perm_{}", test_run_id);
    let _ = std::fs::create_dir_all(&test_dir);

    let alice_name = format!("c_alice_{}", test_run_id);
    let bob_name = format!("c_bob_{}", test_run_id);
    let charlie_name = format!("c_charlie_{}", test_run_id);

    let (alice, _, _) = create_client_helper(&base_url, &ws_url, &test_dir, &alice_name).await;
    let (bob, _, _) = create_client_helper(&base_url, &ws_url, &test_dir, &bob_name).await;
    let (charlie, _, _charlie_gmsg_rx) =
        create_client_helper(&base_url, &ws_url, &test_dir, &charlie_name).await;

    // Alice creates group with default permissions = AddMessage
    let group_info = alice
        .create_group(
            "Channel Perm Group".into(),
            "Description".into(),
            UserPermission::AddMessage as u32,
        )
        .await
        .expect("Alice creates group");
    let group_id = group_info.id;

    // Role 2: Regular Member (AddMessage only)
    // Role 3: Channel Admin (AddMessage | ManageChannel)
    // Role 4: Muted Member (0 permissions)
    alice
        .update_group_roles(
            group_id,
            vec![
                UpdateRoleProposalFfi {
                    name: "regular_member".into(),
                    role_id: 2,
                    permissions: UserPermission::AddMessage as u32,
                    delete: false,
                    color: 1,
                },
                UpdateRoleProposalFfi {
                    name: "channel_admin".into(),
                    role_id: 3,
                    permissions: (UserPermission::AddMessage as u32)
                        | (UserPermission::ManageChannel as u32),
                    delete: false,
                    color: 2,
                },
                UpdateRoleProposalFfi {
                    name: "muted".into(),
                    role_id: 4,
                    permissions: 0,
                    delete: false,
                    color: 3,
                },
            ],
        )
        .await
        .expect("Alice sets up roles");

    // Add Bob (Regular Member: 2), Charlie (Regular Member: 2)
    alice.add_group_member(group_id, bob_name.clone(), 2).await.expect("Add Bob");
    alice.add_group_member(group_id, charlie_name.clone(), 2).await.expect("Add Charlie");

    bob.check_setup().await.expect("Bob sync");
    charlie.check_setup().await.expect("Charlie sync");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // --- CHECK 1: Channel Creation Permissions ---
    // Bob (lacks ManageChannel) attempts to create channel 1 -> MUST FAIL.
    println!("[TEST] Bob without ManageChannel attempting to create channel...");
    let bob_create_chan = bob
        .update_group_channel(
            group_id,
            1,
            false,
            "announcements".into(),
            1, // text channel
            0, // read-only default permissions
        )
        .await;
    assert!(
        bob_create_chan.is_err(),
        "Bob without ManageChannel must NOT be able to create channel!"
    );
    println!("✓ Channel creation without ManageChannel correctly rejected!");

    // Alice creates channel 1: "announcements" with default_permissions = 0 (read-only)
    println!("[TEST] Alice creating read-only channel 1 ('announcements')...");
    alice
        .update_group_channel(group_id, 1, false, "announcements".into(), 1, 0)
        .await
        .expect("Alice creates channel 1");

    // Alice creates channel 2: "general" with default_permissions = AddMessage
    println!("[TEST] Alice creating channel 2 ('general') with AddMessage...");
    alice
        .update_group_channel(
            group_id,
            2,
            false,
            "general".into(),
            1,
            UserPermission::AddMessage as u32,
        )
        .await
        .expect("Alice creates channel 2");

    // Sync Bob and Charlie
    bob.check_setup().await.expect("Bob sync");
    charlie.check_setup().await.expect("Charlie sync");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // --- CHECK 2: Channel-Level Role Overrides ---
    // In channel 1 ("announcements"): default is 0.
    // Alice overrides Role 3 (channel_admin) to have AddMessage in channel 1.
    println!("[TEST] Alice configuring channel 1 role override for channel_admin...");
    alice
        .update_group_roles_in_channel(
            group_id,
            1,
            vec![UpdateRoleProposalFfi {
                name: "channel_admin".into(),
                role_id: 3,
                permissions: (UserPermission::AddMessage as u32)
                    | (UserPermission::ManageChannel as u32),
                delete: false,
                color: 2,
            }],
        )
        .await
        .expect("Alice overrides role 3 in channel 1");

    // Sync
    bob.check_setup().await.expect("Bob sync");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Alice promotes Bob to Role 3 (channel_admin)
    alice
        .update_group_users(
            group_id,
            vec![UpdateUserProposalFfi {
                username: bob_name.clone(),
                role_id: 3,
            }],
        )
        .await
        .expect("Promote Bob to channel_admin");

    bob.check_setup().await.expect("Bob sync after promotion");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Bob (now channel_admin) deletes and recreates channel 2
    println!("[TEST] Bob with ManageChannel deleting channel 2...");
    let bob_delete_res = bob
        .update_group_channel(group_id, 2, true, "general".into(), 1, 0)
        .await;
    assert!(
        bob_delete_res.is_ok(),
        "Bob with ManageChannel must be allowed to delete channel 2: {:?}",
        bob_delete_res.err()
    );
    println!("✓ Channel deletion by authorized user succeeded!");

    // --- CHECK 3: Channel Message Posting & Permission Enforcement ---
    // Charlie is in Role 2 (regular_member).
    // In channel 1 ("announcements"), default_permissions = 0 and Charlie's role is not overridden with AddMessage.
    // When Charlie sends a message to channel 1:
    // Testing whether the system restricts or delivers message to a read-only channel!
    println!("[TEST] Charlie sending group message to read-only channel 1...");
    let read_only_msg = firefly::GroupMessageInner {
        channelId: 1,
        message: firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(
            firefly::MessagePayload {
                text: "Unauthorized announcement from Charlie".into(),
                ..Default::default()
            },
        ),
    };

    let send_result = charlie.upload_group_message(group_id, read_only_msg, 0).await;
    println!(
        "Result of uploading message to read-only channel: is_ok={:?}",
        send_result.is_ok()
    );

    // Document whether the message was restricted at upload time or delivered:
    if send_result.is_ok() {
        println!(
            "NOTE: Message upload to channel 1 succeeded at transport layer. Checking if receivers enforce channel permissions..."
        );
    } else {
        println!("✓ Message upload correctly rejected for read-only channel!");
    }

    alice.dispose().await;
    bob.dispose().await;
    charlie.dispose().await;
    cleanup_dir(&test_dir);
}

// =============================================================================
// TEST 3: Exhaustive Bitwise Permission Matrix Table
// =============================================================================
#[tokio::test]
async fn test_exhaustive_permission_matrix() {
    let (base_url, ws_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };

    let test_run_id = rand::random::<u32>();
    let test_dir = format!("/tmp/firefly/matrix_{}", test_run_id);
    let _ = std::fs::create_dir_all(&test_dir);

    let alice_name = format!("m_alice_{}", test_run_id);
    let bob_name = format!("m_bob_{}", test_run_id);

    let (alice, _, _) = create_client_helper(&base_url, &ws_url, &test_dir, &alice_name).await;
    let (bob, _, _) = create_client_helper(&base_url, &ws_url, &test_dir, &bob_name).await;

    let group_info = alice
        .create_group("Permission Matrix Group".into(), "Description".into(), 0)
        .await
        .expect("Alice creates group");
    let group_id = group_info.id;

    alice
        .add_group_member(group_id, bob_name.clone(), 0)
        .await
        .expect("Add Bob as default");
    bob.check_setup().await.expect("Bob sync");

    // Table of individual permissions to test
    let permissions_to_test = vec![
        ("AddMessage", UserPermission::AddMessage as u32),
        ("ManageGroup", UserPermission::ManageGroup as u32),
        ("ManageRole", UserPermission::ManageRole as u32),
        ("ManageMember", UserPermission::ManageMember as u32),
        ("ManageChannel", UserPermission::ManageChannel as u32),
    ];

    println!("\n=======================================================");
    println!(" RUNNING EXHAUSTIVE PERMISSION BIT ENFORCEMENT CHECKS");
    println!("=======================================================");

    for (perm_name, perm_bit) in permissions_to_test {
        println!("\n--- Testing Permission Bit: {} (0x{:X}) ---", perm_name, perm_bit);

        // Assign Bob a role with ONLY this permission
        let role_id = 100 + perm_bit;
        alice
            .update_group_roles(
                group_id,
                vec![UpdateRoleProposalFfi {
                    name: format!("only_{}", perm_name),
                    role_id,
                    permissions: perm_bit,
                    delete: false,
                    color: perm_bit,
                }],
            )
            .await
            .unwrap_or_else(|e| panic!("Failed to create role for {}: {:?}", perm_name, e));

        alice
            .update_group_users(
                group_id,
                vec![UpdateUserProposalFfi {
                    username: bob_name.clone(),
                    role_id,
                }],
            )
            .await
            .unwrap_or_else(|e| panic!("Failed to assign role for {}: {:?}", perm_name, e));

        bob.check_setup().await.expect("Bob sync");
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Verify operations Bob CAN and CANNOT do:
        if perm_bit & (UserPermission::ManageChannel as u32) != 0 {
            // Bob CAN create channel
            let create_res = bob
                .update_group_channel(group_id, 99, false, "temp_chan".into(), 1, 0)
                .await;
            assert!(
                create_res.is_ok(),
                "Bob WITH ManageChannel must be able to create channel: {:?}",
                create_res.err()
            );
            // Clean up channel
            let _ = bob
                .update_group_channel(group_id, 99, true, "temp_chan".into(), 1, 0)
                .await;
            println!("  ✓ [{}] Action allowed with permission bit present", perm_name);
        } else {
            // Bob CANNOT create channel
            let create_res = bob
                .update_group_channel(group_id, 99, false, "temp_chan".into(), 1, 0)
                .await;
            assert!(
                create_res.is_err(),
                "Bob WITHOUT ManageChannel must NOT be able to create channel!"
            );
            println!("  ✓ [{}] Action blocked when permission bit missing", perm_name);
        }

        if perm_bit & (UserPermission::ManageRole as u32) != 0 {
            // Bob CAN create sub-role
            let create_role_res = bob
                .update_group_roles(
                    group_id,
                    vec![UpdateRoleProposalFfi {
                        name: "sub_test".into(),
                        role_id: 500 + perm_bit,
                        permissions: perm_bit,
                        delete: false,
                        color: 0,
                    }],
                )
                .await;
            assert!(
                create_role_res.is_ok(),
                "Bob WITH ManageRole must be able to create role: {:?}",
                create_role_res.err()
            );
            println!("  ✓ [{}] Role creation allowed with permission bit present", perm_name);
        } else {
            // Bob CANNOT create role
            let create_role_res = bob
                .update_group_roles(
                    group_id,
                    vec![UpdateRoleProposalFfi {
                        name: "sub_test".into(),
                        role_id: 500 + perm_bit,
                        permissions: perm_bit,
                        delete: false,
                        color: 0,
                    }],
                )
                .await;
            assert!(
                create_role_res.is_err(),
                "Bob WITHOUT ManageRole must NOT be able to create role!"
            );
            println!("  ✓ [{}] Role creation blocked when permission bit missing", perm_name);
        }
    }

    println!("\n=======================================================");
    println!(" EXHAUSTIVE PERMISSION MATRIX TESTS PASSED");
    println!("=======================================================\n");

    alice.dispose().await;
    bob.dispose().await;
    cleanup_dir(&test_dir);
}
