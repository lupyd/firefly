//! Classification of group departures used to authorize automatic re-adds requested by the
//! (untrusted) server.

use firefly_core::{Departure, RosterLeaf, compute_departures};

fn leaf(username: &str, expired: bool) -> RosterLeaf {
    RosterLeaf { username: username.into(), expired }
}

fn departed(username: &str, re_addable: bool, epoch: u64) -> Departure {
    Departure { username: username.into(), re_addable, epoch }
}

#[test]
fn no_change_means_no_departures() {
    let before = [leaf("alice", false), leaf("bob", true)];
    assert!(compute_departures(&before, ["alice", "bob"], 3).is_empty());
}

#[test]
fn kicking_a_live_user_is_not_re_addable() {
    let before = [leaf("alice", false), leaf("bob", false)];
    assert_eq!(compute_departures(&before, ["alice"], 5), vec![departed("bob", false, 5)]);
}

#[test]
fn removing_all_expired_devices_is_re_addable() {
    let before = [leaf("alice", false), leaf("bob", true), leaf("bob", true)];
    assert_eq!(compute_departures(&before, ["alice"], 9), vec![departed("bob", true, 9)]);
}

#[test]
fn any_live_removed_device_makes_departure_non_re_addable() {
    for before in [
        [leaf("alice", false), leaf("bob", true), leaf("bob", false)],
        [leaf("alice", false), leaf("bob", false), leaf("bob", true)],
    ] {
        assert_eq!(compute_departures(&before, ["alice"], 2), vec![departed("bob", false, 2)]);
    }
}

#[test]
fn removing_some_devices_of_a_remaining_user_is_not_a_departure() {
    // Stale-device cleanup / address rotation: bob keeps at least one leaf.
    let before = [leaf("alice", false), leaf("bob", true), leaf("bob", false)];
    assert!(compute_departures(&before, ["alice", "bob"], 4).is_empty());
}

#[test]
fn removing_and_re_adding_a_user_in_one_commit_is_not_a_departure() {
    let before = [leaf("alice", false), leaf("bob", false)];
    assert!(compute_departures(&before, ["alice", "bob"], 4).is_empty());
}

#[test]
fn several_users_departing_together_are_classified_independently() {
    let before = [
        leaf("alice", false),
        leaf("bob", true),
        leaf("carol", false),
        leaf("dave", true),
        leaf("dave", false),
    ];
    let mut got = compute_departures(&before, ["alice", "newcomer"], 7);
    got.sort_by(|a, b| a.username.cmp(&b.username));
    assert_eq!(
        got,
        vec![departed("bob", true, 7), departed("carol", false, 7), departed("dave", false, 7)]
    );
}

#[test]
fn added_users_are_not_departures() {
    let before = [leaf("alice", false)];
    assert!(compute_departures(&before, ["alice", "mallory"], 1).is_empty());
}
