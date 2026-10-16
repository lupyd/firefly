use firefly_protos::firefly;

use crate::{
    callbacks::FireflyWsClientCallback, group::FfiMlsGroup, storage::KeyValueStorage,
    utils::deserialize_proto,
};

/// Diffs the group's member usernames against the last stored snapshot and reports
/// joins/leaves through `on_group_members_changed`. The first snapshot of a group is
/// only recorded, so members present when we joined are never reported as new.
/// Call it after every commit is applied, including commits authored locally.
pub async fn sync_group_members(
    group: &FfiMlsGroup,
    key_value_store: &dyn KeyValueStorage,
    callbacks: &dyn FireflyWsClientCallback,
) {
    if let Err(err) = sync_group_members_inner(group, key_value_store, callbacks).await {
        log::warn!("failed to sync member snapshot of group {}: {:?}", group.group_id(), err);
    }
}

async fn sync_group_members_inner(
    group: &FfiMlsGroup,
    key_value_store: &dyn KeyValueStorage,
    callbacks: &dyn FireflyWsClientCallback,
) -> anyhow::Result<()> {
    let group_id = group.group_id();
    let extension_bytes = group.extension().await?;
    let extension = deserialize_proto::<firefly::FireflyGroupExtension>(&extension_bytes)?;
    let mut current: Vec<String> = extension.members.iter().map(|m| m.username.to_string()).collect();
    current.sort();
    current.dedup();

    let key = format!("group-members:{group_id}");
    let previous: Option<Vec<String>> = key_value_store
        .get(&key)
        .await
        .ok()
        .and_then(|value| serde_json::from_str(&value).ok());
    key_value_store.set(&key, &serde_json::to_string(&current)?).await?;

    let Some(previous) = previous else {
        return Ok(());
    };
    let (added, removed) = diff_members(&previous, &current);
    if !added.is_empty() || !removed.is_empty() {
        callbacks.on_group_members_changed(group_id, added, removed).await;
    }
    Ok(())
}

/// Both inputs must be sorted and deduplicated.
fn diff_members(previous: &[String], current: &[String]) -> (Vec<String>, Vec<String>) {
    let added = current.iter().filter(|u| previous.binary_search(u).is_err()).cloned().collect();
    let removed = previous.iter().filter(|u| current.binary_search(u).is_err()).cloned().collect();
    (added, removed)
}

#[cfg(test)]
mod tests {
    use super::diff_members;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn reports_joins_and_leaves() {
        let (added, removed) = diff_members(&names(&["alice", "bob"]), &names(&["alice", "carol"]));
        assert_eq!(added, names(&["carol"]));
        assert_eq!(removed, names(&["bob"]));
    }

    #[test]
    fn unchanged_roster_reports_nothing() {
        let (added, removed) = diff_members(&names(&["alice", "bob"]), &names(&["alice", "bob"]));
        assert!(added.is_empty() && removed.is_empty());
    }
}
