use mls_rs::client_builder::{WithGroupStateStorage, WithKeyPackageRepo, WithPskStore};

use crate::FireflyIdentity;
use crate::storage_provider::{FfiGroupStateStorage, FfiKeyPackageStorage, FfiPreSharedKeyStorage};

use crate::{
    config::{
        FireflyIdentityProvider, UpdateChannelProposal, UpdateRoleProposal, UpdateUserProposal,
    },
    extension::FireflyGroupExtension,
    rules::FireflyMlsRules,
};
use mls_rs::{
    Client,
    client_builder::{BaseConfig, WithCryptoProvider, WithIdentityProvider, WithMlsRules},
    extension::MlsExtension,
    group::proposal::MlsCustomProposal,
};

pub type FireflyClientMlsConfig = WithIdentityProvider<
    FireflyIdentityProvider,
    WithCryptoProvider<
        mls_rs_crypto_rustcrypto::RustCryptoProvider,
        WithMlsRules<
            FireflyMlsRules,
            WithGroupStateStorage<
                FfiGroupStateStorage,
                WithPskStore<
                    FfiPreSharedKeyStorage,
                    WithKeyPackageRepo<FfiKeyPackageStorage, BaseConfig>,
                >,
            >,
        >,
    >,
>;

pub type FireflyClient = Client<FireflyClientMlsConfig>;

#[cfg(not(target_arch = "wasm32"))]
pub fn load_client(
    identity: FireflyIdentity,
    key_package_repo: FfiKeyPackageStorage,
    group_state_storage: FfiGroupStateStorage,
    psk_store: FfiPreSharedKeyStorage,
    identity_provider: FireflyIdentityProvider,
) -> anyhow::Result<FireflyClient> {
    use crate::config::{CIPHERSUITE, UpdateRoleInChannelProposal, UpdateUserInChannelProposal};

    let crypto_provider = mls_rs_crypto_rustcrypto::RustCryptoProvider::default();

    let signing_identity = identity.signing_identity();
    let secret = identity.secret;

    let client = Client::builder()
        .extension_type(FireflyGroupExtension::extension_type())
        .mls_rules(FireflyMlsRules)
        .identity_provider(identity_provider)
        .crypto_provider(crypto_provider)
        .custom_proposal_type(UpdateUserProposal::proposal_type())
        .custom_proposal_type(UpdateRoleProposal::proposal_type())
        .custom_proposal_type(UpdateChannelProposal::proposal_type())
        .custom_proposal_type(UpdateRoleInChannelProposal::proposal_type())
        .custom_proposal_type(UpdateUserInChannelProposal::proposal_type())
        .group_state_storage(group_state_storage)
        .key_package_repo(key_package_repo)
        .psk_store(psk_store)
        .signing_identity(signing_identity, secret, CIPHERSUITE)
        .build();

    Ok(client)
}
