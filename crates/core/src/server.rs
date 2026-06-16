use mls_rs::{
    extension::MlsExtension,
    external_client::{
        ExternalClient,
        builder::{ExternalBaseConfig, WithCryptoProvider, WithIdentityProvider, WithMlsRules},
    },
    group::proposal::MlsCustomProposal,
};
use mls_rs_crypto_rustcrypto::RustCryptoProvider;

use crate::{
    config::{
        FireflyIdentityProvider, UpdateChannelProposal, UpdateRoleInChannelProposal,
        UpdateRoleProposal, UpdateUserProposal,
    },
    extension::FireflyGroupExtension,
    rules::FireflyMlsRules,
};

pub type FireflyServerConfig = WithIdentityProvider<
    FireflyIdentityProvider,
    WithCryptoProvider<RustCryptoProvider, WithMlsRules<FireflyMlsRules, ExternalBaseConfig>>,
>;

pub fn make_server() -> ExternalClient<FireflyServerConfig> {
    let crypto_provider = RustCryptoProvider::default();

    let client = ExternalClient::builder()
        .identity_provider(FireflyIdentityProvider::new(
            std::env::var("FIREFLY_BASE_URL")
                .expect("FIREFLY_BASE_URL env var not set")
                .into(),
        ))
        .cache_proposals(true)
        .mls_rules(FireflyMlsRules)
        .crypto_provider(crypto_provider)
        .custom_proposal_types([
            UpdateUserProposal::proposal_type(),
            UpdateRoleProposal::proposal_type(),
            UpdateChannelProposal::proposal_type(),
            UpdateRoleInChannelProposal::proposal_type(),
        ])
        .extension_type(FireflyGroupExtension::extension_type())
        .build();

    client
}
