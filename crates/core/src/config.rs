use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use firefly_protos::deserialize_proto;
use mls_rs::{
    CipherSuite, ExtensionList, IdentityProvider,
    error::MlsError,
    identity::{Credential, CredentialType, CustomCredential, MlsCredential, SigningIdentity},
    time::MlsTime,
};
use mls_rs_core::identity::MemberValidationContext;

use mls_rs::{
    error::IntoAnyError,
    group::{
        Roster,
        proposal::{MlsCustomProposal, ProposalType},
    },
    mls_rs_codec::{MlsDecode, MlsEncode, MlsSize},
};
use reqwest::header::{self, HeaderMap};
use tokio::sync::RwLock;

use crate::utils::get_current_timestamp_in_secs;

use crate::{
    jwk::JsonWebKeys,
    protos::firefly::{AuthToken, SignedToken},
    utils::HTTP_CLIENT,
};

pub fn verify_signed_token<'a, 'b>(
    keys: JsonWebKeys<'b>,
    signed_token: &'a SignedToken<'a>,
) -> anyhow::Result<AuthToken<'a>> {
    let auth_token = deserialize_proto::<AuthToken>(&signed_token.payload)?;

    let now = get_current_timestamp_in_secs();

    if !*crate::utils::EMULATOR_MODE && auth_token.valid_until < get_current_timestamp_in_secs() {
        return Err(anyhow::anyhow!(
            "token expired valid_until {}: now {}",
            auth_token.valid_until,
            now
        ));
    }

    let verifier_key = keys
        .keys
        .iter()
        .find(|x| x.kid() == signed_token.kid)
        .ok_or(anyhow::anyhow!(
            "no key with associated kid '{}' found",
            signed_token.kid
        ))?;

    let result = verifier_key.verify(&signed_token.payload, &signed_token.signature);

    if result {
        return Ok(auth_token);
    } else {
        return Err(anyhow::anyhow!("verification failed"));
    }
}

pub const CIPHERSUITE: CipherSuite = CipherSuite::P256_AES128;

#[derive(Debug, thiserror::Error)]
pub enum FireflyError {
    Custom(String),
    Codec(#[from] mls_rs_codec::Error),
    Mls(#[from] mls_rs::error::MlsError),
    Extension(#[from] mls_rs::error::ExtensionError),
    Anyhow(#[from] anyhow::Error),
    Proto(#[from] quick_protobuf::Error),
}

impl FireflyError {
    pub fn new(s: impl Into<String>) -> Self {
        Self::Custom(s.into())
    }
}

impl std::fmt::Display for FireflyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<&str> for FireflyError {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for FireflyError {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl IntoAnyError for FireflyError {
    fn into_dyn_error(self) -> Result<Box<dyn std::error::Error + Send + Sync>, Self> {
        Err(self)
    }
}

const UPDATE_ROLE_PROPOSAL_TYPE: ProposalType = ProposalType::new(30006);
const UPDATE_USER_PROPOSAL_TYPE: ProposalType = ProposalType::new(30007);
const UPDATE_CHANNEL_PROPOSAL_TYPE: ProposalType = ProposalType::new(30008);
const UPDATE_ROLE_IN_CHANNEL_PROPOSAL_TYPE: ProposalType = ProposalType::new(30009);
const UPDATE_USER_IN_CHANNEL_PROPOSAL_TYPE: ProposalType = ProposalType::new(30010);

#[derive(Copy, Clone, Debug)]
#[repr(u32)]
pub enum UserPermission {
    AddMessage = 4,
    ManageChannel = 8,
    ManageRole = 16,
    ManageMember = 32,
    ManageGroup = 64,
}

#[derive(MlsSize, MlsDecode, MlsEncode)]
pub struct UpdateRoleProposal {
    pub name: String,
    pub role_id: u32,
    pub permissions: u32,
    pub delete: bool,
    pub color: u32,
}

impl MlsCustomProposal for UpdateRoleProposal {
    fn proposal_type() -> ProposalType {
        UPDATE_ROLE_PROPOSAL_TYPE
    }
}

#[derive(MlsSize, MlsDecode, MlsEncode)]
pub struct UpdateUserProposal {
    pub username: String,
    pub role_id: u32,
}

impl MlsCustomProposal for UpdateUserProposal {
    fn proposal_type() -> ProposalType {
        UPDATE_USER_PROPOSAL_TYPE
    }
}

#[derive(MlsSize, MlsDecode, MlsEncode)]
pub struct UpdateChannelProposal {
    pub id: u32,
    pub delete: bool, // set this flag to delete the channel
    pub name: String,
    pub channel_ty: u8,
    pub default_permissions: u32,
}

#[derive(MlsSize, MlsDecode, MlsEncode)]
pub struct UpdateRoleInChannelProposal {
    pub channel_id: u32,
    pub role_proposal: UpdateRoleProposal,
}

impl MlsCustomProposal for UpdateRoleInChannelProposal {
    fn proposal_type() -> ProposalType {
        UPDATE_ROLE_IN_CHANNEL_PROPOSAL_TYPE
    }
}

#[derive(MlsSize, MlsDecode, MlsEncode)]
pub struct UpdateUserInChannelProposal {
    pub channel_id: u32,
    pub username: String,
    pub role_id: u32,
    pub delete: bool,
}

impl MlsCustomProposal for UpdateUserInChannelProposal {
    fn proposal_type() -> ProposalType {
        UPDATE_USER_IN_CHANNEL_PROPOSAL_TYPE
    }
}

impl MlsCustomProposal for UpdateChannelProposal {
    fn proposal_type() -> ProposalType {
        return UPDATE_CHANNEL_PROPOSAL_TYPE;
    }
}

pub fn is_valid_name(name: &str) -> bool {
    let accepted_ranges = ['a'..='z', 'A'..='Z', '0'..='9'];
    let accepted_chars = ['_', ' ', '-', '.', '#'];

    !name.is_empty()
        && !name.trim().is_empty()
        && name.len() <= 32
        && name
            .chars()
            .all(|c| accepted_ranges.iter().any(|x| x.contains(&c)) || accepted_chars.contains(&c))
}

pub fn does_roster_has_username(username: &str, roster: &Roster) -> bool {
    for member in roster.members_iter() {
        let Ok(credential) = FireflyCredential::from_signing_identity(&member.signing_identity)
        else {
            continue;
        };

        let Ok(signed_token) = credential.signed_token() else {
            continue;
        };
        let Ok(auth_token) = deserialize_proto::<AuthToken>(&signed_token.payload) else {
            continue;
        };
        if auth_token.username == username {
            return true;
        }
    }

    return false;
}

// returns if extension is updated

#[derive(PartialEq, Clone)]
pub struct FireflyCredential {
    credential: Vec<u8>,
}

impl FireflyCredential {
    pub fn signed_token<'a>(&'a self) -> Result<SignedToken<'a>, quick_protobuf::Error> {
        let signed_token: SignedToken = deserialize_proto(&self.credential)?;

        Ok(signed_token)
    }

    pub fn new(credential: Vec<u8>) -> Result<Self, quick_protobuf::Error> {
        let signed_token: SignedToken = deserialize_proto(&credential)?;
        let _auth_token: AuthToken = deserialize_proto(&signed_token.payload)?;

        Ok(Self { credential })
    }

    pub fn from_signing_identity(s: &SigningIdentity) -> anyhow::Result<Self> {
        Self::from_credential(&s.credential)
    }

    pub fn from_credential(credential: &Credential) -> anyhow::Result<Self> {
        let credential = credential
            .as_custom()
            .ok_or(MlsError::RequiredCredentialNotFound(Self::credential_type()))?;

        let s = Self::new(credential.data.clone())?;

        Ok(s)
    }

    pub fn is_expired(&self) -> Option<()> {
        let valid_until = self.valid_until_secs().ok()?;
        let now = get_current_timestamp_in_secs();
        if valid_until > now { None } else { Some(()) }
    }

    pub fn valid_until_secs(&self) -> Result<u64, quick_protobuf::Error> {
        let signed_token: SignedToken = deserialize_proto(&self.credential)?;
        let auth_token: AuthToken = deserialize_proto(&signed_token.payload)?;
        return Ok(auth_token.valid_until);
    }
}

impl MlsCredential for FireflyCredential {
    type Error = MlsError;

    fn credential_type() -> CredentialType {
        CredentialType::new(40001)
    }

    fn into_credential(self) -> Result<Credential, Self::Error> {
        Ok(Credential::Custom(CustomCredential {
            credential_type: Self::credential_type(),
            data: self.credential,
        }))
    }
}

#[derive(Clone)]
pub struct FireflyIdentityProvider {
    base_url: Arc<str>,
    // TODO: handle this more elegantly
    keys_string: Arc<RwLock<(String, u64)>>, // jwks body + expiration time in seconds
}

fn get_expiry_from_headers(headers: &HeaderMap) -> anyhow::Result<(u64, String)> {
    let last_modified = match headers.get(header::LAST_MODIFIED) {
        Some(val) => val.to_str()?.to_string(),
        None => httpdate::HttpDate::from(SystemTime::now()).to_string(),
    };

    let expiry = headers
        .get(header::EXPIRES)
        .map(|x| x.to_str().map(|y| httpdate::parse_http_date(y)));
    if let Some(Ok(Ok(exp))) = expiry {
        return Ok((
            exp.duration_since(UNIX_EPOCH).map(|x| x.as_secs())?,
            last_modified,
        ));
    }

    const MAX_AGE_STR: &str = "max-age=";
    let mut cache_duration = 0u64;
    if let Some(Ok(cache_control)) = headers.get(header::CACHE_CONTROL).map(|x| x.to_str()) {
        let directives = cache_control.split(',');
        for directive in directives {
            let directive = directive.trim();
            if let Some(value_str) = directive.strip_prefix(MAX_AGE_STR) {
                if let Ok(v) = value_str.parse() {
                    cache_duration = v;
                }
                break;
            }
        }
    }

    let date = headers
        .get(header::DATE)
        .context("Date Header is missing")?
        .to_str()?;
    let response_created_at = httpdate::parse_http_date(date)?;

    let expiry = response_created_at.duration_since(UNIX_EPOCH)?.as_secs() + cache_duration;

    Ok((expiry, last_modified))
}

impl FireflyIdentityProvider {
    pub fn new(base_url: Arc<str>) -> Self {
        Self {
            base_url,
            keys_string: Arc::new(RwLock::new((r#"""{"keys": []}"""#.to_string(), 0))),
        }
    }

    async fn refresh(&self) -> anyhow::Result<()> {
        let mut g = self.keys_string.write().await;

        let url = format!("{}/jwks.json", self.base_url);

        let response = HTTP_CLIENT.get(url).send().await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "unexpected status code [{}]: {}",
                response.status(),
                response.text().await?
            ));
        }

        let (expiry, _) = get_expiry_from_headers(response.headers())?;
        let jwks = response.text().await?;

        *g = (jwks, expiry);
        Ok(())
    }

    pub async fn get_keys(&self) -> anyhow::Result<String> {
        {
            let g = self.keys_string.read().await;
            if g.1 > get_current_timestamp_in_secs() {
                return Ok(g.0.clone());
            }
        }

        self.refresh().await?;

        let last_fetched_expiry_secs: u64;
        {
            let g = self.keys_string.read().await;
            last_fetched_expiry_secs = g.1;
            if g.1 > get_current_timestamp_in_secs() {
                return Ok(g.0.clone());
            }
        }

        return Err(anyhow::anyhow!(
            "failed to fetch fresh jwks last fetched expired at: {}",
            last_fetched_expiry_secs
        ));
    }

    pub async fn verify<'a>(
        &self,
        signed_token: &'a SignedToken<'a>,
    ) -> anyhow::Result<AuthToken<'a>> {
        let jwks = self.get_keys().await?;
        let keys: JsonWebKeys = serde_json::from_str(&jwks)?;

        let result = verify_signed_token(keys, signed_token)?;

        Ok(result)
    }

    pub async fn get_credential(
        &self,
        public_key: Vec<u8>,
        token: String,
        device_id: u8,
        address_id: u64,
    ) -> anyhow::Result<Vec<u8>> {
        let url = format!(
            "{}/sign?device_id={}&address_id={}",
            self.base_url, device_id, address_id
        );
        let response = HTTP_CLIENT
            .post(url)
            .bearer_auth(token)
            .body(public_key)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "unexpected status code [{}]: {}",
                response.status(),
                response.text().await?
            ));
        }

        let body = response.bytes().await?;

        let _: SignedToken = deserialize_proto(&body)?;
        return Ok(body.to_vec());
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl IdentityProvider for FireflyIdentityProvider {
    #[doc = " Error type that this provider returns on internal failure."]
    type Error = FireflyError;

    #[doc = " Determine if `signing_identity` is valid for a group member."]
    #[doc = ""]
    #[doc = " A `timestamp` value can optionally be supplied to aid with validation"]
    #[doc = " of a [`Credential`](mls-rs-core::identity::Credential) that requires"]
    #[doc = " time based context. For example, X.509 certificates can become expired."]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    async fn validate_member<'life0, 'life1, 'life2>(
        &'life0 self,
        signing_identity: &'life1 SigningIdentity,
        _timestamp: Option<MlsTime>,
        _context: MemberValidationContext<'life2>,
    ) -> Result<(), Self::Error> {
        let credential = FireflyCredential::from_signing_identity(signing_identity)?;

        let signed_token: SignedToken = credential.signed_token()?;
        if let Err(e) = self.verify(&signed_token).await {
            log::error!("validate_member failed: verify failed: {}", e);
            return Err(FireflyError::Anyhow(e));
        }

        Ok(())
    }

    #[doc = " Determine if `signing_identity` is valid for an external sender in"]
    #[doc = " the ExternalSendersExtension stored in the group context."]
    #[doc = ""]
    #[doc = " A `timestamp` value can optionally be supplied to aid with validation"]
    #[doc = " of a [`Credential`](mls-rs-core::identity::Credential) that requires"]
    #[doc = " time based context. For example, X.509 certificates can become expired."]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    async fn validate_external_sender<'life0, 'life1, 'life2>(
        &'life0 self,
        signing_identity: &'life1 SigningIdentity,
        _timestamp: Option<MlsTime>,
        _extensions: Option<&'life2 ExtensionList>,
    ) -> Result<(), Self::Error> {
        let credential = FireflyCredential::from_signing_identity(signing_identity)?;
        let signed_token = credential.signed_token()?;
        let _result = self.verify(&signed_token).await?;

        Ok(())
    }

    #[doc = " A unique identifier for `signing_identity`."]
    #[doc = ""]
    #[doc = " The MLS protocol requires that each member of a group has a"]
    #[doc = " unique identifiers, which is determined by the application."]
    #[doc = " The identity must be stable over the lifetime of the group."]
    #[doc = ""]
    #[doc = " The identity does not need to be consistent for different"]
    #[doc = " group members: Alice might use `b\"bob-123\"` as the identity"]
    #[doc = " for Bob, while Bob on his side could use `b\"Bob\"` for himself."]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    async fn identity<'life0, 'life1, 'life2>(
        &'life0 self,
        signing_identity: &'life1 SigningIdentity,
        _extensions: &'life2 ExtensionList,
    ) -> Result<Vec<u8>, Self::Error> {
        let credential = FireflyCredential::from_signing_identity(signing_identity)?;
        let token = credential.signed_token()?;
        let token = deserialize_proto::<AuthToken>(&token.payload)?;

        let mut identity = Vec::new();

        identity.extend(token.address_id.to_le_bytes());
        identity.extend(token.device_id.to_le_bytes());

        identity.extend_from_slice(token.username.as_bytes());

        // address_id + device_id + username always unique and will last
        // address_id is unique enough too

        Ok(identity)
    }

    #[doc = " Determines if `successor` can remove `predecessor` as part of an external commit."]
    #[doc = ""]
    #[doc = " The MLS protocol allows for removal of an existing member when adding a"]
    #[doc = " new member via external commit. This function determines if a removal"]
    #[doc = " should be allowed by providing the target member to be remoed as"]
    #[doc = " `predecessor` and the new member as `successor`."]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    async fn valid_successor<'life0, 'life1, 'life2, 'life3>(
        &'life0 self,
        predecessor: &'life1 SigningIdentity,
        successor: &'life2 SigningIdentity,
        _extensions: &'life3 ExtensionList,
    ) -> Result<bool, Self::Error> {
        let successor_credential = FireflyCredential::from_credential(&successor.credential)?;
        let predecessor_credential = FireflyCredential::from_credential(&predecessor.credential)?;

        let predecessor_signed_token = predecessor_credential.signed_token()?;
        let successor_signed_token = successor_credential.signed_token()?;

        let predecessor_auth_token =
            deserialize_proto::<AuthToken>(&predecessor_signed_token.payload)?;
        let successor_auth_token = deserialize_proto::<AuthToken>(&successor_signed_token.payload)?;

        if predecessor_auth_token.username == successor_auth_token.username {
            return Ok(true);
        }

        return Ok(false);
    }

    #[doc = " Credential types that are supported by this provider."]
    fn supported_types(&self) -> Vec<CredentialType> {
        vec![FireflyCredential::credential_type()]
    }
}
