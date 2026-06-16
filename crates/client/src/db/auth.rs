use anyhow::Context;
use base64::{Engine, prelude::BASE64_URL_SAFE_NO_PAD};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct TokenClaims {
    pub uname: String,
    pub perms: u64,
    pub exp: u64,
}

pub fn get_claims_from_token(s: &str) -> anyhow::Result<TokenClaims> {
    if std::env::var("EMULATOR_MODE").unwrap_or_default() == "true" {
        return Ok(TokenClaims {
            uname: s.to_string(),
            perms: u64::MAX,
            exp: u64::MAX,
        });
    }
    let (payload, _signature) = s.rsplit_once('.').context("invalid token: missing '.'")?;

    let (_header, payload) = payload
        .rsplit_once('.')
        .context("invalid token: missing '.'")?;

    let payload = BASE64_URL_SAFE_NO_PAD.decode(&payload)?;

    let claims = serde_json::from_slice::<TokenClaims>(&payload)?;

    return Ok(claims);
}
