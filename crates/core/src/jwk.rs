use std::borrow::Cow;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use p256::{EncodedPoint, U32, ecdsa::VerifyingKey};
use rsa::signature::Verifier;
use serde::{Deserialize, Serialize};
use sha2::digest::generic_array::GenericArray;

#[derive(Serialize, Deserialize)]
pub struct JsonWebKeys<'a> {
    #[serde(borrow)]
    pub keys: Vec<JsonWebKey<'a>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JsonWebKey<'a> {
    kid: Cow<'a, str>,
    r#use: &'a str,
    x: Cow<'a, str>,
    y: Cow<'a, str>,
    crv: &'a str,
    kty: &'a str,
    alg: &'a str,
}

impl<'a> JsonWebKey<'a> {
    pub fn kid(&self) -> &str {
        &self.kid
    }

    pub fn verify(&self, payload: &[u8], signature: &[u8]) -> bool {
        let Ok(x) = URL_SAFE_NO_PAD.decode(self.x.as_ref()) else {
            log::error!("base64 decoding x of jwk: {}", self.x);
            return false;
        };

        let Ok(y) = URL_SAFE_NO_PAD.decode(self.y.as_ref()) else {
            log::error!("base64 decoding y of jwk: {}", self.y);
            return false;
        };

        if x.len() != 32 || y.len() != 32 {
            log::error!("invalid coordinate length, expected 32 bytes");

            return false;
        }
        let x = GenericArray::<_, U32>::from_slice(&x);
        let y = GenericArray::<_, U32>::from_slice(&y);
        let points = EncodedPoint::from_affine_coordinates(x, y, false);
        let Ok(key) = p256::ecdsa::VerifyingKey::from_encoded_point(&points) else {
            log::error!("failed construcitng verifying key of kid: {}", self.kid);
            return false;
        };

        let Ok(signature) = p256::ecdsa::Signature::try_from(signature) else {
            return false;
        };
        return key.verify(payload, &signature).is_ok();
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub fn generate_key() -> Result<p256::ecdsa::SigningKey, rsa::Error> {
    let key = p256::ecdsa::SigningKey::random(&mut rand_core::OsRng);

    Ok(key)
}

pub fn ecdsa_to_jwk<'a>(key: &VerifyingKey, kid: impl Into<Cow<'a, str>>) -> JsonWebKey<'a> {
    let points = key.to_encoded_point(false);

    let x = URL_SAFE_NO_PAD.encode(points.x().unwrap());
    let y = URL_SAFE_NO_PAD.encode(points.y().unwrap());

    JsonWebKey {
        kty: "EC",
        r#use: "sig",
        crv: "P-256",
        kid: kid.into(),
        alg: "ES256",
        x: x.into(),
        y: y.into(),
    }
}
