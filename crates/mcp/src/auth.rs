//! Signing in with an email and password, like a person does, and staying signed in.
//!
//! The account is an ordinary user of the Auth0 tenant the Firefly server trusts. `login`
//! uses the password-realm grant once and keeps the refresh token in a file only the owner can
//! read; the server then renews the access token before it expires.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, anyhow, bail};
use base64::Engine;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

/// Renew this long before the access token expires.
const RENEW_BEFORE: Duration = Duration::from_secs(300);

#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// `https://tenant.auth0.com` (a bare host gets `https://`).
    pub domain: String,
    pub client_id: String,
    pub audience: String,
    /// The database connection holding email and password accounts.
    pub realm: String,
}

impl AuthConfig {
    fn endpoint(&self, path: &str) -> String {
        let domain = self.domain.trim_end_matches('/');
        if domain.starts_with("http://") || domain.starts_with("https://") {
            format!("{domain}{path}")
        } else {
            format!("https://{domain}{path}")
        }
    }

    fn token_url(&self) -> String {
        self.endpoint("/oauth/token")
    }

    fn revoke_url(&self) -> String {
        self.endpoint("/oauth/revoke")
    }
}

/// Signs out: revokes the session's refresh token with the sign-in server (refresh tokens never
/// expire on their own) and deletes the session file. The file is deleted even if the server
/// can't be reached; the error is returned so the user knows the token may still be valid.
pub async fn logout(config: &AuthConfig, path: &Path) -> anyhow::Result<bool> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e.into()),
    };
    let revoked: anyhow::Result<()> = async {
        let session: Session = serde_json::from_slice(&bytes)?;
        let http = reqwest::Client::builder().timeout(Duration::from_secs(30)).build()?;
        let response = http
            .post(config.revoke_url())
            .json(&serde_json::json!({ "client_id": config.client_id, "token": session.refresh_token }))
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(anyhow!("[{}] {}", response.status(), response.text().await.unwrap_or_default()));
        }
        Ok(())
    }
    .await;
    std::fs::remove_file(path)?;
    revoked.context("the session was removed, but revoking its refresh token failed")?;
    Ok(true)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub email: String,
    pub username: String,
    pub access_token: String,
    pub refresh_token: String,
    /// Unix seconds.
    pub expires_at: u64,
}

/// The account must sign in again (`login`): its refresh token was revoked or expired.
#[derive(Debug)]
pub struct SignInRequired(pub String);

impl std::fmt::Display for SignInRequired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for SignInRequired {}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    expires_in: u64,
}

#[derive(Deserialize)]
struct TokenError {
    error: String,
    error_description: Option<String>,
}

fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

/// The Lupyd username the tenant puts in the access token once the account has one.
fn username_claim(access_token: &str) -> Option<String> {
    let payload = access_token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(payload.trim_end_matches('=')).ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    claims.get("uname")?.as_str().filter(|u| !u.is_empty()).map(str::to_string)
}

async fn request_token(http: &reqwest::Client, config: &AuthConfig, body: serde_json::Value) -> anyhow::Result<TokenResponse> {
    let response = http.post(config.token_url()).json(&body).send().await.context("can't reach the sign-in server")?;
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    if status.is_success() {
        return serde_json::from_str(&text).context("unexpected sign-in response");
    }
    let detail = serde_json::from_str::<TokenError>(&text)
        .map(|e| (e.error.clone(), e.error_description.unwrap_or(e.error)))
        .unwrap_or_else(|_| (String::new(), text.clone()));
    if status.is_client_error() && matches!(detail.0.as_str(), "invalid_grant" | "unauthorized_client" | "access_denied") {
        return Err(SignInRequired(detail.1).into());
    }
    bail!("sign-in failed ({status}): {}", detail.1)
}

fn write_private(path: &Path, contents: &[u8]) -> anyhow::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("tmp");
    {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        let mut file = options.open(&tmp)?;
        std::io::Write::write_all(&mut file, contents)?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

pub struct Auth {
    config: AuthConfig,
    path: PathBuf,
    http: reqwest::Client,
    session: Mutex<Session>,
}

impl Auth {
    /// Signs in with the password-realm grant and saves the session at `path`.
    pub async fn login(config: &AuthConfig, path: &Path, email: &str, password: &str) -> anyhow::Result<Session> {
        let http = reqwest::Client::builder().timeout(Duration::from_secs(30)).build()?;
        let body = serde_json::json!({
            "grant_type": "http://auth0.com/oauth/grant-type/password-realm",
            "realm": config.realm,
            "username": email,
            "password": password,
            "client_id": config.client_id,
            "audience": config.audience,
            "scope": "openid profile email offline_access",
        });
        let tokens = request_token(&http, config, body).await?;
        let refresh_token = tokens
            .refresh_token
            .ok_or_else(|| anyhow!("the sign-in server returned no refresh token (offline_access not allowed)"))?;
        let username = username_claim(&tokens.access_token).ok_or_else(|| {
            anyhow!("{email} has no username yet. Finish signing up (choose a username) in the app once, then log in here again.")
        })?;
        let session = Session {
            email: email.to_string(),
            username,
            access_token: tokens.access_token,
            refresh_token,
            expires_at: now() + tokens.expires_in,
        };
        write_private(path, &serde_json::to_vec_pretty(&session)?)?;
        Ok(session)
    }

    /// The session `login` saved.
    pub fn load(config: AuthConfig, path: PathBuf) -> anyhow::Result<Self> {
        let bytes = std::fs::read(&path)
            .map_err(|_| SignInRequired(format!("not signed in yet (no session at {})", path.display())))?;
        let session: Session = serde_json::from_slice(&bytes).with_context(|| format!("unreadable session {}", path.display()))?;
        let http = reqwest::Client::builder().timeout(Duration::from_secs(30)).build()?;
        Ok(Self { config, path, http, session: Mutex::new(session) })
    }

    pub async fn username(&self) -> String {
        self.session.lock().await.username.clone()
    }

    /// A valid access token, renewed first when it expires soon.
    pub async fn access_token(&self) -> anyhow::Result<String> {
        let mut session = self.session.lock().await;
        if session.expires_at > now() + RENEW_BEFORE.as_secs() {
            return Ok(session.access_token.clone());
        }
        let body = serde_json::json!({
            "grant_type": "refresh_token",
            "client_id": self.config.client_id,
            "refresh_token": session.refresh_token,
        });
        let tokens = request_token(&self.http, &self.config, body).await?;
        session.access_token = tokens.access_token;
        session.expires_at = now() + tokens.expires_in;
        // Rotating refresh tokens come back with each renewal.
        if let Some(refresh) = tokens.refresh_token {
            session.refresh_token = refresh;
        }
        write_private(&self.path, &serde_json::to_vec_pretty(&*session)?)?;
        Ok(session.access_token.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_username_claim() {
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(br#"{"uname":"helper_ai","sub":"auth0|1"}"#);
        assert_eq!(username_claim(&format!("h.{payload}.s")), Some("helper_ai".into()));
        let none = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(br#"{"sub":"auth0|1"}"#);
        assert_eq!(username_claim(&format!("h.{none}.s")), None);
    }

    #[test]
    fn token_url_accepts_bare_hosts() {
        let config = |domain: &str| AuthConfig {
            domain: domain.into(),
            client_id: String::new(),
            audience: String::new(),
            realm: String::new(),
        };
        assert_eq!(config("auth.example.com").token_url(), "https://auth.example.com/oauth/token");
        assert_eq!(config("auth.example.com").revoke_url(), "https://auth.example.com/oauth/revoke");
        assert_eq!(config("https://auth.example.com/").token_url(), "https://auth.example.com/oauth/token");
    }
}
