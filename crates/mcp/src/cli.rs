//! The `firefly-mcp` command line, shared with wrappers that bake in their service's endpoints
//! (`lupyd-mcp`): `login` once, then `serve` for an MCP client to start.

use std::io::IsTerminal;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use clap::{Args, Parser, Subcommand};
use rmcp::ServiceExt;

use crate::agent::{Agent, Endpoints};
use crate::auth::{Auth, AuthConfig};
use crate::server::FireflyMcp;

/// Endpoints and sign-in settings a wrapper provides; flags and environment variables override them.
#[derive(Debug, Clone, Default)]
pub struct Defaults {
    /// The program's name: the MCP server name and the data directory under the user's data dir.
    pub name: &'static str,
    pub api_url: Option<&'static str>,
    pub ws_url: Option<&'static str>,
    pub wt_url: Option<&'static str>,
    pub auth0_domain: Option<&'static str>,
    pub auth0_client_id: Option<&'static str>,
    pub auth0_audience: Option<&'static str>,
}

#[derive(Parser)]
#[command(version, about = "A local MCP server that signs in to Firefly as an ordinary user, so an LLM can chat in its direct messages and groups.")]
struct Cli {
    #[command(flatten)]
    common: Common,
    #[command(subcommand)]
    command: Command,
}

#[derive(Args)]
struct Common {
    /// Where the account's keys, messages and session are kept. Default: <data dir>/<name>/<profile>.
    #[arg(long, env = "FIREFLY_MCP_DATA_DIR", global = true)]
    data_dir: Option<PathBuf>,
    /// Keeps several accounts apart under the default data directory.
    #[arg(long, env = "FIREFLY_MCP_PROFILE", default_value = "default", global = true)]
    profile: String,
    #[arg(long, env = "FIREFLY_API_URL", global = true)]
    api_url: Option<String>,
    #[arg(long, env = "FIREFLY_WS_URL", global = true)]
    ws_url: Option<String>,
    /// WebTransport endpoint tried before the WebSocket; "none" turns it off.
    #[arg(long, env = "FIREFLY_WT_URL", global = true)]
    wt_url: Option<String>,
    #[arg(long, env = "AUTH0_DOMAIN", global = true)]
    auth0_domain: Option<String>,
    #[arg(long, env = "AUTH0_CLIENT_ID", global = true)]
    auth0_client_id: Option<String>,
    #[arg(long, env = "AUTH0_AUDIENCE", global = true)]
    auth0_audience: Option<String>,
    /// The Auth0 database connection of email and password accounts.
    #[arg(long, env = "AUTH0_REALM", default_value = "Username-Password-Authentication", global = true)]
    auth0_realm: String,
}

#[derive(Subcommand)]
enum Command {
    /// Sign in with the account's email and password (asked for, or FIREFLY_MCP_PASSWORD).
    Login {
        #[arg(long, env = "FIREFLY_MCP_EMAIL")]
        email: String,
    },
    /// Forget the saved sign-in. The device's keys stay; logging in again keeps its groups.
    Logout,
    /// Show who is signed in.
    Status,
    /// Run the MCP server on stdin and stdout (what an MCP client starts).
    Serve,
}

fn pick(flag: &Option<String>, default: Option<&'static str>, what: &str) -> anyhow::Result<String> {
    flag.clone().or(default.map(str::to_string)).ok_or_else(|| anyhow!("{what} is not set (see --help)"))
}

impl Common {
    fn data_dir(&self, defaults: &Defaults) -> anyhow::Result<PathBuf> {
        match &self.data_dir {
            Some(dir) => Ok(dir.clone()),
            None => Ok(dirs::data_dir().context("no data directory; pass --data-dir")?.join(defaults.name).join(&self.profile)),
        }
    }

    fn auth(&self, defaults: &Defaults) -> anyhow::Result<AuthConfig> {
        Ok(AuthConfig {
            domain: pick(&self.auth0_domain, defaults.auth0_domain, "--auth0-domain")?,
            client_id: pick(&self.auth0_client_id, defaults.auth0_client_id, "--auth0-client-id")?,
            audience: pick(&self.auth0_audience, defaults.auth0_audience, "--auth0-audience")?,
            realm: self.auth0_realm.clone(),
        })
    }

    fn endpoints(&self, defaults: &Defaults) -> anyhow::Result<Endpoints> {
        let wt_url = self.wt_url.clone().or(defaults.wt_url.map(str::to_string)).filter(|u| u != "none" && !u.is_empty());
        Ok(Endpoints {
            api_url: pick(&self.api_url, defaults.api_url, "--api-url")?,
            ws_url: pick(&self.ws_url, defaults.ws_url, "--ws-url")?,
            wt_url,
        })
    }
}

fn read_password() -> anyhow::Result<String> {
    if let Ok(password) = std::env::var("FIREFLY_MCP_PASSWORD") {
        return Ok(password);
    }
    if std::io::stdin().is_terminal() {
        return Ok(rpassword::prompt_password("Password: ")?);
    }
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;
    Ok(line.trim_end_matches(['\r', '\n']).to_string())
}

/// Runs the command line; returns the process exit code.
pub fn run(defaults: Defaults) -> i32 {
    // stdout carries the MCP protocol; logs go to stderr.
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn,firefly_mcp=info"))
        .target(env_logger::Target::Stderr)
        .init();
    let cli = Cli::parse();
    let runtime = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("{e}");
            return 1;
        }
    };
    match runtime.block_on(execute(cli, defaults)) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("{}: {e:#}", std::env::args().next().unwrap_or_default());
            // 78 (EX_CONFIG): a person has to sign the account in again.
            if e.downcast_ref::<crate::auth::SignInRequired>().is_some() { 78 } else { 1 }
        }
    }
}

async fn execute(cli: Cli, defaults: Defaults) -> anyhow::Result<()> {
    let data_dir = cli.common.data_dir(&defaults)?;
    let session_path = data_dir.join("session.json");
    match cli.command {
        Command::Login { email } => {
            let password = read_password()?;
            let session = Auth::login(&cli.common.auth(&defaults)?, &session_path, &email, &password).await?;
            eprintln!("Signed in as {} ({}). Data: {}", session.username, session.email, data_dir.display());
        }
        Command::Logout => {
            match std::fs::remove_file(&session_path) {
                Ok(()) => eprintln!("Signed out ({} removed).", session_path.display()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => eprintln!("Not signed in."),
                Err(e) => return Err(e.into()),
            }
        }
        Command::Status => {
            let auth = Auth::load(cli.common.auth(&defaults)?, session_path)?;
            auth.access_token().await?;
            println!("{}", auth.username().await);
        }
        Command::Serve => {
            let auth = Arc::new(Auth::load(cli.common.auth(&defaults)?, session_path)?);
            // Fail before the MCP client connects when the sign-in is gone.
            auth.access_token().await?;
            let agent = Agent::start(auth, &cli.common.endpoints(&defaults)?, data_dir, defaults.name).await?;
            log::info!("serving {} over stdio", agent.username);
            let service = FireflyMcp::new(agent.clone(), defaults.name).serve(rmcp::transport::stdio()).await?;
            tokio::select! {
                result = service.waiting() => { result?; }
                _ = tokio::signal::ctrl_c() => {}
            }
            agent.dispose().await;
        }
    }
    Ok(())
}
