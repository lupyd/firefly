//! A local MCP server that signs in to Firefly as an ordinary user, so an LLM (through any MCP
//! client or agent harness) can read and answer its direct messages and groups. It is a device
//! of the account like a phone: the keys and decrypted messages stay in its data directory.

pub mod agent;
pub mod auth;
pub mod cli;
pub mod content;
pub mod events;
pub mod server;
