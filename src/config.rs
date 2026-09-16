use std::{env, net::SocketAddr, path::PathBuf, time::Duration};

use anyhow::{Context, Result};

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub http_addr: SocketAddr,
    pub udp_addr: SocketAddr,
    pub database_path: PathBuf,
    pub auth_token: Option<String>,
    pub announce_interval: u32,
    pub peer_timeout: Duration,
    pub persistence_interval: Duration,
    pub rate_limit_per_minute: u32,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            http_addr: parse_env("HIVE_HTTP_ADDR", "[::]:3000")?,
            udp_addr: parse_env("HIVE_UDP_ADDR", "[::]:6969")?,
            database_path: env::var_os("HIVE_DATABASE_PATH")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("hive.db")),
            auth_token: env::var("HIVE_AUTH_TOKEN")
                .ok()
                .filter(|value| !value.is_empty()),
            announce_interval: parse_env("HIVE_ANNOUNCE_INTERVAL", "1800")?,
            peer_timeout: Duration::from_secs(parse_env("HIVE_PEER_TIMEOUT", "3600")?),
            persistence_interval: Duration::from_secs(parse_env(
                "HIVE_PERSISTENCE_INTERVAL",
                "30",
            )?),
            rate_limit_per_minute: parse_env("HIVE_RATE_LIMIT_PER_MINUTE", "120")?,
        })
    }
}

fn parse_env<T>(name: &str, default: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    env::var(name)
        .unwrap_or_else(|_| default.to_owned())
        .parse()
        .with_context(|| format!("invalid value for {name}"))
}
