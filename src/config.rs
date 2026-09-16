use std::{fs, net::SocketAddr, path::Path, path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use clap::ValueEnum;
use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
#[error("expected http, udp, or both; got {value}")]
pub struct ProtocolParseError {
    value: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    Http,
    Udp,
    Both,
}

impl std::str::FromStr for Protocol {
    type Err = ProtocolParseError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "http" => Ok(Self::Http),
            "udp" => Ok(Self::Udp),
            "both" => Ok(Self::Both),
            _ => Err(ProtocolParseError {
                value: value.to_owned(),
            }),
        }
    }
}

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub default_protocol: Protocol,
    pub http_addr: SocketAddr,
    pub udp_addr: SocketAddr,
    pub database_path: PathBuf,
    pub auth_token: Option<String>,
    pub announce_interval: u32,
    pub peer_timeout: Duration,
    pub persistence_interval: Duration,
    pub rate_limit_per_minute: u32,
    pub log_filter: String,
}

impl AppConfig {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path)
            .with_context(|| format!("failed to read configuration from {}", path.display()))?;
        Self::from_toml(&contents)
            .with_context(|| format!("failed to parse configuration from {}", path.display()))
    }

    fn from_toml(contents: &str) -> Result<Self> {
        let config: FileConfig = toml::from_str(contents)?;
        Ok(config.into())
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileConfig {
    default_protocol: Protocol,
    http_addr: SocketAddr,
    udp_addr: SocketAddr,
    database_path: PathBuf,
    auth_token: Option<String>,
    announce_interval: u32,
    peer_timeout: u64,
    persistence_interval: u64,
    rate_limit_per_minute: u32,
    log_filter: String,
}

impl Default for FileConfig {
    fn default() -> Self {
        Self {
            default_protocol: Protocol::Both,
            http_addr: "0.0.0.0:3000"
                .parse()
                .expect("default HTTP address is valid"),
            udp_addr: "[::]:6969".parse().expect("default UDP address is valid"),
            database_path: PathBuf::from("hive.db"),
            auth_token: None,
            announce_interval: 1800,
            peer_timeout: 3600,
            persistence_interval: 30,
            rate_limit_per_minute: 120,
            log_filter: "hive_tracker=info".to_owned(),
        }
    }
}

impl From<FileConfig> for AppConfig {
    fn from(config: FileConfig) -> Self {
        Self {
            default_protocol: config.default_protocol,
            http_addr: config.http_addr,
            udp_addr: config.udp_addr,
            database_path: config.database_path,
            auth_token: config.auth_token.filter(|value| !value.is_empty()),
            announce_interval: config.announce_interval,
            peer_timeout: Duration::from_secs(config.peer_timeout),
            persistence_interval: Duration::from_secs(config.persistence_interval),
            rate_limit_per_minute: config.rate_limit_per_minute,
            log_filter: config.log_filter,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_mixed_case_protocol_when_parsed_then_expected_variant_is_returned() {
        let protocol = "HtTp".parse::<Protocol>();

        assert_eq!(protocol, Ok(Protocol::Http));
    }

    #[test]
    fn given_toml_configuration_when_parsed_then_values_are_loaded() {
        let config = AppConfig::from_toml(
            r#"
                default_protocol = "udp"
                http_addr = "127.0.0.1:8080"
                peer_timeout = 90
                auth_token = "secret"
            "#,
        )
        .expect("configuration should parse");

        assert_eq!(config.default_protocol, Protocol::Udp);
        assert_eq!(config.http_addr, "127.0.0.1:8080".parse().unwrap());
        assert_eq!(config.peer_timeout, Duration::from_secs(90));
        assert_eq!(config.auth_token.as_deref(), Some("secret"));
        assert_eq!(config.udp_addr, "[::]:6969".parse().unwrap());
    }
}
