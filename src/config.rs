use std::{env, fs, net::SocketAddr, path::Path, path::PathBuf, str::FromStr, time::Duration};

use anyhow::{anyhow, Context, Result};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
#[error("expected http, udp, or both; got {value}")]
pub struct ProtocolParseError {
    value: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ValueEnum)]
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
    pub enable_http_scrape: bool,
    pub enable_udp_scrape: bool,
    pub http_addr: SocketAddr,
    pub udp_addr: SocketAddr,
    pub database_path: PathBuf,
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
        let mut config: FileConfig = toml::from_str(&contents)
            .with_context(|| format!("failed to parse configuration from {}", path.display()))?;
        config
            .apply_environment(environment_value)
            .context("failed to apply environment configuration")?;
        Ok(config.into())
    }

    #[cfg(test)]
    fn from_toml(contents: &str) -> Result<Self> {
        let config: FileConfig = toml::from_str(contents)?;
        Ok(config.into())
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileConfig {
    default_protocol: Protocol,
    enable_http_scrape: bool,
    enable_udp_scrape: bool,
    http_addr: SocketAddr,
    udp_addr: SocketAddr,
    database_path: PathBuf,
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
            enable_http_scrape: true,
            enable_udp_scrape: true,
            http_addr: "0.0.0.0:3000"
                .parse()
                .expect("default HTTP address is valid"),
            udp_addr: "0.0.0.0:6969"
                .parse()
                .expect("default UDP address is valid"),
            database_path: PathBuf::from("hive.db"),
            announce_interval: 1800,
            peer_timeout: 3600,
            persistence_interval: 30,
            rate_limit_per_minute: 120,
            log_filter: "hive_tracker=info".to_owned(),
        }
    }
}

impl FileConfig {
    fn apply_environment(
        &mut self,
        mut value: impl FnMut(&str) -> Result<Option<String>>,
    ) -> Result<()> {
        macro_rules! override_parsed {
            ($name:literal, $field:ident) => {
                if let Some(raw) = value($name)? {
                    self.$field = parse_environment_value($name, &raw)?;
                }
            };
        }

        override_parsed!("HIVE_PROTOCOL", default_protocol);
        override_parsed!("HIVE_ENABLE_HTTP_SCRAPE", enable_http_scrape);
        override_parsed!("HIVE_ENABLE_UDP_SCRAPE", enable_udp_scrape);
        override_parsed!("HIVE_HTTP_ADDR", http_addr);
        override_parsed!("HIVE_UDP_ADDR", udp_addr);
        override_parsed!("HIVE_ANNOUNCE_INTERVAL", announce_interval);
        override_parsed!("HIVE_PEER_TIMEOUT", peer_timeout);
        override_parsed!("HIVE_PERSISTENCE_INTERVAL", persistence_interval);
        override_parsed!("HIVE_RATE_LIMIT_PER_MINUTE", rate_limit_per_minute);

        if let Some(raw) = value("HIVE_DATABASE_PATH")? {
            if raw.is_empty() {
                return Err(anyhow!("HIVE_DATABASE_PATH cannot be empty"));
            }
            self.database_path = PathBuf::from(raw);
        }
        if let Some(raw) = value("HIVE_LOG_FILTER")? {
            self.log_filter = raw;
        }
        Ok(())
    }
}

fn environment_value(name: &str) -> Result<Option<String>> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(anyhow!("{name} contains non-Unicode characters")),
    }
}

fn parse_environment_value<T>(name: &str, value: &str) -> Result<T>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|error| anyhow!("invalid {name} value {value:?}: {error}"))
}

impl From<FileConfig> for AppConfig {
    fn from(config: FileConfig) -> Self {
        Self {
            default_protocol: config.default_protocol,
            enable_http_scrape: config.enable_http_scrape,
            enable_udp_scrape: config.enable_udp_scrape,
            http_addr: config.http_addr,
            udp_addr: config.udp_addr,
            database_path: config.database_path,
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
            "#,
        )
        .expect("configuration should parse");

        assert_eq!(config.default_protocol, Protocol::Udp);
        assert_eq!(config.http_addr, "127.0.0.1:8080".parse().unwrap());
        assert_eq!(config.peer_timeout, Duration::from_secs(90));
        assert_eq!(config.udp_addr, "0.0.0.0:6969".parse().unwrap());
    }

    #[test]
    fn given_environment_values_when_applied_then_they_override_file_configuration() {
        let mut config = FileConfig::default();
        let values = std::collections::HashMap::from([
            ("HIVE_PROTOCOL", "http"),
            ("HIVE_ENABLE_HTTP_SCRAPE", "false"),
            ("HIVE_ENABLE_UDP_SCRAPE", "false"),
            ("HIVE_HTTP_ADDR", "127.0.0.1:8080"),
            ("HIVE_UDP_ADDR", "127.0.0.1:6968"),
            ("HIVE_DATABASE_PATH", "/data/custom.db"),
            ("HIVE_ANNOUNCE_INTERVAL", "900"),
            ("HIVE_PEER_TIMEOUT", "1800"),
            ("HIVE_PERSISTENCE_INTERVAL", "60"),
            ("HIVE_RATE_LIMIT_PER_MINUTE", "500"),
            ("HIVE_LOG_FILTER", "hive_tracker=warn"),
        ]);

        config
            .apply_environment(|name| Ok(values.get(name).map(ToString::to_string)))
            .expect("environment values should apply");
        let config = AppConfig::from(config);

        assert_eq!(config.default_protocol, Protocol::Http);
        assert!(!config.enable_http_scrape);
        assert!(!config.enable_udp_scrape);
        assert_eq!(config.http_addr, "127.0.0.1:8080".parse().unwrap());
        assert_eq!(config.udp_addr, "127.0.0.1:6968".parse().unwrap());
        assert_eq!(config.database_path, PathBuf::from("/data/custom.db"));
        assert_eq!(config.announce_interval, 900);
        assert_eq!(config.peer_timeout, Duration::from_secs(1800));
        assert_eq!(config.persistence_interval, Duration::from_secs(60));
        assert_eq!(config.rate_limit_per_minute, 500);
        assert_eq!(config.log_filter, "hive_tracker=warn");
    }

    #[test]
    fn given_invalid_environment_value_when_applied_then_error_names_the_variable() {
        let mut config = FileConfig::default();

        let error = config
            .apply_environment(|name| Ok((name == "HIVE_PEER_TIMEOUT").then(|| "soon".to_owned())))
            .expect_err("invalid environment value should fail");

        assert!(error.to_string().contains("HIVE_PEER_TIMEOUT"));
    }
}
