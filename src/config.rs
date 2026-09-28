use std::{
    env, fs, net::SocketAddr, num::NonZeroUsize, path::Path, path::PathBuf, str::FromStr,
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
use axum::http::HeaderName;
use clap::ValueEnum;
use ipnet::IpNet;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::blacklist::Blacklist;

#[derive(Debug, Error, PartialEq)]
#[error("expected http, udp, or both; got {value}")]
pub struct ProtocolParseError {
    value: String,
}

#[derive(Debug, Error, PartialEq)]
#[error("expected sqlite or memory; got {value}")]
pub struct PersistenceModeParseError {
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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PersistenceMode {
    Sqlite,
    Memory,
}

impl FromStr for PersistenceMode {
    type Err = PersistenceModeParseError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "sqlite" => Ok(Self::Sqlite),
            "memory" => Ok(Self::Memory),
            _ => Err(PersistenceModeParseError {
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
    pub admin_addr: Option<SocketAddr>,
    pub udp_addr: SocketAddr,
    pub persistence: PersistenceMode,
    pub database_path: PathBuf,
    pub announce_interval: u32,
    pub peer_timeout: Duration,
    pub persistence_interval: Duration,
    pub rate_limit_per_minute: u32,
    pub max_concurrent_http_requests: usize,
    pub trusted_proxy_cidrs: Vec<IpNet>,
    pub client_ip_header: HeaderName,
    pub log_filter: String,
    pub blacklist: Blacklist,
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
        let mut config: AppConfig = config.try_into()?;
        config.blacklist = Blacklist::from_file(&path.with_file_name("blacklist.txt"))?;
        Ok(config)
    }

    #[cfg(test)]
    fn from_toml(contents: &str) -> Result<Self> {
        let config: FileConfig = toml::from_str(contents)?;
        config.try_into()
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileConfig {
    default_protocol: Protocol,
    enable_http_scrape: bool,
    enable_udp_scrape: bool,
    http_addr: SocketAddr,
    admin_addr: Option<SocketAddr>,
    udp_addr: SocketAddr,
    persistence: PersistenceMode,
    database_path: PathBuf,
    announce_interval: u32,
    peer_timeout: u64,
    persistence_interval: u64,
    rate_limit_per_minute: u32,
    max_concurrent_http_requests: NonZeroUsize,
    trusted_proxy_cidrs: Vec<IpNet>,
    client_ip_header: String,
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
            admin_addr: None,
            udp_addr: "0.0.0.0:6969"
                .parse()
                .expect("default UDP address is valid"),
            persistence: PersistenceMode::Sqlite,
            database_path: PathBuf::from("hive.db"),
            announce_interval: 1800,
            peer_timeout: 3600,
            persistence_interval: 30,
            rate_limit_per_minute: 120,
            max_concurrent_http_requests: NonZeroUsize::new(128)
                .expect("default HTTP concurrency limit is nonzero"),
            trusted_proxy_cidrs: Vec::new(),
            client_ip_header: "cf-connecting-ip".to_owned(),
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
        override_parsed!("HIVE_PERSISTENCE", persistence);
        override_parsed!("HIVE_ANNOUNCE_INTERVAL", announce_interval);
        override_parsed!("HIVE_PEER_TIMEOUT", peer_timeout);
        override_parsed!("HIVE_PERSISTENCE_INTERVAL", persistence_interval);
        override_parsed!("HIVE_RATE_LIMIT_PER_MINUTE", rate_limit_per_minute);
        override_parsed!(
            "HIVE_MAX_CONCURRENT_HTTP_REQUESTS",
            max_concurrent_http_requests
        );

        if let Some(raw) = value("HIVE_TRUSTED_PROXY_CIDRS")? {
            self.trusted_proxy_cidrs = parse_comma_separated("HIVE_TRUSTED_PROXY_CIDRS", &raw)?;
        }
        if let Some(raw) = value("HIVE_CLIENT_IP_HEADER")? {
            self.client_ip_header = raw;
        }
        if let Some(raw) = value("HIVE_ADMIN_ADDR")? {
            self.admin_addr = if raw.is_empty() {
                None
            } else {
                Some(parse_environment_value("HIVE_ADMIN_ADDR", &raw)?)
            };
        }
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

fn parse_comma_separated<T>(name: &str, value: &str) -> Result<Vec<T>>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(|item| parse_environment_value(name, item))
        .collect()
}

impl TryFrom<FileConfig> for AppConfig {
    type Error = anyhow::Error;

    fn try_from(config: FileConfig) -> Result<Self> {
        let client_ip_header = HeaderName::from_str(&config.client_ip_header)
            .with_context(|| format!("invalid client_ip_header {:?}", config.client_ip_header))?;
        Ok(Self {
            default_protocol: config.default_protocol,
            enable_http_scrape: config.enable_http_scrape,
            enable_udp_scrape: config.enable_udp_scrape,
            http_addr: config.http_addr,
            admin_addr: config.admin_addr,
            udp_addr: config.udp_addr,
            persistence: config.persistence,
            database_path: config.database_path,
            announce_interval: config.announce_interval,
            peer_timeout: Duration::from_secs(config.peer_timeout),
            persistence_interval: Duration::from_secs(config.persistence_interval),
            rate_limit_per_minute: config.rate_limit_per_minute,
            max_concurrent_http_requests: config.max_concurrent_http_requests.get(),
            trusted_proxy_cidrs: config.trusted_proxy_cidrs,
            client_ip_header,
            log_filter: config.log_filter,
            blacklist: Blacklist::default(),
        })
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
                persistence = "memory"
                http_addr = "127.0.0.1:8080"
                admin_addr = "127.0.0.1:8081"
                peer_timeout = 90
                max_concurrent_http_requests = 64
                trusted_proxy_cidrs = ["127.0.0.1/32", "172.20.0.0/16"]
                client_ip_header = "cf-connecting-ip"
            "#,
        )
        .expect("configuration should parse");

        assert_eq!(config.default_protocol, Protocol::Udp);
        assert_eq!(config.persistence, PersistenceMode::Memory);
        assert_eq!(config.http_addr, "127.0.0.1:8080".parse().unwrap());
        assert_eq!(config.admin_addr, Some("127.0.0.1:8081".parse().unwrap()));
        assert_eq!(config.peer_timeout, Duration::from_secs(90));
        assert_eq!(config.udp_addr, "0.0.0.0:6969".parse().unwrap());
        assert_eq!(config.max_concurrent_http_requests, 64);
        assert_eq!(
            config.trusted_proxy_cidrs,
            vec![
                "127.0.0.1/32".parse().unwrap(),
                "172.20.0.0/16".parse().unwrap()
            ]
        );
        assert_eq!(config.client_ip_header, "cf-connecting-ip");
    }

    #[test]
    fn given_default_configuration_when_loaded_then_recommended_http_concurrency_is_used() {
        let config = AppConfig::from_toml("").expect("default configuration should parse");

        assert_eq!(config.max_concurrent_http_requests, 128);
        assert_eq!(config.persistence, PersistenceMode::Sqlite);
        assert_eq!(config.admin_addr, None);
    }

    #[test]
    fn given_configuration_file_when_loaded_then_sibling_blacklist_is_used() {
        let directory = tempfile::tempdir().expect("temporary directory should be created");
        let config_path = directory.path().join("custom.toml");
        fs::write(&config_path, "").expect("configuration should be written");
        fs::write(
            directory.path().join("blacklist.txt"),
            "0303030303030303030303030303030303030303\n192.0.2.3\n",
        )
        .expect("blacklist should be written");

        let config = AppConfig::from_file(config_path).expect("configuration should load");

        assert!(config.blacklist.contains_info_hash(&[3; 20]));
        assert!(config.blacklist.contains_ip(&"192.0.2.3".parse().unwrap()));
    }

    #[test]
    fn given_zero_http_concurrency_in_toml_when_loaded_then_configuration_is_rejected() {
        let result = AppConfig::from_toml("max_concurrent_http_requests = 0");

        assert!(result.is_err());
    }

    #[test]
    fn given_environment_values_when_applied_then_they_override_file_configuration() {
        let mut config = FileConfig::default();
        let values = std::collections::HashMap::from([
            ("HIVE_PROTOCOL", "http"),
            ("HIVE_ENABLE_HTTP_SCRAPE", "false"),
            ("HIVE_ENABLE_UDP_SCRAPE", "false"),
            ("HIVE_HTTP_ADDR", "127.0.0.1:8080"),
            ("HIVE_ADMIN_ADDR", "127.0.0.1:8081"),
            ("HIVE_UDP_ADDR", "127.0.0.1:6968"),
            ("HIVE_PERSISTENCE", "memory"),
            ("HIVE_DATABASE_PATH", "/data/custom.db"),
            ("HIVE_ANNOUNCE_INTERVAL", "900"),
            ("HIVE_PEER_TIMEOUT", "1800"),
            ("HIVE_PERSISTENCE_INTERVAL", "60"),
            ("HIVE_RATE_LIMIT_PER_MINUTE", "500"),
            ("HIVE_MAX_CONCURRENT_HTTP_REQUESTS", "256"),
            ("HIVE_TRUSTED_PROXY_CIDRS", "127.0.0.1/32, 172.20.0.0/16"),
            ("HIVE_CLIENT_IP_HEADER", "CF-Connecting-IP"),
            ("HIVE_LOG_FILTER", "hive_tracker=warn"),
        ]);

        config
            .apply_environment(|name| Ok(values.get(name).map(ToString::to_string)))
            .expect("environment values should apply");
        let config = AppConfig::try_from(config).expect("configuration should be valid");

        assert_eq!(config.default_protocol, Protocol::Http);
        assert!(!config.enable_http_scrape);
        assert!(!config.enable_udp_scrape);
        assert_eq!(config.http_addr, "127.0.0.1:8080".parse().unwrap());
        assert_eq!(config.admin_addr, Some("127.0.0.1:8081".parse().unwrap()));
        assert_eq!(config.udp_addr, "127.0.0.1:6968".parse().unwrap());
        assert_eq!(config.persistence, PersistenceMode::Memory);
        assert_eq!(config.database_path, PathBuf::from("/data/custom.db"));
        assert_eq!(config.announce_interval, 900);
        assert_eq!(config.peer_timeout, Duration::from_secs(1800));
        assert_eq!(config.persistence_interval, Duration::from_secs(60));
        assert_eq!(config.rate_limit_per_minute, 500);
        assert_eq!(config.max_concurrent_http_requests, 256);
        assert_eq!(config.trusted_proxy_cidrs.len(), 2);
        assert_eq!(config.client_ip_header, "cf-connecting-ip");
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

    #[test]
    fn given_zero_http_concurrency_when_applied_then_configuration_is_rejected() {
        let mut config = FileConfig::default();

        let error = config
            .apply_environment(|name| {
                Ok((name == "HIVE_MAX_CONCURRENT_HTTP_REQUESTS").then(|| "0".to_owned()))
            })
            .expect_err("zero concurrency should fail");

        assert!(error
            .to_string()
            .contains("HIVE_MAX_CONCURRENT_HTTP_REQUESTS"));
    }

    #[test]
    fn given_invalid_client_ip_header_when_loaded_then_configuration_is_rejected() {
        let result = AppConfig::from_toml(r#"client_ip_header = "not a header""#);

        assert!(result
            .expect_err("invalid header should fail")
            .to_string()
            .contains("client_ip_header"));
    }

    #[test]
    fn given_invalid_proxy_cidr_in_environment_when_applied_then_variable_is_named() {
        let mut config = FileConfig::default();

        let error = config
            .apply_environment(|name| {
                Ok((name == "HIVE_TRUSTED_PROXY_CIDRS").then(|| "not-a-cidr".to_owned()))
            })
            .expect_err("invalid CIDR should fail");

        assert!(error.to_string().contains("HIVE_TRUSTED_PROXY_CIDRS"));
    }
}
