use std::{collections::HashSet, fs, net::IpAddr, path::Path};

use anyhow::{anyhow, Context, Result};

use crate::state::InfoHash;

#[derive(Clone, Debug, Default)]
pub struct Blacklist {
    info_hashes: HashSet<InfoHash>,
    ips: HashSet<IpAddr>,
}

impl Blacklist {
    pub fn from_file(path: &Path) -> Result<Self> {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("failed to read blacklist from {}", path.display()))?;
        Self::from_contents(&contents)
            .with_context(|| format!("failed to parse blacklist from {}", path.display()))
    }

    pub fn from_entries(info_hashes: Vec<String>, ips: Vec<IpAddr>) -> Result<Self> {
        let info_hashes = info_hashes
            .into_iter()
            .map(|value| parse_info_hash(&value))
            .collect::<Result<HashSet<_>>>()?;
        Ok(Self {
            info_hashes,
            ips: ips.into_iter().collect(),
        })
    }

    pub fn contains_info_hash(&self, info_hash: &InfoHash) -> bool {
        self.info_hashes.contains(info_hash)
    }

    pub fn contains_ip(&self, ip: &IpAddr) -> bool {
        self.ips.contains(ip)
    }

    pub fn info_hash_count(&self) -> usize {
        self.info_hashes.len()
    }

    pub fn ip_count(&self) -> usize {
        self.ips.len()
    }

    fn from_contents(contents: &str) -> Result<Self> {
        let mut blacklist = Self::default();
        for (index, line) in contents.lines().enumerate() {
            let entry = line.split_once('#').map_or(line, |(value, _)| value).trim();
            if entry.is_empty() {
                continue;
            }
            if let Ok(ip) = entry.parse() {
                blacklist.ips.insert(ip);
                continue;
            }
            if entry.len() != 40 {
                return Err(anyhow!(
                    "invalid entry on line {}: expected an IP address or 40-character hexadecimal info hash",
                    index + 1
                ));
            }
            let info_hash = parse_info_hash(entry)
                .with_context(|| format!("invalid entry on line {}", index + 1))?;
            blacklist.info_hashes.insert(info_hash);
        }
        Ok(blacklist)
    }
}

fn parse_info_hash(value: &str) -> Result<InfoHash> {
    let encoded = value.as_bytes();
    if encoded.len() != 40 {
        return Err(anyhow!(
            "invalid blacklist info hash {value:?}: expected 40 hexadecimal characters"
        ));
    }
    let mut info_hash = [0_u8; 20];
    for (index, byte) in info_hash.iter_mut().enumerate() {
        let offset = index * 2;
        let high = hex_digit(encoded[offset]).ok_or_else(|| invalid_info_hash(value))?;
        let low = hex_digit(encoded[offset + 1]).ok_or_else(|| invalid_info_hash(value))?;
        *byte = (high << 4) | low;
    }
    Ok(info_hash)
}

fn hex_digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn invalid_info_hash(value: &str) -> anyhow::Error {
    anyhow!("invalid blacklist info hash {value:?}: expected 40 hexadecimal characters")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_mixed_case_hash_and_ip_when_loaded_then_both_are_blocked() {
        let blacklist = Blacklist::from_contents(
            "# blocked entries\n\
             Aabbccddeeff0011223344556677889900Aabbcc # torrent\n\
             127.0.0.1 # client\n",
        )
        .expect("blacklist should parse");

        assert!(blacklist.contains_info_hash(
            &parse_info_hash("aabbccddeeff0011223344556677889900aabbcc").unwrap()
        ));
        assert!(blacklist.contains_ip(&"127.0.0.1".parse().unwrap()));
    }

    #[test]
    fn given_invalid_hash_when_loaded_then_configuration_is_rejected() {
        let malformed = Blacklist::from_contents("not-a-hash");
        let non_ascii = Blacklist::from_contents(&"é".repeat(20));

        assert!(malformed.is_err());
        assert!(non_ascii.is_err());
    }
}
