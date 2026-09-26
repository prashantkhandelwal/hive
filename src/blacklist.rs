use std::{
    collections::HashSet,
    fs,
    net::IpAddr,
    path::Path,
    sync::{Arc, RwLock},
};

use anyhow::{anyhow, Context, Result};

use crate::state::InfoHash;

#[derive(Clone, Debug, Default)]
pub struct Blacklist {
    entries: Arc<RwLock<BlacklistEntries>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct BlacklistEntries {
    info_hashes: HashSet<InfoHash>,
    ips: HashSet<IpAddr>,
}

impl Blacklist {
    pub fn from_file(path: &Path) -> Result<Self> {
        Ok(Self {
            entries: Arc::new(RwLock::new(read_entries(path)?)),
        })
    }

    pub fn from_entries(info_hashes: Vec<String>, ips: Vec<IpAddr>) -> Result<Self> {
        let info_hashes = info_hashes
            .into_iter()
            .map(|value| parse_info_hash(&value))
            .collect::<Result<HashSet<_>>>()?;
        Ok(Self {
            entries: Arc::new(RwLock::new(BlacklistEntries {
                info_hashes,
                ips: ips.into_iter().collect(),
            })),
        })
    }

    pub fn contains_info_hash(&self, info_hash: &InfoHash) -> bool {
        self.entries
            .read()
            .expect("blacklist lock should not be poisoned")
            .info_hashes
            .contains(info_hash)
    }

    pub fn contains_ip(&self, ip: &IpAddr) -> bool {
        self.entries
            .read()
            .expect("blacklist lock should not be poisoned")
            .ips
            .contains(ip)
    }

    pub fn info_hash_count(&self) -> usize {
        self.entries
            .read()
            .expect("blacklist lock should not be poisoned")
            .info_hashes
            .len()
    }

    pub fn ip_count(&self) -> usize {
        self.entries
            .read()
            .expect("blacklist lock should not be poisoned")
            .ips
            .len()
    }

    pub fn reload_from_file(&self, path: &Path) -> Result<bool> {
        let replacement = read_entries(path)?;
        let mut entries = self
            .entries
            .write()
            .expect("blacklist lock should not be poisoned");
        if *entries == replacement {
            return Ok(false);
        }
        *entries = replacement;
        Ok(true)
    }

    #[cfg(test)]
    fn from_contents(contents: &str) -> Result<Self> {
        Ok(Self {
            entries: Arc::new(RwLock::new(parse_contents(contents)?)),
        })
    }
}

fn read_contents(path: &Path) -> Result<String> {
    fs::read_to_string(path)
        .with_context(|| format!("failed to read blacklist from {}", path.display()))
}

fn read_entries(path: &Path) -> Result<BlacklistEntries> {
    parse_contents(&read_contents(path)?)
        .with_context(|| format!("failed to parse blacklist from {}", path.display()))
}

fn parse_contents(contents: &str) -> Result<BlacklistEntries> {
    let mut entries = BlacklistEntries::default();
    for (index, line) in contents.lines().enumerate() {
        let entry = line.split_once('#').map_or(line, |(value, _)| value).trim();
        if entry.is_empty() {
            continue;
        }
        if let Ok(ip) = entry.parse() {
            entries.ips.insert(ip);
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
        entries.info_hashes.insert(info_hash);
    }
    Ok(entries)
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
    use tempfile::tempdir;

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

    #[test]
    fn given_shared_blacklist_when_file_changes_then_clones_receive_reload() {
        let directory = tempdir().expect("temporary directory should be created");
        let path = directory.path().join("blacklist.txt");
        fs::write(&path, "192.0.2.1\n").expect("blacklist should be written");
        let blacklist = Blacklist::from_file(&path).expect("blacklist should load");
        let shared = blacklist.clone();

        fs::write(&path, "192.0.2.2\n").expect("blacklist should be updated");
        assert!(blacklist
            .reload_from_file(&path)
            .expect("blacklist should reload"));

        assert!(!shared.contains_ip(&"192.0.2.1".parse().unwrap()));
        assert!(shared.contains_ip(&"192.0.2.2".parse().unwrap()));
    }

    #[test]
    fn given_invalid_update_when_reloaded_then_last_valid_blacklist_is_retained() {
        let directory = tempdir().expect("temporary directory should be created");
        let path = directory.path().join("blacklist.txt");
        fs::write(&path, "192.0.2.1\n").expect("blacklist should be written");
        let blacklist = Blacklist::from_file(&path).expect("blacklist should load");

        fs::write(&path, "invalid\n").expect("blacklist should be updated");

        assert!(blacklist.reload_from_file(&path).is_err());
        assert!(blacklist.contains_ip(&"192.0.2.1".parse().unwrap()));
    }
}
