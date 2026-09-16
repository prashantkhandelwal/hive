use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

pub type InfoHash = [u8; 20];
pub type PeerId = [u8; 20];

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AnnounceEvent {
    Started,
    Completed,
    Stopped,
    Update,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Peer {
    pub peer_id: PeerId,
    pub ip: IpAddr,
    pub port: u16,
    pub left: u64,
    pub last_seen: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct SwarmStats {
    pub complete: usize,
    pub incomplete: usize,
    pub downloaded: u64,
}

#[derive(Default)]
pub struct TrackerState {
    swarms: DashMap<InfoHash, HashMap<PeerId, Peer>>,
    completed: DashMap<InfoHash, u64>,
}

impl TrackerState {
    pub fn restore(&self, info_hash: InfoHash, peer: Peer) {
        self.swarms
            .entry(info_hash)
            .or_default()
            .insert(peer.peer_id, peer);
    }

    pub fn set_downloaded(&self, info_hash: InfoHash, downloaded: u64) {
        self.completed.insert(info_hash, downloaded);
    }

    pub fn snapshot(&self) -> Vec<(InfoHash, Vec<Peer>, u64)> {
        let mut seen = HashSet::new();
        let mut snapshot: Vec<_> = self
            .swarms
            .iter()
            .map(|swarm| {
                let info_hash = *swarm.key();
                seen.insert(info_hash);
                let peers = swarm.values().cloned().collect();
                let downloaded = self
                    .completed
                    .get(&info_hash)
                    .map(|value| *value)
                    .unwrap_or(0);
                (info_hash, peers, downloaded)
            })
            .collect();
        snapshot.extend(
            self.completed
                .iter()
                .filter(|entry| !seen.contains(entry.key()))
                .map(|entry| (*entry.key(), Vec::new(), *entry.value())),
        );
        snapshot
    }

    pub fn announce(&self, info_hash: InfoHash, peer: Peer, event: AnnounceEvent) -> SwarmStats {
        if event == AnnounceEvent::Stopped {
            if let Some(mut swarm) = self.swarms.get_mut(&info_hash) {
                swarm.remove(&peer.peer_id);
            }
        } else {
            self.swarms
                .entry(info_hash)
                .or_default()
                .insert(peer.peer_id, peer);
        }

        if event == AnnounceEvent::Completed {
            *self.completed.entry(info_hash).or_default() += 1;
        }

        self.stats(&info_hash)
    }

    pub fn stats(&self, info_hash: &InfoHash) -> SwarmStats {
        let (complete, incomplete) = self
            .swarms
            .get(info_hash)
            .map(|swarm| {
                swarm.values().fold((0, 0), |(seeders, leechers), peer| {
                    if peer.left == 0 {
                        (seeders + 1, leechers)
                    } else {
                        (seeders, leechers + 1)
                    }
                })
            })
            .unwrap_or_default();

        SwarmStats {
            complete,
            incomplete,
            downloaded: self
                .completed
                .get(info_hash)
                .map(|value| *value)
                .unwrap_or(0),
        }
    }

    pub fn peers(&self, info_hash: &InfoHash, exclude: &PeerId, limit: usize) -> Vec<Peer> {
        self.swarms
            .get(info_hash)
            .map(|swarm| {
                swarm
                    .values()
                    .filter(|peer| &peer.peer_id != exclude)
                    .take(limit)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn remove_stale(&self, max_age: Duration) {
        let cutoff = unix_timestamp().saturating_sub(max_age.as_secs());
        self.swarms.retain(|_, swarm| {
            swarm.retain(|_, peer| peer.last_seen >= cutoff);
            !swarm.is_empty()
        });
    }

    pub fn swarm_count(&self) -> usize {
        self.swarms.len()
    }

    pub fn peer_count(&self) -> usize {
        self.swarms.iter().map(|swarm| swarm.len()).sum()
    }
}

pub fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(peer_id: u8, left: u64) -> Peer {
        Peer {
            peer_id: [peer_id; 20],
            ip: "127.0.0.1".parse().expect("test address must parse"),
            port: 6881,
            left,
            last_seen: unix_timestamp(),
        }
    }

    #[test]
    fn given_peer_lifecycle_when_announced_then_swarm_counts_are_consistent() {
        let state = TrackerState::default();
        let info_hash = [7; 20];

        state.announce(info_hash, peer(1, 100), AnnounceEvent::Started);
        state.announce(info_hash, peer(1, 0), AnnounceEvent::Completed);
        state.announce(info_hash, peer(2, 50), AnnounceEvent::Started);
        let active = state.stats(&info_hash);
        state.announce(info_hash, peer(2, 50), AnnounceEvent::Stopped);

        assert_eq!(
            active,
            SwarmStats {
                complete: 1,
                incomplete: 1,
                downloaded: 1
            }
        );
        assert_eq!(state.peer_count(), 1);
    }
}
