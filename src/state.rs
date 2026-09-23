use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use dashmap::mapref::entry::Entry;
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
        let should_record_completion = event == AnnounceEvent::Completed
            && self
                .swarms
                .get(&info_hash)
                .and_then(|swarm| swarm.get(&peer.peer_id).map(|existing| existing.left != 0))
                .unwrap_or(true);

        if event != AnnounceEvent::Stopped {
            self.completed.entry(info_hash).or_default();
        }

        if event == AnnounceEvent::Stopped {
            if let Entry::Occupied(mut entry) = self.swarms.entry(info_hash) {
                entry.get_mut().remove(&peer.peer_id);
                if entry.get().is_empty() {
                    entry.remove();
                    if self
                        .completed
                        .get(&info_hash)
                        .is_some_and(|count| *count == 0)
                    {
                        self.completed.remove(&info_hash);
                    }
                }
            }
        } else {
            self.swarms
                .entry(info_hash)
                .or_default()
                .insert(peer.peer_id, peer);
        }

        if should_record_completion {
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

    pub fn info_hashes(&self) -> Vec<InfoHash> {
        let mut hashes: HashSet<_> = self.swarms.iter().map(|swarm| *swarm.key()).collect();
        hashes.extend(self.completed.iter().map(|entry| *entry.key()));
        hashes.into_iter().collect()
    }

    pub fn torrent_count(&self) -> usize {
        self.info_hashes().len()
    }

    pub fn remove_stale(&self, max_age: Duration) {
        let cutoff = unix_timestamp().saturating_sub(max_age.as_secs());
        self.swarms.retain(|_, swarm| {
            swarm.retain(|_, peer| peer.last_seen >= cutoff);
            !swarm.is_empty()
        });
        self.completed
            .retain(|info_hash, count| *count > 0 || self.swarms.contains_key(info_hash));
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

        state.announce(info_hash, peer(1, 0), AnnounceEvent::Stopped);

        assert_eq!(state.peer_count(), 0);
        assert_eq!(state.swarm_count(), 0);
        assert_eq!(state.info_hashes(), vec![info_hash]);
        assert_eq!(state.torrent_count(), 1);
        assert_eq!(state.stats(&info_hash).downloaded, 1);
    }

    #[test]
    fn given_incomplete_torrent_when_last_peer_stops_then_torrent_is_removed() {
        let state = TrackerState::default();
        let info_hash = [8; 20];

        state.announce(info_hash, peer(1, 100), AnnounceEvent::Started);
        state.announce(info_hash, peer(1, 100), AnnounceEvent::Stopped);

        assert_eq!(state.peer_count(), 0);
        assert_eq!(state.swarm_count(), 0);
        assert_eq!(state.torrent_count(), 0);
    }

    #[test]
    fn given_stale_torrent_when_cleaned_up_then_torrent_is_removed() {
        let state = TrackerState::default();
        let info_hash = [9; 20];
        let mut stale_peer = peer(1, 100);
        stale_peer.last_seen = 0;

        state.announce(info_hash, stale_peer, AnnounceEvent::Started);
        state.remove_stale(Duration::from_secs(1));

        assert_eq!(state.peer_count(), 0);
        assert_eq!(state.swarm_count(), 0);
        assert_eq!(state.torrent_count(), 0);
    }

    #[test]
    fn given_repeated_completed_event_when_announced_then_download_is_counted_once() {
        let state = TrackerState::default();
        let info_hash = [10; 20];

        state.announce(info_hash, peer(1, 100), AnnounceEvent::Started);
        state.announce(info_hash, peer(1, 0), AnnounceEvent::Completed);
        state.announce(info_hash, peer(1, 0), AnnounceEvent::Completed);

        assert_eq!(state.stats(&info_hash).downloaded, 1);
    }

    #[test]
    fn given_completed_torrent_when_peers_expire_then_download_history_is_retained() {
        let state = TrackerState::default();
        let info_hash = [11; 20];
        let mut stale_peer = peer(1, 0);
        stale_peer.last_seen = 0;

        state.announce(info_hash, stale_peer, AnnounceEvent::Completed);
        state.remove_stale(Duration::from_secs(1));

        assert_eq!(state.peer_count(), 0);
        assert_eq!(state.stats(&info_hash).downloaded, 1);
        assert_eq!(state.torrent_count(), 1);
    }
}
