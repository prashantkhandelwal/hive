use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct TrackerSummary {
    pub peers: usize,
    pub seeders: usize,
    pub leechers: usize,
    pub torrents: usize,
    pub completed: u64,
}

#[derive(Clone, Debug)]
pub struct StateChange {
    pub info_hash: InfoHash,
    pub peers: Vec<Peer>,
    pub downloaded: Option<u64>,
    generation: u64,
}

#[derive(Default)]
pub struct TrackerState {
    swarms: DashMap<InfoHash, HashMap<PeerId, Peer>>,
    completed: DashMap<InfoHash, u64>,
    dirty: DashMap<InfoHash, u64>,
    peer_count: AtomicUsize,
    seeder_count: AtomicUsize,
    leecher_count: AtomicUsize,
    torrent_count: AtomicUsize,
    swarm_count: AtomicUsize,
    completed_count: AtomicU64,
    revision: AtomicU64,
}

impl TrackerState {
    pub fn restore(&self, info_hash: InfoHash, peer: Peer) {
        if let Entry::Vacant(entry) = self.completed.entry(info_hash) {
            entry.insert(0);
            self.torrent_count.fetch_add(1, Ordering::Relaxed);
        }
        let previous = match self.swarms.entry(info_hash) {
            Entry::Occupied(mut entry) => entry.get_mut().insert(peer.peer_id, peer.clone()),
            Entry::Vacant(entry) => {
                self.swarm_count.fetch_add(1, Ordering::Relaxed);
                entry.insert(HashMap::from([(peer.peer_id, peer.clone())]));
                None
            }
        };
        self.update_peer_counters(previous.as_ref(), Some(&peer));
    }

    pub fn set_downloaded(&self, info_hash: InfoHash, downloaded: u64) {
        let previous = match self.completed.entry(info_hash) {
            Entry::Occupied(mut entry) => Some(entry.insert(downloaded)),
            Entry::Vacant(entry) => {
                entry.insert(downloaded);
                self.torrent_count.fetch_add(1, Ordering::Relaxed);
                None
            }
        };
        update_atomic_u64(&self.completed_count, previous.unwrap_or(0), downloaded);
    }

    pub fn announce(&self, info_hash: InfoHash, peer: Peer, event: AnnounceEvent) -> SwarmStats {
        if event != AnnounceEvent::Stopped {
            if let Entry::Vacant(entry) = self.completed.entry(info_hash) {
                entry.insert(0);
                self.torrent_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        let mut should_record_completion = false;
        if event == AnnounceEvent::Stopped {
            if let Entry::Occupied(mut entry) = self.swarms.entry(info_hash) {
                let removed = entry.get_mut().remove(&peer.peer_id);
                self.update_peer_counters(removed.as_ref(), None);
                if entry.get().is_empty() {
                    entry.remove();
                    decrement_atomic_usize(&self.swarm_count, 1);
                    if let Entry::Occupied(entry) = self.completed.entry(info_hash) {
                        if *entry.get() == 0 {
                            entry.remove();
                            decrement_atomic_usize(&self.torrent_count, 1);
                        }
                    }
                }
            }
        } else {
            let previous = match self.swarms.entry(info_hash) {
                Entry::Occupied(mut entry) => entry.get_mut().insert(peer.peer_id, peer.clone()),
                Entry::Vacant(entry) => {
                    self.swarm_count.fetch_add(1, Ordering::Relaxed);
                    entry.insert(HashMap::from([(peer.peer_id, peer.clone())]));
                    None
                }
            };
            should_record_completion = event == AnnounceEvent::Completed
                && previous
                    .as_ref()
                    .map(|existing| existing.left != 0)
                    .unwrap_or(true);
            self.update_peer_counters(previous.as_ref(), Some(&peer));
        }

        if should_record_completion {
            let mut count = self.completed.entry(info_hash).or_default();
            *count = count.saturating_add(1);
            self.completed_count
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    Some(value.saturating_add(1))
                })
                .ok();
        }

        self.mark_changed(info_hash);
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
        self.torrent_count.load(Ordering::Relaxed)
    }

    pub fn remove_stale(&self, max_age: Duration) {
        let cutoff = unix_timestamp().saturating_sub(max_age.as_secs());
        let mut removed_peers = 0;
        let mut removed_seeders = 0;
        let mut removed_leechers = 0;
        let mut changed = HashSet::new();
        self.swarms.retain(|info_hash, swarm| {
            let before = swarm.len();
            let seeders_before = swarm.values().filter(|peer| peer.left == 0).count();
            swarm.retain(|_, peer| peer.last_seen >= cutoff);
            let removed = before - swarm.len();
            if removed > 0 {
                let seeders_after = swarm.values().filter(|peer| peer.left == 0).count();
                removed_peers += removed;
                removed_seeders += seeders_before - seeders_after;
                removed_leechers += removed - (seeders_before - seeders_after);
                changed.insert(*info_hash);
            }
            if swarm.is_empty() {
                decrement_atomic_usize(&self.swarm_count, 1);
            }
            !swarm.is_empty()
        });
        decrement_atomic_usize(&self.peer_count, removed_peers);
        decrement_atomic_usize(&self.seeder_count, removed_seeders);
        decrement_atomic_usize(&self.leecher_count, removed_leechers);
        self.completed.retain(|info_hash, count| {
            let retain = *count > 0 || self.swarms.contains_key(info_hash);
            if !retain {
                decrement_atomic_usize(&self.torrent_count, 1);
                changed.insert(*info_hash);
            }
            retain
        });
        if !changed.is_empty() {
            for info_hash in changed {
                self.mark_changed(info_hash);
            }
        }
    }

    pub fn swarm_count(&self) -> usize {
        self.swarm_count.load(Ordering::Relaxed)
    }

    pub fn peer_count(&self) -> usize {
        self.peer_count.load(Ordering::Relaxed)
    }

    pub fn summary(&self) -> TrackerSummary {
        TrackerSummary {
            peers: self.peer_count.load(Ordering::Relaxed),
            seeders: self.seeder_count.load(Ordering::Relaxed),
            leechers: self.leecher_count.load(Ordering::Relaxed),
            torrents: self.torrent_count(),
            completed: self.completed_count.load(Ordering::Relaxed),
        }
    }

    pub fn revision(&self) -> u64 {
        self.revision.load(Ordering::Relaxed)
    }

    pub fn drain_changes(&self) -> Vec<StateChange> {
        self.dirty
            .iter()
            .map(|entry| {
                let info_hash = *entry.key();
                let downloaded = self.completed.get(&info_hash).map(|value| *value);
                let peers = self
                    .swarms
                    .get(&info_hash)
                    .map(|swarm| swarm.values().cloned().collect())
                    .unwrap_or_default();
                StateChange {
                    info_hash,
                    peers,
                    downloaded,
                    generation: *entry.value(),
                }
            })
            .collect()
    }

    pub fn acknowledge_changes(&self, changes: &[StateChange]) {
        for change in changes {
            if let Entry::Occupied(entry) = self.dirty.entry(change.info_hash) {
                if *entry.get() == change.generation {
                    entry.remove();
                }
            }
        }
    }

    fn mark_changed(&self, info_hash: InfoHash) {
        let generation = self.revision.fetch_add(1, Ordering::Relaxed) + 1;
        self.dirty.insert(info_hash, generation);
    }

    fn update_peer_counters(&self, previous: Option<&Peer>, current: Option<&Peer>) {
        match (previous, current) {
            (None, Some(peer)) => {
                self.peer_count.fetch_add(1, Ordering::Relaxed);
                self.peer_kind_counter(peer).fetch_add(1, Ordering::Relaxed);
            }
            (Some(peer), None) => {
                decrement_atomic_usize(&self.peer_count, 1);
                decrement_atomic_usize(self.peer_kind_counter(peer), 1);
            }
            (Some(previous), Some(current)) if (previous.left == 0) != (current.left == 0) => {
                decrement_atomic_usize(self.peer_kind_counter(previous), 1);
                self.peer_kind_counter(current)
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    fn peer_kind_counter(&self, peer: &Peer) -> &AtomicUsize {
        if peer.left == 0 {
            &self.seeder_count
        } else {
            &self.leecher_count
        }
    }
}

fn decrement_atomic_usize(value: &AtomicUsize, amount: usize) {
    value
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_sub(amount))
        })
        .ok();
}

fn update_atomic_u64(value: &AtomicU64, previous: u64, current: u64) {
    if current >= previous {
        value
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |total| {
                Some(total.saturating_add(current - previous))
            })
            .ok();
    } else {
        value
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |total| {
                Some(total.saturating_sub(previous - current))
            })
            .ok();
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

    #[test]
    fn given_mixed_swarm_when_summarized_then_all_dashboard_counts_are_returned() {
        let state = TrackerState::default();
        state.announce([1; 20], peer(1, 0), AnnounceEvent::Completed);
        state.announce([1; 20], peer(2, 50), AnnounceEvent::Started);
        state.announce([2; 20], peer(3, 0), AnnounceEvent::Started);

        assert_eq!(
            state.summary(),
            TrackerSummary {
                peers: 3,
                seeders: 2,
                leechers: 1,
                torrents: 2,
                completed: 1,
            }
        );
    }

    #[test]
    fn given_mutation_after_snapshot_when_old_changes_are_acknowledged_then_state_remains_dirty() {
        let state = TrackerState::default();
        let info_hash = [12; 20];
        state.announce(info_hash, peer(1, 100), AnnounceEvent::Started);
        let first_changes = state.drain_changes();

        state.announce(info_hash, peer(1, 50), AnnounceEvent::Update);
        let latest_changes = state.drain_changes();
        state.acknowledge_changes(&first_changes);

        assert_eq!(state.drain_changes().len(), 1);
        state.acknowledge_changes(&latest_changes);
        assert!(state.drain_changes().is_empty());
    }
}
