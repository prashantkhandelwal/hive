use std::{
    collections::HashMap,
    net::IpAddr,
    path::Path,
    str::FromStr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use sqlx::{sqlite::SqliteConnectOptions, Row, SqlitePool};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::{
    client,
    metrics::TrafficSnapshot,
    state::{InfoHash, Peer, PeerId, TrackerState, TrackerSummary},
};

pub type Result<T> = std::result::Result<T, PersistenceError>;

#[derive(Debug, Error)]
pub enum PersistenceError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("invalid persisted peer address: {0}")]
    InvalidAddress(#[from] std::net::AddrParseError),
    #[error("invalid persisted {field} length: expected 20 bytes, got {actual}")]
    InvalidIdentifier { field: &'static str, actual: usize },
}

#[derive(Clone)]
pub struct Persistence {
    pool: SqlitePool,
    traffic_checkpoint: Arc<Mutex<TrafficCheckpoint>>,
    history_cache: Arc<Mutex<HashMap<(u64, u64), DashboardHistory>>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct MetricPoint {
    pub timestamp: u64,
    pub peers: usize,
    pub seeders: usize,
    pub leechers: usize,
    pub torrents: usize,
    pub completed: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TrafficPoint {
    pub day: String,
    pub ingress_bytes: u64,
    pub egress_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ClientStatistic {
    pub client_name: String,
    pub peer_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TorrentStatistic {
    pub info_hash: InfoHash,
    pub peers: usize,
    pub seeders: usize,
    pub leechers: usize,
    pub downloaded: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DashboardHistory {
    pub metrics: Vec<MetricPoint>,
    pub traffic: Vec<TrafficPoint>,
    pub total_ingress_bytes: u64,
    pub total_egress_bytes: u64,
}

#[derive(Default)]
struct TrafficCheckpoint {
    ingress_bytes: u64,
    egress_bytes: u64,
}

impl Persistence {
    pub async fn open(path: &Path) -> Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(options).await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS swarms (
                info_hash BLOB PRIMARY KEY,
                peers INTEGER NOT NULL DEFAULT 0,
                seeders INTEGER NOT NULL DEFAULT 0,
                leechers INTEGER NOT NULL DEFAULT 0,
                downloaded INTEGER NOT NULL
            )",
        )
        .execute(&pool)
        .await?;
        ensure_swarm_stat_columns(&pool).await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS peers (
                info_hash BLOB NOT NULL,
                peer_id BLOB NOT NULL,
                ip TEXT NOT NULL,
                port INTEGER NOT NULL,
                bytes_left INTEGER NOT NULL,
                last_seen INTEGER NOT NULL,
                PRIMARY KEY (info_hash, peer_id)
            )",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS metric_snapshots (
                recorded_at INTEGER PRIMARY KEY,
                peers INTEGER NOT NULL,
                seeders INTEGER NOT NULL,
                leechers INTEGER NOT NULL,
                torrents INTEGER NOT NULL,
                completed INTEGER NOT NULL,
                uptime_seconds INTEGER NOT NULL
            )",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS daily_traffic (
                day TEXT PRIMARY KEY,
                ingress_bytes INTEGER NOT NULL,
                egress_bytes INTEGER NOT NULL
            )",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS client_statistics (
                client_name TEXT PRIMARY KEY,
                peer_count INTEGER NOT NULL
            )",
        )
        .execute(&pool)
        .await?;
        Ok(Self {
            pool,
            traffic_checkpoint: Arc::new(Mutex::new(TrafficCheckpoint::default())),
            history_cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub async fn record_client_announce(&self, peer_id: PeerId) -> Result<()> {
        let client_name = client::detect(peer_id);
        sqlx::query(
            "INSERT INTO client_statistics (client_name, peer_count)
             VALUES (?, 1)
             ON CONFLICT(client_name) DO UPDATE SET
                peer_count = CASE
                    WHEN peer_count < 9223372036854775807 THEN peer_count + 1
                    ELSE peer_count
                END",
        )
        .bind(client_name)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn client_statistics(&self) -> Result<Vec<ClientStatistic>> {
        sqlx::query(
            "SELECT client_name, peer_count
             FROM client_statistics
             ORDER BY peer_count DESC, client_name",
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| {
            Ok(ClientStatistic {
                client_name: row.try_get("client_name")?,
                peer_count: positive_i64(row.try_get("peer_count")?),
            })
        })
        .collect()
    }

    pub async fn torrent_statistics(&self) -> Result<Vec<TorrentStatistic>> {
        sqlx::query(
            "SELECT info_hash, peers, seeders, leechers, downloaded
             FROM swarms
             ORDER BY info_hash",
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| {
            Ok(TorrentStatistic {
                info_hash: identifier(row.try_get("info_hash")?, "info_hash")?,
                peers: positive_i64(row.try_get("peers")?) as usize,
                seeders: positive_i64(row.try_get("seeders")?) as usize,
                leechers: positive_i64(row.try_get("leechers")?) as usize,
                downloaded: positive_i64(row.try_get("downloaded")?),
            })
        })
        .collect()
    }

    pub async fn record_dashboard_snapshot(
        &self,
        summary: TrackerSummary,
        uptime_seconds: u64,
        traffic: TrafficSnapshot,
    ) -> Result<()> {
        let mut checkpoint = self.traffic_checkpoint.lock().await;
        let ingress_delta = traffic
            .total_ingress_bytes
            .saturating_sub(checkpoint.ingress_bytes);
        let egress_delta = traffic
            .total_egress_bytes
            .saturating_sub(checkpoint.egress_bytes);
        let recorded_at = unix_timestamp() / 60 * 60;
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO metric_snapshots (
                recorded_at, peers, seeders, leechers, torrents, completed, uptime_seconds
             ) VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(recorded_at) DO UPDATE SET
                peers = excluded.peers,
                seeders = excluded.seeders,
                leechers = excluded.leechers,
                torrents = excluded.torrents,
                completed = excluded.completed,
                uptime_seconds = excluded.uptime_seconds",
        )
        .bind(sqlite_integer(recorded_at))
        .bind(sqlite_integer(summary.peers as u64))
        .bind(sqlite_integer(summary.seeders as u64))
        .bind(sqlite_integer(summary.leechers as u64))
        .bind(sqlite_integer(summary.torrents as u64))
        .bind(sqlite_integer(summary.completed))
        .bind(sqlite_integer(uptime_seconds))
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO daily_traffic (day, ingress_bytes, egress_bytes)
             VALUES (date('now'), ?, ?)
             ON CONFLICT(day) DO UPDATE SET
                ingress_bytes = ingress_bytes + excluded.ingress_bytes,
                egress_bytes = egress_bytes + excluded.egress_bytes",
        )
        .bind(sqlite_integer(ingress_delta))
        .bind(sqlite_integer(egress_delta))
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        checkpoint.ingress_bytes = traffic.total_ingress_bytes;
        checkpoint.egress_bytes = traffic.total_egress_bytes;
        self.history_cache.lock().await.clear();
        Ok(())
    }

    pub async fn dashboard_history(
        &self,
        days: u64,
        bucket_seconds: u64,
    ) -> Result<DashboardHistory> {
        let cache_key = (days, bucket_seconds);
        let mut cache = self.history_cache.lock().await;
        if let Some(history) = cache.get(&cache_key) {
            return Ok(history.clone());
        }
        let cutoff = unix_timestamp().saturating_sub(days * 24 * 60 * 60);
        let metrics = sqlx::query(
            "SELECT snapshot.recorded_at, snapshot.peers, snapshot.seeders, snapshot.leechers,
                    snapshot.torrents, snapshot.completed
             FROM metric_snapshots snapshot
             INNER JOIN (
                SELECT MAX(recorded_at) AS recorded_at
                FROM metric_snapshots
                WHERE recorded_at >= ?
                GROUP BY recorded_at / ?
             ) buckets ON buckets.recorded_at = snapshot.recorded_at
             ORDER BY snapshot.recorded_at",
        )
        .bind(sqlite_integer(cutoff))
        .bind(sqlite_integer(bucket_seconds))
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| {
            Ok(MetricPoint {
                timestamp: positive_i64(row.try_get("recorded_at")?),
                peers: positive_i64(row.try_get("peers")?) as usize,
                seeders: positive_i64(row.try_get("seeders")?) as usize,
                leechers: positive_i64(row.try_get("leechers")?) as usize,
                torrents: positive_i64(row.try_get("torrents")?) as usize,
                completed: positive_i64(row.try_get("completed")?),
            })
        })
        .collect::<Result<Vec<_>>>()?;
        let traffic = sqlx::query(
            "SELECT day, ingress_bytes, egress_bytes
             FROM daily_traffic
             WHERE day >= date('now', ?)
             ORDER BY day",
        )
        .bind(format!("-{} days", days.saturating_sub(1)))
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| {
            Ok(TrafficPoint {
                day: row.try_get("day")?,
                ingress_bytes: positive_i64(row.try_get("ingress_bytes")?),
                egress_bytes: positive_i64(row.try_get("egress_bytes")?),
            })
        })
        .collect::<Result<Vec<_>>>()?;
        let (total_ingress_bytes, total_egress_bytes) =
            traffic.iter().fold((0_u64, 0_u64), |totals, point| {
                (
                    totals.0.saturating_add(point.ingress_bytes),
                    totals.1.saturating_add(point.egress_bytes),
                )
            });
        let history = DashboardHistory {
            metrics,
            traffic,
            total_ingress_bytes,
            total_egress_bytes,
        };
        cache.insert(cache_key, history.clone());
        Ok(history)
    }

    pub async fn load(&self, state: &TrackerState) -> Result<()> {
        for row in sqlx::query("SELECT info_hash, downloaded FROM swarms")
            .fetch_all(&self.pool)
            .await?
        {
            let info_hash = identifier(row.try_get("info_hash")?, "info_hash")?;
            let downloaded: i64 = row.try_get("downloaded")?;
            state.set_downloaded(info_hash, downloaded.max(0) as u64);
        }

        for row in
            sqlx::query("SELECT info_hash, peer_id, ip, port, bytes_left, last_seen FROM peers")
                .fetch_all(&self.pool)
                .await?
        {
            let info_hash = identifier(row.try_get("info_hash")?, "info_hash")?;
            let peer_id: PeerId = identifier(row.try_get("peer_id")?, "peer_id")?;
            let ip_text: String = row.try_get("ip")?;
            state.restore(
                info_hash,
                Peer {
                    peer_id,
                    ip: IpAddr::from_str(&ip_text)?,
                    port: positive_i64(row.try_get("port")?) as u16,
                    left: positive_i64(row.try_get("bytes_left")?),
                    last_seen: positive_i64(row.try_get("last_seen")?),
                },
            );
        }
        Ok(())
    }

    pub async fn save(&self, state: &TrackerState) -> Result<()> {
        let changes = state.drain_changes();
        if changes.is_empty() {
            return Ok(());
        }
        let result: Result<()> = async {
            let mut transaction = self.pool.begin().await?;
            for change in &changes {
                sqlx::query("DELETE FROM peers WHERE info_hash = ?")
                    .bind(change.info_hash.as_slice())
                    .execute(&mut *transaction)
                    .await?;
                if let Some(downloaded) = change.downloaded {
                    let peers = change.peers.len();
                    let seeders = change.peers.iter().filter(|peer| peer.left == 0).count();
                    let leechers = peers - seeders;
                    sqlx::query(
                        "INSERT INTO swarms (
                            info_hash, peers, seeders, leechers, downloaded
                         ) VALUES (?, ?, ?, ?, ?)
                         ON CONFLICT(info_hash) DO UPDATE SET
                            peers = excluded.peers,
                            seeders = excluded.seeders,
                            leechers = excluded.leechers,
                            downloaded = excluded.downloaded",
                    )
                    .bind(change.info_hash.as_slice())
                    .bind(sqlite_integer(peers as u64))
                    .bind(sqlite_integer(seeders as u64))
                    .bind(sqlite_integer(leechers as u64))
                    .bind(sqlite_integer(downloaded))
                    .execute(&mut *transaction)
                    .await?;
                    for peer in &change.peers {
                        sqlx::query(
                            "INSERT INTO peers (
                                info_hash, peer_id, ip, port, bytes_left, last_seen
                             ) VALUES (?, ?, ?, ?, ?, ?)",
                        )
                        .bind(change.info_hash.as_slice())
                        .bind(peer.peer_id.as_slice())
                        .bind(peer.ip.to_string())
                        .bind(i64::from(peer.port))
                        .bind(sqlite_integer(peer.left))
                        .bind(sqlite_integer(peer.last_seen))
                        .execute(&mut *transaction)
                        .await?;
                    }
                } else {
                    sqlx::query("DELETE FROM swarms WHERE info_hash = ?")
                        .bind(change.info_hash.as_slice())
                        .execute(&mut *transaction)
                        .await?;
                }
            }
            transaction.commit().await?;
            Ok(())
        }
        .await;
        if result.is_ok() {
            state.acknowledge_changes(&changes);
        }
        result
    }

    pub async fn is_healthy(&self) -> bool {
        sqlx::query_scalar::<_, i64>("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .is_ok()
    }
}

async fn ensure_swarm_stat_columns(pool: &SqlitePool) -> Result<()> {
    let columns = sqlx::query("PRAGMA table_info(swarms)")
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|row| row.try_get::<String, _>("name"))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    for (column, definition) in [
        ("peers", "INTEGER NOT NULL DEFAULT 0"),
        ("seeders", "INTEGER NOT NULL DEFAULT 0"),
        ("leechers", "INTEGER NOT NULL DEFAULT 0"),
    ] {
        if !columns.iter().any(|existing| existing == column) {
            sqlx::query(&format!(
                "ALTER TABLE swarms ADD COLUMN {column} {definition}"
            ))
            .execute(pool)
            .await?;
        }
    }
    Ok(())
}

fn identifier(bytes: Vec<u8>, field: &'static str) -> Result<[u8; 20]> {
    let actual = bytes.len();
    bytes
        .try_into()
        .map_err(|_| PersistenceError::InvalidIdentifier { field, actual })
}

fn positive_i64(value: i64) -> u64 {
    value.max(0) as u64
}

fn sqlite_integer(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn given_dashboard_updates_when_loaded_then_metrics_and_traffic_are_persisted() {
        let database_path = std::env::temp_dir().join(format!(
            "hive-persistence-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let database = Persistence::open(&database_path)
            .await
            .expect("temporary database should open");

        let summary = TrackerSummary {
            peers: 5,
            seeders: 2,
            leechers: 3,
            torrents: 4,
            completed: 7,
        };
        let traffic = TrafficSnapshot {
            total_ingress_bytes: 100,
            total_egress_bytes: 200,
            ..TrafficSnapshot::default()
        };
        database
            .record_dashboard_snapshot(summary, 10, traffic)
            .await
            .expect("first snapshot should persist");
        let initial_history = database
            .dashboard_history(1, 60)
            .await
            .expect("initial dashboard history should load");
        assert_eq!(initial_history.metrics[0].peers, 5);
        database
            .record_dashboard_snapshot(
                TrackerSummary {
                    peers: 6,
                    ..summary
                },
                20,
                TrafficSnapshot {
                    total_ingress_bytes: 150,
                    total_egress_bytes: 260,
                    ..TrafficSnapshot::default()
                },
            )
            .await
            .expect("second snapshot should persist");

        let history = database
            .dashboard_history(1, 60)
            .await
            .expect("dashboard history should load");
        assert_eq!(history.metrics.len(), 1);
        assert_eq!(history.metrics[0].peers, 6);
        assert_eq!(history.metrics[0].completed, 7);
        assert_eq!(history.total_ingress_bytes, 150);
        assert_eq!(history.total_egress_bytes, 260);
        database.pool.close().await;
        drop(database);
        std::fs::remove_file(database_path).expect("temporary database should be removed");
    }

    #[tokio::test]
    async fn given_changed_torrents_when_saved_then_only_current_state_is_restored() {
        use crate::state::{unix_timestamp, AnnounceEvent};

        let database_path = std::env::temp_dir().join(format!(
            "hive-incremental-persistence-{}-{}.db",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let database = Persistence::open(&database_path)
            .await
            .expect("temporary database should open");
        let state = TrackerState::default();
        let first_hash = [1; 20];
        let second_hash = [2; 20];
        let peer = |peer_id| Peer {
            peer_id: [peer_id; 20],
            ip: "127.0.0.1".parse().expect("test address should parse"),
            port: 6881,
            left: 10,
            last_seen: unix_timestamp(),
        };
        state.announce(first_hash, peer(1), AnnounceEvent::Started);
        state.announce(second_hash, peer(2), AnnounceEvent::Started);
        database
            .save(&state)
            .await
            .expect("initial changes should persist");

        state.announce(first_hash, peer(1), AnnounceEvent::Stopped);
        database
            .save(&state)
            .await
            .expect("targeted deletion should persist");

        let restored = TrackerState::default();
        database
            .load(&restored)
            .await
            .expect("persisted state should load");
        assert_eq!(restored.stats(&first_hash), Default::default());
        assert_eq!(restored.peer_count(), 1);
        assert_eq!(restored.torrent_count(), 1);
        assert_eq!(restored.peers(&second_hash, &[0; 20], 10).len(), 1);

        database.pool.close().await;
        drop(database);
        std::fs::remove_file(database_path).expect("temporary database should be removed");
    }

    #[tokio::test]
    async fn given_swarm_changes_when_saved_then_all_per_torrent_counts_are_persisted() {
        use crate::state::{unix_timestamp, AnnounceEvent};

        let database_path = std::env::temp_dir().join(format!(
            "hive-torrent-statistics-{}-{}.db",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let database = Persistence::open(&database_path)
            .await
            .expect("temporary database should open");
        let state = TrackerState::default();
        let info_hash = [3; 20];
        let peer = |peer_id, left| Peer {
            peer_id: [peer_id; 20],
            ip: "127.0.0.1".parse().expect("test address should parse"),
            port: 6881,
            left,
            last_seen: unix_timestamp(),
        };
        state.announce(info_hash, peer(1, 100), AnnounceEvent::Started);
        state.announce(info_hash, peer(2, 0), AnnounceEvent::Started);
        state.announce(info_hash, peer(3, 50), AnnounceEvent::Started);
        state.announce(info_hash, peer(1, 0), AnnounceEvent::Completed);

        database
            .save(&state)
            .await
            .expect("swarm statistics should persist");

        assert_eq!(
            database
                .torrent_statistics()
                .await
                .expect("torrent statistics should load"),
            vec![TorrentStatistic {
                info_hash,
                peers: 3,
                seeders: 2,
                leechers: 1,
                downloaded: 1,
            }]
        );

        database.pool.close().await;
        drop(database);
        std::fs::remove_file(database_path).expect("temporary database should be removed");
    }

    #[tokio::test]
    async fn given_legacy_database_when_opened_then_swarm_stat_columns_are_added() {
        let database_path = std::env::temp_dir().join(format!(
            "hive-legacy-statistics-{}-{}.db",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let options = SqliteConnectOptions::new()
            .filename(&database_path)
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(options)
            .await
            .expect("legacy database should open");
        sqlx::query(
            "CREATE TABLE swarms (
                info_hash BLOB PRIMARY KEY,
                downloaded INTEGER NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .expect("legacy swarm table should be created");
        sqlx::query("INSERT INTO swarms (info_hash, downloaded) VALUES (?, ?)")
            .bind([9_u8; 20].as_slice())
            .bind(4_i64)
            .execute(&pool)
            .await
            .expect("legacy swarm should be inserted");
        pool.close().await;

        let database = Persistence::open(&database_path)
            .await
            .expect("legacy database should migrate");

        assert_eq!(
            database
                .torrent_statistics()
                .await
                .expect("migrated statistics should load"),
            vec![TorrentStatistic {
                info_hash: [9; 20],
                peers: 0,
                seeders: 0,
                leechers: 0,
                downloaded: 4,
            }]
        );

        database.pool.close().await;
        drop(database);
        std::fs::remove_file(database_path).expect("temporary database should be removed");
    }

    #[tokio::test]
    async fn given_repeated_client_announces_when_recorded_then_peer_count_is_incremented() {
        let database_path = std::env::temp_dir().join(format!(
            "hive-client-statistics-{}-{}.db",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let database = Persistence::open(&database_path)
            .await
            .expect("temporary database should open");
        let mut qbittorrent_peer_id = [b'-'; 20];
        qbittorrent_peer_id[..8].copy_from_slice(b"-qB4500-");

        database
            .record_client_announce(qbittorrent_peer_id)
            .await
            .expect("first client announce should persist");
        database
            .record_client_announce(qbittorrent_peer_id)
            .await
            .expect("second client announce should persist");
        database
            .record_client_announce([0xff; 20])
            .await
            .expect("unknown client announce should persist");

        assert_eq!(
            database
                .client_statistics()
                .await
                .expect("client statistics should load"),
            vec![
                ClientStatistic {
                    client_name: "qBittorrent".to_owned(),
                    peer_count: 2,
                },
                ClientStatistic {
                    client_name: "Unknown".to_owned(),
                    peer_count: 1,
                },
            ]
        );

        database.pool.close().await;
        drop(database);
        std::fs::remove_file(database_path).expect("temporary database should be removed");
    }
}
