use std::{
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
    metrics::TrafficSnapshot,
    state::{Peer, PeerId, TrackerState, TrackerSummary},
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
            "CREATE TABLE IF NOT EXISTS swarms (info_hash BLOB PRIMARY KEY, downloaded INTEGER NOT NULL)",
        )
        .execute(&pool)
        .await?;
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
        Ok(Self {
            pool,
            traffic_checkpoint: Arc::new(Mutex::new(TrafficCheckpoint::default())),
        })
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
        Ok(())
    }

    pub async fn dashboard_history(
        &self,
        days: u64,
        bucket_seconds: u64,
    ) -> Result<DashboardHistory> {
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
        Ok(DashboardHistory {
            metrics,
            traffic,
            total_ingress_bytes,
            total_egress_bytes,
        })
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
        let snapshot = state.snapshot();
        let mut transaction = self.pool.begin().await?;
        sqlx::query("DELETE FROM peers")
            .execute(&mut *transaction)
            .await?;
        sqlx::query("DELETE FROM swarms")
            .execute(&mut *transaction)
            .await?;

        for (info_hash, peers, downloaded) in snapshot {
            sqlx::query("INSERT INTO swarms (info_hash, downloaded) VALUES (?, ?)")
                .bind(info_hash.as_slice())
                .bind(sqlite_integer(downloaded))
                .execute(&mut *transaction)
                .await?;
            for peer in peers {
                sqlx::query(
                    "INSERT INTO peers (info_hash, peer_id, ip, port, bytes_left, last_seen)
                     VALUES (?, ?, ?, ?, ?, ?)",
                )
                .bind(info_hash.as_slice())
                .bind(peer.peer_id.as_slice())
                .bind(peer.ip.to_string())
                .bind(i64::from(peer.port))
                .bind(sqlite_integer(peer.left))
                .bind(sqlite_integer(peer.last_seen))
                .execute(&mut *transaction)
                .await?;
            }
        }
        transaction.commit().await?;
        Ok(())
    }

    pub async fn is_healthy(&self) -> bool {
        sqlx::query_scalar::<_, i64>("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .is_ok()
    }
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
        database
            .record_dashboard_snapshot(
                summary,
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
        assert_eq!(history.metrics[0].peers, 5);
        assert_eq!(history.metrics[0].completed, 7);
        assert_eq!(history.total_ingress_bytes, 150);
        assert_eq!(history.total_egress_bytes, 260);
        database.pool.close().await;
        drop(database);
        std::fs::remove_file(database_path).expect("temporary database should be removed");
    }
}
