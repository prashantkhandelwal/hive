use std::{net::IpAddr, path::Path, str::FromStr};

use serde::Serialize;
use sqlx::{sqlite::SqliteConnectOptions, Row, SqlitePool};
use thiserror::Error;

use crate::state::{Peer, PeerId, TrackerState};

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
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DailyTorrentCount {
    pub day: String,
    pub torrents: usize,
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
            "CREATE TABLE IF NOT EXISTS daily_torrent_counts (
                day TEXT PRIMARY KEY,
                torrent_count INTEGER NOT NULL
            )",
        )
        .execute(&pool)
        .await?;
        Ok(Self { pool })
    }

    pub async fn record_daily_torrent_count(&self, torrent_count: usize) -> Result<()> {
        sqlx::query(
            "INSERT INTO daily_torrent_counts (day, torrent_count)
             VALUES (date('now'), ?)
             ON CONFLICT(day) DO UPDATE SET torrent_count = excluded.torrent_count",
        )
        .bind(sqlite_integer(torrent_count as u64))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn daily_torrent_counts(
        &self,
        current_torrent_count: usize,
    ) -> Result<Vec<DailyTorrentCount>> {
        let mut counts = sqlx::query(
            "SELECT day, torrent_count
             FROM daily_torrent_counts
             ORDER BY day DESC
             LIMIT 30",
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| {
            Ok(DailyTorrentCount {
                day: row.try_get("day")?,
                torrents: positive_i64(row.try_get("torrent_count")?) as usize,
            })
        })
        .collect::<Result<Vec<_>>>()?;
        counts.reverse();
        let current_day: String = sqlx::query_scalar("SELECT date('now')")
            .fetch_one(&self.pool)
            .await?;
        if let Some(today) = counts.last_mut().filter(|count| count.day == current_day) {
            today.torrents = current_torrent_count;
        } else {
            counts.push(DailyTorrentCount {
                day: current_day,
                torrents: current_torrent_count,
            });
        }
        Ok(counts)
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
        self.record_daily_torrent_count(state.swarm_count()).await?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn given_multiple_daily_updates_when_loaded_then_latest_count_is_returned() {
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

        database
            .record_daily_torrent_count(2)
            .await
            .expect("first count should persist");
        database
            .record_daily_torrent_count(5)
            .await
            .expect("latest count should replace the earlier count");

        let counts = database
            .daily_torrent_counts(5)
            .await
            .expect("daily counts should load");
        assert_eq!(counts.len(), 1);
        assert_eq!(counts[0].torrents, 5);
        database.pool.close().await;
        drop(database);
        std::fs::remove_file(database_path).expect("temporary database should be removed");
    }
}
