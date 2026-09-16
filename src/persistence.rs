use std::{net::IpAddr, path::Path, str::FromStr};

use sqlx::{sqlite::SqliteConnectOptions, Row, SqlitePool};
use thiserror::Error;

use crate::state::{InfoHash, Peer, PeerId, TrackerState};

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
        Ok(Self { pool })
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
