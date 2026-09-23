use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use prometheus::{Encoder, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder};
use serde::Serialize;

const TRAFFIC_WINDOW_SECONDS: u64 = 60;

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct ProtocolTraffic {
    pub ingress_bytes: u64,
    pub egress_bytes: u64,
    pub requests_per_minute: u64,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct TrafficSnapshot {
    pub torrent_http: ProtocolTraffic,
    pub web_http: ProtocolTraffic,
    pub http: ProtocolTraffic,
    pub udp: ProtocolTraffic,
    pub total_ingress_bytes: u64,
    pub total_egress_bytes: u64,
}

impl TrafficSnapshot {
    pub fn requests_per_second(&self) -> f64 {
        (self.http.requests_per_minute + self.udp.requests_per_minute) as f64
            / TRAFFIC_WINDOW_SECONDS as f64
    }
}

#[derive(Clone, Copy, Debug)]
struct TrafficBucket {
    second: u64,
    protocol: &'static str,
    ingress_bytes: u64,
    egress_bytes: u64,
    requests: u64,
}

#[derive(Default)]
struct TrafficWindow {
    buckets: VecDeque<TrafficBucket>,
}

#[derive(Clone)]
pub struct AppMetrics {
    registry: Registry,
    requests: IntCounterVec,
    announce_events: IntCounterVec,
    traffic_bytes: IntCounterVec,
    requests_per_minute: IntGaugeVec,
    active_peers: IntGauge,
    active_swarms: IntGauge,
    traffic_window: Arc<Mutex<TrafficWindow>>,
}

impl AppMetrics {
    pub fn new() -> Result<Self, prometheus::Error> {
        let registry = Registry::new();
        let requests = IntCounterVec::new(
            Opts::new(
                "hive_requests_total",
                "Tracker requests by protocol and action",
            ),
            &["protocol", "action", "result"],
        )?;
        let announce_events = IntCounterVec::new(
            Opts::new("hive_announce_events_total", "Announce events received"),
            &["protocol", "event"],
        )?;
        let traffic_bytes = IntCounterVec::new(
            Opts::new(
                "hive_traffic_bytes_total",
                "Application traffic bytes by protocol and direction",
            ),
            &["protocol", "direction"],
        )?;
        let requests_per_minute = IntGaugeVec::new(
            Opts::new(
                "hive_requests_per_minute",
                "Requests handled during the rolling 60-second window",
            ),
            &["protocol"],
        )?;
        let active_peers = IntGauge::new("hive_active_peers", "Peers currently in memory")?;
        let active_swarms = IntGauge::new("hive_active_swarms", "Swarms currently in memory")?;

        registry.register(Box::new(requests.clone()))?;
        registry.register(Box::new(announce_events.clone()))?;
        registry.register(Box::new(traffic_bytes.clone()))?;
        registry.register(Box::new(requests_per_minute.clone()))?;
        registry.register(Box::new(active_peers.clone()))?;
        registry.register(Box::new(active_swarms.clone()))?;

        Ok(Self {
            registry,
            requests,
            announce_events,
            traffic_bytes,
            requests_per_minute,
            active_peers,
            active_swarms,
            traffic_window: Arc::new(Mutex::new(TrafficWindow::default())),
        })
    }

    pub fn request(&self, protocol: &str, action: &str, result: &str) {
        self.requests
            .with_label_values(&[protocol, action, result])
            .inc();
    }

    pub fn announce(&self, protocol: &str, event: &str) {
        self.announce_events
            .with_label_values(&[protocol, event])
            .inc();
    }

    pub fn record_traffic(
        &self,
        protocol: &'static str,
        ingress_bytes: usize,
        egress_bytes: usize,
    ) {
        self.record_traffic_at(
            protocol,
            ingress_bytes as u64,
            egress_bytes as u64,
            unix_timestamp(),
        );
    }

    pub fn traffic_snapshot(&self) -> TrafficSnapshot {
        self.traffic_snapshot_at(unix_timestamp())
    }

    pub fn set_population(&self, peers: usize, swarms: usize) {
        self.active_peers.set(peers as i64);
        self.active_swarms.set(swarms as i64);
    }

    pub fn encode(&self) -> Result<Vec<u8>, prometheus::Error> {
        self.traffic_snapshot();
        let families = self.registry.gather();
        let mut output = Vec::new();
        TextEncoder::new().encode(&families, &mut output)?;
        Ok(output)
    }

    fn record_traffic_at(
        &self,
        protocol: &'static str,
        ingress_bytes: u64,
        egress_bytes: u64,
        second: u64,
    ) {
        self.traffic_bytes
            .with_label_values(&[protocol, "ingress"])
            .inc_by(ingress_bytes);
        self.traffic_bytes
            .with_label_values(&[protocol, "egress"])
            .inc_by(egress_bytes);

        let Ok(mut window) = self.traffic_window.lock() else {
            return;
        };
        prune_window(&mut window, second);
        if let Some(bucket) = window
            .buckets
            .iter_mut()
            .find(|bucket| bucket.second == second && bucket.protocol == protocol)
        {
            bucket.ingress_bytes += ingress_bytes;
            bucket.egress_bytes += egress_bytes;
            bucket.requests += 1;
        } else {
            window.buckets.push_back(TrafficBucket {
                second,
                protocol,
                ingress_bytes,
                egress_bytes,
                requests: 1,
            });
        }
    }

    fn traffic_snapshot_at(&self, second: u64) -> TrafficSnapshot {
        let mut snapshot = TrafficSnapshot {
            total_ingress_bytes: self
                .traffic_bytes
                .with_label_values(&["torrent_http", "ingress"])
                .get()
                + self
                    .traffic_bytes
                .with_label_values(&["web_http", "ingress"])
                .get()
                + self
                    .traffic_bytes
                    .with_label_values(&["udp", "ingress"])
                    .get(),
            total_egress_bytes: self
                .traffic_bytes
                .with_label_values(&["torrent_http", "egress"])
                .get()
                + self
                    .traffic_bytes
                .with_label_values(&["web_http", "egress"])
                .get()
                + self
                    .traffic_bytes
                    .with_label_values(&["udp", "egress"])
                    .get(),
            ..TrafficSnapshot::default()
        };
        if let Ok(mut window) = self.traffic_window.lock() {
            prune_window(&mut window, second);
            for bucket in &window.buckets {
                let traffic = match bucket.protocol {
                    "torrent_http" => &mut snapshot.torrent_http,
                    "web_http" => &mut snapshot.web_http,
                    _ => &mut snapshot.udp,
                };
                traffic.ingress_bytes += bucket.ingress_bytes;
                traffic.egress_bytes += bucket.egress_bytes;
                traffic.requests_per_minute += bucket.requests;
            }
        }
        snapshot.http = ProtocolTraffic {
            ingress_bytes: snapshot.torrent_http.ingress_bytes + snapshot.web_http.ingress_bytes,
            egress_bytes: snapshot.torrent_http.egress_bytes + snapshot.web_http.egress_bytes,
            requests_per_minute: snapshot.torrent_http.requests_per_minute
                + snapshot.web_http.requests_per_minute,
        };
        self.requests_per_minute
            .with_label_values(&["torrent_http"])
            .set(snapshot.torrent_http.requests_per_minute as i64);
        self.requests_per_minute
            .with_label_values(&["web_http"])
            .set(snapshot.web_http.requests_per_minute as i64);
        self.requests_per_minute
            .with_label_values(&["udp"])
            .set(snapshot.udp.requests_per_minute as i64);
        snapshot
    }
}

fn prune_window(window: &mut TrafficWindow, second: u64) {
    let cutoff = second.saturating_sub(TRAFFIC_WINDOW_SECONDS - 1);
    window.buckets.retain(|bucket| bucket.second >= cutoff);
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

    #[test]
    fn given_traffic_outside_window_when_snapshotted_then_only_recent_requests_remain() {
        let metrics = AppMetrics::new().expect("metrics should initialize");
        metrics.record_traffic_at("web_http", 100, 200, 1_000);
        metrics.record_traffic_at("udp", 10, 20, 1_060);

        let snapshot = metrics.traffic_snapshot_at(1_060);

        assert_eq!(snapshot.http.requests_per_minute, 0);
        assert_eq!(snapshot.udp.requests_per_minute, 1);
        assert_eq!(snapshot.total_ingress_bytes, 110);
    }

    #[test]
    fn given_torrent_web_and_udp_traffic_when_snapshotted_then_each_is_isolated() {
        let metrics = AppMetrics::new().expect("metrics should initialize");
        metrics.record_traffic_at("torrent_http", 100, 200, 1_000);
        metrics.record_traffic_at("web_http", 10, 20, 1_000);
        metrics.record_traffic_at("udp", 1, 2, 1_000);

        let snapshot = metrics.traffic_snapshot_at(1_000);

        assert_eq!(snapshot.torrent_http.ingress_bytes, 100);
        assert_eq!(snapshot.web_http.ingress_bytes, 10);
        assert_eq!(snapshot.http.ingress_bytes, 110);
        assert_eq!(snapshot.udp.ingress_bytes, 1);
        assert_eq!(snapshot.total_ingress_bytes, 111);
    }

    #[test]
    fn given_requests_in_rolling_window_when_rate_requested_then_returns_requests_per_second() {
        let metrics = AppMetrics::new().expect("metrics should initialize");
        for _ in 0..120 {
            metrics.record_traffic_at("torrent_http", 1, 1, 1_000);
        }

        assert_eq!(
            metrics.traffic_snapshot_at(1_000).requests_per_second(),
            2.0
        );
    }
}
