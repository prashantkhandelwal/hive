use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use prometheus::{
    Encoder, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder,
};
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
    pub total_requests: u64,
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
struct TrafficCounters {
    torrent_http_requests: IntCounter,
    torrent_http_ingress: IntCounter,
    torrent_http_egress: IntCounter,
    web_http_requests: IntCounter,
    web_http_ingress: IntCounter,
    web_http_egress: IntCounter,
    udp_requests: IntCounter,
    udp_ingress: IntCounter,
    udp_egress: IntCounter,
}

impl TrafficCounters {
    fn select(&self, protocol: &str) -> (&IntCounter, &IntCounter, &IntCounter) {
        match protocol {
            "torrent_http" => (
                &self.torrent_http_requests,
                &self.torrent_http_ingress,
                &self.torrent_http_egress,
            ),
            "web_http" => (
                &self.web_http_requests,
                &self.web_http_ingress,
                &self.web_http_egress,
            ),
            _ => (&self.udp_requests, &self.udp_ingress, &self.udp_egress),
        }
    }
}

#[derive(Clone)]
pub struct AppMetrics {
    registry: Registry,
    requests: IntCounterVec,
    announce_events: IntCounterVec,
    traffic_counters: TrafficCounters,
    requests_per_minute: IntGaugeVec,
    requests_per_second: IntGauge,
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
        let traffic_requests = IntCounterVec::new(
            Opts::new(
                "hive_traffic_requests_total",
                "Application requests handled since process start",
            ),
            &["protocol"],
        )?;
        let requests_per_minute = IntGaugeVec::new(
            Opts::new(
                "hive_requests_per_minute",
                "Requests handled during the rolling 60-second window",
            ),
            &["protocol"],
        )?;
        let requests_per_second = IntGauge::new(
            "hive_requests_per_second",
            "Rounded average requests per second during the rolling 60-second window",
        )?;
        let active_peers = IntGauge::new("hive_active_peers", "Peers currently in memory")?;
        let active_swarms = IntGauge::new("hive_active_swarms", "Swarms currently in memory")?;

        registry.register(Box::new(requests.clone()))?;
        registry.register(Box::new(announce_events.clone()))?;
        registry.register(Box::new(traffic_bytes.clone()))?;
        registry.register(Box::new(traffic_requests.clone()))?;
        registry.register(Box::new(requests_per_minute.clone()))?;
        registry.register(Box::new(requests_per_second.clone()))?;
        registry.register(Box::new(active_peers.clone()))?;
        registry.register(Box::new(active_swarms.clone()))?;
        let traffic_counters = TrafficCounters {
            torrent_http_requests: traffic_requests.with_label_values(&["torrent_http"]),
            torrent_http_ingress: traffic_bytes.with_label_values(&["torrent_http", "ingress"]),
            torrent_http_egress: traffic_bytes.with_label_values(&["torrent_http", "egress"]),
            web_http_requests: traffic_requests.with_label_values(&["web_http"]),
            web_http_ingress: traffic_bytes.with_label_values(&["web_http", "ingress"]),
            web_http_egress: traffic_bytes.with_label_values(&["web_http", "egress"]),
            udp_requests: traffic_requests.with_label_values(&["udp"]),
            udp_ingress: traffic_bytes.with_label_values(&["udp", "ingress"]),
            udp_egress: traffic_bytes.with_label_values(&["udp", "egress"]),
        };

        Ok(Self {
            registry,
            requests,
            announce_events,
            traffic_counters,
            requests_per_minute,
            requests_per_second,
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
        let (request_counter, ingress_counter, egress_counter) =
            self.traffic_counters.select(protocol);
        request_counter.inc();
        ingress_counter.inc_by(ingress_bytes);
        egress_counter.inc_by(egress_bytes);

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
            total_requests: self.traffic_counters.torrent_http_requests.get()
                + self.traffic_counters.web_http_requests.get()
                + self.traffic_counters.udp_requests.get(),
            total_ingress_bytes: self.traffic_counters.torrent_http_ingress.get()
                + self.traffic_counters.web_http_ingress.get()
                + self.traffic_counters.udp_ingress.get(),
            total_egress_bytes: self.traffic_counters.torrent_http_egress.get()
                + self.traffic_counters.web_http_egress.get()
                + self.traffic_counters.udp_egress.get(),
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
        self.requests_per_second
            .set(snapshot.requests_per_second().round() as i64);
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
        assert_eq!(snapshot.total_requests, 2);
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
        assert_eq!(snapshot.total_requests, 3);
        assert_eq!(snapshot.total_ingress_bytes, 111);
    }

    #[test]
    fn given_requests_when_snapshotted_then_process_lifetime_total_is_returned() {
        let metrics = AppMetrics::new().expect("metrics should initialize");
        for _ in 0..120 {
            metrics.record_traffic_at("torrent_http", 1, 1, 1_000);
        }

        assert_eq!(metrics.traffic_snapshot_at(1_000).total_requests, 120);
    }

    #[test]
    fn given_requests_in_rolling_window_when_rate_requested_then_total_rate_is_returned() {
        let metrics = AppMetrics::new().expect("metrics should initialize");
        for _ in 0..50 {
            metrics.record_traffic_at("torrent_http", 1, 1, 1_000);
            metrics.record_traffic_at("web_http", 1, 1, 1_000);
            metrics.record_traffic_at("udp", 1, 1, 1_000);
        }

        assert_eq!(
            metrics.traffic_snapshot_at(1_000).requests_per_second(),
            2.5
        );
        assert_eq!(metrics.requests_per_second.get(), 3);
    }

    #[test]
    fn given_concurrent_traffic_when_recorded_then_no_counts_are_lost() {
        let metrics = AppMetrics::new().expect("metrics should initialize");
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let metrics = metrics.clone();
                scope.spawn(move || {
                    for _ in 0..1_000 {
                        metrics.record_traffic_at("torrent_http", 2, 3, 1_000);
                    }
                });
            }
        });

        let snapshot = metrics.traffic_snapshot_at(1_000);
        assert_eq!(snapshot.total_requests, 8_000);
        assert_eq!(snapshot.torrent_http.requests_per_minute, 8_000);
        assert_eq!(snapshot.torrent_http.ingress_bytes, 16_000);
        assert_eq!(snapshot.torrent_http.egress_bytes, 24_000);
    }
}
