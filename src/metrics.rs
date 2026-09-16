use prometheus::{Encoder, IntCounterVec, IntGauge, Opts, Registry, TextEncoder};

#[derive(Clone)]
pub struct AppMetrics {
    registry: Registry,
    requests: IntCounterVec,
    announce_events: IntCounterVec,
    active_peers: IntGauge,
    active_swarms: IntGauge,
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
        let active_peers = IntGauge::new("hive_active_peers", "Peers currently in memory")?;
        let active_swarms = IntGauge::new("hive_active_swarms", "Swarms currently in memory")?;

        registry.register(Box::new(requests.clone()))?;
        registry.register(Box::new(announce_events.clone()))?;
        registry.register(Box::new(active_peers.clone()))?;
        registry.register(Box::new(active_swarms.clone()))?;

        Ok(Self {
            registry,
            requests,
            announce_events,
            active_peers,
            active_swarms,
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

    pub fn set_population(&self, peers: usize, swarms: usize) {
        self.active_peers.set(peers as i64);
        self.active_swarms.set(swarms as i64);
    }

    pub fn encode(&self) -> Result<Vec<u8>, prometheus::Error> {
        let families = self.registry.gather();
        let mut output = Vec::new();
        TextEncoder::new().encode(&families, &mut output)?;
        Ok(output)
    }
}
