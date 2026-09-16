use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::net::UdpSocket;

use crate::{
    metrics::AppMetrics,
    rate_limit::RateLimiter,
    state::{unix_timestamp, AnnounceEvent, Peer, TrackerState},
};

const PROTOCOL_ID: u64 = 0x0417_2710_1980;
const ACTION_CONNECT: u32 = 0;
const ACTION_ANNOUNCE: u32 = 1;
const ACTION_SCRAPE: u32 = 2;
const ACTION_ERROR: u32 = 3;

pub struct UdpTracker {
    socket: UdpSocket,
    state: Arc<TrackerState>,
    metrics: AppMetrics,
    rate_limiter: Arc<RateLimiter>,
    announce_interval: u32,
    connection_secret: u64,
}

impl UdpTracker {
    pub async fn bind(
        address: SocketAddr,
        state: Arc<TrackerState>,
        metrics: AppMetrics,
        rate_limiter: Arc<RateLimiter>,
        announce_interval: u32,
    ) -> std::io::Result<Self> {
        let socket = UdpSocket::bind(address).await?;
        let entropy = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        Ok(Self {
            socket,
            state,
            metrics,
            rate_limiter,
            announce_interval,
            connection_secret: entropy ^ u64::from(std::process::id()),
        })
    }

    pub async fn run(self) -> std::io::Result<()> {
        let mut buffer = [0_u8; 2048];
        loop {
            let (length, remote) = self.socket.recv_from(&mut buffer).await?;
            let response = self.handle_packet(&buffer[..length], remote);
            if let Some(response) = response {
                self.socket.send_to(&response, remote).await?;
            }
        }
    }

    fn handle_packet(&self, packet: &[u8], remote: SocketAddr) -> Option<Vec<u8>> {
        if packet.len() < 16 || !self.rate_limiter.check(remote.ip()) {
            self.metrics.request("udp", "packet", "rejected");
            return None;
        }
        let action = read_u32(packet, 8)?;
        let transaction_id = read_u32(packet, 12)?;
        let response = match action {
            ACTION_CONNECT => self.connect(packet, remote, transaction_id),
            ACTION_ANNOUNCE => self.announce(packet, remote, transaction_id),
            ACTION_SCRAPE => self.scrape(packet, remote, transaction_id),
            _ => error_response(transaction_id, "unsupported action"),
        };
        Some(response)
    }

    fn connect(&self, packet: &[u8], remote: SocketAddr, transaction_id: u32) -> Vec<u8> {
        if read_u64(packet, 0) != Some(PROTOCOL_ID) {
            self.metrics.request("udp", "connect", "invalid");
            return error_response(transaction_id, "invalid protocol id");
        }
        self.metrics.request("udp", "connect", "ok");
        let mut response = Vec::with_capacity(16);
        push_u32(&mut response, ACTION_CONNECT);
        push_u32(&mut response, transaction_id);
        push_u64(
            &mut response,
            self.connection_id(remote.ip(), time_window()),
        );
        response
    }

    fn announce(&self, packet: &[u8], remote: SocketAddr, transaction_id: u32) -> Vec<u8> {
        if packet.len() < 98 || !self.valid_connection(packet, remote.ip()) {
            self.metrics.request("udp", "announce", "invalid");
            return error_response(transaction_id, "invalid announce request");
        }
        let Some(info_hash) = slice_array(packet, 16) else {
            return error_response(transaction_id, "missing info hash");
        };
        let Some(peer_id) = slice_array(packet, 36) else {
            return error_response(transaction_id, "missing peer id");
        };
        let left = read_u64(packet, 64).unwrap_or_default();
        let event = match read_u32(packet, 80).unwrap_or_default() {
            1 => AnnounceEvent::Completed,
            2 => AnnounceEvent::Started,
            3 => AnnounceEvent::Stopped,
            _ => AnnounceEvent::Update,
        };
        let numwant = read_i32(packet, 92).unwrap_or(-1);
        let limit = if numwant < 0 {
            50
        } else {
            (numwant as usize).min(200)
        };
        let port = read_u16(packet, 96).unwrap_or_default();
        if port == 0 {
            return error_response(transaction_id, "invalid peer port");
        }
        let peer = Peer {
            peer_id,
            ip: remote.ip(),
            port,
            left,
            last_seen: unix_timestamp(),
        };
        let stats = self.state.announce(info_hash, peer, event);
        let peers = self.state.peers(&info_hash, &peer_id, limit);
        self.metrics.announce("udp", event_name(event));
        self.metrics
            .set_population(self.state.peer_count(), self.state.swarm_count());
        self.metrics.request("udp", "announce", "ok");

        let mut response = Vec::with_capacity(20 + peers.len() * 18);
        push_u32(&mut response, ACTION_ANNOUNCE);
        push_u32(&mut response, transaction_id);
        push_u32(&mut response, self.announce_interval);
        push_u32(&mut response, stats.incomplete as u32);
        push_u32(&mut response, stats.complete as u32);
        append_compact_peers(&mut response, peers, remote.ip());
        response
    }

    fn scrape(&self, packet: &[u8], remote: SocketAddr, transaction_id: u32) -> Vec<u8> {
        if packet.len() < 36
            || !(packet.len() - 16).is_multiple_of(20)
            || !self.valid_connection(packet, remote.ip())
        {
            self.metrics.request("udp", "scrape", "invalid");
            return error_response(transaction_id, "invalid scrape request");
        }
        let mut response = Vec::with_capacity(8 + ((packet.len() - 16) / 20) * 12);
        push_u32(&mut response, ACTION_SCRAPE);
        push_u32(&mut response, transaction_id);
        let (info_hashes, _) = packet[16..].as_chunks::<20>();
        for info_hash in info_hashes {
            let stats = self.state.stats(info_hash);
            push_u32(&mut response, stats.complete as u32);
            push_u32(&mut response, stats.downloaded as u32);
            push_u32(&mut response, stats.incomplete as u32);
        }
        self.metrics.request("udp", "scrape", "ok");
        response
    }

    fn valid_connection(&self, packet: &[u8], ip: IpAddr) -> bool {
        let Some(provided) = read_u64(packet, 0) else {
            return false;
        };
        let window = time_window();
        provided == self.connection_id(ip, window)
            || provided == self.connection_id(ip, window.saturating_sub(1))
    }

    fn connection_id(&self, ip: IpAddr, window: u64) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.connection_secret.hash(&mut hasher);
        ip.hash(&mut hasher);
        window.hash(&mut hasher);
        hasher.finish()
    }
}

fn time_window() -> u64 {
    unix_timestamp() / 60
}

fn read_u16(packet: &[u8], offset: usize) -> Option<u16> {
    Some(u16::from_be_bytes(
        packet.get(offset..offset + 2)?.try_into().ok()?,
    ))
}

fn read_u32(packet: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_be_bytes(
        packet.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

fn read_i32(packet: &[u8], offset: usize) -> Option<i32> {
    Some(i32::from_be_bytes(
        packet.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

fn read_u64(packet: &[u8], offset: usize) -> Option<u64> {
    Some(u64::from_be_bytes(
        packet.get(offset..offset + 8)?.try_into().ok()?,
    ))
}

fn slice_array(packet: &[u8], offset: usize) -> Option<[u8; 20]> {
    packet.get(offset..offset + 20)?.try_into().ok()
}

fn push_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn push_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn error_response(transaction_id: u32, message: &str) -> Vec<u8> {
    let mut response = Vec::with_capacity(8 + message.len());
    push_u32(&mut response, ACTION_ERROR);
    push_u32(&mut response, transaction_id);
    response.extend_from_slice(message.as_bytes());
    response
}

fn append_compact_peers(output: &mut Vec<u8>, peers: Vec<Peer>, requester: IpAddr) {
    for peer in peers {
        match (requester, peer.ip) {
            (IpAddr::V4(_), IpAddr::V4(ip)) => output.extend_from_slice(&ip.octets()),
            (IpAddr::V6(_), IpAddr::V6(ip)) => output.extend_from_slice(&ip.octets()),
            _ => continue,
        }
        output.extend_from_slice(&peer.port.to_be_bytes());
    }
}

fn event_name(event: AnnounceEvent) -> &'static str {
    match event {
        AnnounceEvent::Started => "started",
        AnnounceEvent::Completed => "completed",
        AnnounceEvent::Stopped => "stopped",
        AnnounceEvent::Update => "update",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_udp_integer_when_read_then_network_byte_order_is_used() {
        assert_eq!(read_u32(&[0, 0, 0, 42], 0), Some(42));
    }
}
