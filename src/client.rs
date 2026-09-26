use aquatic_peer_id::{PeerClient, PeerId as AquaticPeerId};

use crate::state::PeerId;

pub fn detect(peer_id: PeerId) -> &'static str {
    match AquaticPeerId(peer_id).client() {
        PeerClient::BitTorrent(_) => "BitTorrent",
        PeerClient::Deluge(_) => "Deluge",
        PeerClient::LibTorrentRakshasa(_) => "libTorrent (Rakshasa)",
        PeerClient::LibTorrentRasterbar(_) => "libtorrent (Rasterbar)",
        PeerClient::QBitTorrent(_) => "qBittorrent",
        PeerClient::Transmission(_) => "Transmission",
        PeerClient::UTorrent(_) => "uTorrent",
        PeerClient::UTorrentEmbedded(_) => "uTorrent Embedded",
        PeerClient::UTorrentMac(_) => "uTorrent Mac",
        PeerClient::UTorrentWeb(_) => "uTorrent Web",
        PeerClient::Vuze(_) => "Vuze",
        PeerClient::WebTorrent(_) => "WebTorrent",
        PeerClient::WebTorrentDesktop(_) => "WebTorrent Desktop",
        PeerClient::Mainline(_) => "Mainline",
        _ => "Unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_azureus_style_peer_id_when_detected_then_client_name_ignores_version() {
        let mut peer_id = [b'-'; 20];
        peer_id[..8].copy_from_slice(b"-qB4500-");

        assert_eq!(detect(peer_id), "qBittorrent");
    }

    #[test]
    fn given_unrecognized_peer_id_when_detected_then_unknown_is_returned() {
        assert_eq!(detect([0xff; 20]), "Unknown");
    }
}
