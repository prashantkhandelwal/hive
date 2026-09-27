use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    io::Write as _,
    net::IpAddr,
};

use serde::Serialize;

use super::ApiError;
use crate::{
    bencode::{decode as decode_bencode, Value as BencodeValue},
    state::{AnnounceEvent, InfoHash, Peer, SwarmStats, TrackerState},
};

pub(super) type QueryParams<'a> = HashMap<Cow<'a, str>, Vec<Cow<'a, [u8]>>>;

#[derive(Clone, Copy, Debug)]
pub(super) enum ScrapeFormat {
    Bencode,
    Json,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub(super) struct ScrapeJsonResponse {
    pub(super) files: BTreeMap<String, ScrapeJsonStats>,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub(super) struct ScrapeJsonStats {
    pub(super) complete: u64,
    pub(super) downloaded: u64,
    pub(super) incomplete: u64,
}

pub(super) fn parse_query(query: &str) -> Result<QueryParams<'_>, ApiError> {
    let mut params = HashMap::new();
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = match percent_decode(key)? {
            Cow::Borrowed(_) => Cow::Borrowed(key),
            Cow::Owned(key) => Cow::Owned(
                String::from_utf8(key)
                    .map_err(|_| ApiError::bad_request("query key is not valid UTF-8"))?,
            ),
        };
        params
            .entry(key)
            .or_insert_with(Vec::new)
            .push(percent_decode(value)?);
    }
    Ok(params)
}

pub(super) fn percent_decode(value: &str) -> Result<Cow<'_, [u8]>, ApiError> {
    let bytes = value.as_bytes();
    if !bytes.iter().any(|byte| matches!(byte, b'%' | b'+')) {
        return Ok(Cow::Borrowed(bytes));
    }
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' if index + 2 < bytes.len() => {
                let high = hex_digit(bytes[index + 1])?;
                let low = hex_digit(bytes[index + 2])?;
                decoded.push((high << 4) | low);
                index += 3;
            }
            b'%' => return Err(ApiError::bad_request("incomplete percent escape")),
            b'+' => {
                decoded.push(b' ');
                index += 1;
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }
    Ok(Cow::Owned(decoded))
}

fn hex_digit(value: u8) -> Result<u8, ApiError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(ApiError::bad_request("invalid percent escape")),
    }
}

pub(super) fn required_identifier(
    params: &QueryParams<'_>,
    name: &'static str,
) -> Result<[u8; 20], ApiError> {
    identifier(
        first(params, name).ok_or_else(|| ApiError::bad_request(format!("missing {name}")))?,
        name,
    )
}

pub(super) fn identifier(value: &[u8], name: &'static str) -> Result<[u8; 20], ApiError> {
    let length = value.len();
    value
        .try_into()
        .map_err(|_| ApiError::bad_request(format!("{name} must be 20 bytes, got {length}")))
}

pub(super) fn required_number<T>(
    params: &QueryParams<'_>,
    name: &'static str,
) -> Result<T, ApiError>
where
    T: std::str::FromStr,
{
    optional_number(params, name)?.ok_or_else(|| ApiError::bad_request(format!("missing {name}")))
}

pub(super) fn optional_number<T>(
    params: &QueryParams<'_>,
    name: &'static str,
) -> Result<Option<T>, ApiError>
where
    T: std::str::FromStr,
{
    let Some(value) = first(params, name) else {
        return Ok(None);
    };
    let text =
        std::str::from_utf8(value).map_err(|_| ApiError::bad_request(format!("invalid {name}")))?;
    text.parse()
        .map(Some)
        .map_err(|_| ApiError::bad_request(format!("invalid {name}")))
}

pub(super) fn first<'a>(params: &'a QueryParams<'_>, name: &str) -> Option<&'a [u8]> {
    params
        .get(name)
        .and_then(|values| values.first())
        .map(AsRef::as_ref)
}

pub(super) fn parse_event(value: Option<&[u8]>) -> Result<AnnounceEvent, ApiError> {
    match value {
        None | Some(b"") => Ok(AnnounceEvent::Update),
        Some(b"started") => Ok(AnnounceEvent::Started),
        Some(b"completed") => Ok(AnnounceEvent::Completed),
        Some(b"stopped") => Ok(AnnounceEvent::Stopped),
        _ => Err(ApiError::bad_request("invalid event")),
    }
}

pub(super) fn parse_compact(value: Option<&[u8]>) -> Result<bool, ApiError> {
    match value {
        None | Some(b"1") => Ok(true),
        Some(b"0") => Ok(false),
        _ => Err(ApiError::bad_request("compact must be 0 or 1")),
    }
}

pub(super) fn announce_payload(
    stats: SwarmStats,
    peers: Vec<Peer>,
    requester: IpAddr,
    interval: u32,
    compact: bool,
) -> Vec<u8> {
    let mut output = format!(
        "d8:completei{}e10:incompletei{}e8:intervali{}e5:peers",
        stats.complete, stats.incomplete, interval
    )
    .into_bytes();
    if compact {
        let (peers, peers6) = compact_peers(peers);
        append_bencoded_bytes(&mut output, &peers);
        if !peers6.is_empty() {
            output.extend_from_slice(b"6:peers6");
            append_bencoded_bytes(&mut output, &peers6);
        }
    } else {
        append_peer_list(&mut output, peers, requester);
    }
    output.push(b'e');
    output
}

pub(super) fn scrape_payload(state: &TrackerState, mut hashes: Vec<InfoHash>) -> Vec<u8> {
    hashes.sort_unstable();
    let mut output = b"d5:filesd".to_vec();
    for info_hash in hashes {
        let stats = state.stats(&info_hash);
        output.extend_from_slice(b"20:");
        output.extend_from_slice(&info_hash);
        write!(
            output,
            "d8:completei{}e10:downloadedi{}e10:incompletei{}ee",
            stats.complete, stats.downloaded, stats.incomplete
        )
        .expect("writing to a Vec cannot fail");
    }
    output.extend_from_slice(b"ee");
    output
}

pub(super) fn scrape_format(value: Option<&[u8]>) -> Result<ScrapeFormat, ApiError> {
    match value {
        None | Some(b"") | Some(b"bencode") => Ok(ScrapeFormat::Bencode),
        Some(b"json") => Ok(ScrapeFormat::Json),
        _ => Err(ApiError::bad_request("format must be bencode or json")),
    }
}

pub(super) fn decode_scrape_payload(payload: &[u8]) -> Result<ScrapeJsonResponse, String> {
    let BencodeValue::Dictionary(root) =
        decode_bencode(payload).map_err(|error| error.to_string())?
    else {
        return Err("root value is not a dictionary".into());
    };
    let files = root
        .iter()
        .find(|(key, _)| *key == b"files")
        .map(|(_, value)| value)
        .ok_or_else(|| "missing files dictionary".to_owned())?;
    let BencodeValue::Dictionary(files) = files else {
        return Err("files value is not a dictionary".into());
    };

    let mut decoded = BTreeMap::new();
    for (info_hash, stats) in files {
        if info_hash.len() != 20 {
            return Err(format!(
                "info hash must be 20 bytes, got {}",
                info_hash.len()
            ));
        }
        let BencodeValue::Dictionary(stats) = stats else {
            return Err("torrent statistics value is not a dictionary".into());
        };
        decoded.insert(
            hex_string(info_hash),
            ScrapeJsonStats {
                complete: scrape_integer(stats, b"complete")?,
                downloaded: scrape_integer(stats, b"downloaded")?,
                incomplete: scrape_integer(stats, b"incomplete")?,
            },
        );
    }
    Ok(ScrapeJsonResponse { files: decoded })
}

fn scrape_integer(entries: &[(&[u8], BencodeValue<'_>)], key: &[u8]) -> Result<u64, String> {
    let value = entries
        .iter()
        .find(|(entry_key, _)| *entry_key == key)
        .map(|(_, value)| value)
        .ok_or_else(|| format!("missing {} value", String::from_utf8_lossy(key)))?;
    let BencodeValue::Integer(value) = value else {
        return Err(format!(
            "{} value is not an integer",
            String::from_utf8_lossy(key)
        ));
    };
    u64::try_from(*value).map_err(|_| {
        format!(
            "{} value is negative or too large",
            String::from_utf8_lossy(key)
        )
    })
}

fn hex_string(value: &[u8]) -> String {
    const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value {
        encoded.push(HEX_DIGITS[(byte >> 4) as usize] as char);
        encoded.push(HEX_DIGITS[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn compact_peers(peers: Vec<Peer>) -> (Vec<u8>, Vec<u8>) {
    let mut peers4 = Vec::new();
    let mut peers6 = Vec::new();
    for peer in peers {
        match peer.ip {
            IpAddr::V4(ip) => {
                peers4.extend_from_slice(&ip.octets());
                peers4.extend_from_slice(&peer.port.to_be_bytes());
            }
            IpAddr::V6(ip) => {
                peers6.extend_from_slice(&ip.octets());
                peers6.extend_from_slice(&peer.port.to_be_bytes());
            }
        }
    }
    (peers4, peers6)
}

fn append_peer_list(output: &mut Vec<u8>, peers: Vec<Peer>, requester: IpAddr) {
    output.push(b'l');
    for peer in peers {
        if !same_address_family(requester, peer.ip) {
            continue;
        }
        output.extend_from_slice(b"d2:ip");
        append_bencoded_bytes(output, peer.ip.to_string().as_bytes());
        output.extend_from_slice(b"7:peer id20:");
        output.extend_from_slice(&peer.peer_id);
        write!(output, "4:porti{}ee", peer.port).expect("writing to a Vec cannot fail");
    }
    output.push(b'e');
}

fn append_bencoded_bytes(output: &mut Vec<u8>, value: &[u8]) {
    write!(output, "{}:", value.len()).expect("writing to a Vec cannot fail");
    output.extend_from_slice(value);
}

fn same_address_family(left: IpAddr, right: IpAddr) -> bool {
    matches!(
        (left, right),
        (IpAddr::V4(_), IpAddr::V4(_)) | (IpAddr::V6(_), IpAddr::V6(_))
    )
}
