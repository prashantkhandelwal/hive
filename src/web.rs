use std::{
    collections::{BTreeMap, HashMap},
    fmt::Write as _,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    body::{Body, HttpBody},
    extract::{ConnectInfo, Query, RawQuery, Request, State},
    http::{header, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::{
    bencode::{decode as decode_bencode, Value as BencodeValue},
    config::{AppConfig, Protocol},
    metrics::AppMetrics,
    persistence::{DashboardHistory, MetricPoint, Persistence},
    rate_limit::RateLimiter,
    state::{
        unix_timestamp, AnnounceEvent, InfoHash, Peer, SwarmStats, TrackerState, TrackerSummary,
    },
};

#[derive(Clone)]
pub struct AppContext {
    pub config: AppConfig,
    pub protocol: Protocol,
    pub state: Arc<TrackerState>,
    pub persistence: Persistence,
    pub metrics: AppMetrics,
    pub rate_limiter: Arc<RateLimiter>,
    pub scrape_cache: Arc<ScrapeCache>,
    pub started_at: Instant,
}

const SCRAPE_CACHE_TTL: Duration = Duration::from_secs(5);

#[derive(Default)]
pub struct ScrapeCache {
    entry: Mutex<Option<CachedScrape>>,
}

struct CachedScrape {
    revision: u64,
    expires_at: Instant,
    body: Vec<u8>,
}

impl ScrapeCache {
    fn get(&self, revision: u64) -> Option<Vec<u8>> {
        self.entry.lock().ok().and_then(|entry| {
            entry
                .as_ref()
                .filter(|cached| cached.revision == revision && cached.expires_at > Instant::now())
                .map(|cached| cached.body.clone())
        })
    }

    fn insert(&self, revision: u64, body: Vec<u8>) {
        if let Ok(mut entry) = self.entry.lock() {
            *entry = Some(CachedScrape {
                revision,
                expires_at: Instant::now() + SCRAPE_CACHE_TTL,
                body,
            });
        }
    }
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    database: &'static str,
}

#[derive(Serialize)]
struct StatisticsResponse {
    version: &'static str,
    protocol: Protocol,
    #[serde(flatten)]
    summary: TrackerSummary,
    uptime_seconds: u64,
    total_requests: u64,
    requests_per_second: f64,
    history: DashboardHistory,
}

#[derive(Default, Deserialize)]
struct StatisticsQuery {
    period: Option<String>,
}

#[derive(Clone, Copy, Debug)]
enum ScrapeFormat {
    Bencode,
    Json,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
struct ScrapeJsonResponse {
    files: BTreeMap<String, ScrapeJsonStats>,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
struct ScrapeJsonStats {
    complete: u64,
    downloaded: u64,
    incomplete: u64,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

pub fn router(context: AppContext, enable_http_tracker: bool) -> Router {
    let app_metrics = context.metrics.clone();
    let config = &context.config;
    let mut router = Router::new().route("/", get(index));
    if config.enable_http_scrape {
        router = router.route("/scrape", get(scrape));
    }
    router = router
        .route("/stats", get(statistics))
        .route("/metrics", get(metrics))
        .route("/health", get(health));
    if enable_http_tracker {
        router = router.route("/announce", get(announce));
    }
    let router = router.with_state(context);
    router.layer(middleware::from_fn_with_state(app_metrics, observe_traffic))
}

async fn observe_traffic(
    State(metrics): State<AppMetrics>,
    request: Request,
    next: Next,
) -> Response {
    let debug_context = tracing::enabled!(tracing::Level::DEBUG).then(|| {
        (
            Instant::now(),
            request.method().clone(),
            request.uri().path().to_owned(),
        )
    });
    let traffic_kind = match request.uri().path() {
        "/announce" | "/scrape" => "torrent_http",
        _ => "web_http",
    };
    let metadata_bytes = request.method().as_str().len()
        + request.uri().path().len()
        + request
            .uri()
            .query()
            .map(|query| query.len() + 1)
            .unwrap_or_default()
        + request
            .headers()
            .iter()
            .map(|(name, value)| name.as_str().len() + value.as_bytes().len())
            .sum::<usize>();
    let request_body_bytes = request.body().size_hint().exact().unwrap_or_default() as usize;
    if request_body_bytes > 1024 * 1024 {
        let response = (StatusCode::PAYLOAD_TOO_LARGE, "request body too large").into_response();
        metrics.record_traffic(traffic_kind, metadata_bytes, 22);
        return response;
    }
    let ingress_bytes = metadata_bytes + request_body_bytes;
    let response = next.run(request).await;
    let egress_bytes = response.body().size_hint().exact().unwrap_or_default() as usize;
    metrics.record_traffic(traffic_kind, ingress_bytes, egress_bytes);
    if let Some((started_at, method, path)) = debug_context {
        debug!(
            %method,
            %path,
            status = %response.status(),
            ingress_bytes,
            egress_bytes,
            elapsed_ms = started_at.elapsed().as_millis(),
            "HTTP request completed"
        );
    }
    response
}

async fn announce(
    State(context): State<AppContext>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    RawQuery(query): RawQuery,
) -> Result<Response, ApiError> {
    tracker_gate(&context, remote.ip())?;
    let params = parse_query(query.as_deref().unwrap_or_default())?;
    let info_hash = required_identifier(&params, "info_hash")?;
    let peer_id = required_identifier(&params, "peer_id")?;
    let port = required_number::<u16>(&params, "port")?;
    let _uploaded = required_number::<u64>(&params, "uploaded")?;
    let _downloaded = required_number::<u64>(&params, "downloaded")?;
    let left = required_number::<u64>(&params, "left")?;
    let numwant = optional_number::<usize>(&params, "numwant")?
        .unwrap_or(50)
        .min(200);
    let event = parse_event(first(&params, "event"))?;
    let compact = parse_compact(first(&params, "compact"))?;
    let peer = Peer {
        peer_id,
        ip: remote.ip(),
        port,
        left,
        last_seen: unix_timestamp(),
    };
    let (stats, peers) = context
        .state
        .announce_with_peers(info_hash, peer, event, numwant);
    context.metrics.announce("http", event_name(event));
    update_population(&context);
    context.metrics.request("http", "announce", "ok");
    Ok(bencoded_response(announce_payload(
        stats,
        peers,
        remote.ip(),
        context.config.announce_interval,
        compact,
    )))
}

async fn scrape(
    State(context): State<AppContext>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    RawQuery(query): RawQuery,
) -> Result<Response, ApiError> {
    tracker_gate(&context, remote.ip())?;
    let params = parse_query(query.as_deref().unwrap_or_default())?;
    let format = scrape_format(first(&params, "format"))?;
    let body = match params.get("info_hash") {
        Some(values) => {
            let hashes = values
                .iter()
                .map(|value| identifier(value, "info_hash"))
                .collect::<Result<Vec<_>, _>>()?;
            scrape_payload(&context.state, hashes)
        }
        None => {
            let revision = context.state.revision();
            if let Some(body) = context.scrape_cache.get(revision) {
                body
            } else {
                let body = scrape_payload(&context.state, context.state.info_hashes());
                context.scrape_cache.insert(revision, body.clone());
                body
            }
        }
    };
    context.metrics.request("http", "scrape", "ok");
    match format {
        ScrapeFormat::Bencode => Ok(bencoded_response(body)),
        ScrapeFormat::Json => {
            let response = decode_scrape_payload(&body)
                .map_err(|error| ApiError::internal(format!("invalid scrape payload: {error}")))?;
            Ok(Json(response).into_response())
        }
    }
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn statistics(
    State(context): State<AppContext>,
    Query(query): Query<StatisticsQuery>,
) -> Result<Json<StatisticsResponse>, ApiError> {
    let (days, bucket_seconds) = match query.period.as_deref().unwrap_or("day") {
        "day" => (1, 5 * 60),
        "week" => (7, 60 * 60),
        "month" => (30, 6 * 60 * 60),
        _ => return Err(ApiError::bad_request("period must be day, week, or month")),
    };
    let summary = context.state.summary();
    let uptime_seconds = context.started_at.elapsed().as_secs();
    let traffic = context.metrics.traffic_snapshot();
    let mut history = context
        .persistence
        .dashboard_history(days, bucket_seconds)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?;
    let current = MetricPoint {
        timestamp: unix_timestamp() / 60 * 60,
        peers: summary.peers,
        seeders: summary.seeders,
        leechers: summary.leechers,
        torrents: summary.torrents,
        completed: summary.completed,
    };
    if let Some(latest) = history
        .metrics
        .last_mut()
        .filter(|point| point.timestamp == current.timestamp)
    {
        *latest = current;
    } else {
        history.metrics.push(current);
    }
    Ok(Json(StatisticsResponse {
        version: build_version(),
        protocol: context.protocol,
        summary,
        uptime_seconds,
        total_requests: traffic.total_requests,
        requests_per_second: traffic.requests_per_second(),
        history,
    }))
}

fn build_version() -> &'static str {
    match option_env!("HIVE_VERSION") {
        Some(version) => version,
        None => env!("CARGO_PKG_VERSION"),
    }
}

async fn metrics(State(context): State<AppContext>) -> Result<Response, ApiError> {
    update_population(&context);
    let body = context
        .metrics
        .encode()
        .map_err(|error| ApiError::internal(error.to_string()))?;
    let headers = [(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4"),
    )];
    Ok((headers, body).into_response())
}

async fn health(State(context): State<AppContext>) -> (StatusCode, Json<HealthResponse>) {
    if context.persistence.is_healthy().await {
        (
            StatusCode::OK,
            Json(HealthResponse {
                status: "ok",
                database: "ok",
            }),
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(HealthResponse {
                status: "degraded",
                database: "unavailable",
            }),
        )
    }
}

fn tracker_gate(context: &AppContext, ip: IpAddr) -> Result<(), ApiError> {
    if !context.rate_limiter.check(ip) {
        context.metrics.request("http", "tracker", "rate_limited");
        return Err(ApiError {
            status: StatusCode::TOO_MANY_REQUESTS,
            message: "rate limit exceeded".into(),
        });
    }
    Ok(())
}

fn update_population(context: &AppContext) {
    context
        .metrics
        .set_population(context.state.peer_count(), context.state.swarm_count());
}

type QueryParams = HashMap<String, Vec<Vec<u8>>>;

fn parse_query(query: &str) -> Result<QueryParams, ApiError> {
    let mut params = HashMap::new();
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = String::from_utf8(percent_decode(key)?)
            .map_err(|_| ApiError::bad_request("query key is not valid UTF-8"))?;
        params
            .entry(key)
            .or_insert_with(Vec::new)
            .push(percent_decode(value)?);
    }
    Ok(params)
}

fn percent_decode(value: &str) -> Result<Vec<u8>, ApiError> {
    let bytes = value.as_bytes();
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
    Ok(decoded)
}

fn hex_digit(value: u8) -> Result<u8, ApiError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(ApiError::bad_request("invalid percent escape")),
    }
}

fn required_identifier(params: &QueryParams, name: &'static str) -> Result<[u8; 20], ApiError> {
    identifier(
        first(params, name).ok_or_else(|| ApiError::bad_request(format!("missing {name}")))?,
        name,
    )
}

fn identifier(value: &[u8], name: &'static str) -> Result<[u8; 20], ApiError> {
    let length = value.len();
    value
        .try_into()
        .map_err(|_| ApiError::bad_request(format!("{name} must be 20 bytes, got {length}")))
}

fn required_number<T>(params: &QueryParams, name: &'static str) -> Result<T, ApiError>
where
    T: std::str::FromStr,
{
    optional_number(params, name)?.ok_or_else(|| ApiError::bad_request(format!("missing {name}")))
}

fn optional_number<T>(params: &QueryParams, name: &'static str) -> Result<Option<T>, ApiError>
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

fn first<'a>(params: &'a QueryParams, name: &str) -> Option<&'a [u8]> {
    params
        .get(name)
        .and_then(|values| values.first())
        .map(Vec::as_slice)
}

fn parse_event(value: Option<&[u8]>) -> Result<AnnounceEvent, ApiError> {
    match value {
        None | Some(b"") => Ok(AnnounceEvent::Update),
        Some(b"started") => Ok(AnnounceEvent::Started),
        Some(b"completed") => Ok(AnnounceEvent::Completed),
        Some(b"stopped") => Ok(AnnounceEvent::Stopped),
        _ => Err(ApiError::bad_request("invalid event")),
    }
}

fn parse_compact(value: Option<&[u8]>) -> Result<bool, ApiError> {
    match value {
        None | Some(b"1") => Ok(true),
        Some(b"0") => Ok(false),
        _ => Err(ApiError::bad_request("compact must be 0 or 1")),
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

fn announce_payload(
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

fn scrape_payload(state: &TrackerState, mut hashes: Vec<InfoHash>) -> Vec<u8> {
    hashes.sort_unstable();
    let mut output = b"d5:filesd".to_vec();
    for info_hash in hashes {
        let stats = state.stats(&info_hash);
        output.extend_from_slice(b"20:");
        output.extend_from_slice(&info_hash);
        output.extend_from_slice(
            format!(
                "d8:completei{}e10:downloadedi{}e10:incompletei{}ee",
                stats.complete, stats.downloaded, stats.incomplete
            )
            .as_bytes(),
        );
    }
    output.extend_from_slice(b"ee");
    output
}

fn scrape_format(value: Option<&[u8]>) -> Result<ScrapeFormat, ApiError> {
    match value {
        None | Some(b"") | Some(b"bencode") => Ok(ScrapeFormat::Bencode),
        Some(b"json") => Ok(ScrapeFormat::Json),
        _ => Err(ApiError::bad_request("format must be bencode or json")),
    }
}

fn decode_scrape_payload(payload: &[u8]) -> Result<ScrapeJsonResponse, String> {
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
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value {
        write!(encoded, "{byte:02x}").expect("writing to a String cannot fail");
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
        output.extend_from_slice(format!("4:porti{}ee", peer.port).as_bytes());
    }
    output.push(b'e');
}

fn append_bencoded_bytes(output: &mut Vec<u8>, value: &[u8]) {
    output.extend_from_slice(value.len().to_string().as_bytes());
    output.push(b':');
    output.extend_from_slice(value);
}

fn same_address_family(left: IpAddr, right: IpAddr) -> bool {
    matches!(
        (left, right),
        (IpAddr::V4(_), IpAddr::V4(_)) | (IpAddr::V6(_), IpAddr::V6(_))
    )
}

fn bencoded_response(body: Vec<u8>) -> Response {
    let headers = [(
        header::CONTENT_TYPE,
        HeaderValue::from_static(BITTORRENT_CONTENT_TYPE),
    )];
    (headers, body).into_response()
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let message = self.message.into_bytes();
        let mut body = format!("d14:failure reason{}:", message.len()).into_bytes();
        body.extend_from_slice(&message);
        body.push(b'e');
        let headers = [(
            header::CONTENT_TYPE,
            HeaderValue::from_static(BITTORRENT_CONTENT_TYPE),
        )];
        (self.status, headers, Body::from(body)).into_response()
    }
}

const BITTORRENT_CONTENT_TYPE: &str = "application/x-bittorrent";

const INDEX_HTML: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/static/index.html"
));
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_cached_scrape_when_revision_changes_then_cache_is_invalidated() {
        let cache = ScrapeCache::default();
        cache.insert(1, b"cached".to_vec());

        assert_eq!(cache.get(1), Some(b"cached".to_vec()));
        assert_eq!(cache.get(2), None);
    }

    #[test]
    fn given_binary_query_when_parsed_then_identifiers_are_preserved() {
        let params =
            parse_query("info_hash=%00%01%02%03%04%05%06%07%08%09%0A%0B%0C%0D%0E%0F%10%11%12%13")
                .expect("valid query should parse");
        assert_eq!(
            required_identifier(&params, "info_hash").expect("identifier should parse"),
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
        );
    }

    #[test]
    fn given_bencoded_body_when_response_is_built_then_binary_content_type_is_used() {
        let response = bencoded_response(b"de".to_vec());

        assert_eq!(
            response.headers().get(header::CONTENT_TYPE),
            Some(&HeaderValue::from_static(BITTORRENT_CONTENT_TYPE))
        );
    }

    #[test]
    fn given_missing_transfer_counters_when_announce_is_parsed_then_request_is_rejected() {
        let params = parse_query(
            "info_hash=aaaaaaaaaaaaaaaaaaaa&peer_id=bbbbbbbbbbbbbbbbbbbb&port=6881&left=0",
        )
        .expect("query should parse");

        assert_eq!(
            required_number::<u64>(&params, "uploaded")
                .expect_err("uploaded should be required")
                .message,
            "missing uploaded"
        );
        assert_eq!(
            required_number::<u64>(&params, "downloaded")
                .expect_err("downloaded should be required")
                .message,
            "missing downloaded"
        );
    }

    #[test]
    fn given_compact_disabled_when_announce_payload_is_built_then_bep3_peer_list_is_returned() {
        let peer = Peer {
            peer_id: [b'p'; 20],
            ip: "127.0.0.1".parse().unwrap(),
            port: 6881,
            left: 0,
            last_seen: 0,
        };

        let payload = announce_payload(
            SwarmStats {
                complete: 1,
                incomplete: 0,
                downloaded: 1,
            },
            vec![peer],
            "127.0.0.2".parse().unwrap(),
            1800,
            false,
        );

        assert_eq!(
            payload,
            b"d8:completei1e10:incompletei0e8:intervali1800e5:peersld2:ip9:127.0.0.1\
7:peer id20:pppppppppppppppppppp4:porti6881eeee"
        );
    }

    #[test]
    fn given_mixed_address_families_when_compact_payload_is_built_then_peers_are_separated() {
        let peers = vec![
            Peer {
                peer_id: [1; 20],
                ip: "127.0.0.1".parse().unwrap(),
                port: 6881,
                left: 1,
                last_seen: 0,
            },
            Peer {
                peer_id: [2; 20],
                ip: "::1".parse().unwrap(),
                port: 6882,
                left: 1,
                last_seen: 0,
            },
        ];

        let payload = announce_payload(
            SwarmStats::default(),
            peers,
            "127.0.0.2".parse().unwrap(),
            1800,
            true,
        );

        assert!(payload.windows(7).any(|window| window == b"5:peers"));
        assert!(payload.windows(8).any(|window| window == b"6:peers6"));
        assert!(payload.ends_with(&[0x1a, 0xe2, b'e']));
    }

    #[test]
    fn given_completed_torrent_when_scraped_then_bep48_statistics_are_returned() {
        let state = TrackerState::default();
        let info_hash = [b'a'; 20];
        let mut peer = Peer {
            peer_id: [b'p'; 20],
            ip: "127.0.0.1".parse().unwrap(),
            port: 6881,
            left: 10,
            last_seen: 0,
        };
        state.announce(info_hash, peer.clone(), AnnounceEvent::Started);
        peer.left = 0;
        state.announce(info_hash, peer, AnnounceEvent::Completed);

        let payload = scrape_payload(&state, vec![info_hash]);

        assert_eq!(
            payload,
            b"d5:filesd20:aaaaaaaaaaaaaaaaaaaad8:completei1e10:downloadedi1e\
10:incompletei0eeee"
        );
    }

    #[test]
    fn given_bep48_payload_when_json_requested_then_info_hash_and_stats_are_decoded() {
        let state = TrackerState::default();
        let info_hash = [0xab; 20];
        let peer = Peer {
            peer_id: [1; 20],
            ip: "127.0.0.1".parse().unwrap(),
            port: 6881,
            left: 5,
            last_seen: 0,
        };
        state.announce(info_hash, peer, AnnounceEvent::Started);

        let decoded = decode_scrape_payload(&scrape_payload(&state, vec![info_hash]))
            .expect("scrape payload should decode");

        assert_eq!(
            decoded.files.get(&"ab".repeat(20)),
            Some(&ScrapeJsonStats {
                complete: 0,
                downloaded: 0,
                incomplete: 1,
            })
        );
    }

    #[test]
    fn given_unknown_scrape_format_when_parsed_then_request_is_rejected() {
        assert_eq!(
            scrape_format(Some(b"xml"))
                .expect_err("unknown format should fail")
                .message,
            "format must be bencode or json"
        );
    }
}
