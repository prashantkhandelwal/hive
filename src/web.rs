use std::{
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    body::{Body, Bytes, HttpBody},
    extract::{ConnectInfo, Query, RawQuery, Request, State},
    http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tower::limit::ConcurrencyLimitLayer;
use tracing::debug;

use crate::{
    config::{AppConfig, Protocol},
    metrics::AppMetrics,
    persistence::{DashboardHistory, MetricPoint, Persistence},
    rate_limit::RateLimiter,
    state::{unix_timestamp, Peer, TrackerState, TrackerSummary},
};

#[path = "web_protocol.rs"]
mod protocol;

use protocol::{
    announce_payload, decode_scrape_payload, first, identifier, optional_number, parse_compact,
    parse_event, parse_query, required_identifier, required_number, scrape_format, scrape_payload,
    ScrapeFormat,
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
    body: Bytes,
}

impl ScrapeCache {
    fn get(&self, revision: u64) -> Option<Bytes> {
        self.entry.lock().ok().and_then(|entry| {
            entry
                .as_ref()
                .filter(|cached| cached.revision == revision && cached.expires_at > Instant::now())
                .map(|cached| cached.body.clone())
        })
    }

    fn insert(&self, revision: u64, body: Bytes) {
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
    request_per_second: u64,
}

#[derive(Default, Deserialize)]
struct HistoryQuery {
    period: Option<String>,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

pub fn router(context: AppContext, enable_http_tracker: bool) -> Router {
    let mut router = dashboard_routes().route("/health", get(health));
    router = add_tracker_routes(router, &context, enable_http_tracker);
    finish_router(router, context)
}

pub fn tracker_router(context: AppContext, enable_http_tracker: bool) -> Router {
    let router = Router::new().route("/health", get(health));
    let router = add_tracker_routes(router, &context, enable_http_tracker);
    finish_router(router, context)
}

pub fn admin_router(context: AppContext) -> Router {
    finish_router(dashboard_routes(), context)
}

fn dashboard_routes() -> Router<AppContext> {
    Router::new()
        .route("/", get(index))
        .route("/stats", get(statistics))
        .route("/history", get(history))
        .route("/metrics", get(metrics))
}

fn add_tracker_routes(
    mut router: Router<AppContext>,
    context: &AppContext,
    enable_http_tracker: bool,
) -> Router<AppContext> {
    if context.config.enable_http_scrape {
        router = router.route("/scrape", get(scrape));
    }
    if enable_http_tracker {
        router = router.route("/announce", get(announce));
    }
    router
}

fn finish_router(router: Router<AppContext>, context: AppContext) -> Router {
    let app_metrics = context.metrics.clone();
    let max_concurrent_http_requests = context.config.max_concurrent_http_requests;
    let router = router.with_state(context);
    router
        .layer(middleware::from_fn_with_state(app_metrics, observe_traffic))
        .layer(ConcurrencyLimitLayer::new(max_concurrent_http_requests))
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
    headers: HeaderMap,
    RawQuery(query): RawQuery,
) -> Result<Response, ApiError> {
    let client_ip = resolve_client_ip(
        remote.ip(),
        &headers,
        &context.config.trusted_proxy_cidrs,
        &context.config.client_ip_header,
    )?;
    tracker_gate(&context, client_ip)?;
    let params = parse_query(query.as_deref().unwrap_or_default())?;
    let info_hash = required_identifier(&params, "info_hash")?;
    reject_blacklisted_info_hash(&context, &info_hash, "announce")?;
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
        ip: client_ip,
        port,
        left,
        last_seen: unix_timestamp(),
    };
    let (stats, peers) = context
        .state
        .announce_with_peers(info_hash, peer, event, numwant);
    context.metrics.announce("http", event.as_str());
    update_population(&context);
    context.metrics.request("http", "announce", "ok");
    Ok(bencoded_response(announce_payload(
        stats,
        peers,
        client_ip,
        context.config.announce_interval,
        compact,
    )))
}

async fn scrape(
    State(context): State<AppContext>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
) -> Result<Response, ApiError> {
    let client_ip = resolve_client_ip(
        remote.ip(),
        &headers,
        &context.config.trusted_proxy_cidrs,
        &context.config.client_ip_header,
    )?;
    tracker_gate(&context, client_ip)?;
    let params = parse_query(query.as_deref().unwrap_or_default())?;
    let format = scrape_format(first(&params, "format"))?;
    let body = match params.get("info_hash") {
        Some(values) => {
            let hashes = values
                .iter()
                .map(|value| identifier(value, "info_hash"))
                .collect::<Result<Vec<_>, _>>()?;
            if hashes
                .iter()
                .any(|info_hash| context.config.blacklist.contains_info_hash(info_hash))
            {
                context.metrics.request("http", "scrape", "blacklisted");
                return Err(ApiError::forbidden("torrent is blacklisted"));
            }
            Bytes::from(scrape_payload(&context.state, hashes))
        }
        None => {
            let revision = context.state.revision();
            if let Some(body) = context.scrape_cache.get(revision) {
                body
            } else {
                let body = Bytes::from(scrape_payload(&context.state, context.state.info_hashes()));
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

async fn statistics(State(context): State<AppContext>) -> Json<StatisticsResponse> {
    let summary = context.state.summary();
    let uptime_seconds = context.started_at.elapsed().as_secs();
    let traffic = context.metrics.traffic_snapshot();
    Json(StatisticsResponse {
        version: build_version(),
        protocol: context.protocol,
        summary,
        uptime_seconds,
        total_requests: traffic.total_requests,
        request_per_second: traffic.tracker_requests_last_second,
    })
}

async fn history(
    State(context): State<AppContext>,
    Query(query): Query<HistoryQuery>,
) -> Result<Json<DashboardHistory>, ApiError> {
    let (days, bucket_seconds) = match query.period.as_deref().unwrap_or("day") {
        "day" => (1, 5 * 60),
        "week" => (7, 60 * 60),
        "month" => (30, 6 * 60 * 60),
        _ => return Err(ApiError::bad_request("period must be day, week, or month")),
    };
    let summary = context.state.summary();
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
    Ok(Json(history))
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
                database: if context.persistence.is_memory() {
                    "disabled"
                } else {
                    "ok"
                },
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
    if context.config.blacklist.contains_ip(&ip) {
        context.metrics.request("http", "tracker", "blacklisted");
        return Err(ApiError::forbidden("client IP is blacklisted"));
    }
    if !context.rate_limiter.check(ip) {
        context.metrics.request("http", "tracker", "rate_limited");
        return Err(ApiError {
            status: StatusCode::TOO_MANY_REQUESTS,
            message: "rate limit exceeded".into(),
        });
    }
    Ok(())
}

fn reject_blacklisted_info_hash(
    context: &AppContext,
    info_hash: &InfoHash,
    endpoint: &'static str,
) -> Result<(), ApiError> {
    if context.config.blacklist.contains_info_hash(info_hash) {
        context.metrics.request("http", endpoint, "blacklisted");
        return Err(ApiError::forbidden("torrent is blacklisted"));
    }
    Ok(())
}

fn resolve_client_ip(
    remote_ip: IpAddr,
    headers: &HeaderMap,
    trusted_proxy_cidrs: &[ipnet::IpNet],
    client_ip_header: &HeaderName,
) -> Result<IpAddr, ApiError> {
    if !trusted_proxy_cidrs
        .iter()
        .any(|network| network.contains(&remote_ip))
    {
        return Ok(remote_ip);
    }

    let value = headers.get(client_ip_header).ok_or_else(|| {
        ApiError::bad_request(format!(
            "trusted proxy request is missing {client_ip_header}"
        ))
    })?;
    let value = value
        .to_str()
        .map_err(|_| ApiError::bad_request(format!("{client_ip_header} is not valid ASCII")))?;
    value
        .parse()
        .map_err(|_| ApiError::bad_request(format!("{client_ip_header} is not a valid IP address")))
}

fn update_population(context: &AppContext) {
    let summary = context.state.summary();
    context.metrics.set_population(
        summary.peers,
        summary.seeders,
        summary.leechers,
        summary.torrents,
        summary.completed,
    );
}

fn bencoded_response(body: impl Into<Body>) -> Response {
    let headers = [(
        header::CONTENT_TYPE,
        HeaderValue::from_static(BITTORRENT_CONTENT_TYPE),
    )];
    let body = body.into();
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

    fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
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
    use std::borrow::Cow;

    use super::protocol::{percent_decode, ScrapeJsonStats};
    use super::*;
    use crate::state::{AnnounceEvent, SwarmStats};

    #[test]
    fn given_statistics_response_when_serialized_then_history_is_omitted() {
        let response = StatisticsResponse {
            version: "test",
            protocol: Protocol::Http,
            summary: TrackerSummary::default(),
            uptime_seconds: 1,
            total_requests: 2,
            request_per_second: 12,
        };

        let serialized =
            serde_json::to_value(response).expect("statistics response should serialize");

        assert!(serialized.get("history").is_none());
        assert!(serialized.get("requests_per_second").is_none());
        assert_eq!(serialized["uptime_seconds"], 1);
        assert_eq!(serialized["total_requests"], 2);
        assert_eq!(serialized["request_per_second"], 12);
    }

    #[test]
    fn given_cached_scrape_when_revision_changes_then_cache_is_invalidated() {
        let cache = ScrapeCache::default();
        let body = Bytes::from(b"cached".to_vec());
        let body_pointer = body.as_ptr();
        cache.insert(1, body);

        let cached = cache.get(1).expect("cached body should be returned");
        assert_eq!(cached, Bytes::from_static(b"cached"));
        assert_eq!(cached.as_ptr(), body_pointer);
        assert_eq!(cache.get(2), None);
    }

    #[test]
    fn given_unescaped_query_component_when_decoded_then_input_is_borrowed() {
        assert!(matches!(
            percent_decode("uploaded").expect("query component should decode"),
            Cow::Borrowed(b"uploaded")
        ));
        assert!(matches!(
            percent_decode("%75ploaded").expect("query component should decode"),
            Cow::Owned(value) if value == b"uploaded"
        ));
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
    fn given_untrusted_peer_when_client_header_is_present_then_socket_ip_is_used() {
        let mut headers = HeaderMap::new();
        headers.insert("cf-connecting-ip", "203.0.113.10".parse().unwrap());

        let client_ip = resolve_client_ip(
            "198.51.100.20".parse().unwrap(),
            &headers,
            &["127.0.0.1/32".parse().unwrap()],
            &HeaderName::from_static("cf-connecting-ip"),
        )
        .expect("untrusted peer should use its socket address");

        assert_eq!(client_ip, "198.51.100.20".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn given_trusted_proxy_when_client_header_is_valid_then_forwarded_ip_is_used() {
        let mut headers = HeaderMap::new();
        headers.insert("cf-connecting-ip", "203.0.113.10".parse().unwrap());

        let client_ip = resolve_client_ip(
            "127.0.0.1".parse().unwrap(),
            &headers,
            &["127.0.0.1/32".parse().unwrap()],
            &HeaderName::from_static("cf-connecting-ip"),
        )
        .expect("trusted proxy should use the forwarded address");

        assert_eq!(client_ip, "203.0.113.10".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn given_trusted_proxy_without_client_header_then_request_is_rejected() {
        let error = resolve_client_ip(
            "127.0.0.1".parse().unwrap(),
            &HeaderMap::new(),
            &["127.0.0.1/32".parse().unwrap()],
            &HeaderName::from_static("cf-connecting-ip"),
        )
        .expect_err("trusted proxy must supply the configured header");

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(error.message.contains("missing cf-connecting-ip"));
    }

    #[test]
    fn given_trusted_proxy_with_invalid_client_header_then_request_is_rejected() {
        let mut headers = HeaderMap::new();
        headers.insert("cf-connecting-ip", "not-an-ip".parse().unwrap());

        let error = resolve_client_ip(
            "127.0.0.1".parse().unwrap(),
            &headers,
            &["127.0.0.1/32".parse().unwrap()],
            &HeaderName::from_static("cf-connecting-ip"),
        )
        .expect_err("invalid forwarded address should fail");

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(error.message.contains("not a valid IP address"));
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
