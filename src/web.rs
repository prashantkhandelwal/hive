use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Instant,
};

use axum::{
    body::{to_bytes, Body},
    extract::{ConnectInfo, RawQuery, Request, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::Serialize;
use subtle::ConstantTimeEq;

use crate::{
    config::AppConfig,
    metrics::AppMetrics,
    persistence::{DailyTorrentCount, Persistence},
    rate_limit::RateLimiter,
    state::{unix_timestamp, AnnounceEvent, InfoHash, Peer, SwarmStats, TrackerState},
};

#[derive(Clone)]
pub struct AppContext {
    pub config: AppConfig,
    pub state: Arc<TrackerState>,
    pub persistence: Persistence,
    pub metrics: AppMetrics,
    pub rate_limiter: Arc<RateLimiter>,
    pub started_at: Instant,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    database: &'static str,
}

#[derive(Serialize)]
struct StatisticsResponse {
    peers: usize,
    swarms: usize,
    daily_torrents: Vec<DailyTorrentCount>,
    uptime_seconds: u64,
    traffic: crate::metrics::TrafficSnapshot,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

pub fn router(context: AppContext, enable_http_tracker: bool) -> Router {
    let app_metrics = context.metrics.clone();
    let router = Router::new()
        .route("/", get(index))
        .route("/stats", get(statistics))
        .route("/metrics", get(metrics))
        .route("/health", get(health));
    let router = if enable_http_tracker {
        router
            .route("/announce", get(announce))
            .route("/scrape", get(scrape))
    } else {
        router
    };
    router
        .with_state(context)
        .layer(middleware::from_fn_with_state(app_metrics, observe_traffic))
}

async fn observe_traffic(
    State(metrics): State<AppMetrics>,
    request: Request,
    next: Next,
) -> Response {
    let metadata_bytes = request.method().as_str().len()
        + request.uri().to_string().len()
        + request
            .headers()
            .iter()
            .map(|(name, value)| name.as_str().len() + value.as_bytes().len())
            .sum::<usize>();
    let (parts, body) = request.into_parts();
    let request_body = match to_bytes(body, 1024 * 1024).await {
        Ok(body) => body,
        Err(_) => {
            let response =
                (StatusCode::PAYLOAD_TOO_LARGE, "request body too large").into_response();
            metrics.record_traffic("http", metadata_bytes, 22);
            return response;
        }
    };
    let ingress_bytes = metadata_bytes + request_body.len();
    let response = next
        .run(Request::from_parts(parts, Body::from(request_body)))
        .await;
    let (parts, body) = response.into_parts();
    let response_body = match to_bytes(body, 8 * 1024 * 1024).await {
        Ok(body) => body,
        Err(_) => {
            metrics.record_traffic("http", ingress_bytes, 0);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "response body unavailable",
            )
                .into_response();
        }
    };
    metrics.record_traffic("http", ingress_bytes, response_body.len());
    Response::from_parts(parts, Body::from(response_body))
}

async fn announce(
    State(context): State<AppContext>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
) -> Result<Response, ApiError> {
    tracker_gate(&context, &headers, remote.ip())?;
    let params = parse_query(query.as_deref().unwrap_or_default())?;
    let info_hash = required_identifier(&params, "info_hash")?;
    let peer_id = required_identifier(&params, "peer_id")?;
    let port = required_number::<u16>(&params, "port")?;
    let left = required_number::<u64>(&params, "left")?;
    let numwant = optional_number::<usize>(&params, "numwant")?
        .unwrap_or(50)
        .min(200);
    let event = parse_event(first(&params, "event"))?;
    let peer = Peer {
        peer_id,
        ip: remote.ip(),
        port,
        left,
        last_seen: unix_timestamp(),
    };
    let stats = context.state.announce(info_hash, peer, event);
    let peers = context.state.peers(&info_hash, &peer_id, numwant);
    context.metrics.announce("http", event_name(event));
    update_population(&context);
    context.metrics.request("http", "announce", "ok");
    Ok(bencoded_response(announce_payload(
        stats,
        peers,
        remote.ip(),
        context.config.announce_interval,
    )))
}

async fn scrape(
    State(context): State<AppContext>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
) -> Result<Response, ApiError> {
    tracker_gate(&context, &headers, remote.ip())?;
    let params = parse_query(query.as_deref().unwrap_or_default())?;
    let hashes = params
        .get("info_hash")
        .ok_or_else(|| ApiError::bad_request("missing info_hash"))?
        .iter()
        .map(|value| identifier(value, "info_hash"))
        .collect::<Result<Vec<_>, _>>()?;
    context.metrics.request("http", "scrape", "ok");
    Ok(bencoded_response(scrape_payload(&context.state, hashes)))
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn statistics(
    State(context): State<AppContext>,
) -> Result<Json<StatisticsResponse>, ApiError> {
    let swarms = context.state.swarm_count();
    let daily_torrents = context
        .persistence
        .daily_torrent_counts(swarms)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?;
    Ok(Json(StatisticsResponse {
        peers: context.state.peer_count(),
        swarms,
        daily_torrents,
        uptime_seconds: context.started_at.elapsed().as_secs(),
        traffic: context.metrics.traffic_snapshot(),
    }))
}

async fn metrics(
    State(context): State<AppContext>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    authenticate(&context, &headers)?;
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

fn tracker_gate(context: &AppContext, headers: &HeaderMap, ip: IpAddr) -> Result<(), ApiError> {
    authenticate(context, headers)?;
    if !context.rate_limiter.check(ip) {
        context.metrics.request("http", "tracker", "rate_limited");
        return Err(ApiError {
            status: StatusCode::TOO_MANY_REQUESTS,
            message: "rate limit exceeded".into(),
        });
    }
    Ok(())
}

fn authenticate(context: &AppContext, headers: &HeaderMap) -> Result<(), ApiError> {
    let Some(expected) = &context.config.auth_token else {
        return Ok(());
    };
    let provided = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    let valid = provided
        .map(|value| bool::from(value.as_bytes().ct_eq(expected.as_bytes())))
        .unwrap_or(false);
    if valid {
        Ok(())
    } else {
        Err(ApiError {
            status: StatusCode::UNAUTHORIZED,
            message: "authentication required".into(),
        })
    }
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
) -> Vec<u8> {
    let compact = compact_peers(peers, requester);
    let mut output = format!(
        "d8:completei{}e10:incompletei{}e8:intervali{}e5:peers{}:",
        stats.complete,
        stats.incomplete,
        interval,
        compact.len()
    )
    .into_bytes();
    output.extend_from_slice(&compact);
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

fn compact_peers(peers: Vec<Peer>, requester: IpAddr) -> Vec<u8> {
    let mut output = Vec::new();
    for peer in peers {
        match (requester, peer.ip) {
            (IpAddr::V4(_), IpAddr::V4(ip)) => {
                output.extend_from_slice(&ip.octets());
                output.extend_from_slice(&peer.port.to_be_bytes());
            }
            (IpAddr::V6(_), IpAddr::V6(ip)) => {
                output.extend_from_slice(&ip.octets());
                output.extend_from_slice(&peer.port.to_be_bytes());
            }
            _ => {}
        }
    }
    output
}

fn bencoded_response(body: Vec<u8>) -> Response {
    let headers = [(header::CONTENT_TYPE, HeaderValue::from_static("text/plain"))];
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
        let headers = [(header::CONTENT_TYPE, HeaderValue::from_static("text/plain"))];
        (self.status, headers, Body::from(body)).into_response()
    }
}

const INDEX_HTML: &str = include_str!("../static/index.html");
#[cfg(test)]
mod tests {
    use super::*;

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
}
