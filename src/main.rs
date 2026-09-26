use std::{path::PathBuf, sync::Arc, time::Instant};

use anyhow::{ensure, Context, Result};
use clap::Parser;
use hive_tracker::{
    config::{AppConfig, Protocol},
    metrics::AppMetrics,
    persistence::Persistence,
    rate_limit::RateLimiter,
    state::TrackerState,
    udp::UdpTracker,
    web::{router, AppContext, ScrapeCache},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    time,
};
use tracing::{debug, error, info};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "hive-tracker",
    about = "Minimal HTTP and UDP BitTorrent tracker"
)]
struct Cli {
    #[arg(long, help = "Check the local HTTP health endpoint and exit")]
    health_check: bool,

    #[arg(long, value_enum, help = "Protocol listener to run")]
    protocol: Option<Protocol>,

    #[arg(long, default_value = "hive.toml", help = "Configuration file path")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if cli.health_check {
        return health_check().await;
    }
    let config = AppConfig::from_file(&cli.config)?;
    let log_filter = EnvFilter::try_new(&config.log_filter)
        .with_context(|| format!("invalid log_filter: {}", config.log_filter))?;
    tracing_subscriber::fmt().with_env_filter(log_filter).init();

    let protocol = resolve_protocol(cli.protocol, config.default_protocol);
    let state = Arc::new(TrackerState::default());
    let persistence = Persistence::open(&config.database_path).await?;
    persistence.load(&state).await?;
    state.remove_stale(config.peer_timeout);
    let metrics = AppMetrics::new()?;
    metrics.set_population(state.peer_count(), state.swarm_count());
    let started_at = Instant::now();
    persistence
        .record_dashboard_snapshot(state.summary(), 0, metrics.traffic_snapshot())
        .await?;
    let rate_limiter = Arc::new(RateLimiter::per_minute(config.rate_limit_per_minute));

    let context = AppContext {
        config: config.clone(),
        protocol,
        state: Arc::clone(&state),
        persistence: persistence.clone(),
        metrics: metrics.clone(),
        rate_limiter: Arc::clone(&rate_limiter),
        scrape_cache: Arc::new(ScrapeCache::default()),
        started_at,
    };
    let listener = TcpListener::bind(config.http_addr)
        .await
        .with_context(|| format!("failed to bind web listener at {}", config.http_addr))?;
    let udp = if matches!(protocol, Protocol::Udp | Protocol::Both) {
        Some(
            UdpTracker::bind(
                config.udp_addr,
                Arc::clone(&state),
                metrics.clone(),
                Arc::clone(&rate_limiter),
                config.announce_interval,
                config.enable_udp_scrape,
            )
            .await
            .with_context(|| format!("failed to bind UDP listener at {}", config.udp_addr))?,
        )
    } else {
        None
    };

    let persistence_task = tokio::spawn(periodic_maintenance(
        persistence.clone(),
        Arc::clone(&state),
        rate_limiter,
        config.persistence_interval,
        config.peer_timeout,
        metrics.clone(),
        started_at,
    ));
    let traffic_log_task = tokio::spawn(log_traffic(metrics.clone()));
    info!(
        ?protocol,
        web_addr = %config.http_addr,
        udp_addr = %config.udp_addr,
        database = %config.database_path.display(),
        max_concurrent_http_requests = config.max_concurrent_http_requests,
        "Hive tracker started"
    );
    run_protocols(listener, udp, context, protocol).await?;

    persistence_task.abort();
    traffic_log_task.abort();
    persistence.save(&state).await?;
    persistence
        .record_dashboard_snapshot(
            state.summary(),
            started_at.elapsed().as_secs(),
            metrics.traffic_snapshot(),
        )
        .await?;
    info!("Hive tracker stopped");
    Ok(())
}

async fn health_check() -> Result<()> {
    let result = time::timeout(std::time::Duration::from_secs(3), async {
        let mut stream = TcpStream::connect("127.0.0.1:3000")
            .await
            .context("failed to connect to Hive health endpoint")?;
        stream
            .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .context("failed to send Hive health request")?;
        let mut response = Vec::with_capacity(128);
        stream
            .take(128)
            .read_to_end(&mut response)
            .await
            .context("failed to read Hive health response")?;
        ensure!(
            health_response_is_ok(&response),
            "Hive health endpoint returned an unhealthy response"
        );
        Ok(())
    })
    .await
    .context("Hive health check timed out")?;
    result
}

fn health_response_is_ok(response: &[u8]) -> bool {
    response
        .split(|byte| *byte == b'\n')
        .next()
        .map(|status| status.ends_with(b" 200 OK\r") || status.ends_with(b" 200 OK"))
        .unwrap_or(false)
}

async fn log_traffic(metrics: AppMetrics) {
    let mut ticker = time::interval(std::time::Duration::from_secs(60));
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let traffic = metrics.traffic_snapshot();
        info!(
            requests_total = traffic.total_requests,
            ingress_bytes_total = traffic.total_ingress_bytes,
            egress_bytes_total = traffic.total_egress_bytes,
            torrent_http_requests_per_minute = traffic.torrent_http.requests_per_minute,
            web_http_requests_per_minute = traffic.web_http.requests_per_minute,
            udp_requests_per_minute = traffic.udp.requests_per_minute,
            torrent_http_ingress_bytes_per_minute = traffic.torrent_http.ingress_bytes,
            torrent_http_egress_bytes_per_minute = traffic.torrent_http.egress_bytes,
            web_http_ingress_bytes_per_minute = traffic.web_http.ingress_bytes,
            web_http_egress_bytes_per_minute = traffic.web_http.egress_bytes,
            udp_ingress_bytes_per_minute = traffic.udp.ingress_bytes,
            udp_egress_bytes_per_minute = traffic.udp.egress_bytes,
            "traffic summary"
        );
    }
}

fn resolve_protocol(command_line: Option<Protocol>, configured: Protocol) -> Protocol {
    command_line.unwrap_or(configured)
}

async fn run_protocols(
    listener: TcpListener,
    udp: Option<UdpTracker>,
    context: AppContext,
    protocol: Protocol,
) -> Result<()> {
    let (shutdown_sender, shutdown_receiver) = watch::channel(false);
    let http_shutdown = shutdown_receiver.clone();
    let udp_shutdown = shutdown_receiver;
    let http_server = async {
        axum::serve(
            listener,
            router(context, matches!(protocol, Protocol::Http | Protocol::Both))
                .into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .with_graceful_shutdown(wait_for_shutdown(http_shutdown))
        .await
        .context("web server failed")
    };
    let udp_server = async {
        let Some(udp) = udp else {
            wait_for_shutdown(udp_shutdown).await;
            return Ok(());
        };
        udp.run(udp_shutdown).await.context("UDP server failed")
    };

    tokio::pin!(http_server, udp_server);

    tokio::select! {
        result = &mut http_server => {
            shutdown_sender.send_replace(true);
            let udp_result = udp_server.await;
            result?;
            udp_result
        },
        result = &mut udp_server => {
            shutdown_sender.send_replace(true);
            let http_result = http_server.await;
            result?;
            http_result
        },
        _ = shutdown_signal() => {
            info!("shutdown requested; draining active requests");
            shutdown_sender.send_replace(true);
            let (http_result, udp_result) = tokio::join!(http_server, udp_server);
            http_result?;
            udp_result
        },
    }
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if !*shutdown.borrow() {
        let _ = shutdown.changed().await;
    }
}

async fn periodic_maintenance(
    persistence: Persistence,
    state: Arc<TrackerState>,
    rate_limiter: Arc<RateLimiter>,
    interval: std::time::Duration,
    peer_timeout: std::time::Duration,
    metrics: AppMetrics,
    started_at: Instant,
) {
    let mut ticker = time::interval(interval);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let peers_before = state.peer_count();
        let cleanup_state = Arc::clone(&state);
        let cleanup_rate_limiter = Arc::clone(&rate_limiter);
        if let Err(error) = tokio::task::spawn_blocking(move || {
            cleanup_state.remove_stale(peer_timeout);
            cleanup_rate_limiter.remove_idle();
        })
        .await
        {
            error!(%error, "tracker cleanup task failed");
            continue;
        }
        if let Err(error) = persistence.save(&state).await {
            error!(%error, "failed to persist tracker state");
        } else if let Err(error) = persistence
            .record_dashboard_snapshot(
                state.summary(),
                started_at.elapsed().as_secs(),
                metrics.traffic_snapshot(),
            )
            .await
        {
            error!(%error, "failed to persist dashboard metrics");
        } else {
            debug!(
                peers = state.peer_count(),
                removed_peers = peers_before.saturating_sub(state.peer_count()),
                swarms = state.swarm_count(),
                "periodic maintenance completed"
            );
        }
    }
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};

    let mut terminate = match signal(SignalKind::terminate()) {
        Ok(signal) => signal,
        Err(error) => {
            error!(%error, "failed to install SIGTERM handler");
            return;
        }
    };

    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            if let Err(error) = result {
                error!(%error, "failed to install Ctrl+C handler");
            }
        }
        _ = terminate.recv() => {}
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        error!(%error, "failed to install Ctrl+C handler");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_cli_protocol_when_resolved_then_it_overrides_configuration() {
        let protocol = resolve_protocol(Some(Protocol::Udp), Protocol::Http);

        assert_eq!(protocol, Protocol::Udp);
    }

    #[test]
    fn given_no_cli_protocol_when_resolved_then_configuration_is_used() {
        let protocol = resolve_protocol(None, Protocol::Both);

        assert_eq!(protocol, Protocol::Both);
    }

    #[test]
    fn given_http_health_response_when_checked_then_only_success_status_is_accepted() {
        assert!(health_response_is_ok(
            b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok"
        ));
        assert!(!health_response_is_ok(
            b"HTTP/1.1 503 Service Unavailable\r\n\r\n"
        ));
        assert!(!health_response_is_ok(b"not HTTP"));
    }
}
