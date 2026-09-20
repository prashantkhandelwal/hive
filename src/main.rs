use std::{path::PathBuf, sync::Arc, time::Instant};

use anyhow::{Context, Result};
use clap::Parser;
use hive_tracker::{
    config::{AppConfig, Protocol},
    metrics::AppMetrics,
    persistence::Persistence,
    rate_limit::RateLimiter,
    state::TrackerState,
    udp::UdpTracker,
    web::{router, AppContext},
};
use tokio::{net::TcpListener, time};
use tracing::{debug, error, info};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "hive-tracker",
    about = "Minimal HTTP and UDP BitTorrent tracker"
)]
struct Cli {
    #[arg(long, value_enum, help = "Protocol listener to run")]
    protocol: Option<Protocol>,

    #[arg(long, default_value = "hive.toml", help = "Configuration file path")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = AppConfig::from_file(&cli.config)?;
    let log_filter = EnvFilter::try_new(&config.log_filter)
        .with_context(|| format!("invalid log_filter: {}", config.log_filter))?;
    tracing_subscriber::fmt().with_env_filter(log_filter).init();

    let protocol = resolve_protocol(cli.protocol, config.default_protocol);
    let state = Arc::new(TrackerState::default());
    let persistence = Persistence::open(&config.database_path).await?;
    persistence.load(&state).await?;
    state.remove_stale(config.peer_timeout);
    persistence
        .record_daily_torrent_count(state.torrent_count())
        .await?;
    let metrics = AppMetrics::new()?;
    metrics.set_population(state.peer_count(), state.swarm_count());
    let rate_limiter = Arc::new(RateLimiter::per_minute(config.rate_limit_per_minute));

    let context = AppContext {
        config: config.clone(),
        state: Arc::clone(&state),
        persistence: persistence.clone(),
        metrics: metrics.clone(),
        rate_limiter: Arc::clone(&rate_limiter),
        started_at: Instant::now(),
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
    ));
    let traffic_log_task = tokio::spawn(log_traffic(metrics));
    info!(
        ?protocol,
        web_addr = %config.http_addr,
        udp_addr = %config.udp_addr,
        database = %config.database_path.display(),
        "Hive tracker started"
    );
    run_protocols(listener, udp, context, protocol).await?;

    persistence_task.abort();
    traffic_log_task.abort();
    persistence.save(&state).await?;
    info!("Hive tracker stopped");
    Ok(())
}

async fn log_traffic(metrics: AppMetrics) {
    let mut ticker = time::interval(std::time::Duration::from_secs(60));
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let traffic = metrics.traffic_snapshot();
        info!(
            ingress_bytes_total = traffic.total_ingress_bytes,
            egress_bytes_total = traffic.total_egress_bytes,
            http_requests_per_minute = traffic.http.requests_per_minute,
            udp_requests_per_minute = traffic.udp.requests_per_minute,
            http_ingress_bytes_per_minute = traffic.http.ingress_bytes,
            http_egress_bytes_per_minute = traffic.http.egress_bytes,
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
    let http_server = async {
        axum::serve(
            listener,
            router(context, matches!(protocol, Protocol::Http | Protocol::Both))
                .into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .context("web server failed")
    };
    let udp_server = async {
        let Some(udp) = udp else {
            return std::future::pending::<Result<()>>().await;
        };
        udp.run().await.context("UDP server failed")
    };

    tokio::select! {
        result = http_server => result,
        result = udp_server => result,
        _ = shutdown_signal() => Ok(()),
    }
}

async fn periodic_maintenance(
    persistence: Persistence,
    state: Arc<TrackerState>,
    rate_limiter: Arc<RateLimiter>,
    interval: std::time::Duration,
    peer_timeout: std::time::Duration,
) {
    let mut ticker = time::interval(interval);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let peers_before = state.peer_count();
        state.remove_stale(peer_timeout);
        rate_limiter.remove_idle();
        if let Err(error) = persistence.save(&state).await {
            error!(%error, "failed to persist tracker state");
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

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        error!(%error, "failed to install shutdown signal handler");
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
}
