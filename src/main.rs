use std::{sync::Arc, time::Instant};

use anyhow::{Context, Result};
use hive_tracker::{
    config::AppConfig,
    metrics::AppMetrics,
    persistence::Persistence,
    rate_limit::RateLimiter,
    state::TrackerState,
    udp::UdpTracker,
    web::{router, AppContext},
};
use tokio::{net::TcpListener, time};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("hive_tracker=info")),
        )
        .init();

    let config = AppConfig::from_env()?;
    let state = Arc::new(TrackerState::default());
    let persistence = Persistence::open(&config.database_path).await?;
    persistence.load(&state).await?;
    state.remove_stale(config.peer_timeout);
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
        .with_context(|| format!("failed to bind HTTP listener at {}", config.http_addr))?;
    let udp = UdpTracker::bind(
        config.udp_addr,
        Arc::clone(&state),
        metrics,
        Arc::clone(&rate_limiter),
        config.announce_interval,
    )
    .await
    .with_context(|| format!("failed to bind UDP listener at {}", config.udp_addr))?;

    let persistence_task = tokio::spawn(periodic_maintenance(
        persistence.clone(),
        Arc::clone(&state),
        rate_limiter,
        config.persistence_interval,
        config.peer_timeout,
    ));
    info!(http = %config.http_addr, udp = %config.udp_addr, "Hive tracker started");

    let http_server = axum::serve(
        listener,
        router(context).into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal());
    tokio::select! {
        result = http_server => result.context("HTTP server failed")?,
        result = udp.run() => result.context("UDP server failed")?,
    }

    persistence_task.abort();
    persistence.save(&state).await?;
    info!("Hive tracker stopped");
    Ok(())
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
        state.remove_stale(peer_timeout);
        rate_limiter.remove_idle();
        if let Err(error) = persistence.save(&state).await {
            error!(%error, "failed to persist tracker state");
        }
    }
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        error!(%error, "failed to install shutdown signal handler");
    }
}
