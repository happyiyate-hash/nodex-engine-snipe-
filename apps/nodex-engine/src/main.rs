use axum::{routing::get, Json, Router};
use nodex_core::config::EngineConfig;
use serde::Serialize;
use std::net::SocketAddr;
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

#[derive(Serialize)]
struct Health {
    status: &'static str,
    mode: &'static str,
    service: &'static str,
}

async fn health() -> Json<Health> {
    Json(Health { status: "ok", mode: "paper", service: "nodex-engine" })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let config = EngineConfig::from_env();
    let _pool = nodex_persistence::connect(&config.database_url).await?;
    let address: SocketAddr = config.bind_addr.parse()?;
    let app = Router::new()
        .route("/health", get(health))
        .layer(TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind(address).await?;
    tracing::info!(%address, mode = "paper", "NodeX engine API listening");
    axum::serve(listener, app).await?;
    Ok(())
}
