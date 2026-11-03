use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use futures_util::{SinkExt, StreamExt};
use nodex_core::config::EngineConfig;
use nodex_solana::{run_logs_stream, SolanaRpc, StreamStatus};
use nodex_trading::paper_wallet::{PaperWallet, LAMPORTS_PER_SOL};
use serde::Serialize;
use serde_json::json;
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::{broadcast, RwLock};
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct AppState {
    events: broadcast::Sender<serde_json::Value>,
    stream_status: Arc<RwLock<StreamStatus>>,
    wallet: Arc<RwLock<PaperWallet>>,
    rpc: Option<SolanaRpc>,
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
    mode: &'static str,
    service: &'static str,
}

async fn health() -> Json<Health> {
    Json(Health {
        status: "ok",
        mode: "paper",
        service: "nodex-engine",
    })
}

async fn engine_status(State(state): State<AppState>) -> Json<serde_json::Value> {
    let stream = state.stream_status.read().await.clone();
    Json(json!({
        "mode": "paper",
        "live_execution_enabled": false,
        "solana_stream": stream,
    }))
}

async fn paper_wallet(State(state): State<AppState>) -> Json<PaperWallet> {
    Json(state.wallet.read().await.clone())
}

async fn token_account_rent(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let rpc = state.rpc.ok_or((
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "Solana HTTP RPC is not configured".to_string(),
    ))?;
    let rent = rpc
        .token_account_rent_lamports()
        .await
        .map_err(|error| (axum::http::StatusCode::BAD_GATEWAY, error.to_string()))?;
    Ok(Json(json!({
        "account_size_bytes": 165,
        "rent_lamports": rent,
        "rent_sol": rent as f64 / LAMPORTS_PER_SOL as f64,
        "source": "solana_getMinimumBalanceForRentExemption"
    })))
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| client_socket(socket, state))
}

async fn client_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver_socket) = socket.split();
    let mut receiver = state.events.subscribe();
    let status = state.stream_status.read().await.clone();
    let initial = json!({"type":"connection_status","data":status}).to_string();
    if sender.send(Message::Text(initial)).await.is_err() {
        return;
    }
    loop {
        tokio::select! {
            event = receiver.recv() => match event {
                Ok(value) => {
                    if sender.send(Message::Text(value.to_string())).await.is_err() {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    let warning = json!({
                        "type": "stream_warning",
                        "data": {"dropped_events": count}
                    }).to_string();
                    if sender.send(Message::Text(warning)).await.is_err() {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break,
            },
            incoming = receiver_socket.next() => match incoming {
                Some(Ok(Message::Ping(payload))) => {
                    if sender.send(Message::Pong(payload)).await.is_err() {
                        break;
                    }
                }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                _ => {}
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = EngineConfig::from_env();
    let db = nodex_persistence::connect(&config.database_url).await?;
    let wallet = match nodex_persistence::load_paper_wallet(&db).await? {
        Some(wallet) => wallet,
        None => {
            let wallet = PaperWallet::new(config.paper_starting_balance_lamports);
            nodex_persistence::save_paper_wallet(&db, &wallet).await?;
            wallet
        }
    };

    let (events, _) = broadcast::channel(2048);
    let stream_status = Arc::new(RwLock::new(StreamStatus::default()));
    let rpc = config.solana_http_url.clone().map(SolanaRpc::new);

    // Persist raw upstream log notifications independently of browser subscribers.
    let mut persistence_receiver = events.subscribe();
    let persistence_db = db.clone();
    tokio::spawn(async move {
        while let Ok(event) = persistence_receiver.recv().await {
            if let Err(error) =
                nodex_persistence::save_raw_solana_event(&persistence_db, &event).await
            {
                tracing::error!(%error, "Could not persist raw Solana event");
            }
        }
    });

    if let Some(ws_url) = config.solana_ws_url.clone() {
        let sender = events.clone();
        let status = stream_status.clone();
        let program_id = config.pump_fun_program_id.clone();
        tokio::spawn(async move {
            run_logs_stream(ws_url, program_id, sender, status).await;
        });
    } else {
        tracing::warn!("Helius not configured; set HELIUS_API_KEY or SOLANA_RPC_WS_URL");
    }

    let state = AppState {
        events,
        stream_status,
        wallet: Arc::new(RwLock::new(wallet)),
        rpc,
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/engine/status", get(engine_status))
        .route("/api/v1/paper-wallet", get(paper_wallet))
        .route("/api/v1/fees/token-account-rent", get(token_account_rent))
        .route("/api/v1/ws", get(ws_handler))
        .with_state(state)
        .layer(TraceLayer::new_for_http());

    let address: SocketAddr = config.bind_addr.parse()?;
    let listener = tokio::net::TcpListener::bind(address).await?;
    tracing::info!(%address, mode = "paper", "NodeX engine API listening");
    axum::serve(listener, app).await?;
    Ok(())
}
