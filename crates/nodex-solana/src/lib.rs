use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::broadcast;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

pub const SPL_TOKEN_ACCOUNT_SIZE: usize = 165;

#[derive(Debug, Clone)]
pub struct SolanaRpc {
    http_url: String,
    client: reqwest::Client,
}

impl SolanaRpc {
    pub fn new(http_url: impl Into<String>) -> Self {
        Self {
            http_url: http_url.into(),
            client: reqwest::Client::new(),
        }
    }

    async fn call<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: Value,
    ) -> Result<T> {
        let response = self
            .client
            .post(&self.http_url)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
            .send()
            .await
            .context("sending Solana JSON-RPC request")?
            .error_for_status()
            .context("Solana RPC returned HTTP error")?
            .json::<RpcResponse<T>>()
            .await
            .context("decoding Solana JSON-RPC response")?;
        if let Some(error) = response.error {
            return Err(anyhow!("Solana RPC {method} error: {}", error.message));
        }
        response
            .result
            .ok_or_else(|| anyhow!("Solana RPC {method} returned no result"))
    }

    /// Current rent-exempt minimum for a standard 165-byte SPL token account.
    pub async fn token_account_rent_lamports(&self) -> Result<u64> {
        self.call(
            "getMinimumBalanceForRentExemption",
            json!([SPL_TOKEN_ACCOUNT_SIZE, {"commitment":"confirmed"}]),
        )
        .await
    }

    /// Fee quote for an already compiled base64 transaction message; never submits it.
    pub async fn fee_for_message(&self, base64_message: &str) -> Result<Option<u64>> {
        self.call(
            "getFeeForMessage",
            json!([base64_message, {"commitment":"confirmed"}]),
        )
        .await
    }

    pub async fn recent_prioritization_fees(&self) -> Result<Vec<PriorityFeeSample>> {
        self.call("getRecentPrioritizationFees", json!([])).await
    }
}

#[derive(Debug, Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcError>,
}

#[derive(Debug, Deserialize)]
struct RpcError {
    message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorityFeeSample {
    pub slot: u64,
    #[serde(rename = "prioritizationFee")]
    pub prioritization_fee: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StreamStatus {
    pub connected: bool,
    pub subscription_id: Option<u64>,
    pub reconnect_count: u64,
    pub last_error: Option<String>,
}

/// Forwards raw logsNotification JSON. The mentions filter accepts exactly one pubkey.
pub async fn run_logs_stream(
    ws_url: String,
    program_id: String,
    sender: broadcast::Sender<Value>,
    status: std::sync::Arc<tokio::sync::RwLock<StreamStatus>>,
) {
    let mut reconnect_count = 0_u64;
    loop {
        if let Err(error) = connect_and_subscribe(&ws_url, &program_id, &sender, &status).await {
            warn!(%error, "Solana WebSocket stream ended; reconnecting");
            status.write().await.last_error = Some(error.to_string());
            let _ = sender.send(json!({"type":"diagnostic","data":{"severity":"error","component":"solana_websocket","message":error.to_string(),"retrying":true,"received_at":chrono::Utc::now().to_rfc3339()}}));
        }
        reconnect_count = reconnect_count.saturating_add(1);
        {
            let mut current = status.write().await;
            current.connected = false;
            current.subscription_id = None;
            current.reconnect_count = reconnect_count;
        }
        let delay = 2_u64.saturating_mul(1_u64 << reconnect_count.min(4)).min(30);
        tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
    }
}

async fn connect_and_subscribe(
    ws_url: &str,
    program_id: &str,
    sender: &broadcast::Sender<Value>,
    status: &std::sync::Arc<tokio::sync::RwLock<StreamStatus>>,
) -> Result<()> {
    let (mut socket, _) = connect_async(ws_url)
        .await
        .context("connecting to Solana WebSocket")?;
    socket
        .send(Message::Text(
            json!({
                "jsonrpc":"2.0","id":1,"method":"logsSubscribe",
                "params":[{"mentions":[program_id]},{"commitment":"confirmed"}]
            })
            .to_string()
            .into(),
        ))
        .await
        .context("sending logsSubscribe")?;

    let acknowledgement = socket
        .next()
        .await
        .ok_or_else(|| anyhow!("WebSocket closed before subscription acknowledgement"))??;
    let ack: Value = serde_json::from_str(acknowledgement.to_text()?)?;
    let subscription_id = ack
        .get("result")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("logsSubscribe rejected: {ack}"))?;
    {
        let mut current = status.write().await;
        current.connected = true;
        current.subscription_id = Some(subscription_id);
        current.last_error = None;
    }
    info!(subscription_id, program_id, "Subscribed to Solana program logs");
    let _ = sender.send(json!({"type":"provider_status","data":{"provider":"solana_websocket","connected":true,"subscription_id":subscription_id,"program_id":program_id,"received_at":chrono::Utc::now().to_rfc3339()}}));

    while let Some(message) = socket.next().await {
        match message? {
            Message::Text(text) => {
                let value: Value = serde_json::from_str(&text)?;
                if value.get("method").and_then(Value::as_str) == Some("logsNotification") {
                    let _ = sender.send(value);
                }
            }
            Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
            Message::Close(_) => break,
            _ => {}
        }
    }
    let _ = sender.send(json!({"type":"provider_status","data":{"provider":"solana_websocket","connected":false,"reason":"WebSocket stream closed","received_at":chrono::Utc::now().to_rfc3339()}}));
    Ok(())
}
