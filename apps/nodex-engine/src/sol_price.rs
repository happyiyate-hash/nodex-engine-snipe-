use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolPriceSnapshot {
    pub price_usd: f64,
    pub source: String,
    pub updated_at: DateTime<Utc>,
    pub status: String,
    pub last_error: Option<String>,
}

pub fn usd_to_lamports(usd: f64, sol_price_usd: f64) -> Result<u64> {
    if !usd.is_finite() || usd <= 0.0 || !sol_price_usd.is_finite() || sol_price_usd <= 0.0 {
        return Err(anyhow!("USD amount and SOL/USD price must be finite and positive"));
    }
    let lamports = (usd / sol_price_usd) * LAMPORTS_PER_SOL;
    if !lamports.is_finite() || lamports < 1.0 || lamports > u64::MAX as f64 {
        return Err(anyhow!("USD amount cannot be represented as a positive lamport balance"));
    }
    Ok(lamports.round() as u64)
}

/// Fetch a current spot quote. Coinbase is primary; Binance is an independent
/// public fallback. Invalid or unavailable quotes are never replaced by guesses.
pub async fn fetch_sol_usd_price() -> Result<SolPriceSnapshot> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(6))
        .user_agent("NodeX-paper-engine/1.0")
        .build()
        .context("building SOL price HTTP client")?;

    if let Ok(response) = client
        .get("https://api.coinbase.com/v2/prices/SOL-USD/spot")
        .send()
        .await
    {
        if response.status().is_success() {
            if let Ok(body) = response.json::<Value>().await {
                let amount = body
                    .pointer("/data/amount")
                    .and_then(Value::as_str)
                    .and_then(|value| value.parse::<f64>().ok());
                if let Some(price) = amount.filter(|value| value.is_finite() && *value > 0.0) {
                    return Ok(SolPriceSnapshot {
                        price_usd: price,
                        source: "coinbase_spot".into(),
                        updated_at: Utc::now(),
                        status: "live".into(),
                        last_error: None,
                    });
                }
            }
        }
    }

    let response = client
        .get("https://api.binance.com/api/v3/ticker/price?symbol=SOLUSDT")
        .send()
        .await
        .context("Coinbase quote failed and Binance fallback request failed")?
        .error_for_status()
        .context("Coinbase quote failed and Binance fallback returned an HTTP error")?;
    let body: Value = response
        .json()
        .await
        .context("parsing Binance SOL/USDT price response")?;
    let price = body
        .get("price")
        .and_then(Value::as_str)
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0)
        .ok_or_else(|| anyhow!("Both SOL price providers returned an invalid quote"))?;

    Ok(SolPriceSnapshot {
        price_usd: price,
        source: "binance_spot".into(),
        updated_at: Utc::now(),
        status: "live".into(),
        last_error: None,
    })
}

pub fn start_sol_price_feed(
    events: broadcast::Sender<Value>,
    initial: SolPriceSnapshot,
) -> Arc<RwLock<SolPriceSnapshot>> {
    let shared = Arc::new(RwLock::new(initial));
    let task_state = shared.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            interval.tick().await;
            match fetch_sol_usd_price().await {
                Ok(snapshot) => {
                    *task_state.write().await = snapshot.clone();
                    let _ = events.send(serde_json::json!({
                        "type": "sol_price_update",
                        "data": snapshot
                    }));
                }
                Err(error) => {
                    let mut current = task_state.write().await;
                    current.status = "stale".into();
                    current.last_error = Some(format!("{error:#}"));
                    let snapshot = current.clone();
                    drop(current);
                    let _ = events.send(serde_json::json!({
                        "type": "sol_price_update",
                        "data": snapshot
                    }));
                    tracing::warn!(%error, "SOL/USD refresh failed; retaining last quote as stale");
                }
            }
        }
    });
    shared
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_ten_usd_to_lamports_using_live_price() {
        assert_eq!(usd_to_lamports(10.0, 200.0).unwrap(), 50_000_000);
    }

    #[test]
    fn rejects_invalid_prices_and_amounts() {
        assert!(usd_to_lamports(10.0, 0.0).is_err());
        assert!(usd_to_lamports(0.0, 200.0).is_err());
        assert!(usd_to_lamports(f64::NAN, 200.0).is_err());
    }
}
