use serde_json::{json, Value};
use std::collections::HashMap;
use tokio::sync::broadcast;

#[derive(Debug, Clone, Copy)]
struct Candle {
    open_time_ms: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume_sol: f64,
    trade_count: u64,
    buys: u64,
    sells: u64,
}

impl Candle {
    fn new(open_time_ms: i64, price: f64) -> Self {
        Self { open_time_ms, open: price, high: price, low: price, close: price,
            volume_sol: 0.0, trade_count: 0, buys: 0, sells: 0 }
    }

    fn json(self, mint: &str, timeframe: &str, duration_ms: i64, closed: bool) -> Value {
        json!({
            "mint_address": mint, "timeframe": timeframe,
            "open_time_ms": self.open_time_ms, "close_time_ms": self.open_time_ms + duration_ms,
            "open": self.open, "high": self.high, "low": self.low, "close": self.close,
            "volume_sol": self.volume_sol, "trade_count": self.trade_count,
            "buys": self.buys, "sells": self.sells, "closed": closed
        })
    }
}

const TIMEFRAMES: [(&str, i64); 5] = [
    ("1m", 60_000), ("5m", 300_000), ("15m", 900_000),
    ("1h", 3_600_000), ("24h", 86_400_000),
];

/// Independent OHLCV projector. Price and trade events are accepted in either
/// order; a late trade revises the candle for its original event timestamp.
pub fn start_chart_service(
    mut receiver: broadcast::Receiver<Value>,
    events: broadcast::Sender<Value>,
    database: sqlx::SqlitePool,
) {
    tokio::spawn(async move {
        let mut candles = HashMap::<(String, String, i64), Candle>::new();
        loop {
            let event = match receiver.recv().await {
                Ok(event) => event,
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    tracing::warn!(dropped_events=count, "Chart projector lagged; candle history may need reconciliation");
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            };
            let kind = event.get("type").and_then(Value::as_str).unwrap_or_default();
            let Some(data) = event.get("data") else { continue };
            let Some(mint) = data.get("mint_address").and_then(Value::as_str) else { continue };
            let (timestamp_ms, price, volume, side) = match kind {
                "market_price_update" => {
                    let Some(price) = data.get("price_sol").and_then(Value::as_f64)
                        .filter(|value| value.is_finite() && *value > 0.0) else { continue };
                    let timestamp = data.get("timestamp_ms").and_then(Value::as_i64)
                        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
                    (timestamp, Some(price), 0.0, None)
                }
                "pumpswap_activity" => {
                    let event_type = data.get("event_type").and_then(Value::as_str).unwrap_or_default();
                    if event_type != "buy" && event_type != "sell" { continue; }
                    if data.get("quote_amount_unit").and_then(Value::as_str) != Some("lamports") { continue; }
                    let Some(raw) = data.get("quote_amount_raw").and_then(Value::as_str)
                        .and_then(|value| value.parse::<u64>().ok()) else { continue };
                    let timestamp = data.get("timestamp").and_then(Value::as_i64)
                        .map(|seconds| seconds.saturating_mul(1_000))
                        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
                    (timestamp, None, raw as f64 / 1_000_000_000.0, Some(event_type))
                }
                _ => continue,
            };

            for (timeframe, duration) in TIMEFRAMES {
                let open_time = timestamp_ms.div_euclid(duration).saturating_mul(duration);
                let key = (mint.to_owned(), timeframe.to_owned(), open_time);
                if let Some(price) = price {
                    // Close the previous in-memory candle when a newer bucket arrives.
                    let previous = candles.keys()
                        .filter(|(candidate_mint, candidate_tf, candidate_open)|
                            candidate_mint == mint && candidate_tf == timeframe && *candidate_open < open_time)
                        .map(|(_, _, previous_open)| *previous_open)
                        .max();
                    if let Some(previous_open) = previous {
                        if let Some(previous_candle) = candles.get(&(mint.to_owned(), timeframe.to_owned(), previous_open)).copied() {
                            let closed = previous_candle.json(mint, timeframe, duration, true);
                            let _ = nodex_persistence::save_chart_candle(&database, mint, timeframe, previous_open, &closed).await;
                            let _ = events.send(json!({"type":"chart_candle_closed","data":closed}));
                            candles.remove(&(mint.to_owned(), timeframe.to_owned(), previous_open));
                        }
                    }
                    let candle = candles.entry(key.clone()).or_insert_with(|| Candle::new(open_time, price));
                    candle.high = candle.high.max(price);
                    candle.low = candle.low.min(price);
                    candle.close = price;
                } else if let Some(side) = side {
                    let candle = candles.entry(key.clone()).or_insert_with(|| Candle { open_time_ms: open_time, open: None, high: None, low: None, close: None, volume_sol: 0.0, trade_count: 0, buys: 0, sells: 0 });
                    candle.volume_sol += volume;
                    candle.trade_count = candle.trade_count.saturating_add(1);
                    if side == "buy" { candle.buys = candle.buys.saturating_add(1); }
                    else { candle.sells = candle.sells.saturating_add(1); }
                }
                if let Some(candle) = candles.get(&key).copied() {
                    let payload = candle.json(mint, timeframe, duration, false);
                    if let Err(error) = nodex_persistence::save_chart_candle(&database, mint, timeframe, open_time, &payload).await {
                        tracing::warn!(%error, mint_address=mint, timeframe, "Could not persist active chart candle");
                    }
                    let _ = events.send(json!({
                        "type":"chart_candle_update",
                        "data":payload
                    }));
                }
            }
        }
    });
}
