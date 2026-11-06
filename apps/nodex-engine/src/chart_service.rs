use serde_json::{json, Value};
use std::collections::HashMap;
use tokio::sync::broadcast;

#[derive(Debug, Clone, Copy)]
struct Candle {
    open_time_ms: i64,
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
    volume_sol: f64,
    trade_count: u64,
    buys: u64,
    sells: u64,
    closed: bool,
}

impl Candle {
    fn price(open_time_ms: i64, price: f64) -> Self {
        Self { open_time_ms, open: Some(price), high: Some(price), low: Some(price),
            close: Some(price), volume_sol: 0.0, trade_count: 0, buys: 0, sells: 0, closed: false }
    }

    fn empty(open_time_ms: i64) -> Self {
        Self { open_time_ms, open: None, high: None, low: None, close: None,
            volume_sol: 0.0, trade_count: 0, buys: 0, sells: 0, closed: false }
    }

    fn json(self, mint: &str, timeframe: &str, duration_ms: i64) -> Value {
        json!({
            "mint_address": mint, "timeframe": timeframe,
            "open_time_ms": self.open_time_ms, "close_time_ms": self.open_time_ms + duration_ms,
            "open": self.open, "high": self.high, "low": self.low, "close": self.close,
            "volume_sol": self.volume_sol, "trade_count": self.trade_count,
            "buys": self.buys, "sells": self.sells, "closed": self.closed
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
            let (timestamp_ms, price, volume_sol, side) = match kind {
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
                    let previous = candles.keys()
                        .filter(|(candidate_mint, candidate_tf, candidate_open)|
                            candidate_mint == mint && candidate_tf == timeframe && *candidate_open < open_time)
                        .map(|(_, _, previous_open)| *previous_open)
                        .max();
                    if let Some(previous_open) = previous {
                        let previous_key = (mint.to_owned(), timeframe.to_owned(), previous_open);
                        if let Some(previous_candle) = candles.get_mut(&previous_key) {
                            if !previous_candle.closed {
                                previous_candle.closed = true;
                                let payload = previous_candle.json(mint, timeframe, duration);
                                if let Err(error) = nodex_persistence::save_chart_candle(
                                    &database, mint, timeframe, previous_open, &payload,
                                ).await {
                                    tracing::warn!(%error, mint_address=mint, timeframe, "Could not persist closed chart candle");
                                }
                                let _ = events.send(json!({"type":"chart_candle_closed","data":payload}));
                            }
                        }
                    }
                    let candle = candles.entry(key.clone()).or_insert_with(|| Candle::price(open_time, price));
                    candle.open.get_or_insert(price);
                    candle.high = Some(candle.high.map_or(price, |value| value.max(price)));
                    candle.low = Some(candle.low.map_or(price, |value| value.min(price)));
                    candle.close = Some(price);
                } else if let Some(side) = side {
                    let candle = candles.entry(key.clone()).or_insert_with(|| Candle::empty(open_time));
                    candle.volume_sol += volume_sol;
                    candle.trade_count = candle.trade_count.saturating_add(1);
                    if side == "buy" { candle.buys = candle.buys.saturating_add(1); }
                    else { candle.sells = candle.sells.saturating_add(1); }
                }

                if let Some(candle) = candles.get(&key).copied() {
                    let payload = candle.json(mint, timeframe, duration);
                    if let Err(error) = nodex_persistence::save_chart_candle(
                        &database, mint, timeframe, open_time, &payload,
                    ).await {
                        tracing::warn!(%error, mint_address=mint, timeframe, "Could not persist chart candle update");
                    }
                    let _ = events.send(json!({"type":"chart_candle_update","data":payload}));
                }
            }

            // Bound memory while retaining enough recent buckets for late events.
            if candles.len() > 20_000 {
                let mut keys: Vec<_> = candles.keys().cloned().collect();
                keys.sort_by_key(|(_, _, open_time)| *open_time);
                for key in keys.into_iter().take(candles.len().saturating_sub(15_000)) {
                    candles.remove(&key);
                }
            }
        }
    });
}
