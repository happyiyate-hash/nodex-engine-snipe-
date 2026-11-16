use nodex_core::TokenRegistry;
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
    first_trade_order: Option<(i64, u64)>,
    last_trade_order: Option<(i64, u64)>,
}

impl Candle {
    fn price(open_time_ms: i64, price: f64) -> Self {
        Self {
            open_time_ms,
            open: Some(price),
            high: Some(price),
            low: Some(price),
            close: Some(price),
            volume_sol: 0.0,
            trade_count: 0,
            buys: 0,
            sells: 0,
            closed: false,
            first_trade_order: None,
            last_trade_order: None,
        }
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

const TIMEFRAMES: [(&str, i64); 6] = [
    ("1m", 60_000),
    ("5m", 300_000),
    ("15m", 900_000),
    ("30m", 1_800_000),
    ("1h", 3_600_000),
    ("24h", 86_400_000),
];

/// Independent OHLCV projector. Price and trade events are accepted in either
/// order; a late trade revises the candle for its original event timestamp.
pub fn start_chart_service(
    mut receiver: broadcast::Receiver<Value>,
    events: broadcast::Sender<Value>,
    database: sqlx::SqlitePool,
    registry: TokenRegistry,
) {
    tokio::spawn(async move {
        let mut candles = HashMap::<(String, String, i64), Candle>::new();
        let mut last_persisted = HashMap::<(String, String, i64), std::time::Instant>::new();
        loop {
            let event = match receiver.recv().await {
                Ok(event) => event,
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    tracing::warn!(
                        dropped_events = count,
                        "Chart projector lagged; candle history may need reconciliation"
                    );
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            };
            let kind = event
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let Some(data) = event.get("data") else {
                continue;
            };
            let Some(mint) = data.get("mint_address").and_then(Value::as_str) else {
                continue;
            };
            let approved = registry.get(mint).is_some_and(|state| {
                state
                    .data
                    .pointer("/security/decision")
                    .and_then(Value::as_str)
                    == Some("APPROVED")
            });
            if !approved {
                continue;
            }
            // Only normalized confirmed swaps may create or update OHLC.
            // Market-account and market-cap ticks are never chart trades.
            if kind != "confirmed_trade_update" {
                continue;
            }
            let Some(price) = data.get("price_usd").and_then(Value::as_f64)
                .filter(|value| value.is_finite() && *value > 0.0) else { continue; };
            let timestamp_ms = data.get("timestamp_ms").and_then(Value::as_i64)
                .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
            let volume_sol = data.get("volume_sol").and_then(Value::as_f64)
                .filter(|value| value.is_finite() && *value > 0.0).unwrap_or(0.0);
            let side = data.get("side").and_then(Value::as_str).unwrap_or_default();
            if side != "buy" && side != "sell" { continue; }
            let event_index = data.get("event_index").and_then(Value::as_u64).unwrap_or(0);
            let trade_order = (timestamp_ms, event_index);

            for (timeframe, duration) in TIMEFRAMES {
                let open_time = timestamp_ms.div_euclid(duration).saturating_mul(duration);
                let key = (mint.to_owned(), timeframe.to_owned(), open_time);

                let previous = candles.keys()
                    .filter(|(candidate_mint, candidate_tf, candidate_open)| {
                        candidate_mint == mint && candidate_tf == timeframe && *candidate_open < open_time
                    })
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
                                tracing::warn!(%error, mint_address = mint, timeframe, "Could not persist closed chart candle");
                            }
                            let _ = events.send(json!({"type":"chart_candle_closed","data":payload}));
                        }
                    }
                }
                let candle = candles.entry(key.clone())
                    .or_insert_with(|| Candle::price(open_time, price));
                candle.high = Some(candle.high.map_or(price, |value| value.max(price)));
                candle.low = Some(candle.low.map_or(price, |value| value.min(price)));
                if candle.first_trade_order.is_none_or(|first| trade_order < first) {
                    candle.open = Some(price);
                    candle.first_trade_order = Some(trade_order);
                }
                if candle.last_trade_order.is_none_or(|last| trade_order > last) {
                    candle.close = Some(price);
                    candle.last_trade_order = Some(trade_order);
                }
                candle.volume_sol += volume_sol;
                candle.trade_count = candle.trade_count.saturating_add(1);
                if side == "buy" {
                    candle.buys = candle.buys.saturating_add(1);
                } else {
                    candle.sells = candle.sells.saturating_add(1);
                }

                if let Some(candle) = candles.get(&key).copied() {
                    let payload = candle.json(mint, timeframe, duration);
                    let should_persist = candle.closed
                        || last_persisted
                            .get(&key)
                            .is_none_or(|last| last.elapsed() >= std::time::Duration::from_secs(5));
                    if should_persist {
                        if let Err(error) = nodex_persistence::save_chart_candle(
                            &database, mint, timeframe, open_time, &payload,
                        )
                        .await
                        {
                            tracing::warn!(
                                %error,
                                mint_address = mint,
                                timeframe,
                                "Could not persist chart candle update"
                            );
                        } else {
                            last_persisted.insert(key.clone(), std::time::Instant::now());
                        }
                    }
                    let _ = events.send(json!({
                        "type": "chart_candle_update",
                        "data": payload
                    }));
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
