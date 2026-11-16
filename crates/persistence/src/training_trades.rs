use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};

/// One confirmed on-chain trade retained as permanent training evidence.
/// Amounts remain in raw chain units; decimals and USD conversion are applied
/// later using the token/quote metadata that was valid for the event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrainingTradeEvent {
    pub event_key: String,
    pub signature: String,
    pub mint_address: String,
    pub venue: String,
    pub trader_address: String,
    pub side: String,
    pub quote_amount_raw: u64,
    pub base_amount_raw: u64,
    pub quote_mint: Option<String>,
    pub slot: Option<u64>,
    pub timestamp_ms: i64,
    pub evidence_json: String,
}

/// Creates the append-only trade evidence store. Replayed stream messages are
/// idempotent by event_key; original rows are never overwritten or pruned.
pub async fn initialize(pool: &SqlitePool) -> Result<()> {
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS training_trade_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_key TEXT NOT NULL UNIQUE,
            signature TEXT NOT NULL,
            mint_address TEXT NOT NULL,
            creator_address TEXT,
            venue TEXT NOT NULL,
            trader_address TEXT NOT NULL,
            side TEXT NOT NULL CHECK(side IN ('buy', 'sell')),
            quote_amount_raw INTEGER NOT NULL,
            base_amount_raw INTEGER NOT NULL,
            quote_mint TEXT,
            slot INTEGER,
            timestamp_ms INTEGER NOT NULL,
            evidence_json TEXT NOT NULL DEFAULT '{}',
            recorded_at TEXT NOT NULL
        )"#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_training_trades_mint_time ON training_trade_events(mint_address, timestamp_ms)",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_training_trades_creator_time ON training_trade_events(creator_address, timestamp_ms)",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_training_trades_trader_time ON training_trade_events(trader_address, timestamp_ms)",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Appends a verified trade and snapshots the creator relationship currently
/// recorded for the mint. A duplicate event_key returns false without mutation.
pub async fn record_trade_event(
    pool: &SqlitePool,
    event: &TrainingTradeEvent,
) -> Result<bool> {
    let side = event.side.trim().to_ascii_lowercase();
    if event.event_key.trim().is_empty()
        || event.signature.trim().is_empty()
        || event.mint_address.trim().is_empty()
        || event.trader_address.trim().is_empty()
        || event.venue.trim().is_empty()
        || !matches!(side.as_str(), "buy" | "sell")
        || event.quote_amount_raw == 0
        || event.base_amount_raw == 0
        || event.timestamp_ms <= 0
    {
        return Err(anyhow!("confirmed trade event is missing valid identity or amount fields"));
    }

    let result = sqlx::query(
        r#"INSERT INTO training_trade_events (
            event_key, signature, mint_address, creator_address, venue,
            trader_address, side, quote_amount_raw, base_amount_raw,
            quote_mint, slot, timestamp_ms, evidence_json, recorded_at
        )
        VALUES (?, ?, ?, (SELECT creator_address FROM training_tokens WHERE mint_address = ?),
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_key) DO NOTHING"#,
    )
    .bind(event.event_key.trim())
    .bind(event.signature.trim())
    .bind(event.mint_address.trim())
    .bind(event.mint_address.trim())
    .bind(event.venue.trim())
    .bind(event.trader_address.trim())
    .bind(side)
    .bind(event.quote_amount_raw as i64)
    .bind(event.base_amount_raw as i64)
    .bind(event.quote_mint.as_deref())
    .bind(event.slot.map(|value| value as i64))
    .bind(event.timestamp_ms)
    .bind(&event.evidence_json)
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(pool)
    .await?;

    // A zero-row insert can mean either a replay or a launch not yet associated
    // with a creator. Do not silently lose training evidence for the latter:
    // retrying after creator association is safe because the event key is unique.
    if result.rows_affected() == 0 {
        let known_token = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM training_tokens WHERE mint_address = ?",
        )
        .bind(event.mint_address.trim())
        .fetch_one(pool)
        .await?;
        if known_token == 0 {
            return Ok(false);
        }
    }
    Ok(result.rows_affected() == 1)
}

/// Read durable trade history in chronological order for one token.
pub async fn list_token_trades(
    pool: &SqlitePool,
    mint_address: &str,
    limit: u32,
) -> Result<Vec<TrainingTradeEvent>> {
    let rows = sqlx::query(
        r#"SELECT event_key, signature, mint_address, venue, trader_address,
            side, quote_amount_raw, base_amount_raw, quote_mint, slot,
            timestamp_ms, evidence_json
        FROM training_trade_events
        WHERE mint_address = ?
        ORDER BY timestamp_ms ASC, id ASC
        LIMIT ?"#,
    )
    .bind(mint_address.trim())
    .bind(limit.clamp(1, 100_000) as i64)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let quote_amount_raw: i64 = row.try_get("quote_amount_raw")?;
            let base_amount_raw: i64 = row.try_get("base_amount_raw")?;
            let slot: Option<i64> = row.try_get("slot")?;
            Ok(TrainingTradeEvent {
                event_key: row.try_get("event_key")?,
                signature: row.try_get("signature")?,
                mint_address: row.try_get("mint_address")?,
                venue: row.try_get("venue")?,
                trader_address: row.try_get("trader_address")?,
                side: row.try_get("side")?,
                quote_amount_raw: u64::try_from(quote_amount_raw)
                    .map_err(|_| anyhow!("stored quote amount is negative"))?,
                base_amount_raw: u64::try_from(base_amount_raw)
                    .map_err(|_| anyhow!("stored base amount is negative"))?,
                quote_mint: row.try_get("quote_mint")?,
                slot: slot.map(|value| value as u64),
                timestamp_ms: row.try_get("timestamp_ms")?,
                evidence_json: row.try_get("evidence_json")?,
            })
        })
        .collect()
}
