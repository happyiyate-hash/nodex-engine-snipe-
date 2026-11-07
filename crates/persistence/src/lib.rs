use anyhow::Result;
use nodex_core::{TokenLaunch, TokenState};
use nodex_trading::paper_wallet::PaperWallet;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
    Row, SqlitePool,
};
use std::str::FromStr;

pub async fn connect(database_url: &str) -> Result<SqlitePool> {
    let options = SqliteConnectOptions::from_str(database_url)?
        .create_if_missing(true)
        // Live ingestion and maintenance can contend for SQLite's single writer.
        // Wait for short-lived locks instead of failing immediately.
        .busy_timeout(std::time::Duration::from_secs(10))
        // WAL lets readers continue while a writer commits. SQLite still
        // serializes writers, so the busy timeout remains important.
        .journal_mode(SqliteJournalMode::Wal);
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await?;
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS token_launches (
            id TEXT PRIMARY KEY NOT NULL, mint_address TEXT NOT NULL, creator_address TEXT,
            source TEXT NOT NULL, signature TEXT NOT NULL UNIQUE, slot INTEGER NOT NULL,
            detected_at TEXT NOT NULL, payload_json TEXT NOT NULL
        )"#,
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS paper_wallets (
            id TEXT PRIMARY KEY NOT NULL, starting_balance_lamports INTEGER NOT NULL,
            available_balance_lamports INTEGER NOT NULL, invested_balance_lamports INTEGER NOT NULL,
            locked_rent_lamports INTEGER NOT NULL, total_network_fees_lamports INTEGER NOT NULL,
            total_priority_fees_lamports INTEGER NOT NULL, total_swap_fees_lamports INTEGER NOT NULL,
            total_rent_refunded_lamports INTEGER NOT NULL, updated_at TEXT NOT NULL
        )"#,
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS paper_ledger (
            id TEXT PRIMARY KEY NOT NULL, kind TEXT NOT NULL, amount_lamports INTEGER NOT NULL,
            available_after_lamports INTEGER NOT NULL, reference TEXT, created_at TEXT NOT NULL
        )"#,
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS solana_raw_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, signature TEXT, slot INTEGER,
            event_hash TEXT, event_json TEXT NOT NULL, received_at TEXT NOT NULL
        )"#,
    )
    .execute(&pool)
    .await?;
    // Existing databases predate event_hash. Add it without dropping historical data.
    let raw_event_columns = sqlx::query("PRAGMA table_info(solana_raw_events)")
        .fetch_all(&pool)
        .await?;
    let has_event_hash = raw_event_columns
        .iter()
        .any(|row| row.get::<String, _>("name") == "event_hash");
    if !has_event_hash {
        sqlx::query("ALTER TABLE solana_raw_events ADD COLUMN event_hash TEXT")
            .execute(&pool)
            .await?;
    }
    sqlx::query(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_solana_raw_events_event_hash ON solana_raw_events(event_hash) WHERE event_hash IS NOT NULL",
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_solana_raw_events_received_at ON solana_raw_events(received_at)",
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS profiles (
            id TEXT PRIMARY KEY NOT NULL,
            payload_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )"#,
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS portfolio_positions (
            position_id TEXT PRIMARY KEY NOT NULL,
            mint_address TEXT NOT NULL,
            status TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )"#,
    )
    .execute(&pool)
    .await?;

    // Upgrade the original one-position-per-mint table without losing existing
    // paper positions. Legacy rows receive stable IDs and can coexist with future lots.
    let position_columns = sqlx::query("PRAGMA table_info(portfolio_positions)")
        .fetch_all(&pool)
        .await?;
    let has_position_id = position_columns
        .iter()
        .any(|row| row.get::<String, _>("name") == "position_id");
    if !has_position_id {
        sqlx::query("ALTER TABLE portfolio_positions RENAME TO portfolio_positions_legacy")
            .execute(&pool)
            .await?;
        sqlx::query(
            r#"CREATE TABLE portfolio_positions (
                position_id TEXT PRIMARY KEY NOT NULL,
                mint_address TEXT NOT NULL,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )"#,
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            r#"INSERT INTO portfolio_positions
                (position_id, mint_address, status, payload_json, updated_at)
            SELECT 'legacy-' || mint_address, mint_address, status, payload_json, updated_at
            FROM portfolio_positions_legacy"#,
        )
        .execute(&pool)
        .await?;
        sqlx::query("DROP TABLE portfolio_positions_legacy")
            .execute(&pool)
            .await?;
    }
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_portfolio_positions_mint_status ON portfolio_positions(mint_address, status)",
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS token_states (
            mint_address TEXT PRIMARY KEY NOT NULL,
            version INTEGER NOT NULL,
            updated_at TEXT NOT NULL,
            payload_json TEXT NOT NULL
        )"#,
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS chart_candles (
            mint_address TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            open_time_ms INTEGER NOT NULL,
            payload_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (mint_address, timeframe, open_time_ms)
        )"#,
    )
    .execute(&pool)
    .await?;
    Ok(pool)
}

/// Persist the assembled profile snapshot independently from the paper-trading wallet.
pub async fn save_profile(pool: &SqlitePool, profile: &serde_json::Value) -> Result<()> {
    sqlx::query(
        r#"INSERT INTO profiles (id, payload_json, updated_at) VALUES ('default', ?, ?)
        ON CONFLICT(id) DO UPDATE SET payload_json=excluded.payload_json, updated_at=excluded.updated_at"#,
    )
    .bind(serde_json::to_string(profile)?)
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn load_profile(pool: &SqlitePool) -> Result<Option<serde_json::Value>> {
    let payload =
        sqlx::query_scalar::<_, String>("SELECT payload_json FROM profiles WHERE id='default'")
            .fetch_optional(pool)
            .await?;
    payload
        .map(|value| serde_json::from_str(&value).map_err(Into::into))
        .transpose()
}

/// Store a paper position by its unique lot ID. Multiple positions may share a mint.
pub async fn save_portfolio_position(
    pool: &SqlitePool,
    position_id: &str,
    mint: &str,
    status: &str,
    payload: &serde_json::Value,
) -> Result<()> {
    sqlx::query(
        r#"INSERT INTO portfolio_positions
            (position_id, mint_address, status, payload_json, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(position_id) DO UPDATE SET
            mint_address=excluded.mint_address,
            status=excluded.status,
            payload_json=excluded.payload_json,
            updated_at=excluded.updated_at"#,
    )
    .bind(position_id)
    .bind(mint)
    .bind(status)
    .bind(serde_json::to_string(payload)?)
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn load_portfolio_positions(pool: &SqlitePool) -> Result<Vec<serde_json::Value>> {
    let rows = sqlx::query(
        "SELECT position_id, mint_address, status, payload_json FROM portfolio_positions ORDER BY updated_at DESC",
    )
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|row| {
            let position_id: String = row.get("position_id");
            let mint_address: String = row.get("mint_address");
            let status: String = row.get("status");
            let payload: String = row.get("payload_json");
            let mut value: serde_json::Value = serde_json::from_str(&payload)?;
            if let Some(object) = value.as_object_mut() {
                object.insert("position_id".into(), serde_json::Value::String(position_id));
                object.insert(
                    "mint_address".into(),
                    serde_json::Value::String(mint_address),
                );
                object.insert("status".into(), serde_json::Value::String(status));
            }
            Ok(value)
        })
        .collect()
}

pub async fn save_launch(pool: &SqlitePool, launch: &TokenLaunch) -> Result<bool> {
    let payload = serde_json::to_string(launch)?;
    let result = sqlx::query(
        r#"INSERT OR IGNORE INTO token_launches
        (id, mint_address, creator_address, source, signature, slot, detected_at, payload_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)"#,
    )
    .bind(launch.id.to_string())
    .bind(&launch.mint_address)
    .bind(&launch.creator_address)
    .bind("pump_fun")
    .bind(&launch.signature)
    .bind(launch.slot as i64)
    .bind(launch.detected_at.to_rfc3339())
    .bind(payload)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Persist the latest canonical state for a token. Older versions cannot
/// overwrite newer state if independent workers finish writes out of order.
pub async fn save_token_state(pool: &SqlitePool, state: &TokenState) -> Result<()> {
    let payload = serde_json::to_string(state)?;
    sqlx::query(
        r#"INSERT INTO token_states (mint_address, version, updated_at, payload_json)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(mint_address) DO UPDATE SET
            version=excluded.version,
            updated_at=excluded.updated_at,
            payload_json=excluded.payload_json
        WHERE excluded.version >= token_states.version"#,
    )
    .bind(&state.mint_address)
    .bind(state.version as i64)
    .bind(state.updated_at.to_rfc3339())
    .bind(payload)
    .execute(pool)
    .await?;
    Ok(())
}

/// Restore persisted token snapshots during engine startup.
pub async fn load_token_states(pool: &SqlitePool) -> Result<Vec<TokenState>> {
    let rows = sqlx::query_scalar::<_, String>(
        "SELECT payload_json FROM token_states ORDER BY updated_at DESC",
    )
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|payload| serde_json::from_str(&payload).map_err(Into::into))
        .collect()
}

/// Load one token's latest canonical snapshot.
/// Delete rejected token state from local storage.
pub async fn delete_token_state(pool: &SqlitePool, mint: &str) -> Result<()> {
    sqlx::query("DELETE FROM token_states WHERE mint_address = ?")
        .bind(mint)
        .execute(pool)
        .await?;
    sqlx::query("DELETE FROM chart_candles WHERE mint_address = ?")
        .bind(mint)
        .execute(pool)
        .await?;
    Ok(())
}

/// Delete launch-history rows for a rejected token.
pub async fn delete_token_launches(pool: &SqlitePool, mint: &str) -> Result<()> {
    sqlx::query("DELETE FROM token_launches WHERE mint_address = ?")
        .bind(mint)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn get_token_state(pool: &SqlitePool, mint: &str) -> Result<Option<TokenState>> {
    let payload = sqlx::query_scalar::<_, String>(
        "SELECT payload_json FROM token_states WHERE mint_address = ?",
    )
    .bind(mint)
    .fetch_optional(pool)
    .await?;
    payload
        .map(|value| serde_json::from_str(&value).map_err(Into::into))
        .transpose()
}

/// Returns the newest persisted backend events, newest first.
pub async fn list_recent_events(pool: &SqlitePool, limit: u32) -> Result<Vec<serde_json::Value>> {
    let safe_limit = limit.clamp(1, 500) as i64;
    let rows = sqlx::query(
        "SELECT id, signature, slot, event_json, received_at FROM solana_raw_events ORDER BY id DESC LIMIT ?",
    )
    .bind(safe_limit)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let event_json: String = row.get("event_json");
            let event: serde_json::Value = serde_json::from_str(&event_json)?;
            Ok(serde_json::json!({
                "id": row.get::<i64, _>("id"),
                "signature": row.get::<Option<String>, _>("signature"),
                "slot": row.get::<Option<i64>, _>("slot"),
                "received_at": row.get::<String, _>("received_at"),
                "event": event
            }))
        })
        .collect()
}

/// Clears only historical launch and raw upstream notification rows.
/// It does not alter wallets, ledger entries, schema, or live monitor state.
pub async fn clear_token_and_event_history(pool: &SqlitePool) -> Result<(u64, u64)> {
    // Retry the complete transaction on transient SQLITE_BUSY/locked errors.
    // Each failed attempt is rolled back when its transaction is dropped.
    const MAX_ATTEMPTS: usize = 4;

    for attempt in 0..MAX_ATTEMPTS {
        let result = async {
            let mut transaction = pool.begin().await?;
            let launches = sqlx::query("DELETE FROM token_launches")
                .execute(&mut *transaction)
                .await?
                .rows_affected();
            let events = sqlx::query("DELETE FROM solana_raw_events")
                .execute(&mut *transaction)
                .await?
                .rows_affected();
            transaction.commit().await?;
            Ok::<(u64, u64), anyhow::Error>((launches, events))
        }
        .await;

        match result {
            Ok(counts) => return Ok(counts),
            Err(error)
                if attempt + 1 < MAX_ATTEMPTS
                    && error
                        .to_string()
                        .to_ascii_lowercase()
                        .contains("database is locked") =>
            {
                let delay_ms = 250_u64 * (1_u64 << attempt);
                tracing::warn!(
                    attempt = attempt + 1,
                    max_attempts = MAX_ATTEMPTS,
                    delay_ms,
                    %error,
                    "SQLite history cleanup encountered a transient lock; retrying"
                );
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            }
            Err(error) => return Err(error),
        }
    }

    unreachable!("history cleanup retry loop always returns")
}

/// Upsert one OHLCV candle by token, timeframe and its event-time bucket.
pub async fn save_chart_candle(
    pool: &SqlitePool,
    mint: &str,
    timeframe: &str,
    open_time_ms: i64,
    candle: &serde_json::Value,
) -> Result<()> {
    sqlx::query(
        r#"INSERT INTO chart_candles
            (mint_address, timeframe, open_time_ms, payload_json, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(mint_address, timeframe, open_time_ms) DO UPDATE SET
            payload_json=excluded.payload_json,
            updated_at=excluded.updated_at"#,
    )
    .bind(mint)
    .bind(timeframe)
    .bind(open_time_ms)
    .bind(serde_json::to_string(candle)?)
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(pool)
    .await?;
    Ok(())
}

/// Load recent candles in chronological order for one token/timeframe.
pub async fn load_chart_candles(
    pool: &SqlitePool,
    mint: &str,
    timeframe: &str,
    limit: u32,
) -> Result<Vec<serde_json::Value>> {
    let rows = sqlx::query_scalar::<_, String>(
        "SELECT payload_json FROM chart_candles WHERE mint_address = ? AND timeframe = ? ORDER BY open_time_ms DESC LIMIT ?",
    )
    .bind(mint)
    .bind(timeframe)
    .bind(limit.clamp(1, 2_000) as i64)
    .fetch_all(pool)
    .await?;
    let mut candles = rows
        .into_iter()
        .map(|payload| serde_json::from_str(&payload))
        .collect::<std::result::Result<Vec<serde_json::Value>, _>>()?;
    candles.reverse();
    Ok(candles)
}


/// Return the newest persisted launches, newest first.
pub async fn list_recent_launches(
    pool: &SqlitePool,
    limit: u32,
) -> Result<Vec<serde_json::Value>> {
    let rows = sqlx::query_scalar::<_, String>(
        "SELECT payload_json FROM token_launches ORDER BY detected_at DESC LIMIT ?",
    )
    .bind(limit.clamp(1, 500) as i64)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|payload| serde_json::from_str(&payload).map_err(Into::into))
        .collect()
}

/// Restore the persisted paper wallet without mixing it with the on-chain wallet.
pub async fn load_paper_wallet(pool: &SqlitePool) -> Result<Option<PaperWallet>> {
    let row = sqlx::query(
        "SELECT starting_balance_lamports, available_balance_lamports, invested_balance_lamports, locked_rent_lamports, total_network_fees_lamports, total_priority_fees_lamports, total_swap_fees_lamports, total_rent_refunded_lamports FROM paper_wallets WHERE id = 'default'",
    )
    .fetch_optional(pool)
    .await?;

    row.map(|row| {
        Ok(PaperWallet {
            starting_balance_lamports: u64::try_from(row.get::<i64, _>("starting_balance_lamports"))?,
            available_balance_lamports: u64::try_from(row.get::<i64, _>("available_balance_lamports"))?,
            invested_balance_lamports: u64::try_from(row.get::<i64, _>("invested_balance_lamports"))?,
            locked_rent_lamports: u64::try_from(row.get::<i64, _>("locked_rent_lamports"))?,
            total_network_fees_lamports: u64::try_from(row.get::<i64, _>("total_network_fees_lamports"))?,
            total_priority_fees_lamports: u64::try_from(row.get::<i64, _>("total_priority_fees_lamports"))?,
            total_swap_fees_lamports: u64::try_from(row.get::<i64, _>("total_swap_fees_lamports"))?,
            total_rent_refunded_lamports: u64::try_from(row.get::<i64, _>("total_rent_refunded_lamports"))?,
        })
    })
    .transpose()
}

/// Persist the single engine paper wallet using an atomic upsert.
pub async fn save_paper_wallet(pool: &SqlitePool, wallet: &PaperWallet) -> Result<()> {
    sqlx::query(
        r#"INSERT INTO paper_wallets (
            id, starting_balance_lamports, available_balance_lamports,
            invested_balance_lamports, locked_rent_lamports,
            total_network_fees_lamports, total_priority_fees_lamports,
            total_swap_fees_lamports, total_rent_refunded_lamports, updated_at
        ) VALUES ('default', ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            starting_balance_lamports=excluded.starting_balance_lamports,
            available_balance_lamports=excluded.available_balance_lamports,
            invested_balance_lamports=excluded.invested_balance_lamports,
            locked_rent_lamports=excluded.locked_rent_lamports,
            total_network_fees_lamports=excluded.total_network_fees_lamports,
            total_priority_fees_lamports=excluded.total_priority_fees_lamports,
            total_swap_fees_lamports=excluded.total_swap_fees_lamports,
            total_rent_refunded_lamports=excluded.total_rent_refunded_lamports,
            updated_at=excluded.updated_at"#,
    )
    .bind(i64::try_from(wallet.starting_balance_lamports)?)
    .bind(i64::try_from(wallet.available_balance_lamports)?)
    .bind(i64::try_from(wallet.invested_balance_lamports)?)
    .bind(i64::try_from(wallet.locked_rent_lamports)?)
    .bind(i64::try_from(wallet.total_network_fees_lamports)?)
    .bind(i64::try_from(wallet.total_priority_fees_lamports)?)
    .bind(i64::try_from(wallet.total_swap_fees_lamports)?)
    .bind(i64::try_from(wallet.total_rent_refunded_lamports)?)
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(pool)
    .await?;
    Ok(())
}
