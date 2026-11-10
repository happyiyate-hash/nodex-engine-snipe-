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
            starting_sol_price_usd REAL NOT NULL DEFAULT 0,
            available_balance_lamports INTEGER NOT NULL, invested_balance_lamports INTEGER NOT NULL,
            locked_rent_lamports INTEGER NOT NULL, total_network_fees_lamports INTEGER NOT NULL,
            total_priority_fees_lamports INTEGER NOT NULL, total_swap_fees_lamports INTEGER NOT NULL,
            total_rent_refunded_lamports INTEGER NOT NULL, updated_at TEXT NOT NULL
        )"#,
    )
    .execute(&pool)
    .await?;
    // Existing paper wallets predate the SOL cost-basis quote. Zero marks a
    // legacy row whose baseline is initialized once during engine startup.
    let wallet_columns = sqlx::query("PRAGMA table_info(paper_wallets)")
        .fetch_all(&pool)
        .await?;
    let has_starting_sol_price = wallet_columns
        .iter()
        .any(|row| row.get::<String, _>("name") == "starting_sol_price_usd");
    if !has_starting_sol_price {
        sqlx::query(
            "ALTER TABLE paper_wallets ADD COLUMN starting_sol_price_usd REAL NOT NULL DEFAULT 0",
        )
        .execute(&pool)
        .await?;
    }
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS paper_ledger (
            id TEXT PRIMARY KEY NOT NULL, kind TEXT NOT NULL, amount_lamports INTEGER NOT NULL,
            available_after_lamports INTEGER NOT NULL, reference TEXT, created_at TEXT NOT NULL
        )"#,
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS engine_activity_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recorded_at TEXT NOT NULL,
            activity_json TEXT NOT NULL
        )"#,
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_engine_activity_logs_recorded_at ON engine_activity_logs(recorded_at)",
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

/** Persist structured provider activity, retaining only the newest 5,000 cards. */
pub async fn save_engine_activity_log(
    pool: &SqlitePool,
    activity: &serde_json::Value,
) -> Result<()> {
    let recorded_at = activity
        .get("timestamp")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    let activity_json = serde_json::to_string(activity)?;
    sqlx::query("INSERT INTO engine_activity_logs (recorded_at, activity_json) VALUES (?, ?)")
        .bind(recorded_at)
        .bind(activity_json)
        .execute(pool)
        .await?;
    sqlx::query(
        "DELETE FROM engine_activity_logs WHERE id NOT IN (SELECT id FROM engine_activity_logs ORDER BY id DESC LIMIT 5000)",
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn list_engine_activity_logs(
    pool: &SqlitePool,
    limit: u32,
) -> Result<Vec<serde_json::Value>> {
    let safe_limit = limit.clamp(1, 1000) as i64;
    let rows = sqlx::query(
        "SELECT id, recorded_at, activity_json FROM engine_activity_logs ORDER BY id DESC LIMIT ?",
    )
    .bind(safe_limit)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|row| {
            let activity_json: String = row.get("activity_json");
            let activity: serde_json::Value = serde_json::from_str(&activity_json)?;
            Ok(serde_json::json!({
                "id": row.get::<i64, _>("id"),
                "recorded_at": row.get::<String, _>("recorded_at"),
                "activity": activity
            }))
        })
        .collect()
}

/// Permanently removes persisted engine activity stream records only.
pub async fn clear_engine_activity_logs(pool: &SqlitePool) -> Result<u64> {
    let result = sqlx::query("DELETE FROM engine_activity_logs")
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
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

/// Remove stored tokens that have never entered paper trading.
/// Any mint with a paper position (open or closed) is retained, together with
/// its token state, launch history and chart candles. Wallets, ledger, runtime
/// configuration, subscriptions and schema are never modified.
pub async fn clear_non_paper_token_history(
    pool: &SqlitePool,
) -> Result<(u64, u64, u64, u64, Vec<String>)> {
    const MAX_ATTEMPTS: usize = 4;

    for attempt in 0..MAX_ATTEMPTS {
        let result = async {
            let mut transaction = pool.begin().await?;
            let deleted_mints = sqlx::query_scalar::<_, String>(
                r#"SELECT mint_address FROM token_states
                   WHERE NOT EXISTS (
                       SELECT 1 FROM portfolio_positions p
                       WHERE p.mint_address = token_states.mint_address
                   )
                   UNION
                   SELECT mint_address FROM token_launches
                   WHERE NOT EXISTS (
                       SELECT 1 FROM portfolio_positions p
                       WHERE p.mint_address = token_launches.mint_address
                   )"#,
            )
            .fetch_all(&mut *transaction)
            .await?;

            let states = sqlx::query(
                "DELETE FROM token_states WHERE NOT EXISTS (SELECT 1 FROM portfolio_positions p WHERE p.mint_address = token_states.mint_address)",
            )
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            let candles = sqlx::query(
                "DELETE FROM chart_candles WHERE NOT EXISTS (SELECT 1 FROM portfolio_positions p WHERE p.mint_address = chart_candles.mint_address)",
            )
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            let launches = sqlx::query(
                "DELETE FROM token_launches WHERE NOT EXISTS (SELECT 1 FROM portfolio_positions p WHERE p.mint_address = token_launches.mint_address)",
            )
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            // Raw upstream notifications are transient history, not paper-trading
            // accounting, so they can be cleared regardless of token position.
            let events = sqlx::query("DELETE FROM solana_raw_events")
                .execute(&mut *transaction)
                .await?
                .rows_affected();
            transaction.commit().await?;
            Ok::<_, anyhow::Error>((states, launches, candles, events, deleted_mints))
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
                    "SQLite token cleanup encountered a transient lock; retrying"
                );
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            }
            Err(error) => return Err(error),
        }
    }

    unreachable!("token cleanup retry loop always returns")
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
pub async fn list_recent_launches(pool: &SqlitePool, limit: u32) -> Result<Vec<serde_json::Value>> {
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
        "SELECT starting_balance_lamports, starting_sol_price_usd, available_balance_lamports, invested_balance_lamports, locked_rent_lamports, total_network_fees_lamports, total_priority_fees_lamports, total_swap_fees_lamports, total_rent_refunded_lamports FROM paper_wallets WHERE id = 'default'",
    )
    .fetch_optional(pool)
    .await?;

    row.map(|row| {
        Ok(PaperWallet {
            starting_balance_lamports: u64::try_from(
                row.get::<i64, _>("starting_balance_lamports"),
            )?,
            starting_sol_price_usd: row.get::<f64, _>("starting_sol_price_usd"),
            available_balance_lamports: u64::try_from(
                row.get::<i64, _>("available_balance_lamports"),
            )?,
            invested_balance_lamports: u64::try_from(
                row.get::<i64, _>("invested_balance_lamports"),
            )?,
            locked_rent_lamports: u64::try_from(row.get::<i64, _>("locked_rent_lamports"))?,
            total_network_fees_lamports: u64::try_from(
                row.get::<i64, _>("total_network_fees_lamports"),
            )?,
            total_priority_fees_lamports: u64::try_from(
                row.get::<i64, _>("total_priority_fees_lamports"),
            )?,
            total_swap_fees_lamports: u64::try_from(row.get::<i64, _>("total_swap_fees_lamports"))?,
            total_rent_refunded_lamports: u64::try_from(
                row.get::<i64, _>("total_rent_refunded_lamports"),
            )?,
        })
    })
    .transpose()
}

/// Persist the single engine paper wallet using an atomic upsert.
pub async fn save_paper_wallet(pool: &SqlitePool, wallet: &PaperWallet) -> Result<()> {
    sqlx::query(
        r#"INSERT INTO paper_wallets (
            id, starting_balance_lamports, starting_sol_price_usd, available_balance_lamports,
            invested_balance_lamports, locked_rent_lamports,
            total_network_fees_lamports, total_priority_fees_lamports,
            total_swap_fees_lamports, total_rent_refunded_lamports, updated_at
        ) VALUES ('default', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            starting_balance_lamports=excluded.starting_balance_lamports,
            starting_sol_price_usd=excluded.starting_sol_price_usd,
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
    .bind(wallet.starting_sol_price_usd)
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

#[cfg(test)]
mod cleanup_tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    #[tokio::test]
    async fn cleanup_removes_only_tokens_without_any_paper_position() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();

        for statement in [
            "CREATE TABLE token_states (mint_address TEXT PRIMARY KEY, version INTEGER NOT NULL, updated_at TEXT NOT NULL, payload_json TEXT NOT NULL)",
            "CREATE TABLE token_launches (id TEXT PRIMARY KEY, mint_address TEXT NOT NULL, creator_address TEXT, source TEXT NOT NULL, signature TEXT NOT NULL UNIQUE, slot INTEGER NOT NULL, detected_at TEXT NOT NULL, payload_json TEXT NOT NULL)",
            "CREATE TABLE chart_candles (mint_address TEXT NOT NULL, timeframe TEXT NOT NULL, open_time_ms INTEGER NOT NULL, payload_json TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY (mint_address, timeframe, open_time_ms))",
            "CREATE TABLE solana_raw_events (id INTEGER PRIMARY KEY, signature TEXT, slot INTEGER, event_hash TEXT, event_json TEXT NOT NULL, received_at TEXT NOT NULL)",
            "CREATE TABLE portfolio_positions (position_id TEXT PRIMARY KEY, mint_address TEXT NOT NULL, status TEXT NOT NULL, payload_json TEXT NOT NULL, updated_at TEXT NOT NULL)",
            "CREATE TABLE paper_wallets (id TEXT PRIMARY KEY, balance INTEGER NOT NULL)",
            "CREATE TABLE paper_ledger (id TEXT PRIMARY KEY, amount INTEGER NOT NULL)",
        ] {
            sqlx::query(statement).execute(&pool).await.unwrap();
        }

        for mint in ["paper-open", "paper-closed", "rejected", "failed-fetch"] {
            sqlx::query("INSERT INTO token_states VALUES (?, 1, 'now', '{}')")
                .bind(mint)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query(
                "INSERT INTO token_launches VALUES (?, ?, NULL, 'pump_fun', ?, 1, 'now', '{}')",
            )
            .bind(format!("launch-{mint}"))
            .bind(mint)
            .bind(format!("sig-{mint}"))
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query("INSERT INTO chart_candles VALUES (?, '5m', 1, '{}', 'now')")
                .bind(mint)
                .execute(&pool)
                .await
                .unwrap();
        }

        sqlx::query(
            "INSERT INTO portfolio_positions VALUES ('pos-open', 'paper-open', 'open', '{}', 'now')",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO portfolio_positions VALUES ('pos-closed', 'paper-closed', 'closed', '{}', 'now')",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO solana_raw_events (event_json, received_at) VALUES ('{}', 'now')")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO paper_wallets VALUES ('wallet', 123)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO paper_ledger VALUES ('ledger', 123)")
            .execute(&pool)
            .await
            .unwrap();

        let (states, launches, candles, events, deleted_mints) =
            clear_non_paper_token_history(&pool).await.unwrap();

        assert_eq!(states, 2);
        assert_eq!(launches, 2);
        assert_eq!(candles, 2);
        assert_eq!(events, 1);
        assert_eq!(deleted_mints.len(), 2);

        for mint in ["paper-open", "paper-closed"] {
            let exists: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM token_states WHERE mint_address = ?")
                    .bind(mint)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(exists, 1);
        }

        let remaining_wallets: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM paper_wallets")
            .fetch_one(&pool)
            .await
            .unwrap();
        let remaining_ledger: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM paper_ledger")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(remaining_wallets, 1);
        assert_eq!(remaining_ledger, 1);
    }
}
