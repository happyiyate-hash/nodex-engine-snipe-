pub mod creator_intelligence;

use anyhow::Result;
use nodex_core::{TokenLaunch, TokenState};
use nodex_trading::paper_wallet::{LedgerEntry, PaperWallet};
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
    // SQLite permits many readers but only one writer. A single in-process
    // connection serializes engine reads/writes and prevents our own ingestion,
    // snapshots, and maintenance tasks from racing for SQLite's writer lock.
    // WAL still allows external readers, and busy_timeout handles external writers.
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(30))
        .connect_with(options)
        .await?;
    creator_intelligence::initialize(&pool).await?;
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
    // Persist per-token-account rent reservations so automatic rent recovery
    // remains correct after the paper wallet is restored on restart.
    let has_rent_by_token_account = wallet_columns
        .iter()
        .any(|row| row.get::<String, _>("name") == "rent_by_token_account_json");
    if !has_rent_by_token_account {
        sqlx::query(
            "ALTER TABLE paper_wallets ADD COLUMN rent_by_token_account_json TEXT NOT NULL DEFAULT '{}'",
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
        r#"CREATE TABLE IF NOT EXISTS portfolio_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            captured_at TEXT NOT NULL,
            payload_json TEXT NOT NULL
        )"#,
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_captured_at ON portfolio_snapshots(captured_at DESC)",
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
        "CREATE INDEX IF NOT EXISTS idx_token_states_updated_at ON token_states(updated_at DESC)",
    )
    .execute(&pool)
    .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_token_launches_detected_at ON token_launches(detected_at DESC)")
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

/// Persist portfolio valuations while retaining a bounded, deduplicated history.
pub async fn save_portfolio_snapshot(
    pool: &SqlitePool,
    snapshot: &serde_json::Value,
) -> Result<()> {
    let payload = serde_json::to_string(snapshot)?;
    let captured_at = snapshot
        .get("updated_at")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());
    let mut transaction = pool.begin().await?;
    let latest: Option<String> =
        sqlx::query_scalar("SELECT payload_json FROM portfolio_snapshots ORDER BY id DESC LIMIT 1")
            .fetch_optional(&mut *transaction)
            .await?;
    let changed = match latest {
        Some(previous) => {
            let mut previous_value: serde_json::Value = serde_json::from_str(&previous)?;
            let mut next_value = snapshot.clone();
            if let Some(object) = previous_value.as_object_mut() {
                object.remove("updated_at");
            }
            if let Some(object) = next_value.as_object_mut() {
                object.remove("updated_at");
            }
            previous_value != next_value
        }
        None => true,
    };
    if changed {
        sqlx::query("INSERT INTO portfolio_snapshots (captured_at, payload_json) VALUES (?, ?)")
            .bind(captured_at)
            .bind(payload)
            .execute(&mut *transaction)
            .await?;
        sqlx::query("DELETE FROM portfolio_snapshots WHERE id NOT IN (SELECT id FROM portfolio_snapshots ORDER BY id DESC LIMIT 1000)")
            .execute(&mut *transaction)
            .await?;
    }
    transaction.commit().await?;
    Ok(())
}

pub async fn load_latest_portfolio_snapshot(
    pool: &SqlitePool,
) -> Result<Option<serde_json::Value>> {
    let payload = sqlx::query_scalar::<_, String>(
        "SELECT payload_json FROM portfolio_snapshots ORDER BY id DESC LIMIT 1",
    )
    .fetch_optional(pool)
    .await?;
    payload
        .map(|value| serde_json::from_str(&value).map_err(Into::into))
        .transpose()
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

/// Keep only the newest token history rows, while preserving every token with an open paper position.
/// This bounds both startup memory restoration and SQLite growth on small instances.
pub async fn prune_token_history(pool: &SqlitePool, limit: u32) -> Result<(u64, u64)> {
    let safe_limit = limit.clamp(1, 500) as i64;
    let mut transaction = pool.begin().await?;
    let states = sqlx::query(
        r#"DELETE FROM token_states
           WHERE mint_address NOT IN (
               SELECT mint_address FROM token_states
               ORDER BY updated_at DESC LIMIT ?
           )
           AND NOT EXISTS (
               SELECT 1 FROM portfolio_positions p
               WHERE p.mint_address = token_states.mint_address
                 AND p.status = 'open'
           )"#,
    )
    .bind(safe_limit)
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    let launches = sqlx::query(
        r#"DELETE FROM token_launches
           WHERE id NOT IN (
               SELECT id FROM token_launches
               ORDER BY detected_at DESC LIMIT ?
           )
           AND NOT EXISTS (
               SELECT 1 FROM portfolio_positions p
               WHERE p.mint_address = token_launches.mint_address
                 AND p.status = 'open'
           )"#,
    )
    .bind(safe_limit)
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    sqlx::query(
        r#"DELETE FROM chart_candles
           WHERE NOT EXISTS (
               SELECT 1 FROM token_states s
               WHERE s.mint_address = chart_candles.mint_address
           )
           AND NOT EXISTS (
               SELECT 1 FROM portfolio_positions p
               WHERE p.mint_address = chart_candles.mint_address
                 AND p.status = 'open'
           )"#,
    )
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok((states, launches))
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
    let inserted = result.rows_affected() == 1;
    if inserted {
        prune_token_history(pool, 500).await?;
    }
    Ok(inserted)
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
pub async fn save_engine_activity_logs(
    pool: &SqlitePool,
    activities: &[serde_json::Value],
) -> Result<()> {
    if activities.is_empty() {
        return Ok(());
    }

    // One SQLite write transaction per batch avoids a second retention DELETE
    // transaction for every single provider/lifecycle event.
    let mut transaction = pool.begin().await?;
    for activity in activities {
        let recorded_at = activity
            .get("timestamp")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        let activity_json = serde_json::to_string(activity)?;
        sqlx::query("INSERT INTO engine_activity_logs (recorded_at, activity_json) VALUES (?, ?)")
            .bind(recorded_at)
            .bind(activity_json)
            .execute(&mut *transaction)
            .await?;
    }

    sqlx::query(
        "DELETE FROM engine_activity_logs WHERE id NOT IN (SELECT id FROM engine_activity_logs ORDER BY id DESC LIMIT 5000)",
    )
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(())
}

pub async fn save_engine_activity_log(
    pool: &SqlitePool,
    activity: &serde_json::Value,
) -> Result<()> {
    save_engine_activity_logs(pool, std::slice::from_ref(activity)).await
}

pub async fn list_engine_activity_logs(
    pool: &SqlitePool,
    limit: u32,
) -> Result<Vec<serde_json::Value>> {
    let safe_limit = limit.clamp(1, 1000) as i64;
    let rows = sqlx::query(
        "SELECT id, recorded_at, activity_json
         FROM engine_activity_logs
         WHERE json_extract(activity_json, '$.provider') = 'engine'
            OR lower(COALESCE(json_extract(activity_json, '$.severity'), '')) IN ('error', 'warning')
            OR COALESCE(CAST(json_extract(activity_json, '$.http_status') AS INTEGER), 0) >= 400
            OR COALESCE(json_extract(activity_json, '$.error'), '') <> ''
         ORDER BY id DESC LIMIT ?",
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
            // Acquire the writer reservation before reading the keep/delete set.
            // This avoids a deferred read transaction failing when it later tries
            // to upgrade to a write transaction (SQLITE_BUSY_SNAPSHOT).
            let mut connection = pool.acquire().await?;
            sqlx::query("BEGIN IMMEDIATE")
                .execute(&mut *connection)
                .await?;

            let transaction_result = async {
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
                .fetch_all(&mut *connection)
                .await?;

                let states = sqlx::query(
                    "DELETE FROM token_states WHERE NOT EXISTS (SELECT 1 FROM portfolio_positions p WHERE p.mint_address = token_states.mint_address)",
                )
                .execute(&mut *connection)
                .await?
                .rows_affected();
                let candles = sqlx::query(
                    "DELETE FROM chart_candles WHERE NOT EXISTS (SELECT 1 FROM portfolio_positions p WHERE p.mint_address = chart_candles.mint_address)",
                )
                .execute(&mut *connection)
                .await?
                .rows_affected();
                let launches = sqlx::query(
                    "DELETE FROM token_launches WHERE NOT EXISTS (SELECT 1 FROM portfolio_positions p WHERE p.mint_address = token_launches.mint_address)",
                )
                .execute(&mut *connection)
                .await?
                .rows_affected();
                // Raw upstream notifications are transient history, not paper-trading
                // accounting, so they can be cleared regardless of token position.
                let events = sqlx::query("DELETE FROM solana_raw_events")
                    .execute(&mut *connection)
                    .await?
                    .rows_affected();

                Ok::<_, anyhow::Error>((states, launches, candles, events, deleted_mints))
            }
            .await;

            match transaction_result {
                Ok(counts) => {
                    sqlx::query("COMMIT").execute(&mut *connection).await?;
                    Ok(counts)
                }
                Err(error) => {
                    // Never return a connection with a partially executed cleanup
                    // transaction to the pool.
                    let rollback = sqlx::query("ROLLBACK").execute(&mut *connection).await;
                    if let Err(rollback_error) = rollback {
                        tracing::error!(
                            %rollback_error,
                            "SQLite cleanup rollback failed; discarding the connection"
                        );
                        connection.close().await?;
                    }
                    Err(error)
                }
            }
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
                let delay_ms = 500_u64 * (1_u64 << attempt);
                tracing::warn!(
                    attempt = attempt + 1,
                    max_attempts = MAX_ATTEMPTS,
                    delay_ms,
                    %error,
                    "SQLite token cleanup could not acquire the writer lock; retrying"
                );
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            }
            Err(error) => return Err(error),
        }
    }

    unreachable!("token cleanup retry loop always returns")
}

/// Atomically remove inactive token history while preserving every mint that has
/// any paper position. Paper positions, wallet balances, ledger and audit history
/// are never touched by this cleanup.
pub async fn delete_inactive_token_history(
    pool: &SqlitePool,
    candidate_mints: &[String],
) -> Result<Vec<String>> {
    if candidate_mints.is_empty() {
        return Ok(Vec::new());
    }
    let mut transaction = pool.begin().await?;
    let mut deleted = Vec::new();
    for mint in candidate_mints {
        let has_position: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM portfolio_positions WHERE mint_address = ? AND status = 'open'")
                .bind(mint)
                .fetch_one(&mut *transaction)
                .await?;
        if has_position > 0 {
            continue;
        }
        let state_rows = sqlx::query("DELETE FROM token_states WHERE mint_address = ?")
            .bind(mint)
            .execute(&mut *transaction)
            .await?
            .rows_affected();
        let launch_rows = sqlx::query("DELETE FROM token_launches WHERE mint_address = ?")
            .bind(mint)
            .execute(&mut *transaction)
            .await?
            .rows_affected();
        sqlx::query("DELETE FROM chart_candles WHERE mint_address = ?")
            .bind(mint)
            .execute(&mut *transaction)
            .await?;
        if state_rows + launch_rows > 0 {
            deleted.push(mint.clone());
        }
    }
    transaction.commit().await?;
    Ok(deleted)
}

/// Remove token discovery/market history after a terminal rejection or a fully closed paper position.
/// Paper positions, wallet balances, paper ledger and portfolio snapshots are deliberately preserved.
pub async fn delete_token_market_history(pool: &SqlitePool, mint: &str) -> Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("DELETE FROM token_states WHERE mint_address = ?")
        .bind(mint)
        .execute(&mut *transaction)
        .await?;
    sqlx::query("DELETE FROM token_launches WHERE mint_address = ?")
        .bind(mint)
        .execute(&mut *transaction)
        .await?;
    sqlx::query("DELETE FROM chart_candles WHERE mint_address = ?")
        .bind(mint)
        .execute(&mut *transaction)
        .await?;
    let pattern = format!("%{}%", mint);
    sqlx::query("DELETE FROM solana_raw_events WHERE event_json LIKE ?")
        .bind(&pattern)
        .execute(&mut *transaction)
        .await?;
    sqlx::query("DELETE FROM engine_activity_logs WHERE activity_json LIKE ?")
        .bind(&pattern)
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    Ok(())
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
        "SELECT starting_balance_lamports, starting_sol_price_usd, available_balance_lamports, invested_balance_lamports, locked_rent_lamports, total_network_fees_lamports, total_priority_fees_lamports, total_swap_fees_lamports, total_rent_refunded_lamports, rent_by_token_account_json FROM paper_wallets WHERE id = 'default'",
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
            rent_by_token_account: serde_json::from_str(
                &row.get::<String, _>("rent_by_token_account_json"),
            )?,
        })
    })
    .transpose()
}

/// Persist the single engine paper wallet using an atomic upsert.
/// Commit a simulated paper buy atomically across wallet, ledger, and position rows.
/// This function has no Solana RPC, signer, or transaction-submission dependency.
pub async fn commit_paper_buy(
    pool: &SqlitePool,
    wallet: &PaperWallet,
    ledger: &LedgerEntry,
    position_id: &str,
    mint: &str,
    position: &serde_json::Value,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query(
        r#"INSERT INTO paper_wallets (
            id, starting_balance_lamports, starting_sol_price_usd, available_balance_lamports,
            invested_balance_lamports, locked_rent_lamports,
            total_network_fees_lamports, total_priority_fees_lamports,
            total_swap_fees_lamports, total_rent_refunded_lamports,
            rent_by_token_account_json, updated_at
        ) VALUES ('default', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            rent_by_token_account_json=excluded.rent_by_token_account_json,
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
    .bind(serde_json::to_string(&wallet.rent_by_token_account)?)
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(&mut *transaction)
    .await?;

    sqlx::query(
        "INSERT INTO paper_ledger (id, kind, amount_lamports, available_after_lamports, reference, created_at) VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(ledger.id.to_string())
    .bind(&ledger.kind)
    .bind(i64::try_from(ledger.amount_lamports)?)
    .bind(i64::try_from(ledger.available_after_lamports)?)
    .bind(&ledger.reference)
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(&mut *transaction)
    .await?;

    sqlx::query(
        r#"INSERT INTO portfolio_positions
            (position_id, mint_address, status, payload_json, updated_at)
        VALUES (?, ?, 'open', ?, ?)"#,
    )
    .bind(position_id)
    .bind(mint)
    .bind(serde_json::to_string(position)?)
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(&mut *transaction)
    .await?;

    transaction.commit().await?;
    Ok(())
}

/// Atomically settle a full simulated sale across the paper wallet, ledger and position.
pub async fn commit_paper_sell(
    pool: &SqlitePool,
    wallet: &PaperWallet,
    ledger: &LedgerEntry,
    position_id: &str,
    mint: &str,
    position: &serde_json::Value,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query(
        r#"INSERT INTO paper_wallets (
            id, starting_balance_lamports, starting_sol_price_usd, available_balance_lamports,
            invested_balance_lamports, locked_rent_lamports,
            total_network_fees_lamports, total_priority_fees_lamports,
            total_swap_fees_lamports, total_rent_refunded_lamports,
            rent_by_token_account_json, updated_at
        ) VALUES ('default', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            rent_by_token_account_json=excluded.rent_by_token_account_json,
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
    .bind(serde_json::to_string(&wallet.rent_by_token_account)?)
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(&mut *transaction)
    .await?;

    sqlx::query(
        "INSERT INTO paper_ledger (id, kind, amount_lamports, available_after_lamports, reference, created_at) VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(ledger.id.to_string())
    .bind(&ledger.kind)
    .bind(i64::try_from(ledger.amount_lamports)?)
    .bind(i64::try_from(ledger.available_after_lamports)?)
    .bind(&ledger.reference)
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(&mut *transaction)
    .await?;

    let updated = sqlx::query(
        "UPDATE portfolio_positions SET status='closed', payload_json=?, updated_at=? WHERE position_id=? AND mint_address=? AND status='open'",
    )
    .bind(serde_json::to_string(position)?)
    .bind(chrono::Utc::now().to_rfc3339())
    .bind(position_id)
    .bind(mint)
    .execute(&mut *transaction)
    .await?;
    anyhow::ensure!(
        updated.rows_affected() == 1,
        "paper position was no longer open when the sale settlement was committed"
    );

    transaction.commit().await?;
    Ok(())
}

pub async fn save_paper_wallet(pool: &SqlitePool, wallet: &PaperWallet) -> Result<()> {
    sqlx::query(
        r#"INSERT INTO paper_wallets (
            id, starting_balance_lamports, starting_sol_price_usd, available_balance_lamports,
            invested_balance_lamports, locked_rent_lamports,
            total_network_fees_lamports, total_priority_fees_lamports,
            total_swap_fees_lamports, total_rent_refunded_lamports,
            rent_by_token_account_json, updated_at
        ) VALUES ('default', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            rent_by_token_account_json=excluded.rent_by_token_account_json,
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
    .bind(serde_json::to_string(&wallet.rent_by_token_account)?)
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
            sqlx::query(statement).execute(&pool)
            .await
            .unwrap();
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

#[cfg(test)]
mod portfolio_snapshot_tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    #[tokio::test]
    async fn portfolio_snapshots_are_saved_deduplicated_and_restored() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query("CREATE TABLE portfolio_snapshots (id INTEGER PRIMARY KEY AUTOINCREMENT, captured_at TEXT NOT NULL, payload_json TEXT NOT NULL)")
            .execute(&pool).await.unwrap();
        let snapshot = serde_json::json!({
            "portfolio_balance_usd": 10.5,
            "unrealized_pnl_usd": 0.5,
            "unrealized_pnl_pct": 5.0,
            "updated_at": "2026-11-11T12:00:00Z"
        });
        save_portfolio_snapshot(&pool, &snapshot).await.unwrap();
        save_portfolio_snapshot(&pool, &snapshot).await.unwrap();
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM portfolio_snapshots")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 1);
        let restored = load_latest_portfolio_snapshot(&pool)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(restored, snapshot);
    }
}
#[cfg(test)]
mod paper_buy_transaction_tests {
    use super::*;
    use nodex_trading::paper_wallet::{BuyCostBreakdown, PaperWallet, LAMPORTS_PER_SOL};

    #[tokio::test]
    async fn paper_sell_atomically_closes_position_and_settles_wallet() {
        let pool = connect("sqlite::memory:").await.unwrap();
        let mut wallet = PaperWallet::new_with_price(100_000_000, 200.0);
        let costs = BuyCostBreakdown {
            token_input_lamports: 10_000_000,
            network_fee_lamports: 0,
            priority_fee_lamports: 0,
            swap_fee_lamports: 0,
            rent_reserve_lamports: 0,
        };
        let buy_ledger = wallet.reserve_buy_for_token("mint-sell", &costs).unwrap();
        let open = serde_json::json!({
            "position_id":"paper-position-sell",
            "mint_address":"mint-sell",
            "status":"open",
            "cost_basis_sol":0.01
        });
        commit_paper_buy(
            &pool,
            &wallet,
            &buy_ledger,
            "paper-position-sell",
            "mint-sell",
            &open,
        )
        .await
        .unwrap();

        let sell_ledger = wallet
            .settle_full_sale("mint-sell", 10_000_000, 15_000_000)
            .unwrap();
        let closed = serde_json::json!({
            "position_id":"paper-position-sell",
            "mint_address":"mint-sell",
            "status":"closed",
            "realized_pnl_sol":0.005
        });
        commit_paper_sell(
            &pool,
            &wallet,
            &sell_ledger,
            "paper-position-sell",
            "mint-sell",
            &closed,
        )
        .await
        .unwrap();

        let restored_wallet = load_paper_wallet(&pool).await.unwrap().unwrap();
        let positions = load_portfolio_positions(&pool).await.unwrap();
        let ledger_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM paper_ledger")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(restored_wallet.available_balance_lamports, 105_000_000);
        assert_eq!(restored_wallet.invested_balance_lamports, 0);
        assert_eq!(positions[0]["status"], "closed");
        assert_eq!(positions[0]["realized_pnl_sol"], 0.005);
        assert_eq!(ledger_count, 2);
    }

    #[tokio::test]
    async fn paper_buy_commits_wallet_ledger_and_position_together() {
        let pool = connect("sqlite::memory:").await.unwrap();
        let mut wallet = PaperWallet::new_with_price(100_000_000, 200.0);
        let costs = BuyCostBreakdown {
            token_input_lamports: 10_000_000,
            network_fee_lamports: 0,
            priority_fee_lamports: 0,
            swap_fee_lamports: 0,
            rent_reserve_lamports: 0,
        };
        let ledger = wallet.reserve_buy_for_token("mint-a", &costs).unwrap();
        let position = serde_json::json!({
            "position_id":"paper-position-1",
            "mint_address":"mint-a",
            "status":"open",
            "cost_basis_sol":0.01
        });

        commit_paper_buy(
            &pool,
            &wallet,
            &ledger,
            "paper-position-1",
            "mint-a",
            &position,
        )
        .await
        .unwrap();

        let restored_wallet = load_paper_wallet(&pool).await.unwrap().unwrap();
        let positions = load_portfolio_positions(&pool).await.unwrap();
        let ledger_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM paper_ledger")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(restored_wallet.available_balance_lamports, 90_000_000);
        assert_eq!(restored_wallet.invested_balance_lamports, 10_000_000);
        assert_eq!(
            restored_wallet.invested_balance_lamports as f64 / LAMPORTS_PER_SOL as f64,
            0.01
        );
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0]["mint_address"], "mint-a");
        assert_eq!(ledger_count, 1);
    }
}
