use anyhow::Result;
use nodex_core::TokenLaunch;
use nodex_trading::paper_wallet::{LedgerEntry, PaperWallet};
use sqlx::{sqlite::SqliteConnectOptions, sqlite::SqlitePoolOptions, Row, SqlitePool};
use std::str::FromStr;

pub async fn connect(database_url: &str) -> Result<SqlitePool> {
    let options = SqliteConnectOptions::from_str(database_url)?.create_if_missing(true);
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
            event_json TEXT NOT NULL, received_at TEXT NOT NULL
        )"#,
    )
    .execute(&pool)
    .await?;
    Ok(pool)
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

pub async fn save_raw_solana_event(pool: &SqlitePool, event: &serde_json::Value) -> Result<()> {
    let signature = event
        .pointer("/params/result/value/signature")
        .and_then(|value| value.as_str());
    let slot = event
        .pointer("/params/result/context/slot")
        .and_then(|value| value.as_i64());
    sqlx::query(
        "INSERT INTO solana_raw_events (signature, slot, event_json, received_at) VALUES (?, ?, ?, ?)",
    )
    .bind(signature)
    .bind(slot)
    .bind(serde_json::to_string(event)?)
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn load_paper_wallet(pool: &SqlitePool) -> Result<Option<PaperWallet>> {
    let row = sqlx::query(
        "SELECT starting_balance_lamports, available_balance_lamports, invested_balance_lamports, locked_rent_lamports, total_network_fees_lamports, total_priority_fees_lamports, total_swap_fees_lamports, total_rent_refunded_lamports FROM paper_wallets WHERE id = 'default'",
    )
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|row| PaperWallet {
        starting_balance_lamports: row.get::<i64, _>(0) as u64,
        available_balance_lamports: row.get::<i64, _>(1) as u64,
        invested_balance_lamports: row.get::<i64, _>(2) as u64,
        locked_rent_lamports: row.get::<i64, _>(3) as u64,
        total_network_fees_lamports: row.get::<i64, _>(4) as u64,
        total_priority_fees_lamports: row.get::<i64, _>(5) as u64,
        total_swap_fees_lamports: row.get::<i64, _>(6) as u64,
        total_rent_refunded_lamports: row.get::<i64, _>(7) as u64,
    }))
}

pub async fn save_paper_wallet(pool: &SqlitePool, wallet: &PaperWallet) -> Result<()> {
    sqlx::query(
        r#"INSERT INTO paper_wallets
        (id, starting_balance_lamports, available_balance_lamports, invested_balance_lamports,
         locked_rent_lamports, total_network_fees_lamports, total_priority_fees_lamports,
         total_swap_fees_lamports, total_rent_refunded_lamports, updated_at)
        VALUES ('default', ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    .bind(wallet.starting_balance_lamports as i64)
    .bind(wallet.available_balance_lamports as i64)
    .bind(wallet.invested_balance_lamports as i64)
    .bind(wallet.locked_rent_lamports as i64)
    .bind(wallet.total_network_fees_lamports as i64)
    .bind(wallet.total_priority_fees_lamports as i64)
    .bind(wallet.total_swap_fees_lamports as i64)
    .bind(wallet.total_rent_refunded_lamports as i64)
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn save_ledger_entry(pool: &SqlitePool, entry: &LedgerEntry) -> Result<()> {
    sqlx::query(
        "INSERT INTO paper_ledger (id, kind, amount_lamports, available_after_lamports, reference, created_at) VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(entry.id.to_string())
    .bind(&entry.kind)
    .bind(entry.amount_lamports as i64)
    .bind(entry.available_after_lamports as i64)
    .bind(&entry.reference)
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(pool)
    .await?;
    Ok(())
}

/// Returns the newest persisted token launches, newest slot first.
pub async fn list_recent_launches(pool: &SqlitePool, limit: u32) -> Result<Vec<serde_json::Value>> {
    let safe_limit = limit.clamp(1, 500) as i64;
    let rows = sqlx::query_scalar::<_, String>(
        "SELECT payload_json FROM token_launches ORDER BY slot DESC, detected_at DESC LIMIT ?",
    )
    .bind(safe_limit)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|payload| serde_json::from_str(&payload).map_err(Into::into))
        .collect()
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
    Ok((launches, events))
}
