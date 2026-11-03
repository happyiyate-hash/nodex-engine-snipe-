use anyhow::Result;
use nodex_core::TokenLaunch;
use sqlx::{sqlite::SqliteConnectOptions, sqlite::SqlitePoolOptions, SqlitePool};
use std::str::FromStr;

pub async fn connect(database_url: &str) -> Result<SqlitePool> {
    let options = SqliteConnectOptions::from_str(database_url)?
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await?;
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS token_launches (
            id TEXT PRIMARY KEY NOT NULL,
            mint_address TEXT NOT NULL,
            creator_address TEXT,
            source TEXT NOT NULL,
            signature TEXT NOT NULL UNIQUE,
            slot INTEGER NOT NULL,
            detected_at TEXT NOT NULL,
            payload_json TEXT NOT NULL
        )"#,
    ).execute(&pool).await?;

    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS paper_wallets (
            id TEXT PRIMARY KEY NOT NULL,
            starting_balance_lamports INTEGER NOT NULL,
            available_balance_lamports INTEGER NOT NULL,
            invested_balance_lamports INTEGER NOT NULL,
            locked_rent_lamports INTEGER NOT NULL,
            total_network_fees_lamports INTEGER NOT NULL,
            total_priority_fees_lamports INTEGER NOT NULL,
            total_swap_fees_lamports INTEGER NOT NULL,
            total_rent_refunded_lamports INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        )"#,
    ).execute(&pool).await?;

    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS paper_ledger (
            id TEXT PRIMARY KEY NOT NULL,
            kind TEXT NOT NULL,
            amount_lamports INTEGER NOT NULL,
            available_after_lamports INTEGER NOT NULL,
            reference TEXT,
            created_at TEXT NOT NULL
        )"#,
    ).execute(&pool).await?;

    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS solana_raw_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signature TEXT,
            slot INTEGER,
            event_json TEXT NOT NULL,
            received_at TEXT NOT NULL
        )"#,
    ).execute(&pool).await?;
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
    let signature = event.pointer("/params/result/value/signature").and_then(|v| v.as_str());
    let slot = event.pointer("/params/result/context/slot").and_then(|v| v.as_i64());
    sqlx::query("INSERT INTO solana_raw_events (signature, slot, event_json, received_at) VALUES (?, ?, ?, ?)")
        .bind(signature)
        .bind(slot)
        .bind(serde_json::to_string(event)?)
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(pool)
        .await?;
    Ok(())
}
