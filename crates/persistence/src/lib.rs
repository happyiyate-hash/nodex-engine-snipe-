use anyhow::Result;
use nodex_core::TokenLaunch;
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};

pub async fn connect(database_url: &str) -> Result<SqlitePool> {
    let options = SqlitePoolOptions::new().max_connections(5).connect(database_url).await?;
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
        )"#
    ).execute(&options).await?;
    Ok(options)
}

pub async fn save_launch(pool: &SqlitePool, launch: &TokenLaunch) -> Result<bool> {
    let payload = serde_json::to_string(launch)?;
    let result = sqlx::query(
        r#"INSERT OR IGNORE INTO token_launches
           (id, mint_address, creator_address, source, signature, slot, detected_at, payload_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)"#
    )
    .bind(launch.id.to_string())
    .bind(&launch.mint_address)
    .bind(&launch.creator_address)
    .bind("pump_fun")
    .bind(&launch.signature)
    .bind(launch.slot as i64)
    .bind(launch.detected_at.to_rfc3339())
    .bind(payload)
    .execute(pool).await?;
    Ok(result.rows_affected() == 1)
}
