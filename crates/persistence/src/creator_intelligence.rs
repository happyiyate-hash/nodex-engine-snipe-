use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};

/// Aggregate-only creator intelligence. No token mint or per-token history is stored.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatorIntelligence {
    pub creator_address: String,
    pub display_label: String,
    pub observations_count: u64,
    pub qualified_observations: u64,
    pub buy_count: u64,
    pub sell_count: u64,
    pub unique_buyers: u64,
    pub independent_buyers: u64,
    pub sniper_concentration_pct: f64,
    pub activity_score: f64,
    pub source: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct CreatorObservation {
    pub creator_address: String,
    pub display_label: String,
    pub qualified: bool,
    pub buy_count: u64,
    pub sell_count: u64,
    pub unique_buyers: u64,
    pub independent_buyers: u64,
    pub sniper_concentration_pct: f64,
    pub activity_score: f64,
    pub observed_at: String,
}

pub async fn initialize(pool: &SqlitePool) -> Result<()> {
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS creator_intelligence (
            creator_address TEXT PRIMARY KEY NOT NULL,
            display_label TEXT NOT NULL DEFAULT '',
            observations_count INTEGER NOT NULL DEFAULT 0,
            qualified_observations INTEGER NOT NULL DEFAULT 0,
            buy_count INTEGER NOT NULL DEFAULT 0,
            sell_count INTEGER NOT NULL DEFAULT 0,
            unique_buyers INTEGER NOT NULL DEFAULT 0,
            independent_buyers INTEGER NOT NULL DEFAULT 0,
            sniper_concentration_pct REAL NOT NULL DEFAULT 0,
            activity_score REAL NOT NULL DEFAULT 0,
            source TEXT NOT NULL DEFAULT 'learned',
            updated_at TEXT NOT NULL
        )"#,
    ).execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_creator_intelligence_score ON creator_intelligence(activity_score DESC, updated_at DESC)")
        .execute(pool).await?;
    Ok(())
}

/// Apply one completed observation. New creators are inserted only when qualified;
/// existing creators continue accumulating evidence even when a later observation fails.
pub async fn apply_observation(
    pool: &SqlitePool,
    observation: &CreatorObservation,
) -> Result<bool> {
    let address = observation.creator_address.trim();
    if address.is_empty() {
        return Err(anyhow!("creator address is required"));
    }
    for (name, value) in [
        ("sniper concentration", observation.sniper_concentration_pct),
        ("activity score", observation.activity_score),
    ] {
        if !value.is_finite() || !(0.0..=100.0).contains(&value) {
            return Err(anyhow!("{name} must be between 0 and 100"));
        }
    }

    let mut tx = pool.begin().await?;
    let existing = sqlx::query(
        "SELECT observations_count, qualified_observations, buy_count, sell_count, unique_buyers, independent_buyers, activity_score FROM creator_intelligence WHERE creator_address = ?",
    ).bind(address).fetch_optional(&mut *tx).await?;

    let Some(row) = existing else {
        if !observation.qualified {
            tx.rollback().await?;
            return Ok(false);
        }
        sqlx::query(
            r#"INSERT INTO creator_intelligence (
                creator_address, display_label, observations_count, qualified_observations,
                buy_count, sell_count, unique_buyers, independent_buyers,
                sniper_concentration_pct, activity_score, source, updated_at
            ) VALUES (?, ?, 1, 1, ?, ?, ?, ?, ?, ?, 'learned', ?)"#,
        )
        .bind(address)
        .bind(observation.display_label.trim())
        .bind(observation.buy_count as i64)
        .bind(observation.sell_count as i64)
        .bind(observation.unique_buyers as i64)
        .bind(observation.independent_buyers as i64)
        .bind(observation.sniper_concentration_pct)
        .bind(observation.activity_score)
        .bind(&observation.observed_at)
        .execute(&mut *tx).await?;
        tx.commit().await?;
        return Ok(true);
    };

    let count = row.get::<i64, _>("observations_count").max(0) as u64;
    let next_count = count.saturating_add(1);
    let previous_score = row.get::<f64, _>("activity_score");
    let next_score = ((previous_score * count as f64) + observation.activity_score) / next_count as f64;
    let previous_qualified = row.get::<i64, _>("qualified_observations").max(0) as u64;
    let label = observation.display_label.trim();
    sqlx::query(
        r#"UPDATE creator_intelligence SET
            display_label = CASE WHEN ? <> '' THEN ? ELSE display_label END,
            observations_count = ?,
            qualified_observations = ?,
            buy_count = buy_count + ?,
            sell_count = sell_count + ?,
            unique_buyers = unique_buyers + ?,
            independent_buyers = independent_buyers + ?,
            sniper_concentration_pct = ((sniper_concentration_pct * ?) + ?) / ?,
            activity_score = ?,
            updated_at = ?
        WHERE creator_address = ?"#,
    )
    .bind(label).bind(label)
    .bind(next_count as i64)
    .bind(previous_qualified.saturating_add(u64::from(observation.qualified)) as i64)
    .bind(observation.buy_count as i64)
    .bind(observation.sell_count as i64)
    .bind(observation.unique_buyers as i64)
    .bind(observation.independent_buyers as i64)
    .bind(count as f64).bind(observation.sniper_concentration_pct)
    .bind(next_count as f64)
    .bind(next_score)
    .bind(&observation.observed_at)
    .bind(address)
    .execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(true)
}

pub async fn list(pool: &SqlitePool, limit: u32) -> Result<Vec<CreatorIntelligence>> {
    let rows = sqlx::query(
        r#"SELECT creator_address, display_label, observations_count, qualified_observations,
            buy_count, sell_count, unique_buyers, independent_buyers,
            sniper_concentration_pct, activity_score, source, updated_at
        FROM creator_intelligence ORDER BY activity_score DESC, updated_at DESC LIMIT ?"#,
    )
    .bind(limit.clamp(1, 500) as i64)
    .fetch_all(pool).await?;
    rows.into_iter().map(|row| Ok(CreatorIntelligence {
        creator_address: row.try_get("creator_address")?,
        display_label: row.try_get("display_label")?,
        observations_count: row.try_get::<i64, _>("observations_count")?.max(0) as u64,
        qualified_observations: row.try_get::<i64, _>("qualified_observations")?.max(0) as u64,
        buy_count: row.try_get::<i64, _>("buy_count")?.max(0) as u64,
        sell_count: row.try_get::<i64, _>("sell_count")?.max(0) as u64,
        unique_buyers: row.try_get::<i64, _>("unique_buyers")?.max(0) as u64,
        independent_buyers: row.try_get::<i64, _>("independent_buyers")?.max(0) as u64,
        sniper_concentration_pct: row.try_get("sniper_concentration_pct")?,
        activity_score: row.try_get("activity_score")?,
        source: row.try_get("source")?,
        updated_at: row.try_get("updated_at")?,
    })).collect()
}
