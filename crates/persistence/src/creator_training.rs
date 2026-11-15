use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};

/// A durable link between a creator wallet and a token launch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrainingLaunch {
    pub mint_address: String,
    pub creator_address: String,
    pub source: String,
    pub signature: String,
    pub slot: u64,
    pub detected_at: String,
}

/// One time-bucketed market observation. The caller controls sampling cadence;
/// duplicate (mint, observation_key) writes are ignored for restart-safe ingestion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenObservation {
    pub mint_address: String,
    pub observation_key: String,
    pub observed_at: String,
    pub market_cap_usd: Option<f64>,
    pub price_usd: Option<f64>,
    pub liquidity_usd: Option<f64>,
    pub volume_5m_usd: Option<f64>,
    pub buy_count_5m: u64,
    pub sell_count_5m: u64,
    pub unique_buyers_5m: u64,
    pub holder_concentration_pct: Option<f64>,
    pub graduated: bool,
    pub trading_active: bool,
    /// Structured source evidence and availability flags; never interpreted as fact by itself.
    pub evidence_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrainingToken {
    pub mint_address: String,
    pub creator_address: String,
    pub source: String,
    pub launched_at: String,
    pub last_observed_at: Option<String>,
    pub lifecycle_status: String,
    pub graduated_at: Option<String>,
    pub peak_market_cap_usd: Option<f64>,
    pub observation_count: u64,
}

pub async fn initialize(pool: &SqlitePool) -> Result<()> {
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS training_developers (
            creator_address TEXT PRIMARY KEY NOT NULL,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            total_launches INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        )"#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS training_tokens (
            mint_address TEXT PRIMARY KEY NOT NULL,
            creator_address TEXT NOT NULL,
            source TEXT NOT NULL,
            launch_signature TEXT NOT NULL,
            launch_slot INTEGER NOT NULL,
            launched_at TEXT NOT NULL,
            last_observed_at TEXT,
            lifecycle_status TEXT NOT NULL DEFAULT 'observing',
            graduated_at TEXT,
            peak_market_cap_usd REAL,
            observation_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )"#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_training_tokens_creator ON training_tokens(creator_address, launched_at DESC)",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_training_tokens_status ON training_tokens(lifecycle_status, last_observed_at)",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS training_token_observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mint_address TEXT NOT NULL,
            creator_address TEXT NOT NULL,
            observation_key TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            market_cap_usd REAL,
            price_usd REAL,
            liquidity_usd REAL,
            volume_5m_usd REAL,
            buy_count_5m INTEGER NOT NULL DEFAULT 0,
            sell_count_5m INTEGER NOT NULL DEFAULT 0,
            unique_buyers_5m INTEGER NOT NULL DEFAULT 0,
            holder_concentration_pct REAL,
            graduated INTEGER NOT NULL DEFAULT 0,
            trading_active INTEGER NOT NULL DEFAULT 0,
            evidence_json TEXT NOT NULL DEFAULT '{}',
            UNIQUE(mint_address, observation_key)
        )"#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_training_observations_mint_time ON training_token_observations(mint_address, observed_at)",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_training_observations_creator_time ON training_token_observations(creator_address, observed_at)",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Record a launch and its creator relationship atomically. Replayed launches do
/// not inflate launch counts, and existing evidence is never deleted by pruning
/// ordinary token-state caches.
pub async fn record_launch(pool: &SqlitePool, launch: &TrainingLaunch) -> Result<bool> {
    let mint = launch.mint_address.trim();
    let creator = launch.creator_address.trim();
    let source = launch.source.trim();
    if mint.is_empty() || creator.is_empty() || launch.signature.trim().is_empty() {
        return Err(anyhow!("mint, creator and launch signature are required"));
    }
    let mut tx = pool.begin().await?;
    let inserted = sqlx::query(
        r#"INSERT OR IGNORE INTO training_tokens
           (mint_address, creator_address, source, launch_signature, launch_slot,
            launched_at, lifecycle_status, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, 'observing', ?, ?)"#,
    )
    .bind(mint)
    .bind(creator)
    .bind(if source.is_empty() { "unknown" } else { source })
    .bind(launch.signature.trim())
    .bind(launch.slot.min(i64::MAX as u64) as i64)
    .bind(&launch.detected_at)
    .bind(&launch.detected_at)
    .bind(&launch.detected_at)
    .execute(&mut *tx)
    .await?
    .rows_affected()
        == 1;

    if inserted {
        sqlx::query(
            r#"INSERT INTO training_developers
               (creator_address, first_seen_at, last_seen_at, total_launches, updated_at)
               VALUES (?, ?, ?, 1, ?)
               ON CONFLICT(creator_address) DO UPDATE SET
                 first_seen_at = CASE WHEN excluded.first_seen_at < training_developers.first_seen_at
                                      THEN excluded.first_seen_at ELSE training_developers.first_seen_at END,
                 last_seen_at = CASE WHEN excluded.last_seen_at > training_developers.last_seen_at
                                     THEN excluded.last_seen_at ELSE training_developers.last_seen_at END,
                 total_launches = training_developers.total_launches + 1,
                 updated_at = excluded.updated_at"#,
        )
        .bind(creator)
        .bind(&launch.detected_at)
        .bind(&launch.detected_at)
        .bind(&launch.detected_at)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(inserted)
}

fn validate_optional_nonnegative(name: &str, value: Option<f64>) -> Result<()> {
    if let Some(value) = value {
        if !value.is_finite() || value < 0.0 {
            return Err(anyhow!("{name} must be finite and non-negative"));
        }
    }
    Ok(())
}

/// Append a deduplicated observation and update the token's aggregate lifecycle
/// fields in one transaction. It deliberately does not decide that a token is
/// dead: absence of recent trades requires a separate time-based evaluator.
pub async fn record_observation(pool: &SqlitePool, observation: &TokenObservation) -> Result<bool> {
    if observation.mint_address.trim().is_empty() || observation.observation_key.trim().is_empty() {
        return Err(anyhow!("mint address and observation key are required"));
    }
    for (name, value) in [
        ("market cap", observation.market_cap_usd),
        ("price", observation.price_usd),
        ("liquidity", observation.liquidity_usd),
        ("5m volume", observation.volume_5m_usd),
        ("holder concentration", observation.holder_concentration_pct),
    ] {
        validate_optional_nonnegative(name, value)?;
    }
    if observation
        .holder_concentration_pct
        .is_some_and(|v| v > 100.0)
    {
        return Err(anyhow!("holder concentration cannot exceed 100"));
    }

    let mut tx = pool.begin().await?;
    let creator: Option<String> =
        sqlx::query_scalar("SELECT creator_address FROM training_tokens WHERE mint_address = ?")
            .bind(observation.mint_address.trim())
            .fetch_optional(&mut *tx)
            .await?;
    let Some(creator) = creator else {
        return Err(anyhow!(
            "training launch must be recorded before its observations"
        ));
    };

    let inserted = sqlx::query(
        r#"INSERT OR IGNORE INTO training_token_observations
           (mint_address, creator_address, observation_key, observed_at,
            market_cap_usd, price_usd, liquidity_usd, volume_5m_usd,
            buy_count_5m, sell_count_5m, unique_buyers_5m,
            holder_concentration_pct, graduated, trading_active, evidence_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
    )
    .bind(observation.mint_address.trim())
    .bind(&creator)
    .bind(observation.observation_key.trim())
    .bind(&observation.observed_at)
    .bind(observation.market_cap_usd)
    .bind(observation.price_usd)
    .bind(observation.liquidity_usd)
    .bind(observation.volume_5m_usd)
    .bind(observation.buy_count_5m.min(i64::MAX as u64) as i64)
    .bind(observation.sell_count_5m.min(i64::MAX as u64) as i64)
    .bind(observation.unique_buyers_5m.min(i64::MAX as u64) as i64)
    .bind(observation.holder_concentration_pct)
    .bind(observation.graduated)
    .bind(observation.trading_active)
    .bind(if observation.evidence_json.trim().is_empty() {
        "{}"
    } else {
        &observation.evidence_json
    })
    .execute(&mut *tx)
    .await?
    .rows_affected()
        == 1;

    if inserted {
        sqlx::query(
            r#"UPDATE training_tokens SET
                 last_observed_at = CASE WHEN last_observed_at IS NULL OR last_observed_at < ?
                                         THEN ? ELSE last_observed_at END,
                 peak_market_cap_usd = CASE
                   WHEN ? IS NULL THEN peak_market_cap_usd
                   WHEN peak_market_cap_usd IS NULL OR peak_market_cap_usd < ? THEN ?
                   ELSE peak_market_cap_usd END,
                 graduated_at = CASE WHEN ? = 1 AND graduated_at IS NULL THEN ? ELSE graduated_at END,
                 lifecycle_status = CASE WHEN ? = 1 THEN 'graduated' ELSE lifecycle_status END,
                 observation_count = observation_count + 1,
                 updated_at = ?
               WHERE mint_address = ?"#,
        )
        .bind(&observation.observed_at)
        .bind(&observation.observed_at)
        .bind(observation.market_cap_usd)
        .bind(observation.market_cap_usd)
        .bind(observation.market_cap_usd)
        .bind(observation.graduated)
        .bind(&observation.observed_at)
        .bind(observation.graduated)
        .bind(&observation.observed_at)
        .bind(observation.mint_address.trim())
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(inserted)
}

pub async fn list_tokens_for_creator(
    pool: &SqlitePool,
    creator_address: &str,
    limit: u32,
) -> Result<Vec<TrainingToken>> {
    let rows = sqlx::query(
        r#"SELECT t.mint_address, t.creator_address, t.source, t.launched_at,
                  t.last_observed_at, t.lifecycle_status, t.graduated_at,
                  t.peak_market_cap_usd, t.observation_count
           FROM training_tokens t WHERE t.creator_address = ?
           ORDER BY t.launched_at DESC LIMIT ?"#,
    )
    .bind(creator_address.trim())
    .bind(limit.clamp(1, 500) as i64)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|row| {
            Ok(TrainingToken {
                mint_address: row.try_get("mint_address")?,
                creator_address: row.try_get("creator_address")?,
                source: row.try_get("source")?,
                launched_at: row.try_get("launched_at")?,
                last_observed_at: row.try_get("last_observed_at")?,
                lifecycle_status: row.try_get("lifecycle_status")?,
                graduated_at: row.try_get("graduated_at")?,
                peak_market_cap_usd: row.try_get("peak_market_cap_usd")?,
                observation_count: row.try_get::<i64, _>("observation_count")?.max(0) as u64,
            })
        })
        .collect()
}

/// Return stored observations in chronological order for an individual token.
pub async fn list_token_observations(
    pool: &SqlitePool,
    mint_address: &str,
    limit: u32,
) -> Result<Vec<TokenObservation>> {
    let rows = sqlx::query(
        r#"SELECT mint_address, observation_key, observed_at, market_cap_usd,
                  price_usd, liquidity_usd, volume_5m_usd, buy_count_5m,
                  sell_count_5m, unique_buyers_5m, holder_concentration_pct,
                  graduated, trading_active, evidence_json
           FROM training_token_observations WHERE mint_address = ?
           ORDER BY observed_at DESC LIMIT ?"#,
    )
    .bind(mint_address.trim())
    .bind(limit.clamp(1, 5000) as i64)
    .fetch_all(pool)
    .await?;
    let mut observations = rows
        .into_iter()
        .map(|row| {
            Ok(TokenObservation {
                mint_address: row.try_get("mint_address")?,
                observation_key: row.try_get("observation_key")?,
                observed_at: row.try_get("observed_at")?,
                market_cap_usd: row.try_get("market_cap_usd")?,
                price_usd: row.try_get("price_usd")?,
                liquidity_usd: row.try_get("liquidity_usd")?,
                volume_5m_usd: row.try_get("volume_5m_usd")?,
                buy_count_5m: row.try_get::<i64, _>("buy_count_5m")?.max(0) as u64,
                sell_count_5m: row.try_get::<i64, _>("sell_count_5m")?.max(0) as u64,
                unique_buyers_5m: row.try_get::<i64, _>("unique_buyers_5m")?.max(0) as u64,
                holder_concentration_pct: row.try_get("holder_concentration_pct")?,
                graduated: row.try_get::<i64, _>("graduated")? != 0,
                trading_active: row.try_get::<i64, _>("trading_active")? != 0,
                evidence_json: row.try_get("evidence_json")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    observations.reverse();
    Ok(observations)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn test_pool() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("in-memory SQLite pool");
        initialize(&pool).await.expect("training schema");
        pool
    }

    fn launch(mint: &str, creator: &str, signature: &str, at: &str) -> TrainingLaunch {
        TrainingLaunch {
            mint_address: mint.to_string(),
            creator_address: creator.to_string(),
            source: "pump_fun".to_string(),
            signature: signature.to_string(),
            slot: 42,
            detected_at: at.to_string(),
        }
    }

    fn observation(key: &str, at: &str, cap: f64, graduated: bool) -> TokenObservation {
        TokenObservation {
            mint_address: "mint-1".to_string(),
            observation_key: key.to_string(),
            observed_at: at.to_string(),
            market_cap_usd: Some(cap),
            price_usd: Some(0.01),
            liquidity_usd: Some(12_000.0),
            volume_5m_usd: Some(4_000.0),
            buy_count_5m: 10,
            sell_count_5m: 2,
            unique_buyers_5m: 8,
            holder_concentration_pct: Some(35.0),
            graduated,
            trading_active: true,
            evidence_json: "{}".to_string(),
        }
    }

    #[tokio::test]
    async fn launch_replays_do_not_inflate_developer_launch_count() {
        let pool = test_pool().await;
        let first = launch("mint-1", "creator-1", "signature-1", "2026-01-02T00:00:00Z");
        assert!(record_launch(&pool, &first).await.unwrap());
        assert!(!record_launch(&pool, &first).await.unwrap());

        let count: i64 = sqlx::query_scalar(
            "SELECT total_launches FROM training_developers WHERE creator_address = ?",
        )
        .bind("creator-1")
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn observations_are_deduplicated_and_keep_peak_and_graduation() {
        let pool = test_pool().await;
        record_launch(
            &pool,
            &launch("mint-1", "creator-1", "signature-1", "2026-01-02T00:00:00Z"),
        )
        .await
        .unwrap();

        let first = observation("bucket-1", "2026-01-02T00:05:00Z", 500_000.0, false);
        assert!(record_observation(&pool, &first).await.unwrap());
        assert!(!record_observation(&pool, &first).await.unwrap());

        let second = observation("bucket-2", "2026-01-02T00:10:00Z", 250_000.0, true);
        assert!(record_observation(&pool, &second).await.unwrap());

        let tokens = list_tokens_for_creator(&pool, "creator-1", 20)
            .await
            .unwrap();
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].observation_count, 2);
        assert_eq!(tokens[0].peak_market_cap_usd, Some(500_000.0));
        assert_eq!(tokens[0].lifecycle_status, "graduated");
        assert_eq!(
            tokens[0].graduated_at.as_deref(),
            Some("2026-01-02T00:10:00Z")
        );

        let history = list_token_observations(&pool, "mint-1", 20).await.unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].observation_key, "bucket-1");
        assert_eq!(history[1].observation_key, "bucket-2");
    }

    #[tokio::test]
    async fn invalid_market_values_are_rejected() {
        let pool = test_pool().await;
        record_launch(
            &pool,
            &launch("mint-1", "creator-1", "signature-1", "2026-01-02T00:00:00Z"),
        )
        .await
        .unwrap();

        let mut invalid = observation("bucket-1", "2026-01-02T00:05:00Z", 1.0, false);
        invalid.market_cap_usd = Some(-1.0);
        assert!(record_observation(&pool, &invalid).await.is_err());
    }
}
