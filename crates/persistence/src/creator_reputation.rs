use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};

const MIN_ESTABLISHED_LAUNCHES: u64 = 3;
const MIN_ESTABLISHED_OBSERVED_TOKENS: usize = 2;
const MIN_ESTABLISHED_OBSERVATIONS: u64 = 12;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatorReputation {
    pub creator_address: String,
    pub total_launches: u64,
    pub observed_tokens: u64,
    pub total_observations: u64,
    pub graduated_tokens: u64,
    pub longevity_score: Option<f64>,
    pub growth_score: Option<f64>,
    pub graduation_score: Option<f64>,
    pub activity_score: Option<f64>,
    pub stability_score: Option<f64>,
    /// Higher means more observed risk, not more reputation.
    pub risk_score: Option<f64>,
    pub overall_score: Option<f64>,
    pub confidence_pct: f64,
    /// insufficient_data, developing, or established. This is not a trading approval.
    pub sample_status: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
struct TokenEvidence {
    launched_at: String,
    last_observed_at: Option<String>,
    lifecycle_status: String,
    graduated: bool,
    peak_market_cap_usd: Option<f64>,
    latest_market_cap_usd: Option<f64>,
    latest_volume_5m_usd: Option<f64>,
    latest_unique_buyers_5m: Option<u64>,
    latest_holder_concentration_pct: Option<f64>,
    observation_count: u64,
}

#[derive(Debug, Clone)]
struct ScoreBreakdown {
    launches: u64,
    observed_tokens: u64,
    observations: u64,
    graduated_tokens: u64,
    longevity: Option<f64>,
    growth: Option<f64>,
    graduation: Option<f64>,
    activity: Option<f64>,
    stability: Option<f64>,
    risk: Option<f64>,
    overall: Option<f64>,
    confidence_pct: f64,
    sample_status: String,
}

fn clamp_score(value: f64) -> f64 {
    value.clamp(0.0, 100.0)
}

fn average(values: impl Iterator<Item = Option<f64>>) -> Option<f64> {
    let values: Vec<f64> = values.flatten().filter(|v| v.is_finite()).collect();
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }
}

fn parse_time(value: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|time| time.with_timezone(&chrono::Utc))
}

fn token_scores(token: &TokenEvidence, now: chrono::DateTime<chrono::Utc>) -> [Option<f64>; 6] {
    let age_hours = parse_time(&token.launched_at)
        .map(|time| now.signed_duration_since(time).num_seconds().max(0) as f64 / 3600.0);
    let observed_hours = token
        .last_observed_at
        .as_deref()
        .and_then(parse_time)
        .zip(parse_time(&token.launched_at))
        .map(|(last, launch)| {
            last.signed_duration_since(launch).num_seconds().max(0) as f64 / 3600.0
        });

    let phase = token
        .lifecycle_status
        .strip_prefix("graduated_")
        .unwrap_or(&token.lifecycle_status);
    let longevity = observed_hours.map(|hours| {
        let duration = clamp_score(hours / 24.0 * 100.0);
        let lifecycle_bonus = if phase == "active" {
            15.0
        } else if phase == "slowing" {
            5.0
        } else {
            0.0
        };
        clamp_score(duration * 0.8 + lifecycle_bonus)
    });

    let growth = token.peak_market_cap_usd.map(|cap| {
        if cap >= 2_000_000.0 {
            100.0
        } else if cap >= 1_000_000.0 {
            85.0
        } else if cap >= 500_000.0 {
            70.0
        } else if cap >= 250_000.0 {
            55.0
        } else if cap >= 100_000.0 {
            35.0
        } else if cap >= 50_000.0 {
            20.0
        } else {
            5.0
        }
    });

    let graduation = if token.graduated {
        Some(100.0)
    } else if age_hours.is_some_and(|hours| hours >= 24.0) {
        Some(0.0)
    } else {
        None
    };

    let buyer_activity = token
        .latest_unique_buyers_5m
        .map(|buyers| clamp_score(buyers as f64 / 20.0 * 100.0));
    let volume_activity = token
        .latest_volume_5m_usd
        .map(|volume| clamp_score(volume / 10_000.0 * 100.0));
    let activity = average([buyer_activity, volume_activity].into_iter());

    let stability = token
        .peak_market_cap_usd
        .filter(|peak| *peak > 0.0)
        .zip(token.latest_market_cap_usd)
        .map(|(peak, latest)| clamp_score(latest / peak * 100.0));

    let concentration_risk = token
        .latest_holder_concentration_pct
        .map(|pct| clamp_score(pct));
    let drawdown_risk = token
        .peak_market_cap_usd
        .filter(|peak| *peak > 0.0)
        .zip(token.latest_market_cap_usd)
        .map(|(peak, latest)| clamp_score((1.0 - latest / peak) * 100.0));
    let phase = token
        .lifecycle_status
        .strip_prefix("graduated_")
        .unwrap_or(&token.lifecycle_status);
    let lifecycle_risk = if phase == "inactive" {
        Some(85.0)
    } else if phase == "slowing" {
        Some(45.0)
    } else if phase == "active" {
        Some(10.0)
    } else {
        None
    };
    let risk = average([concentration_risk, drawdown_risk, lifecycle_risk].into_iter());

    [longevity, growth, graduation, activity, stability, risk]
}

fn build_breakdown(
    launches: u64,
    tokens: &[TokenEvidence],
    now: chrono::DateTime<chrono::Utc>,
) -> ScoreBreakdown {
    let observed: Vec<&TokenEvidence> = tokens
        .iter()
        .filter(|token| token.observation_count > 0)
        .collect();
    let observations = observed.iter().map(|token| token.observation_count).sum();
    let graduated_tokens = tokens.iter().filter(|token| token.graduated).count() as u64;
    let scores: Vec<[Option<f64>; 6]> = observed
        .iter()
        .map(|token| token_scores(token, now))
        .collect();

    let component = |index: usize| average(scores.iter().map(|score| score[index]));
    let longevity = component(0);
    let growth = component(1);
    let graduation = component(2);
    let activity = component(3);
    let stability = component(4);
    let risk = component(5);

    // Only available dimensions contribute; missing RPC evidence is not a zero.
    let weighted = [
        (longevity, 0.15),
        (growth, 0.20),
        (graduation, 0.20),
        (activity, 0.15),
        (stability, 0.15),
        (risk.map(|value| 100.0 - value), 0.15),
    ];
    let weight_total: f64 = weighted
        .iter()
        .filter_map(|(score, weight)| score.map(|_| *weight))
        .sum();
    let overall = if weight_total > 0.0 {
        Some(
            weighted
                .iter()
                .filter_map(|(score, weight)| score.map(|value| value * weight))
                .sum::<f64>()
                / weight_total,
        )
    } else {
        None
    };

    let sample_status = if launches >= MIN_ESTABLISHED_LAUNCHES
        && observed.len() >= MIN_ESTABLISHED_OBSERVED_TOKENS
        && observations >= MIN_ESTABLISHED_OBSERVATIONS
    {
        "established"
    } else if !observed.is_empty() {
        "developing"
    } else {
        "insufficient_data"
    };
    let launch_coverage = if launches == 0 {
        0.0
    } else {
        observed.len() as f64 / launches as f64
    };
    let volume_coverage = (observations as f64 / 24.0).min(1.0);
    let sample_confidence = (launch_coverage.min(1.0) * 50.0) + (volume_coverage * 50.0);

    ScoreBreakdown {
        launches,
        observed_tokens: observed.len() as u64,
        observations,
        graduated_tokens,
        longevity,
        growth,
        graduation,
        activity,
        stability,
        risk,
        overall,
        confidence_pct: clamp_score(sample_confidence),
        sample_status: sample_status.to_string(),
    }
}

pub async fn initialize(pool: &SqlitePool) -> Result<()> {
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS creator_reputations (
            creator_address TEXT PRIMARY KEY NOT NULL,
            total_launches INTEGER NOT NULL DEFAULT 0,
            observed_tokens INTEGER NOT NULL DEFAULT 0,
            total_observations INTEGER NOT NULL DEFAULT 0,
            graduated_tokens INTEGER NOT NULL DEFAULT 0,
            longevity_score REAL,
            growth_score REAL,
            graduation_score REAL,
            activity_score REAL,
            stability_score REAL,
            risk_score REAL,
            overall_score REAL,
            confidence_pct REAL NOT NULL DEFAULT 0,
            sample_status TEXT NOT NULL DEFAULT 'insufficient_data',
            updated_at TEXT NOT NULL
        )"#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_creator_reputations_rank ON creator_reputations(overall_score DESC, risk_score ASC, updated_at DESC)",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Recompute creator reputation from immutable launch relationships and deduplicated
/// token observations. Recomputing from source evidence avoids score inflation on retries.
pub async fn evaluate_creator_reputations(
    pool: &SqlitePool,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<u64> {
    let creators = sqlx::query(
        "SELECT creator_address, total_launches FROM training_developers ORDER BY creator_address",
    )
    .fetch_all(pool)
    .await?;
    let mut updated = 0_u64;

    for creator in creators {
        let address: String = creator.try_get("creator_address")?;
        let launches = creator.try_get::<i64, _>("total_launches")?.max(0) as u64;
        let rows = sqlx::query(
            r#"SELECT t.launched_at, t.last_observed_at, t.lifecycle_status,
                      t.graduated_at, t.peak_market_cap_usd, t.observation_count,
                      latest.market_cap_usd AS latest_market_cap_usd,
                      latest.volume_5m_usd AS latest_volume_5m_usd,
                      latest.unique_buyers_5m AS latest_unique_buyers_5m,
                      latest.holder_concentration_pct AS latest_holder_concentration_pct
               FROM training_tokens t
               LEFT JOIN training_token_observations latest
                 ON latest.id = (
                    SELECT o.id FROM training_token_observations o
                    WHERE o.mint_address = t.mint_address
                    ORDER BY o.observed_at DESC, o.id DESC LIMIT 1
                 )
               WHERE t.creator_address = ?"#,
        )
        .bind(&address)
        .fetch_all(pool)
        .await?;

        let tokens = rows
            .into_iter()
            .map(|row| {
                Ok(TokenEvidence {
                    launched_at: row.try_get("launched_at")?,
                    last_observed_at: row.try_get("last_observed_at")?,
                    lifecycle_status: row.try_get("lifecycle_status")?,
                    graduated: row.try_get::<Option<String>, _>("graduated_at")?.is_some(),
                    peak_market_cap_usd: row.try_get("peak_market_cap_usd")?,
                    latest_market_cap_usd: row.try_get("latest_market_cap_usd")?,
                    latest_volume_5m_usd: row.try_get("latest_volume_5m_usd")?,
                    latest_unique_buyers_5m: row
                        .try_get::<Option<i64>, _>("latest_unique_buyers_5m")?
                        .map(|v| v.max(0) as u64),
                    latest_holder_concentration_pct: row
                        .try_get("latest_holder_concentration_pct")?,
                    observation_count: row.try_get::<i64, _>("observation_count")?.max(0) as u64,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let score = build_breakdown(launches, &tokens, now);
        let result = sqlx::query(
            r#"INSERT INTO creator_reputations (
                creator_address, total_launches, observed_tokens, total_observations,
                graduated_tokens, longevity_score, growth_score, graduation_score,
                activity_score, stability_score, risk_score, overall_score,
                confidence_pct, sample_status, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(creator_address) DO UPDATE SET
                total_launches=excluded.total_launches,
                observed_tokens=excluded.observed_tokens,
                total_observations=excluded.total_observations,
                graduated_tokens=excluded.graduated_tokens,
                longevity_score=excluded.longevity_score,
                growth_score=excluded.growth_score,
                graduation_score=excluded.graduation_score,
                activity_score=excluded.activity_score,
                stability_score=excluded.stability_score,
                risk_score=excluded.risk_score,
                overall_score=excluded.overall_score,
                confidence_pct=excluded.confidence_pct,
                sample_status=excluded.sample_status,
                updated_at=excluded.updated_at
            WHERE creator_reputations.total_launches != excluded.total_launches
               OR creator_reputations.total_observations != excluded.total_observations
               OR creator_reputations.overall_score IS NOT excluded.overall_score
               OR creator_reputations.risk_score IS NOT excluded.risk_score
               OR creator_reputations.sample_status != excluded.sample_status
               OR creator_reputations.confidence_pct != excluded.confidence_pct"#,
        )
        .bind(&address)
        .bind(score.launches as i64)
        .bind(score.observed_tokens as i64)
        .bind(score.observations as i64)
        .bind(score.graduated_tokens as i64)
        .bind(score.longevity)
        .bind(score.growth)
        .bind(score.graduation)
        .bind(score.activity)
        .bind(score.stability)
        .bind(score.risk)
        .bind(score.overall)
        .bind(score.confidence_pct)
        .bind(&score.sample_status)
        .bind(now.to_rfc3339())
        .execute(pool)
        .await?;
        updated += result.rows_affected();
    }
    Ok(updated)
}

pub async fn list_creator_reputations(
    pool: &SqlitePool,
    limit: u32,
) -> Result<Vec<CreatorReputation>> {
    let rows = sqlx::query(
        r#"SELECT creator_address, total_launches, observed_tokens, total_observations,
                  graduated_tokens, longevity_score, growth_score, graduation_score,
                  activity_score, stability_score, risk_score, overall_score,
                  confidence_pct, sample_status, updated_at
           FROM creator_reputations
           ORDER BY CASE WHEN overall_score IS NULL THEN 1 ELSE 0 END,
                    overall_score DESC, risk_score ASC, updated_at DESC LIMIT ?"#,
    )
    .bind(limit.clamp(1, 500) as i64)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|row| {
            Ok(CreatorReputation {
                creator_address: row.try_get("creator_address")?,
                total_launches: row.try_get::<i64, _>("total_launches")?.max(0) as u64,
                observed_tokens: row.try_get::<i64, _>("observed_tokens")?.max(0) as u64,
                total_observations: row.try_get::<i64, _>("total_observations")?.max(0) as u64,
                graduated_tokens: row.try_get::<i64, _>("graduated_tokens")?.max(0) as u64,
                longevity_score: row.try_get("longevity_score")?,
                growth_score: row.try_get("growth_score")?,
                graduation_score: row.try_get("graduation_score")?,
                activity_score: row.try_get("activity_score")?,
                stability_score: row.try_get("stability_score")?,
                risk_score: row.try_get("risk_score")?,
                overall_score: row.try_get("overall_score")?,
                confidence_pct: row.try_get("confidence_pct")?,
                sample_status: row.try_get("sample_status")?,
                updated_at: row.try_get("updated_at")?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evidence() -> TokenEvidence {
        TokenEvidence {
            launched_at: "2026-11-14T00:00:00Z".into(),
            last_observed_at: Some("2026-11-15T11:50:00Z".into()),
            lifecycle_status: "graduated_active".into(),
            graduated: true,
            peak_market_cap_usd: Some(1_000_000.0),
            latest_market_cap_usd: Some(800_000.0),
            latest_volume_5m_usd: Some(12_000.0),
            latest_unique_buyers_5m: Some(18),
            latest_holder_concentration_pct: Some(30.0),
            observation_count: 12,
        }
    }

    fn now() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339("2026-11-15T12:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc)
    }

    #[test]
    fn established_status_requires_multiple_observed_launches_and_evidence() {
        let tokens = vec![evidence(), evidence()];
        let score = build_breakdown(3, &tokens, now());
        assert_eq!(score.sample_status, "established");
        assert_eq!(score.observed_tokens, 2);
        assert_eq!(score.overall, Some(score.overall.unwrap()));
    }

    #[test]
    fn no_observations_remain_insufficient_and_scores_are_unknown() {
        let token = TokenEvidence {
            observation_count: 0,
            ..evidence()
        };
        let score = build_breakdown(1, &[token], now());
        assert_eq!(score.sample_status, "insufficient_data");
        assert_eq!(score.overall, None);
        assert_eq!(score.risk, None);
    }

    #[test]
    fn immature_ungraduated_tokens_do_not_receive_a_zero_graduation_score() {
        let mut token = evidence();
        token.graduated = false;
        token.lifecycle_status = "active".into();
        token.launched_at = "2026-11-15T11:00:00Z".into();
        let scores = token_scores(&token, now());
        assert_eq!(scores[2], None);
    }

    #[test]
    fn inactive_tokens_are_not_mistaken_for_active_longevity() {
        let mut token = evidence();
        token.lifecycle_status = "inactive".into();
        let scores = token_scores(&token, now());
        assert!(scores[0].unwrap() < 100.0);
        assert_eq!(scores[5], Some(85.0));
    }

    #[test]
    fn large_drawdown_reduces_stability_and_increases_risk() {
        let mut token = evidence();
        token.latest_market_cap_usd = Some(100_000.0);
        let scores = token_scores(&token, now());
        assert_eq!(scores[4], Some(10.0));
        assert!(scores[5].unwrap() > 70.0);
    }
}
