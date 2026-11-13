//! Portfolio valuation, position marking and portfolio event publication.
//!
//! Token prices remain owned by the market-data pipeline. This module consumes
//! those prices to derive paper-position values and aggregate account equity.

use super::*;

pub mod calculator;
pub mod event_publisher;
pub mod position_valuation;

/// Recalculate, persist and publish the complete portfolio whenever SOL/USD changes.
pub(super) fn start_price_projector(
    mut receiver: broadcast::Receiver<serde_json::Value>,
    events: broadcast::Sender<serde_json::Value>,
    database: sqlx::SqlitePool,
    wallet: Arc<RwLock<PaperWallet>>,
    sol_price: Arc<RwLock<SolPriceSnapshot>>,
    positions: Arc<RwLock<Vec<serde_json::Value>>>,
    profile: Arc<RwLock<serde_json::Value>>,
) {
    tokio::spawn(async move {
        loop {
            let event = match receiver.recv().await {
                Ok(event) => event,
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    tracing::warn!(dropped_events=count, "Portfolio price projector lagged; next SOL quote will reconcile the snapshot");
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            };
            if event.get("type").and_then(serde_json::Value::as_str) != Some("sol_price_update") {
                continue;
            }
            let wallet_snapshot = wallet.read().await.clone();
            let price_snapshot = sol_price.read().await.clone();
            let position_snapshot = positions.read().await.clone();
            let portfolio = calculator::calculate_portfolio_snapshot(
                &wallet_snapshot,
                &price_snapshot,
                &position_snapshot,
            );
            if let Err(error) =
                nodex_persistence::save_portfolio_snapshot(&database, &portfolio).await
            {
                tracing::error!(%error, "Could not persist portfolio snapshot after SOL price update");
                continue;
            }
            let profile_snapshot = {
                let mut current = profile.write().await;
                if let Some(object) = current.as_object_mut() {
                    object.insert("paper_wallet".into(), json!(wallet_snapshot));
                    object.insert("sol_price".into(), json!(price_snapshot));
                    object.insert("portfolio".into(), portfolio.clone());
                }
                current.clone()
            };
            if let Err(error) = nodex_persistence::save_profile(&database, &profile_snapshot).await
            {
                tracing::error!(%error, "Could not persist profile after SOL price update");
            }
            event_publisher::publish_profile_and_summary(&events, profile_snapshot, portfolio);
        }
    });
}
