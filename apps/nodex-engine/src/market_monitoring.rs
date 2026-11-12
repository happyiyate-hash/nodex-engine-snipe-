use super::*;

/// Register one approved token's Pump.fun curve and publish the subscription
/// lifecycle through the shared event broadcaster.
pub(super) async fn request_pump_fun_subscription(
    monitor: Option<&MarketMonitor>,
    mint: &str,
    program_id: &str,
    events: &crate::event_broadcaster::EventBroadcaster,
) {
    let Some(monitor) = monitor else {
        let _ = events.publish(json!({"type":"market_subscription_failed","data":{
            "mint_address":mint,
            "venue":"pump_fun_bonding_curve",
            "status":"failed",
            "reason":"Market monitor is unavailable",
            "message":"Market data unavailable",
            "received_at":chrono::Utc::now().to_rfc3339()
        }}));
        return;
    };

    match monitor.subscribe_pump_fun_bonding_curve(mint, program_id).await {
        Ok(account_address) => {
            let _ = events.publish(json!({"type":"market_subscription_requested","data":{
                "mint_address":mint,
                "account_address":account_address,
                "venue":"pump_fun_bonding_curve",
                "status":"pending",
                "received_at":chrono::Utc::now().to_rfc3339()
            }}));
        }
        Err(error) => {
            let reason = error.to_string();
            let _ = events.publish(json!({"type":"market_subscription_failed","data":{
                "mint_address":mint,
                "venue":"pump_fun_bonding_curve",
                "status":"failed",
                "reason":reason,
                "message":"Market data unavailable",
                "received_at":chrono::Utc::now().to_rfc3339()
            }}));
            let _ = events.publish(json!({"type":"diagnostic","data":{
                "severity":"warning",
                "component":"market_monitor_registration",
                "message":reason,
                "mint_address":mint,
                "received_at":chrono::Utc::now().to_rfc3339()
            }}));
        }
    }
}

use nodex_solana::start_market_monitor;
use serde_json::json;

pub(super) async fn ensure_market_monitor(state: &AppState) {
    // Create the monitor once, then restore the newest persisted launch subscriptions.
    // The monitor itself retains its desired set across WebSocket reconnects; this
    // rehydration closes the process-restart gap as well.
    let (monitor, should_restore) = {
        let mut guard = state.market_monitor.write().await;
        let should_restore = guard.is_none();
        if should_restore && !state.solana_ws_urls.is_empty() {
            *guard = Some(start_market_monitor(
                state.solana_ws_urls.clone(),
                state.events.clone(),
            ));
        }
        (guard.clone(), should_restore)
    };
    if !should_restore {
        return;
    }

    // The launch-history table is not the canonical token registry and may be
    // empty even when hundreds of complete token states are saved in SQLite.
    // Rebuild subscriptions from token_states so verified tokens survive a
    // backend restart and begin receiving fresh market data after Start/Resume.
    let token_states = match nodex_persistence::load_token_states(&state.database).await {
        Ok(states) => states,
        Err(error) => {
            tracing::warn!(%error, "Could not load saved token states for market subscription recovery");
            return;
        }
    };
    let Some(monitor) = monitor else {
        let reason =
            "Market monitoring is unavailable because no Solana WebSocket URL is configured.";
        for token in token_states {
            let _ = state
                .events
                .send(json!({"type":"market_subscription_failed","data":{
                    "mint_address":token.mint_address,
                    "venue":"pump_fun_bonding_curve",
                    "status":"failed",
                    "reason":reason,
                    "message":reason,
                    "received_at":chrono::Utc::now().to_rfc3339()
                }}));
        }
        tracing::warn!(%reason, "Could not restore market subscriptions");
        return;
    };

    let mut restored = 0usize;
    let mut failed = 0usize;
    for token in token_states {
        let mint = token.mint_address;
        match monitor
            .subscribe_pump_fun_bonding_curve(&mint, &state.pump_fun_program_id)
            .await
        {
            Ok(account_address) => {
                restored += 1;
                let _ = state.events.send(json!({"type":"market_subscription_requested","data":{
                    "mint_address":mint,
                    "account_address":account_address,
                    "venue":"pump_fun_bonding_curve",
                    "status":"pending",
                    "reason":"Restoring the saved token's market subscription after backend start.",
                    "received_at":chrono::Utc::now().to_rfc3339()
                }}));
            }
            Err(error) => {
                failed += 1;
                let reason =
                    format!("Could not restore the saved token's market subscription: {error}");
                let _ = state
                    .events
                    .send(json!({"type":"market_subscription_failed","data":{
                        "mint_address":mint,
                        "venue":"pump_fun_bonding_curve",
                        "status":"failed",
                        "reason":reason,
                        "message":reason,
                        "received_at":chrono::Utc::now().to_rfc3339()
                    }}));
                tracing::warn!(%error, mint_address = %mint, "Could not restore persisted market subscription");
            }
        }
    }
    tracing::info!(
        restored,
        failed,
        "Restored saved-token market subscriptions from SQLite token states"
    );
}
