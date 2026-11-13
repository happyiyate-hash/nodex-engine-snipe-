//! Portfolio event publication. The WebSocket gateway remains the frontend transport.

use serde_json::{json, Value};
use tokio::sync::broadcast;

/// Publish one exact position update, keyed by its mint address.
pub(super) fn publish_position_update(events: &broadcast::Sender<Value>, position: &Value) {
    let Some(mint) = position.get("mint_address").and_then(Value::as_str) else {
        tracing::warn!("Skipped position update without mint_address");
        return;
    };
    let mut payload = position.clone();
    if let Some(object) = payload.as_object_mut() {
        object.insert("mint_address".into(), json!(mint));
    }
    if position.get("exit_requested").and_then(Value::as_bool) == Some(true)
        && position.get("exit_triggered_at").is_some()
    {
        let _ = events.send(json!({"type":"paper_exit_triggered","data":payload.clone()}));
    }
    let _ = events.send(json!({"type":"position_updated","data":payload}));
}

/// Publish the current profile and aggregate portfolio snapshot.
pub(super) fn publish_profile_and_summary(
    events: &broadcast::Sender<Value>,
    profile: Value,
    portfolio: Value,
) {
    let _ = events.send(json!({"type":"profile_updated","data":profile}));
    let _ = events.send(json!({"type":"portfolio_summary_updated","data":portfolio}));
}
