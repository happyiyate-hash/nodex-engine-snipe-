use serde_json::Value;
use tokio::sync::{broadcast, mpsc};

/// Receives notifications emitted only after a SQLite transaction commits.
/// It does not poll tables or serialize the database into the WebSocket stream.
pub(super) fn spawn(
    mut database_changes: mpsc::Receiver<Value>,
    frontend_events: broadcast::Sender<Value>,
) {
    tokio::spawn(async move {
        while let Some(event) = database_changes.recv().await {
            if super::is_frontend_event(&event) {
                let _ = frontend_events.send(event);
            }
        }
    });
}
