use serde_json::Value;
use tokio::sync::broadcast;

pub(super) fn spawn(
    mut runtime_events: broadcast::Receiver<Value>,
    frontend_events: broadcast::Sender<Value>,
) {
    tokio::spawn(async move {
        loop {
            match runtime_events.recv().await {
                Ok(event) => {
                    if super::is_frontend_event(&event) {
                        let _ = frontend_events.send(event);
                    }
                }
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    tracing::warn!(dropped_events = count, "WebSocket runtime reader lagged");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });
}
