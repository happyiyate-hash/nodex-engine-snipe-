mod gateway;
mod runtime_reader;
mod sqlite_reader;

use serde_json::Value;
use tokio::sync::{broadcast, mpsc};

#[derive(Clone)]
pub struct Gateway {
    frontend_events: broadcast::Sender<Value>,
}

impl Gateway {
    /// Starts the frontend event gateway. Runtime events and committed database
    /// change notifications are consumed independently and forwarded through one
    /// bounded browser-facing broadcast channel.
    pub fn start(
        runtime_events: broadcast::Receiver<Value>,
        database_changes: mpsc::Receiver<Value>,
        capacity: usize,
    ) -> Self {
        let (frontend_events, _) = broadcast::channel(capacity.max(64));
        runtime_reader::spawn(runtime_events, frontend_events.clone());
        sqlite_reader::spawn(database_changes, frontend_events.clone());
        Self { frontend_events }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.frontend_events.subscribe()
    }
}

pub fn is_frontend_event(value: &Value) -> bool {
    gateway::is_frontend_event(value)
}
