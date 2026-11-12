use serde_json::Value;
use tokio::sync::broadcast;

/// Owns the single in-process runtime event channel.
///
/// All backend event consumers subscribe through this type so the channel's
/// creation and fan-out point remain explicit. Existing worker functions can
/// temporarily receive cloned sender handles while they are migrated in later
/// stages; the browser WebSocket gateway remains a subscriber, not a publisher.
#[derive(Clone)]
pub struct EventBroadcaster {
    sender: broadcast::Sender<Value>,
}

impl EventBroadcaster {
    /// Create the shared runtime event channel with a bounded broadcast buffer.
    pub fn new(capacity: usize) -> Self {
        let (sender, _initial_receiver) = broadcast::channel(capacity.max(64));
        Self { sender }
    }

    /// Return a publisher handle for existing worker APIs.
    ///
    /// This compatibility method keeps this stage behavior-preserving. Later
    /// stages can migrate individual workers to publish through this owner.
    pub fn sender(&self) -> broadcast::Sender<Value> {
        self.sender.clone()
    }

    /// Publish one runtime event to every active subscriber.
    pub fn publish(&self, event: Value) -> Result<usize, broadcast::error::SendError<Value>> {
        self.sender.send(event)
    }

    /// Subscribe an independent consumer to the shared runtime event stream.
    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.sender.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn broadcaster_fans_out_events_to_independent_subscribers() {
        let broadcaster = EventBroadcaster::new(8);
        let mut first = broadcaster.subscribe();
        let mut second = broadcaster.subscribe();

        broadcaster
            .publish(json!({"type":"launch_detected","data":{"mint_address":"MintA"}}))
            .expect("at least one receiver is active");

        let expected = json!({"type":"launch_detected","data":{"mint_address":"MintA"}});
        assert_eq!(
            first.recv().await.expect("first subscriber receives event"),
            expected
        );
        assert_eq!(
            second
                .recv()
                .await
                .expect("second subscriber receives event"),
            expected
        );
    }

    #[test]
    fn broadcaster_enforces_a_minimum_buffer_capacity() {
        let broadcaster = EventBroadcaster::new(0);
        let _receiver = broadcaster.subscribe();
        // Construction with a zero capacity must still create a usable channel.
        assert!(broadcaster.publish(json!({"type":"test"})).is_ok());
    }
}
