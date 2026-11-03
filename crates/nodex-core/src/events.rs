use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::models::TokenLaunch;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum EngineEvent {
    LaunchDetected(TokenLaunch),
    LaunchRejected { mint_address: String, reason: String },
    EngineStarted { instance_id: Uuid, mode: String },
}
