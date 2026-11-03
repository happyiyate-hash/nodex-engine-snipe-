use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LaunchSource {
    PumpFun,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenLaunch {
    pub id: Uuid,
    pub mint_address: String,
    pub creator_address: Option<String>,
    pub source: LaunchSource,
    pub signature: String,
    pub slot: u64,
    pub detected_at: DateTime<Utc>,
}

impl TokenLaunch {
    pub fn new(mint_address: String, creator_address: Option<String>, source: LaunchSource, signature: String, slot: u64) -> Self {
        Self { id: Uuid::new_v4(), mint_address, creator_address, source, signature, slot, detected_at: Utc::now() }
    }
}
