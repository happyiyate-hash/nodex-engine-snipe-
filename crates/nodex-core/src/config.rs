use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EngineMode {
    Paper,
}

impl Default for EngineMode {
    fn default() -> Self {
        Self::Paper
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    pub bind_addr: String,
    pub database_url: String,
    pub mode: EngineMode,
}

impl EngineConfig {
    pub fn from_env() -> Self {
        Self {
            bind_addr: std::env::var("NODEX_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".into()),
            database_url: std::env::var("NODEX_DATABASE_URL")
                .unwrap_or_else(|_| "sqlite://nodex.db".into()),
            mode: EngineMode::Paper,
        }
    }
}
