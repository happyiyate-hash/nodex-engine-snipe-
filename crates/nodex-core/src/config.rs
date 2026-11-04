use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EngineMode {
    #[default]
    Paper,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    pub bind_addr: String,
    pub database_url: String,
    pub mode: EngineMode,
    pub paper_starting_balance_lamports: u64,
    pub paper_trade_amount_lamports: u64,
    pub solana_http_url: Option<String>,
    pub solana_ws_url: Option<String>,
    pub pump_fun_program_id: String,
}

impl EngineConfig {
    pub fn from_env() -> Self {
        let api_key = std::env::var("HELIUS_API_KEY")
            .ok()
            .filter(|value| !value.trim().is_empty());
        let http_url = std::env::var("SOLANA_RPC_HTTP_URL")
            .ok()
            .or_else(|| {
                api_key
                    .as_ref()
                    .map(|key| format!("https://mainnet.helius-rpc.com/?api-key={key}"))
            });
        let ws_url = std::env::var("SOLANA_RPC_WS_URL")
            .ok()
            .or_else(|| {
                api_key
                    .as_ref()
                    .map(|key| format!("wss://mainnet.helius-rpc.com/?api-key={key}"))
            });
        Self {
            bind_addr: std::env::var("NODEX_BIND_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:8080".into()),
            database_url: std::env::var("NODEX_DATABASE_URL")
                .unwrap_or_else(|_| "sqlite://nodex.db".into()),
            mode: EngineMode::Paper,
            paper_starting_balance_lamports: std::env::var(
                "NODEX_PAPER_STARTING_BALANCE_LAMPORTS",
            )
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1_000_000_000),
            paper_trade_amount_lamports: std::env::var(
                "NODEX_PAPER_TRADE_AMOUNT_LAMPORTS",
            )
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(100_000_000),
            solana_http_url: http_url,
            solana_ws_url: ws_url,
            pump_fun_program_id: std::env::var("PUMPFUN_PROGRAM_ID")
                .unwrap_or_else(|_| "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P".into()),
        }
    }
}
