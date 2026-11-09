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
    pub solana_http_urls: Vec<String>,
    pub solana_ws_urls: Vec<String>,
    pub pump_fun_program_id: String,
}

impl EngineConfig {
    pub fn from_env() -> Self {
        let keys: Vec<String> = (1..=5)
            .filter_map(|index| {
                let name = if index == 1 {
                    "HELIUS_API_KEY".to_owned()
                } else {
                    format!("HELIUS_API_KEY{index}")
                };
                std::env::var(&name)
                    .ok()
                    .map(|value| value.trim().to_owned())
                    .filter(|value| !value.is_empty())
            })
            .collect();

        let mut http_urls = Vec::new();
        let mut ws_urls = Vec::new();
        if let Ok(value) = std::env::var("SOLANA_RPC_HTTP_URL") {
            if !value.trim().is_empty() {
                http_urls.push(value.trim().to_owned());
            }
        }
        if let Ok(value) = std::env::var("SOLANA_RPC_WS_URL") {
            if !value.trim().is_empty() {
                ws_urls.push(value.trim().to_owned());
            }
        }
        for key in &keys {
            let http = format!("https://mainnet.helius-rpc.com/?api-key={key}");
            let ws = format!("wss://mainnet.helius-rpc.com/?api-key={key}");
            if !http_urls.contains(&http) {
                http_urls.push(http);
            }
            if !ws_urls.contains(&ws) {
                ws_urls.push(ws);
            }
        }

        Self {
            bind_addr: std::env::var("NODEX_BIND_ADDR").unwrap_or_else(|_| {
                std::env::var("PORT")
                    .map(|port| format!("0.0.0.0:{port}"))
                    .unwrap_or_else(|_| "127.0.0.1:8080".into())
            }),
            database_url: std::env::var("NODEX_DATABASE_URL")
                .unwrap_or_else(|_| "sqlite://nodex.db".into()),
            mode: EngineMode::Paper,
            paper_starting_balance_lamports: std::env::var("NODEX_PAPER_STARTING_BALANCE_LAMPORTS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10_000_000),
            paper_trade_amount_lamports: std::env::var("NODEX_PAPER_TRADE_AMOUNT_LAMPORTS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(100_000_000),
            solana_http_urls: http_urls,
            solana_ws_urls: ws_urls,
            pump_fun_program_id: std::env::var("PUMPFUN_PROGRAM_ID")
                .unwrap_or_else(|_| "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P".into()),
        }
    }
}
