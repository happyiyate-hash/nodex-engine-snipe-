pub mod market;
pub use market::{
    decode_pump_swap_activity_event, decode_pump_swap_activity_log_line,
    decode_pump_swap_pool_account, derive_pump_fun_bonding_curve, derive_pump_swap_pool_address,
    start_market_monitor, MarketMonitor, PumpSwapActivityEvent, PumpSwapPoolAccount,
    WRAPPED_SOL_MINT,
};

pub const PUMPSWAP_PROGRAM_ID: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";

use anyhow::{anyhow, Context, Result};
use futures_util::{stream, SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use solana_pubkey::Pubkey;
use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::{broadcast, Semaphore};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

static RUGCHECK_KEY_INDEX: AtomicUsize = AtomicUsize::new(0);

const PUMP_CREATE_DISCRIMINATOR: [u8; 8] = [0x18, 0x1e, 0xc8, 0x28, 0x05, 0x1c, 0x07, 0x77];
const PUMP_CREATE_V2_DISCRIMINATOR: [u8; 8] = [0xd6, 0x90, 0x4c, 0xec, 0x5f, 0x8b, 0x31, 0xb4];

pub const SPL_TOKEN_ACCOUNT_SIZE: usize = 165;

/// Decode PumpSwap Anchor events only while the PumpSwap program is the active
/// invocation. This prevents accepting identically shaped events emitted by a CPI.
pub fn decode_pump_swap_activity_logs(
    logs: &[String],
    target_program_id: &str,
) -> Result<Vec<PumpSwapActivityEvent>> {
    let mut invocation_stack: Vec<(String, usize)> = Vec::new();
    let mut decoded = Vec::new();

    for line in logs {
        if let Some(rest) = line.strip_prefix("Program ") {
            if let Some((program_id, depth_text)) = rest.split_once(" invoke [") {
                if let Some(depth) = depth_text
                    .strip_suffix(']')
                    .and_then(|value| value.parse::<usize>().ok())
                {
                    while invocation_stack
                        .last()
                        .is_some_and(|(_, active_depth)| *active_depth >= depth)
                    {
                        invocation_stack.pop();
                    }
                    invocation_stack.push((program_id.to_owned(), depth));
                    continue;
                }
            }

            if let Some((program_id, _)) = rest.split_once(" success") {
                if let Some(index) = invocation_stack
                    .iter()
                    .rposition(|(active, _)| active == program_id)
                {
                    invocation_stack.truncate(index);
                }
                continue;
            }

            if let Some((program_id, _)) = rest.split_once(" failed:") {
                if let Some(index) = invocation_stack
                    .iter()
                    .rposition(|(active, _)| active == program_id)
                {
                    invocation_stack.truncate(index);
                }
                continue;
            }
        }

        if invocation_stack
            .last()
            .is_some_and(|(program_id, _)| program_id == target_program_id)
        {
            if let Some(event) = decode_pump_swap_activity_log_line(line)? {
                decoded.push(event);
            }
        }
    }

    Ok(decoded)
}

#[derive(Debug, Clone)]
pub struct SolanaRpc {
    http_urls: Vec<String>,
    active_http_index: Arc<AtomicUsize>,
    request_limit: Arc<Semaphore>,
    client: reqwest::Client,
    activity_events: Option<broadcast::Sender<Value>>,
}

fn redact_provider_value(value: &mut Value) {
    let secrets: Vec<String> = (2..=20)
        .filter_map(|i| std::env::var(format!("HELIUS_API_KEY{i}")).ok())
        .chain(std::env::var("HELIUS_API_KEY").ok())
        .chain((1..=10).filter_map(|i| {
            let name = if i == 1 {
                "FLUXRPC_API_KEY".to_owned()
            } else {
                format!("FLUXRPC_API_KEY{i}")
            };
            std::env::var(name).ok()
        }))
        .chain((2..=10).filter_map(|i| std::env::var(format!("RUGCHECK_API_KEY{i}")).ok()))
        .chain(std::env::var("RUGCHECK_API_KEY").ok())
        .filter(|s| !s.trim().is_empty())
        .collect();
    match value {
        Value::String(text) => {
            for secret in &secrets {
                *text = text.replace(secret, "[REDACTED]");
            }
        }
        Value::Array(items) => items.iter_mut().for_each(redact_provider_value),
        Value::Object(items) => items.values_mut().for_each(redact_provider_value),
        _ => {}
    }
}

impl SolanaRpc {
    pub fn new(http_url: impl Into<String>) -> Self {
        Self::new_with_urls(vec![http_url.into()])
    }

    pub fn new_with_urls(http_urls: Vec<String>) -> Self {
        Self::new_with_urls_and_limit(http_urls, "HELIUS_MAX_CONCURRENT_REQUESTS")
    }

    pub fn new_with_urls_and_limit(http_urls: Vec<String>, limit_env: &str) -> Self {
        let request_limit = std::env::var(limit_env)
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(20)
            .clamp(1, 100);
        Self {
            http_urls,
            active_http_index: Arc::new(AtomicUsize::new(0)),
            request_limit: Arc::new(Semaphore::new(request_limit)),
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(8))
                .build()
                .expect("valid Solana RPC HTTP client configuration"),
            activity_events: None,
        }
    }

    /// Attach the engine event bus so provider requests and responses can be inspected live.
    pub fn with_activity_events(mut self, events: broadcast::Sender<Value>) -> Self {
        self.activity_events = Some(events);
        self
    }

    fn emit_activity(&self, data: Value) {
        if let Some(events) = &self.activity_events {
            let _ = events.send(json!({"type":"engine_activity","data":data}));
        }
    }

    /// Fetch indexed token metadata through the Helius Digital Asset Standard API.
    /// Non-Helius RPC providers may return method-not-found; callers should fall back safely.
    pub async fn get_asset(&self, mint: &str) -> Result<Value> {
        self.call("getAsset", json!([mint])).await
    }

    /// Read the confirmed native SOL balance for a public wallet address.
    pub async fn get_balance_lamports(&self, address: &str) -> Result<u64> {
        let response: Value = self
            .call("getBalance", json!([address, {"commitment":"confirmed"}]))
            .await?;
        response
            .get("value")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("getBalance response omitted lamport value"))
    }

    async fn call<T: for<'de> Deserialize<'de>>(&self, method: &str, params: Value) -> Result<T> {
        self.call_with_response(method, params)
            .await
            .map(|(result, _)| result)
    }

    /// Same provider request path as call(), but also returns the redacted JSON-RPC
    /// response so the token's own security report can preserve the evidence.
    async fn call_with_response<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: Value,
    ) -> Result<(T, Value)> {
        if self.http_urls.is_empty() {
            return Err(anyhow!("No Solana HTTP RPC endpoints are configured"));
        }
        let _request_permit = self
            .request_limit
            .acquire()
            .await
            .map_err(|_| anyhow!("Solana RPC request scheduler is shutting down"))?;
        let mut last_error = None;
        let count = self.http_urls.len();
        let correlation_id = uuid::Uuid::new_v4().to_string();
        let token_mint = params
            .as_array()
            .and_then(|items| items.first())
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut request_params = params.clone();
        redact_provider_value(&mut request_params);

        let start = self.active_http_index.fetch_add(1, Ordering::Relaxed) % count;
        for offset in 0..count {
            let index = (start + offset) % count;
            let url = &self.http_urls[index];
            let attempt_started = std::time::Instant::now();
            let attempt_id = format!("{correlation_id}-{index}");
            let endpoint_host = reqwest::Url::parse(url)
                .ok()
                .and_then(|parsed| parsed.host_str().map(str::to_owned))
                .unwrap_or_else(|| "configured_rpc_endpoint".to_string());
            self.emit_activity(json!({
                "id": attempt_id,
                "correlation_id": correlation_id,
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "direction": "request",
                "provider": "solana_rpc",
                "endpoint": endpoint_host,
                "operation": method,
                "token_mint": token_mint,
                "attempt": index + 1,
                "request": {"jsonrpc":"2.0","method":method,"params":request_params}
            }));
            let response = match self
                .client
                .post(url)
                .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    self.emit_activity(json!({
                        "id": format!("{attempt_id}-response"),
                        "correlation_id": correlation_id,
                        "timestamp": chrono::Utc::now().to_rfc3339(),
                        "direction": "response",
                        "provider": "solana_rpc",
                        "endpoint": endpoint_host,
                        "operation": method,
                        "token_mint": token_mint,
                        "duration_ms": attempt_started.elapsed().as_millis(),
                        "severity": "error",
                        "error": error.to_string()
                    }));
                    last_error = Some(anyhow!(
                        "NODEX_PROVIDER_ERROR_JSON:{}",
                        json!({"provider":"solana_rpc","method":method,"kind":"transport_error","message":error.to_string()})
                    ));
                    continue;
                }
            };
            let status = response.status();
            let body = match response.text().await {
                Ok(body) => body,
                Err(error) => {
                    last_error = Some(anyhow!(
                        "NODEX_PROVIDER_ERROR_JSON:{}",
                        json!({"provider":"solana_rpc","method":method,"kind":"response_read_error","http_status":status.as_u16(),"message":error.to_string()})
                    ));
                    continue;
                }
            };
            let mut response_json = serde_json::from_str::<Value>(&body).unwrap_or_else(
                |_| json!({"raw_text":body.chars().take(65_536).collect::<String>()}),
            );
            redact_provider_value(&mut response_json);
            let response_text = serde_json::to_string(&response_json).unwrap_or_default();
            let body_truncated = response_text.len() > 32_768;
            let response_body = if body_truncated {
                json!({"truncated":true,"preview":response_text.chars().take(32_768).collect::<String>()})
            } else {
                response_json.clone()
            };
            self.emit_activity(json!({
                "id": format!("{attempt_id}-response"),
                "correlation_id": correlation_id,
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "direction": "response",
                "provider": "solana_rpc",
                "endpoint": endpoint_host,
                "operation": method,
                "token_mint": token_mint,
                "duration_ms": attempt_started.elapsed().as_millis(),
                "http_status": status.as_u16(),
                "severity": if status.is_success() { "info" } else { "error" },
                "body_truncated": body_truncated,
                "response": response_body
            }));
            if !status.is_success() {
                last_error = Some(anyhow!(
                    "NODEX_PROVIDER_ERROR_JSON:{}",
                    json!({"provider":"solana_rpc","method":method,"kind":"http_error","http_status":status.as_u16(),"response":response_json})
                ));
                if status.as_u16() == 429
                    || status.is_server_error()
                    || status.as_u16() == 401
                    || status.as_u16() == 403
                {
                    continue;
                }
                return Err(last_error.expect("error was set"));
            }
            let decoded = match serde_json::from_value::<RpcResponse<T>>(response_json.clone()) {
                Ok(decoded) => decoded,
                Err(error) => {
                    return Err(anyhow!(
                        "NODEX_PROVIDER_ERROR_JSON:{}",
                        json!({"provider":"solana_rpc","method":method,"kind":"invalid_json_rpc_response","http_status":status.as_u16(),"decode_error":error.to_string(),"response":response_json})
                    ))
                }
            };
            if let Some(error) = decoded.error {
                let message = error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("JSON-RPC provider error");
                let limited = message.to_ascii_lowercase().contains("rate")
                    || message.to_ascii_lowercase().contains("too many")
                    || message.contains("429");
                last_error = Some(anyhow!(
                    "NODEX_PROVIDER_ERROR_JSON:{}",
                    json!({"provider":"solana_rpc","method":method,"kind":"json_rpc_error","http_status":status.as_u16(),"response":error})
                ));
                if limited {
                    continue;
                }
                return Err(last_error.expect("error was set"));
            }
            if let Some(result) = decoded.result {
                return Ok((result, response_json));
            }
            return Err(anyhow!(
                "NODEX_PROVIDER_ERROR_JSON:{}",
                json!({"provider":"solana_rpc","method":method,"kind":"missing_result","http_status":status.as_u16(),"response":response_json})
            ));
        }
        Err(last_error
            .unwrap_or_else(|| anyhow!("All configured Solana HTTP RPC endpoints failed")))
    }

    /// Fetch one confirmed account's owner and raw bytes for verified program-account decoding.
    pub async fn get_account_data(
        &self,
        address: &str,
    ) -> Result<Option<(String, Vec<u8>, Option<u64>)>> {
        self.get_account_data_with_response(address)
            .await
            .map(|result| result.map(|(owner, bytes, slot, _)| (owner, bytes, slot)))
    }

    /// Fetch account bytes together with the provider's redacted JSON response.
    pub async fn get_account_data_with_response(
        &self,
        address: &str,
    ) -> Result<Option<(String, Vec<u8>, Option<u64>, Value)>> {
        let (account, raw_response): (Value, Value) = self
            .call_with_response(
                "getAccountInfo",
                json!([address, {"encoding":"base64","commitment":"confirmed"}]),
            )
            .await?;
        let slot = account.pointer("/context/slot").and_then(Value::as_u64);
        let Some(value) = account.get("value").filter(|value| !value.is_null()) else {
            return Ok(None);
        };
        let owner = value
            .get("owner")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("getAccountInfo response omitted account owner"))?
            .to_owned();
        let encoded = value
            .pointer("/data/0")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("getAccountInfo response omitted base64 account data"))?;
        let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded)
            .context("decoding account data returned by getAccountInfo")?;
        Ok(Some((owner, bytes, slot, raw_response)))
    }

    /// Current rent-exempt minimum for a standard 165-byte SPL token account.
    pub async fn token_account_rent_lamports(&self) -> Result<u64> {
        self.call(
            "getMinimumBalanceForRentExemption",
            json!([SPL_TOKEN_ACCOUNT_SIZE, {"commitment":"confirmed"}]),
        )
        .await
    }

    /// Fee quote for an already compiled base64 transaction message; never submits it.
    pub async fn fee_for_message(&self, base64_message: &str) -> Result<Option<u64>> {
        self.call(
            "getFeeForMessage",
            json!([base64_message, {"commitment":"confirmed"}]),
        )
        .await
    }

    pub async fn recent_prioritization_fees(&self) -> Result<Vec<PriorityFeeSample>> {
        self.call("getRecentPrioritizationFees", json!([])).await
    }

    /// Resolve standard SPL token metadata from the canonical Metaplex metadata PDA.
    /// This reads on-chain metadata only; it does not trust or fetch arbitrary off-chain URLs.
    pub async fn resolve_metaplex_metadata(
        &self,
        mint: &str,
    ) -> Result<Option<TokenMetadataReport>> {
        const METADATA_PROGRAM_ID: &str = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s";
        let mint_key = Pubkey::from_str(mint).context("invalid token mint address")?;
        let metadata_program = Pubkey::from_str(METADATA_PROGRAM_ID)
            .context("invalid Metaplex metadata program id")?;
        let (metadata_address, _) = Pubkey::find_program_address(
            &[b"metadata", metadata_program.as_ref(), mint_key.as_ref()],
            &metadata_program,
        );
        let account: Value = self.call(
            "getAccountInfo",
            json!([metadata_address.to_string(), {"encoding":"base64","commitment":"confirmed"}]),
        ).await?;
        let Some(encoded) = account.pointer("/value/data/0").and_then(Value::as_str) else {
            return Ok(None);
        };
        let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded)
            .context("decoding Metaplex metadata account")?;
        // MetadataV1: key(1), update authority(32), mint(32), then Borsh strings.
        if bytes.len() < 65 {
            return Err(anyhow!(
                "Metaplex metadata account is shorter than its fixed header"
            ));
        }
        if bytes[33..65] != *mint_key.as_ref() {
            return Err(anyhow!(
                "Metaplex metadata account mint does not match requested mint"
            ));
        }
        let mut offset = 65usize;
        let name = read_metadata_string(&bytes, &mut offset)?
            .trim_end_matches('\0')
            .trim()
            .to_owned();
        let symbol = read_metadata_string(&bytes, &mut offset)?
            .trim_end_matches('\0')
            .trim()
            .to_owned();
        let uri = read_metadata_string(&bytes, &mut offset)?
            .trim_end_matches('\0')
            .trim()
            .to_owned();
        let update_authority = bs58::encode(&bytes[1..33]).into_string();
        Ok(Some(TokenMetadataReport {
            source: "metaplex_metadata_pda".into(),
            update_authority: Some(update_authority),
            mutable: None,
            name: (!name.is_empty()).then_some(name),
            symbol: (!symbol.is_empty()).then_some(symbol),
            uri: (!uri.is_empty()).then_some(uri),
            status: "partial".into(),
            caveat: "Name, symbol and metadata URI were read from the canonical Metaplex metadata PDA. Off-chain image content and authenticity have not been independently verified.".into(),
            metadata_pointer_authority: None,
            metadata_pointer_address: None,
        }))
    }

    /// Resolve token identity from the mint's actual token program before security scanning.
    /// Standard SPL tokens use the Metaplex metadata PDA; Token-2022 tokens use their
    /// embedded TokenMetadata extension when present.
    pub async fn resolve_token_metadata(&self, mint: &str) -> Result<TokenMetadataReport> {
        let Some((owner, data, _slot)) = self.get_account_data(mint).await? else {
            return Err(anyhow!(
                "Mint account was not found while resolving metadata"
            ));
        };
        match owner.as_str() {
            SPL_TOKEN_PROGRAM_ID => {
                Ok(self
                    .resolve_metaplex_metadata(mint)
                    .await?
                    .unwrap_or_else(|| TokenMetadataReport {
                        source: "metaplex_metadata_pda".into(),
                        status: "unavailable".into(),
                        caveat: "No Metaplex metadata account was found for this mint.".into(),
                        ..Default::default()
                    }))
            }
            TOKEN_2022_PROGRAM_ID => parse_token_2022_metadata(&data),
            _ => Err(anyhow!(
                "Mint account is owned by an unsupported token program"
            )),
        }
    }

    /// Re-read only the mint account and authority fields for lightweight lifecycle monitoring.
    /// This deliberately does not repeat holder sampling or external provider requests.
    pub async fn scan_token_authorities(&self, mint: &str) -> Result<TokenSecurityReport> {
        let Some((owner, data, slot)) = self.get_account_data(mint).await? else {
            return Ok(TokenSecurityReport::unverified(
                mint,
                "Mint account was not found during authority recheck",
                None,
            ));
        };
        let mut report = parse_mint_security_report(mint, &owner, &data, slot)?;
        report.enforce_diagnostic_only();
        Ok(report)
    }

    /// Inspect mint authorities and Token-2022 extensions using confirmed account data.
    /// This is the first scanner stage; it deliberately cannot approve a token by itself.
    pub async fn scan_token_security(&self, mint: &str) -> Result<TokenSecurityReport> {
        self.scan_token_security_with_progress(mint, |_, _, _| {})
            .await
    }

    /// Runs the security scan while reporting genuine stage transitions to the caller.
    pub async fn scan_token_security_with_progress<F>(
        &self,
        mint: &str,
        on_progress: F,
    ) -> Result<TokenSecurityReport>
    where
        F: FnMut(&str, u8, &str),
    {
        self.scan_token_security_with_progress_mode(mint, true, on_progress)
            .await
    }

    /// Runs a security scan using the caller's current diagnostic-only setting.
    pub async fn scan_token_security_with_progress_mode<F>(
        &self,
        mint: &str,
        diagnostic_mode: bool,
        mut on_progress: F,
    ) -> Result<TokenSecurityReport>
    where
        F: FnMut(&str, u8, &str),
    {
        on_progress("mint_account", 8, "Checking mint account");
        let (account, raw_account_response): (Value, Value) = self
            .call_with_response(
                "getAccountInfo",
                json!([mint, {"encoding":"base64","commitment":"confirmed"}]),
            )
            .await?;
        let account_evidence = json!({
            "provider":"solana_rpc",
            "method":"getAccountInfo",
            "response":raw_account_response
        });
        let context_slot = account.pointer("/context/slot").and_then(Value::as_u64);
        let value = account.get("value").filter(|v| !v.is_null());
        let Some(value) = value else {
            let mut report = TokenSecurityReport::unverified(
                mint,
                "Mint account was not found by RPC",
                context_slot,
            );
            report.provider_responses.push(account_evidence);
            if diagnostic_mode {
                report.enforce_diagnostic_only();
            }
            return Ok(report);
        };
        let owner = value
            .get("owner")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let encoded = value.pointer("/data/0").and_then(Value::as_str);
        let Some(encoded) = encoded else {
            let mut report = TokenSecurityReport::unverified(
                mint,
                "Mint account data was not returned in base64 form",
                context_slot,
            );
            report.provider_responses.push(account_evidence);
            return Ok(report);
        };
        let bytes =
            match base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded) {
                Ok(bytes) => bytes,
                Err(error) => {
                    let mut report = TokenSecurityReport::unverified(
                        mint,
                        &format!("Mint account base64 data could not be decoded: {error}"),
                        context_slot,
                    );
                    report.provider_responses.push(account_evidence);
                    if diagnostic_mode {
                        report.enforce_diagnostic_only();
                    }
                    return Ok(report);
                }
            };
        let mut report = match parse_mint_security_report(mint, &owner, &bytes, context_slot) {
            Ok(report) => report,
            Err(error) => TokenSecurityReport::unverified(
                mint,
                &format!("Mint account security parsing failed: {error:#}"),
                context_slot,
            ),
        };
        report.provider_responses.push(account_evidence);
        on_progress("authorities", 32, "Checking mint and freeze authorities");
        // Holder sampling is deliberately not part of the fast launch-verification
        // gate. It is optional enrichment and must not hold a launch in diagnostics.
        on_progress("rugcheck", 70, "Checking RugCheck risk report");
        report.rugcheck = self.get_rugcheck_summary(mint).await;
        if !diagnostic_mode {
            if let Some(supply_raw) = report.supply_raw.as_deref() {
                on_progress("holder_distribution", 82, "Sampling the largest token holders");
                match tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    self.sample_holder_distribution(mint, supply_raw),
                )
                .await
                {
                    Ok(Ok(distribution)) => report.holder_distribution = Some(distribution),
                    Ok(Err(error)) => report.checks.push(SecurityCheck {
                        name: "holder_distribution".into(),
                        status: "unverified".into(),
                        severity: "warning".into(),
                        detail: format!("Top-holder sampling was unavailable; paper strategy will require stronger market confirmation: {error}"),
                    }),
                    Err(_) => report.checks.push(SecurityCheck {
                        name: "holder_distribution".into(),
                        status: "unverified".into(),
                        severity: "warning".into(),
                        detail: "Top-holder sampling exceeded its 5-second optional-enrichment budget; paper strategy will require stronger market confirmation.".into(),
                    }),
                }
            }
        }
        let blocking_rugcheck_risks = report
            .rugcheck
            .risks
            .iter()
            .filter(|risk| {
                let level = risk
                    .get("level")
                    .or_else(|| risk.get("severity"))
                    .or_else(|| risk.get("riskLevel"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                matches!(
                    level.as_str(),
                    "danger" | "critical" | "high" | "severe" | "blocker"
                )
            })
            .count();
        let authorities_clear =
            report.mint_authority.is_none() && report.freeze_authority.is_none();
        // RugCheck's normalized field is a risk score (lower is safer).
        // A normalized risk score <= 20 corresponds to the Good / low-risk band.
        let rugcheck_positive = report.rugcheck.status == "available"
            && report
                .rugcheck
                .score_normalised
                .is_some_and(|score| score <= 20)
            && blocking_rugcheck_risks == 0;
        if report.rugcheck.status == "available" {
            report.checks.push(SecurityCheck {
                name: "rugcheck_summary".into(),
                status: if rugcheck_positive { "verified" } else { "warning" }.into(),
                severity: if rugcheck_positive { "info" } else { "high" }.into(),
                detail: format!(
                    "RugCheck report received: normalized risk score {:?}, {} reported risks, {} high-severity risks. Lower normalized risk is better; the low-risk band is required for this launch verdict. RugCheck is not a guarantee against loss.",
                    report.rugcheck.score_normalised,
                    report.rugcheck.risks.len(),
                    blocking_rugcheck_risks
                ),
            });
        } else {
            report.checks.push(SecurityCheck {
                name: "rugcheck_summary".into(),
                status: "unverified".into(),
                severity: "warning".into(),
                detail: report.rugcheck.error.clone().unwrap_or_else(|| {
                    "RugCheck did not return a usable report. The token remains monitored but cannot pass the security gate.".into()
                }),
            });
        }
        if authorities_clear && rugcheck_positive {
            report.decision = "APPROVED".into();
            report.decision_reason = "Mint and freeze authorities are disabled and RugCheck returned an available report with no high-severity risk flags. Optional provider enrichments do not block launch monitoring or the configured paper-entry rules.".into();
        } else {
            report.decision = "UNVERIFIED".into();
            report.decision_reason = if !authorities_clear {
                "Mint or freeze authority remains active. The token stays under market monitoring but is blocked from paper entry.".into()
            } else if report.rugcheck.status != "available" {
                "Mint and freeze authorities passed, but RugCheck is unavailable. The token stays under market monitoring; paper entry waits for a usable risk report.".into()
            } else {
                "RugCheck returned one or more high-severity risk flags. The token stays monitored but is blocked from paper entry.".into()
            };
        }
        // Diagnostic-only mode is controlled by the persisted runtime config.
        if diagnostic_mode {
            report.enforce_diagnostic_only();
        }
        on_progress("complete", 100, "Security scan completed");
        Ok(report)
    }

    async fn sample_holder_distribution(
        &self,
        mint: &str,
        supply_raw: &str,
    ) -> Result<HolderDistribution> {
        const HOLDER_SAMPLE_LIMIT: usize = 20;
        let supply = supply_raw
            .parse::<u128>()
            .map_err(|error| anyhow!("invalid raw mint supply: {error}"))?;
        if supply == 0 {
            return Err(anyhow!("mint supply is zero"));
        }

        let largest: Value = self
            .call(
                "getTokenLargestAccounts",
                json!([mint, {"commitment":"confirmed"}]),
            )
            .await?;
        let accounts = largest
            .pointer("/value")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("getTokenLargestAccounts returned no value array"))?;
        let sampled = accounts.iter().take(HOLDER_SAMPLE_LIMIT).collect::<Vec<_>>();
        if sampled.is_empty() {
            return Err(anyhow!("getTokenLargestAccounts returned no token accounts"));
        }
        let indexed_accounts = sampled
            .iter()
            .enumerate()
            .filter_map(|(index, account)| {
                account
                    .get("address")
                    .and_then(Value::as_str)
                    .map(|address| (index, address.to_owned()))
            })
            .collect::<Vec<_>>();
        if indexed_accounts.is_empty() {
            return Err(anyhow!("largest-account response did not include account addresses"));
        }
        let account_addresses = indexed_accounts
            .iter()
            .map(|(_, address)| address.clone())
            .collect::<Vec<_>>();

        let account_details: Value = self
            .call(
                "getMultipleAccounts",
                json!([account_addresses, {"encoding":"jsonParsed","commitment":"confirmed"}]),
            )
            .await?;
        let resolved = account_details
            .pointer("/value")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("getMultipleAccounts returned no value array"))?;

        let mut owner_balances = HashMap::<String, u128>::new();
        let mut top_account_total = 0_u128;
        let mut resolved_accounts = 0_usize;
        for (response_index, (sample_index, _)) in indexed_accounts.iter().enumerate() {
            let account = sampled[*sample_index];
            let amount = account
                .get("amount")
                .and_then(Value::as_str)
                .and_then(|value| value.parse::<u128>().ok())
                .unwrap_or(0);
            top_account_total = top_account_total.saturating_add(amount);
            let Some(owner) = resolved
                .get(response_index)
                .and_then(|value| value.pointer("/data/parsed/info/owner"))
                .and_then(Value::as_str)
            else {
                continue;
            };
            if amount == 0 {
                continue;
            }
            resolved_accounts = resolved_accounts.saturating_add(1);
            let balance = owner_balances.entry(owner.to_owned()).or_default();
            *balance = balance.saturating_add(amount);
        }

        let mut owner_amounts = owner_balances.into_values().collect::<Vec<_>>();
        owner_amounts.sort_unstable_by(|left, right| right.cmp(left));
        let top_ten_owner_total = owner_amounts
            .iter()
            .take(10)
            .fold(0_u128, |total, amount| total.saturating_add(*amount));

        Ok(HolderDistribution {
            sampled_token_accounts: sampled.len(),
            resolved_accounts,
            top_20_account_share_percent: top_account_total as f64 / supply as f64 * 100.0,
            top_10_observed_owner_share_percent: top_ten_owner_total as f64 / supply as f64 * 100.0,
            // The RPC returns only the largest accounts, not every token account.
            sample_is_partial: true,
            caveat: "Concentration is aggregated by owner across the top-20 token-account sample. The sample is partial; paper entry applies an additional concentration buffer.".into(),
        })
    }

    async fn get_rugcheck_summary(&self, mint: &str) -> RugCheckSummary {
        let refresh = std::env::var("RUGCHECK_REFRESH_REPORTS")
            .map(|value| value.eq_ignore_ascii_case("true") || value == "1")
            .unwrap_or(false);
        let path = format!("/v1/tokens/{mint}/report");
        let url = if refresh {
            format!("https://api.rugcheck.xyz{path}?refresh=true")
        } else {
            format!("https://api.rugcheck.xyz{path}")
        };
        let keys: Vec<String> = (1..=10)
            .filter_map(|index| {
                let name = if index == 1 {
                    "RUGCHECK_API_KEY".to_owned()
                } else {
                    format!("RUGCHECK_API_KEY{index}")
                };
                std::env::var(name)
                    .ok()
                    .map(|key| key.trim().to_owned())
                    .filter(|key| !key.is_empty())
            })
            .collect();
        let key_count = keys.len();
        let start_index = if key_count > 0 {
            RUGCHECK_KEY_INDEX.fetch_add(1, Ordering::Relaxed) % key_count
        } else {
            0
        };
        let attempts = key_count.max(1);
        let activity_id = uuid::Uuid::new_v4().to_string();
        let request_started = std::time::Instant::now();
        self.emit_activity(json!({
            "id": activity_id,
            "correlation_id": activity_id,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "direction": "request",
            "provider": "rugcheck",
            "endpoint": "api.rugcheck.xyz",
            "operation": "GET /v1/tokens/{mint}/report",
            "token_mint": mint,
            "configured_key_count": key_count,
            "refresh_requested": refresh,
            "request": {"method":"GET","path":path}
        }));

        let mut final_response = None;
        let mut last_error = None;
        for offset in 0..attempts {
            let key = if key_count > 0 {
                Some(&keys[(start_index + offset) % key_count])
            } else {
                None
            };
            let mut request = self
                .client
                .get(&url)
                .header(reqwest::header::ACCEPT, "application/json")
                .timeout(std::time::Duration::from_secs(12));
            if let Some(key) = key {
                request = request.header("X-API-KEY", key);
            }
            match request.send().await {
                Ok(response) => {
                    let status = response.status();
                    if (status == reqwest::StatusCode::TOO_MANY_REQUESTS
                        || status == reqwest::StatusCode::UNAUTHORIZED
                        || status == reqwest::StatusCode::FORBIDDEN
                        || status.is_server_error())
                        && offset + 1 < attempts
                    {
                        last_error = Some(format!(
                            "RugCheck returned HTTP {status}; trying another configured key"
                        ));
                        continue;
                    }
                    final_response = Some(response);
                    break;
                }
                Err(error) => {
                    last_error = Some(error.to_string());
                    if offset + 1 == attempts {
                        break;
                    }
                }
            }
        }

        let response = match final_response {
            Some(response) => response,
            None => {
                let message = last_error.unwrap_or_else(|| "RugCheck request failed".into());
                self.emit_activity(json!({
                    "id": format!("{activity_id}-response"),
                    "correlation_id": activity_id,
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                    "direction": "response",
                    "provider": "rugcheck",
                    "endpoint": "api.rugcheck.xyz",
                    "operation": "GET /v1/tokens/{mint}/report",
                    "token_mint": mint,
                    "duration_ms": request_started.elapsed().as_millis(),
                    "severity": "error",
                    "error": message
                }));
                return RugCheckSummary {
                    provider: "rugcheck".into(),
                    status: "unavailable".into(),
                    error: Some(message.clone()),
                    error_details: Some(json!({
                        "provider":"rugcheck",
                        "kind":"request_failed",
                        "configured_key_count":key_count,
                        "message":message
                    })),
                    ..Default::default()
                };
            }
        };
        let status = response.status();
        let body = match response.text().await {
            Ok(body) => body,
            Err(error) => {
                return RugCheckSummary {
                    provider: "rugcheck".into(),
                    status: "unavailable".into(),
                    error: Some(error.to_string()),
                    error_details: Some(json!({
                        "provider":"rugcheck",
                        "kind":"response_read_error",
                        "http_status":status.as_u16(),
                        "message":error.to_string()
                    })),
                    ..Default::default()
                };
            }
        };
        let mut value = serde_json::from_str::<Value>(&body)
            .unwrap_or_else(|_| json!({"raw_text":body.chars().take(65_536).collect::<String>()}));
        redact_provider_value(&mut value);
        let response_text = serde_json::to_string(&value).unwrap_or_default();
        let response_truncated = response_text.len() > 65_536;
        self.emit_activity(json!({
            "id": format!("{activity_id}-response"),
            "correlation_id": activity_id,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "direction": "response",
            "provider": "rugcheck",
            "endpoint": "api.rugcheck.xyz",
            "operation": "GET /v1/tokens/{mint}/report",
            "token_mint": mint,
            "duration_ms": request_started.elapsed().as_millis(),
            "http_status": status.as_u16(),
            "severity": if status.is_success() {"info"} else {"error"},
            "configured_key_count": key_count,
            "body_truncated": response_truncated,
            "response": if response_truncated {
                json!({"truncated":true,"preview":response_text.chars().take(65_536).collect::<String>()})
            } else {
                value.clone()
            }
        }));
        if !status.is_success() {
            let message = format!("RugCheck returned HTTP {status}");
            return RugCheckSummary {
                provider: "rugcheck".into(),
                status: "unavailable".into(),
                error: Some(message.clone()),
                error_details: Some(json!({
                    "provider":"rugcheck",
                    "kind":"http_error",
                    "http_status":status.as_u16(),
                    "response":value.clone()
                })),
                raw_response: Some(value),
                ..Default::default()
            };
        }
        if value.get("raw_text").is_some() {
            return RugCheckSummary {
                provider: "rugcheck".into(),
                status: "unavailable".into(),
                error: Some("RugCheck returned a response that was not valid JSON".into()),
                error_details: Some(json!({
                    "provider":"rugcheck",
                    "kind":"invalid_json_response",
                    "http_status":status.as_u16(),
                    "response":value
                })),
                raw_response: Some(value),
                ..Default::default()
            };
        }
        let lp_locked_pct = value
            .get("lpLockedPct")
            .and_then(Value::as_f64)
            .or_else(|| {
                value
                    .get("markets")
                    .and_then(Value::as_array)
                    .and_then(|markets| markets.first())
                    .and_then(|market| market.get("lp"))
                    .and_then(|lp| lp.get("lpLockedPct"))
                    .and_then(Value::as_f64)
            });
        RugCheckSummary {
            provider: "rugcheck".into(),
            status: "available".into(),
            score: value.get("score").and_then(Value::as_u64),
            score_normalised: value.get("score_normalised").and_then(Value::as_u64),
            lp_locked_pct,
            risks: value
                .get("risks")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
            error: None,
            error_details: None,
            // Preserve the full report, including tokenMeta, token authorities,
            // top holders, markets, lockers, creator tokens and insider networks.
            raw_response: Some(value),
        }
    }

    /// Samples a bounded number of recent creator transactions and counts prior Pump.fun launches.
    /// This is partial and does not establish wallet clusters or detect token sales.
    pub async fn scan_creator_history(
        &self,
        creator: &str,
        current_signature: &str,
        pump_fun_program_id: &str,
    ) -> Result<CreatorHistoryReport> {
        const SAMPLE_LIMIT: usize = 12;
        let signatures: Vec<Value> = self
            .call(
                "getSignaturesForAddress",
                json!([creator, {"limit": SAMPLE_LIMIT, "commitment":"confirmed"}]),
            )
            .await?;
        let mut report = CreatorHistoryReport {
            creator_address: creator.to_owned(), signatures_sampled: signatures.len(), transactions_resolved: 0,
            prior_pump_fun_launches: 0, sample_is_partial: true, early_selling: "UNVERIFIED".into(),
            status: "partial".into(),
            caveat: "Only a bounded sample of recent creator-address transactions is inspected. Linked wallets are not identified and creator token sales are not detected.".into(),
        };
        for entry in signatures {
            let Some(signature) = entry.get("signature").and_then(Value::as_str) else {
                continue;
            };
            if signature == current_signature {
                continue;
            }
            let Some(transaction) = self.get_transaction(signature).await? else {
                continue;
            };
            report.transactions_resolved += 1;
            if let Some(launch) =
                decode_pump_fun_create(&transaction, signature, pump_fun_program_id)?
            {
                if launch.creator_address == creator {
                    report.prior_pump_fun_launches += 1;
                }
            }
        }
        if report.transactions_resolved == 0 {
            report.status = "unverified".into();
            report.caveat = "No sampled creator transactions could be resolved. Creator history and early selling remain UNVERIFIED.".into();
        }
        Ok(report)
    }

    /// Check whether a signature is still available from the RPC node's transaction history.
    pub async fn signature_status_available(&self, signature: &str) -> Result<bool> {
        let response: Value = self
            .call(
                "getSignatureStatuses",
                json!([[signature], {"searchTransactionHistory":true}]),
            )
            .await?;
        Ok(response
            .pointer("/value/0")
            .is_some_and(|status| !status.is_null()))
    }

    /// Return recent signatures for an address, optionally bounded by before/until cursors.
    /// The RPC limit is capped at Solana's documented maximum of 1,000 entries.
    pub async fn get_signatures_for_address(
        &self,
        address: &str,
        before: Option<&str>,
        until: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Value>> {
        let mut config = json!({
            "commitment":"confirmed",
            "limit":limit.clamp(1, 1_000)
        });
        if let Some(before) = before {
            config["before"] = Value::String(before.to_owned());
        }
        if let Some(until) = until {
            config["until"] = Value::String(until.to_owned());
        }
        self.call("getSignaturesForAddress", json!([address, config]))
            .await
    }

    /// Fetches a confirmed transaction in raw JSON form, including version-0 transactions.
    /// A null result is returned as None because RPC nodes may not have indexed it yet.
    pub async fn get_transaction(&self, signature: &str) -> Result<Option<Value>> {
        if self.http_urls.is_empty() {
            return Err(anyhow!("No Solana HTTP RPC endpoints are configured"));
        }
        let _request_permit = self
            .request_limit
            .acquire()
            .await
            .map_err(|_| anyhow!("Solana RPC request scheduler is shutting down"))?;
        let mut last_error = None;
        let count = self.http_urls.len();
        let start = self.active_http_index.fetch_add(1, Ordering::Relaxed) % count;
        for offset in 0..count {
            let index = (start + offset) % count;
            let url = &self.http_urls[index];
            let response = match self.client.post(url)
                .json(&json!({"jsonrpc":"2.0","id":1,"method":"getTransaction","params":[signature,{"encoding":"json","commitment":"confirmed","maxSupportedTransactionVersion":1}]}))
                .send().await {
                Ok(response) => response,
                Err(error) => {
                    last_error = Some(anyhow!("sending Solana getTransaction request: {error}"));
                    continue;
                }
            };
            let status = response.status();
            if !status.is_success() {
                last_error = Some(anyhow!("Solana getTransaction returned HTTP {status}"));
                if status.as_u16() == 429
                    || status.is_server_error()
                    || status.as_u16() == 401
                    || status.as_u16() == 403
                {
                    continue;
                }
                return Err(last_error.expect("error was set"));
            }
            let decoded = response
                .json::<RpcResponse<Value>>()
                .await
                .context("decoding getTransaction response")?;
            if let Some(error) = decoded.error {
                let message = error
                    .get("message")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .unwrap_or_else(|| error.to_string());
                let limited = message.to_ascii_lowercase().contains("rate")
                    || message.to_ascii_lowercase().contains("too many")
                    || message.contains("429");
                last_error = Some(anyhow!("Solana RPC getTransaction error: {message}"));
                if limited {
                    continue;
                }
                return Err(last_error.expect("error was set"));
            }
            return Ok(decoded.result.filter(|value| !value.is_null()));
        }
        Err(last_error
            .unwrap_or_else(|| anyhow!("All configured Solana HTTP RPC endpoints failed")))
    }
}

pub const SPL_TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
pub const TOKEN_2022_PROGRAM_ID: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HolderDistribution {
    pub sampled_token_accounts: usize,
    pub resolved_accounts: usize,
    pub top_20_account_share_percent: f64,
    pub top_10_observed_owner_share_percent: f64,
    pub sample_is_partial: bool,
    pub caveat: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatorHistoryReport {
    pub creator_address: String,
    pub signatures_sampled: usize,
    pub transactions_resolved: usize,
    pub prior_pump_fun_launches: usize,
    pub sample_is_partial: bool,
    pub early_selling: String,
    pub status: String,
    pub caveat: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityCheck {
    pub name: String,
    pub status: String,
    pub severity: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TokenMetadataReport {
    pub source: String,
    pub update_authority: Option<String>,
    pub mutable: Option<bool>,
    pub name: Option<String>,
    pub symbol: Option<String>,
    pub uri: Option<String>,
    pub metadata_pointer_authority: Option<String>,
    pub metadata_pointer_address: Option<String>,
    pub status: String,
    pub caveat: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RugCheckSummary {
    pub provider: String,
    pub status: String,
    pub score: Option<u64>,
    pub score_normalised: Option<u64>,
    pub lp_locked_pct: Option<f64>,
    pub risks: Vec<Value>,
    pub error: Option<String>,
    #[serde(default)]
    pub error_details: Option<Value>,
    /// Redacted provider JSON, retained on the token's own security report for debugging.
    #[serde(default)]
    pub raw_response: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenSecurityReport {
    pub mint_address: String,
    pub scanned_at: String,
    pub context_slot: Option<u64>,
    pub token_program: String,
    pub supply_raw: Option<String>,
    pub decimals: Option<u8>,
    pub mint_authority: Option<String>,
    pub freeze_authority: Option<String>,
    pub extensions: Vec<String>,
    pub holder_distribution: Option<HolderDistribution>,
    #[serde(default)]
    pub creator_history: Option<CreatorHistoryReport>,
    pub metadata: TokenMetadataReport,
    pub checks: Vec<SecurityCheck>,
    pub decision: String,
    pub decision_reason: String,
    /// True while NodeX collects provider evidence without making verification decisions.
    #[serde(default)]
    pub diagnostic_only: bool,
    /// The parser's provisional decision, retained for debugging only.
    #[serde(default)]
    pub diagnostic_assessment: Option<Value>,
    #[serde(default)]
    pub provider_responses: Vec<Value>,
    pub rugcheck: RugCheckSummary,
}

impl TokenSecurityReport {
    fn enforce_diagnostic_only(&mut self) {
        self.diagnostic_assessment = Some(json!({
            "decision":self.decision,
            "decision_reason":self.decision_reason
        }));
        self.diagnostic_only = true;
        self.decision = "UNVERIFIED".into();
        self.decision_reason = "Diagnostic-only mode is active. Provider requests and checks are retained for inspection, but NodeX is not verifying or approving any token. Inspect diagnostic_assessment and provider_responses.".into();
    }

    fn unverified(mint: &str, reason: &str, context_slot: Option<u64>) -> Self {
        Self {
            mint_address: mint.to_owned(),
            scanned_at: chrono::Utc::now().to_rfc3339(),
            context_slot,
            token_program: "unknown".into(),
            supply_raw: None,
            decimals: None,
            mint_authority: None,
            freeze_authority: None,
            extensions: vec![],
            holder_distribution: None,
            creator_history: None,
            metadata: TokenMetadataReport {
                status: "unverified".into(),
                caveat: "Mint account metadata was not available.".into(),
                ..Default::default()
            },
            checks: vec![SecurityCheck {
                name: "mint_account".into(),
                status: "unverified".into(),
                severity: "critical".into(),
                detail: reason.into(),
            }],
            decision: "UNVERIFIED".into(),
            decision_reason: reason.into(),
            diagnostic_only: false,
            diagnostic_assessment: None,
            provider_responses: Vec::new(),
            rugcheck: RugCheckSummary {
                provider: "rugcheck".into(),
                status: "not_run".into(),
                ..Default::default()
            },
        }
    }
}

fn read_coption_pubkey(data: &[u8], offset: usize) -> Result<Option<String>> {
    let tag = data
        .get(offset..offset + 4)
        .ok_or_else(|| anyhow!("mint account is shorter than the SPL mint layout"))?;
    let tag = u32::from_le_bytes(tag.try_into().expect("four-byte slice"));
    match tag {
        0 => Ok(None),
        1 => {
            let key = data
                .get(offset + 4..offset + 36)
                .ok_or_else(|| anyhow!("mint authority public key is truncated"))?;
            Ok(Some(bs58::encode(key).into_string()))
        }
        other => Err(anyhow!(
            "invalid COption public-key tag {other} in mint account"
        )),
    }
}

fn token_2022_extension_name(kind: u16) -> &'static str {
    match kind {
        1 => "transfer_fee_config",
        2 => "transfer_fee_amount",
        3 => "mint_close_authority",
        4 => "confidential_transfer_mint",
        5 => "confidential_transfer_account",
        6 => "default_account_state",
        7 => "immutable_owner",
        8 => "memo_transfer",
        9 => "non_transferable",
        10 => "interest_bearing_config",
        11 => "cpi_guard",
        12 => "permanent_delegate",
        13 => "non_transferable_account",
        14 => "transfer_hook",
        15 => "transfer_hook_account",
        16 => "confidential_transfer_fee_config",
        17 => "confidential_transfer_fee_amount",
        18 => "metadata_pointer",
        19 => "token_metadata",
        20 => "group_pointer",
        21 => "token_group",
        22 => "group_member_pointer",
        23 => "token_group_member",
        24 => "confidential_mint_burn",
        25 => "scaled_ui_amount",
        26 => "pausable",
        _ => "unknown_extension",
    }
}

const TOKEN_2022_MINT_BASE_LEN: usize = 82;
const TOKEN_2022_ACCOUNT_TYPE_OFFSET: usize = 165;
const TOKEN_2022_TLV_OFFSET: usize = 166;
const TOKEN_2022_MINT_ACCOUNT_TYPE: u8 = 1;

fn token_2022_tlv_start(data: &[u8]) -> Result<Option<usize>> {
    // A mint without extensions uses the original 82-byte SPL Mint layout.
    if data.len() == TOKEN_2022_MINT_BASE_LEN {
        return Ok(None);
    }
    // Extended mints are padded to 165 bytes, carry AccountType::Mint at
    // offset 165, and start TLV records at offset 166.
    if data.len() < TOKEN_2022_TLV_OFFSET {
        return Err(anyhow!("Token-2022 mint extension envelope is truncated"));
    }
    if data[TOKEN_2022_MINT_BASE_LEN..TOKEN_2022_ACCOUNT_TYPE_OFFSET]
        .iter()
        .any(|byte| *byte != 0)
    {
        return Err(anyhow!("Token-2022 mint padding is invalid"));
    }
    if data[TOKEN_2022_ACCOUNT_TYPE_OFFSET] != TOKEN_2022_MINT_ACCOUNT_TYPE {
        return Err(anyhow!("Token-2022 account type is not Mint"));
    }
    Ok(Some(TOKEN_2022_TLV_OFFSET))
}

fn parse_token_2022_extensions(data: &[u8]) -> Result<Vec<String>> {
    let Some(mut offset) = token_2022_tlv_start(data)? else {
        return Ok(vec![]);
    };
    let mut extensions = Vec::new();
    while offset < data.len() {
        if data[offset..].iter().all(|byte| *byte == 0) {
            break;
        }
        let header = data
            .get(offset..offset + 4)
            .ok_or_else(|| anyhow!("Token-2022 extension TLV header is truncated"))?;
        let kind = u16::from_le_bytes([header[0], header[1]]);
        let length = u16::from_le_bytes([header[2], header[3]]) as usize;
        offset += 4;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| anyhow!("Token-2022 extension length overflow"))?;
        if end > data.len() {
            return Err(anyhow!("Token-2022 extension value is truncated"));
        }
        extensions.push(token_2022_extension_name(kind).to_owned());
        offset = end;
    }
    Ok(extensions)
}

fn optional_nonzero_pubkey(data: &[u8]) -> Result<Option<String>> {
    if data.len() < 32 {
        return Err(anyhow!("metadata public key field is truncated"));
    }
    if data[..32].iter().all(|byte| *byte == 0) {
        Ok(None)
    } else {
        Ok(Some(bs58::encode(&data[..32]).into_string()))
    }
}

fn read_metadata_string(data: &[u8], offset: &mut usize) -> Result<String> {
    let length_bytes = data
        .get(*offset..*offset + 4)
        .ok_or_else(|| anyhow!("metadata string length is truncated"))?;
    let length =
        u32::from_le_bytes(length_bytes.try_into().expect("four-byte metadata length")) as usize;
    *offset += 4;
    let end = (*offset)
        .checked_add(length)
        .ok_or_else(|| anyhow!("metadata string length overflow"))?;
    let bytes = data
        .get(*offset..end)
        .ok_or_else(|| anyhow!("metadata string value is truncated"))?;
    *offset = end;
    String::from_utf8(bytes.to_vec()).context("metadata string is not valid UTF-8")
}

fn parse_token_2022_metadata(data: &[u8]) -> Result<TokenMetadataReport> {
    let mut report = TokenMetadataReport {
        source: "token_2022_extensions".into(),
        status: "unverified".into(),
        caveat: "No on-mint TokenMetadata extension was found; external metadata accounts are not yet decoded.".into(),
        ..Default::default()
    };
    let Some(mut offset) = token_2022_tlv_start(data)? else {
        return Ok(report);
    };
    while offset + 4 <= data.len() {
        if data[offset..].iter().all(|byte| *byte == 0) {
            break;
        }
        let kind = u16::from_le_bytes([data[offset], data[offset + 1]]);
        let length = u16::from_le_bytes([data[offset + 2], data[offset + 3]]) as usize;
        offset += 4;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| anyhow!("metadata extension length overflow"))?;
        let value = data
            .get(offset..end)
            .ok_or_else(|| anyhow!("metadata extension value is truncated"))?;
        match kind {
            18 => {
                if value.len() < 64 {
                    return Err(anyhow!("metadata pointer extension is truncated"));
                }
                report.metadata_pointer_authority = optional_nonzero_pubkey(&value[..32])?;
                report.metadata_pointer_address = optional_nonzero_pubkey(&value[32..64])?;
            }
            19 => {
                if value.len() < 64 {
                    return Err(anyhow!(
                        "TokenMetadata extension is shorter than its fixed fields"
                    ));
                }
                report.update_authority = optional_nonzero_pubkey(&value[..32])?;
                let mut cursor = 64usize;
                report.name = Some(read_metadata_string(value, &mut cursor)?);
                report.symbol = Some(read_metadata_string(value, &mut cursor)?);
                report.uri = Some(read_metadata_string(value, &mut cursor)?);
                report.mutable = Some(report.update_authority.is_some());
                report.status = "partial".into();
                report.caveat = "Embedded Token-2022 metadata was parsed. Update authority presence is used as the mutability indicator; external URI content, image integrity and impersonation are not verified.".into();
            }
            _ => {}
        }
        offset = end;
    }
    Ok(report)
}

pub fn parse_mint_security_report(
    mint: &str,
    owner: &str,
    data: &[u8],
    context_slot: Option<u64>,
) -> Result<TokenSecurityReport> {
    if data.len() < 82 {
        return Err(anyhow!(
            "mint account data is shorter than the 82-byte SPL Mint layout"
        ));
    }
    let token_program = match owner {
        SPL_TOKEN_PROGRAM_ID => "spl_token",
        TOKEN_2022_PROGRAM_ID => "token_2022",
        _ => {
            return Ok(TokenSecurityReport::unverified(
                mint,
                "Mint account is owned by an unrecognized token program",
                context_slot,
            ))
        }
    };
    let mint_authority = read_coption_pubkey(data, 0)?;
    let supply = u64::from_le_bytes(data[36..44].try_into().expect("eight-byte supply"));
    let decimals = data[44];
    let freeze_authority = read_coption_pubkey(data, 46)?;
    let (extensions, extension_error) = if token_program == "token_2022" {
        match parse_token_2022_extensions(data) {
            Ok(extensions) => (extensions, None),
            Err(error) => (vec![], Some(error.to_string())),
        }
    } else {
        (vec![], None)
    };
    let metadata = if token_program == "token_2022" {
        if let Some(error) = extension_error.as_deref() {
            TokenMetadataReport {
                source: "token_2022_extensions".into(),
                status: "unverified".into(),
                caveat: format!("Token-2022 extension data could not be verified: {error}"),
                ..Default::default()
            }
        } else {
            match parse_token_2022_metadata(data) {
                Ok(metadata) => metadata,
                Err(error) => TokenMetadataReport {
                    source: "token_2022_extensions".into(),
                    status: "unverified".into(),
                    caveat: format!("Token-2022 metadata could not be verified: {error}"),
                    ..Default::default()
                },
            }
        }
    } else {
        TokenMetadataReport {
            source: "metaplex_external_not_yet_supported".into(),
            status: "unverified".into(),
            caveat: "Standard SPL metadata PDA and off-chain metadata are not yet fetched; mutability cannot be determined.".into(),
            ..Default::default()
        }
    };
    let mut checks = Vec::new();
    if let Some(error) = extension_error {
        checks.push(SecurityCheck {
            name: "token_2022_extensions".into(),
            status: "unverified".into(),
            severity: "critical".into(),
            detail: format!("Token-2022 extensions could not be verified; the token must remain blocked: {error}"),
        });
    }
    checks.push(SecurityCheck {
        name: "mint_authority".into(),
        status: if mint_authority.is_none() { "passed" } else { "failed" }.into(),
        severity: if mint_authority.is_none() { "info" } else { "critical" }.into(),
        detail: mint_authority.as_ref().map(|key| format!("Mint authority remains active: {key}")).unwrap_or_else(|| "Mint authority is disabled; additional supply cannot be minted through this authority.".into()),
    });
    checks.push(SecurityCheck {
        name: "freeze_authority".into(),
        status: if freeze_authority.is_none() {
            "passed"
        } else {
            "failed"
        }
        .into(),
        severity: if freeze_authority.is_none() {
            "info"
        } else {
            "critical"
        }
        .into(),
        detail: freeze_authority
            .as_ref()
            .map(|key| format!("Freeze authority remains active: {key}"))
            .unwrap_or_else(|| "Freeze authority is disabled.".into()),
    });
    for extension in &extensions {
        let high_risk = matches!(
            extension.as_str(),
            "permanent_delegate"
                | "transfer_hook"
                | "default_account_state"
                | "non_transferable"
                | "pausable"
                | "mint_close_authority"
        );
        checks.push(SecurityCheck {
            name: format!("token_2022_{extension}"),
            status: if high_risk { "warning" } else { "info" }.into(),
            severity: if high_risk { "high" } else { "info" }.into(),
            detail: format!("Token-2022 extension detected: {extension}. Its configuration and authority require extension-specific analysis."),
        });
    }
    checks.push(SecurityCheck {
        name: "holder_distribution".into(),
        status: "unverified".into(),
        severity: "high".into(),
        detail: "Top holder ownership and wallet clustering have not yet been analyzed.".into(),
    });
    checks.push(SecurityCheck {
        name: "creator_history".into(),
        status: "unverified".into(),
        severity: "high".into(),
        detail: "Creator wallet history and previous launch behaviour have not yet been analyzed."
            .into(),
    });
    checks.push(SecurityCheck {
        name: "metadata".into(),
        status: metadata.status.clone(),
        severity: if metadata.status == "partial" {
            "medium"
        } else {
            "high"
        }
        .into(),
        detail: metadata.caveat.clone(),
    });
    checks.push(SecurityCheck {
        name: "liquidity".into(),
        status: "unverified".into(),
        severity: "critical".into(),
        detail: "Pool reserves, LP ownership and liquidity depth are not yet verified.".into(),
    });
    checks.push(SecurityCheck {
        name: "sellability".into(),
        status: "unverified".into(),
        severity: "critical".into(),
        detail: "A live sell quote and transaction simulation have not yet been performed.".into(),
    });
    // Active authorities are a serious risk signal, but at launch they can change.
    // Keep the token under observation instead of making this first snapshot terminal.
    // UNVERIFIED always blocks paper-entry eligibility until the full policy passes.
    let authority_pending = mint_authority.is_some() || freeze_authority.is_some();
    let decision = "UNVERIFIED";
    let decision_reason = if authority_pending {
        "Mint or freeze authority is still active. This is a high-risk warning and will be rechecked; automatic paper entry remains blocked.".into()
    } else {
        "Authority checks passed, but holder, creator, metadata, liquidity and sellability checks remain incomplete. Automatic paper entry remains blocked.".into()
    };
    Ok(TokenSecurityReport {
        mint_address: mint.to_owned(),
        scanned_at: chrono::Utc::now().to_rfc3339(),
        context_slot,
        token_program: token_program.into(),
        supply_raw: Some(supply.to_string()),
        decimals: Some(decimals),
        mint_authority,
        freeze_authority,
        extensions,
        holder_distribution: None,
        creator_history: None,
        metadata,
        checks,
        decision: decision.into(),
        decision_reason,
        diagnostic_only: false,
        diagnostic_assessment: None,
        provider_responses: Vec::new(),
        rugcheck: RugCheckSummary {
            provider: "rugcheck".into(),
            status: "not_run".into(),
            ..Default::default()
        },
    })
}

/// Confirmed Pump.fun TradeEvent fields needed for activity and volume analytics.
/// The decoder consumes the stable IDL prefix and tolerates appended event fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PumpFunTradeEvent {
    pub mint_address: String,
    pub sol_amount_lamports: u64,
    pub token_amount_raw: u64,
    pub is_buy: bool,
    pub trader_address: String,
    pub timestamp: i64,
}

const PUMP_FUN_TRADE_EVENT_DISCRIMINATOR: [u8; 8] = [189, 219, 127, 211, 78, 230, 97, 238];

/// Decode Pump.fun TradeEvent logs only while the configured Pump.fun program
/// is the active invocation. The first 89 Borsh bytes are stable in the official
/// IDL; newer appended fields are intentionally ignored.
pub fn decode_pump_fun_trade_logs(
    logs: &[String],
    target_program_id: &str,
) -> Result<Vec<PumpFunTradeEvent>> {
    let mut invocation_stack: Vec<(String, usize)> = Vec::new();
    let mut decoded = Vec::new();

    for line in logs {
        if let Some(rest) = line.strip_prefix("Program ") {
            if let Some((program_id, depth_text)) = rest.split_once(" invoke [") {
                if let Some(depth) = depth_text
                    .strip_suffix(']')
                    .and_then(|value| value.parse::<usize>().ok())
                {
                    while invocation_stack
                        .last()
                        .is_some_and(|(_, active_depth)| *active_depth >= depth)
                    {
                        invocation_stack.pop();
                    }
                    invocation_stack.push((program_id.to_owned(), depth));
                    continue;
                }
            }
            if let Some((program_id, _)) = rest.split_once(" success") {
                if let Some(index) = invocation_stack
                    .iter()
                    .rposition(|(active, _)| active == program_id)
                {
                    invocation_stack.truncate(index);
                }
                continue;
            }
            if let Some((program_id, _)) = rest.split_once(" failed:") {
                if let Some(index) = invocation_stack
                    .iter()
                    .rposition(|(active, _)| active == program_id)
                {
                    invocation_stack.truncate(index);
                }
                continue;
            }
        }

        if !invocation_stack
            .last()
            .is_some_and(|(program_id, _)| program_id == target_program_id)
        {
            continue;
        }
        let Some(encoded) = line.strip_prefix("Program data: ") else {
            continue;
        };
        let bytes =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded.trim())
                .context("decoding Pump.fun TradeEvent base64")?;
        if bytes.len() < 97 || bytes[..8] != PUMP_FUN_TRADE_EVENT_DISCRIMINATOR {
            continue;
        }
        let is_buy = match bytes[56] {
            0 => false,
            1 => true,
            value => return Err(anyhow!("invalid Pump.fun TradeEvent is_buy flag: {value}")),
        };
        decoded.push(PumpFunTradeEvent {
            mint_address: bs58::encode(&bytes[8..40]).into_string(),
            sol_amount_lamports: u64::from_le_bytes(
                bytes[40..48].try_into().expect("eight-byte SOL amount"),
            ),
            token_amount_raw: u64::from_le_bytes(
                bytes[48..56].try_into().expect("eight-byte token amount"),
            ),
            is_buy,
            trader_address: bs58::encode(&bytes[57..89]).into_string(),
            timestamp: i64::from_le_bytes(bytes[89..97].try_into().expect("eight-byte timestamp")),
        });
    }
    Ok(decoded)
}

/// A Pump.fun create/create_v2 instruction decoded from a confirmed transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecodedPumpFunCreate {
    pub mint_address: String,
    pub creator_address: String,
    pub slot: u64,
}

/// Scans top-level and CPI instructions for documented Pump.fun create discriminators.
/// Account indexes are resolved against static keys plus v0 loaded addresses.
pub fn decode_pump_fun_create(
    transaction: &Value,
    signature: &str,
    configured_program_id: &str,
) -> Result<Option<DecodedPumpFunCreate>> {
    let meta = transaction
        .get("meta")
        .filter(|value| !value.is_null())
        .ok_or_else(|| anyhow!("getTransaction response is missing transaction metadata"))?;
    let execution_error = meta
        .get("err")
        .ok_or_else(|| anyhow!("getTransaction response is missing execution status"))?;
    if !execution_error.is_null() {
        return Ok(None);
    }
    let signatures = transaction
        .pointer("/transaction/signatures")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("getTransaction response has no transaction signatures"))?;
    if !signatures
        .iter()
        .any(|value| value.as_str() == Some(signature))
    {
        return Err(anyhow!(
            "requested signature does not match the returned transaction"
        ));
    }
    let message = transaction
        .pointer("/transaction/message")
        .ok_or_else(|| anyhow!("getTransaction response has no transaction.message"))?;
    let mut keys: Vec<String> = message
        .get("accountKeys")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("transaction message has no accountKeys array"))?
        .iter()
        .map(|key| {
            key.as_str()
                .map(str::to_owned)
                .or_else(|| key.get("pubkey").and_then(Value::as_str).map(str::to_owned))
                .ok_or_else(|| anyhow!("transaction contains an invalid account key"))
        })
        .collect::<Result<Vec<_>>>()?;
    if let Some(loaded) = transaction.pointer("/meta/loadedAddresses") {
        for field in ["writable", "readonly"] {
            if let Some(addresses) = loaded.get(field).and_then(Value::as_array) {
                for address in addresses {
                    if let Some(address) = address.as_str() {
                        keys.push(address.to_owned());
                    }
                }
            }
        }
    }
    let mut instructions: Vec<&Value> = Vec::new();
    if let Some(top) = message.get("instructions").and_then(Value::as_array) {
        instructions.extend(top.iter());
    }
    if let Some(groups) = transaction
        .pointer("/meta/innerInstructions")
        .and_then(Value::as_array)
    {
        for group in groups {
            if let Some(inner) = group.get("instructions").and_then(Value::as_array) {
                instructions.extend(inner.iter());
            }
        }
    }
    for instruction in instructions {
        let Some(program_index) = instruction.get("programIdIndex").and_then(Value::as_u64) else {
            continue;
        };
        let Some(program_id) = keys.get(program_index as usize) else {
            continue;
        };
        if program_id != configured_program_id {
            continue;
        }
        let Some(data) = instruction.get("data").and_then(Value::as_str) else {
            continue;
        };
        let bytes = bs58::decode(data)
            .into_vec()
            .context("decoding Pump.fun instruction data")?;
        if bytes.len() < 8 {
            continue;
        }
        let discriminator: [u8; 8] = bytes[..8].try_into().expect("checked discriminator length");
        if discriminator != PUMP_CREATE_DISCRIMINATOR
            && discriminator != PUMP_CREATE_V2_DISCRIMINATOR
        {
            continue;
        }
        let accounts = instruction
            .get("accounts")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("Pump.fun create instruction has no accounts array"))?;
        let mint_index = accounts
            .first()
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("Pump.fun create instruction has no mint account index"))?;
        let mint_address = keys
            .get(mint_index as usize)
            .ok_or_else(|| anyhow!("Pump.fun mint account index is out of range"))?
            .clone();
        let creator_address = decode_create_creator(&bytes[8..])?;
        let slot = transaction
            .get("slot")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        if signature.trim().is_empty() {
            return Err(anyhow!("transaction signature is empty"));
        }
        return Ok(Some(DecodedPumpFunCreate {
            mint_address,
            creator_address,
            slot,
        }));
    }
    Ok(None)
}

fn decode_create_creator(args: &[u8]) -> Result<String> {
    let mut offset = 0usize;
    for label in ["name", "symbol", "uri"] {
        let length_bytes = args
            .get(offset..offset + 4)
            .ok_or_else(|| anyhow!("Pump.fun create instruction is missing {label} length"))?;
        let length = u32::from_le_bytes(length_bytes.try_into().expect("four-byte slice")) as usize;
        offset += 4;
        let value = args
            .get(offset..offset + length)
            .ok_or_else(|| anyhow!("Pump.fun create instruction is missing {label} bytes"))?;
        std::str::from_utf8(value).with_context(|| format!("Pump.fun {label} is not UTF-8"))?;
        offset += length;
    }
    let creator = args
        .get(offset..offset + 32)
        .ok_or_else(|| anyhow!("Pump.fun create instruction is missing creator pubkey"))?;
    Ok(bs58::encode(creator).into_string())
}

#[derive(Debug, Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorityFeeSample {
    pub slot: u64,
    #[serde(rename = "prioritizationFee")]
    pub prioritization_fee: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StreamStatus {
    pub connected: bool,
    pub subscription_id: Option<u64>,
    pub reconnect_count: u64,
    pub last_error: Option<String>,
    pub program_id: Option<String>,
    pub last_message_at: Option<String>,
}

const MAX_BACKFILL_PAGES: usize = 10;
const SIGNATURE_PAGE_SIZE: usize = 1_000;
const BACKFILL_CONCURRENCY: usize = 8;

fn backfilled_log_notification(
    program_id: &str,
    signature: String,
    slot: u64,
    err: Value,
    logs: Vec<String>,
) -> Value {
    json!({
        "jsonrpc":"2.0",
        "method":"logsNotification",
        "nodex_program_id":program_id,
        "nodex_backfilled":true,
        "params":{"result":{
            "context":{"slot":slot},
            "value":{"signature":signature,"err":err,"logs":logs}
        }}
    })
}

/// Recover confirmed program transactions after the last fully processed signature.
/// A fresh WebSocket subscription is established first, so transactions arriving during
/// the backfill are also buffered live; downstream signature de-duplication removes overlap.
async fn backfill_logs_gap(
    rpc: &SolanaRpc,
    program_id: &str,
    checkpoint_signature: &str,
    sender: &broadcast::Sender<Value>,
) -> Result<(usize, bool, Option<(u64, String)>)> {
    let mut signatures = Vec::<(String, u64)>::new();
    let mut before: Option<String> = None;
    let mut reached_checkpoint = false;
    let mut newest: Option<(u64, String)> = None;
    let checkpoint_available = rpc.signature_status_available(checkpoint_signature).await?;

    for _ in 0..MAX_BACKFILL_PAGES {
        let page = rpc
            .get_signatures_for_address(
                program_id,
                before.as_deref(),
                Some(checkpoint_signature),
                SIGNATURE_PAGE_SIZE,
            )
            .await?;
        if page.is_empty() {
            reached_checkpoint = checkpoint_available;
            break;
        }

        if newest.is_none() {
            if let Some(item) = page.first() {
                if let (Some(signature), Some(slot)) = (
                    item.get("signature").and_then(Value::as_str),
                    item.get("slot").and_then(Value::as_u64),
                ) {
                    newest = Some((slot, signature.to_owned()));
                }
            }
        }

        for item in &page {
            let Some(signature) = item.get("signature").and_then(Value::as_str) else {
                continue;
            };
            if signature == checkpoint_signature {
                reached_checkpoint = true;
                break;
            }
            if let Some(slot) = item.get("slot").and_then(Value::as_u64) {
                signatures.push((signature.to_owned(), slot));
            }
        }
        if reached_checkpoint {
            break;
        }

        let Some(oldest_signature) = page
            .last()
            .and_then(|item| item.get("signature"))
            .and_then(Value::as_str)
        else {
            break;
        };
        if page.len() < SIGNATURE_PAGE_SIZE {
            // The until cursor is bounded only when the checkpoint is still queryable.
            reached_checkpoint = checkpoint_available;
            break;
        }
        before = Some(oldest_signature.to_owned());
    }

    signatures.sort_by_key(|(_, slot)| *slot);
    let recovered = stream::iter(signatures.into_iter().map(|(signature, hinted_slot)| {
        let rpc = rpc.clone();
        async move {
            let transaction = rpc.get_transaction(&signature).await;
            (signature, hinted_slot, transaction)
        }
    }))
    .buffer_unordered(BACKFILL_CONCURRENCY)
    .collect::<Vec<_>>()
    .await;

    let mut recovered = recovered;
    recovered.sort_by_key(|(_, hinted_slot, _)| *hinted_slot);
    let mut emitted = 0_usize;
    let mut unavailable = 0_usize;

    for (signature, hinted_slot, transaction) in recovered {
        let transaction = match transaction {
            Ok(Some(value)) => value,
            Ok(None) => {
                unavailable = unavailable.saturating_add(1);
                continue;
            }
            Err(error) => {
                unavailable = unavailable.saturating_add(1);
                let _ = sender.send(json!({"type":"diagnostic","data":{
                    "severity":"warning","component":"solana_reconnect_backfill",
                    "message":error.to_string(),"signature":signature,
                    "received_at":chrono::Utc::now().to_rfc3339()
                }}));
                continue;
            }
        };
        let Some(meta) = transaction.get("meta").filter(|meta| !meta.is_null()) else {
            unavailable = unavailable.saturating_add(1);
            continue;
        };
        let Some(logs) = meta.get("logMessages").and_then(Value::as_array) else {
            unavailable = unavailable.saturating_add(1);
            continue;
        };
        let logs = logs
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let slot = transaction
            .get("slot")
            .and_then(Value::as_u64)
            .unwrap_or(hinted_slot);
        let err = meta.get("err").cloned().unwrap_or(Value::Null);
        let _ = sender.send(backfilled_log_notification(
            program_id, signature, slot, err, logs,
        ));
        emitted = emitted.saturating_add(1);
    }

    if !reached_checkpoint || unavailable > 0 {
        let _ = sender.send(json!({"type":"diagnostic","data":{
            "severity":"warning","component":"solana_reconnect_backfill",
            "message":"Reconnect recovery was partial; some signatures may be unavailable or the checkpoint may be outside the bounded RPC history scan.",
            "program_id":program_id,
            "checkpoint_signature":checkpoint_signature,
            "scanned_signatures":emitted.saturating_add(unavailable),
            "emitted_transactions":emitted,
            "unavailable_transactions":unavailable,
            "page_limit":MAX_BACKFILL_PAGES,
            "retrying_on_next_reconnect":true,
            "received_at":chrono::Utc::now().to_rfc3339()
        }}));
    }

    Ok((emitted, reached_checkpoint && unavailable == 0, newest))
}

/// Forwards raw logsNotification JSON. The mentions filter accepts exactly one pubkey.
pub async fn run_logs_stream(
    ws_urls: Vec<String>,
    program_id: String,
    sender: broadcast::Sender<Value>,
    status: std::sync::Arc<tokio::sync::RwLock<StreamStatus>>,
    rpc: Option<SolanaRpc>,
) {
    let mut reconnect_count = 0_u64;
    let mut next_endpoint = 0usize;
    let mut last_seen_signature: Option<(u64, String)> = None;
    let mut recovery_incomplete = false;
    if ws_urls.is_empty() {
        warn!("No Solana WebSocket endpoints are configured");
        return;
    }
    loop {
        let active_url = ws_urls[next_endpoint % ws_urls.len()].clone();
        if let Err(error) = connect_and_subscribe(
            &active_url,
            &program_id,
            &sender,
            &status,
            rpc.as_ref(),
            &mut last_seen_signature,
            &mut recovery_incomplete,
        )
        .await
        {
            reconnect_count = reconnect_count.saturating_add(1);
            next_endpoint = (next_endpoint + 1) % ws_urls.len();
            let error_message = format!("{error:#}");
            warn!(error = %error_message, reconnect_count, "Solana WebSocket stream ended; reconnecting");
            status.write().await.last_error = Some(error_message.clone());
            let stream_name = if program_id == PUMPSWAP_PROGRAM_ID {
                "pumpswap"
            } else {
                "pump_fun"
            };
            let _ = sender.send(json!({
                "type":"provider_status",
                "data":{
                    "provider":"solana_websocket",
                    "stream_name":stream_name,
                    "connected":false,
                    "reconnect_count":reconnect_count,
                    "program_id":program_id,
                    "reason":error_message,
                    "received_at":chrono::Utc::now().to_rfc3339()
                }
            }));
            let _ = sender.send(json!({
                "type":"diagnostic",
                "data":{
                    "severity":"error",
                    "component":"solana_websocket",
                    "message":error_message,
                    "retrying":true,
                    "reconnect_count":reconnect_count,
                    "program_id":program_id,
                    "received_at":chrono::Utc::now().to_rfc3339()
                }
            }));
        }
        {
            let mut current = status.write().await;
            current.connected = false;
            current.subscription_id = None;
            current.reconnect_count = reconnect_count;
            current.program_id = Some(program_id.clone());
        }
        let delay = 2_u64
            .saturating_mul(1_u64 << reconnect_count.min(4))
            .min(30);
        tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
    }
}

async fn connect_and_subscribe(
    ws_url: &str,
    program_id: &str,
    sender: &broadcast::Sender<Value>,
    status: &std::sync::Arc<tokio::sync::RwLock<StreamStatus>>,
    rpc: Option<&SolanaRpc>,
    last_seen_signature: &mut Option<(u64, String)>,
    recovery_incomplete: &mut bool,
) -> Result<()> {
    let (mut socket, _) = connect_async(ws_url)
        .await
        .context("connecting to Solana WebSocket")?;
    socket
        .send(Message::Text(
            json!({
                "jsonrpc":"2.0","id":1,"method":"logsSubscribe",
                "params":[{"mentions":[program_id]},{"commitment":"confirmed"}]
            })
            .to_string()
            .into(),
        ))
        .await
        .context("sending logsSubscribe")?;

    let acknowledgement = socket
        .next()
        .await
        .ok_or_else(|| anyhow!("WebSocket closed before subscription acknowledgement"))??;
    let ack: Value = serde_json::from_str(acknowledgement.to_text()?)?;
    let subscription_id = ack
        .get("result")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("logsSubscribe rejected: {ack}"))?;
    {
        let mut current = status.write().await;
        current.connected = true;
        current.subscription_id = Some(subscription_id);
        current.last_error = None;
        current.program_id = Some(program_id.to_owned());
        current.last_message_at = Some(chrono::Utc::now().to_rfc3339());
    }
    info!(
        subscription_id,
        program_id, "Subscribed to Solana program logs"
    );
    let stream_name = if program_id == PUMPSWAP_PROGRAM_ID {
        "pumpswap"
    } else {
        "pump_fun"
    };
    let reconnect_count = status.read().await.reconnect_count;
    let _ = sender.send(json!({"type":"provider_status","data":{"provider":"solana_websocket","stream_name":stream_name,"connected":true,"subscription_id":subscription_id,"reconnect_count":reconnect_count,"program_id":program_id,"received_at":chrono::Utc::now().to_rfc3339()}}));

    if let Some((_, checkpoint)) = last_seen_signature.clone() {
        if let Some(rpc) = rpc {
            match backfill_logs_gap(rpc, program_id, &checkpoint, sender).await {
                Ok((recovered, complete, newest)) => {
                    *recovery_incomplete = !complete;
                    if complete {
                        if let Some(newest) = newest {
                            *last_seen_signature = Some(newest);
                        }
                    }
                    let _ = sender.send(json!({
                        "type":"reconnect_recovery_status",
                        "data":{
                            "program_id":program_id,
                            "recovered_transactions":recovered,
                            "complete":complete,
                            "checkpoint_signature":checkpoint,
                            "received_at":chrono::Utc::now().to_rfc3339()
                        }
                    }));
                }
                Err(error) => {
                    *recovery_incomplete = true;
                    let _ = sender.send(json!({"type":"diagnostic","data":{
                        "severity":"error","component":"solana_reconnect_backfill",
                        "message":error.to_string(),"program_id":program_id,
                        "checkpoint_signature":checkpoint,"retrying_on_next_reconnect":true,
                        "received_at":chrono::Utc::now().to_rfc3339()
                    }}));
                }
            }
        } else {
            *recovery_incomplete = true;
            let _ = sender.send(json!({"type":"diagnostic","data":{
                "severity":"warning","component":"solana_reconnect_backfill",
                "message":"Reconnect gap cannot be backfilled because HTTP Solana RPC is not configured.",
                "program_id":program_id,"checkpoint_signature":checkpoint,
                "received_at":chrono::Utc::now().to_rfc3339()
            }}));
        }
    }

    while let Some(message) = socket.next().await {
        match message? {
            Message::Text(text) => {
                let value: Value = serde_json::from_str(&text)?;
                if value.get("method").and_then(Value::as_str) == Some("logsNotification") {
                    let mut tagged = value;
                    if let Some(object) = tagged.as_object_mut() {
                        object.insert(
                            "nodex_program_id".into(),
                            Value::String(program_id.to_owned()),
                        );
                    }
                    if !*recovery_incomplete {
                        let slot = tagged
                            .pointer("/params/result/context/slot")
                            .and_then(Value::as_u64)
                            .unwrap_or_default();
                        if let Some(signature) = tagged
                            .pointer("/params/result/value/signature")
                            .and_then(Value::as_str)
                        {
                            let is_newer = last_seen_signature
                                .as_ref()
                                .is_none_or(|(last_slot, _)| slot >= *last_slot);
                            if is_newer {
                                *last_seen_signature = Some((slot, signature.to_owned()));
                            }
                        }
                    }
                    {
                        let mut current = status.write().await;
                        current.last_message_at = Some(chrono::Utc::now().to_rfc3339());
                    }
                    let _ = sender.send(tagged);
                }
            }
            Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
            Message::Close(_) => break,
            _ => {}
        }
    }
    {
        let mut current = status.write().await;
        current.connected = false;
        current.subscription_id = None;
    }
    let stream_name = if program_id == PUMPSWAP_PROGRAM_ID {
        "pumpswap"
    } else {
        "pump_fun"
    };
    let reconnect_count = status.read().await.reconnect_count;
    let _ = sender.send(json!({"type":"provider_status","data":{"provider":"solana_websocket","stream_name":stream_name,"connected":false,"reconnect_count":reconnect_count,"program_id":program_id,"reason":"WebSocket stream closed","received_at":chrono::Utc::now().to_rfc3339()}}));
    Ok(())
}

#[cfg(test)]
mod token_2022_layout_tests {
    use super::*;

    fn mint_with_extension(kind: u16, value: &[u8]) -> Vec<u8> {
        let mut data = vec![0u8; TOKEN_2022_ACCOUNT_TYPE_OFFSET];
        data.push(TOKEN_2022_MINT_ACCOUNT_TYPE);
        data.extend_from_slice(&kind.to_le_bytes());
        data.extend_from_slice(&(value.len() as u16).to_le_bytes());
        data.extend_from_slice(value);
        data
    }

    #[test]
    fn reads_extensions_from_the_canonical_offset_166() {
        let data = mint_with_extension(9, &[]);
        assert_eq!(
            parse_token_2022_extensions(&data).unwrap(),
            vec!["non_transferable"]
        );
    }

    #[test]
    fn malformed_extension_is_reported_as_unverified_not_a_scan_abort() {
        let mut data = vec![0u8; TOKEN_2022_ACCOUNT_TYPE_OFFSET];
        data.push(TOKEN_2022_MINT_ACCOUNT_TYPE);
        data.extend_from_slice(&9u16.to_le_bytes());
        data.extend_from_slice(&64u16.to_le_bytes());
        data.extend_from_slice(&[1, 2, 3]);

        let report = parse_mint_security_report(
            "11111111111111111111111111111111",
            TOKEN_2022_PROGRAM_ID,
            &data,
            Some(1),
        )
        .expect("malformed extension must not abort the whole security report");

        assert_eq!(report.decision, "UNVERIFIED");
        assert!(report
            .checks
            .iter()
            .any(|check| check.name == "token_2022_extensions" && check.status == "unverified"));
    }

    #[test]
    fn rejects_wrong_account_type_and_nonzero_mint_padding() {
        let mut wrong_type = vec![0u8; TOKEN_2022_TLV_OFFSET];
        wrong_type[TOKEN_2022_ACCOUNT_TYPE_OFFSET] = 2;
        assert!(parse_token_2022_extensions(&wrong_type).is_err());

        let mut bad_padding = vec![0u8; TOKEN_2022_TLV_OFFSET];
        bad_padding[100] = 1;
        bad_padding[TOKEN_2022_ACCOUNT_TYPE_OFFSET] = TOKEN_2022_MINT_ACCOUNT_TYPE;
        assert!(parse_token_2022_extensions(&bad_padding).is_err());
    }
}

#[cfg(test)]
mod pumpswap_log_tests {
    use super::*;

    #[test]
    fn only_accepts_program_data_from_the_active_pumpswap_invocation() {
        let encoded = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            [103, 244, 82, 31, 44, 245, 119, 119]
                .into_iter()
                .chain(1_700_000_000_i64.to_le_bytes())
                .chain((1_u64..=13).flat_map(u64::to_le_bytes))
                .chain([8_u8; 32])
                .chain([9_u8; 32])
                .chain([0_u8; 128])
                .collect::<Vec<_>>(),
        );
        let logs = vec![
            format!("Program {PUMPSWAP_PROGRAM_ID} invoke [1]"),
            "Program SomeOtherProgram invoke [2]".to_owned(),
            format!("Program data: {encoded}"),
            "Program SomeOtherProgram success".to_owned(),
            format!("Program data: {encoded}"),
            format!("Program {PUMPSWAP_PROGRAM_ID} success"),
        ];
        let events = decode_pump_swap_activity_logs(&logs, PUMPSWAP_PROGRAM_ID).unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], PumpSwapActivityEvent::Buy { .. }));
    }

    #[test]
    fn ignores_program_data_without_a_matching_invocation() {
        let logs = vec!["Program data: AQID".to_owned()];
        assert!(decode_pump_swap_activity_logs(&logs, PUMPSWAP_PROGRAM_ID)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn backfilled_notifications_match_the_live_logs_notification_shape() {
        let notification = backfilled_log_notification(
            PUMPSWAP_PROGRAM_ID,
            "test-signature".to_owned(),
            42,
            Value::Null,
            vec!["Program log: test".to_owned()],
        );
        assert_eq!(
            notification.get("nodex_program_id").and_then(Value::as_str),
            Some(PUMPSWAP_PROGRAM_ID)
        );
        assert_eq!(
            notification
                .pointer("/params/result/context/slot")
                .and_then(Value::as_u64),
            Some(42)
        );
        assert_eq!(
            notification
                .pointer("/params/result/value/signature")
                .and_then(Value::as_str),
            Some("test-signature")
        );
        assert_eq!(
            notification
                .pointer("/params/result/value/logs/0")
                .and_then(Value::as_str),
            Some("Program log: test")
        );
        assert_eq!(
            notification
                .get("nodex_backfilled")
                .and_then(Value::as_bool),
            Some(true)
        );
    }
}

#[cfg(test)]
mod transaction_decoder_tests {
    use super::*;

    fn borsh_string(value: &str, out: &mut Vec<u8>) {
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        out.extend_from_slice(value.as_bytes());
    }

    fn create_instruction(discriminator: [u8; 8], creator: [u8; 32]) -> String {
        let mut data = discriminator.to_vec();
        borsh_string("Example Token", &mut data);
        borsh_string("EX", &mut data);
        borsh_string("https://example.invalid/token.json", &mut data);
        data.extend_from_slice(&creator);
        bs58::encode(data).into_string()
    }

    #[test]
    fn decodes_legacy_pump_create_instruction() {
        let creator = [7u8; 32];
        let tx = json!({
            "slot": 123,
            "transaction": {"signatures": ["signature"], "message": {
                "accountKeys": ["Mint111", "PumpProgram"],
                "instructions": [{
                    "programIdIndex": 1, "accounts": [0],
                    "data": create_instruction(PUMP_CREATE_DISCRIMINATOR, creator)
                }]
            }},
            "meta": {"err": null, "loadedAddresses": {"writable": [], "readonly": []}}
        });
        let decoded = decode_pump_fun_create(&tx, "signature", "PumpProgram")
            .expect("decode should succeed")
            .expect("create should be found");
        assert_eq!(decoded.mint_address, "Mint111");
        assert_eq!(decoded.creator_address, bs58::encode(creator).into_string());
        assert_eq!(decoded.slot, 123);
    }

    #[test]
    fn decodes_pump_create_v2_instruction() {
        let creator = [9u8; 32];
        let tx = json!({
            "slot": 456,
            "transaction": {"signatures": ["signature"], "message": {
                "accountKeys": ["Mint222", "PumpProgram"],
                "instructions": [{
                    "programIdIndex": 1, "accounts": [0],
                    "data": create_instruction(PUMP_CREATE_V2_DISCRIMINATOR, creator)
                }]
            }},
            "meta": {"err": null}
        });
        let decoded = decode_pump_fun_create(&tx, "signature", "PumpProgram")
            .expect("decode should succeed")
            .expect("create_v2 should be found");
        assert_eq!(decoded.mint_address, "Mint222");
        assert_eq!(decoded.creator_address, bs58::encode(creator).into_string());
    }

    fn base_mint_data(mint_authority: bool, freeze_authority: bool) -> Vec<u8> {
        let mut data = vec![0u8; 82];
        if mint_authority {
            data[0..4].copy_from_slice(&1u32.to_le_bytes());
            data[4..36].fill(7);
        }
        data[36..44].copy_from_slice(&1_000_000u64.to_le_bytes());
        data[44] = 6;
        data[45] = 1;
        if freeze_authority {
            data[46..50].copy_from_slice(&1u32.to_le_bytes());
            data[50..82].fill(8);
        }
        data
    }

    #[test]
    fn parses_token_2022_embedded_metadata_and_update_authority() {
        let mut data = base_mint_data(false, false);
        data.resize(TOKEN_2022_ACCOUNT_TYPE_OFFSET, 0);
        data.push(TOKEN_2022_MINT_ACCOUNT_TYPE);
        let mut metadata = vec![9u8; 32];
        metadata.extend_from_slice(&[4u8; 32]);
        borsh_string("Metadata Token", &mut metadata);
        borsh_string("META", &mut metadata);
        borsh_string("https://example.invalid/meta.json", &mut metadata);
        data.extend_from_slice(&19u16.to_le_bytes());
        data.extend_from_slice(&(metadata.len() as u16).to_le_bytes());
        data.extend_from_slice(&metadata);
        let report =
            parse_mint_security_report("MintAddress", TOKEN_2022_PROGRAM_ID, &data, Some(7))
                .unwrap();
        assert_eq!(report.metadata.name.as_deref(), Some("Metadata Token"));
        assert_eq!(report.metadata.symbol.as_deref(), Some("META"));
        assert_eq!(report.metadata.mutable, Some(true));
        assert_eq!(report.metadata.status, "partial");
    }

    #[test]
    fn clean_spl_mint_is_unverified_until_remaining_checks_exist() {
        let report = parse_mint_security_report(
            "MintAddress",
            SPL_TOKEN_PROGRAM_ID,
            &base_mint_data(false, false),
            Some(123),
        )
        .expect("valid SPL mint should parse");
        assert_eq!(report.decision, "UNVERIFIED");
        assert_eq!(report.decimals, Some(6));
        assert_eq!(report.supply_raw.as_deref(), Some("1000000"));
        assert!(report
            .checks
            .iter()
            .any(|check| check.name == "sellability" && check.status == "unverified"));
    }

    #[test]
    fn active_authority_remains_unverified_and_requires_rechecks() {
        let report = parse_mint_security_report(
            "MintAddress",
            SPL_TOKEN_PROGRAM_ID,
            &base_mint_data(true, false),
            Some(123),
        )
        .expect("valid SPL mint should parse");
        assert_eq!(report.decision, "UNVERIFIED");
        assert!(report.mint_authority.is_some());
        assert!(report.decision_reason.contains("high-risk warning"));
    }

    #[test]
    fn detects_token_2022_permanent_delegate_extension() {
        let mut data = base_mint_data(false, false);
        data.resize(TOKEN_2022_ACCOUNT_TYPE_OFFSET, 0);
        data.push(TOKEN_2022_MINT_ACCOUNT_TYPE); // Mint account type
        data.extend_from_slice(&12u16.to_le_bytes()); // PermanentDelegate
        data.extend_from_slice(&32u16.to_le_bytes());
        data.extend_from_slice(&[9u8; 32]);
        let report =
            parse_mint_security_report("MintAddress", TOKEN_2022_PROGRAM_ID, &data, Some(456))
                .expect("Token-2022 mint should parse");
        assert!(report
            .extensions
            .iter()
            .any(|name| name == "permanent_delegate"));
        assert!(report.checks.iter().any(
            |check| check.name == "token_2022_permanent_delegate" && check.status == "warning"
        ));
        assert_eq!(report.decision, "UNVERIFIED");
    }

    #[test]
    fn ignores_failed_transactions() {
        let tx = json!({"meta":{"err":{"InstructionError":[0,"Custom"]}}});
        assert!(decode_pump_fun_create(&tx, "signature", "PumpProgram")
            .expect("failed transaction is not a decoder error")
            .is_none());
    }
}

#[cfg(test)]
mod pump_fun_trade_event_tests {
    use super::*;

    #[test]
    fn decodes_program_scoped_pump_fun_trade_event_prefix() {
        let mint = [3u8; 32];
        let trader = [4u8; 32];
        let mut bytes = PUMP_FUN_TRADE_EVENT_DISCRIMINATOR.to_vec();
        bytes.extend_from_slice(&mint);
        bytes.extend_from_slice(&250_000_000_u64.to_le_bytes());
        bytes.extend_from_slice(&1_500_000_u64.to_le_bytes());
        bytes.push(1);
        bytes.extend_from_slice(&trader);
        bytes.extend_from_slice(&1_700_000_000_i64.to_le_bytes());
        bytes.extend_from_slice(&[0u8; 24]); // appended IDL fields
        let encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bytes);
        let logs = vec![
            "Program PumpProgram invoke [1]".to_owned(),
            format!("Program data: {encoded}"),
            "Program PumpProgram success".to_owned(),
        ];
        let trades = decode_pump_fun_trade_logs(&logs, "PumpProgram").unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].mint_address, bs58::encode(mint).into_string());
        assert_eq!(trades[0].trader_address, bs58::encode(trader).into_string());
        assert_eq!(trades[0].sol_amount_lamports, 250_000_000);
        assert_eq!(trades[0].token_amount_raw, 1_500_000);
        assert!(trades[0].is_buy);
        assert_eq!(trades[0].timestamp, 1_700_000_000);
    }

    #[test]
    fn ignores_trade_event_emitted_by_nested_unrelated_program() {
        let mut bytes = PUMP_FUN_TRADE_EVENT_DISCRIMINATOR.to_vec();
        bytes.extend_from_slice(&[3u8; 32]);
        bytes.extend_from_slice(&1_u64.to_le_bytes());
        bytes.extend_from_slice(&2_u64.to_le_bytes());
        bytes.push(1);
        bytes.extend_from_slice(&[4u8; 32]);
        bytes.extend_from_slice(&1_i64.to_le_bytes());
        let encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bytes);
        let logs = vec![
            "Program PumpProgram invoke [1]".to_owned(),
            "Program OtherProgram invoke [2]".to_owned(),
            format!("Program data: {encoded}"),
            "Program OtherProgram success".to_owned(),
            "Program PumpProgram success".to_owned(),
        ];
        assert!(decode_pump_fun_trade_logs(&logs, "PumpProgram")
            .unwrap()
            .is_empty());
    }
}
