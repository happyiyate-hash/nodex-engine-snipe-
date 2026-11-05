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
use std::collections::HashMap;
use tokio::sync::broadcast;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

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
    http_url: String,
    client: reqwest::Client,
}

impl SolanaRpc {
    pub fn new(http_url: impl Into<String>) -> Self {
        Self {
            http_url: http_url.into(),
            client: reqwest::Client::new(),
        }
    }

    async fn call<T: for<'de> Deserialize<'de>>(&self, method: &str, params: Value) -> Result<T> {
        let response = self
            .client
            .post(&self.http_url)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
            .send()
            .await
            .context("sending Solana JSON-RPC request")?
            .error_for_status()
            .context("Solana RPC returned HTTP error")?
            .json::<RpcResponse<T>>()
            .await
            .context("decoding Solana JSON-RPC response")?;
        if let Some(error) = response.error {
            return Err(anyhow!("Solana RPC {method} error: {}", error.message));
        }
        response
            .result
            .ok_or_else(|| anyhow!("Solana RPC {method} returned no result"))
    }

    /// Fetch one confirmed account's owner and raw bytes for verified program-account decoding.
    pub async fn get_account_data(
        &self,
        address: &str,
    ) -> Result<Option<(String, Vec<u8>, Option<u64>)>> {
        let account: Value = self
            .call(
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
        Ok(Some((owner, bytes, slot)))
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

    /// Inspect mint authorities and Token-2022 extensions using confirmed account data.
    /// This is the first scanner stage; it deliberately cannot approve a token by itself.
    pub async fn scan_token_security(&self, mint: &str) -> Result<TokenSecurityReport> {
        self.scan_token_security_with_progress(mint, |_, _, _| {}).await
    }

    /// Runs the security scan while reporting genuine stage transitions to the caller.
    pub async fn scan_token_security_with_progress<F>(
        &self,
        mint: &str,
        mut on_progress: F,
    ) -> Result<TokenSecurityReport>
    where
        F: FnMut(&str, u8, &str),
    {
        on_progress("mint_account", 8, "Checking mint account");
        let account: Value = self
            .call(
                "getAccountInfo",
                json!([mint, {"encoding":"base64","commitment":"confirmed"}]),
            )
            .await?;
        let context_slot = account.pointer("/context/slot").and_then(Value::as_u64);
        let value = account.get("value").filter(|v| !v.is_null());
        let Some(value) = value else {
            return Ok(TokenSecurityReport::unverified(
                mint,
                "Mint account was not found by RPC",
                context_slot,
            ));
        };
        let owner = value
            .get("owner")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let encoded = value.pointer("/data/0").and_then(Value::as_str);
        let Some(encoded) = encoded else {
            return Ok(TokenSecurityReport::unverified(
                mint,
                "Mint account data was not returned in base64 form",
                context_slot,
            ));
        };
        let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded)
            .context("decoding mint account base64 data")?;
        let mut report = parse_mint_security_report(mint, &owner, &bytes, context_slot)?;
        on_progress("authorities", 32, "Checking mint and freeze authorities");
        on_progress("holder_distribution", 52, "Checking holder distribution");
        match self
            .get_holder_distribution(mint, report.supply_raw.as_deref().unwrap_or("0"))
            .await
        {
            Ok(Some(distribution)) => {
                report.holder_distribution = Some(distribution.clone());
                if let Some(check) = report
                    .checks
                    .iter_mut()
                    .find(|check| check.name == "holder_distribution")
                {
                    check.status = "partial".into();
                    check.severity = "medium".into();
                    check.detail = format!(
                        "Sampled {} largest token accounts and resolved {} owners. The top 10 observed owners represent {:.2}% of supply. Pool vaults and linked wallets have not been classified or excluded.",
                        distribution.sampled_token_accounts,
                        distribution.resolved_accounts,
                        distribution.top_10_observed_owner_share_percent
                    );
                }
            }
            Ok(None) => {}
            Err(error) => {
                if let Some(check) = report
                    .checks
                    .iter_mut()
                    .find(|check| check.name == "holder_distribution")
                {
                    check.detail = format!(
                        "Holder distribution is UNVERIFIED because RPC analysis failed: {error}"
                    );
                }
            }
        }
        on_progress("rugcheck", 88, "Reviewing external risk signals");
        report.rugcheck = self.get_rugcheck_summary(mint).await;
        if report.rugcheck.status == "available" {
            let detail = format!(
                "RugCheck summary received: normalized score {:?}, liquidity locked {:?}%, {} reported risks. This provider score is advisory and does not independently approve a token.",
                report.rugcheck.score_normalised,
                report.rugcheck.lp_locked_pct,
                report.rugcheck.risks.len()
            );
            report.checks.push(SecurityCheck {
                name: "rugcheck_summary".into(),
                status: "info".into(),
                severity: "info".into(),
                detail,
            });
        } else {
            report.checks.push(SecurityCheck {
                name: "rugcheck_summary".into(),
                status: "unverified".into(),
                severity: "warning".into(),
                detail: report.rugcheck.error.clone().unwrap_or_else(|| {
                    "RugCheck did not return a usable report. This is not evidence that the token is safe.".into()
                }),
            });
        }
        on_progress("complete", 100, "Security scan completed");
        Ok(report)
    }

    async fn get_rugcheck_summary(&self, mint: &str) -> RugCheckSummary {
        let url = format!("https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary");
        let mut request = self
            .client
            .get(url)
            .header(reqwest::header::ACCEPT, "application/json")
            .timeout(std::time::Duration::from_secs(8));
        if let Ok(api_key) = std::env::var("RUGCHECK_API_KEY") {
            if !api_key.trim().is_empty() {
                request = request.header("X-API-KEY", api_key);
            }
        }
        let response = match request.send().await {
            Ok(response) => response,
            Err(error) => {
                return RugCheckSummary {
                    provider: "rugcheck".into(),
                    status: "unavailable".into(),
                    error: Some(format!("RugCheck request failed: {error}")),
                    ..Default::default()
                };
            }
        };
        let status_code = response.status();
        if !status_code.is_success() {
            return RugCheckSummary {
                provider: "rugcheck".into(),
                status: "unavailable".into(),
                error: Some(format!("RugCheck returned HTTP {status_code}")),
                ..Default::default()
            };
        }
        let value = match response.json::<Value>().await {
            Ok(value) => value,
            Err(error) => {
                return RugCheckSummary {
                    provider: "rugcheck".into(),
                    status: "unavailable".into(),
                    error: Some(format!("Could not decode RugCheck response: {error}")),
                    ..Default::default()
                };
            }
        };
        RugCheckSummary {
            provider: "rugcheck".into(),
            status: "available".into(),
            score: value.get("score").and_then(Value::as_u64),
            score_normalised: value.get("score_normalised").and_then(Value::as_u64),
            lp_locked_pct: value.get("lpLockedPct").and_then(Value::as_f64),
            risks: value
                .get("risks")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
            error: None,
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

    async fn get_holder_distribution(
        &self,
        mint: &str,
        supply_raw: &str,
    ) -> Result<Option<HolderDistribution>> {
        let largest: Value = self
            .call(
                "getTokenLargestAccounts",
                json!([mint, {"commitment":"confirmed"}]),
            )
            .await?;
        let entries = largest
            .pointer("/value")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("getTokenLargestAccounts returned no value array"))?;
        if entries.is_empty() {
            return Ok(Some(HolderDistribution::default()));
        }
        let addresses: Vec<String> = entries
            .iter()
            .filter_map(|entry| {
                entry
                    .get("address")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .collect();
        let account_data: Value = self
            .call(
                "getMultipleAccounts",
                json!([addresses, {"encoding":"base64","commitment":"confirmed"}]),
            )
            .await?;
        let values = account_data
            .pointer("/value")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("getMultipleAccounts returned no value array"))?;
        let expected_mint = bs58::decode(mint)
            .into_vec()
            .context("decoding mint address for holder account validation")?;
        let supply = supply_raw
            .parse::<u128>()
            .context("parsing raw mint supply for holder distribution")?;
        let mut owners: HashMap<String, u128> = HashMap::new();
        let mut top_account_total = 0u128;
        let mut resolved_accounts = 0usize;
        for value in values {
            let Some(value) = value.as_object() else {
                continue;
            };
            let Some(encoded) = value
                .get("data")
                .and_then(|data| data.get(0))
                .and_then(Value::as_str)
            else {
                continue;
            };
            let data = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded)
                .context("decoding token account data")?;
            if data.len() < 72 || data.get(0..32) != Some(expected_mint.as_slice()) {
                continue;
            }
            let owner = bs58::encode(&data[32..64]).into_string();
            let amount =
                u64::from_le_bytes(data[64..72].try_into().expect("eight-byte token amount"))
                    as u128;
            top_account_total = top_account_total.saturating_add(amount);
            *owners.entry(owner).or_default() += amount;
            resolved_accounts += 1;
        }
        let mut owner_amounts: Vec<u128> = owners.into_values().collect();
        owner_amounts.sort_unstable_by(|a, b| b.cmp(a));
        let top_10_owner_total = owner_amounts.iter().take(10).copied().sum::<u128>();
        let percent = |amount: u128| -> f64 {
            if supply == 0 {
                0.0
            } else {
                (amount as f64 / supply as f64) * 100.0
            }
        };
        Ok(Some(HolderDistribution {
            sampled_token_accounts: entries.len(),
            resolved_accounts,
            top_20_account_share_percent: percent(top_account_total),
            top_10_observed_owner_share_percent: percent(top_10_owner_total),
            sample_is_partial: true,
            caveat: "Only the largest RPC-returned token accounts are sampled. Pool vaults, burn addresses, exchange wallets and linked-wallet clusters are not yet classified or excluded.".into(),
        }))
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
        let response = self.client
            .post(&self.http_url)
            .json(&json!({
                "jsonrpc":"2.0", "id":1, "method":"getTransaction",
                "params":[signature, {"encoding":"json","commitment":"confirmed","maxSupportedTransactionVersion":1}]
            }))
            .send().await.context("sending Solana getTransaction request")?
            .error_for_status().context("Solana getTransaction returned HTTP error")?
            .json::<RpcResponse<Value>>().await.context("decoding getTransaction response")?;
        if let Some(error) = response.error {
            return Err(anyhow!(
                "Solana RPC getTransaction error: {}",
                error.message
            ));
        }
        Ok(response.result.filter(|value| !value.is_null()))
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
    pub metadata: TokenMetadataReport,
    pub checks: Vec<SecurityCheck>,
    pub decision: String,
    pub decision_reason: String,
    pub rugcheck: RugCheckSummary,
}

impl TokenSecurityReport {
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
            decision_reason: "The mint account could not be verified; automatic buying is blocked."
                .into(),
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

fn parse_token_2022_extensions(data: &[u8]) -> Result<Vec<String>> {
    if data.len() <= 82 {
        return Ok(vec![]);
    }
    // Token-2022 extended mint accounts contain an account-type byte after the
    // 82-byte base Mint layout, followed by little-endian type/length TLV records.
    if data.len() < 83 {
        return Err(anyhow!("Token-2022 mint extension header is truncated"));
    }
    let mut offset = 83usize;
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
    if data.len() <= 82 {
        return Ok(report);
    }
    let mut offset = 83usize;
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
    let extensions = if token_program == "token_2022" {
        parse_token_2022_extensions(data)?
    } else {
        vec![]
    };
    let metadata = if token_program == "token_2022" {
        parse_token_2022_metadata(data)?
    } else {
        TokenMetadataReport {
            source: "metaplex_external_not_yet_supported".into(),
            status: "unverified".into(),
            caveat: "Standard SPL metadata PDA and off-chain metadata are not yet fetched; mutability cannot be determined.".into(),
            ..Default::default()
        }
    };
    let mut checks = Vec::new();
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
    let hard_blocker = mint_authority.is_some() || freeze_authority.is_some();
    let decision = if hard_blocker {
        "REJECTED"
    } else {
        "UNVERIFIED"
    };
    let decision_reason = if hard_blocker {
        "A mint or freeze authority is still active. This token fails the current mandatory authority policy.".into()
    } else {
        "Authority checks passed, but holder, creator, metadata, liquidity and sellability checks remain incomplete. Automatic buying is blocked.".into()
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
        metadata,
        checks,
        decision: decision.into(),
        decision_reason,
        rugcheck: RugCheckSummary {
            provider: "rugcheck".into(),
            status: "not_run".into(),
            ..Default::default()
        },
    })
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
    error: Option<RpcError>,
}

#[derive(Debug, Deserialize)]
struct RpcError {
    message: String,
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
    ws_url: String,
    program_id: String,
    sender: broadcast::Sender<Value>,
    status: std::sync::Arc<tokio::sync::RwLock<StreamStatus>>,
    rpc: Option<SolanaRpc>,
) {
    let mut reconnect_count = 0_u64;
    let mut last_seen_signature: Option<(u64, String)> = None;
    let mut recovery_incomplete = false;
    loop {
        if let Err(error) = connect_and_subscribe(
            &ws_url,
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
            let error_message = format!("{error:#}");
            warn!(error = %error_message, reconnect_count, "Solana WebSocket stream ended; reconnecting");
            status.write().await.last_error = Some(error_message.clone());
            let stream_name = if program_id == PUMPSWAP_PROGRAM_ID { "pumpswap" } else { "pump_fun" };
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
    let stream_name = if program_id == PUMPSWAP_PROGRAM_ID { "pumpswap" } else { "pump_fun" };
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
    let stream_name = if program_id == PUMPSWAP_PROGRAM_ID { "pumpswap" } else { "pump_fun" };
    let reconnect_count = status.read().await.reconnect_count;
    let _ = sender.send(json!({"type":"provider_status","data":{"provider":"solana_websocket","stream_name":stream_name,"connected":false,"reconnect_count":reconnect_count,"program_id":program_id,"reason":"WebSocket stream closed","received_at":chrono::Utc::now().to_rfc3339()}}));
    Ok(())
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
        data.push(1);
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
    fn active_authority_is_a_mandatory_policy_blocker() {
        let report = parse_mint_security_report(
            "MintAddress",
            SPL_TOKEN_PROGRAM_ID,
            &base_mint_data(true, false),
            Some(123),
        )
        .expect("valid SPL mint should parse");
        assert_eq!(report.decision, "REJECTED");
        assert!(report.mint_authority.is_some());
    }

    #[test]
    fn detects_token_2022_permanent_delegate_extension() {
        let mut data = base_mint_data(false, false);
        data.push(1); // Mint account type
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
