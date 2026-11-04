use anyhow::{anyhow, Context, Result};
use base64::Engine as _;
use futures_util::{Sink, SinkExt, StreamExt};
use serde_json::{json, Value};
use solana_pubkey::Pubkey;
use std::{collections::HashMap, str::FromStr, time::Duration};
use tokio::sync::{broadcast, mpsc};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

const MAX_MARKET_SUBSCRIPTIONS: usize = 250;
const MARKET_COMMAND_CAPACITY: usize = 512;
const SPL_TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022_PROGRAM_ID: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

#[derive(Debug, Clone)]
struct MarketSubscription {
    mint: String,
    account: String,
    venue: &'static str,
    program_id: String,
    expected_mint: Option<String>,
}

enum MarketCommand {
    Subscribe(MarketSubscription),
}

#[derive(Clone)]
pub struct MarketMonitor {
    commands: mpsc::Sender<MarketCommand>,
}

impl MarketMonitor {
    /// Queue a Pump.fun bonding-curve account for monitoring and return its PDA.
    /// This subscribes to public account data only; it never signs or submits trades.
    pub async fn subscribe_pump_fun_bonding_curve(
        &self,
        mint: &str,
        program_id: &str,
    ) -> Result<String> {
        let account = derive_pump_fun_bonding_curve(mint, program_id)?;
        self.commands
            .send(MarketCommand::Subscribe(MarketSubscription {
                mint: mint.to_owned(),
                account: account.clone(),
                venue: "pump_fun_bonding_curve",
                program_id: program_id.to_owned(),
                expected_mint: None,
            }))
            .await
            .map_err(|_| anyhow!("market monitor task is not running"))?;
        Ok(account)
    }
}

/// Derive the documented Pump.fun bonding-curve PDA using seeds [bonding-curve, mint].
pub fn derive_pump_fun_bonding_curve(mint: &str, program_id: &str) -> Result<String> {
    let mint = Pubkey::from_str(mint).context("invalid token mint public key")?;
    let program_id = Pubkey::from_str(program_id).context("invalid Pump.fun program id")?;
    let (address, _) =
        Pubkey::try_find_program_address(&[b"bonding-curve", mint.as_ref()], &program_id)
            .ok_or_else(|| anyhow!("could not derive Pump.fun bonding-curve PDA"))?;
    Ok(address.to_string())
}

/// Start one multiplexed account-subscription connection with automatic reconnect.
/// Account updates are emitted independently as market_account_update events.
/// Raw account bytes are preserved; reserve decoding is a separate layout-verified stage.
pub fn start_market_monitor(ws_url: String, events: broadcast::Sender<Value>) -> MarketMonitor {
    let (commands, receiver) = mpsc::channel(MARKET_COMMAND_CAPACITY);
    tokio::spawn(run_market_monitor(ws_url, events, receiver));
    MarketMonitor { commands }
}

async fn run_market_monitor(
    ws_url: String,
    events: broadcast::Sender<Value>,
    mut commands: mpsc::Receiver<MarketCommand>,
) {
    let mut desired = HashMap::<String, MarketSubscription>::new();
    let mut reconnect_count = 0_u64;
    loop {
        match connect_market_stream(&ws_url, &events, &mut commands, &mut desired).await {
            Ok(()) => {
                reconnect_count = reconnect_count.saturating_add(1);
                let _ = events.send(json!({
                    "type":"market_monitor_status",
                    "data":{"connected":false,"reason":"websocket_closed","reconnect_count":reconnect_count}
                }));
            }
            Err(error) => {
                reconnect_count = reconnect_count.saturating_add(1);
                warn!(%error, reconnect_count, "Market account stream disconnected; reconnecting");
                let _ = events.send(json!({
                    "type":"diagnostic",
                    "data":{
                        "severity":"warning",
                        "component":"market_monitor",
                        "message":error.to_string(),
                        "retrying":true,
                        "reconnect_count":reconnect_count,
                        "received_at":chrono::Utc::now().to_rfc3339()
                    }
                }));
            }
        }
        let delay = 2_u64
            .saturating_mul(1_u64 << reconnect_count.min(4))
            .min(30);
        tokio::time::sleep(Duration::from_secs(delay)).await;
    }
}

async fn send_account_subscribe<S>(sink: &mut S, request_id: u64, account: &str) -> Result<()>
where
    S: Sink<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    let request = json!({
        "jsonrpc":"2.0",
        "id":request_id,
        "method":"accountSubscribe",
        "params":[account,{"encoding":"base64","commitment":"confirmed"}]
    });
    sink.send(Message::Text(request.to_string().into()))
        .await
        .context("sending Solana accountSubscribe request")
}

#[derive(Debug, Clone, PartialEq)]
pub struct PumpBondingCurveState {
    pub virtual_token_reserves: u64,
    pub virtual_quote_reserves: u64,
    pub real_token_reserves: u64,
    pub real_quote_reserves: u64,
    pub token_total_supply: u64,
    pub complete: bool,
    pub creator: String,
    pub price_sol: f64,
    pub market_cap_sol: f64,
}

/// Decode the documented Anchor BondingCurve account layout.
/// The account discriminator and required fields are checked; unknown trailing
/// fields are tolerated because Pump.fun appends fields over time.
pub fn decode_pump_bonding_curve_account(data: &[u8]) -> Result<PumpBondingCurveState> {
    const DISCRIMINATOR: [u8; 8] = [23, 183, 248, 55, 96, 216, 172, 96];
    const MINIMUM_ACCOUNT_LENGTH: usize = 81;
    if data.len() < MINIMUM_ACCOUNT_LENGTH {
        return Err(anyhow!(
            "Pump.fun bonding-curve account is too short: {} bytes",
            data.len()
        ));
    }
    if data[..8] != DISCRIMINATOR {
        return Err(anyhow!(
            "account discriminator is not Pump.fun BondingCurve"
        ));
    }
    let read_u64 = |offset: usize| -> Result<u64> {
        let bytes: [u8; 8] = data
            .get(offset..offset + 8)
            .ok_or_else(|| anyhow!("bonding-curve field is truncated"))?
            .try_into()
            .map_err(|_| anyhow!("invalid bonding-curve integer field"))?;
        Ok(u64::from_le_bytes(bytes))
    };
    let complete = match data[48] {
        0 => false,
        1 => true,
        value => return Err(anyhow!("invalid Pump.fun complete flag: {value}")),
    };
    let virtual_token_reserves = read_u64(8)?;
    let virtual_quote_reserves = read_u64(16)?;
    let real_token_reserves = read_u64(24)?;
    let real_quote_reserves = read_u64(32)?;
    let token_total_supply = read_u64(40)?;
    if virtual_token_reserves == 0 || token_total_supply == 0 {
        return Err(anyhow!("bonding-curve reserves or token supply are zero"));
    }

    // The current IDL calls this virtual_quote_reserves, not virtual_sol_reserves.
    // Legacy accounts (81-83 bytes) predate quote_mint and are SOL-paired.
    // Newer accounts store quote_mint after the two appended boolean flags.
    if data.len() > 83 && data.len() < 115 {
        return Err(anyhow!(
            "Pump.fun bonding-curve account has a truncated quote_mint field"
        ));
    }
    if data.len() >= 115 && data[83..115].iter().any(|byte| *byte != 0) {
        return Err(anyhow!(
            "non-SOL Pump.fun quote pairs are not yet supported by the SOL price monitor"
        ));
    }

    // Pump.fun tokens use 6 decimals; SOL quote reserves are denominated in lamports.
    let price_sol = (virtual_quote_reserves as f64 / 1_000_000_000.0)
        / (virtual_token_reserves as f64 / 1_000_000.0);
    if !price_sol.is_finite() || price_sol <= 0.0 {
        return Err(anyhow!("bonding-curve price is not finite and positive"));
    }
    let market_cap_sol = price_sol * (token_total_supply as f64 / 1_000_000.0);
    let creator = bs58::encode(&data[49..81]).into_string();

    Ok(PumpBondingCurveState {
        virtual_token_reserves,
        virtual_quote_reserves,
        real_token_reserves,
        real_quote_reserves,
        token_total_supply,
        complete,
        creator,
        price_sol,
        market_cap_sol,
    })
}

pub const PUMP_SWAP_PROGRAM_ID: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
pub const WRAPPED_SOL_MINT: &str = "So11111111111111111111111111111111111111112";

/// Derive the canonical PumpSwap migration pool using Pump's pool-authority PDA.
pub fn derive_pump_swap_pool(mint: &str, pump_program_id: &str) -> Result<String> {
    let mint = Pubkey::from_str(mint).context("invalid token mint public key")?;
    let pump_program = Pubkey::from_str(pump_program_id).context("invalid Pump.fun program id")?;
    let amm_program = Pubkey::from_str(PUMP_SWAP_PROGRAM_ID).expect("valid PumpSwap program id");
    let wrapped_sol = Pubkey::from_str(WRAPPED_SOL_MINT).expect("valid wrapped SOL mint");
    let (pool_authority, _) =
        Pubkey::try_find_program_address(&[b"pool-authority", mint.as_ref()], &pump_program)
            .ok_or_else(|| anyhow!("could not derive Pump.fun pool-authority PDA"))?;
    let canonical_index = 0_u16.to_le_bytes();
    let (pool, _) = Pubkey::try_find_program_address(
        &[
            b"pool",
            &canonical_index,
            pool_authority.as_ref(),
            mint.as_ref(),
            wrapped_sol.as_ref(),
        ],
        &amm_program,
    )
    .ok_or_else(|| anyhow!("could not derive canonical PumpSwap pool PDA"))?;
    Ok(pool.to_string())
}

#[derive(Debug, Clone, PartialEq)]
pub struct PumpSwapPoolAccount {
    pub index: u16,
    pub creator: String,
    pub base_mint: String,
    pub quote_mint: String,
    pub pool_base_token_account: String,
    pub pool_quote_token_account: String,
    pub lp_supply: u64,
    pub virtual_quote_reserves: i128,
}

/// Decode the current official PumpSwap Pool layout; absent appended fields default to zero.
pub fn decode_pump_swap_pool_account(data: &[u8]) -> Result<PumpSwapPoolAccount> {
    const DISCRIMINATOR: [u8; 8] = [241, 154, 109, 4, 17, 177, 109, 188];
    const MINIMUM_LENGTH: usize = 211;
    if data.len() < MINIMUM_LENGTH {
        return Err(anyhow!(
            "PumpSwap Pool account is too short: {} bytes",
            data.len()
        ));
    }
    if data[..8] != DISCRIMINATOR {
        return Err(anyhow!("account discriminator is not PumpSwap Pool"));
    }
    let read_u64 = |offset: usize| -> Result<u64> {
        let bytes: [u8; 8] = data
            .get(offset..offset + 8)
            .ok_or_else(|| anyhow!("PumpSwap Pool integer field is truncated"))?
            .try_into()
            .map_err(|_| anyhow!("invalid PumpSwap Pool integer field"))?;
        Ok(u64::from_le_bytes(bytes))
    };
    let read_key = |offset: usize| -> Result<String> {
        let bytes = data
            .get(offset..offset + 32)
            .ok_or_else(|| anyhow!("PumpSwap Pool public-key field is truncated"))?;
        Ok(bs58::encode(bytes).into_string())
    };
    let index = u16::from_le_bytes(data[9..11].try_into().expect("two-byte index"));
    let virtual_quote_reserves = if data.len() >= 261 {
        i128::from_le_bytes(
            data[245..261]
                .try_into()
                .expect("sixteen-byte signed reserve"),
        )
    } else {
        0
    };
    Ok(PumpSwapPoolAccount {
        index,
        creator: read_key(11)?,
        base_mint: read_key(43)?,
        quote_mint: read_key(75)?,
        pool_base_token_account: read_key(139)?,
        pool_quote_token_account: read_key(171)?,
        lp_supply: read_u64(203)?,
        virtual_quote_reserves,
    })
}

#[derive(Debug, Clone, Default)]
struct PumpSwapLiveState {
    virtual_quote_reserves: i128,
    base_decimals: Option<u8>,
    base_total_supply: Option<u64>,
    base_reserves: Option<u64>,
    quote_reserves: Option<u64>,
    pool_slot: Option<u64>,
    latest_slot: Option<u64>,
}

fn is_supported_token_program(owner: &str) -> bool {
    owner == SPL_TOKEN_PROGRAM_ID || owner == TOKEN_2022_PROGRAM_ID
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SplMintState {
    pub supply: u64,
    pub decimals: u8,
}

/// Decode the stable SPL Mint base layout. Token-2022 extension bytes may follow it.
pub fn decode_spl_mint_account(data: &[u8]) -> Result<SplMintState> {
    if data.len() < 82 {
        return Err(anyhow!(
            "SPL mint account is too short: {} bytes",
            data.len()
        ));
    }
    if data[45] != 1 {
        return Err(anyhow!("SPL mint is not initialized"));
    }
    let supply = u64::from_le_bytes(data[36..44].try_into().expect("eight-byte mint supply"));
    Ok(SplMintState {
        supply,
        decimals: data[44],
    })
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SplTokenAccountState {
    pub amount: u64,
}

/// Decode the common SPL Token / Token-2022 account base layout.
pub fn decode_spl_token_account(data: &[u8]) -> Result<SplTokenAccountState> {
    if data.len() < 165 {
        return Err(anyhow!(
            "SPL token account is too short: {} bytes",
            data.len()
        ));
    }
    if data[108] != 1 && data[108] != 2 {
        return Err(anyhow!("SPL token account is not initialized"));
    }
    let amount = u64::from_le_bytes(data[64..72].try_into().expect("eight-byte token amount"));
    Ok(SplTokenAccountState { amount })
}

fn pump_swap_price(state: &PumpSwapLiveState) -> Result<(f64, f64, u64)> {
    let base_reserves = state
        .base_reserves
        .ok_or_else(|| anyhow!("base vault has not reported yet"))?;
    let quote_reserves = state
        .quote_reserves
        .ok_or_else(|| anyhow!("quote vault has not reported yet"))?;
    let decimals = state
        .base_decimals
        .ok_or_else(|| anyhow!("base mint decimals have not reported yet"))?;
    let supply = state
        .base_total_supply
        .ok_or_else(|| anyhow!("base mint supply has not reported yet"))?;
    if base_reserves == 0 {
        return Err(anyhow!("PumpSwap base vault reserve is zero"));
    }
    let effective_quote = i128::from(quote_reserves)
        .checked_add(state.virtual_quote_reserves)
        .ok_or_else(|| anyhow!("effective quote reserve overflow"))?;
    let effective_quote = u64::try_from(effective_quote)
        .map_err(|_| anyhow!("effective quote reserves are negative or exceed u64"))?;
    if effective_quote == 0 {
        return Err(anyhow!("PumpSwap effective quote reserve is zero"));
    }
    let base_scale = 10_f64.powi(i32::from(decimals));
    let price_sol =
        (effective_quote as f64 / 1_000_000_000.0) / (base_reserves as f64 / base_scale);
    let market_cap_sol = price_sol * (supply as f64 / base_scale);
    if !price_sol.is_finite() || price_sol <= 0.0 || !market_cap_sol.is_finite() {
        return Err(anyhow!(
            "PumpSwap price calculation was not finite and positive"
        ));
    }
    Ok((price_sol, market_cap_sol, effective_quote))
}

#[derive(Debug, Clone, Copy)]
struct PriceSample {
    timestamp_ms: i64,
    price_sol: f64,
}

fn percentage_change(
    samples: &std::collections::VecDeque<PriceSample>,
    now_ms: i64,
    seconds: i64,
    current: f64,
) -> Option<f64> {
    let target = now_ms.saturating_sub(seconds.saturating_mul(1_000));
    let baseline = samples
        .iter()
        .rev()
        .find(|sample| sample.timestamp_ms <= target)?;
    if baseline.price_sol <= 0.0 {
        return None;
    }
    Some(((current / baseline.price_sol) - 1.0) * 100.0)
}

async fn connect_market_stream(
    ws_url: &str,
    events: &broadcast::Sender<Value>,
    commands: &mut mpsc::Receiver<MarketCommand>,
    desired: &mut HashMap<String, MarketSubscription>,
) -> Result<()> {
    let (socket, _) = connect_async(ws_url)
        .await
        .context("connecting market monitor WebSocket")?;
    let (mut sink, mut stream) = socket.split();
    let mut next_request_id = 100_u64;
    let mut pending = HashMap::<u64, MarketSubscription>::new();
    let mut active = HashMap::<u64, MarketSubscription>::new();
    let mut price_history = HashMap::<String, std::collections::VecDeque<PriceSample>>::new();
    let mut pumpswap_states = HashMap::<String, PumpSwapLiveState>::new();

    for subscription in desired.values() {
        if pending.len() >= MAX_MARKET_SUBSCRIPTIONS {
            break;
        }
        let request_id = next_request_id;
        next_request_id = next_request_id.saturating_add(1);
        send_account_subscribe(&mut sink, request_id, &subscription.account).await?;
        pending.insert(request_id, subscription.clone());
    }

    let _ = events.send(json!({
        "type":"market_monitor_status",
        "data":{
            "connected":true,
            "desired_accounts":desired.len(),
            "pending_subscriptions":pending.len(),
            "active_subscriptions":0,
            "provider":"solana_account_subscribe",
            "received_at":chrono::Utc::now().to_rfc3339()
        }
    }));
    info!(
        desired_accounts = desired.len(),
        "Market account monitor connected"
    );

    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(MarketCommand::Subscribe(subscription)) = command else {
                    return Err(anyhow!("market monitor command channel closed"));
                };
                if desired.contains_key(&subscription.account) {
                    continue;
                }
                if desired.len() >= MAX_MARKET_SUBSCRIPTIONS {
                    let _ = events.send(json!({
                        "type":"diagnostic",
                        "data":{
                            "severity":"warning",
                            "component":"market_monitor",
                            "message":"Market account subscription capacity reached; new account was not subscribed.",
                            "mint_address":subscription.mint,
                            "limit":MAX_MARKET_SUBSCRIPTIONS,
                            "received_at":chrono::Utc::now().to_rfc3339()
                        }
                    }));
                    continue;
                }
                desired.insert(subscription.account.clone(), subscription.clone());
                let request_id = next_request_id;
                next_request_id = next_request_id.saturating_add(1);
                send_account_subscribe(&mut sink, request_id, &subscription.account).await?;
                pending.insert(request_id, subscription);
            }
            message = stream.next() => {
                let Some(message) = message else {
                    return Ok(());
                };
                match message.context("reading market monitor WebSocket")? {
                    Message::Text(text) => {
                        let value: Value = serde_json::from_str(text.as_ref())
                            .context("decoding market monitor JSON-RPC message")?;
                        if let Some(request_id) = value.get("id").and_then(Value::as_u64) {
                            if let Some(subscription) = pending.remove(&request_id) {
                                if let Some(subscription_id) = value.get("result").and_then(Value::as_u64) {
                                    active.insert(subscription_id, subscription.clone());
                                    let _ = events.send(json!({
                                        "type":"market_subscription_active",
                                        "data":{
                                            "mint_address":subscription.mint,
                                            "account_address":subscription.account,
                                            "venue":subscription.venue,
                                            "subscription_id":subscription_id,
                                            "active_subscriptions":active.len(),
                                            "received_at":chrono::Utc::now().to_rfc3339()
                                        }
                                    }));
                                } else {
                                    let error = value.get("error").cloned().unwrap_or(Value::Null);
                                    let _ = events.send(json!({
                                        "type":"diagnostic",
                                        "data":{
                                            "severity":"warning",
                                            "component":"market_monitor_subscribe",
                                            "mint_address":subscription.mint,
                                            "account_address":subscription.account,
                                            "error":error,
                                            "received_at":chrono::Utc::now().to_rfc3339()
                                        }
                                    }));
                                }
                            }
                            continue;
                        }
                        if value.get("method").and_then(Value::as_str) != Some("accountNotification") {
                            continue;
                        }
                        let Some(subscription_id) = value.pointer("/params/subscription").and_then(Value::as_u64) else {
                            continue;
                        };
                        let Some(subscription) = active.get(&subscription_id) else {
                            continue;
                        };
                        let result = value.pointer("/params/result");
                        let slot = result.and_then(|v| v.pointer("/context/slot")).and_then(Value::as_u64);
                        let account_value = result.and_then(|v| v.get("value"));
                        let encoded = account_value
                            .and_then(|v| v.pointer("/data/0"))
                            .and_then(Value::as_str);
                        let Some(encoded) = encoded else {
                            let _ = events.send(json!({
                                "type":"diagnostic",
                                "data":{
                                    "severity":"warning",
                                    "component":"market_monitor_decode",
                                    "message":"Account notification did not contain base64 account data.",
                                    "mint_address":subscription.mint,
                                    "account_address":subscription.account
                                }
                            }));
                            continue;
                        };
                        let owner = account_value.and_then(|v| v.get("owner")).and_then(Value::as_str);
                        let lamports = account_value.and_then(|v| v.get("lamports")).and_then(Value::as_u64);
                        let space = account_value.and_then(|v| v.get("space")).and_then(Value::as_u64);
                        let owner_valid = match subscription.venue {
                            "spl_mint" | "pump_swap_base_vault" => owner.is_some_and(is_supported_token_program),
                            "pump_swap_quote_vault" => owner == Some(SPL_TOKEN_PROGRAM_ID),
                            _ => owner == Some(subscription.program_id.as_str()),
                        };
                        if !owner_valid {
                            let _ = events.send(json!({
                                "type":"diagnostic",
                                "data":{
                                    "severity":"warning",
                                    "component":"market_monitor_owner_check",
                                    "message":"Market account owner did not match the expected program for this subscription.",
                                    "mint_address":subscription.mint,
                                    "account_address":subscription.account,
                                    "venue":subscription.venue,
                                    "expected_owner":subscription.program_id,
                                    "observed_owner":owner
                                }
                            }));
                            continue;
                        }
                        let raw = match base64::engine::general_purpose::STANDARD.decode(encoded) {
                            Ok(bytes) => bytes,
                            Err(error) => {
                                let _ = events.send(json!({
                                    "type":"diagnostic",
                                    "data":{
                                        "severity":"warning",
                                        "component":"market_monitor_base64",
                                        "message":error.to_string(),
                                        "mint_address":subscription.mint
                                    }
                                }));
                                continue;
                            }
                        };
                        if subscription.venue == "pump_swap_pool" {
                            let pool = match decode_pump_swap_pool_account(&raw) {
                                Ok(pool) => pool,
                                Err(error) => {
                                    let _ = events.send(json!({"type":"diagnostic","data":{
                                        "severity":"warning","component":"pump_swap_pool_decoder",
                                        "message":error.to_string(),"mint_address":subscription.mint,
                                        "account_address":subscription.account
                                    }}));
                                    continue;
                                }
                            };
                            if pool.base_mint != subscription.mint
                                || pool.quote_mint != WRAPPED_SOL_MINT
                                || pool.index != 0
                            {
                                let _ = events.send(json!({"type":"diagnostic","data":{
                                    "severity":"warning","component":"pump_swap_pool_validation",
                                    "message":"Derived PumpSwap pool did not match expected mint, wrapped-SOL quote mint, or canonical migration index.",
                                    "mint_address":subscription.mint,"account_address":subscription.account,
                                    "observed_base_mint":pool.base_mint,"observed_quote_mint":pool.quote_mint,
                                    "observed_index":pool.index
                                }}));
                                continue;
                            }
                            let mut state = PumpSwapLiveState {
                                virtual_quote_reserves: pool.virtual_quote_reserves,
                                pool_slot: slot,
                                latest_slot: slot,
                                ..PumpSwapLiveState::default()
                            };
                            if let Some(existing) = pumpswap_states.get(&subscription.mint) {
                                state.base_decimals = existing.base_decimals;
                                state.base_total_supply = existing.base_total_supply;
                                state.base_reserves = existing.base_reserves;
                                state.quote_reserves = existing.quote_reserves;
                            }
                            pumpswap_states.insert(subscription.mint.clone(), state);
                            let required = [
                                (
                                    "spl_mint",
                                    subscription.mint.clone(),
                                    None,
                                    "spl_token_program",
                                ),
                                (
                                    "pump_swap_base_vault",
                                    pool.pool_base_token_account.clone(),
                                    Some(pool.base_mint.clone()),
                                    "spl_token_program",
                                ),
                                (
                                    "pump_swap_quote_vault",
                                    pool.pool_quote_token_account.clone(),
                                    Some(pool.quote_mint.clone()),
                                    SPL_TOKEN_PROGRAM_ID,
                                ),
                            ];
                            if desired.len().saturating_add(required.len()) <= MAX_MARKET_SUBSCRIPTIONS {
                                for (venue, account, expected_mint, expected_program) in required {
                                    if desired.contains_key(&account) {
                                        continue;
                                    }
                                    let child = MarketSubscription {
                                        mint: subscription.mint.clone(),
                                        account: account.clone(),
                                        venue,
                                        program_id: expected_program.to_owned(),
                                        expected_mint,
                                    };
                                    let request_id = next_request_id;
                                    next_request_id = next_request_id.saturating_add(1);
                                    send_account_subscribe(&mut sink, request_id, &account).await?;
                                    desired.insert(account, child.clone());
                                    pending.insert(request_id, child);
                                }
                            } else {
                                let _ = events.send(json!({"type":"diagnostic","data":{
                                    "severity":"warning","component":"market_monitor",
                                    "message":"Not enough remaining account-subscription capacity for the PumpSwap mint and both vaults.",
                                    "mint_address":subscription.mint,"required_additional_accounts":3,
                                    "remaining_capacity":MAX_MARKET_SUBSCRIPTIONS.saturating_sub(desired.len())
                                }}));
                            }
                            let _ = events.send(json!({"type":"market_pool_verified","data":{
                                "mint_address":subscription.mint,"pool_address":subscription.account,
                                "venue":"pump_swap","index":pool.index,"pool_creator":pool.creator,
                                "base_mint":pool.base_mint,"quote_mint":pool.quote_mint,
                                "pool_base_token_account":pool.pool_base_token_account,
                                "pool_quote_token_account":pool.pool_quote_token_account,
                                "lp_supply":pool.lp_supply,
                                "virtual_quote_reserves":pool.virtual_quote_reserves,
                                "slot":slot,"status":"pool_verified_vault_subscriptions_pending",
                                "received_at":chrono::Utc::now().to_rfc3339()
                            }}));
                            continue;
                        }
                        if subscription.venue == "spl_mint" {
                            let mint_state = match decode_spl_mint_account(&raw) {
                                Ok(value) => value,
                                Err(error) => {
                                    let _ = events.send(json!({"type":"diagnostic","data":{
                                        "severity":"warning","component":"pump_swap_mint_decoder",
                                        "message":error.to_string(),"mint_address":subscription.mint
                                    }}));
                                    continue;
                                }
                            };
                            if let Some(state) = pumpswap_states.get_mut(&subscription.mint) {
                                state.base_decimals = Some(mint_state.decimals);
                                state.base_total_supply = Some(mint_state.supply);
                                state.latest_slot = slot.or(state.latest_slot);
                            }
                        } else if subscription.venue == "pump_swap_base_vault"
                            || subscription.venue == "pump_swap_quote_vault"
                        {
                            let token_state = match decode_spl_token_account(&raw) {
                                Ok(value) => value,
                                Err(error) => {
                                    let _ = events.send(json!({"type":"diagnostic","data":{
                                        "severity":"warning","component":"pump_swap_vault_decoder",
                                        "message":error.to_string(),"mint_address":subscription.mint,
                                        "account_address":subscription.account
                                    }}));
                                    continue;
                                }
                            };
                            let actual_mint = bs58::encode(&raw[0..32]).into_string();
                            if subscription.expected_mint.as_deref() != Some(actual_mint.as_str()) {
                                let _ = events.send(json!({"type":"diagnostic","data":{
                                    "severity":"warning","component":"pump_swap_vault_validation",
                                    "message":"Vault token mint did not match the mint recorded in the verified PumpSwap pool.",
                                    "mint_address":subscription.mint,"account_address":subscription.account,
                                    "expected_mint":subscription.expected_mint,"observed_mint":actual_mint
                                }}));
                                continue;
                            }
                            if let Some(state) = pumpswap_states.get_mut(&subscription.mint) {
                                if subscription.venue == "pump_swap_base_vault" {
                                    state.base_reserves = Some(token_state.amount);
                                } else {
                                    state.quote_reserves = Some(token_state.amount);
                                }
                                state.latest_slot = slot.or(state.latest_slot);
                            }
                        } else {
                            // Bonding-curve account path continues below.
                        }
                        if subscription.venue == "spl_mint"
                            || subscription.venue == "pump_swap_base_vault"
                            || subscription.venue == "pump_swap_quote_vault"
                        {
                            if let Some(state) = pumpswap_states.get(&subscription.mint) {
                                if let Ok((price_sol, market_cap_sol, effective_quote_reserves)) =
                                    pump_swap_price(state)
                                {
                                        let now_ms = chrono::Utc::now().timestamp_millis();
                                        let history =
                                            price_history.entry(subscription.mint.clone()).or_default();
                                        let change_1m = percentage_change(history, now_ms, 60, price_sol);
                                        let change_5m = percentage_change(history, now_ms, 300, price_sol);
                                        let should_sample = history.back().is_none_or(|sample| {
                                            now_ms.saturating_sub(sample.timestamp_ms) >= 500
                                        });
                                        if should_sample {
                                            history.push_back(PriceSample {
                                                timestamp_ms: now_ms,
                                                price_sol,
                                            });
                                        }
                                        while history.front().is_some_and(|sample| {
                                            now_ms.saturating_sub(sample.timestamp_ms) > 600_000
                                        }) || history.len() > 1_200
                                        {
                                            history.pop_front();
                                        }
                                        let _ = events.send(json!({"type":"market_price_update","data":{
                                            "mint_address":subscription.mint,"account_address":subscription.account,
                                            "venue":"pump_swap","pool_address":desired.values().find(|item| item.mint == subscription.mint && item.venue == "pump_swap_pool").map(|item| item.account.clone()),
                                            "slot":state.latest_slot,"pool_slot":state.pool_slot,
                                            "price_sol":price_sol,"market_cap_sol":market_cap_sol,
                                            "base_reserves":state.base_reserves,"quote_vault_reserves":state.quote_reserves,
                                            "virtual_quote_reserves":state.virtual_quote_reserves,
                                            "effective_quote_reserves":effective_quote_reserves,
                                            "base_decimals":state.base_decimals,"token_total_supply":state.base_total_supply,
                                            "price_change_1m_pct":change_1m,"price_change_5m_pct":change_5m,
                                            "history_samples":history.len(),
                                            "price_change_status":if change_1m.is_some() && change_5m.is_some() {"ready"} else {"warming_up"},
                                            "received_at":chrono::Utc::now().to_rfc3339()
                                        }}));
                                }
                            }
                            continue;
                        }
                        let curve = match decode_pump_bonding_curve_account(&raw) {
                            Ok(curve) => curve,
                            Err(error) => {
                                let _ = events.send(json!({
                                    "type":"diagnostic",
                                    "data":{
                                        "severity":"warning",
                                        "component":"pump_curve_decoder",
                                        "message":error.to_string(),
                                        "mint_address":subscription.mint,
                                        "account_address":subscription.account
                                    }
                                }));
                                continue;
                            }
                        };
                        let now_ms = chrono::Utc::now().timestamp_millis();
                        let history = price_history.entry(subscription.mint.clone()).or_default();
                        let change_1m = percentage_change(history, now_ms, 60, curve.price_sol);
                        let change_5m = percentage_change(history, now_ms, 300, curve.price_sol);
                        let should_sample = history
                            .back()
                            .is_none_or(|sample| now_ms.saturating_sub(sample.timestamp_ms) >= 500);
                        if should_sample {
                            history.push_back(PriceSample {
                                timestamp_ms: now_ms,
                                price_sol: curve.price_sol,
                            });
                        }
                        while history.front().is_some_and(|sample| now_ms.saturating_sub(sample.timestamp_ms) > 600_000)
                            || history.len() > 1_200
                        {
                            history.pop_front();
                        }
                        let real_token_reserves = curve.real_token_reserves;
                        let complete = curve.complete;
                        let _ = events.send(json!({
                            "type":"market_account_update",
                            "data":{
                                "mint_address":subscription.mint,
                                "account_address":subscription.account,
                                "venue":"pump_fun_bonding_curve",
                                "slot":slot,
                                "owner":owner,
                                "lamports":lamports,
                                "space":space,
                                "account_data_size":raw.len(),
                                "account_data_base64":encoded,
                                "received_at":chrono::Utc::now().to_rfc3339()
                            }
                        }));
                        let _ = events.send(json!({
                            "type":"market_price_update",
                            "data":{
                                "mint_address":subscription.mint,
                                "account_address":subscription.account,
                                "venue":"pump_fun_bonding_curve",
                                "slot":slot,
                                "price_sol":curve.price_sol,
                                "price_lamports_per_token":curve.price_sol * 1_000_000_000.0,
                                "market_cap_sol":curve.market_cap_sol,
                                "virtual_token_reserves":curve.virtual_token_reserves,
                                "virtual_quote_reserves":curve.virtual_quote_reserves,
                                "virtual_sol_reserves":curve.virtual_quote_reserves,
                                "real_token_reserves":curve.real_token_reserves,
                                "real_quote_reserves":curve.real_quote_reserves,
                                "real_sol_reserves":curve.real_quote_reserves,
                                "token_total_supply":curve.token_total_supply,
                                "creator":curve.creator,
                                "complete":curve.complete,
                                "price_change_1m_pct":change_1m,
                                "price_change_5m_pct":change_5m,
                                "history_samples":history.len(),
                                "price_change_status":if change_1m.is_some() && change_5m.is_some() {"ready"} else {"warming_up"},
                                "received_at":chrono::Utc::now().to_rfc3339()
                            }
                        }));
                        if complete {
                            let _ = events.send(json!({
                                "type":"market_curve_completed",
                                "data":{
                                    "mint_address":subscription.mint,
                                    "account_address":subscription.account,
                                    "venue":"pump_fun_bonding_curve",
                                    "real_token_reserves":real_token_reserves,
                                    "status":"graduation_required",
                                    "received_at":chrono::Utc::now().to_rfc3339()
                                }
                            }));
                            match derive_pump_swap_pool(&subscription.mint, &subscription.program_id) {
                                Ok(pool_address) if !desired.contains_key(&pool_address) => {
                                    if desired.len() < MAX_MARKET_SUBSCRIPTIONS {
                                        let pool_subscription = MarketSubscription {
                                            mint: subscription.mint.clone(),
                                            account: pool_address.clone(),
                                            venue: "pump_swap_pool",
                                            program_id: PUMP_SWAP_PROGRAM_ID.to_owned(),
                                            expected_mint: None,
                                        };
                                        let request_id = next_request_id;
                                        next_request_id = next_request_id.saturating_add(1);
                                        send_account_subscribe(&mut sink, request_id, &pool_address).await?;
                                        desired.insert(pool_address.clone(), pool_subscription.clone());
                                        pending.insert(request_id, pool_subscription);
                                        let _ = events.send(json!({"type":"market_pool_discovery","data":{
                                            "mint_address":subscription.mint,"pool_address":pool_address,
                                            "venue":"pump_swap","status":"subscription_pending",
                                            "received_at":chrono::Utc::now().to_rfc3339()
                                        }}));
                                    } else {
                                        let _ = events.send(json!({"type":"diagnostic","data":{
                                            "severity":"warning","component":"market_monitor",
                                            "message":"Market account subscription capacity reached before PumpSwap pool could be monitored.",
                                            "mint_address":subscription.mint,"limit":MAX_MARKET_SUBSCRIPTIONS
                                        }}));
                                    }
                                }
                                Ok(_) => {}
                                Err(error) => {
                                    let _ = events.send(json!({"type":"diagnostic","data":{
                                        "severity":"warning","component":"pump_swap_pool_derivation",
                                        "message":error.to_string(),"mint_address":subscription.mint
                                    }}));
                                }
                            }
                        }
                    }
                    Message::Ping(payload) => {
                        sink.send(Message::Pong(payload)).await.context("replying to market monitor ping")?;
                    }
                    Message::Close(_) => return Ok(()),
                    _ => {}
                }
            }
        }
    }
}


/// Verified PumpSwap activity decoded from the official Anchor event layouts.
/// Amounts are raw token units; quote amounts are SOL lamports only for WSOL pools.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PumpSwapActivityEvent {
    Buy { timestamp: i64, pool: String, user: String, base_amount: u64, quote_amount: u64 },
    Sell { timestamp: i64, pool: String, user: String, base_amount: u64, quote_amount: u64 },
    Deposit { timestamp: i64, pool: String, user: String, base_amount: u64, quote_amount: u64 },
    Withdraw { timestamp: i64, pool: String, user: String, base_amount: u64, quote_amount: u64 },
}

const PUMPSWAP_BUY_EVENT: [u8; 8] = [103, 244, 82, 31, 44, 245, 119, 119];
const PUMPSWAP_SELL_EVENT: [u8; 8] = [62, 47, 55, 10, 165, 3, 220, 42];
const PUMPSWAP_DEPOSIT_EVENT: [u8; 8] = [120, 248, 61, 83, 31, 142, 107, 144];
const PUMPSWAP_WITHDRAW_EVENT: [u8; 8] = [22, 9, 133, 26, 160, 44, 71, 192];

/// Decode the stable prefix of PumpSwap events from the official Anchor IDL.
/// Unknown trailing fields are ignored to tolerate future appended fields.
pub fn decode_pump_swap_activity_event(data: &[u8]) -> Result<Option<PumpSwapActivityEvent>> {
    if data.len() < 8 { return Ok(None); }
    let discriminator: [u8; 8] = data[..8].try_into().expect("eight-byte discriminator");
    let read_u64 = |offset: usize| -> Result<u64> {
        let bytes: [u8; 8] = data.get(offset..offset + 8)
            .ok_or_else(|| anyhow!("PumpSwap activity event is truncated"))?
            .try_into().map_err(|_| anyhow!("invalid PumpSwap activity integer"))?;
        Ok(u64::from_le_bytes(bytes))
    };
    let read_i64 = |offset: usize| -> Result<i64> {
        let bytes: [u8; 8] = data.get(offset..offset + 8)
            .ok_or_else(|| anyhow!("PumpSwap activity timestamp is truncated"))?
            .try_into().map_err(|_| anyhow!("invalid PumpSwap activity timestamp"))?;
        Ok(i64::from_le_bytes(bytes))
    };
    let read_key = |offset: usize| -> Result<String> {
        let bytes = data.get(offset..offset + 32)
            .ok_or_else(|| anyhow!("PumpSwap activity public key is truncated"))?;
        Ok(bs58::encode(bytes).into_string())
    };
    let event = match discriminator {
        PUMPSWAP_BUY_EVENT | PUMPSWAP_SELL_EVENT => {
            if data.len() < 184 { return Err(anyhow!("PumpSwap trade event is shorter than its stable prefix")); }
            let timestamp = read_i64(8)?;
            let base_amount = read_u64(16)?;
            let quote_amount = read_u64(64)?;
            let pool = read_key(120)?;
            let user = read_key(152)?;
            if discriminator == PUMPSWAP_BUY_EVENT {
                PumpSwapActivityEvent::Buy { timestamp, pool, user, base_amount, quote_amount }
            } else {
                PumpSwapActivityEvent::Sell { timestamp, pool, user, base_amount, quote_amount }
            }
        }
        PUMPSWAP_DEPOSIT_EVENT | PUMPSWAP_WITHDRAW_EVENT => {
            if data.len() < 160 { return Err(anyhow!("PumpSwap liquidity event is shorter than its stable prefix")); }
            let timestamp = read_i64(8)?;
            let base_amount = read_u64(72)?;
            let quote_amount = read_u64(80)?;
            let pool = read_key(96)?;
            let user = read_key(128)?;
            if discriminator == PUMPSWAP_DEPOSIT_EVENT {
                PumpSwapActivityEvent::Deposit { timestamp, pool, user, base_amount, quote_amount }
            } else {
                PumpSwapActivityEvent::Withdraw { timestamp, pool, user, base_amount, quote_amount }
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(event))
}

/// Extract an Anchor Program data payload. The caller must verify the emitting
/// program using the transaction's invocation stack before accepting the event.
pub fn decode_pump_swap_activity_log_line(line: &str) -> Result<Option<PumpSwapActivityEvent>> {
    let Some(encoded) = line.strip_prefix("Program data: ") else { return Ok(None); };
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded.trim()).context("decoding PumpSwap Anchor event base64")?;
    decode_pump_swap_activity_event(&bytes)
}

#[cfg(test)]
mod market_monitor_tests {
    use super::*;


    fn encoded_pumpswap_trade(discriminator: [u8; 8]) -> Vec<u8> {
        let mut data = discriminator.to_vec();
        data.extend_from_slice(&1_700_000_000_i64.to_le_bytes());
        for value in 1_u64..=13 { data.extend_from_slice(&value.to_le_bytes()); }
        data.extend_from_slice(&[8_u8; 32]);
        data.extend_from_slice(&[9_u8; 32]);
        data.extend_from_slice(&[0_u8; 128]);
        data
    }

    #[test]
    fn decodes_official_pumpswap_buy_and_sell_event_prefixes() {
        let buy = decode_pump_swap_activity_event(&encoded_pumpswap_trade(PUMPSWAP_BUY_EVENT))
            .unwrap().unwrap();
        assert_eq!(buy, PumpSwapActivityEvent::Buy {
            timestamp: 1_700_000_000,
            pool: bs58::encode([8_u8; 32]).into_string(),
            user: bs58::encode([9_u8; 32]).into_string(),
            base_amount: 1, quote_amount: 7,
        });
        let sell = decode_pump_swap_activity_event(&encoded_pumpswap_trade(PUMPSWAP_SELL_EVENT))
            .unwrap().unwrap();
        assert_eq!(sell, PumpSwapActivityEvent::Sell {
            timestamp: 1_700_000_000,
            pool: bs58::encode([8_u8; 32]).into_string(),
            user: bs58::encode([9_u8; 32]).into_string(),
            base_amount: 1, quote_amount: 7,
        });
    }

    #[test]
    fn decodes_pumpswap_liquidity_events_separately_from_trades() {
        for (discriminator, deposit) in [(PUMPSWAP_DEPOSIT_EVENT, true), (PUMPSWAP_WITHDRAW_EVENT, false)] {
            let mut data = discriminator.to_vec();
            data.extend_from_slice(&1_700_000_000_i64.to_le_bytes());
            for value in 1_u64..=10 { data.extend_from_slice(&value.to_le_bytes()); }
            data.extend_from_slice(&[8_u8; 32]);
            data.extend_from_slice(&[9_u8; 32]);
            let event = decode_pump_swap_activity_event(&data).unwrap().unwrap();
            if deposit {
                assert_eq!(event, PumpSwapActivityEvent::Deposit {
                    timestamp: 1_700_000_000,
                    pool: bs58::encode([8_u8; 32]).into_string(),
                    user: bs58::encode([9_u8; 32]).into_string(),
                    base_amount: 8, quote_amount: 9,
                });
            } else {
                assert_eq!(event, PumpSwapActivityEvent::Withdraw {
                    timestamp: 1_700_000_000,
                    pool: bs58::encode([8_u8; 32]).into_string(),
                    user: bs58::encode([9_u8; 32]).into_string(),
                    base_amount: 8, quote_amount: 9,
                });
            }
        }
    }

    #[test]
    fn ignores_unrelated_anchor_event_and_rejects_truncated_pumpswap_event() {
        assert_eq!(decode_pump_swap_activity_event(&[0_u8; 8]).unwrap(), None);
        assert!(decode_pump_swap_activity_event(&PUMPSWAP_BUY_EVENT).is_err());
    }

    fn encoded_curve(complete: u8) -> Vec<u8> {
        let mut data = vec![23, 183, 248, 55, 96, 216, 172, 96];
        for value in [
            1_000_000_000_u64,
            30_000_000_000,
            800_000_000,
            1_000_000,
            1_000_000_000,
        ] {
            data.extend_from_slice(&value.to_le_bytes());
        }
        data.push(complete);
        data.extend_from_slice(&[7_u8; 32]);
        // Current IDL appends mayhem/cashback flags and a quote_mint pubkey.
        data.extend_from_slice(&[0_u8, 0_u8]);
        data.extend_from_slice(&[0_u8; 32]);
        data
    }

    fn encoded_pool() -> Vec<u8> {
        let mut data = vec![241, 154, 109, 4, 17, 177, 109, 188];
        data.push(254);
        data.extend_from_slice(&0_u16.to_le_bytes());
        for byte in [1_u8, 2, 3, 4, 5, 6] {
            data.extend_from_slice(&[byte; 32]);
        }
        data.extend_from_slice(&99_u64.to_le_bytes());
        data.extend_from_slice(&[7_u8; 32]);
        data.extend_from_slice(&[0_u8, 0_u8]);
        data.extend_from_slice(&(-12_i128).to_le_bytes());
        data.extend_from_slice(&[0_u8; 10]);
        data
    }

    #[test]
    fn decodes_pumpswap_pool_fields_and_signed_virtual_quote_reserves() {
        let pool = decode_pump_swap_pool_account(&encoded_pool()).unwrap();
        assert_eq!(pool.index, 0);
        assert_eq!(pool.base_mint, bs58::encode([2_u8; 32]).into_string());
        assert_eq!(pool.quote_mint, bs58::encode([3_u8; 32]).into_string());
        assert_eq!(
            pool.pool_base_token_account,
            bs58::encode([5_u8; 32]).into_string()
        );
        assert_eq!(
            pool.pool_quote_token_account,
            bs58::encode([6_u8; 32]).into_string()
        );
        assert_eq!(pool.lp_supply, 99);
        assert_eq!(pool.virtual_quote_reserves, -12);
    }

    #[test]
    fn rejects_invalid_pumpswap_pool_discriminator_and_short_layout() {
        assert!(decode_pump_swap_pool_account(&[0; 211]).is_err());
        let mut invalid = encoded_pool();
        invalid[0] = 0;
        assert!(decode_pump_swap_pool_account(&invalid).is_err());
    }

    #[test]
    fn accepts_legacy_pumpswap_pool_without_virtual_quote_reserves() {
        let mut legacy = encoded_pool();
        legacy.truncate(245);
        let pool = decode_pump_swap_pool_account(&legacy).unwrap();
        assert_eq!(pool.virtual_quote_reserves, 0);
    }

    #[test]
    fn decodes_spl_mint_supply_and_decimals() {
        let mut data = vec![0_u8; 82];
        data[36..44].copy_from_slice(&1_000_000_000_u64.to_le_bytes());
        data[44] = 6;
        data[45] = 1;
        assert_eq!(
            decode_spl_mint_account(&data).unwrap(),
            SplMintState {
                supply: 1_000_000_000,
                decimals: 6
            }
        );
        data[45] = 0;
        assert!(decode_spl_mint_account(&data).is_err());
    }

    #[test]
    fn decodes_spl_token_account_amount_and_rejects_uninitialized() {
        let mut data = vec![0_u8; 165];
        data[64..72].copy_from_slice(&987_654_u64.to_le_bytes());
        data[108] = 1;
        assert_eq!(decode_spl_token_account(&data).unwrap().amount, 987_654);
        data[108] = 0;
        assert!(decode_spl_token_account(&data).is_err());
    }

    #[test]
    fn pumpswap_price_uses_signed_virtual_quote_reserves() {
        let state = PumpSwapLiveState {
            virtual_quote_reserves: -500_000_000,
            base_decimals: Some(6),
            base_total_supply: Some(1_000_000_000),
            base_reserves: Some(10_000_000),
            quote_reserves: Some(1_500_000_000),
            ..PumpSwapLiveState::default()
        };
        let (price, cap, effective) = pump_swap_price(&state).unwrap();
        assert_eq!(effective, 1_000_000_000);
        assert!((price - 0.1).abs() < 1e-12);
        assert!((cap - 100.0).abs() < 1e-9);
    }

    #[test]
    fn pumpswap_price_rejects_negative_effective_quote_reserves() {
        let state = PumpSwapLiveState {
            virtual_quote_reserves: -2_000_000_000,
            base_decimals: Some(6),
            base_total_supply: Some(1_000_000_000),
            base_reserves: Some(10_000_000),
            quote_reserves: Some(1_000_000_000),
            ..PumpSwapLiveState::default()
        };
        assert!(pump_swap_price(&state).is_err());
    }

    #[test]
    fn decodes_documented_bonding_curve_layout_and_price() {
        let curve = decode_pump_bonding_curve_account(&encoded_curve(0)).unwrap();
        assert_eq!(curve.virtual_token_reserves, 1_000_000_000);
        assert_eq!(curve.virtual_quote_reserves, 30_000_000_000);
        assert!((curve.price_sol - 0.03).abs() < 1e-12);
        assert!((curve.market_cap_sol - 30.0).abs() < 1e-9);
        assert!(!curve.complete);
    }

    #[test]
    fn rejects_wrong_discriminator_and_truncated_accounts() {
        let mut wrong = encoded_curve(0);
        wrong[0] = 0;
        assert!(decode_pump_bonding_curve_account(&wrong).is_err());
        assert!(decode_pump_bonding_curve_account(&[0; 80]).is_err());
    }

    #[test]
    fn rejects_non_sol_quote_mints_instead_of_mispricing_them() {
        let mut data = encoded_curve(0);
        data[83] = 1;
        assert!(decode_pump_bonding_curve_account(&data).is_err());
    }

    #[test]
    fn supports_legacy_sol_curve_accounts_without_appended_quote_mint() {
        let mut data = encoded_curve(0);
        data.truncate(81);
        let curve = decode_pump_bonding_curve_account(&data).unwrap();
        assert!((curve.price_sol - 0.03).abs() < 1e-12);
    }

    #[test]
    fn rejects_invalid_complete_flag_and_zero_reserves() {
        assert!(decode_pump_bonding_curve_account(&encoded_curve(2)).is_err());
        let mut zero = encoded_curve(0);
        zero[8..16].copy_from_slice(&0_u64.to_le_bytes());
        assert!(decode_pump_bonding_curve_account(&zero).is_err());
    }

    #[test]
    fn price_windows_require_a_sample_at_or_before_the_window() {
        let mut samples = std::collections::VecDeque::new();
        samples.push_back(PriceSample {
            timestamp_ms: 1_000,
            price_sol: 1.0,
        });
        assert!(percentage_change(&samples, 30_000, 60, 1.5).is_none());
        samples.push_back(PriceSample {
            timestamp_ms: 20_000,
            price_sol: 1.1,
        });
        samples.push_back(PriceSample {
            timestamp_ms: 30_000,
            price_sol: 1.2,
        });
        assert_eq!(percentage_change(&samples, 90_000, 60, 1.5), Some(25.0));
    }
}
