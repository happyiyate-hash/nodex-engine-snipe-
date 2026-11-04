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

#[derive(Debug, Clone)]
struct MarketSubscription {
    mint: String,
    account: String,
    venue: &'static str,
    program_id: String,
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
                        if owner != Some(subscription.program_id.as_str()) {
                            let _ = events.send(json!({
                                "type":"diagnostic",
                                "data":{
                                    "severity":"warning",
                                    "component":"market_monitor_owner_check",
                                    "message":"Market account owner did not match the expected program for this subscription.",
                                    "mint_address":subscription.mint,
                                    "account_address":subscription.account,
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
                            let _ = events.send(json!({"type":"market_pool_verified","data":{
                                "mint_address":subscription.mint,"pool_address":subscription.account,
                                "venue":"pump_swap","index":pool.index,"pool_creator":pool.creator,
                                "base_mint":pool.base_mint,"quote_mint":pool.quote_mint,
                                "pool_base_token_account":pool.pool_base_token_account,
                                "pool_quote_token_account":pool.pool_quote_token_account,
                                "lp_supply":pool.lp_supply,
                                "virtual_quote_reserves":pool.virtual_quote_reserves,
                                "slot":slot,"status":"pool_verified_vault_monitoring_pending",
                                "received_at":chrono::Utc::now().to_rfc3339()
                            }}));
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

#[cfg(test)]
mod market_monitor_tests {
    use super::*;

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
