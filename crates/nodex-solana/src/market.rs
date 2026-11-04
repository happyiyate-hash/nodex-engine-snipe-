use anyhow::{anyhow, Context, Result};
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
                        let _ = events.send(json!({
                            "type":"market_account_update",
                            "data":{
                                "mint_address":subscription.mint,
                                "account_address":subscription.account,
                                "venue":subscription.venue,
                                "slot":slot,
                                "owner":owner,
                                "lamports":lamports,
                                "space":space,
                                "account_data_base64":encoded,
                                "received_at":chrono::Utc::now().to_rfc3339()
                            }
                        }));
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

    #[test]
    fn pump_fun_bonding_curve_pda_is_deterministic() {
        let program_id = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
        let mint = "So11111111111111111111111111111111111111112";
        let first = derive_pump_fun_bonding_curve(mint, program_id).unwrap();
        let second = derive_pump_fun_bonding_curve(mint, program_id).unwrap();
        assert_eq!(first, second);
        assert_ne!(first, mint);
    }

    #[test]
    fn invalid_public_keys_are_rejected() {
        assert!(derive_pump_fun_bonding_curve("not-a-mint", "not-a-program").is_err());
    }
}
