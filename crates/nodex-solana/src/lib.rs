use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::broadcast;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

const PUMP_CREATE_DISCRIMINATOR: [u8; 8] = [0x18, 0x1e, 0xc8, 0x28, 0x05, 0x1c, 0x07, 0x77];
const PUMP_CREATE_V2_DISCRIMINATOR: [u8; 8] = [0xd6, 0x90, 0x4c, 0xec, 0x5f, 0x8b, 0x31, 0xb4];

pub const SPL_TOKEN_ACCOUNT_SIZE: usize = 165;

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

    async fn call<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: Value,
    ) -> Result<T> {
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

    /// Fetches a confirmed transaction in raw JSON form, including version-0 transactions.
    /// A null result is returned as None because RPC nodes may not have indexed it yet.
    pub async fn get_transaction(&self, signature: &str) -> Result<Option<Value>> {
        let response = self.client
            .post(&self.http_url)
            .json(&json!({
                "jsonrpc":"2.0", "id":1, "method":"getTransaction",
                "params":[signature, {"encoding":"json","commitment":"confirmed","maxSupportedTransactionVersion":0}]
            }))
            .send().await.context("sending Solana getTransaction request")?
            .error_for_status().context("Solana getTransaction returned HTTP error")?
            .json::<RpcResponse<Value>>().await.context("decoding getTransaction response")?;
        if let Some(error) = response.error {
            return Err(anyhow!("Solana RPC getTransaction error: {}", error.message));
        }
        Ok(response.result.filter(|value| !value.is_null()))
    }
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
    if transaction.pointer("/meta/err").is_some_and(|error| !error.is_null()) {
        return Ok(None);
    }
    let message = transaction.pointer("/transaction/message")
        .ok_or_else(|| anyhow!("getTransaction response has no transaction.message"))?;
    let mut keys: Vec<String> = message.get("accountKeys").and_then(Value::as_array)
        .ok_or_else(|| anyhow!("transaction message has no accountKeys array"))?
        .iter().map(|key| {
            key.as_str().map(str::to_owned)
                .or_else(|| key.get("pubkey").and_then(Value::as_str).map(str::to_owned))
                .ok_or_else(|| anyhow!("transaction contains an invalid account key"))
        }).collect::<Result<Vec<_>>>()?;
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
    if let Some(groups) = transaction.pointer("/meta/innerInstructions").and_then(Value::as_array) {
        for group in groups {
            if let Some(inner) = group.get("instructions").and_then(Value::as_array) {
                instructions.extend(inner.iter());
            }
        }
    }
    for instruction in instructions {
        let Some(program_index) = instruction.get("programIdIndex").and_then(Value::as_u64) else { continue };
        let Some(program_id) = keys.get(program_index as usize) else { continue };
        if program_id != configured_program_id { continue; }
        let Some(data) = instruction.get("data").and_then(Value::as_str) else { continue };
        let bytes = bs58::decode(data).into_vec().context("decoding Pump.fun instruction data")?;
        if bytes.len() < 8 { continue; }
        let discriminator: [u8; 8] = bytes[..8].try_into().expect("checked discriminator length");
        if discriminator != PUMP_CREATE_DISCRIMINATOR && discriminator != PUMP_CREATE_V2_DISCRIMINATOR {
            continue;
        }
        let accounts = instruction.get("accounts").and_then(Value::as_array)
            .ok_or_else(|| anyhow!("Pump.fun create instruction has no accounts array"))?;
        let mint_index = accounts.first().and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("Pump.fun create instruction has no mint account index"))?;
        let mint_address = keys.get(mint_index as usize)
            .ok_or_else(|| anyhow!("Pump.fun mint account index is out of range"))?.clone();
        let creator_address = decode_create_creator(&bytes[8..])?;
        let slot = transaction.get("slot").and_then(Value::as_u64).unwrap_or_default();
        if signature.trim().is_empty() {
            return Err(anyhow!("transaction signature is empty"));
        }
        return Ok(Some(DecodedPumpFunCreate { mint_address, creator_address, slot }));
    }
    Ok(None)
}

fn decode_create_creator(args: &[u8]) -> Result<String> {
    let mut offset = 0usize;
    for label in ["name", "symbol", "uri"] {
        let length_bytes = args.get(offset..offset + 4)
            .ok_or_else(|| anyhow!("Pump.fun create instruction is missing {label} length"))?;
        let length = u32::from_le_bytes(length_bytes.try_into().expect("four-byte slice")) as usize;
        offset += 4;
        let value = args.get(offset..offset + length)
            .ok_or_else(|| anyhow!("Pump.fun create instruction is missing {label} bytes"))?;
        std::str::from_utf8(value).with_context(|| format!("Pump.fun {label} is not UTF-8"))?;
        offset += length;
    }
    let creator = args.get(offset..offset + 32)
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
}

/// Forwards raw logsNotification JSON. The mentions filter accepts exactly one pubkey.
pub async fn run_logs_stream(
    ws_url: String,
    program_id: String,
    sender: broadcast::Sender<Value>,
    status: std::sync::Arc<tokio::sync::RwLock<StreamStatus>>,
) {
    let mut reconnect_count = 0_u64;
    loop {
        if let Err(error) = connect_and_subscribe(&ws_url, &program_id, &sender, &status).await {
            warn!(%error, "Solana WebSocket stream ended; reconnecting");
            status.write().await.last_error = Some(error.to_string());
            let _ = sender.send(json!({"type":"diagnostic","data":{"severity":"error","component":"solana_websocket","message":error.to_string(),"retrying":true,"received_at":chrono::Utc::now().to_rfc3339()}}));
        }
        reconnect_count = reconnect_count.saturating_add(1);
        {
            let mut current = status.write().await;
            current.connected = false;
            current.subscription_id = None;
            current.reconnect_count = reconnect_count;
        }
        let delay = 2_u64.saturating_mul(1_u64 << reconnect_count.min(4)).min(30);
        tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
    }
}

async fn connect_and_subscribe(
    ws_url: &str,
    program_id: &str,
    sender: &broadcast::Sender<Value>,
    status: &std::sync::Arc<tokio::sync::RwLock<StreamStatus>>,
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
    }
    info!(subscription_id, program_id, "Subscribed to Solana program logs");
    let _ = sender.send(json!({"type":"provider_status","data":{"provider":"solana_websocket","connected":true,"subscription_id":subscription_id,"program_id":program_id,"received_at":chrono::Utc::now().to_rfc3339()}}));

    while let Some(message) = socket.next().await {
        match message? {
            Message::Text(text) => {
                let value: Value = serde_json::from_str(&text)?;
                if value.get("method").and_then(Value::as_str) == Some("logsNotification") {
                    let _ = sender.send(value);
                }
            }
            Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
            Message::Close(_) => break,
            _ => {}
        }
    }
    let _ = sender.send(json!({"type":"provider_status","data":{"provider":"solana_websocket","connected":false,"reason":"WebSocket stream closed","received_at":chrono::Utc::now().to_rfc3339()}}));
    Ok(())
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
            "transaction": {"message": {
                "accountKeys": ["Mint111", "PumpProgram"],
                "instructions": [{
                    "programIdIndex": 1, "accounts": [0], 
                    "data": create_instruction(PUMP_CREATE_DISCRIMINATOR, creator)
                }]
            }},
            "meta": {"err": null, "loadedAddresses": {"writable": [], "readonly": []}}
        });
        let decoded = decode_pump_fun_create(&tx, "signature", "PumpProgram")
            .expect("decode should succeed").expect("create should be found");
        assert_eq!(decoded.mint_address, "Mint111");
        assert_eq!(decoded.creator_address, bs58::encode(creator).into_string());
        assert_eq!(decoded.slot, 123);
    }

    #[test]
    fn decodes_pump_create_v2_instruction() {
        let creator = [9u8; 32];
        let tx = json!({
            "slot": 456,
            "transaction": {"message": {
                "accountKeys": ["Mint222", "PumpProgram"],
                "instructions": [{
                    "programIdIndex": 1, "accounts": [0],
                    "data": create_instruction(PUMP_CREATE_V2_DISCRIMINATOR, creator)
                }]
            }},
            "meta": {"err": null}
        });
        let decoded = decode_pump_fun_create(&tx, "signature", "PumpProgram")
            .expect("decode should succeed").expect("create_v2 should be found");
        assert_eq!(decoded.mint_address, "Mint222");
        assert_eq!(decoded.creator_address, bs58::encode(creator).into_string());
    }

    #[test]
    fn ignores_failed_transactions() {
        let tx = json!({"meta":{"err":{"InstructionError":[0,"Custom"]}}});
        assert!(decode_pump_fun_create(&tx, "signature", "PumpProgram")
            .expect("failed transaction is not a decoder error").is_none());
    }
}

