use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

/// Hard ceiling for in-memory token snapshots on the small monitoring instance.
pub const MAX_TOKEN_STATES: usize = 500;

fn evict_oldest_if_over_limit(
    records: &mut HashMap<String, TokenState>,
    protected_mint: &str,
) {
    if records.len() <= MAX_TOKEN_STATES {
        return;
    }
    let oldest_mint = records
        .iter()
        .filter(|(mint, _)| mint.as_str() != protected_mint)
        .min_by(|left, right| left.1.updated_at.cmp(&right.1.updated_at))
        .map(|(mint, _)| mint.clone());
    if let Some(oldest_mint) = oldest_mint {
        records.remove(&oldest_mint);
    }
}

/// Canonical, versioned state for one token. `data` contains independently
/// maintained domains (identity, discovery, security, market, trading, etc.).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TokenState {
    pub mint_address: String,
    pub version: u64,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub data: Value,
}

impl TokenState {
    pub fn new(mint_address: impl Into<String>) -> Self {
        Self {
            mint_address: mint_address.into(),
            version: 0,
            updated_at: Utc::now(),
            data: Value::Object(Map::new()),
        }
    }

    /// Applies an object-shaped partial update. Nested objects merge recursively;
    /// scalar/array values replace only their own field, so unrelated domains survive.
    pub fn apply_patch(&mut self, patch: Value) -> Result<(), &'static str> {
        if !patch.is_object() {
            return Err("token state patches must be JSON objects");
        }
        merge_json(&mut self.data, patch);
        self.version = self.version.saturating_add(1);
        self.updated_at = Utc::now();
        Ok(())
    }
}

fn merge_json(target: &mut Value, patch: Value) {
    match (target, patch) {
        (Value::Object(target), Value::Object(patch)) => {
            for (key, value) in patch {
                match target.get_mut(&key) {
                    Some(existing) if existing.is_object() && value.is_object() => {
                        merge_json(existing, value);
                    }
                    _ => {
                        target.insert(key, value);
                    }
                }
            }
        }
        (target, patch) => *target = patch,
    }
}

/// Thread-safe in-process registry. The database remains the durable source of
/// truth; this registry provides fast shared state to independent engine workers.
#[derive(Debug, Clone, Default)]
pub struct TokenRegistry {
    records: Arc<RwLock<HashMap<String, TokenState>>>,
}

impl TokenRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, mint: &str) -> Option<TokenState> {
        self.records.read().ok()?.get(mint).cloned()
    }

    pub fn upsert_patch(&self, mint: &str, patch: Value) -> Result<TokenState, &'static str> {
        if mint.trim().is_empty() {
            return Err("mint address cannot be empty");
        }
        let mut records = self
            .records
            .write()
            .map_err(|_| "token registry lock poisoned")?;
        let updated = {
            let state = records
                .entry(mint.to_owned())
                .or_insert_with(|| TokenState::new(mint));
            state.apply_patch(patch)?;
            state.clone()
        };
        evict_oldest_if_over_limit(&mut records, mint);
        Ok(updated)
    }

    pub fn restore(&self, state: TokenState) -> Result<(), &'static str> {
        if state.mint_address.trim().is_empty() {
            return Err("mint address cannot be empty");
        }
        let mut records = self
            .records
            .write()
            .map_err(|_| "token registry lock poisoned")?;
        let replace = records
            .get(&state.mint_address)
            .map(|current| state.version >= current.version)
            .unwrap_or(true);
        if replace {
            let mint = state.mint_address.clone();
            records.insert(mint.clone(), state);
            evict_oldest_if_over_limit(&mut records, &mint);
        }
        Ok(())
    }

    /// Remove a token from the in-memory registry after a terminal rejection.
    pub fn remove(&self, mint: &str) -> Option<TokenState> {
        self.records.write().ok()?.remove(mint)
    }

    pub fn snapshot(&self) -> Vec<TokenState> {
        self.records
            .read()
            .map(|records| records.values().cloned().collect())
            .unwrap_or_default()
    }

    pub fn len(&self) -> usize {
        self.records
            .read()
            .map(|records| records.len())
            .unwrap_or_default()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn patches_merge_without_erasing_other_domains() {
        let registry = TokenRegistry::new();
        registry
            .upsert_patch(
                "mint-1",
                json!({
                    "identity": {"name": "Example", "symbol": "EX"},
                    "market": {"price": 1.0}
                }),
            )
            .unwrap();
        let updated = registry
            .upsert_patch(
                "mint-1",
                json!({
                    "market": {"price": 2.0}
                }),
            )
            .unwrap();

        assert_eq!(updated.version, 2);
        assert_eq!(updated.data["identity"]["name"], "Example");
        assert_eq!(updated.data["market"]["price"], 2.0);
    }

    #[test]
    fn registry_retains_at_most_500_most_recent_tokens() {
        let registry = TokenRegistry::new();
        for index in 0..510 {
            registry
                .restore(TokenState {
                    mint_address: format!("mint-{index}"),
                    version: 1,
                    updated_at: Utc::now() + chrono::Duration::seconds(index),
                    data: json!({"index": index}),
                })
                .unwrap();
        }
        assert_eq!(registry.len(), MAX_TOKEN_STATES);
        assert!(registry.get("mint-0").is_none());
        assert!(registry.get("mint-509").is_some());
    }

    #[test]
    fn rejects_non_object_patch() {
        let mut state = TokenState::new("mint-1");
        assert!(state.apply_patch(json!(["not", "an", "object"])).is_err());
    }

    #[test]
    fn restore_keeps_newer_version() {
        let registry = TokenRegistry::new();
        registry
            .restore(TokenState {
                mint_address: "mint-1".into(),
                version: 8,
                updated_at: Utc::now(),
                data: json!({"market":{"price":8}}),
            })
            .unwrap();
        registry
            .restore(TokenState {
                mint_address: "mint-1".into(),
                version: 7,
                updated_at: Utc::now(),
                data: json!({"market":{"price":7}}),
            })
            .unwrap();
        assert_eq!(registry.get("mint-1").unwrap().version, 8);
    }
}
