use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{collections::HashMap, sync::{Arc, RwLock}};

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
                    _ => { target.insert(key, value); }
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
    pub fn new() -> Self { Self::default() }

    pub fn get(&self, mint: &str) -> Option<TokenState> {
        self.records.read().ok()?.get(mint).cloned()
    }

    pub fn upsert_patch(&self, mint: &str, patch: Value) -> Result<TokenState, &'static str> {
        if mint.trim().is_empty() { return Err("mint address cannot be empty"); }
        let mut records = self.records.write().map_err(|_| "token registry lock poisoned")?;
        let state = records.entry(mint.to_owned())
            .or_insert_with(|| TokenState::new(mint));
        state.apply_patch(patch)?;
        Ok(state.clone())
    }

    pub fn restore(&self, state: TokenState) -> Result<(), &'static str> {
        if state.mint_address.trim().is_empty() { return Err("mint address cannot be empty"); }
        let mut records = self.records.write().map_err(|_| "token registry lock poisoned")?;
        let replace = records.get(&state.mint_address)
            .map(|current| state.version >= current.version)
            .unwrap_or(true);
        if replace { records.insert(state.mint_address.clone(), state); }
        Ok(())
    }

    pub fn snapshot(&self) -> Vec<TokenState> {
        self.records.read().map(|records| records.values().cloned().collect()).unwrap_or_default()
    }

    pub fn len(&self) -> usize {
        self.records.read().map(|records| records.len()).unwrap_or_default()
    }

    pub fn is_empty(&self) -> bool { self.len() == 0 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn patches_merge_without_erasing_other_domains() {
        let registry = TokenRegistry::new();
        registry.upsert_patch("mint-1", json!({
            "identity": {"name": "Example", "symbol": "EX"},
            "market": {"price": 1.0}
        })).unwrap();
        let updated = registry.upsert_patch("mint-1", json!({
            "market": {"price": 2.0}
        })).unwrap();

        assert_eq!(updated.version, 2);
        assert_eq!(updated.data["identity"]["name"], "Example");
        assert_eq!(updated.data["market"]["price"], 2.0);
    }

    #[test]
    fn rejects_non_object_patch() {
        let mut state = TokenState::new("mint-1");
        assert!(state.apply_patch(json!(["not", "an", "object"])).is_err());
    }

    #[test]
    fn restore_keeps_newer_version() {
        let registry = TokenRegistry::new();
        registry.restore(TokenState {
            mint_address: "mint-1".into(),
            version: 8,
            updated_at: Utc::now(),
            data: json!({"market":{"price":8}}),
        }).unwrap();
        registry.restore(TokenState {
            mint_address: "mint-1".into(),
            version: 7,
            updated_at: Utc::now(),
            data: json!({"market":{"price":7}}),
        }).unwrap();
        assert_eq!(registry.get("mint-1").unwrap().version, 8);
    }
}
