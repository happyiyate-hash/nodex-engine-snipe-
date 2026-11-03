use nodex_core::{LaunchSource, TokenLaunch};

/// Minimal decoded instruction facts supplied by a Solana transaction adapter.
/// Keeping protocol decoding separate makes it possible to add launchpads later.
#[derive(Debug, Clone)]
pub struct CreateInstruction {
    pub program_id: String,
    pub configured_pump_fun_program_id: String,
    pub mint_address: String,
    pub creator_address: Option<String>,
    pub signature: String,
    pub slot: u64,
}

pub fn detect_pump_fun_launch(ix: CreateInstruction) -> Option<TokenLaunch> {
    if ix.program_id != ix.configured_pump_fun_program_id
        || ix.mint_address.trim().is_empty()
        || ix.signature.trim().is_empty()
    {
        return None;
    }

    Some(TokenLaunch::new(
        ix.mint_address,
        ix.creator_address,
        LaunchSource::PumpFun,
        ix.signature,
        ix.slot,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(program_id: &str) -> CreateInstruction {
        CreateInstruction {
            program_id: program_id.into(),
            configured_pump_fun_program_id: "configured-pump-program".into(),
            mint_address: "mint-address".into(),
            creator_address: Some("creator-address".into()),
            signature: "transaction-signature".into(),
            slot: 42,
        }
    }

    #[test]
    fn accepts_configured_pump_program() {
        assert!(detect_pump_fun_launch(sample("configured-pump-program")).is_some());
    }

    #[test]
    fn rejects_other_programs() {
        assert!(detect_pump_fun_launch(sample("unrelated-program")).is_none());
    }
}
