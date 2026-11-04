# NodeX Engine

NodeX is a modular Solana launch-analysis and paper-trading engine. The current implementation adds a persistent paper-wallet foundation and a raw Pump.fun program log stream using a Helius-compatible Solana RPC endpoint.

## Safety status

- Paper mode is the only supported mode.
- No wallet signer, private key handling, transaction builder, or live transaction submission exists.
- The upstream stream only forwards raw Solana logs notifications. It does not yet decode Pump.fun create/buy/sell instructions or derive token prices.
- Do not connect a funded wallet to this milestone.

## Workspace

- `nodex-core`: shared models and environment configuration.
- `launch-detector`: launch classification boundary for decoded instruction facts.
- `nodex-persistence`: SQLite storage for launches, paper wallet snapshots, ledger entries and raw Solana events.
- `nodex-trading`: integer-lamport paper wallet accounting and strategy foundation.
- `nodex-solana`: Helius-compatible HTTP RPC helpers, rent/fee quote methods and reconnecting WebSocket logs subscription.
- `nodex-engine`: Axum HTTP/WebSocket API.

## Configure and run

Requires stable Rust.

1. Copy `.env.example` to `.env`.
2. Create a Helius API key and set `HELIUS_API_KEY` in `.env`. Keep the real `.env` file private; it is ignored by Git.
3. Run:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo run -p nodex-engine
```

The engine listens on `http://127.0.0.1:8080` by default.

## Current endpoints

- `GET /health` — health status.
- `GET /api/v1/engine/status` — paper mode and upstream stream status.
- `GET /api/v1/paper-wallet` — persisted paper wallet snapshot.
- `GET /api/v1/fees/token-account-rent` — live rent-exempt minimum for a standard 165-byte SPL token account.
- `WS /api/v1/ws` — upstream logs, provider status, diagnostics, and engine control messages.

### WebSocket commands

Send JSON text messages to `/api/v1/ws`:

```json
{"command":"pause"}
{"command":"resume"}
{"command":"start"}
{"command":"status"}
{"command":"set_mode","mode":"paper"}
```

The server acknowledges state changes with `engine_state`. A request for `set_mode` to anything other than `paper` returns `command_rejected`. The backend does not accept a frontend-only live-trading toggle.

### Safety and current implementation boundary

The engine remains paper-only. Token discovery currently forwards raw Pump.fun program log notifications; it does not yet decode verified create instructions or perform token risk analysis. No real signing, swap construction, transaction submission, position tracking, or automatic sell path is implemented. Do not configure a funded private key. Live mode is explicitly rejected by the backend, regardless of UI state. Provider connection failures are emitted as `diagnostic` events with a component, message, retry flag and timestamp. Reconnect uses bounded backoff.

## Paper wallet defaults

- Starting balance: 1 SOL (1,000,000,000 lamports) by default. Existing database wallet balances are preserved; this setting only applies when the paper wallet is first created.
- Default strategy trade size: 0.1 SOL (100,000,000 lamports).
- All wallet arithmetic uses integer lamports.
- Token-account rent is tracked separately as locked/reclaimable capital.
- Network, priority and venue swap fees are separate cost fields.
- A fee quote for a specific compiled message uses Solana's getFeeForMessage RPC method; no message is signed or submitted.

The paper-wallet module has unit tests for default balances, cost aggregation, insufficient funds and rent refunds.

## Important next work

The WebSocket currently subscribes to Pump.fun program logs and persists the raw JSON notifications. Next implementation work is transaction retrieval and verified instruction decoding, launch persistence from decoded events, bonding-curve/PumpSwap state tracking and executable-price calculations. Paper order execution and ledger posting must then be connected to strategy decisions. Live execution remains out of scope and disabled.
