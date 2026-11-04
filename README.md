# NodeX Engine

NodeX is a modular Solana launch-analysis and paper-trading engine. The current implementation adds a persistent paper-wallet foundation, a Pump.fun program log stream, confirmed transaction retrieval, and instruction decoding for Pump.fun create and create_v2 launches.

## Safety status

- Paper mode is the only supported mode.
- No wallet signer, private key handling, transaction builder, or live transaction submission exists.
- New Pump.fun create and create_v2 instructions are decoded from confirmed transaction JSON and newly discovered launches are persisted before being broadcast.
- Transaction decoding supports legacy, version-0 and version-1 responses where the RPC provider supports them.
- Yellowstone gRPC is not integrated yet; the current live feed uses standard Solana WebSocket logsSubscribe plus HTTP getTransaction.
- Do not connect a funded wallet to this milestone.

## Workspace

- `nodex-core`: shared models and environment configuration.
- `launch-detector`: launch classification boundary for decoded instruction facts.
- `nodex-persistence`: SQLite storage for launches, paper wallet snapshots, ledger entries and raw Solana events.
- `nodex-trading`: integer-lamport paper wallet accounting and strategy foundation.
- `nodex-solana`: Helius-compatible HTTP RPC helpers, transaction retrieval/decoding, rent/fee quote methods and reconnecting WebSocket logs subscription.
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
- `GET /api/v1/tokens/recent?limit=100` — most recently persisted decoded Pump.fun launches (limit is clamped to 1–500).
- `GET /api/v1/tokens/{mint}/scan` — on-demand SPL Token / Token-2022 mint authority and extension scan.
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

The engine remains paper-only. The launch processor watches Pump.fun logs, fetches each confirmed transaction, decodes the documented create/create_v2 instruction discriminator and Borsh creator field, then persists and emits `launch_detected` and `token_scan_started`. This is launch identification, not a security approval: the first security-scanner stage now inspects mint/freeze authorities and detects Token-2022 extension types. It deliberately returns `UNVERIFIED` when authority checks pass because holder concentration, creator history, metadata, liquidity and executable sellability are not yet verified. An active mint or freeze authority is a mandatory-policy `REJECTED` result. No token can be `APPROVED` from this partial scanner. Prices, full liquidity analysis, sell simulation, paper order execution, positions, real signing and transaction submission are not implemented. No funded private key should be configured. Live mode is explicitly rejected by the backend, regardless of UI state. The transaction processor uses a bounded concurrency limit of four and a bounded in-memory duplicate-signature set; provider/decoder/persistence failures are emitted as diagnostic events. Do not configure a funded private key. Live mode is explicitly rejected by the backend, regardless of UI state. Provider connection failures are emitted as `diagnostic` events with a component, message, retry flag and timestamp. Reconnect uses bounded backoff.

## Paper wallet defaults

- Starting balance: 1 SOL (1,000,000,000 lamports) by default. Existing database wallet balances are preserved; this setting only applies when the paper wallet is first created.
- Default strategy trade size: 0.1 SOL (100,000,000 lamports).
- All wallet arithmetic uses integer lamports.
- Token-account rent is tracked separately as locked/reclaimable capital.
- Network, priority and venue swap fees are separate cost fields.
- A fee quote for a specific compiled message uses Solana's getFeeForMessage RPC method; no message is signed or submitted.

The paper-wallet module has unit tests for default balances, cost aggregation, insufficient funds and rent refunds.

## Scanner design references

The scanner design follows publicly documented controls used in established Solana sniper products: Trojan documents mint/freeze authority filters, developer wallet allow/deny lists, liquidity ranges and developer-holding limits; BONKbot emphasizes mint/freeze authority, LP control and token distribution; Jupiter documents quote routes and price impact as evidence for executable swap paths. These product filters are design references, not proof that NodeX has those checks implemented. NodeX records evidence per check and fails closed when evidence is unavailable.

## Important next work

The next scanner phases are Token-2022 extension-specific configuration checks, top-holder and creator-wallet analysis, metadata authority/mutability, liquidity and LP ownership, then Jupiter sell quotes and transaction simulation. Then add real market-state tracking and quote/simulation adapters, followed by a fully persisted paper order/position engine. Yellowstone gRPC can be added as a higher-throughput provider adapter after its endpoint and subscription schema are configured. Live execution remains out of scope and disabled until all safeguards and tests exist.
