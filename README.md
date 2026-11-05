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
- `nodex-solana`: Helius-compatible HTTP RPC helpers, transaction retrieval/decoding, rent/fee quote methods, reconnecting launch logs subscription, and a multiplexed account monitor for Pump.fun bonding curves.
- `nodex-engine`: Axum HTTP/WebSocket API.

## Configure and run

Requires stable Rust.

1. Copy `.env.example` to `.env`.
2. Create a Helius API key and set `HELIUS_API_KEY` in `.env`. Keep the real `.env` file private; it is ignored by Git.
3. Optionally set `RUGCHECK_API_KEY` for RugCheck API access. The backend calls RugCheck's token report summary endpoint; if the provider is unavailable or rate-limited, the report remains explicitly unverified and never becomes safe by default.
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
- `GET /api/v1/tokens/{mint}/scan` — on-demand SPL Token / Token-2022 mint authority and extension scan, enriched with a RugCheck summary when available.
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

RugCheck data is advisory: the summary score and risks are included in each security report, but a provider score alone never marks a token safe or authorizes a paper purchase. Provider failures are represented as unavailable/unverified.

The engine remains paper-only. The launch processor watches Pump.fun logs, fetches each confirmed transaction, decodes create/create_v2 instructions, persists new launches, and registers each launch's bonding-curve PDA with a separate multiplexed account WebSocket. Token-security scans and creator-history scans are spawned as independent asynchronous workers, so slow creator-history analysis does not delay the token-security result or block market-account monitoring. The market monitor validates the account owner and Anchor discriminator, decodes virtual/real token and quote reserves, and emits `market_account_update`, `market_price_update`, and `market_curve_completed` events. SOL-quoted curves are converted using the documented six token decimals and nine SOL decimals; price history is sampled with a bounded per-token buffer and exposes 1-minute/5-minute percentage changes only after sufficient history has accumulated. Legacy SOL curve layouts are supported. Newer non-SOL quote-mint curves and PumpSwap pool state are deliberately not priced yet; they are rejected from the SOL price feed rather than assigned a misleading price. This is market-data intelligence, not a security approval or trade signal. The security scanner still returns `UNVERIFIED` when authority checks pass because holder concentration, full metadata authenticity, liquidity and executable sellability are not yet verified. An active mint or freeze authority is a mandatory-policy `REJECTED` result. Price feeds, complete liquidity analysis, sell simulation, paper order execution, positions, real signing and transaction submission remain incomplete. No funded private key should be configured. Live mode is explicitly rejected by the backend, regardless of UI state. The transaction processor uses a bounded concurrency limit of four and a bounded in-memory duplicate-signature set; provider/decoder/persistence failures are emitted as diagnostic events. Provider connection failures are emitted as `diagnostic` events with a component, message, retry flag and timestamp. Reconnect uses bounded backoff.

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

Creator history now samples up to 12 recent transactions for the decoded Pump.fun creator, resolves available transactions, and counts prior Pump.fun create/create_v2 launches by that same creator address. It emits `creator_history_started` and `creator_history_update` WebSocket events. The sample is partial; linked wallets are not clustered and early creator selling is explicitly `UNVERIFIED` because token-sale attribution is not implemented. Metadata analysis now parses Token-2022 metadata-pointer and embedded TokenMetadata extension fields when present, including update authority, name, symbol and URI. A present update authority is treated as mutable; an absent update authority is treated as immutable for embedded Token-2022 metadata. Standard SPL Metaplex metadata PDA fetching and off-chain URI/image validation remain UNVERIFIED; this stage does not establish authenticity or detect impersonation. The next scanner phases are extension-specific configuration checks, standard SPL metadata fetching, liquidity and LP ownership, then Jupiter sell quotes and transaction simulation. Holder sampling uses `getTokenLargestAccounts` plus `getMultipleAccounts` and reports a clearly labelled partial sample. Then add real market-state tracking and quote/simulation adapters, followed by a fully persisted paper order/position engine. Yellowstone gRPC can be added as a higher-throughput provider adapter after its endpoint and subscription schema are configured. Live execution remains out of scope and disabled until all safeguards and tests exist.


## Market data events

After a newly detected Pump.fun launch has been persisted, NodeX subscribes to its bonding-curve PDA through one shared Solana WebSocket connection (up to 250 account subscriptions in this initial implementation). Each account notification is owner-checked and decoded against the Pump.fun account discriminator and current/legacy field layout.

- `market_account_update` — account address, owner, slot and data size.
- `market_price_update` — SOL price per token, market cap in SOL, virtual/real reserves, creator, completion flag, 1-minute and 5-minute price changes, and history warm-up status.
- `market_curve_completed` — emitted when the curve's `complete` flag is true; this is a graduation/migration hand-off signal, not proof that a PumpSwap pool is already indexed or tradable.
- `market_monitor_status`, `market_subscription_active`, and `diagnostic` — connection/subscription health and decoding failures.

Price changes are based on account-state samples, not a fixed polling loop. The price history is in-memory and resets on process restart or market-stream reconnect. The current price monitor only supports SOL-quoted Pump.fun curves. PumpSwap pool discovery, effective quote-vault reserve handling, persisted price history, stale-feed gates, volume/buy-sell pressure, and strategy triggers are subsequent milestones.
