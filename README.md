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
- `nodex-solana`: Helius/FluxRPC-compatible HTTP RPC helpers, transaction retrieval/decoding, rent/fee quote methods, reconnecting launch logs subscription, and a multiplexed account monitor for Pump.fun bonding curves.
- `nodex-engine`: Axum HTTP/WebSocket API.

## Configure and run

Requires stable Rust.

1. Copy `.env.example` to `.env`.
2. Helius remains supported as the fallback provider. Configure up to 20 Helius API keys using `HELIUS_API_KEY` through `HELIUS_API_KEY20`. When no FluxRPC keys are configured, NodeX uses the Helius HTTP and WebSocket pools. HTTP requests rotate across configured endpoints and retry another key on transient failures; WebSocket workers rotate endpoints on reconnect. Keep real keys private; `.env` is ignored by Git.
3. Add up to 10 FluxRPC keys using `FLUXRPC_API_KEY` through `FLUXRPC_API_KEY10`. When at least one FluxRPC key is configured, FluxRPC is selected as the active Solana provider for the entire engine: Pump.fun launch `logsSubscribe`, confirmed `getTransaction` decoding, token/account HTTP reads and security scans, PumpSwap activity `logsSubscribe`, and Pump.fun/PumpSwap account `accountSubscribe` market monitoring. NodeX derives HTTP JSON-RPC and WebSocket endpoints from `FLUXRPC_REGION` (`eu` by default). HTTP requests rotate across configured FluxRPC endpoints and retry another key on transient failures; persistent WebSocket workers move to another configured endpoint when reconnecting. Helius endpoints are not added to the active pool while FluxRPC keys are present, so exhausted Helius keys remain idle. Remove the FluxRPC keys and restart to return to the Helius pool. Set `FLUXRPC_MAX_CONCURRENT_REQUESTS` to bound FluxRPC HTTP concurrency. FluxRPC account keys share the owning account's bandwidth quota; extra keys do not multiply it.
4. Configure up to 10 RugCheck API keys using `RUGCHECK_API_KEY` through `RUGCHECK_API_KEY10`. The backend rotates the starting key between token scans and retries transient HTTP/auth/rate-limit/server failures with other configured keys. It requests the full `/v1/tokens/{mint}/report` response and preserves the complete report (including token metadata, authorities, top holders, markets/lockers, creator-token signals and insider networks) in the security report. Set `RUGCHECK_REFRESH_REPORTS=true` only when your RugCheck plan supports paid fresh-report regeneration; it defaults to cached reports. Multiple keys are credential rotation/failover, not extra account quota: rate limits and plan capacity still apply at the account level. If the provider is unavailable or rate-limited, the report remains explicitly unverified and never becomes safe by default.
5. Run:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo run -p nodex-engine
```

The engine listens on `http://127.0.0.1:8080` by default.

## Current endpoints

- `GET /health` — health status.
- `GET /api/v1/engine/status`
- `GET /api/v1/engine/control` — current runtime state and independent discovery/entry/position-monitoring switches.
- `POST /api/v1/engine/control` — operator-authenticated `start`, `resume`, `pause`, or `stop` command; accepts optional `token_discovery_enabled`, `new_entries_enabled`, and `position_monitoring_enabled` booleans.
- `GET /api/v1/config/trading` and `PUT /api/v1/config/trading` — read/update persisted paper strategy settings; updates require `x-nodex-admin-key`.
- `PATCH /api/v1/config/trading/execution` — update paper-execution controls.
- `GET /api/v1/creators/intelligence` — creator intelligence summary.
- `GET /api/v1/creators/reputation?limit=100` — ranked persisted creator reputation records (limit 1–500).
- `GET /api/v1/creators/{creator_address}/training` — one creator's tracked launches and training data.
- `GET /api/v1/training/tokens/{mint}/observations` — persisted observations for one token.
- `GET /api/v1/portfolio/balance` — paper portfolio balance and P&L.
- `GET /api/v1/portfolio/positions/{position_id}/risk` and `PUT` — read/update an open position's stop/trailing policy.
- `POST /api/v1/portfolio/positions/{position_id}/sell` — manual simulated paper close.
- `GET /api/v1/admin/storage/overview` — operator-authenticated filesystem, SQLite table payload, database/WAL/SHM and systemd journal measurements; requires `x-nodex-admin-key`.
- `POST /api/v1/admin/storage/system-journal/cleanup` — operator-authenticated, opt-in system journal rotation and vacuum to a 200 MB target. Requires `NODEX_ALLOW_SYSTEM_JOURNAL_CLEANUP=true` and sufficient OS permissions for the NodeX service.
- `GET /api/v1/paper-wallet` — persisted paper wallet snapshot.
- `GET /api/v1/tokens/recent?limit=100` — most recently persisted decoded Pump.fun launches (limit is clamped to 1–500).
- `GET /api/v1/tokens/{mint}/scan` — on-demand SPL Token / Token-2022 mint authority and extension scan, enriched with a RugCheck summary when available.
- `GET /api/v1/fees/token-account-rent` — live rent-exempt minimum for a standard 165-byte SPL token account.
- `WS /api/v1/ws` — upstream logs, provider status, diagnostics, and engine control messages.

### Runtime configuration

The first startup creates a persisted `trading` configuration record when none exists. Current defaults include a 15% stop loss, 100% fixed take-profit, $3,000 minimum liquidity, $15 USD paper position size, 3 maximum open positions, 80 minimum scanner safety score, 10% maximum slippage, $1,000 minimum 5-minute buy volume, and 20% maximum top-10 concentration. Automatic paper buying and simulated execution are enabled in settings by default, but the engine itself starts paused; the independent engine entry switch must also be enabled. Known-rug-creator blacklisting defaults to on. The trusted-creator fast-entry threshold is 50/100. Trailing stop defaults to off, with a 20% activation threshold and 12% callback when enabled. Settings are validated and persisted in SQLite.

Mutating configuration and HTTP engine controls require the `x-nodex-admin-key` header. Do not embed this operator key in a distributed mobile app. Live/real trading remains unavailable and requests to switch away from paper mode are rejected.

The process starts paused. Start/Resume opens the configured provider streams and enables launch discovery; Pause/Stop pauses discovery. Discovery, new paper entries, and position monitoring have independent runtime switches. The paper-entry worker requires all applicable runtime/settings gates, a fresh explicit scanner approval, and a live SOL/USD quote. It creates simulated paper positions and persists the wallet ledger and position atomically. Position monitoring updates valuations and evaluates stop-loss, fixed take-profit or trailing-stop exits; closes are simulated and settled against the paper wallet. The engine re-checks pause/new-entry controls while holding settlement locks so a pause cannot race through and open a position.

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

The engine remains paper-only: it has no wallet signer, private-key management, live transaction builder or transaction submission path. The launch processor watches Pump.fun logs, fetches confirmed transactions, decodes create/create_v2 instructions, persists launches and extracts the creator address. The token-security worker and creator-history/training work run independently so creator-history latency does not hold up the security result or market subscription. Paper entry requires an explicit, fresh `APPROVED` scanner result with mandatory core checks; diagnostic-only, missing, stale or rejected approvals cannot authorize entry. RugCheck/provider failures remain unverified rather than being treated as safe. The market monitor validates account ownership and Anchor discriminators, decodes Pump.fun curve reserves and emits market-account, price and curve-completion events. Current curve-price monitoring supports SOL-quoted Pump.fun curves; unsupported non-SOL quote curves are not assigned fabricated SOL prices. PumpSwap support and additional pool layouts remain separate follow-up work. The transaction processor uses bounded concurrency and a bounded duplicate-signature set; provider, decoder and persistence failures are surfaced as diagnostic events, and reconnect uses bounded backoff. Live mode is rejected by the backend regardless of UI state. Do not configure a funded wallet or private key.

## Paper wallet and position management

- A new paper wallet is seeded with $10 USD worth of SOL using the startup SOL/USD quote; existing funded paper ledgers are preserved.
- Default paper position sizing is $15 USD per token (settings can be changed through the authenticated trading-config API).
- Wallet arithmetic uses integer lamports. Token-account rent is tracked separately as locked/reclaimable capital; network, priority and venue fees have separate accounting fields.
- Paper buys reserve balance and persist the ledger and position atomically. Manual and automatic exits are simulated; no signed transaction is sent to Solana.
- Each position stores its own immutable risk-policy snapshot. Hard stop-loss remains active. Fixed take-profit applies when trailing mode is off. When trailing mode is enabled, the stop activates at the configured profit threshold and follows the position's own high-water mark using its callback percentage; an optional maximum-profit cap can close it earlier.
- The global trailing-stop setting is captured for newly opened positions. Existing positions retain their saved policy and can be changed individually through the position risk endpoint.
- Current defaults: 15% hard stop-loss, 100% fixed take-profit, trailing disabled, 20% trailing activation, 12% callback, and maximum-profit cap disabled (0).

The paper wallet, trading policy, creator reputation, position exits and API paths have unit tests; run the workspace commands above before deploying.

## Scanner design references

The scanner design follows publicly documented controls used in established Solana sniper products: Trojan documents mint/freeze authority filters, developer wallet allow/deny lists, liquidity ranges and developer-holding limits; BONKbot emphasizes mint/freeze authority, LP control and token distribution; Jupiter documents quote routes and price impact as evidence for executable swap paths. These product filters are design references, not proof that NodeX has those checks implemented. NodeX records evidence per check and fails closed when evidence is unavailable.

## Creator training and trusted-creator fast entry

Every decoded Pump.fun launch records the exact creator wallet address and mint relationship in SQLite. Market observations are persisted against each tracked token, deduplicated by observation key, and used by the lifecycle evaluator and creator-reputation evaluator. Reputation aggregates the creator's tracked launches and observed tokens using longevity, growth, graduation, buyer/volume activity, stability and risk evidence. The record includes an overall score, confidence percentage, sample status, launch count, observed-token count and graduated-token count. Creator addresses are matched exactly; the entry worker loads the persisted record for the token's extracted creator address rather than relying on a short recent-transaction sample.

A creator qualifies for the trusted fast path only when the persisted record is `established`, confidence is at least 50%, and overall score is at least 50/100. The score is passed into entry evaluation only after those sample-quality checks. After a fresh explicit scanner approval and normal runtime/paper-execution controls, a qualifying creator bypasses the ordinary observation delay and additional market-confirmation waits for liquidity, holder samples, market cap, volume, buyer counts, buy pressure, slippage and candle movement. The path still requires a valid live token price, a valid live SOL/USD quote, any configured maximum-entry-price limit, no known-rug blacklist hit, available position capacity, a valid paper position size and normal duplicate-position/settlement checks. This is a faster simulated paper entry, not a promise of being the first on-chain buyer. Unknown or developing creators continue through the normal confirmation strategy.

The reputation API exposes ranked creator records, each creator's tracked launches and per-token training observations. Creator training is evidence-driven; unavailable market data is kept unknown rather than converted into a positive score. Creator wallet clustering and reliable attribution of early creator selling are not implemented, so linked wallets must not be assumed to belong to the same person.


## Market data events

After a newly detected Pump.fun launch has been persisted, NodeX subscribes to its bonding-curve PDA through one shared Solana WebSocket connection (up to 250 account subscriptions in this initial implementation). Each account notification is owner-checked and decoded against the Pump.fun account discriminator and current/legacy field layout.

- `market_account_update` — account address, owner, slot and data size.
- `market_price_update` — SOL price per token, market cap in SOL, virtual/real reserves, creator, completion flag, 1-minute and 5-minute price changes, and history warm-up status.
- `market_curve_completed` — emitted when the curve's `complete` flag is true; this is a graduation/migration hand-off signal, not proof that a PumpSwap pool is already indexed or tradable.
- `market_monitor_status`, `market_subscription_active`, and `diagnostic` — connection/subscription health and decoding failures.

Price changes are based on account-state samples, not a fixed polling loop. The price history is in-memory and resets on process restart or market-stream reconnect. The current curve-price monitor supports SOL-quoted Pump.fun curves; PumpSwap pool pricing and unsupported quote-mint layouts remain outside this feed. Confirmed market trade observations are also persisted for creator training and ordinary entry confirmation. The trusted-creator fast path uses the stored creator record after scanner approval and does not wait for the ordinary buyer/volume confirmation thresholds.

### Storage management API

Set `NODEX_ADMIN_API_KEY` to a long random secret in the server environment. The storage overview endpoint reports the root filesystem via `df`, logical payload estimates for token, launch, raw-event, chart and paper-trading tables, physical SQLite database/WAL/SHM file sizes, and the systemd journal's reported total size.

SQLite category payload estimates overlap the physical database size and must not be summed with it. Launch and discovery figures refer to the same launch-history rows. NodeX currently writes tracing output to stdout, so systemd captures it in journald; journald does not provide a reliable per-service disk-size breakdown. The journal size is therefore reported as a system-wide value, not falsely attributed to NodeX alone.

System journal cleanup is disabled by default. Only enable `NODEX_ALLOW_SYSTEM_JOURNAL_CLEANUP=true` after reviewing the system-wide effect. The endpoint rotates the journal and vacuums archived journal data toward 200 MB, subject to the service's operating-system permissions. It does not delete SQLite data, token records, wallet data, or trading history. The engine process is not granted unrestricted root access.


## Live SOL price and USD paper portfolio

The engine fetches a live SOL/USD spot quote from Coinbase, with Binance as a
fallback, and refreshes it every 10 seconds. The quote is broadcast as
`sol_price_update`. If both providers fail, the last quote is retained and
marked `stale`; no synthetic price is substituted.

A new paper wallet starts with **$10 USD worth of SOL**, converted to lamports
using the startup quote. An existing wallet is reset to this seed only when it
has no paper ledger entries, no positions, and no invested or locked balances.
The default per-token paper sizing is reduced to 0.01 SOL for the small seed.

- `GET /api/v1/market/sol-price` — latest SOL/USD quote, source, timestamp and freshness.
- `GET /api/v1/portfolio/balance` — aggregate USD portfolio balance, including
  available paper SOL, open token holdings and recoverable rent.
- `GET /api/v1/profile` — includes `portfolio_balance_usd`, USD PnL fields,
  per-position USD marks and the SOL quote used for valuation.
