# NodeX Engine

NodeX is an intelligent Solana launch-analysis and trading engine. The initial milestone is a **paper-only** Rust foundation. Pump.fun is the first launchpad target; protocol adapters are kept separate so additional launchpads can be added without coupling strategy logic to one venue.

## Safety status

- Paper mode is the only supported mode in this milestone.
- No wallet signer, private key handling, transaction builder, or live transaction submission is included.
- Do not connect a funded wallet to this milestone.
- Detection is currently a decoded-instruction boundary. A Yellowstone/RPC adapter and verified Pump.fun instruction decoder are the next integration step; this code does not yet stream mainnet launches.

## Workspace

- `nodex-core`: shared models, events and configuration.
- `launch-detector`: launch classification from decoded instruction facts.
- `nodex-persistence`: SQLite persistence with duplicate-signature protection.
- `nodex-engine`: Axum health API and application startup.

## Run

Requires a current stable Rust toolchain.

```bash
cp .env.example .env
cargo test --workspace
cargo run -p nodex-engine
```

Then check `GET http://127.0.0.1:8080/health`.

## Current milestone

Phase 1 foundation: workspace, typed launch model, configurable Pump.fun detector boundary, SQLite launch store, paper-only API startup and unit tests.

Next: implement and test the actual Solana transaction/account stream adapter, decode verified Pump.fun create instructions, persist detected launches from the stream, then add token-intelligence checks. Live execution must remain disabled until paper results and safety controls are independently validated.
