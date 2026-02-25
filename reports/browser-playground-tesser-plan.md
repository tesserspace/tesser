# Browser Playground → “Powered by Tesser” Plan (Draft)

Date: 2026-02-25

## Goal

Make the browser playground demonstrably use Tesser’s Rust core (compiled to WebAssembly), instead of a parallel JS-only backtest implementation, while keeping the “paste JS strategy” UX.

## Industry-practice defaults (decisions)

1) **No-lookahead execution**
- Strategy observes bar `i` (including `close`) and produces a target weight for bar `i`.
- Orders are executed at **next bar open** (`open[i+1]`), not at `close[i]`.
- Rationale: avoids implicit lookahead that many retail backtests accidentally introduce.

2) **Target weight semantics**
- `targetWeight ∈ [-1, 1]` when shorting enabled, else `[0, 1]`.
- Interpreted as *target exposure vs total equity* (single-asset portfolio):
  - `targetNotional = targetWeight * equity`
  - `targetUnits = targetNotional / price`

3) **Fees & slippage**
- Apply taker-style fee as `feeBps` on traded notional.
- Apply slippage as `slippageBps` on execution price:
  - Buy: `execPrice = open[i+1] * (1 + slip)`
  - Sell: `execPrice = open[i+1] * (1 - slip)`

4) **Last bar behavior**
- Do **not** execute trades that would require `open[i+1]` when `i` is the last bar.

## Architecture (recommended approach)

### A) New crate: `tesser-backtest-core` (pure, sync)
Purpose: the deterministic “math engine” for candle backtests.

Requirements:
- No `tokio`, no file/network IO.
- Reuse existing crates where possible:
  - Types: `tesser-core` (or a small “core types” module if needed for WASM-friendliness).
  - Indicators: `tesser-indicators` (preferred).
  - Portfolio / PnL: reuse `tesser-portfolio` if it can be made WASM/sync-friendly; otherwise implement a minimal single-asset portfolio in this crate and later reconcile.

Public API sketch:
- `run_backtest(candles, options, strategy_driver) -> BacktestResult`
- Deterministic outputs:
  - `equity_curve[]`
  - `trades[]` (time, side, qty, price, fee, targetWeightAfter, realizedPnl)
  - `metrics` (total return, CAGR, max drawdown, Sharpe, fees, trades, etc.)

### B) New crate: `tesser-backtest-wasm` (`wasm32-unknown-unknown`)
Purpose: expose `tesser-backtest-core` to the browser via `wasm-bindgen`.

Binding design:
- JS provides:
  - candle array
  - options
  - a strategy callback `(ctx) => targetWeight | null`
  - optional params JSON
- WASM returns:
  - equity curve / trades / metrics as structured JS objects

Strategy context (keep cross-boundary small):
- `i`, `bar`, `equity`, `positionWeight`
- Provide indicator values computed in Rust (e.g. `sma20`, `sma50`, `rsi14`) so user JS is minimal and consistent.

### C) Docs app integration (Next.js)
Purpose: keep UI/UX, replace calculation engine.

Implementation:
- Run backtest in a **WebWorker**:
  - loads WASM module
  - evaluates user JS strategy (still in worker)
  - calls into WASM backtest core
- UI remains:
  - editor
  - candlestick + markers
  - equity chart
  - metrics table

## Determinism & correctness

### Numeric approach (choose one early)
Option 1 (fastest to ship): `f64` inside core.
- Pros: easiest WASM/JS interop.
- Cons: weaker “correctness” story; harder to guarantee cross-platform stability.

Option 2 (recommended for Tesser positioning): fixed-point or decimal.
- Either:
  - `rust_decimal` internally, convert to/from string at the WASM boundary, or
  - `i128` fixed-point (e.g. 1e8) internally, boundary uses integers.
- Pros: aligns with Tesser “correctness over convenience”.
- Cons: more boilerplate in bindings.

## Tests (must-have to prove “uses Tesser”)

1) Core golden test (native):
- A fixed candle fixture + fixed strategy → stable trades/metrics snapshot.
- Assert a few load-bearing values (final equity, max drawdown, trade count).

2) WASM parity test (CI optional, local ok):
- Run the same fixture through WASM and compare key outputs within tolerance (or exactly if fixed-point).

## Milestones

### M1: Replace JS backtest core with WASM (minimal indicators)
- Implement `tesser-backtest-core` single-asset engine
- Implement `tesser-backtest-wasm` bindings
- Hook docs playground to call worker→WASM
- Acceptance:
  - Same UI, but results originate from WASM
  - Indicators available from Rust (`sma`, `rsi` at least)

### M2: Tighten semantics & alignment
- Formalize fill model (next open) and document in UI
- Add export: generate CLI config skeleton (dataset + params + strategy template)
- Add more metrics and ensure stable naming/definitions

### M3: Strategy execution inside WASM (optional, heavier)
- Embed a JS engine (QuickJS/Boa) into a WASM module
- Reduce JS↔WASM calls; improve sandboxing story

## Open questions (to confirm before implementation)

- Fill model: next open is the default; do we ever offer close-fill mode as a toggle?
- Shorting: keep toggle, or always allow `[-1, 1]` for perp-style demos?
- Which numeric approach (decimal vs fixed vs f64) should be the default for the playground?

