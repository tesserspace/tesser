# tesser-backtester

Event-driven simulation engine that replays historical data through strategies and the execution stack.

## Overview
- Wires `Strategy`, `ExecutionEngine`, `Portfolio`, and `tesser-paper` into a deterministic loop.
- Supports configurable history length, latency (in candles), slippage, and fees via `BacktestConfig`.
- Produces `BacktestReport` summaries (signals emitted, orders sent, equity, dropped orders).

## Usage
Most users drive the backtester through `tesser-cli backtest run`, but you can embed it yourself:
```rust
let cfg = BacktestConfig::new(symbol.clone(), candles);
let report = Backtester::new(cfg, strategy, execution, None).run().await?;
```

## Tests
```sh
cargo test -p tesser-backtester
```
