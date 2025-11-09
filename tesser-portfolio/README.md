# tesser-portfolio

Portfolio accounting primitives: tracks cash, positions, and realized/unrealized PnL.

## Overview
- `Portfolio` applies fills, updates entries/exits, and produces equity snapshots.
- Supports serialization via `PortfolioState` so live trading can persist and reload.
- Includes `PortfolioConfig` for setting initial equity and future risk parameters.

## Typical Usage
- Backtester and live runtime call `apply_fill` whenever orders settle.
- Strategies can inspect positions through `Portfolio::positions()` via the `StrategyContext`.
- Live trading snapshots the state with `portfolio.snapshot()` and restores with `Portfolio::from_state`.

## Tests
```sh
cargo test -p tesser-portfolio
```
