# tesser-strategy

Reference implementations of the `Strategy` trait plus shared strategy utilities.

## Overview
- Defines the `Strategy` trait and `StrategyContext`, which delivers recent candles/ticks/order books and positions.
- Ships several built-in strategies (`SmaCross`, `RsiReversion`, `BollingerBreakout`, `MlClassifier`, etc.) for testing and demos.
- Includes helpers for indicator calculations and signal emission.

## Adding a Strategy
1. Implement the `Strategy` trait in a new module.
2. Add a `register_strategy!(YourType, "YourType")` invocation (optionally with aliases) so the registry discovers it automatically.
3. Provide a TOML config schema in `research/strategies/` for easy CLI usage.

## Tests
```sh
cargo test -p tesser-strategy
```
