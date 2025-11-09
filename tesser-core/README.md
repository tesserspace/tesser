# tesser-core

Foundational data types shared across the entire workspace.

## Overview
- Defines `Candle`, `Tick`, `Order`, `Fill`, `Position`, `Signal`, and supporting enums like `Side`, `OrderType`, `Interval`, etc.
- Contains no exchange-specific code; zero or minimal dependencies so every crate can rely on it.
- Provides helper methods (`Interval::as_duration`, `Position::mark_price`, etc.) used in both backtests and live trading.

## When to Touch This Crate
- Adding/adjusting core types that must be shared everywhere.
- Extending enums to support new order styles or intervals.
- Never include exchange-specific logic—put that in connectors.

## Tests
Run the focused test suite every time you change types:
```sh
cargo test -p tesser-core
```
