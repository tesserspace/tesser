# tesser-paper

Deterministic in-memory connector used for backtests and paper trading.

## Components
- `PaperExecutionClient`: fills orders immediately at the requested price (or last trade) and tracks mock balances/positions.
- `PaperMarketStream`: replays preloaded ticks/candles through the `MarketStream` interface.

## Use Cases
- Backtester wires strategies to this client to simulate fills.
- `tesser-cli live run` defaults to paper execution so you can validate real-time signals before hitting a real exchange.

## Tests
```sh
cargo test -p tesser-paper
```
