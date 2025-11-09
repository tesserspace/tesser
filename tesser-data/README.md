# tesser-data

Data ingestion utilities used by both the backtester and live trading.

## Overview
- Defines `DataDistributor`, which pumps events from any `MarketStream` implementation into user-defined handlers.
- Houses downloader helpers (e.g., Bybit REST kline fetcher) consumed by `tesser-cli data` commands.
- Keeps data-path logic separate from execution/strategy code.

## Extending
- Implement new downloaders under `download/` for additional exchanges or datasets.
- Provide new handler traits if you need to fan out to other sinks (DB writers, feature stores, etc.).

## Tests
```sh
cargo test -p tesser-data
```
