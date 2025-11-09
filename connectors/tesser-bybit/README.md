# tesser-bybit

Bybit v5 connector providing both REST execution and WebSocket market data.

## Features
- `BybitClient`: signed REST client implementing `ExecutionClient` (place/cancel orders, list open orders, fetch balances/positions).
- `BybitMarketStream`: resilient public WebSocket stream implementing `MarketStream` with support for klines and public trades, automatic heartbeats, and graceful shutdown.
- `PublicChannel`/`BybitSubscription` helpers for selecting categories (`linear`, `inverse`, `spot`, etc.) and topics.

## Configuration
Point `config/[env].toml` exchange profiles at the correct REST/WS endpoints and supply API credentials if you want to use the REST client.

## Tests
The crate currently provides unit tests for request signing. You can run the full suite with:
```sh
cargo test -p tesser-bybit
```
