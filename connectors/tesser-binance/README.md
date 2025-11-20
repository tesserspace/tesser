# tesser-binance

Binance USD-M futures connector for the Tesser framework. This crate wraps the
[`binance-sdk`](https://crates.io/crates/binance-sdk) client to expose Tesser's
`ExecutionClient` and `MarketStream` traits.

The implementation focuses on perpetual linear contracts, providing:

- Signed REST endpoints (`place_order`, `cancel_order`, balances, positions, instruments)
- Public WebSocket subscriptions for trades, klines, and partial order books
- User data WebSocket stream that forwards order updates and trade executions

Refer to the main Tesser documentation for usage instructions.
