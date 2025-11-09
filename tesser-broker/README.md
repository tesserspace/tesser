# tesser-broker

Exchange-agnostic traits that define how the rest of the system talks to brokers.

## Overview
- Declares interfaces like `MarketStream`, `ExecutionClient`, `HistoricalData`, `EventPublisher`, and `SignalBus`.
- Provides the `BrokerError` hierarchy used for consistent error handling across connectors.
- No concrete exchange logic lives here—connectors implement these traits.

## Implementing a Connector
1. Depend on `tesser-broker` and `tesser-core`.
2. Implement the necessary traits (`ExecutionClient`, `MarketStream`, etc.).
3. Map transport or exchange errors into `BrokerErrorKind` so upstream crates can react uniformly.

## Tests
```sh
cargo test -p tesser-broker
```
