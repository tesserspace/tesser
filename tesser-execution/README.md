# tesser-execution

Order management helpers that translate signals into broker requests.

## Overview
- Provides the `OrderSizer` trait plus a simple `FixedOrderSizer` implementation.
- `ExecutionEngine` consumes `Signal`s, computes quantities, and calls the configured `ExecutionClient`.
- Central place to add risk/remediation logic before orders hit the exchange connector.

## Extending
- Implement new sizers (e.g., Kelly, volatility scaling) by satisfying `OrderSizer`.
- Add pre-trade checks or risk guards inside `ExecutionEngine::handle_signal` before delegating to the client.

## Tests
```sh
cargo test -p tesser-execution
```
