# tesser-test-utils

`tesser-test-utils` provides a programmable Bybit v5 mock exchange that can be used to drive
end-to-end tests for the Tesser runtime. It spins up lightweight REST and WebSocket servers that
emulate the portions of the API exercised by `tesser-cli live run`, allowing strategies, execution
clients, and portfolio persistence to be verified without touching the real exchange.

## Features

- **Stateful mock exchange** &mdash; Seed accounts with balances and positions, inspect fills and
  open orders, and mutate shared state from tests via `MockExchangeState`.
- **Bybit-compatible REST** &mdash; Implements `/v5/order/create`, `/v5/order/cancel`,
  `/v5/position/list`, `/v5/account/wallet-balance`, `/v5/execution/list`, and `/v5/order/realtime`
  with header-based HMAC verification identical to the real API.
- **Scriptable WebSocket streams** &mdash; Public subscriptions stream canned trades, candles, and
  order books, while the private stream echoes order/execution events pushed via the shared state.
- **Scenario engine** &mdash; Queue deterministic behaviors (delays, failures, staged fills, custom
  private events) that are consumed when REST calls or WebSocket events fire.

## Quick start

```rust
use anyhow::Result;
use chrono::Utc;
use rust_decimal::Decimal;
use tesser_core::{AccountBalance, Interval};
use tesser_test_utils::{
    AccountConfig, MockExchange, MockExchangeConfig, OrderFillStep, Scenario, ScenarioAction,
    ScenarioTrigger,
};

#[tokio::test]
async fn runs_with_mock_exchange() -> Result<()> {
    let account = AccountConfig::new("test-key", "test-secret").with_balance(AccountBalance {
        currency: "USDT".into(),
        total: Decimal::new(10_000, 0),
        available: Decimal::new(10_000, 0),
        updated_at: Utc::now(),
    });
    let exchange = MockExchange::start(
        MockExchangeConfig::new()
            .with_account(account)
            .with_candles(vec![])
            .with_ticks(vec![]),
    )
    .await?;

    // Script the next order placement to fill immediately.
    let scenarios = exchange.state().scenarios();
    scenarios
        .push(Scenario {
            name: "instant-fill".into(),
            trigger: ScenarioTrigger::OrderCreate,
            action: ScenarioAction::FillPlan {
                steps: vec![OrderFillStep {
                    after: std::time::Duration::from_millis(10),
                    quantity: Decimal::ONE,
                    price: Some(Decimal::new(1000, 0)),
                }],
            },
        })
        .await;

    // Point `tesser-cli` or a Bybit execution client at these URLs.
    println!("REST at {}", exchange.rest_url());
    println!("WebSocket at {}", exchange.ws_url());

    Ok(())
}
```

In integration tests you can combine the mock with `tesser_cli::live::run_live_with_shutdown` to
execute strategies end-to-end. Use the `ScenarioManager` to queue how the mock should respond to
order placements (delays, failures, custom JSON payloads, or multi-step fills). You can also query
the shared state after the run to assert on balances, positions, or order history without touching
SQLite.

## When to use it

- Exercising `tesser-cli live run` control-plane logic (bootstrap, reconciliation, signal routing).
- Verifying new execution algorithms or strategy behaviors with deterministic fills.
- Reproducing edge cases such as WebSocket drops, reconciliation mismatches, or staged fills.

Because the mock exchange runs entirely inside the test process, it is fast enough to run in CI and
does not require external secrets or network connectivity. Add `tesser-test-utils` as a
dev-dependency and start scripting your own scenarios.
