# Tesser: A High-Performance Quantitative Trading Framework in Rust

[![CI](https://github.com/pluveto/tesser/actions/workflows/ci.yml/badge.svg)](https://github.com/pluveto/tesser/actions/workflows/ci.yml)
[![License: MIT/Apache-2.0](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](https://opensource.org/licenses/MIT)
[![Crates.io](https://img.shields.io/crates/v/tesser.svg)](https://crates.io/crates/tesser)

Tesser is a modular, event-driven framework for building and executing quantitative trading strategies. Written entirely in Rust, it is designed for performance, reliability, and extensibility, allowing traders and developers to move from idea to backtesting and live deployment with confidence.

The core design principle is the separation of concerns, decoupling trading logic from the specifics of any particular exchange.

## Key Features

*   **Unified Exchange Interface**: Connect to any exchange by implementing a common set of traits. Add a new exchange without touching the core logic.
*   **Event-Driven Architecture**: A single, consistent event loop for both high-performance backtesting and live trading.
*   **Accurate Backtesting Engine**: Simulate strategies against historical market data to evaluate performance before risking capital.
*   **Decoupled Components**: Clear boundaries between data handling, strategy logic, execution, and portfolio management make the system robust and easy to maintain.
*   **Built for Performance**: Leverages Rust's zero-cost abstractions to handle high-frequency data and execute orders with minimal latency.

## Architecture & Crate Responsibilities

Tesser is organized as a Cargo workspace. Understanding the role of each crate is crucial for contributing to the project. The primary goal is to keep the core logic (strategy, portfolio) completely independent of any specific broker or data source.

```
tesser/
├── Cargo.toml
|
├── tesser-core         # Foundational data structures
├── tesser-broker       # The universal API for all exchanges (Traits)
|
├── tesser-strategy     # Your trading algorithms
├── tesser-portfolio    # Position, risk, and PnL management
|
├── tesser-data         # Consumes data via the broker trait
├── tesser-execution    # Sends orders via the broker trait
|
├── tesser-backtester   # The simulation engine
├── tesser-cli          # The command-line user interface
|
└── connectors/         # Directory for specific exchange implementations
    ├── tesser-binance  # Concrete implementation for Binance
    ├── tesser-coinbase # Concrete implementation for Coinbase
    └── tesser-paper    # A simulated exchange for backtesting
```

---

### Core Crates

#### `tesser-core`
**Responsibility**: Defines the universal, foundational data structures and enums used everywhere.
*   **Contents**: `struct`s like `Order`, `Trade`, `Candle`, `Position`, `Tick`. `enum`s like `Side` (Buy/Sell), `OrderType` (Limit/Market), `Interval` (1m, 1h).
*   **Rule**: This crate should have minimal to no dependencies. It is the bedrock of the entire project.

#### `tesser-broker`
**Responsibility**: The API Unification Layer. It defines the abstract interface for interacting with any exchange.
*   **Contents**: `trait`s (interfaces) like `MarketStream` (for subscribing to live data) and `ExecutionClient` (for placing orders and managing accounts). It contains **no concrete implementations**.
*   **Rule**: If you are defining a behavior that an exchange must provide (e.g., "fetch open orders"), the trait for it belongs here.

---

### Logic Crates

#### `tesser-strategy`
**Responsibility**: Contains the "brains" of the trading system—your algorithms.
*   **Contents**: Implementations of a `Strategy` trait. This is where you calculate indicators (e.g., RSI, Moving Averages) and generate `Signal` events (e.g., "go long", "exit position").
*   **Rule**: Strategy code must be pure and self-contained. It should only depend on `tesser-core` for data types and operate on the data it is given, without any knowledge of where the data comes from (live feed or backtest).

#### `tesser-portfolio`
**Responsibility**: Manages the state of your trading portfolio. It is the system's "accountant."
*   **Contents**: Logic for tracking positions, calculating Profit and Loss (PnL), managing margin, and evaluating risk metrics. It updates its state by listening to `Fill` events (i.e., when an order is executed).
*   **Rule**: Portfolio logic should not be aware of any specific exchange. It works with the abstract `Position` and `Fill` types from `tesser-core`.

---

### Engine Crates

#### `tesser-data`
**Responsibility**: Manages the flow of market data from a source to the strategies.
*   **Contents**: The logic to handle incoming data streams (from a `MarketStream` implementation) and historical data feeds. It is responsible for tasks like building candles from ticks or replaying historical data files.
*   **Rule**: This crate uses a generic `MarketStream` trait object. It does not know *which* exchange it is getting data from.

#### `tesser-execution`
**Responsibility**: Translates trading signals from strategies into actionable orders.
*   **Contents**: The Order Management System (OMS). It receives a `Signal` and decides how to act on it (e.g., calculate order size, set price). It then sends the order to the broker.
*   **Rule**: This crate uses a generic `ExecutionClient` trait object. It does not know *which* exchange it is sending orders to.

---

### Application & Connector Crates

#### `connectors/` (Directory)
**Responsibility**: This is where all the specific, concrete exchange logic lives.
*   **Contents**: A collection of crates, one for each supported exchange (e.g., `tesser-binance`, `tesser-ftx`). Each crate implements the traits defined in `tesser-broker`.
*   **`tesser-paper`**: A special connector that implements the broker traits to simulate an exchange. It is used by the backtester and for paper trading.
*   **Rule**: All code that is specific to one exchange (e.g., endpoint URLs, authentication methods, JSON payload formats) must be confined to a crate within this directory.

#### `tesser-backtester`
**Responsibility**: An offline engine that simulates a strategy's performance against historical data.
*   **Contents**: An event loop that reads historical data, feeds it to the `tesser-data` module, and uses the `tesser-paper` connector to simulate order fills. It generates performance reports (Sharpe ratio, max drawdown, etc.).
*   **Rule**: The backtester's job is to wire the other components together in a simulated environment.

#### `tesser-cli`
**Responsibility**: The user-facing application.
*   **Contents**: The `main.rs` file. It parses command-line arguments and configuration files to decide what to do (e.g., run a backtest, start a live trading session).
*   **Rule**: This is the application's entry point. It is responsible for initializing and connecting all the necessary components for a given task.

## Getting Started

### Prerequisites

*   Rust toolchain (latest stable version recommended): [https://rustup.rs/](https://rustup.rs/)

### Building

1.  Clone the repository:
    ```sh
    git clone https://github.com/pluveto/tesser.git
    cd tesser
    ```

2.  Build the entire project workspace:
    ```sh
    cargo build --release
    ```

### Example: Running a Backtest

1.  Ensure you have historical data available in the required format.
2.  Configure your backtest in `config.toml`.
3.  Run the backtester from the command line:
    ```sh
    cargo run --release --bin tesser-cli -- \
        backtest \
        --strategy "SmaCross" \
        --symbol "BTC-USDT" \
        --from "2024-01-01" \
        --to "2024-06-30"
    ```

### Quick Start (Mock Backtest)

You can validate the pipeline without any external data feeds:

```sh
cargo run -p tesser-cli -- backtest --symbol BTCUSDT --candles 200 --quantity 0.01
```

### Bybit Testnet Utilities

The Bybit connector follows the signing process described in `bybit-api-docs/docs/v5/guide.mdx`. You can verify connectivity with the public server-time endpoint via:

```sh
cargo run -p tesser-cli -- bybit-time
```

Set the `BAPI_KEY`/`BAPI_SECRET` environment variables (or wire them into your own config loader) before attempting private calls such as order placement.

## Contributing

Contributions are highly welcome! To get started, please read our `CODING_STYLE.md` guide. A great way to contribute is by adding a new exchange implementation in the `connectors/` directory.

## License

This project is licensed under either of
*   Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
*   MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Disclaimer

Trading financial markets involves substantial risk. Tesser is a software framework and not financial advice. All trading decisions are your own. The authors and contributors are not responsible for any financial losses. Always test your strategies thoroughly in a simulated environment before deploying with real capital.
