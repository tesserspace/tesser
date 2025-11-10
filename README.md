# Tesser: A High-Performance Quantitative Trading Framework in Rust


[![CI](https://github.com/pluveto/tesser/actions/workflows/ci.yml/badge.svg)](https://github.com/pluveto/tesser/actions/workflows/ci.yml)
[![License: MIT/Apache-2.0](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](https://opensource.org/licenses/MIT)
[![Crates.io](https://img.shields.io/crates/v/tesser.svg)](https://crates.io/crates/tesser)

<img src="./tesser.jpg" alt="Tesser" width="200" />

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
    ├── tesser-bybit    # Concrete Bybit connector (REST + WebSocket)
    └── tesser-paper    # A simulated exchange for backtesting/paper fills
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
*   **Contents**: One crate per exchange. Today that includes `tesser-bybit` (production-grade REST + WebSocket client based on the official v5 docs) and `tesser-paper` (deterministic connector used by the backtester and paper trading engine).
*   **Rule**: All code that is specific to one exchange (e.g., endpoint URLs, authentication methods, JSON payload formats) must be confined to a crate within this directory.

##### `tesser-bybit`
* Implements the Bybit v5 REST API (`ExecutionClient`) plus a resilient WebSocket stream (`MarketStream`) that powers live trading. Configurable via `config/[env].toml` exchange profiles.

##### `tesser-paper`
* Simulates fills instantly and replays deterministic ticks/candles. Used by the backtester and as the default execution target for `live run` until you're ready to wire real capital.

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

1.  Generate a strategy parameter file (see the `research/` section below). Example `research/strategies/sma_cross_optimal.toml`:
    ```toml
    strategy_name = "SmaCross"

    [params]
    symbol = "BTCUSDT"
    fast_period = 12
    slow_period = 30
    min_samples = 40
    ```
2.  Run a mock backtest with the CLI:
    ```sh
    cargo run -p tesser-cli -- \
        backtest run \
        --strategy-config research/strategies/sma_cross_optimal.toml \
        --data data/bybit_testnet/BTCUSDT/1m_20231201-20240201.csv \
        --candles 500 \
        --quantity 0.02
    ```
    (Omit `--data` to fall back to synthetic candles; supply one or more CSVs to replay exchange data produced by `tesser-cli data download`.)

### CLI Overview

`tesser-cli` is the single entry point for local research and operations:

```text
tesser-cli --env default <COMMAND>

Commands:
  data download|validate|resample   # Download/inspect historical data
  backtest run --strategy-config    # Executes a single backtest (supports multiple --data inputs)
  backtest batch --config ...       # Runs multiple configs and writes an optional summary CSV
  live run --strategy-config        # Runs the live Bybit stream + paper execution loop
  state inspect [--path <file>]     # Prints the persisted SQLite state snapshot (use --raw for JSON)
  strategies                        # Lists compiled strategies
```

Configuration files live in `config/`. The loader merges the following sources (lowest → highest priority):

1. `config/default.toml`
2. `config/{env}.toml` (selected by `--env`)
3. `config/local.toml` (gitignored)
4. Environment variables prefixed with `TESSER_` (e.g. `TESSER_exchange__bybit_testnet__api_key`)

### Live Trading & Observability

`tesser-cli live run` now drives the full production event loop introduced in Phase 2:

```sh
cargo run -p tesser-cli -- \
  live run \
  --strategy-config research/strategies/sma_cross.toml \
  --quantity 0.02 \
  --interval 1m
```

Add `--exec live` (and populate your `[exchange.<name>]` `api_key`/`api_secret`) to forward orders to the real Bybit REST API instead of the paper engine. **This will route live capital**; use the testnet profile while validating your setup.

What happens under the hood:

- **Market data**: `tesser-bybit` maintains a resilient WebSocket connection to the public `linear` stream (`kline.<interval>.<symbol>` and `publicTrade.<symbol>` topics). The connection automatically heartbeats every 20s and reconnects on transient errors.
- **Execution**: Signals are routed through `tesser-execution` into the selected backend. `--exec paper` keeps the previous behavior (instant synthetic fills). `--exec live` submits real REST orders and records their IDs in `live_state.json` so you can reconcile with the exchange.
- **State persistence**: Portfolio equity, open orders and last prices are persisted to a SQLite database at `config.live.state_path` (default `./reports/live_state.db`). Restart the process and it will resume from this ACID snapshot; run `tesser-cli state inspect` to view the current contents or dump the raw JSON payload for auditing.
- **Structured logging**: When running `live`, a JSON file is written to `config.live.log_path` (default `./logs/live.json`). Point Promtail/Loki/Grafana at that file to build dashboards without touching stdout logs.
- **Metrics**: A Prometheus endpoint is exposed at `config.live.metrics_addr` (default `127.0.0.1:9100`). Scrape `/metrics` to monitor tick/candle throughput, portfolio equity, order errors, and data-gap gauges.
- **Alerting**: The `[live.alerting]` section lets you enforce guardrails (max data gap, consecutive order failures, drawdown limit). Provide a `webhook_url` (Slack, Telegram, Alertmanager, etc.) or leave it empty for log-only alerts.

#### State Database Backups

The SQLite file referenced by `config.live.state_path` is the source of truth for portfolio and order recovery. Treat it like any other operational database:

1. **Ad-hoc inspection**: `tesser-cli state inspect` prints a human-readable summary, while `--raw` dumps the JSON payload you can edit or version-control.
2. **Offline backup**: stop the live process and copy the file (`cp reports/live_state.db reports/live_state.db.bak`).
3. **Online backup**: when a session must stay up, use SQLite's native snapshotting: `sqlite3 reports/live_state.db ".backup 'reports/live_state.db.bak'"`. The command is safe because the repository enables WAL journaling.
4. **Restoring**: replace the file with a known-good backup and restart `tesser-cli live run`; the runtime will hydrate the portfolio from the restored snapshot.

> ⚠️ **Risk warning**: `--exec live` forwards orders exactly as produced by your strategy—there is no extra confirmation prompt, and portfolio P&L stays paper-based until a future release. Always dry-run on Bybit testnet before pointing to mainnet keys.

Key CLI flags:

| Flag | Description | Default |
| --- | --- | --- |
| `--exchange` | Exchange profile defined under `[exchange.*]` | `bybit_testnet` |
| `--category` | Bybit channel (`linear`, `inverse`, `spot`, …) | `linear` |
| `--interval` | Candle interval understood by `tesser_core::Interval` | `1m` |
| `--quantity` | Fixed order size routed through `FixedOrderSizer` | `1.0` |
| `--exec` | Execution backend (`paper` or `live`) | `paper` |
| `--slippage-bps` / `--fee-bps` | Synthetic execution frictions in basis points | `0` |
| `--latency-ms` | Delay between signal and fill simulation | `0` |
| `--state-path`, `--metrics-addr`, `--log-path` | Override the `[live]` config block | see config |
| `--webhook-url` | Per-run override for `[live.alerting].webhook_url` | empty |

Inspect all options with `cargo run -p tesser-cli -- live run --help`.

Sample snippet from `config/default.toml`:

```toml
[live]
state_path = "./reports/live_state.json"
metrics_addr = "127.0.0.1:9100"
log_path = "./logs/live.json"

[live.alerting]
webhook_url = ""          # Optional HTTP endpoint
max_data_gap_secs = 300     # Alert if no ticks/candles are seen for 5 minutes
max_order_failures = 3      # Trigger after N consecutive execution errors
max_drawdown = 0.03         # 3% peak-to-trough drawdown guardrail

[risk_management]
max_order_quantity = 1.0    # Fat-finger guard per order (base asset qty)
max_position_quantity = 2.0 # Absolute cap on aggregate exposure per symbol
max_drawdown = 0.05         # Liquidate-only kill switch threshold (fractional)
```

Every CLI flag (e.g., `--state-path`, `--metrics-addr`, `--log-path`, `--webhook-url`) overrides the config file so you can spin up multiple paper sessions with different telemetry endpoints.

When `tesser-cli live run` executes, each order is filtered through the pre-trade risk layer: quantities above `max_order_quantity` are rejected, projected exposure cannot exceed `max_position_quantity`, and once equity suffers a drawdown beyond `max_drawdown` the portfolio flips into liquidate-only mode (only allowing exposure-reducing orders) until the process is restarted.

### Python Research Workflow

Rust handles live execution; Python (powered by [`uv`](https://github.com/astral-sh/uv)) owns fast research loops. The `research/` directory provides:

```
research/
├── notebooks/      # Exploratory analysis
├── scripts/        # Batch jobs (e.g., parameter sweeps)
├── strategies/     # Outputs consumed by Rust (TOML, ONNX, etc.)
└── pyproject.toml  # Locked dependencies for uv
```

Quick start:

```sh
cd research
uv venv
source .venv/bin/activate
uv pip install -e .
uv run python scripts/find_optimal_sma.py --data ../data/btc.parquet
uv run python scripts/optimize_rsi.py --data ../data/btc.parquet --output strategies/rsi_from_python.toml
uv run python scripts/train_ml_classifier.py --data ../data/btc.parquet --output models/ml_linear.toml
```

The generated TOML files feed directly into `tesser-cli backtest run --strategy-config ...`.

### Strategy Portfolio

The upgraded `tesser-strategy` crate bundles a diverse suite for pressure-testing the stack:

| Name | Type | Highlights |
| --- | --- | --- |
| `SmaCross` | Trend following | Dual moving-average crossover |
| `RsiReversion` | Mean reversion | RSI thresholds with configurable lookbacks |
| `BollingerBreakout` | Volatility/Band breakout | Uses standard deviation bands for entries |
| `MlClassifier` | Machine learning | Loads an external model artifact for real-time inference |
| `PairsTradingArbitrage` | Statistical arbitrage | Operates on two correlated symbols |
| `OrderBookImbalance` | Microstructure | Consumes order-book snapshots to trade short-term imbalances |

Each strategy exposes a typed configuration schema and registers the symbols (one or many) it operates on. Sample configs live in `research/strategies/` and the ML artifact in `research/models/`, so you can run them directly with the CLI.

## Contributing

Contributions are highly welcome! To get started, please read our `CODING_STYLE.md` guide. A great way to contribute is by adding a new exchange implementation in the `connectors/` directory.

## License

This project is licensed under either of
*   Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
*   MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Disclaimer

Trading financial markets involves substantial risk. Tesser is a software framework and not financial advice. All trading decisions are your own. The authors and contributors are not responsible for any financial losses. Always test your strategies thoroughly in a simulated environment before deploying with real capital.
