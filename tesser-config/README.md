# tesser-config

Layered configuration loader shared by every binary in the workspace.

## Overview
- Wraps the `config` crate to merge `config/default.toml`, `config/{env}.toml`, `config/local.toml`, and `TESSER_*` environment variables.
- Exposes strongly typed structs (`AppConfig`, `ExchangeConfig`, `LiveRuntimeConfig`, etc.) consumed by `tesser-cli` and other binaries.
- Handles defaults for data paths, log levels, backtest equity, live state path, metrics bind address, and alert thresholds.

## Usage
```rust
let cfg = tesser_config::load_config(Some("default"))?;
println!("Data path: {}", cfg.data_path.display());
```

## Tests
```sh
cargo test -p tesser-config
```
