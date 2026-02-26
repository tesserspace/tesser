# tesser-backtest-wasm

Minimal WebAssembly ABI for running `tesser-backtest-core` in the browser.

This crate intentionally avoids `wasm-bindgen` glue to keep the integration simple:

- JS passes a UTF-8 JSON payload into linear memory
- WASM returns a UTF-8 JSON payload (result or error)

