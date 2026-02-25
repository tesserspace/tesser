# tesser-backtest-core

Deterministic, synchronous backtest primitives used by the browser playground.

This crate is intentionally **pure compute** (no tokio, no IO) so it can be compiled to WebAssembly
and reused by native tooling with identical semantics.

