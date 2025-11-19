# tesser-py

`tesser-py` is the Python SDK for building remote strategies that plug into the Tesser trading engine via gRPC.

## Requirements

- Python 3.9+
- [`uv`](https://github.com/astral-sh/uv) for dependency management (pip/venv also works, but all docs/tests assume uv)
- `protoc` (v27+) available on PATH when regenerating protobuf stubs

## Quick Start

```bash
cd sdk/tesser-py
uv sync --all-extras
uv run scripts/codegen.py
uv run pytest
```

Then start a strategy:

```python
import asyncio
from tesser import Runner, Strategy, Signal, SignalKind

class PyCross(Strategy):
    def __init__(self):
        super().__init__(name="py-cross", symbol="BTC-USD")

    async def on_tick(self, context, tick):
        if tick.price > 50_000:
            return [Signal(symbol=tick.symbol, kind=SignalKind.ENTER_LONG)]
        return []

if __name__ == "__main__":
    asyncio.run(Runner(PyCross()).serve())
```

## Layout

```
sdk/tesser-py/
├── pyproject.toml
├── src/tesser/        # Library sources
├── scripts/codegen.py # Protobuf generator
├── tests/             # Unit/integration tests
└── examples/
```

## Code Generation

`uv run scripts/codegen.py` compiles the protobuf definition located under `tesser-rpc/proto` and drops the generated files in `src/tesser/protos`. This step runs automatically in CI and should be executed whenever the proto changes.

## Version Sync

The Python package version is derived from the workspace `Cargo.toml`. Run `uv run scripts/sync_version.py` whenever the Rust version changes to keep the SDK aligned with the rest of the monorepo.
