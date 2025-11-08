# Research Environment

This directory hosts all Python-based research workflows:

- `notebooks/`: Jupyter or VS Code notebooks for exploratory data analysis.
- `scripts/`: Reusable Python scripts (feature generation, parameter sweeps, ML training).
- `strategies/`: Serialized outputs such as TOML parameter files that Rust consumers load (sample configs ship in this repo).
- `models/`: Artifacts produced by ML pipelines (e.g., the linear momentum model consumed by `MlClassifier`).

## Quick Start

```bash
cd research
uv venv
source .venv/bin/activate
uv pip install -e .
```

Once the environment is ready, you can open notebooks or run scripts:

```bash
uv run python scripts/find_optimal_sma.py --data ../data/btc.parquet
```

Store generated strategy configs under `strategies/` (see `sma_cross.toml`, `rsi_reversion.toml`, etc.) and drop ML artifacts under `models/`. The Rust CLI consumes these files directly via `--strategy-config` (and the referenced `model_path`).
