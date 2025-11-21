from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from tesser.data import DataStore, load_fills, load_orders, load_ticks


def _write_partition(root: Path, kind: str, partition: str, rows: list[dict]):
    dir_path = root / kind / partition
    dir_path.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, dir_path / "part-0000.parquet")


def _populate_store(tmp_path: Path) -> DataStore:
    root = tmp_path / "flight"
    root.mkdir()
    _write_partition(
        root,
        "ticks",
        "2024-01-01",
        [
            {
                "symbol": "BTCUSDT",
                "price": 100.0,
                "size": 1.0,
                "side": 1,
                "exchange_timestamp": datetime(2024, 1, 1, 0, 0),
                "received_at": datetime(2024, 1, 1, 0, 0),
            },
            {
                "symbol": "ETHUSDT",
                "price": 10.0,
                "size": 5.0,
                "side": -1,
                "exchange_timestamp": datetime(2024, 1, 1, 0, 1),
                "received_at": datetime(2024, 1, 1, 0, 1),
            },
        ],
    )
    _write_partition(
        root,
        "ticks",
        "2024-01-02",
        [
            {
                "symbol": "BTCUSDT",
                "price": 101.0,
                "size": 2.0,
                "side": 1,
                "exchange_timestamp": datetime(2024, 1, 2, 0, 0),
                "received_at": datetime(2024, 1, 2, 0, 0),
            }
        ],
    )
    _write_partition(
        root,
        "orders",
        "2024-01-01",
        [
            {
                "id": "order-1",
                "symbol": "BTCUSDT",
                "side": 1,
                "order_type": "limit",
                "status": 2,
                "time_in_force": "gtc",
                "quantity": 2,
                "price": 100.5,
                "trigger_price": None,
                "client_order_id": "twap-demo",
                "take_profit": None,
                "stop_loss": None,
                "display_quantity": None,
                "filled_quantity": 1,
                "avg_fill_price": None,
                "created_at": datetime(2024, 1, 1, 0, 0),
                "updated_at": datetime(2024, 1, 1, 0, 5),
            }
        ],
    )
    _write_partition(
        root,
        "fills",
        "2024-01-01",
        [
            {
                "order_id": "order-1",
                "symbol": "BTCUSDT",
                "side": 1,
                "fill_price": 100.1,
                "fill_quantity": 1,
                "fee": 0.01,
                "timestamp": datetime(2024, 1, 1, 0, 1),
            }
        ],
    )
    return DataStore(root)


def test_files_filtered_by_partition(tmp_path):
    store = _populate_store(tmp_path)
    files = store.files("ticks", start_date="2024-01-02", end_date="2024-01-02")
    assert len(files) == 1
    assert "2024-01-02" in str(files[0])


def test_loaders_filter_by_symbol_and_dates(tmp_path):
    store = _populate_store(tmp_path)
    ticks = load_ticks(store, symbol="BTCUSDT", start="2024-01-01", end="2024-01-01")
    assert list(ticks["symbol"]) == ["BTCUSDT"]
    assert pd.to_datetime(ticks["exchange_timestamp"]).dt.tz_localize("UTC").iloc[0].day == 1

    orders = load_orders(store, symbol="BTCUSDT", start="2024-01-01", end="2024-01-02")
    assert orders["client_order_id"].iloc[0] == "twap-demo"

    fills = load_fills(store, symbol=["BTCUSDT", "ETHUSDT"], start="2024-01-01", end="2024-01-03")
    assert fills["fill_price"].tolist() == [100.1]
