from __future__ import annotations

from typing import Literal, Sequence, Tuple, Union, TYPE_CHECKING, cast

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

from .store import DataStore, DateLike

if TYPE_CHECKING:  # pragma: no cover - imported only for type hints
    import polars as pl

FrameLike = Union[pd.DataFrame, "pl.DataFrame"]
SymbolFilter = Union[str, Sequence[str]]

_DATASET_COLUMNS = {
    "ticks": ["symbol", "price", "size", "side", "exchange_timestamp", "received_at"],
    "candles": [
        "symbol",
        "interval",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "timestamp",
    ],
    "fills": ["order_id", "symbol", "side", "fill_price", "fill_quantity", "fee", "timestamp"],
    "orders": [
        "id",
        "symbol",
        "side",
        "order_type",
        "status",
        "time_in_force",
        "quantity",
        "price",
        "trigger_price",
        "client_order_id",
        "take_profit",
        "stop_loss",
        "display_quantity",
        "filled_quantity",
        "avg_fill_price",
        "created_at",
        "updated_at",
    ],
}

_TIME_FIELDS = {
    "ticks": "exchange_timestamp",
    "candles": "timestamp",
    "fills": "timestamp",
    "orders": "created_at",
}


def load_ticks(
    store: DataStore,
    *,
    symbol: SymbolFilter | None = None,
    start: DateLike | None = None,
    end: DateLike | None = None,
    columns: Sequence[str] | None = None,
    engine: Literal["pandas", "polars"] = "pandas",
) -> FrameLike:
    """Load tick snapshots recorded by the flight recorder."""

    return _load_dataset("ticks", store, symbol, start, end, columns, engine)


def load_candles(
    store: DataStore,
    *,
    symbol: SymbolFilter | None = None,
    start: DateLike | None = None,
    end: DateLike | None = None,
    columns: Sequence[str] | None = None,
    engine: Literal["pandas", "polars"] = "pandas",
) -> FrameLike:
    """Load recorded candle bars."""

    return _load_dataset("candles", store, symbol, start, end, columns, engine)


def load_orders(
    store: DataStore,
    *,
    symbol: SymbolFilter | None = None,
    start: DateLike | None = None,
    end: DateLike | None = None,
    columns: Sequence[str] | None = None,
    engine: Literal["pandas", "polars"] = "pandas",
) -> FrameLike:
    """Load persisted order lifecycle events."""

    return _load_dataset("orders", store, symbol, start, end, columns, engine)


def load_fills(
    store: DataStore,
    *,
    symbol: SymbolFilter | None = None,
    start: DateLike | None = None,
    end: DateLike | None = None,
    columns: Sequence[str] | None = None,
    engine: Literal["pandas", "polars"] = "pandas",
) -> FrameLike:
    """Load recorded fills."""

    return _load_dataset("fills", store, symbol, start, end, columns, engine)


def _load_dataset(
    kind: str,
    store: DataStore,
    symbol: SymbolFilter | None,
    start: DateLike | None,
    end: DateLike | None,
    columns: Sequence[str] | None,
    engine: Literal["pandas", "polars"],
) -> FrameLike:
    start_ts = _normalize_timestamp(start)
    end_ts = _normalize_timestamp(end)
    paths = store.files(
        kind,
        start_date=start_ts.date() if start_ts else None,
        end_date=end_ts.date() if end_ts else None,
    )
    if not paths:
        return _empty_frame(kind, columns, engine)

    dataset = ds.dataset([str(path) for path in paths], format="parquet")
    symbols = _normalize_symbol_filter(symbol)
    filter_expr = _build_filter(symbols, start_ts, end_ts, _TIME_FIELDS[kind])
    table = dataset.to_table(columns=columns, filter=filter_expr)
    if table.num_rows == 0:
        return _empty_frame(kind, columns or table.schema.names, engine)
    return _materialize(table, engine)


def _normalize_symbol_filter(symbols: SymbolFilter | None) -> Tuple[str, ...] | None:
    if symbols is None:
        return None
    if isinstance(symbols, str):
        return (symbols,)
    unique: list[str] = []
    for entry in symbols:
        if not isinstance(entry, str):
            raise TypeError("symbol filter entries must be strings")
        if entry not in unique:
            unique.append(entry)
    return tuple(unique) if unique else None


def _normalize_timestamp(value: DateLike | None) -> pd.Timestamp | None:
    if value is None:
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts


def _build_filter(
    symbols: Tuple[str, ...] | None,
    start_ts: pd.Timestamp | None,
    end_ts: pd.Timestamp | None,
    time_field: str,
):
    expression = None
    if symbols:
        expression = ds.field("symbol").isin(list(symbols))
    if time_field:
        if start_ts is not None:
            expr = ds.field(time_field) >= pa.scalar(start_ts.to_pydatetime(), type=pa.timestamp("ns"))
            expression = expr if expression is None else expression & expr
        if end_ts is not None:
            expr = ds.field(time_field) <= pa.scalar(end_ts.to_pydatetime(), type=pa.timestamp("ns"))
            expression = expr if expression is None else expression & expr
    return expression


def _materialize(table, engine: Literal["pandas", "polars"]) -> FrameLike:
    if engine == "pandas":
        return table.to_pandas(use_threads=True, timestamp_as_object=False)
    if engine == "polars":
        try:
            import polars as pl
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("polars is not installed. Install the 'data' extra to enable it.") from exc
        return cast(FrameLike, pl.from_arrow(table))
    raise ValueError(f"unsupported engine '{engine}'")


def _empty_frame(
    kind: str,
    columns: Sequence[str] | None,
    engine: Literal["pandas", "polars"],
) -> FrameLike:
    names = list(columns) if columns else list(_DATASET_COLUMNS[kind])
    if engine == "pandas":
        return pd.DataFrame(columns=names)
    if engine == "polars":
        try:
            import polars as pl
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("polars is not installed. Install the 'data' extra to enable it.") from exc
        return cast(FrameLike, pl.DataFrame({name: [] for name in names}))
    raise ValueError(f"unsupported engine '{engine}'")
