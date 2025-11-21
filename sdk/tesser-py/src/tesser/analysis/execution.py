from __future__ import annotations

from decimal import Decimal
from typing import List, Sequence

import pandas as pd

BPS_FACTOR = Decimal("10000")
ZERO = Decimal("0")

_SLIPPAGE_COLUMNS = [
    "id",
    "symbol",
    "side",
    "created_at",
    "client_order_id",
    "algo_label",
    "fill_count",
    "filled_quantity",
    "avg_fill_price",
    "arrival_price",
    "notional",
    "total_fees",
    "implementation_shortfall",
    "slippage_bps",
]


def calculate_slippage(
    orders_df: pd.DataFrame,
    fills_df: pd.DataFrame,
    ticks_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Return per-order slippage metrics derived from Parquet exports."""

    _require_columns(orders_df, {"id", "symbol", "side", "created_at"}, "orders")
    _require_columns(fills_df, {"order_id", "fill_price", "fill_quantity"}, "fills")

    if orders_df.empty or fills_df.empty:
        return pd.DataFrame(columns=_SLIPPAGE_COLUMNS)

    orders = orders_df.copy()
    orders["created_at"] = pd.to_datetime(orders["created_at"], utc=True, errors="coerce")
    if "client_order_id" not in orders:
        orders["client_order_id"] = None
    orders["algo_label"] = orders["client_order_id"].apply(_infer_algo_label)

    fills = fills_df.copy()
    if "fee" not in fills:
        fills["fee"] = [ZERO for _ in range(len(fills))]
    else:
        fills["fee"] = [value if value is not None else ZERO for value in fills["fee"]]
    fills["notional"] = fills["fill_price"] * fills["fill_quantity"]
    fill_summary = (
        fills.groupby("order_id", dropna=False)
        .agg(
            fill_count=("order_id", "size"),
            filled_quantity=("fill_quantity", "sum"),
            notional=("notional", "sum"),
            total_fees=("fee", "sum"),
        )
        .reset_index()
    )
    fill_summary = fill_summary[fill_summary["filled_quantity"] > ZERO]
    if fill_summary.empty:
        return pd.DataFrame(columns=_SLIPPAGE_COLUMNS)
    fill_summary["avg_fill_price"] = fill_summary["notional"] / fill_summary["filled_quantity"]

    merged = orders.merge(fill_summary, left_on="id", right_on="order_id", how="inner")
    if merged.empty:
        return pd.DataFrame(columns=_SLIPPAGE_COLUMNS)

    merged = merged.sort_values("created_at")
    merged["arrival_price"] = _arrival_prices(merged, ticks_df)
    merged["implementation_shortfall"], merged["slippage_bps"] = zip(
        *merged.apply(_slippage_row, axis=1)
    )
    result = merged[
        [
            "id",
            "symbol",
            "side",
            "created_at",
            "client_order_id",
            "algo_label",
            "fill_count",
            "filled_quantity",
            "avg_fill_price",
            "arrival_price",
            "notional",
            "total_fees",
            "implementation_shortfall",
            "slippage_bps",
        ]
    ].copy()
    return result


def summarize_fills(
    fills_df: pd.DataFrame,
    *,
    group_by: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Aggregate fills per symbol/algo for TCA-style breakdowns."""

    _require_columns(fills_df, {"fill_price", "fill_quantity", "order_id"}, "fills")
    if fills_df.empty:
        group_cols = list(group_by or ("symbol",))
        metrics = [
            "order_count",
            "fill_count",
            "filled_quantity",
            "notional",
            "avg_fill_price",
            "fees",
            "first_fill",
            "last_fill",
        ]
        return pd.DataFrame(columns=group_cols + metrics)

    group_cols = list(group_by or ("symbol",))
    _require_columns(fills_df, set(group_cols), "fills")
    fills = fills_df.copy()
    if "fee" not in fills:
        fills["fee"] = [ZERO for _ in range(len(fills))]
    else:
        fills["fee"] = [value if value is not None else ZERO for value in fills["fee"]]
    fills["notional"] = fills["fill_price"] * fills["fill_quantity"]

    agg_kwargs = {
        "order_count": ("order_id", pd.Series.nunique),
        "fill_count": ("order_id", "size"),
        "filled_quantity": ("fill_quantity", "sum"),
        "notional": ("notional", "sum"),
        "fees": ("fee", "sum"),
    }
    if "timestamp" in fills.columns:
        agg_kwargs["first_fill"] = ("timestamp", "min")
        agg_kwargs["last_fill"] = ("timestamp", "max")

    grouped = fills.groupby(group_cols, dropna=False).agg(**agg_kwargs).reset_index()
    avg_values: List[Decimal | None] = []
    for notional, quantity in zip(grouped["notional"], grouped["filled_quantity"]):
        if quantity in (ZERO, 0):
            avg_values.append(None)
        else:
            avg_values.append(notional / quantity)
    grouped["avg_fill_price"] = avg_values
    return grouped


def _require_columns(df: pd.DataFrame, required: set[str], label: str) -> None:
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"{label} dataframe missing required columns: {', '.join(sorted(missing))}")


def _infer_algo_label(client_order_id: str | None) -> str:
    if not client_order_id:
        return "SIGNAL"
    normalized = client_order_id.lower()
    if normalized.startswith("twap"):
        return "TWAP"
    if normalized.startswith("vwap"):
        return "VWAP"
    if normalized.startswith("iceberg"):
        return "ICEBERG"
    if normalized.startswith("pegged"):
        return "PEGGED"
    if normalized.startswith("sniper"):
        return "SNIPER"
    if normalized.endswith("-sl"):
        return "STOP_LOSS"
    if normalized.endswith("-tp"):
        return "TAKE_PROFIT"
    return "SIGNAL"


def _side_sign(value) -> Decimal:
    if isinstance(value, str):
        normalized = value.lower()
        if normalized in {"buy", "long"}:
            return Decimal(1)
        if normalized in {"sell", "short"}:
            return Decimal(-1)
    try:
        numeric = Decimal(value)
    except Exception:  # pragma: no cover - defensive fallback
        return Decimal(1)
    return Decimal(1) if numeric >= ZERO else Decimal(-1)


def _slippage_row(row: pd.Series):
    filled = row["filled_quantity"]
    arrival = row["arrival_price"]
    avg_price = row["avg_fill_price"]
    if pd.isna(arrival) or arrival in (None, ZERO) or filled in (None, ZERO):
        return (None, None)
    sign = _side_sign(row["side"])
    price_delta = (avg_price - arrival) * sign
    shortfall = price_delta * filled
    slippage_bps = None
    if arrival != ZERO:
        slippage_bps = (price_delta / arrival) * BPS_FACTOR
    return (shortfall, slippage_bps)


def _arrival_prices(orders: pd.DataFrame, ticks_df: pd.DataFrame | None) -> pd.Series:
    if ticks_df is None or ticks_df.empty:
        return pd.Series([None] * len(orders), index=orders.index)
    _require_columns(ticks_df, {"symbol", "price", "exchange_timestamp"}, "ticks")
    ticks = ticks_df.copy()
    ticks = ticks.dropna(subset=["symbol"])
    ticks["exchange_timestamp"] = pd.to_datetime(
        ticks["exchange_timestamp"], utc=True, errors="coerce"
    )
    ticks = ticks.dropna(subset=["exchange_timestamp"])
    if ticks.empty:
        return pd.Series([None] * len(orders), index=orders.index)

    ticks = ticks.sort_values(["symbol", "exchange_timestamp"])
    probe = orders[["symbol", "created_at"]].copy()
    probe = probe.dropna(subset=["created_at", "symbol"])
    probe = probe.reset_index().rename(columns={"index": "__order_idx"})
    if probe.empty:
        return pd.Series([None] * len(orders), index=orders.index)

    aligned = pd.merge_asof(
        probe.sort_values(["symbol", "created_at"]),
        ticks,
        left_on="created_at",
        right_on="exchange_timestamp",
        by="symbol",
        direction="backward",
        allow_exact_matches=True,
    )
    arrivals = pd.Series([None] * len(orders), index=orders.index)
    arrivals.loc[aligned["__order_idx"]] = aligned["price"].values
    return arrivals
