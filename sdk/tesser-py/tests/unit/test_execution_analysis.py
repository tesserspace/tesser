from __future__ import annotations

from decimal import Decimal

import pandas as pd

from tesser.analysis import calculate_slippage, summarize_fills


def test_calculate_slippage_uses_arrival_price():
    orders = pd.DataFrame(
        [
            {
                "id": "order-1",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "created_at": pd.Timestamp("2024-01-01T00:00:00Z"),
                "client_order_id": "twap-demo",
            }
        ]
    )
    fills = pd.DataFrame(
        [
            {
                "order_id": "order-1",
                "fill_price": Decimal("101.5"),
                "fill_quantity": Decimal("2"),
                "fee": Decimal("0.02"),
                "timestamp": pd.Timestamp("2024-01-01T00:01:00Z"),
                "symbol": "BTCUSDT",
            }
        ]
    )
    ticks = pd.DataFrame(
        [
            {
                "symbol": "BTCUSDT",
                "price": Decimal("100"),
                "exchange_timestamp": pd.Timestamp("2024-01-01T00:00:00Z"),
            }
        ]
    )

    report = calculate_slippage(orders, fills, ticks_df=ticks)
    assert len(report) == 1
    entry = report.iloc[0]
    assert entry["algo_label"] == "TWAP"
    assert entry["filled_quantity"] == Decimal("2")
    assert entry["implementation_shortfall"] == Decimal("3")
    assert entry["slippage_bps"] == Decimal("150")


def test_summarize_fills_groups_metrics():
    fills = pd.DataFrame(
        [
            {
                "order_id": "order-1",
                "symbol": "BTCUSDT",
                "fill_price": Decimal("101"),
                "fill_quantity": Decimal("1"),
                "fee": Decimal("0.01"),
                "timestamp": pd.Timestamp("2024-01-01T00:00:30Z"),
            },
            {
                "order_id": "order-2",
                "symbol": "BTCUSDT",
                "fill_price": Decimal("102"),
                "fill_quantity": Decimal("2"),
                "fee": Decimal("0.02"),
                "timestamp": pd.Timestamp("2024-01-01T00:01:00Z"),
            },
        ]
    )

    summary = summarize_fills(fills)
    assert summary.iloc[0]["symbol"] == "BTCUSDT"
    assert summary.iloc[0]["order_count"] == 2
    assert summary.iloc[0]["fill_count"] == 2
    assert summary.iloc[0]["filled_quantity"] == Decimal("3")
    assert summary.iloc[0]["fees"] == Decimal("0.03")
