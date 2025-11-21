"""Python SDK for remote Tesser strategies and offline analytics."""

from .analysis import calculate_slippage, summarize_fills
from .data import DataStore, load_candles, load_fills, load_orders, load_ticks
from .models import (
    Candle,
    OrderBook,
    OrderBookLevel,
    Position,
    Side,
    Signal,
    SignalKind,
    StrategyContext,
    Tick,
)
from .runner import Runner
from .strategy import Strategy, StrategyInitResult

__all__ = [
    "Candle",
    "OrderBook",
    "OrderBookLevel",
    "Position",
    "Side",
    "Signal",
    "SignalKind",
    "Strategy",
    "StrategyContext",
    "StrategyInitResult",
    "Tick",
    "Runner",
    "DataStore",
    "load_candles",
    "load_fills",
    "load_orders",
    "load_ticks",
    "calculate_slippage",
    "summarize_fills",
]
