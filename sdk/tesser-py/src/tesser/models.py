from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import List, Optional

from .utils.pandas import to_dataframe


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class SignalKind(str, Enum):
    ENTER_LONG = "ENTER_LONG"
    EXIT_LONG = "EXIT_LONG"
    ENTER_SHORT = "ENTER_SHORT"
    EXIT_SHORT = "EXIT_SHORT"
    FLATTEN = "FLATTEN"


@dataclass(slots=True)
class DecimalValue:
    value: Decimal


@dataclass(slots=True)
class Tick:
    symbol: str
    price: Decimal
    size: Decimal
    side: Side
    exchange_timestamp: datetime
    received_at: datetime


@dataclass(slots=True)
class Candle:
    symbol: str
    interval: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    timestamp: datetime


@dataclass(slots=True)
class OrderBookLevel:
    price: Decimal
    size: Decimal


@dataclass(slots=True)
class OrderBook:
    symbol: str
    bids: List[OrderBookLevel]
    asks: List[OrderBookLevel]
    timestamp: datetime


@dataclass(slots=True)
class Fill:
    order_id: str
    symbol: str
    side: Side
    fill_price: Decimal
    fill_quantity: Decimal
    fee: Optional[Decimal]
    timestamp: datetime


@dataclass(slots=True)
class Position:
    symbol: str
    side: Optional[Side]
    quantity: Decimal
    entry_price: Optional[Decimal]
    unrealized_pnl: Decimal
    updated_at: datetime


@dataclass(slots=True)
class StrategyContext:
    positions: List[Position] = field(default_factory=list)

    def to_pandas(self):  # pragma: no cover - import heavy
        """Return a pandas DataFrame of positions if pandas is installed."""
        return to_dataframe(self.positions)


@dataclass(slots=True)
class Signal:
    symbol: str
    kind: SignalKind
    confidence: float = 1.0
    stop_loss: Optional[Decimal] = None
    take_profit: Optional[Decimal] = None
    note: Optional[str] = None


@dataclass(slots=True)
class StrategyInitResult:
    symbols: List[str] = field(default_factory=list)
    success: bool = True
    error_message: str = ""
