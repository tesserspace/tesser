from __future__ import annotations

import json
from typing import Iterable, List

from .models import (
    Candle,
    Fill,
    OrderBook,
    OrderBookLevel,
    Position,
    Side,
    Signal,
    SignalKind,
    StrategyContext,
    StrategyInitResult,
    Tick,
)
from .utils.decimal import from_decimal, from_timestamp, to_decimal
from .protos import tesser_pb2 as proto


_SIDE_MAP = {
    proto.Side.SIDE_BUY: Side.BUY,
    proto.Side.SIDE_SELL: Side.SELL,
}

_SIGNAL_KIND_MAP = {
    proto.Signal.Kind.KIND_ENTER_LONG: SignalKind.ENTER_LONG,
    proto.Signal.Kind.KIND_EXIT_LONG: SignalKind.EXIT_LONG,
    proto.Signal.Kind.KIND_ENTER_SHORT: SignalKind.ENTER_SHORT,
    proto.Signal.Kind.KIND_EXIT_SHORT: SignalKind.EXIT_SHORT,
    proto.Signal.Kind.KIND_FLATTEN: SignalKind.FLATTEN,
}


def tick_from_proto(message: proto.Tick) -> Tick:
    return Tick(
        symbol=message.symbol,
        price=to_decimal(message.price.value),
        size=to_decimal(message.size.value),
        side=_SIDE_MAP.get(message.side, Side.BUY),
        exchange_timestamp=from_timestamp(message.exchange_timestamp),
        received_at=from_timestamp(message.received_at),
    )


def candle_from_proto(message: proto.Candle) -> Candle:
    return Candle(
        symbol=message.symbol,
        interval=proto.Interval.Name(message.interval),
        open=to_decimal(message.open.value),
        high=to_decimal(message.high.value),
        low=to_decimal(message.low.value),
        close=to_decimal(message.close.value),
        volume=to_decimal(message.volume.value),
        timestamp=from_timestamp(message.timestamp),
    )


def order_book_from_proto(message: proto.OrderBook) -> OrderBook:
    levels = [
        OrderBookLevel(price=to_decimal(level.price.value), size=to_decimal(level.size.value))
        for level in message.bids
    ]
    asks = [
        OrderBookLevel(price=to_decimal(level.price.value), size=to_decimal(level.size.value))
        for level in message.asks
    ]
    return OrderBook(
        symbol=message.symbol,
        bids=levels,
        asks=asks,
        timestamp=from_timestamp(message.timestamp),
    )


def fill_from_proto(message: proto.Fill) -> Fill:
    fee = to_decimal(message.fee.value) if message.HasField("fee") else None
    return Fill(
        order_id=message.order_id,
        symbol=message.symbol,
        side=_SIDE_MAP.get(message.side, Side.BUY),
        fill_price=to_decimal(message.fill_price.value),
        fill_quantity=to_decimal(message.fill_quantity.value),
        fee=fee,
        timestamp=from_timestamp(message.timestamp),
    )


def context_from_proto(message: proto.StrategyContext) -> StrategyContext:
    positions: List[Position] = []
    for pos in message.positions:
        positions.append(
            Position(
                symbol=pos.symbol,
                side=_SIDE_MAP.get(pos.side, None),
                quantity=to_decimal(pos.quantity.value),
                entry_price=to_decimal(pos.entry_price.value) if pos.HasField("entry_price") else None,
                unrealized_pnl=to_decimal(pos.unrealized_pnl.value),
                updated_at=from_timestamp(pos.updated_at),
            )
        )
    return StrategyContext(positions=positions)


def signal_to_proto(signal: Signal) -> proto.Signal:
    kind_name = f"KIND_{signal.kind.name}"
    proto_signal = proto.Signal(
        symbol=signal.symbol,
        kind=kind_name,
        confidence=signal.confidence,
    )
    if signal.stop_loss is not None:
        proto_signal.stop_loss.value = from_decimal(signal.stop_loss)
    if signal.take_profit is not None:
        proto_signal.take_profit.value = from_decimal(signal.take_profit)
    if signal.note:
        proto_signal.note = signal.note
    return proto_signal


def signals_to_proto(signals: Iterable[Signal]) -> proto.SignalList:
    return proto.SignalList(signals=[signal_to_proto(sig) for sig in signals])


def init_response_from_strategy(result: StrategyInitResult) -> proto.InitResponse:
    return proto.InitResponse(
        symbols=result.symbols,
        success=result.success,
        error_message=result.error_message,
    )


def parse_config(config_json: str) -> dict:
    if not config_json:
        return {}
    return json.loads(config_json)


def pack_config(config: dict) -> str:
    return json.dumps(config or {})


def apply_config(strategy, config_json: str) -> StrategyInitResult:
    config = parse_config(config_json)
    return strategy.on_init(config)
