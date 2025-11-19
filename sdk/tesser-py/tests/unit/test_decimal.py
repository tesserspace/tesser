from decimal import Decimal

from tesser.conversions import signal_to_proto
from tesser.models import Signal, SignalKind


def test_signal_conversion_round_trip():
    signal = Signal(
        symbol="BTC-USD",
        kind=SignalKind.ENTER_LONG,
        confidence=0.5,
        stop_loss=Decimal("100"),
        take_profit=Decimal("140"),
        note="demo",
    )
    proto = signal_to_proto(signal)
    assert proto.symbol == signal.symbol
    assert proto.note == "demo"
    assert proto.stop_loss.value == "100"
