import asyncio

import grpc
import pytest

from tesser import Runner, Signal, SignalKind, Strategy
from tesser.models import StrategyContext, Tick
from tesser.protos import tesser_pb2 as proto
from tesser.protos import tesser_pb2_grpc


class DummyStrategy(Strategy):
    def __init__(self):
        super().__init__(name="dummy", symbol="BTC-USD")

    async def on_tick(self, context: StrategyContext, tick: Tick):
        return [Signal(symbol=tick.symbol, kind=SignalKind.ENTER_LONG)]


@pytest.mark.asyncio
async def test_runner_serves_grpc(tmp_path):
    strategy = DummyStrategy()
    runner = Runner(strategy, host="127.0.0.1", port=50080)
    task = asyncio.create_task(runner.serve())
    await asyncio.sleep(0.2)

    try:
        async with grpc.aio.insecure_channel("127.0.0.1:50080") as channel:
            stub = tesser_pb2_grpc.StrategyServiceStub(channel)
            init = await stub.Initialize(proto.InitRequest(config_json="{}"))
            assert init.success

            hb = await stub.Heartbeat(proto.HeartbeatRequest())
            assert hb.healthy
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
