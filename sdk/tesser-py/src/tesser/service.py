from __future__ import annotations

import asyncio
import logging

import grpc

from .conversions import (
    candle_from_proto,
    context_from_proto,
    fill_from_proto,
    init_response_from_strategy,
    order_book_from_proto,
    parse_config,
    signal_to_proto,
    tick_from_proto,
)
from .models import StrategyContext
from .protos import tesser_pb2 as proto
from .protos import tesser_pb2_grpc
from .strategy import Strategy

logger = logging.getLogger(__name__)


class StrategyServiceImpl(tesser_pb2_grpc.StrategyServiceServicer):
    def __init__(self, strategy: Strategy):
        self.strategy = strategy

    async def Initialize(self, request, context):  # noqa: N802
        try:
            config = parse_config(request.config_json)
            result = self.strategy.on_init(config)
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("Strategy init failed: %s", exc)
            return proto.InitResponse(success=False, error_message=str(exc))
        return init_response_from_strategy(result)

    async def OnTick(self, request, context):  # noqa: N802
        return await self._handle_event(
            context=context,
            converter=tick_from_proto,
            handler=self.strategy.on_tick,
            proto_event=request.tick,
            proto_context=request.context,
        )

    async def OnCandle(self, request, context):  # noqa: N802
        return await self._handle_event(
            context=context,
            converter=candle_from_proto,
            handler=self.strategy.on_candle,
            proto_event=request.candle,
            proto_context=request.context,
        )

    async def OnOrderBook(self, request, context):  # noqa: N802
        return await self._handle_event(
            context=context,
            converter=order_book_from_proto,
            handler=self.strategy.on_order_book,
            proto_event=request.order_book,
            proto_context=request.context,
        )

    async def OnFill(self, request, context):  # noqa: N802
        return await self._handle_event(
            context=context,
            converter=fill_from_proto,
            handler=self.strategy.on_fill,
            proto_event=request.fill,
            proto_context=request.context,
        )

    async def Heartbeat(self, request, context):  # noqa: N802
        return proto.HeartbeatResponse(healthy=True, status_msg="OK")

    async def _handle_event(
        self,
        context: grpc.aio.ServicerContext,
        converter,
        handler,
        proto_event,
        proto_context,
    ) -> proto.SignalList:
        strategy_ctx: StrategyContext = context_from_proto(proto_context)
        try:
            event = converter(proto_event)
            result = handler(strategy_ctx, event)
            if asyncio.iscoroutine(result):
                signals = await result
            else:
                signals = result
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("Strategy hook raised: %s", exc)
            await context.abort(grpc.StatusCode.INTERNAL, str(exc))
            return proto.SignalList()

        proto_signals = [signal_to_proto(sig) for sig in list(signals or [])]
        return proto.SignalList(signals=proto_signals)
