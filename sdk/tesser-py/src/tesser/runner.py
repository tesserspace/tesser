from __future__ import annotations

import asyncio
import os
import signal
from typing import Optional

import grpc

from .protos import tesser_pb2_grpc
from .service import StrategyServiceImpl
from .strategy import Strategy
from .utils.logging import configure_logging


class Runner:
    def __init__(self, strategy: Strategy, host: str = "0.0.0.0", port: int = 50051):
        self.strategy = strategy
        self.host = host
        self.port = port
        self._server: Optional[grpc.aio.Server] = None

    async def serve(self):
        configure_logging()
        server = grpc.aio.server()
        tesser_pb2_grpc.add_StrategyServiceServicer_to_server(
            StrategyServiceImpl(self.strategy), server
        )
        server.add_insecure_port(f"{self.host}:{self.port}")
        self._server = server
        try:
            await server.start()
            print(f"Strategy '{self.strategy.name}' listening on {self.host}:{self.port}")
            await self._graceful_wait(server)
        finally:
            await server.stop(0)
            self._server = None

    async def _graceful_wait(self, server: grpc.aio.Server):
        loop = asyncio.get_event_loop()
        stop_event = asyncio.Event()

        def _handle_stop(*_):
            stop_event.set()

        if os.name != "nt":
            loop.add_signal_handler(signal.SIGINT, _handle_stop)
            loop.add_signal_handler(signal.SIGTERM, _handle_stop)
        else:  # Windows lacks universal signal support
            loop.create_task(self._wait_for_keyboard(stop_event))
        await stop_event.wait()
        await server.stop(5)
        self._server = None

    async def _wait_for_keyboard(self, stop_event: asyncio.Event):
        loop = asyncio.get_running_loop()
        fut = loop.run_in_executor(None, lambda: input())
        await fut
        stop_event.set()
