from __future__ import annotations

import grpc

from .protos import tesser_pb2 as proto
from .protos import tesser_pb2_grpc


class StrategyServiceClient:
    """Thin async client for integration testing and monitoring."""

    def __init__(self, address: str):
        self.address = address
        self._channel: grpc.aio.Channel | None = None
        self._stub: tesser_pb2_grpc.StrategyServiceStub | None = None

    async def __aenter__(self):
        if self._channel is None:
            self._channel = grpc.aio.insecure_channel(self.address)
            self._stub = tesser_pb2_grpc.StrategyServiceStub(self._channel)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._channel:
            await self._channel.close()
            self._channel = None
            self._stub = None

    async def heartbeat(self) -> proto.HeartbeatResponse:
        if self._stub is None:
            raise RuntimeError("Client not connected")
        return await self._stub.Heartbeat(proto.HeartbeatRequest())
