#!/usr/bin/env python3
"""Minimal control-plane inspector demonstrating the gRPC bridge."""

from __future__ import annotations

import asyncio
import datetime as dt

import grpc

from tesser.protos import tesser_pb2, tesser_pb2_grpc

CONTROL_ADDR = "127.0.0.1:50052"


def fmt_decimal(value: tesser_pb2.Decimal | None) -> str:
    return value.value if value else "n/a"


def fmt_timestamp(ts: tesser_pb2.Timestamp | None) -> str:
    if ts is None or ts.seconds <= 0:
        return "n/a"
    return dt.datetime.fromtimestamp(ts.seconds).isoformat()


async def main() -> None:
    async with grpc.aio.insecure_channel(CONTROL_ADDR) as channel:
        stub = tesser_pb2_grpc.ControlServiceStub(channel)
        while True:
            portfolio = await stub.GetPortfolio(tesser_pb2.GetPortfolioRequest())
            status = await stub.GetStatus(tesser_pb2.GetStatusRequest())
            equity = fmt_decimal(portfolio.portfolio.equity if portfolio.portfolio else None)
            realized = fmt_decimal(
                portfolio.portfolio.realized_pnl if portfolio.portfolio else None
            )
            print(
                f"equity={equity} realized={realized} "
                f"liquidate_only={status.liquidate_only} "
                f"last_data={fmt_timestamp(status.last_data_timestamp)}"
            )
            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
