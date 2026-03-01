import { describe, expect, it } from "vitest";

import { tableFromArrays, tableToIPC } from "apache-arrow";

import { candlesQueryAndDecode } from "./client";
import type { WsCloseEvent, WsLike, WsMessageEvent } from "@/lib/streaming/loopback_ws";

function uuidToBytes(uuid: string): Uint8Array {
  const hex = uuid.replace(/-/g, "");
  if (hex.length !== 32) throw new Error("uuid must be 16 bytes");
  const out = new Uint8Array(16);
  for (let i = 0; i < 16; i++) {
    out[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}

function makeTsr1Frame(args: {
  kind: 1 | 2;
  seq: bigint;
  streamId: string;
  payload?: Uint8Array;
}): ArrayBuffer {
  const payload = args.payload ?? new Uint8Array();
  const out = new Uint8Array(32 + payload.length);
  out.set(new TextEncoder().encode("TSR1"), 0);
  out[4] = args.kind;
  out[5] = 0;
  out[6] = 0;
  out[7] = 32;
  new DataView(out.buffer).setBigUint64(8, args.seq, false);
  out.set(uuidToBytes(args.streamId), 16);
  out.set(payload, 32);
  return out.buffer;
}

class FakeWs implements WsLike {
  readyState = 0;
  sent: string[] = [];

  private listeners = {
    open: new Set<() => void>(),
    message: new Set<(ev: WsMessageEvent) => void>(),
    error: new Set<() => void>(),
    close: new Set<(ev: WsCloseEvent) => void>(),
  };

  open(): void {
    this.readyState = 1;
    for (const fn of this.listeners.open) fn();
  }

  emitMessage(data: unknown): void {
    for (const fn of this.listeners.message) fn({ data });
  }

  emitClose(ev: WsCloseEvent = {}): void {
    this.readyState = 3;
    for (const fn of this.listeners.close) fn(ev);
  }

  send(data: string): void {
    this.sent.push(data);
  }

  close(): void {
    this.emitClose({ code: 1000, reason: "closed" });
  }

  addEventListener(type: "open", listener: () => void): void;
  addEventListener(type: "message", listener: (ev: WsMessageEvent) => void): void;
  addEventListener(type: "error", listener: () => void): void;
  addEventListener(type: "close", listener: (ev: WsCloseEvent) => void): void;
  addEventListener(type: any, listener: any): void {
    switch (type) {
      case "open":
        this.listeners.open.add(listener);
        return;
      case "message":
        this.listeners.message.add(listener);
        return;
      case "error":
        this.listeners.error.add(listener);
        return;
      case "close":
        this.listeners.close.add(listener);
        return;
      default:
        throw new Error(`unknown event type: ${String(type)}`);
    }
  }

  removeEventListener(type: "open", listener: () => void): void;
  removeEventListener(type: "message", listener: (ev: WsMessageEvent) => void): void;
  removeEventListener(type: "error", listener: () => void): void;
  removeEventListener(type: "close", listener: (ev: WsCloseEvent) => void): void;
  removeEventListener(type: any, listener: any): void {
    switch (type) {
      case "open":
        this.listeners.open.delete(listener);
        return;
      case "message":
        this.listeners.message.delete(listener);
        return;
      case "error":
        this.listeners.error.delete(listener);
        return;
      case "close":
        this.listeners.close.delete(listener);
        return;
      default:
        throw new Error(`unknown event type: ${String(type)}`);
    }
  }
}

describe("candlesQueryAndDecode", () => {
  it("pulls Arrow IPC bytes and decodes ohlcv columns", async () => {
    const streamId = "00112233-4455-6677-8899-aabbccddeeff";

    const table = tableFromArrays({
      ts_ms: new BigInt64Array([0n, 1000n]),
      open: new Float64Array([1, 2]),
      high: new Float64Array([2, 3]),
      low: new Float64Array([0.5, 1.5]),
      close: new Float64Array([1.2, 1.8]),
      volume: new Float64Array([10, 20]),
    });
    const arrowBytes = tableToIPC(table);

    const ws = new FakeWs();
    const createWs = () => ws;

    const invoke = async (cmd: string): Promise<any> => {
      expect(cmd).toBe("candles_query");
      return {
        meta: {
          correlation_id: "c1",
          manifest_hash: "m1",
          lod_profile: "candles_ohlcv_v1",
          lod_level: 0,
          bucket_ms: 1000,
          points_returned: 2,
          data_source: "raw_fallback",
          cache: "miss",
        },
        stream_ref: {
          stream_id: streamId,
          format: "arrow_ipc_stream",
          schema_id: "candles.ohlcv.f64.v1",
          transport: {
            kind: "loopback_ws",
            url: "ws://127.0.0.1:1",
            auth_token: "token",
            token_in: "sec-websocket-protocol",
            expires_at_ms: 9999999999999,
          },
        },
      };
    };

    const p = candlesQueryAndDecode({
      invoke,
      createWs,
      req: {
        envelope: {
          protocol_version: "tesser.viz.ipc.v1",
          correlation_id: "c1",
          request_id: "r1",
        },
        dataset_id: "ds1",
        range: { start_ms: 0, end_ms: 2000 },
        target_points: 100,
      },
    });

    ws.open();

    // First pull -> chunk
    await new Promise((r) => setTimeout(r, 0));
    const pull1 = JSON.parse(ws.sent[0]) as { type: string; next_seq: number };
    expect(pull1.type).toBe("streams.pull");
    expect(pull1.next_seq).toBe(1);
    ws.emitMessage(
      makeTsr1Frame({
        kind: 1,
        seq: 1n,
        streamId,
        payload: new Uint8Array(arrowBytes),
      }),
    );

    // Second pull -> eof + closed
    await new Promise((r) => setTimeout(r, 0));
    const pull2 = JSON.parse(ws.sent[1]) as { type: string; next_seq: number };
    expect(pull2.next_seq).toBe(2);
    ws.emitMessage(makeTsr1Frame({ kind: 2, seq: 2n, streamId }));
    ws.emitMessage(
      JSON.stringify({
        type: "streams.closed",
        stream_id: streamId,
        reason_code: "eof",
        seq: 2,
      }),
    );

    const out = await p;
    expect(out.meta.points_returned).toBe(2);
    expect(out.candles.ts_ms[0]).toBe(0);
    expect(out.candles.ts_ms[1]).toBe(1000);
    expect(out.candles.open[1]).toBe(2);
    expect(out.candles.high[0]).toBe(2);
    expect(out.candles.low[0]).toBe(0.5);
    expect(out.candles.close[0]).toBe(1.2);
    expect(out.candles.volume[1]).toBe(20);
  });
});

