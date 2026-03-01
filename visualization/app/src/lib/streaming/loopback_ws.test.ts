import { describe, expect, it } from "vitest";

import { pullLoopbackWsStream, type WsCloseEvent, type WsLike, type WsMessageEvent } from "./loopback_ws";

function uuidToBytes(uuid: string): Uint8Array {
  const hex = uuid.replace(/-/g, "");
  if (hex.length !== 32) throw new Error("uuid must be 16 bytes");
  const out = new Uint8Array(16);
  for (let i = 0; i < 16; i++) {
    out[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}

function makeFrame(args: {
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

describe("pullLoopbackWsStream", () => {
  it("pulls chunks then eof then closed without races", async () => {
    const ws = new FakeWs();
    const streamId = "00112233-4455-6677-8899-aabbccddeeff";

    const p = pullLoopbackWsStream({
      ws,
      streamId,
      correlationId: "c1",
      maxBytes: 256 * 1024,
      requestId: "r1",
    });

    ws.open();

    // First pull request.
    await new Promise((r) => setTimeout(r, 0));
    expect(ws.sent.length).toBe(1);
    ws.emitMessage(
      makeFrame({
        kind: 1,
        seq: 1n,
        streamId,
        payload: new TextEncoder().encode("abc"),
      }),
    );

    // Second pull request.
    await new Promise((r) => setTimeout(r, 0));
    expect(ws.sent.length).toBe(2);
    ws.emitMessage(makeFrame({ kind: 2, seq: 2n, streamId }));
    ws.emitMessage(
      JSON.stringify({
        type: "streams.closed",
        stream_id: streamId,
        reason_code: "eof",
        seq: 2,
      }),
    );

    const out = await p;
    expect(new TextDecoder().decode(out.bytes)).toBe("abc");
    expect(out.eofSeq).toBe(2n);
  });
});
