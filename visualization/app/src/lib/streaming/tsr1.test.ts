import { describe, expect, it } from "vitest";

import { parseTsr1Frame } from "./tsr1";

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
}): Uint8Array {
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
  return out;
}

describe("parseTsr1Frame", () => {
  it("parses chunk frames", () => {
    const streamId = "00112233-4455-6677-8899-aabbccddeeff";
    const frame = makeFrame({
      kind: 1,
      seq: 1n,
      streamId,
      payload: new TextEncoder().encode("hello"),
    });
    const parsed = parseTsr1Frame(frame);
    expect(parsed.kind).toBe("chunk");
    expect(parsed.seq).toBe(1n);
    expect(parsed.streamId).toBe(streamId);
    expect(new TextDecoder().decode(parsed.payload)).toBe("hello");
  });

  it("rejects bad magic", () => {
    const bytes = new Uint8Array(32);
    bytes.set(new TextEncoder().encode("NOPE"), 0);
    expect(() => parseTsr1Frame(bytes)).toThrow(/bad magic/i);
  });
});
