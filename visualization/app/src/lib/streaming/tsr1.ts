export type Tsr1FrameKind = "chunk" | "eof";

export type Tsr1Frame = {
  kind: Tsr1FrameKind;
  seq: bigint;
  streamId: string;
  payload: Uint8Array;
};

function bytesToHex(bytes: Uint8Array): string {
  let out = "";
  for (const b of bytes) out += b.toString(16).padStart(2, "0");
  return out;
}

function uuidFromBytes(bytes16: Uint8Array): string {
  if (bytes16.length !== 16) throw new Error("uuid bytes must be 16 bytes");
  const hex = bytesToHex(bytes16);
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

export function parseTsr1Frame(frameBytes: Uint8Array): Tsr1Frame {
  if (frameBytes.length < 32) throw new Error("TSR1 frame too short");

  const magic = new TextDecoder().decode(frameBytes.slice(0, 4));
  if (magic !== "TSR1") throw new Error(`bad magic: ${magic}`);

  const kindByte = frameBytes[4];
  const headerLen = (frameBytes[6] << 8) | frameBytes[7];
  if (headerLen !== 32) throw new Error(`unexpected header_len: ${headerLen}`);

  const seq = new DataView(frameBytes.buffer, frameBytes.byteOffset + 8, 8).getBigUint64(0, false);
  const streamId = uuidFromBytes(frameBytes.slice(16, 32));
  const payload = frameBytes.slice(32);

  if (kindByte === 1) return { kind: "chunk", seq, streamId, payload };
  if (kindByte === 2) return { kind: "eof", seq, streamId, payload };
  throw new Error(`unknown frame kind: ${kindByte}`);
}

