import { parseTsr1Frame } from "./tsr1";

export type StreamRef = {
  stream_id: string;
  format: string;
  transport: StreamTransport;
  schema_id?: string | null;
};

export type StreamTransport = StreamTransportLoopbackWs;

export type StreamTransportLoopbackWs = {
  kind: "loopback_ws";
  url: string;
  auth_token: string;
  token_in: string;
  expires_at_ms: number;
};

export type StreamsPullRequest = {
  type: "streams.pull";
  stream_id: string;
  next_seq: number;
  max_bytes: number;
  correlation_id: string;
  request_id: string;
};

export type StreamsCancelRequest = {
  type: "streams.cancel";
  stream_id: string;
  correlation_id: string;
  request_id: string;
};

export type StreamsClosedMessage = {
  type: "streams.closed";
  stream_id: string;
  reason_code: string;
  seq?: number;
  correlation_id?: string;
  request_id?: string;
};

export type StreamsErrorMessage = {
  type: "streams.error";
  stream_id: string;
  code: string;
  message: string;
  terminal: boolean;
  seq?: number;
  correlation_id?: string;
  request_id?: string;
};

export type WsMessageEvent = { data: unknown };
export type WsCloseEvent = { code?: number; reason?: string };
export type WsLike = {
  readyState: number;
  send(data: string): void;
  close(): void;
  addEventListener(type: "open", listener: () => void): void;
  addEventListener(type: "message", listener: (ev: WsMessageEvent) => void): void;
  addEventListener(type: "error", listener: () => void): void;
  addEventListener(type: "close", listener: (ev: WsCloseEvent) => void): void;
  removeEventListener(type: "open", listener: () => void): void;
  removeEventListener(type: "message", listener: (ev: WsMessageEvent) => void): void;
  removeEventListener(type: "error", listener: () => void): void;
  removeEventListener(type: "close", listener: (ev: WsCloseEvent) => void): void;
};

export class StreamProtocolError extends Error {
  readonly code: string;

  constructor(code: string, message: string) {
    super(message);
    this.name = "StreamProtocolError";
    this.code = code;
  }
}

function abortError(): Error {
  const e = new Error("aborted");
  e.name = "AbortError";
  return e;
}

function defaultRequestId(): string {
  const c = globalThis.crypto as Crypto | undefined;
  if (c?.randomUUID) return c.randomUUID();
  return `req_${Date.now()}_${Math.random().toString(16).slice(2)}`;
}

function waitForOpen(ws: WsLike, signal?: AbortSignal): Promise<void> {
  if (ws.readyState === 1) return Promise.resolve();
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(abortError());

    const onAbort = () => {
      cleanup();
      reject(abortError());
    };
    const onOpen = () => {
      cleanup();
      resolve();
    };
    const onClose = (ev: WsCloseEvent) => {
      cleanup();
      reject(new Error(`ws closed before open (code=${ev.code ?? "?"} reason=${ev.reason ?? "?"})`));
    };
    const onError = () => {
      cleanup();
      reject(new Error("ws error before open"));
    };
    const cleanup = () => {
      ws.removeEventListener("open", onOpen);
      ws.removeEventListener("close", onClose);
      ws.removeEventListener("error", onError);
      signal?.removeEventListener("abort", onAbort);
    };

    ws.addEventListener("open", onOpen);
    ws.addEventListener("close", onClose);
    ws.addEventListener("error", onError);
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

type WsQueue = {
  next(): Promise<unknown>;
  dispose(): void;
};

function createWsMessageQueue(ws: WsLike, signal?: AbortSignal): WsQueue {
  const queue: unknown[] = [];
  let pending:
    | { resolve: (v: unknown) => void; reject: (e: Error) => void }
    | null = null;
  let terminalError: Error | null = null;

  const onMessage = (ev: WsMessageEvent) => {
    if (pending) {
      const { resolve } = pending;
      pending = null;
      resolve(ev.data);
      return;
    }
    queue.push(ev.data);
  };
  const onClose = (ev: WsCloseEvent) => {
    terminalError = new Error(
      `ws closed (code=${ev.code ?? "?"} reason=${ev.reason ?? "?"})`,
    );
    if (pending) {
      const { reject } = pending;
      pending = null;
      reject(terminalError);
    }
  };
  const onError = () => {
    terminalError = new Error("ws error");
    if (pending) {
      const { reject } = pending;
      pending = null;
      reject(terminalError);
    }
  };
  const onAbort = () => {
    terminalError = abortError();
    if (pending) {
      const { reject } = pending;
      pending = null;
      reject(terminalError);
    }
  };

  ws.addEventListener("message", onMessage);
  ws.addEventListener("close", onClose);
  ws.addEventListener("error", onError);
  signal?.addEventListener("abort", onAbort);

  return {
    async next(): Promise<unknown> {
      if (signal?.aborted) throw abortError();
      if (queue.length > 0) return queue.shift();
      if (terminalError) throw terminalError;
      return await new Promise((resolve, reject) => {
        pending = { resolve, reject };
      });
    },
    dispose(): void {
      ws.removeEventListener("message", onMessage);
      ws.removeEventListener("close", onClose);
      ws.removeEventListener("error", onError);
      signal?.removeEventListener("abort", onAbort);
      pending = null;
      queue.length = 0;
      terminalError = new Error("disposed");
    },
  };
}

function tryParseServerTextMessage(text: string): StreamsClosedMessage | StreamsErrorMessage | null {
  try {
    const v = JSON.parse(text) as unknown;
    if (!v || typeof v !== "object") return null;
    const obj = v as Record<string, unknown>;
    if (obj.type === "streams.closed") return obj as StreamsClosedMessage;
    if (obj.type === "streams.error") return obj as StreamsErrorMessage;
    return null;
  } catch {
    return null;
  }
}

export type PullLoopOptions = {
  ws: WsLike;
  streamId: string;
  correlationId: string;
  maxBytes: number;
  requestId?: string;
  signal?: AbortSignal;
  collectBytes?: boolean;
  maxTotalBytes?: number;
  onChunk?: (args: { seq: bigint; payload: Uint8Array }) => void;
  onEof?: (args: { seq: bigint }) => void;
  onClosed?: (args: { reasonCode: string; seq?: number }) => void;
};

export async function pullLoopbackWsStream(
  options: PullLoopOptions,
): Promise<{ bytes: Uint8Array; eofSeq: bigint }> {
  const {
    ws,
    streamId,
    correlationId,
    maxBytes,
    signal,
    onChunk,
    onEof,
    onClosed,
    collectBytes = true,
    maxTotalBytes = 64 * 1024 * 1024,
  } = options;
  const requestId = options.requestId ?? defaultRequestId();

  await waitForOpen(ws, signal);
  const q = createWsMessageQueue(ws, signal);

  let expectedSeq = 1n;
  const chunks: Uint8Array[] = [];
  let eofSeq: bigint | null = null;
  let closed: StreamsClosedMessage | null = null;
  let totalBytes = 0;

  let cancelSent = false;
  const sendCancel = () => {
    if (cancelSent) return;
    cancelSent = true;
    if (ws.readyState !== 1) return;
    try {
      const cancel: StreamsCancelRequest = {
        type: "streams.cancel",
        stream_id: streamId,
        correlation_id: correlationId,
        request_id: requestId,
      };
      ws.send(JSON.stringify(cancel));
    } catch {
      // best-effort
    }
  };

  // The caller controls backpressure by awaiting this loop; we only pull again after a chunk arrives.
  try {
    signal?.addEventListener("abort", sendCancel, { once: true });
    while (eofSeq === null && closed === null) {
      if (expectedSeq > BigInt(Number.MAX_SAFE_INTEGER)) {
        throw new Error(
          `next_seq exceeds JS safe integer range: ${expectedSeq.toString()}`,
        );
      }
      const pull: StreamsPullRequest = {
        type: "streams.pull",
        stream_id: streamId,
        next_seq: Number(expectedSeq),
        max_bytes: maxBytes,
        correlation_id: correlationId,
        request_id: requestId,
      };
      ws.send(JSON.stringify(pull));

      while (true) {
        const msg = await q.next();

        if (typeof msg === "string") {
          const parsed = tryParseServerTextMessage(msg);
          if (parsed?.type === "streams.error") {
            if (parsed.stream_id !== streamId) continue;
            throw new StreamProtocolError(parsed.code, parsed.message);
          }
          if (parsed?.type === "streams.closed") {
            if (parsed.stream_id !== streamId) continue;
            closed = parsed;
            onClosed?.({ reasonCode: parsed.reason_code, seq: parsed.seq });
            break;
          }
          continue;
        }

        if (typeof Blob !== "undefined" && msg instanceof Blob) {
          const bytes = new Uint8Array(await msg.arrayBuffer());
          const frame = parseTsr1Frame(bytes);
          if (frame.streamId !== streamId)
            throw new Error(`unexpected stream_id: ${frame.streamId}`);
          if (frame.seq !== expectedSeq)
            throw new Error(
              `unexpected seq: got=${frame.seq} expected=${expectedSeq}`,
            );

	          if (frame.kind === "chunk") {
	            onChunk?.({ seq: frame.seq, payload: frame.payload });
	            if (collectBytes) {
	              totalBytes += frame.payload.length;
	              if (totalBytes > maxTotalBytes) {
	                throw new Error(
	                  `stream exceeded maxTotalBytes=${maxTotalBytes} while buffering`,
	                );
	              }
	              chunks.push(frame.payload);
	            }
	            expectedSeq = frame.seq + 1n;
	            break;
	          }

          eofSeq = frame.seq;
          onEof?.({ seq: frame.seq });
          break;
        }

        if (msg instanceof ArrayBuffer) {
          const frame = parseTsr1Frame(new Uint8Array(msg));
          if (frame.streamId !== streamId)
            throw new Error(`unexpected stream_id: ${frame.streamId}`);
          if (frame.seq !== expectedSeq)
            throw new Error(
              `unexpected seq: got=${frame.seq} expected=${expectedSeq}`,
            );

          if (frame.kind === "chunk") {
            onChunk?.({ seq: frame.seq, payload: frame.payload });
            if (collectBytes) {
              totalBytes += frame.payload.length;
              if (totalBytes > maxTotalBytes) {
                throw new Error(
                  `stream exceeded maxTotalBytes=${maxTotalBytes} while buffering`,
                );
              }
              chunks.push(frame.payload);
            }
            expectedSeq = frame.seq + 1n;
            break;
          }

          eofSeq = frame.seq;
          onEof?.({ seq: frame.seq });
          break;
        }

        if (msg instanceof Uint8Array) {
          const frame = parseTsr1Frame(msg);
          if (frame.streamId !== streamId)
            throw new Error(`unexpected stream_id: ${frame.streamId}`);
          if (frame.seq !== expectedSeq)
            throw new Error(
              `unexpected seq: got=${frame.seq} expected=${expectedSeq}`,
            );

          if (frame.kind === "chunk") {
            onChunk?.({ seq: frame.seq, payload: frame.payload });
            if (collectBytes) {
              totalBytes += frame.payload.length;
              if (totalBytes > maxTotalBytes) {
                throw new Error(
                  `stream exceeded maxTotalBytes=${maxTotalBytes} while buffering`,
                );
              }
              chunks.push(frame.payload);
            }
            expectedSeq = frame.seq + 1n;
            break;
          }

          eofSeq = frame.seq;
          onEof?.({ seq: frame.seq });
          break;
        }
      }
    }

    if (closed === null) {
      // Expect a terminal `streams.closed` text message.
      while (true) {
        const msg = await q.next();
        if (typeof msg !== "string") continue;
        const parsed = tryParseServerTextMessage(msg);
        if (parsed?.type === "streams.error") {
          if (parsed.stream_id !== streamId) continue;
          throw new StreamProtocolError(parsed.code, parsed.message);
        }
        if (parsed?.type === "streams.closed") {
          if (parsed.stream_id !== streamId) continue;
          onClosed?.({ reasonCode: parsed.reason_code, seq: parsed.seq });
          break;
        }
      }
    }
  } finally {
    signal?.removeEventListener("abort", sendCancel);
    q.dispose();
  }

  if (!collectBytes) return { bytes: new Uint8Array(), eofSeq: eofSeq ?? 0n };

  const total = chunks.reduce((n, c) => n + c.length, 0);
  const out = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) {
    out.set(c, off);
    off += c.length;
  }
  return { bytes: out, eofSeq: eofSeq ?? 0n };
}
