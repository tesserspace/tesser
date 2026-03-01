import type { StreamRef, StreamTransportLoopbackWs, WsLike } from "@/lib/streaming/loopback_ws";
import { pullLoopbackWsStream } from "@/lib/streaming/loopback_ws";
import { decodeArrowIpcStream } from "@/lib/arrow/ipc";
import { decodeCandlesOhlcvF64V1, type CandlesOhlcv } from "@/lib/candles/schema";

export type RequestEnvelope = {
  protocol_version: string;
  correlation_id: string;
  request_id: string;
};

export type RangeMs = {
  start_ms: number;
  end_ms: number;
};

export type CandlesQueryRequest = {
  envelope: RequestEnvelope;
  dataset_id: string;
  range: RangeMs;
  target_points: number;
  prefer_tiles?: boolean;
  allow_raw_fallback?: boolean;
};

export type CandlesQueryMeta = {
  correlation_id: string;
  manifest_hash: string;
  lod_profile: string;
  lod_level: number;
  bucket_ms: number;
  points_returned: number;
  data_source: string;
  cache: string;
};

export type CandlesQueryResponse = {
  meta: CandlesQueryMeta;
  stream_ref: StreamRef;
};

export type InvokeFn = <T>(cmd: string, args?: Record<string, unknown>) => Promise<T>;
export type CreateWsFn = (args: { url: string; authToken: string; tokenIn: string }) => WsLike;

export async function candlesQueryAndDecode(args: {
  invoke: InvokeFn;
  createWs: CreateWsFn;
  req: CandlesQueryRequest;
  maxBytes?: number;
  signal?: AbortSignal;
}): Promise<{ meta: CandlesQueryMeta; candles: CandlesOhlcv }> {
  const { invoke, req, createWs, signal } = args;
  const maxBytes = args.maxBytes ?? 256 * 1024;

  const resp = await invoke<CandlesQueryResponse>("candles_query", { req });

  if (resp.stream_ref.format !== "arrow_ipc_stream") {
    throw new Error(`unsupported stream format: ${resp.stream_ref.format}`);
  }

  if (resp.stream_ref.schema_id !== "candles.ohlcv.f64.v1") {
    throw new Error(`unsupported schema_id: ${resp.stream_ref.schema_id ?? "null"}`);
  }

  const transport = resp.stream_ref.transport as StreamTransportLoopbackWs;
  if (transport.kind !== "loopback_ws") throw new Error("unsupported transport kind");
  if (transport.token_in !== "sec-websocket-protocol") {
    throw new Error(`unsupported token_in: ${transport.token_in}`);
  }

  const ws = createWs({
    url: transport.url,
    authToken: transport.auth_token,
    tokenIn: transport.token_in,
  });

  try {
    const { bytes } = await pullLoopbackWsStream({
      ws,
      streamId: resp.stream_ref.stream_id,
      correlationId: req.envelope.correlation_id,
      requestId: req.envelope.request_id,
      maxBytes,
      signal,
      maxTotalBytes: 64 * 1024 * 1024,
      collectBytes: true,
    });

    const table = decodeArrowIpcStream(bytes);
    const candles = decodeCandlesOhlcvF64V1(table);
    return { meta: resp.meta, candles };
  } finally {
    ws.close();
  }
}

