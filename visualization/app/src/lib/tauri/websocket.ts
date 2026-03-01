import type { WsLike } from "@/lib/streaming/loopback_ws";

export const createBrowserWebSocket = (args: {
  url: string;
  authToken: string;
  tokenIn: string;
}): WsLike => {
  const { url, authToken, tokenIn } = args;
  if (tokenIn !== "sec-websocket-protocol") {
    throw new Error(`unsupported token_in: ${tokenIn}`);
  }
  const ws = new WebSocket(url, authToken);
  ws.binaryType = "arraybuffer";
  return ws as unknown as WsLike;
};
