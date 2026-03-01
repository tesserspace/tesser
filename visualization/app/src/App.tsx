import { invoke } from "@tauri-apps/api/core";
import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { pullLoopbackWsStream, type WsLike } from "@/lib/streaming/loopback_ws";
import { CandlesPanel } from "@/components/candles/CandlesPanel";

type ProtocolInfo = {
  protocol_version: string;
  app_version: string;
  platform: string;
};

type TransportInfo = {
  ws_url: string;
};

type DemoStreamRef = {
  stream_id: string;
  ws_url: string;
  auth_token: string;
  token_in: string;
  expires_at_ms: number;
};

function App() {
  const [protocolInfo, setProtocolInfo] = useState<ProtocolInfo | null>(null);
  const [transportInfo, setTransportInfo] = useState<TransportInfo | null>(null);
  const [demoStreamRef, setDemoStreamRef] = useState<DemoStreamRef | null>(null);
  const [demoLog, setDemoLog] = useState<string>("");
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    (async () => {
      try {
        const info = await invoke<ProtocolInfo>("protocol_get_info");
        setProtocolInfo(info);
      } catch (e) {
        setError(String(e));
      }
    })();
  }, []);

  return (
    <div className="h-screen w-screen bg-background text-foreground">
      <div className="mx-auto flex h-full max-w-6xl flex-col gap-4 p-6">
        <div className="flex items-center justify-between">
          <div className="space-y-1">
            <div className="text-xl font-semibold">Tesser Visualization</div>
            <div className="text-sm text-muted-foreground">
              Local-first backtest visualization for crypto.
            </div>
          </div>
          <Button
            variant="secondary"
            onClick={async () => {
              setError(null);
              try {
                const info = await invoke<TransportInfo>("transport_start");
                setTransportInfo(info);
              } catch (e) {
                setError(String(e));
              }
            }}
          >
            Start Transport
          </Button>
        </div>

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center justify-between">
              <span>Demo Stream</span>
              <div className="flex gap-2">
                <Button
                  variant="outline"
                  onClick={async () => {
                    setError(null);
                    setDemoLog("");
                    try {
                      const ref = await invoke<DemoStreamRef>("debug_open_demo_stream");
                      setDemoStreamRef(ref);

                      if (ref.token_in !== "sec-websocket-protocol") {
                        throw new Error(`unsupported token_in: ${ref.token_in}`);
                      }
                      if (Date.now() > ref.expires_at_ms) {
                        throw new Error("demo stream auth token expired");
                      }

                      const ws = new WebSocket(ref.ws_url, ref.auth_token);
                      ws.binaryType = "arraybuffer";

                      try {
                        await pullLoopbackWsStream({
                          ws: ws as unknown as WsLike,
                          streamId: ref.stream_id,
                          correlationId: "ui.demo",
                          maxBytes: 256 * 1024,
                          requestId: crypto.randomUUID(),
                          onChunk: ({ seq, payload }) => {
                            setDemoLog(
                              (s) =>
                                s +
                                `\n[CHUNK seq=${seq.toString()}] ${new TextDecoder().decode(payload)}`,
                            );
                          },
                          onEof: ({ seq }) => {
                            setDemoLog((s) => s + `\n[EOF seq=${seq.toString()}]`);
                          },
                          onClosed: ({ reasonCode, seq }) => {
                            setDemoLog(
                              (s) => s + `\n[CLOSED] ${reasonCode} seq=${seq ?? "?"}`,
                            );
                          },
                        });
                      } finally {
                        ws.close();
                      }
                    } catch (e) {
                      setError(String(e));
                    }
                  }}
                >
                  Open Demo Stream
                </Button>
              </div>
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-2 text-sm">
            <div>
              <span className="text-muted-foreground">stream_id:</span>{" "}
              {demoStreamRef?.stream_id ?? "—"}
            </div>
            <div>
              <span className="text-muted-foreground">ws_url:</span> {demoStreamRef?.ws_url ?? "—"}
            </div>
            <div className="rounded-md border p-2 font-mono text-xs whitespace-pre-wrap h-40 overflow-auto">
              {demoLog || "—"}
            </div>
          </CardContent>
        </Card>

        {error ? (
          <Card>
            <CardHeader>
              <CardTitle className="text-destructive">Error</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="whitespace-pre-wrap text-sm">{error}</div>
            </CardContent>
          </Card>
        ) : null}

        <Tabs defaultValue="runs" className="flex-1">
          <TabsList>
            <TabsTrigger value="runs">Runs</TabsTrigger>
            <TabsTrigger value="candles" data-testid="tab-candles">
              Candles
            </TabsTrigger>
            <TabsTrigger value="datasets">Datasets</TabsTrigger>
            <TabsTrigger value="compare">Compare</TabsTrigger>
            <TabsTrigger value="downloads">Downloads</TabsTrigger>
          </TabsList>

          <TabsContent value="runs" className="mt-4">
            <Card>
              <CardHeader>
                <CardTitle>Host</CardTitle>
              </CardHeader>
              <CardContent className="space-y-2 text-sm">
                <div>
                  <span className="text-muted-foreground">Protocol:</span>{" "}
                  {protocolInfo?.protocol_version ?? "—"}
                </div>
                <div>
                  <span className="text-muted-foreground">App:</span>{" "}
                  {protocolInfo?.app_version ?? "—"}
                </div>
                <div>
                  <span className="text-muted-foreground">Platform:</span>{" "}
                  {protocolInfo?.platform ?? "—"}
                </div>
                <div>
                  <span className="text-muted-foreground">WS:</span>{" "}
                  {transportInfo?.ws_url ?? "not started"}
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="datasets" className="mt-4">
            <Card>
              <CardHeader>
                <CardTitle>Datasets</CardTitle>
              </CardHeader>
              <CardContent className="text-sm text-muted-foreground">
                Coming soon: dataset manifests, health checks, and imports.
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="candles" className="mt-4">
            <CandlesPanel />
          </TabsContent>

          <TabsContent value="compare" className="mt-4">
            <Card>
              <CardHeader>
                <CardTitle>Compare</CardTitle>
              </CardHeader>
              <CardContent className="text-sm text-muted-foreground">
                Coming soon: multi-run comparisons with metrics version gating.
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="downloads" className="mt-4">
            <Card>
              <CardHeader>
                <CardTitle>Downloads</CardTitle>
              </CardHeader>
              <CardContent className="text-sm text-muted-foreground">
                Coming soon: downloads with progress, cancel, retry, and resume.
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}

export default App;
