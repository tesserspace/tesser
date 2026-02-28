import { invoke } from "@tauri-apps/api/core";
import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

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

                      const ws = new WebSocket(ref.ws_url, ref.auth_token);
                      ws.binaryType = "arraybuffer";

                      let expectedSeq = 1n;

                      const pull = () => {
                        ws.send(
                          JSON.stringify({
                            type: "streams.pull",
                            stream_id: ref.stream_id,
                            next_seq: Number(expectedSeq),
                            max_bytes: 256 * 1024,
                            correlation_id: "ui.demo",
                            request_id: crypto.randomUUID(),
                          }),
                        );
                      };

                      ws.onopen = () => pull();
                      ws.onmessage = (event) => {
                        if (typeof event.data === "string") {
                          try {
                            const msg = JSON.parse(event.data) as {
                              type: string;
                              reason_code?: string;
                              seq?: number;
                              code?: string;
                              message?: string;
                            };
                            if (msg.type === "streams.closed") {
                              setDemoLog((s) => s + `\n[CLOSED] ${msg.reason_code} seq=${msg.seq}`);
                              ws.close();
                            } else if (msg.type === "streams.error") {
                              setDemoLog((s) => s + `\n[ERROR] ${msg.code}: ${msg.message}`);
                              ws.close();
                            }
                          } catch {
                            setDemoLog((s) => s + `\n[TEXT] ${event.data}`);
                          }
                          return;
                        }

                        const buf = new Uint8Array(event.data as ArrayBuffer);
                        if (buf.length < 32) {
                          setDemoLog((s) => s + `\n[BINARY] too short: ${buf.length}`);
                          return;
                        }
                        const magic = new TextDecoder().decode(buf.slice(0, 4));
                        const kind = buf[4];
                        const seq = new DataView(buf.buffer, buf.byteOffset + 8, 8).getBigUint64(
                          0,
                          false,
                        );
                        const payload = buf.slice(32);
                        if (magic !== "TSR1") {
                          setDemoLog((s) => s + `\n[BINARY] bad magic: ${magic}`);
                          return;
                        }
                        if (kind === 1) {
                          setDemoLog(
                            (s) =>
                              s +
                              `\n[CHUNK seq=${seq.toString()}] ${new TextDecoder().decode(payload)}`,
                          );
                          expectedSeq = seq + 1n;
                          pull();
                        } else if (kind === 2) {
                          setDemoLog((s) => s + `\n[EOF seq=${seq.toString()}]`);
                        } else {
                          setDemoLog((s) => s + `\n[BINARY] unknown kind=${kind}`);
                        }
                      };
                      ws.onerror = () => {
                        setDemoLog((s) => s + "\n[WS ERROR]");
                      };
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
