import { invoke } from "@tauri-apps/api/core";
import { useEffect, useMemo, useState } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { CandlesCanvas } from "@/components/candles/CandlesCanvas";
import { makeEnvelope } from "@/lib/tauri/envelope";
import { createBrowserWebSocket } from "@/lib/tauri/websocket";
import { candlesQueryAndDecode, type CandlesQueryMeta } from "@/lib/candles/client";
import type { CandlesOhlcv } from "@/lib/candles/schema";

type DatasetPreview = {
  dataset_id: string;
  time_range?: { start_ms: number; end_ms: number } | null;
};

type ProtocolInfo = { protocol_version: string };

export function CandlesPanel() {
  const [protocolVersion, setProtocolVersion] = useState<string | null>(null);
  const [datasets, setDatasets] = useState<DatasetPreview[]>([]);
  const [selected, setSelected] = useState<string>("");
  const [loading, setLoading] = useState(false);
  const [meta, setMeta] = useState<CandlesQueryMeta | null>(null);
  const [candles, setCandles] = useState<CandlesOhlcv | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    (async () => {
      try {
        const info = await invoke<ProtocolInfo>("protocol_get_info");
        setProtocolVersion(info.protocol_version);
      } catch (e) {
        setError(String(e));
      }
    })();
  }, []);

  const selectedPreview = useMemo(
    () => datasets.find((d) => d.dataset_id === selected) ?? null,
    [datasets, selected],
  );

  async function refreshDatasets() {
    if (!protocolVersion) return;
    setError(null);
    const envelope = makeEnvelope({ protocolVersion, correlationId: "ui.datasets.list" });
    const list = await invoke<DatasetPreview[]>("datasets_list", { req: { envelope } });
    setDatasets(list);
    if (!selected && list.length > 0) setSelected(list[0].dataset_id);
  }

  async function createSynthetic() {
    if (!protocolVersion) return;
    setError(null);
    const dataset_id = "crypto.synthetic.spot.demo.series.1s.v1";
    const envelope = makeEnvelope({ protocolVersion, correlationId: "ui.datasets.create" });
    await invoke("datasets_create_synthetic", { req: { envelope, dataset_id } });
    await refreshDatasets();
    setSelected(dataset_id);
  }

  async function loadCandles() {
    if (!protocolVersion) return;
    if (!selected) return;

    setLoading(true);
    setError(null);
    setMeta(null);
    setCandles(null);

    try {
      const correlationId = "ui.candles.query";
      const envelope = makeEnvelope({ protocolVersion, correlationId });

      const timeRange = selectedPreview?.time_range ?? null;
      const range = timeRange
        ? { start_ms: timeRange.start_ms, end_ms: timeRange.end_ms }
        : { start_ms: 0, end_ms: 86_400_000 };

      const target_points = Math.max(
        64,
        Math.ceil(window.innerWidth * (window.devicePixelRatio || 1)),
      );

      const out = await candlesQueryAndDecode({
        invoke,
        createWs: createBrowserWebSocket,
        req: {
          envelope,
          dataset_id: selected,
          range,
          target_points,
          prefer_tiles: true,
          allow_raw_fallback: true,
        },
      });

      setMeta(out.meta);
      setCandles(out.candles);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    if (!protocolVersion) return;
    refreshDatasets().catch((e) => setError(String(e)));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [protocolVersion]);

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center justify-between">
            <span>Candles</span>
            <div className="flex gap-2">
              <Button
                variant="secondary"
                onClick={() => refreshDatasets()}
                disabled={!protocolVersion}
                data-testid="candles-refresh"
              >
                Refresh
              </Button>
              <Button
                variant="outline"
                onClick={() => createSynthetic()}
                disabled={!protocolVersion}
                data-testid="candles-create-synthetic"
              >
                Create Synthetic Dataset
              </Button>
              <Button
                onClick={() => loadCandles()}
                disabled={!protocolVersion || !selected || loading}
                data-testid="candles-load"
              >
                {loading ? "Loading…" : "Load"}
              </Button>
            </div>
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-3 text-sm">
          {error ? <div className="rounded-md border p-2 text-destructive">{error}</div> : null}

          <div className="grid gap-2 md:grid-cols-2">
            <label className="space-y-1">
              <div className="text-muted-foreground">dataset_id</div>
              <select
                className="w-full rounded-md border bg-background p-2"
                value={selected}
                onChange={(e) => setSelected(e.target.value)}
                data-testid="candles-dataset-select"
              >
                {datasets.map((d) => (
                  <option key={d.dataset_id} value={d.dataset_id}>
                    {d.dataset_id}
                  </option>
                ))}
              </select>
            </label>

            <div className="space-y-1">
              <div className="text-muted-foreground">meta</div>
              <div
                className="rounded-md border p-2 font-mono text-xs whitespace-pre-wrap"
                data-testid="candles-meta"
              >
                {meta ? JSON.stringify(meta, null, 2) : "—"}
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      <CandlesCanvas candles={candles} />
    </div>
  );
}
