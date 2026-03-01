import { useEffect, useMemo, useRef } from "react";
import type { CandlesOhlcv } from "@/lib/candles/schema";

type Props = {
  candles: CandlesOhlcv | null;
};

function finiteMinMax(values: Float64Array): { min: number; max: number } {
  let min = Number.POSITIVE_INFINITY;
  let max = Number.NEGATIVE_INFINITY;
  for (let i = 0; i < values.length; i++) {
    const v = values[i];
    if (!Number.isFinite(v)) continue;
    if (v < min) min = v;
    if (v > max) max = v;
  }
  if (!Number.isFinite(min) || !Number.isFinite(max)) return { min: 0, max: 1 };
  if (min === max) return { min: min - 1, max: max + 1 };
  return { min, max };
}

export function CandlesCanvas({ candles }: Props) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const stats = useMemo(() => {
    if (!candles || candles.ts_ms.length === 0) return null;
    const { min, max } = finiteMinMax(candles.high);
    const { min: minLow } = finiteMinMax(candles.low);
    return { min: Math.min(minLow, min), max };
  }, [candles]);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const dpr = window.devicePixelRatio || 1;
    const cssWidth = canvas.clientWidth;
    const cssHeight = canvas.clientHeight;
    canvas.width = Math.max(1, Math.floor(cssWidth * dpr));
    canvas.height = Math.max(1, Math.floor(cssHeight * dpr));
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

    ctx.clearRect(0, 0, cssWidth, cssHeight);

    if (!candles || candles.ts_ms.length === 0 || !stats) {
      ctx.fillStyle = "rgba(148,163,184,0.8)";
      ctx.font = "12px ui-sans-serif, system-ui";
      ctx.fillText("No candles loaded", 8, 18);
      return;
    }

    const { min, max } = stats;
    const padX = 8;
    const padY = 8;
    const w = cssWidth - padX * 2;
    const h = cssHeight - padY * 2;
    const n = candles.ts_ms.length;
    const pxPerCandle = w / Math.max(1, n);
    const bodyWidth = Math.max(1, Math.min(8, pxPerCandle * 0.6));

    const yOf = (price: number) => {
      const t = (price - min) / (max - min);
      return padY + h * (1 - t);
    };

    ctx.lineWidth = 1;
    for (let i = 0; i < n; i++) {
      const o = candles.open[i];
      const c = candles.close[i];
      const hi = candles.high[i];
      const lo = candles.low[i];

      const xCenter = padX + (i + 0.5) * pxPerCandle;
      const x0 = xCenter - bodyWidth / 2;

      const yOpen = yOf(o);
      const yClose = yOf(c);
      const yHigh = yOf(hi);
      const yLow = yOf(lo);

      const up = c >= o;
      const bodyTop = Math.min(yOpen, yClose);
      const bodyBottom = Math.max(yOpen, yClose);

      ctx.strokeStyle = up ? "rgb(34,197,94)" : "rgb(239,68,68)";
      ctx.fillStyle = up ? "rgba(34,197,94,0.25)" : "rgba(239,68,68,0.25)";

      // Wick
      ctx.beginPath();
      ctx.moveTo(xCenter, yHigh);
      ctx.lineTo(xCenter, yLow);
      ctx.stroke();

      // Body
      const bodyHeight = Math.max(1, bodyBottom - bodyTop);
      ctx.fillRect(x0, bodyTop, bodyWidth, bodyHeight);
      ctx.strokeRect(x0, bodyTop, bodyWidth, bodyHeight);
    }
  }, [candles, stats]);

  return (
    <canvas
      ref={canvasRef}
      data-testid="candles-canvas"
      className="h-64 w-full rounded-md border bg-background"
    />
  );
}
