'use client';

import type { SeriesMarker, UTCTimestamp } from 'lightweight-charts';
import {
  CandlestickSeries,
  ColorType,
  LineSeries,
  createChart,
  createSeriesMarkers,
} from 'lightweight-charts';
import { useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { runBacktest, type StrategyFn } from '@/lib/playground/backtest';
import { clamp, rsi, sma } from '@/lib/playground/indicators';
import type { BacktestOptions, BacktestResult, Candle } from '@/lib/playground/types';

type DatasetManifest = {
  source: { name: string; fetched_at_iso: string; note?: string };
  datasets: Array<{
    id: string;
    symbol: string;
    interval: string;
    limit: number;
    label: string;
    path: string;
  }>;
};

const defaultStrategyCode = `// Return target weight in [-1, 1] (or [0, 1] if short is disabled).
// Return null/undefined to keep the current position.
//
// ctx: { i, bar, close, equity, positionWeight }
// helpers: { sma, rsi, clamp }
(ctx, { sma }) => {
  const fast = sma(ctx.close, 20);
  const slow = sma(ctx.close, 50);
  if (fast == null || slow == null) return null;
  return fast > slow ? 1 : 0;
}`;

function formatNumber(value: number, digits = 2): string {
  if (!Number.isFinite(value)) return '—';
  return value.toLocaleString(undefined, { maximumFractionDigits: digits, minimumFractionDigits: digits });
}

function compileStrategy(code: string): { fn: StrategyFn | null; error: string | null } {
  try {
    const compiled = new Function(`\"use strict\"; return (${code});`)() as unknown;
    if (typeof compiled !== 'function') {
      return { fn: null, error: 'Strategy must evaluate to a function like: (ctx, helpers) => number | null' };
    }
    const fn: StrategyFn = (ctx, helpers) =>
      (compiled as (ctx: unknown, helpers: unknown) => unknown)(ctx, helpers) as number | null | undefined;
    return { fn, error: null };
  } catch (err) {
    return { fn: null, error: err instanceof Error ? err.message : 'Failed to compile strategy.' };
  }
}

export default function BrowserBacktestPlaygroundPage() {
  const [manifest, setManifest] = useState<DatasetManifest | null>(null);
  const [datasetPath, setDatasetPath] = useState<string>('');
  const [candles, setCandles] = useState<Candle[]>([]);

  const [strategyCode, setStrategyCode] = useState<string>(defaultStrategyCode);
  const [compileError, setCompileError] = useState<string | null>(null);
  const [runtimeError, setRuntimeError] = useState<string | null>(null);

  const [options, setOptions] = useState<BacktestOptions>({
    initialEquity: 10_000,
    feeBps: 5,
    slippageBps: 2,
    allowShort: false,
  });

  const [result, setResult] = useState<BacktestResult | null>(null);

  const candleChartEl = useRef<HTMLDivElement | null>(null);
  const equityChartEl = useRef<HTMLDivElement | null>(null);

  const helpers = useMemo(() => ({ sma, rsi, clamp }), []);

  useEffect(() => {
    let alive = true;
    void (async () => {
      try {
        const res = await fetch('/datasets/manifest.json', { cache: 'force-cache' });
        if (!res.ok) throw new Error(`Failed to load dataset manifest: ${res.status}`);
        const next = (await res.json()) as DatasetManifest;
        if (!alive) return;
        setManifest(next);
        setDatasetPath(next.datasets[0]?.path ?? '');
      } catch (err) {
        if (!alive) return;
        setRuntimeError(err instanceof Error ? err.message : 'Failed to load datasets.');
      }
    })();
    return () => {
      alive = false;
    };
  }, []);

  useEffect(() => {
    if (!datasetPath) return;
    let alive = true;
    void (async () => {
      try {
        setRuntimeError(null);
        setResult(null);
        const res = await fetch(datasetPath, { cache: 'force-cache' });
        if (!res.ok) throw new Error(`Failed to load dataset: ${res.status}`);
        const data = (await res.json()) as Candle[];
        if (!alive) return;
        setCandles(data);
      } catch (err) {
        if (!alive) return;
        setRuntimeError(err instanceof Error ? err.message : 'Failed to load dataset.');
      }
    })();
    return () => {
      alive = false;
    };
  }, [datasetPath]);

  const markers = useMemo((): SeriesMarker<UTCTimestamp>[] => {
    if (!result) return [];
    return result.trades.map((t) => ({
      time: t.time as UTCTimestamp,
      position: (t.side === 'BUY' ? 'belowBar' : 'aboveBar') as 'belowBar' | 'aboveBar',
      color: t.side === 'BUY' ? '#22c55e' : '#ef4444',
      shape: (t.side === 'BUY' ? 'arrowUp' : 'arrowDown') as 'arrowUp' | 'arrowDown',
      text: t.side === 'BUY' ? 'Buy' : 'Sell',
    }));
  }, [result]);

  useEffect(() => {
    if (!candleChartEl.current) return;
    if (candles.length === 0) return;

    const chart = createChart(candleChartEl.current, {
      layout: {
        background: { type: ColorType.Solid, color: 'transparent' },
        textColor: '#e4e4e7',
      },
      grid: {
        vertLines: { color: 'rgba(63, 63, 70, 0.4)' },
        horzLines: { color: 'rgba(63, 63, 70, 0.4)' },
      },
      rightPriceScale: { borderColor: 'rgba(63, 63, 70, 0.6)' },
      timeScale: { borderColor: 'rgba(63, 63, 70, 0.6)' },
      crosshair: { vertLine: { color: 'rgba(148, 163, 184, 0.5)' }, horzLine: { color: 'rgba(148, 163, 184, 0.5)' } },
      height: 520,
    });

    const series = chart.addSeries(CandlestickSeries, {
      upColor: '#22c55e',
      downColor: '#ef4444',
      borderUpColor: '#22c55e',
      borderDownColor: '#ef4444',
      wickUpColor: '#22c55e',
      wickDownColor: '#ef4444',
    });

    series.setData(
      candles.map((c) => ({
        time: c.t as UTCTimestamp,
        open: c.o,
        high: c.h,
        low: c.l,
        close: c.c,
      })),
    );
    const seriesMarkers = createSeriesMarkers(series, markers);
    chart.timeScale().fitContent();

    return () => {
      seriesMarkers.detach();
      chart.remove();
    };
  }, [candles, markers]);

  useEffect(() => {
    if (!equityChartEl.current) return;
    if (!result) return;

    const chart = createChart(equityChartEl.current, {
      layout: {
        background: { type: ColorType.Solid, color: 'transparent' },
        textColor: '#e4e4e7',
      },
      grid: {
        vertLines: { color: 'rgba(63, 63, 70, 0.4)' },
        horzLines: { color: 'rgba(63, 63, 70, 0.4)' },
      },
      rightPriceScale: { borderColor: 'rgba(63, 63, 70, 0.6)' },
      timeScale: { borderColor: 'rgba(63, 63, 70, 0.6)' },
      height: 220,
    });

    const series = chart.addSeries(LineSeries, { color: '#60a5fa', lineWidth: 2 });
    series.setData(
      result.equity.map((p) => ({
        time: p.time as UTCTimestamp,
        value: p.equity,
      })),
    );
    chart.timeScale().fitContent();

    return () => {
      chart.remove();
    };
  }, [result]);

  function onRun() {
    setRuntimeError(null);
    const compiled = compileStrategy(strategyCode);
    setCompileError(compiled.error);
    if (!compiled.fn) return;

    try {
      const next = runBacktest({
        candles,
        strategy: (ctx, h) => {
          try {
            return compiled.fn!(ctx, h);
          } catch (err) {
            const day = new Date(ctx.bar.t * 1000).toISOString().slice(0, 10);
            const message = err instanceof Error ? err.message : String(err);
            throw new Error(`Strategy error at bar ${ctx.i} (${day}): ${message}`);
          }
        },
        options,
        helpers,
      });
      setResult(next);
    } catch (err) {
      setRuntimeError(err instanceof Error ? err.message : 'Backtest failed.');
    }
  }

  const selectedDataset = manifest?.datasets.find((d) => d.path === datasetPath) ?? null;

  return (
    <div className="mx-auto w-full max-w-7xl px-6 pb-10 pt-28 text-zinc-100">
      <div className="mb-6">
        <h1 className="text-2xl font-semibold tracking-tight">Browser Backtest Playground</h1>
        <p className="mt-2 text-sm text-zinc-400">
          Signal-style strategies: return a target weight, the engine handles rebalancing.
        </p>
      </div>

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-[420px_1fr]">
        <div className="space-y-6">
          <section className="rounded-xl border border-zinc-800 bg-zinc-950/40 p-4">
            <div className="flex items-center justify-between">
              <h2 className="text-sm font-medium text-zinc-200">Dataset</h2>
              {manifest ? (
                <span className="text-xs text-zinc-500">
                  Source: {manifest.source.name} · Snapshot:{' '}
                  {new Date(manifest.source.fetched_at_iso).toISOString().slice(0, 10)}
                </span>
              ) : null}
            </div>
            <div className="mt-3 space-y-3">
              <label className="block text-xs text-zinc-400">Select</label>
              <select
                className="w-full rounded-lg border border-zinc-800 bg-black/40 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-zinc-700"
                value={datasetPath}
                onChange={(e) => setDatasetPath(e.target.value)}
              >
                {(manifest?.datasets ?? []).map((d) => (
                  <option key={d.id} value={d.path}>
                    {d.label}
                  </option>
                ))}
              </select>
              {manifest?.source.note ? (
                <div className="text-xs text-zinc-500">{manifest.source.note}</div>
              ) : null}

              <div className="grid grid-cols-2 gap-3">
                <Field label="Initial equity (USD)">
                  <input
                    className="w-full rounded-lg border border-zinc-800 bg-black/40 px-3 py-2 text-sm outline-none focus:border-zinc-700"
                    type="number"
                    value={options.initialEquity}
                    onChange={(e) =>
                      setOptions((o) => ({ ...o, initialEquity: Number(e.target.value) || 0 }))
                    }
                  />
                </Field>
                <Field label="Fee (bps)">
                  <input
                    className="w-full rounded-lg border border-zinc-800 bg-black/40 px-3 py-2 text-sm outline-none focus:border-zinc-700"
                    type="number"
                    value={options.feeBps}
                    onChange={(e) => setOptions((o) => ({ ...o, feeBps: Number(e.target.value) || 0 }))}
                  />
                </Field>
                <Field label="Slippage (bps)">
                  <input
                    className="w-full rounded-lg border border-zinc-800 bg-black/40 px-3 py-2 text-sm outline-none focus:border-zinc-700"
                    type="number"
                    value={options.slippageBps}
                    onChange={(e) =>
                      setOptions((o) => ({ ...o, slippageBps: Number(e.target.value) || 0 }))
                    }
                  />
                </Field>
                <Field label="Allow short">
                  <label className="flex h-[38px] items-center gap-2 rounded-lg border border-zinc-800 bg-black/40 px-3 text-sm">
                    <input
                      type="checkbox"
                      checked={options.allowShort}
                      onChange={(e) => setOptions((o) => ({ ...o, allowShort: e.target.checked }))}
                    />
                    <span className="text-zinc-200">Enabled</span>
                  </label>
                </Field>
              </div>

              <div className="text-xs text-zinc-500">
                {selectedDataset ? (
                  <>
                    <div>
                      Loaded: {selectedDataset.symbol} · {selectedDataset.interval} · {selectedDataset.limit} bars
                    </div>
                    <div className="mt-1">
                      Target weight is clamped to {options.allowShort ? '[-1, 1]' : '[0, 1]'}.
                    </div>
                  </>
                ) : null}
              </div>
            </div>
          </section>

          <section className="rounded-xl border border-zinc-800 bg-zinc-950/40 p-4">
            <div className="flex items-center justify-between">
              <h2 className="text-sm font-medium text-zinc-200">Strategy (JavaScript)</h2>
              <div className="flex gap-2">
                <button
                  className="rounded-lg border border-zinc-800 bg-black/40 px-3 py-1.5 text-xs text-zinc-200 hover:border-zinc-700"
                  onClick={() => {
                    setStrategyCode(defaultStrategyCode);
                    setCompileError(null);
                    setRuntimeError(null);
                    setResult(null);
                  }}
                >
                  Reset
                </button>
                <button
                  className="rounded-lg bg-blue-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-blue-500 disabled:opacity-40"
                  onClick={onRun}
                  disabled={candles.length === 0}
                >
                  Run backtest
                </button>
              </div>
            </div>
            <textarea
              className="mt-3 h-[260px] w-full resize-none rounded-lg border border-zinc-800 bg-black/40 p-3 font-mono text-xs leading-5 text-zinc-100 outline-none focus:border-zinc-700"
              value={strategyCode}
              onChange={(e) => setStrategyCode(e.target.value)}
              spellCheck={false}
            />
            {compileError ? <p className="mt-2 text-xs text-red-400">{compileError}</p> : null}
            {runtimeError ? <p className="mt-2 text-xs text-red-400">{runtimeError}</p> : null}
          </section>

          <section className="rounded-xl border border-zinc-800 bg-zinc-950/40 p-4">
            <h2 className="text-sm font-medium text-zinc-200">Results</h2>
            {result ? (
              <div className="mt-3 grid grid-cols-2 gap-3 text-sm">
                <Stat label="Period" value={`${result.metrics.startIso} → ${result.metrics.endIso}`} />
                <Stat label="Bars" value={`${result.metrics.bars}`} />
                <Stat label="Trades" value={`${result.metrics.trades}`} />
                <Stat label="Total return" value={`${formatNumber(result.metrics.totalReturnPct, 2)}%`} />
                <Stat
                  label="CAGR"
                  value={result.metrics.cagrPct == null ? '—' : `${formatNumber(result.metrics.cagrPct, 2)}%`}
                />
                <Stat label="Max drawdown" value={`${formatNumber(result.metrics.maxDrawdownPct, 2)}%`} />
                <Stat
                  label="Sharpe"
                  value={result.metrics.sharpe == null ? '—' : formatNumber(result.metrics.sharpe, 2)}
                />
                <Stat label="Fees paid" value={`$${formatNumber(result.metrics.feesPaid, 2)}`} />
              </div>
            ) : (
              <p className="mt-3 text-sm text-zinc-500">Run a backtest to see metrics and trade markers.</p>
            )}
          </section>
        </div>

        <div className="space-y-6">
          <section className="rounded-xl border border-zinc-800 bg-zinc-950/30 p-3">
            <div className="flex items-center justify-between px-1 pb-2">
              <div className="text-sm font-medium text-zinc-200">Candles</div>
              <div className="text-xs text-zinc-500">Markers show rebalances at close</div>
            </div>
            <div ref={candleChartEl} className="w-full" />
          </section>

          <section className="rounded-xl border border-zinc-800 bg-zinc-950/30 p-3">
            <div className="flex items-center justify-between px-1 pb-2">
              <div className="text-sm font-medium text-zinc-200">Equity</div>
              <div className="text-xs text-zinc-500">USD</div>
            </div>
            <div ref={equityChartEl} className="w-full" />
          </section>

          {result && result.trades.length > 0 ? (
            <section className="rounded-xl border border-zinc-800 bg-zinc-950/30 p-3">
              <div className="flex items-center justify-between px-1 pb-2">
                <div className="text-sm font-medium text-zinc-200">Trades</div>
                <div className="text-xs text-zinc-500">First 200 rows</div>
              </div>
              <div className="max-h-[420px] overflow-auto rounded-lg border border-zinc-800">
                <table className="w-full text-left text-xs">
                  <thead className="sticky top-0 bg-black/60 text-zinc-400 backdrop-blur">
                    <tr>
                      <th className="px-3 py-2">Time</th>
                      <th className="px-3 py-2">Side</th>
                      <th className="px-3 py-2">Price</th>
                      <th className="px-3 py-2">Qty</th>
                      <th className="px-3 py-2">Fee</th>
                      <th className="px-3 py-2">Target</th>
                      <th className="px-3 py-2">Realized</th>
                    </tr>
                  </thead>
                  <tbody className="text-zinc-200">
                    {result.trades.slice(0, 200).map((t, idx) => (
                      <tr key={`${t.time}-${idx}`} className="border-t border-zinc-900">
                        <td className="px-3 py-2">{new Date(t.time * 1000).toISOString().slice(0, 10)}</td>
                        <td className={`px-3 py-2 ${t.side === 'BUY' ? 'text-green-400' : 'text-red-400'}`}>
                          {t.side}
                        </td>
                        <td className="px-3 py-2">{formatNumber(t.price, 2)}</td>
                        <td className="px-3 py-2">{formatNumber(t.quantity, 6)}</td>
                        <td className="px-3 py-2">{formatNumber(t.fee, 2)}</td>
                        <td className="px-3 py-2">{formatNumber(t.targetWeightAfter, 2)}</td>
                        <td className="px-3 py-2">
                          {t.realizedPnl == null ? '—' : formatNumber(t.realizedPnl, 2)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          ) : null}
        </div>
      </div>
    </div>
  );
}

function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="space-y-1">
      <div className="text-xs text-zinc-400">{label}</div>
      {children}
    </div>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-zinc-800 bg-black/30 px-3 py-2">
      <div className="text-xs text-zinc-500">{label}</div>
      <div className="mt-0.5 text-sm text-zinc-100">{value}</div>
    </div>
  );
}
