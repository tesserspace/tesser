import { clamp } from './indicators';
import type {
  BacktestOptions,
  BacktestResult,
  Candle,
  EquityPoint,
  Trade,
} from './types';

type StrategyContext = {
  i: number;
  bar: Candle;
  close: readonly number[];
  equity: number;
  positionWeight: number;
};

export type StrategyFn = (
  ctx: StrategyContext,
  helpers: Record<string, unknown>,
) => number | null | undefined;

function isoDateFromUnixSeconds(seconds: number): string {
  return new Date(seconds * 1000).toISOString().slice(0, 10);
}

function computeMaxDrawdownPct(equity: readonly number[]): number {
  let peak = -Infinity;
  let maxDd = 0;
  for (const value of equity) {
    if (value > peak) peak = value;
    if (peak > 0) {
      const dd = (peak - value) / peak;
      if (dd > maxDd) maxDd = dd;
    }
  }
  return maxDd * 100;
}

function computeSharpe(dailyReturns: readonly number[]): number | null {
  if (dailyReturns.length < 2) return null;
  let sum = 0;
  for (const r of dailyReturns) sum += r;
  const mean = sum / dailyReturns.length;
  let variance = 0;
  for (const r of dailyReturns) variance += (r - mean) * (r - mean);
  variance /= Math.max(1, dailyReturns.length - 1);
  const std = Math.sqrt(variance);
  if (std === 0) return null;
  return (Math.sqrt(252) * mean) / std;
}

export function runBacktest(params: {
  candles: readonly Candle[];
  strategy: StrategyFn;
  options: BacktestOptions;
  helpers?: Record<string, unknown>;
}): BacktestResult {
  const { candles, strategy, options } = params;
  const helpers = params.helpers ?? {};

  if (candles.length === 0) {
    return {
      equity: [],
      trades: [],
      metrics: {
        startIso: '',
        endIso: '',
        bars: 0,
        trades: 0,
        totalReturnPct: 0,
        cagrPct: null,
        maxDrawdownPct: 0,
        sharpe: null,
        feesPaid: 0,
        realizedPnl: 0,
      },
    };
  }

  const initialEquity = options.initialEquity;
  let cash = initialEquity;
  let positionUnits = 0;

  let avgEntryPrice = 0;
  let positionSide: 'LONG' | 'SHORT' | 'FLAT' = 'FLAT';

  const trades: Trade[] = [];
  const equitySeries: EquityPoint[] = [];
  const closes: number[] = [];

  let feesPaid = 0;
  let realizedPnl = 0;
  let targetWeight = 0;

  for (let i = 0; i < candles.length; i++) {
    const bar = candles[i]!;
    closes.push(bar.c);

    const equity = cash + positionUnits * bar.c;
    const positionWeight = equity === 0 ? 0 : (positionUnits * bar.c) / equity;

    let nextTarget = targetWeight;
    const out = strategy({ i, bar, close: closes, equity, positionWeight }, helpers);
    if (typeof out === 'number' && Number.isFinite(out)) nextTarget = out;

    nextTarget = clamp(nextTarget, options.allowShort ? -1 : 0, 1);

    if (nextTarget !== targetWeight) {
      const desiredValue = nextTarget * equity;
      const desiredUnits = bar.c === 0 ? 0 : desiredValue / bar.c;
      const deltaUnits = desiredUnits - positionUnits;

      const epsilon = 1e-12;
      if (Math.abs(deltaUnits) > epsilon) {
        const isBuy = deltaUnits > 0;
        const slip = options.slippageBps / 10000;
        const execPrice = bar.c * (1 + (isBuy ? slip : -slip));
        const notional = Math.abs(deltaUnits) * execPrice;
        const fee = (options.feeBps / 10000) * notional;

        cash -= deltaUnits * execPrice;
        cash -= fee;
        positionUnits += deltaUnits;
        feesPaid += fee;

        let tradeRealized: number | undefined;
        const deltaQty = Math.abs(deltaUnits);

        if (positionSide === 'FLAT') {
          positionSide = positionUnits > 0 ? 'LONG' : positionUnits < 0 ? 'SHORT' : 'FLAT';
          avgEntryPrice = execPrice;
        } else if (positionSide === 'LONG') {
          if (isBuy) {
            const prevQty = Math.abs(positionUnits - deltaUnits);
            const newQty = prevQty + deltaQty;
            avgEntryPrice = newQty === 0 ? 0 : (avgEntryPrice * prevQty + execPrice * deltaQty) / newQty;
          } else {
            const qtyClosed = Math.min(deltaQty, Math.abs(positionUnits - deltaUnits));
            tradeRealized = (execPrice - avgEntryPrice) * qtyClosed;
            realizedPnl += tradeRealized;
            if (positionUnits <= 0) {
              positionSide = positionUnits < 0 ? 'SHORT' : 'FLAT';
              avgEntryPrice = positionSide === 'FLAT' ? 0 : execPrice;
            }
          }
        } else if (positionSide === 'SHORT') {
          if (!isBuy) {
            const prevQty = Math.abs(positionUnits - deltaUnits);
            const newQty = prevQty + deltaQty;
            avgEntryPrice = newQty === 0 ? 0 : (avgEntryPrice * prevQty + execPrice * deltaQty) / newQty;
          } else {
            const qtyClosed = Math.min(deltaQty, Math.abs(positionUnits - deltaUnits));
            tradeRealized = (avgEntryPrice - execPrice) * qtyClosed;
            realizedPnl += tradeRealized;
            if (positionUnits >= 0) {
              positionSide = positionUnits > 0 ? 'LONG' : 'FLAT';
              avgEntryPrice = positionSide === 'FLAT' ? 0 : execPrice;
            }
          }
        }

        trades.push({
          time: bar.t,
          side: isBuy ? 'BUY' : 'SELL',
          price: execPrice,
          quantity: Math.abs(deltaUnits),
          fee,
          targetWeightAfter: nextTarget,
          realizedPnl: tradeRealized,
        });
      }

      targetWeight = nextTarget;
    }

    const equityAfter = cash + positionUnits * bar.c;
    equitySeries.push({ time: bar.t, equity: equityAfter, targetWeight });
  }

  const equityValues = equitySeries.map((p) => p.equity);
  const dailyReturns: number[] = [];
  for (let i = 1; i < equityValues.length; i++) {
    const prev = equityValues[i - 1]!;
    const next = equityValues[i]!;
    dailyReturns.push(prev === 0 ? 0 : next / prev - 1);
  }

  const startIso = isoDateFromUnixSeconds(candles[0]!.t);
  const endIso = isoDateFromUnixSeconds(candles[candles.length - 1]!.t);
  const totalReturnPct =
    initialEquity === 0 ? 0 : ((equityValues[equityValues.length - 1]! / initialEquity) - 1) * 100;

  const startSec = candles[0]!.t;
  const endSec = candles[candles.length - 1]!.t;
  const years = Math.max(0, (endSec - startSec) / (365.25 * 24 * 60 * 60));
  const cagrPct =
    years > 0 && initialEquity > 0
      ? (Math.pow(equityValues[equityValues.length - 1]! / initialEquity, 1 / years) - 1) * 100
      : null;

  return {
    equity: equitySeries,
    trades,
    metrics: {
      startIso,
      endIso,
      bars: candles.length,
      trades: trades.length,
      totalReturnPct,
      cagrPct,
      maxDrawdownPct: computeMaxDrawdownPct(equityValues),
      sharpe: computeSharpe(dailyReturns),
      feesPaid,
      realizedPnl,
    },
  };
}
