import type { Candle } from './types';

export type StrategyContext = {
  i: number;
  bar: Candle;
  close: readonly number[];
  equity: number;
  positionWeight: number;
  targetWeight: number;
};

export type StrategyFn = (
  ctx: StrategyContext,
  helpers: Record<string, unknown>,
) => number | null | undefined;

