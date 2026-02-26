export type Candle = {
  t: number; // unix seconds (UTC)
  o: number;
  h: number;
  l: number;
  c: number;
  v: number;
};

export type TradeSide = 'BUY' | 'SELL';

export type Trade = {
  time: number; // unix seconds
  side: TradeSide;
  price: number;
  quantity: number; // base units
  fee: number; // quote currency
  targetWeightAfter: number;
  realizedPnl?: number;
};

export type EquityPoint = {
  time: number; // unix seconds
  equity: number;
  targetWeight: number;
};

export type BacktestOptions = {
  initialEquity: number;
  feeBps: number;
  slippageBps: number;
  allowShort: boolean;
};

export type BacktestMetrics = {
  startTime: number; // unix seconds
  endTime: number; // unix seconds
  bars: number;
  trades: number;
  totalReturnPct: number;
  cagrPct: number | null;
  maxDrawdownPct: number;
  sharpe: number | null;
  feesPaid: number;
  realizedPnl: number;
};

export type BacktestResult = {
  equity: EquityPoint[];
  trades: Trade[];
  metrics: BacktestMetrics;
};
