export function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

export function sma(series: readonly number[], period: number): number | null {
  if (!Number.isFinite(period) || period <= 0) return null;
  if (series.length < period) return null;
  let sum = 0;
  for (let i = series.length - period; i < series.length; i++) sum += series[i]!;
  return sum / period;
}

export function rsi(series: readonly number[], period: number): number | null {
  if (!Number.isFinite(period) || period <= 0) return null;
  if (series.length < period + 1) return null;

  let gains = 0;
  let losses = 0;
  for (let i = series.length - period; i < series.length; i++) {
    const change = series[i]! - series[i - 1]!;
    if (change >= 0) gains += change;
    else losses -= change;
  }

  const avgGain = gains / period;
  const avgLoss = losses / period;
  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return 100 - 100 / (1 + rs);
}

