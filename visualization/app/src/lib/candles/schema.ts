import type { Table } from "apache-arrow";

export type CandlesOhlcv = {
  ts_ms: Float64Array;
  open: Float64Array;
  high: Float64Array;
  low: Float64Array;
  close: Float64Array;
  volume: Float64Array;
};

function getColumn(table: Table, name: string) {
  const col = table.getChild(name);
  if (!col) throw new Error(`missing column: ${name}`);
  return col;
}

export function decodeCandlesOhlcvF64V1(table: Table): CandlesOhlcv {
  const tsCol = getColumn(table, "ts_ms");
  const openCol = getColumn(table, "open");
  const highCol = getColumn(table, "high");
  const lowCol = getColumn(table, "low");
  const closeCol = getColumn(table, "close");
  const volumeCol = getColumn(table, "volume");

  const n = table.numRows;
  const ts_ms = new Float64Array(n);
  const open = new Float64Array(n);
  const high = new Float64Array(n);
  const low = new Float64Array(n);
  const close = new Float64Array(n);
  const volume = new Float64Array(n);

  for (let i = 0; i < n; i++) {
    const ts = tsCol.get(i);
    if (typeof ts === "bigint") ts_ms[i] = Number(ts);
    else if (typeof ts === "number") ts_ms[i] = ts;
    else throw new Error(`unexpected ts_ms type at row ${i}`);

    const o = openCol.get(i);
    const h = highCol.get(i);
    const l = lowCol.get(i);
    const c = closeCol.get(i);
    const v = volumeCol.get(i);

    if (typeof o !== "number") throw new Error(`unexpected open type at row ${i}`);
    if (typeof h !== "number") throw new Error(`unexpected high type at row ${i}`);
    if (typeof l !== "number") throw new Error(`unexpected low type at row ${i}`);
    if (typeof c !== "number") throw new Error(`unexpected close type at row ${i}`);
    if (typeof v !== "number") throw new Error(`unexpected volume type at row ${i}`);

    open[i] = o;
    high[i] = h;
    low[i] = l;
    close[i] = c;
    volume[i] = v;
  }

  return { ts_ms, open, high, low, close, volume };
}
