import type { BacktestOptions, BacktestResult, Candle } from './types';
import type { StrategyFn } from './strategy';

type WasmExports = {
  memory: WebAssembly.Memory;
  alloc: (len: number) => number;
  dealloc: (ptr: number, len: number) => void;
  backtest: (inputPtr: number, inputLen: number, outPtr: number) => void;
};

let wasmBytesPromise: Promise<ArrayBuffer> | null = null;

async function loadWasmBytes(): Promise<ArrayBuffer> {
  if (!wasmBytesPromise) {
    wasmBytesPromise = fetch('/wasm/tesser_backtest_wasm.wasm')
      .then((res) => {
        if (!res.ok) {
          throw new Error(`Failed to load WASM engine: ${res.status} ${res.statusText}`);
        }
        return res.arrayBuffer();
      })
      .catch((err) => {
        wasmBytesPromise = null;
        throw err;
      });
  }
  return wasmBytesPromise;
}

function decodeResult(buffer: Uint8Array): BacktestResult {
  const text = new TextDecoder().decode(buffer);
  const parsed = JSON.parse(text) as unknown;
  if (parsed && typeof parsed === 'object' && 'error' in (parsed as any)) {
    const message = (parsed as any).error;
    throw new Error(typeof message === 'string' ? message : 'Backtest failed.');
  }
  return parsed as BacktestResult;
}

export async function runBacktestWasm(params: {
  candles: readonly Candle[];
  options: BacktestOptions;
  strategy: StrategyFn;
  helpers?: Record<string, unknown>;
}): Promise<BacktestResult> {
  const bytes = await loadWasmBytes();

  const helpers = params.helpers ?? {};
  const close: number[] = [];
  let strategyError: Error | null = null;

  const compiledStrategy = params.strategy;
  const strategy_on_bar = (
    i: number,
    t: number,
    o: number,
    h: number,
    l: number,
    c: number,
    v: number,
    equity: number,
    positionWeight: number,
    targetWeight: number,
  ): number => {
    close.push(c);
    const ctx = {
      i,
      bar: { t, o, h, l, c, v },
      close,
      equity,
      positionWeight,
      targetWeight,
    };
    let out: unknown;
    try {
      out = compiledStrategy(ctx as any, helpers);
    } catch (err) {
      if (!strategyError) {
        const day = new Date(t * 1000).toISOString().slice(0, 10);
        const message = err instanceof Error ? err.message : String(err);
        strategyError = new Error(`Strategy error at bar ${i} (${day}): ${message}`);
      }
      return Number.NaN;
    }
    if (typeof out !== 'number' || !Number.isFinite(out)) return Number.NaN;
    return out;
  };

  const instance = await WebAssembly.instantiate(bytes, {
    tesser_playground: {
      strategy_on_bar,
    },
  });

  const exports = instance.instance.exports as unknown as WasmExports;
  if (!exports || typeof exports.alloc !== 'function' || typeof exports.backtest !== 'function') {
    throw new Error('Invalid WASM backtest engine exports.');
  }

  const input = {
    candles: params.candles,
    options: params.options,
  };

  const inputBytes = new TextEncoder().encode(JSON.stringify(input));

  const inputPtr = exports.alloc(inputBytes.length);
  new Uint8Array(exports.memory.buffer, inputPtr, inputBytes.length).set(inputBytes);

  const outPtr = exports.alloc(8);
  exports.backtest(inputPtr, inputBytes.length, outPtr);

  const outView = new Uint32Array(exports.memory.buffer, outPtr, 2);
  const resultPtr = outView[0]!;
  const resultLen = outView[1]!;

  const resultBytes = new Uint8Array(exports.memory.buffer, resultPtr, resultLen);
  const copied = new Uint8Array(resultBytes);

  exports.dealloc(inputPtr, inputBytes.length);
  exports.dealloc(outPtr, 8);
  exports.dealloc(resultPtr, resultLen);

  if (strategyError) throw strategyError;
  return decodeResult(copied);
}
