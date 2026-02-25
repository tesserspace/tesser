import type { Metadata } from 'next';
import type { ReactNode } from 'react';

export const metadata: Metadata = {
  title: { absolute: 'Backtest Playground | Tesser' },
  description:
    'Run a JavaScript strategy directly in the browser, powered by Tesser’s Rust/WASM backtest engine.',
};

export default function PlaygroundBacktestLayout({ children }: { children: ReactNode }) {
  return children;
}

