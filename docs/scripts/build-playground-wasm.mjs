import { mkdirSync, copyFileSync, existsSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { execFileSync } from 'node:child_process';
import { statSync, readdirSync } from 'node:fs';

const repoRoot = resolve(new URL('..', import.meta.url).pathname, '..');
const cargo = process.env.CARGO ?? 'cargo';
const rustup = process.env.RUSTUP ?? 'rustup';

const outPath = resolve(repoRoot, 'docs/public/wasm/tesser_backtest_wasm.wasm');
const targetDir = resolve(repoRoot, 'target/playground-wasm');
const builtWasm = resolve(
  targetDir,
  'wasm32-unknown-unknown/release/tesser_backtest_wasm.wasm',
);

function latestMtimeMs(path) {
  const stat = statSync(path);
  if (!stat.isDirectory()) return stat.mtimeMs;
  let latest = stat.mtimeMs;
  for (const entry of readdirSync(path, { withFileTypes: true })) {
    if (entry.name.startsWith('.')) continue;
    const child = resolve(path, entry.name);
    latest = Math.max(latest, latestMtimeMs(child));
  }
  return latest;
}

function run(cmd, args, options = {}) {
  execFileSync(cmd, args, {
    cwd: repoRoot,
    stdio: 'inherit',
    ...options,
  });
}

function ensureWasmTarget() {
  // This is idempotent.
  run(rustup, ['target', 'add', 'wasm32-unknown-unknown']);
}

function buildWasm() {
  run(
    cargo,
    [
      'build',
      '-p',
      'tesser-backtest-wasm',
      '--release',
      '--target',
      'wasm32-unknown-unknown',
    ],
    {
      env: {
        ...process.env,
        CARGO_TARGET_DIR: targetDir,
      },
    },
  );
}

const inputs = [
  resolve(repoRoot, 'tesser-backtest-core/Cargo.toml'),
  resolve(repoRoot, 'tesser-backtest-core/src'),
  resolve(repoRoot, 'tesser-backtest-wasm/Cargo.toml'),
  resolve(repoRoot, 'tesser-backtest-wasm/src'),
];

const needsBuild = (() => {
  if (!existsSync(outPath)) return true;
  const outTime = statSync(outPath).mtimeMs;
  const inTime = Math.max(...inputs.map((p) => latestMtimeMs(p)));
  return inTime > outTime;
})();

if (needsBuild) {
  console.log('[playground-wasm] building WASM backtest engine...');
  ensureWasmTarget();
  buildWasm();
  mkdirSync(dirname(outPath), { recursive: true });
  copyFileSync(builtWasm, outPath);
  console.log(`[playground-wasm] wrote ${outPath}`);
} else {
  console.log('[playground-wasm] WASM engine up-to-date, skipping build.');
}
