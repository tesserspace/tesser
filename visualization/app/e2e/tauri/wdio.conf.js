import os from "os";
import path from "path";
import { spawn, spawnSync } from "child_process";
import fs from "fs";
import http from "http";
import net from "net";
import { fileURLToPath } from "url";

const __dirname = fileURLToPath(new URL(".", import.meta.url));

function appBinaryPath() {
  const bin = process.platform === "win32" ? "tesser-visualization.exe" : "tesser-visualization";
  return path.resolve(__dirname, "../../src-tauri/target/debug", bin);
}

let tauriDriver;
let exit = false;

const TAURI_DRIVER_PORT = Number.parseInt(process.env.TAURI_DRIVER_PORT ?? "4444", 10);
const TAURI_NATIVE_DRIVER_PORT = Number.parseInt(
  process.env.TAURI_NATIVE_DRIVER_PORT ?? String(TAURI_DRIVER_PORT + 1),
  10,
);

export const config = {
  host: "127.0.0.1",
  port: TAURI_DRIVER_PORT,
  specs: ["./test/specs/**/*.e2e.js"],
  maxInstances: 1,
  capabilities: [
    {
      maxInstances: 1,
      "tauri:options": {
        application: appBinaryPath(),
      },
    },
  ],
  reporters: ["spec"],
  framework: "mocha",
  mochaOpts: {
    ui: "bdd",
    timeout: 120_000,
  },

  onPrepare: () => {
    const r = spawnSync("pnpm", ["tauri", "build", "--debug", "--no-bundle"], {
      cwd: path.resolve(__dirname, "../.."),
      stdio: "inherit",
      shell: true,
    });
    if (r.error) throw r.error;
    if (typeof r.status === "number" && r.status !== 0) {
      throw new Error(`pnpm tauri build failed (status=${r.status})`);
    }
  },

  beforeSession: async () => {
    const tauriDriverPath = process.env.TAURI_DRIVER_PATH
      ? path.resolve(process.env.TAURI_DRIVER_PATH)
      : path.resolve(os.homedir(), ".cargo", "bin", "tauri-driver");

    tauriDriver = spawn(
      tauriDriverPath,
      [
        "--port",
        String(TAURI_DRIVER_PORT),
        "--native-port",
        String(TAURI_NATIVE_DRIVER_PORT),
      ],
      {
        stdio: [null, process.stdout, process.stderr],
      },
    );

    tauriDriver.on("error", (error) => {
      console.error("tauri-driver error:", error);
      process.exit(1);
    });
    tauriDriver.on("exit", (code) => {
      if (!exit) {
        console.error("tauri-driver exited with code:", code);
        process.exit(1);
      }
    });

    await waitForWebDriver({
      host: "127.0.0.1",
      port: TAURI_DRIVER_PORT,
      timeoutMs: 10_000,
    });
  },

  afterSession: () => {
    closeTauriDriver();
  },

  afterTest: async (test, _context, { passed }) => {
    if (passed) return;
    const artifactsDir = path.resolve(__dirname, "artifacts");
    fs.mkdirSync(artifactsDir, { recursive: true });
    const safeTitle = String(test?.title ?? "test")
      .replaceAll(/[^\w.-]+/g, "_")
      .slice(0, 80);
    const outPath = path.join(artifactsDir, `${Date.now()}-${safeTitle}.png`);
    await browser.saveScreenshot(outPath);
  },
};

function closeTauriDriver() {
  exit = true;
  tauriDriver?.kill();
}

function onShutdown(fn) {
  const cleanup = () => {
    try {
      fn();
    } finally {
      process.exit(1);
    }
  };

  process.once("SIGINT", cleanup);
  process.once("SIGTERM", cleanup);
  process.once("SIGHUP", cleanup);
}

onShutdown(closeTauriDriver);

function waitForPort({ host, port, timeoutMs }) {
  const startedAt = Date.now();
  return new Promise((resolve, reject) => {
    const tick = () => {
      const socket = new net.Socket();
      socket.setTimeout(500);
      socket.once("connect", () => {
        socket.destroy();
        resolve();
      });
      socket.once("timeout", () => socket.destroy());
      socket.once("error", () => socket.destroy());
      socket.once("close", () => {
        if (Date.now() - startedAt >= timeoutMs) {
          reject(new Error(`timeout waiting for ${host}:${port}`));
          return;
        }
        setTimeout(tick, 100);
      });
      socket.connect(port, host);
    };
    tick();
  });
}

async function waitForWebDriver({ host, port, timeoutMs }) {
  await waitForPort({ host, port, timeoutMs });

  const startedAt = Date.now();
  // WebDriver servers should respond to GET /status; poll it to reduce session-start flakiness.
  // https://www.w3.org/TR/webdriver/#dfn-status
  // eslint-disable-next-line no-constant-condition
  while (true) {
    if (Date.now() - startedAt >= timeoutMs) {
      throw new Error(`timeout waiting for WebDriver status at ${host}:${port}`);
    }
    const ok = await getWebDriverStatus({ host, port });
    if (ok) return;
    await new Promise((r) => setTimeout(r, 100));
  }
}

function getWebDriverStatus({ host, port }) {
  return new Promise((resolve) => {
    const req = http.get(
      {
        host,
        port,
        path: "/status",
        timeout: 500,
      },
      (res) => {
        if (res.statusCode !== 200) {
          res.resume();
          resolve(false);
          return;
        }
        let body = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => {
          body += chunk;
        });
        res.on("end", () => {
          try {
            JSON.parse(body);
            resolve(true);
          } catch {
            resolve(false);
          }
        });
      },
    );
    req.on("timeout", () => {
      req.destroy();
      resolve(false);
    });
    req.on("error", () => resolve(false));
  });
}
