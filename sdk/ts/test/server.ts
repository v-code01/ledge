// Test harness: build the Rust `ledge` binary once, then spawn it on an
// ephemeral port over a tmp data dir, poll `/healthz` until ready, and hand back
// a base URL + a kill handle. Used by the e2e suite to drive the real server.

import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = fileURLToPath(new URL(".", import.meta.url));
// sdk/ts/test -> repo root is three levels up.
const repoRoot = resolve(here, "..", "..", "..");
const binPath = join(repoRoot, "target", "debug", "ledge");

/** Build the `ledge` binary once (cargo is incremental; a no-op if up to date). */
export function buildServer(): void {
  const r = spawnSync("cargo", ["build", "--bin", "ledge"], {
    cwd: repoRoot,
    stdio: "inherit",
  });
  if (r.status !== 0) {
    throw new Error(`cargo build --bin ledge failed (status ${r.status})`);
  }
}

/** A running server: its base URL and a stop() that kills the process + tmp dir. */
export interface RunningServer {
  baseUrl: string;
  stop: () => Promise<void>;
}

function randomPort(): number {
  // Ephemeral-ish range; we retry on bind failure so collisions are harmless.
  return 20000 + Math.floor(Math.random() * 40000);
}

async function waitForHealth(baseUrl: string, deadlineMs: number): Promise<boolean> {
  while (Date.now() < deadlineMs) {
    try {
      const r = await fetch(`${baseUrl}/healthz`);
      if (r.ok) return true;
    } catch {
      // Not up yet; fall through to the backoff sleep.
    }
    await new Promise((res) => setTimeout(res, 50));
  }
  return false;
}

/**
 * Spawn the prebuilt server on a free port over a fresh tmp data dir and wait
 * for `/healthz`. Retries a few times on a port/startup race.
 */
export async function startServer(): Promise<RunningServer> {
  const attempts = 8;
  let lastErr: unknown;
  for (let i = 0; i < attempts; i++) {
    const port = randomPort();
    const dataDir = mkdtempSync(join(tmpdir(), "ledge-sdk-e2e-"));
    const addr = `127.0.0.1:${port}`;
    const baseUrl = `http://${addr}`;

    const child: ChildProcess = spawn(
      binPath,
      ["start", "--addr", addr, "--data-dir", dataDir],
      { stdio: ["ignore", "ignore", "inherit"], env: { ...process.env, RUST_LOG: "warn" } },
    );

    let exitedEarly = false;
    child.once("exit", () => {
      exitedEarly = true;
    });

    const ready = await waitForHealth(baseUrl, Date.now() + 10_000);

    if (ready && !exitedEarly) {
      const stop = async (): Promise<void> => {
        await new Promise<void>((res) => {
          if (child.exitCode !== null || child.signalCode !== null) {
            res();
            return;
          }
          child.once("exit", () => res());
          child.kill("SIGKILL");
        });
        rmSync(dataDir, { recursive: true, force: true });
      };
      return { baseUrl, stop };
    }

    // Failed to come up on this port; clean up and retry on a new one.
    child.kill("SIGKILL");
    rmSync(dataDir, { recursive: true, force: true });
    lastErr = new Error(`server failed to become healthy on ${addr}`);
  }
  throw lastErr ?? new Error("server failed to start");
}
