#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const { loadWalletExplorerV3Snapshot } = require("../src/services/read_model/wallet_storage_v3");
const { createRestClient } = require("../src/services/transport/rest_client");
const { createRetryPolicy } = require("../src/services/transport/retry_policy");
const { createRateLimitGuard } = require("../src/services/transport/rate_limit_guard");
const { createClockSync } = require("../src/services/transport/clock_sync");

const ROOT = path.join(__dirname, "..");
const DEFAULT_MANIFEST = path.join(ROOT, "data", "wallet_explorer_v3", "manifest.json");
const DEFAULT_PROXY_FILE = path.join(ROOT, "data", "live_positions", "detector_proxies.txt");
const DEFAULT_OUT = path.join(ROOT, "data", "live_positions", "all_wallet_open_positions_live.json");

function readArg(name, fallback = null) {
  const prefix = `--${name}=`;
  const hit = process.argv.find((arg) => String(arg).startsWith(prefix));
  if (!hit) return fallback;
  return hit.slice(prefix.length);
}

function toInt(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : fallback;
}

function stableShard(wallet, shardCount) {
  let hash = 2166136261;
  const text = String(wallet || "");
  for (let i = 0; i < text.length; i += 1) {
    hash ^= text.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return Math.abs(hash >>> 0) % Math.max(1, shardCount);
}

function loadProxyRows(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return [];
  return fs
    .readFileSync(filePath, "utf8")
    .split(/\r?\n/)
    .map((line) => String(line || "").trim())
    .filter(Boolean)
    .filter((line) => !line.startsWith("#"));
}

function buildClients({
  baseUrl,
  proxyRows,
  includeDirect,
  transport,
  rpmCapPerIp,
  directRpmCap,
  rateLimitWindowMs,
  timeoutMs,
}) {
  const retryPolicy = createRetryPolicy({
    maxAttempts: 2,
    baseDelayMs: 250,
    maxDelayMs: 3000,
    jitterRatio: 0.2,
  });
  const clockSync = createClockSync();
  const clients = [];

  if (includeDirect) {
    clients.push({
      id: "direct",
      client: createRestClient({
        baseUrl,
        retryPolicy,
        rateLimitGuard: createRateLimitGuard({
          capacity: Math.max(0.1, (directRpmCap * rateLimitWindowMs) / 60000),
          refillWindowMs: rateLimitWindowMs,
          safetyMarginRatio: 0.9,
        }),
        clockSync,
        transport: "fetch",
        clientId: "open_positions_direct",
        logger: {
          warn() {},
        },
      }),
    });
  }

  proxyRows.forEach((proxyUrl, idx) => {
    clients.push({
      id: `proxy_${idx + 1}`,
      client: createRestClient({
        baseUrl,
        retryPolicy,
        rateLimitGuard: createRateLimitGuard({
          capacity: Math.max(0.1, (rpmCapPerIp * rateLimitWindowMs) / 60000),
          refillWindowMs: rateLimitWindowMs,
          safetyMarginRatio: 0.9,
        }),
        clockSync,
        transport,
        proxyUrl,
        clientId: `open_positions_proxy_${idx + 1}`,
        logger: {
          warn() {},
        },
      }),
    });
  });

  if (!clients.length) {
    throw new Error("no REST clients available");
  }

  let cursor = 0;
  function nextClient() {
    const entry = clients[cursor % clients.length];
    cursor += 1;
    return entry;
  }

  async function fetchAccount(wallet) {
    let lastError = null;
    for (let attempt = 0; attempt < clients.length; attempt += 1) {
      const entry = nextClient();
      try {
        const res = await entry.client.get("/account", {
          query: { account: wallet },
          timeoutMs,
          retryMaxAttempts: 1,
          cost: 1,
        });
        const data = res && res.payload && typeof res.payload.data === "object"
          ? res.payload.data
          : res && res.payload && typeof res.payload === "object"
          ? res.payload.data !== undefined
            ? res.payload.data
            : res.payload
          : {};
        return {
          clientId: entry.id,
          positionsCount: Math.max(
            0,
            Number(data.positions_count !== undefined ? data.positions_count : data.positionsCount) || 0
          ),
          updatedAt: Number(data.updated_at !== undefined ? data.updated_at : data.updatedAt) || null,
        };
      } catch (error) {
        lastError = error;
        const text = String(error && error.message ? error.message : error || "").toLowerCase();
        if (text.includes("404") && text.includes("account not found")) {
          return {
            clientId: entry.id,
            positionsCount: 0,
            updatedAt: Date.now(),
          };
        }
      }
    }
    throw lastError || new Error("account_fetch_failed");
  }

  return {
    clientsCount: clients.length,
    fetchAccount,
  };
}

async function main() {
  const manifestPath = String(readArg("manifest", DEFAULT_MANIFEST));
  const proxyFile = String(readArg("proxy-file", DEFAULT_PROXY_FILE));
  const outPath = String(readArg("out", DEFAULT_OUT));
  const progressPath = String(readArg("progress-out", `${outPath}.progress.json`));
  const apiBase = String(readArg("api-base", "https://api.pacifica.fi/api/v1"));
  const shardIndex = Math.max(0, toInt(readArg("shard-index", "0"), 0));
  const shardCount = Math.max(1, toInt(readArg("shard-count", "1"), 1));
  const proxyLimit = Math.max(0, toInt(readArg("proxy-limit", "64"), 64));
  const concurrency = Math.max(1, toInt(readArg("concurrency", "96"), 96));
  const timeoutMs = Math.max(1000, toInt(readArg("timeout-ms", "5000"), 5000));
  const rpmCapPerIp = Math.max(1, toInt(readArg("rpm-per-ip", "220"), 220));
  const directRpmCap = Math.max(1, toInt(readArg("direct-rpm", "60"), 60));
  const includeDirect = String(readArg("include-direct", "false")).toLowerCase() === "true";
  const limit = Math.max(0, toInt(readArg("limit", "0"), 0));
  const offset = Math.max(0, toInt(readArg("offset", "0"), 0));

  const snap = loadWalletExplorerV3Snapshot(manifestPath);
  const allWallets = (snap.rows || []).map((row) => String(row && row.wallet || "").trim()).filter(Boolean);
  const wallets = allWallets
    .filter((wallet) => stableShard(wallet, shardCount) === shardIndex)
    .slice(offset)
    .slice(0, limit > 0 ? limit : undefined);
  const proxyRows = loadProxyRows(proxyFile).slice(0, proxyLimit > 0 ? proxyLimit : undefined);
  const transport = "curl";

  const pool = buildClients({
    baseUrl: apiBase,
    proxyRows,
    includeDirect,
    transport,
    rpmCapPerIp,
    directRpmCap,
    rateLimitWindowMs: 60000,
    timeoutMs,
  });

  const startedAt = Date.now();
  let completed = 0;
  let walletsWithOpenPositions = 0;
  let totalOpenPositions = 0;
  let failed = 0;
  let lastUpdatedAt = 0;
  const failures = [];

  function writeProgress(done = false) {
    const now = Date.now();
    const payload = {
      ok: true,
      source: "pacifica_account_positions_count_live_census_progress",
      apiBase,
      manifestPath,
      proxyFile,
      proxiesUsed: proxyRows.length,
      includeDirect,
      transport,
      shardIndex,
      shardCount,
      scannedWallets: wallets.length,
      completedWallets: completed,
      failedWallets: failed,
      walletsWithOpenPositions,
      totalOpenPositions,
      startedAt,
      updatedAt: now,
      latestAccountUpdatedAt: lastUpdatedAt || null,
      done,
      failuresSample: failures.slice(0, 25),
    };
    fs.mkdirSync(path.dirname(progressPath), { recursive: true });
    fs.writeFileSync(progressPath, JSON.stringify(payload, null, 2));
  }

  async function workerLoop(workerId) {
    while (true) {
      const index = completed + failed;
      if (index >= wallets.length) return;
      const wallet = wallets[index];
      if (!wallet) return;
      completed += 1;
      try {
        const res = await pool.fetchAccount(wallet);
        if (res.positionsCount > 0) {
          walletsWithOpenPositions += 1;
          totalOpenPositions += res.positionsCount;
        }
        if (Number(res.updatedAt || 0) > lastUpdatedAt) {
          lastUpdatedAt = Number(res.updatedAt || 0);
        }
        if (completed % 250 === 0 || completed === wallets.length) {
          writeProgress(false);
          const elapsedSec = Math.max(1, Math.round((Date.now() - startedAt) / 1000));
          console.log(
            JSON.stringify({
              workerId,
              completed,
              total: wallets.length,
              walletsWithOpenPositions,
              totalOpenPositions,
              failed,
              rps: Number((completed / elapsedSec).toFixed(2)),
            })
          );
        }
      } catch (error) {
        failed += 1;
        failures.push({
          wallet,
          error: String(error && error.message ? error.message : error || "unknown_error").slice(0, 240),
        });
      }
    }
  }

  let cursor = 0;
  completed = 0;
  failed = 0;
  const queue = wallets.slice();
  async function queueWorker(workerId) {
    while (true) {
      const idx = cursor;
      if (idx >= queue.length) return;
      cursor += 1;
      const wallet = queue[idx];
      try {
        const res = await pool.fetchAccount(wallet);
        completed += 1;
        if (res.positionsCount > 0) {
          walletsWithOpenPositions += 1;
          totalOpenPositions += res.positionsCount;
        }
        if (Number(res.updatedAt || 0) > lastUpdatedAt) {
          lastUpdatedAt = Number(res.updatedAt || 0);
        }
        if (completed % 250 === 0 || completed === wallets.length) {
          const elapsedSec = Math.max(1, Math.round((Date.now() - startedAt) / 1000));
          console.log(
            JSON.stringify({
              workerId,
              completed,
              total: wallets.length,
              walletsWithOpenPositions,
              totalOpenPositions,
              failed,
              rps: Number((completed / elapsedSec).toFixed(2)),
            })
          );
        }
      } catch (error) {
        completed += 1;
        failed += 1;
        failures.push({
          wallet,
          error: String(error && error.message ? error.message : error || "unknown_error").slice(0, 240),
        });
        if (completed % 250 === 0 || completed === wallets.length) {
          writeProgress(false);
        }
      }
    }
  }

  const workers = [];
  for (let i = 0; i < concurrency; i += 1) {
    workers.push(queueWorker(i));
  }
  await Promise.all(workers);

  const finishedAt = Date.now();
  const summary = {
    ok: true,
    source: "pacifica_account_positions_count_live_census",
    apiBase,
    manifestPath,
    proxyFile,
    proxiesUsed: proxyRows.length,
    includeDirect,
    transport,
    shardIndex,
    shardCount,
    scannedWallets: wallets.length,
    completedWallets: completed,
    failedWallets: failed,
    walletsWithOpenPositions,
    totalOpenPositions,
    startedAt,
    finishedAt,
    durationMs: finishedAt - startedAt,
    latestAccountUpdatedAt: lastUpdatedAt || null,
    failuresSample: failures.slice(0, 25),
  };

  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(summary, null, 2));
  writeProgress(true);
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((error) => {
  console.error(String(error && error.stack ? error.stack : error));
  process.exit(1);
});
