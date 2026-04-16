#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

const { createWsClient } = require("../src/services/transport/ws_client");

function toNum(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function parseArgs(argv = []) {
  const out = {};
  argv.forEach((arg) => {
    const text = String(arg || "");
    if (!text.startsWith("--")) return;
    const eq = text.indexOf("=");
    if (eq < 0) {
      out[text.slice(2)] = "true";
      return;
    }
    out[text.slice(2, eq)] = text.slice(eq + 1);
  });
  return out;
}

function parseProxyRows(rawText) {
  const text = String(rawText || "").trim();
  if (!text) return [];
  return text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .filter((line) => !line.startsWith("#"));
}

function uniqStrings(list = []) {
  return Array.from(
    new Set(
      (Array.isArray(list) ? list : [])
        .map((value) => String(value || "").trim())
        .filter(Boolean)
    )
  );
}

function loadJson(filePath, fallback = null) {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return fallback;
  }
}

async function soakProxy({ wsUrl, proxyUrl, wallets, timeoutMs }) {
  const startedAt = Date.now();
  const rows = wallets.map((wallet) => ({
    wallet,
    opened: false,
    subscribeAck: false,
    snapshotAt: 0,
    error: null,
    closed: false,
    openAt: 0,
  }));

  await Promise.all(
    rows.map(
      (row) =>
        new Promise((resolve) => {
          const client = createWsClient({
            url: wsUrl,
            proxyUrl,
            reconnectMs: timeoutMs * 2,
            pingIntervalMs: 25000,
            logger: {
              warn: () => {},
            },
          });
          let done = false;
          const finish = () => {
            if (done) return;
            done = true;
            try {
              client.stop();
            } catch {}
            resolve();
          };
          client.setHandlers({
            onStatus: (status) => {
              if (status === "open") {
                row.opened = true;
                row.openAt = Date.now();
                client.subscribe({ source: "account_positions", account: row.wallet });
                return;
              }
              if (status === "closed" || status === "error" || status === "stopped") {
                row.closed = true;
              }
            },
            onMessage: (payload) => {
              if (!payload || typeof payload !== "object") return;
              if (
                payload.channel === "subscribe" &&
                payload.data &&
                String(payload.data.account || "") === row.wallet
              ) {
                row.subscribeAck = true;
                return;
              }
              if (payload.channel === "account_positions") {
                row.snapshotAt = Date.now();
                finish();
              }
            },
            onError: (error) => {
              row.error = String(error && error.message ? error.message : error || "ws_error");
            },
          });
          client.start();
          setTimeout(finish, timeoutMs);
        })
    )
  );

  const completedAt = Date.now();
  const opened = rows.filter((row) => row.opened).length;
  const subscribeAck = rows.filter((row) => row.subscribeAck).length;
  const snapshots = rows.filter((row) => row.snapshotAt > 0).length;
  const errors = rows.filter((row) => row.error).length;
  const pings = rows
    .filter((row) => row.snapshotAt > 0 && row.openAt > 0)
    .map((row) => row.snapshotAt - row.openAt)
    .sort((a, b) => a - b);
  const percentile = (pct) => {
    if (!pings.length) return null;
    const idx = Math.min(pings.length - 1, Math.max(0, Math.floor((pct / 100) * pings.length)));
    return pings[idx];
  };

  return {
    proxyUrl,
    walletCount: wallets.length,
    durationMs: completedAt - startedAt,
    opened,
    subscribeAck,
    snapshots,
    errors,
    openRate: Number((opened / Math.max(1, wallets.length)).toFixed(4)),
    snapshotRate: Number((snapshots / Math.max(1, wallets.length)).toFixed(4)),
    p50SnapshotMs: percentile(50),
    p90SnapshotMs: percentile(90),
    p95SnapshotMs: percentile(95),
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const rootDir = path.resolve(__dirname, "..");
  const baseDir = path.resolve(
    args.base_dir ||
      process.env.PACIFICA_LIVE_TRADE_BASE_DIR ||
      path.join(rootDir, "data", "live_trade_leaderboard")
  );
  const datasetPath = path.resolve(
    args.dataset ||
      process.env.PACIFICA_LIVE_WALLET_FIRST_WALLET_DATASET_PATH ||
      path.join(baseDir, "wallet_dataset.json")
  );
  const proxyFile = path.resolve(
    args.proxy_file ||
      process.env.PACIFICA_MULTI_EGRESS_PROXY_FILE ||
      path.join(rootDir, "data", "live_positions", "positions_proxies.txt")
  );
  const wsUrl = String(args.ws_url || process.env.PACIFICA_WS_URL || "wss://ws.pacifica.fi/ws").trim();
  const timeoutMs = Math.max(5000, toNum(args.timeout_ms, 15000));
  const proxyCount = Math.max(1, toNum(args.proxy_count, 3));
  const connectionStages = uniqStrings(String(args.wallets_per_proxy || "4,8,12,16").split(","))
    .map((value) => Math.max(1, toNum(value, 0)))
    .filter((value) => value > 0);

  const dataset = loadJson(datasetPath, {});
  const wallets = uniqStrings(
    (Array.isArray(dataset && dataset.rows) ? dataset.rows : [])
      .map((row) => String(row && row.wallet ? row.wallet : "").trim())
      .filter(Boolean)
  );
  if (!wallets.length) {
    throw new Error(`no wallets found in ${datasetPath}`);
  }

  const proxies = uniqStrings(parseProxyRows(fs.readFileSync(proxyFile, "utf8")));
  if (!proxies.length) {
    throw new Error(`no proxies found in ${proxyFile}`);
  }

  const selectedProxies = proxies.slice(0, Math.min(proxyCount, proxies.length));
  let walletCursor = 0;
  const results = [];

  for (const stage of connectionStages) {
    for (const proxyUrl of selectedProxies) {
      const stageWallets = [];
      for (let idx = 0; idx < stage; idx += 1) {
        stageWallets.push(wallets[walletCursor % wallets.length]);
        walletCursor += 1;
      }
      const summary = await soakProxy({
        wsUrl,
        proxyUrl,
        wallets: stageWallets,
        timeoutMs,
      });
      results.push({
        stageWalletsPerProxy: stage,
        ...summary,
      });
    }
  }

  const grouped = new Map();
  results.forEach((row) => {
    const key = row.stageWalletsPerProxy;
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(row);
  });

  const stageSummary = Array.from(grouped.entries()).map(([walletsPerProxy, rows]) => {
    const avg = (key) =>
      Number(
        (
          rows.reduce((sum, row) => sum + Number(row[key] || 0), 0) / Math.max(1, rows.length)
        ).toFixed(4)
      );
    return {
      walletsPerProxy: Number(walletsPerProxy),
      proxyTests: rows.length,
      avgOpenRate: avg("openRate"),
      avgSnapshotRate: avg("snapshotRate"),
      avgP50SnapshotMs: avg("p50SnapshotMs"),
      avgP95SnapshotMs: avg("p95SnapshotMs"),
      passed: rows.every(
        (row) =>
          Number(row.openRate || 0) >= 0.95 &&
          Number(row.snapshotRate || 0) >= 0.95 &&
          Number(row.errors || 0) <= Math.ceil(Number(row.walletCount || 0) * 0.1)
      ),
    };
  });

  console.log(
    JSON.stringify(
      {
        done: true,
        wsUrl,
        datasetPath,
        proxyFile,
        walletsInDataset: wallets.length,
        proxiesTested: selectedProxies.length,
        timeoutMs,
        stageSummary,
        results,
      },
      null,
      2
    )
  );
}

main().catch((error) => {
  console.error(
    JSON.stringify(
      {
        done: false,
        error: error && error.message ? error.message : String(error),
      },
      null,
      2
    )
  );
  process.exit(1);
});
