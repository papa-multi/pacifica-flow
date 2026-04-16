#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const {
  WalletFirstLivePositionsMonitor,
} = require("../src/services/analytics/wallet_first_live_positions");

function normalizeWallet(value) {
  return String(value || "").trim();
}

function loadWallets(datasetPath) {
  const payload = JSON.parse(fs.readFileSync(datasetPath, "utf8"));
  const rows = Array.isArray(payload)
    ? payload
    : Array.isArray(payload.rows)
    ? payload.rows
    : Array.isArray(payload.wallets)
    ? payload.wallets
    : Array.isArray(payload.data)
    ? payload.data
    : [];
  return rows
    .map((row) => normalizeWallet(row && (row.wallet || row.account || row.address || row)))
    .filter(Boolean);
}

function countBy(list = [], keyFn = (value) => value) {
  const out = new Map();
  for (const item of list) {
    const key = keyFn(item);
    out.set(key, (out.get(key) || 0) + 1);
  }
  return Object.fromEntries(Array.from(out.entries()).sort((a, b) => b[1] - a[1]));
}

function main() {
  const datasetPath = path.resolve(
    process.argv[2] ||
      process.env.PACIFICA_LIVE_TRADE_DATASET_PATH ||
      "/root/pacifica-flow/data/live_trade_leaderboard/wallet_dataset.json"
  );
  const walletCount = Math.max(1, Number(process.argv[3] || process.env.BURST_WALLETS || 80));
  const includeOrderEcho =
    String(process.argv[4] || process.env.BURST_INCLUDE_ORDER_ECHO || "true").toLowerCase() !==
    "false";
  const wallets = loadWallets(datasetPath).slice(0, walletCount);
  if (!wallets.length) {
    throw new Error(`No wallets loaded from ${datasetPath}`);
  }

  const monitor = new WalletFirstLivePositionsMonitor({
    enabled: false,
    restClientEntries: [],
    logger: { log() {}, info() {}, warn() {}, error() {} },
    hotOpenTriggerDedupMs: 3000,
    wsAccountInfoTriggerDedupMs: 3000,
    maxEvents: Math.max(10000, walletCount * 10),
    maxPositionOpenedEvents: Math.max(10000, walletCount * 10),
    maxPersistedEvents: 100,
    maxPersistedPositionOpenedEvents: Math.max(1000, walletCount * 4),
    persistEveryMs: 60 * 60 * 1000,
    openedEventMaxLagMs: 5000,
    unrefTimer: true,
  });

  const baseTs = Date.now();
  const startedAt = process.hrtime.bigint();
  for (let idx = 0; idx < wallets.length; idx += 1) {
    const wallet = wallets[idx];
    const at = baseTs + idx;
    const symbol = `TST${idx % 10}`;
    const side = idx % 2 === 0 ? "open_long" : "open_short";
    monitor.ingestHotWalletActivity(wallet, "ws_account_trade", {
      at,
      symbol,
      side,
      amount: 1,
      price: 100 + idx,
      entry: 100 + idx,
      source: "ws.account_trades",
    });
    if (includeOrderEcho) {
      monitor.ingestHotWalletActivity(wallet, "ws_account_order", {
        at,
        symbol,
        side,
        amount: 1,
        price: 100 + idx,
        entry: 100 + idx,
        orderEvent: "fill",
        orderStatus: "filled",
        reduceOnly: false,
        source: "ws.account_order_updates",
      });
    }
  }
  const elapsedMs = Number(process.hrtime.bigint() - startedAt) / 1e6;
  const opened = monitor.getRecentPositionOpenedEvents(wallets.length * 4);
  const sourceCounts = countBy(opened, (row) => row.source || "unknown");
  const keyCounts = countBy(opened, (row) =>
    [row.wallet, row.symbol, row.side].map((value) => String(value || "")).join("|")
  );

  process.stdout.write(
    `${JSON.stringify(
      {
        datasetPath,
        walletsLoaded: wallets.length,
        includeOrderEcho,
        ingestElapsedMs: Number(elapsedMs.toFixed(3)),
        ingestThroughputPerSec: Number(((wallets.length / Math.max(elapsedMs, 1)) * 1000).toFixed(2)),
        emittedOpenEvents: opened.length,
        uniqueWalletSymbolSide: Object.keys(keyCounts).length,
        duplicateOpenEvents: Math.max(0, opened.length - Object.keys(keyCounts).length),
        sourceCounts,
        sample: opened.slice(0, 5).map((row) => ({
          wallet: row.wallet,
          symbol: row.symbol,
          side: row.side,
          source: row.source,
          observedAt: row.observedAt,
          openedAt: row.openedAt,
        })),
      },
      null,
      2
    )}\n`
  );
}

main();
