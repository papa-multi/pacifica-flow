"use strict";

const fs = require("fs");
const path = require("path");
const { readJson, writeJsonAtomic } = require("../pipeline/utils");

const DAY_MS = 24 * 60 * 60 * 1000;

function toNum(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function normalizeWallet(value) {
  return String(value || "").trim();
}

function normalizeSymbol(value) {
  return String(value || "").trim().toUpperCase();
}

function normalizeTradeRow(wallet, row = {}) {
  const timestamp = Number(
    row.timestamp !== undefined ? row.timestamp : row.created_at !== undefined ? row.created_at : 0
  );
  if (!Number.isFinite(timestamp) || timestamp <= 0) return null;
  const symbol = normalizeSymbol(row.symbol);
  const amount = toNum(row.amount, NaN);
  const price = toNum(row.price, NaN);
  const fee = toNum(row.fee, 0);
  const pnl = toNum(row.pnl, 0);
  const volumeUsd =
    Number.isFinite(amount) && Number.isFinite(price) ? Math.abs(amount * price) : 0;
  const historyId = row.historyId || row.history_id || null;
  const orderId = row.orderId || row.order_id || null;
  const li = row.li || row.last_order_id || row.lastOrderId || null;
  const side =
    amount > 0 ? "buy" : amount < 0 ? "sell" : null;
  return {
    tradeId: historyId ? `h:${historyId}` : `f:${wallet}:${timestamp}:${symbol}:${amount}:${price}:${fee}:${pnl}`,
    wallet,
    historyId: historyId || null,
    orderId: orderId || null,
    li: li || null,
    symbol,
    amount: Number.isFinite(amount) ? amount : 0,
    price: Number.isFinite(price) ? price : 0,
    fee,
    pnl,
    volumeUsd: Number(volumeUsd.toFixed(8)),
    side,
    timestamp,
  };
}

function summarizeWalletTrades(wallet, rows = []) {
  const safeRows = Array.isArray(rows) ? rows : [];
  const trades = safeRows.length;
  const volumeUsd = safeRows.reduce((sum, row) => sum + toNum(row && row.volumeUsd, 0), 0);
  const feesUsd = safeRows.reduce((sum, row) => sum + Math.max(0, toNum(row && row.fee, 0)), 0);
  const pnlUsd = safeRows.reduce((sum, row) => sum + toNum(row && row.pnl, 0), 0);
  const lastTradeAt = safeRows.reduce(
    (max, row) => Math.max(max, Number((row && row.timestamp) || 0)),
    0
  );
  const firstTradeAt = safeRows.reduce((min, row) => {
    const ts = Number((row && row.timestamp) || 0);
    if (!Number.isFinite(ts) || ts <= 0) return min;
    return min <= 0 ? ts : Math.min(min, ts);
  }, 0);
  const symbols = Array.from(
    new Set(
      safeRows.map((row) => normalizeSymbol(row && row.symbol)).filter(Boolean)
    )
  ).sort();
  return {
    wallet,
    trades,
    volumeUsd: Number(volumeUsd.toFixed(2)),
    feesUsd: Number(feesUsd.toFixed(2)),
    pnlUsd: Number(pnlUsd.toFixed(2)),
    firstTradeAt: firstTradeAt || null,
    lastTradeAt: lastTradeAt || null,
    symbols,
  };
}

function listWalletHistoryFiles(roots = []) {
  const files = [];
  const safeRoots = Array.isArray(roots) ? roots : [];
  safeRoots.forEach((rootDir) => {
    if (!rootDir || !fs.existsSync(rootDir)) return;
    let wallets = [];
    try {
      wallets = fs.readdirSync(rootDir, { withFileTypes: true });
    } catch {
      wallets = [];
    }
    wallets.forEach((entry) => {
      if (!entry || !entry.isDirectory()) return;
      const filePath = path.join(rootDir, String(entry.name || ""), "wallet_history.json");
      if (fs.existsSync(filePath)) files.push(filePath);
    });
  });
  return files.sort();
}

function buildSharedWalletTradeStore({
  historyRoots = [],
  outputPath,
  recentWindowMs = DAY_MS,
  maxTrades = 200_000,
}) {
  const now = Date.now();
  const sinceTs = now - Math.max(60_000, Number(recentWindowMs || DAY_MS));
  const previous =
    outputPath && fs.existsSync(outputPath) ? readJson(outputPath, null) : null;
  const previousFileStats =
    previous && Array.isArray(previous.fileStats)
      ? previous.fileStats.reduce((acc, row) => {
          const wallet = normalizeWallet(row && row.wallet);
          if (wallet) acc[wallet] = row;
          return acc;
        }, {})
      : {};
  const previousWalletTradeRows =
    previous && previous.walletTradeRows && typeof previous.walletTradeRows === "object"
      ? previous.walletTradeRows
      : {};
  const historyFiles = listWalletHistoryFiles(historyRoots);
  const tradeRows = [];
  const walletRows = {};
  const walletTradeRows = {};
  const fileStats = [];
  let latestHistoryUpdatedAt = 0;

  historyFiles.forEach((filePath) => {
    const wallet = normalizeWallet(path.basename(path.dirname(filePath)));
    if (!wallet) return;
    let statUpdatedAt = 0;
    try {
      statUpdatedAt = Math.max(0, Math.trunc(fs.statSync(filePath).mtimeMs || 0));
    } catch {
      statUpdatedAt = 0;
    }
    const previousStat = previousFileStats[wallet];
    if (
      previousStat &&
      Number(previousStat.updatedAt || 0) === statUpdatedAt &&
      Array.isArray(previousWalletTradeRows[wallet])
    ) {
      const reusedRows = previousWalletTradeRows[wallet];
      walletTradeRows[wallet] = reusedRows;
      reusedRows.forEach((row) => tradeRows.push(row));
      walletRows[wallet] = summarizeWalletTrades(wallet, reusedRows);
      latestHistoryUpdatedAt = Math.max(latestHistoryUpdatedAt, statUpdatedAt);
      fileStats.push({
        wallet,
        updatedAt: statUpdatedAt || null,
        recentTrades: reusedRows.length,
        reused: true,
      });
      return;
    }
    const payload = readJson(filePath, null);
    if (!payload || typeof payload !== "object") return;
    const updatedAt = Math.max(0, Number(payload.updatedAt || 0), statUpdatedAt);
    latestHistoryUpdatedAt = Math.max(latestHistoryUpdatedAt, updatedAt);
    const trades = Array.isArray(payload.trades) ? payload.trades : [];
    const recentRows = [];
    for (const row of trades) {
      const normalized = normalizeTradeRow(wallet, row);
      if (!normalized) continue;
      if (normalized.timestamp < sinceTs) continue;
      recentRows.push(normalized);
      tradeRows.push(normalized);
    }
    walletTradeRows[wallet] = recentRows;
    walletRows[wallet] = summarizeWalletTrades(wallet, recentRows);
    fileStats.push({
      wallet,
      updatedAt: updatedAt || null,
      recentTrades: recentRows.length,
      reused: false,
    });
  });

  tradeRows.sort((left, right) => {
    return (
      Number((right && right.timestamp) || 0) - Number((left && left.timestamp) || 0) ||
      String((left && left.tradeId) || "").localeCompare(String((right && right.tradeId) || ""))
    );
  });
  const cappedTrades = tradeRows.slice(0, Math.max(1000, Number(maxTrades || 200_000)));
  const walletEntries = Object.values(walletRows).sort((left, right) => {
    return (
      Number((right && right.lastTradeAt) || 0) - Number((left && left.lastTradeAt) || 0) ||
      Number((right && right.volumeUsd) || 0) - Number((left && left.volumeUsd) || 0) ||
      String((left && left.wallet) || "").localeCompare(String((right && right.wallet) || ""))
    );
  });
  const summary = {
    historyFiles: historyFiles.length,
    walletsWithRecentTrades: walletEntries.filter((row) => Number(row.trades || 0) > 0).length,
    trades24h: cappedTrades.length,
    totalVolumeUsd: Number(
      cappedTrades.reduce((sum, row) => sum + toNum(row && row.volumeUsd, 0), 0).toFixed(2)
    ),
    latestTradeAt: cappedTrades.reduce(
      (max, row) => Math.max(max, Number((row && row.timestamp) || 0)),
      0
    ) || null,
    latestHistoryUpdatedAt: latestHistoryUpdatedAt || null,
    recentWindowMs: Math.max(60_000, Number(recentWindowMs || DAY_MS)),
  };
  const result = {
    generatedAt: Date.now(),
    source: {
      mode: "wallet_history_recent_trade_store",
      historyRoots,
      windowStartAt: sinceTs,
      windowEndAt: now,
    },
    summary,
    wallets: walletEntries.reduce((acc, row) => {
      acc[row.wallet] = row;
      return acc;
    }, {}),
    trades: cappedTrades,
    walletTradeRows,
    fileStats: fileStats.slice(0, 5000),
  };
  if (outputPath) {
    writeJsonAtomic(outputPath, result);
  }
  return result;
}

module.exports = {
  DAY_MS,
  buildSharedWalletTradeStore,
};
