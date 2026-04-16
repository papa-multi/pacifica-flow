#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

const ROOT = path.resolve(__dirname, "..");
const {
  fetchPacificaPublicWalletSources,
} = require(path.join(ROOT, "src", "services", "read_model", "pacifica_public_wallet_sources"));

const BASE_DIR = path.resolve(
  process.env.PACIFICA_LIVE_TRADE_BASE_DIR ||
    path.join(ROOT, "data", "live_trade_leaderboard")
);
const SOURCES_PATH = path.join(BASE_DIR, "leaderboard_sources.json");
const DATASET_PATH = path.join(BASE_DIR, "wallet_dataset.json");

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function writeJsonAtomic(filePath, payload) {
  ensureDir(path.dirname(filePath));
  const tmpPath = `${filePath}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmpPath, JSON.stringify(payload, null, 2));
  fs.renameSync(tmpPath, filePath);
}

function toNum(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

async function main() {
  const snapshot = await fetchPacificaPublicWalletSources({ limit: 25000 });
  const rows =
    snapshot && snapshot.leaderboard && Array.isArray(snapshot.leaderboard.rows)
      ? snapshot.leaderboard.rows
      : [];
  const generatedAt = Date.now();

  const datasetRows = rows.map((row) => ({
    wallet: String((row && row.wallet) || "").trim(),
    username: row && row.username ? String(row.username).trim() : null,
    rankAllTime: toNum(row && row.rankAllTime, 0),
    pnlAllTime: toNum(row && row.pnlAllTime, 0),
    pnl30d: toNum(row && row.pnl30d, 0),
    pnl7d: toNum(row && row.pnl7d, 0),
    pnl1d: toNum(row && row.pnl1d, 0),
    volumeAllTime: toNum(row && row.volumeAllTime, 0),
    volume30d: toNum(row && row.volume30d, 0),
    volume7d: toNum(row && row.volume7d, 0),
    volume1d: toNum(row && row.volume1d, 0),
    equityCurrent: toNum(row && row.equityCurrent, 0),
    oiCurrent: toNum(row && row.oiCurrent, 0),
    openPositions: toNum(row && row.oiCurrent, 0) > 0 ? 1 : 0,
    lastTrade: 0,
    lastActivity: 0,
    source: "leaderboard_all_time_pnl_desc",
  }));

  writeJsonAtomic(SOURCES_PATH, {
    ...snapshot,
    generatedAt,
    scope: "live_trade_leaderboard_only",
  });
  writeJsonAtomic(DATASET_PATH, {
    generatedAt,
    source: "leaderboard_all_time_pnl_desc",
    walletCount: datasetRows.length,
    rows: datasetRows,
  });

  console.log(
    JSON.stringify({
      done: true,
      generatedAt,
      walletCount: datasetRows.length,
      sourcesPath: SOURCES_PATH,
      datasetPath: DATASET_PATH,
    })
  );
}

main().catch((error) => {
  console.error(
    JSON.stringify({
      done: false,
      error: error && error.message ? error.message : String(error),
    })
  );
  process.exit(1);
});
