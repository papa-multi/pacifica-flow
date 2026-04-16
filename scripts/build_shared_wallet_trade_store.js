"use strict";

const path = require("path");
const { buildSharedWalletTradeStore } = require("../src/services/analytics/shared_wallet_trade_store");

const root = path.resolve(__dirname, "..");
const outputPath =
  process.env.PACIFICA_SHARED_WALLET_TRADE_STORE_PATH ||
  path.join(root, "data", "nextgen", "state", "wallet-trade-events.json");
const historyRoot =
  process.env.PACIFICA_SHARED_WALLET_HISTORY_ROOT ||
  path.join(root, "data", "wallet_explorer_v2", "isolated");
const recentWindowMs = Math.max(
  5 * 60 * 1000,
  Number(process.env.PACIFICA_SHARED_WALLET_TRADE_WINDOW_MS || 24 * 60 * 60 * 1000)
);
const maxTrades = Math.max(
  1000,
  Number(process.env.PACIFICA_SHARED_WALLET_TRADE_MAX_ROWS || 250000)
);

const startedAt = Date.now();
const payload = buildSharedWalletTradeStore({
  historyRoots: [historyRoot],
  outputPath,
  recentWindowMs,
  maxTrades,
});

console.log(
  JSON.stringify(
    {
      ok: true,
      ms: Date.now() - startedAt,
      outputPath,
      summary: payload.summary,
    },
    null,
    2
  )
);
