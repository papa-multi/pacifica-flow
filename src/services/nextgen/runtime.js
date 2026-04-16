"use strict";

const fs = require("fs");
const path = require("path");
const { appendEvents } = require("./core/event-log");
const { runProjection } = require("./core/projector-runner");
const { loadState } = require("./core/state-store");
const { createLogger } = require("./common/logger");
const {
  loadWalletExplorerRows,
  loadReviewState,
  readSourceState,
  writeSourceState,
  buildBootstrapEvents,
  buildReviewStatusEvents,
  buildLivePositionEvents,
  buildMarketEvents,
} = require("./adapters/current_sources");
const { loadLiveMonitor, syncLiveMonitor } = require("./live_monitor");
const {
  loadWalletExplorerV3Snapshot,
  loadWalletExplorerV3WalletRecord,
} = require("../read_model/wallet_storage_v3");

const SHARED_WALLET_TRADE_STORE_PATH =
  process.env.PACIFICA_SHARED_WALLET_TRADE_STORE_PATH ||
  path.join("/root/pacifica-flow", "data", "nextgen", "state", "wallet-trade-events.json");
const LIVE_POSITIONS_BASELINE_PATH =
  process.env.PACIFICA_LIVE_POSITIONS_BASELINE_PATH ||
  path.join("/root/pacifica-flow", "data", "nextgen", "state", "live_positions_baseline.json");
const GLOBAL_KPI_SNAPSHOT_PATH =
  process.env.PACIFICA_GLOBAL_KPI_SNAPSHOT_PATH ||
  path.join("/root/pacifica-flow", "data", "pipeline", "global_kpi.json");
const DERIVED_SNAPSHOT_TTL_MS = Math.max(
  250,
  Number(process.env.PACIFICA_NEXTGEN_DERIVED_SNAPSHOT_TTL_MS || 2_000)
);
const WALLET_EXPLORER_V3_MANIFEST_PATH =
  process.env.PACIFICA_WALLET_EXPLORER_V3_MANIFEST_PATH ||
  path.join("/root/pacifica-flow", "data", "wallet_explorer_v3", "manifest.json");
const WALLET_EXPLORER_ISOLATED_HISTORY_DIR =
  process.env.PACIFICA_WALLET_EXPLORER_ISOLATED_HISTORY_DIR ||
  path.join("/root/pacifica-flow", "data", "wallet_explorer_v2", "isolated");

const sharedWalletTradeStoreCache = {
  mtimeMs: 0,
  payload: null,
};
const livePositionsBaselineCache = {
  mtimeMs: 0,
  payload: null,
};
const globalKpiPriceLookupCache = {
  mtimeMs: 0,
  lookup: null,
};
const walletExplorerCompactDatasetCache = {
  mtimeMs: 0,
  payload: null,
};
const walletExplorerIsolatedHistoryFeeCache = {
  wallet: null,
  mtimeMs: 0,
  payload: null,
};

function normalizeWallet(value) {
  return String(value || "").trim();
}

function readJsonFile(filePath, fallback) {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch (_error) {
    return fallback;
  }
}

function loadWalletExplorerV3Dataset() {
  const v3Snapshot = loadWalletExplorerV3Snapshot();
  if (v3Snapshot && Array.isArray(v3Snapshot.preparedRows)) {
    const normalizedRows = v3Snapshot.preparedRows.map((row) => {
      const safeRow = row && typeof row === "object" ? { ...row } : {};
      const volumeUsdRaw = Number(
        safeRow.volumeUsdRaw !== undefined && safeRow.volumeUsdRaw !== null
          ? safeRow.volumeUsdRaw
          : safeRow.volumeUsd !== undefined && safeRow.volumeUsd !== null
          ? safeRow.volumeUsd
          : 0
      );
      if (!Number.isNaN(volumeUsdRaw)) {
        safeRow.volumeUsdRaw = volumeUsdRaw;
        safeRow.volumeUsd = volumeUsdRaw;
      }
      return safeRow;
    });
    const rowByWallet = new Map(
      normalizedRows
        .map((row) => {
          const key = normalizeWallet(row && row.wallet);
          return key ? [key, row] : null;
        })
        .filter(Boolean)
    );
    const normalized = {
      generatedAt: Number(v3Snapshot.generatedAt || Date.now()) || Date.now(),
      rows: normalizedRows,
      counts: v3Snapshot.counts || v3Snapshot.summaryCounts || {
        totalWallets: normalizedRows.length,
        hiddenZeroTradeWallets: 0,
      },
      rowByWallet,
      availableSymbols: Array.isArray(v3Snapshot.availableSymbols)
        ? v3Snapshot.availableSymbols
        : [],
      mode: v3Snapshot.mode || "wallet_explorer_v3_snapshot",
    };
    walletExplorerCompactDatasetCache.payload = normalized;
    walletExplorerCompactDatasetCache.mtimeMs = Number(v3Snapshot.generatedAt || 0) || 0;
    return normalized;
  }
  let mtimeMs = 0;
  try {
    mtimeMs = Math.max(0, Math.trunc(fs.statSync(WALLET_EXPLORER_V3_MANIFEST_PATH).mtimeMs || 0));
  } catch (_error) {
    mtimeMs = 0;
  }
  if (
    walletExplorerCompactDatasetCache.payload &&
    walletExplorerCompactDatasetCache.mtimeMs > 0 &&
    walletExplorerCompactDatasetCache.mtimeMs === mtimeMs
  ) {
    return walletExplorerCompactDatasetCache.payload;
  }
  const payload = readJsonFile(WALLET_EXPLORER_V3_MANIFEST_PATH, {
    generatedAt: Date.now(),
    rows: [],
    counts: {
      totalWallets: 0,
      hiddenZeroTradeWallets: 0,
    },
  });
  const rows = Array.isArray(payload.rows) ? payload.rows : [];
  const normalizedRows = rows.map((row) => {
    const safeRow = row && typeof row === "object" ? { ...row } : {};
    const volumeUsdRaw = Number(
      safeRow.volumeUsdRaw !== undefined && safeRow.volumeUsdRaw !== null
        ? safeRow.volumeUsdRaw
        : safeRow.volumeUsd !== undefined && safeRow.volumeUsd !== null
        ? safeRow.volumeUsd
        : 0
    );
    if (!Number.isNaN(volumeUsdRaw)) {
      safeRow.volumeUsdRaw = volumeUsdRaw;
      safeRow.volumeUsd = volumeUsdRaw;
    }
    return safeRow;
  });
  const rowByWallet = new Map(
    normalizedRows
      .map((row) => {
        const key = normalizeWallet(row && row.wallet);
        return key ? [key, row] : null;
      })
      .filter(Boolean)
  );
  const normalized = {
    ...payload,
    rows: normalizedRows,
    rowByWallet,
    generatedAt: Number(payload.generatedAt || mtimeMs || Date.now()) || Date.now(),
  };
  walletExplorerCompactDatasetCache.mtimeMs = mtimeMs;
  walletExplorerCompactDatasetCache.payload = normalized;
  return normalized;
}

function loadWalletExplorerIsolatedHistoryFeeSummary(wallet) {
  const key = normalizeWallet(wallet);
  if (!key) return null;
  const v3Record = loadWalletExplorerV3WalletRecord(key);
  if (v3Record && Number(v3Record.feesPaidUsd || v3Record.feesUsd || 0) > 0) {
    return {
      wallet: key,
      feesPaidUsd: Number(toNum(v3Record.feesPaidUsd ?? v3Record.feesUsd, 0).toFixed(2)),
      feesUsd: Number(toNum(v3Record.feesUsd ?? v3Record.feesPaidUsd, 0).toFixed(2)),
      liquidityPoolFeesUsd: Number(toNum(v3Record.feeRebatesUsd, 0).toFixed(2)),
      netFeesUsd: Number(toNum(v3Record.netFeesUsd, 0).toFixed(2)),
      firstTrade: v3Record.firstTrade || null,
      lastTrade: v3Record.lastTrade || null,
      source: "wallet_explorer_v3",
      updatedAt: Number(v3Record.updatedAt || 0) || Date.now(),
    };
  }
  return null;
}

function loadSharedWalletTradeStore() {
  let mtimeMs = 0;
  try {
    mtimeMs = Math.max(0, Math.trunc(fs.statSync(SHARED_WALLET_TRADE_STORE_PATH).mtimeMs || 0));
  } catch (_error) {
    mtimeMs = 0;
  }
  if (
    sharedWalletTradeStoreCache.payload &&
    sharedWalletTradeStoreCache.mtimeMs > 0 &&
    sharedWalletTradeStoreCache.mtimeMs === mtimeMs
  ) {
    return sharedWalletTradeStoreCache.payload;
  }
  const payload = readJsonFile(SHARED_WALLET_TRADE_STORE_PATH, {
    generatedAt: Date.now(),
    summary: {
      recentWindowMs: 24 * 60 * 60 * 1000,
      walletsWithRecentTrades: 0,
    },
    wallets: {},
    walletTradeRows: {},
    trades: [],
  });
  sharedWalletTradeStoreCache.mtimeMs = mtimeMs;
  sharedWalletTradeStoreCache.payload = payload;
  return payload;
}

function loadLivePositionsBaseline() {
  let mtimeMs = 0;
  try {
    mtimeMs = Math.max(0, Math.trunc(fs.statSync(LIVE_POSITIONS_BASELINE_PATH).mtimeMs || 0));
  } catch (_error) {
    mtimeMs = 0;
  }
  if (
    livePositionsBaselineCache.payload &&
    livePositionsBaselineCache.mtimeMs > 0 &&
    livePositionsBaselineCache.mtimeMs === mtimeMs
  ) {
    return livePositionsBaselineCache.payload;
  }
  const payload = readJsonFile(LIVE_POSITIONS_BASELINE_PATH, {
    generatedAt: Date.now(),
    wallets: [],
    positions: [],
  });
  livePositionsBaselineCache.mtimeMs = mtimeMs;
  livePositionsBaselineCache.payload = payload;
  return payload;
}

function getLivePositionsBaselineWalletSummary(wallet) {
  const key = normalizeWallet(wallet);
  if (!key) return null;
  const payload = loadLivePositionsBaseline();
  const wallets = Array.isArray(payload && payload.wallets) ? payload.wallets : [];
  return (
    wallets.find((row) => normalizeWallet(row && row.wallet) === key) || null
  );
}

function getLivePositionsBaselineWalletPositions(wallet) {
  const key = normalizeWallet(wallet);
  if (!key) return [];
  const payload = loadLivePositionsBaseline();
  const positions = Array.isArray(payload && payload.positions) ? payload.positions : [];
  return positions.filter((row) => normalizeWallet(row && row.wallet) === key);
}

function getSharedWalletTradeSummary(wallet) {
  const key = normalizeWallet(wallet);
  if (!key) return null;
  const payload = loadSharedWalletTradeStore();
  const wallets = payload && payload.wallets && typeof payload.wallets === "object" ? payload.wallets : {};
  const row = wallets[key];
  return row && typeof row === "object" ? row : null;
}

function getSharedWalletRecentTrades(wallet, limit = 100) {
  const key = normalizeWallet(wallet);
  if (!key) {
    return { generatedAt: Date.now(), summary: null, total: 0, rows: [] };
  }
  const payload = loadSharedWalletTradeStore();
  const walletTradeRows =
    payload && payload.walletTradeRows && typeof payload.walletTradeRows === "object"
      ? payload.walletTradeRows
      : {};
  const rows = Array.isArray(walletTradeRows[key]) ? walletTradeRows[key] : [];
  const safeLimit = Math.max(1, Math.min(500, Number(limit || 100)));
  return {
    generatedAt: Number((payload && payload.generatedAt) || Date.now()),
    summary: payload && payload.summary ? payload.summary : null,
    total: rows.length,
    rows: rows.slice(0, safeLimit),
  };
}

function toNum(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function normalizeSymbol(value) {
  return String(value || "").trim().toUpperCase();
}

function normalizePositionSide(value) {
  return String(value || "").trim().toLowerCase();
}

function calculatePositionUnrealizedPnlUsd({ entry = null, mark = null, size = null, side = null, fallback = 0 } = {}) {
  const entryNum = toNum(entry, NaN);
  const markNum = toNum(mark, NaN);
  const sizeNum = Math.abs(toNum(size, 0));
  if (Number.isFinite(entryNum) && Number.isFinite(markNum) && sizeNum > 0) {
    const sideText = normalizePositionSide(side);
    const direction =
      sideText.includes("short") || sideText === "ask" || sideText === "sell" ? -1 : 1;
    return Number(((markNum - entryNum) * sizeNum * direction).toFixed(2));
  }
  return Number(toNum(fallback, 0).toFixed(2));
}

function loadPersistedGlobalKpiPriceLookup() {
  let mtimeMs = 0;
  try {
    mtimeMs = Math.max(0, Math.trunc(fs.statSync(GLOBAL_KPI_SNAPSHOT_PATH).mtimeMs || 0));
  } catch (_error) {
    mtimeMs = 0;
  }
  if (
    globalKpiPriceLookupCache.lookup instanceof Map &&
    globalKpiPriceLookupCache.mtimeMs > 0 &&
    globalKpiPriceLookupCache.mtimeMs === mtimeMs
  ) {
    return globalKpiPriceLookupCache.lookup;
  }
  const payload = readJsonFile(GLOBAL_KPI_SNAPSHOT_PATH, { prices: [] });
  const lookup = new Map();
  const prices = Array.isArray(payload && payload.prices) ? payload.prices : [];
  prices.forEach((row) => {
    const symbol = normalizeSymbol(row && row.symbol);
    if (!symbol) return;
    const mark = toNum(
      row && row.mark !== undefined
        ? row.mark
        : row && row.markPrice !== undefined
        ? row.markPrice
        : row && row.mid !== undefined
        ? row.mid
        : row && row.price !== undefined
        ? row.price
        : NaN,
      NaN
    );
    if (!Number.isFinite(mark) || mark <= 0) return;
    lookup.set(symbol, mark);
  });
  globalKpiPriceLookupCache.mtimeMs = mtimeMs;
  globalKpiPriceLookupCache.lookup = lookup;
  return lookup;
}

function buildRuntimeMarketPriceLookup(markets = {}) {
  const lookup = new Map();
  const source = markets && typeof markets === "object" ? markets : {};
  Object.entries(source).forEach(([symbol, row]) => {
    const normalizedSymbol = normalizeSymbol(symbol);
    if (!normalizedSymbol) return;
    const safeRow = row && typeof row === "object" ? row : {};
    const mark = toNum(
      safeRow.last_price !== undefined
        ? safeRow.last_price
        : safeRow.mark !== undefined
        ? safeRow.mark
        : safeRow.price,
      NaN
    );
    if (!Number.isFinite(mark) || mark <= 0) return;
    lookup.set(normalizedSymbol, mark);
  });
  const fallbackLookup = loadPersistedGlobalKpiPriceLookup();
  if (fallbackLookup instanceof Map) {
    fallbackLookup.forEach((mark, symbol) => {
      if (!lookup.has(symbol)) {
        lookup.set(symbol, mark);
      }
    });
  }
  return lookup;
}

function calculateWalletEquityUsd({ exposureUsd = 0, realizedPnlUsd = 0, unrealizedPnlUsd = 0 } = {}) {
  return Math.max(1, toNum(exposureUsd, 0) + toNum(realizedPnlUsd, 0) + toNum(unrealizedPnlUsd, 0));
}

function calculateDrawdownPct(drawdownUsd = 0, walletEquityUsd = 1) {
  return Number(((Math.max(0, toNum(drawdownUsd, 0)) / Math.max(1, toNum(walletEquityUsd, 1))) * 100).toFixed(2));
}

function normalizeWalletProfilePositionRows(rows = [], realizedPnlUsd = 0, marketPriceLookup = null) {
  const safeRows = Array.isArray(rows) ? rows : [];
  const priceLookup = marketPriceLookup instanceof Map ? marketPriceLookup : null;
  const exposureUsd = Number(
    safeRows.reduce((sum, row) => sum + toNum(row && (row.positionUsd ?? row.notionalUsd), 0), 0).toFixed(2)
  );
  const normalizedRows = safeRows.map((row) => {
    const safeRow = row && typeof row === "object" ? row : {};
    const symbol = normalizeSymbol(safeRow.symbol);
    const lookupMark = priceLookup && symbol ? toNum(priceLookup.get(symbol), NaN) : NaN;
    const fallbackMark = toNum(
      safeRow.mark !== undefined
        ? safeRow.mark
        : safeRow.markPrice !== undefined
        ? safeRow.markPrice
        : safeRow.currentPrice,
      NaN
    );
    const mark = Number.isFinite(lookupMark) && lookupMark > 0 ? lookupMark : fallbackMark;
    const unrealized = calculatePositionUnrealizedPnlUsd({
      entry: safeRow.entry !== undefined ? safeRow.entry : safeRow.entryPrice,
      mark,
      size:
        safeRow.size !== undefined
          ? safeRow.size
          : safeRow.qty !== undefined
          ? safeRow.qty
          : safeRow.amount,
      side: safeRow.side,
      fallback:
        safeRow.unrealizedPnlUsd !== undefined
          ? safeRow.unrealizedPnlUsd
          : safeRow.positionPnlUsd !== undefined
          ? safeRow.positionPnlUsd
          : safeRow.pnl,
    });
    return {
      ...safeRow,
      mark: Number.isFinite(mark) ? Number(mark.toFixed(8)) : safeRow.mark,
      markPrice: Number.isFinite(mark) ? Number(mark.toFixed(8)) : safeRow.markPrice,
      unrealizedPnlUsd: unrealized,
      positionPnlUsd: unrealized,
    };
  });
  const unrealizedPnlUsd = Number(
    normalizedRows.reduce((sum, row) => sum + toNum(row && row.unrealizedPnlUsd, 0), 0).toFixed(2)
  );
  const walletEquityUsd = Number(
    calculateWalletEquityUsd({
      exposureUsd,
      realizedPnlUsd,
      unrealizedPnlUsd,
    }).toFixed(2)
  );
  const normalizedRowsWithDrawdown = normalizedRows.map((row) => {
    const unrealized = Number(toNum(row && row.unrealizedPnlUsd, 0).toFixed(2));
    const drawdownUsd = Number(Math.max(0, -unrealized).toFixed(2));
    return {
      ...(row && typeof row === "object" ? row : {}),
      unrealizedPnlUsd: unrealized,
      positionPnlUsd: unrealized,
      drawdownUsd,
      drawdownPct: calculateDrawdownPct(drawdownUsd, walletEquityUsd),
      walletEquityUsd,
    };
  });
  const drawdownUsd = Number(
    normalizedRowsWithDrawdown.reduce((sum, row) => sum + Math.max(0, toNum(row && row.drawdownUsd, 0)), 0).toFixed(2)
  );
  return {
    rows: normalizedRowsWithDrawdown,
    exposureUsd,
    unrealizedPnlUsd,
    walletEquityUsd,
    drawdownUsd,
    drawdownPct: calculateDrawdownPct(drawdownUsd, walletEquityUsd),
  };
}

function summarizeGroup(label, phaseType, count, summaryMetrics, wallets, focusLabel) {
  return {
    label,
    phaseType,
    count: Math.max(0, Number(count || 0)),
    active: 0,
    complete: 0,
    progressPct: 0,
    badgeLabel: label,
    focusLabel,
    summaryMetrics: Array.isArray(summaryMetrics) ? summaryMetrics : [],
    wallets: Array.isArray(wallets) ? wallets : [],
  };
}

function createNextgenRuntime({
  restClient,
  logger = console,
  marketSyncMs = 60_000,
  walletSyncMs = 30_000,
  liveMonitorSyncMs = 5_000,
}) {
  const log = createLogger("nextgen-runtime");
  const marketSyncEnabled =
    String(process.env.PACIFICA_NEXTGEN_MARKET_SYNC_ENABLED || process.env.PACIFICA_GLOBAL_KPI_ENABLED || "false")
      .trim()
      .toLowerCase() === "true";
  let started = false;
  let marketTimer = null;
  let walletTimer = null;
  let liveMonitorTimer = null;
  let lastSummary = {
    startedAt: 0,
    lastBootstrapAt: 0,
    lastReviewAt: 0,
    lastMarketAt: 0,
    lastProjectAt: 0,
    bootstrapEvents: 0,
    reviewEvents: 0,
    marketEvents: 0,
    projectRuns: 0,
  };
  const derivedSnapshotCache = {
    expiresAt: 0,
    payload: null,
  };

  function buildDerivedSnapshot() {
    const state = loadState();
    const liveMonitor = loadLiveMonitor();
    const liveMonitorWallets =
      liveMonitor && liveMonitor.wallets && typeof liveMonitor.wallets === "object"
        ? liveMonitor.wallets
        : {};
    const rows = Object.values(state.wallets || {}).map((row) => {
      const trade = (state.trades || {})[row.wallet] || {};
      const live = (state.live || {})[row.wallet] || {};
      const strictLive = liveMonitorWallets[row.wallet] || {};
      const strictLivePositionKeys =
        strictLive.positionKeys && typeof strictLive.positionKeys === "object"
          ? strictLive.positionKeys
          : {};
      const strictLiveDerivedLastOpenedPosition = Object.entries(strictLivePositionKeys).reduce(
        (best, [positionKey, meta]) => {
          const safeMeta = meta && typeof meta === "object" ? meta : {};
          const openedAt = Number(safeMeta.openedAt || 0) || 0;
          const updatedAt = Number(safeMeta.updatedAt || 0) || 0;
          if (!openedAt) return best;
          const currentOpenedAt = Number((best && best.openedAt) || 0) || 0;
          const currentUpdatedAt = Number((best && best.updatedAt) || 0) || 0;
          if (
            currentOpenedAt > openedAt ||
            (currentOpenedAt === openedAt && currentUpdatedAt > updatedAt)
          ) {
            return best;
          }
          const [symbol = "", rawSide = ""] = String(positionKey || "").split("|");
          const normalizedRawSide = String(rawSide || "").trim().toLowerCase();
          const side =
            normalizedRawSide === "bid"
              ? "open_long"
              : normalizedRawSide === "ask"
              ? "open_short"
              : normalizedRawSide || null;
          return {
            wallet: row.wallet,
            positionKey: positionKey || null,
            symbol: String(symbol || "").trim().toUpperCase() || null,
            side,
            direction:
              normalizedRawSide === "ask"
                ? "short"
                : normalizedRawSide === "bid"
                ? "long"
                : null,
            entry: null,
            mark: null,
            size:
              safeMeta.size !== undefined && safeMeta.size !== null
                ? Number(safeMeta.size || 0)
                : null,
            positionUsd: null,
            unrealizedPnlUsd: null,
            openedAt,
            observedAt: Number(safeMeta.observedAt || 0) || null,
            updatedAt: updatedAt || null,
            source: "strict_live_position_keys",
            confidence: "high",
          };
        },
        null
      );
      const strictLiveLastOpenedPosition =
        Number(strictLive.lastOpenedPositionAt || 0) > 0
          ? {
              wallet: row.wallet,
              positionKey: strictLive.lastOpenedPositionKey || null,
              symbol: strictLive.lastOpenedPositionSymbol || null,
              side: strictLive.lastOpenedPositionSide || null,
              direction: strictLive.lastOpenedPositionDirection || null,
              entry: Number(strictLive.lastOpenedPositionEntry || 0) || null,
              mark: Number(strictLive.lastOpenedPositionMark || 0) || null,
              size: Number(strictLive.lastOpenedPositionSize || 0) || null,
              positionUsd: Number(strictLive.lastOpenedPositionUsd || 0) || null,
              unrealizedPnlUsd:
                strictLive.lastOpenedPositionPnlUsd !== undefined &&
                strictLive.lastOpenedPositionPnlUsd !== null
                  ? Number(strictLive.lastOpenedPositionPnlUsd || 0)
                  : null,
              openedAt: Number(strictLive.lastOpenedPositionAt || 0) || null,
              observedAt: Number(strictLive.lastOpenedPositionObservedAt || 0) || null,
              updatedAt: Number(strictLive.lastOpenedPositionUpdatedAt || 0) || null,
              source: strictLive.lastOpenedPositionSource || null,
              confidence: strictLive.lastOpenedPositionConfidence || null,
            }
          : strictLiveDerivedLastOpenedPosition;
      const tradeCount = Number(trade.trades || row.trades || 0);
      const volumeUsd = Number(trade.volume_usd || row.volume_usd || 0);
      const pnlUsd = Number(trade.realized_pnl_usd || row.realized_pnl_usd || 0);
      const positionState = (state.positions || {})[row.wallet] || {};
      const liveOpenPositions = Number((live && live.open_positions) || 0);
      const projectedOpenPositions = Number(positionState.open_positions_count || 0);
      const lastTrade = Math.max(
        Number((trade && trade.last_trade_at) || 0),
        Number(row.last_trade_at || 0),
        0
      ) || null;
      const firstTrade = Number(row.first_trade_at || row.first_deposit_at || 0) || null;
      const reviewStage = String(live.review_stage || row.review_stage || "bootstrap");
      const lastPositionActivityConfidence = String(strictLive.lastPositionActivityConfidence || "");
      const trustedPositionActivity =
        lastPositionActivityConfidence === "high" || lastPositionActivityConfidence === "medium";
      const lastPositionActivityAt = trustedPositionActivity
        ? Number(strictLive.lastPositionActivityAt || 0) || null
        : null;
      const lastOpenedAt =
        Number((strictLiveLastOpenedPosition && strictLiveLastOpenedPosition.openedAt) || 0) ||
        null;
      const openPositions = Math.max(
        Number(row.open_positions || 0),
        liveOpenPositions,
        projectedOpenPositions,
        Number(strictLive.openPositions || 0),
        Array.isArray(positionState.positions) ? positionState.positions.length : 0
      );
      const canonicalLastActivity = lastOpenedAt || null;
      const liveFreshnessStatus = String(strictLive.liveFreshnessStatus || "");
      const liveHeartbeatHealthy = Boolean(strictLive.liveHeartbeatHealthy);
      const trulyLive =
        openPositions > 0 &&
        liveHeartbeatHealthy &&
        (liveFreshnessStatus === "healthy" || liveFreshnessStatus === "degraded");

      return {
        wallet: row.wallet,
        trades: tradeCount,
        volumeUsd: volumeUsd,
        volumeUsdRaw: volumeUsd,
        pnlUsd: pnlUsd,
        totalPnlUsd: pnlUsd + Number(row.unrealized_pnl_usd || 0),
        openPositions,
        openOrders: Number(row.open_orders || 0),
        lastActivity: canonicalLastActivity,
        lastOpenedAt,
        lastOpenedPosition: strictLiveLastOpenedPosition,
        lastPositionActivityAt,
        lastPositionActivityType: strictLive.lastPositionActivityType || null,
        lastPositionActivityConfidence: strictLive.lastPositionActivityConfidence || null,
        lastTrade,
        firstTrade,
        verifiedThroughAt: Number(live.verified_through_at || row.verified_through_at || 0) || null,
        lastHeadCheckAt: Number(live.last_head_check_at || row.last_head_check_at || 0) || null,
        reviewStage,
        zeroTradeVerified: reviewStage === "zero_verified",
        historyComplete: ["gap", "live_transition", "live", "zero_verified"].includes(
          reviewStage
        ),
        gapClosed: ["live_transition", "live", "zero_verified"].includes(
          reviewStage
        ),
        liveSourceMode: strictLive.liveSourceMode || null,
        activeWsSubscription: Boolean(strictLive.activeWsSubscription),
        wsSubscriptionStatus: strictLive.wsSubscriptionStatus || null,
        lastEventReceivedAt: Number(strictLive.lastEventReceivedAt || 0) || null,
        lastLocalStateUpdateAt: Number(strictLive.lastLocalStateUpdateAt || 0) || null,
        lastPositionSnapshotAt: Number(strictLive.lastPositionSnapshotAt || 0) || null,
        liveHeartbeatHealthy,
        liveFreshnessStatus: liveFreshnessStatus || null,
        liveFreshnessAgeMs: Number(strictLive.liveFreshnessAgeMs || 0) || null,
        snapshotConsistency: strictLive.snapshotConsistency || null,
        hasReliablePositionSet: Boolean(strictLive.hasReliablePositionSet),
        accountHintReason: strictLive.accountHintReason || null,
        accountHintMissCount: Number(strictLive.accountHintMissCount || 0) || null,
        materializedPositions: Number(strictLive.materializedPositions || 0) || null,
        isTrulyLive: trulyLive,
        feesPaidUsd: Number(row.fees_paid_usd || 0),
        totalWins: Number(row.total_wins || 0),
        totalLosses: Number(row.total_losses || 0),
        winRate: Number(row.win_rate || 0),
        symbolBreakdown: Array.isArray(row.symbol_breakdown) ? row.symbol_breakdown : [],
        updatedAt: Math.max(
          Number((live && live.updated_at) || 0),
          Number(strictLive.lastLocalStateUpdateAt || 0),
          Number(strictLive.openPositionsUpdatedAt || 0),
          Number(row.last_head_check_at || 0),
          Number(row.last_opened_at || 0),
          0
        ),
      };
    });
    const rowByWallet = new Map(rows.map((row) => [row.wallet, row]));
    return {
      generatedAt: Date.now(),
      state,
      liveMonitor,
      liveMonitorWallets,
      rows,
      rowByWallet,
    };
  }

  function getDerivedSnapshot() {
    const now = Date.now();
    if (derivedSnapshotCache.payload && derivedSnapshotCache.expiresAt > now) {
      return derivedSnapshotCache.payload;
    }
    const payload = buildDerivedSnapshot();
    derivedSnapshotCache.payload = payload;
    derivedSnapshotCache.expiresAt = now + DERIVED_SNAPSHOT_TTL_MS;
    return payload;
  }

  function getAllRows() {
    return getDerivedSnapshot().rows;
  }

  function getRows() {
    return getAllRows().filter((row) => Number(row.trades || 0) > 0);
  }

  async function syncBootstrapAndReview() {
    const sourceState = readSourceState();
    const bootstrapEvents = buildBootstrapEvents(sourceState);
    const reviewEvents = buildReviewStatusEvents(sourceState);
    const livePositionEvents = buildLivePositionEvents(sourceState);
    if (bootstrapEvents.length) {
      appendEvents(bootstrapEvents);
      lastSummary.bootstrapEvents += bootstrapEvents.length;
      lastSummary.lastBootstrapAt = Date.now();
    }
    if (reviewEvents.length) {
      appendEvents(reviewEvents);
      lastSummary.reviewEvents += reviewEvents.length;
      lastSummary.lastReviewAt = Date.now();
    }
    if (livePositionEvents.length) {
      appendEvents(livePositionEvents);
      lastSummary.reviewEvents += livePositionEvents.length;
      lastSummary.lastReviewAt = Date.now();
    }
    if (bootstrapEvents.length || reviewEvents.length || livePositionEvents.length) {
      writeSourceState(sourceState);
      runProjection({ cursorName: "nextgen-main" });
      lastSummary.projectRuns += 1;
      lastSummary.lastProjectAt = Date.now();
    }
  }

  async function syncMarkets() {
    if (!restClient || typeof restClient.get !== "function") return;
    try {
      const response = await restClient.get("/info/prices", { cost: 1 });
      const payload = response && response.data && Array.isArray(response.data)
        ? response.data
        : Array.isArray(response && response.data && response.data.rows)
        ? response.data.rows
        : Array.isArray(response && response.data)
        ? response.data
        : [];
      const sourceState = readSourceState();
      const events = buildMarketEvents(sourceState, payload);
      if (events.length) {
        appendEvents(events);
        writeSourceState(sourceState);
        runProjection({ cursorName: "nextgen-main" });
        lastSummary.marketEvents += events.length;
        lastSummary.lastMarketAt = Date.now();
        lastSummary.projectRuns += 1;
        lastSummary.lastProjectAt = Date.now();
      }
    } catch (error) {
      logger.warn(`[nextgen-runtime] market sync failed: ${error.message}`);
    }
  }

  async function syncStrictLiveMonitor() {
    try {
      syncLiveMonitor();
      lastSummary.lastProjectAt = Date.now();
    } catch (error) {
      logger.warn(`[nextgen-runtime] live monitor sync failed: ${error.message}`);
    }
  }

  function start() {
    if (started) return;
    started = true;
    lastSummary.startedAt = Date.now();
    const warmCaches = () => {
      try {
        getDerivedSnapshot();
        loadSharedWalletTradeStore();
      } catch (error) {
        logger.warn(`[nextgen-runtime] cache warmup failed: ${error.message}`);
      }
    };
    syncBootstrapAndReview().catch((error) => {
      logger.warn(`[nextgen-runtime] bootstrap sync failed: ${error.message}`);
    });
    if (marketSyncEnabled) {
      syncMarkets().catch((error) => {
        logger.warn(`[nextgen-runtime] market sync failed: ${error.message}`);
      });
    }
    syncStrictLiveMonitor().catch((error) => {
      logger.warn(`[nextgen-runtime] live monitor bootstrap failed: ${error.message}`);
    });
    walletTimer = setInterval(() => {
      syncBootstrapAndReview().catch((error) => {
        logger.warn(`[nextgen-runtime] wallet sync failed: ${error.message}`);
      });
    }, Math.max(5_000, Number(walletSyncMs || 30_000)));
    if (marketSyncEnabled) {
      marketTimer = setInterval(() => {
        syncMarkets().catch((error) => {
          logger.warn(`[nextgen-runtime] market timer failed: ${error.message}`);
        });
      }, Math.max(15_000, Number(marketSyncMs || 60_000)));
    }
    liveMonitorTimer = setInterval(() => {
      syncStrictLiveMonitor().catch((error) => {
        logger.warn(`[nextgen-runtime] live monitor timer failed: ${error.message}`);
      });
    }, Math.max(2_000, Number(liveMonitorSyncMs || 5_000)));
    if (walletTimer.unref) walletTimer.unref();
    if (marketTimer && marketTimer.unref) marketTimer.unref();
    if (liveMonitorTimer.unref) liveMonitorTimer.unref();
    if (typeof setTimeout === "function") {
      const warmTimer = setTimeout(warmCaches, 50);
      if (warmTimer && warmTimer.unref) warmTimer.unref();
    }
    log.info("nextgen runtime started", {
      walletSyncMs,
      marketSyncMs,
      liveMonitorSyncMs,
    });
  }

  function stop() {
    if (walletTimer) clearInterval(walletTimer);
    if (marketTimer) clearInterval(marketTimer);
    if (liveMonitorTimer) clearInterval(liveMonitorTimer);
    walletTimer = null;
    marketTimer = null;
    liveMonitorTimer = null;
    started = false;
  }

  function queryWallets({
    q = "",
    page = 1,
    pageSize = 100,
    sort = "lastActivity",
    order = "desc",
  } = {}) {
    const needle = normalizeWallet(q).toLowerCase();
    const compactDataset = loadWalletExplorerV3Dataset();
    const allRows = Array.isArray(compactDataset.rows) ? compactDataset.rows : [];
    const hiddenZeroTradeWallets = Number(
      (compactDataset.counts && compactDataset.counts.hiddenZeroTradeWallets) || 0
    );
    let rows = allRows.slice();
    if (needle) {
      rows = rows.filter((row) => normalizeWallet(row.wallet).toLowerCase().includes(needle));
    }
    const sortKey = String(sort || "lastActivity");
    const direction = String(order || "desc").toLowerCase() === "asc" ? 1 : -1;
    rows.sort((a, b) => {
      const valueFor = (row = {}) => {
        if (sortKey === "wallet") return String(row.wallet || "");
        if (sortKey === "trades") return Number(row.trades || 0);
        if (sortKey === "trades30") return Number(row.trades30 || row.d30 && row.d30.trades || 0);
        if (sortKey === "volumeUsdRaw" || sortKey === "volumeUsd" || sortKey === "rankingVolumeUsd") {
          return Number(row.volumeUsdRaw || row.volumeUsd || 0);
        }
        if (sortKey === "totalWins") return Number(row.totalWins || 0);
        if (sortKey === "totalLosses") return Number(row.totalLosses || 0);
        if (sortKey === "pnlUsd") return Number(row.pnlUsd || 0);
        if (sortKey === "pnl7Usd") return Number(row.pnl7Usd || 0);
        if (sortKey === "pnl30Usd") return Number(row.pnl30Usd || 0);
        if (sortKey === "winRate" || sortKey === "winRate30") return Number(row.winRate || row.winRate30 || 0);
        if (sortKey === "drawdownPct") return Number(row.drawdownPct || 0);
        if (sortKey === "firstTrade") return Number(row.firstTrade || 0);
        if (sortKey === "lastTrade") return Number(row.lastTrade || 0);
        if (sortKey === "lastActivity") {
          return Number(
            row.lastActivity ||
              row.lastActivityAt ||
              row.lastOpenedAt ||
              row.lastOpenedPosition && row.lastOpenedPosition.openedAt ||
              0
          );
        }
        if (sortKey === "openPositions") return Number(row.openPositions || 0);
        if (sortKey === "exposureUsd") return Number(row.exposureUsd || 0);
        return Number(row.volumeUsdRaw || row.volumeUsd || 0);
      };
      const av = valueFor(a);
      const bv = valueFor(b);
      if (av !== bv) return (av - bv) * direction;
      return String(a.wallet || "").localeCompare(String(b.wallet || "")) * direction;
    });
    const total = rows.length;
    const safePageSize = Math.max(1, Math.min(500, Number(pageSize || 100)));
    const safePage = Math.max(1, Number(page || 1));
    const start = (safePage - 1) * safePageSize;
    return {
      total,
      totalWallets: allRows.length,
      hiddenZeroTradeWallets,
      page: safePage,
      pageSize: safePageSize,
      rows: rows.slice(start, start + safePageSize),
    };
  }

  function getWallet(wallet) {
    const key = normalizeWallet(wallet);
    const compactDataset = loadWalletExplorerV3Dataset();
    const row = compactDataset && compactDataset.rowByWallet instanceof Map
      ? compactDataset.rowByWallet.get(key) || null
      : null;
    return row || null;
  }

  function getProfile(wallet) {
    const key = normalizeWallet(wallet);
    const snapshot = getDerivedSnapshot();
    const compactDataset = loadWalletExplorerV3Dataset();
    const row = compactDataset && compactDataset.rowByWallet instanceof Map
      ? compactDataset.rowByWallet.get(key) || null
      : null;
    const sharedTradeSummary = getSharedWalletTradeSummary(key);
    const isolatedFeeSummary = loadWalletExplorerIsolatedHistoryFeeSummary(key);
    const recentTradesPayload = getSharedWalletRecentTrades(key, 100);
    const state = snapshot.state;
    const liveMonitorWallets = snapshot.liveMonitorWallets;
    const baselineWalletSummary = getLivePositionsBaselineWalletSummary(key);
    const rawBaselinePositions = getLivePositionsBaselineWalletPositions(key);
    if (!row) {
      return {
        found: false,
        wallet: key,
        summary: null,
        symbolBreakdown: [],
        positions: { wallet: key, positions: [] },
        orders: { wallet: key, orders: [] },
        trades: { wallet: key, trades: 0, volume_usd: 0, realized_pnl_usd: 0 },
        recentTrades: [],
        tradeStore: null,
        live: null,
        openPositions: 0,
      };
    }
    const fallbackPositions =
      state.positions[key] && Array.isArray(state.positions[key].positions)
        ? state.positions[key].positions
        : [];
    const resolvedPositionRows =
      rawBaselinePositions.length > 0 ? rawBaselinePositions : fallbackPositions;
    const realizedPnlUsd = Number(toNum(row.pnlUsd, 0).toFixed(2));
    const marketPriceLookup = buildRuntimeMarketPriceLookup(state.markets);
    const normalizedPositions = normalizeWalletProfilePositionRows(
      resolvedPositionRows,
      realizedPnlUsd,
      marketPriceLookup
    );
    const resolvedLastOpenedPosition =
      baselineWalletSummary &&
      baselineWalletSummary.lastOpenedPosition &&
      typeof baselineWalletSummary.lastOpenedPosition === "object"
        ? baselineWalletSummary.lastOpenedPosition
        : row.lastOpenedPosition || null;
    const resolvedLastOpenedAt =
      Number(
        (resolvedLastOpenedPosition && resolvedLastOpenedPosition.openedAt) ||
          (baselineWalletSummary && baselineWalletSummary.lastOpenedAt) ||
          row.lastOpenedAt ||
          0
      ) || null;
    const resolvedOpenPositions =
      normalizedPositions.rows.length > 0
        ? normalizedPositions.rows.length
        : Math.max(
            Number(row.openPositions || 0),
            Number((baselineWalletSummary && baselineWalletSummary.openPositions) || 0)
          );
    const resolvedLastPositionSnapshotAt =
      normalizedPositions.rows.reduce(
        (max, position) =>
          Math.max(
            max,
            Number(
              (position &&
                (position.observedAt ||
                  position.lastObservedAt ||
                  position.updatedAt ||
                  position.timestamp)) ||
                0
            ) || 0
          ),
        0
      ) ||
      Number((baselineWalletSummary && baselineWalletSummary.lastObservedAt) || 0) ||
      row.lastPositionSnapshotAt ||
      null;
    const corpusLastActivityAt =
      Number(row.lastActivity || row.lastActivityAt || row.lastOpenedAt || 0) || null;
    const rowFeesUsd = Math.max(
      0,
      toNum(
        row.feesPaidUsd !== undefined
          ? row.feesPaidUsd
          : row.feesUsd !== undefined
          ? row.feesUsd
          : row.netFeesUsd,
        NaN
      )
    );
    const sharedFeesUsd = Math.max(
      0,
      toNum(sharedTradeSummary && sharedTradeSummary.feesUsd, NaN)
    );
    const isolatedFeesUsd = Math.max(
      0,
      toNum(
        isolatedFeeSummary && isolatedFeeSummary.feesPaidUsd !== undefined
          ? isolatedFeeSummary.feesPaidUsd
          : isolatedFeeSummary && isolatedFeeSummary.feesUsd !== undefined
          ? isolatedFeeSummary.feesUsd
          : NaN,
        NaN
      )
    );
    const summarizedFeesUsd = Number(
      (rowFeesUsd > 0 ? rowFeesUsd : sharedFeesUsd > 0 ? sharedFeesUsd : isolatedFeesUsd > 0 ? isolatedFeesUsd : 0).toFixed(2)
    );
    return {
      found: true,
      wallet: key,
      summary: {
        wallet: key,
        volumeUsd: row.volumeUsd,
        pnlUsd: row.pnlUsd,
        realizedPnlUsd,
        trades: row.trades,
        feesPaidUsd: summarizedFeesUsd,
        feesUsd: summarizedFeesUsd,
        netFeesUsd: summarizedFeesUsd,
        totalWins: row.totalWins,
        totalLosses: row.totalLosses,
        openPositions: resolvedOpenPositions,
        winRate: row.winRate,
        firstTrade: row.firstTrade,
        lastTrade: row.lastTrade,
        lastActivity: corpusLastActivityAt || resolvedLastOpenedAt || null,
        lastOpenedAt:
          Number(row.lastOpenedAt || row.lastActivityAt || row.lastActivity || 0) ||
          resolvedLastOpenedAt ||
          null,
        lastOpenedPosition: resolvedLastOpenedPosition,
        lastPositionActivityAt: row.lastPositionActivityAt,
        lastPositionActivityType: row.lastPositionActivityType,
        lastPositionActivityConfidence: row.lastPositionActivityConfidence,
        verifiedThroughAt: row.verifiedThroughAt,
        lastHeadCheckAt: row.lastHeadCheckAt,
        reviewStage: row.reviewStage,
        liveSourceMode: row.liveSourceMode,
        wsSubscriptionStatus: row.wsSubscriptionStatus,
        activeWsSubscription: row.activeWsSubscription,
        lastEventReceivedAt: row.lastEventReceivedAt,
        lastLocalStateUpdateAt: row.lastLocalStateUpdateAt,
        lastPositionSnapshotAt: row.lastPositionSnapshotAt,
        liveHeartbeatHealthy: row.liveHeartbeatHealthy,
        liveFreshnessStatus: row.liveFreshnessStatus,
        liveFreshnessAgeMs: row.liveFreshnessAgeMs,
        snapshotConsistency: row.snapshotConsistency,
        hasReliablePositionSet: row.hasReliablePositionSet,
        accountHintReason: row.accountHintReason,
        accountHintMissCount: row.accountHintMissCount,
        materializedPositions: row.materializedPositions,
        isTrulyLive: row.isTrulyLive,
        exposureUsd: normalizedPositions.exposureUsd,
        unrealizedPnlUsd: normalizedPositions.unrealizedPnlUsd,
        totalPnlUsd: Number((realizedPnlUsd + normalizedPositions.unrealizedPnlUsd).toFixed(2)),
        walletEquityUsd: normalizedPositions.walletEquityUsd,
        drawdownUsd: normalizedPositions.drawdownUsd,
        drawdownPct: normalizedPositions.drawdownPct,
        positionCount: normalizedPositions.rows.length,
        lastPositionSnapshotAt: resolvedLastPositionSnapshotAt,
        recentTrades24h: Number((sharedTradeSummary && sharedTradeSummary.trades) || 0),
        recentTradeVolume24hUsd: Number((sharedTradeSummary && sharedTradeSummary.volumeUsd) || 0),
        recentTradeFees24hUsd: Number((sharedTradeSummary && sharedTradeSummary.feesUsd) || 0),
        recentTradePnl24hUsd: Number((sharedTradeSummary && sharedTradeSummary.pnlUsd) || 0),
        recentTradeLastAt: Number((sharedTradeSummary && sharedTradeSummary.lastTradeAt) || 0) || null,
      },
      symbolBreakdown: Array.isArray(row.symbolBreakdown) ? row.symbolBreakdown : [],
      openPositions: row.openPositions,
      reviewStage: row.reviewStage,
      verifiedThroughAt: row.verifiedThroughAt,
      lastHeadCheckAt: row.lastHeadCheckAt,
      lastActivity: corpusLastActivityAt || resolvedLastOpenedAt || null,
      lastOpenedAt:
        Number(row.lastOpenedAt || row.lastActivityAt || row.lastActivity || 0) ||
        resolvedLastOpenedAt ||
        null,
      lastOpenedPosition: resolvedLastOpenedPosition,
      lastPositionActivityAt: row.lastPositionActivityAt,
      lastPositionActivityType: row.lastPositionActivityType,
      lastPositionActivityConfidence: row.lastPositionActivityConfidence,
      positions: {
        wallet: key,
        positions: normalizedPositions.rows,
        open_positions_count: resolvedOpenPositions,
        updated_at: resolvedLastPositionSnapshotAt,
      },
      orders: state.orders[key] || { wallet: key, orders: [] },
      trades: state.trades[key] || { wallet: key, trades: 0, volume_usd: 0, realized_pnl_usd: 0 },
      recentTrades: Array.isArray(recentTradesPayload.rows) ? recentTradesPayload.rows : [],
      tradeStore:
        recentTradesPayload && recentTradesPayload.summary
          ? {
              generatedAt: recentTradesPayload.generatedAt,
              recentWindowMs: Number(recentTradesPayload.summary.recentWindowMs || 0) || null,
              walletRecentTrades: Number(recentTradesPayload.total || 0),
              walletsWithRecentTrades: Number(
                recentTradesPayload.summary.walletsWithRecentTrades || 0
              ),
            }
          : null,
      live: state.live[key] || null,
      liveMonitor: liveMonitorWallets[key] || null,
    };
  }

  function getSyncHealth() {
    const review = loadReviewState();
    const snapshot = getDerivedSnapshot();
    const rows = snapshot.rows;
    const liveMonitor = snapshot.liveMonitor;
    const visibleWallets = rows.filter((row) => Number(row.trades || 0) > 0).length;
    const hiddenZeroTradeWallets = Math.max(0, rows.length - visibleWallets);
    const stageCounts = rows.reduce((acc, row) => {
      const stage = String(row.reviewStage || "bootstrap");
      acc[stage] = (acc[stage] || 0) + 1;
      return acc;
    }, {});
    const fallbackGroups = {
      historyCompletion: summarizeGroup(
        "History Completion",
        "history",
        stageCounts.history || 0,
        [{ label: "Queued", value: String(stageCounts.history || 0), note: "wallets in history validation" }],
        [],
        "Validate baseline history backward to first deposit."
      ),
      deepRepair: summarizeGroup(
        "Deep Repair",
        "deep_repair",
        stageCounts.deep_repair || 0,
        [{ label: "Queued", value: String(stageCounts.deep_repair || 0), note: "wallets in deep repair" }],
        [],
        "Repair older history when trades exist before the deposit floor."
      ),
      gapRecovery: summarizeGroup(
        "Gap Recovery",
        "gap",
        stageCounts.gap || 0,
        [{ label: "Queued", value: String(stageCounts.gap || 0), note: "wallets in gap recovery" }],
        [],
        "Close the gap from the March 24-29, 2026 baseline to now."
      ),
      liveMode: summarizeGroup(
        "Live Mode",
        "live",
        (stageCounts.live || 0) + (stageCounts.live_transition || 0),
        [
          { label: "Transition", value: String(stageCounts.live_transition || 0), note: "temporary live catch-up" },
          { label: "Permanent Live", value: String(stageCounts.live || 0), note: "steady-state live tracking" },
          { label: "Verified Zero", value: String(stageCounts.zero_verified || 0), note: "kept out of live mode by design" },
        ],
        [],
        "Live transition and permanent live polling."
      ),
    };
    const reviewPipeline = review && typeof review === "object"
      ? {
          ...review,
          generatedAt: Number(review.generatedAt || Date.now()),
          totalWallets: rows.length,
          visibleWallets,
          hiddenZeroTradeWallets,
          verifiedZeroTradeWallets: Number(review.verifiedZeroTradeWallets || stageCounts.zero_verified || 0),
          stageCounts: review.stageCounts || stageCounts,
          groups: review.groups && typeof review.groups === "object" ? review.groups : fallbackGroups,
        }
      : {
          generatedAt: Date.now(),
          totalWallets: rows.length,
          visibleWallets,
          hiddenZeroTradeWallets,
          verifiedZeroTradeWallets: Number(stageCounts.zero_verified || 0),
          stageCounts,
          baselineThroughDate: "2026-03-29",
          groups: fallbackGroups,
        };
    return {
      generatedAt: Date.now(),
      visibleWallets,
      hiddenZeroTradeWallets,
      verifiedZeroTradeWallets: Number(reviewPipeline.verifiedZeroTradeWallets || 0),
      reviewPipeline,
      nextgen: {
        startedAt: lastSummary.startedAt || null,
        bootstrapEvents: lastSummary.bootstrapEvents,
        reviewEvents: lastSummary.reviewEvents,
        marketEvents: lastSummary.marketEvents,
        projectRuns: lastSummary.projectRuns,
        strictLive: liveMonitor && liveMonitor.summary ? liveMonitor.summary : null,
      },
    };
  }

  return {
    start,
    stop,
    queryWallets,
    getWallet,
    getProfile,
    getSyncHealth,
  };
}

module.exports = {
  createNextgenRuntime,
};
