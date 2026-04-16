#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

const ROOT = path.resolve(__dirname, "..");
const ISOLATED_DIR = path.join(ROOT, "data", "wallet_explorer_v2", "isolated");
const NEXTGEN_CURRENT_PATH = path.join(ROOT, "data", "nextgen", "state", "wallet-current-state.json");
const NEXTGEN_LIVE_PATH = path.join(ROOT, "data", "nextgen", "state", "wallet-live-status.json");
const V3_DIR = path.join(ROOT, "data", "wallet_explorer_v3");
const STATE_PATH = path.join(V3_DIR, "refresh_state.json");
const MANIFEST_PATH = path.join(V3_DIR, "manifest.json");

try {
  if (!process.env.PACIFICA_WALLET_EXPLORER_V3_SHARDS && fs.existsSync(MANIFEST_PATH)) {
    const manifest = JSON.parse(fs.readFileSync(MANIFEST_PATH, "utf8"));
    const shardCount = Number(manifest && manifest.shardCount ? manifest.shardCount : 0);
    if (Number.isFinite(shardCount) && shardCount > 0) {
      process.env.PACIFICA_WALLET_EXPLORER_V3_SHARDS = String(Math.trunc(shardCount));
    }
  }
} catch {}

const {
  loadWalletExplorerV3Snapshot,
  writeWalletExplorerV3Snapshot,
} = require(path.join(ROOT, "src", "services", "read_model", "wallet_storage_v3"));
const {
  buildLeaderboardIndex,
  fetchPacificaPublicWalletSources,
  loadPacificaPublicWalletDetailCache,
  loadPacificaPublicWalletSourcesSnapshot,
  writePacificaPublicWalletSourcesSnapshot,
} = require(path.join(ROOT, "src", "services", "read_model", "pacifica_public_wallet_sources"));
const {
  ensureDir,
  readJson,
  writeJsonAtomic,
} = require(path.join(ROOT, "src", "services", "pipeline", "utils"));

const args = new Set(process.argv.slice(2));
const FORCE = args.has("--force");

function toNum(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function normalizeWallet(value) {
  return String(value || "").trim();
}

function readMtimeMs(filePath) {
  try {
    return Math.trunc(fs.statSync(filePath).mtimeMs || 0);
  } catch {
    return 0;
  }
}

function hasOwn(value, key) {
  return Boolean(value) && Object.prototype.hasOwnProperty.call(value, key);
}

function loadLastOpenedAt(historyPath, previousState = null) {
  const historyMtimeMs = readMtimeMs(historyPath);
  if (!historyMtimeMs) {
    return { lastOpenedAt: null, historyMtimeMs: 0, parsed: false };
  }
  if (
    previousState &&
    Number(previousState.historyMtimeMs || 0) === historyMtimeMs &&
    Object.prototype.hasOwnProperty.call(previousState, "lastOpenedAt")
  ) {
    return {
      lastOpenedAt: toNum(previousState.lastOpenedAt, 0) || null,
      historyMtimeMs,
      parsed: false,
    };
  }
  const payload = readJson(historyPath, null);
  const trades = Array.isArray(payload && payload.trades) ? payload.trades : [];
  let lastOpenedAt = 0;
  for (const trade of trades) {
    if (!trade || typeof trade !== "object") continue;
    const side = String(trade.side || "").trim().toLowerCase();
    if (!side.startsWith("open_")) continue;
    const ts = Math.max(
      toNum(trade.timestamp, 0),
      toNum(trade.created_at, 0),
      toNum(trade.createdAt, 0),
      toNum(trade.openedAt, 0)
    );
    if (ts > lastOpenedAt) lastOpenedAt = ts;
  }
  return {
    lastOpenedAt: lastOpenedAt || null,
    historyMtimeMs,
    parsed: true,
  };
}

function buildFingerprint(
  recordMtimeMs,
  stateMtimeMs,
  historyMtimeMs,
  nextgenWallet = null,
  nextgenLive = null,
  detailEntry = null
) {
  return [
    recordMtimeMs,
    stateMtimeMs,
    historyMtimeMs,
    toNum(nextgenWallet && nextgenWallet.updated_at, 0),
    toNum(nextgenWallet && nextgenWallet.last_opened_at, 0),
    toNum(nextgenWallet && nextgenWallet.last_trade_at, 0),
    toNum(nextgenWallet && nextgenWallet.open_positions, -1),
    toNum(nextgenWallet && nextgenWallet.unrealized_pnl_usd, 0),
    toNum(nextgenLive && nextgenLive.updated_at, 0),
    toNum(nextgenLive && nextgenLive.last_opened_at, 0),
    toNum(nextgenLive && nextgenLive.last_trade_at, 0),
    toNum(nextgenLive && nextgenLive.open_positions, -1),
    toNum(nextgenLive && nextgenLive.lastLocalStateUpdateAt, 0),
    toNum(detailEntry && detailEntry.fetchedAt, 0),
    toNum(detailEntry && detailEntry.volumeAllTime, 0),
    toNum(detailEntry && detailEntry.pnlAllTime, 0),
    toNum(detailEntry && detailEntry.drawdownPct, 0),
    toNum(detailEntry && detailEntry.returnPct, 0),
    toNum(detailEntry && detailEntry.accountEquityUsd, 0),
    toNum(detailEntry && detailEntry.openPositions, -1),
  ].join(":");
}

function buildEndpointSeedRow(wallet, endpointRow, fetchedAt, detailEntry = null) {
  if (!endpointRow || typeof endpointRow !== "object") return null;
  const username = String(endpointRow.username || "").trim() || null;
  const firstTrade = toNum(detailEntry && detailEntry.firstSeenAt, 0) || null;
  const lastTrade = toNum(detailEntry && detailEntry.lastSeenAt, 0) || null;
  return {
    wallet,
    walletRecordId: null,
    createdAt: null,
    updatedAt: fetchedAt || Date.now(),
    trades: 0,
    volumeUsdRaw: toNum(endpointRow.volumeAllTime, 0),
    volumeUsd: toNum(endpointRow.volumeAllTime, 0),
    pnlUsd: toNum(endpointRow.pnlAllTime, 0),
    unrealizedPnlUsd: 0,
    totalPnlUsd: toNum(endpointRow.pnlAllTime, 0),
    totalWins: 0,
    totalLosses: 0,
    winRate: 0,
    openPositions: 0,
    firstTrade,
    lastTrade,
    lastOpenedAt: null,
    lastActivity: null,
    exposureUsd: Math.max(0, toNum(endpointRow.oiCurrent, 0)),
    feesPaidUsd: 0,
    feesUsd: 0,
    netFeesUsd: 0,
    feeRebatesUsd: 0,
    revenueUsd: 0,
    d24: null,
    d7: null,
    d30: null,
    all: null,
    completeness: {
      historyVerified: false,
      metricsVerified: false,
      leaderboardEligible: false,
      endpointSeeded: true,
      endpointCoverage: ["leaderboard", "portfolio_detail"],
      leaderboardBlockedReasons: ["endpoint_seeded_missing_canonical_history"],
    },
    searchText: `${wallet} ${username || ""}`.trim().toLowerCase(),
    symbols: [],
    symbolBreakdown: [],
    lifecycleStage: "endpoint_seeded",
    status: {
      coverageModel: "public_endpoint_seed",
      sourceCoverage: {
        leaderboard: true,
        portfolioDetail: true,
      },
      publicEndpoints: {
        username,
        rankAllTime: toNum(endpointRow.rankAllTime, 0) || null,
        pnlAllTime: toNum(endpointRow.pnlAllTime, 0),
        volumeAllTime: toNum(endpointRow.volumeAllTime, 0),
        equityCurrent: toNum(endpointRow.equityCurrent, 0),
        oiCurrent: toNum(endpointRow.oiCurrent, 0),
        fetchedAt: fetchedAt || Date.now(),
        firstSeenAt: firstTrade,
        lastSeenAt: lastTrade,
      },
    },
  };
}

function applyPortfolioMetricOverlay(row = {}, detailEntry = null) {
  const safeRow = row && typeof row === "object" ? { ...row } : {};
  const detail = detailEntry && typeof detailEntry === "object" ? detailEntry : null;
  if (!detail) return safeRow;
  const volumeAllTime = Number(detail.volumeAllTime || 0);
  const pnlAllTime = Number(detail.pnlAllTime || 0);
  const drawdownPct = Number(detail.drawdownPct || 0);
  const returnPct = Number(detail.returnPct || 0);
  const accountEquityUsd = Number(detail.accountEquityUsd || 0);
  const openPositions = Math.max(0, Number(detail.openPositions || 0));
  if (Number.isFinite(volumeAllTime)) {
    safeRow.volumeUsdRaw = volumeAllTime;
    safeRow.volumeUsd = volumeAllTime;
  }
  if (Number.isFinite(pnlAllTime)) {
    safeRow.pnlUsd = pnlAllTime;
    safeRow.totalPnlUsd = pnlAllTime;
    safeRow.unrealizedPnlUsd = 0;
  }
  if (Number.isFinite(drawdownPct)) safeRow.drawdownPct = drawdownPct;
  if (Number.isFinite(returnPct)) safeRow.returnPct = returnPct;
  if (Number.isFinite(accountEquityUsd)) safeRow.accountEquityUsd = accountEquityUsd;
  safeRow.openPositions = openPositions;
  const baseAll = safeRow.all && typeof safeRow.all === "object" ? { ...safeRow.all } : {};
  safeRow.all = {
    ...baseAll,
    volumeUsd: Number.isFinite(volumeAllTime) ? volumeAllTime : Number(baseAll.volumeUsd || 0),
    pnlUsd: Number.isFinite(pnlAllTime) ? pnlAllTime : Number(baseAll.pnlUsd || 0),
    drawdownPct: Number.isFinite(drawdownPct) ? drawdownPct : Number(baseAll.drawdownPct || 0),
    returnPct: Number.isFinite(returnPct) ? returnPct : Number(baseAll.returnPct || 0),
    accountEquityUsd: Number.isFinite(accountEquityUsd)
      ? accountEquityUsd
      : Number(baseAll.accountEquityUsd || 0),
    openPositions,
  };
  const baseStatus = safeRow.status && typeof safeRow.status === "object" ? { ...safeRow.status } : {};
  safeRow.status = {
    ...baseStatus,
    metricSources: {
      ...(baseStatus.metricSources && typeof baseStatus.metricSources === "object"
        ? baseStatus.metricSources
        : {}),
      volumeUsd: "portfolio_volume",
      pnlUsd: "portfolio_timeline",
      drawdownPct: "portfolio_timeline",
      returnPct: "portfolio_timeline",
      accountEquityUsd: "portfolio_account",
      openPositions: "portfolio_account",
    },
    portfolioMetrics: {
      source: String(detail.source || "portfolio_timeline_volume_account"),
      fetchedAt: Number(detail.fetchedAt || 0) || null,
      firstSeenAt: Number(detail.firstSeenAt || 0) || null,
      lastSeenAt: Number(detail.lastSeenAt || 0) || null,
      accountUpdatedAt: Number(detail.accountUpdatedAt || 0) || null,
    },
  };
  return safeRow;
}

function buildRow(
  wallet,
  baseRow,
  record,
  lastOpenedAt,
  nextgenWallet,
  nextgenLive,
  endpointRow,
  endpointSnapshot,
  detailEntry = null
) {
  const endpointSeed =
    !baseRow && !(record && typeof record === "object" && Object.keys(record).length) && endpointRow
      ? buildEndpointSeedRow(wallet, endpointRow, endpointSnapshot && endpointSnapshot.generatedAt, detailEntry)
      : null;
  const base = baseRow && typeof baseRow === "object" ? { ...baseRow } : endpointSeed ? { ...endpointSeed } : {};
  const source = record && typeof record === "object" ? record : {};
  const canonicalLastOpenedAt =
    Math.max(
      toNum(lastOpenedAt, 0),
      toNum(source.lastOpenedAt, 0),
      toNum(source.lastActivity, 0),
      toNum(source.lastActivityAt, 0),
      toNum(source.all && source.all.lastOpenedAt, 0),
      toNum(base.lastOpenedAt, 0),
      toNum(base.lastActivity, 0)
    ) || null;

  const liveOpenPositionsAvailable =
    hasOwn(nextgenWallet, "open_positions") || hasOwn(nextgenLive, "open_positions");
  const liveOpenPositions = Math.max(
    0,
    toNum(nextgenWallet && nextgenWallet.open_positions, 0),
    toNum(nextgenLive && nextgenLive.open_positions, 0)
  );
  const openPositions = liveOpenPositionsAvailable
    ? liveOpenPositions
    : Math.max(0, toNum(source.openPositions ?? base.openPositions, 0));

  const liveExposureAvailable =
    hasOwn(nextgenWallet, "exposure_usd") || hasOwn(nextgenLive, "exposure_usd");
  const exposureUsd = liveExposureAvailable
    ? Math.max(
        0,
        toNum(nextgenWallet && nextgenWallet.exposure_usd, 0),
        toNum(nextgenLive && nextgenLive.exposure_usd, 0)
      )
    : Math.max(0, toNum(source.exposureUsd ?? base.exposureUsd, 0));

  const liveUnrealizedAvailable =
    hasOwn(nextgenWallet, "unrealized_pnl_usd") || hasOwn(nextgenLive, "unrealized_pnl_usd");
  const unrealizedPnlUsd = liveUnrealizedAvailable
    ? toNum(
        hasOwn(nextgenWallet, "unrealized_pnl_usd")
          ? nextgenWallet.unrealized_pnl_usd
          : nextgenLive && nextgenLive.unrealized_pnl_usd,
        0
      )
    : toNum(source.unrealizedPnlUsd ?? base.unrealizedPnlUsd, 0);

  const pnlUsd = toNum(source.pnlUsd ?? base.pnlUsd, 0);
  const totalPnlUsd = toNum(source.totalPnlUsd, pnlUsd + unrealizedPnlUsd);
  const lastTrade = Math.max(
    toNum(source.lastTrade, 0),
    toNum(base.lastTrade, 0),
    toNum(nextgenWallet && nextgenWallet.last_trade_at, 0),
    toNum(nextgenLive && nextgenLive.last_trade_at, 0)
  ) || null;
  const firstTrade = Math.max(
    toNum(source.firstTrade, 0),
    toNum(base.firstTrade, 0),
    toNum(nextgenWallet && nextgenWallet.first_trade_at, 0),
    toNum(detailEntry && detailEntry.firstSeenAt, 0)
  ) || null;
  const canonicalLastTrade = Math.max(
    toNum(source.lastTrade, 0),
    toNum(base.lastTrade, 0),
    toNum(nextgenWallet && nextgenWallet.last_trade_at, 0),
    toNum(nextgenLive && nextgenLive.last_trade_at, 0),
    toNum(detailEntry && detailEntry.lastSeenAt, 0)
  ) || null;
  const updatedAt = Math.max(
    toNum(source.updatedAt, 0),
    toNum(base.updatedAt, 0),
    toNum(nextgenWallet && nextgenWallet.updated_at, 0),
    toNum(nextgenLive && nextgenLive.updated_at, 0),
    toNum(nextgenLive && nextgenLive.lastLocalStateUpdateAt, 0),
    toNum(canonicalLastTrade, 0),
    toNum(lastOpenedAt, 0)
  ) || null;

  const mergedStatus = {
    ...(base.status && typeof base.status === "object" ? base.status : {}),
    ...(source.status && typeof source.status === "object" ? source.status : {}),
    coverageModel: endpointRow
      ? baseRow || (record && Object.keys(record).length)
        ? "pipeline_plus_public_endpoint"
        : "public_endpoint_seed"
      : "pipeline_only",
    sourceCoverage: {
      leaderboard: Boolean(endpointRow),
      portfolioDetail: Boolean(endpointRow),
    },
  };
  if (endpointRow) {
    mergedStatus.publicEndpoints = {
      username: String(endpointRow.username || "").trim() || null,
      rankAllTime: toNum(endpointRow.rankAllTime, 0) || null,
      pnlAllTime: toNum(endpointRow.pnlAllTime, 0),
      volumeAllTime: toNum(endpointRow.volumeAllTime, 0),
      equityCurrent: toNum(endpointRow.equityCurrent, 0),
      oiCurrent: toNum(endpointRow.oiCurrent, 0),
      fetchedAt: endpointSnapshot && endpointSnapshot.generatedAt ? endpointSnapshot.generatedAt : null,
      firstSeenAt: toNum(detailEntry && detailEntry.firstSeenAt, 0) || null,
      lastSeenAt: toNum(detailEntry && detailEntry.lastSeenAt, 0) || null,
    };
  }

  return applyPortfolioMetricOverlay({
    ...base,
    ...source,
    wallet,
    trades: Math.max(0, toNum(source.trades ?? base.trades, 0)),
    volumeUsdRaw: toNum(source.volumeUsdRaw ?? source.volumeUsd ?? base.volumeUsdRaw ?? base.volumeUsd, 0),
    volumeUsd: toNum(source.volumeUsd ?? source.volumeUsdRaw ?? base.volumeUsd ?? base.volumeUsdRaw, 0),
    pnlUsd,
    unrealizedPnlUsd,
    totalPnlUsd,
    totalWins: Math.max(0, toNum(source.totalWins ?? base.totalWins, 0)),
    totalLosses: Math.max(0, toNum(source.totalLosses ?? base.totalLosses, 0)),
    winRate: toNum(source.winRate ?? base.winRate, 0),
    openPositions,
    firstTrade,
    lastTrade: canonicalLastTrade,
    lastOpenedAt: canonicalLastOpenedAt,
    lastActivity: canonicalLastOpenedAt,
    lastActivityAt: canonicalLastOpenedAt,
    lastActiveAt: canonicalLastOpenedAt,
    updatedAt,
    exposureUsd,
    feesPaidUsd: toNum(source.feesPaidUsd ?? source.feesUsd ?? base.feesPaidUsd ?? base.feesUsd, 0),
    feesUsd: toNum(source.feesUsd ?? source.feesPaidUsd ?? base.feesUsd ?? base.feesPaidUsd, 0),
    netFeesUsd: toNum(source.netFeesUsd ?? base.netFeesUsd, 0),
    feeRebatesUsd: toNum(source.feeRebatesUsd ?? base.feeRebatesUsd, 0),
    revenueUsd: toNum(source.revenueUsd ?? base.revenueUsd, 0),
    d24: source.d24 && typeof source.d24 === "object" ? source.d24 : base.d24 || null,
    d7: source.d7 && typeof source.d7 === "object" ? source.d7 : base.d7 || null,
    d30: source.d30 && typeof source.d30 === "object" ? source.d30 : base.d30 || null,
    all: source.all && typeof source.all === "object" ? source.all : base.all || null,
    completeness:
      source.completeness && typeof source.completeness === "object"
        ? source.completeness
        : base.completeness || null,
    searchText: String(source.searchText || base.searchText || "").toLowerCase(),
    symbols: Array.isArray(source.symbols)
      ? source.symbols.slice()
      : Array.isArray(base.symbols)
        ? base.symbols.slice()
        : [],
    symbolBreakdown: Array.isArray(source.symbolBreakdown)
      ? source.symbolBreakdown
      : Array.isArray(base.symbolBreakdown)
        ? base.symbolBreakdown
        : [],
    lifecycleStage:
      nextgenWallet && nextgenWallet.review_stage
        ? nextgenWallet.review_stage
        : source.lifecycleStage || source.lifecycle || base.lifecycleStage || base.lifecycle || null,
    status: mergedStatus,
  }, detailEntry);
}

async function main() {
  ensureDir(V3_DIR);
  const startedAt = Date.now();
  const snapshot = loadWalletExplorerV3Snapshot();
  if (!snapshot || !Array.isArray(snapshot.rows)) {
    throw new Error(`missing wallet_explorer_v3 snapshot at ${MANIFEST_PATH}`);
  }

  const previousState = readJson(STATE_PATH, {}) || {};
  const previousWallets =
    previousState.wallets && typeof previousState.wallets === "object" ? previousState.wallets : {};

  const nextgenWallets = readJson(NEXTGEN_CURRENT_PATH, {}) || {};
  const nextgenLive = readJson(NEXTGEN_LIVE_PATH, {}) || {};
  const rowsByWallet = new Map(snapshot.rows.map((row) => [normalizeWallet(row.wallet), row]));
  let endpointSnapshot = loadPacificaPublicWalletSourcesSnapshot();
  try {
    endpointSnapshot = await fetchPacificaPublicWalletSources();
    writePacificaPublicWalletSourcesSnapshot(endpointSnapshot);
  } catch (_error) {}
  const leaderboardIndex = buildLeaderboardIndex(endpointSnapshot);
  const detailCache = loadPacificaPublicWalletDetailCache();
  const detailByWallet =
    detailCache && detailCache.wallets && typeof detailCache.wallets === "object"
      ? detailCache.wallets
      : {};

  const wallets = new Set(rowsByWallet.keys());
  try {
    const entries = fs.readdirSync(ISOLATED_DIR, { withFileTypes: true });
    for (const entry of entries) {
      if (entry && entry.isDirectory()) wallets.add(entry.name);
    }
  } catch {}
  Object.keys(nextgenWallets).forEach((wallet) => wallets.add(normalizeWallet(wallet)));
  Object.keys(nextgenLive).forEach((wallet) => wallets.add(normalizeWallet(wallet)));
  leaderboardIndex.forEach((_row, wallet) => wallets.add(normalizeWallet(wallet)));

  const nextState = {
    version: 1,
    generatedAt: 0,
    walletCount: wallets.size,
    lastActivitySource: "isolated_wallet_open_history",
    refreshMode: "incremental_canonical",
    wallets: {},
  };

  const nextRows = [];
  let changedWallets = 0;
  let unchangedWallets = 0;
  let parsedHistories = 0;
  let endpointCoveredWallets = 0;
  let endpointSeededWallets = 0;

  const sortedWallets = Array.from(wallets).filter(Boolean).sort();
  for (const wallet of sortedWallets) {
    const walletDir = path.join(ISOLATED_DIR, wallet);
    const recordPath = path.join(walletDir, "wallet_record.json");
    const statePath = path.join(walletDir, "wallet_state.json");
    const historyPath = path.join(walletDir, "wallet_history.json");

    const recordMtimeMs = readMtimeMs(recordPath);
    const stateMtimeMs = readMtimeMs(statePath);
    const historyInfo = loadLastOpenedAt(historyPath, previousWallets[wallet]);
    const nextgenWallet = nextgenWallets[wallet] && typeof nextgenWallets[wallet] === "object"
      ? nextgenWallets[wallet]
      : null;
    const nextgenLiveRow = nextgenLive[wallet] && typeof nextgenLive[wallet] === "object"
      ? nextgenLive[wallet]
      : null;

    if (historyInfo.parsed) parsedHistories += 1;

    const detailEntry = detailByWallet[wallet] || null;
    const fingerprint = buildFingerprint(
      recordMtimeMs,
      stateMtimeMs,
      historyInfo.historyMtimeMs,
      nextgenWallet,
      nextgenLiveRow,
      detailEntry
    );
    const previous = previousWallets[wallet] || null;
    const baseRow = rowsByWallet.get(wallet) || null;
    const endpointRow = leaderboardIndex.get(wallet) || null;

    const record = recordMtimeMs ? readJson(recordPath, {}) || {} : {};
    const canonicalLastOpenedAt =
      Math.max(
        toNum(historyInfo.lastOpenedAt, 0),
        toNum(record.lastOpenedAt, 0),
        toNum(record.lastActivity, 0),
        toNum(record.lastActivityAt, 0),
        toNum(record.all && record.all.lastOpenedAt, 0)
      ) || null;

    nextState.wallets[wallet] = {
      fingerprint,
      recordMtimeMs,
      stateMtimeMs,
      historyMtimeMs: historyInfo.historyMtimeMs,
      lastOpenedAt: canonicalLastOpenedAt,
    };

    if (!FORCE && previous && previous.fingerprint === fingerprint && baseRow) {
      if (endpointRow) endpointCoveredWallets += 1;
      nextRows.push(
        endpointRow
          ? buildRow(
              wallet,
              baseRow,
              record,
              canonicalLastOpenedAt,
              nextgenWallet,
              nextgenLiveRow,
              endpointRow,
              endpointSnapshot,
              detailEntry
            )
          : baseRow
      );
      unchangedWallets += 1;
      continue;
    }

    if (endpointRow) {
      endpointCoveredWallets += 1;
      if (!baseRow && !recordMtimeMs && !stateMtimeMs && !historyInfo.historyMtimeMs) {
        endpointSeededWallets += 1;
      }
    }
    nextRows.push(
      buildRow(
        wallet,
        baseRow,
        record,
        canonicalLastOpenedAt,
        nextgenWallet,
        nextgenLiveRow,
        endpointRow,
        endpointSnapshot,
        detailEntry
      )
    );
    changedWallets += 1;
  }

  const manifest = snapshot.manifest && typeof snapshot.manifest === "object" ? snapshot.manifest : {};
  const writtenManifest = writeWalletExplorerV3Snapshot({
    sourcePath: "canonical_incremental_refresh",
    rows: nextRows,
    counts: manifest.counts || snapshot.counts || null,
    availableSymbols: manifest.availableSymbols || snapshot.availableSymbols || null,
    manifestPath: MANIFEST_PATH,
  });

  const manifestPayload = readJson(MANIFEST_PATH, {}) || {};
  manifestPayload.generatedAt = writtenManifest.generatedAt || Date.now();
  manifestPayload.lastActivitySource = "isolated_wallet_open_history";
  manifestPayload.historicalSource = "wallet_explorer_v2_isolated_records_and_history";
  manifestPayload.liveOverlaySource = "nextgen_wallet_state";
  manifestPayload.refreshMode = "incremental_canonical";
  manifestPayload.publicEndpointCoverage = {
    leaderboardWallets: leaderboardIndex.size,
    endpointCoveredWallets,
    endpointSeededWallets,
    pipelineOnlyWallets: Math.max(0, nextRows.length - endpointCoveredWallets),
    portfolioMode:
      endpointSnapshot && endpointSnapshot.portfolio && endpointSnapshot.portfolio.mode
        ? endpointSnapshot.portfolio.mode
        : "account_scoped_detail",
  };
  writeJsonAtomic(MANIFEST_PATH, manifestPayload);

  nextState.generatedAt = manifestPayload.generatedAt;
  nextState.publicEndpointCoverage = manifestPayload.publicEndpointCoverage;
  writeJsonAtomic(STATE_PATH, nextState);

  console.log(
    JSON.stringify({
      done: true,
      walletCount: nextRows.length,
      changedWallets,
      unchangedWallets,
      parsedHistories,
      endpointCoveredWallets,
      endpointSeededWallets,
      elapsedMs: Date.now() - startedAt,
      manifestPath: MANIFEST_PATH,
      statePath: STATE_PATH,
    })
  );
}

main().catch((error) => {
  console.error(error && error.stack ? error.stack : String(error));
  process.exitCode = 1;
});
