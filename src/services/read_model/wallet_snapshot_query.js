const fs = require("fs");
const path = require("path");

const { normalizeAddress, readJson } = require("../pipeline/utils");
const { STATE_PATHS } = require("../nextgen/core/state-store");
const {
  loadWalletExplorerV3Snapshot,
} = require("./wallet_storage_v3");
const {
  listShardSnapshotFiles,
  loadMergedShardSnapshot,
} = require("../analytics/live_positions_snapshot_store");

function toNum(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function cloneJson(value) {
  if (value === null || value === undefined) return value;
  return JSON.parse(JSON.stringify(value));
}

const nextgenStateCache = {
  walletsMtimeMs: 0,
  liveMtimeMs: 0,
  wallets: null,
  live: null,
};

const isolatedOpenedAtCache = new Map();

function readMtimeMs(filePath) {
  try {
    return Math.trunc(fs.statSync(filePath).mtimeMs || 0);
  } catch {
    return 0;
  }
}

function loadIsolatedOpenedAt(isolatedDir, wallet) {
  const normalizedWallet = normalizeAddress(String(wallet || "").trim());
  if (!normalizedWallet || !isolatedDir) return null;
  const filePath = path.join(isolatedDir, normalizedWallet, "wallet_history.json");
  const mtimeMs = readMtimeMs(filePath);
  const cached = isolatedOpenedAtCache.get(normalizedWallet);
  if (cached && cached.mtimeMs === mtimeMs) {
    return cached.openedAt;
  }
  const payload = readJson(filePath, null);
  const trades = Array.isArray(payload && payload.trades) ? payload.trades : [];
  let openedAt = 0;
  for (const row of trades) {
    if (!row || typeof row !== "object") continue;
    const side = String(row.side || "").trim().toLowerCase();
    if (!side.startsWith("open_")) continue;
    const ts = toNum(row.timestamp || row.created_at || row.createdAt || 0, 0);
    if (ts > openedAt) openedAt = ts;
  }
  const value = openedAt > 0 ? openedAt : null;
  isolatedOpenedAtCache.set(normalizedWallet, { mtimeMs, openedAt: value });
  return value;
}

function loadNextgenStateOverlay() {
  const walletsMtimeMs = readMtimeMs(STATE_PATHS.wallets);
  const liveMtimeMs = readMtimeMs(STATE_PATHS.live);
  if (
    nextgenStateCache.wallets &&
    nextgenStateCache.live &&
    nextgenStateCache.walletsMtimeMs === walletsMtimeMs &&
    nextgenStateCache.liveMtimeMs === liveMtimeMs
  ) {
    return {
      wallets: nextgenStateCache.wallets,
      live: nextgenStateCache.live,
    };
  }
  const walletsPayload = readJson(STATE_PATHS.wallets, {}) || {};
  const livePayload = readJson(STATE_PATHS.live, {}) || {};
  nextgenStateCache.walletsMtimeMs = walletsMtimeMs;
  nextgenStateCache.liveMtimeMs = liveMtimeMs;
  nextgenStateCache.wallets = walletsPayload && typeof walletsPayload === "object" ? walletsPayload : {};
  nextgenStateCache.live = livePayload && typeof livePayload === "object" ? livePayload : {};
  return {
    wallets: nextgenStateCache.wallets,
    live: nextgenStateCache.live,
  };
}

function clampMetric(value, min = 0, max = 100) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return min;
  return Math.min(max, Math.max(min, numeric));
}

function scalePositive(value, target = 1) {
  const numeric = Math.max(0, toNum(value, 0));
  const ceiling = Math.max(1, toNum(target, 1));
  if (numeric <= 0) return 0;
  return clampMetric(Math.log1p(numeric) / Math.log1p(ceiling), 0, 1);
}

function scaleRange(value, min = 0, max = 1) {
  const numeric = toNum(value, NaN);
  if (!Number.isFinite(numeric)) return 0;
  const low = Number(min);
  const high = Number(max);
  if (!Number.isFinite(low) || !Number.isFinite(high) || high <= low) return 0;
  return clampMetric((numeric - low) / (high - low), 0, 1);
}

function freshnessFactor(lastActivityAt, nowMs = Date.now()) {
  const ts = Number(lastActivityAt || 0);
  if (!Number.isFinite(ts) || ts <= 0) return 0.05;
  const ageMs = Math.max(0, nowMs - ts);
  if (ageMs <= 15 * 60 * 1000) return 1;
  if (ageMs <= 60 * 60 * 1000) return 0.94;
  if (ageMs <= 6 * 60 * 60 * 1000) return 0.82;
  if (ageMs <= 24 * 60 * 60 * 1000) return 0.64;
  if (ageMs <= 3 * 24 * 60 * 60 * 1000) return 0.38;
  if (ageMs <= 7 * 24 * 60 * 60 * 1000) return 0.18;
  return 0.05;
}

function getCanonicalLastOpenedAt(row = {}, isolatedDir = null) {
  const safe = row && typeof row === "object" ? row : {};
  const candidates = [
    safe.lastOpenedAt,
    safe.lastOpenedPosition && safe.lastOpenedPosition.openedAt,
    safe.positionOpenedAt,
    safe.lastPositionOpenedAt,
    safe.d24 && safe.d24.lastOpenedAt,
    safe.d7 && safe.d7.lastOpenedAt,
    safe.d30 && safe.d30.lastOpenedAt,
    safe.all && safe.all.lastOpenedAt,
    safe.wallet ? loadIsolatedOpenedAt(isolatedDir, safe.wallet) : null,
  ];
  for (const candidate of candidates) {
    const ts = toNum(candidate, 0);
    if (ts > 0) return ts;
  }
  return null;
}

function getCanonicalLastActivityAt(row = {}, isolatedDir = null) {
  const safe = row && typeof row === "object" ? row : {};
  const candidates = [
    safe.lastTrade,
    safe.lastActivity,
    safe.lastActivityAt,
    safe.lastActiveAt,
    safe.all && safe.all.lastTrade,
    safe.d30 && safe.d30.lastTrade,
    safe.d7 && safe.d7.lastTrade,
    safe.d24 && safe.d24.lastTrade,
  ];
  for (const candidate of candidates) {
    const ts = toNum(candidate, 0);
    if (ts > 0) return ts;
  }
  return getCanonicalLastOpenedAt(safe, isolatedDir);
}

function mergeNextgenOverlay(row = {}, overlay = null, isolatedDir = null) {
  const safeRow = row && typeof row === "object" ? row : {};
  const state = overlay && overlay.wallets ? overlay.wallets[safeRow.wallet] : null;
  const live = overlay && overlay.live ? overlay.live[safeRow.wallet] : null;
  if (!state && !live) return safeRow;
  const nextgenWallet = state && typeof state === "object" ? state : {};
  const nextgenLive = live && typeof live === "object" ? live : {};
  const isolatedOpenedAt = safeRow.wallet ? loadIsolatedOpenedAt(isolatedDir, safeRow.wallet) : null;
  const currentLastTradeAt = Math.max(
    toNum(nextgenWallet.last_trade_at, 0),
    toNum(nextgenLive.last_trade_at, 0),
    toNum(safeRow.lastTrade, 0)
  );
  const currentLastActivityAt = Math.max(
    currentLastTradeAt,
    toNum(safeRow.lastActivity, 0),
    toNum(safeRow.lastActivityAt, 0),
    toNum(safeRow.lastActiveAt, 0),
    toNum(nextgenWallet.last_opened_at, 0),
    toNum(nextgenLive.last_opened_at, 0),
    toNum(isolatedOpenedAt, 0)
  );
  const currentOpenPositions = Math.max(
    0,
    toNum(nextgenWallet.open_positions, 0),
    toNum(nextgenLive.open_positions, 0),
    toNum(safeRow.openPositions, 0)
  );
  const currentUpdatedAt = Math.max(
    toNum(nextgenWallet.updated_at, 0),
    toNum(nextgenLive.updated_at, 0),
    toNum(nextgenLive.lastLocalStateUpdateAt, 0),
    currentLastActivityAt,
    currentLastTradeAt,
    toNum(safeRow.updatedAt, 0)
  );
  const currentVolumeUsd = Math.max(
    toNum(nextgenWallet.volume_usd, 0),
    toNum(safeRow.volumeUsdRaw, 0),
    toNum(safeRow.volumeUsd, 0)
  );
  const currentTrades = Math.max(
    0,
    toNum(nextgenWallet.trades, 0),
    toNum(safeRow.trades, 0)
  );
  const currentWins = Math.max(
    0,
    toNum(nextgenWallet.total_wins, 0),
    toNum(safeRow.totalWins, 0)
  );
  const currentLosses = Math.max(
    0,
    toNum(nextgenWallet.total_losses, 0),
    toNum(safeRow.totalLosses, 0)
  );
  const currentPnlUsd = toNum(nextgenWallet.realized_pnl_usd, toNum(safeRow.pnlUsd, 0));
  const currentUnrealizedPnlUsd = toNum(nextgenWallet.unrealized_pnl_usd, toNum(safeRow.unrealizedPnlUsd, 0));
  const currentTotalPnlUsd = currentPnlUsd + currentUnrealizedPnlUsd;
  const currentWinRate = toNum(nextgenWallet.win_rate, toNum(safeRow.winRate, 0));
  const currentFirstTrade = Math.max(
    0,
    toNum(nextgenWallet.first_trade_at, 0),
    toNum(safeRow.firstTrade, 0)
  ) || null;
  const symbolBreakdown = Array.isArray(nextgenWallet.symbol_breakdown)
    ? nextgenWallet.symbol_breakdown.map((item) => ({
        symbol: String(item && item.symbol || "").trim().toUpperCase(),
        volumeUsd: toNum(item && item.volumeUsd !== undefined ? item.volumeUsd : item && item.volume_usd, 0),
      })).filter((item) => item.symbol)
    : Array.isArray(safeRow.symbolBreakdown)
      ? safeRow.symbolBreakdown
      : [];
  return {
    ...safeRow,
    trades: currentTrades,
    volumeUsd: currentVolumeUsd,
    volumeUsdRaw: currentVolumeUsd,
    totalWins: currentWins,
    totalLosses: currentLosses,
    pnlUsd: currentPnlUsd,
    winRate: currentWinRate,
    firstTrade: currentFirstTrade,
    lastTrade: currentLastTradeAt || safeRow.lastTrade || null,
    lastOpenedAt:
      Math.max(
        toNum(nextgenWallet.last_opened_at, 0),
        toNum(nextgenLive.last_opened_at, 0),
        toNum(isolatedOpenedAt, 0),
        toNum(safeRow.lastOpenedAt, 0)
      ) || null,
    lastActivityAt: currentLastActivityAt || safeRow.lastActivityAt || null,
    lastActivity: currentLastActivityAt || safeRow.lastActivity || null,
    lastActiveAt: currentLastActivityAt || safeRow.lastActiveAt || null,
    openPositions: currentOpenPositions,
    exposureUsd: toNum(nextgenWallet.open_positions ? safeRow.exposureUsd : safeRow.exposureUsd, 0),
    unrealizedPnlUsd: currentUnrealizedPnlUsd,
    totalPnlUsd: currentTotalPnlUsd,
    updatedAt: currentUpdatedAt || safeRow.updatedAt || null,
    symbolBreakdown,
    lifecycleStage: nextgenWallet.review_stage || safeRow.lifecycleStage || null,
  };
}

function summarizeBucket(bucket = {}) {
  const safe = bucket && typeof bucket === "object" ? bucket : {};
  return {
    trades: Number(safe.trades || 0),
    volumeUsd: Number(toNum(safe.volumeUsd, 0).toFixed(2)),
    pnlUsd: Number(toNum(safe.pnlUsd, 0).toFixed(2)),
    wins: Number(safe.wins !== undefined ? safe.wins : safe.winCount || 0),
    losses: Number(safe.losses !== undefined ? safe.losses : safe.lossCount || 0),
    winRatePct: Number(toNum(safe.winRatePct !== undefined ? safe.winRatePct : safe.winRate, 0).toFixed(2)),
    feesUsd: Number(toNum(safe.feesUsd !== undefined ? safe.feesUsd : safe.feesPaidUsd, 0).toFixed(2)),
    firstTrade: safe.firstTrade || null,
    lastTrade: safe.lastTrade || null,
    symbolVolumes: safe.symbolVolumes && typeof safe.symbolVolumes === "object" ? safe.symbolVolumes : {},
  };
}

function buildDerivedMetrics(row = {}, nowMs = Date.now(), isolatedDir = null) {
  const d24 = summarizeBucket(row && row.d24);
  const d7 = summarizeBucket(row && row.d7);
  const d30 = summarizeBucket(row && row.d30);
  const all = summarizeBucket(row && row.all);
  const lastOpenedAt = getCanonicalLastOpenedAt(row, isolatedDir);
  const lastActivity = getCanonicalLastActivityAt(row, isolatedDir);
  const source = d30.volumeUsd > 0 ? d30 : all;
  const totalVolume = toNum(source.volumeUsd, 0);
  const topSharePct =
    totalVolume > 0
      ? Number(
          (
            (Math.max(
              0,
              ...Object.values(source.symbolVolumes || {}).map((value) => toNum(value, 0))
            ) /
              totalVolume) *
            100
          ).toFixed(2)
        )
      : 0;
  const sampleFactor = scalePositive(d30.trades || all.trades, 250);
  const freshFactor = freshnessFactor(lastActivity || lastOpenedAt, nowMs);
  const winRate30 = Number(
    toNum(
      d30.trades > 0 ? d30.winRatePct : d7.trades > 0 ? d7.winRatePct : all.winRatePct,
      0
    ).toFixed(2)
  );
  const winRateFactor = scaleRange(winRate30, 43, 62);
  const winRates = [d24, d7, d30]
    .filter((bucket) => Number(bucket.trades || 0) > 0)
    .map((bucket) => toNum(bucket.winRatePct, 0));
  const winSpread = winRates.length >= 2 ? Math.max(...winRates) - Math.min(...winRates) : 0;
  const stabilityFactor = winRates.length >= 2 ? 1 - clampMetric(winSpread / 18, 0, 1) : sampleFactor * 0.85;
  const positiveWindows = [d24.pnlUsd, d7.pnlUsd, d30.pnlUsd].filter((value) => toNum(value, 0) > 0).length;
  const avgTradeUsd = d30.trades > 0 ? toNum(d30.volumeUsd, 0) / Math.max(1, d30.trades) : 0;
  const avgTradeFactor = scalePositive(avgTradeUsd, 25000);
  const concentrationBalance = 1 - clampMetric(Math.abs(topSharePct - 42) / 42, 0, 1);
  const pnlScore = clampMetric(
    scalePositive(Math.max(0, d30.pnlUsd), 150000) * 100 -
      scalePositive(Math.max(0, Math.abs(Math.min(0, d30.pnlUsd))), 75000) * 60,
    0,
    100
  );
  const winLossRatio = Number(d30.wins || 0) / Math.max(1, Number(d30.losses || 0));
  const pnlToFees = toNum(d30.pnlUsd, 0) / Math.max(25, toNum(d30.feesUsd, 0));
  const profitFactorScore = clampMetric(
    scaleRange(winLossRatio, 0.9, 1.8) * 58 +
      clampMetric(Math.max(0, pnlToFees) / 6, 0, 1) * 42,
    0,
    100
  );
  const downsidePressure =
    Math.max(0, -toNum(d24.pnlUsd, 0)) +
    Math.max(0, -toNum(d7.pnlUsd, 0)) * 0.7 +
    Math.max(0, -toNum(d30.pnlUsd, 0)) * 0.45;
  const upsideSupport =
    Math.max(0, toNum(d24.pnlUsd, 0)) +
    Math.max(0, toNum(d7.pnlUsd, 0)) * 0.7 +
    Math.max(0, toNum(d30.pnlUsd, 0)) * 0.45 +
    500;
  const drawdownPct = Number(
    clampMetric(
      (downsidePressure / Math.max(1, downsidePressure + upsideSupport)) * 100 +
        (winRate30 < 45 && toNum(d30.pnlUsd, 0) < 0 ? 10 : 0),
      0,
      95
    ).toFixed(2)
  );
  const pnlPerTradeAbs = Math.abs(toNum(d30.pnlUsd, 0)) / Math.max(1, Number(d30.trades || 0));
  const outlierRisk = clampMetric(
    (pnlPerTradeAbs / 2500) * (30 / Math.max(30, Number(d30.trades || 0))) +
      (topSharePct >= 82 ? 0.22 : topSharePct >= 72 ? 0.1 : 0),
    0,
    1
  );
  const stalePenalty = freshFactor <= 0.18 ? (0.18 - freshFactor) * 50 : 0;
  const tooFewTradesPenalty = Number(d30.trades || 0) < 10 ? 18 : Number(d30.trades || 0) < 25 ? 9 : 0;
  const consistencyScore = Number(
    clampMetric(
      (positiveWindows / 3) * 24 +
        winRateFactor * 30 +
        stabilityFactor * 18 +
        sampleFactor * 16 +
        freshFactor * 12 -
        drawdownPct * 0.14,
      0,
      100
    ).toFixed(2)
  );
  const momentumScore = Number(
    clampMetric(
      scalePositive(Math.max(0, d7.pnlUsd), 50000) * 42 +
        scalePositive(Math.max(0, d24.pnlUsd), 10000) * 28 +
        freshFactor * 18 +
        scaleRange(d24.trades, 5, 100) * 6 +
        scaleRange(d7.winRatePct, 45, 60) * 6 -
        scalePositive(Math.max(0, Math.abs(Math.min(0, d24.pnlUsd))), 10000) * 18,
      0,
      100
    ).toFixed(2)
  );
  const convictionScore = Number(
    clampMetric(
      avgTradeFactor * 34 +
        concentrationBalance * 24 +
        sampleFactor * 18 +
        scaleRange(row.openPositions, 1, 6) * 12 +
        freshFactor * 12,
      0,
      100
    ).toFixed(2)
  );
  const copyabilityScore = Number(
    clampMetric(
      pnlScore * 0.26 +
        consistencyScore * 0.2 +
        profitFactorScore * 0.14 +
        winRateFactor * 100 * 0.12 +
        sampleFactor * 100 * 0.1 +
        (100 - drawdownPct) * 0.1 +
        momentumScore * 0.08 +
        freshFactor * 100 * 0.06 -
        tooFewTradesPenalty -
        outlierRisk * 14 -
        stalePenalty,
      0,
      100
    ).toFixed(2)
  );
  const copyCandidateScore = Number(
    clampMetric(
      copyabilityScore * 0.42 +
        consistencyScore * 0.2 +
        momentumScore * 0.15 +
        (100 - drawdownPct) * 0.13 +
        freshFactor * 100 * 0.1 -
        outlierRisk * 8,
      0,
      100
    ).toFixed(2)
  );
  const badges = [];
  if (consistencyScore >= 72 && drawdownPct <= 35 && Number(d30.trades || 0) >= 25) badges.push("Consistent");
  if (momentumScore >= 68 && toNum(d7.pnlUsd, 0) > 0 && freshFactor >= 0.55) badges.push("Momentum");
  if (drawdownPct >= 60 || outlierRisk >= 0.55 || (Number(d30.trades || 0) < 12 && toNum(d30.pnlUsd, 0) > 0)) {
    badges.push("High Risk");
  }
  if (freshFactor >= 0.85 && (toNum(d24.pnlUsd, 0) > 0 || toNum(d7.pnlUsd, 0) > 0) && Number(d7.trades || 0) >= 10) {
    badges.push("Fresh Alpha");
  }
  return {
    lastActivity,
    lastActiveAt: lastActivity,
    lastOpenedAt,
    trades30: Number(d30.trades || 0),
    pnl7Usd: Number(toNum(d7.pnlUsd, 0).toFixed(2)),
    pnl30Usd: Number(toNum(d30.pnlUsd, 0).toFixed(2)),
    winRate30,
    drawdownPct,
    consistencyScore,
    momentumScore,
    convictionScore,
    copyabilityScore,
    copyCandidateScore,
    profitFactorScore: Number(profitFactorScore.toFixed(2)),
    topSymbolSharePct: Number(topSharePct.toFixed(2)),
    outlierRisk: Number(outlierRisk.toFixed(4)),
    badges,
  };
}

function enrichRow(row = {}, nowMs = Date.now(), isolatedDir = null) {
  const normalizedLastOpenedAt = getCanonicalLastOpenedAt(row, isolatedDir);
  const normalizedLastActivity = getCanonicalLastActivityAt(row, isolatedDir);
  if (
    row &&
    row.copyabilityScore !== undefined &&
    row.consistencyScore !== undefined &&
    row.drawdownPct !== undefined &&
    Array.isArray(row.badges)
  ) {
    return {
      ...row,
      lastOpenedAt: normalizedLastOpenedAt || row.lastOpenedAt || null,
      lastActivity: normalizedLastActivity,
      lastActiveAt: normalizedLastActivity,
    };
  }
  return {
    ...row,
    lastOpenedAt: normalizedLastOpenedAt || row.lastOpenedAt || null,
    lastActivity: normalizedLastActivity,
    lastActiveAt: normalizedLastActivity,
    ...buildDerivedMetrics(row, nowMs, isolatedDir),
  };
}

function sortMeta(sortKey = "volumeUsd", dir = "desc") {
  const key = String(sortKey || "volumeUsd").trim() || "volumeUsd";
  const normalizedDir = String(dir || "desc").trim().toLowerCase() === "asc" ? "asc" : "desc";
  if (key === "rankingVolumeUsd" || key === "volumeUsd") {
    return {
      key: "rankingVolumeUsd",
      dir: normalizedDir,
      label: "Trusted Volume Rank",
      shortLabel: "Trusted Rank",
      description:
        "Visible rank is based on ranking volume. Leaderboard-eligible wallets rank first, then partial wallets by raw signal.",
    };
  }
  if (key === "volumeUsdRaw") {
    return {
      key: "volumeUsdRaw",
      dir: normalizedDir,
      label: "Volume",
      shortLabel: "Volume",
      description:
        "Visible rank is based on wallet volume.",
    };
  }
  const labelMap = {
    wallet: "Wallet Address",
    trades: "Trades",
    trades30: "Trades",
    totalWins: "Wins",
    totalLosses: "Losses",
    pnlUsd: "PnL",
    pnl7Usd: "7D PnL",
    pnl30Usd: "30D PnL",
    winRate: "Win Rate",
    winRate30: "Win Rate",
    drawdownPct: "Drawdown",
    firstTrade: "First Trade",
    lastTrade: "Last Trade",
    lastActivity: "Last Trade",
    openPositions: "Open Positions",
    exposureUsd: "Exposure",
  };
  return {
    key,
    dir: normalizedDir,
    label: labelMap[key] || key,
    shortLabel: labelMap[key] || key,
    description: `Visible rank is based on ${labelMap[key] || key}.`,
  };
}

function normalizeQuery(rawQuery = {}) {
  const source =
    rawQuery && typeof rawQuery.get === "function"
      ? {
          get: (key) => rawQuery.get(key),
          keys: () => rawQuery.keys(),
        }
      : {
          get: (key) => rawQuery[key],
          keys: () => Object.keys(rawQuery || {}),
        };
  const requestedSort = String(source.get("sort") || "volumeUsd").trim();
  return {
    q: String(source.get("q") || "").trim().toLowerCase(),
    page: Math.max(1, Number(source.get("page") || 1) || 1),
    pageSize: Math.max(1, Math.min(200, Number(source.get("pageSize") || 20) || 20)),
    sort:
      {
        wallet: "wallet",
        trades: "trades",
        trades30: "trades30",
        volume: "rankingVolumeUsd",
        volumeUsd: "rankingVolumeUsd",
        rankingVolumeUsd: "rankingVolumeUsd",
        volumeUsdRaw: "volumeUsdRaw",
        totalWins: "totalWins",
        totalLosses: "totalLosses",
        pnl: "pnlUsd",
        pnlUsd: "pnlUsd",
        pnl7Usd: "pnl7Usd",
        pnl30Usd: "pnl30Usd",
        winRate: "winRate",
        winRate30: "winRate30",
        drawdown: "drawdownPct",
        drawdownPct: "drawdownPct",
        firstTrade: "firstTrade",
        lastTrade: "lastTrade",
        lastActivity: "lastTrade",
        openPositions: "openPositions",
        exposureUsd: "exposureUsd",
      }[requestedSort] || "volumeUsdRaw",
    dir: String(source.get("dir") || "desc").trim().toLowerCase() === "asc" ? "asc" : "desc",
    symbol: String(source.get("symbol") || source.get("symbols") || "").trim().toUpperCase(),
    status: Array.from(
      new Set(
        String(source.get("status") || "")
          .split(",")
          .map((item) => String(item || "").trim().toLowerCase())
          .filter(Boolean)
      )
    ),
    minTrades: parseOptionalNumber(source.get("minTrades")),
    maxTrades: parseOptionalNumber(source.get("maxTrades")),
    minVolumeUsd: parseOptionalNumber(source.get("minVolumeUsd") ?? source.get("minVolume")),
    maxVolumeUsd: parseOptionalNumber(source.get("maxVolumeUsd") ?? source.get("maxVolume")),
    minPnlUsd: parseOptionalNumber(source.get("minPnlUsd") ?? source.get("minPnl")),
    maxPnlUsd: parseOptionalNumber(source.get("maxPnlUsd") ?? source.get("maxPnl")),
    minWinRate: parseOptionalNumber(source.get("minWinRate")),
    maxWinRate: parseOptionalNumber(source.get("maxWinRate")),
  };
}

function parseOptionalNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function buildAppliedFilters(parsed) {
  const filters = [];
  if (parsed.q) filters.push({ key: "q", label: "Search", value: parsed.q });
  if (parsed.symbol) filters.push({ key: "symbol", label: "Symbol", value: parsed.symbol });
  if (parsed.status.length) {
    parsed.status.forEach((value) => {
      filters.push({ key: "status", label: "Status", value });
    });
  }
  if (parsed.minTrades !== null) filters.push({ key: "minTrades", label: "Min Trades", value: String(parsed.minTrades) });
  if (parsed.maxTrades !== null) filters.push({ key: "maxTrades", label: "Max Trades", value: String(parsed.maxTrades) });
  if (parsed.minVolumeUsd !== null) filters.push({ key: "minVolumeUsd", label: "Min Volume", value: String(parsed.minVolumeUsd) });
  if (parsed.maxVolumeUsd !== null) filters.push({ key: "maxVolumeUsd", label: "Max Volume", value: String(parsed.maxVolumeUsd) });
  if (parsed.minPnlUsd !== null) filters.push({ key: "minPnlUsd", label: "Min PnL", value: String(parsed.minPnlUsd) });
  if (parsed.maxPnlUsd !== null) filters.push({ key: "maxPnlUsd", label: "Max PnL", value: String(parsed.maxPnlUsd) });
  if (parsed.minWinRate !== null) filters.push({ key: "minWinRate", label: "Min Win Rate", value: String(parsed.minWinRate) });
  if (parsed.maxWinRate !== null) filters.push({ key: "maxWinRate", label: "Max Win Rate", value: String(parsed.maxWinRate) });
  return filters;
}

function statusTokens(row) {
  const completeness = row && row.completeness && typeof row.completeness === "object" ? row.completeness : {};
  return new Set(
    [
      row && row.validationStatus,
      row && row.lifecycleStatus,
      row && row.lifecycleStage,
      completeness.trackingState,
      completeness.tradeHistoryState,
      completeness.metricsState,
      completeness.volumeExactness,
      completeness.leaderboardEligible ? "rankable" : "blocked",
    ]
      .map((value) => String(value || "").trim().toLowerCase())
      .filter(Boolean)
  );
}

function compareRows(left, right, sortKey, dir) {
  const direction = dir === "asc" ? 1 : -1;
  const rankingValue = (row) => {
    const value = row && row.rankingVolumeUsd;
    return value === null || value === undefined ? null : toNum(value, null);
  };
  const rawValue = (row) => toNum(row && row.volumeUsdRaw, 0);
  if (sortKey === "rankingVolumeUsd") {
    const leftTrusted = rankingValue(left);
    const rightTrusted = rankingValue(right);
    if (leftTrusted === null && rightTrusted !== null) return 1;
    if (leftTrusted !== null && rightTrusted === null) return -1;
    if (leftTrusted !== null && rightTrusted !== null && leftTrusted !== rightTrusted) {
      return direction * (leftTrusted - rightTrusted);
    }
    const rawDiff = rawValue(left) - rawValue(right);
    if (rawDiff !== 0) return direction * rawDiff;
  }
  const valueFor = (row) => {
    if (sortKey === "copyabilityScore") return toNum(row && row.copyabilityScore, 0);
    if (sortKey === "consistencyScore") return toNum(row && row.consistencyScore, 0);
    if (sortKey === "momentumScore") return toNum(row && row.momentumScore, 0);
    if (sortKey === "convictionScore") return toNum(row && row.convictionScore, 0);
    if (sortKey === "copyCandidateScore") return toNum(row && row.copyCandidateScore, 0);
    if (sortKey === "wallet") return String((row && row.wallet) || "");
    if (sortKey === "volumeUsdRaw") return rawValue(row);
    if (sortKey === "trades") return toNum(row && row.trades, 0);
    if (sortKey === "trades30") return toNum(row && row.trades30, toNum(row && row.d30 && row.d30.trades, 0));
    if (sortKey === "totalWins") return toNum(row && row.totalWins, 0);
    if (sortKey === "totalLosses") return toNum(row && row.totalLosses, 0);
    if (sortKey === "pnlUsd") return toNum(row && row.pnlUsd, 0);
    if (sortKey === "winRate") return toNum(row && row.winRate, 0);
    if (sortKey === "pnl7Usd") return toNum(row && row.pnl7Usd, 0);
    if (sortKey === "pnl30Usd") return toNum(row && row.pnl30Usd, 0);
    if (sortKey === "winRate30") return toNum(row && row.winRate30, 0);
    if (sortKey === "drawdownPct") return toNum(row && row.drawdownPct, 0);
    if (sortKey === "firstTrade") return toNum(row && row.firstTrade, 0);
    if (sortKey === "lastTrade") return toNum(row && row.lastTrade, 0);
    if (sortKey === "lastActivity") return toNum(row && row.lastActivity, 0);
    if (sortKey === "openPositions") return toNum(row && row.openPositions, 0);
    if (sortKey === "exposureUsd") return toNum(row && row.exposureUsd, 0);
    return rawValue(row);
  };
  const a = valueFor(left);
  const b = valueFor(right);
  if (typeof a === "string" || typeof b === "string") {
    const diff = String(a).localeCompare(String(b));
    if (diff !== 0) return direction * diff;
  } else if (a !== b) {
    return direction * (a - b);
  }
  return String((left && left.wallet) || "").localeCompare(String((right && right.wallet) || ""));
}

function buildSearchText(row) {
  const wallet = String((row && row.wallet) || "").toLowerCase();
  const symbols = Array.isArray(row && row.symbols) ? row.symbols.map((value) => String(value || "").toLowerCase()) : [];
  return `${wallet} ${symbols.join(" ")}`.trim();
}

function buildWalletRowForApi(row, rank, isolatedDir = null) {
  const completeness = row && row.completeness && typeof row.completeness === "object" ? row.completeness : {};
  const warnings = Array.isArray(completeness.warnings) ? completeness.warnings.slice() : [];
  const blockedReasons = Array.isArray(completeness.fullBlockedReasons)
    ? completeness.fullBlockedReasons.slice()
    : [];
  return {
    wallet: row.wallet,
    walletRecordId: row.walletRecordId || null,
    rank,
    trades: Number(row.trades || 0),
    volumeUsd: toNum(row.volumeUsdRaw, 0),
    volumeUsdRaw: toNum(row.volumeUsdRaw, 0),
    volumeUsdVerified:
      row.volumeUsdVerified === null || row.volumeUsdVerified === undefined
        ? null
        : toNum(row.volumeUsdVerified, 0),
    rankingVolumeUsd:
      row.rankingVolumeUsd === null || row.rankingVolumeUsd === undefined
        ? null
        : toNum(row.rankingVolumeUsd, 0),
    volumeStatus: completeness.volumeExactness || "partial",
    volumeStatusLabel: completeness.volumeExactnessLabel || "Partial History Volume",
    totalWins: Number(row.totalWins || 0),
    totalLosses: Number(row.totalLosses || 0),
    pnlUsd: toNum(row.pnlUsd, 0),
    winRate: toNum(row.winRate, 0),
    firstTrade: row.firstTrade || null,
    lastTrade: row.lastTrade || null,
    updatedAt: row.updatedAt || null,
    lastOpenedAt: getCanonicalLastOpenedAt(row, isolatedDir),
    lastActivity: getCanonicalLastActivityAt(row, isolatedDir),
    lastActiveAt: getCanonicalLastActivityAt(row, isolatedDir),
    openPositions: Number(row.openPositions || 0),
    exposureUsd: toNum(row.exposureUsd, 0),
    unrealizedPnlUsd: toNum(row.unrealizedPnlUsd, 0),
    totalPnlUsd: toNum(row.totalPnlUsd, 0),
    copyabilityScore:
      row.copyabilityScore === null || row.copyabilityScore === undefined ? null : toNum(row.copyabilityScore, 0),
    consistencyScore:
      row.consistencyScore === null || row.consistencyScore === undefined ? null : toNum(row.consistencyScore, 0),
    momentumScore:
      row.momentumScore === null || row.momentumScore === undefined ? null : toNum(row.momentumScore, 0),
    convictionScore:
      row.convictionScore === null || row.convictionScore === undefined ? null : toNum(row.convictionScore, 0),
    copyCandidateScore:
      row.copyCandidateScore === null || row.copyCandidateScore === undefined ? null : toNum(row.copyCandidateScore, 0),
    profitFactorScore:
      row.profitFactorScore === null || row.profitFactorScore === undefined ? null : toNum(row.profitFactorScore, 0),
    trades30: Number(row.trades30 !== undefined ? row.trades30 : toNum(row.d30 && row.d30.trades, 0)),
    pnl7Usd: toNum(row.pnl7Usd !== undefined ? row.pnl7Usd : row.d7 && row.d7.pnlUsd, 0),
    pnl30Usd: toNum(row.pnl30Usd !== undefined ? row.pnl30Usd : row.d30 && row.d30.pnlUsd, 0),
    winRate30: toNum(row.winRate30 !== undefined ? row.winRate30 : row.d30 && row.d30.winRatePct, 0),
    drawdownPct:
      row.drawdownPct === null || row.drawdownPct === undefined ? null : toNum(row.drawdownPct, 0),
    topSymbolSharePct:
      row.topSymbolSharePct === null || row.topSymbolSharePct === undefined ? null : toNum(row.topSymbolSharePct, 0),
    outlierRisk:
      row.outlierRisk === null || row.outlierRisk === undefined ? null : toNum(row.outlierRisk, 0),
    badges: Array.isArray(row.badges) ? row.badges.slice() : [],
    liveActiveRank: null,
    liveActiveScore: 0,
    recentEvents15m: 0,
    recentEvents1h: 0,
    freshness: {
      updatedAt: row.updatedAt || null,
      historyPersistedAt: completeness.historyPersistedAt || null,
    },
    lifecycleStage: row.lifecycleStage || null,
    lifecycleStatus: row.lifecycleStatus || null,
    backfillComplete: Boolean(row.backfillComplete),
    discoveredAt: row.discoveredAt || null,
    discoveredBy: row.discoveredBy || null,
    backfillCompletedAt: row.backfillCompletedAt || null,
    liveTrackingSince: row.liveTrackingSince || null,
    liveLastScanAt: row.liveLastScanAt || null,
    validationStatus: row.validationStatus || null,
    validationCheckedAt: row.validationCheckedAt || null,
    discoveryConfidence:
      row.discoveryConfidence === null || row.discoveryConfidence === undefined
        ? null
        : toNum(row.discoveryConfidence, 0),
    depositEvidenceCount: Number(row.depositEvidenceCount || 0),
    restShardId: row.restShardId || null,
    restShardIndex: row.restShardIndex === null || row.restShardIndex === undefined ? null : Number(row.restShardIndex),
    historyPhase: row.historyPhase || null,
    tradeRowsLoaded: Number(row.tradeRowsLoaded || 0),
    fundingRowsLoaded: Number(row.fundingRowsLoaded || 0),
    tradeDone: Boolean(row.tradeDone),
    fundingDone: Boolean(row.fundingDone),
    tradeHasMore: Boolean(row.tradeHasMore),
    fundingHasMore: Boolean(row.fundingHasMore),
    tradePagination: row.tradePagination || null,
    fundingPagination: row.fundingPagination || null,
    retryPending: Boolean(row.retryPending),
    retryReason: row.retryReason || null,
    retryQueuedAt: row.retryQueuedAt || null,
    forceHeadRefetch: Boolean(row.forceHeadRefetch),
    forceHeadRefetchReason: row.forceHeadRefetchReason || null,
    forceHeadRefetchQueuedAt: row.forceHeadRefetchQueuedAt || null,
    lastAttemptMode: row.lastAttemptMode || null,
    lastSuccessMode: row.lastSuccessMode || null,
    lastFailureMode: row.lastFailureMode || null,
    metricsReady: Boolean(row.metricsReady),
    analyticsReady: Boolean(row.analyticsReady),
    tracked: Boolean(row.tracked),
    closedPositionsObserved: 0,
    observedPositionEvents: 0,
    positionLifecycle: null,
    completeness: {
      historyVerified: Boolean(completeness.historyVerified),
      metricsVerified: Boolean(completeness.metricsVerified),
      leaderboardEligible: Boolean(completeness.leaderboardEligible),
      trackingState: completeness.trackingState || null,
      trackingLabel: completeness.trackingLabel || null,
      tradeHistoryState: completeness.tradeHistoryState || null,
      tradeHistoryLabel: completeness.tradeHistoryLabel || null,
      metricsState: completeness.metricsState || null,
      metricsLabel: completeness.metricsLabel || null,
      volumeExactness: completeness.volumeExactness || "partial",
      volumeExactnessLabel: completeness.volumeExactnessLabel || "Partial History Volume",
      historyPersistedAt: completeness.historyPersistedAt || null,
      tradesPersisted: completeness.tradesPersisted || null,
      fundingPersisted: completeness.fundingPersisted || null,
      pendingReasons: blockedReasons.slice(),
      blockerReasons: blockedReasons.slice(),
      fullBlockedReasons: blockedReasons.slice(),
      leaderboardBlockedReasons: blockedReasons.slice(),
      warnings,
      pendingWarnings: warnings.slice(),
    },
    trackingStatus: completeness.trackingState || null,
    trackingStatusLabel: completeness.trackingLabel || null,
    tradeHistoryStatus: completeness.tradeHistoryState || null,
    tradeHistoryStatusLabel: completeness.tradeHistoryLabel || null,
    metricsStatus: completeness.metricsState || null,
    metricsStatusLabel: completeness.metricsLabel || null,
    historyVerified: Boolean(completeness.historyVerified),
    metricsVerified: Boolean(completeness.metricsVerified),
    historyPersistedAt: completeness.historyPersistedAt || null,
    tradesPersisted: completeness.tradesPersisted || null,
    fundingPersisted: completeness.fundingPersisted || null,
    fullBlockedReasons: blockedReasons.slice(),
    warnings,
    leaderboardEligible: Boolean(completeness.leaderboardEligible),
    leaderboardBlockedReasons: blockedReasons.slice(),
    side: "none",
    symbolBreakdown: Array.isArray(row.symbolBreakdown) ? row.symbolBreakdown.slice() : [],
  };
}

function buildWalletProfile(dataset, wallet, isolatedDir = null) {
  const normalizedWallet = normalizeAddress(String(wallet || "").trim());
  if (!normalizedWallet) {
    return {
      generatedAt: Date.now(),
      wallet: null,
      found: false,
      error: "Wallet is required.",
    };
  }
  const row = dataset && dataset.rowsByWallet instanceof Map ? dataset.rowsByWallet.get(normalizedWallet) : null;
  if (!row) {
    return {
      generatedAt: Number((dataset && dataset.generatedAt) || Date.now()),
      wallet: normalizedWallet,
      found: false,
      error: "Wallet not found in the persisted wallet dataset.",
    };
  }
  const completeness = row.completeness || {};
  return {
    generatedAt: Number((dataset && dataset.generatedAt) || Date.now()),
    wallet: normalizedWallet,
    found: true,
    summary: {
      volumeUsd: toNum(row.volumeUsdRaw, 0),
      pnlUsd: toNum(row.pnlUsd, 0),
      trades: Number(row.trades || 0),
      netFeesUsd: null,
      firstTrade: row.firstTrade || null,
      lastTrade: row.lastTrade || null,
      lastOpenedAt: getCanonicalLastOpenedAt(row, isolatedDir),
      lastActivity: getCanonicalLastActivityAt(row, isolatedDir),
      winRate: toNum(row.winRate, 0),
      totalWins: Number(row.totalWins || 0),
      totalLosses: Number(row.totalLosses || 0),
      openPositions: Number(row.openPositions || 0),
    },
    status: {
      trackingStatus: completeness.trackingState || null,
      trackingStatusLabel: completeness.trackingLabel || null,
      tradeHistoryStatus: completeness.tradeHistoryState || null,
      tradeHistoryStatusLabel: completeness.tradeHistoryLabel || null,
      metricsStatus: completeness.metricsState || null,
      metricsStatusLabel: completeness.metricsLabel || null,
      historyVerified: Boolean(completeness.historyVerified),
      metricsVerified: Boolean(completeness.metricsVerified),
      leaderboardEligible: Boolean(completeness.leaderboardEligible),
      leaderboardBlockedReasons: Array.isArray(completeness.leaderboardBlockedReasons)
        ? completeness.leaderboardBlockedReasons.slice()
        : [],
      warnings: Array.isArray(completeness.warnings) ? completeness.warnings.slice() : [],
    },
    volumeTruth: {
      rawVolumeUsd: toNum(row.volumeUsdRaw, 0),
      verifiedVolumeUsd:
        row.volumeUsdVerified === null || row.volumeUsdVerified === undefined
          ? null
          : toNum(row.volumeUsdVerified, 0),
      rankingVolumeUsd:
        row.rankingVolumeUsd === null || row.rankingVolumeUsd === undefined
          ? null
          : toNum(row.rankingVolumeUsd, 0),
      volumeStatus: completeness.volumeExactness || "partial",
      volumeStatusLabel: completeness.volumeExactnessLabel || "Partial History Volume",
    },
    ranking: {
      rawVolumeRank:
        dataset && dataset.rawRanks instanceof Map ? dataset.rawRanks.get(normalizedWallet) || null : null,
      rankingVolumeRank:
        dataset && dataset.rankingRanks instanceof Map
          ? dataset.rankingRanks.get(normalizedWallet) || null
          : null,
      verifiedVolumeRank:
        dataset && dataset.verifiedRanks instanceof Map
          ? dataset.verifiedRanks.get(normalizedWallet) || null
          : null,
    },
    lifecycle: {
      stage: row.lifecycleStage || null,
      status: row.lifecycleStatus || null,
      discoveredAt: row.discoveredAt || null,
      backfillCompletedAt: row.backfillCompletedAt || null,
      liveTrackingSince: row.liveTrackingSince || null,
      liveLastScanAt: row.liveLastScanAt || null,
      historyPhase: row.historyPhase || null,
    },
    dataAudit: {
      remoteHistoryVerified: Boolean(row.remoteHistoryVerified),
      tradeRowsLoaded: Number(row.tradeRowsLoaded || 0),
      fundingRowsLoaded: Number(row.fundingRowsLoaded || 0),
      tradeHasMore: Boolean(row.tradeHasMore),
      fundingHasMore: Boolean(row.fundingHasMore),
      retryPending: Boolean(row.retryPending),
      retryReason: row.retryReason || null,
      tradePagination: row.tradePagination || null,
      fundingPagination: row.fundingPagination || null,
    },
    symbolBreakdown: Array.isArray(row.symbolBreakdown) ? row.symbolBreakdown.slice() : [],
  };
}

function createRankMap(rows) {
  const map = new Map();
  rows.forEach((row, index) => {
    map.set(String((row && row.wallet) || ""), index + 1);
  });
  return map;
}

function rowScore(row) {
  if (!row || typeof row !== "object") {
    return [0, 0, 0, 0, 0, 0, 0, 0];
  }
  const tradePagination =
    row.tradePagination && typeof row.tradePagination === "object" ? row.tradePagination : {};
  const fundingPagination =
    row.fundingPagination && typeof row.fundingPagination === "object" ? row.fundingPagination : {};
  return [
    row.backfillComplete ? 1 : 0,
    row.tradeDone ? 1 : 0,
    row.fundingDone ? 1 : 0,
    toNum(row.tradeRowsLoaded ?? row.trades, 0),
    toNum(row.fundingRowsLoaded, 0),
    toNum(tradePagination.fetchedPages, 0),
    toNum(row.volumeUsdRaw, 0),
    toNum(row.updatedAt, 0),
  ];
}

function compareScore(left, right) {
  const a = rowScore(left);
  const b = rowScore(right);
  for (let index = 0; index < Math.max(a.length, b.length); index += 1) {
    const diff = (a[index] || 0) - (b[index] || 0);
    if (diff !== 0) return diff;
  }
  return 0;
}

function buildDerivedCounts(rows) {
  const totalWallets = rows.length;
  const trackedFullWallets = rows.filter((row) => Boolean(row && row.backfillComplete)).length;
  const metricsCompleteWallets = rows.filter((row) => Boolean(row && row.metricsReady)).length;
  const leaderboardEligibleWallets = rows.filter(
    (row) => row && row.rankingVolumeUsd !== null && row.rankingVolumeUsd !== undefined
  ).length;
  const tradePagesFetched = rows.reduce(
    (sum, row) => sum + toNum(row && row.tradePagination && row.tradePagination.fetchedPages, 0),
    0
  );
  const fundingPagesFetched = rows.reduce(
    (sum, row) => sum + toNum(row && row.fundingPagination && row.fundingPagination.fetchedPages, 0),
    0
  );
  const tradePagesPending = rows.reduce(
    (sum, row) => sum + toNum(row && row.tradePagination && row.tradePagination.pendingPages, 0),
    0
  );
  const fundingPagesPending = rows.reduce(
    (sum, row) => sum + toNum(row && row.fundingPagination && row.fundingPagination.pendingPages, 0),
    0
  );
  const tradePagesFailed = rows.reduce(
    (sum, row) => sum + toNum(row && row.tradePagination && row.tradePagination.failedPages, 0),
    0
  );
  const fundingPagesFailed = rows.reduce(
    (sum, row) => sum + toNum(row && row.fundingPagination && row.fundingPagination.failedPages, 0),
    0
  );
  const rawVolumeUsd = rows.reduce((sum, row) => sum + toNum(row && row.volumeUsdRaw, 0), 0);
  const verifiedVolumeUsd = rows.reduce(
    (sum, row) =>
      sum +
      (row && row.volumeUsdVerified !== null && row.volumeUsdVerified !== undefined
        ? toNum(row.volumeUsdVerified, 0)
        : 0),
    0
  );
  return {
    totalWallets,
    discoveredWallets: totalWallets,
    trackedWallets: totalWallets,
    trackedLiteWallets: totalWallets - trackedFullWallets,
    trackedFullWallets,
    tradeHistoryCompleteWallets: trackedFullWallets,
    metricsCompleteWallets,
    metricsPartialWallets: Math.max(0, totalWallets - metricsCompleteWallets),
    leaderboardEligibleWallets,
    validatedWallets: trackedFullWallets,
    backfilledWallets: trackedFullWallets,
    tradePagesFetched,
    fundingPagesFetched,
    tradePagesPending,
    fundingPagesPending,
    tradePagesFailed,
    fundingPagesFailed,
    rawVolumeUsd: Number(rawVolumeUsd.toFixed(2)),
    verifiedVolumeUsd: Number(verifiedVolumeUsd.toFixed(2)),
    pendingWallets: Math.max(0, totalWallets - trackedFullWallets),
  };
}

function buildAvailableSymbols(rows, fallback = []) {
  const symbols = new Set(Array.isArray(fallback) ? fallback : []);
  rows.forEach((row) => {
    if (!row || !Array.isArray(row.symbols)) return;
    row.symbols.forEach((symbol) => {
      const normalized = String(symbol || "").trim().toUpperCase();
      if (normalized) symbols.add(normalized);
    });
  });
  return Array.from(symbols).sort((left, right) => left.localeCompare(right));
}

function createWalletSnapshotQuery({
  datasetPath,
  isolatedDir = null,
  heavyLaneStatePath = null,
  livePositionsDir = null,
}) {
  const allowIsolatedOverlay =
    String(process.env.PACIFICA_WALLET_EXPLORER_USE_ISOLATED_OVERLAY || "")
      .trim()
      .toLowerCase() === "true";
  if (!allowIsolatedOverlay) {
    isolatedDir = null;
  }
  const cache = {
    baseMtimeMs: 0,
    baseValue: null,
    overlayStamp: "",
    overlayValue: null,
    livePositionsStamp: "",
    livePositionsValue: null,
    effectiveStamp: "",
    effectiveValue: null,
  };

  function buildDatasetView(payload, rows, overlayMeta = null) {
    const nextgenOverlay = loadNextgenStateOverlay();
    const withSearch = rows.map((row) => {
      const merged = mergeNextgenOverlay(row, nextgenOverlay, isolatedDir);
      const enriched = enrichRow(merged, Date.now(), isolatedDir);
      return {
        ...enriched,
        _searchText: buildSearchText(enriched),
      };
    });
    const rawSorted = withSearch
      .slice()
      .sort((left, right) => compareRows(left, right, "volumeUsdRaw", "desc"));
    const rankingSorted = withSearch
      .slice()
      .sort((left, right) => compareRows(left, right, "rankingVolumeUsd", "desc"));
    const verifiedSorted = withSearch
      .filter((row) => row.volumeUsdVerified !== null && row.volumeUsdVerified !== undefined)
      .sort((left, right) =>
        compareRows(
          { ...left, volumeUsdRaw: left.volumeUsdVerified },
          { ...right, volumeUsdRaw: right.volumeUsdVerified },
          "volumeUsdRaw",
          "desc"
        )
      );
    const derivedCounts = buildDerivedCounts(withSearch);
    return {
      ...payload,
      rows: withSearch,
      rowsByWallet: new Map(withSearch.map((row) => [String(row.wallet || ""), row])),
      rawRanks: createRankMap(rawSorted),
      rankingRanks: createRankMap(
        rankingSorted.filter((row) => row.rankingVolumeUsd !== null && row.rankingVolumeUsd !== undefined)
      ),
      verifiedRanks: createRankMap(verifiedSorted),
      counts: {
        ...(payload && payload.counts && typeof payload.counts === "object" ? payload.counts : {}),
        ...derivedCounts,
      },
      availableSymbols: buildAvailableSymbols(
        withSearch,
        payload && Array.isArray(payload.availableSymbols) ? payload.availableSymbols : []
      ),
      liveOverlay: overlayMeta || {
        activeWalletCount: 0,
        overlaidWalletCount: 0,
        latestUpdatedAt: 0,
      },
    };
  }

  function loadBaseDataset(force = false) {
    let mtimeMs = 0;
    try {
      mtimeMs = Number(fs.statSync(datasetPath).mtimeMs || 0) || 0;
    } catch (_error) {
      mtimeMs = 0;
    }
    if (!force && cache.baseValue && cache.baseMtimeMs >= mtimeMs) {
      return cache.baseValue;
    }
    const payload = readJson(datasetPath, null);
    if (payload && Array.isArray(payload.rows)) {
      cache.baseValue = buildDatasetView(payload, payload.rows, null);
      cache.baseMtimeMs = mtimeMs;
      return cache.baseValue;
    }
    const resolvedPath = String(datasetPath || "");
    if (
      resolvedPath &&
      resolvedPath.toLowerCase().includes("wallet_explorer_v3") &&
      resolvedPath.toLowerCase().endsWith("manifest.json")
    ) {
      const v3Snapshot = loadWalletExplorerV3Snapshot();
      if (!v3Snapshot || !Array.isArray(v3Snapshot.rows)) return null;
      const v3Payload = {
        ...v3Snapshot,
        rows: v3Snapshot.rows,
        counts: v3Snapshot.counts || v3Snapshot.summaryCounts || null,
      };
      cache.baseValue = buildDatasetView(v3Payload, v3Payload.rows, null);
      cache.baseMtimeMs = mtimeMs;
      return cache.baseValue;
    }
    cache.baseValue = buildDatasetView(payload, payload.rows, null);
    cache.baseMtimeMs = mtimeMs;
    return cache.baseValue;
  }

  function loadLiveOverlay(force = false) {
    const isolatedRoot = isolatedDir ? path.resolve(isolatedDir) : null;
    const heavyStateFile = heavyLaneStatePath ? path.resolve(heavyLaneStatePath) : null;
    if (!isolatedRoot || !fs.existsSync(isolatedRoot)) {
      cache.overlayStamp = "";
      cache.overlayValue = {
        rowsByWallet: new Map(),
        activeWallets: new Set(),
        activeWalletCount: 0,
        overlaidWalletCount: 0,
        latestUpdatedAt: 0,
      };
      return cache.overlayValue;
    }

    const activeWallets = new Set();
    let heavyStateMtimeMs = 0;
    if (heavyStateFile && fs.existsSync(heavyStateFile)) {
      try {
        heavyStateMtimeMs = Number(fs.statSync(heavyStateFile).mtimeMs || 0) || 0;
        const heavyState = readJson(heavyStateFile, null);
        const lanes = Array.isArray(heavyState && heavyState.lanes) ? heavyState.lanes : [];
        lanes.forEach((lane) => {
          const wallet = normalizeAddress(String((lane && lane.wallet) || "").trim());
          if (wallet) activeWallets.add(wallet);
        });
      } catch (_error) {
        heavyStateMtimeMs = 0;
      }
    }

    const records = [];
    const stampParts = [`heavy:${heavyStateMtimeMs}`];
    try {
      const directories = fs
        .readdirSync(isolatedRoot, { withFileTypes: true })
        .filter((entry) => entry.isDirectory())
        .map((entry) => entry.name)
        .sort((left, right) => left.localeCompare(right));
      directories.forEach((wallet) => {
        const recordPath = path.join(isolatedRoot, wallet, "wallet_record.json");
        if (!fs.existsSync(recordPath)) return;
        const stat = fs.statSync(recordPath);
        const payload = readJson(recordPath, null);
        const normalizedWallet = normalizeAddress(String((payload && payload.wallet) || wallet).trim());
        if (!normalizedWallet || !payload || typeof payload !== "object") return;
        records.push({
          wallet: normalizedWallet,
          mtimeMs: Number(stat.mtimeMs || 0) || 0,
          payload,
        });
        stampParts.push(`${normalizedWallet}:${Number(stat.mtimeMs || 0) || 0}`);
      });
    } catch (_error) {
      // ignore and fall back to no overlay
    }

    const stamp = stampParts.join("|");
    if (!force && cache.overlayValue && cache.overlayStamp === stamp) {
      return cache.overlayValue;
    }

    const rowsByWallet = new Map();
    let latestUpdatedAt = 0;
    records.forEach(({ wallet, payload }) => {
      rowsByWallet.set(wallet, payload);
      latestUpdatedAt = Math.max(
        latestUpdatedAt,
        toNum(payload && payload.updatedAt, 0),
        toNum(payload && payload.lastTrade, 0)
      );
    });
    const overlayValue = {
      rowsByWallet,
      activeWallets,
      activeWalletCount: activeWallets.size,
      overlaidWalletCount: rowsByWallet.size,
      latestUpdatedAt,
    };
    cache.overlayStamp = stamp;
    cache.overlayValue = overlayValue;
    return overlayValue;
  }

  function loadLivePositionsOverlay(force = false) {
    const positionsRoot = livePositionsDir ? path.resolve(livePositionsDir) : null;
    if (!positionsRoot || !fs.existsSync(positionsRoot)) {
      cache.livePositionsStamp = "";
      cache.livePositionsValue = {
        byWallet: new Map(),
        activeWalletCount: 0,
        openPositionsTotal: 0,
        latestUpdatedAt: 0,
      };
      return cache.livePositionsValue;
    }

    const files = listShardSnapshotFiles(positionsRoot);
    if (!files.length) {
      cache.livePositionsStamp = "";
      cache.livePositionsValue = {
        byWallet: new Map(),
        activeWalletCount: 0,
        openPositionsTotal: 0,
        latestUpdatedAt: 0,
      };
      return cache.livePositionsValue;
    }

    const stamp = files
      .map((filePath) => {
        let mtimeMs = 0;
        try {
          mtimeMs = Number(fs.statSync(filePath).mtimeMs || 0) || 0;
        } catch (_error) {
          mtimeMs = 0;
        }
        return `${filePath}:${mtimeMs}`;
      })
      .join("|");
    if (!force && cache.livePositionsValue && cache.livePositionsStamp === stamp) {
      return cache.livePositionsValue;
    }

    const snapshot = loadMergedShardSnapshot(positionsRoot, { eventsLimit: 1 });
    const byWallet = new Map();
    let latestUpdatedAt = Number((snapshot && snapshot.generatedAt) || 0) || 0;
    let openPositionsTotal = 0;
    const rows = Array.isArray(snapshot && snapshot.positions) ? snapshot.positions : [];
    rows.forEach((row) => {
      const wallet = normalizeAddress(String((row && row.wallet) || "").trim());
      if (!wallet) return;
      const aggregate = byWallet.get(wallet) || {
        openPositions: 0,
        exposureUsd: 0,
        unrealizedPnlUsd: 0,
        totalPnlUsd: 0,
        lastOpenedAt: 0,
        lastActivityAt: 0,
        liveLastScanAt: 0,
      };
      aggregate.openPositions += 1;
      aggregate.exposureUsd += toNum(row && row.positionUsd, 0);
      aggregate.unrealizedPnlUsd += toNum(row && row.pnl, 0);
      aggregate.lastOpenedAt = Math.max(
        aggregate.lastOpenedAt,
        toNum(row && row.openedAt, 0)
      );
      aggregate.lastActivityAt = Math.max(
        aggregate.lastActivityAt,
        toNum(row && row.openedAt, 0)
      );
      aggregate.liveLastScanAt = Math.max(
        aggregate.liveLastScanAt,
        toNum(row && row.updatedAt, 0),
        toNum(row && row.timestamp, 0),
        toNum(row && row.openedAt, 0)
      );
      latestUpdatedAt = Math.max(latestUpdatedAt, aggregate.liveLastScanAt);
      aggregate.totalPnlUsd = aggregate.unrealizedPnlUsd;
      byWallet.set(wallet, aggregate);
      openPositionsTotal += 1;
    });

    const livePositionsValue = {
      byWallet,
      activeWalletCount: byWallet.size,
      openPositionsTotal:
        Number((snapshot && snapshot.status && snapshot.status.openPositionsTotal) || 0) ||
        openPositionsTotal,
      latestUpdatedAt,
    };
    cache.livePositionsStamp = stamp;
    cache.livePositionsValue = livePositionsValue;
    return livePositionsValue;
  }

  function choosePreferredRow(baseRow, overlayRow) {
    if (!overlayRow || typeof overlayRow !== "object") return baseRow || null;
    if (!baseRow || compareScore(overlayRow, baseRow) >= 0) {
      return {
        ...overlayRow,
        _searchText: buildSearchText(overlayRow),
      };
    }
    return baseRow;
  }

  function applyOverlayToRows(baseRows, overlay) {
    if (!overlay || !(overlay.rowsByWallet instanceof Map) || overlay.rowsByWallet.size <= 0) {
      return baseRows.slice();
    }
    const seen = new Set();
    const rows = baseRows.map((row) => {
      const wallet = String((row && row.wallet) || "");
      seen.add(wallet);
      return choosePreferredRow(row, overlay.rowsByWallet.get(wallet));
    });
    overlay.rowsByWallet.forEach((overlayRow, wallet) => {
      if (seen.has(wallet)) return;
      rows.push({
        ...overlayRow,
        _searchText: buildSearchText(overlayRow),
      });
    });
    return rows;
  }

function applyLivePositionsToRow(row, livePositions, isolatedDir = null) {
  if (!row || typeof row !== "object") return row;
  if (!livePositions || !(livePositions.byWallet instanceof Map)) return row;
  const wallet = String((row && row.wallet) || "");
  const aggregate = livePositions.byWallet.get(wallet);
    if (!aggregate) {
    return {
      ...row,
      openPositions: 0,
      exposureUsd: 0,
      unrealizedPnlUsd: 0,
      totalPnlUsd: toNum(row && row.pnlUsd, 0),
      lastOpenedAt: getCanonicalLastOpenedAt(row, isolatedDir) || row.lastOpenedAt || null,
      lastActivity:
        getCanonicalLastOpenedAt(row, isolatedDir) ||
        row.lastActivity ||
        row.lastActivityAt ||
        null,
      lastActiveAt:
        getCanonicalLastOpenedAt(row, isolatedDir) ||
        row.lastActivity ||
        row.lastActivityAt ||
        null,
    };
  }
  const unrealizedPnlUsd = Number(toNum(aggregate.unrealizedPnlUsd, 0).toFixed(2));
  const lastOpenedAt = Math.max(
    getCanonicalLastOpenedAt(row, isolatedDir) || 0,
    toNum(aggregate.lastOpenedAt, 0)
  ) || null;
  const lastActivity = lastOpenedAt;
  return {
    ...row,
    openPositions: Number(aggregate.openPositions || 0),
    exposureUsd: Number(toNum(aggregate.exposureUsd, 0).toFixed(2)),
    unrealizedPnlUsd,
    totalPnlUsd: Number((toNum(row && row.pnlUsd, 0) + unrealizedPnlUsd).toFixed(2)),
    lastOpenedAt,
    lastActivity,
    lastActiveAt: lastActivity,
    liveLastScanAt: Math.max(
      toNum(row && row.liveLastScanAt, 0),
      toNum(aggregate.liveLastScanAt, 0),
        toNum(livePositions && livePositions.latestUpdatedAt, 0)
      ) || row.liveLastScanAt || null,
    };
  }

  function applyLivePositionsToRows(rows, livePositions) {
    if (!livePositions || !(livePositions.byWallet instanceof Map)) {
      return rows.slice();
    }
    return rows.map((row) => applyLivePositionsToRow(row, livePositions, isolatedDir));
  }

  function adjustCountsWithOverlay(baseDataset, overlay, livePositions) {
    const counts = cloneJson((baseDataset && baseDataset.counts) || {});
    if (!overlay || !(overlay.rowsByWallet instanceof Map) || overlay.rowsByWallet.size <= 0) {
      return {
        counts,
        liveOverlay: {
          activeWalletCount: 0,
          overlaidWalletCount: 0,
          latestUpdatedAt: Math.max(
            0,
            Number((livePositions && livePositions.latestUpdatedAt) || 0) || 0
          ),
          livePositionWalletCount: Number((livePositions && livePositions.activeWalletCount) || 0),
          openPositionsTotal: Number((livePositions && livePositions.openPositionsTotal) || 0),
        },
        generatedAt: Number((baseDataset && baseDataset.generatedAt) || 0) || Date.now(),
      };
    }

    let overlaidWalletCount = 0;
    overlay.rowsByWallet.forEach((overlayRow, wallet) => {
      const baseRow =
        baseDataset && baseDataset.rowsByWallet instanceof Map
          ? baseDataset.rowsByWallet.get(wallet)
          : null;
      const chosen = choosePreferredRow(baseRow, overlayRow);
      if (!chosen || chosen === baseRow) return;
      overlaidWalletCount += 1;

      const tradePagesDelta =
        toNum(chosen && chosen.tradePagination && chosen.tradePagination.fetchedPages, 0) -
        toNum(baseRow && baseRow.tradePagination && baseRow.tradePagination.fetchedPages, 0);
      const fundingPagesDelta =
        toNum(chosen && chosen.fundingPagination && chosen.fundingPagination.fetchedPages, 0) -
        toNum(baseRow && baseRow.fundingPagination && baseRow.fundingPagination.fetchedPages, 0);
      const tradePendingDelta =
        toNum(chosen && chosen.tradePagination && chosen.tradePagination.pendingPages, 0) -
        toNum(baseRow && baseRow.tradePagination && baseRow.tradePagination.pendingPages, 0);
      const fundingPendingDelta =
        toNum(chosen && chosen.fundingPagination && chosen.fundingPagination.pendingPages, 0) -
        toNum(baseRow && baseRow.fundingPagination && baseRow.fundingPagination.pendingPages, 0);
      const tradeFailedDelta =
        toNum(chosen && chosen.tradePagination && chosen.tradePagination.failedPages, 0) -
        toNum(baseRow && baseRow.tradePagination && baseRow.tradePagination.failedPages, 0);
      const fundingFailedDelta =
        toNum(chosen && chosen.fundingPagination && chosen.fundingPagination.failedPages, 0) -
        toNum(baseRow && baseRow.fundingPagination && baseRow.fundingPagination.failedPages, 0);

      counts.tradePagesFetched = toNum(counts.tradePagesFetched, 0) + tradePagesDelta;
      counts.fundingPagesFetched = toNum(counts.fundingPagesFetched, 0) + fundingPagesDelta;
      counts.tradePagesPending = toNum(counts.tradePagesPending, 0) + tradePendingDelta;
      counts.fundingPagesPending = toNum(counts.fundingPagesPending, 0) + fundingPendingDelta;
      counts.tradePagesFailed = toNum(counts.tradePagesFailed, 0) + tradeFailedDelta;
      counts.fundingPagesFailed = toNum(counts.fundingPagesFailed, 0) + fundingFailedDelta;
      counts.rawVolumeUsd = Number(
        (
          toNum(counts.rawVolumeUsd, 0) +
          toNum(chosen && chosen.volumeUsdRaw, 0) -
          toNum(baseRow && baseRow.volumeUsdRaw, 0)
        ).toFixed(2)
      );
      counts.verifiedVolumeUsd = Number(
        (
          toNum(counts.verifiedVolumeUsd, 0) +
          (chosen && chosen.volumeUsdVerified !== null && chosen.volumeUsdVerified !== undefined
            ? toNum(chosen.volumeUsdVerified, 0)
            : 0) -
          (baseRow && baseRow.volumeUsdVerified !== null && baseRow.volumeUsdVerified !== undefined
            ? toNum(baseRow.volumeUsdVerified, 0)
            : 0)
        ).toFixed(2)
      );

      const boolDelta = (nextValue, prevValue) => (nextValue ? 1 : 0) - (prevValue ? 1 : 0);
      counts.trackedFullWallets =
        toNum(counts.trackedFullWallets, 0) + boolDelta(chosen.backfillComplete, baseRow && baseRow.backfillComplete);
      counts.tradeHistoryCompleteWallets =
        toNum(counts.tradeHistoryCompleteWallets, 0) + boolDelta(chosen.backfillComplete, baseRow && baseRow.backfillComplete);
      counts.metricsCompleteWallets =
        toNum(counts.metricsCompleteWallets, 0) + boolDelta(chosen.metricsReady, baseRow && baseRow.metricsReady);
      counts.leaderboardEligibleWallets =
        toNum(counts.leaderboardEligibleWallets, 0) +
        boolDelta(
          chosen.rankingVolumeUsd !== null && chosen.rankingVolumeUsd !== undefined,
          baseRow && baseRow.rankingVolumeUsd !== null && baseRow.rankingVolumeUsd !== undefined
        );
      counts.validatedWallets =
        toNum(counts.validatedWallets, 0) + boolDelta(chosen.backfillComplete, baseRow && baseRow.backfillComplete);
      counts.backfilledWallets =
        toNum(counts.backfilledWallets, 0) + boolDelta(chosen.backfillComplete, baseRow && baseRow.backfillComplete);
    });

    counts.totalWallets = toNum(counts.totalWallets, 0);
    counts.discoveredWallets = toNum(counts.discoveredWallets, counts.totalWallets);
    counts.trackedWallets = toNum(counts.trackedWallets, counts.totalWallets);
    counts.trackedLiteWallets = Math.max(0, toNum(counts.totalWallets, 0) - toNum(counts.trackedFullWallets, 0));
    counts.metricsPartialWallets = Math.max(0, toNum(counts.totalWallets, 0) - toNum(counts.metricsCompleteWallets, 0));
    counts.pendingWallets = Math.max(0, toNum(counts.totalWallets, 0) - toNum(counts.trackedFullWallets, 0));

    return {
      counts,
      liveOverlay: {
        activeWalletCount: overlay.activeWalletCount || 0,
        overlaidWalletCount,
        latestUpdatedAt: Math.max(
          Number(overlay.latestUpdatedAt || 0) || 0,
          Number((livePositions && livePositions.latestUpdatedAt) || 0) || 0
        ),
        livePositionWalletCount: Number((livePositions && livePositions.activeWalletCount) || 0),
        openPositionsTotal: Number((livePositions && livePositions.openPositionsTotal) || 0),
      },
      generatedAt: Math.max(
        Number((baseDataset && baseDataset.generatedAt) || 0) || 0,
        Number(overlay.latestUpdatedAt || 0) || 0,
        Number((livePositions && livePositions.latestUpdatedAt) || 0) || 0
      ) || Date.now(),
    };
  }

  function loadDataset(force = false) {
    return loadBaseDataset(force);
  }

  function getEffectiveDataset(force = false) {
    const baseDataset = loadBaseDataset(force);
    if (!baseDataset) return null;
    const overlay = loadLiveOverlay(force);
    const livePositions = loadLivePositionsOverlay(force);
    const stamp = [
      Number(cache.baseMtimeMs || 0) || 0,
      cache.overlayStamp || "",
      cache.livePositionsStamp || "",
    ].join("|");
    if (!force && cache.effectiveValue && cache.effectiveStamp === stamp) {
      return cache.effectiveValue;
    }
    const adjusted = adjustCountsWithOverlay(baseDataset, overlay, livePositions);
    const rows = applyLivePositionsToRows(
      applyOverlayToRows(baseDataset.rows, overlay),
      livePositions
    );
    const effectiveValue = buildDatasetView(
      {
        ...baseDataset,
        generatedAt: Number(adjusted.generatedAt || baseDataset.generatedAt || Date.now()),
        counts: adjusted.counts || baseDataset.counts || {},
      },
      rows,
      adjusted.liveOverlay || null
    );
    cache.effectiveStamp = stamp;
    cache.effectiveValue = effectiveValue;
    return effectiveValue;
  }

  function queryWallets(rawQuery = {}, status = null) {
    const dataset = loadBaseDataset(false);
    if (!dataset) return null;
    const overlay = loadLiveOverlay(false);
    const livePositions = loadLivePositionsOverlay(false);
    const adjusted = adjustCountsWithOverlay(dataset, overlay, livePositions);
    const parsed = normalizeQuery(rawQuery);
    let filtered = applyLivePositionsToRows(applyOverlayToRows(dataset.rows, overlay), livePositions);

    if (parsed.q) {
      filtered = filtered.filter((row) => row._searchText.includes(parsed.q));
    }
    if (parsed.symbol) {
      filtered = filtered.filter(
        (row) => Array.isArray(row.symbols) && row.symbols.includes(parsed.symbol)
      );
    }
    if (parsed.status.length) {
      filtered = filtered.filter((row) => {
        const tokens = statusTokens(row);
        return parsed.status.some((value) => tokens.has(value));
      });
    }

    const within = (value, minValue, maxValue) => {
      if (minValue !== null && value < minValue) return false;
      if (maxValue !== null && value > maxValue) return false;
      return true;
    };
    filtered = filtered.filter((row) =>
      within(toNum(row.trades, 0), parsed.minTrades, parsed.maxTrades) &&
      within(toNum(row.volumeUsdRaw, 0), parsed.minVolumeUsd, parsed.maxVolumeUsd) &&
      within(toNum(row.pnlUsd, 0), parsed.minPnlUsd, parsed.maxPnlUsd) &&
      within(toNum(row.winRate, 0), parsed.minWinRate, parsed.maxWinRate)
    );

    filtered.sort((left, right) => compareRows(left, right, parsed.sort, parsed.dir));
    const total = filtered.length;
    const pages = Math.max(1, Math.ceil(total / parsed.pageSize));
    const page = Math.min(parsed.page, pages);
    const start = (page - 1) * parsed.pageSize;
    const rows = filtered
      .slice(start, start + parsed.pageSize)
      .map((row, index) => buildWalletRowForApi(row, start + index + 1, isolatedDir));
    const statusSummary =
      status && status.status && status.status.summary && typeof status.status.summary === "object"
        ? status.status.summary
        : null;
    return {
      generatedAt: Number(adjusted.generatedAt || dataset.generatedAt || Date.now()),
      timeframe: "all",
      query: {
        q: parsed.q,
        page,
        pageSize: parsed.pageSize,
        sort: parsed.sort,
        dir: parsed.dir,
      },
      sorting: sortMeta(parsed.sort, parsed.dir),
      total,
      page,
      pageSize: parsed.pageSize,
      pages,
      rows,
      filters: {
        applied: buildAppliedFilters(parsed),
        availableSymbols: Array.isArray(dataset.availableSymbols) ? dataset.availableSymbols : [],
      },
      counts: {
        ...(adjusted.counts || dataset.counts || {}),
        visibleRows: rows.length,
        filteredRows: total,
      },
      pagination: {
        total,
        page,
        pageSize: parsed.pageSize,
        pages,
      },
      performance: {
        datasetBuiltAt: Number(dataset.generatedAt || Date.now()),
        liveOverlayUpdatedAt: Math.max(
          Number((overlay && overlay.latestUpdatedAt) || 0) || 0,
          Number((livePositions && livePositions.latestUpdatedAt) || 0) || 0
        ) || null,
      },
      topCorrection: dataset.topCorrection || null,
      progress:
        statusSummary && statusSummary.walletExplorerProgress
          ? statusSummary.walletExplorerProgress
          : dataset.counts || {},
      system:
        statusSummary && (statusSummary.systemStatus || statusSummary.system)
          ? {
              mode: statusSummary.systemMode || null,
              ...(statusSummary.systemStatus || statusSummary.system),
            }
          : null,
      freshness: statusSummary ? statusSummary.dataFreshness || null : null,
      warmup: {
        walletExplorerReady: true,
        persistedPreview: true,
        liveEnrichmentPending: false,
        retryAfterMs: 0,
      },
    };
  }

  return {
    loadDataset,
    getEffectiveDataset,
    hasLiveOverlay() {
      const overlay = loadLiveOverlay(false);
      return Boolean(
        overlay && (overlay.activeWalletCount > 0 || overlay.overlaidWalletCount > 0)
      );
    },
    hasLivePositionsOverlay() {
      const livePositions = loadLivePositionsOverlay(false);
      return Boolean(
        livePositions &&
          (Number(livePositions.activeWalletCount || 0) > 0 ||
            Number(livePositions.openPositionsTotal || 0) > 0)
      );
    },
    getDatasetSummary() {
      const dataset = loadBaseDataset(false);
      if (!dataset) return null;
      const overlay = loadLiveOverlay(false);
      const livePositions = loadLivePositionsOverlay(false);
      return adjustCountsWithOverlay(dataset, overlay, livePositions);
    },
    queryWallets,
    getWalletProfile(wallet) {
      const dataset = loadBaseDataset(false);
      if (!dataset) return null;
      const normalizedWallet = normalizeAddress(String(wallet || "").trim());
      const overlay = loadLiveOverlay(false);
      const livePositions = loadLivePositionsOverlay(false);
      const baseRow =
        normalizedWallet && dataset.rowsByWallet instanceof Map
          ? dataset.rowsByWallet.get(normalizedWallet)
          : null;
      const effectiveRow =
        normalizedWallet && overlay && overlay.rowsByWallet instanceof Map
          ? choosePreferredRow(baseRow, overlay.rowsByWallet.get(normalizedWallet))
          : baseRow;
      if (!effectiveRow) {
        return buildWalletProfile(dataset, wallet, isolatedDir);
      }
      const liveRow = applyLivePositionsToRow(effectiveRow, livePositions, isolatedDir);
      if (effectiveRow === baseRow) {
        if (liveRow === effectiveRow) {
          return buildWalletProfile(dataset, wallet, isolatedDir);
        }
        const profileDataset = {
          ...dataset,
          rowsByWallet: new Map(dataset.rowsByWallet),
        };
        profileDataset.rowsByWallet.set(normalizedWallet, liveRow);
        return buildWalletProfile(profileDataset, wallet, isolatedDir);
      }
      const profileDataset = {
        ...dataset,
        rowsByWallet: new Map(dataset.rowsByWallet),
      };
      profileDataset.rowsByWallet.set(normalizedWallet, liveRow);
      return buildWalletProfile(profileDataset, wallet, isolatedDir);
    },
  };
}

module.exports = {
  createWalletSnapshotQuery,
};
