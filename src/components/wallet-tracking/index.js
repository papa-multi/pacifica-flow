const {
  buildExchangeOverviewPayload,
  buildWalletProfilePayload,
} = require("../../services/pipeline/api");
const { buildWalletRecordFromState } = require("../../services/analytics/wallet_stats");
const { LiveTradeAttributionStore } = require("../../services/analytics/live_trade_attribution_store");
const { HotWalletWsMonitor } = require("../../services/analytics/hot_wallet_ws_monitor");
const { loadMergedShardSnapshot } = require("../../services/analytics/live_positions_snapshot_store");
const { LiveWalletTriggerStore } = require("../../services/analytics/live_wallet_trigger_store");
const { WalletFirstLivePositionsMonitor } = require("../../services/analytics/wallet_first_live_positions");
const { readJson } = require("../../services/pipeline/utils");
const fs = require("fs");
const http = require("http");
const https = require("https");
const path = require("path");

function toNum(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function toFixed(value, digits = 2) {
  const num = toNum(value, NaN);
  if (!Number.isFinite(num)) return "0";
  return num.toFixed(digits);
}

function toCompact(value) {
  const num = toNum(value, 0);
  const abs = Math.abs(num);
  if (abs >= 1e12) return `${toFixed(num / 1e12, 2)}T`;
  if (abs >= 1e9) return `${toFixed(num / 1e9, 2)}B`;
  if (abs >= 1e6) return `${toFixed(num / 1e6, 2)}M`;
  if (abs >= 1e3) return `${toFixed(num / 1e3, 2)}K`;
  return toFixed(num, 2);
}

function buildMarketPriceLookup(pricesBySymbol = {}) {
  const lookup = new Map();
  const source =
    pricesBySymbol && typeof pricesBySymbol === "object" ? pricesBySymbol : {};
  Object.entries(source).forEach(([symbol, value]) => {
    const normalized = String(symbol || "").trim().toUpperCase();
    if (!normalized) return;
    const row = value && typeof value === "object" ? value : {};
    const mark = toNum(
      row.mark !== undefined
        ? row.mark
        : row.markPrice !== undefined
        ? row.markPrice
        : row.mid !== undefined
        ? row.mid
        : row.price,
      NaN
    );
    if (!Number.isFinite(mark) || mark <= 0) return;
    lookup.set(normalized, mark);
  });
  return lookup;
}

function enrichPositionRowWithMarketPrice(row = {}, priceLookup = null) {
  const safeRow = row && typeof row === "object" ? row : {};
  const lookup = priceLookup instanceof Map ? priceLookup : null;
  const readMaybeNumber = (value) => {
    if (value === null || value === undefined || value === "") return NaN;
    const num = Number(value);
    return Number.isFinite(num) ? num : NaN;
  };
  const symbol = String(safeRow.symbol || "").trim().toUpperCase();
  const entry = readMaybeNumber(
    safeRow.entry !== undefined
      ? safeRow.entry
      : safeRow.entryPrice !== undefined
      ? safeRow.entryPrice
      : safeRow.entry_price !== undefined
      ? safeRow.entry_price
      : safeRow.raw && safeRow.raw.entry_price !== undefined
      ? safeRow.raw.entry_price
      : NaN
  );
  const existingMark = readMaybeNumber(
    safeRow.mark !== undefined
      ? safeRow.mark
      : safeRow.markPrice !== undefined
      ? safeRow.markPrice
      : safeRow.mark_price !== undefined
      ? safeRow.mark_price
      : safeRow.currentPrice !== undefined
      ? safeRow.currentPrice
      : safeRow.raw && safeRow.raw.mark_price !== undefined
      ? safeRow.raw.mark_price
      : NaN
  );
  const marketMark = symbol && lookup ? toNum(lookup.get(symbol), NaN) : NaN;
  const rawPnlInput =
    safeRow.unrealizedPnlUsd !== undefined && safeRow.unrealizedPnlUsd !== null
      ? safeRow.unrealizedPnlUsd
      : safeRow.unrealized_pnl !== undefined && safeRow.unrealized_pnl !== null
      ? safeRow.unrealized_pnl
      : safeRow.unrealizedPnl !== undefined && safeRow.unrealizedPnl !== null
      ? safeRow.unrealizedPnl
      : safeRow.pnl !== undefined && safeRow.pnl !== null
      ? safeRow.pnl
      : undefined;
  const hasExplicitPnl = Number.isFinite(readMaybeNumber(rawPnlInput));
  const mark =
    Number.isFinite(marketMark) &&
    (
      !Number.isFinite(existingMark) ||
      !hasExplicitPnl ||
      (Number.isFinite(entry) && Math.abs(existingMark - entry) <= 1e-12)
    )
      ? marketMark
      : existingMark;
  const size = Math.abs(
    readMaybeNumber(
      safeRow.size !== undefined
        ? safeRow.size
        : safeRow.amount !== undefined
        ? safeRow.amount
        : safeRow.qty !== undefined
        ? safeRow.qty
        : safeRow.raw && safeRow.raw.amount !== undefined
        ? safeRow.raw.amount
        : NaN
    )
  );
  let pnl = readMaybeNumber(rawPnlInput);
  if (!Number.isFinite(pnl) && Number.isFinite(entry) && Number.isFinite(mark) && Number.isFinite(size)) {
    const sideRaw = String(safeRow.side || safeRow.rawSide || "").trim().toLowerCase();
    const direction =
      sideRaw.includes("short") || sideRaw === "ask" || sideRaw === "short" ? -1 : 1;
    pnl = (mark - entry) * size * direction;
  }
  const positionUsd =
    Number.isFinite(mark) && Number.isFinite(size)
      ? Math.abs(mark * size)
      : toNum(
          safeRow.positionUsd !== undefined
            ? safeRow.positionUsd
            : safeRow.notionalUsd !== undefined
            ? safeRow.notionalUsd
            : NaN,
          NaN
        );
  return {
    ...safeRow,
    mark: Number.isFinite(mark) ? Number(mark.toFixed(8)) : safeRow.mark,
    pnl: Number.isFinite(pnl) ? Number(pnl.toFixed(2)) : safeRow.pnl,
    unrealizedPnlUsd: Number.isFinite(pnl)
      ? Number(pnl.toFixed(2))
      : safeRow.unrealizedPnlUsd,
    positionUsd: Number.isFinite(positionUsd)
      ? Number(positionUsd.toFixed(2))
      : safeRow.positionUsd,
  };
}

function computeDefiLlamaV2FromPrices(rows = []) {
  return (Array.isArray(rows) ? rows : []).reduce(
    (acc, item) => {
      const volume24h = toNum(item && item.volume_24h !== undefined ? item.volume_24h : 0);
      const openInterest = toNum(
        item && item.open_interest !== undefined ? item.open_interest : 0
      );
      const mark = toNum(item && item.mark !== undefined ? item.mark : 0);

      acc.dailyVolume += volume24h;
      acc.openInterestAtEnd += openInterest * mark;
      return acc;
    },
    {
      dailyVolume: 0,
      openInterestAtEnd: 0,
    }
  );
}

function buildDefiLlamaVolumeRank(rows = [], limit = 100) {
  return (Array.isArray(rows) ? rows : [])
    .map((item) => {
      const symbolRaw = item && item.symbol !== undefined ? item.symbol : "";
      const symbol = String(symbolRaw || "").trim();
      const volume24h = toNum(item && item.volume_24h !== undefined ? item.volume_24h : 0);
      const openInterest = toNum(
        item && item.open_interest !== undefined ? item.open_interest : 0
      );
      const mark = toNum(item && item.mark !== undefined ? item.mark : 0);
      const openInterestUsd = openInterest * mark;

      return {
        symbol,
        volume_24h_usd: Number(volume24h.toFixed(8)),
        open_interest_usd: Number(openInterestUsd.toFixed(8)),
      };
    })
    .filter((row) => Boolean(row.symbol))
    .sort((a, b) => toNum(b.volume_24h_usd, 0) - toNum(a.volume_24h_usd, 0))
    .slice(0, Math.max(1, Number(limit || 100)))
    .map((row, idx) => ({
      rank: idx + 1,
      symbol: row.symbol,
      market: row.symbol,
      volume_24h_usd: row.volume_24h_usd,
      open_interest_usd: row.open_interest_usd,
      volumeUsd: toFixed(row.volume_24h_usd, 2),
      volumeCompact: (() => {
        const value = toNum(row.volume_24h_usd, 0);
        const abs = Math.abs(value);
        if (abs >= 1e12) return `${toFixed(value / 1e12, 2)}T`;
        if (abs >= 1e9) return `${toFixed(value / 1e9, 2)}B`;
        if (abs >= 1e6) return `${toFixed(value / 1e6, 2)}M`;
        if (abs >= 1e3) return `${toFixed(value / 1e3, 2)}K`;
        return toFixed(value, 2);
      })(),
      live: true,
    }));
}

function formatUtcDateFromMs(ms) {
  const n = Number(ms);
  if (!Number.isFinite(n)) return null;
  return new Date(n).toISOString().slice(0, 10);
}

function addUtcDays(dateStr, deltaDays) {
  const base = Date.parse(`${String(dateStr).slice(0, 10)}T00:00:00.000Z`);
  if (!Number.isFinite(base)) return null;
  return formatUtcDateFromMs(base + Number(deltaDays || 0) * 24 * 60 * 60 * 1000);
}

function buildHistoricalSymbolVolumeRank({
  dailyByDate = null,
  startDate = null,
  endDate = null,
  limit = 100,
  requireReliableDay = true,
}) {
  if (!dailyByDate || typeof dailyByDate !== "object") return [];
  const safeEndDate =
    endDate && String(endDate).trim() ? String(endDate).slice(0, 10) : null;
  if (!safeEndDate) return [];
  const safeStartDate =
    startDate && String(startDate).trim() ? String(startDate).slice(0, 10) : null;

  const volumeBySymbol = new Map();
  const entries = Object.entries(dailyByDate);
  for (const [dayRaw, dayRowRaw] of entries) {
    const day = String(dayRaw || "").slice(0, 10);
    if (!day) continue;
    if (safeStartDate && day < safeStartDate) continue;
    if (day > safeEndDate) continue;
    const dayRow = dayRowRaw && typeof dayRowRaw === "object" ? dayRowRaw : {};
    const failedSymbols = Number(dayRow.failedSymbols || 0);
    if (requireReliableDay && Number.isFinite(failedSymbols) && failedSymbols > 0) continue;
    const symbolVolumes =
      dayRow.symbolVolumes && typeof dayRow.symbolVolumes === "object"
        ? dayRow.symbolVolumes
        : {};
    Object.entries(symbolVolumes).forEach(([symbolRaw, valueRaw]) => {
      const symbol = String(symbolRaw || "").trim();
      if (!symbol) return;
      const volume = toNum(valueRaw, 0);
      if (!Number.isFinite(volume) || volume <= 0) return;
      volumeBySymbol.set(symbol, (volumeBySymbol.get(symbol) || 0) + volume);
    });
  }

  return Array.from(volumeBySymbol.entries())
    .map(([symbol, volume]) => ({
      symbol,
      volume_usd: Number(volume.toFixed(8)),
    }))
    .sort((a, b) => toNum(b.volume_usd, 0) - toNum(a.volume_usd, 0))
    .slice(0, Math.max(1, Number(limit || 100)))
    .map((row, idx) => ({
      rank: idx + 1,
      symbol: row.symbol,
      market: row.symbol,
      volume_usd: row.volume_usd,
      // Kept for UI backward compatibility (table renderer checks this key first).
      volume_24h_usd: row.volume_usd,
      volumeUsd: toFixed(row.volume_usd, 2),
      volumeCompact: toCompact(row.volume_usd),
      live: true,
    }));
}

function countHistoricalDaysWithSymbolVolumes({
  dailyByDate = null,
  startDate = null,
  endDate = null,
  requireReliableDay = true,
}) {
  if (!dailyByDate || typeof dailyByDate !== "object") return 0;
  const safeEndDate =
    endDate && String(endDate).trim() ? String(endDate).slice(0, 10) : null;
  if (!safeEndDate) return 0;
  const safeStartDate =
    startDate && String(startDate).trim() ? String(startDate).slice(0, 10) : null;
  let days = 0;
  Object.entries(dailyByDate).forEach(([dayRaw, rowRaw]) => {
    const day = String(dayRaw || "").slice(0, 10);
    if (!day) return;
    if (safeStartDate && day < safeStartDate) return;
    if (day > safeEndDate) return;
    const row = rowRaw && typeof rowRaw === "object" ? rowRaw : {};
    const failedSymbols = Number(row.failedSymbols || 0);
    if (requireReliableDay && Number.isFinite(failedSymbols) && failedSymbols > 0) return;
    const volumes =
      row.symbolVolumes && typeof row.symbolVolumes === "object"
        ? row.symbolVolumes
        : null;
    if (!volumes || Array.isArray(volumes)) return;
    if (Object.keys(volumes).length <= 0) return;
    days += 1;
  });
  return days;
}

function sumRankVolumeUsd(rows = []) {
  return (Array.isArray(rows) ? rows : []).reduce((acc, row) => {
    const raw =
      row && row.volume_24h_usd !== undefined ? row.volume_24h_usd : row && row.volume_usd;
    return acc + toNum(raw, 0);
  }, 0);
}

const KPI_DAY_MS = 24 * 60 * 60 * 1000;

function pickWalletWindowBucket(record = {}, timeframe = "all") {
  const safeRecord = record && typeof record === "object" ? record : {};
  if (timeframe === "24h") return safeRecord.d24 || null;
  if (timeframe === "7d") return safeRecord.d7 || null;
  if (timeframe === "30d") return safeRecord.d30 || null;
  return safeRecord.all || null;
}

function aggregateWalletWindowMetrics(walletRecords = [], timeframe = "all") {
  const rows = Array.isArray(walletRecords) ? walletRecords : [];
  let totalTrades = 0;
  let totalVolumeUsd = 0;
  let totalFeesUsd = 0;
  let activeAccounts = 0;

  rows.forEach((record) => {
    const bucket = pickWalletWindowBucket(record, timeframe);
    if (!bucket || typeof bucket !== "object") return;

    const trades = toNum(bucket.trades, 0);
    const volumeUsd = toNum(bucket.volumeUsd, 0);
    const feesPaid = toNum(
      bucket.feesPaidUsd !== undefined ? bucket.feesPaidUsd : bucket.feesUsd,
      0
    );
    const liquidityPoolFeesUsd = toNum(bucket.liquidityPoolFeesUsd, 0);

    totalTrades += trades;
    totalVolumeUsd += volumeUsd;
    totalFeesUsd += feesPaid + liquidityPoolFeesUsd;

    if (trades > 0 || volumeUsd > 0 || feesPaid > 0 || liquidityPoolFeesUsd > 0) {
      activeAccounts += 1;
    }
  });

  return {
    totalAccounts: rows.length,
    activeAccounts,
    totalTrades,
    totalVolumeUsd,
    totalFeesUsd,
  };
}

function computeWalletCoverageDays(walletRecords = []) {
  const rows = Array.isArray(walletRecords) ? walletRecords : [];
  let minFirstTrade = Infinity;
  let maxLastTrade = 0;

  rows.forEach((record) => {
    const bucket = record && record.all && typeof record.all === "object" ? record.all : null;
    if (!bucket) return;
    const firstTrade = toNum(bucket.firstTrade, NaN);
    const lastTrade = toNum(bucket.lastTrade, NaN);
    if (Number.isFinite(firstTrade) && firstTrade > 0) {
      minFirstTrade = Math.min(minFirstTrade, firstTrade);
    }
    if (Number.isFinite(lastTrade) && lastTrade > 0) {
      maxLastTrade = Math.max(maxLastTrade, lastTrade);
    }
  });

  if (!Number.isFinite(minFirstTrade) || !Number.isFinite(maxLastTrade) || maxLastTrade < minFirstTrade) {
    return null;
  }

  return Math.max(1, Math.floor((maxLastTrade - minFirstTrade) / KPI_DAY_MS) + 1);
}

function buildWalletPerformanceSupportSummary({
  walletMetricsHistoryStatus = null,
  positionLifecycleStatus = null,
} = {}) {
  const historyStatus = walletMetricsHistoryStatus;
  const lifecycleStatus = positionLifecycleStatus;
  const available = [
    {
      key: "recent_trade_count",
      label: "Recent trade count",
      source: "wallet trade history",
    },
    {
      key: "recent_volume",
      label: "Recent traded volume",
      source: "wallet trade history",
    },
    {
      key: "realized_pnl",
      label: "Realized PnL",
      source: "wallet trade history",
    },
    {
      key: "unrealized_pnl",
      label: "Unrealized PnL",
      source: "live open positions",
    },
    {
      key: "total_pnl",
      label: "Total PnL",
      source: "wallet trade history + live open positions",
    },
    {
      key: "win_rate",
      label: "Win rate",
      source: "wallet trade history",
    },
    {
      key: "open_positions",
      label: "Open positions count",
      source: "wallet-first live positions",
    },
    {
      key: "exposure",
      label: "Current exposure",
      source: "wallet-first live positions",
    },
    {
      key: "symbol_concentration",
      label: "Symbol concentration",
      source: "wallet trade history",
    },
    {
      key: "recent_activity_score",
      label: "Recent activity score",
      source: "live wallet events + open positions",
    },
    {
      key: "live_active_score",
      label: "Live active score",
      source: "derived from live activity + exposure",
    },
    {
      key: "freshness_state",
      label: "Freshness / last update state",
      source: "live scan timestamps",
    },
  ];
  const unavailable = [];

  if (historyStatus && Number(historyStatus.hourlyFiles || 0) > 0) {
    available.push({
      key: "hourly_wallet_snapshots",
      label: "Hourly wallet snapshots",
      source: "persisted hourly wallet aggregate snapshot store",
    });
  } else {
    unavailable.push({
      key: "hourly_wallet_snapshots",
      label: "Hourly wallet snapshots",
      reason: "Rolling wallet snapshots are not persisted today.",
      required: "Hourly aggregate snapshot store.",
    });
  }

  if (historyStatus && Number(historyStatus.dailyFiles || 0) > 0) {
    available.push({
      key: "daily_wallet_snapshots",
      label: "Daily wallet snapshots",
      source: "persisted daily wallet aggregate snapshot store",
    });
  } else {
    unavailable.push({
      key: "daily_wallet_snapshots",
      label: "Daily wallet snapshots",
      reason: "Daily wallet performance aggregates are recomputed from history, not snapshotted.",
      required: "Daily wallet aggregate snapshot store.",
    });
  }

  if (lifecycleStatus && Number(lifecycleStatus.trackedPositions || 0) > 0) {
    available.push({
      key: "position_change_history",
      label: "Position increase / reduce history",
      source: "observed position lifecycle store",
      scope: "observed_from_tracking_start",
    });
    available.push({
      key: "position_holding_time",
      label: "Position holding time",
      source: "live position age + observed position lifecycle store",
      scope: "observed_from_tracking_start",
    });
    available.push({
      key: "wallet_holding_time",
      label: "Wallet holding time",
      source: "observed position lifecycle store",
      scope: "observed_from_tracking_start",
    });
  } else {
    unavailable.push({
      key: "wallet_holding_time",
      label: "Wallet holding time",
      reason: "Wallet snapshots do not persist entry/exit duration history.",
      required: "Wallet-level snapshot history with entry/exit timestamps.",
    });
    unavailable.push({
      key: "position_holding_time",
      label: "Position holding time",
      reason: "Position lifecycle events are not persisted historically per wallet.",
      required: "Explicit position lifecycle history with open/increase/reduce/close events.",
    });
    unavailable.push({
      key: "position_change_history",
      label: "Position increase / reduce history",
      reason: "Historical position state transitions are not materialized yet.",
      required: "Versioned position snapshots or lifecycle event log.",
    });
  }

  return {
    windows: ["24h", "7d", "30d", "all"],
    available,
    unavailable,
    status: {
      walletMetricsHistory: historyStatus || { enabled: false },
      positionLifecycle: lifecycleStatus || { enabled: false },
    },
  };
}

function buildKpiComparisonEntry({
  currentValue,
  baselineValue = null,
  comparisonLabel = "",
  fallbackLabel = "snapshot",
}) {
  const current = toNum(currentValue, 0);
  const baseline = toNum(baselineValue, NaN);
  if (!Number.isFinite(baseline) || baseline <= 0) {
    return {
      trend: "flat",
      deltaAbs: null,
      deltaPct: null,
      comparisonLabel: fallbackLabel,
      baselineValue: null,
      currentValue: current,
      comparisonAvailable: false,
    };
  }

  const deltaAbs = current - baseline;
  const deltaPct = baseline !== 0 ? (deltaAbs / baseline) * 100 : null;
  const trend =
    Math.abs(deltaAbs) < Math.max(1e-9, Math.abs(baseline) * 0.0005)
      ? "flat"
      : deltaAbs > 0
        ? "up"
        : "down";

  return {
    trend,
    deltaAbs,
    deltaPct,
    comparisonLabel,
    baselineValue: baseline,
    currentValue: current,
    comparisonAvailable: true,
  };
}

function buildExchangeKpiComparisons({
  timeframe = "all",
  walletWindows = {},
  volumeWindows = {},
  openInterestUsd = 0,
  walletCoverageDays = null,
  volumeCoverageDays = null,
}) {
  const tf = String(timeframe || "all").toLowerCase();
  const allWallets = walletWindows.all || {};
  const window24h = walletWindows["24h"] || {};
  const window30d = walletWindows["30d"] || {};
  const volume24h = volumeWindows["24h"] || {};
  const volume30d = volumeWindows["30d"] || {};
  const volumeAll = volumeWindows.all || {};

  if (tf === "24h") {
    return {
      timeframe: tf,
      metrics: {
        totalAccounts: {
          trend: "flat",
          deltaAbs: null,
          deltaPct: null,
          comparisonLabel: "active accounts",
          baselineValue: null,
          currentValue: toNum(window24h.activeAccounts, 0),
          comparisonAvailable: false,
        },
        totalTrades: buildKpiComparisonEntry({
          currentValue: window24h.totalTrades,
          baselineValue: toNum(window30d.totalTrades, 0) / 30,
          comparisonLabel: "vs 30d avg/day",
          fallbackLabel: "24h window",
        }),
        totalVolumeUsd: buildKpiComparisonEntry({
          currentValue: volume24h.totalVolumeUsd,
          baselineValue: toNum(volume30d.totalVolumeUsd, 0) / 30,
          comparisonLabel: "vs 30d avg/day",
          fallbackLabel: "24h window",
        }),
        totalFeesUsd: buildKpiComparisonEntry({
          currentValue: window24h.totalFeesUsd,
          baselineValue: toNum(window30d.totalFeesUsd, 0) / 30,
          comparisonLabel: "vs 30d avg/day",
          fallbackLabel: "24h window",
        }),
        openInterestAtEnd: {
          trend: "flat",
          deltaAbs: null,
          deltaPct: null,
          comparisonLabel: "historical delta unavailable",
          baselineValue: null,
          currentValue: toNum(openInterestUsd, 0),
          comparisonAvailable: false,
        },
      },
    };
  }

  if (tf === "30d") {
    const wallet30dBaseline =
      Number.isFinite(walletCoverageDays) && walletCoverageDays > 30
        ? (toNum(allWallets.totalTrades, 0) / walletCoverageDays) * 30
        : null;
    const wallet30dFeesBaseline =
      Number.isFinite(walletCoverageDays) && walletCoverageDays > 30
        ? (toNum(allWallets.totalFeesUsd, 0) / walletCoverageDays) * 30
        : null;
    const volume30dBaseline =
      Number.isFinite(volumeCoverageDays) && volumeCoverageDays > 30
        ? (toNum(volumeAll.totalVolumeUsd, 0) / volumeCoverageDays) * 30
        : null;

    return {
      timeframe: tf,
      metrics: {
        totalAccounts: {
          trend: "flat",
          deltaAbs: null,
          deltaPct: null,
          comparisonLabel: "active accounts",
          baselineValue: null,
          currentValue: toNum(window30d.activeAccounts, 0),
          comparisonAvailable: false,
        },
        totalTrades: buildKpiComparisonEntry({
          currentValue: window30d.totalTrades,
          baselineValue: wallet30dBaseline,
          comparisonLabel: "vs lifetime 30d run rate",
          fallbackLabel: "30d window",
        }),
        totalVolumeUsd: buildKpiComparisonEntry({
          currentValue: volume30d.totalVolumeUsd,
          baselineValue: volume30dBaseline,
          comparisonLabel: "vs lifetime 30d run rate",
          fallbackLabel: "30d window",
        }),
        totalFeesUsd: buildKpiComparisonEntry({
          currentValue: window30d.totalFeesUsd,
          baselineValue: wallet30dFeesBaseline,
          comparisonLabel: "vs lifetime 30d run rate",
          fallbackLabel: "30d window",
        }),
        openInterestAtEnd: {
          trend: "flat",
          deltaAbs: null,
          deltaPct: null,
          comparisonLabel: "historical delta unavailable",
          baselineValue: null,
          currentValue: toNum(openInterestUsd, 0),
          comparisonAvailable: false,
        },
      },
    };
  }

  return {
    timeframe: tf,
    metrics: {
      totalAccounts: {
        trend: "flat",
        deltaAbs: null,
        deltaPct: null,
        comparisonLabel: "active accounts",
        baselineValue: null,
        currentValue: toNum(allWallets.activeAccounts, 0),
        comparisonAvailable: false,
      },
      totalTrades: {
        trend: "flat",
        deltaAbs: null,
        deltaPct: null,
        comparisonLabel: "lifetime total",
        baselineValue: null,
        currentValue: toNum(allWallets.totalTrades, 0),
        comparisonAvailable: false,
      },
      totalVolumeUsd: {
        trend: "flat",
        deltaAbs: null,
        deltaPct: null,
        comparisonLabel: "lifetime total",
        baselineValue: null,
        currentValue: toNum(volumeAll.totalVolumeUsd, 0),
        comparisonAvailable: false,
      },
      totalFeesUsd: {
        trend: "flat",
        deltaAbs: null,
        deltaPct: null,
        comparisonLabel: "lifetime total",
        baselineValue: null,
        currentValue: toNum(allWallets.totalFeesUsd, 0),
        comparisonAvailable: false,
      },
      openInterestAtEnd: {
        trend: "flat",
        deltaAbs: null,
        deltaPct: null,
        comparisonLabel: "historical delta unavailable",
        baselineValue: null,
        currentValue: toNum(openInterestUsd, 0),
        comparisonAvailable: false,
      },
    },
  };
}

function extractPayloadData(result, fallback = null) {
  if (!result || !result.payload) return fallback;
  if (Object.prototype.hasOwnProperty.call(result.payload, "data")) {
    return result.payload.data;
  }
  return result.payload;
}

function normalizeTokenSymbol(value) {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9/_-]/g, "");
}

function buildTokenSymbolCandidates(symbol) {
  const normalized = normalizeTokenSymbol(symbol);
  if (!normalized) return [];
  const compact = normalized.replace(/[^A-Z0-9]/g, "");
  const noPerp = compact.replace(/PERP$/, "");
  const noUsd = noPerp.replace(/USDT$/, "").replace(/USD$/, "");
  return Array.from(new Set([normalized, compact, noPerp, noUsd].filter(Boolean)));
}

function findPriceRowForSymbol(rows = [], symbol) {
  const byCandidate = new Map();
  (Array.isArray(rows) ? rows : []).forEach((row) => {
    const rowSymbol = normalizeTokenSymbol(row && row.symbol);
    if (!rowSymbol) return;
    buildTokenSymbolCandidates(rowSymbol).forEach((candidate) => {
      if (!byCandidate.has(candidate)) byCandidate.set(candidate, row);
    });
  });
  const candidates = buildTokenSymbolCandidates(symbol);
  for (const candidate of candidates) {
    if (byCandidate.has(candidate)) return byCandidate.get(candidate);
  }
  return null;
}

function toTradeTimestampMs(row = {}) {
  const candidates = [
    row.timestamp,
    row.t,
    row.createdAt,
    row.created_at,
    row.updatedAt,
    row.updated_at,
  ];
  for (const candidate of candidates) {
    const ts = Number(candidate);
    if (!Number.isFinite(ts) || ts <= 0) continue;
    // Normalize second-based timestamps to ms.
    return ts < 1e12 ? ts * 1000 : ts;
  }
  return NaN;
}

function normalizeTradeSide(side) {
  return String(side || "")
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, "_");
}

function toNotionalUsd(row = {}) {
  const rawNotional = row.notional !== undefined ? row.notional : row.notional_usd;
  const asNumber = Number(rawNotional);
  if (Number.isFinite(asNumber) && asNumber > 0) return Math.abs(asNumber);
  const amount = Number(row.amount !== undefined ? row.amount : row.a);
  const price = Number(row.price !== undefined ? row.price : row.p);
  if (!Number.isFinite(amount) || !Number.isFinite(price)) return 0;
  return Math.abs(amount * price);
}

function normalizePublicTradeRow(row = {}) {
  return {
    historyId: row.history_id || row.historyId || row.h || null,
    symbol: normalizeTokenSymbol(row.symbol || row.s || ""),
    side: normalizeTradeSide(row.side || row.d || ""),
    cause: String(row.cause || row.tc || "").toLowerCase(),
    amount: Number(row.amount !== undefined ? row.amount : row.a),
    price: Number(row.price !== undefined ? row.price : row.p),
    timestamp: toTradeTimestampMs(row),
    notionalUsd: toNotionalUsd(row),
  };
}

function normalizeFundingHistoryRow(row = {}) {
  const createdAtRaw =
    row.created_at !== undefined ? row.created_at : row.createdAt !== undefined ? row.createdAt : row.t;
  const createdAt = toTradeTimestampMs({ timestamp: createdAtRaw });
  const alreadyPct = Number(row.fundingRatePct);
  if (Number.isFinite(alreadyPct)) {
    return {
      createdAt,
      fundingRatePct: alreadyPct,
    };
  }
  const fundingRateRaw =
    row.funding_rate !== undefined
      ? row.funding_rate
      : row.fundingRate !== undefined
      ? row.fundingRate
      : row.funding;
  const fundingRate = Number(fundingRateRaw);
  return {
    createdAt,
    fundingRatePct: Number.isFinite(fundingRate) ? fundingRate * 100 : NaN,
  };
}

function isLiquidationCause(cause) {
  const normalized = String(cause || "").toLowerCase();
  return (
    normalized.includes("liq") ||
    normalized.includes("liquid") ||
    normalized === "market_liquidation"
  );
}

function classifyNetflowSign(side) {
  const normalized = normalizeTradeSide(side);
  if (!normalized) return 0;
  if (
    normalized === "buy" ||
    normalized === "b" ||
    normalized === "open_long" ||
    normalized === "close_short"
  ) {
    return 1;
  }
  if (
    normalized === "sell" ||
    normalized === "s" ||
    normalized === "open_short" ||
    normalized === "close_long"
  ) {
    return -1;
  }
  return 0;
}

function classifyLiquidationBucket(side) {
  const normalized = normalizeTradeSide(side);
  if (!normalized) return "unknown";
  // Long liquidations close long risk (typically sell / close_long / open_short prints).
  if (
    normalized === "sell" ||
    normalized === "s" ||
    normalized === "close_long" ||
    normalized === "open_short"
  ) {
    return "long";
  }
  // Short liquidations close short risk (typically buy / close_short / open_long prints).
  if (
    normalized === "buy" ||
    normalized === "b" ||
    normalized === "close_short" ||
    normalized === "open_long"
  ) {
    return "short";
  }
  return "unknown";
}

function dedupeBy(rows = [], keyFn) {
  const seen = new Set();
  const out = [];
  (Array.isArray(rows) ? rows : []).forEach((row) => {
    const key = keyFn(row);
    if (!key || seen.has(key)) return;
    seen.add(key);
    out.push(row);
  });
  return out;
}

function normalizeHistoryIdKey(value) {
  if (value === null || value === undefined) return "";
  const raw = String(value).trim();
  return raw || "";
}

function createWalletTrackingComponent({
  sendJson,
  pipeline,
  walletStore,
  walletIndexer,
  restClient,
  liveRestClientEntries = [],
  globalKpiProvider,
  solanaLogTriggerMonitor = null,
  walletMetricsHistoryStore = null,
  positionLifecycleStore = null,
}) {
  const liveTradesUpstreamBaseRaw = String(
    process.env.PACIFICA_LIVE_TRADES_UPSTREAM_BASE || ""
  ).trim();
  const liveTradesUpstreamBase = liveTradesUpstreamBaseRaw.replace(/\/+$/, "");
  const liveTradesUpstreamEnabled = /^https?:\/\//i.test(liveTradesUpstreamBase);
  const defillamaCache = {
    fetchedAt: 0,
    ttlMs: 15000,
    value: {
      prices: [],
      dailyVolume: 0,
      openInterestAtEnd: 0,
    },
    stale: false,
    lastError: null,
    lastFetchDurationMs: null,
  };
  const walletStoreRefresh = {
    lastAt: 0,
    minIntervalMs: Math.max(
      1500,
      Number(process.env.PACIFICA_WALLET_STORE_REFRESH_MIN_INTERVAL_MS || 60000)
    ),
  };
  const indexerStatusCache = {
    fetchedAt: 0,
    ttlMs: 2000,
    value: null,
  };
  const livePositionPriceCache = {
    fetchedAt: 0,
    ttlMs: Math.max(
      1000,
      Number(process.env.PACIFICA_LIVE_POSITION_PRICE_CACHE_TTL_MS || 2500)
    ),
    value: new Map(),
    inflight: null,
    stale: false,
    lastError: null,
  };
  const walletExplorerDatasetCache = {
    generatedAt: 0,
    walletStoreUpdatedAt: 0,
    liveSnapshotGeneratedAt: 0,
    ttlMs: Math.max(
      1000,
      Number(process.env.PACIFICA_WALLET_EXPLORER_DATASET_TTL_MS || 2000)
    ),
    refreshIntervalMs: Math.max(
      2000,
      Number(process.env.PACIFICA_WALLET_EXPLORER_DATASET_REFRESH_MS || 3000)
    ),
    value: null,
    inflight: null,
    lastError: null,
    lastDurationMs: null,
  };
  const defaultDataRoot = process.env.PACIFICA_DATA_DIR
    ? path.resolve(process.env.PACIFICA_DATA_DIR)
    : path.join(process.cwd(), "data");
  const liveTradeAttributionDbEnabled =
    Object.prototype.hasOwnProperty.call(
      process.env,
      "PACIFICA_LIVE_TRADE_ATTRIBUTION_DB_ENABLED"
    )
      ? String(process.env.PACIFICA_LIVE_TRADE_ATTRIBUTION_DB_ENABLED || "true").toLowerCase() !==
        "false"
      : Boolean(walletIndexer);
  const liveTradeAttributionDbPath = String(
    process.env.PACIFICA_LIVE_TRADE_ATTRIBUTION_DB_PATH ||
      path.join(defaultDataRoot, "indexer", "live_trade_attribution_db.json")
  ).trim();
  const liveTradeAttributionStore = liveTradeAttributionDbEnabled
    ? new LiveTradeAttributionStore({
        dataDir: path.join(defaultDataRoot, "indexer"),
        filePath: liveTradeAttributionDbPath,
        flushIntervalMs: Math.max(
          1000,
          Number(process.env.PACIFICA_LIVE_TRADE_ATTRIBUTION_DB_FLUSH_MS || 15000)
        ),
        maxBufferedUpdates: Math.max(
          20,
          Number(process.env.PACIFICA_LIVE_TRADE_ATTRIBUTION_DB_MAX_BUFFERED || 500)
        ),
        maxEvidenceRows: Math.max(
          1000,
          Number(process.env.PACIFICA_LIVE_TRADE_ATTRIBUTION_DB_MAX_EVIDENCE || 50000)
        ),
        maxLinksPerKind: Math.max(
          10000,
          Number(process.env.PACIFICA_LIVE_TRADE_ATTRIBUTION_DB_MAX_LINKS_PER_KIND || 300000)
        ),
      })
    : null;
  const liveTradeWalletHistoryDir = String(
    process.env.PACIFICA_LIVE_TRADE_WALLET_HISTORY_DIR ||
      process.env.PACIFICA_INDEXER_WALLET_HISTORY_DIR ||
      path.join(defaultDataRoot, "indexer", "wallet_history")
  ).trim();
  const liveTradeWalletAttributionEnabled =
    Object.prototype.hasOwnProperty.call(
      process.env,
      "PACIFICA_LIVE_TRADE_WALLET_ATTRIBUTION_ENABLED"
    )
      ? String(process.env.PACIFICA_LIVE_TRADE_WALLET_ATTRIBUTION_ENABLED || "true").toLowerCase() !==
        "false"
      : Boolean(walletIndexer);
  const liveTradeWalletAttributionScanFilesPerPass = Math.max(
    1,
    Number(process.env.PACIFICA_LIVE_TRADE_WALLET_ATTRIBUTION_SCAN_FILES_PER_PASS || 80)
  );
  const liveTradeWalletAttributionInitialScanFiles = Math.max(
    200,
    Number(
      process.env.PACIFICA_LIVE_TRADE_WALLET_ATTRIBUTION_INITIAL_SCAN_FILES ||
        liveTradeWalletAttributionScanFilesPerPass
    )
  );
  const liveTradeWalletAttributionScanIntervalMs = Math.max(
    1000,
    Number(process.env.PACIFICA_LIVE_TRADE_WALLET_ATTRIBUTION_SCAN_INTERVAL_MS || 10000)
  );
  const liveTradeWalletAttributionFileListTtlMs = Math.max(
    10000,
    Number(process.env.PACIFICA_LIVE_TRADE_WALLET_ATTRIBUTION_FILE_LIST_TTL_MS || 300000)
  );
  const liveTradeWalletAttributionPriorityWallets = Math.max(
    100,
    Number(process.env.PACIFICA_LIVE_TRADE_WALLET_ATTRIBUTION_PRIORITY_WALLETS || 500)
  );
  const liveTradeWalletAttributionPendingMax = Math.max(
    1000,
    Number(process.env.PACIFICA_LIVE_TRADE_WALLET_ATTRIBUTION_PENDING_MAX || 80000)
  );
  const liveTradeWalletAttributionCacheMax = Math.max(
    5000,
    Number(process.env.PACIFICA_LIVE_TRADE_WALLET_ATTRIBUTION_CACHE_MAX || 300000)
  );
  const liveTradeWalletAttributionQueuePerBuild = Math.max(
    0,
    Number(process.env.PACIFICA_LIVE_TRADE_WALLET_ATTRIBUTION_QUEUE_PER_BUILD || 6000)
  );
  const liveTradeTxAttributionEnabled =
    Object.prototype.hasOwnProperty.call(process.env, "PACIFICA_LIVE_TRADE_TX_ATTRIBUTION_ENABLED")
      ? String(process.env.PACIFICA_LIVE_TRADE_TX_ATTRIBUTION_ENABLED || "true").toLowerCase() !==
        "false"
      : Boolean(walletIndexer);
  const liveTradeTxAttributionScanIntervalMs = Math.max(
    2000,
    Number(process.env.PACIFICA_LIVE_TRADE_TX_ATTRIBUTION_SCAN_INTERVAL_MS || 12000)
  );
  const liveTradeTxAttributionRpcLimit = Math.max(
    10,
    Math.min(200, Number(process.env.PACIFICA_LIVE_TRADE_TX_ATTRIBUTION_RPC_LIMIT || 80))
  );
  const liveTradeTxAttributionFetchMaxPerPass = Math.max(
    5,
    Math.min(
      liveTradeTxAttributionRpcLimit,
      Number(process.env.PACIFICA_LIVE_TRADE_TX_ATTRIBUTION_FETCH_MAX_PER_PASS || 24)
    )
  );
  const liveTradeTxAttributionSeenMax = Math.max(
    1000,
    Number(process.env.PACIFICA_LIVE_TRADE_TX_ATTRIBUTION_SEEN_MAX || 40000)
  );
  const liveTradeTxAttributionCacheMax = Math.max(
    1000,
    Number(process.env.PACIFICA_LIVE_TRADE_TX_ATTRIBUTION_CACHE_MAX || 120000)
  );
  const pacificaProgramId = String(
    process.env.PACIFICA_LIVE_TRADE_TX_ATTRIBUTION_PROGRAM_ID ||
      (process.env.PACIFICA_PROGRAM_IDS || "").split(",")[0] ||
      ""
  ).trim();
  const solanaRpcUrl = String(process.env.SOLANA_RPC_URL || "").trim();
  const liveTradeWalletAttribution = {
    historyToWallet: new Map(),
    orderToWallet: new Map(),
    liToWallet: new Map(),
    pendingHistoryIds: new Set(),
    pendingOrderIds: new Set(),
    pendingLiIds: new Set(),
    files: [],
    fileCursor: 0,
    filesLoadedAt: 0,
    lastScanAt: 0,
    scanPasses: 0,
    scannedFiles: 0,
    resolved: 0,
    resolvedByOrder: 0,
    resolvedByLi: 0,
    queueCursor: 0,
  };
  const liveTradeTxAttribution = {
    historyToTx: new Map(),
    orderToTx: new Map(),
    liToTx: new Map(),
    pendingHistoryIds: new Set(),
    pendingOrderIds: new Set(),
    pendingLiIds: new Set(),
    seenSignatures: new Set(),
    seenSignatureQueue: [],
    lastScanAt: 0,
    scanPasses: 0,
    rpcCalls: 0,
    txFetched: 0,
    txMatchedByHistory: 0,
    txMatchedByOrder: 0,
    txMatchedByLi: 0,
    running: false,
  };
  const liveTradeRecentSignatureWallets = new Map();
  const liveTradeRecentSignatureWalletQueue = [];
  const liveTradeRecentSignatureWalletMax = Math.max(
    1000,
    Number(process.env.PACIFICA_LIVE_TRADE_TRIGGER_SIGNATURE_CACHE_MAX || 20000)
  );
  const liveWalletTriggerDedup = new Map();
  function recordLiveWalletTrigger(trigger) {
    if (!liveWalletTriggerStore || typeof liveWalletTriggerStore.append !== "function") return;
    const wallet = String(trigger && trigger.wallet ? trigger.wallet : "").trim();
    if (!wallet) return;
    const at = Math.max(0, Number(trigger && (trigger.at || trigger.timestamp) ? trigger.at || trigger.timestamp : Date.now()));
    const source = String(trigger && trigger.source ? trigger.source : "activity").trim() || "activity";
    const method = String(trigger && trigger.method ? trigger.method : "").trim() || "activity";
    const key = `${wallet}:${source}:${method}:${Math.floor(at / 1000)}`;
    const lastSeen = Number(liveWalletTriggerDedup.get(key) || 0);
    if (lastSeen && at - lastSeen < 30000) return;
    liveWalletTriggerDedup.set(key, at);
    if (liveWalletTriggerDedup.size > 5000) {
      const cutoff = Date.now() - 10 * 60 * 1000;
      for (const [dedupeKey, ts] of liveWalletTriggerDedup.entries()) {
        if (Number(ts || 0) < cutoff) liveWalletTriggerDedup.delete(dedupeKey);
      }
    }
    const signature = String(
      trigger && (trigger.signature || trigger.txSignature) ? trigger.signature || trigger.txSignature : ""
    ).trim();
    if (signature) {
      liveTradeRecentSignatureWallets.set(signature, {
        wallet,
        at,
        source,
        method,
        confidence: String(trigger && trigger.confidence ? trigger.confidence : "unknown").trim() || "unknown",
      });
      liveTradeRecentSignatureWalletQueue.push(signature);
      while (liveTradeRecentSignatureWalletQueue.length > liveTradeRecentSignatureWalletMax) {
        const evicted = liveTradeRecentSignatureWalletQueue.shift();
        if (evicted) liveTradeRecentSignatureWallets.delete(evicted);
      }
    }
    liveWalletTriggerStore.append(trigger);
  }
  if (liveTradeAttributionStore) {
    try {
      liveTradeAttributionStore.load();
      const hydrated = liveTradeAttributionStore.hydrateMaps();
      if (hydrated && hydrated.walletMaps) {
        hydrated.walletMaps.history.forEach((value, key) => {
          liveTradeWalletAttribution.historyToWallet.set(key, value);
        });
        hydrated.walletMaps.order.forEach((value, key) => {
          liveTradeWalletAttribution.orderToWallet.set(key, value);
        });
        hydrated.walletMaps.li.forEach((value, key) => {
          liveTradeWalletAttribution.liToWallet.set(key, value);
        });
      }
      if (hydrated && hydrated.txMaps) {
        hydrated.txMaps.history.forEach((value, key) => {
          liveTradeTxAttribution.historyToTx.set(key, value);
        });
        hydrated.txMaps.order.forEach((value, key) => {
          liveTradeTxAttribution.orderToTx.set(key, value);
        });
        hydrated.txMaps.li.forEach((value, key) => {
          liveTradeTxAttribution.liToTx.set(key, value);
        });
      }
    } catch (_error) {
      // Keep service available even if attribution DB load fails.
    }
  }
  const liveWalletFirstEnabled =
    (Object.prototype.hasOwnProperty.call(process.env, "PACIFICA_LIVE_WALLET_FIRST_ENABLED")
      ? String(process.env.PACIFICA_LIVE_WALLET_FIRST_ENABLED || "true").toLowerCase() !== "false"
      : Boolean(walletIndexer)) && !liveTradesUpstreamEnabled;
  const liveWalletFirstPersistDir = String(
    process.env.PACIFICA_LIVE_WALLET_FIRST_PERSIST_DIR ||
      path.join(defaultDataRoot, "live_positions")
  ).trim();
  const hotWalletWsEnabled =
    String(process.env.PACIFICA_HOT_WALLET_WS_ENABLED || "true").toLowerCase() !== "false";
  const hotWalletWsUrl = String(
    process.env.PACIFICA_HOT_WALLET_WS_URL || process.env.PACIFICA_WS_URL || "wss://ws.pacifica.fi/ws"
  ).trim();
  const hotWalletWsMaxWallets = Math.max(
    1,
    Number(process.env.PACIFICA_HOT_WALLET_WS_MAX_WALLETS || 32)
  );
  const hotWalletWsInitialWallets = Math.max(
    1,
    Math.min(
      hotWalletWsMaxWallets,
      Number(process.env.PACIFICA_HOT_WALLET_WS_INITIAL_WALLETS || hotWalletWsMaxWallets)
    )
  );
  const hotWalletWsCapacityStep = Math.max(
    1,
    Number(process.env.PACIFICA_HOT_WALLET_WS_CAPACITY_STEP || 4)
  );
  const hotWalletWsSoakWindowMs = Math.max(
    30000,
    Number(process.env.PACIFICA_HOT_WALLET_WS_SOAK_WINDOW_MS || 3 * 60 * 1000)
  );
  const hotWalletWsMaxScaleErrors = Math.max(
    0,
    Number(process.env.PACIFICA_HOT_WALLET_WS_MAX_SCALE_ERRORS || 0)
  );
  const hotWalletWsMaxScaleReconnects = Math.max(
    0,
    Number(process.env.PACIFICA_HOT_WALLET_WS_MAX_SCALE_RECONNECTS || 1)
  );
  const hotWalletWsMinScaleUtilizationPct = Math.max(
    1,
    Math.min(100, Number(process.env.PACIFICA_HOT_WALLET_WS_MIN_SCALE_UTILIZATION_PCT || 92))
  );
  const hotWalletWsMinScaleBacklog = Math.max(
    0,
    Number(process.env.PACIFICA_HOT_WALLET_WS_MIN_SCALE_BACKLOG || 1)
  );
  const hotWalletWsMemoryCeilingMb = Math.max(
    0,
    Number(process.env.PACIFICA_HOT_WALLET_WS_MEMORY_CEILING_MB || 0)
  );
  const hotWalletWsInactivityMs = Math.max(
    15000,
    Number(process.env.PACIFICA_HOT_WALLET_WS_INACTIVITY_MS || 2 * 60 * 1000)
  );
  const hotWalletWsOpenPositionHoldMs = Math.max(
    hotWalletWsInactivityMs,
    Number(process.env.PACIFICA_HOT_WALLET_WS_OPEN_POSITION_HOLD_MS || 5 * 60 * 1000)
  );
  const hotWalletWsTradeHoldMs = Math.max(
    15000,
    Number(process.env.PACIFICA_HOT_WALLET_WS_TRADE_HOLD_MS || 45 * 1000)
  );
  const hotWalletWsAggressiveEvictMs = Math.max(
    10000,
    Number(process.env.PACIFICA_HOT_WALLET_WS_AGGRESSIVE_EVICT_MS || 20 * 1000)
  );
  const liveWalletTriggerStoreEnabled =
    String(process.env.PACIFICA_LIVE_WALLET_TRIGGER_STORE_ENABLED || "true").toLowerCase() !== "false";
  const liveWalletTriggerFile = String(
    process.env.PACIFICA_LIVE_WALLET_TRIGGER_FILE ||
      path.join(liveWalletFirstPersistDir, "wallet_activity_triggers.ndjson")
  ).trim();
  const liveWalletTriggerPollMs = Math.max(
    500,
    Number(process.env.PACIFICA_LIVE_WALLET_TRIGGER_POLL_MS || 2000)
  );
  const liveWalletFirstExternalShardsEnabled =
    String(
      process.env.PACIFICA_LIVE_WALLET_FIRST_EXTERNAL_SHARDS || "false"
    ).toLowerCase() === "true";
  const normalizedLiveRestClientEntries = Array.isArray(liveRestClientEntries)
    ? liveRestClientEntries
        .map((entry, idx) => {
          const client =
            entry && entry.client && typeof entry.client.get === "function" ? entry.client : null;
          if (!client) return null;
          return {
            id: String((entry && entry.id) || `live_client_${idx + 1}`),
            client,
            proxyUrl: entry && entry.proxyUrl ? String(entry.proxyUrl) : null,
          };
        })
        .filter(Boolean)
    : [];
  if (!normalizedLiveRestClientEntries.length && restClient && typeof restClient.get === "function") {
    normalizedLiveRestClientEntries.push({
      id: "direct",
      client: restClient,
      proxyUrl: null,
    });
  }
  const liveWalletFirstIncludeDirect =
    String(process.env.PACIFICA_LIVE_WALLET_FIRST_INCLUDE_DIRECT || "false").toLowerCase() === "true";
  const liveWalletFirstForceDirect =
    String(process.env.PACIFICA_LIVE_WALLET_FIRST_FORCE_DIRECT || "false").toLowerCase() === "true";
  let effectiveLiveRestClientEntries =
    normalizedLiveRestClientEntries.length > 1 && !liveWalletFirstIncludeDirect
      ? normalizedLiveRestClientEntries.filter((entry) => String(entry && entry.id) !== "direct")
      : normalizedLiveRestClientEntries;
  if (liveWalletFirstForceDirect) {
    const directOnly = normalizedLiveRestClientEntries.filter(
      (entry) => String(entry && entry.id).toLowerCase() === "direct"
    );
    if (directOnly.length > 0) {
      effectiveLiveRestClientEntries = directOnly;
    }
  }
  const liveWalletFirstClientCount = effectiveLiveRestClientEntries.length;
  const liveWalletFirstMultiClient = liveWalletFirstClientCount > 1;
  const liveWalletFirstScanIntervalMs = Math.max(
    500,
    Number(
      process.env.PACIFICA_LIVE_WALLET_FIRST_SCAN_INTERVAL_MS ||
        (liveWalletFirstMultiClient ? 1500 : 5000)
    )
  );
  const liveWalletFirstWalletListRefreshMs = Math.max(
    5000,
    Number(process.env.PACIFICA_LIVE_WALLET_FIRST_WALLET_LIST_REFRESH_MS || 60000)
  );
  const liveWalletFirstWalletsPerPassRaw = String(
    process.env.PACIFICA_LIVE_WALLET_FIRST_WALLETS_PER_PASS || ""
  ).trim();
  const liveWalletFirstWalletsPerPass = (() => {
    if (!liveWalletFirstWalletsPerPassRaw) return 0;
    const parsed = Number(liveWalletFirstWalletsPerPassRaw);
    if (Number.isFinite(parsed)) return Math.max(0, Math.floor(parsed));
    // 0 means "auto rolling micro-batches" while preserving full-wallet coverage over time.
    return 0;
  })();
  const liveWalletFirstHotWalletsPerPassRaw = String(
    process.env.PACIFICA_LIVE_WALLET_FIRST_HOT_WALLETS_PER_PASS || ""
  ).trim();
  const liveWalletFirstHotWalletsPerPass = (() => {
    if (!liveWalletFirstHotWalletsPerPassRaw) {
      // Even in full-sweep mode, prioritize hot wallets first to reduce live-latency.
      if (liveWalletFirstWalletsPerPass <= 0) {
        return liveWalletFirstMultiClient
          ? Math.max(64, Math.min(1000, liveWalletFirstClientCount * 6))
          : 16;
      }
      return liveWalletFirstMultiClient
        ? Math.max(40, Math.min(400, liveWalletFirstClientCount * 2))
        : 4;
    }
    const parsed = Number(liveWalletFirstHotWalletsPerPassRaw);
    if (Number.isFinite(parsed)) return Math.max(0, Math.floor(parsed));
    return 0;
  })();
  const liveWalletFirstWarmWalletsPerPass = Math.max(
    0,
    Number(
      process.env.PACIFICA_LIVE_WALLET_FIRST_WARM_WALLETS_PER_PASS ||
        Math.max(liveWalletFirstHotWalletsPerPass, liveWalletFirstClientCount * 8 || 32)
    )
  );
  const liveWalletFirstRecentActiveWalletsPerPass = Math.max(
    0,
    Number(
      process.env.PACIFICA_LIVE_WALLET_FIRST_RECENT_ACTIVE_WALLETS_PER_PASS ||
        Math.max(16, Math.floor(liveWalletFirstHotWalletsPerPass / 2))
    )
  );
  const liveWalletFirstMaxConcurrency = Math.max(
    1,
    Number(
      process.env.PACIFICA_LIVE_WALLET_FIRST_MAX_CONCURRENCY ||
        (liveWalletFirstMultiClient
          ? Math.max(48, Math.min(512, liveWalletFirstClientCount * 2))
          : 8)
    )
  );
  const liveWalletFirstRequestTimeoutMs = Math.max(
    1500,
    Number(process.env.PACIFICA_LIVE_WALLET_FIRST_REQUEST_TIMEOUT_MS || 3000)
  );
  const liveWalletFirstMaxFetchAttempts = Math.max(
    1,
    Math.min(5, Number(process.env.PACIFICA_LIVE_WALLET_FIRST_MAX_FETCH_ATTEMPTS || 2))
  );
  const liveWalletFirstMaxInFlightPerClient = Math.max(
    1,
    Math.floor(Number(process.env.PACIFICA_LIVE_WALLET_FIRST_MAX_INFLIGHT_PER_CLIENT || 2))
  );
  const liveWalletFirstMaxInFlightDirect = Math.max(
    1,
    Math.floor(Number(process.env.PACIFICA_LIVE_WALLET_FIRST_MAX_INFLIGHT_DIRECT || 1))
  );
  const liveWalletFirstDirectFallbackOnLastAttempt =
    String(process.env.PACIFICA_LIVE_WALLET_FIRST_DIRECT_FALLBACK_ON_LAST_ATTEMPT || "false").toLowerCase() ===
    "true";
  const liveWalletFirstRateLimitBackoffBaseMs = Math.max(
    1000,
    Number(process.env.PACIFICA_LIVE_WALLET_FIRST_RATE_LIMIT_BACKOFF_BASE_MS || 5000)
  );
  const liveWalletFirstRateLimitBackoffMaxMs = Math.max(
    liveWalletFirstRateLimitBackoffBaseMs,
    Number(process.env.PACIFICA_LIVE_WALLET_FIRST_RATE_LIMIT_BACKOFF_MAX_MS || 120000)
  );
  const liveWalletFirstShardCount = Math.max(
    1,
    Math.floor(Number(process.env.PACIFICA_LIVE_WALLET_FIRST_SHARD_COUNT || 1))
  );
  const liveWalletFirstShardIndex = Math.min(
    liveWalletFirstShardCount - 1,
    Math.max(0, Math.floor(Number(process.env.PACIFICA_LIVE_WALLET_FIRST_SHARD_INDEX || 0)))
  );
  const liveWalletFirstMaxEvents = Math.max(
    100,
    Number(process.env.PACIFICA_LIVE_WALLET_FIRST_MAX_EVENTS || 50000)
  );
  const liveWalletFirstStaleMs = Math.max(
    15000,
    Number(process.env.PACIFICA_LIVE_WALLET_FIRST_STALE_MS || 180000)
  );
  const liveWalletFirstCoolingMs = Math.max(
    5000,
    Number(process.env.PACIFICA_LIVE_WALLET_FIRST_COOLING_MS || 60000)
  );
  const liveWalletFirstTargetPassDurationMs = Math.max(
    3000,
    Number(process.env.PACIFICA_LIVE_WALLET_FIRST_TARGET_PASS_MS || 15000)
  );
  const liveWalletFirstRecentActivityTtlMs = Math.max(
    60000,
    Number(process.env.PACIFICA_LIVE_WALLET_FIRST_RECENT_ACTIVITY_TTL_MS || 30 * 60 * 1000)
  );
  const liveWalletFirstWarmWalletRecentMs = Math.max(
    60 * 60 * 1000,
    Number(process.env.PACIFICA_LIVE_WALLET_FIRST_WARM_WALLET_RECENT_MS || 3 * 24 * 60 * 60 * 1000)
  );
  const liveWalletFirstMaxWarmWallets = Math.max(
    100,
    Number(process.env.PACIFICA_LIVE_WALLET_FIRST_MAX_WARM_WALLETS || 4000)
  );
  const liveWalletFirstHotReconcileMaxAgeMs = Math.max(
    5000,
    Number(process.env.PACIFICA_LIVE_WALLET_FIRST_HOT_RECONCILE_MAX_AGE_MS || 30 * 1000)
  );
  const liveWalletFirstWarmReconcileMaxAgeMs = Math.max(
    liveWalletFirstHotReconcileMaxAgeMs,
    Number(process.env.PACIFICA_LIVE_WALLET_FIRST_WARM_RECONCILE_MAX_AGE_MS || 3 * 60 * 1000)
  );
  const liveWalletTriggerStore =
    liveWalletTriggerStoreEnabled && liveWalletFirstPersistDir
      ? new LiveWalletTriggerStore({
          filePath: liveWalletTriggerFile,
          maxEntries: Math.max(
            5000,
            Number(process.env.PACIFICA_LIVE_WALLET_TRIGGER_MAX_ENTRIES || 50000)
          ),
        })
      : null;
  const pacificaPublicActiveCache = {
    fetchedAt: 0,
    ttlMs: Math.max(10000, Number(process.env.PACIFICA_PUBLIC_ACTIVE_CACHE_TTL_MS || 60000)),
    value: null,
    lastError: null,
    inflight: null,
  };
  const liveActiveWindowMs = Math.max(
    5 * 60 * 1000,
    Number(process.env.PACIFICA_LIVE_ACTIVE_WINDOW_MS || 30 * 60 * 1000)
  );
  let liveWalletPositionsMonitor = null;
  const hotWalletWsMonitor = new HotWalletWsMonitor({
    enabled: hotWalletWsEnabled && liveWalletFirstEnabled && !liveWalletFirstExternalShardsEnabled,
    wsUrl: hotWalletWsUrl,
    logger: console,
    maxWallets: hotWalletWsMaxWallets,
    initialWallets: hotWalletWsInitialWallets,
    capacityStep: hotWalletWsCapacityStep,
    soakWindowMs: hotWalletWsSoakWindowMs,
    maxScaleErrors: hotWalletWsMaxScaleErrors,
    maxScaleReconnects: hotWalletWsMaxScaleReconnects,
    minScaleUtilizationPct: hotWalletWsMinScaleUtilizationPct,
    minScaleBacklog: hotWalletWsMinScaleBacklog,
    memoryCeilingMb: hotWalletWsMemoryCeilingMb,
    inactivityMs: hotWalletWsInactivityMs,
    openPositionHoldMs: hotWalletWsOpenPositionHoldMs,
    tradeHoldMs: hotWalletWsTradeHoldMs,
    aggressiveEvictMs: hotWalletWsAggressiveEvictMs,
    onPositions: (wallet, rows, meta) => {
      if (
        liveWalletPositionsMonitor &&
        typeof liveWalletPositionsMonitor.ingestHotWalletPositions === "function"
      ) {
        liveWalletPositionsMonitor.ingestHotWalletPositions(wallet, rows, meta);
      }
    },
    onTrades: (wallet, rows, meta) => {
      const first = Array.isArray(rows) && rows.length ? rows[0] : null;
      if (
        liveWalletPositionsMonitor &&
        typeof liveWalletPositionsMonitor.ingestHotWalletActivity === "function"
      ) {
        liveWalletPositionsMonitor.ingestHotWalletActivity(wallet, "ws_account_trade", {
          ...meta,
          symbol: first && first.s ? String(first.s).toUpperCase() : null,
          side: first && first.ts ? String(first.ts).toLowerCase() : null,
        });
      }
    },
    onOrderUpdates: (wallet, rows, meta) => {
      const first = Array.isArray(rows) && rows.length ? rows[0] : null;
      if (
        liveWalletPositionsMonitor &&
        typeof liveWalletPositionsMonitor.ingestHotWalletActivity === "function"
      ) {
        liveWalletPositionsMonitor.ingestHotWalletActivity(wallet, "ws_account_order_update", {
          ...meta,
          symbol: first && first.s ? String(first.s).toUpperCase() : null,
          side: first && first.d ? String(first.d).toLowerCase() : null,
        });
      }
    },
  });
  liveWalletPositionsMonitor = new WalletFirstLivePositionsMonitor({
    enabled: liveWalletFirstEnabled,
    restClient,
    restClientEntries: effectiveLiveRestClientEntries,
    walletStore,
    hotWalletWsMonitor,
    triggerStore: liveWalletTriggerStore,
    logger: console,
    scanIntervalMs: liveWalletFirstScanIntervalMs,
    walletListRefreshMs: liveWalletFirstWalletListRefreshMs,
    walletsPerPass: liveWalletFirstWalletsPerPass,
    hotWalletsPerPass: liveWalletFirstHotWalletsPerPass,
    warmWalletsPerPass: liveWalletFirstWarmWalletsPerPass,
    recentActiveWalletsPerPass: liveWalletFirstRecentActiveWalletsPerPass,
    maxConcurrency: liveWalletFirstMaxConcurrency,
    requestTimeoutMs: liveWalletFirstRequestTimeoutMs,
    maxFetchAttempts: liveWalletFirstMaxFetchAttempts,
    maxInFlightPerClient: liveWalletFirstMaxInFlightPerClient,
    maxInFlightDirect: liveWalletFirstMaxInFlightDirect,
    directFallbackOnLastAttempt: liveWalletFirstDirectFallbackOnLastAttempt,
    rateLimitBackoffBaseMs: liveWalletFirstRateLimitBackoffBaseMs,
    rateLimitBackoffMaxMs: liveWalletFirstRateLimitBackoffMaxMs,
    shardCount: liveWalletFirstShardCount,
    shardIndex: liveWalletFirstShardIndex,
    maxEvents: liveWalletFirstMaxEvents,
    staleMs: liveWalletFirstStaleMs,
    coolingMs: liveWalletFirstCoolingMs,
    targetPassDurationMs: liveWalletFirstTargetPassDurationMs,
    recentActivityTtlMs: liveWalletFirstRecentActivityTtlMs,
    warmWalletRecentMs: liveWalletFirstWarmWalletRecentMs,
    maxWarmWallets: liveWalletFirstMaxWarmWallets,
    hotReconcileMaxAgeMs: liveWalletFirstHotReconcileMaxAgeMs,
    warmReconcileMaxAgeMs: liveWalletFirstWarmReconcileMaxAgeMs,
    triggerPollMs: liveWalletTriggerPollMs,
    publicActiveWalletsProvider: getPacificaTrackedPublicActiveWallets,
    persistDir: liveWalletFirstPersistDir,
    persistEveryMs: Math.max(
      1000,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_PERSIST_EVERY_MS || 5000)
    ),
    maxPersistedEvents: Math.max(
      50,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_MAX_PERSISTED_EVENTS || 500)
    ),
  });
  if (liveWalletFirstEnabled) {
    console.log(
      `[wallet-first-live] enabled clients=${liveWalletFirstClientCount} include_direct=${liveWalletFirstIncludeDirect} wallets_per_pass=${
        liveWalletFirstWalletsPerPass > 0 ? liveWalletFirstWalletsPerPass : "auto"
      } hot_wallets_per_pass=${liveWalletFirstHotWalletsPerPass} warm_wallets_per_pass=${liveWalletFirstWarmWalletsPerPass} recent_active_wallets_per_pass=${liveWalletFirstRecentActiveWalletsPerPass} target_pass_ms=${liveWalletFirstTargetPassDurationMs} max_concurrency=${liveWalletFirstMaxConcurrency} request_timeout_ms=${liveWalletFirstRequestTimeoutMs} max_fetch_attempts=${liveWalletFirstMaxFetchAttempts} max_inflight_per_client=${liveWalletFirstMaxInFlightPerClient} max_inflight_direct=${liveWalletFirstMaxInFlightDirect} direct_fallback_on_last_attempt=${liveWalletFirstDirectFallbackOnLastAttempt} shard=${liveWalletFirstShardIndex}/${liveWalletFirstShardCount} scan_interval_ms=${liveWalletFirstScanIntervalMs} persist_dir=${liveWalletFirstPersistDir} external_shards=${liveWalletFirstExternalShardsEnabled} force_direct=${liveWalletFirstForceDirect} hot_ws_max_wallets=${hotWalletWsMaxWallets} hot_ws_inactivity_ms=${hotWalletWsInactivityMs} hot_ws_trade_hold_ms=${hotWalletWsTradeHoldMs} hot_ws_open_hold_ms=${hotWalletWsOpenPositionHoldMs} hot_ws_aggressive_evict_ms=${hotWalletWsAggressiveEvictMs}`
    );
  } else if (liveTradesUpstreamEnabled) {
    console.log(
      `[wallet-first-live] local scanner disabled; proxying live-trades API to ${liveTradesUpstreamBase}`
    );
  } else if (liveWalletFirstExternalShardsEnabled) {
    console.log(
      `[wallet-first-live] local scanner disabled; reading merged shard snapshots from ${liveWalletFirstPersistDir}`
    );
  }
  function getLiveWalletSnapshot(eventsLimit = 300) {
    if (liveWalletFirstExternalShardsEnabled) {
      const merged = loadMergedShardSnapshot(liveWalletFirstPersistDir, { eventsLimit });
      if (merged && merged.status && merged.status.enabled) return merged;
    }
    return liveWalletPositionsMonitor &&
      typeof liveWalletPositionsMonitor.getSnapshot === "function"
      ? liveWalletPositionsMonitor.getSnapshot({ eventsLimit })
      : { status: { enabled: false }, positions: [], events: [] };
  }
  const DAY_MS = 24 * 60 * 60 * 1000;
  const tokenAnalyticsCache = new Map();
  const tokenAnalyticsInflight = new Map();
  const tokenAnalyticsCacheTtlMs = Math.max(
    3000,
    Number(process.env.PACIFICA_TOKEN_ANALYTICS_CACHE_TTL_MS || 15000)
  );
  const tokenAnalyticsTradesPageSize = Math.max(
    20,
    Math.min(400, Number(process.env.PACIFICA_TOKEN_ANALYTICS_TRADES_PAGE_SIZE || 200))
  );
  const tokenAnalyticsFundingPageSize = Math.max(
    20,
    Math.min(400, Number(process.env.PACIFICA_TOKEN_ANALYTICS_FUNDING_PAGE_SIZE || 200))
  );
  const tokenAnalyticsTradesMaxPages = Math.max(
    1,
    Number(process.env.PACIFICA_TOKEN_ANALYTICS_TRADES_MAX_PAGES || 24)
  );
  const tokenAnalyticsFundingMaxPages = Math.max(
    1,
    Number(process.env.PACIFICA_TOKEN_ANALYTICS_FUNDING_MAX_PAGES || 16)
  );
  const tokenAnalyticsAllLookbackDays = Math.max(
    30,
    Number(process.env.PACIFICA_TOKEN_ANALYTICS_ALL_LOOKBACK_DAYS || 365)
  );
  const tokenAnalyticsTradesCostPerPage = Math.max(
    0.2,
    Number(process.env.PACIFICA_TOKEN_ANALYTICS_TRADES_COST || 1)
  );
  const tokenAnalyticsFundingCostPerPage = Math.max(
    0.2,
    Number(process.env.PACIFICA_TOKEN_ANALYTICS_FUNDING_COST || 1)
  );
  const defaultIndexerStatusUrl =
    String(process.env.PORT || "") === "3200"
      ? "http://127.0.0.1:3201/api/indexer/status"
      : "";
  const indexerStatusUrl = String(
    process.env.PACIFICA_INDEXER_STATUS_PROXY_URL || defaultIndexerStatusUrl
  ).trim();
  let indexerApiOrigin = "";
  try {
    if (indexerStatusUrl) {
      const parsed = new URL(indexerStatusUrl);
      indexerApiOrigin = `${parsed.protocol}//${parsed.host}`;
    }
  } catch (_error) {
    indexerApiOrigin = "";
  }
  const persistedIndexerStatePath = path.join(defaultDataRoot, "indexer", "indexer_state.json");

  function summarizePersistedIndexerErrorReason(message) {
    const msg = String(message || "").toLowerCase();
    if (!msg) return "unknown_error";
    if (msg.includes("curl_request_failed")) {
      if (msg.includes("timed out")) return "timeout";
      if (
        msg.includes("failed to connect") ||
        msg.includes("connection to proxy closed") ||
        msg.includes("can't complete socks5 connection") ||
        msg.includes("recv failure") ||
        msg.includes("connection reset")
      ) {
        return "proxy_error";
      }
      return "network_error";
    }
    if (msg.includes("429")) return "rate_limit_429";
    if (msg.includes("timeout")) return "timeout";
    if (msg.includes("503")) return "service_unavailable_503";
    if (msg.includes("500")) return "server_error_500";
    if (msg.includes("404")) return "not_found_404";
    if (msg.includes("network")) return "network_error";
    if (msg.includes("proxy")) return "proxy_error";
    if (msg.includes("failed to connect")) return "network_error";
    if (msg.includes("econnreset")) return "connection_reset";
    return msg.slice(0, 120);
  }

  function isPersistedBackfillCompleteRow(row) {
    const safe = row || {};
    const hasSuccess =
      Number(safe.lastSuccessAt || 0) > 0 || Number(safe.scansSucceeded || 0) > 0;
    if (!hasSuccess) return false;
    if (!Boolean(safe.tradeDone) || !Boolean(safe.fundingDone)) return false;
    if (safe.tradeCursor) return false;
    if (safe.fundingCursor) return false;
    return true;
  }

  function derivePersistedWalletLifecycle(row) {
    const safe = row || {};
    const explicit = String(safe.lifecycleStage || "").trim();
    const hasSuccess =
      Number(safe.lastSuccessAt || 0) > 0 || Number(safe.scansSucceeded || 0) > 0;
    const hasAttempts =
      hasSuccess ||
      Number(safe.scansFailed || 0) > 0 ||
      Number(safe.lastAttemptAt || safe.lastScannedAt || 0) > 0;
    const complete = isPersistedBackfillCompleteRow(safe);

    if (complete) {
      if (explicit === "fully_indexed" || explicit === "live_tracking") return explicit;
      return safe.liveTrackingSince ? "live_tracking" : "fully_indexed";
    }
    if (hasAttempts) return "backfilling";
    if (explicit === "discovered") return "discovered";
    return "pending_backfill";
  }

  function derivePersistedWalletStatus(row) {
    const safe = row || {};
    const hasSuccess =
      Number(safe.lastSuccessAt || 0) > 0 || Number(safe.scansSucceeded || 0) > 0;
    const hasFailure = Number(safe.lastFailureAt || 0) > 0;
    const lastError = String(safe.lastError || "").trim();
    const lifecycle = derivePersistedWalletLifecycle(safe);

    if (!hasSuccess) {
      return lastError ? "failed" : "pending";
    }
    if (hasFailure && Number(safe.lastFailureAt || 0) >= Number(safe.lastSuccessAt || 0)) {
      return "failed";
    }
    if (lastError && Number(safe.consecutiveFailures || 0) > 0) {
      return "failed";
    }
    if (!isPersistedBackfillCompleteRow(safe) || lifecycle === "backfilling") {
      return "partial";
    }
    return "indexed";
  }

  function buildPersistedIndexerStatus() {
    const rawState = readJson(persistedIndexerStatePath, null);
    if (!rawState || typeof rawState !== "object") return null;

    const rows = Object.values(rawState.walletStates || {});
    const knownWallets = Array.isArray(rawState.knownWallets) ? rawState.knownWallets.length : rows.length;
    const liveWallets = Array.isArray(rawState.liveWallets) ? rawState.liveWallets.length : 0;
    const priorityQueueSize = Array.isArray(rawState.priorityQueue) ? rawState.priorityQueue.length : 0;
    const now = Date.now();
    const liveRefreshTargetMs = Math.max(
      60_000,
      Number(process.env.PACIFICA_LIVE_REFRESH_TARGET_MS || 10 * 60 * 1000)
    );

    let indexed = 0;
    let partial = 0;
    let pending = 0;
    let failed = 0;
    let failedBackfill = 0;
    let pendingBackfill = 0;
    let backfilling = 0;
    let fullyIndexed = 0;
    let liveTracking = 0;
    let attemptedWallets = 0;
    let successfulScans = 0;
    let failedScans = 0;
    let pendingWaitSum = 0;
    let pendingWaitCount = 0;
    let liveAgeSum = 0;
    let liveAgeCount = 0;
    let liveMaxAgeMs = 0;
    let liveStaleWallets = 0;
    const reasonCounts = new Map();

    rows.forEach((row) => {
      const status = derivePersistedWalletStatus(row);
      const lifecycle = derivePersistedWalletLifecycle(row);
      if (status === "indexed") indexed += 1;
      else if (status === "partial") partial += 1;
      else if (status === "failed") {
        failed += 1;
        if (lifecycle !== "live_tracking") failedBackfill += 1;
      } else pending += 1;

      if (lifecycle === "discovered" || lifecycle === "pending_backfill") pendingBackfill += 1;
      else if (lifecycle === "backfilling") backfilling += 1;
      else if (lifecycle === "fully_indexed") fullyIndexed += 1;
      else if (lifecycle === "live_tracking") {
        liveTracking += 1;
        const anchor = Number(
          row &&
            (row.liveLastScanAt ||
              row.lastSuccessAt ||
              row.liveTrackingSince ||
              row.backfillCompletedAt ||
              row.discoveredAt ||
              0)
        );
        if (anchor > 0) {
          const age = Math.max(0, now - anchor);
          liveAgeSum += age;
          liveAgeCount += 1;
          if (age > liveMaxAgeMs) liveMaxAgeMs = age;
          if (age >= liveRefreshTargetMs) liveStaleWallets += 1;
        } else {
          liveStaleWallets += 1;
        }
      }

      const rowScansSucceeded = Number(row && row.scansSucceeded ? row.scansSucceeded : 0);
      const rowScansFailed = Number(row && row.scansFailed ? row.scansFailed : 0);
      const hasLegacyAttempt = Boolean(
        row &&
          (row.lastAttemptAt ||
            row.lastScannedAt ||
            row.lastSuccessAt ||
            row.lastFailureAt ||
            row.lastError)
      );
      if (rowScansSucceeded + rowScansFailed > 0 || hasLegacyAttempt) attemptedWallets += 1;
      successfulScans += rowScansSucceeded;
      failedScans += rowScansFailed > 0 ? rowScansFailed : row && row.lastError ? 1 : 0;

      if (status === "partial" || status === "pending" || status === "failed") {
        const discoveredAt = Number(row && row.discoveredAt ? row.discoveredAt : 0);
        if (discoveredAt > 0) {
          pendingWaitSum += Math.max(0, now - discoveredAt);
          pendingWaitCount += 1;
        }
      }

      if (status === "failed") {
        const reason =
          row && row.lastErrorReason
            ? String(row.lastErrorReason)
            : summarizePersistedIndexerErrorReason(row && row.lastError ? row.lastError : "");
        reasonCounts.set(reason, (reasonCounts.get(reason) || 0) + 1);
      }
    });

    const backfillComplete = fullyIndexed + liveTracking;
    const completionPct = knownWallets > 0 ? (backfillComplete / knownWallets) * 100 : 0;

    return {
      running: true,
      knownWallets,
      completionPct: Number(completionPct.toFixed(4)),
      attemptedWallets,
      successfulScans,
      failedScans,
      indexedCompleteWallets: indexed,
      partiallyIndexedWallets: partial,
      pendingWallets: pending,
      failedWallets: failed,
      walletBacklog: pendingBackfill + backfilling + failedBackfill,
      failedBackfillWallets: failedBackfill,
      lifecycle: {
        discovered: rows.length,
        pendingBackfill,
        backfilling,
        failedBackfill,
        fullyIndexed,
        liveTracking,
        backfillComplete,
      },
      lastDiscoveryAt: rawState.lastDiscoveryAt || null,
      lastScanAt: rawState.lastScanAt || null,
      discoveryCycles: Number(rawState.discoveryCycles || 0),
      scanCycles: Number(rawState.scanCycles || 0),
      priorityQueueSize,
      liveQueueSize: liveWallets || liveTracking,
      liveGroupSize: liveWallets || liveTracking,
      averagePendingWaitMs:
        pendingWaitCount > 0 ? Math.round(pendingWaitSum / pendingWaitCount) : 0,
      averageQueueWaitMs: 0,
      liveAverageAgeMs: liveAgeCount > 0 ? Math.round(liveAgeSum / liveAgeCount) : 0,
      liveMaxAgeMs,
      liveStaleWallets,
      topErrorReasons: Array.from(reasonCounts.entries())
        .map(([reason, count]) => ({ reason, count }))
        .sort((a, b) => b.count - a.count)
        .slice(0, 10),
      source: {
        type: "persisted_state",
        statePath: persistedIndexerStatePath,
        cachedAt: now,
      },
    };
  }

  function syncWalletStoreFromDisk() {
    if (!walletStore || typeof walletStore.load !== "function") return;
    // When an in-process indexer is present, the in-memory wallet store is the source of truth.
    // Reloading from disk here can overwrite fresh unflushed updates from live scans.
    if (walletIndexer && typeof walletIndexer.getStatus === "function") return;
    const now = Date.now();
    if (now - walletStoreRefresh.lastAt < walletStoreRefresh.minIntervalMs) return;
    walletStore.load();
    walletStoreRefresh.lastAt = now;
  }

  async function getFreshIndexerStatus() {
    const localStatus =
      walletIndexer && typeof walletIndexer.getStatus === "function"
        ? walletIndexer.getStatus()
        : null;
    if (localStatus && localStatus.running) return localStatus;
    if (!indexerStatusUrl) return localStatus;

    const now = Date.now();
    if (
      now - Number(indexerStatusCache.fetchedAt || 0) <= Number(indexerStatusCache.ttlMs || 0) &&
      indexerStatusCache.value
    ) {
      return indexerStatusCache.value;
    }

    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 4000);
      const res = await fetch(indexerStatusUrl, {
        method: "GET",
        signal: controller.signal,
      });
      clearTimeout(timeout);
      if (!res.ok) return localStatus;
      const payload = await res.json();
      if (payload && payload.status && typeof payload.status === "object") {
        indexerStatusCache.fetchedAt = now;
        indexerStatusCache.value = payload.status;
        return payload.status;
      }
      const persistedStatus = buildPersistedIndexerStatus();
      if (persistedStatus) {
        indexerStatusCache.fetchedAt = now;
        indexerStatusCache.value = persistedStatus;
      }
      return indexerStatusCache.value || localStatus || persistedStatus;
    } catch (_error) {
      const persistedStatus = buildPersistedIndexerStatus();
      if (persistedStatus) {
        indexerStatusCache.fetchedAt = now;
        indexerStatusCache.value = persistedStatus;
      }
      return indexerStatusCache.value || localStatus || persistedStatus;
    }
  }

  async function getLivePositionPriceLookup() {
    const now = Date.now();
    if (
      livePositionPriceCache.value instanceof Map &&
      livePositionPriceCache.value.size > 0 &&
      now - Number(livePositionPriceCache.fetchedAt || 0) <= Number(livePositionPriceCache.ttlMs || 0)
    ) {
      return livePositionPriceCache.value;
    }

    if (livePositionPriceCache.inflight) {
      try {
        return await livePositionPriceCache.inflight;
      } catch (_error) {
        return livePositionPriceCache.value instanceof Map
          ? livePositionPriceCache.value
          : new Map();
      }
    }

    const fetcher = async () => {
      if (!restClient || typeof restClient.get !== "function") {
        return livePositionPriceCache.value instanceof Map
          ? livePositionPriceCache.value
          : new Map();
      }
      try {
        const response = await restClient.get("/info/prices", {
          cost: 1,
          timeoutMs: 5000,
          retryMaxAttempts: 1,
        });
        const payload =
          response && response.payload && typeof response.payload === "object"
            ? response.payload
            : {};
        const rows = Array.isArray(payload.data) ? payload.data : [];
        const next = buildMarketPriceLookup(
          rows.reduce((acc, row) => {
            const symbol = String((row && row.symbol) || "").trim().toUpperCase();
            if (symbol) acc[symbol] = row;
            return acc;
          }, {})
        );
        if (next.size > 0) {
          livePositionPriceCache.value = next;
          livePositionPriceCache.fetchedAt = Date.now();
          livePositionPriceCache.stale = false;
          livePositionPriceCache.lastError = null;
        }
        return livePositionPriceCache.value;
      } catch (error) {
        livePositionPriceCache.stale = true;
        livePositionPriceCache.lastError = error && error.message ? error.message : "price_fetch_failed";
        return livePositionPriceCache.value instanceof Map
          ? livePositionPriceCache.value
          : new Map();
      } finally {
        livePositionPriceCache.inflight = null;
      }
    };

    livePositionPriceCache.inflight = fetcher();
    return livePositionPriceCache.inflight;
  }

  function parseWalletListParam(value) {
    return String(value || "")
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean);
  }

  function buildLiveTradesUpstreamUrl(url) {
    if (!liveTradesUpstreamEnabled) return null;
    if (!url || !url.pathname) return null;
    return `${liveTradesUpstreamBase}${url.pathname}${url.search || ""}`;
  }

  async function proxyLiveTradesJson(req, res, url) {
    const target = buildLiveTradesUpstreamUrl(url);
    if (!target) return false;
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 20000);
      const upstreamRes = await fetch(target, {
        method: "GET",
        signal: controller.signal,
      });
      clearTimeout(timeout);
      const text = await upstreamRes.text();
      const contentType =
        upstreamRes.headers.get("content-type") || "application/json; charset=utf-8";
      res.writeHead(Number(upstreamRes.status || 502), {
        "Content-Type": contentType,
        "Cache-Control": "no-store",
      });
      res.end(text);
    } catch (error) {
      sendJson(res, 502, {
        ok: false,
        error: `live_trades_upstream_unavailable: ${error.message || "request_failed"}`,
        upstream: target,
      });
    }
    return true;
  }

  function proxyLiveTradesStream(req, res, url) {
    const targetText = buildLiveTradesUpstreamUrl(url);
    if (!targetText) return false;
    let target = null;
    try {
      target = new URL(targetText);
    } catch (_error) {
      sendJson(res, 502, {
        ok: false,
        error: "live_trades_stream_upstream_invalid_url",
      });
      return true;
    }

    const client = target.protocol === "https:" ? https : http;
    const upstreamReq = client.request(
      {
        protocol: target.protocol,
        hostname: target.hostname,
        port: target.port || (target.protocol === "https:" ? 443 : 80),
        path: `${target.pathname}${target.search || ""}`,
        method: "GET",
        headers: {
          Accept: "text/event-stream",
          "Cache-Control": "no-cache",
          Connection: "keep-alive",
        },
      },
      (upstreamRes) => {
        const statusCode = Number(upstreamRes.statusCode || 502);
        const contentType =
          upstreamRes.headers["content-type"] || "text/event-stream; charset=utf-8";
        res.writeHead(statusCode, {
          "Content-Type": contentType,
          "Cache-Control": "no-cache, no-transform",
          Connection: "keep-alive",
          "X-Accel-Buffering": "no",
        });
        upstreamRes.pipe(res);

        const cleanup = () => {
          try {
            upstreamReq.destroy();
          } catch (_error) {
            // ignore
          }
          try {
            upstreamRes.destroy();
          } catch (_error) {
            // ignore
          }
        };
        req.on("close", cleanup);
        req.on("aborted", cleanup);
        res.on("close", cleanup);
        res.on("error", cleanup);
      }
    );

    upstreamReq.setTimeout(20000, () => {
      upstreamReq.destroy(new Error("upstream_timeout"));
    });

    upstreamReq.on("error", (error) => {
      if (!res.headersSent) {
        sendJson(res, 502, {
          ok: false,
          error: `live_trades_stream_upstream_unavailable: ${error.message || "request_failed"}`,
          upstream: targetText,
        });
      } else {
        try {
          res.end();
        } catch (_error) {
          // ignore
        }
      }
    });

    upstreamReq.end();
    return true;
  }

  if (liveTradeWalletAttributionEnabled) {
    const timer = setInterval(() => {
      try {
        runLiveTradeWalletAttributionPass();
      } catch (_error) {
        // Ignore attribution background-pass errors; live endpoint must stay available.
      }
    }, Math.max(1000, liveTradeWalletAttributionScanIntervalMs));
    if (timer && typeof timer.unref === "function") {
      timer.unref();
    }
  }
  if (liveTradeTxAttributionEnabled) {
    const timer = setInterval(() => {
      runLiveTradeTxAttributionPass().catch(() => {});
    }, Math.max(2000, liveTradeTxAttributionScanIntervalMs));
    if (timer && typeof timer.unref === "function") {
      timer.unref();
    }
  }
  if (liveWalletPositionsMonitor && typeof liveWalletPositionsMonitor.start === "function") {
    liveWalletPositionsMonitor.start();
  }

  function trimLiveTradeWalletAttributionCache() {
    const caches = [
      liveTradeWalletAttribution.historyToWallet,
      liveTradeWalletAttribution.orderToWallet,
      liveTradeWalletAttribution.liToWallet,
    ];
    caches.forEach((cache) => {
      if (cache.size <= liveTradeWalletAttributionCacheMax) return;
      const removeCount = cache.size - liveTradeWalletAttributionCacheMax;
      let removed = 0;
      for (const key of cache.keys()) {
        cache.delete(key);
        removed += 1;
        if (removed >= removeCount) break;
      }
    });
  }

  function trimLiveTradeTxAttributionCache() {
    const caches = [
      liveTradeTxAttribution.historyToTx,
      liveTradeTxAttribution.orderToTx,
      liveTradeTxAttribution.liToTx,
    ];
    caches.forEach((cache) => {
      if (cache.size <= liveTradeTxAttributionCacheMax) return;
      const removeCount = cache.size - liveTradeTxAttributionCacheMax;
      let removed = 0;
      for (const key of cache.keys()) {
        cache.delete(key);
        removed += 1;
        if (removed >= removeCount) break;
      }
    });
  }

  function trimPendingIdsSet(pendingSet) {
    if (!pendingSet || typeof pendingSet.size !== "number") return;
    if (pendingSet.size <= liveTradeWalletAttributionPendingMax) return;
    const removeCount = pendingSet.size - liveTradeWalletAttributionPendingMax;
    let removed = 0;
    for (const key of pendingSet.keys()) {
      pendingSet.delete(key);
      removed += 1;
      if (removed >= removeCount) break;
    }
  }

  function trimPendingLiveTradeHistoryIds() {
    trimPendingIdsSet(liveTradeWalletAttribution.pendingHistoryIds);
    trimPendingIdsSet(liveTradeWalletAttribution.pendingOrderIds);
    trimPendingIdsSet(liveTradeWalletAttribution.pendingLiIds);
    trimPendingIdsSet(liveTradeTxAttribution.pendingHistoryIds);
    trimPendingIdsSet(liveTradeTxAttribution.pendingOrderIds);
    trimPendingIdsSet(liveTradeTxAttribution.pendingLiIds);
  }

  function queueLiveTradeHistoryIdForAttribution(historyId, li = null, orderId = null) {
    if (!liveTradeWalletAttributionEnabled) return false;
    let added = false;
    const key = normalizeHistoryIdKey(historyId);
    if (
      key &&
      !liveTradeWalletAttribution.historyToWallet.has(key) &&
      !liveTradeWalletAttribution.pendingHistoryIds.has(key)
    ) {
      liveTradeWalletAttribution.pendingHistoryIds.add(key);
      added = true;
    }
    const orderKey = normalizeHistoryIdKey(orderId);
    if (
      orderKey &&
      !liveTradeWalletAttribution.orderToWallet.has(orderKey) &&
      !liveTradeWalletAttribution.pendingOrderIds.has(orderKey)
    ) {
      liveTradeWalletAttribution.pendingOrderIds.add(orderKey);
      added = true;
    }
    const liKey = normalizeHistoryIdKey(li);
    if (
      liKey &&
      !liveTradeWalletAttribution.liToWallet.has(liKey) &&
      !liveTradeWalletAttribution.pendingLiIds.has(liKey)
    ) {
      liveTradeWalletAttribution.pendingLiIds.add(liKey);
      added = true;
    }
    if (liveTradeTxAttributionEnabled) {
      if (
        key &&
        !liveTradeTxAttribution.historyToTx.has(key) &&
        !liveTradeTxAttribution.pendingHistoryIds.has(key)
      ) {
        liveTradeTxAttribution.pendingHistoryIds.add(key);
        added = true;
      }
      if (
        orderKey &&
        !liveTradeTxAttribution.orderToTx.has(orderKey) &&
        !liveTradeTxAttribution.pendingOrderIds.has(orderKey)
      ) {
        liveTradeTxAttribution.pendingOrderIds.add(orderKey);
        added = true;
      }
      if (
        liKey &&
        !liveTradeTxAttribution.liToTx.has(liKey) &&
        !liveTradeTxAttribution.pendingLiIds.has(liKey)
      ) {
        liveTradeTxAttribution.pendingLiIds.add(liKey);
        added = true;
      }
    }
    trimPendingLiveTradeHistoryIds();
    return added;
  }

  function getAttributedWalletByHistoryId(historyId) {
    const key = normalizeHistoryIdKey(historyId);
    if (!key) return null;
    const value = liveTradeWalletAttribution.historyToWallet.get(key);
    const wallet = value === null || value === undefined ? "" : String(value).trim();
    return wallet || null;
  }

  function getAttributedWalletByLi(li) {
    const key = normalizeHistoryIdKey(li);
    if (!key) return null;
    const value = liveTradeWalletAttribution.liToWallet.get(key);
    const wallet = value === null || value === undefined ? "" : String(value).trim();
    return wallet || null;
  }

  function getAttributedWalletByOrderId(orderId) {
    const key = normalizeHistoryIdKey(orderId);
    if (!key) return null;
    const value = liveTradeWalletAttribution.orderToWallet.get(key);
    const wallet = value === null || value === undefined ? "" : String(value).trim();
    return wallet || null;
  }

  function rememberWalletAttribution(historyId, wallet) {
    const key = normalizeHistoryIdKey(historyId);
    const normalizedWallet = String(wallet || "").trim();
    if (!key || !normalizedWallet) return;
    const cache = liveTradeWalletAttribution.historyToWallet;
    if (cache.has(key)) {
      cache.delete(key);
    }
    cache.set(key, normalizedWallet);
    if (liveTradeAttributionStore) {
      liveTradeAttributionStore.upsertWalletLink("history", key, normalizedWallet, {
        walletSource: "wallet_history_attribution_history_id",
        walletConfidence: "hard_history_id",
      });
    }
    trimLiveTradeWalletAttributionCache();
  }

  function rememberLiWalletAttribution(li, wallet) {
    const key = normalizeHistoryIdKey(li);
    const normalizedWallet = String(wallet || "").trim();
    if (!key || !normalizedWallet) return;
    const cache = liveTradeWalletAttribution.liToWallet;
    if (cache.has(key)) {
      cache.delete(key);
    }
    cache.set(key, normalizedWallet);
    if (liveTradeAttributionStore) {
      liveTradeAttributionStore.upsertWalletLink("li", key, normalizedWallet, {
        walletSource: "wallet_history_attribution_li",
        walletConfidence: "fallback_li",
      });
    }
    trimLiveTradeWalletAttributionCache();
  }

  function rememberOrderWalletAttribution(orderId, wallet) {
    const key = normalizeHistoryIdKey(orderId);
    const normalizedWallet = String(wallet || "").trim();
    if (!key || !normalizedWallet) return;
    const cache = liveTradeWalletAttribution.orderToWallet;
    if (cache.has(key)) {
      cache.delete(key);
    }
    cache.set(key, normalizedWallet);
    if (liveTradeAttributionStore) {
      liveTradeAttributionStore.upsertWalletLink("order", key, normalizedWallet, {
        walletSource: "wallet_history_attribution_order_id",
        walletConfidence: "hard_order_id",
      });
    }
    trimLiveTradeWalletAttributionCache();
  }

  function normalizeTxSignature(signature) {
    const raw = String(signature || "").trim();
    return raw || "";
  }

  function rememberLiveTradeTxAttribution(kind, keyValue, txMeta) {
    const key = normalizeHistoryIdKey(keyValue);
    const signature = normalizeTxSignature(txMeta && txMeta.signature);
    if (!key || !signature) return;
    const signer = String((txMeta && txMeta.signer) || "").trim() || null;
    const observedAt = Number((txMeta && txMeta.observedAt) || Date.now());
    const source = String((txMeta && txMeta.source) || "solana_program_event").trim();
    const confidence = String((txMeta && txMeta.confidence) || "soft").trim();
    const value = {
      signature,
      signer,
      source,
      confidence,
      observedAt,
    };
    const map =
      kind === "history"
        ? liveTradeTxAttribution.historyToTx
        : kind === "order"
        ? liveTradeTxAttribution.orderToTx
        : liveTradeTxAttribution.liToTx;
    if (map.has(key)) map.delete(key);
    map.set(key, value);
    if (liveTradeAttributionStore) {
      liveTradeAttributionStore.upsertTxLink(kind, key, {
        signature,
        signer,
        source,
        confidence,
      });
    }
    trimLiveTradeTxAttributionCache();
  }

  function getLiveTradeTxAttributionByHistoryId(historyId) {
    const key = normalizeHistoryIdKey(historyId);
    if (!key) return null;
    return liveTradeTxAttribution.historyToTx.get(key) || null;
  }

  function getLiveTradeTxAttributionByOrderId(orderId) {
    const key = normalizeHistoryIdKey(orderId);
    if (!key) return null;
    return liveTradeTxAttribution.orderToTx.get(key) || null;
  }

  function getLiveTradeTxAttributionByLi(li) {
    const key = normalizeHistoryIdKey(li);
    if (!key) return null;
    return liveTradeTxAttribution.liToTx.get(key) || null;
  }

  function parseU64CandidatesFromProgramDataLog(logMessage) {
    const marker = "Program data:";
    const idx = String(logMessage || "").indexOf(marker);
    if (idx < 0) return [];
    const encoded = String(logMessage || "")
      .slice(idx + marker.length)
      .trim();
    if (!encoded) return [];
    let buf = null;
    try {
      buf = Buffer.from(encoded, "base64");
    } catch (_error) {
      return [];
    }
    if (!buf || !buf.length) return [];
    const out = [];
    for (let offset = 0; offset + 8 <= buf.length; offset += 8) {
      try {
        const value = buf.readBigUInt64LE(offset);
        if (value <= 0n) continue;
        out.push(value.toString());
      } catch (_error) {
        // ignore parse errors
      }
    }
    return out;
  }

  function extractSignerFromRpcTransaction(txResult) {
    const message =
      txResult &&
      txResult.transaction &&
      txResult.transaction.message &&
      typeof txResult.transaction.message === "object"
        ? txResult.transaction.message
        : null;
    if (!message) return null;
    const keys = Array.isArray(message.accountKeys) ? message.accountKeys : [];
    for (const entry of keys) {
      if (!entry) continue;
      if (typeof entry === "string") {
        return String(entry || "").trim() || null;
      }
      const signer = Boolean(entry.signer);
      const pubkey = String(entry.pubkey || "").trim();
      if (signer && pubkey) return pubkey;
    }
    const first = keys[0];
    if (typeof first === "string") return String(first || "").trim() || null;
    if (first && typeof first === "object") {
      const pubkey = String(first.pubkey || "").trim();
      return pubkey || null;
    }
    return null;
  }

  async function rpcCall(method, params = []) {
    if (!solanaRpcUrl) return null;
    const body = {
      jsonrpc: "2.0",
      id: Math.floor(Math.random() * 1e9),
      method,
      params,
    };
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 8000);
    try {
      const res = await fetch(solanaRpcUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
        signal: controller.signal,
      });
      if (!res.ok) return null;
      const payload = await res.json();
      if (payload && payload.error) return null;
      return payload && Object.prototype.hasOwnProperty.call(payload, "result")
        ? payload.result
        : null;
    } catch (_error) {
      return null;
    } finally {
      clearTimeout(timeout);
    }
  }

  function rememberSeenLiveTradeSignature(signature) {
    const normalized = normalizeTxSignature(signature);
    if (!normalized) return false;
    if (liveTradeTxAttribution.seenSignatures.has(normalized)) return false;
    liveTradeTxAttribution.seenSignatures.add(normalized);
    liveTradeTxAttribution.seenSignatureQueue.push(normalized);
    if (liveTradeTxAttribution.seenSignatureQueue.length > liveTradeTxAttributionSeenMax) {
      const removeCount = liveTradeTxAttribution.seenSignatureQueue.length - liveTradeTxAttributionSeenMax;
      for (let i = 0; i < removeCount; i += 1) {
        const old = liveTradeTxAttribution.seenSignatureQueue.shift();
        if (old) liveTradeTxAttribution.seenSignatures.delete(old);
      }
    }
    return true;
  }

  function tryResolveLiveTradeTxByCandidates(candidates = [], txMeta = {}) {
    const unique = Array.from(new Set((Array.isArray(candidates) ? candidates : []).map((v) => normalizeHistoryIdKey(v)).filter(Boolean)));
    if (!unique.length) return;
    unique.forEach((candidate) => {
      if (liveTradeTxAttribution.pendingHistoryIds.has(candidate)) {
        rememberLiveTradeTxAttribution("history", candidate, txMeta);
        liveTradeTxAttribution.pendingHistoryIds.delete(candidate);
        liveTradeTxAttribution.txMatchedByHistory += 1;
      }
      if (liveTradeTxAttribution.pendingOrderIds.has(candidate)) {
        rememberLiveTradeTxAttribution("order", candidate, txMeta);
        liveTradeTxAttribution.pendingOrderIds.delete(candidate);
        liveTradeTxAttribution.txMatchedByOrder += 1;
      }
      if (liveTradeTxAttribution.pendingLiIds.has(candidate)) {
        rememberLiveTradeTxAttribution("li", candidate, txMeta);
        liveTradeTxAttribution.pendingLiIds.delete(candidate);
        liveTradeTxAttribution.txMatchedByLi += 1;
      }
    });
  }

  function parseCandidatesFromRpcTransaction(txResult) {
    const out = new Set();
    const meta = txResult && txResult.meta && typeof txResult.meta === "object" ? txResult.meta : null;
    const logs = meta && Array.isArray(meta.logMessages) ? meta.logMessages : [];
    logs.forEach((logMessage) => {
      const candidates = parseU64CandidatesFromProgramDataLog(logMessage);
      candidates.forEach((candidate) => out.add(candidate));
      if (typeof logMessage === "string") {
        const numeric = logMessage.match(/\b\d{6,20}\b/g);
        if (Array.isArray(numeric)) {
          numeric.forEach((token) => out.add(String(token)));
        }
      }
    });
    return Array.from(out.values());
  }

  async function runLiveTradeTxAttributionPass() {
    if (!liveTradeTxAttributionEnabled) return;
    if (!solanaRpcUrl || !pacificaProgramId) return;
    if (liveTradeTxAttribution.running) return;
    if (
      !liveTradeTxAttribution.pendingHistoryIds.size &&
      !liveTradeTxAttribution.pendingOrderIds.size &&
      !liveTradeTxAttribution.pendingLiIds.size
    ) {
      return;
    }
    const now = Date.now();
    if (now - Number(liveTradeTxAttribution.lastScanAt || 0) < liveTradeTxAttributionScanIntervalMs) {
      return;
    }
    liveTradeTxAttribution.running = true;
    liveTradeTxAttribution.lastScanAt = now;
    try {
      const sigRows = await rpcCall("getSignaturesForAddress", [
        pacificaProgramId,
        { limit: liveTradeTxAttributionRpcLimit, commitment: "confirmed" },
      ]);
      liveTradeTxAttribution.rpcCalls += 1;
      const signatureRows = Array.isArray(sigRows) ? sigRows : [];
      const freshSignatures = [];
      for (const row of signatureRows) {
        const signature = normalizeTxSignature(row && row.signature);
        if (!signature) continue;
        if (!rememberSeenLiveTradeSignature(signature)) continue;
        freshSignatures.push(signature);
        if (freshSignatures.length >= liveTradeTxAttributionFetchMaxPerPass) break;
      }
      for (const signature of freshSignatures) {
        const txResult = await rpcCall("getTransaction", [
          signature,
          {
            encoding: "jsonParsed",
            commitment: "confirmed",
            maxSupportedTransactionVersion: 0,
          },
        ]);
        liveTradeTxAttribution.rpcCalls += 1;
        if (!txResult || typeof txResult !== "object") continue;
        liveTradeTxAttribution.txFetched += 1;
        const signer = extractSignerFromRpcTransaction(txResult);
        const candidates = parseCandidatesFromRpcTransaction(txResult);
        if (!candidates.length) continue;
        tryResolveLiveTradeTxByCandidates(candidates, {
          signature,
          signer,
          source: "solana_rpc_program_log",
          confidence: "soft_onchain_log_match",
          observedAt: Date.now(),
        });
      }
      liveTradeTxAttribution.scanPasses += 1;
    } catch (_error) {
      liveTradeTxAttribution.scanPasses += 1;
    } finally {
      liveTradeTxAttribution.running = false;
    }
  }

  function refreshLiveTradeWalletAttributionFileList(force = false) {
    if (!liveTradeWalletAttributionEnabled) return;
    const now = Date.now();
    if (
      !force &&
      liveTradeWalletAttribution.files.length > 0 &&
      now - Number(liveTradeWalletAttribution.filesLoadedAt || 0) <=
        liveTradeWalletAttributionFileListTtlMs
    ) {
      return;
    }

    try {
      if (!liveTradeWalletHistoryDir || !fs.existsSync(liveTradeWalletHistoryDir)) {
        liveTradeWalletAttribution.files = [];
        liveTradeWalletAttribution.fileCursor = 0;
        liveTradeWalletAttribution.filesLoadedAt = now;
        return;
      }

      const allNames = fs
        .readdirSync(liveTradeWalletHistoryDir)
        .filter((name) => String(name || "").toLowerCase().endsWith(".json"));
      const allNameSet = new Set(allNames);
      const prioritized = [];

      if (walletStore && typeof walletStore.list === "function") {
        const walletRows = walletStore.list();
        if (Array.isArray(walletRows) && walletRows.length) {
          const rankByFreshness = walletRows
            .slice()
            .sort((a, b) => {
              const aScore = Number(
                a && (a.liveScannedAt || a.updatedAt || (a.all && a.all.lastTrade) || 0)
              );
              const bScore = Number(
                b && (b.liveScannedAt || b.updatedAt || (b.all && b.all.lastTrade) || 0)
              );
              return bScore - aScore;
            })
            .slice(0, liveTradeWalletAttributionPriorityWallets);
          for (const row of rankByFreshness) {
            const wallet = String(row && row.wallet ? row.wallet : "").trim();
            if (!wallet) continue;
            const fileName = `${wallet}.json`;
            if (!allNameSet.has(fileName)) continue;
            prioritized.push(fileName);
          }
        }
      }

      const seen = new Set();
      const orderedNames = [];
      for (const name of prioritized) {
        if (seen.has(name)) continue;
        seen.add(name);
        orderedNames.push(name);
      }
      for (const name of allNames) {
        if (seen.has(name)) continue;
        seen.add(name);
        orderedNames.push(name);
      }
      const files = orderedNames.map((name) => ({
        name,
        path: path.join(liveTradeWalletHistoryDir, name),
      }));

      const previousCursor = Number(liveTradeWalletAttribution.fileCursor || 0);
      const previousCount = Number(liveTradeWalletAttribution.files.length || 0);
      let nextCursor = 0;
      if (previousCount > 0 && files.length > 0) {
        const ratio = Math.max(0, Math.min(1, previousCursor / previousCount));
        nextCursor = Math.min(files.length - 1, Math.floor(ratio * files.length));
      }

      liveTradeWalletAttribution.files = files;
      liveTradeWalletAttribution.fileCursor = nextCursor;
      liveTradeWalletAttribution.filesLoadedAt = now;
    } catch (_error) {
      liveTradeWalletAttribution.files = [];
      liveTradeWalletAttribution.fileCursor = 0;
      liveTradeWalletAttribution.filesLoadedAt = now;
    }
  }

  function runLiveTradeWalletAttributionPass(options = {}) {
    if (!liveTradeWalletAttributionEnabled) return;
    if (
      !liveTradeWalletAttribution.pendingHistoryIds.size &&
      !liveTradeWalletAttribution.pendingOrderIds.size &&
      !liveTradeWalletAttribution.pendingLiIds.size
    ) {
      return;
    }

    const now = Date.now();
    const force = Boolean(options.force);
    if (
      !force &&
      now - Number(liveTradeWalletAttribution.lastScanAt || 0) < liveTradeWalletAttributionScanIntervalMs
    ) {
      return;
    }

    refreshLiveTradeWalletAttributionFileList(force);
    const files = liveTradeWalletAttribution.files;
    if (!files.length) {
      liveTradeWalletAttribution.lastScanAt = now;
      return;
    }

    const maxFiles = Math.max(
      1,
      Number(
        options.maxFiles ||
          (liveTradeWalletAttribution.historyToWallet.size <= 0
            ? liveTradeWalletAttributionInitialScanFiles
            : liveTradeWalletAttributionScanFilesPerPass)
      )
    );

    let processedFiles = 0;
    const pendingHistory = liveTradeWalletAttribution.pendingHistoryIds;
    const pendingOrder = liveTradeWalletAttribution.pendingOrderIds;
    const pendingLi = liveTradeWalletAttribution.pendingLiIds;

    while (
      processedFiles < maxFiles &&
      (pendingHistory.size > 0 || pendingOrder.size > 0 || pendingLi.size > 0) &&
      files.length > 0
    ) {
      const index = liveTradeWalletAttribution.fileCursor % files.length;
      const file = files[index];
      liveTradeWalletAttribution.fileCursor =
        (liveTradeWalletAttribution.fileCursor + 1) % files.length;
      processedFiles += 1;

      const payload = readJson(file.path, null);
      if (!payload || typeof payload !== "object") continue;
      const wallet = String(payload.wallet || file.name.replace(/\.json$/i, "") || "").trim();
      if (!wallet) continue;
      const trades = Array.isArray(payload.trades) ? payload.trades : [];
      for (const trade of trades) {
        const txSignature =
          (trade &&
            (trade.txSignature ||
              trade.tx_signature ||
              trade.signature ||
              trade.txid ||
              trade.txId ||
              trade.transactionId ||
              trade.transaction_id)) ||
          null;
        const historyIdKey = normalizeHistoryIdKey(trade && trade.historyId);
        if (historyIdKey && pendingHistory.has(historyIdKey)) {
          rememberWalletAttribution(historyIdKey, wallet);
          pendingHistory.delete(historyIdKey);
          liveTradeWalletAttribution.resolved += 1;
          if (txSignature) {
            rememberLiveTradeTxAttribution("history", historyIdKey, {
              signature: txSignature,
              signer: wallet,
              source: "wallet_history",
              confidence: "hard_wallet_history",
            });
            liveTradeTxAttribution.pendingHistoryIds.delete(historyIdKey);
          }
        }
        const orderCandidates = [trade && trade.orderId];
        for (const candidate of orderCandidates) {
          const orderKey = normalizeHistoryIdKey(candidate);
          if (!orderKey || !pendingOrder.has(orderKey)) continue;
          rememberOrderWalletAttribution(orderKey, wallet);
          pendingOrder.delete(orderKey);
          liveTradeWalletAttribution.resolvedByOrder += 1;
          if (txSignature) {
            rememberLiveTradeTxAttribution("order", orderKey, {
              signature: txSignature,
              signer: wallet,
              source: "wallet_history",
              confidence: "hard_wallet_history",
            });
            liveTradeTxAttribution.pendingOrderIds.delete(orderKey);
          }
        }
        const liCandidates = [trade && trade.lastOrderId, trade && trade.li];
        for (const candidate of liCandidates) {
          const liKey = normalizeHistoryIdKey(candidate);
          if (!liKey || !pendingLi.has(liKey)) continue;
          rememberLiWalletAttribution(liKey, wallet);
          pendingLi.delete(liKey);
          liveTradeWalletAttribution.resolvedByLi += 1;
          if (txSignature) {
            rememberLiveTradeTxAttribution("li", liKey, {
              signature: txSignature,
              signer: wallet,
              source: "wallet_history",
              confidence: "hard_wallet_history",
            });
            liveTradeTxAttribution.pendingLiIds.delete(liKey);
          }
        }
        if (pendingHistory.size <= 0 && pendingOrder.size <= 0 && pendingLi.size <= 0) break;
      }
    }

    liveTradeWalletAttribution.scannedFiles += processedFiles;
    liveTradeWalletAttribution.scanPasses += 1;
    liveTradeWalletAttribution.lastScanAt = now;
  }

  async function fetchIndexerApi(pathname, params = {}, timeoutMs = 4000) {
    if (!indexerApiOrigin) return null;
    try {
      const endpoint = new URL(pathname, indexerApiOrigin);
      Object.entries(params || {}).forEach(([key, value]) => {
        if (value === null || value === undefined) return;
        endpoint.searchParams.set(key, String(value));
      });
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), timeoutMs);
      const res = await fetch(endpoint.toString(), {
        method: "GET",
        signal: controller.signal,
      });
      clearTimeout(timeout);
      if (!res.ok) return null;
      return await res.json();
    } catch (_error) {
      return null;
    }
  }

  async function getLifecycleMap(wallets = []) {
    const list = Array.from(new Set((Array.isArray(wallets) ? wallets : []).map((x) => String(x || "").trim()).filter(Boolean)));
    if (!list.length) return {};
    if (walletIndexer && typeof walletIndexer.getWalletLifecycleMap === "function") {
      return walletIndexer.getWalletLifecycleMap(list);
    }
    const payload = await fetchIndexerApi("/api/indexer/lifecycle-map", {
      wallets: list.join(","),
    });
    if (payload && payload.map && typeof payload.map === "object") {
      return payload.map;
    }
    return {};
  }

  async function getLifecycleSnapshot(wallet) {
    const normalized = String(wallet || "").trim();
    if (!normalized) return null;
    if (walletIndexer && typeof walletIndexer.getWalletLifecycleSnapshot === "function") {
      return walletIndexer.getWalletLifecycleSnapshot(normalized);
    }
    const payload = await fetchIndexerApi("/api/indexer/lifecycle", {
      wallet: normalized,
    });
    if (payload && payload.snapshot && typeof payload.snapshot === "object") {
      return payload.snapshot;
    }
    return null;
  }

  function buildVolumeWindowInfo(truth = {}) {
    const MS_DAY = 24 * 60 * 60 * 1000;
    const todayUtcDate =
      (truth &&
        truth.volumeMeta &&
        truth.volumeMeta.todayUtcDate &&
        String(truth.volumeMeta.todayUtcDate).slice(0, 10)) ||
      new Date().toISOString().slice(0, 10);
    const computeRemainingDays = (trackedDate) => {
      if (!trackedDate) return null;
      const trackedMs = Date.parse(`${trackedDate}T00:00:00.000Z`);
      const todayMs = Date.parse(`${todayUtcDate}T00:00:00.000Z`);
      if (!Number.isFinite(trackedMs) || !Number.isFinite(todayMs)) return null;
      return Math.max(0, Math.floor((todayMs - trackedMs) / MS_DAY));
    };

    const method = String(truth.volumeMethod || "prices_rolling_24h");
    if (method === "defillama_compat") {
      const meta = truth && truth.volumeMeta && typeof truth.volumeMeta === "object" ? truth.volumeMeta : {};
      const trackingStartDate = meta.trackingStartDate
        ? String(meta.trackingStartDate).slice(0, 10)
        : null;
      const trackedThroughDate = meta.lastProcessedDate
        ? String(meta.lastProcessedDate).slice(0, 10)
        : null;
      const remainingDaysToToday =
        meta.remainingDaysToToday !== null && meta.remainingDaysToToday !== undefined
          ? Number(meta.remainingDaysToToday)
          : computeRemainingDays(trackedThroughDate);
      const processedDays =
        meta.processedDays !== null && meta.processedDays !== undefined
          ? Number(meta.processedDays)
          : null;
      const totalDaysToToday =
        meta.totalDaysToToday !== null && meta.totalDaysToToday !== undefined
          ? Number(meta.totalDaysToToday)
          : null;
      const backfillComplete =
        meta.backfillComplete !== null && meta.backfillComplete !== undefined
          ? Boolean(meta.backfillComplete)
          : remainingDaysToToday === 0;
      const latestDayDate = meta.latestDayDate ? String(meta.latestDayDate).slice(0, 10) : null;
      return {
        kind: "defillama_historical",
        date: latestDayDate,
        windowStartIso: trackingStartDate ? `${trackingStartDate}T00:00:00.000Z` : null,
        windowEndIso: trackedThroughDate ? `${trackedThroughDate}T23:59:59.999Z` : null,
        label:
          trackingStartDate && trackedThroughDate
            ? `UTC ${trackingStartDate} -> ${trackedThroughDate}`
            : "defillama historical",
        trackedThroughDate,
        trackingStartDate,
        todayUtcDate,
        remainingDaysToToday,
        processedDays,
        totalDaysToToday,
        backfillComplete,
      };
    }

    const fetchedAt = Number(truth.fetchedAt || 0);
    if (Number.isFinite(fetchedAt) && fetchedAt > 0) {
      const start = new Date(fetchedAt - 24 * 60 * 60 * 1000);
      const end = new Date(fetchedAt);
      return {
        kind: "rolling_24h",
        date: null,
        windowStartIso: start.toISOString(),
        windowEndIso: end.toISOString(),
        label: `rolling 24h to ${end.toISOString().slice(0, 10)}`,
        trackedThroughDate: end.toISOString().slice(0, 10),
        todayUtcDate,
        remainingDaysToToday: computeRemainingDays(end.toISOString().slice(0, 10)),
      };
    }

    return {
      kind: method,
      date: null,
      windowStartIso: null,
      windowEndIso: null,
      label: method,
      trackedThroughDate: null,
      todayUtcDate,
      remainingDaysToToday: null,
    };
  }

  async function getDefiLlamaPricesTruth() {
    const now = Date.now();
    const shared =
      typeof globalKpiProvider === "function" ? globalKpiProvider() : null;
    const sharedHasPrices =
      shared &&
      typeof shared === "object" &&
      Number(shared.fetchedAt || 0) > 0 &&
      Array.isArray(shared.prices);
    const sharedHistory = {
      volumeMethod:
        (shared && shared.volumeMethod) || "prices_rolling_24h",
      volumeSource:
        (shared && shared.volumeSource) || "/api/v1/info/prices:sum(volume_24h)",
      volumeMeta: (shared && shared.volumeMeta) || null,
      totalHistoricalVolume:
        shared &&
        shared.totalHistoricalVolume !== null &&
        shared.totalHistoricalVolume !== undefined &&
        Number.isFinite(Number(shared.totalHistoricalVolume))
          ? Number(shared.totalHistoricalVolume)
          : null,
      dailyVolumeFromPrices24h:
        shared && Number.isFinite(Number(shared.dailyVolumeFromPrices24h))
          ? Number(shared.dailyVolumeFromPrices24h)
          : null,
      dailyVolumeDefillamaCompat:
        shared && Number.isFinite(Number(shared.dailyVolumeDefillamaCompat))
          ? Number(shared.dailyVolumeDefillamaCompat)
          : null,
      dailyHistoryByDate:
        shared &&
        shared.history &&
        shared.history.dailyByDate &&
        typeof shared.history.dailyByDate === "object" &&
        !Array.isArray(shared.history.dailyByDate)
          ? shared.history.dailyByDate
          : null,
    };

    // Source-of-truth for total volume + symbol rank is /info/prices (live + short cache).
    // Global KPI contributes only historical metadata (tracking progress/cumulative totals).
    if (
      Number(defillamaCache.ttlMs || 0) > 0 &&
      now - Number(defillamaCache.fetchedAt || 0) <= defillamaCache.ttlMs
    ) {
      return {
        ...defillamaCache.value,
        volumeMethod: sharedHistory.volumeMethod,
        volumeSource: sharedHistory.volumeSource,
        volumeMeta: sharedHistory.volumeMeta,
        totalHistoricalVolume: sharedHistory.totalHistoricalVolume,
        dailyVolumeFromPrices24h: Number(defillamaCache.value.dailyVolume || 0),
        dailyVolumeDefillamaCompat: sharedHistory.dailyVolumeDefillamaCompat,
        dailyHistoryByDate: sharedHistory.dailyHistoryByDate,
        stale: Boolean(defillamaCache.stale),
        lastError: defillamaCache.lastError || null,
        fetchedAt: defillamaCache.fetchedAt || null,
        fetchDurationMs: defillamaCache.lastFetchDurationMs,
        cacheHit: true,
        source: "local_cache",
      };
    }

    if (!restClient || typeof restClient.get !== "function") {
      if (sharedHasPrices) {
        return {
          prices: shared.prices,
          dailyVolume: Number(shared.dailyVolume || 0),
          openInterestAtEnd: Number(shared.openInterestAtEnd || 0),
          volumeMethod: sharedHistory.volumeMethod,
          volumeSource: sharedHistory.volumeSource,
          volumeMeta: sharedHistory.volumeMeta,
          totalHistoricalVolume: sharedHistory.totalHistoricalVolume,
          dailyVolumeFromPrices24h: sharedHistory.dailyVolumeFromPrices24h,
          dailyVolumeDefillamaCompat: sharedHistory.dailyVolumeDefillamaCompat,
          dailyHistoryByDate: sharedHistory.dailyHistoryByDate,
          stale: true,
          lastError: "rest_client_unavailable",
          fetchedAt: Number(shared.fetchedAt || 0) || null,
          fetchDurationMs: Number(shared.fetchDurationMs || 0),
          cacheHit: true,
          source: "global_kpi_worker_fallback",
        };
      }

      return {
        ...defillamaCache.value,
        volumeMethod: sharedHistory.volumeMethod,
        volumeSource: sharedHistory.volumeSource,
        volumeMeta: sharedHistory.volumeMeta,
        totalHistoricalVolume: sharedHistory.totalHistoricalVolume,
        dailyVolumeFromPrices24h: Number(defillamaCache.value.dailyVolume || 0),
        dailyVolumeDefillamaCompat: sharedHistory.dailyVolumeDefillamaCompat,
        dailyHistoryByDate: sharedHistory.dailyHistoryByDate,
        stale: true,
        lastError: "rest_client_unavailable",
        fetchedAt: defillamaCache.fetchedAt || null,
        fetchDurationMs: defillamaCache.lastFetchDurationMs,
        cacheHit: true,
        source: "local_cache_fallback",
      };
    }

    try {
      const startedAt = Date.now();
      const res = await restClient.get("/info/prices", { cost: 1 });
      const fetchDurationMs = Date.now() - startedAt;
      const rows = extractPayloadData(res, []);
      const totals = computeDefiLlamaV2FromPrices(rows);
      const next = {
        prices: Array.isArray(rows) ? rows : [],
        dailyVolume: totals.dailyVolume,
        openInterestAtEnd: totals.openInterestAtEnd,
      };
      defillamaCache.value = next;
      defillamaCache.fetchedAt = now;
      defillamaCache.stale = false;
      defillamaCache.lastError = null;
      defillamaCache.lastFetchDurationMs = fetchDurationMs;
      return {
        ...next,
        volumeMethod: sharedHistory.volumeMethod,
        volumeSource: sharedHistory.volumeSource,
        volumeMeta: sharedHistory.volumeMeta,
        totalHistoricalVolume: sharedHistory.totalHistoricalVolume,
        dailyVolumeFromPrices24h: Number(next.dailyVolume || 0),
        dailyVolumeDefillamaCompat: sharedHistory.dailyVolumeDefillamaCompat,
        dailyHistoryByDate: sharedHistory.dailyHistoryByDate,
        stale: false,
        lastError: null,
        fetchedAt: defillamaCache.fetchedAt,
        fetchDurationMs,
        cacheHit: false,
        source: "local_fetch",
      };
    } catch (error) {
      if (Number(defillamaCache.fetchedAt || 0) > 0) {
        defillamaCache.stale = true;
        defillamaCache.lastError = error.message || "defillama_fetch_failed";
        return {
          ...defillamaCache.value,
          volumeMethod: sharedHistory.volumeMethod,
          volumeSource: sharedHistory.volumeSource,
          volumeMeta: sharedHistory.volumeMeta,
          totalHistoricalVolume: sharedHistory.totalHistoricalVolume,
          dailyVolumeFromPrices24h: Number(defillamaCache.value.dailyVolume || 0),
          dailyVolumeDefillamaCompat: sharedHistory.dailyVolumeDefillamaCompat,
          dailyHistoryByDate: sharedHistory.dailyHistoryByDate,
          stale: true,
          lastError: defillamaCache.lastError,
          fetchedAt: defillamaCache.fetchedAt || null,
          fetchDurationMs: defillamaCache.lastFetchDurationMs,
          cacheHit: true,
          source: "local_cache_fallback",
        };
      }

      if (sharedHasPrices) {
        return {
          prices: shared.prices,
          dailyVolume: Number(shared.dailyVolume || 0),
          openInterestAtEnd: Number(shared.openInterestAtEnd || 0),
          volumeMethod: sharedHistory.volumeMethod,
          volumeSource: sharedHistory.volumeSource,
          volumeMeta: sharedHistory.volumeMeta,
          totalHistoricalVolume: sharedHistory.totalHistoricalVolume,
          dailyVolumeFromPrices24h: Number(shared.dailyVolume || 0),
          dailyVolumeDefillamaCompat: sharedHistory.dailyVolumeDefillamaCompat,
          dailyHistoryByDate: sharedHistory.dailyHistoryByDate,
          stale: true,
          lastError: error.message || "defillama_fetch_failed",
          fetchedAt: Number(shared.fetchedAt || 0) || null,
          fetchDurationMs: Number(shared.fetchDurationMs || 0),
          cacheHit: true,
          source: "global_kpi_worker_fallback",
        };
      }

      throw error;
    }
  }

  function normalizeTokenAnalyticsTimeframe(value) {
    const raw = String(value || "all").trim().toLowerCase();
    if (raw === "24h" || raw === "1d" || raw === "day") return "24h";
    if (raw === "30d" || raw === "1m" || raw === "month") return "30d";
    return "all";
  }

  function getTokenAnalyticsLookbackMs(timeframe) {
    const normalized = normalizeTokenAnalyticsTimeframe(timeframe);
    if (normalized === "24h") return 2 * DAY_MS;
    if (normalized === "30d") return 35 * DAY_MS;
    return tokenAnalyticsAllLookbackDays * DAY_MS;
  }

  function trimRowsByLookback(rows, lookbackMs) {
    const cutoffMs = Date.now() - Math.max(0, Number(lookbackMs || 0));
    if (!Number.isFinite(cutoffMs) || cutoffMs <= 0) return rows;
    return (Array.isArray(rows) ? rows : []).filter((row) => {
      const ts = Number(row && row.timestamp !== undefined ? row.timestamp : row && row.createdAt);
      return Number.isFinite(ts) && ts >= cutoffMs;
    });
  }

  function buildDashboardTokenFallback(symbol) {
    const dashboard = pipeline.getDashboardPayload();
    const market = dashboard && dashboard.market && typeof dashboard.market === "object" ? dashboard.market : {};
    const prices = Array.isArray(market.prices) ? market.prices : [];
    const priceRow = findPriceRowForSymbol(prices, symbol);
    const symbolKey = normalizeTokenSymbol(priceRow && priceRow.symbol ? priceRow.symbol : symbol);
    const fundingRowsRaw =
      market &&
      market.fundingBySymbol &&
      market.fundingBySymbol[symbolKey] &&
      Array.isArray(market.fundingBySymbol[symbolKey].rows)
        ? market.fundingBySymbol[symbolKey].rows
        : [];
    const tradeRowsRaw =
      market && market.publicTradesBySymbol && Array.isArray(market.publicTradesBySymbol[symbolKey])
        ? market.publicTradesBySymbol[symbolKey]
        : [];

    const fundingRows = fundingRowsRaw
      .map(normalizeFundingHistoryRow)
      .filter((row) => Number.isFinite(row.createdAt) && Number.isFinite(row.fundingRatePct));
    const tradesRows = tradeRowsRaw
      .map(normalizePublicTradeRow)
      .filter((row) => Number.isFinite(row.timestamp) && row.timestamp > 0);
    return {
      symbol: symbolKey,
      priceRow: priceRow || null,
      fundingRows,
      tradesRows,
    };
  }

  async function fetchPaginatedTokenRows(pathname, options = {}) {
    const queryBase = options && options.query && typeof options.query === "object" ? options.query : {};
    const pageSize = Math.max(20, Math.min(400, Number(options.pageSize || 200)));
    const maxPages = Math.max(1, Number(options.maxPages || 1));
    const cutoffMs = Number(options.cutoffMs || 0);
    const cost = Math.max(0.1, Number(options.cost || 1));
    const normalizeRow = typeof options.normalizeRow === "function" ? options.normalizeRow : (x) => x;

    if (!restClient || typeof restClient.get !== "function") {
      return {
        rows: [],
        pages: 0,
        hasMore: false,
        lastCursor: null,
      };
    }

    let cursor = null;
    let pages = 0;
    let hasMore = false;
    const rows = [];

    while (pages < maxPages) {
      const query = {
        ...queryBase,
        page_size: pageSize,
      };
      if (cursor) query.cursor = cursor;
      const response = await restClient.get(pathname, {
        query,
        cost,
      });
      const payload = response && response.payload && typeof response.payload === "object" ? response.payload : {};
      const pageRowsRaw = extractPayloadData(response, []);
      const pageRows = (Array.isArray(pageRowsRaw) ? pageRowsRaw : [])
        .map(normalizeRow)
        .filter(Boolean);
      rows.push(...pageRows);
      pages += 1;

      hasMore = Boolean(payload.has_more);
      const nextCursor = payload.next_cursor ? String(payload.next_cursor) : null;
      const oldestTs = pageRows.reduce((minTs, row) => {
        const ts = Number(row && row.timestamp !== undefined ? row.timestamp : row && row.createdAt);
        if (!Number.isFinite(ts) || ts <= 0) return minTs;
        if (!Number.isFinite(minTs) || ts < minTs) return ts;
        return minTs;
      }, NaN);

      if (Number.isFinite(cutoffMs) && cutoffMs > 0 && Number.isFinite(oldestTs) && oldestTs <= cutoffMs) {
        break;
      }
      if (!hasMore || !nextCursor) break;
      cursor = nextCursor;
    }

    return {
      rows,
      pages,
      hasMore,
      lastCursor: cursor,
    };
  }

  function buildTokenAnalyticsPayload({
    symbol,
    timeframe,
    priceRow,
    tradesRows,
    fundingRows,
    sourceMeta,
  }) {
    const normalizedSymbol = normalizeTokenSymbol(
      (priceRow && priceRow.symbol) || symbol || ""
    );
    const trades = dedupeBy(
      (Array.isArray(tradesRows) ? tradesRows : [])
        .map(normalizePublicTradeRow)
        .filter((row) => Number.isFinite(row.timestamp) && row.timestamp > 0),
      (row) => String(row.historyId || `${row.timestamp}:${row.price}:${row.amount}:${row.side}`)
    ).sort((a, b) => a.timestamp - b.timestamp);

    const funding = dedupeBy(
      (Array.isArray(fundingRows) ? fundingRows : [])
        .map(normalizeFundingHistoryRow)
        .filter((row) => Number.isFinite(row.createdAt) && Number.isFinite(row.fundingRatePct)),
      (row) => String(row.createdAt)
    ).sort((a, b) => a.createdAt - b.createdAt);

    const volumeSeries = trades.map((row) => ({
      timestamp: row.timestamp,
      value: toNum(row.notionalUsd, 0),
    }));

    const netflowSeries = trades.map((row) => {
      const sign = classifyNetflowSign(row.side);
      const notional = toNum(row.notionalUsd, 0);
      return {
        timestamp: row.timestamp,
        value: sign === 0 ? 0 : notional * sign,
      };
    });

    const liquidationSeries = trades
      .filter((row) => isLiquidationCause(row.cause))
      .map((row) => {
        const notional = toNum(row.notionalUsd, 0);
        const bucket = classifyLiquidationBucket(row.side);
        if (bucket === "long") {
          return {
            timestamp: row.timestamp,
            value: notional,
            longValue: notional,
            shortValue: 0,
          };
        }
        if (bucket === "short") {
          return {
            timestamp: row.timestamp,
            value: notional,
            longValue: 0,
            shortValue: notional,
          };
        }
        const sign = classifyNetflowSign(row.side);
        const inferShort = sign >= 0;
        return {
          timestamp: row.timestamp,
          value: notional,
          longValue: inferShort ? 0 : notional,
          shortValue: inferShort ? notional : 0,
        };
      });

    const fundingSeries = funding.map((row) => ({
      timestamp: row.createdAt,
      value: row.fundingRatePct,
    }));

    const walletActivitySeries = trades.map((row) => ({
      timestamp: row.timestamp,
      value: 1,
    }));

    const mark = toNum(priceRow && priceRow.mark !== undefined ? priceRow.mark : 0, 0);
    const openInterest = toNum(
      priceRow && priceRow.open_interest !== undefined ? priceRow.open_interest : 0,
      0
    );
    const oiUsdNow = mark * openInterest;
    let oiSeries = [];
    if (oiUsdNow > 0) {
      if (fundingSeries.length) {
        oiSeries = fundingSeries.map((row) => ({
          timestamp: row.timestamp,
          value: oiUsdNow,
        }));
      } else if (trades.length) {
        const step = Math.max(1, Math.floor(trades.length / 64));
        oiSeries = trades
          .filter((_, idx) => idx % step === 0 || idx === trades.length - 1)
          .map((row) => ({
            timestamp: row.timestamp,
            value: oiUsdNow,
          }));
      } else {
        oiSeries = [
          {
            timestamp: Date.now(),
            value: oiUsdNow,
          },
        ];
      }
    }

    const horizon24h = Date.now() - DAY_MS;
    const netflow24hUsd = netflowSeries
      .filter((row) => Number(row.timestamp || 0) >= horizon24h)
      .reduce((sum, row) => sum + toNum(row.value, 0), 0);
    const liquidations24hUsd = liquidationSeries
      .filter((row) => Number(row.timestamp || 0) >= horizon24h)
      .reduce((sum, row) => sum + toNum(row.value, 0), 0);
    const activity24h = walletActivitySeries
      .filter((row) => Number(row.timestamp || 0) >= horizon24h)
      .length;
    const events = trades
      .slice(-80)
      .reverse()
      .map((row) => ({
        timestamp: Number(row.timestamp || 0),
        side: String(row.side || "").toLowerCase(),
        cause: String(row.cause || "").toLowerCase() || "normal",
        notionalUsd: toNum(row.notionalUsd, 0),
      }))
      .filter((row) => Number.isFinite(row.timestamp) && row.timestamp > 0);

    const lastTradeAt = trades.length ? Number(trades[trades.length - 1].timestamp || 0) : null;
    const lastFundingAt = funding.length ? Number(funding[funding.length - 1].createdAt || 0) : null;
    const priceTs = toTradeTimestampMs({
      timestamp: priceRow && priceRow.timestamp !== undefined ? priceRow.timestamp : null,
    });
    const updatedAt = [lastTradeAt, lastFundingAt, priceTs]
      .filter((value) => Number.isFinite(Number(value)) && Number(value) > 0)
      .reduce((max, value) => Math.max(max, Number(value)), 0);

    return {
      generatedAt: Date.now(),
      symbol: normalizedSymbol,
      timeframe: normalizeTokenAnalyticsTimeframe(timeframe),
      kpis: {
        currentOiUsd: oiUsdNow,
        volume24hUsd: toNum(priceRow && priceRow.volume_24h !== undefined ? priceRow.volume_24h : 0, 0),
        fundingPctCurrent:
          toNum(priceRow && priceRow.funding !== undefined ? priceRow.funding : 0, 0) * 100,
        netflow24hUsd,
        liquidations24hUsd,
        activity24h,
        lastTradeAt,
        lastFundingAt,
      },
      series: {
        volume: volumeSeries,
        oi: oiSeries,
        funding: fundingSeries,
        liquidations: liquidationSeries,
        netflow: netflowSeries,
        wallet_activity: walletActivitySeries,
      },
      events,
      meta: {
        source: sourceMeta || {},
        rows: {
          trades: trades.length,
          funding: funding.length,
          liquidations: liquidationSeries.length,
        },
        mode: {
          oi: fundingSeries.length ? "constant_snapshot_on_funding_timestamps" : "snapshot_only",
          funding: "funding_rate_history",
          liquidations: "cause_filtered_only",
          netflow: "side_mapped_open_close",
        },
      },
      freshness: {
        updatedAt: updatedAt || Date.now(),
        priceTimestamp: Number.isFinite(priceTs) ? priceTs : null,
      },
    };
  }

  async function buildTokenAnalytics(symbol, timeframe) {
    const normalizedSymbol = normalizeTokenSymbol(symbol);
    const normalizedTimeframe = normalizeTokenAnalyticsTimeframe(timeframe);
    const lookbackMs = getTokenAnalyticsLookbackMs(normalizedTimeframe);
    const cutoffMs = Date.now() - lookbackMs;
    const warnings = [];

    const fallback = buildDashboardTokenFallback(normalizedSymbol);
    const sourceMeta = {
      price: "dashboard_fallback",
      funding: "dashboard_fallback",
      trades: "dashboard_fallback",
      tradesPages: 0,
      fundingPages: 0,
      tradesHasMore: false,
      fundingHasMore: false,
      warnings,
    };

    let priceRow = fallback.priceRow;
    try {
      const truth = await getDefiLlamaPricesTruth();
      const remotePrice = findPriceRowForSymbol(
        truth && Array.isArray(truth.prices) ? truth.prices : [],
        normalizedSymbol
      );
      if (remotePrice) {
        priceRow = remotePrice;
        sourceMeta.price = "/api/v1/info/prices";
      } else if (!priceRow) {
        warnings.push("price_symbol_not_found");
      }
    } catch (error) {
      warnings.push(`price_fetch_failed:${error.message}`);
    }

    let tradesRows = trimRowsByLookback(fallback.tradesRows, lookbackMs);
    let fundingRows = trimRowsByLookback(fallback.fundingRows, lookbackMs);
    if (restClient && typeof restClient.get === "function") {
      try {
        const remoteTrades = await fetchPaginatedTokenRows("/trades", {
          query: { symbol: normalizedSymbol },
          pageSize: tokenAnalyticsTradesPageSize,
          maxPages: tokenAnalyticsTradesMaxPages,
          cutoffMs,
          normalizeRow: normalizePublicTradeRow,
          cost: tokenAnalyticsTradesCostPerPage,
        });
        if (Array.isArray(remoteTrades.rows) && remoteTrades.rows.length) {
          tradesRows = trimRowsByLookback(remoteTrades.rows, lookbackMs);
          sourceMeta.trades = "/api/v1/trades";
          sourceMeta.tradesPages = Number(remoteTrades.pages || 0);
          sourceMeta.tradesHasMore = Boolean(remoteTrades.hasMore);
          if (remoteTrades.hasMore) warnings.push("trades_truncated_max_pages");
        } else {
          warnings.push("trades_remote_empty");
        }
      } catch (error) {
        warnings.push(`trades_fetch_failed:${error.message}`);
      }

      try {
        const remoteFunding = await fetchPaginatedTokenRows("/funding_rate/history", {
          query: { symbol: normalizedSymbol },
          pageSize: tokenAnalyticsFundingPageSize,
          maxPages: tokenAnalyticsFundingMaxPages,
          cutoffMs,
          normalizeRow: normalizeFundingHistoryRow,
          cost: tokenAnalyticsFundingCostPerPage,
        });
        if (Array.isArray(remoteFunding.rows) && remoteFunding.rows.length) {
          fundingRows = trimRowsByLookback(remoteFunding.rows, lookbackMs);
          sourceMeta.funding = "/api/v1/funding_rate/history";
          sourceMeta.fundingPages = Number(remoteFunding.pages || 0);
          sourceMeta.fundingHasMore = Boolean(remoteFunding.hasMore);
          if (remoteFunding.hasMore) warnings.push("funding_truncated_max_pages");
        } else {
          warnings.push("funding_remote_empty");
        }
      } catch (error) {
        warnings.push(`funding_fetch_failed:${error.message}`);
      }
    }

    return buildTokenAnalyticsPayload({
      symbol: normalizedSymbol,
      timeframe: normalizedTimeframe,
      priceRow,
      tradesRows,
      fundingRows,
      sourceMeta,
    });
  }

  async function getTokenAnalytics(symbol, timeframe, { force = false } = {}) {
    const normalizedSymbol = normalizeTokenSymbol(symbol);
    const normalizedTimeframe = normalizeTokenAnalyticsTimeframe(timeframe);
    const cacheKey = `${normalizedSymbol}:${normalizedTimeframe}`;
    const now = Date.now();

    if (!force && tokenAnalyticsCache.has(cacheKey)) {
      const cached = tokenAnalyticsCache.get(cacheKey);
      if (cached && now - Number(cached.generatedAt || 0) <= tokenAnalyticsCacheTtlMs) {
        return {
          ...cached.payload,
          cache: {
            hit: true,
            ttlMs: tokenAnalyticsCacheTtlMs,
            generatedAt: cached.generatedAt,
          },
        };
      }
    }

    if (!force && tokenAnalyticsInflight.has(cacheKey)) {
      return tokenAnalyticsInflight.get(cacheKey);
    }

    const task = (async () => {
      const payload = await buildTokenAnalytics(normalizedSymbol, normalizedTimeframe);
      tokenAnalyticsCache.set(cacheKey, {
        generatedAt: Date.now(),
        payload,
      });
      return {
        ...payload,
        cache: {
          hit: false,
          ttlMs: tokenAnalyticsCacheTtlMs,
          generatedAt: Date.now(),
        },
      };
    })()
      .finally(() => {
        tokenAnalyticsInflight.delete(cacheKey);
      });

    tokenAnalyticsInflight.set(cacheKey, task);
    return task;
  }

  function parseWindowParam(rawValue, fallback = 0) {
    if (rawValue === null || rawValue === undefined || String(rawValue).trim() === "") {
      return fallback;
    }
    const n = Number(rawValue);
    if (!Number.isFinite(n)) return fallback;
    const v = Math.floor(n);
    return v < 0 ? 0 : v;
  }

  function parseFlagParam(rawValue, fallback = false) {
    if (rawValue === null || rawValue === undefined || String(rawValue).trim() === "") {
      return Boolean(fallback);
    }
    const value = String(rawValue).trim().toLowerCase();
    if (["1", "true", "yes", "on", "y"].includes(value)) return true;
    if (["0", "false", "no", "off", "n"].includes(value)) return false;
    return Boolean(fallback);
  }

  function applyWindow(rows, { offset = 0, limit = 0 } = {}) {
    const list = Array.isArray(rows) ? rows : [];
    const safeOffset = parseWindowParam(offset, 0);
    const safeLimit = parseWindowParam(limit, 0);
    const total = list.length;
    const start = Math.min(safeOffset, total);
    const end = safeLimit > 0 ? Math.min(total, start + safeLimit) : total;
    const windowed = list.slice(start, end);
    const windowedByQuery = safeLimit > 0 || safeOffset > 0;
    return {
      rows: windowed,
      total,
      offset: start,
      limit: safeLimit,
      returned: windowed.length,
      windowedByQuery,
      hasMore: end < total,
    };
  }

  async function refreshPacificaPublicActiveCache(force = false) {
    if (!restClient || typeof restClient.get !== "function") {
      return pacificaPublicActiveCache.value;
    }
    const now = Date.now();
    if (
      !force &&
      pacificaPublicActiveCache.value &&
      now - Number(pacificaPublicActiveCache.fetchedAt || 0) < pacificaPublicActiveCache.ttlMs
    ) {
      return pacificaPublicActiveCache.value;
    }
    if (pacificaPublicActiveCache.inflight) {
      return pacificaPublicActiveCache.inflight;
    }
    pacificaPublicActiveCache.inflight = restClient
      .get("/leaderboard", {
        query: { limit: 25000 },
        timeoutMs: Math.max(
          5000,
          Number(process.env.PACIFICA_PUBLIC_ACTIVE_TIMEOUT_MS || 12000)
        ),
      })
      .then((response) => {
        const rows =
          response &&
          response.payload &&
          Array.isArray(response.payload.data)
            ? response.payload.data
            : [];
        const trackedWallets = new Set(
          (walletStore ? walletStore.list() : [])
            .map((row) => String((row && row.wallet) || "").trim())
            .filter(Boolean)
        );
        const activeWallets = [];
        let trackedOverlap = 0;
        const trackedActiveWallets = [];
        rows.forEach((row) => {
          const wallet = String((row && row.address) || "").trim();
          const oiCurrent = Number(row && row.oi_current !== undefined ? row.oi_current : 0);
          if (!wallet || !Number.isFinite(oiCurrent) || oiCurrent <= 0) return;
          activeWallets.push(wallet);
          if (trackedWallets.has(wallet)) {
            trackedOverlap += 1;
            trackedActiveWallets.push(wallet);
          }
        });
        pacificaPublicActiveCache.value = {
          source: "pacifica_leaderboard_oi_current_gt_0",
          fetchedAt: Date.now(),
          rowsFetched: rows.length,
          activeWalletsLowerBound: activeWallets.length,
          trackedOverlap,
          untrackedActiveWallets: Math.max(0, activeWallets.length - trackedOverlap),
          trackedActiveWallets,
        };
        pacificaPublicActiveCache.fetchedAt = Date.now();
        pacificaPublicActiveCache.lastError = null;
        return pacificaPublicActiveCache.value;
      })
      .catch((error) => {
        pacificaPublicActiveCache.lastError = String(
          error && error.message ? error.message : error || "public_active_fetch_failed"
        );
        return pacificaPublicActiveCache.value;
      })
      .finally(() => {
        pacificaPublicActiveCache.inflight = null;
      });
    return pacificaPublicActiveCache.inflight;
  }

  function getPacificaPublicActiveSnapshot() {
    const now = Date.now();
    if (
      !pacificaPublicActiveCache.inflight &&
      (!pacificaPublicActiveCache.value ||
        now - Number(pacificaPublicActiveCache.fetchedAt || 0) >= pacificaPublicActiveCache.ttlMs)
    ) {
      refreshPacificaPublicActiveCache().catch(() => {});
    }
    return {
      ...(pacificaPublicActiveCache.value || {
        source: "pacifica_leaderboard_oi_current_gt_0",
        activeWalletsLowerBound: null,
        trackedOverlap: null,
        untrackedActiveWallets: null,
        rowsFetched: null,
      }),
      fetchedAt: pacificaPublicActiveCache.fetchedAt || null,
      stale:
        !pacificaPublicActiveCache.fetchedAt ||
        now - Number(pacificaPublicActiveCache.fetchedAt || 0) >= pacificaPublicActiveCache.ttlMs,
      lastError: pacificaPublicActiveCache.lastError,
      inflight: Boolean(pacificaPublicActiveCache.inflight),
    };
  }

  function getPacificaTrackedPublicActiveWallets() {
    const snapshot = getPacificaPublicActiveSnapshot();
    return Array.isArray(snapshot && snapshot.trackedActiveWallets)
      ? snapshot.trackedActiveWallets
      : [];
  }

  function buildLiveActiveWalletRows({ walletRows = [], positions = [], events = [], nowMs = Date.now() }) {
    const walletMap = new Map();
    (Array.isArray(walletRows) ? walletRows : []).forEach((row) => {
      const wallet = String((row && row.wallet) || "").trim();
      if (!wallet) return;
      walletMap.set(wallet, row);
    });

    const positionStats = new Map();
    (Array.isArray(positions) ? positions : []).forEach((row) => {
      const wallet = String((row && row.wallet) || "").trim();
      if (!wallet) return;
      const current =
        positionStats.get(wallet) || {
          wallet,
          openPositions: 0,
          positionUsd: 0,
          lastPositionAt: 0,
          symbols: new Set(),
          freshPositions: 0,
          stalePositions: 0,
        };
      current.openPositions += 1;
      current.positionUsd += Math.max(
        0,
        Number(row && row.positionUsd !== undefined ? row.positionUsd : 0) || 0
      );
      current.lastPositionAt = Math.max(
        current.lastPositionAt,
        Number((row && (row.updatedAt || row.timestamp)) || 0)
      );
      const symbol = String((row && row.symbol) || "").trim().toUpperCase();
      if (symbol) current.symbols.add(symbol);
      const freshness = String((row && (row.freshness || row.status)) || "").trim().toLowerCase();
      if (freshness === "fresh") current.freshPositions += 1;
      else if (freshness === "stale") current.stalePositions += 1;
      positionStats.set(wallet, current);
    });

    const fifteenMinutesAgo = nowMs - 15 * 60 * 1000;
    const oneHourAgo = nowMs - 60 * 60 * 1000;
    const eventStats = new Map();
    (Array.isArray(events) ? events : []).forEach((row) => {
      const wallet = String((row && row.wallet) || "").trim();
      if (!wallet) return;
      const timestamp = Number((row && row.timestamp) || 0);
      if (!Number.isFinite(timestamp) || timestamp <= 0) return;
      const current =
        eventStats.get(wallet) || {
          wallet,
          lastEventAt: 0,
          events15m: 0,
          events1h: 0,
        };
      current.lastEventAt = Math.max(current.lastEventAt, timestamp);
      if (timestamp >= fifteenMinutesAgo) current.events15m += 1;
      if (timestamp >= oneHourAgo) current.events1h += 1;
      eventStats.set(wallet, current);
    });

    const wallets = new Set([
      ...walletMap.keys(),
      ...positionStats.keys(),
      ...eventStats.keys(),
    ]);

    const rows = [];
    wallets.forEach((wallet) => {
      const walletRow = walletMap.get(wallet) || {};
      const positionRow =
        positionStats.get(wallet) || {
          openPositions: 0,
          positionUsd: 0,
          lastPositionAt: 0,
          symbols: new Set(),
          freshPositions: 0,
          stalePositions: 0,
        };
      const eventRow =
        eventStats.get(wallet) || {
          lastEventAt: 0,
          events15m: 0,
          events1h: 0,
        };
      const d24 = walletRow && walletRow.d24 ? walletRow.d24 : {};
      const d7 = walletRow && walletRow.d7 ? walletRow.d7 : {};
      const d30 = walletRow && walletRow.d30 ? walletRow.d30 : {};
      const all = walletRow && walletRow.all ? walletRow.all : {};
      const liveScannedAt = Number((walletRow && walletRow.liveScannedAt) || 0);
      const lastTradeAt = Number((d24 && d24.lastTrade) || 0);
      const lastActivityAt = Math.max(
        liveScannedAt,
        lastTradeAt,
        Number(positionRow.lastPositionAt || 0),
        Number(eventRow.lastEventAt || 0)
      );
      if (
        Number(positionRow.openPositions || 0) <= 0 &&
        Number(eventRow.events1h || 0) <= 0 &&
        Number((d24 && d24.trades) || 0) <= 0 &&
        (!lastActivityAt || nowMs - lastActivityAt > liveActiveWindowMs)
      ) {
        return;
      }
      const ageMs = lastActivityAt > 0 ? Math.max(0, nowMs - lastActivityAt) : null;
      const freshness =
        ageMs === null
          ? "unknown"
          : ageMs <= 5 * 60 * 1000
          ? "fresh"
          : ageMs <= 15 * 60 * 1000
          ? "warm"
          : "stale";
      const activityScore =
        Number(positionRow.openPositions || 0) * 1_000_000_000 +
        Number(eventRow.events15m || 0) * 10_000_000 +
        Number(eventRow.events1h || 0) * 1_000_000 +
        Math.round(Number(positionRow.positionUsd || 0)) * 10 +
        Math.max(0, 100_000 - Math.floor((ageMs || 0) / 1000));
      rows.push({
        wallet,
        freshness,
        openPositions: Number(positionRow.openPositions || 0),
        positionUsd: Number(Number(positionRow.positionUsd || 0).toFixed(2)),
        lastActivityAt: lastActivityAt || null,
        liveScannedAt: liveScannedAt || null,
        lastTradeAt: lastTradeAt || null,
        lastPositionAt: Number(positionRow.lastPositionAt || 0) || null,
        recentEvents15m: Number(eventRow.events15m || 0),
        recentEvents1h: Number(eventRow.events1h || 0),
        freshPositions: Number(positionRow.freshPositions || 0),
        stalePositions: Number(positionRow.stalePositions || 0),
        symbols: Array.from(positionRow.symbols || []).sort(),
        d24: {
          trades: Number((d24 && d24.trades) || 0),
          volumeUsd: Number(toNum(d24 && d24.volumeUsd, 0).toFixed(2)),
          pnlUsd: Number(toNum(d24 && d24.pnlUsd, 0).toFixed(2)),
          winRatePct: Number((d24 && d24.winRatePct) || 0),
        },
        d30: {
          trades: Number((d30 && d30.trades) || 0),
          volumeUsd: Number(toNum(d30 && d30.volumeUsd, 0).toFixed(2)),
          pnlUsd: Number(toNum(d30 && d30.pnlUsd, 0).toFixed(2)),
          winRatePct: Number((d30 && d30.winRatePct) || 0),
        },
        d7: {
          trades: Number((d7 && d7.trades) || 0),
          volumeUsd: Number(toNum(d7 && d7.volumeUsd, 0).toFixed(2)),
          pnlUsd: Number(toNum(d7 && d7.pnlUsd, 0).toFixed(2)),
          winRatePct: Number((d7 && d7.winRatePct) || 0),
        },
        all: {
          trades: Number((all && all.trades) || 0),
          volumeUsd: Number(toNum(all && all.volumeUsd, 0).toFixed(2)),
          pnlUsd: Number(toNum(all && all.pnlUsd, 0).toFixed(2)),
          winRatePct: Number((all && all.winRatePct) || 0),
        },
        activityScore,
      });
    });

    rows.sort((a, b) => {
      if (b.activityScore !== a.activityScore) return b.activityScore - a.activityScore;
      if ((b.lastActivityAt || 0) !== (a.lastActivityAt || 0)) {
        return Number(b.lastActivityAt || 0) - Number(a.lastActivityAt || 0);
      }
      return String(a.wallet).localeCompare(String(b.wallet));
    });
    return rows;
  }

  function summarizeWalletWindow(bucket = {}) {
    const safeBucket = bucket && typeof bucket === "object" ? bucket : {};
    return {
      trades: Number(safeBucket.trades || 0),
      volumeUsd: Number(toNum(safeBucket.volumeUsd, 0).toFixed(2)),
      pnlUsd: Number(toNum(safeBucket.pnlUsd, 0).toFixed(2)),
      wins: Number(
        safeBucket.wins !== undefined ? safeBucket.wins : safeBucket.winCount || 0
      ),
      losses: Number(
        safeBucket.losses !== undefined ? safeBucket.losses : safeBucket.lossCount || 0
      ),
      winRatePct: Number(toNum(safeBucket.winRatePct, 0).toFixed(2)),
      feesUsd: Number(
        toNum(
          safeBucket.feesPaidUsd !== undefined ? safeBucket.feesPaidUsd : safeBucket.feesUsd,
          0
        ).toFixed(2)
      ),
      fundingPayoutUsd: Number(toNum(safeBucket.fundingPayoutUsd, 0).toFixed(2)),
      firstTrade: safeBucket.firstTrade || null,
      lastTrade: safeBucket.lastTrade || null,
      symbolVolumes:
        safeBucket.symbolVolumes && typeof safeBucket.symbolVolumes === "object"
          ? safeBucket.symbolVolumes
          : {},
    };
  }

  function buildWalletConcentrationRows(symbolVolumes = {}, totalVolumeUsd = 0, limit = 8) {
    const total = toNum(totalVolumeUsd, 0);
    return Object.entries(symbolVolumes && typeof symbolVolumes === "object" ? symbolVolumes : {})
      .map(([symbol, volumeUsd]) => {
        const volume = toNum(volumeUsd, 0);
        return {
          symbol: String(symbol || "").trim().toUpperCase(),
          volumeUsd: Number(volume.toFixed(2)),
          sharePct: total > 0 ? Number(((volume / total) * 100).toFixed(2)) : 0,
        };
      })
      .filter((row) => row.symbol && row.volumeUsd > 0)
      .sort((a, b) => b.volumeUsd - a.volumeUsd)
      .slice(0, Math.max(1, Number(limit || 8)));
  }

  function buildWalletFreshnessLabel(lastActivityAt, nowMs = Date.now()) {
    const ts = Number(lastActivityAt || 0);
    if (!Number.isFinite(ts) || ts <= 0) return "unknown";
    const ageMs = Math.max(0, nowMs - ts);
    if (ageMs <= 5 * 60 * 1000) return "fresh";
    if (ageMs <= 15 * 60 * 1000) return "warm";
    return "stale";
  }

  function normalizeWalletExplorerTimeframe(value) {
    const raw = String(value || "all").trim().toLowerCase();
    if (raw === "24h") return "24h";
    if (raw === "30d") return "30d";
    return "all";
  }

  function pickWalletExplorerBucket(row = {}, timeframe = "all") {
    if (timeframe === "24h") return row.d24 || {};
    if (timeframe === "30d") return row.d30 || {};
    return row.all || {};
  }

  function parseWalletExplorerNumber(value) {
    if (value === null || value === undefined || value === "") return null;
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : null;
  }

  function parseWalletExplorerDate(value, endOfDay = false) {
    const raw = String(value || "").trim();
    if (!raw) return null;
    const normalized = raw.length === 10 ? `${raw}T${endOfDay ? "23:59:59.999" : "00:00:00.000"}Z` : raw;
    const parsed = Date.parse(normalized);
    return Number.isFinite(parsed) ? parsed : null;
  }

  function parseWalletExplorerTokens(value) {
    return Array.from(
      new Set(
        String(value || "")
          .split(",")
          .map((token) => String(token || "").trim().toUpperCase())
          .filter(Boolean)
      )
    );
  }

  function parseWalletExplorerQuery(rawQuery = {}) {
    const query = rawQuery && typeof rawQuery === "object" ? rawQuery : {};
    const requestedSort = String(query.sort || "volumeUsd").trim();
    return {
      timeframe: normalizeWalletExplorerTimeframe(query.timeframe),
      q: String(query.q || "").trim().toLowerCase(),
      page: Math.max(1, Number(query.page || 1)),
      pageSize: Math.max(1, Math.min(200, Number(query.pageSize || 20))),
      sort: {
        wallet: "wallet",
        trades: "trades",
        volume: "volumeUsd",
        volumeUsd: "volumeUsd",
        totalWins: "totalWins",
        totalLosses: "totalLosses",
        pnl: "pnlUsd",
        pnlUsd: "pnlUsd",
        winRate: "winRate",
        firstTrade: "firstTrade",
        lastTrade: "lastTrade",
        openPositions: "openPositions",
        exposureUsd: "exposureUsd",
      }[requestedSort] || "volumeUsd",
      dir: String(query.dir || "desc").trim().toLowerCase() === "asc" ? "asc" : "desc",
      symbols: parseWalletExplorerTokens(query.symbols || query.symbol || ""),
      side: String(query.side || "").trim().toLowerCase(),
      status: Array.from(
        new Set(
          String(query.status || "")
            .split(",")
            .map((item) => String(item || "").trim().toLowerCase())
            .filter(Boolean)
        )
      ),
      openPositions: String(query.openPositions || query.open || "").trim().toLowerCase(),
      minTrades: parseWalletExplorerNumber(query.minTrades),
      maxTrades: parseWalletExplorerNumber(query.maxTrades),
      minVolumeUsd: parseWalletExplorerNumber(query.minVolumeUsd ?? query.minVolume),
      maxVolumeUsd: parseWalletExplorerNumber(query.maxVolumeUsd ?? query.maxVolume),
      minPnlUsd: parseWalletExplorerNumber(query.minPnlUsd ?? query.minPnl),
      maxPnlUsd: parseWalletExplorerNumber(query.maxPnlUsd ?? query.maxPnl),
      minWinRate: parseWalletExplorerNumber(query.minWinRate),
      maxWinRate: parseWalletExplorerNumber(query.maxWinRate),
      minOpenPositions: parseWalletExplorerNumber(query.minOpenPositions),
      maxOpenPositions: parseWalletExplorerNumber(query.maxOpenPositions),
      minExposureUsd: parseWalletExplorerNumber(query.minExposureUsd ?? query.minPositionUsd),
      maxExposureUsd: parseWalletExplorerNumber(query.maxExposureUsd ?? query.maxPositionUsd),
      firstTradeFrom: parseWalletExplorerDate(query.firstTradeFrom, false),
      firstTradeTo: parseWalletExplorerDate(query.firstTradeTo, true),
      lastTradeFrom: parseWalletExplorerDate(query.lastTradeFrom, false),
      lastTradeTo: parseWalletExplorerDate(query.lastTradeTo, true),
    };
  }

  function buildWalletExplorerAppliedFilters(parsed) {
    const filters = [];
    if (parsed.q) filters.push({ key: "q", label: "Search", value: parsed.q });
    if (parsed.symbols.length) {
      parsed.symbols.forEach((symbol) => {
        filters.push({ key: "symbol", label: "Symbol", value: symbol });
      });
    }
    if (parsed.side) filters.push({ key: "side", label: "Side", value: parsed.side });
    if (parsed.status.length) {
      parsed.status.forEach((status) => {
        filters.push({ key: "status", label: "Status", value: status });
      });
    }
    if (parsed.openPositions) filters.push({ key: "openPositions", label: "Open Positions", value: parsed.openPositions });
    if (parsed.minTrades !== null) filters.push({ key: "minTrades", label: "Min Trades", value: String(parsed.minTrades) });
    if (parsed.maxTrades !== null) filters.push({ key: "maxTrades", label: "Max Trades", value: String(parsed.maxTrades) });
    if (parsed.minVolumeUsd !== null) filters.push({ key: "minVolumeUsd", label: "Min Volume", value: toCompact(parsed.minVolumeUsd) });
    if (parsed.maxVolumeUsd !== null) filters.push({ key: "maxVolumeUsd", label: "Max Volume", value: toCompact(parsed.maxVolumeUsd) });
    if (parsed.minPnlUsd !== null) filters.push({ key: "minPnlUsd", label: "Min PnL", value: toCompact(parsed.minPnlUsd) });
    if (parsed.maxPnlUsd !== null) filters.push({ key: "maxPnlUsd", label: "Max PnL", value: toCompact(parsed.maxPnlUsd) });
    if (parsed.minWinRate !== null) filters.push({ key: "minWinRate", label: "Min Win Rate", value: `${parsed.minWinRate}%` });
    if (parsed.maxWinRate !== null) filters.push({ key: "maxWinRate", label: "Max Win Rate", value: `${parsed.maxWinRate}%` });
    if (parsed.minOpenPositions !== null) filters.push({ key: "minOpenPositions", label: "Min Open", value: String(parsed.minOpenPositions) });
    if (parsed.maxOpenPositions !== null) filters.push({ key: "maxOpenPositions", label: "Max Open", value: String(parsed.maxOpenPositions) });
    if (parsed.minExposureUsd !== null) filters.push({ key: "minExposureUsd", label: "Min Exposure", value: toCompact(parsed.minExposureUsd) });
    if (parsed.maxExposureUsd !== null) filters.push({ key: "maxExposureUsd", label: "Max Exposure", value: toCompact(parsed.maxExposureUsd) });
    if (parsed.firstTradeFrom !== null) filters.push({ key: "firstTradeFrom", label: "First Trade From", value: new Date(parsed.firstTradeFrom).toISOString().slice(0, 10) });
    if (parsed.firstTradeTo !== null) filters.push({ key: "firstTradeTo", label: "First Trade To", value: new Date(parsed.firstTradeTo).toISOString().slice(0, 10) });
    if (parsed.lastTradeFrom !== null) filters.push({ key: "lastTradeFrom", label: "Last Trade From", value: new Date(parsed.lastTradeFrom).toISOString().slice(0, 10) });
    if (parsed.lastTradeTo !== null) filters.push({ key: "lastTradeTo", label: "Last Trade To", value: new Date(parsed.lastTradeTo).toISOString().slice(0, 10) });
    return filters;
  }

  async function refreshWalletExplorerDataset(force = false, snapshotSeed = null) {
    const now = Date.now();
    const walletStoreUpdatedAt = Number(
      walletStore && walletStore.data ? walletStore.data.updatedAt || 0 : 0
    );
    const snapshotForFreshness = snapshotSeed || getLiveWalletSnapshot(1);
    const liveSnapshotGeneratedAt = Number(
      snapshotForFreshness && snapshotForFreshness.generatedAt
        ? snapshotForFreshness.generatedAt
        : 0
    );
    const current = walletExplorerDatasetCache.value;
    const currentFresh =
      current &&
      !force &&
      now - Number(walletExplorerDatasetCache.generatedAt || 0) < walletExplorerDatasetCache.ttlMs &&
      walletStoreUpdatedAt <= Number(walletExplorerDatasetCache.walletStoreUpdatedAt || 0) &&
      liveSnapshotGeneratedAt <= Number(walletExplorerDatasetCache.liveSnapshotGeneratedAt || 0);
    if (currentFresh) return current;
    if (walletExplorerDatasetCache.inflight) return walletExplorerDatasetCache.inflight;

    walletExplorerDatasetCache.inflight = (async () => {
      const startedAt = Date.now();
      const walletRows = walletStore ? walletStore.list() : [];
      const liveSnapshot =
        snapshotSeed && Array.isArray(snapshotSeed.positions)
          ? snapshotSeed
          : getLiveWalletSnapshot(1500);
      const priceLookup = await getLivePositionPriceLookup();
      const livePositions =
        liveSnapshot && Array.isArray(liveSnapshot.positions) ? liveSnapshot.positions : [];
      const enrichedPositions = livePositions.map((row) =>
        enrichPositionRowWithMarketPrice(row, priceLookup)
      );
      const liveActiveRows = buildLiveActiveWalletRows({
        walletRows,
        positions: enrichedPositions,
        events:
          liveSnapshot && Array.isArray(liveSnapshot.events) ? liveSnapshot.events : [],
        nowMs: Date.now(),
      }).map((row, index) => ({
        ...row,
        rank: index + 1,
      }));

      const liveActiveMap = new Map(
        liveActiveRows.map((row) => [String(row.wallet || "").trim(), row])
      );
      const positionsByWallet = new Map();
      const unrealizedByWallet = new Map();
      const exposureByWallet = new Map();
      const openSideByWallet = new Map();
      const symbolUniverse = new Map();

      enrichedPositions.forEach((row) => {
        const wallet = String((row && row.wallet) || "").trim();
        if (!wallet) return;
        const list = positionsByWallet.get(wallet) || [];
        list.push(row);
        positionsByWallet.set(wallet, list);
        unrealizedByWallet.set(
          wallet,
          Number(
            (
              toNum(unrealizedByWallet.get(wallet), 0) +
              toNum(row && row.unrealizedPnlUsd, 0)
            ).toFixed(2)
          )
        );
        exposureByWallet.set(
          wallet,
          Number(
            (
              toNum(exposureByWallet.get(wallet), 0) +
              toNum(row && row.positionUsd, 0)
            ).toFixed(2)
          )
        );
        const sideSet = openSideByWallet.get(wallet) || new Set();
        const side = String((row && row.side) || "").trim().toLowerCase();
        if (side.includes("long") || side === "buy") sideSet.add("long");
        else if (side.includes("short") || side === "sell") sideSet.add("short");
        openSideByWallet.set(wallet, sideSet);
        const symbol = String((row && row.symbol) || "").trim().toUpperCase();
        if (symbol) {
          symbolUniverse.set(
            symbol,
            Number(
              (
                toNum(symbolUniverse.get(symbol), 0) +
                toNum(row && row.positionUsd, 0)
              ).toFixed(2)
            )
          );
        }
      });

      walletRows.forEach((record) => {
        const symbols = new Set();
        [record && record.all, record && record.d30, record && record.d7, record && record.d24].forEach((bucket) => {
          const volumes = bucket && typeof bucket.symbolVolumes === "object" ? bucket.symbolVolumes : {};
          Object.entries(volumes).forEach(([symbol, volumeUsd]) => {
            const normalized = String(symbol || "").trim().toUpperCase();
            if (!normalized) return;
            symbols.add(normalized);
            symbolUniverse.set(
              normalized,
              Number(
                (
                  toNum(symbolUniverse.get(normalized), 0) +
                  toNum(volumeUsd, 0)
                ).toFixed(2)
              )
            );
          });
        });
      });

      const preparedRows = walletRows.map((record) => {
        const wallet = String((record && record.wallet) || "").trim();
        const d24 = summarizeWalletWindow(record && record.d24);
        const d7 = summarizeWalletWindow(record && record.d7);
        const d30 = summarizeWalletWindow(record && record.d30);
        const all = summarizeWalletWindow(record && record.all);
        const liveActive = liveActiveMap.get(wallet) || {};
        const positions = positionsByWallet.get(wallet) || [];
        const openSides = openSideByWallet.get(wallet) || new Set();
        const sideState =
          openSides.has("long") && openSides.has("short")
            ? "mixed"
            : openSides.has("long")
            ? "long"
            : openSides.has("short")
            ? "short"
            : "none";
        const symbolSet = new Set([
          ...Object.keys(all.symbolVolumes || {}),
          ...Object.keys(d30.symbolVolumes || {}),
          ...Object.keys(d7.symbolVolumes || {}),
          ...Object.keys(d24.symbolVolumes || {}),
          ...positions.map((row) => String((row && row.symbol) || "").trim().toUpperCase()).filter(Boolean),
        ]);
        const lastActivity =
          Math.max(
            Number(liveActive.lastActivityAt || 0),
            Number(liveActive.lastPositionAt || 0),
            Number(record && record.updatedAt ? record.updatedAt : 0),
            Number(all.lastTrade || 0)
          ) || null;
        const unrealizedPnlUsd = Number(toNum(unrealizedByWallet.get(wallet), 0).toFixed(2));
        const realizedAllPnl = Number(toNum(all.pnlUsd, 0).toFixed(2));
        return {
          wallet,
          updatedAt: record && record.updatedAt ? record.updatedAt : null,
          d24,
          d7,
          d30,
          all,
          openPositions: Number(liveActive.openPositions || positions.length || 0),
          exposureUsd: Number(
            toNum(
              liveActive.positionUsd !== undefined ? liveActive.positionUsd : exposureByWallet.get(wallet),
              0
            ).toFixed(2)
          ),
          unrealizedPnlUsd,
          totalPnlUsd: Number((realizedAllPnl + unrealizedPnlUsd).toFixed(2)),
          liveActiveRank: liveActive.rank || null,
          liveActiveScore: Number(toNum(liveActive.activityScore, 0).toFixed(2)),
          recentEvents15m: Number(liveActive.recentEvents15m || 0),
          recentEvents1h: Number(liveActive.recentEvents1h || 0),
          lastActivity,
          freshness: buildWalletFreshnessLabel(lastActivity),
          sideState,
          hasLongOpen: openSides.has("long"),
          hasShortOpen: openSides.has("short"),
          symbols: Array.from(symbolSet)
            .map((item) => String(item || "").trim().toUpperCase())
            .filter(Boolean)
            .sort(),
          searchText: `${wallet} ${Array.from(symbolSet).join(" ")}`.toLowerCase(),
        };
      });

      const nextValue = {
        generatedAt: Date.now(),
        walletStoreUpdatedAt,
        walletRows,
        preparedRows,
        preparedRowsByWallet: new Map(preparedRows.map((row) => [row.wallet, row])),
        liveActiveRows,
        liveSnapshot,
        positions: enrichedPositions,
        positionsByWallet,
        availableSymbols: Array.from(symbolUniverse.entries())
          .sort((a, b) => b[1] - a[1])
          .slice(0, 250)
          .map(([symbol]) => symbol),
      };

      walletExplorerDatasetCache.value = nextValue;
      walletExplorerDatasetCache.generatedAt = nextValue.generatedAt;
      walletExplorerDatasetCache.walletStoreUpdatedAt = walletStoreUpdatedAt;
      walletExplorerDatasetCache.liveSnapshotGeneratedAt = Number(
        liveSnapshot && liveSnapshot.generatedAt ? liveSnapshot.generatedAt : 0
      );
      walletExplorerDatasetCache.lastError = null;
      walletExplorerDatasetCache.lastDurationMs = Date.now() - startedAt;
      return nextValue;
    })()
      .catch((error) => {
        walletExplorerDatasetCache.lastError = String(
          error && error.message ? error.message : error || "wallet_explorer_dataset_failed"
        );
        if (walletExplorerDatasetCache.value) return walletExplorerDatasetCache.value;
        throw error;
      })
      .finally(() => {
        walletExplorerDatasetCache.inflight = null;
      });

    return walletExplorerDatasetCache.inflight;
  }

  async function getWalletExplorerDataset(options = {}) {
    const force = Boolean(options.force);
    const now = Date.now();
    const current = walletExplorerDatasetCache.value;
    const walletStoreUpdatedAt = Number(
      walletStore && walletStore.data ? walletStore.data.updatedAt || 0 : 0
    );
    const liveSnapshot = getLiveWalletSnapshot(1);
    const liveSnapshotGeneratedAt = Number(
      liveSnapshot && liveSnapshot.generatedAt ? liveSnapshot.generatedAt : 0
    );
    const datasetAgeMs = Math.max(0, now - Number(walletExplorerDatasetCache.generatedAt || 0));
    const liveSnapshotAdvanced =
      liveSnapshotGeneratedAt > Number(walletExplorerDatasetCache.liveSnapshotGeneratedAt || 0);
    const walletStoreAdvanced =
      walletStoreUpdatedAt > Number(walletExplorerDatasetCache.walletStoreUpdatedAt || 0);
    const expired =
      !current ||
      force ||
      datasetAgeMs >= walletExplorerDatasetCache.ttlMs ||
      walletStoreAdvanced ||
      liveSnapshotAdvanced;
    if (!expired) return current;
    // Explorer should stay visually consistent with Live Trade when the live snapshot
    // or tracked-wallet store has advanced. Serve stale-on-refresh only for plain TTL expiry.
    if (current && !force && !liveSnapshotAdvanced && !walletStoreAdvanced) {
      refreshWalletExplorerDataset(true, liveSnapshot).catch(() => {});
      return current;
    }
    return refreshWalletExplorerDataset(true, liveSnapshot);
  }

  function queryWalletExplorerDataset(dataset, rawQuery = {}) {
    const parsed = parseWalletExplorerQuery(rawQuery);
    const timeframe = parsed.timeframe;
    const rows = Array.isArray(dataset && dataset.preparedRows) ? dataset.preparedRows : [];
    let filtered = rows;

    if (parsed.q) {
      filtered = filtered.filter((row) => String(row.searchText || "").includes(parsed.q));
    }

    if (parsed.symbols.length) {
      filtered = filtered.filter((row) =>
        parsed.symbols.some((symbol) => Array.isArray(row.symbols) && row.symbols.includes(symbol))
      );
    }

    if (parsed.side === "long") {
      filtered = filtered.filter((row) => row.hasLongOpen);
    } else if (parsed.side === "short") {
      filtered = filtered.filter((row) => row.hasShortOpen);
    } else if (parsed.side === "mixed") {
      filtered = filtered.filter((row) => row.sideState === "mixed");
    } else if (parsed.side === "flat") {
      filtered = filtered.filter((row) => !row.openPositions);
    }

    if (parsed.status.length) {
      filtered = filtered.filter((row) => parsed.status.includes(String(row.freshness || "").toLowerCase()));
    }

    if (parsed.openPositions === "has" || parsed.openPositions === "open") {
      filtered = filtered.filter((row) => Number(row.openPositions || 0) > 0);
    } else if (parsed.openPositions === "none" || parsed.openPositions === "closed") {
      filtered = filtered.filter((row) => Number(row.openPositions || 0) <= 0);
    }

    const applyRangeFilter = (getter, minValue, maxValue) => {
      if (minValue === null && maxValue === null) return;
      filtered = filtered.filter((row) => {
        const value = getter(row);
        if (minValue !== null && value < minValue) return false;
        if (maxValue !== null && value > maxValue) return false;
        return true;
      });
    };

    applyRangeFilter((row) => Number(pickWalletExplorerBucket(row, timeframe).trades || 0), parsed.minTrades, parsed.maxTrades);
    applyRangeFilter((row) => toNum(pickWalletExplorerBucket(row, timeframe).volumeUsd, 0), parsed.minVolumeUsd, parsed.maxVolumeUsd);
    applyRangeFilter((row) => toNum(pickWalletExplorerBucket(row, timeframe).pnlUsd, 0), parsed.minPnlUsd, parsed.maxPnlUsd);
    applyRangeFilter((row) => toNum(pickWalletExplorerBucket(row, timeframe).winRatePct, 0), parsed.minWinRate, parsed.maxWinRate);
    applyRangeFilter((row) => Number(row.openPositions || 0), parsed.minOpenPositions, parsed.maxOpenPositions);
    applyRangeFilter((row) => toNum(row.exposureUsd, 0), parsed.minExposureUsd, parsed.maxExposureUsd);

    if (parsed.firstTradeFrom !== null || parsed.firstTradeTo !== null) {
      filtered = filtered.filter((row) => {
        const ts = Number(pickWalletExplorerBucket(row, timeframe).firstTrade || 0);
        if (parsed.firstTradeFrom !== null && (!ts || ts < parsed.firstTradeFrom)) return false;
        if (parsed.firstTradeTo !== null && (!ts || ts > parsed.firstTradeTo)) return false;
        return true;
      });
    }

    if (parsed.lastTradeFrom !== null || parsed.lastTradeTo !== null) {
      filtered = filtered.filter((row) => {
        const ts = Number(pickWalletExplorerBucket(row, timeframe).lastTrade || row.lastActivity || 0);
        if (parsed.lastTradeFrom !== null && (!ts || ts < parsed.lastTradeFrom)) return false;
        if (parsed.lastTradeTo !== null && (!ts || ts > parsed.lastTradeTo)) return false;
        return true;
      });
    }

    const sortKey = parsed.sort;
    const sortDir = parsed.dir;
    const sortable = filtered.slice();
    sortable.sort((left, right) => {
      const leftBucket = pickWalletExplorerBucket(left, timeframe);
      const rightBucket = pickWalletExplorerBucket(right, timeframe);
      const getValue = (row, bucket) => {
        if (sortKey === "wallet") return String(row.wallet || "");
        if (sortKey === "trades") return Number(bucket.trades || 0);
        if (sortKey === "volumeUsd") return toNum(bucket.volumeUsd, 0);
        if (sortKey === "totalWins") return Number(bucket.wins || 0);
        if (sortKey === "totalLosses") return Number(bucket.losses || 0);
        if (sortKey === "pnlUsd") return toNum(bucket.pnlUsd, 0);
        if (sortKey === "winRate") return toNum(bucket.winRatePct, 0);
        if (sortKey === "firstTrade") return Number(bucket.firstTrade || 0);
        if (sortKey === "lastTrade") return Number(bucket.lastTrade || row.lastActivity || 0);
        if (sortKey === "openPositions") return Number(row.openPositions || 0);
        if (sortKey === "exposureUsd") return toNum(row.exposureUsd, 0);
        return toNum(bucket.volumeUsd, 0);
      };
      const a = getValue(left, leftBucket);
      const b = getValue(right, rightBucket);
      if (typeof a === "string" || typeof b === "string") {
        const cmp = String(a).localeCompare(String(b));
        if (cmp !== 0) return sortDir === "asc" ? cmp : -cmp;
      } else if (a !== b) {
        return sortDir === "asc" ? a - b : b - a;
      }
      return String(left.wallet || "").localeCompare(String(right.wallet || ""));
    });

    const total = sortable.length;
    const pages = Math.max(1, Math.ceil(total / parsed.pageSize));
    const page = Math.min(parsed.page, pages);
    const start = (page - 1) * parsed.pageSize;
    const pageRows = sortable.slice(start, start + parsed.pageSize).map((row, idx) => {
      const bucket = pickWalletExplorerBucket(row, timeframe);
      return {
        wallet: row.wallet,
        rank: start + idx + 1,
        trades: Number(bucket.trades || 0),
        volumeUsd: toFixed(toNum(bucket.volumeUsd, 0), 2),
        totalWins: Number(bucket.wins || 0),
        totalLosses: Number(bucket.losses || 0),
        pnlUsd: toFixed(toNum(bucket.pnlUsd, 0), 2),
        winRate: toFixed(toNum(bucket.winRatePct, 0), 2),
        firstTrade: bucket.firstTrade || null,
        lastTrade: bucket.lastTrade || null,
        updatedAt: row.updatedAt || null,
        lastActivity: row.lastActivity || null,
        openPositions: Number(row.openPositions || 0),
        exposureUsd: Number(toNum(row.exposureUsd, 0).toFixed(2)),
        unrealizedPnlUsd: Number(toNum(row.unrealizedPnlUsd, 0).toFixed(2)),
        totalPnlUsd: Number(toNum(row.totalPnlUsd, 0).toFixed(2)),
        liveActiveRank: row.liveActiveRank || null,
        liveActiveScore: Number(toNum(row.liveActiveScore, 0).toFixed(2)),
        recentEvents15m: Number(row.recentEvents15m || 0),
        recentEvents1h: Number(row.recentEvents1h || 0),
        freshness: row.freshness || "unknown",
        side: row.sideState || "none",
        d24: row.d24,
        d7: row.d7,
        d30: row.d30,
        all: row.all,
      };
    });

    return {
      generatedAt: Number(dataset && dataset.generatedAt ? dataset.generatedAt : Date.now()),
      timeframe,
      query: {
        q: parsed.q,
        page,
        pageSize: parsed.pageSize,
        sort: sortKey,
        dir: sortDir,
      },
      total,
      page,
      pageSize: parsed.pageSize,
      pages,
      rows: pageRows,
      filters: {
        applied: buildWalletExplorerAppliedFilters(parsed),
        availableSymbols: Array.isArray(dataset && dataset.availableSymbols)
          ? dataset.availableSymbols
          : [],
      },
      performance: {
        datasetBuiltAt: Number(dataset && dataset.generatedAt ? dataset.generatedAt : 0) || null,
        datasetAgeMs:
          dataset && dataset.generatedAt ? Math.max(0, Date.now() - Number(dataset.generatedAt || 0)) : null,
        datasetBuildMs: walletExplorerDatasetCache.lastDurationMs,
        stale: !dataset || Date.now() - Number(dataset.generatedAt || 0) >= walletExplorerDatasetCache.ttlMs,
        lastError: walletExplorerDatasetCache.lastError,
      },
    };
  }

  function mapWalletDetailPositions(rows = [], wallet) {
    const normalizedWallet = String(wallet || "").trim();
    return (Array.isArray(rows) ? rows : [])
      .filter((row) => String((row && row.wallet) || "").trim() === normalizedWallet)
      .map((row, index) => {
        const safeRow = row && typeof row === "object" ? row : {};
        const unrealizedPnlUsd =
          safeRow.unrealizedPnlUsd !== undefined
            ? toNum(safeRow.unrealizedPnlUsd, 0)
            : safeRow.unrealizedPnl !== undefined
            ? toNum(safeRow.unrealizedPnl, 0)
            : safeRow.pnl !== undefined
            ? toNum(safeRow.pnl, 0)
            : 0;
        return {
          id:
            safeRow.positionKey ||
            safeRow.historyId ||
            safeRow.orderId ||
            safeRow.li ||
            `${safeRow.symbol || "na"}:${safeRow.side || "na"}:${index}`,
          wallet: normalizedWallet,
          symbol: String((safeRow.symbol || "").trim() || "-").toUpperCase(),
          side: safeRow.side || null,
          size: Number(
            toNum(
              safeRow.size !== undefined
                ? safeRow.size
                : safeRow.qty !== undefined
                ? safeRow.qty
                : safeRow.amount,
              0
            ).toFixed(8)
          ),
          positionUsd: Number(
            toNum(
              safeRow.positionUsd !== undefined ? safeRow.positionUsd : safeRow.notionalUsd,
              0
            ).toFixed(2)
          ),
          entry: Number(
            toNum(
              safeRow.entry !== undefined ? safeRow.entry : safeRow.entryPrice,
              0
            ).toFixed(8)
          ),
          mark: Number(
            toNum(
              safeRow.mark !== undefined
                ? safeRow.mark
                : safeRow.markPrice !== undefined
                ? safeRow.markPrice
                : safeRow.currentPrice,
              0
            ).toFixed(8)
          ),
          unrealizedPnlUsd: Number(unrealizedPnlUsd.toFixed(2)),
          leverage: Number(
            toNum(safeRow.leverage, NaN).toFixed(Number.isFinite(toNum(safeRow.leverage, NaN)) ? 4 : 0)
          ) || null,
          roe:
            Number.isFinite(Number(safeRow.roe))
              ? Number(toNum(safeRow.roe, 0).toFixed(4))
              : null,
          liquidationPrice:
            Number.isFinite(Number(safeRow.liquidationPrice))
              ? Number(toNum(safeRow.liquidationPrice, 0).toFixed(8))
              : null,
          openedAt: safeRow.openedAt || safeRow.createdAt || null,
          updatedAt: safeRow.updatedAt || safeRow.timestamp || safeRow.lastTradeAt || null,
          freshness: safeRow.freshness || safeRow.status || "unknown",
          status: safeRow.status || null,
          source: safeRow.source || "wallet_first_positions",
          lifecycle:
            safeRow.lifecycle && typeof safeRow.lifecycle === "object" ? safeRow.lifecycle : null,
        };
      })
      .sort((a, b) => Number(b.updatedAt || 0) - Number(a.updatedAt || 0));
  }

  function buildLiveTradeWalletDetailPayload({
    wallet,
    livePayload,
    lifecycleSnapshot = null,
    supportSummary = null,
    priceLookup = null,
  }) {
    const normalizedWallet = String(wallet || "").trim();
    const payload = livePayload && typeof livePayload === "object" ? livePayload : {};
    const walletRows = Array.isArray(payload.walletPerformance) ? payload.walletPerformance : [];
    const positionsAll = Array.isArray(payload.positions) ? payload.positions : [];
    const liveActiveRows = Array.isArray(payload.liveActiveWallets) ? payload.liveActiveWallets : [];
    const record =
      walletRows.find((row) => String((row && row.wallet) || "").trim() === normalizedWallet) || null;
    const liveActive =
      liveActiveRows.find((row) => String((row && row.wallet) || "").trim() === normalizedWallet) ||
      null;
    const positions = mapWalletDetailPositions(
      positionsAll.map((row) => enrichPositionRowWithMarketPrice(row, priceLookup)),
      normalizedWallet
    );
    const d24 = summarizeWalletWindow(record && record.d24);
    const d7 = summarizeWalletWindow(record && record.d7);
    const d30 = summarizeWalletWindow(record && record.d30);
    const all = summarizeWalletWindow(record && record.all);
    const exposureUsd =
      liveActive && liveActive.positionUsd !== undefined
        ? toNum(liveActive.positionUsd, 0)
        : positions.reduce((sum, row) => sum + toNum(row.positionUsd, 0), 0);
    const unrealizedPnlUsd = positions.reduce(
      (sum, row) => sum + toNum(row.unrealizedPnlUsd, 0),
      0
    );
    const lastActivityAt = Math.max(
      Number((liveActive && liveActive.lastActivityAt) || 0),
      Number((liveActive && liveActive.lastPositionAt) || 0),
      Number(all.lastTrade || 0),
      Number(d30.lastTrade || 0),
      Number(d7.lastTrade || 0),
      Number(d24.lastTrade || 0),
      Number((record && record.updatedAt) || 0)
    );
    const freshness = buildWalletFreshnessLabel(lastActivityAt);
    const concentration = buildWalletConcentrationRows(
      all.symbolVolumes,
      all.volumeUsd
    );
    const found = Boolean(record) || Boolean(liveActive) || positions.length > 0;

    return {
      generatedAt: Date.now(),
      wallet: normalizedWallet,
      found,
      summary: {
        wallet: normalizedWallet,
        freshness,
        lastActivityAt: lastActivityAt || null,
        liveScannedAt: liveActive ? liveActive.liveScannedAt || null : null,
        liveActiveRank: liveActive ? liveActive.rank || null : null,
        liveActiveScore: liveActive ? toNum(liveActive.activityScore, 0) : 0,
        openPositions: liveActive ? Number(liveActive.openPositions || positions.length) : positions.length,
        exposureUsd: Number(exposureUsd.toFixed(2)),
        unrealizedPnlUsd: Number(unrealizedPnlUsd.toFixed(2)),
        recentEvents15m: liveActive ? Number(liveActive.recentEvents15m || 0) : 0,
        recentEvents1h: liveActive ? Number(liveActive.recentEvents1h || 0) : 0,
        d24,
        d7,
        d30,
        all,
        totalPnlUsd: Number((toNum(all.pnlUsd, 0) + unrealizedPnlUsd).toFixed(2)),
        concentration,
        lifecycle:
          lifecycleSnapshot && typeof lifecycleSnapshot === "object" ? lifecycleSnapshot : null,
      },
      positions,
      support: supportSummary || null,
    };
  }

  function buildWalletFreshnessDistribution(walletRows = [], nowMs = Date.now()) {
    const distribution = {
      fresh5m: 0,
      fresh15m: 0,
      stale: 0,
      unknown: 0,
    };
    (Array.isArray(walletRows) ? walletRows : []).forEach((row) => {
      const timestamp = Number((row && (row.liveScannedAt || row.updatedAt)) || 0);
      if (!Number.isFinite(timestamp) || timestamp <= 0) {
        distribution.unknown += 1;
        return;
      }
      const ageMs = Math.max(0, nowMs - timestamp);
      if (ageMs <= 5 * 60 * 1000) distribution.fresh5m += 1;
      else if (ageMs <= 15 * 60 * 1000) distribution.fresh15m += 1;
      else distribution.stale += 1;
    });
    return distribution;
  }

  function buildPositionFreshnessDistribution(positions = []) {
    const distribution = {
      fresh: 0,
      cooling: 0,
      stale: 0,
      unknown: 0,
    };
    (Array.isArray(positions) ? positions : []).forEach((row) => {
      const status = String((row && (row.freshness || row.status)) || "").trim().toLowerCase();
      if (status === "fresh") distribution.fresh += 1;
      else if (status === "cooling") distribution.cooling += 1;
      else if (status === "stale") distribution.stale += 1;
      else distribution.unknown += 1;
    });
    return distribution;
  }

  function buildLiveTradesPayload(options = {}, context = {}) {
    const dashboard = pipeline.getDashboardPayload();
    const market = dashboard.market || {};
    const account = dashboard.account || {};
    const priceLookup =
      context.priceLookup instanceof Map && context.priceLookup.size > 0
        ? context.priceLookup
        : buildMarketPriceLookup(market.pricesBySymbol || {});
    const retentionPublicTradesPerSymbolRaw = Number(
      process.env.PACIFICA_MAX_PUBLIC_TRADES_PER_SYMBOL || 0
    );
    const retentionPublicTradesPerSymbol =
      Number.isFinite(retentionPublicTradesPerSymbolRaw) &&
      retentionPublicTradesPerSymbolRaw > 0
        ? Math.max(50, Math.floor(retentionPublicTradesPerSymbolRaw))
        : null;
    const queryPublicOffset = parseWindowParam(options.publicOffset, 0);
    const queryPublicLimit = parseWindowParam(options.publicLimit, 0);
    const queryWalletOffset = parseWindowParam(options.walletOffset, 0);
    const queryWalletLimit = parseWindowParam(options.walletLimit, 0);
    const queryWalletSortRaw = String(options.walletSort || "pnlAll").trim();
    const queryWalletSort = {
      wallet: "wallet",
      pnl7: "pnl7Usd",
      realized7: "pnl7Usd",
      pnl30: "pnl30Usd",
      realized30: "pnl30Usd",
      pnlAll: "realizedPnlUsd",
      realizedAll: "realizedPnlUsd",
      open: "openPositions",
      openPositions: "openPositions",
      activity: "lastActivity",
      lastActivity: "lastActivity",
    }[queryWalletSortRaw] || "realizedPnlUsd";
    const queryWalletDir =
      String(options.walletDir || "desc").trim().toLowerCase() === "asc" ? "asc" : "desc";
    const queryPositionOffset = parseWindowParam(options.positionOffset, 0);
    const queryPositionLimit = parseWindowParam(options.positionLimit, 0);
    const queryPositionSortRaw = String(options.positionSort || "openedAt").trim();
    const queryPositionSort = {
      wallet: "wallet",
      symbol: "symbol",
      side: "side",
      size: "size",
      position: "positionUsd",
      positionUsd: "positionUsd",
      entry: "entry",
      mark: "mark",
      unrealized: "unrealized",
      pnl: "unrealized",
      activity: "openedAt",
      opened: "openedAt",
      openedAt: "openedAt",
      updated: "updatedAt",
      updatedAt: "updatedAt",
    }[queryPositionSortRaw] || "openedAt";
    const queryPositionDir =
      String(options.positionDir || "desc").trim().toLowerCase() === "asc" ? "asc" : "desc";
    const queryAccountTradeOffset = parseWindowParam(options.accountTradeOffset, 0);
    const queryAccountTradeLimit = parseWindowParam(options.accountTradeLimit, 0);
    const queryLiveActiveOffset = parseWindowParam(options.liveActiveOffset, 0);
    const queryLiveActiveLimit = parseWindowParam(
      options.liveActiveLimit,
      Number(process.env.PACIFICA_LIVE_ACTIVE_DEFAULT_LIMIT || 200)
    );
    const skipPublicTrades = parseFlagParam(options.skipPublicTrades, false);

    const publicTradesBySymbol =
      !skipPublicTrades &&
      market &&
      market.publicTradesBySymbol &&
      typeof market.publicTradesBySymbol === "object"
        ? market.publicTradesBySymbol
        : {};

    const publicTradesRaw = Object.entries(publicTradesBySymbol)
      .flatMap(([symbol, rows]) =>
        (Array.isArray(rows) ? rows : []).map((row) => {
          const wallet =
            row.wallet ||
            row.owner ||
            row.trader ||
            row.account ||
            row.authority ||
            row.user ||
            null;
          return {
            historyId: row.historyId || null,
            orderId: row.orderId || row.i || null,
            symbol: row.symbol || String(symbol || "").toUpperCase(),
            side: row.side || null,
            cause: row.cause || null,
            amount: row.amount || "0",
            price: row.price || "0",
            timestamp: Number(row.timestamp || 0),
            wallet,
            walletSource: wallet ? "payload" : "unresolved",
            walletConfidence: wallet ? "hard_payload" : "unresolved",
            li: row.li || null,
            tradeRef:
              row.historyId || row.orderId || row.i || row.li
                ? String(row.historyId || row.orderId || row.i || row.li)
                : null,
            tradeRefType: row.historyId
              ? "history_id"
              : row.orderId || row.i
              ? "order_id"
              : row.li
              ? "li"
              : "unknown",
            txSignature:
              row.txSignature ||
              row.tx_signature ||
              row.signature ||
              row.txid ||
              row.txId ||
              row.transactionId ||
              row.transaction_id ||
              null,
            txSource:
              row.txSignature ||
              row.tx_signature ||
              row.signature ||
              row.txid ||
              row.txId ||
              row.transactionId ||
              row.transaction_id
                ? "payload"
                : "unresolved",
            txConfidence:
              row.txSignature ||
              row.tx_signature ||
              row.signature ||
              row.txid ||
              row.txId ||
              row.transactionId ||
              row.transaction_id
                ? "hard_payload"
                : "unresolved",
          };
        })
      )
      .sort((a, b) => Number(b.timestamp || 0) - Number(a.timestamp || 0));

    if (liveTradeWalletAttributionEnabled) {
      const totalRows = publicTradesRaw.length;
      const queueLimit =
        liveTradeWalletAttributionQueuePerBuild > 0
          ? Math.min(liveTradeWalletAttributionQueuePerBuild, totalRows)
          : Math.min(6000, totalRows);
      let queuedForAttribution = 0;
      if (queueLimit > 0 && totalRows > 0) {
        let cursor = Number(liveTradeWalletAttribution.queueCursor || 0);
        if (!Number.isFinite(cursor) || cursor < 0) cursor = 0;
        cursor %= totalRows;
        const stride = Math.max(1, Math.floor(totalRows / queueLimit));
        for (let i = 0; i < queueLimit; i += 1) {
          const index = (cursor + i * stride) % totalRows;
          const row = publicTradesRaw[index];
          const hasJoinKey =
            row &&
            (row.historyId !== null && row.historyId !== undefined
              ? true
              : row.orderId !== null && row.orderId !== undefined
              ? true
              : row.li !== null && row.li !== undefined);
          if (row && !row.wallet && hasJoinKey) {
            if (queueLiveTradeHistoryIdForAttribution(row.historyId, row.li, row.orderId)) {
              queuedForAttribution += 1;
            }
          }
        }
        // Advance sample offset so subsequent requests sweep different subsets.
        liveTradeWalletAttribution.queueCursor = (cursor + 1) % totalRows;
      }
    }
    if (liveTradeTxAttributionEnabled) {
      runLiveTradeTxAttributionPass().catch(() => {});
    }

    const applyTxAttribution = (rowWithWallet) => {
      const next = rowWithWallet && typeof rowWithWallet === "object" ? { ...rowWithWallet } : {};
      const applyTriggerSignatureWallet = () => {
        const signature = normalizeTxSignature(next.txSignature);
        if (!signature || next.wallet) return false;
        const triggerWallet = liveTradeRecentSignatureWallets.get(signature);
        if (!triggerWallet || !triggerWallet.wallet) return false;
        next.wallet = String(triggerWallet.wallet);
        next.walletSource = "solana_log_trigger_signature";
        next.walletConfidence = `trigger_signature_${String(triggerWallet.confidence || "unknown")}`;
        return true;
      };
      if (next.txSignature) {
        if (!next.txSource) next.txSource = "payload";
        if (!next.txConfidence) next.txConfidence = "hard_payload";
        applyTriggerSignatureWallet();
        return next;
      }
      const txByHistory =
        next && next.historyId !== null && next.historyId !== undefined
          ? getLiveTradeTxAttributionByHistoryId(next.historyId)
          : null;
      if (txByHistory && txByHistory.signature) {
        next.txSignature = txByHistory.signature;
        next.txSource = txByHistory.source || "tx_attribution_history_id";
        next.txConfidence = txByHistory.confidence || "soft";
        if (!next.wallet && txByHistory.signer) {
          next.wallet = txByHistory.signer;
          next.walletSource = "tx_attribution_signer";
          next.walletConfidence = "soft_tx_signer";
        }
        applyTriggerSignatureWallet();
        return next;
      }
      const txByOrder = getLiveTradeTxAttributionByOrderId(next.orderId);
      if (txByOrder && txByOrder.signature) {
        next.txSignature = txByOrder.signature;
        next.txSource = txByOrder.source || "tx_attribution_order_id";
        next.txConfidence = txByOrder.confidence || "soft";
        if (!next.wallet && txByOrder.signer) {
          next.wallet = txByOrder.signer;
          next.walletSource = "tx_attribution_signer";
          next.walletConfidence = "soft_tx_signer";
        }
        applyTriggerSignatureWallet();
        return next;
      }
      const txByLi = getLiveTradeTxAttributionByLi(next.li);
      if (txByLi && txByLi.signature) {
        next.txSignature = txByLi.signature;
        next.txSource = txByLi.source || "tx_attribution_li";
        next.txConfidence = txByLi.confidence || "soft";
        if (!next.wallet && txByLi.signer) {
          next.wallet = txByLi.signer;
          next.walletSource = "tx_attribution_signer";
          next.walletConfidence = "soft_tx_signer";
        }
        applyTriggerSignatureWallet();
        return next;
      }
      if (!next.txSource) next.txSource = "unresolved";
      if (!next.txConfidence) next.txConfidence = "unresolved";
      return next;
    };

    const publicTradesAll = publicTradesRaw.map((row) => {
      const baseRow = row && typeof row === "object" ? { ...row } : {};
      if (baseRow && baseRow.wallet) {
        if (!baseRow.walletSource) baseRow.walletSource = "payload";
        if (!baseRow.walletConfidence) baseRow.walletConfidence = "hard_payload";
        return applyTxAttribution(baseRow);
      }
      const attributedWallet =
        baseRow && baseRow.historyId !== null && baseRow.historyId !== undefined
          ? getAttributedWalletByHistoryId(baseRow.historyId)
          : null;
      if (attributedWallet) {
        return applyTxAttribution({
          ...baseRow,
          wallet: attributedWallet,
          walletSource: "wallet_history_attribution_history_id",
          walletConfidence: "hard_history_id",
        });
      }
      const attributedByOrder = baseRow ? getAttributedWalletByOrderId(baseRow.orderId) : null;
      if (attributedByOrder) {
        return applyTxAttribution({
          ...baseRow,
          wallet: attributedByOrder,
          walletSource: "wallet_history_attribution_order_id",
          walletConfidence: "hard_order_id",
        });
      }
      const attributedByLi = baseRow ? getAttributedWalletByLi(baseRow.li) : null;
      if (!attributedByLi) {
        return applyTxAttribution({
          ...baseRow,
          walletSource: baseRow.walletSource || "unresolved",
          walletConfidence: baseRow.walletConfidence || "unresolved",
        });
      }
      return applyTxAttribution({
        ...baseRow,
        wallet: attributedByLi,
        walletSource: "wallet_history_attribution_li",
        walletConfidence: "fallback_li",
      });
    });
    const publicTradesWindow = applyWindow(publicTradesAll, {
      offset: queryPublicOffset,
      limit: queryPublicLimit,
    });
    publicTradesAll.forEach((row) => {
      if (!row || !row.wallet) return;
      recordLiveWalletTrigger({
        wallet: row.wallet,
        at: Number(row.timestamp || Date.now()),
        symbol: row.symbol || null,
        source: "public_trade_attributed",
        reason: "public_trade_attributed",
      });
    });

    const accountTradesAll = Array.isArray(dashboard.trades && dashboard.trades.recent)
      ? dashboard.trades.recent
      : [];
    const accountTradesWindow = applyWindow(accountTradesAll, {
      offset: queryAccountTradeOffset,
      limit: queryAccountTradeLimit,
    });

    const liveWalletSnapshot = getLiveWalletSnapshot(
      Math.max(200, queryPublicLimit || 200)
    );
    const now = Date.now();
    const liveWalletStatus =
      liveWalletSnapshot && liveWalletSnapshot.status && typeof liveWalletSnapshot.status === "object"
        ? liveWalletSnapshot.status
        : {
            enabled: false,
          };
    const walletFirstPositionsAll =
      liveWalletSnapshot && Array.isArray(liveWalletSnapshot.positions)
        ? liveWalletSnapshot.positions
        : [];
    const positionsBase = walletFirstPositionsAll.length
      ? walletFirstPositionsAll
      : Array.isArray(dashboard.positions && dashboard.positions.rows)
      ? dashboard.positions.rows
      : [];
    const positionsAll = Array.isArray(positionsBase)
      ? positionsBase.map((row) => {
          const enriched = enrichPositionRowWithMarketPrice(row, priceLookup);
          const lifecycle =
            positionLifecycleStore &&
            typeof positionLifecycleStore.getPositionSummary === "function"
              ? positionLifecycleStore.getPositionSummary(
                  enriched && enriched.wallet,
                  enriched && enriched.positionKey,
                  enriched,
                  now
                )
              : null;
          return lifecycle ? { ...enriched, lifecycle } : enriched;
        })
      : [];
    const comparePositionRows = (left, right) => {
      const numericValueFor = (row, key) => {
        if (!row || typeof row !== "object") return NaN;
        if (key === "unrealized") {
          return toNum(
            row.unrealizedPnlUsd !== undefined ? row.unrealizedPnlUsd : row.pnl,
            NaN
          );
        }
        return toNum(row[key], NaN);
      };
      const stringValueFor = (row, key) => {
        if (!row || typeof row !== "object") return "";
        if (key === "wallet") {
          return String(row.wallet || row.walletLabel || row.walletTitle || "").trim().toLowerCase();
        }
        return String(row[key] || "").trim().toLowerCase();
      };

      const numericKeys = new Set([
        "size",
        "positionUsd",
        "entry",
        "mark",
        "unrealized",
        "activity",
        "openedAt",
        "updatedAt",
      ]);
      if (numericKeys.has(queryPositionSort)) {
        const leftVal = numericValueFor(left, queryPositionSort);
        const rightVal = numericValueFor(right, queryPositionSort);
        const leftMissing = !Number.isFinite(leftVal);
        const rightMissing = !Number.isFinite(rightVal);
        if (leftMissing && rightMissing) {
          return (
            Number(right.openedAt || 0) - Number(left.openedAt || 0) ||
            Number(right.updatedAt || 0) - Number(left.updatedAt || 0)
          );
        }
        if (leftMissing) return 1;
        if (rightMissing) return -1;
        if (leftVal === rightVal) {
          return (
            Number(right.openedAt || 0) - Number(left.openedAt || 0) ||
            Number(right.updatedAt || 0) - Number(left.updatedAt || 0)
          );
        }
        return queryPositionDir === "asc" ? leftVal - rightVal : rightVal - leftVal;
      }

      const leftVal = stringValueFor(left, queryPositionSort);
      const rightVal = stringValueFor(right, queryPositionSort);
      const cmp = leftVal.localeCompare(rightVal);
      if (cmp === 0) {
        return (
          Number(right.openedAt || 0) - Number(left.openedAt || 0) ||
          Number(right.updatedAt || 0) - Number(left.updatedAt || 0)
        );
      }
      return queryPositionDir === "asc" ? cmp : -cmp;
    };
    positionsAll.sort(comparePositionRows);
    const positionsWindow = applyWindow(positionsAll, {
      offset: queryPositionOffset,
      limit: queryPositionLimit,
    });

    const walletRowsBase = walletStore ? walletStore.list() : [];
    const walletRowsSeed = Array.isArray(walletRowsBase)
      ? walletRowsBase.map((row) => {
          const lifecycle =
            positionLifecycleStore &&
            typeof positionLifecycleStore.getWalletSummary === "function"
              ? positionLifecycleStore.getWalletSummary(row && row.wallet)
              : null;
          return lifecycle ? { ...row, lifecycle } : row;
        })
      : [];
    const oneMinuteAgo = now - 60 * 1000;
    const recentTrades = [];
    for (const row of publicTradesAll) {
      const ts = Number(row && row.timestamp ? row.timestamp : 0);
      if (!ts || ts < oneMinuteAgo) break;
      recentTrades.push(row);
    }
    const longEvents = recentTrades.filter((row) => {
      const side = String(row.side || "").toLowerCase();
      return side.includes("long") || side === "buy";
    }).length;
    const shortEvents = recentTrades.filter((row) => {
      const side = String(row.side || "").toLowerCase();
      return side.includes("short") || side === "sell";
    }).length;
    const activeSymbolsFromPublic = Object.keys(publicTradesBySymbol)
      .map((symbol) => String(symbol || "").trim().toUpperCase())
      .filter(Boolean).length;
    const activeSymbolsFromPositions = new Set(
      positionsAll
        .map((row) => String((row && row.symbol) || "").trim().toUpperCase())
        .filter(Boolean)
    ).size;
    const activeSymbols = activeSymbolsFromPublic || activeSymbolsFromPositions;
    const whaleEvents = recentTrades.filter((row) => {
      const amount = Number(row.amount || 0);
      const price = Number(row.price || 0);
      return Number.isFinite(amount) && Number.isFinite(price) && amount * price >= 100000;
    }).length;
    const fundingChips = Array.isArray(market.topFunding)
      ? market.topFunding.slice(0, 8).map((row) => ({
          symbol: String(row.symbol || "").toUpperCase(),
          funding: Number(row.funding || 0),
        }))
      : [];
    const walletFirstEvents = liveWalletSnapshot && Array.isArray(liveWalletSnapshot.events)
      ? liveWalletSnapshot.events
      : [];
    const liveActiveWalletRows = buildLiveActiveWalletRows({
      walletRows: walletRowsSeed,
      positions: positionsAll,
      events: walletFirstEvents,
      nowMs: now,
    });
    const unrealizedByWallet = new Map();
    positionsAll.forEach((position) => {
      const wallet = String((position && position.wallet) || "").trim();
      if (!wallet) return;
      unrealizedByWallet.set(
        wallet,
        Number(
          (
            toNum(unrealizedByWallet.get(wallet), 0) +
            toNum(position && position.unrealizedPnlUsd, 0)
          ).toFixed(2)
        )
      );
    });
    const liveActiveMap = new Map(
      liveActiveWalletRows
        .filter((row) => row && row.wallet)
        .map((row) => [String(row.wallet).trim(), row])
    );
    const walletRowsAll = walletRowsSeed.map((row) => {
      const wallet = String((row && row.wallet) || "").trim();
      const liveActive = liveActiveMap.get(wallet) || null;
      const pnl24Usd = Number(
        toNum(
          row &&
            row.d24 &&
            (row.d24.pnlUsd !== undefined ? row.d24.pnlUsd : row.d24.realizedPnlUsd),
          NaN
        ).toFixed(
          Number.isFinite(
            toNum(
              row &&
                row.d24 &&
                (row.d24.pnlUsd !== undefined ? row.d24.pnlUsd : row.d24.realizedPnlUsd),
              NaN
            )
          )
            ? 2
            : 0
        )
      );
      const pnl7Usd = Number(
        toNum(
          row &&
            row.d7 &&
            (row.d7.pnlUsd !== undefined ? row.d7.pnlUsd : row.d7.realizedPnlUsd),
          NaN
        ).toFixed(
          Number.isFinite(
            toNum(
              row &&
                row.d7 &&
                (row.d7.pnlUsd !== undefined ? row.d7.pnlUsd : row.d7.realizedPnlUsd),
              NaN
            )
          )
            ? 2
            : 0
        )
      );
      const pnl30Usd = Number(
        toNum(
          row &&
            row.d30 &&
            (row.d30.pnlUsd !== undefined ? row.d30.pnlUsd : row.d30.realizedPnlUsd),
          NaN
        ).toFixed(
          Number.isFinite(
            toNum(
              row &&
                row.d30 &&
                (row.d30.pnlUsd !== undefined ? row.d30.pnlUsd : row.d30.realizedPnlUsd),
              NaN
            )
          )
            ? 2
            : 0
        )
      );
      const realizedPnlUsd = Number(
        toNum(
          row &&
            row.all &&
            (row.all.pnlUsd !== undefined ? row.all.pnlUsd : row.all.realizedPnlUsd) !== undefined
            ? row.all.pnlUsd !== undefined
              ? row.all.pnlUsd
              : row.all.realizedPnlUsd
            : row && row.realizedPnlUsd !== undefined
            ? row.realizedPnlUsd
            : row && row.pnlUsd,
          NaN
        ).toFixed(
          Number.isFinite(
            toNum(
              row &&
                row.all &&
                (row.all.pnlUsd !== undefined ? row.all.pnlUsd : row.all.realizedPnlUsd) !== undefined
                ? row.all.pnlUsd !== undefined
                  ? row.all.pnlUsd
                  : row.all.realizedPnlUsd
                : row && row.realizedPnlUsd !== undefined
                ? row.realizedPnlUsd
                : row && row.pnlUsd,
              NaN
            )
          )
            ? 2
            : 0
        )
      );
      const historicalLastActivity = Math.max(
        Number(row && row.lastActivity ? row.lastActivity : 0),
        Number(row && row.lastTrade ? row.lastTrade : 0),
        Number(row && row.updatedAt ? row.updatedAt : 0),
        Number(row && row.all && row.all.lastTrade ? row.all.lastTrade : 0),
        Number(row && row.d30 && row.d30.lastTrade ? row.d30.lastTrade : 0),
        Number(row && row.d7 && row.d7.lastTrade ? row.d7.lastTrade : 0),
        Number(row && row.d24 && row.d24.lastTrade ? row.d24.lastTrade : 0)
      );
      const liveLastActivity = Math.max(
        Number(liveActive && liveActive.lastActivityAt ? liveActive.lastActivityAt : 0),
        Number(liveActive && liveActive.lastPositionAt ? liveActive.lastPositionAt : 0),
        Number(liveActive && liveActive.liveScannedAt ? liveActive.liveScannedAt : 0)
      );
      const lastActivity = Math.max(historicalLastActivity, liveLastActivity);
      const openPositions = Number(liveActive && liveActive.openPositions ? liveActive.openPositions : 0);
      const exposureUsd = Number(
        toNum(liveActive && liveActive.positionUsd !== undefined ? liveActive.positionUsd : 0, 0).toFixed(2)
      );
      const unrealizedPnlUsd = Number(toNum(unrealizedByWallet.get(wallet), 0).toFixed(2));
      return {
        ...row,
        pnl24Usd,
        pnl7Usd,
        pnl30Usd,
        realizedPnlUsd,
        openPositions,
        exposureUsd,
        unrealizedPnlUsd,
        totalPnlUsd: Number(
          (
            toNum(
              Number.isFinite(realizedPnlUsd) ? realizedPnlUsd : 0,
              0
            ) + unrealizedPnlUsd
          ).toFixed(2)
        ),
        liveActiveRank: liveActive ? liveActive.rank || null : null,
        liveActiveScore: liveActive ? Number(toNum(liveActive.activityScore, 0).toFixed(2)) : 0,
        recentEvents15m: liveActive ? Number(liveActive.recentEvents15m || 0) : 0,
        recentEvents1h: liveActive ? Number(liveActive.recentEvents1h || 0) : 0,
        lastActivity: lastActivity || null,
        freshness: buildWalletFreshnessLabel(lastActivity, now),
      };
    });
    const compareWalletRows = (left, right) => {
      const stringValueFor = (row, key) => {
        if (!row || typeof row !== "object") return "";
        if (key === "wallet") {
          return String(row.walletLabel || row.wallet || "").trim().toLowerCase();
        }
        return String(row[key] || "").trim().toLowerCase();
      };
      const numericKeys = new Set(["pnl7Usd", "pnl30Usd", "realizedPnlUsd", "openPositions", "lastActivity"]);
      if (numericKeys.has(queryWalletSort)) {
        const leftVal = toNum(left && left[queryWalletSort], NaN);
        const rightVal = toNum(right && right[queryWalletSort], NaN);
        const leftMissing = !Number.isFinite(leftVal);
        const rightMissing = !Number.isFinite(rightVal);
        if (leftMissing && rightMissing) {
          return Number(right && right.lastActivity ? right.lastActivity : 0) - Number(left && left.lastActivity ? left.lastActivity : 0);
        }
        if (leftMissing) return 1;
        if (rightMissing) return -1;
        if (leftVal === rightVal) {
          return Number(right && right.lastActivity ? right.lastActivity : 0) - Number(left && left.lastActivity ? left.lastActivity : 0);
        }
        return queryWalletDir === "asc" ? leftVal - rightVal : rightVal - leftVal;
      }
      const leftVal = stringValueFor(left, queryWalletSort);
      const rightVal = stringValueFor(right, queryWalletSort);
      const cmp = leftVal.localeCompare(rightVal);
      if (cmp === 0) {
        return Number(right && right.lastActivity ? right.lastActivity : 0) - Number(left && left.lastActivity ? left.lastActivity : 0);
      }
      return queryWalletDir === "asc" ? cmp : -cmp;
    };
    walletRowsAll.sort(compareWalletRows);
    const walletRowsWindow = applyWindow(walletRowsAll, {
      offset: queryWalletOffset,
      limit: queryWalletLimit,
    });
    const liveActiveWalletWindow = applyWindow(liveActiveWalletRows, {
      offset: queryLiveActiveOffset,
      limit: queryLiveActiveLimit,
    });
    const pacificaPublicActive = getPacificaPublicActiveSnapshot();
    const positionsWithWallet = positionsAll.filter((row) => Boolean(row && row.wallet)).length;
    const positionsWalletCoveragePct = positionsAll.length
      ? Number(((positionsWithWallet / positionsAll.length) * 100).toFixed(2))
      : 0;
    const hotWsUtilizationPct =
      liveWalletStatus && Number(liveWalletStatus.hotWsCapacity || 0) > 0
        ? Number(
            (
              (Number(liveWalletStatus.hotWsActiveWallets || 0) /
                Math.max(1, Number(liveWalletStatus.hotWsCapacity || 0))) *
              100
            ).toFixed(2)
          )
        : null;
    const publicActiveCoveragePct =
      Number(pacificaPublicActive && pacificaPublicActive.activeWalletsLowerBound) > 0
        ? Number(
            (
              (Number((liveWalletStatus && liveWalletStatus.walletsWithOpenPositions) || 0) /
                Math.max(1, Number(pacificaPublicActive.activeWalletsLowerBound || 0))) *
              100
            ).toFixed(2)
          )
        : null;
    const reconciliationLagMs =
      liveWalletStatus && liveWalletStatus.lastPassFinishedAt
        ? Math.max(0, now - Number(liveWalletStatus.lastPassFinishedAt || 0))
        : null;
    const walletFreshnessDistribution = buildWalletFreshnessDistribution(walletRowsAll, now);
    const positionFreshnessDistribution = buildPositionFreshnessDistribution(positionsAll);
    const walletMetricsHistoryStatus =
      walletMetricsHistoryStore &&
      typeof walletMetricsHistoryStore.getStatus === "function"
        ? walletMetricsHistoryStore.getStatus()
        : { enabled: false };
    const positionLifecycleStatus =
      positionLifecycleStore &&
      typeof positionLifecycleStore.getStatus === "function"
        ? positionLifecycleStore.getStatus()
        : { enabled: false };
    const walletPerformanceSupport = buildWalletPerformanceSupportSummary({
      walletMetricsHistoryStatus,
      positionLifecycleStatus,
    });
    const recentWalletEvents = walletFirstEvents.filter((row) => {
      const ts = Number(row && row.timestamp ? row.timestamp : 0);
      return ts > 0 && ts >= oneMinuteAgo;
    }).length;
    const publicTradesWithWallet = publicTradesAll.filter((row) => Boolean(row && row.wallet)).length;
    const publicTradesUnresolved = Math.max(0, publicTradesAll.length - publicTradesWithWallet);
    const attributionTiers = {
      hard_payload: 0,
      hard_history_id: 0,
      hard_order_id: 0,
      fallback_li: 0,
      soft_tx_signer: 0,
      unresolved: 0,
    };
    publicTradesAll.forEach((row) => {
      const confidenceRaw = row && row.walletConfidence ? String(row.walletConfidence) : "";
      const confidence =
        confidenceRaw && Object.prototype.hasOwnProperty.call(attributionTiers, confidenceRaw)
          ? confidenceRaw
          : row && row.wallet
          ? "hard_payload"
          : "unresolved";
      attributionTiers[confidence] += 1;
    });
    const txAttributionTiers = {
      hard_payload: 0,
      hard_wallet_history: 0,
      soft_onchain_log_match: 0,
      unresolved: 0,
    };
    let publicTradesWithTxSignature = 0;
    publicTradesAll.forEach((row) => {
      const signature = String((row && row.txSignature) || "").trim();
      if (signature) publicTradesWithTxSignature += 1;
      const confidenceRaw = row && row.txConfidence ? String(row.txConfidence).trim().toLowerCase() : "";
      const confidence =
        confidenceRaw && Object.prototype.hasOwnProperty.call(txAttributionTiers, confidenceRaw)
          ? confidenceRaw
          : signature
          ? "soft_onchain_log_match"
          : "unresolved";
      txAttributionTiers[confidence] += 1;
    });
    const publicTradesTxCoveragePct = publicTradesAll.length
      ? Number(((publicTradesWithTxSignature / publicTradesAll.length) * 100).toFixed(2))
      : 0;
    const publicTradesWalletCoveragePct = publicTradesAll.length
      ? Number(((publicTradesWithWallet / publicTradesAll.length) * 100).toFixed(2))
      : 0;

    return {
      generatedAt: now,
      sync: dashboard.sync || {},
      environment: dashboard.environment || {},
      summary: {
        publicTrades: publicTradesWindow.returned,
        publicTradesTotal: publicTradesWindow.total,
        publicTradesWindowed: publicTradesWindow.windowedByQuery,
        publicTradesHasMore: publicTradesWindow.hasMore,
        publicTradesOffset: publicTradesWindow.offset,
        publicTradesLimit: publicTradesWindow.limit,
        accountTrades: accountTradesWindow.returned,
        accountTradesTotal: accountTradesWindow.total,
        accountTradesWindowed: accountTradesWindow.windowedByQuery,
        accountTradesHasMore: accountTradesWindow.hasMore,
        accountTradesOffset: accountTradesWindow.offset,
        accountTradesLimit: accountTradesWindow.limit,
        openPositions: positionsWindow.returned,
        openPositionsTotal: positionsWindow.total,
        openPositionsWindowed: positionsWindow.windowedByQuery,
        openPositionsHasMore: positionsWindow.hasMore,
        openPositionsOffset: positionsWindow.offset,
        openPositionsLimit: positionsWindow.limit,
        openPositionsSort: queryPositionSort,
        openPositionsDir: queryPositionDir,
        indexedWallets: walletRowsWindow.returned,
        indexedWalletsTotal: walletRowsAll.length,
        indexedWalletsWindowed: walletRowsWindow.windowedByQuery,
        indexedWalletsHasMore: walletRowsWindow.hasMore,
        indexedWalletsOffset: walletRowsWindow.offset,
        indexedWalletsLimit: walletRowsWindow.limit,
        indexedWalletsSort: queryWalletSort,
        indexedWalletsDir: queryWalletDir,
        liveActiveWallets: liveActiveWalletWindow.returned,
        liveActiveWalletsTotal: liveActiveWalletWindow.total,
        liveActiveWalletsWindowed: liveActiveWalletWindow.windowedByQuery,
        liveActiveWalletsHasMore: liveActiveWalletWindow.hasMore,
        liveActiveWalletsOffset: liveActiveWalletWindow.offset,
        liveActiveWalletsLimit: liveActiveWalletWindow.limit,
        walletPerformanceSupport,
        wsStatus:
          (dashboard.environment && dashboard.environment.wsStatus) ||
          (dashboard.sync && dashboard.sync.wsStatus) ||
          "idle",
        streamScope:
          liveWalletStatus && liveWalletStatus.enabled
            ? "wallet_first_tracked_wallet_positions"
            : "exchange_wide_public_trades",
        skipPublicTrades,
        retentionPublicTradesPerSymbol,
        publicTradesWithWallet,
        publicTradesUnresolved,
        publicTradesWalletCoveragePct,
        publicTradesWithTxSignature,
        publicTradesTxCoveragePct,
        walletFirstLive: liveWalletStatus,
        walletAttribution: {
          enabled: liveTradeWalletAttributionEnabled,
          source: "wallet_history_by_history_id_or_order_id_or_li",
          resolvedHistoryCacheSize: liveTradeWalletAttribution.historyToWallet.size,
          resolvedOrderCacheSize: liveTradeWalletAttribution.orderToWallet.size,
          resolvedLiCacheSize: liveTradeWalletAttribution.liToWallet.size,
          triggerSignatureCacheSize: liveTradeRecentSignatureWallets.size,
          pendingHistoryIds: liveTradeWalletAttribution.pendingHistoryIds.size,
          pendingOrderIds: liveTradeWalletAttribution.pendingOrderIds.size,
          pendingLiIds: liveTradeWalletAttribution.pendingLiIds.size,
          scanPasses: liveTradeWalletAttribution.scanPasses,
          scannedFiles: liveTradeWalletAttribution.scannedFiles,
          resolved: liveTradeWalletAttribution.resolved,
          resolvedByOrder: liveTradeWalletAttribution.resolvedByOrder,
          resolvedByLi: liveTradeWalletAttribution.resolvedByLi,
          tiers: attributionTiers,
          coveragePct: publicTradesWalletCoveragePct,
        },
        txAttribution: {
          enabled: liveTradeTxAttributionEnabled,
          programId: pacificaProgramId || null,
          source: "solana_rpc_program_logs_or_payload",
          tiers: txAttributionTiers,
          coveragePct: publicTradesTxCoveragePct,
          resolvedHistoryCacheSize: liveTradeTxAttribution.historyToTx.size,
          resolvedOrderCacheSize: liveTradeTxAttribution.orderToTx.size,
          resolvedLiCacheSize: liveTradeTxAttribution.liToTx.size,
          pendingHistoryIds: liveTradeTxAttribution.pendingHistoryIds.size,
          pendingOrderIds: liveTradeTxAttribution.pendingOrderIds.size,
          pendingLiIds: liveTradeTxAttribution.pendingLiIds.size,
          scanPasses: liveTradeTxAttribution.scanPasses,
          rpcCalls: liveTradeTxAttribution.rpcCalls,
          txFetched: liveTradeTxAttribution.txFetched,
          txMatchedByHistory: liveTradeTxAttribution.txMatchedByHistory,
          txMatchedByOrder: liveTradeTxAttribution.txMatchedByOrder,
          txMatchedByLi: liveTradeTxAttribution.txMatchedByLi,
        },
        attributionDb: liveTradeAttributionStore
          ? {
              enabled: true,
              ...liveTradeAttributionStore.getStatus(),
              growth: liveTradeAttributionStore.getGrowthSnapshot(),
            }
          : {
              enabled: false,
            },
        observability: {
          hotTier: {
            capacity: Number((liveWalletStatus && liveWalletStatus.hotWsCapacity) || 0),
            capacityCeiling: Number((liveWalletStatus && liveWalletStatus.hotWsCapacityCeiling) || 0),
            activeWallets: Number((liveWalletStatus && liveWalletStatus.hotWsActiveWallets) || 0),
            openConnections: Number((liveWalletStatus && liveWalletStatus.hotWsOpenConnections) || 0),
            availableSlots: Number((liveWalletStatus && liveWalletStatus.hotWsAvailableSlots) || 0),
            droppedPromotions: Number(
              (liveWalletStatus && liveWalletStatus.hotWsDroppedPromotions) || 0
            ),
            promotionBacklog: Number(
              (liveWalletStatus && liveWalletStatus.hotWsPromotionBacklog) || 0
            ),
            utilizationPct: hotWsUtilizationPct,
            scaleEvents: Number((liveWalletStatus && liveWalletStatus.hotWsScaleEvents) || 0),
            processRssMb:
              liveWalletStatus && liveWalletStatus.hotWsProcessRssMb !== undefined
                ? Number(liveWalletStatus.hotWsProcessRssMb)
                : null,
            triggerToEventAvgMs:
              liveWalletStatus && liveWalletStatus.hotWsTriggerToEventAvgMs !== undefined
                ? Number(liveWalletStatus.hotWsTriggerToEventAvgMs)
                : null,
            triggerToEventLastMs:
              liveWalletStatus && liveWalletStatus.hotWsLastTriggerToEventMs !== undefined
                ? Number(liveWalletStatus.hotWsLastTriggerToEventMs)
                : null,
            reconnectTransitions:
              liveWalletStatus && liveWalletStatus.hotWsReconnectTransitions !== undefined
                ? Number(liveWalletStatus.hotWsReconnectTransitions)
                : null,
            errorCount:
              liveWalletStatus && liveWalletStatus.hotWsErrorCount !== undefined
                ? Number(liveWalletStatus.hotWsErrorCount)
                : null,
          },
          reconciliation: {
            estimatedSweepSeconds:
              liveWalletStatus &&
              liveWalletStatus.estimatedSweepSeconds !== undefined &&
              liveWalletStatus.estimatedSweepSeconds !== null
                ? Number(liveWalletStatus.estimatedSweepSeconds)
                : null,
            estimatedHotLoopSeconds:
              liveWalletStatus &&
              liveWalletStatus.estimatedHotLoopSeconds !== undefined &&
              liveWalletStatus.estimatedHotLoopSeconds !== null
                ? Number(liveWalletStatus.estimatedHotLoopSeconds)
                : null,
            estimatedWarmLoopSeconds:
              liveWalletStatus &&
              liveWalletStatus.estimatedWarmLoopSeconds !== undefined &&
              liveWalletStatus.estimatedWarmLoopSeconds !== null
                ? Number(liveWalletStatus.estimatedWarmLoopSeconds)
                : null,
            reconciliationLagMs,
            walletsCoveragePct:
              liveWalletStatus &&
              liveWalletStatus.walletsCoveragePct !== undefined &&
              liveWalletStatus.walletsCoveragePct !== null
                ? Number(liveWalletStatus.walletsCoveragePct)
                : null,
            priorityQueueDepth:
              liveWalletStatus && liveWalletStatus.priorityQueueDepth !== undefined
                ? Number(liveWalletStatus.priorityQueueDepth)
                : null,
            hotReconcileDueWallets:
              liveWalletStatus && liveWalletStatus.hotReconcileDueWallets !== undefined
                ? Number(liveWalletStatus.hotReconcileDueWallets)
                : null,
            warmReconcileDueWallets:
              liveWalletStatus && liveWalletStatus.warmReconcileDueWallets !== undefined
                ? Number(liveWalletStatus.warmReconcileDueWallets)
                : null,
          },
          freshness: {
            wallets: walletFreshnessDistribution,
            positions: positionFreshnessDistribution,
            lifecycle:
              liveWalletStatus
                ? {
                    hot: Number(liveWalletStatus.lifecycleHotWallets || 0),
                    warm: Number(liveWalletStatus.lifecycleWarmWallets || 0),
                    cold: Number(liveWalletStatus.lifecycleColdWallets || 0),
                    fresh: Number(liveWalletStatus.freshWallets || 0),
                    cooling: Number(liveWalletStatus.coolingWallets || 0),
                    stale: Number(liveWalletStatus.staleWallets || 0),
                  }
                : null,
          },
          publicActiveCoverage: {
            ...pacificaPublicActive,
            activeCoveragePct: publicActiveCoveragePct,
          },
          openPositionAttributionCoveragePct: positionsWalletCoveragePct,
          transport:
            liveWalletStatus
              ? {
                  healthyClients: Number(liveWalletStatus.healthyClients || 0),
                  avgClientLatencyMs: Number(liveWalletStatus.avgClientLatencyMs || 0),
                  timeoutClients: Number(liveWalletStatus.timeoutClients || 0),
                  proxyFailingClients: Number(liveWalletStatus.proxyFailingClients || 0),
                  clientsCooling: Number(liveWalletStatus.clientsCooling || 0),
                  clientsDisabled: Number(liveWalletStatus.clientsDisabled || 0),
                  clients429: Number(liveWalletStatus.clients429 || 0),
                }
              : null,
          triggers:
            solanaLogTriggerMonitor && typeof solanaLogTriggerMonitor.getStatus === "function"
              ? solanaLogTriggerMonitor.getStatus()
              : { enabled: false },
          history: {
            walletMetrics: walletMetricsHistoryStatus,
            positionLifecycle: positionLifecycleStatus,
          },
        },
      },
      marketContext: {
        fundingChips,
        longShortRatio: shortEvents > 0 ? Number((longEvents / shortEvents).toFixed(3)) : null,
        eventsPerMin:
          liveWalletStatus && liveWalletStatus.enabled ? recentWalletEvents : recentTrades.length,
        whaleEvents,
        activeSymbols,
        lastEventAt: Math.max(
          publicTradesAll.length ? Number(publicTradesAll[0].timestamp || 0) : 0,
          Number((liveWalletStatus && liveWalletStatus.lastEventAt) || 0)
        ) || null,
      },
      publicTrades: publicTradesWindow.rows,
      accountTrades: accountTradesWindow.rows,
      positions: positionsWindow.rows,
      liveActiveWallets: liveActiveWalletWindow.rows,
      walletPerformance: walletRowsWindow.rows,
      accountOverview: account.overview || null,
    };
  }

  function syncCurrentWallet() {
    if (!walletStore) return;
    const wallet = pipeline.getAccount();
    if (!wallet) return;
    const record = buildWalletRecordFromState({
      wallet,
      state: pipeline.getState(),
    });
    walletStore.upsert(record, { force: true });
  }

  async function handleRequest(req, res, url) {
    syncWalletStoreFromDisk();
    syncCurrentWallet();

    if (liveTradesUpstreamEnabled && String(url.pathname || "").startsWith("/api/live-trades")) {
      if (url.pathname === "/api/live-trades/stream") {
        return proxyLiveTradesStream(req, res, url);
      }
      if (
        url.pathname === "/api/live-trades" ||
        url.pathname === "/api/live-trades/attribution-db" ||
        url.pathname === "/api/live-trades/wallet-first" ||
        /^\/api\/live-trades\/wallet\/[^/]+$/i.test(String(url.pathname || ""))
      ) {
        return proxyLiveTradesJson(req, res, url);
      }
    }

    const liveDefaultPublicLimitRaw = Number(process.env.PACIFICA_LIVE_PUBLIC_DEFAULT_LIMIT || 500);
    const liveDefaultWalletLimitRaw = Number(process.env.PACIFICA_LIVE_WALLET_DEFAULT_LIMIT || 500);
    const liveDefaultPositionLimitRaw = Number(process.env.PACIFICA_LIVE_POSITION_DEFAULT_LIMIT || 500);
    const liveDefaultAccountTradeLimitRaw = Number(
      process.env.PACIFICA_LIVE_ACCOUNT_TRADE_DEFAULT_LIMIT || 250
    );
    const liveDefaultPublicLimit =
      Number.isFinite(liveDefaultPublicLimitRaw) && liveDefaultPublicLimitRaw > 0
        ? Math.max(50, Math.floor(liveDefaultPublicLimitRaw))
        : 500;
    const liveDefaultWalletLimit =
      Number.isFinite(liveDefaultWalletLimitRaw) && liveDefaultWalletLimitRaw > 0
        ? Math.max(50, Math.floor(liveDefaultWalletLimitRaw))
        : 500;
    const liveDefaultPositionLimit =
      Number.isFinite(liveDefaultPositionLimitRaw) && liveDefaultPositionLimitRaw > 0
        ? Math.max(50, Math.floor(liveDefaultPositionLimitRaw))
        : 500;
    const liveDefaultAccountTradeLimit =
      Number.isFinite(liveDefaultAccountTradeLimitRaw) && liveDefaultAccountTradeLimitRaw > 0
        ? Math.max(25, Math.floor(liveDefaultAccountTradeLimitRaw))
        : 250;

    if (url.pathname === "/api/live-trades/stream") {
      const streamIntervalMs = Math.max(
        1000,
        Number(process.env.PACIFICA_LIVE_STREAM_INTERVAL_MS || 2500)
      );
      res.writeHead(200, {
        "Content-Type": "text/event-stream; charset=utf-8",
        "Cache-Control": "no-cache, no-transform",
        Connection: "keep-alive",
        "X-Accel-Buffering": "no",
      });

      const writeEvent = (event, data) => {
        try {
          const serialized = JSON.stringify(data || {});
          res.write(`event: ${event}\n`);
          res.write(`data: ${serialized}\n\n`);
        } catch (_error) {
          // ignore write errors during teardown
        }
      };

      const pushSnapshot = async () => {
        const skipPublicTrades = parseFlagParam(
          url.searchParams.get("skip_public"),
          parseFlagParam(url.searchParams.get("lite"), false)
        );
        const priceLookup = await getLivePositionPriceLookup();
        writeEvent("snapshot", {
          type: "snapshot",
          at: Date.now(),
          payload: buildLiveTradesPayload(
            {
              publicOffset: parseWindowParam(url.searchParams.get("public_offset"), 0),
              publicLimit: parseWindowParam(
                url.searchParams.get("public_limit"),
                liveDefaultPublicLimit
              ),
              walletOffset: parseWindowParam(url.searchParams.get("wallet_offset"), 0),
              walletLimit: parseWindowParam(
                url.searchParams.get("wallet_limit"),
                liveDefaultWalletLimit
              ),
              positionOffset: parseWindowParam(url.searchParams.get("position_offset"), 0),
              positionLimit: parseWindowParam(
                url.searchParams.get("position_limit"),
                liveDefaultPositionLimit
              ),
              walletSort: String(url.searchParams.get("wallet_sort") || "pnlAll").trim(),
              walletDir: String(url.searchParams.get("wallet_dir") || "desc").trim(),
              positionSort: String(url.searchParams.get("position_sort") || "openedAt").trim(),
              positionDir: String(url.searchParams.get("position_dir") || "desc").trim(),
              accountTradeOffset: parseWindowParam(url.searchParams.get("account_trade_offset"), 0),
              accountTradeLimit: parseWindowParam(
                url.searchParams.get("account_trade_limit"),
                liveDefaultAccountTradeLimit
              ),
              liveActiveOffset: parseWindowParam(url.searchParams.get("live_active_offset"), 0),
              liveActiveLimit: parseWindowParam(url.searchParams.get("live_active_limit"), 200),
              skipPublicTrades,
            },
            { priceLookup }
          ),
        });
      };

      const pushHeartbeat = () => {
        writeEvent("heartbeat", {
          type: "heartbeat",
          at: Date.now(),
        });
      };

      pushSnapshot().catch(() => null);
      const snapshotTimer = setInterval(() => {
        pushSnapshot().catch(() => null);
      }, streamIntervalMs);
      const heartbeatTimer = setInterval(pushHeartbeat, 10000);

      const cleanup = () => {
        clearInterval(snapshotTimer);
        clearInterval(heartbeatTimer);
      };

      req.on("close", cleanup);
      req.on("aborted", cleanup);
      res.on("close", cleanup);
      res.on("error", cleanup);
      return true;
    }

    const dashboard = pipeline.getDashboardPayload();

    if (url.pathname === "/api/token-analytics" && req.method === "GET") {
      const symbol = normalizeTokenSymbol(url.searchParams.get("symbol"));
      const timeframe = normalizeTokenAnalyticsTimeframe(url.searchParams.get("timeframe"));
      const force =
        String(url.searchParams.get("force") || "").trim() === "1" ||
        String(url.searchParams.get("force") || "").trim().toLowerCase() === "true";

      if (!symbol) {
        sendJson(res, 400, {
          ok: false,
          error: "missing symbol query param",
        });
        return true;
      }

      try {
        const payload = await getTokenAnalytics(symbol, timeframe, { force });
        sendJson(res, 200, {
          ok: true,
          ...payload,
        });
      } catch (error) {
        sendJson(res, 500, {
          ok: false,
          error: error.message || "token_analytics_failed",
          symbol,
          timeframe,
        });
      }
      return true;
    }

    if (url.pathname === "/api/account") {
      sendJson(res, 200, {
        generatedAt: Date.now(),
        account: dashboard.account.overview,
        settingsBySymbol: dashboard.account.settingsBySymbol,
        risk: dashboard.risk,
      });
      return true;
    }

    if (url.pathname === "/api/positions") {
      sendJson(res, 200, {
        generatedAt: Date.now(),
        rows: dashboard.positions.rows,
        summary: {
          totalNotionalUsd: dashboard.positions.totalNotionalUsd,
          totalUnrealizedPnlUsd: dashboard.positions.totalUnrealizedPnlUsd,
          closestLiquidationDistancePct: dashboard.positions.closestLiquidationDistancePct,
        },
      });
      return true;
    }

    if (url.pathname === "/api/orders") {
      sendJson(res, 200, {
        generatedAt: Date.now(),
        rows: dashboard.orders.open,
      });
      return true;
    }

    if (url.pathname === "/api/exchange/overview") {
      const timeframeRaw = String(url.searchParams.get("timeframe") || "all")
        .toLowerCase()
        .trim();
      const timeframe =
        timeframeRaw === "24h" || timeframeRaw === "30d" || timeframeRaw === "all"
          ? timeframeRaw
          : "all";
      const walletRows = walletStore ? walletStore.list() : [];
      const payload = buildExchangeOverviewPayload({
        state: pipeline.getState(),
        transport: pipeline.getTransportState(),
        wallets: walletRows,
        timeframe,
      });

      // Source of truth: use Pacifica /info/prices DefiLlama-style formula for volume/OI.
      // dailyVolume = sum(Number(volume_24h))
      // openInterestAtEnd = sum(Number(open_interest) * Number(mark))
      try {
        const truth = await getDefiLlamaPricesTruth();
        const volumeWindow = buildVolumeWindowInfo(truth);
        const rank24h = buildDefiLlamaVolumeRank(
          truth.prices,
          Array.isArray(truth.prices) ? truth.prices.length : 0
        );
        const total24hFromRank = rank24h.reduce(
          (acc, row) => acc + toNum(row && row.volume_24h_usd !== undefined ? row.volume_24h_usd : 0),
          0
        );
        const historicalTotal =
          truth.totalHistoricalVolume !== null &&
          truth.totalHistoricalVolume !== undefined &&
          Number.isFinite(Number(truth.totalHistoricalVolume))
            ? Number(truth.totalHistoricalVolume)
            : null;
        const dailyHistoryByDate =
          truth.dailyHistoryByDate &&
          typeof truth.dailyHistoryByDate === "object" &&
          !Array.isArray(truth.dailyHistoryByDate)
            ? truth.dailyHistoryByDate
            : null;
        const trackingStartDate =
          truth &&
          truth.volumeMeta &&
          truth.volumeMeta.trackingStartDate
            ? String(truth.volumeMeta.trackingStartDate).slice(0, 10)
            : null;
        const trackedThroughDate =
          truth &&
          truth.volumeMeta &&
          truth.volumeMeta.lastProcessedDate
            ? String(truth.volumeMeta.lastProcessedDate).slice(0, 10)
            : volumeWindow && volumeWindow.trackedThroughDate
            ? String(volumeWindow.trackedThroughDate).slice(0, 10)
            : null;
        const expectedProcessedDays =
          truth &&
          truth.volumeMeta &&
          Number.isFinite(Number(truth.volumeMeta.processedDays))
            ? Number(truth.volumeMeta.processedDays)
            : null;
        const lookback30dStart = trackedThroughDate
          ? addUtcDays(trackedThroughDate, -29)
          : null;
        const allCoveredDays = countHistoricalDaysWithSymbolVolumes({
          dailyByDate: dailyHistoryByDate,
          startDate: trackingStartDate,
          endDate: trackedThroughDate,
          requireReliableDay: true,
        });
        const covered30dDays = countHistoricalDaysWithSymbolVolumes({
          dailyByDate: dailyHistoryByDate,
          startDate: lookback30dStart,
          endDate: trackedThroughDate,
          requireReliableDay: true,
        });
        const expected30dDays = (() => {
          if (!trackedThroughDate || !lookback30dStart) return null;
          const startMs = Date.parse(`${lookback30dStart}T00:00:00.000Z`);
          const endMs = Date.parse(`${trackedThroughDate}T00:00:00.000Z`);
          if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs < startMs) return null;
          return Math.floor((endMs - startMs) / (24 * 60 * 60 * 1000)) + 1;
        })();

        const rankAllHistoricalReliable = buildHistoricalSymbolVolumeRank({
          dailyByDate: dailyHistoryByDate,
          startDate: trackingStartDate,
          endDate: trackedThroughDate,
          limit: 20000,
          requireReliableDay: true,
        });
        const rankAllHistoricalBestEffort =
          rankAllHistoricalReliable.length > 0
            ? rankAllHistoricalReliable
            : buildHistoricalSymbolVolumeRank({
                dailyByDate: dailyHistoryByDate,
                startDate: trackingStartDate,
                endDate: trackedThroughDate,
                limit: 20000,
                requireReliableDay: false,
              });
        const totalAllFromRankReliable = sumRankVolumeUsd(rankAllHistoricalReliable);
        const totalAllFromRankBestEffort = sumRankVolumeUsd(rankAllHistoricalBestEffort);

        const rank30dHistoricalReliable = trackedThroughDate
          ? buildHistoricalSymbolVolumeRank({
              dailyByDate: dailyHistoryByDate,
              startDate: lookback30dStart,
              endDate: trackedThroughDate,
              limit: 20000,
              requireReliableDay: true,
            })
          : [];
        const rank30dHistoricalBestEffort =
          rank30dHistoricalReliable.length > 0
            ? rank30dHistoricalReliable
            : trackedThroughDate
            ? buildHistoricalSymbolVolumeRank({
                dailyByDate: dailyHistoryByDate,
                startDate: lookback30dStart,
                endDate: trackedThroughDate,
                limit: 20000,
                requireReliableDay: false,
              })
            : [];
        const total30dFromRankReliable = sumRankVolumeUsd(rank30dHistoricalReliable);
        const total30dFromRankBestEffort = sumRankVolumeUsd(rank30dHistoricalBestEffort);
        const allCoveredDaysAny = countHistoricalDaysWithSymbolVolumes({
          dailyByDate: dailyHistoryByDate,
          startDate: trackingStartDate,
          endDate: trackedThroughDate,
          requireReliableDay: false,
        });
        const covered30dDaysAny = countHistoricalDaysWithSymbolVolumes({
          dailyByDate: dailyHistoryByDate,
          startDate: lookback30dStart,
          endDate: trackedThroughDate,
          requireReliableDay: false,
        });

        // Keep total volume and volume rank on the exact same window/source.
        let volumeWindowUsed = "24h";
        let selectedRank = rank24h;
        let selectedTotalVolume = total24hFromRank;
        let selectedTotalVolumeSource = "/api/v1/info/prices:sum(volume_24h)";
        let selectedRankSource = "/api/v1/info/prices:rank_by(volume_24h)";
        let volumeWindowQuality = "defillama_live";
        let volumeWindowFallback = false;
        if (timeframe === "all") {
          if (rankAllHistoricalReliable.length > 0) {
            volumeWindowUsed = "all";
            selectedRank = rankAllHistoricalReliable;
            selectedTotalVolume = totalAllFromRankReliable;
            selectedTotalVolumeSource = `${truth.volumeSource || "/api/v1/kline"}:historical_sum(v*c)`;
            selectedRankSource = `${truth.volumeSource || "/api/v1/kline"}:historical_rank_by_symbol_sum(v*c)`;
            volumeWindowQuality = "historical_reliable";
            volumeWindowFallback =
              expectedProcessedDays !== null ? allCoveredDays < expectedProcessedDays : false;
          } else if (rankAllHistoricalBestEffort.length > 0) {
            volumeWindowUsed = "all";
            selectedRank = rankAllHistoricalBestEffort;
            selectedTotalVolume = totalAllFromRankBestEffort;
            selectedTotalVolumeSource = `${truth.volumeSource || "/api/v1/kline"}:historical_sum(v*c)_best_effort`;
            selectedRankSource = `${truth.volumeSource || "/api/v1/kline"}:historical_rank_by_symbol_sum(v*c)_best_effort`;
            volumeWindowQuality = "historical_best_effort";
            volumeWindowFallback = false;
          } else {
            volumeWindowFallback = true;
          }
        } else if (timeframe === "30d") {
          if (rank30dHistoricalReliable.length > 0) {
            volumeWindowUsed = "30d";
            selectedRank = rank30dHistoricalReliable;
            selectedTotalVolume = total30dFromRankReliable;
            selectedTotalVolumeSource = `${truth.volumeSource || "/api/v1/kline"}:30d_sum(v*c)`;
            selectedRankSource = `${truth.volumeSource || "/api/v1/kline"}:30d_rank_by_symbol_sum(v*c)`;
            volumeWindowQuality = "historical_reliable";
            volumeWindowFallback =
              expected30dDays !== null ? covered30dDays < expected30dDays : false;
          } else if (rank30dHistoricalBestEffort.length > 0) {
            volumeWindowUsed = "30d";
            selectedRank = rank30dHistoricalBestEffort;
            selectedTotalVolume = total30dFromRankBestEffort;
            selectedTotalVolumeSource = `${truth.volumeSource || "/api/v1/kline"}:30d_sum(v*c)_best_effort`;
            selectedRankSource = `${truth.volumeSource || "/api/v1/kline"}:30d_rank_by_symbol_sum(v*c)_best_effort`;
            volumeWindowQuality = "historical_best_effort";
            volumeWindowFallback = false;
          } else {
            volumeWindowFallback = true;
          }
        }

        const walletWindowMetrics = {
          all: aggregateWalletWindowMetrics(walletRows, "all"),
          "24h": aggregateWalletWindowMetrics(walletRows, "24h"),
          "30d": aggregateWalletWindowMetrics(walletRows, "30d"),
        };
        const walletCoverageDays = computeWalletCoverageDays(walletRows);
        const volumeCoverageDays =
          rankAllHistoricalReliable.length > 0
            ? allCoveredDays
            : rankAllHistoricalBestEffort.length > 0
              ? allCoveredDaysAny
              : expectedProcessedDays;
        const volumeWindowMetrics = {
          all: {
            totalVolumeUsd:
              rankAllHistoricalReliable.length > 0
                ? totalAllFromRankReliable
                : totalAllFromRankBestEffort,
          },
          "24h": {
            totalVolumeUsd: total24hFromRank,
          },
          "30d": {
            totalVolumeUsd:
              rank30dHistoricalReliable.length > 0
                ? total30dFromRankReliable
                : total30dFromRankBestEffort,
          },
        };
        const kpiComparisons = buildExchangeKpiComparisons({
          timeframe,
          walletWindows: walletWindowMetrics,
          volumeWindows: volumeWindowMetrics,
          openInterestUsd: truth.openInterestAtEnd,
          walletCoverageDays,
          volumeCoverageDays,
        });
        const kpiPeriodChanges = {
          "24h": buildExchangeKpiComparisons({
            timeframe: "24h",
            walletWindows: walletWindowMetrics,
            volumeWindows: volumeWindowMetrics,
            openInterestUsd: truth.openInterestAtEnd,
            walletCoverageDays,
            volumeCoverageDays,
          }).metrics,
          "30d": buildExchangeKpiComparisons({
            timeframe: "30d",
            walletWindows: walletWindowMetrics,
            volumeWindows: volumeWindowMetrics,
            openInterestUsd: truth.openInterestAtEnd,
            walletCoverageDays,
            volumeCoverageDays,
          }).metrics,
        };

        payload.kpis.totalVolumeUsd = toFixed(selectedTotalVolume, 2);
        payload.kpis.totalVolumeCompact = toCompact(payload.kpis.totalVolumeUsd);
        payload.kpis.activeAccounts = Math.max(
          0,
          Number((walletWindowMetrics[timeframe] && walletWindowMetrics[timeframe].activeAccounts) || 0)
        );
        payload.kpis.protocolCumulativeVolumeUsd = toFixed(
          historicalTotal !== null ? historicalTotal : 0,
          2
        );
        payload.kpis.openInterestAtEnd = toFixed(truth.openInterestAtEnd, 2);
        payload.kpiComparisons = kpiComparisons;
        payload.kpiPeriodChanges = kpiPeriodChanges;
        payload.volumeRank = selectedRank;
        payload.source = {
          ...(payload.source || {}),
          prices: Array.isArray(truth.prices) ? truth.prices : [],
          dailyVolumeSource: "/api/v1/info/prices:sum(volume_24h)",
          openInterestSource: "/api/v1/info/prices:sum(open_interest*mark)",
          volumeRankSource: selectedRankSource,
          totalVolumeSource: selectedTotalVolumeSource,
          volumeRankWindowUsed: volumeWindowUsed,
          totalVolumeWindowUsed: volumeWindowUsed,
          volumeRankSymbolCount: selectedRank.length,
          volumeRankTotalUsd: Number(selectedTotalVolume || 0),
          requestedTimeframe: timeframe,
          volumeWindowQuality,
          volumeWindowFallback,
          historicalSymbolCoverageDays: allCoveredDays,
          historicalSymbolExpectedDays: expectedProcessedDays,
          historicalSymbolCoverageDaysAny: allCoveredDaysAny,
          historical30dCoverageDays: covered30dDays,
          historical30dExpectedDays: expected30dDays,
          historical30dCoverageDaysAny: covered30dDaysAny,
          protocolCumulativeVolumeUsd:
            historicalTotal !== null && Number.isFinite(Number(historicalTotal))
              ? Number(historicalTotal)
              : null,
          symbolNormalization: "use_api_symbol_as_is",
          totalVolumeSourceOfTruth: "defillama_adapter_logic",
          defillamaSource: truth.source || "local_fetch",
          defillamaVolumeMethod: truth.volumeMethod || "prices_rolling_24h",
          defillamaVolumeSource: truth.volumeSource || "/api/v1/info/prices:sum(volume_24h)",
          defillamaVolumeMeta: truth.volumeMeta || null,
          kpiComparisonMethod:
            timeframe === "24h"
              ? "24h_vs_30d_avg_day_for_flows"
              : timeframe === "30d"
                ? "30d_vs_lifetime_30d_run_rate_for_flows"
                : "lifetime_snapshot",
          defillamaDailyVolumeFromPrices24h:
            truth.dailyVolumeFromPrices24h !== null &&
            truth.dailyVolumeFromPrices24h !== undefined &&
            Number.isFinite(Number(truth.dailyVolumeFromPrices24h))
              ? Number(truth.dailyVolumeFromPrices24h)
              : null,
          defillamaDailyVolumeCompat:
            truth.dailyVolumeDefillamaCompat !== null &&
            truth.dailyVolumeDefillamaCompat !== undefined &&
            Number.isFinite(Number(truth.dailyVolumeDefillamaCompat))
              ? Number(truth.dailyVolumeDefillamaCompat)
              : null,
          defillamaCacheTtlMs: defillamaCache.ttlMs,
          defillamaStale: Boolean(truth.stale),
          defillamaLastError: truth.lastError || null,
          defillamaFetchedAt: truth.fetchedAt || defillamaCache.fetchedAt || null,
          defillamaCheckedAt: Date.now(),
          defillamaFetchDurationMs:
            Number.isFinite(Number(truth.fetchDurationMs)) ? Number(truth.fetchDurationMs) : null,
          defillamaCacheHit: Boolean(truth.cacheHit),
          defillamaVolumeWindow: volumeWindow,
          defillamaVolumeDate: volumeWindow.date || null,
          defillamaVolumeWindowStartIso: volumeWindow.windowStartIso || null,
          defillamaVolumeWindowEndIso: volumeWindow.windowEndIso || null,
          defillamaVolumeWindowLabel: volumeWindow.label || null,
          defillamaTrackedThroughDate: volumeWindow.trackedThroughDate || null,
          defillamaTrackingStartDate: volumeWindow.trackingStartDate || null,
          defillamaTodayUtcDate: volumeWindow.todayUtcDate || null,
          defillamaRemainingDaysToToday:
            volumeWindow.remainingDaysToToday !== null &&
            volumeWindow.remainingDaysToToday !== undefined
              ? Number(volumeWindow.remainingDaysToToday)
              : null,
          defillamaProcessedDays:
            volumeWindow.processedDays !== null && volumeWindow.processedDays !== undefined
              ? Number(volumeWindow.processedDays)
              : null,
          defillamaTotalDaysToToday:
            volumeWindow.totalDaysToToday !== null && volumeWindow.totalDaysToToday !== undefined
              ? Number(volumeWindow.totalDaysToToday)
              : null,
          defillamaBackfillComplete:
            volumeWindow.backfillComplete !== null && volumeWindow.backfillComplete !== undefined
              ? Boolean(volumeWindow.backfillComplete)
              : null,
          defillamaTotalHistoricalVolume:
            historicalTotal !== null ? Number(historicalTotal) : null,
          defillamaBackfillProgress: {
            start_date: volumeWindow.trackingStartDate || null,
            current_processed_day: volumeWindow.trackedThroughDate || null,
            days_processed:
              volumeWindow.processedDays !== null && volumeWindow.processedDays !== undefined
                ? Number(volumeWindow.processedDays)
                : null,
            days_remaining:
              volumeWindow.remainingDaysToToday !== null &&
              volumeWindow.remainingDaysToToday !== undefined
                ? Number(volumeWindow.remainingDaysToToday)
                : null,
          },
        };
      } catch (error) {
        sendJson(res, 503, {
          ok: false,
          error: `DefiLlama source unavailable: ${error.message}`,
          source: "/api/v1/info/prices",
        });
        return true;
      }

      payload.indexer = await getFreshIndexerStatus();

      sendJson(res, 200, payload);
      return true;
    }

    if (url.pathname === "/api/live-trades") {
      const skipPublicTrades = parseFlagParam(
        url.searchParams.get("skip_public"),
        parseFlagParam(url.searchParams.get("lite"), false)
      );
      const priceLookup = await getLivePositionPriceLookup();
      sendJson(
        res,
        200,
        buildLiveTradesPayload(
          {
            publicOffset: parseWindowParam(url.searchParams.get("public_offset"), 0),
            publicLimit: parseWindowParam(
              url.searchParams.get("public_limit"),
              liveDefaultPublicLimit
            ),
            walletOffset: parseWindowParam(url.searchParams.get("wallet_offset"), 0),
            walletLimit: parseWindowParam(
              url.searchParams.get("wallet_limit"),
              liveDefaultWalletLimit
            ),
            positionOffset: parseWindowParam(url.searchParams.get("position_offset"), 0),
            positionLimit: parseWindowParam(
              url.searchParams.get("position_limit"),
              liveDefaultPositionLimit
            ),
            walletSort: String(url.searchParams.get("wallet_sort") || "pnlAll").trim(),
            walletDir: String(url.searchParams.get("wallet_dir") || "desc").trim(),
            positionSort: String(url.searchParams.get("position_sort") || "openedAt").trim(),
            positionDir: String(url.searchParams.get("position_dir") || "desc").trim(),
            accountTradeOffset: parseWindowParam(url.searchParams.get("account_trade_offset"), 0),
            accountTradeLimit: parseWindowParam(
              url.searchParams.get("account_trade_limit"),
              liveDefaultAccountTradeLimit
            ),
            liveActiveOffset: parseWindowParam(url.searchParams.get("live_active_offset"), 0),
            liveActiveLimit: parseWindowParam(url.searchParams.get("live_active_limit"), 200),
            skipPublicTrades,
          },
          { priceLookup }
        )
      );
      return true;
    }

    if (/^\/api\/live-trades\/wallet\/[^/]+$/i.test(String(url.pathname || "")) && req.method === "GET") {
      const wallet = decodeURIComponent(String(url.pathname || "").split("/").pop() || "").trim();
      if (!wallet) {
        sendJson(res, 400, {
          generatedAt: Date.now(),
          error: "wallet_required",
        });
        return true;
      }
      const explorerDataset = await getWalletExplorerDataset();
      const walletRows =
        explorerDataset && Array.isArray(explorerDataset.walletRows)
          ? explorerDataset.walletRows
          : [];
      const positionsAll =
        explorerDataset && Array.isArray(explorerDataset.positions)
          ? explorerDataset.positions
          : [];
      const liveActiveRows =
        explorerDataset && Array.isArray(explorerDataset.liveActiveRows)
          ? explorerDataset.liveActiveRows
          : [];
      const priceLookup = await getLivePositionPriceLookup();
      const walletPerformanceSupport = buildWalletPerformanceSupportSummary({
        walletMetricsHistoryStatus:
          walletMetricsHistoryStore &&
          typeof walletMetricsHistoryStore.getStatus === "function"
            ? walletMetricsHistoryStore.getStatus()
            : { enabled: false },
        positionLifecycleStatus:
          positionLifecycleStore &&
          typeof positionLifecycleStore.getStatus === "function"
            ? positionLifecycleStore.getStatus()
            : { enabled: false },
      });
      const payload = buildLiveTradeWalletDetailPayload({
        wallet,
        livePayload: {
          walletPerformance: walletRows,
          positions: positionsAll,
          liveActiveWallets: liveActiveRows,
        },
        lifecycleSnapshot: await getLifecycleSnapshot(wallet),
        supportSummary: walletPerformanceSupport,
        priceLookup,
      });
      if (!payload.found) {
        sendJson(res, 404, payload);
        return true;
      }
      sendJson(res, 200, payload);
      return true;
    }

    if (url.pathname === "/api/live-trades/attribution-db" && req.method === "GET") {
      const evidenceLimit = Math.max(
        1,
        Math.min(1000, parseWindowParam(url.searchParams.get("evidence_limit"), 100))
      );
      sendJson(res, 200, {
        generatedAt: Date.now(),
        db: liveTradeAttributionStore
          ? {
              enabled: true,
              ...liveTradeAttributionStore.getStatus(),
              growth: liveTradeAttributionStore.getGrowthSnapshot(),
              recentEvidence: liveTradeAttributionStore.getRecentEvidence(evidenceLimit),
            }
          : { enabled: false, reason: "db_disabled" },
      });
      return true;
    }

    if (url.pathname === "/api/live-trades/wallet-first" && req.method === "GET") {
      const eventsLimit = Math.max(
        1,
        Math.min(5000, parseWindowParam(url.searchParams.get("events_limit"), 300))
      );
      const snapshot = getLiveWalletSnapshot(eventsLimit);
      sendJson(res, 200, {
        generatedAt: Date.now(),
        status: snapshot.status || { enabled: false },
        openPositions: Array.isArray(snapshot.positions) ? snapshot.positions.length : 0,
        positions: Array.isArray(snapshot.positions) ? snapshot.positions : [],
        events: Array.isArray(snapshot.events) ? snapshot.events : [],
        triggers:
          solanaLogTriggerMonitor && typeof solanaLogTriggerMonitor.getStatus === "function"
            ? solanaLogTriggerMonitor.getStatus()
            : { enabled: false },
      });
      return true;
    }

    if (url.pathname === "/api/indexer/status") {
      sendJson(res, 200, {
        generatedAt: Date.now(),
        status: await getFreshIndexerStatus(),
      });
      return true;
    }

    if (url.pathname === "/api/indexer/lifecycle-map") {
      const wallets = parseWalletListParam(url.searchParams.get("wallets"));
      const map =
        walletIndexer && typeof walletIndexer.getWalletLifecycleMap === "function"
          ? walletIndexer.getWalletLifecycleMap(wallets)
          : {};
      sendJson(res, 200, {
        generatedAt: Date.now(),
        count: wallets.length,
        map,
      });
      return true;
    }

    if (url.pathname === "/api/indexer/lifecycle") {
      const wallet = String(url.searchParams.get("wallet") || "").trim();
      const snapshot =
        walletIndexer && typeof walletIndexer.getWalletLifecycleSnapshot === "function"
          ? walletIndexer.getWalletLifecycleSnapshot(wallet)
          : null;
      sendJson(res, 200, {
        generatedAt: Date.now(),
        wallet,
        snapshot,
      });
      return true;
    }

    if (url.pathname === "/api/wallets") {
      const query = Object.fromEntries(url.searchParams.entries());
      const payload = queryWalletExplorerDataset(
        await getWalletExplorerDataset({
          force:
            String(query.force || query.refresh || "")
              .trim()
              .toLowerCase() === "1",
        }),
        query
      );
      sendJson(res, 200, payload);
      return true;
    }

    if (url.pathname === "/api/wallets/profile") {
      const explorerDataset = await getWalletExplorerDataset();
      const payload = buildWalletProfilePayload({
        wallets:
          explorerDataset && Array.isArray(explorerDataset.walletRows)
            ? explorerDataset.walletRows
            : [],
        wallet: url.searchParams.get("wallet"),
        timeframe: url.searchParams.get("timeframe"),
      });
      if (payload && payload.wallet) {
        payload.lifecycle = await getLifecycleSnapshot(payload.wallet);
      }
      sendJson(res, 200, payload);
      return true;
    }

    if (url.pathname === "/api/orders/history") {
      sendJson(res, 200, pipeline.getOrdersHistoryPayload(Object.fromEntries(url.searchParams.entries())));
      return true;
    }

    if (url.pathname === "/api/trades") {
      sendJson(res, 200, {
        generatedAt: Date.now(),
        rows: dashboard.trades.recent,
      });
      return true;
    }

    if (url.pathname === "/api/trades/history") {
      sendJson(res, 200, pipeline.getTradesHistoryPayload(Object.fromEntries(url.searchParams.entries())));
      return true;
    }

    if (url.pathname === "/api/funding/history") {
      sendJson(res, 200, pipeline.getFundingHistoryPayload(Object.fromEntries(url.searchParams.entries())));
      return true;
    }

    if (url.pathname === "/api/timeline") {
      sendJson(res, 200, pipeline.getTimelinePayload(Object.fromEntries(url.searchParams.entries())));
      return true;
    }

    if (url.pathname === "/api/allocations") {
      sendJson(res, 200, {
        generatedAt: Date.now(),
        allocations: dashboard.allocations,
      });
      return true;
    }

    if (url.pathname === "/api/performance") {
      sendJson(res, 200, {
        generatedAt: Date.now(),
        performance: dashboard.performance,
      });
      return true;
    }

    return false;
  }

  function ingestWalletTrigger(trigger) {
    recordLiveWalletTrigger(trigger);
    if (
      liveWalletPositionsMonitor &&
      typeof liveWalletPositionsMonitor.noteRecentWalletActivity === "function" &&
      trigger &&
      trigger.wallet
    ) {
      liveWalletPositionsMonitor.noteRecentWalletActivity(trigger.wallet, trigger.at || Date.now());
    }
  }

  refreshWalletExplorerDataset(true).catch(() => {});
  const walletExplorerRefreshTimer = setInterval(() => {
    refreshWalletExplorerDataset(true).catch(() => {});
  }, walletExplorerDatasetCache.refreshIntervalMs);
  if (walletExplorerRefreshTimer && typeof walletExplorerRefreshTimer.unref === "function") {
    walletExplorerRefreshTimer.unref();
  }

  return {
    handleRequest,
    ingestWalletTrigger,
    getLiveWalletSnapshot,
  };
}

module.exports = {
  createWalletTrackingComponent,
};
