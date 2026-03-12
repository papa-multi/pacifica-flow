const LIVE_STALE_MS = 20000;
const LIVE_DELAYED_MS = 8000;
const RECONNECT_MAX_MS = 15000;
const SNAPSHOT_TIMEOUT_MS = 25000;
const DRAWER_FETCH_TIMEOUT_MS = 8000;
const WALLET_DETAIL_ENDPOINT_PREFIX = "/api/live-trades/wallet/";

if (typeof window !== "undefined") {
  window.__PF_LIVE_TRADE_SCRIPT_EXECUTED = true;
  // Mark bundle as loaded immediately; runtime errors are surfaced separately.
  window.__PF_LIVE_TRADE_BOOTED = true;
}

const state = {
  payload: null,
  payloadSignature: "",
  activeView: "positions",
  activeWalletGroup: "pnl",
  positionsSortKey: "openedAt",
  positionsSortDir: "desc",
  walletsSortKey: "pnlAll",
  walletsSortDir: "desc",
  selection: {
    type: "",
    key: "",
  },
  rows: {
    positions: [],
    wallets: [],
    liveActive: [],
  },
  rowSignatures: {
    positions: "",
    wallets: "",
    liveActive: "",
  },
  search: "",
  filters: {
    wallet: "",
    symbol: "",
    side: "",
    minUsd: 0,
    maxUsd: 0,
    minPnl: 0,
    maxPnl: 0,
    freshness: "",
    trackedOnly: false,
  },
  pagination: {
    positions: {
      page: 1,
      pageSize: 100,
    },
    wallets: {
      page: 1,
      pageSize: 100,
    },
  },
  stream: {
    source: "bootstrap",
    connection: "idle",
    paused: false,
    retryMs: 1000,
    eventSource: null,
    fallbackTimer: null,
    healthTimer: null,
    lastPayloadAt: 0,
    lastEventAt: 0,
    lastErrorAt: 0,
    lastError: null,
  },
  drawer: {
    wallet: "",
    loading: false,
    error: "",
    detail: null,
    sourceRow: null,
    requestId: 0,
    abortController: null,
  },
};

const VIEW_CONFIG = {
  positions: {
    kicker: "Live positions",
    title: "Positions",
  },
  wallets: {
    kicker: "Wallet intelligence",
    title: "Wallet Performance",
  },
};

function el(id) {
  return document.getElementById(id);
}

function n(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function fmtNum(value, digits = 2) {
  const num = n(value, NaN);
  if (!Number.isFinite(num)) return "-";
  return num.toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function fmtInt(value) {
  return fmtNum(value, 0);
}

function fmtUsd(value, digits = 2) {
  const num = n(value, NaN);
  if (!Number.isFinite(num)) return "-";
  return `$${fmtNum(num, digits)}`;
}

function fmtSignedUsd(value, digits = 2) {
  const num = n(value, NaN);
  if (!Number.isFinite(num)) return "-";
  const abs = Math.abs(num);
  return `${num > 0 ? "+" : num < 0 ? "-" : ""}$${fmtNum(abs, digits)}`;
}

function fmtPct(value, digits = 2) {
  const num = n(value, NaN);
  if (!Number.isFinite(num)) return "-";
  return `${num > 0 ? "+" : ""}${fmtNum(num, digits)}%`;
}

function tsMs(value) {
  const maybe = n(value, NaN);
  if (Number.isFinite(maybe) && maybe > 0) return maybe;
  const parsed = Date.parse(String(value || ""));
  return Number.isFinite(parsed) ? parsed : 0;
}

function fmtTs(value) {
  const ms = tsMs(value);
  if (!ms) return "-";
  return new Date(ms).toLocaleString();
}

function fmtAgo(value) {
  const ms = tsMs(value);
  if (!ms) return "-";
  const delta = Math.max(0, Date.now() - ms);
  if (delta < 1000) return "now";
  const sec = Math.floor(delta / 1000);
  if (sec < 60) return `${sec}s ago`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m ago`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h ago`;
  const day = Math.floor(hr / 24);
  return `${day}d ago`;
}

function fmtDurationShort(value) {
  const ms = n(value, NaN);
  if (!Number.isFinite(ms) || ms <= 0) return "n/a";
  const sec = Math.floor(ms / 1000);
  if (sec < 60) return `${sec}s`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m`;
  const hr = Math.floor(min / 60);
  if (hr < 24) {
    const remMin = min % 60;
    return remMin > 0 ? `${hr}h ${remMin}m` : `${hr}h`;
  }
  const day = Math.floor(hr / 24);
  const remHr = hr % 24;
  return remHr > 0 ? `${day}d ${remHr}h` : `${day}d`;
}

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function asObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function extractRows(value) {
  if (Array.isArray(value)) return value.filter(Boolean);
  const obj = asObject(value);
  if (Array.isArray(obj.items)) return obj.items.filter(Boolean);
  if (Array.isArray(obj.rows)) return obj.rows.filter(Boolean);
  if (Array.isArray(obj.data)) return obj.data.filter(Boolean);
  if (Array.isArray(obj.list)) return obj.list.filter(Boolean);
  return [];
}

function extractPublicTrades(payload) {
  const primary = extractRows(payload?.publicTrades);
  if (primary.length > 0) return primary;
  const legacy = extractRows(payload?.rows);
  return legacy;
}

function extractWalletPerformance(payload) {
  const primary = extractRows(payload?.walletPerformance);
  if (primary.length > 0) return primary;
  const legacy = extractRows(payload?.wallets);
  return legacy;
}

function extractPositions(payload) {
  const primary = extractRows(payload?.positions);
  return primary;
}

function extractLiveActiveWallets(payload) {
  const primary = extractRows(payload?.liveActiveWallets);
  if (primary.length > 0) return primary;
  return [];
}

function extractWalletPerformanceSupport(payload) {
  return asObject(payload?.summary?.walletPerformanceSupport);
}

function payloadPublicTradesTotal(payload) {
  const summaryTotal = n(payload?.summary?.publicTradesTotal, NaN);
  if (Number.isFinite(summaryTotal)) return summaryTotal;
  const legacyTotal = n(payload?.totalRows, NaN);
  if (Number.isFinite(legacyTotal)) return legacyTotal;
  return extractPublicTrades(payload).length;
}

function payloadWalletsTotal(payload) {
  const summaryTotal = n(payload?.summary?.indexedWalletsTotal, NaN);
  if (Number.isFinite(summaryTotal)) return summaryTotal;
  const legacyTotal = n(payload?.totalWallets, NaN);
  if (Number.isFinite(legacyTotal)) return legacyTotal;
  return extractWalletPerformance(payload).length;
}

function payloadPositionsTotal(payload) {
  const summaryTotal = n(payload?.summary?.openPositionsTotal, NaN);
  if (Number.isFinite(summaryTotal)) return summaryTotal;
  const legacyTotal = n(payload?.summary?.publicTradesTotal, NaN);
  if (Number.isFinite(legacyTotal)) return legacyTotal;
  return extractPositions(payload).length;
}

function positionsSignature(payload) {
  const positionRows = extractPositions(payload);
  const rows = positionRows.length > 0 ? positionRows : extractPublicTrades(payload);
  const head = asObject(rows[0]);
  return [
    payloadPositionsTotal(payload),
    rows.length,
    tsMs(head.timestamp || head.lastTradeAt || head.updatedAt || head.lastUpdatedAt || 0),
    String(head.historyId || head.id || head.accountId || head.symbol || ""),
  ].join(":");
}

function walletsSignature(payload) {
  const rows = extractWalletPerformance(payload);
  const sample = rows
    .slice(0, 16)
    .map((row) => {
      const item = asObject(row);
      const all = asObject(item.all || item.d24 || item.d30 || {});
      return [
        normalizeWallet(item.wallet || item.walletAddress || ""),
        tsMs(
          all.lastTrade ||
            all.last_trade ||
            item.lastTradeAt ||
            item.lastUpdatedAt ||
            item.updatedAt ||
            item.timestamp ||
            0
        ),
        n(item.totalTrades, 0),
        n(item.totalVolumeUsd ?? item.totalVolume ?? item.volumeUsd, 0),
      ].join("~");
    })
    .join("|");
  return [payloadWalletsTotal(payload), rows.length, sample].join(":");
}

function liveActiveSignature(payload) {
  const rows = extractLiveActiveWallets(payload);
  const sample = rows
    .slice(0, 12)
    .map((row) => {
      const item = asObject(row);
      return [
        normalizeWallet(item.wallet || ""),
        n(item.openPositions, 0),
        n(item.positionUsd, 0),
        tsMs(item.lastActivityAt || item.lastPositionAt || item.liveScannedAt || 0),
      ].join("~");
    })
    .join("|");
  const total = Number.isFinite(n(payload?.summary?.liveActiveWalletsTotal, NaN))
    ? n(payload?.summary?.liveActiveWalletsTotal, rows.length)
    : rows.length;
  return [total, rows.length, sample].join(":");
}

function escapeHtml(value) {
  return String(value == null ? "" : value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function normalizeSide(side) {
  return String(side || "").trim().toLowerCase();
}

function normalizeWallet(value) {
  return String(value || "").trim();
}

function shortWallet(value, head = 6, tail = 6) {
  const wallet = normalizeWallet(value);
  if (!wallet) return "";
  if (wallet.length <= head + tail + 1) return wallet;
  return `${wallet.slice(0, head)}...${wallet.slice(-tail)}`;
}

function shortTx(value, head = 8, tail = 8) {
  const tx = String(value || "").trim();
  if (!tx) return "";
  if (tx.length <= head + tail + 1) return tx;
  return `${tx.slice(0, head)}...${tx.slice(-tail)}`;
}

function walletLooksResolved(value) {
  const wallet = normalizeWallet(value);
  if (!wallet) return false;
  if (wallet === "-") return false;
  const lower = wallet.toLowerCase();
  if (
    lower === "unknown" ||
    lower === "unattributed" ||
    lower === "(none)" ||
    lower === "none" ||
    lower === "null" ||
    lower === "n/a"
  ) {
    return false;
  }
  if (lower.startsWith("market:")) return false;
  return true;
}

function resolveWalletIdentity(rawWallet, options = {}) {
  const wallet = normalizeWallet(rawWallet);
  const tracked = Boolean(options.tracked);
  const allowUnresolved = Boolean(options.allowUnresolved);

  if (walletLooksResolved(wallet)) {
    return {
      wallet,
      label: shortWallet(wallet),
      title: wallet,
      resolved: true,
      kind: tracked ? "tracked" : "resolved",
    };
  }

  if (tracked) {
    return {
      wallet: "",
      label: "Tracked Account",
      title: "Tracked account (address unavailable in payload)",
      resolved: false,
      kind: "tracked_fallback",
    };
  }

  if (allowUnresolved) {
    return {
      wallet: "",
      label: "Unattributed Flow",
      title: "Wallet identity unavailable from source payload",
      resolved: false,
      kind: "unresolved",
    };
  }

  return {
    wallet: "",
    label: "-",
    title: "-",
    resolved: false,
    kind: "empty",
  };
}

function rowFreshnessByTs(timestampMs) {
  if (!timestampMs) return "unknown";
  const age = Date.now() - timestampMs;
  if (age <= 5 * 60 * 1000) return "fresh";
  if (age <= 30 * 60 * 1000) return "cooling";
  return "stale";
}

function clamp(value, min, max) {
  const numeric = n(value, NaN);
  if (!Number.isFinite(numeric)) return min;
  return Math.min(max, Math.max(min, numeric));
}

function titleCase(value) {
  return String(value || "")
    .trim()
    .replaceAll("_", " ")
    .replace(/\s+/g, " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

async function copyTextToClipboard(text) {
  const value = String(text || "").trim();
  if (!value) return false;
  if (navigator?.clipboard?.writeText) {
    await navigator.clipboard.writeText(value);
    return true;
  }
  const input = document.createElement("textarea");
  input.value = value;
  input.setAttribute("readonly", "readonly");
  input.style.position = "fixed";
  input.style.opacity = "0";
  document.body.appendChild(input);
  input.select();
  let copied = false;
  try {
    copied = document.execCommand("copy");
  } finally {
    document.body.removeChild(input);
  }
  return copied;
}

function activityLevelFromAge(ageMs) {
  const age = Math.max(0, n(ageMs, Infinity));
  if (age <= 60 * 1000) return 5;
  if (age <= 5 * 60 * 1000) return 4;
  if (age <= 15 * 60 * 1000) return 3;
  if (age <= 30 * 60 * 1000) return 2;
  if (age <= 60 * 60 * 1000) return 1;
  return 0;
}

function derivePositionState(status, freshness, ageMs) {
  const normalizedStatus = String(status || "").trim().toLowerCase();
  if (normalizedStatus.includes("clos")) return { label: "closing", tone: "down" };
  if (normalizedStatus.includes("reduc")) return { label: "reduced", tone: "down" };
  if (normalizedStatus.includes("increas") || normalizedStatus.includes("add")) {
    return { label: "increased", tone: "up" };
  }
  if (freshness === "stale") return { label: "stale", tone: "stale" };
  if (n(ageMs, Infinity) <= 15 * 60 * 1000) return { label: "new", tone: "up" };
  if (freshness === "fresh") return { label: "live", tone: "fresh" };
  if (freshness === "cooling") return { label: "watch", tone: "cool" };
  return { label: normalizedStatus || freshness || "watch", tone: "cool" };
}

function topSymbolSharePct(symbolVolumes, totalVolumeUsd) {
  const values = Object.values(asObject(symbolVolumes))
    .map((value) => n(value, NaN))
    .filter((value) => Number.isFinite(value) && value > 0);
  const total = n(totalVolumeUsd, NaN);
  if (!values.length || !Number.isFinite(total) || total <= 0) return NaN;
  return (Math.max(...values) / total) * 100;
}

function computeWalletScores(metrics) {
  const exposureFactor = Math.min(1, Math.max(0, n(metrics.exposureUsd, 0)) / 250000);
  const avgTradeUsd =
    n(metrics.totalTrades, 0) > 0 ? n(metrics.totalVolumeUsd, 0) / Math.max(1, n(metrics.totalTrades, 0)) : 0;
  const avgTradeFactor = Math.min(1, avgTradeUsd / 25000);
  const liveEventFactor = Math.min(1, n(metrics.recentEvents15m, 0) / 15);
  const positiveWindows = [metrics.pnl7Usd, metrics.pnl30Usd, metrics.totalPnlUsd].filter(
    (value) => Number.isFinite(n(value, NaN)) && n(value, 0) > 0
  ).length;
  const winRate = clamp(metrics.winRate, 0, 100);
  const concentrationPct = clamp(metrics.concentrationPct, 0, 100);

  const convictionScore = clamp(
    avgTradeFactor * 35 +
      exposureFactor * 30 +
      Math.min(n(metrics.openPositions, 0), 12) * 2 +
      liveEventFactor * 11 +
      concentrationPct * 0.12,
    0,
    100
  );
  const consistencyScore = clamp(
    winRate * 0.58 + positiveWindows * 12 + (n(metrics.totalTrades, 0) > 20 ? 12 : n(metrics.totalTrades, 0) > 5 ? 6 : 0),
    0,
    100
  );
  const riskScore = clamp(
    concentrationPct * 0.55 +
      exposureFactor * 25 +
      (metrics.freshness === "stale" ? 10 : 0) +
      (n(metrics.openPositions, 0) <= 1 && exposureFactor > 0.2 ? 12 : 0),
    0,
    100
  );
  const liveActiveScore = clamp(
    liveEventFactor * 45 +
      Math.min(n(metrics.openPositions, 0), 20) * 1.5 +
      exposureFactor * 25 +
      (metrics.freshness === "fresh" ? 20 : metrics.freshness === "cooling" ? 12 : metrics.freshness === "stale" ? 0 : 6),
    0,
    100
  );

  return {
    convictionScore,
    consistencyScore,
    concentrationScore: concentrationPct,
    riskScore,
    liveActiveScore,
  };
}

function deriveWalletBehaviorTags(metrics) {
  const tags = [];
  if (n(metrics.recentEvents15m, 0) >= 10) tags.push("live burst");
  if (n(metrics.openPositions, 0) >= 12) tags.push("broad book");
  if (n(metrics.concentrationPct, 0) >= 55) tags.push("concentrated");
  if (n(metrics.winRate, 0) >= 55 && n(metrics.totalPnlUsd, 0) > 0) tags.push("disciplined");
  if (n(metrics.pnl7Usd, 0) > 0 && n(metrics.pnl30Usd, 0) > 0) tags.push("trend intact");
  if (n(metrics.increaseCount, 0) >= 3 && n(metrics.increaseCount, 0) > n(metrics.reduceCount, 0)) {
    tags.push("scales in");
  }
  if (n(metrics.reduceCount, 0) >= 3 && n(metrics.reduceCount, 0) >= n(metrics.increaseCount, 0)) {
    tags.push("scales out");
  }
  if (n(metrics.avgHoldingMs, 0) >= 24 * 60 * 60 * 1000) tags.push("patient holds");
  if (
    n(metrics.avgHoldingMs, 0) > 0 &&
    n(metrics.avgHoldingMs, 0) <= 2 * 60 * 60 * 1000 &&
    n(metrics.totalTrades, 0) >= 15
  ) {
    tags.push("fast rotation");
  }
  if (metrics.freshness === "stale") tags.push("stale watch");
  if (n(metrics.totalPnlUsd, 0) < 0 && n(metrics.recentEvents1h, 0) > 0) tags.push("drawdown");
  if (!tags.length && n(metrics.openPositions, 0) === 0) tags.push("quiet");
  return tags.slice(0, 3);
}

function scoreTone(score, inverse = false) {
  const numeric = n(score, NaN);
  if (!Number.isFinite(numeric)) return "mid";
  if (inverse) {
    if (numeric >= 70) return "bad";
    if (numeric >= 45) return "low";
    return "high";
  }
  if (numeric >= 70) return "high";
  if (numeric >= 45) return "mid";
  return "low";
}

function walletWinRateProfile(row) {
  if (n(row.trades7, 0) >= 8 && Number.isFinite(n(row.winRate7, NaN))) {
    return {
      valid: true,
      rate: n(row.winRate7, NaN),
      trades: n(row.trades7, 0),
      windowLabel: "7D",
    };
  }
  if (n(row.trades30, 0) >= 15 && Number.isFinite(n(row.winRate30, NaN))) {
    return {
      valid: true,
      rate: n(row.winRate30, NaN),
      trades: n(row.trades30, 0),
      windowLabel: "30D",
    };
  }
  if (n(row.totalTrades, 0) >= 30 && Number.isFinite(n(row.winRate, NaN))) {
    return {
      valid: true,
      rate: n(row.winRate, NaN),
      trades: n(row.totalTrades, 0),
      windowLabel: "All",
    };
  }
  return {
    valid: false,
    rate: NaN,
    trades: n(row.totalTrades, 0),
    windowLabel: "",
  };
}

function walletPerformanceFocus(row) {
  const pnl30 = n(row.pnl30Usd, NaN);
  const pnl7 = n(row.pnl7Usd, NaN);
  if (Number.isFinite(pnl30) && Math.abs(pnl30) > 0) {
    return {
      value: pnl30,
      label: "30D PnL",
      text: fmtSignedUsd(pnl30, 0),
    };
  }
  if (Number.isFinite(pnl7) && Math.abs(pnl7) > 0) {
    return {
      value: pnl7,
      label: "7D PnL",
      text: fmtSignedUsd(pnl7, 0),
    };
  }
  return {
    value: n(row.totalPnlUsd, 0),
    label: "Total PnL",
    text: fmtSignedUsd(row.totalPnlUsd, 0),
  };
}

function buildWalletRankingGroups(rows) {
  const list = safeArray(rows).filter((row) => row && row.walletResolved);
  const buildGroup = (key, title, subtitle, tone, rankedRows) => ({
    key,
    title,
    subtitle,
    tone,
    rankedRows,
    count: rankedRows.length,
  });

  const mostActive = 
    list
      .filter(
        (row) =>
          n(row.recentEvents15m, 0) > 0 ||
          n(row.recentEvents1h, 0) > 0 ||
          n(row.openPositions, 0) > 0
      )
      .sort(
        (a, b) =>
          n(b.recentEvents15m, 0) - n(a.recentEvents15m, 0) ||
          n(b.recentEvents1h, 0) - n(a.recentEvents1h, 0) ||
          n(b.liveActiveScore, 0) - n(a.liveActiveScore, 0) ||
          tsMs(b.lastActivity) - tsMs(a.lastActivity)
      )
  ;

  const highestVolume =
    list
      .filter(
        (row) =>
          n(row.volume7Usd, 0) > 0 ||
          n(row.volume30Usd, 0) > 0 ||
          n(row.totalVolumeUsd, 0) > 0 ||
          n(row.exposureUsd, 0) > 0
      )
      .sort(
        (a, b) =>
          Math.max(n(b.volume7Usd, 0), n(b.volume30Usd, 0), n(b.totalVolumeUsd, 0)) -
            Math.max(n(a.volume7Usd, 0), n(a.volume30Usd, 0), n(a.totalVolumeUsd, 0)) ||
          n(b.exposureUsd, 0) - n(a.exposureUsd, 0)
      )
  ;

  const highestWinRate =
    list
      .map((row) => ({ row, profile: walletWinRateProfile(row) }))
      .filter((entry) => entry.profile.valid)
      .sort(
        (a, b) =>
          n(b.profile.rate, 0) - n(a.profile.rate, 0) ||
          n(b.profile.trades, 0) - n(a.profile.trades, 0) ||
          n(b.row.totalPnlUsd, 0) - n(a.row.totalPnlUsd, 0)
      )
      .map((entry) => ({ ...entry.row, winRateProfile: entry.profile }))
  ;

  const strongestPnl =
    list
      .filter(
        (row) =>
          Number.isFinite(n(row.pnl30Usd, NaN)) ||
          Number.isFinite(n(row.pnl7Usd, NaN)) ||
          Number.isFinite(n(row.totalPnlUsd, NaN))
      )
      .sort(
        (a, b) =>
          walletPerformanceFocus(b).value - walletPerformanceFocus(a).value ||
          n(b.totalPnlUsd, 0) - n(a.totalPnlUsd, 0)
      )
  ;

  const highestConviction =
    list
      .filter((row) => n(row.convictionScore, 0) > 0 || n(row.exposureUsd, 0) > 0)
      .sort(
        (a, b) =>
          n(b.convictionScore, 0) - n(a.convictionScore, 0) ||
          n(b.exposureUsd, 0) - n(a.exposureUsd, 0) ||
          n(b.concentrationPct, 0) - n(a.concentrationPct, 0)
      )
  ;

  const mostConsistent =
    list
      .filter((row) => n(row.totalTrades, 0) >= 20 && n(row.consistencyScore, 0) > 0)
      .sort(
        (a, b) =>
          n(b.consistencyScore, 0) - n(a.consistencyScore, 0) ||
          n(b.totalTrades, 0) - n(a.totalTrades, 0) ||
          n(b.totalPnlUsd, 0) - n(a.totalPnlUsd, 0)
      )
  ;

  return [
    buildGroup("active", "Most Active Wallets", "Highest recent live activity and position motion.", "cool", mostActive),
    buildGroup("volume", "Highest Volume Wallets", "Largest recent traded volume and exposure.", "cool", highestVolume),
    buildGroup("winrate", "Highest Win Rate Wallets", "Best hit rate where sample size is meaningful.", "up", highestWinRate),
    buildGroup("pnl", "Strongest PnL Wallets", "Best realized + live performance windows.", "up", strongestPnl),
    buildGroup("conviction", "Highest Conviction Wallets", "Concentrated exposure with stronger persistence.", "cool", highestConviction),
    buildGroup("consistency", "Most Consistent Wallets", "Repeatable quality over a meaningful sample.", "up", mostConsistent),
  ].filter((group) => group.count > 0);
}

function walletGroupMetric(groupKey, row) {
  if (groupKey === "active") {
    return {
      primary: `${fmtInt(row.recentEvents15m || 0)} ev / 15m`,
      secondary: `${fmtInt(row.openPositions || 0)} open • ${fmtAgo(row.lastActivity)}`,
    };
  }
  if (groupKey === "volume") {
    const primaryValue =
      n(row.volume7Usd, 0) > 0 ? row.volume7Usd : n(row.volume30Usd, 0) > 0 ? row.volume30Usd : row.totalVolumeUsd;
    const primaryLabel = n(row.volume7Usd, 0) > 0 ? "7D" : n(row.volume30Usd, 0) > 0 ? "30D" : "All";
    return {
      primary: `${primaryLabel} ${fmtUsd(primaryValue, 0)}`,
      secondary: `${fmtUsd(row.exposureUsd, 0)} live exposure`,
    };
  }
  if (groupKey === "winrate") {
    const profile = row.winRateProfile || walletWinRateProfile(row);
    return {
      primary: `${fmtPct(profile.rate, 1)} ${profile.windowLabel}`,
      secondary: `${fmtInt(profile.trades)} trades • ${fmtSignedUsd(row.totalPnlUsd, 0)}`,
    };
  }
  if (groupKey === "pnl") {
    const focus = walletPerformanceFocus(row);
    return {
      primary: `${focus.label} ${focus.text}`,
      secondary: `Total ${fmtSignedUsd(row.totalPnlUsd, 0)}`,
    };
  }
  if (groupKey === "conviction") {
    return {
      primary: `Conviction ${fmtInt(row.convictionScore || 0)}`,
      secondary: `${fmtUsd(row.exposureUsd, 0)} • ${fmtInt(row.concentrationPct || 0)}% concentration`,
    };
  }
  return {
    primary: `Consistency ${fmtInt(row.consistencyScore || 0)}`,
    secondary: `${fmtPct(row.winRate, 1)} • ${fmtInt(row.totalTrades || 0)} trades`,
  };
}

function getUniqueValues(rows, key) {
  return Array.from(
    new Set(
      rows
        .map((row) => String(row?.[key] || "").trim())
        .filter(Boolean)
        .sort((a, b) => a.localeCompare(b))
    )
  );
}

function buildRows(payload, options = {}) {
  const includePositions = options.includePositions !== false;
  const includeWallets = options.includeWallets !== false;
  const includeLiveActive = options.includeLiveActive !== false;
  const account = normalizeWallet(payload?.environment?.account || "");
  const publicTrades = extractPublicTrades(payload);
  const positionsPrimary = includePositions ? extractPositions(payload) : [];
  const positionsFallback = includePositions ? extractPositions(payload) : [];
  const walletPerformance = includeWallets ? extractWalletPerformance(payload) : [];
  const liveActiveSourceRows =
    includePositions || includeWallets || includeLiveActive
      ? extractLiveActiveWallets(payload).map((row, idx) => {
          const item = asObject(row);
          const walletIdentity = resolveWalletIdentity(item.wallet || "", {
            tracked: true,
            allowUnresolved: true,
          });
          return {
            key: `la:${item.wallet || "na"}:${idx}`,
            rank: idx + 1,
            wallet: walletIdentity.wallet,
            walletLabel: walletIdentity.label,
            walletTitle: walletIdentity.title,
            walletResolved: walletIdentity.resolved,
            trackedWallet: true,
            freshness: String(item.freshness || "unknown").trim().toLowerCase(),
            openPositions: n(item.openPositions, 0),
            positionUsd: n(item.positionUsd, 0),
            recentEvents15m: n(item.recentEvents15m, 0),
            recentEvents1h: n(item.recentEvents1h, 0),
            lastActivityAt: tsMs(item.lastActivityAt || 0),
            lastPositionAt: tsMs(item.lastPositionAt || 0),
            liveScannedAt: tsMs(item.liveScannedAt || 0),
            symbols: safeArray(item.symbols).map((value) => String(value || "").trim()).filter(Boolean),
            d24: asObject(item.d24),
            d30: asObject(item.d30),
            all: asObject(item.all),
            activityScore: n(item.activityScore, NaN),
            raw: item,
          };
        })
      : [];
  const liveActiveByWallet = new Map(
    liveActiveSourceRows
      .filter((row) => walletLooksResolved(row.wallet))
      .map((row) => [normalizeWallet(row.wallet), row])
  );

  const trackedWallets = includeWallets
    ? new Set(
        walletPerformance
          .map((row) => normalizeWallet(row?.wallet || row?.walletAddress))
          .filter((wallet) => walletLooksResolved(wallet))
      )
    : new Set(
        safeArray(state.rows?.wallets)
          .map((row) => normalizeWallet(row?.wallet))
          .filter((wallet) => walletLooksResolved(wallet))
      );

  const positionRows = includePositions
    ? (() => {
        const positionRowsFromPrimary = positionsPrimary
          .map((row, idx) => {
            const item = asObject(row);
            const rawSnapshot = asObject(item.raw);
            const timestamp = tsMs(
              item.timestamp ||
                item.updatedAt ||
                item.lastTradeAt ||
                item.lastUpdatedAt ||
                item.openedAt ||
                0
            );
            const rawWallet = normalizeWallet(
              item.wallet ||
                item.walletAddress ||
                item.owner ||
                item.trader ||
                item.account ||
                item.authority ||
                item.user ||
                ""
            );
            const tracked =
              Boolean(item.trackedWallet) ||
              (walletLooksResolved(rawWallet) ? trackedWallets.has(rawWallet) : false);
            const walletIdentity = resolveWalletIdentity(rawWallet, {
              tracked,
              allowUnresolved: true,
            });
            const side = normalizeSide(item.side || item.rawSide || "");
            const size = Math.abs(n(item.size, n(item.amount, n(item.qty, 0))));
            const entry = n(
              item.entry,
              n(item.entryPrice, n(item.entry_price, n(item.price, NaN)))
            );
            const mark = n(
              item.mark,
              n(item.markPrice, n(item.mark_price, n(item.price, entry)))
            );
            const positionUsd = n(
              item.positionUsd,
              n(item.notionalUsd, Number.isFinite(mark) ? Math.abs(size * mark) : Number.isFinite(entry) ? Math.abs(size * entry) : 0)
            );
            const margin = n(
              item.margin,
              n(item.marginUsd, n(item.margin_usd, n(rawSnapshot.margin, n(rawSnapshot.margin_usd, NaN))))
            );
            const funding = n(item.funding, n(item.fundingUsd, n(rawSnapshot.funding, NaN)));
            const pnl = n(item.pnl, n(item.unrealizedPnlUsd, n(item.unrealizedPnl, NaN)));
            const liquidationPrice = n(
              item.liquidationPrice,
              n(item.liquidation_price, n(rawSnapshot.liquidation_price, NaN))
            );
            const lifecycle = asObject(item.lifecycle);
            const openedAt = tsMs(item.openedAt || item.createdAt || item.openTime || rawSnapshot.created_at || timestamp);
            const lifecycleAgeMs = n(lifecycle.currentAgeMs, NaN);
            const ageMs = openedAt
              ? Math.max(0, Date.now() - openedAt)
              : Number.isFinite(lifecycleAgeMs)
              ? lifecycleAgeMs
              : NaN;
            const baseFreshness = String(item.freshness || "").trim().toLowerCase();
            const freshness = walletIdentity.resolved
              ? baseFreshness || rowFreshnessByTs(timestamp)
              : "unresolved";
            const statusRaw = String(item.status || lifecycle.lastChangeType || "").trim().toLowerCase();
            const status = walletIdentity.resolved ? statusRaw || freshness : "unresolved";
            const positionState = derivePositionState(status, freshness, ageMs);
            const leverage =
              Number.isFinite(margin) && margin > 0 && Number.isFinite(positionUsd) ? positionUsd / margin : NaN;
            const roe = Number.isFinite(margin) && margin > 0 && Number.isFinite(pnl) ? (pnl / margin) * 100 : NaN;
            const walletConfidence = String(
              item.walletConfidence || (walletIdentity.resolved ? "hard_payload" : "unresolved")
            )
              .trim()
              .toLowerCase();
            const txSignature = String(item.txSignature || item.signature || item.txid || "").trim();
            const tradeRefRaw =
              item.tradeRef || item.positionKey || item.historyId || item.orderId || item.i || item.li || null;
            const tradeRef = tradeRefRaw !== null && tradeRefRaw !== undefined ? String(tradeRefRaw) : "";
            const tradeRefType = String(
              item.tradeRefType ||
                (item.positionKey
                  ? "wallet_position"
                  : item.historyId
                  ? "history_id"
                  : item.orderId || item.i
                  ? "order_id"
                  : item.li
                  ? "li"
                  : "unknown")
            );
            const txConfidence = String(item.txConfidence || (txSignature ? "hard_payload" : "unresolved"))
              .trim()
              .toLowerCase();
            const liveWallet = walletIdentity.resolved ? liveActiveByWallet.get(walletIdentity.wallet) : null;
            return {
              key: `p:pos:${item.positionKey || item.id || item.historyId || `${item.symbol || "na"}:${item.side || "na"}:${idx}`}`,
              timestamp,
              wallet: walletIdentity.wallet,
              walletLabel: walletIdentity.label,
              walletTitle: walletIdentity.title,
              walletResolved: walletIdentity.resolved,
              trackedWallet: tracked,
              symbol: String(item.symbol || "-").toUpperCase(),
              side,
              size,
              positionUsd,
              entry,
              mark,
              pnl,
              margin,
              funding,
              leverage,
              roe,
              liquidationPrice,
              status,
              freshness,
              positionState,
              walletSource: String(item.walletSource || "").trim().toLowerCase() || "wallet_positions_api",
              walletConfidence,
              txSignature,
              txSource: String(item.txSource || (txSignature ? "payload" : "wallet_positions_api"))
                .trim()
                .toLowerCase(),
              txConfidence,
              tradeRef,
              tradeRefType,
              openedAt,
              updatedAt: timestamp,
              ageMs,
              activityLevel: activityLevelFromAge(Date.now() - timestamp),
              walletLiveRank: liveWallet?.rank || null,
              walletOpenPositions: liveWallet?.openPositions || 0,
              walletRecentEvents15m: liveWallet?.recentEvents15m || 0,
              walletRecentEvents1h: liveWallet?.recentEvents1h || 0,
              lifecycle,
              source: String(item.source || "wallet_first_positions"),
              raw: item,
            };
          })
          .sort(
            (a, b) =>
              Number(b.openedAt || 0) - Number(a.openedAt || 0) ||
              Number(b.updatedAt || 0) - Number(a.updatedAt || 0)
          );

        if (positionRowsFromPrimary.length > 0) return positionRowsFromPrimary;

        const positionRowsFromPublic = publicTrades
          .map((row, idx) => {
            const item = asObject(row);
            const timestamp = tsMs(
              item.timestamp ||
                item.updatedAt ||
                item.lastTradeAt ||
                item.lastUpdatedAt ||
                item.openedAt ||
                0
            );
            const rawWallet = normalizeWallet(
              item.wallet ||
                item.walletAddress ||
                item.owner ||
                item.trader ||
                item.account ||
                item.authority ||
                item.user ||
                ""
            );
            const tracked = walletLooksResolved(rawWallet) ? trackedWallets.has(rawWallet) : false;
            const walletIdentity = resolveWalletIdentity(rawWallet, {
              tracked,
              allowUnresolved: true,
            });
            const side = normalizeSide(item.side || "");
            const size = Math.abs(n(item.amount, n(item.size, n(item.qty, 0))));
            const entry = n(item.entry, n(item.price, n(item.entryPrice, NaN)));
            const mark = n(item.mark, n(item.price, n(item.currentPrice, NaN)));
            const positionUsd = n(
              item.notionalUsd,
              n(item.openNotional, Number.isFinite(entry) ? size * entry : 0)
            );
            const pnl = n(item.pnl, n(item.unrealizedPnlUsd, n(item.unrealizedPnl, NaN)));
            const openedAt = timestamp;
            const ageMs = openedAt ? Math.max(0, Date.now() - openedAt) : NaN;
            const freshness = walletIdentity.resolved ? rowFreshnessByTs(timestamp) : "unresolved";
            const cause = String(item.cause || "").trim().toLowerCase();
            const status = walletIdentity.resolved ? cause || freshness : "unresolved";
            const positionState = derivePositionState(status, freshness, ageMs);
            const walletConfidence = String(
              item.walletConfidence || (walletIdentity.resolved ? "hard_payload" : "unresolved")
            )
              .trim()
              .toLowerCase();
            const txSignature = String(item.txSignature || item.signature || item.txid || "").trim();
            const tradeRefRaw =
              item.tradeRef || item.historyId || item.orderId || item.i || item.li || null;
            const tradeRef = tradeRefRaw !== null && tradeRefRaw !== undefined ? String(tradeRefRaw) : "";
            const tradeRefType = String(item.tradeRefType || (item.historyId ? "history_id" : item.orderId || item.i ? "order_id" : item.li ? "li" : "unknown"));
            const txConfidence = String(item.txConfidence || (txSignature ? "hard_payload" : "unresolved"))
              .trim()
              .toLowerCase();
            const liveWallet = walletIdentity.resolved ? liveActiveByWallet.get(walletIdentity.wallet) : null;
            return {
              key: `p:pub:${item.historyId || item.id || item.accountId || idx}`,
              timestamp,
              wallet: walletIdentity.wallet,
              walletLabel: walletIdentity.label,
              walletTitle: walletIdentity.title,
              walletResolved: walletIdentity.resolved,
              trackedWallet: tracked,
              symbol: String(item.symbol || "-").toUpperCase(),
              side,
              size,
              positionUsd,
              entry,
              mark,
              pnl,
              margin: NaN,
              funding: NaN,
              leverage: NaN,
              roe: NaN,
              liquidationPrice: NaN,
              status,
              freshness,
              positionState,
              walletSource: String(item.walletSource || "").trim().toLowerCase(),
              walletConfidence,
              txSignature,
              txSource: String(item.txSource || (txSignature ? "payload" : "unresolved")).trim().toLowerCase(),
              txConfidence,
              tradeRef,
              tradeRefType,
              openedAt,
              updatedAt: timestamp,
              ageMs,
              activityLevel: activityLevelFromAge(Date.now() - timestamp),
              walletLiveRank: liveWallet?.rank || null,
              walletOpenPositions: liveWallet?.openPositions || 0,
              walletRecentEvents15m: liveWallet?.recentEvents15m || 0,
              walletRecentEvents1h: liveWallet?.recentEvents1h || 0,
              source: "exchange_public_trade",
              raw: item,
            };
          })
          .sort(
            (a, b) =>
              Number(b.openedAt || 0) - Number(a.openedAt || 0) ||
              Number(b.updatedAt || 0) - Number(a.updatedAt || 0)
          );

        const positionRowsFromFallback = positionsFallback
          .map((row, idx) => {
            const item = asObject(row);
            const timestamp = tsMs(item.updatedAt || item.timestamp || item.openedAt || 0);
            const rawWallet = normalizeWallet(item.wallet || item.account || account || "");
            const tracked = walletLooksResolved(rawWallet) ? trackedWallets.has(rawWallet) : true;
            const walletIdentity = resolveWalletIdentity(rawWallet, {
              tracked,
              allowUnresolved: true,
            });
            const freshness = walletIdentity.resolved ? rowFreshnessByTs(timestamp) : "unresolved";
            const walletConfidence = walletIdentity.resolved ? "hard_payload" : "unresolved";
            const openedAt = tsMs(item.openedAt || item.openTime || 0);
            const ageMs = openedAt ? Math.max(0, Date.now() - openedAt) : NaN;
            const pnl = n(item.unrealizedPnlUsd, NaN);
            const margin = n(item.margin, n(item.marginUsd, NaN));
            const positionUsd = n(item.notionalUsd, 0);
            const leverage =
              Number.isFinite(margin) && margin > 0 && Number.isFinite(positionUsd) ? positionUsd / margin : NaN;
            const roe = Number.isFinite(margin) && margin > 0 && Number.isFinite(pnl) ? (pnl / margin) * 100 : NaN;
            const positionState = derivePositionState(item.riskTag || freshness, freshness, ageMs);
            const liveWallet = walletIdentity.resolved ? liveActiveByWallet.get(walletIdentity.wallet) : null;
            return {
              key: `p:fb:${item.symbol || "na"}:${item.side || "na"}:${idx}`,
              timestamp,
              wallet: walletIdentity.wallet,
              walletLabel: walletIdentity.label,
              walletTitle: walletIdentity.title,
              walletResolved: walletIdentity.resolved,
              trackedWallet: tracked,
              symbol: String(item.symbol || "-").toUpperCase(),
              side: normalizeSide(item.side || ""),
              size: n(item.amountAbs, n(item.size, 0)),
              positionUsd,
              entry: n(item.entryPrice, NaN),
              mark: n(item.markPrice, NaN),
              pnl,
              margin,
              funding: n(item.funding, NaN),
              leverage,
              roe,
              liquidationPrice: n(item.liquidationPrice, NaN),
              status: walletIdentity.resolved ? (item.riskTag || freshness) : "unresolved",
              freshness,
              positionState,
              walletSource: "account_positions_fallback",
              walletConfidence,
              txSignature: "",
              txSource: "unresolved",
              txConfidence: "unresolved",
              tradeRef: "",
              tradeRefType: "unknown",
              openedAt,
              updatedAt: timestamp,
              ageMs,
              activityLevel: activityLevelFromAge(Date.now() - timestamp),
              walletLiveRank: liveWallet?.rank || null,
              walletOpenPositions: liveWallet?.openPositions || 0,
              walletRecentEvents15m: liveWallet?.recentEvents15m || 0,
              walletRecentEvents1h: liveWallet?.recentEvents1h || 0,
              source: "account_positions_fallback",
              raw: item,
            };
          })
          .sort(
            (a, b) =>
              Number(b.openedAt || 0) - Number(a.openedAt || 0) ||
              Number(b.updatedAt || 0) - Number(a.updatedAt || 0)
          );

        return positionRowsFromPublic.length > 0 ? positionRowsFromPublic : positionRowsFromFallback;
      })()
    : null;

  const walletRows = includeWallets
    ? walletPerformance
        .map((row, idx) => {
          const item = asObject(row);
          const all = asObject(item.all || item.d24 || item.d30 || {});
          const d24 = asObject(item.d24);
          const d7 = asObject(item.d7);
          const d30 = asObject(item.d30);
          const walletIdentity = resolveWalletIdentity(item.wallet || item.walletAddress || "", {
            tracked: true,
            allowUnresolved: true,
          });
          const liveWallet = walletIdentity.resolved ? liveActiveByWallet.get(walletIdentity.wallet) : null;
          const historicalLastActivity = tsMs(
            all.lastTrade ||
              all.last_trade ||
              item.lastTradeAt ||
              item.lastUpdatedAt ||
              item.updatedAt ||
              item.timestamp ||
              0
          );
          const liveLastActivity = tsMs(
            liveWallet?.lastActivityAt || liveWallet?.lastPositionAt || liveWallet?.liveScannedAt || 0
          );
          const lastActivity = Math.max(historicalLastActivity, liveLastActivity);
          const freshness = walletIdentity.resolved ? rowFreshnessByTs(lastActivity) : "unresolved";
          const realizedPnlUsd = n(
            all.realizedPnlUsd ??
              all.realized_pnl_usd ??
              all.pnlUsd ??
              item.realizedPnlUsd ??
              item.pnlUsd ??
              item.pnlAllTime,
            NaN
          );
          const unrealizedPnlUsd = n(
            all.unrealizedPnlUsd ?? all.unrealized_pnl_usd ?? item.unrealizedPnlUsd ?? item.unrealizedPnl,
            NaN
          );
          const totalVolumeUsd = n(all.volumeUsd ?? item.volumeUsd ?? item.totalVolume, 0);
          const totalTrades = n(all.trades ?? item.totalTrades, 0);
          const feesPaidUsd = n(all.feesPaidUsd ?? all.fees_paid_usd ?? item.feesPaidUsd, NaN);
          const rawWinRate = n(
            all.winRatePct ?? item.winRatePct ?? item.winRate ?? item.winRate30d,
            NaN
          );
          const winRate =
            Number.isFinite(rawWinRate) && rawWinRate >= 0 && rawWinRate <= 1
              ? rawWinRate * 100
              : rawWinRate;
          const pnl24Usd = n(d24.pnlUsd ?? d24.realizedPnlUsd, NaN);
          const pnl7Usd = n(d7.pnlUsd ?? d7.realizedPnlUsd, NaN);
          const pnl30Usd = n(d30.pnlUsd ?? d30.realizedPnlUsd, NaN);
          const topSymbolSharePctValue = topSymbolSharePct(all.symbolVolumes, totalVolumeUsd);
          const openPositions = n(liveWallet?.openPositions, 0);
          const exposureUsd = n(liveWallet?.positionUsd, 0);
          const recentEvents15m = n(liveWallet?.recentEvents15m, 0);
          const recentEvents1h = n(liveWallet?.recentEvents1h, 0);
          const lifecycle = asObject(item.lifecycle);
          const avgHoldingMs = n(
            lifecycle.avgObservedHoldingMs ??
              lifecycle.avgCompletedHoldingMs ??
              lifecycle.currentAvgOpenAgeMs,
            NaN
          );
          const increaseCount = n(lifecycle.increaseCount, 0);
          const reduceCount = n(lifecycle.reduceCount, 0);
          const closeCount = n(lifecycle.closeCount, 0);
          const completedPositionCount = n(lifecycle.completedPositionCount, 0);
          const derivedScores = computeWalletScores({
            exposureUsd,
            totalVolumeUsd,
            totalTrades,
            recentEvents15m,
            openPositions,
            concentrationPct: topSymbolSharePctValue,
            winRate,
            pnl7Usd,
            pnl24Usd,
            pnl30Usd,
            totalPnlUsd:
              (walletIdentity.resolved && Number.isFinite(realizedPnlUsd) ? realizedPnlUsd : 0) +
              (walletIdentity.resolved && Number.isFinite(unrealizedPnlUsd) ? unrealizedPnlUsd : 0),
            freshness,
          });
          const totalPnlUsd =
            (walletIdentity.resolved && Number.isFinite(realizedPnlUsd) ? realizedPnlUsd : 0) +
            (walletIdentity.resolved && Number.isFinite(unrealizedPnlUsd) ? unrealizedPnlUsd : 0);
          const behaviorTags = deriveWalletBehaviorTags({
            recentEvents15m,
            recentEvents1h,
            openPositions,
            concentrationPct: topSymbolSharePctValue,
            winRate,
            pnl7Usd,
            totalPnlUsd,
            freshness,
            increaseCount,
            reduceCount,
            avgHoldingMs,
            totalTrades,
          });
          return {
            key: `w:${item.wallet || "na"}:${idx}`,
            wallet: walletIdentity.wallet || normalizeWallet(item.walletAddress || ""),
            walletLabel: walletIdentity.label,
            walletTitle: walletIdentity.title,
            walletResolved: walletIdentity.resolved,
            trackedWallet: true,
            totalTrades: walletIdentity.resolved ? totalTrades : NaN,
            totalVolumeUsd: walletIdentity.resolved ? totalVolumeUsd : NaN,
            realizedPnlUsd: walletIdentity.resolved ? realizedPnlUsd : NaN,
            unrealizedPnlUsd: walletIdentity.resolved ? unrealizedPnlUsd : NaN,
            totalPnlUsd,
            winRate: walletIdentity.resolved ? winRate : NaN,
            feesPaidUsd: walletIdentity.resolved ? feesPaidUsd : NaN,
            lastActivity,
            freshness,
            status: freshness,
            pnl24Usd,
            pnl7Usd,
            pnl30Usd,
            trades24: n(d24.trades, NaN),
            trades7: n(d7.trades, NaN),
            trades30: n(d30.trades, NaN),
            volume24Usd: n(d24.volumeUsd, NaN),
            volume7Usd: n(d7.volumeUsd, NaN),
            volume30Usd: n(d30.volumeUsd, NaN),
            winRate7: n(d7.winRatePct, NaN),
            winRate30: n(d30.winRatePct, NaN),
            liveActiveRank: liveWallet?.rank || null,
            openPositions,
            exposureUsd,
            recentEvents15m,
            recentEvents1h,
            activityScore: n(liveWallet?.activityScore, NaN),
            concentrationPct: topSymbolSharePctValue,
            avgHoldingMs,
            increaseCount,
            reduceCount,
            closeCount,
            completedPositionCount,
            topSymbols: Object.entries(asObject(all.symbolVolumes))
              .map(([symbol, value]) => [String(symbol || "").trim(), n(value, NaN)])
              .filter((entry) => entry[0] && Number.isFinite(entry[1]))
              .sort((a, b) => b[1] - a[1])
              .slice(0, 5),
            behaviorTags,
            lifecycle,
            ...derivedScores,
            raw: item,
          };
        })
        .sort((a, b) => n(b.liveActiveScore, 0) - n(a.liveActiveScore, 0) || n(b.totalPnlUsd, 0) - n(a.totalPnlUsd, 0))
    : null;
  const liveActiveRows = includeLiveActive ? liveActiveSourceRows : null;

  return { positions: positionRows, wallets: walletRows, liveActive: liveActiveRows };
}

function refreshFilterOptions(rows) {
  const symbolSelect = el("lt-filter-symbol");
  const sideSelect = el("lt-filter-side");
  const freshnessSelect = el("lt-filter-freshness");
  if (!symbolSelect || !sideSelect || !freshnessSelect) return;

  const currentSymbol = symbolSelect.value;
  const currentSide = sideSelect.value;
  const currentFreshness = freshnessSelect.value;

  const symbols = getUniqueValues(rows.positions, "symbol");
  const sides = getUniqueValues(rows.positions, "side");
  const freshnessStates = Array.from(
    new Set([...getUniqueValues(rows.positions, "freshness"), ...getUniqueValues(rows.wallets, "freshness")])
  ).sort((a, b) => a.localeCompare(b));

  symbolSelect.innerHTML = ["<option value=\"\">All</option>"]
    .concat(symbols.map((value) => `<option value=\"${escapeHtml(value)}\">${escapeHtml(value)}</option>`))
    .join("");

  sideSelect.innerHTML = ["<option value=\"\">All</option>"]
    .concat(sides.map((value) => `<option value=\"${escapeHtml(value)}\">${escapeHtml(value)}</option>`))
    .join("");

  freshnessSelect.innerHTML = ["<option value=\"\">All</option>"]
    .concat(freshnessStates.map((value) => `<option value=\"${escapeHtml(value)}\">${escapeHtml(value)}</option>`))
    .join("");

  if (symbols.includes(currentSymbol)) symbolSelect.value = currentSymbol;
  if (sides.includes(currentSide)) sideSelect.value = currentSide;
  if (freshnessStates.includes(currentFreshness)) freshnessSelect.value = currentFreshness;
}

function matchesSearch(row, query) {
  if (!query) return true;
  const haystack = [
    row.wallet,
    row.walletLabel,
    row.walletTitle,
    row.symbol,
    row.side,
    row.status,
    row.totalTrades,
    row.totalVolumeUsd,
    row.realizedPnlUsd,
    row.unrealizedPnlUsd,
    row.positionUsd,
    row.tradeRef,
    row.tradeRefType,
    row.txSignature,
    row.txSource,
  ]
    .join(" ")
    .toLowerCase();
  return haystack.includes(query);
}

function withinRange(value, minValue, maxValue) {
  const min = n(minValue, 0);
  const max = n(maxValue, 0);
  const numeric = n(value, NaN);
  if (!Number.isFinite(numeric)) return min === 0 && max === 0;
  if (min !== 0 && numeric < min) return false;
  if (max !== 0 && numeric > max) return false;
  return true;
}

function resetPagination() {
  state.pagination.positions.page = 1;
  state.pagination.wallets.page = 1;
}

function currentWindowParams() {
  const positions = state.pagination.positions || { page: 1, pageSize: 100 };
  const wallets = state.pagination.wallets || { page: 1, pageSize: 100 };
  const positionsPage = Math.max(1, Math.floor(n(positions.page, 1)));
  const walletsPage = Math.max(1, Math.floor(n(wallets.page, 1)));
  const positionsPageSize = Math.max(10, Math.min(5000, Math.floor(n(positions.pageSize, 100))));
  const walletsPageSize = Math.max(10, Math.min(5000, Math.floor(n(wallets.pageSize, 100))));

  return {
    public_offset: (positionsPage - 1) * positionsPageSize,
    public_limit: positionsPageSize,
    position_offset: (positionsPage - 1) * positionsPageSize,
    position_limit: positionsPageSize,
    position_sort: state.positionsSortKey,
    position_dir: state.positionsSortDir,
    wallet_offset: (walletsPage - 1) * walletsPageSize,
    wallet_limit: walletsPageSize,
    wallet_sort: state.walletsSortKey,
    wallet_dir: state.walletsSortDir,
  };
}

function buildLiveTradesPath(basePath) {
  const params = new URLSearchParams();
  const window = currentWindowParams();
  for (const [key, value] of Object.entries(window)) {
    params.set(key, String(value));
  }
  // Wallet-first live UI does not require full exchange publicTrades hydration on each poll.
  params.set("skip_public", "1");
  params.set("live_active_offset", "0");
  params.set("live_active_limit", "12");
  return `${basePath}?${params.toString()}`;
}

function pageWindowFromSummary(key, rowsLength) {
  const config = state.pagination[key] || { page: 1, pageSize: 100 };
  const pageSize = Math.max(10, Math.min(5000, Math.floor(n(config.pageSize, 100))));
  const payload = state.payload || {};
  const summary = payload.summary || {};
  const totalRows =
    key === "positions"
      ? Number.isFinite(n(summary.openPositionsTotal, NaN))
        ? n(summary.openPositionsTotal, rowsLength)
        : Number.isFinite(n(summary.publicTradesTotal, NaN))
        ? n(summary.publicTradesTotal, rowsLength)
        : rowsLength
      : Number.isFinite(n(summary.indexedWalletsTotal, NaN))
      ? n(summary.indexedWalletsTotal, rowsLength)
      : rowsLength;
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize));
  const page = Math.min(Math.max(1, Math.floor(n(config.page, 1))), totalPages);
  config.page = page;
  config.pageSize = pageSize;
  state.pagination[key] = config;

  return {
    totalRows,
    totalPages,
    page,
    pageSize,
    start: totalRows > 0 ? (page - 1) * pageSize + 1 : 0,
    end: totalRows > 0 ? Math.min(page * pageSize, totalRows) : 0,
  };
}

function paginateRows(rows, key) {
  const list = Array.isArray(rows) ? rows : [];
  const config = state.pagination[key] || { page: 1, pageSize: 100 };
  const pageSize = Math.max(10, Math.min(5000, Math.floor(n(config.pageSize, 100))));
  const totalRows = list.length;
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize));
  const page = Math.min(Math.max(1, Math.floor(n(config.page, 1))), totalPages);
  const startIndex = totalRows > 0 ? (page - 1) * pageSize : 0;
  const endIndex = Math.min(startIndex + pageSize, totalRows);
  const pageRows = totalRows > 0 ? list.slice(startIndex, endIndex) : [];

  config.pageSize = pageSize;
  config.page = page;
  state.pagination[key] = config;

  return {
    rows: pageRows,
    totalRows,
    totalPages,
    page,
    pageSize,
    start: totalRows > 0 ? startIndex + 1 : 0,
    end: endIndex,
  };
}

function renderPagination(key, pageData) {
  const info = el(`lt-${key}-page-info`);
  const windowNode = el(`lt-${key}-page-window`);
  const prev = el(`lt-${key}-page-prev`);
  const next = el(`lt-${key}-page-next`);
  if (!info || !windowNode || !prev || !next) return;

  info.textContent = `Page ${fmtInt(pageData.page)} / ${fmtInt(pageData.totalPages)}`;
  windowNode.textContent = `${fmtInt(pageData.start)}-${fmtInt(pageData.end)} of ${fmtInt(pageData.totalRows)}`;
  prev.disabled = pageData.page <= 1;
  next.disabled = pageData.page >= pageData.totalPages;
}

function applyPositionFilters(rows) {
  const q = state.search.trim().toLowerCase();
  const f = state.filters;
  const walletNeedle = String(f.wallet || "").trim().toLowerCase();
  return rows.filter((row) => {
    if (!matchesSearch(row, q)) return false;
    if (walletNeedle) {
      const walletFields = [row.wallet, row.walletLabel, row.walletTitle].join(" ").toLowerCase();
      if (!walletFields.includes(walletNeedle)) return false;
    }
    if (f.symbol && row.symbol !== f.symbol) return false;
    if (f.side && row.side !== f.side) return false;
    if (f.freshness && String(row.freshness || row.status || "") !== f.freshness) return false;
    if (!withinRange(row.positionUsd, f.minUsd, f.maxUsd)) return false;
    if (!withinRange(row.pnl, f.minPnl, f.maxPnl)) return false;
    if (f.trackedOnly && !row.trackedWallet) return false;
    return true;
  });
}

function applyWalletFilters(rows) {
  const q = state.search.trim().toLowerCase();
  const f = state.filters;
  const walletNeedle = String(f.wallet || "").trim().toLowerCase();
  return rows.filter((row) => {
    if (!matchesSearch(row, q)) return false;
    if (walletNeedle) {
      const walletFields = [row.wallet, row.walletLabel, row.walletTitle].join(" ").toLowerCase();
      if (!walletFields.includes(walletNeedle)) return false;
    }
    if (f.freshness && String(row.freshness || row.status || "") !== f.freshness) return false;
    if (!withinRange(row.totalVolumeUsd, f.minUsd, f.maxUsd)) return false;
    if (!withinRange(row.totalPnlUsd, f.minPnl, f.maxPnl)) return false;
    if (f.trackedOnly && !row.trackedWallet) return false;
    return true;
  });
}

function badge(type, value) {
  return `<span class="lt-badge ${escapeHtml(type)}">${escapeHtml(String(value || "-"))}</span>`;
}

function renderActivityRail(level, tone) {
  const active = clamp(level, 0, 5);
  return `<span class="lt-activity-rail">${Array.from({ length: 5 }, (_, idx) => {
    const on = idx < active;
    return `<span class="${on ? `is-on ${escapeHtml(tone || "cooling")}` : ""}"></span>`;
  }).join("")}</span>`;
}

function renderScorePill(label, score, options = {}) {
  const inverse = Boolean(options.inverse);
  const tone = scoreTone(score, inverse);
  const numeric = n(score, NaN);
  const text = Number.isFinite(numeric) ? `${label} ${fmtInt(numeric)}` : `${label} -`;
  return `<span class="lt-score-pill ${escapeHtml(tone)}">${escapeHtml(text)}</span>`;
}

function renderMiniBars(values) {
  const numbers = safeArray(values).map((value) => n(value, 0));
  const maxAbs = Math.max(1, ...numbers.map((value) => Math.abs(value)));
  return `<span class="lt-mini-bars">${numbers
    .map((value) => {
      const height = Math.max(8, Math.round((Math.abs(value) / maxAbs) * 28));
      const tone = value >= 0 ? "up" : "down";
      return `<span class="${tone}" style="height:${height}px"></span>`;
    })
    .join("")}</span>`;
}

function renderWalletCell(row, options = {}) {
  const walletText = row.walletLabel || shortWallet(row.wallet) || "Unattributed Flow";
  const showTrackedBadge = options.showTrackedBadge !== false;
  const showFreshnessBadge = options.showFreshnessBadge !== false;
  const showCopyButton = Boolean(options.showCopyButton);
  const statusClass = row.walletResolved
    ? row.freshness === "fresh"
      ? "fresh"
      : row.freshness === "stale"
      ? "stale"
      : "cool"
    : "warn";
  const statusLabel = row.walletResolved ? row.freshness || "unknown" : "unresolved";
  const tracked = showTrackedBadge && row.trackedWallet ? badge("tracked", "tracked") : "";
  const freshness = showFreshnessBadge ? badge(statusClass, statusLabel) : "";
  const walletFilterValue = row.walletResolved ? row.wallet : "";
  const walletButtonClass = row.walletResolved ? "lt-wallet-link" : "lt-wallet-link lt-wallet-link-disabled";
  const copyButton =
    showCopyButton && row.walletResolved
      ? `<button class="lt-inline-copy" type="button" data-copy-wallet="${escapeHtml(row.wallet)}" title="Copy wallet">Copy</button>`
      : "";
  const metaBits = [tracked, freshness].filter(Boolean).join("");
  return `<div class="lt-wallet-cell">
    <div class="lt-wallet-main">
      <button class="${walletButtonClass}" data-filter-wallet="${escapeHtml(walletFilterValue)}" title="${escapeHtml(
        row.walletTitle || row.wallet || walletText
      )}" ${row.walletResolved ? "" : "disabled"}>${escapeHtml(walletText)}</button>
      ${copyButton}
    </div>
    ${metaBits ? `<div class="lt-wallet-meta">${metaBits}</div>` : ""}
  </div>`;
}

function attachWalletCopyButtons(root) {
  if (!root) return;
  root.querySelectorAll("[data-copy-wallet]").forEach((button) => {
    button.addEventListener("click", async (event) => {
      event.preventDefault();
      event.stopPropagation();
      const walletValue = String(button.getAttribute("data-copy-wallet") || "").trim();
      if (!walletValue) return;
      const original = button.textContent || "Copy";
      try {
        const copied = await copyTextToClipboard(walletValue);
        button.textContent = copied ? "Copied" : "Copy failed";
      } catch (_error) {
        button.textContent = "Copy failed";
      }
      window.setTimeout(() => {
        button.textContent = original;
      }, 1400);
    });
  });
}

function scrollDetailPanelIntoView() {
  const panel = el("lt-detail-panel");
  if (!panel || panel.hidden) return;
  panel.scrollIntoView({ behavior: "smooth", block: "start" });
  window.requestAnimationFrame(() => {
    if (panel instanceof HTMLElement) panel.focus({ preventScroll: true });
  });
}

async function fetchJsonWithTimeout(url, timeoutMs = SNAPSHOT_TIMEOUT_MS, externalSignal = null) {
  const controller = typeof AbortController === "function" ? new AbortController() : null;
  const abortFromExternal = () => {
    if (controller) controller.abort();
  };
  if (externalSignal && controller) {
    if (externalSignal.aborted) controller.abort();
    else externalSignal.addEventListener("abort", abortFromExternal, { once: true });
  }
  const timeoutId = controller
    ? setTimeout(() => {
        try {
          controller.abort();
        } catch (_error) {}
      }, timeoutMs)
    : null;
  try {
    const res = await fetch(url, {
      cache: "no-store",
      headers: { Accept: "application/json" },
      signal: controller ? controller.signal : undefined,
    });
    if (!res.ok) {
      let message = `HTTP ${res.status}`;
      try {
        const body = await res.json();
        if (body && body.error) message = String(body.error);
      } catch (_error) {}
      throw new Error(message);
    }
    return await res.json();
  } finally {
    if (timeoutId) clearTimeout(timeoutId);
    if (externalSignal && controller) {
      externalSignal.removeEventListener("abort", abortFromExternal);
    }
  }
}

function renderPositionsTable(rows) {
  const head = el("lt-positions-head");
  const body = el("lt-positions-body");
  if (!head || !body) return;

  const columns = [
    { key: "wallet", label: "Wallet", align: "left", sortKey: "wallet" },
    { key: "symbol", label: "Symbol", align: "left", sortKey: "symbol" },
    { key: "side", label: "Side", align: "left", sortKey: "side" },
    { key: "size", label: "Size", align: "right", sortKey: "size" },
    { key: "position", label: "Position USD", align: "right", sortKey: "positionUsd" },
    { key: "entry", label: "Entry", align: "right", sortKey: "entry" },
    { key: "mark", label: "Mark", align: "right", sortKey: "mark" },
    { key: "unrealized", label: "Unrealized", align: "right", sortKey: "unrealized" },
    { key: "activity", label: "Opened", align: "left", sortKey: "openedAt" },
  ];

  head.innerHTML = `<tr>${columns
    .map(
      (col) =>
        `<th class="${col.align === "right" ? "right" : "left"}" data-col="${escapeHtml(col.key)}" aria-sort="${
          state.positionsSortKey === col.sortKey ? (state.positionsSortDir === "asc" ? "ascending" : "descending") : "none"
        }"><button class="lt-table-sort-btn${
          state.positionsSortKey === col.sortKey ? " active" : ""
        }" type="button" data-position-sort-key="${escapeHtml(col.sortKey)}" data-position-sort-label="${escapeHtml(
          col.label
        )}">${escapeHtml(col.label)} <span data-position-sort-indicator>${
          state.positionsSortKey === col.sortKey ? (state.positionsSortDir === "asc" ? "↑" : "↓") : "↑↓"
        }</span></button></th>`
    )
    .join("")}</tr>`;

  head.querySelectorAll("[data-position-sort-key]").forEach((button) => {
    button.addEventListener("click", () => {
      const nextKey = String(button.getAttribute("data-position-sort-key") || "").trim();
      if (!nextKey) return;
      if (state.positionsSortKey === nextKey) {
        state.positionsSortDir = state.positionsSortDir === "asc" ? "desc" : "asc";
      } else {
        state.positionsSortKey = nextKey;
        state.positionsSortDir = "desc";
      }
      state.pagination.positions.page = 1;
      fetchSnapshot()
        .then((payload) => applyPayload(payload, "sort"))
        .catch(() => {});
      connectSse();
    });
  });

  if (!rows.length) {
    body.innerHTML =
      '<tr><td colspan="9" class="lt-empty-state">No open positions match the current filters.</td></tr>';
    return;
  }

  body.innerHTML = rows
    .map((row) => {
      const isSelected = state.selection.type === "positions" && state.selection.key === row.key;
      const side = normalizeSide(row.side);
      const sideClass =
        side.includes("long") || side === "buy" ? "up" : side.includes("short") || side === "sell" ? "down" : "flat";
      const sizeText = Number.isFinite(n(row.size, NaN)) ? fmtNum(row.size, 4) : "-";
      const exposureText = Number.isFinite(n(row.positionUsd, NaN)) ? fmtUsd(row.positionUsd, 2) : "Partial";
      const entryText = Number.isFinite(n(row.entry, NaN)) ? fmtNum(row.entry, 4) : "Partial";
      const markText = Number.isFinite(n(row.mark, NaN)) ? fmtNum(row.mark, 4) : "Partial";
      const pnlText = Number.isFinite(n(row.pnl, NaN)) ? fmtSignedUsd(row.pnl, 2) : "Partial";
      const ageText = row.openedAt ? fmtAgo(row.openedAt) : "n/a";
      const updateText = row.updatedAt ? fmtAgo(row.updatedAt) : "-";
      const activityTone =
        row.freshness === "fresh" ? "fresh" : row.freshness === "stale" ? "stale" : "cooling";
      return `<tr class="${isSelected ? "is-selected" : ""}" data-select-type="positions" data-select-key="${escapeHtml(
        row.key
      )}" tabindex="0" aria-selected="${isSelected ? "true" : "false"}">
        <td class="left" data-col="wallet">
          ${renderWalletCell(row, { showTrackedBadge: false, showFreshnessBadge: false, showCopyButton: true })}
        </td>
        <td class="left" data-col="symbol">
          <strong>${escapeHtml(row.symbol || "-")}</strong>
        </td>
        <td class="left" data-col="side">
          ${badge(sideClass, titleCase(row.side || "-"))}
        </td>
        <td class="right mono" data-col="size">${escapeHtml(sizeText)}</td>
        <td class="right mono" data-col="position">${escapeHtml(exposureText)}</td>
        <td class="right mono" data-col="entry">${escapeHtml(entryText)}</td>
        <td class="right mono" data-col="mark">${escapeHtml(markText)}</td>
        <td class="right mono ${n(row.pnl, 0) >= 0 ? "up" : "down"}" data-col="unrealized">${escapeHtml(pnlText)}</td>
        <td class="left" data-col="activity">
          <div class="lt-activity-cell">
            <div class="lt-activity-meta">
              ${badge(activityTone, row.freshness || "unknown")}
              <span>opened ${escapeHtml(ageText)}</span>
            </div>
            <div class="lt-activity-meta">
              ${renderActivityRail(row.activityLevel, activityTone)}
              <span>updated ${escapeHtml(updateText)}</span>
            </div>
          </div>
        </td>
      </tr>`;
    })
    .join("");

  body.querySelectorAll("button.lt-wallet-link[data-filter-wallet]").forEach((btn) => {
    btn.addEventListener("click", (event) => {
      event.stopPropagation();
      const walletValue = String(btn.getAttribute("data-filter-wallet") || "").trim();
      if (!walletValue) return;
      const walletInput = el("lt-filter-wallet");
      if (walletInput) walletInput.value = walletValue;
      state.filters.wallet = walletValue;
      resetPagination();
      render();
    });
  });
  attachWalletCopyButtons(body);

  body.querySelectorAll("tr[data-select-type='positions'][data-select-key]").forEach((rowNode) => {
    const selectRow = (event) => {
      const button = event.target.closest("button.lt-wallet-link");
      if (button) return;
      const row = rows.find(
        (entry) => entry.key === String(rowNode.getAttribute("data-select-key") || "")
      );
      state.selection = {
        type: "positions",
        key: String(rowNode.getAttribute("data-select-key") || ""),
      };
      if (row && row.walletResolved) {
        openWalletDrawerForRow(row);
      } else {
        render();
      }
    };
    rowNode.addEventListener("click", selectRow);
    rowNode.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" && event.key !== " ") return;
      event.preventDefault();
      selectRow(event);
    });
  });
}

function renderWalletsTable(rows) {
  const head = el("lt-wallets-head");
  const body = el("lt-wallets-body");
  if (!head || !body) return;

  const columns = [
    { key: "wallet", label: "Wallet", align: "left" },
    { key: "pnl7", label: "Realized 7D", align: "right", sortKey: "pnl7" },
    { key: "pnl30", label: "Realized 30D", align: "right", sortKey: "pnl30" },
    { key: "pnlAll", label: "Realized All", align: "right", sortKey: "pnlAll" },
    { key: "open", label: "Open Positions", align: "right" },
    { key: "activity", label: "Last Activity", align: "left" },
  ];

  head.innerHTML = `<tr>${columns
    .map(
      (col) =>
        `<th class="${col.align === "right" ? "right" : "left"}" data-col="${escapeHtml(col.key)}" aria-sort="${
          col.sortKey && state.walletsSortKey === col.sortKey ? (state.walletsSortDir === "asc" ? "ascending" : "descending") : "none"
        }">${
          col.sortKey
            ? `<button class="lt-table-sort-btn${
                state.walletsSortKey === col.sortKey ? " active" : ""
              }" type="button" data-wallet-sort-key="${escapeHtml(col.sortKey)}" data-wallet-sort-label="${escapeHtml(
                col.label
              )}">${escapeHtml(col.label)} <span data-wallet-sort-indicator>${
                state.walletsSortKey === col.sortKey ? (state.walletsSortDir === "asc" ? "↑" : "↓") : "↑↓"
              }</span></button>`
            : escapeHtml(col.label)
        }</th>`
    )
    .join("")}</tr>`;

  head.querySelectorAll("[data-wallet-sort-key]").forEach((button) => {
    button.addEventListener("click", () => {
      const nextKey = String(button.getAttribute("data-wallet-sort-key") || "").trim();
      if (!nextKey) return;
      if (state.walletsSortKey === nextKey) {
        state.walletsSortDir = state.walletsSortDir === "asc" ? "desc" : "asc";
      } else {
        state.walletsSortKey = nextKey;
        state.walletsSortDir = "desc";
      }
      state.pagination.wallets.page = 1;
      fetchSnapshot()
        .then((payload) => applyPayload(payload, "sort"))
        .catch(() => {});
      connectSse();
    });
  });

  if (!rows.length) {
    body.innerHTML =
      '<tr><td colspan="6" class="lt-empty-state">No wallets match the current filters.</td></tr>';
    return;
  }

  body.innerHTML = rows
    .map((row) => {
      const isSelected = state.selection.type === "wallets" && state.selection.key === row.key;
      const freshClass =
        row.freshness === "fresh"
          ? "fresh"
          : row.freshness === "stale"
          ? "stale"
          : row.freshness === "unresolved"
          ? "warn"
          : "cool";
      const liveRankText = row.liveActiveRank ? `#${fmtInt(row.liveActiveRank)} live` : "tracked wallet";
      const realized7 = Number.isFinite(n(row.pnl7Usd, NaN)) ? fmtSignedUsd(row.pnl7Usd, 2) : "n/a";
      const realized30 = Number.isFinite(n(row.pnl30Usd, NaN)) ? fmtSignedUsd(row.pnl30Usd, 2) : "n/a";
      const realizedAll = Number.isFinite(n(row.realizedPnlUsd, NaN))
        ? fmtSignedUsd(row.realizedPnlUsd, 2)
        : "n/a";
      const lastActivity = row.lastActivity ? fmtAgo(row.lastActivity) : "-";
      return `<tr class="${isSelected ? "is-selected" : ""}" data-select-type="wallets" data-select-key="${escapeHtml(
        row.key
      )}" tabindex="0" aria-selected="${isSelected ? "true" : "false"}">
        <td class="left" data-col="wallet">
          <div class="lt-wallet-cell">
            ${renderWalletCell(row)}
            <div class="lt-wallet-meta">
              <span>${escapeHtml(liveRankText)}</span>
              <span>${escapeHtml(fmtInt(row.openPositions || 0))} open</span>
            </div>
          </div>
        </td>
        <td class="right mono ${n(row.pnl7Usd, 0) >= 0 ? "up" : "down"}" data-col="pnl7">${escapeHtml(realized7)}</td>
        <td class="right mono ${n(row.pnl30Usd, 0) >= 0 ? "up" : "down"}" data-col="pnl30">${escapeHtml(realized30)}</td>
        <td class="right mono ${n(row.realizedPnlUsd, 0) >= 0 ? "up" : "down"}" data-col="pnlAll">${escapeHtml(realizedAll)}</td>
        <td class="right mono" data-col="open">${escapeHtml(fmtInt(row.openPositions || 0))}</td>
        <td class="left" data-col="activity">
          <div class="lt-activity-cell">
            <div class="lt-activity-meta">
              ${badge(freshClass, row.freshness || "unknown")}
              <span>${escapeHtml(lastActivity)}</span>
            </div>
            <div class="lt-metric-subrow">
              ${row.liveActiveRank ? `<span>#${escapeHtml(fmtInt(row.liveActiveRank))} live</span>` : "<span>tracked</span>"}
              <span>${escapeHtml(fmtInt(row.recentEvents15m || 0))} ev / 15m</span>
            </div>
          </div>
        </td>
      </tr>`;
    })
    .join("");

  body.querySelectorAll("button.lt-wallet-link[data-filter-wallet]").forEach((btn) => {
    btn.addEventListener("click", (event) => {
      event.stopPropagation();
      const walletValue = String(btn.getAttribute("data-filter-wallet") || "").trim();
      if (!walletValue) return;
      const walletInput = el("lt-filter-wallet");
      if (walletInput) walletInput.value = walletValue;
      state.filters.wallet = walletValue;
      resetPagination();
      render();
    });
  });
  attachWalletCopyButtons(body);

  body.querySelectorAll("tr[data-select-type='wallets'][data-select-key]").forEach((rowNode) => {
    const selectRow = (event) => {
      const button = event.target.closest("button.lt-wallet-link");
      if (button) return;
      const row = rows.find(
        (entry) => entry.key === String(rowNode.getAttribute("data-select-key") || "")
      );
      state.selection = {
        type: "wallets",
        key: String(rowNode.getAttribute("data-select-key") || ""),
      };
      if (row && row.walletResolved) {
        openWalletDrawerForRow(row);
      } else {
        render();
      }
    };
    rowNode.addEventListener("click", selectRow);
    rowNode.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" && event.key !== " ") return;
      event.preventDefault();
      selectRow(event);
    });
  });
}

function sortWalletRows(rows) {
  const list = safeArray(rows).slice();
  const key = String(state.walletsSortKey || "pnlAll").trim();
  const dir = state.walletsSortDir === "asc" ? "asc" : "desc";
  const numericValue = (row, sortKey) => {
    if (sortKey === "pnl7") return n(row?.pnl7Usd, NaN);
    if (sortKey === "pnl30") return n(row?.pnl30Usd, NaN);
    if (sortKey === "pnlAll") return n(row?.realizedPnlUsd, NaN);
    return NaN;
  };
  list.sort((left, right) => {
    const leftVal = numericValue(left, key);
    const rightVal = numericValue(right, key);
    const leftMissing = !Number.isFinite(leftVal);
    const rightMissing = !Number.isFinite(rightVal);
    if (leftMissing && rightMissing) {
      return tsMs(right?.lastActivity) - tsMs(left?.lastActivity);
    }
    if (leftMissing) return 1;
    if (rightMissing) return -1;
    if (leftVal === rightVal) {
      return tsMs(right?.lastActivity) - tsMs(left?.lastActivity);
    }
    return dir === "asc" ? leftVal - rightVal : rightVal - leftVal;
  });
  return list;
}

function renderActiveView() {
  const activeView = state.activeView === "wallets" ? "wallets" : "positions";
  const inactiveView = activeView === "positions" ? "wallets" : "positions";
  const activePanel = el(`lt-view-${activeView}`);
  const inactivePanel = el(`lt-view-${inactiveView}`);
  const activeButton = el(`lt-view-${activeView}-btn`);
  const inactiveButton = el(`lt-view-${inactiveView}-btn`);
  const config = VIEW_CONFIG[activeView] || VIEW_CONFIG.positions;

  if (activePanel) {
    activePanel.hidden = false;
    activePanel.classList.add("is-active");
  }
  if (inactivePanel) {
    inactivePanel.hidden = true;
    inactivePanel.classList.remove("is-active");
  }

  if (activeButton) {
    activeButton.classList.add("active");
    activeButton.setAttribute("aria-selected", "true");
  }
  if (inactiveButton) {
    inactiveButton.classList.remove("active");
    inactiveButton.setAttribute("aria-selected", "false");
  }

  const kicker = el("lt-stage-kicker");
  if (kicker) kicker.textContent = config.kicker;
  const title = el("lt-stage-title");
  if (title) title.textContent = config.title;

  const minUsdLabel = el("lt-filter-min-usd-label");
  const maxUsdLabel = el("lt-filter-max-usd-label");
  const minPnlLabel = el("lt-filter-min-pnl-label");
  const maxPnlLabel = el("lt-filter-max-pnl-label");
  if (minUsdLabel) {
    minUsdLabel.textContent = activeView === "positions" ? "Min Position USD" : "Min Total Volume USD";
  }
  if (maxUsdLabel) {
    maxUsdLabel.textContent = activeView === "positions" ? "Max Position USD" : "Max Total Volume USD";
  }
  if (minPnlLabel) {
    minPnlLabel.textContent = activeView === "positions" ? "Min Unrealized PnL" : "Min Total PnL";
  }
  if (maxPnlLabel) {
    maxPnlLabel.textContent = activeView === "positions" ? "Max Unrealized PnL" : "Max Total PnL";
  }

  document.querySelectorAll("[data-filter-scope='positions']").forEach((node) => {
    node.hidden = activeView !== "positions";
  });
}

function closeWalletDrawer() {
  if (state.drawer.abortController) {
    try {
      state.drawer.abortController.abort();
    } catch (_error) {}
  }
  state.drawer = {
    wallet: "",
    loading: false,
    error: "",
    detail: null,
    sourceRow: null,
    requestId: state.drawer.requestId + 1,
    abortController: null,
  };
}

async function openWalletDrawerForRow(row) {
  const wallet = normalizeWallet(row && row.wallet);
  if (!wallet) return;
  if (state.drawer.abortController) {
    try {
      state.drawer.abortController.abort();
    } catch (_error) {}
  }
  const abortController = typeof AbortController === "function" ? new AbortController() : null;
  const requestId = state.drawer.requestId + 1;
  state.drawer = {
    wallet,
    loading: true,
    error: "",
    detail: null,
    sourceRow: row || null,
    requestId,
    abortController,
  };
  renderDetailPanel();
  scrollDetailPanelIntoView();
  try {
    const payload = await fetchJsonWithTimeout(
      `${WALLET_DETAIL_ENDPOINT_PREFIX}${encodeURIComponent(wallet)}`,
      DRAWER_FETCH_TIMEOUT_MS,
      abortController ? abortController.signal : null
    );
    if (requestId !== state.drawer.requestId) return;
    state.drawer = {
      wallet,
      loading: false,
      error: "",
      detail: payload,
      sourceRow: row || null,
      requestId,
      abortController: null,
    };
  } catch (error) {
    if (abortController && abortController.signal.aborted) return;
    if (requestId !== state.drawer.requestId) return;
    state.drawer = {
      wallet,
      loading: false,
      error: error && error.message ? error.message : "Failed to load wallet detail.",
      detail: null,
      sourceRow: row || null,
      requestId,
      abortController: null,
    };
  }
  renderDetailPanel();
  scrollDetailPanelIntoView();
}

function renderWalletPositionsList(positions, sourceRow) {
  const rows = safeArray(positions);
  if (!rows.length) {
    return '<div class="lt-drawer-empty">No open positions currently tracked for this wallet.</div>';
  }
  return `<div class="lt-drawer-positions-wrap">
    <table class="lt-drawer-positions-table">
      <thead>
        <tr>
          <th>Symbol</th>
          <th>Side</th>
          <th class="right">Position USD</th>
          <th class="right">Entry</th>
          <th class="right">Mark</th>
          <th class="right">Unrealized</th>
          <th>Updated</th>
        </tr>
      </thead>
      <tbody>
        ${rows
          .map((row) => {
            const selected =
              sourceRow &&
              sourceRow.tradeRef &&
              row.id &&
              String(sourceRow.tradeRef) === String(row.id);
            const side = normalizeSide(row.side);
            const sideClass =
              side.includes("long") || side === "buy"
                ? "up"
                : side.includes("short") || side === "sell"
                ? "down"
                : "flat";
            return `<tr${selected ? ' class="is-focus"' : ""}>
              <td><strong>${escapeHtml(row.symbol || "-")}</strong></td>
              <td>${badge(sideClass, titleCase(row.side || "-"))}</td>
              <td class="right mono">${escapeHtml(fmtUsd(row.positionUsd, 2))}</td>
              <td class="right mono">${escapeHtml(Number.isFinite(n(row.entry, NaN)) ? fmtNum(row.entry, 4) : "-")}</td>
              <td class="right mono">${escapeHtml(Number.isFinite(n(row.mark, NaN)) ? fmtNum(row.mark, 4) : "-")}</td>
              <td class="right mono ${n(row.unrealizedPnlUsd, 0) >= 0 ? "up" : "down"}">${escapeHtml(fmtSignedUsd(row.unrealizedPnlUsd, 2))}</td>
              <td>${escapeHtml(row.updatedAt ? fmtAgo(row.updatedAt) : "-")}</td>
            </tr>`;
          })
          .join("")}
      </tbody>
    </table>
  </div>`;
}

function renderAllocationBars(entries) {
  const rows = safeArray(entries);
  if (!rows.length) {
    return '<div class="lt-drawer-empty">No concentration data available.</div>';
  }
  return `<div class="lt-allocation-list">
    ${rows
      .map((entry) => {
        const pct = clamp(entry.sharePct, 0, 100);
        return `<div class="lt-allocation-row">
          <div class="lt-allocation-label">
            <span class="lt-allocation-symbol">${escapeHtml(entry.symbol || "-")}</span>
            <span class="lt-allocation-values">
              <strong>${escapeHtml(fmtUsd(entry.volumeUsd, 2))}</strong>
              <span>${escapeHtml(fmtPct(pct, 1))}</span>
            </span>
          </div>
          <div class="lt-allocation-bar"><span style="width:${pct.toFixed(2)}%"></span></div>
        </div>`;
      })
      .join("")}
  </div>`;
}

function renderDetailPanel() {
  const panel = el("lt-detail-panel");
  const detailTitle = el("lt-detail-title");
  const detailSubtitle = el("lt-detail-subtitle");
  const detailKicker = el("lt-detail-kicker");
  const detailBody = el("lt-detail-body");
  if (!panel || !detailTitle || !detailSubtitle || !detailKicker || !detailBody) return;

  if (!state.drawer.wallet) {
    panel.hidden = true;
    detailBody.innerHTML = "";
    return;
  }

  panel.hidden = false;
  detailKicker.textContent = "Wallet detail";

  if (state.drawer.loading) {
    detailTitle.textContent =
      state.drawer.sourceRow?.walletTitle ||
      state.drawer.sourceRow?.walletLabel ||
      shortWallet(state.drawer.wallet);
    detailSubtitle.textContent = "Loading wallet summary and open positions.";
    detailBody.innerHTML = '<div class="lt-drawer-empty">Loading wallet details...</div>';
    return;
  }

  if (state.drawer.error) {
    detailTitle.textContent =
      state.drawer.sourceRow?.walletTitle ||
      state.drawer.sourceRow?.walletLabel ||
      shortWallet(state.drawer.wallet);
    detailSubtitle.textContent = "Wallet detail unavailable";
    detailBody.innerHTML = `<div class="lt-drawer-empty">${escapeHtml(state.drawer.error)}</div>`;
    return;
  }

  const detail = asObject(state.drawer.detail);
  const summary = asObject(detail.summary);
  const positions = safeArray(detail.positions);
  const sourceRow = state.drawer.sourceRow || null;
  detailTitle.textContent =
    sourceRow?.walletTitle || sourceRow?.walletLabel || shortWallet(detail.wallet || state.drawer.wallet);
  detailSubtitle.textContent = `${titleCase(summary.freshness || "unknown")} • ${
    summary.liveActiveRank ? `#${fmtInt(summary.liveActiveRank)} live` : "tracked wallet"
  }`;

  const all = asObject(summary.all);
  const d7 = asObject(summary.d7);
  const d30 = asObject(summary.d30);
  const d24 = asObject(summary.d24);
  const lifecycle = asObject(summary.lifecycle);
  const behaviorTags = deriveWalletBehaviorTags({
    recentEvents15m: summary.recentEvents15m,
    recentEvents1h: summary.recentEvents1h,
    openPositions: summary.openPositions,
    concentrationPct: safeArray(summary.concentration)[0]?.sharePct || 0,
    winRate: all.winRatePct,
    pnl7Usd: d7.pnlUsd,
    totalPnlUsd: summary.totalPnlUsd,
    freshness: summary.freshness,
    increaseCount: lifecycle.increaseCount,
    reduceCount: lifecycle.reduceCount,
    avgHoldingMs:
      lifecycle.avgObservedHoldingMs ||
      lifecycle.avgCompletedHoldingMs ||
      lifecycle.currentAvgOpenAgeMs,
    totalTrades: all.trades,
  });

  const walletAddress = detail.wallet || state.drawer.wallet;
  const shortAddress = shortWallet(walletAddress, 8, 6);
  const summaryParts = [];
  if (Number.isFinite(n(all.winRatePct, NaN))) {
    summaryParts.push(`Win Rate ${fmtPct(all.winRatePct, 2)}`);
  }
  if (Number.isFinite(n(all.trades, NaN))) {
    summaryParts.push(`${fmtInt(all.trades)} trades`);
  }
  if (Number.isFinite(n(summary.exposureUsd, NaN))) {
    summaryParts.push(`${fmtUsd(summary.exposureUsd, 2)} exposure`);
  }

  detailBody.innerHTML = `
    <div class="lt-detail-header">
      <div class="lt-detail-identity">
        <span class="lt-detail-label">Wallet</span>
        <span class="lt-detail-address">${escapeHtml(shortAddress)}</span>
        <div class="lt-detail-actions">
          <button type="button" class="lt-inline-copy" data-copy-wallet="${escapeHtml(walletAddress)}">Copy</button>
        </div>
      </div>
      <div class="lt-detail-badges">
        ${badge(summary.freshness === "fresh" ? "fresh" : summary.freshness === "stale" ? "stale" : "cool", titleCase(summary.freshness || "unknown"))}
        ${badge("rank", `${fmtInt(summary.openPositions || positions.length)} open`)}
      </div>
    </div>
    ${summaryParts.length ? `<div class="lt-detail-summary">${escapeHtml(summaryParts.join(" • "))}</div>` : ""}
    <div class="lt-detail-kpi-grid">
      <div class="lt-detail-kpi-card">
        <span>24H Realized</span>
        <strong class="${n(d24.pnlUsd, 0) >= 0 ? "up" : "down"}">${escapeHtml(fmtSignedUsd(d24.pnlUsd, 2))}</strong>
      </div>
      <div class="lt-detail-kpi-card">
        <span>7D Realized</span>
        <strong class="${n(d7.pnlUsd, 0) >= 0 ? "up" : "down"}">${escapeHtml(fmtSignedUsd(d7.pnlUsd, 2))}</strong>
      </div>
      <div class="lt-detail-kpi-card">
        <span>30D Realized</span>
        <strong class="${n(d30.pnlUsd, 0) >= 0 ? "up" : "down"}">${escapeHtml(fmtSignedUsd(d30.pnlUsd, 2))}</strong>
      </div>
      <div class="lt-detail-kpi-card">
        <span>Total PnL</span>
        <strong class="${n(summary.totalPnlUsd, 0) >= 0 ? "up" : "down"}">${escapeHtml(fmtSignedUsd(summary.totalPnlUsd || 0, 2))}</strong>
      </div>
    </div>
    <section class="lt-detail-section-block">
      <h4>Wallet posture</h4>
      <div class="lt-detail-section-grid">
        <div class="lt-detail-item">
          <div class="lt-detail-item-label">Open positions</div>
          <div class="lt-detail-item-value">${escapeHtml(fmtInt(summary.openPositions || positions.length))}</div>
        </div>
        <div class="lt-detail-item">
          <div class="lt-detail-item-label">Exposure</div>
          <div class="lt-detail-item-value">${escapeHtml(fmtUsd(summary.exposureUsd || 0, 2))}</div>
        </div>
        <div class="lt-detail-item">
          <div class="lt-detail-item-label">Wallet unrealized</div>
          <div class="lt-detail-item-value ${n(summary.unrealizedPnlUsd, 0) >= 0 ? "up" : "down"}">${escapeHtml(
            fmtSignedUsd(summary.unrealizedPnlUsd || 0, 2)
          )}</div>
        </div>
        <div class="lt-detail-item">
          <div class="lt-detail-item-label">Last activity</div>
          <div class="lt-detail-item-value">${escapeHtml(summary.lastActivityAt ? fmtTs(summary.lastActivityAt) : "-")}</div>
        </div>
        <div class="lt-detail-item">
          <div class="lt-detail-item-label">Recent events</div>
          <div class="lt-detail-item-value">${escapeHtml(fmtInt(summary.recentEvents15m || 0))} / 15m</div>
        </div>
        <div class="lt-detail-item">
          <div class="lt-detail-item-label">Live score</div>
          <div class="lt-detail-item-value">${escapeHtml(fmtInt(summary.liveActiveScore || 0))}</div>
        </div>
      </div>
    </section>
    <div class="lt-detail-columns">
      <section class="lt-detail-section-block">
        <h4>Open positions</h4>
        ${renderWalletPositionsList(positions, sourceRow)}
      </section>
      <section class="lt-detail-section-block">
        <h4>Concentration</h4>
        ${renderAllocationBars(summary.concentration)}
      </section>
    </div>
    <section class="lt-detail-section-block">
      <h4>Behavior context</h4>
      <div class="lt-tag-group">
        ${
          behaviorTags.length
            ? behaviorTags.map((tag) => badge("cool", tag)).join(" ")
            : '<span class="lt-drawer-empty">No recent behavior signals.</span>'
        }
      </div>
    </section>
  `;
  attachWalletCopyButtons(detailBody);
}

function connectionState() {
  if (state.stream.paused) return { label: "PAUSED", className: "paused" };

  const age = state.stream.lastPayloadAt ? Date.now() - state.stream.lastPayloadAt : Infinity;
  if (state.stream.connection === "open" && age <= LIVE_DELAYED_MS) return { label: "LIVE", className: "live" };
  if (state.stream.connection === "open" && age <= LIVE_STALE_MS) return { label: "DELAYED", className: "delayed" };
  if (state.stream.connection === "reconnecting") return { label: "RECONNECTING", className: "warn" };
  if (state.stream.connection === "fallback") return { label: "DEGRADED", className: "warn" };
  if (age > LIVE_STALE_MS) return { label: "STALE", className: "bad" };
  return { label: "IDLE", className: "idle" };
}

function renderHealth(payload) {
  const status = connectionState();
  const liveBadge = el("lt-live-badge");
  if (liveBadge) {
    liveBadge.textContent = status.label;
    liveBadge.className = `lt-pill lt-pill-live ${status.className}`;
  }

  const freshness = state.stream.lastPayloadAt ? Date.now() - state.stream.lastPayloadAt : null;

  const freshNode = el("lt-stage-freshness");
  if (freshNode) {
    freshNode.textContent = freshness === null ? "Freshness -" : `Freshness ${fmtInt(freshness)} ms`;
  }
}

function renderObservability(payload) {
  const obs = asObject(payload?.summary?.observability);
  const hot = asObject(obs.hotTier);
  const reconciliation = asObject(obs.reconciliation);
  const publicCoverage = asObject(obs.publicActiveCoverage);

  const hotValue = el("lt-obs-hot-value");
  if (hotValue) {
    const active = n(hot.activeWallets, 0);
    const capacity = n(hot.capacity, 0);
    hotValue.textContent =
      capacity > 0 ? `${fmtInt(active)} / ${fmtInt(capacity)}` : `${fmtInt(active)}`;
  }
  const hotMeta = el("lt-obs-hot-meta");
  if (hotMeta) {
    hotMeta.textContent = `${fmtPct(hot.utilizationPct, 0)} util • dropped ${fmtInt(
      hot.droppedPromotions
    )}`;
  }

  const triggerValue = el("lt-obs-trigger-value");
  if (triggerValue) {
    const avg = n(hot.triggerToEventAvgMs, NaN);
    triggerValue.textContent = Number.isFinite(avg) && avg > 0 ? `${fmtInt(avg)} ms` : "-";
  }
  const triggerMeta = el("lt-obs-trigger-meta");
  if (triggerMeta) {
    const last = n(hot.triggerToEventLastMs, NaN);
    triggerMeta.textContent = Number.isFinite(last) && last > 0 ? `last ${fmtInt(last)} ms` : "awaiting trigger traffic";
  }

  const coverageValue = el("lt-obs-coverage-value");
  if (coverageValue) {
    const pct = n(publicCoverage.activeCoveragePct, NaN);
    coverageValue.textContent = Number.isFinite(pct) ? fmtPct(pct, 2) : "-";
  }
  const coverageMeta = el("lt-obs-coverage-meta");
  if (coverageMeta) {
    const tracked = n(payload?.summary?.walletFirstLive?.walletsWithOpenPositions, NaN);
    const lowerBound = n(publicCoverage.activeWalletsLowerBound, NaN);
    if (Number.isFinite(tracked) && Number.isFinite(lowerBound) && lowerBound > 0) {
      coverageMeta.textContent = `${fmtInt(tracked)} / ${fmtInt(lowerBound)} active wallets`;
    } else {
      coverageMeta.textContent = "public lower bound unavailable";
    }
  }

  const reconcileValue = el("lt-obs-reconcile-value");
  if (reconcileValue) {
    const sweep = n(reconciliation.estimatedSweepSeconds, NaN);
    reconcileValue.textContent = Number.isFinite(sweep) && sweep > 0 ? `${fmtInt(sweep)}s sweep` : "-";
  }
  const reconcileMeta = el("lt-obs-reconcile-meta");
  if (reconcileMeta) {
    const hotLoop = n(reconciliation.estimatedHotLoopSeconds, NaN);
    const lagMs = n(reconciliation.reconciliationLagMs, NaN);
    const parts = [];
    if (Number.isFinite(hotLoop) && hotLoop > 0) parts.push(`hot ${fmtInt(hotLoop)}s`);
    if (Number.isFinite(lagMs) && lagMs >= 0) parts.push(`lag ${fmtInt(lagMs)} ms`);
    reconcileMeta.textContent = parts.length ? parts.join(" • ") : "reconciliation warming";
  }
}

function renderWalletSupportNote(payload) {
  const note = el("lt-wallet-support-note");
  if (!note) return;
  const support = extractWalletPerformanceSupport(payload);
  const available = safeArray(support.available);
  const unavailable = safeArray(support.unavailable);
  if (!available.length && !unavailable.length) {
    note.hidden = true;
    note.innerHTML = "";
    return;
  }

  const availableLabels = available
    .filter((entry) =>
      [
        "recent_trade_count",
        "recent_volume",
        "realized_pnl",
        "unrealized_pnl",
        "open_positions",
        "exposure",
        "symbol_concentration",
        "live_active_score",
        "freshness_state",
        "wallet_holding_time",
        "position_change_history",
      ].includes(String(entry.key || ""))
    )
    .map((entry) => String(entry.label || "").trim())
    .filter(Boolean);
  const missingLabels = unavailable
    .slice(0, 3)
    .map((entry) => String(entry.label || "").trim())
    .filter(Boolean);

  note.hidden = false;
  note.innerHTML = `
    <div class="lt-wallet-support-copy">
      <strong>Backed by live activity and 24H / 7D / 30D / all-time wallet aggregates.</strong>
      <span>Primary surface only shows metrics with real support: ${escapeHtml(availableLabels.slice(0, 5).join(" • "))}.</span>
    </div>
    ${
      missingLabels.length
        ? `<div class="lt-wallet-support-muted">Hidden until a real dataset exists: ${escapeHtml(
            missingLabels.join(" • ")
          )}.</div>`
        : ""
    }
  `;
}

function renderWalletRankingGroups(rows) {
  const totalNode = el("lt-live-active-total");
  const list = el("lt-wallet-groups-grid");
  if (!list) return { groups: [], activeGroup: null, activeRows: rows };

  const groups = buildWalletRankingGroups(rows);
  const activeGroupKey =
    groups.some((group) => group.key === state.activeWalletGroup)
      ? state.activeWalletGroup
      : groups.some((group) => group.key === "pnl")
      ? "pnl"
      : groups[0]?.key || "";
  state.activeWalletGroup = activeGroupKey;
  const activeGroup = groups.find((group) => group.key === activeGroupKey) || null;

  if (totalNode) {
    totalNode.textContent = activeGroup
      ? `${fmtInt(activeGroup.count)} wallets`
      : `${fmtInt(rows.length)} ranked wallets`;
  }

  if (!groups.length) {
    list.innerHTML = `<div class="lt-live-active-empty">No wallet groups match the current filter set.</div>`;
    return { groups: [], activeGroup: null, activeRows: rows };
  }

  list.innerHTML = groups
    .map((group) => {
      const selected = group.key === activeGroupKey;
      return `<button class="lt-wallet-group-card${selected ? " is-active" : ""}" type="button" data-wallet-group="${escapeHtml(
        group.key
      )}" aria-pressed="${selected ? "true" : "false"}">
        <div class="lt-wallet-group-head">
          <div>
            <h4>${escapeHtml(group.title)}</h4>
            <p>${escapeHtml(group.subtitle)}</p>
          </div>
          ${badge(group.tone || "cool", `${fmtInt(group.count)} ranked`)}
        </div>
        <div class="lt-wallet-group-summary">
          <span class="lt-wallet-group-selection">${selected ? "Selected" : "Select group"}</span>
          <span class="lt-wallet-group-preview">${
            activeGroupKey === group.key && group.rankedRows[0]
              ? escapeHtml(walletGroupMetric(group.key, group.rankedRows[0]).primary)
              : escapeHtml(group.subtitle)
          }</span>
        </div>
      </button>`;
    })
    .join("");

  list.querySelectorAll("button[data-wallet-group]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const nextKey = String(btn.getAttribute("data-wallet-group") || "").trim();
      if (!nextKey || nextKey === state.activeWalletGroup) return;
      state.activeWalletGroup = nextKey;
      resetPagination();
      render();
    });
  });

  return {
    groups,
    activeGroup,
    activeRows: activeGroup ? activeGroup.rankedRows : rows,
  };
}

function payloadSignature(payload) {
  const positionsTotal = payloadPositionsTotal(payload);
  const indexedWalletsTotal = payloadWalletsTotal(payload);
  const publicTradesLen = extractPublicTrades(payload).length;
  const positionsLen = extractPositions(payload).length;
  const walletPerfLen = extractWalletPerformance(payload).length;
  return [
    positionsTotal,
    indexedWalletsTotal,
    publicTradesLen,
    positionsLen,
    walletPerfLen,
  ].join(":");
}

function render(options = {}) {
  const refreshOptions = Boolean(options.refreshOptions);
  const payload = state.payload;
  if (!payload) return;

  const rows = state.rows || { positions: [], wallets: [], liveActive: [] };
  if (refreshOptions) {
    refreshFilterOptions(rows);
  }

  const positionsFiltered = applyPositionFilters(rows.positions);
  const walletsFiltered = applyWalletFilters(rows.wallets);
  const walletGroupState = renderWalletRankingGroups(walletsFiltered);
  const walletsActive = sortWalletRows(safeArray(walletGroupState?.activeRows));
  const positionsWindow = pageWindowFromSummary("positions", positionsFiltered.length);
  const walletsWindow = pageWindowFromSummary("wallets", walletsActive.length);

  renderHealth(payload);
  renderObservability(payload);
  renderActiveView();
  renderWalletSupportNote(payload);
  renderPositionsTable(positionsFiltered);
  renderWalletsTable(walletsActive);
  renderDetailPanel();
  renderPagination("positions", positionsWindow);
  renderPagination("wallets", walletsWindow);
}

async function fetchSnapshot() {
  const controller = typeof AbortController === "function" ? new AbortController() : null;
  const timeoutId = controller
    ? setTimeout(() => {
        try {
          controller.abort();
        } catch (_error) {
          // ignore abort errors
        }
      }, SNAPSHOT_TIMEOUT_MS)
    : null;

  const res = await fetch(buildLiveTradesPath("/api/live-trades"), {
    cache: "no-store",
    signal: controller ? controller.signal : undefined,
  }).finally(() => {
    if (timeoutId) clearTimeout(timeoutId);
  });
  if (!res.ok) throw new Error(`live-trades: ${res.status}`);
  return res.json();
}

function applyPayload(payload, source = "poll") {
  state.payload = payload;
  state.stream.source = source;
  state.stream.lastPayloadAt = Date.now();
  state.stream.lastEventAt = tsMs(payload?.generatedAt) || Date.now();

  const nextPositionSignature = positionsSignature(payload);
  const nextWalletSignature = walletsSignature(payload);
  const nextLiveActiveSignature = liveActiveSignature(payload);
  const remapPositions = nextPositionSignature !== state.rowSignatures.positions;
  const remapWallets = nextWalletSignature !== state.rowSignatures.wallets;
  const remapLiveActive = nextLiveActiveSignature !== state.rowSignatures.liveActive;

  const nextSignature = payloadSignature(payload);
  if (remapPositions || remapWallets || remapLiveActive || nextSignature !== state.payloadSignature) {
    try {
      const nextRows = buildRows(payload, {
        includePositions: remapPositions,
        includeWallets: remapWallets,
        includeLiveActive: remapLiveActive,
      });
      const prevRows = state.rows || { positions: [], wallets: [], liveActive: [] };
      const summary = payload?.summary || {};
      const openPositionsTotal = Number.isFinite(n(summary.openPositionsTotal, NaN))
        ? n(summary.openPositionsTotal, 0)
        : payloadPositionsTotal(payload);
      const indexedWalletsTotal = Number.isFinite(n(summary.indexedWalletsTotal, NaN))
        ? n(summary.indexedWalletsTotal, 0)
        : payloadWalletsTotal(payload);
      const streamLikelyAlive = source === "sse" || state.stream.connection === "open";

      // Guard against transient empty snapshots that can briefly wipe the UI.
      const preservePositions =
        remapPositions &&
        streamLikelyAlive &&
        safeArray(nextRows.positions).length === 0 &&
        prevRows.positions.length > 0 &&
        openPositionsTotal > 0;
      const preserveWallets =
        remapWallets &&
        streamLikelyAlive &&
        safeArray(nextRows.wallets).length === 0 &&
        prevRows.wallets.length > 0 &&
        indexedWalletsTotal > 0;

      state.rows = {
        positions: remapPositions
          ? preservePositions
            ? prevRows.positions
            : safeArray(nextRows.positions)
          : prevRows.positions,
        wallets: remapWallets
          ? preserveWallets
            ? prevRows.wallets
            : safeArray(nextRows.wallets)
          : prevRows.wallets,
        liveActive: remapLiveActive ? safeArray(nextRows.liveActive) : prevRows.liveActive,
      };
      if (remapPositions) state.rowSignatures.positions = nextPositionSignature;
      if (remapWallets) state.rowSignatures.wallets = nextWalletSignature;
      if (remapLiveActive) state.rowSignatures.liveActive = nextLiveActiveSignature;
      state.payloadSignature = nextSignature;
    } catch (error) {
      state.stream.lastError = `payload_map_error: ${error && error.message ? error.message : "unknown"}`;
      state.stream.lastErrorAt = Date.now();
    }
  }
  render({ refreshOptions: true });
}

function stopEventSource() {
  if (state.stream.eventSource) {
    state.stream.eventSource.close();
    state.stream.eventSource = null;
  }
}

function ensureFallbackPolling() {
  if (state.stream.fallbackTimer) return;
  state.stream.fallbackTimer = setInterval(async () => {
    if (state.stream.paused) return;
    try {
      const payload = await fetchSnapshot();
      applyPayload(payload, "poll");
      if (state.stream.connection !== "open") state.stream.connection = "fallback";
    } catch (error) {
      state.stream.lastError = error.message;
      state.stream.lastErrorAt = Date.now();
    }
  }, 5000);
}

function clearFallbackPolling() {
  if (state.stream.fallbackTimer) {
    clearInterval(state.stream.fallbackTimer);
    state.stream.fallbackTimer = null;
  }
}

function connectSse() {
  if (state.stream.paused) return;
  stopEventSource();
  state.stream.connection = "connecting";

  const streamPath = buildLiveTradesPath("/api/live-trades/stream");
  const es = new EventSource(streamPath);
  state.stream.eventSource = es;

  const handleStreamMessage = (event) => {
    if (!event || !event.data) return;
    try {
      const message = JSON.parse(event.data);
      if (message && message.type === "heartbeat") {
        state.stream.lastEventAt = Date.now();
        renderHealth(state.payload || {});
        return;
      }
      if (message && message.type === "snapshot" && message.payload) {
        applyPayload(message.payload, "sse");
        return;
      }
      if (message && message.payload) {
        applyPayload(message.payload, "sse");
      }
    } catch (_error) {
      // ignore malformed message
    }
  };

  es.onopen = () => {
    state.stream.connection = "open";
    state.stream.retryMs = 1000;
    state.stream.lastError = null;
    clearFallbackPolling();
    render();
  };

  es.onmessage = handleStreamMessage;
  es.addEventListener("snapshot", handleStreamMessage);
  es.addEventListener("heartbeat", handleStreamMessage);

  es.onerror = () => {
    state.stream.lastError = "sse_error";
    state.stream.lastErrorAt = Date.now();
    state.stream.connection = "reconnecting";
    render();
    stopEventSource();
    ensureFallbackPolling();
    const waitMs = state.stream.retryMs;
    setTimeout(() => connectSse(), waitMs);
    state.stream.retryMs = Math.min(RECONNECT_MAX_MS, Math.round(waitMs * 1.6));
  };
}

function startHealthLoop() {
  if (state.stream.healthTimer) return;
  state.stream.healthTimer = setInterval(() => {
    renderHealth(state.payload || {});
  }, 1000);
}

function readNumber(id, options = {}) {
  const floorZero = options.floorZero !== false;
  const value = n(el(id)?.value, 0);
  return floorZero ? Math.max(0, value) : value;
}

function wireEvents() {
  el("lt-view-positions-btn")?.addEventListener("click", () => {
    state.activeView = "positions";
    if (state.selection.type && state.selection.type !== "positions") {
      state.selection = { type: "", key: "" };
    }
    render();
  });

  el("lt-view-wallets-btn")?.addEventListener("click", () => {
    state.activeView = "wallets";
    if (state.selection.type && state.selection.type !== "wallets") {
      state.selection = { type: "", key: "" };
    }
    render();
  });

  el("lt-detail-close")?.addEventListener("click", () => {
    state.selection = { type: "", key: "" };
    closeWalletDrawer();
    render();
  });

  window.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && (state.selection.key || state.drawer.wallet)) {
      state.selection = { type: "", key: "" };
      closeWalletDrawer();
      render();
    }
  });

  el("lt-filter-wallet")?.addEventListener("input", (event) => {
    state.filters.wallet = String(event.target.value || "");
    resetPagination();
    render();
  });

  el("lt-filter-symbol")?.addEventListener("change", (event) => {
    state.filters.symbol = String(event.target.value || "");
    resetPagination();
    render();
  });

  el("lt-filter-side")?.addEventListener("change", (event) => {
    state.filters.side = String(event.target.value || "");
    resetPagination();
    render();
  });

  el("lt-filter-min-usd")?.addEventListener("input", () => {
    state.filters.minUsd = readNumber("lt-filter-min-usd");
    resetPagination();
    render();
  });

  el("lt-filter-max-usd")?.addEventListener("input", () => {
    state.filters.maxUsd = readNumber("lt-filter-max-usd");
    resetPagination();
    render();
  });

  el("lt-filter-min-pnl")?.addEventListener("input", () => {
    state.filters.minPnl = readNumber("lt-filter-min-pnl", { floorZero: false });
    resetPagination();
    render();
  });

  el("lt-filter-max-pnl")?.addEventListener("input", () => {
    state.filters.maxPnl = readNumber("lt-filter-max-pnl", { floorZero: false });
    resetPagination();
    render();
  });

  el("lt-filter-freshness")?.addEventListener("change", (event) => {
    state.filters.freshness = String(event.target.value || "");
    resetPagination();
    render();
  });

  el("lt-filter-tracked")?.addEventListener("change", (event) => {
    state.filters.trackedOnly = Boolean(event.target.checked);
    resetPagination();
    render();
  });

  el("lt-positions-page-size")?.addEventListener("change", (event) => {
    state.pagination.positions.pageSize = Math.max(10, Math.floor(n(event.target.value, 100)));
    state.pagination.positions.page = 1;
    fetchSnapshot()
      .then((payload) => applyPayload(payload, "page"))
      .catch(() => {});
    connectSse();
  });

  el("lt-wallets-page-size")?.addEventListener("change", (event) => {
    state.pagination.wallets.pageSize = Math.max(10, Math.floor(n(event.target.value, 100)));
    state.pagination.wallets.page = 1;
    fetchSnapshot()
      .then((payload) => applyPayload(payload, "page"))
      .catch(() => {});
    connectSse();
  });

  el("lt-positions-page-prev")?.addEventListener("click", () => {
    state.pagination.positions.page = Math.max(1, n(state.pagination.positions.page, 1) - 1);
    fetchSnapshot()
      .then((payload) => applyPayload(payload, "page"))
      .catch(() => {});
    connectSse();
  });

  el("lt-positions-page-next")?.addEventListener("click", () => {
    state.pagination.positions.page = n(state.pagination.positions.page, 1) + 1;
    fetchSnapshot()
      .then((payload) => applyPayload(payload, "page"))
      .catch(() => {});
    connectSse();
  });

  el("lt-wallets-page-prev")?.addEventListener("click", () => {
    state.pagination.wallets.page = Math.max(1, n(state.pagination.wallets.page, 1) - 1);
    fetchSnapshot()
      .then((payload) => applyPayload(payload, "page"))
      .catch(() => {});
    connectSse();
  });

  el("lt-wallets-page-next")?.addEventListener("click", () => {
    state.pagination.wallets.page = n(state.pagination.wallets.page, 1) + 1;
    fetchSnapshot()
      .then((payload) => applyPayload(payload, "page"))
      .catch(() => {});
    connectSse();
  });

  el("lt-reset-filters")?.addEventListener("click", () => {
    state.search = "";
    state.filters = {
      wallet: "",
      symbol: "",
      side: "",
      minUsd: 0,
      maxUsd: 0,
      minPnl: 0,
      maxPnl: 0,
      freshness: "",
      trackedOnly: false,
    };

    [
      "lt-filter-wallet",
      "lt-filter-min-usd",
      "lt-filter-max-usd",
      "lt-filter-min-pnl",
      "lt-filter-max-pnl",
    ].forEach((id) => {
      if (el(id)) el(id).value = "";
    });

    ["lt-filter-symbol", "lt-filter-side", "lt-filter-freshness"].forEach((id) => {
      if (el(id)) el(id).value = "";
    });

    if (el("lt-filter-tracked")) el("lt-filter-tracked").checked = false;

    resetPagination();
    render();
  });

}

async function boot() {
  wireEvents();
  startHealthLoop();

  // Start live channels first so UI is never stuck waiting on an initial snapshot.
  state.stream.connection = "connecting";
  renderHealth(state.payload || {});
  connectSse();
  ensureFallbackPolling();

  try {
    const payload = await fetchSnapshot();
    applyPayload(payload, "bootstrap");
  } catch (_error) {
    // continue with stream fallback
  }

  if (typeof window !== "undefined") {
    window.__PF_LIVE_TRADE_BOOTED = true;
  }
}

boot();
