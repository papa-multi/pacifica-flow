const {
  buildExchangeOverviewPayload,
  buildWalletExplorerPayload,
  buildWalletProfilePayload,
} = require("../../services/pipeline/api");
const { buildWalletRecordFromState } = require("../../services/analytics/wallet_stats");

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

function createWalletTrackingComponent({
  sendJson,
  pipeline,
  walletStore,
  walletIndexer,
  restClient,
  globalKpiProvider,
}) {
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
    minIntervalMs: 1500,
  };
  const indexerStatusCache = {
    fetchedAt: 0,
    ttlMs: 2000,
    value: null,
  };
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
      return indexerStatusCache.value || localStatus;
    } catch (_error) {
      return indexerStatusCache.value || localStatus;
    }
  }

  function parseWalletListParam(value) {
    return String(value || "")
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean);
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

  function buildLiveTradesPayload(options = {}) {
    const dashboard = pipeline.getDashboardPayload();
    const market = dashboard.market || {};
    const account = dashboard.account || {};
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
    const queryPositionOffset = parseWindowParam(options.positionOffset, 0);
    const queryPositionLimit = parseWindowParam(options.positionLimit, 0);
    const queryAccountTradeOffset = parseWindowParam(options.accountTradeOffset, 0);
    const queryAccountTradeLimit = parseWindowParam(options.accountTradeLimit, 0);

    const publicTradesBySymbol =
      market && market.publicTradesBySymbol && typeof market.publicTradesBySymbol === "object"
        ? market.publicTradesBySymbol
        : {};

    const publicTradesAll = Object.entries(publicTradesBySymbol)
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
            symbol: row.symbol || String(symbol || "").toUpperCase(),
            side: row.side || null,
            cause: row.cause || null,
            amount: row.amount || "0",
            price: row.price || "0",
            timestamp: Number(row.timestamp || 0),
            wallet,
            walletSource: wallet ? "payload" : "unresolved",
            li: row.li || null,
          };
        })
      )
      .sort((a, b) => Number(b.timestamp || 0) - Number(a.timestamp || 0));
    const publicTradesWindow = applyWindow(publicTradesAll, {
      offset: queryPublicOffset,
      limit: queryPublicLimit,
    });

    const accountTradesAll = Array.isArray(dashboard.trades && dashboard.trades.recent)
      ? dashboard.trades.recent
      : [];
    const accountTradesWindow = applyWindow(accountTradesAll, {
      offset: queryAccountTradeOffset,
      limit: queryAccountTradeLimit,
    });

    const positionsAll = Array.isArray(dashboard.positions && dashboard.positions.rows)
      ? dashboard.positions.rows
      : [];
    const positionsWindow = applyWindow(positionsAll, {
      offset: queryPositionOffset,
      limit: queryPositionLimit,
    });

    const walletRowsAll = walletStore ? walletStore.list() : [];
    const walletRowsWindow = applyWindow(walletRowsAll, {
      offset: queryWalletOffset,
      limit: queryWalletLimit,
    });
    const now = Date.now();
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
    const activeSymbols = Object.keys(publicTradesBySymbol)
      .map((symbol) => String(symbol || "").trim().toUpperCase())
      .filter(Boolean).length;
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
        indexedWallets: walletRowsWindow.returned,
        indexedWalletsTotal: walletRowsAll.length,
        indexedWalletsWindowed: walletRowsWindow.windowedByQuery,
        indexedWalletsHasMore: walletRowsWindow.hasMore,
        indexedWalletsOffset: walletRowsWindow.offset,
        indexedWalletsLimit: walletRowsWindow.limit,
        wsStatus:
          (dashboard.environment && dashboard.environment.wsStatus) ||
          (dashboard.sync && dashboard.sync.wsStatus) ||
          "idle",
        streamScope: "exchange_wide_public_trades",
        retentionPublicTradesPerSymbol,
      },
      marketContext: {
        fundingChips,
        longShortRatio: shortEvents > 0 ? Number((longEvents / shortEvents).toFixed(3)) : null,
        eventsPerMin: recentTrades.length,
        whaleEvents,
        activeSymbols,
        lastEventAt: publicTradesAll.length ? Number(publicTradesAll[0].timestamp || 0) : null,
      },
      publicTrades: publicTradesWindow.rows,
      accountTrades: accountTradesWindow.rows,
      positions: positionsWindow.rows,
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

      const pushSnapshot = () => {
        writeEvent("snapshot", {
          type: "snapshot",
          at: Date.now(),
          payload: buildLiveTradesPayload({
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
            accountTradeOffset: parseWindowParam(url.searchParams.get("account_trade_offset"), 0),
            accountTradeLimit: parseWindowParam(
              url.searchParams.get("account_trade_limit"),
              liveDefaultAccountTradeLimit
            ),
          }),
        });
      };

      const pushHeartbeat = () => {
        writeEvent("heartbeat", {
          type: "heartbeat",
          at: Date.now(),
        });
      };

      pushSnapshot();
      const snapshotTimer = setInterval(pushSnapshot, streamIntervalMs);
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
      const payload = buildExchangeOverviewPayload({
        state: pipeline.getState(),
        transport: pipeline.getTransportState(),
        wallets: walletStore ? walletStore.list() : [],
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

        payload.kpis.totalVolumeUsd = toFixed(selectedTotalVolume, 2);
        payload.kpis.totalVolumeCompact = toCompact(payload.kpis.totalVolumeUsd);
        payload.kpis.protocolCumulativeVolumeUsd = toFixed(
          historicalTotal !== null ? historicalTotal : 0,
          2
        );
        payload.kpis.openInterestAtEnd = toFixed(truth.openInterestAtEnd, 2);
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
      sendJson(
        res,
        200,
        buildLiveTradesPayload({
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
          accountTradeOffset: parseWindowParam(url.searchParams.get("account_trade_offset"), 0),
          accountTradeLimit: parseWindowParam(
            url.searchParams.get("account_trade_limit"),
            liveDefaultAccountTradeLimit
          ),
        })
      );
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
      const payload = buildWalletExplorerPayload({
        wallets: walletStore ? walletStore.list() : [],
        query: Object.fromEntries(url.searchParams.entries()),
      });
      if (Array.isArray(payload.rows)) {
        const lifecycleMap = await getLifecycleMap(payload.rows.map((row) => row.wallet));
        payload.rows = payload.rows.map((row) => ({
          ...row,
          lifecycle: lifecycleMap[row.wallet] || null,
        }));
      }
      sendJson(res, 200, payload);
      return true;
    }

    if (url.pathname === "/api/wallets/profile") {
      const payload = buildWalletProfilePayload({
        wallets: walletStore ? walletStore.list() : [],
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

  return {
    handleRequest,
  };
}

module.exports = {
  createWalletTrackingComponent,
};
