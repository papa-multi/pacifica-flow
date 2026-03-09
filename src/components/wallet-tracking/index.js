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

  function buildLiveTradesPayload() {
    const dashboard = pipeline.getDashboardPayload();
    const market = dashboard.market || {};
    const account = dashboard.account || {};

    const publicTradesBySymbol =
      market && market.publicTradesBySymbol && typeof market.publicTradesBySymbol === "object"
        ? market.publicTradesBySymbol
        : {};

    const publicTrades = Object.entries(publicTradesBySymbol)
      .flatMap(([symbol, rows]) =>
        (Array.isArray(rows) ? rows : []).map((row) => ({
          historyId: row.historyId || null,
          symbol: row.symbol || String(symbol || "").toUpperCase(),
          side: row.side || null,
          cause: row.cause || null,
          amount: row.amount || "0",
          price: row.price || "0",
          timestamp: Number(row.timestamp || 0),
          li: row.li || null,
        }))
      )
      .sort((a, b) => Number(b.timestamp || 0) - Number(a.timestamp || 0))
      .slice(0, 400);

    const accountTrades = Array.isArray(dashboard.trades && dashboard.trades.recent)
      ? dashboard.trades.recent.slice(0, 400)
      : [];
    const positions = Array.isArray(dashboard.positions && dashboard.positions.rows)
      ? dashboard.positions.rows.slice(0, 200)
      : [];
    const walletRows = walletStore ? walletStore.list().slice(0, 300) : [];

    return {
      generatedAt: Date.now(),
      sync: dashboard.sync || {},
      environment: dashboard.environment || {},
      summary: {
        publicTrades: publicTrades.length,
        accountTrades: accountTrades.length,
        openPositions: positions.length,
        indexedWallets: walletRows.length,
        wsStatus:
          (dashboard.environment && dashboard.environment.wsStatus) ||
          (dashboard.sync && dashboard.sync.wsStatus) ||
          "idle",
      },
      publicTrades,
      accountTrades,
      positions,
      walletPerformance: walletRows,
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

  async function handleRequest(_req, res, url) {
    syncWalletStoreFromDisk();
    const dashboard = pipeline.getDashboardPayload();
    syncCurrentWallet();

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
      sendJson(res, 200, buildLiveTradesPayload());
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
