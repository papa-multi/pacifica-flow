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
    if (typeof globalKpiProvider === "function") {
      const shared = globalKpiProvider();
      if (
        shared &&
        typeof shared === "object" &&
        Number(shared.fetchedAt || 0) > 0 &&
        Array.isArray(shared.prices)
      ) {
        return {
          prices: shared.prices,
          dailyVolume: Number(shared.dailyVolume || 0),
          openInterestAtEnd: Number(shared.openInterestAtEnd || 0),
          volumeMethod: shared.volumeMethod || "prices_rolling_24h",
          volumeSource: shared.volumeSource || "/api/v1/info/prices:sum(volume_24h)",
          volumeMeta: shared.volumeMeta || null,
          totalHistoricalVolume:
            shared.totalHistoricalVolume !== null &&
            shared.totalHistoricalVolume !== undefined &&
            Number.isFinite(Number(shared.totalHistoricalVolume))
              ? Number(shared.totalHistoricalVolume)
              : null,
          dailyVolumeFromPrices24h: Number.isFinite(Number(shared.dailyVolumeFromPrices24h))
            ? Number(shared.dailyVolumeFromPrices24h)
            : null,
          dailyVolumeDefillamaCompat: Number.isFinite(Number(shared.dailyVolumeDefillamaCompat))
            ? Number(shared.dailyVolumeDefillamaCompat)
            : null,
          stale: false,
          lastError: null,
          fetchedAt: Number(shared.fetchedAt || 0),
          fetchDurationMs: Number(shared.fetchDurationMs || 0),
          cacheHit: true,
          source: "global_kpi_worker",
        };
      }
    }

    if (
      Number(defillamaCache.ttlMs || 0) > 0 &&
      now - Number(defillamaCache.fetchedAt || 0) <= defillamaCache.ttlMs
    ) {
      return {
        ...defillamaCache.value,
        volumeMethod: "prices_rolling_24h",
        volumeSource: "/api/v1/info/prices:sum(volume_24h)",
        volumeMeta: null,
        totalHistoricalVolume: null,
        stale: Boolean(defillamaCache.stale),
        lastError: defillamaCache.lastError || null,
        fetchedAt: defillamaCache.fetchedAt || null,
        fetchDurationMs: defillamaCache.lastFetchDurationMs,
        cacheHit: true,
      };
    }
    if (!restClient || typeof restClient.get !== "function") {
      return {
        ...defillamaCache.value,
        volumeMethod: "prices_rolling_24h",
        volumeSource: "/api/v1/info/prices:sum(volume_24h)",
        volumeMeta: null,
        totalHistoricalVolume: null,
        stale: true,
        lastError: "rest_client_unavailable",
        fetchedAt: defillamaCache.fetchedAt || null,
        fetchDurationMs: defillamaCache.lastFetchDurationMs,
        cacheHit: true,
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
        volumeMethod: "prices_rolling_24h",
        volumeSource: "/api/v1/info/prices:sum(volume_24h)",
        volumeMeta: null,
        totalHistoricalVolume: null,
        stale: false,
        lastError: null,
        fetchedAt: defillamaCache.fetchedAt,
        fetchDurationMs,
        cacheHit: false,
        source: "local_fetch",
      };
    } catch (error) {
      // Never fall back to wallet-derived volume/rank for this KPI.
      // If live fetch fails, use last known DefiLlama snapshot if available.
      if (Number(defillamaCache.fetchedAt || 0) > 0) {
        defillamaCache.stale = true;
        defillamaCache.lastError = error.message || "defillama_fetch_failed";
        return {
          ...defillamaCache.value,
          volumeMethod: "prices_rolling_24h",
          volumeSource: "/api/v1/info/prices:sum(volume_24h)",
          volumeMeta: null,
          totalHistoricalVolume: null,
          stale: true,
          lastError: defillamaCache.lastError,
          fetchedAt: defillamaCache.fetchedAt || null,
          fetchDurationMs: defillamaCache.lastFetchDurationMs,
          cacheHit: true,
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
    walletStore.upsert(record);
  }

  async function handleRequest(_req, res, url) {
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
      const payload = buildExchangeOverviewPayload({
        state: pipeline.getState(),
        transport: pipeline.getTransportState(),
        wallets: walletStore ? walletStore.list() : [],
        timeframe: url.searchParams.get("timeframe"),
      });

      // Source of truth: use Pacifica /info/prices DefiLlama-style formula for volume/OI.
      // dailyVolume = sum(Number(volume_24h))
      // openInterestAtEnd = sum(Number(open_interest) * Number(mark))
      try {
        const truth = await getDefiLlamaPricesTruth();
        const volumeWindow = buildVolumeWindowInfo(truth);
        const historicalTotal =
          truth.totalHistoricalVolume !== null &&
          truth.totalHistoricalVolume !== undefined &&
          Number.isFinite(Number(truth.totalHistoricalVolume))
            ? Number(truth.totalHistoricalVolume)
            : null;
        payload.kpis.totalVolumeUsd = toFixed(
          historicalTotal !== null ? historicalTotal : truth.dailyVolume,
          2
        );
        payload.kpis.openInterestAtEnd = toFixed(truth.openInterestAtEnd, 2);
        payload.volumeRank = buildDefiLlamaVolumeRank(truth.prices, 100);
        payload.source = {
          ...(payload.source || {}),
          dailyVolumeSource: "/api/v1/info/prices:sum(volume_24h)",
          openInterestSource: "/api/v1/info/prices:sum(open_interest*mark)",
          volumeRankSource: "/api/v1/info/prices:rank_by(volume_24h)",
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

      payload.indexer =
        walletIndexer && typeof walletIndexer.getStatus === "function"
          ? walletIndexer.getStatus()
          : null;

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
        status:
          walletIndexer && typeof walletIndexer.getStatus === "function"
            ? walletIndexer.getStatus()
            : null,
      });
      return true;
    }

    if (url.pathname === "/api/wallets") {
      const payload = buildWalletExplorerPayload({
        wallets: walletStore ? walletStore.list() : [],
        query: Object.fromEntries(url.searchParams.entries()),
      });
      if (
        walletIndexer &&
        typeof walletIndexer.getWalletLifecycleMap === "function" &&
        Array.isArray(payload.rows)
      ) {
        const lifecycleMap = walletIndexer.getWalletLifecycleMap(payload.rows.map((row) => row.wallet));
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
      if (
        walletIndexer &&
        typeof walletIndexer.getWalletLifecycleSnapshot === "function" &&
        payload &&
        payload.wallet
      ) {
        payload.lifecycle = walletIndexer.getWalletLifecycleSnapshot(payload.wallet);
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
