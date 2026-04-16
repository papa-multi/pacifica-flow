const path = require("path");
const fs = require("fs");
const {
  ensureDir,
  normalizeAddress,
  readJson,
  writeJsonAtomic,
} = require("../pipeline/utils");
const { buildWalletRecordFromHistory } = require("../analytics/wallet_stats");
const { assignShardByKey, shardIndexForKey } = require("./sharding");
const {
  buildWalletHistoryAudit,
  chooseStrongerPaginationSummary,
  deriveWalletHistoryTruth,
  isPaginationComplete,
} = require("./wallet_truth");

const STATE_VERSION = 5;
const HISTORY_CURSOR_HEAD = "__head__";
const WALLET_HISTORY_VERSION = 3;
const WALLET_HISTORY_PAGINATION_VERSION = 1;

const WALLET_LIFECYCLE = {
  DISCOVERED: "discovered",
  PENDING_BACKFILL: "pending_backfill",
  BACKFILLING: "backfilling",
  FULLY_INDEXED: "fully_indexed",
  LIVE_TRACKING: "live_tracking",
};

function emptyState() {
  return {
    version: STATE_VERSION,
    knownWallets: [],
    liveWallets: [],
    priorityQueue: [],
    continuationQueue: [],
    scanCursor: 0,
    liveScanCursor: 0,
    lastDiscoveryAt: null,
    lastScanAt: null,
    discoveryCycles: 0,
    scanCycles: 0,
    walletStates: {},
  };
}

function uniq(list = []) {
  return Array.from(new Set(Array.isArray(list) ? list : []));
}

function normalizeWallets(list = []) {
  return uniq(
    (Array.isArray(list) ? list : [])
      .map((value) => normalizeAddress(value))
      .filter(Boolean)
  );
}

function extractPayloadData(result, fallback = []) {
  if (!result || !result.payload) return fallback;
  if (Object.prototype.hasOwnProperty.call(result.payload, "data")) {
    return result.payload.data;
  }
  return fallback;
}

function toErrorMessage(error) {
  if (!error) return "unknown_error";
  if (error.payload && error.payload.error) {
    return `${error.message}: ${error.payload.error}`;
  }
  return error.message || "unknown_error";
}

function normalizeTradeHistoryRow(row = {}) {
  return {
    historyId: row.history_id || null,
    orderId: row.order_id || null,
    lastOrderId: row.last_order_id || null,
    li: row.last_order_id || null,
    symbol: String(row.symbol || "").toUpperCase(),
    amount: row.amount !== undefined ? row.amount : "0",
    price: row.price !== undefined ? row.price : "0",
    fee: row.fee !== undefined ? row.fee : "0",
    pnl: row.pnl !== undefined ? row.pnl : "0",
    timestamp: Number(
      row.timestamp !== undefined ? row.timestamp : row.created_at !== undefined ? row.created_at : 0
    ),
  };
}

function normalizeFundingHistoryRow(row = {}) {
  return {
    historyId: row.history_id || null,
    symbol: String(row.symbol || "").toUpperCase(),
    payout: row.payout !== undefined ? row.payout : "0",
    createdAt: Number(
      row.createdAt !== undefined ? row.createdAt : row.created_at !== undefined ? row.created_at : 0
    ),
  };
}

function summarizeErrorReason(message) {
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

function isRetriableRequestReason(reason) {
  const normalized = String(reason || "").toLowerCase();
  return (
    normalized === "rate_limit_429" ||
    normalized === "timeout" ||
    normalized === "service_unavailable_503" ||
    normalized === "server_error_500" ||
    normalized === "network_error" ||
    normalized === "connection_reset" ||
    normalized === "proxy_error"
  );
}

function normalizeTradeKey(row = {}) {
  const historyId = row && row.historyId ? String(row.historyId) : "";
  if (historyId) return `h:${historyId}`;
  return `f:${[
    Number(row.timestamp || 0),
    String(row.symbol || "").toUpperCase(),
    String(row.amount || "0"),
    String(row.price || "0"),
    String(row.fee || "0"),
    String(row.pnl || "0"),
  ].join("|")}`;
}

function normalizeFundingKey(row = {}) {
  const historyId = row && row.historyId ? String(row.historyId) : "";
  if (historyId) return `h:${historyId}`;
  return `f:${[
    Number(row.createdAt || 0),
    String(row.symbol || "").toUpperCase(),
    String(row.payout || "0"),
  ].join("|")}`;
}

function cursorCacheKey(cursor) {
  return cursor ? String(cursor) : HISTORY_CURSOR_HEAD;
}

function emptyWalletHistory(wallet) {
  return {
    version: WALLET_HISTORY_VERSION,
    wallet,
    trades: [],
    funding: [],
    tradeSeenKeys: {},
    fundingSeenKeys: {},
    pageCache: {
      trades: {},
      funding: {},
    },
    pagination: {
      version: WALLET_HISTORY_PAGINATION_VERSION,
      mode: "cursor",
      trades: {},
      funding: {},
    },
    audit: null,
    updatedAt: null,
  };
}

function emptyHistoryEndpointPagination() {
  return {
    version: WALLET_HISTORY_PAGINATION_VERSION,
    mode: "cursor",
    exhausted: false,
    frontierCursor: null,
    lastSuccessfulCursor: null,
    lastSuccessfulPage: null,
    lastSuccessfulAt: null,
    lastAttemptedCursor: null,
    lastAttemptedPage: null,
    lastAttemptedAt: null,
    highestFetchedPage: 0,
    highestKnownPage: 0,
    totalKnownPages: null,
    pages: {},
  };
}

function normalizeHistoryPageEntry(cursor, entry = {}) {
  const cursorKey = cursorCacheKey(cursor);
  const normalizedStatus = String(entry.status || "").trim().toLowerCase();
  const status =
    normalizedStatus === "persisted" ||
    normalizedStatus === "failed" ||
    normalizedStatus === "pending"
      ? normalizedStatus
      : "persisted";
  return {
    cursor: cursorKey === HISTORY_CURSOR_HEAD ? null : cursorKey,
    cursorKey,
    pageIndex:
      Number.isFinite(Number(entry.pageIndex)) && Number(entry.pageIndex) > 0
        ? Number(entry.pageIndex)
        : null,
    status,
    hasMore: Boolean(entry.hasMore),
    nextCursor: entry.nextCursor || null,
    rowCount:
      Number.isFinite(Number(entry.rowCount)) && Number(entry.rowCount) >= 0
        ? Number(entry.rowCount)
        : null,
    firstAt:
      Number.isFinite(Number(entry.firstAt)) && Number(entry.firstAt) > 0
        ? Number(entry.firstAt)
        : null,
    lastAt:
      Number.isFinite(Number(entry.lastAt)) && Number(entry.lastAt) > 0
        ? Number(entry.lastAt)
        : null,
    requests:
      Number.isFinite(Number(entry.requests)) && Number(entry.requests) > 0
        ? Number(entry.requests)
        : 0,
    retryCount:
      Number.isFinite(Number(entry.retryCount)) && Number(entry.retryCount) > 0
        ? Number(entry.retryCount)
        : 0,
    discoveredAt:
      Number.isFinite(Number(entry.discoveredAt)) && Number(entry.discoveredAt) > 0
        ? Number(entry.discoveredAt)
        : null,
    fetchedAt:
      Number.isFinite(Number(entry.fetchedAt)) && Number(entry.fetchedAt) > 0
        ? Number(entry.fetchedAt)
        : null,
    lastAttemptedAt:
      Number.isFinite(Number(entry.lastAttemptedAt)) && Number(entry.lastAttemptedAt) > 0
        ? Number(entry.lastAttemptedAt)
        : null,
    lastError: entry.lastError || null,
    legacy: Boolean(entry.legacy),
  };
}

function summarizePageRows(rows = [], timestampKey) {
  let firstAt = null;
  let lastAt = null;
  (Array.isArray(rows) ? rows : []).forEach((row) => {
    const ts = Number(
      row && row[timestampKey] !== undefined
        ? row[timestampKey]
        : row && row.created_at !== undefined
        ? row.created_at
        : row && row.createdAt !== undefined
        ? row.createdAt
        : 0
    );
    if (!Number.isFinite(ts) || ts <= 0) return;
    firstAt = firstAt === null ? ts : Math.min(firstAt, ts);
    lastAt = lastAt === null ? ts : Math.max(lastAt, ts);
  });
  return {
    rowCount: Array.isArray(rows) ? rows.length : 0,
    firstAt,
    lastAt,
  };
}

function findEndpointPaginationPredecessor(pages = {}, cursor) {
  const cursorKey = cursorCacheKey(cursor);
  return Object.values(pages || {}).find((page) => {
    if (!page || typeof page !== "object") return false;
    return cursorCacheKey(page.nextCursor || null) === cursorKey;
  }) || null;
}

function ensureEndpointPaginationPage(state, cursor, options = {}) {
  const safeState =
    state && typeof state === "object" ? state : emptyHistoryEndpointPagination();
  const pages =
    safeState.pages && typeof safeState.pages === "object" ? safeState.pages : {};
  const cursorKey = cursorCacheKey(cursor);
  const existing =
    pages[cursorKey] && typeof pages[cursorKey] === "object" ? pages[cursorKey] : null;
  if (existing) {
    if (!Number.isFinite(Number(existing.pageIndex)) || Number(existing.pageIndex) <= 0) {
      const predecessor = findEndpointPaginationPredecessor(pages, cursor);
      existing.pageIndex =
        predecessor && Number.isFinite(Number(predecessor.pageIndex))
          ? Number(predecessor.pageIndex) + 1
          : cursorKey === HISTORY_CURSOR_HEAD
          ? 1
          : Math.max(
              1,
              ...Object.values(pages)
                .map((page) => Number(page && page.pageIndex ? page.pageIndex : 0))
                .filter((value) => Number.isFinite(value))
            ) + (cursorKey === HISTORY_CURSOR_HEAD ? 0 : 1);
    }
    if (options.status && existing.status === "pending") {
      existing.status = String(options.status);
    }
    return existing;
  }
  const predecessor = findEndpointPaginationPredecessor(pages, cursor);
  const highestKnownPage = Math.max(
    0,
    ...Object.values(pages)
      .map((page) => Number(page && page.pageIndex ? page.pageIndex : 0))
      .filter((value) => Number.isFinite(value))
  );
  const pageIndex =
    Number.isFinite(Number(options.pageIndex)) && Number(options.pageIndex) > 0
      ? Number(options.pageIndex)
      : predecessor && Number.isFinite(Number(predecessor.pageIndex))
      ? Number(predecessor.pageIndex) + 1
      : cursorKey === HISTORY_CURSOR_HEAD
      ? 1
      : highestKnownPage + 1;
  const next = normalizeHistoryPageEntry(cursor, {
    status: options.status || "pending",
    pageIndex,
    discoveredAt: options.discoveredAt || Date.now(),
  });
  pages[cursorKey] = next;
  safeState.pages = pages;
  return next;
}

function finalizeEndpointPaginationState(state = {}) {
  const safeState = {
    ...emptyHistoryEndpointPagination(),
    ...(state && typeof state === "object" ? state : {}),
  };
  const pages = {};
  Object.entries(
    safeState.pages && typeof safeState.pages === "object" ? safeState.pages : {}
  ).forEach(([cursorKey, entry]) => {
    pages[cursorKey] = normalizeHistoryPageEntry(cursorKey, entry);
  });
  if (!pages[HISTORY_CURSOR_HEAD] && !safeState.exhausted) {
    pages[HISTORY_CURSOR_HEAD] = normalizeHistoryPageEntry(null, {
      status: "pending",
      pageIndex: 1,
      discoveredAt: Date.now(),
    });
  }

  let currentKey = HISTORY_CURSOR_HEAD;
  let pageIndex = 1;
  const visited = new Set();
  let exhausted = false;
  let frontierCursor = safeState.frontierCursor || null;
  while (pages[currentKey] && !visited.has(currentKey)) {
    const page = pages[currentKey];
    visited.add(currentKey);
    if (!Number.isFinite(Number(page.pageIndex)) || Number(page.pageIndex) <= 0) {
      page.pageIndex = pageIndex;
    }
    pageIndex = Math.max(pageIndex + 1, Number(page.pageIndex || 0) + 1);
    if (page.hasMore && page.nextCursor) {
      const nextKey = cursorCacheKey(page.nextCursor);
      if (!pages[nextKey]) {
        pages[nextKey] = normalizeHistoryPageEntry(page.nextCursor, {
          status: "pending",
          pageIndex: Number(page.pageIndex || 0) + 1,
          discoveredAt: page.fetchedAt || page.lastAttemptedAt || Date.now(),
        });
      }
      frontierCursor = page.nextCursor;
      currentKey = nextKey;
      continue;
    }
    if (page.status === "persisted" && (!page.hasMore || !page.nextCursor)) {
      exhausted = true;
      frontierCursor = null;
    }
    break;
  }

  Object.values(pages)
    .filter((page) => !visited.has(page.cursorKey))
    .sort((left, right) => {
      const leftTs = Number(left.fetchedAt || left.lastAttemptedAt || left.discoveredAt || 0);
      const rightTs = Number(right.fetchedAt || right.lastAttemptedAt || right.discoveredAt || 0);
      if (leftTs !== rightTs) return leftTs - rightTs;
      return String(left.cursorKey || "").localeCompare(String(right.cursorKey || ""));
    })
    .forEach((page) => {
      if (!Number.isFinite(Number(page.pageIndex)) || Number(page.pageIndex) <= 0) {
        page.pageIndex = pageIndex;
        pageIndex += 1;
      }
    });

  const orderedPages = Object.values(pages).sort(
    (left, right) => Number(left.pageIndex || 0) - Number(right.pageIndex || 0)
  );
  let highestFetchedPage = 0;
  let highestKnownPage = 0;
  let lastSuccessfulPage = null;
  let lastSuccessfulCursor = null;
  let lastSuccessfulAt = null;
  let lastAttemptedPage = null;
  let lastAttemptedCursor = null;
  let lastAttemptedAt = null;
  orderedPages.forEach((page) => {
    const pageNumber = Number(page.pageIndex || 0);
    highestKnownPage = Math.max(highestKnownPage, pageNumber);
    if (page.status === "persisted") {
      highestFetchedPage = Math.max(highestFetchedPage, pageNumber);
      if (Number(page.fetchedAt || 0) >= Number(lastSuccessfulAt || 0)) {
        lastSuccessfulAt = Number(page.fetchedAt || page.lastAttemptedAt || 0) || null;
        lastSuccessfulPage = pageNumber || null;
        lastSuccessfulCursor = page.cursor;
      }
    }
    if (Number(page.lastAttemptedAt || 0) >= Number(lastAttemptedAt || 0)) {
      lastAttemptedAt = Number(page.lastAttemptedAt || 0) || null;
      lastAttemptedPage = pageNumber || null;
      lastAttemptedCursor = page.cursor;
    }
  });

  safeState.pages = pages;
  safeState.exhausted = exhausted || Boolean(safeState.exhausted && frontierCursor === null);
  safeState.frontierCursor = safeState.exhausted ? null : frontierCursor;
  safeState.highestFetchedPage = highestFetchedPage;
  safeState.highestKnownPage = highestKnownPage;
  safeState.totalKnownPages = safeState.exhausted ? highestKnownPage || 0 : null;
  safeState.lastSuccessfulPage = lastSuccessfulPage;
  safeState.lastSuccessfulCursor = lastSuccessfulCursor;
  safeState.lastSuccessfulAt = lastSuccessfulAt;
  safeState.lastAttemptedPage = lastAttemptedPage;
  safeState.lastAttemptedCursor = lastAttemptedCursor;
  safeState.lastAttemptedAt = lastAttemptedAt;
  return safeState;
}

function normalizeHistoryEndpointPagination(rawState = {}, rawPageCache = {}) {
  const state = {
    ...emptyHistoryEndpointPagination(),
    ...(rawState && typeof rawState === "object" ? rawState : {}),
    pages: {},
  };
  const rawPages =
    rawState && rawState.pages && typeof rawState.pages === "object" ? rawState.pages : {};
  Object.entries(rawPages).forEach(([cursorKey, entry]) => {
    state.pages[cursorKey] = normalizeHistoryPageEntry(cursorKey, entry);
  });
  Object.entries(rawPageCache && typeof rawPageCache === "object" ? rawPageCache : {}).forEach(
    ([cursorKey, entry]) => {
      if (state.pages[cursorKey]) return;
      state.pages[cursorKey] = normalizeHistoryPageEntry(cursorKey, {
        status: "persisted",
        hasMore: Boolean(entry && entry.hasMore),
        nextCursor: entry && entry.nextCursor ? entry.nextCursor : null,
        fetchedAt: Number(entry && entry.fetchedAt ? entry.fetchedAt : 0) || null,
        lastAttemptedAt:
          Number(entry && (entry.touchedAt || entry.fetchedAt) ? entry.touchedAt || entry.fetchedAt : 0) ||
          null,
        legacy: true,
      });
    }
  );
  return finalizeEndpointPaginationState(state);
}

function normalizeWalletHistoryPagination(rawPagination = {}, rawPageCache = {}) {
  const safePagination = rawPagination && typeof rawPagination === "object" ? rawPagination : {};
  return {
    version: WALLET_HISTORY_PAGINATION_VERSION,
    mode: "cursor",
    trades: normalizeHistoryEndpointPagination(
      safePagination.trades,
      rawPageCache && rawPageCache.trades ? rawPageCache.trades : {}
    ),
    funding: normalizeHistoryEndpointPagination(
      safePagination.funding,
      rawPageCache && rawPageCache.funding ? rawPageCache.funding : {}
    ),
  };
}

function isHistoryEndpointPaginationSummaryShape(state = {}) {
  if (!state || typeof state !== "object" || Array.isArray(state)) return false;
  if (state.pages && typeof state.pages === "object") return false;
  return (
    Object.prototype.hasOwnProperty.call(state, "retryPages") ||
    Object.prototype.hasOwnProperty.call(state, "pendingPages") ||
    Object.prototype.hasOwnProperty.call(state, "fetchedPages") ||
    Object.prototype.hasOwnProperty.call(state, "verifiedPages") ||
    Object.prototype.hasOwnProperty.call(state, "frontierCursor") ||
    Object.prototype.hasOwnProperty.call(state, "highestFetchedPage") ||
    Object.prototype.hasOwnProperty.call(state, "highestKnownPage") ||
    Object.prototype.hasOwnProperty.call(state, "totalKnownPages")
  );
}

function normalizeHistoryEndpointPaginationSummary(state = {}) {
  const safeState = state && typeof state === "object" ? state : {};
  const missingPageRanges = Array.isArray(safeState.missingPageRanges)
    ? safeState.missingPageRanges
        .map((entry) => ({
          page:
            Number.isFinite(Number(entry && entry.page)) && Number(entry.page) > 0
              ? Number(entry.page)
              : null,
          cursor:
            entry && Object.prototype.hasOwnProperty.call(entry, "cursor")
              ? entry.cursor || null
              : null,
          reason: String((entry && entry.reason) || "retry_pending"),
        }))
        .filter((entry) => entry.page !== null || entry.cursor !== null)
    : [];
  return {
    mode: "cursor",
    exhausted: Boolean(safeState.exhausted),
    frontierCursor:
      safeState.frontierCursor !== undefined && safeState.frontierCursor !== null
        ? safeState.frontierCursor
        : null,
    highestFetchedPage: Number(safeState.highestFetchedPage || 0),
    highestKnownPage: Number(safeState.highestKnownPage || 0),
    totalKnownPages:
      safeState.totalKnownPages !== undefined && safeState.totalKnownPages !== null
        ? Number(safeState.totalKnownPages || 0)
        : null,
    fetchedPages: Number(safeState.fetchedPages || 0),
    verifiedPages: Number(
      safeState.verifiedPages !== undefined ? safeState.verifiedPages : safeState.fetchedPages || 0
    ),
    retryPages: Number(safeState.retryPages || 0),
    pendingPages: Number(safeState.pendingPages || 0),
    lastSuccessfulPage:
      Number.isFinite(Number(safeState.lastSuccessfulPage)) && Number(safeState.lastSuccessfulPage) > 0
        ? Number(safeState.lastSuccessfulPage)
        : null,
    lastSuccessfulCursor:
      safeState.lastSuccessfulCursor !== undefined && safeState.lastSuccessfulCursor !== null
        ? safeState.lastSuccessfulCursor
        : null,
    lastSuccessfulAt:
      Number.isFinite(Number(safeState.lastSuccessfulAt)) && Number(safeState.lastSuccessfulAt) > 0
        ? Number(safeState.lastSuccessfulAt)
        : null,
    lastAttemptedPage:
      Number.isFinite(Number(safeState.lastAttemptedPage)) && Number(safeState.lastAttemptedPage) > 0
        ? Number(safeState.lastAttemptedPage)
        : null,
    lastAttemptedCursor:
      safeState.lastAttemptedCursor !== undefined && safeState.lastAttemptedCursor !== null
        ? safeState.lastAttemptedCursor
        : null,
    lastAttemptedAt:
      Number.isFinite(Number(safeState.lastAttemptedAt)) && Number(safeState.lastAttemptedAt) > 0
        ? Number(safeState.lastAttemptedAt)
        : null,
    missingPageRanges,
  };
}

function buildHistoryEndpointPaginationSummary(state = {}) {
  if (isHistoryEndpointPaginationSummaryShape(state)) {
    return normalizeHistoryEndpointPaginationSummary(state);
  }
  const safeState = finalizeEndpointPaginationState(state);
  const orderedPages = Object.values(safeState.pages || {}).sort(
    (left, right) => Number(left.pageIndex || 0) - Number(right.pageIndex || 0)
  );
  const retryPages = orderedPages.filter((page) => page.status === "failed");
  const pendingPages = orderedPages.filter((page) => page.status === "pending");
  const persistedPages = orderedPages.filter((page) => page.status === "persisted");
  return {
    mode: "cursor",
    exhausted: Boolean(safeState.exhausted),
    frontierCursor: safeState.frontierCursor || null,
    highestFetchedPage: Number(safeState.highestFetchedPage || 0),
    highestKnownPage: Number(safeState.highestKnownPage || 0),
    totalKnownPages:
      safeState.totalKnownPages !== null && safeState.totalKnownPages !== undefined
        ? Number(safeState.totalKnownPages || 0)
        : null,
    fetchedPages: persistedPages.length,
    verifiedPages: persistedPages.length,
    retryPages: retryPages.length,
    pendingPages: pendingPages.length,
    lastSuccessfulPage:
      Number.isFinite(Number(safeState.lastSuccessfulPage)) && Number(safeState.lastSuccessfulPage) > 0
        ? Number(safeState.lastSuccessfulPage)
        : null,
    lastSuccessfulCursor: safeState.lastSuccessfulCursor || null,
    lastSuccessfulAt:
      Number.isFinite(Number(safeState.lastSuccessfulAt)) && Number(safeState.lastSuccessfulAt) > 0
        ? Number(safeState.lastSuccessfulAt)
        : null,
    lastAttemptedPage:
      Number.isFinite(Number(safeState.lastAttemptedPage)) && Number(safeState.lastAttemptedPage) > 0
        ? Number(safeState.lastAttemptedPage)
        : null,
    lastAttemptedCursor: safeState.lastAttemptedCursor || null,
    lastAttemptedAt:
      Number.isFinite(Number(safeState.lastAttemptedAt)) && Number(safeState.lastAttemptedAt) > 0
        ? Number(safeState.lastAttemptedAt)
        : null,
    missingPageRanges: retryPages.map((page) => ({
      page: Number(page.pageIndex || 0) || null,
      cursor: page.cursor || null,
      reason: page.lastError || "retry_pending",
    })),
  };
}

function isHistoryEndpointPaginationComplete(summary = null) {
  if (!summary || typeof summary !== "object") return false;
  const safeSummary = buildHistoryEndpointPaginationSummary(summary);
  return (
    Boolean(safeSummary.exhausted) &&
    Number(safeSummary.retryPages || 0) <= 0 &&
    !safeSummary.frontierCursor
  );
}

function historyEndpointPaginationScore(summary = null) {
  if (!summary || typeof summary !== "object") return -1;
  const safeSummary = buildHistoryEndpointPaginationSummary(summary);
  const complete = isHistoryEndpointPaginationComplete(safeSummary);
  return (
    (complete ? 1000000 : 0) +
    (Boolean(safeSummary.exhausted) ? 100000 : 0) +
    Number(safeSummary.verifiedPages || 0) * 1000 +
    Number(safeSummary.fetchedPages || 0) * 100 +
    Number(safeSummary.highestFetchedPage || 0) * 10 +
    (safeSummary.frontierCursor ? 0 : 1) -
    Number(safeSummary.retryPages || 0) * 500 -
    Number(safeSummary.pendingPages || 0) * 5 +
    Number(safeSummary.lastSuccessfulAt || 0) / 1000000000000
  );
}

function chooseStrongerHistoryEndpointPaginationSummary(existing = null, incoming = null) {
  const existingScore = historyEndpointPaginationScore(existing);
  const incomingScore = historyEndpointPaginationScore(incoming);
  if (incomingScore > existingScore) {
    return incoming ? buildHistoryEndpointPaginationSummary(incoming) : null;
  }
  if (existingScore >= 0) {
    return existing ? buildHistoryEndpointPaginationSummary(existing) : null;
  }
  return incoming ? buildHistoryEndpointPaginationSummary(incoming) : null;
}

function mergeWalletHistoryPagination(basePagination = {}, incomingPagination = {}) {
  const base = normalizeWalletHistoryPagination(basePagination);
  const incoming = normalizeWalletHistoryPagination(incomingPagination);
  const mergeEndpoint = (left = {}, right = {}) => {
    const merged = {
      ...left,
      ...right,
      pages: {
        ...(left.pages && typeof left.pages === "object" ? left.pages : {}),
        ...(right.pages && typeof right.pages === "object" ? right.pages : {}),
      },
    };
    return finalizeEndpointPaginationState(merged);
  };
  return {
    version: WALLET_HISTORY_PAGINATION_VERSION,
    mode: "cursor",
    trades: mergeEndpoint(base.trades, incoming.trades),
    funding: mergeEndpoint(base.funding, incoming.funding),
  };
}

function nextHistoryEndpointCursor(state = {}, fallbackCursor = null) {
  if (isHistoryEndpointPaginationSummaryShape(state)) {
    const safeSummary = normalizeHistoryEndpointPaginationSummary(state);
    const retryEntry = Array.isArray(safeSummary.missingPageRanges)
      ? safeSummary.missingPageRanges.find((entry) => entry && entry.cursor !== undefined)
      : null;
    if (retryEntry) return retryEntry.cursor || null;
    if (safeSummary.frontierCursor !== undefined && safeSummary.frontierCursor !== null) {
      return safeSummary.frontierCursor;
    }
    if (fallbackCursor !== undefined && fallbackCursor !== null) return fallbackCursor;
    return null;
  }
  const safeState = finalizeEndpointPaginationState(state);
  const orderedPages = Object.values(safeState.pages || {}).sort(
    (left, right) => Number(left.pageIndex || 0) - Number(right.pageIndex || 0)
  );
  const retryPage = orderedPages.find((page) => page.status === "failed");
  if (retryPage) return retryPage.cursor || null;
  if (safeState.frontierCursor !== undefined && safeState.frontierCursor !== null) {
    return safeState.frontierCursor;
  }
  if (fallbackCursor !== undefined && fallbackCursor !== null) return fallbackCursor;
  if (safeState.exhausted) return null;
  return null;
}

function buildHistoryEndpointFallbackResult(options = {}) {
  const paginationState =
    options && options.paginationState && typeof options.paginationState === "object"
      ? finalizeEndpointPaginationState(options.paginationState)
      : normalizeHistoryEndpointPagination();
  return {
    rows: [],
    pages: 0,
    hasMore: Boolean(options && options.hasMore),
    done: Boolean(options && options.done),
    nextCursor:
      options && Object.prototype.hasOwnProperty.call(options, "nextCursor")
        ? options.nextCursor || null
        : nextHistoryEndpointCursor(paginationState, null),
    cacheHits: 0,
    requests: 0,
    pageCache:
      options && options.pageCache && typeof options.pageCache === "object"
        ? { ...options.pageCache }
        : {},
    paginationState,
  };
}

function rebuildHistoryIndex(rows = [], normalizeRow, normalizeKey) {
  const nextRows = [];
  const seenKeys = {};
  let duplicatesRemoved = 0;
  (Array.isArray(rows) ? rows : []).forEach((row) => {
    const normalizedRow = normalizeRow(row);
    const key = normalizeKey(normalizedRow);
    if (seenKeys[key]) {
      duplicatesRemoved += 1;
      return;
    }
    seenKeys[key] = 1;
    nextRows.push(normalizedRow);
  });
  return {
    rows: nextRows,
    seenKeys,
    duplicatesRemoved,
  };
}

function summarizeHistoryCollection(rows = [], timestampKey) {
  let firstAt = null;
  let lastAt = null;
  (Array.isArray(rows) ? rows : []).forEach((row) => {
    const ts = Number(row && row[timestampKey] ? row[timestampKey] : 0);
    if (!Number.isFinite(ts) || ts <= 0) return;
    firstAt = firstAt === null ? ts : Math.min(firstAt, ts);
    lastAt = lastAt === null ? ts : Math.max(lastAt, ts);
  });
  return {
    count: Array.isArray(rows) ? rows.length : 0,
    firstAt,
    lastAt,
  };
}

function normalizeWalletHistoryPayload(wallet, history = {}, options = {}) {
  const rebuildSeenKeys = Boolean(options.rebuildSeenKeys);
  const base = emptyWalletHistory(wallet);
  const raw = history && typeof history === "object" ? history : {};

  const tradeIndex = rebuildSeenKeys
    ? rebuildHistoryIndex(raw.trades, normalizeTradeHistoryRow, normalizeTradeKey)
    : {
        rows: Array.isArray(raw.trades) ? raw.trades.map(normalizeTradeHistoryRow) : [],
        seenKeys:
          raw.tradeSeenKeys && typeof raw.tradeSeenKeys === "object"
            ? { ...raw.tradeSeenKeys }
            : {},
        duplicatesRemoved: 0,
      };
  const fundingIndex = rebuildSeenKeys
    ? rebuildHistoryIndex(raw.funding, normalizeFundingHistoryRow, normalizeFundingKey)
    : {
        rows: Array.isArray(raw.funding) ? raw.funding.map(normalizeFundingHistoryRow) : [],
        seenKeys:
          raw.fundingSeenKeys && typeof raw.fundingSeenKeys === "object"
            ? { ...raw.fundingSeenKeys }
            : {},
        duplicatesRemoved: 0,
      };

  if (!rebuildSeenKeys) {
    if (Object.keys(tradeIndex.seenKeys).length < tradeIndex.rows.length) {
      const rebuilt = rebuildHistoryIndex(tradeIndex.rows, normalizeTradeHistoryRow, normalizeTradeKey);
      tradeIndex.rows = rebuilt.rows;
      tradeIndex.seenKeys = rebuilt.seenKeys;
      tradeIndex.duplicatesRemoved += rebuilt.duplicatesRemoved;
    }
    if (Object.keys(fundingIndex.seenKeys).length < fundingIndex.rows.length) {
      const rebuilt = rebuildHistoryIndex(
        fundingIndex.rows,
        normalizeFundingHistoryRow,
        normalizeFundingKey
      );
      fundingIndex.rows = rebuilt.rows;
      fundingIndex.seenKeys = rebuilt.seenKeys;
      fundingIndex.duplicatesRemoved += rebuilt.duplicatesRemoved;
    }
  }

  const pageCache = {
    trades:
      raw.pageCache && raw.pageCache.trades && typeof raw.pageCache.trades === "object"
        ? raw.pageCache.trades
        : {},
    funding:
      raw.pageCache && raw.pageCache.funding && typeof raw.pageCache.funding === "object"
        ? raw.pageCache.funding
        : {},
  };
  const pagination = normalizeWalletHistoryPagination(raw.pagination, pageCache);
  const tradesSummary = summarizeHistoryCollection(tradeIndex.rows, "timestamp");
  const fundingSummary = summarizeHistoryCollection(fundingIndex.rows, "createdAt");
  const tradePaginationSummary = buildHistoryEndpointPaginationSummary(pagination.trades);
  const fundingPaginationSummary = buildHistoryEndpointPaginationSummary(pagination.funding);
  const persistedAt =
    Number(options.persistedAt || raw.updatedAt || Date.now()) || Date.now();

  return {
    ...base,
    ...raw,
    wallet,
    version: WALLET_HISTORY_VERSION,
    trades: tradeIndex.rows,
    funding: fundingIndex.rows,
    tradeSeenKeys: tradeIndex.seenKeys,
    fundingSeenKeys: fundingIndex.seenKeys,
    pageCache,
    pagination,
    updatedAt: persistedAt,
    audit: {
      version: WALLET_HISTORY_VERSION,
      persistedAt,
      tradesStored: tradesSummary.count,
      fundingStored: fundingSummary.count,
      tradeSeenKeys: Object.keys(tradeIndex.seenKeys).length,
      fundingSeenKeys: Object.keys(fundingIndex.seenKeys).length,
      firstTradeAt: tradesSummary.firstAt,
      lastTradeAt: tradesSummary.lastAt,
      firstFundingAt: fundingSummary.firstAt,
      lastFundingAt: fundingSummary.lastAt,
      pageCacheTradeEntries: Object.keys(pageCache.trades || {}).length,
      pageCacheFundingEntries: Object.keys(pageCache.funding || {}).length,
      duplicateTradesRemoved: tradeIndex.duplicatesRemoved,
      duplicateFundingRemoved: fundingIndex.duplicatesRemoved,
      pagination: {
        trades: tradePaginationSummary,
        funding: fundingPaginationSummary,
      },
    },
  };
}

function mergeWalletHistoryPayloads(wallet, baseHistory = {}, incomingHistory = {}, persistedAt = Date.now()) {
  const base = normalizeWalletHistoryPayload(wallet, baseHistory, { rebuildSeenKeys: false });
  const incoming = normalizeWalletHistoryPayload(wallet, incomingHistory, {
    rebuildSeenKeys: false,
    persistedAt,
  });

  const trades = base.trades.slice();
  const tradeSeenKeys = { ...base.tradeSeenKeys };
  incoming.trades.forEach((row) => {
    const key = normalizeTradeKey(row);
    if (tradeSeenKeys[key]) return;
    tradeSeenKeys[key] = 1;
    trades.push(row);
  });

  const funding = base.funding.slice();
  const fundingSeenKeys = { ...base.fundingSeenKeys };
  incoming.funding.forEach((row) => {
    const key = normalizeFundingKey(row);
    if (fundingSeenKeys[key]) return;
    fundingSeenKeys[key] = 1;
    funding.push(row);
  });

  return normalizeWalletHistoryPayload(
    wallet,
    {
      ...base,
      ...incoming,
      trades,
      funding,
      tradeSeenKeys,
      fundingSeenKeys,
      pageCache: {
        trades: {
          ...(base.pageCache && base.pageCache.trades ? base.pageCache.trades : {}),
          ...(incoming.pageCache && incoming.pageCache.trades ? incoming.pageCache.trades : {}),
        },
        funding: {
          ...(base.pageCache && base.pageCache.funding ? base.pageCache.funding : {}),
          ...(incoming.pageCache && incoming.pageCache.funding ? incoming.pageCache.funding : {}),
        },
      },
      pagination: mergeWalletHistoryPagination(base.pagination, incoming.pagination),
    },
    {
      rebuildSeenKeys: false,
      persistedAt,
    }
  );
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, Number(value)));
}

function normalizeClientId(value) {
  const text = String(value || "").trim();
  return text || null;
}

class ExchangeWalletIndexer {
  constructor(options = {}) {
    this.restClient = options.restClient;
    const rawConfiguredClientEntries = Array.isArray(options.restClientEntries)
      ? options.restClientEntries
      : [];
    this.restClients = Array.isArray(options.restClients)
      ? options.restClients.filter((client) => client && typeof client.get === "function")
      : [];
    if (!this.restClients.length && rawConfiguredClientEntries.length) {
      this.restClients = rawConfiguredClientEntries
        .map((entry) => (entry && entry.client ? entry.client : null))
        .filter((client) => client && typeof client.get === "function");
    }
    if (!this.restClients.length && this.restClient && typeof this.restClient.get === "function") {
      this.restClients = [this.restClient];
    }
    if (!this.restClients.length) {
      throw new Error("ExchangeWalletIndexer requires at least one rest client.");
    }
    this.restClientIds = Array.isArray(options.restClientIds) ? options.restClientIds : [];
    this.restClientEntries = [];
    const configuredClientEntries = rawConfiguredClientEntries.length
      ? rawConfiguredClientEntries
          .filter((entry) => entry && entry.client && typeof entry.client.get === "function")
          .map((entry, idx) => ({
            id: String(entry.id || this.restClientIds[idx] || `client_${idx + 1}`),
            client: entry.client,
            proxyUrl: entry.proxyUrl || null,
            rateGuard:
              entry.rateGuard && typeof entry.rateGuard.getState === "function"
                ? entry.rateGuard
                : null,
          }))
      : [];
    if (configuredClientEntries.length) {
      this.restClientEntries = configuredClientEntries;
    } else {
      this.restClientEntries = this.restClients.map((client, idx) => ({
        id: String(this.restClientIds[idx] || `client_${idx + 1}`),
        client,
        proxyUrl: null,
        rateGuard: null,
      }));
    }
    this.restClients = this.restClientEntries.map((entry) => entry.client);
    this.restClientIds = this.restClientEntries.map((entry) => entry.id);
    this.clientPoolState = this.restClientEntries.map((entry) => ({
      id: entry.id,
      client: entry.client,
      proxyUrl: entry.proxyUrl || null,
      rateGuard: entry.rateGuard || null,
      inFlight: 0,
      requests: 0,
      successes: 0,
      failures: 0,
      failures429: 0,
      failures500: 0,
      timeoutFailures: 0,
      networkFailures: 0,
      consecutiveFailures: 0,
      consecutive429: 0,
      cooldownUntil: 0,
      lastUsedAt: 0,
      lastErrorReason: null,
      lastErrorAt: 0,
      latencyMsAvg: 0,
      latencySampleCount: 0,
      weight: 1,
      backfillCooldownUntil: 0,
      qualityWindows: [],
    }));
    this.clientPoolById = new Map(this.clientPoolState.map((entry) => [entry.id, entry]));
    this.walletStore = options.walletStore;
    this.walletSource = options.walletSource || null;
    this.walletHistoryCache = new Map();
    this.walletHistoryCacheSize = Math.max(
      128,
      Number(options.walletHistoryCacheSize || 2048)
    );
    this.onchainDiscovery = options.onchainDiscovery || null;
    this.liveTriggerStore = options.liveTriggerStore || null;
    this.logger = options.logger || console;

    this.dataDir = options.dataDir || path.join(process.cwd(), "data", "indexer");
    this.statePath = options.statePath || path.join(this.dataDir, "indexer_state.json");
    this.onchainStatePath =
      options.onchainStatePath || path.join(this.dataDir, "onchain_state.json");
    this.depositWalletsPath =
      options.depositWalletsPath || path.join(this.dataDir, "deposit_wallets.json");
    this.walletHistoryDir =
      options.walletHistoryDir || path.join(this.dataDir, "wallet_history");
    this.workerShardCount = Math.max(
      1,
      Number(options.workerShardCount || 1)
    );
    this.workerShardIndex = Math.max(
      0,
      Math.min(
        this.workerShardCount - 1,
        Number(options.workerShardIndex || 0)
      )
    );
    this.workerShardId = String(
      options.workerShardId ||
        (this.workerShardCount > 1
          ? `indexer_shard_${this.workerShardIndex}`
          : "indexer_primary")
    ).trim();

    this.seedWallets = normalizeWallets(options.seedWallets || []);

    this.scanIntervalMs = Math.max(5000, Number(options.scanIntervalMs || 30000));
    this.discoveryIntervalMs = Math.max(15000, Number(options.discoveryIntervalMs || 120000));
    this.maxWalletsPerScan = Math.max(1, Number(options.maxWalletsPerScan || 40));
    this.maxPagesPerWallet = Math.max(1, Number(options.maxPagesPerWallet || 3));
    this.fullHistoryPerWallet = (() => {
      if (options.fullHistoryPerWallet === undefined) return true;
      return String(options.fullHistoryPerWallet).toLowerCase() !== "false";
    })();
    this.tradesPageLimit = Math.max(20, Math.min(4000, Number(options.tradesPageLimit || 400)));
    this.fundingPageLimit = Math.max(20, Math.min(4000, Number(options.fundingPageLimit || 400)));
    this.walletScanConcurrency = Math.max(
      1,
      Math.min(2048, Number(options.walletScanConcurrency || 4))
    );
    this.restShardParallelismMax = Math.max(
      1,
      Math.min(256, Number(options.restShardParallelismMax || 1))
    );
    this.liveWalletsPerScanConfigured = Math.max(
      0,
      Math.min(
        2048,
        Number(options.liveWalletsPerScan || Math.max(1, this.maxWalletsPerScan))
      )
    );
    this.liveWalletsPerScanMin = Math.max(
      1,
      Math.min(
        512,
        Number(options.liveWalletsPerScanMin || Math.max(4, Math.ceil(this.walletScanConcurrency * 0.1)))
      )
    );
    this.liveWalletsPerScanMax = Math.max(
      this.liveWalletsPerScanMin,
      Math.min(2048, Number(options.liveWalletsPerScanMax || 1024))
    );
    this.liveWalletsPerScan = Math.max(
      this.liveWalletsPerScanConfigured,
      this.liveWalletsPerScanMin
    );
    this.liveRefreshTargetMs = Math.max(
      5000,
      Number(options.liveRefreshTargetMs || 90000)
    );
    this.liveRefreshConcurrency = Math.max(
      1,
      Math.min(
        2048,
        Number(
          options.liveRefreshConcurrency ||
            Math.max(
              this.liveWalletsPerScanMin,
              Math.min(512, this.liveWalletsPerScanMax, this.clientPoolState.length || 1)
            )
        )
      )
    );
    this.liveMaxPagesPerWallet = Math.max(
      1,
      Math.min(50, Number(options.liveMaxPagesPerWallet || 1))
    );
    this.liveTriggerPollMs = Math.max(
      500,
      Number(options.liveTriggerPollMs || 2000)
    );
    this.liveTriggerReadLimit = Math.max(
      100,
      Number(options.liveTriggerReadLimit || 5000)
    );
    this.liveTriggerTtlMs = Math.max(
      10000,
      Number(options.liveTriggerTtlMs || 10 * 60 * 1000)
    );
    this.fullHistoryPagesPerScan = Math.max(
      1,
      Number(options.fullHistoryPagesPerScan || 12)
    );
    this.deepHistoryPagesPerScan = Math.max(
      1,
      Math.min(
        this.fullHistoryPagesPerScan,
        Number(
          options.deepHistoryPagesPerScan ||
            Math.max(1, Math.min(this.fullHistoryPagesPerScan, 3))
        )
      )
    );
    this.activationBootstrapPages = Math.max(
      1,
      Number(options.activationBootstrapPages || 1)
    );
    this.activationTradesPageLimit = Math.max(
      20,
      Math.min(this.tradesPageLimit, Number(options.activationTradesPageLimit || 100))
    );
    this.activationFundingPageLimit = Math.max(
      20,
      Math.min(this.fundingPageLimit, Number(options.activationFundingPageLimit || 100))
    );
    this.deepHistoryTradesPageLimit = Math.max(
      20,
      Math.min(
        this.tradesPageLimit,
        Number(
          options.deepHistoryTradesPageLimit ||
            Math.max(20, Math.min(this.tradesPageLimit, 160))
        )
      )
    );
    this.deepHistoryFundingPageLimit = Math.max(
      20,
      Math.min(
        this.fundingPageLimit,
        Number(
          options.deepHistoryFundingPageLimit ||
            Math.max(20, Math.min(this.fundingPageLimit, 160))
        )
      )
    );
    this.activationReserveMin = Math.max(
      1,
      Number(options.activationReserveMin || Math.max(12, Math.ceil(this.maxWalletsPerScan * 0.35)))
    );
    this.activationReserveMax = Math.max(
      this.activationReserveMin,
      Number(
        options.activationReserveMax ||
          Math.max(this.activationReserveMin, Math.ceil(this.maxWalletsPerScan * 0.6))
      )
    );
    this.continuationReserveMin = Math.max(
      1,
      Number(
        options.continuationReserveMin ||
          Math.max(8, Math.ceil(this.maxWalletsPerScan * 0.2))
      )
    );
    this.continuationReserveMax = Math.max(
      this.continuationReserveMin,
      Number(
        options.continuationReserveMax ||
          Math.max(this.continuationReserveMin, Math.ceil(this.maxWalletsPerScan * 0.45))
      )
    );
    this.deepBackfillMinPerScan = Math.max(
      0,
      Number(
        options.deepBackfillMinPerScan ||
          Math.max(1, Math.ceil(this.maxWalletsPerScan * 0.05))
      )
    );
    this.backfillPageBudgetWhenLivePressure = Math.max(
      1,
      Number(options.backfillPageBudgetWhenLivePressure || 2)
    );
    this.stateSaveMinIntervalMs = Math.max(
      500,
      Number(options.stateSaveMinIntervalMs || 15000)
    );
    this.cacheEntriesPerEndpoint = Math.max(
      20,
      Number(options.cacheEntriesPerEndpoint || 500)
    );
    this.historyAuditRepairBatchSize = Math.max(
      0,
      Number(
        options.historyAuditRepairBatchSize ||
          Math.max(4, Math.min(24, Math.ceil(this.maxWalletsPerScan * 0.025)))
      )
    );
    this.historyAuditRepairHotWalletLimit = Math.max(
      this.historyAuditRepairBatchSize,
      Number(options.historyAuditRepairHotWalletLimit || 1024)
    );
    this.topCorrectionCohortSize = Math.max(
      10,
      Number(options.topCorrectionCohortSize || 100)
    );

    this.discoveryCost = Math.max(1, Number(options.discoveryCost || 1));
    this.historyCost = Math.max(1, Number(options.historyCost || 2));

    this.onchainPagesPerDiscoveryCycle = Math.max(
      1,
      Number(options.onchainPagesPerDiscoveryCycle || 2)
    );
    this.onchainPagesMaxPerCycle = Math.max(
      this.onchainPagesPerDiscoveryCycle,
      Number(options.onchainPagesMaxPerCycle || this.onchainPagesPerDiscoveryCycle)
    );
    this.onchainValidatePerCycle = Math.max(
      0,
      Number(options.onchainValidatePerCycle || 20)
    );
    this.discoveryOnly = Boolean(options.discoveryOnly);
    this.backlogModeEnabled =
      String(options.backlogModeEnabled !== undefined ? options.backlogModeEnabled : true) !==
      "false";
    this.backlogWalletThreshold = Math.max(
      1,
      Number(options.backlogWalletThreshold || 2000)
    );
    this.backlogAvgWaitMsThreshold = Math.max(
      1000,
      Number(options.backlogAvgWaitMsThreshold || 45 * 60 * 1000)
    );
    this.backlogDiscoverEveryCycles = Math.max(
      1,
      Number(options.backlogDiscoverEveryCycles || 8)
    );
    this.backlogRefillBatch = Math.max(
      this.maxWalletsPerScan,
      Number(options.backlogRefillBatch || this.maxWalletsPerScan * 8)
    );
    this.scanRampQuietMs = Math.max(
      10000,
      Number(options.scanRampQuietMs || 180000)
    );
    this.scanRampStepMs = Math.max(
      5000,
      Number(options.scanRampStepMs || 60000)
    );
    this.client429CooldownBaseMs = Math.max(
      1000,
      Number(options.client429CooldownBaseMs || 2000)
    );
    this.client429CooldownMaxMs = Math.max(
      this.client429CooldownBaseMs,
      Number(options.client429CooldownMaxMs || 120000)
    );
    this.clientServerErrorCooldownBaseMs = Math.max(
      250,
      Number(options.clientServerErrorCooldownBaseMs || 1000)
    );
    this.clientServerErrorCooldownMaxMs = Math.max(
      this.clientServerErrorCooldownBaseMs,
      Number(options.clientServerErrorCooldownMaxMs || 30000)
    );
    this.clientTimeoutCooldownBaseMs = Math.max(
      250,
      Number(options.clientTimeoutCooldownBaseMs || 1500)
    );
    this.clientTimeoutCooldownMaxMs = Math.max(
      this.clientTimeoutCooldownBaseMs,
      Number(options.clientTimeoutCooldownMaxMs || 45000)
    );
    this.clientProxyErrorCooldownBaseMs = Math.max(
      1000,
      Number(options.clientProxyErrorCooldownBaseMs || 5000)
    );
    this.clientProxyErrorCooldownMaxMs = Math.max(
      this.clientProxyErrorCooldownBaseMs,
      Number(options.clientProxyErrorCooldownMaxMs || 300000)
    );
    this.clientDefaultCooldownMs = Math.max(
      100,
      Number(options.clientDefaultCooldownMs || 1000)
    );
    this.backfillProxyMinRequests = Math.max(
      5,
      Number(options.backfillProxyMinRequests || 20)
    );
    this.backfillProxyHealthyFloor = Math.max(
      4,
      Math.min(32, Number(options.backfillProxyHealthyFloor || 12))
    );
    this.backfillProxyMinWeight = clamp(
      Number(options.backfillProxyMinWeight || 0.7),
      0.2,
      4
    );
    this.backfillProxyMaxLatencyMs = Math.max(
      2000,
      Number(options.backfillProxyMaxLatencyMs || 18000)
    );
    this.backfillProxyMaxFailureRatio = clamp(
      Number(options.backfillProxyMaxFailureRatio || 0.58),
      0.05,
      0.95
    );
    this.backfillProxyMaxTimeoutRatio = clamp(
      Number(options.backfillProxyMaxTimeoutRatio || 0.5),
      0.05,
      0.95
    );
    this.backfillProxyMaxNetworkRatio = clamp(
      Number(options.backfillProxyMaxNetworkRatio || 0.35),
      0.05,
      0.95
    );
    this.backfillProxyStrictContinuationThreshold = Math.max(
      this.maxWalletsPerScan * 4,
      Number(options.backfillProxyStrictContinuationThreshold || this.maxWalletsPerScan * 8)
    );
    this.backfillProxyStrictMinWeight = clamp(
      Number(options.backfillProxyStrictMinWeight || Math.max(this.backfillProxyMinWeight, 0.95)),
      0.2,
      4
    );
    this.backfillProxyStrictMaxLatencyMs = Math.max(
      2000,
      Math.min(
        this.backfillProxyMaxLatencyMs,
        Number(options.backfillProxyStrictMaxLatencyMs || 14000)
      )
    );
    this.backfillProxyStrictMaxFailureRatio = clamp(
      Number(
        options.backfillProxyStrictMaxFailureRatio ||
          Math.min(this.backfillProxyMaxFailureRatio, 0.45)
      ),
      0.05,
      0.95
    );
    this.backfillProxyStrictMaxTimeoutRatio = clamp(
      Number(
        options.backfillProxyStrictMaxTimeoutRatio ||
          Math.min(this.backfillProxyMaxTimeoutRatio, 0.35)
      ),
      0.05,
      0.95
    );
    this.backfillProxyStrictMaxNetworkRatio = clamp(
      Number(
        options.backfillProxyStrictMaxNetworkRatio ||
          Math.min(this.backfillProxyMaxNetworkRatio, 0.22)
      ),
      0.05,
      0.95
    );
    this.backfillProxyVeryHighLatencyMs = Math.max(
      this.backfillProxyMaxLatencyMs,
      Number(options.backfillProxyVeryHighLatencyMs || 28000)
    );
    this.backfillProxyVeryHighLatencyMinSamples = Math.max(
      this.backfillProxyMinRequests,
      Number(options.backfillProxyVeryHighLatencyMinSamples || 12)
    );
    this.backfillQualityWindowMs = Math.max(
      10000,
      Number(options.backfillQualityWindowMs || 30000)
    );
    this.backfillQualityWindowCount = Math.max(
      3,
      Math.min(24, Number(options.backfillQualityWindowCount || 6))
    );
    this.backfillWindowHighLatencyCountThreshold = Math.max(
      1,
      Number(options.backfillWindowHighLatencyCountThreshold || 2)
    );
    this.backfillWindowBadCountThreshold = Math.max(
      1,
      Number(options.backfillWindowBadCountThreshold || 2)
    );
    this.backfillWindowCooldownMs = Math.max(
      30000,
      Number(options.backfillWindowCooldownMs || 120000)
    );
    this.backfillProxyStrictMinActiveWindows = Math.max(
      1,
      Number(options.backfillProxyStrictMinActiveWindows || 2)
    );
    this.backfillProxyStrictWarmupMinRequests = Math.max(
      2,
      Number(
        options.backfillProxyStrictWarmupMinRequests ||
          Math.min(this.backfillProxyMinRequests, 8)
      )
    );
    this.deepBackfillMaxSharePerProxy = clamp(
      Number(options.deepBackfillMaxSharePerProxy || 0.14),
      0.02,
      0.5
    );
    this.deepBackfillMinTasksPerProxy = Math.max(
      1,
      Number(options.deepBackfillMinTasksPerProxy || 4)
    );
    this.deepBackfillMaxTasksPerProxy = Math.max(
      this.deepBackfillMinTasksPerProxy,
      Number(options.deepBackfillMaxTasksPerProxy || 16)
    );
    this.historyRequestMaxAttempts = Math.max(
      1,
      Math.min(6, Number(options.historyRequestMaxAttempts || 3))
    );
    this.walletLeaseMs = Math.max(
      this.scanIntervalMs * 2,
      Number(options.walletLeaseMs || 2 * 60 * 1000)
    );
    this.activateTaskTimeoutMs = Math.max(
      5000,
      Number(options.activateTaskTimeoutMs || 20000)
    );
    this.recentTaskTimeoutMs = Math.max(
      this.activateTaskTimeoutMs,
      Number(options.recentTaskTimeoutMs || 30000)
    );
    this.backfillTaskTimeoutMs = Math.max(
      this.recentTaskTimeoutMs,
      Number(options.backfillTaskTimeoutMs || 45000)
    );
    this.liveTaskTimeoutMs = Math.max(
      5000,
      Number(options.liveTaskTimeoutMs || 20000)
    );
    this.activateRequestTimeoutMs = Math.max(
      3000,
      Number(
        options.activateRequestTimeoutMs ||
          Math.min(this.activateTaskTimeoutMs, 10000)
      )
    );
    this.recentRequestTimeoutMs = Math.max(
      3000,
      Number(
        options.recentRequestTimeoutMs ||
          Math.min(this.recentTaskTimeoutMs, 10000)
      )
    );
    this.backfillRequestTimeoutMs = Math.max(
      3000,
      Number(
        options.backfillRequestTimeoutMs ||
          Math.min(this.backfillTaskTimeoutMs, 12000)
      )
    );
    this.liveRequestTimeoutMs = Math.max(
      3000,
      Number(
        options.liveRequestTimeoutMs ||
          Math.min(this.liveTaskTimeoutMs, 9000)
      )
    );
    this.restShardStrategy = String(options.restShardStrategy || "wallet_hash")
      .trim()
      .toLowerCase();
    this.liveRefreshLoopIntervalMs = Math.max(
      5000,
      Number(options.liveRefreshLoopIntervalMs || 5000)
    );

    this.state = emptyState();
    this.runtime = {
      running: false,
      inDiscovery: false,
      inScan: false,
      inRepair: false,
      inLiveRefresh: false,
      discoveryTimer: null,
      scanTimer: null,
      repairTimer: null,
      liveRefreshTimer: null,
      nextDiscoveryAt: null,
      nextScanAt: null,
      nextRepairAt: null,
      nextLiveRefreshAt: null,
      discoveryDelayMs: null,
      scanDelayMs: null,
      repairDelayMs: null,
      liveRefreshDelayMs: null,
      lastDiscoveryStartedAt: null,
      lastScanStartedAt: null,
      lastLiveRefreshStartedAt: null,
      lastDiscoveryDurationMs: null,
      lastScanDurationMs: null,
      lastRepairDurationMs: null,
      lastLiveRefreshDurationMs: null,
      lastError: null,
      onchainPagesCurrent: this.onchainPagesPerDiscoveryCycle,
      onchainLast429At: 0,
      onchainLastRampAt: 0,
      backlogMode: false,
      backlogReason: null,
      priorityScanQueue: [],
      priorityScanSet: new Set(),
      priorityEnqueuedAt: new Map(),
      continuationScanQueue: [],
      continuationScanSet: new Set(),
      continuationEnqueuedAt: new Map(),
      liveScanQueue: [],
      liveScanSet: new Set(),
      liveEnqueuedAt: new Map(),
      liveRefreshCursor: 0,
      liveRefreshSnapshot: null,
      lastLiveRefreshSummary: null,
      hotLiveWallets: [],
      hotLiveWalletsAt: 0,
      liveTriggeredWalletAt: new Map(),
      lastLiveTriggerReadAt: 0,
      lastLiveTriggerObservedAt: 0,
      lastLiveTriggerSummary: null,
      stateDirty: false,
      lastStateSaveAt: 0,
      scanConcurrencyCurrent: this.walletScanConcurrency,
      scanPagesCurrent: this.maxPagesPerWallet,
      scanLast429At: 0,
      scanLastRampAt: 0,
      clientPickCursor: 0,
      shardQueueSnapshot: null,
      depositRegistryUpdatedAt: 0,
      onchainStateSnapshotAt: 0,
      onchainStateSnapshot: null,
      historyRepairCursor: 0,
      legacyVerificationCursor: 0,
      lastHistoryRepairSummary: null,
      walletPriorityCache: null,
      walletPriorityCacheAt: 0,
      lastContinuationRebalance: null,
      topCorrectionWallets: [],
      topCorrectionWalletsAt: 0,
      lastTopCorrectionPromotion: null,
    };
  }

  isWalletOwnedByWorker(wallet) {
    const normalized = normalizeAddress(wallet);
    if (!normalized) return false;
    if (this.workerShardCount <= 1) return true;
    return shardIndexForKey(`wallet:${normalized}`, this.workerShardCount) === this.workerShardIndex;
  }

  filterOwnedWallets(wallets = []) {
    return normalizeWallets(wallets).filter((wallet) => this.isWalletOwnedByWorker(wallet));
  }

  buildWalletStorageRefs(wallet) {
    const normalized = normalizeAddress(wallet);
    if (!normalized) return {};
    return {
      indexerShardId: this.workerShardId,
      indexerStatePath: this.statePath,
      walletHistoryPath: this.walletHistoryPath(normalized),
    };
  }

  isBackfillCompleteRow(row) {
    const safe = row || {};
    const hasSuccess =
      Number(safe.lastSuccessAt || 0) > 0 || Number(safe.scansSucceeded || 0) > 0;
    if (!hasSuccess) return false;
    if (!this.isRemoteHistoryVerifiedRow(safe)) return false;
    if (!Boolean(safe.tradeDone) || !Boolean(safe.fundingDone)) return false;
    if (safe.tradeCursor) return false;
    if (safe.fundingCursor) return false;
    return true;
  }

  isRemoteHistoryVerifiedRow(row) {
    const safe = row || {};
    return Boolean(safe.remoteHistoryVerified) || Number(safe.remoteHistoryVerifiedAt || 0) > 0;
  }

  getWalletPaginationSummary(row, stream = "trades") {
    const safe = row || {};
    const normalizedStream = String(stream || "trades").trim().toLowerCase();
    const rowKey = normalizedStream === "funding" ? "fundingPagination" : "tradePagination";
    const auditKey = normalizedStream === "funding" ? "funding" : "trades";
    const direct =
      safe[rowKey] && typeof safe[rowKey] === "object" ? safe[rowKey] : null;
    if (direct) return buildHistoryEndpointPaginationSummary(direct);
    const historyAudit =
      safe.historyAudit && typeof safe.historyAudit === "object" ? safe.historyAudit : null;
    const auditPagination =
      historyAudit &&
      historyAudit.pagination &&
      typeof historyAudit.pagination === "object" &&
      historyAudit.pagination[auditKey] &&
      typeof historyAudit.pagination[auditKey] === "object"
        ? historyAudit.pagination[auditKey]
        : null;
    if (auditPagination) return buildHistoryEndpointPaginationSummary(auditPagination);
    const fileAudit =
      historyAudit && historyAudit.file && typeof historyAudit.file === "object"
        ? historyAudit.file
        : null;
    const filePagination =
      fileAudit &&
      fileAudit.pagination &&
      typeof fileAudit.pagination === "object" &&
      fileAudit.pagination[auditKey] &&
      typeof fileAudit.pagination[auditKey] === "object"
        ? fileAudit.pagination[auditKey]
        : null;
    return filePagination ? buildHistoryEndpointPaginationSummary(filePagination) : null;
  }

  isWalletPaginationStreamComplete(row, stream = "trades") {
    return isHistoryEndpointPaginationComplete(this.getWalletPaginationSummary(row, stream));
  }

  needsPaginationStateRepair(row) {
    const safe = row || {};
    const hasSuccess =
      Number(safe.lastSuccessAt || 0) > 0 || Number(safe.scansSucceeded || 0) > 0;
    if (!hasSuccess) return false;
    const tradeComplete = this.isWalletPaginationStreamComplete(safe, "trades");
    const fundingComplete = this.isWalletPaginationStreamComplete(safe, "funding");
    if (!tradeComplete && !fundingComplete) return false;
    const tradeMismatch =
      tradeComplete &&
      (!Boolean(safe.tradeDone) || Boolean(safe.tradeHasMore) || Boolean(safe.tradeCursor));
    const fundingMismatch =
      fundingComplete &&
      (!Boolean(safe.fundingDone) || Boolean(safe.fundingHasMore) || Boolean(safe.fundingCursor));
    const remoteMismatch =
      tradeComplete &&
      fundingComplete &&
      !this.isRemoteHistoryVerifiedRow(safe);
    return tradeMismatch || fundingMismatch || remoteMismatch;
  }

  needsRemoteHistoryVerification(row) {
    const safe = row || {};
    const hasSuccess =
      Number(safe.lastSuccessAt || 0) > 0 || Number(safe.scansSucceeded || 0) > 0;
    if (!hasSuccess) return false;
    if (this.isRemoteHistoryVerifiedRow(safe)) return false;
    const paginationComplete =
      this.isWalletPaginationStreamComplete(safe, "trades") &&
      this.isWalletPaginationStreamComplete(safe, "funding");
    if (!Boolean(safe.tradeDone) && !Boolean(safe.fundingDone) && !paginationComplete) {
      return false;
    }
    if ((safe.tradeCursor || safe.fundingCursor) && !paginationComplete) return false;
    return (
      Number(safe.tradeRowsLoaded || 0) > 0 ||
      Number(safe.fundingRowsLoaded || 0) > 0 ||
      paginationComplete
    );
  }

  needsHistoryAuditRepair(row) {
    const safe = row || {};
    if (!this.isBackfillCompleteRow(safe)) return false;
    const historyAudit =
      safe.historyAudit && typeof safe.historyAudit === "object" ? safe.historyAudit : null;
    const fileAudit =
      historyAudit && historyAudit.file && typeof historyAudit.file === "object"
        ? historyAudit.file
        : null;
    const pendingReasons = Array.isArray(historyAudit && historyAudit.pendingReasons)
      ? historyAudit.pendingReasons
      : [];
    const warnings = Array.isArray(historyAudit && historyAudit.warnings)
      ? historyAudit.warnings
      : [];
    if (!fileAudit) return true;
    if (pendingReasons.length > 0 || warnings.length > 0) return true;
    if (Number(fileAudit.tradesStored || 0) < Number(safe.tradeRowsLoaded || 0)) return true;
    if (Number(fileAudit.fundingStored || 0) < Number(safe.fundingRowsLoaded || 0)) return true;
    return false;
  }

  rewriteWalletHistory(wallet, history, options = {}) {
    ensureDir(this.walletHistoryDir);
    const rebuildSeenKeys = Boolean(options.rebuildSeenKeys);
    const normalized = normalizeWalletHistoryPayload(wallet, history, {
      rebuildSeenKeys,
      persistedAt: Date.now(),
    });
    writeJsonAtomic(this.walletHistoryPath(wallet), normalized);
    return normalized;
  }

  repairWalletHistoryFile(wallet) {
    ensureDir(this.walletHistoryDir);
    const filePath = this.walletHistoryPath(wallet);
    const raw = readJson(filePath, null);
    if (!raw || typeof raw !== "object") return null;
    const hasSeenIndexes =
      raw.tradeSeenKeys &&
      typeof raw.tradeSeenKeys === "object" &&
      raw.fundingSeenKeys &&
      typeof raw.fundingSeenKeys === "object";
    const tradesLength = Array.isArray(raw.trades) ? raw.trades.length : 0;
    const fundingLength = Array.isArray(raw.funding) ? raw.funding.length : 0;
    const tradeSeenLength = hasSeenIndexes ? Object.keys(raw.tradeSeenKeys).length : 0;
    const fundingSeenLength = hasSeenIndexes ? Object.keys(raw.fundingSeenKeys).length : 0;
    const needsFullRebuild =
      !hasSeenIndexes ||
      tradeSeenLength < tradesLength ||
      fundingSeenLength < fundingLength;
    return this.rewriteWalletHistory(wallet, raw, {
      rebuildSeenKeys: needsFullRebuild,
    });
  }

  collectHistoryAuditRepairCandidates(limit = this.historyAuditRepairBatchSize) {
    const max = Math.max(0, Number(limit || 0));
    if (max <= 0) return [];
    const selected = [];
    const seen = new Set();
    const maybeAdd = (wallet) => {
      const normalized = normalizeAddress(wallet);
      if (!normalized || seen.has(normalized)) return;
      const row = this.state.walletStates && this.state.walletStates[normalized];
      if (
        !this.needsHistoryAuditRepair(row) &&
        !this.needsRemoteHistoryVerification(row) &&
        !this.needsPaginationStateRepair(row)
      ) {
        return;
      }
      seen.add(normalized);
      selected.push(normalized);
    };

    this.getHotLiveWallets(this.historyAuditRepairHotWalletLimit).forEach((wallet) => {
      if (selected.length >= max) return;
      maybeAdd(wallet);
    });

    this.getTopCorrectionWallets(
      Math.max(max * 2, this.topCorrectionCohortSize),
      false
    ).forEach((wallet) => {
      if (selected.length >= max) return;
      maybeAdd(wallet);
    });

    this.runtime.priorityScanQueue
      .slice(0, Math.max(max * 4, 256))
      .forEach((wallet) => {
        if (selected.length >= max) return;
        maybeAdd(wallet);
      });

    this.runtime.continuationScanQueue
      .slice(0, Math.max(max * 8, 512))
      .forEach((wallet) => {
        if (selected.length >= max) return;
        maybeAdd(wallet);
      });

    const known = normalizeWallets(this.state.knownWallets || []);
    if (!known.length || selected.length >= max) return selected;

    let cursor = Math.max(0, Number(this.runtime.historyRepairCursor || 0)) % known.length;
    let scanned = 0;
    while (selected.length < max && scanned < known.length) {
      maybeAdd(known[cursor]);
      cursor = (cursor + 1) % known.length;
      scanned += 1;
    }
    this.runtime.historyRepairCursor = cursor;
    return selected;
  }

  queueWalletForAuditRepairFetch(
    wallet,
    row = null,
    reason = "history_audit_missing",
    options = {}
  ) {
    const normalized = normalizeAddress(wallet);
    if (!normalized) return false;
    const now = Date.now();
    const current = row && typeof row === "object" ? row : this.getOrInitWalletState(normalized, "repair");
    const normalizedReason = String(reason || "history_audit_missing").trim().toLowerCase();
    const forceHeadRefetchRequested = Object.prototype.hasOwnProperty.call(
      options,
      "forceHeadRefetch"
    )
      ? Boolean(options.forceHeadRefetch)
      : normalizedReason.includes("remote_history") ||
        normalizedReason.includes("top_correction") ||
        normalizedReason === "history_file_missing" ||
        normalizedReason === "history_file_invalid";
    const nextState = {
      ...current,
      wallet: normalized,
      historyPhase:
        String((current && current.historyPhase) || "").trim().toLowerCase() === "recent"
          ? "recent"
          : "deep",
      lifecycleStage:
        Number((current && current.liveTrackingSince) || 0) > 0
          ? WALLET_LIFECYCLE.LIVE_TRACKING
          : WALLET_LIFECYCLE.BACKFILLING,
      tradeDone: false,
      fundingDone: false,
      tradeHasMore: true,
      fundingHasMore: true,
      tradeCursor: null,
      fundingCursor: null,
      backfillCompletedAt: null,
      remoteHistoryVerified: false,
      remoteHistoryVerifiedAt: null,
      retryPending: true,
      retryReason: reason,
      retryQueuedAt: now,
      forceHeadRefetch: forceHeadRefetchRequested || Boolean(current.forceHeadRefetch),
      forceHeadRefetchReason: forceHeadRefetchRequested
        ? reason
        : current && current.forceHeadRefetchReason
        ? current.forceHeadRefetchReason
        : null,
      forceHeadRefetchQueuedAt: forceHeadRefetchRequested
        ? now
        : Number((current && current.forceHeadRefetchQueuedAt) || 0) || null,
    };
    const finalizedState = this.finalizeWalletState(
      normalized,
      nextState,
      null,
      current.discoveredBy || "repair"
    );
    this.state.walletStates[normalized] = finalizedState;
    this.enqueueContinuationWallets([normalized], {
      reason: "history_audit_repair",
      front: true,
    });
    if (Number(nextState.liveTrackingSince || 0) > 0) {
      this.moveWalletToLiveGroup(normalized, now, { partial: true });
    }
    this.markStateDirty();
    return true;
  }

  runHistoryAuditRepairCycle(limit = this.historyAuditRepairBatchSize) {
    const wallets = this.collectHistoryAuditRepairCandidates(limit);
    const summary = {
      scanned: wallets.length,
      repairedLocal: 0,
      requeuedRemote: 0,
      failed: 0,
      at: Date.now(),
    };
    if (!wallets.length) {
      this.runtime.lastHistoryRepairSummary = summary;
      return summary;
    }

    wallets.forEach((wallet) => {
      const current = this.state.walletStates && this.state.walletStates[wallet];
      if (this.needsRemoteHistoryVerification(current) || this.needsPaginationStateRepair(current)) {
        if (
          this.queueWalletForAuditRepairFetch(
            wallet,
            current,
            "remote_history_verification_pending"
          )
        ) {
          summary.requeuedRemote += 1;
        } else {
          summary.failed += 1;
        }
        return;
      }
      if (!this.needsHistoryAuditRepair(current)) return;
      try {
        const filePath = this.walletHistoryPath(wallet);
        if (!fs.existsSync(filePath)) {
          if (this.queueWalletForAuditRepairFetch(wallet, current, "history_file_missing")) {
            summary.requeuedRemote += 1;
          }
          return;
        }
        const repairedHistory = this.repairWalletHistoryFile(wallet);
        if (!repairedHistory) {
          if (this.queueWalletForAuditRepairFetch(wallet, current, "history_file_invalid")) {
            summary.requeuedRemote += 1;
          } else {
            summary.failed += 1;
          }
          return;
        }
        const nextState = {
          ...current,
          wallet,
          tradeRowsLoaded: Number(
            repairedHistory.audit && repairedHistory.audit.tradesStored
              ? repairedHistory.audit.tradesStored
              : 0
          ),
          fundingRowsLoaded: Number(
            repairedHistory.audit && repairedHistory.audit.fundingStored
              ? repairedHistory.audit.fundingStored
              : 0
          ),
          retryPending:
            current && current.retryReason === "history_audit_missing"
              ? false
              : Boolean(current && current.retryPending),
          retryReason:
            current && current.retryReason === "history_audit_missing"
              ? null
              : current && current.retryReason
              ? current.retryReason
              : null,
          retryQueuedAt:
            current && current.retryReason === "history_audit_missing"
              ? null
              : Number((current && current.retryQueuedAt) || 0) || null,
          remoteHistoryVerified: Boolean(current && current.remoteHistoryVerified),
          remoteHistoryVerifiedAt:
            Number((current && current.remoteHistoryVerifiedAt) || 0) || null,
          tradePagination:
            repairedHistory &&
            repairedHistory.audit &&
            repairedHistory.audit.pagination &&
            repairedHistory.audit.pagination.trades
              ? { ...repairedHistory.audit.pagination.trades }
              : current && current.tradePagination && typeof current.tradePagination === "object"
              ? { ...current.tradePagination }
              : null,
          fundingPagination:
            repairedHistory &&
            repairedHistory.audit &&
            repairedHistory.audit.pagination &&
            repairedHistory.audit.pagination.funding
              ? { ...repairedHistory.audit.pagination.funding }
              : current && current.fundingPagination && typeof current.fundingPagination === "object"
              ? { ...current.fundingPagination }
              : null,
        };
        const tradePaginationSummary =
          nextState.tradePagination && typeof nextState.tradePagination === "object"
            ? buildHistoryEndpointPaginationSummary(nextState.tradePagination)
            : null;
        const fundingPaginationSummary =
          nextState.fundingPagination && typeof nextState.fundingPagination === "object"
            ? buildHistoryEndpointPaginationSummary(nextState.fundingPagination)
            : null;
        const tradeComplete = isHistoryEndpointPaginationComplete(tradePaginationSummary);
        const fundingComplete = isHistoryEndpointPaginationComplete(fundingPaginationSummary);
        const backfillComplete = tradeComplete && fundingComplete;
        nextState.tradeDone = tradeComplete;
        nextState.fundingDone = fundingComplete;
        nextState.tradeHasMore = !tradeComplete;
        nextState.fundingHasMore = !fundingComplete;
        nextState.tradeCursor = tradeComplete
          ? null
          : nextHistoryEndpointCursor(nextState.tradePagination, current && current.tradeCursor);
        nextState.fundingCursor = fundingComplete
          ? null
          : nextHistoryEndpointCursor(nextState.fundingPagination, current && current.fundingCursor);
        nextState.remoteHistoryVerified = backfillComplete;
        nextState.remoteHistoryVerifiedAt = backfillComplete
          ? Number(
              (current && current.remoteHistoryVerifiedAt) ||
                (current && current.backfillCompletedAt) ||
                Date.now()
            ) || Date.now()
          : null;
        nextState.backfillCompletedAt = backfillComplete
          ? Number(
              (current && current.backfillCompletedAt) ||
                nextState.remoteHistoryVerifiedAt ||
                Date.now()
            ) || Date.now()
          : null;
        nextState.lifecycleStage = backfillComplete
          ? Number((current && current.liveTrackingSince) || 0) > 0
            ? WALLET_LIFECYCLE.LIVE_TRACKING
            : WALLET_LIFECYCLE.FULLY_INDEXED
          : Number((current && current.liveTrackingSince) || 0) > 0
          ? WALLET_LIFECYCLE.LIVE_TRACKING
          : WALLET_LIFECYCLE.BACKFILLING;
        nextState.historyPhase = backfillComplete
          ? "complete"
          : String((current && current.historyPhase) || "deep").trim().toLowerCase() === "recent"
          ? "recent"
          : "deep";
        if (backfillComplete) {
          nextState.retryPending = false;
          nextState.retryReason = null;
          nextState.retryQueuedAt = null;
          nextState.forceHeadRefetch = false;
          nextState.forceHeadRefetchReason = null;
          nextState.forceHeadRefetchQueuedAt = null;
        }
        const finalizedState = this.finalizeWalletState(
          wallet,
          nextState,
          repairedHistory,
          current && current.discoveredBy ? current.discoveredBy : "repair"
        );
        this.state.walletStates[wallet] = finalizedState;
        if (this.walletStore && typeof this.walletStore.upsert === "function") {
          const repairedRecord = buildWalletRecordFromHistory({
            wallet,
            tradesHistory: repairedHistory.trades,
            fundingHistory: repairedHistory.funding,
            computedAt: Date.now(),
          });
          repairedRecord.storage = {
            ...(repairedRecord.storage && typeof repairedRecord.storage === "object"
              ? repairedRecord.storage
              : {}),
            ...this.buildWalletStorageRefs(wallet),
          };
          this.walletStore.upsert(repairedRecord, { deferFlush: true });
        }
        summary.repairedLocal += 1;
        this.markStateDirty();
      } catch (error) {
        summary.failed += 1;
        this.logger.warn(
          `[wallet-indexer] history audit repair failed wallet=${wallet} error=${toErrorMessage(error)}`
        );
      }
    });

    this.persistStateIfNeeded();
    this.runtime.lastHistoryRepairSummary = summary;
    return summary;
  }

  runLegacyRemoteVerificationMigration(limit = Infinity) {
    const max =
      Number.isFinite(Number(limit)) && Number(limit) > 0
        ? Math.max(1, Math.floor(Number(limit)))
        : Number.POSITIVE_INFINITY;
    const wallets = normalizeWallets(
      (this.state.knownWallets && this.state.knownWallets.length
        ? this.state.knownWallets
        : Object.keys(this.state.walletStates || {})) || []
    );
    const totalWallets = wallets.length;
    if (!totalWallets) {
      return {
        migrated: 0,
        totalWallets: 0,
        scanned: 0,
        cursor: 0,
        at: Date.now(),
      };
    }
    const finiteMax = Number.isFinite(max);
    const scanLimit = finiteMax
      ? Math.min(
          totalWallets,
          Math.max(max * 32, this.topCorrectionCohortSize * 4, 512)
        )
      : totalWallets;
    let cursor =
      Math.max(0, Number(this.runtime.legacyVerificationCursor || 0)) % totalWallets;
    let migrated = 0;
    let scanned = 0;
    while (scanned < scanLimit && (!finiteMax || migrated < max)) {
      const wallet = wallets[cursor];
      cursor = (cursor + 1) % totalWallets;
      scanned += 1;
      const row = this.state.walletStates && this.state.walletStates[wallet];
      if (!this.needsRemoteHistoryVerification(row)) continue;
      if (
        this.queueWalletForAuditRepairFetch(
          wallet,
          row,
          "remote_history_verification_pending"
        )
      ) {
        migrated += 1;
      }
    }
    this.runtime.legacyVerificationCursor = cursor;
    if (migrated > 0) {
      this.persistStateIfNeeded();
    }
    return {
      migrated,
      totalWallets,
      scanned,
      cursor,
      at: Date.now(),
    };
  }

  sanitizeRetryReasons() {
    let repaired = 0;
    Object.entries(this.state.walletStates || {}).forEach(([wallet, row]) => {
      if (!row || typeof row !== "object") return;
      const retryReason = String(row.retryReason || "").trim();
      const lastErrorReason = String(row.lastErrorReason || row.lastError || "").trim();
      const historyAuditReason = String(
        row.historyAudit && row.historyAudit.retryReason ? row.historyAudit.retryReason : ""
      ).trim();
      const hasBuggyReason =
        retryReason.toLowerCase().includes("prevbackfillcomplete") ||
        lastErrorReason.toLowerCase().includes("prevbackfillcomplete") ||
        historyAuditReason.toLowerCase().includes("prevbackfillcomplete");
      if (!hasBuggyReason) return;
      const needsRemote = this.needsRemoteHistoryVerification(row);
      const needsRepair = this.needsHistoryAuditRepair(row);
      const nextRetryPending = needsRemote || needsRepair;
      const nextRetryReason = nextRetryPending
        ? row.forceHeadRefetchReason || (needsRemote ? "remote_history_verification_pending" : "history_audit_repair_pending")
        : null;
      const nextState = {
        ...row,
        retryPending: nextRetryPending,
        retryReason: nextRetryReason,
        retryQueuedAt: nextRetryPending ? Number(row.retryQueuedAt || Date.now()) || Date.now() : null,
        lastError: hasBuggyReason ? null : row.lastError || null,
        lastErrorReason: hasBuggyReason ? null : row.lastErrorReason || null,
      };
      const finalizedState = this.finalizeWalletState(
        wallet,
        nextState,
        null,
        row && row.discoveredBy ? row.discoveredBy : "migration"
      );
      this.state.walletStates[wallet] = finalizedState;
      repaired += 1;
    });
    if (repaired > 0) {
      this.markStateDirty();
      this.persistStateIfNeeded(true);
    }
    this.runtime.lastRetryReasonSanitization = {
      at: Date.now(),
      repaired,
    };
    return this.runtime.lastRetryReasonSanitization;
  }

  mergeHistoryRepairSummaries(base = null, incoming = null) {
    const left = base && typeof base === "object" ? base : null;
    const right = incoming && typeof incoming === "object" ? incoming : null;
    if (!left) return right;
    if (!right) return left;
    return {
      scanned: Number(left.scanned || 0) + Number(right.scanned || 0),
      repairedLocal: Number(left.repairedLocal || 0) + Number(right.repairedLocal || 0),
      requeuedRemote: Number(left.requeuedRemote || 0) + Number(right.requeuedRemote || 0),
      failed: Number(left.failed || 0) + Number(right.failed || 0),
      at: Math.max(Number(left.at || 0), Number(right.at || 0)),
    };
  }

  isWalletLeaseActive(row, now = Date.now()) {
    const safe = row || {};
    return Number(safe.activeLeaseExpiresAt || 0) > now;
  }

  getTaskTimeoutMs(mode = "backfill") {
    const normalized = String(mode || "backfill").trim().toLowerCase();
    if (normalized === "activate") return this.activateTaskTimeoutMs;
    if (normalized === "recent") return this.recentTaskTimeoutMs;
    if (normalized === "live") return this.liveTaskTimeoutMs;
    return this.backfillTaskTimeoutMs;
  }

  deriveWalletLifecycle(row) {
    const safe = row || {};
    const explicit = String(safe.lifecycleStage || "").trim();
    const hasSuccess =
      Number(safe.lastSuccessAt || 0) > 0 || Number(safe.scansSucceeded || 0) > 0;
    const hasAttempts =
      hasSuccess ||
      Number(safe.scansFailed || 0) > 0 ||
      Number(safe.lastAttemptAt || safe.lastScannedAt || 0) > 0;
    const complete = this.isBackfillCompleteRow(safe);

    if (complete) {
      if (explicit === WALLET_LIFECYCLE.FULLY_INDEXED || explicit === WALLET_LIFECYCLE.LIVE_TRACKING) {
        return explicit;
      }
      return safe.liveTrackingSince ? WALLET_LIFECYCLE.LIVE_TRACKING : WALLET_LIFECYCLE.FULLY_INDEXED;
    }
    if (this.isLiveRefreshEligibleRow(safe)) {
      return WALLET_LIFECYCLE.LIVE_TRACKING;
    }
    if (hasAttempts) return WALLET_LIFECYCLE.BACKFILLING;
    if (explicit === WALLET_LIFECYCLE.DISCOVERED) return WALLET_LIFECYCLE.DISCOVERED;
    return WALLET_LIFECYCLE.PENDING_BACKFILL;
  }

  isLiveRefreshEligibleRow(row) {
    const safe = row || {};
    if (this.isBackfillCompleteRow(safe)) return true;
    if (String(safe.lifecycleStage || "").trim() === WALLET_LIFECYCLE.LIVE_TRACKING) {
      return true;
    }
    return Number(safe.liveTrackingSince || 0) > 0;
  }

  deriveWalletStatus(row) {
    const safe = row || {};
    const hasSuccess =
      Number(safe.lastSuccessAt || 0) > 0 || Number(safe.scansSucceeded || 0) > 0;
    const hasFailure = Number(safe.lastFailureAt || 0) > 0;
    const lastError = String(safe.lastError || "").trim();
    const lifecycle = this.deriveWalletLifecycle(safe);
    const hasMore = !this.isBackfillCompleteRow(safe);

    if (!hasSuccess) {
      return lastError ? "failed" : "pending";
    }

    if (hasFailure && Number(safe.lastFailureAt || 0) >= Number(safe.lastSuccessAt || 0)) {
      return "failed";
    }

    if (lastError && Number(safe.consecutiveFailures || 0) > 0) {
      return "failed";
    }

    if (hasMore || lifecycle === WALLET_LIFECYCLE.BACKFILLING) return "partial";
    return "indexed";
  }

  isWalletIndexed(wallet) {
    const normalized = normalizeAddress(wallet);
    if (!normalized) return false;

    const stateRow = this.state.walletStates && this.state.walletStates[normalized];
    return this.isBackfillCompleteRow(stateRow);
  }

  isWalletLiveTracking(wallet) {
    const normalized = normalizeAddress(wallet);
    if (!normalized) return false;
    const stateRow = this.state.walletStates && this.state.walletStates[normalized];
    return this.isLiveRefreshEligibleRow(stateRow);
  }

  isWalletBackfillComplete(wallet) {
    const normalized = normalizeAddress(wallet);
    if (!normalized) return false;
    const stateRow = this.state.walletStates && this.state.walletStates[normalized];
    return this.isBackfillCompleteRow(stateRow);
  }

  loadPersistedOnchainSnapshot(force = false) {
    const now = Date.now();
    if (
      !force &&
      this.runtime.onchainStateSnapshot &&
      now - Number(this.runtime.onchainStateSnapshotAt || 0) < 5000
    ) {
      return this.runtime.onchainStateSnapshot;
    }
    const payload = this.onchainStatePath ? readJson(this.onchainStatePath, null) : null;
    const programs =
      payload && payload.programs && typeof payload.programs === "object"
        ? Object.values(payload.programs)
        : [];
    const pendingTransactions =
      payload && payload.pendingTransactions && typeof payload.pendingTransactions === "object"
        ? Object.keys(payload.pendingTransactions).length
        : 0;
    const donePrograms = programs.filter((row) => row && row.done).length;
    const oldestReached = programs
      .map((row) => Number((row && row.firstBlockTimeMs) || 0))
      .filter((value) => Number.isFinite(value) && value > 0)
      .sort((a, b) => a - b)[0] || null;
    const latestReached = programs
      .map((row) => Number((row && row.lastBlockTimeMs) || 0))
      .filter((value) => Number.isFinite(value) && value > 0)
      .sort((a, b) => b - a)[0] || null;
    const startTimeMs = Number((payload && payload.startTimeMs) || 0) || null;
    const historyExhausted = programs.length > 0 && donePrograms === programs.length;
    const windowReached =
      Number.isFinite(startTimeMs) && Number.isFinite(oldestReached)
        ? oldestReached <= startTimeMs
        : null;
    const catchupComplete =
      String((payload && payload.mode) || "").trim().toLowerCase() === "live" ||
      (historyExhausted && pendingTransactions <= 0 && (windowReached === null || Boolean(windowReached)));
    const snapshot = {
      mode: payload && payload.mode ? payload.mode : null,
      startTimeMs,
      oldestReached,
      latestReached,
      pendingTransactions,
      historyExhausted,
      windowReached,
      catchupComplete,
    };
    this.runtime.onchainStateSnapshot = snapshot;
    this.runtime.onchainStateSnapshotAt = now;
    return snapshot;
  }

  isDiscoveryCatchupComplete() {
    if (!this.onchainDiscovery || typeof this.onchainDiscovery.getStatus !== "function") {
      const persisted = this.loadPersistedOnchainSnapshot();
      return Boolean(persisted && persisted.catchupComplete);
    }
    const status = this.onchainDiscovery.getStatus();
    const progress = status && status.progress && typeof status.progress === "object"
      ? status.progress
      : null;
    return (
      String((status && status.mode) || "").trim().toLowerCase() === "live" ||
      (progress &&
        Number(progress.pct || 0) >= 100 &&
        Number(progress.pendingTransactions || 0) <= 0 &&
        Boolean(progress.historyExhausted) &&
        (progress.windowReached === null || Boolean(progress.windowReached)))
    );
  }

  getRestShardAssignment(wallet, currentRow = null) {
    const normalizedWallet = normalizeAddress(wallet);
    if (!normalizedWallet || !this.clientPoolState.length) {
      return {
        shardId: null,
        shardIndex: null,
      };
    }

    const existingId = normalizeClientId(currentRow && currentRow.restShardId);
    const existingIndex = Number.isFinite(Number(currentRow && currentRow.restShardIndex))
      ? Number(currentRow.restShardIndex)
      : null;
    if (existingId && this.clientPoolById.has(existingId)) {
      const safeIndex =
        existingIndex !== null
          ? existingIndex
          : this.clientPoolState.findIndex((row) => row.id === existingId);
      return {
        shardId: existingId,
        shardIndex: safeIndex >= 0 ? safeIndex : 0,
      };
    }

    const assigned = assignShardByKey(
      this.restShardStrategy === "wallet_hash"
        ? `wallet:${normalizedWallet}`
        : normalizedWallet,
      this.clientPoolState.map((row, idx) => ({
        id: row.id,
        index: idx,
      }))
    );

    return {
      shardId:
        assigned && assigned.item && assigned.item.id
          ? assigned.item.id
          : this.clientPoolState[0].id,
      shardIndex: assigned ? assigned.index : 0,
    };
  }

  getClientEntryById(clientId) {
    const normalizedId = normalizeClientId(clientId);
    if (!normalizedId) return null;
    return this.clientPoolState.find((row) => row.id === normalizedId) || null;
  }

  normalizeWalletShardState(wallet, row = {}, source = "unknown") {
    const assignment = this.getRestShardAssignment(wallet, row);
    const fileAudit =
      row &&
      row.historyAudit &&
      row.historyAudit.file &&
      typeof row.historyAudit.file === "object"
        ? row.historyAudit.file
        : null;
    const truth = deriveWalletHistoryTruth(row, { fileAudit });
    const tradePagination = truth.tradePagination;
    const fundingPagination = truth.fundingPagination;
    const historyComplete = truth.backfillComplete;
    const nextRow = {
      ...row,
      wallet,
      discoveredBy: row.discoveredBy || source,
      discoveredAt: row.discoveredAt || Date.now(),
      tradePagination,
      fundingPagination,
      tradeRowsLoaded: Math.max(
        Number(row.tradeRowsLoaded || 0),
        Number(fileAudit && fileAudit.tradesStored !== undefined ? fileAudit.tradesStored : 0)
      ),
      fundingRowsLoaded: Math.max(
        Number(row.fundingRowsLoaded || 0),
        Number(fileAudit && fileAudit.fundingStored !== undefined ? fileAudit.fundingStored : 0)
      ),
      tradePagesLoaded: Math.max(
        Number(row.tradePagesLoaded || 0),
        Number(tradePagination && tradePagination.highestFetchedPage ? tradePagination.highestFetchedPage : 0)
      ),
      fundingPagesLoaded: Math.max(
        Number(row.fundingPagesLoaded || 0),
        Number(
          fundingPagination && fundingPagination.highestFetchedPage ? fundingPagination.highestFetchedPage : 0
        )
      ),
      tradeDone: truth.tradeDone,
      fundingDone: truth.fundingDone,
      tradeHasMore: truth.tradeHasMore,
      fundingHasMore: truth.fundingHasMore,
      tradeCursor: truth.tradeCursor,
      fundingCursor: truth.fundingCursor,
      remoteHistoryVerified: truth.remoteHistoryVerified,
      remoteHistoryVerifiedAt: truth.remoteHistoryVerified
        ? Number(
            row.remoteHistoryVerifiedAt ||
              row.backfillCompletedAt ||
              (fileAudit && fileAudit.persistedAt) ||
              row.updatedAt ||
              Date.now()
          ) || Date.now()
        : null,
      backfillCompletedAt: truth.backfillComplete
        ? Number(
            row.backfillCompletedAt ||
              row.remoteHistoryVerifiedAt ||
              (fileAudit && fileAudit.persistedAt) ||
              row.updatedAt ||
              Date.now()
          ) || Date.now()
        : null,
      retryPending: truth.retryPending,
      retryReason: truth.retryReason,
      retryQueuedAt: truth.retryPending ? Number(row.retryQueuedAt || 0) || null : null,
      forceHeadRefetch: truth.backfillComplete ? false : Boolean(row.forceHeadRefetch),
      forceHeadRefetchReason: truth.backfillComplete ? null : row.forceHeadRefetchReason || null,
      forceHeadRefetchQueuedAt: truth.backfillComplete
        ? null
        : Number(row.forceHeadRefetchQueuedAt || 0) || null,
      historyPhase: truth.backfillComplete
        ? "complete"
        : row.historyPhase ||
          (Number(row.liveTrackingSince || 0) > 0 || Number(row.scansSucceeded || 0) > 0
            ? "deep"
            : "activate"),
      lifecycleStage: truth.backfillComplete
        ? Number(row.liveTrackingSince || 0) > 0
          ? WALLET_LIFECYCLE.LIVE_TRACKING
          : WALLET_LIFECYCLE.FULLY_INDEXED
        : row.lifecycleStage,
      restShardId: assignment.shardId,
      restShardIndex: assignment.shardIndex,
      restShardAssignedAt: Number(row.restShardAssignedAt || 0) || Date.now(),
      restShardStrategy: this.restShardStrategy,
      activeLeaseClientId: row.activeLeaseClientId || null,
      activeLeaseStartedAt: Number(row.activeLeaseStartedAt || 0) || null,
      activeLeaseExpiresAt: Number(row.activeLeaseExpiresAt || 0) || null,
    };
    return nextRow;
  }

  finalizeWalletState(wallet, row = {}, history = null, source = "unknown") {
    const normalized = this.normalizeWalletShardState(wallet, row, source);
    normalized.historyAudit = this.buildWalletHistoryAudit(normalized, history);
    return normalized;
  }

  createAttemptToken(wallet, mode = "backfill", at = Date.now()) {
    return `${String(wallet || "").trim()}:${String(mode || "backfill")}:${Number(at || Date.now())}:${Math.random()
      .toString(36)
      .slice(2, 10)}`;
  }

  isCurrentAttemptToken(wallet, token) {
    const normalized = normalizeAddress(wallet);
    if (!normalized || !token) return false;
    const row = this.state.walletStates && this.state.walletStates[normalized];
    return Boolean(row && row.activeAttemptToken && row.activeAttemptToken === token);
  }

  buildWalletHistoryAudit(row = {}, history = null) {
    const safeRow = row && typeof row === "object" ? row : {};
    const fileAudit =
      history && history.audit && typeof history.audit === "object"
        ? history.audit
        : safeRow.historyAudit && safeRow.historyAudit.file && typeof safeRow.historyAudit.file === "object"
        ? safeRow.historyAudit.file
        : null;
    return buildWalletHistoryAudit(safeRow, { fileAudit });
  }

  markWalletRetryPending(wallet, reason, mode = "backfill", now = Date.now()) {
    const normalized = normalizeAddress(wallet);
    if (!normalized) return;
    const current = this.getOrInitWalletState(normalized, "retry");
    this.state.walletStates[normalized] = {
      ...current,
      retryPending: true,
      retryReason: String(reason || "").trim() || "unknown_retry_pending",
      retryQueuedAt: now,
      lastFailureMode: mode,
      historyAudit: this.buildWalletHistoryAudit({
        ...current,
        retryPending: true,
        retryReason: String(reason || "").trim() || "unknown_retry_pending",
        retryQueuedAt: now,
        lastFailureMode: mode,
      }),
    };
    this.markStateDirty();
  }

  removeFromPriorityQueue(wallet) {
    const normalized = normalizeAddress(wallet);
    if (!normalized) return;
    if (!this.runtime.priorityScanSet.has(normalized)) return;
    this.runtime.priorityScanSet.delete(normalized);
    this.runtime.priorityEnqueuedAt.delete(normalized);
    this.runtime.priorityScanQueue = this.runtime.priorityScanQueue.filter((item) => item !== normalized);
  }

  removeFromContinuationQueue(wallet) {
    const normalized = normalizeAddress(wallet);
    if (!normalized) return;
    if (!this.runtime.continuationScanSet.has(normalized)) return;
    this.runtime.continuationScanSet.delete(normalized);
    this.runtime.continuationEnqueuedAt.delete(normalized);
    this.runtime.continuationScanQueue = this.runtime.continuationScanQueue.filter(
      (item) => item !== normalized
    );
  }

  enqueueLiveWallets(wallets = []) {
    const normalized = this.filterOwnedWallets(wallets);
    if (!normalized.length) return 0;
    let queued = 0;
    normalized.forEach((wallet) => {
      if (!this.runtime.liveScanSet.has(wallet)) {
        this.runtime.liveScanSet.add(wallet);
        this.runtime.liveScanQueue.push(wallet);
        this.runtime.liveEnqueuedAt.set(wallet, Date.now());
        queued += 1;
      }
    });
    return queued;
  }

  moveWalletToLiveGroup(wallet, at = Date.now(), options = {}) {
    const normalized = normalizeAddress(wallet);
    if (!normalized) return false;
    if (!this.isWalletOwnedByWorker(normalized)) return false;
    const row = this.state.walletStates && this.state.walletStates[normalized];
    if (!row) return false;
    const partial = Boolean(options.partial);

    this.state.walletStates[normalized] = {
      ...row,
      lifecycleStage: WALLET_LIFECYCLE.LIVE_TRACKING,
      backfillCompletedAt: partial
        ? Number(row.backfillCompletedAt || 0) || null
        : Number(row.backfillCompletedAt || at),
      liveTrackingSince: Number(row.liveTrackingSince || at),
      liveLastScanAt: Number(row.liveLastScanAt || 0) || null,
    };

    const liveSet = new Set(normalizeWallets(this.state.liveWallets || []));
    if (!liveSet.has(normalized)) {
      liveSet.add(normalized);
      this.state.liveWallets = Array.from(liveSet.values());
    }

    this.removeFromPriorityQueue(normalized);
    if (!partial) this.removeFromContinuationQueue(normalized);
    this.enqueueLiveWallets([normalized]);
    return true;
  }

  reconcileLiveWalletSet() {
    const now = Date.now();
    const liveSet = new Set(this.filterOwnedWallets(this.state.liveWallets || []));
    let added = 0;
    Object.entries(this.state.walletStates || {}).forEach(([key, row]) => {
      if (!row || typeof row !== "object") return;
      const wallet = normalizeAddress(row.wallet || key);
      if (!wallet) return;
      if (!this.isWalletOwnedByWorker(wallet)) return;
      if (!this.isLiveRefreshEligibleRow(row)) return;
      if (!liveSet.has(wallet)) {
        liveSet.add(wallet);
        added += 1;
      }
      if (!this.runtime.liveScanSet.has(wallet)) {
        this.runtime.liveScanSet.add(wallet);
        this.runtime.liveScanQueue.push(wallet);
        this.runtime.liveEnqueuedAt.set(wallet, now);
      }
    });
    if (added > 0) {
      this.state.liveWallets = Array.from(liveSet.values());
      this.markStateDirty();
    }
    return added;
  }

  removeFromLiveGroup(wallet) {
    const normalized = normalizeAddress(wallet);
    if (!normalized) return;
    if (!this.isWalletOwnedByWorker(normalized)) return;
    this.state.liveWallets = normalizeWallets((this.state.liveWallets || []).filter((item) => item !== normalized));
    if (this.runtime.liveScanSet.has(normalized)) {
      this.runtime.liveScanSet.delete(normalized);
      this.runtime.liveEnqueuedAt.delete(normalized);
      this.runtime.liveScanQueue = this.runtime.liveScanQueue.filter((item) => item !== normalized);
    }
  }

  enqueuePriorityWallets(wallets = [], options = {}) {
    const normalized = this.filterOwnedWallets(wallets);
    if (!normalized.length) return 0;
    const includeLive = Boolean(options.includeLive);
    const reason = String(options.reason || "queue");
    const front = Boolean(options.front);

    let queued = 0;
    normalized.forEach((wallet) => {
      const row = this.state.walletStates && this.state.walletStates[wallet];
      const lifecycle = this.deriveWalletLifecycle(row);
      if (
        !includeLive &&
        lifecycle === WALLET_LIFECYCLE.LIVE_TRACKING &&
        this.isBackfillCompleteRow(row)
      ) {
        return;
      }
      if (this.isWalletIndexed(wallet) && !includeLive) return;
      if (this.runtime.priorityScanSet.has(wallet)) return;
      const current = this.getOrInitWalletState(wallet, "queue");
      if (lifecycle === WALLET_LIFECYCLE.DISCOVERED || lifecycle === WALLET_LIFECYCLE.PENDING_BACKFILL) {
        this.state.walletStates[wallet] = {
          ...current,
          lifecycleStage: WALLET_LIFECYCLE.PENDING_BACKFILL,
          queueReason: reason,
          queuedAt: Date.now(),
        };
      }
      this.runtime.priorityScanSet.add(wallet);
      if (front) this.runtime.priorityScanQueue.unshift(wallet);
      else this.runtime.priorityScanQueue.push(wallet);
      this.runtime.priorityEnqueuedAt.set(wallet, Date.now());
      queued += 1;
    });

    return queued;
  }

  enqueueContinuationWallets(wallets = [], options = {}) {
    const normalized = this.filterOwnedWallets(wallets);
    if (!normalized.length) return 0;
    const reason = String(options.reason || "continuation");
    const front = Boolean(options.front);

    let queued = 0;
    normalized.forEach((wallet) => {
      const row = this.state.walletStates && this.state.walletStates[wallet];
      if (this.isBackfillCompleteRow(row)) return;
      if (this.runtime.continuationScanSet.has(wallet)) return;

      const current = this.getOrInitWalletState(wallet, "continuation");
      this.state.walletStates[wallet] = {
        ...current,
        queueReason: reason,
      };
      this.runtime.continuationScanSet.add(wallet);
      if (front) this.runtime.continuationScanQueue.unshift(wallet);
      else this.runtime.continuationScanQueue.push(wallet);
      this.runtime.continuationEnqueuedAt.set(wallet, Date.now());
      queued += 1;
    });

    return queued;
  }

  summarizeContinuationDemand() {
    const summary = {
      total: 0,
      recent: 0,
      deep: 0,
    };
    this.runtime.continuationScanQueue.forEach((wallet) => {
      if (!wallet) return;
      const row = this.state.walletStates && this.state.walletStates[wallet];
      if (this.isBackfillCompleteRow(row)) return;
      summary.total += 1;
      const historyPhase = String((row && row.historyPhase) || "deep").trim().toLowerCase();
      if (historyPhase === "recent") summary.recent += 1;
      else summary.deep += 1;
    });
    return summary;
  }

  dequeueContinuationTask(selected = new Set(), now = Date.now(), preferredMode = null) {
    const queueLength = this.runtime.continuationScanQueue.length;
    if (queueLength <= 0) return null;
    const preferredModes = Array.isArray(preferredMode)
      ? new Set(preferredMode.filter(Boolean))
      : preferredMode
      ? new Set([preferredMode])
      : null;

    for (let i = 0; i < queueLength; i += 1) {
      const wallet = this.runtime.continuationScanQueue.shift();
      if (!wallet) continue;
      const row = this.state.walletStates && this.state.walletStates[wallet];
      if (this.isBackfillCompleteRow(row)) {
        this.runtime.continuationScanSet.delete(wallet);
        this.runtime.continuationEnqueuedAt.delete(wallet);
        continue;
      }
      if (selected.has(wallet) || this.isWalletLeaseActive(row, now)) {
        this.runtime.continuationScanQueue.push(wallet);
        continue;
      }
      const historyPhase = String((row && row.historyPhase) || "deep").trim().toLowerCase();
      const mode = historyPhase === "recent" ? "recent" : "backfill";
      if (preferredModes && !preferredModes.has(mode)) {
        this.runtime.continuationScanQueue.push(wallet);
        continue;
      }
      this.runtime.continuationScanSet.delete(wallet);
      this.runtime.continuationEnqueuedAt.delete(wallet);
      return {
        wallet,
        mode,
        reason: mode === "recent" ? "recent_backfill" : "deep_backfill",
      };
    }

    return null;
  }

  walletHistoryPath(wallet) {
    return path.join(this.walletHistoryDir, `${wallet}.json`);
  }

  getCachedWalletHistory(wallet) {
    const normalized = normalizeAddress(wallet);
    if (!normalized) return null;
    const cached = this.walletHistoryCache.get(normalized);
    if (!cached) return null;
    this.walletHistoryCache.delete(normalized);
    this.walletHistoryCache.set(normalized, cached);
    return cached;
  }

  setCachedWalletHistory(wallet, history) {
    const normalized = normalizeAddress(wallet);
    if (!normalized || !history || typeof history !== "object") return;
    if (this.walletHistoryCache.has(normalized)) {
      this.walletHistoryCache.delete(normalized);
    }
    this.walletHistoryCache.set(normalized, history);
    while (this.walletHistoryCache.size > this.walletHistoryCacheSize) {
      const oldestKey = this.walletHistoryCache.keys().next();
      if (oldestKey.done) break;
      this.walletHistoryCache.delete(oldestKey.value);
    }
  }

  loadWalletHistory(wallet) {
    ensureDir(this.walletHistoryDir);
    const cached = this.getCachedWalletHistory(wallet);
    if (cached) return cached;
    const filePath = this.walletHistoryPath(wallet);
    const loaded = readJson(filePath, null);
    if (!loaded || typeof loaded !== "object") {
      const empty = emptyWalletHistory(wallet);
      this.setCachedWalletHistory(wallet, empty);
      return empty;
    }
    const hasSeenIndexes =
      loaded.tradeSeenKeys &&
      typeof loaded.tradeSeenKeys === "object" &&
      loaded.fundingSeenKeys &&
      typeof loaded.fundingSeenKeys === "object";
    const needsRebuildSeenKeys =
      !hasSeenIndexes ||
      Object.keys(loaded.tradeSeenKeys || {}).length <
        (Array.isArray(loaded.trades) ? loaded.trades.length : 0) ||
      Object.keys(loaded.fundingSeenKeys || {}).length <
        (Array.isArray(loaded.funding) ? loaded.funding.length : 0);
    const normalized = normalizeWalletHistoryPayload(wallet, loaded, {
      rebuildSeenKeys: needsRebuildSeenKeys,
    });
    const needsRewrite =
      Number(loaded.version || 0) !== WALLET_HISTORY_VERSION ||
      !loaded.pagination ||
      !loaded.audit ||
      !loaded.audit.pagination ||
      needsRebuildSeenKeys;
    if (needsRewrite) {
      writeJsonAtomic(filePath, normalized);
    }
    this.setCachedWalletHistory(wallet, normalized);
    return normalized;
  }

  saveWalletHistory(wallet, history) {
    ensureDir(this.walletHistoryDir);
    const filePath = this.walletHistoryPath(wallet);
    const persistedAt = Date.now();
    const out = normalizeWalletHistoryPayload(wallet, history, {
      rebuildSeenKeys: false,
      persistedAt,
    });
    writeJsonAtomic(filePath, out);
    this.setCachedWalletHistory(wallet, out);
    return out;
  }

  trimPageCache(cache = {}) {
    const entries = Object.entries(cache);
    if (entries.length <= this.cacheEntriesPerEndpoint) return cache;
    const sorted = entries.sort((a, b) => {
      const aTs = Number(a[1] && a[1].touchedAt ? a[1].touchedAt : a[1].fetchedAt || 0);
      const bTs = Number(b[1] && b[1].touchedAt ? b[1].touchedAt : b[1].fetchedAt || 0);
      return bTs - aTs;
    });
    const next = {};
    sorted.slice(0, this.cacheEntriesPerEndpoint).forEach(([key, value]) => {
      next[key] = value;
    });
    return next;
  }

  getOrInitWalletState(wallet, source = "unknown") {
    const existing = this.state.walletStates[wallet] || {};
    const lifecycleStage = this.deriveWalletLifecycle(existing);
    const base = {
      ...existing,
      wallet,
      discoveredBy: existing.discoveredBy || source,
      discoveredAt: existing.discoveredAt || Date.now(),
      scansSucceeded: Number(existing.scansSucceeded || 0),
      scansFailed: Number(existing.scansFailed || 0),
      consecutiveFailures: Number(existing.consecutiveFailures || 0),
      tradeCursor: Object.prototype.hasOwnProperty.call(existing, "tradeCursor")
        ? existing.tradeCursor
        : null,
      fundingCursor: Object.prototype.hasOwnProperty.call(existing, "fundingCursor")
        ? existing.fundingCursor
        : null,
      tradeDone: Boolean(existing.tradeDone),
      fundingDone: Boolean(existing.fundingDone),
      tradeHasMore: Boolean(existing.tradeHasMore),
      fundingHasMore: Boolean(existing.fundingHasMore),
      historyPhase:
        existing.historyPhase ||
        (this.isBackfillCompleteRow(existing)
          ? "complete"
          : Number(existing.liveTrackingSince || 0) > 0 || Number(existing.scansSucceeded || 0) > 0
          ? "deep"
          : "activate"),
      lifecycleStage:
        lifecycleStage ||
        (source === "seed" || source === "store" || source === "onchain_bootstrap"
          ? WALLET_LIFECYCLE.PENDING_BACKFILL
          : WALLET_LIFECYCLE.DISCOVERED),
      backfillCompletedAt: Number(existing.backfillCompletedAt || 0) || null,
      liveTrackingSince: Number(existing.liveTrackingSince || 0) || null,
      liveLastScanAt: Number(existing.liveLastScanAt || 0) || null,
      liveHeadTradeKey: existing.liveHeadTradeKey || null,
      liveHeadFundingKey: existing.liveHeadFundingKey || null,
      retryPending: Boolean(existing.retryPending),
      retryReason: existing.retryReason || null,
      retryQueuedAt: Number(existing.retryQueuedAt || 0) || null,
      tradePagination:
        existing.tradePagination && typeof existing.tradePagination === "object"
          ? existing.tradePagination
          : null,
      fundingPagination:
        existing.fundingPagination && typeof existing.fundingPagination === "object"
          ? existing.fundingPagination
          : null,
      remoteHistoryVerified: Boolean(existing.remoteHistoryVerified),
      remoteHistoryVerifiedAt: Number(existing.remoteHistoryVerifiedAt || 0) || null,
      lastAttemptMode: existing.lastAttemptMode || null,
      lastSuccessMode: existing.lastSuccessMode || null,
      lastFailureMode: existing.lastFailureMode || null,
      activeAttemptToken: existing.activeAttemptToken || null,
      historyAudit:
        existing.historyAudit && typeof existing.historyAudit === "object"
          ? existing.historyAudit
          : null,
    };
    const merged = this.normalizeWalletShardState(wallet, base, source);
    this.state.walletStates[wallet] = merged;
    return merged;
  }

  refillPriorityFromBacklog(limit = this.backlogRefillBatch) {
    const rows = Object.values(this.state.walletStates || {});
    const candidates = rows
      .map((row) => ({
        wallet: row.wallet,
        status: this.deriveWalletStatus(row),
        lifecycle: this.deriveWalletLifecycle(row),
        discoveredAt: Number(row.discoveredAt || 0),
        lastAttemptAt: Number(row.lastAttemptAt || row.lastScannedAt || 0),
        neverAttempted: Number(row.lastAttemptAt || row.lastScannedAt || 0) <= 0,
      }))
      .filter((row) => row.wallet)
      .filter((row) => row.status !== "indexed")
      .filter(
        (row) =>
          row.lifecycle === WALLET_LIFECYCLE.DISCOVERED ||
          row.lifecycle === WALLET_LIFECYCLE.PENDING_BACKFILL
      )
      .sort((a, b) => {
        const pri = { failed: 0, pending: 1 };
        const ap = Object.prototype.hasOwnProperty.call(pri, a.status) ? pri[a.status] : 9;
        const bp = Object.prototype.hasOwnProperty.call(pri, b.status) ? pri[b.status] : 9;
        if (ap !== bp) return ap - bp;
        if (a.neverAttempted !== b.neverAttempted) {
          return a.neverAttempted ? -1 : 1;
        }
        if (a.neverAttempted && b.neverAttempted) {
          return b.discoveredAt - a.discoveredAt;
        }
        const aTs = a.lastAttemptAt || a.discoveredAt || 0;
        const bTs = b.lastAttemptAt || b.discoveredAt || 0;
        return aTs - bTs;
      })
      .slice(0, Math.max(1, Number(limit || this.backlogRefillBatch)));

    return this.enqueuePriorityWallets(candidates.map((row) => row.wallet));
  }

  refillContinuationFromBacklog(limit = this.backlogRefillBatch) {
    const rows = Object.values(this.state.walletStates || {});
    const candidates = rows
      .map((row) => ({
        wallet: row.wallet,
        status: this.deriveWalletStatus(row),
        lastAttemptAt: Number(row.lastAttemptAt || row.lastScannedAt || 0),
        liveTrackingSince: Number(row.liveTrackingSince || 0),
        priority: this.getWalletCorrectionPriority(row.wallet, row),
      }))
      .filter((row) => row.wallet)
      .filter((row) => row.status === "partial")
      .sort((a, b) => {
        const cmp = this.compareWalletCorrectionPriority(a.priority, b.priority);
        if (cmp !== 0) return cmp;
        const aTs = a.lastAttemptAt || a.liveTrackingSince || 0;
        const bTs = b.lastAttemptAt || b.liveTrackingSince || 0;
        return aTs - bTs;
      })
      .slice(0, Math.max(1, Number(limit || this.backlogRefillBatch)));

    return this.enqueueContinuationWallets(candidates.map((row) => row.wallet), {
      reason: "backlog_refill",
    });
  }

  evaluateBacklogMode(summary) {
    if (!this.backlogModeEnabled) {
      return {
        active: false,
        reason: null,
      };
    }
    const backlog = Number(summary && summary.backlog ? summary.backlog : 0);
    const avgWait = Number(summary && summary.averagePendingWaitMs ? summary.averagePendingWaitMs : 0);
    const byBacklog = backlog >= this.backlogWalletThreshold;
    const byWait = avgWait >= this.backlogAvgWaitMsThreshold;
    return {
      active: byBacklog || byWait,
      reason: byBacklog
        ? `backlog>${this.backlogWalletThreshold}`
        : byWait
        ? `avg_wait_ms>${this.backlogAvgWaitMsThreshold}`
        : null,
    };
  }

  computeActivationStrategy(clientPoolSummary = null, diagnosticsSummary = null) {
    const clientPool =
      clientPoolSummary && typeof clientPoolSummary === "object"
        ? clientPoolSummary
        : this.buildClientPoolSummary();
    const diagnostics =
      diagnosticsSummary && typeof diagnosticsSummary === "object"
        ? diagnosticsSummary
        : this.buildDiagnosticsSummary();
    const rpmCap = Number(clientPool.rpmCap || 0);
    const rpmUsed = Number(clientPool.rpmUsed || 0);
    const headroomRatio =
      rpmCap > 0 ? Math.max(0, Math.min(1, (rpmCap - rpmUsed) / rpmCap)) : 0;
    const discoveryComplete = this.isDiscoveryCatchupComplete();
    const queueDepth = Number(this.runtime.priorityScanQueue.length || 0);
    const activationDemand = Math.max(
      0,
      Number(
        discoveryComplete
          ? diagnostics.activationPending
          : Number(diagnostics.pendingBackfill || 0) +
              Number(diagnostics.backfilling || 0) +
              Number(diagnostics.failedBackfill || 0)
      )
    );
    const continuationQueueDepth = Number(this.runtime.continuationScanQueue.length || 0);
    const continuationPending = Math.max(0, Number(diagnostics.continuationPending || 0));
    const continuationCanSpendExtra =
      Math.max(continuationQueueDepth, continuationPending) > this.maxWalletsPerScan * 2 &&
      headroomRatio >= 0.6;
    const continuationCanSpendMore =
      Math.max(continuationQueueDepth, continuationPending) > this.maxWalletsPerScan * 4 &&
      headroomRatio >= 0.75;

    const highPressure = activationDemand > this.maxWalletsPerScan * 2;
    const veryHighPressure = activationDemand > this.maxWalletsPerScan * 4;
    const extremePressure = activationDemand > this.maxWalletsPerScan * 6;
    const sprintPressure = activationDemand > this.maxWalletsPerScan * 10;

    let reserveTarget =
      activationDemand > 0 ? Math.min(this.activationReserveMin, activationDemand) : 0;
    let tradeLimit = this.activationTradesPageLimit;
    let fundingLimit = this.activationFundingPageLimit;
    let continuationPageBudget = this.backfillPageBudgetWhenLivePressure;
    let shardParallelism = 1;
    let mode = "normal";

    if (highPressure) {
      reserveTarget = Math.max(
        reserveTarget,
        Math.ceil(this.maxWalletsPerScan * 0.5)
      );
      mode = "high";
    }

    if (veryHighPressure && headroomRatio >= 0.5) {
      reserveTarget = Math.max(
        reserveTarget,
        Math.ceil(this.maxWalletsPerScan * 0.62)
      );
      tradeLimit = Math.min(tradeLimit, 64);
      fundingLimit = Math.min(fundingLimit, 64);
      continuationPageBudget = Math.min(
        continuationPageBudget,
        continuationCanSpendExtra ? 2 : 1
      );
      shardParallelism = Math.max(
        shardParallelism,
        Math.min(this.restShardParallelismMax, headroomRatio >= 0.82 ? 8 : headroomRatio >= 0.7 ? 6 : 4)
      );
      mode = "very_high";
    }

    if (extremePressure && headroomRatio >= 0.5) {
      reserveTarget = Math.max(
        reserveTarget,
        Math.ceil(this.maxWalletsPerScan * 0.82)
      );
      tradeLimit = Math.min(tradeLimit, 32);
      fundingLimit = Math.min(fundingLimit, 32);
      continuationPageBudget = continuationCanSpendExtra ? 2 : 1;
      shardParallelism = Math.max(
        shardParallelism,
        Math.min(
          this.restShardParallelismMax,
          headroomRatio >= 0.9 ? 10 : headroomRatio >= 0.8 ? 8 : headroomRatio >= 0.7 ? 6 : 4
        )
      );
      mode = "extreme";
    }

    if (sprintPressure && headroomRatio >= 0.7) {
      reserveTarget = Math.max(
        reserveTarget,
        Math.ceil(this.maxWalletsPerScan * 0.9)
      );
      tradeLimit = Math.min(tradeLimit, 20);
      fundingLimit = Math.min(fundingLimit, 20);
      continuationPageBudget = continuationCanSpendMore ? 2 : 1;
      shardParallelism = Math.max(
        shardParallelism,
        Math.min(
          this.restShardParallelismMax,
          headroomRatio >= 0.95 ? 14 : headroomRatio >= 0.9 ? 12 : headroomRatio >= 0.82 ? 10 : headroomRatio >= 0.74 ? 8 : 6
        )
      );
      mode = "sprint";
    }

    if (activationDemand <= this.maxWalletsPerScan && headroomRatio >= 0.65) {
      continuationPageBudget = Math.max(
        continuationPageBudget,
        continuationCanSpendMore ? 4 : continuationCanSpendExtra ? 3 : continuationPageBudget
      );
      shardParallelism = Math.max(
        shardParallelism,
        Math.min(
          this.restShardParallelismMax,
          headroomRatio >= 0.95 ? 14 : headroomRatio >= 0.9 ? 12 : headroomRatio >= 0.82 ? 10 : headroomRatio >= 0.74 ? 8 : 6
        )
      );
      mode = mode === "normal" ? "continuation_bias" : mode;
    }

    if (discoveryComplete) {
      reserveTarget =
        activationDemand > 0
          ? Math.min(
              Math.max(16, Math.ceil(this.maxWalletsPerScan * 0.18)),
              Math.min(this.activationReserveMax, activationDemand)
            )
          : 0;
      tradeLimit = Math.min(tradeLimit, 24);
      fundingLimit = Math.min(fundingLimit, 24);
      continuationPageBudget = Math.max(
        continuationPageBudget,
        headroomRatio >= 0.88 ? 5 : headroomRatio >= 0.74 ? 4 : 3
      );
      shardParallelism = Math.max(
        shardParallelism,
        Math.min(
          this.restShardParallelismMax,
          headroomRatio >= 0.98
            ? 64
            : headroomRatio >= 0.95
            ? 48
            : headroomRatio >= 0.9
            ? 40
            : headroomRatio >= 0.82
            ? 32
            : headroomRatio >= 0.72
            ? 24
            : 16
        )
      );
      mode =
        continuationPending > activationDemand
          ? "history_catchup"
          : activationDemand > 0
          ? "history_catchup_hybrid"
          : "live_maintenance";
    }

    reserveTarget =
      activationDemand > 0
        ? Math.min(
            Math.max(1, reserveTarget),
            Math.min(this.activationReserveMax, activationDemand)
          )
        : 0;

    return {
      mode,
      queueDepth,
      discoveryComplete,
      activationDemand,
      continuationPending,
      headroomRatio: Number(headroomRatio.toFixed(4)),
      reserveTarget,
      tradeLimit,
      fundingLimit,
      continuationPageBudget,
      continuationQueueDepth,
      shardParallelism,
    };
  }

  computeBackfillLanePlan(totalBackfillCapacity, activationStrategy, continuationDemand = null) {
    const capacity = Math.max(0, Number(totalBackfillCapacity || 0));
    const queueDepth = Number(this.runtime.priorityScanQueue.length || 0);
    const activationDemand = Math.max(
      0,
      Number(
        activationStrategy && activationStrategy.activationDemand !== undefined
          ? activationStrategy.activationDemand
          : queueDepth
      )
    );
    const demand =
      continuationDemand && typeof continuationDemand === "object"
        ? continuationDemand
        : this.summarizeContinuationDemand();
    const continuationQueueDepth = Number(demand.total || 0);
    const recentDemand = Number(demand.recent || 0);
    const deepDemand = Number(demand.deep || 0);
    const headroomRatio = Number(
      activationStrategy && activationStrategy.headroomRatio !== undefined
        ? activationStrategy.headroomRatio
        : 0
    );
    const discoveryComplete = Boolean(
      activationStrategy && activationStrategy.discoveryComplete
    );

    let continuationReserve = 0;
    if (capacity > 0 && continuationQueueDepth > 0) {
      continuationReserve = Math.max(
        this.continuationReserveMin,
        Math.ceil(capacity * 0.22)
      );
      if (activationDemand > this.maxWalletsPerScan * 2) {
        continuationReserve = Math.max(continuationReserve, Math.ceil(capacity * 0.28));
      }
      if (activationDemand > this.maxWalletsPerScan * 6 && headroomRatio >= 0.4) {
        continuationReserve = Math.max(continuationReserve, Math.ceil(capacity * 0.34));
      }
      if (activationDemand > this.maxWalletsPerScan * 12 && headroomRatio >= 0.5) {
        continuationReserve = Math.max(continuationReserve, Math.ceil(capacity * 0.4));
      }
      if (activationDemand > this.maxWalletsPerScan * 20 && headroomRatio >= 0.65) {
        continuationReserve = Math.max(continuationReserve, Math.ceil(capacity * 0.45));
      }
      if (activationDemand <= Math.ceil(capacity * 0.25) && headroomRatio >= 0.55) {
        continuationReserve = Math.max(continuationReserve, Math.ceil(capacity * 0.58));
      }
      if (
        activationDemand <= Math.ceil(capacity * 0.12) &&
        continuationQueueDepth > capacity * 10 &&
        headroomRatio >= 0.7
      ) {
        continuationReserve = Math.max(continuationReserve, Math.ceil(capacity * 0.7));
      }
      if (discoveryComplete) {
        continuationReserve = Math.max(
          continuationReserve,
          Math.ceil(capacity * (activationDemand > Math.ceil(capacity * 0.3) ? 0.52 : 0.62))
        );
        if (headroomRatio >= 0.8) {
          continuationReserve = Math.max(continuationReserve, Math.ceil(capacity * 0.68));
        }
      }
      continuationReserve = Math.min(
        capacity,
        continuationQueueDepth,
        this.continuationReserveMax
      );
    }

    const activationCapacity = Math.min(
      activationDemand,
      Math.max(0, capacity - continuationReserve)
    );
    let continuationCapacity = Math.min(
      continuationQueueDepth,
      Math.max(0, capacity - activationCapacity)
    );

    const deepFloor =
      continuationCapacity > 0 && deepDemand > 0
        ? Math.min(
            deepDemand,
            Math.max(
              1,
              Math.min(
                continuationCapacity,
                discoveryComplete
                  ? Math.max(this.deepBackfillMinPerScan, Math.ceil(continuationCapacity * 0.18))
                  : this.deepBackfillMinPerScan
              )
            )
          )
        : 0;
    let recentCapacity =
      continuationCapacity > 0 && recentDemand > 0
        ? Math.min(recentDemand, Math.max(0, continuationCapacity - deepFloor))
        : 0;
    let deepCapacity =
      continuationCapacity > 0 && deepDemand > 0
        ? Math.min(deepDemand, Math.max(deepFloor, continuationCapacity - recentCapacity))
        : 0;

    if (recentCapacity + deepCapacity < continuationCapacity) {
      const remaining = continuationCapacity - recentCapacity - deepCapacity;
      if (recentDemand > recentCapacity) {
        const addRecent = Math.min(remaining, recentDemand - recentCapacity);
        recentCapacity += addRecent;
      }
      const stillRemaining = continuationCapacity - recentCapacity - deepCapacity;
      if (stillRemaining > 0 && deepDemand > deepCapacity) {
        deepCapacity += Math.min(stillRemaining, deepDemand - deepCapacity);
      }
    }

    continuationCapacity = recentCapacity + deepCapacity;

    return {
      totalBackfillCapacity: capacity,
      activationCapacity,
      continuationCapacity,
      continuationReserve,
      discoveryComplete,
      recentCapacity,
      deepCapacity,
      continuationDemand: continuationQueueDepth,
      recentDemand,
      deepDemand,
    };
  }

  load() {
    ensureDir(this.dataDir);
    ensureDir(this.walletHistoryDir);
    const loaded = readJson(this.statePath, null);
    const loadedKnownCount = Array.isArray(loaded && loaded.knownWallets) ? loaded.knownWallets.length : 0;
    const loadedLiveCount = Array.isArray(loaded && loaded.liveWallets) ? loaded.liveWallets.length : 0;
    const loadedPriorityCount = Array.isArray(loaded && loaded.priorityQueue)
      ? loaded.priorityQueue.length
      : 0;
    const loadedContinuationCount = Array.isArray(loaded && loaded.continuationQueue)
      ? loaded.continuationQueue.length
      : 0;
    const loadedStateCount =
      loaded && loaded.walletStates && typeof loaded.walletStates === "object"
        ? Object.keys(loaded.walletStates).length
        : 0;
    if (
      loaded &&
      (loaded.version === STATE_VERSION ||
        loaded.version === 4 ||
        loaded.version === 1 ||
        loaded.version === 2 ||
        loaded.version === 3)
    ) {
      this.state = {
        ...emptyState(),
        ...loaded,
        version: STATE_VERSION,
        knownWallets: this.filterOwnedWallets(loaded.knownWallets || []),
        liveWallets: this.filterOwnedWallets(loaded.liveWallets || []),
        priorityQueue: this.filterOwnedWallets(loaded.priorityQueue || []),
        continuationQueue: this.filterOwnedWallets(loaded.continuationQueue || []),
        walletStates:
          loaded.walletStates && typeof loaded.walletStates === "object"
            ? loaded.walletStates
            : {},
      };
    } else {
      this.state = emptyState();
    }

    const normalizedWalletStates = {};
    Object.entries(this.state.walletStates || {}).forEach(([key, row]) => {
      if (!row || typeof row !== "object") return;
      const wallet = normalizeAddress(row.wallet || key);
      if (!wallet) return;
      if (!this.isWalletOwnedByWorker(wallet)) return;
      normalizedWalletStates[wallet] = this.normalizeWalletShardState(wallet, row, row.discoveredBy || "state");
    });
    this.state.walletStates = normalizedWalletStates;
    const stateNormalized =
      loadedKnownCount !== this.state.knownWallets.length ||
      loadedLiveCount !== this.state.liveWallets.length ||
      loadedPriorityCount !== this.state.priorityQueue.length ||
      loadedContinuationCount !== this.state.continuationQueue.length ||
      loadedStateCount !== Object.keys(this.state.walletStates || {}).length;

    this.addWallets(this.seedWallets, "seed");

    if (this.walletStore) {
      if (
        this.workerShardCount > 1 &&
        typeof this.walletStore.prune === "function"
      ) {
        this.walletStore.prune((_row, wallet) => this.isWalletOwnedByWorker(wallet));
      }
      this.addWallets(
        this.walletStore.list().map((row) => row.wallet),
        "store"
      );
    }

    if (this.onchainDiscovery && typeof this.onchainDiscovery.load === "function") {
      this.onchainDiscovery.load();
      const discovered = this.onchainDiscovery
        .listWallets({ confidenceMin: 0.8 })
        .map((row) => row.wallet);
      this.addWallets(discovered, "onchain_bootstrap");
    }

    if (stateNormalized) {
      this.markStateDirty();
      this.persistStateIfNeeded(true);
    }

    this.syncDepositRegistryWallets(true);

    const now = Date.now();
    const restoredQueue = this.filterOwnedWallets(this.state.priorityQueue || []);
    const restoredContinuationQueue = this.filterOwnedWallets(this.state.continuationQueue || []);
    const liveQueued = this.filterOwnedWallets(this.runtime.priorityScanQueue || []);
    this.runtime.priorityScanQueue = this.filterOwnedWallets([...restoredQueue, ...liveQueued]);
    this.runtime.priorityScanSet = new Set(this.runtime.priorityScanQueue);
    this.runtime.priorityEnqueuedAt = new Map(
      this.runtime.priorityScanQueue.map((wallet) => [wallet, now])
    );
    this.runtime.continuationScanQueue = restoredContinuationQueue.slice();
    this.runtime.continuationScanSet = new Set(this.runtime.continuationScanQueue);
    this.runtime.continuationEnqueuedAt = new Map(
      this.runtime.continuationScanQueue.map((wallet) => [wallet, now])
    );

    // Restore live-tracking wallets in a single pass.
    // This includes partially backfilled wallets that already entered live maintenance.
    // Avoid per-wallet queue/set rebuilds at startup (O(n^2) for large states).
    const restoredLiveSet = new Set(this.filterOwnedWallets(this.state.liveWallets || []));
    Object.entries(this.state.walletStates || {}).forEach(([key, row]) => {
      if (!row || typeof row !== "object") return;
      const normalizedWallet = normalizeAddress(row.wallet || key);
      if (!normalizedWallet) return;
      if (!this.isWalletOwnedByWorker(normalizedWallet)) return;
      if (!this.isLiveRefreshEligibleRow(row)) return;
      const complete = this.isBackfillCompleteRow(row);

      const successAt = Number(row.lastSuccessAt || now) || now;
      this.state.walletStates[normalizedWallet] = {
        ...row,
        wallet: normalizedWallet,
        lifecycleStage: WALLET_LIFECYCLE.LIVE_TRACKING,
        backfillCompletedAt: complete
          ? Number(row.backfillCompletedAt || successAt)
          : Number(row.backfillCompletedAt || 0) || null,
        liveTrackingSince: Number(row.liveTrackingSince || successAt),
        liveLastScanAt: Number(row.liveLastScanAt || 0) || null,
      };
      if (normalizedWallet !== key) {
        delete this.state.walletStates[key];
      }
      restoredLiveSet.add(normalizedWallet);
    });

    this.state.liveWallets = Array.from(restoredLiveSet.values());
    this.runtime.liveScanQueue = this.state.liveWallets.slice();
    this.runtime.liveScanSet = new Set(this.runtime.liveScanQueue);
    this.runtime.liveEnqueuedAt = new Map(
      this.runtime.liveScanQueue.map((wallet) => [wallet, now])
    );
    this.reconcileLiveWalletSet();

    if (this.runtime.priorityScanQueue.length > 0 && this.runtime.liveScanSet.size > 0) {
      const beforeLen = this.runtime.priorityScanQueue.length;
      this.runtime.priorityScanQueue = this.runtime.priorityScanQueue.filter(
        (wallet) =>
          !(
            this.runtime.liveScanSet.has(wallet) &&
            this.isWalletBackfillComplete(wallet)
          )
      );
      if (this.runtime.priorityScanQueue.length !== beforeLen) {
        this.runtime.priorityScanSet = new Set(this.runtime.priorityScanQueue);
        this.runtime.priorityEnqueuedAt = new Map(
          this.runtime.priorityScanQueue.map((wallet) => [wallet, now])
        );
      }
    }
    if (this.runtime.continuationScanQueue.length > 0) {
      const beforeLen = this.runtime.continuationScanQueue.length;
      this.runtime.continuationScanQueue = this.runtime.continuationScanQueue.filter((wallet) => {
        const row = this.state.walletStates && this.state.walletStates[wallet];
        return !this.isBackfillCompleteRow(row);
      });
      if (this.runtime.continuationScanQueue.length !== beforeLen) {
        this.runtime.continuationScanSet = new Set(this.runtime.continuationScanQueue);
        this.runtime.continuationEnqueuedAt = new Map(
          this.runtime.continuationScanQueue.map((wallet) => [wallet, now])
        );
      }
    }

    if (this.runtime.priorityScanQueue.length > 0) {
      const activation = [];
      const continuation = [];
      this.runtime.priorityScanQueue.forEach((wallet) => {
        const row = this.state.walletStates && this.state.walletStates[wallet];
        const lifecycle = this.deriveWalletLifecycle(row);
        const shouldContinue =
          this.isBackfillCompleteRow(row) ||
          String((row && row.historyPhase) || "") === "recent" ||
          String((row && row.historyPhase) || "") === "deep" ||
          (lifecycle === WALLET_LIFECYCLE.LIVE_TRACKING &&
            !this.isBackfillCompleteRow(row)) ||
          Number((row && row.scansSucceeded) || 0) > 0;
        if (shouldContinue) continuation.push(wallet);
        else activation.push(wallet);
      });
      this.runtime.priorityScanQueue = normalizeWallets(activation);
      this.runtime.priorityScanSet = new Set(this.runtime.priorityScanQueue);
      this.runtime.priorityEnqueuedAt = new Map(
        this.runtime.priorityScanQueue.map((wallet) => [wallet, now])
      );
      if (continuation.length) {
        this.runtime.continuationScanQueue = this.filterOwnedWallets([
          ...this.runtime.continuationScanQueue,
          ...continuation,
        ]);
        this.runtime.continuationScanSet = new Set(this.runtime.continuationScanQueue);
        this.runtime.continuationEnqueuedAt = new Map(
          this.runtime.continuationScanQueue.map((wallet) => [wallet, now])
        );
      }
    }

    if (this.runtime.priorityScanQueue.length > 0) {
      this.logger.info(
        `[wallet-indexer] startup queue resumed=${this.runtime.priorityScanQueue.length} source=state.priorityQueue`
      );
    } else {
      const startupActivationQueued = this.refillPriorityFromBacklog(this.backlogRefillBatch);
      const startupContinuationQueued = this.refillContinuationFromBacklog(this.backlogRefillBatch);
      if (startupActivationQueued > 0 || startupContinuationQueued > 0) {
        this.logger.info(
          `[wallet-indexer] startup backlog queued_activation=${startupActivationQueued} queued_continuation=${startupContinuationQueued} source=deposit_registry_or_known_wallets`
        );
      }
    }

    this.save();
    return this.state;
  }

  syncDepositRegistryWallets(force = false) {
    let updatedAt = 0;
    try {
      updatedAt = Number(fs.statSync(this.depositWalletsPath).mtimeMs || 0) || 0;
    } catch (_error) {
      updatedAt = 0;
    }

    if (
      !force &&
      updatedAt > 0 &&
      updatedAt <= Number(this.runtime.depositRegistryUpdatedAt || 0)
    ) {
      return {
        added: 0,
        total: this.state.knownWallets.length,
        updatedAt,
      };
    }

    const depositRegistry = readJson(this.depositWalletsPath, null);
    const depositWallets = Array.isArray(depositRegistry)
      ? depositRegistry
      : depositRegistry && Array.isArray(depositRegistry.wallets)
      ? depositRegistry.wallets
      : [];
    const added = depositWallets.length
      ? this.addWallets(depositWallets, "deposit_registry")
      : 0;
    this.runtime.depositRegistryUpdatedAt = updatedAt;
    return {
      added,
      total: this.state.knownWallets.length,
      updatedAt,
    };
  }

  save() {
    this.state.priorityQueue = Array.isArray(this.runtime.priorityScanQueue)
      ? this.runtime.priorityScanQueue.slice()
      : [];
    this.state.continuationQueue = Array.isArray(this.runtime.continuationScanQueue)
      ? this.runtime.continuationScanQueue.slice()
      : [];
    this.state.liveWallets = Array.isArray(this.state.liveWallets)
      ? normalizeWallets(this.state.liveWallets)
      : [];
    writeJsonAtomic(this.statePath, this.state);
    this.runtime.stateDirty = false;
    this.runtime.lastStateSaveAt = Date.now();
  }

  markStateDirty() {
    this.runtime.stateDirty = true;
  }

  persistStateIfNeeded(force = false) {
    if (!force && !this.runtime.stateDirty) return false;
    const now = Date.now();
    if (
      !force &&
      Number(this.runtime.lastStateSaveAt || 0) > 0 &&
      now - Number(this.runtime.lastStateSaveAt || 0) < this.stateSaveMinIntervalMs
    ) {
      return false;
    }
    this.save();
    return true;
  }

  resetIndexingState(options = {}) {
    if (this.runtime.inScan || this.runtime.inDiscovery) {
      throw new Error("indexer is busy; retry reset after current cycle completes");
    }

    const preserveKnownWallets = options.preserveKnownWallets !== false;
    const resetWalletStore = options.resetWalletStore !== false;
    const clearHistoryFiles = options.clearHistoryFiles !== false;

    const knownWallets = preserveKnownWallets ? normalizeWallets(this.state.knownWallets) : [];
    const knownCountBefore = Number(this.state.knownWallets.length || 0);
    const historyFileCountBefore = fs.existsSync(this.walletHistoryDir)
      ? fs
          .readdirSync(this.walletHistoryDir)
          .filter((name) => String(name || "").toLowerCase().endsWith(".json")).length
      : 0;

    this.runtime.priorityScanQueue = [];
    this.runtime.priorityScanSet = new Set();
    this.runtime.priorityEnqueuedAt = new Map();
    this.runtime.continuationScanQueue = [];
    this.runtime.continuationScanSet = new Set();
    this.runtime.continuationEnqueuedAt = new Map();
    this.runtime.liveScanQueue = [];
    this.runtime.liveScanSet = new Set();
    this.runtime.liveEnqueuedAt = new Map();
    this.runtime.scanConcurrencyCurrent = this.walletScanConcurrency;
    this.runtime.scanPagesCurrent = this.maxPagesPerWallet;
    this.runtime.lastError = null;

    if (clearHistoryFiles) {
      fs.rmSync(this.walletHistoryDir, { recursive: true, force: true });
      ensureDir(this.walletHistoryDir);
    }

    if (resetWalletStore && this.walletStore && typeof this.walletStore.reset === "function") {
      this.walletStore.reset();
    }

    this.state = {
      ...emptyState(),
      version: STATE_VERSION,
      knownWallets,
      walletStates: {},
      continuationQueue: [],
      scanCursor: 0,
      liveScanCursor: 0,
      liveWallets: [],
      lastDiscoveryAt: this.state.lastDiscoveryAt || null,
      discoveryCycles: this.state.discoveryCycles || 0,
      scanCycles: 0,
    };

    knownWallets.forEach((wallet) => {
      this.getOrInitWalletState(wallet, "reset");
    });
    const queued = this.enqueuePriorityWallets(knownWallets);
    this.save();

    return {
      ok: true,
      preserveKnownWallets,
      resetWalletStore,
      clearHistoryFiles,
      knownWalletsBefore: knownCountBefore,
      knownWalletsAfter: knownWallets.length,
      historyFilesBefore: historyFileCountBefore,
      queuedWallets: queued,
      resetAt: Date.now(),
    };
  }

  addWallets(wallets = [], source = "unknown") {
    const normalized = this.filterOwnedWallets(wallets);
    if (!normalized.length) return 0;

    const knownSet = new Set(this.state.knownWallets);
    let added = 0;

    normalized.forEach((wallet) => {
      const isNewWallet = !knownSet.has(wallet);
      if (isNewWallet) {
        knownSet.add(wallet);
        added += 1;
      }

      this.getOrInitWalletState(wallet, source);
      if (
        this.walletStore &&
        typeof this.walletStore.get === "function" &&
        typeof this.walletStore.upsert === "function" &&
        !this.walletStore.get(wallet)
      ) {
        const seededRecord = buildWalletRecordFromHistory({
          wallet,
          tradesHistory: [],
          fundingHistory: [],
          computedAt: Date.now(),
        });
        seededRecord.storage = {
          ...(seededRecord.storage && typeof seededRecord.storage === "object"
            ? seededRecord.storage
            : {}),
          ...this.buildWalletStorageRefs(wallet),
        };
        this.walletStore.upsert(seededRecord, { deferFlush: true });
      }

      if (!this.isWalletIndexed(wallet)) {
        this.enqueuePriorityWallets([wallet], {
          reason: isNewWallet ? "new_wallet" : "known_wallet",
          front: isNewWallet,
        });
      }
    });

    if (added > 0) {
      this.state.knownWallets = Array.from(knownSet.values());
    }

    return added;
  }

  async discoverWallets() {
    if (this.runtime.inDiscovery) {
      return {
        added: 0,
        total: this.state.knownWallets.length,
      };
    }

    this.runtime.inDiscovery = true;

    let added = 0;
    let onchain = null;

    try {
      added += this.syncDepositRegistryWallets().added;

      if (this.walletSource && typeof this.walletSource.fetchWallets === "function") {
        const wallets = await this.walletSource.fetchWallets();
        added += this.addWallets(wallets, "source");
      }

      if (
        this.onchainDiscovery &&
        typeof this.onchainDiscovery.discoverStep === "function"
      ) {
        const summaryBefore = this.buildDiagnosticsSummary();
        const backlogMode = this.evaluateBacklogMode(summaryBefore);
        this.runtime.backlogMode = backlogMode.active;
        this.runtime.backlogReason = backlogMode.reason;

        const cycle = Number(this.state.discoveryCycles || 0);
        const shouldThrottleDiscovery =
          backlogMode.active &&
          cycle % this.backlogDiscoverEveryCycles !== 0;
        const onchainPages = shouldThrottleDiscovery
          ? 0
          : Math.max(
              1,
              Math.min(this.onchainPagesMaxPerCycle, Number(this.runtime.onchainPagesCurrent || 1))
            );

        if (backlogMode.active) {
          const refilled = this.refillPriorityFromBacklog();
          this.logger.info(
            `[wallet-indexer] backlog_mode=on reason=${backlogMode.reason} backlog=${summaryBefore.backlog} avg_pending_wait_ms=${summaryBefore.averagePendingWaitMs} onchain_pages=${onchainPages} refilled_priority=${refilled}`
          );
        }

        if (onchainPages > 0) {
          this.logger.info(
            `[wallet-indexer] onchain step start pages=${onchainPages} known_wallets=${this.state.knownWallets.length} priority_queue=${this.runtime.priorityScanQueue.length}`
          );
          onchain = await this.onchainDiscovery.discoverStep({
            pages: onchainPages,
            validateLimit: this.onchainValidatePerCycle,
          });
        } else {
          onchain = {
            ok: true,
            skipped: true,
            reason: "backlog_mode_backpressure",
            scannedPrograms: 0,
            newWallets: [],
            pendingTransactions: null,
            progress: null,
          };
        }

        if (!onchain || onchain.ok !== true) {
          this.logger.warn(
            `[wallet-indexer] onchain step failed: ${
              onchain && onchain.error ? onchain.error : "unknown_error"
            }`
          );
        } else {
          const progressPct =
            onchain.progress && onchain.progress.pct !== null && onchain.progress.pct !== undefined
              ? onchain.progress.pct
              : "n/a";
          this.logger.info(
            `[wallet-indexer] onchain step ok scanned_programs=${onchain.scannedPrograms || 0} new_wallets=${(onchain.newWallets || []).length} pending_tx=${onchain.pendingTransactions || 0} progress_pct=${progressPct}`
          );
        }

        const rpc429 = Number(onchain && onchain.rpc ? onchain.rpc.rate429PerMin || 0 : 0);
        const now = Date.now();
        if (onchainPages === 0) {
          this.runtime.onchainPagesCurrent = 1;
        } else if (rpc429 > 0) {
          this.runtime.onchainLast429At = now;
          const next = Math.max(1, Math.floor(onchainPages / 2) || 1);
          this.runtime.onchainPagesCurrent = next;
        } else if (
          onchainPages < this.onchainPagesMaxPerCycle &&
          now - Number(this.runtime.onchainLast429At || 0) > 180000 &&
          now - Number(this.runtime.onchainLastRampAt || 0) > 60000
        ) {
          this.runtime.onchainPagesCurrent = onchainPages + 1;
          this.runtime.onchainLastRampAt = now;
        } else {
          this.runtime.onchainPagesCurrent = onchainPages;
        }

        if (
          onchain &&
          onchain.ok &&
          Array.isArray(onchain.newWallets) &&
          typeof this.onchainDiscovery.listWallets === "function"
        ) {
          const walletRows = this.onchainDiscovery.listWallets({
            confidenceMin: 0.75,
            onlyConfirmed: false,
          });
          const rowMap = new Map(walletRows.map((row) => [row.wallet, row]));
          const eligible = onchain.newWallets.filter((wallet) => {
            const row = rowMap.get(wallet);
            if (!row) return false;
            if (row.validation && row.validation.status === "rejected") return false;
            return Number(row.confidenceMax || 0) >= 0.75;
          });

          added += this.addWallets(eligible, "onchain");
        }
      }

      if (this.walletStore) {
        added += this.addWallets(
          this.walletStore.list().map((row) => row.wallet),
          "store"
        );
      }

      this.state.lastDiscoveryAt = Date.now();
      this.state.discoveryCycles += 1;
      this.markStateDirty();
      this.persistStateIfNeeded();

      const summary = this.buildDiagnosticsSummary();
      const topReason =
        summary.topErrorReasons && summary.topErrorReasons.length
          ? `${summary.topErrorReasons[0].reason}:${summary.topErrorReasons[0].count}`
          : "none";
      this.logger.info(
        `[wallet-indexer] discovery summary known=${this.state.knownWallets.length} indexed=${summary.indexed} partial=${summary.partial} pending=${summary.pending} failed=${summary.failed} backlog=${summary.backlog} queue=${this.runtime.priorityScanQueue.length} avg_pending_wait_ms=${summary.averagePendingWaitMs} avg_queue_wait_ms=${summary.averageQueueWaitMs} top_error=${topReason}`
      );
    } catch (error) {
      this.runtime.lastError = `[discover] ${toErrorMessage(error)}`;
      this.logger.warn(`[wallet-indexer] discovery failed: ${toErrorMessage(error)}`);
    } finally {
      this.runtime.inDiscovery = false;
    }

    return {
      added,
      total: this.state.knownWallets.length,
      onchain,
    };
  }

  getHotLiveWallets(limit = 256) {
    const max = Math.max(1, Number(limit || 256));
    const now = Date.now();
    if (
      Array.isArray(this.runtime.hotLiveWallets) &&
      this.runtime.hotLiveWallets.length > 0 &&
      now - Number(this.runtime.hotLiveWalletsAt || 0) < 60000
    ) {
      return this.runtime.hotLiveWallets.slice(0, max);
    }
    if (!this.walletStore || typeof this.walletStore.list !== "function") {
      return [];
    }

    const liveSet = new Set(normalizeWallets(this.state.liveWallets || []));
    const ranked = this.walletStore
      .list()
      .map((row) => ({
        wallet: normalizeAddress(row && row.wallet),
        volumeUsd: Number(
          (row && row.all && row.all.volumeUsd) ||
            (row && row.volumeUsd) ||
            0
        ),
        lastTrade: Number(
          (row && row.all && row.all.lastTrade) ||
            (row && row.lastTrade) ||
            0
        ),
      }))
      .filter((row) => row.wallet && liveSet.has(row.wallet))
      .sort((a, b) => {
        if (b.lastTrade !== a.lastTrade) return b.lastTrade - a.lastTrade;
        return b.volumeUsd - a.volumeUsd;
      })
      .slice(0, Math.max(max, 512))
      .map((row) => row.wallet);

    this.runtime.hotLiveWallets = ranked;
    this.runtime.hotLiveWalletsAt = now;
    return ranked.slice(0, max);
  }

  pruneTriggeredLiveWallets(now = Date.now()) {
    const cutoff = now - this.liveTriggerTtlMs;
    const map =
      this.runtime.liveTriggeredWalletAt instanceof Map
        ? this.runtime.liveTriggeredWalletAt
        : new Map();
    for (const [wallet, at] of map.entries()) {
      if (!wallet || Number(at || 0) < cutoff) {
        map.delete(wallet);
      }
    }
    this.runtime.liveTriggeredWalletAt = map;
    return map;
  }

  consumeLiveTriggerStore(now = Date.now()) {
    if (!this.liveTriggerStore || typeof this.liveTriggerStore.readSince !== "function") {
      return this.pruneTriggeredLiveWallets(now);
    }
    if (now - Number(this.runtime.lastLiveTriggerReadAt || 0) < this.liveTriggerPollMs) {
      return this.pruneTriggeredLiveWallets(now);
    }
    this.runtime.lastLiveTriggerReadAt = now;
    const sinceAt = Number(this.runtime.lastLiveTriggerObservedAt || 0);
    let rows = [];
    try {
      rows = this.liveTriggerStore.readSince(sinceAt, {
        limit: this.liveTriggerReadLimit,
      });
    } catch (error) {
      this.runtime.lastLiveTriggerSummary = {
        at: now,
        ok: false,
        error: toErrorMessage(error),
        activeWallets:
          this.runtime.liveTriggeredWalletAt instanceof Map
            ? this.runtime.liveTriggeredWalletAt.size
            : 0,
      };
      return this.pruneTriggeredLiveWallets(now);
    }
    let tracked = 0;
    rows.forEach((row) => {
      const wallet = normalizeAddress(row && row.wallet);
      if (!wallet) return;
      const stateRow = this.state.walletStates && this.state.walletStates[wallet];
      const known =
        Boolean(stateRow) ||
        (this.walletStore &&
          typeof this.walletStore.get === "function" &&
          Boolean(this.walletStore.get(wallet)));
      const at = Math.max(0, Number(row && row.at ? row.at : now));
      this.runtime.lastLiveTriggerObservedAt = Math.max(
        Number(this.runtime.lastLiveTriggerObservedAt || 0),
        at
      );
      if (!known) return;
      this.runtime.liveTriggeredWalletAt.set(wallet, at);
      tracked += 1;
    });
    const active = this.pruneTriggeredLiveWallets(now);
    this.runtime.lastLiveTriggerSummary = {
      at: now,
      ok: true,
      rowsRead: Array.isArray(rows) ? rows.length : 0,
      tracked,
      activeWallets: active.size,
      lastObservedAt: Number(this.runtime.lastLiveTriggerObservedAt || 0) || null,
    };
    return active;
  }

  getTriggeredLiveWallets(limit = 256, now = Date.now()) {
    const max = Math.max(1, Number(limit || 256));
    const active = this.consumeLiveTriggerStore(now);
    return Array.from(active.entries())
      .sort((left, right) => {
        if (Number(right[1] || 0) !== Number(left[1] || 0)) {
          return Number(right[1] || 0) - Number(left[1] || 0);
        }
        const leftPriority = this.getWalletCorrectionPriority(left[0]);
        const rightPriority = this.getWalletCorrectionPriority(right[0]);
        return this.compareWalletCorrectionPriority(rightPriority, leftPriority);
      })
      .slice(0, max)
      .map(([wallet]) => wallet);
  }

  getTopCorrectionWallets(limit = this.topCorrectionCohortSize, force = false) {
    const max = Math.max(1, Number(limit || this.topCorrectionCohortSize || 100));
    const now = Date.now();
    if (
      !force &&
      Array.isArray(this.runtime.topCorrectionWallets) &&
      this.runtime.topCorrectionWallets.length > 0 &&
      now - Number(this.runtime.topCorrectionWalletsAt || 0) < 60000
    ) {
      return this.runtime.topCorrectionWallets.slice(0, max);
    }
    if (!this.walletStore || typeof this.walletStore.list !== "function") {
      return [];
    }

    const ranked = this.walletStore
      .list()
      .map((row) => ({
        wallet: normalizeAddress(row && row.wallet),
        volumeUsd: Number(
          (row && row.all && row.all.volumeUsd) ||
            row.volumeUsd ||
            0
        ),
        lastTrade: Number(
          (row && row.all && row.all.lastTrade) ||
            row.lastTrade ||
            0
        ),
      }))
      .filter((row) => row.wallet)
      .sort((a, b) => {
        if (b.volumeUsd !== a.volumeUsd) return b.volumeUsd - a.volumeUsd;
        return b.lastTrade - a.lastTrade;
      })
      .slice(0, Math.max(max, 256))
      .map((row) => row.wallet);

    this.runtime.topCorrectionWallets = ranked;
    this.runtime.topCorrectionWalletsAt = now;
    return ranked.slice(0, max);
  }

  buildTopCorrectionCohortSummary(limit = this.topCorrectionCohortSize) {
    const cohort = this.getTopCorrectionWallets(limit, false);
    const summary = cohort.reduce(
      (acc, wallet) => {
        const row = this.state.walletStates && this.state.walletStates[wallet];
        acc.total += 1;
        if (this.isBackfillCompleteRow(row)) acc.backfillComplete += 1;
        if (this.isRemoteHistoryVerifiedRow(row)) acc.remoteHistoryVerified += 1;
        if (this.needsRemoteHistoryVerification(row)) acc.remoteVerificationPending += 1;
        if (this.needsHistoryAuditRepair(row)) acc.historyAuditRepairPending += 1;
        if (row && row.forceHeadRefetch) acc.forceHeadRefetch += 1;
        if (row && row.retryPending) acc.retryPending += 1;
        if (row && row.tradeHasMore) acc.tradePagesPending += 1;
        if (row && row.fundingHasMore) acc.fundingPagesPending += 1;
        return acc;
      },
      {
        total: 0,
        backfillComplete: 0,
        remoteHistoryVerified: 0,
        remoteVerificationPending: 0,
        historyAuditRepairPending: 0,
        forceHeadRefetch: 0,
        retryPending: 0,
        tradePagesPending: 0,
        fundingPagesPending: 0,
      }
    );
    return {
      size: cohort.length,
      wallets: cohort.slice(0, Math.min(20, cohort.length)),
      summary,
      lastSweep: this.runtime.lastTopCorrectionSweep || null,
      lastPromotion: this.runtime.lastTopCorrectionPromotion || null,
    };
  }

  isPendingTopCorrectionWallet(wallet, row = null) {
    const normalized = normalizeAddress(wallet);
    if (!normalized) return false;
    const cohort = this.getTopCorrectionWallets(this.topCorrectionCohortSize, false);
    if (!cohort.includes(normalized)) return false;
    return this.needsTopCorrectionSweep(
      row && typeof row === "object" ? row : this.state.walletStates[normalized]
    );
  }

  needsTopCorrectionSweep(row = null) {
    const safe = row && typeof row === "object" ? row : {};
    return (
      !this.isBackfillCompleteRow(safe) ||
      !this.isRemoteHistoryVerifiedRow(safe) ||
      this.needsHistoryAuditRepair(safe)
    );
  }

  refreshWalletPriorityCache(force = false) {
    const now = Date.now();
    if (
      !force &&
      this.runtime.walletPriorityCache instanceof Map &&
      now - Number(this.runtime.walletPriorityCacheAt || 0) < 60000
    ) {
      return this.runtime.walletPriorityCache;
    }
    const map = new Map();
    if (this.walletStore && typeof this.walletStore.list === "function") {
      this.walletStore.list().forEach((row) => {
        const wallet = normalizeAddress(row && row.wallet);
        if (!wallet) return;
        const all =
          row && row.all && typeof row.all === "object"
            ? row.all
            : row && typeof row === "object"
            ? row
            : {};
        map.set(wallet, {
          volumeUsd: Number((all && all.volumeUsd) || row.volumeUsd || 0) || 0,
          lastTrade: Number((all && all.lastTrade) || row.lastTrade || 0) || 0,
          updatedAt: Number((row && row.updatedAt) || 0) || 0,
        });
      });
    }
    this.runtime.walletPriorityCache = map;
    this.runtime.walletPriorityCacheAt = now;
    return map;
  }

  getWalletPriorityStats(wallet) {
    const normalized = normalizeAddress(wallet);
    if (!normalized) {
      return {
        volumeUsd: 0,
        lastTrade: 0,
        updatedAt: 0,
      };
    }
    const cache = this.refreshWalletPriorityCache(false);
    return (
      cache.get(normalized) || {
        volumeUsd: 0,
        lastTrade: 0,
        updatedAt: 0,
      }
    );
  }

  getWalletCorrectionPriority(wallet, row = null) {
    const normalized = normalizeAddress(wallet);
    if (!normalized) return null;
    const safe =
      row && typeof row === "object"
        ? row
        : (this.state.walletStates && this.state.walletStates[normalized]) || {};
    const stats = this.getWalletPriorityStats(normalized);
    const topCohort = this.getTopCorrectionWallets(this.topCorrectionCohortSize, false);
    const topCohortRank = topCohort.indexOf(normalized);
    const rowsLoaded =
      Number(safe.tradeRowsLoaded || 0) + Number(safe.fundingRowsLoaded || 0);
    return {
      wallet: normalized,
      topCohort: topCohortRank >= 0 ? 1 : 0,
      topCohortRank: topCohortRank >= 0 ? topCohortRank : Number.POSITIVE_INFINITY,
      remoteVerificationPending: this.needsRemoteHistoryVerification(safe) ? 1 : 0,
      auditRepairPending: this.needsHistoryAuditRepair(safe) ? 1 : 0,
      liveTracked:
        Number(safe.liveTrackingSince || 0) > 0 ||
        this.deriveWalletLifecycle(safe) === WALLET_LIFECYCLE.LIVE_TRACKING
          ? 1
          : 0,
      retryPending: Boolean(safe.retryPending) ? 1 : 0,
      volumeUsd: Number(stats.volumeUsd || 0) || 0,
      rowsLoaded,
      lastTrade: Number(stats.lastTrade || 0) || 0,
      lastTouchedAt: Math.max(
        Number(safe.lastSuccessAt || 0),
        Number(safe.lastAttemptAt || 0),
        Number(safe.discoveredAt || 0),
        Number(stats.updatedAt || 0)
      ),
    };
  }

  compareWalletCorrectionPriority(left = null, right = null) {
    const a = left || {};
    const b = right || {};
    if (b.topCohort !== a.topCohort) {
      return b.topCohort - a.topCohort;
    }
    if (a.topCohortRank !== b.topCohortRank) {
      return a.topCohortRank - b.topCohortRank;
    }
    if (b.remoteVerificationPending !== a.remoteVerificationPending) {
      return b.remoteVerificationPending - a.remoteVerificationPending;
    }
    if (b.auditRepairPending !== a.auditRepairPending) {
      return b.auditRepairPending - a.auditRepairPending;
    }
    if (b.liveTracked !== a.liveTracked) {
      return b.liveTracked - a.liveTracked;
    }
    if (b.volumeUsd !== a.volumeUsd) {
      return b.volumeUsd - a.volumeUsd;
    }
    if (b.rowsLoaded !== a.rowsLoaded) {
      return b.rowsLoaded - a.rowsLoaded;
    }
    if (b.retryPending !== a.retryPending) {
      return b.retryPending - a.retryPending;
    }
    if (b.lastTrade !== a.lastTrade) {
      return b.lastTrade - a.lastTrade;
    }
    if (a.lastTouchedAt !== b.lastTouchedAt) {
      return a.lastTouchedAt - b.lastTouchedAt;
    }
    return String(a.wallet || "").localeCompare(String(b.wallet || ""));
  }

  rankWalletsForCorrection(wallets = [], limit = null) {
    const ranked = normalizeWallets(wallets)
      .map((wallet) => this.getWalletCorrectionPriority(wallet))
      .filter(Boolean)
      .sort((a, b) => this.compareWalletCorrectionPriority(a, b))
      .map((entry) => entry.wallet);
    if (Number.isFinite(Number(limit)) && Number(limit) > 0) {
      return ranked.slice(0, Math.max(1, Number(limit)));
    }
    return ranked;
  }

  rebalanceContinuationQueue(force = false) {
    const total = this.runtime.continuationScanQueue.length;
    if (total < 2) return null;
    const now = Date.now();
    if (
      !force &&
      this.runtime.lastContinuationRebalance &&
      now - Number(this.runtime.lastContinuationRebalance.at || 0) < 30000
    ) {
      return this.runtime.lastContinuationRebalance;
    }
    const window = Math.min(total, 4096);
    const head = this.runtime.continuationScanQueue.slice(0, window);
    const tail = this.runtime.continuationScanQueue.slice(window);
    const ranked = this.rankWalletsForCorrection(head, head.length);
    this.runtime.continuationScanQueue = ranked.concat(tail);
    this.runtime.continuationScanSet = new Set(this.runtime.continuationScanQueue);
    this.runtime.lastContinuationRebalance = {
      at: now,
      total,
      window,
      promoted: ranked.slice(0, Math.min(16, ranked.length)),
    };
    return this.runtime.lastContinuationRebalance;
  }

  promoteTopCorrectionCohort(limit = this.topCorrectionCohortSize) {
    const cohort = this.getTopCorrectionWallets(limit, false);
    if (!cohort.length) return null;
    const continuationPromoted = [];
    const activationPromoted = [];
    const continuationSet = new Set(cohort);
    const activationSet = new Set(cohort);

    this.runtime.continuationScanQueue = this.runtime.continuationScanQueue.filter((wallet) => {
      if (!wallet || !continuationSet.has(wallet)) return true;
      continuationPromoted.push(wallet);
      return false;
    });
    this.runtime.priorityScanQueue = this.runtime.priorityScanQueue.filter((wallet) => {
      if (!wallet || !activationSet.has(wallet)) return true;
      activationPromoted.push(wallet);
      return false;
    });

    const missingContinuation = [];
    const missingActivation = [];
    cohort.forEach((wallet) => {
      const row = this.state.walletStates && this.state.walletStates[wallet];
      if (this.isBackfillCompleteRow(row)) return;
      const lifecycle = this.deriveWalletLifecycle(row);
      if (
        lifecycle === WALLET_LIFECYCLE.DISCOVERED ||
        lifecycle === WALLET_LIFECYCLE.PENDING_BACKFILL
      ) {
        if (!activationPromoted.includes(wallet)) missingActivation.push(wallet);
      } else if (!continuationPromoted.includes(wallet)) {
        missingContinuation.push(wallet);
      }
    });

    const orderedContinuation = this.rankWalletsForCorrection(
      continuationPromoted.concat(missingContinuation),
      continuationPromoted.length + missingContinuation.length
    );
    const orderedActivation = this.rankWalletsForCorrection(
      activationPromoted.concat(missingActivation),
      activationPromoted.length + missingActivation.length
    );

    this.runtime.continuationScanQueue = orderedContinuation.concat(
      this.runtime.continuationScanQueue
    );
    this.runtime.priorityScanQueue = orderedActivation.concat(
      this.runtime.priorityScanQueue
    );
    this.runtime.continuationScanSet = new Set(this.runtime.continuationScanQueue);
    this.runtime.priorityScanSet = new Set(this.runtime.priorityScanQueue);
    const now = Date.now();
    orderedContinuation.forEach((wallet) => {
      if (!this.runtime.continuationEnqueuedAt.has(wallet)) {
        this.runtime.continuationEnqueuedAt.set(wallet, now);
      }
    });
    orderedActivation.forEach((wallet) => {
      if (!this.runtime.priorityEnqueuedAt.has(wallet)) {
        this.runtime.priorityEnqueuedAt.set(wallet, now);
      }
    });
    return {
      at: now,
      continuationPromoted: orderedContinuation.slice(0, limit),
      activationPromoted: orderedActivation.slice(0, limit),
    };
  }

  queueTopCorrectionSweep(limit = this.topCorrectionCohortSize) {
    const now = Date.now();
    const last = this.runtime.lastTopCorrectionSweep;
    if (last && now - Number(last.at || 0) < 30000) return last;
    const cohort = this.getTopCorrectionWallets(limit, false);
    const summary = {
      at: now,
      cohortSize: cohort.length,
      queued: 0,
      alreadyQueued: 0,
      skipped: 0,
    };
    cohort.forEach((wallet) => {
      const row = this.state.walletStates && this.state.walletStates[wallet];
      if (!this.needsTopCorrectionSweep(row)) {
        summary.skipped += 1;
        return;
      }
      if (row && row.forceHeadRefetch) {
        summary.alreadyQueued += 1;
        return;
      }
      if (
        this.queueWalletForAuditRepairFetch(wallet, row, "top_correction_sweep", {
          forceHeadRefetch: true,
        })
      ) {
        summary.queued += 1;
      } else {
        summary.skipped += 1;
      }
    });
    this.runtime.lastTopCorrectionSweep = summary;
    return summary;
  }

  annotateShardTasks(tasks = []) {
    const byShard = new Map();
    const normalized = (Array.isArray(tasks) ? tasks : [])
      .map((task) => {
        if (!task || !task.wallet) return null;
        const row = this.state.walletStates && this.state.walletStates[task.wallet];
        const assignment = this.getRestShardAssignment(task.wallet, row);
        const nextTask = {
          ...task,
          restShardId: assignment.shardId,
          restShardIndex: assignment.shardIndex,
        };
        const shardId = assignment.shardId || "unassigned";
        if (!byShard.has(shardId)) byShard.set(shardId, []);
        byShard.get(shardId).push(nextTask);
        return nextTask;
      })
      .filter(Boolean);
    const shardSizes = Array.from(byShard.entries()).map(([shardId, queue]) => ({
      shardId,
      queued: queue.length,
    }));

    const ordered = [];
    const shardIds = Array.from(byShard.keys()).sort();
    let remaining = normalized.length;
    while (remaining > 0) {
      shardIds.forEach((shardId) => {
        const queue = byShard.get(shardId);
        if (!queue || !queue.length) return;
        ordered.push(queue.shift());
        remaining -= 1;
      });
    }

    this.runtime.shardQueueSnapshot = shardSizes;
    return ordered;
  }

  pickWalletBatch() {
    const wallets = normalizeWallets(this.state.knownWallets || []);
    const liveWallets = normalizeWallets(this.state.liveWallets || []);
    if (!wallets.length && !liveWallets.length) return [];

    const now = Date.now();
    const activationStrategy = this.computeActivationStrategy();
    const discoveryComplete = Boolean(activationStrategy && activationStrategy.discoveryComplete);
    this.runtime.lastTopCorrectionSweep = this.queueTopCorrectionSweep(this.topCorrectionCohortSize);
    this.runtime.lastTopCorrectionPromotion = this.promoteTopCorrectionCohort(this.topCorrectionCohortSize);
    this.rebalanceContinuationQueue(false);
    const tasks = [];
    const selected = new Set();

    let liveAgeSum = 0;
    let liveAgeCount = 0;
    let liveAgeMax = 0;
    let staleLiveWallets = 0;
    liveWallets.forEach((wallet) => {
      const row = this.state.walletStates && this.state.walletStates[wallet];
      const anchor = Number(
        (row && (row.liveLastScanAt || row.lastSuccessAt || row.liveTrackingSince || row.backfillCompletedAt || row.discoveredAt)) ||
          0
      );
      if (anchor <= 0) {
        staleLiveWallets += 1;
        return;
      }
      const age = Math.max(0, now - anchor);
      liveAgeSum += age;
      liveAgeCount += 1;
      if (age > liveAgeMax) liveAgeMax = age;
      if (age >= this.liveRefreshTargetMs) staleLiveWallets += 1;
    });
    const liveAgeAvg = liveAgeCount > 0 ? Math.round(liveAgeSum / liveAgeCount) : 0;
    const topCorrectionCohort = this.buildTopCorrectionCohortSummary(this.topCorrectionCohortSize);
    const topCorrectionSummary =
      topCorrectionCohort && topCorrectionCohort.summary && typeof topCorrectionCohort.summary === "object"
        ? topCorrectionCohort.summary
        : {};
    const topCorrectionPendingCount =
      Number(topCorrectionSummary.remoteVerificationPending || 0) +
      Number(topCorrectionSummary.historyAuditRepairPending || 0) +
      Number(topCorrectionSummary.tradePagesPending || 0) +
      Number(topCorrectionSummary.fundingPagesPending || 0);
    const prioritizeTopCorrection = discoveryComplete && topCorrectionPendingCount > 0;
    const topCorrectionLiveCapacity = prioritizeTopCorrection
      ? Math.min(
          liveWallets.length,
          Math.max(
            8,
            Math.min(
              this.topCorrectionCohortSize,
              Math.ceil(this.walletScanConcurrency * 0.25)
            )
          )
        )
      : 0;

    let liveCapacity = Math.min(
      liveWallets.length,
      Math.max(this.liveWalletsPerScanConfigured, this.liveWalletsPerScanMin)
    );
    if (staleLiveWallets > 0) {
      const staleBoost = Math.ceil(staleLiveWallets * 0.35);
      liveCapacity = Math.max(liveCapacity, staleBoost);
      if (liveAgeMax > this.liveRefreshTargetMs * 2) {
        liveCapacity = Math.max(
          liveCapacity,
          Math.ceil(Math.max(this.liveWalletsPerScanConfigured, this.liveWalletsPerScanMin) * 1.5)
        );
      }
    }
    const adaptiveLiveHardCap = prioritizeTopCorrection
      ? Math.max(
          Math.max(this.liveWalletsPerScanMin, 8),
          Math.min(
            this.liveWalletsPerScanMax,
            Math.max(topCorrectionLiveCapacity + 4, Math.ceil(this.walletScanConcurrency * 0.5))
          )
        )
      : Math.max(
          this.liveWalletsPerScanMax,
          Math.min(256, Math.ceil(this.walletScanConcurrency * 0.25))
        );
    const activationQueueDepth = this.runtime.priorityScanQueue.length;
    if (activationQueueDepth > 0) {
      const reserveCeiling = Math.max(
        0,
        this.maxWalletsPerScan - this.liveWalletsPerScanMin
      );
      const desiredReserve = activationStrategy.reserveTarget;
      const activationReserve = Math.min(reserveCeiling, desiredReserve);
      liveCapacity = Math.min(
        liveCapacity,
        Math.max(this.liveWalletsPerScanMin, this.maxWalletsPerScan - activationReserve)
      );
    }
    if (activationQueueDepth > this.maxWalletsPerScan * 12 && activationStrategy.headroomRatio >= 0.65) {
      liveCapacity = Math.min(liveCapacity, Math.max(2, Math.min(this.liveWalletsPerScanMin, 4)));
    } else if (
      activationQueueDepth > this.maxWalletsPerScan * 6 &&
      activationStrategy.headroomRatio >= 0.5
    ) {
      liveCapacity = Math.min(
        liveCapacity,
        Math.max(4, Math.min(this.liveWalletsPerScanMin + 2, 8))
      );
    }
    liveCapacity = clamp(
      liveCapacity,
      0,
      Math.min(liveWallets.length, adaptiveLiveHardCap)
    );
    if (topCorrectionLiveCapacity > 0) {
      liveCapacity = Math.max(
        liveCapacity,
        Math.min(
          liveWallets.length,
          adaptiveLiveHardCap,
          topCorrectionLiveCapacity + Math.max(this.liveWalletsPerScanMin, 4)
        )
      );
    }

    const totalBackfillCapacity = Math.max(
      0,
      Math.min(this.maxWalletsPerScan - Math.min(liveCapacity, this.maxWalletsPerScan), wallets.length)
    );
    const continuationDemand = this.summarizeContinuationDemand();
    const lanePlan = this.computeBackfillLanePlan(
      totalBackfillCapacity,
      activationStrategy,
      continuationDemand
    );
    const activationCapacity = lanePlan.activationCapacity;
    const continuationCapacity = lanePlan.continuationCapacity;
    const recentCapacity = lanePlan.recentCapacity;
    const deepCapacity = lanePlan.deepCapacity;
    let backfillCursor = wallets.length > 0 ? this.state.scanCursor % wallets.length : 0;

    let liveCursor = liveWallets.length > 0 ? this.state.liveScanCursor % liveWallets.length : 0;
    let liveAttempts = 0;
    let liveAdded = 0;
    const hotCapacity = Math.min(
      liveCapacity,
      Math.max(this.liveWalletsPerScanMin, Math.min(this.walletScanConcurrency, 96))
    );
    const topCorrectionReserve = Math.min(
      continuationCapacity,
      Math.max(24, Math.min(this.topCorrectionCohortSize, Math.ceil(totalBackfillCapacity * 0.35)))
    );
    this.runtime.scanLanePlan = {
      at: now,
      liveCapacity,
      hotCapacity,
      topCorrectionLiveCapacity,
      topCorrectionReserve,
      topCorrectionPendingCount,
      prioritizeTopCorrection,
      ...lanePlan,
    };
    const topCorrectionLiveWallets = prioritizeTopCorrection
      ? this.rankWalletsForCorrection(
          this.getTopCorrectionWallets(this.topCorrectionCohortSize, false),
          this.topCorrectionCohortSize
        )
      : [];
    let topCorrectionLiveAdded = 0;
    let topCorrectionLiveCursor = 0;
    while (
      topCorrectionLiveAdded < topCorrectionLiveCapacity &&
      liveAdded < liveCapacity &&
      topCorrectionLiveCursor < topCorrectionLiveWallets.length
    ) {
      const wallet = topCorrectionLiveWallets[topCorrectionLiveCursor];
      topCorrectionLiveCursor += 1;
      if (!wallet || selected.has(wallet)) continue;
      const row = this.state.walletStates && this.state.walletStates[wallet];
      if (!this.isLiveRefreshEligibleRow(row)) continue;
      if (this.isWalletLeaseActive(row, now)) continue;
      selected.add(wallet);
      tasks.push({ wallet, mode: "live", reason: "live_top_correction" });
      liveAdded += 1;
      topCorrectionLiveAdded += 1;
    }
    const hotWallets = this.getHotLiveWallets(hotCapacity * 2);
    let hotAdded = 0;
    for (let i = 0; i < hotWallets.length && liveAdded < hotCapacity; i += 1) {
      const wallet = hotWallets[i];
      if (!wallet || selected.has(wallet)) continue;
      const row = this.state.walletStates && this.state.walletStates[wallet];
      if (!this.isWalletLiveTracking(wallet)) continue;
      if (this.isPendingTopCorrectionWallet(wallet, row)) continue;
      if (this.isWalletLeaseActive(row, now)) continue;
      selected.add(wallet);
      tasks.push({ wallet, mode: "live", reason: "live_hotset" });
      liveAdded += 1;
      hotAdded += 1;
    }
    const maxLiveAttempts = Math.max(1, liveWallets.length * 4);
    while (liveAdded < liveCapacity && liveAttempts < maxLiveAttempts) {
      const wallet = liveWallets[liveCursor];
      liveCursor = liveWallets.length > 0 ? (liveCursor + 1) % liveWallets.length : 0;
      liveAttempts += 1;
      if (!wallet || selected.has(wallet)) continue;
      const row = this.state.walletStates && this.state.walletStates[wallet];
      if (!this.isWalletLiveTracking(wallet)) continue;
      if (this.isPendingTopCorrectionWallet(wallet, row)) continue;
      if (this.isWalletLeaseActive(row, now)) continue;
      selected.add(wallet);
      tasks.push({ wallet, mode: "live", reason: "live_round_robin" });
      liveAdded += 1;
    }
    this.state.liveScanCursor = liveCursor;
    this.runtime.liveRefreshSnapshot = {
      at: now,
      capacity: liveCapacity,
      selected: liveAdded,
      topCorrectionSelected: topCorrectionLiveAdded,
      hotSelected: hotAdded,
      staleWallets: staleLiveWallets,
      avgAgeMs: liveAgeAvg,
      maxAgeMs: liveAgeMax,
    };

    let activationAdded = 0;
    while (activationAdded < activationCapacity && this.runtime.priorityScanQueue.length > 0) {
      const wallet = this.runtime.priorityScanQueue.shift();
      this.runtime.priorityScanSet.delete(wallet);
      this.runtime.priorityEnqueuedAt.delete(wallet);
      if (!wallet) continue;
      if (selected.has(wallet)) continue;
      const row = this.state.walletStates && this.state.walletStates[wallet];
      const lifecycle = this.deriveWalletLifecycle(row);
      if (
        lifecycle === WALLET_LIFECYCLE.LIVE_TRACKING &&
        this.isBackfillCompleteRow(row)
      ) {
        continue;
      }
      if (this.isWalletLeaseActive(row, now)) continue;
      selected.add(wallet);
      tasks.push({ wallet, mode: "activate", reason: "activation_priority" });
      activationAdded += 1;
    }

    let continuationAdded = 0;
    let topCorrectionAdded = 0;
    const topCorrectionWallets = this.rankWalletsForCorrection(
      this.getTopCorrectionWallets(this.topCorrectionCohortSize, false),
      this.topCorrectionCohortSize
    );
    let topCorrectionCursor = 0;
    while (topCorrectionAdded < topCorrectionReserve && topCorrectionCursor < topCorrectionWallets.length) {
      const wallet = topCorrectionWallets[topCorrectionCursor];
      topCorrectionCursor += 1;
      if (!wallet) break;
      if (selected.has(wallet)) continue;
      const row = this.state.walletStates && this.state.walletStates[wallet];
      if (!this.isPendingTopCorrectionWallet(wallet, row)) continue;
      const lifecycle = this.deriveWalletLifecycle(row);
      if (
        lifecycle === WALLET_LIFECYCLE.DISCOVERED ||
        lifecycle === WALLET_LIFECYCLE.PENDING_BACKFILL
      ) {
        continue;
      }
      if (this.isWalletLeaseActive(row, now)) continue;
      const historyPhase = String((row && row.historyPhase) || "deep").trim().toLowerCase();
      const mode =
        this.needsRemoteHistoryVerification(row) || historyPhase === "recent"
          ? "recent"
          : "backfill";
      selected.add(wallet);
      tasks.push({ wallet, mode, reason: "top_correction" });
      topCorrectionAdded += 1;
      continuationAdded += 1;
    }
    this.runtime.scanLanePlan.topCorrectionAdded = topCorrectionAdded;
    let recentAdded = 0;
    while (recentAdded < recentCapacity) {
      const task = this.dequeueContinuationTask(selected, now, "recent");
      if (!task) break;
      selected.add(task.wallet);
      tasks.push(task);
      recentAdded += 1;
      continuationAdded += 1;
    }

    let deepAdded = 0;
    while (deepAdded < deepCapacity) {
      const task = this.dequeueContinuationTask(selected, now, "backfill");
      if (!task) break;
      selected.add(task.wallet);
      tasks.push(task);
      deepAdded += 1;
      continuationAdded += 1;
    }

    while (
      continuationAdded < continuationCapacity &&
      this.runtime.continuationScanQueue.length > 0
    ) {
      const task = this.dequeueContinuationTask(selected, now, null);
      if (!task) break;
      selected.add(task.wallet);
      tasks.push(task);
      continuationAdded += 1;
    }

    let attempts = 0;
    const maxAttempts = wallets.length * 4;
    while (continuationAdded < continuationCapacity && attempts < maxAttempts && wallets.length > 0) {
      const wallet = wallets[backfillCursor];
      backfillCursor = (backfillCursor + 1) % wallets.length;
      attempts += 1;
      if (!wallet || selected.has(wallet)) continue;
      const row = this.state.walletStates && this.state.walletStates[wallet];
      const lifecycle = this.deriveWalletLifecycle(row);
      if (
        lifecycle === WALLET_LIFECYCLE.LIVE_TRACKING &&
        this.isBackfillCompleteRow(row)
      ) {
        continue;
      }
      if (
        lifecycle === WALLET_LIFECYCLE.DISCOVERED ||
        lifecycle === WALLET_LIFECYCLE.PENDING_BACKFILL
      ) {
        continue;
      }
      if (this.isWalletLeaseActive(row, now)) continue;
      selected.add(wallet);
      const historyPhase = String((row && row.historyPhase) || "deep").trim().toLowerCase();
      tasks.push({
        wallet,
        mode: historyPhase === "recent" ? "recent" : "backfill",
        reason: "round_robin",
      });
      continuationAdded += 1;
    }
    this.state.scanCursor = backfillCursor;

    return this.annotateShardTasks(tasks);
  }

  async fetchPaginatedHistory(
    pathname,
    {
      restClient,
      preferredClientEntry = null,
      requestMode = "backfill",
      activationStrategy = null,
      endpointKey,
      account,
      limit,
      maxPages,
      startCursor = null,
      done = false,
      pageCache = {},
      paginationState = null,
      ignorePageCache = false,
    }
  ) {
    const pagination = normalizeHistoryEndpointPagination(paginationState, pageCache);
    let cursor = ignorePageCache ? startCursor || null : nextHistoryEndpointCursor(pagination, startCursor);
    let pages = 0;
    let hasMore = !done;
    let lastNextCursor = cursor;
    const rows = [];
    let cacheHits = 0;
    let requests = 0;
    let doneNow = Boolean(done);
    const parsedMaxPages = Number(maxPages);
    const unlimitedPaging =
      !Number.isFinite(parsedMaxPages) || parsedMaxPages <= 0;
    const maxPagesLimit = unlimitedPaging
      ? Number.POSITIVE_INFINITY
      : Math.max(1, Math.floor(parsedMaxPages));
    const cache =
      !ignorePageCache && pageCache && typeof pageCache === "object" ? { ...pageCache } : {};
    const requestTimeoutMs =
      requestMode === "backfill"
        ? this.backfillRequestTimeoutMs
        : requestMode === "recent"
        ? this.recentRequestTimeoutMs
        : requestMode === "live"
        ? this.liveRequestTimeoutMs
        : this.activateRequestTimeoutMs;
    if (!doneNow && !ignorePageCache) {
      ensureEndpointPaginationPage(pagination, cursor, {
        status: "pending",
        pageIndex: cursor ? null : 1,
      });
    }

    while (pages < maxPagesLimit && !doneNow) {
      const key = cursorCacheKey(cursor);
      const pageEntry = ensureEndpointPaginationPage(pagination, cursor, {
        status: "pending",
        pageIndex: cursor ? null : 1,
      });
      pageEntry.lastAttemptedAt = Date.now();
      pagination.lastAttemptedAt = pageEntry.lastAttemptedAt;
      pagination.lastAttemptedCursor = pageEntry.cursor;
      pagination.lastAttemptedPage = Number(pageEntry.pageIndex || 0) || null;
      const cached = cache[key];
      if (cached && typeof cached === "object" && pageEntry.status === "persisted") {
        cacheHits += 1;
        pages += 1;
        cached.touchedAt = Date.now();
        hasMore = Boolean(cached.hasMore);
        const nextCursor = cached.nextCursor || null;
        lastNextCursor = nextCursor;
        doneNow = !hasMore || !nextCursor;
        pageEntry.hasMore = hasMore;
        pageEntry.nextCursor = nextCursor;
        pageEntry.requests = Number(pageEntry.requests || 0);
        pageEntry.status = "persisted";
        cursor = doneNow ? null : nextCursor;
        pagination.frontierCursor = doneNow ? null : nextCursor;
        pagination.exhausted = doneNow;
        if (!doneNow) {
          ensureEndpointPaginationPage(pagination, nextCursor, {
            status: "pending",
            pageIndex: Number(pageEntry.pageIndex || 0) + 1,
            discoveredAt: Date.now(),
          });
        }
        continue;
      }

      const query = {
        account,
        limit,
      };
      if (cursor) query.cursor = cursor;

      let response = null;
      let requestError = null;
      const directClientProvided = restClient && typeof restClient.get === "function";
      const preferredEntry =
        preferredClientEntry && preferredClientEntry.client && typeof preferredClientEntry.client.get === "function"
          ? preferredClientEntry
          : null;
      const maxAttempts =
        directClientProvided && !preferredEntry ? 1 : this.historyRequestMaxAttempts;
      const triedClientIds = new Set();
      let attempt = 0;
      while (attempt < maxAttempts) {
        attempt += 1;
        requestError = null;
        let clientEntry = null;
        if (preferredEntry && attempt === 1) {
          clientEntry = preferredEntry;
        } else if (!directClientProvided || preferredEntry) {
          clientEntry = this.pickRestClientEntry({
            excludeIds: triedClientIds,
            mode: requestMode,
            requireHealthyBackfill: requestMode === "backfill" || requestMode === "recent",
            activationStrategy,
          });
        }
        if (clientEntry && clientEntry.id) {
          triedClientIds.add(clientEntry.id);
        }
        const selectedClient = directClientProvided
          ? attempt === 1 && restClient && typeof restClient.get === "function"
            ? restClient
            : clientEntry && clientEntry.client
          : clientEntry && clientEntry.client
          ? clientEntry.client
          : this.restClients[0];
        const requestStartedAt = Date.now();
        this.observeClientRequestStart(clientEntry);
        requests += 1;
        try {
          response = await selectedClient.get(pathname, {
            query,
            cost: this.historyCost,
            timeoutMs: requestTimeoutMs,
          });
        } catch (error) {
          requestError = error;
        } finally {
          if (requestError) {
            this.observeClientRequestFailure(clientEntry, requestError);
          } else {
            this.observeClientRequestSuccess(clientEntry, Date.now() - requestStartedAt);
          }
          this.observeClientRequestEnd(clientEntry);
        }
        if (!requestError) break;
        const reason = summarizeErrorReason(toErrorMessage(requestError));
        pageEntry.status = "failed";
        pageEntry.retryCount = Number(pageEntry.retryCount || 0) + 1;
        pageEntry.lastError = reason;
        if (!isRetriableRequestReason(reason) || attempt >= maxAttempts) {
          pagination.frontierCursor = pageEntry.cursor;
          const wrappedError =
            requestError instanceof Error ? requestError : new Error(toErrorMessage(requestError));
          wrappedError.historyEndpointKey = endpointKey;
          wrappedError.historyPaginationState = finalizeEndpointPaginationState(pagination);
          break;
        }
      }
      if (requestError) {
        if (requestError && !requestError.historyPaginationState) {
          const wrappedError =
            requestError instanceof Error ? requestError : new Error(toErrorMessage(requestError));
          wrappedError.historyEndpointKey = endpointKey;
          wrappedError.historyPaginationState = finalizeEndpointPaginationState(pagination);
          requestError = wrappedError;
        }
        throw requestError;
      }

      const payload = response && response.payload ? response.payload : {};
      const pageRows = extractPayloadData(response, []);
      if (Array.isArray(pageRows) && pageRows.length) rows.push(...pageRows);

      pages += 1;
      hasMore = Boolean(payload.has_more);
      const nextCursor = payload.next_cursor || null;
      lastNextCursor = nextCursor;
      const pageRowsSummary = summarizePageRows(
        pageRows,
        endpointKey === "funding" ? "created_at" : "timestamp"
      );

      cache[key] = {
        endpoint: endpointKey,
        fetchedAt: Date.now(),
        touchedAt: Date.now(),
        hasMore,
        nextCursor,
      };
      pageEntry.status = "persisted";
      pageEntry.hasMore = hasMore;
      pageEntry.nextCursor = nextCursor;
      pageEntry.rowCount = Number(pageRowsSummary.rowCount || 0);
      pageEntry.firstAt = pageRowsSummary.firstAt;
      pageEntry.lastAt = pageRowsSummary.lastAt;
      pageEntry.fetchedAt = Number(cache[key].fetchedAt || Date.now());
      pageEntry.lastAttemptedAt = pageEntry.fetchedAt;
      pageEntry.requests = Number(pageEntry.requests || 0) + 1;
      pageEntry.lastError = null;
      pagination.lastSuccessfulAt = pageEntry.fetchedAt;
      pagination.lastSuccessfulCursor = pageEntry.cursor;
      pagination.lastSuccessfulPage = Number(pageEntry.pageIndex || 0) || null;

      doneNow = !hasMore || !nextCursor;
      cursor = doneNow ? null : nextCursor;
      pagination.exhausted = doneNow;
      pagination.frontierCursor = doneNow ? null : nextCursor;
      if (!doneNow) {
        ensureEndpointPaginationPage(pagination, nextCursor, {
          status: "pending",
          pageIndex: Number(pageEntry.pageIndex || 0) + 1,
          discoveredAt: pageEntry.fetchedAt || Date.now(),
        });
      }
    }

    const finalPagination = finalizeEndpointPaginationState(pagination);
    return {
      rows,
      pages,
      hasMore: !doneNow && hasMore,
      done: doneNow,
      nextCursor: doneNow ? null : lastNextCursor,
      cacheHits,
      requests,
      pageCache: this.trimPageCache(cache),
      paginationState: finalPagination,
    };
  }

  async fetchActivationSnapshot(wallet, { restClient, preferredClientEntry = null } = {}) {
    const selectedClient =
      restClient && typeof restClient.get === "function"
        ? restClient
        : preferredClientEntry && preferredClientEntry.client
        ? preferredClientEntry.client
        : this.restClients[0];
    const clientEntry =
      preferredClientEntry ||
      (selectedClient
        ? this.clientPoolState.find((entry) => entry && entry.client === selectedClient) || null
        : null);

    let response = null;
    let requestError = null;
    const requestStartedAt = Date.now();
    this.observeClientRequestStart(clientEntry);
    try {
      response = await selectedClient.get("/account", {
        query: { account: wallet },
        cost: this.discoveryCost,
        timeoutMs: this.activateRequestTimeoutMs,
      });
    } catch (error) {
      requestError = error;
    } finally {
      if (requestError) {
        this.observeClientRequestFailure(clientEntry, requestError);
      } else {
        this.observeClientRequestSuccess(clientEntry, Date.now() - requestStartedAt);
      }
      this.observeClientRequestEnd(clientEntry);
    }
    if (requestError) throw requestError;

    const payload = response && response.payload ? response.payload : {};
    const data =
      payload && payload.data && typeof payload.data === "object" && !Array.isArray(payload.data)
        ? payload.data
        : null;

    return {
      payload,
      data,
    };
  }

  async executeScanTask(task, clientEntry, activationStrategy, liveBatchSize) {
    if (!task || !task.wallet) {
      return {
        wallet: null,
        ok: false,
        error: "invalid_scan_task",
      };
    }
    const backfillPageBudget =
      task.mode === "backfill" && liveBatchSize > 0
        ? activationStrategy.continuationPageBudget
        : 0;
    const timeoutMs = this.getTaskTimeoutMs(task.mode);
    let settled = false;
    const taskPromise = this.scanWallet(task.wallet, clientEntry && clientEntry.client, {
      mode: task.mode,
      backfillPageBudget,
      preferredClientEntry: clientEntry,
      activationStrategy,
    })
      .then((result) => {
        settled = true;
        return result;
      })
      .catch((error) => {
        settled = true;
        return {
          wallet: task.wallet,
          ok: false,
          error: toErrorMessage(error),
          mode: task.mode,
        };
      });

    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) return taskPromise;

    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        if (settled) return;
        resolve({
          wallet: task.wallet,
          ok: false,
          error: `scan_timeout_${String(task.mode || "backfill")}_${timeoutMs}ms`,
          mode: task.mode,
          timedOut: true,
        });
      }, timeoutMs);

      taskPromise.then((result) => {
        clearTimeout(timer);
        resolve(result);
      });
    });
  }

  async scanWallet(wallet, restClient, options = {}) {
    const now = Date.now();
    const startedAt = now;
    const prev = this.getOrInitWalletState(wallet, "scan");
    const requestedMode = options && options.mode ? String(options.mode) : "backfill";
    const mode =
      requestedMode === "live" ||
      requestedMode === "activate" ||
      requestedMode === "recent"
        ? requestedMode
        : "backfill";
    const activationStrategy =
      options && options.activationStrategy && typeof options.activationStrategy === "object"
        ? options.activationStrategy
        : this.computeActivationStrategy();
    const preferredClientEntry =
      options && options.preferredClientEntry
        ? options.preferredClientEntry
        : this.getClientEntryById(prev.restShardId);
    const leaseClientId =
      normalizeClientId(
        (preferredClientEntry && preferredClientEntry.id) ||
          (options && options.preferredClientId) ||
          prev.restShardId
      ) || null;
    const attemptToken = this.createAttemptToken(wallet, mode, now);
    const backfillPageBudget = Math.max(
      0,
      Number(options && options.backfillPageBudget ? options.backfillPageBudget : 0)
    );
    let history = null;
    const client =
      restClient && typeof restClient.get === "function" ? restClient : null;

    if (mode === "activate" || mode === "backfill" || mode === "recent") {
      const currentLifecycle = this.deriveWalletLifecycle(prev);
      if (
        currentLifecycle === WALLET_LIFECYCLE.DISCOVERED ||
        currentLifecycle === WALLET_LIFECYCLE.PENDING_BACKFILL
      ) {
        this.state.walletStates[wallet] = {
          ...prev,
          lifecycleStage: WALLET_LIFECYCLE.BACKFILLING,
          lastAttemptAt: now,
          lastAttemptMode: mode,
          activeLeaseClientId: leaseClientId,
          activeLeaseStartedAt: now,
          activeLeaseExpiresAt: now + this.walletLeaseMs,
          activeAttemptToken: attemptToken,
        };
      } else {
        this.state.walletStates[wallet] = {
          ...prev,
          lastAttemptAt: now,
          lastAttemptMode: mode,
          activeLeaseClientId: leaseClientId,
          activeLeaseStartedAt: now,
          activeLeaseExpiresAt: now + this.walletLeaseMs,
          activeAttemptToken: attemptToken,
        };
      }
    } else {
      this.state.walletStates[wallet] = {
        ...prev,
        lastAttemptAt: now,
        lastAttemptMode: mode,
        activeLeaseClientId: leaseClientId,
        activeLeaseStartedAt: now,
        activeLeaseExpiresAt: now + this.walletLeaseMs,
        activeAttemptToken: attemptToken,
      };
    }
    this.markStateDirty();
    this.persistStateIfNeeded();

    try {
      if (mode === "activate") {
        const activation = await this.fetchActivationSnapshot(wallet, {
          restClient: client,
          preferredClientEntry,
        });
        const hasAccount = Boolean(activation && activation.data);
        if (!hasAccount) {
          throw new Error(
            activation && activation.payload && activation.payload.error
              ? String(activation.payload.error)
              : "account_not_found"
          );
        }
        if (this.walletStore && typeof this.walletStore.touch === "function") {
          this.walletStore.touch(wallet, now);
        }
        const nextState = {
          ...prev,
          wallet,
          historyPhase: "recent",
          lifecycleStage: WALLET_LIFECYCLE.LIVE_TRACKING,
          lastAttemptAt: now,
          lastAttemptMode: mode,
          lastScannedAt: now,
          lastSuccessAt: now,
          lastSuccessMode: mode,
          lastError: null,
          lastErrorReason: null,
          lastFailureAt: prev.lastFailureAt || null,
          scansSucceeded: Number(prev.scansSucceeded || 0) + 1,
          scansFailed: Number(prev.scansFailed || 0),
          consecutiveFailures: 0,
          lastScanDurationMs: Date.now() - startedAt,
          tradeHasMore: true,
          fundingHasMore: true,
          tradeDone: false,
          fundingDone: false,
          tradeCursor: Object.prototype.hasOwnProperty.call(prev, "tradeCursor")
            ? prev.tradeCursor
            : null,
          fundingCursor: Object.prototype.hasOwnProperty.call(prev, "fundingCursor")
            ? prev.fundingCursor
            : null,
          backfillCompletedAt: null,
          remoteHistoryVerified: false,
          remoteHistoryVerifiedAt: null,
          liveTrackingSince: Number(prev.liveTrackingSince || now),
          liveLastScanAt: Number(prev.liveLastScanAt || 0) || null,
          validationStatus: "confirmed",
          validationCheckedAt: now,
          retryPending: false,
          retryReason: null,
          retryQueuedAt: null,
          tradePagination:
            prev.tradePagination && typeof prev.tradePagination === "object"
              ? { ...prev.tradePagination }
              : null,
          fundingPagination:
            prev.fundingPagination && typeof prev.fundingPagination === "object"
              ? { ...prev.fundingPagination }
              : null,
          forceHeadRefetch: Boolean(prev.forceHeadRefetch),
          forceHeadRefetchReason: prev.forceHeadRefetch
            ? prev.forceHeadRefetchReason || null
            : null,
          forceHeadRefetchQueuedAt: prev.forceHeadRefetch
            ? Number(prev.forceHeadRefetchQueuedAt || 0) || null
            : null,
          restShardId: prev.restShardId || leaseClientId,
          restShardIndex: Number(prev.restShardIndex || 0) || 0,
          lastRestClientId: leaseClientId,
          lastRestClientUsedAt: now,
          activeLeaseClientId: null,
          activeLeaseStartedAt: null,
          activeLeaseExpiresAt: null,
          activeAttemptToken: null,
        };
        const finalizedState = this.finalizeWalletState(
          wallet,
          nextState,
          null,
          prev.discoveredBy || "activate"
        );
        const attemptCurrent = this.isCurrentAttemptToken(wallet, attemptToken);
        if (attemptCurrent) {
          this.state.walletStates[wallet] = finalizedState;
          this.moveWalletToLiveGroup(wallet, now, { partial: true });
          this.enqueueContinuationWallets([wallet], { reason: "phase2_recent" });
          this.markStateDirty();
          this.persistStateIfNeeded();
        }

        return {
          wallet,
          ok: true,
          mode,
          phase: "activate",
          trades: Number(prev.tradeRowsLoaded || 0),
          funding: Number(prev.fundingRowsLoaded || 0),
          newTrades: 0,
          newFunding: 0,
          stale: !attemptCurrent,
        };
      }

      if (mode !== "live") {
        history = this.loadWalletHistory(wallet);
      }
      const firstSuccessfulBootstrap =
        mode === "backfill" &&
        Number(prev.scansSucceeded || 0) <= 0 &&
        Number(prev.lastSuccessAt || 0) <= 0;
      const recentBootstrap = mode === "recent";
      const useFullHistory = mode === "backfill" ? this.fullHistoryPerWallet : false;
      const historyCatchupChunked =
        mode === "backfill" &&
        !firstSuccessfulBootstrap &&
        Boolean(activationStrategy && activationStrategy.discoveryComplete);
      const tradeLimitForMode =
        (mode === "backfill" && firstSuccessfulBootstrap) || recentBootstrap
          ? activationStrategy.tradeLimit
          : historyCatchupChunked
          ? this.deepHistoryTradesPageLimit
          : this.tradesPageLimit;
      const fundingLimitForMode =
        (mode === "backfill" && firstSuccessfulBootstrap) || recentBootstrap
          ? activationStrategy.fundingLimit
          : historyCatchupChunked
          ? this.deepHistoryFundingPageLimit
          : this.fundingPageLimit;
      const maxPagesForMode =
        mode === "live"
          ? this.liveMaxPagesPerWallet
          : recentBootstrap
          ? 1
          : firstSuccessfulBootstrap
          ? this.activationBootstrapPages
          : useFullHistory
          ? backfillPageBudget > 0
            ? Math.min(
                historyCatchupChunked
                  ? this.deepHistoryPagesPerScan
                  : this.fullHistoryPagesPerScan,
                backfillPageBudget
              )
            : historyCatchupChunked
            ? this.deepHistoryPagesPerScan
            : this.fullHistoryPagesPerScan
          : this.runtime.scanPagesCurrent;
      const forceHeadRefetch = Boolean(prev.forceHeadRefetch) && mode !== "live";
      const tradePaginationInput =
        history && history.pagination && history.pagination.trades
          ? history.pagination.trades
          : {};
      const fundingPaginationInput =
        history && history.pagination && history.pagination.funding
          ? history.pagination.funding
          : {};
      const tradePageCacheInput =
        history && history.pageCache && history.pageCache.trades
          ? history.pageCache.trades
          : {};
      const fundingPageCacheInput =
        history && history.pageCache && history.pageCache.funding
          ? history.pageCache.funding
          : {};
      const tradeStartCursor =
        mode === "live"
          ? null
          : forceHeadRefetch
          ? null
          : nextHistoryEndpointCursor(tradePaginationInput, prev.tradeCursor || null);
      const fundingStartCursor =
        mode === "live"
          ? null
          : forceHeadRefetch
          ? null
          : nextHistoryEndpointCursor(fundingPaginationInput, prev.fundingCursor || null);
      const prevRemoteHistoryVerified = this.isRemoteHistoryVerifiedRow(prev);
      const tradeDoneInput =
        mode === "live" ? false : prevRemoteHistoryVerified ? Boolean(prev.tradeDone) : false;
      const fundingDoneInput =
        mode === "live" ? false : prevRemoteHistoryVerified ? Boolean(prev.fundingDone) : false;
      const tradeFetchTask = this.fetchPaginatedHistory("/trades/history", {
        restClient: client,
        preferredClientEntry,
        requestMode: mode,
        activationStrategy,
        endpointKey: "trades",
        account: wallet,
        limit: tradeLimitForMode,
        maxPages: maxPagesForMode,
        startCursor: tradeStartCursor,
        done: tradeDoneInput,
        ignorePageCache: forceHeadRefetch,
        paginationState: tradePaginationInput,
        pageCache: tradePageCacheInput,
      });
      const fundingFetchTask = this.fetchPaginatedHistory("/funding/history", {
        restClient: client,
        preferredClientEntry,
        requestMode: mode,
        activationStrategy,
        endpointKey: "funding",
        account: wallet,
        limit: fundingLimitForMode,
        maxPages: maxPagesForMode,
        startCursor: fundingStartCursor,
        done: fundingDoneInput,
        ignorePageCache: forceHeadRefetch,
        paginationState: fundingPaginationInput,
        pageCache: fundingPageCacheInput,
      });

      let tradesRes = null;
      let fundingRes = null;
      let partialLiveRetryReason = null;
      let tradePartialFailure = null;
      let fundingPartialFailure = null;
      if (mode === "live") {
        const [tradeSettled, fundingSettled] = await Promise.allSettled([
          tradeFetchTask,
          fundingFetchTask,
        ]);
        if (tradeSettled.status === "fulfilled") {
          tradesRes = tradeSettled.value;
        } else {
          tradePartialFailure = tradeSettled.reason;
          const failedTradePagination =
            tradePartialFailure &&
            tradePartialFailure.historyPaginationState &&
            typeof tradePartialFailure.historyPaginationState === "object"
              ? tradePartialFailure.historyPaginationState
              : tradePaginationInput;
          tradesRes = buildHistoryEndpointFallbackResult({
            paginationState: failedTradePagination,
            pageCache: tradePageCacheInput,
            done: Boolean(prev.tradeDone),
            hasMore: Boolean(prev.tradeHasMore) || !Boolean(prev.tradeDone),
            nextCursor: nextHistoryEndpointCursor(
              failedTradePagination,
              prev.tradeCursor || prev.lastTradeNextCursor || null
            ),
          });
        }
        if (fundingSettled.status === "fulfilled") {
          fundingRes = fundingSettled.value;
        } else {
          fundingPartialFailure = fundingSettled.reason;
          const failedFundingPagination =
            fundingPartialFailure &&
            fundingPartialFailure.historyPaginationState &&
            typeof fundingPartialFailure.historyPaginationState === "object"
              ? fundingPartialFailure.historyPaginationState
              : fundingPaginationInput;
          fundingRes = buildHistoryEndpointFallbackResult({
            paginationState: failedFundingPagination,
            pageCache: fundingPageCacheInput,
            done: Boolean(prev.fundingDone),
            hasMore: Boolean(prev.fundingHasMore) || !Boolean(prev.fundingDone),
            nextCursor: nextHistoryEndpointCursor(
              failedFundingPagination,
              prev.fundingCursor || prev.lastFundingNextCursor || null
            ),
          });
        }
        if (tradePartialFailure && fundingPartialFailure) {
          throw tradePartialFailure;
        }
        if (tradePartialFailure || fundingPartialFailure) {
          const reasons = [];
          if (tradePartialFailure) {
            reasons.push(
              `trades:${summarizeErrorReason(toErrorMessage(tradePartialFailure))}`
            );
          }
          if (fundingPartialFailure) {
            reasons.push(
              `funding:${summarizeErrorReason(toErrorMessage(fundingPartialFailure))}`
            );
          }
          partialLiveRetryReason = reasons.join(",");
        }
      } else {
        [tradesRes, fundingRes] = await Promise.all([tradeFetchTask, fundingFetchTask]);
      }

      const incomingTrades = uniq(
        tradesRes.rows.map((row) => JSON.stringify(normalizeTradeHistoryRow(row)))
      ).map((raw) => JSON.parse(raw));
      const incomingFunding = uniq(
        fundingRes.rows.map((row) => JSON.stringify(normalizeFundingHistoryRow(row)))
      ).map((raw) => JSON.parse(raw));

      const headTradeKey = incomingTrades.length ? normalizeTradeKey(incomingTrades[0]) : null;
      const headFundingKey = incomingFunding.length
        ? normalizeFundingKey(incomingFunding[0])
        : null;

      const prevTradeHeadKey = prev.liveHeadTradeKey || null;
      const prevFundingHeadKey = prev.liveHeadFundingKey || null;
      const liveHeadKnown = Boolean(prevTradeHeadKey || prevFundingHeadKey);
      const prevBackfillComplete = this.isBackfillCompleteRow(prev);
      const liveHeadUnchanged =
        mode === "live" &&
        liveHeadKnown &&
        headTradeKey === prevTradeHeadKey &&
        headFundingKey === prevFundingHeadKey;

      if (liveHeadUnchanged) {
        if (this.walletStore && typeof this.walletStore.touch === "function") {
          this.walletStore.touch(wallet, now);
        }
        const nextState = {
          ...prev,
          wallet,
          lastAttemptAt: now,
          lastAttemptMode: mode,
          lastScannedAt: now,
          lastSuccessAt: now,
          lastSuccessMode: mode,
          lastError: null,
          lastErrorReason: null,
          scansSucceeded: Number(prev.scansSucceeded || 0) + 1,
          scansFailed: Number(prev.scansFailed || 0),
          consecutiveFailures: 0,
          lastScanDurationMs: Date.now() - startedAt,
          newTradeRowsLoaded: 0,
          newFundingRowsLoaded: 0,
          tradePagesLoaded: tradesRes.pages,
          fundingPagesLoaded: fundingRes.pages,
          tradeHasMore: prevBackfillComplete
            ? false
            : Boolean(prev.tradeHasMore) || !Boolean(prev.tradeDone),
          fundingHasMore: prevBackfillComplete
            ? false
            : Boolean(prev.fundingHasMore) || !Boolean(prev.fundingDone),
          tradeDone: prevBackfillComplete ? true : Boolean(prev.tradeDone),
          fundingDone: prevBackfillComplete ? true : Boolean(prev.fundingDone),
          tradeCursor: prevBackfillComplete
            ? null
            : Object.prototype.hasOwnProperty.call(prev, "tradeCursor")
            ? prev.tradeCursor
            : null,
          fundingCursor: prevBackfillComplete
            ? null
            : Object.prototype.hasOwnProperty.call(prev, "fundingCursor")
            ? prev.fundingCursor
            : null,
          lastTradeNextCursor: tradesRes.nextCursor || null,
          lastFundingNextCursor: fundingRes.nextCursor || null,
          tradeCacheHits: Number(prev.tradeCacheHits || 0) + Number(tradesRes.cacheHits || 0),
          fundingCacheHits: Number(prev.fundingCacheHits || 0) + Number(fundingRes.cacheHits || 0),
          tradeRequests: Number(prev.tradeRequests || 0) + Number(tradesRes.requests || 0),
          fundingRequests: Number(prev.fundingRequests || 0) + Number(fundingRes.requests || 0),
          tradePagination:
            prev.tradePagination && typeof prev.tradePagination === "object"
              ? { ...prev.tradePagination }
              : null,
          fundingPagination:
            prev.fundingPagination && typeof prev.fundingPagination === "object"
              ? { ...prev.fundingPagination }
              : null,
          lifecycleStage: WALLET_LIFECYCLE.LIVE_TRACKING,
          backfillCompletedAt: prevBackfillComplete
            ? Number(prev.backfillCompletedAt || prev.lastSuccessAt || now)
            : Number(prev.backfillCompletedAt || 0) || null,
          remoteHistoryVerified: prevBackfillComplete
            ? true
            : Boolean(prev.remoteHistoryVerified),
          remoteHistoryVerifiedAt: prevBackfillComplete
            ? Number(
                prev.remoteHistoryVerifiedAt ||
                  prev.backfillCompletedAt ||
                  prev.lastSuccessAt ||
                  now
              )
            : Number(prev.remoteHistoryVerifiedAt || 0) || null,
          liveTrackingSince: Number(prev.liveTrackingSince || now),
          liveLastScanAt: now,
          liveHeadTradeKey: headTradeKey,
          liveHeadFundingKey: headFundingKey,
          retryPending: false,
          retryReason: null,
          retryQueuedAt: null,
          restShardId: prev.restShardId || leaseClientId,
          restShardIndex: Number(prev.restShardIndex || 0) || 0,
          lastRestClientId: leaseClientId,
          lastRestClientUsedAt: now,
          activeLeaseClientId: null,
          activeLeaseStartedAt: null,
          activeLeaseExpiresAt: null,
          activeAttemptToken: null,
        };
        const finalizedState = this.finalizeWalletState(
          wallet,
          nextState,
          null,
          prev.discoveredBy || "live"
        );
        const attemptCurrent = this.isCurrentAttemptToken(wallet, attemptToken);
        if (attemptCurrent) {
          this.state.walletStates[wallet] = finalizedState;
          if (this.walletStore && typeof this.walletStore.touch === "function") {
            this.walletStore.touch(wallet, now, { deferFlush: true });
          }
          this.moveWalletToLiveGroup(wallet, now, { partial: !prevBackfillComplete });
          if (!prevBackfillComplete) {
            this.enqueueContinuationWallets([wallet], { reason: "live_catchup_pending" });
          }
          this.markStateDirty();
          this.persistStateIfNeeded();
        }

        return {
          wallet,
          ok: true,
          mode,
          fastPath: "live_head_unchanged",
          trades: Number(prev.tradeRowsLoaded || 0),
          funding: Number(prev.fundingRowsLoaded || 0),
          newTrades: 0,
          newFunding: 0,
          stale: !attemptCurrent,
        };
      }

      if (!history) {
        history = this.loadWalletHistory(wallet);
      }

      let newTrades = 0;
      incomingTrades.forEach((row) => {
        const key = normalizeTradeKey(row);
        if (history.tradeSeenKeys[key]) return;
        history.tradeSeenKeys[key] = 1;
        history.trades.push(row);
        newTrades += 1;
      });

      let newFunding = 0;
      incomingFunding.forEach((row) => {
        const key = normalizeFundingKey(row);
        if (history.fundingSeenKeys[key]) return;
        history.fundingSeenKeys[key] = 1;
        history.funding.push(row);
        newFunding += 1;
      });

      history.pageCache = {
        trades: tradesRes.pageCache || {},
        funding: fundingRes.pageCache || {},
      };
      history.pagination = {
        version: WALLET_HISTORY_PAGINATION_VERSION,
        mode: "cursor",
        trades: tradesRes.paginationState || normalizeHistoryEndpointPagination(tradePaginationInput),
        funding:
          fundingRes.paginationState || normalizeHistoryEndpointPagination(fundingPaginationInput),
      };
      let persistedHistory = history;
      const shouldPersistHistory =
        mode !== "live" ||
        newTrades > 0 ||
        newFunding > 0 ||
        !liveHeadKnown;
      if (shouldPersistHistory) {
        persistedHistory = this.saveWalletHistory(wallet, history);
      }

      const record = buildWalletRecordFromHistory({
        wallet,
        tradesHistory: persistedHistory.trades,
        fundingHistory: persistedHistory.funding,
        computedAt: now,
      });
      record.storage = {
        ...(record.storage && typeof record.storage === "object" ? record.storage : {}),
        ...this.buildWalletStorageRefs(wallet),
      };

      this.walletStore.upsert(record, { deferFlush: true });

      const tradePaginationSummary = buildHistoryEndpointPaginationSummary(
        history && history.pagination ? history.pagination.trades : tradePaginationInput
      );
      const fundingPaginationSummary = buildHistoryEndpointPaginationSummary(
        history && history.pagination ? history.pagination.funding : fundingPaginationInput
      );

      let tradeDone =
        Boolean(tradesRes.done) &&
        Boolean(tradePaginationSummary.exhausted) &&
        Number(tradePaginationSummary.retryPages || 0) <= 0;
      let fundingDone =
        Boolean(fundingRes.done) &&
        Boolean(fundingPaginationSummary.exhausted) &&
        Number(fundingPaginationSummary.retryPages || 0) <= 0;
      let lifecycleStage = WALLET_LIFECYCLE.BACKFILLING;
      let backfillCompletedAt = Number(prev.backfillCompletedAt || 0) || null;
      let liveTrackingSince = Number(prev.liveTrackingSince || 0) || null;
      let liveLastScanAt = Number(prev.liveLastScanAt || 0) || null;
      let tradeCursor = tradeDone
        ? null
        : nextHistoryEndpointCursor(
            history && history.pagination ? history.pagination.trades : tradePaginationInput,
            tradesRes.nextCursor || prev.tradeCursor || null
          );
      let fundingCursor = fundingDone
        ? null
        : nextHistoryEndpointCursor(
            history && history.pagination ? history.pagination.funding : fundingPaginationInput,
            fundingRes.nextCursor || prev.fundingCursor || null
          );
      let tradeHasMore = !tradeDone;
      let fundingHasMore = !fundingDone;
      let activatedForLive = false;
      let nextHistoryPhase = recentBootstrap ? "deep" : String(prev.historyPhase || "deep");
      let remoteHistoryVerified =
        mode === "live" ? prevBackfillComplete : Boolean(prev.remoteHistoryVerified);
      let remoteHistoryVerifiedAt = Number(prev.remoteHistoryVerifiedAt || 0) || null;

      if (mode === "live") {
        const tradeCapacity = Math.max(1, this.tradesPageLimit * this.liveMaxPagesPerWallet);
        const fundingCapacity = Math.max(1, this.fundingPageLimit * this.liveMaxPagesPerWallet);
        const liveOverflow = newTrades >= tradeCapacity || newFunding >= fundingCapacity;
        const liveBackfillComplete = tradeDone && fundingDone;
        if (liveOverflow) {
          // Overflow guard: if a live polling window is fully filled, switch this wallet back
          // to backfill mode so historical catch-up can close potential gaps deterministically.
          tradeDone = false;
          fundingDone = false;
          tradeCursor = null;
          fundingCursor = null;
          tradeHasMore = true;
          fundingHasMore = true;
          lifecycleStage = WALLET_LIFECYCLE.BACKFILLING;
          remoteHistoryVerified = false;
          remoteHistoryVerifiedAt = null;
          this.removeFromLiveGroup(wallet);
        } else {
          lifecycleStage = WALLET_LIFECYCLE.LIVE_TRACKING;
          if (prevBackfillComplete || liveBackfillComplete) {
            tradeDone = true;
            fundingDone = true;
            tradeCursor = null;
            fundingCursor = null;
            tradeHasMore = false;
            fundingHasMore = false;
            backfillCompletedAt = Number(backfillCompletedAt || prev.lastSuccessAt || now);
            nextHistoryPhase = "complete";
            remoteHistoryVerified = true;
            remoteHistoryVerifiedAt = Number(
              remoteHistoryVerifiedAt ||
                prev.remoteHistoryVerifiedAt ||
                backfillCompletedAt ||
                now
            );
          } else {
            tradeDone = Boolean(tradeDone) || Boolean(prev.tradeDone);
            fundingDone = Boolean(fundingDone) || Boolean(prev.fundingDone);
            tradeCursor = tradeDone
              ? null
              : tradeCursor !== undefined
              ? tradeCursor
              : Object.prototype.hasOwnProperty.call(prev, "tradeCursor")
              ? prev.tradeCursor
              : null;
            fundingCursor = fundingDone
              ? null
              : fundingCursor !== undefined
              ? fundingCursor
              : Object.prototype.hasOwnProperty.call(prev, "fundingCursor")
              ? prev.fundingCursor
              : null;
            tradeHasMore = !tradeDone;
            fundingHasMore = !fundingDone;
            backfillCompletedAt = Number(prev.backfillCompletedAt || 0) || null;
            remoteHistoryVerified = false;
            remoteHistoryVerifiedAt = null;
          }
          liveTrackingSince = Number(liveTrackingSince || now);
          liveLastScanAt = now;
          activatedForLive = true;
        }
      } else if (tradeDone && fundingDone) {
        lifecycleStage = WALLET_LIFECYCLE.FULLY_INDEXED;
        backfillCompletedAt = Number(backfillCompletedAt || now);
        nextHistoryPhase = "complete";
        remoteHistoryVerified = true;
        remoteHistoryVerifiedAt = Number(remoteHistoryVerifiedAt || now);
      } else if (Number(prev.liveTrackingSince || 0) > 0 || firstSuccessfulBootstrap) {
        lifecycleStage = WALLET_LIFECYCLE.LIVE_TRACKING;
        liveTrackingSince = Number(liveTrackingSince || prev.liveTrackingSince || now);
        liveLastScanAt = Number(prev.liveLastScanAt || 0) || null;
        activatedForLive = true;
        nextHistoryPhase = "deep";
        remoteHistoryVerified = false;
        remoteHistoryVerifiedAt = null;
      }

      const nextState = {
        ...prev,
        wallet,
        historyPhase: nextHistoryPhase,
        lastAttemptAt: now,
        lastAttemptMode: mode,
        lastScannedAt: now,
        lastSuccessAt: now,
        lastSuccessMode: mode,
        lastError: null,
        lastErrorReason: null,
        lastFailureAt: prev.lastFailureAt || null,
        scansSucceeded: Number(prev.scansSucceeded || 0) + 1,
        scansFailed: Number(prev.scansFailed || 0),
        consecutiveFailures: 0,
        lastScanDurationMs: Date.now() - startedAt,
        tradeRowsLoaded: persistedHistory.trades.length,
        fundingRowsLoaded: persistedHistory.funding.length,
        newTradeRowsLoaded: newTrades,
        newFundingRowsLoaded: newFunding,
        tradePagesLoaded: tradesRes.pages,
        fundingPagesLoaded: fundingRes.pages,
        tradeHasMore,
        fundingHasMore,
        tradeDone,
        fundingDone,
        tradeCursor,
        fundingCursor,
        lastTradeNextCursor: tradePartialFailure
          ? prev.lastTradeNextCursor || prev.tradeCursor || null
          : tradesRes.nextCursor || null,
        lastFundingNextCursor: fundingPartialFailure
          ? prev.lastFundingNextCursor || prev.fundingCursor || null
          : fundingRes.nextCursor || null,
        tradeCacheHits: Number(prev.tradeCacheHits || 0) + Number(tradesRes.cacheHits || 0),
        fundingCacheHits: Number(prev.fundingCacheHits || 0) + Number(fundingRes.cacheHits || 0),
        tradeRequests: Number(prev.tradeRequests || 0) + Number(tradesRes.requests || 0),
        fundingRequests: Number(prev.fundingRequests || 0) + Number(fundingRes.requests || 0),
        tradePagination: tradePaginationSummary,
        fundingPagination: fundingPaginationSummary,
        lifecycleStage,
        backfillCompletedAt,
        remoteHistoryVerified,
        remoteHistoryVerifiedAt,
        liveTrackingSince,
        liveLastScanAt,
        liveHeadTradeKey: headTradeKey || prev.liveHeadTradeKey || null,
        liveHeadFundingKey: headFundingKey || prev.liveHeadFundingKey || null,
        retryPending: Boolean(partialLiveRetryReason),
        retryReason: partialLiveRetryReason || null,
        retryQueuedAt: partialLiveRetryReason ? now : null,
        forceHeadRefetch: tradeDone && fundingDone ? false : Boolean(prev.forceHeadRefetch),
        forceHeadRefetchReason:
          tradeDone && fundingDone ? null : prev.forceHeadRefetchReason || null,
        forceHeadRefetchQueuedAt:
          tradeDone && fundingDone
            ? null
            : Number(prev.forceHeadRefetchQueuedAt || 0) || null,
        restShardId: prev.restShardId || leaseClientId,
        restShardIndex: Number(prev.restShardIndex || 0) || 0,
        lastRestClientId: leaseClientId,
        lastRestClientUsedAt: now,
        lastError: partialLiveRetryReason
          ? `partial_live_update:${partialLiveRetryReason}`
          : null,
        lastErrorReason: partialLiveRetryReason || null,
        lastFailureAt: partialLiveRetryReason ? now : prev.lastFailureAt || null,
        lastFailureMode: partialLiveRetryReason ? mode : prev.lastFailureMode || null,
        activeLeaseClientId: null,
        activeLeaseStartedAt: null,
        activeLeaseExpiresAt: null,
        activeAttemptToken: null,
      };
      const finalizedState = this.finalizeWalletState(
        wallet,
        nextState,
        persistedHistory,
        prev.discoveredBy || "scan"
      );
      const attemptCurrent = this.isCurrentAttemptToken(wallet, attemptToken);
      if (attemptCurrent) {
        this.state.walletStates[wallet] = finalizedState;
        if (finalizedState.tradeDone && finalizedState.fundingDone) {
          this.moveWalletToLiveGroup(wallet, now);
        } else if (activatedForLive) {
          this.moveWalletToLiveGroup(wallet, now, { partial: true });
          this.enqueueContinuationWallets([wallet], { reason: "continue_backfill" });
        } else if (mode === "backfill" || mode === "recent") {
          this.enqueueContinuationWallets([wallet], { reason: "continue_backfill" });
        } else {
          this.enqueueContinuationWallets([wallet], { reason: "live_overflow_catchup" });
        }
        this.markStateDirty();
        this.persistStateIfNeeded();
      }

      return {
        wallet,
        ok: true,
        mode,
        trades: persistedHistory.trades.length,
        funding: persistedHistory.funding.length,
        newTrades,
        newFunding,
        stale: !attemptCurrent,
      };
    } catch (error) {
      const message = toErrorMessage(error);
      const failedEndpointKey =
        error && error.historyEndpointKey ? String(error.historyEndpointKey).trim().toLowerCase() : null;
      const failedPaginationState =
        error && error.historyPaginationState && typeof error.historyPaginationState === "object"
          ? error.historyPaginationState
          : null;
      if (mode !== "live" && failedEndpointKey && failedPaginationState) {
        try {
          const failedHistory = this.loadWalletHistory(wallet);
          failedHistory.pagination = {
            version: WALLET_HISTORY_PAGINATION_VERSION,
            mode: "cursor",
            trades:
              failedEndpointKey === "trades"
                ? failedPaginationState
                : failedHistory.pagination && failedHistory.pagination.trades
                ? failedHistory.pagination.trades
                : normalizeHistoryEndpointPagination(),
            funding:
              failedEndpointKey === "funding"
                ? failedPaginationState
                : failedHistory.pagination && failedHistory.pagination.funding
                ? failedHistory.pagination.funding
                : normalizeHistoryEndpointPagination(),
          };
          this.saveWalletHistory(wallet, failedHistory);
        } catch (persistError) {
          console.error(
            `[wallet-indexer] failed to persist pagination checkpoint wallet=${wallet} endpoint=${failedEndpointKey} error=${toErrorMessage(
              persistError
            )}`
          );
        }
      }
      const keepLiveRefresh =
        mode === "live" ||
        this.isLiveRefreshEligibleRow(prev) ||
        Number(prev.liveTrackingSince || 0) > 0;
      const failedState = {
        ...prev,
        wallet,
        lastAttemptAt: now,
        lastAttemptMode: mode,
        lastScannedAt: now,
        lastFailureAt: now,
        lastError: message,
        lastErrorReason: summarizeErrorReason(message),
        lastFailureMode: mode,
        scansSucceeded: Number(prev.scansSucceeded || 0),
        scansFailed: Number(prev.scansFailed || 0) + 1,
        consecutiveFailures: Number(prev.consecutiveFailures || 0) + 1,
        lastScanDurationMs: Date.now() - startedAt,
        historyPhase:
          mode === "activate"
            ? "activate"
            : String(prev.historyPhase || (mode === "recent" ? "recent" : "deep")),
        lifecycleStage: keepLiveRefresh
          ? WALLET_LIFECYCLE.LIVE_TRACKING
          : this.isBackfillCompleteRow(prev)
          ? WALLET_LIFECYCLE.LIVE_TRACKING
          : WALLET_LIFECYCLE.BACKFILLING,
        retryPending: true,
        retryReason: summarizeErrorReason(message),
        retryQueuedAt: now,
        tradePagination:
          failedEndpointKey === "trades" && failedPaginationState
            ? buildHistoryEndpointPaginationSummary(failedPaginationState)
            : prev.tradePagination && typeof prev.tradePagination === "object"
            ? { ...prev.tradePagination }
            : null,
        fundingPagination:
          failedEndpointKey === "funding" && failedPaginationState
            ? buildHistoryEndpointPaginationSummary(failedPaginationState)
            : prev.fundingPagination && typeof prev.fundingPagination === "object"
            ? { ...prev.fundingPagination }
            : null,
        restShardId: prev.restShardId || leaseClientId,
        restShardIndex: Number(prev.restShardIndex || 0) || 0,
        lastRestClientId: leaseClientId,
        lastRestClientUsedAt: now,
        activeLeaseClientId: null,
        activeLeaseStartedAt: null,
        activeLeaseExpiresAt: null,
        activeAttemptToken: null,
      };
      const finalizedFailedState = this.finalizeWalletState(
        wallet,
        failedState,
        null,
        prev.discoveredBy || "failure"
      );
      const attemptCurrent = this.isCurrentAttemptToken(wallet, attemptToken);
      if (attemptCurrent) {
        this.state.walletStates[wallet] = finalizedFailedState;
        if (!this.isBackfillCompleteRow(prev)) {
          if (keepLiveRefresh) {
            this.enqueueLiveWallets([wallet]);
          }
          if (mode === "activate") {
            this.enqueuePriorityWallets([wallet], { reason: "retry_failed" });
          } else {
            this.enqueueContinuationWallets([wallet], { reason: "retry_failed" });
          }
        } else {
          this.enqueueLiveWallets([wallet]);
        }
        this.markStateDirty();
        this.persistStateIfNeeded();
      }

      return {
        wallet,
        ok: false,
        mode,
        error: message,
        stale: !attemptCurrent,
      };
    }
  }

  async scanCycle() {
    if (this.discoveryOnly) {
      return {
        startedAt: Date.now(),
        endedAt: Date.now(),
        scanned: 0,
        ok: 0,
        failed: 0,
        skipped: true,
      };
    }

    if (this.runtime.inScan) return null;
    this.runtime.inScan = true;
    this.runtime.lastScanStartedAt = Date.now();
    this.runtime.scanPhase = "start";
    this.runtime.scanPhaseAt = this.runtime.lastScanStartedAt;

    const startedAt = Date.now();
    try {
      const minimalRepairSummary = {
        scanned: 0,
        repairedLocal: 0,
        requeuedRemote: 0,
        failed: 0,
        at: Date.now(),
      };
      this.reconcileLiveWalletSet();
      this.runtime.scanPhase = "legacy_migration";
      this.runtime.scanPhaseAt = Date.now();
      const inlineMigrationLimit = this.runtime.inRepair
        ? 0
        : Math.max(0, Math.min(128, Math.floor(this.historyAuditRepairBatchSize / 4) || 0));
      if (inlineMigrationLimit > 0) {
        this.runtime.lastLegacyVerificationMigration = this.runLegacyRemoteVerificationMigration(
          inlineMigrationLimit
        );
      }
      this.runtime.scanPhase = "diagnostics";
      this.runtime.scanPhaseAt = Date.now();
      const summaryBefore = this.buildDiagnosticsSummary();
      let repairSummary =
        this.runtime.lastHistoryRepairSummary &&
        typeof this.runtime.lastHistoryRepairSummary === "object"
          ? { ...this.runtime.lastHistoryRepairSummary }
          : { ...minimalRepairSummary };
      const auditRepairPending = Math.max(
        0,
        Number(
          summaryBefore && summaryBefore.historyAuditRepairPending !== undefined
            ? summaryBefore.historyAuditRepairPending
            : 0
        )
      );
      const remoteVerificationPending = Math.max(
        0,
        Number(
          summaryBefore && summaryBefore.remoteHistoryVerificationPending !== undefined
            ? summaryBefore.remoteHistoryVerificationPending
            : 0
        )
      );
      const remoteCatchupHeavy =
        remoteVerificationPending >= Math.max(1000, this.topCorrectionCohortSize) ||
        Number(summaryBefore && summaryBefore.backlog ? summaryBefore.backlog : 0) >=
          this.maxWalletsPerScan * 4;
      const inlineRepairLimit =
        !this.runtime.inRepair && !remoteCatchupHeavy
          ? Math.max(0, Math.min(64, Math.floor(this.historyAuditRepairBatchSize / 8) || 0))
          : 0;
      if (inlineRepairLimit > 0) {
        this.runtime.scanPhase = "inline_repair";
        this.runtime.scanPhaseAt = Date.now();
        repairSummary = this.runHistoryAuditRepairCycle(inlineRepairLimit);
      }
      this.runtime.scanPhase = "backlog_eval";
      this.runtime.scanPhaseAt = Date.now();
      const backlogMode = this.evaluateBacklogMode(summaryBefore);
      this.runtime.backlogMode = backlogMode.active;
      this.runtime.backlogReason = backlogMode.reason;
      if (backlogMode.active) {
        this.refillPriorityFromBacklog();
        this.refillContinuationFromBacklog();
      }

      this.runtime.scanPhase = "pick_batch";
      this.runtime.scanPhaseAt = Date.now();
      const batch = this.pickWalletBatch();
      const activationBatchSize = batch.filter((item) => item && item.mode === "activate").length;
      const recentBatchSize = batch.filter((item) => item && item.mode === "recent").length;
      const backfillBatchSize = batch.filter((item) => item && item.mode === "backfill").length;
      const liveBatchSize = batch.filter((item) => item && item.mode === "live").length;
      let ok = 0;
      let failed = 0;
      const errorReasons = new Map();

      this.runtime.scanPhase = "client_pool";
      this.runtime.scanPhaseAt = Date.now();
      const clientPoolSnapshot = this.buildClientPoolSummary();
      const activationStrategy = this.computeActivationStrategy(clientPoolSnapshot);
      const strictBackfillPool =
        backfillBatchSize > 0
          ? this.getHealthyBackfillClientIds({
              selectionProfile: this.buildDeepBackfillSelectionProfile(
                activationStrategy,
                "backfill"
              ),
            })
          : new Set();
      const deepBackfillCapPerProxy =
        backfillBatchSize > 0
          ? this.computeDeepBackfillPerProxyCap(
              backfillBatchSize,
              strictBackfillPool.size || this.clientPoolState.length,
              activationStrategy
            )
          : 0;
      const deepBackfillAssignedByClient = new Map();
      const deferredDeepBackfillTasks = [];
      const shardQueues = new Map();
      this.runtime.scanPhase = "assign_tasks";
      this.runtime.scanPhaseAt = Date.now();
      batch.forEach((task) => {
        if (!task || !task.wallet) return;
        let excludedByCap = null;
        if (task.mode === "backfill" && deepBackfillCapPerProxy > 0) {
          const eligiblePoolSize = Math.max(
            1,
            Number(
              strictBackfillPool.size > 0
                ? strictBackfillPool.size
                : this.clientPoolState.length
            )
          );
          const cappedIds = new Set(
            Array.from(deepBackfillAssignedByClient.entries())
              .filter(([, count]) => Number(count || 0) >= deepBackfillCapPerProxy)
              .map(([clientId]) => clientId)
          );
          if (cappedIds.size >= eligiblePoolSize) {
            deferredDeepBackfillTasks.push(task);
            return;
          }
          if (cappedIds.size > 0) {
            excludedByCap = cappedIds;
          }
        }
        const clientEntry =
          this.selectClientEntryForTask(task, activationStrategy, {
            excludeIds: excludedByCap,
            strictOnly: task.mode === "backfill" && strictBackfillPool.size > 0,
          }) ||
          null;
        if (!clientEntry) {
          if (task.mode === "backfill") {
            deferredDeepBackfillTasks.push(task);
          }
          return;
        }
        const shardId = clientEntry && clientEntry.id ? clientEntry.id : "unassigned";
        if (task.mode === "backfill" && shardId !== "unassigned") {
          deepBackfillAssignedByClient.set(
            shardId,
            Math.max(0, Number(deepBackfillAssignedByClient.get(shardId) || 0)) + 1
          );
        }
        if (!shardQueues.has(shardId)) {
          shardQueues.set(shardId, {
            clientEntry,
            tasks: [],
          });
        }
        shardQueues.get(shardId).tasks.push({
          ...task,
          executionRestShardId: shardId,
        });
      });
      this.runtime.shardQueueSnapshot = Array.from(shardQueues.entries()).map(([shardId, row]) => ({
        shardId,
        queued: Array.isArray(row.tasks) ? row.tasks.length : 0,
      }));
      this.runtime.deepBackfillAssignment = {
        at: Date.now(),
        strictPoolSize: strictBackfillPool.size,
        capPerProxy: deepBackfillCapPerProxy,
        deferredCount: deferredDeepBackfillTasks.length,
        assignedByClient: Array.from(deepBackfillAssignedByClient.entries()).map(
          ([clientId, count]) => ({
            clientId,
            count,
          })
        ),
      };
      if (deferredDeepBackfillTasks.length > 0) {
        this.enqueueContinuationWallets(
          deferredDeepBackfillTasks.map((task) => task.wallet),
          { reason: "deep_backfill_cap_deferred" }
        );
      }

      this.runtime.scanPhase = "run_workers";
      this.runtime.scanPhaseAt = Date.now();
      const workers = Array.from(shardQueues.values()).flatMap(({ clientEntry, tasks }) => {
        const shardWorkerCount = Math.max(
          1,
          Math.min(
            Array.isArray(tasks) ? tasks.length : 0,
            clientEntry && clientEntry.proxyUrl
              ? Number(activationStrategy.shardParallelism || 1)
              : 1
          )
        );
        return Array.from({ length: shardWorkerCount }, () =>
          (async () => {
            while (tasks.length > 0) {
              const task = tasks.shift();
              if (!task || !task.wallet) continue;
              let result = await this.executeScanTask(
                task,
                clientEntry,
                activationStrategy,
                liveBatchSize
              );
              if (!result || !result.ok) {
                const reason = summarizeErrorReason(result && result.error ? result.error : "");
                if (result && result.timedOut) {
                  this.markWalletRetryPending(task.wallet, "timeout", task.mode, Date.now());
                  if (task.mode === "activate") {
                    this.enqueuePriorityWallets([task.wallet], {
                      reason: "retry_timeout",
                      front: true,
                    });
                  } else if (task.mode === "live") {
                    this.enqueueLiveWallets([task.wallet]);
                  } else {
                    this.enqueueContinuationWallets([task.wallet], {
                      reason: "retry_timeout",
                    });
                  }
                  this.logger.warn(
                    `[wallet-indexer] scan_timeout wallet=${String(task.wallet).slice(
                      0,
                      8
                    )} mode=${task.mode} shard=${task.executionRestShardId || task.restShardId || "none"} timeout_ms=${this.getTaskTimeoutMs(
                      task.mode
                    )}`
                  );
                }
                const canRetry =
                  task.mode === "live" &&
                  !Boolean(result && result.timedOut) &&
                  isRetriableRequestReason(reason) &&
                  !String(result && result.error ? result.error : "")
                    .toLowerCase()
                    .includes("not found");
                if (canRetry) {
                  result = await this.executeScanTask(
                    task,
                    clientEntry,
                    activationStrategy,
                    liveBatchSize
                  );
                  if (result && result.ok) {
                    this.logger.log(
                      `[wallet-indexer] retry_ok wallet=${task.wallet.slice(
                        0,
                        8
                      )} mode=${task.mode} shard=${task.executionRestShardId || task.restShardId || "none"} reason=${reason}`
                    );
                  }
                }
              }
              if (result && result.ok) ok += 1;
              else {
                failed += 1;
                const reason = summarizeErrorReason(result && result.error ? result.error : "");
                errorReasons.set(reason, (errorReasons.get(reason) || 0) + 1);
              }
            }
          })()
        );
      });
      await Promise.all(workers);

      this.runtime.scanPhase = "persist_results";
      this.runtime.scanPhaseAt = Date.now();
      this.state.lastScanAt = Date.now();
      this.state.scanCycles += 1;
      this.markStateDirty();
      this.persistStateIfNeeded();
      if (this.walletStore && typeof this.walletStore.flush === "function") {
        this.walletStore.flush(false);
      }

      const now = Date.now();
      const rate429Count = Number(errorReasons.get("rate_limit_429") || 0);
      if (rate429Count > 0) {
        this.runtime.scanLast429At = now;
        const ratio = batch.length > 0 ? rate429Count / batch.length : 0;
        if (ratio >= 0.5) {
          this.runtime.scanConcurrencyCurrent = Math.max(
            1,
            this.runtime.scanConcurrencyCurrent - 2
          );
        } else if (ratio >= 0.2) {
          this.runtime.scanConcurrencyCurrent = Math.max(
            1,
            this.runtime.scanConcurrencyCurrent - 1
          );
        }
        if (!this.fullHistoryPerWallet) {
          this.runtime.scanPagesCurrent = Math.max(
            1,
            Math.floor(this.runtime.scanPagesCurrent / 2) || 1
          );
        }
      } else if (
        this.runtime.scanConcurrencyCurrent < this.walletScanConcurrency &&
        now - Number(this.runtime.scanLast429At || 0) > this.scanRampQuietMs &&
        now - Number(this.runtime.scanLastRampAt || 0) > this.scanRampStepMs
      ) {
        this.runtime.scanConcurrencyCurrent += 1;
        this.runtime.scanLastRampAt = now;
      } else if (
        !this.fullHistoryPerWallet &&
        this.runtime.scanPagesCurrent < this.maxPagesPerWallet &&
        now - Number(this.runtime.scanLast429At || 0) > this.scanRampQuietMs &&
        now - Number(this.runtime.scanLastRampAt || 0) > this.scanRampStepMs
      ) {
        this.runtime.scanPagesCurrent += 1;
        this.runtime.scanLastRampAt = now;
      }

      const summary = this.buildDiagnosticsSummary();
      const clientPool = this.buildClientPoolSummary();
      const restShardSummary = this.buildRestShardSummary();
      const activeShards = restShardSummary.filter((row) => Number(row.cycleQueuedWallets || 0) > 0).length;
      const topReason =
        summary.topErrorReasons && summary.topErrorReasons.length
          ? `${summary.topErrorReasons[0].reason}:${summary.topErrorReasons[0].count}`
          : "none";
      const lanePlan = this.runtime.scanLanePlan || null;
      this.logger.info(
        `[wallet-indexer] scan summary scanned=${batch.length} activate=${activationBatchSize} recent=${recentBatchSize} backfill=${backfillBatchSize} live=${liveBatchSize} ok=${ok} failed=${failed} repair_local=${Number(repairSummary && repairSummary.repairedLocal || 0)} repair_requeue=${Number(repairSummary && repairSummary.requeuedRemote || 0)} known=${this.state.knownWallets.length} indexed=${summary.indexed} partial=${summary.partial} pending=${summary.pending} failed_total=${summary.failed} live_tracking=${summary.liveTracking} backlog=${summary.backlog} verified=${summary.verifiedBackfillComplete} audit_pending=${summary.historyAuditRepairPending} queue=${this.runtime.priorityScanQueue.length} continuation_queue=${this.runtime.continuationScanQueue.length} live_queue=${this.runtime.liveScanQueue.length} live_stale=${summary.liveStaleWallets} live_avg_age_ms=${summary.liveAverageAgeMs} live_max_age_ms=${summary.liveMaxAgeMs} avg_pending_wait_ms=${summary.averagePendingWaitMs} avg_queue_wait_ms=${summary.averageQueueWaitMs} backlog_mode=${this.runtime.backlogMode ? "on" : "off"} backlog_reason=${this.runtime.backlogReason || "none"} scan_concurrency=${this.runtime.scanConcurrencyCurrent} scan_pages=${this.fullHistoryPerWallet ? `full_history_chunk:${this.fullHistoryPagesPerScan}` : this.runtime.scanPagesCurrent} lane_activation=${lanePlan ? lanePlan.activationCapacity : 0} lane_recent=${lanePlan ? lanePlan.recentCapacity : 0} lane_deep=${lanePlan ? lanePlan.deepCapacity : 0} lane_live=${lanePlan ? lanePlan.liveCapacity : 0} shard_parallelism=${activationStrategy.shardParallelism || 1} client_active=${clientPool.active}/${clientPool.total} client_cooling=${clientPool.cooling} client_inflight=${clientPool.inFlight} shard_active=${activeShards}/${restShardSummary.length} top_error=${topReason}`
      );

      return {
        startedAt,
        endedAt: Date.now(),
        scanned: batch.length,
        scannedActivation: activationBatchSize,
        scannedRecent: recentBatchSize,
        scannedBackfill: backfillBatchSize,
        scannedLive: liveBatchSize,
        ok,
        failed,
        backlogMode: this.runtime.backlogMode,
        backlogReason: this.runtime.backlogReason,
        scanConcurrency: this.runtime.scanConcurrencyCurrent,
        scanPagesPerWallet: this.fullHistoryPerWallet ? null : this.runtime.scanPagesCurrent,
        scanHistoryMode: this.fullHistoryPerWallet ? "full_history" : "capped",
      };
    } finally {
      this.runtime.inScan = false;
      this.runtime.scanPhase = null;
      this.runtime.scanPhaseAt = null;
    }
  }

  async start() {
    if (this.runtime.running) return;
    this.runtime.running = true;
    this.logger.info(
      `[wallet-indexer] start running=true discovery_only=${this.discoveryOnly ? "true" : "false"} scan_interval_ms=${this.scanIntervalMs} discovery_interval_ms=${this.discoveryIntervalMs}`
    );
    this.runtime.lastLegacyVerificationMigration = this.runLegacyRemoteVerificationMigration(
      Math.max(32, Math.min(128, Math.floor(this.historyAuditRepairBatchSize / 8) || 32))
    );
    this.runtime.lastRetryReasonSanitization = this.sanitizeRetryReasons();
    this.scheduleDiscoveryLoop(0);
    this.scheduleLiveRefreshLoop(0);
    if (!this.discoveryOnly) this.scheduleScanLoop(0);
    this.scheduleRepairLoop(Math.max(5000, this.computeRepairDelayMs()));
  }

  stop() {
    this.runtime.running = false;
    if (this.runtime.discoveryTimer) {
      clearTimeout(this.runtime.discoveryTimer);
      this.runtime.discoveryTimer = null;
    }
    if (this.runtime.scanTimer) {
      clearTimeout(this.runtime.scanTimer);
      this.runtime.scanTimer = null;
    }
    if (this.runtime.repairTimer) {
      clearTimeout(this.runtime.repairTimer);
      this.runtime.repairTimer = null;
    }
    if (this.runtime.liveRefreshTimer) {
      clearTimeout(this.runtime.liveRefreshTimer);
      this.runtime.liveRefreshTimer = null;
    }
    this.runtime.nextDiscoveryAt = null;
    this.runtime.nextScanAt = null;
    this.runtime.nextRepairAt = null;
    this.runtime.nextLiveRefreshAt = null;
    if (this.walletStore && typeof this.walletStore.stop === "function") {
      this.walletStore.stop();
    } else if (this.walletStore && typeof this.walletStore.flush === "function") {
      this.walletStore.flush(true);
    }
    this.persistStateIfNeeded(true);
  }

  buildLiveRefreshBatch(now = Date.now()) {
    this.reconcileLiveWalletSet();
    const liveWallets = normalizeWallets(this.state.liveWallets || []);
    if (!liveWallets.length) {
      return {
        activationStrategy: this.computeActivationStrategy(),
        tasks: [],
      };
    }

    const activationStrategy = this.computeActivationStrategy();
    const cyclesPerSweep = Math.max(
      1,
      Math.floor(this.liveRefreshTargetMs / Math.max(1000, this.liveRefreshLoopIntervalMs))
    );
    const targetForSweep = Math.ceil(liveWallets.length / cyclesPerSweep);
    const target = Math.min(
      liveWallets.length,
      clamp(
        Math.max(
          this.liveWalletsPerScanConfigured,
          this.liveWalletsPerScanMin,
          Math.min(this.liveWalletsPerScanMax, this.liveRefreshConcurrency),
          targetForSweep
        ),
        this.liveWalletsPerScanMin,
        this.liveWalletsPerScanMax
      )
    );
    const remoteBacklogHeavy =
      this.runtime.continuationScanQueue.length >= this.maxWalletsPerScan * 2 ||
      this.runtime.priorityScanQueue.length >= this.maxWalletsPerScan;
    const effectiveTarget = remoteBacklogHeavy
      ? Math.min(target, Math.max(8, Math.min(24, this.liveRefreshConcurrency)))
      : target;
    const tasks = [];
    const selected = new Set();
    const addWallet = (wallet, reason) => {
      if (!wallet || selected.has(wallet) || tasks.length >= effectiveTarget) return false;
      const row = this.state.walletStates && this.state.walletStates[wallet];
      const triggerDriven = reason === "live_refresh_trigger";
      const knownWallet =
        Boolean(row) ||
        (this.walletStore &&
          typeof this.walletStore.get === "function" &&
          Boolean(this.walletStore.get(wallet)));
      if (!triggerDriven && !this.isLiveRefreshEligibleRow(row)) return false;
      if (triggerDriven && !knownWallet) return false;
      if (this.isWalletLeaseActive(row, now)) return false;
      selected.add(wallet);
      tasks.push({ wallet, mode: "live", reason });
      return true;
    };

    const triggeredWallets = this.getTriggeredLiveWallets(
      Math.max(effectiveTarget * 3, this.topCorrectionCohortSize * 2),
      now
    );
    const triggerOnlyTarget = Math.min(
      effectiveTarget,
      Math.max(1, Math.min(64, triggeredWallets.length))
    );
    for (
      let i = 0;
      i < triggeredWallets.length && tasks.length < triggerOnlyTarget;
      i += 1
    ) {
      addWallet(triggeredWallets[i], "live_refresh_trigger");
    }
    if (tasks.length > 0) {
      return {
        activationStrategy,
        tasks,
      };
    }

    const topCorrectionWallets = this.rankWalletsForCorrection(
      this.getTopCorrectionWallets(this.topCorrectionCohortSize, false),
      this.topCorrectionCohortSize
    );
    for (
      let i = 0;
      i < topCorrectionWallets.length && tasks.length < effectiveTarget;
      i += 1
    ) {
      addWallet(topCorrectionWallets[i], "live_refresh_top_correction");
    }

    const hotWallets = this.getHotLiveWallets(target * 4);
    for (let i = 0; i < hotWallets.length && tasks.length < effectiveTarget; i += 1) {
      addWallet(hotWallets[i], "live_refresh_hot");
    }

    let cursor = liveWallets.length > 0 ? this.runtime.liveRefreshCursor % liveWallets.length : 0;
    let attempts = 0;
    const maxAttempts = Math.max(1, liveWallets.length * 2);
    const roundRobinCap = remoteBacklogHeavy
      ? Math.max(1, Math.floor(effectiveTarget * 0.2))
      : effectiveTarget;
    let roundRobinAdded = 0;
    while (
      tasks.length < effectiveTarget &&
      roundRobinAdded < roundRobinCap &&
      attempts < maxAttempts &&
      liveWallets.length > 0
    ) {
      const wallet = liveWallets[cursor];
      cursor = (cursor + 1) % liveWallets.length;
      attempts += 1;
      if (addWallet(wallet, "live_refresh_round_robin")) {
        roundRobinAdded += 1;
      }
    }
    this.runtime.liveRefreshCursor = cursor;

    return {
      activationStrategy,
      tasks,
    };
  }

  async liveRefreshCycle() {
    if (this.runtime.inLiveRefresh) return null;
    this.runtime.inLiveRefresh = true;
    this.runtime.lastLiveRefreshStartedAt = Date.now();
    try {
      const { activationStrategy, tasks } = this.buildLiveRefreshBatch(Date.now());
      if (!tasks.length) {
        this.runtime.lastLiveRefreshSummary = {
          at: Date.now(),
          selected: 0,
          ok: 0,
          failed: 0,
        };
        return this.runtime.lastLiveRefreshSummary;
      }

      let ok = 0;
      let failed = 0;
      let cursor = 0;
      const workerCount = Math.max(
        1,
        Math.min(tasks.length, this.liveRefreshConcurrency)
      );
      const workers = Array.from({ length: workerCount }, () =>
        (async () => {
          while (cursor < tasks.length) {
            const task = tasks[cursor];
            cursor += 1;
            if (!task) continue;
            const clientEntry =
              this.selectClientEntryForTask(task, activationStrategy) || null;
            const result = await this.executeScanTask(
              task,
              clientEntry,
              activationStrategy,
              tasks.length
            );
            if (result && result.ok) ok += 1;
            else failed += 1;
          }
        })()
      );
      await Promise.all(workers);

      if (this.walletStore && typeof this.walletStore.flush === "function") {
        this.walletStore.flush(false);
      }
      this.persistStateIfNeeded();
      this.runtime.lastLiveRefreshSummary = {
        at: Date.now(),
        selected: tasks.length,
        workerCount,
        ok,
        failed,
        modeCounts: tasks.reduce((acc, task) => {
          const mode = String((task && task.mode) || "live");
          acc[mode] = Number(acc[mode] || 0) + 1;
          return acc;
        }, {}),
        wallets: tasks.slice(0, 10).map((task) => task.wallet),
      };
      this.logger.log(
        `[wallet-indexer] live_refresh selected=${tasks.length} workers=${workerCount} ok=${ok} failed=${failed}`
      );
      return this.runtime.lastLiveRefreshSummary;
    } finally {
      this.runtime.inLiveRefresh = false;
    }
  }

  getStatus() {
    const diagnostics = this.buildDiagnosticsSummary();
    const backlogEval = this.evaluateBacklogMode(diagnostics);
    const activationStrategy = this.computeActivationStrategy();
    const indexedWallets = this.walletStore ? this.walletStore.list().length : 0;
    const knownWallets = this.state.knownWallets.length;
    const completionPct =
      knownWallets > 0 ? (diagnostics.backfillComplete / knownWallets) * 100 : 0;
    const coverage = this.buildCoverageWindowSummary();

    return {
      running: this.runtime.running,
      inDiscovery: this.runtime.inDiscovery,
      inScan: this.runtime.inScan,
      worker: {
        shardId: this.workerShardId,
        shardIndex: this.workerShardIndex,
        shardCount: this.workerShardCount,
        ownsAllWallets: this.workerShardCount <= 1,
      },
      knownWallets,
      discoveryOnly: this.discoveryOnly,
      indexedWallets,
      completionPct: Number(completionPct.toFixed(4)),
      attemptedWallets: diagnostics.attemptedWallets,
      successfulScans: diagnostics.successfulScans,
      failedScans: diagnostics.failedScans,
      indexedCompleteWallets: diagnostics.indexed,
      partiallyIndexedWallets: diagnostics.partial,
      pendingWallets: diagnostics.pending,
      failedWallets: diagnostics.failed,
      walletBacklog: diagnostics.backlog,
      failedBackfillWallets: diagnostics.failedBackfill,
      lifecycle: {
        discovered: diagnostics.discovered,
        pendingBackfill: diagnostics.pendingBackfill,
        backfilling: diagnostics.backfilling,
        failedBackfill: diagnostics.failedBackfill,
        fullyIndexed: diagnostics.fullyIndexed,
        liveTracking: diagnostics.liveTracking,
        liveTrackingPartial: diagnostics.liveTrackingPartial,
        backfillComplete: diagnostics.backfillComplete,
        verifiedBackfillComplete: diagnostics.verifiedBackfillComplete,
        historyAuditRepairPending: diagnostics.historyAuditRepairPending,
        remoteHistoryVerificationPending: diagnostics.remoteHistoryVerificationPending,
      },
      completionCondition: {
        rule:
          "remote_history_verified=true AND trade_done=true AND funding_done=true AND trade_cursor=null AND funding_cursor=null from a successful scan",
        verifiedBy: [
          "wallet_state.remoteHistoryVerified",
          "wallet_state.tradeDone",
          "wallet_state.fundingDone",
          "wallet_state.tradeCursor",
          "wallet_state.fundingCursor",
          "wallet_state.lastSuccessAt",
        ],
      },
      integrity: {
        verifiedBackfillComplete: diagnostics.verifiedBackfillComplete,
        historyAuditRepairPending: diagnostics.historyAuditRepairPending,
        remoteHistoryVerificationPending: diagnostics.remoteHistoryVerificationPending,
        lastLegacyVerificationMigration: this.runtime.lastLegacyVerificationMigration || null,
        lastHistoryRepairSummary: this.runtime.lastHistoryRepairSummary || null,
        lastContinuationRebalance: this.runtime.lastContinuationRebalance || null,
        topCorrectionCohort: this.buildTopCorrectionCohortSummary(this.topCorrectionCohortSize),
      },
      scanCursor: this.state.scanCursor,
      liveScanCursor: this.state.liveScanCursor,
      scanIntervalMs: this.scanIntervalMs,
      discoveryIntervalMs: this.discoveryIntervalMs,
      maxWalletsPerScan: this.maxWalletsPerScan,
      liveWalletsPerScan: this.liveWalletsPerScanConfigured,
      liveWalletsPerScanMin: this.liveWalletsPerScanMin,
      liveWalletsPerScanMax: this.liveWalletsPerScanMax,
      liveWalletsPerScanAdaptiveMax: Math.max(
        this.liveWalletsPerScanMax,
        this.walletScanConcurrency * 4
      ),
      liveRefreshTargetMs: this.liveRefreshTargetMs,
      liveRefreshConcurrency: this.liveRefreshConcurrency,
      liveMaxPagesPerWallet: this.liveMaxPagesPerWallet,
      maxPagesPerWallet: this.fullHistoryPerWallet ? null : this.maxPagesPerWallet,
      fullHistoryPagesPerScan: this.fullHistoryPerWallet ? this.fullHistoryPagesPerScan : null,
      deepHistoryPagesPerScan: this.fullHistoryPerWallet ? this.deepHistoryPagesPerScan : null,
      fullHistoryPerWallet: this.fullHistoryPerWallet,
      tradesPageLimit: this.tradesPageLimit,
      fundingPageLimit: this.fundingPageLimit,
      activationTuning: {
        ...activationStrategy,
        activationTradesPageLimitConfigured: this.activationTradesPageLimit,
        activationFundingPageLimitConfigured: this.activationFundingPageLimit,
        activationReserveMin: this.activationReserveMin,
        activationReserveMax: this.activationReserveMax,
        continuationReserveMin: this.continuationReserveMin,
        continuationReserveMax: this.continuationReserveMax,
        deepBackfillMinPerScan: this.deepBackfillMinPerScan,
        deepHistoryTradesPageLimit: this.deepHistoryTradesPageLimit,
        deepHistoryFundingPageLimit: this.deepHistoryFundingPageLimit,
        restShardParallelismMax: this.restShardParallelismMax,
      },
      lastDiscoveryAt: this.state.lastDiscoveryAt,
      lastScanAt: this.state.lastScanAt,
      lastScanStartedAt: this.runtime.lastScanStartedAt,
      lastLiveRefreshStartedAt: this.runtime.lastLiveRefreshStartedAt,
      discoveryCycles: this.state.discoveryCycles,
      scanCycles: this.state.scanCycles,
      lastError: this.runtime.lastError,
      priorityQueueSize: this.runtime.priorityScanQueue.length,
      continuationQueueSize: this.runtime.continuationScanQueue.length,
      liveQueueSize: this.runtime.liveScanQueue.length,
      liveGroupSize: normalizeWallets(this.state.liveWallets || []).length,
      averagePendingWaitMs: diagnostics.averagePendingWaitMs,
      averageQueueWaitMs: diagnostics.averageQueueWaitMs,
      liveAverageAgeMs: diagnostics.liveAverageAgeMs,
      liveMaxAgeMs: diagnostics.liveMaxAgeMs,
      liveStaleWallets: diagnostics.liveStaleWallets,
      liveRefreshSnapshot: this.runtime.liveRefreshSnapshot || null,
      liveRefreshLoop: {
        intervalMs: this.liveRefreshLoopIntervalMs,
        nextRunAt: this.runtime.nextLiveRefreshAt,
        lastDurationMs: this.runtime.lastLiveRefreshDurationMs,
        lastSummary: this.runtime.lastLiveRefreshSummary || null,
        running: this.runtime.inLiveRefresh,
      },
      liveTriggerState: {
        enabled: Boolean(
          this.liveTriggerStore && typeof this.liveTriggerStore.readSince === "function"
        ),
        pollMs: this.liveTriggerPollMs,
        ttlMs: this.liveTriggerTtlMs,
        readLimit: this.liveTriggerReadLimit,
        activeWallets:
          this.runtime.liveTriggeredWalletAt instanceof Map
            ? this.runtime.liveTriggeredWalletAt.size
            : 0,
        lastReadAt: Number(this.runtime.lastLiveTriggerReadAt || 0) || null,
        lastObservedAt: Number(this.runtime.lastLiveTriggerObservedAt || 0) || null,
        lastSummary: this.runtime.lastLiveTriggerSummary || null,
      },
      topErrorReasons: diagnostics.topErrorReasons,
      backlogMode: {
        active: backlogEval.active,
        reason: backlogEval.reason,
        runtimeActive: this.runtime.backlogMode,
        runtimeReason: this.runtime.backlogReason,
        enabled: this.backlogModeEnabled,
        thresholdWallets: this.backlogWalletThreshold,
        thresholdAvgWaitMs: this.backlogAvgWaitMsThreshold,
        discoverEveryCycles: this.backlogDiscoverEveryCycles,
      },
      adaptiveScan: {
        concurrencyCurrent: this.runtime.scanConcurrencyCurrent,
        concurrencyMax: this.walletScanConcurrency,
        pagesCurrent: this.fullHistoryPerWallet ? null : this.runtime.scanPagesCurrent,
        pagesMax: this.fullHistoryPerWallet ? null : this.maxPagesPerWallet,
        fullHistoryPagesPerScan: this.fullHistoryPerWallet ? this.fullHistoryPagesPerScan : null,
        deepHistoryPagesPerScan: this.fullHistoryPerWallet ? this.deepHistoryPagesPerScan : null,
        mode: this.fullHistoryPerWallet ? "full_history" : "capped",
      },
      scheduling: this.buildSchedulingSnapshot(),
      scanPhase: this.runtime.scanPhase || null,
      scanPhaseAt: Number(this.runtime.scanPhaseAt || 0) || null,
      coverage,
      restClients: {
        count: this.restClients.length,
        ids:
          this.restClientIds && this.restClientIds.length
            ? this.restClientIds.slice(0, this.restClients.length)
            : null,
        pool: this.buildClientPoolSummary(),
        shards: this.buildRestShardSummary(),
        shardStrategy: this.restShardStrategy,
      },
      source:
        this.walletSource && typeof this.walletSource.getState === "function"
          ? this.walletSource.getState()
          : null,
      onchain:
        this.onchainDiscovery && typeof this.onchainDiscovery.getStatus === "function"
          ? this.onchainDiscovery.getStatus()
          : null,
      onchainPages: {
        current: this.runtime.onchainPagesCurrent,
        configured: this.onchainPagesPerDiscoveryCycle,
        max: this.onchainPagesMaxPerCycle,
      },
    };
  }

  buildDiagnosticsSummary() {
    const rows = Object.values(this.state.walletStates || {});
    const now = Date.now();

    let indexed = 0;
    let partial = 0;
    let pending = 0;
    let failed = 0;
    let failedBackfill = 0;
    let attempted = 0;
    let succeeded = 0;
    let failedAttempts = 0;
    let pendingWaitSum = 0;
    let pendingWaitCount = 0;
    const discovered = rows.length;
    let pendingBackfill = 0;
    let backfilling = 0;
    let fullyIndexed = 0;
    let liveTracking = 0;
    let liveTrackingPartial = 0;
    let activationPending = 0;
    let continuationPending = 0;
    let recentContinuationPending = 0;
    let deepContinuationPending = 0;
    let liveAgeSum = 0;
    let liveAgeCount = 0;
    let liveAgeMax = 0;
    let liveStaleWallets = 0;
    let verifiedBackfillComplete = 0;
    let historyAuditRepairPending = 0;
    let remoteHistoryVerificationPending = 0;

    const reasonCounts = new Map();
    rows.forEach((row) => {
      const status = this.deriveWalletStatus(row);
      const lifecycle = this.deriveWalletLifecycle(row);
      const complete = this.isBackfillCompleteRow(row);
      const needsRepair = this.needsHistoryAuditRepair(row);
      const needsRemoteVerification = this.needsRemoteHistoryVerification(row);
      const hasSuccess =
        Number(row && row.lastSuccessAt ? row.lastSuccessAt : 0) > 0 ||
        Number(row && row.scansSucceeded ? row.scansSucceeded : 0) > 0;
      if (status === "indexed") indexed += 1;
      else if (status === "partial") partial += 1;
      else if (status === "failed") {
        failed += 1;
        if (lifecycle !== WALLET_LIFECYCLE.LIVE_TRACKING) {
          failedBackfill += 1;
        }
      }
      else pending += 1;
      if (lifecycle === WALLET_LIFECYCLE.DISCOVERED || lifecycle === WALLET_LIFECYCLE.PENDING_BACKFILL) pendingBackfill += 1;
      else if (lifecycle === WALLET_LIFECYCLE.BACKFILLING) backfilling += 1;
      else if (lifecycle === WALLET_LIFECYCLE.FULLY_INDEXED) fullyIndexed += 1;
      else if (lifecycle === WALLET_LIFECYCLE.LIVE_TRACKING) {
        liveTracking += 1;
        if (!complete) liveTrackingPartial += 1;
        const anchor = Number(
          (row &&
            (row.liveLastScanAt ||
              row.lastSuccessAt ||
              row.liveTrackingSince ||
              row.backfillCompletedAt ||
              row.discoveredAt)) ||
            0
        );
        if (anchor > 0) {
          const age = Math.max(0, now - anchor);
          liveAgeSum += age;
          liveAgeCount += 1;
          if (age > liveAgeMax) liveAgeMax = age;
          if (age >= this.liveRefreshTargetMs) liveStaleWallets += 1;
        } else {
          liveStaleWallets += 1;
        }
      }

      if (complete && !needsRepair) verifiedBackfillComplete += 1;
      if (needsRepair) historyAuditRepairPending += 1;
      if (needsRemoteVerification) remoteHistoryVerificationPending += 1;

      if (!complete) {
        if (!hasSuccess) {
          activationPending += 1;
        } else {
          continuationPending += 1;
          const historyPhase = String((row && row.historyPhase) || "deep").trim().toLowerCase();
          if (historyPhase === "recent") recentContinuationPending += 1;
          else deepContinuationPending += 1;
        }
      }

      const scansSucceeded = Number(row && row.scansSucceeded ? row.scansSucceeded : 0);
      const scansFailed = Number(row && row.scansFailed ? row.scansFailed : 0);
      const attemptsForRow = scansSucceeded + scansFailed;
      const hasLegacyAttempt = Boolean(
        row &&
          (row.lastAttemptAt || row.lastScannedAt || row.lastSuccessAt || row.lastFailureAt || row.lastError)
      );
      if (attemptsForRow > 0 || hasLegacyAttempt) {
        attempted += 1;
      }
      succeeded += scansSucceeded;
      failedAttempts += scansFailed > 0 ? scansFailed : row && row.lastError ? 1 : 0;

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
            : summarizeErrorReason(row && row.lastError ? row.lastError : "");
        reasonCounts.set(reason, (reasonCounts.get(reason) || 0) + 1);
      }
    });

    const queueWaitValues = [...this.runtime.priorityScanQueue, ...this.runtime.continuationScanQueue]
      .map((wallet) => {
        const at = Number(
          this.runtime.priorityEnqueuedAt.get(wallet) ||
            this.runtime.continuationEnqueuedAt.get(wallet) ||
            0
        );
        return at > 0 ? Math.max(0, now - at) : null;
      })
      .filter((value) => Number.isFinite(value));

    const topErrorReasons = Array.from(reasonCounts.entries())
      .map(([reason, count]) => ({ reason, count }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 10);

    return {
      indexed,
      partial,
      pending,
      failed,
      failedBackfill,
      backlog: pendingBackfill + backfilling + liveTrackingPartial + failedBackfill,
      discovered,
      pendingBackfill,
      backfilling,
      activationPending,
      continuationPending,
      recentContinuationPending,
      deepContinuationPending,
      fullyIndexed,
      liveTracking,
      liveTrackingPartial,
      backfillComplete: rows.reduce(
        (sum, row) => sum + (this.isBackfillCompleteRow(row) ? 1 : 0),
        0
      ),
      verifiedBackfillComplete,
      historyAuditRepairPending,
      remoteHistoryVerificationPending,
      averagePendingWaitMs:
        pendingWaitCount > 0 ? Math.round(pendingWaitSum / pendingWaitCount) : 0,
      averageQueueWaitMs:
        queueWaitValues.length > 0
          ? Math.round(queueWaitValues.reduce((sum, v) => sum + v, 0) / queueWaitValues.length)
          : 0,
      liveAverageAgeMs: liveAgeCount > 0 ? Math.round(liveAgeSum / liveAgeCount) : 0,
      liveMaxAgeMs: liveAgeMax,
      liveStaleWallets,
      topErrorReasons,
      attemptedWallets: attempted,
      successfulScans: succeeded,
      failedScans: failedAttempts,
    };
  }

  getWalletLifecycleSnapshot(wallet) {
    const normalized = normalizeAddress(wallet);
    if (!normalized) return null;
    const row = this.state.walletStates && this.state.walletStates[normalized];
    if (!row) return null;
    const lifecycleStage = this.deriveWalletLifecycle(row);
    return {
      wallet: normalized,
      status: this.deriveWalletStatus(row),
      lifecycleStage,
      backfillComplete: this.isBackfillCompleteRow(row),
      discoveredAt: row.discoveredAt || null,
      backfillCompletedAt: row.backfillCompletedAt || null,
      liveTrackingSince: row.liveTrackingSince || null,
      liveLastScanAt: row.liveLastScanAt || null,
      restShardId: row.restShardId || null,
      restShardIndex:
        Number.isFinite(Number(row.restShardIndex)) ? Number(row.restShardIndex) : null,
      lastRestClientId: row.lastRestClientId || null,
      lastRestClientUsedAt: row.lastRestClientUsedAt || null,
      lastAttemptAt: row.lastAttemptAt || null,
      lastSuccessAt: row.lastSuccessAt || null,
      lastFailureAt: row.lastFailureAt || null,
      lastErrorReason: row.lastErrorReason || null,
    };
  }

  getWalletLifecycleMap(wallets = []) {
    const out = {};
    normalizeWallets(wallets).forEach((wallet) => {
      const snapshot = this.getWalletLifecycleSnapshot(wallet);
      if (snapshot) out[wallet] = snapshot;
    });
    return out;
  }

  buildRestShardSummary() {
    const assignedByShard = new Map();
    Object.values(this.state.walletStates || {}).forEach((row) => {
      if (!row || !row.wallet) return;
      const shardId = normalizeClientId(row.restShardId) || "unassigned";
      if (!assignedByShard.has(shardId)) {
        assignedByShard.set(shardId, {
          shardId,
          assignedWallets: 0,
          liveWallets: 0,
          queuedWallets: 0,
          backfillWallets: 0,
        });
      }
      const bucket = assignedByShard.get(shardId);
      bucket.assignedWallets += 1;
      const lifecycle = this.deriveWalletLifecycle(row);
      if (lifecycle === WALLET_LIFECYCLE.LIVE_TRACKING) bucket.liveWallets += 1;
      else bucket.backfillWallets += 1;
      if (
        this.runtime.priorityScanSet.has(row.wallet) ||
        this.runtime.continuationScanSet.has(row.wallet)
      ) {
        bucket.queuedWallets += 1;
      }
    });

    const queueSnapshotMap = new Map(
      (Array.isArray(this.runtime.shardQueueSnapshot) ? this.runtime.shardQueueSnapshot : []).map((row) => [
        normalizeClientId(row && row.shardId) || "unassigned",
        Number(row && row.queued ? row.queued : 0),
      ])
    );

    return this.clientPoolState.map((client, idx) => {
      const shardId = client.id;
      const counts =
        assignedByShard.get(shardId) || {
          shardId,
          assignedWallets: 0,
          liveWallets: 0,
          queuedWallets: 0,
          backfillWallets: 0,
        };
      return {
        shardId,
        shardIndex: idx,
        proxyUrl: client.proxyUrl || null,
        assignedWallets: counts.assignedWallets,
        liveWallets: counts.liveWallets,
        queuedWallets: counts.queuedWallets,
        backfillWallets: counts.backfillWallets,
        cycleQueuedWallets: queueSnapshotMap.get(shardId) || 0,
        inFlight: Number(client.inFlight || 0),
        cooldownRemainingMs: Math.max(0, Number(client.cooldownUntil || 0) - Date.now()),
      };
    });
  }

  getClientSelectionMetrics(clientEntry, now = Date.now()) {
    const requests = Math.max(0, Number(clientEntry && clientEntry.requests ? clientEntry.requests : 0));
    const successes = Math.max(
      0,
      Number(clientEntry && clientEntry.successes ? clientEntry.successes : 0)
    );
    const failures = Math.max(0, Number(clientEntry && clientEntry.failures ? clientEntry.failures : 0));
    const timeoutFailures = Math.max(
      0,
      Number(clientEntry && clientEntry.timeoutFailures ? clientEntry.timeoutFailures : 0)
    );
    const networkFailures = Math.max(
      0,
      Number(clientEntry && clientEntry.networkFailures ? clientEntry.networkFailures : 0)
    );
    const latencyMsAvg = Math.max(
      0,
      Number(clientEntry && clientEntry.latencyMsAvg ? clientEntry.latencyMsAvg : 0)
    );
    const weight = clamp(Number((clientEntry && clientEntry.weight) || 1), 0.2, 4);
    const failureRatio = requests > 0 ? failures / requests : 0;
    const timeoutRatio = failures > 0 ? timeoutFailures / failures : 0;
    const networkRatio = failures > 0 ? networkFailures / failures : 0;
    const successRate = requests > 0 ? successes / requests : 1;
    const coolingMs = Math.max(
      0,
      Number(clientEntry && clientEntry.cooldownUntil ? clientEntry.cooldownUntil : 0) - now
    );
    const rateState =
      clientEntry && clientEntry.rateGuard && typeof clientEntry.rateGuard.getState === "function"
        ? clientEntry.rateGuard.getState()
        : null;
    const rpmCap = Number((rateState && rateState.rpmCap) || 0);
    const rpmUsed = Number((rateState && rateState.used1m) || 0);
    const rpmUtilization = rpmCap > 0 ? clamp(rpmUsed / rpmCap, 0, 1) : 0;
    return {
      requests,
      successes,
      failures,
      timeoutFailures,
      networkFailures,
      latencyMsAvg,
      weight,
      failureRatio,
      timeoutRatio,
      networkRatio,
      successRate,
      coolingMs,
      rpmCap,
      rpmUsed,
      rpmUtilization,
      backfillCoolingMs: Math.max(
        0,
        Number(clientEntry && clientEntry.backfillCooldownUntil ? clientEntry.backfillCooldownUntil : 0) -
          now
      ),
    };
  }

  rollClientQualityWindows(clientEntry, now = Date.now()) {
    if (!clientEntry) return [];
    const windows = Array.isArray(clientEntry.qualityWindows)
      ? clientEntry.qualityWindows.filter((row) => row && typeof row === "object")
      : [];
    const currentStart =
      Math.floor(Number(now || Date.now()) / this.backfillQualityWindowMs) *
      this.backfillQualityWindowMs;
    let current = windows.length ? windows[windows.length - 1] : null;
    if (!current || Number(current.startAt || 0) !== currentStart) {
      current = {
        startAt: currentStart,
        requests: 0,
        successes: 0,
        failures: 0,
        timeoutFailures: 0,
        networkFailures: 0,
        latencyMsSum: 0,
        latencySamples: 0,
      };
      windows.push(current);
    }
    while (windows.length > this.backfillQualityWindowCount) {
      windows.shift();
    }
    clientEntry.qualityWindows = windows;
    return windows;
  }

  recordClientQualitySample(clientEntry, sample = {}) {
    if (!clientEntry) return null;
    const now = Number(sample.now || Date.now());
    const windows = this.rollClientQualityWindows(clientEntry, now);
    const current = windows.length ? windows[windows.length - 1] : null;
    if (!current) return null;
    current.requests = Math.max(0, Number(current.requests || 0)) + 1;
    if (sample.success) {
      current.successes = Math.max(0, Number(current.successes || 0)) + 1;
    } else {
      current.failures = Math.max(0, Number(current.failures || 0)) + 1;
      const reason = String(sample.reason || "").trim().toLowerCase();
      if (reason === "timeout") {
        current.timeoutFailures = Math.max(0, Number(current.timeoutFailures || 0)) + 1;
      }
      if (
        reason === "network_error" ||
        reason === "connection_reset" ||
        reason === "proxy_error"
      ) {
        current.networkFailures = Math.max(0, Number(current.networkFailures || 0)) + 1;
      }
    }
    if (Number.isFinite(Number(sample.latencyMs)) && Number(sample.latencyMs) > 0) {
      current.latencyMsSum =
        Math.max(0, Number(current.latencyMsSum || 0)) + Number(sample.latencyMs);
      current.latencySamples = Math.max(0, Number(current.latencySamples || 0)) + 1;
    }
    return current;
  }

  getClientRecentWindowMetrics(clientEntry, now = Date.now()) {
    if (!clientEntry) {
      return {
        windowCount: 0,
        activeWindowCount: 0,
        requests: 0,
        failures: 0,
        timeoutFailures: 0,
        networkFailures: 0,
        failureRatio: 0,
        timeoutRatio: 0,
        networkRatio: 0,
        latencyMsAvg: 0,
        highLatencyWindows: 0,
        badWindows: 0,
      };
    }
    const windows = this.rollClientQualityWindows(clientEntry, now);
    const activeWindows = windows.filter((row) => Number(row.requests || 0) > 0);
    const requests = activeWindows.reduce((sum, row) => sum + Number(row.requests || 0), 0);
    const failures = activeWindows.reduce((sum, row) => sum + Number(row.failures || 0), 0);
    const timeoutFailures = activeWindows.reduce(
      (sum, row) => sum + Number(row.timeoutFailures || 0),
      0
    );
    const networkFailures = activeWindows.reduce(
      (sum, row) => sum + Number(row.networkFailures || 0),
      0
    );
    const latencyMsSum = activeWindows.reduce(
      (sum, row) => sum + Number(row.latencyMsSum || 0),
      0
    );
    const latencySamples = activeWindows.reduce(
      (sum, row) => sum + Number(row.latencySamples || 0),
      0
    );
    const latencyMsAvg = latencySamples > 0 ? latencyMsSum / latencySamples : 0;
    const highLatencyWindows = activeWindows.filter((row) => {
      const samples = Number(row.latencySamples || 0);
      const avg = samples > 0 ? Number(row.latencyMsSum || 0) / samples : 0;
      return samples > 0 && avg >= this.backfillProxyStrictMaxLatencyMs;
    }).length;
    const badWindows = activeWindows.filter((row) => {
      const rowRequests = Math.max(0, Number(row.requests || 0));
      const rowFailures = Math.max(0, Number(row.failures || 0));
      const rowTimeouts = Math.max(0, Number(row.timeoutFailures || 0));
      const rowNetwork = Math.max(0, Number(row.networkFailures || 0));
      const failureRatio = rowRequests > 0 ? rowFailures / rowRequests : 0;
      const timeoutRatio = rowFailures > 0 ? rowTimeouts / rowFailures : 0;
      const networkRatio = rowFailures > 0 ? rowNetwork / rowFailures : 0;
      return (
        failureRatio >= this.backfillProxyStrictMaxFailureRatio ||
        timeoutRatio >= this.backfillProxyStrictMaxTimeoutRatio ||
        networkRatio >= this.backfillProxyStrictMaxNetworkRatio
      );
    }).length;
    return {
      windowCount: windows.length,
      activeWindowCount: activeWindows.length,
      requests,
      failures,
      timeoutFailures,
      networkFailures,
      failureRatio: requests > 0 ? failures / requests : 0,
      timeoutRatio: failures > 0 ? timeoutFailures / failures : 0,
      networkRatio: failures > 0 ? networkFailures / failures : 0,
      latencyMsAvg,
      highLatencyWindows,
      badWindows,
    };
  }

  updateClientBackfillPenalty(clientEntry, now = Date.now()) {
    if (!clientEntry) return null;
    const recent = this.getClientRecentWindowMetrics(clientEntry, now);
    if (recent.activeWindowCount < 2) return recent;

    let cooldownUntil = Number(clientEntry.backfillCooldownUntil || 0);
    if (
      recent.highLatencyWindows >= this.backfillWindowHighLatencyCountThreshold &&
      recent.latencyMsAvg >= this.backfillProxyStrictMaxLatencyMs
    ) {
      cooldownUntil = Math.max(cooldownUntil, now + this.backfillWindowCooldownMs);
      clientEntry.weight = Number(
        clamp(Number(clientEntry.weight || 1) * 0.82, 0.2, 4).toFixed(3)
      );
    }
    if (recent.badWindows >= this.backfillWindowBadCountThreshold) {
      cooldownUntil = Math.max(cooldownUntil, now + this.backfillWindowCooldownMs * 1.5);
      clientEntry.weight = Number(
        clamp(Number(clientEntry.weight || 1) * 0.72, 0.2, 4).toFixed(3)
      );
    }
    clientEntry.backfillCooldownUntil = cooldownUntil;
    return recent;
  }

  buildDeepBackfillSelectionProfile(activationStrategy = null, mode = "backfill") {
    const normalizedMode = String(mode || "backfill").trim().toLowerCase();
    const safe = activationStrategy && typeof activationStrategy === "object" ? activationStrategy : null;
    const continuationPending = Math.max(
      0,
      Number(safe && safe.continuationPending !== undefined ? safe.continuationPending : 0)
    );
    const headroomRatio = Number(safe && safe.headroomRatio !== undefined ? safe.headroomRatio : 0);
    const strictCatchup =
      normalizedMode === "backfill" &&
      this.shouldPreferHealthyBackfillPool(safe) &&
      continuationPending >= this.backfillProxyStrictContinuationThreshold &&
      headroomRatio >= 0.35;
    const veryStrictCatchup =
      strictCatchup &&
      continuationPending >= this.backfillProxyStrictContinuationThreshold * 1.8 &&
      headroomRatio >= 0.5;

    const baseProfile = {
      minRequests: this.backfillProxyMinRequests,
      minWeight: this.backfillProxyMinWeight,
      maxLatencyMs: this.backfillProxyMaxLatencyMs,
      maxFailureRatio: this.backfillProxyMaxFailureRatio,
      maxTimeoutRatio: this.backfillProxyMaxTimeoutRatio,
      maxNetworkRatio: this.backfillProxyMaxNetworkRatio,
      strictCatchup,
      veryStrictCatchup,
      penaltyScale: normalizedMode === "recent" ? 1.12 : 1,
    };

    if (!strictCatchup) {
      return baseProfile;
    }

    return {
      ...baseProfile,
      minWeight: veryStrictCatchup
        ? Math.max(baseProfile.minWeight, this.backfillProxyStrictMinWeight)
        : this.backfillProxyStrictMinWeight,
      minActiveWindows: veryStrictCatchup
        ? Math.max(this.backfillProxyStrictMinActiveWindows, 3)
        : this.backfillProxyStrictMinActiveWindows,
      warmupMinRequests: veryStrictCatchup
        ? Math.max(this.backfillProxyStrictWarmupMinRequests, 10)
        : this.backfillProxyStrictWarmupMinRequests,
      maxLatencyMs: veryStrictCatchup
        ? Math.max(8000, Math.floor(this.backfillProxyStrictMaxLatencyMs * 0.95))
        : this.backfillProxyStrictMaxLatencyMs,
      maxFailureRatio: veryStrictCatchup
        ? Math.max(0.16, this.backfillProxyStrictMaxFailureRatio * 0.95)
        : this.backfillProxyStrictMaxFailureRatio,
      maxTimeoutRatio: veryStrictCatchup
        ? Math.max(0.12, this.backfillProxyStrictMaxTimeoutRatio * 0.95)
        : this.backfillProxyStrictMaxTimeoutRatio,
      maxNetworkRatio: veryStrictCatchup
        ? Math.max(0.08, this.backfillProxyStrictMaxNetworkRatio * 0.95)
        : this.backfillProxyStrictMaxNetworkRatio,
      penaltyScale: veryStrictCatchup ? 1.18 : 1.08,
    };
  }

  isClientPreferredForDeepBackfill(clientEntry, now = Date.now(), selectionProfile = null) {
    if (!clientEntry) return false;
    const metrics = this.getClientSelectionMetrics(clientEntry, now);
    const profile =
      selectionProfile && typeof selectionProfile === "object"
        ? selectionProfile
        : this.buildDeepBackfillSelectionProfile(null, "backfill");
    if (metrics.coolingMs > 0) return false;
    if (metrics.backfillCoolingMs > 0) return false;
    if (metrics.rpmUtilization >= 0.95) return false;
    const recent = this.getClientRecentWindowMetrics(clientEntry, now);
    if (
      recent.activeWindowCount >= 2 &&
      recent.highLatencyWindows >= this.backfillWindowHighLatencyCountThreshold
    ) {
      return false;
    }
    if (
      recent.activeWindowCount >= 2 &&
      recent.badWindows >= this.backfillWindowBadCountThreshold
    ) {
      return false;
    }
    if (profile.strictCatchup) {
      const enoughWindowHistory =
        recent.activeWindowCount >= Math.max(1, Number(profile.minActiveWindows || 1));
      const enoughWarmupRequests =
        Math.max(metrics.requests, recent.requests) >=
        Math.max(1, Number(profile.warmupMinRequests || 1));
      if (!enoughWindowHistory && !enoughWarmupRequests) {
        return false;
      }
    }
    if (metrics.requests < profile.minRequests) {
      if (
        metrics.latencyMsAvg > 0 &&
        metrics.latencyMsAvg > profile.maxLatencyMs * 1.1
      ) {
        return false;
      }
      if (
        recent.activeWindowCount >= 2 &&
        recent.latencyMsAvg > 0 &&
        recent.latencyMsAvg >= profile.maxLatencyMs
      ) {
        return false;
      }
      return metrics.weight >= Math.max(0.55, profile.minWeight - 0.15);
    }
    if (metrics.weight < profile.minWeight) return false;
    if (metrics.failureRatio >= profile.maxFailureRatio) return false;
    if (metrics.timeoutRatio >= profile.maxTimeoutRatio) return false;
    if (metrics.networkRatio >= profile.maxNetworkRatio) return false;
    if (recent.activeWindowCount >= 2) {
      if (recent.failureRatio >= profile.maxFailureRatio) return false;
      if (recent.timeoutRatio >= profile.maxTimeoutRatio) return false;
      if (recent.networkRatio >= profile.maxNetworkRatio) return false;
    }
    if (
      metrics.latencyMsAvg > 0 &&
      metrics.latencyMsAvg >= profile.maxLatencyMs
    ) {
      return false;
    }
    if (
      recent.activeWindowCount >= 2 &&
      recent.latencyMsAvg > 0 &&
      recent.latencyMsAvg >= profile.maxLatencyMs
    ) {
      return false;
    }
    return true;
  }

  shouldPreferHealthyBackfillPool(activationStrategy = null) {
    const safe = activationStrategy && typeof activationStrategy === "object" ? activationStrategy : null;
    const discoveryComplete = Boolean(safe && safe.discoveryComplete);
    const continuationPending = Math.max(
      0,
      Number(safe && safe.continuationPending !== undefined ? safe.continuationPending : 0)
    );
    const headroomRatio = Number(safe && safe.headroomRatio !== undefined ? safe.headroomRatio : 0);
    return (
      discoveryComplete &&
      continuationPending > this.maxWalletsPerScan * 4 &&
      headroomRatio >= 0.35
    );
  }

  getHealthyBackfillClientIds(options = {}) {
    const excludeIds = options && options.excludeIds instanceof Set ? options.excludeIds : null;
    const now = Number(options && options.now ? options.now : Date.now());
    const selectionProfile =
      options && options.selectionProfile && typeof options.selectionProfile === "object"
        ? options.selectionProfile
        : null;
    const ids = new Set();
    this.clientPoolState.forEach((entry) => {
      if (!entry || (excludeIds && excludeIds.has(entry.id))) return;
      if (this.isClientPreferredForDeepBackfill(entry, now, selectionProfile)) {
        ids.add(entry.id);
      }
    });
    if (
      ids.size === 0 &&
      selectionProfile &&
      selectionProfile.strictCatchup
    ) {
      const ranked = [];
      this.clientPoolState.forEach((entry) => {
        if (!entry || (excludeIds && excludeIds.has(entry.id))) return;
        const metrics = this.getClientSelectionMetrics(entry, now);
        const recent = this.getClientRecentWindowMetrics(entry, now);
        const warmed =
          recent.activeWindowCount >= 1 ||
          Math.max(metrics.requests, recent.requests) >=
            Math.max(3, Math.ceil(Number(selectionProfile.warmupMinRequests || 6) / 2));
        if (!warmed) return;
        if (metrics.coolingMs > 0 || metrics.backfillCoolingMs > 0) return;
        if (metrics.rpmUtilization >= 0.95) return;
        if (metrics.networkRatio >= 0.6 || metrics.timeoutRatio >= 0.8) return;
        if (recent.badWindows >= Math.max(3, this.backfillWindowBadCountThreshold + 1)) return;
        if (
          recent.activeWindowCount >= 2 &&
          recent.highLatencyWindows >=
            Math.max(3, this.backfillWindowHighLatencyCountThreshold + 1)
        ) {
          return;
        }
        const score =
          metrics.failureRatio * 18 +
          metrics.timeoutRatio * 26 +
          metrics.networkRatio * 30 +
          (metrics.latencyMsAvg > 0
            ? metrics.latencyMsAvg / Math.max(2500, Number(selectionProfile.maxLatencyMs || 14000))
            : 0) *
            10 +
          recent.badWindows * 6 +
          recent.highLatencyWindows * 5 +
          2 / Math.max(0.3, Number(metrics.weight || 1));
        ranked.push({ id: entry.id, score });
      });
      ranked
        .sort((a, b) => a.score - b.score)
        .slice(0, Math.max(this.backfillProxyHealthyFloor, Math.min(24, ranked.length)))
        .forEach((row) => ids.add(row.id));
    }
    return ids;
  }

  computeDeepBackfillPerProxyCap(deepTaskCount, healthyProxyCount, activationStrategy = null) {
    const tasks = Math.max(0, Number(deepTaskCount || 0));
    const proxies = Math.max(1, Number(healthyProxyCount || 0));
    if (tasks <= 0) return 0;
    const safe = activationStrategy && typeof activationStrategy === "object" ? activationStrategy : null;
    const continuationPending = Math.max(
      0,
      Number(safe && safe.continuationPending !== undefined ? safe.continuationPending : 0)
    );
    const strictBackfill =
      continuationPending >= this.backfillProxyStrictContinuationThreshold;
    const share = strictBackfill
      ? Math.max(0.06, this.deepBackfillMaxSharePerProxy * 0.72)
      : this.deepBackfillMaxSharePerProxy;
    const approxRequestsPerTask = Math.max(
      2,
      Number(this.deepHistoryPagesPerScan || 1) *
        2 *
        (strictBackfill
          ? Math.max(1.8, this.historyRequestMaxAttempts * 0.75)
          : Math.max(1.35, this.historyRequestMaxAttempts * 0.55))
    );
    const requestTargetPerProxy = strictBackfill ? 30 : 42;
    const requestAwareCap = Math.max(
      1,
      Math.floor(requestTargetPerProxy / approxRequestsPerTask)
    );
    const sharedCap = Math.ceil(tasks * share);
    const balancedCap = Math.ceil(tasks / proxies);
    const minTasksFloor = strictBackfill ? 1 : this.deepBackfillMinTasksPerProxy;
    return Math.max(
      minTasksFloor,
      Math.min(
        this.deepBackfillMaxTasksPerProxy,
        strictBackfill
          ? Math.min(sharedCap, Math.max(1, balancedCap + 1), requestAwareCap)
          : Math.min(
              Math.max(sharedCap, balancedCap),
              Math.max(requestAwareCap, minTasksFloor)
            )
      )
    );
  }

  pickRestClientEntry(options = {}) {
    if (!this.clientPoolState.length) return null;
    if (this.clientPoolState.length === 1) return this.clientPoolState[0];

    const excludeIds =
      options && options.excludeIds instanceof Set ? options.excludeIds : null;
    const mode = String((options && options.mode) || "generic").trim().toLowerCase();
    const requireHealthyBackfill =
      Boolean(options && options.requireHealthyBackfill) &&
      (mode === "backfill" || mode === "recent");
    const now = Date.now();
    const selectionProfile =
      requireHealthyBackfill
        ? this.buildDeepBackfillSelectionProfile(
            options && options.activationStrategy ? options.activationStrategy : null,
            mode
          )
        : null;
    const strictOnly = Boolean(options && options.strictOnly) && requireHealthyBackfill;
    const strictBackfillIds = requireHealthyBackfill
      ? this.getHealthyBackfillClientIds({ excludeIds, now, selectionProfile })
      : null;
    const strictBackfillPass =
      strictBackfillIds && strictBackfillIds.size > 0
        ? strictBackfillIds
        : null;
    const selectionPasses = strictBackfillPass
      ? strictOnly
        ? [strictBackfillPass]
        : [strictBackfillPass, null]
      : [null];

    for (const candidateIds of selectionPasses) {
      let best = null;
      let bestScore = Number.POSITIVE_INFINITY;
      const start = this.runtime.clientPickCursor % this.clientPoolState.length;

      for (let i = 0; i < this.clientPoolState.length; i += 1) {
        const idx = (start + i) % this.clientPoolState.length;
        const row = this.clientPoolState[idx];
        if (excludeIds && excludeIds.has(row.id)) continue;
        if (candidateIds && !candidateIds.has(row.id)) continue;
        const metrics = this.getClientSelectionMetrics(row, now);
        const recent = this.getClientRecentWindowMetrics(row, now);
        if (metrics.coolingMs > 0) continue;
        const latencyPenalty =
          metrics.latencyMsAvg > 0
            ? clamp(
                metrics.latencyMsAvg /
                  (mode === "backfill" || mode === "recent"
                    ? Math.max(
                        2200,
                        Math.round(
                          Number(
                            (selectionProfile && selectionProfile.maxLatencyMs) ||
                              this.backfillProxyMaxLatencyMs
                          ) / 4
                        )
                      )
                    : 5000),
                0,
                mode === "backfill" || mode === "recent"
                  ? Math.max(
                      12,
                      Math.round(12 * Number((selectionProfile && selectionProfile.penaltyScale) || 1))
                    )
                  : 8
              )
            : 0;
        const recentLatencyPenalty =
          recent.activeWindowCount >= 2 && recent.latencyMsAvg > 0
            ? clamp(
                recent.latencyMsAvg /
                  Math.max(
                    2200,
                    Math.round(
                      Number(
                        (selectionProfile && selectionProfile.maxLatencyMs) ||
                          this.backfillProxyMaxLatencyMs
                      ) / 4
                    )
                  ),
                0,
                14
              )
            : 0;
        const hardFailurePenalty =
          metrics.requests >= 20 && metrics.successes <= 1
            ? 500
            : metrics.requests >= 40 && metrics.failureRatio >= 0.75
            ? 250
            : 0;
        const ratePenalty =
          metrics.rpmUtilization >= 0.98
            ? 50
            : metrics.rpmUtilization >= 0.95
            ? 18
            : metrics.rpmUtilization >= 0.9
            ? 8
            : metrics.rpmUtilization * 3;
        const timeoutPenaltyFactor =
          mode === "backfill"
            ? 24 * Number((selectionProfile && selectionProfile.penaltyScale) || 1)
            : mode === "recent"
            ? 18 * Number((selectionProfile && selectionProfile.penaltyScale) || 1)
            : 10;
        const networkPenaltyFactor =
          mode === "backfill"
            ? 28 * Number((selectionProfile && selectionProfile.penaltyScale) || 1)
            : mode === "recent"
            ? 20 * Number((selectionProfile && selectionProfile.penaltyScale) || 1)
            : 14;
        const failurePenaltyFactor =
          mode === "backfill"
            ? 18 * Number((selectionProfile && selectionProfile.penaltyScale) || 1)
            : mode === "recent"
            ? 16 * Number((selectionProfile && selectionProfile.penaltyScale) || 1)
            : 14;
        const inFlightPenaltyFactor =
          mode === "backfill" ? 4.2 : mode === "recent" ? 4.6 : 5;
        const score =
          Number(row.inFlight || 0) * (inFlightPenaltyFactor / metrics.weight) +
          Number(row.consecutiveFailures || 0) * (mode === "backfill" ? 5 : 4) +
          Number(row.consecutive429 || 0) * 8 +
          metrics.failureRatio * failurePenaltyFactor +
          metrics.timeoutRatio * timeoutPenaltyFactor +
          metrics.networkRatio * networkPenaltyFactor +
          recent.highLatencyWindows *
            (mode === "backfill" ? 10 : mode === "recent" ? 7 : 0) +
          recent.badWindows *
            (mode === "backfill" ? 12 : mode === "recent" ? 8 : 0) +
          hardFailurePenalty +
          ratePenalty +
          latencyPenalty +
          recentLatencyPenalty +
          2 / metrics.weight +
          Math.random() * 0.5;
        if (score < bestScore) {
          bestScore = score;
          best = row;
        }
      }

      if (best) {
        this.runtime.clientPickCursor =
          (this.clientPoolState.findIndex((row) => row.id === best.id) + 1) %
          this.clientPoolState.length;
        return best;
      }
    }

    let earliest = null;
    for (let i = 0; i < this.clientPoolState.length; i += 1) {
      const row = this.clientPoolState[i];
      if (excludeIds && excludeIds.has(row.id)) continue;
      if (!earliest || Number(row.cooldownUntil || 0) < Number(earliest.cooldownUntil || 0)) {
        earliest = row;
      }
    }
    if (!earliest) {
      earliest = this.clientPoolState[0];
    }
    this.runtime.clientPickCursor =
      (this.clientPoolState.findIndex((row) => row.id === earliest.id) + 1) %
      this.clientPoolState.length;
    return earliest;
  }

  selectClientEntryForTask(task, activationStrategy = null, options = {}) {
    const safeTask = task && typeof task === "object" ? task : {};
    const mode = String(safeTask.mode || "backfill").trim().toLowerCase();
    const reason = String(safeTask.reason || "").trim().toLowerCase();
    const extraExcludeIds =
      options && options.excludeIds instanceof Set ? options.excludeIds : null;
    const strictOnly = Boolean(options && options.strictOnly);
    const preferred =
      this.getClientEntryById(safeTask.restShardId) ||
      this.getClientEntryById(safeTask.executionRestShardId) ||
      null;
    const preferHealthyLive = mode === "live" && reason === "live_refresh_trigger";
    const preferHealthyBackfill =
      (mode === "backfill" || mode === "recent") &&
      this.shouldPreferHealthyBackfillPool(activationStrategy);
    if (preferHealthyLive) {
      return (
        this.pickRestClientEntry({ excludeIds: extraExcludeIds, mode }) ||
        (preferred && !(extraExcludeIds && extraExcludeIds.has(preferred.id))
          ? preferred
          : null) ||
        this.clientPoolState[0] ||
        null
      );
    }
    const selectionProfile = preferHealthyBackfill
      ? this.buildDeepBackfillSelectionProfile(activationStrategy, mode)
      : null;
    if (preferHealthyBackfill) {
      if (
        preferred &&
        !(extraExcludeIds && extraExcludeIds.has(preferred.id)) &&
        this.isClientPreferredForDeepBackfill(preferred, Date.now(), selectionProfile)
      ) {
        return preferred;
      }
      return (
        this.pickRestClientEntry({
          excludeIds: extraExcludeIds,
          mode,
          requireHealthyBackfill: true,
          activationStrategy,
          strictOnly,
        }) ||
        (!strictOnly &&
        preferred &&
        !(extraExcludeIds && extraExcludeIds.has(preferred.id))
          ? preferred
          : null) ||
        (!strictOnly
          ? this.pickRestClientEntry({ excludeIds: extraExcludeIds, mode })
          : null) ||
        (!strictOnly ? this.clientPoolState[0] || null : null) ||
        null
      );
    }
    return (
      (preferred && !(extraExcludeIds && extraExcludeIds.has(preferred.id)) ? preferred : null) ||
      this.pickRestClientEntry({ excludeIds: extraExcludeIds, mode }) ||
      this.clientPoolState[0] ||
      null
    );
  }

  observeClientRequestStart(clientEntry) {
    if (!clientEntry) return;
    clientEntry.inFlight = Math.max(0, Number(clientEntry.inFlight || 0)) + 1;
    clientEntry.requests = Math.max(0, Number(clientEntry.requests || 0)) + 1;
    clientEntry.lastUsedAt = Date.now();
  }

  observeClientRequestSuccess(clientEntry, latencyMs) {
    if (!clientEntry) return;
    clientEntry.successes = Math.max(0, Number(clientEntry.successes || 0)) + 1;
    clientEntry.consecutiveFailures = 0;
    clientEntry.consecutive429 = 0;
    const sampleCount = Math.max(0, Number(clientEntry.latencySampleCount || 0)) + 1;
    const prevAvg = Number(clientEntry.latencyMsAvg || 0);
    const nextAvg =
      sampleCount === 1
        ? Number(latencyMs || 0)
        : prevAvg + (Number(latencyMs || 0) - prevAvg) / sampleCount;
    clientEntry.latencySampleCount = sampleCount;
    clientEntry.latencyMsAvg = Number(nextAvg.toFixed(2));
    const requests = Math.max(1, Number(clientEntry.requests || 0));
    const failures = Math.max(0, Number(clientEntry.failures || 0));
    const failureRatio = failures / requests;
    const timeoutRatio = failures > 0 ? Number(clientEntry.timeoutFailures || 0) / failures : 0;
    const networkRatio = failures > 0 ? Number(clientEntry.networkFailures || 0) / failures : 0;
    const latency = Number(latencyMs || clientEntry.latencyMsAvg || 0);
    const latencyScore =
      latency <= 2000
        ? 1
        : latency <= 5000
        ? 0.92
        : latency <= 10000
        ? 0.78
        : latency <= 20000
        ? 0.58
        : 0.4;
    const targetWeight = clamp(
      0.5 +
        latencyScore * 1.9 +
        (1 - failureRatio) * 0.55 -
        timeoutRatio * 0.55 -
        networkRatio * 0.7,
      0.25,
      3.4
    );
    let adjustedTargetWeight = targetWeight;
    if (
      sampleCount >= this.backfillProxyMinRequests &&
      latency >= this.backfillProxyMaxLatencyMs
    ) {
      adjustedTargetWeight = Math.min(adjustedTargetWeight, 0.68);
    }
    if (
      sampleCount >= this.backfillProxyVeryHighLatencyMinSamples &&
      latency >= this.backfillProxyVeryHighLatencyMs
    ) {
      adjustedTargetWeight = Math.min(adjustedTargetWeight, 0.38);
    }
    const nextWeight =
      Number(clientEntry.weight || 1) +
      (adjustedTargetWeight - Number(clientEntry.weight || 1)) * 0.18;
    clientEntry.weight = Number(clamp(nextWeight, 0.2, 4).toFixed(3));
    if (Number(clientEntry.cooldownUntil || 0) > Date.now()) {
      clientEntry.cooldownUntil = Date.now();
    }
    this.recordClientQualitySample(clientEntry, {
      success: true,
      latencyMs,
    });
    this.updateClientBackfillPenalty(clientEntry, Date.now());
  }

  computeClientCooldownMs(reason, clientEntry) {
    const failCount = Math.max(
      1,
      Number(clientEntry && clientEntry.consecutiveFailures ? clientEntry.consecutiveFailures : 1)
    );
    if (reason === "rate_limit_429") {
      const streak = Math.max(
        1,
        Number(clientEntry && clientEntry.consecutive429 ? clientEntry.consecutive429 : 1)
      );
      return clamp(
        this.client429CooldownBaseMs * 2 ** Math.min(6, streak - 1),
        this.client429CooldownBaseMs,
        this.client429CooldownMaxMs
      );
    }
    if (reason === "server_error_500" || reason === "service_unavailable_503") {
      return clamp(
        this.clientServerErrorCooldownBaseMs * 2 ** Math.min(6, failCount - 1),
        this.clientServerErrorCooldownBaseMs,
        this.clientServerErrorCooldownMaxMs
      );
    }
    if (reason === "timeout" || reason === "network_error" || reason === "connection_reset") {
      return clamp(
        this.clientTimeoutCooldownBaseMs * 2 ** Math.min(6, failCount - 1),
        this.clientTimeoutCooldownBaseMs,
        this.clientTimeoutCooldownMaxMs
      );
    }
    if (reason === "proxy_error") {
      return clamp(
        this.clientProxyErrorCooldownBaseMs * 2 ** Math.min(7, failCount - 1),
        this.clientProxyErrorCooldownBaseMs,
        this.clientProxyErrorCooldownMaxMs
      );
    }
    return this.clientDefaultCooldownMs;
  }

  observeClientRequestFailure(clientEntry, error) {
    if (!clientEntry) return;
    const reason = summarizeErrorReason(toErrorMessage(error));
    const now = Date.now();
    clientEntry.failures = Math.max(0, Number(clientEntry.failures || 0)) + 1;
    clientEntry.consecutiveFailures = Math.max(0, Number(clientEntry.consecutiveFailures || 0)) + 1;
    clientEntry.lastErrorReason = reason;
    clientEntry.lastErrorAt = now;
    if (reason === "rate_limit_429") {
      clientEntry.failures429 = Math.max(0, Number(clientEntry.failures429 || 0)) + 1;
      clientEntry.consecutive429 = Math.max(0, Number(clientEntry.consecutive429 || 0)) + 1;
    } else {
      clientEntry.consecutive429 = 0;
    }
    if (reason === "server_error_500") {
      clientEntry.failures500 = Math.max(0, Number(clientEntry.failures500 || 0)) + 1;
    }
    if (reason === "timeout") {
      clientEntry.timeoutFailures = Math.max(0, Number(clientEntry.timeoutFailures || 0)) + 1;
    }
    if (
      reason === "network_error" ||
      reason === "connection_reset" ||
      reason === "proxy_error"
    ) {
      clientEntry.networkFailures = Math.max(0, Number(clientEntry.networkFailures || 0)) + 1;
    }
    const cooldownMs = this.computeClientCooldownMs(reason, clientEntry);
    let nextCooldownUntil = Math.max(Number(clientEntry.cooldownUntil || 0), now + cooldownMs);
    const totalRequests = Math.max(0, Number(clientEntry.requests || 0));
    const totalSuccesses = Math.max(0, Number(clientEntry.successes || 0));
    const totalFailures = Math.max(0, Number(clientEntry.failures || 0));
    const timeoutRatio =
      totalFailures > 0
        ? Math.max(0, Number(clientEntry.timeoutFailures || 0)) / totalFailures
        : 0;
    const networkRatio =
      totalFailures > 0
        ? Math.max(0, Number(clientEntry.networkFailures || 0)) / totalFailures
        : 0;
    if (totalRequests >= 20 && totalSuccesses <= 1) {
      nextCooldownUntil = Math.max(nextCooldownUntil, now + 10 * 60 * 1000);
    } else if (
      totalRequests >= 8 &&
      totalSuccesses === 0 &&
      (reason === "timeout" ||
        reason === "network_error" ||
        reason === "connection_reset" ||
        reason === "proxy_error")
    ) {
      nextCooldownUntil = Math.max(nextCooldownUntil, now + 12 * 60 * 1000);
    } else if (
      totalRequests >= 60 &&
      totalFailures >= totalSuccesses * 3 &&
      reason !== "rate_limit_429"
    ) {
      nextCooldownUntil = Math.max(nextCooldownUntil, now + 3 * 60 * 1000);
    }
    if (reason === "timeout" && totalFailures >= 6 && timeoutRatio >= 0.33) {
      nextCooldownUntil = Math.max(nextCooldownUntil, now + 3 * 60 * 1000);
    }
    if (reason === "timeout" && totalFailures >= 12 && timeoutRatio >= 0.45) {
      nextCooldownUntil = Math.max(nextCooldownUntil, now + 90 * 1000);
    }
    if (
      (reason === "network_error" || reason === "connection_reset" || reason === "proxy_error") &&
      totalFailures >= 4 &&
      networkRatio >= 0.25
    ) {
      nextCooldownUntil = Math.max(nextCooldownUntil, now + 6 * 60 * 1000);
    }
    if (
      (reason === "network_error" || reason === "connection_reset" || reason === "proxy_error") &&
      totalFailures >= 10 &&
      networkRatio >= 0.3
    ) {
      nextCooldownUntil = Math.max(nextCooldownUntil, now + 3 * 60 * 1000);
    }
    clientEntry.cooldownUntil = nextCooldownUntil;
    const currentWeight = clamp(Number(clientEntry.weight || 1), 0.2, 4);
    let targetWeight = currentWeight;
    if (reason === "rate_limit_429") {
      targetWeight = Math.max(0.45, currentWeight * 0.96);
    } else if (reason === "timeout") {
      targetWeight = Math.max(0.25, currentWeight * 0.58);
      if (totalFailures >= 6 && timeoutRatio >= 0.33) {
        targetWeight = Math.max(0.2, currentWeight * 0.42);
      }
    } else if (
      reason === "network_error" ||
      reason === "connection_reset" ||
      reason === "proxy_error"
    ) {
      targetWeight = Math.max(0.2, currentWeight * 0.48);
      if (totalFailures >= 4 && networkRatio >= 0.25) {
        targetWeight = Math.max(0.2, currentWeight * 0.32);
      }
    } else if (reason === "server_error_500" || reason === "service_unavailable_503") {
      targetWeight = Math.max(0.35, currentWeight * 0.82);
    } else {
      targetWeight = Math.max(0.3, currentWeight * 0.86);
    }
    clientEntry.weight = Number(
      clamp(currentWeight + (targetWeight - currentWeight) * 0.55, 0.2, 4).toFixed(3)
    );
    this.recordClientQualitySample(clientEntry, {
      success: false,
      reason,
      now,
    });
    this.updateClientBackfillPenalty(clientEntry, now);
  }

  observeClientRequestEnd(clientEntry) {
    if (!clientEntry) return;
    clientEntry.inFlight = Math.max(0, Number(clientEntry.inFlight || 0) - 1);
  }

  buildCoverageWindowSummary() {
    const rows =
      this.walletStore && typeof this.walletStore.list === "function"
        ? this.walletStore.list()
        : [];
    const walletRowsByWallet = new Map();
    let oldestProcessedAt = null;
    let newestProcessedAt = null;
    let frontierTimeMs = null;
    let frontierWallet = null;
    let activationOnlyWallets = 0;
    let partialHistoryWallets = 0;
    let recentHistoryWallets = 0;
    let completeWallets = 0;
    let incompleteWalletsWithHistory = 0;

    rows.forEach((row) => {
      const wallet = normalizeAddress(row && row.wallet);
      if (wallet) {
        walletRowsByWallet.set(wallet, row);
      }
      const firstTrade = Number(row && row.firstTrade ? row.firstTrade : 0);
      const lastTrade = Number(
        row && (row.lastTrade || row.lastActivity || row.updatedAt)
          ? row.lastTrade || row.lastActivity || row.updatedAt
          : 0
      );
      if (firstTrade > 0 && (!oldestProcessedAt || firstTrade < oldestProcessedAt)) {
        oldestProcessedAt = firstTrade;
      }
      if (lastTrade > 0 && (!newestProcessedAt || lastTrade > newestProcessedAt)) {
        newestProcessedAt = lastTrade;
      }
    });

    Object.values(this.state.walletStates || {}).forEach((row) => {
      const wallet = normalizeAddress(row && row.wallet);
      const safeRow = wallet ? walletRowsByWallet.get(wallet) || null : null;
      const complete = this.isBackfillCompleteRow(row);
      const hasSuccess =
        Number(row && row.lastSuccessAt ? row.lastSuccessAt : 0) > 0 ||
        Number(row && row.scansSucceeded ? row.scansSucceeded : 0) > 0;
      if (complete) {
        completeWallets += 1;
        return;
      }
      const firstTrade = Number(safeRow && safeRow.firstTrade ? safeRow.firstTrade : 0);
      if (hasSuccess && firstTrade > 0) {
        incompleteWalletsWithHistory += 1;
        partialHistoryWallets += 1;
        if (!frontierTimeMs || firstTrade < frontierTimeMs) {
          frontierTimeMs = firstTrade;
          frontierWallet = wallet || null;
        }
        if (String((row && row.historyPhase) || "deep").trim().toLowerCase() === "recent") {
          recentHistoryWallets += 1;
        }
        return;
      }
      activationOnlyWallets += 1;
    });

    const onchainStatus =
      this.onchainDiscovery && typeof this.onchainDiscovery.getStatus === "function"
        ? this.onchainDiscovery.getStatus()
        : null;
    const persistedOnchain = !onchainStatus ? this.loadPersistedOnchainSnapshot() : null;
    const referenceStartTimeMs =
      Number(
        (onchainStatus && onchainStatus.progress && onchainStatus.progress.startTimeMs) ||
          (persistedOnchain && persistedOnchain.startTimeMs) ||
          0
      ) ||
      null;
    const remainingHistoryMs =
      frontierTimeMs && referenceStartTimeMs && frontierTimeMs > referenceStartTimeMs
        ? Math.max(0, frontierTimeMs - referenceStartTimeMs)
        : 0;

    return {
      indexedWalletRows: rows.length,
      oldestProcessedAt,
      newestProcessedAt,
      frontierTimeMs,
      frontierWallet,
      frontierMethod:
        frontierTimeMs && incompleteWalletsWithHistory > 0
          ? "earliest_loaded_trade_among_incomplete_wallets"
          : activationOnlyWallets > 0
          ? "activation_only_wallets_pending"
          : "history_complete",
      referenceStartTimeMs,
      remainingHistoryMs,
      activationOnlyWallets,
      partialHistoryWallets,
      recentHistoryWallets,
      completeWallets,
      incompleteWalletsWithHistory,
    };
  }

  buildSchedulingSnapshot() {
    return {
      discovery: {
        baseIntervalMs: this.discoveryIntervalMs,
        nextRunAt: this.runtime.nextDiscoveryAt,
        nextDelayMs: this.runtime.discoveryDelayMs,
        lastDurationMs: this.runtime.lastDiscoveryDurationMs,
      },
      repair: {
        nextRunAt: this.runtime.nextRepairAt,
        nextDelayMs: this.runtime.repairDelayMs,
        lastDurationMs: this.runtime.lastRepairDurationMs,
        lastSummary: this.runtime.lastHistoryRepairSummary || null,
      },
      scan: {
        baseIntervalMs: this.scanIntervalMs,
        nextRunAt: this.runtime.nextScanAt,
        nextDelayMs: this.runtime.scanDelayMs,
        lastDurationMs: this.runtime.lastScanDurationMs,
        lanes: this.runtime.scanLanePlan || null,
        deepBackfillAssignment: this.runtime.deepBackfillAssignment || null,
      },
    };
  }

  computeDiscoveryDelayMs(lastResult = null) {
    const diagnostics = this.buildDiagnosticsSummary();
    const now = Date.now();
    const externalDiscoveryOnly =
      !this.onchainDiscovery ||
      typeof this.onchainDiscovery.discoverStep !== "function";
    if (externalDiscoveryOnly) {
      let delayMs = Math.min(this.discoveryIntervalMs, 3000);
      if (lastResult && Number(lastResult.added || 0) > 0) {
        delayMs = Math.min(delayMs, 1000);
      } else if (this.runtime.priorityScanQueue.length <= 0 && diagnostics.backlog <= 0) {
        delayMs = Math.min(this.discoveryIntervalMs, 5000);
      }
      return Math.max(1000, Math.round(delayMs));
    }
    const onchainProgress =
      this.onchainDiscovery && typeof this.onchainDiscovery.getStatus === "function"
        ? this.onchainDiscovery.getStatus().progress
        : null;
    let delayMs = this.discoveryIntervalMs;

    if (diagnostics.discovered <= 0) {
      delayMs = Math.min(delayMs, 5000);
    }
    if (
      lastResult &&
      Array.isArray(lastResult.newWallets) &&
      lastResult.newWallets.length > 0 &&
      diagnostics.backlog < this.backlogWalletThreshold
    ) {
      delayMs = Math.min(delayMs, 7000);
    }
    if (this.runtime.backlogMode || diagnostics.backlog >= this.backlogWalletThreshold) {
      delayMs = Math.min(delayMs, 2500);
    }
    if (
      onchainProgress &&
      Number.isFinite(Number(onchainProgress.remainingMs)) &&
      Number(onchainProgress.remainingMs) > 0
    ) {
      delayMs = Math.min(delayMs, 10000);
    }
    if (now - Number(this.state.lastDiscoveryAt || 0) > Math.max(60000, this.discoveryIntervalMs * 3)) {
      delayMs = Math.min(delayMs, 5000);
    }

    return Math.max(1000, Math.round(delayMs));
  }

  computeRepairDelayMs() {
    const diagnostics = this.buildDiagnosticsSummary();
    const totalQueued =
      Number(this.runtime.priorityScanQueue.length || 0) +
      Number(this.runtime.continuationScanQueue.length || 0);
    const pending = Math.max(
      0,
      Math.max(
        Number(diagnostics.historyAuditRepairPending || 0),
        Number(diagnostics.remoteHistoryVerificationPending || 0)
      )
    );
    if (pending <= 0) return 15000;
    if (totalQueued >= this.maxWalletsPerScan * 8) return 15000;
    if (totalQueued >= this.maxWalletsPerScan * 4) return 10000;
    if (pending >= 30000) return 1000;
    if (pending >= 15000) return 1500;
    if (pending >= 5000) return 2500;
    return 5000;
  }

  computeScanDelayMs(lastResult = null) {
    const diagnostics = this.buildDiagnosticsSummary();
    const clientPool = this.buildClientPoolSummary();
    const totalQueued =
      Number(this.runtime.priorityScanQueue.length || 0) +
      Number(this.runtime.continuationScanQueue.length || 0);
    const coolingRatio =
      clientPool.total > 0 ? Number(clientPool.cooling || 0) / Number(clientPool.total || 1) : 0;
    const pressured =
      coolingRatio >= 0.5 ||
      Number(clientPool.pauseRemainingMsMax || 0) > 0 ||
      Number(clientPool.failures429 || 0) > 0;
    const livePressure =
      diagnostics.liveTracking > 0 ||
      this.runtime.liveScanQueue.length > 0 ||
      Number(diagnostics.liveStaleWallets || 0) > 0;
    let delayMs = this.scanIntervalMs;

    if (diagnostics.backlog > 0 && !pressured) {
      delayMs = Math.min(delayMs, Math.max(3000, Math.round(this.scanIntervalMs * 0.35)));
    }
    if (livePressure) {
      delayMs = Math.min(
        delayMs,
        Math.max(
          2500,
          Math.round(
            this.liveRefreshTargetMs /
              Math.max(1, this.liveWalletsPerScanConfigured || this.liveWalletsPerScanMin)
          )
        )
      );
    }
    if (pressured) {
      delayMs = Math.max(delayMs, Math.round(this.scanIntervalMs * (coolingRatio >= 0.75 ? 2.5 : 1.5)));
      delayMs = Math.max(delayMs, Number(clientPool.pauseRemainingMsMax || 0) + 500);
    }
    if (lastResult && Number(lastResult.failed || 0) > Number(lastResult.ok || 0)) {
      delayMs = Math.max(delayMs, Math.round(this.scanIntervalMs * 1.4));
    }
    if (this.runtime.priorityScanQueue.length > this.maxWalletsPerScan * 2 && !pressured) {
      delayMs = Math.min(delayMs, 2500);
    }
    if (
      this.runtime.priorityScanQueue.length > this.maxWalletsPerScan * 4 &&
      !pressured &&
      Number(clientPool.rpmUsed || 0) < Number(clientPool.rpmCap || 0) * 0.4
    ) {
      delayMs = Math.min(delayMs, 1000);
    }
    if (
      this.runtime.priorityScanQueue.length > this.maxWalletsPerScan * 20 &&
      !pressured &&
      Number(clientPool.rpmUsed || 0) < Number(clientPool.rpmCap || 0) * 0.15
    ) {
      delayMs = Math.min(delayMs, 250);
    }
    if (
      totalQueued > this.maxWalletsPerScan * 12 &&
      !pressured &&
      Number(clientPool.rpmUsed || 0) < Number(clientPool.rpmCap || 0) * 0.2
    ) {
      delayMs = Math.min(delayMs, 500);
    }

    return Math.max(250, Math.round(delayMs));
  }

  scheduleDiscoveryLoop(delayMs = null) {
    if (!this.runtime.running) return;
    if (this.runtime.discoveryTimer) {
      clearTimeout(this.runtime.discoveryTimer);
      this.runtime.discoveryTimer = null;
    }
    const nextDelay = Math.max(0, Number(delayMs !== null ? delayMs : this.computeDiscoveryDelayMs()) || 0);
    this.runtime.discoveryDelayMs = nextDelay;
    this.runtime.nextDiscoveryAt = Date.now() + nextDelay;
    this.runtime.discoveryTimer = setTimeout(async () => {
      this.runtime.discoveryTimer = null;
      const startedAt = Date.now();
      let result = null;
      try {
        result = await this.discoverWallets();
        this.runtime.lastError = null;
      } catch (error) {
        this.runtime.lastError = `[discover] ${toErrorMessage(error)}`;
      } finally {
        this.runtime.lastDiscoveryDurationMs = Date.now() - startedAt;
        this.scheduleDiscoveryLoop(this.computeDiscoveryDelayMs(result));
      }
    }, nextDelay);
  }

  scheduleRepairLoop(delayMs = null) {
    if (!this.runtime.running) return;
    if (this.runtime.repairTimer) {
      clearTimeout(this.runtime.repairTimer);
      this.runtime.repairTimer = null;
    }
    const nextDelay = Math.max(
      0,
      Number(delayMs !== null ? delayMs : this.computeRepairDelayMs()) || 0
    );
    this.runtime.repairDelayMs = nextDelay;
    this.runtime.nextRepairAt = Date.now() + nextDelay;
    this.runtime.repairTimer = setTimeout(() => {
      this.runtime.repairTimer = null;
      const startedAt = Date.now();
      try {
        if (!this.runtime.inRepair) {
          this.runtime.inRepair = true;
          const diagnostics = this.buildDiagnosticsSummary();
          const remotePending = Math.max(
            0,
            Number(diagnostics.remoteHistoryVerificationPending || 0)
          );
          const historyPending = Math.max(
            0,
            Number(diagnostics.historyAuditRepairPending || 0)
          );
          const remoteCatchupHeavy =
            remotePending >= Math.max(1000, this.topCorrectionCohortSize) ||
            Number(diagnostics.walletBacklog || 0) >= this.maxWalletsPerScan * 4;
          const totalQueued =
            Number(this.runtime.priorityScanQueue.length || 0) +
            Number(this.runtime.continuationScanQueue.length || 0);
          const scanStarved =
            !this.runtime.lastScanStartedAt ||
            Date.now() - Number(this.runtime.lastScanStartedAt || 0) >
              Math.max(15000, Number(this.runtime.scanDelayMs || 0) * 4);
          if (remoteCatchupHeavy && totalQueued >= this.maxWalletsPerScan * 2 && scanStarved) {
            this.runtime.lastHistoryRepairSummary = {
              scanned: 0,
              repairedLocal: 0,
              requeuedRemote: 0,
              failed: 0,
              deferred: true,
              reason: "scan_priority",
              at: Date.now(),
            };
            return;
          }
          const migrationLimit = remoteCatchupHeavy
            ? Math.max(0, Math.min(32, Math.floor(this.historyAuditRepairBatchSize / 16) || 0))
            : Math.max(this.historyAuditRepairBatchSize * 4, 512);
          this.runtime.lastLegacyVerificationMigration = this.runLegacyRemoteVerificationMigration(
            migrationLimit
          );
          const pending = Math.max(historyPending, remotePending);
          const repairBatchLimit = remoteCatchupHeavy
            ? Math.max(0, Math.min(32, Math.floor(this.historyAuditRepairBatchSize / 16) || 0))
            : this.historyAuditRepairBatchSize;
          let passes = 1;
          if (!remoteCatchupHeavy) {
            if (pending >= 30000) passes = 3;
            else if (pending >= 12000) passes = 2;
          }
          let combined = null;
          for (let pass = 0; pass < passes; pass += 1) {
            combined = this.mergeHistoryRepairSummaries(
              combined,
              this.runHistoryAuditRepairCycle(repairBatchLimit)
            );
          }
          this.runtime.lastHistoryRepairSummary = combined;
          if (this.walletStore && typeof this.walletStore.flush === "function") {
            this.walletStore.flush(false);
          }
        }
      } catch (error) {
        this.runtime.lastError = `[repair] ${toErrorMessage(error)}`;
      } finally {
        this.runtime.inRepair = false;
        this.runtime.lastRepairDurationMs = Date.now() - startedAt;
        this.scheduleRepairLoop(this.computeRepairDelayMs());
      }
    }, nextDelay);
  }

  scheduleScanLoop(delayMs = null) {
    if (!this.runtime.running || this.discoveryOnly) {
      if (!this.runtime.running || this.discoveryOnly) {
        this.logger.info(
          `[wallet-indexer] scan loop skipped running=${this.runtime.running ? "true" : "false"} discovery_only=${this.discoveryOnly ? "true" : "false"}`
        );
      }
      return;
    }
    if (this.runtime.scanTimer) {
      clearTimeout(this.runtime.scanTimer);
      this.runtime.scanTimer = null;
    }
    const nextDelay = Math.max(0, Number(delayMs !== null ? delayMs : this.computeScanDelayMs()) || 0);
    this.runtime.scanDelayMs = nextDelay;
    this.runtime.nextScanAt = Date.now() + nextDelay;
    if (!this.runtime.lastScanStartedAt) {
      this.logger.info(
        `[wallet-indexer] scan loop scheduled delay_ms=${nextDelay} concurrency=${this.runtime.scanConcurrencyCurrent}/${this.walletScanConcurrency}`
      );
    }
    this.runtime.scanTimer = setTimeout(async () => {
      this.runtime.scanTimer = null;
      const startedAt = Date.now();
      if (!this.runtime.lastScanStartedAt) {
        this.logger.info(`[wallet-indexer] scan loop fired delay_ms=${nextDelay}`);
      }
      let result = null;
      try {
        result = await this.scanCycle();
        this.runtime.lastError = null;
      } catch (error) {
        this.runtime.lastError = `[scan] ${toErrorMessage(error)}`;
      } finally {
        this.runtime.lastScanDurationMs = Date.now() - startedAt;
        this.scheduleScanLoop(this.computeScanDelayMs(result));
      }
    }, nextDelay);
  }

  scheduleLiveRefreshLoop(delayMs = null) {
    if (!this.runtime.running || this.discoveryOnly) return;
    if (this.runtime.liveRefreshTimer) {
      clearTimeout(this.runtime.liveRefreshTimer);
      this.runtime.liveRefreshTimer = null;
    }
    const nextDelay = Math.max(
      0,
      Number(delayMs !== null ? delayMs : this.liveRefreshLoopIntervalMs) || 0
    );
    this.runtime.liveRefreshDelayMs = nextDelay;
    this.runtime.nextLiveRefreshAt = Date.now() + nextDelay;
    if (!this.runtime.lastLiveRefreshStartedAt) {
      this.logger.info(
        `[wallet-indexer] live refresh scheduled delay_ms=${nextDelay} concurrency=${this.liveRefreshConcurrency} target_ms=${this.liveRefreshTargetMs}`
      );
    }
    this.runtime.liveRefreshTimer = setTimeout(async () => {
      this.runtime.liveRefreshTimer = null;
      const startedAt = Date.now();
      if (!this.runtime.lastLiveRefreshStartedAt) {
        this.logger.info(`[wallet-indexer] live refresh fired delay_ms=${nextDelay}`);
      }
      try {
        await this.liveRefreshCycle();
      } catch (error) {
        this.runtime.lastError = `[live-refresh] ${toErrorMessage(error)}`;
      } finally {
        this.runtime.lastLiveRefreshDurationMs = Date.now() - startedAt;
        this.scheduleLiveRefreshLoop(this.liveRefreshLoopIntervalMs);
      }
    }, nextDelay);
  }

  buildClientPoolSummary() {
    const now = Date.now();
    const rows = this.clientPoolState.map((entry) => {
      const recent = this.getClientRecentWindowMetrics(entry, now);
      return {
        id: entry.id,
        inFlight: Number(entry.inFlight || 0),
        coolingMs: Math.max(0, Number(entry.cooldownUntil || 0) - now),
        backfillCoolingMs: Math.max(0, Number(entry.backfillCooldownUntil || 0) - now),
        requests: Number(entry.requests || 0),
        successes: Number(entry.successes || 0),
        failures: Number(entry.failures || 0),
        failures429: Number(entry.failures429 || 0),
        failures500: Number(entry.failures500 || 0),
        timeoutFailures: Number(entry.timeoutFailures || 0),
        networkFailures: Number(entry.networkFailures || 0),
        latencyMsAvg: Number(entry.latencyMsAvg || 0),
        weight: Number(entry.weight || 1),
        backfillEligible: this.isClientPreferredForDeepBackfill(entry, now),
        recentWindowLatencyMsAvg: Number(recent.latencyMsAvg || 0),
        recentHighLatencyWindows: Number(recent.highLatencyWindows || 0),
        recentBadWindows: Number(recent.badWindows || 0),
        recentActiveWindows: Number(recent.activeWindowCount || 0),
        successRate:
          Number(entry.requests || 0) > 0
            ? Number((Number(entry.successes || 0) / Number(entry.requests || 0)).toFixed(4))
            : null,
        proxyUrl: entry.proxyUrl || null,
        lastErrorReason: entry.lastErrorReason || null,
        rate:
          entry.rateGuard && typeof entry.rateGuard.getState === "function"
            ? entry.rateGuard.getState()
            : null,
      };
    });
    const active = rows.filter((row) => row.inFlight > 0 || row.requests > 0).length;
    const cooling = rows.filter((row) => row.coolingMs > 0).length;
    const inFlight = rows.reduce((sum, row) => sum + row.inFlight, 0);
    const rpmUsed = rows.reduce(
      (sum, row) => sum + Number((row.rate && row.rate.used1m) || 0),
      0
    );
    const rpmCap = rows.reduce(
      (sum, row) => sum + Number((row.rate && row.rate.rpmCap) || 0),
      0
    );
    const pauseRemainingMsMax = rows.reduce(
      (max, row) => Math.max(max, Number((row.rate && row.rate.pauseRemainingMs) || 0)),
      0
    );
    const failures429 = rows.reduce((sum, row) => sum + Number(row.failures429 || 0), 0);
    const healthyBackfill = rows.filter((row) => row.backfillEligible).length;
    const topByRequests = rows
      .slice()
      .sort((a, b) => b.requests - a.requests)
      .slice(0, 8);
    return {
      total: rows.length,
      active,
      cooling,
      inFlight,
      rpmUsed: Number(rpmUsed.toFixed(3)),
      rpmCap: Number(rpmCap.toFixed(3)),
      pauseRemainingMsMax,
      failures429,
      healthyBackfill,
      rows,
      topByRequests,
    };
  }

  getDiagnostics(options = {}) {
    const page = Math.max(1, Number(options.page || 1));
    const pageSize = Math.max(1, Math.min(500, Number(options.pageSize || 100)));
    const statusFilter = String(options.status || "").toLowerCase();
    const lifecycleFilter = String(options.lifecycle || "").toLowerCase();
    const search = String(options.q || "").trim().toLowerCase();
    const now = Date.now();

    const rows = Object.values(this.state.walletStates || {}).map((row) => {
      const status = this.deriveWalletStatus(row);
      const lifecycleStage = this.deriveWalletLifecycle(row);
      const discoveredAt = Number(row && row.discoveredAt ? row.discoveredAt : 0);
      const scansSucceeded =
        Number(row && row.scansSucceeded ? row.scansSucceeded : 0) ||
        (row && row.lastSuccessAt ? 1 : 0);
      const scansFailed =
        Number(row && row.scansFailed ? row.scansFailed : 0) ||
        (row && row.lastError && !row.lastSuccessAt ? 1 : 0);
      const lastErrorReason =
        (row && row.lastErrorReason) ||
        summarizeErrorReason(row && row.lastError ? row.lastError : "");
      return {
        wallet: row.wallet,
        status,
        lifecycleStage,
        backfillComplete: this.isBackfillCompleteRow(row),
        liveTracking: lifecycleStage === WALLET_LIFECYCLE.LIVE_TRACKING,
        discoveredBy: row.discoveredBy || null,
        discoveredAt: discoveredAt || null,
        waitMs: discoveredAt > 0 ? Math.max(0, now - discoveredAt) : null,
        lastAttemptAt: row.lastAttemptAt || null,
        lastScannedAt: row.lastScannedAt || null,
        lastSuccessAt: row.lastSuccessAt || null,
        lastIndexedAt: status === "indexed" ? row.lastSuccessAt || null : null,
        lastFailureAt: row.lastFailureAt || null,
        lastScanDurationMs: row.lastScanDurationMs || null,
        tradeRowsLoaded: Number(row.tradeRowsLoaded || 0),
        fundingRowsLoaded: Number(row.fundingRowsLoaded || 0),
        tradePagesLoaded: Number(row.tradePagesLoaded || 0),
        fundingPagesLoaded: Number(row.fundingPagesLoaded || 0),
        tradeHasMore: Boolean(row.tradeHasMore),
        fundingHasMore: Boolean(row.fundingHasMore),
        tradeDone: Boolean(row.tradeDone),
        fundingDone: Boolean(row.fundingDone),
        tradeCursor: row.tradeCursor || null,
        fundingCursor: row.fundingCursor || null,
        lastTradeNextCursor: row.lastTradeNextCursor || null,
        lastFundingNextCursor: row.lastFundingNextCursor || null,
        tradeCacheHits: Number(row.tradeCacheHits || 0),
        fundingCacheHits: Number(row.fundingCacheHits || 0),
        tradeRequests: Number(row.tradeRequests || 0),
        fundingRequests: Number(row.fundingRequests || 0),
        backfillCompletedAt: row.backfillCompletedAt || null,
        liveTrackingSince: row.liveTrackingSince || null,
        liveLastScanAt: row.liveLastScanAt || null,
        retryPending: Boolean(row.retryPending),
        retryReason: row.retryReason || null,
        retryQueuedAt: row.retryQueuedAt || null,
        lastAttemptMode: row.lastAttemptMode || null,
        lastSuccessMode: row.lastSuccessMode || null,
        lastFailureMode: row.lastFailureMode || null,
        historyAudit:
          row.historyAudit && typeof row.historyAudit === "object"
            ? { ...row.historyAudit }
            : null,
        scansSucceeded,
        scansFailed,
        consecutiveFailures: Number(row.consecutiveFailures || 0),
        lastErrorReason: lastErrorReason || null,
        lastError: row.lastError || null,
      };
    });

    const filtered = rows
      .filter((row) => {
        if (!statusFilter) return true;
        return row.status === statusFilter || row.lifecycleStage === statusFilter;
      })
      .filter((row) => !lifecycleFilter || row.lifecycleStage === lifecycleFilter)
      .filter((row) => !search || String(row.wallet || "").toLowerCase().includes(search))
      .sort((a, b) => {
        const pri = { failed: 0, pending: 1, partial: 2, indexed: 3 };
        const aPri = Object.prototype.hasOwnProperty.call(pri, a.status) ? pri[a.status] : 9;
        const bPri = Object.prototype.hasOwnProperty.call(pri, b.status) ? pri[b.status] : 9;
        if (aPri !== bPri) return aPri - bPri;
        const aTs = Number(a.lastScannedAt || a.discoveredAt || 0);
        const bTs = Number(b.lastScannedAt || b.discoveredAt || 0);
        return aTs - bTs;
      });

    const total = filtered.length;
    const start = (page - 1) * pageSize;
    const paged = filtered.slice(start, start + pageSize);
    const summary = this.buildDiagnosticsSummary();

    return {
      generatedAt: Date.now(),
      queue: {
        prioritySize: this.runtime.priorityScanQueue.length,
        averageQueueWaitMs: summary.averageQueueWaitMs,
      },
      backlogMode: {
        active: this.runtime.backlogMode,
        reason: this.runtime.backlogReason,
      },
      counts: {
        knownWallets: this.state.knownWallets.length,
        attemptedWallets: summary.attemptedWallets,
        indexed: summary.indexed,
        partial: summary.partial,
        pending: summary.pending,
        failed: summary.failed,
        failedBackfill: summary.failedBackfill,
        backlog: summary.backlog,
        discovered: summary.discovered,
        pendingBackfill: summary.pendingBackfill,
        backfilling: summary.backfilling,
        fullyIndexed: summary.fullyIndexed,
        liveTracking: summary.liveTracking,
        backfillComplete: summary.backfillComplete,
        successfulScans: summary.successfulScans,
        failedScans: summary.failedScans,
        completionPct:
          this.state.knownWallets.length > 0
            ? Number(((summary.backfillComplete / this.state.knownWallets.length) * 100).toFixed(4))
            : 0,
      },
      averagePendingWaitMs: summary.averagePendingWaitMs,
      topErrorReasons: summary.topErrorReasons,
      query: {
        status: statusFilter || null,
        lifecycle: lifecycleFilter || null,
        q: search || null,
        page,
        pageSize,
      },
      total,
      pages: Math.max(1, Math.ceil(total / pageSize)),
      rows: paged,
    };
  }
}

module.exports = {
  ExchangeWalletIndexer,
};
