const crypto = require("crypto");

const EPSILON = 1e-10;

function toNum(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function extractPayloadData(result, fallback = null) {
  if (!result || !result.payload) return fallback;
  if (Object.prototype.hasOwnProperty.call(result.payload, "data")) {
    return result.payload.data;
  }
  return result.payload;
}

function normalizeSide(rawSide = "") {
  const value = String(rawSide || "").trim().toLowerCase();
  if (!value) return "";
  if (["long", "buy", "bid"].includes(value)) return "long";
  if (["short", "sell", "ask"].includes(value)) return "short";
  return value;
}

function normalizePositionRow(row = {}) {
  const token = String(row.token || row.symbol || row.market || row.asset || "-").trim().toUpperCase() || "-";
  const side = normalizeSide(row.side || row.direction || row.rawSide || row.positionSide || "");
  const rawSize =
    row.size ??
    row.amount ??
    row.positionSize ??
    row.contractSize ??
    row.positionQty ??
    row.quantity ??
    row.qty;
  let size = toNum(rawSize, NaN);
  if (!Number.isFinite(size)) {
    const positionUsd = toNum(
      row.positionSizeUsd ?? row.positionUsd ?? row.notionalUsd ?? row.sizeUsd ?? row.positionValueUsd,
      NaN
    );
    const markPrice = toNum(row.markPrice ?? row.mark ?? row.lastPrice ?? row.price, NaN);
    if (Number.isFinite(positionUsd) && Number.isFinite(markPrice) && markPrice > 0) {
      size = positionUsd / markPrice;
    }
  }
  const entry = toNum(row.entryPrice ?? row.entry ?? row.avgEntryPrice ?? row.price, NaN);
  const markPrice = toNum(row.markPrice ?? row.mark ?? row.lastPrice ?? row.price, NaN);
  return {
    token,
    side,
    size: Number.isFinite(size) ? Math.abs(size) : NaN,
    entry,
    markPrice,
    positionSizeUsd: toNum(
      row.positionSizeUsd ?? row.positionUsd ?? row.notionalUsd ?? row.sizeUsd ?? row.positionValueUsd,
      NaN
    ),
    leverage: toNum(row.leverage ?? row.leveragex ?? row.maxLeverage ?? row.max_leverage, NaN),
    pnlUsd: toNum(row.pnlUsd ?? row.positionPnlUsd ?? row.unrealizedPnlUsd ?? row.pnl, NaN),
    openedAt: toNum(row.openedAt ?? row.createdAt ?? row.timestamp ?? row.detectedAt, NaN),
  };
}

function normalizeOrderRow(row = {}) {
  return {
    token: String(row.token || row.symbol || row.market || "-").trim().toUpperCase() || "-",
    side: normalizeSide(row.side || row.direction || row.rawSide || row.orderSide || ""),
    size: toNum(row.size ?? row.amount ?? row.quantity ?? row.orderSize ?? row.orderQty, NaN),
    price: toNum(row.price ?? row.limitPrice ?? row.orderPrice ?? row.avgPrice, NaN),
    reduceOnly: Boolean(row.reduceOnly ?? row.reduce_only ?? row.reduce_only_order),
    status: String(row.status || row.orderStatus || row.state || "open").trim(),
    time: toNum(row.time ?? row.timestamp ?? row.createdAt ?? row.updatedAt, NaN),
  };
}

function buildPositionKey(token, side) {
  return `${String(token || "").trim().toUpperCase()}:${normalizeSide(side)}`;
}

function hashToUuid(input) {
  const hex = crypto.createHash("sha256").update(String(input || "")).digest("hex").slice(0, 32);
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`;
}

function formatAmount(value) {
  const num = Math.max(0, Math.abs(toNum(value, 0)));
  if (!Number.isFinite(num)) return "0";
  const text = num.toFixed(10).replace(/\.?0+$/, "");
  return text || "0";
}

function snapDownToStep(value, step) {
  const num = Math.max(0, Math.abs(toNum(value, 0)));
  const increment = Math.max(0, Math.abs(toNum(step, 0)));
  if (!Number.isFinite(num) || !Number.isFinite(increment) || increment <= EPSILON) {
    return num;
  }
  const snapped = Math.floor((num / increment) + EPSILON) * increment;
  return Number.isFinite(snapped) && snapped > EPSILON ? snapped : 0;
}

function buildSideForAction(desiredSide, delta) {
  const side = normalizeSide(desiredSide);
  const increasing = delta > 0;
  if (side === "short") {
    return increasing ? "ask" : "bid";
  }
  return increasing ? "bid" : "ask";
}

function aggregatePositions(rows, copyRatio = 1) {
  const target = new Map();
  const normalizedRatio = Number.isFinite(Number(copyRatio)) ? Math.max(0, Number(copyRatio)) : 1;
  (Array.isArray(rows) ? rows : []).forEach((row) => {
    const position = normalizePositionRow(row);
    if (!position.token || !position.side) return;
    if (!Number.isFinite(position.size) || position.size <= 0) return;
    const key = buildPositionKey(position.token, position.side);
    const current = target.get(key) || {
      token: position.token,
      side: position.side,
      size: 0,
      sourceRows: [],
    };
    current.size += position.size * normalizedRatio;
    current.sourceRows.push(position);
    target.set(key, current);
  });
  return target;
}

function aggregateCurrentPositions(rows) {
  const current = new Map();
  (Array.isArray(rows) ? rows : []).forEach((row) => {
    const position = normalizePositionRow(row);
    if (!position.token || !position.side) return;
    if (!Number.isFinite(position.size) || position.size <= 0) return;
    const key = buildPositionKey(position.token, position.side);
    const currentRow = current.get(key) || {
      token: position.token,
      side: position.side,
      size: 0,
      sourceRows: [],
    };
    currentRow.size += position.size;
    currentRow.sourceRows.push(position);
    current.set(key, currentRow);
  });
  return current;
}

function buildActions({ targetMap, currentMap }) {
  const actions = [];
  const keys = new Set([...targetMap.keys(), ...currentMap.keys()]);
  for (const key of keys) {
    const target = targetMap.get(key) || null;
    const current = currentMap.get(key) || null;
    const targetSize = target ? Number(target.size || 0) : 0;
    const currentSize = current ? Number(current.size || 0) : 0;
    const delta = targetSize - currentSize;
    if (Math.abs(delta) <= EPSILON) continue;
    const [token, side] = key.split(":");
    const action = currentSize <= EPSILON && targetSize > EPSILON
      ? "open"
      : targetSize <= EPSILON && currentSize > EPSILON
      ? "close"
      : delta > 0
      ? "increase"
      : "reduce";
    actions.push({
      key,
      token,
      side: side || "long",
      action,
      targetSize,
      currentSize,
      deltaSize: Math.abs(delta),
    });
  }
  actions.sort((left, right) => Number(right.deltaSize || 0) - Number(left.deltaSize || 0));
  return actions;
}

async function runWithConcurrency(items, limit, iterator) {
  const rows = Array.isArray(items) ? items.slice() : [];
  const nextLimit = Math.max(1, Number(limit || 1));
  const results = [];
  let cursor = 0;
  async function worker() {
    while (cursor < rows.length) {
      const index = cursor;
      cursor += 1;
      results[index] = await iterator(rows[index], index);
    }
  }
  const workers = Array.from({ length: Math.min(nextLimit, rows.length || 1) }, () => worker());
  await Promise.all(workers);
  return results;
}

function createCopyTradingExecutionWorker({
  restClient,
  loadStore,
  saveStore,
  getLeaderWalletsForUser,
  getApiKeysForUser,
  getCopyProfileForUser = null,
  getCopyRiskRulesForUser = null,
  getCopyRuntimeStatusForUser = null,
  fetchLeaderActivityPayload,
  createPacificaSignedRequest,
  onEvent = null,
  logger = console,
  pollIntervalMs = 5000,
  dryRunDefault = true,
  slippagePercentDefault = 0.5,
  maxConcurrentLeaders = 4,
  maxEventsPerUser = 100,
} = {}) {
  if (!restClient || typeof restClient.get !== "function" || typeof restClient.post !== "function") {
    throw new Error("rest_client_required");
  }
  if (typeof loadStore !== "function" || typeof saveStore !== "function") {
    throw new Error("copy_trading_store_required");
  }
  if (
    typeof getLeaderWalletsForUser !== "function" ||
    typeof getApiKeysForUser !== "function" ||
    typeof fetchLeaderActivityPayload !== "function" ||
    typeof createPacificaSignedRequest !== "function"
  ) {
    throw new Error("copy_trading_dependencies_required");
  }

  const runtime = {
    running: false,
    inFlight: false,
    nextRunAt: null,
    lastStartedAt: null,
    lastFinishedAt: null,
    lastDurationMs: null,
    lastSuccessAt: null,
    lastError: null,
    lastSummary: null,
    userLocks: new Map(),
  };

  const debugCopyTrading = String(process.env.PACIFICA_COPY_TRADE_DEBUG || "").toLowerCase() === "1";

  function ensureStoreShape(store) {
    const next = store && typeof store === "object" ? store : {};
    if (!next.executionByUserId || typeof next.executionByUserId !== "object") next.executionByUserId = {};
    if (!next.executionEventsByUserId || typeof next.executionEventsByUserId !== "object") next.executionEventsByUserId = {};
    if (!next.executionStateByUserId || typeof next.executionStateByUserId !== "object") next.executionStateByUserId = {};
    return next;
  }

  function getExecutionConfig(store, userId) {
    const current = store.executionByUserId && store.executionByUserId[userId]
      ? store.executionByUserId[userId]
      : {};
    return {
      enabled: Boolean(current.enabled),
      dryRun: current.dryRun === undefined ? dryRunDefault : Boolean(current.dryRun),
      copyRatio: Number.isFinite(Number(current.copyRatio)) ? Math.max(0, Number(current.copyRatio)) : 1,
      maxAllocationPct: Number.isFinite(Number(current.maxAllocationPct))
        ? Math.max(0, Number(current.maxAllocationPct))
        : 100,
      minNotionalUsd: Number.isFinite(Number(current.minNotionalUsd))
        ? Math.max(0, Number(current.minNotionalUsd))
        : 1,
      leveragePolicy: ["mirror", "cap", "fixed"].includes(String(current.leveragePolicy || "").trim())
        ? String(current.leveragePolicy).trim()
        : "cap",
      fixedNotionalUsd: Number.isFinite(Number(current.fixedNotionalUsd))
        ? Math.max(0, Number(current.fixedNotionalUsd))
        : 50,
      slippagePercent: Number.isFinite(Number(current.slippagePercent))
        ? Math.max(0, Number(current.slippagePercent))
        : slippagePercentDefault,
      retryPolicy: ["aggressive", "balanced", "conservative"].includes(String(current.retryPolicy || "").trim())
        ? String(current.retryPolicy).trim()
        : "balanced",
      syncBehavior: ["baseline_only", "mirror_all", "safe_pause"].includes(String(current.syncBehavior || "").trim())
        ? String(current.syncBehavior).trim()
        : "baseline_only",
      accessKeyId: String(current.accessKeyId || "").trim(),
      updatedAt: Number(current.updatedAt || 0) || null,
      lastRunAt: Number(current.lastRunAt || 0) || null,
      lastSuccessAt: Number(current.lastSuccessAt || 0) || null,
      lastError: current.lastError ? String(current.lastError) : null,
    };
  }

  function getExecutionStatus(store, userId) {
    const config = getExecutionConfig(store, userId);
    const state = store.executionStateByUserId && store.executionStateByUserId[userId]
      ? store.executionStateByUserId[userId]
      : {};
    const events = Array.isArray(store.executionEventsByUserId && store.executionEventsByUserId[userId])
      ? store.executionEventsByUserId[userId].slice(0, maxEventsPerUser)
      : [];
    return {
      config,
      state,
      events,
    };
  }

  function getCopyProfile(store, userId) {
    if (typeof getCopyProfileForUser === "function") {
      const profile = getCopyProfileForUser(userId, store) || {};
      return {
        copyMode: ["proportional", "fixed_amount", "fixed_allocation", "smart_sync"].includes(String(profile.copyMode || "").trim())
          ? String(profile.copyMode).trim()
          : "proportional",
        copyOnlyNewTrades: true,
        copyExistingTrades: false,
        allowReopenAfterManualClose: Boolean(profile.allowReopenAfterManualClose),
        pauseNewOpensOnly: Boolean(profile.pauseNewOpensOnly),
        stopMode: String(profile.stopMode || "keep").trim() === "close_all" ? "close_all" : "keep",
        updatedAt: Number(profile.updatedAt || 0) || null,
      };
    }
    return {
      copyMode: "proportional",
      copyOnlyNewTrades: true,
      copyExistingTrades: false,
      allowReopenAfterManualClose: false,
      pauseNewOpensOnly: false,
      stopMode: "keep",
      updatedAt: null,
    };
  }

  function getCopyRisk(store, userId) {
    if (typeof getCopyRiskRulesForUser === "function") {
      const risk = getCopyRiskRulesForUser(userId, store) || {};
      const global = risk.global && typeof risk.global === "object" ? risk.global : {};
      const leaders = risk.leaders && typeof risk.leaders === "object" ? risk.leaders : {};
      return {
        global: {
          totalCopiedExposureCapUsd: Number.isFinite(Number(global.totalCopiedExposureCapUsd)) ? Math.max(0, Number(global.totalCopiedExposureCapUsd)) : null,
          totalDailyLossCapUsd: Number.isFinite(Number(global.totalDailyLossCapUsd)) ? Math.max(0, Number(global.totalDailyLossCapUsd)) : null,
          leverageCap: Number.isFinite(Number(global.leverageCap)) ? Math.max(0, Number(global.leverageCap)) : null,
          maxOpenPositions: Number.isFinite(Number(global.maxOpenPositions)) ? Math.max(0, Number(global.maxOpenPositions)) : null,
          maxAllocationPct: Number.isFinite(Number(global.maxAllocationPct)) ? Math.max(0, Number(global.maxAllocationPct)) : null,
          autoPauseAfterFailures: Number.isFinite(Number(global.autoPauseAfterFailures)) ? Math.max(0, Number(global.autoPauseAfterFailures)) : 3,
          allowedSymbols: Array.isArray(global.allowedSymbols) ? global.allowedSymbols : [],
          blockedSymbols: Array.isArray(global.blockedSymbols) ? global.blockedSymbols : [],
        },
        leaders,
      };
    }
    return {
      global: {
        totalCopiedExposureCapUsd: null,
        totalDailyLossCapUsd: null,
        leverageCap: null,
        maxOpenPositions: null,
        maxAllocationPct: null,
        autoPauseAfterFailures: 3,
        allowedSymbols: [],
        blockedSymbols: [],
      },
      leaders: {},
    };
  }

  function getRuntimeState(store, userId) {
    if (typeof getCopyRuntimeStatusForUser === "function") {
      const runtime = getCopyRuntimeStatusForUser(userId, store) || {};
      return {
        state: String(runtime.state || "stopped_keep").trim() || "stopped_keep",
        health: String(runtime.health || "healthy").trim() || "healthy",
        pausedReason: runtime.pausedReason || null,
        blockedReason: runtime.blockedReason || null,
        openOnly: Boolean(runtime.openOnly),
        closeAllRequested: Boolean(runtime.closeAllRequested),
        lastStateChangeAt: Number(runtime.lastStateChangeAt || 0) || null,
        lastBlockedAt: Number(runtime.lastBlockedAt || 0) || null,
        lastHealthyAt: Number(runtime.lastHealthyAt || 0) || null,
      };
    }
    return {
      state: "running",
      health: "healthy",
      pausedReason: null,
      blockedReason: null,
      openOnly: false,
      closeAllRequested: false,
      lastStateChangeAt: null,
      lastBlockedAt: null,
      lastHealthyAt: null,
    };
  }

  function getRawApiKeyRow(store, userId, keyId) {
    const rows = Array.isArray(store.apiKeysByUserId && store.apiKeysByUserId[userId])
      ? store.apiKeysByUserId[userId]
      : [];
    const targetId = String(keyId || "").trim();
    if (targetId) {
      const found = rows.find((row) => String(row.id || "") === targetId) || null;
      if (found) return found;
    }
    return (
      rows.find(
        (row) =>
          String(row.accountWallet || row.account || "").trim() &&
          String(row.agentWallet || row.apiKey || "").trim() &&
          String(row.agentPrivateKey || row.apiSecret || "").trim()
      ) || null
    );
  }

  function appendEvent(store, userId, event) {
    const payload = {
      id: crypto.randomUUID(),
      createdAt: Date.now(),
      ...event,
    };
    if (!store.executionEventsByUserId[userId]) store.executionEventsByUserId[userId] = [];
    store.executionEventsByUserId[userId].unshift(payload);
    store.executionEventsByUserId[userId] = store.executionEventsByUserId[userId].slice(0, maxEventsPerUser);
    if (typeof onEvent === "function") {
      try {
        onEvent(userId, payload, store);
      } catch (_error) {}
    }
  }

  function getPendingByKey(store, userId) {
    const state = store.executionStateByUserId && store.executionStateByUserId[userId]
      ? store.executionStateByUserId[userId]
      : {};
    const pending = state.pendingByKey && typeof state.pendingByKey === "object" ? state.pendingByKey : {};
    return pending;
  }

  function setPendingByKey(store, userId, pendingByKey) {
    if (!store.executionStateByUserId[userId]) store.executionStateByUserId[userId] = {};
    store.executionStateByUserId[userId].pendingByKey = pendingByKey;
    store.executionStateByUserId[userId].updatedAt = Date.now();
  }

  function getLeaderPositionBaselines(store, userId) {
    const state = store.executionStateByUserId && store.executionStateByUserId[userId]
      ? store.executionStateByUserId[userId]
      : {};
    return state.leaderPositionBaselinesByWallet && typeof state.leaderPositionBaselinesByWallet === "object"
      ? state.leaderPositionBaselinesByWallet
      : {};
  }

  function setLeaderPositionBaselines(store, userId, baselinesByWallet) {
    if (!store.executionStateByUserId[userId]) store.executionStateByUserId[userId] = {};
    store.executionStateByUserId[userId].leaderPositionBaselinesByWallet = baselinesByWallet;
    store.executionStateByUserId[userId].updatedAt = Date.now();
  }

function getPositionKeySet(rows) {
  const keys = new Set();
  (Array.isArray(rows) ? rows : []).forEach((row) => {
    const normalized = normalizePositionRow(row);
      if (!normalized.token || !normalized.side) return;
      if (!Number.isFinite(normalized.size) || normalized.size <= 0) return;
      keys.add(buildPositionKey(normalized.token, normalized.side));
  });
  return keys;
}

function ensureLeaderBaselineSnapshot(store, userId, leaderWallet, currentKeys) {
  const baselineByWallet = { ...(getLeaderPositionBaselines(store, userId) || {}) };
  const now = Date.now();
  const keySet = currentKeys instanceof Set ? currentKeys : getPositionKeySet(currentKeys);
  let baseline = baselineByWallet[leaderWallet];
  let changed = false;
  let initialized = false;

  if (!baseline || typeof baseline !== "object") {
    baseline = {
      ignoredKeys: {},
      trackedKeys: {},
      initializedAt: now,
      updatedAt: now,
    };
    keySet.forEach((key) => {
      baseline.ignoredKeys[key] = true;
      baseline.trackedKeys[key] = {
        state: "BASELINE_IGNORED",
        initializedAt: now,
        updatedAt: now,
      };
    });
    baselineByWallet[leaderWallet] = baseline;
    changed = true;
    initialized = true;
    return { baselineByWallet, baseline, changed, initialized };
  }

  if (!baseline.ignoredKeys || typeof baseline.ignoredKeys !== "object") {
    baseline.ignoredKeys = {};
    changed = true;
  }
  if (!baseline.trackedKeys || typeof baseline.trackedKeys !== "object") {
    baseline.trackedKeys = {};
    changed = true;
  }
  Object.keys(baseline.trackedKeys).forEach((key) => {
    const tracked = baseline.trackedKeys[key];
    if (!tracked || typeof tracked !== "object") {
      delete baseline.trackedKeys[key];
      changed = true;
      return;
    }
    if (baseline.ignoredKeys[key]) {
      baseline.trackedKeys[key] = {
        ...tracked,
        state: "BASELINE_IGNORED",
        updatedAt: now,
      };
    } else if (currentKeys instanceof Set && currentKeys.has(key)) {
      baseline.trackedKeys[key] = {
        ...tracked,
        state: tracked.state === "TRACKED_CLOSED" ? "ELIGIBLE_NEW_OPEN" : tracked.state || "ELIGIBLE_NEW_OPEN",
        updatedAt: now,
      };
    }
  });
  Object.keys(baseline.ignoredKeys).forEach((key) => {
    if (!keySet.has(key)) {
      delete baseline.ignoredKeys[key];
      if (baseline.trackedKeys[key] && baseline.trackedKeys[key].state === "BASELINE_IGNORED") {
        delete baseline.trackedKeys[key];
      }
      changed = true;
    }
  });
  baseline.updatedAt = now;
  baselineByWallet[leaderWallet] = baseline;
  return { baselineByWallet, baseline, changed, initialized };
}

function lifecycleStateForLeaderPosition({ baseline, key, currentMap, action = null, manualBlocked = false }) {
  if (manualBlocked) return "MANUAL_OVERRIDE_BLOCKED";
  if (baseline && baseline.ignoredKeys && baseline.ignoredKeys[key]) return "BASELINE_IGNORED";
  if (action === "close") return "TRACKED_CLOSED";
  const current = currentMap && typeof currentMap.has === "function" ? currentMap.has(key) : false;
  return current ? "TRACKED_ACTIVE" : "ELIGIBLE_NEW_OPEN";
}

  function getLeaderSnapshotHashes(store, userId) {
    const state = store.executionStateByUserId && store.executionStateByUserId[userId]
      ? store.executionStateByUserId[userId]
      : {};
    return state.leaderSnapshotHashes && typeof state.leaderSnapshotHashes === "object"
      ? state.leaderSnapshotHashes
      : {};
  }

  function setLeaderSnapshotHashes(store, userId, hashes) {
    if (!store.executionStateByUserId[userId]) store.executionStateByUserId[userId] = {};
    store.executionStateByUserId[userId].leaderSnapshotHashes = hashes;
    store.executionStateByUserId[userId].updatedAt = Date.now();
  }

  async function fetchFollowerPositions(accountWallet) {
    if (!accountWallet) return [];
    const response = await restClient.get("/positions", {
      query: { account: accountWallet },
      timeoutMs: 15000,
      retryMaxAttempts: 2,
    });
    const data = extractPayloadData(response, []);
    return Array.isArray(data) ? data : [];
  }

  async function fetchFollowerAccount(accountWallet) {
    if (!accountWallet) return null;
    try {
      const response = await restClient.get("/account", {
        query: { account: accountWallet },
        timeoutMs: 15000,
        retryMaxAttempts: 2,
      });
      return extractPayloadData(response, null);
    } catch (_error) {
      return null;
    }
  }

  const marketInfoCache = {
    updatedAt: 0,
    bySymbol: {},
  };

  async function fetchMarketInfoBySymbol(force = false) {
    const now = Date.now();
    if (!force && marketInfoCache.updatedAt && now - marketInfoCache.updatedAt < 5 * 60 * 1000) {
      return marketInfoCache.bySymbol;
    }
    try {
      const response = await restClient.get("/info", {
        timeoutMs: 15000,
        retryMaxAttempts: 1,
        cost: 1,
      });
      const data = extractPayloadData(response, []);
      const rows = Array.isArray(data) ? data : [];
      const next = {};
      rows.forEach((item) => {
        const symbol = String(item && (item.symbol || item.s || "")).trim().toUpperCase();
        if (!symbol) return;
        next[symbol] = {
          lotSize: toNum(item.lot_size ?? item.lotSize ?? item.min_order_size ?? item.minOrderSize, NaN),
          minOrderSize: toNum(item.min_order_size ?? item.minOrderSize ?? item.lot_size ?? item.lotSize, NaN),
          tickSize: toNum(item.tick_size ?? item.tickSize, NaN),
          maxLeverage: toNum(item.max_leverage ?? item.maxLeverage, NaN),
        };
      });
      marketInfoCache.bySymbol = next;
      marketInfoCache.updatedAt = now;
      return next;
    } catch (error) {
      logger.warn(`[copy-trading] market info fetch failed: ${error && error.message ? error.message : error}`);
      return marketInfoCache.bySymbol || {};
    }
  }

  function getMarketLotSize(marketInfoBySymbol, symbol) {
    const row = marketInfoBySymbol && marketInfoBySymbol[String(symbol || "").trim().toUpperCase()]
      ? marketInfoBySymbol[String(symbol || "").trim().toUpperCase()]
      : null;
    const lotSize = row ? toNum(row.lotSize, NaN) : NaN;
    const minOrderSize = row ? toNum(row.minOrderSize, NaN) : NaN;
    if (Number.isFinite(lotSize) && lotSize > EPSILON) return lotSize;
    if (Number.isFinite(minOrderSize) && minOrderSize > EPSILON) return minOrderSize;
    return NaN;
  }

  function symbolAllowed(symbol, profile, risk) {
    const token = String(symbol || "").trim().toUpperCase();
    if (!token) return false;
    const global = risk && risk.global ? risk.global : {};
    const leader = profile && profile.allowedSymbols ? profile.allowedSymbols : [];
    const globalAllowed = Array.isArray(global.allowedSymbols) ? global.allowedSymbols : [];
    const globalBlocked = Array.isArray(global.blockedSymbols) ? global.blockedSymbols : [];
    if (globalAllowed.length && !globalAllowed.includes(token)) return false;
    if (globalBlocked.includes(token)) return false;
    return true;
  }

  function leaderRuleForWallet(risk, wallet) {
    const leaders = risk && risk.leaders && typeof risk.leaders === "object" ? risk.leaders : {};
    return leaders[String(wallet || "").trim().toUpperCase()] || null;
  }

  function effectiveLeverageCap(globalRisk, leaderRisk) {
    const globalCap = Number.isFinite(Number(globalRisk && globalRisk.leverageCap)) && Number(globalRisk.leverageCap) > 0
      ? Number(globalRisk.leverageCap)
      : null;
    const leaderCap = Number.isFinite(Number(leaderRisk && leaderRisk.leverageCap)) && Number(leaderRisk.leverageCap) > 0
      ? Number(leaderRisk.leverageCap)
      : null;
    if (globalCap && leaderCap) return Math.min(globalCap, leaderCap);
    return globalCap || leaderCap || null;
  }

  function getLeaderEquityUsd(payload) {
    const summary = payload && payload.summary && typeof payload.summary === "object" ? payload.summary : {};
    const account = summary.account && typeof summary.account === "object" ? summary.account : {};
    return toNum(
      summary.accountEquityUsd ??
        summary.equityUsd ??
        summary.accountEquity ??
        account.accountEquityUsd ??
        account.account_equity ??
        account.balance ??
        summary.balanceUsd,
      NaN
    );
  }

  function computeTargetUnits({
    policy,
    ratio,
    followerEquityUsd,
    leaderEquityUsd,
    leaderSizeUnits,
    leaderNotionalUsd,
    leaderPriceUsd,
    fixedNotionalUsd,
    maxAllocationPct,
    minNotionalUsd,
    lotSize,
  }) {
    const normalizedRatio = Number.isFinite(Number(ratio)) ? Math.max(0, Number(ratio)) : 1;
    const normalizedLeaderSize = Number.isFinite(Number(leaderSizeUnits)) ? Math.max(0, Number(leaderSizeUnits)) : 0;
    const normalizedNotional = Number.isFinite(Number(leaderNotionalUsd)) ? Math.max(0, Number(leaderNotionalUsd)) : NaN;
    const normalizedPrice = Number.isFinite(Number(leaderPriceUsd)) && Number(leaderPriceUsd) > 0 ? Number(leaderPriceUsd) : NaN;
    const normalizedFollowerEquity = Number.isFinite(Number(followerEquityUsd)) && Number(followerEquityUsd) > 0
      ? Number(followerEquityUsd)
      : NaN;
    const normalizedLeaderEquity = Number.isFinite(Number(leaderEquityUsd)) && Number(leaderEquityUsd) > 0
      ? Number(leaderEquityUsd)
      : NaN;
    const normalizedFixedNotional = Number.isFinite(Number(fixedNotionalUsd)) ? Math.max(0, Number(fixedNotionalUsd)) : 0;
    const normalizedMaxAllocationPct = Number.isFinite(Number(maxAllocationPct)) ? Math.max(0, Number(maxAllocationPct)) : 100;
    const normalizedMinNotionalUsd = Number.isFinite(Number(minNotionalUsd)) ? Math.max(0, Number(minNotionalUsd)) : 0;
    let desiredUnits = normalizedLeaderSize * normalizedRatio;

    if (policy === "fixed") {
      if (normalizedPrice > 0 && normalizedFixedNotional > 0) {
        desiredUnits = (normalizedFixedNotional * normalizedRatio) / normalizedPrice;
      }
    } else if (policy === "mirror") {
      if (normalizedPrice > 0 && normalizedNotional > 0 && normalizedFollowerEquity > 0 && normalizedLeaderEquity > 0) {
        const sourceExposure = normalizedNotional / normalizedLeaderEquity;
        desiredUnits = (normalizedFollowerEquity * sourceExposure * normalizedRatio) / normalizedPrice;
      }
    }

    if (normalizedPrice > 0 && normalizedFollowerEquity > 0 && normalizedMaxAllocationPct > 0) {
      const maxAllocationUsd = (normalizedFollowerEquity * normalizedMaxAllocationPct) / 100;
      const maxUnits = maxAllocationUsd / normalizedPrice;
      if (Number.isFinite(maxUnits) && maxUnits > 0) {
        desiredUnits = Math.min(desiredUnits, maxUnits);
      }
    }

    const snappedUnits = snapDownToStep(desiredUnits, lotSize);
    if (snappedUnits <= EPSILON) return 0;

    if (normalizedPrice > 0 && normalizedMinNotionalUsd > 0) {
      const finalNotionalUsd = snappedUnits * normalizedPrice;
      if (Number.isFinite(finalNotionalUsd) && finalNotionalUsd > 0 && finalNotionalUsd < normalizedMinNotionalUsd) {
        return 0;
      }
    }

    return Number.isFinite(snappedUnits) && snappedUnits > EPSILON ? snappedUnits : 0;
  }

  function buildOrderBody({
    accountWallet,
    agentWallet,
    agentPrivateKey,
    symbol,
    amount,
    side,
    reduceOnly,
    slippagePercent,
    clientOrderId,
  }) {
    const signed = createPacificaSignedRequest({
      accountWallet,
      agentWallet,
      agentPrivateKey,
      type: "create_market_order",
      operationData: {
        symbol,
        amount,
        side,
        slippage_percent: String(slippagePercent),
        reduce_only: Boolean(reduceOnly),
        client_order_id: clientOrderId,
      },
      expiryWindow: 30000,
    });
    return signed;
  }

  async function submitOrder({
    accountWallet,
    agentWallet,
    agentPrivateKey,
    symbol,
    amount,
    side,
    reduceOnly,
    slippagePercent,
    clientOrderId,
  }) {
    const signed = buildOrderBody({
      accountWallet,
      agentWallet,
      agentPrivateKey,
      symbol,
      amount,
      side,
      reduceOnly,
      slippagePercent,
      clientOrderId,
    });
    const result = await restClient.post("/orders/create_market", {
      timeoutMs: 15000,
      retryMaxAttempts: 2,
      body: signed.body,
    });
    return { signed, result };
  }

  async function buildExecutionPreview(store, userId) {
    const config = getExecutionConfig(store, userId);
    const accessRow = getRawApiKeyRow(store, userId, config.accessKeyId);
    if (!accessRow) {
      return {
        userId,
        ok: false,
        error: "pacifica access set not found",
      };
    }

    const accountWallet = String(accessRow.accountWallet || accessRow.account || "").trim();
    const agentWallet = String(accessRow.agentWallet || accessRow.apiKey || "").trim();
    const agentPrivateKey = String(accessRow.agentPrivateKey || accessRow.apiSecret || "").trim();
    if (!accountWallet || !agentWallet || !agentPrivateKey) {
      return {
        userId,
        ok: false,
        error: "pacifica access set incomplete",
      };
    }

    const profile = getCopyProfile(store, userId);
    const risk = getCopyRisk(store, userId);
    const leaders = getLeaderWalletsForUser(userId);
    const followerAccount = await fetchFollowerAccount(accountWallet);
    const marketInfoBySymbol = await fetchMarketInfoBySymbol(false);
    const baselineByWallet = { ...(getLeaderPositionBaselines(store, userId) || {}) };
    const followerEquityUsd = toNum(
      followerAccount && (followerAccount.account_equity ?? followerAccount.balance ?? followerAccount.equity),
      NaN
    );
    const leaderResults = await runWithConcurrency(leaders, maxConcurrentLeaders, async (leader) => {
      const wallet = String(leader && leader.wallet ? leader.wallet : "").trim();
      if (!wallet) {
        return { leaderWallet: "", ok: false, error: "invalid_leader_wallet" };
      }
      try {
        const payload = await fetchLeaderActivityPayload(wallet, { timeoutMs: 20000 });
        return { leaderWallet: wallet, ok: true, payload };
      } catch (error) {
        return {
          leaderWallet: wallet,
          ok: false,
          error: error && error.message ? String(error.message) : "leader_fetch_failed",
        };
      }
    });

    const fetchedLeaders = leaderResults.filter((row) => row && row.ok && row.payload);
    const targetMap = new Map();
    const skippedOrders = [];
    let baselineChanged = false;
    fetchedLeaders.forEach((row) => {
      const payload = row.payload || {};
      const positions = Array.isArray(payload.positions) ? payload.positions : [];
      const leaderEquityUsd = getLeaderEquityUsd(payload);
      const currentKeys = getPositionKeySet(positions);
      const leaderWallet = String(row.leaderWallet || "").trim();
      const leaderRisk = leaderRuleForWallet(risk, leaderWallet) || {};
      const baselineSnapshot = ensureLeaderBaselineSnapshot(store, userId, leaderWallet, currentKeys);
      const baseline = baselineSnapshot.baseline;
      if (baselineSnapshot.changed) {
        baselineByWallet[leaderWallet] = baseline;
        baselineChanged = true;
      }
      if (baselineSnapshot.initialized) {
        return;
      }
      const ratio = config.copyRatio || 1;
      positions.forEach((positionRow) => {
        const normalized = normalizePositionRow(positionRow);
        if (!normalized.token || !normalized.side) return;
        if (!Number.isFinite(normalized.size) || normalized.size <= 0) return;
        const key = buildPositionKey(normalized.token, normalized.side);
        if (baseline && baseline.ignoredKeys && baseline.ignoredKeys[key]) {
          skippedOrders.push({
            key,
            token: normalized.token,
            side: normalized.side,
            action: "open",
            reason: "baseline_ignored",
            lifecycleState: "BASELINE_IGNORED",
          });
          return;
        }
        if (!symbolAllowed(normalized.token, profile, risk)) {
          skippedOrders.push({
            key,
            token: normalized.token,
            side: normalized.side,
            action: "open",
            reason: "symbol_filtered",
          });
          return;
        }
        if (profile.pauseNewOpensOnly) {
          skippedOrders.push({
            key,
            token: normalized.token,
            side: normalized.side,
            action: "open",
            reason: "new_opens_paused",
          });
          return;
        }
        if (!profile.allowReopenAfterManualClose) {
          const manualRows = store.manualOverridesByUserId && store.manualOverridesByUserId[userId];
          const manualRow = manualRows && manualRows[key] ? manualRows[key] : null;
          if (manualRow && manualRow.protectFromReopen) {
            skippedOrders.push({
              key,
              token: normalized.token,
              side: normalized.side,
              action: "open",
              reason: "manual_close_protection",
            });
            return;
          }
        }
        const leverageCap = effectiveLeverageCap(risk.global || {}, leaderRisk);
        if (Number.isFinite(leverageCap) && leverageCap > 0 && Number(normalized.leverage) > leverageCap) {
          skippedOrders.push({
            key,
            token: normalized.token,
            side: normalized.side,
            action: "open",
            reason: "leader_leverage_capped",
          });
          return;
        }
        const desiredUnits = computeTargetUnits({
          policy: config.leveragePolicy,
          ratio,
          followerEquityUsd,
          leaderEquityUsd,
          leaderSizeUnits: normalized.size,
          leaderNotionalUsd: normalized.positionSizeUsd,
          leaderPriceUsd: normalized.markPrice,
          fixedNotionalUsd: config.fixedNotionalUsd,
          maxAllocationPct:
            Number.isFinite(Number(leaderRisk.maxAllocationPct)) && Number(leaderRisk.maxAllocationPct) > 0
              ? Math.min(Number(config.maxAllocationPct || 100), Number(leaderRisk.maxAllocationPct))
              : config.maxAllocationPct,
          minNotionalUsd: config.minNotionalUsd,
          lotSize: getMarketLotSize(marketInfoBySymbol, normalized.token),
        });
        if (!desiredUnits || desiredUnits <= 0) {
          skippedOrders.push({
            key,
            token: normalized.token,
            side: normalized.side,
            action: "open",
            reason: Number.isFinite(Number(config.minNotionalUsd)) && Number(config.minNotionalUsd) > 0
              ? "below_min_notional_or_size_too_small"
              : "sized_to_zero",
          });
          return;
        }
        const current = targetMap.get(key) || {
          token: normalized.token,
          side: normalized.side,
          size: 0,
          positionSizeUsd: 0,
          priceUsd: NaN,
          leverage: NaN,
          leaderWallets: [],
        };
        current.size += desiredUnits;
        const contributionUsd = Number.isFinite(normalized.markPrice) && normalized.markPrice > 0
          ? desiredUnits * normalized.markPrice
          : Number.isFinite(normalized.positionSizeUsd)
          ? normalized.positionSizeUsd * ratio
          : NaN;
        if (Number.isFinite(contributionUsd)) {
          current.positionSizeUsd += contributionUsd;
        }
        if (Number.isFinite(normalized.leverage) && normalized.leverage > 0) {
          current.leverage = Number(normalized.leverage);
        }
        if (!Number.isFinite(current.priceUsd) && Number.isFinite(normalized.markPrice) && normalized.markPrice > 0) {
          current.priceUsd = Number(normalized.markPrice);
        }
        current.leaderWallets.push(row.leaderWallet);
        targetMap.set(key, current);
      });
    });
    if (baselineChanged) {
      setLeaderPositionBaselines(store, userId, baselineByWallet);
      saveStore();
    }

    const followerPositions = await fetchFollowerPositions(accountWallet);
    const currentMap = aggregateCurrentPositions(followerPositions);
    const actions = buildActions({
      targetMap,
      currentMap,
    });
    if (debugCopyTrading) {
      logger.info?.(
        `[copy-trading] preview debug user=${userId} targetKeys=${targetMap.size} currentKeys=${currentMap.size} actions=${actions.length}`
      );
    }

    const previewOrders = [];
    for (const action of actions) {
      const side = buildSideForAction(action.side, action.deltaSize * (action.targetSize >= action.currentSize ? 1 : -1));
      const reduceOnly = action.action === "close" || action.action === "reduce";
      const lotSize = getMarketLotSize(marketInfoBySymbol, action.token);
      let amount = formatAmount(snapDownToStep(action.deltaSize, lotSize));
      const target = targetMap.get(action.key) || null;
      const targetPriceUsd = target && Number.isFinite(Number(target.priceUsd)) && Number(target.priceUsd) > 0
        ? Number(target.priceUsd)
        : target && Number.isFinite(Number(target.positionSizeUsd)) && Number.isFinite(Number(target.size)) && Number(target.size) > 0
        ? Number(target.positionSizeUsd) / Number(target.size)
        : NaN;
      const maxAllocationPct = Number.isFinite(Number(config.maxAllocationPct)) ? Math.max(0, Number(config.maxAllocationPct)) : 100;
      if (Number.isFinite(followerEquityUsd) && followerEquityUsd > 0 && Number.isFinite(targetPriceUsd) && targetPriceUsd > 0 && maxAllocationPct > 0) {
        const maxAllocationUsd = (followerEquityUsd * maxAllocationPct) / 100;
        const maxUnits = maxAllocationUsd / targetPriceUsd;
        if (Number.isFinite(maxUnits) && maxUnits > 0) {
          amount = formatAmount(snapDownToStep(Math.min(toNum(amount, 0), maxUnits), lotSize));
        }
      }
      if (Number(amount) <= 0) {
        skippedOrders.push({
          key: action.key,
          token: action.token,
          side: action.side,
          action: action.action,
          reason:
            Number.isFinite(Number(config.minNotionalUsd)) &&
            Number(config.minNotionalUsd) > 0 &&
            Number.isFinite(Number(targetPriceUsd)) &&
            Number(targetPriceUsd) > 0
              ? "below_min_notional"
              : "sized_to_zero",
        });
        continue;
      }
      const snapshotDigest = leaderResults
        .map((row) => {
          const payload = row && row.payload ? row.payload : {};
          const positions = Array.isArray(payload.positions) ? payload.positions : [];
          const openOrders = Array.isArray(payload.openOrders)
            ? payload.openOrders
            : Array.isArray(payload.orders)
            ? payload.orders
            : [];
          return JSON.stringify({
            leaderWallet: row.leaderWallet,
            positions: positions
              .map((item) => {
                const normalized = normalizePositionRow(item);
                return `${normalized.token}:${normalized.side}:${formatAmount(normalized.size)}`;
              })
              .sort(),
            openOrders: openOrders
              .map((item) => {
                const normalized = normalizeOrderRow(item);
                return `${normalized.token}:${normalized.side}:${formatAmount(normalized.size)}:${normalized.status}`;
              })
              .sort(),
          });
        })
        .sort()
        .join("|");
      const clientOrderId = hashToUuid(
        `${userId}:${accountWallet}:${action.key}:${action.action}:${amount}:${config.copyRatio}:${config.slippagePercent}:${snapshotDigest}`
      );
      const signed = buildOrderBody({
        accountWallet,
        agentWallet,
        agentPrivateKey,
        symbol: action.token,
        amount,
        side,
        reduceOnly,
        slippagePercent: config.slippagePercent || slippagePercentDefault,
        clientOrderId,
      });
      previewOrders.push({
        key: action.key,
        token: action.token,
        side: action.side,
        action: action.action,
        amount: Number(amount),
        reduceOnly,
        clientOrderId,
        signedBody: signed.body,
        derivedAgentWallet: signed.derivedAgentWallet,
        leaderWallets: targetMap.get(action.key) ? targetMap.get(action.key).leaderWallets || [] : [],
      });
    }

    return {
      ok: true,
      config,
      accountWallet,
      followerEquityUsd: Number.isFinite(followerEquityUsd) ? followerEquityUsd : null,
      leaders: leaders.length,
      actions: actions.length,
      previewOrders,
      skippedOrders,
    };
  }

  async function runUserCycle(store, userId) {
    const config = getExecutionConfig(store, userId);
    if (!config.enabled) {
      return {
        userId,
        enabled: false,
        skipped: "disabled",
      };
    }

    if (runtime.userLocks.has(userId)) {
      return {
        userId,
        enabled: true,
        skipped: "locked",
      };
    }

    const accessRow = getRawApiKeyRow(store, userId, config.accessKeyId);
    if (!accessRow) {
      const message = "pacifica access set not found";
      appendEvent(store, userId, {
        kind: "error",
        scope: "execution",
        message,
      });
      store.executionByUserId[userId] = {
        ...(store.executionByUserId[userId] || {}),
        lastError: message,
        lastRunAt: Date.now(),
        updatedAt: Date.now(),
      };
      return {
        userId,
        enabled: true,
        error: message,
      };
    }

    const accountWallet = String(accessRow.accountWallet || accessRow.account || "").trim();
    const agentWallet = String(accessRow.agentWallet || accessRow.apiKey || "").trim();
    const agentPrivateKey = String(accessRow.agentPrivateKey || accessRow.apiSecret || "").trim();
    if (!accountWallet || !agentWallet || !agentPrivateKey) {
      const message = "pacifica access set incomplete";
      appendEvent(store, userId, {
        kind: "error",
        scope: "execution",
        message,
      });
      store.executionByUserId[userId] = {
        ...(store.executionByUserId[userId] || {}),
        lastError: message,
        lastRunAt: Date.now(),
        updatedAt: Date.now(),
      };
      return {
        userId,
        enabled: true,
        error: message,
      };
    }

    const profile = getCopyProfile(store, userId);
    const risk = getCopyRisk(store, userId);
    const runtimeState = getRuntimeState(store, userId);

    runtime.userLocks.set(userId, Date.now());
    const startedAt = Date.now();
    let storeChanged = false;
    try {
      const leaders = getLeaderWalletsForUser(userId);
      const accessSets = getApiKeysForUser(userId);
      const followerAccount = await fetchFollowerAccount(accountWallet);
      const marketInfoBySymbol = await fetchMarketInfoBySymbol(false);
      const followerEquityUsd = toNum(
        followerAccount && (followerAccount.account_equity ?? followerAccount.balance ?? followerAccount.equity),
        NaN
      );
      const stopCloseAll = runtimeState.state === "close_all" || Boolean(runtimeState.closeAllRequested);
      if (!leaders.length && !stopCloseAll) {
        store.executionByUserId[userId] = {
          ...(store.executionByUserId[userId] || {}),
          lastRunAt: startedAt,
          lastSuccessAt: startedAt,
          lastError: null,
          updatedAt: startedAt,
        };
        appendEvent(store, userId, {
          kind: "info",
          scope: "execution",
          message: "No tracked leaders configured.",
        });
        storeChanged = true;
        return {
          userId,
          enabled: true,
          leaders: 0,
          actions: 0,
          ordersPlaced: 0,
        };
      }

      if (stopCloseAll) {
        saveCopyRuntimeStatusForUser(userId, {
          state: "close_all",
          closeAllRequested: true,
          health: "paused",
          lastStateChangeAt: startedAt,
        });
      }

      const leaderResults = await runWithConcurrency(leaders, maxConcurrentLeaders, async (leader) => {
        const wallet = String(leader && leader.wallet ? leader.wallet : "").trim();
        if (!wallet) {
          return { leaderWallet: "", ok: false, error: "invalid_leader_wallet" };
        }
        try {
          const payload = await fetchLeaderActivityPayload(wallet, { timeoutMs: 20000 });
          return {
            leaderWallet: wallet,
            ok: true,
            payload,
          };
        } catch (error) {
          return {
            leaderWallet: wallet,
            ok: false,
            error: error && error.message ? String(error.message) : "leader_fetch_failed",
          };
        }
      });

      const fetchedLeaders = leaderResults.filter((row) => row && row.ok && row.payload);
      const leaderSnapshotHashes = { ...getLeaderSnapshotHashes(store, userId) };
      const baselineByWallet = { ...(getLeaderPositionBaselines(store, userId) || {}) };
      let baselineChanged = false;
      fetchedLeaders.forEach((row) => {
        const payload = row.payload || {};
        const leaderPositions = Array.isArray(payload.positions) ? payload.positions : [];
        const currentKeys = getPositionKeySet(leaderPositions);
        const leaderWallet = String(row.leaderWallet || "").trim();
        const baselineSnapshot = ensureLeaderBaselineSnapshot(store, userId, leaderWallet, currentKeys);
        const baseline = baselineSnapshot.baseline;
        if (baselineSnapshot.changed) {
          baselineByWallet[leaderWallet] = baseline;
          baselineChanged = true;
        }
        if (baselineSnapshot.initialized) {
          return;
        }
        const leaderOrders = Array.isArray(payload.openOrders)
          ? payload.openOrders
          : Array.isArray(payload.orders)
          ? payload.orders
          : [];
        const snapshotHash = crypto
          .createHash("sha256")
          .update(
            JSON.stringify({
              positions: leaderPositions
                .map((item) => {
                  const normalized = normalizePositionRow(item);
                  return {
                    token: normalized.token,
                    side: normalized.side,
                    size: Number(normalized.size || 0),
                    openedAt: Number(normalized.openedAt || 0),
                  };
                })
                .sort((a, b) => `${a.token}:${a.side}`.localeCompare(`${b.token}:${b.side}`)),
              orders: leaderOrders
                .map((item) => {
                  const normalized = normalizeOrderRow(item);
                  return {
                    token: normalized.token,
                    side: normalized.side,
                    size: Number(normalized.size || 0),
                    price: Number(normalized.price || 0),
                    status: normalized.status,
                  };
                })
                .sort((a, b) => `${a.token}:${a.side}`.localeCompare(`${b.token}:${b.side}`)),
            })
          )
          .digest("hex");
        if (leaderSnapshotHashes[row.leaderWallet] && leaderSnapshotHashes[row.leaderWallet] !== snapshotHash) {
          appendEvent(store, userId, {
            kind: "leader_update",
            scope: "leader",
            leaderWallet: row.leaderWallet,
            message: "Leader activity changed.",
          });
          storeChanged = true;
        }
        leaderSnapshotHashes[row.leaderWallet] = snapshotHash;
      });
      if (baselineChanged) {
        setLeaderPositionBaselines(store, userId, baselineByWallet);
        storeChanged = true;
      }

      let targetMap = new Map();
      fetchedLeaders.forEach((row) => {
        const payload = row.payload || {};
        const positions = Array.isArray(payload.positions) ? payload.positions : [];
        const leaderEquityUsd = getLeaderEquityUsd(payload);
        const ratio = config.copyRatio || 1;
        const leaderWallet = String(row.leaderWallet || "").trim();
        const leaderRisk = leaderRuleForWallet(risk, leaderWallet) || {};
        const baseline = baselineByWallet[leaderWallet] || null;
        positions.forEach((positionRow) => {
          const normalized = normalizePositionRow(positionRow);
          if (!normalized.token || !normalized.side) return;
          if (!Number.isFinite(normalized.size) || normalized.size <= 0) return;
          const key = buildPositionKey(normalized.token, normalized.side);
          if (baseline && baseline.ignoredKeys && baseline.ignoredKeys[key]) {
            appendEvent(store, userId, {
              kind: "baseline",
              scope: "baseline",
              leaderWallet,
              token: normalized.token,
              side: normalized.side,
              action: "ignore",
              amount: 0,
              status: "ignored",
              message: "Baseline position ignored at follow start.",
              lifecycleState: "BASELINE_IGNORED",
            });
            storeChanged = true;
            return;
          }
          if (!symbolAllowed(normalized.token, profile, risk)) {
            appendEvent(store, userId, {
              kind: "preview",
              scope: "risk",
              token: normalized.token,
              side: normalized.side,
              action: "open",
              amount: 0,
              status: "skipped",
              message: "Blocked by symbol filter.",
              lifecycleState: lifecycleStateForLeaderPosition({
                baseline,
                key,
                action: "open",
              }),
            });
            storeChanged = true;
            return;
          }
          if (!profile.allowReopenAfterManualClose) {
            const manualRows = store.manualOverridesByUserId && store.manualOverridesByUserId[userId];
            const manualRow = manualRows && manualRows[key] ? manualRows[key] : null;
            if (manualRow && manualRow.protectFromReopen) {
              appendEvent(store, userId, {
                kind: "preview",
                scope: "risk",
                token: normalized.token,
                side: normalized.side,
                action: "open",
              amount: 0,
              status: "skipped",
              message: "Manual close protection is active.",
              lifecycleState: lifecycleStateForLeaderPosition({
                baseline,
                key,
                action: "open",
                manualBlocked: true,
              }),
            });
              storeChanged = true;
              return;
            }
          }
          const leverageCap = effectiveLeverageCap(risk.global || {}, leaderRisk);
          if (Number.isFinite(leverageCap) && leverageCap > 0 && Number(normalized.leverage) > leverageCap) {
            appendEvent(store, userId, {
              kind: "preview",
              scope: "risk",
              token: normalized.token,
              side: normalized.side,
              action: "open",
              amount: 0,
              status: "skipped",
              message: "Blocked by leverage cap.",
              lifecycleState: lifecycleStateForLeaderPosition({
                baseline,
                key,
                action: "open",
              }),
            });
            storeChanged = true;
            return;
          }
          const desiredUnits = computeTargetUnits({
            policy: config.leveragePolicy,
            ratio,
            followerEquityUsd,
            leaderEquityUsd,
            leaderSizeUnits: normalized.size,
            leaderNotionalUsd: normalized.positionSizeUsd,
            leaderPriceUsd: normalized.markPrice,
            fixedNotionalUsd: config.fixedNotionalUsd,
            maxAllocationPct:
              Number.isFinite(Number(leaderRisk.maxAllocationPct)) && Number(leaderRisk.maxAllocationPct) > 0
                ? Math.min(Number(config.maxAllocationPct || 100), Number(leaderRisk.maxAllocationPct))
                : config.maxAllocationPct,
            minNotionalUsd: config.minNotionalUsd,
            lotSize: getMarketLotSize(marketInfoBySymbol, normalized.token),
          });
          if (!desiredUnits || desiredUnits <= 0) {
          appendEvent(store, userId, {
            kind: "preview",
            scope: "execution",
            token: normalized.token,
            side: normalized.side,
              action: "open",
              amount: 0,
              reduceOnly: false,
              clientOrderId: hashToUuid(
                `${userId}:${accountWallet}:${normalized.token}:${normalized.side}:preview:${config.copyRatio}:${config.slippagePercent}`
              ),
            status: "skipped",
            message: Number.isFinite(Number(config.minNotionalUsd)) && Number(config.minNotionalUsd) > 0
              ? `Skipped open ${normalized.token}: below minimum notional ${formatAmount(config.minNotionalUsd)} USD or sized too small.`
              : `Skipped open ${normalized.token}: sized to zero.`,
            lifecycleState: lifecycleStateForLeaderPosition({
              baseline,
              key,
              action: "open",
            }),
          });
            storeChanged = true;
            return;
          }
          const current = targetMap.get(key) || {
            token: normalized.token,
            side: normalized.side,
            size: 0,
            positionSizeUsd: 0,
            priceUsd: NaN,
            leverage: NaN,
            leaderWallets: [],
          };
          current.size += desiredUnits;
          const contributionUsd = Number.isFinite(normalized.markPrice) && normalized.markPrice > 0
            ? desiredUnits * normalized.markPrice
            : Number.isFinite(normalized.positionSizeUsd)
            ? normalized.positionSizeUsd * ratio
            : NaN;
          if (Number.isFinite(contributionUsd)) {
            current.positionSizeUsd += contributionUsd;
          }
          if (Number.isFinite(normalized.leverage) && normalized.leverage > 0) {
            current.leverage = Number(normalized.leverage);
          }
          if (!Number.isFinite(current.priceUsd) && Number.isFinite(normalized.markPrice) && normalized.markPrice > 0) {
            current.priceUsd = Number(normalized.markPrice);
          }
          current.leaderWallets.push(row.leaderWallet);
          targetMap.set(key, current);
        });
      });

      const followerPositions = await fetchFollowerPositions(accountWallet);
      const currentMap = aggregateCurrentPositions(followerPositions);
      if (!store.copiedPositionsByUserId || typeof store.copiedPositionsByUserId !== "object") {
        store.copiedPositionsByUserId = {};
      }
      store.copiedPositionsByUserId[userId] = followerPositions
        .map((row) => {
          const normalized = normalizePositionRow(row);
          return {
            ...normalized,
            updatedAt: startedAt,
            source: "pacifica",
          };
        })
        .filter((row) => row.token && row.side);
      storeChanged = true;
      if (stopCloseAll) {
        targetMap = new Map();
      }
      const actions =
        runtimeState.state === "running" || runtimeState.state === "close_all"
          ? buildActions({
              targetMap,
              currentMap,
            })
          : [];
      if (debugCopyTrading) {
        logger.info?.(
          `[copy-trading] cycle debug user=${userId} targetKeys=${targetMap.size} currentKeys=${currentMap.size} actions=${actions.length}`
        );
      }

      const pendingByKey = { ...(getPendingByKey(store, userId) || {}) };
      const now = Date.now();
      Object.entries(pendingByKey).forEach(([key, pending]) => {
        const current = currentMap.get(key) || null;
        const target = targetMap.get(key) || null;
        const currentSize = current ? Number(current.size || 0) : 0;
        const targetSize = target ? Number(target.size || 0) : 0;
        const stillPending =
          pending &&
          Number.isFinite(Number(pending.expiresAt || 0)) &&
          Number(pending.expiresAt || 0) > now &&
          Math.abs(currentSize - targetSize) > 0.000001;
        if (!stillPending) {
          delete pendingByKey[key];
          storeChanged = true;
        }
      });

      let ordersPlaced = 0;
      const actionable = actions.filter((action) => {
        const pending = pendingByKey[action.key];
        if (!pending) return true;
        const nowTs = Date.now();
        if (Number(pending.expiresAt || 0) <= nowTs) return true;
        const sameTarget = Math.abs(Number(pending.targetSize || 0) - Number(action.targetSize || 0)) <= 0.000001;
        const sameCurrent = Math.abs(Number(pending.currentSize || 0) - Number(action.currentSize || 0)) <= 0.000001;
        return !(sameTarget && sameCurrent);
      });

      for (const originalAction of actionable) {
        if (ordersPlaced >= 100) break;
        let action = originalAction;
        const key = action.key;
        const target = targetMap.get(action.key) || null;
        const current = currentMap.get(action.key) || null;
        const currentSize = current ? Number(current.size || 0) : 0;
        const actionLeaderWallet =
          target && Array.isArray(target.leaderWallets) && target.leaderWallets[0]
            ? String(target.leaderWallets[0]).trim()
            : "";
        const baseline = actionLeaderWallet ? baselineByWallet[actionLeaderWallet] || null : null;
        const manualRows = store.manualOverridesByUserId && store.manualOverridesByUserId[userId];
        const manualRow = manualRows && manualRows[key] ? manualRows[key] : null;
        if (manualRow && manualRow.closeOnce && currentSize > EPSILON) {
          action = {
            ...action,
            action: "close",
            targetSize: 0,
            deltaSize: currentSize,
          };
        }
        if (profile.pauseNewOpensOnly && action.action === "open") {
          appendEvent(store, userId, {
            kind: "manual",
            scope: "execution",
            token: action.token,
            side: action.side,
            action: action.action,
            amount: 0,
            reduceOnly: false,
            clientOrderId: hashToUuid(`${userId}:${accountWallet}:${action.key}:paused-open`),
            status: "skipped",
            message: "New opens are paused.",
            lifecycleState: lifecycleStateForLeaderPosition({
              baseline,
              key,
              action: action.action,
              currentMap,
            }),
          });
          continue;
        }
        const side = buildSideForAction(action.side, action.deltaSize * (action.targetSize >= action.currentSize ? 1 : -1));
        const reduceOnly = action.action === "close" || action.action === "reduce";
        const lotSize = getMarketLotSize(marketInfoBySymbol, action.token);
        let amount = formatAmount(snapDownToStep(action.deltaSize, lotSize));
        if (action.action === "open" && manualRow && manualRow.protectFromReopen) {
          appendEvent(store, userId, {
            kind: "manual",
            scope: "execution",
            token: action.token,
            side: action.side,
            action: action.action,
            amount: 0,
            reduceOnly,
            clientOrderId: hashToUuid(`${userId}:${accountWallet}:${action.key}:manual-protect`),
            status: "skipped",
            message: "Position is protected from reopening after manual close.",
            lifecycleState: lifecycleStateForLeaderPosition({
              baseline,
              key,
              action: action.action,
              currentMap,
              manualBlocked: true,
            }),
          });
          continue;
        }
        const targetPriceUsd = target && Number.isFinite(Number(target.priceUsd)) && target.priceUsd > 0
          ? Number(target.priceUsd)
          : target && Number.isFinite(Number(target.positionSizeUsd)) && Number.isFinite(Number(target.size)) && target.size > 0
          ? Number(target.positionSizeUsd) / Number(target.size)
          : NaN;
        const globalRisk = risk && risk.global ? risk.global : {};
        const maxOpenPositions = Number.isFinite(Number(globalRisk.maxOpenPositions)) ? Math.max(0, Number(globalRisk.maxOpenPositions)) : null;
        if (action.action === "open" && maxOpenPositions && !currentMap.has(key) && currentMap.size >= maxOpenPositions) {
          appendEvent(store, userId, {
            kind: "risk",
            scope: "execution",
            token: action.token,
            side: action.side,
            action: action.action,
            amount: 0,
            reduceOnly,
            clientOrderId: hashToUuid(`${userId}:${accountWallet}:${action.key}:max-open-positions`),
            status: "skipped",
            message: "Blocked by max open positions.",
            lifecycleState: lifecycleStateForLeaderPosition({
              baseline,
              key,
              action: action.action,
              currentMap,
            }),
          });
          continue;
        }
        const maxAllocationPct = Number.isFinite(Number(config.maxAllocationPct)) ? Math.max(0, Number(config.maxAllocationPct)) : 100;
        if (Number.isFinite(followerEquityUsd) && followerEquityUsd > 0 && Number.isFinite(targetPriceUsd) && targetPriceUsd > 0 && maxAllocationPct > 0) {
          const maxAllocationUsd = (followerEquityUsd * maxAllocationPct) / 100;
          const maxUnits = maxAllocationUsd / targetPriceUsd;
          if (Number.isFinite(maxUnits) && maxUnits > 0) {
            amount = formatAmount(snapDownToStep(Math.min(toNum(amount, 0), maxUnits), lotSize));
          }
        }
        if (Number.isFinite(Number(globalRisk.totalCopiedExposureCapUsd)) && Number(globalRisk.totalCopiedExposureCapUsd) > 0 && Number.isFinite(targetPriceUsd) && targetPriceUsd > 0) {
          const totalExposureUsd = Array.from(currentMap.values()).reduce((sum, row) => sum + Number(row.positionSizeUsd || 0), 0);
          const nextExposureUsd = Number(amount) * targetPriceUsd;
          if (totalExposureUsd + nextExposureUsd > Number(globalRisk.totalCopiedExposureCapUsd) && action.action === "open") {
            appendEvent(store, userId, {
              kind: "risk",
              scope: "execution",
              token: action.token,
              side: action.side,
              action: action.action,
              amount: 0,
            reduceOnly,
            clientOrderId: hashToUuid(`${userId}:${accountWallet}:${action.key}:exposure-cap`),
            status: "skipped",
            message: "Blocked by total copied exposure cap.",
            lifecycleState: lifecycleStateForLeaderPosition({
              baseline,
              key,
              action: action.action,
              currentMap,
            }),
          });
          continue;
        }
        }
        const snapshotDigest = leaderResults
          .map((row) => {
            const payload = row && row.payload ? row.payload : {};
            const positions = Array.isArray(payload.positions) ? payload.positions : [];
            const openOrders = Array.isArray(payload.openOrders)
              ? payload.openOrders
              : Array.isArray(payload.orders)
              ? payload.orders
              : [];
            return JSON.stringify({
              leaderWallet: row.leaderWallet,
              positions: positions
                .map((item) => {
                  const normalized = normalizePositionRow(item);
                  return `${normalized.token}:${normalized.side}:${formatAmount(normalized.size)}`;
                })
                .sort(),
              openOrders: openOrders
                .map((item) => {
                  const normalized = normalizeOrderRow(item);
                  return `${normalized.token}:${normalized.side}:${formatAmount(normalized.size)}:${normalized.status}`;
                })
                .sort(),
            });
          })
          .sort()
          .join("|");
        const clientOrderId = hashToUuid(
          `${userId}:${accountWallet}:${action.key}:${action.action}:${amount}:${config.copyRatio}:${config.slippagePercent}:${snapshotDigest}`
        );
        if (Number(amount) <= 0) {
          appendEvent(store, userId, {
            kind: "preview",
            scope: "execution",
            token: action.token,
            side: action.side,
            action: action.action,
            amount: 0,
            reduceOnly,
            clientOrderId,
            status: "skipped",
            message: `Skipped ${action.action} ${action.token}: ${
              Number.isFinite(Number(config.minNotionalUsd)) &&
              Number(config.minNotionalUsd) > 0 &&
              Number.isFinite(Number(targetPriceUsd)) &&
              Number(targetPriceUsd) > 0
                ? `below minimum notional ${formatAmount(config.minNotionalUsd)} USD`
                : "sized to zero after allocation cap"
            }.`,
            lifecycleState: lifecycleStateForLeaderPosition({
              baseline,
              key,
              action: action.action,
              currentMap,
            }),
          });
          ordersPlaced += 1;
          storeChanged = true;
          continue;
        }
        try {
          if (!config.dryRun) {
            const submitted = await submitOrder({
              accountWallet,
              agentWallet,
              agentPrivateKey,
              symbol: action.token,
              amount,
              side,
              reduceOnly,
              slippagePercent: config.slippagePercent || slippagePercentDefault,
              clientOrderId,
            });
            appendEvent(store, userId, {
              kind: "order_submitted",
              scope: "execution",
              leaderWallets: targetMap.get(action.key)
                ? targetMap.get(action.key).leaderWallets || []
                : [],
              token: action.token,
              side: action.side,
              action: action.action,
              amount: Number(amount),
              reduceOnly,
              clientOrderId,
              status: "submitted",
              message: `Submitted ${action.action} ${action.token} ${amount}`,
              pacificaStatus: Number(submitted && submitted.result ? submitted.result.status || 0 : 0) || null,
              lifecycleState: lifecycleStateForLeaderPosition({
                baseline,
                key,
                action: action.action,
                currentMap,
              }),
            });
            pendingByKey[key] = {
              key,
              token: action.token,
              side: action.side,
              action: action.action,
              amount: Number(amount),
              reduceOnly,
              clientOrderId,
              submittedAt: now,
              expiresAt: now + 30000,
              targetSize: Number(action.targetSize || 0),
              currentSize: Number(action.currentSize || 0),
            };
          } else {
            appendEvent(store, userId, {
              kind: "preview",
              scope: "execution",
              token: action.token,
              side: action.side,
              action: action.action,
              amount: Number(amount),
              reduceOnly,
              clientOrderId,
              status: "dry_run",
              message: `Dry run ${action.action} ${action.token} ${amount}`,
            });
          }
          ordersPlaced += 1;
          storeChanged = true;
        } catch (error) {
          const message = error && error.message ? String(error.message) : "order_submission_failed";
          appendEvent(store, userId, {
            kind: "error",
            scope: "execution",
            token: action.token,
            side: action.side,
            action: action.action,
            amount: Number(amount),
            reduceOnly,
            clientOrderId,
            message,
          });
          storeChanged = true;
        }
      }

      store.executionByUserId[userId] = {
        ...(store.executionByUserId[userId] || {}),
        enabled: stopCloseAll ? false : true,
        dryRun: Boolean(config.dryRun),
        copyRatio: config.copyRatio,
        maxAllocationPct: config.maxAllocationPct,
        slippagePercent: config.slippagePercent,
        accessKeyId: config.accessKeyId || null,
        lastRunAt: now,
        lastSuccessAt: now,
        lastError: null,
        updatedAt: now,
      };
      if (stopCloseAll) {
        saveCopyRuntimeStatusForUser(userId, {
          state: "stopped_keep",
          closeAllRequested: false,
          health: "healthy",
          lastStateChangeAt: now,
        });
      }
      setLeaderSnapshotHashes(store, userId, leaderSnapshotHashes);
      setPendingByKey(store, userId, pendingByKey);
      storeChanged = true;
      appendEvent(store, userId, {
        kind: "cycle",
        scope: "execution",
        message: config.dryRun
          ? `Dry run complete: ${actions.length} target diffs, ${ordersPlaced} previews`
          : `Execution complete: ${actions.length} target diffs, ${ordersPlaced} orders submitted`,
        leaders: leaders.length,
        diffs: actions.length,
        ordersPlaced,
        dryRun: Boolean(config.dryRun),
      });
      return {
        userId,
        enabled: true,
        leaders: leaders.length,
        diffs: actions.length,
        ordersPlaced,
        dryRun: Boolean(config.dryRun),
      };
    } catch (error) {
      const message = error && error.message ? String(error.message) : "execution_cycle_failed";
      store.executionByUserId[userId] = {
        ...(store.executionByUserId[userId] || {}),
        lastRunAt: Date.now(),
        lastError: message,
        updatedAt: Date.now(),
      };
      appendEvent(store, userId, {
        kind: "error",
        scope: "execution",
        message,
      });
      storeChanged = true;
      throw error;
    } finally {
      runtime.userLocks.delete(userId);
      if (storeChanged) {
        saveStore();
      }
    }
  }

  async function runCycle() {
    if (runtime.inFlight) {
      return {
        ok: false,
        skipped: "already running",
      };
    }
    runtime.inFlight = true;
    runtime.lastStartedAt = Date.now();
    let store = null;
    try {
      store = ensureStoreShape(loadStore());
      const users = Object.keys(store.executionByUserId || {}).filter((userId) => {
        const config = getExecutionConfig(store, userId);
        if (!config.enabled) return false;
        const accessRow = getRawApiKeyRow(store, userId, config.accessKeyId);
        if (!accessRow) return false;
        const leaders = getLeaderWalletsForUser(userId);
        return leaders.length > 0;
      });
      const userResults = [];
      for (const userId of users) {
        try {
          const result = await runUserCycle(store, userId);
          userResults.push(result);
        } catch (error) {
          const message = error && error.message ? String(error.message) : "user_cycle_failed";
          runtime.lastError = message;
          userResults.push({ userId, enabled: true, error: message });
          logger.warn(`[copy-trading-execution] user=${userId} failed: ${message}`);
        }
      }
      runtime.lastSuccessAt = Date.now();
      runtime.lastError = null;
      runtime.lastSummary = {
        ok: true,
        users: userResults.length,
        activeUsers: userResults.filter((row) => row && row.enabled).length,
        results: userResults,
      };
      return runtime.lastSummary;
    } catch (error) {
      const message = error && error.message ? String(error.message) : "execution_cycle_failed";
      runtime.lastError = message;
      runtime.lastSummary = {
        ok: false,
        error: message,
      };
      throw error;
    } finally {
      runtime.inFlight = false;
      runtime.lastFinishedAt = Date.now();
      runtime.lastDurationMs = runtime.lastFinishedAt - Number(runtime.lastStartedAt || runtime.lastFinishedAt);
    }
  }

  function getStatus() {
    return {
      running: runtime.running,
      inFlight: runtime.inFlight,
      nextRunAt: runtime.nextRunAt,
      lastStartedAt: runtime.lastStartedAt,
      lastFinishedAt: runtime.lastFinishedAt,
      lastDurationMs: runtime.lastDurationMs,
      lastSuccessAt: runtime.lastSuccessAt,
      lastError: runtime.lastError,
      lastSummary: runtime.lastSummary,
    };
  }

  return {
    start() {
      runtime.running = true;
    },
    stop() {
      runtime.running = false;
    },
    async runCycle() {
      if (!runtime.running) {
        runtime.running = true;
      }
      return runCycle();
    },
    async previewExecution(userId) {
      const store = ensureStoreShape(loadStore());
      return buildExecutionPreview(store, userId);
    },
    getStatus,
    getUserExecutionView(userId) {
      const store = ensureStoreShape(loadStore());
      const config = getExecutionConfig(store, userId);
      const status = getExecutionStatus(store, userId);
      return {
        ok: true,
        config,
        status,
      };
    },
    async updateUserConfig(userId, patch = {}) {
      const store = ensureStoreShape(loadStore());
      if (!store.executionByUserId[userId]) store.executionByUserId[userId] = {};
      const current = store.executionByUserId[userId];
      const next = {
        ...current,
        enabled: patch.enabled !== undefined ? Boolean(patch.enabled) : Boolean(current.enabled),
        dryRun: patch.dryRun !== undefined ? Boolean(patch.dryRun) : current.dryRun === undefined ? dryRunDefault : Boolean(current.dryRun),
        copyRatio: patch.copyRatio !== undefined && Number.isFinite(Number(patch.copyRatio))
          ? Math.max(0, Number(patch.copyRatio))
          : Number.isFinite(Number(current.copyRatio))
          ? Math.max(0, Number(current.copyRatio))
          : 1,
        maxAllocationPct: patch.maxAllocationPct !== undefined && Number.isFinite(Number(patch.maxAllocationPct))
          ? Math.max(0, Number(patch.maxAllocationPct))
          : Number.isFinite(Number(current.maxAllocationPct))
          ? Math.max(0, Number(current.maxAllocationPct))
          : 100,
        minNotionalUsd: patch.minNotionalUsd !== undefined && Number.isFinite(Number(patch.minNotionalUsd))
          ? Math.max(0, Number(patch.minNotionalUsd))
          : Number.isFinite(Number(current.minNotionalUsd))
          ? Math.max(0, Number(current.minNotionalUsd))
          : 1,
        slippagePercent: patch.slippagePercent !== undefined && Number.isFinite(Number(patch.slippagePercent))
          ? Math.max(0, Number(patch.slippagePercent))
          : Number.isFinite(Number(current.slippagePercent))
          ? Math.max(0, Number(current.slippagePercent))
          : slippagePercentDefault,
        leveragePolicy: ["mirror", "cap", "fixed"].includes(String(patch.leveragePolicy || current.leveragePolicy || "").trim())
          ? String(patch.leveragePolicy || current.leveragePolicy).trim()
          : ["mirror", "cap", "fixed"].includes(String(current.leveragePolicy || "").trim())
          ? String(current.leveragePolicy).trim()
          : "cap",
        fixedNotionalUsd: patch.fixedNotionalUsd !== undefined && Number.isFinite(Number(patch.fixedNotionalUsd))
          ? Math.max(0, Number(patch.fixedNotionalUsd))
          : Number.isFinite(Number(current.fixedNotionalUsd))
          ? Math.max(0, Number(current.fixedNotionalUsd))
          : 50,
        retryPolicy: ["aggressive", "balanced", "conservative"].includes(String(patch.retryPolicy || current.retryPolicy || "").trim())
          ? String(patch.retryPolicy || current.retryPolicy).trim()
          : ["aggressive", "balanced", "conservative"].includes(String(current.retryPolicy || "").trim())
          ? String(current.retryPolicy).trim()
          : "balanced",
        syncBehavior: ["baseline_only", "mirror_all", "safe_pause"].includes(String(patch.syncBehavior || current.syncBehavior || "").trim())
          ? String(patch.syncBehavior || current.syncBehavior).trim()
          : ["baseline_only", "mirror_all", "safe_pause"].includes(String(current.syncBehavior || "").trim())
          ? String(current.syncBehavior).trim()
          : "baseline_only",
        accessKeyId: patch.accessKeyId !== undefined ? String(patch.accessKeyId || "").trim() : String(current.accessKeyId || "").trim(),
        updatedAt: Date.now(),
      };
      store.executionByUserId[userId] = next;
      saveStore();
      return getExecutionConfig(store, userId);
    },
  };
}

module.exports = {
  createCopyTradingExecutionWorker,
  aggregateCurrentPositions,
  aggregatePositions,
  buildActions,
  buildPositionKey,
  extractPayloadData,
  formatAmount,
  hashToUuid,
  normalizeOrderRow,
  normalizePositionRow,
};
