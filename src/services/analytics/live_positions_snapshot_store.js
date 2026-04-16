const fs = require("fs");
const path = require("path");
const { ensureDir, readJson, writeJsonAtomic } = require("../pipeline/utils");

const SHARD_FILE_PREFIX = "wallet_first_shard_";
const ACCOUNT_SHARD_FILE_PREFIX = "wallet_first_account_shard_";
const SHARD_FILE_SUFFIX = ".json";
const MAX_CACHED_EVENTS = 5000;
const MAX_CACHED_POSITION_OPENED_EVENTS = Math.max(
  MAX_CACHED_EVENTS,
  Number(process.env.PACIFICA_LIVE_POSITIONS_MAX_CACHED_OPEN_EVENTS || 20000)
);
const MIN_MERGED_CACHE_RELOAD_MS = Math.max(
  100,
  Number(process.env.PACIFICA_LIVE_POSITIONS_MERGED_CACHE_MIN_RELOAD_MS || 500)
);
const mergedShardSnapshotCache = new Map();

function buildShardSnapshotPath(dirPath, shardIndex = 0) {
  const safeIndex = Math.max(0, Math.floor(Number(shardIndex || 0)));
  return path.join(dirPath, `${SHARD_FILE_PREFIX}${safeIndex}${SHARD_FILE_SUFFIX}`);
}

function listShardSnapshotFiles(dirPath) {
  if (!dirPath || !fs.existsSync(dirPath)) return [];
  try {
    const prefixes = [SHARD_FILE_PREFIX, ACCOUNT_SHARD_FILE_PREFIX];
    return fs
      .readdirSync(dirPath)
      .filter(
        (name) =>
          prefixes.some((prefix) => name.startsWith(prefix)) &&
          name.endsWith(SHARD_FILE_SUFFIX)
      )
      .map((name) => path.join(dirPath, name))
      .sort();
  } catch (_error) {
    return [];
  }
}

function getFileSignature(filePath) {
  try {
    const stat = fs.statSync(filePath);
    return `${filePath}:${Number(stat.mtimeMs || 0)}:${Number(stat.size || 0)}`;
  } catch (_error) {
    return `${filePath}:0:0`;
  }
}

function buildSnapshotCollectionSignature(files = []) {
  return (Array.isArray(files) ? files : []).map((filePath) => getFileSignature(filePath)).join("|");
}

function trimMergedSnapshotEvents(snapshot = null, eventsLimit = 300) {
  const safeLimit = Math.max(1, Math.min(MAX_CACHED_EVENTS, Number(eventsLimit || 300)));
  const safeOpenedLimit = Math.max(
    safeLimit,
    Math.min(MAX_CACHED_POSITION_OPENED_EVENTS, Number(eventsLimit || 300))
  );
  if (!snapshot || typeof snapshot !== "object") {
    return mergeShardSnapshots([], { eventsLimit: safeLimit });
  }
  return {
    ...snapshot,
    events: Array.isArray(snapshot.events) ? snapshot.events.slice(0, safeLimit) : [],
    positionOpenedEvents: Array.isArray(snapshot.positionOpenedEvents)
      ? snapshot.positionOpenedEvents.slice(0, safeOpenedLimit)
      : [],
  };
}

function positionIdentity(row = {}) {
  const wallet = String(row.wallet || "").trim();
  const key = String(row.positionKey || "").trim();
  if (wallet && key) return `${wallet}|${key}`;
  const symbol = String(row.symbol || "").trim().toUpperCase();
  const side = String(row.rawSide || row.side || "").trim().toLowerCase();
  const isolated = row.isolated ? "iso" : "cross";
  const entry = Number.isFinite(Number(row.entry))
    ? Number(row.entry).toFixed(8)
    : "na";
  return `${wallet}|${symbol}|${side}|${isolated}|${entry}`;
}

function positionFreshnessTs(row = {}) {
  return Math.max(
    0,
    Number(row.lastObservedAt || 0),
    Number(row.observedAt || 0),
    Number(row.timestamp || 0),
    Number(row.lastUpdatedAt || 0),
    Number(row.updatedAt || 0),
    Number(row.openedAt || 0)
  );
}

function normalizeSnapshot(raw = {}) {
  const status =
    raw && raw.status && typeof raw.status === "object"
      ? raw.status
      : { enabled: false };
  return {
    generatedAt: Number(raw.generatedAt || Date.now()),
    shardIndex: Number(raw.shardIndex || status.shardIndex || 0),
    shardCount: Number(raw.shardCount || status.shardCount || 1),
    status,
    positions: Array.isArray(raw.positions) ? raw.positions : [],
    liveStates: Array.isArray(raw.liveStates) ? raw.liveStates : [],
    events: Array.isArray(raw.events) ? raw.events : [],
    positionOpenedEvents: Array.isArray(raw.positionOpenedEvents)
      ? raw.positionOpenedEvents
      : [],
    successScannedWallets: Array.isArray(raw.successScannedWallets)
      ? raw.successScannedWallets
      : [],
  };
}

function openedEventIdentity(row = {}) {
  const wallet = String((row && row.wallet) || "").trim();
  const positionKey = String((row && (row.positionKey || row.key)) || "").trim();
  const symbol = String((row && row.symbol) || "").trim().toUpperCase();
  const side = String((row && row.side) || "").trim().toLowerCase();
  const openedAt = Math.max(
    0,
    Number((row && (row.openedAt || row.at || row.timestamp)) || 0)
  );
  if (wallet && positionKey && openedAt > 0) {
    return `${wallet}|${positionKey}|${openedAt}`;
  }
  return `${wallet}|${symbol}|${side}|${openedAt}`;
}

function openedEventFreshnessTs(row = {}) {
  return Math.max(
    0,
    Number((row && (row.openedAt || row.at || row.timestamp)) || 0),
    Number((row && row.observedAt) || 0),
    Number((row && row.updatedAt) || 0)
  );
}

function liveStateFreshnessTs(row = {}) {
  return Math.max(
    0,
    Number(row.lastAccountAt || 0),
    Number(row.lastPositionsAt || 0),
    Number(row.lastMaterializedOpenAt || 0),
    Number(row.lastHintOpenAt || 0),
    Number(row.lastStateChangeAt || 0),
    Number(row.updatedAt || 0)
  );
}

function mergeLiveStateRows(existing = null, incoming = null) {
  const current = existing && typeof existing === "object" ? existing : {};
  const next = incoming && typeof incoming === "object" ? incoming : {};
  const currentFreshness = liveStateFreshnessTs(current);
  const nextFreshness = liveStateFreshnessTs(next);
  const freshestFirst =
    nextFreshness >= currentFreshness
      ? { ...current, ...next }
      : { ...next, ...current };
  const merged = {
    ...freshestFirst,
    wallet: String(next.wallet || current.wallet || "").trim(),
  };
  [
    "lastStateChangeAt",
    "lastAccountAt",
    "lastPositionsAt",
    "lastHintOpenAt",
    "lastMaterializedOpenAt",
    "lastConfirmedClosedAt",
    "lastPositionsErrorAt",
  ].forEach((key) => {
    merged[key] = Math.max(0, Number(current[key] || 0), Number(next[key] || 0));
  });
  [
    "accountPositionsCount",
    "materializedPositionsCount",
    "accountZeroStreak",
    "positionsEmptyStreak",
  ].forEach((key) => {
    merged[key] = Math.max(0, Number(current[key] || 0), Number(next[key] || 0));
  });
  const currentErrorAt = Math.max(0, Number(current.lastPositionsErrorAt || 0));
  const nextErrorAt = Math.max(0, Number(next.lastPositionsErrorAt || 0));
  if (nextErrorAt >= currentErrorAt) {
    merged.lastPositionsError = next.lastPositionsError || null;
  } else {
    merged.lastPositionsError = current.lastPositionsError || null;
  }
  const currentLastOpenedAt = Math.max(0, Number(current.lastOpenedPositionAt || 0));
  const nextLastOpenedAt = Math.max(0, Number(next.lastOpenedPositionAt || 0));
  const currentLastOpenedUpdatedAt = Math.max(
    0,
    Number(current.lastOpenedPositionUpdatedAt || 0),
    Number(current.lastOpenedPositionObservedAt || 0)
  );
  const nextLastOpenedUpdatedAt = Math.max(
    0,
    Number(next.lastOpenedPositionUpdatedAt || 0),
    Number(next.lastOpenedPositionObservedAt || 0)
  );
  const lastOpenedSource =
    nextLastOpenedAt > currentLastOpenedAt ||
    (nextLastOpenedAt === currentLastOpenedAt &&
      nextLastOpenedUpdatedAt >= currentLastOpenedUpdatedAt)
      ? next
      : current;
  [
    "lastOpenedPositionAt",
    "lastOpenedPositionObservedAt",
    "lastOpenedPositionUpdatedAt",
  ].forEach((key) => {
    merged[key] = Math.max(0, Number(lastOpenedSource[key] || 0)) || null;
  });
  [
    "lastOpenedPositionKey",
    "lastOpenedPositionSymbol",
    "lastOpenedPositionSide",
    "lastOpenedPositionDirection",
    "lastOpenedPositionSource",
    "lastOpenedPositionConfidence",
  ].forEach((key) => {
    merged[key] = lastOpenedSource[key] ? String(lastOpenedSource[key]) : null;
  });
  [
    "lastOpenedPositionEntry",
    "lastOpenedPositionMark",
    "lastOpenedPositionPnlUsd",
  ].forEach((key) => {
    merged[key] = Number.isFinite(Number(lastOpenedSource[key])) ? Number(lastOpenedSource[key]) : null;
  });
  [
    "lastOpenedPositionSize",
    "lastOpenedPositionUsd",
  ].forEach((key) => {
    merged[key] = Number.isFinite(Number(lastOpenedSource[key])) ? Number(lastOpenedSource[key]) : 0;
  });
  merged.pendingBootstrap = Boolean(
    nextFreshness >= currentFreshness
      ? next.pendingBootstrap
      : current.pendingBootstrap
  );
  return merged;
}

function loadShardSnapshot(filePath) {
  const raw = readJson(filePath, null);
  if (!raw || typeof raw !== "object") return null;
  return normalizeSnapshot(raw);
}

function writeShardSnapshot(filePath, payload) {
  ensureDir(path.dirname(filePath));
  writeJsonAtomic(filePath, payload);
}

function mergeShardSnapshots(snapshots = [], options = {}) {
  const eventsLimit = Math.max(
    1,
    Math.min(5000, Number(options.eventsLimit || 300))
  );
  const positionOpenedEventsLimit = Math.max(
    eventsLimit,
    Math.min(
      MAX_CACHED_POSITION_OPENED_EVENTS,
      Number(options.positionOpenedEventsLimit || options.eventsLimit || 300)
    )
  );
  const valid = (Array.isArray(snapshots) ? snapshots : []).filter(Boolean);
  if (!valid.length) {
    return {
      generatedAt: Date.now(),
      status: { enabled: false, mode: "wallet_first_external_shards" },
      positions: [],
      events: [],
      positionOpenedEvents: [],
      successScannedWallets: [],
    };
  }

  const positionMap = new Map();
  const liveStateMap = new Map();
  const walletOpenSet = new Set();
  const eventRows = [];
  const positionOpenedEventMap = new Map();
  const successWalletSet = new Set();

  let walletsKnown = 0;
  let walletsKnownGlobal = 0;
  let scannedWalletsTotal = 0;
  let failedWalletsTotal = 0;
  let passes = 0;
  let passThroughputRps = 0;
  let clientsTotal = 0;
  let proxiedClients = 0;
  let clientsCooling = 0;
  let clientsDisabled = 0;
  let clientsInFlight = 0;
  let clients429 = 0;
  let directClientIncluded = false;
  let hotWalletsPerPass = 0;
  let warmWalletsPerPass = 0;
  let recentActiveWalletsPerPass = 0;
  let maxConcurrency = 0;
  let batchTargetWallets = 0;
  let avgWalletScanMs = 0;
  let targetPassDurationMs = 0;
  let recentActiveWallets = 0;
  let warmWallets = 0;
  let publicActiveWallets = 0;
  let estimatedHotLoopSeconds = 0;
  let estimatedWarmLoopSeconds = 0;
  let hotWsActiveWallets = 0;
  let hotWsOpenConnections = 0;
  let hotWsDroppedPromotions = 0;
  let hotWsCapacity = 0;
  let hotWsAvailableSlots = 0;
  let hotWsCapacityCeiling = 0;
  let hotWsPromotionBacklog = 0;
  let hotWsTriggerToEventAvgMs = 0;
  let hotWsLastTriggerToEventMs = 0;
  let hotWsReconnectTransitions = 0;
  let hotWsErrorCount = 0;
  let hotWsScaleEvents = 0;
  let hotWsProcessRssMb = 0;
  let requestTimeoutMs = 0;
  let accountRequestTimeoutMs = 0;
  let positionsRequestTimeoutMs = 0;
  let maxFetchAttempts = 0;
  let maxInFlightPerClient = 0;
  let maxInFlightDirect = 0;
  let healthyClients = 0;
  let avgClientLatencyMs = 0;
  let timeoutClients = 0;
  let proxyFailingClients = 0;
  let positionsClientsTotal = 0;
  let positionsProxiedClients = 0;
  let positionsClientsCooling = 0;
  let positionsClientsDisabled = 0;
  let positionsClientsInFlight = 0;
  let positionsClients429 = 0;
  let positionsHealthyClients = 0;
  let positionsAvgClientLatencyMs = 0;
  let positionsTimeoutClients = 0;
  let positionsProxyFailingClients = 0;
  let accountClientsTotal = 0;
  let accountProxiedClients = 0;
  let accountClientsCooling = 0;
  let accountClientsDisabled = 0;
  let accountClientsInFlight = 0;
  let accountClients429 = 0;
  let accountHealthyClients = 0;
  let accountAvgClientLatencyMs = 0;
  let accountTimeoutClients = 0;
  let accountProxyFailingClients = 0;
  let accountLastError = null;
  let accountLastErrorAt = 0;
  let lifecycleHotWallets = 0;
  let lifecycleWarmWallets = 0;
  let lifecycleColdWallets = 0;
  let accountIndicatedOpenWallets = 0;
  let accountIndicatedOpenPositionsTotal = 0;
  let positionMaterializationGapWallets = 0;
  let positionMaterializationGapTotal = 0;
  let accountRepairDueWallets = 0;
  let liveStateHintedWallets = 0;
  let liveStateMaterializedWallets = 0;
  let liveStateUncertainWallets = 0;
  let liveStateClosedWallets = 0;
  let liveStatePendingBootstrapWallets = 0;
  let closeConfirmAccountZeroThreshold = 0;
  let closeConfirmPositionsEmptyThreshold = 0;
  let staleWallets = 0;
  let freshWallets = 0;
  let coolingWallets = 0;
  let priorityQueueDepth = 0;
  let hotReconcileDueWallets = 0;
  let warmReconcileDueWallets = 0;
  let scanIntervalMs = 0;
  let staleMs = 0;
  let coolingMs = 0;
  let running = false;
  let lastSuccessAt = 0;
  let lastErrorAt = 0;
  let lastEventAt = 0;
  let lastPassStartedAt = 0;
  let lastPassFinishedAt = 0;
  let lastPassDurationMs = 0;
  let lastError = null;
  let directFallbackOnLastAttempt = false;
  let shardCount = 0;
  let enabled = false;

  valid.forEach((snapshot) => {
    const status = snapshot.status || {};
    enabled = enabled || Boolean(status.enabled);
    walletsKnown += Math.max(0, Number(status.walletsKnown || 0));
    walletsKnownGlobal = Math.max(
      walletsKnownGlobal,
      Math.max(0, Number(status.walletsKnownGlobal || status.walletsKnown || 0))
    );
    scannedWalletsTotal += Math.max(0, Number(status.scannedWalletsTotal || 0));
    failedWalletsTotal += Math.max(0, Number(status.failedWalletsTotal || 0));
    passes += Math.max(0, Number(status.passes || 0));
    passThroughputRps += Math.max(0, Number(status.passThroughputRps || 0));
    clientsTotal += Math.max(0, Number(status.clientsTotal || 0));
    proxiedClients += Math.max(0, Number(status.proxiedClients || 0));
    clientsCooling += Math.max(0, Number(status.clientsCooling || 0));
    clientsDisabled += Math.max(0, Number(status.clientsDisabled || 0));
    clientsInFlight += Math.max(0, Number(status.clientsInFlight || 0));
    clients429 += Math.max(0, Number(status.clients429 || 0));
    directClientIncluded =
      directClientIncluded || Boolean(status.directClientIncluded);
    hotWalletsPerPass = Math.max(
      hotWalletsPerPass,
      Math.max(0, Number(status.hotWalletsPerPass || 0))
    );
    warmWalletsPerPass = Math.max(
      warmWalletsPerPass,
      Math.max(0, Number(status.warmWalletsPerPass || 0))
    );
    recentActiveWalletsPerPass = Math.max(
      recentActiveWalletsPerPass,
      Math.max(0, Number(status.recentActiveWalletsPerPass || 0))
    );
    maxConcurrency += Math.max(0, Number(status.maxConcurrency || 0));
    batchTargetWallets += Math.max(0, Number(status.batchTargetWallets || 0));
    avgWalletScanMs = Math.max(
      avgWalletScanMs,
      Math.max(0, Number(status.avgWalletScanMs || 0))
    );
    targetPassDurationMs = Math.max(
      targetPassDurationMs,
      Math.max(0, Number(status.targetPassDurationMs || 0))
    );
    recentActiveWallets += Math.max(0, Number(status.recentActiveWallets || 0));
    warmWallets += Math.max(0, Number(status.warmWallets || 0));
    publicActiveWallets += Math.max(0, Number(status.publicActiveWallets || 0));
    estimatedHotLoopSeconds = Math.max(
      estimatedHotLoopSeconds,
      Math.max(0, Number(status.estimatedHotLoopSeconds || 0))
    );
    estimatedWarmLoopSeconds = Math.max(
      estimatedWarmLoopSeconds,
      Math.max(0, Number(status.estimatedWarmLoopSeconds || 0))
    );
    hotWsActiveWallets += Math.max(0, Number(status.hotWsActiveWallets || 0));
    hotWsOpenConnections += Math.max(0, Number(status.hotWsOpenConnections || 0));
    hotWsDroppedPromotions += Math.max(0, Number(status.hotWsDroppedPromotions || 0));
    hotWsCapacity += Math.max(0, Number(status.hotWsCapacity || 0));
    hotWsAvailableSlots += Math.max(0, Number(status.hotWsAvailableSlots || 0));
    hotWsCapacityCeiling += Math.max(0, Number(status.hotWsCapacityCeiling || 0));
    hotWsPromotionBacklog += Math.max(0, Number(status.hotWsPromotionBacklog || 0));
    hotWsTriggerToEventAvgMs = Math.max(
      hotWsTriggerToEventAvgMs,
      Math.max(0, Number(status.hotWsTriggerToEventAvgMs || 0))
    );
    hotWsLastTriggerToEventMs = Math.max(
      hotWsLastTriggerToEventMs,
      Math.max(0, Number(status.hotWsLastTriggerToEventMs || 0))
    );
    hotWsReconnectTransitions += Math.max(
      0,
      Number(status.hotWsReconnectTransitions || 0)
    );
    hotWsErrorCount += Math.max(0, Number(status.hotWsErrorCount || 0));
    hotWsScaleEvents += Math.max(0, Number(status.hotWsScaleEvents || 0));
    hotWsProcessRssMb = Math.max(
      hotWsProcessRssMb,
      Math.max(0, Number(status.hotWsProcessRssMb || 0))
    );
    requestTimeoutMs = Math.max(
      requestTimeoutMs,
      Math.max(0, Number(status.requestTimeoutMs || 0))
    );
    accountRequestTimeoutMs = Math.max(
      accountRequestTimeoutMs,
      Math.max(0, Number(status.accountRequestTimeoutMs || 0))
    );
    positionsRequestTimeoutMs = Math.max(
      positionsRequestTimeoutMs,
      Math.max(
        0,
        Number(status.positionsRequestTimeoutMs || status.requestTimeoutMs || 0)
      )
    );
    maxFetchAttempts = Math.max(
      maxFetchAttempts,
      Math.max(0, Number(status.maxFetchAttempts || 0))
    );
    maxInFlightPerClient = Math.max(
      maxInFlightPerClient,
      Math.max(0, Number(status.maxInFlightPerClient || 0))
    );
    maxInFlightDirect = Math.max(
      maxInFlightDirect,
      Math.max(0, Number(status.maxInFlightDirect || 0))
    );
    healthyClients += Math.max(0, Number(status.healthyClients || 0));
    avgClientLatencyMs = Math.max(
      avgClientLatencyMs,
      Math.max(0, Number(status.avgClientLatencyMs || 0))
    );
    timeoutClients += Math.max(0, Number(status.timeoutClients || 0));
    proxyFailingClients += Math.max(0, Number(status.proxyFailingClients || 0));
    positionsClientsTotal += Math.max(
      0,
      Number(status.positionsClientsTotal || status.clientsTotal || 0)
    );
    positionsProxiedClients += Math.max(
      0,
      Number(status.positionsProxiedClients || status.proxiedClients || 0)
    );
    positionsClientsCooling += Math.max(
      0,
      Number(status.positionsClientsCooling || status.clientsCooling || 0)
    );
    positionsClientsDisabled += Math.max(
      0,
      Number(status.positionsClientsDisabled || status.clientsDisabled || 0)
    );
    positionsClientsInFlight += Math.max(
      0,
      Number(status.positionsClientsInFlight || status.clientsInFlight || 0)
    );
    positionsClients429 += Math.max(
      0,
      Number(status.positionsClients429 || status.clients429 || 0)
    );
    positionsHealthyClients += Math.max(
      0,
      Number(status.positionsHealthyClients || status.healthyClients || 0)
    );
    positionsAvgClientLatencyMs = Math.max(
      positionsAvgClientLatencyMs,
      Math.max(
        0,
        Number(status.positionsAvgClientLatencyMs || status.avgClientLatencyMs || 0)
      )
    );
    positionsTimeoutClients += Math.max(
      0,
      Number(status.positionsTimeoutClients || status.timeoutClients || 0)
    );
    positionsProxyFailingClients += Math.max(
      0,
      Number(status.positionsProxyFailingClients || status.proxyFailingClients || 0)
    );
    accountClientsTotal += Math.max(0, Number(status.accountClientsTotal || 0));
    accountProxiedClients += Math.max(0, Number(status.accountProxiedClients || 0));
    accountClientsCooling += Math.max(0, Number(status.accountClientsCooling || 0));
    accountClientsDisabled += Math.max(0, Number(status.accountClientsDisabled || 0));
    accountClientsInFlight += Math.max(0, Number(status.accountClientsInFlight || 0));
    accountClients429 += Math.max(0, Number(status.accountClients429 || 0));
    accountHealthyClients += Math.max(0, Number(status.accountHealthyClients || 0));
    accountAvgClientLatencyMs = Math.max(
      accountAvgClientLatencyMs,
      Math.max(0, Number(status.accountAvgClientLatencyMs || 0))
    );
    accountTimeoutClients += Math.max(0, Number(status.accountTimeoutClients || 0));
    accountProxyFailingClients += Math.max(
      0,
      Number(status.accountProxyFailingClients || 0)
    );
    accountIndicatedOpenWallets += Math.max(
      0,
      Number(status.accountIndicatedOpenWallets || 0)
    );
    accountIndicatedOpenPositionsTotal += Math.max(
      0,
      Number(status.accountIndicatedOpenPositionsTotal || 0)
    );
    positionMaterializationGapWallets += Math.max(
      0,
      Number(status.positionMaterializationGapWallets || 0)
    );
    positionMaterializationGapTotal += Math.max(
      0,
      Number(status.positionMaterializationGapTotal || 0)
    );
    accountRepairDueWallets += Math.max(
      0,
      Number(status.accountRepairDueWallets || 0)
    );
    liveStateHintedWallets += Math.max(
      0,
      Number(status.liveStateHintedWallets || 0)
    );
    liveStateMaterializedWallets += Math.max(
      0,
      Number(status.liveStateMaterializedWallets || 0)
    );
    liveStateUncertainWallets += Math.max(
      0,
      Number(status.liveStateUncertainWallets || 0)
    );
    liveStateClosedWallets += Math.max(
      0,
      Number(status.liveStateClosedWallets || 0)
    );
    liveStatePendingBootstrapWallets += Math.max(
      0,
      Number(status.liveStatePendingBootstrapWallets || 0)
    );
    closeConfirmAccountZeroThreshold = Math.max(
      closeConfirmAccountZeroThreshold,
      Math.max(0, Number(status.closeConfirmAccountZeroThreshold || 0))
    );
    closeConfirmPositionsEmptyThreshold = Math.max(
      closeConfirmPositionsEmptyThreshold,
      Math.max(0, Number(status.closeConfirmPositionsEmptyThreshold || 0))
    );
    if (status.accountLastErrorAt && String(status.accountLastError || "").trim()) {
      if (!accountLastError || Number(status.accountLastErrorAt || 0) >= accountLastErrorAt) {
        accountLastError = String(status.accountLastError || "").trim() || null;
        accountLastErrorAt = Number(status.accountLastErrorAt || 0);
      }
    }
    lifecycleHotWallets += Math.max(0, Number(status.lifecycleHotWallets || 0));
    lifecycleWarmWallets += Math.max(0, Number(status.lifecycleWarmWallets || 0));
    lifecycleColdWallets += Math.max(0, Number(status.lifecycleColdWallets || 0));
    staleWallets += Math.max(0, Number(status.staleWallets || 0));
    freshWallets += Math.max(0, Number(status.freshWallets || 0));
    coolingWallets += Math.max(0, Number(status.coolingWallets || 0));
    priorityQueueDepth += Math.max(0, Number(status.priorityQueueDepth || 0));
    hotReconcileDueWallets += Math.max(0, Number(status.hotReconcileDueWallets || 0));
    warmReconcileDueWallets += Math.max(0, Number(status.warmReconcileDueWallets || 0));
    scanIntervalMs = Math.max(
      scanIntervalMs,
      Math.max(0, Number(status.scanIntervalMs || 0))
    );
    staleMs = Math.max(staleMs, Math.max(0, Number(status.staleMs || 0)));
    coolingMs = Math.max(
      coolingMs,
      Math.max(0, Number(status.coolingMs || 0))
    );
    running = running || Boolean(status.running);
    lastSuccessAt = Math.max(lastSuccessAt, Number(status.lastSuccessAt || 0));
    lastErrorAt = Math.max(lastErrorAt, Number(status.lastErrorAt || 0));
    lastEventAt = Math.max(lastEventAt, Number(status.lastEventAt || 0));
    lastPassStartedAt = Math.max(
      lastPassStartedAt,
      Number(status.lastPassStartedAt || 0)
    );
    lastPassFinishedAt = Math.max(
      lastPassFinishedAt,
      Number(status.lastPassFinishedAt || 0)
    );
    lastPassDurationMs = Math.max(
      lastPassDurationMs,
      Math.max(0, Number(status.lastPassDurationMs || 0))
    );
    if (status.lastErrorAt && String(status.lastError || "").trim()) {
      if (!lastError || Number(status.lastErrorAt || 0) >= lastErrorAt) {
        lastError = String(status.lastError || "").trim() || null;
        lastErrorAt = Number(status.lastErrorAt || 0);
      }
    }
    directFallbackOnLastAttempt =
      directFallbackOnLastAttempt ||
      Boolean(status.directFallbackOnLastAttempt);
    shardCount = Math.max(
      shardCount,
      Math.max(0, Number(snapshot.shardCount || status.shardCount || 0))
    );

    snapshot.positions.forEach((row) => {
      const key = positionIdentity(row);
      if (!key) return;
      const existing = positionMap.get(key);
      if (
        !existing ||
        positionFreshnessTs(row) > positionFreshnessTs(existing)
      ) {
        positionMap.set(key, row);
      }
      const wallet = String(row.wallet || "").trim();
      if (wallet) walletOpenSet.add(wallet);
    });

    snapshot.liveStates.forEach((row) => {
      const wallet = String((row && row.wallet) || "").trim();
      if (!wallet) return;
      const existing = liveStateMap.get(wallet) || null;
      liveStateMap.set(wallet, mergeLiveStateRows(existing, row));
    });

    snapshot.events.forEach((row) => {
      eventRows.push(row);
    });
    const openedRows = Array.isArray(snapshot.positionOpenedEvents)
      ? snapshot.positionOpenedEvents
      : [];
    openedRows.forEach((row) => {
      const key = openedEventIdentity(row);
      if (!key) return;
      const existing = positionOpenedEventMap.get(key);
      if (!existing || openedEventFreshnessTs(row) >= openedEventFreshnessTs(existing)) {
        positionOpenedEventMap.set(key, row);
      }
    });

    snapshot.successScannedWallets.forEach((wallet) => {
      const text = String(wallet || "").trim();
      if (text) successWalletSet.add(text);
    });
  });

  const positions = Array.from(positionMap.values()).sort(
    (a, b) => positionFreshnessTs(b) - positionFreshnessTs(a)
  );
  const liveStates = Array.from(liveStateMap.values()).sort(
    (a, b) => liveStateFreshnessTs(b) - liveStateFreshnessTs(a)
  );
  const events = eventRows
    .sort(
      (a, b) => Number(b.timestamp || b.at || 0) - Number(a.timestamp || a.at || 0)
    )
    .slice(0, eventsLimit);
  const positionOpenedEvents = Array.from(positionOpenedEventMap.values())
    .sort((a, b) => openedEventFreshnessTs(b) - openedEventFreshnessTs(a))
    .slice(0, positionOpenedEventsLimit);

  const coverageUniverseWallets =
    walletsKnownGlobal > 0 ? walletsKnownGlobal : walletsKnown;
  const walletsScannedAtLeastOnce = successWalletSet.size;
  const clampedWalletsScannedAtLeastOnce =
    coverageUniverseWallets > 0
      ? Math.min(coverageUniverseWallets, walletsScannedAtLeastOnce)
      : walletsScannedAtLeastOnce;
  const coveragePct =
    coverageUniverseWallets > 0
      ? Number(
          ((clampedWalletsScannedAtLeastOnce / coverageUniverseWallets) * 100).toFixed(2)
        )
      : 0;
  const estimatedSweepSeconds =
    coverageUniverseWallets > 0 && passThroughputRps > 0
      ? Math.ceil(coverageUniverseWallets / passThroughputRps)
      : null;
  const effectiveLastError =
    lastError && lastErrorAt > 0 && lastErrorAt >= lastSuccessAt ? lastError : null;
  const effectiveLastErrorAt =
    effectiveLastError && lastErrorAt > 0 ? lastErrorAt : null;
  const effectiveAccountLastError =
    accountLastError && accountLastErrorAt > 0 && accountLastErrorAt >= lastSuccessAt
      ? accountLastError
      : null;
  const effectiveAccountLastErrorAt =
    effectiveAccountLastError && accountLastErrorAt > 0 ? accountLastErrorAt : null;

  return {
    generatedAt: Math.max(
      ...valid.map((snapshot) => Number(snapshot.generatedAt || 0)),
      Date.now()
    ),
    status: {
      enabled,
      mode: "wallet_first_tracked_wallet_positions_sharded",
      running,
      startedAt: Math.min(
        ...valid
          .map((snapshot) => Number((snapshot.status || {}).startedAt || 0))
          .filter((value) => value > 0),
        Date.now()
      ),
      lastPassStartedAt: lastPassStartedAt || null,
      lastPassFinishedAt: lastPassFinishedAt || null,
      lastPassDurationMs: lastPassDurationMs || null,
      passThroughputRps: Number(passThroughputRps.toFixed(2)),
      estimatedSweepSeconds,
      lastSuccessAt: lastSuccessAt || null,
      lastErrorAt: effectiveLastErrorAt,
      lastError: effectiveLastError,
      scannedWalletsTotal,
      failedWalletsTotal,
      passes,
      walletsKnownGlobal: walletsKnownGlobal || walletsKnown,
      walletsKnown,
      walletsScannedAtLeastOnce: clampedWalletsScannedAtLeastOnce,
      walletsCoveragePct: coveragePct,
      walletsWithOpenPositions: walletOpenSet.size,
      openPositionsTotal: positions.length,
      lastEventAt: lastEventAt || null,
      warmupDone:
        coverageUniverseWallets > 0
          ? walletsScannedAtLeastOnce >= coverageUniverseWallets
          : false,
      clientsTotal,
      proxiedClients,
      clientsCooling,
      clientsDisabled,
      clientsInFlight,
      clients429,
      directClientIncluded,
      shardIndex: null,
      shardCount: shardCount || valid.length,
      maxConcurrency,
      hotWalletsPerPass,
      warmWalletsPerPass,
      recentActiveWalletsPerPass,
      batchTargetWallets,
      avgWalletScanMs,
      targetPassDurationMs,
      recentActiveWallets,
      warmWallets,
      publicActiveWallets,
      hotWsActiveWallets,
      hotWsOpenConnections,
      hotWsDroppedPromotions,
      hotWsCapacity,
      hotWsAvailableSlots,
      hotWsCapacityCeiling,
      hotWsPromotionBacklog,
      hotWsTriggerToEventAvgMs: hotWsTriggerToEventAvgMs || null,
      hotWsLastTriggerToEventMs: hotWsLastTriggerToEventMs || null,
      hotWsReconnectTransitions,
      hotWsErrorCount,
      hotWsScaleEvents,
      hotWsProcessRssMb: hotWsProcessRssMb || null,
      estimatedHotLoopSeconds: estimatedHotLoopSeconds || null,
      estimatedWarmLoopSeconds: estimatedWarmLoopSeconds || null,
      requestTimeoutMs,
      accountRequestTimeoutMs,
      positionsRequestTimeoutMs,
      maxFetchAttempts,
      maxInFlightPerClient,
      maxInFlightDirect,
      healthyClients,
      avgClientLatencyMs: avgClientLatencyMs || null,
      timeoutClients,
      proxyFailingClients,
      positionsClientsTotal,
      positionsProxiedClients,
      positionsClientsCooling,
      positionsClientsDisabled,
      positionsClientsInFlight,
      positionsClients429,
      positionsHealthyClients,
      positionsAvgClientLatencyMs: positionsAvgClientLatencyMs || null,
      positionsTimeoutClients,
      positionsProxyFailingClients,
      accountClientsTotal,
      accountProxiedClients,
      accountClientsCooling,
      accountClientsDisabled,
      accountClientsInFlight,
      accountClients429,
      accountHealthyClients,
      accountAvgClientLatencyMs: accountAvgClientLatencyMs || null,
      accountTimeoutClients,
      accountProxyFailingClients,
      accountLastError: effectiveAccountLastError,
      accountLastErrorAt: effectiveAccountLastErrorAt,
      accountIndicatedOpenWallets,
      accountIndicatedOpenPositionsTotal,
      positionMaterializationGapWallets,
      positionMaterializationGapTotal,
      accountRepairDueWallets,
      liveStateHintedWallets,
      liveStateMaterializedWallets,
      liveStateUncertainWallets,
      liveStateClosedWallets,
      liveStatePendingBootstrapWallets,
      closeConfirmAccountZeroThreshold:
        closeConfirmAccountZeroThreshold || null,
      closeConfirmPositionsEmptyThreshold:
        closeConfirmPositionsEmptyThreshold || null,
      lifecycleHotWallets,
      lifecycleWarmWallets,
      lifecycleColdWallets,
      staleWallets,
      freshWallets,
      coolingWallets,
      priorityQueueDepth,
      hotReconcileDueWallets,
      warmReconcileDueWallets,
      directFallbackOnLastAttempt,
      scanIntervalMs,
      staleMs,
      coolingMs,
    },
    positions,
    liveStates,
    events,
    positionOpenedEvents,
    successScannedWallets: Array.from(successWalletSet),
  };
}

function loadMergedShardSnapshot(dirPath, options = {}) {
  const eventsLimit = Math.max(
    1,
    Math.min(MAX_CACHED_EVENTS, Number(options.eventsLimit || 300))
  );
  const cacheKey = String(dirPath || "").trim() || "__default__";
  const cached = mergedShardSnapshotCache.get(cacheKey);
  if (
    cached &&
    cached.value &&
    Math.max(0, Date.now() - Number(cached.loadedAt || 0)) <= MIN_MERGED_CACHE_RELOAD_MS
  ) {
    return trimMergedSnapshotEvents(cached.value, eventsLimit);
  }
  const files = listShardSnapshotFiles(dirPath);
  const signature = buildSnapshotCollectionSignature(files);
  if (cached && cached.signature === signature && cached.value) {
    cached.loadedAt = Date.now();
    return trimMergedSnapshotEvents(cached.value, eventsLimit);
  }
  const snapshots = files.map((filePath) => loadShardSnapshot(filePath)).filter(Boolean);
  const merged = mergeShardSnapshots(snapshots, {
    eventsLimit: MAX_CACHED_EVENTS,
    positionOpenedEventsLimit: MAX_CACHED_POSITION_OPENED_EVENTS,
  });
  mergedShardSnapshotCache.set(cacheKey, {
    signature,
    value: merged,
    loadedAt: Date.now(),
  });
  return trimMergedSnapshotEvents(merged, eventsLimit);
}

module.exports = {
  buildShardSnapshotPath,
  listShardSnapshotFiles,
  loadMergedShardSnapshot,
  loadShardSnapshot,
  mergeShardSnapshots,
  writeShardSnapshot,
};
