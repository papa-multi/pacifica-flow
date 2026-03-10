const fs = require("fs");
const path = require("path");
const { ensureDir, readJson, writeJsonAtomic } = require("../pipeline/utils");

const SHARD_FILE_PREFIX = "wallet_first_shard_";
const SHARD_FILE_SUFFIX = ".json";

function buildShardSnapshotPath(dirPath, shardIndex = 0) {
  const safeIndex = Math.max(0, Math.floor(Number(shardIndex || 0)));
  return path.join(dirPath, `${SHARD_FILE_PREFIX}${safeIndex}${SHARD_FILE_SUFFIX}`);
}

function listShardSnapshotFiles(dirPath) {
  if (!dirPath || !fs.existsSync(dirPath)) return [];
  try {
    return fs
      .readdirSync(dirPath)
      .filter(
        (name) =>
          name.startsWith(SHARD_FILE_PREFIX) &&
          name.endsWith(SHARD_FILE_SUFFIX)
      )
      .map((name) => path.join(dirPath, name))
      .sort();
  } catch (_error) {
    return [];
  }
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
    events: Array.isArray(raw.events) ? raw.events : [],
    successScannedWallets: Array.isArray(raw.successScannedWallets)
      ? raw.successScannedWallets
      : [],
  };
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
  const valid = (Array.isArray(snapshots) ? snapshots : []).filter(Boolean);
  if (!valid.length) {
    return {
      generatedAt: Date.now(),
      status: { enabled: false, mode: "wallet_first_external_shards" },
      positions: [],
      events: [],
      successScannedWallets: [],
    };
  }

  const positionMap = new Map();
  const walletOpenSet = new Set();
  const eventRows = [];
  const successWalletSet = new Set();

  let walletsKnown = 0;
  let walletsKnownGlobal = 0;
  let scannedWalletsTotal = 0;
  let failedWalletsTotal = 0;
  let passes = 0;
  let passThroughputRps = 0;
  let clientsTotal = 0;
  let clientsCooling = 0;
  let clientsDisabled = 0;
  let clientsInFlight = 0;
  let clients429 = 0;
  let hotWalletsPerPass = 0;
  let warmWalletsPerPass = 0;
  let recentActiveWalletsPerPass = 0;
  let maxConcurrency = 0;
  let batchTargetWallets = 0;
  let avgWalletScanMs = 0;
  let targetPassDurationMs = 0;
  let recentActiveWallets = 0;
  let warmWallets = 0;
  let estimatedHotLoopSeconds = 0;
  let estimatedWarmLoopSeconds = 0;
  let requestTimeoutMs = 0;
  let maxFetchAttempts = 0;
  let maxInFlightPerClient = 0;
  let maxInFlightDirect = 0;
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
    clientsCooling += Math.max(0, Number(status.clientsCooling || 0));
    clientsDisabled += Math.max(0, Number(status.clientsDisabled || 0));
    clientsInFlight += Math.max(0, Number(status.clientsInFlight || 0));
    clients429 += Math.max(0, Number(status.clients429 || 0));
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
    estimatedHotLoopSeconds = Math.max(
      estimatedHotLoopSeconds,
      Math.max(0, Number(status.estimatedHotLoopSeconds || 0))
    );
    estimatedWarmLoopSeconds = Math.max(
      estimatedWarmLoopSeconds,
      Math.max(0, Number(status.estimatedWarmLoopSeconds || 0))
    );
    requestTimeoutMs = Math.max(
      requestTimeoutMs,
      Math.max(0, Number(status.requestTimeoutMs || 0))
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
        Number(row.updatedAt || row.timestamp || 0) >
          Number(existing.updatedAt || existing.timestamp || 0)
      ) {
        positionMap.set(key, row);
      }
      const wallet = String(row.wallet || "").trim();
      if (wallet) walletOpenSet.add(wallet);
    });

    snapshot.events.forEach((row) => {
      eventRows.push(row);
    });

    snapshot.successScannedWallets.forEach((wallet) => {
      const text = String(wallet || "").trim();
      if (text) successWalletSet.add(text);
    });
  });

  const positions = Array.from(positionMap.values()).sort(
    (a, b) => Number(b.updatedAt || b.timestamp || 0) - Number(a.updatedAt || a.timestamp || 0)
  );
  const events = eventRows
    .sort(
      (a, b) => Number(b.timestamp || b.at || 0) - Number(a.timestamp || a.at || 0)
    )
    .slice(0, eventsLimit);

  const walletsScannedAtLeastOnce = successWalletSet.size;
  const coveragePct =
    walletsKnown > 0
      ? Number(((walletsScannedAtLeastOnce / walletsKnown) * 100).toFixed(2))
      : 0;
  const estimatedSweepSeconds =
    walletsKnown > 0 && passThroughputRps > 0
      ? Math.ceil(walletsKnown / passThroughputRps)
      : null;

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
      lastErrorAt: lastErrorAt || null,
      lastError,
      scannedWalletsTotal,
      failedWalletsTotal,
      passes,
      walletsKnownGlobal: walletsKnownGlobal || walletsKnown,
      walletsKnown,
      walletsScannedAtLeastOnce,
      walletsCoveragePct: coveragePct,
      walletsWithOpenPositions: walletOpenSet.size,
      openPositionsTotal: positions.length,
      lastEventAt: lastEventAt || null,
      warmupDone: walletsKnown > 0 ? walletsScannedAtLeastOnce >= walletsKnown : false,
      clientsTotal,
      clientsCooling,
      clientsDisabled,
      clientsInFlight,
      clients429,
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
      estimatedHotLoopSeconds: estimatedHotLoopSeconds || null,
      estimatedWarmLoopSeconds: estimatedWarmLoopSeconds || null,
      requestTimeoutMs,
      maxFetchAttempts,
      maxInFlightPerClient,
      maxInFlightDirect,
      directFallbackOnLastAttempt,
      scanIntervalMs,
      staleMs,
      coolingMs,
    },
    positions,
    events,
    successScannedWallets: Array.from(successWalletSet),
  };
}

function loadMergedShardSnapshot(dirPath, options = {}) {
  const files = listShardSnapshotFiles(dirPath);
  const snapshots = files.map((filePath) => loadShardSnapshot(filePath)).filter(Boolean);
  return mergeShardSnapshots(snapshots, options);
}

module.exports = {
  buildShardSnapshotPath,
  listShardSnapshotFiles,
  loadMergedShardSnapshot,
  loadShardSnapshot,
  mergeShardSnapshots,
  writeShardSnapshot,
};
