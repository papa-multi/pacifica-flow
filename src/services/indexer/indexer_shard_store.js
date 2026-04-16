const fs = require("fs");
const path = require("path");

const {
  ensureDir,
  normalizeAddress,
  readJson,
  writeJsonAtomic,
} = require("../pipeline/utils");

function listShardDirectories(baseDir, options = {}) {
  if (!baseDir || !fs.existsSync(baseDir)) return [];
  const shardCount = Math.max(0, Number(options.shardCount || 0) || 0);
  if (shardCount > 0) {
    return Array.from({ length: shardCount }, (_value, index) => path.join(baseDir, `shard_${index}`)).filter(
      (dirPath) => fs.existsSync(dirPath)
    );
  }
  try {
    return fs
      .readdirSync(baseDir, { withFileTypes: true })
      .filter((entry) => entry && entry.isDirectory())
      .map((entry) => path.join(baseDir, entry.name))
      .sort();
  } catch (_error) {
    return [];
  }
}

function listShardDataFiles(baseDir, fileName, options = {}) {
  const safeName = String(fileName || "").trim();
  if (!safeName) return [];
  return listShardDirectories(baseDir, options)
    .map((dirPath) => path.join(dirPath, safeName))
    .filter((filePath) => fs.existsSync(filePath));
}

function getFileMtimeMs(filePath) {
  if (!filePath) return 0;
  try {
    return Number(fs.statSync(filePath).mtimeMs || 0) || 0;
  } catch (_error) {
    return 0;
  }
}

function getCollectionMtimeMs(filePaths = []) {
  return (Array.isArray(filePaths) ? filePaths : []).reduce(
    (max, filePath) => Math.max(max, getFileMtimeMs(filePath)),
    0
  );
}

function mergeDefinedObjects(base = null, override = null) {
  const left = base && typeof base === "object" && !Array.isArray(base) ? base : {};
  const right =
    override && typeof override === "object" && !Array.isArray(override) ? override : {};
  const merged = {};
  const keys = new Set([...Object.keys(left), ...Object.keys(right)]);
  keys.forEach((key) => {
    const leftValue = left[key];
    const rightValue = right[key];
    if (
      leftValue &&
      typeof leftValue === "object" &&
      !Array.isArray(leftValue) &&
      rightValue &&
      typeof rightValue === "object" &&
      !Array.isArray(rightValue)
    ) {
      merged[key] = mergeDefinedObjects(leftValue, rightValue);
      return;
    }
    merged[key] = rightValue !== null && rightValue !== undefined ? rightValue : leftValue;
  });
  return merged;
}

function walletStateFreshness(row = {}) {
  return Math.max(
    Number(row.updatedAt || 0) || 0,
    Number(row.lastSuccessAt || 0) || 0,
    Number(row.lastAttemptAt || 0) || 0,
    Number(row.lastScannedAt || 0) || 0,
    Number(row.lastFailureAt || 0) || 0,
    Number(row.retryQueuedAt || 0) || 0,
    Number(row.backfillCompletedAt || 0) || 0,
    Number(row.liveTrackingSince || 0) || 0,
    Number(row.discoveredAt || 0) || 0
  );
}

function normalizePaginationSummary(summary = null) {
  if (!summary || typeof summary !== "object") return null;
  return {
    exhausted: Boolean(summary.exhausted),
    frontierCursor:
      summary.frontierCursor !== undefined && summary.frontierCursor !== null
        ? summary.frontierCursor
        : null,
    highestFetchedPage: Number(summary.highestFetchedPage || 0),
    highestKnownPage: Number(summary.highestKnownPage || 0),
    totalKnownPages:
      summary.totalKnownPages !== undefined && summary.totalKnownPages !== null
        ? Number(summary.totalKnownPages || 0)
        : null,
    fetchedPages: Number(summary.fetchedPages || 0),
    verifiedPages: Number(
      summary.verifiedPages !== undefined ? summary.verifiedPages : summary.fetchedPages || 0
    ),
    retryPages: Number(summary.retryPages || 0),
    pendingPages: Number(summary.pendingPages || 0),
    lastSuccessfulAt: Number(summary.lastSuccessfulAt || 0) || null,
    missingPageRanges: Array.isArray(summary.missingPageRanges) ? summary.missingPageRanges : [],
  };
}

function isPaginationComplete(summary = null) {
  const safe = normalizePaginationSummary(summary);
  if (!safe) return false;
  return Boolean(safe.exhausted) && Number(safe.retryPages || 0) <= 0 && !safe.frontierCursor;
}

function paginationScore(summary = null) {
  const safe = normalizePaginationSummary(summary);
  if (!safe) return -1;
  return (
    (isPaginationComplete(safe) ? 1000000 : 0) +
    (Boolean(safe.exhausted) ? 100000 : 0) +
    Number(safe.verifiedPages || 0) * 1000 +
    Number(safe.fetchedPages || 0) * 100 +
    Number(safe.highestFetchedPage || 0) * 10 -
    Number(safe.retryPages || 0) * 500 -
    Number(safe.pendingPages || 0) * 5 +
    Number(safe.lastSuccessfulAt || 0) / 1000000000000
  );
}

function walletStateTruthScore(row = {}) {
  const historyAudit = row && row.historyAudit && typeof row.historyAudit === "object" ? row.historyAudit : {};
  const fileAudit =
    historyAudit.file && typeof historyAudit.file === "object" ? historyAudit.file : {};
  const tradePagination = normalizePaginationSummary(
    row.tradePagination || (historyAudit.pagination && historyAudit.pagination.trades) || fileAudit.pagination?.trades
  );
  const fundingPagination = normalizePaginationSummary(
    row.fundingPagination || (historyAudit.pagination && historyAudit.pagination.funding) || fileAudit.pagination?.funding
  );
  const tradeComplete = isPaginationComplete(tradePagination);
  const fundingComplete = isPaginationComplete(fundingPagination);
  const remoteHistoryVerified =
    Boolean(row.remoteHistoryVerified) ||
    Number(row.remoteHistoryVerifiedAt || 0) > 0 ||
    (tradeComplete && fundingComplete);
  return (
    (tradeComplete ? 1000000000 : 0) +
    (fundingComplete ? 1000000000 : 0) +
    (remoteHistoryVerified ? 500000000 : 0) +
    (Boolean(fileAudit && Object.keys(fileAudit).length) ? 100000000 : 0) +
    Number(row.tradeRowsLoaded || fileAudit.tradesStored || 0) * 1000 +
    Number(row.fundingRowsLoaded || fileAudit.fundingStored || 0) * 100 -
    (Boolean(row.retryPending) ? 10000000 : 0) +
    paginationScore(tradePagination) +
    paginationScore(fundingPagination)
  );
}

function mergeWalletStateRows(existing = null, incoming = null) {
  const left = existing && typeof existing === "object" ? existing : null;
  const right = incoming && typeof incoming === "object" ? incoming : null;
  if (!left) return right ? { ...right } : null;
  if (!right) return { ...left };
  const leftTruth = walletStateTruthScore(left);
  const rightTruth = walletStateTruthScore(right);
  if (rightTruth > leftTruth) {
    return mergeDefinedObjects(left, right);
  }
  if (leftTruth > rightTruth) {
    return mergeDefinedObjects(right, left);
  }
  const leftFreshness = walletStateFreshness(left);
  const rightFreshness = walletStateFreshness(right);
  if (rightFreshness > leftFreshness) {
    return mergeDefinedObjects(left, right);
  }
  if (leftFreshness > rightFreshness) {
    return mergeDefinedObjects(right, left);
  }
  return mergeDefinedObjects(left, right);
}

function emptyMergedIndexerState() {
  return {
    version: 5,
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
    shardSources: [],
    shardCount: 0,
    mergedAt: Date.now(),
  };
}

function loadMergedIndexerState(baseDir, options = {}) {
  const filePaths = listShardDataFiles(baseDir, "indexer_state.json", options);
  const writeMergedPath = String(options.writeMergedPath || "").trim();
  if (!filePaths.length) {
    const fallback = writeMergedPath ? readJson(writeMergedPath, null) : null;
    return {
      state: fallback && typeof fallback === "object" ? fallback : emptyMergedIndexerState(),
      filePaths: [],
      mtimeMs: writeMergedPath ? getFileMtimeMs(writeMergedPath) : 0,
    };
  }
  const merged = emptyMergedIndexerState();
  const knownSet = new Set();
  const liveSet = new Set();
  const prioritySet = new Set();
  const continuationSet = new Set();

  filePaths.forEach((filePath) => {
    const payload = readJson(filePath, null);
    if (!payload || typeof payload !== "object") return;
    const shardId = path.basename(path.dirname(filePath));
    merged.shardSources.push({
      shardId,
      filePath,
      updatedAt: getFileMtimeMs(filePath),
    });
    merged.version = Math.max(merged.version, Number(payload.version || 0) || 0);
    merged.lastDiscoveryAt = Math.max(
      Number(merged.lastDiscoveryAt || 0) || 0,
      Number(payload.lastDiscoveryAt || 0) || 0
    ) || null;
    merged.lastScanAt = Math.max(
      Number(merged.lastScanAt || 0) || 0,
      Number(payload.lastScanAt || 0) || 0
    ) || null;
    merged.discoveryCycles += Math.max(0, Number(payload.discoveryCycles || 0));
    merged.scanCycles += Math.max(0, Number(payload.scanCycles || 0));

    (Array.isArray(payload.knownWallets) ? payload.knownWallets : []).forEach((wallet) => {
      const normalized = normalizeAddress(wallet);
      if (normalized) knownSet.add(normalized);
    });
    (Array.isArray(payload.liveWallets) ? payload.liveWallets : []).forEach((wallet) => {
      const normalized = normalizeAddress(wallet);
      if (normalized) liveSet.add(normalized);
    });
    (Array.isArray(payload.priorityQueue) ? payload.priorityQueue : []).forEach((wallet) => {
      const normalized = normalizeAddress(wallet);
      if (normalized) prioritySet.add(normalized);
    });
    (Array.isArray(payload.continuationQueue) ? payload.continuationQueue : []).forEach((wallet) => {
      const normalized = normalizeAddress(wallet);
      if (normalized) continuationSet.add(normalized);
    });
    Object.entries(payload.walletStates || {}).forEach(([key, row]) => {
      if (!row || typeof row !== "object") return;
      const wallet = normalizeAddress(row.wallet || key);
      if (!wallet) return;
      const withStorage = {
        ...row,
        wallet,
        storage: mergeDefinedObjects(row.storage || null, {
          indexerStatePath: filePath,
          walletHistoryDir: path.join(path.dirname(filePath), "wallet_history"),
          indexerShardId: shardId,
        }),
      };
      merged.walletStates[wallet] = mergeWalletStateRows(
        merged.walletStates[wallet] || null,
        withStorage
      );
    });
  });

  merged.knownWallets = [...knownSet];
  merged.liveWallets = [...liveSet];
  merged.priorityQueue = [...prioritySet];
  merged.continuationQueue = [...continuationSet];
  merged.scanCursor = 0;
  merged.liveScanCursor = 0;
  merged.shardCount = merged.shardSources.length;
  merged.mergedAt = Date.now();
  merged.mtimeMs = getCollectionMtimeMs(filePaths);

  if (writeMergedPath) {
    ensureDir(path.dirname(writeMergedPath));
    writeJsonAtomic(writeMergedPath, merged);
  }

  return {
    state: merged,
    filePaths,
    mtimeMs: merged.mtimeMs,
  };
}

module.exports = {
  listShardDirectories,
  listShardDataFiles,
  getCollectionMtimeMs,
  getFileMtimeMs,
  loadMergedIndexerState,
  mergeWalletStateRows,
};
