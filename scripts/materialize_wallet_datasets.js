#!/usr/bin/env node
const fs = require("fs");
const path = require("path");
const { execFileSync } = require("child_process");

const {
  ensureDir,
  normalizeAddress,
  readJson,
  writeJsonAtomic,
} = require("../src/services/pipeline/utils");
const { buildWalletRecordFromHistory } = require("../src/services/analytics/wallet_stats");
const {
  buildWalletHistoryAudit,
  deriveWalletHistoryTruth,
} = require("../src/services/indexer/wallet_truth");
const { shardIndexForKey } = require("../src/services/indexer/sharding");

const ROOT = "/root/pacifica-flow";
const INDEXER_DIR = path.join(ROOT, "data", "indexer");
const SHARDS_DIR = path.join(INDEXER_DIR, "shards");
const ACTIVE_SHARDS = Math.max(
  1,
  Number(process.env.PACIFICA_INDEXER_MULTI_WORKER_SHARDS || 8) || 8
);

function listShardDirs(baseDir, count) {
  return Array.from({ length: count }, (_v, index) => ({
    index,
    dir: path.join(baseDir, `shard_${index}`),
  })).filter((entry) => fs.existsSync(entry.dir));
}

function rowsFromWalletStore(payload) {
  if (!payload || typeof payload !== "object") return {};
  if (payload.wallets && typeof payload.wallets === "object" && !Array.isArray(payload.wallets)) {
    return payload.wallets;
  }
  if (Array.isArray(payload.wallets)) {
    return payload.wallets.reduce((acc, row) => {
      const wallet = normalizeAddress(row && row.wallet);
      if (wallet) acc[wallet] = row;
      return acc;
    }, {});
  }
  return {};
}

function ownerShardIndex(wallet, shardCount) {
  return shardIndexForKey(`wallet:${String(wallet || "").trim()}`, shardCount);
}

function listHistoryWallets(historyDir) {
  if (!fs.existsSync(historyDir)) return [];
  return fs
    .readdirSync(historyDir)
    .filter((fileName) => fileName.endsWith(".json"))
    .map((fileName) => normalizeAddress(fileName.slice(0, -5)))
    .filter(Boolean);
}

function pickTimestamp(...values) {
  return values.reduce((max, value) => {
    const num = Number(value || 0) || 0;
    return num > max ? num : max;
  }, 0);
}

function buildCanonicalRow(wallet, row = {}, state = {}, history = null, shardEntry) {
  const fileAudit =
    history && history.audit && typeof history.audit === "object" ? history.audit : null;
  const truth = deriveWalletHistoryTruth(state, { fileAudit });
  const metricsRow =
    history && Array.isArray(history.trades) && Array.isArray(history.funding)
      ? buildWalletRecordFromHistory({
          wallet,
          tradesHistory: history.trades,
          fundingHistory: history.funding,
          computedAt:
            Number((history.audit && history.audit.persistedAt) || 0) ||
            Number(history.updatedAt || 0) ||
            Date.now(),
        })
      : null;
  const baseRow = metricsRow ? { ...row, ...metricsRow } : { ...row, wallet };
  const tradeRowsLoaded = Math.max(
    Number(state.tradeRowsLoaded || 0),
    Number(fileAudit && fileAudit.tradesStored !== undefined ? fileAudit.tradesStored : 0),
    Array.isArray(history && history.trades) ? history.trades.length : 0
  );
  const fundingRowsLoaded = Math.max(
    Number(state.fundingRowsLoaded || 0),
    Number(fileAudit && fileAudit.fundingStored !== undefined ? fileAudit.fundingStored : 0),
    Array.isArray(history && history.funding) ? history.funding.length : 0
  );
  const historyAudit = buildWalletHistoryAudit(
    {
      ...state,
      tradeRowsLoaded,
      fundingRowsLoaded,
      tradePagination: truth.tradePagination,
      fundingPagination: truth.fundingPagination,
      tradeDone: truth.tradeDone,
      fundingDone: truth.fundingDone,
      tradeHasMore: truth.tradeHasMore,
      fundingHasMore: truth.fundingHasMore,
      tradeCursor: truth.tradeCursor,
      fundingCursor: truth.fundingCursor,
      retryPending: truth.retryPending,
      retryReason: truth.retryReason,
      remoteHistoryVerified: truth.remoteHistoryVerified,
    },
    { fileAudit }
  );
  const storage = {
    ...(row.storage && typeof row.storage === "object" ? row.storage : {}),
    indexerShardId: `indexer_shard_${shardEntry.index}`,
    indexerStatePath: path.join(shardEntry.dir, "indexer_state.json"),
    walletStorePath: path.join(shardEntry.dir, "wallets.json"),
    walletHistoryPath: path.join(shardEntry.dir, "wallet_history", `${wallet}.json`),
  };
  return {
    ...baseRow,
    wallet,
    createdAt:
      Number(row.createdAt || 0) ||
      Number(baseRow.createdAt || 0) ||
      Number(baseRow.updatedAt || 0) ||
      Date.now(),
    updatedAt:
      Number(baseRow.updatedAt || 0) ||
      Number((historyAudit.file && historyAudit.file.persistedAt) || 0) ||
      Date.now(),
    tradeRowsLoaded,
    fundingRowsLoaded,
    tradePagesLoaded: Math.max(
      Number(row.tradePagesLoaded || 0),
      Number((truth.tradePagination && truth.tradePagination.highestFetchedPage) || 0)
    ),
    fundingPagesLoaded: Math.max(
      Number(row.fundingPagesLoaded || 0),
      Number((truth.fundingPagination && truth.fundingPagination.highestFetchedPage) || 0)
    ),
    tradePagination: truth.tradePagination,
    fundingPagination: truth.fundingPagination,
    tradeDone: truth.tradeDone,
    fundingDone: truth.fundingDone,
    tradeHasMore: truth.tradeHasMore,
    fundingHasMore: truth.fundingHasMore,
    tradeCursor: truth.tradeCursor,
    fundingCursor: truth.fundingCursor,
    remoteHistoryVerified: truth.remoteHistoryVerified,
    remoteHistoryVerifiedAt: truth.remoteHistoryVerified
      ? pickTimestamp(
          row.remoteHistoryVerifiedAt,
          state.remoteHistoryVerifiedAt,
          row.backfillCompletedAt,
          state.backfillCompletedAt,
          historyAudit.file && historyAudit.file.persistedAt,
          baseRow.updatedAt
        ) || Date.now()
      : null,
    backfillCompletedAt: truth.backfillComplete
      ? pickTimestamp(
          row.backfillCompletedAt,
          state.backfillCompletedAt,
          row.remoteHistoryVerifiedAt,
          state.remoteHistoryVerifiedAt,
          historyAudit.file && historyAudit.file.persistedAt,
          baseRow.updatedAt
        ) || Date.now()
      : null,
    retryPending: truth.retryPending,
    retryReason: truth.retryReason,
    retryQueuedAt: truth.retryPending ? Number(state.retryQueuedAt || 0) || null : null,
    forceHeadRefetch: truth.backfillComplete ? false : Boolean(state.forceHeadRefetch || row.forceHeadRefetch),
    forceHeadRefetchReason: truth.backfillComplete
      ? null
      : state.forceHeadRefetchReason || row.forceHeadRefetchReason || null,
    forceHeadRefetchQueuedAt: truth.backfillComplete
      ? null
      : Number(state.forceHeadRefetchQueuedAt || row.forceHeadRefetchQueuedAt || 0) || null,
    openPositions: Number(row.openPositions || 0) || 0,
    exposureUsd: Number(row.exposureUsd || 0) || 0,
    unrealizedPnlUsd: Number(row.unrealizedPnlUsd || 0) || 0,
    storage,
    historyAudit,
  };
}

function buildCanonicalState(wallet, row = {}, state = {}, shardEntry) {
  const fileAudit =
    row.historyAudit && row.historyAudit.file && typeof row.historyAudit.file === "object"
      ? row.historyAudit.file
      : null;
  const truth = deriveWalletHistoryTruth(
    {
      ...state,
      tradeRowsLoaded: row.tradeRowsLoaded,
      fundingRowsLoaded: row.fundingRowsLoaded,
      tradePagination: row.tradePagination,
      fundingPagination: row.fundingPagination,
      tradeDone: row.tradeDone,
      fundingDone: row.fundingDone,
      tradeHasMore: row.tradeHasMore,
      fundingHasMore: row.fundingHasMore,
      tradeCursor: row.tradeCursor,
      fundingCursor: row.fundingCursor,
      retryPending: row.retryPending,
      retryReason: row.retryReason,
    },
    { fileAudit }
  );
  const liveTrackingSince = Number(state.liveTrackingSince || 0) || null;
  return {
    ...state,
    wallet,
    tradePagination: truth.tradePagination,
    fundingPagination: truth.fundingPagination,
    tradeRowsLoaded: Number(row.tradeRowsLoaded || 0),
    fundingRowsLoaded: Number(row.fundingRowsLoaded || 0),
    tradePagesLoaded: Number(row.tradePagesLoaded || 0),
    fundingPagesLoaded: Number(row.fundingPagesLoaded || 0),
    tradeDone: truth.tradeDone,
    fundingDone: truth.fundingDone,
    tradeHasMore: truth.tradeHasMore,
    fundingHasMore: truth.fundingHasMore,
    tradeCursor: truth.tradeCursor,
    fundingCursor: truth.fundingCursor,
    remoteHistoryVerified: truth.remoteHistoryVerified,
    remoteHistoryVerifiedAt: row.remoteHistoryVerifiedAt || null,
    backfillCompletedAt: row.backfillCompletedAt || null,
    retryPending: truth.retryPending,
    retryReason: truth.retryReason,
    retryQueuedAt: truth.retryPending ? Number(state.retryQueuedAt || 0) || null : null,
    forceHeadRefetch: row.forceHeadRefetch,
    forceHeadRefetchReason: row.forceHeadRefetchReason || null,
    forceHeadRefetchQueuedAt: row.forceHeadRefetchQueuedAt || null,
    historyPhase: truth.backfillComplete
      ? "complete"
      : state.historyPhase || (row.tradeRowsLoaded || row.fundingRowsLoaded ? "deep" : "activate"),
    lifecycleStage: truth.backfillComplete
      ? liveTrackingSince
        ? "live_tracking"
        : "fully_indexed"
      : state.lifecycleStage || (row.tradeRowsLoaded || row.fundingRowsLoaded ? "backfilling" : "discovered"),
    historyAudit: row.historyAudit,
    storage: {
      ...(state.storage && typeof state.storage === "object" ? state.storage : {}),
      indexerShardId: `indexer_shard_${shardEntry.index}`,
      indexerStatePath: path.join(shardEntry.dir, "indexer_state.json"),
      walletHistoryDir: path.join(shardEntry.dir, "wallet_history"),
    },
  };
}

function materializeShard(shardEntry) {
  const statePath = path.join(shardEntry.dir, "indexer_state.json");
  const storePath = path.join(shardEntry.dir, "wallets.json");
  const historyDir = path.join(shardEntry.dir, "wallet_history");
  ensureDir(historyDir);

  const statePayload = readJson(statePath, {}) || {};
  const storePayload = readJson(storePath, {}) || {};
  const sourceStates =
    statePayload.walletStates && typeof statePayload.walletStates === "object"
      ? statePayload.walletStates
      : {};
  const sourceRows = rowsFromWalletStore(storePayload);
  const walletSet = new Set();
  Object.keys(sourceStates).forEach((wallet) => walletSet.add(normalizeAddress(wallet)));
  Object.keys(sourceRows).forEach((wallet) => walletSet.add(normalizeAddress(wallet)));
  (Array.isArray(statePayload.knownWallets) ? statePayload.knownWallets : []).forEach((wallet) =>
    walletSet.add(normalizeAddress(wallet))
  );
  listHistoryWallets(historyDir).forEach((wallet) => walletSet.add(wallet));

  const nextRows = {};
  const nextStates = {};
  const knownWallets = [];
  const liveWallets = [];
  const priorityQueue = [];
  const continuationQueue = [];

  Array.from(walletSet)
    .filter(Boolean)
    .sort()
    .forEach((wallet) => {
      if (ownerShardIndex(wallet, ACTIVE_SHARDS) !== shardEntry.index) return;
      const row = sourceRows[wallet] && typeof sourceRows[wallet] === "object" ? sourceRows[wallet] : {};
      const state =
        sourceStates[wallet] && typeof sourceStates[wallet] === "object" ? sourceStates[wallet] : {};
      const historyPath = path.join(historyDir, `${wallet}.json`);
      const history = readJson(historyPath, null);
      const canonicalRow = buildCanonicalRow(wallet, row, state, history, shardEntry);
      const canonicalState = buildCanonicalState(wallet, canonicalRow, state, shardEntry);
      nextRows[wallet] = canonicalRow;
      nextStates[wallet] = canonicalState;
      knownWallets.push(wallet);
      if (String(canonicalState.lifecycleStage || "").toLowerCase() === "live_tracking") {
        liveWallets.push(wallet);
      }
      if (canonicalState.tradeDone && canonicalState.fundingDone && canonicalState.remoteHistoryVerified) {
        return;
      }
      if (Number(canonicalState.tradeRowsLoaded || 0) > 0 || Number(canonicalState.fundingRowsLoaded || 0) > 0) {
        continuationQueue.push(wallet);
      } else {
        priorityQueue.push(wallet);
      }
    });

  const nextStatePayload = {
    ...statePayload,
    version: 5,
    knownWallets,
    liveWallets,
    priorityQueue,
    continuationQueue,
    walletStates: nextStates,
  };
  const nextStorePayload = {
    version: 2,
    wallets: nextRows,
    updatedAt: Date.now(),
  };

  writeJsonAtomic(storePath, nextStorePayload);
  writeJsonAtomic(statePath, nextStatePayload);

  return {
    shard: shardEntry.index,
    walletRows: Object.keys(nextRows).length,
    walletStates: Object.keys(nextStates).length,
    liveWallets: liveWallets.length,
    priorityQueue: priorityQueue.length,
    continuationQueue: continuationQueue.length,
  };
}

function main() {
  const summaries = listShardDirs(SHARDS_DIR, ACTIVE_SHARDS).map(materializeShard);
  const rebuildScript = path.join(ROOT, "scripts", "rebuild_ui_read_model.py");
  execFileSync("python3", [rebuildScript], {
    cwd: ROOT,
    stdio: "inherit",
  });
  console.log(
    JSON.stringify(
      {
        version: 1,
        shardCount: summaries.length,
        summaries,
      },
      null,
      2
    )
  );
}

main();
