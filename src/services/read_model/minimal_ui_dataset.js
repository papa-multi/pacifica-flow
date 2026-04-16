const path = require("path");

const {
  ensureDir,
  normalizeAddress,
  readJson,
  writeJsonAtomic,
} = require("../pipeline/utils");
const { getCollectionMtimeMs, getFileMtimeMs, listShardDataFiles, loadMergedIndexerState } = require("../indexer/indexer_shard_store");

const MINIMAL_UI_DATASET_VERSION = 1;

function rowsFromWalletStore(payload) {
  if (Array.isArray(payload)) return payload;
  if (!payload || typeof payload !== "object") return [];
  if (payload.wallets && typeof payload.wallets === "object" && !Array.isArray(payload.wallets)) {
    return Object.values(payload.wallets);
  }
  if (Array.isArray(payload.wallets)) return payload.wallets;
  if (Array.isArray(payload.rows)) return payload.rows;
  return [];
}

function walletRowFreshness(row = {}) {
  return Math.max(
    Number(row.updatedAt || 0) || 0,
    Number((row.all && row.all.computedAt) || 0) || 0,
    Number(row.createdAt || 0) || 0,
    Number((row.storage && row.storage.updatedAt) || 0) || 0
  );
}

function pickFresherWalletRow(existing = null, incoming = null) {
  if (!existing) return incoming;
  if (!incoming) return existing;
  return walletRowFreshness(incoming) >= walletRowFreshness(existing) ? incoming : existing;
}

function buildMinimalUiDataset({ shardsDir, globalKpiPath, shardCount = 0 }) {
  const shardOptions = shardCount > 0 ? { shardCount } : {};
  const mergedState = loadMergedIndexerState(shardsDir, shardOptions).state || {};
  const addressSet = new Set(
    [
      ...(Array.isArray(mergedState.knownWallets) ? mergedState.knownWallets : []),
      ...Object.keys(mergedState.walletStates || {}),
    ]
      .map((wallet) => normalizeAddress(wallet))
      .filter(Boolean)
  );

  const walletRows = new Map();
  const walletStorePaths = listShardDataFiles(shardsDir, "wallets.json", shardOptions);
  walletStorePaths.forEach((filePath) => {
    const payload = readJson(filePath, null);
    rowsFromWalletStore(payload).forEach((row) => {
      const wallet = normalizeAddress(row && row.wallet ? row.wallet : "");
      if (!wallet) return;
      addressSet.add(wallet);
      const current = walletRows.get(wallet) || null;
      walletRows.set(wallet, pickFresherWalletRow(current, row));
    });
  });

  let walletSummedVolumeUsd = 0;
  walletRows.forEach((row) => {
    walletSummedVolumeUsd += Number((row && row.all && row.all.volumeUsd) || 0) || 0;
  });

  const addresses = [...addressSet].sort((left, right) => String(left).localeCompare(String(right)));
  const globalKpi = readJson(globalKpiPath, {}) || {};
  const totalHistoricalVolumeUsd = Number(globalKpi.totalHistoricalVolume || 0) || 0;
  const dailyVolumeUsd = Number(globalKpi.dailyVolume || 0) || 0;
  const openInterestUsd = Number(globalKpi.openInterestAtEnd || 0) || 0;
  const exchangeUpdatedAt =
    Number(globalKpi.updatedAt || 0) ||
    Number(globalKpi.fetchedAt || 0) ||
    null;
  const volumeGapUsd = totalHistoricalVolumeUsd - walletSummedVolumeUsd;
  const volumeRatio =
    totalHistoricalVolumeUsd > 0 ? walletSummedVolumeUsd / totalHistoricalVolumeUsd : null;

  return {
    version: MINIMAL_UI_DATASET_VERSION,
    generatedAt: Date.now(),
    wallets: {
      total: addresses.length,
      addresses,
    },
    exchangeOverview: {
      totalHistoricalVolumeUsd,
      dailyVolumeUsd,
      openInterestUsd,
      updatedAt: exchangeUpdatedAt,
      walletSummedVolumeUsd,
      walletCount: addresses.length,
      volumeGapUsd,
      volumeRatio,
    },
    validation: {
      walletVolumeMatchesExchange:
        totalHistoricalVolumeUsd > 0 && Math.abs(volumeGapUsd) < 0.01,
      note:
        totalHistoricalVolumeUsd > 0
          ? "exchange_total_vs_wallet_sum_checked"
          : "exchange_total_unavailable",
    },
  };
}

function writeMinimalUiDataset(filePath, dataset) {
  ensureDir(path.dirname(filePath));
  writeJsonAtomic(filePath, dataset);
  return dataset;
}

function loadMinimalUiDataset(filePath) {
  const payload = readJson(filePath, null);
  if (
    !payload ||
    typeof payload !== "object" ||
    Number(payload.version || 0) !== MINIMAL_UI_DATASET_VERSION ||
    !payload.wallets ||
    !Array.isArray(payload.wallets.addresses) ||
    !payload.exchangeOverview ||
    typeof payload.exchangeOverview !== "object"
  ) {
    return null;
  }
  return payload;
}

function getMinimalUiSourceMtimeMs(shardsDir, globalKpiPath, shardCount = 0) {
  const shardOptions = shardCount > 0 ? { shardCount } : {};
  return Math.max(
    getCollectionMtimeMs(listShardDataFiles(shardsDir, "indexer_state.json", shardOptions)),
    getCollectionMtimeMs(listShardDataFiles(shardsDir, "wallets.json", shardOptions)),
    getFileMtimeMs(globalKpiPath)
  );
}

module.exports = {
  MINIMAL_UI_DATASET_VERSION,
  buildMinimalUiDataset,
  getMinimalUiSourceMtimeMs,
  loadMinimalUiDataset,
  writeMinimalUiDataset,
};
