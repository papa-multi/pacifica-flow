const crypto = require("crypto");
const path = require("path");
const { ensureDir, readJson, writeJsonAtomic } = require("../pipeline/utils");
const { shardIndexForKey } = require("../indexer/sharding");
const {
  listShardDataFiles,
  getCollectionMtimeMs,
  getFileMtimeMs,
} = require("../indexer/indexer_shard_store");

const STORE_VERSION = 2;

function emptyData() {
  return {
    version: STORE_VERSION,
    wallets: {},
    updatedAt: null,
  };
}

function createWalletRecordId() {
  return `wal_${crypto.randomUUID().replace(/-/g, "")}`;
}

class WalletExplorerStore {
  constructor(options = {}) {
    this.dataDir = options.dataDir || path.join(process.cwd(), "data", "indexer");
    this.filePath = options.filePath || path.join(this.dataDir, "wallets.json");
    this.externalShardsEnabled = Boolean(options.externalShardsEnabled);
    this.externalShardsDir =
      options.externalShardsDir || path.join(this.dataDir, "shards");
    this.ownerShardCount = Math.max(1, Number(options.ownerShardCount || 1) || 1);
    this.ownerShardIndex = Math.max(
      0,
      Math.min(this.ownerShardCount - 1, Number(options.ownerShardIndex || 0) || 0)
    );
    this.enforceOwnerOnly =
      !this.externalShardsEnabled &&
      this.ownerShardCount > 1 &&
      Boolean(
        options.enforceOwnerOnly !== undefined ? options.enforceOwnerOnly : true
      );
    this.readOnly = this.externalShardsEnabled || Boolean(options.readOnly);
    this.data = emptyData();
    this.flushIntervalMs = Math.max(250, Number(options.flushIntervalMs || 5000));
    this.maxBufferedUpdates = Math.max(1, Number(options.maxBufferedUpdates || 300));
    this.lastWriteAt = 0;
    this.lastExternalMtimeMs = 0;
    this.bufferedUpdates = 0;
    this.dirty = false;
    this.flushTimer = null;
    this.lastExternalShardMtimeMs = 0;
    this.lastMergedSnapshotMtimeMs = 0;
  }

  load() {
    if (this.externalShardsEnabled) {
      return this.loadExternalShards(false, { preferSnapshotFirst: true });
    }
    ensureDir(path.dirname(this.filePath));
    this.data = readJson(this.filePath, emptyData()) || emptyData();
    if (!this.data || typeof this.data !== "object") {
      this.data = emptyData();
    }
    let mutated = false;
    if (!Number.isFinite(Number(this.data.version)) || Number(this.data.version) < STORE_VERSION) {
      this.data.version = STORE_VERSION;
      mutated = true;
    }
    if (!this.data.wallets || typeof this.data.wallets !== "object") {
      this.data.wallets = {};
      mutated = true;
    }
    Object.entries(this.data.wallets).forEach(([wallet, record]) => {
      const safeRecord = record && typeof record === "object" ? record : {};
      const normalized = this.normalizeRecord(safeRecord, { wallet });
      this.data.wallets[wallet] = normalized;
      const needsMigration =
        String(safeRecord.wallet || "").trim() !== wallet ||
        !String(safeRecord.walletRecordId || "").trim() ||
        !Number.isFinite(Number(safeRecord.createdAt)) ||
        !safeRecord.storage ||
        safeRecord.storage.walletStorePath !== this.filePath;
      if (needsMigration) mutated = true;
    });
    if (this.enforceOwnerOnly) {
      mutated = this.pruneUnownedWallets() || mutated;
    }
    this.dirty = false;
    this.bufferedUpdates = 0;
    if (mutated) {
      this.dirty = true;
      this.flush(true);
    }
    return this.data;
  }

  getExternalShardFilePaths() {
    return listShardDataFiles(this.externalShardsDir, "wallets.json");
  }

  getExternalShardMtimeMs() {
    return getCollectionMtimeMs(this.getExternalShardFilePaths());
  }

  getMergedSnapshotMtimeMs() {
    return getFileMtimeMs(this.filePath);
  }

  needsExternalRefresh() {
    return this.getExternalShardMtimeMs() > this.getMergedSnapshotMtimeMs();
  }

  loadMergedSnapshot() {
    ensureDir(path.dirname(this.filePath));
    this.data = readJson(this.filePath, emptyData()) || emptyData();
    if (!this.data || typeof this.data !== "object") {
      this.data = emptyData();
    }
    if (!this.data.wallets || typeof this.data.wallets !== "object") {
      this.data.wallets = {};
    }
    Object.entries(this.data.wallets).forEach(([wallet, record]) => {
      const safeRecord = record && typeof record === "object" ? record : {};
      this.data.wallets[wallet] = this.normalizeRecord(safeRecord, { wallet });
    });
    this.lastMergedSnapshotMtimeMs = this.getMergedSnapshotMtimeMs();
    return this.data;
  }

  loadExternalShards(force = false, options = {}) {
    const preferSnapshotFirst =
      options && Object.prototype.hasOwnProperty.call(options, "preferSnapshotFirst")
        ? Boolean(options.preferSnapshotFirst)
        : true;
    const filePaths = this.getExternalShardFilePaths();
    const shardMtimeMs = getCollectionMtimeMs(filePaths);
    const snapshotMtimeMs = this.getMergedSnapshotMtimeMs();
    this.lastExternalShardMtimeMs = shardMtimeMs;
    this.lastMergedSnapshotMtimeMs = snapshotMtimeMs;

    if (!force && preferSnapshotFirst && snapshotMtimeMs > 0) {
      return this.loadMergedSnapshot();
    }

    const mtimeMs = shardMtimeMs;
    if (!filePaths.length) {
      this.loadMergedSnapshot();
      this.lastExternalMtimeMs = 0;
      return this.data;
    }
    if (
      !force &&
      this.data &&
      this.data.wallets &&
      Object.keys(this.data.wallets).length > 0 &&
      mtimeMs > 0 &&
      mtimeMs === Number(this.lastExternalMtimeMs || 0)
    ) {
      return this.data;
    }

    const merged = emptyData();
    const mergedWallets = {};
    filePaths.forEach((filePath) => {
      const payload = readJson(filePath, null);
      if (!payload || typeof payload !== "object" || !payload.wallets) return;
      const shardId = path.basename(path.dirname(filePath));
      Object.entries(payload.wallets || {}).forEach(([wallet, record]) => {
        const normalized = String(wallet || "").trim();
        if (!normalized || !record || typeof record !== "object") return;
        const current = mergedWallets[normalized] || null;
        const currentUpdatedAt = Number(
          current &&
            (current.updatedAt ||
              (current.all && current.all.computedAt) ||
              current.createdAt ||
              0)
        ) || 0;
        const nextUpdatedAt = Number(
          record.updatedAt ||
            (record.all && record.all.computedAt) ||
            record.createdAt ||
            0
        ) || 0;
        const sourceRecord =
          current && currentUpdatedAt > nextUpdatedAt ? current : record;
        const previousRecord =
          current && currentUpdatedAt > nextUpdatedAt ? record : current;
        const normalizedRecord = this.normalizeRecord(sourceRecord, {
          wallet: normalized,
          previous: previousRecord || null,
        });
        normalizedRecord.storage = {
          ...(normalizedRecord.storage && typeof normalizedRecord.storage === "object"
            ? normalizedRecord.storage
            : {}),
          walletStorePath:
            record.storage && record.storage.walletStorePath
              ? String(record.storage.walletStorePath)
              : filePath,
          mergedWalletStorePath: this.filePath,
          indexerShardId:
            (record.storage && record.storage.indexerShardId) || shardId,
        };
        mergedWallets[normalized] = normalizedRecord;
      });
      merged.updatedAt = Math.max(
        Number(merged.updatedAt || 0) || 0,
        Number(payload.updatedAt || 0) || 0
      ) || merged.updatedAt;
    });

    merged.wallets = mergedWallets;
    merged.updatedAt = Number(merged.updatedAt || 0) || mtimeMs || Date.now();
    this.data = merged;
    this.lastExternalMtimeMs = mtimeMs;
    this.dirty = false;
    this.bufferedUpdates = 0;
    ensureDir(path.dirname(this.filePath));
    writeJsonAtomic(this.filePath, this.data);
    this.lastWriteAt = Date.now();
    this.lastMergedSnapshotMtimeMs = this.getMergedSnapshotMtimeMs();
    return this.data;
  }

  normalizeRecord(record = {}, options = {}) {
    const wallet = String((options && options.wallet) || record.wallet || "").trim();
    const previous =
      options && options.previous && typeof options.previous === "object" ? options.previous : {};
    const createdAtSource =
      Number(record.createdAt || 0) ||
      Number(previous.createdAt || 0) ||
      Number(record.updatedAt || 0) ||
      Number(previous.updatedAt || 0) ||
      Date.now();
    const walletRecordId =
      String(record.walletRecordId || previous.walletRecordId || "").trim() || createWalletRecordId();
    const storage = {
      ...(previous.storage && typeof previous.storage === "object" ? previous.storage : {}),
      ...(record.storage && typeof record.storage === "object" ? record.storage : {}),
    };
    if (!this.externalShardsEnabled || !String(storage.walletStorePath || "").trim()) {
      storage.walletStorePath = this.filePath;
    }
    const all =
      record && record.all && typeof record.all === "object"
        ? record.all
        : previous && previous.all && typeof previous.all === "object"
        ? previous.all
        : null;
    const nextRecord = {
      ...previous,
      ...record,
    };
    if (all) {
      [
        "trades",
        "volumeUsd",
        "feesUsd",
        "feesPaidUsd",
        "liquidityPoolFeesUsd",
        "feeRebatesUsd",
        "netFeesUsd",
        "fundingPayoutUsd",
        "revenueUsd",
        "pnlUsd",
        "wins",
        "losses",
        "winRatePct",
        "firstTrade",
        "lastTrade",
      ].forEach((key) => {
        if (nextRecord[key] === undefined || nextRecord[key] === null) {
          nextRecord[key] = all[key];
        }
      });
    }
    return {
      ...nextRecord,
      wallet,
      walletRecordId,
      createdAt: createdAtSource,
      storage,
    };
  }

  ownsWallet(wallet) {
    const normalized = String(wallet || "").trim();
    if (!normalized) return false;
    if (!this.enforceOwnerOnly) return true;
    return (
      shardIndexForKey(`wallet:${normalized}`, this.ownerShardCount) === this.ownerShardIndex
    );
  }

  pruneUnownedWallets() {
    if (!this.enforceOwnerOnly) return false;
    const wallets =
      this.data && this.data.wallets && typeof this.data.wallets === "object"
        ? this.data.wallets
        : {};
    let removed = 0;
    Object.keys(wallets).forEach((wallet) => {
      if (this.ownsWallet(wallet)) return;
      delete wallets[wallet];
      removed += 1;
    });
    if (removed > 0) {
      this.data.updatedAt = Date.now();
    }
    return removed > 0;
  }

  scheduleFlush() {
    if (this.flushTimer) return;
    this.flushTimer = setTimeout(() => {
      this.flushTimer = null;
      this.flush(false);
    }, this.flushIntervalMs);
  }

  flush(force = false) {
    if (this.readOnly) return false;
    if (!this.dirty && !force) return false;
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    ensureDir(path.dirname(this.filePath));
    writeJsonAtomic(this.filePath, this.data);
    this.dirty = false;
    this.bufferedUpdates = 0;
    this.lastWriteAt = Date.now();
    return true;
  }

  upsert(record, options = {}) {
    if (this.readOnly) return false;
    if (!record || !record.wallet) return false;
    const wallet = String(record.wallet).trim();
    if (!wallet) return false;
    if (!this.ownsWallet(wallet)) return false;

    this.data.wallets[wallet] = this.normalizeRecord(record, {
      wallet,
      previous: this.data.wallets[wallet] || null,
    });

    this.data.updatedAt = Date.now();
    this.data.version = STORE_VERSION;
    this.dirty = true;
    this.bufferedUpdates += 1;
    const force = Boolean(options && options.force);
    const deferFlush = Boolean(options && options.deferFlush);
    const shouldFlushNow =
      this.bufferedUpdates >= this.maxBufferedUpdates ||
      Date.now() - Number(this.lastWriteAt || 0) >= this.flushIntervalMs;
    if (force || shouldFlushNow) {
      this.flush(true);
    } else if (deferFlush) {
      this.scheduleFlush();
    } else {
      this.scheduleFlush();
    }
    return true;
  }

  touch(wallet, scannedAt = Date.now(), options = {}) {
    if (this.readOnly) return false;
    const key = wallet ? String(wallet).trim() : "";
    if (!key) return false;
    if (!this.ownsWallet(key)) return false;
    const prev = this.data.wallets && this.data.wallets[key];
    if (!prev) return false;

    const record = {
      ...prev,
      updatedAt: Number(scannedAt) || Date.now(),
      liveScannedAt: Number(scannedAt) || Date.now(),
    };
    this.data.wallets[key] = record;
    this.data.updatedAt = Date.now();
    this.dirty = true;
    this.bufferedUpdates += 1;

    const force = Boolean(options && options.force);
    const deferFlush = Boolean(options && options.deferFlush);
    const shouldFlushNow =
      this.bufferedUpdates >= this.maxBufferedUpdates ||
      Date.now() - Number(this.lastWriteAt || 0) >= this.flushIntervalMs;
    if (force || shouldFlushNow) {
      this.flush(true);
    } else if (deferFlush) {
      this.scheduleFlush();
    } else {
      this.scheduleFlush();
    }
    return true;
  }

  list() {
    return Object.values(this.data.wallets || {});
  }

  prune(predicate) {
    if (this.readOnly) return 0;
    if (typeof predicate !== "function") return 0;
    const wallets = this.data.wallets && typeof this.data.wallets === "object" ? this.data.wallets : {};
    let removed = 0;
    Object.entries(wallets).forEach(([wallet, record]) => {
      if (predicate(record, wallet)) return;
      delete wallets[wallet];
      removed += 1;
    });
    if (removed > 0) {
      this.data.updatedAt = Date.now();
      this.dirty = true;
      this.bufferedUpdates += removed;
      this.flush(true);
    }
    return removed;
  }

  get(wallet) {
    if (!wallet) return null;
    const key = String(wallet).trim();
    if (!this.ownsWallet(key)) return null;
    return (this.data.wallets && this.data.wallets[key]) || null;
  }

  reset() {
    if (this.readOnly) return false;
    this.data = emptyData();
    this.data.updatedAt = Date.now();
    this.dirty = true;
    this.bufferedUpdates = 0;
    ensureDir(path.dirname(this.filePath));
    this.flush(true);
    return true;
  }

  stop() {
    if (!this.readOnly) this.flush(true);
  }
}

module.exports = {
  WalletExplorerStore,
};
