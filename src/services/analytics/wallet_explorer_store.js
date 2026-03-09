const path = require("path");
const { ensureDir, readJson, writeJsonAtomic } = require("../pipeline/utils");

function emptyData() {
  return {
    wallets: {},
    updatedAt: null,
  };
}

class WalletExplorerStore {
  constructor(options = {}) {
    this.dataDir = options.dataDir || path.join(process.cwd(), "data", "indexer");
    this.filePath = options.filePath || path.join(this.dataDir, "wallets.json");
    this.data = emptyData();
    this.flushIntervalMs = Math.max(250, Number(options.flushIntervalMs || 5000));
    this.maxBufferedUpdates = Math.max(1, Number(options.maxBufferedUpdates || 300));
    this.lastWriteAt = 0;
    this.bufferedUpdates = 0;
    this.dirty = false;
    this.flushTimer = null;
  }

  load() {
    ensureDir(path.dirname(this.filePath));
    this.data = readJson(this.filePath, emptyData()) || emptyData();
    if (!this.data.wallets || typeof this.data.wallets !== "object") {
      this.data.wallets = {};
    }
    this.dirty = false;
    this.bufferedUpdates = 0;
    return this.data;
  }

  scheduleFlush() {
    if (this.flushTimer) return;
    this.flushTimer = setTimeout(() => {
      this.flushTimer = null;
      this.flush(false);
    }, this.flushIntervalMs);
  }

  flush(force = false) {
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
    if (!record || !record.wallet) return false;
    const wallet = String(record.wallet).trim();
    if (!wallet) return false;

    this.data.wallets[wallet] = {
      ...record,
      wallet,
    };

    this.data.updatedAt = Date.now();
    this.dirty = true;
    this.bufferedUpdates += 1;
    const force = Boolean(options && options.force);
    if (
      force ||
      this.bufferedUpdates >= this.maxBufferedUpdates ||
      Date.now() - Number(this.lastWriteAt || 0) >= this.flushIntervalMs
    ) {
      this.flush(true);
    } else {
      this.scheduleFlush();
    }
    return true;
  }

  touch(wallet, scannedAt = Date.now(), options = {}) {
    const key = wallet ? String(wallet).trim() : "";
    if (!key) return false;
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
    if (
      force ||
      this.bufferedUpdates >= this.maxBufferedUpdates ||
      Date.now() - Number(this.lastWriteAt || 0) >= this.flushIntervalMs
    ) {
      this.flush(true);
    } else {
      this.scheduleFlush();
    }
    return true;
  }

  list() {
    return Object.values(this.data.wallets || {});
  }

  get(wallet) {
    if (!wallet) return null;
    return (this.data.wallets && this.data.wallets[String(wallet).trim()]) || null;
  }

  reset() {
    this.data = emptyData();
    this.data.updatedAt = Date.now();
    this.dirty = true;
    this.bufferedUpdates = 0;
    ensureDir(path.dirname(this.filePath));
    this.flush(true);
    return true;
  }

  stop() {
    this.flush(true);
  }
}

module.exports = {
  WalletExplorerStore,
};
