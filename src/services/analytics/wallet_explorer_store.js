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
  }

  load() {
    ensureDir(path.dirname(this.filePath));
    this.data = readJson(this.filePath, emptyData()) || emptyData();
    if (!this.data.wallets || typeof this.data.wallets !== "object") {
      this.data.wallets = {};
    }
    return this.data;
  }

  upsert(record) {
    if (!record || !record.wallet) return false;
    const wallet = String(record.wallet).trim();
    if (!wallet) return false;

    this.data.wallets[wallet] = {
      ...record,
      wallet,
    };

    this.data.updatedAt = Date.now();
    writeJsonAtomic(this.filePath, this.data);
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
    ensureDir(path.dirname(this.filePath));
    writeJsonAtomic(this.filePath, this.data);
    return true;
  }
}

module.exports = {
  WalletExplorerStore,
};
