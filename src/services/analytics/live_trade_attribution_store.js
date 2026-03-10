const path = require("path");
const { ensureDir, readJson, writeJsonAtomic } = require("../pipeline/utils");

function normalizeKey(value) {
  if (value === null || value === undefined) return "";
  const text = String(value).trim();
  return text || "";
}

function normalizeWallet(value) {
  const text = String(value || "").trim();
  return text || "";
}

function normalizeSignature(value) {
  const text = String(value || "").trim();
  return text || "";
}

function emptyData() {
  return {
    version: 1,
    updatedAt: 0,
    stats: {
      writes: 0,
      lastWriteAt: 0,
      walletUpserts: 0,
      txUpserts: 0,
    },
    links: {
      history: {},
      order: {},
      li: {},
    },
    evidence: [],
  };
}

class LiveTradeAttributionStore {
  constructor(options = {}) {
    this.dataDir = options.dataDir || path.join(process.cwd(), "data", "indexer");
    this.filePath =
      options.filePath || path.join(this.dataDir, "live_trade_attribution_db.json");
    this.flushIntervalMs = Math.max(1000, Number(options.flushIntervalMs || 15000));
    this.maxBufferedUpdates = Math.max(10, Number(options.maxBufferedUpdates || 500));
    this.maxEvidenceRows = Math.max(1000, Number(options.maxEvidenceRows || 50000));
    this.maxLinksPerKind = Math.max(10000, Number(options.maxLinksPerKind || 300000));

    this.data = emptyData();
    this.flushTimer = null;
    this.dirty = false;
    this.bufferedUpdates = 0;
    this.lastWriteAt = 0;
  }

  load() {
    ensureDir(path.dirname(this.filePath));
    this.data = readJson(this.filePath, emptyData()) || emptyData();
    if (!this.data.links || typeof this.data.links !== "object") {
      this.data.links = { history: {}, order: {}, li: {} };
    }
    if (!this.data.links.history || typeof this.data.links.history !== "object") {
      this.data.links.history = {};
    }
    if (!this.data.links.order || typeof this.data.links.order !== "object") {
      this.data.links.order = {};
    }
    if (!this.data.links.li || typeof this.data.links.li !== "object") {
      this.data.links.li = {};
    }
    if (!Array.isArray(this.data.evidence)) this.data.evidence = [];
    if (!this.data.stats || typeof this.data.stats !== "object") {
      this.data.stats = {
        writes: 0,
        lastWriteAt: 0,
        walletUpserts: 0,
        txUpserts: 0,
      };
    }
    this.dirty = false;
    this.bufferedUpdates = 0;
    return this.data;
  }

  stop() {
    this.flush(true);
  }

  scheduleFlush() {
    if (this.flushTimer) return;
    this.flushTimer = setTimeout(() => {
      this.flushTimer = null;
      this.flush(false);
    }, this.flushIntervalMs);
    if (this.flushTimer && typeof this.flushTimer.unref === "function") {
      this.flushTimer.unref();
    }
  }

  flush(force = false) {
    if (!this.dirty && !force) return false;
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    ensureDir(path.dirname(this.filePath));
    this.data.updatedAt = Date.now();
    this.data.stats.writes = Number(this.data.stats.writes || 0) + 1;
    this.data.stats.lastWriteAt = this.data.updatedAt;
    writeJsonAtomic(this.filePath, this.data);
    this.dirty = false;
    this.bufferedUpdates = 0;
    this.lastWriteAt = this.data.updatedAt;
    return true;
  }

  markDirty() {
    this.dirty = true;
    this.bufferedUpdates += 1;
    if (
      this.bufferedUpdates >= this.maxBufferedUpdates ||
      Date.now() - Number(this.lastWriteAt || 0) >= this.flushIntervalMs
    ) {
      this.flush(true);
    } else {
      this.scheduleFlush();
    }
  }

  getKindMap(kind) {
    if (kind === "history") return this.data.links.history;
    if (kind === "order") return this.data.links.order;
    if (kind === "li") return this.data.links.li;
    return null;
  }

  trimKind(kind) {
    const map = this.getKindMap(kind);
    if (!map) return;
    const keys = Object.keys(map);
    if (keys.length <= this.maxLinksPerKind) return;
    keys.sort((a, b) => {
      const aAt = Number((map[a] && map[a].updatedAt) || 0);
      const bAt = Number((map[b] && map[b].updatedAt) || 0);
      return aAt - bAt;
    });
    const removeCount = keys.length - this.maxLinksPerKind;
    for (let i = 0; i < removeCount; i += 1) {
      delete map[keys[i]];
    }
  }

  appendEvidence(row) {
    this.data.evidence.push(row);
    if (this.data.evidence.length > this.maxEvidenceRows) {
      this.data.evidence.splice(0, this.data.evidence.length - this.maxEvidenceRows);
    }
  }

  upsertWalletLink(kind, keyValue, wallet, meta = {}) {
    const key = normalizeKey(keyValue);
    const walletNorm = normalizeWallet(wallet);
    if (!key || !walletNorm) return false;
    const map = this.getKindMap(kind);
    if (!map) return false;
    const now = Date.now();
    const prev = map[key] && typeof map[key] === "object" ? map[key] : {};
    map[key] = {
      ...prev,
      key,
      kind,
      wallet: walletNorm,
      walletSource: String(meta.walletSource || meta.source || prev.walletSource || "").trim() || null,
      walletConfidence:
        String(meta.walletConfidence || meta.confidence || prev.walletConfidence || "").trim() || null,
      updatedAt: now,
    };
    this.data.stats.walletUpserts = Number(this.data.stats.walletUpserts || 0) + 1;
    this.appendEvidence({
      at: now,
      action: "wallet_upsert",
      kind,
      key,
      wallet: walletNorm,
      source: map[key].walletSource,
      confidence: map[key].walletConfidence,
    });
    this.trimKind(kind);
    this.markDirty();
    return true;
  }

  upsertTxLink(kind, keyValue, txMeta = {}) {
    const key = normalizeKey(keyValue);
    if (!key) return false;
    const signature = normalizeSignature(txMeta.signature || txMeta.txSignature || txMeta.txid);
    const map = this.getKindMap(kind);
    if (!map) return false;
    const now = Date.now();
    const prev = map[key] && typeof map[key] === "object" ? map[key] : {};
    map[key] = {
      ...prev,
      key,
      kind,
      txSignature: signature || prev.txSignature || null,
      txSource: String(txMeta.source || prev.txSource || "").trim() || null,
      txConfidence: String(txMeta.confidence || prev.txConfidence || "").trim() || null,
      signer: normalizeWallet(txMeta.signer || prev.signer || ""),
      updatedAt: now,
    };
    this.data.stats.txUpserts = Number(this.data.stats.txUpserts || 0) + 1;
    this.appendEvidence({
      at: now,
      action: "tx_upsert",
      kind,
      key,
      txSignature: map[key].txSignature,
      source: map[key].txSource,
      confidence: map[key].txConfidence,
      signer: map[key].signer || null,
    });
    this.trimKind(kind);
    this.markDirty();
    return true;
  }

  hydrateMaps() {
    const history = new Map();
    const order = new Map();
    const li = new Map();
    const historyTx = new Map();
    const orderTx = new Map();
    const liTx = new Map();

    const copy = (kind, outWallet, outTx) => {
      const map = this.getKindMap(kind) || {};
      Object.entries(map).forEach(([key, row]) => {
        if (!key || !row || typeof row !== "object") return;
        if (row.wallet) outWallet.set(key, String(row.wallet));
        if (row.txSignature) {
          outTx.set(key, {
            signature: String(row.txSignature),
            signer: row.signer ? String(row.signer) : null,
            source: row.txSource ? String(row.txSource) : null,
            confidence: row.txConfidence ? String(row.txConfidence) : null,
            observedAt: Number(row.updatedAt || 0) || Date.now(),
          });
        }
      });
    };

    copy("history", history, historyTx);
    copy("order", order, orderTx);
    copy("li", li, liTx);

    return {
      walletMaps: { history, order, li },
      txMaps: { history: historyTx, order: orderTx, li: liTx },
    };
  }

  getStatus() {
    const history = this.getKindMap("history");
    const order = this.getKindMap("order");
    const li = this.getKindMap("li");
    return {
      filePath: this.filePath,
      updatedAt: Number(this.data.updatedAt || 0) || null,
      links: {
        history: history ? Object.keys(history).length : 0,
        order: order ? Object.keys(order).length : 0,
        li: li ? Object.keys(li).length : 0,
      },
      evidenceRows: Array.isArray(this.data.evidence) ? this.data.evidence.length : 0,
      writes: Number(this.data.stats.writes || 0),
      walletUpserts: Number(this.data.stats.walletUpserts || 0),
      txUpserts: Number(this.data.stats.txUpserts || 0),
      lastWriteAt: Number(this.data.stats.lastWriteAt || 0) || null,
    };
  }

  getRecentEvidence(limit = 100) {
    const safeLimit = Math.max(1, Math.min(1000, Number(limit || 100)));
    const rows = Array.isArray(this.data.evidence) ? this.data.evidence : [];
    return rows.slice(Math.max(0, rows.length - safeLimit)).reverse();
  }

  getGrowthSnapshot(nowMs = Date.now()) {
    const rows = Array.isArray(this.data.evidence) ? this.data.evidence : [];
    const now = Number(nowMs || Date.now());
    const oneHourAgo = now - 60 * 60 * 1000;
    const oneDayAgo = now - 24 * 60 * 60 * 1000;
    let wallet1h = 0;
    let wallet24h = 0;
    let tx1h = 0;
    let tx24h = 0;
    rows.forEach((row) => {
      const at = Number((row && row.at) || 0);
      if (!Number.isFinite(at) || at <= 0) return;
      const action = String((row && row.action) || "").trim().toLowerCase();
      if (action === "wallet_upsert") {
        if (at >= oneHourAgo) wallet1h += 1;
        if (at >= oneDayAgo) wallet24h += 1;
      } else if (action === "tx_upsert") {
        if (at >= oneHourAgo) tx1h += 1;
        if (at >= oneDayAgo) tx24h += 1;
      }
    });
    const total1h = wallet1h + tx1h;
    const total24h = wallet24h + tx24h;
    const perHourRate = Number(total1h.toFixed(2));
    return {
      walletUpserts1h: wallet1h,
      walletUpserts24h: wallet24h,
      txUpserts1h: tx1h,
      txUpserts24h: tx24h,
      totalUpserts1h: total1h,
      totalUpserts24h: total24h,
      ratePerHour: perHourRate,
      computedAt: now,
    };
  }
}

module.exports = {
  LiveTradeAttributionStore,
};
