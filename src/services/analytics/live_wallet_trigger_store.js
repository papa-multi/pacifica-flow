const fs = require("fs");
const path = require("path");
const { ensureDir } = require("../pipeline/utils");

function normalizeWallet(value) {
  const text = String(value || "").trim();
  return text || "";
}

function normalizeTrigger(trigger = {}) {
  const wallet = normalizeWallet(trigger.wallet);
  if (!wallet) return null;
  return {
    wallet,
    at: Math.max(0, Number(trigger.at || trigger.timestamp || Date.now())),
    slot: Number.isFinite(Number(trigger.slot)) ? Number(trigger.slot) : null,
    symbol: String(trigger.symbol || "").trim().toUpperCase() || null,
    source: String(trigger.source || "external").trim() || "external",
    reason: String(trigger.reason || trigger.source || "activity").trim() || "activity",
    method:
      String(trigger.method || trigger.reason || trigger.source || "activity").trim() ||
      "activity",
    confidence: String(trigger.confidence || "unknown").trim() || "unknown",
    signature: String(trigger.signature || trigger.txSignature || "").trim() || null,
    programId: String(trigger.programId || "").trim() || null,
  };
}

class LiveWalletTriggerStore {
  constructor(options = {}) {
    this.filePath = String(options.filePath || "").trim();
    this.maxEntries = Math.max(1000, Number(options.maxEntries || 50000));
    this.compactEvery = Math.max(100, Number(options.compactEvery || 1000));
    this.appendCount = 0;
    if (this.filePath) ensureDir(path.dirname(this.filePath));
  }

  append(trigger) {
    const normalized = normalizeTrigger(trigger);
    if (!normalized || !this.filePath) return false;
    fs.appendFileSync(this.filePath, `${JSON.stringify(normalized)}\n`);
    this.appendCount += 1;
    if (this.appendCount % this.compactEvery === 0) {
      this.compact();
    }
    return true;
  }

  readSince(sinceAt = 0, options = {}) {
    if (!this.filePath || !fs.existsSync(this.filePath)) return [];
    const limit = Math.max(1, Math.min(this.maxEntries, Number(options.limit || 2000)));
    const raw = fs.readFileSync(this.filePath, "utf8");
    const lines = raw.split("\n").filter(Boolean);
    const rows = [];
    for (let i = lines.length - 1; i >= 0 && rows.length < limit; i -= 1) {
      try {
        const parsed = JSON.parse(lines[i]);
        const normalized = normalizeTrigger(parsed);
        if (!normalized) continue;
        if (Number(normalized.at || 0) <= Number(sinceAt || 0)) break;
        rows.push(normalized);
      } catch (_error) {
        // ignore malformed line
      }
    }
    return rows.reverse();
  }

  compact() {
    if (!this.filePath || !fs.existsSync(this.filePath)) return;
    const raw = fs.readFileSync(this.filePath, "utf8");
    const lines = raw.split("\n").filter(Boolean);
    if (lines.length <= this.maxEntries) return;
    const kept = lines.slice(-this.maxEntries).join("\n");
    fs.writeFileSync(this.filePath, kept ? `${kept}\n` : "", "utf8");
  }
}

module.exports = {
  LiveWalletTriggerStore,
};
