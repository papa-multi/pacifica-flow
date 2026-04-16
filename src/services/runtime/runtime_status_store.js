const fs = require("fs");
const path = require("path");

const {
  ensureDir,
  readJson,
  writeJsonAtomic,
} = require("../pipeline/utils");

function sanitizeName(value, fallback = "service") {
  const text = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return text || fallback;
}

function listStatusFiles(dirPath) {
  if (!dirPath || !fs.existsSync(dirPath)) return [];
  try {
    return fs
      .readdirSync(dirPath)
      .filter((name) => String(name || "").toLowerCase().endsWith(".json"))
      .map((name) => path.join(dirPath, name));
  } catch (_error) {
    return [];
  }
}

class RuntimeStatusStore {
  constructor(options = {}) {
    this.baseDir =
      options.baseDir || path.join(process.cwd(), "data", "runtime", "status");
    this.serviceName = sanitizeName(options.serviceName, "service");
    this.instanceId = sanitizeName(
      options.instanceId || `${this.serviceName}-${process.pid}`,
      `${this.serviceName}-${process.pid}`
    );
    this.filePath =
      options.filePath ||
      path.join(this.baseDir, `${this.serviceName}.${this.instanceId}.json`);
    this.logger = options.logger || console;
    this.staleAfterMs = Math.max(
      10000,
      Number(options.staleAfterMs || 5 * 60 * 1000)
    );
    this.lastSnapshot = null;
  }

  publish(snapshot = {}) {
    const now = Date.now();
    const payload = {
      serviceName: this.serviceName,
      instanceId: this.instanceId,
      pid: process.pid,
      hostname: process.env.HOSTNAME || null,
      generatedAt: now,
      ...((snapshot && typeof snapshot === "object") ? snapshot : {}),
    };
    ensureDir(path.dirname(this.filePath));
    writeJsonAtomic(this.filePath, payload);
    this.lastSnapshot = payload;
    return payload;
  }

  load() {
    const payload = readJson(this.filePath, null);
    if (payload && typeof payload === "object") {
      this.lastSnapshot = payload;
      return payload;
    }
    return this.lastSnapshot;
  }

  remove() {
    try {
      if (fs.existsSync(this.filePath)) fs.unlinkSync(this.filePath);
    } catch (error) {
      this.logger.warn(
        `[runtime-status] failed to remove ${this.filePath}: ${error.message}`
      );
    }
  }
}

function loadRuntimeStatusCollection(baseDir, options = {}) {
  const staleAfterMs = Math.max(
    10000,
    Number(options.staleAfterMs || 5 * 60 * 1000)
  );
  const now = Date.now();
  const rows = listStatusFiles(baseDir)
    .map((filePath) => {
      const payload = readJson(filePath, null);
      if (!payload || typeof payload !== "object") return null;
      return {
        filePath,
        payload,
        stale:
          now - Number(payload.generatedAt || payload.updatedAt || 0) > staleAfterMs,
      };
    })
    .filter(Boolean)
    .sort(
      (a, b) =>
        Number(b.payload.generatedAt || b.payload.updatedAt || 0) -
        Number(a.payload.generatedAt || a.payload.updatedAt || 0)
    );

  const latestByService = {};
  rows.forEach((row) => {
    const key = sanitizeName(row.payload.serviceName, "service");
    if (!latestByService[key] || row.stale === false) {
      latestByService[key] = row.payload;
    }
  });

  return {
    generatedAt: now,
    baseDir,
    staleAfterMs,
    count: rows.length,
    active: rows.filter((row) => !row.stale).length,
    rows: rows.map((row) => ({
      filePath: row.filePath,
      stale: row.stale,
      payload: row.payload,
    })),
    latestByService,
  };
}

module.exports = {
  RuntimeStatusStore,
  loadRuntimeStatusCollection,
};
