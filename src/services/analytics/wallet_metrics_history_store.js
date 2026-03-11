"use strict";

const fs = require("fs");
const path = require("path");
const { ensureDir, readJson, writeJsonAtomic } = require("../pipeline/utils");

function toNum(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function bucketHourKey(nowMs = Date.now()) {
  const d = new Date(Number(nowMs) || Date.now());
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}-${String(
    d.getUTCDate()
  ).padStart(2, "0")}T${String(d.getUTCHours()).padStart(2, "0")}`;
}

function bucketDayKey(nowMs = Date.now()) {
  const d = new Date(Number(nowMs) || Date.now());
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}-${String(
    d.getUTCDate()
  ).padStart(2, "0")}`;
}

function trimOldFiles(dirPath, keep = 120) {
  if (!dirPath || !fs.existsSync(dirPath)) return;
  const files = fs
    .readdirSync(dirPath)
    .filter((name) => name.endsWith(".json"))
    .sort();
  while (files.length > keep) {
    const name = files.shift();
    if (!name) break;
    try {
      fs.unlinkSync(path.join(dirPath, name));
    } catch (_error) {
      break;
    }
  }
}

class WalletMetricsHistoryStore {
  constructor(options = {}) {
    this.baseDir = options.baseDir || path.join(process.cwd(), "data", "indexer", "wallet_metrics_history");
    this.hourlyDir = path.join(this.baseDir, "hourly");
    this.dailyDir = path.join(this.baseDir, "daily");
    this.statusPath = path.join(this.baseDir, "status.json");
    this.retentionHourly = Math.max(24, Number(options.retentionHourly || 24 * 14));
    this.retentionDaily = Math.max(14, Number(options.retentionDaily || 180));
    this.logger = options.logger || console;
    this.status = {
      enabled: true,
      lastCaptureAt: 0,
      lastHourlyKey: null,
      lastDailyKey: null,
      hourlyFiles: 0,
      dailyFiles: 0,
      lastWalletCount: 0,
    };
  }

  load() {
    ensureDir(this.hourlyDir);
    ensureDir(this.dailyDir);
    const status = readJson(this.statusPath, null);
    if (status && typeof status === "object") {
      this.status = {
        ...this.status,
        ...status,
      };
    }
    this.refreshCounts();
    return this.status;
  }

  refreshCounts() {
    this.status.hourlyFiles = fs.existsSync(this.hourlyDir)
      ? fs.readdirSync(this.hourlyDir).filter((name) => name.endsWith(".json")).length
      : 0;
    this.status.dailyFiles = fs.existsSync(this.dailyDir)
      ? fs.readdirSync(this.dailyDir).filter((name) => name.endsWith(".json")).length
      : 0;
  }

  buildWalletSnapshotRows({ walletRows = [], positions = [], nowMs = Date.now() }) {
    const positionMap = new Map();
    (Array.isArray(positions) ? positions : []).forEach((row) => {
      const wallet = String((row && row.wallet) || "").trim();
      if (!wallet) return;
      const current =
        positionMap.get(wallet) || {
          openPositions: 0,
          exposureUsd: 0,
          freshestUpdateAt: 0,
        };
      current.openPositions += 1;
      current.exposureUsd += Math.max(
        0,
        toNum(row && row.positionUsd !== undefined ? row.positionUsd : row && row.notionalUsd, 0)
      );
      current.freshestUpdateAt = Math.max(
        current.freshestUpdateAt,
        toNum(row && (row.updatedAt || row.timestamp), 0)
      );
      positionMap.set(wallet, current);
    });

    const wallets = {};
    (Array.isArray(walletRows) ? walletRows : []).forEach((row) => {
      const wallet = String((row && row.wallet) || "").trim();
      if (!wallet) return;
      const all = row && row.all && typeof row.all === "object" ? row.all : {};
      const d24 = row && row.d24 && typeof row.d24 === "object" ? row.d24 : {};
      const d7 = row && row.d7 && typeof row.d7 === "object" ? row.d7 : {};
      const d30 = row && row.d30 && typeof row.d30 === "object" ? row.d30 : {};
      const live = positionMap.get(wallet) || {};
      const realizedPnlUsd = toNum(
        all.realizedPnlUsd !== undefined ? all.realizedPnlUsd : all.pnlUsd,
        0
      );
      const unrealizedPnlUsd = toNum(row && row.unrealizedPnlUsd, 0);
      const totalPnlUsd = realizedPnlUsd + unrealizedPnlUsd;
      const totalVolumeUsd = toNum(all.volumeUsd, 0);
      const concentrationPct = (() => {
        const symbolVolumes =
          all && all.symbolVolumes && typeof all.symbolVolumes === "object" ? all.symbolVolumes : {};
        const values = Object.values(symbolVolumes)
          .map((value) => toNum(value, NaN))
          .filter((value) => Number.isFinite(value) && value > 0);
        if (!values.length || totalVolumeUsd <= 0) return null;
        return Number(((Math.max(...values) / totalVolumeUsd) * 100).toFixed(2));
      })();
      wallets[wallet] = {
        wallet,
        capturedAt: nowMs,
        updatedAt: toNum(row && row.updatedAt, 0) || nowMs,
        liveScannedAt: toNum(row && row.liveScannedAt, 0) || null,
        all: {
          trades: toNum(all.trades, 0),
          volumeUsd: Number(totalVolumeUsd.toFixed(2)),
          realizedPnlUsd: Number(realizedPnlUsd.toFixed(2)),
          winRatePct: Number(toNum(all.winRatePct, 0).toFixed(2)),
        },
        d24: {
          trades: toNum(d24.trades, 0),
          volumeUsd: Number(toNum(d24.volumeUsd, 0).toFixed(2)),
          pnlUsd: Number(toNum(d24.pnlUsd, 0).toFixed(2)),
          winRatePct: Number(toNum(d24.winRatePct, 0).toFixed(2)),
        },
        d7: {
          trades: toNum(d7.trades, 0),
          volumeUsd: Number(toNum(d7.volumeUsd, 0).toFixed(2)),
          pnlUsd: Number(toNum(d7.pnlUsd, 0).toFixed(2)),
          winRatePct: Number(toNum(d7.winRatePct, 0).toFixed(2)),
        },
        d30: {
          trades: toNum(d30.trades, 0),
          volumeUsd: Number(toNum(d30.volumeUsd, 0).toFixed(2)),
          pnlUsd: Number(toNum(d30.pnlUsd, 0).toFixed(2)),
          winRatePct: Number(toNum(d30.winRatePct, 0).toFixed(2)),
        },
        live: {
          openPositions: toNum(live.openPositions, 0),
          exposureUsd: Number(toNum(live.exposureUsd, 0).toFixed(2)),
          latestPositionAt: toNum(live.freshestUpdateAt, 0) || null,
          unrealizedPnlUsd: Number(unrealizedPnlUsd.toFixed(2)),
          totalPnlUsd: Number(totalPnlUsd.toFixed(2)),
        },
        concentrationPct,
      };
    });
    return wallets;
  }

  writeSnapshot(filePath, payload) {
    ensureDir(path.dirname(filePath));
    writeJsonAtomic(filePath, payload);
  }

  capture({ walletRows = [], positions = [], nowMs = Date.now(), force = false }) {
    const hourlyKey = bucketHourKey(nowMs);
    const dailyKey = bucketDayKey(nowMs);
    const hourlyPath = path.join(this.hourlyDir, `${hourlyKey}.json`);
    const dailyPath = path.join(this.dailyDir, `${dailyKey}.json`);
    const needsHourly = force || this.status.lastHourlyKey !== hourlyKey || !fs.existsSync(hourlyPath);
    const needsDaily = force || this.status.lastDailyKey !== dailyKey || !fs.existsSync(dailyPath);

    if (!needsHourly && !needsDaily) {
      this.status.lastCaptureAt = nowMs;
      writeJsonAtomic(this.statusPath, this.status);
      return this.getStatus();
    }

    const wallets = this.buildWalletSnapshotRows({ walletRows, positions, nowMs });
    const walletCount = Object.keys(wallets).length;

    if (needsHourly) {
      this.writeSnapshot(hourlyPath, {
        bucket: "hourly",
        bucketKey: hourlyKey,
        generatedAt: nowMs,
        walletCount,
        wallets,
      });
      this.status.lastHourlyKey = hourlyKey;
    }

    if (needsDaily) {
      this.writeSnapshot(dailyPath, {
        bucket: "daily",
        bucketKey: dailyKey,
        generatedAt: nowMs,
        walletCount,
        wallets,
      });
      this.status.lastDailyKey = dailyKey;
    }

    trimOldFiles(this.hourlyDir, this.retentionHourly);
    trimOldFiles(this.dailyDir, this.retentionDaily);
    this.status.lastCaptureAt = nowMs;
    this.status.lastWalletCount = walletCount;
    this.refreshCounts();
    writeJsonAtomic(this.statusPath, this.status);
    return this.getStatus();
  }

  getWalletHistorySummary(wallet) {
    const target = String(wallet || "").trim();
    if (!target) {
      return {
        wallet: "",
        hourlySnapshots: 0,
        dailySnapshots: 0,
        firstSnapshotAt: null,
        lastSnapshotAt: null,
      };
    }
    const summary = {
      wallet: target,
      hourlySnapshots: 0,
      dailySnapshots: 0,
      firstSnapshotAt: null,
      lastSnapshotAt: null,
    };
    const scanDir = (dirPath, bucket) => {
      if (!fs.existsSync(dirPath)) return;
      const files = fs
        .readdirSync(dirPath)
        .filter((name) => name.endsWith(".json"))
        .sort();
      files.forEach((name) => {
        const payload = readJson(path.join(dirPath, name), null);
        const row =
          payload &&
          payload.wallets &&
          typeof payload.wallets === "object" &&
          payload.wallets[target]
            ? payload.wallets[target]
            : null;
        if (!row) return;
        if (bucket === "hourly") summary.hourlySnapshots += 1;
        else summary.dailySnapshots += 1;
        const at = toNum(payload.generatedAt, 0);
        if (at > 0) {
          summary.firstSnapshotAt =
            summary.firstSnapshotAt === null ? at : Math.min(summary.firstSnapshotAt, at);
          summary.lastSnapshotAt =
            summary.lastSnapshotAt === null ? at : Math.max(summary.lastSnapshotAt, at);
        }
      });
    };
    scanDir(this.hourlyDir, "hourly");
    scanDir(this.dailyDir, "daily");
    return summary;
  }

  getStatus() {
    return {
      enabled: true,
      lastCaptureAt: Number(this.status.lastCaptureAt || 0) || null,
      lastHourlyKey: this.status.lastHourlyKey || null,
      lastDailyKey: this.status.lastDailyKey || null,
      hourlyFiles: Number(this.status.hourlyFiles || 0),
      dailyFiles: Number(this.status.dailyFiles || 0),
      lastWalletCount: Number(this.status.lastWalletCount || 0),
    };
  }

  stop() {
    writeJsonAtomic(this.statusPath, this.status);
  }
}

module.exports = {
  WalletMetricsHistoryStore,
};
