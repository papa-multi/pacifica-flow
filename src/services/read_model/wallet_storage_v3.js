"use strict";

const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const zlib = require("zlib");

const { ensureDir, readJson, writeJsonAtomic } = require("../pipeline/utils");

const DEFAULT_ROOT = path.resolve(
  process.env.PACIFICA_WALLET_EXPLORER_V3_DIR ||
    path.join(__dirname, "../../..", "data", "wallet_explorer_v3")
);
const DEFAULT_SHARD_COUNT = Math.max(16, Number(process.env.PACIFICA_WALLET_EXPLORER_V3_SHARDS || 32));
const MANIFEST_PATH = path.join(DEFAULT_ROOT, "manifest.json");
const SHARDS_DIR = path.join(DEFAULT_ROOT, "shards");
const DEFAULT_ISOLATED_DIR = path.resolve(
  process.env.PACIFICA_WALLET_EXPLORER_ISOLATED_DIR ||
    path.join(__dirname, "../../..", "data", "wallet_explorer_v2", "isolated")
);
const DEFAULT_PENDING_DATASET_PATH = path.resolve(
  process.env.PACIFICA_WALLET_EXPLORER_PENDING_DATASET_PATH ||
    path.join(__dirname, "../../..", "data", "wallet_explorer_v2", "pending_wallet_review_v3.json")
);
const PENDING_OVERLAY_ENABLED = String(
  process.env.PACIFICA_WALLET_EXPLORER_PENDING_OVERLAY_ENABLED || "false"
)
  .trim()
  .toLowerCase() === "true";

const cache = {
  manifestMtimeMs: 0,
  manifest: null,
  snapshotKey: null,
  snapshot: null,
  shardCache: new Map(),
  pendingDatasetMtimeMs: 0,
  pendingDataset: null,
};

const V3_SCHEMA = [
  "wallet",
  "walletRecordId",
  "createdAt",
  "updatedAt",
  "trades",
  "volumeUsdRaw",
  "volumeUsd",
  "pnlUsd",
  "unrealizedPnlUsd",
  "totalPnlUsd",
  "totalWins",
  "totalLosses",
  "winRate",
  "openPositions",
  "drawdownPct",
  "returnPct",
  "accountEquityUsd",
  "firstTrade",
  "lastTrade",
  "lastActivity",
  "lastOpenedAt",
  "exposureUsd",
  "feesPaidUsd",
  "feesUsd",
  "netFeesUsd",
  "feeRebatesUsd",
  "revenueUsd",
  "copyabilityScore",
  "consistencyScore",
  "momentumScore",
  "convictionScore",
  "copyCandidateScore",
  "symbolBreakdown",
  "d24",
  "d7",
  "d30",
  "all",
  "completeness",
  "searchText",
  "symbols",
  "lifecycleStage",
  "status",
];

function toNum(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function normalizeWallet(value) {
  return String(value || "").trim();
}

function loadIsolatedOpenedAt(wallet) {
  const normalizedWallet = normalizeWallet(wallet);
  if (!normalizedWallet) return null;
  const filePath = path.join(DEFAULT_ISOLATED_DIR, normalizedWallet, "wallet_history.json");
  try {
    if (!fs.existsSync(filePath)) return null;
    const payload = readJson(filePath, null);
    const trades = Array.isArray(payload && payload.trades) ? payload.trades : [];
    let openedAt = 0;
    for (const row of trades) {
      if (!row || typeof row !== "object") continue;
      const side = String(row.side || "").trim().toLowerCase();
      if (!side.startsWith("open_")) continue;
      const ts = toNum(row.timestamp || row.created_at || row.createdAt || 0, 0);
      if (ts > openedAt) openedAt = ts;
    }
    return openedAt > 0 ? openedAt : null;
  } catch (_error) {
    return null;
  }
}

function readGzJson(filePath, fallback = null) {
  try {
    if (!fs.existsSync(filePath)) return fallback;
    const buf = fs.readFileSync(filePath);
    const text = filePath.endsWith(".gz") ? zlib.gunzipSync(buf).toString("utf8") : buf.toString("utf8");
    return JSON.parse(text);
  } catch (_error) {
    return fallback;
  }
}

function writeGzJson(filePath, payload) {
  ensureDir(path.dirname(filePath));
  const tmpPath = `${filePath}.${process.pid}.${Date.now()}.${Math.random().toString(16).slice(2)}.tmp`;
  const json = JSON.stringify(payload);
  const gz = zlib.gzipSync(Buffer.from(json, "utf8"), { level: 9 });
  fs.writeFileSync(tmpPath, gz);
  fs.renameSync(tmpPath, filePath);
}

function readMtimeMs(filePath) {
  try {
    return Math.trunc(fs.statSync(filePath).mtimeMs || 0);
  } catch {
    return 0;
  }
}

function loadPendingDataset() {
  if (!PENDING_OVERLAY_ENABLED) {
    cache.pendingDatasetMtimeMs = 0;
    cache.pendingDataset = null;
    return null;
  }
  const mtimeMs = readMtimeMs(DEFAULT_PENDING_DATASET_PATH);
  if (
    cache.pendingDataset &&
    cache.pendingDatasetMtimeMs > 0 &&
    cache.pendingDatasetMtimeMs === mtimeMs
  ) {
    return cache.pendingDataset;
  }
  const payload = readJson(DEFAULT_PENDING_DATASET_PATH, null);
  if (!payload || typeof payload !== "object" || !Array.isArray(payload.rows) || !payload.rows.length) {
    cache.pendingDatasetMtimeMs = mtimeMs;
    cache.pendingDataset = null;
    return null;
  }
  const rows = payload.rows
    .filter((row) => row && typeof row === "object" && normalizeWallet(row.wallet))
    .map((row) => ({ ...row }));
  const snapshot = {
    generatedAt: Number(payload.generatedAt || 0) || 0,
    rows,
    rowsByWallet: new Map(rows.map((row) => [normalizeWallet(row.wallet), row])),
  };
  cache.pendingDatasetMtimeMs = mtimeMs;
  cache.pendingDataset = snapshot;
  return snapshot;
}

function overlayPendingRow(baseRow, pendingRow) {
  const safeBase = baseRow && typeof baseRow === "object" ? baseRow : null;
  const safePending = pendingRow && typeof pendingRow === "object" ? pendingRow : null;
  if (!safeBase || !safePending) return safeBase;
  const pendingBucket = safePending.all && typeof safePending.all === "object" ? safePending.all : null;
  const baseUpdatedAt = toNum(safeBase.updatedAt, 0);
  const baseLastOpenedAt = toNum(safeBase.lastOpenedAt ?? safeBase.lastActivity, 0);
  const baseLastTrade = toNum(safeBase.lastTrade, 0);
  const pendingUpdatedAt = toNum(safePending.updatedAt ?? (pendingBucket && pendingBucket.updatedAt), 0);
  const pendingLastOpenedAt = toNum(
    safePending.lastOpenedAt ??
      safePending.lastActivity ??
      (pendingBucket && (pendingBucket.lastOpenedAt ?? pendingBucket.lastActivity)),
    0
  );
  const pendingLastTrade = toNum(
    safePending.lastTrade ?? (pendingBucket && pendingBucket.lastTrade),
    0
  );
  if (
    pendingUpdatedAt < baseUpdatedAt &&
    pendingLastOpenedAt <= baseLastOpenedAt &&
    pendingLastTrade <= baseLastTrade
  ) {
    return safeBase;
  }
  return decodeV3Record(
    rowToV3Record({
      ...safeBase,
      ...(pendingBucket || {}),
      ...safePending,
    })
  );
}

function shardCountFromManifest(manifest = null) {
  const count = Number(manifest && manifest.shardCount ? manifest.shardCount : DEFAULT_SHARD_COUNT);
  return Math.max(1, Number.isFinite(count) ? count : DEFAULT_SHARD_COUNT);
}

function shardIdForWallet(wallet, shardCount = DEFAULT_SHARD_COUNT) {
  const normalized = normalizeWallet(wallet);
  if (!normalized) return "000";
  const digest = crypto.createHash("sha1").update(normalized).digest();
  const shardIndex = digest.readUInt16BE(0) % Math.max(1, shardCount);
  return String(shardIndex).padStart(3, "0");
}

function rowToV3Record(row = {}) {
  const safe = row && typeof row === "object" ? row : {};
  const lastTradeAt = toNum(
    safe.lastTrade ??
      (safe.all && safe.all.lastTrade) ??
      (safe.d30 && safe.d30.lastTrade) ??
      (safe.d7 && safe.d7.lastTrade) ??
      (safe.d24 && safe.d24.lastTrade),
    0
  ) || null;
  const lastOpenedAt = toNum(
    safe.lastOpenedAt ??
      (safe.lastOpenedPosition && safe.lastOpenedPosition.openedAt) ??
      safe.positionOpenedAt ??
      safe.lastPositionOpenedAt ??
      (safe.d24 && safe.d24.lastOpenedAt) ??
      (safe.d7 && safe.d7.lastOpenedAt) ??
      (safe.d30 && safe.d30.lastOpenedAt) ??
      (safe.all && safe.all.lastOpenedAt) ??
      loadIsolatedOpenedAt(safe.wallet),
    0
  ) || null;
  const normalized = {
    wallet: normalizeWallet(safe.wallet),
    walletRecordId: String(safe.walletRecordId || "").trim() || null,
    createdAt: toNum(safe.createdAt, 0) || null,
    updatedAt: toNum(safe.updatedAt, 0) || null,
    trades: toNum(safe.trades, 0),
    volumeUsdRaw: toNum(safe.volumeUsdRaw ?? safe.volumeUsd, 0),
    volumeUsd: toNum(safe.volumeUsd ?? safe.volumeUsdRaw, 0),
    pnlUsd: toNum(safe.pnlUsd, 0),
    unrealizedPnlUsd: toNum(safe.unrealizedPnlUsd, 0),
    totalPnlUsd: toNum(safe.totalPnlUsd, toNum(safe.pnlUsd, 0) + toNum(safe.unrealizedPnlUsd, 0)),
    totalWins: toNum(safe.totalWins, 0),
    totalLosses: toNum(safe.totalLosses, 0),
    winRate: toNum(safe.winRate, 0),
    openPositions: toNum(safe.openPositions, 0),
    drawdownPct: toNum(safe.drawdownPct, 0),
    returnPct: toNum(safe.returnPct, 0),
    accountEquityUsd: toNum(safe.accountEquityUsd, 0),
    firstTrade: toNum(safe.firstTrade, 0) || null,
    lastTrade: lastTradeAt,
    lastActivity: lastTradeAt || lastOpenedAt,
    lastOpenedAt,
    exposureUsd: toNum(safe.exposureUsd, 0),
    feesPaidUsd: toNum(safe.feesPaidUsd ?? safe.feesUsd, 0),
    feesUsd: toNum(safe.feesUsd ?? safe.feesPaidUsd, 0),
    netFeesUsd: toNum(safe.netFeesUsd, 0),
    feeRebatesUsd: toNum(safe.feeRebatesUsd, 0),
    revenueUsd: toNum(safe.revenueUsd, 0),
    copyabilityScore: toNum(safe.copyabilityScore, 0),
    consistencyScore: toNum(safe.consistencyScore, 0),
    momentumScore: toNum(safe.momentumScore, 0),
    convictionScore: toNum(safe.convictionScore, 0),
    copyCandidateScore: toNum(safe.copyCandidateScore, 0),
    symbolBreakdown: Array.isArray(safe.symbolBreakdown)
      ? safe.symbolBreakdown
          .map((item) => ({
            symbol: String(item && item.symbol ? item.symbol : "").trim().toUpperCase(),
            volumeUsd: toNum(item && (item.volumeUsd ?? item.volume_usd), 0),
          }))
          .filter((item) => item.symbol)
      : [],
    d24: safe.d24 && typeof safe.d24 === "object" ? safe.d24 : null,
    d7: safe.d7 && typeof safe.d7 === "object" ? safe.d7 : null,
    d30: safe.d30 && typeof safe.d30 === "object" ? safe.d30 : null,
    all: safe.all && typeof safe.all === "object" ? safe.all : null,
    completeness: safe.completeness && typeof safe.completeness === "object" ? safe.completeness : null,
    searchText: String(safe.searchText || "").toLowerCase(),
    symbols: Array.isArray(safe.symbols) ? safe.symbols.slice() : [],
    lifecycleStage: safe.lifecycleStage || safe.lifecycle || null,
    status: safe.status && typeof safe.status === "object" ? safe.status : null,
  };
  return normalized;
}

function decodeV3Record(record = {}) {
  const safe = record && typeof record === "object" ? record : {};
  const lastTradeAt = toNum(safe.lastTrade ?? safe.lastActivity, 0) || null;
  const lastOpenedAt = toNum(safe.lastOpenedAt, 0) || null;
  const lastActivityAt = lastTradeAt || lastOpenedAt;
  return {
    ...safe,
    wallet: normalizeWallet(safe.wallet),
    createdAt: toNum(safe.createdAt, 0) || null,
    updatedAt: toNum(safe.updatedAt, 0) || null,
    trades: toNum(safe.trades, 0),
    volumeUsdRaw: toNum(safe.volumeUsdRaw ?? safe.volumeUsd, 0),
    volumeUsd: toNum(safe.volumeUsd ?? safe.volumeUsdRaw, 0),
    pnlUsd: toNum(safe.pnlUsd, 0),
    unrealizedPnlUsd: toNum(safe.unrealizedPnlUsd, 0),
    totalPnlUsd: toNum(
      safe.totalPnlUsd,
      toNum(safe.pnlUsd, 0) + toNum(safe.unrealizedPnlUsd, 0)
    ),
    totalWins: toNum(safe.totalWins, 0),
    totalLosses: toNum(safe.totalLosses, 0),
    winRate: toNum(safe.winRate, 0),
    openPositions: toNum(safe.openPositions, 0),
    drawdownPct: toNum(safe.drawdownPct, 0),
    returnPct: toNum(safe.returnPct, 0),
    accountEquityUsd: toNum(safe.accountEquityUsd, 0),
    firstTrade: toNum(safe.firstTrade, 0) || null,
    lastTrade: lastTradeAt,
    lastOpenedAt,
    lastActivity: lastActivityAt,
    lastActivityAt,
    lastActiveAt: lastActivityAt,
    exposureUsd: toNum(safe.exposureUsd, 0),
    feesPaidUsd: toNum(safe.feesPaidUsd ?? safe.feesUsd, 0),
    feesUsd: toNum(safe.feesUsd ?? safe.feesPaidUsd, 0),
    netFeesUsd: toNum(safe.netFeesUsd, 0),
    feeRebatesUsd: toNum(safe.feeRebatesUsd, 0),
    revenueUsd: toNum(safe.revenueUsd, 0),
    searchText: String(safe.searchText || "").toLowerCase(),
    symbols: Array.isArray(safe.symbols) ? safe.symbols.slice() : [],
    symbolBreakdown: Array.isArray(safe.symbolBreakdown)
      ? safe.symbolBreakdown.map((item) => ({
          symbol: String(item && item.symbol ? item.symbol : "").trim().toUpperCase(),
          volumeUsd: toNum(item && (item.volumeUsd ?? item.volume_usd), 0),
        }))
      : [],
  };
}

function manifestPath() {
  return MANIFEST_PATH;
}

function shardPath(shardId) {
  return path.join(SHARDS_DIR, `shard_${String(shardId).padStart(3, "0")}.json.gz`);
}

function ensureManifest() {
  const mtimeMs = readMtimeMs(MANIFEST_PATH);
  if (
    cache.manifest &&
    cache.manifestMtimeMs > 0 &&
    cache.manifestMtimeMs === mtimeMs
  ) {
    return cache.manifest;
  }
  const manifest = readGzJson(MANIFEST_PATH, null) || null;
  cache.manifestMtimeMs = mtimeMs;
  cache.manifest = manifest;
  return manifest;
}

function loadShard(shardId) {
  const manifest = ensureManifest();
  const shardCount = shardCountFromManifest(manifest);
  const normalizedShardId = String(shardId).padStart(3, "0");
  const filePath = shardPath(normalizedShardId);
  const mtimeMs = readMtimeMs(filePath);
  const cached = cache.shardCache.get(normalizedShardId);
  if (cached && cached.mtimeMs === mtimeMs) return cached.value;
  const payload = readGzJson(filePath, null);
  if (!payload || typeof payload !== "object" || !Array.isArray(payload.rows)) {
    cache.shardCache.set(normalizedShardId, { mtimeMs, value: null });
    return null;
  }
  const rows = payload.rows.map((row) => decodeV3Record(row));
  const snapshot = {
    version: payload.version || 1,
    generatedAt: Number(payload.generatedAt || manifest?.generatedAt || Date.now()) || Date.now(),
    shardId: normalizedShardId,
    shardCount,
    rows,
    preparedRows: rows,
    preparedRowsByWallet: new Map(rows.map((row) => [normalizeWallet(row.wallet), row])),
    summaryCounts: payload.summaryCounts || null,
    availableSymbols: Array.isArray(payload.availableSymbols) ? payload.availableSymbols : [],
  };
  cache.shardCache.set(normalizedShardId, { mtimeMs, value: snapshot });
  return snapshot;
}

function loadWalletExplorerV3WalletRecord(wallet) {
  const normalizedWallet = normalizeWallet(wallet);
  if (!normalizedWallet) return null;
  const manifest = ensureManifest();
  if (!manifest) return null;
  const shardCount = shardCountFromManifest(manifest);
  const shardId = shardIdForWallet(normalizedWallet, shardCount);
  const shard = loadShard(shardId);
  if (!shard || !Array.isArray(shard.rows)) return null;
  const baseRow = shard.preparedRowsByWallet.get(normalizedWallet) || null;
  if (!baseRow) return null;
  const pendingDataset = loadPendingDataset();
  const pendingRow =
    pendingDataset && pendingDataset.rowsByWallet
      ? pendingDataset.rowsByWallet.get(normalizedWallet)
      : null;
  return overlayPendingRow(baseRow, pendingRow) || baseRow;
}

function loadWalletExplorerV3Snapshot() {
  const manifest = ensureManifest();
  if (!manifest) return null;
  const shardCount = shardCountFromManifest(manifest);
  const pendingDataset = loadPendingDataset();
  const snapshotKey = `${manifest.generatedAt || 0}:${shardCount}:${pendingDataset ? pendingDataset.generatedAt || 0 : 0}:${cache.pendingDatasetMtimeMs || 0}`;
  if (cache.snapshot && cache.snapshotKey === snapshotKey) {
    return cache.snapshot;
  }
  const rows = [];
  const shards = [];
  for (let shardIndex = 0; shardIndex < shardCount; shardIndex += 1) {
    const shardId = String(shardIndex).padStart(3, "0");
    const shard = loadShard(shardId);
    if (!shard) continue;
    shards.push(shardId);
    rows.push(...shard.rows);
  }
  if (!rows.length) return null;
  const preparedRows = rows.map((row) => {
    const pendingRow =
      pendingDataset && pendingDataset.rowsByWallet
        ? pendingDataset.rowsByWallet.get(normalizeWallet(row && row.wallet))
        : null;
    const overlaid = overlayPendingRow(row, pendingRow) || row;
    return {
      ...overlaid,
      symbols: Array.isArray(overlaid && overlaid.symbols) ? overlaid.symbols.slice() : [],
      symbolBreakdown: Array.isArray(overlaid && overlaid.symbolBreakdown)
        ? overlaid.symbolBreakdown.map((item) => ({
            symbol: String(item && item.symbol ? item.symbol : "").trim().toUpperCase(),
            volumeUsd: toNum(item && (item.volumeUsd ?? item.volume_usd), 0),
          }))
        : [],
    };
  });
  const snapshot = {
    generatedAt: Math.max(
      Number(manifest.generatedAt || Date.now()) || Date.now(),
      Number(pendingDataset && pendingDataset.generatedAt ? pendingDataset.generatedAt : 0) || 0
    ),
    mode: "wallet_explorer_v3_snapshot",
    shardCount,
    shards,
    rows: preparedRows,
    preparedRows,
    preparedRowsByWallet: new Map(preparedRows.map((row) => [normalizeWallet(row.wallet), row])),
    counts: manifest.counts || null,
    summaryCounts: manifest.counts || null,
    availableSymbols: Array.isArray(manifest.availableSymbols) ? manifest.availableSymbols : [],
    manifest,
  };
  cache.snapshotKey = snapshotKey;
  cache.snapshot = snapshot;
  return snapshot;
}

function buildWalletExplorerV3Manifest({ sourcePath, rows, counts = null, availableSymbols = null } = {}) {
  const shardCount = DEFAULT_SHARD_COUNT;
  const manifest = {
    version: 1,
    generatedAt: Date.now(),
    sourcePath: sourcePath || null,
    shardCount,
    rowCount: Array.isArray(rows) ? rows.length : 0,
    counts: counts && typeof counts === "object" ? counts : {},
    availableSymbols: Array.isArray(availableSymbols) ? availableSymbols.slice() : [],
    shards: [],
  };
  const symbolSet = new Set();
  const buckets = new Map();
  (Array.isArray(rows) ? rows : []).forEach((row) => {
    const normalized = rowToV3Record(row);
    const shardId = shardIdForWallet(normalized.wallet, shardCount);
    if (!buckets.has(shardId)) buckets.set(shardId, []);
    buckets.get(shardId).push(normalized);
    (normalized.symbols || []).forEach((symbol) => symbolSet.add(symbol));
  });
  for (let i = 0; i < shardCount; i += 1) {
    const shardId = String(i).padStart(3, "0");
    const shardRows = buckets.get(shardId) || [];
    manifest.shards.push({
      shardId,
      file: `shards/shard_${shardId}.json.gz`,
      rowCount: shardRows.length,
    });
  }
  if (!manifest.availableSymbols.length) {
    manifest.availableSymbols = Array.from(symbolSet).sort();
  }
  return { manifest, buckets };
}

function writeWalletExplorerV3Snapshot({
  sourcePath,
  rows,
  counts = null,
  availableSymbols = null,
  manifestPath: targetManifestPath = MANIFEST_PATH,
} = {}) {
  const { manifest, buckets } = buildWalletExplorerV3Manifest({ sourcePath, rows, counts, availableSymbols });
  ensureDir(path.dirname(targetManifestPath));
  ensureDir(SHARDS_DIR);
  for (const [shardId, shardRows] of buckets.entries()) {
    const shardPayload = {
      version: 1,
      generatedAt: manifest.generatedAt,
      shardId,
      rows: shardRows,
      summaryCounts: manifest.counts || null,
      availableSymbols: manifest.availableSymbols,
    };
    writeGzJson(shardPath(shardId), shardPayload);
  }
  fs.writeFileSync(targetManifestPath, JSON.stringify(manifest, null, 2));
  cache.manifest = manifest;
  cache.manifestMtimeMs = readMtimeMs(targetManifestPath);
  cache.snapshot = null;
  cache.snapshotKey = null;
  cache.shardCache.clear();
  return manifest;
}

module.exports = {
  DEFAULT_ROOT,
  MANIFEST_PATH,
  SHARDS_DIR,
  V3_SCHEMA,
  buildWalletExplorerV3Manifest,
  decodeV3Record,
  loadWalletExplorerV3Snapshot,
  loadWalletExplorerV3WalletRecord,
  shardIdForWallet,
  writeWalletExplorerV3Snapshot,
};
