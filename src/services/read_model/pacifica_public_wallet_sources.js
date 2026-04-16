"use strict";

const fs = require("fs");
const path = require("path");
const https = require("https");

const DEFAULT_BASE_URL = String(
  process.env.PACIFICA_PUBLIC_BASE_URL || "https://app.pacifica.fi"
).replace(/\/+$/, "");
const DEFAULT_TIMEOUT_MS = Math.max(
  3000,
  Number(process.env.PACIFICA_PUBLIC_SOURCE_TIMEOUT_MS || 15000)
);
const DEFAULT_DETAIL_CACHE_MAX_AGE_MS = Math.max(
  5 * 60 * 1000,
  Number(process.env.PACIFICA_PUBLIC_DETAIL_CACHE_MAX_AGE_MS || 24 * 60 * 60 * 1000)
);
const DEFAULT_LIMIT = 25000;
const SNAPSHOT_PATH = path.resolve(
  process.env.PACIFICA_PUBLIC_WALLET_SOURCES_PATH ||
    path.join(__dirname, "../../..", "data", "wallet_explorer_v3", "public_endpoint_sources.json")
);
const DETAIL_CACHE_PATH = path.resolve(
  process.env.PACIFICA_PUBLIC_WALLET_DETAIL_CACHE_PATH ||
    path.join(__dirname, "../../..", "data", "wallet_explorer_v3", "public_endpoint_wallet_details.json")
);

function sortTimelineRows(rows = []) {
  return (Array.isArray(rows) ? rows : [])
    .filter((row) => row && typeof row === "object")
    .slice()
    .sort((left, right) => toNum(left && left.timestamp, 0) - toNum(right && right.timestamp, 0));
}

function computePortfolioTimelineMetrics(payload) {
  const rows = sortTimelineRows(payload && payload.data);
  if (!rows.length) {
    return {
      firstSeenAt: null,
      lastSeenAt: null,
      sampleCount: 0,
      pnlAllTime: 0,
      returnPct: 0,
      drawdownPct: 0,
      accountEquityUsd: 0,
    };
  }
  const firstRow = rows[0] || {};
  const lastRow = rows[rows.length - 1] || {};
  const firstSeenAt = toNum(firstRow.timestamp, 0) || null;
  const lastSeenAt = toNum(lastRow.timestamp, 0) || null;
  const firstPnl = toNum(firstRow.pnl, 0);
  const lastPnl = toNum(lastRow.pnl, 0);
  const firstEquity = toNum(firstRow.account_equity, 0);
  const lastEquity = toNum(lastRow.account_equity, 0);
  const pnlAllTime = rows.length >= 2 ? Number((lastPnl - firstPnl).toFixed(6)) : 0;
  const returnPct =
    rows.length >= 2 && firstEquity > 0 ? Number(((pnlAllTime / firstEquity) * 100).toFixed(6)) : 0;
  let peakEquity = firstEquity;
  let maxDrawdown = 0;
  for (let index = 1; index < rows.length; index += 1) {
    const equity = toNum(rows[index] && rows[index].account_equity, 0);
    if (equity > peakEquity) peakEquity = equity;
    const drawdown = peakEquity > 0 ? ((peakEquity - equity) / peakEquity) * 100 : 0;
    if (drawdown > maxDrawdown) maxDrawdown = drawdown;
  }
  return {
    firstSeenAt,
    lastSeenAt,
    sampleCount: rows.length,
    pnlAllTime,
    returnPct: Number(returnPct.toFixed(6)),
    drawdownPct: Number(maxDrawdown.toFixed(6)),
    accountEquityUsd: Number(lastEquity.toFixed(6)),
  };
}

function toNum(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function normalizeWallet(value) {
  return String(value || "").trim();
}

function readJson(filePath, fallback = null) {
  try {
    if (!fs.existsSync(filePath)) return fallback;
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return fallback;
  }
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function writeJsonAtomic(filePath, payload) {
  ensureDir(path.dirname(filePath));
  const tmpPath = `${filePath}.${process.pid}.${Date.now()}.${Math.random().toString(16).slice(2)}.tmp`;
  fs.writeFileSync(tmpPath, JSON.stringify(payload, null, 2));
  fs.renameSync(tmpPath, filePath);
}

function fetchJson(url, timeoutMs = DEFAULT_TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    const req = https.get(
      url,
      {
        headers: {
          accept: "application/json",
          "user-agent": "pacifica-flow/1.0",
        },
      },
      (res) => {
        let body = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => {
          body += chunk;
        });
        res.on("end", () => {
          if (res.statusCode && res.statusCode >= 400) {
            reject(new Error(`HTTP ${res.statusCode}: ${body.slice(0, 400)}`));
            return;
          }
          try {
            resolve(JSON.parse(body));
          } catch (error) {
            reject(error);
          }
        });
      }
    );
    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error(`timeout after ${timeoutMs}ms`));
    });
    req.on("error", reject);
  });
}

function buildLeaderboardSnapshot(payload, fetchedAt = Date.now()) {
  const rows = Array.isArray(payload && payload.data) ? payload.data : [];
  const normalizedRows = rows
    .map((row, index) => {
      const wallet = normalizeWallet(row && row.address);
      if (!wallet) return null;
      return {
        wallet,
        username: String((row && row.username) || "").trim() || null,
        pnlAllTime: toNum(row && row.pnl_all_time, 0),
        pnl30d: toNum(row && row.pnl_30d, 0),
        pnl7d: toNum(row && row.pnl_7d, 0),
        pnl1d: toNum(row && row.pnl_1d, 0),
        volumeAllTime: toNum(row && row.volume_all_time, 0),
        volume30d: toNum(row && row.volume_30d, 0),
        volume7d: toNum(row && row.volume_7d, 0),
        volume1d: toNum(row && row.volume_1d, 0),
        equityCurrent: toNum(row && row.equity_current, 0),
        oiCurrent: toNum(row && row.oi_current, 0),
        rankAllTime: index + 1,
      };
    })
    .filter(Boolean);
  return {
    source: "pacifica_public_endpoints",
    generatedAt: fetchedAt,
    leaderboard: {
      source: "https://app.pacifica.fi/api/v1/leaderboard?limit=25000",
      walletCount: normalizedRows.length,
      rows: normalizedRows,
    },
    portfolio: {
      source: "https://app.pacifica.fi/api/v1/portfolio",
      mode: "account_scoped_detail",
      supportedTimeRanges: ["1d", "7d", "14d", "30d", "all"],
      note: "Portfolio is an account-specific detail source, not a global wallet list endpoint.",
    },
  };
}

async function fetchPacificaPublicWalletSources({ limit = DEFAULT_LIMIT } = {}) {
  const payload = await fetchJson(
    `${DEFAULT_BASE_URL}/api/v1/leaderboard?limit=${encodeURIComponent(String(limit))}`
  );
  return buildLeaderboardSnapshot(payload, Date.now());
}

function loadPacificaPublicWalletSourcesSnapshot() {
  const payload = readJson(SNAPSHOT_PATH, null);
  if (!payload || typeof payload !== "object") return null;
  return payload;
}

function loadPacificaPublicWalletDetailCache() {
  const payload = readJson(DETAIL_CACHE_PATH, null);
  if (!payload || typeof payload !== "object") return null;
  if (!payload.wallets || typeof payload.wallets !== "object") return null;
  return payload;
}

function writePacificaPublicWalletSourcesSnapshot(snapshot) {
  if (!snapshot || typeof snapshot !== "object") return null;
  writeJsonAtomic(SNAPSHOT_PATH, snapshot);
  return SNAPSHOT_PATH;
}

function writePacificaPublicWalletDetailCache(snapshot) {
  if (!snapshot || typeof snapshot !== "object") return null;
  writeJsonAtomic(DETAIL_CACHE_PATH, snapshot);
  return DETAIL_CACHE_PATH;
}

function buildLeaderboardIndex(snapshot) {
  const rows =
    snapshot &&
    snapshot.leaderboard &&
    Array.isArray(snapshot.leaderboard.rows)
      ? snapshot.leaderboard.rows
      : [];
  return new Map(rows.map((row) => [normalizeWallet(row.wallet), row]));
}

async function fetchPacificaPublicWalletDetail(wallet, { force = false } = {}) {
  const normalizedWallet = normalizeWallet(wallet);
  if (!normalizedWallet) return null;
  const existingCache =
    loadPacificaPublicWalletDetailCache() || {
      source: "pacifica_public_endpoints",
      generatedAt: 0,
      wallets: {},
    };
  const existingWallets =
    existingCache.wallets && typeof existingCache.wallets === "object" ? existingCache.wallets : {};
  const existingEntry =
    existingWallets[normalizedWallet] && typeof existingWallets[normalizedWallet] === "object"
      ? existingWallets[normalizedWallet]
      : null;
  const fetchedAt = Number(existingEntry && existingEntry.fetchedAt ? existingEntry.fetchedAt : 0) || 0;
  const cacheAgeMs = Math.max(0, Date.now() - fetchedAt);
  const hasMetrics =
    existingEntry &&
    Number.isFinite(Number(existingEntry.volumeAllTime)) &&
    Number.isFinite(Number(existingEntry.pnlAllTime)) &&
    Number.isFinite(Number(existingEntry.drawdownPct)) &&
    Number.isFinite(Number(existingEntry.returnPct)) &&
    Number.isFinite(Number(existingEntry.accountEquityUsd)) &&
    Number.isFinite(Number(existingEntry.openPositions));
  if (!force && hasMetrics && cacheAgeMs <= DEFAULT_DETAIL_CACHE_MAX_AGE_MS) {
    return existingEntry;
  }
  try {
    const [timelinePayload, volumePayload, accountPayload] = await Promise.all([
      fetchJson(
        `${DEFAULT_BASE_URL}/api/v1/portfolio?account=${encodeURIComponent(
          normalizedWallet
        )}&time_range=all`
      ),
      fetchJson(
        `${DEFAULT_BASE_URL}/api/v1/portfolio/volume?account=${encodeURIComponent(normalizedWallet)}`
      ),
      fetchJson(`${DEFAULT_BASE_URL}/api/v1/account?account=${encodeURIComponent(normalizedWallet)}`),
    ]);
    const timeline = computePortfolioTimelineMetrics(timelinePayload);
    const volumeData =
      volumePayload && volumePayload.data && typeof volumePayload.data === "object"
        ? volumePayload.data
        : {};
    const accountData =
      accountPayload && accountPayload.data && typeof accountPayload.data === "object"
        ? accountPayload.data
        : {};
    const nextEntry = {
      wallet: normalizedWallet,
      firstSeenAt: Number(timeline.firstSeenAt || 0) || null,
      lastSeenAt: Number(timeline.lastSeenAt || 0) || null,
      sampleCount: Number(timeline.sampleCount || 0) || 0,
      pnlAllTime: Number(timeline.pnlAllTime || 0),
      drawdownPct: Number(timeline.drawdownPct || 0),
      returnPct: Number(timeline.returnPct || 0),
      accountEquityUsd: Number(accountData.account_equity || timeline.accountEquityUsd || 0),
      openPositions: Math.max(0, Number(accountData.positions_count || 0)),
      volume1d: Number(volumeData.volume_1d || 0),
      volume7d: Number(volumeData.volume_7d || 0),
      volume14d: Number(volumeData.volume_14d || 0),
      volume30d: Number(volumeData.volume_30d || 0),
      volumeAllTime: Number(volumeData.volume_all_time || 0),
      accountUpdatedAt: Number(accountData.updated_at || 0) || null,
      fetchedAt: Date.now(),
      source: "portfolio_timeline_volume_account",
    };
    const nextPayload = {
      source: "pacifica_public_endpoints",
      generatedAt: Date.now(),
      walletCount: Object.keys(existingWallets).length + (existingEntry ? 0 : 1),
      wallets: {
        ...existingWallets,
        [normalizedWallet]: nextEntry,
      },
    };
    writePacificaPublicWalletDetailCache(nextPayload);
    return nextEntry;
  } catch (_error) {
    return existingEntry || null;
  }
}

module.exports = {
  DETAIL_CACHE_PATH,
  SNAPSHOT_PATH,
  buildLeaderboardIndex,
  computePortfolioTimelineMetrics,
  fetchPacificaPublicWalletDetail,
  fetchPacificaPublicWalletSources,
  loadPacificaPublicWalletDetailCache,
  loadPacificaPublicWalletSourcesSnapshot,
  writePacificaPublicWalletDetailCache,
  writePacificaPublicWalletSourcesSnapshot,
};
