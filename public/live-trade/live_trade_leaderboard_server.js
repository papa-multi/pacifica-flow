#!/usr/bin/env node
"use strict";

const fs = require("fs");
const http = require("http");
const path = require("path");
const url = require("url");

const HOST = String(process.env.PACIFICA_LIVE_TRADE_HOST || "0.0.0.0").trim();
const PORT = Math.max(1, Number(process.env.PACIFICA_LIVE_TRADE_PORT || 3331));
const DEFAULT_BASE_DIR = path.resolve(
  process.env.PACIFICA_LIVE_TRADE_BASE_DIR ||
    path.join(__dirname, "..", "data", "live_trade_leaderboard")
);
const FALLBACK_LIVE_POSITIONS_DIR = path.join(__dirname, "..", "data", "live_positions");
const BASE_DIR = fs.existsSync(DEFAULT_BASE_DIR)
  ? DEFAULT_BASE_DIR
  : FALLBACK_LIVE_POSITIONS_DIR;
const LIVE_UI_DIR = path.join(__dirname, "..", "public", "live-trade");
const POSITIONS_DIR = fs.existsSync(path.join(BASE_DIR, "live_positions"))
  ? path.join(BASE_DIR, "live_positions")
  : BASE_DIR;
const SOURCES_PATH = path.join(BASE_DIR, "leaderboard_sources.json");
const DETAIL_CACHE_PATH = path.join(BASE_DIR, "public_endpoint_wallet_details.json");
const POSITIVE_WALLET_ROWS_SNAPSHOT_PATH = path.join(
  __dirname,
  "..",
  "data",
  "live_positions",
  "positive_wallet_rows_snapshot.json"
);
const NEW_EVENT_MAX_LAG_MS = Math.max(
  1000,
  Number(process.env.PACIFICA_LIVE_TRADE_NEW_EVENT_MAX_LAG_MS || 15 * 60 * 1000)
);
const SSE_INTERVAL_MS = Math.max(
  100,
  Number(process.env.PACIFICA_LIVE_TRADE_SSE_INTERVAL_MS || 500)
);
const DATA_CACHE_TTL_MS = Math.max(
  100,
  Number(process.env.PACIFICA_LIVE_TRADE_DATA_CACHE_TTL_MS || 500)
);

const dataCache = {
  loadedAt: 0,
  ttlMs: DATA_CACHE_TTL_MS,
  value: null,
};
const streamState = {
  lastBroadcastSignature: "",
};

function readJson(filePath, fallback = null) {
  try {
    if (!fs.existsSync(filePath)) return fallback;
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return fallback;
  }
}

function toNum(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function roundTo(value, decimals = 2) {
  const num = Number(value);
  if (!Number.isFinite(num)) return NaN;
  const factor = 10 ** Math.max(0, Number(decimals) || 0);
  return Math.round(num * factor) / factor;
}

function resolvePositiveCohortPnlUsd(row = {}, detail = {}) {
  const candidates = [
    row && row.pnlAllTime,
    row && row.totalPnlUsd,
    row && row.pnlUsd,
    detail && detail.totalPnlUsd,
    detail && detail.pnlAllTime,
    detail && detail.pnlUsd,
  ];
  for (const candidate of candidates) {
    const num = Number(candidate);
    if (Number.isFinite(num)) return num;
  }
  return NaN;
}

function isPositiveCohortWallet(row = {}, detail = {}) {
  const pnlUsd = resolvePositiveCohortPnlUsd(row, detail);
  if (!Number.isFinite(pnlUsd) || pnlUsd <= 0) return false;
  const accountEquityUsd = Number(
    detail && detail.accountEquityUsd !== undefined ? detail.accountEquityUsd : row.accountEquityUsd
  );
  if (!Number.isFinite(accountEquityUsd) || accountEquityUsd <= 0) return false;
  const returnPct = Number(
    detail && detail.returnPct !== undefined ? detail.returnPct : row.returnPct
  );
  const lifecycleStage = String(
    (detail && (detail.lifecycleStage || detail.lifecycle || detail.status)) ||
      (row && (row.lifecycleStage || row.lifecycle || row.status)) ||
      ""
  )
    .trim()
    .toLowerCase();
  return returnPct > 0 || lifecycleStage === "bootstrap";
}

function normalizeWallet(value) {
  return String(value || "").trim();
}

function tsMs(value) {
  const ts = Number(value);
  if (!Number.isFinite(ts) || ts <= 0) return 0;
  return ts < 1e12 ? ts * 1000 : ts;
}

function shortWallet(wallet) {
  const text = normalizeWallet(wallet);
  if (text.length <= 12) return text;
  return `${text.slice(0, 6)}...${text.slice(-4)}`;
}

function quantile(values = [], p = 0.5) {
  const list = (Array.isArray(values) ? values : [])
    .map((value) => Number(value))
    .filter((value) => Number.isFinite(value))
    .sort((a, b) => a - b);
  if (!list.length) return 0;
  const idx = Math.min(list.length - 1, Math.max(0, Math.round(p * (list.length - 1))));
  return Number(list[idx].toFixed(3));
}

function sendJson(res, statusCode, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store",
  });
  res.end(body);
}

function sendText(res, statusCode, contentType, body, cacheControl = "no-cache") {
  res.writeHead(statusCode, {
    "Content-Type": `${contentType}; charset=utf-8`,
    "Cache-Control": cacheControl,
  });
  res.end(body);
}

function compareNumbers(left, right, dir = "desc") {
  const a = toNum(left, 0);
  const b = toNum(right, 0);
  return dir === "asc" ? a - b : b - a;
}

function compareStrings(left, right, dir = "asc") {
  const a = String(left || "").toLowerCase();
  const b = String(right || "").toLowerCase();
  if (a === b) return 0;
  if (dir === "asc") return a < b ? -1 : 1;
  return a > b ? -1 : 1;
}

function contentTypeForLiveUi(filePath) {
  const ext = path.extname(String(filePath || "")).toLowerCase();
  if (ext === ".html") return "text/html";
  if (ext === ".js") return "application/javascript";
  if (ext === ".css") return "text/css";
  if (ext === ".json") return "application/json";
  if (ext === ".svg") return "image/svg+xml";
  if (ext === ".png") return "image/png";
  if (ext === ".jpg" || ext === ".jpeg") return "image/jpeg";
  if (ext === ".webp") return "image/webp";
  return "application/octet-stream";
}

function serveLiveTradeUi(req, res, parsedUrl) {
  if (req.method !== "GET" && req.method !== "HEAD") return false;
  const pathname = String(parsedUrl.pathname || "");
  if (pathname === "/") {
    res.writeHead(308, {
      Location: "/live-trade/",
      "Cache-Control": "no-store",
    });
    res.end();
    return true;
  }
  if (!pathname.startsWith("/live-trade")) return false;
  const relativePath =
    pathname === "/live-trade" || pathname === "/live-trade/"
      ? "index.html"
      : pathname.replace(/^\/live-trade\/+/, "");
  const safePath = path.normalize(relativePath).replace(/^\.\.(\/|\\|$)+/, "");
  const filePath = path.join(LIVE_UI_DIR, safePath);
  if (!filePath.startsWith(LIVE_UI_DIR) || !fs.existsSync(filePath)) return false;
  const stat = fs.statSync(filePath);
  if (!stat.isFile()) return false;
  const ext = path.extname(filePath).toLowerCase();
  const cacheControl =
    ext === ".html" || ext === ".js" || ext === ".css" || ext === ".json"
      ? "no-cache, max-age=0, must-revalidate"
      : "public, max-age=86400";
  const body =
    req.method === "HEAD"
      ? ""
      : ext === ".html"
      ? fs.readFileSync(filePath, "utf8")
      : fs.readFileSync(filePath);
  sendText(
    res,
    200,
    contentTypeForLiveUi(filePath),
    body,
    cacheControl
  );
  return true;
}

function applyWindow(rows, offset = 0, limit = 0) {
  const list = Array.isArray(rows) ? rows : [];
  const safeOffset = Math.max(0, Math.floor(Number(offset || 0) || 0));
  const safeLimit = Math.max(0, Math.floor(Number(limit || 0) || 0));
  const total = list.length;
  const start = Math.min(safeOffset, total);
  const end = safeLimit > 0 ? Math.min(total, start + safeLimit) : total;
  return {
    rows: list.slice(start, end),
    total,
    offset: start,
    limit: safeLimit,
    returned: Math.max(0, end - start),
    hasMore: end < total,
    windowedByQuery: safeOffset > 0 || safeLimit > 0,
  };
}

function computeWalletScore(row) {
  const pnl = toNum(row.pnlAllTime, 0);
  const ret = toNum(row.returnPct, 0);
  const drawdown = toNum(row.drawdownPct, 0);
  const equity = toNum(row.accountEquityUsd, 0);
  const live = toNum(row.liveOpenPositions, 0);
  return Number(
    (
      pnl +
      ret * 20 +
      Math.min(500000, equity) * 0.05 +
      live * 250 -
      drawdown * 100
    ).toFixed(2)
  );
}

function loadCurrentData(force = false) {
  const now = Date.now();
  if (!force && dataCache.value && now - dataCache.loadedAt < dataCache.ttlMs) {
    return dataCache.value;
  }

  const sources = readJson(SOURCES_PATH, null) || {};
  const positiveRowsSnapshot = readJson(POSITIVE_WALLET_ROWS_SNAPSHOT_PATH, null) || {};
  const detailCache = readJson(DETAIL_CACHE_PATH, null) || { wallets: {} };
  const detailWallets =
    detailCache.wallets && typeof detailCache.wallets === "object" ? detailCache.wallets : {};
  const leaderboardRows =
    sources && sources.leaderboard && Array.isArray(sources.leaderboard.rows)
      ? sources.leaderboard.rows
      : Array.isArray(positiveRowsSnapshot.rows)
      ? positiveRowsSnapshot.rows
      : [];

  const positionFiles = fs.existsSync(POSITIONS_DIR)
    ? fs
        .readdirSync(POSITIONS_DIR)
        .filter((name) => /^wallet_first_shard_\d+\.json$/i.test(name))
        .map((name) => path.join(POSITIONS_DIR, name))
    : [];

  const positions = [];
  const openedEvents = [];
  const changeEvents = [];
  const liveStates = [];
  const shardStatuses = [];
  const liveSummaryByWallet = new Map();
  let liveGeneratedAt = 0;
  let walletsWithOpenPositions = 0;

  positionFiles.forEach((filePath) => {
    const payload = readJson(filePath, null);
    if (!payload || typeof payload !== "object") return;
    liveGeneratedAt = Math.max(liveGeneratedAt, toNum(payload.generatedAt, 0));
    const filePositions = Array.isArray(payload.positions) ? payload.positions : [];
    const fileOpened = Array.isArray(payload.positionOpenedEvents)
      ? payload.positionOpenedEvents
      : [];
    const fileChanges = Array.isArray(payload.positionChangeEvents)
      ? payload.positionChangeEvents
      : [];
    const fileStates = Array.isArray(payload.liveStates) ? payload.liveStates : [];
    positions.push(...filePositions);
    openedEvents.push(...fileOpened);
    changeEvents.push(...fileChanges);
    liveStates.push(...fileStates);
    const status = payload.status && typeof payload.status === "object" ? payload.status : {};
    shardStatuses.push({
      shardIndex: toNum(payload.shardIndex, shardStatuses.length),
      shardCount: toNum(payload.shardCount, 0),
      generatedAt: toNum(payload.generatedAt, 0),
      status,
      recentEventSources: (Array.isArray(payload.events) ? payload.events : [])
        .slice(0, 20)
        .map((row) => String((row && row.source) || "")),
    });
    walletsWithOpenPositions += toNum(status.walletsWithOpenPositions, 0);
  });

  positions.forEach((row) => {
    const wallet = normalizeWallet(row && row.wallet);
    if (!wallet) return;
    const existing = liveSummaryByWallet.get(wallet) || {
      openPositions: 0,
      exposureUsd: 0,
      unrealizedPnlUsd: 0,
      lastOpenedAt: 0,
      lastUpdatedAt: 0,
    };
    existing.openPositions += 1;
    existing.exposureUsd += toNum(row && row.positionUsd, 0);
    existing.unrealizedPnlUsd += toNum(
      row && (row.unrealizedPnlUsd !== undefined ? row.unrealizedPnlUsd : row.pnl),
      0
    );
    existing.lastOpenedAt = Math.max(existing.lastOpenedAt, tsMs(row && row.openedAt));
    existing.lastUpdatedAt = Math.max(existing.lastUpdatedAt, tsMs(row && row.updatedAt));
    liveSummaryByWallet.set(wallet, existing);
  });

  const walletPerformance = leaderboardRows
    .map((row) => {
      const wallet = normalizeWallet(row && row.wallet);
      if (!wallet) return null;
      const detail = detailWallets[wallet] && typeof detailWallets[wallet] === "object"
        ? detailWallets[wallet]
        : {};
      if (!isPositiveCohortWallet(row, detail)) return null;
      const live = liveSummaryByWallet.get(wallet) || null;
      const performanceOpenPositions = toNum(detail.openPositions, 0);
      const liveOpenPositions = live ? toNum(live.openPositions, 0) : 0;
      const out = {
        wallet,
        walletLabel: shortWallet(wallet),
        username: row && row.username ? String(row.username).trim() : null,
        rankAllTime: toNum(row && row.rankAllTime, 0),
        leaderboardPnlAllTime: toNum(row && row.pnlAllTime, 0),
        leaderboardPnl30d: toNum(row && row.pnl30d, 0),
        leaderboardPnl7d: toNum(row && row.pnl7d, 0),
        leaderboardPnl1d: toNum(row && row.pnl1d, 0),
        leaderboardVolumeAllTime: toNum(row && row.volumeAllTime, 0),
        leaderboardVolume30d: toNum(row && row.volume30d, 0),
        leaderboardVolume7d: toNum(row && row.volume7d, 0),
        leaderboardVolume1d: toNum(row && row.volume1d, 0),
        oiCurrent: toNum(row && row.oiCurrent, 0),
        volumeAllTime: toNum(detail.volumeAllTime, 0),
        volume30d: toNum(detail.volume30d, 0),
        volume14d: toNum(detail.volume14d, 0),
        volume7d: toNum(detail.volume7d, 0),
        volume1d: toNum(detail.volume1d, 0),
        pnlAllTime: toNum(detail.pnlAllTime, 0),
        drawdownPct: toNum(detail.drawdownPct, 0),
        returnPct: toNum(detail.returnPct, 0),
        accountEquityUsd: toNum(detail.accountEquityUsd, 0),
        openPositions: performanceOpenPositions,
        liveOpenPositions,
        exposureUsd: live ? Number(live.exposureUsd.toFixed(2)) : 0,
        unrealizedPnlUsd: live ? Number(live.unrealizedPnlUsd.toFixed(2)) : 0,
        lastSeenAt: tsMs(detail.lastSeenAt),
        lastUpdatedAt: tsMs(detail.accountUpdatedAt || detail.fetchedAt),
        liveLastOpenedAt: live ? tsMs(live.lastOpenedAt) : 0,
        liveLastUpdatedAt: live ? tsMs(live.lastUpdatedAt) : 0,
        metricSources: {
          walletUniverse: "leaderboard_all_time_pnl_desc",
          walletPerformance: "portfolio_endpoint",
          livePositions: "live_position_shards",
        },
      };
      out.score = computeWalletScore(out);
      return out;
    })
    .filter(Boolean);

  const liveActiveWallets = walletPerformance
    .filter((row) => row.liveOpenPositions > 0)
    .slice()
    .sort((left, right) => {
      const byOpen = compareNumbers(left.liveOpenPositions, right.liveOpenPositions, "desc");
      if (byOpen !== 0) return byOpen;
      return compareNumbers(left.exposureUsd, right.exposureUsd, "desc");
    });

  const positiveWalletSet = new Set(walletPerformance.map((row) => row.wallet));
  const positivePositions = positions.filter((row) => positiveWalletSet.has(normalizeWallet(row && row.wallet)));
  const positiveOpenedEvents = openedEvents.filter((row) => positiveWalletSet.has(normalizeWallet(row && row.wallet)));
  const positiveChangeEvents = changeEvents.filter((row) =>
    positiveWalletSet.has(normalizeWallet(row && row.wallet))
  );
  const positiveLiveStates = liveStates.filter((row) => positiveWalletSet.has(normalizeWallet(row && row.wallet)));
  const canonicalPositions = canonicalizePositionRows(positivePositions);
  positiveOpenedEvents.sort((left, right) =>
    compareNumbers(left && (left.openedAt || left.timestamp), right && (right.openedAt || right.timestamp), "desc")
  );
  positiveChangeEvents.sort((left, right) =>
    compareNumbers(
      left && (left.detectedAt || left.updatedAt || left.openedAt || left.timestamp),
      right && (right.detectedAt || right.updatedAt || right.openedAt || right.timestamp),
      "desc"
    )
  );

  const payload = {
    generatedAt: now,
    leaderboardGeneratedAt: toNum(sources.generatedAt, 0),
    detailGeneratedAt: toNum(detailCache.generatedAt, 0),
    liveGeneratedAt,
    walletPerformance,
    positions: canonicalPositions,
    positionOpenedEvents: positiveOpenedEvents,
    positionChangeEvents: positiveChangeEvents,
    liveStates: positiveLiveStates,
    liveActiveWallets,
    walletCount: walletPerformance.length,
    walletsWithPortfolioMetrics: walletPerformance.filter((row) => row.accountEquityUsd > 0 || row.volumeAllTime > 0 || row.pnlAllTime !== 0).length,
    walletsWithLiveOpenPositions: liveActiveWallets.length,
    openPositionsTotal: canonicalPositions.length,
    positionEventsTotal: positiveOpenedEvents.length + positiveChangeEvents.length,
    shardStatuses,
  };
  dataCache.loadedAt = now;
  dataCache.value = payload;
  return payload;
}

function positionIdentity(row = null) {
  const safeRow = row && typeof row === "object" ? row : {};
  const explicitKey = String(safeRow.positionKey || safeRow.key || "").trim();
  if (explicitKey) return explicitKey;
  const wallet = normalizeWallet(safeRow.wallet);
  const symbol = String(safeRow.symbol || "").trim().toUpperCase();
  const side = String(safeRow.side || safeRow.rawSide || "").trim().toLowerCase();
  const entry = toNum(safeRow.entry, NaN);
  const entryKey = Number.isFinite(entry) ? entry.toFixed(8) : "";
  const parts = [wallet, symbol, side, entryKey].filter((part) => String(part || "").trim());
  return parts.length ? parts.join("|") : null;
}

function isProvisionalPositionRow(row = null) {
  const safeRow = row && typeof row === "object" ? row : {};
  const status = String(safeRow.status || "").trim().toLowerCase();
  return Boolean(safeRow.provisional) || status === "provisional";
}

function positionRowStrength(row = null) {
  const safeRow = row && typeof row === "object" ? row : {};
  const freshness = String(safeRow.freshness || safeRow.status || "").trim().toLowerCase();
  const freshnessScore =
    freshness === "fresh"
      ? 4000
      : freshness === "cooling"
      ? 3000
      : freshness === "warm"
      ? 2000
      : freshness === "stale"
      ? 1000
      : 0;
  const provisionalPenalty = isProvisionalPositionRow(safeRow) ? -1000000 : 0;
  const sizeScore =
    Math.max(
      Math.abs(toNum(safeRow.size, 0)),
      Math.abs(toNum(safeRow.currentSize, 0)),
      Math.abs(toNum(safeRow.newSize, 0)),
      Math.abs(toNum(safeRow.sizeDeltaAbs, 0))
    ) > 0
      ? 250
      : 0;
  const positionScore =
    Math.max(Math.abs(toNum(safeRow.positionUsd, 0)), Math.abs(toNum(safeRow.eventUsd, 0))) > 0
      ? 100
      : 0;
  const updatedAt = Math.max(
    0,
    Number(
      safeRow.updatedAt ||
        safeRow.lastUpdatedAt ||
        safeRow.observedAt ||
        safeRow.lastObservedAt ||
        safeRow.timestamp ||
        0
    ) || 0
  );
  return provisionalPenalty + freshnessScore + sizeScore + positionScore + Math.floor(updatedAt / 1000);
}

function canonicalizePositionRows(rows = []) {
  const byKey = new Map();
  (Array.isArray(rows) ? rows : []).forEach((row) => {
    if (!row || typeof row !== "object") return;
    const status = String(row.status || "").trim().toLowerCase();
    if (status === "closed" || status === "inactive") return;
    const sizeValue = Math.max(
      Math.abs(toNum(row.size, 0)),
      Math.abs(toNum(row.currentSize, 0)),
      Math.abs(toNum(row.newSize, 0)),
      Math.abs(toNum(row.sizeDeltaAbs, 0))
    );
    const positionValue = Math.max(Math.abs(toNum(row.positionUsd, 0)), Math.abs(toNum(row.eventUsd, 0)));
    if (sizeValue <= 0 && positionValue <= 0) return;
    const key = positionIdentity(row);
    if (!key) return;
    const entryValue = Math.max(
      Math.abs(toNum(row.entry, 0)),
      Math.abs(toNum(row.entryPrice, 0)),
      Math.abs(toNum(row.entry_price, 0)),
      Math.abs(toNum(row.price, 0)),
      Math.abs(toNum(row.avgEntryPrice, 0)),
      Math.abs(toNum(row.avg_entry_price, 0))
    );
    const markValue = Math.max(
      Math.abs(toNum(row.mark, 0)),
      Math.abs(toNum(row.markPrice, 0)),
      Math.abs(toNum(row.mark_price, 0)),
      Math.abs(toNum(row.currentPrice, 0)),
      Math.abs(toNum(row.price, 0))
    );
    const derivedPositionValue =
      positionValue > 0
        ? positionValue
        : sizeValue > 0 && entryValue > 0
        ? sizeValue * entryValue
        : sizeValue > 0 && markValue > 0
        ? sizeValue * markValue
        : 0;
    const derivedEntryValue =
      entryValue > 0
        ? entryValue
        : sizeValue > 0 && derivedPositionValue > 0
        ? derivedPositionValue / sizeValue
        : 0;
    const current = byKey.get(key) || null;
    if (!current) {
      byKey.set(key, {
        ...row,
        size: sizeValue > 0 ? sizeValue : row.size,
        currentSize: sizeValue > 0 ? sizeValue : row.currentSize,
        newSize: sizeValue > 0 ? sizeValue : row.newSize,
        amount: sizeValue > 0 ? sizeValue : row.amount,
        positionUsd: derivedPositionValue > 0 ? derivedPositionValue : row.positionUsd,
        eventUsd: derivedPositionValue > 0 ? derivedPositionValue : row.eventUsd,
        entry: derivedEntryValue > 0 ? derivedEntryValue : row.entry,
        entryPrice: derivedEntryValue > 0 ? derivedEntryValue : row.entryPrice,
        entry_price: derivedEntryValue > 0 ? derivedEntryValue : row.entry_price,
      });
      return;
    }
    const currentScore = positionRowStrength(current);
    const nextScore = positionRowStrength(row);
    if (nextScore > currentScore) {
      byKey.set(key, {
        ...row,
        size: sizeValue > 0 ? sizeValue : row.size,
        currentSize: sizeValue > 0 ? sizeValue : row.currentSize,
        newSize: sizeValue > 0 ? sizeValue : row.newSize,
        amount: sizeValue > 0 ? sizeValue : row.amount,
        positionUsd: derivedPositionValue > 0 ? derivedPositionValue : row.positionUsd,
        eventUsd: derivedPositionValue > 0 ? derivedPositionValue : row.eventUsd,
        entry: derivedEntryValue > 0 ? derivedEntryValue : row.entry,
        entryPrice: derivedEntryValue > 0 ? derivedEntryValue : row.entryPrice,
        entry_price: derivedEntryValue > 0 ? derivedEntryValue : row.entry_price,
      });
      return;
    }
    if (nextScore === currentScore) {
      const currentUpdatedAt = Math.max(
        0,
        Number(
          current.updatedAt ||
            current.lastUpdatedAt ||
            current.observedAt ||
            current.lastObservedAt ||
            current.timestamp ||
            0
        ) || 0
      );
      const nextUpdatedAt = Math.max(
        0,
        Number(
          row.updatedAt || row.lastUpdatedAt || row.observedAt || row.lastObservedAt || row.timestamp || 0
        ) || 0
      );
      if (nextUpdatedAt >= currentUpdatedAt) {
        byKey.set(key, {
          ...row,
          size: sizeValue > 0 ? sizeValue : row.size,
          currentSize: sizeValue > 0 ? sizeValue : row.currentSize,
          newSize: sizeValue > 0 ? sizeValue : row.newSize,
          amount: sizeValue > 0 ? sizeValue : row.amount,
          positionUsd: derivedPositionValue > 0 ? derivedPositionValue : row.positionUsd,
          eventUsd: derivedPositionValue > 0 ? derivedPositionValue : row.eventUsd,
          entry: derivedEntryValue > 0 ? derivedEntryValue : row.entry,
          entryPrice: derivedEntryValue > 0 ? derivedEntryValue : row.entryPrice,
          entry_price: derivedEntryValue > 0 ? derivedEntryValue : row.entry_price,
        });
      }
    }
  });
  return Array.from(byKey.values())
    .map((row) => {
      const safeRow = row && typeof row === "object" ? row : {};
      const marginValue = Number(
        toNum(
          safeRow.margin !== undefined
            ? safeRow.margin
            : safeRow.marginUsd !== undefined
            ? safeRow.marginUsd
            : safeRow.margin_usd,
          NaN
        )
      );
      const positionUsdValue = Number(
        toNum(
          safeRow.positionUsd !== undefined ? safeRow.positionUsd : safeRow.eventUsd,
          NaN
        )
      );
      const leverageValue =
        Number.isFinite(marginValue) &&
        marginValue > 0 &&
        Number.isFinite(positionUsdValue)
          ? positionUsdValue / marginValue
          : Number(toNum(safeRow.leverage, NaN));
      return {
        ...safeRow,
        margin: Number.isFinite(marginValue) ? roundTo(marginValue, 2) : safeRow.margin,
        marginUsd: Number.isFinite(marginValue) ? roundTo(marginValue, 2) : safeRow.marginUsd,
        leverage: Number.isFinite(leverageValue) ? roundTo(leverageValue, 4) : safeRow.leverage,
      };
    })
    .sort((left, right) =>
      compareNumbers(
        Math.max(
          tsMs(left && (left.openedAt || left.timestamp || left.observedAt || left.updatedAt)),
          tsMs(left && (left.observedAt || left.updatedAt || left.timestamp || left.openedAt))
        ),
        Math.max(
          tsMs(right && (right.openedAt || right.timestamp || right.observedAt || right.updatedAt)),
          tsMs(right && (right.observedAt || right.updatedAt || right.timestamp || right.openedAt))
        ),
        "desc"
      )
    );
}

function buildWalletPerformanceRows(data, searchParams) {
  const rows = Array.isArray(data.walletPerformance) ? data.walletPerformance.slice() : [];
  const exactWallet = normalizeWallet(searchParams.get("wallet"));
  const walletSort = String(searchParams.get("wallet_sort") || "score").trim() || "score";
  const walletDir = String(searchParams.get("wallet_dir") || "desc").trim().toLowerCase() === "asc" ? "asc" : "desc";
  const offset = Math.max(0, Number(searchParams.get("wallet_offset") || 0) || 0);
  const limit = Math.max(0, Number(searchParams.get("wallet_limit") || 50) || 50);

  let filtered = rows;
  if (exactWallet) {
    filtered = filtered.filter((row) => row.wallet === exactWallet);
  }
  filtered.sort((left, right) => {
    if (walletSort === "wallet") return compareStrings(left.wallet, right.wallet, walletDir);
    if (walletSort === "rankAllTime") return compareNumbers(left.rankAllTime, right.rankAllTime, walletDir);
    return compareNumbers(left[walletSort], right[walletSort], walletDir);
  });
  return applyWindow(filtered, offset, limit);
}

function normalizePositionEventType(rawType, oldSize = NaN, newSize = NaN) {
  const normalized = String(rawType || "")
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, "_");
  if (!normalized) {
    const hasOld = Number.isFinite(toNum(oldSize, NaN));
    const hasNext = Number.isFinite(toNum(newSize, NaN));
    if (hasOld && hasNext) {
      const prev = Math.abs(toNum(oldSize, 0));
      const next = Math.abs(toNum(newSize, 0));
      if (prev <= 0 && next > 0) return "position_opened";
      if (prev > 0 && next <= 0) return "position_closed";
      if (prev > 0 && next < prev) return "position_closed_partial";
      if (prev > 0 && next > prev) return "position_increased";
    }
    return "";
  }
  if (
    normalized.includes("position_opened") ||
    normalized === "opened" ||
    normalized === "open" ||
    normalized.includes("new_position")
  ) {
    return "position_opened";
  }
  if (
    normalized.includes("position_closed_partial") ||
    normalized.includes("partial_close") ||
    normalized.includes("partial_closed") ||
    normalized.includes("position_reduced") ||
    normalized.includes("size_reduced")
  ) {
    return "position_closed_partial";
  }
  if (
    normalized.includes("position_closed") ||
    normalized === "closed" ||
    normalized.includes("full_close") ||
    normalized.includes("position_flattened") ||
    normalized.includes("exit")
  ) {
    return "position_closed";
  }
  if (
    normalized.includes("position_increased") ||
    normalized.includes("position_size_changed") ||
    normalized.includes("size_changed") ||
    normalized.includes("increase")
  ) {
    return "position_increased";
  }
  return normalized;
}

function decoratePositionEventRow(row = null) {
  const safeRow = row && typeof row === "object" ? row : {};
  const eventType = normalizePositionEventType(safeRow.eventType || safeRow.sideEvent || safeRow.cause || safeRow.type, safeRow.oldSize, safeRow.newSize);
  const detectedAt = Math.max(
    0,
    Number(
      safeRow.detectedAt ||
        safeRow.observedAt ||
        safeRow.updatedAt ||
        safeRow.lastObservedAt ||
        safeRow.timestamp ||
        safeRow.openedAt ||
        0
    ) || 0
  );
  const openedAt = Math.max(0, Number(safeRow.openedAt || safeRow.timestamp || detectedAt || 0) || 0);
  return {
    ...safeRow,
    eventType: eventType || safeRow.eventType || safeRow.sideEvent || safeRow.cause || safeRow.type || "",
    detectedAt,
    openedAt,
    timestamp: detectedAt || safeRow.timestamp || openedAt,
    source: safeRow.source || "wallet_first_positions",
  };
}

function positionEventIdentity(row = null) {
  const safeRow = row && typeof row === "object" ? row : {};
  const eventType = String(safeRow.eventType || safeRow.sideEvent || safeRow.cause || safeRow.type || "").trim().toLowerCase();
  const detectedAt = Math.max(
    0,
    Number(safeRow.detectedAt || safeRow.observedAt || safeRow.updatedAt || safeRow.timestamp || safeRow.openedAt || 0) || 0
  );
  const positionKey = String(safeRow.positionKey || safeRow.key || "").trim();
  const wallet = normalizeWallet(safeRow.wallet);
  const symbol = String(safeRow.symbol || "").trim().toUpperCase();
  const side = String(safeRow.side || safeRow.rawSide || "").trim().toLowerCase();
  return [wallet, symbol, side, eventType, detectedAt].filter(Boolean).join("|");
}

function positionMatchKey(row = null) {
  const safeRow = row && typeof row === "object" ? row : {};
  const wallet = normalizeWallet(safeRow.wallet);
  const symbol = String(safeRow.symbol || "").trim().toUpperCase();
  const side = String(safeRow.side || safeRow.rawSide || "").trim().toLowerCase();
  if (!wallet || !symbol || !side) return null;
  return [wallet, symbol, side].join("|");
}

function hydratePositionFromCurrent(row = null, currentByKey = new Map()) {
  const safeRow = row && typeof row === "object" ? row : {};
  const eventType = String(safeRow.eventType || safeRow.sideEvent || safeRow.cause || safeRow.type || "").trim().toLowerCase();
  if (
    eventType.includes("position_closed") ||
    eventType.includes("partial_close") ||
    eventType.includes("position_flattened") ||
    eventType.includes("closed")
  ) {
    return safeRow;
  }
  const directKey = positionIdentity(safeRow);
  const fallbackKey = positionMatchKey(safeRow);
  const current = (directKey && currentByKey.get(directKey)) || (fallbackKey && currentByKey.get(fallbackKey));
  if (!current) return safeRow;
  const sizeValue = Math.max(
    Math.abs(toNum(safeRow.size, 0)),
    Math.abs(toNum(safeRow.currentSize, 0)),
    Math.abs(toNum(safeRow.newSize, 0)),
    Math.abs(toNum(safeRow.sizeDeltaAbs, 0)),
    Math.abs(toNum(safeRow.positionUsd, 0))
  );
  const shouldHydrate =
    sizeValue <= 0 ||
    !Number.isFinite(toNum(safeRow.positionUsd, NaN)) ||
    !Number.isFinite(toNum(safeRow.entry, NaN)) ||
    !Number.isFinite(toNum(safeRow.mark, NaN));
  if (!shouldHydrate) return safeRow;
  const pickFinite = (...values) => {
    for (const value of values) {
      const num = toNum(value, NaN);
      if (Number.isFinite(num)) return num;
    }
    return NaN;
  };
  const marginValue = pickFinite(
    safeRow.margin,
    safeRow.marginUsd,
    safeRow.margin_usd,
    current && current.margin,
    current && current.marginUsd,
    current && current.margin_usd
  );
  const leverageValue = pickFinite(
    safeRow.leverage,
    safeRow.lev,
    current && current.leverage,
    current && current.lev,
    Number.isFinite(marginValue) && marginValue > 0 && Number.isFinite(toNum(safeRow.positionUsd, NaN))
      ? toNum(safeRow.positionUsd, 0) / marginValue
      : Number.isFinite(marginValue) &&
        marginValue > 0 &&
        Number.isFinite(toNum(current && current.positionUsd, NaN))
      ? toNum(current.positionUsd, 0) / marginValue
      : NaN
  );
  return {
    ...current,
    ...safeRow,
    wallet: safeRow.wallet || current.wallet,
    symbol: safeRow.symbol || current.symbol,
    side: safeRow.side || current.side,
    rawSide: safeRow.rawSide || current.rawSide,
    entry: Number.isFinite(toNum(safeRow.entry, NaN)) && toNum(safeRow.entry, 0) > 0 ? safeRow.entry : current.entry,
    mark: Number.isFinite(toNum(safeRow.mark, NaN)) && toNum(safeRow.mark, 0) > 0 ? safeRow.mark : current.mark,
    size:
      Number.isFinite(toNum(safeRow.size, NaN)) && Math.abs(toNum(safeRow.size, 0)) > 0
        ? safeRow.size
        : current.size,
    currentSize:
      Number.isFinite(toNum(safeRow.currentSize, NaN)) && Math.abs(toNum(safeRow.currentSize, 0)) > 0
        ? safeRow.currentSize
        : current.currentSize || current.size,
    newSize:
      Number.isFinite(toNum(safeRow.newSize, NaN)) && Math.abs(toNum(safeRow.newSize, 0)) > 0
        ? safeRow.newSize
        : current.newSize || current.size,
    amount:
      Number.isFinite(toNum(safeRow.amount, NaN)) && Math.abs(toNum(safeRow.amount, 0)) > 0
        ? safeRow.amount
        : current.amount || current.size,
    positionUsd:
      Number.isFinite(toNum(safeRow.positionUsd, NaN)) && Math.abs(toNum(safeRow.positionUsd, 0)) > 0
        ? safeRow.positionUsd
        : current.positionUsd,
    eventUsd:
      Number.isFinite(toNum(safeRow.eventUsd, NaN)) && Math.abs(toNum(safeRow.eventUsd, 0)) > 0
        ? safeRow.eventUsd
        : current.eventUsd || current.positionUsd,
    isolated:
      safeRow.isolated !== undefined ? safeRow.isolated : current.isolated,
    margin:
      Number.isFinite(toNum(safeRow.margin, NaN)) && Math.abs(toNum(safeRow.margin, 0)) > 0
        ? safeRow.margin
        : Number.isFinite(marginValue)
        ? roundTo(marginValue, 2)
        : current.margin,
    marginUsd:
      Number.isFinite(toNum(safeRow.marginUsd, NaN)) && Math.abs(toNum(safeRow.marginUsd, 0)) > 0
        ? safeRow.marginUsd
        : Number.isFinite(marginValue)
        ? roundTo(marginValue, 2)
        : current.marginUsd,
    leverage:
      Number.isFinite(toNum(safeRow.leverage, NaN)) && Math.abs(toNum(safeRow.leverage, 0)) > 0
        ? safeRow.leverage
        : Number.isFinite(leverageValue)
        ? roundTo(leverageValue, 4)
        : current.leverage,
    funding:
      Number.isFinite(toNum(safeRow.funding, NaN)) && Math.abs(toNum(safeRow.funding, 0)) > 0 ? safeRow.funding : current.funding,
    liquidationPrice:
      Number.isFinite(toNum(safeRow.liquidationPrice, NaN)) && Math.abs(toNum(safeRow.liquidationPrice, 0)) > 0
        ? safeRow.liquidationPrice
        : current.liquidationPrice,
    detectedAt: Math.max(
      0,
      Number(
        safeRow.detectedAt ||
          safeRow.observedAt ||
          safeRow.updatedAt ||
          safeRow.timestamp ||
          current.detectedAt ||
          current.observedAt ||
          current.updatedAt ||
          current.timestamp ||
          0
      ) || 0
    ),
    observedAt: Math.max(0, Number(safeRow.observedAt || current.observedAt || safeRow.detectedAt || current.detectedAt || 0) || 0),
    timestamp: Math.max(0, Number(safeRow.timestamp || safeRow.detectedAt || current.timestamp || current.detectedAt || 0) || 0),
    status: safeRow.status || current.status,
    freshness: safeRow.freshness || current.freshness,
  };
}

function buildPositionsRows(data, searchParams) {
  const exactWallet = normalizeWallet(searchParams.get("wallet"));
  const needle = String(searchParams.get("q") || searchParams.get("search") || "").trim().toLowerCase();
  const feedMode = String(searchParams.get("feed_mode") || "current").trim().toLowerCase();
  const offset = Math.max(0, Number(searchParams.get("position_offset") || 0) || 0);
  const limit = Math.max(0, Number(searchParams.get("position_limit") || 50) || 50);
  const defaultSortKey = feedMode === "events" ? "detectedAt" : feedMode === "new" ? "observedAt" : "openedAt";
  const sortKey = String(searchParams.get("position_sort") || defaultSortKey).trim() || defaultSortKey;
  const sortDir = String(searchParams.get("position_dir") || "desc").trim().toLowerCase() === "asc" ? "asc" : "desc";
  const openedSince = Math.max(0, Number(searchParams.get("opened_since") || 0) || 0);
  const positiveWalletSet = new Set(
    (Array.isArray(data.walletPerformance) ? data.walletPerformance : [])
      .map((row) => normalizeWallet(row && row.wallet))
      .filter(Boolean)
  );
  const currentPositionRows = Array.isArray(data.positions) ? data.positions : [];
  const currentPositionByKey = new Map();
  currentPositionRows.forEach((row) => {
    const identity = positionIdentity(row);
    const matchKey = positionMatchKey(row);
    if (identity) currentPositionByKey.set(identity, row);
    if (matchKey) currentPositionByKey.set(matchKey, row);
  });

  let rows;
  if (feedMode === "events") {
    const changeRows = Array.isArray(data.positionChangeEvents) ? data.positionChangeEvents.slice() : [];
    const openedRows = Array.isArray(data.positionOpenedEvents) ? data.positionOpenedEvents.slice() : [];
    const dedupedRows = new Map();
    changeRows.concat(openedRows).map(decoratePositionEventRow).map((row) => hydratePositionFromCurrent(row, currentPositionByKey)).forEach((row) => {
      const key = positionEventIdentity(row);
      if (!key) return;
      const current = dedupedRows.get(key);
      if (!current) {
        dedupedRows.set(key, row);
        return;
      }
      const currentUpdatedAt = Math.max(
        0,
        Number(current.detectedAt || current.observedAt || current.updatedAt || current.timestamp || current.openedAt || 0) || 0
      );
      const nextUpdatedAt = Math.max(
        0,
        Number(row.detectedAt || row.observedAt || row.updatedAt || row.timestamp || row.openedAt || 0) || 0
      );
      if (nextUpdatedAt >= currentUpdatedAt) {
        dedupedRows.set(key, row);
      }
    });
    rows = Array.from(dedupedRows.values());
  } else if (feedMode === "new") {
    rows = Array.isArray(data.positionOpenedEvents) ? data.positionOpenedEvents.slice() : [];
  } else {
    rows = Array.isArray(data.positions) ? data.positions.slice() : [];
  }

  if (positiveWalletSet.size) {
    rows = rows.filter((row) => positiveWalletSet.has(normalizeWallet(row && row.wallet)));
  }
  if (feedMode === "current") {
    rows = canonicalizePositionRows(rows);
  }
  if (exactWallet) rows = rows.filter((row) => normalizeWallet(row && row.wallet) === exactWallet);
  if (needle) {
    rows = rows.filter((row) => {
      const wallet = normalizeWallet(row && row.wallet).toLowerCase();
      const symbol = String((row && row.symbol) || "").toLowerCase();
      return wallet.includes(needle) || symbol.includes(needle);
    });
  }
  if (openedSince > 0) {
    rows = rows.filter((row) => tsMs(row && (row.openedAt || row.timestamp)) >= openedSince);
  }
  if (feedMode === "new") {
    rows = rows.filter((row) => {
      const openedAt = tsMs(row && (row.openedAt || row.timestamp));
      const observedAt = tsMs(row && (row.observedAt || row.lastObservedAt || row.updatedAt || row.timestamp));
      if (openedAt <= 0 || observedAt <= 0) return false;
      return Math.max(0, observedAt - openedAt) <= NEW_EVENT_MAX_LAG_MS;
    });
  }
  if (feedMode === "events") {
    rows = rows.map(decoratePositionEventRow);
    rows = rows.map((row) => hydratePositionFromCurrent(row, currentPositionByKey));
    rows = rows.filter((row) => {
      const eventType = String(row && (row.eventType || row.sideEvent || row.cause || row.type) || "")
        .trim()
        .toLowerCase();
      return (
        eventType.includes("position_opened") ||
        eventType.includes("position_increased") ||
        eventType.includes("position_closed_partial") ||
        eventType.includes("position_closed") ||
        eventType.includes("position_size_changed")
      );
    });
  }
  rows.sort((left, right) => compareNumbers(left && left[sortKey], right && right[sortKey], sortDir));
  return {
    feedMode,
    window: applyWindow(rows, offset, limit),
    total: rows.length,
  };
}

function buildWalletDetail(data, wallet) {
  const normalizedWallet = normalizeWallet(wallet);
  if (!normalizedWallet) return null;
  const summary =
    (Array.isArray(data.walletPerformance) ? data.walletPerformance : []).find(
      (row) => row.wallet === normalizedWallet
    ) || null;
  if (!summary) return null;
  const positions = (Array.isArray(data.positions) ? data.positions : []).filter(
    (row) => normalizeWallet(row && row.wallet) === normalizedWallet
  );
  const recentOpens = (Array.isArray(data.positionOpenedEvents) ? data.positionOpenedEvents : []).filter(
    (row) => normalizeWallet(row && row.wallet) === normalizedWallet
  );
  return {
    generatedAt: Date.now(),
    found: true,
    mode: "leaderboard_portfolio_live_trade_v1",
    wallet: normalizedWallet,
    summary,
    positions,
    recentOpens,
  };
}

function buildMetrics(data) {
  const now = Date.now();
  const shardStatuses = Array.isArray(data.shardStatuses) ? data.shardStatuses : [];
  const filteredNewRows = (Array.isArray(data.positionOpenedEvents) ? data.positionOpenedEvents : []).filter((row) => {
    const openedAt = tsMs(row && (row.openedAt || row.timestamp));
    const observedAt = tsMs(row && (row.observedAt || row.lastObservedAt || row.updatedAt || row.timestamp));
    return openedAt > 0 && observedAt > 0 && Math.max(0, observedAt - openedAt) <= NEW_EVENT_MAX_LAG_MS;
  });
  const lagMs = filteredNewRows
    .map((row) => Math.max(0, tsMs(row && (row.observedAt || row.timestamp)) - tsMs(row && (row.openedAt || row.timestamp))))
    .filter((value) => Number.isFinite(value));
  const ageSec = filteredNewRows
    .map((row) => Math.max(0, now - tsMs(row && (row.observedAt || row.timestamp))) / 1000)
    .filter((value) => Number.isFinite(value));
  const allPositionEventRows = []
    .concat(Array.isArray(data.positionOpenedEvents) ? data.positionOpenedEvents : [])
    .concat(Array.isArray(data.positionChangeEvents) ? data.positionChangeEvents : []);
  const latestPositionEventAt = allPositionEventRows.reduce((acc, row) => {
    const at = tsMs(row && (row.detectedAt || row.observedAt || row.updatedAt || row.timestamp || row.openedAt));
    return at > acc ? at : acc;
  }, 0);
  const latestPositionEventAgeSec = latestPositionEventAt > 0 ? Math.max(0, now - latestPositionEventAt) / 1000 : null;
  return {
    generatedAt: now,
    mode: "leaderboard_portfolio_live_trade_v1",
    aggregate: {
      trackedWallets: data.walletCount,
      subscribedWallets: shardStatuses.reduce((sum, row) => sum + toNum(row.status && row.status.hotWsActiveWallets, 0), 0),
      openWebsocketConnections: shardStatuses.reduce((sum, row) => sum + toNum(row.status && row.status.hotWsOpenConnections, 0), 0),
      eventsPerMinute: shardStatuses.reduce((sum, row) => sum + toNum(row.status && row.status.eventsPerMinute, 0), 0),
      wsEventsPerMinute: shardStatuses.reduce((sum, row) => sum + toNum(row.status && row.status.wsEventsPerMinute, 0), 0),
      newOpenEventsPerMinute: shardStatuses.reduce((sum, row) => sum + toNum(row.status && row.status.newOpenEventsPerMinute, 0), 0),
      positionOpenedEventsPerMinute: shardStatuses.reduce(
        (sum, row) => sum + toNum(row.status && row.status.positionOpenedEventsPerMinute, 0),
        0
      ),
      positionIncreasedEventsPerMinute: shardStatuses.reduce(
        (sum, row) => sum + toNum(row.status && row.status.positionIncreasedEventsPerMinute, 0),
        0
      ),
      positionPartialCloseEventsPerMinute: shardStatuses.reduce(
        (sum, row) => sum + toNum(row.status && row.status.positionPartialCloseEventsPerMinute, 0),
        0
      ),
      positionClosedEventsPerMinute: shardStatuses.reduce(
        (sum, row) => sum + toNum(row.status && row.status.positionClosedEventsPerMinute, 0),
        0
      ),
      delayedOpenEventsOver5s1m: shardStatuses.reduce((sum, row) => sum + toNum(row.status && row.status.delayedOpenEventsOver5s1m, 0), 0),
      hotWsErrorCount: shardStatuses.reduce((sum, row) => sum + toNum(row.status && row.status.hotWsErrorCount, 0), 0),
      hotWsReconnectTransitions: shardStatuses.reduce((sum, row) => sum + toNum(row.status && row.status.hotWsReconnectTransitions, 0), 0),
      hotWsDroppedPromotions: shardStatuses.reduce((sum, row) => sum + toNum(row.status && row.status.hotWsDroppedPromotions, 0), 0),
      detectionLagP50Ms: quantile(lagMs, 0.5),
      detectionLagP90Ms: quantile(lagMs, 0.9),
      detectionLagP95Ms: quantile(lagMs, 0.95),
      feedAgeP50Sec: quantile(ageSec, 0.5),
      feedAgeP90Sec: quantile(ageSec, 0.9),
      feedAgeP95Sec: quantile(ageSec, 0.95),
      latestPositionEventAt,
      latestPositionEventAgeSec: Number.isFinite(latestPositionEventAgeSec)
        ? Number(latestPositionEventAgeSec.toFixed(3))
        : null,
    },
    perShard: shardStatuses
      .slice()
      .sort((left, right) => compareNumbers(left.shardIndex, right.shardIndex, "asc"))
      .map((row) => ({
        shardIndex: row.shardIndex,
        shardCount: row.shardCount,
        generatedAt: row.generatedAt,
        mode: row.status && row.status.mode,
        walletsKnown: toNum(row.status && row.status.walletsKnown, 0),
        subscribedWallets: toNum(row.status && row.status.hotWsActiveWallets, 0),
        openWebsocketConnections: toNum(row.status && row.status.hotWsOpenConnections, 0),
        openPositionsTotal: toNum(row.status && row.status.openPositionsTotal, 0),
        walletsWithOpenPositions: toNum(row.status && row.status.walletsWithOpenPositions, 0),
        eventsPerMinute: toNum(row.status && row.status.eventsPerMinute, 0),
        wsEventsPerMinute: toNum(row.status && row.status.wsEventsPerMinute, 0),
        newOpenEventsPerMinute: toNum(row.status && row.status.newOpenEventsPerMinute, 0),
        positionOpenedEventsPerMinute: toNum(
          row.status && row.status.positionOpenedEventsPerMinute,
          0
        ),
        positionIncreasedEventsPerMinute: toNum(
          row.status && row.status.positionIncreasedEventsPerMinute,
          0
        ),
        positionPartialCloseEventsPerMinute: toNum(
          row.status && row.status.positionPartialCloseEventsPerMinute,
          0
        ),
        positionClosedEventsPerMinute: toNum(
          row.status && row.status.positionClosedEventsPerMinute,
          0
        ),
        delayedOpenEventsOver5s1m: toNum(row.status && row.status.delayedOpenEventsOver5s1m, 0),
        detectionLagP50Ms1m: toNum(row.status && row.status.detectionLagP50Ms1m, 0),
        detectionLagP90Ms1m: toNum(row.status && row.status.detectionLagP90Ms1m, 0),
        detectionLagP95Ms1m: toNum(row.status && row.status.detectionLagP95Ms1m, 0),
        hotWsTriggerToEventAvgMs: toNum(row.status && row.status.hotWsTriggerToEventAvgMs, 0),
        hotWsLastTriggerToEventMs: toNum(row.status && row.status.hotWsLastTriggerToEventMs, 0),
        hotWsErrorCount: toNum(row.status && row.status.hotWsErrorCount, 0),
        hotWsReconnectTransitions: toNum(row.status && row.status.hotWsReconnectTransitions, 0),
        hotWsDroppedPromotions: toNum(row.status && row.status.hotWsDroppedPromotions, 0),
        lastPassDurationMs: toNum(row.status && row.status.lastPassDurationMs, 0),
        passThroughputRps: toNum(row.status && row.status.passThroughputRps, 0),
        recentEventSources: row.recentEventSources,
      })),
  };
}

function writeSseSnapshot(res, data) {
  const payload = {
    generatedAt: Date.now(),
    type: "snapshot",
    payload: {
      generatedAt: Date.now(),
      mode: "leaderboard_portfolio_live_trade_v1",
      summary: {
        trackedWallets: data.walletCount,
        indexedWallets: Math.min(50, data.walletCount),
        indexedWalletsTotal: data.walletCount,
        walletsWithPortfolioMetrics: data.walletsWithPortfolioMetrics,
        walletsWithOpenPositions: data.walletsWithLiveOpenPositions,
        openPositions: Math.min(50, data.openPositionsTotal),
        openPositionsTotal: data.openPositionsTotal,
        source: "leaderboard+portfolio+live_positions",
      },
      walletPerformance: data.walletPerformance.slice(0, 50),
      positions: data.positions.slice(0, 50),
      liveActiveWallets: data.liveActiveWallets.slice(0, 12),
      publicTrades: [],
      accountTrades: [],
      accountOverview: null,
    },
  };
  res.write(`event: snapshot\n`);
  res.write(`data: ${JSON.stringify(payload)}\n\n`);
  streamState.lastBroadcastSignature = buildStreamSignature(data);
}

function writeSseDelta(res, data) {
  const payload = {
    generatedAt: Date.now(),
    mode: "leaderboard_portfolio_live_trade_v1",
    status: {
      enabled: true,
      sourceMode: "leaderboard_wallet_universe",
      streamMode: "delta",
    },
    summary: {
      trackedWallets: data.walletCount,
      indexedWallets: 0,
      indexedWalletsTotal: data.walletCount,
      walletsWithPortfolioMetrics: data.walletsWithPortfolioMetrics,
      openPositions: Math.min(100, data.openPositionsTotal),
      openPositionsTotal: data.openPositionsTotal,
      liveActiveWallets: Math.min(20, data.liveActiveWallets.length),
      liveActiveWalletsTotal: data.liveActiveWallets.length,
      liveGeneratedAt: data.liveGeneratedAt,
      leaderboardGeneratedAt: data.leaderboardGeneratedAt,
      portfolioGeneratedAt: data.detailGeneratedAt,
      walletUniverseSource:
        "https://app.pacifica.fi/leaderboard?timeframe=allTime&sortField=pnl&sortDir=desc",
      walletPerformanceSource: "https://app.pacifica.fi/portfolio/",
    },
    walletPerformance: [],
    positions: data.positions.slice(0, 100),
    liveActiveWallets: data.liveActiveWallets.slice(0, 20),
    publicTrades: [],
    accountTrades: [],
    accountOverview: null,
  };
  res.write(`event: delta\n`);
  res.write(`data: ${JSON.stringify(payload)}\n\n`);
}

function writeSseHeartbeat(res, data) {
  res.write(`event: heartbeat\n`);
  res.write(
    `data: ${JSON.stringify({
      generatedAt: Date.now(),
      mode: "leaderboard_portfolio_live_trade_v1",
      status: {
        enabled: true,
        sourceMode: "leaderboard_wallet_universe",
        streamMode: "heartbeat",
      },
      summary: {
        trackedWallets: data.walletCount,
        indexedWalletsTotal: data.walletCount,
        walletsWithPortfolioMetrics: data.walletsWithPortfolioMetrics,
        openPositionsTotal: data.openPositionsTotal,
        liveGeneratedAt: data.liveGeneratedAt,
        leaderboardGeneratedAt: data.leaderboardGeneratedAt,
        portfolioGeneratedAt: data.detailGeneratedAt,
      },
    })}\n\n`
  );
}

function buildStreamSignature(data) {
  return [
    Number(data && data.generatedAt ? data.generatedAt : 0),
    Number(data && data.walletCount ? data.walletCount : 0),
    Number(data && data.openPositionsTotal ? data.openPositionsTotal : 0),
    Number(data && data.liveGeneratedAt ? data.liveGeneratedAt : 0),
    Number(Array.isArray(data && data.positionOpenedEvents) ? data.positionOpenedEvents.length : 0),
    Number(Array.isArray(data && data.liveStates) ? data.liveStates.length : 0),
  ].join(":");
}

function handleApiRequest(req, res, parsedUrl) {
  const pathname = String(parsedUrl.pathname || "");
  if (serveLiveTradeUi(req, res, parsedUrl)) {
    return true;
  }
  const data = loadCurrentData(false);

  if (pathname === "/health" || pathname === "/api/live-trades/health") {
    sendJson(res, 200, {
      ok: true,
      mode: "leaderboard_portfolio_live_trade_v1",
      generatedAt: Date.now(),
      walletCount: data.walletCount,
      walletsWithPortfolioMetrics: data.walletsWithPortfolioMetrics,
      openPositionsTotal: data.openPositionsTotal,
    });
    return true;
  }

  if (pathname === "/api/live-trades/wallet-first/stream" || pathname === "/api/live-trades/stream") {
    res.writeHead(200, {
      "Content-Type": "text/event-stream; charset=utf-8",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
      "X-Accel-Buffering": "no",
    });
    if (typeof res.flushHeaders === "function") res.flushHeaders();
    writeSseSnapshot(res, data);
    const timer = setInterval(() => {
      try {
        const nextData = loadCurrentData(false);
        const nextSignature = buildStreamSignature(nextData);
        if (nextSignature !== streamState.lastBroadcastSignature) {
          streamState.lastBroadcastSignature = nextSignature;
          writeSseDelta(res, nextData);
        } else {
          writeSseHeartbeat(res, nextData);
        }
      } catch (_error) {
        // ignore
      }
    }, SSE_INTERVAL_MS);
    const cleanup = () => clearInterval(timer);
    req.on("close", cleanup);
    req.on("aborted", cleanup);
    res.on("close", cleanup);
    return true;
  }

  if (pathname === "/api/live-trades/wallet-first" || pathname === "/api/live-trades") {
    const walletWindow = buildWalletPerformanceRows(data, parsedUrl.searchParams);
    const positionsResult = buildPositionsRows(data, parsedUrl.searchParams);
    const liveActiveOffset = Math.max(0, Number(parsedUrl.searchParams.get("live_active_offset") || 0) || 0);
    const liveActiveLimit = Math.max(0, Number(parsedUrl.searchParams.get("live_active_limit") || 12) || 12);
    const liveActiveWindow = applyWindow(data.liveActiveWallets, liveActiveOffset, liveActiveLimit);
    sendJson(res, 200, {
      generatedAt: Date.now(),
      mode: "leaderboard_portfolio_live_trade_v1",
      status: {
        enabled: true,
        sourceMode: "leaderboard_wallet_universe",
      },
      summary: {
        trackedWallets: data.walletCount,
        indexedWallets: walletWindow.returned,
        indexedWalletsTotal: walletWindow.total,
        indexedWalletsOffset: walletWindow.offset,
        indexedWalletsLimit: walletWindow.limit,
        indexedWalletsHasMore: walletWindow.hasMore,
        walletsWithPortfolioMetrics: data.walletsWithPortfolioMetrics,
        openPositions: positionsResult.window.returned,
        openPositionsTotal: positionsResult.total,
        positionEventsTotal: data.positionEventsTotal,
        openPositionsOffset: positionsResult.window.offset,
        openPositionsLimit: positionsResult.window.limit,
        openPositionsHasMore: positionsResult.window.hasMore,
        liveActiveWallets: liveActiveWindow.returned,
        liveActiveWalletsTotal: data.liveActiveWallets.length,
        liveGeneratedAt: data.liveGeneratedAt,
        leaderboardGeneratedAt: data.leaderboardGeneratedAt,
        portfolioGeneratedAt: data.detailGeneratedAt,
        walletUniverseSource: "https://app.pacifica.fi/leaderboard?timeframe=allTime&sortField=pnl&sortDir=desc",
        walletPerformanceSource: "https://app.pacifica.fi/portfolio/",
      },
      publicTrades: [],
      accountTrades: [],
      positions: positionsResult.window.rows,
      liveActiveWallets: liveActiveWindow.rows,
      walletPerformance: walletWindow.rows,
      accountOverview: null,
    });
    return true;
  }

  if (pathname === "/api/live-trades/metrics" || pathname === "/api/live-trades/wallet-first/metrics") {
    sendJson(res, 200, buildMetrics(data));
    return true;
  }

  if (pathname === "/api/live-trades/wallet-first/positions") {
    const positionsResult = buildPositionsRows(data, parsedUrl.searchParams);
    sendJson(res, 200, {
      generatedAt: Date.now(),
      mode: "leaderboard_portfolio_live_trade_v1",
      status: {
        enabled: true,
        feedMode: positionsResult.feedMode,
      },
      openPositionsTotal: positionsResult.total,
      walletsWithOpenPositions: data.walletsWithLiveOpenPositions,
      summary: {
        openPositions: positionsResult.window.returned,
        openPositionsTotal: positionsResult.total,
        positionEventsTotal: data.positionEventsTotal,
        openPositionsOffset: positionsResult.window.offset,
        openPositionsLimit: positionsResult.window.limit,
        openPositionsHasMore: positionsResult.window.hasMore,
        feedMode: positionsResult.feedMode,
      },
      positions: positionsResult.window.rows,
    });
    return true;
  }

  if (/^\/api\/live-trades\/wallet\/[^/]+$/i.test(pathname)) {
    const wallet = decodeURIComponent(String(pathname).split("/").pop() || "").trim();
    const payload = buildWalletDetail(data, wallet);
    if (!payload) {
      sendJson(res, 404, {
        generatedAt: Date.now(),
        found: false,
        wallet,
      });
      return true;
    }
    sendJson(res, 200, payload);
    return true;
  }

  return false;
}

const server = http.createServer((req, res) => {
  const parsedUrl = new url.URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET,OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
    });
    res.end();
    return;
  }
  res.setHeader("Access-Control-Allow-Origin", "*");
  if (handleApiRequest(req, res, parsedUrl)) return;
  sendJson(res, 404, {
    error: "Not found",
    path: parsedUrl.pathname,
  });
});

server.listen(PORT, HOST, () => {
  console.log(
    JSON.stringify({
      ok: true,
      mode: "leaderboard_portfolio_live_trade_v1",
      host: HOST,
      port: PORT,
      baseDir: BASE_DIR,
    })
  );
});
