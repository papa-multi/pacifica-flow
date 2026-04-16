#!/usr/bin/env node
"use strict";

const fs = require("fs");
const http = require("http");
const https = require("https");
const path = require("path");
const url = require("url");
let HttpsProxyAgent = null;
try {
  ({ HttpsProxyAgent } = require("https-proxy-agent"));
} catch {
  HttpsProxyAgent = null;
}

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
const ROOT_PUBLIC_DIR = path.join(__dirname, "..", "public");
const LIVE_UI_DIR = path.join(__dirname, "..", "public", "live-trade");
const WALLET_PERFORMANCE_UI_DIR = path.join(__dirname, "..", "public", "wallet-performance");
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
const PACIFICA_PUBLIC_BASE_URL = String(
  process.env.PACIFICA_PUBLIC_BASE_URL || "https://app.pacifica.fi"
).replace(/\/+$/, "");
const WALLET_PERFORMANCE_REMOTE_DETAIL_BASE = String(
  process.env.PACIFICA_WALLET_PERFORMANCE_REMOTE_DETAIL_BASE ||
    process.env.PACIFICA_LIVE_TRADE_PUBLIC_BASE ||
    ""
)
  .trim()
  .replace(/\/+$/, "");
const PACIFICA_MULTI_EGRESS_PROXY_FILE = process.env.PACIFICA_MULTI_EGRESS_PROXY_FILE
  ? path.resolve(process.env.PACIFICA_MULTI_EGRESS_PROXY_FILE)
  : path.join(__dirname, "..", "data", "live_positions", "positions_proxies.txt");
const PACIFICA_WALLET_PERFORMANCE_PROXY_FILE = process.env.PACIFICA_WALLET_PERFORMANCE_PROXY_FILE
  ? path.resolve(process.env.PACIFICA_WALLET_PERFORMANCE_PROXY_FILE)
  : PACIFICA_MULTI_EGRESS_PROXY_FILE;
const PACIFICA_WALLET_PERFORMANCE_PROXY_ENABLED = Object.prototype.hasOwnProperty.call(
  process.env,
  "PACIFICA_WALLET_PERFORMANCE_PROXY_ENABLED"
)
  ? String(process.env.PACIFICA_WALLET_PERFORMANCE_PROXY_ENABLED || "false").toLowerCase() === "true"
  : fs.existsSync(PACIFICA_WALLET_PERFORMANCE_PROXY_FILE);
const PACIFICA_WALLET_PERFORMANCE_PROXY_INCLUDE_DIRECT =
  String(process.env.PACIFICA_WALLET_PERFORMANCE_PROXY_INCLUDE_DIRECT || "false").toLowerCase() ===
  "true";
const PACIFICA_WALLET_PERFORMANCE_PROXY_MAX = Math.max(
  0,
  Number(process.env.PACIFICA_WALLET_PERFORMANCE_PROXY_MAX || 0)
);
const PACIFICA_WALLET_PERFORMANCE_PROXY_RELOAD_MS = Math.max(
  5000,
  Number(process.env.PACIFICA_WALLET_PERFORMANCE_PROXY_RELOAD_MS || 30 * 1000)
);
const PACIFICA_WALLET_PERFORMANCE_JINA_FALLBACK_ENABLED =
  String(process.env.PACIFICA_WALLET_PERFORMANCE_JINA_FALLBACK_ENABLED || "true").toLowerCase() !==
  "false";
const PACIFICA_PUBLIC_TIMEOUT_MS = Math.max(
  3000,
  Number(process.env.PACIFICA_PUBLIC_SOURCE_TIMEOUT_MS || 15000)
);
const WALLET_PERFORMANCE_DETAIL_CACHE_TTL_MS = Math.max(
  5000,
  Number(process.env.PACIFICA_WALLET_PERFORMANCE_DETAIL_CACHE_TTL_MS || 90 * 1000)
);
const WALLET_PERFORMANCE_DETAIL_SOURCE_TIMEOUT_MS = Math.max(
  1500,
  Number(
    process.env.PACIFICA_WALLET_PERFORMANCE_DETAIL_SOURCE_TIMEOUT_MS ||
      Math.min(PACIFICA_PUBLIC_TIMEOUT_MS, 3500)
  )
);
const WALLET_PERFORMANCE_DETAIL_SOURCE_ATTEMPTS = Math.max(
  1,
  Number(process.env.PACIFICA_WALLET_PERFORMANCE_DETAIL_SOURCE_ATTEMPTS || 1)
);
const WALLET_PERFORMANCE_DETAIL_CRITICAL_TIMEOUT_MS = Math.max(
  1200,
  Number(
    process.env.PACIFICA_WALLET_PERFORMANCE_DETAIL_CRITICAL_TIMEOUT_MS ||
      WALLET_PERFORMANCE_DETAIL_SOURCE_TIMEOUT_MS
  )
);
const WALLET_PERFORMANCE_DETAIL_NONCRITICAL_TIMEOUT_MS = Math.max(
  900,
  Number(
    process.env.PACIFICA_WALLET_PERFORMANCE_DETAIL_NONCRITICAL_TIMEOUT_MS ||
      Math.min(WALLET_PERFORMANCE_DETAIL_SOURCE_TIMEOUT_MS, 1800)
  )
);
const WALLET_PERFORMANCE_DETAIL_CRITICAL_ATTEMPTS = Math.max(
  1,
  Number(
    process.env.PACIFICA_WALLET_PERFORMANCE_DETAIL_CRITICAL_ATTEMPTS ||
      WALLET_PERFORMANCE_DETAIL_SOURCE_ATTEMPTS
  )
);
const WALLET_PERFORMANCE_DETAIL_NONCRITICAL_ATTEMPTS = Math.max(
  1,
  Number(process.env.PACIFICA_WALLET_PERFORMANCE_DETAIL_NONCRITICAL_ATTEMPTS || 1)
);
const WALLET_PERFORMANCE_PRICE_CACHE_TTL_MS = Math.max(
  1000,
  Number(process.env.PACIFICA_WALLET_PERFORMANCE_PRICE_CACHE_TTL_MS || 5000)
);
const WALLET_PERFORMANCE_HISTORY_PAGE_LIMIT = Math.max(
  1,
  Number(process.env.PACIFICA_WALLET_PERFORMANCE_HISTORY_PAGE_LIMIT || 4)
);
const WALLET_PERFORMANCE_HISTORY_ROW_LIMIT = Math.max(
  25,
  Number(process.env.PACIFICA_WALLET_PERFORMANCE_HISTORY_ROW_LIMIT || 200)
);
const WALLET_PERFORMANCE_HISTORY_DETAIL_PAGE_LIMIT = Math.max(
  1,
  Number(process.env.PACIFICA_WALLET_PERFORMANCE_HISTORY_DETAIL_PAGE_LIMIT || 2)
);
const WALLET_PERFORMANCE_RANGE_CACHE_TTL_MS = Math.max(
  5000,
  Number(process.env.PACIFICA_WALLET_PERFORMANCE_RANGE_CACHE_TTL_MS || 2 * 60 * 1000)
);
const WALLET_PERFORMANCE_SUMMARY_ENRICH_CONCURRENCY = Math.max(
  1,
  Number(process.env.PACIFICA_WALLET_PERFORMANCE_SUMMARY_ENRICH_CONCURRENCY || 6)
);
const WALLET_PERFORMANCE_SUMMARY_REMOTE_ENRICH_ENABLED =
  String(process.env.PACIFICA_WALLET_PERFORMANCE_SUMMARY_REMOTE_ENRICH_ENABLED || "false")
    .trim()
    .toLowerCase() === "true";
const WALLET_PERFORMANCE_MIN_TIMELINE_POINTS = Math.max(
  3,
  Number(process.env.PACIFICA_WALLET_PERFORMANCE_MIN_TIMELINE_POINTS || 16)
);
const WALLET_PERFORMANCE_DETAIL_PREFETCH_ENABLED =
  String(process.env.PACIFICA_WALLET_PERFORMANCE_DETAIL_PREFETCH_ENABLED || "false").toLowerCase() !==
  "false";
const WALLET_PERFORMANCE_DETAIL_PREFETCH_INTERVAL_MS = Math.max(
  1000,
  Number(process.env.PACIFICA_WALLET_PERFORMANCE_DETAIL_PREFETCH_INTERVAL_MS || 1000)
);
const WALLET_PERFORMANCE_DETAIL_PREFETCH_BATCH_SIZE = Math.max(
  1,
  Number(process.env.PACIFICA_WALLET_PERFORMANCE_DETAIL_PREFETCH_BATCH_SIZE || 48)
);
const WALLET_PERFORMANCE_DETAIL_PREFETCH_CONCURRENCY = Math.max(
  1,
  Number(process.env.PACIFICA_WALLET_PERFORMANCE_DETAIL_PREFETCH_CONCURRENCY || 16)
);
const WALLET_PERFORMANCE_DETAIL_PREFETCH_LIMIT = Math.max(
  0,
  Number(process.env.PACIFICA_WALLET_PERFORMANCE_DETAIL_PREFETCH_LIMIT || 0)
);

const dataCache = {
  loadedAt: 0,
  ttlMs: DATA_CACHE_TTL_MS,
  value: null,
};
const streamState = {
  lastBroadcastSignature: "",
};
const walletPerformanceDetailCache = new Map();
const walletPerformanceDetailInflight = new Map();
const walletPerformanceRangeCache = new Map();
const walletPerformanceRangeInflight = new Map();
const pacificaPriceCache = {
  loadedAt: 0,
  value: null,
  inflight: null,
};
const pacificaProxyState = {
  loadedAt: 0,
  rows: [],
  cursor: 0,
};
const walletPerformancePrefetchState = {
  running: false,
  nextIndex: 0,
  lastStartedAt: 0,
  lastFinishedAt: 0,
  lastDurationMs: 0,
  lastBatchSize: 0,
  lastSuccessCount: 0,
  lastErrorCount: 0,
  timer: null,
};
const pacificaProxyAgentCache = new Map();

function readJson(filePath, fallback = null) {
  try {
    if (!fs.existsSync(filePath)) return fallback;
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return fallback;
  }
}

function normalizeProxyUrl(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  if (/^https?:\/\//i.test(raw)) return raw;
  return `http://${raw}`;
}

function loadPacificaProxyList(force = false) {
  const now = Date.now();
  if (
    !force &&
    pacificaProxyState.loadedAt > 0 &&
    now - pacificaProxyState.loadedAt <= PACIFICA_WALLET_PERFORMANCE_PROXY_RELOAD_MS
  ) {
    return pacificaProxyState.rows;
  }
  let rows = [];
  try {
    if (fs.existsSync(PACIFICA_WALLET_PERFORMANCE_PROXY_FILE)) {
      const raw = fs.readFileSync(PACIFICA_WALLET_PERFORMANCE_PROXY_FILE, "utf8");
      rows = raw
        .split(/\r?\n/)
        .map((line) => normalizeProxyUrl(line))
        .filter(Boolean);
    }
  } catch {
    rows = [];
  }
  if (PACIFICA_WALLET_PERFORMANCE_PROXY_MAX > 0) {
    rows = rows.slice(0, PACIFICA_WALLET_PERFORMANCE_PROXY_MAX);
  }
  pacificaProxyState.loadedAt = now;
  pacificaProxyState.rows = rows;
  if (pacificaProxyState.cursor >= rows.length) {
    pacificaProxyState.cursor = 0;
  }
  return rows;
}

function pickPacificaProxyUrl() {
  const rows = loadPacificaProxyList(false);
  if (!Array.isArray(rows) || rows.length === 0) return "";
  const index = pacificaProxyState.cursor % rows.length;
  pacificaProxyState.cursor = (pacificaProxyState.cursor + 1) % rows.length;
  return rows[index] || "";
}

function getPacificaProxyAgent(proxyUrl) {
  const normalized = normalizeProxyUrl(proxyUrl);
  if (!normalized || !HttpsProxyAgent) return null;
  if (pacificaProxyAgentCache.has(normalized)) {
    return pacificaProxyAgentCache.get(normalized);
  }
  try {
    const agent = new HttpsProxyAgent(normalized);
    pacificaProxyAgentCache.set(normalized, agent);
    return agent;
  } catch {
    return null;
  }
}

function shouldUsePacificaProxy(targetUrl) {
  if (!PACIFICA_WALLET_PERFORMANCE_PROXY_ENABLED) return false;
  if (!HttpsProxyAgent) return false;
  const hostname = String(targetUrl && targetUrl.hostname ? targetUrl.hostname : "").toLowerCase();
  if (!hostname) return false;
  return hostname.includes("pacifica.fi");
}

function isPacificaRequestUrl(requestUrl) {
  try {
    const target = new URL(requestUrl);
    return String(target.hostname || "").toLowerCase().includes("pacifica.fi");
  } catch {
    return false;
  }
}

function fetchText(requestUrl, timeoutMs = PACIFICA_PUBLIC_TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    const target = new URL(requestUrl);
    const transport = target.protocol === "http:" ? http : https;
    const req = transport.get(
      target,
      {
        headers: {
          accept: "text/plain,application/json;q=0.9,*/*;q=0.8",
          "user-agent": "pacifica-flow-wallet-performance/1.0",
        },
      },
      (response) => {
        let body = "";
        response.setEncoding("utf8");
        response.on("data", (chunk) => {
          body += chunk;
        });
        response.on("end", () => {
          if (response.statusCode && response.statusCode >= 400) {
            reject(new Error(`HTTP ${response.statusCode}: ${body.slice(0, 500)}`));
            return;
          }
          resolve(body);
        });
      }
    );
    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error(`timeout after ${timeoutMs}ms`));
    });
    req.on("error", reject);
  });
}

function parseJinaMirroredJson(text) {
  const raw = String(text || "");
  const attempts = [];
  const direct = raw.trim();
  if (direct) attempts.push(direct);
  const marker = "Markdown Content:";
  const markerIndex = raw.indexOf(marker);
  if (markerIndex >= 0) {
    const afterMarker = raw.slice(markerIndex + marker.length).trim();
    if (afterMarker) attempts.push(afterMarker);
  }
  for (const candidate of attempts) {
    const cleaned = candidate
      .replace(/^```json\s*/i, "")
      .replace(/^```\s*/i, "")
      .replace(/\s*```$/i, "")
      .trim();
    if (!cleaned) continue;
    const firstObject = cleaned.indexOf("{");
    const firstArray = cleaned.indexOf("[");
    const startIndex = [firstObject, firstArray]
      .filter((value) => value >= 0)
      .reduce((min, value) => (value < min ? value : min), Number.MAX_SAFE_INTEGER);
    const parseTargets =
      Number.isFinite(startIndex) && startIndex !== Number.MAX_SAFE_INTEGER
        ? [cleaned, cleaned.slice(startIndex)]
        : [cleaned];
    for (const parseTarget of parseTargets) {
      try {
        return JSON.parse(parseTarget);
      } catch {
        // continue
      }
    }
  }
  throw new Error("invalid_jina_mirror_payload");
}

async function fetchJsonViaJinaMirror(requestUrl, timeoutMs = PACIFICA_PUBLIC_TIMEOUT_MS) {
  const httpSource = String(requestUrl || "").replace(/^https:\/\//i, "http://");
  const mirrorUrl = `https://r.jina.ai/${httpSource}`;
  const text = await fetchText(mirrorUrl, Math.max(timeoutMs, 10000));
  return parseJinaMirroredJson(text);
}

function toNum(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function readLiquidationPrice(value = null) {
  const safeValue = value && typeof value === "object" ? value : {};
  const raw = safeValue.raw && typeof safeValue.raw === "object" ? safeValue.raw : {};
  const candidates = [
    safeValue.liquidationPrice,
    safeValue.liquidation_price,
    safeValue.liquidation,
    safeValue.liqPrice,
    safeValue.liq_price,
    safeValue.liqPx,
    safeValue.liq_px,
    safeValue.liquidationPx,
    safeValue.liquidation_px,
    raw.liquidationPrice,
    raw.liquidation_price,
    raw.liquidation,
    raw.liqPrice,
    raw.liq_price,
    raw.liqPx,
    raw.liq_px,
    raw.liquidationPx,
    raw.liquidation_px,
  ];
  for (const candidate of candidates) {
    const numeric = Number(candidate);
    if (Number.isFinite(numeric) && numeric > 0) return numeric;
  }
  return null;
}

function parseFlagParam(rawValue, fallback = false) {
  if (rawValue === undefined || rawValue === null) return Boolean(fallback);
  const value = String(rawValue).trim().toLowerCase();
  if (!value) return Boolean(fallback);
  if (["1", "true", "yes", "on", "y"].includes(value)) return true;
  if (["0", "false", "no", "off", "n"].includes(value)) return false;
  return Boolean(fallback);
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

function comparePositionRows(left, right, sortKey = "detectedAt", sortDir = "desc") {
  const primary = compareNumbers(left && left[sortKey], right && right[sortKey], sortDir);
  if (primary !== 0) return primary;

  const timestampKeys = ["detectedAt", "observedAt", "updatedAt", "openedAt", "timestamp"];
  for (const key of timestampKeys) {
    if (key === sortKey) continue;
    const next = compareNumbers(left && left[key], right && right[key], "desc");
    if (next !== 0) return next;
  }

  const walletCompare = compareStrings(left && left.wallet, right && right.wallet, "asc");
  if (walletCompare !== 0) return walletCompare;
  const symbolCompare = compareStrings(left && left.symbol, right && right.symbol, "asc");
  if (symbolCompare !== 0) return symbolCompare;
  const sideCompare = compareStrings(left && left.side, right && right.side, "asc");
  if (sideCompare !== 0) return sideCompare;
  return compareStrings(
    left && (left.positionKey || left.key || ""),
    right && (right.positionKey || right.key || ""),
    "asc"
  );
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

function serveStaticFile(req, res, filePath) {
  if (!fs.existsSync(filePath)) return false;
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
  sendText(res, 200, contentTypeForLiveUi(filePath), body, cacheControl);
  return true;
}

function serveUiMount(req, res, parsedUrl, mountPath, uiDir) {
  if (parseFlagParam(process.env.PACIFICA_LIVE_TRADE_DISABLE_UI, false)) {
    return false;
  }
  if (req.method !== "GET" && req.method !== "HEAD") return false;
  const pathname = String(parsedUrl.pathname || "");
  const mountRoot = `/${String(mountPath || "").replace(/^\/+|\/+$/g, "")}`;
  if (!mountRoot || mountRoot === "/") return false;
  if (pathname === mountRoot) {
    res.writeHead(308, {
      Location: `${mountRoot}/`,
      "Cache-Control": "no-store",
    });
    res.end();
    return true;
  }
  if (!pathname.startsWith(mountRoot)) return false;
  const relativePath =
    pathname === mountRoot || pathname === `${mountRoot}/`
      ? "index.html"
      : pathname.replace(new RegExp(`^${mountRoot.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\/+`), "");
  const safePath = path.normalize(relativePath).replace(/^\.\.(\/|\\|$)+/, "");
  const filePath = path.join(uiDir, safePath);
  if (!filePath.startsWith(uiDir)) return false;
  return serveStaticFile(req, res, filePath);
}

function serveLiveTradeUi(req, res, parsedUrl) {
  return serveUiMount(req, res, parsedUrl, "/live-trade", LIVE_UI_DIR);
}

function serveWalletPerformanceUi(req, res, parsedUrl) {
  return serveUiMount(req, res, parsedUrl, "/wallet-performance", WALLET_PERFORMANCE_UI_DIR);
}

function serveSharedPublicAsset(req, res, parsedUrl) {
  if (req.method !== "GET" && req.method !== "HEAD") return false;
  const pathname = String(parsedUrl.pathname || "");
  const allowed =
    pathname === "/favicon.png" ||
    pathname === "/apple-touch-icon.png" ||
    pathname === "/theme.css" ||
    pathname.startsWith("/assets/");
  if (!allowed) return false;
  const relativePath = pathname.replace(/^\/+/, "");
  const safePath = path.normalize(relativePath).replace(/^\.\.(\/|\\|$)+/, "");
  const filePath = path.join(ROOT_PUBLIC_DIR, safePath);
  if (!filePath.startsWith(ROOT_PUBLIC_DIR)) return false;
  return serveStaticFile(req, res, filePath);
}

function sortTimelineRows(rows = []) {
  return (Array.isArray(rows) ? rows : [])
    .filter((row) => row && typeof row === "object")
    .slice()
    .sort((left, right) => toNum(left && left.timestamp, 0) - toNum(right && right.timestamp, 0));
}

function computeSharpeRatio(rows = []) {
  const returns = [];
  for (let index = 1; index < rows.length; index += 1) {
    const previousEquity = toNum(rows[index - 1] && rows[index - 1].account_equity, NaN);
    const currentEquity = toNum(rows[index] && rows[index].account_equity, NaN);
    if (!Number.isFinite(previousEquity) || previousEquity <= 0 || !Number.isFinite(currentEquity)) {
      continue;
    }
    returns.push((currentEquity - previousEquity) / previousEquity);
  }
  if (returns.length < 2) return 0;
  const mean = returns.reduce((sum, value) => sum + value, 0) / returns.length;
  const variance =
    returns.reduce((sum, value) => sum + (value - mean) ** 2, 0) / Math.max(1, returns.length - 1);
  const stdDev = Math.sqrt(Math.max(0, variance));
  if (!Number.isFinite(stdDev) || stdDev <= 0) return 0;
  return Number(((mean / stdDev) * Math.sqrt(returns.length)).toFixed(4));
}

function computePortfolioRangeMetrics(payload) {
  const rows = sortTimelineRows(payload && payload.data);
  if (!rows.length) {
    return {
      sampleCount: 0,
      firstSeenAt: null,
      lastSeenAt: null,
      startEquityUsd: 0,
      equityUsd: 0,
      pnlUsd: 0,
      tradingPnlUsd: 0,
      returnPct: 0,
      maxDrawdownPct: 0,
      sharpeRatio: 0,
      points: [],
    };
  }
  const firstRow = rows[0] || {};
  const lastRow = rows[rows.length - 1] || {};
  const firstSeenAt = toNum(firstRow.timestamp, 0) || null;
  const lastSeenAt = toNum(lastRow.timestamp, 0) || null;
  const firstPnl = toNum(firstRow.pnl, 0);
  const lastPnl = toNum(lastRow.pnl, 0);
  const basePnl = firstPnl;
  const startEquityUsd = toNum(firstRow.account_equity, 0);
  const equityUsd = toNum(lastRow.account_equity, 0);
  const tradingPnlUsd = rows.length >= 2 ? lastPnl - firstPnl : 0;
  const pnlUsd = Number(tradingPnlUsd.toFixed(6));
  const returnPct = startEquityUsd > 0 ? Number(((pnlUsd / startEquityUsd) * 100).toFixed(6)) : 0;
  let peakEquity = startEquityUsd;
  let maxDrawdownPct = 0;
  rows.forEach((row) => {
    const equity = toNum(row && row.account_equity, 0);
    if (equity > peakEquity) peakEquity = equity;
    const drawdownPct = peakEquity > 0 ? ((peakEquity - equity) / peakEquity) * 100 : 0;
    if (drawdownPct > maxDrawdownPct) maxDrawdownPct = drawdownPct;
  });
  return {
    sampleCount: rows.length,
    firstSeenAt,
    lastSeenAt,
    startEquityUsd: Number(startEquityUsd.toFixed(6)),
    equityUsd: Number(equityUsd.toFixed(6)),
    pnlUsd,
    tradingPnlUsd: Number(tradingPnlUsd.toFixed(6)),
    returnPct,
    maxDrawdownPct: Number(maxDrawdownPct.toFixed(6)),
    sharpeRatio: computeSharpeRatio(rows),
    points: rows.map((row) => ({
      timestamp: toNum(row && row.timestamp, 0),
      accountEquityUsd: Number(toNum(row && row.account_equity, 0).toFixed(6)),
      pnlUsd: Number((toNum(row && row.pnl, 0) - basePnl).toFixed(6)),
      tradingPnlUsd: Number(toNum(row && row.pnl, 0).toFixed(6)),
    })),
  };
}

function sliceTimelineRowsByWindow(rows = [], windowMs = 0, nowMs = Date.now()) {
  const safeRows = sortTimelineRows(rows);
  if (!safeRows.length) return [];
  const durationMs = Math.max(0, Number(windowMs) || 0);
  if (!durationMs) return safeRows;
  const cutoff = nowMs - durationMs;
  let startIndex = safeRows.findIndex((row) => toNum(row && row.timestamp, 0) >= cutoff);
  if (startIndex < 0) startIndex = safeRows.length - 1;
  if (startIndex > 0) startIndex -= 1;
  return safeRows.slice(startIndex);
}

function fetchJson(requestUrl, timeoutMs = PACIFICA_PUBLIC_TIMEOUT_MS, options = {}) {
  return new Promise((resolve, reject) => {
    const target = new URL(requestUrl);
    const transport = target.protocol === "http:" ? http : https;
    const disableProxy = Boolean(options && options.disableProxy);
    const proxyEligible = !disableProxy && shouldUsePacificaProxy(target);
    const proxyUrl = proxyEligible ? pickPacificaProxyUrl() : "";
    const proxyAgent = proxyUrl ? getPacificaProxyAgent(proxyUrl) : null;
    if (proxyEligible && !proxyAgent && !PACIFICA_WALLET_PERFORMANCE_PROXY_INCLUDE_DIRECT) {
      reject(new Error("proxy_unavailable_for_pacifica_request"));
      return;
    }
    let settled = false;
    let req = null;
    const hardTimeoutId = setTimeout(() => {
      const timeoutError = new Error(`timeout after ${timeoutMs}ms`);
      finishReject(timeoutError);
      if (req && typeof req.destroy === "function") {
        try {
          req.destroy(timeoutError);
        } catch (_ignoreDestroyError) {
          // noop
        }
      }
    }, Math.max(1, timeoutMs));

    const finishReject = (error) => {
      if (settled) return;
      settled = true;
      clearTimeout(hardTimeoutId);
      reject(error);
    };

    const finishResolve = (payload) => {
      if (settled) return;
      settled = true;
      clearTimeout(hardTimeoutId);
      resolve(payload);
    };

    req = transport.get(
      target,
      {
        headers: {
          accept: "application/json",
          "user-agent": "pacifica-flow-wallet-performance/1.0",
        },
        agent: proxyAgent || undefined,
      },
      (response) => {
        let body = "";
        response.setEncoding("utf8");
        response.on("data", (chunk) => {
          body += chunk;
        });
        response.on("end", () => {
          if (response.statusCode && response.statusCode >= 400) {
            finishReject(new Error(`HTTP ${response.statusCode}: ${body.slice(0, 500)}`));
            return;
          }
          try {
            const parsed = JSON.parse(body);
            if (
              parsed &&
              typeof parsed === "object" &&
              Object.prototype.hasOwnProperty.call(parsed, "success") &&
              parsed.success === false
            ) {
              finishReject(
                new Error(
                  `Pacifica error for ${target.pathname}: ${String(parsed.error || parsed.code || "unknown")}`
                )
              );
              return;
            }
            finishResolve(parsed);
          } catch (error) {
            finishReject(error);
          }
        });
      }
    );
    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error(`timeout after ${timeoutMs}ms`));
    });
    req.on("error", (error) => {
      finishReject(error);
    });
  });
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, Math.max(0, Number(ms || 0))));
}

async function fetchJsonWithRetry(
  requestUrl,
  timeoutMs = PACIFICA_PUBLIC_TIMEOUT_MS,
  attempts = 3,
  options = {}
) {
  const safeOptions = options && typeof options === "object" ? options : {};
  const allowDirectFallback =
    safeOptions.allowDirectFallback === undefined ? true : Boolean(safeOptions.allowDirectFallback);
  const allowJinaFallback =
    safeOptions.allowJinaFallback === undefined ? PACIFICA_WALLET_PERFORMANCE_JINA_FALLBACK_ENABLED : Boolean(safeOptions.allowJinaFallback);
  let lastError = null;
  for (let attempt = 1; attempt <= Math.max(1, attempts); attempt += 1) {
    try {
      return await fetchJson(requestUrl, timeoutMs);
    } catch (error) {
      lastError = error;
      const message = String(error && error.message ? error.message : error || "").toLowerCase();
      const shouldTryDirectFallback =
        isPacificaRequestUrl(requestUrl) &&
        (message.includes("407") ||
          message.includes("429") ||
          message.includes("403") ||
          message.includes("forbidden") ||
          message.includes("rate limit") ||
          message.includes("proxy authentication") ||
          message.includes("proxy_unavailable_for_pacifica_request") ||
          message.includes("key not found in keystore"));
      if (allowDirectFallback && shouldTryDirectFallback) {
        try {
          return await fetchJson(requestUrl, timeoutMs, { disableProxy: true });
        } catch (directError) {
          lastError = directError;
        }
      }
      const isRetryable =
        message.includes("429") ||
        message.includes("timeout") ||
        message.includes("econnreset") ||
        message.includes("socket hang up");
      const shouldTryJinaFallback =
        isPacificaRequestUrl(requestUrl) &&
        (message.includes("429") ||
          message.includes("timeout") ||
          message.includes("econnreset") ||
          message.includes("socket hang up") ||
          message.includes("403") ||
          message.includes("407") ||
          message.includes("forbidden") ||
          message.includes("rate limit") ||
          message.includes("proxy authentication") ||
          message.includes("proxy_unavailable_for_pacifica_request"));
      if (allowJinaFallback && shouldTryJinaFallback) {
        try {
          return await fetchJsonViaJinaMirror(requestUrl, timeoutMs);
        } catch (mirrorError) {
          lastError = mirrorError;
        }
      }
      if (!isRetryable || attempt >= attempts) break;
      await sleep(attempt * 800);
    }
  }
  throw lastError || new Error(`request_failed:${requestUrl}`);
}

function buildPacificaUrl(pathname, params = {}) {
  const nextUrl = new URL(`${PACIFICA_PUBLIC_BASE_URL}${pathname}`);
  Object.entries(params || {}).forEach(([key, value]) => {
    if (value === undefined || value === null || value === "") return;
    nextUrl.searchParams.set(key, String(value));
  });
  return nextUrl.toString();
}

function buildRemoteWalletDetailUrl(wallet, force = false) {
  const base = WALLET_PERFORMANCE_REMOTE_DETAIL_BASE;
  if (!base) return "";
  const target = new URL(
    `${base}/api/wallet-performance/wallet/${encodeURIComponent(normalizeWallet(wallet))}`
  );
  if (force) target.searchParams.set("force", "1");
  target.searchParams.set("_remote_passthrough", "1");
  return target.toString();
}

function extractPayloadData(payload, fallback) {
  if (payload && typeof payload === "object" && Object.prototype.hasOwnProperty.call(payload, "data")) {
    return payload.data;
  }
  return fallback !== undefined ? fallback : payload;
}

function tsOrNull(value) {
  const nextValue = tsMs(value);
  return nextValue > 0 ? nextValue : null;
}

function metricNumber(value, decimals = 2) {
  const num = Number(value);
  return Number.isFinite(num) ? Number(num.toFixed(decimals)) : 0;
}

const MAX_REASONABLE_LEVERAGE = Math.max(
  25,
  Number(process.env.PACIFICA_WALLET_PERFORMANCE_MAX_LEVERAGE || 250)
);

function isReasonableLeverage(value) {
  const num = Number(value);
  return Number.isFinite(num) && num > 0 && num <= MAX_REASONABLE_LEVERAGE;
}

function pickReasonableLeverage(...candidates) {
  for (let index = 0; index < candidates.length; index += 1) {
    const candidate = toNum(candidates[index], NaN);
    if (isReasonableLeverage(candidate)) return candidate;
  }
  return NaN;
}

function buildPriceLookup(rows = []) {
  const lookup = new Map();
  (Array.isArray(rows) ? rows : []).forEach((row) => {
    const safeRow = row && typeof row === "object" ? row : {};
    const symbol = String(safeRow.symbol || "").trim().toUpperCase();
    if (!symbol) return;
    lookup.set(symbol, {
      symbol,
      mark: toNum(safeRow.mark, 0),
      oracle: toNum(safeRow.oracle, 0),
      mid: toNum(safeRow.mid, 0),
      funding: toNum(safeRow.funding, 0),
      timestamp: toNum(safeRow.timestamp, 0),
    });
  });
  return lookup;
}

function buildMarginSettingsLookup(payload) {
  const data = extractPayloadData(payload, {});
  const rows =
    data && Array.isArray(data.margin_settings)
      ? data.margin_settings
      : Array.isArray(data)
      ? data
      : [];
  const lookup = new Map();
  rows.forEach((row) => {
    const safeRow = row && typeof row === "object" ? row : {};
    const symbol = String(safeRow.symbol || "").trim().toUpperCase();
    if (!symbol) return;
    lookup.set(symbol, {
      leverage: toNum(safeRow.leverage, 0),
      isolated: safeRow.isolated === true,
      updatedAt: toNum(safeRow.updated_at || safeRow.updatedAt, 0),
    });
  });
  return lookup;
}

async function fetchPacificaPrices() {
  const now = Date.now();
  if (
    pacificaPriceCache.value &&
    now - pacificaPriceCache.loadedAt <= WALLET_PERFORMANCE_PRICE_CACHE_TTL_MS
  ) {
    return pacificaPriceCache.value;
  }
  if (pacificaPriceCache.inflight) return pacificaPriceCache.inflight;
  pacificaPriceCache.inflight = fetchJsonWithRetry(
    buildPacificaUrl("/api/v1/info/prices"),
    WALLET_PERFORMANCE_DETAIL_NONCRITICAL_TIMEOUT_MS,
    WALLET_PERFORMANCE_DETAIL_NONCRITICAL_ATTEMPTS
  )
    .then((payload) => {
      const rows = extractPayloadData(payload, []);
      const nextValue = buildPriceLookup(rows);
      pacificaPriceCache.loadedAt = Date.now();
      pacificaPriceCache.value = nextValue;
      pacificaPriceCache.inflight = null;
      return nextValue;
    })
    .catch((error) => {
      pacificaPriceCache.inflight = null;
      if (pacificaPriceCache.value) return pacificaPriceCache.value;
      throw error;
    });
  return pacificaPriceCache.inflight;
}

async function fetchPacificaBalanceHistory(wallet, options = {}) {
  const normalizedWallet = normalizeWallet(wallet);
  if (!normalizedWallet) return [];
  const safeOptions = options && typeof options === "object" ? options : {};
  const timeoutMs = Math.max(
    900,
    Number(
      safeOptions.timeoutMs !== undefined
        ? safeOptions.timeoutMs
        : WALLET_PERFORMANCE_DETAIL_NONCRITICAL_TIMEOUT_MS
    )
  );
  const attempts = Math.max(
    1,
    Number(
      safeOptions.attempts !== undefined
        ? safeOptions.attempts
        : WALLET_PERFORMANCE_DETAIL_NONCRITICAL_ATTEMPTS
    )
  );
  const pageLimit = Math.max(
    1,
    Number(
      safeOptions.pageLimit !== undefined
        ? safeOptions.pageLimit
        : WALLET_PERFORMANCE_HISTORY_DETAIL_PAGE_LIMIT
    )
  );
  const rowLimit = Math.max(
    25,
    Number(
      safeOptions.rowLimit !== undefined
        ? safeOptions.rowLimit
        : WALLET_PERFORMANCE_HISTORY_ROW_LIMIT
    )
  );
  const fetchOptions =
    safeOptions.fetchOptions && typeof safeOptions.fetchOptions === "object"
      ? safeOptions.fetchOptions
      : undefined;
  const rows = [];
  let cursor = null;
  for (
    let pageIndex = 0;
    pageIndex < pageLimit &&
    rows.length < rowLimit;
    pageIndex += 1
  ) {
    const payload = await fetchJsonWithRetry(
      buildPacificaUrl("/api/v1/account/balance/history", {
        account: normalizedWallet,
        cursor,
      }),
      timeoutMs,
      attempts,
      fetchOptions
    );
    const pageRows = Array.isArray(extractPayloadData(payload, [])) ? extractPayloadData(payload, []) : [];
    rows.push(...pageRows);
    const hasMore = Boolean(payload && payload.has_more);
    const nextCursor = payload && payload.next_cursor ? String(payload.next_cursor) : "";
    if (!hasMore || !nextCursor) break;
    cursor = nextCursor;
  }
  const normalizedRows = rows
    .map((row) => (row && typeof row === "object" ? row : {}))
    .filter((row) => Object.keys(row).length > 0);
  const fundingRows = normalizedRows.filter((row) => {
    const eventType = String(row.event_type || row.type || row.kind || "").trim().toLowerCase();
    return (
      eventType.includes("deposit") ||
      eventType.includes("withdraw") ||
      eventType.includes("credit") ||
      eventType.includes("debit") ||
      eventType.includes("inflow") ||
      eventType.includes("outflow")
    );
  });
  const selectedRows = (fundingRows.length ? fundingRows : normalizedRows).slice(
    0,
    rowLimit
  );
  return selectedRows
    .map((row) => {
      const safeRow = row && typeof row === "object" ? row : {};
      const amount = toNum(
        safeRow.amount != null
          ? safeRow.amount
          : safeRow.delta != null
          ? safeRow.delta
          : safeRow.change != null
          ? safeRow.change
          : safeRow.usdc_change,
        0
      );
      const balance = toNum(
        safeRow.balance != null ? safeRow.balance : safeRow.balance_after,
        0
      );
      const pendingBalance = toNum(
        safeRow.pending_balance != null ? safeRow.pending_balance : safeRow.pendingBalance,
        0
      );
      return {
        eventType: String(safeRow.event_type || safeRow.type || safeRow.kind || "")
          .trim()
          .toLowerCase(),
        amount: metricNumber(amount, 6),
        balance: metricNumber(balance, 6),
        pendingBalance: metricNumber(pendingBalance, 6),
        createdAt: tsOrNull(
          safeRow.created_at || safeRow.createdAt || safeRow.timestamp || safeRow.time
        ),
      };
    })
    .sort((left, right) => compareNumbers(left.createdAt, right.createdAt, "desc"));
}

function buildWalletPerformancePositionRows({
  wallet,
  positionsPayload,
  priceLookup,
  settingsLookup,
  accountPayload,
}) {
  const normalizedWallet = normalizeWallet(wallet);
  const sourceRows = Array.isArray(extractPayloadData(positionsPayload, []))
    ? extractPayloadData(positionsPayload, [])
    : [];
  const accountData = extractPayloadData(accountPayload, {});
  const accountEquityUsd = toNum(accountData && accountData.account_equity, 0);
  const totalMarginUsedUsd = toNum(accountData && accountData.total_margin_used, 0);
  const normalizedRows = sourceRows
    .map((row) => {
      const safeRow = row && typeof row === "object" ? row : {};
      const symbol = String(safeRow.symbol || "").trim().toUpperCase();
      if (!symbol) return null;
      const size = Math.abs(toNum(safeRow.amount, 0));
      if (!Number.isFinite(size) || size <= 0) return null;
      const sideRaw = String(safeRow.side || "").trim().toLowerCase();
      const side = sideRaw === "ask" ? "short" : "long";
      const entry = toNum(safeRow.entry_price || safeRow.entryPrice, 0);
      const priceRow = priceLookup.get(symbol) || null;
      const markPrice = priceRow ? toNum(priceRow.mark, entry) : entry;
      const positionValue = size * (markPrice > 0 ? markPrice : entry);
      const marginSetting = settingsLookup.get(symbol) || null;
      const leverageDirect = toNum(safeRow.leverage, NaN);
      const leverageFromSetting = marginSetting ? toNum(marginSetting.leverage, NaN) : NaN;
      const explicitMargin = pickPositiveNumber(
        safeRow.margin,
        safeRow.marginUsd,
        safeRow.margin_usd
      );
      const explicitPnl = toNum(
        safeRow.unrealized_pnl !== undefined
          ? safeRow.unrealized_pnl
          : safeRow.unrealizedPnl !== undefined
          ? safeRow.unrealizedPnl
          : safeRow.unrealizedPnlUsd !== undefined
          ? safeRow.unrealizedPnlUsd
          : safeRow.pnl,
        NaN
      );
      return {
        safeRow,
        symbol,
        side,
        size,
        entry,
        markPrice,
        positionValue,
        marginSetting,
        leverageDirect,
        leverageFromSetting,
        explicitMargin,
        explicitPnl,
      };
    })
    .filter(Boolean);

  const totalPositionValue = normalizedRows.reduce((sum, row) => {
    return sum + Math.max(0, toNum(row && row.positionValue, 0));
  }, 0);

  return normalizedRows
    .map((row) => {
      const safeRow = row.safeRow;
      const symbol = row.symbol;
      const side = row.side;
      const size = row.size;
      const entry = row.entry;
      const markPrice = row.markPrice;
      const positionValue = row.positionValue;
      const marginSetting = row.marginSetting;
      const leverageDirect = row.leverageDirect;
      const leverageFromSetting = row.leverageFromSetting;
      const explicitPnl = row.explicitPnl;
      let leverageDerived = pickReasonableLeverage(leverageDirect, leverageFromSetting);
      let margin = row.explicitMargin;
      let marginEstimated = false;
      const impliedLeverage =
        Number.isFinite(margin) && margin > 0 && Number.isFinite(positionValue) && positionValue > 0
          ? positionValue / margin
          : NaN;
      if (!Number.isFinite(leverageDerived) && isReasonableLeverage(impliedLeverage)) {
        leverageDerived = impliedLeverage;
      }
      if (
        Number.isFinite(leverageDerived) &&
        leverageDerived > 0 &&
        Number.isFinite(positionValue) &&
        positionValue > 0 &&
        Number.isFinite(margin) &&
        margin > 0 &&
        !isReasonableLeverage(impliedLeverage)
      ) {
        margin = positionValue / leverageDerived;
        marginEstimated = true;
      }
      if ((!Number.isFinite(margin) || margin <= 0) && Number.isFinite(leverageDerived) && leverageDerived > 0) {
        margin = positionValue / leverageDerived;
        marginEstimated = true;
      }
      if (
        (!Number.isFinite(margin) || margin <= 0) &&
        totalMarginUsedUsd > 0 &&
        totalPositionValue > 0 &&
        positionValue > 0
      ) {
        margin = (positionValue / totalPositionValue) * totalMarginUsedUsd;
        marginEstimated = true;
      }
      if ((!Number.isFinite(margin) || margin <= 0) && accountEquityUsd > 0) {
        margin = Math.min(positionValue, accountEquityUsd);
        marginEstimated = true;
      }
      if (!Number.isFinite(margin) || margin < 0) {
        margin = 0;
      }
      const leverage =
        Number.isFinite(leverageDerived) && leverageDerived > 0
          ? leverageDerived
          : margin > 0 && positionValue > 0 && isReasonableLeverage(positionValue / margin)
          ? positionValue / margin
          : 1;
      const rawPnl = (markPrice - entry) * size;
      const pnlUsd = Number.isFinite(explicitPnl)
        ? explicitPnl
        : side === "short"
        ? -rawPnl
        : rawPnl;
      const roiPct = margin > 0 ? (pnlUsd / margin) * 100 : 0;
      return {
        wallet: normalizedWallet,
        symbol,
        side,
        sideLabel: side === "short" ? "Short" : "Long",
        size: metricNumber(size, 6),
        leverage: metricNumber(leverage, 2),
        positionValueUsd: metricNumber(positionValue, 2),
        entryPrice: metricNumber(entry, 6),
        markPrice: metricNumber(markPrice, 6),
        pnlUsd: metricNumber(pnlUsd, 2),
        roiPct: metricNumber(roiPct, 2),
        liquidationPrice: (() => {
          const liquidationPrice = readLiquidationPrice(safeRow);
          return Number.isFinite(liquidationPrice) ? metricNumber(liquidationPrice, 6) : null;
        })(),
        marginUsd: metricNumber(margin, 2),
        marginMode: safeRow.isolated === true || (marginSetting && marginSetting.isolated) ? "Isolated" : "Cross",
        fundingUsd: metricNumber(toNum(safeRow.funding, 0), 6),
        createdAt: tsOrNull(safeRow.created_at || safeRow.createdAt),
        updatedAt: tsOrNull(safeRow.updated_at || safeRow.updatedAt),
        marginEstimated,
      };
    })
    .filter(Boolean)
    .sort((left, right) => compareNumbers(left.positionValueUsd, right.positionValueUsd, "desc"));
}

function buildWalletPerformanceSummaryRows(data, searchParams) {
  const rows = Array.isArray(data.walletPerformance) ? data.walletPerformance.slice() : [];
  const q = String(searchParams.get("q") || "").trim().toLowerCase();
  const exactWallet = normalizeWallet(searchParams.get("wallet"));
  const minPnl7d = searchParams.get("min_pnl_7d");
  const maxPnl7d = searchParams.get("max_pnl_7d");
  const minPnl30d = searchParams.get("min_pnl_30d");
  const maxPnl30d = searchParams.get("max_pnl_30d");
  const minTotalPnl = searchParams.get("min_total_pnl");
  const maxTotalPnl = searchParams.get("max_total_pnl");
  const minOpenPositions = searchParams.get("min_open_positions");
  const maxOpenPositions = searchParams.get("max_open_positions");
  const minEquity = searchParams.get("min_equity");
  const maxEquity = searchParams.get("max_equity");
  const sortKey = String(searchParams.get("sort") || "totalPnlUsd").trim() || "totalPnlUsd";
  const sortDir =
    String(searchParams.get("dir") || "desc").trim().toLowerCase() === "asc" ? "asc" : "desc";
  const page = Math.max(1, Number(searchParams.get("page") || 1) || 1);
  const pageSize = Math.max(1, Math.min(100, Number(searchParams.get("page_size") || 20) || 20));

  const matchMin = (value, raw) => raw === null || raw === "" || toNum(value, NaN) >= toNum(raw, NaN);
  const matchMax = (value, raw) => raw === null || raw === "" || toNum(value, NaN) <= toNum(raw, NaN);

  let filtered = rows.filter((row) => {
    if (exactWallet && row.wallet !== exactWallet) return false;
    if (q) {
      const haystack = `${String(row.wallet || "").toLowerCase()} ${String(row.walletLabel || "").toLowerCase()}`;
      if (!haystack.includes(q)) return false;
    }
    if (!matchMin(row.pnl7d, minPnl7d) || !matchMax(row.pnl7d, maxPnl7d)) return false;
    if (!matchMin(row.pnl30d, minPnl30d) || !matchMax(row.pnl30d, maxPnl30d)) return false;
    if (!matchMin(row.totalPnlUsd, minTotalPnl) || !matchMax(row.totalPnlUsd, maxTotalPnl)) return false;
    if (
      !matchMin(row.displayOpenPositions, minOpenPositions) ||
      !matchMax(row.displayOpenPositions, maxOpenPositions)
    ) {
      return false;
    }
    if (!matchMin(row.accountEquityUsd, minEquity) || !matchMax(row.accountEquityUsd, maxEquity)) return false;
    return true;
  });

  filtered.sort((left, right) => {
    if (sortKey === "wallet") return compareStrings(left.wallet, right.wallet, sortDir);
    return compareNumbers(left[sortKey], right[sortKey], sortDir);
  });

  const totalRows = filtered.length;
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize));
  const safePage = Math.min(page, totalPages);
  const offset = (safePage - 1) * pageSize;
  const windowed = applyWindow(filtered, offset, pageSize);
  return {
    page: safePage,
    pageSize,
    totalRows,
    totalPages,
    rows: windowed.rows,
    offset: windowed.offset,
    returned: windowed.returned,
    hasMore: windowed.hasMore,
  };
}

async function mapWithConcurrency(items = [], concurrency = 4, worker) {
  const list = Array.isArray(items) ? items : [];
  const nextWorker = typeof worker === "function" ? worker : async () => {};
  if (!list.length) return;
  const width = Math.max(1, Math.min(list.length, Number(concurrency) || 1));
  let cursor = 0;
  const runners = Array.from({ length: width }, async () => {
    while (true) {
      const index = cursor;
      cursor += 1;
      if (index >= list.length) return;
      await nextWorker(list[index], index);
    }
  });
  await Promise.all(runners);
}

function applyWalletPerformanceRangeOverrides(row, overrides) {
  if (!row || typeof row !== "object" || !overrides || typeof overrides !== "object") return row;
  const nextRow = row;
  if (Number.isFinite(toNum(overrides.pnl7d, NaN))) {
    const pnl7d = toNum(overrides.pnl7d, 0);
    nextRow.pnl7d = pnl7d;
    nextRow.leaderboardPnl7d = pnl7d;
  }
  if (Number.isFinite(toNum(overrides.pnl30d, NaN))) {
    const pnl30d = toNum(overrides.pnl30d, 0);
    nextRow.pnl30d = pnl30d;
    nextRow.leaderboardPnl30d = pnl30d;
  }
  nextRow.metricSources =
    nextRow.metricSources && typeof nextRow.metricSources === "object"
      ? { ...nextRow.metricSources }
      : {};
  nextRow.metricSources.pnl7d = "portfolio_endpoint_range_delta";
  nextRow.metricSources.pnl30d = "portfolio_endpoint_range_delta";
  return nextRow;
}

function buildWalletPerformanceRangePayload({
  wallet,
  generatedAt = Date.now(),
  pnl7d = 0,
  pnl30d = 0,
}) {
  const normalizedWallet = normalizeWallet(wallet);
  if (!normalizedWallet) return null;
  return {
    generatedAt: Math.max(0, toNum(generatedAt, Date.now())),
    wallet: normalizedWallet,
    pnl7d: metricNumber(toNum(pnl7d, 0), 6),
    pnl30d: metricNumber(toNum(pnl30d, 0), 6),
  };
}

async function fetchWalletPerformanceRangeMetrics(wallet, force = false) {
  const normalizedWallet = normalizeWallet(wallet);
  if (!normalizedWallet) return null;
  const cached = walletPerformanceRangeCache.get(normalizedWallet) || null;
  if (
    !force &&
    cached &&
    cached.payload &&
    Date.now() - toNum(cached.loadedAt, 0) <= WALLET_PERFORMANCE_RANGE_CACHE_TTL_MS
  ) {
    return cached.payload;
  }
  if (walletPerformanceRangeInflight.has(normalizedWallet)) {
    return walletPerformanceRangeInflight.get(normalizedWallet);
  }
  const requestPromise = (async () => {
    try {
      const attempts = Math.max(3, WALLET_PERFORMANCE_DETAIL_SOURCE_ATTEMPTS);
      const [timeline7dPayload, timeline30dPayload] = await Promise.all([
        fetchJsonWithRetry(
          buildPacificaUrl("/api/v1/portfolio", { account: normalizedWallet, time_range: "7d" }),
          WALLET_PERFORMANCE_DETAIL_SOURCE_TIMEOUT_MS,
          attempts
        ),
        fetchJsonWithRetry(
          buildPacificaUrl("/api/v1/portfolio", { account: normalizedWallet, time_range: "30d" }),
          WALLET_PERFORMANCE_DETAIL_SOURCE_TIMEOUT_MS,
          attempts
        ),
      ]);
      const metrics7d = computePortfolioRangeMetrics(timeline7dPayload);
      const metrics30d = computePortfolioRangeMetrics(timeline30dPayload);
      const payload = buildWalletPerformanceRangePayload({
        wallet: normalizedWallet,
        generatedAt: Date.now(),
        pnl7d: metrics7d && metrics7d.pnlUsd,
        pnl30d: metrics30d && metrics30d.pnlUsd,
      });
      walletPerformanceRangeCache.set(normalizedWallet, {
        loadedAt: Date.now(),
        payload,
      });
      return payload;
    } catch (error) {
      // Keep serving last verified range values during upstream rate limiting/outages.
      if (cached && cached.payload && typeof cached.payload === "object") {
        walletPerformanceRangeCache.set(normalizedWallet, {
          loadedAt: Date.now(),
          payload: cached.payload,
        });
        return cached.payload;
      }
      throw error;
    }
  })().finally(() => {
    walletPerformanceRangeInflight.delete(normalizedWallet);
  });
  walletPerformanceRangeInflight.set(normalizedWallet, requestPromise);
  return requestPromise;
}

async function enrichWalletPerformanceSummaryRows(rows = []) {
  const inputRows = Array.isArray(rows) ? rows : [];
  if (!inputRows.length) return [];
  const outputRows = inputRows.map((row) => ({ ...(row || {}) }));
  await mapWithConcurrency(
    outputRows,
    WALLET_PERFORMANCE_SUMMARY_ENRICH_CONCURRENCY,
    async (row) => {
      const wallet = normalizeWallet(row && row.wallet);
      if (!wallet) return;
      try {
        const overrides = await fetchWalletPerformanceRangeMetrics(wallet, false);
        applyWalletPerformanceRangeOverrides(row, overrides);
      } catch {
        // keep snapshot values when source fetch fails
      }
    }
  );
  return outputRows;
}

function applyWalletPerformanceRangeOverrideToBucket({
  bucket,
  rangeKey,
  overridePnl,
  accountEquityUsd,
  fallbackLastSeenAt,
}) {
  const overrideValue = toNum(overridePnl, NaN);
  if (!Number.isFinite(overrideValue)) return bucket;
  const safeLastSeenAt = Math.max(tsMs(fallbackLastSeenAt), Date.now());
  const safeEquity = Math.max(0, toNum(accountEquityUsd, 0));
  let nextBucket = bucket && typeof bucket === "object" ? { ...bucket } : null;
  if (!nextBucket) {
    nextBucket = buildFallbackRangeMetrics({
      pnlUsd: overrideValue,
      equityUsd: safeEquity,
      drawdownPct: 0,
      lastSeenAt: safeLastSeenAt,
      windowMs: rangeWindowMs(rangeKey),
    });
  }
  const equityUsd = Math.max(0, toNum(nextBucket.equityUsd, 0), safeEquity);
  const startEquityUsd = Math.max(0, equityUsd - overrideValue);
  nextBucket.pnlUsd = metricNumber(overrideValue, 6);
  nextBucket.tradingPnlUsd = metricNumber(overrideValue, 6);
  nextBucket.startEquityUsd = metricNumber(startEquityUsd, 6);
  nextBucket.returnPct = startEquityUsd > 0 ? metricNumber((overrideValue / startEquityUsd) * 100, 6) : 0;
  if (Array.isArray(nextBucket.points) && nextBucket.points.length > 0) {
    const lastPoint = nextBucket.points[nextBucket.points.length - 1] || {};
    const lastPointPnl = toNum(lastPoint.pnlUsd, 0);
    const pnlShift = overrideValue - lastPointPnl;
    nextBucket.points = nextBucket.points.map((point) => {
      const safePoint = point && typeof point === "object" ? point : {};
      return {
        ...safePoint,
        pnlUsd: metricNumber(toNum(safePoint.pnlUsd, 0) + pnlShift, 6),
      };
    });
  } else {
    const endTs = safeLastSeenAt;
    const startTs = Math.max(0, endTs - rangeWindowMs(rangeKey));
    nextBucket.points = [
      {
        timestamp: startTs,
        accountEquityUsd: metricNumber(startEquityUsd, 6),
        pnlUsd: 0,
      },
      {
        timestamp: endTs,
        accountEquityUsd: metricNumber(equityUsd, 6),
        pnlUsd: metricNumber(overrideValue, 6),
      },
    ];
    nextBucket.sampleCount = nextBucket.points.length;
    nextBucket.firstSeenAt = startTs;
    nextBucket.lastSeenAt = endTs;
  }
  return nextBucket;
}

function applyWalletPerformanceRangeOverridesToDetailPayload(payload, overrides) {
  if (!payload || typeof payload !== "object" || !overrides || typeof overrides !== "object") {
    return payload;
  }
  const metricsByRange =
    payload.metricsByRange && typeof payload.metricsByRange === "object"
      ? { ...payload.metricsByRange }
      : {};
  const accountEquityUsd = toNum(payload.account && payload.account.accountEquityUsd, 0);
  const fallbackLastSeenAt = Date.now();
  if (Number.isFinite(toNum(overrides.pnl7d, NaN))) {
    metricsByRange["7d"] = applyWalletPerformanceRangeOverrideToBucket({
      bucket: metricsByRange["7d"],
      rangeKey: "7d",
      overridePnl: overrides.pnl7d,
      accountEquityUsd,
      fallbackLastSeenAt,
    });
  }
  if (Number.isFinite(toNum(overrides.pnl30d, NaN))) {
    metricsByRange["30d"] = applyWalletPerformanceRangeOverrideToBucket({
      bucket: metricsByRange["30d"],
      rangeKey: "30d",
      overridePnl: overrides.pnl30d,
      accountEquityUsd,
      fallbackLastSeenAt,
    });
  }
  payload.metricsByRange = metricsByRange;
  payload.summaryReference =
    payload.summaryReference && typeof payload.summaryReference === "object"
      ? { ...payload.summaryReference }
      : {};
  if (Number.isFinite(toNum(overrides.pnl7d, NaN))) {
    payload.summaryReference.pnl7d = metricNumber(toNum(overrides.pnl7d, 0), 6);
  }
  if (Number.isFinite(toNum(overrides.pnl30d, NaN))) {
    payload.summaryReference.pnl30d = metricNumber(toNum(overrides.pnl30d, 0), 6);
  }
  return payload;
}

function findWalletPerformanceSummaryRow(dataSnapshot, wallet) {
  const normalizedWallet = normalizeWallet(wallet);
  if (!normalizedWallet) return null;
  const rows = dataSnapshot && Array.isArray(dataSnapshot.walletPerformance) ? dataSnapshot.walletPerformance : [];
  for (const row of rows) {
    if (normalizeWallet(row && row.wallet) === normalizedWallet) return row;
  }
  return null;
}

function buildFallbackRangeMetrics({
  pnlUsd = 0,
  equityUsd = 0,
  drawdownPct = 0,
  lastSeenAt = 0,
  windowMs = 0,
}) {
  const safeEquity = Math.max(0, toNum(equityUsd, 0));
  const safePnl = toNum(pnlUsd, 0);
  const safeDrawdown = Math.max(0, toNum(drawdownPct, 0));
  const endTs = Math.max(tsMs(lastSeenAt), Date.now());
  const startTs = Math.max(0, endTs - Math.max(1, toNum(windowMs, 24 * 60 * 60 * 1000)));
  const startEquity = Math.max(0, safeEquity - safePnl);
  const returnPct = startEquity > 0 ? (safePnl / startEquity) * 100 : 0;
  const points = [
    {
      timestamp: startTs,
      accountEquityUsd: metricNumber(startEquity, 6),
      pnlUsd: 0,
    },
    {
      timestamp: endTs,
      accountEquityUsd: metricNumber(safeEquity, 6),
      pnlUsd: metricNumber(safePnl, 6),
    },
  ];
  return {
    sampleCount: points.length,
    firstSeenAt: startTs,
    lastSeenAt: endTs,
    startEquityUsd: metricNumber(startEquity, 6),
    equityUsd: metricNumber(safeEquity, 6),
    pnlUsd: metricNumber(safePnl, 6),
    returnPct: metricNumber(returnPct, 6),
    maxDrawdownPct: metricNumber(safeDrawdown, 6),
    sharpeRatio: 0,
    points,
  };
}

function buildFallbackMetricsByRange(summaryRow) {
  const safeRow = summaryRow && typeof summaryRow === "object" ? summaryRow : {};
  const equityUsd = toNum(safeRow.accountEquityUsd, 0);
  const drawdownPct = toNum(safeRow.drawdownPct, 0);
  const lastSeenAt = tsMs(safeRow.lastSeenAt || safeRow.liveLastUpdatedAt || safeRow.liveLastOpenedAt || Date.now());
  return {
    "1d": {
      ...buildFallbackRangeMetrics({
        pnlUsd: toNum(safeRow.pnl1d, 0),
        equityUsd,
        drawdownPct,
        lastSeenAt,
        windowMs: 24 * 60 * 60 * 1000,
      }),
      tradingVolumeUsd: metricNumber(toNum(safeRow.volume1d, 0), 2),
    },
    "7d": {
      ...buildFallbackRangeMetrics({
        pnlUsd: toNum(safeRow.pnl7d, 0),
        equityUsd,
        drawdownPct,
        lastSeenAt,
        windowMs: 7 * 24 * 60 * 60 * 1000,
      }),
      tradingVolumeUsd: metricNumber(toNum(safeRow.volume7d, 0), 2),
    },
    "30d": {
      ...buildFallbackRangeMetrics({
        pnlUsd: toNum(safeRow.pnl30d, 0),
        equityUsd,
        drawdownPct,
        lastSeenAt,
        windowMs: 30 * 24 * 60 * 60 * 1000,
      }),
      tradingVolumeUsd: metricNumber(toNum(safeRow.volume30d, 0), 2),
    },
    all: {
      ...buildFallbackRangeMetrics({
        pnlUsd: toNum(safeRow.totalPnlUsd, 0),
        equityUsd,
        drawdownPct,
        lastSeenAt,
        windowMs: 365 * 24 * 60 * 60 * 1000,
      }),
      tradingVolumeUsd: metricNumber(toNum(safeRow.volumeAllTime, 0), 2),
    },
  };
}

function buildWalletPerformanceQuickFallbackPayload({
  wallet,
  summaryRow,
  dataSnapshot,
  reason = "warming",
  stale = false,
}) {
  const normalizedWallet = normalizeWallet(wallet);
  const safeSummary = summaryRow && typeof summaryRow === "object" ? summaryRow : {};
  const fallbackMetricsByRange = buildFallbackMetricsByRange(safeSummary);
  const fallbackPositions = buildWalletPerformanceFallbackPositions({
    wallet: normalizedWallet,
    dataSnapshot,
    accountPayload: {
      data: {
        account_equity: toNum(safeSummary.accountEquityUsd, 0),
        positions_count: Math.max(
          0,
          toNum(
            safeSummary.displayOpenPositions != null
              ? safeSummary.displayOpenPositions
              : safeSummary.openPositions,
            0
          )
        ),
      },
    },
    priceLookup: new Map(),
  });
  const positionsCount = Math.max(
    0,
    toNum(
      safeSummary.displayOpenPositions != null
        ? safeSummary.displayOpenPositions
        : safeSummary.openPositions,
      0
    ),
    Array.isArray(fallbackPositions) ? fallbackPositions.length : 0
  );
  return {
    generatedAt: Date.now(),
    wallet: normalizedWallet,
    walletLabel: shortWallet(normalizedWallet),
    account: {
      accountEquityUsd: metricNumber(toNum(safeSummary.accountEquityUsd, 0), 2),
      totalMarginUsedUsd: 0,
      availableToSpendUsd: 0,
      availableToWithdrawUsd: 0,
      positionsCount,
      updatedAt: tsOrNull(
        safeSummary.lastSeenAt || safeSummary.liveLastUpdatedAt || safeSummary.liveLastOpenedAt
      ),
    },
    metricsByRange: fallbackMetricsByRange,
    defaultRange: "30d",
    positions: fallbackPositions,
    openOrders: [],
    history: [],
    sources: {
      fallbackSummary: "walletPerformance_snapshot_row",
      fallbackLivePositions: "live_position_shards",
    },
    summaryReference: buildSummaryReference(safeSummary),
    diagnostics: {
      usedFallbackMetrics: true,
      usedFallbackPositions: true,
      quickPathReason: reason,
    },
    found: true,
    fallbackOnly: true,
    stale: Boolean(stale),
    warming: true,
  };
}

function summaryRangePnlOverrides(summaryRow) {
  const safeRow = summaryRow && typeof summaryRow === "object" ? summaryRow : {};
  return {
    "1d": toNum(safeRow.pnl1d, NaN),
    "7d": toNum(safeRow.pnl7d, NaN),
    "30d": toNum(safeRow.pnl30d, NaN),
    all: toNum(safeRow.totalPnlUsd, NaN),
  };
}

function summaryRangeVolumeOverrides(summaryRow) {
  const safeRow = summaryRow && typeof summaryRow === "object" ? summaryRow : {};
  return {
    "1d": toNum(safeRow.volume1d, NaN),
    "7d": toNum(safeRow.volume7d, NaN),
    "30d": toNum(safeRow.volume30d, NaN),
    all: toNum(safeRow.volumeAllTime, NaN),
  };
}

function rangeWindowMs(rangeKey) {
  if (rangeKey === "1d") return 24 * 60 * 60 * 1000;
  if (rangeKey === "7d") return 7 * 24 * 60 * 60 * 1000;
  if (rangeKey === "30d") return 30 * 24 * 60 * 60 * 1000;
  return 365 * 24 * 60 * 60 * 1000;
}

function timelinePointCount(metricsByRange, rangeKey) {
  const bucket =
    metricsByRange &&
    typeof metricsByRange === "object" &&
    metricsByRange[rangeKey] &&
    typeof metricsByRange[rangeKey] === "object"
      ? metricsByRange[rangeKey]
      : null;
  return Array.isArray(bucket && bucket.points) ? bucket.points.length : 0;
}

function hasDenseTimelineForRanges(
  metricsByRange,
  rangeKeys = ["7d", "30d", "all"],
  minPoints = WALLET_PERFORMANCE_MIN_TIMELINE_POINTS
) {
  const keys = Array.isArray(rangeKeys) ? rangeKeys : [];
  if (!keys.length) return false;
  return keys.every((rangeKey) => timelinePointCount(metricsByRange, rangeKey) >= Math.max(3, Number(minPoints) || 3));
}

function mergeCachedTimelineIntoMetrics({
  currentMetricsByRange,
  cachedMetricsByRange,
  minPoints = WALLET_PERFORMANCE_MIN_TIMELINE_POINTS,
}) {
  const current =
    currentMetricsByRange && typeof currentMetricsByRange === "object" ? currentMetricsByRange : {};
  const cached =
    cachedMetricsByRange && typeof cachedMetricsByRange === "object" ? cachedMetricsByRange : {};
  const next = { ...current };
  ["1d", "7d", "30d", "all"].forEach((rangeKey) => {
    const currentBucket = next[rangeKey] && typeof next[rangeKey] === "object" ? { ...next[rangeKey] } : null;
    const cachedBucket = cached[rangeKey] && typeof cached[rangeKey] === "object" ? cached[rangeKey] : null;
    const currentPoints = Array.isArray(currentBucket && currentBucket.points) ? currentBucket.points.length : 0;
    const cachedPoints = Array.isArray(cachedBucket && cachedBucket.points) ? cachedBucket.points.length : 0;
    if (cachedPoints < Math.max(3, Number(minPoints) || 3) || cachedPoints <= currentPoints) return;
    const merged = currentBucket ? { ...currentBucket } : {};
    merged.points = cachedBucket.points.map((point) => ({ ...(point || {}) }));
    merged.sampleCount = merged.points.length;
    merged.firstSeenAt = toNum(
      currentBucket && currentBucket.firstSeenAt,
      toNum(cachedBucket.firstSeenAt, 0)
    );
    merged.lastSeenAt = toNum(
      currentBucket && currentBucket.lastSeenAt,
      toNum(cachedBucket.lastSeenAt, 0)
    );
    if (!Number.isFinite(toNum(merged.sharpeRatio, NaN))) {
      merged.sharpeRatio = toNum(cachedBucket.sharpeRatio, 0);
    }
    if (!Number.isFinite(toNum(merged.maxDrawdownPct, NaN))) {
      merged.maxDrawdownPct = toNum(cachedBucket.maxDrawdownPct, 0);
    }
    next[rangeKey] = merged;
  });
  return next;
}

function alignRangeMetricsWithSummary({
  metricsByRange,
  summaryRow,
  accountEquityUsd,
  fallbackLastSeenAt,
}) {
  const safeMetrics = metricsByRange && typeof metricsByRange === "object" ? metricsByRange : {};
  const pnlOverrides = summaryRangePnlOverrides(summaryRow);
  const volumeOverrides = summaryRangeVolumeOverrides(summaryRow);
  const safeEquity = Math.max(0, toNum(accountEquityUsd, 0), toNum(summaryRow && summaryRow.accountEquityUsd, 0));
  const safeLastSeenAt = Math.max(
    tsMs(fallbackLastSeenAt),
    tsMs(summaryRow && (summaryRow.lastSeenAt || summaryRow.liveLastUpdatedAt || summaryRow.liveLastOpenedAt)),
    Date.now()
  );

  ["1d", "7d", "30d", "all"].forEach((rangeKey) => {
    const overridePnl = pnlOverrides[rangeKey];
    const overrideVolume = volumeOverrides[rangeKey];
    const existing = safeMetrics[rangeKey] && typeof safeMetrics[rangeKey] === "object" ? safeMetrics[rangeKey] : null;
    const hasExisting =
      existing &&
      (toNum(existing.sampleCount, 0) > 0 ||
        Math.abs(toNum(existing.pnlUsd, 0)) > 0 ||
        Math.abs(toNum(existing.equityUsd, 0)) > 0);
    let bucket = existing;

    if (!hasExisting && Number.isFinite(overridePnl)) {
      bucket = {
        ...buildFallbackRangeMetrics({
          pnlUsd: overridePnl,
          equityUsd: safeEquity,
          drawdownPct: toNum(summaryRow && summaryRow.drawdownPct, 0),
          lastSeenAt: safeLastSeenAt,
          windowMs: rangeWindowMs(rangeKey),
        }),
      };
    }
    if (!bucket) return;

    if (Number.isFinite(overridePnl)) {
      const equityUsd = Math.max(0, toNum(bucket.equityUsd, 0), safeEquity);
      const startEquityUsd = Math.max(0, equityUsd - overridePnl);
      bucket.pnlUsd = metricNumber(overridePnl, 6);
      bucket.startEquityUsd = metricNumber(startEquityUsd, 6);
      bucket.returnPct = startEquityUsd > 0 ? metricNumber((overridePnl / startEquityUsd) * 100, 6) : 0;
      if (Array.isArray(bucket.points) && bucket.points.length > 0) {
        const lastPoint = bucket.points[bucket.points.length - 1] || {};
        const lastPointPnl = toNum(lastPoint.pnlUsd, 0);
        const pnlShift = overridePnl - lastPointPnl;
        bucket.points = bucket.points.map((point) => {
          const safePoint = point && typeof point === "object" ? point : {};
          return {
            ...safePoint,
            pnlUsd: metricNumber(toNum(safePoint.pnlUsd, 0) + pnlShift, 6),
          };
        });
      } else {
        const endTs = safeLastSeenAt;
        const startTs = Math.max(0, endTs - rangeWindowMs(rangeKey));
        bucket.points = [
          {
            timestamp: startTs,
            accountEquityUsd: metricNumber(startEquityUsd, 6),
            pnlUsd: 0,
          },
          {
            timestamp: endTs,
            accountEquityUsd: metricNumber(equityUsd, 6),
            pnlUsd: metricNumber(overridePnl, 6),
          },
        ];
        bucket.sampleCount = bucket.points.length;
        bucket.firstSeenAt = startTs;
        bucket.lastSeenAt = endTs;
      }
      bucket.tradingPnlUsd = metricNumber(toNum(bucket.tradingPnlUsd, bucket.pnlUsd), 6);
    }

    if (Number.isFinite(overrideVolume)) {
      bucket.tradingVolumeUsd = metricNumber(overrideVolume, 2);
    }

    safeMetrics[rangeKey] = bucket;
  });

  return safeMetrics;
}

function buildSummaryReference(summaryRow) {
  if (!summaryRow || typeof summaryRow !== "object") return null;
  return {
    pnl1d: toNum(summaryRow.pnl1d, 0),
    pnl7d: toNum(summaryRow.pnl7d, 0),
    pnl30d: toNum(summaryRow.pnl30d, 0),
    totalPnlUsd: toNum(summaryRow.totalPnlUsd, 0),
    volume1d: toNum(summaryRow.volume1d, 0),
    volume7d: toNum(summaryRow.volume7d, 0),
    volume30d: toNum(summaryRow.volume30d, 0),
    volumeAllTime: toNum(summaryRow.volumeAllTime, 0),
  };
}

function buildWalletPerformanceFallbackPositions({
  wallet,
  dataSnapshot,
  accountPayload,
  priceLookup,
}) {
  const normalizedWallet = normalizeWallet(wallet);
  if (!normalizedWallet) return [];
  const sourceRows = dataSnapshot && Array.isArray(dataSnapshot.positions) ? dataSnapshot.positions : [];
  const walletRows = sourceRows.filter((row) => normalizeWallet(row && row.wallet) === normalizedWallet);
  if (!walletRows.length) return [];
  const accountData = extractPayloadData(accountPayload, {});
  const totalMarginUsedUsd = Math.max(0, toNum(accountData && accountData.total_margin_used, 0));
  const accountEquityUsd = Math.max(0, toNum(accountData && accountData.account_equity, 0));
  const totalPositionValue = walletRows.reduce((sum, row) => sum + Math.max(0, toNum(row && row.positionUsd, 0)), 0);

  return walletRows
    .map((row) => {
      const safeRow = row && typeof row === "object" ? row : {};
      const symbol = String(safeRow.symbol || "").trim().toUpperCase();
      if (!symbol) return null;
      const sideKey = normalizePositionSideKey(safeRow.side || safeRow.rawSide || "");
      const side = sideKey === "short" ? "short" : "long";
      const size = Math.max(
        Math.abs(toNum(safeRow.size, 0)),
        Math.abs(toNum(safeRow.currentSize, 0)),
        Math.abs(toNum(safeRow.newSize, 0)),
        Math.abs(toNum(safeRow.amount, 0))
      );
      const entryPrice = Math.max(
        0,
        toNum(safeRow.entry, 0) ||
          toNum(safeRow.entryPrice, 0) ||
          toNum(safeRow.entry_price, 0)
      );
      const markPrice = Math.max(
        0,
        toNum(safeRow.mark, 0) ||
          toNum(safeRow.markPrice, 0) ||
          toNum(safeRow.mark_price, 0) ||
          (priceLookup && typeof priceLookup.get === "function"
            ? toNum((priceLookup.get(symbol) || {}).mark, 0)
            : 0) ||
          entryPrice
      );
      const positionValueUsd =
        Math.max(0, toNum(safeRow.positionUsd, 0) || toNum(safeRow.eventUsd, 0)) ||
        size * Math.max(markPrice, entryPrice, 0);
      let marginUsd = Math.max(0, toNum(safeRow.marginUsd, NaN));
      let marginEstimated = false;
      if ((!Number.isFinite(marginUsd) || marginUsd <= 0) && totalMarginUsedUsd > 0 && totalPositionValue > 0) {
        marginUsd = (positionValueUsd / totalPositionValue) * totalMarginUsedUsd;
        marginEstimated = true;
      }
      if ((!Number.isFinite(marginUsd) || marginUsd <= 0) && accountEquityUsd > 0 && totalPositionValue > 0) {
        marginUsd = (positionValueUsd / totalPositionValue) * accountEquityUsd;
        marginEstimated = true;
      }
      if (!Number.isFinite(marginUsd) || marginUsd <= 0) {
        marginUsd = positionValueUsd > 0 ? positionValueUsd : 0;
        marginEstimated = true;
      }
      let leverage = pickReasonableLeverage(safeRow.leverage);
      if ((!Number.isFinite(leverage) || leverage <= 0) && marginUsd > 0 && positionValueUsd > 0) {
        const impliedLeverage = positionValueUsd / marginUsd;
        if (isReasonableLeverage(impliedLeverage)) {
          leverage = impliedLeverage;
        }
      }
      if (
        Number.isFinite(leverage) &&
        leverage > 0 &&
        marginUsd > 0 &&
        positionValueUsd > 0 &&
        !isReasonableLeverage(positionValueUsd / marginUsd)
      ) {
        marginUsd = positionValueUsd / leverage;
        marginEstimated = true;
      }
      if ((!Number.isFinite(leverage) || leverage <= 0) && marginUsd > 0 && positionValueUsd > 0) {
        leverage = positionValueUsd / marginUsd;
      }
      if (!isReasonableLeverage(leverage)) leverage = 1;
      const explicitPnlUsd = toNum(
        safeRow.unrealizedPnlUsd !== undefined
          ? safeRow.unrealizedPnlUsd
          : safeRow.pnlUsd !== undefined
          ? safeRow.pnlUsd
          : safeRow.pnl,
        NaN
      );
      const derivedRawPnl = (markPrice - entryPrice) * size;
      const pnlUsd = Number.isFinite(explicitPnlUsd)
        ? explicitPnlUsd
        : side === "short"
        ? -derivedRawPnl
        : derivedRawPnl;
      const roiPct = marginUsd > 0 ? (pnlUsd / marginUsd) * 100 : 0;
      return {
        wallet: normalizedWallet,
        symbol,
        side,
        sideLabel: side === "short" ? "Short" : "Long",
        size: metricNumber(size, 6),
        leverage: metricNumber(leverage, 2),
        positionValueUsd: metricNumber(positionValueUsd, 2),
        entryPrice: metricNumber(entryPrice, 6),
        markPrice: metricNumber(markPrice, 6),
        pnlUsd: metricNumber(pnlUsd, 2),
        roiPct: metricNumber(roiPct, 2),
        liquidationPrice: (() => {
          const liquidationPrice = readLiquidationPrice(safeRow);
          return Number.isFinite(liquidationPrice) ? metricNumber(liquidationPrice, 6) : null;
        })(),
        marginUsd: metricNumber(marginUsd, 2),
        marginMode: safeRow.isolated === true || safeRow.isolatedFromPayload === true ? "Isolated" : "Cross",
        fundingUsd: metricNumber(toNum(safeRow.funding, 0), 6),
        createdAt: tsOrNull(safeRow.openedAt || safeRow.created_at || safeRow.createdAt),
        updatedAt: tsOrNull(
          safeRow.updatedAt || safeRow.lastUpdatedAt || safeRow.observedAt || safeRow.timestamp
        ),
        marginEstimated,
      };
    })
    .filter(Boolean)
    .sort((left, right) => compareNumbers(left.positionValueUsd, right.positionValueUsd, "desc"));
}

function isOpenOrderStatus(statusValue) {
  const status = String(statusValue || "").trim().toLowerCase();
  if (!status) return false;
  if (
    status.includes("open") ||
    status.includes("new") ||
    status.includes("resting") ||
    status.includes("active") ||
    status.includes("pending") ||
    status.includes("triggered") ||
    status.includes("partially")
  ) {
    return true;
  }
  if (
    status.includes("filled") ||
    status.includes("cancel") ||
    status.includes("closed") ||
    status.includes("rejected") ||
    status.includes("expired")
  ) {
    return false;
  }
  return true;
}

function buildWalletPerformanceOpenOrderRows({ wallet, ordersPayload, allowUnknownStatus = false }) {
  const normalizedWallet = normalizeWallet(wallet);
  if (!normalizedWallet) return [];
  const rows = extractPayloadData(ordersPayload, []);
  if (!Array.isArray(rows)) return [];
  return rows
    .map((row) => {
      const safeRow = row && typeof row === "object" ? row : {};
      const symbol = String(safeRow.symbol || safeRow.token || "").trim().toUpperCase();
      if (!symbol) return null;
      const sideKey = normalizePositionSideKey(
        safeRow.side || safeRow.order_side || safeRow.direction || safeRow.rawSide || ""
      );
      const side = sideKey === "short" ? "short" : "long";
      const sideLabel = side === "short" ? "Short" : "Long";
      const status = String(
        safeRow.order_status || safeRow.status || safeRow.state || ""
      ).trim();
      if (!status && !allowUnknownStatus) return null;
      if (status && !isOpenOrderStatus(status)) return null;
      const amountValue = toNum(
        safeRow.amount !== undefined
          ? safeRow.amount
          : safeRow.size !== undefined
          ? safeRow.size
          : safeRow.qty !== undefined
          ? safeRow.qty
          : safeRow.quantity,
        0
      );
      const remainingValue = toNum(
        safeRow.remaining_amount !== undefined
          ? safeRow.remaining_amount
          : safeRow.remaining !== undefined
          ? safeRow.remaining
          : safeRow.unfilled_amount,
        NaN
      );
      const filledValue = toNum(
        safeRow.filled_amount !== undefined
          ? safeRow.filled_amount
          : safeRow.filled !== undefined
          ? safeRow.filled
          : safeRow.executed_amount,
        NaN
      );
      const size = Math.max(
        0,
        Math.abs(
          Number.isFinite(remainingValue)
            ? remainingValue
            : Number.isFinite(filledValue)
            ? Math.max(0, amountValue - filledValue)
            : amountValue
        )
      );
      const price = Math.max(
        0,
        toNum(
          safeRow.price !== undefined
            ? safeRow.price
            : safeRow.limit_price !== undefined
            ? safeRow.limit_price
            : safeRow.limitPrice !== undefined
            ? safeRow.limitPrice
            : safeRow.trigger_price !== undefined
            ? safeRow.trigger_price
            : safeRow.triggerPrice,
          0
        )
      );
      const orderValueUsd = Math.max(
        0,
        toNum(
          safeRow.order_value_usd !== undefined
            ? safeRow.order_value_usd
            : safeRow.orderValueUsd !== undefined
            ? safeRow.orderValueUsd
            : safeRow.value !== undefined
            ? safeRow.value
            : size * price,
          0
        )
      );
      const orderType = String(
        safeRow.order_type || safeRow.orderType || safeRow.type || "Order"
      );
      const createdAt = tsOrNull(
        safeRow.createdAt || safeRow.created_at || safeRow.timestamp || safeRow.time
      );
      const updatedAt = tsOrNull(
        safeRow.updatedAt || safeRow.updated_at || safeRow.modifiedAt || createdAt
      );
      return {
        wallet: normalizedWallet,
        symbol,
        side,
        sideLabel,
        size: metricNumber(size, 6),
        price: metricNumber(price, 6),
        orderValueUsd: metricNumber(orderValueUsd, 2),
        orderType,
        status: status || "open",
        createdAt,
        updatedAt,
      };
    })
    .filter(Boolean)
    .sort((left, right) => compareNumbers(left.updatedAt || left.createdAt, right.updatedAt || right.createdAt, "desc"));
}

async function fetchWalletPerformanceDetail(wallet, force = false, options = null) {
  const normalizedWallet = normalizeWallet(wallet);
  if (!normalizedWallet) return null;
  const safeOptions = options && typeof options === "object" ? options : {};
  const summaryRowFromOptions =
    safeOptions.summaryRow && typeof safeOptions.summaryRow === "object"
      ? safeOptions.summaryRow
      : null;
  const cachedEntry = walletPerformanceDetailCache.get(normalizedWallet) || null;
  if (
    !force &&
    cachedEntry &&
    cachedEntry.payload &&
    Date.now() - cachedEntry.loadedAt <= WALLET_PERFORMANCE_DETAIL_CACHE_TTL_MS
  ) {
    return cachedEntry.payload;
  }
  if (walletPerformanceDetailInflight.has(normalizedWallet)) {
    return walletPerformanceDetailInflight.get(normalizedWallet);
  }
  const fetchPromise = (async () => {
    const optionDataSnapshot =
      safeOptions.dataSnapshot && typeof safeOptions.dataSnapshot === "object"
        ? safeOptions.dataSnapshot
        : null;
    const dataSnapshot = optionDataSnapshot || loadCurrentData(false);
    const summaryRow =
      summaryRowFromOptions || findWalletPerformanceSummaryRow(dataSnapshot, normalizedWallet);
    const fallbackMetricsByRange = buildFallbackMetricsByRange(summaryRow);

    const safeFetch = async (label, requestFactory, fallbackValue) => {
      try {
        const payload = await requestFactory();
        return { ok: true, payload, fallbackUsed: false, error: null, label };
      } catch (error) {
        return {
          ok: false,
          payload: fallbackValue,
          fallbackUsed: true,
          error: String(error && error.message ? error.message : error || "request_failed"),
          label,
        };
      }
    };
    const detailFetchOptionsCritical = {
      allowDirectFallback: true,
      allowJinaFallback: true,
    };
    const detailFetchOptionsNonCritical = {
      allowDirectFallback: true,
      allowJinaFallback: true,
    };

    const priceLookupPromise = fetchPacificaPrices().catch(() => new Map());
    const [
      accountResult,
      positionsResult,
      settingsResult,
      volumeResult,
      timelineAllResult,
      historyResult,
    ] = await Promise.all([
      safeFetch(
        "account",
        () =>
          fetchJsonWithRetry(
            buildPacificaUrl("/api/v1/account", { account: normalizedWallet }),
            WALLET_PERFORMANCE_DETAIL_CRITICAL_TIMEOUT_MS,
            WALLET_PERFORMANCE_DETAIL_CRITICAL_ATTEMPTS,
            detailFetchOptionsCritical
          ),
        { data: {} }
      ),
      safeFetch(
        "positions",
        () =>
          fetchJsonWithRetry(
            buildPacificaUrl("/api/v1/positions", { account: normalizedWallet }),
            WALLET_PERFORMANCE_DETAIL_CRITICAL_TIMEOUT_MS,
            WALLET_PERFORMANCE_DETAIL_CRITICAL_ATTEMPTS,
            detailFetchOptionsCritical
          ),
        { data: [] }
      ),
      safeFetch(
        "settings",
        () =>
          fetchJsonWithRetry(
            buildPacificaUrl("/api/v1/account/settings", { account: normalizedWallet }),
            WALLET_PERFORMANCE_DETAIL_NONCRITICAL_TIMEOUT_MS,
            WALLET_PERFORMANCE_DETAIL_NONCRITICAL_ATTEMPTS,
            detailFetchOptionsNonCritical
          ),
        { data: {} }
      ),
      safeFetch(
        "volume",
        () =>
          fetchJsonWithRetry(
            buildPacificaUrl("/api/v1/portfolio/volume", { account: normalizedWallet }),
            WALLET_PERFORMANCE_DETAIL_NONCRITICAL_TIMEOUT_MS,
            WALLET_PERFORMANCE_DETAIL_NONCRITICAL_ATTEMPTS,
            detailFetchOptionsNonCritical
          ),
        { data: {} }
      ),
      safeFetch(
        "portfolio_all",
        () =>
          fetchJsonWithRetry(
            buildPacificaUrl("/api/v1/portfolio", { account: normalizedWallet, time_range: "all" }),
            WALLET_PERFORMANCE_DETAIL_CRITICAL_TIMEOUT_MS,
            WALLET_PERFORMANCE_DETAIL_CRITICAL_ATTEMPTS,
            detailFetchOptionsCritical
          ),
        { data: [] }
      ),
      safeFetch(
        "balance_history",
        () =>
          fetchPacificaBalanceHistory(normalizedWallet, {
            timeoutMs: WALLET_PERFORMANCE_DETAIL_NONCRITICAL_TIMEOUT_MS,
            attempts: WALLET_PERFORMANCE_DETAIL_NONCRITICAL_ATTEMPTS,
            pageLimit: WALLET_PERFORMANCE_HISTORY_DETAIL_PAGE_LIMIT,
            rowLimit: WALLET_PERFORMANCE_HISTORY_ROW_LIMIT,
            fetchOptions: detailFetchOptionsNonCritical,
          }),
        []
      ),
    ]);

    const priceLookup = await priceLookupPromise;

    const accountPayload = accountResult.payload;
    const positionsPayload = positionsResult.payload;
    const settingsPayload = settingsResult.payload;
    const volumePayload = volumeResult.payload;
    const timelineAllPayload = timelineAllResult.payload;
    const timelineAllRows = sortTimelineRows(extractPayloadData(timelineAllPayload, []));
    const nowMs = Date.now();
    const timeline30dPayload = {
      data: sliceTimelineRowsByWindow(timelineAllRows, 30 * 24 * 60 * 60 * 1000, nowMs),
    };
    const timeline7dPayload = {
      data: sliceTimelineRowsByWindow(timelineAllRows, 7 * 24 * 60 * 60 * 1000, nowMs),
    };
    const timeline1dPayload = {
      data: sliceTimelineRowsByWindow(timelineAllRows, 1 * 24 * 60 * 60 * 1000, nowMs),
    };
    let balanceHistoryRows = Array.isArray(historyResult.payload) ? historyResult.payload : [];
    if (
      (!Array.isArray(balanceHistoryRows) || balanceHistoryRows.length === 0) &&
      cachedEntry &&
      cachedEntry.payload &&
      Array.isArray(cachedEntry.payload.history) &&
      cachedEntry.payload.history.length > 0
    ) {
      balanceHistoryRows = cachedEntry.payload.history.slice(0, WALLET_PERFORMANCE_HISTORY_ROW_LIMIT);
    }

    const accountData = extractPayloadData(accountPayload, {});
    const volumeData = extractPayloadData(volumePayload, {});
    const settingsLookup = buildMarginSettingsLookup(settingsPayload);
    let positions = buildWalletPerformancePositionRows({
      wallet: normalizedWallet,
      positionsPayload,
      priceLookup,
      settingsLookup,
      accountPayload,
    });
    const openOrders = [];

    const remoteMetricsByRange = {
      "1d": {
        ...computePortfolioRangeMetrics(timeline1dPayload),
        tradingVolumeUsd: metricNumber(toNum(volumeData && volumeData.volume_1d, 0), 2),
      },
      "7d": {
        ...computePortfolioRangeMetrics(timeline7dPayload),
        tradingVolumeUsd: metricNumber(toNum(volumeData && volumeData.volume_7d, 0), 2),
      },
      "30d": {
        ...computePortfolioRangeMetrics(timeline30dPayload),
        tradingVolumeUsd: metricNumber(toNum(volumeData && volumeData.volume_30d, 0), 2),
      },
      all: {
        ...computePortfolioRangeMetrics(timelineAllPayload),
        tradingVolumeUsd: metricNumber(toNum(volumeData && volumeData.volume_all_time, 0), 2),
      },
    };
    let remoteHasMetrics = Object.values(remoteMetricsByRange).some((metrics) => {
      if (!metrics || typeof metrics !== "object") return false;
      return (
        toNum(metrics.sampleCount, 0) > 0 ||
        Math.abs(toNum(metrics.pnlUsd, 0)) > 0 ||
        Math.abs(toNum(metrics.equityUsd, 0)) > 0
      );
    });
    const fallbackPositions = buildWalletPerformanceFallbackPositions({
      wallet: normalizedWallet,
      dataSnapshot,
      accountPayload,
      priceLookup,
    });
    const shouldUseFallbackPositions =
      !Boolean(positionsResult && positionsResult.ok) ||
      Boolean(positionsResult && positionsResult.fallbackUsed);
    let usedFallbackLivePositions = false;
    let usedCachedPositions = false;
    if (
      shouldUseFallbackPositions &&
      (!Array.isArray(positions) || positions.length === 0) &&
      fallbackPositions.length > 0
    ) {
      positions = fallbackPositions;
      usedFallbackLivePositions = true;
    }
    if (
      shouldUseFallbackPositions &&
      (!Array.isArray(positions) || positions.length === 0) &&
      cachedEntry &&
      cachedEntry.payload &&
      Array.isArray(cachedEntry.payload.positions) &&
      cachedEntry.payload.positions.length > 0
    ) {
      positions = cachedEntry.payload.positions.slice(0, WALLET_PERFORMANCE_POSITION_LIMIT);
      usedCachedPositions = true;
    }
    if (
      (!Array.isArray(balanceHistoryRows) || balanceHistoryRows.length === 0) &&
      cachedEntry &&
      cachedEntry.payload &&
      Array.isArray(cachedEntry.payload.history) &&
      cachedEntry.payload.history.length > 0
    ) {
      balanceHistoryRows = cachedEntry.payload.history.slice(0, WALLET_PERFORMANCE_HISTORY_ROW_LIMIT);
    }

    const fallbackAccountEquity = toNum(summaryRow && summaryRow.accountEquityUsd, 0);
    const fallbackOpenPositions = Math.max(
      0,
      toNum(summaryRow && summaryRow.displayOpenPositions, 0),
      toNum(summaryRow && summaryRow.openPositions, 0),
      positions.length
    );
    const accountEquityUsdRaw = toNum(accountData && accountData.account_equity, 0);
    const totalMarginUsedUsdRaw = toNum(accountData && accountData.total_margin_used, 0);
    const availableToSpendUsdRaw = toNum(accountData && accountData.available_to_spend, 0);
    const availableToWithdrawUsdRaw = toNum(accountData && accountData.available_to_withdraw, 0);
    const positionsCountRaw = toNum(accountData && accountData.positions_count, 0);

    const account = {
      accountEquityUsd: metricNumber(
        accountEquityUsdRaw > 0 ? accountEquityUsdRaw : fallbackAccountEquity,
        2
      ),
      totalMarginUsedUsd: metricNumber(
        totalMarginUsedUsdRaw > 0 ? totalMarginUsedUsdRaw : 0,
        2
      ),
      availableToSpendUsd: metricNumber(
        availableToSpendUsdRaw > 0 ? availableToSpendUsdRaw : 0,
        2
      ),
      availableToWithdrawUsd: metricNumber(
        availableToWithdrawUsdRaw > 0 ? availableToWithdrawUsdRaw : 0,
        2
      ),
      positionsCount: Math.max(positionsCountRaw, fallbackOpenPositions),
      updatedAt: tsOrNull(accountData && accountData.updated_at),
    };

    let metricsByRange = remoteHasMetrics ? remoteMetricsByRange : fallbackMetricsByRange;
    const cachedMetricsByRange =
      cachedEntry &&
      cachedEntry.payload &&
      cachedEntry.payload.metricsByRange &&
      typeof cachedEntry.payload.metricsByRange === "object"
        ? cachedEntry.payload.metricsByRange
        : null;
    if (cachedMetricsByRange) {
      metricsByRange = mergeCachedTimelineIntoMetrics({
        currentMetricsByRange: metricsByRange,
        cachedMetricsByRange,
      });
    }
    if (!remoteHasMetrics) {
      const targetEquity = account.accountEquityUsd > 0 ? account.accountEquityUsd : fallbackAccountEquity;
      Object.values(metricsByRange).forEach((bucket) => {
        if (!bucket || typeof bucket !== "object") return;
        if (toNum(bucket.equityUsd, 0) <= 0 && targetEquity > 0) {
          bucket.equityUsd = metricNumber(targetEquity, 6);
          const pnlUsd = toNum(bucket.pnlUsd, 0);
          const startEquity = Math.max(0, targetEquity - pnlUsd);
          bucket.startEquityUsd = metricNumber(startEquity, 6);
          bucket.returnPct = startEquity > 0 ? metricNumber((pnlUsd / startEquity) * 100, 6) : 0;
          if (Array.isArray(bucket.points) && bucket.points.length >= 2) {
            bucket.points[0].accountEquityUsd = metricNumber(startEquity, 6);
            bucket.points[1].accountEquityUsd = metricNumber(targetEquity, 6);
          }
        }
      });
    }

    if (!remoteHasMetrics) {
      metricsByRange = alignRangeMetricsWithSummary({
        metricsByRange,
        summaryRow,
        accountEquityUsd: account.accountEquityUsd,
        fallbackLastSeenAt: tsMs(
          summaryRow && (summaryRow.lastSeenAt || summaryRow.liveLastUpdatedAt || summaryRow.liveLastOpenedAt)
        ),
      });
    }

    const fetchDiagnostics = [
      accountResult,
      positionsResult,
      settingsResult,
      volumeResult,
      timelineAllResult,
      historyResult,
    ].filter(Boolean).reduce((acc, row) => {
      acc[row.label] = {
        ok: Boolean(row.ok),
        fallbackUsed: Boolean(row.fallbackUsed),
        error: row.error || null,
      };
      return acc;
    }, {});
    fetchDiagnostics.portfolio_30d = {
      ok: Boolean(timelineAllResult.ok),
      fallbackUsed: Boolean(timelineAllResult.fallbackUsed),
      error: timelineAllResult.error || null,
      derivedFrom: "portfolio_all",
    };
    fetchDiagnostics.portfolio_7d = {
      ok: Boolean(timelineAllResult.ok),
      fallbackUsed: Boolean(timelineAllResult.fallbackUsed),
      error: timelineAllResult.error || null,
      derivedFrom: "portfolio_all",
    };
    fetchDiagnostics.portfolio_1d = {
      ok: Boolean(timelineAllResult.ok),
      fallbackUsed: Boolean(timelineAllResult.fallbackUsed),
      error: timelineAllResult.error || null,
      derivedFrom: "portfolio_all",
    };

    const criticalSourcesUnavailable =
      !Boolean(accountResult.ok) &&
      !Boolean(positionsResult.ok) &&
      !Boolean(timelineAllResult.ok);
    if (
      criticalSourcesUnavailable &&
      cachedEntry &&
      cachedEntry.payload &&
      typeof cachedEntry.payload === "object"
    ) {
      return {
        ...cachedEntry.payload,
        generatedAt: Date.now(),
        stale: true,
        staleReason: "critical_sources_unavailable",
        diagnostics: {
          ...(cachedEntry.payload.diagnostics && typeof cachedEntry.payload.diagnostics === "object"
            ? cachedEntry.payload.diagnostics
            : {}),
          fetch: fetchDiagnostics,
          preservedFromCache: true,
        },
      };
    }

    const payload = {
      generatedAt: Date.now(),
      wallet: normalizedWallet,
      walletLabel: shortWallet(normalizedWallet),
      account,
      metricsByRange,
      defaultRange: "30d",
      positions,
      openOrders,
      history: balanceHistoryRows,
      sources: {
        portfolio: `${PACIFICA_PUBLIC_BASE_URL}/api/v1/portfolio`,
        positions: `${PACIFICA_PUBLIC_BASE_URL}/api/v1/positions`,
        volume: `${PACIFICA_PUBLIC_BASE_URL}/api/v1/portfolio/volume`,
        account: `${PACIFICA_PUBLIC_BASE_URL}/api/v1/account`,
        settings: `${PACIFICA_PUBLIC_BASE_URL}/api/v1/account/settings`,
        balanceHistory: `${PACIFICA_PUBLIC_BASE_URL}/api/v1/account/balance/history`,
        prices: `${PACIFICA_PUBLIC_BASE_URL}/api/v1/info/prices`,
        fallbackSummary: summaryRow ? "walletPerformance_snapshot_row" : null,
        fallbackLivePositions: fallbackPositions.length > 0 ? "live_position_shards" : null,
      },
      summaryReference: buildSummaryReference(summaryRow),
      diagnostics: {
        usedFallbackMetrics: !remoteHasMetrics,
        usedFallbackPositions: usedFallbackLivePositions || usedCachedPositions,
        positionSource: usedFallbackLivePositions
          ? "fallback_live_shards"
          : usedCachedPositions
          ? "cached_wallet_detail"
          : "live_positions_endpoint",
        fetch: fetchDiagnostics,
      },
    };
    walletPerformanceDetailCache.set(normalizedWallet, {
      loadedAt: Date.now(),
      payload,
    });
    return payload;
  })().finally(() => {
    walletPerformanceDetailInflight.delete(normalizedWallet);
  });
  walletPerformanceDetailInflight.set(normalizedWallet, fetchPromise);
  return fetchPromise;
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

function lifecycleEventFreshnessTs(row = null) {
  const safeRow = row && typeof row === "object" ? row : {};
  return Math.max(
    0,
    tsMs(
      safeRow.detectedAt ||
        safeRow.observedAt ||
        safeRow.updatedAt ||
        safeRow.timestamp ||
        safeRow.openedAt ||
        0
    )
  );
}

function lifecycleEventStableKey(row = null) {
  const safeRow = row && typeof row === "object" ? row : {};
  const wallet = normalizeWallet(safeRow.wallet);
  const eventType = normalizePositionEventType(
    safeRow.eventType || safeRow.sideEvent || safeRow.cause || safeRow.type,
    safeRow.oldSize,
    safeRow.newSize
  );
  const positionKey = String(safeRow.positionKey || safeRow.key || "").trim();
  const symbol = String(safeRow.symbol || "").trim().toUpperCase();
  const side = String(safeRow.side || safeRow.rawSide || "").trim().toLowerCase();
  const openedAt = tsMs(safeRow.openedAt || safeRow.timestamp || 0);
  const updatedAt = tsMs(safeRow.updatedAt || safeRow.observedAt || safeRow.timestamp || 0);
  const oldSize = Number.isFinite(toNum(safeRow.oldSize, NaN))
    ? Number(toNum(safeRow.oldSize, 0).toFixed(8))
    : "";
  const newSize = Number.isFinite(toNum(safeRow.newSize, NaN))
    ? Number(toNum(safeRow.newSize, 0).toFixed(8))
    : Number.isFinite(toNum(safeRow.amount, NaN))
    ? Number(toNum(safeRow.amount, 0).toFixed(8))
    : "";
  const sizeDelta = Number.isFinite(toNum(safeRow.sizeDelta, NaN))
    ? Number(toNum(safeRow.sizeDelta, 0).toFixed(8))
    : "";

  if (wallet && eventType === "position_opened" && openedAt > 0) {
    if (symbol && side) {
      return ["opened", wallet, symbol, side, openedAt].join("|");
    }
    if (positionKey) {
      return ["opened", wallet, positionKey, openedAt].join("|");
    }
  }

  const parts = [
    eventType || "unknown",
    wallet || "",
    positionKey || symbol || "",
    side || "",
    openedAt || 0,
    updatedAt || 0,
    oldSize,
    newSize,
    sizeDelta,
  ];
  if (!parts.some(Boolean)) return "";
  return parts.join("|");
}

function dedupeLifecycleEvents(rows = []) {
  const deduped = new Map();
  (Array.isArray(rows) ? rows : []).forEach((row) => {
    if (!row || typeof row !== "object") return;
    const key = lifecycleEventStableKey(row);
    if (!key) return;
    const current = deduped.get(key);
    if (!current || lifecycleEventFreshnessTs(row) >= lifecycleEventFreshnessTs(current)) {
      deduped.set(key, row);
    }
  });
  return Array.from(deduped.values());
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
      const d1 = row && row.d24 && typeof row.d24 === "object" ? row.d24 : {};
      const d7 = row && row.d7 && typeof row.d7 === "object" ? row.d7 : {};
      const d30 = row && row.d30 && typeof row.d30 === "object" ? row.d30 : {};
      const allBucket = row && row.all && typeof row.all === "object" ? row.all : {};
      const performanceOpenPositions = toNum(detail.openPositions, 0);
      const snapshotOpenPositions = toNum(row && row.openPositions, 0);
      const liveOpenPositions = live ? toNum(live.openPositions, 0) : 0;
      const snapshotTotalPnlUsd = toNum(
        row && (row.totalPnlUsd !== undefined ? row.totalPnlUsd : row.pnlUsd !== undefined ? row.pnlUsd : allBucket.pnlUsd),
        0
      );
      const officialTotalPnlUsd = Number.isFinite(Number(detail.pnlAllTime))
        ? toNum(detail.pnlAllTime, 0)
        : snapshotTotalPnlUsd;
      const displayOpenPositions = Math.max(performanceOpenPositions, snapshotOpenPositions, liveOpenPositions);
      const out = {
        wallet,
        walletLabel: shortWallet(wallet),
        username: row && row.username ? String(row.username).trim() : null,
        rankAllTime: toNum(row && row.rankAllTime, 0),
        leaderboardPnlAllTime: snapshotTotalPnlUsd,
        leaderboardPnl30d: toNum(d30 && d30.pnlUsd, 0),
        leaderboardPnl7d: toNum(d7 && d7.pnlUsd, 0),
        leaderboardPnl1d: toNum(d1 && d1.pnlUsd, 0),
        leaderboardVolumeAllTime: toNum(
          row && (row.volumeUsd !== undefined ? row.volumeUsd : allBucket.volumeUsd),
          0
        ),
        leaderboardVolume30d: toNum(d30 && d30.volumeUsd, 0),
        leaderboardVolume7d: toNum(d7 && d7.volumeUsd, 0),
        leaderboardVolume1d: toNum(d1 && d1.volumeUsd, 0),
        oiCurrent: toNum(row && row.oiCurrent, 0),
        volumeAllTime: Number.isFinite(Number(detail.volumeAllTime))
          ? toNum(detail.volumeAllTime, 0)
          : toNum(row && (row.volumeUsd !== undefined ? row.volumeUsd : allBucket.volumeUsd), 0),
        volume30d: Number.isFinite(Number(detail.volume30d))
          ? toNum(detail.volume30d, 0)
          : toNum(d30 && d30.volumeUsd, 0),
        volume14d: toNum(detail.volume14d, 0),
        volume7d: Number.isFinite(Number(detail.volume7d))
          ? toNum(detail.volume7d, 0)
          : toNum(d7 && d7.volumeUsd, 0),
        volume1d: Number.isFinite(Number(detail.volume1d))
          ? toNum(detail.volume1d, 0)
          : toNum(d1 && d1.volumeUsd, 0),
        pnlAllTime: officialTotalPnlUsd,
        totalPnlUsd: officialTotalPnlUsd,
        pnl30d: toNum(d30 && d30.pnlUsd, 0),
        pnl7d: toNum(d7 && d7.pnlUsd, 0),
        pnl1d: toNum(d1 && d1.pnlUsd, 0),
        drawdownPct: Number.isFinite(Number(detail.drawdownPct))
          ? toNum(detail.drawdownPct, 0)
          : toNum(row && row.drawdownPct, 0),
        returnPct: Number.isFinite(Number(detail.returnPct))
          ? toNum(detail.returnPct, 0)
          : toNum(row && row.returnPct, 0),
        accountEquityUsd: Number.isFinite(Number(detail.accountEquityUsd))
          ? toNum(detail.accountEquityUsd, 0)
          : toNum(row && row.accountEquityUsd, 0),
        openPositions: displayOpenPositions,
        displayOpenPositions,
        liveOpenPositions,
        exposureUsd: live ? Number(live.exposureUsd.toFixed(2)) : 0,
        unrealizedPnlUsd: live ? Number(live.unrealizedPnlUsd.toFixed(2)) : 0,
        lastSeenAt: Math.max(tsMs(detail.lastSeenAt), tsMs(row && row.lastTrade), tsMs(row && row.lastActivity)),
        lastUpdatedAt: tsMs(detail.accountUpdatedAt || detail.fetchedAt),
        liveLastOpenedAt: live ? tsMs(live.lastOpenedAt) : 0,
        liveLastUpdatedAt: live ? tsMs(live.lastUpdatedAt) : 0,
        completeness: row && row.completeness && typeof row.completeness === "object" ? row.completeness : null,
        metricSources: {
          walletUniverse: "positive_wallet_rows_snapshot",
          walletPerformance: "portfolio_endpoint_cache",
          livePositions: "live_position_shards",
          pnl7d: "positive_wallet_rows_snapshot.d7.pnlUsd",
          pnl30d: "positive_wallet_rows_snapshot.d30.pnlUsd",
          totalPnlUsd: Number.isFinite(Number(detail.pnlAllTime))
            ? "public_endpoint_wallet_details.pnlAllTime"
            : "positive_wallet_rows_snapshot.totalPnlUsd",
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
  const positiveOpenedEvents = dedupeLifecycleEvents(
    openedEvents.filter((row) => positiveWalletSet.has(normalizeWallet(row && row.wallet)))
  );
  const positiveChangeEvents = dedupeLifecycleEvents(
    changeEvents.filter((row) => positiveWalletSet.has(normalizeWallet(row && row.wallet)))
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

async function mapWithConcurrency(items, concurrency, worker) {
  const list = Array.isArray(items) ? items : [];
  if (!list.length) return [];
  const results = new Array(list.length);
  let cursor = 0;
  const workerCount = Math.max(1, Number(concurrency || 1));
  const runners = Array.from({ length: workerCount }, async () => {
    while (true) {
      const index = cursor;
      cursor += 1;
      if (index >= list.length) return;
      try {
        results[index] = await worker(list[index], index);
      } catch (error) {
        results[index] = {
          ok: false,
          error: String(error && error.message ? error.message : error || "prefetch_failed"),
        };
      }
    }
  });
  await Promise.all(runners);
  return results;
}

function buildWalletPerformancePrefetchWallets(dataSnapshot) {
  const rows = Array.isArray(dataSnapshot && dataSnapshot.walletPerformance)
    ? dataSnapshot.walletPerformance.slice()
    : [];
  rows.sort((left, right) => {
    const leftOpen = Math.max(0, toNum(left && (left.displayOpenPositions || left.openPositions), 0));
    const rightOpen = Math.max(0, toNum(right && (right.displayOpenPositions || right.openPositions), 0));
    if (rightOpen !== leftOpen) return rightOpen - leftOpen;
    const leftPnl = Math.abs(toNum(left && left.totalPnlUsd, 0));
    const rightPnl = Math.abs(toNum(right && right.totalPnlUsd, 0));
    if (rightPnl !== leftPnl) return rightPnl - leftPnl;
    return normalizeWallet(left && left.wallet).localeCompare(normalizeWallet(right && right.wallet));
  });
  const wallets = rows
    .map((row) => normalizeWallet(row && row.wallet))
    .filter(Boolean);
  if (WALLET_PERFORMANCE_DETAIL_PREFETCH_LIMIT > 0) {
    return wallets.slice(0, WALLET_PERFORMANCE_DETAIL_PREFETCH_LIMIT);
  }
  return wallets;
}

function sliceWalletPerformancePrefetchBatch(wallets) {
  const list = Array.isArray(wallets) ? wallets : [];
  if (!list.length) return [];
  const batchSize = Math.min(list.length, WALLET_PERFORMANCE_DETAIL_PREFETCH_BATCH_SIZE);
  const startIndex = Math.max(0, walletPerformancePrefetchState.nextIndex % list.length);
  const batch = [];
  for (let offset = 0; offset < batchSize; offset += 1) {
    batch.push(list[(startIndex + offset) % list.length]);
  }
  walletPerformancePrefetchState.nextIndex = (startIndex + batch.length) % list.length;
  return batch;
}

async function runWalletPerformanceDetailPrefetchTick() {
  if (!WALLET_PERFORMANCE_DETAIL_PREFETCH_ENABLED) return;
  if (walletPerformancePrefetchState.running) return;
  walletPerformancePrefetchState.running = true;
  walletPerformancePrefetchState.lastStartedAt = Date.now();
  try {
    const dataSnapshot = loadCurrentData(false);
    const wallets = buildWalletPerformancePrefetchWallets(dataSnapshot);
    const batch = sliceWalletPerformancePrefetchBatch(wallets);
    walletPerformancePrefetchState.lastBatchSize = batch.length;
    if (!batch.length) {
      walletPerformancePrefetchState.lastSuccessCount = 0;
      walletPerformancePrefetchState.lastErrorCount = 0;
      return;
    }
    const batchSet = new Set(batch);
    const summaryByWallet = new Map(
      (Array.isArray(dataSnapshot.walletPerformance) ? dataSnapshot.walletPerformance : [])
        .filter((row) => batchSet.has(normalizeWallet(row && row.wallet)))
        .map((row) => [normalizeWallet(row && row.wallet), row])
    );
    const results = await mapWithConcurrency(
      batch,
      WALLET_PERFORMANCE_DETAIL_PREFETCH_CONCURRENCY,
      async (wallet) => {
        const summaryRow = summaryByWallet.get(wallet) || null;
        await fetchWalletPerformanceDetail(wallet, false, {
          summaryRow,
          dataSnapshot,
        });
        return { ok: true };
      }
    );
    let successCount = 0;
    let errorCount = 0;
    results.forEach((row) => {
      if (row && row.ok) successCount += 1;
      else errorCount += 1;
    });
    walletPerformancePrefetchState.lastSuccessCount = successCount;
    walletPerformancePrefetchState.lastErrorCount = errorCount;
  } catch (_error) {
    walletPerformancePrefetchState.lastErrorCount = Math.max(
      1,
      walletPerformancePrefetchState.lastBatchSize || 1
    );
  } finally {
    walletPerformancePrefetchState.lastFinishedAt = Date.now();
    walletPerformancePrefetchState.lastDurationMs = Math.max(
      0,
      walletPerformancePrefetchState.lastFinishedAt - walletPerformancePrefetchState.lastStartedAt
    );
    walletPerformancePrefetchState.running = false;
  }
}

function startWalletPerformanceDetailPrefetchLoop() {
  if (!WALLET_PERFORMANCE_DETAIL_PREFETCH_ENABLED) return;
  if (walletPerformancePrefetchState.timer) return;
  walletPerformancePrefetchState.timer = setInterval(() => {
    runWalletPerformanceDetailPrefetchTick().catch(() => {
      // noop
    });
  }, WALLET_PERFORMANCE_DETAIL_PREFETCH_INTERVAL_MS);
  if (walletPerformancePrefetchState.timer && typeof walletPerformancePrefetchState.timer.unref === "function") {
    walletPerformancePrefetchState.timer.unref();
  }
  setTimeout(() => {
    runWalletPerformanceDetailPrefetchTick().catch(() => {
      // noop
    });
  }, 750);
}

function positionIdentity(row = null) {
  const safeRow = row && typeof row === "object" ? row : {};
  const explicitKey = String(safeRow.positionKey || safeRow.key || "").trim();
  if (explicitKey) return explicitKey;
  const wallet = normalizeWallet(safeRow.wallet);
  const symbol = String(safeRow.symbol || "").trim().toUpperCase();
  const side = normalizePositionSideKey(safeRow.side || safeRow.rawSide || "");
  const entry = toNum(safeRow.entry, NaN);
  const entryKey = Number.isFinite(entry) ? entry.toFixed(8) : "";
  const parts = [wallet, symbol, side, entryKey].filter((part) => String(part || "").trim());
  return parts.length ? parts.join("|") : null;
}

function normalizePositionSideKey(side) {
  const raw = String(side || "").trim().toLowerCase();
  if (raw === "bid" || raw === "open_long" || raw === "long") return "long";
  if (raw === "ask" || raw === "open_short" || raw === "short") return "short";
  if (raw === "close_long") return "close_long";
  if (raw === "close_short") return "close_short";
  return raw;
}

function normalizePositionSideValue(side) {
  const normalized = normalizePositionSideKey(side);
  if (normalized === "long") return "open_long";
  if (normalized === "short") return "open_short";
  return normalized;
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
      const normalizedSide = normalizePositionSideValue(safeRow.side || safeRow.rawSide || "");
      const normalizedRawSide = String(safeRow.rawSide || safeRow.side || "").trim().toLowerCase();
      const isolatedValue =
        safeRow.isolated !== undefined
          ? Boolean(safeRow.isolated)
          : safeRow.raw && typeof safeRow.raw === "object" && Object.prototype.hasOwnProperty.call(safeRow.raw, "isolated")
          ? Boolean(safeRow.raw.isolated)
          : false;
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
        side: normalizedSide || safeRow.side,
        rawSide: normalizedRawSide || safeRow.rawSide || safeRow.side,
        isolated: isolatedValue,
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
  if (
    normalized.includes("ws_account_order_update") ||
    normalized.includes("ws_account_trade") ||
    normalized.includes("wallet_first_position_change") ||
    normalized.includes("order_update")
  ) {
    return "position_size_changed";
  }
  return normalized;
}

function parseEntryFromPositionKey(row = null) {
  const safeRow = row && typeof row === "object" ? row : {};
  const key = String(safeRow.positionKey || safeRow.key || "").trim();
  if (!key) return NaN;
  const parts = key.split("|");
  if (parts.length < 4) return NaN;
  const entry = Number(parts[3]);
  return Number.isFinite(entry) && entry > 0 ? entry : NaN;
}

function pickPositiveNumber(...values) {
  for (const value of values) {
    const num = parseFlexibleNumber(value);
    if (Number.isFinite(num) && num > 0) return num;
  }
  return NaN;
}

function pickNonZeroNumber(...values) {
  for (const value of values) {
    const num = parseFlexibleNumber(value);
    if (Number.isFinite(num) && Math.abs(num) > 0) return num;
  }
  return NaN;
}

function parseFlexibleNumber(value) {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : NaN;
  }
  if (typeof value === "string") {
    const raw = value.trim();
    if (!raw) return NaN;
    const normalized = raw
      .replace(/,/g, "")
      .replace(/\s+/g, "")
      .replace(/x$/i, "");
    const direct = Number(normalized);
    if (Number.isFinite(direct)) return direct;
    const match = normalized.match(/-?\d+(?:\.\d+)?/);
    if (!match) return NaN;
    const parsed = Number(match[0]);
    return Number.isFinite(parsed) ? parsed : NaN;
  }
  return NaN;
}

function roundTo(value, decimals = 8) {
  const num = Number(value);
  if (!Number.isFinite(num)) return NaN;
  return Number(num.toFixed(decimals));
}

function applyNonBlankMarginLeverage(rows = []) {
  const safeRows = Array.isArray(rows) ? rows : [];
  if (!safeRows.length) return [];

  const byPosition = new Map();
  const byWalletSymbolSide = new Map();
  safeRows.forEach((row) => {
    const safeRow = row && typeof row === "object" ? row : {};
    const wallet = normalizeWallet(safeRow.wallet);
    const symbol = String(safeRow.symbol || "").trim().toUpperCase();
    const side = normalizePositionSideKey(safeRow.side || safeRow.rawSide || "");
    const positionKey = String(safeRow.positionKey || safeRow.key || "").trim();
    if (!wallet || !symbol) return;
    const hintKey = `${wallet}|${symbol}|${side}`;
    const hintKeyWithoutSide = `${wallet}|${symbol}|*`;
    const margin = pickPositiveNumber(
      safeRow.margin,
      safeRow.marginUsd,
      safeRow.margin_usd
    );
    const leverage = pickPositiveNumber(
      safeRow.leverage,
      safeRow.lev,
      safeRow.leverageValue,
      safeRow.leverage_value
    );
    const positionUsd = pickPositiveNumber(
      safeRow.positionUsd,
      safeRow.eventUsd,
      safeRow.notionalUsd
    );
    const ts = Math.max(
      0,
      Number(
        safeRow.detectedAt ||
          safeRow.observedAt ||
          safeRow.updatedAt ||
          safeRow.timestamp ||
          safeRow.openedAt ||
          0
      ) || 0
    );
    const score =
      (Number.isFinite(margin) ? 2 : 0) +
      (Number.isFinite(leverage) ? 2 : 0) +
      (Number.isFinite(positionUsd) ? 1 : 0);
    const nextHint = { margin, leverage, positionUsd, ts };
    const storeIfBetter = (map, key) => {
      if (!key) return;
      const current = map.get(key);
      const currentScore = current
        ? (Number.isFinite(current.margin) ? 2 : 0) +
          (Number.isFinite(current.leverage) ? 2 : 0) +
          (Number.isFinite(current.positionUsd) ? 1 : 0)
        : -1;
      if (!current || score > currentScore || (score === currentScore && ts >= current.ts)) {
        map.set(key, nextHint);
      }
    };
    storeIfBetter(byWalletSymbolSide, hintKey);
    storeIfBetter(byWalletSymbolSide, hintKeyWithoutSide);
    if (positionKey) {
      storeIfBetter(byPosition, `${wallet}|${positionKey}`);
    }
  });

  return safeRows.map((row) => {
    const safeRow = row && typeof row === "object" ? row : {};
    const wallet = normalizeWallet(safeRow.wallet);
    const symbol = String(safeRow.symbol || "").trim().toUpperCase();
    const side = normalizePositionSideKey(safeRow.side || safeRow.rawSide || "");
    const positionKey = String(safeRow.positionKey || safeRow.key || "").trim();
    const hint =
      (wallet && positionKey ? byPosition.get(`${wallet}|${positionKey}`) : null) ||
      (wallet && symbol ? byWalletSymbolSide.get(`${wallet}|${symbol}|${side}`) : null) ||
      (wallet && symbol ? byWalletSymbolSide.get(`${wallet}|${symbol}|*`) : null) ||
      null;

    const positionUsd = pickPositiveNumber(
      safeRow.positionUsd,
      safeRow.eventUsd,
      safeRow.notionalUsd,
      hint && hint.positionUsd
    );

    let margin = pickPositiveNumber(
      safeRow.margin,
      safeRow.marginUsd,
      safeRow.margin_usd,
      hint && hint.margin
    );
    let leverage = pickPositiveNumber(
      safeRow.leverage,
      safeRow.lev,
      safeRow.leverageValue,
      safeRow.leverage_value,
      hint && hint.leverage
    );

    let marginEstimated = false;
    let leverageEstimated = false;

    if (
      !Number.isFinite(leverage) &&
      Number.isFinite(margin) &&
      margin > 0 &&
      Number.isFinite(positionUsd) &&
      positionUsd > 0
    ) {
      leverage = positionUsd / margin;
      leverageEstimated = true;
    }
    if (
      !Number.isFinite(margin) &&
      Number.isFinite(leverage) &&
      leverage > 0 &&
      Number.isFinite(positionUsd) &&
      positionUsd > 0
    ) {
      margin = positionUsd / leverage;
      marginEstimated = true;
    }
    const roundedMargin =
      Number.isFinite(margin) && margin > 0 ? Math.max(0, roundTo(margin, 2)) : null;
    const roundedLeverage =
      Number.isFinite(leverage) && leverage > 0 ? Math.max(0, roundTo(leverage, 4)) : null;

    return {
      ...safeRow,
      margin: roundedMargin,
      marginUsd: roundedMargin,
      leverage: roundedLeverage,
      marginEstimated:
        roundedMargin !== null ? Boolean(safeRow.marginEstimated) || marginEstimated : false,
      leverageEstimated:
        roundedLeverage !== null ? Boolean(safeRow.leverageEstimated) || leverageEstimated : false,
    };
  });
}

function decoratePositionEventRow(row = null) {
  const safeRow = row && typeof row === "object" ? row : {};
  const eventType = normalizePositionEventType(safeRow.eventType || safeRow.sideEvent || safeRow.cause || safeRow.type, safeRow.oldSize, safeRow.newSize);
  const normalizedSide = normalizePositionSideValue(safeRow.side || safeRow.rawSide || "");
  const normalizedRawSide = String(safeRow.rawSide || safeRow.side || "").trim().toLowerCase();
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
    side: normalizedSide || safeRow.side,
    rawSide: normalizedRawSide || safeRow.rawSide || safeRow.side,
    eventType: eventType || safeRow.eventType || safeRow.sideEvent || safeRow.cause || safeRow.type || "",
    detectedAt,
    openedAt,
    timestamp: detectedAt || safeRow.timestamp || openedAt,
    source: safeRow.source || "wallet_first_positions",
  };
}

function positionEventIdentity(row = null) {
  const stableKey = lifecycleEventStableKey(row);
  if (stableKey) return stableKey;
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
  const side = normalizePositionSideKey(safeRow.side || safeRow.rawSide || "");
  if (!wallet || !symbol || !side) return null;
  return [wallet, symbol, side].join("|");
}

function hydratePositionFromCurrent(row = null, currentByKey = new Map()) {
  const safeRow = row && typeof row === "object" ? row : {};
  const eventType = String(safeRow.eventType || safeRow.sideEvent || safeRow.cause || safeRow.type || "").trim().toLowerCase();
  const isCloseEvent =
    eventType.includes("position_closed") ||
    eventType.includes("partial_close") ||
    eventType.includes("position_flattened") ||
    eventType.includes("closed");
  const directKey = positionIdentity(safeRow);
  const fallbackKey = positionMatchKey(safeRow);
  const current = (directKey && currentByKey.get(directKey)) || (fallbackKey && currentByKey.get(fallbackKey));
  const normalizedSide = normalizePositionSideValue(
    safeRow.side || safeRow.rawSide || (current && (current.side || current.rawSide)) || ""
  );
  const normalizedRawSide = String(
    safeRow.rawSide || safeRow.side || (current && (current.rawSide || current.side)) || ""
  )
    .trim()
    .toLowerCase();
  const isolatedValue =
    safeRow.isolated !== undefined
      ? Boolean(safeRow.isolated)
      : current && current.isolated !== undefined
      ? Boolean(current.isolated)
      : safeRow.raw && typeof safeRow.raw === "object" && Object.prototype.hasOwnProperty.call(safeRow.raw, "isolated")
      ? Boolean(safeRow.raw.isolated)
      : current &&
        current.raw &&
        typeof current.raw === "object" &&
        Object.prototype.hasOwnProperty.call(current.raw, "isolated")
      ? Boolean(current.raw.isolated)
      : false;
  const parsedEntry = parseEntryFromPositionKey(safeRow);
  const entryValue = pickPositiveNumber(
    safeRow.entry,
    safeRow.entryPrice,
    safeRow.entry_price,
    safeRow.price,
    safeRow.avgEntryPrice,
    safeRow.avg_entry_price,
    current && current.entry,
    current && current.entryPrice,
    current && current.entry_price,
    current && current.price,
    parsedEntry
  );
  const markValue = pickPositiveNumber(
    safeRow.mark,
    safeRow.markPrice,
    safeRow.mark_price,
    safeRow.currentPrice,
    safeRow.price,
    current && current.mark,
    current && current.markPrice,
    current && current.mark_price,
    current && current.currentPrice,
    entryValue
  );
  const isOpenLikeEvent =
    eventType.includes("position_opened") ||
    eventType.includes("position_increased") ||
    eventType.includes("position_size_changed") ||
    eventType.includes("position_update");
  const oldSize = pickPositiveNumber(
    safeRow.oldSize,
    isCloseEvent ? current && current.oldSize : NaN,
    isCloseEvent ? current && current.currentSize : NaN,
    isCloseEvent ? current && current.size : NaN
  );
  const newSize = pickPositiveNumber(
    safeRow.newSize,
    safeRow.size,
    safeRow.amount,
    isOpenLikeEvent ? current && current.newSize : NaN,
    isOpenLikeEvent ? current && current.currentSize : NaN,
    isOpenLikeEvent ? current && current.size : NaN
  );
  const explicitSizeDelta = pickNonZeroNumber(safeRow.sizeDelta, current && current.sizeDelta);
  const sizeDelta =
    Number.isFinite(explicitSizeDelta)
      ? explicitSizeDelta
      : Number.isFinite(toNum(safeRow.oldSize, NaN)) && Number.isFinite(toNum(safeRow.newSize, NaN))
      ? toNum(safeRow.newSize, 0) - toNum(safeRow.oldSize, 0)
      : isCloseEvent && Number.isFinite(oldSize)
      ? -Math.abs(oldSize)
      : Number.isFinite(newSize)
      ? newSize
      : NaN;
  const sizeDeltaAbs = Number.isFinite(sizeDelta)
    ? Math.abs(sizeDelta)
    : pickPositiveNumber(safeRow.sizeDeltaAbs, Math.abs(toNum(safeRow.sizeDelta, NaN)));
  const anchorPrice = Number.isFinite(entryValue) ? entryValue : markValue;
  let eventUsd = pickPositiveNumber(safeRow.eventUsd, current && current.eventUsd);
  if (!Number.isFinite(eventUsd) && Number.isFinite(sizeDeltaAbs) && Number.isFinite(anchorPrice)) {
    eventUsd = sizeDeltaAbs * anchorPrice;
  }
  if (!Number.isFinite(eventUsd) && isCloseEvent && Number.isFinite(oldSize) && Number.isFinite(anchorPrice)) {
    eventUsd = Math.abs(oldSize) * anchorPrice;
  }
  let positionUsd = pickPositiveNumber(safeRow.positionUsd, current && current.positionUsd);
  if (!Number.isFinite(positionUsd) && Number.isFinite(eventUsd)) {
    positionUsd = eventUsd;
  }
  if (!Number.isFinite(positionUsd) && Number.isFinite(newSize) && Number.isFinite(anchorPrice)) {
    positionUsd = Math.abs(newSize) * anchorPrice;
  }
  const pickFinite = (...values) => {
    for (const value of values) {
      const num = parseFlexibleNumber(value);
      if (Number.isFinite(num)) return num;
    }
    return NaN;
  };
  const marginPositive = pickPositiveNumber(
    safeRow.margin,
    safeRow.marginUsd,
    safeRow.margin_usd,
    current && current.margin,
    current && current.marginUsd,
    current && current.margin_usd
  );
  const marginFinite = pickFinite(
    safeRow.margin,
    safeRow.marginUsd,
    safeRow.margin_usd,
    current && current.margin,
    current && current.marginUsd,
    current && current.margin_usd
  );
  const marginValue = Number.isFinite(marginPositive) ? marginPositive : marginFinite;
  const leveragePositive = pickPositiveNumber(
    safeRow.leverage,
    safeRow.lev,
    current && current.leverage,
    current && current.lev,
    Number.isFinite(marginValue) && marginValue > 0 && Number.isFinite(positionUsd)
      ? positionUsd / marginValue
      : NaN
  );
  const leverageFinite = pickFinite(
    safeRow.leverage,
    safeRow.lev,
    current && current.leverage,
    current && current.lev
  );
  const leverageValue = Number.isFinite(leveragePositive) ? leveragePositive : leverageFinite;
  const hasDerivedValue =
    (Number.isFinite(entryValue) && entryValue > 0) ||
    (Number.isFinite(positionUsd) && positionUsd > 0) ||
    (Number.isFinite(eventUsd) && eventUsd > 0) ||
    (Number.isFinite(sizeDeltaAbs) && sizeDeltaAbs > 0);
  if (!current && hasDerivedValue) {
    return {
      ...safeRow,
      side: normalizedSide || safeRow.side,
      rawSide: normalizedRawSide || safeRow.rawSide || safeRow.side,
      isolated: isolatedValue,
      entry: roundTo(entryValue, 8),
      entryPrice: roundTo(entryValue, 8),
      entry_price: roundTo(entryValue, 8),
      mark: Number.isFinite(markValue) ? roundTo(markValue, 8) : safeRow.mark,
      positionUsd: roundTo(positionUsd, 2),
      eventUsd: Number.isFinite(eventUsd) ? roundTo(eventUsd, 2) : safeRow.eventUsd,
      oldSize: Number.isFinite(oldSize) ? roundTo(oldSize, 8) : safeRow.oldSize,
      newSize: Number.isFinite(newSize) ? roundTo(newSize, 8) : safeRow.newSize,
      sizeDelta: Number.isFinite(sizeDelta) ? roundTo(sizeDelta, 8) : safeRow.sizeDelta,
      sizeDeltaAbs: Number.isFinite(sizeDeltaAbs) ? roundTo(sizeDeltaAbs, 8) : safeRow.sizeDeltaAbs,
      margin: Number.isFinite(marginValue) ? roundTo(marginValue, 2) : safeRow.margin,
      marginUsd: Number.isFinite(marginValue) ? roundTo(marginValue, 2) : safeRow.marginUsd,
      leverage: Number.isFinite(leverageValue) ? roundTo(leverageValue, 4) : safeRow.leverage,
    };
  }
  if (!current) return safeRow;
  return {
    ...current,
    ...safeRow,
    wallet: safeRow.wallet || current.wallet,
    symbol: safeRow.symbol || current.symbol,
    side: normalizedSide || safeRow.side || current.side,
    rawSide: normalizedRawSide || safeRow.rawSide || current.rawSide || current.side,
    isolated: isolatedValue,
    entry: Number.isFinite(entryValue) && entryValue > 0 ? roundTo(entryValue, 8) : current.entry,
    entryPrice: Number.isFinite(entryValue) && entryValue > 0 ? roundTo(entryValue, 8) : current.entryPrice,
    entry_price: Number.isFinite(entryValue) && entryValue > 0 ? roundTo(entryValue, 8) : current.entry_price,
    mark: Number.isFinite(markValue) && markValue > 0 ? roundTo(markValue, 8) : current.mark,
    size:
      Number.isFinite(toNum(safeRow.size, NaN)) && Math.abs(toNum(safeRow.size, 0)) > 0
        ? safeRow.size
        : isCloseEvent
        ? safeRow.size
        : current.size,
    currentSize:
      Number.isFinite(toNum(safeRow.currentSize, NaN)) && Math.abs(toNum(safeRow.currentSize, 0)) > 0
        ? safeRow.currentSize
        : isCloseEvent
        ? safeRow.currentSize
        : current.currentSize || current.size,
    newSize:
      Number.isFinite(newSize) && newSize > 0
        ? roundTo(newSize, 8)
        : isCloseEvent
        ? safeRow.newSize
        : current.newSize || current.size,
    oldSize:
      Number.isFinite(oldSize) && oldSize > 0
        ? roundTo(oldSize, 8)
        : safeRow.oldSize,
    sizeDelta:
      Number.isFinite(sizeDelta)
        ? roundTo(sizeDelta, 8)
        : safeRow.sizeDelta,
    sizeDeltaAbs:
      Number.isFinite(sizeDeltaAbs) && sizeDeltaAbs > 0
        ? roundTo(sizeDeltaAbs, 8)
        : safeRow.sizeDeltaAbs,
    amount:
      Number.isFinite(toNum(safeRow.amount, NaN)) && Math.abs(toNum(safeRow.amount, 0)) > 0
        ? safeRow.amount
        : Number.isFinite(newSize) && newSize > 0
        ? roundTo(newSize, 8)
        : current.amount || current.size,
    positionUsd:
      Number.isFinite(positionUsd) && positionUsd > 0 ? roundTo(positionUsd, 2) : current.positionUsd,
    eventUsd:
      Number.isFinite(eventUsd) && eventUsd > 0
        ? roundTo(eventUsd, 2)
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
    liquidationPrice: (() => {
      const nextLiquidation = readLiquidationPrice(safeRow);
      if (Number.isFinite(nextLiquidation)) return roundTo(nextLiquidation, 6);
      const currentLiquidation = readLiquidationPrice(current);
      return Number.isFinite(currentLiquidation) ? roundTo(currentLiquidation, 6) : null;
    })(),
    detectedAt: Math.max(
      0,
      Number(
        safeRow.detectedAt ||
          safeRow.observedAt ||
          safeRow.updatedAt ||
          safeRow.timestamp ||
          safeRow.openedAt ||
          0
      ) || 0
    ),
    observedAt: Math.max(0, Number(safeRow.observedAt || safeRow.updatedAt || safeRow.detectedAt || 0) || 0),
    timestamp: Math.max(0, Number(safeRow.timestamp || safeRow.detectedAt || safeRow.updatedAt || safeRow.openedAt || 0) || 0),
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
  const requestedSortKey = String(searchParams.get("position_sort") || defaultSortKey).trim() || defaultSortKey;
  const requestedSortDir =
    String(searchParams.get("position_dir") || "desc").trim().toLowerCase() === "asc" ? "asc" : "desc";
  const sortKey = feedMode === "events" ? "detectedAt" : requestedSortKey;
  const sortDir = feedMode === "events" ? "desc" : requestedSortDir;
  const openedSince = Math.max(0, Number(searchParams.get("opened_since") || 0) || 0);
  const detectedSince = Math.max(
    0,
    Number(searchParams.get("detected_since") || searchParams.get("event_since") || 0) || 0
  );
  const snapshotFallbackEnabled = parseFlagParam(searchParams.get("snapshot_fallback"), false);
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
    dedupeLifecycleEvents(changeRows.concat(openedRows))
      .map(decoratePositionEventRow)
      .map((row) => hydratePositionFromCurrent(row, currentPositionByKey))
      .forEach((row) => {
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
    rows = dedupeLifecycleEvents(Array.isArray(data.positionOpenedEvents) ? data.positionOpenedEvents.slice() : []);
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
      const isTrackedEvent =
        eventType.includes("position_opened") ||
        eventType.includes("position_increased") ||
        eventType.includes("position_closed_partial") ||
        eventType.includes("position_closed") ||
        eventType.includes("position_size_changed") ||
        eventType.includes("position_update");
      if (!isTrackedEvent) return false;
      const hasSizeSignal =
        Math.max(
          Math.abs(toNum(row && row.size, 0)),
          Math.abs(toNum(row && row.amount, 0)),
          Math.abs(toNum(row && row.newSize, 0)),
          Math.abs(toNum(row && row.oldSize, 0)),
          Math.abs(toNum(row && row.sizeDeltaAbs, 0)),
          Math.abs(toNum(row && row.sizeDelta, 0))
        ) > 0;
      const hasValueSignal =
        Math.max(
          Math.abs(toNum(row && row.positionUsd, 0)),
          Math.abs(toNum(row && row.eventUsd, 0)),
          Math.abs(toNum(row && row.margin, 0))
        ) > 0;
      const hasPriceSignal =
        Math.max(
          Math.abs(toNum(row && row.entry, 0)),
          Math.abs(toNum(row && row.entryPrice, 0)),
          Math.abs(toNum(row && row.entry_price, 0)),
          Math.abs(toNum(row && row.mark, 0)),
          Math.abs(toNum(row && row.markPrice, 0))
        ) > 0;
      const oldSize = Math.abs(toNum(row && row.oldSize, 0));
      const newSize = Math.abs(toNum(row && row.newSize, 0));
      const deltaAbs = Math.max(
        Math.abs(toNum(row && row.sizeDeltaAbs, 0)),
        Math.abs(toNum(row && row.sizeDelta, 0)),
        Number.isFinite(toNum(row && row.oldSize, NaN)) && Number.isFinite(toNum(row && row.newSize, NaN))
          ? Math.abs(toNum(row.newSize, 0) - toNum(row.oldSize, 0))
          : 0
      );
      const structurallyValid =
        eventType.includes("position_opened")
          ? newSize > 0
          : eventType.includes("position_closed_partial") || eventType.includes("position_increased")
          ? deltaAbs > 1e-10
          : eventType.includes("position_closed")
          ? oldSize > 0 || deltaAbs > 1e-10
          : eventType.includes("position_size_changed") || eventType.includes("position_update")
          ? deltaAbs > 1e-10
          : true;
      return structurallyValid && ((hasSizeSignal && (hasPriceSignal || hasValueSignal)) || (hasValueSignal && hasPriceSignal));
    });
    const latestDetectedAt = rows.reduce(
      (maxTs, row) =>
        Math.max(
          maxTs,
          tsMs(row && (row.detectedAt || row.observedAt || row.updatedAt || row.timestamp || row.openedAt))
        ),
      0
    );
    const fallbackToCurrentSnapshot =
      snapshotFallbackEnabled && (!latestDetectedAt || Date.now() - latestDetectedAt > 15000);
    if (fallbackToCurrentSnapshot) {
      const snapshotRows = canonicalizePositionRows(Array.isArray(data.positions) ? data.positions : [])
        .map((row) => {
          const detectedAt = tsMs(
            row && (row.observedAt || row.updatedAt || row.timestamp || row.openedAt)
          );
          return {
            ...row,
            eventType: "position_snapshot",
            source: row && row.source ? row.source : "wallet_first_positions",
            detectedAt,
            timestamp: detectedAt || tsMs(row && row.timestamp) || tsMs(row && row.openedAt),
          };
        })
        .filter((row) => {
          const wallet = normalizeWallet(row && row.wallet);
          if (positiveWalletSet.size && !positiveWalletSet.has(wallet)) return false;
          if (exactWallet && wallet !== exactWallet) return false;
          if (needle) {
            const symbol = String((row && row.symbol) || "").toLowerCase();
            if (!wallet.toLowerCase().includes(needle) && !symbol.includes(needle)) return false;
          }
          if (openedSince > 0 && tsMs(row && (row.openedAt || row.timestamp)) < openedSince) {
            return false;
          }
          return true;
        });
      if (snapshotRows.length) {
        rows = snapshotRows;
      }
    }
  }
  if (detectedSince > 0) {
    rows = rows.filter((row) => {
      const detectedAt = tsMs(row && (row.detectedAt || row.timestamp || row.openedAt));
      return detectedAt >= detectedSince;
    });
  }
  rows = applyNonBlankMarginLeverage(rows);
  rows.sort((left, right) => comparePositionRows(left, right, sortKey, sortDir));
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
  if (
    parseFlagParam(process.env.PACIFICA_LIVE_TRADE_DISABLE_UI, false) &&
    (pathname.startsWith("/live-trade") || pathname.startsWith("/wallet-performance"))
  ) {
    sendJson(res, 404, {
      error: "not_found",
      path: pathname,
    });
    return true;
  }
  if (serveSharedPublicAsset(req, res, parsedUrl)) {
    return true;
  }
  if (serveWalletPerformanceUi(req, res, parsedUrl)) {
    return true;
  }
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

  if (pathname === "/api/wallet-performance") {
    const walletWindow = buildWalletPerformanceSummaryRows(data, parsedUrl.searchParams);
    const shouldEnrichSummary = parseFlagParam(
      parsedUrl.searchParams.get("enrich"),
      WALLET_PERFORMANCE_SUMMARY_REMOTE_ENRICH_ENABLED
    );
    const summaryRowsPromise = shouldEnrichSummary
      ? enrichWalletPerformanceSummaryRows(walletWindow.rows)
      : Promise.resolve(walletWindow.rows);
    summaryRowsPromise
      .then((summaryRows) => {
        sendJson(res, 200, {
          generatedAt: Date.now(),
          mode: "wallet_performance_v1",
          summary: {
            trackedWallets: data.walletCount,
            positiveWallets: data.walletCount,
            returned: walletWindow.returned,
            totalRows: walletWindow.totalRows,
            totalPages: walletWindow.totalPages,
            page: walletWindow.page,
            pageSize: walletWindow.pageSize,
            offset: walletWindow.offset,
            sources: {
              positiveWalletUniverse: "positive_wallet_rows_snapshot.json",
              officialWalletMetricsCache: "public_endpoint_wallet_details.json",
              livePositions: "wallet_first_shard_*.json",
              pnlRangeOverride: `${PACIFICA_PUBLIC_BASE_URL}/api/v1/portfolio?time_range=7d|30d`,
            },
            detailDrilldownSource: `${PACIFICA_PUBLIC_BASE_URL}/api/v1/portfolio + /positions + /account + /account/settings + /account/balance/history + /info/prices`,
            detailPrefetch: {
              enabled: WALLET_PERFORMANCE_DETAIL_PREFETCH_ENABLED,
              intervalMs: WALLET_PERFORMANCE_DETAIL_PREFETCH_INTERVAL_MS,
              batchSize: WALLET_PERFORMANCE_DETAIL_PREFETCH_BATCH_SIZE,
              concurrency: WALLET_PERFORMANCE_DETAIL_PREFETCH_CONCURRENCY,
              running: walletPerformancePrefetchState.running,
              lastDurationMs: walletPerformancePrefetchState.lastDurationMs,
              lastBatchSize: walletPerformancePrefetchState.lastBatchSize,
              lastSuccessCount: walletPerformancePrefetchState.lastSuccessCount,
              lastErrorCount: walletPerformancePrefetchState.lastErrorCount,
              lastFinishedAt: walletPerformancePrefetchState.lastFinishedAt || null,
            },
            leaderboardGeneratedAt: data.leaderboardGeneratedAt,
            portfolioGeneratedAt: data.detailGeneratedAt,
            liveGeneratedAt: data.liveGeneratedAt,
          },
          rows: summaryRows,
        });
      })
      .catch(() => {
        sendJson(res, 200, {
          generatedAt: Date.now(),
          mode: "wallet_performance_v1",
          summary: {
            trackedWallets: data.walletCount,
            positiveWallets: data.walletCount,
            returned: walletWindow.returned,
            totalRows: walletWindow.totalRows,
            totalPages: walletWindow.totalPages,
            page: walletWindow.page,
            pageSize: walletWindow.pageSize,
            offset: walletWindow.offset,
            sources: {
              positiveWalletUniverse: "positive_wallet_rows_snapshot.json",
              officialWalletMetricsCache: "public_endpoint_wallet_details.json",
              livePositions: "wallet_first_shard_*.json",
            },
            detailDrilldownSource: `${PACIFICA_PUBLIC_BASE_URL}/api/v1/portfolio + /positions + /account + /account/settings + /account/balance/history + /info/prices`,
            detailPrefetch: {
              enabled: WALLET_PERFORMANCE_DETAIL_PREFETCH_ENABLED,
              intervalMs: WALLET_PERFORMANCE_DETAIL_PREFETCH_INTERVAL_MS,
              batchSize: WALLET_PERFORMANCE_DETAIL_PREFETCH_BATCH_SIZE,
              concurrency: WALLET_PERFORMANCE_DETAIL_PREFETCH_CONCURRENCY,
              running: walletPerformancePrefetchState.running,
              lastDurationMs: walletPerformancePrefetchState.lastDurationMs,
              lastBatchSize: walletPerformancePrefetchState.lastBatchSize,
              lastSuccessCount: walletPerformancePrefetchState.lastSuccessCount,
              lastErrorCount: walletPerformancePrefetchState.lastErrorCount,
              lastFinishedAt: walletPerformancePrefetchState.lastFinishedAt || null,
            },
            leaderboardGeneratedAt: data.leaderboardGeneratedAt,
            portfolioGeneratedAt: data.detailGeneratedAt,
            liveGeneratedAt: data.liveGeneratedAt,
          },
          rows: walletWindow.rows,
        });
      });
    return true;
  }

  if (/^\/api\/wallet-performance\/wallet\/[^/]+$/i.test(pathname)) {
    const wallet = decodeURIComponent(String(pathname).split("/").pop() || "").trim();
    const normalizedWallet = normalizeWallet(wallet);
    const summaryRow = findWalletPerformanceSummaryRow(data, normalizedWallet);
    const forceDetail = parseFlagParam(parsedUrl.searchParams.get("force"), false);
    const remotePassthrough = parseFlagParam(
      parsedUrl.searchParams.get("_remote_passthrough"),
      false
    );
    if (WALLET_PERFORMANCE_REMOTE_DETAIL_BASE && !remotePassthrough) {
      const remoteUrl = buildRemoteWalletDetailUrl(normalizedWallet, forceDetail);
      if (remoteUrl) {
        fetchJsonWithRetry(remoteUrl, Math.max(3000, WALLET_PERFORMANCE_DETAIL_CRITICAL_TIMEOUT_MS), 1, {
          allowDirectFallback: true,
          allowJinaFallback: false,
        })
          .then((remotePayload) => {
            if (remotePayload && typeof remotePayload === "object" && remotePayload.found !== false) {
              walletPerformanceDetailCache.set(normalizedWallet, {
                loadedAt: Date.now(),
                payload: remotePayload,
              });
              sendJson(res, 200, {
                ...remotePayload,
                found: true,
                remoteRelay: true,
              });
              return;
            }
            // fallback to local pipeline when remote is unavailable
            fetchWalletPerformanceDetail(wallet, forceDetail, {
              summaryRow,
              dataSnapshot: data,
            })
              .then((payload) => {
                sendJson(res, 200, {
                  ...payload,
                  found: true,
                });
              })
              .catch((error) => {
                sendJson(res, 502, {
                  generatedAt: Date.now(),
                  found: false,
                  wallet,
                  error: String(
                    error && error.message ? error.message : error || "wallet detail fetch failed"
                  ),
                });
              });
          })
          .catch(() => {
            // fallback to local pipeline when remote relay fails
            fetchWalletPerformanceDetail(wallet, forceDetail, {
              summaryRow,
              dataSnapshot: data,
            })
              .then((payload) => {
                sendJson(res, 200, {
                  ...payload,
                  found: true,
                });
              })
              .catch((error) => {
                const cached = walletPerformanceDetailCache.get(normalizedWallet);
                if (cached && cached.payload && typeof cached.payload === "object") {
                  sendJson(res, 200, {
                    ...cached.payload,
                    found: true,
                    stale: true,
                    staleReason: String(
                      error && error.message ? error.message : error || "wallet_detail_fetch_failed"
                    ),
                  });
                  return;
                }
                const fallbackSummaryRow = summaryRow || findWalletPerformanceSummaryRow(data, normalizedWallet);
                if (fallbackSummaryRow) {
                  sendJson(
                    res,
                    200,
                    {
                      ...buildWalletPerformanceQuickFallbackPayload({
                        wallet: normalizedWallet,
                        summaryRow: fallbackSummaryRow,
                        dataSnapshot: data,
                        reason: "error_quick_fallback",
                        stale: true,
                      }),
                      diagnostics: {
                        usedFallbackMetrics: true,
                        usedFallbackPositions: true,
                        fetchError: String(
                          error && error.message
                            ? error.message
                            : error || "wallet_detail_fetch_failed"
                        ),
                      },
                    }
                  );
                  return;
                }
                sendJson(res, 502, {
                  generatedAt: Date.now(),
                  found: false,
                  wallet,
                  error: String(
                    error && error.message ? error.message : error || "wallet detail fetch failed"
                  ),
                });
              });
          });
        return true;
      }
    }
    if (!forceDetail) {
      const cachedEntry = walletPerformanceDetailCache.get(normalizedWallet);
      if (cachedEntry && cachedEntry.payload && typeof cachedEntry.payload === "object") {
        const staleAgeMs = Math.max(0, Date.now() - Number(cachedEntry.loadedAt || 0));
        const isFresh = staleAgeMs <= WALLET_PERFORMANCE_DETAIL_CACHE_TTL_MS;
        const cachedPayload = cachedEntry.payload;
        const cachedDiagnostics =
          cachedPayload.diagnostics && typeof cachedPayload.diagnostics === "object"
            ? cachedPayload.diagnostics
            : {};
        const needsHydration =
          Boolean(cachedPayload.warming) ||
          Boolean(cachedPayload.fallbackOnly) ||
          Boolean(cachedDiagnostics.usedFallbackMetrics) ||
          Boolean(cachedDiagnostics.usedFallbackPositions);
        const shouldWarmRefresh = !isFresh || needsHydration;
        sendJson(res, 200, {
          ...cachedPayload,
          found: true,
          stale: !isFresh,
          warming: shouldWarmRefresh,
        });
        if (shouldWarmRefresh) {
          fetchWalletPerformanceDetail(wallet, true, {
            summaryRow,
            dataSnapshot: data,
          }).catch(() => {
            // noop
          });
        }
        return true;
      }
      if (summaryRow) {
        sendJson(
          res,
          200,
          buildWalletPerformanceQuickFallbackPayload({
            wallet: normalizedWallet,
            summaryRow,
            dataSnapshot: data,
            reason: "initial_quick_path",
            stale: false,
          })
        );
        fetchWalletPerformanceDetail(wallet, true, {
          summaryRow,
          dataSnapshot: data,
        }).catch(() => {
          // noop
        });
        return true;
      }
    }
    fetchWalletPerformanceDetail(wallet, forceDetail, {
      summaryRow,
      dataSnapshot: data,
    })
      .then((payload) => {
        sendJson(res, 200, {
          ...payload,
          found: true,
        });
      })
      .catch((error) => {
        const cached = walletPerformanceDetailCache.get(normalizedWallet);
        if (cached && cached.payload && typeof cached.payload === "object") {
          sendJson(res, 200, {
            ...cached.payload,
            found: true,
            stale: true,
            staleReason: String(error && error.message ? error.message : error || "wallet_detail_fetch_failed"),
          });
          return;
        }
        const fallbackSummaryRow = summaryRow || findWalletPerformanceSummaryRow(data, normalizedWallet);
        if (fallbackSummaryRow) {
          sendJson(
            res,
            200,
            {
              ...buildWalletPerformanceQuickFallbackPayload({
                wallet: normalizedWallet,
                summaryRow: fallbackSummaryRow,
                dataSnapshot: data,
                reason: "error_quick_fallback",
                stale: true,
              }),
              diagnostics: {
                usedFallbackMetrics: true,
                usedFallbackPositions: true,
                fetchError: String(
                  error && error.message ? error.message : error || "wallet_detail_fetch_failed"
                ),
              },
            }
          );
          return;
        }
        sendJson(res, 502, {
          generatedAt: Date.now(),
          found: false,
          wallet,
          error: String(error && error.message ? error.message : error || "wallet detail fetch failed"),
        });
      });
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

if (require.main === module) {
  startWalletPerformanceDetailPrefetchLoop();
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
}

module.exports = {
  buildMetrics,
  buildPositionsRows,
  buildWalletDetail,
  buildWalletPerformanceSummaryRows,
  canonicalizePositionRows,
  computePortfolioRangeMetrics,
  fetchWalletPerformanceDetail,
  handleApiRequest,
  loadCurrentData,
  server,
};
