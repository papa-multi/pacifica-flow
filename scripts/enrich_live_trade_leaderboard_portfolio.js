#!/usr/bin/env node
"use strict";

const fs = require("fs");
const https = require("https");
const path = require("path");
const crypto = require("crypto");
const { execFile } = require("child_process");

const ROOT = path.resolve(__dirname, "..");
const {
  computePortfolioTimelineMetrics,
} = require(path.join(ROOT, "src", "services", "read_model", "pacifica_public_wallet_sources"));

const BASE_DIR = path.resolve(
  process.env.PACIFICA_LIVE_TRADE_BASE_DIR ||
    path.join(ROOT, "data", "live_trade_leaderboard")
);
const DEFAULT_BASE_URL = String(
  process.env.PACIFICA_PUBLIC_BASE_URL || "https://app.pacifica.fi"
).replace(/\/+$/, "");
const DEFAULT_TIMEOUT_MS = Math.max(
  3000,
  Number(process.env.PACIFICA_PUBLIC_SOURCE_TIMEOUT_MS || 15000)
);
const MAX_FETCH = Math.max(
  1,
  Number(process.env.PACIFICA_LIVE_TRADE_PORTFOLIO_ENRICH_LIMIT || 400)
);
const MAX_CONCURRENCY = Math.max(
  1,
  Number(process.env.PACIFICA_LIVE_TRADE_PORTFOLIO_ENRICH_CONCURRENCY || 8)
);
const CACHE_MAX_AGE_MS = Math.max(
  5 * 60 * 1000,
  Number(process.env.PACIFICA_LIVE_TRADE_PORTFOLIO_CACHE_MAX_AGE_MS || 6 * 60 * 60 * 1000)
);
const HOT_REFRESH_MS = Math.max(
  60 * 1000,
  Number(process.env.PACIFICA_LIVE_TRADE_PORTFOLIO_HOT_REFRESH_MS || 10 * 60 * 1000)
);
const WARM_REFRESH_MS = Math.max(
  HOT_REFRESH_MS,
  Number(process.env.PACIFICA_LIVE_TRADE_PORTFOLIO_WARM_REFRESH_MS || 2 * 60 * 60 * 1000)
);
const COLD_REFRESH_MS = Math.max(
  WARM_REFRESH_MS,
  Number(process.env.PACIFICA_LIVE_TRADE_PORTFOLIO_COLD_REFRESH_MS || 12 * 60 * 60 * 1000)
);
const FAILURE_BASE_BACKOFF_MS = Math.max(
  60 * 1000,
  Number(process.env.PACIFICA_LIVE_TRADE_PORTFOLIO_FAILURE_BASE_BACKOFF_MS || 10 * 60 * 1000)
);
const FAILURE_MAX_BACKOFF_MS = Math.max(
  FAILURE_BASE_BACKOFF_MS,
  Number(process.env.PACIFICA_LIVE_TRADE_PORTFOLIO_FAILURE_MAX_BACKOFF_MS || 12 * 60 * 60 * 1000)
);
const PROXY_FILE = String(process.env.PACIFICA_LIVE_TRADE_PORTFOLIO_PROXY_FILE || "").trim();
const INCLUDE_DIRECT = String(
  process.env.PACIFICA_LIVE_TRADE_PORTFOLIO_PROXY_INCLUDE_DIRECT || "true"
)
  .trim()
  .toLowerCase() === "true";

const SOURCES_PATH = path.join(BASE_DIR, "leaderboard_sources.json");
const DETAIL_CACHE_PATH = path.join(BASE_DIR, "public_endpoint_wallet_details.json");
const STATE_PATH = path.join(BASE_DIR, "public_endpoint_wallet_details.state.json");

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function writeJsonAtomic(filePath, payload) {
  ensureDir(path.dirname(filePath));
  const tmpPath = `${filePath}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmpPath, JSON.stringify(payload, null, 2));
  fs.renameSync(tmpPath, filePath);
}

function readJson(filePath, fallback = null) {
  try {
    if (!fs.existsSync(filePath)) return fallback;
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return fallback;
  }
}

function normalizeWallet(value) {
  return String(value || "").trim();
}

function toNum(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function loadProxyList(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return [];
  const raw = fs.readFileSync(filePath, "utf8");
  const seen = new Set();
  const out = [];
  raw.split(/\r?\n/).forEach((line) => {
    const proxy = String(line || "").trim();
    if (!proxy || proxy.startsWith("#") || seen.has(proxy)) return;
    seen.add(proxy);
    out.push(proxy);
  });
  return out;
}

function buildCurlJsonRequest(url, proxyUrl, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const timeoutSec = Math.max(1, Math.ceil(timeoutMs / 1000));
  const args = [
    "--silent",
    "--show-error",
    "--location",
    "--max-time",
    String(timeoutSec),
    "--header",
    "accept: application/json",
    "--header",
    "user-agent: pacifica-flow/1.0",
    "--write-out",
    "\n__STATUS__:%{http_code}",
  ];
  if (proxyUrl) args.push("--proxy", proxyUrl);
  args.push(url);
  return new Promise((resolve, reject) => {
    execFile("curl", args, { maxBuffer: 8 * 1024 * 1024 }, (error, stdout, stderr) => {
      if (error) {
        const wrapped = new Error(
          String(stderr || error.message || "curl_failed").trim().slice(0, 300)
        );
        reject(wrapped);
        return;
      }
      const output = String(stdout || "");
      const marker = "\n__STATUS__:";
      const markerIndex = output.lastIndexOf(marker);
      if (markerIndex < 0) {
        reject(new Error("curl_missing_status"));
        return;
      }
      const body = output.slice(0, markerIndex);
      const statusCode = Number(output.slice(markerIndex + marker.length).trim() || 0);
      if (statusCode >= 400) {
        reject(new Error(`HTTP ${statusCode}: ${body.slice(0, 400)}`));
        return;
      }
      try {
        resolve(JSON.parse(body));
      } catch (error) {
        reject(error);
      }
    });
  });
}

function buildDirectJsonRequest(url, timeoutMs = DEFAULT_TIMEOUT_MS) {
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

function createJsonFetcher() {
  const proxyPool = loadProxyList(PROXY_FILE);
  const pool = proxyPool.length ? proxyPool.slice() : [];
  if (INCLUDE_DIRECT) pool.push(null);
  let cursor = 0;
  async function fetchJson(url, timeoutMs = DEFAULT_TIMEOUT_MS) {
    if (!pool.length) return buildDirectJsonRequest(url, timeoutMs);
    let lastError = null;
    for (let offset = 0; offset < pool.length; offset += 1) {
      const proxyUrl = pool[(cursor + offset) % pool.length];
      try {
        const payload = proxyUrl
          ? await buildCurlJsonRequest(url, proxyUrl, timeoutMs)
          : await buildDirectJsonRequest(url, timeoutMs);
        cursor = (cursor + offset + 1) % pool.length;
        return payload;
      } catch (error) {
        lastError = error;
      }
    }
    throw lastError || new Error("proxy_pool_exhausted");
  }
  return {
    fetchJson,
    proxyPoolSize: proxyPool.length,
  };
}

function loadState() {
  const payload = readJson(STATE_PATH, null);
  if (!payload || typeof payload !== "object") {
    return { version: 1, cursor: 0, wallets: {} };
  }
  return {
    version: 1,
    cursor: Math.max(0, Number(payload.cursor || 0)),
    wallets: payload.wallets && typeof payload.wallets === "object" ? payload.wallets : {},
  };
}

function hasCoreMetrics(entry) {
  return Boolean(
    entry &&
      entry.volumeAllTime !== undefined &&
      entry.pnlAllTime !== undefined &&
      entry.drawdownPct !== undefined &&
      entry.returnPct !== undefined &&
      entry.accountEquityUsd !== undefined &&
      entry.openPositions !== undefined
  );
}

function computeRefreshClass(row, cacheEntry, now) {
  if (!cacheEntry || !hasCoreMetrics(cacheEntry)) return "hot";
  if (toNum(row && row.oiCurrent, 0) > 0) return "hot";
  if (toNum(row && row.volume1d, 0) > 0) return "hot";
  if (Math.abs(toNum(row && row.pnl1d, 0)) > 0) return "hot";
  if (toNum(row && row.volume7d, 0) > 0 || toNum(row && row.volume30d, 0) > 0) return "warm";
  const lastSeenAt = toNum(cacheEntry.lastSeenAt, 0);
  if (lastSeenAt && now - lastSeenAt <= 7 * 24 * 60 * 60 * 1000) return "warm";
  return "cold";
}

function nextDueAtForClass(refreshClass, now) {
  if (refreshClass === "hot") return now + HOT_REFRESH_MS;
  if (refreshClass === "warm") return now + WARM_REFRESH_MS;
  return now + COLD_REFRESH_MS;
}

function nextDueAtForFailure(failures, now) {
  const exponent = Math.max(0, Math.min(8, failures - 1));
  return now + Math.min(FAILURE_MAX_BACKOFF_MS, FAILURE_BASE_BACKOFF_MS * 2 ** exponent);
}

function walletIsDue({ cacheEntry, stateEntry, now }) {
  if (!cacheEntry || !hasCoreMetrics(cacheEntry)) return true;
  const fetchedAt = toNum(cacheEntry.fetchedAt, 0);
  if (!fetchedAt || now - fetchedAt > CACHE_MAX_AGE_MS) return true;
  return now >= toNum(stateEntry && stateEntry.nextDueAt, 0);
}

async function main() {
  const now = Date.now();
  const sourcePayload = readJson(SOURCES_PATH, null);
  const leaderboardRows =
    sourcePayload && sourcePayload.leaderboard && Array.isArray(sourcePayload.leaderboard.rows)
      ? sourcePayload.leaderboard.rows
      : [];
  const leaderboardWallets = leaderboardRows
    .map((row) => ({
      wallet: normalizeWallet(row && row.wallet),
      row,
    }))
    .filter((entry) => entry.wallet)
    .sort((left, right) => left.wallet.localeCompare(right.wallet));

  const existingCache =
    readJson(DETAIL_CACHE_PATH, null) || {
      source: "pacifica_public_endpoints",
      generatedAt: 0,
      wallets: {},
    };
  const existingWallets =
    existingCache.wallets && typeof existingCache.wallets === "object" ? existingCache.wallets : {};
  const state = loadState();
  writeJsonAtomic(DETAIL_CACHE_PATH, {
    source: "pacifica_public_endpoints",
    generatedAt: Date.now(),
    walletCount: Object.keys(existingWallets).length,
    wallets: existingWallets,
  });
  writeJsonAtomic(STATE_PATH, {
    version: 1,
    generatedAt: Date.now(),
    cursor: state.cursor,
    wallets: state.wallets,
  });

  const selected = [];
  const count = leaderboardWallets.length;
  let scanned = 0;
  let cursor = count ? state.cursor % count : 0;
  const startCursor = cursor;
  while (count && scanned < count && selected.length < MAX_FETCH) {
    const target = leaderboardWallets[cursor];
    cursor = (cursor + 1) % count;
    scanned += 1;
    if (!target) continue;
    const wallet = target.wallet;
    const row = target.row;
    const cacheEntry = existingWallets[wallet] || null;
    const stateEntry = state.wallets[wallet] || {};
    const refreshClass = computeRefreshClass(row, cacheEntry, now);
    if (!walletIsDue({ cacheEntry, stateEntry, now })) continue;
    selected.push({ wallet, row, refreshClass });
  }
  state.cursor = cursor;

  const { fetchJson, proxyPoolSize } = createJsonFetcher();

  async function fetchPortfolioTimelineSummary(wallet) {
    const payload = await fetchJson(
      `${DEFAULT_BASE_URL}/api/v1/portfolio?account=${encodeURIComponent(wallet)}&time_range=all`
    );
    return computePortfolioTimelineMetrics(payload);
  }

  async function fetchPortfolioVolumeSummary(wallet) {
    const payload = await fetchJson(
      `${DEFAULT_BASE_URL}/api/v1/portfolio/volume?account=${encodeURIComponent(wallet)}`
    );
    const data = payload && payload.data && typeof payload.data === "object" ? payload.data : {};
    return {
      volume1d: toNum(data.volume_1d, 0),
      volume7d: toNum(data.volume_7d, 0),
      volume14d: toNum(data.volume_14d, 0),
      volume30d: toNum(data.volume_30d, 0),
      volumeAllTime: toNum(data.volume_all_time, 0),
    };
  }

  async function fetchPortfolioAccountSummary(wallet) {
    const payload = await fetchJson(
      `${DEFAULT_BASE_URL}/api/v1/account?account=${encodeURIComponent(wallet)}`
    );
    const data = payload && payload.data && typeof payload.data === "object" ? payload.data : {};
    return {
      accountEquityUsd: toNum(data.account_equity, 0),
      openPositions: Math.max(0, toNum(data.positions_count, 0)),
      accountUpdatedAt: toNum(data.updated_at, 0) || null,
    };
  }

  let workerCursor = 0;
  let fetched = 0;
  let updated = 0;
  let failed = 0;
  let flushed = 0;

  function flushProgress() {
    writeJsonAtomic(DETAIL_CACHE_PATH, {
      source: "pacifica_public_endpoints",
      generatedAt: Date.now(),
      walletCount: Object.keys(existingWallets).length,
      wallets: existingWallets,
    });
    writeJsonAtomic(STATE_PATH, {
      version: 1,
      generatedAt: Date.now(),
      cursor: state.cursor,
      wallets: state.wallets,
    });
    flushed += 1;
  }

  async function worker() {
    while (workerCursor < selected.length) {
      const index = workerCursor;
      workerCursor += 1;
      const target = selected[index];
      if (!target) continue;
      const { wallet, row, refreshClass } = target;
      const stateEntry = state.wallets[wallet] || {};
      stateEntry.lastAttemptAt = Date.now();
      fetched += 1;
      try {
        const [timelineSummary, volumeSummary, accountSummary] = await Promise.all([
          fetchPortfolioTimelineSummary(wallet),
          fetchPortfolioVolumeSummary(wallet),
          fetchPortfolioAccountSummary(wallet),
        ]);
        existingWallets[wallet] = {
          wallet,
          username: row && row.username ? String(row.username).trim() : null,
          rankAllTime: toNum(row && row.rankAllTime, 0),
          firstSeenAt: toNum(timelineSummary.firstSeenAt, 0) || null,
          lastSeenAt: toNum(timelineSummary.lastSeenAt, 0) || null,
          sampleCount: toNum(timelineSummary.sampleCount, 0),
          pnlAllTime: toNum(timelineSummary.pnlAllTime, 0),
          drawdownPct: toNum(timelineSummary.drawdownPct, 0),
          returnPct: toNum(timelineSummary.returnPct, 0),
          accountEquityUsd: toNum(
            accountSummary.accountEquityUsd || timelineSummary.accountEquityUsd,
            0
          ),
          openPositions: Math.max(0, toNum(accountSummary.openPositions, 0)),
          volume1d: toNum(volumeSummary.volume1d, 0),
          volume7d: toNum(volumeSummary.volume7d, 0),
          volume14d: toNum(volumeSummary.volume14d, 0),
          volume30d: toNum(volumeSummary.volume30d, 0),
          volumeAllTime: toNum(volumeSummary.volumeAllTime, 0),
          accountUpdatedAt: toNum(accountSummary.accountUpdatedAt, 0) || null,
          fetchedAt: Date.now(),
          source: "portfolio_timeline_volume_account",
        };
        state.wallets[wallet] = {
          ...stateEntry,
          freshnessClass: refreshClass,
          consecutiveFailures: 0,
          lastSuccessAt: Date.now(),
          nextDueAt: nextDueAtForClass(refreshClass, Date.now()),
          lastError: null,
        };
        updated += 1;
        if ((updated + failed) % 25 === 0) flushProgress();
      } catch (error) {
        const failures = Math.max(0, toNum(stateEntry.consecutiveFailures, 0)) + 1;
        state.wallets[wallet] = {
          ...stateEntry,
          freshnessClass: refreshClass,
          consecutiveFailures: failures,
          nextDueAt: nextDueAtForFailure(failures, Date.now()),
          lastError: error && error.message ? String(error.message).slice(0, 300) : "unknown_error",
        };
        failed += 1;
        if ((updated + failed) % 25 === 0) flushProgress();
      }
    }
  }

  const workerCount = Math.min(MAX_CONCURRENCY, Math.max(1, selected.length));
  await Promise.all(Array.from({ length: workerCount }, () => worker()));

  flushProgress();

  console.log(
    JSON.stringify({
      done: true,
      sourcesPath: SOURCES_PATH,
      detailCachePath: DETAIL_CACHE_PATH,
      statePath: STATE_PATH,
      walletUniverse: leaderboardWallets.length,
      cursorBefore: startCursor,
      cursorAfter: state.cursor,
      scanned,
      targeted: selected.length,
      fetched,
      updated,
      failed,
      flushed,
      proxyPoolSize,
      cachedWallets: Object.keys(existingWallets).length,
    })
  );
}

main().catch((error) => {
  console.error(
    JSON.stringify({
      done: false,
      error: error && error.message ? error.message : String(error),
    })
  );
  process.exit(1);
});
