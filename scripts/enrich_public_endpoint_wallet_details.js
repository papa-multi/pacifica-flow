#!/usr/bin/env node
"use strict";

const fs = require("fs");
const https = require("https");
const path = require("path");
const crypto = require("crypto");
const { execFile } = require("child_process");

const ROOT = path.resolve(__dirname, "..");
const {
  buildLeaderboardIndex,
  computePortfolioTimelineMetrics,
  DETAIL_CACHE_PATH,
  loadPacificaPublicWalletDetailCache,
  loadPacificaPublicWalletSourcesSnapshot,
  writePacificaPublicWalletDetailCache,
} = require(path.join(ROOT, "src", "services", "read_model", "pacifica_public_wallet_sources"));
const { loadWalletExplorerV3Snapshot } = require(path.join(
  ROOT,
  "src",
  "services",
  "read_model",
  "wallet_storage_v3"
));

const DEFAULT_BASE_URL = String(
  process.env.PACIFICA_PUBLIC_BASE_URL || "https://app.pacifica.fi"
).replace(/\/+$/, "");
const DEFAULT_TIMEOUT_MS = Math.max(
  3000,
  Number(process.env.PACIFICA_PUBLIC_SOURCE_TIMEOUT_MS || 15000)
);
const MAX_FETCH = Math.max(1, Number(process.env.PACIFICA_PUBLIC_DETAIL_ENRICH_LIMIT || 250));
const MAX_CONCURRENCY = Math.max(1, Number(process.env.PACIFICA_PUBLIC_DETAIL_ENRICH_CONCURRENCY || 6));
const CACHE_MAX_AGE_MS = Math.max(
  5 * 60 * 1000,
  Number(process.env.PACIFICA_PUBLIC_DETAIL_CACHE_MAX_AGE_MS || 24 * 60 * 60 * 1000)
);
const WORKER_SHARD_COUNT = Math.max(
  1,
  Number(process.env.PACIFICA_PUBLIC_DETAIL_WORKER_SHARD_COUNT || 1)
);
const WORKER_SHARD_INDEX = Math.max(
  0,
  Math.min(
    WORKER_SHARD_COUNT - 1,
    Number(process.env.PACIFICA_PUBLIC_DETAIL_WORKER_SHARD_INDEX || 0)
  )
);
const LOCAL_SUBSHARD_COUNT = Math.max(
  1,
  Number(process.env.PACIFICA_PUBLIC_DETAIL_LOCAL_SUBSHARD_COUNT || 1)
);
const LOCAL_SUBSHARD_INDEX = Math.max(
  0,
  Math.min(
    LOCAL_SUBSHARD_COUNT - 1,
    Number(process.env.PACIFICA_PUBLIC_DETAIL_LOCAL_SUBSHARD_INDEX || 0)
  )
);
const HOT_REFRESH_MS = Math.max(
  60 * 1000,
  Number(process.env.PACIFICA_PUBLIC_DETAIL_HOT_REFRESH_MS || 15 * 60 * 1000)
);
const WARM_REFRESH_MS = Math.max(
  HOT_REFRESH_MS,
  Number(process.env.PACIFICA_PUBLIC_DETAIL_WARM_REFRESH_MS || 6 * 60 * 60 * 1000)
);
const COLD_REFRESH_MS = Math.max(
  WARM_REFRESH_MS,
  Number(process.env.PACIFICA_PUBLIC_DETAIL_COLD_REFRESH_MS || 36 * 60 * 60 * 1000)
);
const FAILURE_BASE_BACKOFF_MS = Math.max(
  60 * 1000,
  Number(process.env.PACIFICA_PUBLIC_DETAIL_FAILURE_BASE_BACKOFF_MS || 10 * 60 * 1000)
);
const FAILURE_MAX_BACKOFF_MS = Math.max(
  FAILURE_BASE_BACKOFF_MS,
  Number(process.env.PACIFICA_PUBLIC_DETAIL_FAILURE_MAX_BACKOFF_MS || 12 * 60 * 60 * 1000)
);
const PROXY_FILE = String(process.env.PACIFICA_PUBLIC_DETAIL_PROXY_FILE || "").trim();
const PROXY_SERVER_SHARD_COUNT = Math.max(
  1,
  Number(process.env.PACIFICA_PUBLIC_DETAIL_PROXY_SERVER_SHARD_COUNT || WORKER_SHARD_COUNT)
);
const PROXY_SERVER_SHARD_INDEX = Math.max(
  0,
  Math.min(
    PROXY_SERVER_SHARD_COUNT - 1,
    Number(process.env.PACIFICA_PUBLIC_DETAIL_PROXY_SERVER_SHARD_INDEX || WORKER_SHARD_INDEX)
  )
);
const INCLUDE_DIRECT = String(
  process.env.PACIFICA_PUBLIC_DETAIL_PROXY_INCLUDE_DIRECT || "true"
).trim().toLowerCase() === "true";
const LANE_MODE = String(process.env.PACIFICA_PUBLIC_DETAIL_LANE_MODE || "all")
  .trim()
  .toLowerCase();

const STATE_PATH = path.resolve(
  process.env.PACIFICA_PUBLIC_DETAIL_STATE_PATH ||
    DETAIL_CACHE_PATH.replace(/\.json$/i, ".state.json")
);

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
  const tmpPath = `${filePath}.${process.pid}.${Date.now()}.${Math.random()
    .toString(16)
    .slice(2)}.tmp`;
  fs.writeFileSync(tmpPath, JSON.stringify(payload, null, 2));
  fs.renameSync(tmpPath, filePath);
}

function normalizeWallet(value) {
  return String(value || "").trim();
}

function walletShardIndex(wallet, shardCount) {
  const normalizedWallet = normalizeWallet(wallet);
  if (!normalizedWallet || shardCount <= 1) return 0;
  const digest = crypto.createHash("sha1").update(normalizedWallet).digest();
  const value = digest.readUInt32BE(0);
  return value % shardCount;
}

function walletMatchesWorkerShard(wallet) {
  return walletShardIndex(wallet, WORKER_SHARD_COUNT) === WORKER_SHARD_INDEX;
}

function walletMatchesLocalSubshard(wallet) {
  return walletShardIndex(`${wallet}:local`, LOCAL_SUBSHARD_COUNT) === LOCAL_SUBSHARD_INDEX;
}

function loadProxyList(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return [];
  const raw = fs.readFileSync(filePath, "utf8");
  const seen = new Set();
  const result = [];
  raw.split(/\r?\n/).forEach((line) => {
    const proxy = String(line || "").trim();
    if (!proxy || proxy.startsWith("#") || seen.has(proxy)) return;
    seen.add(proxy);
    result.push(proxy);
  });
  return result;
}

function buildAssignedProxyPool() {
  const proxies = loadProxyList(PROXY_FILE);
  if (!proxies.length) return INCLUDE_DIRECT ? [null] : [];
  const assigned = proxies.filter(
    (_proxy, index) => index % PROXY_SERVER_SHARD_COUNT === PROXY_SERVER_SHARD_INDEX
  );
  if (INCLUDE_DIRECT) assigned.push(null);
  return assigned.length ? assigned : INCLUDE_DIRECT ? [null] : [];
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
  if (proxyUrl) {
    args.push("--proxy", proxyUrl);
  }
  args.push(url);
  return new Promise((resolve, reject) => {
    execFile("curl", args, { maxBuffer: 8 * 1024 * 1024 }, (error, stdout, stderr) => {
      if (error) {
        const wrapped = new Error(
          String(stderr || error.message || "curl_failed").trim().slice(0, 300)
        );
        wrapped.code = error.code;
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
        const wrapped = new Error(`HTTP ${statusCode}: ${body.slice(0, 400)}`);
        wrapped.statusCode = statusCode;
        reject(wrapped);
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
            const wrapped = new Error(`HTTP ${res.statusCode}: ${body.slice(0, 400)}`);
            wrapped.statusCode = res.statusCode;
            reject(wrapped);
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
      const wrapped = new Error(`timeout after ${timeoutMs}ms`);
      wrapped.code = "ETIMEDOUT";
      req.destroy(wrapped);
    });
    req.on("error", reject);
  });
}

function createJsonFetcher() {
  const proxyPool = buildAssignedProxyPool();
  let proxyCursor = walletShardIndex(
    `${WORKER_SHARD_INDEX}:${LOCAL_SUBSHARD_INDEX}`,
    Math.max(1, proxyPool.length)
  );
  async function fetchJson(url, timeoutMs = DEFAULT_TIMEOUT_MS) {
    if (!proxyPool.length) {
      return buildDirectJsonRequest(url, timeoutMs);
    }
    const attempts = proxyPool.length;
    let lastError = null;
    for (let offset = 0; offset < attempts; offset += 1) {
      const proxyUrl = proxyPool[(proxyCursor + offset) % proxyPool.length];
      try {
        const payload = proxyUrl
          ? await buildCurlJsonRequest(url, proxyUrl, timeoutMs)
          : await buildDirectJsonRequest(url, timeoutMs);
        proxyCursor = (proxyCursor + offset + 1) % proxyPool.length;
        return payload;
      } catch (error) {
        lastError = error;
      }
    }
    throw lastError || new Error("proxy_pool_exhausted");
  }
  return {
    fetchJson,
    proxyPoolSize: proxyPool.filter(Boolean).length,
  };
}

function loadState() {
  const payload = readJson(STATE_PATH, null);
  if (!payload || typeof payload !== "object") {
    return {
      version: 1,
      cursor: 0,
      wallets: {},
    };
  }
  return {
    version: 1,
    cursor: Math.max(0, Number(payload.cursor || 0)),
    wallets: payload.wallets && typeof payload.wallets === "object" ? payload.wallets : {},
  };
}

function metricValuePresent(entry, field) {
  return entry && entry[field] !== undefined && entry[field] !== null && entry[field] !== "";
}

function hasCoreMetrics(entry) {
  return (
    metricValuePresent(entry, "volumeAllTime") &&
    metricValuePresent(entry, "pnlAllTime") &&
    metricValuePresent(entry, "drawdownPct") &&
    metricValuePresent(entry, "returnPct") &&
    metricValuePresent(entry, "accountEquityUsd") &&
    metricValuePresent(entry, "openPositions")
  );
}

function computeRefreshClass(row, cacheEntry, now) {
  const endpointSeeded = String((row && row.lifecycleStage) || "").trim() === "endpoint_seeded";
  const incompleteHistoryHints =
    !Number(row && row.firstTrade ? row.firstTrade : 0) ||
    !Number(row && row.lastTrade ? row.lastTrade : 0);
  const missingCoreMetrics = !hasCoreMetrics(cacheEntry);
  const openPositions = Number(row && row.openPositions ? row.openPositions : 0) || 0;
  const lastActivity = Number(
    row && (row.lastActivity || row.lastActivityAt || row.lastOpenedAt)
      ? row.lastActivity || row.lastActivityAt || row.lastOpenedAt
      : 0
  ) || 0;
  if (missingCoreMetrics || endpointSeeded || incompleteHistoryHints || openPositions > 0) {
    return "hot";
  }
  if (lastActivity && now - lastActivity <= 3 * 24 * 60 * 60 * 1000) {
    return "hot";
  }
  if (
    lastActivity && now - lastActivity <= 30 * 24 * 60 * 60 * 1000 ||
    Number(row && row.volumeUsd ? row.volumeUsd : 0) > 0 ||
    Number(row && row.trades ? row.trades : 0) > 0
  ) {
    return "warm";
  }
  return "cold";
}

function nextDueAtForClass(refreshClass, now) {
  if (refreshClass === "hot") return now + HOT_REFRESH_MS;
  if (refreshClass === "warm") return now + WARM_REFRESH_MS;
  return now + COLD_REFRESH_MS;
}

function nextDueAtForFailure(failures, now) {
  const exponent = Math.max(0, Math.min(8, failures - 1));
  const backoffMs = Math.min(FAILURE_MAX_BACKOFF_MS, FAILURE_BASE_BACKOFF_MS * 2 ** exponent);
  return now + backoffMs;
}

function laneAllowsRefreshClass(refreshClass) {
  if (LANE_MODE === "all") return true;
  if (LANE_MODE === "hot") return refreshClass === "hot";
  if (LANE_MODE === "non_hot") return refreshClass === "warm" || refreshClass === "cold";
  if (LANE_MODE === "warm") return refreshClass === "warm";
  if (LANE_MODE === "cold") return refreshClass === "cold";
  return true;
}

function walletIsDue({ row, cacheEntry, stateEntry, now }) {
  const endpointSeeded = String((row && row.lifecycleStage) || "").trim() === "endpoint_seeded";
  const incompleteHistoryHints =
    !Number(row && row.firstTrade ? row.firstTrade : 0) ||
    !Number(row && row.lastTrade ? row.lastTrade : 0);
  if (!cacheEntry || !hasCoreMetrics(cacheEntry)) return true;
  if (endpointSeeded || incompleteHistoryHints) return true;
  const fetchedAt = Number(cacheEntry.fetchedAt || 0) || 0;
  if (!fetchedAt || now - fetchedAt > CACHE_MAX_AGE_MS) return true;
  return now >= (Number(stateEntry && stateEntry.nextDueAt ? stateEntry.nextDueAt : 0) || 0);
}

async function main() {
  const now = Date.now();
  const snapshot = loadPacificaPublicWalletSourcesSnapshot();
  const leaderboardIndex = buildLeaderboardIndex(snapshot);
  const existingCache =
    loadPacificaPublicWalletDetailCache() || {
      source: "pacifica_public_endpoints",
      generatedAt: 0,
      wallets: {},
    };
  const existingWallets =
    existingCache.wallets && typeof existingCache.wallets === "object" ? existingCache.wallets : {};
  const state = loadState();

  const v3 = loadWalletExplorerV3Snapshot();
  const rows = Array.isArray(v3 && v3.rows) ? v3.rows : [];
  const rowsByWallet = new Map(
    rows.map((row) => [normalizeWallet(row && row.wallet), row]).filter(([wallet]) => wallet)
  );

  const shardWallets = Array.from(new Set([...leaderboardIndex.keys(), ...rowsByWallet.keys()]))
    .map((wallet) => normalizeWallet(wallet))
    .filter((wallet) => wallet && walletMatchesWorkerShard(wallet) && walletMatchesLocalSubshard(wallet))
    .sort((left, right) => left.localeCompare(right));

  const shardWalletSet = new Set(shardWallets);
  const stateWallets =
    state.wallets && typeof state.wallets === "object" ? state.wallets : {};
  Object.keys(stateWallets).forEach((wallet) => {
    if (!shardWalletSet.has(wallet)) delete stateWallets[wallet];
  });

  const selected = [];
  let scanned = 0;
  const shardCount = shardWallets.length;
  let cursor = shardCount ? state.cursor % shardCount : 0;
  const startCursor = cursor;
  while (shardCount && scanned < shardCount && selected.length < MAX_FETCH) {
    const wallet = shardWallets[cursor];
    cursor = (cursor + 1) % shardCount;
    scanned += 1;
    const row = rowsByWallet.get(wallet) || null;
    const cacheEntry = existingWallets[wallet] || null;
    const stateEntry = stateWallets[wallet] || {};
    const refreshClass = computeRefreshClass(row, cacheEntry, now);
    if (!laneAllowsRefreshClass(refreshClass)) continue;
    if (!walletIsDue({ row, cacheEntry, stateEntry, now })) continue;
    selected.push({
      wallet,
      row,
      cacheEntry,
      refreshClass,
    });
  }
  state.cursor = cursor;

  const { fetchJson, proxyPoolSize } = createJsonFetcher();

  async function fetchPortfolioTimelineSummary(wallet) {
    const url =
      `${DEFAULT_BASE_URL}/api/v1/portfolio?account=${encodeURIComponent(wallet)}&time_range=all`;
    const payload = await fetchJson(url);
    return computePortfolioTimelineMetrics(payload);
  }

  async function fetchPortfolioVolumeSummary(wallet) {
    const url =
      `${DEFAULT_BASE_URL}/api/v1/portfolio/volume?account=${encodeURIComponent(wallet)}`;
    const payload = await fetchJson(url);
    const data = payload && payload.data && typeof payload.data === "object" ? payload.data : {};
    return {
      volume1d: Number(data.volume_1d || 0),
      volume7d: Number(data.volume_7d || 0),
      volume14d: Number(data.volume_14d || 0),
      volume30d: Number(data.volume_30d || 0),
      volumeAllTime: Number(data.volume_all_time || 0),
    };
  }

  async function fetchPortfolioAccountSummary(wallet) {
    const url =
      `${DEFAULT_BASE_URL}/api/v1/account?account=${encodeURIComponent(wallet)}`;
    const payload = await fetchJson(url);
    const data = payload && payload.data && typeof payload.data === "object" ? payload.data : {};
    return {
      accountEquityUsd: Number(data.account_equity || 0),
      openPositions: Math.max(0, Number(data.positions_count || 0)),
      accountUpdatedAt: Number(data.updated_at || 0) || null,
    };
  }

  let workerCursor = 0;
  let fetched = 0;
  let updated = 0;
  let failed = 0;
  async function worker() {
    while (workerCursor < selected.length) {
      const index = workerCursor;
      workerCursor += 1;
      const target = selected[index];
      if (!target) continue;
      const { wallet, refreshClass } = target;
      fetched += 1;
      const stateEntry = stateWallets[wallet] || {};
      stateEntry.lastAttemptAt = Date.now();
      try {
        const [timelineSummary, volumeSummary, accountSummary] = await Promise.all([
          fetchPortfolioTimelineSummary(wallet),
          fetchPortfolioVolumeSummary(wallet),
          fetchPortfolioAccountSummary(wallet),
        ]);
        existingWallets[wallet] = {
          wallet,
          firstSeenAt: Number(timelineSummary.firstSeenAt || 0) || null,
          lastSeenAt: Number(timelineSummary.lastSeenAt || 0) || null,
          sampleCount: Number(timelineSummary.sampleCount || 0) || 0,
          pnlAllTime: Number(timelineSummary.pnlAllTime || 0),
          drawdownPct: Number(timelineSummary.drawdownPct || 0),
          returnPct: Number(timelineSummary.returnPct || 0),
          accountEquityUsd: Number(
            accountSummary.accountEquityUsd || timelineSummary.accountEquityUsd || 0
          ),
          openPositions: Math.max(0, Number(accountSummary.openPositions || 0)),
          volume1d: Number(volumeSummary.volume1d || 0),
          volume7d: Number(volumeSummary.volume7d || 0),
          volume14d: Number(volumeSummary.volume14d || 0),
          volume30d: Number(volumeSummary.volume30d || 0),
          volumeAllTime: Number(volumeSummary.volumeAllTime || 0),
          accountUpdatedAt: Number(accountSummary.accountUpdatedAt || 0) || null,
          fetchedAt: Date.now(),
          source: "portfolio_timeline_volume_account",
        };
        stateWallets[wallet] = {
          ...stateEntry,
          consecutiveFailures: 0,
          lastSuccessAt: Date.now(),
          nextDueAt: nextDueAtForClass(refreshClass, Date.now()),
          freshnessClass: refreshClass,
          lastError: null,
        };
        updated += 1;
      } catch (error) {
        const failuresCount = Math.max(0, Number(stateEntry.consecutiveFailures || 0)) + 1;
        stateWallets[wallet] = {
          ...stateEntry,
          consecutiveFailures: failuresCount,
          nextDueAt: nextDueAtForFailure(failuresCount, Date.now()),
          freshnessClass: refreshClass,
          lastError: error && error.message ? String(error.message).slice(0, 300) : "unknown_error",
        };
        failed += 1;
      }
    }
  }

  const workerCount = Math.min(MAX_CONCURRENCY, Math.max(1, selected.length));
  await Promise.all(Array.from({ length: workerCount }, () => worker()));

  const nextPayload = {
    source: "pacifica_public_endpoints",
    generatedAt: Date.now(),
    walletCount: Object.keys(existingWallets).length,
    shardCount: WORKER_SHARD_COUNT,
    shardIndex: WORKER_SHARD_INDEX,
    localSubshardCount: LOCAL_SUBSHARD_COUNT,
    localSubshardIndex: LOCAL_SUBSHARD_INDEX,
    wallets: existingWallets,
  };
  const nextState = {
    version: 1,
    generatedAt: Date.now(),
    shardCount: WORKER_SHARD_COUNT,
    shardIndex: WORKER_SHARD_INDEX,
    localSubshardCount: LOCAL_SUBSHARD_COUNT,
    localSubshardIndex: LOCAL_SUBSHARD_INDEX,
    cursor: state.cursor,
    wallets: stateWallets,
  };

  writePacificaPublicWalletDetailCache(nextPayload);
  writeJsonAtomic(STATE_PATH, nextState);

  console.log(
    JSON.stringify({
      done: true,
      detailCachePath: DETAIL_CACHE_PATH,
      statePath: STATE_PATH,
      leaderboardWallets: leaderboardIndex.size,
      shardCount: WORKER_SHARD_COUNT,
      shardIndex: WORKER_SHARD_INDEX,
      localSubshardCount: LOCAL_SUBSHARD_COUNT,
      localSubshardIndex: LOCAL_SUBSHARD_INDEX,
      laneMode: LANE_MODE,
      shardWallets: shardWallets.length,
      cursorBefore: startCursor,
      cursorAfter: state.cursor,
      scanned,
      targeted: selected.length,
      concurrency: workerCount,
      fetched,
      updated,
      failed,
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
      statePath: STATE_PATH,
    })
  );
  process.exit(1);
});
