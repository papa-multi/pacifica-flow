#!/usr/bin/env node

const fs = require("fs");
const path = require("path");

const { WalletFirstLivePositionsMonitor } = require("../src/services/analytics/wallet_first_live_positions");
const { HotWalletWsMonitor } = require("../src/services/analytics/hot_wallet_ws_monitor");
const { LiveWalletTriggerStore } = require("../src/services/analytics/live_wallet_trigger_store");
const { createClockSync } = require("../src/services/transport/clock_sync");
const { createRateLimitGuard } = require("../src/services/transport/rate_limit_guard");
const { createRestClient } = require("../src/services/transport/rest_client");
const { createRetryPolicy } = require("../src/services/transport/retry_policy");
const { readJson } = require("../src/services/pipeline/utils");
const { loadWalletExplorerV3Snapshot } = require("../src/services/read_model/wallet_storage_v3");

function normalizeWallet(value) {
  const text = String(value || "").trim();
  return text || "";
}

function normalizeTimestampMs(value, fallback = 0) {
  const ts = Number(value);
  if (!Number.isFinite(ts) || ts <= 0) return Number(fallback || 0);
  return ts < 1e12 ? ts * 1000 : ts;
}

function toNum(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function uniqStrings(list = []) {
  return Array.from(
    new Set(
      (Array.isArray(list) ? list : [])
        .map((value) => String(value || "").trim())
        .filter(Boolean)
    )
  );
}

function parseProxyRows(rawText) {
  const text = String(rawText || "").trim();
  if (!text) return [];

  try {
    const parsed = JSON.parse(text);
    if (Array.isArray(parsed)) {
      return parsed
        .map((row) => {
          if (typeof row === "string") return row.trim();
          if (row && typeof row === "object" && row.proxy) return String(row.proxy).trim();
          return "";
        })
        .filter(Boolean);
    }
  } catch (_error) {
    // Fall back to line parsing.
  }

  return text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .filter((line) => !line.startsWith("#"));
}

function loadProxyList(filePath) {
  if (!filePath) return [];
  if (!fs.existsSync(filePath)) return [];
  return uniqStrings(parseProxyRows(fs.readFileSync(filePath, "utf8")));
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function createClientEntries({
  name,
  proxyRows,
  includeDirect = false,
  baseUrl,
  apiConfigKey,
  transport,
  clockSync,
  retryPolicy,
  rateLimitWindowMs,
  rateLimitSafetyRatio,
  rpmCapPerIp,
  directRpmCap,
  logger,
}) {
  const entries = [];
  const defaultHeaders = apiConfigKey ? { "PF-API-KEY": apiConfigKey } : {};

  if (includeDirect) {
    const directRateGuard = createRateLimitGuard({
      capacity: Math.max(0.1, Number(((directRpmCap * rateLimitWindowMs) / 60000).toFixed(3))),
      refillWindowMs: rateLimitWindowMs,
      safetyMarginRatio: rateLimitSafetyRatio,
    });
    const directClient = createRestClient({
      baseUrl,
      retryPolicy,
      rateLimitGuard: directRateGuard,
      clockSync,
      defaultHeaders,
      logger,
      transport: "fetch",
      clientId: `${name}_direct`,
    });
    entries.push({
      id: "direct",
      client: directClient,
      proxyUrl: null,
    });
  }

  proxyRows.forEach((proxyUrl, idx) => {
    const rateGuard = createRateLimitGuard({
      capacity: Math.max(0.1, Number(((rpmCapPerIp * rateLimitWindowMs) / 60000).toFixed(3))),
      refillWindowMs: rateLimitWindowMs,
      safetyMarginRatio: rateLimitSafetyRatio,
    });
    const client = createRestClient({
      baseUrl,
      retryPolicy,
      rateLimitGuard: rateGuard,
      clockSync,
      defaultHeaders,
      logger,
      transport,
      proxyUrl,
      clientId: `${name}_proxy_${idx + 1}`,
    });
    entries.push({
      id: `proxy_${idx + 1}`,
      client,
      proxyUrl,
    });
  });

  return entries;
}

function createRequestPool({
  name,
  entries,
  requestTimeoutMs,
  maxFetchAttempts,
  maxInFlightPerClient,
  maxInFlightDirect,
  directFallbackOnLastAttempt,
  backoffBaseMs,
  backoffMaxMs,
}) {
  const safeEntries = (Array.isArray(entries) ? entries : []).filter(
    (entry) => entry && entry.client && typeof entry.client.get === "function"
  );
  const clientStates = safeEntries.map((entry) => ({
    id: String(entry.id || ""),
    proxyUrl: entry.proxyUrl || null,
    inFlight: 0,
    cooldownUntil: 0,
    disabledUntil: 0,
    consecutive429: 0,
    consecutiveTimeout: 0,
    requests: 0,
    successes: 0,
    failures: 0,
    lastUsedAt: 0,
    lastSuccessAt: 0,
    lastErrorAt: 0,
    lastError: null,
    avgLatencyMs: 0,
    timeoutCount: 0,
    proxyFailureCount: 0,
    rateLimitCount: 0,
  }));
  const directClientIndex = safeEntries.findIndex(
    (entry) => String(entry && entry.id || "").toLowerCase() === "direct"
  );
  const proxiedClientCount = safeEntries.filter((entry) => entry && entry.proxyUrl).length;
  let clientCursor = 0;
  let lastError = null;
  let lastErrorAt = 0;

  function chooseClient(options = {}) {
    const exclude = options.exclude instanceof Set ? options.exclude : null;
    const preferHealthy = options.preferHealthy !== false;
    const forceDirect = Boolean(options.forceDirect);
    const allowDirect = Boolean(options.allowDirect);
    if (!safeEntries.length) return null;
    const now = Date.now();
    let best = null;
    for (let i = 0; i < safeEntries.length; i += 1) {
      const idx = (clientCursor + i) % safeEntries.length;
      const state = clientStates[idx];
      if (!state) continue;
      if (exclude && exclude.has(idx)) continue;
      if (forceDirect && idx !== directClientIndex) continue;
      if (
        !forceDirect &&
        !allowDirect &&
        proxiedClientCount > 0 &&
        idx === directClientIndex
      ) {
        continue;
      }
      if (now < Number(state.cooldownUntil || 0)) continue;
      if (now < Number(state.disabledUntil || 0)) continue;
      const maxInflightForState =
        idx === directClientIndex ? maxInFlightDirect : maxInFlightPerClient;
      if (Number(state.inFlight || 0) >= maxInflightForState) continue;
      const requests = Number(state.requests || 0);
      const successes = Number(state.successes || 0);
      const failures = Number(state.failures || 0);
      const successRate = requests > 0 ? successes / requests : 1;
      if (requests >= 60 && successes === 0 && failures >= 60) continue;
      if (requests >= 120 && successRate < 0.02) continue;
      const inflight = Number(state.inFlight || 0);
      const latencyPenalty = Math.round(Math.max(0, Number(state.avgLatencyMs || 0)) / 100);
      const timeoutPenalty = Number(state.consecutiveTimeout || 0) * 4;
      const rateLimitPenalty = Number(state.consecutive429 || 0) * 6;
      const healthPenalty =
        preferHealthy && requests >= 4
          ? Math.max(0, Math.round((1 - Math.max(0, Math.min(1, successRate))) * 50))
          : 0;
      const sinceLastUseMs = Math.max(0, now - Number(state.lastUsedAt || 0));
      const recencyPenalty =
        sinceLastUseMs <= 0
          ? 40
          : Math.max(0, Math.round((250 - Math.min(250, sinceLastUseMs)) / 5));
      const score =
        inflight * 100 +
        latencyPenalty +
        timeoutPenalty +
        rateLimitPenalty +
        healthPenalty +
        recencyPenalty;
      if (!best || score < best.score) {
        best = { idx, state, score };
      }
    }
    if (!best) return null;
    clientCursor = (best.idx + 1) % safeEntries.length;
    return {
      idx: best.idx,
      entry: safeEntries[best.idx],
      state: best.state,
    };
  }

  function markClientResult(idx, error = null, meta = {}) {
    const state = clientStates[idx];
    if (!state) return;
    const now = Date.now();
    const durationMs = Math.max(0, Number(meta.durationMs || 0));
    if (durationMs > 0) {
      state.avgLatencyMs = state.avgLatencyMs
        ? Math.round(state.avgLatencyMs * 0.8 + durationMs * 0.2)
        : durationMs;
    }
    const maybeDisableUnhealthyClient = () => {
      const requests = Number(state.requests || 0);
      const successes = Number(state.successes || 0);
      const failures = Number(state.failures || 0);
      if (requests < 24) return;
      const successRate = requests > 0 ? successes / requests : 0;
      if (successes === 0 && failures >= 24) {
        state.disabledUntil = now + 60 * 1000;
        state.cooldownUntil = Math.max(Number(state.cooldownUntil || 0), state.disabledUntil);
        return;
      }
      if (requests >= 120 && successRate < 0.05) {
        state.disabledUntil = now + 90 * 1000;
        state.cooldownUntil = Math.max(Number(state.cooldownUntil || 0), state.disabledUntil);
        return;
      }
      if (requests >= 160 && failures > successes * 4 && successRate < 0.15) {
        state.disabledUntil = now + 45 * 1000;
        state.cooldownUntil = Math.max(Number(state.cooldownUntil || 0), state.disabledUntil);
      }
    };
    if (!error) {
      state.successes = Number(state.successes || 0) + 1;
      state.lastSuccessAt = now;
      state.lastError = null;
      state.lastErrorAt = 0;
      state.consecutive429 = 0;
      state.consecutiveTimeout = 0;
      if (Number(state.failures || 0) > 0) {
        state.failures = Math.max(0, Number(state.failures || 0) - 1);
      }
      if (Number(state.requests || 0) > 200) {
        state.requests = Math.ceil(Number(state.requests || 0) * 0.5);
        state.successes = Math.ceil(Number(state.successes || 0) * 0.5);
        state.failures = Math.ceil(Number(state.failures || 0) * 0.5);
      }
      maybeDisableUnhealthyClient();
      return;
    }

    const message = String(error && error.message ? error.message : error || "");
    state.failures = Number(state.failures || 0) + 1;
    state.lastError = message || "request_failed";
    state.lastErrorAt = now;
    lastError = state.lastError;
    lastErrorAt = now;

    if (message.includes("429")) {
      state.rateLimitCount = Number(state.rateLimitCount || 0) + 1;
      state.consecutive429 = Math.min(10, Number(state.consecutive429 || 0) + 1);
      state.consecutiveTimeout = 0;
      const cooldownMs = Math.min(
        backoffMaxMs,
        backoffBaseMs * 2 ** Math.max(0, state.consecutive429 - 1)
      );
      state.cooldownUntil = now + cooldownMs;
      maybeDisableUnhealthyClient();
      return;
    }

    if (message.toLowerCase().includes("timeout")) {
      state.timeoutCount = Number(state.timeoutCount || 0) + 1;
      state.consecutiveTimeout = Math.min(10, Number(state.consecutiveTimeout || 0) + 1);
      state.consecutive429 = 0;
      const cooldownMs = Math.min(30000, backoffBaseMs * 2 ** Math.max(0, state.consecutiveTimeout - 1));
      state.cooldownUntil = now + cooldownMs;
      maybeDisableUnhealthyClient();
      return;
    }

    if (message.toLowerCase().includes("failed to connect") || message.toLowerCase().includes("proxy")) {
      state.proxyFailureCount = Number(state.proxyFailureCount || 0) + 1;
      state.consecutiveTimeout = Math.min(10, Number(state.consecutiveTimeout || 0) + 1);
      state.cooldownUntil = now + Math.min(10000, Math.max(3000, backoffBaseMs));
      maybeDisableUnhealthyClient();
      return;
    }

    maybeDisableUnhealthyClient();
  }

  async function get(pathname, options = {}) {
    const maxAttempts = Math.max(1, Math.min(maxFetchAttempts, safeEntries.length || 1));
    let requestError = null;
    const tried = new Set();
    for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
      const forceDirect =
        directFallbackOnLastAttempt &&
        directClientIndex >= 0 &&
        (attempt === maxAttempts - 1 || (requestError && attempt > 0));
      let selected = chooseClient({
        exclude: tried,
        forceDirect,
        allowDirect: Boolean(options.allowDirect),
        preferHealthy: options.preferHealthy !== false,
      });
      if (!selected) {
        for (let spin = 0; spin < 4; spin += 1) {
          // eslint-disable-next-line no-await-in-loop
          await new Promise((resolve) => setTimeout(resolve, 40 + spin * 20));
          selected = chooseClient({
            exclude: tried,
            forceDirect,
            allowDirect: Boolean(options.allowDirect),
            preferHealthy: options.preferHealthy !== false,
          });
          if (selected) break;
        }
      }
      if (!selected) {
        const fallback = chooseClient({
          exclude: tried,
          preferHealthy: false,
          allowDirect: true,
        });
        if (!fallback) {
          requestError = new Error(`${name}_clients_cooling_down`);
          break;
        }
        tried.add(fallback.idx);
        selected = fallback;
      } else {
        tried.add(selected.idx);
      }

      const { idx, entry, state } = selected;
      state.inFlight = Number(state.inFlight || 0) + 1;
      state.requests = Number(state.requests || 0) + 1;
      state.lastUsedAt = Date.now();
      const requestStartedAt = Date.now();
      try {
        const response = await entry.client.get(pathname, {
          query: options.query || {},
          cost: Number(options.cost || 1),
          timeoutMs: Math.max(1000, Number(options.timeoutMs || requestTimeoutMs)),
          retryMaxAttempts: 1,
        });
        const payload =
          response && response.payload && typeof response.payload === "object"
            ? response.payload
            : {};
        if (payload.success === false) {
          throw new Error(String(payload.error || `${name}_request_failed`).trim() || `${name}_request_failed`);
        }
        markClientResult(idx, null, { durationMs: Date.now() - requestStartedAt });
        return payload;
      } catch (error) {
        markClientResult(idx, error, { durationMs: Date.now() - requestStartedAt });
        requestError = error;
        const msg = String(error && error.message ? error.message : error || "").toLowerCase();
        const retryable =
          msg.includes("429") ||
          msg.includes("timeout") ||
          msg.includes("failed to connect") ||
          msg.includes("proxy") ||
          msg.includes("503") ||
          msg.includes("502") ||
          msg.includes("504");
        if (!retryable) break;
      } finally {
        state.inFlight = Math.max(0, Number(state.inFlight || 1) - 1);
      }
    }
    throw requestError || new Error(`${name}_request_failed`);
  }

  function getStatus() {
    const now = Date.now();
    const clientsCooling = clientStates.filter(
      (row) => Number(row && row.cooldownUntil ? row.cooldownUntil : 0) > now
    ).length;
    const clientsDisabled = clientStates.filter(
      (row) => Number(row && row.disabledUntil ? row.disabledUntil : 0) > now
    ).length;
    const clientsInFlight = clientStates.reduce(
      (acc, row) => acc + Math.max(0, Number((row && row.inFlight) || 0)),
      0
    );
    const clients429 = clientStates.filter((row) => Number((row && row.consecutive429) || 0) > 0).length;
    const healthyClients = clientStates.filter((row) => {
      if (!row) return false;
      if (Number(row.cooldownUntil || 0) > now) return false;
      if (Number(row.disabledUntil || 0) > now) return false;
      const requests = Number(row.requests || 0);
      const successes = Number(row.successes || 0);
      if (requests <= 0) return true;
      return successes / Math.max(1, requests) >= 0.1;
    }).length;
    const avgClientLatencyMs =
      clientStates.reduce((acc, row) => acc + Math.max(0, Number((row && row.avgLatencyMs) || 0)), 0) /
      Math.max(1, clientStates.length || 1);
    const timeoutClients = clientStates.filter((row) => Number((row && row.timeoutCount) || 0) > 0).length;
    const proxyFailingClients = clientStates.filter(
      (row) => Number((row && row.proxyFailureCount) || 0) > 0
    ).length;

    return {
      clientsTotal: safeEntries.length,
      proxiedClients: safeEntries.filter((entry) => entry && entry.proxyUrl).length,
      directClientIncluded: directClientIndex >= 0,
      clientsCooling,
      clientsDisabled,
      clientsInFlight,
      clients429,
      healthyClients,
      avgClientLatencyMs: Math.round(avgClientLatencyMs || 0),
      timeoutClients,
      proxyFailingClients,
      lastError,
      lastErrorAt,
    };
  }

  return {
    entries: safeEntries,
    clientStates,
    get,
    getStatus,
  };
}

function createTrackedPublicActiveWalletsProvider({
  datasetPath,
  accountPool,
  refreshMs,
  logger,
  leaderboardEnabled = true,
}) {
  let walletSet = new Set();
  let datasetRows = [];
  let walletSetLoadedAt = 0;
  let walletSetMtimeMs = 0;
  let cached = [];
  let cachedAt = 0;

  function ensureTrackedWalletSet(now = Date.now()) {
    if (!datasetPath) return walletSet;
    const usesV3Snapshot =
      String(datasetPath || "").includes("/wallet_explorer_v3/manifest.json") ||
      String(datasetPath || "").endsWith("wallet_explorer_v3/manifest.json");
    if (usesV3Snapshot) {
      const snapshot = loadWalletExplorerV3Snapshot();
      const rows = Array.isArray(snapshot && snapshot.rows) ? snapshot.rows : [];
      if (rows.length) {
        datasetRows = rows;
        walletSet = new Set(rows.map((row) => normalizeWallet(row && row.wallet)).filter(Boolean));
        walletSetLoadedAt = now;
        walletSetMtimeMs = Number(snapshot && snapshot.generatedAt ? snapshot.generatedAt : 0) || 0;
        return walletSet;
      }
    }
    let mtimeMs = 0;
    try {
      mtimeMs = Number(fs.statSync(datasetPath).mtimeMs || 0) || 0;
    } catch (_error) {
      mtimeMs = 0;
    }
    const refreshDue =
      walletSet.size === 0 ||
      now - walletSetLoadedAt >= Math.max(10000, refreshMs) ||
      mtimeMs > walletSetMtimeMs;
    if (!refreshDue) return walletSet;
    const payload = readJson(datasetPath, null);
    const rows = Array.isArray(payload && payload.rows) ? payload.rows : [];
    datasetRows = rows;
    walletSet = new Set(rows.map((row) => normalizeWallet(row && row.wallet)).filter(Boolean));
    walletSetLoadedAt = now;
    walletSetMtimeMs = mtimeMs;
    return walletSet;
  }

  function buildDatasetSeedWallets() {
    return (Array.isArray(datasetRows) ? datasetRows : [])
      .map((row) => {
        const wallet = normalizeWallet(row && row.wallet);
        if (!wallet) return null;
        const openPositions = Math.max(0, Number(row && row.openPositions ? row.openPositions : 0));
        const liveActiveRank = Math.max(0, Number(row && row.liveActiveRank ? row.liveActiveRank : 0));
        const lastTrade = Math.max(
          0,
          Number(
            row && row.all && row.all.lastTrade
              ? row.all.lastTrade
              : row && row.lastTrade
              ? row.lastTrade
              : 0
          )
        );
        const recentVolumeUsd = Math.max(
          0,
          Number(
            row && row.d24 && row.d24.volumeUsd
              ? row.d24.volumeUsd
              : row && row.all && row.all.volumeUsd
              ? row.all.volumeUsd
              : 0
          )
        );
        const priority =
          openPositions * 1_000_000 +
          (liveActiveRank > 0 ? Math.max(0, 50_000 - liveActiveRank * 200) : 0) +
          Math.min(250_000, Math.round(Math.sqrt(Math.max(0, recentVolumeUsd)) * 120)) +
          Math.min(100_000, Math.floor(lastTrade / 1000));
        return {
          wallet,
          priority,
          openPositions,
          liveActiveRank,
          lastTrade,
        };
      })
      .filter(Boolean)
      .sort((a, b) => {
        if (b.priority !== a.priority) return b.priority - a.priority;
        if (b.openPositions !== a.openPositions) return b.openPositions - a.openPositions;
        if (a.liveActiveRank !== b.liveActiveRank) {
          const aRank = a.liveActiveRank > 0 ? a.liveActiveRank : Number.MAX_SAFE_INTEGER;
          const bRank = b.liveActiveRank > 0 ? b.liveActiveRank : Number.MAX_SAFE_INTEGER;
          return aRank - bRank;
        }
        return b.lastTrade - a.lastTrade;
      })
      .slice(0, 8000)
      .map((row) => row.wallet);
  }

  return async function getTrackedPublicActiveWallets() {
    const now = Date.now();
    const trackedWallets = ensureTrackedWalletSet(now);
    const datasetSeedWallets = buildDatasetSeedWallets();
    if (!leaderboardEnabled) {
      cached = uniqStrings(datasetSeedWallets);
      cachedAt = now;
      return cached;
    }
    if (cached.length > 0 && now - cachedAt < refreshMs) return cached;
    try {
      const payload = await accountPool.get("/leaderboard", {
        query: { limit: 2500 },
        timeoutMs: Math.max(5000, Math.floor(accountPool.getStatus().avgClientLatencyMs * 2) || 8000),
      });
      const rows = Array.isArray(payload.data) ? payload.data : [];
      const leaderboardWallets = rows
        .map((row) => {
          const wallet = normalizeWallet(row && row.address);
          const oiCurrent = toNum(row && row.oi_current, 0);
          return wallet && oiCurrent > 0 ? wallet : null;
        })
        .filter(Boolean)
        .filter((wallet) => trackedWallets.size === 0 || trackedWallets.has(wallet));
      cached = uniqStrings([...leaderboardWallets, ...datasetSeedWallets]);
      cachedAt = now;
    } catch (error) {
      logger.warn(
        `[live-public-active] refresh_failed reason=${String(error && error.message ? error.message : error)}`
      );
      cached = uniqStrings(datasetSeedWallets);
      cachedAt = now;
    }
    return cached;
  };
}

async function main() {
  const rootDir = path.resolve(__dirname, "..");
  const workerModeRaw = String(
    process.env.PACIFICA_LIVE_WALLET_FIRST_WORKER_MODE || "combined"
  ).trim();
  const workerMode = ["combined", "account_census", "positions_materializer"].includes(
    workerModeRaw
  )
    ? workerModeRaw
    : "combined";
  const shardIndex = Math.max(0, Number(process.env.PACIFICA_LIVE_WALLET_FIRST_SHARD_INDEX || 0));
  const shardCount = Math.max(1, Number(process.env.PACIFICA_LIVE_WALLET_FIRST_SHARD_COUNT || 1));
  const apiBase = String(process.env.PACIFICA_API_BASE || "https://api.pacifica.fi/api/v1").trim();
  const apiConfigKey = String(process.env.PACIFICA_API_CONFIG_KEY || "").trim();
  const wsUrl = String(process.env.PACIFICA_WS_URL || "wss://ws.pacifica.fi/ws").trim();
  const persistDir = String(
    process.env.PACIFICA_LIVE_WALLET_FIRST_PERSIST_DIR ||
      path.join(rootDir, "data", "live_positions")
  ).trim();
  const positionsPersistPath = path.join(persistDir, `wallet_first_shard_${shardIndex}.json`);
  const accountPersistPath = path.join(
    persistDir,
    `wallet_first_account_shard_${shardIndex}.json`
  );
  const walletDatasetPath = String(
    process.env.PACIFICA_LIVE_WALLET_FIRST_WALLET_DATASET_PATH ||
      path.join(rootDir, "data", "live_positions", "positive_wallet_rows_snapshot.json")
  ).trim();
  const triggerFile = String(
    process.env.PACIFICA_LIVE_WALLET_TRIGGER_FILE ||
      path.join(persistDir, "wallet_activity_triggers.ndjson")
  ).trim();
  const proxyFile = String(
    process.env.PACIFICA_MULTI_EGRESS_PROXY_FILE ||
      path.join(rootDir, "data", "live_positions", "positions_proxies.txt")
  ).trim();
  const transport =
    String(process.env.PACIFICA_MULTI_EGRESS_TRANSPORT || "fetch").toLowerCase() === "curl"
      ? "curl"
      : "fetch";
  const rateLimitWindowMs = Math.max(
    1000,
    Number(process.env.PACIFICA_RATE_LIMIT_WINDOW_MS || 60000)
  );
  const rateLimitSafetyRatio = clamp(
    Number(process.env.PACIFICA_RATE_LIMIT_SAFETY_RATIO || 0.95),
    0.5,
    0.99
  );
  const rpmCapPerIp = Math.max(
    1,
    Number(process.env.PACIFICA_MULTI_EGRESS_RPM_CAP_PER_IP || 250)
  );
  const directRpmCap = Math.max(1, Number(process.env.PACIFICA_API_RPM_CAP || 250));
  const positionsRequestTimeoutMs = Math.max(
    1500,
    Number(
      process.env.PACIFICA_LIVE_WALLET_FIRST_POSITIONS_REQUEST_TIMEOUT_MS ||
        process.env.PACIFICA_LIVE_WALLET_FIRST_REQUEST_TIMEOUT_MS ||
        10000
    )
  );
  const accountRequestTimeoutMs = Math.max(
    1000,
    Number(
      process.env.PACIFICA_LIVE_WALLET_FIRST_DETECTOR_REQUEST_TIMEOUT_MS ||
        process.env.PACIFICA_LIVE_WALLET_FIRST_ACCOUNT_REQUEST_TIMEOUT_MS ||
        4500
    )
  );
  const positionsMaxFetchAttempts = Math.max(
    1,
    Math.min(
      5,
      Number(
        process.env.PACIFICA_LIVE_WALLET_FIRST_POSITIONS_MAX_FETCH_ATTEMPTS ||
          process.env.PACIFICA_LIVE_WALLET_FIRST_MAX_FETCH_ATTEMPTS ||
          3
      )
    )
  );
  const accountMaxFetchAttempts = Math.max(
    1,
    Math.min(
      5,
      Number(
        process.env.PACIFICA_LIVE_WALLET_FIRST_ACCOUNT_MAX_FETCH_ATTEMPTS ||
          process.env.PACIFICA_LIVE_WALLET_FIRST_DETECTOR_MAX_FETCH_ATTEMPTS ||
          process.env.PACIFICA_LIVE_WALLET_FIRST_MAX_FETCH_ATTEMPTS ||
          2
      )
    )
  );
  const positionsDirectFallbackOnLastAttempt =
    String(
      process.env.PACIFICA_LIVE_WALLET_FIRST_POSITIONS_DIRECT_FALLBACK_ON_LAST_ATTEMPT ||
        process.env.PACIFICA_LIVE_WALLET_FIRST_DIRECT_FALLBACK_ON_LAST_ATTEMPT ||
        "true"
    ).toLowerCase() === "true";
  const accountDirectFallbackOnLastAttempt =
    String(
      process.env.PACIFICA_LIVE_WALLET_FIRST_ACCOUNT_DIRECT_FALLBACK_ON_LAST_ATTEMPT ||
        process.env.PACIFICA_LIVE_WALLET_FIRST_DETECTOR_DIRECT_FALLBACK_ON_LAST_ATTEMPT ||
        process.env.PACIFICA_LIVE_WALLET_FIRST_DIRECT_FALLBACK_ON_LAST_ATTEMPT ||
        "true"
    ).toLowerCase() === "true";
  const positionsIncludeDirect =
    String(process.env.PACIFICA_LIVE_WALLET_FIRST_POSITIONS_INCLUDE_DIRECT || "true").toLowerCase() ===
    "true";
  const accountIncludeDirect =
    String(
      process.env.PACIFICA_LIVE_WALLET_FIRST_DETECTOR_INCLUDE_DIRECT ||
        process.env.PACIFICA_LIVE_WALLET_FIRST_ACCOUNT_INCLUDE_DIRECT ||
        "false"
    ).toLowerCase() === "true";
  const maxInFlightPerClient = Math.max(
    1,
    Math.floor(Number(process.env.PACIFICA_LIVE_WALLET_FIRST_MAX_INFLIGHT_PER_CLIENT || 2))
  );
  const maxInFlightDirect = Math.max(
    1,
    Math.floor(Number(process.env.PACIFICA_LIVE_WALLET_FIRST_MAX_INFLIGHT_DIRECT || 1))
  );
  const positionsMaxConcurrency = Math.max(
    1,
    Number(
      process.env.PACIFICA_LIVE_WALLET_FIRST_POSITIONS_MAX_CONCURRENCY ||
        process.env.PACIFICA_LIVE_WALLET_FIRST_MAX_CONCURRENCY ||
        64
    )
  );
  const accountMaxConcurrency = Math.max(
    1,
    Number(
      process.env.PACIFICA_LIVE_WALLET_FIRST_DETECTOR_MAX_CONCURRENCY ||
        process.env.PACIFICA_LIVE_WALLET_FIRST_ACCOUNT_MAX_CONCURRENCY ||
        process.env.PACIFICA_LIVE_WALLET_FIRST_MAX_CONCURRENCY ||
        64
    )
  );
  const positionsMaxInFlightPerClient = Math.max(
    1,
    Math.floor(
      Number(
        process.env.PACIFICA_LIVE_WALLET_FIRST_POSITIONS_MAX_INFLIGHT_PER_CLIENT ||
          process.env.PACIFICA_LIVE_WALLET_FIRST_MAX_INFLIGHT_PER_CLIENT ||
          2
      )
    )
  );
  const positionsMaxInFlightDirect = Math.max(
    1,
    Math.floor(
      Number(
        process.env.PACIFICA_LIVE_WALLET_FIRST_POSITIONS_MAX_INFLIGHT_DIRECT ||
          process.env.PACIFICA_LIVE_WALLET_FIRST_MAX_INFLIGHT_DIRECT ||
          1
      )
    )
  );
  const accountMaxInFlightPerClient = Math.max(
    1,
    Math.floor(
      Number(
        process.env.PACIFICA_LIVE_WALLET_FIRST_DETECTOR_MAX_INFLIGHT_PER_CLIENT ||
        process.env.PACIFICA_LIVE_WALLET_FIRST_ACCOUNT_MAX_INFLIGHT_PER_CLIENT ||
          process.env.PACIFICA_LIVE_WALLET_FIRST_MAX_INFLIGHT_PER_CLIENT ||
          2
      )
    )
  );
  const accountMaxInFlightDirect = Math.max(
    1,
    Math.floor(
      Number(
        process.env.PACIFICA_LIVE_WALLET_FIRST_DETECTOR_MAX_INFLIGHT_DIRECT ||
        process.env.PACIFICA_LIVE_WALLET_FIRST_ACCOUNT_MAX_INFLIGHT_DIRECT ||
          process.env.PACIFICA_LIVE_WALLET_FIRST_MAX_INFLIGHT_DIRECT ||
          1
      )
    )
  );
  const positionsProxyCountPerShard = Math.max(
    0,
    Math.floor(Number(process.env.PACIFICA_LIVE_WALLET_FIRST_POSITIONS_PROXY_COUNT_PER_SHARD || 24))
  );
  const accountProxyCountPerShard = Math.max(
    0,
    Math.floor(
      Number(
        process.env.PACIFICA_LIVE_WALLET_FIRST_DETECTOR_PROXY_COUNT_PER_SHARD ||
        process.env.PACIFICA_LIVE_WALLET_FIRST_ACCOUNT_PROXY_COUNT_PER_SHARD ||
          positionsProxyCountPerShard ||
          24
      )
    )
  );
  const bootstrapSeedLimit = Math.max(
    0,
    Math.floor(Number(process.env.PACIFICA_LIVE_WALLET_FIRST_BOOTSTRAP_WALLET_SEED_LIMIT || 8000))
  );
  const logger = console;
  const hotWalletWsUseProxies =
    String(process.env.PACIFICA_HOT_WALLET_WS_USE_PROXIES || "false").toLowerCase() === "true";
  const hotWalletWsSubscribeAllWallets =
    String(process.env.PACIFICA_HOT_WALLET_WS_SUBSCRIBE_ALL_DATASET || "false").toLowerCase() ===
    "true";

  const retryPolicy = createRetryPolicy({
    maxAttempts: Math.max(1, Number(process.env.PACIFICA_REST_RETRY_MAX_ATTEMPTS || 2)),
    baseDelayMs: Math.max(100, Number(process.env.PACIFICA_REST_RETRY_BASE_DELAY_MS || 500)),
    maxDelayMs: Math.max(100, Number(process.env.PACIFICA_REST_RETRY_MAX_DELAY_MS || 5000)),
  });
  const clockSync = createClockSync();

  const shardProxies = loadProxyList(proxyFile);
  const safePositionsProxyCount =
    positionsProxyCountPerShard > 0
      ? Math.min(shardProxies.length, positionsProxyCountPerShard)
      : Math.min(shardProxies.length, Math.max(8, Math.floor(shardProxies.length * 0.2)));
  const safeAccountProxyCount =
    accountProxyCountPerShard > 0
      ? Math.min(shardProxies.length, accountProxyCountPerShard)
      : Math.min(shardProxies.length, Math.max(8, Math.floor(shardProxies.length * 0.2)));
  let effectiveAccountProxyRows = [];
  let effectivePositionsProxyRows = [];

  if (workerMode === "account_census") {
    effectiveAccountProxyRows = shardProxies.slice(0, safeAccountProxyCount || undefined);
  } else if (workerMode === "positions_materializer") {
    effectivePositionsProxyRows = shardProxies.slice(0, safePositionsProxyCount || undefined);
  } else {
    const sharedSelectionWindow = Math.min(
      shardProxies.length,
      Math.max(safePositionsProxyCount, safeAccountProxyCount) * 2
    );
    const topProxyRows =
      sharedSelectionWindow > 0 ? shardProxies.slice(0, sharedSelectionWindow) : shardProxies.slice();
    const positionsProxyRows = topProxyRows
      .filter((_, idx) => idx % 2 === 0)
      .slice(0, safePositionsProxyCount);
    const accountProxyRows = topProxyRows
      .filter((_, idx) => idx % 2 === 1)
      .slice(0, safeAccountProxyCount);
    effectiveAccountProxyRows =
      accountProxyRows.length > 0 ? accountProxyRows : shardProxies.slice(0, safeAccountProxyCount || undefined);
    effectivePositionsProxyRows =
      positionsProxyRows.length > 0
        ? positionsProxyRows
        : shardProxies.slice(0, safePositionsProxyCount || undefined);
  }

  const positionsEntries = createClientEntries({
    name: "positions",
    proxyRows: effectivePositionsProxyRows,
    includeDirect: positionsIncludeDirect,
    baseUrl: apiBase,
    apiConfigKey,
    transport,
    clockSync,
    retryPolicy,
    rateLimitWindowMs,
    rateLimitSafetyRatio,
    rpmCapPerIp,
    directRpmCap,
    logger,
  });
  const accountEntries = createClientEntries({
    name: "account",
    proxyRows: effectiveAccountProxyRows,
    includeDirect: accountIncludeDirect,
    baseUrl: apiBase,
    apiConfigKey,
    transport,
    clockSync,
    retryPolicy,
    rateLimitWindowMs,
    rateLimitSafetyRatio,
    rpmCapPerIp,
    directRpmCap,
    logger,
  });

  const accountPool = createRequestPool({
    name: "account_pool",
    entries: accountEntries,
    requestTimeoutMs: accountRequestTimeoutMs,
    maxFetchAttempts: accountMaxFetchAttempts,
    maxInFlightPerClient: accountMaxInFlightPerClient,
    maxInFlightDirect: accountMaxInFlightDirect,
    directFallbackOnLastAttempt: accountDirectFallbackOnLastAttempt,
    backoffBaseMs: Math.max(
      1000,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_RATE_LIMIT_BACKOFF_BASE_MS || 750)
    ),
    backoffMaxMs: Math.max(
      1000,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_RATE_LIMIT_BACKOFF_MAX_MS || 8000)
    ),
  });

  const publicActiveProvider = createTrackedPublicActiveWalletsProvider({
    datasetPath: walletDatasetPath,
    accountPool,
    refreshMs: Math.max(10000, Number(process.env.PACIFICA_PUBLIC_ACTIVE_CACHE_TTL_MS || 60000)),
    leaderboardEnabled:
      String(process.env.PACIFICA_LIVE_WALLET_FIRST_PUBLIC_ACTIVE_LEADERBOARD_ENABLED || "false").toLowerCase() ===
      "true",
    logger,
  });

  const triggerStore =
    String(process.env.PACIFICA_LIVE_WALLET_TRIGGER_STORE_ENABLED || "true").toLowerCase() !== "false"
      ? new LiveWalletTriggerStore({
          filePath: triggerFile,
          maxEntries: Math.max(
            5000,
            Number(process.env.PACIFICA_LIVE_WALLET_TRIGGER_MAX_ENTRIES || 50000)
          ),
        })
      : null;

  let monitor = null;
  const hotWalletWsMonitor = new HotWalletWsMonitor({
    enabled:
      workerMode !== "account_census" &&
      String(process.env.PACIFICA_HOT_WALLET_WS_ENABLED || "true").toLowerCase() !== "false",
    wsUrl,
    logger,
    subscribePositions:
      String(process.env.PACIFICA_HOT_WALLET_WS_SUBSCRIBE_POSITIONS || "true").toLowerCase() !==
      "false",
    subscribeAccountInfo:
      String(process.env.PACIFICA_HOT_WALLET_WS_SUBSCRIBE_ACCOUNT_INFO || "true").toLowerCase() !==
      "false",
    subscribeTrades:
      String(process.env.PACIFICA_HOT_WALLET_WS_SUBSCRIBE_TRADES || "false").toLowerCase() ===
      "true",
    subscribeOrderUpdates:
      String(
        process.env.PACIFICA_HOT_WALLET_WS_SUBSCRIBE_ORDER_UPDATES || "false"
      ).toLowerCase() === "true",
    retainColdWallets: hotWalletWsSubscribeAllWallets,
    proxyRows: hotWalletWsUseProxies
      ? effectivePositionsProxyRows.length > 0
        ? effectivePositionsProxyRows
        : shardProxies
      : [],
    walletAllowPredicate: (wallet) => {
      const normalized = String(wallet || "").trim();
      if (!normalized) return false;
      if (!monitor || !monitor.walletListSet || monitor.walletListSet.size <= 0) return true;
      return monitor.walletListSet.has(normalized);
    },
    maxWallets: Math.max(1, Number(process.env.PACIFICA_HOT_WALLET_WS_MAX_WALLETS || 1024)),
    initialWallets: Math.max(1, Number(process.env.PACIFICA_HOT_WALLET_WS_INITIAL_WALLETS || 768)),
    capacityStep: Math.max(1, Number(process.env.PACIFICA_HOT_WALLET_WS_CAPACITY_STEP || 128)),
    soakWindowMs: Math.max(5000, Number(process.env.PACIFICA_HOT_WALLET_WS_SOAK_WINDOW_MS || 5000)),
    maxScaleErrors: Math.max(0, Number(process.env.PACIFICA_HOT_WALLET_WS_MAX_SCALE_ERRORS || 64)),
    maxScaleReconnects: Math.max(
      0,
      Number(process.env.PACIFICA_HOT_WALLET_WS_MAX_SCALE_RECONNECTS || 512)
    ),
    minScaleUtilizationPct: Math.max(
      1,
      Math.min(100, Number(process.env.PACIFICA_HOT_WALLET_WS_MIN_SCALE_UTILIZATION_PCT || 50))
    ),
    minScaleBacklog: Math.max(0, Number(process.env.PACIFICA_HOT_WALLET_WS_MIN_SCALE_BACKLOG || 1)),
    memoryCeilingMb: Math.max(0, Number(process.env.PACIFICA_HOT_WALLET_WS_MEMORY_CEILING_MB || 6144)),
    inactivityMs: Math.max(15000, Number(process.env.PACIFICA_HOT_WALLET_WS_INACTIVITY_MS || 30000)),
    tradeHoldMs: Math.max(15000, Number(process.env.PACIFICA_HOT_WALLET_WS_TRADE_HOLD_MS || 30000)),
    openPositionHoldMs: Math.max(
      15000,
      Number(process.env.PACIFICA_HOT_WALLET_WS_OPEN_POSITION_HOLD_MS || 180000)
    ),
    aggressiveEvictMs: Math.max(
      10000,
      Number(process.env.PACIFICA_HOT_WALLET_WS_AGGRESSIVE_EVICT_MS || 5000)
    ),
    onPositions: (wallet, rows, meta) => {
      if (monitor && typeof monitor.ingestHotWalletPositions === "function") {
        monitor.ingestHotWalletPositions(wallet, rows, meta);
      }
    },
    onAccountInfo: (wallet, summary, meta) => {
      if (monitor && typeof monitor.ingestHotWalletAccountInfo === "function") {
        monitor.ingestHotWalletAccountInfo(wallet, summary, meta);
      }
    },
    onTrades: (wallet, rows, meta) => {
      const first = Array.isArray(rows) && rows.length ? rows[0] : null;
      if (monitor && typeof monitor.ingestHotWalletActivity === "function") {
        monitor.ingestHotWalletActivity(wallet, "ws_account_trade", {
          ...meta,
          symbol: first && first.s ? String(first.s).toUpperCase() : null,
          side: first && first.ts ? String(first.ts).toLowerCase() : null,
          amount: first && first.a !== undefined ? Number(first.a) : null,
          price: first && first.p !== undefined ? Number(first.p) : null,
          entry: first && first.o !== undefined ? Number(first.o) : null,
          eventType: first && first.te ? String(first.te).toLowerCase() : null,
          cause: first && first.tc ? String(first.tc).toLowerCase() : null,
        });
      }
    },
    onOrderUpdates: (wallet, rows, meta) => {
      const first = Array.isArray(rows) && rows.length ? rows[0] : null;
      if (monitor && typeof monitor.ingestHotWalletActivity === "function") {
        monitor.ingestHotWalletActivity(wallet, "ws_account_order_update", {
          ...meta,
          symbol: first && first.s ? String(first.s).toUpperCase() : null,
          side: first && first.d ? String(first.d).toLowerCase() : null,
          amount: first && first.a !== undefined ? Number(first.a) : null,
          price: first && first.p !== undefined ? Number(first.p) : null,
          entry: first && first.ip !== undefined ? Number(first.ip) : null,
          orderEvent: first && first.oe ? String(first.oe).toLowerCase() : null,
          orderStatus: first && first.os ? String(first.os).toLowerCase() : null,
          reduceOnly: Boolean(first && first.r),
        });
      }
    },
  });

  const coreEntries =
    workerMode === "account_census" ? accountEntries : positionsEntries;
  const coreMaxConcurrency =
    workerMode === "account_census" ? accountMaxConcurrency : positionsMaxConcurrency;
  const coreRequestTimeoutMs =
    workerMode === "account_census" ? accountRequestTimeoutMs : positionsRequestTimeoutMs;
  const coreMaxFetchAttempts =
    workerMode === "account_census" ? accountMaxFetchAttempts : positionsMaxFetchAttempts;
  const coreMaxInFlightPerClient =
    workerMode === "account_census"
      ? accountMaxInFlightPerClient
      : positionsMaxInFlightPerClient;
  const coreMaxInFlightDirect =
    workerMode === "account_census" ? accountMaxInFlightDirect : positionsMaxInFlightDirect;
  const coreDirectFallbackOnLastAttempt =
    workerMode === "account_census"
      ? accountDirectFallbackOnLastAttempt
      : positionsDirectFallbackOnLastAttempt;

  monitor = new WalletFirstLivePositionsMonitor({
    enabled: true,
    restClientEntries: coreEntries,
    hotWalletWsMonitor,
    triggerStore,
    publicActiveWalletsProvider: publicActiveProvider,
    livePublicActiveRemoteEnabled:
      String(
        process.env.PACIFICA_LIVE_WALLET_FIRST_PUBLIC_ACTIVE_REMOTE_ENABLED || "false"
      ).toLowerCase() === "true",
    logger,
    scanIntervalMs: Math.max(500, Number(process.env.PACIFICA_LIVE_WALLET_FIRST_SCAN_INTERVAL_MS || 500)),
    walletListRefreshMs: Math.max(
      1000,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_WALLET_LIST_REFRESH_MS || 3000)
    ),
    walletsPerPass: Math.max(0, Number(process.env.PACIFICA_LIVE_WALLET_FIRST_WALLETS_PER_PASS || 768)),
    hotWalletsPerPass: Math.max(
      0,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_HOT_WALLETS_PER_PASS || 192)
    ),
    warmWalletsPerPass: Math.max(
      0,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_WARM_WALLETS_PER_PASS || 128)
    ),
    recentActiveWalletsPerPass: Math.max(
      0,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_RECENT_ACTIVE_WALLETS_PER_PASS || 96)
    ),
    maxConcurrency: coreMaxConcurrency,
    requestTimeoutMs: coreRequestTimeoutMs,
    accountRequestTimeoutMs,
    maxFetchAttempts: coreMaxFetchAttempts,
    maxInFlightPerClient: coreMaxInFlightPerClient,
    maxInFlightDirect: coreMaxInFlightDirect,
    directFallbackOnLastAttempt: coreDirectFallbackOnLastAttempt,
    rateLimitBackoffBaseMs: Math.max(
      1000,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_RATE_LIMIT_BACKOFF_BASE_MS || 750)
    ),
    rateLimitBackoffMaxMs: Math.max(
      1000,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_RATE_LIMIT_BACKOFF_MAX_MS || 8000)
    ),
    shardCount,
    shardIndex,
    workerMode,
    maxEvents: Math.max(100, Number(process.env.PACIFICA_LIVE_WALLET_FIRST_MAX_EVENTS || 50000)),
    staleMs: Math.max(15000, Number(process.env.PACIFICA_LIVE_WALLET_FIRST_STALE_MS || 180000)),
    coolingMs: Math.max(5000, Number(process.env.PACIFICA_LIVE_WALLET_FIRST_COOLING_MS || 60000)),
    targetPassDurationMs: Math.max(
      3000,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_TARGET_PASS_MS || 3000)
    ),
    recentActivityTtlMs: Math.max(
      60000,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_RECENT_ACTIVITY_TTL_MS || 30 * 60 * 1000)
    ),
    warmWalletRecentMs: Math.max(
      60 * 60 * 1000,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_WARM_WALLET_RECENT_MS || 3 * 24 * 60 * 60 * 1000)
    ),
    maxWarmWallets: Math.max(
      100,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_MAX_WARM_WALLETS || 4000)
    ),
    hotReconcileMaxAgeMs: Math.max(
      3000,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_HOT_RECONCILE_MAX_AGE_MS || 5000)
    ),
    warmReconcileMaxAgeMs: Math.max(
      3000,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_WARM_RECONCILE_MAX_AGE_MS || 30000)
    ),
    gapRescueShare: clamp(
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_GAP_RESCUE_SHARE || 0.55),
      0.1,
      0.95
    ),
    gapRescueMinWorkers: Math.max(
      1,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_GAP_RESCUE_MIN_WORKERS || 12)
    ),
    openedEventMaxLagMs: Math.max(
      1000,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_OPEN_EVENT_MAX_LAG_MS || 15 * 60 * 1000)
    ),
    wsHotOnly:
      String(process.env.PACIFICA_LIVE_WALLET_FIRST_WS_HOT_ONLY || "false").toLowerCase() ===
      "true",
    ignorePersistedEvents:
      String(process.env.PACIFICA_LIVE_WALLET_FIRST_IGNORE_PERSISTED_EVENTS || "false").toLowerCase() ===
      "true",
    wsAccountInfoTriggerDedupMs: Math.max(
      1000,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_WS_ACCOUNT_INFO_TRIGGER_DEDUP_MS || 3000)
    ),
    hotOpenTriggerDedupMs: Math.max(
      100,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_HOT_OPEN_TRIGGER_DEDUP_MS || 1200)
    ),
    triggerPollMs: Math.max(100, Number(process.env.PACIFICA_LIVE_WALLET_TRIGGER_POLL_MS || 200)),
    wsReconcileCooldownMs: Math.max(
      100,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_WS_RECONCILE_COOLDOWN_MS || 120)
    ),
    wsReconcileMaxConcurrency: Math.max(
      1,
      Number(
        process.env.PACIFICA_LIVE_WALLET_FIRST_WS_RECONCILE_MAX_CONCURRENCY ||
          (workerMode === "positions_materializer" ? 96 : 48)
      )
    ),
    persistDir,
    persistPath: workerMode === "account_census" ? "" : positionsPersistPath,
    accountPersistPath,
    accountPersistReadPath: accountPersistPath,
    walletDatasetPath,
    subscribeAllWallets: hotWalletWsSubscribeAllWallets,
    walletDatasetRefreshMs: Math.max(
      1000,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_WALLET_DATASET_REFRESH_MS || 3000)
    ),
    unrefTimer: false,
    persistEveryMs: Math.max(
      500,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_PERSIST_EVERY_MS || 1000)
    ),
    maxPersistedEvents: Math.max(
      50,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_MAX_PERSISTED_EVENTS || 500)
    ),
    maxPersistedPositionOpenedEvents: Math.max(
      100,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_MAX_PERSISTED_OPEN_EVENTS || 10000)
    ),
    maxPositionOpenedEvents: Math.max(
      100,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_MAX_OPEN_EVENTS || 50000)
    ),
    clientSoftDisableMs: Math.max(
      15000,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_CLIENT_SOFT_DISABLE_MS || 90 * 1000)
    ),
    clientHardDisableMs: Math.max(
      30000,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_CLIENT_HARD_DISABLE_MS || 5 * 60 * 1000)
    ),
    clientSoftDisableConsecutiveTimeouts: Math.max(
      2,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_CLIENT_SOFT_DISABLE_TIMEOUTS || 3)
    ),
    clientHardDisableConsecutiveTimeouts: Math.max(
      3,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_CLIENT_HARD_DISABLE_TIMEOUTS || 6)
    ),
    clientSoftDisableConsecutiveProxyFailures: Math.max(
      2,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_CLIENT_SOFT_DISABLE_PROXY_FAILURES || 2)
    ),
    clientHardDisableConsecutiveProxyFailures: Math.max(
      3,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_CLIENT_HARD_DISABLE_PROXY_FAILURES || 4)
    ),
    clientDisableMinRequests: Math.max(
      6,
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_CLIENT_DISABLE_MIN_REQUESTS || 12)
    ),
    clientDisableLowSuccessRate: clamp(
      Number(process.env.PACIFICA_LIVE_WALLET_FIRST_CLIENT_DISABLE_LOW_SUCCESS_RATE || 0.12),
      0.01,
      0.9
    ),
  });

  if (workerMode === "account_census") {
    monitor.persistPath = "";
    monitor.pickHotRefreshWallets = () => [];
    monitor.pickAccountHintWallets = () => [];
    monitor.pickPositionsBootstrapWallets = () => [];
    monitor.refreshWalletPositions = monitor.accountCensusWallet.bind(monitor);
    monitor.scanWallet = monitor.accountCensusWallet.bind(monitor);
  } else if (workerMode === "positions_materializer") {
    monitor.accountPersistReadPath = accountPersistPath;
    monitor.accountPersistPath = "";
    monitor.pickDiscoveryWallets = () => [];
    monitor.computeBatchTarget = () => 0;
    monitor.walletNeedsFullScan = () => false;
    monitor.splitWalletsByScanNeed = function splitWalletsByScanNeedPositionsOnly(wallets = []) {
      return {
        scan: [],
        refresh: (Array.isArray(wallets) ? wallets : []).filter(Boolean),
      };
    };
    monitor.scanWallet = monitor.refreshWalletPositions.bind(monitor);
    monitor.refreshPublicActiveWallets = async function refreshPublicActiveWalletsNoop() {
      return this.publicActiveWalletList;
    };
  }

  monitor.fetchWalletAccount = async function fetchWalletAccount(wallet) {
    let payload;
    try {
      payload = await accountPool.get("/account", {
        query: { account: wallet },
        timeoutMs: accountRequestTimeoutMs,
        allowDirect: true,
      });
    } catch (error) {
      const message = String(error && error.message ? error.message : error || "").toLowerCase();
      if (message.includes("404") && message.includes("account not found")) {
        return {
          positionsCount: 0,
          ordersCount: 0,
          updatedAt: Date.now(),
          raw: {},
        };
      }
      throw error;
    }
    const data = payload && payload.data && typeof payload.data === "object" ? payload.data : {};
    return {
      positionsCount: Math.max(0, Number(data.positions_count || data.positionsCount || 0) || 0),
      ordersCount: Math.max(0, Number(data.orders_count || data.ordersCount || 0) || 0),
      updatedAt: normalizeTimestampMs(
        data.updated_at !== undefined ? data.updated_at : data.updatedAt,
        Date.now()
      ),
      raw: data,
    };
  };

  const originalRefreshPublicActiveWallets = monitor.refreshPublicActiveWallets.bind(monitor);
  monitor.refreshPublicActiveWallets = async function refreshPublicActiveWallets(force = false) {
    const now = Date.now();
    if (
      !force &&
      this.publicActiveWalletList.length > 0 &&
      now - Number(this.lastPublicActiveRefreshAt || 0) < this.publicActiveRefreshMs
    ) {
      return this.publicActiveWalletList;
    }
    try {
      const wallets = await publicActiveProvider();
      const tracked = new Set(this.walletList);
      this.publicActiveWalletList = (Array.isArray(wallets) ? wallets : [])
        .slice(0, bootstrapSeedLimit > 0 ? bootstrapSeedLimit : undefined)
        .map((wallet) => normalizeWallet(wallet))
        .filter(Boolean)
        .filter((wallet) =>
          this.shardCount > 1
            ? this.walletBelongsToShard(wallet)
            : true
        )
        .filter((wallet) => tracked.size === 0 || tracked.has(wallet));
      this.status.publicActiveWallets = this.publicActiveWalletList.length;
      this.lastPublicActiveRefreshAt = now;
      if (this.publicActiveWalletCursor >= this.publicActiveWalletList.length) {
        this.publicActiveWalletCursor = 0;
      }
      return this.publicActiveWalletList;
    } catch (_error) {
      return this.livePublicActiveRemoteEnabled
        ? originalRefreshPublicActiveWallets(force)
        : this.publicActiveWalletList;
    }
  };

  const originalGetStatus = monitor.getStatus.bind(monitor);
  monitor.getStatus = function patchedGetStatus() {
    const status = originalGetStatus();
    const wsHotOnlyEnabled =
      String(process.env.PACIFICA_LIVE_WALLET_FIRST_WS_HOT_ONLY || "false").toLowerCase() ===
      "true";
    const zeroLaneStatus = {
      clientsTotal: 0,
      proxiedClients: 0,
      clientsCooling: 0,
      clientsDisabled: 0,
      clientsInFlight: 0,
      clients429: 0,
      healthyClients: 0,
      avgClientLatencyMs: 0,
      timeoutClients: 0,
      proxyFailingClients: 0,
      lastError: null,
      lastErrorAt: 0,
    };
    const accountStatus =
      workerMode === "positions_materializer" ? zeroLaneStatus : accountPool.getStatus();
    const effectiveCoreStatus =
      workerMode === "account_census"
        ? {
            ...status,
            clientsTotal: accountStatus.clientsTotal,
            proxiedClients: accountStatus.proxiedClients,
            clientsCooling: accountStatus.clientsCooling,
            clientsDisabled: accountStatus.clientsDisabled,
            clientsInFlight: accountStatus.clientsInFlight,
            clients429: accountStatus.clients429,
            healthyClients: accountStatus.healthyClients,
            avgClientLatencyMs: accountStatus.avgClientLatencyMs,
            timeoutClients: accountStatus.timeoutClients,
            proxyFailingClients: accountStatus.proxyFailingClients,
          }
        : status;
    return {
      ...effectiveCoreStatus,
      mode: `wallet_first_live_trade_worker_${wsHotOnlyEnabled ? "ws_hot_only" : workerMode}`,
      positionsClientsTotal:
        workerMode === "account_census" ? 0 : status.clientsTotal,
      positionsProxiedClients:
        workerMode === "account_census" ? 0 : status.proxiedClients,
      positionsClientsCooling:
        workerMode === "account_census" ? 0 : status.clientsCooling,
      positionsClientsDisabled:
        workerMode === "account_census" ? 0 : status.clientsDisabled,
      positionsClientsInFlight:
        workerMode === "account_census" ? 0 : status.clientsInFlight,
      positionsClients429:
        workerMode === "account_census" ? 0 : status.clients429,
      positionsHealthyClients:
        workerMode === "account_census" ? 0 : status.healthyClients,
      positionsAvgClientLatencyMs:
        workerMode === "account_census" ? 0 : status.avgClientLatencyMs,
      positionsTimeoutClients:
        workerMode === "account_census" ? 0 : status.timeoutClients,
      positionsProxyFailingClients:
        workerMode === "account_census" ? 0 : status.proxyFailingClients,
      accountClientsTotal: accountStatus.clientsTotal,
      accountProxiedClients: accountStatus.proxiedClients,
      accountClientsCooling: accountStatus.clientsCooling,
      accountClientsDisabled: accountStatus.clientsDisabled,
      accountClientsInFlight: accountStatus.clientsInFlight,
      accountClients429: accountStatus.clients429,
      accountHealthyClients: accountStatus.healthyClients,
      accountAvgClientLatencyMs: accountStatus.avgClientLatencyMs,
      accountTimeoutClients: accountStatus.timeoutClients,
      accountProxyFailingClients: accountStatus.proxyFailingClients,
      accountLastError: accountStatus.lastError,
      accountLastErrorAt: accountStatus.lastErrorAt,
    };
  };

  const logIntervalMs = Math.max(
    5000,
    Number(process.env.PACIFICA_LIVE_WALLET_FIRST_LOG_INTERVAL_MS || 15000)
  );
  let logTimer = null;
  function stopAll(exitCode = 0) {
    try {
      if (logTimer) clearInterval(logTimer);
      monitor.stop();
    } finally {
      process.exit(exitCode);
    }
  }

  process.on("SIGINT", () => stopAll(0));
  process.on("SIGTERM", () => stopAll(0));
  process.on("uncaughtException", (error) => {
    console.error(`[live-trade-worker] uncaught_exception ${String(error && error.stack ? error.stack : error)}`);
    stopAll(1);
  });
  process.on("unhandledRejection", (error) => {
    console.error(`[live-trade-worker] unhandled_rejection ${String(error && error.stack ? error.stack : error)}`);
    stopAll(1);
  });

  const wsHotOnlyEnabled =
    String(process.env.PACIFICA_LIVE_WALLET_FIRST_WS_HOT_ONLY || "false").toLowerCase() ===
    "true";
  const runtimeModeLabel = wsHotOnlyEnabled ? "ws_hot_only" : workerMode;
  console.log(
    `[live-trade-worker] mode=${runtimeModeLabel} shard=${shardIndex}/${shardCount} proxy_file=${proxyFile} account_proxies=${effectiveAccountProxyRows.length} positions_proxies=${effectivePositionsProxyRows.length} positions_include_direct=${positionsIncludeDirect} positions_timeout_ms=${positionsRequestTimeoutMs} account_timeout_ms=${accountRequestTimeoutMs} dataset=${walletDatasetPath}`
  );

  monitor.start();
  if (
    workerMode === "account_census" &&
    monitor.timer &&
    typeof monitor.timer.ref === "function"
  ) {
    monitor.timer.ref();
  }
  logTimer = setInterval(() => {
    const status = monitor.getStatus();
    console.log(
      `[live-trade-worker] mode=${runtimeModeLabel} shard=${status.shardIndex}/${status.shardCount} open_wallets=${status.walletsWithOpenPositions} open_positions=${status.openPositionsTotal} hinted_wallets=${status.accountIndicatedOpenWallets} hinted_positions=${status.accountIndicatedOpenPositionsTotal} gap_wallets=${status.positionMaterializationGapWallets} gap_positions=${status.positionMaterializationGapTotal} account_latency_ms=${status.accountAvgClientLatencyMs} positions_latency_ms=${status.positionsAvgClientLatencyMs} account_timeout_clients=${status.accountTimeoutClients} positions_timeout_clients=${status.positionsTimeoutClients} last_error=${status.lastError || status.accountLastError || "none"}`
    );
  }, logIntervalMs);
  if (logTimer && typeof logTimer.unref === "function") logTimer.unref();
}

main().catch((error) => {
  console.error(`[live-trade-worker] fatal ${String(error && error.stack ? error.stack : error)}`);
  process.exit(1);
});
