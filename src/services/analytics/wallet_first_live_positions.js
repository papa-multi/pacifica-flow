const { readJson } = require("../pipeline/utils");
const {
  buildShardSnapshotPath,
  writeShardSnapshot,
} = require("./live_positions_snapshot_store");

function toNum(value, fallback = NaN) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function normalizeWallet(value) {
  const text = String(value || "").trim();
  return text || "";
}

function normalizeSymbol(value) {
  const text = String(value || "").trim().toUpperCase();
  return text || "";
}

function normalizeSide(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return "";
  if (raw === "bid") return "open_long";
  if (raw === "ask") return "open_short";
  return raw;
}

function normalizeTimestampMs(value, fallback = 0) {
  const ts = Number(value);
  if (!Number.isFinite(ts) || ts <= 0) return Number(fallback || 0);
  return ts < 1e12 ? ts * 1000 : ts;
}

function stableWalletHash(value) {
  const text = String(value || "");
  let hash = 5381;
  for (let i = 0; i < text.length; i += 1) {
    hash = ((hash << 5) + hash + text.charCodeAt(i)) >>> 0;
  }
  return hash >>> 0;
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function safeWindow(row, key) {
  return row && row[key] && typeof row[key] === "object" ? row[key] : {};
}

function buildWalletPriorityMeta(row = {}) {
  const d24 = safeWindow(row, "d24");
  const d7 = safeWindow(row, "d7");
  const d30 = safeWindow(row, "d30");
  const all = safeWindow(row, "all");
  const volume24 = Math.max(0, toNum(d24.volumeUsd, 0));
  const volume7Daily = Math.max(0, toNum(d7.volumeUsd, 0) / 7);
  const volume30Daily = Math.max(0, toNum(d30.volumeUsd, 0) / 30);
  const recentVolumeUsd = Math.max(volume24, volume7Daily, volume30Daily);
  const trades24 = Math.max(0, toNum(d24.trades, 0));
  const trades7Daily = Math.max(0, toNum(d7.trades, 0) / 7);
  const trades30Daily = Math.max(0, toNum(d30.trades, 0) / 30);
  const recentTradeCount = Math.round(Math.max(trades24, trades7Daily, trades30Daily));
  const openPositions = Math.max(0, toNum(row.openPositions, 0));
  const liveActiveRank = Math.max(0, toNum(row.liveActiveRank, 0));
  const rankTs = Math.max(
    Number(row.liveScannedAt || 0),
    Number(row.updatedAt || 0),
    Number(d24.lastTrade || 0),
    Number(d7.lastTrade || 0),
    Number(d30.lastTrade || 0),
    Number(all.lastTrade || 0)
  );
  const rankBonus = liveActiveRank > 0 ? Math.max(0, 25_000 - liveActiveRank * 150) : 0;
  const priorityBoost = Math.round(
    Math.min(
      350_000,
      openPositions * 150_000 +
        Math.sqrt(Math.max(0, recentVolumeUsd)) * 80 +
        recentTradeCount * 250 +
        rankBonus
    )
  );
  return {
    recentVolumeUsd: Number(recentVolumeUsd.toFixed(2)),
    recentTradeCount,
    openPositions,
    liveActiveRank,
    rankTs,
    priorityBoost,
    wsPriorityScore: priorityBoost + Math.round(Math.min(200_000, recentVolumeUsd / 25)),
  };
}

function positionKey(row = {}) {
  const symbol = normalizeSymbol(row.symbol);
  const sideRaw = String(row.rawSide || row.sideRaw || row.side || "").trim().toLowerCase();
  const isolated = row.isolated ? "iso" : "cross";
  const entry = Number.isFinite(toNum(row.entry, NaN)) ? Number(toNum(row.entry, 0)).toFixed(8) : "na";
  return `${symbol}|${sideRaw || "na"}|${isolated}|${entry}`;
}

function normalizePositionRow(wallet, row = {}) {
  const symbol = normalizeSymbol(row.symbol);
  if (!symbol) return null;

  const rawSide = String(row.side || "").trim().toLowerCase();
  const side = normalizeSide(rawSide);
  const amount = Math.abs(toNum(row.amount, 0));
  const entry = toNum(row.entry_price !== undefined ? row.entry_price : row.entryPrice, NaN);
  const mark = toNum(
    row.mark_price !== undefined ? row.mark_price : row.markPrice !== undefined ? row.markPrice : entry,
    NaN
  );
  const notionalFromMark = Number.isFinite(mark) ? Math.abs(amount * mark) : NaN;
  const notionalFromEntry = Number.isFinite(entry) ? Math.abs(amount * entry) : NaN;
  const positionUsd = Number.isFinite(notionalFromMark)
    ? notionalFromMark
    : Number.isFinite(notionalFromEntry)
    ? notionalFromEntry
    : 0;

  const openedAt = normalizeTimestampMs(
    row.created_at !== undefined ? row.created_at : row.createdAt,
    Date.now()
  );
  const updatedAt = normalizeTimestampMs(
    row.updated_at !== undefined ? row.updated_at : row.updatedAt,
    openedAt
  );

  const normalized = {
    wallet,
    symbol,
    rawSide,
    side,
    size: Number.isFinite(amount) ? amount : 0,
    positionUsd,
    entry: Number.isFinite(entry) ? entry : NaN,
    mark: Number.isFinite(mark) ? mark : NaN,
    pnl: toNum(
      row.unrealized_pnl !== undefined
        ? row.unrealized_pnl
        : row.unrealizedPnl !== undefined
        ? row.unrealizedPnl
        : row.pnl,
      NaN
    ),
    status: "open",
    walletSource: "wallet_positions_api",
    walletConfidence: "hard_payload",
    txSignature: "",
    txSource: "wallet_positions_api",
    txConfidence: "unresolved",
    tradeRef: "",
    tradeRefType: "wallet_position",
    openedAt,
    updatedAt,
    timestamp: updatedAt,
    source: "wallet_first_positions",
    isolated: Boolean(row.isolated),
    margin: toNum(row.margin, NaN),
    funding: toNum(row.funding, NaN),
    liquidationPrice: toNum(
      row.liquidation_price !== undefined ? row.liquidation_price : row.liquidationPrice,
      NaN
    ),
    raw: row,
  };

  normalized.positionKey = positionKey(normalized);
  return normalized;
}

function compareAmount(a, b) {
  const av = Number.isFinite(toNum(a, NaN)) ? toNum(a, 0) : 0;
  const bv = Number.isFinite(toNum(b, NaN)) ? toNum(b, 0) : 0;
  return Math.abs(av - bv) <= 1e-10;
}

class WalletFirstLivePositionsMonitor {
  constructor(options = {}) {
    this.logger = options.logger || console;
    this.restClient = options.restClient || null;
    this.walletStore = options.walletStore || null;
    this.hotWalletWsMonitor = options.hotWalletWsMonitor || null;
    this.triggerStore = options.triggerStore || null;
    const entryCandidates = Array.isArray(options.restClientEntries)
      ? options.restClientEntries
      : [];
    const normalizedEntries = entryCandidates
      .map((entry, idx) => {
        const client =
          entry && entry.client && typeof entry.client.get === "function"
            ? entry.client
            : null;
        if (!client) return null;
        return {
          id: String((entry && entry.id) || `client_${idx + 1}`),
          client,
          proxyUrl: entry && entry.proxyUrl ? String(entry.proxyUrl) : null,
        };
      })
      .filter(Boolean);
    if (!normalizedEntries.length && this.restClient && typeof this.restClient.get === "function") {
      normalizedEntries.push({
        id: "direct",
        client: this.restClient,
        proxyUrl: null,
      });
    }
    this.restClientEntries = normalizedEntries;
    this.clientStates = this.restClientEntries.map((entry) => ({
      id: entry.id,
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
    this.directClientIndex = this.restClientEntries.findIndex(
      (entry) => entry && String(entry.id || "").toLowerCase() === "direct"
    );
    this.clientCursor = 0;

    this.enabled = Boolean(options.enabled) && this.restClientEntries.length > 0;
    this.scanIntervalMs = Math.max(500, Number(options.scanIntervalMs || 5000));
    this.walletListRefreshMs = Math.max(5000, Number(options.walletListRefreshMs || 60000));
    const walletsPerPassRaw = Number(options.walletsPerPass);
    this.walletsPerPass = Number.isFinite(walletsPerPassRaw)
      ? Math.max(0, Math.floor(walletsPerPassRaw))
      : 12;
    this.maxConcurrency = Math.max(1, Number(options.maxConcurrency || 4));
    this.hotWalletsPerPass = Math.max(0, Number(options.hotWalletsPerPass || 4));
    this.warmWalletsPerPass = Math.max(
      0,
      Number(options.warmWalletsPerPass || Math.max(16, this.hotWalletsPerPass))
    );
    this.recentActiveWalletsPerPass = Math.max(
      0,
      Number(options.recentActiveWalletsPerPass || Math.max(8, Math.floor(this.hotWalletsPerPass / 2)))
    );
    this.requestTimeoutMs = Math.max(1500, Number(options.requestTimeoutMs || 4000));
    this.maxFetchAttempts = Math.max(1, Math.min(5, Number(options.maxFetchAttempts || 2)));
    this.maxInFlightPerClient = Math.max(1, Number(options.maxInFlightPerClient || 2));
    this.maxInFlightDirect = Math.max(1, Number(options.maxInFlightDirect || 1));
    this.directFallbackOnLastAttempt = Boolean(options.directFallbackOnLastAttempt);
    this.targetPassDurationMs = Math.max(
      3000,
      Number(options.targetPassDurationMs || 15000)
    );
    this.recentActivityTtlMs = Math.max(
      60000,
      Number(options.recentActivityTtlMs || 30 * 60 * 1000)
    );
    this.warmWalletRecentMs = Math.max(
      60 * 60 * 1000,
      Number(options.warmWalletRecentMs || 3 * 24 * 60 * 60 * 1000)
    );
    this.maxWarmWallets = Math.max(
      100,
      Number(options.maxWarmWallets || 4000)
    );
    this.hotReconcileMaxAgeMs = Math.max(
      5000,
      Number(options.hotReconcileMaxAgeMs || Math.max(this.scanIntervalMs * 8, 30 * 1000))
    );
    this.warmReconcileMaxAgeMs = Math.max(
      this.hotReconcileMaxAgeMs,
      Number(options.warmReconcileMaxAgeMs || Math.max(this.scanIntervalMs * 24, 3 * 60 * 1000))
    );
    this.persistDir = options.persistDir ? String(options.persistDir).trim() : "";
    this.persistEveryMs = Math.max(1000, Number(options.persistEveryMs || 5000));
    this.maxPersistedEvents = Math.max(
      50,
      Number(options.maxPersistedEvents || 500)
    );
    this.maxEvents = Math.max(100, Number(options.maxEvents || 50000));
    this.staleMs = Math.max(15000, Number(options.staleMs || 180000));
    this.coolingMs = Math.max(5000, Number(options.coolingMs || 60000));
    this.triggerPollMs = Math.max(500, Number(options.triggerPollMs || 2000));
    this.triggerReadLimit = Math.max(200, Number(options.triggerReadLimit || 5000));
    this.rateLimitBackoffBaseMs = Math.max(1000, Number(options.rateLimitBackoffBaseMs || 5000));
    this.rateLimitBackoffMaxMs = Math.max(
      this.rateLimitBackoffBaseMs,
      Number(options.rateLimitBackoffMaxMs || 120000)
    );
    this.rateLimitCooldownUntil = 0;
    this.rateLimitStreak = 0;
    this.shardCount = Math.max(1, Math.floor(Number(options.shardCount || 1)));
    const shardIndexRaw = Math.floor(Number(options.shardIndex || 0));
    this.shardIndex = Math.min(this.shardCount - 1, Math.max(0, Number.isFinite(shardIndexRaw) ? shardIndexRaw : 0));
    this.persistPath = options.persistPath
      ? String(options.persistPath).trim()
      : this.persistDir
      ? buildShardSnapshotPath(this.persistDir, this.shardIndex)
      : "";

    this.walletList = [];
    this.warmWalletList = [];
    this.highValueWalletList = [];
    this.walletPriorityMeta = new Map();
    this.walletCursor = 0;
    this.openWalletCursor = 0;
    this.warmWalletCursor = 0;
    this.highValueWalletCursor = 0;
    this.recentActiveWalletCursor = 0;
    this.priorityWalletQueue = [];
    this.priorityWalletSet = new Set();
    this.lastWalletListRefreshAt = 0;
    this.lastTriggerLoadAt = 0;
    this.lastExternalTriggerAt = 0;

    this.walletSnapshots = new Map();
    this.walletLifecycle = new Map();
    this.successScannedWallets = new Set();
    this.openWalletSet = new Set();
    this.recentActiveWalletAt = new Map();
    this.events = [];

    this.flatPositionsCache = [];
    this.flatPositionsDirty = true;
    this.avgWalletScanMs = this.requestTimeoutMs;

    this.running = false;
    this.timer = null;
    this.warming = false;
    this.loadedPersistedState = false;
    this.lastPersistAt = 0;

    this.status = {
      enabled: this.enabled,
      mode: "wallet_first_tracked_wallet_positions",
      startedAt: this.enabled ? Date.now() : null,
      lastPassStartedAt: null,
      lastPassFinishedAt: null,
      lastPassDurationMs: null,
      passThroughputRps: 0,
      estimatedSweepSeconds: null,
      lastSuccessAt: null,
      lastErrorAt: null,
      lastError: null,
      scannedWalletsTotal: 0,
      scannedWallets1h: 0,
      failedWalletsTotal: 0,
      failedWallets1h: 0,
      passes: 0,
      walletsKnownGlobal: 0,
      walletsKnown: 0,
      walletsScannedAtLeastOnce: 0,
      walletsCoveragePct: 0,
      walletsWithOpenPositions: 0,
      openPositionsTotal: 0,
      recentChanges1h: 0,
      recentChanges24h: 0,
      lastEventAt: null,
      lastBatchWallets: 0,
      batchTargetWallets: 0,
      queueCursor: 0,
      warmupDone: false,
      cooldownUntil: null,
      cooldownMsRemaining: 0,
      clientsTotal: this.restClientEntries.length,
      clientsCooling: 0,
      clientsInFlight: 0,
      clients429: 0,
      avgWalletScanMs: this.avgWalletScanMs,
      targetPassDurationMs: this.targetPassDurationMs,
      recentActiveWallets: 0,
      warmWallets: 0,
      highValueWallets: 0,
      hotWsActiveWallets: 0,
      hotWsOpenConnections: 0,
      hotWsDroppedPromotions: 0,
      hotWsCapacity: 0,
      hotWsTriggerToEventAvgMs: 0,
      hotWsLastTriggerToEventMs: 0,
      hotWsReconnectTransitions: 0,
      hotWsErrorCount: 0,
      hotWsAvailableSlots: 0,
      hotWsCapacityCeiling: 0,
      hotWsPromotionBacklog: 0,
      hotWsScaleEvents: 0,
      hotWsProcessRssMb: 0,
      lifecycleHotWallets: 0,
      lifecycleWarmWallets: 0,
      lifecycleColdWallets: 0,
      staleWallets: 0,
      freshWallets: 0,
      coolingWallets: 0,
      priorityQueueDepth: 0,
      hotReconcileDueWallets: 0,
      warmReconcileDueWallets: 0,
      healthyClients: 0,
      avgClientLatencyMs: 0,
      timeoutClients: 0,
      proxyFailingClients: 0,
      shardIndex: this.shardIndex,
      shardCount: this.shardCount,
    };
  }

  updateCoverage() {
    const known = Math.max(0, Number(this.status.walletsKnown || 0));
    const scanned = Math.max(0, Number(this.status.walletsScannedAtLeastOnce || 0));
    const coveragePct = known > 0 ? (scanned / known) * 100 : 0;
    this.status.walletsCoveragePct = Number(coveragePct.toFixed(2));
    this.status.warmupDone = known > 0 ? scanned >= known : false;
  }

  ensureLifecycle(wallet) {
    const normalized = normalizeWallet(wallet);
    if (!normalized) return null;
    let row = this.walletLifecycle.get(normalized);
    if (row) return row;
    row = {
      wallet: normalized,
      lifecycle: "cold",
      lastStateChangeAt: 0,
      lastTriggerAt: 0,
      lastTradeAt: 0,
      lastOrderAt: 0,
      lastPositionAt: 0,
      lastOpenPositionAt: 0,
      lastReconcileAt: 0,
      lastSuccessAt: 0,
      lastErrorAt: 0,
      lastActivityAt: 0,
      lastPromotionReason: null,
      lastDemotionReason: null,
      openPositionsCount: 0,
    };
    this.walletLifecycle.set(normalized, row);
    return row;
  }

  recalculateLifecycle(row, now = Date.now(), meta = {}) {
    if (!row) return null;
    const nextOpenCount =
      meta.openPositionsCount !== undefined
        ? Math.max(0, Number(meta.openPositionsCount || 0))
        : Math.max(0, Number(row.openPositionsCount || 0));
    row.openPositionsCount = nextOpenCount;
    const recentHotSignalAt = Math.max(
      Number(row.lastTriggerAt || 0),
      Number(row.lastTradeAt || 0),
      Number(row.lastOrderAt || 0),
      Number(row.lastOpenPositionAt || 0)
    );
    const recentWarmSignalAt = Math.max(
      recentHotSignalAt,
      Number(row.lastPositionAt || 0),
      Number(row.lastReconcileAt || 0),
      Number(row.lastSuccessAt || 0)
    );
    let nextLifecycle = "cold";
    if (
      nextOpenCount > 0 ||
      (recentHotSignalAt > 0 && now - recentHotSignalAt <= this.hotReconcileMaxAgeMs)
    ) {
      nextLifecycle = "hot";
    } else if (
      recentWarmSignalAt > 0 &&
      now - recentWarmSignalAt <= Math.max(this.warmReconcileMaxAgeMs, this.warmWalletRecentMs)
    ) {
      nextLifecycle = "warm";
    }
    if (nextLifecycle !== row.lifecycle) {
      row.lastStateChangeAt = now;
      if (nextLifecycle === "cold") {
        row.lastDemotionReason = String(meta.reason || "inactive").trim() || "inactive";
      } else {
        row.lastPromotionReason = String(meta.reason || nextLifecycle).trim() || nextLifecycle;
      }
      row.lifecycle = nextLifecycle;
    }
    return row.lifecycle;
  }

  touchLifecycle(wallet, meta = {}) {
    const row = this.ensureLifecycle(wallet);
    if (!row) return null;
    const now = Math.max(0, Number(meta.at || Date.now()));
    row.lastActivityAt = Math.max(row.lastActivityAt, now);
    row.lastSuccessAt = meta.success === true ? Math.max(row.lastSuccessAt, now) : row.lastSuccessAt;
    row.lastReconcileAt =
      meta.reconcile === true ? Math.max(row.lastReconcileAt, now) : row.lastReconcileAt;
    row.lastTriggerAt =
      meta.trigger === true ? Math.max(row.lastTriggerAt, now) : row.lastTriggerAt;
    row.lastTradeAt = meta.trade === true ? Math.max(row.lastTradeAt, now) : row.lastTradeAt;
    row.lastOrderAt = meta.order === true ? Math.max(row.lastOrderAt, now) : row.lastOrderAt;
    row.lastPositionAt =
      meta.position === true ? Math.max(row.lastPositionAt, now) : row.lastPositionAt;
    if (meta.openPositionsCount !== undefined) {
      row.openPositionsCount = Math.max(0, Number(meta.openPositionsCount || 0));
      if (row.openPositionsCount > 0) {
        row.lastOpenPositionAt = Math.max(row.lastOpenPositionAt, now);
      }
    }
    if (meta.error === true) {
      row.lastErrorAt = Math.max(row.lastErrorAt, now);
    }
    return this.recalculateLifecycle(row, now, meta);
  }

  buildDueWalletList(targetLifecycle, maxAgeMs, now = Date.now()) {
    return Array.from(this.walletLifecycle.values())
      .filter((row) => row && row.lifecycle === targetLifecycle)
      .sort((a, b) => {
        const aDue =
          Math.max(0, now - Math.max(Number(a.lastReconcileAt || 0), Number(a.lastSuccessAt || 0))) >=
          maxAgeMs
            ? 1
            : 0;
        const bDue =
          Math.max(0, now - Math.max(Number(b.lastReconcileAt || 0), Number(b.lastSuccessAt || 0))) >=
          maxAgeMs
            ? 1
            : 0;
        if (bDue !== aDue) return bDue - aDue;
        const aLast = Math.max(Number(a.lastReconcileAt || 0), Number(a.lastSuccessAt || 0));
        const bLast = Math.max(Number(b.lastReconcileAt || 0), Number(b.lastSuccessAt || 0));
        if (aLast !== bLast) return aLast - bLast;
        return String(a.wallet).localeCompare(String(b.wallet));
      })
      .map((row) => row.wallet);
  }

  start() {
    if (!this.enabled || this.timer) return;
    if (!this.loadedPersistedState) this.loadPersistedState();
    if (this.hotWalletWsMonitor && typeof this.hotWalletWsMonitor.start === "function") {
      this.hotWalletWsMonitor.start();
      this.seedHotWalletTier();
    }
    this.timer = setInterval(() => {
      this.runPass().catch((error) => {
        this.status.lastErrorAt = Date.now();
        this.status.lastError = String(error && error.message ? error.message : error);
      });
    }, this.scanIntervalMs);
    if (this.timer && typeof this.timer.unref === "function") this.timer.unref();

    // Kick off quickly so API does not stay empty until first interval tick.
    this.runPass().catch((error) => {
      this.status.lastErrorAt = Date.now();
      this.status.lastError = String(error && error.message ? error.message : error);
    });
  }

  stop() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    if (this.hotWalletWsMonitor && typeof this.hotWalletWsMonitor.stop === "function") {
      this.hotWalletWsMonitor.stop();
    }
    this.persistMaybe(true);
  }

  ensureWalletList() {
    const now = Date.now();
    if (
      this.walletList.length > 0 &&
      now - Number(this.lastWalletListRefreshAt || 0) < this.walletListRefreshMs
    ) {
      return;
    }
    if (!this.walletStore || typeof this.walletStore.list !== "function") {
      this.walletList = [];
      this.status.walletsKnown = 0;
      this.lastWalletListRefreshAt = now;
      return;
    }

    const rows = this.walletStore.list();
    const unique = new Set();
    const sorted = (Array.isArray(rows) ? rows : [])
      .map((row) => ({
        wallet: normalizeWallet(row && row.wallet),
        priority: buildWalletPriorityMeta(row),
      }))
      .filter((row) => row.wallet)
      .sort((a, b) => Number(b.priority.rankTs || 0) - Number(a.priority.rankTs || 0))
      .filter((row) => {
        if (unique.has(row.wallet)) return false;
        unique.add(row.wallet);
        return true;
      });

    this.status.walletsKnownGlobal = sorted.length;
    const shardFiltered =
      this.shardCount > 1
        ? sorted.filter((row) => stableWalletHash(row.wallet) % this.shardCount === this.shardIndex)
        : sorted;
    this.walletPriorityMeta = new Map(shardFiltered.map((row) => [row.wallet, row.priority]));
    this.walletList = shardFiltered.map((row) => row.wallet);
    const warmCutoff = now - this.warmWalletRecentMs;
    this.warmWalletList = shardFiltered
      .filter(
        (row) =>
          Number(row.priority.rankTs || 0) >= warmCutoff ||
          Number(row.priority.openPositions || 0) > 0 ||
          Number(row.priority.priorityBoost || 0) >= 20_000
      )
      .slice(0, this.maxWarmWallets)
      .map((row) => row.wallet);
    this.highValueWalletList = shardFiltered
      .slice()
      .sort(
        (a, b) => Number(b.priority.wsPriorityScore || 0) - Number(a.priority.wsPriorityScore || 0)
      )
      .slice(0, Math.max(this.maxWarmWallets, this.hotWalletsPerPass * 32, 1000))
      .map((row) => row.wallet);
    this.status.walletsKnown = this.walletList.length;
    this.status.warmWallets = this.warmWalletList.length;
    this.status.highValueWallets = this.highValueWalletList.length;
    this.lastWalletListRefreshAt = now;
    if (this.walletCursor >= this.walletList.length) this.walletCursor = 0;
    if (this.warmWalletCursor >= this.warmWalletList.length) this.warmWalletCursor = 0;
    if (this.highValueWalletCursor >= this.highValueWalletList.length) this.highValueWalletCursor = 0;
    this.updateCoverage();
  }

  loadPersistedState() {
    this.loadedPersistedState = true;
    if (!this.persistPath) return;
    const payload = readJson(this.persistPath, null);
    if (!payload || typeof payload !== "object") return;
    const positions = Array.isArray(payload.positions) ? payload.positions : [];
    const events = Array.isArray(payload.events) ? payload.events : [];
    const successWallets = Array.isArray(payload.successScannedWallets)
      ? payload.successScannedWallets
      : [];
    const lifecycleRows = Array.isArray(payload.lifecycle) ? payload.lifecycle : [];
    const grouped = new Map();
    positions.forEach((row) => {
      const wallet = normalizeWallet(row && row.wallet);
      if (!wallet) return;
      if (!grouped.has(wallet)) grouped.set(wallet, []);
      grouped.get(wallet).push(row);
    });
    grouped.forEach((rows, wallet) => {
      const lastSuccessAt = rows.reduce(
        (acc, row) =>
          Math.max(
            acc,
            Number(row && (row.updatedAt || row.timestamp || row.openedAt) ? row.updatedAt || row.timestamp || row.openedAt : 0)
          ),
        0
      );
      this.walletSnapshots.set(wallet, {
        wallet,
        positions: rows,
        scannedAt: lastSuccessAt || Date.now(),
        lastSuccessAt: lastSuccessAt || Date.now(),
        lastErrorAt: null,
        lastError: null,
        success: true,
      });
      if (rows.length > 0) this.openWalletSet.add(wallet);
      this.touchLifecycle(wallet, {
        at: lastSuccessAt || Date.now(),
        reconcile: true,
        success: true,
        position: rows.length > 0,
        openPositionsCount: rows.length,
        reason: rows.length > 0 ? "persisted_open_position" : "persisted_wallet",
      });
    });
    successWallets.forEach((wallet) => {
      const text = normalizeWallet(wallet);
      if (text) this.successScannedWallets.add(text);
    });
    this.events = events;
    events.forEach((event) => {
      const wallet = normalizeWallet(event && event.wallet);
      const at = Number(event && event.at ? event.at : event && event.timestamp ? event.timestamp : 0);
      if (wallet && at > 0) {
        this.recentActiveWalletAt.set(wallet, at);
        this.touchLifecycle(wallet, {
          at,
          trigger: true,
          position: String(event && event.type || "").includes("position"),
          trade: String(event && event.type || "").includes("trade"),
          order: String(event && event.type || "").includes("order"),
          reason: event && event.type ? String(event.type) : "persisted_event",
        });
      }
    });
    lifecycleRows.forEach((row) => {
      const wallet = normalizeWallet(row && row.wallet);
      if (!wallet) return;
      const lifecycle = this.ensureLifecycle(wallet);
      if (!lifecycle) return;
      Object.assign(lifecycle, {
        ...lifecycle,
        ...row,
        wallet,
        lifecycle: ["hot", "warm", "cold"].includes(String(row && row.lifecycle || "").trim())
          ? String(row.lifecycle).trim()
          : lifecycle.lifecycle,
      });
    });
    const persistedStatus =
      payload && payload.status && typeof payload.status === "object"
        ? payload.status
        : {};
    this.status = {
      ...this.status,
      scannedWalletsTotal: Math.max(
        Number(this.status.scannedWalletsTotal || 0),
        Number(persistedStatus.scannedWalletsTotal || 0),
        this.successScannedWallets.size
      ),
      failedWalletsTotal: Math.max(
        Number(this.status.failedWalletsTotal || 0),
        Number(persistedStatus.failedWalletsTotal || 0)
      ),
      passes: Math.max(
        Number(this.status.passes || 0),
        Number(persistedStatus.passes || 0)
      ),
      lastSuccessAt:
        Number(persistedStatus.lastSuccessAt || 0) || this.status.lastSuccessAt,
      lastErrorAt:
        Number(persistedStatus.lastErrorAt || 0) || this.status.lastErrorAt,
      lastError:
        String(persistedStatus.lastError || "").trim() || this.status.lastError,
      lastEventAt:
        Number(persistedStatus.lastEventAt || 0) || this.status.lastEventAt,
      walletsScannedAtLeastOnce: Math.max(
        this.successScannedWallets.size,
        Number(persistedStatus.walletsScannedAtLeastOnce || 0)
      ),
      avgWalletScanMs: Math.max(
        this.avgWalletScanMs,
        Number(persistedStatus.avgWalletScanMs || 0)
      ),
    };
    this.avgWalletScanMs = Math.max(250, Number(this.status.avgWalletScanMs || this.avgWalletScanMs));
    this.flatPositionsDirty = true;
    this.updateCoverage();
  }

  pruneRecentActiveWallets(now = Date.now()) {
    const cutoff = now - this.recentActivityTtlMs;
    for (const [wallet, at] of this.recentActiveWalletAt.entries()) {
      if (!wallet || Number(at || 0) < cutoff) {
        this.recentActiveWalletAt.delete(wallet);
      }
    }
  }

  noteRecentWalletActivity(wallet, at = Date.now()) {
    const normalized = normalizeWallet(wallet);
    if (!normalized) return;
    const observedAt = Number(at || Date.now());
    const priorityMeta = this.walletPriorityMeta.get(normalized) || null;
    this.recentActiveWalletAt.set(normalized, observedAt);
    this.touchLifecycle(normalized, {
      at: observedAt,
      trigger: true,
      reason: "recent_activity",
    });
    this.enqueuePriorityWallet(normalized);
    if (this.hotWalletWsMonitor && typeof this.hotWalletWsMonitor.promoteWallet === "function") {
      this.hotWalletWsMonitor.promoteWallet(normalized, {
        at: observedAt,
        reason: "recent_activity",
        recentVolumeUsd: priorityMeta ? priorityMeta.recentVolumeUsd : 0,
        recentTradeCount: priorityMeta ? priorityMeta.recentTradeCount : 0,
        liveActiveRank: priorityMeta ? priorityMeta.liveActiveRank : 0,
        priorityBoost: priorityMeta ? priorityMeta.priorityBoost : 0,
        pinMs:
          priorityMeta && (priorityMeta.recentVolumeUsd > 0 || priorityMeta.recentTradeCount > 0)
            ? this.warmReconcileMaxAgeMs
            : 0,
      });
    }
  }

  enqueuePriorityWallet(wallet) {
    const normalized = normalizeWallet(wallet);
    if (!normalized || this.priorityWalletSet.has(normalized)) return;
    this.priorityWalletSet.add(normalized);
    this.priorityWalletQueue.push(normalized);
  }

  drainPriorityWallets(target, seen, picked) {
    let taken = 0;
    while (this.priorityWalletQueue.length > 0 && taken < target) {
      const wallet = normalizeWallet(this.priorityWalletQueue.shift());
      if (!wallet) continue;
      this.priorityWalletSet.delete(wallet);
      if (seen.has(wallet)) continue;
      seen.add(wallet);
      picked.push(wallet);
      taken += 1;
    }
    return taken;
  }

  consumeExternalTriggers(now = Date.now()) {
    if (!this.triggerStore || typeof this.triggerStore.readSince !== "function") return;
    if (now - Number(this.lastTriggerLoadAt || 0) < this.triggerPollMs) return;
    this.lastTriggerLoadAt = now;
    const rows = this.triggerStore.readSince(this.lastExternalTriggerAt, {
      limit: this.triggerReadLimit,
    });
    rows.forEach((row) => {
      const wallet = normalizeWallet(row && row.wallet);
      if (!wallet) return;
      if (
        this.shardCount > 1 &&
        stableWalletHash(wallet) % this.shardCount !== this.shardIndex
      ) {
        return;
      }
      const at = Number(row && row.at ? row.at : now);
      this.lastExternalTriggerAt = Math.max(this.lastExternalTriggerAt, at);
      this.recentActiveWalletAt.set(wallet, at);
      this.touchLifecycle(wallet, {
        at,
        trigger: true,
        reason:
          row && row.method
            ? String(row.method)
            : row && row.reason
            ? String(row.reason)
            : row && row.source
            ? String(row.source)
            : "external_trigger",
      });
      this.enqueuePriorityWallet(wallet);
      if (this.hotWalletWsMonitor && typeof this.hotWalletWsMonitor.promoteWallet === "function") {
        const priorityMeta = this.walletPriorityMeta.get(wallet) || null;
        this.hotWalletWsMonitor.promoteWallet(wallet, {
          at,
          reason:
            row && row.method
              ? row.method
              : row && row.reason
              ? row.reason
              : row && row.source
              ? row.source
              : "external_trigger",
          recentVolumeUsd: priorityMeta ? priorityMeta.recentVolumeUsd : 0,
          recentTradeCount: priorityMeta ? priorityMeta.recentTradeCount : 0,
          liveActiveRank: priorityMeta ? priorityMeta.liveActiveRank : 0,
          priorityBoost: priorityMeta ? priorityMeta.priorityBoost : 0,
          pinMs: this.warmReconcileMaxAgeMs,
        });
      }
    });
  }

  promoteHighValueWallets(now = Date.now()) {
    if (!this.hotWalletWsMonitor || typeof this.hotWalletWsMonitor.promoteWallet !== "function") {
      return;
    }
    if (!this.highValueWalletList.length) return;
    const target = Math.max(this.hotWalletsPerPass * 2, this.recentActiveWalletsPerPass, 64);
    const picked = [];
    const seen = new Set();
    this.pickRotatingWallets(
      this.highValueWalletList,
      Math.min(target, this.highValueWalletList.length),
      "highValueWalletCursor",
      seen,
      picked
    );
    picked.forEach((wallet) => {
      const meta = this.walletPriorityMeta.get(wallet) || null;
      if (!meta) return;
      this.hotWalletWsMonitor.promoteWallet(wallet, {
        at: now,
        reason:
          Number(meta.openPositions || 0) > 0
            ? "high_value_open_position"
            : Number(meta.liveActiveRank || 0) > 0
            ? "active_rank_priority"
            : "high_volume_wallet",
        openPositions: Number(meta.openPositions || 0) > 0,
        recentVolumeUsd: meta.recentVolumeUsd,
        recentTradeCount: meta.recentTradeCount,
        liveActiveRank: meta.liveActiveRank,
        priorityBoost: meta.priorityBoost,
        pinMs:
          Number(meta.openPositions || 0) > 0
            ? this.hotReconcileMaxAgeMs * 4
            : this.warmReconcileMaxAgeMs,
      });
    });
  }

  seedHotWalletTier() {
    if (!this.hotWalletWsMonitor || typeof this.hotWalletWsMonitor.promoteWallet !== "function") {
      return;
    }
    this.ensureWalletList();
    const openWallets = Array.from(this.openWalletSet).slice(
      0,
      Math.max(this.hotWalletsPerPass, 16)
    );
    openWallets.forEach((wallet) => {
      const priorityMeta = this.walletPriorityMeta.get(wallet) || null;
      this.hotWalletWsMonitor.promoteWallet(wallet, {
        at: Date.now(),
        reason: "seed_open_position",
        openPositions: true,
        recentVolumeUsd: priorityMeta ? priorityMeta.recentVolumeUsd : 0,
        recentTradeCount: priorityMeta ? priorityMeta.recentTradeCount : 0,
        liveActiveRank: priorityMeta ? priorityMeta.liveActiveRank : 0,
        priorityBoost: priorityMeta ? priorityMeta.priorityBoost : 0,
        pinMs: this.hotReconcileMaxAgeMs * 4,
      });
    });
    this.promoteHighValueWallets(Date.now());
  }

  observeWalletScanDuration(durationMs) {
    const safeDuration = clamp(Number(durationMs || 0) || this.requestTimeoutMs, 100, 60000);
    this.avgWalletScanMs =
      this.avgWalletScanMs > 0
        ? Math.round(this.avgWalletScanMs * 0.85 + safeDuration * 0.15)
        : safeDuration;
    this.status.avgWalletScanMs = this.avgWalletScanMs;
  }

  computeBatchTarget(availableSlots = 0) {
    if (!this.walletList.length) return 0;
    if (this.walletsPerPass > 0) {
      return Math.min(this.walletList.length, Math.max(1, this.walletsPerPass));
    }
    const slots = Math.max(1, Number(availableSlots || 0) || 1);
    const scanMs = clamp(
      Number(this.avgWalletScanMs || this.requestTimeoutMs),
      250,
      this.requestTimeoutMs * Math.max(2, this.maxFetchAttempts * 2)
    );
    const target = Math.floor((slots * this.targetPassDurationMs) / scanMs);
    const minTarget = slots * 4;
    const maxTarget = Math.max(minTarget, Math.min(this.walletList.length, slots * 64));
    return clamp(target || minTarget, 1, maxTarget);
  }

  pickRotatingWallets(list, count, cursorKey, seen, picked) {
    if (!Array.isArray(list) || !list.length || count <= 0) return 0;
    const baseCursor = Math.max(0, Number(this[cursorKey] || 0));
    let taken = 0;
    for (let i = 0; i < list.length && taken < count; i += 1) {
      const index = (baseCursor + i) % list.length;
      const wallet = list[index];
      if (!wallet || seen.has(wallet)) continue;
      seen.add(wallet);
      picked.push(wallet);
      taken += 1;
    }
    this[cursorKey] = (baseCursor + taken) % Math.max(1, list.length);
    return taken;
  }

  pickTopWallets(list, count, seen, picked) {
    if (!Array.isArray(list) || !list.length || count <= 0) return 0;
    let taken = 0;
    for (const wallet of list) {
      if (!wallet || seen.has(wallet)) continue;
      seen.add(wallet);
      picked.push(wallet);
      taken += 1;
      if (taken >= count) break;
    }
    return taken;
  }

  pickBatchWallets(availableSlots = 0) {
    this.ensureWalletList();
    this.consumeExternalTriggers();
    if (!this.walletList.length) return [];
    const now = Date.now();
    this.pruneRecentActiveWallets(now);
    this.promoteHighValueWallets(now);

    const target = this.computeBatchTarget(availableSlots);
    if (!target) return [];
    const picked = [];
    const seen = new Set();

    this.drainPriorityWallets(target, seen, picked);

    const hotDueWallets = this.buildDueWalletList("hot", this.hotReconcileMaxAgeMs, now);
    if (hotDueWallets.length > 0 && this.hotWalletsPerPass > 0) {
      this.pickTopWallets(
        hotDueWallets,
        Math.min(target - picked.length, this.hotWalletsPerPass, hotDueWallets.length),
        seen,
        picked
      );
    }

    const recentActiveWallets = Array.from(this.recentActiveWalletAt.entries())
      .sort((a, b) => Number(b[1] || 0) - Number(a[1] || 0))
      .map(([wallet]) => wallet);
    if (picked.length < target && recentActiveWallets.length > 0 && this.recentActiveWalletsPerPass > 0) {
      this.pickRotatingWallets(
        recentActiveWallets,
        Math.min(target - picked.length, this.recentActiveWalletsPerPass, recentActiveWallets.length),
        "recentActiveWalletCursor",
        seen,
        picked
      );
    }

    const warmDueWallets = this.buildDueWalletList("warm", this.warmReconcileMaxAgeMs, now);
    if (picked.length < target && warmDueWallets.length > 0 && this.warmWalletsPerPass > 0) {
      this.pickTopWallets(
        warmDueWallets,
        Math.min(target - picked.length, this.warmWalletsPerPass, warmDueWallets.length),
        seen,
        picked
      );
    }

    if (picked.length < target && this.warmWalletList.length > 0 && this.warmWalletsPerPass > 0) {
      this.pickRotatingWallets(
        this.warmWalletList,
        Math.min(target - picked.length, this.warmWalletsPerPass, this.warmWalletList.length),
        "warmWalletCursor",
        seen,
        picked
      );
    }

    if (picked.length < target && this.highValueWalletList.length > 0 && this.warmWalletsPerPass > 0) {
      this.pickRotatingWallets(
        this.highValueWalletList,
        Math.min(target - picked.length, this.warmWalletsPerPass, this.highValueWalletList.length),
        "highValueWalletCursor",
        seen,
        picked
      );
    }

    const maxAttempts = this.walletList.length * 2;
    let attempts = 0;
    while (picked.length < target && attempts < maxAttempts && this.walletList.length > 0) {
      const wallet = this.walletList[this.walletCursor];
      this.walletCursor = (this.walletCursor + 1) % this.walletList.length;
      attempts += 1;
      if (!wallet || seen.has(wallet)) continue;
      seen.add(wallet);
      picked.push(wallet);
    }

    this.status.queueCursor = this.walletCursor;
    this.status.batchTargetWallets = target;
    this.status.recentActiveWallets = recentActiveWallets.length;
    this.status.warmWallets = this.warmWalletList.length;
    this.status.highValueWallets = this.highValueWalletList.length;
    this.status.priorityQueueDepth = this.priorityWalletQueue.length;
    this.status.hotReconcileDueWallets = hotDueWallets.length;
    this.status.warmReconcileDueWallets = warmDueWallets.length;
    return picked;
  }

  chooseClient(options = {}) {
    const exclude = options && options.exclude instanceof Set ? options.exclude : null;
    const preferHealthy = options && options.preferHealthy !== false;
    const forceDirect = Boolean(options && options.forceDirect);
    if (!this.restClientEntries.length) return null;
    const now = Date.now();
    let best = null;
    for (let i = 0; i < this.restClientEntries.length; i += 1) {
      const idx = (this.clientCursor + i) % this.restClientEntries.length;
      const state = this.clientStates[idx];
      if (!state) continue;
      if (exclude && exclude.has(idx)) continue;
      if (forceDirect && idx !== this.directClientIndex) continue;
      if (now < Number(state.cooldownUntil || 0)) continue;
      if (now < Number(state.disabledUntil || 0)) continue;
      const maxInflightForState =
        idx === this.directClientIndex ? this.maxInFlightDirect : this.maxInFlightPerClient;
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
      const score =
        inflight * 100 + latencyPenalty + timeoutPenalty + rateLimitPenalty + healthPenalty;
      if (!best) {
        best = { idx, state, score };
        continue;
      }
      if (score < best.score) {
        best = { idx, state, score };
      }
    }
    if (!best) return null;
    this.clientCursor = (best.idx + 1) % this.restClientEntries.length;
    return {
      idx: best.idx,
      entry: this.restClientEntries[best.idx],
      state: best.state,
    };
  }

  markClientResult(idx, error = null, meta = {}) {
    const state = this.clientStates[idx];
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
      if (Number(state.disabledUntil || 0) > 0 && Number(state.disabledUntil || 0) <= now) {
        state.disabledUntil = 0;
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

    if (message.includes("429")) {
      state.rateLimitCount = Number(state.rateLimitCount || 0) + 1;
      state.consecutive429 = Math.min(10, Number(state.consecutive429 || 0) + 1);
      state.consecutiveTimeout = 0;
      const cooldownMs = Math.min(
        this.rateLimitBackoffMaxMs,
        this.rateLimitBackoffBaseMs * 2 ** Math.max(0, state.consecutive429 - 1)
      );
      state.cooldownUntil = now + cooldownMs;
      if (state.consecutive429 >= 6) {
        state.disabledUntil = now + 20 * 1000;
        state.cooldownUntil = Math.max(Number(state.cooldownUntil || 0), state.disabledUntil);
      }
      maybeDisableUnhealthyClient();
      return;
    }

    if (message.toLowerCase().includes("timeout")) {
      state.timeoutCount = Number(state.timeoutCount || 0) + 1;
      state.consecutiveTimeout = Math.min(10, Number(state.consecutiveTimeout || 0) + 1);
      state.consecutive429 = 0;
      const timeoutCooldownMs = Math.min(
        30000,
        this.rateLimitBackoffBaseMs * 2 ** Math.max(0, state.consecutiveTimeout - 1)
      );
      state.cooldownUntil = now + timeoutCooldownMs;
      if (state.consecutiveTimeout >= 6) {
        state.disabledUntil = now + 20 * 1000;
        state.cooldownUntil = Math.max(Number(state.cooldownUntil || 0), state.disabledUntil);
      }
      maybeDisableUnhealthyClient();
      return;
    }

    if (message.toLowerCase().includes("failed to connect") || message.toLowerCase().includes("proxy")) {
      state.proxyFailureCount = Number(state.proxyFailureCount || 0) + 1;
      state.consecutiveTimeout = Math.min(10, Number(state.consecutiveTimeout || 0) + 1);
      state.cooldownUntil = now + Math.min(10000, Math.max(3000, this.rateLimitBackoffBaseMs));
      if (state.consecutiveTimeout >= 6) {
        state.disabledUntil = now + 15 * 1000;
        state.cooldownUntil = Math.max(Number(state.cooldownUntil || 0), state.disabledUntil);
      }
      maybeDisableUnhealthyClient();
      return;
    }
    maybeDisableUnhealthyClient();
  }

  async fetchWalletPositions(wallet) {
    const maxAttempts = Math.max(1, Math.min(this.maxFetchAttempts, this.restClientEntries.length));
    let lastError = null;
    const tried = new Set();
    for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
      const forceDirect =
        this.directFallbackOnLastAttempt &&
        this.directClientIndex >= 0 &&
        (attempt === maxAttempts - 1 || (lastError && attempt > 0));
      let selected = this.chooseClient({ exclude: tried, forceDirect });
      if (!selected) {
        for (let spin = 0; spin < 4; spin += 1) {
          // All clients may be temporarily in-flight; wait briefly before treating as a failure.
          // eslint-disable-next-line no-await-in-loop
          await new Promise((resolve) => setTimeout(resolve, 40 + spin * 20));
          selected = this.chooseClient({ exclude: tried, forceDirect });
          if (selected) break;
        }
      }
      if (!selected) {
        const fallback = this.chooseClient({ exclude: tried, preferHealthy: false });
        if (!fallback) {
          const error = new Error("all_live_wallet_clients_cooling_down");
          lastError = error;
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
        const response = await entry.client.get("/positions", {
          query: { account: wallet },
          cost: 1,
          timeoutMs: this.requestTimeoutMs,
          retryMaxAttempts: 1,
        });
        const payload =
          response && response.payload && typeof response.payload === "object"
            ? response.payload
            : {};
        if (payload.success === false) {
          const reason = String(payload.error || "positions_request_failed").trim();
          throw new Error(reason || "positions_request_failed");
        }
        const rowsRaw = Array.isArray(payload.data) ? payload.data : [];
        const rows = rowsRaw
          .map((row) => normalizePositionRow(wallet, row))
          .filter(Boolean)
          .sort((a, b) => Number(b.updatedAt || 0) - Number(a.updatedAt || 0));
        this.markClientResult(idx, null, { durationMs: Date.now() - requestStartedAt });
        return rows;
      } catch (error) {
        this.markClientResult(idx, error, { durationMs: Date.now() - requestStartedAt });
        lastError = error;
        const msg = String(error && error.message ? error.message : error || "").toLowerCase();
        const retryable =
          msg.includes("429") ||
          msg.includes("timeout") ||
          msg.includes("failed to connect") ||
          msg.includes("proxy") ||
          msg.includes("503") ||
          msg.includes("502");
        if (!retryable) break;
      } finally {
        state.inFlight = Math.max(0, Number(state.inFlight || 1) - 1);
      }
    }
    throw lastError || new Error("wallet_positions_fetch_failed");
  }

  pushEvent(event) {
    if (!event || typeof event !== "object") return;
    this.events.push(event);
    const eventAt = Number(event.at || 0);
    if (event.wallet) {
      this.noteRecentWalletActivity(event.wallet, eventAt || Date.now());
    }
    if (Number.isFinite(eventAt) && eventAt > 0) {
      this.status.lastEventAt = eventAt;
    }
    if (this.events.length > this.maxEvents) {
      this.events.splice(0, this.events.length - this.maxEvents);
    }
  }

  ingestHotWalletPositions(wallet, rowsRaw, meta = {}) {
    const normalizedWallet = normalizeWallet(wallet);
    if (!normalizedWallet) return;
    const observedAt = Number(meta.at || Date.now());
    const rows = (Array.isArray(rowsRaw) ? rowsRaw : [])
      .map((row) => normalizePositionRow(normalizedWallet, row))
      .filter(Boolean)
      .sort((a, b) => Number(b.updatedAt || 0) - Number(a.updatedAt || 0));
    this.updateSnapshot(normalizedWallet, rows, observedAt);
    this.noteRecentWalletActivity(normalizedWallet, observedAt);
    this.touchLifecycle(normalizedWallet, {
      at: observedAt,
      position: true,
      success: true,
      openPositionsCount: rows.length,
      reason: "ws_account_positions",
    });
    this.flatPositionsDirty = true;
    this.status.lastSuccessAt = observedAt;
    this.persistMaybe();
  }

  ingestHotWalletActivity(wallet, type, meta = {}) {
    const normalizedWallet = normalizeWallet(wallet);
    if (!normalizedWallet) return;
    const at = Number(meta.at || Date.now());
    this.noteRecentWalletActivity(normalizedWallet, at);
    this.touchLifecycle(normalizedWallet, {
      at,
      trigger: true,
      trade: String(type || "").includes("trade"),
      order: String(type || "").includes("order"),
      reason: type || "hot_wallet_ws",
    });
    this.pushEvent({
      at,
      type,
      wallet: normalizedWallet,
      symbol: meta.symbol || null,
      side: meta.side || null,
      source: meta.source || "hot_wallet_ws",
    });
    this.persistMaybe();
  }

  updateSnapshot(wallet, nextRows, observedAt) {
    const prev = this.walletSnapshots.get(wallet) || null;
    const prevRows = prev && Array.isArray(prev.positions) ? prev.positions : [];

    const prevMap = new Map(prevRows.map((row) => [row.positionKey || positionKey(row), row]));
    const nextMap = new Map(nextRows.map((row) => [row.positionKey || positionKey(row), row]));

    for (const [key, row] of nextMap.entries()) {
      const oldRow = prevMap.get(key);
      if (!oldRow) {
        this.pushEvent({
          at: observedAt,
          type: "position_opened",
          wallet,
          symbol: row.symbol,
          side: row.side,
          key,
          oldSize: null,
          newSize: row.size,
          source: "wallet_first_positions",
        });
        continue;
      }
      if (!compareAmount(oldRow.size, row.size)) {
        this.pushEvent({
          at: observedAt,
          type: "position_size_changed",
          wallet,
          symbol: row.symbol,
          side: row.side,
          key,
          oldSize: oldRow.size,
          newSize: row.size,
          source: "wallet_first_positions",
        });
      }
    }

    for (const [key, row] of prevMap.entries()) {
      if (nextMap.has(key)) continue;
      this.pushEvent({
        at: observedAt,
        type: "position_closed",
        wallet,
        symbol: row.symbol,
        side: row.side,
        key,
        oldSize: row.size,
        newSize: 0,
        source: "wallet_first_positions",
      });
    }

    this.walletSnapshots.set(wallet, {
      wallet,
      positions: nextRows,
      scannedAt: observedAt,
      lastSuccessAt: observedAt,
      lastErrorAt: null,
      lastError: null,
      success: true,
    });

    const oldOpen = prevRows.length > 0;
    const newOpen = nextRows.length > 0;
    if (!oldOpen && newOpen) this.openWalletSet.add(wallet);
    if (oldOpen && !newOpen) this.openWalletSet.delete(wallet);
    this.touchLifecycle(wallet, {
      at: observedAt,
      reconcile: true,
      success: true,
      position: nextRows.length > 0 || prevRows.length > 0,
      openPositionsCount: nextRows.length,
      reason: newOpen ? "open_position" : "reconcile_scan",
    });
    if (this.hotWalletWsMonitor && typeof this.hotWalletWsMonitor.promoteWallet === "function") {
      this.hotWalletWsMonitor.promoteWallet(wallet, {
        at: observedAt,
        reason: newOpen ? "open_position" : "reconcile_scan",
        openPositions: newOpen,
      });
    }

    if (prevRows.length !== nextRows.length) {
      this.status.openPositionsTotal = Math.max(
        0,
        Number(this.status.openPositionsTotal || 0) - prevRows.length + nextRows.length
      );
      this.flatPositionsDirty = true;
    } else {
      const changed = nextRows.some((row) => {
        const oldRow = prevMap.get(row.positionKey || positionKey(row));
        if (!oldRow) return true;
        if (!compareAmount(oldRow.size, row.size)) return true;
        return Number(oldRow.updatedAt || 0) !== Number(row.updatedAt || 0);
      });
      if (changed) this.flatPositionsDirty = true;
    }
  }

  markWalletError(wallet, error) {
    const now = Date.now();
    const message = String(error && error.message ? error.message : error || "");
    const prev = this.walletSnapshots.get(wallet) || {
      wallet,
      positions: [],
      scannedAt: null,
      lastSuccessAt: null,
    };
    this.walletSnapshots.set(wallet, {
      ...prev,
      scannedAt: now,
      lastErrorAt: now,
      lastError: message || "wallet_positions_failed",
      success: false,
    });
    this.touchLifecycle(wallet, {
      at: now,
      error: true,
      reason: message || "wallet_positions_failed",
    });

    if (message.includes("all_live_wallet_clients_cooling_down")) {
      this.rateLimitStreak = Math.min(8, this.rateLimitStreak + 1);
      const cooldownMs = Math.min(
        this.rateLimitBackoffMaxMs,
        this.rateLimitBackoffBaseMs * 2 ** Math.max(0, this.rateLimitStreak - 1)
      );
      this.rateLimitCooldownUntil = Math.max(this.rateLimitCooldownUntil, now + cooldownMs);
      return;
    }

    if (message.includes("429")) {
      this.rateLimitStreak = Math.min(8, this.rateLimitStreak + 1);
      const cooldownMs = Math.min(
        this.rateLimitBackoffMaxMs,
        this.rateLimitBackoffBaseMs * 2 ** Math.max(0, this.rateLimitStreak - 1)
      );
      this.rateLimitCooldownUntil = Math.max(this.rateLimitCooldownUntil, now + Math.min(cooldownMs, 5000));
    }
  }

  async scanWallet(wallet) {
    const observedAt = Date.now();
    try {
      const rows = await this.fetchWalletPositions(wallet);
      this.updateSnapshot(wallet, rows, observedAt);
      this.status.lastSuccessAt = observedAt;
      this.status.scannedWalletsTotal = Number(this.status.scannedWalletsTotal || 0) + 1;
      this.successScannedWallets.add(wallet);
      this.status.walletsScannedAtLeastOnce = this.successScannedWallets.size;
      this.rateLimitStreak = 0;
      this.touchLifecycle(wallet, {
        at: observedAt,
        reconcile: true,
        success: true,
        openPositionsCount: Array.isArray(rows) ? rows.length : 0,
        reason: "rest_reconcile_success",
      });
      this.observeWalletScanDuration(Date.now() - observedAt);
      this.persistMaybe();
    } catch (error) {
      const message = String(error && error.message ? error.message : error || "");
      if (message.includes("all_live_wallet_clients_cooling_down")) {
        this.status.lastErrorAt = Date.now();
        this.status.lastError = message;
        return;
      }
      this.markWalletError(wallet, error);
      this.status.failedWalletsTotal = Number(this.status.failedWalletsTotal || 0) + 1;
      this.status.lastErrorAt = Date.now();
      this.status.lastError = message;
      const lifecycle = this.ensureLifecycle(wallet);
      if (lifecycle && lifecycle.lifecycle === "hot") {
        this.enqueuePriorityWallet(wallet);
      }
      this.observeWalletScanDuration(Date.now() - observedAt);
      this.persistMaybe();
    }
  }

  getAvailableClientSlots(now = Date.now()) {
    if (!this.clientStates.length) return 0;
    let freeSlots = 0;
    for (let idx = 0; idx < this.clientStates.length; idx += 1) {
      const state = this.clientStates[idx];
      if (!state) continue;
      if (now < Number(state.cooldownUntil || 0)) continue;
      if (now < Number(state.disabledUntil || 0)) continue;
      const cap = idx === this.directClientIndex ? this.maxInFlightDirect : this.maxInFlightPerClient;
      freeSlots += Math.max(0, cap - Number(state.inFlight || 0));
    }
    return freeSlots;
  }

  async runPass() {
    if (!this.enabled || this.running) return;
    if (Date.now() < this.rateLimitCooldownUntil) return;
    this.running = true;
    const startedAt = Date.now();
    this.status.lastPassStartedAt = startedAt;

    try {
      const availableSlots = this.getAvailableClientSlots();
      const batch = this.pickBatchWallets(availableSlots);
      this.status.lastBatchWallets = batch.length;
      if (!batch.length) {
        this.status.lastPassFinishedAt = Date.now();
        this.status.lastPassDurationMs = this.status.lastPassFinishedAt - startedAt;
        return;
      }

      let cursor = 0;
      const dynamicMaxConcurrency =
        availableSlots > 0 ? Math.max(1, Math.min(this.maxConcurrency, availableSlots)) : 1;
      const workerCount = Math.max(1, Math.min(dynamicMaxConcurrency, batch.length));
      const workers = Array.from({ length: workerCount }).map(async () => {
        while (true) {
          const index = cursor;
          cursor += 1;
          if (index >= batch.length) break;
          const wallet = batch[index];
          if (!wallet) continue;
          // eslint-disable-next-line no-await-in-loop
          await this.scanWallet(wallet);
        }
      });

      await Promise.all(workers);

      this.status.passes = Number(this.status.passes || 0) + 1;
      this.status.walletsScannedAtLeastOnce = this.successScannedWallets.size;
      this.status.walletsWithOpenPositions = this.openWalletSet.size;
      this.updateCoverage();
      this.status.lastPassFinishedAt = Date.now();
      this.status.lastPassDurationMs = this.status.lastPassFinishedAt - startedAt;
      const passDurationSec = Math.max(0.001, Number(this.status.lastPassDurationMs || 0) / 1000);
      const throughputRps = batch.length > 0 ? batch.length / passDurationSec : 0;
      this.status.passThroughputRps = Number(throughputRps.toFixed(2));
      this.status.estimatedSweepSeconds =
        this.status.walletsKnown > 0 && throughputRps > 0
          ? Math.ceil(this.status.walletsKnown / throughputRps)
          : null;
      this.status.lastEventAt = this.events.length
        ? Number(this.events[this.events.length - 1].at || 0) || this.status.lastEventAt
        : this.status.lastEventAt;
      this.status.lastError = null;
      this.flatPositionsDirty = true;
      if (this.hotWalletWsMonitor && typeof this.hotWalletWsMonitor.getStatus === "function") {
        const hotStatus = this.hotWalletWsMonitor.getStatus();
        this.status.hotWsActiveWallets = Number(hotStatus.activeWallets || 0);
        this.status.hotWsOpenConnections = Number(hotStatus.openConnections || 0);
        this.status.hotWsDroppedPromotions = Number(hotStatus.droppedPromotions || 0);
        this.status.hotWsCapacity = Number(hotStatus.maxWallets || 0);
        this.status.hotWsAvailableSlots = Number(hotStatus.availableSlots || 0);
        this.status.hotWsCapacityCeiling = Number(hotStatus.capacityCeiling || 0);
        this.status.hotWsPromotionBacklog = Number(hotStatus.promotionBacklog || 0);
        this.status.hotWsTriggerToEventAvgMs = Number(hotStatus.triggerToEventAvgMs || 0);
        this.status.hotWsLastTriggerToEventMs = Number(hotStatus.lastTriggerToEventMs || 0);
        this.status.hotWsReconnectTransitions = Number(hotStatus.reconnectTransitions || 0);
        this.status.hotWsErrorCount = Number(hotStatus.errorCount || 0);
        this.status.hotWsScaleEvents = Number(hotStatus.scaleEvents || 0);
        this.status.hotWsProcessRssMb = Number(hotStatus.processRssMb || 0);
      }
    } finally {
      this.running = false;
      this.updateRecentCounters();
    }
  }

  updateRecentCounters(nowMs = Date.now()) {
    const oneHourAgo = nowMs - 60 * 60 * 1000;
    const oneDayAgo = nowMs - 24 * 60 * 60 * 1000;
    let changes1h = 0;
    let changes24h = 0;
    const events = this.events;
    for (let i = events.length - 1; i >= 0; i -= 1) {
      const at = Number(events[i] && events[i].at ? events[i].at : 0);
      if (!Number.isFinite(at) || at <= 0) continue;
      if (at >= oneHourAgo) changes1h += 1;
      if (at >= oneDayAgo) changes24h += 1;
      if (at < oneDayAgo) break;
    }
    this.status.recentChanges1h = changes1h;
    this.status.recentChanges24h = changes24h;
  }

  getPositionsRows(nowMs = Date.now()) {
    if (!this.flatPositionsDirty) return this.flatPositionsCache;
    const rows = [];
    for (const [wallet, snapshot] of this.walletSnapshots.entries()) {
      const positions = snapshot && Array.isArray(snapshot.positions) ? snapshot.positions : [];
      if (!positions.length) continue;
      const lastSuccessAt = Number(snapshot.lastSuccessAt || snapshot.scannedAt || 0);
      const ageMs = lastSuccessAt > 0 ? Math.max(0, nowMs - lastSuccessAt) : Infinity;
      const freshness = ageMs <= this.coolingMs ? "fresh" : ageMs <= this.staleMs ? "cooling" : "stale";
      positions.forEach((row) => {
        rows.push({
          ...row,
          wallet,
          trackedWallet: true,
          freshness,
          status: freshness,
          timestamp: Number(row.timestamp || row.updatedAt || lastSuccessAt || nowMs),
          updatedAt: Number(row.updatedAt || lastSuccessAt || nowMs),
        });
      });
    }

    rows.sort((a, b) => Number(b.updatedAt || 0) - Number(a.updatedAt || 0));
    this.flatPositionsCache = rows;
    this.flatPositionsDirty = false;
    this.status.openPositionsTotal = rows.length;
    this.status.walletsWithOpenPositions = this.openWalletSet.size;
    return this.flatPositionsCache;
  }

  getRecentEvents(limit = 200) {
    const safeLimit = Math.max(1, Math.min(5000, Number(limit || 200)));
    const now = Date.now();
    const rows = this.events
      .slice(Math.max(0, this.events.length - safeLimit))
      .reverse()
      .map((row, idx) => ({
        id: `${row.at || now}:${row.wallet || "na"}:${row.symbol || "na"}:${idx}`,
        historyId: null,
        wallet: row.wallet,
        symbol: row.symbol,
        side: row.side,
        amount: Number.isFinite(toNum(row.newSize, NaN)) ? Number(row.newSize) : 0,
        price: NaN,
        timestamp: Number(row.at || now),
        sideEvent: row.type,
        cause: row.type,
        source: "wallet_first_position_change",
        walletSource: "wallet_positions_api",
        walletConfidence: "hard_payload",
      }));
    return rows;
  }

  getStatus() {
    this.updateRecentCounters();
    this.updateCoverage();
    const now = Date.now();
    const clientsCooling = this.clientStates.filter(
      (row) => Number(row && row.cooldownUntil ? row.cooldownUntil : 0) > now
    ).length;
    const clientsDisabled = this.clientStates.filter(
      (row) => Number(row && row.disabledUntil ? row.disabledUntil : 0) > now
    ).length;
    const clientsInFlight = this.clientStates.reduce(
      (acc, row) => acc + Math.max(0, Number((row && row.inFlight) || 0)),
      0
    );
    const clients429 = this.clientStates.filter((row) => Number((row && row.consecutive429) || 0) > 0).length;
    const healthyClients = this.clientStates.filter((row) => {
      if (!row) return false;
      if (Number(row.cooldownUntil || 0) > now) return false;
      if (Number(row.disabledUntil || 0) > now) return false;
      const requests = Number(row.requests || 0);
      const successes = Number(row.successes || 0);
      if (requests <= 0) return true;
      return successes / Math.max(1, requests) >= 0.1;
    }).length;
    const avgClientLatencyMs =
      this.clientStates.reduce((acc, row) => acc + Math.max(0, Number((row && row.avgLatencyMs) || 0)), 0) /
      Math.max(1, this.clientStates.length || 1);
    const timeoutClients = this.clientStates.filter((row) => Number((row && row.timeoutCount) || 0) > 0).length;
    const proxyFailingClients = this.clientStates.filter(
      (row) => Number((row && row.proxyFailureCount) || 0) > 0
    ).length;
    let freshWallets = 0;
    let coolingWallets = 0;
    let staleWallets = 0;
    for (const snapshot of this.walletSnapshots.values()) {
      const lastSuccessAt = Number(snapshot && (snapshot.lastSuccessAt || snapshot.scannedAt) ? snapshot.lastSuccessAt || snapshot.scannedAt : 0);
      const ageMs = lastSuccessAt > 0 ? Math.max(0, now - lastSuccessAt) : Infinity;
      if (ageMs <= this.coolingMs) freshWallets += 1;
      else if (ageMs <= this.staleMs) coolingWallets += 1;
      else staleWallets += 1;
    }
    let lifecycleHotWallets = 0;
    let lifecycleWarmWallets = 0;
    let lifecycleColdWallets = 0;
    for (const row of this.walletLifecycle.values()) {
      if (!row) continue;
      this.recalculateLifecycle(row, now, {});
      if (row.lifecycle === "hot") lifecycleHotWallets += 1;
      else if (row.lifecycle === "warm") lifecycleWarmWallets += 1;
      else lifecycleColdWallets += 1;
    }
    const hotStatus =
      this.hotWalletWsMonitor && typeof this.hotWalletWsMonitor.getStatus === "function"
        ? this.hotWalletWsMonitor.getStatus()
        : null;
    return {
      ...this.status,
      enabled: this.enabled,
      running: this.running,
      cooldownUntil:
        Number(this.rateLimitCooldownUntil || 0) > Date.now()
          ? Number(this.rateLimitCooldownUntil || 0)
          : null,
      cooldownMsRemaining: Math.max(0, Number(this.rateLimitCooldownUntil || 0) - Date.now()),
      walletsKnown: this.status.walletsKnown,
      walletsScannedAtLeastOnce: this.status.walletsScannedAtLeastOnce,
      walletsCoveragePct: this.status.walletsCoveragePct,
      walletsKnownGlobal: this.status.walletsKnownGlobal,
      walletsWithOpenPositions: this.status.walletsWithOpenPositions,
      openPositionsTotal: this.status.openPositionsTotal,
      queueCursor: this.status.queueCursor,
      scanIntervalMs: this.scanIntervalMs,
      walletsPerPass: this.walletsPerPass,
      maxConcurrency: this.maxConcurrency,
      hotWalletsPerPass: this.hotWalletsPerPass,
      warmWalletsPerPass: this.warmWalletsPerPass,
      recentActiveWalletsPerPass: this.recentActiveWalletsPerPass,
      targetPassDurationMs: this.targetPassDurationMs,
      avgWalletScanMs: this.avgWalletScanMs,
      recentActiveWallets: this.recentActiveWalletAt.size,
      warmWallets: this.warmWalletList.length,
      requestTimeoutMs: this.requestTimeoutMs,
      maxFetchAttempts: this.maxFetchAttempts,
      maxInFlightPerClient: this.maxInFlightPerClient,
      maxInFlightDirect: this.maxInFlightDirect,
      directFallbackOnLastAttempt: this.directFallbackOnLastAttempt,
      staleMs: this.staleMs,
      coolingMs: this.coolingMs,
      clientsTotal: this.restClientEntries.length,
      clientsCooling,
      clientsDisabled,
      clientsInFlight,
      clients429,
      healthyClients,
      avgClientLatencyMs: Math.round(avgClientLatencyMs || 0),
      timeoutClients,
      proxyFailingClients,
      hotWsActiveWallets: Number(hotStatus && hotStatus.activeWallets ? hotStatus.activeWallets : 0),
      hotWsOpenConnections: Number(hotStatus && hotStatus.openConnections ? hotStatus.openConnections : 0),
      hotWsDroppedPromotions: Number(
        hotStatus && hotStatus.droppedPromotions ? hotStatus.droppedPromotions : 0
      ),
      hotWsCapacity: Number(hotStatus && hotStatus.maxWallets ? hotStatus.maxWallets : 0),
      hotWsAvailableSlots: Number(hotStatus && hotStatus.availableSlots ? hotStatus.availableSlots : 0),
      hotWsCapacityCeiling: Number(
        hotStatus && hotStatus.capacityCeiling ? hotStatus.capacityCeiling : 0
      ),
      hotWsPromotionBacklog: Number(
        hotStatus && hotStatus.promotionBacklog ? hotStatus.promotionBacklog : 0
      ),
      hotWsTriggerToEventAvgMs: Number(
        hotStatus && hotStatus.triggerToEventAvgMs ? hotStatus.triggerToEventAvgMs : 0
      ),
      hotWsLastTriggerToEventMs: Number(
        hotStatus && hotStatus.lastTriggerToEventMs ? hotStatus.lastTriggerToEventMs : 0
      ),
      hotWsReconnectTransitions: Number(
        hotStatus && hotStatus.reconnectTransitions ? hotStatus.reconnectTransitions : 0
      ),
      hotWsErrorCount: Number(hotStatus && hotStatus.errorCount ? hotStatus.errorCount : 0),
      hotWsScaleEvents: Number(hotStatus && hotStatus.scaleEvents ? hotStatus.scaleEvents : 0),
      hotWsProcessRssMb: Number(hotStatus && hotStatus.processRssMb ? hotStatus.processRssMb : 0),
      lifecycleHotWallets,
      lifecycleWarmWallets,
      lifecycleColdWallets,
      staleWallets,
      freshWallets,
      coolingWallets,
      priorityQueueDepth: this.priorityWalletQueue.length,
      hotReconcileDueWallets: Number(this.status.hotReconcileDueWallets || 0),
      warmReconcileDueWallets: Number(this.status.warmReconcileDueWallets || 0),
      estimatedHotLoopSeconds:
        Math.max(this.openWalletSet.size, lifecycleHotWallets) > 0 && this.hotWalletsPerPass > 0
          ? Math.ceil(
              (Math.max(this.openWalletSet.size, lifecycleHotWallets) / Math.max(1, this.hotWalletsPerPass)) *
                (Math.max(1000, Number(this.status.lastPassDurationMs || this.targetPassDurationMs)) / 1000)
            )
          : null,
      estimatedWarmLoopSeconds:
        Math.max(this.warmWalletList.length, lifecycleWarmWallets) > 0 && this.warmWalletsPerPass > 0
          ? Math.ceil(
              (Math.max(this.warmWalletList.length, lifecycleWarmWallets) / Math.max(1, this.warmWalletsPerPass)) *
                (Math.max(1000, Number(this.status.lastPassDurationMs || this.targetPassDurationMs)) / 1000)
            )
          : null,
      shardIndex: this.shardIndex,
      shardCount: this.shardCount,
    };
  }

  buildPersistedPayload() {
    return {
      generatedAt: Date.now(),
      shardIndex: this.shardIndex,
      shardCount: this.shardCount,
      status: this.getStatus(),
      positions: this.getPositionsRows(),
      events: this.getRecentEvents(this.maxPersistedEvents),
      successScannedWallets: Array.from(this.successScannedWallets),
      lifecycle: Array.from(this.walletLifecycle.values()),
    };
  }

  persistMaybe(force = false) {
    if (!this.persistPath) return;
    const now = Date.now();
    if (!force && now - Number(this.lastPersistAt || 0) < this.persistEveryMs) return;
    writeShardSnapshot(this.persistPath, this.buildPersistedPayload());
    this.lastPersistAt = now;
  }

  getSnapshot(options = {}) {
    const eventsLimit = Math.max(1, Math.min(2000, Number(options.eventsLimit || 200)));
    const positions = this.getPositionsRows();
    return {
      status: this.getStatus(),
      positions,
      events: this.getRecentEvents(eventsLimit),
    };
  }
}

module.exports = {
  WalletFirstLivePositionsMonitor,
};
