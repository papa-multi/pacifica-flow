const path = require("path");
const fs = require("fs");
const {
  ensureDir,
  normalizeAddress,
  readJson,
  writeJsonAtomic,
} = require("../pipeline/utils");
const { buildWalletRecordFromHistory } = require("../analytics/wallet_stats");

const STATE_VERSION = 4;
const HISTORY_CURSOR_HEAD = "__head__";

const WALLET_LIFECYCLE = {
  DISCOVERED: "discovered",
  PENDING_BACKFILL: "pending_backfill",
  BACKFILLING: "backfilling",
  FULLY_INDEXED: "fully_indexed",
  LIVE_TRACKING: "live_tracking",
};

function emptyState() {
  return {
    version: STATE_VERSION,
    knownWallets: [],
    liveWallets: [],
    priorityQueue: [],
    scanCursor: 0,
    liveScanCursor: 0,
    lastDiscoveryAt: null,
    lastScanAt: null,
    discoveryCycles: 0,
    scanCycles: 0,
    walletStates: {},
  };
}

function uniq(list = []) {
  return Array.from(new Set(Array.isArray(list) ? list : []));
}

function normalizeWallets(list = []) {
  return uniq(
    (Array.isArray(list) ? list : [])
      .map((value) => normalizeAddress(value))
      .filter(Boolean)
  );
}

function extractPayloadData(result, fallback = []) {
  if (!result || !result.payload) return fallback;
  if (Object.prototype.hasOwnProperty.call(result.payload, "data")) {
    return result.payload.data;
  }
  return fallback;
}

function toErrorMessage(error) {
  if (!error) return "unknown_error";
  if (error.payload && error.payload.error) {
    return `${error.message}: ${error.payload.error}`;
  }
  return error.message || "unknown_error";
}

function normalizeTradeHistoryRow(row = {}) {
  return {
    historyId: row.history_id || null,
    symbol: String(row.symbol || "").toUpperCase(),
    amount: row.amount !== undefined ? row.amount : "0",
    price: row.price !== undefined ? row.price : "0",
    fee: row.fee !== undefined ? row.fee : "0",
    pnl: row.pnl !== undefined ? row.pnl : "0",
    timestamp: Number(row.created_at || 0),
  };
}

function normalizeFundingHistoryRow(row = {}) {
  return {
    historyId: row.history_id || null,
    symbol: String(row.symbol || "").toUpperCase(),
    payout: row.payout !== undefined ? row.payout : "0",
    createdAt: Number(row.created_at || 0),
  };
}

function summarizeErrorReason(message) {
  const msg = String(message || "").toLowerCase();
  if (!msg) return "unknown_error";
  if (msg.includes("429")) return "rate_limit_429";
  if (msg.includes("timeout")) return "timeout";
  if (msg.includes("503")) return "service_unavailable_503";
  if (msg.includes("500")) return "server_error_500";
  if (msg.includes("404")) return "not_found_404";
  if (msg.includes("network")) return "network_error";
  if (msg.includes("econnreset")) return "connection_reset";
  return msg.slice(0, 120);
}

function normalizeTradeKey(row = {}) {
  const historyId = row && row.historyId ? String(row.historyId) : "";
  if (historyId) return `h:${historyId}`;
  return `f:${[
    Number(row.timestamp || 0),
    String(row.symbol || "").toUpperCase(),
    String(row.amount || "0"),
    String(row.price || "0"),
    String(row.fee || "0"),
    String(row.pnl || "0"),
  ].join("|")}`;
}

function normalizeFundingKey(row = {}) {
  const historyId = row && row.historyId ? String(row.historyId) : "";
  if (historyId) return `h:${historyId}`;
  return `f:${[
    Number(row.createdAt || 0),
    String(row.symbol || "").toUpperCase(),
    String(row.payout || "0"),
  ].join("|")}`;
}

function cursorCacheKey(cursor) {
  return cursor ? String(cursor) : HISTORY_CURSOR_HEAD;
}

function emptyWalletHistory(wallet) {
  return {
    wallet,
    trades: [],
    funding: [],
    tradeSeenKeys: {},
    fundingSeenKeys: {},
    pageCache: {
      trades: {},
      funding: {},
    },
    updatedAt: null,
  };
}

class ExchangeWalletIndexer {
  constructor(options = {}) {
    this.restClient = options.restClient;
    this.restClients = Array.isArray(options.restClients)
      ? options.restClients.filter((client) => client && typeof client.get === "function")
      : [];
    if (!this.restClients.length && this.restClient && typeof this.restClient.get === "function") {
      this.restClients = [this.restClient];
    }
    if (!this.restClients.length) {
      throw new Error("ExchangeWalletIndexer requires at least one rest client.");
    }
    this.restClientIds = Array.isArray(options.restClientIds) ? options.restClientIds : [];
    this.walletStore = options.walletStore;
    this.walletSource = options.walletSource || null;
    this.onchainDiscovery = options.onchainDiscovery || null;
    this.logger = options.logger || console;

    this.dataDir = options.dataDir || path.join(process.cwd(), "data", "indexer");
    this.statePath = options.statePath || path.join(this.dataDir, "indexer_state.json");
    this.depositWalletsPath =
      options.depositWalletsPath || path.join(this.dataDir, "deposit_wallets.json");
    this.walletHistoryDir =
      options.walletHistoryDir || path.join(this.dataDir, "wallet_history");

    this.seedWallets = normalizeWallets(options.seedWallets || []);

    this.scanIntervalMs = Math.max(5000, Number(options.scanIntervalMs || 30000));
    this.discoveryIntervalMs = Math.max(15000, Number(options.discoveryIntervalMs || 120000));
    this.maxWalletsPerScan = Math.max(1, Number(options.maxWalletsPerScan || 40));
    this.maxPagesPerWallet = Math.max(1, Number(options.maxPagesPerWallet || 3));
    this.fullHistoryPerWallet = (() => {
      if (options.fullHistoryPerWallet === undefined) return true;
      return String(options.fullHistoryPerWallet).toLowerCase() !== "false";
    })();
    this.tradesPageLimit = Math.max(20, Math.min(4000, Number(options.tradesPageLimit || 400)));
    this.fundingPageLimit = Math.max(20, Math.min(4000, Number(options.fundingPageLimit || 400)));
    this.walletScanConcurrency = Math.max(
      1,
      Math.min(512, Number(options.walletScanConcurrency || 4))
    );
    this.liveWalletsPerScan = Math.max(
      0,
      Math.min(512, Number(options.liveWalletsPerScan || Math.max(1, Math.floor(this.maxWalletsPerScan / 4))))
    );
    this.liveMaxPagesPerWallet = Math.max(
      1,
      Math.min(50, Number(options.liveMaxPagesPerWallet || 1))
    );
    this.cacheEntriesPerEndpoint = Math.max(
      20,
      Number(options.cacheEntriesPerEndpoint || 500)
    );

    this.discoveryCost = Math.max(1, Number(options.discoveryCost || 1));
    this.historyCost = Math.max(1, Number(options.historyCost || 2));

    this.onchainPagesPerDiscoveryCycle = Math.max(
      1,
      Number(options.onchainPagesPerDiscoveryCycle || 2)
    );
    this.onchainPagesMaxPerCycle = Math.max(
      this.onchainPagesPerDiscoveryCycle,
      Number(options.onchainPagesMaxPerCycle || this.onchainPagesPerDiscoveryCycle)
    );
    this.onchainValidatePerCycle = Math.max(
      0,
      Number(options.onchainValidatePerCycle || 20)
    );
    this.discoveryOnly = Boolean(options.discoveryOnly);
    this.backlogModeEnabled =
      String(options.backlogModeEnabled !== undefined ? options.backlogModeEnabled : true) !==
      "false";
    this.backlogWalletThreshold = Math.max(
      1,
      Number(options.backlogWalletThreshold || 2000)
    );
    this.backlogAvgWaitMsThreshold = Math.max(
      1000,
      Number(options.backlogAvgWaitMsThreshold || 45 * 60 * 1000)
    );
    this.backlogDiscoverEveryCycles = Math.max(
      1,
      Number(options.backlogDiscoverEveryCycles || 8)
    );
    this.backlogRefillBatch = Math.max(
      this.maxWalletsPerScan,
      Number(options.backlogRefillBatch || this.maxWalletsPerScan * 8)
    );
    this.scanRampQuietMs = Math.max(
      10000,
      Number(options.scanRampQuietMs || 180000)
    );
    this.scanRampStepMs = Math.max(
      5000,
      Number(options.scanRampStepMs || 60000)
    );

    this.state = emptyState();
    this.runtime = {
      running: false,
      inDiscovery: false,
      inScan: false,
      discoveryTimer: null,
      scanTimer: null,
      lastError: null,
      onchainPagesCurrent: this.onchainPagesPerDiscoveryCycle,
      onchainLast429At: 0,
      onchainLastRampAt: 0,
      backlogMode: false,
      backlogReason: null,
      priorityScanQueue: [],
      priorityScanSet: new Set(),
      priorityEnqueuedAt: new Map(),
      liveScanQueue: [],
      liveScanSet: new Set(),
      liveEnqueuedAt: new Map(),
      scanConcurrencyCurrent: this.walletScanConcurrency,
      scanPagesCurrent: this.maxPagesPerWallet,
      scanLast429At: 0,
      scanLastRampAt: 0,
    };
  }

  isBackfillCompleteRow(row) {
    const safe = row || {};
    const hasSuccess =
      Number(safe.lastSuccessAt || 0) > 0 || Number(safe.scansSucceeded || 0) > 0;
    if (!hasSuccess) return false;
    if (!Boolean(safe.tradeDone) || !Boolean(safe.fundingDone)) return false;
    if (safe.tradeCursor) return false;
    if (safe.fundingCursor) return false;
    return true;
  }

  deriveWalletLifecycle(row) {
    const safe = row || {};
    const explicit = String(safe.lifecycleStage || "").trim();
    const hasSuccess =
      Number(safe.lastSuccessAt || 0) > 0 || Number(safe.scansSucceeded || 0) > 0;
    const hasAttempts =
      hasSuccess ||
      Number(safe.scansFailed || 0) > 0 ||
      Number(safe.lastAttemptAt || safe.lastScannedAt || 0) > 0;
    const complete = this.isBackfillCompleteRow(safe);

    if (complete) {
      if (explicit === WALLET_LIFECYCLE.FULLY_INDEXED || explicit === WALLET_LIFECYCLE.LIVE_TRACKING) {
        return explicit;
      }
      return safe.liveTrackingSince ? WALLET_LIFECYCLE.LIVE_TRACKING : WALLET_LIFECYCLE.FULLY_INDEXED;
    }
    if (hasAttempts) return WALLET_LIFECYCLE.BACKFILLING;
    if (explicit === WALLET_LIFECYCLE.DISCOVERED) return WALLET_LIFECYCLE.DISCOVERED;
    return WALLET_LIFECYCLE.PENDING_BACKFILL;
  }

  deriveWalletStatus(row) {
    const safe = row || {};
    const hasSuccess =
      Number(safe.lastSuccessAt || 0) > 0 || Number(safe.scansSucceeded || 0) > 0;
    const hasFailure = Number(safe.lastFailureAt || 0) > 0;
    const lastError = String(safe.lastError || "").trim();
    const lifecycle = this.deriveWalletLifecycle(safe);
    const hasMore = !this.isBackfillCompleteRow(safe);

    if (!hasSuccess) {
      return lastError ? "failed" : "pending";
    }

    if (hasFailure && Number(safe.lastFailureAt || 0) >= Number(safe.lastSuccessAt || 0)) {
      return "failed";
    }

    if (lastError && Number(safe.consecutiveFailures || 0) > 0) {
      return "failed";
    }

    if (hasMore || lifecycle === WALLET_LIFECYCLE.BACKFILLING) return "partial";
    return "indexed";
  }

  isWalletIndexed(wallet) {
    const normalized = normalizeAddress(wallet);
    if (!normalized) return false;

    const stateRow = this.state.walletStates && this.state.walletStates[normalized];
    return this.isBackfillCompleteRow(stateRow);
  }

  isWalletLiveTracking(wallet) {
    const normalized = normalizeAddress(wallet);
    if (!normalized) return false;
    const stateRow = this.state.walletStates && this.state.walletStates[normalized];
    return this.deriveWalletLifecycle(stateRow) === WALLET_LIFECYCLE.LIVE_TRACKING;
  }

  removeFromPriorityQueue(wallet) {
    const normalized = normalizeAddress(wallet);
    if (!normalized) return;
    if (!this.runtime.priorityScanSet.has(normalized)) return;
    this.runtime.priorityScanSet.delete(normalized);
    this.runtime.priorityEnqueuedAt.delete(normalized);
    this.runtime.priorityScanQueue = this.runtime.priorityScanQueue.filter((item) => item !== normalized);
  }

  enqueueLiveWallets(wallets = []) {
    const normalized = normalizeWallets(wallets);
    if (!normalized.length) return 0;
    let queued = 0;
    normalized.forEach((wallet) => {
      if (!this.runtime.liveScanSet.has(wallet)) {
        this.runtime.liveScanSet.add(wallet);
        this.runtime.liveScanQueue.push(wallet);
        this.runtime.liveEnqueuedAt.set(wallet, Date.now());
        queued += 1;
      }
    });
    return queued;
  }

  moveWalletToLiveGroup(wallet, at = Date.now()) {
    const normalized = normalizeAddress(wallet);
    if (!normalized) return false;
    const row = this.state.walletStates && this.state.walletStates[normalized];
    if (!row) return false;

    this.state.walletStates[normalized] = {
      ...row,
      lifecycleStage: WALLET_LIFECYCLE.LIVE_TRACKING,
      backfillCompletedAt: Number(row.backfillCompletedAt || at),
      liveTrackingSince: Number(row.liveTrackingSince || at),
      liveLastScanAt: Number(row.liveLastScanAt || 0) || null,
    };

    const liveSet = new Set(normalizeWallets(this.state.liveWallets || []));
    if (!liveSet.has(normalized)) {
      liveSet.add(normalized);
      this.state.liveWallets = Array.from(liveSet.values());
    }

    this.removeFromPriorityQueue(normalized);
    this.enqueueLiveWallets([normalized]);
    return true;
  }

  removeFromLiveGroup(wallet) {
    const normalized = normalizeAddress(wallet);
    if (!normalized) return;
    this.state.liveWallets = normalizeWallets((this.state.liveWallets || []).filter((item) => item !== normalized));
    if (this.runtime.liveScanSet.has(normalized)) {
      this.runtime.liveScanSet.delete(normalized);
      this.runtime.liveEnqueuedAt.delete(normalized);
      this.runtime.liveScanQueue = this.runtime.liveScanQueue.filter((item) => item !== normalized);
    }
  }

  enqueuePriorityWallets(wallets = [], options = {}) {
    const normalized = normalizeWallets(wallets);
    if (!normalized.length) return 0;
    const includeLive = Boolean(options.includeLive);
    const reason = String(options.reason || "queue");

    let queued = 0;
    normalized.forEach((wallet) => {
      const row = this.state.walletStates && this.state.walletStates[wallet];
      const lifecycle = this.deriveWalletLifecycle(row);
      if (!includeLive && lifecycle === WALLET_LIFECYCLE.LIVE_TRACKING) return;
      if (this.isWalletIndexed(wallet) && !includeLive) return;
      if (this.runtime.priorityScanSet.has(wallet)) return;
      const current = this.getOrInitWalletState(wallet, "queue");
      if (lifecycle === WALLET_LIFECYCLE.DISCOVERED || lifecycle === WALLET_LIFECYCLE.PENDING_BACKFILL) {
        this.state.walletStates[wallet] = {
          ...current,
          lifecycleStage: WALLET_LIFECYCLE.PENDING_BACKFILL,
          queueReason: reason,
          queuedAt: Date.now(),
        };
      }
      this.runtime.priorityScanSet.add(wallet);
      this.runtime.priorityScanQueue.push(wallet);
      this.runtime.priorityEnqueuedAt.set(wallet, Date.now());
      queued += 1;
    });

    return queued;
  }

  walletHistoryPath(wallet) {
    return path.join(this.walletHistoryDir, `${wallet}.json`);
  }

  loadWalletHistory(wallet) {
    ensureDir(this.walletHistoryDir);
    const filePath = this.walletHistoryPath(wallet);
    const loaded = readJson(filePath, null);
    const base = emptyWalletHistory(wallet);
    if (!loaded || typeof loaded !== "object") return base;

    return {
      ...base,
      ...loaded,
      wallet,
      trades: Array.isArray(loaded.trades) ? loaded.trades : [],
      funding: Array.isArray(loaded.funding) ? loaded.funding : [],
      tradeSeenKeys:
        loaded.tradeSeenKeys && typeof loaded.tradeSeenKeys === "object"
          ? loaded.tradeSeenKeys
          : {},
      fundingSeenKeys:
        loaded.fundingSeenKeys && typeof loaded.fundingSeenKeys === "object"
          ? loaded.fundingSeenKeys
          : {},
      pageCache: {
        trades:
          loaded.pageCache &&
          loaded.pageCache.trades &&
          typeof loaded.pageCache.trades === "object"
            ? loaded.pageCache.trades
            : {},
        funding:
          loaded.pageCache &&
          loaded.pageCache.funding &&
          typeof loaded.pageCache.funding === "object"
            ? loaded.pageCache.funding
            : {},
      },
    };
  }

  saveWalletHistory(wallet, history) {
    ensureDir(this.walletHistoryDir);
    const filePath = this.walletHistoryPath(wallet);
    const out = {
      ...emptyWalletHistory(wallet),
      ...(history || {}),
      wallet,
      updatedAt: Date.now(),
    };
    writeJsonAtomic(filePath, out);
  }

  trimPageCache(cache = {}) {
    const entries = Object.entries(cache);
    if (entries.length <= this.cacheEntriesPerEndpoint) return cache;
    const sorted = entries.sort((a, b) => {
      const aTs = Number(a[1] && a[1].touchedAt ? a[1].touchedAt : a[1].fetchedAt || 0);
      const bTs = Number(b[1] && b[1].touchedAt ? b[1].touchedAt : b[1].fetchedAt || 0);
      return bTs - aTs;
    });
    const next = {};
    sorted.slice(0, this.cacheEntriesPerEndpoint).forEach(([key, value]) => {
      next[key] = value;
    });
    return next;
  }

  getOrInitWalletState(wallet, source = "unknown") {
    const existing = this.state.walletStates[wallet] || {};
    const lifecycleStage = this.deriveWalletLifecycle(existing);
    const merged = {
      ...existing,
      wallet,
      discoveredBy: existing.discoveredBy || source,
      discoveredAt: existing.discoveredAt || Date.now(),
      scansSucceeded: Number(existing.scansSucceeded || 0),
      scansFailed: Number(existing.scansFailed || 0),
      consecutiveFailures: Number(existing.consecutiveFailures || 0),
      tradeCursor: Object.prototype.hasOwnProperty.call(existing, "tradeCursor")
        ? existing.tradeCursor
        : null,
      fundingCursor: Object.prototype.hasOwnProperty.call(existing, "fundingCursor")
        ? existing.fundingCursor
        : null,
      tradeDone: Boolean(existing.tradeDone),
      fundingDone: Boolean(existing.fundingDone),
      tradeHasMore: Boolean(existing.tradeHasMore),
      fundingHasMore: Boolean(existing.fundingHasMore),
      lifecycleStage:
        lifecycleStage ||
        (source === "seed" || source === "store" || source === "onchain_bootstrap"
          ? WALLET_LIFECYCLE.PENDING_BACKFILL
          : WALLET_LIFECYCLE.DISCOVERED),
      backfillCompletedAt: Number(existing.backfillCompletedAt || 0) || null,
      liveTrackingSince: Number(existing.liveTrackingSince || 0) || null,
      liveLastScanAt: Number(existing.liveLastScanAt || 0) || null,
    };
    this.state.walletStates[wallet] = merged;
    return merged;
  }

  refillPriorityFromBacklog(limit = this.backlogRefillBatch) {
    const rows = Object.values(this.state.walletStates || {});
    const candidates = rows
      .map((row) => ({
        wallet: row.wallet,
        status: this.deriveWalletStatus(row),
        discoveredAt: Number(row.discoveredAt || 0),
        lastAttemptAt: Number(row.lastAttemptAt || row.lastScannedAt || 0),
      }))
      .filter((row) => row.wallet)
      .filter((row) => row.status !== "indexed")
      .sort((a, b) => {
        const pri = { failed: 0, pending: 1, partial: 2 };
        const ap = Object.prototype.hasOwnProperty.call(pri, a.status) ? pri[a.status] : 9;
        const bp = Object.prototype.hasOwnProperty.call(pri, b.status) ? pri[b.status] : 9;
        if (ap !== bp) return ap - bp;
        const aTs = a.lastAttemptAt || a.discoveredAt || 0;
        const bTs = b.lastAttemptAt || b.discoveredAt || 0;
        return aTs - bTs;
      })
      .slice(0, Math.max(1, Number(limit || this.backlogRefillBatch)));

    return this.enqueuePriorityWallets(candidates.map((row) => row.wallet));
  }

  evaluateBacklogMode(summary) {
    if (!this.backlogModeEnabled) {
      return {
        active: false,
        reason: null,
      };
    }
    const backlog = Number(summary && summary.backlog ? summary.backlog : 0);
    const avgWait = Number(summary && summary.averagePendingWaitMs ? summary.averagePendingWaitMs : 0);
    const byBacklog = backlog >= this.backlogWalletThreshold;
    const byWait = avgWait >= this.backlogAvgWaitMsThreshold;
    return {
      active: byBacklog || byWait,
      reason: byBacklog
        ? `backlog>${this.backlogWalletThreshold}`
        : byWait
        ? `avg_wait_ms>${this.backlogAvgWaitMsThreshold}`
        : null,
    };
  }

  load() {
    ensureDir(this.dataDir);
    ensureDir(this.walletHistoryDir);
    const loaded = readJson(this.statePath, null);
    if (
      loaded &&
      (loaded.version === STATE_VERSION ||
        loaded.version === 1 ||
        loaded.version === 2 ||
        loaded.version === 3)
    ) {
      this.state = {
        ...emptyState(),
        ...loaded,
        version: STATE_VERSION,
        knownWallets: normalizeWallets(loaded.knownWallets || []),
        liveWallets: normalizeWallets(loaded.liveWallets || []),
        priorityQueue: normalizeWallets(loaded.priorityQueue || []),
        walletStates:
          loaded.walletStates && typeof loaded.walletStates === "object"
            ? loaded.walletStates
            : {},
      };
    } else {
      this.state = emptyState();
    }

    this.addWallets(this.seedWallets, "seed");

    if (this.walletStore) {
      this.addWallets(
        this.walletStore.list().map((row) => row.wallet),
        "store"
      );
    }

    if (this.onchainDiscovery && typeof this.onchainDiscovery.load === "function") {
      this.onchainDiscovery.load();
      const discovered = this.onchainDiscovery
        .listWallets({ confidenceMin: 0.8 })
        .map((row) => row.wallet);
      this.addWallets(discovered, "onchain_bootstrap");
    }

    const depositRegistry = readJson(this.depositWalletsPath, null);
    const depositWallets = Array.isArray(depositRegistry)
      ? depositRegistry
      : depositRegistry && Array.isArray(depositRegistry.wallets)
      ? depositRegistry.wallets
      : [];
    if (depositWallets.length) {
      this.addWallets(depositWallets, "deposit_registry");
    }

    const restoredQueue = normalizeWallets(this.state.priorityQueue || []);
    const liveQueued = normalizeWallets(this.runtime.priorityScanQueue || []);
    this.runtime.priorityScanQueue = normalizeWallets([...restoredQueue, ...liveQueued]);
    this.runtime.priorityScanSet = new Set(this.runtime.priorityScanQueue);
    this.runtime.priorityEnqueuedAt = new Map(
      this.runtime.priorityScanQueue.map((wallet) => [wallet, Date.now()])
    );

    const restoredLive = normalizeWallets(this.state.liveWallets || []);
    this.runtime.liveScanQueue = normalizeWallets(restoredLive);
    this.runtime.liveScanSet = new Set(this.runtime.liveScanQueue);
    this.runtime.liveEnqueuedAt = new Map(
      this.runtime.liveScanQueue.map((wallet) => [wallet, Date.now()])
    );

    Object.values(this.state.walletStates || {}).forEach((row) => {
      if (!row || !row.wallet) return;
      if (this.isBackfillCompleteRow(row)) {
        this.moveWalletToLiveGroup(row.wallet, Number(row.lastSuccessAt || Date.now()));
      }
    });

    if (this.runtime.priorityScanQueue.length > 0) {
      this.logger.info(
        `[wallet-indexer] startup queue resumed=${this.runtime.priorityScanQueue.length} source=state.priorityQueue`
      );
    } else {
      const startupQueued = this.enqueuePriorityWallets(this.state.knownWallets);
      if (startupQueued > 0) {
        this.logger.info(
          `[wallet-indexer] startup backlog queued=${startupQueued} source=deposit_registry_or_known_wallets`
        );
      }
    }

    this.save();
    return this.state;
  }

  save() {
    this.state.priorityQueue = Array.isArray(this.runtime.priorityScanQueue)
      ? this.runtime.priorityScanQueue.slice()
      : [];
    this.state.liveWallets = Array.isArray(this.state.liveWallets)
      ? normalizeWallets(this.state.liveWallets)
      : [];
    writeJsonAtomic(this.statePath, this.state);
  }

  resetIndexingState(options = {}) {
    if (this.runtime.inScan || this.runtime.inDiscovery) {
      throw new Error("indexer is busy; retry reset after current cycle completes");
    }

    const preserveKnownWallets = options.preserveKnownWallets !== false;
    const resetWalletStore = options.resetWalletStore !== false;
    const clearHistoryFiles = options.clearHistoryFiles !== false;

    const knownWallets = preserveKnownWallets ? normalizeWallets(this.state.knownWallets) : [];
    const knownCountBefore = Number(this.state.knownWallets.length || 0);
    const historyFileCountBefore = fs.existsSync(this.walletHistoryDir)
      ? fs
          .readdirSync(this.walletHistoryDir)
          .filter((name) => String(name || "").toLowerCase().endsWith(".json")).length
      : 0;

    this.runtime.priorityScanQueue = [];
    this.runtime.priorityScanSet = new Set();
    this.runtime.priorityEnqueuedAt = new Map();
    this.runtime.liveScanQueue = [];
    this.runtime.liveScanSet = new Set();
    this.runtime.liveEnqueuedAt = new Map();
    this.runtime.scanConcurrencyCurrent = this.walletScanConcurrency;
    this.runtime.scanPagesCurrent = this.maxPagesPerWallet;
    this.runtime.lastError = null;

    if (clearHistoryFiles) {
      fs.rmSync(this.walletHistoryDir, { recursive: true, force: true });
      ensureDir(this.walletHistoryDir);
    }

    if (resetWalletStore && this.walletStore && typeof this.walletStore.reset === "function") {
      this.walletStore.reset();
    }

    this.state = {
      ...emptyState(),
      version: STATE_VERSION,
      knownWallets,
      walletStates: {},
      scanCursor: 0,
      liveScanCursor: 0,
      liveWallets: [],
      lastDiscoveryAt: this.state.lastDiscoveryAt || null,
      discoveryCycles: this.state.discoveryCycles || 0,
      scanCycles: 0,
    };

    knownWallets.forEach((wallet) => {
      this.getOrInitWalletState(wallet, "reset");
    });
    const queued = this.enqueuePriorityWallets(knownWallets);
    this.save();

    return {
      ok: true,
      preserveKnownWallets,
      resetWalletStore,
      clearHistoryFiles,
      knownWalletsBefore: knownCountBefore,
      knownWalletsAfter: knownWallets.length,
      historyFilesBefore: historyFileCountBefore,
      queuedWallets: queued,
      resetAt: Date.now(),
    };
  }

  addWallets(wallets = [], source = "unknown") {
    const normalized = normalizeWallets(wallets);
    if (!normalized.length) return 0;

    const knownSet = new Set(this.state.knownWallets);
    let added = 0;

    normalized.forEach((wallet) => {
      if (!knownSet.has(wallet)) {
        knownSet.add(wallet);
        added += 1;
      }

      this.getOrInitWalletState(wallet, source);

      if (!this.isWalletIndexed(wallet)) {
        this.enqueuePriorityWallets([wallet]);
      }
    });

    if (added > 0) {
      this.state.knownWallets = Array.from(knownSet.values());
    }

    return added;
  }

  async discoverWallets() {
    if (this.runtime.inDiscovery) {
      return {
        added: 0,
        total: this.state.knownWallets.length,
      };
    }

    this.runtime.inDiscovery = true;

    let added = 0;
    let onchain = null;

    try {
      if (this.walletSource && typeof this.walletSource.fetchWallets === "function") {
        const wallets = await this.walletSource.fetchWallets();
        added += this.addWallets(wallets, "source");
      }

      if (
        this.onchainDiscovery &&
        typeof this.onchainDiscovery.discoverStep === "function"
      ) {
        const summaryBefore = this.buildDiagnosticsSummary();
        const backlogMode = this.evaluateBacklogMode(summaryBefore);
        this.runtime.backlogMode = backlogMode.active;
        this.runtime.backlogReason = backlogMode.reason;

        const cycle = Number(this.state.discoveryCycles || 0);
        const shouldThrottleDiscovery =
          backlogMode.active &&
          cycle % this.backlogDiscoverEveryCycles !== 0;
        const onchainPages = shouldThrottleDiscovery
          ? 0
          : Math.max(
              1,
              Math.min(this.onchainPagesMaxPerCycle, Number(this.runtime.onchainPagesCurrent || 1))
            );

        if (backlogMode.active) {
          const refilled = this.refillPriorityFromBacklog();
          this.logger.info(
            `[wallet-indexer] backlog_mode=on reason=${backlogMode.reason} backlog=${summaryBefore.backlog} avg_pending_wait_ms=${summaryBefore.averagePendingWaitMs} onchain_pages=${onchainPages} refilled_priority=${refilled}`
          );
        }

        if (onchainPages > 0) {
          this.logger.info(
            `[wallet-indexer] onchain step start pages=${onchainPages} known_wallets=${this.state.knownWallets.length} priority_queue=${this.runtime.priorityScanQueue.length}`
          );
          onchain = await this.onchainDiscovery.discoverStep({
            pages: onchainPages,
            validateLimit: this.onchainValidatePerCycle,
          });
        } else {
          onchain = {
            ok: true,
            skipped: true,
            reason: "backlog_mode_backpressure",
            scannedPrograms: 0,
            newWallets: [],
            pendingTransactions: null,
            progress: null,
          };
        }

        if (!onchain || onchain.ok !== true) {
          this.logger.warn(
            `[wallet-indexer] onchain step failed: ${
              onchain && onchain.error ? onchain.error : "unknown_error"
            }`
          );
        } else {
          const progressPct =
            onchain.progress && onchain.progress.pct !== null && onchain.progress.pct !== undefined
              ? onchain.progress.pct
              : "n/a";
          this.logger.info(
            `[wallet-indexer] onchain step ok scanned_programs=${onchain.scannedPrograms || 0} new_wallets=${(onchain.newWallets || []).length} pending_tx=${onchain.pendingTransactions || 0} progress_pct=${progressPct}`
          );
        }

        const rpc429 = Number(onchain && onchain.rpc ? onchain.rpc.rate429PerMin || 0 : 0);
        const now = Date.now();
        if (onchainPages === 0) {
          this.runtime.onchainPagesCurrent = 1;
        } else if (rpc429 > 0) {
          this.runtime.onchainLast429At = now;
          const next = Math.max(1, Math.floor(onchainPages / 2) || 1);
          this.runtime.onchainPagesCurrent = next;
        } else if (
          onchainPages < this.onchainPagesMaxPerCycle &&
          now - Number(this.runtime.onchainLast429At || 0) > 180000 &&
          now - Number(this.runtime.onchainLastRampAt || 0) > 60000
        ) {
          this.runtime.onchainPagesCurrent = onchainPages + 1;
          this.runtime.onchainLastRampAt = now;
        } else {
          this.runtime.onchainPagesCurrent = onchainPages;
        }

        if (
          onchain &&
          onchain.ok &&
          Array.isArray(onchain.newWallets) &&
          typeof this.onchainDiscovery.listWallets === "function"
        ) {
          const walletRows = this.onchainDiscovery.listWallets({
            confidenceMin: 0.75,
            onlyConfirmed: false,
          });
          const rowMap = new Map(walletRows.map((row) => [row.wallet, row]));
          const eligible = onchain.newWallets.filter((wallet) => {
            const row = rowMap.get(wallet);
            if (!row) return false;
            if (row.validation && row.validation.status === "rejected") return false;
            return Number(row.confidenceMax || 0) >= 0.75;
          });

          added += this.addWallets(eligible, "onchain");
        }
      }

      if (this.walletStore) {
        added += this.addWallets(
          this.walletStore.list().map((row) => row.wallet),
          "store"
        );
      }

      this.state.lastDiscoveryAt = Date.now();
      this.state.discoveryCycles += 1;
      this.save();

      const summary = this.buildDiagnosticsSummary();
      const topReason =
        summary.topErrorReasons && summary.topErrorReasons.length
          ? `${summary.topErrorReasons[0].reason}:${summary.topErrorReasons[0].count}`
          : "none";
      this.logger.info(
        `[wallet-indexer] discovery summary known=${this.state.knownWallets.length} indexed=${summary.indexed} partial=${summary.partial} pending=${summary.pending} failed=${summary.failed} backlog=${summary.backlog} queue=${this.runtime.priorityScanQueue.length} avg_pending_wait_ms=${summary.averagePendingWaitMs} avg_queue_wait_ms=${summary.averageQueueWaitMs} top_error=${topReason}`
      );
    } catch (error) {
      this.runtime.lastError = `[discover] ${toErrorMessage(error)}`;
      this.logger.warn(`[wallet-indexer] discovery failed: ${toErrorMessage(error)}`);
    } finally {
      this.runtime.inDiscovery = false;
    }

    return {
      added,
      total: this.state.knownWallets.length,
      onchain,
    };
  }

  pickWalletBatch() {
    const wallets = normalizeWallets(this.state.knownWallets || []);
    const liveWallets = normalizeWallets(this.state.liveWallets || []);
    if (!wallets.length && !liveWallets.length) return [];

    const tasks = [];
    const selected = new Set();

    const backfillCapacity = Math.min(this.maxWalletsPerScan, wallets.length);
    let backfillCursor = wallets.length > 0 ? this.state.scanCursor % wallets.length : 0;

    while (tasks.length < backfillCapacity && this.runtime.priorityScanQueue.length > 0) {
      const wallet = this.runtime.priorityScanQueue.shift();
      this.runtime.priorityScanSet.delete(wallet);
      this.runtime.priorityEnqueuedAt.delete(wallet);
      if (!wallet) continue;
      if (selected.has(wallet)) continue;
      if (this.isWalletLiveTracking(wallet)) continue;
      const row = this.state.walletStates && this.state.walletStates[wallet];
      const lifecycle = this.deriveWalletLifecycle(row);
      if (lifecycle === WALLET_LIFECYCLE.LIVE_TRACKING) continue;
      selected.add(wallet);
      tasks.push({ wallet, mode: "backfill", reason: "priority" });
    }

    let attempts = 0;
    const maxAttempts = wallets.length * 4;
    while (tasks.length < backfillCapacity && attempts < maxAttempts && wallets.length > 0) {
      const wallet = wallets[backfillCursor];
      backfillCursor = (backfillCursor + 1) % wallets.length;
      attempts += 1;
      if (!wallet || selected.has(wallet)) continue;
      if (this.isWalletLiveTracking(wallet)) continue;
      const row = this.state.walletStates && this.state.walletStates[wallet];
      const lifecycle = this.deriveWalletLifecycle(row);
      if (lifecycle === WALLET_LIFECYCLE.LIVE_TRACKING) continue;
      selected.add(wallet);
      tasks.push({ wallet, mode: "backfill", reason: "round_robin" });
    }
    this.state.scanCursor = backfillCursor;

    const liveCapacity = Math.max(0, Math.min(this.liveWalletsPerScan, liveWallets.length));
    let liveCursor = liveWallets.length > 0 ? this.state.liveScanCursor % liveWallets.length : 0;
    let liveAttempts = 0;
    const maxLiveAttempts = liveWallets.length * 3;
    while (liveCapacity > 0 && liveAttempts < maxLiveAttempts) {
      if (tasks.filter((task) => task.mode === "live").length >= liveCapacity) break;
      const wallet = liveWallets[liveCursor];
      liveCursor = (liveCursor + 1) % liveWallets.length;
      liveAttempts += 1;
      if (!wallet || selected.has(wallet)) continue;
      if (!this.isWalletLiveTracking(wallet)) continue;
      selected.add(wallet);
      tasks.push({ wallet, mode: "live", reason: "live_round_robin" });
    }
    this.state.liveScanCursor = liveCursor;

    return tasks;
  }

  async fetchPaginatedHistory(
    pathname,
    {
      restClient,
      endpointKey,
      account,
      limit,
      maxPages,
      startCursor = null,
      done = false,
      pageCache = {},
    }
  ) {
    let cursor = startCursor || null;
    let pages = 0;
    let hasMore = !done;
    let lastNextCursor = cursor;
    const rows = [];
    let cacheHits = 0;
    let requests = 0;
    let doneNow = Boolean(done);
    const parsedMaxPages = Number(maxPages);
    const unlimitedPaging =
      !Number.isFinite(parsedMaxPages) || parsedMaxPages <= 0;
    const maxPagesLimit = unlimitedPaging
      ? Number.POSITIVE_INFINITY
      : Math.max(1, Math.floor(parsedMaxPages));
    const cache = pageCache && typeof pageCache === "object" ? { ...pageCache } : {};

    while (pages < maxPagesLimit && !doneNow) {
      const key = cursorCacheKey(cursor);
      const cached = cache[key];
      if (cached && typeof cached === "object") {
        cacheHits += 1;
        pages += 1;
        cached.touchedAt = Date.now();
        hasMore = Boolean(cached.hasMore);
        const nextCursor = cached.nextCursor || null;
        lastNextCursor = nextCursor;
        doneNow = !hasMore || !nextCursor;
        cursor = doneNow ? null : nextCursor;
        continue;
      }

      const query = {
        account,
        limit,
      };
      if (cursor) query.cursor = cursor;

      const response = await restClient.get(pathname, {
        query,
        cost: this.historyCost,
      });
      requests += 1;

      const payload = response && response.payload ? response.payload : {};
      const pageRows = extractPayloadData(response, []);
      if (Array.isArray(pageRows) && pageRows.length) rows.push(...pageRows);

      pages += 1;
      hasMore = Boolean(payload.has_more);
      const nextCursor = payload.next_cursor || null;
      lastNextCursor = nextCursor;

      cache[key] = {
        endpoint: endpointKey,
        fetchedAt: Date.now(),
        touchedAt: Date.now(),
        hasMore,
        nextCursor,
      };

      doneNow = !hasMore || !nextCursor;
      cursor = doneNow ? null : nextCursor;
    }

    return {
      rows,
      pages,
      hasMore: !doneNow && hasMore,
      done: doneNow,
      nextCursor: doneNow ? null : lastNextCursor,
      cacheHits,
      requests,
      pageCache: this.trimPageCache(cache),
    };
  }

  async scanWallet(wallet, restClient, options = {}) {
    const now = Date.now();
    const startedAt = now;
    const prev = this.getOrInitWalletState(wallet, "scan");
    const mode = options && options.mode === "live" ? "live" : "backfill";
    const history = this.loadWalletHistory(wallet);
    const client =
      restClient && typeof restClient.get === "function" ? restClient : this.restClients[0];

    if (mode === "backfill") {
      const currentLifecycle = this.deriveWalletLifecycle(prev);
      if (
        currentLifecycle === WALLET_LIFECYCLE.DISCOVERED ||
        currentLifecycle === WALLET_LIFECYCLE.PENDING_BACKFILL
      ) {
        this.state.walletStates[wallet] = {
          ...prev,
          lifecycleStage: WALLET_LIFECYCLE.BACKFILLING,
          lastAttemptAt: now,
        };
      }
    }

    try {
      const useFullHistory = mode === "backfill" ? this.fullHistoryPerWallet : false;
      const maxPagesForMode =
        mode === "live"
          ? this.liveMaxPagesPerWallet
          : useFullHistory
          ? 0
          : this.runtime.scanPagesCurrent;
      const tradeStartCursor = mode === "live" ? null : prev.tradeCursor || null;
      const fundingStartCursor = mode === "live" ? null : prev.fundingCursor || null;
      const tradeDoneInput = mode === "live" ? false : Boolean(prev.tradeDone);
      const fundingDoneInput = mode === "live" ? false : Boolean(prev.fundingDone);

      const [tradesRes, fundingRes] = await Promise.all([
        this.fetchPaginatedHistory("/trades/history", {
          restClient: client,
          endpointKey: "trades",
          account: wallet,
          limit: this.tradesPageLimit,
          maxPages: maxPagesForMode,
          startCursor: tradeStartCursor,
          done: tradeDoneInput,
          pageCache: history.pageCache && history.pageCache.trades ? history.pageCache.trades : {},
        }),
        this.fetchPaginatedHistory("/funding/history", {
          restClient: client,
          endpointKey: "funding",
          account: wallet,
          limit: this.fundingPageLimit,
          maxPages: maxPagesForMode,
          startCursor: fundingStartCursor,
          done: fundingDoneInput,
          pageCache:
            history.pageCache && history.pageCache.funding ? history.pageCache.funding : {},
        }),
      ]);

      const incomingTrades = uniq(
        tradesRes.rows.map((row) => JSON.stringify(normalizeTradeHistoryRow(row)))
      ).map((raw) => JSON.parse(raw));
      const incomingFunding = uniq(
        fundingRes.rows.map((row) => JSON.stringify(normalizeFundingHistoryRow(row)))
      ).map((raw) => JSON.parse(raw));

      let newTrades = 0;
      incomingTrades.forEach((row) => {
        const key = normalizeTradeKey(row);
        if (history.tradeSeenKeys[key]) return;
        history.tradeSeenKeys[key] = 1;
        history.trades.push(row);
        newTrades += 1;
      });

      let newFunding = 0;
      incomingFunding.forEach((row) => {
        const key = normalizeFundingKey(row);
        if (history.fundingSeenKeys[key]) return;
        history.fundingSeenKeys[key] = 1;
        history.funding.push(row);
        newFunding += 1;
      });

      history.pageCache = {
        trades: tradesRes.pageCache || {},
        funding: fundingRes.pageCache || {},
      };
      this.saveWalletHistory(wallet, history);

      const record = buildWalletRecordFromHistory({
        wallet,
        tradesHistory: history.trades,
        fundingHistory: history.funding,
        computedAt: now,
      });

      this.walletStore.upsert(record);

      let tradeDone = Boolean(tradesRes.done);
      let fundingDone = Boolean(fundingRes.done);
      let lifecycleStage = WALLET_LIFECYCLE.BACKFILLING;
      let backfillCompletedAt = Number(prev.backfillCompletedAt || 0) || null;
      let liveTrackingSince = Number(prev.liveTrackingSince || 0) || null;
      let liveLastScanAt = Number(prev.liveLastScanAt || 0) || null;
      let tradeCursor = tradesRes.nextCursor || null;
      let fundingCursor = fundingRes.nextCursor || null;
      let tradeHasMore = !tradeDone;
      let fundingHasMore = !fundingDone;

      if (mode === "live") {
        const tradeCapacity = Math.max(1, this.tradesPageLimit * this.liveMaxPagesPerWallet);
        const fundingCapacity = Math.max(1, this.fundingPageLimit * this.liveMaxPagesPerWallet);
        const liveOverflow = newTrades >= tradeCapacity || newFunding >= fundingCapacity;
        if (liveOverflow) {
          // Overflow guard: if a live polling window is fully filled, switch this wallet back
          // to backfill mode so historical catch-up can close potential gaps deterministically.
          tradeDone = false;
          fundingDone = false;
          tradeCursor = null;
          fundingCursor = null;
          tradeHasMore = true;
          fundingHasMore = true;
          lifecycleStage = WALLET_LIFECYCLE.BACKFILLING;
          this.removeFromLiveGroup(wallet);
        } else {
          tradeDone = true;
          fundingDone = true;
          tradeCursor = null;
          fundingCursor = null;
          tradeHasMore = false;
          fundingHasMore = false;
          lifecycleStage = WALLET_LIFECYCLE.LIVE_TRACKING;
          backfillCompletedAt = Number(backfillCompletedAt || prev.lastSuccessAt || now);
          liveTrackingSince = Number(liveTrackingSince || now);
          liveLastScanAt = now;
        }
      } else if (tradeDone && fundingDone) {
        lifecycleStage = WALLET_LIFECYCLE.FULLY_INDEXED;
        backfillCompletedAt = Number(backfillCompletedAt || now);
      }

      this.state.walletStates[wallet] = {
        ...prev,
        wallet,
        lastAttemptAt: now,
        lastScannedAt: now,
        lastSuccessAt: now,
        lastError: null,
        lastErrorReason: null,
        lastFailureAt: prev.lastFailureAt || null,
        scansSucceeded: Number(prev.scansSucceeded || 0) + 1,
        scansFailed: Number(prev.scansFailed || 0),
        consecutiveFailures: 0,
        lastScanDurationMs: Date.now() - startedAt,
        tradeRowsLoaded: history.trades.length,
        fundingRowsLoaded: history.funding.length,
        newTradeRowsLoaded: newTrades,
        newFundingRowsLoaded: newFunding,
        tradePagesLoaded: tradesRes.pages,
        fundingPagesLoaded: fundingRes.pages,
        tradeHasMore,
        fundingHasMore,
        tradeDone,
        fundingDone,
        tradeCursor,
        fundingCursor,
        lastTradeNextCursor: tradesRes.nextCursor || null,
        lastFundingNextCursor: fundingRes.nextCursor || null,
        tradeCacheHits: Number(prev.tradeCacheHits || 0) + Number(tradesRes.cacheHits || 0),
        fundingCacheHits: Number(prev.fundingCacheHits || 0) + Number(fundingRes.cacheHits || 0),
        tradeRequests: Number(prev.tradeRequests || 0) + Number(tradesRes.requests || 0),
        fundingRequests: Number(prev.fundingRequests || 0) + Number(fundingRes.requests || 0),
        lifecycleStage,
        backfillCompletedAt,
        liveTrackingSince,
        liveLastScanAt,
      };
      if (tradeDone && fundingDone) {
        this.moveWalletToLiveGroup(wallet, now);
      } else if (mode === "backfill") {
        this.enqueuePriorityWallets([wallet], { reason: "continue_backfill" });
      } else {
        this.enqueuePriorityWallets([wallet], { reason: "live_overflow_catchup", includeLive: true });
      }
      this.save();

      return {
        wallet,
        ok: true,
        mode,
        trades: history.trades.length,
        funding: history.funding.length,
        newTrades,
        newFunding,
      };
    } catch (error) {
      const message = toErrorMessage(error);
      this.state.walletStates[wallet] = {
        ...prev,
        wallet,
        lastAttemptAt: now,
        lastScannedAt: now,
        lastFailureAt: now,
        lastError: message,
        lastErrorReason: summarizeErrorReason(message),
        scansSucceeded: Number(prev.scansSucceeded || 0),
        scansFailed: Number(prev.scansFailed || 0) + 1,
        consecutiveFailures: Number(prev.consecutiveFailures || 0) + 1,
        lastScanDurationMs: Date.now() - startedAt,
        lifecycleStage: this.isBackfillCompleteRow(prev)
          ? WALLET_LIFECYCLE.LIVE_TRACKING
          : WALLET_LIFECYCLE.BACKFILLING,
      };
      if (!this.isBackfillCompleteRow(prev)) {
        this.enqueuePriorityWallets([wallet], { reason: "retry_failed" });
      } else {
        this.enqueueLiveWallets([wallet]);
      }
      this.save();

      return {
        wallet,
        ok: false,
        mode,
        error: message,
      };
    }
  }

  async scanCycle() {
    if (this.discoveryOnly) {
      return {
        startedAt: Date.now(),
        endedAt: Date.now(),
        scanned: 0,
        ok: 0,
        failed: 0,
        skipped: true,
      };
    }

    if (this.runtime.inScan) return null;
    this.runtime.inScan = true;

    const startedAt = Date.now();
    const summaryBefore = this.buildDiagnosticsSummary();
    const backlogMode = this.evaluateBacklogMode(summaryBefore);
    this.runtime.backlogMode = backlogMode.active;
    this.runtime.backlogReason = backlogMode.reason;
    if (backlogMode.active) {
      this.refillPriorityFromBacklog();
    }

    const batch = this.pickWalletBatch();
    const backfillBatchSize = batch.filter((item) => item && item.mode === "backfill").length;
    const liveBatchSize = batch.filter((item) => item && item.mode === "live").length;
    let ok = 0;
    let failed = 0;
    const errorReasons = new Map();

    try {
      let index = 0;
      const workers = [];
      const workerCount = Math.max(
        1,
        Math.min(this.runtime.scanConcurrencyCurrent, batch.length || 1)
      );

      const worker = async (workerIndex) => {
        const client = this.restClients[workerIndex % this.restClients.length];
        while (true) {
          const current = index;
          index += 1;
          if (current >= batch.length) break;

          const task = batch[current];
          if (!task || !task.wallet) continue;
          const result = await this.scanWallet(task.wallet, client, { mode: task.mode });
          if (result && result.ok) ok += 1;
          else {
            failed += 1;
            const reason = summarizeErrorReason(result && result.error ? result.error : "");
            errorReasons.set(reason, (errorReasons.get(reason) || 0) + 1);
          }
        }
      };

      for (let i = 0; i < workerCount; i += 1) {
        workers.push(worker(i));
      }
      await Promise.all(workers);

      this.state.lastScanAt = Date.now();
      this.state.scanCycles += 1;
      this.save();

      const now = Date.now();
      const saw429 = Number(errorReasons.get("rate_limit_429") || 0) > 0;
      if (saw429) {
        this.runtime.scanLast429At = now;
        this.runtime.scanConcurrencyCurrent = Math.max(
          1,
          Math.floor(this.runtime.scanConcurrencyCurrent / 2) || 1
        );
        if (!this.fullHistoryPerWallet) {
          this.runtime.scanPagesCurrent = Math.max(
            1,
            Math.floor(this.runtime.scanPagesCurrent / 2) || 1
          );
        }
      } else if (
        this.runtime.scanConcurrencyCurrent < this.walletScanConcurrency &&
        now - Number(this.runtime.scanLast429At || 0) > this.scanRampQuietMs &&
        now - Number(this.runtime.scanLastRampAt || 0) > this.scanRampStepMs
      ) {
        this.runtime.scanConcurrencyCurrent += 1;
        this.runtime.scanLastRampAt = now;
      } else if (
        !this.fullHistoryPerWallet &&
        this.runtime.scanPagesCurrent < this.maxPagesPerWallet &&
        now - Number(this.runtime.scanLast429At || 0) > this.scanRampQuietMs &&
        now - Number(this.runtime.scanLastRampAt || 0) > this.scanRampStepMs
      ) {
        this.runtime.scanPagesCurrent += 1;
        this.runtime.scanLastRampAt = now;
      }

      const summary = this.buildDiagnosticsSummary();
      const topReason =
        summary.topErrorReasons && summary.topErrorReasons.length
          ? `${summary.topErrorReasons[0].reason}:${summary.topErrorReasons[0].count}`
          : "none";
      this.logger.info(
        `[wallet-indexer] scan summary scanned=${batch.length} backfill=${backfillBatchSize} live=${liveBatchSize} ok=${ok} failed=${failed} known=${this.state.knownWallets.length} indexed=${summary.indexed} partial=${summary.partial} pending=${summary.pending} failed_total=${summary.failed} live_tracking=${summary.liveTracking} backlog=${summary.backlog} queue=${this.runtime.priorityScanQueue.length} live_queue=${this.runtime.liveScanQueue.length} avg_pending_wait_ms=${summary.averagePendingWaitMs} avg_queue_wait_ms=${summary.averageQueueWaitMs} backlog_mode=${this.runtime.backlogMode ? "on" : "off"} backlog_reason=${this.runtime.backlogReason || "none"} scan_concurrency=${this.runtime.scanConcurrencyCurrent} scan_pages=${this.fullHistoryPerWallet ? "full_history" : this.runtime.scanPagesCurrent} top_error=${topReason}`
      );

      return {
        startedAt,
        endedAt: Date.now(),
        scanned: batch.length,
        scannedBackfill: backfillBatchSize,
        scannedLive: liveBatchSize,
        ok,
        failed,
        backlogMode: this.runtime.backlogMode,
        backlogReason: this.runtime.backlogReason,
        scanConcurrency: this.runtime.scanConcurrencyCurrent,
        scanPagesPerWallet: this.fullHistoryPerWallet ? null : this.runtime.scanPagesCurrent,
        scanHistoryMode: this.fullHistoryPerWallet ? "full_history" : "capped",
      };
    } finally {
      this.runtime.inScan = false;
    }
  }

  async start() {
    if (this.runtime.running) return;
    this.runtime.running = true;

    await this.discoverWallets();
    if (!this.discoveryOnly) {
      await this.scanCycle();
    }

    this.runtime.discoveryTimer = setInterval(() => {
      this.discoverWallets().catch((error) => {
        this.runtime.lastError = `[discover] ${toErrorMessage(error)}`;
      });
    }, this.discoveryIntervalMs);

    if (!this.discoveryOnly) {
      this.runtime.scanTimer = setInterval(() => {
        this.scanCycle().catch((error) => {
          this.runtime.lastError = `[scan] ${toErrorMessage(error)}`;
        });
      }, this.scanIntervalMs);
    }
  }

  stop() {
    this.runtime.running = false;
    if (this.runtime.discoveryTimer) {
      clearInterval(this.runtime.discoveryTimer);
      this.runtime.discoveryTimer = null;
    }
    if (this.runtime.scanTimer) {
      clearInterval(this.runtime.scanTimer);
      this.runtime.scanTimer = null;
    }
    this.save();
  }

  getStatus() {
    const diagnostics = this.buildDiagnosticsSummary();
    const backlogEval = this.evaluateBacklogMode(diagnostics);
    const indexedWallets = this.walletStore ? this.walletStore.list().length : 0;
    const knownWallets = this.state.knownWallets.length;
    const completionPct =
      knownWallets > 0 ? (diagnostics.backfillComplete / knownWallets) * 100 : 0;

    return {
      running: this.runtime.running,
      inDiscovery: this.runtime.inDiscovery,
      inScan: this.runtime.inScan,
      knownWallets,
      discoveryOnly: this.discoveryOnly,
      indexedWallets,
      completionPct: Number(completionPct.toFixed(4)),
      attemptedWallets: diagnostics.attemptedWallets,
      successfulScans: diagnostics.successfulScans,
      failedScans: diagnostics.failedScans,
      indexedCompleteWallets: diagnostics.indexed,
      partiallyIndexedWallets: diagnostics.partial,
      pendingWallets: diagnostics.pending,
      failedWallets: diagnostics.failed,
      walletBacklog: diagnostics.backlog,
      failedBackfillWallets: diagnostics.failedBackfill,
      lifecycle: {
        discovered: diagnostics.discovered,
        pendingBackfill: diagnostics.pendingBackfill,
        backfilling: diagnostics.backfilling,
        failedBackfill: diagnostics.failedBackfill,
        fullyIndexed: diagnostics.fullyIndexed,
        liveTracking: diagnostics.liveTracking,
        backfillComplete: diagnostics.backfillComplete,
      },
      completionCondition: {
        rule:
          "trade_done=true AND funding_done=true AND trade_cursor=null AND funding_cursor=null from a successful scan",
        verifiedBy: [
          "wallet_state.tradeDone",
          "wallet_state.fundingDone",
          "wallet_state.tradeCursor",
          "wallet_state.fundingCursor",
          "wallet_state.lastSuccessAt",
        ],
      },
      scanCursor: this.state.scanCursor,
      liveScanCursor: this.state.liveScanCursor,
      scanIntervalMs: this.scanIntervalMs,
      discoveryIntervalMs: this.discoveryIntervalMs,
      maxWalletsPerScan: this.maxWalletsPerScan,
      liveWalletsPerScan: this.liveWalletsPerScan,
      liveMaxPagesPerWallet: this.liveMaxPagesPerWallet,
      maxPagesPerWallet: this.fullHistoryPerWallet ? null : this.maxPagesPerWallet,
      fullHistoryPerWallet: this.fullHistoryPerWallet,
      tradesPageLimit: this.tradesPageLimit,
      fundingPageLimit: this.fundingPageLimit,
      lastDiscoveryAt: this.state.lastDiscoveryAt,
      lastScanAt: this.state.lastScanAt,
      discoveryCycles: this.state.discoveryCycles,
      scanCycles: this.state.scanCycles,
      lastError: this.runtime.lastError,
      priorityQueueSize: this.runtime.priorityScanQueue.length,
      liveQueueSize: this.runtime.liveScanQueue.length,
      liveGroupSize: normalizeWallets(this.state.liveWallets || []).length,
      averagePendingWaitMs: diagnostics.averagePendingWaitMs,
      averageQueueWaitMs: diagnostics.averageQueueWaitMs,
      topErrorReasons: diagnostics.topErrorReasons,
      backlogMode: {
        active: backlogEval.active,
        reason: backlogEval.reason,
        runtimeActive: this.runtime.backlogMode,
        runtimeReason: this.runtime.backlogReason,
        enabled: this.backlogModeEnabled,
        thresholdWallets: this.backlogWalletThreshold,
        thresholdAvgWaitMs: this.backlogAvgWaitMsThreshold,
        discoverEveryCycles: this.backlogDiscoverEveryCycles,
      },
      adaptiveScan: {
        concurrencyCurrent: this.runtime.scanConcurrencyCurrent,
        concurrencyMax: this.walletScanConcurrency,
        pagesCurrent: this.fullHistoryPerWallet ? null : this.runtime.scanPagesCurrent,
        pagesMax: this.fullHistoryPerWallet ? null : this.maxPagesPerWallet,
        mode: this.fullHistoryPerWallet ? "full_history" : "capped",
      },
      restClients: {
        count: this.restClients.length,
        ids:
          this.restClientIds && this.restClientIds.length
            ? this.restClientIds.slice(0, this.restClients.length)
            : null,
      },
      source:
        this.walletSource && typeof this.walletSource.getState === "function"
          ? this.walletSource.getState()
          : null,
      onchain:
        this.onchainDiscovery && typeof this.onchainDiscovery.getStatus === "function"
          ? this.onchainDiscovery.getStatus()
          : null,
      onchainPages: {
        current: this.runtime.onchainPagesCurrent,
        configured: this.onchainPagesPerDiscoveryCycle,
        max: this.onchainPagesMaxPerCycle,
      },
    };
  }

  buildDiagnosticsSummary() {
    const rows = Object.values(this.state.walletStates || {});
    const now = Date.now();

    let indexed = 0;
    let partial = 0;
    let pending = 0;
    let failed = 0;
    let failedBackfill = 0;
    let attempted = 0;
    let succeeded = 0;
    let failedAttempts = 0;
    let pendingWaitSum = 0;
    let pendingWaitCount = 0;
    const discovered = rows.length;
    let pendingBackfill = 0;
    let backfilling = 0;
    let fullyIndexed = 0;
    let liveTracking = 0;

    const reasonCounts = new Map();
    rows.forEach((row) => {
      const status = this.deriveWalletStatus(row);
      const lifecycle = this.deriveWalletLifecycle(row);
      if (status === "indexed") indexed += 1;
      else if (status === "partial") partial += 1;
      else if (status === "failed") {
        failed += 1;
        if (lifecycle !== WALLET_LIFECYCLE.LIVE_TRACKING) {
          failedBackfill += 1;
        }
      }
      else pending += 1;
      if (lifecycle === WALLET_LIFECYCLE.DISCOVERED || lifecycle === WALLET_LIFECYCLE.PENDING_BACKFILL) pendingBackfill += 1;
      else if (lifecycle === WALLET_LIFECYCLE.BACKFILLING) backfilling += 1;
      else if (lifecycle === WALLET_LIFECYCLE.FULLY_INDEXED) fullyIndexed += 1;
      else if (lifecycle === WALLET_LIFECYCLE.LIVE_TRACKING) liveTracking += 1;

      const scansSucceeded = Number(row && row.scansSucceeded ? row.scansSucceeded : 0);
      const scansFailed = Number(row && row.scansFailed ? row.scansFailed : 0);
      const attemptsForRow = scansSucceeded + scansFailed;
      const hasLegacyAttempt = Boolean(
        row &&
          (row.lastAttemptAt || row.lastScannedAt || row.lastSuccessAt || row.lastFailureAt || row.lastError)
      );
      if (attemptsForRow > 0 || hasLegacyAttempt) {
        attempted += 1;
      }
      succeeded += scansSucceeded;
      failedAttempts += scansFailed > 0 ? scansFailed : row && row.lastError ? 1 : 0;

      if (status === "partial" || status === "pending" || status === "failed") {
        const discoveredAt = Number(row && row.discoveredAt ? row.discoveredAt : 0);
        if (discoveredAt > 0) {
          pendingWaitSum += Math.max(0, now - discoveredAt);
          pendingWaitCount += 1;
        }
      }

      if (status === "failed") {
        const reason =
          row && row.lastErrorReason
            ? String(row.lastErrorReason)
            : summarizeErrorReason(row && row.lastError ? row.lastError : "");
        reasonCounts.set(reason, (reasonCounts.get(reason) || 0) + 1);
      }
    });

    const queueWaitValues = this.runtime.priorityScanQueue
      .map((wallet) => {
        const at = Number(this.runtime.priorityEnqueuedAt.get(wallet) || 0);
        return at > 0 ? Math.max(0, now - at) : null;
      })
      .filter((value) => Number.isFinite(value));

    const topErrorReasons = Array.from(reasonCounts.entries())
      .map(([reason, count]) => ({ reason, count }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 10);

    return {
      indexed,
      partial,
      pending,
      failed,
      failedBackfill,
      backlog: pendingBackfill + backfilling + failedBackfill,
      discovered,
      pendingBackfill,
      backfilling,
      fullyIndexed,
      liveTracking,
      backfillComplete: fullyIndexed + liveTracking,
      averagePendingWaitMs:
        pendingWaitCount > 0 ? Math.round(pendingWaitSum / pendingWaitCount) : 0,
      averageQueueWaitMs:
        queueWaitValues.length > 0
          ? Math.round(queueWaitValues.reduce((sum, v) => sum + v, 0) / queueWaitValues.length)
          : 0,
      topErrorReasons,
      attemptedWallets: attempted,
      successfulScans: succeeded,
      failedScans: failedAttempts,
    };
  }

  getWalletLifecycleSnapshot(wallet) {
    const normalized = normalizeAddress(wallet);
    if (!normalized) return null;
    const row = this.state.walletStates && this.state.walletStates[normalized];
    if (!row) return null;
    const lifecycleStage = this.deriveWalletLifecycle(row);
    return {
      wallet: normalized,
      status: this.deriveWalletStatus(row),
      lifecycleStage,
      backfillComplete: this.isBackfillCompleteRow(row),
      discoveredAt: row.discoveredAt || null,
      backfillCompletedAt: row.backfillCompletedAt || null,
      liveTrackingSince: row.liveTrackingSince || null,
      liveLastScanAt: row.liveLastScanAt || null,
      lastAttemptAt: row.lastAttemptAt || null,
      lastSuccessAt: row.lastSuccessAt || null,
      lastFailureAt: row.lastFailureAt || null,
      lastErrorReason: row.lastErrorReason || null,
    };
  }

  getWalletLifecycleMap(wallets = []) {
    const out = {};
    normalizeWallets(wallets).forEach((wallet) => {
      const snapshot = this.getWalletLifecycleSnapshot(wallet);
      if (snapshot) out[wallet] = snapshot;
    });
    return out;
  }

  getDiagnostics(options = {}) {
    const page = Math.max(1, Number(options.page || 1));
    const pageSize = Math.max(1, Math.min(500, Number(options.pageSize || 100)));
    const statusFilter = String(options.status || "").toLowerCase();
    const lifecycleFilter = String(options.lifecycle || "").toLowerCase();
    const search = String(options.q || "").trim().toLowerCase();
    const now = Date.now();

    const rows = Object.values(this.state.walletStates || {}).map((row) => {
      const status = this.deriveWalletStatus(row);
      const lifecycleStage = this.deriveWalletLifecycle(row);
      const discoveredAt = Number(row && row.discoveredAt ? row.discoveredAt : 0);
      const scansSucceeded =
        Number(row && row.scansSucceeded ? row.scansSucceeded : 0) ||
        (row && row.lastSuccessAt ? 1 : 0);
      const scansFailed =
        Number(row && row.scansFailed ? row.scansFailed : 0) ||
        (row && row.lastError && !row.lastSuccessAt ? 1 : 0);
      const lastErrorReason =
        (row && row.lastErrorReason) ||
        summarizeErrorReason(row && row.lastError ? row.lastError : "");
      return {
        wallet: row.wallet,
        status,
        lifecycleStage,
        backfillComplete: this.isBackfillCompleteRow(row),
        liveTracking: lifecycleStage === WALLET_LIFECYCLE.LIVE_TRACKING,
        discoveredBy: row.discoveredBy || null,
        discoveredAt: discoveredAt || null,
        waitMs: discoveredAt > 0 ? Math.max(0, now - discoveredAt) : null,
        lastAttemptAt: row.lastAttemptAt || null,
        lastScannedAt: row.lastScannedAt || null,
        lastSuccessAt: row.lastSuccessAt || null,
        lastIndexedAt: status === "indexed" ? row.lastSuccessAt || null : null,
        lastFailureAt: row.lastFailureAt || null,
        lastScanDurationMs: row.lastScanDurationMs || null,
        tradeRowsLoaded: Number(row.tradeRowsLoaded || 0),
        fundingRowsLoaded: Number(row.fundingRowsLoaded || 0),
        tradePagesLoaded: Number(row.tradePagesLoaded || 0),
        fundingPagesLoaded: Number(row.fundingPagesLoaded || 0),
        tradeHasMore: Boolean(row.tradeHasMore),
        fundingHasMore: Boolean(row.fundingHasMore),
        tradeDone: Boolean(row.tradeDone),
        fundingDone: Boolean(row.fundingDone),
        tradeCursor: row.tradeCursor || null,
        fundingCursor: row.fundingCursor || null,
        lastTradeNextCursor: row.lastTradeNextCursor || null,
        lastFundingNextCursor: row.lastFundingNextCursor || null,
        tradeCacheHits: Number(row.tradeCacheHits || 0),
        fundingCacheHits: Number(row.fundingCacheHits || 0),
        tradeRequests: Number(row.tradeRequests || 0),
        fundingRequests: Number(row.fundingRequests || 0),
        backfillCompletedAt: row.backfillCompletedAt || null,
        liveTrackingSince: row.liveTrackingSince || null,
        liveLastScanAt: row.liveLastScanAt || null,
        scansSucceeded,
        scansFailed,
        consecutiveFailures: Number(row.consecutiveFailures || 0),
        lastErrorReason: lastErrorReason || null,
        lastError: row.lastError || null,
      };
    });

    const filtered = rows
      .filter((row) => {
        if (!statusFilter) return true;
        return row.status === statusFilter || row.lifecycleStage === statusFilter;
      })
      .filter((row) => !lifecycleFilter || row.lifecycleStage === lifecycleFilter)
      .filter((row) => !search || String(row.wallet || "").toLowerCase().includes(search))
      .sort((a, b) => {
        const pri = { failed: 0, pending: 1, partial: 2, indexed: 3 };
        const aPri = Object.prototype.hasOwnProperty.call(pri, a.status) ? pri[a.status] : 9;
        const bPri = Object.prototype.hasOwnProperty.call(pri, b.status) ? pri[b.status] : 9;
        if (aPri !== bPri) return aPri - bPri;
        const aTs = Number(a.lastScannedAt || a.discoveredAt || 0);
        const bTs = Number(b.lastScannedAt || b.discoveredAt || 0);
        return aTs - bTs;
      });

    const total = filtered.length;
    const start = (page - 1) * pageSize;
    const paged = filtered.slice(start, start + pageSize);
    const summary = this.buildDiagnosticsSummary();

    return {
      generatedAt: Date.now(),
      queue: {
        prioritySize: this.runtime.priorityScanQueue.length,
        averageQueueWaitMs: summary.averageQueueWaitMs,
      },
      backlogMode: {
        active: this.runtime.backlogMode,
        reason: this.runtime.backlogReason,
      },
      counts: {
        knownWallets: this.state.knownWallets.length,
        attemptedWallets: summary.attemptedWallets,
        indexed: summary.indexed,
        partial: summary.partial,
        pending: summary.pending,
        failed: summary.failed,
        failedBackfill: summary.failedBackfill,
        backlog: summary.backlog,
        discovered: summary.discovered,
        pendingBackfill: summary.pendingBackfill,
        backfilling: summary.backfilling,
        fullyIndexed: summary.fullyIndexed,
        liveTracking: summary.liveTracking,
        backfillComplete: summary.backfillComplete,
        successfulScans: summary.successfulScans,
        failedScans: summary.failedScans,
        completionPct:
          this.state.knownWallets.length > 0
            ? Number(((summary.backfillComplete / this.state.knownWallets.length) * 100).toFixed(4))
            : 0,
      },
      averagePendingWaitMs: summary.averagePendingWaitMs,
      topErrorReasons: summary.topErrorReasons,
      query: {
        status: statusFilter || null,
        lifecycle: lifecycleFilter || null,
        q: search || null,
        page,
        pageSize,
      },
      total,
      pages: Math.max(1, Math.ceil(total / pageSize)),
      rows: paged,
    };
  }
}

module.exports = {
  ExchangeWalletIndexer,
};
