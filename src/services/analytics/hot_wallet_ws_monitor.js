const { createWsClient } = require("../transport/ws_client");

function normalizeWallet(value) {
  const text = String(value || "").trim();
  return text || "";
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function reasonPriority(reason = "") {
  const text = String(reason || "").trim().toLowerCase();
  if (!text) return 1;
  if (text.includes("open") || text.includes("position")) return 6;
  if (text.includes("trade") || text.includes("order")) return 5;
  if (text.includes("volume") || text.includes("high_value") || text.includes("active_rank")) {
    return 4;
  }
  if (text.includes("solana") || text.includes("trigger") || text.includes("public_trade")) return 4;
  if (text.includes("recent")) return 3;
  if (text.includes("reconcile")) return 2;
  return 1;
}

class HotWalletWsMonitor {
  constructor(options = {}) {
    this.enabled = Boolean(options.enabled);
    this.wsUrl = String(options.wsUrl || "").trim();
    this.logger = options.logger || console;
    this.capacityCeiling = Math.max(1, Number(options.maxWallets || 32));
    this.initialWallets = clamp(
      Number(options.initialWallets || Math.min(this.capacityCeiling, Math.max(8, this.capacityCeiling))),
      1,
      this.capacityCeiling
    );
    this.maxWallets = this.initialWallets;
    this.capacityStep = Math.max(1, Number(options.capacityStep || 4));
    this.soakWindowMs = Math.max(30000, Number(options.soakWindowMs || 3 * 60 * 1000));
    this.maxScaleErrors = Math.max(0, Number(options.maxScaleErrors || 0));
    this.maxScaleReconnects = Math.max(0, Number(options.maxScaleReconnects || 1));
    this.minScaleUtilizationPct = clamp(
      Number(options.minScaleUtilizationPct || 92),
      1,
      100
    );
    this.minScaleBacklog = Math.max(0, Number(options.minScaleBacklog || 1));
    this.memoryCeilingMb = Math.max(0, Number(options.memoryCeilingMb || 0));
    this.inactivityMs = Math.max(15000, Number(options.inactivityMs || 2 * 60 * 1000));
    this.openPositionHoldMs = Math.max(
      this.inactivityMs,
      Number(options.openPositionHoldMs || 5 * 60 * 1000)
    );
    this.tradeHoldMs = Math.max(15000, Number(options.tradeHoldMs || 60 * 1000));
    this.aggressiveEvictMs = Math.max(10000, Number(options.aggressiveEvictMs || 30 * 1000));
    this.tickMs = Math.max(1000, Number(options.tickMs || 5000));
    this.reconnectMs = Math.max(500, Number(options.reconnectMs || 2000));
    this.pingIntervalMs = Math.max(5000, Number(options.pingIntervalMs || 25000));
    this.onPositions = typeof options.onPositions === "function" ? options.onPositions : null;
    this.onTrades = typeof options.onTrades === "function" ? options.onTrades : null;
    this.onOrderUpdates =
      typeof options.onOrderUpdates === "function" ? options.onOrderUpdates : null;

    this.wallets = new Map();
    this.timer = null;
    this.triggerToEventTotalMs = 0;
    this.scaleBaseline = {
      errors: 0,
      reconnects: 0,
      droppedPromotions: 0,
      at: Date.now(),
    };
    this.status = {
      enabled: this.enabled && Boolean(this.wsUrl),
      activeWallets: 0,
      openConnections: 0,
      connectingConnections: 0,
      walletsWithOpenPositions: 0,
      droppedPromotions: 0,
      promotionCount: 0,
      demotionCount: 0,
      evictionCount: 0,
      reconnectTransitions: 0,
      errorCount: 0,
      triggerToEventSamples: 0,
      triggerToEventAvgMs: null,
      lastTriggerToEventMs: null,
      lastEventAt: null,
      lastPromotionAt: null,
      lastDemotionAt: null,
      lastErrorAt: null,
      lastError: null,
      maxWallets: this.maxWallets,
      capacityCeiling: this.capacityCeiling,
      availableSlots: Math.max(0, this.maxWallets),
      promotionBacklog: 0,
      utilizationPct: 0,
      lastScaleAt: null,
      scaleEvents: 0,
      stableSinceAt: Date.now(),
      processRssMb: 0,
      hotWallets: 0,
      warmWallets: 0,
      coldWallets: 0,
    };
  }

  start() {
    if (!this.status.enabled || this.timer) return;
    this.timer = setInterval(() => {
      this.pruneInactiveWallets();
      this.evaluateCapacity();
      this.refreshStatus();
    }, this.tickMs);
    if (this.timer && typeof this.timer.unref === "function") this.timer.unref();
    this.refreshStatus();
  }

  stop() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    Array.from(this.wallets.keys()).forEach((wallet) => this.demoteWallet(wallet, "stop"));
    this.refreshStatus();
  }

  classifyPromotion(reason = "", meta = {}, now = Date.now()) {
    const text = String(reason || "").trim().toLowerCase();
    if (meta.openPositions === true) return "hot";
    if (text.includes("open") || text.includes("position")) return "hot";
    if (text.includes("trade") || text.includes("order")) return "warm";
    if (text.includes("volume") || text.includes("high_value") || text.includes("active_rank")) {
      return "warm";
    }
    if (text.includes("solana") || text.includes("trigger") || text.includes("public_trade")) {
      return "warm";
    }
    const at = Math.max(0, Number(meta.at || now));
    if (at > 0 && now - at <= this.tradeHoldMs) return "warm";
    return "cold";
  }

  promoteWallet(wallet, meta = {}) {
    const normalized = normalizeWallet(wallet);
    if (!this.status.enabled || !normalized) return false;
    const now = Math.max(0, Number(meta.at || Date.now()));
    const reason = String(meta.reason || "activity").trim() || "activity";
    let record = this.wallets.get(normalized);
    if (!record) {
      if (this.wallets.size >= this.maxWallets) {
        const evicted = this.evictOneFor(normalized, now);
        if (!evicted) {
          this.status.droppedPromotions += 1;
          this.refreshStatus();
          return false;
        }
      }
      record = this.createWalletRecord(normalized);
      this.wallets.set(normalized, record);
      this.attachClient(record);
    }
    const nextTier = this.classifyPromotion(reason, meta, now);
    this.applyPromotionMeta(record, meta, now, reason, nextTier);
    this.status.promotionCount += 1;
    this.status.lastPromotionAt = now;
    this.evaluateCapacity(now);
    this.refreshStatus();
    return true;
  }

  createWalletRecord(wallet) {
    return {
      wallet,
      client: null,
      status: "idle",
      promotedAt: 0,
      lastActivityAt: 0,
      lastWsEventAt: 0,
      lastPositionsAt: 0,
      lastTradeAt: 0,
      lastOrderAt: 0,
      lastTriggerAt: 0,
      lastOpenPositionAt: 0,
      lastReason: "activity",
      lastErrorAt: 0,
      lastError: null,
      openPositionsCount: 0,
      recentVolumeUsd: 0,
      recentTradeCount: 0,
      liveActiveRank: 0,
      priorityBoost: 0,
      pinUntil: 0,
      triggeredAt: 0,
      awaitingTriggerLatency: false,
      tier: "cold",
      priorityScore: 0,
    };
  }

  applyPromotionMeta(record, meta, now, reason, nextTier) {
    record.promotedAt = Math.max(record.promotedAt, now);
    record.lastActivityAt = Math.max(record.lastActivityAt, now);
    record.lastTriggerAt = Math.max(record.lastTriggerAt, now);
    record.triggeredAt = now;
    record.awaitingTriggerLatency = true;
    record.lastReason = reason;
    record.tier = meta.forceTier || nextTier || record.tier || "cold";
    if (meta.recentVolumeUsd !== undefined) {
      record.recentVolumeUsd = Math.max(
        Number(record.recentVolumeUsd || 0),
        Math.max(0, Number(meta.recentVolumeUsd || 0))
      );
    }
    if (meta.recentTradeCount !== undefined) {
      record.recentTradeCount = Math.max(
        Number(record.recentTradeCount || 0),
        Math.max(0, Number(meta.recentTradeCount || 0))
      );
    }
    if (meta.liveActiveRank !== undefined) {
      const nextRank = Math.max(0, Number(meta.liveActiveRank || 0));
      if (nextRank > 0) {
        record.liveActiveRank =
          Number(record.liveActiveRank || 0) > 0
            ? Math.min(Number(record.liveActiveRank || 0), nextRank)
            : nextRank;
      }
    }
    if (meta.priorityBoost !== undefined) {
      record.priorityBoost = Math.max(
        Number(record.priorityBoost || 0),
        Math.max(0, Number(meta.priorityBoost || 0))
      );
    }
    if (meta.openPositions === true) {
      record.openPositionsCount = Math.max(1, Number(record.openPositionsCount || 0));
      record.lastOpenPositionAt = Math.max(record.lastOpenPositionAt, now);
      record.pinUntil = Math.max(record.pinUntil, now + this.openPositionHoldMs);
      record.tier = "hot";
    } else if (reason.includes("trade") || reason.includes("order")) {
      record.pinUntil = Math.max(record.pinUntil, now + this.tradeHoldMs);
    }
    if (meta.pinMs !== undefined) {
      record.pinUntil = Math.max(
        record.pinUntil,
        now + Math.max(0, Number(meta.pinMs || 0))
      );
    }
    if (record.tier === "warm") {
      record.pinUntil = Math.max(record.pinUntil, now + this.tradeHoldMs);
    }
    record.priorityScore = this.computePriorityScore(record, now);
  }

  attachClient(record) {
    const wallet = record.wallet;
    const client = createWsClient({
      url: this.wsUrl,
      logger: this.logger,
      reconnectMs: this.reconnectMs,
      pingIntervalMs: this.pingIntervalMs,
    });
    record.client = client;
    client.setHandlers({
      onStatus: (status) => {
        const previous = record.status;
        record.status = status;
        if (previous === "open" && status !== "open") {
          this.status.reconnectTransitions += 1;
        }
        if (status === "open") {
          client.subscribe({ source: "account_positions", account: wallet });
          client.subscribe({ source: "account_trades", account: wallet });
          client.subscribe({ source: "account_order_updates", account: wallet });
        }
        this.refreshStatus();
      },
      onMessage: (payload) => {
        this.handleWalletMessage(record, payload);
      },
      onError: (error) => {
        record.lastErrorAt = Date.now();
        record.lastError = String(error && error.message ? error.message : error || "ws_error");
        this.status.errorCount += 1;
        this.status.lastErrorAt = record.lastErrorAt;
        this.status.lastError = record.lastError;
        this.refreshStatus();
      },
    });
    client.start();
  }

  handleWalletMessage(record, payload) {
    if (!payload || typeof payload !== "object" || !payload.channel) return;
    const wallet = record.wallet;
    const now = Date.now();
    record.lastActivityAt = now;
    record.lastWsEventAt = now;
    this.status.lastEventAt = now;
    if (record.awaitingTriggerLatency) {
      const baseAt = Math.max(0, Number(record.triggeredAt || record.promotedAt || 0));
      if (baseAt > 0) {
        const latencyMs = Math.max(0, now - baseAt);
        this.triggerToEventTotalMs += latencyMs;
        this.status.triggerToEventSamples += 1;
        this.status.lastTriggerToEventMs = latencyMs;
        this.status.triggerToEventAvgMs = Math.round(
          this.triggerToEventTotalMs / Math.max(1, this.status.triggerToEventSamples)
        );
      }
      record.awaitingTriggerLatency = false;
    }

    if (payload.channel === "account_positions") {
      const rows = Array.isArray(payload.data) ? payload.data : [];
      record.lastPositionsAt = now;
      record.openPositionsCount = rows.length;
      if (rows.length > 0) {
        record.lastOpenPositionAt = now;
        record.tier = "hot";
      }
      record.pinUntil = Math.max(
        record.pinUntil,
        now + (rows.length > 0 ? this.openPositionHoldMs : this.tradeHoldMs)
      );
      record.priorityScore = this.computePriorityScore(record, now);
      if (this.onPositions) {
        this.onPositions(wallet, rows, {
          at: now,
          source: "ws.account_positions",
          li: payload.li || null,
        });
      }
      this.refreshStatus();
      return;
    }

    if (payload.channel === "account_trades") {
      record.lastTradeAt = now;
      record.tier = Number(record.openPositionsCount || 0) > 0 ? "hot" : "warm";
      record.pinUntil = Math.max(record.pinUntil, now + this.tradeHoldMs);
      record.priorityScore = this.computePriorityScore(record, now);
      if (this.onTrades) {
        this.onTrades(wallet, Array.isArray(payload.data) ? payload.data : [], {
          at: now,
          source: "ws.account_trades",
          li: payload.li || null,
        });
      }
      this.refreshStatus();
      return;
    }

    if (payload.channel === "account_order_updates") {
      record.lastOrderAt = now;
      record.tier = Number(record.openPositionsCount || 0) > 0 ? "hot" : "warm";
      record.pinUntil = Math.max(record.pinUntil, now + this.tradeHoldMs);
      record.priorityScore = this.computePriorityScore(record, now);
      if (this.onOrderUpdates) {
        this.onOrderUpdates(wallet, Array.isArray(payload.data) ? payload.data : [], {
          at: now,
          source: "ws.account_order_updates",
          li: payload.li || null,
        });
      }
      this.refreshStatus();
    }
  }

  computePriorityScore(record, now = Date.now()) {
    const lastTouch = Math.max(
      Number(record.lastWsEventAt || 0),
      Number(record.lastPositionsAt || 0),
      Number(record.lastTradeAt || 0),
      Number(record.lastOrderAt || 0),
      Number(record.lastTriggerAt || 0),
      Number(record.promotedAt || 0)
    );
    const agePenalty = Math.max(0, Math.floor((now - lastTouch) / 1000));
    const openBonus = Number(record.openPositionsCount || 0) > 0 ? 1_000_000 : 0;
    const pinBonus = Number(record.pinUntil || 0) > now ? 100_000 : 0;
    const tierBonus = record.tier === "hot" ? 50_000 : record.tier === "warm" ? 10_000 : 0;
    const reasonBonus = reasonPriority(record.lastReason) * 2_500;
    const recentVolumeUsd = Math.max(0, Number(record.recentVolumeUsd || 0));
    const recentTradeCount = Math.max(0, Number(record.recentTradeCount || 0));
    const volumeBonus = Math.round(Math.min(120_000, Math.sqrt(recentVolumeUsd) * 150));
    const tradeBonus = Math.min(30_000, recentTradeCount * 400);
    const liveRank = Math.max(0, Number(record.liveActiveRank || 0));
    const liveRankBonus = liveRank > 0 ? Math.max(0, 40_000 - liveRank * 250) : 0;
    const priorityBoost = Math.max(0, Number(record.priorityBoost || 0));
    return (
      openBonus +
      pinBonus +
      tierBonus +
      reasonBonus +
      volumeBonus +
      tradeBonus +
      liveRankBonus +
      priorityBoost -
      agePenalty
    );
  }

  computeEvictionScore(record, now = Date.now()) {
    const pinned =
      Number(record.openPositionsCount || 0) > 0 || Number(record.pinUntil || 0) > now;
    if (pinned) return Number.POSITIVE_INFINITY;
    const lastTouch = Math.max(
      Number(record.lastWsEventAt || 0),
      Number(record.lastActivityAt || 0),
      Number(record.promotedAt || 0),
      Number(record.lastPositionsAt || 0),
      Number(record.lastTradeAt || 0),
      Number(record.lastOrderAt || 0)
    );
    const recentProtection = lastTouch > 0 && now - lastTouch < this.aggressiveEvictMs ? 10_000 : 0;
    const tierPenalty = record.tier === "warm" ? 5_000 : record.tier === "hot" ? 50_000 : 0;
    const priority = Math.max(0, Number(record.priorityScore || 0));
    return priority + recentProtection + tierPenalty;
  }

  evictOneFor(_wallet, now = Date.now()) {
    let candidate = null;
    for (const record of this.wallets.values()) {
      const score = this.computeEvictionScore(record, now);
      if (!Number.isFinite(score)) continue;
      if (!candidate || score < candidate.score) {
        candidate = {
          wallet: record.wallet,
          score,
        };
      }
    }
    if (!candidate) return false;
    this.status.evictionCount += 1;
    this.demoteWallet(candidate.wallet, "capacity");
    return true;
  }

  demoteWallet(wallet, reason = "inactive") {
    const normalized = normalizeWallet(wallet);
    const record = this.wallets.get(normalized);
    if (!record) return false;
    if (record.client && typeof record.client.stop === "function") {
      try {
        record.client.stop();
      } catch (_error) {
        // ignore
      }
    }
    this.wallets.delete(normalized);
    this.status.demotionCount += 1;
    this.status.lastDemotionAt = Date.now();
    if (reason === "inactive" || reason === "capacity") {
      this.status.lastError = this.status.lastError;
    }
    this.refreshStatus();
    return true;
  }

  pruneInactiveWallets(now = Date.now()) {
    for (const record of this.wallets.values()) {
      if (Number(record.openPositionsCount || 0) > 0) {
        record.tier = "hot";
        continue;
      }
      const lastTouch = Math.max(
        Number(record.lastWsEventAt || 0),
        Number(record.lastActivityAt || 0),
        Number(record.promotedAt || 0),
        Number(record.lastPositionsAt || 0),
        Number(record.lastTradeAt || 0),
        Number(record.lastOrderAt || 0)
      );
      if (Number(record.pinUntil || 0) > now) {
        record.tier = "warm";
        continue;
      }
      if (lastTouch > 0 && now - lastTouch <= this.tradeHoldMs) {
        record.tier = "warm";
        continue;
      }
      record.tier = "cold";
      if (lastTouch > 0 && now - lastTouch <= this.inactivityMs) continue;
      this.demoteWallet(record.wallet, "inactive");
    }
  }

  evaluateCapacity(now = Date.now()) {
    const rssMb = Math.round((process.memoryUsage().rss || 0) / (1024 * 1024));
    this.status.processRssMb = rssMb;
    if (this.maxWallets >= this.capacityCeiling) return;
    const sinceScale = now - Number(this.scaleBaseline.at || 0);
    if (sinceScale < this.soakWindowMs) return;
    const utilizationPct = this.maxWallets > 0 ? (this.wallets.size / this.maxWallets) * 100 : 0;
    const errorDelta = this.status.errorCount - Number(this.scaleBaseline.errors || 0);
    const reconnectDelta =
      this.status.reconnectTransitions - Number(this.scaleBaseline.reconnects || 0);
    const droppedDelta =
      this.status.droppedPromotions - Number(this.scaleBaseline.droppedPromotions || 0);
    if (utilizationPct < this.minScaleUtilizationPct) return;
    if (droppedDelta < this.minScaleBacklog) return;
    if (errorDelta > this.maxScaleErrors) return;
    if (reconnectDelta > this.maxScaleReconnects) return;
    if (this.memoryCeilingMb > 0 && rssMb >= this.memoryCeilingMb) return;
    this.maxWallets = Math.min(this.capacityCeiling, this.maxWallets + this.capacityStep);
    this.status.maxWallets = this.maxWallets;
    this.status.lastScaleAt = now;
    this.status.scaleEvents = Number(this.status.scaleEvents || 0) + 1;
    this.scaleBaseline = {
      errors: this.status.errorCount,
      reconnects: this.status.reconnectTransitions,
      droppedPromotions: this.status.droppedPromotions,
      at: now,
    };
  }

  refreshStatus() {
    const records = Array.from(this.wallets.values());
    this.status.activeWallets = records.length;
    this.status.openConnections = records.filter((row) => row.status === "open").length;
    this.status.connectingConnections = records.filter(
      (row) => row.status === "connecting"
    ).length;
    this.status.walletsWithOpenPositions = records.filter(
      (row) => Number(row.openPositionsCount || 0) > 0
    ).length;
    this.status.hotWallets = records.filter((row) => row.tier === "hot").length;
    this.status.warmWallets = records.filter((row) => row.tier === "warm").length;
    this.status.coldWallets = Math.max(
      0,
      records.length - this.status.hotWallets - this.status.warmWallets
    );
    this.status.utilizationPct = Number(
      ((records.length / Math.max(1, this.maxWallets)) * 100).toFixed(2)
    );
    this.status.availableSlots = Math.max(0, this.maxWallets - records.length);
    this.status.promotionBacklog = Math.max(
      0,
      Number(this.status.droppedPromotions || 0) - Number(this.scaleBaseline.droppedPromotions || 0)
    );
    this.status.maxWallets = this.maxWallets;
    this.status.capacityCeiling = this.capacityCeiling;
    this.status.processRssMb = Math.round((process.memoryUsage().rss || 0) / (1024 * 1024));
  }

  getStatus() {
    this.refreshStatus();
    return {
      ...this.status,
    };
  }
}

module.exports = {
  HotWalletWsMonitor,
};
