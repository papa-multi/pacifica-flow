"use strict";

const path = require("path");
const { ensureDir, readJson, writeJsonAtomic } = require("../pipeline/utils");

function toNum(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function positionIdentity(row = {}) {
  const wallet = String(row.wallet || "").trim();
  const key = String(row.positionKey || "").trim();
  if (wallet && key) return `${wallet}|${key}`;
  const symbol = String(row.symbol || "").trim().toUpperCase();
  const side = String(row.rawSide || row.side || "").trim().toLowerCase();
  const isolated = row.isolated ? "iso" : "cross";
  const entry = Number.isFinite(Number(row.entry))
    ? Number(row.entry).toFixed(8)
    : "na";
  return `${wallet}|${symbol}|${side}|${isolated}|${entry}`;
}

function emptyState() {
  return {
    version: 1,
    updatedAt: null,
    lastIngestAt: null,
    lastEventAt: null,
    positions: {},
    wallets: {},
  };
}

class PositionLifecycleStore {
  constructor(options = {}) {
    this.dataDir = options.dataDir || path.join(process.cwd(), "data", "indexer");
    this.filePath =
      options.filePath || path.join(this.dataDir, "position_lifecycle_state.json");
    this.flushIntervalMs = Math.max(1000, Number(options.flushIntervalMs || 15000));
    this.maxBufferedEvents = Math.max(1, Number(options.maxBufferedEvents || 200));
    this.retentionClosedMs = Math.max(
      24 * 60 * 60 * 1000,
      Number(options.retentionClosedMs || 180 * 24 * 60 * 60 * 1000)
    );
    this.logger = options.logger || console;
    this.state = emptyState();
    this.flushTimer = null;
    this.dirty = false;
    this.bufferedEvents = 0;
    this.lastWriteAt = 0;
  }

  load() {
    ensureDir(path.dirname(this.filePath));
    const payload = readJson(this.filePath, null);
    if (payload && typeof payload === "object") {
      this.state = {
        ...emptyState(),
        ...payload,
        positions:
          payload.positions && typeof payload.positions === "object"
            ? payload.positions
            : {},
        wallets:
          payload.wallets && typeof payload.wallets === "object"
            ? payload.wallets
            : {},
      };
    } else {
      this.state = emptyState();
    }
    this.dirty = false;
    this.bufferedEvents = 0;
    return this.getStatus();
  }

  scheduleFlush() {
    if (this.flushTimer) return;
    this.flushTimer = setTimeout(() => {
      this.flushTimer = null;
      this.flush(false);
    }, this.flushIntervalMs);
  }

  flush(force = false) {
    if (!this.dirty && !force) return false;
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    ensureDir(path.dirname(this.filePath));
    this.state.updatedAt = Date.now();
    writeJsonAtomic(this.filePath, this.state);
    this.dirty = false;
    this.bufferedEvents = 0;
    this.lastWriteAt = Date.now();
    return true;
  }

  markDirty(eventCount = 1) {
    this.dirty = true;
    this.bufferedEvents += Math.max(1, Number(eventCount || 1));
    if (
      this.bufferedEvents >= this.maxBufferedEvents ||
      Date.now() - Number(this.lastWriteAt || 0) >= this.flushIntervalMs
    ) {
      this.flush(true);
    } else {
      this.scheduleFlush();
    }
  }

  ensureWalletSummary(wallet, nowMs = Date.now()) {
    const key = String(wallet || "").trim();
    if (!key) return null;
    if (!this.state.wallets[key]) {
      this.state.wallets[key] = {
        wallet: key,
        firstSeenAt: nowMs,
        lastSeenAt: nowMs,
        lastLifecycleEventAt: nowMs,
        lastOpenedAt: null,
        lastClosedAt: null,
        observedPositions: 0,
        openedCount: 0,
        increaseCount: 0,
        reduceCount: 0,
        closeCount: 0,
        currentOpenPositions: 0,
        currentExposureUsd: 0,
        currentAvgOpenAgeMs: null,
        totalObservedHoldingMs: 0,
        completedPositionCount: 0,
        avgCompletedHoldingMs: null,
        avgObservedHoldingMs: null,
      };
    }
    return this.state.wallets[key];
  }

  noteWalletEvent(wallet, eventType, positionRecord, nowMs = Date.now()) {
    const summary = this.ensureWalletSummary(wallet, nowMs);
    if (!summary) return;
    summary.lastSeenAt = nowMs;
    summary.lastLifecycleEventAt = nowMs;
    if (eventType === "opened") {
      summary.openedCount += 1;
      summary.observedPositions += 1;
      summary.lastOpenedAt = nowMs;
    } else if (eventType === "increased") {
      summary.increaseCount += 1;
    } else if (eventType === "reduced") {
      summary.reduceCount += 1;
    } else if (eventType === "closed") {
      summary.closeCount += 1;
      summary.lastClosedAt = nowMs;
      const openedAt = Number(
        (positionRecord &&
          (positionRecord.currentOpenedAt ||
            positionRecord.currentFirstSeenAt ||
            positionRecord.firstSeenAt)) ||
          0
      );
      if (openedAt > 0 && nowMs >= openedAt) {
        const holdingMs = nowMs - openedAt;
        summary.totalObservedHoldingMs += holdingMs;
        summary.completedPositionCount += 1;
        summary.avgCompletedHoldingMs = Number(
          (
            summary.totalObservedHoldingMs / Math.max(1, summary.completedPositionCount)
          ).toFixed(0)
        );
      }
    }
    summary.avgObservedHoldingMs =
      summary.avgCompletedHoldingMs !== null
        ? summary.avgCompletedHoldingMs
        : summary.currentAvgOpenAgeMs;
  }

  normalizeRow(row = {}, nowMs = Date.now()) {
    const openedAt = toNum(row.openedAt || row.createdAt || row.timestamp, 0) || nowMs;
    const updatedAt = toNum(row.updatedAt || row.timestamp || openedAt, openedAt) || nowMs;
    const positionUsd = Math.max(
      0,
      toNum(row.positionUsd !== undefined ? row.positionUsd : row.notionalUsd, 0)
    );
    const size = Math.abs(toNum(row.size !== undefined ? row.size : row.amount, 0));
    return {
      wallet: String(row.wallet || "").trim(),
      positionKey: String(row.positionKey || "").trim(),
      symbol: String(row.symbol || "").trim().toUpperCase(),
      side: String(row.rawSide || row.side || "").trim().toLowerCase(),
      isolated: Boolean(row.isolated),
      openedAt,
      updatedAt,
      positionUsd,
      size,
      freshness: String(row.freshness || row.status || "").trim().toLowerCase() || "unknown",
    };
  }

  ingest({ positions = [], nowMs = Date.now() } = {}) {
    const current = new Map();
    const touchedWallets = new Set();
    let eventCount = 0;

    (Array.isArray(positions) ? positions : []).forEach((row) => {
      const normalized = this.normalizeRow(row, nowMs);
      if (!normalized.wallet) return;
      const id = positionIdentity({
        wallet: normalized.wallet,
        positionKey: normalized.positionKey,
        symbol: normalized.symbol,
        rawSide: normalized.side,
        isolated: normalized.isolated,
        entry: row.entry,
      });
      if (!id) return;
      current.set(id, {
        ...normalized,
        positionKey: normalized.positionKey || id.split("|").slice(1).join("|"),
      });

      const existing = this.state.positions[id];
      if (!existing) {
        this.state.positions[id] = {
          id,
          wallet: normalized.wallet,
          positionKey: normalized.positionKey || id,
          symbol: normalized.symbol,
          side: normalized.side,
          isolated: normalized.isolated,
          firstSeenAt: nowMs,
          lastSeenAt: nowMs,
          firstOpenedAt: normalized.openedAt || nowMs,
          currentOpenedAt: normalized.openedAt || nowMs,
          currentFirstSeenAt: nowMs,
          currentLastSeenAt: nowMs,
          lastUpdatedAt: normalized.updatedAt || nowMs,
          lastKnownPositionUsd: normalized.positionUsd,
          lastKnownSize: normalized.size,
          currentOpen: true,
          openedCount: 1,
          increaseCount: 0,
          reduceCount: 0,
          closeCount: 0,
          totalObservedHoldingMs: 0,
          completedPositionCount: 0,
          lastClosedAt: null,
          lastChangeAt: nowMs,
          lastChangeType: "opened",
          freshness: normalized.freshness,
        };
        touchedWallets.add(normalized.wallet);
        this.noteWalletEvent(normalized.wallet, "opened", this.state.positions[id], nowMs);
        eventCount += 1;
        return;
      }

      if (!existing.currentOpen) {
        existing.currentOpen = true;
        existing.currentOpenedAt = normalized.openedAt || nowMs;
        existing.currentFirstSeenAt = nowMs;
        existing.currentLastSeenAt = nowMs;
        existing.lastUpdatedAt = normalized.updatedAt || nowMs;
        existing.lastKnownPositionUsd = normalized.positionUsd;
        existing.lastKnownSize = normalized.size;
        existing.freshness = normalized.freshness;
        existing.openedCount += 1;
        existing.lastChangeAt = nowMs;
        existing.lastChangeType = "opened";
        touchedWallets.add(normalized.wallet);
        this.noteWalletEvent(normalized.wallet, "opened", existing, nowMs);
        eventCount += 1;
      } else {
        const prevSize = toNum(existing.lastKnownSize, 0);
        const prevUsd = toNum(existing.lastKnownPositionUsd, 0);
        const deltaSize = normalized.size - prevSize;
        const deltaUsd = normalized.positionUsd - prevUsd;
        const changedUp = deltaSize > 1e-9 || deltaUsd > 0.01;
        const changedDown = deltaSize < -1e-9 || deltaUsd < -0.01;
        if (changedUp) {
          existing.increaseCount += 1;
          existing.lastChangeAt = nowMs;
          existing.lastChangeType = "increased";
          touchedWallets.add(normalized.wallet);
          this.noteWalletEvent(normalized.wallet, "increased", existing, nowMs);
          eventCount += 1;
        } else if (changedDown) {
          existing.reduceCount += 1;
          existing.lastChangeAt = nowMs;
          existing.lastChangeType = "reduced";
          touchedWallets.add(normalized.wallet);
          this.noteWalletEvent(normalized.wallet, "reduced", existing, nowMs);
          eventCount += 1;
        }
        existing.currentLastSeenAt = nowMs;
        existing.lastUpdatedAt = normalized.updatedAt || nowMs;
        existing.lastKnownPositionUsd = normalized.positionUsd;
        existing.lastKnownSize = normalized.size;
        existing.freshness = normalized.freshness;
      }
      existing.lastSeenAt = nowMs;
    });

    Object.values(this.state.positions).forEach((record) => {
      if (!record || !record.currentOpen) return;
      if (current.has(record.id)) return;
      record.currentOpen = false;
      record.lastSeenAt = nowMs;
      record.lastClosedAt = nowMs;
      record.lastChangeAt = nowMs;
      record.lastChangeType = "closed";
      record.closeCount = toNum(record.closeCount, 0) + 1;
      const openedAt = toNum(
        record.currentOpenedAt || record.currentFirstSeenAt || record.firstSeenAt,
        0
      );
      if (openedAt > 0 && nowMs >= openedAt) {
        const holdingMs = nowMs - openedAt;
        record.totalObservedHoldingMs = toNum(record.totalObservedHoldingMs, 0) + holdingMs;
        record.completedPositionCount = toNum(record.completedPositionCount, 0) + 1;
      }
      touchedWallets.add(record.wallet);
      this.noteWalletEvent(record.wallet, "closed", record, nowMs);
      eventCount += 1;
    });

    const exposureByWallet = new Map();
    const ageTotalsByWallet = new Map();
    current.forEach((row) => {
      const wallet = row.wallet;
      if (!wallet) return;
      const exposure = exposureByWallet.get(wallet) || {
        currentOpenPositions: 0,
        currentExposureUsd: 0,
        totalOpenAgeMs: 0,
        openAgeSamples: 0,
      };
      exposure.currentOpenPositions += 1;
      exposure.currentExposureUsd += row.positionUsd;
      const ageMs = Math.max(0, nowMs - Math.max(0, row.openedAt || nowMs));
      exposure.totalOpenAgeMs += ageMs;
      exposure.openAgeSamples += 1;
      exposureByWallet.set(wallet, exposure);
      ageTotalsByWallet.set(wallet, exposure);
    });

    Object.values(this.state.wallets).forEach((summary) => {
      const live = exposureByWallet.get(summary.wallet) || {
        currentOpenPositions: 0,
        currentExposureUsd: 0,
        totalOpenAgeMs: 0,
        openAgeSamples: 0,
      };
      summary.currentOpenPositions = live.currentOpenPositions;
      summary.currentExposureUsd = Number(live.currentExposureUsd.toFixed(2));
      summary.currentAvgOpenAgeMs =
        live.openAgeSamples > 0
          ? Number((live.totalOpenAgeMs / live.openAgeSamples).toFixed(0))
          : null;
      summary.avgObservedHoldingMs =
        summary.avgCompletedHoldingMs !== null
          ? summary.avgCompletedHoldingMs
          : summary.currentAvgOpenAgeMs;
      if (live.currentOpenPositions > 0 || touchedWallets.has(summary.wallet)) {
        summary.lastSeenAt = nowMs;
      }
    });

    this.prune(nowMs);
    this.state.lastIngestAt = nowMs;
    if (eventCount > 0) this.state.lastEventAt = nowMs;
    if (eventCount > 0) this.markDirty(eventCount);
    return this.getStatus();
  }

  prune(nowMs = Date.now()) {
    const cutoff = nowMs - this.retentionClosedMs;
    let removed = 0;
    Object.entries(this.state.positions).forEach(([key, record]) => {
      if (!record || record.currentOpen) return;
      const closedAt = toNum(record.lastClosedAt, 0);
      if (closedAt > 0 && closedAt < cutoff) {
        delete this.state.positions[key];
        removed += 1;
      }
    });
    if (removed > 0) {
      this.markDirty(removed);
    }
    return removed;
  }

  getWalletSummary(wallet) {
    const key = String(wallet || "").trim();
    if (!key) return null;
    const summary = this.state.wallets[key];
    if (!summary) return null;
    return {
      wallet: key,
      firstSeenAt: toNum(summary.firstSeenAt, 0) || null,
      lastSeenAt: toNum(summary.lastSeenAt, 0) || null,
      lastLifecycleEventAt: toNum(summary.lastLifecycleEventAt, 0) || null,
      lastOpenedAt: toNum(summary.lastOpenedAt, 0) || null,
      lastClosedAt: toNum(summary.lastClosedAt, 0) || null,
      observedPositions: toNum(summary.observedPositions, 0),
      openedCount: toNum(summary.openedCount, 0),
      increaseCount: toNum(summary.increaseCount, 0),
      reduceCount: toNum(summary.reduceCount, 0),
      closeCount: toNum(summary.closeCount, 0),
      currentOpenPositions: toNum(summary.currentOpenPositions, 0),
      currentExposureUsd: Number(toNum(summary.currentExposureUsd, 0).toFixed(2)),
      currentAvgOpenAgeMs: toNum(summary.currentAvgOpenAgeMs, 0) || null,
      totalObservedHoldingMs: toNum(summary.totalObservedHoldingMs, 0),
      completedPositionCount: toNum(summary.completedPositionCount, 0),
      avgCompletedHoldingMs: toNum(summary.avgCompletedHoldingMs, 0) || null,
      avgObservedHoldingMs: toNum(summary.avgObservedHoldingMs, 0) || null,
    };
  }

  getPositionSummary(wallet, positionKey, row = null, nowMs = Date.now()) {
    const id = positionIdentity({
      wallet,
      positionKey,
      symbol: row && row.symbol,
      rawSide: row && (row.rawSide || row.side),
      isolated: row && row.isolated,
      entry: row && row.entry,
    });
    const record = this.state.positions[id];
    if (!record) {
      return null;
    }
    const currentOpenedAt = toNum(record.currentOpenedAt || record.currentFirstSeenAt, 0);
    const currentAgeMs = record.currentOpen && currentOpenedAt > 0 ? Math.max(0, nowMs - currentOpenedAt) : null;
    return {
      wallet: String(record.wallet || "").trim(),
      positionKey: String(record.positionKey || "").trim(),
      firstSeenAt: toNum(record.firstSeenAt, 0) || null,
      lastSeenAt: toNum(record.lastSeenAt, 0) || null,
      lastUpdatedAt: toNum(record.lastUpdatedAt, 0) || null,
      currentOpen: Boolean(record.currentOpen),
      openedCount: toNum(record.openedCount, 0),
      increaseCount: toNum(record.increaseCount, 0),
      reduceCount: toNum(record.reduceCount, 0),
      closeCount: toNum(record.closeCount, 0),
      currentAgeMs,
      completedPositionCount: toNum(record.completedPositionCount, 0),
      avgCompletedHoldingMs:
        toNum(record.completedPositionCount, 0) > 0
          ? Number(
              (
                toNum(record.totalObservedHoldingMs, 0) /
                Math.max(1, toNum(record.completedPositionCount, 0))
              ).toFixed(0)
            )
          : null,
      lastClosedAt: toNum(record.lastClosedAt, 0) || null,
      lastChangeAt: toNum(record.lastChangeAt, 0) || null,
      lastChangeType: String(record.lastChangeType || "").trim() || null,
    };
  }

  getStatus() {
    const positionRecords = Object.values(this.state.positions || {});
    const walletRecords = Object.values(this.state.wallets || {});
    const activePositions = positionRecords.filter((row) => row && row.currentOpen).length;
    const completedPositions = positionRecords.reduce(
      (sum, row) => sum + toNum(row && row.completedPositionCount, 0),
      0
    );
    return {
      enabled: true,
      lastIngestAt: toNum(this.state.lastIngestAt, 0) || null,
      lastEventAt: toNum(this.state.lastEventAt, 0) || null,
      trackedWallets: walletRecords.length,
      trackedPositions: positionRecords.length,
      activePositions,
      completedPositions,
      walletsWithCompletedPositions: walletRecords.filter(
        (row) => toNum(row && row.completedPositionCount, 0) > 0
      ).length,
    };
  }

  stop() {
    this.flush(true);
  }
}

module.exports = {
  PositionLifecycleStore,
};
