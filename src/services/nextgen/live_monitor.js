"use strict";

const fs = require("fs");
const path = require("path");
const config = require("./common/config");
const { readJson, writeJsonAtomic } = require("./core/json-file");

const LIVE_POSITIONS_DIR = path.join(config.rootDir, "data", "live_positions");
const MONITOR_PATH = path.join(config.stateDir, "wallet-live-monitor.json");
const MONITOR_SCHEMA_VERSION = 5;

const HEALTHY_MAX_AGE_MS = 15_000;
const DEGRADED_MAX_AGE_MS = 60_000;

function createWalletSnapshot(wallet) {
  return {
    wallet,
    positions: {},
    openPositions: 0,
    materializedPositions: 0,
    lastEventReceivedAt: 0,
    lastPositionSnapshotAt: 0,
    lastKnownPositionChangeAt: 0,
    accountHintAt: 0,
    accountHintReason: "",
    accountHintMissCount: 0,
    hasSnapshotGap: false,
    positionSources: new Set(),
  };
}

function listFiles(pattern) {
  try {
    return fs
      .readdirSync(LIVE_POSITIONS_DIR)
      .filter((name) => pattern.test(name))
      .map((name) => path.join(LIVE_POSITIONS_DIR, name))
      .sort();
  } catch (_) {
    return [];
  }
}

function asNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function normalizeWallet(value) {
  return String(value || "").trim();
}

function positionSignature(row) {
  const isolated = Boolean(
    row && row.isolated !== undefined
      ? row.isolated
      : row && row.raw && row.raw.isolated !== undefined
      ? row.raw.isolated
      : false
  );
  return {
    key: `${String(row.symbol || "").toUpperCase()}|${String(
      row.rawSide || row.side || ""
    ).toLowerCase()}|${isolated ? "isolated" : "cross"}`,
    size: asNumber(row.size, 0),
    symbol: String(row.symbol || "").toUpperCase(),
    side: String(row.rawSide || row.side || "").toLowerCase(),
    entry: asNumber(row.entry, 0),
    openedAt: asNumber(row.openedAt, 0),
    stateChangedAt: Math.max(
      asNumber(row.lastStateChangeAt, 0),
      asNumber(row.lastUpdatedAt, 0),
      asNumber(row.updatedAt, 0)
    ),
    observedAt: Math.max(
      asNumber(row.lastObservedAt, 0),
      asNumber(row.observedAt, 0),
      asNumber(row.timestamp, 0)
    ),
  };
}

function loadCurrentSnapshot() {
  const wallets = new Map();
  let generatedAt = 0;

  const positionFiles = listFiles(/^wallet_first_shard_\d+\.json$/);
  const accountFiles = listFiles(/^wallet_first_account_shard_\d+\.json$/);

  for (const filePath of positionFiles) {
    const payload = readJson(filePath, {});
    const fileGeneratedAt = Math.max(asNumber(payload.generatedAt, 0), asNumber(fs.statSync(filePath).mtimeMs, 0));
    generatedAt = Math.max(generatedAt, fileGeneratedAt);
    const positions = Array.isArray(payload.positions) ? payload.positions : [];
    for (const row of positions) {
      const wallet = normalizeWallet(row && row.wallet);
      if (!wallet) continue;
      const current = wallets.get(wallet) || createWalletSnapshot(wallet);
      const sig = positionSignature(row || {});
      current.positions[sig.key] = sig;
      current.materializedPositions = Object.keys(current.positions).length;
      current.openPositions = Math.max(current.openPositions, current.materializedPositions);
      current.lastEventReceivedAt = Math.max(
        current.lastEventReceivedAt,
        sig.observedAt,
        sig.stateChangedAt,
        fileGeneratedAt
      );
      current.lastPositionSnapshotAt = Math.max(current.lastPositionSnapshotAt, sig.observedAt, fileGeneratedAt);
      current.lastKnownPositionChangeAt = Math.max(
        current.lastKnownPositionChangeAt,
        sig.stateChangedAt,
        sig.openedAt
      );
      current.positionSources.add(String(row.source || "wallet_first_positions"));
      wallets.set(wallet, current);
    }
  }

  for (const filePath of accountFiles) {
    const payload = readJson(filePath, {});
    const fileGeneratedAt = Math.max(asNumber(payload.generatedAt, 0), asNumber(fs.statSync(filePath).mtimeMs, 0));
    generatedAt = Math.max(generatedAt, fileGeneratedAt);
    const hints = Array.isArray(payload.accountOpenHints) ? payload.accountOpenHints : [];
    for (const row of hints) {
      const wallet = normalizeWallet(row && row.wallet);
      if (!wallet) continue;
      const current = wallets.get(wallet) || createWalletSnapshot(wallet);
      current.openPositions = Math.max(current.openPositions, Math.max(0, asNumber(row.positionsCount, 0)));
      current.materializedPositions = Math.max(
        current.materializedPositions,
        Math.max(0, asNumber(row.materializedPositions, 0))
      );
      current.accountHintAt = Math.max(
        current.accountHintAt,
        asNumber(row.lastAccountAt, 0),
        asNumber(row.updatedAt, 0),
        fileGeneratedAt
      );
      current.accountHintReason = String(row.lastReason || current.accountHintReason || "");
      current.accountHintMissCount = Math.max(
        current.accountHintMissCount,
        Math.max(0, asNumber(row.missCount, 0))
      );
      current.hasSnapshotGap =
        current.hasSnapshotGap ||
        current.accountHintMissCount > 0 ||
        /gap/i.test(current.accountHintReason);
      current.lastEventReceivedAt = Math.max(current.lastEventReceivedAt, current.accountHintAt);
      current.lastPositionSnapshotAt = Math.max(current.lastPositionSnapshotAt, current.accountHintAt);
      current.positionSources.add("account_open_hint");
      wallets.set(wallet, current);
    }
  }

  for (const current of wallets.values()) {
    current.materializedPositions = Math.max(
      current.materializedPositions,
      Object.keys(current.positions || {}).length
    );
    const hasPositionSet = current.materializedPositions > 0;
    current.hasReliablePositionSet =
      !current.hasSnapshotGap &&
      (
        (hasPositionSet && current.materializedPositions === current.openPositions) ||
        (!current.openPositions && !hasPositionSet)
      );
    current.snapshotConsistency = current.hasSnapshotGap
      ? "gap"
      : current.materializedPositions < current.openPositions
      ? "partial"
      : current.hasReliablePositionSet
      ? "reliable"
      : "unknown";
  }

  return { generatedAt, wallets };
}

function diffWallet(prev = {}, current = {}, generatedAt = 0) {
  const prevPositions = prev.positionKeys && typeof prev.positionKeys === "object" ? prev.positionKeys : {};
  const currentPositions = current.positions && typeof current.positions === "object" ? current.positions : {};
  const prevKeys = new Set(Object.keys(prevPositions));
  const currentKeys = new Set(Object.keys(currentPositions));

  let changeAt = 0;
  let changeType = "";
  const prevConfidence = String(prev.lastPositionActivityConfidence || "");
  const prevTrusted = prevConfidence === "high" || prevConfidence === "medium";
  let confidence = prevTrusted ? prevConfidence : "";

  const currentCount = Math.max(0, asNumber(current.openPositions, 0));
  const prevCount = Math.max(0, asNumber(prev.openPositions, 0));
  const currentReliable = Boolean(current.hasReliablePositionSet);
  const prevReliable = Boolean(prev.hasReliablePositionSet);
  const currentOpenedAt = currentCount > 0
    ? Math.max(
        0,
        ...Object.values(currentPositions).map((row) => asNumber(row && row.openedAt, 0))
      )
    : 0;

  if (currentReliable) {
    for (const key of currentKeys) {
      const next = currentPositions[key];
      const prior = prevPositions[key];
      if (!prior) {
        if (!prevReliable) continue;
        const openedAt = asNumber(next.openedAt, 0);
        if (openedAt > 0) {
          changeAt = Math.max(changeAt, openedAt, asNumber(next.stateChangedAt, 0));
          changeType = prevCount > 0 ? "position_added" : "position_opened";
          confidence = "high";
        }
        continue;
      }
      if (asNumber(prior.size, 0) !== asNumber(next.size, 0)) {
        changeAt = Math.max(changeAt, asNumber(next.stateChangedAt, 0));
        changeType = currentCount >= prevCount ? "position_resized" : "position_reduced";
        confidence = "high";
      }
    }
  }

  if (prevReliable && currentReliable) {
    for (const key of prevKeys) {
      if (!currentKeys.has(key)) {
        changeAt = Math.max(changeAt, asNumber(current.accountHintAt, 0), generatedAt);
        changeType = currentCount > 0 ? "position_closed_partial" : "position_closed";
        confidence = "high";
      }
    }
  }

  if (!changeAt && currentReliable && currentCount > 0 && !prev.lastPositionActivityAt) {
    changeAt = currentOpenedAt;
    changeType = "snapshot_seed";
    confidence = "medium";
  }

  const lastPositionActivityAt = currentCount > 0
    ? Math.max(prevTrusted ? asNumber(prev.lastPositionActivityAt, 0) : 0, currentOpenedAt)
    : 0;
  const lastPositionChangeAt = Math.max(
    asNumber(current.lastKnownPositionChangeAt, 0),
    asNumber(prev.lastKnownPositionChangeAt, 0),
    changeAt
  );
  const lastPositionActivityType =
    currentCount > 0
      ? changeType || (prevTrusted ? prev.lastPositionActivityType : null) || null
      : null;
  return {
    lastPositionActivityAt,
    lastPositionChangeAt: lastPositionChangeAt || null,
    lastPositionActivityType,
    lastPositionActivityConfidence: confidence || null,
  };
}

function freshnessStatus(snapshotAt, now = Date.now()) {
  const ts = asNumber(snapshotAt, 0);
  if (!ts) return { status: "missing", healthy: false, ageMs: null };
  const ageMs = Math.max(0, now - ts);
  if (ageMs <= HEALTHY_MAX_AGE_MS) return { status: "healthy", healthy: true, ageMs };
  if (ageMs <= DEGRADED_MAX_AGE_MS) return { status: "degraded", healthy: false, ageMs };
  return { status: "stale", healthy: false, ageMs };
}

function buildMonitorPayload(previous = {}) {
  const now = Date.now();
  const current = loadCurrentSnapshot();
  const previousWallets = previous && previous.wallets && typeof previous.wallets === "object" ? previous.wallets : {};
  const currentWallets = current.wallets;
  const allWallets = new Set([...Object.keys(previousWallets), ...currentWallets.keys()]);
  const walletPayload = {};

  for (const wallet of allWallets) {
    const prev = previousWallets[wallet] || {};
    const hasCurrentWallet = currentWallets.has(wallet);
    const currentRow = currentWallets.get(wallet) || createWalletSnapshot(wallet);
    const diff = diffWallet(prev, currentRow, current.generatedAt);
    const lastPositionSnapshotAt = Math.max(
      asNumber(currentRow.lastPositionSnapshotAt, 0),
      asNumber(currentRow.accountHintAt, 0),
      asNumber(prev.lastPositionSnapshotAt, 0)
    );
    const openPositionsUpdatedAt = Math.max(
      asNumber(currentRow.lastPositionSnapshotAt, 0),
      asNumber(currentRow.accountHintAt, 0),
      asNumber(prev.openPositionsUpdatedAt, 0)
    );
    const fresh = freshnessStatus(lastPositionSnapshotAt, now);
    walletPayload[wallet] = {
      wallet,
      liveSourceMode: "snapshot_stream",
      activeWsSubscription: false,
      wsSubscriptionStatus: "upstream_snapshot_only",
      lastEventReceivedAt: Math.max(
        asNumber(currentRow.lastEventReceivedAt, 0),
        asNumber(prev.lastEventReceivedAt, 0)
      ) || null,
      lastLocalStateUpdateAt: now,
      lastPositionSnapshotAt: lastPositionSnapshotAt || null,
      lastPositionActivityAt: diff.lastPositionActivityAt || null,
      lastPositionChangeAt: diff.lastPositionChangeAt || null,
      lastPositionActivityType: diff.lastPositionActivityType || null,
      lastPositionActivityConfidence: diff.lastPositionActivityConfidence || null,
      liveHeartbeatHealthy: fresh.healthy,
      liveFreshnessStatus: fresh.status,
      liveFreshnessAgeMs: fresh.ageMs,
      openPositions: Math.max(0, asNumber(currentRow.openPositions, 0)),
      openPositionsUpdatedAt: openPositionsUpdatedAt || null,
      accountHintAt: asNumber(currentRow.accountHintAt, 0) || null,
      accountHintReason: currentRow.accountHintReason || null,
      accountHintMissCount: Math.max(0, asNumber(currentRow.accountHintMissCount, 0)),
      materializedPositions: Math.max(0, asNumber(currentRow.materializedPositions, 0)),
      hasReliablePositionSet: Boolean(currentRow.hasReliablePositionSet),
      snapshotConsistency: currentRow.snapshotConsistency || "unknown",
      hasSnapshotGap: Boolean(currentRow.hasSnapshotGap),
      currentSnapshotPresent: hasCurrentWallet,
      lastKnownPositionChangeAt: Math.max(
        asNumber(currentRow.lastKnownPositionChangeAt, 0),
        asNumber(prev.lastKnownPositionChangeAt, 0),
        asNumber(diff.lastPositionChangeAt, 0)
      ) || null,
      positionSources: Array.from(currentRow.positionSources || []).sort(),
      positionKeys: Object.fromEntries(
        Object.entries(currentRow.positions || {}).map(([key, row]) => [
          key,
          {
            size: asNumber(row.size, 0),
            updatedAt: asNumber(row.stateChangedAt, 0),
            openedAt: asNumber(row.openedAt, 0),
            observedAt: asNumber(row.observedAt, 0),
          },
        ])
      ),
    };
  }

  const summaryRows = Object.values(walletPayload);
  const summary = {
    sourceMode: "snapshot_stream",
    generatedAt: now,
    snapshotGeneratedAt: current.generatedAt || null,
    totalWallets: summaryRows.length,
    openWallets: summaryRows.filter((row) => asNumber(row.openPositions, 0) > 0).length,
    healthyWallets: summaryRows.filter((row) => row.liveFreshnessStatus === "healthy").length,
    degradedWallets: summaryRows.filter((row) => row.liveFreshnessStatus === "degraded").length,
    staleWallets: summaryRows.filter((row) => row.liveFreshnessStatus === "stale").length,
    missingWallets: summaryRows.filter((row) => row.liveFreshnessStatus === "missing").length,
  };

  return {
    version: MONITOR_SCHEMA_VERSION,
    generatedAt: now,
    snapshotGeneratedAt: current.generatedAt || null,
    sourceMode: "snapshot_stream",
    summary,
    wallets: walletPayload,
  };
}

function loadLiveMonitor() {
  return readJson(MONITOR_PATH, {
    version: MONITOR_SCHEMA_VERSION,
    generatedAt: 0,
    snapshotGeneratedAt: 0,
    sourceMode: "snapshot_stream",
    summary: {
      sourceMode: "snapshot_stream",
      generatedAt: 0,
      snapshotGeneratedAt: 0,
      totalWallets: 0,
      openWallets: 0,
      healthyWallets: 0,
      degradedWallets: 0,
      staleWallets: 0,
      missingWallets: 0,
    },
    wallets: {},
  });
}

function syncLiveMonitor() {
  const previous = loadLiveMonitor();
  const next = buildMonitorPayload(
    Number(previous.version || 0) === MONITOR_SCHEMA_VERSION ? previous : {}
  );
  writeJsonAtomic(MONITOR_PATH, next);
  return next;
}

module.exports = {
  MONITOR_PATH,
  loadLiveMonitor,
  syncLiveMonitor,
};
