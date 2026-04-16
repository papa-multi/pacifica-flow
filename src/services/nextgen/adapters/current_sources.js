"use strict";

const path = require("path");
const { readJson, writeJsonAtomic } = require("../core/json-file");
const { buildEventEnvelope } = require("../common/event-envelope");
const { loadWalletExplorerV3Snapshot } = require("../../read_model/wallet_storage_v3");
const config = require("../common/config");

const LEGACY_DATA_ROOT = path.join(config.rootDir, "data");
const REVIEW_STATUS_PATH = path.join(
  LEGACY_DATA_ROOT,
  "wallet_explorer_v2",
  "review_pipeline",
  "wallet_status.json"
);
const REVIEW_STATE_PATH = path.join(
  LEGACY_DATA_ROOT,
  "wallet_explorer_v2",
  "review_pipeline",
  "state.json"
);
const LIVE_WALLET_UNIVERSE_PATH = path.join(
  LEGACY_DATA_ROOT,
  "live_positions",
  "live_wallet_universe.json"
);
const LIVE_POSITIONS_DIR = path.join(LEGACY_DATA_ROOT, "live_positions");
const SOURCE_STATE_PATH = path.join(config.dataDir, "source-state.json");

function readSourceState() {
  return readJson(SOURCE_STATE_PATH, {
    walletExplorerUpdatedAt: 0,
    reviewStatusUpdatedAt: 0,
    liveUniverseUpdatedAt: 0,
    marketUpdatedAt: 0,
    sequence: 0,
  });
}

function writeSourceState(state) {
  writeJsonAtomic(SOURCE_STATE_PATH, {
    walletExplorerUpdatedAt: Math.max(0, Number(state.walletExplorerUpdatedAt || 0)),
    reviewStatusUpdatedAt: Math.max(0, Number(state.reviewStatusUpdatedAt || 0)),
    liveUniverseUpdatedAt: Math.max(0, Number(state.liveUniverseUpdatedAt || 0)),
    marketUpdatedAt: Math.max(0, Number(state.marketUpdatedAt || 0)),
    sequence: Math.max(0, Number(state.sequence || 0)),
  });
}

function nextSequence(sourceState) {
  sourceState.sequence = Math.max(0, Number(sourceState.sequence || 0)) + 1;
  return sourceState.sequence;
}

function loadWalletExplorerRows() {
  const v3Snapshot = loadWalletExplorerV3Snapshot();
  if (v3Snapshot && Array.isArray(v3Snapshot.preparedRows)) {
    return v3Snapshot.preparedRows;
  }
  return [];
}

function loadReviewStatusRows() {
  const payload = readJson(REVIEW_STATUS_PATH, {});
  return Array.isArray(payload.rows) ? payload.rows : [];
}

function loadReviewState() {
  return readJson(REVIEW_STATE_PATH, {});
}

function loadLiveWalletUniverse() {
  return readJson(LIVE_WALLET_UNIVERSE_PATH, {});
}

function normalizeWallet(value) {
  return String(value || "").trim();
}

function normalizeMillis(...values) {
  for (const value of values) {
    const num = Number(value || 0);
    if (!Number.isFinite(num) || num <= 0) continue;
    return num >= 1e12 ? Math.trunc(num) : Math.trunc(num * 1000);
  }
  return 0;
}

function fileMtimeMs(filePath) {
  try {
    return Math.trunc(require("fs").statSync(filePath).mtimeMs || 0);
  } catch {
    return 0;
  }
}

function upsertLiveWallet(map, wallet, { updatedAt = 0, liveScannedAt = 0, openPositions = null, source = null } = {}) {
  const key = normalizeWallet(wallet);
  if (!key) return;
  const row = map.get(key) || {
    wallet: key,
    updatedAt: 0,
    liveScannedAt: 0,
    openPositions: 0,
    openPositionsUpdatedAt: 0,
    openPositionsSource: null,
    openPositionsPriority: 0,
    sources: [],
  };
  row.updatedAt = Math.max(Number(row.updatedAt || 0), Number(updatedAt || 0));
  row.liveScannedAt = Math.max(Number(row.liveScannedAt || 0), Number(liveScannedAt || 0));
  if (openPositions !== null && openPositions !== undefined) {
    const nextOpenPositions = Math.max(0, Number(openPositions || 0));
    const nextObservedAt = Math.max(Number(updatedAt || 0), Number(liveScannedAt || 0));
    const nextPriority =
      source === "live_positions_snapshot" ? 2 : source === "account_open_hint" ? 1 : 0;
    const currentPriority = Number(row.openPositionsPriority || 0);
    const currentObservedAt = Number(row.openPositionsUpdatedAt || 0);
    const shouldReplace =
      currentObservedAt <= 0 ||
      nextPriority > currentPriority ||
      (nextPriority === currentPriority && nextObservedAt >= currentObservedAt);
    if (shouldReplace) {
      row.openPositions = nextOpenPositions;
      row.openPositionsUpdatedAt = nextObservedAt;
      row.openPositionsSource = source || row.openPositionsSource || null;
      row.openPositionsPriority = nextPriority;
    }
  }
  if (source && !row.sources.includes(source)) {
    row.sources.push(source);
  }
  map.set(key, row);
}

function synthesizeLiveWalletUniverseFromShards() {
  const fs = require("fs");
  let shardPaths = [];
  try {
    shardPaths = fs
      .readdirSync(LIVE_POSITIONS_DIR)
      .filter((name) => /^wallet_first_shard_\d+\.json$/.test(name))
      .map((name) => path.join(LIVE_POSITIONS_DIR, name))
      .sort();
  } catch {
    shardPaths = [];
  }
  const wallets = new Map();
  let generatedAt = 0;
  for (const shardPath of shardPaths) {
    const payload = readJson(shardPath, {}) || {};
    const shardGeneratedAt = normalizeMillis(payload.generatedAt, fileMtimeMs(shardPath), Date.now());
    generatedAt = Math.max(generatedAt, shardGeneratedAt);
    const positions = Array.isArray(payload.positions) ? payload.positions : [];
    const positionCounts = new Map();
    for (const position of positions) {
      if (!position || typeof position !== "object") continue;
      const wallet = normalizeWallet(position.wallet);
      if (!wallet) continue;
      const observedAt = normalizeMillis(
        position.observedAt,
        position.updatedAt,
        position.openedAt,
        shardGeneratedAt
      );
      positionCounts.set(wallet, (positionCounts.get(wallet) || 0) + 1);
      upsertLiveWallet(wallets, wallet, {
        updatedAt: observedAt,
        liveScannedAt: observedAt,
        source: "live_positions_snapshot",
      });
    }
    for (const [wallet, count] of positionCounts.entries()) {
      upsertLiveWallet(wallets, wallet, {
        updatedAt: shardGeneratedAt,
        liveScannedAt: shardGeneratedAt,
        openPositions: count,
        source: "live_positions_snapshot",
      });
    }
    const hints = Array.isArray(payload.accountOpenHints) ? payload.accountOpenHints : [];
    for (const hint of hints) {
      if (!hint || typeof hint !== "object") continue;
      const wallet = normalizeWallet(hint.wallet);
      if (!wallet) continue;
      const hintAt = normalizeMillis(hint.lastAccountAt, hint.updatedAt, shardGeneratedAt);
      upsertLiveWallet(wallets, wallet, {
        updatedAt: hintAt,
        liveScannedAt: hintAt,
        openPositions: Number(hint.positionsCount || 0),
        source: "account_open_hint",
      });
    }
  }
  return {
    generatedAt,
    wallets: Array.from(wallets.values()).sort((a, b) => {
      const aOpen = Number(a.openPositions || 0);
      const bOpen = Number(b.openPositions || 0);
      if (aOpen !== bOpen) return bOpen - aOpen;
      const aSeen = Number(a.liveScannedAt || a.updatedAt || 0);
      const bSeen = Number(b.liveScannedAt || b.updatedAt || 0);
      if (aSeen !== bSeen) return bSeen - aSeen;
      return String(a.wallet || "").localeCompare(String(b.wallet || ""));
    }),
  };
}

function loadFreshestLiveWalletUniverse() {
  const universe = loadLiveWalletUniverse();
  const universeGeneratedAt = normalizeMillis(universe.generatedAt, fileMtimeMs(LIVE_WALLET_UNIVERSE_PATH));
  const fs = require("fs");
  let freshestShardAt = 0;
  try {
    const shardPaths = fs
      .readdirSync(LIVE_POSITIONS_DIR)
      .filter((name) => /^wallet_first_shard_\d+\.json$/.test(name))
      .map((name) => path.join(LIVE_POSITIONS_DIR, name));
    for (const shardPath of shardPaths) {
      freshestShardAt = Math.max(freshestShardAt, fileMtimeMs(shardPath));
    }
  } catch {
    freshestShardAt = 0;
  }
  if (freshestShardAt > universeGeneratedAt) {
    return synthesizeLiveWalletUniverseFromShards();
  }
  return universe;
}

function buildBootstrapEvents(sourceState) {
  const rows = loadWalletExplorerRows();
  const minUpdatedAt = Math.max(0, Number(sourceState.walletExplorerUpdatedAt || 0));
  const nextRows = rows.filter((row) => Number(row && row.updatedAt ? row.updatedAt : 0) > minUpdatedAt);
  const events = nextRows.map((row) =>
    buildEventEnvelope({
      eventId: `bootstrap-${row.wallet}-${row.updatedAt}`,
      source: "bootstrap",
      channel: "bootstrap.wallet_snapshot",
      entityType: "wallet",
      entityKey: row.wallet,
      wallet: row.wallet,
      sequence: nextSequence(sourceState),
      eventTime: Number(row.updatedAt || row.lastActivity || row.lastOpenedAt || Date.now()),
      payload: {
        first_deposit_at: Number(row.firstTrade || 0),
        first_trade_at: Number(row.firstTrade || 0),
        last_opened_at: Number(
          row.lastOpenedAt ||
            row.lastActivity ||
            row.lastActivityAt ||
            (row.lastOpenedPosition && row.lastOpenedPosition.openedAt) ||
            0
        ),
        last_activity_at: Number(
          row.lastActivity ||
            row.lastActivityAt ||
            row.lastOpenedAt ||
            (row.lastOpenedPosition && row.lastOpenedPosition.openedAt) ||
            0
        ),
        verified_through_at: 0,
        last_head_check_at: 0,
        review_stage: String(row.historyPhase || "bootstrap"),
        open_positions: Number(row.openPositions || 0),
        open_orders: 0,
        volume_usd: Number(row.volumeUsdRaw || row.volumeUsd || 0),
        trades: Number(row.trades || 0),
        realized_pnl_usd: Number(row.pnlUsd || 0),
        unrealized_pnl_usd: Number(row.unrealizedPnlUsd || 0),
        fees_paid_usd: Number(row.feesPaidUsd || row.feesUsd || 0),
        total_wins: Number(row.totalWins || 0),
        total_losses: Number(row.totalLosses || 0),
        win_rate: Number(row.winRate || 0),
        last_trade_at: Number(row.lastTrade || 0),
        symbol_breakdown: Array.isArray(row.symbolBreakdown) ? row.symbolBreakdown : [],
      },
    })
  );
  if (nextRows.length > 0) {
    sourceState.walletExplorerUpdatedAt = Math.max(
      sourceState.walletExplorerUpdatedAt || 0,
      ...nextRows.map((row) => Number(row && row.updatedAt ? row.updatedAt : 0))
    );
  }
  return events;
}

function buildReviewStatusEvents(sourceState) {
  const rows = loadReviewStatusRows();
  const minUpdatedAt = Math.max(0, Number(sourceState.reviewStatusUpdatedAt || 0));
  const nextRows = rows.filter((row) => Number(row && row.updated_at ? row.updated_at : 0) > minUpdatedAt);
  const events = nextRows.map((row) =>
    buildEventEnvelope({
      eventId: `review-${row.wallet}-${row.updated_at}`,
      source: "review_pipeline",
      channel: "wallet.head_check",
      entityType: "wallet",
      entityKey: row.wallet,
      wallet: row.wallet,
      sequence: nextSequence(sourceState),
      eventTime: Number(row.verified_through_at || row.last_head_check_at || row.updated_at || Date.now()),
      payload: {
        verified_through_at: Number(row.verified_through_at || 0),
        last_head_check_at: Number(row.last_head_check_at || 0),
        review_stage: String(row.stage || "history"),
        last_trade_at: Number(row.last_trade_at || 0),
        first_trade_at: Number(row.first_trade_at || 0),
        trade_rows_loaded: Number(row.trade_rows_loaded || 0),
        volume_usd: Number(row.volume_usd || 0),
        zero_trade: Number(row.zero_trade || 0),
        history_complete: Number(row.history_complete || 0),
        gap_closed: Number(row.gap_closed || 0),
      },
    })
  );
  if (nextRows.length > 0) {
    sourceState.reviewStatusUpdatedAt = Math.max(
      sourceState.reviewStatusUpdatedAt || 0,
      ...nextRows.map((row) => Number(row && row.updated_at ? row.updated_at : 0))
    );
  }
  return events;
}

function buildLivePositionEvents(sourceState) {
  const payload = loadFreshestLiveWalletUniverse();
  const rows = Array.isArray(payload.wallets) ? payload.wallets : [];
  const generatedAt = Math.max(
    0,
    Number(payload.generatedAt || 0),
    ...rows.map((row) => Number(row && row.updatedAt ? row.updatedAt : 0))
  );
  const minUpdatedAt = Math.max(0, Number(sourceState.liveUniverseUpdatedAt || 0));
  const nextRows = rows.filter((row) => Number(row && row.updatedAt ? row.updatedAt : 0) > minUpdatedAt);
  const events = nextRows
    .map((row) => {
      const wallet = String(row && row.wallet ? row.wallet : "").trim();
      if (!wallet) return null;
      const updatedAt = Number(row.updatedAt || row.liveScannedAt || generatedAt || Date.now());
      return buildEventEnvelope({
        eventId: `livepos-${wallet}-${updatedAt}`,
        source: "live_positions",
        channel: "wallet.live_snapshot",
        entityType: "wallet",
        entityKey: wallet,
        wallet,
        sequence: nextSequence(sourceState),
        eventTime: updatedAt,
        payload: {
          open_positions: Number(row.openPositions || 0),
          open_positions_updated_at: Number(
            row.openPositionsUpdatedAt || row.liveScannedAt || updatedAt || 0
          ),
          live_scanned_at: Number(row.liveScannedAt || updatedAt || 0),
          sources: Array.isArray(row.sources) ? row.sources : [],
        },
      });
    })
    .filter(Boolean);
  if (generatedAt > 0) {
    sourceState.liveUniverseUpdatedAt = Math.max(
      Number(sourceState.liveUniverseUpdatedAt || 0),
      generatedAt,
      ...nextRows.map((row) => Number(row && row.updatedAt ? row.updatedAt : 0))
    );
  }
  return events;
}

function buildMarketEvents(sourceState, pricesPayload) {
  const rows = Array.isArray(pricesPayload) ? pricesPayload : [];
  const now = Date.now();
  sourceState.marketUpdatedAt = now;
  return rows.map((row) =>
    buildEventEnvelope({
      eventId: `market-${String(row && row.symbol || "").toUpperCase()}-${now}`,
      source: "rest",
      channel: "market.price",
      entityType: "market",
      entityKey: String(row && row.symbol || "").toUpperCase(),
      market: String(row && row.symbol || "").toUpperCase(),
      sequence: nextSequence(sourceState),
      eventTime: now,
      payload: {
        price: Number(row && row.mark !== undefined ? row.mark : row && row.price !== undefined ? row.price : 0),
        bid: Number(row && row.bid !== undefined ? row.bid : 0),
        ask: Number(row && row.ask !== undefined ? row.ask : 0),
      },
    })
  ).filter((event) => Boolean(event.market));
}

module.exports = {
  REVIEW_STATUS_PATH,
  REVIEW_STATE_PATH,
  LIVE_WALLET_UNIVERSE_PATH,
  LIVE_POSITIONS_DIR,
  SOURCE_STATE_PATH,
  loadWalletExplorerRows,
  loadReviewStatusRows,
  loadReviewState,
  loadLiveWalletUniverse,
  loadFreshestLiveWalletUniverse,
  readSourceState,
  writeSourceState,
  buildBootstrapEvents,
  buildReviewStatusEvents,
  buildLivePositionEvents,
  buildMarketEvents,
};
