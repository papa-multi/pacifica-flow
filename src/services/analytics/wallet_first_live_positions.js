const fs = require("fs");

const { readJson } = require("../pipeline/utils");
const { loadWalletExplorerV3Snapshot } = require("../read_model/wallet_storage_v3");
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

function normalizeOpenSide(value) {
  const side = normalizeSide(value);
  if (side === "close_long") return "open_long";
  if (side === "close_short") return "open_short";
  return side;
}

function normalizeTimestampMs(value, fallback = 0) {
  const ts = Number(value);
  if (!Number.isFinite(ts) || ts <= 0) return Number(fallback || 0);
  return ts < 1e12 ? ts * 1000 : ts;
}

function normalizePositionDirection(value) {
  const side = normalizeOpenSide(value);
  if (side === "open_long") return "long";
  if (side === "open_short") return "short";
  return "";
}

function logicalPositionKey(row = {}) {
  const symbol = normalizeSymbol(row.symbol);
  const side = normalizeOpenSide(row.rawSide || row.side || "");
  if (!symbol || !side) return "";
  return `${symbol}|${side}`;
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

function isPositivePnlWalletRow(row = {}) {
  const all = safeWindow(row, "all");
  const candidates = [
    row.totalPnlUsd,
    row.pnlUsd,
    row.realizedPnlUsd,
    row.pnlAllTime,
    all.totalPnlUsd,
    all.pnlUsd,
    all.realizedPnlUsd,
  ]
    .map((value) => toNum(value, NaN))
    .filter((value) => Number.isFinite(value));
  return candidates.length > 0 && Math.max(...candidates) > 0;
}

function buildWalletPriorityMeta(row = {}) {
  const now = Date.now();
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
  const unrealizedPositivePnlUsd = Math.max(0, toNum(row.unrealizedPnlUsd, 0));
  const realizedPositivePnlUsd = Math.max(0, toNum(row.totalPnlUsd, toNum(row.pnlUsd, 0)));
  const firstDepositAt =
    normalizeTimestampMs(
      row.firstDepositAt ||
        row.firstTrade ||
        all.firstTrade ||
        d30.firstTrade ||
        d7.firstTrade ||
        d24.firstTrade,
      0
    ) || 0;
  const firstDepositAgeDays =
    firstDepositAt > 0 ? Math.max(0, Math.floor((now - firstDepositAt) / (24 * 60 * 60 * 1000))) : 0;
  const rankTs = Math.max(
    Number(row.liveScannedAt || 0),
    Number(row.updatedAt || 0),
    Number(d24.lastTrade || 0),
    Number(d7.lastTrade || 0),
    Number(d30.lastTrade || 0),
    Number(all.lastTrade || 0)
  );
  const rankBonus = liveActiveRank > 0 ? Math.max(0, 25_000 - liveActiveRank * 150) : 0;
  const positivePnlBoost = Math.round(
    Math.min(
      1_000_000,
      (openPositions > 0 ? Math.sqrt(unrealizedPositivePnlUsd) * 4500 : 0) +
        Math.sqrt(realizedPositivePnlUsd) * 1200
    )
  );
  const firstDepositBoost = Math.round(
    Math.min(
      500_000,
      Math.sqrt(Math.max(0, firstDepositAgeDays)) * 12000
    )
  );
  const priorityBoost = Math.round(
    Math.min(
      2_500_000,
      openPositions * 250_000 +
        positivePnlBoost +
        firstDepositBoost +
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
    unrealizedPositivePnlUsd: Number(unrealizedPositivePnlUsd.toFixed(2)),
    realizedPositivePnlUsd: Number(realizedPositivePnlUsd.toFixed(2)),
    firstDepositAt,
    firstDepositAgeDays,
    positivePnlBoost,
    firstDepositBoost,
    rankTs,
    priorityBoost,
    wsPriorityScore:
      priorityBoost +
      positivePnlBoost * 8 +
      firstDepositBoost * 4 +
      Math.round(Math.min(200_000, recentVolumeUsd / 25)) +
      Math.round(Math.min(150_000, unrealizedPositivePnlUsd * 4)),
  };
}

function positionKey(row = {}) {
  const symbol = normalizeSymbol(row.symbol);
  const sideRaw = String(row.rawSide || row.sideRaw || row.side || "").trim().toLowerCase();
  const isolated = row.isolated ? "iso" : "cross";
  const entry = Number.isFinite(toNum(row.entry, NaN)) ? Number(toNum(row.entry, 0)).toFixed(8) : "na";
  return `${symbol}|${sideRaw || "na"}|${isolated}|${entry}`;
}

function normalizeAccountSettingRow(row = {}) {
  if (!row || typeof row !== "object") return null;
  const symbol = normalizeSymbol(
    row.symbol !== undefined
      ? row.symbol
      : row.s !== undefined
      ? row.s
      : row.market !== undefined
      ? row.market
      : row.asset
  );
  if (!symbol) return null;
  const leverage = toNum(
    row.leverage !== undefined
      ? row.leverage
      : row.l !== undefined
      ? row.l
      : row.maxLeverage !== undefined
      ? row.maxLeverage
      : row.max_leverage,
    NaN
  );
  const marginMode = String(
    row.marginMode !== undefined
      ? row.marginMode
      : row.margin_mode !== undefined
      ? row.margin_mode
      : row.mode !== undefined
      ? row.mode
      : ""
  )
    .trim()
    .toLowerCase();
  const isolated =
    row.isolated !== undefined
      ? Boolean(row.isolated)
      : row.i !== undefined
      ? Boolean(row.i)
      : marginMode === "isolated"
      ? true
      : marginMode === "cross"
      ? false
      : undefined;
  const updatedAt = normalizeTimestampMs(
    row.updated_at !== undefined
      ? row.updated_at
      : row.updatedAt !== undefined
      ? row.updatedAt
      : row.created_at !== undefined
      ? row.created_at
      : row.createdAt,
    Date.now()
  );
  return {
    symbol,
    leverage:
      Number.isFinite(leverage) && leverage > 0
        ? Number(leverage)
        : NaN,
    isolated: isolated === undefined ? null : Boolean(isolated),
    updatedAt,
  };
}

function extractAccountSettingsRows(data) {
  if (Array.isArray(data)) return data;
  if (!data || typeof data !== "object") return [];
  const candidates = [
    data.data,
    data.settings,
    data.margin_settings,
    data.marginSettings,
    data.account_settings,
    data.accountSettings,
    data.items,
    data.rows,
    data.list,
  ];
  for (const candidate of candidates) {
    if (Array.isArray(candidate)) return candidate;
  }
  if (data.margin_settings && typeof data.margin_settings === "object") {
    return Object.entries(data.margin_settings).map(([symbol, value]) => ({
      symbol,
      ...(value && typeof value === "object" ? value : {}),
    }));
  }
  if (data.symbol || data.s || data.market || data.asset) {
    return [data];
  }
  return [];
}

function buildAccountSettingsBySymbol(data) {
  const rows = extractAccountSettingsRows(data);
  const settingsBySymbol = new Map();
  rows.forEach((raw) => {
    const setting = normalizeAccountSettingRow(raw);
    if (!setting || !setting.symbol) return;
    const previous = settingsBySymbol.get(setting.symbol);
    if (!previous || Number(setting.updatedAt || 0) >= Number(previous.updatedAt || 0)) {
      settingsBySymbol.set(setting.symbol, setting);
    }
  });
  return settingsBySymbol;
}

function normalizeRowsWithSettings(rows = [], settingsBySymbol = null) {
  if (!Array.isArray(rows) || rows.length <= 0) return [];
  return rows.map((rawRow) => {
    const row = rawRow && typeof rawRow === "object" ? rawRow : null;
    if (!row) return rawRow;
    const setting = settingsBySymbol && row.symbol ? settingsBySymbol.get(row.symbol) || null : null;
    const hasRawIsolated =
      Boolean(row.raw && typeof row.raw === "object" && Object.prototype.hasOwnProperty.call(row.raw, "isolated")) ||
      Boolean(row.isolatedFromPayload);
    const isolated =
      hasRawIsolated
        ? Boolean(row.isolated)
        : setting && setting.isolated !== null && setting.isolated !== undefined
        ? Boolean(setting.isolated)
        : Boolean(row.isolated);
    const marginFromRow = toNum(
      row.margin !== undefined
        ? row.margin
        : row.marginUsd !== undefined
        ? row.marginUsd
        : row.margin_usd,
      NaN
    );
    const leverageFromRow = toNum(
      row.leverage !== undefined
        ? row.leverage
        : row.lev !== undefined
        ? row.lev
        : row.leverageValue !== undefined
        ? row.leverageValue
        : row.leverage_value,
      NaN
    );
    const leverageFromSettings = setting ? toNum(setting.leverage, NaN) : NaN;
    const positionUsd = toNum(row.positionUsd, NaN);
    const leverage =
      Number.isFinite(leverageFromRow) && leverageFromRow > 0
        ? leverageFromRow
        : Number.isFinite(leverageFromSettings) && leverageFromSettings > 0
        ? leverageFromSettings
        : Number.isFinite(marginFromRow) &&
          marginFromRow > 0 &&
          Number.isFinite(positionUsd) &&
          positionUsd > 0
        ? positionUsd / marginFromRow
        : NaN;
    const margin =
      Number.isFinite(marginFromRow) && marginFromRow > 0
        ? marginFromRow
        : Number.isFinite(leverage) && leverage > 0 && Number.isFinite(positionUsd) && positionUsd > 0
        ? positionUsd / leverage
        : NaN;
    return {
      ...row,
      isolated,
      leverage: Number.isFinite(leverage) && leverage > 0 ? Number(leverage.toFixed(6)) : NaN,
      margin: Number.isFinite(margin) && margin > 0 ? Number(margin.toFixed(2)) : NaN,
      marginUsd: Number.isFinite(margin) && margin > 0 ? Number(margin.toFixed(2)) : NaN,
    };
  });
}

function normalizePositionRow(wallet, row = {}, meta = {}) {
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
  const observedAt = normalizeTimestampMs(
    meta.observedAt !== undefined ? meta.observedAt : meta.at,
    Date.now()
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
    observedAt,
    lastObservedAt: observedAt,
    lastStateChangeAt: updatedAt,
    lastUpdatedAt: updatedAt,
    timestamp: observedAt,
    source: "wallet_first_positions",
    isolated: Boolean(row.isolated),
    isolatedFromPayload: row.isolated !== undefined,
    margin: toNum(
      row.margin !== undefined
        ? row.margin
        : row.marginUsd !== undefined
        ? row.marginUsd
        : row.margin_usd,
      NaN
    ),
    marginUsd: toNum(
      row.margin !== undefined
        ? row.margin
        : row.marginUsd !== undefined
        ? row.marginUsd
        : row.margin_usd,
      NaN
    ),
    leverage: toNum(
      row.leverage !== undefined
        ? row.leverage
        : row.lev !== undefined
        ? row.lev
        : row.leverageValue !== undefined
        ? row.leverageValue
        : row.leverage_value,
      NaN
    ),
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

function buildLastOpenedPositionSummary(wallet, row = {}, meta = {}) {
  const normalizedWallet = normalizeWallet(wallet || row.wallet);
  const symbol = normalizeSymbol(row.symbol);
  if (!normalizedWallet || !symbol) return null;
  const side = normalizeSide(row.rawSide || row.side || "");
  const direction = normalizePositionDirection(side || row.rawSide || row.side || "");
  const openedAt = normalizeTimestampMs(
    row.openedAt !== undefined
      ? row.openedAt
      : row.created_at !== undefined
      ? row.created_at
      : row.createdAt !== undefined
      ? row.createdAt
      : row.timestamp !== undefined
      ? row.timestamp
      : meta.at,
    meta.at || Date.now()
  );
  const observedAt = normalizeTimestampMs(
    row.observedAt !== undefined
      ? row.observedAt
      : row.lastObservedAt !== undefined
      ? row.lastObservedAt
      : meta.observedAt !== undefined
      ? meta.observedAt
      : meta.at,
    openedAt
  );
  const updatedAt = normalizeTimestampMs(
    row.updatedAt !== undefined
      ? row.updatedAt
      : row.lastUpdatedAt !== undefined
      ? row.lastUpdatedAt
      : row.updated_at !== undefined
      ? row.updated_at
      : observedAt,
    observedAt
  );
  const size = Math.abs(
    toNum(
      row.size !== undefined
        ? row.size
        : row.amount !== undefined
        ? row.amount
        : row.newSize,
      0
    )
  );
  const positionUsd = Math.max(
    0,
    toNum(
      row.positionUsd !== undefined
        ? row.positionUsd
        : row.notionalUsd !== undefined
        ? row.notionalUsd
        : row.position_value !== undefined
        ? row.position_value
        : 0,
      0
    )
  );
  const entry = toNum(
    row.entry !== undefined
      ? row.entry
      : row.entry_price !== undefined
      ? row.entry_price
      : row.entryPrice,
    NaN
  );
  const mark = toNum(
    row.mark !== undefined
      ? row.mark
      : row.mark_price !== undefined
      ? row.mark_price
      : row.markPrice,
    NaN
  );
  const pnlUsd = toNum(
    row.pnl !== undefined
      ? row.pnl
      : row.unrealizedPnl !== undefined
      ? row.unrealizedPnl
      : row.unrealized_pnl !== undefined
      ? row.unrealized_pnl
      : row.unrealizedPnlUsd !== undefined
      ? row.unrealizedPnlUsd
      : row.positionPnlUsd,
    NaN
  );
  const positionKeyValue = String(row.positionKey || row.key || "").trim() || positionKey(row);
  return {
    wallet: normalizedWallet,
    positionKey: positionKeyValue || null,
    symbol,
    side: side || null,
    direction: direction || null,
    entry: Number.isFinite(entry) ? entry : null,
    mark: Number.isFinite(mark) ? mark : null,
    size: Number.isFinite(size) ? Number(size.toFixed(8)) : 0,
    positionUsd: Number(positionUsd.toFixed(2)),
    unrealizedPnlUsd: Number.isFinite(pnlUsd) ? Number(pnlUsd.toFixed(2)) : null,
    openedAt: openedAt || null,
    observedAt: observedAt || null,
    updatedAt: updatedAt || null,
    source: String(meta.source || row.source || "wallet_first_positions").trim() || "wallet_first_positions",
    confidence: String(meta.confidence || row.walletConfidence || "high").trim() || "high",
  };
}

function clearLastOpenedPositionSummary(liveState) {
  if (!liveState || typeof liveState !== "object") return liveState;
  liveState.lastOpenedPositionAt = 0;
  liveState.lastOpenedPositionKey = null;
  liveState.lastOpenedPositionSymbol = null;
  liveState.lastOpenedPositionSide = null;
  liveState.lastOpenedPositionDirection = null;
  liveState.lastOpenedPositionEntry = null;
  liveState.lastOpenedPositionMark = null;
  liveState.lastOpenedPositionSize = 0;
  liveState.lastOpenedPositionUsd = 0;
  liveState.lastOpenedPositionPnlUsd = null;
  liveState.lastOpenedPositionObservedAt = 0;
  liveState.lastOpenedPositionUpdatedAt = 0;
  liveState.lastOpenedPositionSource = null;
  liveState.lastOpenedPositionConfidence = null;
  return liveState;
}

function buildProvisionalPositionRow(wallet, liveState = {}, nowMs = Date.now()) {
  const normalizedWallet = normalizeWallet(wallet || liveState.wallet);
  const symbol = normalizeSymbol(liveState.lastOpenedPositionSymbol);
  if (!normalizedWallet || !symbol) return null;
  const side = normalizeSide(liveState.lastOpenedPositionSide || "");
  const observedAt = normalizeTimestampMs(
    liveState.lastOpenedPositionObservedAt || liveState.lastOpenedPositionUpdatedAt || nowMs,
    nowMs
  );
  const updatedAt = normalizeTimestampMs(
    liveState.lastOpenedPositionUpdatedAt || liveState.lastOpenedPositionObservedAt || observedAt,
    observedAt
  );
  const openedAt = normalizeTimestampMs(liveState.lastOpenedPositionAt || observedAt, observedAt);
  const row = {
    wallet: normalizedWallet,
    symbol,
    rawSide: side || "",
    side,
    size: Math.max(0, Number(liveState.lastOpenedPositionSize || 0)),
    positionUsd: Math.max(0, Number(liveState.lastOpenedPositionUsd || 0)),
    entry: Number.isFinite(toNum(liveState.lastOpenedPositionEntry, NaN))
      ? Number(liveState.lastOpenedPositionEntry)
      : NaN,
    mark: Number.isFinite(toNum(liveState.lastOpenedPositionMark, NaN))
      ? Number(liveState.lastOpenedPositionMark)
      : NaN,
    pnl: Number.isFinite(toNum(liveState.lastOpenedPositionPnlUsd, NaN))
      ? Number(liveState.lastOpenedPositionPnlUsd)
      : NaN,
    status: "provisional",
    trackedWallet: true,
    provisional: true,
    freshness: "fresh",
    walletSource: liveState.lastOpenedPositionSource || "wallet_ws_trigger",
    walletConfidence: liveState.lastOpenedPositionConfidence || "ws_trigger",
    txSignature: "",
    txSource: liveState.lastOpenedPositionSource || "wallet_ws_trigger",
    txConfidence: liveState.lastOpenedPositionConfidence || "ws_trigger",
    tradeRef: "",
    tradeRefType: "wallet_live_provisional",
    openedAt,
    updatedAt,
    observedAt,
    lastObservedAt: observedAt,
    lastStateChangeAt: Math.max(0, Number(liveState.lastStateChangeAt || updatedAt)),
    lastUpdatedAt: updatedAt,
    timestamp: observedAt,
    source: "wallet_first_live_provisional",
    isolated: false,
    margin: NaN,
    funding: NaN,
    liquidationPrice: NaN,
    raw: null,
  };
  row.positionKey =
    String(liveState.lastOpenedPositionKey || "").trim() || positionKey(row);
  row.direction =
    liveState.lastOpenedPositionDirection || normalizePositionDirection(side || "");
  row.unrealizedPnlUsd = Number.isFinite(toNum(row.pnl, NaN)) ? Number(row.pnl) : null;
  return row;
}

function compareAmount(a, b) {
  const av = Number.isFinite(toNum(a, NaN)) ? toNum(a, 0) : 0;
  const bv = Number.isFinite(toNum(b, NaN)) ? toNum(b, 0) : 0;
  return Math.abs(av - bv) <= 1e-10;
}

function rowsLatestObservedAt(rows = []) {
  if (!Array.isArray(rows) || rows.length === 0) return 0;
  return rows.reduce((max, row) => {
    const observedAt = Math.max(
      0,
      Number(
        row &&
          (row.lastObservedAt || row.observedAt || row.timestamp || row.updatedAt || row.openedAt)
          ? row.lastObservedAt || row.observedAt || row.timestamp || row.updatedAt || row.openedAt
          : 0
      ) || 0
    );
    return Math.max(max, observedAt);
  }, 0);
}

class WalletFirstLivePositionsMonitor {
  constructor(options = {}) {
    this.logger = options.logger || console;
    this.restClient = options.restClient || null;
    this.walletStore = options.walletStore || null;
    this.hotWalletWsMonitor = options.hotWalletWsMonitor || null;
    this.triggerStore = options.triggerStore || null;
    this.publicActiveWalletsProvider =
      typeof options.publicActiveWalletsProvider === "function"
        ? options.publicActiveWalletsProvider
        : null;
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
    this.subscribeAllWallets = Boolean(options.subscribeAllWallets);
    this.scanIntervalMs = Math.max(500, Number(options.scanIntervalMs || 1000));
    this.walletListRefreshMs = Math.max(1000, Number(options.walletListRefreshMs || 3000));
    const walletsPerPassRaw = Number(options.walletsPerPass);
    this.walletsPerPass = Number.isFinite(walletsPerPassRaw)
      ? Math.max(0, Math.floor(walletsPerPassRaw))
      : 12;
    this.maxConcurrency = Math.max(1, Number(options.maxConcurrency || 8));
    this.hotWalletsPerPass = Math.max(0, Number(options.hotWalletsPerPass || 32));
    this.warmWalletsPerPass = Math.max(
      0,
      Number(options.warmWalletsPerPass || Math.max(64, this.hotWalletsPerPass * 2))
    );
    this.recentActiveWalletsPerPass = Math.max(
      0,
      Number(options.recentActiveWalletsPerPass || Math.max(24, Math.floor(this.hotWalletsPerPass / 2)))
    );
    this.requestTimeoutMs = Math.max(1500, Number(options.requestTimeoutMs || 4000));
    this.accountRequestTimeoutMs = Math.max(
      1000,
      Math.min(
        this.requestTimeoutMs,
        Number(
          options.accountRequestTimeoutMs ||
            Math.max(1000, Math.min(this.requestTimeoutMs, Math.floor(this.requestTimeoutMs * 0.6)))
        )
      )
    );
    this.accountSettingsRequestTimeoutMs = Math.max(
      1000,
      Math.min(
        this.requestTimeoutMs,
        Number(
          options.accountSettingsRequestTimeoutMs ||
            Math.max(1000, Math.min(this.requestTimeoutMs, Math.floor(this.requestTimeoutMs * 0.8)))
        )
      )
    );
    this.accountSettingsCacheTtlMs = Math.max(
      5000,
      Number(options.accountSettingsCacheTtlMs || 60 * 1000)
    );
    this.accountSettingsFailureBackoffMs = Math.max(
      2000,
      Number(options.accountSettingsFailureBackoffMs || 10 * 1000)
    );
    this.accountSettingsCacheMax = Math.max(
      500,
      Number(options.accountSettingsCacheMax || 20000)
    );
    this.maxFetchAttempts = Math.max(1, Math.min(5, Number(options.maxFetchAttempts || 2)));
    this.maxInFlightPerClient = Math.max(1, Number(options.maxInFlightPerClient || 2));
    this.maxInFlightDirect = Math.max(1, Number(options.maxInFlightDirect || 1));
    this.directFallbackOnLastAttempt = Boolean(options.directFallbackOnLastAttempt);
    this.clientSoftDisableMs = Math.max(
      15000,
      Number(options.clientSoftDisableMs || 90 * 1000)
    );
    this.clientHardDisableMs = Math.max(
      this.clientSoftDisableMs,
      Number(options.clientHardDisableMs || 5 * 60 * 1000)
    );
    this.clientSoftDisableConsecutiveTimeouts = Math.max(
      2,
      Number(options.clientSoftDisableConsecutiveTimeouts || 3)
    );
    this.clientHardDisableConsecutiveTimeouts = Math.max(
      this.clientSoftDisableConsecutiveTimeouts + 1,
      Number(options.clientHardDisableConsecutiveTimeouts || 6)
    );
    this.clientSoftDisableConsecutiveProxyFailures = Math.max(
      2,
      Number(options.clientSoftDisableConsecutiveProxyFailures || 2)
    );
    this.clientHardDisableConsecutiveProxyFailures = Math.max(
      this.clientSoftDisableConsecutiveProxyFailures + 1,
      Number(options.clientHardDisableConsecutiveProxyFailures || 4)
    );
    this.clientDisableMinRequests = Math.max(
      6,
      Number(options.clientDisableMinRequests || 12)
    );
    this.clientDisableLowSuccessRate = clamp(
      Number(options.clientDisableLowSuccessRate || 0.12),
      0.01,
      0.9
    );
    this.targetPassDurationMs = Math.max(
      3000,
      Number(options.targetPassDurationMs || 5000)
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
      3000,
      Number(options.hotReconcileMaxAgeMs || Math.max(this.scanIntervalMs * 4, 5000))
    );
    this.warmReconcileMaxAgeMs = Math.max(
      this.hotReconcileMaxAgeMs,
      Number(options.warmReconcileMaxAgeMs || Math.max(this.scanIntervalMs * 12, 30 * 1000))
    );
    this.persistDir = options.persistDir ? String(options.persistDir).trim() : "";
    this.persistEveryMs = Math.max(500, Number(options.persistEveryMs || 1000));
    this.maxPersistedEvents = Math.max(
      50,
      Number(options.maxPersistedEvents || 500)
    );
    this.maxPersistedPositionOpenedEvents = Math.max(
      100,
      Number(
        options.maxPersistedPositionOpenedEvents ||
          options.maxPositionOpenedEvents ||
          Math.max(this.maxPersistedEvents * 10, 10000)
      )
    );
    this.maxEvents = Math.max(100, Number(options.maxEvents || 50000));
    this.maxPositionOpenedEvents = Math.max(
      this.maxPersistedPositionOpenedEvents,
      Number(
        options.maxPositionOpenedEvents ||
          Math.max(this.maxEvents, this.maxPersistedPositionOpenedEvents)
      )
    );
    this.maxPositionChangeEvents = Math.max(
      this.maxPersistedEvents,
      Number(
        options.maxPositionChangeEvents ||
          Math.max(this.maxEvents, this.maxPersistedEvents * 4)
      )
    );
    this.staleMs = Math.max(15000, Number(options.staleMs || 180000));
    this.coolingMs = Math.max(5000, Number(options.coolingMs || 60000));
    this.triggerPollMs = Math.max(500, Number(options.triggerPollMs || 500));
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
    this.accountPersistPath = options.accountPersistPath
      ? String(options.accountPersistPath).trim()
      : "";
    this.accountPersistReadPath = options.accountPersistReadPath
      ? String(options.accountPersistReadPath).trim()
      : this.accountPersistPath;
    this.workerMode = ["combined", "account_census", "positions_materializer"].includes(
      String(options.workerMode || "").trim()
    )
      ? String(options.workerMode || "").trim()
      : "combined";
    this.accountPersistEveryMs = Math.max(
      500,
      Number(options.accountPersistEveryMs || this.persistEveryMs || 1000)
    );
    this.unrefTimer = options.unrefTimer !== false;
    this.accountStateRefreshMs = Math.max(
      500,
      Number(options.accountStateRefreshMs || this.scanIntervalMs || 1000)
    );
    this.walletDatasetPath = options.walletDatasetPath
      ? String(options.walletDatasetPath).trim()
      : "";
    this.walletDatasetRefreshMs = Math.max(
      1000,
      Number(options.walletDatasetRefreshMs || this.walletListRefreshMs)
    );
    this.publicActiveRefreshMs = Math.max(
      5000,
      Number(options.publicActiveRefreshMs || 30000)
    );
    this.livePublicActiveRemoteEnabled =
      String(
        options.livePublicActiveRemoteEnabled !== undefined
          ? options.livePublicActiveRemoteEnabled
          : "false"
      ).toLowerCase() === "true";
    this.gapRescueShare = clamp(
      Number(options.gapRescueShare || 0.45),
      0.1,
      0.95
    );
    this.gapRescueMinWorkers = Math.max(
      1,
      Number(options.gapRescueMinWorkers || 12)
    );
    this.openedEventMaxLagMs = Math.max(
      1000,
      Number(options.openedEventMaxLagMs || 15 * 60 * 1000)
    );
    this.wsHotOnly =
      String(options.wsHotOnly !== undefined ? options.wsHotOnly : "false").toLowerCase() ===
      "true";
    this.ignorePersistedEvents =
      String(
        options.ignorePersistedEvents !== undefined ? options.ignorePersistedEvents : "false"
      ).toLowerCase() === "true";
    this.hotOpenTriggerDedupMs = Math.max(
      100,
      Number(options.hotOpenTriggerDedupMs || 1200)
    );
    this.wsAccountInfoTriggerDedupMs = Math.max(
      1000,
      Number(options.wsAccountInfoTriggerDedupMs || 5000)
    );
    this.wsReconcileCooldownMs = Math.max(
      100,
      Number(options.wsReconcileCooldownMs || 300)
    );
    this.wsReconcileMaxConcurrency = Math.max(
      1,
      Number(
        options.wsReconcileMaxConcurrency ||
          Math.max(8, Math.floor((this.maxConcurrency || 32) * 0.75))
      )
    );

    this.walletList = [];
    this.warmWalletList = [];
    this.highValueWalletList = [];
    this.publicActiveWalletList = [];
    this.walletPriorityMeta = new Map();
    this.hotOpenTriggerAt = new Map();
    this.wsAccountInfoTriggerAt = new Map();
    this.walletCursor = 0;
    this.positionsBootstrapCursor = 0;
    this.openWalletCursor = 0;
    this.warmWalletCursor = 0;
    this.highValueWalletCursor = 0;
    this.publicActiveWalletCursor = 0;
    this.recentActiveWalletCursor = 0;
    this.priorityWalletQueue = [];
    this.priorityWalletSet = new Set();
    this.lastWalletListRefreshAt = 0;
    this.lastPublicActiveRefreshAt = 0;
    this.lastTriggerLoadAt = 0;
    this.lastExternalTriggerAt = 0;
    this.publicActiveInflight = null;
    this.walletDatasetRows = [];
    this.walletDatasetLoadedAt = 0;
    this.walletDatasetMtimeMs = 0;
    this.wsReconcileQueue = [];
    this.wsReconcileSet = new Set();
    this.wsReconcileInFlight = 0;
    this.wsReconcileLastAt = new Map();
    this.wsReconcileDispatchAt = [];

    this.walletSnapshots = new Map();
    this.accountSettingsCache = new Map();
    this.accountSnapshots = new Map();
    this.accountOpenHints = new Map();
    this.livePositionStates = new Map();
    this.provisionalPositions = new Map();
    this.positionCloseTombstones = new Map();
    this.walletLifecycle = new Map();
    this.walletScanLocks = new Set();
    this.successScannedWallets = new Set();
    this.openWalletSet = new Set();
    this.recentActiveWalletAt = new Map();
    this.events = [];
    this.positionOpenedEvents = [];
    this.positionChangeEvents = [];

    this.flatPositionsCache = [];
    this.flatPositionsDirty = true;
    this.avgWalletScanMs = this.requestTimeoutMs;

    this.running = false;
    this.timer = null;
    this.warming = false;
    this.loadedPersistedState = false;
    this.lastPersistAt = 0;
    this.lastAccountPersistAt = 0;
    this.lastAccountStateRefreshAt = 0;
    this.accountPersistGeneratedAt = 0;
    this.accountPersistMtimeMs = 0;
    this.accountSettingsCacheHits = 0;
    this.accountSettingsFetches = 0;
    this.accountSettingsFetchFailures = 0;

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
      proxiedClients: Math.max(
        0,
        this.restClientEntries.filter((entry) => entry && entry.proxyUrl).length
      ),
      directClientIncluded: this.directClientIndex >= 0,
      clientsCooling: 0,
      clientsInFlight: 0,
      clients429: 0,
      avgWalletScanMs: this.avgWalletScanMs,
      accountSettingsCacheSize: 0,
      accountSettingsCacheHits: 0,
      accountSettingsFetches: 0,
      accountSettingsFetchFailures: 0,
      targetPassDurationMs: this.targetPassDurationMs,
      recentActiveWallets: 0,
      warmWallets: 0,
      highValueWallets: 0,
      publicActiveWallets: 0,
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
      accountIndicatedOpenWallets: 0,
      accountIndicatedOpenPositionsTotal: 0,
      positionMaterializationGapWallets: 0,
      positionMaterializationGapTotal: 0,
      accountRepairDueWallets: 0,
      liveStateHintedWallets: 0,
      liveStateMaterializedWallets: 0,
      liveStateUncertainWallets: 0,
      liveStateClosedWallets: 0,
      liveStatePendingBootstrapWallets: 0,
      closeConfirmAccountZeroThreshold: 2,
      closeConfirmPositionsEmptyThreshold: 1,
      shardIndex: this.shardIndex,
      shardCount: this.shardCount,
      wsReconcileQueued: 0,
      wsReconcileInFlight: 0,
      wsReconcileDispatches1m: 0,
      wsReconcileMaxConcurrency: this.wsReconcileMaxConcurrency,
      wsReconcileCooldownMs: this.wsReconcileCooldownMs,
      positionOpenedEventsPerMinute: 0,
      positionIncreasedEventsPerMinute: 0,
      positionPartialCloseEventsPerMinute: 0,
      positionClosedEventsPerMinute: 0,
      positionOpenedEventsPer5Minutes: 0,
      positionIncreasedEventsPer5Minutes: 0,
      positionPartialCloseEventsPer5Minutes: 0,
      positionClosedEventsPer5Minutes: 0,
    };
  }

  pruneWsReconcileDispatch(nowMs = Date.now()) {
    const minTs = Math.max(0, nowMs - 60 * 1000);
    this.wsReconcileDispatchAt = this.wsReconcileDispatchAt.filter((ts) => Number(ts) >= minTs);
    return this.wsReconcileDispatchAt.length;
  }

  enqueueWsReconcile(wallet, meta = {}) {
    const normalizedWallet = normalizeWallet(wallet);
    if (!normalizedWallet) return false;
    if (!this.walletBelongsToShard(normalizedWallet)) return false;
    const now = Math.max(0, Number(meta.at || Date.now()));
    const force = Boolean(meta.force);
    const lastAt = Number(this.wsReconcileLastAt.get(normalizedWallet) || 0);
    if (!force && lastAt > 0 && now - lastAt < this.wsReconcileCooldownMs) {
      return false;
    }
    this.wsReconcileLastAt.set(normalizedWallet, now);
    if (this.wsReconcileSet.has(normalizedWallet)) {
      return false;
    }
    this.wsReconcileSet.add(normalizedWallet);
    this.wsReconcileQueue.push(normalizedWallet);
    this.status.wsReconcileQueued = this.wsReconcileQueue.length;
    this.drainWsReconcileQueue();
    return true;
  }

  drainWsReconcileQueue() {
    if (!this.enabled) return;
    while (
      this.wsReconcileInFlight < this.wsReconcileMaxConcurrency &&
      this.wsReconcileQueue.length > 0
    ) {
      const wallet = this.wsReconcileQueue.shift();
      if (!wallet) continue;
      this.wsReconcileSet.delete(wallet);
      this.wsReconcileInFlight += 1;
      this.status.wsReconcileInFlight = this.wsReconcileInFlight;
      this.status.wsReconcileQueued = this.wsReconcileQueue.length;
      const dispatchedAt = Date.now();
      this.wsReconcileDispatchAt.push(dispatchedAt);
      this.pruneWsReconcileDispatch(dispatchedAt);
      this.status.wsReconcileDispatches1m = this.wsReconcileDispatchAt.length;
      Promise.resolve()
        .then(() => this.refreshWalletPositions(wallet))
        .catch((error) => {
          this.status.lastErrorAt = Date.now();
          this.status.lastError = String(error && error.message ? error.message : error);
        })
        .finally(() => {
          this.wsReconcileInFlight = Math.max(0, this.wsReconcileInFlight - 1);
          this.status.wsReconcileInFlight = this.wsReconcileInFlight;
          this.status.wsReconcileQueued = this.wsReconcileQueue.length;
          this.status.wsReconcileDispatches1m = this.pruneWsReconcileDispatch(Date.now());
          this.drainWsReconcileQueue();
        });
    }
  }

  updateCoverage() {
    const known = Math.max(0, Number(this.status.walletsKnown || 0));
    const scannedRaw = Math.max(0, Number(this.status.walletsScannedAtLeastOnce || 0));
    const scanned = known > 0 ? Math.min(known, scannedRaw) : scannedRaw;
    this.status.walletsScannedAtLeastOnce = scanned;
    const coveragePct = known > 0 ? (scanned / known) * 100 : 0;
    this.status.walletsCoveragePct = Number(coveragePct.toFixed(2));
    this.status.warmupDone = known > 0 ? scanned >= known : false;
  }

  walletBelongsToShard(wallet) {
    const normalized = normalizeWallet(wallet);
    if (!normalized) return false;
    if (this.shardCount <= 1) return true;
    return stableWalletHash(normalized) % this.shardCount === this.shardIndex;
  }

  pruneToShardOwnership(forcePersist = false) {
    if (this.shardCount <= 1) return false;
    let changed = false;

    for (const wallet of Array.from(this.walletSnapshots.keys())) {
      if (this.walletBelongsToShard(wallet)) continue;
      this.walletSnapshots.delete(wallet);
      changed = true;
    }
    for (const wallet of Array.from(this.accountSettingsCache.keys())) {
      if (this.walletBelongsToShard(wallet)) continue;
      this.accountSettingsCache.delete(wallet);
      changed = true;
    }
    for (const wallet of Array.from(this.accountSnapshots.keys())) {
      if (this.walletBelongsToShard(wallet)) continue;
      this.accountSnapshots.delete(wallet);
      changed = true;
    }
    for (const wallet of Array.from(this.accountOpenHints.keys())) {
      if (this.walletBelongsToShard(wallet)) continue;
      this.accountOpenHints.delete(wallet);
      changed = true;
    }
    for (const wallet of Array.from(this.livePositionStates.keys())) {
      if (this.walletBelongsToShard(wallet)) continue;
      this.livePositionStates.delete(wallet);
      changed = true;
    }
    for (const wallet of Array.from(this.provisionalPositions.keys())) {
      if (this.walletBelongsToShard(wallet)) continue;
      this.provisionalPositions.delete(wallet);
      changed = true;
    }
    for (const wallet of Array.from(this.positionCloseTombstones.keys())) {
      if (this.walletBelongsToShard(wallet)) continue;
      this.positionCloseTombstones.delete(wallet);
      changed = true;
    }
    for (const wallet of Array.from(this.walletLifecycle.keys())) {
      if (this.walletBelongsToShard(wallet)) continue;
      this.walletLifecycle.delete(wallet);
      changed = true;
    }
    for (const wallet of Array.from(this.successScannedWallets)) {
      if (this.walletBelongsToShard(wallet)) continue;
      this.successScannedWallets.delete(wallet);
      changed = true;
    }
    for (const wallet of Array.from(this.openWalletSet)) {
      if (this.walletBelongsToShard(wallet)) continue;
      this.openWalletSet.delete(wallet);
      changed = true;
    }
    for (const wallet of Array.from(this.recentActiveWalletAt.keys())) {
      if (this.walletBelongsToShard(wallet)) continue;
      this.recentActiveWalletAt.delete(wallet);
      changed = true;
    }
    const nextPriorityQueue = this.priorityWalletQueue.filter((wallet) =>
      this.walletBelongsToShard(wallet)
    );
    if (nextPriorityQueue.length !== this.priorityWalletQueue.length) {
      this.priorityWalletQueue = nextPriorityQueue;
      changed = true;
    }
    const nextPrioritySet = new Set(
      Array.from(this.priorityWalletSet).filter((wallet) => this.walletBelongsToShard(wallet))
    );
    if (nextPrioritySet.size !== this.priorityWalletSet.size) {
      this.priorityWalletSet = nextPrioritySet;
      changed = true;
    }

    if (changed) {
      this.flatPositionsDirty = true;
      this.status.walletsScannedAtLeastOnce = this.successScannedWallets.size;
      this.status.walletsWithOpenPositions = this.openWalletSet.size;
      this.recomputeAccountOpenHintStatus();
      this.recomputeLivePositionStateStatus();
      this.updateCoverage();
      if (forcePersist) this.persistMaybe(true);
    }
    return changed;
  }

  ensureLivePositionState(wallet) {
    const normalized = normalizeWallet(wallet);
    if (!normalized) return null;
    let row = this.livePositionStates.get(normalized);
    if (row) return row;
    row = {
      wallet: normalized,
      state: "closed",
      lastStateChangeAt: 0,
      lastAccountAt: 0,
      accountPositionsCount: 0,
      lastPositionsAt: 0,
      materializedPositionsCount: 0,
      lastHintOpenAt: 0,
      lastMaterializedOpenAt: 0,
      lastConfirmedClosedAt: 0,
      accountZeroStreak: 0,
      positionsEmptyStreak: 0,
      lastPositionsErrorAt: 0,
      lastPositionsError: null,
      pendingBootstrap: false,
      lastReason: null,
      lastOpenedPositionAt: 0,
      lastOpenedPositionKey: null,
      lastOpenedPositionSymbol: null,
      lastOpenedPositionSide: null,
      lastOpenedPositionDirection: null,
      lastOpenedPositionEntry: null,
      lastOpenedPositionMark: null,
      lastOpenedPositionSize: 0,
      lastOpenedPositionUsd: 0,
      lastOpenedPositionPnlUsd: null,
      lastOpenedPositionObservedAt: 0,
      lastOpenedPositionUpdatedAt: 0,
      lastOpenedPositionSource: null,
      lastOpenedPositionConfidence: null,
    };
    this.livePositionStates.set(normalized, row);
    return row;
  }

  rememberLastOpenedPosition(wallet, row = {}, meta = {}) {
    const normalizedWallet = normalizeWallet(wallet);
    if (!normalizedWallet) return null;
    const summary = buildLastOpenedPositionSummary(normalizedWallet, row, meta);
    if (!summary) return null;
    const liveState = this.ensureLivePositionState(normalizedWallet);
    if (!liveState) return null;
    const currentOpenedAt = Math.max(0, Number(liveState.lastOpenedPositionAt || 0));
    const nextOpenedAt = Math.max(0, Number(summary.openedAt || 0));
    const currentUpdatedAt = Math.max(
      0,
      Number(liveState.lastOpenedPositionUpdatedAt || 0),
      Number(liveState.lastOpenedPositionObservedAt || 0)
    );
    const nextUpdatedAt = Math.max(
      0,
      Number(summary.updatedAt || 0),
      Number(summary.observedAt || 0)
    );
    if (currentOpenedAt > nextOpenedAt) return liveState;
    if (currentOpenedAt === nextOpenedAt && currentUpdatedAt > nextUpdatedAt) return liveState;
    liveState.lastOpenedPositionAt = nextOpenedAt || null;
    liveState.lastOpenedPositionKey = summary.positionKey || null;
    liveState.lastOpenedPositionSymbol = summary.symbol || null;
    liveState.lastOpenedPositionSide = summary.side || null;
    liveState.lastOpenedPositionDirection = summary.direction || null;
    liveState.lastOpenedPositionEntry = summary.entry;
    liveState.lastOpenedPositionMark = summary.mark;
    liveState.lastOpenedPositionSize = Number(summary.size || 0);
    liveState.lastOpenedPositionUsd = Number(summary.positionUsd || 0);
    liveState.lastOpenedPositionPnlUsd =
      summary.unrealizedPnlUsd !== null && summary.unrealizedPnlUsd !== undefined
        ? Number(summary.unrealizedPnlUsd || 0)
        : null;
    liveState.lastOpenedPositionObservedAt = Number(summary.observedAt || 0) || null;
    liveState.lastOpenedPositionUpdatedAt = Number(summary.updatedAt || 0) || null;
    liveState.lastOpenedPositionSource = summary.source || null;
    liveState.lastOpenedPositionConfidence = summary.confidence || null;
    return liveState;
  }

  ensureProvisionalWalletPositions(wallet) {
    const normalized = normalizeWallet(wallet);
    if (!normalized) return null;
    let rows = this.provisionalPositions.get(normalized);
    if (rows) return rows;
    rows = new Map();
    this.provisionalPositions.set(normalized, rows);
    return rows;
  }

  ensurePositionCloseTombstones(wallet) {
    const normalized = normalizeWallet(wallet);
    if (!normalized) return null;
    let rows = this.positionCloseTombstones.get(normalized);
    if (rows) return rows;
    rows = new Map();
    this.positionCloseTombstones.set(normalized, rows);
    return rows;
  }

  clearPositionTombstone(wallet, key) {
    const normalized = normalizeWallet(wallet);
    if (!normalized || !key) return;
    const rows = this.positionCloseTombstones.get(normalized);
    if (!rows) return;
    rows.delete(key);
    if (rows.size <= 0) this.positionCloseTombstones.delete(normalized);
  }

  markPositionClosed(wallet, row = {}, meta = {}) {
    const normalized = normalizeWallet(wallet);
    if (!normalized) return;
    const key = logicalPositionKey(row);
    if (!key) return;
    const at = normalizeTimestampMs(meta.at || row.observedAt || row.updatedAt || Date.now(), Date.now());
    const rows = this.ensurePositionCloseTombstones(normalized);
    rows.set(key, {
      at,
      source: String(meta.source || row.source || "wallet_ws_close").trim() || "wallet_ws_close",
      reason: String(meta.reason || "ws_close_trigger").trim() || "ws_close_trigger",
    });
    const provisional = this.provisionalPositions.get(normalized);
    if (provisional) {
      provisional.delete(key);
      if (provisional.size <= 0) this.provisionalPositions.delete(normalized);
    }
  }

  closeAllWalletPositions(wallet, meta = {}) {
    const normalized = normalizeWallet(wallet);
    if (!normalized) return;
    const at = normalizeTimestampMs(meta.at || Date.now(), Date.now());
    const snapshot = this.walletSnapshots.get(normalized) || null;
    const rows = snapshot && Array.isArray(snapshot.positions) ? snapshot.positions : [];
    rows.forEach((row) =>
      this.markPositionClosed(normalized, row, {
        at,
        source: meta.source || "wallet_ws_close_all",
        reason: meta.reason || "ws_close_all",
      })
    );
    const provisional = this.provisionalPositions.get(normalized) || null;
    if (provisional) {
      for (const row of provisional.values()) {
        this.markPositionClosed(normalized, row, {
          at,
          source: meta.source || "wallet_ws_close_all",
          reason: meta.reason || "ws_close_all",
        });
      }
    }
  }

  prunePositionCloseTombstones(nowMs = Date.now()) {
    const ttlMs = Math.max(this.staleMs, 5 * 60 * 1000);
    for (const [wallet, rows] of this.positionCloseTombstones.entries()) {
      if (!rows || rows.size <= 0) {
        this.positionCloseTombstones.delete(wallet);
        continue;
      }
      for (const [key, row] of rows.entries()) {
        const at = Math.max(0, Number(row && row.at ? row.at : 0));
        if (at > 0 && nowMs - at > ttlMs) rows.delete(key);
      }
      if (rows.size <= 0) this.positionCloseTombstones.delete(wallet);
    }
  }

  upsertProvisionalPosition(wallet, row = {}, meta = {}) {
    const normalized = normalizeWallet(wallet);
    if (!normalized) return null;
    const summary = buildLastOpenedPositionSummary(normalized, row, meta);
    if (!summary) return null;
    const logicalKey = logicalPositionKey(summary);
    if (!logicalKey) return null;
    const positions = this.ensureProvisionalWalletPositions(normalized);
    const previous = positions.get(logicalKey) || null;
    const deltaSize = Math.max(0, Number(summary.size || 0));
    const nextSize =
      meta.applyDelta && previous
        ? Math.max(0, Number(previous.size || 0) + deltaSize)
        : Math.max(deltaSize, Number(previous && previous.size ? previous.size : 0));
    const entry =
      summary.entry !== null && summary.entry !== undefined
        ? Number(summary.entry)
        : previous && Number.isFinite(toNum(previous.entry, NaN))
        ? Number(previous.entry)
        : NaN;
    const positionUsd =
      summary.positionUsd > 0
        ? Number(summary.positionUsd)
        : Number.isFinite(entry)
        ? Math.abs(nextSize * entry)
        : Number(previous && previous.positionUsd ? previous.positionUsd : 0);
    const observedAt = Math.max(
      Number(summary.observedAt || 0),
      Number(previous && previous.observedAt ? previous.observedAt : 0)
    );
    const updatedAt = Math.max(
      Number(summary.updatedAt || 0),
      Number(previous && previous.updatedAt ? previous.updatedAt : 0),
      observedAt
    );
    const provisionalRow = {
      wallet: normalized,
      symbol: summary.symbol,
      rawSide: summary.side || "",
      side: summary.side || "",
      size: Number(nextSize.toFixed(8)),
      positionUsd: Number(Number(positionUsd || 0).toFixed(2)),
      entry: Number.isFinite(entry) ? entry : NaN,
      mark:
        summary.mark !== null && summary.mark !== undefined
          ? Number(summary.mark)
          : Number.isFinite(entry)
          ? entry
          : NaN,
      pnl:
        summary.unrealizedPnlUsd !== null && summary.unrealizedPnlUsd !== undefined
          ? Number(summary.unrealizedPnlUsd)
          : previous && Number.isFinite(toNum(previous.pnl, NaN))
          ? Number(previous.pnl)
          : NaN,
      status: "provisional",
      trackedWallet: true,
      provisional: true,
      freshness: "fresh",
      walletSource: summary.source || "wallet_ws_trigger",
      walletConfidence: summary.confidence || "ws_trigger",
      txSignature: "",
      txSource: summary.source || "wallet_ws_trigger",
      txConfidence: summary.confidence || "ws_trigger",
      tradeRef: "",
      tradeRefType: "wallet_live_provisional",
      openedAt:
        previous && Number(previous.openedAt || 0) > 0
          ? Number(previous.openedAt)
          : Number(summary.openedAt || observedAt),
      updatedAt,
      observedAt,
      lastObservedAt: observedAt,
      lastStateChangeAt: updatedAt,
      lastUpdatedAt: updatedAt,
      timestamp: observedAt,
      source: "wallet_first_live_provisional",
      isolated: false,
      margin: NaN,
      funding: NaN,
      liquidationPrice: NaN,
      raw: null,
      direction: summary.direction || normalizePositionDirection(summary.side || ""),
      unrealizedPnlUsd:
        summary.unrealizedPnlUsd !== null && summary.unrealizedPnlUsd !== undefined
          ? Number(summary.unrealizedPnlUsd)
          : previous && previous.unrealizedPnlUsd !== undefined
          ? previous.unrealizedPnlUsd
          : null,
    };
    provisionalRow.positionKey = logicalKey;
    positions.set(logicalKey, provisionalRow);
    this.clearPositionTombstone(normalized, logicalKey);
    return provisionalRow;
  }

  applyProvisionalCloseOrReduce(wallet, row = {}, meta = {}) {
    const normalized = normalizeWallet(wallet);
    if (!normalized) return null;
    const normalizedSide = normalizeOpenSide(row.side || row.rawSide || "");
    const symbol = normalizeSymbol(row.symbol);
    const logicalKey = logicalPositionKey({ symbol, side: normalizedSide });
    if (!logicalKey) return null;
    const positions = this.ensureProvisionalWalletPositions(normalized);
    const previous = positions.get(logicalKey) || null;
    const reduceAmount = Math.max(0, Number(row.amount || row.size || 0));
    const previousSize = Math.max(0, Number(previous && previous.size ? previous.size : 0));
    const nextSize =
      reduceAmount > 0 && previous
        ? Math.max(0, Number((previousSize - reduceAmount).toFixed(8)))
        : 0;
    const at = normalizeTimestampMs(
      meta.at || row.observedAt || row.updatedAt || Date.now(),
      Date.now()
    );
    if (nextSize <= 1e-10) {
      this.markPositionClosed(
        normalized,
        { symbol, side: normalizedSide, observedAt: at, updatedAt: at, source: meta.source },
        meta
      );
      return {
        action: "closed",
        previousSize,
        nextSize: 0,
        logicalKey,
      };
    }
    const nextRow = {
      ...previous,
      wallet: normalized,
      symbol,
      rawSide: normalizedSide,
      side: normalizedSide,
      size: nextSize,
      positionUsd:
        Number(previous && previous.entry ? previous.entry : 0) > 0
          ? Number((nextSize * Number(previous.entry || 0)).toFixed(2))
          : Number(previous && previous.positionUsd ? previous.positionUsd : 0),
      observedAt: at,
      updatedAt: at,
      lastObservedAt: at,
      lastUpdatedAt: at,
      lastStateChangeAt: at,
      timestamp: at,
      provisional: true,
      status: "provisional",
      source: "wallet_first_live_provisional",
    };
    positions.set(logicalKey, nextRow);
    return {
      action: "reduced",
      previousSize,
      nextSize,
      logicalKey,
      row: nextRow,
    };
  }

  seedLastOpenedPositionFromRows(wallet, rows = [], meta = {}) {
    const normalizedWallet = normalizeWallet(wallet);
    if (!normalizedWallet) return null;
    const list = Array.isArray(rows) ? rows : [];
    if (!list.length) return null;
    const latestRow = list
      .slice()
      .sort((left, right) => {
        const leftOpenedAt = Math.max(
          0,
          Number(
            (left &&
              (left.openedAt || left.created_at || left.createdAt || left.timestamp)) ||
              0
          )
        );
        const rightOpenedAt = Math.max(
          0,
          Number(
            (right &&
              (right.openedAt || right.created_at || right.createdAt || right.timestamp)) ||
              0
          )
        );
        if (rightOpenedAt !== leftOpenedAt) return rightOpenedAt - leftOpenedAt;
        const leftUpdatedAt = Math.max(
          0,
          Number((left && (left.updatedAt || left.updated_at || left.observedAt)) || 0)
        );
        const rightUpdatedAt = Math.max(
          0,
          Number((right && (right.updatedAt || right.updated_at || right.observedAt)) || 0)
        );
        return rightUpdatedAt - leftUpdatedAt;
      })[0];
    if (!latestRow) return null;
    return this.rememberLastOpenedPosition(normalizedWallet, latestRow, meta);
  }

  transitionLivePositionState(wallet, nextState, meta = {}) {
    const row = this.ensureLivePositionState(wallet);
    if (!row) return null;
    const stateWallet = row.wallet;
    const normalizedState = ["closed", "hinted_open", "materialized_open", "uncertain"].includes(
      String(nextState || "").trim()
    )
      ? String(nextState).trim()
      : row.state;
    const observedAt = Math.max(0, Number(meta.at || Date.now()));
    if (normalizedState !== row.state) {
      row.state = normalizedState;
      row.lastStateChangeAt = observedAt;
    }
    if (meta.lastAccountAt !== undefined) {
      row.lastAccountAt = Math.max(row.lastAccountAt, Number(meta.lastAccountAt || 0));
    }
    if (meta.lastPositionsAt !== undefined) {
      row.lastPositionsAt = Math.max(row.lastPositionsAt, Number(meta.lastPositionsAt || 0));
    }
    if (meta.accountPositionsCount !== undefined) {
      row.accountPositionsCount = Math.max(0, Number(meta.accountPositionsCount || 0));
    }
    if (meta.materializedPositionsCount !== undefined) {
      row.materializedPositionsCount = Math.max(0, Number(meta.materializedPositionsCount || 0));
    }
    if (meta.lastHintOpenAt !== undefined) {
      row.lastHintOpenAt = Math.max(row.lastHintOpenAt, Number(meta.lastHintOpenAt || 0));
    }
    if (meta.lastMaterializedOpenAt !== undefined) {
      row.lastMaterializedOpenAt = Math.max(
        row.lastMaterializedOpenAt,
        Number(meta.lastMaterializedOpenAt || 0)
      );
    }
    if (meta.lastConfirmedClosedAt !== undefined) {
      row.lastConfirmedClosedAt = Math.max(
        row.lastConfirmedClosedAt,
        Number(meta.lastConfirmedClosedAt || 0)
      );
    }
    if (meta.accountZeroStreak !== undefined) {
      row.accountZeroStreak = Math.max(0, Number(meta.accountZeroStreak || 0));
    }
    if (meta.positionsEmptyStreak !== undefined) {
      row.positionsEmptyStreak = Math.max(0, Number(meta.positionsEmptyStreak || 0));
    }
    if (meta.pendingBootstrap !== undefined) {
      row.pendingBootstrap = Boolean(meta.pendingBootstrap);
    }
    if (meta.lastPositionsError !== undefined) {
      row.lastPositionsError = meta.lastPositionsError ? String(meta.lastPositionsError) : null;
    }
    if (meta.lastPositionsErrorAt !== undefined) {
      row.lastPositionsErrorAt = Math.max(0, Number(meta.lastPositionsErrorAt || 0));
    }
    if (meta.reason !== undefined) {
      row.lastReason = meta.reason ? String(meta.reason) : null;
    }

    if (row.state === "closed") {
      row.pendingBootstrap = false;
      row.materializedPositionsCount = 0;
      this.openWalletSet.delete(stateWallet);
    } else {
      this.openWalletSet.add(stateWallet);
    }

    return row;
  }

  recomputeLivePositionStateStatus() {
    let hinted = 0;
    let materialized = 0;
    let uncertain = 0;
    let closed = 0;
    let pendingBootstrap = 0;
    for (const [wallet, row] of this.livePositionStates.entries()) {
      if (!this.walletBelongsToShard(wallet) || !row) continue;
      if (row.state === "hinted_open") hinted += 1;
      else if (row.state === "materialized_open") materialized += 1;
      else if (row.state === "uncertain") uncertain += 1;
      else closed += 1;
      if (row.pendingBootstrap) pendingBootstrap += 1;
    }
    this.status.liveStateHintedWallets = hinted;
    this.status.liveStateMaterializedWallets = materialized;
    this.status.liveStateUncertainWallets = uncertain;
    this.status.liveStateClosedWallets = closed;
    this.status.liveStatePendingBootstrapWallets = pendingBootstrap;
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
    if (this.workerMode === "positions_materializer") {
      this.refreshAccountPersistedState(true);
    }
    if (this.hotWalletWsMonitor && typeof this.hotWalletWsMonitor.start === "function") {
      this.hotWalletWsMonitor.start();
      this.seedHotWalletTier();
    }
    const tick = () => {
      this.runPass().catch((error) => {
        this.status.lastErrorAt = Date.now();
        this.status.lastError = String(error && error.message ? error.message : error);
      });
    };
    const initialDelayMs = Math.max(
      0,
      Math.floor((this.scanIntervalMs / Math.max(1, this.shardCount)) * this.shardIndex)
    );
    const armInterval = () => {
      if (this.timer) return;
      this.timer = setInterval(tick, this.scanIntervalMs);
      if (this.unrefTimer && this.timer && typeof this.timer.unref === "function") {
        this.timer.unref();
      }
      tick();
    };
    if (initialDelayMs <= 0) {
      armInterval();
      return;
    }
    const bootstrapTimer = setTimeout(armInterval, initialDelayMs);
    if (this.unrefTimer && bootstrapTimer && typeof bootstrapTimer.unref === "function") {
      bootstrapTimer.unref();
    }
  }

  stop() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    if (this.hotWalletWsMonitor && typeof this.hotWalletWsMonitor.stop === "function") {
      this.hotWalletWsMonitor.stop();
    }
    this.wsReconcileQueue = [];
    this.wsReconcileSet.clear();
    this.wsReconcileInFlight = 0;
    this.wsReconcileDispatchAt = [];
    this.status.wsReconcileQueued = 0;
    this.status.wsReconcileInFlight = 0;
    this.status.wsReconcileDispatches1m = 0;
    this.persistMaybe(true);
    this.persistAccountStateMaybe(true);
  }

  loadWalletRows() {
    const now = Date.now();
    const datasetPath = this.walletDatasetPath;
    if (datasetPath) {
      let datasetMtimeMs = 0;
      try {
        datasetMtimeMs = Number(fs.statSync(datasetPath).mtimeMs || 0) || 0;
      } catch (_error) {
        datasetMtimeMs = 0;
      }
      const datasetRefreshDue =
        !this.walletDatasetRows.length ||
        now - Number(this.walletDatasetLoadedAt || 0) >= this.walletDatasetRefreshMs ||
        datasetMtimeMs > Number(this.walletDatasetMtimeMs || 0);
      if (datasetRefreshDue) {
        const payload = readJson(datasetPath, null);
        let rawRows = Array.isArray(payload && payload.rows) ? payload.rows : [];
        const rawWallets = Array.isArray(payload && payload.wallets) ? payload.wallets : [];
        if (!rawRows.length && payload && Array.isArray(payload.shards)) {
          const snapshot = loadWalletExplorerV3Snapshot();
          rawRows = Array.isArray(snapshot && snapshot.rows) ? snapshot.rows : [];
        }
        const fallbackRankTs = Math.max(
          0,
          Number(payload && payload.generatedAt ? payload.generatedAt : 0) || now
        );
        const rows =
          (rawRows.length > 0
            ? rawRows
            : rawWallets
                .map((entry) => {
                  const wallet =
                    typeof entry === "string"
                      ? normalizeWallet(entry)
                      : normalizeWallet(entry && entry.wallet);
                  if (!wallet) return null;
                  return {
                    wallet,
                    openPositions: Math.max(
                      0,
                      Number(
                        entry &&
                          (entry.openPositions !== undefined
                            ? entry.openPositions
                            : entry.positionsCount !== undefined
                            ? entry.positionsCount
                            : 0)
                      ) || 0
                    ),
                    liveActiveRank: Math.max(
                      0,
                      Number(entry && entry.liveActiveRank !== undefined ? entry.liveActiveRank : 0) || 0
                    ),
                    updatedAt: Math.max(
                      fallbackRankTs,
                      Number(entry && entry.updatedAt !== undefined ? entry.updatedAt : 0) || 0
                    ),
                    liveScannedAt: Math.max(
                      fallbackRankTs,
                      Number(entry && entry.liveScannedAt !== undefined ? entry.liveScannedAt : 0) || 0
                    ),
                    d24: {},
                    d7: {},
                    d30: {},
                    all: {},
                  };
                })
                .filter(Boolean))
            .filter((row) => isPositivePnlWalletRow(row));
        if (rows.length > 0) {
          this.walletDatasetRows = rows;
          this.walletDatasetLoadedAt = now;
          this.walletDatasetMtimeMs = datasetMtimeMs;
        }
      }
      if (this.walletDatasetRows.length > 0) {
        return this.walletDatasetRows;
      }
    }
    return [];
  }

  ensureWalletList() {
    const now = Date.now();
    if (
      this.walletList.length > 0 &&
      now - Number(this.lastWalletListRefreshAt || 0) < this.walletListRefreshMs
    ) {
      return;
    }
    const rows = this.loadWalletRows();
    if (!rows.length) {
      this.walletList = [];
      this.walletListSet = new Set();
      this.status.walletsKnown = 0;
      this.lastWalletListRefreshAt = now;
      return;
    }
    const unique = new Set();
    const sorted = (Array.isArray(rows) ? rows : [])
      .map((row) => ({
        wallet: normalizeWallet(row && row.wallet),
        priority: buildWalletPriorityMeta(row),
      }))
      .filter((row) => row.wallet)
      .sort((a, b) =>
        Number(b.priority.wsPriorityScore || 0) - Number(a.priority.wsPriorityScore || 0) ||
        Number(b.priority.priorityBoost || 0) - Number(a.priority.priorityBoost || 0) ||
        Number(a.priority.firstDepositAt || 0) - Number(b.priority.firstDepositAt || 0) ||
        Number(b.priority.rankTs || 0) - Number(a.priority.rankTs || 0)
      )
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
    this.walletListSet = new Set(this.walletList);
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
    let seededAccountOpenHints = false;
    shardFiltered.forEach((row) => {
      const openPositions = Math.max(
        0,
        Number(row && row.priority && row.priority.openPositions ? row.priority.openPositions : 0)
      );
      if (openPositions <= 0 || this.accountOpenHints.has(row.wallet)) return;
      this.accountOpenHints.set(row.wallet, {
        wallet: row.wallet,
        positionsCount: openPositions,
        updatedAt: Number(row && row.priority && row.priority.rankTs ? row.priority.rankTs : now) || now,
        lastAccountAt: now,
        lastPositionsRefreshAt: 0,
        materializedPositions: 0,
        missCount: 0,
        lastReason: "wallet_dataset_open_positions_seed",
      });
      seededAccountOpenHints = true;
    });
    if (seededAccountOpenHints) this.recomputeAccountOpenHintStatus();
    this.status.walletsKnown = this.walletList.length;
    this.status.warmWallets = this.warmWalletList.length;
    this.status.highValueWallets = this.highValueWalletList.length;
    this.lastWalletListRefreshAt = now;
    if (this.walletCursor >= this.walletList.length) this.walletCursor = 0;
    if (this.positionsBootstrapCursor >= this.walletList.length) this.positionsBootstrapCursor = 0;
    if (this.warmWalletCursor >= this.warmWalletList.length) this.warmWalletCursor = 0;
    if (this.highValueWalletCursor >= this.highValueWalletList.length) this.highValueWalletCursor = 0;
    this.pruneToShardOwnership(false);
    this.updateCoverage();
  }

  ensurePublicActiveWalletList() {
    if (!this.publicActiveWalletsProvider) {
      this.publicActiveWalletList = [];
      this.status.publicActiveWallets = 0;
      return;
    }
    const rows = this.publicActiveWalletsProvider();
    const filtered = Array.isArray(rows)
      ? rows
          .map((wallet) => normalizeWallet(wallet))
          .filter(Boolean)
          .filter((wallet) =>
            this.shardCount > 1
              ? stableWalletHash(wallet) % this.shardCount === this.shardIndex
              : true
          )
      : [];
    this.publicActiveWalletList = filtered;
    this.status.publicActiveWallets = filtered.length;
    if (this.publicActiveWalletCursor >= this.publicActiveWalletList.length) {
      this.publicActiveWalletCursor = 0;
    }
  }

  async refreshPublicActiveWallets(force = false) {
    const now = Date.now();
    if (
      !force &&
      this.publicActiveWalletList.length > 0 &&
      now - Number(this.lastPublicActiveRefreshAt || 0) < this.publicActiveRefreshMs
    ) {
      return this.publicActiveWalletList;
    }
    if (this.publicActiveInflight) {
      return this.publicActiveInflight;
    }
    this.publicActiveInflight = (async () => {
      this.ensureWalletList();
      const trackedWalletSet = new Set(this.walletList);
      const applyList = (wallets = []) => {
        const filtered = (Array.isArray(wallets) ? wallets : [])
          .map((wallet) => normalizeWallet(wallet))
          .filter(Boolean)
          .filter((wallet) =>
            this.shardCount > 1
              ? stableWalletHash(wallet) % this.shardCount === this.shardIndex
              : true
          )
          .filter((wallet) => trackedWalletSet.size === 0 || trackedWalletSet.has(wallet));
        this.publicActiveWalletList = filtered;
        this.status.publicActiveWallets = filtered.length;
        this.lastPublicActiveRefreshAt = Date.now();
        if (this.publicActiveWalletCursor >= this.publicActiveWalletList.length) {
          this.publicActiveWalletCursor = 0;
        }
        return this.publicActiveWalletList;
      };

      try {
        const selected = this.chooseClient({ forceDirect: this.directClientIndex >= 0 });
        const chosen = selected || this.chooseClient({ preferHealthy: false });
        if (!chosen) {
          throw new Error("public_active_leaderboard_clients_unavailable");
        }
        const { idx, entry, state } = chosen;
        state.inFlight = Number(state.inFlight || 0) + 1;
        state.requests = Number(state.requests || 0) + 1;
        state.lastUsedAt = Date.now();
        const requestStartedAt = Date.now();
        try {
          const response = await entry.client.get("/leaderboard", {
            query: { limit: 25000 },
            cost: 1,
            timeoutMs: Math.max(5000, this.requestTimeoutMs),
            retryMaxAttempts: 1,
          });
          const payload =
            response && response.payload && typeof response.payload === "object"
              ? response.payload
              : {};
          if (payload.success === false) {
            throw new Error(String(payload.error || "public_active_leaderboard_failed"));
          }
          const rows = Array.isArray(payload.data) ? payload.data : [];
          const activeWallets = rows
            .map((row) => {
              const wallet = normalizeWallet(row && row.address);
              const oiCurrent = toNum(row && row.oi_current, 0);
              return wallet && oiCurrent > 0 ? wallet : null;
            })
            .filter(Boolean);
          this.markClientResult(idx, null, { durationMs: Date.now() - requestStartedAt });
          return applyList(activeWallets);
        } finally {
          state.inFlight = Math.max(0, Number(state.inFlight || 1) - 1);
        }
      } catch (_error) {
        if (this.publicActiveWalletsProvider) {
          return applyList(this.publicActiveWalletsProvider());
        }
        return applyList([]);
      }
    })()
      .finally(() => {
        this.publicActiveInflight = null;
      });
    return this.publicActiveInflight;
  }

  loadPersistedState() {
    this.loadedPersistedState = true;
    if (!this.persistPath) return;
    const payload = readJson(this.persistPath, null);
    if (!payload || typeof payload !== "object") return;
    const positions = Array.isArray(payload.positions) ? payload.positions : [];
    const events = this.ignorePersistedEvents ? [] : Array.isArray(payload.events) ? payload.events : [];
    const positionOpenedEvents = this.ignorePersistedEvents
      ? []
      : Array.isArray(payload.positionOpenedEvents)
      ? payload.positionOpenedEvents
      : events.filter((event) =>
          String(event && (event.type || event.cause || event.sideEvent) || "")
            .trim()
            .toLowerCase()
            .includes("position_opened")
        );
    const positionChangeEvents = this.ignorePersistedEvents
      ? []
      : Array.isArray(payload.positionChangeEvents)
      ? payload.positionChangeEvents
      : events.filter((event) => {
          const type = String(event && (event.type || event.cause || event.sideEvent) || "")
            .trim()
            .toLowerCase();
          return type.includes("position_");
        });
    const successWallets = Array.isArray(payload.successScannedWallets)
      ? payload.successScannedWallets
      : [];
    const lifecycleRows = Array.isArray(payload.lifecycle) ? payload.lifecycle : [];
    const accountOpenHintRows = Array.isArray(payload.accountOpenHints)
      ? payload.accountOpenHints
      : [];
    const liveStateRows = Array.isArray(payload.liveStates) ? payload.liveStates : [];
    const grouped = new Map();
    positions.forEach((row) => {
      const wallet = normalizeWallet(row && row.wallet);
      if (!wallet) return;
      if (!grouped.has(wallet)) grouped.set(wallet, []);
      grouped.get(wallet).push(row);
    });
    grouped.forEach((rows, wallet) => {
      if (!this.walletBelongsToShard(wallet)) return;
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
      const liveState = this.ensureLivePositionState(wallet);
      if (liveState) {
        this.transitionLivePositionState(wallet, rows.length > 0 ? "materialized_open" : "closed", {
          at: lastSuccessAt || Date.now(),
          lastPositionsAt: lastSuccessAt || Date.now(),
          lastMaterializedOpenAt: rows.length > 0 ? (lastSuccessAt || Date.now()) : 0,
          materializedPositionsCount: rows.length,
          positionsEmptyStreak: rows.length > 0 ? 0 : 1,
          pendingBootstrap: false,
          reason: rows.length > 0 ? "persisted_materialized_open" : "persisted_closed",
        });
      }
      this.touchLifecycle(wallet, {
        at: lastSuccessAt || Date.now(),
        reconcile: true,
        success: true,
        position: rows.length > 0,
        openPositionsCount: rows.length,
        reason: rows.length > 0 ? "persisted_open_position" : "persisted_wallet",
      });
      if (rows.length > 0) {
        this.seedLastOpenedPositionFromRows(wallet, rows, {
          at: lastSuccessAt || Date.now(),
          observedAt: lastSuccessAt || Date.now(),
          source: "persisted_snapshot_seed",
          confidence: "medium",
        });
      }
    });
    successWallets.forEach((wallet) => {
      const text = normalizeWallet(wallet);
      if (text && this.walletBelongsToShard(text)) this.successScannedWallets.add(text);
    });
    this.events = events;
    this.positionOpenedEvents = positionOpenedEvents;
    this.positionChangeEvents = positionChangeEvents;
    events.forEach((event) => {
      const wallet = normalizeWallet(event && event.wallet);
      const at = Number(event && event.at ? event.at : event && event.timestamp ? event.timestamp : 0);
      if (wallet && !this.walletBelongsToShard(wallet)) return;
      if (wallet && String(event && (event.type || event.cause || event.sideEvent) || "").includes("position_opened")) {
        this.rememberLastOpenedPosition(wallet, event, {
          at,
          observedAt: at,
          source: String(event && event.source ? event.source : "persisted_event").trim() || "persisted_event",
          confidence: "medium",
        });
      }
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
      if (!this.walletBelongsToShard(wallet)) return;
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
    accountOpenHintRows.forEach((row) => {
      const wallet = normalizeWallet(row && row.wallet);
      if (!wallet || !this.walletBelongsToShard(wallet)) return;
      const positionsCount = Math.max(0, Number(row && row.positionsCount ? row.positionsCount : 0));
      if (positionsCount <= 0) return;
      this.accountOpenHints.set(wallet, {
        wallet,
        positionsCount,
        updatedAt: Number(row && row.updatedAt ? row.updatedAt : Date.now()) || Date.now(),
        lastAccountAt: Number(row && row.lastAccountAt ? row.lastAccountAt : 0) || 0,
        lastPositionsRefreshAt:
          Number(row && row.lastPositionsRefreshAt ? row.lastPositionsRefreshAt : 0) || 0,
        materializedPositions:
          Math.max(0, Number(row && row.materializedPositions ? row.materializedPositions : 0)) || 0,
        missCount: Math.max(0, Number(row && row.missCount ? row.missCount : 0)) || 0,
        lastReason: row && row.lastReason ? String(row.lastReason) : "persisted_account_hint",
      });
      const liveState = this.ensureLivePositionState(wallet);
      if (liveState) {
        const materializedPositions = Math.max(
          0,
          Number(row && row.materializedPositions ? row.materializedPositions : 0)
        );
        this.transitionLivePositionState(
          wallet,
          materializedPositions > 0 ? "materialized_open" : "hinted_open",
          {
            at: Number(row && row.lastAccountAt ? row.lastAccountAt : Date.now()) || Date.now(),
            lastAccountAt: Number(row && row.lastAccountAt ? row.lastAccountAt : Date.now()) || Date.now(),
            accountPositionsCount: positionsCount,
            materializedPositionsCount: materializedPositions,
            pendingBootstrap: materializedPositions < positionsCount,
            lastHintOpenAt: Number(row && row.updatedAt ? row.updatedAt : Date.now()) || Date.now(),
            reason: row && row.lastReason ? String(row.lastReason) : "persisted_account_hint",
          }
        );
      }
    });
    liveStateRows.forEach((row) => {
      const wallet = normalizeWallet(row && row.wallet);
      if (!wallet || !this.walletBelongsToShard(wallet)) return;
      const state = this.ensureLivePositionState(wallet);
      if (!state) return;
      Object.assign(state, {
        ...state,
        ...row,
        wallet,
        state: ["closed", "hinted_open", "materialized_open", "uncertain"].includes(
          String(row && row.state || "").trim()
        )
          ? String(row.state).trim()
          : state.state,
      });
      const persistedAccountPositions = Math.max(
        0,
        Number(row && row.accountPositionsCount ? row.accountPositionsCount : 0)
      );
      const persistedMaterializedPositions = Math.max(
        0,
        Number(row && row.materializedPositionsCount ? row.materializedPositionsCount : 0)
      );
      if (persistedAccountPositions > 0) {
        if (!this.accountOpenHints.has(wallet)) {
          this.accountOpenHints.set(wallet, {
            wallet,
            positionsCount: persistedAccountPositions,
            updatedAt:
              Number(
                row &&
                  (row.lastHintOpenAt || row.lastAccountAt || row.lastStateChangeAt)
                  ? row.lastHintOpenAt || row.lastAccountAt || row.lastStateChangeAt
                  : Date.now()
              ) || Date.now(),
            lastAccountAt:
              Number(
                row && (row.lastAccountAt || row.lastStateChangeAt)
                  ? row.lastAccountAt || row.lastStateChangeAt
                  : Date.now()
              ) || Date.now(),
            lastPositionsRefreshAt:
              Number(row && row.lastPositionsAt ? row.lastPositionsAt : 0) || 0,
            materializedPositions: persistedMaterializedPositions,
            missCount: Math.max(
              0,
              Number(row && row.positionsEmptyStreak ? row.positionsEmptyStreak : 0)
            ),
            lastReason:
              row && row.lastReason ? String(row.lastReason) : "persisted_live_state_hint",
          });
        }
        if (persistedMaterializedPositions > 0) {
          state.state = "materialized_open";
          state.pendingBootstrap =
            persistedMaterializedPositions < persistedAccountPositions;
        } else if (state.state === "closed") {
          state.state = "hinted_open";
          state.pendingBootstrap = true;
        }
      }
      if (state.state !== "closed") {
        this.openWalletSet.add(wallet);
      }
    });
    for (const [wallet, snapshot] of this.walletSnapshots.entries()) {
      if (!this.walletBelongsToShard(wallet)) continue;
      const rows = snapshot && Array.isArray(snapshot.positions) ? snapshot.positions : [];
      if (!rows.length) continue;
      this.transitionLivePositionState(wallet, "materialized_open", {
        at: Number(snapshot.lastSuccessAt || snapshot.scannedAt || Date.now()) || Date.now(),
        lastPositionsAt: Number(snapshot.lastSuccessAt || snapshot.scannedAt || Date.now()) || Date.now(),
        lastMaterializedOpenAt:
          Number(snapshot.lastSuccessAt || snapshot.scannedAt || Date.now()) || Date.now(),
        materializedPositionsCount: rows.length,
        positionsEmptyStreak: 0,
        pendingBootstrap:
          Math.max(
            0,
            Number((this.accountOpenHints.get(wallet) || {}).positionsCount || 0)
          ) > rows.length,
        reason: "persisted_positions_snapshot",
      });
    }
    for (const [wallet, hint] of this.accountOpenHints.entries()) {
      if (!this.walletBelongsToShard(wallet) || !hint) continue;
      const snapshot = this.walletSnapshots.get(wallet) || null;
      const materializedPositions = Math.max(
        0,
        Number(snapshot && Array.isArray(snapshot.positions) ? snapshot.positions.length : 0)
      );
      if (materializedPositions > 0) continue;
      this.transitionLivePositionState(wallet, "hinted_open", {
        at: Number(hint.lastAccountAt || hint.updatedAt || Date.now()) || Date.now(),
        lastAccountAt: Number(hint.lastAccountAt || hint.updatedAt || Date.now()) || Date.now(),
        accountPositionsCount: Math.max(0, Number(hint.positionsCount || 0)),
        materializedPositionsCount: 0,
        pendingBootstrap: true,
        lastHintOpenAt: Number(hint.updatedAt || Date.now()) || Date.now(),
        reason: "persisted_account_hint",
      });
    }
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
    this.seedPriorityWalletsFromState(Math.max(this.hotWalletsPerPass * 24, 4096));
    this.pruneToShardOwnership(true);
    this.recomputeAccountOpenHintStatus();
    this.recomputeLivePositionStateStatus();
    this.updateCoverage();
  }

  mergeAccountStatePayload(payload = {}) {
    if (!payload || typeof payload !== "object") return false;
    let changed = false;
    const successWallets = Array.isArray(payload.successScannedWallets)
      ? payload.successScannedWallets
      : [];
    const lifecycleRows = Array.isArray(payload.lifecycle) ? payload.lifecycle : [];
    const accountOpenHintRows = Array.isArray(payload.accountOpenHints)
      ? payload.accountOpenHints
      : [];
    const liveStateRows = Array.isArray(payload.liveStates) ? payload.liveStates : [];

    successWallets.forEach((wallet) => {
      const text = normalizeWallet(wallet);
      if (text && this.walletBelongsToShard(text) && !this.successScannedWallets.has(text)) {
        this.successScannedWallets.add(text);
        changed = true;
      }
    });

    lifecycleRows.forEach((row) => {
      const wallet = normalizeWallet(row && row.wallet);
      if (!wallet || !this.walletBelongsToShard(wallet)) return;
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
      changed = true;
    });

    accountOpenHintRows.forEach((row) => {
      const wallet = normalizeWallet(row && row.wallet);
      if (!wallet || !this.walletBelongsToShard(wallet)) return;
      const positionsCount = Math.max(0, Number(row && row.positionsCount ? row.positionsCount : 0));
      if (positionsCount <= 0) return;
      const existing = this.accountOpenHints.get(wallet) || null;
      const nextUpdatedAt = Number(row && row.updatedAt ? row.updatedAt : Date.now()) || Date.now();
      if (existing && Number(existing.updatedAt || 0) > nextUpdatedAt) return;
      this.accountOpenHints.set(wallet, {
        wallet,
        positionsCount,
        updatedAt: nextUpdatedAt,
        lastAccountAt: Number(row && row.lastAccountAt ? row.lastAccountAt : 0) || 0,
        lastPositionsRefreshAt:
          Number(row && row.lastPositionsRefreshAt ? row.lastPositionsRefreshAt : 0) || 0,
        materializedPositions:
          Math.max(0, Number(row && row.materializedPositions ? row.materializedPositions : 0)) || 0,
        missCount: Math.max(0, Number(row && row.missCount ? row.missCount : 0)) || 0,
        lastReason: row && row.lastReason ? String(row.lastReason) : "account_state_merge",
      });
      changed = true;
    });

    liveStateRows.forEach((row) => {
      const wallet = normalizeWallet(row && row.wallet);
      if (!wallet || !this.walletBelongsToShard(wallet)) return;
      const state = this.ensureLivePositionState(wallet);
      if (!state) return;
      const nextObservedAt = Math.max(
        0,
        Number(
          row &&
            (row.lastStateChangeAt || row.lastAccountAt || row.lastPositionsAt || row.lastHintOpenAt)
            ? row.lastStateChangeAt || row.lastAccountAt || row.lastPositionsAt || row.lastHintOpenAt
            : 0
        )
      );
      const currentObservedAt = Math.max(
        0,
        Number(
          state.lastStateChangeAt || state.lastAccountAt || state.lastPositionsAt || state.lastHintOpenAt || 0
        )
      );
      if (currentObservedAt > nextObservedAt) return;
      Object.assign(state, {
        ...state,
        ...row,
        wallet,
        state: ["closed", "hinted_open", "materialized_open", "uncertain"].includes(
          String(row && row.state || "").trim()
        )
          ? String(row.state).trim()
          : state.state,
      });
      if (state.state !== "closed") {
        this.openWalletSet.add(wallet);
      }
      changed = true;
    });

    if (changed) {
      this.flatPositionsDirty = true;
      this.status.walletsScannedAtLeastOnce = this.successScannedWallets.size;
      this.recomputeAccountOpenHintStatus();
      this.recomputeLivePositionStateStatus();
      this.updateCoverage();
      this.seedPriorityWalletsFromState(Math.max(this.hotWalletsPerPass * 24, 4096));
    }
    return changed;
  }

  refreshAccountPersistedState(force = false) {
    const readPath = this.accountPersistReadPath || this.accountPersistPath;
    if (!readPath) return false;
    const now = Date.now();
    if (
      !force &&
      now - Number(this.lastAccountStateRefreshAt || 0) < this.accountStateRefreshMs
    ) {
      return false;
    }
    this.lastAccountStateRefreshAt = now;
    let stat = null;
    try {
      stat = fs.statSync(readPath);
    } catch (_error) {
      return false;
    }
    const mtimeMs = Number(stat && stat.mtimeMs ? stat.mtimeMs : 0) || 0;
    if (!force && mtimeMs <= Number(this.accountPersistMtimeMs || 0)) {
      return false;
    }
    const payload = readJson(readPath, null);
    if (!payload || typeof payload !== "object") return false;
    const generatedAt = Math.max(0, Number(payload.generatedAt || 0));
    if (!force && generatedAt > 0 && generatedAt <= Number(this.accountPersistGeneratedAt || 0)) {
      this.accountPersistMtimeMs = mtimeMs;
      return false;
    }
    const changed = this.mergeAccountStatePayload(payload);
    this.accountPersistMtimeMs = mtimeMs;
    this.accountPersistGeneratedAt = Math.max(
      Number(this.accountPersistGeneratedAt || 0),
      generatedAt
    );
    return changed;
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

  seedPriorityWalletsFromState(limit = 0) {
    const candidates = [];
    for (const [wallet, liveState] of this.livePositionStates.entries()) {
      if (!wallet || !this.walletBelongsToShard(wallet) || !liveState) continue;
      const state = String(liveState.state || "").trim();
      const accountPositionsCount = Math.max(
        0,
        Number(liveState.accountPositionsCount || 0)
      );
      const materializedPositionsCount = Math.max(
        0,
        Number(liveState.materializedPositionsCount || 0)
      );
      if (
        state === "closed" &&
        !liveState.pendingBootstrap &&
        accountPositionsCount <= 0 &&
        materializedPositionsCount <= 0
      ) {
        continue;
      }
      candidates.push(wallet);
    }
    candidates.sort((a, b) => this.priorityQueueScore(b) - this.priorityQueueScore(a));
    const safeLimit =
      Number(limit || 0) > 0
        ? Math.min(candidates.length, Math.max(1, Math.floor(Number(limit || 0))))
        : candidates.length;
    for (let i = 0; i < safeLimit; i += 1) {
      this.enqueuePriorityWallet(candidates[i]);
    }
  }

  noteAccountOpenHint(wallet, positionsCount = 0, meta = {}) {
    const normalized = normalizeWallet(wallet);
    const count = Math.max(0, Number(positionsCount || 0));
    if (!normalized) return null;
    if (count <= 0) {
      this.clearAccountOpenHint(normalized);
      return null;
    }
    const snapshot = this.walletSnapshots.get(normalized) || null;
    const existing = this.accountOpenHints.get(normalized) || null;
    const materializedPositions = Math.max(
      0,
      Number(
        meta.materializedPositions !== undefined
          ? meta.materializedPositions
          : snapshot && Array.isArray(snapshot.positions)
          ? snapshot.positions.length
          : existing && existing.materializedPositions
          ? existing.materializedPositions
          : 0
      ) || 0
    );
    this.accountOpenHints.set(normalized, {
      wallet: normalized,
      positionsCount: count,
      updatedAt: Number(meta.updatedAt || (existing && existing.updatedAt) || Date.now()) || Date.now(),
      lastAccountAt:
        Number(meta.observedAt || (existing && existing.lastAccountAt) || Date.now()) || Date.now(),
      lastPositionsRefreshAt:
        Number(meta.lastPositionsRefreshAt || (existing && existing.lastPositionsRefreshAt) || 0) || 0,
      materializedPositions,
      missCount: Math.max(
        0,
        Number(
          meta.missCount !== undefined
            ? meta.missCount
            : existing && existing.missCount
            ? existing.missCount
            : 0
        ) || 0
      ),
      lastReason:
        meta.reason !== undefined
          ? String(meta.reason || "")
          : existing && existing.lastReason
          ? String(existing.lastReason)
          : "account_positions_count",
    });
    const state = this.ensureLivePositionState(normalized);
    const nextState =
      Math.max(materializedPositions, snapshot && Array.isArray(snapshot.positions) ? snapshot.positions.length : 0) > 0
        ? "materialized_open"
        : "hinted_open";
    this.transitionLivePositionState(normalized, nextState, {
      at: Number(meta.observedAt || Date.now()) || Date.now(),
      lastAccountAt: Number(meta.observedAt || Date.now()) || Date.now(),
      accountPositionsCount: count,
      materializedPositionsCount: Math.max(
        0,
        Number(
          snapshot && Array.isArray(snapshot.positions)
            ? snapshot.positions.length
            : materializedPositions
        ) || 0
      ),
      accountZeroStreak: 0,
      pendingBootstrap:
        Math.max(
          0,
          Number(
            snapshot && Array.isArray(snapshot.positions)
              ? snapshot.positions.length
              : materializedPositions
          ) || 0
        ) < count,
      lastHintOpenAt: Number(meta.observedAt || Date.now()) || Date.now(),
      reason: meta.reason || (state && state.lastReason) || "account_positions_count",
    });
    this.enqueuePriorityWallet(normalized);
    this.recomputeAccountOpenHintStatus();
    this.recomputeLivePositionStateStatus();
    return this.accountOpenHints.get(normalized) || null;
  }

  clearAccountOpenHint(wallet) {
    const normalized = normalizeWallet(wallet);
    if (!normalized) return false;
    const changed = this.accountOpenHints.delete(normalized);
    if (changed) {
      this.recomputeAccountOpenHintStatus();
      this.recomputeLivePositionStateStatus();
    }
    return changed;
  }

  recomputeAccountOpenHintStatus() {
    let hintedWallets = 0;
    let hintedPositions = 0;
    let gapWallets = 0;
    let gapPositions = 0;
    for (const [wallet, hint] of this.accountOpenHints.entries()) {
      if (!this.walletBelongsToShard(wallet)) continue;
      const positionsCount = Math.max(0, Number(hint && hint.positionsCount ? hint.positionsCount : 0));
      if (positionsCount <= 0) continue;
      hintedWallets += 1;
      hintedPositions += positionsCount;
      const snapshot = this.walletSnapshots.get(wallet) || null;
      const materialized = Math.max(
        0,
        Number(
          snapshot && Array.isArray(snapshot.positions)
            ? snapshot.positions.length
            : hint && hint.materializedPositions
            ? hint.materializedPositions
            : 0
        ) || 0
      );
      const gap = Math.max(0, positionsCount - materialized);
      if (gap > 0) {
        gapWallets += 1;
        gapPositions += gap;
      }
    }
    this.status.accountIndicatedOpenWallets = hintedWallets;
    this.status.accountIndicatedOpenPositionsTotal = hintedPositions;
    this.status.positionMaterializationGapWallets = gapWallets;
    this.status.positionMaterializationGapTotal = gapPositions;
  }

  priorityQueueScore(wallet) {
    const normalized = normalizeWallet(wallet);
    if (!normalized) return Number.NEGATIVE_INFINITY;
    const priorityMeta = this.walletPriorityMeta.get(normalized) || null;
    const liveState = this.livePositionStates.get(normalized) || null;
    const openHint = this.accountOpenHints.get(normalized) || null;
    const hintCount = Math.max(0, Number(openHint && openHint.positionsCount || 0));
    const priorityBoost = Math.max(
      0,
      Number(priorityMeta && priorityMeta.priorityBoost || 0)
    );
    const recentVolumeUsd = Math.max(
      0,
      Number(priorityMeta && priorityMeta.recentVolumeUsd || 0)
    );
    const recentTradeCount = Math.max(
      0,
      Number(priorityMeta && priorityMeta.recentTradeCount || 0)
    );
    const pendingBootstrap = Boolean(liveState && liveState.pendingBootstrap);
    const lastMaterializedOpenAt = Math.max(
      0,
      Number(liveState && liveState.lastMaterializedOpenAt || 0)
    );
    const lastTouchAt = Math.max(
      Number(openHint && openHint.updatedAt || 0) || 0,
      Number(liveState && (liveState.lastPositionsAt || liveState.lastAccountAt) || 0) || 0,
      Number(priorityMeta && priorityMeta.rankTs || 0) || 0
    );
    return (
      (pendingBootstrap ? 50_000_000 : 0) +
      hintCount * 1_000_000 +
      (lastMaterializedOpenAt > 0 ? 5_000_000 : 0) +
      priorityBoost * 10 +
      Math.round(Math.sqrt(recentVolumeUsd)) * 100 +
      recentTradeCount * 100 +
      Math.round(lastTouchAt / 1000)
    );
  }

  drainPriorityWallets(target, seen, picked) {
    if (this.priorityWalletQueue.length > 1) {
      const queued = Array.from(
        new Set(
          this.priorityWalletQueue
            .map((wallet) => normalizeWallet(wallet))
            .filter(Boolean)
        )
      );
      queued.sort((a, b) => this.priorityQueueScore(b) - this.priorityQueueScore(a));
      this.priorityWalletQueue = queued;
      this.priorityWalletSet = new Set(queued);
    }
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
          pinMs: Math.max(this.tradeHoldMs || 0, Math.floor(this.hotReconcileMaxAgeMs / 2)),
        });
      }
    });
  }

  promoteHighValueWallets(now = Date.now()) {
    if (!this.hotWalletWsMonitor || typeof this.hotWalletWsMonitor.promoteWallet !== "function") {
      return;
    }
    if (!this.highValueWalletList.length) return;
    const target = Math.min(
      Math.max(Math.floor(this.hotWalletsPerPass / 2), Math.floor(this.recentActiveWalletsPerPass / 2), 48),
      96
    );
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
      this.enqueuePriorityWallet(wallet);
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
            ? this.hotReconcileMaxAgeMs * 3
            : Math.max(this.tradeHoldMs || 0, Math.floor(this.warmReconcileMaxAgeMs / 2)),
      });
    });
  }

  promotePublicActiveWallets(now = Date.now()) {
    if (!this.hotWalletWsMonitor || typeof this.hotWalletWsMonitor.promoteWallet !== "function") {
      return;
    }
    if (!this.publicActiveWalletList.length) return;
    const target = Math.min(Math.max(Math.floor(this.hotWalletsPerPass / 3), 32), 64);
    const picked = [];
    const seen = new Set();
    this.pickRotatingWallets(
      this.publicActiveWalletList,
      Math.min(target, this.publicActiveWalletList.length),
      "publicActiveWalletCursor",
      seen,
      picked
    );
    picked.forEach((wallet, idx) => {
      const meta = this.walletPriorityMeta.get(wallet) || null;
      this.enqueuePriorityWallet(wallet);
      this.hotWalletWsMonitor.promoteWallet(wallet, {
        at: now,
        reason: "public_active_wallet",
        forceTier: Number(meta && meta.openPositions || 0) > 0 ? "hot" : "warm",
        openPositions: Number(meta && meta.openPositions || 0) > 0,
        recentVolumeUsd: meta ? meta.recentVolumeUsd : 0,
        recentTradeCount: meta ? meta.recentTradeCount : 0,
        liveActiveRank: idx + 1,
        priorityBoost: Math.max(
          35_000,
          Number(meta && meta.priorityBoost ? meta.priorityBoost : 0) + 20_000
        ),
        pinMs:
          Number(meta && meta.openPositions || 0) > 0
            ? this.hotReconcileMaxAgeMs * 3
            : Math.max(this.tradeHoldMs || 0, Math.floor(this.warmReconcileMaxAgeMs / 2)),
      });
    });
  }

  promoteOpenWallets(now = Date.now()) {
    if (!this.hotWalletWsMonitor || typeof this.hotWalletWsMonitor.promoteWallet !== "function") {
      return;
    }
    if (!this.openWalletSet.size) return;
    const openWallets = Array.from(this.openWalletSet);
    const target = Math.min(
      openWallets.length,
      Math.max(this.hotWalletsPerPass * 2, this.recentActiveWalletsPerPass, 128)
    );
    const picked = [];
    const seen = new Set();
    this.pickRotatingWallets(
      openWallets,
      Math.min(target, openWallets.length),
      "openWalletCursor",
      seen,
      picked
    );
    picked.forEach((wallet) => {
      const meta = this.walletPriorityMeta.get(wallet) || null;
      this.enqueuePriorityWallet(wallet);
      this.hotWalletWsMonitor.promoteWallet(wallet, {
        at: now,
        reason: "known_open_wallet",
        forceTier: "hot",
        openPositions: true,
        recentVolumeUsd: meta ? meta.recentVolumeUsd : 0,
        recentTradeCount: meta ? meta.recentTradeCount : 0,
        liveActiveRank: meta ? meta.liveActiveRank : 0,
        priorityBoost: meta ? meta.priorityBoost : 0,
        pinMs: this.hotReconcileMaxAgeMs * 4,
      });
    });
  }

  seedHotWalletTier() {
    if (!this.hotWalletWsMonitor || typeof this.hotWalletWsMonitor.promoteWallet !== "function") {
      return;
    }
    this.ensureWalletList();
    const now = Date.now();
    this.seedPriorityWalletsFromState(Math.max(this.hotWalletsPerPass * 24, 4096));
    const openWallets = Array.from(this.openWalletSet);
    openWallets.forEach((wallet) => {
      const priorityMeta = this.walletPriorityMeta.get(wallet) || null;
      this.enqueuePriorityWallet(wallet);
      this.hotWalletWsMonitor.promoteWallet(wallet, {
        at: now,
        reason: "seed_open_position",
        openPositions: true,
        recentVolumeUsd: priorityMeta ? priorityMeta.recentVolumeUsd : 0,
        recentTradeCount: priorityMeta ? priorityMeta.recentTradeCount : 0,
        liveActiveRank: priorityMeta ? priorityMeta.liveActiveRank : 0,
        priorityBoost: priorityMeta ? priorityMeta.priorityBoost : 0,
        pinMs: this.hotReconcileMaxAgeMs * 4,
      });
    });
    const hintedWallets = Array.from(this.accountOpenHints.values())
      .filter((row) => row && this.walletBelongsToShard(row.wallet))
      .sort((a, b) => {
        const aCount = Math.max(0, Number(a && a.positionsCount ? a.positionsCount : 0));
        const bCount = Math.max(0, Number(b && b.positionsCount ? b.positionsCount : 0));
        if (bCount !== aCount) return bCount - aCount;
        const aAt = Math.max(0, Number(a && (a.updatedAt || a.lastAccountAt) ? a.updatedAt || a.lastAccountAt : 0));
        const bAt = Math.max(0, Number(b && (b.updatedAt || b.lastAccountAt) ? b.updatedAt || b.lastAccountAt : 0));
        return bAt - aAt;
      })
      .slice(0, Math.max(this.hotWalletsPerPass * 12, 2048))
      .map((row) => row.wallet);
    hintedWallets.forEach((wallet) => {
      const priorityMeta = this.walletPriorityMeta.get(wallet) || null;
      const hint = this.accountOpenHints.get(wallet) || null;
      this.enqueuePriorityWallet(wallet);
      this.hotWalletWsMonitor.promoteWallet(wallet, {
        at: now,
        reason: "seed_account_hint",
        forceTier: "hot",
        openPositions: Math.max(0, Number(hint && hint.positionsCount || 0)) > 0,
        recentVolumeUsd: priorityMeta ? priorityMeta.recentVolumeUsd : 0,
        recentTradeCount: priorityMeta ? priorityMeta.recentTradeCount : 0,
        liveActiveRank: priorityMeta ? priorityMeta.liveActiveRank : 0,
        priorityBoost: priorityMeta ? priorityMeta.priorityBoost : 0,
        pinMs: this.hotReconcileMaxAgeMs * 4,
      });
    });
    if (this.subscribeAllWallets) {
      this.walletList.forEach((wallet) => {
        const priorityMeta = this.walletPriorityMeta.get(wallet) || null;
        this.hotWalletWsMonitor.promoteWallet(wallet, {
          at: now,
          reason: "seed_full_shard_ws",
          forceTier:
            Number(priorityMeta && priorityMeta.openPositions ? priorityMeta.openPositions : 0) > 0
              ? "hot"
              : "warm",
          openPositions:
            Number(priorityMeta && priorityMeta.openPositions ? priorityMeta.openPositions : 0) > 0,
          recentVolumeUsd: priorityMeta ? priorityMeta.recentVolumeUsd : 0,
          recentTradeCount: priorityMeta ? priorityMeta.recentTradeCount : 0,
          liveActiveRank: priorityMeta ? priorityMeta.liveActiveRank : 0,
          priorityBoost: Math.max(
            10000,
            Number(priorityMeta && priorityMeta.priorityBoost ? priorityMeta.priorityBoost : 0)
          ),
          pinMs: Math.max(this.tradeHoldMs || 0, Math.floor(this.warmReconcileMaxAgeMs / 2)),
        });
      });
    }
    this.ensurePublicActiveWalletList();
    this.promoteHighValueWallets(now);
    this.promotePublicActiveWallets(now);
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

  prepareWalletQueues(now = Date.now()) {
    this.ensureWalletList();
    this.ensurePublicActiveWalletList();
    this.consumeExternalTriggers(now);
    if (!this.walletList.length) {
      this.status.recentActiveWallets = 0;
      this.status.warmWallets = this.warmWalletList.length;
      this.status.highValueWallets = this.highValueWalletList.length;
      this.status.publicActiveWallets = this.publicActiveWalletList.length;
      this.status.priorityQueueDepth = this.priorityWalletQueue.length;
      this.status.hotReconcileDueWallets = 0;
      this.status.warmReconcileDueWallets = 0;
      return {
        hotDueWallets: [],
        warmDueWallets: [],
        recentActiveWallets: [],
      };
    }
    this.pruneRecentActiveWallets(now);
    this.promoteOpenWallets(now);
    this.promotePublicActiveWallets(now);
    this.promoteHighValueWallets(now);
    const hotDueWallets = this.buildDueWalletList("hot", this.hotReconcileMaxAgeMs, now);
    const warmDueWallets = this.buildDueWalletList("warm", this.warmReconcileMaxAgeMs, now);
    const recentActiveWallets = Array.from(this.recentActiveWalletAt.entries())
      .sort((a, b) => Number(b[1] || 0) - Number(a[1] || 0))
      .map(([wallet]) => wallet);
    this.status.recentActiveWallets = recentActiveWallets.length;
    this.status.warmWallets = this.warmWalletList.length;
    this.status.highValueWallets = this.highValueWalletList.length;
    this.status.publicActiveWallets = this.publicActiveWalletList.length;
    this.status.priorityQueueDepth = this.priorityWalletQueue.length;
    this.status.hotReconcileDueWallets = hotDueWallets.length;
    this.status.warmReconcileDueWallets = warmDueWallets.length;
    return {
      hotDueWallets,
      warmDueWallets,
      recentActiveWallets,
    };
  }

  pickHotRefreshWallets(target = 0, seedSeen = null) {
    const now = Date.now();
    const { hotDueWallets, warmDueWallets, recentActiveWallets } = this.prepareWalletQueues(now);
    if (!this.walletList.length) return [];
    const safeTarget = Math.max(
      0,
      Math.min(
        this.walletList.length,
        Number(target || Math.max(this.hotWalletsPerPass, this.recentActiveWalletsPerPass, 64))
      )
    );
    if (!safeTarget) return [];
    const picked = [];
    const seen = seedSeen instanceof Set ? new Set(seedSeen) : new Set();

    this.drainPriorityWallets(safeTarget, seen, picked);

    if (picked.length < safeTarget && hotDueWallets.length > 0 && this.hotWalletsPerPass > 0) {
      this.pickTopWallets(
        hotDueWallets,
        Math.min(safeTarget - picked.length, this.hotWalletsPerPass, hotDueWallets.length),
        seen,
        picked
      );
    }

    if (
      picked.length < safeTarget &&
      recentActiveWallets.length > 0 &&
      this.recentActiveWalletsPerPass > 0
    ) {
      this.pickRotatingWallets(
        recentActiveWallets,
        Math.min(
          safeTarget - picked.length,
          this.recentActiveWalletsPerPass,
          recentActiveWallets.length
        ),
        "recentActiveWalletCursor",
        seen,
        picked
      );
    }

    if (picked.length < safeTarget && this.openWalletSet.size > 0) {
      this.pickRotatingWallets(
        Array.from(this.openWalletSet),
        Math.min(safeTarget - picked.length, this.openWalletSet.size),
        "openWalletCursor",
        seen,
        picked
      );
    }

    if (
      picked.length < safeTarget &&
      this.publicActiveWalletList.length > 0 &&
      this.hotWalletsPerPass > 0
    ) {
      this.pickRotatingWallets(
        this.publicActiveWalletList,
        Math.min(safeTarget - picked.length, this.hotWalletsPerPass, this.publicActiveWalletList.length),
        "publicActiveWalletCursor",
        seen,
        picked
      );
    }

    if (picked.length < safeTarget && warmDueWallets.length > 0 && this.warmWalletsPerPass > 0) {
      this.pickTopWallets(
        warmDueWallets,
        Math.min(safeTarget - picked.length, this.warmWalletsPerPass, warmDueWallets.length),
        seen,
        picked
      );
    }

    return picked;
  }

  async runWalletBatch(list = [], workerCount = 0, runner) {
    if (!Array.isArray(list) || !list.length || workerCount <= 0 || typeof runner !== "function") {
      return;
    }
    let cursor = 0;
    const workers = Array.from({ length: workerCount }).map(async () => {
      while (true) {
        const index = cursor;
        cursor += 1;
        if (index >= list.length) break;
        const wallet = list[index];
        if (!wallet) continue;
        // eslint-disable-next-line no-await-in-loop
        await runner.call(this, wallet);
      }
    });
    await Promise.all(workers);
  }

  computeGapRescueTarget(availableSlots = 0, bootstrapMode = false) {
    const slots = Math.max(0, Number(availableSlots || 0));
    if (slots <= 0) return 0;
    if (this.workerMode === "account_census") return 0;
    const gapWallets = Math.max(
      0,
      Number(this.status.positionMaterializationGapWallets || 0),
      Number(this.status.accountRepairDueWallets || 0),
      Number(this.status.liveStatePendingBootstrapWallets || 0)
    );
    if (gapWallets <= 0) return 0;
    const shareFloor = bootstrapMode ? Math.max(this.gapRescueShare, 0.55) : this.gapRescueShare;
    const target = Math.max(
      this.gapRescueMinWorkers,
      Math.floor(slots * shareFloor)
    );
    return Math.max(1, Math.min(slots, gapWallets, target));
  }

  walletNeedsFullScan(wallet, now = Date.now()) {
    const normalizedWallet = normalizeWallet(wallet);
    if (!normalizedWallet) return false;
    const liveState = this.livePositionStates.get(normalizedWallet) || null;
    const openHint = this.accountOpenHints.get(normalizedWallet) || null;
    const snapshot = this.walletSnapshots.get(normalizedWallet) || null;
    const hintedCount = Math.max(
      0,
      Number(
        openHint && openHint.positionsCount !== undefined
          ? openHint.positionsCount
          : liveState && liveState.accountPositionsCount
      ) || 0
    );
    if (!liveState && hintedCount <= 0) return true;
    const lastAccountAt = Math.max(
      0,
      Number(liveState && liveState.lastAccountAt ? liveState.lastAccountAt : 0),
      Number(openHint && openHint.lastAccountAt ? openHint.lastAccountAt : 0),
      Number(this.accountSnapshots.get(normalizedWallet)?.scannedAt || 0)
    );
    if (lastAccountAt <= 0) return true;
    const accountAgeMs = Math.max(0, now - lastAccountAt);
    const accountFreshnessThresholdMs = Math.max(
      this.scanIntervalMs * 4,
      Math.floor(this.hotReconcileMaxAgeMs / 2)
    );
    if (accountAgeMs > accountFreshnessThresholdMs) return true;
    const accountPositionsCount = Math.max(0, Number(liveState.accountPositionsCount || 0));
    const materializedPositionsCount = Math.max(
      0,
      Number(
        snapshot && Array.isArray(snapshot.positions)
          ? snapshot.positions.length
          : liveState && liveState.materializedPositionsCount
      ) || 0,
      Number(openHint && openHint.materializedPositions ? openHint.materializedPositions : 0)
    );
    if (Math.max(accountPositionsCount, hintedCount) > materializedPositionsCount) return false;
    if (Boolean(liveState && liveState.pendingBootstrap)) return false;
    if (String(liveState && liveState.state || "") === "hinted_open") return false;
    if (String(liveState && liveState.state || "") === "uncertain") return false;
    if (
      String(liveState && liveState.state || "") === "closed" &&
      Math.max(0, Number(liveState && liveState.lastMaterializedOpenAt || 0)) > 0
    ) {
      return false;
    }
    return false;
  }

  splitWalletsByScanNeed(wallets = [], now = Date.now()) {
    const scan = [];
    const refresh = [];
    for (const wallet of Array.isArray(wallets) ? wallets : []) {
      if (!wallet) continue;
      if (this.walletNeedsFullScan(wallet, now)) scan.push(wallet);
      else refresh.push(wallet);
    }
    return { scan, refresh };
  }

  pickDiscoveryWallets(target = 0, seedSeen = null) {
    const now = Date.now();
    this.prepareWalletQueues(now);
    if (!this.walletList.length) return [];
    const safeTarget = Math.max(
      0,
      Math.min(this.walletList.length, Number(target || this.computeBatchTarget()))
    );
    if (!safeTarget) return [];
    const picked = [];
    const seen = seedSeen instanceof Set ? new Set(seedSeen) : new Set();
    const maxAttempts = this.walletList.length * 2;
    let attempts = 0;
    while (picked.length < safeTarget && attempts < maxAttempts && this.walletList.length > 0) {
      const wallet = this.walletList[this.walletCursor];
      this.walletCursor = (this.walletCursor + 1) % this.walletList.length;
      attempts += 1;
      if (!wallet || seen.has(wallet)) continue;
      seen.add(wallet);
      picked.push(wallet);
    }
    this.status.queueCursor = this.walletCursor;
    this.status.batchTargetWallets = safeTarget;
    return picked;
  }

  pickPositionsBootstrapWallets(target = 0, seedSeen = null) {
    this.ensureWalletList();
    this.ensurePublicActiveWalletList();
    const safeTarget = Math.max(0, Math.min(this.walletList.length, Number(target || 0)));
    if (!safeTarget || !this.walletList.length) return [];
    const seen = seedSeen instanceof Set ? new Set(seedSeen) : new Set();
    const picked = [];
    const hintedWallets = Array.from(this.accountOpenHints.values())
      .filter((row) => row && this.walletBelongsToShard(row.wallet))
      .sort((a, b) => {
        const aCount = Math.max(0, Number(a && a.positionsCount ? a.positionsCount : 0));
        const bCount = Math.max(0, Number(b && b.positionsCount ? b.positionsCount : 0));
        if (bCount !== aCount) return bCount - aCount;
        const aAt = Math.max(0, Number(a && (a.updatedAt || a.lastAccountAt) ? a.updatedAt || a.lastAccountAt : 0));
        const bAt = Math.max(0, Number(b && (b.updatedAt || b.lastAccountAt) ? b.updatedAt || b.lastAccountAt : 0));
        return bAt - aAt;
      })
      .map((row) => row.wallet);
    const materializedOpenWallets = Array.from(this.openWalletSet).filter((wallet) =>
      this.walletBelongsToShard(wallet)
    );
    const priorityWarmWallets = this.highValueWalletList.length
      ? this.highValueWalletList
      : this.warmWalletList;

    if (picked.length < safeTarget && hintedWallets.length > 0) {
      this.pickTopWallets(
        hintedWallets,
        Math.min(safeTarget - picked.length, hintedWallets.length),
        seen,
        picked
      );
    }

    if (picked.length < safeTarget && materializedOpenWallets.length > 0) {
      this.pickRotatingWallets(
        materializedOpenWallets,
        Math.min(safeTarget - picked.length, materializedOpenWallets.length),
        "openWalletCursor",
        seen,
        picked
      );
    }

    if (picked.length < safeTarget && this.publicActiveWalletList.length > 0) {
      this.pickRotatingWallets(
        this.publicActiveWalletList,
        Math.min(safeTarget - picked.length, this.publicActiveWalletList.length),
        "publicActiveWalletCursor",
        seen,
        picked
      );
    }

    if (picked.length < safeTarget && priorityWarmWallets.length > 0) {
      this.pickRotatingWallets(
        priorityWarmWallets,
        Math.min(safeTarget - picked.length, priorityWarmWallets.length),
        "highValueWalletCursor",
        seen,
        picked
      );
    }

    if (picked.length < safeTarget && this.warmWalletList.length > 0) {
      this.pickRotatingWallets(
        this.warmWalletList,
        Math.min(safeTarget - picked.length, this.warmWalletList.length),
        "warmWalletCursor",
        seen,
        picked
      );
    }

    if (picked.length < safeTarget) {
      this.pickRotatingWallets(
        this.walletList,
        safeTarget - picked.length,
        "positionsBootstrapCursor",
        seen,
        picked
      );
    }
    return picked;
  }

  pickAccountHintWallets(target = 0, seedSeen = null) {
    this.recomputeAccountOpenHintStatus();
    this.recomputeLivePositionStateStatus();
    const safeTarget = Math.max(0, Number(target || 0));
    if (!safeTarget) {
      this.status.accountRepairDueWallets = Math.max(
        0,
        Number(this.status.positionMaterializationGapWallets || 0)
      );
      return [];
    }
    const seen = seedSeen instanceof Set ? new Set(seedSeen) : new Set();
    const candidates = [];
    for (const [wallet, liveState] of this.livePositionStates.entries()) {
      if (!wallet || seen.has(wallet) || !this.walletBelongsToShard(wallet) || !liveState) continue;
      const hint = this.accountOpenHints.get(wallet) || null;
      const positionsCount = Math.max(
        0,
        Number(
          hint && hint.positionsCount !== undefined
            ? hint.positionsCount
            : liveState.accountPositionsCount
        ) || 0
      );
      const snapshot = this.walletSnapshots.get(wallet) || null;
      const materialized = Math.max(
        0,
        Number(
          snapshot && Array.isArray(snapshot.positions)
            ? snapshot.positions.length
            : liveState.materializedPositionsCount
        ) || 0
      );
      const gap = Math.max(0, positionsCount - materialized);
      const unresolved =
        gap > 0 ||
        Boolean(liveState.pendingBootstrap) ||
        String(liveState.state || "") === "hinted_open" ||
        String(liveState.state || "") === "uncertain";
      if (!unresolved) continue;
      candidates.push({
        wallet,
        gap,
        positionsCount,
        materialized,
        lastPositionsRefreshAt:
          Number(
            hint && hint.lastPositionsRefreshAt
              ? hint.lastPositionsRefreshAt
              : liveState.lastPositionsAt
              ? liveState.lastPositionsAt
              : snapshot && (snapshot.lastSuccessAt || snapshot.scannedAt)
              ? snapshot.lastSuccessAt || snapshot.scannedAt
              : 0
          ) || 0,
        updatedAt:
          Number(hint && hint.updatedAt ? hint.updatedAt : liveState.lastAccountAt || 0) || 0,
        missCount: Math.max(
          0,
          Number(
            hint && hint.missCount !== undefined
              ? hint.missCount
              : liveState.positionsEmptyStreak
          ) || 0
        ),
        statePriority:
          String(liveState.state || "") === "hinted_open"
            ? 3
            : String(liveState.state || "") === "uncertain"
            ? 2
            : String(liveState.state || "") === "materialized_open"
            ? 1
            : 0,
        pendingBootstrap: Boolean(liveState.pendingBootstrap),
      });
    }
    candidates.sort((a, b) => {
      if (Number(Boolean(b.pendingBootstrap)) !== Number(Boolean(a.pendingBootstrap))) {
        return Number(Boolean(b.pendingBootstrap)) - Number(Boolean(a.pendingBootstrap));
      }
      if (b.gap !== a.gap) return b.gap - a.gap;
      if (b.statePriority !== a.statePriority) return b.statePriority - a.statePriority;
      if (a.lastPositionsRefreshAt !== b.lastPositionsRefreshAt) {
        return a.lastPositionsRefreshAt - b.lastPositionsRefreshAt;
      }
      if (b.missCount !== a.missCount) return b.missCount - a.missCount;
      if (a.updatedAt !== b.updatedAt) return a.updatedAt - b.updatedAt;
      return a.wallet.localeCompare(b.wallet);
    });
    this.status.accountRepairDueWallets = candidates.length;
    return candidates.slice(0, safeTarget).map((row) => row.wallet);
  }

  pickBatchWallets(availableSlots = 0) {
    this.ensureWalletList();
    this.ensurePublicActiveWalletList();
    this.consumeExternalTriggers();
    if (!this.walletList.length) return [];
    const now = Date.now();
    this.pruneRecentActiveWallets(now);
    this.promoteOpenWallets(now);
    this.promotePublicActiveWallets(now);
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

    if (picked.length < target && this.publicActiveWalletList.length > 0 && this.hotWalletsPerPass > 0) {
      this.pickRotatingWallets(
        this.publicActiveWalletList,
        Math.min(target - picked.length, this.hotWalletsPerPass, this.publicActiveWalletList.length),
        "publicActiveWalletCursor",
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
    this.status.publicActiveWallets = this.publicActiveWalletList.length;
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
      const sinceLastUseMs = Math.max(0, now - Number(state.lastUsedAt || 0));
      const recencyPenalty = sinceLastUseMs <= 0 ? 40 : Math.max(0, Math.round((250 - Math.min(250, sinceLastUseMs)) / 5));
      const score =
        inflight * 100 + latencyPenalty + timeoutPenalty + rateLimitPenalty + healthPenalty + recencyPenalty;
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
      const timeoutCount = Number(state.timeoutCount || 0);
      const proxyFailureCount = Number(state.proxyFailureCount || 0);
      const consecutiveTimeout = Number(state.consecutiveTimeout || 0);
      if (consecutiveTimeout >= this.clientHardDisableConsecutiveTimeouts) {
        state.disabledUntil = now + this.clientHardDisableMs;
        state.cooldownUntil = Math.max(Number(state.cooldownUntil || 0), state.disabledUntil);
        return;
      }
      if (proxyFailureCount >= this.clientHardDisableConsecutiveProxyFailures && successes <= 0) {
        state.disabledUntil = now + this.clientHardDisableMs;
        state.cooldownUntil = Math.max(Number(state.cooldownUntil || 0), state.disabledUntil);
        return;
      }
      if (consecutiveTimeout >= this.clientSoftDisableConsecutiveTimeouts) {
        state.disabledUntil = now + this.clientSoftDisableMs;
        state.cooldownUntil = Math.max(Number(state.cooldownUntil || 0), state.disabledUntil);
        return;
      }
      if (proxyFailureCount >= this.clientSoftDisableConsecutiveProxyFailures && successes <= 1) {
        state.disabledUntil = now + this.clientSoftDisableMs;
        state.cooldownUntil = Math.max(Number(state.cooldownUntil || 0), state.disabledUntil);
        return;
      }
      if (requests < this.clientDisableMinRequests) return;
      const successRate = requests > 0 ? successes / requests : 0;
      if (successes === 0 && failures >= this.clientDisableMinRequests) {
        state.disabledUntil = now + this.clientHardDisableMs;
        state.cooldownUntil = Math.max(Number(state.cooldownUntil || 0), state.disabledUntil);
        return;
      }
      if (
        requests >= this.clientDisableMinRequests * 2 &&
        successRate < this.clientDisableLowSuccessRate &&
        (timeoutCount >= 2 || proxyFailureCount >= 2)
      ) {
        state.disabledUntil = now + this.clientHardDisableMs;
        state.cooldownUntil = Math.max(Number(state.cooldownUntil || 0), state.disabledUntil);
        return;
      }
      if (requests >= 160 && failures > successes * 4 && successRate < 0.15) {
        state.disabledUntil = now + this.clientSoftDisableMs;
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
      if (Number(state.timeoutCount || 0) > 0) {
        state.timeoutCount = Math.max(0, Number(state.timeoutCount || 0) - 1);
      }
      if (Number(state.proxyFailureCount || 0) > 0) {
        state.proxyFailureCount = Math.max(0, Number(state.proxyFailureCount || 0) - 1);
      }
      if (Number(state.rateLimitCount || 0) > 0) {
        state.rateLimitCount = Math.max(0, Number(state.rateLimitCount || 0) - 1);
      }
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
      if (state.consecutiveTimeout >= this.clientHardDisableConsecutiveTimeouts) {
        state.disabledUntil = now + this.clientHardDisableMs;
        state.cooldownUntil = Math.max(Number(state.cooldownUntil || 0), state.disabledUntil);
      } else if (state.consecutiveTimeout >= this.clientSoftDisableConsecutiveTimeouts) {
        state.disabledUntil = now + this.clientSoftDisableMs;
        state.cooldownUntil = Math.max(Number(state.cooldownUntil || 0), state.disabledUntil);
      }
      maybeDisableUnhealthyClient();
      return;
    }

    if (message.toLowerCase().includes("failed to connect") || message.toLowerCase().includes("proxy")) {
      state.proxyFailureCount = Number(state.proxyFailureCount || 0) + 1;
      state.consecutiveTimeout = Math.min(10, Number(state.consecutiveTimeout || 0) + 1);
      state.cooldownUntil = now + Math.min(10000, Math.max(3000, this.rateLimitBackoffBaseMs));
      if (state.proxyFailureCount >= this.clientHardDisableConsecutiveProxyFailures) {
        state.disabledUntil = now + this.clientHardDisableMs;
        state.cooldownUntil = Math.max(Number(state.cooldownUntil || 0), state.disabledUntil);
      } else if (state.proxyFailureCount >= this.clientSoftDisableConsecutiveProxyFailures) {
        state.disabledUntil = now + this.clientSoftDisableMs;
        state.cooldownUntil = Math.max(Number(state.cooldownUntil || 0), state.disabledUntil);
      }
      maybeDisableUnhealthyClient();
      return;
    }
    maybeDisableUnhealthyClient();
  }

  getCachedWalletAccountSettings(wallet, nowMs = Date.now()) {
    const normalizedWallet = normalizeWallet(wallet);
    if (!normalizedWallet) return null;
    const cached = this.accountSettingsCache.get(normalizedWallet);
    if (!cached || typeof cached !== "object") return null;
    const expiresAt = Number(cached.expiresAt || 0);
    if (expiresAt > 0 && expiresAt <= nowMs) {
      this.accountSettingsCache.delete(normalizedWallet);
      return null;
    }
    if (!cached.settingsBySymbol || !(cached.settingsBySymbol instanceof Map)) return null;
    this.accountSettingsCacheHits = Number(this.accountSettingsCacheHits || 0) + 1;
    return cached.settingsBySymbol;
  }

  setWalletAccountSettingsCache(wallet, settingsBySymbol, options = {}) {
    const normalizedWallet = normalizeWallet(wallet);
    if (!normalizedWallet) return;
    const nowMs = Math.max(0, Number(options.nowMs || Date.now()));
    const ttlMs = Math.max(
      1000,
      Number(options.ttlMs || this.accountSettingsCacheTtlMs || 60 * 1000)
    );
    this.accountSettingsCache.set(normalizedWallet, {
      wallet: normalizedWallet,
      settingsBySymbol:
        settingsBySymbol instanceof Map ? settingsBySymbol : new Map(),
      updatedAt: nowMs,
      expiresAt: nowMs + ttlMs,
    });
    if (this.accountSettingsCache.size > this.accountSettingsCacheMax) {
      const entries = Array.from(this.accountSettingsCache.entries()).sort(
        (a, b) => Number((a[1] && a[1].updatedAt) || 0) - Number((b[1] && b[1].updatedAt) || 0)
      );
      const overflow = this.accountSettingsCache.size - this.accountSettingsCacheMax;
      for (let idx = 0; idx < overflow; idx += 1) {
        const row = entries[idx];
        if (!row) break;
        this.accountSettingsCache.delete(row[0]);
      }
    }
  }

  async fetchWalletAccountSettings(wallet) {
    const normalizedWallet = normalizeWallet(wallet);
    if (!normalizedWallet) return new Map();
    const nowMs = Date.now();
    const cached = this.getCachedWalletAccountSettings(normalizedWallet, nowMs);
    if (cached) return cached;

    this.accountSettingsFetches = Number(this.accountSettingsFetches || 0) + 1;
    const maxAttempts = Math.max(
      2,
      Math.min(
        Math.max(2, Number(this.maxFetchAttempts || 2)),
        Math.max(2, this.restClientEntries.length)
      )
    );
    let lastError = null;
    const tried = new Set();
    for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
      const forceDirect =
        this.directClientIndex >= 0 &&
        (attempt === maxAttempts - 1 || (lastError && attempt > 0));
      let selected = this.chooseClient({ exclude: tried, forceDirect, allowDirect: true });
      if (!selected) {
        for (let spin = 0; spin < 3; spin += 1) {
          // eslint-disable-next-line no-await-in-loop
          await new Promise((resolve) => setTimeout(resolve, 30 + spin * 20));
          selected = this.chooseClient({ exclude: tried, forceDirect, allowDirect: true });
          if (selected) break;
        }
      }
      if (!selected) {
        const fallback = this.chooseClient({ exclude: tried, preferHealthy: false, allowDirect: true });
        if (!fallback) {
          lastError = new Error("all_live_wallet_clients_cooling_down");
          break;
        }
        selected = fallback;
      }
      tried.add(selected.idx);

      const { idx, entry, state } = selected;
      state.inFlight = Number(state.inFlight || 0) + 1;
      state.requests = Number(state.requests || 0) + 1;
      state.lastUsedAt = Date.now();
      const requestStartedAt = Date.now();
      try {
        const response = await entry.client.get("/account/settings", {
          query: { account: normalizedWallet },
          cost: 1,
          timeoutMs: this.accountSettingsRequestTimeoutMs,
          retryMaxAttempts: 1,
        });
        const payload =
          response && response.payload && typeof response.payload === "object"
            ? response.payload
            : {};
        if (payload.success === false) {
          const reason = String(payload.error || "account_settings_request_failed").trim();
          throw new Error(reason || "account_settings_request_failed");
        }
        const settingsBySymbol = buildAccountSettingsBySymbol(
          payload && payload.data !== undefined ? payload.data : payload
        );
        this.setWalletAccountSettingsCache(normalizedWallet, settingsBySymbol, {
          nowMs: Date.now(),
          ttlMs: this.accountSettingsCacheTtlMs,
        });
        this.markClientResult(idx, null, { durationMs: Date.now() - requestStartedAt });
        return settingsBySymbol;
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

    this.accountSettingsFetchFailures = Number(this.accountSettingsFetchFailures || 0) + 1;
    const staleFallback = this.accountSettingsCache.get(normalizedWallet);
    if (staleFallback && staleFallback.settingsBySymbol instanceof Map) {
      this.setWalletAccountSettingsCache(normalizedWallet, staleFallback.settingsBySymbol, {
        nowMs: Date.now(),
        ttlMs: this.accountSettingsFailureBackoffMs,
      });
      return staleFallback.settingsBySymbol;
    }
    throw lastError || new Error("wallet_account_settings_fetch_failed");
  }

  async enrichWalletPositions(wallet, rows = []) {
    if (!Array.isArray(rows) || rows.length <= 0) return [];
    let normalizedRows = normalizeRowsWithSettings(rows, null);
    const needsSettings = normalizedRows.some((row) => {
      if (!row || !row.symbol) return false;
      const hasLeverage = Number.isFinite(toNum(row.leverage, NaN)) && toNum(row.leverage, 0) > 0;
      const hasMargin = Number.isFinite(toNum(row.margin, NaN)) && toNum(row.margin, 0) > 0;
      return !hasLeverage || !hasMargin || !row.isolatedFromPayload;
    });
    if (!needsSettings) return normalizedRows;
    try {
      const settingsBySymbol = await this.fetchWalletAccountSettings(wallet);
      normalizedRows = normalizeRowsWithSettings(normalizedRows, settingsBySymbol);
    } catch (_error) {
      // Keep tracker latency deterministic when settings lookup fails.
    }
    return normalizedRows;
  }

  async fetchWalletPositions(wallet, options = {}) {
    const forceDirect = Boolean(options && options.forceDirect);
    const requestedMaxAttempts = Math.max(
      1,
      Number(options && options.maxAttempts ? options.maxAttempts : this.maxFetchAttempts) || 1
    );
    const maxAttempts = forceDirect
      ? 1
      : Math.max(1, Math.min(requestedMaxAttempts, this.restClientEntries.length));
    let lastError = null;
    let successfulRows = null;
    const tried = new Set();
    for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
      const useDirectForAttempt =
        forceDirect ||
        (this.directFallbackOnLastAttempt &&
        this.directClientIndex >= 0 &&
        (attempt === maxAttempts - 1 || (lastError && attempt > 0)));
      let selected = this.chooseClient({ exclude: tried, forceDirect: useDirectForAttempt });
      if (!selected) {
        for (let spin = 0; spin < 4; spin += 1) {
          // All clients may be temporarily in-flight; wait briefly before treating as a failure.
          // eslint-disable-next-line no-await-in-loop
          await new Promise((resolve) => setTimeout(resolve, 40 + spin * 20));
          selected = this.chooseClient({ exclude: tried, forceDirect: useDirectForAttempt });
          if (selected) break;
        }
      }
      if (!selected) {
        if (forceDirect) {
          const error = new Error("direct_live_wallet_client_unavailable");
          lastError = error;
          break;
        }
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
        const observedAt = Date.now();
        const rows = rowsRaw
          .map((row) => normalizePositionRow(wallet, row, { observedAt }))
          .filter(Boolean)
          .sort((a, b) => Number(b.updatedAt || 0) - Number(a.updatedAt || 0));
        this.markClientResult(idx, null, { durationMs: Date.now() - requestStartedAt });
        successfulRows = rows;
        break;
      } catch (error) {
        this.markClientResult(idx, error, { durationMs: Date.now() - requestStartedAt });
        lastError = error;
        const msg = String(error && error.message ? error.message : error || "").toLowerCase();
        if (msg.includes("404") && msg.includes("account not found")) {
          successfulRows = [];
          break;
        }
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
    if (Array.isArray(successfulRows)) {
      return this.enrichWalletPositions(wallet, successfulRows);
    }
    throw lastError || new Error("wallet_positions_fetch_failed");
  }

  async confirmWalletPositionsWithDirect(wallet, expectedCount = 0, currentRows = []) {
    const baselineRows = Array.isArray(currentRows) ? currentRows : [];
    const safeExpectedCount = Math.max(0, Number(expectedCount || 0) || 0);
    if (safeExpectedCount <= 0) return baselineRows;
    if (baselineRows.length >= safeExpectedCount) return baselineRows;
    if (this.directClientIndex < 0) return baselineRows;
    try {
      const directRows = await this.fetchWalletPositions(wallet, {
        forceDirect: true,
        maxAttempts: 3,
      });
      if (Array.isArray(directRows) && directRows.length > baselineRows.length) {
        this.pushEvent({
          at: Date.now(),
          type: "positions_direct_confirmed",
          wallet,
          oldSize: baselineRows.length,
          newSize: directRows.length,
          source: "wallet_first_positions_direct_confirm",
        });
        return directRows;
      }
      return baselineRows;
    } catch (error) {
      this.pushEvent({
        at: Date.now(),
        type: "positions_direct_confirm_failed",
        wallet,
        reason: String(error && error.message ? error.message : error || "direct_confirm_failed"),
        source: "wallet_first_positions_direct_confirm",
      });
      return baselineRows;
    }
  }

  async fetchWalletAccount(wallet) {
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
        const response = await entry.client.get("/account", {
          query: { account: wallet },
          cost: 1,
          timeoutMs: this.accountRequestTimeoutMs,
          retryMaxAttempts: 1,
        });
        const payload =
          response && response.payload && typeof response.payload === "object"
            ? response.payload
            : {};
        if (payload.success === false) {
          const reason = String(payload.error || "account_request_failed").trim();
          throw new Error(reason || "account_request_failed");
        }
        const data = payload && payload.data && typeof payload.data === "object" ? payload.data : {};
        const positionsCount = Math.max(0, Number(data.positions_count || data.positionsCount || 0) || 0);
        const ordersCount = Math.max(0, Number(data.orders_count || data.ordersCount || 0) || 0);
        const updatedAt = normalizeTimestampMs(
          data.updated_at !== undefined ? data.updated_at : data.updatedAt,
          Date.now()
        );
        this.markClientResult(idx, null, { durationMs: Date.now() - requestStartedAt });
        return {
          positionsCount,
          ordersCount,
          updatedAt,
          raw: data,
        };
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
    throw lastError || new Error("wallet_account_fetch_failed");
  }

  pushEvent(event) {
    if (!event || typeof event !== "object") return;
    this.events.push(event);
    const eventType = String(event.type || event.cause || event.sideEvent || "").trim().toLowerCase();
    const isPositionChangeEvent =
      eventType.includes("position_opened") ||
      eventType.includes("position_increased") ||
      eventType.includes("position_closed_partial") ||
      eventType.includes("position_closed") ||
      eventType.includes("position_size_changed");
    if (eventType.includes("position_opened")) {
      this.positionOpenedEvents.push(event);
      if (this.positionOpenedEvents.length > this.maxPositionOpenedEvents) {
        this.positionOpenedEvents.splice(
          0,
          this.positionOpenedEvents.length - this.maxPositionOpenedEvents
        );
      }
    }
    if (isPositionChangeEvent) {
      this.positionChangeEvents.push(event);
      if (this.positionChangeEvents.length > this.maxPositionChangeEvents) {
        this.positionChangeEvents.splice(
          0,
          this.positionChangeEvents.length - this.maxPositionChangeEvents
        );
      }
    }
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
      .map((row) => normalizePositionRow(normalizedWallet, row, { observedAt }))
      .filter(Boolean)
      .sort((a, b) => Number(b.updatedAt || 0) - Number(a.updatedAt || 0));
    const normalizedRows = normalizeRowsWithSettings(rows, null);
    const liveState = this.ensureLivePositionState(normalizedWallet);
    const snapshot = this.walletSnapshots.get(normalizedWallet) || null;
    const previousRows = snapshot && Array.isArray(snapshot.positions) ? snapshot.positions : [];
    const openHint = this.accountOpenHints.get(normalizedWallet) || null;
    const hintedCount = Math.max(
      0,
      Number(
        openHint && openHint.positionsCount !== undefined
          ? openHint.positionsCount
          : liveState && liveState.accountPositionsCount
      ) || 0
    );
    const lastAccountAt = Math.max(0, Number(liveState && liveState.lastAccountAt || 0));
    const previousRowsObservedAt = rowsLatestObservedAt(previousRows);
    const previousRowsFresh =
      previousRows.length > 0 &&
      previousRowsObservedAt > 0 &&
      Math.max(0, observedAt - previousRowsObservedAt) <=
        Math.max(this.coolingMs, Math.floor(this.staleMs / 2));
    const requiresAccountConfirmation =
      lastAccountAt <= 0 ||
      Math.max(0, observedAt - lastAccountAt) >
        Math.max(this.scanIntervalMs * 4, Math.floor(this.hotReconcileMaxAgeMs / 2)) ||
      Math.max(0, Number(liveState && liveState.lastMaterializedOpenAt || 0)) > 0;
    const preserveExistingSnapshot =
      rows.length === 0 &&
      (hintedCount > 0 ||
        previousRowsFresh ||
        (Boolean(liveState && String(liveState.state || "") !== "closed") && previousRowsFresh) ||
        (requiresAccountConfirmation && previousRowsFresh));
    if (!preserveExistingSnapshot) {
      this.updateSnapshot(normalizedWallet, normalizedRows, observedAt);
    } else {
      const hintedBootstrapCount = Math.max(
        hintedCount,
        previousRows.length,
        Math.max(0, Number(liveState && liveState.lastMaterializedOpenAt ? 1 : 0))
      );
      if (hintedBootstrapCount > 0) {
        this.noteAccountOpenHint(normalizedWallet, hintedBootstrapCount, {
          updatedAt: observedAt,
          observedAt,
          materializedPositions: previousRows.length,
          missCount: 1,
          reason: "ws_positions_gap",
        });
      }
      this.transitionLivePositionState(
        normalizedWallet,
        hintedCount > 0 ? "hinted_open" : "uncertain",
        {
          at: observedAt,
          lastPositionsAt: observedAt,
          materializedPositionsCount: previousRows.length,
          positionsEmptyStreak: Math.max(
            1,
            Number(liveState && liveState.positionsEmptyStreak || 0) + 1
          ),
          pendingBootstrap: true,
          reason: "ws_positions_gap",
        }
      );
      this.recomputeLivePositionStateStatus();
      this.enqueuePriorityWallet(normalizedWallet);
    }
    this.noteRecentWalletActivity(normalizedWallet, observedAt);
    this.touchLifecycle(normalizedWallet, {
      at: observedAt,
      position: true,
      success: true,
        openPositionsCount: preserveExistingSnapshot ? previousRows.length : normalizedRows.length,
      reason: "ws_account_positions",
    });
    this.flatPositionsDirty = true;
    this.status.lastSuccessAt = observedAt;
    this.persistMaybe();
  }

  ingestHotWalletAccountInfo(wallet, summaryRaw = {}, meta = {}) {
    const normalizedWallet = normalizeWallet(wallet);
    if (!normalizedWallet) return;
    const observedAt = Number(meta.at || Date.now());
    const summary =
      summaryRaw && typeof summaryRaw === "object" && !Array.isArray(summaryRaw) ? summaryRaw : {};
    const positionsCount = Math.max(0, Number(summary.pc || summary.positionsCount || 0) || 0);
    const ordersCount = Math.max(0, Number(summary.oc || summary.ordersCount || 0) || 0);
    const updatedAt = normalizeTimestampMs(summary.t, observedAt);
    const previousSnapshot = this.accountSnapshots.get(normalizedWallet) || null;
    const liveState = this.ensureLivePositionState(normalizedWallet);
    const previousCount = Math.max(
      0,
      Number(
        previousSnapshot && previousSnapshot.positionsCount !== undefined
          ? previousSnapshot.positionsCount
          : liveState && liveState.accountPositionsCount
      ) || 0
    );
    const materializedCount = Math.max(
      0,
      Number(liveState && liveState.materializedPositionsCount !== undefined
        ? liveState.materializedPositionsCount
        : 0) || 0,
      this.walletSnapshots.has(normalizedWallet) &&
        Array.isArray(this.walletSnapshots.get(normalizedWallet).positions)
        ? this.walletSnapshots.get(normalizedWallet).positions.length
        : 0
    );

    this.accountSnapshots.set(normalizedWallet, {
      wallet: normalizedWallet,
      positionsCount,
      ordersCount,
      updatedAt,
      scannedAt: observedAt,
      success: true,
      lastErrorAt: null,
      lastError: null,
      raw: summary,
    });

    this.noteRecentWalletActivity(normalizedWallet, observedAt);
    this.touchLifecycle(normalizedWallet, {
      at: observedAt,
      trigger: true,
      reconcile: true,
      success: true,
      openPositionsCount: positionsCount,
      reason: "ws_account_info",
    });

    if (positionsCount > 0) {
      this.noteAccountOpenHint(normalizedWallet, positionsCount, {
        updatedAt,
        observedAt,
        materializedPositions: materializedCount,
        reason: "ws_account_info_positions_count",
      });
      this.transitionLivePositionState(normalizedWallet, "hinted_open", {
        at: observedAt,
        lastAccountAt: observedAt,
        accountPositionsCount: positionsCount,
        materializedPositionsCount: materializedCount,
        pendingBootstrap: positionsCount > materializedCount,
        reason: "ws_account_info_positions_count",
      });
      if (positionsCount > materializedCount) {
        const dedupeKey = `${normalizedWallet}:pc:${positionsCount}`;
        const previousAt = Math.max(
          0,
          Number(this.wsAccountInfoTriggerAt.get(dedupeKey) || 0)
        );
        if (
          positionsCount > previousCount &&
          observedAt - previousAt >= this.wsAccountInfoTriggerDedupMs
        ) {
          this.wsAccountInfoTriggerAt.set(dedupeKey, observedAt);
          this.pushEvent({
            at: observedAt,
            type: "position_opened_provisional",
            wallet: normalizedWallet,
            symbol: null,
            side: null,
            source: meta.source || "ws_account_info",
            walletSource: "wallet_ws_account_info",
            walletConfidence: "ws_trigger",
            openedAt: observedAt,
            observedAt,
            updatedAt,
            timestamp: observedAt,
            expectedOpenPositions: positionsCount,
            materializedPositionsCount: materializedCount,
            provisional: true,
            cause: "ws_account_info_pc_increase",
            sideEvent: "position_opened",
          });
        }
        this.enqueuePriorityWallet(normalizedWallet);
      }
      if (this.hotWalletWsMonitor && typeof this.hotWalletWsMonitor.promoteWallet === "function") {
        const priorityMeta = this.walletPriorityMeta.get(normalizedWallet) || null;
        this.hotWalletWsMonitor.promoteWallet(normalizedWallet, {
          at: observedAt,
          reason: "ws_account_info_positions_count",
          forceTier: "hot",
          openPositions: true,
          recentVolumeUsd: priorityMeta ? priorityMeta.recentVolumeUsd : 0,
          recentTradeCount: priorityMeta ? priorityMeta.recentTradeCount : 0,
          liveActiveRank: priorityMeta ? priorityMeta.liveActiveRank : 0,
          priorityBoost: priorityMeta ? priorityMeta.priorityBoost : 0,
          pinMs: this.hotReconcileMaxAgeMs * 4,
        });
      }
    } else {
      this.closeAllWalletPositions(normalizedWallet, {
        at: observedAt,
        source: meta.source || "ws_account_info",
        reason: "ws_account_info_zero_positions",
      });
      this.transitionLivePositionState(normalizedWallet, "closed", {
        at: observedAt,
        lastAccountAt: observedAt,
        accountPositionsCount: 0,
        materializedPositionsCount: materializedCount,
        pendingBootstrap: false,
        reason: "ws_account_info_zero_positions",
      });
      this.clearAccountOpenHint(normalizedWallet);
    }
    if (positionsCount !== previousCount) {
      this.enqueueWsReconcile(normalizedWallet, {
        at: observedAt,
        reason: "ws_account_info_delta",
        force: true,
      });
    } else if (positionsCount > 0 && materializedCount < positionsCount) {
      this.enqueueWsReconcile(normalizedWallet, {
        at: observedAt,
        reason: "ws_account_info_gap",
        force: false,
      });
    }

    this.recomputeLivePositionStateStatus();
    this.status.lastSuccessAt = observedAt;
    this.persistAccountStateMaybe();
    this.persistMaybe();
  }

  ingestHotWalletActivity(wallet, type, meta = {}) {
    const normalizedWallet = normalizeWallet(wallet);
    if (!normalizedWallet) return;
    const at = Number(meta.at || Date.now());
    const normalizedSide = String(meta.side || "").trim().toLowerCase();
    const normalizedSymbol = String(meta.symbol || "").trim().toUpperCase();
    const normalizedOrderEvent = String(meta.orderEvent || "").trim().toLowerCase();
    const normalizedOrderStatus = String(meta.orderStatus || "").trim().toLowerCase();
    const normalizedTriggerType = String(type || "").trim().toLowerCase();
    const inferredTradeSide = normalizeSide(normalizedSide);
    const isTradeEvent = normalizedTriggerType.includes("trade");
    const isOrderEvent = normalizedTriggerType.includes("order");
    const isOpenTradeTrigger =
      isTradeEvent && (inferredTradeSide === "open_long" || inferredTradeSide === "open_short");
    const isCloseTradeTrigger =
      isTradeEvent && (inferredTradeSide === "close_long" || inferredTradeSide === "close_short");
    const inferredOrderSide = normalizeSide(normalizedSide);
    const orderIsFilled = normalizedOrderEvent.includes("fill") || normalizedOrderStatus === "filled";
    const isOpenOrderTrigger =
      isOrderEvent &&
      orderIsFilled &&
      !meta.reduceOnly &&
      (inferredOrderSide === "open_long" || inferredOrderSide === "open_short");
    const isCloseOrderTrigger =
      isOrderEvent &&
      orderIsFilled &&
      (Boolean(meta.reduceOnly) ||
        inferredOrderSide === "close_long" ||
        inferredOrderSide === "close_short");
    const liveState = this.ensureLivePositionState(normalizedWallet);
    const snapshot = this.walletSnapshots.get(normalizedWallet) || null;
    if (snapshot && Array.isArray(snapshot.positions) && snapshot.positions.length > 0) {
      const refreshedPositions = snapshot.positions.map((row) => {
        const previousObservedAt = Math.max(
          0,
          Number(
            row && (row.lastObservedAt || row.observedAt || row.timestamp || row.updatedAt)
              ? row.lastObservedAt || row.observedAt || row.timestamp || row.updatedAt
              : 0
          )
        );
        const nextObservedAt = Math.max(previousObservedAt, at);
        return {
          ...row,
          observedAt: nextObservedAt,
          lastObservedAt: nextObservedAt,
          timestamp: nextObservedAt,
        };
      });
      this.walletSnapshots.set(normalizedWallet, {
        ...snapshot,
        positions: refreshedPositions,
        scannedAt: Math.max(Number(snapshot.scannedAt || 0), at),
        lastSuccessAt: Math.max(Number(snapshot.lastSuccessAt || 0), at),
        success: true,
        lastErrorAt: null,
        lastError: null,
      });
      this.flatPositionsDirty = true;
      this.status.lastSuccessAt = Math.max(0, Number(this.status.lastSuccessAt || 0), at);
    }
    this.noteRecentWalletActivity(normalizedWallet, at);
    this.touchLifecycle(normalizedWallet, {
      at,
      trigger: true,
      trade: String(type || "").includes("trade"),
      order: String(type || "").includes("order"),
      reason: type || "hot_wallet_ws",
    });
    if (isOpenTradeTrigger || isOpenOrderTrigger) {
      const openSide = isOpenTradeTrigger ? inferredTradeSide : inferredOrderSide;
      const openSource = isOpenTradeTrigger
        ? meta.source || "ws_account_trade"
        : meta.source || "ws_account_order_update";
      const openWalletSource = isOpenTradeTrigger ? "wallet_ws_trade" : "wallet_ws_order";
      const openCause = isOpenTradeTrigger ? "ws_trade_open_trigger" : "ws_order_fill_open_trigger";
      const provisionalSize =
        meta.amount !== undefined && meta.amount !== null ? Number(meta.amount) || 0 : 0;
      const provisionalEntry =
        meta.entry !== undefined && meta.entry !== null
          ? Number(meta.entry) || null
          : meta.price !== undefined && meta.price !== null
          ? Number(meta.price) || null
          : null;
      const provisionalUsd =
        meta.positionUsd !== undefined && meta.positionUsd !== null
          ? Number(meta.positionUsd) || 0
          : provisionalEntry !== null
          ? Math.abs(provisionalSize * provisionalEntry)
          : 0;
      const provisionalLogicalKey = logicalPositionKey({
        symbol: normalizedSymbol || null,
        side: openSide,
      });
      const provisionalWalletPositions = this.ensureProvisionalWalletPositions(normalizedWallet);
      const provisionalPrevRow =
        provisionalLogicalKey && provisionalWalletPositions
          ? provisionalWalletPositions.get(provisionalLogicalKey) || null
          : null;
      const previousSize = Number.isFinite(toNum(provisionalPrevRow && provisionalPrevRow.size, NaN))
        ? Number(toNum(provisionalPrevRow.size, 0).toFixed(8))
        : 0;
      this.rememberLastOpenedPosition(
        normalizedWallet,
        {
          wallet: normalizedWallet,
          symbol: normalizedSymbol || null,
          side: openSide,
          amount: provisionalSize,
          size: provisionalSize,
          positionUsd: provisionalUsd,
          entry: provisionalEntry,
          mark: provisionalEntry,
          openedAt: at,
          observedAt: at,
          updatedAt: at,
          source: openSource,
          walletConfidence: "ws_trigger",
        },
        {
          at,
          observedAt: at,
          source: openSource,
          confidence: "ws_trigger",
        }
      );
      const provisionalRow = this.upsertProvisionalPosition(
        normalizedWallet,
        {
          wallet: normalizedWallet,
          symbol: normalizedSymbol || null,
          side: openSide,
          amount: provisionalSize,
          size: provisionalSize,
          positionUsd: provisionalUsd,
          entry: provisionalEntry,
          mark: provisionalEntry,
          openedAt: at,
          observedAt: at,
          updatedAt: at,
          source: openSource,
          walletConfidence: "ws_trigger",
        },
        {
          at,
          observedAt: at,
          source: openSource,
          confidence: "ws_trigger",
          applyDelta: true,
        }
      );
      const nextSize = Number.isFinite(toNum(provisionalRow && provisionalRow.size, NaN))
        ? Number(toNum(provisionalRow.size, 0).toFixed(8))
        : Number.isFinite(toNum(provisionalSize, NaN))
        ? Number(toNum(provisionalSize, 0).toFixed(8))
        : 0;
      const sizeDelta = Number.isFinite(toNum(nextSize, NaN)) && Number.isFinite(toNum(previousSize, NaN))
        ? Number((toNum(nextSize, 0) - toNum(previousSize, 0)).toFixed(8))
        : null;
      const isIncreaseEvent =
        previousSize > 1e-10 &&
        Number.isFinite(toNum(nextSize, NaN)) &&
        toNum(nextSize, 0) > toNum(previousSize, 0) + 1e-10;
      const increaseCause = isOpenTradeTrigger
        ? "ws_trade_increase_trigger"
        : "ws_order_fill_increase_trigger";
      const eventType = isIncreaseEvent ? "position_increased" : "position_opened";
      const eventCause = isIncreaseEvent ? increaseCause : openCause;
      const eventUsd = Number.isFinite(toNum(sizeDelta, NaN)) && Number.isFinite(toNum(provisionalEntry, NaN))
        ? Number(Math.abs(toNum(sizeDelta, 0) * toNum(provisionalEntry, 0)).toFixed(2))
        : Number.isFinite(toNum(provisionalUsd, NaN))
        ? Number(toNum(provisionalUsd, 0).toFixed(2))
        : null;
      this.transitionLivePositionState(normalizedWallet, "hinted_open", {
        at,
        lastHintOpenAt: at,
        accountPositionsCount: Math.max(1, Number((liveState && liveState.accountPositionsCount) || 0)),
        materializedPositionsCount: Math.max(
          0,
          Number((liveState && liveState.materializedPositionsCount) || 0)
        ),
        pendingBootstrap: false,
        reason: openCause,
      });
      this.flatPositionsDirty = true;
      this.recomputeLivePositionStateStatus();
      const dedupeKey = `${normalizedWallet}:${normalizedSymbol}:${openSide}:${
        isIncreaseEvent ? "increase_trigger" : "open_trigger"
      }`;
      const dedupeMs = isIncreaseEvent ? Math.min(1000, this.hotOpenTriggerDedupMs) : this.hotOpenTriggerDedupMs;
      const previousAt = Math.max(0, Number(this.hotOpenTriggerAt.get(dedupeKey) || 0));
      if (at - previousAt >= dedupeMs) {
        this.hotOpenTriggerAt.set(dedupeKey, at);
        const skipProvisionalIncreaseEvent =
          isIncreaseEvent && (!Number.isFinite(sizeDelta) || Math.abs(sizeDelta) <= 1e-10);
        if (!skipProvisionalIncreaseEvent) {
          this.pushEvent({
            at,
            type: eventType,
            wallet: normalizedWallet,
            symbol: normalizedSymbol || null,
            side: openSide,
            source: openSource,
            walletSource: openWalletSource,
            walletConfidence: "ws_trigger",
            openedAt: at,
            observedAt: at,
            updatedAt: at,
            timestamp: at,
            oldSize: isIncreaseEvent ? previousSize : null,
            newSize: isIncreaseEvent ? nextSize : null,
            sizeDelta: isIncreaseEvent ? sizeDelta : null,
            sizeDeltaAbs:
              isIncreaseEvent && Number.isFinite(toNum(sizeDelta, NaN))
                ? Number(Math.abs(toNum(sizeDelta, 0)).toFixed(8))
                : null,
            amount:
              meta.amount !== undefined && meta.amount !== null ? Number(meta.amount) || 0 : null,
            entry:
              meta.entry !== undefined && meta.entry !== null
                ? Number(meta.entry) || null
                : meta.price !== undefined && meta.price !== null
                ? Number(meta.price) || null
                : null,
            positionUsd:
              provisionalRow && provisionalRow.positionUsd !== undefined
                ? Number(provisionalRow.positionUsd) || 0
                : meta.positionUsd !== undefined && meta.positionUsd !== null
                ? Number(meta.positionUsd) || 0
                : null,
            eventUsd,
            provisional: true,
            cause: eventCause,
            sideEvent: eventType,
          });
        }
      }
    }
    if (isCloseTradeTrigger || isCloseOrderTrigger) {
      const closeSide = isCloseTradeTrigger ? inferredTradeSide : inferredOrderSide;
      const expectedOpenSide = closeSide === "close_long" ? "open_long" : "open_short";
      const currentLiveState = this.ensureLivePositionState(normalizedWallet);
      const currentSummaryMatches =
        currentLiveState &&
        normalizeSymbol(currentLiveState.lastOpenedPositionSymbol) === normalizedSymbol &&
        normalizeSide(currentLiveState.lastOpenedPositionSide || "") === expectedOpenSide;
      const closeResult = this.applyProvisionalCloseOrReduce(
        normalizedWallet,
        {
          symbol: normalizedSymbol || null,
          side: expectedOpenSide,
          amount:
            meta.amount !== undefined && meta.amount !== null ? Number(meta.amount) || 0 : 0,
          observedAt: at,
          updatedAt: at,
          source: isCloseTradeTrigger
            ? meta.source || "ws_account_trade"
            : meta.source || "ws_account_order_update",
        },
        {
          at,
          source: isCloseTradeTrigger
            ? meta.source || "ws_account_trade"
            : meta.source || "ws_account_order_update",
          reason: isCloseTradeTrigger ? "ws_trade_close_trigger" : "ws_order_close_trigger",
        }
      );
      if (currentSummaryMatches && (!closeResult || closeResult.action === "closed")) {
        clearLastOpenedPositionSummary(currentLiveState);
      }
      if (closeResult) {
        const closeRow =
          closeResult && closeResult.row && typeof closeResult.row === "object"
            ? closeResult.row
            : null;
        const closeEntry = Number.isFinite(toNum(closeRow && closeRow.entry, NaN))
          ? Number(toNum(closeRow.entry, 0).toFixed(8))
          : Number.isFinite(toNum(currentLiveState && currentLiveState.lastOpenedPositionEntry, NaN))
          ? Number(toNum(currentLiveState.lastOpenedPositionEntry, 0).toFixed(8))
          : null;
        const closeMark = Number.isFinite(toNum(closeRow && closeRow.mark, NaN))
          ? Number(toNum(closeRow.mark, 0).toFixed(8))
          : Number.isFinite(toNum(currentLiveState && currentLiveState.lastOpenedPositionMark, NaN))
          ? Number(toNum(currentLiveState.lastOpenedPositionMark, 0).toFixed(8))
          : closeEntry;
        const closePositionUsd =
          Number.isFinite(toNum(closeRow && closeRow.positionUsd, NaN))
            ? Number(toNum(closeRow.positionUsd, 0).toFixed(2))
            : Number.isFinite(toNum(closeResult && closeResult.nextSize, NaN)) &&
              Number.isFinite(toNum(closeEntry, NaN))
            ? Number(Math.abs(toNum(closeResult.nextSize, 0) * toNum(closeEntry, 0)).toFixed(2))
            : Number.isFinite(toNum(closeResult && closeResult.previousSize, NaN)) &&
              Number.isFinite(toNum(closeEntry, NaN))
            ? Number(Math.abs(toNum(closeResult.previousSize, 0) * toNum(closeEntry, 0)).toFixed(2))
            : 0;
        const closeEventUsd =
          Number.isFinite(toNum(closeResult && closeResult.previousSize, NaN)) &&
          Number.isFinite(toNum(closeResult && closeResult.nextSize, NaN)) &&
          Number.isFinite(toNum(closeEntry, NaN))
            ? Number(
                Math.abs(
                  (toNum(closeResult.nextSize, 0) - toNum(closeResult.previousSize, 0)) *
                    toNum(closeEntry, 0)
                ).toFixed(2)
              )
            : closePositionUsd;
        this.pushEvent({
          at,
          type:
            closeResult.action === "closed" ? "position_closed" : "position_size_changed",
          wallet: normalizedWallet,
          symbol: normalizedSymbol || null,
          side: expectedOpenSide,
          source: isCloseTradeTrigger
            ? meta.source || "ws_account_trade"
            : meta.source || "ws_account_order_update",
          walletSource: isCloseTradeTrigger ? "wallet_ws_trade" : "wallet_ws_order",
          walletConfidence: "ws_trigger",
          openedAt: at,
          observedAt: at,
          updatedAt: at,
          timestamp: at,
          oldSize: closeResult.previousSize,
          newSize: closeResult.nextSize,
          sizeDelta:
            Number.isFinite(toNum(closeResult.previousSize, NaN)) &&
            Number.isFinite(toNum(closeResult.nextSize, NaN))
              ? Number(
                  (
                    toNum(closeResult.nextSize, 0) - toNum(closeResult.previousSize, 0)
                  ).toFixed(8)
                )
              : null,
          sizeDeltaAbs:
            Number.isFinite(toNum(closeResult.previousSize, NaN)) &&
            Number.isFinite(toNum(closeResult.nextSize, NaN))
              ? Number(
                  Math.abs(
                    toNum(closeResult.nextSize, 0) - toNum(closeResult.previousSize, 0)
                  ).toFixed(8)
                )
              : null,
          entry: closeEntry,
          mark: closeMark,
          positionUsd: closePositionUsd,
          eventUsd: closeEventUsd,
          positionKey:
            (closeRow && closeRow.positionKey) ||
            (currentLiveState && currentLiveState.lastOpenedPositionKey) ||
            null,
          amount:
            meta.amount !== undefined && meta.amount !== null ? Number(meta.amount) || 0 : null,
          provisional: true,
          cause: isCloseTradeTrigger ? "ws_trade_close_trigger" : "ws_order_close_trigger",
          sideEvent:
            closeResult.action === "closed" ? "position_closed" : "position_size_changed",
        });
      }
      if (currentSummaryMatches) {
        const nextAccountPositionsCount =
          closeResult && closeResult.action === "reduced"
            ? Math.max(1, Number(currentLiveState.accountPositionsCount || 0))
            : Math.max(0, Number(currentLiveState.accountPositionsCount || 0) - 1);
        this.transitionLivePositionState(
          normalizedWallet,
          nextAccountPositionsCount > 0 ? "hinted_open" : "closed",
          {
            at,
            accountPositionsCount: nextAccountPositionsCount,
            materializedPositionsCount: Math.max(
              0,
              Number(currentLiveState.materializedPositionsCount || 0)
            ),
            pendingBootstrap: false,
            lastConfirmedClosedAt: nextAccountPositionsCount > 0 ? 0 : at,
            reason:
              closeResult && closeResult.action === "reduced"
                ? isCloseTradeTrigger
                  ? "ws_trade_reduce_trigger"
                  : "ws_order_reduce_trigger"
                : isCloseTradeTrigger
                ? "ws_trade_close_trigger"
                : "ws_order_close_trigger",
          }
        );
        if (nextAccountPositionsCount <= 0) {
          this.clearAccountOpenHint(normalizedWallet);
        }
        this.flatPositionsDirty = true;
        this.recomputeLivePositionStateStatus();
      }
    }
    this.pushEvent({
      at,
      type,
      wallet: normalizedWallet,
      symbol: normalizedSymbol || null,
      side: normalizedSide || null,
      source: meta.source || "hot_wallet_ws",
    });
    if (isOpenTradeTrigger || isCloseTradeTrigger || isOpenOrderTrigger || isCloseOrderTrigger) {
      this.enqueueWsReconcile(normalizedWallet, {
        at,
        reason: "ws_trade_or_order_trigger",
        force: false,
      });
    }
    this.persistMaybe();
  }

  updateSnapshot(wallet, nextRows, observedAt) {
    const prev = this.walletSnapshots.get(wallet) || null;
    const prevRows = prev && Array.isArray(prev.positions) ? prev.positions : [];
    const liveState = this.ensureLivePositionState(wallet);
    const isBootstrapSnapshot = !prev;
    const processStartedAt = Math.max(0, Number(this.status && this.status.startedAt || 0));

    const prevMap = new Map(prevRows.map((row) => [row.positionKey || positionKey(row), row]));
    const nextMap = new Map(nextRows.map((row) => [row.positionKey || positionKey(row), row]));

    for (const [key, row] of nextMap.entries()) {
      const oldRow = prevMap.get(key);
      this.clearPositionTombstone(wallet, logicalPositionKey(row));
      if (!oldRow) {
        const openedAt = Number(row.openedAt || observedAt) || observedAt;
        const eventObservedAt =
          Number(row.observedAt || row.lastObservedAt || observedAt) || observedAt;
        const suppressBootstrapOpenEvent =
          isBootstrapSnapshot && processStartedAt > 0 && openedAt < processStartedAt;
        const suppressStaleOpenedEvent =
          openedAt > 0 &&
          eventObservedAt > 0 &&
          Math.max(0, eventObservedAt - openedAt) > this.openedEventMaxLagMs;
        const openedEvent = {
          at: observedAt,
          type: "position_opened",
          wallet,
          symbol: row.symbol,
          side: row.side,
          key,
          oldSize: null,
          newSize: row.size,
          source: "wallet_first_positions",
          positionKey: row.positionKey || key,
          direction: normalizePositionDirection(row.side || row.rawSide || ""),
          entry: Number.isFinite(toNum(row.entry, NaN)) ? Number(toNum(row.entry, 0).toFixed(8)) : null,
          mark: Number.isFinite(toNum(row.mark, NaN)) ? Number(toNum(row.mark, 0).toFixed(8)) : null,
          positionUsd: Number(toNum(row.positionUsd, 0).toFixed(2)),
          margin: Number.isFinite(toNum(row.margin, NaN))
            ? Number(toNum(row.margin, 0).toFixed(2))
            : null,
          marginUsd: Number.isFinite(toNum(row.marginUsd, NaN))
            ? Number(toNum(row.marginUsd, 0).toFixed(2))
            : Number.isFinite(toNum(row.margin, NaN))
            ? Number(toNum(row.margin, 0).toFixed(2))
            : null,
          leverage: Number.isFinite(toNum(row.leverage, NaN))
            ? Number(toNum(row.leverage, 0).toFixed(4))
            : null,
          isolated: row.isolated !== undefined ? Boolean(row.isolated) : null,
          unrealizedPnlUsd: Number.isFinite(toNum(row.pnl, NaN)) ? Number(toNum(row.pnl, 0).toFixed(2)) : null,
          openedAt,
          updatedAt: Number(row.updatedAt || observedAt) || observedAt,
          observedAt: eventObservedAt,
        };
        if (!suppressBootstrapOpenEvent && !suppressStaleOpenedEvent) {
          this.pushEvent(openedEvent);
        }
        this.rememberLastOpenedPosition(wallet, row, {
          at: openedAt,
          observedAt: eventObservedAt,
          source: "wallet_first_positions",
          confidence: "high",
        });
        continue;
      }
      if (!compareAmount(oldRow.size, row.size)) {
        const oldSize = Number.isFinite(toNum(oldRow.size, NaN))
          ? Number(toNum(oldRow.size, 0).toFixed(8))
          : null;
        const newSize = Number.isFinite(toNum(row.size, NaN))
          ? Number(toNum(row.size, 0).toFixed(8))
          : null;
        const entry = Number.isFinite(toNum(row.entry, NaN))
          ? Number(toNum(row.entry, 0).toFixed(8))
          : Number.isFinite(toNum(oldRow.entry, NaN))
          ? Number(toNum(oldRow.entry, 0).toFixed(8))
          : null;
        const mark = Number.isFinite(toNum(row.mark, NaN))
          ? Number(toNum(row.mark, 0).toFixed(8))
          : Number.isFinite(toNum(oldRow.mark, NaN))
          ? Number(toNum(oldRow.mark, 0).toFixed(8))
          : entry;
        const sizeDelta =
          Number.isFinite(toNum(oldSize, NaN)) && Number.isFinite(toNum(newSize, NaN))
            ? Number((toNum(newSize, 0) - toNum(oldSize, 0)).toFixed(8))
            : null;
        const positionUsd = Number.isFinite(toNum(row.positionUsd, NaN))
          ? Number(toNum(row.positionUsd, 0).toFixed(2))
          : Number.isFinite(toNum(newSize, NaN)) && Number.isFinite(toNum(entry, NaN))
          ? Number(Math.abs(toNum(newSize, 0) * toNum(entry, 0)).toFixed(2))
          : 0;
        const eventUsd = Number.isFinite(toNum(sizeDelta, NaN)) && Number.isFinite(toNum(entry, NaN))
          ? Number(Math.abs(toNum(sizeDelta, 0) * toNum(entry, 0)).toFixed(2))
          : Number.isFinite(toNum(oldRow.positionUsd, NaN)) && Number.isFinite(toNum(positionUsd, NaN))
          ? Number(Math.abs(toNum(positionUsd, 0) - toNum(oldRow.positionUsd, 0)).toFixed(2))
          : positionUsd;
        this.pushEvent({
          at: observedAt,
          type: "position_size_changed",
          wallet,
          symbol: row.symbol,
          side: row.side,
          key,
          oldSize,
          newSize,
          sizeDelta,
          sizeDeltaAbs: Number.isFinite(toNum(sizeDelta, NaN))
            ? Number(Math.abs(toNum(sizeDelta, 0)).toFixed(8))
            : null,
          amount: newSize,
          positionKey: row.positionKey || key,
          direction: normalizePositionDirection(row.side || row.rawSide || ""),
          entry,
          mark,
          positionUsd,
          eventUsd,
          margin: Number.isFinite(toNum(row.margin, NaN))
            ? Number(toNum(row.margin, 0).toFixed(2))
            : Number.isFinite(toNum(oldRow.margin, NaN))
            ? Number(toNum(oldRow.margin, 0).toFixed(2))
            : null,
          marginUsd: Number.isFinite(toNum(row.marginUsd, NaN))
            ? Number(toNum(row.marginUsd, 0).toFixed(2))
            : Number.isFinite(toNum(row.margin, NaN))
            ? Number(toNum(row.margin, 0).toFixed(2))
            : Number.isFinite(toNum(oldRow.marginUsd, NaN))
            ? Number(toNum(oldRow.marginUsd, 0).toFixed(2))
            : Number.isFinite(toNum(oldRow.margin, NaN))
            ? Number(toNum(oldRow.margin, 0).toFixed(2))
            : null,
          leverage: Number.isFinite(toNum(row.leverage, NaN))
            ? Number(toNum(row.leverage, 0).toFixed(4))
            : Number.isFinite(toNum(oldRow.leverage, NaN))
            ? Number(toNum(oldRow.leverage, 0).toFixed(4))
            : null,
          isolated:
            row.isolated !== undefined
              ? Boolean(row.isolated)
              : oldRow.isolated !== undefined
              ? Boolean(oldRow.isolated)
              : null,
          openedAt: Number(row.openedAt || oldRow.openedAt || observedAt) || observedAt,
          updatedAt: Number(row.updatedAt || observedAt) || observedAt,
          observedAt:
            Number(row.observedAt || row.lastObservedAt || observedAt) || observedAt,
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
        oldSize: Number.isFinite(toNum(row.size, NaN))
          ? Number(toNum(row.size, 0).toFixed(8))
          : null,
        newSize: 0,
        sizeDelta: Number.isFinite(toNum(row.size, NaN))
          ? Number((-Math.abs(toNum(row.size, 0))).toFixed(8))
          : null,
        sizeDeltaAbs: Number.isFinite(toNum(row.size, NaN))
          ? Number(Math.abs(toNum(row.size, 0)).toFixed(8))
          : null,
        amount: 0,
        positionKey: row.positionKey || key,
        direction: normalizePositionDirection(row.side || row.rawSide || ""),
        entry: Number.isFinite(toNum(row.entry, NaN))
          ? Number(toNum(row.entry, 0).toFixed(8))
          : null,
        mark: Number.isFinite(toNum(row.mark, NaN))
          ? Number(toNum(row.mark, 0).toFixed(8))
          : Number.isFinite(toNum(row.entry, NaN))
          ? Number(toNum(row.entry, 0).toFixed(8))
          : null,
        positionUsd: Number.isFinite(toNum(row.positionUsd, NaN))
          ? Number(toNum(row.positionUsd, 0).toFixed(2))
          : Number.isFinite(toNum(row.size, NaN)) && Number.isFinite(toNum(row.entry, NaN))
          ? Number(Math.abs(toNum(row.size, 0) * toNum(row.entry, 0)).toFixed(2))
          : 0,
        eventUsd: Number.isFinite(toNum(row.positionUsd, NaN))
          ? Number(toNum(row.positionUsd, 0).toFixed(2))
          : Number.isFinite(toNum(row.size, NaN)) && Number.isFinite(toNum(row.entry, NaN))
          ? Number(Math.abs(toNum(row.size, 0) * toNum(row.entry, 0)).toFixed(2))
          : 0,
        margin: Number.isFinite(toNum(row.margin, NaN))
          ? Number(toNum(row.margin, 0).toFixed(2))
          : null,
        marginUsd: Number.isFinite(toNum(row.marginUsd, NaN))
          ? Number(toNum(row.marginUsd, 0).toFixed(2))
          : Number.isFinite(toNum(row.margin, NaN))
          ? Number(toNum(row.margin, 0).toFixed(2))
          : null,
        leverage: Number.isFinite(toNum(row.leverage, NaN))
          ? Number(toNum(row.leverage, 0).toFixed(4))
          : null,
        isolated: row.isolated !== undefined ? Boolean(row.isolated) : null,
        openedAt: Number(row.openedAt || observedAt) || observedAt,
        updatedAt: Number(row.updatedAt || observedAt) || observedAt,
        observedAt:
          Number(row.observedAt || row.lastObservedAt || observedAt) || observedAt,
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
    if (nextRows.length > 0) {
      const provisionalRows = this.provisionalPositions.get(wallet) || null;
      if (provisionalRows) {
        nextRows.forEach((row) => {
          provisionalRows.delete(logicalPositionKey(row));
        });
        if (provisionalRows.size <= 0) this.provisionalPositions.delete(wallet);
      }
    }

    const oldOpen = prevRows.length > 0;
    const newOpen = nextRows.length > 0;
    const openHint = this.accountOpenHints.get(wallet) || null;
    if (openHint) {
      const expectedCount = Math.max(0, Number(openHint.positionsCount || 0));
      const materializedCount = nextRows.length;
      if (expectedCount <= 0 || materializedCount >= expectedCount) {
        this.accountOpenHints.delete(wallet);
      } else {
        this.accountOpenHints.set(wallet, {
          ...openHint,
          materializedPositions: materializedCount,
          lastPositionsRefreshAt: observedAt,
          missCount:
            materializedCount > 0
              ? Math.max(0, Number(openHint.missCount || 0) - 1)
              : Math.max(0, Number(openHint.missCount || 0) + 1),
          lastReason: materializedCount > 0 ? "positions_gap_partial" : "positions_gap_empty",
        });
      }
      this.recomputeAccountOpenHintStatus();
    }
    if (liveState) {
      const expectedAccountPositions = Math.max(
        0,
        Number(
          openHint && openHint.positionsCount !== undefined
            ? openHint.positionsCount
            : liveState.accountPositionsCount
        ) || 0
      );
      if (newOpen) {
        this.transitionLivePositionState(wallet, "materialized_open", {
          at: observedAt,
          lastPositionsAt: observedAt,
          lastMaterializedOpenAt: observedAt,
          materializedPositionsCount: nextRows.length,
          positionsEmptyStreak: 0,
          pendingBootstrap:
            Math.max(0, Number(liveState.accountPositionsCount || 0)) > nextRows.length,
          reason: "positions_materialized",
        });
      } else {
        const nextPositionsEmptyStreak = Math.max(
          1,
          Number(liveState.positionsEmptyStreak || 0) + 1
        );
        if (expectedAccountPositions > 0) {
          this.transitionLivePositionState(wallet, "hinted_open", {
            at: observedAt,
            lastPositionsAt: observedAt,
            accountPositionsCount: expectedAccountPositions,
            materializedPositionsCount: 0,
            positionsEmptyStreak: nextPositionsEmptyStreak,
            pendingBootstrap: true,
            reason: "positions_gap_waiting_materialization",
          });
        } else {
          this.transitionLivePositionState(wallet, "closed", {
            at: observedAt,
            lastPositionsAt: observedAt,
            materializedPositionsCount: 0,
            positionsEmptyStreak: nextPositionsEmptyStreak,
            lastConfirmedClosedAt: observedAt,
            pendingBootstrap: false,
            reason: "positions_confirmed_closed",
          });
        }
      }
      this.recomputeLivePositionStateStatus();
    }
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

    if ((!oldOpen && newOpen) || (!newOpen && oldOpen)) {
      if (newOpen) this.openWalletSet.add(wallet);
      else this.openWalletSet.delete(wallet);
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
    const liveState = this.ensureLivePositionState(wallet);
    if (liveState && liveState.state !== "closed") {
      this.transitionLivePositionState(wallet, "uncertain", {
        at: now,
        lastPositionsErrorAt: now,
        lastPositionsError: message || "wallet_positions_failed",
        pendingBootstrap: true,
        reason: message || "wallet_positions_failed",
      });
      this.recomputeLivePositionStateStatus();
    }

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

  markWalletAccountError(wallet, error) {
    const now = Date.now();
    const message = String(error && error.message ? error.message : error || "");
    const previous = this.accountSnapshots.get(wallet) || { wallet };
    this.accountSnapshots.set(wallet, {
      ...previous,
      wallet,
      scannedAt: now,
      lastErrorAt: now,
      lastError: message || "wallet_account_failed",
      success: false,
    });
    this.touchLifecycle(wallet, {
      at: now,
      error: true,
      reason: message || "wallet_account_failed",
    });
    const liveState = this.ensureLivePositionState(wallet);
    if (liveState && liveState.state !== "closed") {
      this.transitionLivePositionState(wallet, "uncertain", {
        at: now,
        pendingBootstrap: true,
        reason: message || "wallet_account_failed",
      });
      this.recomputeLivePositionStateStatus();
    }
  }

  async accountCensusWallet(wallet) {
    const normalizedWallet = normalizeWallet(wallet);
    if (!normalizedWallet) return;
    if (this.walletScanLocks.has(normalizedWallet)) return;
    this.walletScanLocks.add(normalizedWallet);
    const scanStartedAt = Date.now();
    try {
      const lifecycle = this.ensureLifecycle(normalizedWallet);
      const liveState = this.ensureLivePositionState(normalizedWallet);
      const closeConfirmAccountZeroThreshold = Math.max(
        1,
        Number(this.status.closeConfirmAccountZeroThreshold || 2)
      );
      const snapshot = this.walletSnapshots.get(normalizedWallet) || null;
      const previousRowsCount = snapshot && Array.isArray(snapshot.positions) ? snapshot.positions.length : 0;
      const previousMaterializedOpen =
        previousRowsCount > 0 ||
        Math.max(0, Number(liveState && liveState.materializedPositionsCount || 0)) > 0;
      const accountSummary = await this.fetchWalletAccount(normalizedWallet);
      const observedAt = Date.now();
      this.accountSnapshots.set(normalizedWallet, {
        wallet: normalizedWallet,
        positionsCount: Number(accountSummary.positionsCount || 0),
        ordersCount: Number(accountSummary.ordersCount || 0),
        updatedAt: Number(accountSummary.updatedAt || observedAt),
        scannedAt: observedAt,
        success: true,
        lastErrorAt: null,
        lastError: null,
      });

      const positionsCount = Math.max(0, Number(accountSummary.positionsCount || 0));
      const lastPositionsAt = Math.max(
        0,
        Number(liveState && liveState.lastPositionsAt || 0),
        Number(snapshot && (snapshot.lastSuccessAt || snapshot.scannedAt) || 0)
      );
      const positionsFreshEnough =
        lastPositionsAt > 0 &&
        observedAt - lastPositionsAt <= Math.max(this.hotReconcileMaxAgeMs, this.scanIntervalMs * 4);

      if (positionsCount > 0) {
        this.noteAccountOpenHint(normalizedWallet, positionsCount, {
          updatedAt: accountSummary.updatedAt,
          observedAt,
          materializedPositions: previousRowsCount,
          reason: "account_positions_count",
        });
        if (
          previousRowsCount < positionsCount ||
          !positionsFreshEnough ||
          Boolean(liveState && liveState.pendingBootstrap) ||
          String(liveState && liveState.state || "") !== "materialized_open"
        ) {
          this.enqueuePriorityWallet(normalizedWallet);
        }
        if (this.hotWalletWsMonitor && typeof this.hotWalletWsMonitor.promoteWallet === "function") {
          const priorityMeta = this.walletPriorityMeta.get(normalizedWallet) || null;
          this.hotWalletWsMonitor.promoteWallet(normalizedWallet, {
            at: observedAt,
            reason: "account_positions_count",
            forceTier: "hot",
            openPositions: true,
            recentVolumeUsd: priorityMeta ? priorityMeta.recentVolumeUsd : 0,
            recentTradeCount: priorityMeta ? priorityMeta.recentTradeCount : 0,
            liveActiveRank: priorityMeta ? priorityMeta.liveActiveRank : 0,
            priorityBoost: priorityMeta ? priorityMeta.priorityBoost : 0,
            pinMs: this.hotReconcileMaxAgeMs * 4,
          });
        }
      } else {
        const nextZeroStreak = Math.max(
          0,
          Number(liveState && liveState.accountZeroStreak || 0) + 1
        );
        const needsCloseConfirmation =
          previousMaterializedOpen ||
          Boolean(liveState && liveState.pendingBootstrap) ||
          String(liveState && liveState.state || "") === "hinted_open" ||
          String(liveState && liveState.state || "") === "materialized_open";
        this.transitionLivePositionState(
          normalizedWallet,
          needsCloseConfirmation ? "uncertain" : "closed",
          {
            at: observedAt,
            lastAccountAt: observedAt,
            accountPositionsCount: 0,
            accountZeroStreak: nextZeroStreak,
            pendingBootstrap:
              needsCloseConfirmation &&
              nextZeroStreak < closeConfirmAccountZeroThreshold,
            reason:
              needsCloseConfirmation
                ? "account_zero_pending_positions_confirmation"
                : "account_zero_confirmed",
          }
        );
        if (needsCloseConfirmation) {
          this.enqueuePriorityWallet(normalizedWallet);
        }
        if (!needsCloseConfirmation || nextZeroStreak >= closeConfirmAccountZeroThreshold) {
          this.clearAccountOpenHint(normalizedWallet);
        }
      }

      this.status.lastSuccessAt = observedAt;
      this.status.scannedWalletsTotal = Number(this.status.scannedWalletsTotal || 0) + 1;
      this.successScannedWallets.add(normalizedWallet);
      this.status.walletsScannedAtLeastOnce = this.successScannedWallets.size;
      this.rateLimitStreak = 0;
      this.touchLifecycle(normalizedWallet, {
        at: observedAt,
        reconcile: true,
        success: true,
        openPositionsCount: positionsCount,
        reason: positionsCount > 0 ? "account_census_open" : "account_census_zero",
      });
      this.observeWalletScanDuration(Date.now() - scanStartedAt);
      this.persistAccountStateMaybe();
      this.persistMaybe();
    } catch (error) {
      const message = String(error && error.message ? error.message : error || "");
      if (message.includes("all_live_wallet_clients_cooling_down")) {
        this.status.lastErrorAt = Date.now();
        this.status.lastError = message;
        return;
      }
      this.markWalletAccountError(normalizedWallet, error);
      this.status.failedWalletsTotal = Number(this.status.failedWalletsTotal || 0) + 1;
      this.status.lastErrorAt = Date.now();
      this.status.lastError = message;
      const lifecycle = this.ensureLifecycle(normalizedWallet);
      if (
        lifecycle &&
        (lifecycle.lifecycle === "hot" ||
          lifecycle.lifecycle === "warm" ||
          this.accountOpenHints.has(normalizedWallet))
      ) {
        this.enqueuePriorityWallet(normalizedWallet);
      }
      this.observeWalletScanDuration(Date.now() - scanStartedAt);
      this.persistAccountStateMaybe();
      this.persistMaybe();
    } finally {
      this.walletScanLocks.delete(normalizedWallet);
    }
  }

  async scanWallet(wallet) {
    const normalizedWallet = normalizeWallet(wallet);
    if (!normalizedWallet) return;
    if (this.walletScanLocks.has(normalizedWallet)) return;
    this.walletScanLocks.add(normalizedWallet);
    const scanStartedAt = Date.now();
    try {
      const lifecycle = this.ensureLifecycle(normalizedWallet);
      const liveState = this.ensureLivePositionState(normalizedWallet);
      const closeConfirmAccountZeroThreshold = Math.max(
        1,
        Number(this.status.closeConfirmAccountZeroThreshold || 2)
      );
      const closeConfirmPositionsEmptyThreshold = Math.max(
        1,
        Number(this.status.closeConfirmPositionsEmptyThreshold || 1)
      );
      const likelyHot =
        this.openWalletSet.has(normalizedWallet) ||
        this.recentActiveWalletAt.has(normalizedWallet) ||
        Boolean(lifecycle && lifecycle.lifecycle === "hot") ||
        Boolean(liveState && liveState.state !== "closed");
      let accountSummary = null;
      let accountError = null;
      try {
        accountSummary = await this.fetchWalletAccount(normalizedWallet);
      } catch (error) {
        accountError = error;
        if (!likelyHot) throw error;
      }
      const observedAt = Date.now();
      if (accountSummary) {
        this.accountSnapshots.set(normalizedWallet, {
          wallet: normalizedWallet,
          positionsCount: Number(accountSummary.positionsCount || 0),
          ordersCount: Number(accountSummary.ordersCount || 0),
          updatedAt: Number(accountSummary.updatedAt || observedAt),
          scannedAt: observedAt,
        });
      }
      const positionsCount = Math.max(
        0,
        Number(accountSummary && accountSummary.positionsCount !== undefined ? accountSummary.positionsCount : 0)
      );
      if (accountSummary && positionsCount > 0) {
        this.noteAccountOpenHint(normalizedWallet, positionsCount, {
          updatedAt: accountSummary.updatedAt,
          observedAt,
          reason: "account_positions_count",
        });
      } else if (accountSummary) {
        const nextZeroStreak = Math.max(0, Number(liveState && liveState.accountZeroStreak || 0) + 1);
        this.transitionLivePositionState(normalizedWallet, liveState && liveState.materializedPositionsCount > 0 ? "uncertain" : "closed", {
          at: observedAt,
          lastAccountAt: observedAt,
          accountPositionsCount: 0,
          accountZeroStreak: nextZeroStreak,
          pendingBootstrap:
            Boolean(liveState && liveState.materializedPositionsCount > 0) &&
            nextZeroStreak < closeConfirmAccountZeroThreshold,
          reason:
            nextZeroStreak >= closeConfirmAccountZeroThreshold
              ? "account_zero_pending_close_confirmation"
              : "account_zero_uncertain",
        });
        if (nextZeroStreak >= closeConfirmAccountZeroThreshold) {
          this.clearAccountOpenHint(normalizedWallet);
        }
      }
      const shouldFetchPositions =
        positionsCount > 0 ||
        this.openWalletSet.has(normalizedWallet) ||
        likelyHot ||
        Boolean(liveState && liveState.pendingBootstrap);
      if (!shouldFetchPositions) {
        this.updateSnapshot(normalizedWallet, [], observedAt);
        this.status.lastSuccessAt = observedAt;
        this.status.scannedWalletsTotal = Number(this.status.scannedWalletsTotal || 0) + 1;
        this.successScannedWallets.add(normalizedWallet);
        this.status.walletsScannedAtLeastOnce = this.successScannedWallets.size;
        this.rateLimitStreak = 0;
        this.touchLifecycle(normalizedWallet, {
          at: observedAt,
          reconcile: true,
          success: true,
          openPositionsCount: 0,
          reason: accountSummary ? "account_scan_no_positions" : "positions_refresh_no_positions",
        });
        this.observeWalletScanDuration(Date.now() - scanStartedAt);
        this.persistMaybe();
        return;
      }

      if (positionsCount > 0) {
        this.openWalletSet.add(normalizedWallet);
        this.touchLifecycle(normalizedWallet, {
          at: observedAt,
          reconcile: true,
          openPositionsCount: positionsCount,
          reason: "account_positions_count",
        });
      }

      let rows = await this.fetchWalletPositions(normalizedWallet);
      const previousRows =
        this.walletSnapshots.has(normalizedWallet) &&
        this.walletSnapshots.get(normalizedWallet) &&
        Array.isArray(this.walletSnapshots.get(normalizedWallet).positions)
          ? this.walletSnapshots.get(normalizedWallet).positions
          : [];
      const previousRowsCount = previousRows.length;
      const openHint = this.accountOpenHints.get(normalizedWallet) || null;
      const directConfirmTarget = Math.max(
        positionsCount,
        previousRowsCount,
        Math.max(0, Number(liveState && liveState.materializedPositionsCount || 0)),
        Math.max(0, Number(openHint && openHint.positionsCount || 0)),
        Math.max(0, Number(liveState && liveState.lastMaterializedOpenAt ? 1 : 0))
      );
      if (directConfirmTarget > 0 && rows.length < directConfirmTarget) {
        rows = await this.confirmWalletPositionsWithDirect(normalizedWallet, directConfirmTarget, rows);
      }
      const nextPositionsEmptyStreak = rows.length === 0
        ? Math.max(1, Number(liveState && liveState.positionsEmptyStreak || 0) + 1)
        : 0;
      const closeConfirmed =
        rows.length === 0 &&
        positionsCount <= 0 &&
        Math.max(0, Number(liveState && liveState.accountZeroStreak || 0)) >= closeConfirmAccountZeroThreshold &&
        nextPositionsEmptyStreak >= closeConfirmPositionsEmptyThreshold;
      const previousRowsObservedAt = rowsLatestObservedAt(previousRows);
      const previousRowsFresh =
        previousRowsCount > 0 &&
        previousRowsObservedAt > 0 &&
        Math.max(0, observedAt - previousRowsObservedAt) <=
          Math.max(this.coolingMs, Math.floor(this.staleMs / 2));
      const preserveExistingSnapshot =
        rows.length === 0 &&
        !closeConfirmed &&
        (positionsCount > 0 ||
          previousRowsFresh ||
          (Boolean(liveState && liveState.state !== "closed") && previousRowsFresh));

      if (!preserveExistingSnapshot) {
        this.updateSnapshot(normalizedWallet, rows, observedAt);
      } else {
        this.transitionLivePositionState(normalizedWallet, positionsCount > 0 ? "hinted_open" : "uncertain", {
          at: observedAt,
          lastPositionsAt: observedAt,
          materializedPositionsCount: previousRowsCount,
          positionsEmptyStreak: nextPositionsEmptyStreak,
          pendingBootstrap: true,
          reason:
            positionsCount > 0
              ? "positions_gap_waiting_materialization"
              : "awaiting_close_confirmation",
        });
        this.recomputeLivePositionStateStatus();
      }
      if (positionsCount > rows.length) {
        this.noteAccountOpenHint(normalizedWallet, positionsCount, {
          updatedAt: accountSummary && accountSummary.updatedAt,
          observedAt,
          materializedPositions: preserveExistingSnapshot ? previousRowsCount : rows.length,
          missCount: rows.length === 0 ? 1 : 0,
          reason: rows.length === 0 ? "account_positions_gap_empty" : "account_positions_gap_partial",
        });
        this.enqueuePriorityWallet(normalizedWallet);
      } else if (positionsCount > 0 && !preserveExistingSnapshot) {
        this.clearAccountOpenHint(normalizedWallet);
      }
      this.status.lastSuccessAt = observedAt;
      this.status.scannedWalletsTotal = Number(this.status.scannedWalletsTotal || 0) + 1;
      this.successScannedWallets.add(normalizedWallet);
      this.status.walletsScannedAtLeastOnce = this.successScannedWallets.size;
      this.rateLimitStreak = 0;
      this.touchLifecycle(normalizedWallet, {
        at: observedAt,
        reconcile: true,
        success: true,
        openPositionsCount: preserveExistingSnapshot ? previousRowsCount : Array.isArray(rows) ? rows.length : 0,
        reason:
          accountError && !accountSummary
            ? "positions_refresh_fallback"
            : positionsCount > 0
            ? "account_and_positions_reconcile_success"
            : "rest_reconcile_success",
      });
      this.observeWalletScanDuration(Date.now() - scanStartedAt);
      this.persistMaybe();
    } catch (error) {
      const message = String(error && error.message ? error.message : error || "");
      if (message.includes("all_live_wallet_clients_cooling_down")) {
        this.status.lastErrorAt = Date.now();
        this.status.lastError = message;
        return;
      }
      this.markWalletError(normalizedWallet, error);
      this.status.failedWalletsTotal = Number(this.status.failedWalletsTotal || 0) + 1;
      this.status.lastErrorAt = Date.now();
      this.status.lastError = message;
      const lifecycle = this.ensureLifecycle(normalizedWallet);
      if (lifecycle && lifecycle.lifecycle === "hot") {
        this.enqueuePriorityWallet(normalizedWallet);
      }
      this.observeWalletScanDuration(Date.now() - scanStartedAt);
      this.persistMaybe();
    } finally {
      this.walletScanLocks.delete(normalizedWallet);
    }
  }

  async refreshWalletPositions(wallet) {
    const normalizedWallet = normalizeWallet(wallet);
    if (!normalizedWallet) return;
    if (this.walletNeedsFullScan(normalizedWallet)) {
      await this.scanWallet(normalizedWallet);
      return;
    }
    if (this.walletScanLocks.has(normalizedWallet)) return;
    this.walletScanLocks.add(normalizedWallet);
    const scanStartedAt = Date.now();
    try {
      const liveState = this.ensureLivePositionState(normalizedWallet);
      const closeConfirmAccountZeroThreshold = Math.max(
        1,
        Number(this.status.closeConfirmAccountZeroThreshold || 2)
      );
      const closeConfirmPositionsEmptyThreshold = Math.max(
        1,
        Number(this.status.closeConfirmPositionsEmptyThreshold || 1)
      );
      const expectedPositionsCount = Math.max(
        0,
        Number(liveState && liveState.accountPositionsCount || 0)
      );
      let rows = await this.fetchWalletPositions(normalizedWallet);
      const observedAt = Date.now();
      const previousRows =
        this.walletSnapshots.has(normalizedWallet) &&
        this.walletSnapshots.get(normalizedWallet) &&
        Array.isArray(this.walletSnapshots.get(normalizedWallet).positions)
          ? this.walletSnapshots.get(normalizedWallet).positions
          : [];
      const previousRowsCount = previousRows.length;
      const directConfirmTarget = Math.max(
        expectedPositionsCount,
        previousRowsCount,
        Math.max(0, Number(liveState && liveState.materializedPositionsCount || 0)),
        Math.max(0, Number(liveState && liveState.lastMaterializedOpenAt ? 1 : 0))
      );
      if (directConfirmTarget > 0 && rows.length < directConfirmTarget) {
        rows = await this.confirmWalletPositionsWithDirect(
          normalizedWallet,
          directConfirmTarget,
          rows
        );
      }
      const accountZeroStreak = Math.max(0, Number(liveState && liveState.accountZeroStreak || 0));
      const nextPositionsEmptyStreak = rows.length === 0
        ? Math.max(1, Number(liveState && liveState.positionsEmptyStreak || 0) + 1)
        : 0;
      const closeConfirmed =
        rows.length === 0 &&
        Number(liveState && liveState.accountPositionsCount || 0) <= 0 &&
        accountZeroStreak >= closeConfirmAccountZeroThreshold &&
        nextPositionsEmptyStreak >= closeConfirmPositionsEmptyThreshold;
      const previousRowsObservedAt = rowsLatestObservedAt(previousRows);
      const previousRowsFresh =
        previousRowsCount > 0 &&
        previousRowsObservedAt > 0 &&
        Math.max(0, observedAt - previousRowsObservedAt) <=
          Math.max(this.coolingMs, Math.floor(this.staleMs / 2));
      const preserveExistingSnapshot =
        rows.length === 0 &&
        !closeConfirmed &&
        (Number(liveState && liveState.accountPositionsCount || 0) > 0 ||
          previousRowsFresh ||
          (Boolean(liveState && liveState.state !== "closed") && previousRowsFresh));

      if (!preserveExistingSnapshot) {
        this.updateSnapshot(normalizedWallet, rows, observedAt);
      } else {
        this.transitionLivePositionState(
          normalizedWallet,
          Number(liveState && liveState.accountPositionsCount || 0) > 0 ? "hinted_open" : "uncertain",
          {
            at: observedAt,
            lastPositionsAt: observedAt,
            materializedPositionsCount: previousRowsCount,
            positionsEmptyStreak: nextPositionsEmptyStreak,
            pendingBootstrap: true,
            reason:
              Number(liveState && liveState.accountPositionsCount || 0) > 0
                ? "positions_refresh_gap"
                : "positions_refresh_awaiting_close_confirmation",
          }
        );
        this.recomputeLivePositionStateStatus();
      }
      this.status.lastSuccessAt = observedAt;
      this.status.scannedWalletsTotal = Number(this.status.scannedWalletsTotal || 0) + 1;
      this.successScannedWallets.add(normalizedWallet);
      this.status.walletsScannedAtLeastOnce = this.successScannedWallets.size;
      this.rateLimitStreak = 0;
      this.touchLifecycle(normalizedWallet, {
        at: observedAt,
        reconcile: true,
        success: true,
        openPositionsCount: preserveExistingSnapshot ? previousRowsCount : Array.isArray(rows) ? rows.length : 0,
        reason: "positions_refresh_success",
      });
      this.observeWalletScanDuration(Date.now() - scanStartedAt);
      this.persistMaybe();
    } catch (error) {
      const message = String(error && error.message ? error.message : error || "");
      if (message.includes("all_live_wallet_clients_cooling_down")) {
        this.status.lastErrorAt = Date.now();
        this.status.lastError = message;
        return;
      }
      this.markWalletError(normalizedWallet, error);
      this.status.failedWalletsTotal = Number(this.status.failedWalletsTotal || 0) + 1;
      this.status.lastErrorAt = Date.now();
      this.status.lastError = message;
      this.enqueuePriorityWallet(normalizedWallet);
      this.observeWalletScanDuration(Date.now() - scanStartedAt);
      this.persistMaybe();
    } finally {
      this.walletScanLocks.delete(normalizedWallet);
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
    if (Date.now() < this.rateLimitCooldownUntil) {
      this.status.lastPassFinishedAt = Date.now();
      this.persistAccountStateMaybe();
      this.persistMaybe();
      return;
    }
    this.running = true;
    const startedAt = Date.now();
    this.status.lastPassStartedAt = startedAt;

    try {
      if (this.wsHotOnly) {
        this.loadWalletRows();
        this.status.walletsKnown = this.walletList.length;
        this.status.walletsKnownGlobal = this.walletList.length;
        this.status.walletsScannedAtLeastOnce = Math.max(
          Number(this.status.walletsScannedAtLeastOnce || 0),
          this.walletSnapshots.size
        );
        this.updateCoverage();
        this.status.batchTargetWallets = 0;
        this.status.lastBatchWallets = 0;
        this.status.passes = Math.max(0, Number(this.status.passes || 0)) + 1;
        this.status.lastPassFinishedAt = Date.now();
        this.status.lastPassDurationMs = Math.max(
          1,
          Number(this.status.lastPassFinishedAt || 0) - startedAt
        );
        this.status.passThroughputRps = 0;
        this.persistAccountStateMaybe();
        this.persistMaybe();
        return;
      }
      if (this.workerMode === "positions_materializer") {
        this.refreshAccountPersistedState(false);
      }
      if (this.livePublicActiveRemoteEnabled) {
        this.refreshPublicActiveWallets(false).catch(() => {});
      }
      const availableSlots = this.getAvailableClientSlots();
      const dynamicMaxConcurrency =
        availableSlots > 0 ? Math.max(1, Math.min(this.maxConcurrency, availableSlots)) : 1;
      const startedAtMs = Math.max(0, Number(this.status.startedAt || 0));
      const bootstrapMode =
        !this.status.warmupDone ||
        Number(this.status.liveStatePendingBootstrapWallets || 0) > 0 ||
        Number(this.status.positionMaterializationGapWallets || 0) > 0 ||
        (startedAtMs > 0 && Date.now() - startedAtMs < 10 * 60 * 1000);
      const rescueTarget = this.computeGapRescueTarget(
        dynamicMaxConcurrency,
        bootstrapMode
      );
      const rescueBatch = this.pickAccountHintWallets(rescueTarget);
      const rescueWorkerCount = Math.max(
        0,
        Math.min(rescueBatch.length, rescueTarget)
      );
      const reservedAfterRescue = Math.max(
        0,
        dynamicMaxConcurrency - rescueWorkerCount
      );
      const minDiscoveryWorkers = Math.max(
        1,
        Math.min(
          reservedAfterRescue || dynamicMaxConcurrency,
          bootstrapMode
            ? Math.max(8, Math.floor((reservedAfterRescue || dynamicMaxConcurrency) * 0.25))
            : Math.max(4, Math.floor((reservedAfterRescue || dynamicMaxConcurrency) * 0.35))
        )
      );
      const hotTarget = Math.min(
        this.walletList.length || reservedAfterRescue,
        Math.max(
          0,
          Math.min(this.hotWalletsPerPass, this.openWalletSet.size),
          Math.min(this.recentActiveWalletsPerPass, this.recentActiveWalletAt.size),
          Math.min(this.hotWalletsPerPass, this.publicActiveWalletList.length),
          Math.min(reservedAfterRescue, this.priorityWalletQueue.length)
        )
      );
      const rescueSeen = new Set(rescueBatch);
      const hotBatch = this.pickHotRefreshWallets(hotTarget, rescueSeen);
      const hotWorkerCount = hotBatch.length
        ? Math.max(
            1,
            Math.min(
              hotBatch.length,
              Math.max(1, Math.floor((reservedAfterRescue || dynamicMaxConcurrency) * (bootstrapMode ? 0.35 : 0.25))),
              this.hotWalletsPerPass > 0 ? this.hotWalletsPerPass : (reservedAfterRescue || dynamicMaxConcurrency)
            )
          )
        : 0;
      const hotSeen = new Set([...rescueBatch, ...hotBatch]);
      const hintTarget = Math.max(
        0,
        Math.min(
          this.status.positionMaterializationGapWallets > 0
            ? Math.max(16, Math.floor((reservedAfterRescue || dynamicMaxConcurrency) * (bootstrapMode ? 0.35 : 0.2)))
            : 0,
          Math.max(0, (reservedAfterRescue || dynamicMaxConcurrency) - hotWorkerCount - minDiscoveryWorkers)
        )
      );
      const hintBatch = this.pickAccountHintWallets(hintTarget, hotSeen);
      const hintWorkerCount = Math.max(
        0,
        Math.min(
          hintBatch.length,
          hintBatch.length > 0
            ? Math.max(1, Math.min(hintBatch.length, Math.floor(dynamicMaxConcurrency * 0.35)))
            : 0
        )
      );
      const bootstrapTarget = Math.max(
        0,
        Math.min(
          Math.max(bootstrapMode ? 48 : 24, Math.floor(dynamicMaxConcurrency * (bootstrapMode ? 0.4 : 0.2))),
          Math.max(0, (reservedAfterRescue || dynamicMaxConcurrency) - hotWorkerCount - hintWorkerCount - minDiscoveryWorkers)
        )
      );
      const bootstrapSeed = new Set([...rescueBatch, ...hotBatch, ...hintBatch]);
      const positionsBootstrapBatch = this.pickPositionsBootstrapWallets(
        bootstrapTarget,
        bootstrapSeed
      );
      const positionsBootstrapWorkerCount = Math.max(
        0,
        Math.min(
          positionsBootstrapBatch.length,
          positionsBootstrapBatch.length > 0 ? bootstrapTarget : 0
        )
      );
      const discoveryTarget = this.computeBatchTarget(
        Math.max(
          1,
          Math.max(
            minDiscoveryWorkers,
            (reservedAfterRescue || dynamicMaxConcurrency) -
              hotWorkerCount -
              hintWorkerCount -
              positionsBootstrapWorkerCount
          )
        )
      );
      const discoverySeed = new Set([...hotBatch, ...hintBatch, ...positionsBootstrapBatch]);
      const discoveryBatch = this.pickDiscoveryWallets(discoveryTarget, discoverySeed);
      const hotSplit = this.splitWalletsByScanNeed(hotBatch, Date.now());
      const hotRefreshWorkerCount = Math.max(
        0,
        Math.min(hotSplit.refresh.length, hotWorkerCount)
      );
      const hotScanWorkerCount = Math.max(
        0,
        Math.min(hotSplit.scan.length, Math.max(0, hotWorkerCount - hotRefreshWorkerCount))
      );
      const hintSplit = this.splitWalletsByScanNeed(hintBatch, Date.now());
      const hintRefreshWorkerCount = Math.max(
        0,
        Math.min(hintSplit.refresh.length, hintWorkerCount)
      );
      const hintScanWorkerCount = Math.max(
        0,
        Math.min(hintSplit.scan.length, Math.max(0, hintWorkerCount - hintRefreshWorkerCount))
      );
      const positionsBootstrapSplit = this.splitWalletsByScanNeed(
        positionsBootstrapBatch,
        Date.now()
      );
      const positionsBootstrapRefreshWorkerCount = Math.max(
        0,
        Math.min(
          positionsBootstrapSplit.refresh.length,
          positionsBootstrapWorkerCount
        )
      );
      const positionsBootstrapScanWorkerCount = Math.max(
        0,
        Math.min(
          positionsBootstrapSplit.scan.length,
          Math.max(
            0,
            positionsBootstrapWorkerCount - positionsBootstrapRefreshWorkerCount
          )
        )
      );
      const batch = [...rescueBatch, ...hotBatch, ...hintBatch, ...positionsBootstrapBatch, ...discoveryBatch];
      this.status.lastBatchWallets = batch.length;
      if (!batch.length) {
        this.status.lastPassFinishedAt = Date.now();
        this.status.lastPassDurationMs = this.status.lastPassFinishedAt - startedAt;
        return;
      }

      const runWorkerPool = async (list, workerCount, runner) => {
        if (!Array.isArray(list) || !list.length || workerCount <= 0) return;
        let cursor = 0;
        const workers = Array.from({ length: workerCount }).map(async () => {
          while (true) {
            const index = cursor;
            cursor += 1;
            if (index >= list.length) break;
            const wallet = list[index];
            if (!wallet) continue;
            // eslint-disable-next-line no-await-in-loop
            await runner.call(this, wallet);
          }
        });
        await Promise.all(workers);
      };

      const discoveryWorkerCount = Math.max(
        0,
        Math.min(
          Math.max(
            minDiscoveryWorkers,
            dynamicMaxConcurrency -
              hotWorkerCount -
              hintWorkerCount -
              positionsBootstrapWorkerCount
          ),
          discoveryBatch.length
        )
      );

      await Promise.all([
        runWorkerPool(
          rescueBatch,
          rescueWorkerCount,
          this.refreshWalletPositions
        ),
        runWorkerPool(
          hotSplit.refresh,
          hotRefreshWorkerCount,
          this.refreshWalletPositions
        ),
        runWorkerPool(
          hotSplit.scan,
          hotScanWorkerCount,
          this.scanWallet
        ),
        runWorkerPool(
          hintSplit.refresh,
          hintRefreshWorkerCount,
          this.refreshWalletPositions
        ),
        runWorkerPool(hintSplit.scan, hintScanWorkerCount, this.scanWallet),
        runWorkerPool(
          positionsBootstrapSplit.refresh,
          positionsBootstrapRefreshWorkerCount,
          this.refreshWalletPositions
        ),
        runWorkerPool(
          positionsBootstrapSplit.scan,
          positionsBootstrapScanWorkerCount,
          this.scanWallet
        ),
        runWorkerPool(
          discoveryBatch,
          discoveryWorkerCount,
          this.accountCensusWallet
        ),
      ]);

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

  reconcileSnapshotBackedLiveStates(nowMs = Date.now()) {
    for (const [wallet, snapshot] of this.walletSnapshots.entries()) {
      if (!this.walletBelongsToShard(wallet)) continue;
      const positions = snapshot && Array.isArray(snapshot.positions) ? snapshot.positions : [];
      const rowsCount = positions.length;
      if (rowsCount <= 0) continue;
      const liveState = this.ensureLivePositionState(wallet);
      if (!liveState) continue;
      const observedAt = Math.max(
        0,
        Number(snapshot.lastSuccessAt || snapshot.scannedAt || nowMs)
      ) || nowMs;
      const hint = this.accountOpenHints.get(wallet) || null;
      const accountPositionsCount = Math.max(
        rowsCount,
        Number(
          hint && hint.positionsCount !== undefined
            ? hint.positionsCount
            : liveState.accountPositionsCount
        ) || 0
      );
      this.transitionLivePositionState(wallet, "materialized_open", {
        at: observedAt,
        lastAccountAt: Math.max(
          Number(liveState.lastAccountAt || 0) || 0,
          Number(hint && hint.lastAccountAt ? hint.lastAccountAt : 0) || 0
        ),
        accountPositionsCount,
        lastPositionsAt: observedAt,
        lastMaterializedOpenAt: observedAt,
        materializedPositionsCount: rowsCount,
        positionsEmptyStreak: 0,
        pendingBootstrap: accountPositionsCount > rowsCount,
        reason: "snapshot_consistency_reconcile",
      });
      this.openWalletSet.add(wallet);
    }
    this.recomputeLivePositionStateStatus();
  }

  getPositionsRows(nowMs = Date.now()) {
    this.reconcileSnapshotBackedLiveStates(nowMs);
    this.prunePositionCloseTombstones(nowMs);
    if (!this.flatPositionsDirty) return this.flatPositionsCache;
    const rowsByKey = new Map();
    for (const [wallet, snapshot] of this.walletSnapshots.entries()) {
      if (!this.walletBelongsToShard(wallet)) continue;
      const positions = snapshot && Array.isArray(snapshot.positions) ? snapshot.positions : [];
      const lastSuccessAt = Number(snapshot.lastSuccessAt || snapshot.scannedAt || 0);
      const ageMs = lastSuccessAt > 0 ? Math.max(0, nowMs - lastSuccessAt) : Infinity;
      const freshness = ageMs <= this.coolingMs ? "fresh" : ageMs <= this.staleMs ? "cooling" : "stale";
      const tombstones = this.positionCloseTombstones.get(wallet) || null;
      positions.forEach((row) => {
        const observedAt = Number(
          row.observedAt || row.lastObservedAt || lastSuccessAt || nowMs
        );
        const logicalKey = logicalPositionKey(row);
        if (logicalKey && tombstones && tombstones.has(logicalKey)) {
          const closedAt = Number((tombstones.get(logicalKey) || {}).at || 0);
          if (closedAt >= observedAt) return;
        }
        const normalizedRow = {
          ...row,
          wallet,
          trackedWallet: true,
          freshness,
          status: freshness,
          observedAt,
          lastObservedAt: observedAt,
          timestamp: Number(row.timestamp || observedAt || row.updatedAt || lastSuccessAt || nowMs),
          updatedAt: Number(row.updatedAt || lastSuccessAt || nowMs),
          lastUpdatedAt: Number(
            row.lastUpdatedAt || row.updatedAt || lastSuccessAt || nowMs
          ),
        };
        const key = `${wallet}:${logicalKey || normalizedRow.positionKey || positionKey(normalizedRow)}`;
        const existing = rowsByKey.get(key) || null;
        if (!existing || Number(existing.observedAt || 0) <= Number(normalizedRow.observedAt || 0)) {
          rowsByKey.set(key, normalizedRow);
        }
      });
    }

    for (const [wallet, liveState] of this.livePositionStates.entries()) {
      if (!this.walletBelongsToShard(wallet)) continue;
      if (!liveState || String(liveState.state || "") === "closed") continue;
      const provisionalRow = buildProvisionalPositionRow(wallet, liveState, nowMs);
      if (!provisionalRow) continue;
      const logicalKey = logicalPositionKey(provisionalRow);
      const tombstones = this.positionCloseTombstones.get(wallet) || null;
      if (logicalKey && tombstones && tombstones.has(logicalKey)) {
        const closedAt = Number((tombstones.get(logicalKey) || {}).at || 0);
        if (closedAt >= Number(provisionalRow.observedAt || 0)) continue;
      }
      const key = `${wallet}:${logicalKey || provisionalRow.positionKey}`;
      const existing = rowsByKey.get(key) || null;
      if (!existing || Number(existing.observedAt || 0) <= Number(provisionalRow.observedAt || 0)) {
        rowsByKey.set(key, provisionalRow);
      }
    }

    for (const [wallet, provisionalRows] of this.provisionalPositions.entries()) {
      if (!this.walletBelongsToShard(wallet) || !provisionalRows) continue;
      const tombstones = this.positionCloseTombstones.get(wallet) || null;
      for (const [logicalKey, row] of provisionalRows.entries()) {
        if (!row) continue;
        if (tombstones && tombstones.has(logicalKey)) {
          const closedAt = Number((tombstones.get(logicalKey) || {}).at || 0);
          if (closedAt >= Number(row.observedAt || 0)) continue;
        }
        const key = `${wallet}:${logicalKey || row.positionKey || positionKey(row)}`;
        const existing = rowsByKey.get(key) || null;
        if (!existing || Number(existing.observedAt || 0) <= Number(row.observedAt || 0)) {
          rowsByKey.set(key, {
            ...row,
            wallet,
            trackedWallet: true,
            freshness: "fresh",
            status: row.status || "provisional",
            provisional: true,
            source: row.source || "wallet_first_live_provisional",
          });
        }
      }
    }

    const rows = Array.from(rowsByKey.values()).filter((row) => {
      if (!row) return false;
      if (row.provisional || String(row.status || "").trim().toLowerCase() === "provisional") return false;
      const sizeValue = Number.isFinite(toNum(row.size, NaN))
        ? Math.abs(Number(toNum(row.size, 0)))
        : Number.isFinite(toNum(row.amount, NaN))
        ? Math.abs(Number(toNum(row.amount, 0)))
        : Number.isFinite(toNum(row.positionUsd, NaN))
        ? Math.abs(Number(toNum(row.positionUsd, 0)))
        : 0;
      return sizeValue > 0;
    });
    const openWallets = new Set(rows.map((row) => row.wallet).filter(Boolean));
    rows.sort((a, b) => {
      const aObserved = Number(a.observedAt || a.lastObservedAt || a.timestamp || a.updatedAt || 0);
      const bObserved = Number(b.observedAt || b.lastObservedAt || b.timestamp || b.updatedAt || 0);
      if (bObserved !== aObserved) return bObserved - aObserved;
      return Number(b.updatedAt || 0) - Number(a.updatedAt || 0);
    });
    this.flatPositionsCache = rows;
    this.flatPositionsDirty = false;
    this.status.openPositionsTotal = rows.length;
    this.status.walletsWithOpenPositions = openWallets.size;
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
        direction:
          row.direction ||
          normalizePositionDirection(row.side || ""),
        amount: Number.isFinite(toNum(row.newSize, NaN)) ? Number(row.newSize) : 0,
        price: Number.isFinite(toNum(row.entry, NaN)) ? Number(toNum(row.entry, 0).toFixed(8)) : NaN,
        entry: Number.isFinite(toNum(row.entry, NaN)) ? Number(toNum(row.entry, 0).toFixed(8)) : null,
        mark: Number.isFinite(toNum(row.mark, NaN)) ? Number(toNum(row.mark, 0).toFixed(8)) : null,
        positionUsd: (() => {
          const v = Number.isFinite(toNum(row && row.positionUsd, NaN))
            ? Number(toNum(row.positionUsd, 0).toFixed(2))
            : 0;
          return v;
        })(),
        margin: (() => {
          const primary = toNum(row && row.margin, NaN);
          const secondary = toNum(row && row.marginUsd, NaN);
          if (Number.isFinite(primary) && primary > 0) return Number(primary.toFixed(2));
          if (Number.isFinite(secondary) && secondary > 0) return Number(secondary.toFixed(2));
          return null;
        })(),
        marginUsd: (() => {
          const primary = toNum(row && row.marginUsd, NaN);
          const secondary = toNum(row && row.margin, NaN);
          if (Number.isFinite(primary) && primary > 0) return Number(primary.toFixed(2));
          if (Number.isFinite(secondary) && secondary > 0) return Number(secondary.toFixed(2));
          return null;
        })(),
        leverage: (() => {
          const leverageRaw = toNum(row && row.leverage, NaN);
          if (Number.isFinite(leverageRaw) && leverageRaw > 0) return Number(leverageRaw.toFixed(4));
          const marginValue = Number.isFinite(toNum(row && row.margin, NaN)) && toNum(row.margin, 0) > 0
            ? Number(toNum(row.margin, 0))
            : Number.isFinite(toNum(row && row.marginUsd, NaN)) && toNum(row.marginUsd, 0) > 0
            ? Number(toNum(row.marginUsd, 0))
            : NaN;
          const positionUsdValue = Number.isFinite(toNum(row && row.positionUsd, NaN))
            ? Number(toNum(row.positionUsd, 0))
            : NaN;
          return Number.isFinite(marginValue) && marginValue > 0 && Number.isFinite(positionUsdValue) && positionUsdValue > 0
            ? Number((positionUsdValue / marginValue).toFixed(4))
            : null;
        })(),
        isolated:
          row && row.isolated !== undefined
            ? Boolean(row.isolated)
            : row && row.raw && typeof row.raw === "object" && Object.prototype.hasOwnProperty.call(row.raw, "isolated")
            ? Boolean(row.raw.isolated)
            : false,
        unrealizedPnlUsd: Number.isFinite(toNum(row.unrealizedPnlUsd, NaN))
          ? Number(toNum(row.unrealizedPnlUsd, 0).toFixed(2))
          : null,
        oldSize: Number.isFinite(toNum(row.oldSize, NaN))
          ? Number(toNum(row.oldSize, 0).toFixed(8))
          : null,
        newSize: Number.isFinite(toNum(row.newSize, NaN))
          ? Number(toNum(row.newSize, 0).toFixed(8))
          : null,
        openedAt: Number(row.openedAt || row.at || now) || now,
        updatedAt: Number(row.updatedAt || row.observedAt || row.at || now) || now,
        observedAt: Number(row.observedAt || row.at || now) || now,
        positionKey: row.positionKey || row.key || null,
        timestamp: Number(row.at || now),
        sideEvent: row.type,
        cause: row.type,
        source: "wallet_first_position_change",
        walletSource: "wallet_positions_api",
        walletConfidence: "hard_payload",
        raw:
          row && row.raw && typeof row.raw === "object"
            ? row.raw
            : { isolated: row && row.isolated !== undefined ? Boolean(row.isolated) : false },
      }));
    return rows;
  }

  getRecentPositionChangeEvents(limit = 200) {
    const safeLimit = Math.max(1, Math.min(this.maxPersistedEvents, Number(limit || 200)));
    const now = Date.now();
    const normalizeEventType = (row) => {
      const raw = String((row && (row.type || row.cause || row.sideEvent)) || "")
        .trim()
        .toLowerCase();
      if (!raw) return null;
      if (raw.includes("position_opened")) return "position_opened";
      if (raw.includes("position_closed_partial")) return "position_closed_partial";
      if (raw.includes("position_closed")) return "position_closed";
      if (raw.includes("position_size_changed")) {
        const oldSize = toNum(row && row.oldSize, NaN);
        const newSize = toNum(row && row.newSize, NaN);
        if (Number.isFinite(oldSize) && Number.isFinite(newSize)) {
          if (newSize <= 1e-10) return "position_closed";
          if (newSize < oldSize) return "position_closed_partial";
          if (newSize > oldSize) return "position_increased";
        }
        return "position_size_changed";
      }
      return null;
    };
    return this.positionChangeEvents
      .slice(Math.max(0, this.positionChangeEvents.length - safeLimit))
      .reverse()
      .map((row, idx) => {
        const eventType = normalizeEventType(row);
        if (!eventType) return null;
        const entry = Number.isFinite(toNum(row && row.entry, NaN))
          ? Number(toNum(row.entry, 0).toFixed(8))
          : null;
        const mark = Number.isFinite(toNum(row && row.mark, NaN))
          ? Number(toNum(row.mark, 0).toFixed(8))
          : null;
        const oldSize = Number.isFinite(toNum(row && row.oldSize, NaN))
          ? Number(toNum(row.oldSize, 0).toFixed(8))
          : null;
        const newSize = Number.isFinite(toNum(row && row.newSize, NaN))
          ? Number(toNum(row.newSize, 0).toFixed(8))
          : Number.isFinite(toNum(row && row.amount, NaN))
          ? Number(toNum(row.amount, 0).toFixed(8))
          : null;
        let sizeDelta = null;
        if (Number.isFinite(oldSize) && Number.isFinite(newSize)) {
          sizeDelta = Number((newSize - oldSize).toFixed(8));
        } else if (eventType === "position_opened" && Number.isFinite(newSize)) {
          sizeDelta = Number(newSize.toFixed(8));
        } else if (eventType === "position_closed" && Number.isFinite(oldSize)) {
          sizeDelta = Number((-Math.abs(oldSize)).toFixed(8));
        }
        const anchorPrice = Number.isFinite(entry) ? entry : Number.isFinite(mark) ? mark : null;
        const eventUsd = Number.isFinite(toNum(row && row.eventUsd, NaN))
          ? Number(toNum(row.eventUsd, 0).toFixed(2))
          : Number.isFinite(sizeDelta) && Number.isFinite(anchorPrice)
          ? Number(Math.abs(sizeDelta * anchorPrice).toFixed(2))
          : Number.isFinite(toNum(row && row.positionUsd, NaN))
          ? Number(toNum(row.positionUsd, 0).toFixed(2))
          : 0;
        const positionUsdValue = Number.isFinite(toNum(row && row.positionUsd, NaN))
          ? Number(toNum(row.positionUsd, 0).toFixed(2))
          : 0;
        const marginPrimary = toNum(row && row.margin, NaN);
        const marginSecondary = toNum(row && row.marginUsd, NaN);
        const marginValue =
          Number.isFinite(marginPrimary) && marginPrimary > 0
            ? Number(marginPrimary.toFixed(2))
            : Number.isFinite(marginSecondary) && marginSecondary > 0
            ? Number(marginSecondary.toFixed(2))
            : null;
        const leverageRaw = toNum(row && row.leverage, NaN);
        const leverageValue =
          Number.isFinite(leverageRaw) && leverageRaw > 0
            ? Number(leverageRaw.toFixed(4))
            : Number.isFinite(positionUsdValue) && positionUsdValue > 0 && Number.isFinite(marginValue) && marginValue > 0
            ? Number((positionUsdValue / marginValue).toFixed(4))
            : null;
        const isolatedValue =
          row && row.isolated !== undefined
            ? Boolean(row.isolated)
            : row && row.raw && typeof row.raw === "object" && Object.prototype.hasOwnProperty.call(row.raw, "isolated")
            ? Boolean(row.raw.isolated)
            : false;
        return {
          id: `${row.at || now}:${row.wallet || "na"}:${row.positionKey || row.key || row.symbol || "na"}:${idx}`,
          historyId: null,
          wallet: row.wallet,
          symbol: row.symbol,
          side: row.side,
          direction: row.direction || normalizePositionDirection(row.side || ""),
          amount: Number.isFinite(toNum(newSize, NaN)) ? Number(newSize) : 0,
          oldSize,
          newSize,
          sizeDelta,
          sizeDeltaAbs: Number.isFinite(toNum(sizeDelta, NaN)) ? Number(Math.abs(sizeDelta).toFixed(8)) : null,
          price: Number.isFinite(toNum(entry, NaN)) ? entry : NaN,
          entry,
          mark,
          positionUsd: positionUsdValue,
          eventUsd,
          margin: marginValue,
          marginUsd: marginValue,
          leverage: leverageValue,
          isolated: isolatedValue,
          unrealizedPnlUsd: Number.isFinite(toNum(row && row.unrealizedPnlUsd, NaN))
            ? Number(toNum(row.unrealizedPnlUsd, 0).toFixed(2))
            : null,
          openedAt: Number(row && (row.openedAt || row.at) || now) || now,
          updatedAt: Number(row && (row.updatedAt || row.observedAt || row.at) || now) || now,
          observedAt: Number(row && (row.observedAt || row.at) || now) || now,
          detectedAt: Number(row && (row.observedAt || row.updatedAt || row.at) || now) || now,
          positionKey: row && (row.positionKey || row.key) || null,
          timestamp: Number(row && row.at || now),
          eventType,
          sideEvent: eventType,
          cause: eventType,
          source: row && row.source ? row.source : "wallet_first_position_change",
          walletSource: row && row.walletSource ? row.walletSource : "wallet_positions_api",
          walletConfidence: row && (row.walletConfidence || row.confidence) ? row.walletConfidence || row.confidence : "hard_payload",
          provisional: Boolean(row && row.provisional),
          raw:
            row && row.raw && typeof row.raw === "object"
              ? row.raw
              : { isolated: isolatedValue },
        };
      })
      .filter((row) => {
        if (!row) return false;
        const hasEntry = Number.isFinite(toNum(row.entry, NaN)) && toNum(row.entry, 0) > 0;
        const hasNotional =
          (Number.isFinite(toNum(row.positionUsd, NaN)) && Math.abs(toNum(row.positionUsd, 0)) > 0) ||
          (Number.isFinite(toNum(row.eventUsd, NaN)) && Math.abs(toNum(row.eventUsd, 0)) > 0);
        const hasSize =
          (Number.isFinite(toNum(row.sizeDeltaAbs, NaN)) && toNum(row.sizeDeltaAbs, 0) > 0) ||
          (Number.isFinite(toNum(row.newSize, NaN)) && Math.abs(toNum(row.newSize, 0)) > 0) ||
          (Number.isFinite(toNum(row.amount, NaN)) && Math.abs(toNum(row.amount, 0)) > 0);
        return hasEntry && hasNotional && hasSize;
      });
  }

  getRecentPositionOpenedEvents(limit = 200) {
    const safeLimit = Math.max(
      1,
      Math.min(this.maxPersistedPositionOpenedEvents, Number(limit || 200))
    );
    const now = Date.now();
    return this.positionOpenedEvents
      .slice(Math.max(0, this.positionOpenedEvents.length - safeLimit))
      .reverse()
      .map((row, idx) => ({
        id: `${row.at || row.openedAt || now}:${row.wallet || "na"}:${row.positionKey || row.key || row.symbol || "na"}:${idx}`,
        historyId: null,
        wallet: row.wallet,
        symbol: row.symbol,
        side: row.side,
        direction:
          row.direction ||
          normalizePositionDirection(row.side || ""),
        amount: Number.isFinite(toNum(row.amount, NaN))
          ? Number(row.amount)
          : Number.isFinite(toNum(row.newSize, NaN))
          ? Number(row.newSize)
          : 0,
        newSize: Number.isFinite(toNum(row.newSize, NaN))
          ? Number(row.newSize)
          : Number.isFinite(toNum(row.amount, NaN))
          ? Number(row.amount)
          : 0,
        price: Number.isFinite(toNum(row.entry, NaN)) ? Number(toNum(row.entry, 0).toFixed(8)) : NaN,
        entry: Number.isFinite(toNum(row.entry, NaN)) ? Number(toNum(row.entry, 0).toFixed(8)) : null,
        mark: Number.isFinite(toNum(row.mark, NaN)) ? Number(toNum(row.mark, 0).toFixed(8)) : null,
        positionUsd: (() => {
          const v = Number.isFinite(toNum(row && row.positionUsd, NaN))
            ? Number(toNum(row.positionUsd, 0).toFixed(2))
            : 0;
          return v;
        })(),
        margin: (() => {
          const primary = toNum(row && row.margin, NaN);
          const secondary = toNum(row && row.marginUsd, NaN);
          if (Number.isFinite(primary) && primary > 0) return Number(primary.toFixed(2));
          if (Number.isFinite(secondary) && secondary > 0) return Number(secondary.toFixed(2));
          return null;
        })(),
        marginUsd: (() => {
          const primary = toNum(row && row.marginUsd, NaN);
          const secondary = toNum(row && row.margin, NaN);
          if (Number.isFinite(primary) && primary > 0) return Number(primary.toFixed(2));
          if (Number.isFinite(secondary) && secondary > 0) return Number(secondary.toFixed(2));
          return null;
        })(),
        leverage: (() => {
          const leverageRaw = toNum(row && row.leverage, NaN);
          if (Number.isFinite(leverageRaw) && leverageRaw > 0) return Number(leverageRaw.toFixed(4));
          const marginValue = Number.isFinite(toNum(row && row.margin, NaN)) && toNum(row.margin, 0) > 0
            ? Number(toNum(row.margin, 0))
            : Number.isFinite(toNum(row && row.marginUsd, NaN)) && toNum(row.marginUsd, 0) > 0
            ? Number(toNum(row.marginUsd, 0))
            : NaN;
          const positionUsdValue = Number.isFinite(toNum(row && row.positionUsd, NaN))
            ? Number(toNum(row.positionUsd, 0))
            : NaN;
          return Number.isFinite(marginValue) && marginValue > 0 && Number.isFinite(positionUsdValue) && positionUsdValue > 0
            ? Number((positionUsdValue / marginValue).toFixed(4))
            : null;
        })(),
        isolated:
          row && row.isolated !== undefined
            ? Boolean(row.isolated)
            : row && row.raw && typeof row.raw === "object" && Object.prototype.hasOwnProperty.call(row.raw, "isolated")
            ? Boolean(row.raw.isolated)
            : false,
        unrealizedPnlUsd: Number.isFinite(toNum(row.unrealizedPnlUsd, NaN))
          ? Number(toNum(row.unrealizedPnlUsd, 0).toFixed(2))
          : null,
        openedAt: Number(row.openedAt || row.at || now) || now,
        updatedAt: Number(row.updatedAt || row.observedAt || row.at || now) || now,
        observedAt: Number(row.observedAt || row.at || now) || now,
        positionKey: row.positionKey || row.key || null,
        timestamp: Number(row.at || row.openedAt || now),
        sideEvent: row.type || "position_opened",
        cause: row.type || "position_opened",
        source: row.source || "wallet_first_position_change",
        walletSource: row.walletSource || "wallet_positions_api",
        walletConfidence: row.walletConfidence || row.confidence || "hard_payload",
        provisional: Boolean(row.provisional),
        expectedOpenPositions:
          row.expectedOpenPositions !== undefined ? Number(row.expectedOpenPositions || 0) : null,
        materializedPositionsCount:
          row.materializedPositionsCount !== undefined
            ? Number(row.materializedPositionsCount || 0)
            : null,
        raw:
          row && row.raw && typeof row.raw === "object"
            ? row.raw
            : { isolated: row && row.isolated !== undefined ? Boolean(row.isolated) : false },
      }))
      .filter((row) => {
        if (!row) return false;
        const hasEntry = Number.isFinite(toNum(row.entry, NaN)) && toNum(row.entry, 0) > 0;
        const hasNotional = Number.isFinite(toNum(row.positionUsd, NaN)) && Math.abs(toNum(row.positionUsd, 0)) > 0;
        const hasSize =
          (Number.isFinite(toNum(row.amount, NaN)) && Math.abs(toNum(row.amount, 0)) > 0) ||
          (Number.isFinite(toNum(row.newSize, NaN)) && Math.abs(toNum(row.newSize, 0)) > 0);
        return hasEntry && hasNotional && hasSize;
      });
  }

  getStatus() {
    this.updateRecentCounters();
    this.updateCoverage();
    this.recomputeAccountOpenHintStatus();
    this.recomputeLivePositionStateStatus();
    const now = Date.now();
    const quantile = (values = [], p = 0.5) => {
      const sorted = (Array.isArray(values) ? values : [])
        .map((value) => Number(value))
        .filter((value) => Number.isFinite(value))
        .sort((a, b) => a - b);
      if (!sorted.length) return 0;
      const idx = Math.min(sorted.length - 1, Math.max(0, Math.round(p * (sorted.length - 1))));
      return Math.round(sorted[idx]);
    };
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
    const rawLastError = String(this.status.lastError || "").trim() || null;
    const rawLastErrorAt = Number(this.status.lastErrorAt || 0) || 0;
    const rawLastSuccessAt = Number(this.status.lastSuccessAt || 0) || 0;
    const effectiveLastError =
      rawLastError && rawLastErrorAt > 0 && rawLastErrorAt >= rawLastSuccessAt
        ? rawLastError
        : null;
    const effectiveLastErrorAt =
      effectiveLastError && rawLastErrorAt > 0 ? rawLastErrorAt : null;
    const hotStatus =
      this.hotWalletWsMonitor && typeof this.hotWalletWsMonitor.getStatus === "function"
        ? this.hotWalletWsMonitor.getStatus()
        : null;
    const recentEvents1m = this.events.filter(
      (row) =>
        now -
          normalizeTimestampMs(row && (row.observedAt || row.at || row.timestamp), 0) <=
        60 * 1000
    );
    const recentEvents5m = this.events.filter(
      (row) =>
        now -
          normalizeTimestampMs(row && (row.observedAt || row.at || row.timestamp), 0) <=
        5 * 60 * 1000
    );
    const recentOpened1m = this.positionOpenedEvents.filter(
      (row) =>
        now -
          normalizeTimestampMs(row && (row.observedAt || row.timestamp || row.openedAt), 0) <=
        60 * 1000
    );
    const recentOpened5m = this.positionOpenedEvents.filter(
      (row) =>
        now -
          normalizeTimestampMs(row && (row.observedAt || row.timestamp || row.openedAt), 0) <=
        5 * 60 * 1000
    );
    const recentPositionChanges1m = this.positionChangeEvents.filter(
      (row) =>
        now -
          normalizeTimestampMs(
            row && (row.detectedAt || row.observedAt || row.updatedAt || row.timestamp || row.at),
            0
          ) <=
        60 * 1000
    );
    const recentPositionChanges5m = this.positionChangeEvents.filter(
      (row) =>
        now -
          normalizeTimestampMs(
            row && (row.detectedAt || row.observedAt || row.updatedAt || row.timestamp || row.at),
            0
          ) <=
        5 * 60 * 1000
    );
    const detectionLagMs1m = recentOpened1m
      .map((row) => {
        const openedAt = normalizeTimestampMs(row && (row.openedAt || row.timestamp), 0);
        const observedAt = normalizeTimestampMs(
          row && (row.observedAt || row.timestamp || row.updatedAt),
          0
        );
        return openedAt > 0 && observedAt > 0 ? Math.max(0, observedAt - openedAt) : null;
      })
      .filter((value) => Number.isFinite(value));
    const detectionLagMs5m = recentOpened5m
      .map((row) => {
        const openedAt = normalizeTimestampMs(row && (row.openedAt || row.timestamp), 0);
        const observedAt = normalizeTimestampMs(
          row && (row.observedAt || row.timestamp || row.updatedAt),
          0
        );
        return openedAt > 0 && observedAt > 0 ? Math.max(0, observedAt - openedAt) : null;
      })
      .filter((value) => Number.isFinite(value));
    const classifyPositionEventType = (row) => {
      const raw = String((row && (row.type || row.cause || row.sideEvent)) || "")
        .trim()
        .toLowerCase();
      if (!raw) return null;
      if (raw.includes("position_opened")) return "opened";
      if (raw.includes("position_increased")) return "increased";
      if (raw.includes("position_closed_partial")) return "partial";
      if (raw.includes("position_closed")) return "closed";
      if (!raw.includes("position_size_changed")) return null;
      const oldSize = toNum(row && row.oldSize, NaN);
      const newSize = toNum(row && row.newSize, NaN);
      if (!Number.isFinite(oldSize) || !Number.isFinite(newSize)) return null;
      if (newSize <= 1e-10) return "closed";
      if (newSize < oldSize) return "partial";
      if (newSize > oldSize) return "increased";
      return null;
    };
    const summarizePositionEvents = (rows = []) => {
      const summary = {
        opened: 0,
        increased: 0,
        partial: 0,
        closed: 0,
      };
      (Array.isArray(rows) ? rows : []).forEach((row) => {
        const type = classifyPositionEventType(row);
        if (type && summary[type] !== undefined) summary[type] += 1;
      });
      return summary;
    };
    const positionEvents1m = summarizePositionEvents(recentPositionChanges1m);
    const positionEvents5m = summarizePositionEvents(recentPositionChanges5m);
    const isWsEvent = (row) => {
      const source = String((row && row.source) || "")
        .trim()
        .toLowerCase();
      return (
        source.startsWith("ws.") ||
        source.startsWith("ws_") ||
        source.startsWith("wallet_ws")
      );
    };
    return {
      ...this.status,
      lastError: effectiveLastError,
      lastErrorAt: effectiveLastErrorAt,
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
      accountRequestTimeoutMs: this.accountRequestTimeoutMs,
      accountSettingsRequestTimeoutMs: this.accountSettingsRequestTimeoutMs,
      accountSettingsCacheTtlMs: this.accountSettingsCacheTtlMs,
      accountSettingsFailureBackoffMs: this.accountSettingsFailureBackoffMs,
      maxFetchAttempts: this.maxFetchAttempts,
      maxInFlightPerClient: this.maxInFlightPerClient,
      maxInFlightDirect: this.maxInFlightDirect,
      directFallbackOnLastAttempt: this.directFallbackOnLastAttempt,
      staleMs: this.staleMs,
      coolingMs: this.coolingMs,
      clientsTotal: this.restClientEntries.length,
      proxiedClients: Math.max(
        0,
        this.restClientEntries.filter((entry) => entry && entry.proxyUrl).length
      ),
      directClientIncluded: this.directClientIndex >= 0,
      clientsCooling,
      clientsDisabled,
      clientsInFlight,
      clients429,
      healthyClients,
      avgClientLatencyMs: Math.round(avgClientLatencyMs || 0),
      timeoutClients,
      proxyFailingClients,
      accountSettingsCacheSize: this.accountSettingsCache.size,
      accountSettingsCacheHits: Number(this.accountSettingsCacheHits || 0),
      accountSettingsFetches: Number(this.accountSettingsFetches || 0),
      accountSettingsFetchFailures: Number(this.accountSettingsFetchFailures || 0),
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
      eventsPerMinute: recentEvents1m.length,
      eventsPer5Minutes: recentEvents5m.length,
      wsEventsPerMinute: recentEvents1m.filter(isWsEvent).length,
      wsEventsPer5Minutes: recentEvents5m.filter(isWsEvent).length,
      newOpenEventsPerMinute: recentOpened1m.length,
      newOpenEventsPer5Minutes: recentOpened5m.length,
      positionOpenedEventsPerMinute: positionEvents1m.opened,
      positionIncreasedEventsPerMinute: positionEvents1m.increased,
      positionPartialCloseEventsPerMinute: positionEvents1m.partial,
      positionClosedEventsPerMinute: positionEvents1m.closed,
      positionOpenedEventsPer5Minutes: positionEvents5m.opened,
      positionIncreasedEventsPer5Minutes: positionEvents5m.increased,
      positionPartialCloseEventsPer5Minutes: positionEvents5m.partial,
      positionClosedEventsPer5Minutes: positionEvents5m.closed,
      detectionLagP50Ms1m: quantile(detectionLagMs1m, 0.5),
      detectionLagP90Ms1m: quantile(detectionLagMs1m, 0.9),
      detectionLagP95Ms1m: quantile(detectionLagMs1m, 0.95),
      detectionLagP50Ms5m: quantile(detectionLagMs5m, 0.5),
      detectionLagP90Ms5m: quantile(detectionLagMs5m, 0.9),
      detectionLagP95Ms5m: quantile(detectionLagMs5m, 0.95),
      delayedOpenEventsOver5s1m: detectionLagMs1m.filter((value) => value > 5000).length,
      delayedOpenEventsOver5s5m: detectionLagMs5m.filter((value) => value > 5000).length,
      wsReconcileQueued: this.wsReconcileQueue.length,
      wsReconcileInFlight: this.wsReconcileInFlight,
      wsReconcileDispatches1m: this.pruneWsReconcileDispatch(now),
      wsReconcileMaxConcurrency: this.wsReconcileMaxConcurrency,
      wsReconcileCooldownMs: this.wsReconcileCooldownMs,
      lifecycleHotWallets,
      lifecycleWarmWallets,
      lifecycleColdWallets,
      staleWallets,
      freshWallets,
      coolingWallets,
      priorityQueueDepth: this.priorityWalletQueue.length,
      hotReconcileDueWallets: Number(this.status.hotReconcileDueWallets || 0),
      warmReconcileDueWallets: Number(this.status.warmReconcileDueWallets || 0),
      accountRepairDueWallets: Number(this.status.accountRepairDueWallets || 0),
      liveStateHintedWallets: Number(this.status.liveStateHintedWallets || 0),
      liveStateMaterializedWallets: Number(this.status.liveStateMaterializedWallets || 0),
      liveStateUncertainWallets: Number(this.status.liveStateUncertainWallets || 0),
      liveStateClosedWallets: Number(this.status.liveStateClosedWallets || 0),
      liveStatePendingBootstrapWallets: Number(this.status.liveStatePendingBootstrapWallets || 0),
      closeConfirmAccountZeroThreshold: Number(this.status.closeConfirmAccountZeroThreshold || 2),
      closeConfirmPositionsEmptyThreshold: Number(this.status.closeConfirmPositionsEmptyThreshold || 1),
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
    this.pruneToShardOwnership(false);
    this.reconcileSnapshotBackedLiveStates(Date.now());
    const positions = this.getPositionsRows();
    const status = this.getStatus();
    const compactLiveStates = Array.from(this.livePositionStates.values())
      .map((row) => {
        if (!row || typeof row !== "object") return null;
        return {
          wallet: normalizeWallet(row.wallet),
          state: String(row.state || "").trim().toLowerCase() || "unknown",
          stateChangedAt: Number(row.stateChangedAt || 0) || 0,
          accountPositionsCount: Number(row.accountPositionsCount || 0) || 0,
          materializedPositionsCount: Number(row.materializedPositionsCount || 0) || 0,
          pendingBootstrap: Boolean(row.pendingBootstrap),
          lastHintOpenAt: Number(row.lastHintOpenAt || 0) || 0,
          lastMaterializedOpenAt: Number(row.lastMaterializedOpenAt || 0) || 0,
          lastConfirmedClosedAt: Number(row.lastConfirmedClosedAt || 0) || 0,
          lastPositionsAt: Number(row.lastPositionsAt || 0) || 0,
          lastAccountAt: Number(row.lastAccountAt || 0) || 0,
          lastOpenedPositionSymbol: row.lastOpenedPositionSymbol || null,
          lastOpenedPositionSide: row.lastOpenedPositionSide || null,
          lastOpenedPositionDirection: row.lastOpenedPositionDirection || null,
          lastOpenedPositionSize: Number(row.lastOpenedPositionSize || 0) || 0,
          lastOpenedPositionUsd: Number(row.lastOpenedPositionUsd || 0) || 0,
          lastOpenedPositionEntry:
            Number.isFinite(toNum(row.lastOpenedPositionEntry, NaN))
              ? Number(toNum(row.lastOpenedPositionEntry, 0).toFixed(8))
              : null,
          lastOpenedAt: Number(row.lastOpenedAt || 0) || 0,
          lastObservedAt: Number(row.lastObservedAt || 0) || 0,
        };
      })
      .filter((row) => row && row.wallet);
    return {
      generatedAt: Date.now(),
      shardIndex: this.shardIndex,
      shardCount: this.shardCount,
      status,
      positions,
      accountOpenHints: Array.from(this.accountOpenHints.values()),
      liveStates: compactLiveStates,
      events: this.getRecentEvents(this.maxPersistedEvents),
      positionChangeEvents: this.getRecentPositionChangeEvents(this.maxPersistedEvents),
      positionOpenedEvents: this.getRecentPositionOpenedEvents(
        this.maxPersistedPositionOpenedEvents
      ),
    };
  }

  buildAccountPersistedPayload() {
    this.pruneToShardOwnership(false);
    return {
      generatedAt: Date.now(),
      shardIndex: this.shardIndex,
      shardCount: this.shardCount,
      status: this.getStatus(),
      accountOpenHints: Array.from(this.accountOpenHints.values()),
      liveStates: Array.from(this.livePositionStates.values()),
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

  persistAccountStateMaybe(force = false) {
    if (!this.accountPersistPath) return;
    const now = Date.now();
    if (!force && now - Number(this.lastAccountPersistAt || 0) < this.accountPersistEveryMs) return;
    writeShardSnapshot(this.accountPersistPath, this.buildAccountPersistedPayload());
    this.lastAccountPersistAt = now;
  }

  getSnapshot(options = {}) {
    const eventsLimit = Math.max(1, Math.min(2000, Number(options.eventsLimit || 200)));
    const positions = this.getPositionsRows();
    const status = this.getStatus();
    return {
      status,
      positions,
      liveStates: Array.from(this.livePositionStates.values()),
      events: this.getRecentEvents(eventsLimit),
      positionChangeEvents: this.getRecentPositionChangeEvents(eventsLimit),
      positionOpenedEvents: this.getRecentPositionOpenedEvents(eventsLimit),
    };
  }
}

module.exports = {
  WalletFirstLivePositionsMonitor,
};
