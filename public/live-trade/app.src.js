const LIVE_STALE_MS = 20000;
const LIVE_DELAYED_MS = 8000;
const RECONNECT_MAX_MS = 15000;
const SNAPSHOT_TIMEOUT_MS = 25000;

if (typeof window !== "undefined") {
  window.__PF_LIVE_TRADE_SCRIPT_EXECUTED = true;
  // Mark bundle as loaded immediately; runtime errors are surfaced separately.
  window.__PF_LIVE_TRADE_BOOTED = true;
}

const state = {
  payload: null,
  payloadSignature: "",
  activeView: "positions",
  rows: {
    positions: [],
    wallets: [],
  },
  rowSignatures: {
    positions: "",
    wallets: "",
  },
  search: "",
  filters: {
    wallet: "",
    symbol: "",
    side: "",
    minUsd: 0,
    maxUsd: 0,
    minPnl: 0,
    maxPnl: 0,
    freshness: "",
    trackedOnly: false,
  },
  pagination: {
    positions: {
      page: 1,
      pageSize: 100,
    },
    wallets: {
      page: 1,
      pageSize: 100,
    },
  },
  stream: {
    source: "bootstrap",
    connection: "idle",
    paused: false,
    retryMs: 1000,
    eventSource: null,
    fallbackTimer: null,
    healthTimer: null,
    lastPayloadAt: 0,
    lastEventAt: 0,
    lastErrorAt: 0,
    lastError: null,
  },
};

const VIEW_CONFIG = {
  positions: {
    kicker: "Live positions",
    title: "Positions",
    subtitle: "Live open positions across tracked wallets, refreshed in one focused trading surface.",
  },
  wallets: {
    kicker: "Wallet intelligence",
    title: "Wallet Performance",
    subtitle: "Tracked wallet quality, pnl, and activity in a calmer analytical view.",
  },
};

function el(id) {
  return document.getElementById(id);
}

function n(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function fmtNum(value, digits = 2) {
  const num = n(value, NaN);
  if (!Number.isFinite(num)) return "-";
  return num.toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function fmtInt(value) {
  return fmtNum(value, 0);
}

function fmtUsd(value, digits = 2) {
  const num = n(value, NaN);
  if (!Number.isFinite(num)) return "-";
  return `$${fmtNum(num, digits)}`;
}

function fmtSignedUsd(value, digits = 2) {
  const num = n(value, NaN);
  if (!Number.isFinite(num)) return "-";
  return `${num > 0 ? "+" : ""}$${fmtNum(num, digits)}`;
}

function fmtPct(value, digits = 2) {
  const num = n(value, NaN);
  if (!Number.isFinite(num)) return "-";
  return `${num > 0 ? "+" : ""}${fmtNum(num, digits)}%`;
}

function tsMs(value) {
  const maybe = n(value, NaN);
  if (Number.isFinite(maybe) && maybe > 0) return maybe;
  const parsed = Date.parse(String(value || ""));
  return Number.isFinite(parsed) ? parsed : 0;
}

function fmtTs(value) {
  const ms = tsMs(value);
  if (!ms) return "-";
  return new Date(ms).toLocaleString();
}

function fmtAgo(value) {
  const ms = tsMs(value);
  if (!ms) return "-";
  const delta = Math.max(0, Date.now() - ms);
  if (delta < 1000) return "now";
  const sec = Math.floor(delta / 1000);
  if (sec < 60) return `${sec}s ago`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m ago`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h ago`;
  const day = Math.floor(hr / 24);
  return `${day}d ago`;
}

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function asObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function extractRows(value) {
  if (Array.isArray(value)) return value.filter(Boolean);
  const obj = asObject(value);
  if (Array.isArray(obj.items)) return obj.items.filter(Boolean);
  if (Array.isArray(obj.rows)) return obj.rows.filter(Boolean);
  if (Array.isArray(obj.data)) return obj.data.filter(Boolean);
  if (Array.isArray(obj.list)) return obj.list.filter(Boolean);
  return [];
}

function extractPublicTrades(payload) {
  const primary = extractRows(payload?.publicTrades);
  if (primary.length > 0) return primary;
  const legacy = extractRows(payload?.rows);
  return legacy;
}

function extractWalletPerformance(payload) {
  const primary = extractRows(payload?.walletPerformance);
  if (primary.length > 0) return primary;
  const legacy = extractRows(payload?.wallets);
  return legacy;
}

function extractPositions(payload) {
  const primary = extractRows(payload?.positions);
  return primary;
}

function payloadPublicTradesTotal(payload) {
  const summaryTotal = n(payload?.summary?.publicTradesTotal, NaN);
  if (Number.isFinite(summaryTotal)) return summaryTotal;
  const legacyTotal = n(payload?.totalRows, NaN);
  if (Number.isFinite(legacyTotal)) return legacyTotal;
  return extractPublicTrades(payload).length;
}

function payloadWalletsTotal(payload) {
  const summaryTotal = n(payload?.summary?.indexedWalletsTotal, NaN);
  if (Number.isFinite(summaryTotal)) return summaryTotal;
  const legacyTotal = n(payload?.totalWallets, NaN);
  if (Number.isFinite(legacyTotal)) return legacyTotal;
  return extractWalletPerformance(payload).length;
}

function payloadPositionsTotal(payload) {
  const summaryTotal = n(payload?.summary?.openPositionsTotal, NaN);
  if (Number.isFinite(summaryTotal)) return summaryTotal;
  const legacyTotal = n(payload?.summary?.publicTradesTotal, NaN);
  if (Number.isFinite(legacyTotal)) return legacyTotal;
  return extractPositions(payload).length;
}

function positionsSignature(payload) {
  const positionRows = extractPositions(payload);
  const rows = positionRows.length > 0 ? positionRows : extractPublicTrades(payload);
  const head = asObject(rows[0]);
  return [
    payloadPositionsTotal(payload),
    rows.length,
    tsMs(head.timestamp || head.lastTradeAt || head.updatedAt || head.lastUpdatedAt || 0),
    String(head.historyId || head.id || head.accountId || head.symbol || ""),
  ].join(":");
}

function walletsSignature(payload) {
  const rows = extractWalletPerformance(payload);
  const sample = rows
    .slice(0, 16)
    .map((row) => {
      const item = asObject(row);
      const all = asObject(item.all || item.d24 || item.d30 || {});
      return [
        normalizeWallet(item.wallet || item.walletAddress || ""),
        tsMs(
          all.lastTrade ||
            all.last_trade ||
            item.lastTradeAt ||
            item.lastUpdatedAt ||
            item.updatedAt ||
            item.timestamp ||
            0
        ),
        n(item.totalTrades, 0),
        n(item.totalVolumeUsd ?? item.totalVolume ?? item.volumeUsd, 0),
      ].join("~");
    })
    .join("|");
  return [payloadWalletsTotal(payload), rows.length, sample].join(":");
}

function escapeHtml(value) {
  return String(value == null ? "" : value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function normalizeSide(side) {
  return String(side || "").trim().toLowerCase();
}

function normalizeWallet(value) {
  return String(value || "").trim();
}

function shortWallet(value, head = 6, tail = 6) {
  const wallet = normalizeWallet(value);
  if (!wallet) return "";
  if (wallet.length <= head + tail + 1) return wallet;
  return `${wallet.slice(0, head)}...${wallet.slice(-tail)}`;
}

function shortTx(value, head = 8, tail = 8) {
  const tx = String(value || "").trim();
  if (!tx) return "";
  if (tx.length <= head + tail + 1) return tx;
  return `${tx.slice(0, head)}...${tx.slice(-tail)}`;
}

function walletLooksResolved(value) {
  const wallet = normalizeWallet(value);
  if (!wallet) return false;
  if (wallet === "-") return false;
  const lower = wallet.toLowerCase();
  if (
    lower === "unknown" ||
    lower === "unattributed" ||
    lower === "(none)" ||
    lower === "none" ||
    lower === "null" ||
    lower === "n/a"
  ) {
    return false;
  }
  if (lower.startsWith("market:")) return false;
  return true;
}

function resolveWalletIdentity(rawWallet, options = {}) {
  const wallet = normalizeWallet(rawWallet);
  const tracked = Boolean(options.tracked);
  const allowUnresolved = Boolean(options.allowUnresolved);

  if (walletLooksResolved(wallet)) {
    return {
      wallet,
      label: shortWallet(wallet),
      title: wallet,
      resolved: true,
      kind: tracked ? "tracked" : "resolved",
    };
  }

  if (tracked) {
    return {
      wallet: "",
      label: "Tracked Account",
      title: "Tracked account (address unavailable in payload)",
      resolved: false,
      kind: "tracked_fallback",
    };
  }

  if (allowUnresolved) {
    return {
      wallet: "",
      label: "Unattributed Flow",
      title: "Wallet identity unavailable from source payload",
      resolved: false,
      kind: "unresolved",
    };
  }

  return {
    wallet: "",
    label: "-",
    title: "-",
    resolved: false,
    kind: "empty",
  };
}

function rowFreshnessByTs(timestampMs) {
  if (!timestampMs) return "unknown";
  const age = Date.now() - timestampMs;
  if (age <= 5 * 60 * 1000) return "fresh";
  if (age <= 30 * 60 * 1000) return "cooling";
  return "stale";
}

function getUniqueValues(rows, key) {
  return Array.from(
    new Set(
      rows
        .map((row) => String(row?.[key] || "").trim())
        .filter(Boolean)
        .sort((a, b) => a.localeCompare(b))
    )
  );
}

function buildRows(payload, options = {}) {
  const includePositions = options.includePositions !== false;
  const includeWallets = options.includeWallets !== false;
  const account = normalizeWallet(payload?.environment?.account || "");
  const publicTrades = extractPublicTrades(payload);
  const positionsPrimary = includePositions ? extractPositions(payload) : [];
  const positionsFallback = includePositions ? extractPositions(payload) : [];
  const walletPerformance = includeWallets ? extractWalletPerformance(payload) : [];

  const trackedWallets = includeWallets
    ? new Set(
        walletPerformance
          .map((row) => normalizeWallet(row?.wallet || row?.walletAddress))
          .filter((wallet) => walletLooksResolved(wallet))
      )
    : new Set(
        safeArray(state.rows?.wallets)
          .map((row) => normalizeWallet(row?.wallet))
          .filter((wallet) => walletLooksResolved(wallet))
      );

  const positionRows = includePositions
    ? (() => {
        const positionRowsFromPrimary = positionsPrimary
          .map((row, idx) => {
            const item = asObject(row);
            const timestamp = tsMs(
              item.timestamp ||
                item.updatedAt ||
                item.lastTradeAt ||
                item.lastUpdatedAt ||
                item.openedAt ||
                0
            );
            const rawWallet = normalizeWallet(
              item.wallet ||
                item.walletAddress ||
                item.owner ||
                item.trader ||
                item.account ||
                item.authority ||
                item.user ||
                ""
            );
            const tracked =
              Boolean(item.trackedWallet) ||
              (walletLooksResolved(rawWallet) ? trackedWallets.has(rawWallet) : false);
            const walletIdentity = resolveWalletIdentity(rawWallet, {
              tracked,
              allowUnresolved: true,
            });
            const side = normalizeSide(item.side || item.rawSide || "");
            const size = Math.abs(n(item.size, n(item.amount, n(item.qty, 0))));
            const entry = n(
              item.entry,
              n(item.entryPrice, n(item.entry_price, n(item.price, NaN)))
            );
            const mark = n(
              item.mark,
              n(item.markPrice, n(item.mark_price, n(item.price, entry)))
            );
            const positionUsd = n(
              item.positionUsd,
              n(item.notionalUsd, Number.isFinite(mark) ? Math.abs(size * mark) : Number.isFinite(entry) ? Math.abs(size * entry) : 0)
            );
            const baseFreshness = String(item.freshness || "").trim().toLowerCase();
            const freshness = walletIdentity.resolved
              ? baseFreshness || rowFreshnessByTs(timestamp)
              : "unresolved";
            const statusRaw = String(item.status || "").trim().toLowerCase();
            const status = walletIdentity.resolved ? statusRaw || freshness : "unresolved";
            const walletConfidence = String(
              item.walletConfidence || (walletIdentity.resolved ? "hard_payload" : "unresolved")
            )
              .trim()
              .toLowerCase();
            const txSignature = String(item.txSignature || item.signature || item.txid || "").trim();
            const tradeRefRaw =
              item.tradeRef || item.positionKey || item.historyId || item.orderId || item.i || item.li || null;
            const tradeRef = tradeRefRaw !== null && tradeRefRaw !== undefined ? String(tradeRefRaw) : "";
            const tradeRefType = String(
              item.tradeRefType ||
                (item.positionKey
                  ? "wallet_position"
                  : item.historyId
                  ? "history_id"
                  : item.orderId || item.i
                  ? "order_id"
                  : item.li
                  ? "li"
                  : "unknown")
            );
            const txConfidence = String(item.txConfidence || (txSignature ? "hard_payload" : "unresolved"))
              .trim()
              .toLowerCase();
            return {
              key: `p:pos:${item.positionKey || item.id || item.historyId || `${item.symbol || "na"}:${item.side || "na"}:${idx}`}`,
              timestamp,
              wallet: walletIdentity.wallet,
              walletLabel: walletIdentity.label,
              walletTitle: walletIdentity.title,
              walletResolved: walletIdentity.resolved,
              trackedWallet: tracked,
              symbol: String(item.symbol || "-").toUpperCase(),
              side,
              size,
              positionUsd,
              entry,
              mark,
              pnl: n(item.pnl, n(item.unrealizedPnlUsd, n(item.unrealizedPnl, NaN))),
              status,
              freshness,
              walletSource: String(item.walletSource || "").trim().toLowerCase() || "wallet_positions_api",
              walletConfidence,
              txSignature,
              txSource: String(item.txSource || (txSignature ? "payload" : "wallet_positions_api"))
                .trim()
                .toLowerCase(),
              txConfidence,
              tradeRef,
              tradeRefType,
              openedAt: tsMs(item.openedAt || item.createdAt || item.openTime || timestamp),
              updatedAt: timestamp,
              source: String(item.source || "wallet_first_positions"),
              raw: item,
            };
          })
          .sort((a, b) => b.updatedAt - a.updatedAt);

        if (positionRowsFromPrimary.length > 0) return positionRowsFromPrimary;

        const positionRowsFromPublic = publicTrades
          .map((row, idx) => {
            const item = asObject(row);
            const timestamp = tsMs(
              item.timestamp ||
                item.updatedAt ||
                item.lastTradeAt ||
                item.lastUpdatedAt ||
                item.openedAt ||
                0
            );
            const rawWallet = normalizeWallet(
              item.wallet ||
                item.walletAddress ||
                item.owner ||
                item.trader ||
                item.account ||
                item.authority ||
                item.user ||
                ""
            );
            const tracked = walletLooksResolved(rawWallet) ? trackedWallets.has(rawWallet) : false;
            const walletIdentity = resolveWalletIdentity(rawWallet, {
              tracked,
              allowUnresolved: true,
            });
            const side = normalizeSide(item.side || "");
            const size = Math.abs(n(item.amount, n(item.size, n(item.qty, 0))));
            const entry = n(item.entry, n(item.price, n(item.entryPrice, NaN)));
            const mark = n(item.mark, n(item.price, n(item.currentPrice, NaN)));
            const positionUsd = n(
              item.notionalUsd,
              n(item.openNotional, Number.isFinite(entry) ? size * entry : 0)
            );
            const freshness = walletIdentity.resolved ? rowFreshnessByTs(timestamp) : "unresolved";
            const cause = String(item.cause || "").trim().toLowerCase();
            const status = walletIdentity.resolved ? cause || freshness : "unresolved";
            const walletConfidence = String(
              item.walletConfidence || (walletIdentity.resolved ? "hard_payload" : "unresolved")
            )
              .trim()
              .toLowerCase();
            const txSignature = String(item.txSignature || item.signature || item.txid || "").trim();
            const tradeRefRaw =
              item.tradeRef || item.historyId || item.orderId || item.i || item.li || null;
            const tradeRef = tradeRefRaw !== null && tradeRefRaw !== undefined ? String(tradeRefRaw) : "";
            const tradeRefType = String(item.tradeRefType || (item.historyId ? "history_id" : item.orderId || item.i ? "order_id" : item.li ? "li" : "unknown"));
            const txConfidence = String(item.txConfidence || (txSignature ? "hard_payload" : "unresolved"))
              .trim()
              .toLowerCase();
            return {
              key: `p:pub:${item.historyId || item.id || item.accountId || idx}`,
              timestamp,
              wallet: walletIdentity.wallet,
              walletLabel: walletIdentity.label,
              walletTitle: walletIdentity.title,
              walletResolved: walletIdentity.resolved,
              trackedWallet: tracked,
              symbol: String(item.symbol || "-").toUpperCase(),
              side,
              size,
              positionUsd,
              entry,
              mark,
              pnl: n(item.pnl, n(item.unrealizedPnlUsd, n(item.unrealizedPnl, NaN))),
              status,
              freshness,
              walletSource: String(item.walletSource || "").trim().toLowerCase(),
              walletConfidence,
              txSignature,
              txSource: String(item.txSource || (txSignature ? "payload" : "unresolved")).trim().toLowerCase(),
              txConfidence,
              tradeRef,
              tradeRefType,
              openedAt: timestamp,
              updatedAt: timestamp,
              source: "exchange_public_trade",
              raw: item,
            };
          })
          .sort((a, b) => b.updatedAt - a.updatedAt);

        const positionRowsFromFallback = positionsFallback
          .map((row, idx) => {
            const item = asObject(row);
            const timestamp = tsMs(item.updatedAt || item.timestamp || item.openedAt || 0);
            const rawWallet = normalizeWallet(item.wallet || item.account || account || "");
            const tracked = walletLooksResolved(rawWallet) ? trackedWallets.has(rawWallet) : true;
            const walletIdentity = resolveWalletIdentity(rawWallet, {
              tracked,
              allowUnresolved: true,
            });
            const freshness = walletIdentity.resolved ? rowFreshnessByTs(timestamp) : "unresolved";
            const walletConfidence = walletIdentity.resolved ? "hard_payload" : "unresolved";
            return {
              key: `p:fb:${item.symbol || "na"}:${item.side || "na"}:${idx}`,
              timestamp,
              wallet: walletIdentity.wallet,
              walletLabel: walletIdentity.label,
              walletTitle: walletIdentity.title,
              walletResolved: walletIdentity.resolved,
              trackedWallet: tracked,
              symbol: String(item.symbol || "-").toUpperCase(),
              side: normalizeSide(item.side || ""),
              size: n(item.amountAbs, n(item.size, 0)),
              positionUsd: n(item.notionalUsd, 0),
              entry: n(item.entryPrice, NaN),
              mark: n(item.markPrice, NaN),
              pnl: n(item.unrealizedPnlUsd, NaN),
              status: walletIdentity.resolved ? (item.riskTag || freshness) : "unresolved",
              freshness,
              walletSource: "account_positions_fallback",
              walletConfidence,
              txSignature: "",
              txSource: "unresolved",
              txConfidence: "unresolved",
              tradeRef: "",
              tradeRefType: "unknown",
              openedAt: tsMs(item.openedAt || item.openTime || 0),
              updatedAt: timestamp,
              source: "account_positions_fallback",
              raw: item,
            };
          })
          .sort((a, b) => b.updatedAt - a.updatedAt);

        return positionRowsFromPublic.length > 0 ? positionRowsFromPublic : positionRowsFromFallback;
      })()
    : null;

  const walletRows = includeWallets
    ? walletPerformance
        .map((row, idx) => {
          const item = asObject(row);
          const all = asObject(item.all || item.d24 || item.d30 || {});
          const walletIdentity = resolveWalletIdentity(item.wallet || item.walletAddress || "", {
            tracked: true,
            allowUnresolved: true,
          });
          const lastActivity = tsMs(
            all.lastTrade ||
              all.last_trade ||
              item.lastTradeAt ||
              item.lastUpdatedAt ||
              item.updatedAt ||
              item.timestamp ||
              0
          );
          const freshness = walletIdentity.resolved ? rowFreshnessByTs(lastActivity) : "unresolved";
          const realizedPnlUsd = n(
            all.realizedPnlUsd ??
              all.realized_pnl_usd ??
              all.pnlUsd ??
              item.realizedPnlUsd ??
              item.pnlUsd ??
              item.pnlAllTime,
            NaN
          );
          const unrealizedPnlUsd = n(
            all.unrealizedPnlUsd ?? all.unrealized_pnl_usd ?? item.unrealizedPnlUsd ?? item.unrealizedPnl,
            NaN
          );
          const totalVolumeUsd = n(all.volumeUsd ?? item.volumeUsd ?? item.totalVolume, 0);
          const totalTrades = n(all.trades ?? item.totalTrades, 0);
          const feesPaidUsd = n(all.feesPaidUsd ?? all.fees_paid_usd ?? item.feesPaidUsd, NaN);
          const rawWinRate = n(
            all.winRatePct ?? item.winRatePct ?? item.winRate ?? item.winRate30d,
            NaN
          );
          const winRate =
            Number.isFinite(rawWinRate) && rawWinRate >= 0 && rawWinRate <= 1
              ? rawWinRate * 100
              : rawWinRate;
          return {
            key: `w:${item.wallet || "na"}:${idx}`,
            wallet: walletIdentity.wallet || normalizeWallet(item.walletAddress || ""),
            walletLabel: walletIdentity.label,
            walletTitle: walletIdentity.title,
            walletResolved: walletIdentity.resolved,
            trackedWallet: true,
            totalTrades: walletIdentity.resolved ? totalTrades : NaN,
            totalVolumeUsd: walletIdentity.resolved ? totalVolumeUsd : NaN,
            realizedPnlUsd: walletIdentity.resolved ? realizedPnlUsd : NaN,
            unrealizedPnlUsd: walletIdentity.resolved ? unrealizedPnlUsd : NaN,
            totalPnlUsd:
              (walletIdentity.resolved && Number.isFinite(realizedPnlUsd) ? realizedPnlUsd : 0) +
              (walletIdentity.resolved && Number.isFinite(unrealizedPnlUsd) ? unrealizedPnlUsd : 0),
            winRate: walletIdentity.resolved ? winRate : NaN,
            feesPaidUsd: walletIdentity.resolved ? feesPaidUsd : NaN,
            lastActivity,
            freshness,
            status: freshness,
            raw: item,
          };
        })
        .sort((a, b) => n(b.totalPnlUsd, 0) - n(a.totalPnlUsd, 0))
    : null;

  return { positions: positionRows, wallets: walletRows };
}

function refreshFilterOptions(rows) {
  const symbolSelect = el("lt-filter-symbol");
  const sideSelect = el("lt-filter-side");
  const freshnessSelect = el("lt-filter-freshness");
  if (!symbolSelect || !sideSelect || !freshnessSelect) return;

  const currentSymbol = symbolSelect.value;
  const currentSide = sideSelect.value;
  const currentFreshness = freshnessSelect.value;

  const symbols = getUniqueValues(rows.positions, "symbol");
  const sides = getUniqueValues(rows.positions, "side");
  const freshnessStates = Array.from(
    new Set([...getUniqueValues(rows.positions, "freshness"), ...getUniqueValues(rows.wallets, "freshness")])
  ).sort((a, b) => a.localeCompare(b));

  symbolSelect.innerHTML = ["<option value=\"\">All</option>"]
    .concat(symbols.map((value) => `<option value=\"${escapeHtml(value)}\">${escapeHtml(value)}</option>`))
    .join("");

  sideSelect.innerHTML = ["<option value=\"\">All</option>"]
    .concat(sides.map((value) => `<option value=\"${escapeHtml(value)}\">${escapeHtml(value)}</option>`))
    .join("");

  freshnessSelect.innerHTML = ["<option value=\"\">All</option>"]
    .concat(freshnessStates.map((value) => `<option value=\"${escapeHtml(value)}\">${escapeHtml(value)}</option>`))
    .join("");

  if (symbols.includes(currentSymbol)) symbolSelect.value = currentSymbol;
  if (sides.includes(currentSide)) sideSelect.value = currentSide;
  if (freshnessStates.includes(currentFreshness)) freshnessSelect.value = currentFreshness;
}

function matchesSearch(row, query) {
  if (!query) return true;
  const haystack = [
    row.wallet,
    row.walletLabel,
    row.walletTitle,
    row.symbol,
    row.side,
    row.status,
    row.totalTrades,
    row.totalVolumeUsd,
    row.realizedPnlUsd,
    row.unrealizedPnlUsd,
    row.positionUsd,
    row.tradeRef,
    row.tradeRefType,
    row.txSignature,
    row.txSource,
  ]
    .join(" ")
    .toLowerCase();
  return haystack.includes(query);
}

function withinRange(value, minValue, maxValue) {
  const min = n(minValue, 0);
  const max = n(maxValue, 0);
  const numeric = n(value, NaN);
  if (!Number.isFinite(numeric)) return min === 0 && max === 0;
  if (min !== 0 && numeric < min) return false;
  if (max !== 0 && numeric > max) return false;
  return true;
}

function resetPagination() {
  state.pagination.positions.page = 1;
  state.pagination.wallets.page = 1;
}

function currentWindowParams() {
  const positions = state.pagination.positions || { page: 1, pageSize: 100 };
  const wallets = state.pagination.wallets || { page: 1, pageSize: 100 };
  const positionsPage = Math.max(1, Math.floor(n(positions.page, 1)));
  const walletsPage = Math.max(1, Math.floor(n(wallets.page, 1)));
  const positionsPageSize = Math.max(10, Math.min(5000, Math.floor(n(positions.pageSize, 100))));
  const walletsPageSize = Math.max(10, Math.min(5000, Math.floor(n(wallets.pageSize, 100))));

  return {
    public_offset: (positionsPage - 1) * positionsPageSize,
    public_limit: positionsPageSize,
    position_offset: (positionsPage - 1) * positionsPageSize,
    position_limit: positionsPageSize,
    wallet_offset: (walletsPage - 1) * walletsPageSize,
    wallet_limit: walletsPageSize,
  };
}

function buildLiveTradesPath(basePath) {
  const params = new URLSearchParams();
  const window = currentWindowParams();
  for (const [key, value] of Object.entries(window)) {
    params.set(key, String(value));
  }
  // Wallet-first live UI does not require full exchange publicTrades hydration on each poll.
  params.set("skip_public", "1");
  return `${basePath}?${params.toString()}`;
}

function pageWindowFromSummary(key, rowsLength) {
  const config = state.pagination[key] || { page: 1, pageSize: 100 };
  const pageSize = Math.max(10, Math.min(5000, Math.floor(n(config.pageSize, 100))));
  const payload = state.payload || {};
  const summary = payload.summary || {};
  const totalRows =
    key === "positions"
      ? Number.isFinite(n(summary.openPositionsTotal, NaN))
        ? n(summary.openPositionsTotal, rowsLength)
        : Number.isFinite(n(summary.publicTradesTotal, NaN))
        ? n(summary.publicTradesTotal, rowsLength)
        : rowsLength
      : Number.isFinite(n(summary.indexedWalletsTotal, NaN))
      ? n(summary.indexedWalletsTotal, rowsLength)
      : rowsLength;
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize));
  const page = Math.min(Math.max(1, Math.floor(n(config.page, 1))), totalPages);
  config.page = page;
  config.pageSize = pageSize;
  state.pagination[key] = config;

  return {
    totalRows,
    totalPages,
    page,
    pageSize,
    start: totalRows > 0 ? (page - 1) * pageSize + 1 : 0,
    end: totalRows > 0 ? Math.min(page * pageSize, totalRows) : 0,
  };
}

function paginateRows(rows, key) {
  const list = Array.isArray(rows) ? rows : [];
  const config = state.pagination[key] || { page: 1, pageSize: 100 };
  const pageSize = Math.max(10, Math.min(5000, Math.floor(n(config.pageSize, 100))));
  const totalRows = list.length;
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize));
  const page = Math.min(Math.max(1, Math.floor(n(config.page, 1))), totalPages);
  const startIndex = totalRows > 0 ? (page - 1) * pageSize : 0;
  const endIndex = Math.min(startIndex + pageSize, totalRows);
  const pageRows = totalRows > 0 ? list.slice(startIndex, endIndex) : [];

  config.pageSize = pageSize;
  config.page = page;
  state.pagination[key] = config;

  return {
    rows: pageRows,
    totalRows,
    totalPages,
    page,
    pageSize,
    start: totalRows > 0 ? startIndex + 1 : 0,
    end: endIndex,
  };
}

function renderPagination(key, pageData) {
  const info = el(`lt-${key}-page-info`);
  const windowNode = el(`lt-${key}-page-window`);
  const prev = el(`lt-${key}-page-prev`);
  const next = el(`lt-${key}-page-next`);
  if (!info || !windowNode || !prev || !next) return;

  info.textContent = `Page ${fmtInt(pageData.page)} / ${fmtInt(pageData.totalPages)}`;
  windowNode.textContent = `${fmtInt(pageData.start)}-${fmtInt(pageData.end)} of ${fmtInt(pageData.totalRows)}`;
  prev.disabled = pageData.page <= 1;
  next.disabled = pageData.page >= pageData.totalPages;
}

function applyPositionFilters(rows) {
  const q = state.search.trim().toLowerCase();
  const f = state.filters;
  const walletNeedle = String(f.wallet || "").trim().toLowerCase();
  return rows.filter((row) => {
    if (!matchesSearch(row, q)) return false;
    if (walletNeedle) {
      const walletFields = [row.wallet, row.walletLabel, row.walletTitle].join(" ").toLowerCase();
      if (!walletFields.includes(walletNeedle)) return false;
    }
    if (f.symbol && row.symbol !== f.symbol) return false;
    if (f.side && row.side !== f.side) return false;
    if (f.freshness && String(row.freshness || row.status || "") !== f.freshness) return false;
    if (!withinRange(row.positionUsd, f.minUsd, f.maxUsd)) return false;
    if (!withinRange(row.pnl, f.minPnl, f.maxPnl)) return false;
    if (f.trackedOnly && !row.trackedWallet) return false;
    return true;
  });
}

function applyWalletFilters(rows) {
  const q = state.search.trim().toLowerCase();
  const f = state.filters;
  const walletNeedle = String(f.wallet || "").trim().toLowerCase();
  return rows.filter((row) => {
    if (!matchesSearch(row, q)) return false;
    if (walletNeedle) {
      const walletFields = [row.wallet, row.walletLabel, row.walletTitle].join(" ").toLowerCase();
      if (!walletFields.includes(walletNeedle)) return false;
    }
    if (f.freshness && String(row.freshness || row.status || "") !== f.freshness) return false;
    if (!withinRange(row.totalVolumeUsd, f.minUsd, f.maxUsd)) return false;
    if (!withinRange(row.totalPnlUsd, f.minPnl, f.maxPnl)) return false;
    if (f.trackedOnly && !row.trackedWallet) return false;
    return true;
  });
}

function badge(type, value) {
  return `<span class="lt-badge ${escapeHtml(type)}">${escapeHtml(String(value || "-"))}</span>`;
}

function renderWalletCell(row) {
  const walletText = row.walletLabel || shortWallet(row.wallet) || "Unattributed Flow";
  const statusClass = row.walletResolved
    ? row.freshness === "fresh"
      ? "fresh"
      : row.freshness === "stale"
      ? "stale"
      : "cool"
    : "warn";
  const statusLabel = row.walletResolved ? row.freshness || "unknown" : "unresolved";
  const tracked = row.trackedWallet ? badge("tracked", "tracked") : "";
  const walletFilterValue = row.walletResolved ? row.wallet : "";
  const walletButtonClass = row.walletResolved ? "lt-wallet-link" : "lt-wallet-link lt-wallet-link-disabled";
  return `<div class="lt-wallet-cell">
    <button class="${walletButtonClass}" data-filter-wallet="${escapeHtml(walletFilterValue)}" title="${escapeHtml(
      row.walletTitle || row.wallet || walletText
    )}" ${row.walletResolved ? "" : "disabled"}>${escapeHtml(walletText)}</button>
    ${tracked}
    ${badge(statusClass, statusLabel)}
  </div>`;
}

function renderPositionsTable(rows) {
  const head = el("lt-positions-head");
  const body = el("lt-positions-body");
  if (!head || !body) return;

  const columns = [
    { key: "time", label: "Time", align: "left" },
    { key: "wallet", label: "Wallet", align: "left" },
    { key: "symbol", label: "Symbol", align: "left" },
    { key: "side", label: "Side", align: "left" },
    { key: "usd", label: "Position", align: "right" },
    { key: "entryLast", label: "Entry / Last", align: "right" },
    { key: "pnl", label: "PnL", align: "right" },
    { key: "status", label: "Status", align: "left" },
    { key: "updated", label: "Updated", align: "left" },
  ];

  head.innerHTML = `<tr>${columns
    .map(
      (col) =>
        `<th class="${col.align === "right" ? "right" : "left"}" data-col="${escapeHtml(col.key)}">${escapeHtml(
          col.label
        )}</th>`
    )
    .join("")}</tr>`;

  body.innerHTML = rows
    .map((row) => {
      const side = normalizeSide(row.side);
      const sideClass = side.includes("long") || side === "buy" ? "up" : side.includes("short") || side === "sell" ? "down" : "flat";
      const statusClass = row.status === "unresolved" ? "warn" : row.freshness === "fresh" ? "fresh" : row.freshness === "stale" ? "stale" : "cool";
      const sizeText = Number.isFinite(n(row.size, NaN)) ? fmtNum(row.size, 4) : "-";
      const usdText = Number.isFinite(n(row.positionUsd, NaN)) ? fmtUsd(row.positionUsd, 2) : "Partial";
      const entryText = Number.isFinite(n(row.entry, NaN)) ? fmtNum(row.entry, 6) : "Partial";
      const markText = Number.isFinite(n(row.mark, NaN)) ? fmtNum(row.mark, 6) : "Partial";
      const pnlText = Number.isFinite(n(row.pnl, NaN)) ? fmtSignedUsd(row.pnl, 2) : "Partial";
      const secondaryStatus = !row.walletResolved
        ? badge("warn", "unresolved")
        : row.trackedWallet
        ? badge("tracked", "tracked")
        : "";
      return `<tr>
        <td class="left mono" data-col="time">${escapeHtml(fmtTs(row.timestamp))}</td>
        <td class="left" data-col="wallet">${renderWalletCell(row)}</td>
        <td class="left" data-col="symbol"><strong>${escapeHtml(row.symbol || "-")}</strong></td>
        <td class="left" data-col="side">${badge(sideClass, row.side || "-")}</td>
        <td class="right mono" data-col="usd">
          <div class="lt-cell-stack">
            <span>${escapeHtml(usdText)}</span>
            <small>size ${escapeHtml(sizeText)}</small>
          </div>
        </td>
        <td class="right mono" data-col="entryLast">${escapeHtml(entryText)} / ${escapeHtml(markText)}</td>
        <td class="right mono ${n(row.pnl, 0) >= 0 ? "up" : "down"}" data-col="pnl">${escapeHtml(pnlText)}</td>
        <td class="left" data-col="status">${badge(statusClass, row.status || "-")} ${secondaryStatus}</td>
        <td class="left mono" data-col="updated">${escapeHtml(fmtAgo(row.updatedAt || row.timestamp))}</td>
      </tr>`;
    })
    .join("");

  body.querySelectorAll("button.lt-wallet-link[data-filter-wallet]").forEach((btn) => {
    btn.addEventListener("click", (event) => {
      event.stopPropagation();
      const walletValue = String(btn.getAttribute("data-filter-wallet") || "").trim();
      if (!walletValue) return;
      const walletInput = el("lt-filter-wallet");
      if (walletInput) walletInput.value = walletValue;
      state.filters.wallet = walletValue;
      resetPagination();
      render();
    });
  });
}

function renderWalletsTable(rows) {
  const head = el("lt-wallets-head");
  const body = el("lt-wallets-body");
  if (!head || !body) return;

  const columns = [
    { key: "wallet", label: "Wallet", align: "left" },
    { key: "trades", label: "Total Trades", align: "right" },
    { key: "volume", label: "Total Volume", align: "right" },
    { key: "realized", label: "Realized PnL", align: "right" },
    { key: "unrealized", label: "Unrealized PnL", align: "right" },
    { key: "winrate", label: "Win Rate", align: "right" },
    { key: "activity", label: "Last Activity", align: "left" },
    { key: "fresh", label: "Freshness", align: "left" },
  ];

  head.innerHTML = `<tr>${columns
    .map(
      (col) =>
        `<th class="${col.align === "right" ? "right" : "left"}" data-col="${escapeHtml(col.key)}">${escapeHtml(
          col.label
        )}</th>`
    )
    .join("")}</tr>`;

  body.innerHTML = rows
    .map((row) => {
      const freshClass = row.freshness === "fresh" ? "fresh" : row.freshness === "stale" ? "stale" : row.freshness === "unresolved" ? "warn" : "cool";
      const trades = Number.isFinite(n(row.totalTrades, NaN)) ? fmtInt(row.totalTrades) : "Partial";
      const volume = Number.isFinite(n(row.totalVolumeUsd, NaN)) ? fmtUsd(row.totalVolumeUsd, 2) : "Partial";
      const realized = Number.isFinite(n(row.realizedPnlUsd, NaN)) ? fmtSignedUsd(row.realizedPnlUsd, 2) : "Partial";
      const unrealized = Number.isFinite(n(row.unrealizedPnlUsd, NaN)) ? fmtSignedUsd(row.unrealizedPnlUsd, 2) : "Partial";
      const winRate = Number.isFinite(n(row.winRate, NaN)) ? fmtPct(row.winRate, 2) : "Partial";
      return `<tr>
        <td class="left" data-col="wallet">${renderWalletCell(row)}</td>
        <td class="right mono" data-col="trades">${escapeHtml(trades)}</td>
        <td class="right mono" data-col="volume">${escapeHtml(volume)}</td>
        <td class="right mono ${n(row.realizedPnlUsd, 0) >= 0 ? "up" : "down"}" data-col="realized">${escapeHtml(realized)}</td>
        <td class="right mono ${n(row.unrealizedPnlUsd, 0) >= 0 ? "up" : "down"}" data-col="unrealized">${escapeHtml(unrealized)}</td>
        <td class="right mono" data-col="winrate">${escapeHtml(winRate)}</td>
        <td class="left mono" data-col="activity">${escapeHtml(fmtTs(row.lastActivity))}</td>
        <td class="left" data-col="fresh">${badge(freshClass, row.freshness || "unknown")}</td>
      </tr>`;
    })
    .join("");

  body.querySelectorAll("button.lt-wallet-link[data-filter-wallet]").forEach((btn) => {
    btn.addEventListener("click", (event) => {
      event.stopPropagation();
      const walletValue = String(btn.getAttribute("data-filter-wallet") || "").trim();
      if (!walletValue) return;
      const walletInput = el("lt-filter-wallet");
      if (walletInput) walletInput.value = walletValue;
      state.filters.wallet = walletValue;
      resetPagination();
      render();
    });
  });
}

function renderFilterMeta(positionsCount, walletsCount) {
  const summary = [];
  const f = state.filters;
  if (f.wallet) summary.push(`wallet=${f.wallet}`);
  if (f.symbol) summary.push(`symbol=${f.symbol}`);
  if (f.side) summary.push(`side=${f.side}`);
  if (n(f.minUsd, 0) > 0 || n(f.maxUsd, 0) > 0) summary.push(`position_usd=${n(f.minUsd, 0)}..${n(f.maxUsd, 0) || "max"}`);
  if (n(f.minPnl, 0) !== 0 || n(f.maxPnl, 0) !== 0) summary.push(`pnl=${f.minPnl || 0}..${f.maxPnl || "max"}`);
  if (f.freshness) summary.push(`freshness=${f.freshness}`);
  if (f.trackedOnly) summary.push("tracked_only");

  const filterSummary = el("lt-filter-summary");
  if (filterSummary) {
    filterSummary.textContent = `Filters: ${summary.length ? summary.join(" • ") : "none"}`;
  }

  const walletSummary = el("lt-wallet-summary");
  if (walletSummary) {
    const summary = state.payload?.summary || {};
    const posTotal = payloadPositionsTotal(state.payload);
    const walletTotal = payloadWalletsTotal(state.payload);
    const posLabel = `${fmtInt(positionsCount)} shown • ${fmtInt(posTotal)} tracked live`;
    const walletLabel = `${fmtInt(walletsCount)} shown • ${fmtInt(walletTotal)} tracked wallets`;
    walletSummary.textContent = `${posLabel} • ${walletLabel}`;
  }
}

function renderActiveView(positionsCount, walletsCount) {
  const activeView = state.activeView === "wallets" ? "wallets" : "positions";
  const inactiveView = activeView === "positions" ? "wallets" : "positions";
  const activePanel = el(`lt-view-${activeView}`);
  const inactivePanel = el(`lt-view-${inactiveView}`);
  const activeButton = el(`lt-view-${activeView}-btn`);
  const inactiveButton = el(`lt-view-${inactiveView}-btn`);
  const config = VIEW_CONFIG[activeView] || VIEW_CONFIG.positions;
  const activeCount = activeView === "positions" ? positionsCount : walletsCount;

  if (activePanel) {
    activePanel.hidden = false;
    activePanel.classList.add("is-active");
  }
  if (inactivePanel) {
    inactivePanel.hidden = true;
    inactivePanel.classList.remove("is-active");
  }

  if (activeButton) {
    activeButton.classList.add("active");
    activeButton.setAttribute("aria-selected", "true");
  }
  if (inactiveButton) {
    inactiveButton.classList.remove("active");
    inactiveButton.setAttribute("aria-selected", "false");
  }

  const kicker = el("lt-stage-kicker");
  if (kicker) kicker.textContent = config.kicker;
  const title = el("lt-stage-title");
  if (title) title.textContent = config.title;
  const subtitle = el("lt-stage-subtitle");
  if (subtitle) subtitle.textContent = config.subtitle;
  const activeCountNode = el("lt-active-view-count");
  if (activeCountNode) {
    activeCountNode.textContent = `${fmtInt(activeCount)} ${activeView === "positions" ? "rows" : "wallets"}`;
  }
}

function connectionState() {
  if (state.stream.paused) return { label: "PAUSED", className: "paused" };

  const age = state.stream.lastPayloadAt ? Date.now() - state.stream.lastPayloadAt : Infinity;
  if (state.stream.connection === "open" && age <= LIVE_DELAYED_MS) return { label: "LIVE", className: "live" };
  if (state.stream.connection === "open" && age <= LIVE_STALE_MS) return { label: "DELAYED", className: "delayed" };
  if (state.stream.connection === "reconnecting") return { label: "RECONNECTING", className: "warn" };
  if (state.stream.connection === "fallback") return { label: "DEGRADED", className: "warn" };
  if (age > LIVE_STALE_MS) return { label: "STALE", className: "bad" };
  return { label: "IDLE", className: "idle" };
}

function renderHealth(payload) {
  const status = connectionState();
  const liveEl = el("lt-live-state");
  if (liveEl) {
    liveEl.textContent = status.label;
    liveEl.className = `lt-status-value lt-status-value-live ${status.className}`;
  }

  const generatedAt = tsMs(payload?.generatedAt);
  const syncUpdatedAt = tsMs(payload?.sync?.updatedAt);
  const marketLastEventAt = tsMs(payload?.marketContext?.lastEventAt);
  const eventAt = Math.max(generatedAt, syncUpdatedAt, marketLastEventAt, state.stream.lastEventAt || 0);
  const freshness = state.stream.lastPayloadAt ? Date.now() - state.stream.lastPayloadAt : null;
  const walletFirst = asObject(payload?.summary?.walletFirstLive);
  const openPositionsTotal = payloadPositionsTotal(payload);
  const indexedWalletsTotal = payloadWalletsTotal(payload);

  const scope = String(payload?.summary?.streamScope || "exchange_wide_public_trades").replace(/_/g, " ");
  const windowed =
    payload?.summary?.openPositionsWindowed || payload?.summary?.publicTradesWindowed
      ? " • paged query"
      : "";
  const retention = Number(payload?.summary?.retentionPublicTradesPerSymbol || 0);
  const retentionLabel = Number.isFinite(retention) && retention > 0 ? ` • retention ${fmtInt(retention)}/symbol` : "";

  const lastEventNode = el("lt-last-event");
  if (lastEventNode) lastEventNode.textContent = eventAt ? fmtTs(eventAt) : "-";

  const freshNode = el("lt-freshness");
  if (freshNode) freshNode.textContent = freshness === null ? "-" : `${fmtInt(freshness)} ms`;

  const streamNode = el("lt-stream-health");
  if (streamNode) {
    streamNode.textContent = `${state.stream.source} • ${state.stream.connection} • ${scope}${windowed}${retentionLabel}`;
  }

  const connNode = el("lt-conn");
  if (connNode) {
    const wsStatus =
      payload?.sync?.wsStatus ||
      payload?.summary?.wsStatus ||
      payload?.environment?.wsStatus ||
      (state.stream.connection === "open" ? "open" : state.stream.connection || "unknown");
    connNode.textContent = `${wsStatus || "unknown"} • ${fmtInt(n(payload?.marketContext?.eventsPerMin, 0))} ev/min`;
  }

  const latencyNode = el("lt-latency");
  if (latencyNode) {
    const lastPassMs = n(walletFirst.lastPassDurationMs, NaN);
    const hotLoopSec = n(walletFirst.estimatedHotLoopSeconds, NaN);
    if (Number.isFinite(lastPassMs) && Number.isFinite(hotLoopSec)) {
      latencyNode.textContent = `Loop latency ${fmtInt(lastPassMs)} ms • hot ${fmtInt(hotLoopSec)}s`;
    } else if (Number.isFinite(lastPassMs)) {
      latencyNode.textContent = `Loop latency ${fmtInt(lastPassMs)} ms`;
    } else {
      latencyNode.textContent = "Loop latency -";
    }
  }

  const coverageNode = el("lt-coverage");
  if (coverageNode) {
    coverageNode.textContent = `${fmtInt(openPositionsTotal)} positions • ${fmtInt(indexedWalletsTotal)} wallets`;
  }

  const attributionNode = el("lt-attribution");
  if (attributionNode) {
    const attribution = asObject(payload?.summary?.walletAttribution);
    const tiers = asObject(attribution.tiers);
    const hardPayload = Math.max(0, Math.floor(n(tiers.hard_payload, 0)));
    const hardHistory = Math.max(0, Math.floor(n(tiers.hard_history_id, 0)));
    const hardOrder = Math.max(0, Math.floor(n(tiers.hard_order_id, 0)));
    const fallbackLi = Math.max(0, Math.floor(n(tiers.fallback_li, 0)));
    const unresolved = Math.max(0, Math.floor(n(tiers.unresolved, 0)));
    const coverageRaw = n(attribution.coveragePct, NaN);
    const coverageSummaryRaw = n(payload?.summary?.publicTradesWalletCoveragePct, NaN);
    const coverage = Number.isFinite(coverageRaw)
      ? coverageRaw
      : Number.isFinite(coverageSummaryRaw)
      ? coverageSummaryRaw
      : NaN;
    const hasBreakdown =
      hardPayload > 0 || hardHistory > 0 || hardOrder > 0 || fallbackLi > 0 || unresolved > 0;
    if (!hasBreakdown && !Number.isFinite(coverage)) {
      attributionNode.textContent = "Attribution unavailable";
    } else {
      const coverageText = Number.isFinite(coverage) ? `${fmtNum(coverage, 2)}%` : "-";
      const txAttribution = asObject(payload?.summary?.txAttribution);
      const txCoverageRaw = n(txAttribution.coveragePct, NaN);
      const txCoverageSummaryRaw = n(payload?.summary?.publicTradesTxCoveragePct, NaN);
      const txCoverage = Number.isFinite(txCoverageRaw)
        ? txCoverageRaw
        : Number.isFinite(txCoverageSummaryRaw)
        ? txCoverageSummaryRaw
        : NaN;
      const txCoverageText = Number.isFinite(txCoverage) ? `${fmtNum(txCoverage, 2)}%` : "-";
      attributionNode.textContent = `Wallet ${coverageText} • tx ${txCoverageText} • hard ${fmtInt(
        hardHistory + hardOrder + hardPayload
      )} • li ${fmtInt(fallbackLi)} • unresolved ${fmtInt(unresolved)}`;
    }
  }

  const attributionGrowthNode = el("lt-attribution-growth");
  if (attributionGrowthNode) {
    const db = asObject(payload?.summary?.attributionDb);
    const growth = asObject(db.growth);
    const wallet1h = Math.max(0, Math.floor(n(growth.walletUpserts1h, 0)));
    const wallet24h = Math.max(0, Math.floor(n(growth.walletUpserts24h, 0)));
    const tx1h = Math.max(0, Math.floor(n(growth.txUpserts1h, 0)));
    const tx24h = Math.max(0, Math.floor(n(growth.txUpserts24h, 0)));
    const total1h = Math.max(0, Math.floor(n(growth.totalUpserts1h, wallet1h + tx1h)));
    const total24h = Math.max(0, Math.floor(n(growth.totalUpserts24h, wallet24h + tx24h)));
    const rate = n(growth.ratePerHour, NaN);
    if (!db.enabled) {
      attributionGrowthNode.textContent = "DB disabled";
    } else {
      const rateText = Number.isFinite(rate) ? fmtNum(rate, 2) : "-";
      attributionGrowthNode.textContent = `+${fmtInt(total1h)}/1h • +${fmtInt(total24h)}/24h • ${rateText}/h`;
    }
  }
}

function payloadSignature(payload) {
  const positionsTotal = payloadPositionsTotal(payload);
  const indexedWalletsTotal = payloadWalletsTotal(payload);
  const publicTradesLen = extractPublicTrades(payload).length;
  const positionsLen = extractPositions(payload).length;
  const walletPerfLen = extractWalletPerformance(payload).length;
  return [
    positionsTotal,
    indexedWalletsTotal,
    publicTradesLen,
    positionsLen,
    walletPerfLen,
  ].join(":");
}

function render(options = {}) {
  const refreshOptions = Boolean(options.refreshOptions);
  const payload = state.payload;
  if (!payload) return;

  const rows = state.rows || { positions: [], wallets: [] };
  if (refreshOptions) {
    refreshFilterOptions(rows);
  }

  const positionsFiltered = applyPositionFilters(rows.positions);
  const walletsFiltered = applyWalletFilters(rows.wallets);
  const positionsWindow = pageWindowFromSummary("positions", positionsFiltered.length);
  const walletsWindow = pageWindowFromSummary("wallets", walletsFiltered.length);

  const posCountNode = el("lt-positions-row-count");
  if (posCountNode) {
    posCountNode.textContent = `${fmtInt(positionsFiltered.length)} shown • ${fmtInt(
      positionsWindow.totalRows
    )} total`;
  }

  const walletCountNode = el("lt-wallets-row-count");
  if (walletCountNode) {
    walletCountNode.textContent = `${fmtInt(walletsFiltered.length)} shown • ${fmtInt(
      walletsWindow.totalRows
    )} total`;
  }

  renderHealth(payload);
  renderFilterMeta(positionsFiltered.length, walletsFiltered.length);
  renderActiveView(positionsFiltered.length, walletsFiltered.length);
  renderPositionsTable(positionsFiltered);
  renderWalletsTable(walletsFiltered);
  renderPagination("positions", positionsWindow);
  renderPagination("wallets", walletsWindow);
}

async function fetchSnapshot() {
  const controller = typeof AbortController === "function" ? new AbortController() : null;
  const timeoutId = controller
    ? setTimeout(() => {
        try {
          controller.abort();
        } catch (_error) {
          // ignore abort errors
        }
      }, SNAPSHOT_TIMEOUT_MS)
    : null;

  const res = await fetch(buildLiveTradesPath("/api/live-trades"), {
    cache: "no-store",
    signal: controller ? controller.signal : undefined,
  }).finally(() => {
    if (timeoutId) clearTimeout(timeoutId);
  });
  if (!res.ok) throw new Error(`live-trades: ${res.status}`);
  return res.json();
}

function applyPayload(payload, source = "poll") {
  state.payload = payload;
  state.stream.source = source;
  state.stream.lastPayloadAt = Date.now();
  state.stream.lastEventAt = tsMs(payload?.generatedAt) || Date.now();

  const nextPositionSignature = positionsSignature(payload);
  const nextWalletSignature = walletsSignature(payload);
  const remapPositions = nextPositionSignature !== state.rowSignatures.positions;
  const remapWallets = nextWalletSignature !== state.rowSignatures.wallets;

  const nextSignature = payloadSignature(payload);
  if (remapPositions || remapWallets || nextSignature !== state.payloadSignature) {
    try {
      const nextRows = buildRows(payload, {
        includePositions: remapPositions,
        includeWallets: remapWallets,
      });
      const prevRows = state.rows || { positions: [], wallets: [] };
      const summary = payload?.summary || {};
      const openPositionsTotal = Number.isFinite(n(summary.openPositionsTotal, NaN))
        ? n(summary.openPositionsTotal, 0)
        : payloadPositionsTotal(payload);
      const indexedWalletsTotal = Number.isFinite(n(summary.indexedWalletsTotal, NaN))
        ? n(summary.indexedWalletsTotal, 0)
        : payloadWalletsTotal(payload);
      const streamLikelyAlive = source === "sse" || state.stream.connection === "open";

      // Guard against transient empty snapshots that can briefly wipe the UI.
      const preservePositions =
        remapPositions &&
        streamLikelyAlive &&
        safeArray(nextRows.positions).length === 0 &&
        prevRows.positions.length > 0 &&
        openPositionsTotal > 0;
      const preserveWallets =
        remapWallets &&
        streamLikelyAlive &&
        safeArray(nextRows.wallets).length === 0 &&
        prevRows.wallets.length > 0 &&
        indexedWalletsTotal > 0;

      state.rows = {
        positions: remapPositions
          ? preservePositions
            ? prevRows.positions
            : safeArray(nextRows.positions)
          : prevRows.positions,
        wallets: remapWallets
          ? preserveWallets
            ? prevRows.wallets
            : safeArray(nextRows.wallets)
          : prevRows.wallets,
      };
      if (remapPositions) state.rowSignatures.positions = nextPositionSignature;
      if (remapWallets) state.rowSignatures.wallets = nextWalletSignature;
      state.payloadSignature = nextSignature;
    } catch (error) {
      state.stream.lastError = `payload_map_error: ${error && error.message ? error.message : "unknown"}`;
      state.stream.lastErrorAt = Date.now();
    }
  }
  render({ refreshOptions: true });
}

function stopEventSource() {
  if (state.stream.eventSource) {
    state.stream.eventSource.close();
    state.stream.eventSource = null;
  }
}

function ensureFallbackPolling() {
  if (state.stream.fallbackTimer) return;
  state.stream.fallbackTimer = setInterval(async () => {
    if (state.stream.paused) return;
    try {
      const payload = await fetchSnapshot();
      applyPayload(payload, "poll");
      if (state.stream.connection !== "open") state.stream.connection = "fallback";
    } catch (error) {
      state.stream.lastError = error.message;
      state.stream.lastErrorAt = Date.now();
    }
  }, 5000);
}

function clearFallbackPolling() {
  if (state.stream.fallbackTimer) {
    clearInterval(state.stream.fallbackTimer);
    state.stream.fallbackTimer = null;
  }
}

function connectSse() {
  if (state.stream.paused) return;
  stopEventSource();
  state.stream.connection = "connecting";

  const streamPath = buildLiveTradesPath("/api/live-trades/stream");
  const es = new EventSource(streamPath);
  state.stream.eventSource = es;

  const handleStreamMessage = (event) => {
    if (!event || !event.data) return;
    try {
      const message = JSON.parse(event.data);
      if (message && message.type === "heartbeat") {
        state.stream.lastEventAt = Date.now();
        renderHealth(state.payload || {});
        return;
      }
      if (message && message.type === "snapshot" && message.payload) {
        applyPayload(message.payload, "sse");
        return;
      }
      if (message && message.payload) {
        applyPayload(message.payload, "sse");
      }
    } catch (_error) {
      // ignore malformed message
    }
  };

  es.onopen = () => {
    state.stream.connection = "open";
    state.stream.retryMs = 1000;
    state.stream.lastError = null;
    clearFallbackPolling();
    render();
  };

  es.onmessage = handleStreamMessage;
  es.addEventListener("snapshot", handleStreamMessage);
  es.addEventListener("heartbeat", handleStreamMessage);

  es.onerror = () => {
    state.stream.lastError = "sse_error";
    state.stream.lastErrorAt = Date.now();
    state.stream.connection = "reconnecting";
    render();
    stopEventSource();
    ensureFallbackPolling();
    const waitMs = state.stream.retryMs;
    setTimeout(() => connectSse(), waitMs);
    state.stream.retryMs = Math.min(RECONNECT_MAX_MS, Math.round(waitMs * 1.6));
  };
}

function startHealthLoop() {
  if (state.stream.healthTimer) return;
  state.stream.healthTimer = setInterval(() => {
    renderHealth(state.payload || {});
  }, 1000);
}

function readNumber(id, options = {}) {
  const floorZero = options.floorZero !== false;
  const value = n(el(id)?.value, 0);
  return floorZero ? Math.max(0, value) : value;
}

function wireEvents() {
  el("lt-view-positions-btn")?.addEventListener("click", () => {
    state.activeView = "positions";
    render();
  });

  el("lt-view-wallets-btn")?.addEventListener("click", () => {
    state.activeView = "wallets";
    render();
  });

  el("lt-search")?.addEventListener("input", (event) => {
    state.search = String(event.target.value || "");
    resetPagination();
    render();
  });

  el("lt-filter-wallet")?.addEventListener("input", (event) => {
    state.filters.wallet = String(event.target.value || "");
    resetPagination();
    render();
  });

  el("lt-filter-symbol")?.addEventListener("change", (event) => {
    state.filters.symbol = String(event.target.value || "");
    resetPagination();
    render();
  });

  el("lt-filter-side")?.addEventListener("change", (event) => {
    state.filters.side = String(event.target.value || "");
    resetPagination();
    render();
  });

  el("lt-filter-min-usd")?.addEventListener("input", () => {
    state.filters.minUsd = readNumber("lt-filter-min-usd");
    resetPagination();
    render();
  });

  el("lt-filter-max-usd")?.addEventListener("input", () => {
    state.filters.maxUsd = readNumber("lt-filter-max-usd");
    resetPagination();
    render();
  });

  el("lt-filter-min-pnl")?.addEventListener("input", () => {
    state.filters.minPnl = readNumber("lt-filter-min-pnl", { floorZero: false });
    resetPagination();
    render();
  });

  el("lt-filter-max-pnl")?.addEventListener("input", () => {
    state.filters.maxPnl = readNumber("lt-filter-max-pnl", { floorZero: false });
    resetPagination();
    render();
  });

  el("lt-filter-freshness")?.addEventListener("change", (event) => {
    state.filters.freshness = String(event.target.value || "");
    resetPagination();
    render();
  });

  el("lt-filter-tracked")?.addEventListener("change", (event) => {
    state.filters.trackedOnly = Boolean(event.target.checked);
    resetPagination();
    render();
  });

  el("lt-positions-page-size")?.addEventListener("change", (event) => {
    state.pagination.positions.pageSize = Math.max(10, Math.floor(n(event.target.value, 100)));
    state.pagination.positions.page = 1;
    fetchSnapshot()
      .then((payload) => applyPayload(payload, "page"))
      .catch(() => {});
    connectSse();
  });

  el("lt-wallets-page-size")?.addEventListener("change", (event) => {
    state.pagination.wallets.pageSize = Math.max(10, Math.floor(n(event.target.value, 100)));
    state.pagination.wallets.page = 1;
    fetchSnapshot()
      .then((payload) => applyPayload(payload, "page"))
      .catch(() => {});
    connectSse();
  });

  el("lt-positions-page-prev")?.addEventListener("click", () => {
    state.pagination.positions.page = Math.max(1, n(state.pagination.positions.page, 1) - 1);
    fetchSnapshot()
      .then((payload) => applyPayload(payload, "page"))
      .catch(() => {});
    connectSse();
  });

  el("lt-positions-page-next")?.addEventListener("click", () => {
    state.pagination.positions.page = n(state.pagination.positions.page, 1) + 1;
    fetchSnapshot()
      .then((payload) => applyPayload(payload, "page"))
      .catch(() => {});
    connectSse();
  });

  el("lt-wallets-page-prev")?.addEventListener("click", () => {
    state.pagination.wallets.page = Math.max(1, n(state.pagination.wallets.page, 1) - 1);
    fetchSnapshot()
      .then((payload) => applyPayload(payload, "page"))
      .catch(() => {});
    connectSse();
  });

  el("lt-wallets-page-next")?.addEventListener("click", () => {
    state.pagination.wallets.page = n(state.pagination.wallets.page, 1) + 1;
    fetchSnapshot()
      .then((payload) => applyPayload(payload, "page"))
      .catch(() => {});
    connectSse();
  });

  el("lt-reset-filters")?.addEventListener("click", () => {
    state.search = "";
    state.filters = {
      wallet: "",
      symbol: "",
      side: "",
      minUsd: 0,
      maxUsd: 0,
      minPnl: 0,
      maxPnl: 0,
      freshness: "",
      trackedOnly: false,
    };

    [
      "lt-search",
      "lt-filter-wallet",
      "lt-filter-min-usd",
      "lt-filter-max-usd",
      "lt-filter-min-pnl",
      "lt-filter-max-pnl",
    ].forEach((id) => {
      if (el(id)) el(id).value = "";
    });

    ["lt-filter-symbol", "lt-filter-side", "lt-filter-freshness"].forEach((id) => {
      if (el(id)) el(id).value = "";
    });

    if (el("lt-filter-tracked")) el("lt-filter-tracked").checked = false;

    resetPagination();
    render();
  });

  el("lt-refresh-btn")?.addEventListener("click", async () => {
    try {
      const payload = await fetchSnapshot();
      applyPayload(payload, "manual");
    } catch (_error) {
      // keep UI alive
    }
  });

  el("lt-pause-btn")?.addEventListener("click", () => {
    state.stream.paused = !state.stream.paused;
    const btn = el("lt-pause-btn");
    if (btn) {
      btn.textContent = state.stream.paused ? "Resume Stream" : "Pause Stream";
      btn.setAttribute("aria-pressed", state.stream.paused ? "true" : "false");
    }

    if (state.stream.paused) {
      stopEventSource();
      clearFallbackPolling();
      state.stream.connection = "paused";
    } else {
      connectSse();
      ensureFallbackPolling();
    }
    render();
  });
}

async function boot() {
  wireEvents();
  startHealthLoop();

  // Start live channels first so UI is never stuck waiting on an initial snapshot.
  state.stream.connection = "connecting";
  renderHealth(state.payload || {});
  connectSse();
  ensureFallbackPolling();

  try {
    const payload = await fetchSnapshot();
    applyPayload(payload, "bootstrap");
  } catch (_error) {
    // continue with stream fallback
  }

  if (typeof window !== "undefined") {
    window.__PF_LIVE_TRADE_BOOTED = true;
  }
}

boot();
