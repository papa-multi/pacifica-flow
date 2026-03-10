const path = require("path");
const { ensureDir, parseNumber, readJson, writeJsonAtomic } = require("./utils");

const MAX_RECENT = 180;
const MAX_HISTORY = 1500;
const MAX_PUBLIC_TRADES_RAW = Number(process.env.PACIFICA_MAX_PUBLIC_TRADES_PER_SYMBOL || 0);
const MAX_PUBLIC_TRADES =
  Number.isFinite(MAX_PUBLIC_TRADES_RAW) && MAX_PUBLIC_TRADES_RAW > 0
    ? Math.max(50, Math.floor(MAX_PUBLIC_TRADES_RAW))
    : Infinity;
const MAX_CANDLES = 500;

function emptyState() {
  return {
    market: {
      infoBySymbol: {},
      pricesBySymbol: {},
      fundingBySymbol: {},
      candlesBySymbol: {},
      markCandlesBySymbol: {},
      orderbooksBySymbol: {},
      bboBySymbol: {},
      publicTradesBySymbol: {},
      updatedAt: null,
    },
    account: {
      overview: null,
      settingsBySymbol: {},
      marginModeBySymbol: {},
      leverageBySymbol: {},
      openPositions: [],
      openOrders: [],
      orderHistory: [],
      recentOrderUpdates: [],
      tradeHistory: [],
      recentTrades: [],
      fundingHistory: [],
      balanceHistory: [],
      portfolioHistory: [],
      updatedAt: null,
    },
    creator: {
      collectionsCount: 0,
      updatedAt: null,
    },
    sync: {
      wsStatus: "idle",
      lastBootstrapAt: null,
      lastEventId: null,
      lastLiByChannel: {},
      updatedAt: null,
    },
  };
}

function toKey(value) {
  return String(value === undefined || value === null ? "" : value);
}

function uniqueBy(list = [], keyFn) {
  const map = new Map();
  list.forEach((item) => {
    map.set(keyFn(item), item);
  });
  return Array.from(map.values());
}

function normalizePriceItem(item = {}) {
  return {
    symbol: String(item.symbol || item.s || "").toUpperCase(),
    mark: String(item.mark || item.m || "0"),
    oracle: String(item.oracle || "0"),
    mid: String(item.mid || "0"),
    funding: String(item.funding || item.funding_rate || "0"),
    nextFunding: String(item.next_funding || item.next_funding_rate || "0"),
    openInterest: String(item.open_interest || "0"),
    volume24h: String(item.volume_24h || "0"),
    yesterdayPrice: String(item.yesterday_price || "0"),
    timestamp: Number(item.timestamp || item.t || Date.now()),
  };
}

function normalizeMarketInfoItem(item = {}) {
  return {
    symbol: String(item.symbol || "").toUpperCase(),
    tickSize: String(item.tick_size || "0"),
    lotSize: String(item.lot_size || "0"),
    maxLeverage: Number(item.max_leverage || 0),
    isolatedOnly: Boolean(item.isolated_only),
    minOrderSize: String(item.min_order_size || "0"),
    maxOrderSize: String(item.max_order_size || "0"),
    fundingRate: String(item.funding_rate || "0"),
    nextFundingRate: String(item.next_funding_rate || "0"),
    createdAt: Number(item.created_at || 0),
  };
}

function normalizeFundingRateHistoryItem(item = {}) {
  return {
    oraclePrice: String(item.oracle_price || "0"),
    bidImpactPrice: String(item.bid_impact_price || "0"),
    askImpactPrice: String(item.ask_impact_price || "0"),
    fundingRate: String(item.funding_rate || "0"),
    nextFundingRate: String(item.next_funding_rate || "0"),
    createdAt: Number(item.created_at || Date.now()),
  };
}

function normalizeAccountOverviewFromRest(item = {}) {
  return {
    balance: String(item.balance || "0"),
    accountEquity: String(item.account_equity || "0"),
    availableToSpend: String(item.available_to_spend || "0"),
    availableToWithdraw: String(item.available_to_withdraw || "0"),
    pendingBalance: String(item.pending_balance || "0"),
    makerFee: String(item.maker_fee || "0"),
    takerFee: String(item.taker_fee || "0"),
    feeLevel: Number(item.fee_level || 0),
    totalMarginUsed: String(item.total_margin_used || "0"),
    crossMmr: String(item.cross_mmr || "0"),
    positionsCount: Number(item.positions_count || 0),
    ordersCount: Number(item.orders_count || 0),
    stopOrdersCount: Number(item.stop_orders_count || 0),
    useLtpForStopOrders: Boolean(item.use_ltp_for_stop_orders),
    updatedAt: Number(item.updated_at || Date.now()),
  };
}

function normalizeAccountOverviewFromWs(item = {}) {
  return {
    balance: String(item.b || "0"),
    accountEquity: String(item.ae || "0"),
    availableToSpend: String(item.as || "0"),
    availableToWithdraw: String(item.aw || "0"),
    pendingBalance: String(item.pb || "0"),
    makerFee: null,
    takerFee: null,
    feeLevel: Number(item.f || 0),
    totalMarginUsed: String(item.mu || "0"),
    crossMmr: String(item.cm || "0"),
    positionsCount: Number(item.pc || 0),
    ordersCount: Number(item.oc || 0),
    stopOrdersCount: Number(item.sc || 0),
    useLtpForStopOrders: null,
    updatedAt: Number(item.t || Date.now()),
  };
}

function normalizeAccountSettingFromRest(item = {}) {
  return {
    symbol: String(item.symbol || "").toUpperCase(),
    isolated: Boolean(item.isolated),
    leverage: Number(item.leverage || 0),
    createdAt: Number(item.created_at || Date.now()),
    updatedAt: Number(item.updated_at || Date.now()),
  };
}

function normalizePositionFromRest(item = {}) {
  return {
    symbol: String(item.symbol || "").toUpperCase(),
    side: String(item.side || "").toLowerCase(),
    amount: String(item.amount || "0"),
    entryPrice: String(item.entry_price || "0"),
    margin: item.margin === undefined || item.margin === null ? null : String(item.margin),
    funding: String(item.funding || "0"),
    isolated: Boolean(item.isolated),
    liquidationPrice: item.liquidation_price === undefined ? null : String(item.liquidation_price),
    createdAt: Number(item.created_at || Date.now()),
    updatedAt: Number(item.updated_at || Date.now()),
  };
}

function normalizePositionFromWs(item = {}) {
  return {
    symbol: String(item.s || "").toUpperCase(),
    side: String(item.d || "").toLowerCase(),
    amount: String(item.a || "0"),
    entryPrice: String(item.p || "0"),
    margin: item.m === undefined || item.m === null ? null : String(item.m),
    funding: String(item.f || "0"),
    isolated: Boolean(item.i),
    liquidationPrice: item.l === undefined || item.l === null ? null : String(item.l),
    createdAt: Number(item.t || Date.now()),
    updatedAt: Number(item.t || Date.now()),
  };
}

function normalizeOrderFromRest(item = {}) {
  return {
    orderId: item.order_id || null,
    clientOrderId: item.client_order_id || null,
    symbol: String(item.symbol || "").toUpperCase(),
    side: String(item.side || "").toLowerCase(),
    price: String(item.price || item.initial_price || "0"),
    initialAmount: String(item.initial_amount || item.amount || "0"),
    filledAmount: String(item.filled_amount || "0"),
    cancelledAmount: String(item.cancelled_amount || "0"),
    orderStatus: item.order_status || "open",
    orderType: item.order_type || null,
    stopPrice: item.stop_price === undefined ? null : item.stop_price,
    stopParentOrderId: item.stop_parent_order_id || null,
    reduceOnly: Boolean(item.reduce_only),
    reason: item.reason || null,
    eventType: item.event_type || null,
    createdAt: Number(item.created_at || Date.now()),
    updatedAt: Number(item.updated_at || item.created_at || Date.now()),
    li: item.last_order_id || null,
  };
}

function normalizeOrderUpdateFromWs(item = {}) {
  return {
    orderId: item.i || null,
    clientOrderId: item.I || null,
    account: item.u || null,
    symbol: String(item.s || "").toUpperCase(),
    side: String(item.d || "").toLowerCase(),
    averageFilledPrice: String(item.p || "0"),
    initialPrice: String(item.ip || "0"),
    lastFilledPrice: String(item.lp || "0"),
    amount: String(item.a || "0"),
    filledAmount: String(item.f || "0"),
    orderEvent: item.oe || null,
    orderStatus: item.os || null,
    orderType: item.ot || null,
    stopPrice: item.sp || null,
    stopParentOrderId: item.si || null,
    reduceOnly: Boolean(item.r),
    createdAt: Number(item.ct || Date.now()),
    updatedAt: Number(item.ut || Date.now()),
    li: item.li || null,
  };
}

function normalizeTradeFromRest(item = {}) {
  return {
    historyId: item.history_id || null,
    orderId: item.order_id || null,
    clientOrderId: item.client_order_id || null,
    symbol: String(item.symbol || "").toUpperCase(),
    side: String(item.side || "").toLowerCase(),
    amount: String(item.amount || "0"),
    price: String(item.price || "0"),
    entryPrice: String(item.entry_price || "0"),
    fee: String(item.fee || "0"),
    pnl: String(item.pnl || "0"),
    eventType: item.event_type || null,
    cause: item.cause || null,
    timestamp: Number(item.created_at || Date.now()),
    li: item.last_order_id || null,
  };
}

function normalizeTradeFromWs(item = {}) {
  return {
    historyId: item.h || null,
    orderId: item.i || null,
    clientOrderId: item.I || null,
    symbol: String(item.s || "").toUpperCase(),
    side: String(item.ts || "").toLowerCase(),
    amount: String(item.a || "0"),
    price: String(item.p || "0"),
    entryPrice: String(item.o || "0"),
    fee: String(item.f || "0"),
    pnl: String(item.n || "0"),
    eventType: item.te || null,
    cause: item.tc || null,
    timestamp: Number(item.t || Date.now()),
    li: item.li || null,
  };
}

function normalizeFundingHistoryItem(item = {}) {
  return {
    historyId: item.history_id || null,
    symbol: String(item.symbol || "").toUpperCase(),
    side: String(item.side || "").toLowerCase(),
    amount: String(item.amount || "0"),
    payout: String(item.payout || "0"),
    rate: String(item.rate || "0"),
    createdAt: Number(item.created_at || Date.now()),
  };
}

function normalizeBalanceHistoryItem(item = {}) {
  return {
    amount: String(item.amount || "0"),
    balance: String(item.balance || "0"),
    pendingBalance: String(item.pending_balance || "0"),
    eventType: item.event_type || null,
    createdAt: Number(item.created_at || Date.now()),
  };
}

function normalizePortfolioItem(item = {}) {
  return {
    accountEquity: String(item.account_equity || "0"),
    pnl: String(item.pnl || "0"),
    timestamp: Number(item.timestamp || Date.now()),
  };
}

function normalizeBookItem(item = {}) {
  return {
    symbol: String(item.s || item.symbol || "").toUpperCase(),
    levels: item.l || [[], []],
    timestamp: Number(item.t || Date.now()),
    li: item.li || null,
  };
}

function normalizeBboItem(item = {}) {
  return {
    symbol: String(item.s || "").toUpperCase(),
    bid: String(item.b || "0"),
    bidSize: String(item.B || "0"),
    ask: String(item.a || "0"),
    askSize: String(item.A || "0"),
    orderId: item.i || null,
    timestamp: Number(item.t || Date.now()),
    li: item.li || null,
  };
}

function normalizePublicTradeItem(item = {}) {
  return {
    historyId: item.h || null,
    orderId: item.i || null,
    symbol: String(item.s || "").toUpperCase(),
    amount: String(item.a || "0"),
    price: String(item.p || "0"),
    side: item.d || null,
    cause: item.tc || null,
    timestamp: Number(item.t || Date.now()),
    li: item.li || null,
  };
}

function normalizeCandleItem(item = {}) {
  return {
    t: Number(item.t || Date.now()),
    T: Number(item.T || Date.now()),
    s: String(item.s || "").toUpperCase(),
    i: String(item.i || "1m"),
    o: String(item.o || "0"),
    c: String(item.c || "0"),
    h: String(item.h || "0"),
    l: String(item.l || "0"),
    v: String(item.v || "0"),
    n: Number(item.n || 0),
  };
}

class StateService {
  constructor(options = {}) {
    this.dataDir = options.dataDir;
    this.filePath = options.filePath || path.join(this.dataDir || ".", "snapshots", "state.json");
    this.state = emptyState();
  }

  load() {
    ensureDir(path.dirname(this.filePath));
    this.state = readJson(this.filePath, emptyState()) || emptyState();
    return this.state;
  }

  reset() {
    this.state = emptyState();
  }

  getState() {
    return this.state;
  }

  applyEvent(event = {}) {
    const type = String(event.type || "");
    const now = Date.now();

    switch (type) {
      case "snapshot.market_info": {
        const list = Array.isArray(event.data) ? event.data : [];
        list.forEach((raw) => {
          const row = normalizeMarketInfoItem(raw);
          if (!row.symbol) return;
          this.state.market.infoBySymbol[row.symbol] = row;
        });
        this.state.market.updatedAt = now;
        break;
      }

      case "snapshot.prices":
      case "ws.prices": {
        const list = Array.isArray(event.data) ? event.data : [];
        list.forEach((raw) => {
          const row = normalizePriceItem(raw);
          if (!row.symbol) return;
          this.state.market.pricesBySymbol[row.symbol] = row;
        });
        this.state.market.updatedAt = now;
        break;
      }

      case "snapshot.funding_rates": {
        const payload = event.data && typeof event.data === "object" ? event.data : {};
        Object.entries(payload).forEach(([symbolRaw, rowsRaw]) => {
          const symbol = String(symbolRaw || "").toUpperCase();
          if (!symbol) return;
          const rows = (Array.isArray(rowsRaw) ? rowsRaw : [])
            .map(normalizeFundingRateHistoryItem)
            .sort((a, b) => b.createdAt - a.createdAt)
            .slice(0, MAX_HISTORY);
          this.state.market.fundingBySymbol[symbol] = {
            symbol,
            rows,
            updatedAt: now,
          };
        });
        this.state.market.updatedAt = now;
        break;
      }

      case "snapshot.orderbooks": {
        const payload = event.data && typeof event.data === "object" ? event.data : {};
        Object.entries(payload).forEach(([symbolRaw, raw]) => {
          const book = normalizeBookItem(raw || {});
          if (!book.symbol) book.symbol = String(symbolRaw || "").toUpperCase();
          if (!book.symbol) return;
          this.state.market.orderbooksBySymbol[book.symbol] = book;
        });
        this.state.market.updatedAt = now;
        break;
      }

      case "snapshot.candles": {
        const payload = event.data && typeof event.data === "object" ? event.data : {};
        Object.entries(payload).forEach(([symbolRaw, rowsRaw]) => {
          const symbol = String(symbolRaw || "").toUpperCase();
          if (!symbol) return;
          const rows = (Array.isArray(rowsRaw) ? rowsRaw : [])
            .map(normalizeCandleItem)
            .sort((a, b) => a.t - b.t)
            .slice(-MAX_CANDLES);
          this.state.market.candlesBySymbol[symbol] = {
            symbol,
            interval: rows[0] ? rows[0].i : "1h",
            rows,
            updatedAt: now,
          };
        });
        this.state.market.updatedAt = now;
        break;
      }

      case "snapshot.mark_candles": {
        const payload = event.data && typeof event.data === "object" ? event.data : {};
        Object.entries(payload).forEach(([symbolRaw, rowsRaw]) => {
          const symbol = String(symbolRaw || "").toUpperCase();
          if (!symbol) return;
          const rows = (Array.isArray(rowsRaw) ? rowsRaw : [])
            .map(normalizeCandleItem)
            .sort((a, b) => a.t - b.t)
            .slice(-MAX_CANDLES);
          this.state.market.markCandlesBySymbol[symbol] = {
            symbol,
            interval: rows[0] ? rows[0].i : "1h",
            rows,
            updatedAt: now,
          };
        });
        this.state.market.updatedAt = now;
        break;
      }

      case "ws.bbo": {
        const row = normalizeBboItem(event.data || {});
        if (row.symbol) {
          this.state.market.bboBySymbol[row.symbol] = row;
          this.state.market.updatedAt = now;
        }
        break;
      }

      case "ws.book": {
        const row = normalizeBookItem(event.data || {});
        if (row.symbol) {
          this.state.market.orderbooksBySymbol[row.symbol] = row;
          this.state.market.updatedAt = now;
        }
        break;
      }

      case "ws.trades_public": {
        const list = Array.isArray(event.data) ? event.data : [];
        list.forEach((raw) => {
          const row = normalizePublicTradeItem(raw);
          if (!row.symbol) return;
          const prev = Array.isArray(this.state.market.publicTradesBySymbol[row.symbol])
            ? this.state.market.publicTradesBySymbol[row.symbol]
            : [];
          const dedupedSorted = uniqueBy(
            [row, ...prev],
            (item) => toKey(item.historyId || `${item.timestamp}:${item.price}:${item.amount}`)
          ).sort((a, b) => b.timestamp - a.timestamp);
          const next = Number.isFinite(MAX_PUBLIC_TRADES)
            ? dedupedSorted.slice(0, MAX_PUBLIC_TRADES)
            : dedupedSorted;
          this.state.market.publicTradesBySymbol[row.symbol] = next;
        });
        this.state.market.updatedAt = now;
        break;
      }

      case "ws.candle": {
        const row = normalizeCandleItem(event.data || {});
        if (!row.s) break;
        const prevRows =
          (this.state.market.candlesBySymbol[row.s] && this.state.market.candlesBySymbol[row.s].rows) || [];
        const nextRows = uniqueBy([...prevRows, row], (item) => toKey(item.t))
          .sort((a, b) => a.t - b.t)
          .slice(-MAX_CANDLES);
        this.state.market.candlesBySymbol[row.s] = {
          symbol: row.s,
          interval: row.i,
          rows: nextRows,
          updatedAt: now,
        };
        this.state.market.updatedAt = now;
        break;
      }

      case "ws.mark_price_candle": {
        const row = normalizeCandleItem(event.data || {});
        if (!row.s) break;
        const prevRows =
          (this.state.market.markCandlesBySymbol[row.s] &&
            this.state.market.markCandlesBySymbol[row.s].rows) || [];
        const nextRows = uniqueBy([...prevRows, row], (item) => toKey(item.t))
          .sort((a, b) => a.t - b.t)
          .slice(-MAX_CANDLES);
        this.state.market.markCandlesBySymbol[row.s] = {
          symbol: row.s,
          interval: row.i,
          rows: nextRows,
          updatedAt: now,
        };
        this.state.market.updatedAt = now;
        break;
      }

      case "snapshot.account_info": {
        this.state.account.overview = normalizeAccountOverviewFromRest(event.data || {});
        this.state.account.updatedAt = now;
        break;
      }

      case "ws.account_info": {
        this.state.account.overview = normalizeAccountOverviewFromWs(event.data || {});
        this.state.account.updatedAt = now;
        break;
      }

      case "snapshot.account_settings": {
        const list = Array.isArray(event.data) ? event.data : [];
        const next = {};
        list.forEach((raw) => {
          const row = normalizeAccountSettingFromRest(raw);
          if (!row.symbol) return;
          next[row.symbol] = row;
          this.state.account.marginModeBySymbol[row.symbol] = row.isolated;
          this.state.account.leverageBySymbol[row.symbol] = row.leverage;
        });
        this.state.account.settingsBySymbol = next;
        this.state.account.updatedAt = now;
        break;
      }

      case "ws.account_margin": {
        const raw = event.data || {};
        const symbol = String(raw.s || "").toUpperCase();
        if (symbol) {
          this.state.account.marginModeBySymbol[symbol] = Boolean(raw.i);
          const base = this.state.account.settingsBySymbol[symbol] || {
            symbol,
            isolated: Boolean(raw.i),
            leverage: Number(this.state.account.leverageBySymbol[symbol] || 0),
            createdAt: Number(raw.t || now),
            updatedAt: Number(raw.t || now),
          };
          this.state.account.settingsBySymbol[symbol] = {
            ...base,
            isolated: Boolean(raw.i),
            updatedAt: Number(raw.t || now),
          };
          this.state.account.updatedAt = now;
        }
        break;
      }

      case "ws.account_leverage": {
        const raw = event.data || {};
        const symbol = String(raw.s || "").toUpperCase();
        if (symbol) {
          const leverage = Number(raw.l || 0);
          this.state.account.leverageBySymbol[symbol] = leverage;
          const base = this.state.account.settingsBySymbol[symbol] || {
            symbol,
            isolated: Boolean(this.state.account.marginModeBySymbol[symbol]),
            leverage,
            createdAt: Number(raw.t || now),
            updatedAt: Number(raw.t || now),
          };
          this.state.account.settingsBySymbol[symbol] = {
            ...base,
            leverage,
            updatedAt: Number(raw.t || now),
          };
          this.state.account.updatedAt = now;
        }
        break;
      }

      case "snapshot.positions": {
        const list = Array.isArray(event.data) ? event.data : [];
        this.state.account.openPositions = list.map(normalizePositionFromRest);
        this.state.account.updatedAt = now;
        break;
      }

      case "ws.account_positions": {
        const list = Array.isArray(event.data) ? event.data : [];
        this.state.account.openPositions = list.map(normalizePositionFromWs);
        this.state.account.updatedAt = now;
        break;
      }

      case "snapshot.orders": {
        const list = Array.isArray(event.data) ? event.data : [];
        this.state.account.openOrders = list.map(normalizeOrderFromRest);
        this.state.account.updatedAt = now;
        break;
      }

      case "snapshot.order_history": {
        const list = Array.isArray(event.data) ? event.data : [];
        this.state.account.orderHistory = list
          .map(normalizeOrderFromRest)
          .sort((a, b) => Number(b.updatedAt || b.createdAt || 0) - Number(a.updatedAt || a.createdAt || 0))
          .slice(0, MAX_HISTORY);
        this.state.account.updatedAt = now;
        break;
      }

      case "ws.account_order_updates": {
        const list = Array.isArray(event.data) ? event.data : [];
        const normalized = list.map(normalizeOrderUpdateFromWs);

        this.state.account.recentOrderUpdates = uniqueBy(
          [...normalized, ...this.state.account.recentOrderUpdates],
          (row) => toKey(row.li || `${row.orderId}:${row.updatedAt}:${row.orderEvent}`)
        )
          .sort((a, b) => Number(b.updatedAt || 0) - Number(a.updatedAt || 0))
          .slice(0, MAX_RECENT);

        const mergedHistory = normalized.map((row) => ({
          orderId: row.orderId,
          clientOrderId: row.clientOrderId,
          symbol: row.symbol,
          side: row.side,
          price: row.initialPrice || row.averageFilledPrice || "0",
          initialAmount: row.amount,
          filledAmount: row.filledAmount,
          cancelledAmount: "0",
          orderStatus: row.orderStatus || "open",
          orderType: row.orderType,
          stopPrice: row.stopPrice,
          stopParentOrderId: row.stopParentOrderId,
          reduceOnly: row.reduceOnly,
          reason: null,
          eventType: row.orderEvent,
          createdAt: row.createdAt,
          updatedAt: row.updatedAt,
          li: row.li,
        }));

        this.state.account.orderHistory = uniqueBy(
          [...mergedHistory, ...this.state.account.orderHistory],
          (row) => toKey(row.li || `${row.orderId}:${row.updatedAt}:${row.eventType || row.orderStatus}`)
        )
          .sort((a, b) => Number(b.updatedAt || b.createdAt || 0) - Number(a.updatedAt || a.createdAt || 0))
          .slice(0, MAX_HISTORY);

        const openMap = new Map(
          (Array.isArray(this.state.account.openOrders) ? this.state.account.openOrders : []).map((row) => [
            toKey(row.orderId),
            row,
          ])
        );

        normalized.forEach((row) => {
          const key = toKey(row.orderId);
          if (!key) return;
          if (["filled", "cancelled", "rejected"].includes(String(row.orderStatus || "").toLowerCase())) {
            openMap.delete(key);
            return;
          }
          const prev = openMap.get(key) || {};
          openMap.set(key, {
            ...prev,
            orderId: row.orderId,
            clientOrderId: row.clientOrderId,
            symbol: row.symbol,
            side: row.side,
            price: row.initialPrice || row.averageFilledPrice || prev.price || "0",
            initialAmount: row.amount || prev.initialAmount || "0",
            filledAmount: row.filledAmount || prev.filledAmount || "0",
            cancelledAmount: prev.cancelledAmount || "0",
            orderStatus: row.orderStatus || "open",
            orderType: row.orderType,
            stopPrice: row.stopPrice,
            stopParentOrderId: row.stopParentOrderId,
            reduceOnly: row.reduceOnly,
            reason: null,
            eventType: row.orderEvent,
            createdAt: prev.createdAt || row.createdAt,
            updatedAt: row.updatedAt,
            li: row.li,
          });
        });

        this.state.account.openOrders = Array.from(openMap.values()).sort(
          (a, b) => Number(b.updatedAt || 0) - Number(a.updatedAt || 0)
        );

        this.state.account.updatedAt = now;
        break;
      }

      case "snapshot.trades": {
        const list = Array.isArray(event.data) ? event.data : [];
        const rows = list
          .map(normalizeTradeFromRest)
          .sort((a, b) => Number(b.timestamp || 0) - Number(a.timestamp || 0));
        this.state.account.tradeHistory = rows.slice(0, MAX_HISTORY);
        this.state.account.recentTrades = rows.slice(0, MAX_RECENT);
        this.state.account.updatedAt = now;
        break;
      }

      case "ws.account_trades": {
        const list = Array.isArray(event.data) ? event.data : [];
        const normalized = list.map(normalizeTradeFromWs);

        this.state.account.tradeHistory = uniqueBy(
          [...normalized, ...this.state.account.tradeHistory],
          (row) => toKey(row.li || row.historyId || `${row.timestamp}:${row.orderId}:${row.symbol}:${row.amount}`)
        )
          .sort((a, b) => Number(b.timestamp || 0) - Number(a.timestamp || 0))
          .slice(0, MAX_HISTORY);

        this.state.account.recentTrades = this.state.account.tradeHistory.slice(0, MAX_RECENT);
        this.state.account.updatedAt = now;
        break;
      }

      case "snapshot.funding_history": {
        const list = Array.isArray(event.data) ? event.data : [];
        this.state.account.fundingHistory = list
          .map(normalizeFundingHistoryItem)
          .sort((a, b) => Number(b.createdAt || 0) - Number(a.createdAt || 0))
          .slice(0, MAX_HISTORY);
        this.state.account.updatedAt = now;
        break;
      }

      case "snapshot.balance_history": {
        const list = Array.isArray(event.data) ? event.data : [];
        this.state.account.balanceHistory = list
          .map(normalizeBalanceHistoryItem)
          .sort((a, b) => Number(b.createdAt || 0) - Number(a.createdAt || 0))
          .slice(0, MAX_HISTORY);
        this.state.account.updatedAt = now;
        break;
      }

      case "snapshot.portfolio_history": {
        const list = Array.isArray(event.data) ? event.data : [];
        this.state.account.portfolioHistory = list
          .map(normalizePortfolioItem)
          .sort((a, b) => Number(a.timestamp || 0) - Number(b.timestamp || 0))
          .slice(-MAX_HISTORY);
        this.state.account.updatedAt = now;
        break;
      }

      case "creator.collection_count": {
        this.state.creator.collectionsCount = parseNumber(event.data && event.data.count, 0);
        this.state.creator.updatedAt = now;
        break;
      }

      case "status.ws": {
        this.state.sync.wsStatus = String(event.data && event.data.status ? event.data.status : "unknown");
        this.state.sync.updatedAt = now;
        break;
      }

      case "status.bootstrap": {
        this.state.sync.lastBootstrapAt = Number(event.timestamp || now);
        this.state.sync.updatedAt = now;
        break;
      }

      default:
        break;
    }

    if (event.eventId !== undefined && event.eventId !== null) {
      this.state.sync.lastEventId = event.eventId;
    }

    const liKey = event.channelKey || event.channel;
    if (liKey && event.li !== undefined && event.li !== null) {
      this.state.sync.lastLiByChannel[String(liKey)] = Number(event.li);
    }

    return this.state;
  }

  save() {
    writeJsonAtomic(this.filePath, this.state);
  }
}

module.exports = {
  StateService,
  emptyState,
};
