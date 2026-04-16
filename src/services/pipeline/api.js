function toNum(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function toFixed(value, digits = 2) {
  const num = toNum(value, NaN);
  if (!Number.isFinite(num)) return "0";
  return num.toFixed(digits);
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function normalizeArray(value) {
  return Array.isArray(value) ? value : [];
}

function parseQuery(query = {}) {
  const source = query || {};
  return {
    symbol: source.symbol ? String(source.symbol).toUpperCase() : "",
    side: source.side ? String(source.side).toLowerCase() : "",
    event: source.event ? String(source.event).toLowerCase() : "",
    status: source.status ? String(source.status).toLowerCase() : "",
    startTime: source.startTime ? Number(source.startTime) : null,
    endTime: source.endTime ? Number(source.endTime) : null,
    page: Math.max(1, Number(source.page || 1)),
    pageSize: Math.max(1, Math.min(500, Number(source.pageSize || 100))),
    limit: Math.max(1, Math.min(1000, Number(source.limit || 200))),
  };
}

function queryRows(rows, query = {}, config = {}) {
  const q = parseQuery(query);
  let list = normalizeArray(rows);

  if (q.symbol) {
    list = list.filter((row) => String(row.symbol || "").toUpperCase() === q.symbol);
  }

  if (q.side) {
    list = list.filter((row) => String(row.side || "").toLowerCase().includes(q.side));
  }

  if (q.status) {
    list = list.filter((row) => String(row.orderStatus || "").toLowerCase() === q.status);
  }

  if (q.event) {
    list = list.filter((row) => {
      const ev = String(row.eventType || row.orderEvent || row.event || "").toLowerCase();
      return ev.includes(q.event);
    });
  }

  if (Number.isFinite(q.startTime)) {
    list = list.filter((row) => Number(row.timestamp || row.updatedAt || row.createdAt || 0) >= q.startTime);
  }

  if (Number.isFinite(q.endTime)) {
    list = list.filter((row) => Number(row.timestamp || row.updatedAt || row.createdAt || 0) <= q.endTime);
  }

  const sortKey = config.sortKey || ((row) => Number(row.timestamp || row.updatedAt || row.createdAt || 0));
  const sorted = [...list].sort((a, b) => sortKey(b) - sortKey(a));

  const total = sorted.length;
  const page = q.page;
  const pageSize = q.pageSize;
  const start = (page - 1) * pageSize;
  const end = start + pageSize;

  return {
    query: q,
    total,
    page,
    pageSize,
    pages: Math.max(1, Math.ceil(total / pageSize)),
    rows: sorted.slice(start, end),
    limitedRows: sorted.slice(0, q.limit),
  };
}

function buildPositionViews(state = {}, metrics = {}) {
  const market = state.market || {};
  const account = state.account || {};
  const pricesBySymbol = market.pricesBySymbol || {};
  const overview = account.overview || {};

  const positions = normalizeArray(account.openPositions);
  const equity = toNum(overview.accountEquity, 0);

  const rows = positions.map((position) => {
    const symbol = String(position.symbol || "").toUpperCase();
    const amount = Math.abs(toNum(position.amount, 0));
    const entryPrice = toNum(position.entryPrice, 0);
    const markPrice = toNum(pricesBySymbol[symbol] ? pricesBySymbol[symbol].mark : entryPrice, entryPrice);
    const liqPrice = toNum(position.liquidationPrice, NaN);
    const notional = amount * markPrice;

    const sideRaw = String(position.side || "").toLowerCase();
    const side = sideRaw === "ask" ? "short" : "long";
    const rawPnl = (markPrice - entryPrice) * amount;
    const unrealizedPnl = side === "short" ? -rawPnl : rawPnl;
    const basis = Math.abs(entryPrice * amount);
    const unrealizedPnlPct = basis > 0 ? (unrealizedPnl / basis) * 100 : 0;

    const margin = toNum(position.margin, 0);
    const effectiveLev = margin > 0 ? notional / margin : equity > 0 ? notional / equity : 0;
    const liqDistancePct = Number.isFinite(liqPrice) && markPrice > 0
      ? (Math.abs(liqPrice - markPrice) / markPrice) * 100
      : null;

    let riskTag = "low";
    if (liqDistancePct !== null) {
      if (liqDistancePct < 5) riskTag = "high";
      else if (liqDistancePct < 12) riskTag = "medium";
    }

    return {
      ...position,
      symbol,
      side,
      sideRaw,
      amount: String(position.amount || "0"),
      amountAbs: toFixed(amount, 8),
      entryPrice: toFixed(entryPrice, 6),
      markPrice: toFixed(markPrice, 6),
      notionalUsd: toFixed(notional, 2),
      unrealizedPnlUsd: toFixed(unrealizedPnl, 2),
      unrealizedPnlPct: toFixed(unrealizedPnlPct, 2),
      fundingPaidUsd: toFixed(toNum(position.funding, 0), 6),
      liquidationPrice: Number.isFinite(liqPrice) ? toFixed(liqPrice, 6) : null,
      liquidationDistancePct: liqDistancePct === null ? null : toFixed(liqDistancePct, 2),
      effectiveLeverage: toFixed(effectiveLev, 2),
      riskTag,
    };
  });

  const sorted = [...rows].sort((a, b) => toNum(b.notionalUsd, 0) - toNum(a.notionalUsd, 0));
  const totalNotional = sorted.reduce((sum, row) => sum + toNum(row.notionalUsd, 0), 0);

  const withExposure = sorted.map((row) => ({
    ...row,
    exposurePct: totalNotional > 0 ? toFixed((toNum(row.notionalUsd, 0) / totalNotional) * 100, 2) : "0.00",
  }));

  const closestLiq = withExposure
    .map((row) => toNum(row.liquidationDistancePct, NaN))
    .filter((value) => Number.isFinite(value))
    .sort((a, b) => a - b)[0];

  return {
    rows: withExposure,
    totalNotionalUsd: toFixed(totalNotional, 2),
    totalUnrealizedPnlUsd: metrics.totalUnrealizedPnlUsd || "0.00",
    closestLiquidationDistancePct: Number.isFinite(closestLiq) ? toFixed(closestLiq, 2) : null,
  };
}

function buildAllocations(positionView = { rows: [] }) {
  const rows = normalizeArray(positionView.rows);
  const total = rows.reduce((sum, row) => sum + toNum(row.notionalUsd, 0), 0);

  const bySymbolMap = new Map();
  const bySideMap = new Map();

  rows.forEach((row) => {
    const symbol = row.symbol;
    const side = row.side;
    const notional = toNum(row.notionalUsd, 0);

    bySymbolMap.set(symbol, (bySymbolMap.get(symbol) || 0) + notional);
    bySideMap.set(side, (bySideMap.get(side) || 0) + notional);
  });

  const bySymbol = Array.from(bySymbolMap.entries())
    .map(([symbol, notional]) => ({
      symbol,
      notionalUsd: toFixed(notional, 2),
      sharePct: total > 0 ? toFixed((notional / total) * 100, 2) : "0.00",
    }))
    .sort((a, b) => toNum(b.notionalUsd, 0) - toNum(a.notionalUsd, 0));

  const bySide = Array.from(bySideMap.entries())
    .map(([side, notional]) => ({
      side,
      notionalUsd: toFixed(notional, 2),
      sharePct: total > 0 ? toFixed((notional / total) * 100, 2) : "0.00",
    }))
    .sort((a, b) => toNum(b.notionalUsd, 0) - toNum(a.notionalUsd, 0));

  const top1 = bySymbol[0] ? toNum(bySymbol[0].sharePct, 0) : 0;
  const top3 = bySymbol.slice(0, 3).reduce((sum, row) => sum + toNum(row.sharePct, 0), 0);
  const hhi = bySymbol.reduce((sum, row) => {
    const share = toNum(row.sharePct, 0) / 100;
    return sum + share * share;
  }, 0);

  return {
    totalNotionalUsd: toFixed(total, 2),
    bySymbol,
    bySide,
    concentration: {
      top1SharePct: toFixed(top1, 2),
      top3SharePct: toFixed(top3, 2),
      hhi: toFixed(hhi, 4),
    },
  };
}

function computeEstimatedFundingImpact(positionRows = [], pricesBySymbol = {}) {
  let total = 0;
  const bySymbol = [];

  positionRows.forEach((row) => {
    const symbol = row.symbol;
    const price = pricesBySymbol[symbol] || {};
    const nextFunding = toNum(price.nextFunding, 0);
    const notional = toNum(row.notionalUsd, 0);
    const sign = row.side === "long" ? -1 : 1;
    const est = sign * nextFunding * notional;

    bySymbol.push({
      symbol,
      nextFundingRate: toFixed(nextFunding, 8),
      estimatedImpactUsd: toFixed(est, 4),
    });

    total += est;
  });

  return {
    totalEstimatedNextFundingUsd: toFixed(total, 4),
    bySymbol,
  };
}

function buildPerformance(state = {}, metrics = {}, positionView = { rows: [] }) {
  const account = state.account || {};
  const trades = normalizeArray(account.tradeHistory);
  const portfolio = normalizeArray(account.portfolioHistory);

  const realizedPnl = toNum(metrics.realizedPnlUsd, 0);
  const unrealizedPnl = toNum(metrics.totalUnrealizedPnlUsd, 0);

  let wins = 0;
  let losses = 0;
  let totalNotional = 0;

  const symbolPnL = new Map();
  const volumeByDay = new Map();

  trades.forEach((trade) => {
    const pnl = toNum(trade.pnl, 0);
    if (pnl > 0) wins += 1;
    else if (pnl < 0) losses += 1;

    const symbol = String(trade.symbol || "").toUpperCase();
    if (symbol) {
      symbolPnL.set(symbol, (symbolPnL.get(symbol) || 0) + pnl);
    }

    const notional = Math.abs(toNum(trade.amount, 0) * toNum(trade.price, 0));
    totalNotional += notional;

    const day = new Date(Number(trade.timestamp || 0)).toISOString().slice(0, 10);
    if (day && day !== "1970-01-01") {
      volumeByDay.set(day, (volumeByDay.get(day) || 0) + notional);
    }
  });

  const volumeSeries = Array.from(volumeByDay.entries())
    .map(([day, volume]) => ({ day, volumeUsd: toFixed(volume, 2) }))
    .sort((a, b) => (a.day < b.day ? -1 : 1));

  const symbolPerformance = Array.from(symbolPnL.entries())
    .map(([symbol, pnl]) => ({ symbol, realizedPnlUsd: toFixed(pnl, 2) }))
    .sort((a, b) => toNum(b.realizedPnlUsd, 0) - toNum(a.realizedPnlUsd, 0));

  const bestSymbols = symbolPerformance.slice(0, 5);
  const worstSymbols = [...symbolPerformance]
    .sort((a, b) => toNum(a.realizedPnlUsd, 0) - toNum(b.realizedPnlUsd, 0))
    .slice(0, 5);

  const avgHoldMins = computeApproxAvgHoldMinutes(trades);

  const portfolioCurve = portfolio.map((row) => ({
    timestamp: row.timestamp,
    equity: toFixed(toNum(row.accountEquity, 0), 2),
    pnl: toFixed(toNum(row.pnl, 0), 2),
  }));

  return {
    realizedPnlUsd: toFixed(realizedPnl, 2),
    unrealizedPnlUsd: toFixed(unrealizedPnl, 2),
    totalPnlUsd: toFixed(realizedPnl + unrealizedPnl, 2),
    tradeCount: trades.length,
    winCount: wins,
    lossCount: losses,
    winRatePct: toFixed(wins + losses > 0 ? (wins / (wins + losses)) * 100 : 0, 2),
    avgHoldMinutes: toFixed(avgHoldMins, 2),
    totalVolumeUsd: toFixed(totalNotional, 2),
    volumeSeries,
    portfolioCurve,
    bestSymbols,
    worstSymbols,
    openPositionCount: normalizeArray(positionView.rows).length,
  };
}

function computeApproxAvgHoldMinutes(trades = []) {
  const sorted = [...normalizeArray(trades)].sort((a, b) => Number(a.timestamp || 0) - Number(b.timestamp || 0));
  const queues = new Map();
  let weightedMs = 0;
  let matchedQty = 0;

  function getQueue(key) {
    if (!queues.has(key)) queues.set(key, []);
    return queues.get(key);
  }

  function enqueue(key, qty, timestamp) {
    getQueue(key).push({ qty, timestamp });
  }

  function consume(key, qty, timestamp) {
    let remain = qty;
    const queue = getQueue(key);
    while (remain > 0 && queue.length) {
      const head = queue[0];
      const take = Math.min(remain, head.qty);
      weightedMs += Math.max(0, timestamp - head.timestamp) * take;
      matchedQty += take;
      head.qty -= take;
      remain -= take;
      if (head.qty <= 1e-12) queue.shift();
    }
  }

  sorted.forEach((trade) => {
    const side = String(trade.side || "").toLowerCase();
    const symbol = String(trade.symbol || "").toUpperCase();
    const qty = Math.abs(toNum(trade.amount, 0));
    const timestamp = Number(trade.timestamp || 0);
    if (!symbol || qty <= 0 || timestamp <= 0) return;

    if (side === "open_long") enqueue(`${symbol}:long`, qty, timestamp);
    else if (side === "close_long") consume(`${symbol}:long`, qty, timestamp);
    else if (side === "open_short") enqueue(`${symbol}:short`, qty, timestamp);
    else if (side === "close_short") consume(`${symbol}:short`, qty, timestamp);
  });

  if (matchedQty <= 0) return 0;
  return weightedMs / matchedQty / 60000;
}

function buildRisk(overview = {}, metrics = {}, positionView = { closestLiquidationDistancePct: null }) {
  const marginUsage = toNum(metrics.marginUsagePct, 0);
  const effectiveLev = toNum(metrics.effectiveLeverage, 0);
  const liqDistance = toNum(positionView.closestLiquidationDistancePct, NaN);

  let liqPenalty = 0;
  if (Number.isFinite(liqDistance)) {
    liqPenalty = liqDistance >= 20 ? 0 : (20 - liqDistance) * 2;
  }

  const score = clamp(marginUsage * 0.65 + effectiveLev * 5.5 + liqPenalty, 0, 100);

  let level = "low";
  if (score >= 75) level = "critical";
  else if (score >= 55) level = "high";
  else if (score >= 35) level = "medium";

  return {
    score: toFixed(score, 2),
    level,
    marginUsagePct: toFixed(marginUsage, 2),
    effectiveLeverage: toFixed(effectiveLev, 2),
    closestLiquidationDistancePct: Number.isFinite(liqDistance) ? toFixed(liqDistance, 2) : null,
    accountEquityUsd: toFixed(toNum(overview.accountEquity, 0), 2),
    totalMarginUsedUsd: toFixed(toNum(overview.totalMarginUsed, 0), 2),
  };
}

function buildTimeline(state = {}) {
  const account = state.account || {};
  const events = [];

  normalizeArray(account.tradeHistory).forEach((row) => {
    events.push({
      kind: "trade",
      symbol: row.symbol,
      side: row.side,
      event: row.eventType,
      amount: row.amount,
      price: row.price,
      pnl: row.pnl,
      fee: row.fee,
      cause: row.cause,
      timestamp: Number(row.timestamp || 0),
      li: row.li || null,
      key: `trade:${row.li || row.historyId || row.timestamp}:${row.symbol}`,
    });
  });

  normalizeArray(account.recentOrderUpdates).forEach((row) => {
    events.push({
      kind: "order_update",
      symbol: row.symbol,
      side: row.side,
      event: row.orderEvent,
      status: row.orderStatus,
      amount: row.amount,
      price: row.initialPrice,
      timestamp: Number(row.updatedAt || row.createdAt || 0),
      li: row.li || null,
      key: `order_update:${row.li || `${row.orderId}:${row.updatedAt}`}`,
    });
  });

  normalizeArray(account.orderHistory).forEach((row) => {
    events.push({
      kind: "order_history",
      symbol: row.symbol,
      side: row.side,
      event: row.eventType || row.orderStatus,
      status: row.orderStatus,
      amount: row.initialAmount,
      price: row.price,
      timestamp: Number(row.updatedAt || row.createdAt || 0),
      li: row.li || null,
      key: `order_history:${row.li || `${row.orderId}:${row.updatedAt}`}`,
    });
  });

  normalizeArray(account.fundingHistory).forEach((row) => {
    events.push({
      kind: "funding",
      symbol: row.symbol,
      side: row.side,
      event: "funding_payment",
      amount: row.amount,
      payout: row.payout,
      rate: row.rate,
      timestamp: Number(row.createdAt || 0),
      li: null,
      key: `funding:${row.historyId || row.createdAt}:${row.symbol}`,
    });
  });

  normalizeArray(account.balanceHistory).forEach((row) => {
    events.push({
      kind: "balance",
      symbol: null,
      side: null,
      event: row.eventType,
      amount: row.amount,
      balance: row.balance,
      pendingBalance: row.pendingBalance,
      timestamp: Number(row.createdAt || 0),
      li: null,
      key: `balance:${row.createdAt}:${row.eventType}:${row.amount}`,
    });
  });

  const deduped = Array.from(new Map(events.map((row) => [row.key, row])).values());

  deduped.sort((a, b) => {
    const aHasLi = Number.isFinite(toNum(a.li, NaN));
    const bHasLi = Number.isFinite(toNum(b.li, NaN));

    if (aHasLi && bHasLi) {
      const diff = toNum(b.li, 0) - toNum(a.li, 0);
      if (diff !== 0) return diff;
    }

    if (aHasLi !== bHasLi) return aHasLi ? -1 : 1;

    const tDiff = Number(b.timestamp || 0) - Number(a.timestamp || 0);
    if (tDiff !== 0) return tDiff;

    return a.key < b.key ? 1 : -1;
  });

  return deduped.slice(0, 1500);
}

function buildMarketViews(state = {}) {
  const market = state.market || {};
  const prices = normalizeArray(Object.values(market.pricesBySymbol || {}));

  const rows = prices
    .map((row) => {
      const mark = toNum(row.mark, 0);
      const yday = toNum(row.yesterdayPrice, 0);
      const changePct = yday > 0 ? ((mark - yday) / yday) * 100 : 0;

      return {
        ...row,
        change24hPct: toFixed(changePct, 2),
        mark: toFixed(mark, 6),
        mid: toFixed(toNum(row.mid, mark), 6),
      };
    })
    .sort((a, b) => toNum(b.volume24h, 0) - toNum(a.volume24h, 0));

  const topFunding = [...rows]
    .sort((a, b) => Math.abs(toNum(b.funding, 0)) - Math.abs(toNum(a.funding, 0)))
    .slice(0, 10);

  const topOpenInterest = [...rows]
    .sort((a, b) => toNum(b.openInterest, 0) - toNum(a.openInterest, 0))
    .slice(0, 10);

  return {
    prices: rows,
    topFunding,
    topOpenInterest,
    bboBySymbol: market.bboBySymbol || {},
    orderbooksBySymbol: market.orderbooksBySymbol || {},
    publicTradesBySymbol: market.publicTradesBySymbol || {},
    candlesBySymbol: market.candlesBySymbol || {},
    markCandlesBySymbol: market.markCandlesBySymbol || {},
    infoBySymbol: market.infoBySymbol || {},
    fundingBySymbol: market.fundingBySymbol || {},
  };
}

function buildDashboardPayload({ state, metrics, account, transport, service }) {
  const safeState = state || {};
  const safeMetrics = metrics || {};
  const accountState = safeState.account || {};

  const market = buildMarketViews(safeState);
  const positions = buildPositionViews(safeState, safeMetrics);
  const allocations = buildAllocations(positions);
  const performance = buildPerformance(safeState, safeMetrics, positions);
  const fundingImpact = computeEstimatedFundingImpact(positions.rows, safeState.market && safeState.market.pricesBySymbol ? safeState.market.pricesBySymbol : {});
  const risk = buildRisk(accountState.overview || {}, safeMetrics, positions);
  const timeline = buildTimeline(safeState);

  return {
    generatedAt: Date.now(),
    environment: {
      service,
      account: account || null,
      apiBase: transport && transport.apiBase ? transport.apiBase : null,
      wsUrl: transport && transport.wsUrl ? transport.wsUrl : null,
      wsStatus: transport && transport.wsStatus ? transport.wsStatus : (safeState.sync && safeState.sync.wsStatus) || "idle",
    },
    sync: safeState.sync || {},
    metrics: safeMetrics,
    account: {
      overview: accountState.overview || null,
      settingsBySymbol: accountState.settingsBySymbol || {},
      marginModeBySymbol: accountState.marginModeBySymbol || {},
      leverageBySymbol: accountState.leverageBySymbol || {},
    },
    market,
    positions,
    orders: {
      open: normalizeArray(accountState.openOrders),
      history: normalizeArray(accountState.orderHistory),
      updates: normalizeArray(accountState.recentOrderUpdates),
    },
    trades: {
      recent: normalizeArray(accountState.recentTrades),
      history: normalizeArray(accountState.tradeHistory),
    },
    funding: {
      history: normalizeArray(accountState.fundingHistory),
      ratesBySymbol: safeState.market && safeState.market.fundingBySymbol ? safeState.market.fundingBySymbol : {},
      estimatedImpact: fundingImpact,
    },
    balance: {
      history: normalizeArray(accountState.balanceHistory),
      portfolioCurve: normalizeArray(accountState.portfolioHistory),
    },
    allocations,
    performance,
    risk,
    timeline,
  };
}

function buildOrdersHistoryPayload({ state, query }) {
  const rows = state && state.account ? state.account.orderHistory : [];
  const result = queryRows(rows, query, {
    sortKey: (row) => Number(row.updatedAt || row.createdAt || 0),
  });
  return {
    generatedAt: Date.now(),
    ...result,
  };
}

function buildTradesHistoryPayload({ state, query }) {
  const rows = state && state.account ? state.account.tradeHistory : [];
  const result = queryRows(rows, query, {
    sortKey: (row) => Number(row.timestamp || 0),
  });
  return {
    generatedAt: Date.now(),
    ...result,
  };
}

function buildFundingHistoryPayload({ state, query }) {
  const rows = state && state.account ? state.account.fundingHistory : [];
  const result = queryRows(rows, query, {
    sortKey: (row) => Number(row.createdAt || 0),
  });
  return {
    generatedAt: Date.now(),
    ...result,
  };
}

function buildTimelinePayload({ state, query }) {
  const rows = buildTimeline(state || {});
  const result = queryRows(rows, query, {
    sortKey: (row) => Number(row.timestamp || 0),
  });
  return {
    generatedAt: Date.now(),
    ...result,
  };
}

function normalizeTimeframe(value) {
  const raw = String(value || "all").toLowerCase();
  if (raw === "24h") return "24h";
  if (raw === "30d") return "30d";
  return "all";
}

function pickBucket(record, timeframe) {
  const safeRecord = record || {};
  if (timeframe === "24h") return safeRecord.d24 || null;
  if (timeframe === "30d") return safeRecord.d30 || null;
  return safeRecord.all || null;
}

function formatCompact(value) {
  const num = toNum(value, 0);
  const abs = Math.abs(num);
  if (abs >= 1_000_000_000_000) return `${toFixed(num / 1_000_000_000_000, 2)}T`;
  if (abs >= 1_000_000_000) return `${toFixed(num / 1_000_000_000, 2)}B`;
  if (abs >= 1_000_000) return `${toFixed(num / 1_000_000, 2)}M`;
  if (abs >= 1_000) return `${toFixed(num / 1_000, 2)}K`;
  return toFixed(num, 2);
}

function aggregateWallets(walletRecords = [], timeframe = "all") {
  const safe = normalizeArray(walletRecords);
  let totalTrades = 0;
  let totalVolumeUsd = 0;
  let totalFeesUsd = 0;
  let totalRevenueUsd = 0;
  let totalFeeRebatesUsd = 0;
  let totalNetFeesUsd = 0;
  let totalLiquidityPoolFeesUsd = 0;

  safe.forEach((record) => {
    const bucket = pickBucket(record, timeframe);
    if (!bucket) return;
    totalTrades += toNum(bucket.trades, 0);
    totalVolumeUsd += toNum(bucket.volumeUsd, 0);
    const feesPaid = toNum(
      bucket.feesPaidUsd !== undefined ? bucket.feesPaidUsd : bucket.feesUsd,
      0
    );
    const feeRebates = toNum(bucket.feeRebatesUsd, 0);
    const netFees = toNum(
      bucket.netFeesUsd !== undefined ? bucket.netFeesUsd : feesPaid - feeRebates,
      0
    );
    const liquidityPoolFees = toNum(bucket.liquidityPoolFeesUsd, 0);
    totalFeesUsd += feesPaid + liquidityPoolFees;
    totalRevenueUsd += toNum(bucket.revenueUsd, 0);
    totalFeeRebatesUsd += feeRebates;
    totalNetFeesUsd += netFees;
    totalLiquidityPoolFeesUsd += liquidityPoolFees;
  });

  return {
    totalAccounts: safe.length,
    totalTrades,
    totalVolumeUsd,
    totalFeesUsd,
    totalRevenueUsd,
    totalFeeRebatesUsd,
    totalNetFeesUsd,
    totalLiquidityPoolFeesUsd,
  };
}

function buildWalletSymbolRank(walletRecords = [], timeframe = "all") {
  const volumes = new Map();
  normalizeArray(walletRecords).forEach((record) => {
    const bucket = pickBucket(record, timeframe);
    const symbolVolumes = bucket && bucket.symbolVolumes ? bucket.symbolVolumes : {};
    Object.entries(symbolVolumes).forEach(([symbolRaw, volume]) => {
      const symbol = String(symbolRaw || "").toUpperCase();
      if (!symbol) return;
      volumes.set(symbol, (volumes.get(symbol) || 0) + toNum(volume, 0));
    });
  });

  return Array.from(volumes.entries())
    .map(([symbol, volumeUsd]) => ({
      symbol,
      market: `${symbol}USD-PERP`,
      volumeUsd,
    }))
    .sort((a, b) => b.volumeUsd - a.volumeUsd);
}

function buildMarketSymbolRank(state = {}) {
  const prices = normalizeArray(
    Object.values((state && state.market && state.market.pricesBySymbol) || {})
  );
  return prices
    .map((row) => {
      const symbol = String(row.symbol || "").toUpperCase();
      return {
        symbol,
        market: `${symbol}USD-PERP`,
        volumeUsd: toNum(row.volume24h, 0) / 2,
      };
    })
    .filter((row) => row.symbol)
    .sort((a, b) => b.volumeUsd - a.volumeUsd);
}

// Pacifica exchange-wide rolling 24h volume should come from /info/prices as:
// sum(volume_24h) / 2 across all symbols because Pacifica reports both sides.
function buildMarketDailyStatsFromPrices(state = {}) {
  const prices = normalizeArray(
    Object.values((state && state.market && state.market.pricesBySymbol) || {})
  );

  return prices.reduce(
    (acc, row) => {
      const mark = toNum(row.mark, 0);
      const openInterest = toNum(row.openInterest, 0);
      const volume24h = toNum(row.volume24h, 0) / 2;

      acc.dailyVolume += volume24h;
      acc.openInterestAtEnd += openInterest * mark;
      return acc;
    },
    {
      dailyVolume: 0,
      openInterestAtEnd: 0,
    }
  );
}

function buildExchangeOverviewPayload({ state, transport, wallets, timeframe }) {
  const tf = normalizeTimeframe(timeframe);
  const walletRows = normalizeArray(wallets);
  const aggregated = aggregateWallets(walletRows, tf);
  const walletRank = buildWalletSymbolRank(walletRows, tf);
  const marketRank = buildMarketSymbolRank(state);
  const marketDailyStats = buildMarketDailyStatsFromPrices(state);

  let totalVolumeUsd = aggregated.totalVolumeUsd;
  if (tf === "24h") {
    totalVolumeUsd = marketDailyStats.dailyVolume;
  }

  let symbolRank = walletRank;
  if (!symbolRank.length || tf === "24h") symbolRank = marketRank;

  const ranked = symbolRank.slice(0, 100).map((row, idx) => ({
    rank: idx + 1,
    symbol: row.symbol,
    market: row.market,
    volumeUsd: toFixed(row.volumeUsd, 2),
    volumeCompact: formatCompact(row.volumeUsd),
    live: true,
  }));

  return {
    generatedAt: Date.now(),
    timeframe: tf,
    sync: {
      wsStatus: transport && transport.wsStatus ? transport.wsStatus : "idle",
      lastBootstrapAt: state && state.sync ? state.sync.lastBootstrapAt : null,
      lastEventId: state && state.sync ? state.sync.lastEventId : null,
    },
    kpis: {
      totalRevenueUsd: toFixed(aggregated.totalRevenueUsd, 2),
      totalAccounts: aggregated.totalAccounts,
      totalTrades: aggregated.totalTrades,
      totalVolumeUsd: toFixed(totalVolumeUsd, 2),
      openInterestAtEnd: toFixed(marketDailyStats.openInterestAtEnd, 2),
      totalFeesUsd: toFixed(aggregated.totalFeesUsd, 2),
      totalTradingFeesUsd: toFixed(
        Math.max(0, aggregated.totalFeesUsd - aggregated.totalLiquidityPoolFeesUsd),
        2
      ),
      totalLiquidityPoolFeesUsd: toFixed(aggregated.totalLiquidityPoolFeesUsd, 2),
      totalFeeRebatesUsd: toFixed(aggregated.totalFeeRebatesUsd, 2),
      totalNetFeesUsd: toFixed(aggregated.totalNetFeesUsd, 2),
      totalRevenueCompact: formatCompact(aggregated.totalRevenueUsd),
      totalVolumeCompact: formatCompact(totalVolumeUsd),
      totalFeesCompact: formatCompact(aggregated.totalFeesUsd),
    },
    volumeRank: ranked,
    source: {
      scope: "wallet_indexer",
      walletsIndexed: walletRows.length,
      marketFallbackUsed: !walletRank.length || tf === "24h",
      dailyVolumeSource: "/api/v1/info/prices:sum(volume_24h)/2",
      totalFeesSource:
        "wallet_indexer:sum(max(trade.fee,0))+sum(liquidity_pool_fee, if_available)",
      totalTradingFeesSource: "wallet_indexer:sum(max(trade.fee,0))",
      totalLiquidityPoolFeesSource:
        "wallet_indexer:sum(liquidity_pool_fee) (not currently available in Pacifica REST payloads)",
      totalRevenueSource: "wallet_indexer:sum(max(trade.fee,0))",
      totalFeeRebatesSource: "wallet_indexer:sum(abs(min(trade.fee,0)))",
      totalNetFeesSource: "wallet_indexer:sum(trade.fee)",
      liquidityPoolFeesIncluded: aggregated.totalLiquidityPoolFeesUsd > 0,
    },
  };
}

function buildWalletExplorerPayload({ wallets, query }) {
  const summarizeBucket = (bucket = {}) => {
    const safeBucket = bucket && typeof bucket === "object" ? bucket : {};
    return {
      trades: Number(safeBucket.trades || 0),
      volumeUsd: toNum(safeBucket.volumeUsd, 0),
      pnlUsd: toNum(safeBucket.pnlUsd, 0),
      winRate: toNum(safeBucket.winRatePct, 0),
      firstTrade: safeBucket.firstTrade || null,
      lastTrade: safeBucket.lastTrade || null,
      feesUsd: toNum(
        safeBucket.feesPaidUsd !== undefined ? safeBucket.feesPaidUsd : safeBucket.feesUsd,
        0
      ),
      fundingPayoutUsd: toNum(safeBucket.fundingPayoutUsd, 0),
      symbolVolumes:
        safeBucket.symbolVolumes && typeof safeBucket.symbolVolumes === "object"
          ? safeBucket.symbolVolumes
          : {},
    };
  };
  const q = query || {};
  const timeframe = normalizeTimeframe(q.timeframe);
  const search = String(q.q || "").trim().toLowerCase();
  const page = Math.max(1, Number(q.page || 1));
  const pageSize = Math.max(1, Math.min(100, Number(q.pageSize || 20)));
  const requestedSort = String(q.sort || "volumeUsd").trim();
  const sortKey = {
    trades: "trades",
    volume: "volumeUsd",
    volumeUsd: "volumeUsd",
    totalWins: "totalWins",
    totalLosses: "totalLosses",
    pnl: "pnlUsd",
    pnlUsd: "pnlUsd",
    winRate: "winRate",
    firstTrade: "firstTrade",
    lastTrade: "lastTrade",
  }[requestedSort] || "volumeUsd";
  const sortDir = String(q.dir || "desc").trim().toLowerCase() === "asc" ? "asc" : "desc";

  const compareWalletRows = (left, right) => {
    const numericKeys = new Set([
      "trades",
      "volumeUsd",
      "totalWins",
      "totalLosses",
      "pnlUsd",
      "winRate",
      "firstTrade",
      "lastTrade",
    ]);
    const leftRaw = left ? left[sortKey] : null;
    const rightRaw = right ? right[sortKey] : null;
    const leftMissing = leftRaw === null || leftRaw === undefined || leftRaw === "";
    const rightMissing = rightRaw === null || rightRaw === undefined || rightRaw === "";
    if (leftMissing && rightMissing) return String(left?.wallet || "").localeCompare(String(right?.wallet || ""));
    if (leftMissing) return 1;
    if (rightMissing) return -1;

    if (numericKeys.has(sortKey)) {
      const a = toNum(leftRaw, 0);
      const b = toNum(rightRaw, 0);
      if (a === b) return String(left?.wallet || "").localeCompare(String(right?.wallet || ""));
      return sortDir === "asc" ? a - b : b - a;
    }

    const a = String(leftRaw || "");
    const b = String(rightRaw || "");
    const cmp = a.localeCompare(b);
    if (cmp === 0) return String(left?.wallet || "").localeCompare(String(right?.wallet || ""));
    return sortDir === "asc" ? cmp : -cmp;
  };

  const rows = normalizeArray(wallets)
    .map((record) => {
      const bucket = pickBucket(record, timeframe) || {};
      const d24 = summarizeBucket(record && record.d24);
      const d7 = summarizeBucket(record && record.d7);
      const d30 = summarizeBucket(record && record.d30);
      const all = summarizeBucket(record && record.all);
      const fallbackRecord = record && record.wallet ? loadBestLocalWalletRecord(record.wallet) : null;
      const lastOpenedAt = resolveWalletLastOpenedAt(
        {
          summary: {
            lastOpenedAt:
              Number(bucket.lastOpenedAt || 0) ||
              Number(record && record.lastOpenedAt ? record.lastOpenedAt : 0) ||
              Number(d24.lastOpenedAt || 0) ||
              Number(d7.lastOpenedAt || 0) ||
              Number(d30.lastOpenedAt || 0) ||
              Number(all.lastOpenedAt || 0) ||
              null,
            lastOpenedPosition:
              bucket.lastOpenedPosition ||
              (record && record.lastOpenedPosition) ||
              null,
          },
          lastOpenedAt:
            Number(bucket.lastOpenedAt || 0) ||
            Number(record && record.lastOpenedAt ? record.lastOpenedAt : 0) ||
            null,
          lastOpenedPosition: bucket.lastOpenedPosition || (record && record.lastOpenedPosition) || null,
          positions: Array.isArray(record && record.positions) ? record.positions : [],
        },
        fallbackRecord
      );
      return {
        wallet: record.wallet,
        trades: Number(bucket.trades || 0),
        volumeUsd: toNum(bucket.volumeUsd, 0),
        totalWins: Number(bucket.wins || 0),
        totalLosses: Number(bucket.losses || 0),
        pnlUsd: toNum(bucket.pnlUsd, 0),
        winRate: toNum(bucket.winRatePct, 0),
        firstTrade: bucket.firstTrade || null,
        lastTrade: bucket.lastTrade || null,
        updatedAt: record.updatedAt || null,
        lastOpenedAt: lastOpenedAt || null,
        lastActivity: lastOpenedAt || null,
        d24,
        d7,
        d30,
        all,
      };
    })
    .filter((row) => !search || String(row.wallet || "").toLowerCase().includes(search))
    .sort(compareWalletRows)
    .map((row, idx) => ({
      ...row,
      rank: idx + 1,
      volumeCompact: formatCompact(row.volumeUsd),
      pnlCompact: formatCompact(row.pnlUsd),
    }));

  const total = rows.length;
  const pages = Math.max(1, Math.ceil(total / pageSize));
  const start = (page - 1) * pageSize;
  const sliced = rows.slice(start, start + pageSize).map((row) => ({
    ...row,
    volumeUsd: toFixed(row.volumeUsd, 2),
    pnlUsd: toFixed(row.pnlUsd, 2),
    winRate: toFixed(row.winRate, 2),
    d24: {
      ...row.d24,
      volumeUsd: toFixed(row.d24.volumeUsd, 2),
      pnlUsd: toFixed(row.d24.pnlUsd, 2),
      winRate: toFixed(row.d24.winRate, 2),
      feesUsd: toFixed(row.d24.feesUsd, 2),
      fundingPayoutUsd: toFixed(row.d24.fundingPayoutUsd, 2),
    },
    d7: {
      ...row.d7,
      volumeUsd: toFixed(row.d7.volumeUsd, 2),
      pnlUsd: toFixed(row.d7.pnlUsd, 2),
      winRate: toFixed(row.d7.winRate, 2),
      feesUsd: toFixed(row.d7.feesUsd, 2),
      fundingPayoutUsd: toFixed(row.d7.fundingPayoutUsd, 2),
    },
    d30: {
      ...row.d30,
      volumeUsd: toFixed(row.d30.volumeUsd, 2),
      pnlUsd: toFixed(row.d30.pnlUsd, 2),
      winRate: toFixed(row.d30.winRate, 2),
      feesUsd: toFixed(row.d30.feesUsd, 2),
      fundingPayoutUsd: toFixed(row.d30.fundingPayoutUsd, 2),
    },
    all: {
      ...row.all,
      volumeUsd: toFixed(row.all.volumeUsd, 2),
      pnlUsd: toFixed(row.all.pnlUsd, 2),
      winRate: toFixed(row.all.winRate, 2),
      feesUsd: toFixed(row.all.feesUsd, 2),
      fundingPayoutUsd: toFixed(row.all.fundingPayoutUsd, 2),
    },
  }));

  return {
    generatedAt: Date.now(),
    timeframe,
    query: {
      q: search,
      page,
      pageSize,
      sort: sortKey,
      dir: sortDir,
    },
    total,
    page,
    pageSize,
    pages,
    rows: sliced,
  };
}

function buildWalletProfilePayload({ wallets, wallet, timeframe }) {
  const tf = normalizeTimeframe(timeframe);
  const rows = normalizeArray(wallets);
  const sorted = [...rows].sort((a, b) => {
    const aVolume = toNum((pickBucket(a, tf) || {}).volumeUsd, 0);
    const bVolume = toNum((pickBucket(b, tf) || {}).volumeUsd, 0);
    return bVolume - aVolume;
  });

  const normalizedWallet = String(wallet || "").trim();
  const record = sorted.find((row) => String(row.wallet || "").trim() === normalizedWallet) || null;
  const bucket = pickBucket(record, tf) || null;
  const rank = record
    ? sorted.findIndex((row) => String(row.wallet || "").trim() === normalizedWallet) + 1
    : null;

  const symbolVolumes = bucket && bucket.symbolVolumes ? bucket.symbolVolumes : {};
  const symbolBreakdown = Object.entries(symbolVolumes)
    .map(([symbol, volumeUsd]) => ({
      symbol: String(symbol || "").toUpperCase(),
      volumeUsd: toFixed(toNum(volumeUsd, 0), 2),
      volumeCompact: formatCompact(volumeUsd),
    }))
    .sort((a, b) => toNum(b.volumeUsd, 0) - toNum(a.volumeUsd, 0))
    .slice(0, 25);

  return {
    generatedAt: Date.now(),
    timeframe: tf,
    wallet: normalizedWallet || null,
    found: Boolean(record),
    summary: record
      ? {
          rank,
          trades: Number(bucket.trades || 0),
          volumeUsd: toFixed(toNum(bucket.volumeUsd, 0), 2),
          totalWins: Number(bucket.wins || 0),
          totalLosses: Number(bucket.losses || 0),
          pnlUsd: toFixed(toNum(bucket.pnlUsd, 0), 2),
          drawdownPct: toFixed(toNum(bucket.drawdownPct ?? record.drawdownPct, 0), 2),
          returnPct: toFixed(toNum(bucket.returnPct ?? record.returnPct, 0), 2),
          accountEquityUsd: toFixed(
            toNum(bucket.accountEquityUsd ?? record.accountEquityUsd, 0),
            2
          ),
          openPositions: Number(record.openPositions || 0),
          winRate: toFixed(toNum(bucket.winRatePct, 0), 2),
          firstTrade: bucket.firstTrade || null,
          lastTrade: bucket.lastTrade || null,
          lastOpenedAt:
            Number(bucket.lastOpenedAt || 0) ||
            Number(bucket.lastOpenedPosition && bucket.lastOpenedPosition.openedAt ? bucket.lastOpenedPosition.openedAt : 0) ||
            Number(record.lastOpenedAt || 0) ||
            Number(record.lastOpenedPosition && record.lastOpenedPosition.openedAt ? record.lastOpenedPosition.openedAt : 0) ||
            null,
          lastActivity:
            Number(bucket.lastOpenedAt || 0) ||
            Number(bucket.lastOpenedPosition && bucket.lastOpenedPosition.openedAt ? bucket.lastOpenedPosition.openedAt : 0) ||
            Number(record.lastOpenedAt || 0) ||
            Number(record.lastOpenedPosition && record.lastOpenedPosition.openedAt ? record.lastOpenedPosition.openedAt : 0) ||
            null,
          feesUsd: toFixed(toNum(bucket.feesUsd, 0), 2),
          feeRebatesUsd: toFixed(toNum(bucket.feeRebatesUsd, 0), 2),
          netFeesUsd: toFixed(toNum(bucket.netFeesUsd, 0), 2),
          revenueUsd: toFixed(toNum(bucket.revenueUsd, 0), 2),
          updatedAt: record.updatedAt || null,
        }
      : null,
    symbolBreakdown,
  };
}

module.exports = {
  buildDashboardPayload,
  buildExchangeOverviewPayload,
  buildFundingHistoryPayload,
  buildOrdersHistoryPayload,
  buildTimelinePayload,
  buildTradesHistoryPayload,
  buildWalletExplorerPayload,
  buildWalletProfilePayload,
};
