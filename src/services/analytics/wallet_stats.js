const DAY_MS = 24 * 60 * 60 * 1000;

function toNum(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function mapEntriesToObject(map) {
  return Array.from(map.entries()).reduce((acc, [key, value]) => {
    acc[key] = value;
    return acc;
  }, {});
}

function normalizeTradeLike(row = {}) {
  const timestamp = Number(
    row.timestamp !== undefined
      ? row.timestamp
      : row.created_at !== undefined
      ? row.created_at
      : row.createdAt !== undefined
      ? row.createdAt
      : 0
  );

  return {
    historyId:
      row.history_id !== undefined ? row.history_id : row.historyId !== undefined ? row.historyId : null,
    orderId: row.order_id !== undefined ? row.order_id : row.orderId !== undefined ? row.orderId : null,
    symbol: String(row.symbol || "").toUpperCase(),
    side: String(row.side || "").toLowerCase(),
    amount: row.amount !== undefined ? row.amount : "0",
    price: row.price !== undefined ? row.price : "0",
    entryPrice:
      row.entry_price !== undefined ? row.entry_price : row.entryPrice !== undefined ? row.entryPrice : "0",
    fee: row.fee !== undefined ? row.fee : "0",
    liquidityPoolFee:
      row.liquidity_pool_fee !== undefined
        ? row.liquidity_pool_fee
        : row.liquidityPoolFee !== undefined
        ? row.liquidityPoolFee
        : row.lp_fee !== undefined
        ? row.lp_fee
        : row.lpFee !== undefined
        ? row.lpFee
        : row.supply_side_fee !== undefined
        ? row.supply_side_fee
        : row.supplySideFee !== undefined
        ? row.supplySideFee
        : "0",
    pnl: row.pnl !== undefined ? row.pnl : "0",
    eventType:
      row.event_type !== undefined
        ? String(row.event_type || "").toLowerCase()
        : row.eventType !== undefined
        ? String(row.eventType || "").toLowerCase()
        : "",
    cause: String(row.cause || "").toLowerCase(),
    timestamp: Number.isFinite(timestamp) ? timestamp : 0,
  };
}

function classifyTradeExecution(row = {}) {
  const eventType = String(row.eventType || "").toLowerCase();
  const fee = toNum(row.fee, 0);
  if (eventType.includes("maker")) return "maker";
  if (eventType.includes("taker")) return "taker";
  if (fee > 0) return "taker";
  if (fee < 0) return "maker";
  return "maker";
}

function normalizeFundingLike(row = {}) {
  const createdAt = Number(
    row.createdAt !== undefined
      ? row.createdAt
      : row.created_at !== undefined
      ? row.created_at
      : 0
  );

  return {
    symbol: String(row.symbol || "").toUpperCase(),
    payout: row.payout !== undefined ? row.payout : "0",
    createdAt: Number.isFinite(createdAt) ? createdAt : 0,
  };
}

function aggregateBucket({ trades = [], funding = [], now, sinceTs = null }) {
  const filteredTrades = Array.isArray(trades)
    ? trades.filter((row) => {
        const ts = Number(row.timestamp || 0);
        return Number.isFinite(ts) && (sinceTs === null || ts >= sinceTs);
      })
    : [];

  const filteredFunding = Array.isArray(funding)
    ? funding.filter((row) => {
        const ts = Number(row.createdAt || 0);
        return Number.isFinite(ts) && (sinceTs === null || ts >= sinceTs);
      })
    : [];

  let volumeUsd = 0;
  let makerVolumeUsd = 0;
  let takerVolumeUsd = 0;
  let feeBearingVolumeUsd = 0;
  let feesPaidUsd = 0;
  let liquidityPoolFeesUsd = 0;
  let feeRebatesUsd = 0;
  let netFeesUsd = 0;
  let pnlUsd = 0;
  let makerTrades = 0;
  let takerTrades = 0;
  let wins = 0;
  let losses = 0;
  let firstTrade = null;
  let lastTrade = null;

  const symbolVolumes = new Map();

  filteredTrades.forEach((row) => {
    const ts = Number(row.timestamp || 0);
    const amount = Math.abs(toNum(row.amount, 0));
    const price = toNum(row.price, 0);
    const notional = amount * price;
    const feeSigned = toNum(row.fee, 0);
    const liquidityPoolFee = Math.max(0, toNum(row.liquidityPoolFee, 0));
    const pnl = toNum(row.pnl, 0);
    const executionKind = classifyTradeExecution(row);

    volumeUsd += notional;
    if (executionKind === "taker") {
      takerTrades += 1;
      takerVolumeUsd += notional;
    } else {
      makerTrades += 1;
      makerVolumeUsd += notional;
    }
    if (feeSigned > 0) feesPaidUsd += feeSigned;
    else if (feeSigned < 0) feeRebatesUsd += Math.abs(feeSigned);
    if (feeSigned > 0 || liquidityPoolFee > 0) feeBearingVolumeUsd += notional;
    liquidityPoolFeesUsd += liquidityPoolFee;
    netFeesUsd += feeSigned;
    pnlUsd += pnl;

    if (pnl > 0) wins += 1;
    else if (pnl < 0) losses += 1;

    if (Number.isFinite(ts) && ts > 0) {
      firstTrade = firstTrade === null ? ts : Math.min(firstTrade, ts);
      lastTrade = lastTrade === null ? ts : Math.max(lastTrade, ts);
    }

    const symbol = String(row.symbol || "").toUpperCase();
    if (symbol) {
      symbolVolumes.set(symbol, (symbolVolumes.get(symbol) || 0) + notional);
    }
  });

  let fundingPayoutUsd = 0;
  filteredFunding.forEach((row) => {
    fundingPayoutUsd += toNum(row.payout, 0);
  });

  const tradeCount = filteredTrades.length;
  const winRatePct = wins + losses > 0 ? (wins / (wins + losses)) * 100 : 0;

  return {
    computedAt: now,
    trades: tradeCount,
    volumeUsd,
    makerTrades,
    takerTrades,
    makerVolumeUsd,
    takerVolumeUsd,
    feeBearingVolumeUsd,
    makerSharePct: volumeUsd > 0 ? (makerVolumeUsd / volumeUsd) * 100 : 0,
    takerSharePct: volumeUsd > 0 ? (takerVolumeUsd / volumeUsd) * 100 : 0,
    // feesUsd remains "fees paid by wallet" for compatibility with existing consumers.
    feesUsd: feesPaidUsd,
    feesPaidUsd,
    liquidityPoolFeesUsd,
    feeRebatesUsd,
    netFeesUsd,
    effectiveFeeRateBps: volumeUsd > 0 ? ((feesPaidUsd + liquidityPoolFeesUsd) / volumeUsd) * 10000 : 0,
    feeBearingRateBps:
      feeBearingVolumeUsd > 0 ? ((feesPaidUsd + liquidityPoolFeesUsd) / feeBearingVolumeUsd) * 10000 : 0,
    fundingPayoutUsd,
    // Revenue tracks gross wallet-paid trading fees (platform-side net can be derived from netFeesUsd).
    revenueUsd: feesPaidUsd,
    pnlUsd,
    wins,
    losses,
    winRatePct,
    firstTrade,
    lastTrade,
    symbolVolumes: mapEntriesToObject(symbolVolumes),
  };
}

function buildWalletRecord({ wallet, trades = [], funding = [], computedAt = Date.now() }) {
  const normalizedTrades = Array.isArray(trades) ? trades.map(normalizeTradeLike) : [];
  const normalizedFunding = Array.isArray(funding) ? funding.map(normalizeFundingLike) : [];
  const now = Number(computedAt) || Date.now();

  const all = aggregateBucket({
    trades: normalizedTrades,
    funding: normalizedFunding,
    now,
    sinceTs: null,
  });

  const d30 = aggregateBucket({
    trades: normalizedTrades,
    funding: normalizedFunding,
    now,
    sinceTs: now - 30 * DAY_MS,
  });

  const d7 = aggregateBucket({
    trades: normalizedTrades,
    funding: normalizedFunding,
    now,
    sinceTs: now - 7 * DAY_MS,
  });

  const d24 = aggregateBucket({
    trades: normalizedTrades,
    funding: normalizedFunding,
    now,
    sinceTs: now - DAY_MS,
  });

  return {
    wallet: String(wallet || "").trim(),
    updatedAt: now,
    trades: all.trades,
    volumeUsd: all.volumeUsd,
    feesUsd: all.feesUsd,
    feesPaidUsd: all.feesPaidUsd,
    liquidityPoolFeesUsd: all.liquidityPoolFeesUsd,
    feeRebatesUsd: all.feeRebatesUsd,
    netFeesUsd: all.netFeesUsd,
    fundingPayoutUsd: all.fundingPayoutUsd,
    revenueUsd: all.revenueUsd,
    pnlUsd: all.pnlUsd,
    wins: all.wins,
    losses: all.losses,
    winRatePct: all.winRatePct,
    firstTrade: all.firstTrade,
    lastTrade: all.lastTrade,
    all,
    d30,
    d7,
    d24,
  };
}

function buildWalletRecordFromState({ wallet, state }) {
  const now = Date.now();
  const safeState = state || {};
  const account = safeState.account || {};

  const trades = Array.isArray(account.tradeHistory) ? account.tradeHistory : [];
  const funding = Array.isArray(account.fundingHistory) ? account.fundingHistory : [];

  return buildWalletRecord({
    wallet,
    trades,
    funding,
    computedAt: now,
  });
}

function buildWalletRecordFromHistory({ wallet, tradesHistory = [], fundingHistory = [], computedAt }) {
  return buildWalletRecord({
    wallet,
    trades: Array.isArray(tradesHistory) ? tradesHistory : [],
    funding: Array.isArray(fundingHistory) ? fundingHistory : [],
    computedAt,
  });
}

module.exports = {
  buildWalletRecord,
  buildWalletRecordFromHistory,
  buildWalletRecordFromState,
};
