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
    symbol: String(row.symbol || "").toUpperCase(),
    amount: row.amount !== undefined ? row.amount : "0",
    price: row.price !== undefined ? row.price : "0",
    fee: row.fee !== undefined ? row.fee : "0",
    pnl: row.pnl !== undefined ? row.pnl : "0",
    timestamp: Number.isFinite(timestamp) ? timestamp : 0,
  };
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
  let feesUsd = 0;
  let pnlUsd = 0;
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
    const fee = Math.abs(toNum(row.fee, 0));
    const pnl = toNum(row.pnl, 0);

    volumeUsd += notional;
    feesUsd += fee;
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
    feesUsd,
    fundingPayoutUsd,
    revenueUsd: feesUsd,
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

  const d24 = aggregateBucket({
    trades: normalizedTrades,
    funding: normalizedFunding,
    now,
    sinceTs: now - DAY_MS,
  });

  return {
    wallet: String(wallet || "").trim(),
    updatedAt: now,
    all,
    d30,
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
