const path = require("path");
const { ensureDir, parseNumber, readJson, toFixedSafe, writeJsonAtomic } = require("./utils");

function emptyMetrics() {
  return {
    marketSymbols: 0,
    openPositions: 0,
    openOrders: 0,
    totalOpenNotionalUsd: "0.00",
    totalUnrealizedPnlUsd: "0.00",
    realizedPnlUsd: "0.00",
    fundingPayoutUsd: "0.00",
    marginUsagePct: "0.00",
    effectiveLeverage: "0.00",
    winRatePct: "0.00",
    updatedAt: null,
  };
}

function toNum(value, fallback = 0) {
  return parseNumber(value, fallback);
}

class MetricsService {
  constructor(options = {}) {
    this.dataDir = options.dataDir;
    this.filePath =
      options.filePath || path.join(this.dataDir || ".", "snapshots", "metrics.json");
    this.metrics = emptyMetrics();
  }

  load() {
    ensureDir(path.dirname(this.filePath));
    this.metrics = readJson(this.filePath, emptyMetrics()) || emptyMetrics();
    return this.metrics;
  }

  reset() {
    this.metrics = emptyMetrics();
  }

  getMetrics() {
    return this.metrics;
  }

  refresh(state = {}) {
    const market = state.market || {};
    const account = state.account || {};

    const pricesBySymbol = market.pricesBySymbol || {};
    const openPositions = Array.isArray(account.openPositions) ? account.openPositions : [];
    const openOrders = Array.isArray(account.openOrders) ? account.openOrders : [];
    const tradeHistory = Array.isArray(account.tradeHistory) ? account.tradeHistory : [];
    const fundingHistory = Array.isArray(account.fundingHistory) ? account.fundingHistory : [];

    let totalOpenNotionalUsd = 0;
    let totalUnrealizedPnlUsd = 0;

    openPositions.forEach((position) => {
      const amount = Math.abs(toNum(position.amount, 0));
      const entryPrice = toNum(position.entryPrice, 0);
      const mark = toNum(
        pricesBySymbol[position.symbol] ? pricesBySymbol[position.symbol].mark : entryPrice,
        entryPrice
      );

      const notional = amount * mark;
      totalOpenNotionalUsd += notional;

      const pnlDelta = (mark - entryPrice) * amount;
      const side = String(position.side || "").toLowerCase();
      const pnl = side === "ask" ? -pnlDelta : pnlDelta;
      totalUnrealizedPnlUsd += pnl;
    });

    let realizedPnlUsd = 0;
    let wins = 0;
    let losses = 0;

    tradeHistory.forEach((trade) => {
      const pnl = toNum(trade.pnl, 0);
      realizedPnlUsd += pnl;
      if (pnl > 0) wins += 1;
      else if (pnl < 0) losses += 1;
    });

    let fundingPayoutUsd = 0;
    fundingHistory.forEach((row) => {
      fundingPayoutUsd += toNum(row.payout, 0);
    });

    const equity = toNum(account.overview && account.overview.accountEquity, 0);
    const marginUsed = toNum(account.overview && account.overview.totalMarginUsed, 0);

    const marginUsagePct = equity > 0 ? (marginUsed / equity) * 100 : 0;
    const effectiveLeverage = equity > 0 ? totalOpenNotionalUsd / equity : 0;
    const winRatePct = wins + losses > 0 ? (wins / (wins + losses)) * 100 : 0;

    this.metrics = {
      marketSymbols: Object.keys(pricesBySymbol).length,
      openPositions: openPositions.length,
      openOrders: openOrders.length,
      totalOpenNotionalUsd: toFixedSafe(totalOpenNotionalUsd, 2),
      totalUnrealizedPnlUsd: toFixedSafe(totalUnrealizedPnlUsd, 2),
      realizedPnlUsd: toFixedSafe(realizedPnlUsd, 2),
      fundingPayoutUsd: toFixedSafe(fundingPayoutUsd, 2),
      marginUsagePct: toFixedSafe(marginUsagePct, 2),
      effectiveLeverage: toFixedSafe(effectiveLeverage, 2),
      winRatePct: toFixedSafe(winRatePct, 2),
      updatedAt: Date.now(),
    };

    return this.metrics;
  }

  save() {
    writeJsonAtomic(this.filePath, this.metrics);
  }
}

module.exports = {
  MetricsService,
  emptyMetrics,
};
