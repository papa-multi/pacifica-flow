"use strict";

function asNumber(value, fallback = 0) {
  const out = Number(value);
  return Number.isFinite(out) ? out : fallback;
}

function upsertWallet(state, wallet) {
  if (!state.wallets[wallet]) {
    state.wallets[wallet] = {
      wallet,
      last_opened_at: 0,
      last_activity_at: 0,
      last_head_check_at: 0,
      verified_through_at: 0,
      first_deposit_at: 0,
      state_version: 0,
      review_stage: "bootstrap",
      open_positions: 0,
      open_positions_updated_at: 0,
      open_orders: 0,
      volume_usd: 0,
      trades: 0,
      realized_pnl_usd: 0,
      unrealized_pnl_usd: 0,
      fees_paid_usd: 0,
      total_wins: 0,
      total_losses: 0,
      win_rate: 0,
      first_trade_at: 0,
      last_trade_at: 0,
      symbol_breakdown: [],
    };
  }
  return state.wallets[wallet];
}

function applyMarketEvent(state, event) {
  const market = String(event.market || event.entity_key || "");
  if (!market) return;
  const payload = event.payload || {};
  const current = state.markets[market] || { market };
  state.markets[market] = {
    ...current,
    market,
    last_price: asNumber(payload.price, current.last_price || 0),
    bid: asNumber(payload.bid, current.bid || 0),
    ask: asNumber(payload.ask, current.ask || 0),
    last_trade_at: asNumber(event.event_time, current.last_trade_at || 0),
    updated_at: Date.now(),
  };
}

function applyWalletSnapshot(state, event) {
  const wallet = String(event.wallet || event.entity_key || "");
  if (!wallet) return;
  const payload = event.payload || {};
  const row = upsertWallet(state, wallet);
  row.last_opened_at = asNumber(
    payload.last_opened_at,
    row.last_opened_at || asNumber(payload.last_activity_at, 0)
  );
  row.last_activity_at = row.last_opened_at;
  row.first_deposit_at = asNumber(payload.first_deposit_at, row.first_deposit_at || 0);
  row.verified_through_at = asNumber(payload.verified_through_at, row.verified_through_at || 0);
  row.last_head_check_at = asNumber(payload.last_head_check_at, row.last_head_check_at || 0);
  row.review_stage = payload.review_stage || row.review_stage || "bootstrap";
  row.open_positions = asNumber(payload.open_positions, row.open_positions || 0);
  row.open_positions_updated_at = asNumber(payload.open_positions_updated_at, row.open_positions_updated_at || 0);
  row.open_orders = asNumber(payload.open_orders, row.open_orders || 0);
  row.volume_usd = asNumber(payload.volume_usd, row.volume_usd || 0);
  row.trades = asNumber(payload.trades, row.trades || 0);
  row.realized_pnl_usd = asNumber(payload.realized_pnl_usd, row.realized_pnl_usd || 0);
  row.unrealized_pnl_usd = asNumber(payload.unrealized_pnl_usd, row.unrealized_pnl_usd || 0);
  row.fees_paid_usd = asNumber(payload.fees_paid_usd, row.fees_paid_usd || 0);
  row.total_wins = asNumber(payload.total_wins, row.total_wins || 0);
  row.total_losses = asNumber(payload.total_losses, row.total_losses || 0);
  row.win_rate = asNumber(payload.win_rate, row.win_rate || 0);
  row.first_trade_at = asNumber(payload.first_trade_at, row.first_trade_at || 0);
  row.last_trade_at = asNumber(payload.last_trade_at, row.last_trade_at || 0);
  row.symbol_breakdown = Array.isArray(payload.symbol_breakdown)
    ? payload.symbol_breakdown
    : Array.isArray(row.symbol_breakdown)
    ? row.symbol_breakdown
    : [];
  row.state_version = Math.max(asNumber(event.sequence, 0), row.state_version || 0);
  state.trades[wallet] = {
    wallet,
    trades: row.trades,
    volume_usd: row.volume_usd,
    realized_pnl_usd: row.realized_pnl_usd,
    last_trade_at: asNumber(payload.last_trade_at, row.last_trade_at || row.last_activity_at || 0),
    updated_at: Date.now(),
  };
  state.live[wallet] = {
    wallet,
    last_opened_at: row.last_opened_at,
    last_activity_at: row.last_activity_at,
    last_head_check_at: row.last_head_check_at,
    verified_through_at: row.verified_through_at,
    open_positions: row.open_positions,
    open_positions_updated_at: row.open_positions_updated_at,
    review_stage: row.review_stage,
    is_live: row.review_stage === "live",
    updated_at: Date.now(),
  };
}

function applyPositionEvent(state, event) {
  const wallet = String(event.wallet || event.entity_key || "");
  if (!wallet) return;
  const payload = event.payload || {};
  state.positions[wallet] = {
    wallet,
    positions: Array.isArray(payload.positions) ? payload.positions : [],
    open_positions_count: Array.isArray(payload.positions)
      ? payload.positions.length
      : asNumber(payload.open_positions, 0),
    updated_at: Date.now(),
  };
  const row = upsertWallet(state, wallet);
  row.open_positions = Math.max(
    row.open_positions || 0,
    asNumber(state.positions[wallet].open_positions_count, 0)
  );
  row.open_positions_updated_at = Math.max(
    row.open_positions_updated_at || 0,
    asNumber(event.event_time, 0)
  );
  const positions = Array.isArray(payload.positions) ? payload.positions : [];
  const openedAt = positions.reduce((max, position) => {
    const value = asNumber(position && position.openedAt, 0);
    return value > max ? value : max;
  }, 0);
  if (openedAt > 0) {
    row.last_opened_at = Math.max(row.last_opened_at || 0, openedAt);
    row.last_activity_at = row.last_opened_at;
  }
}

function applyOrderEvent(state, event) {
  const wallet = String(event.wallet || event.entity_key || "");
  if (!wallet) return;
  const payload = event.payload || {};
  state.orders[wallet] = {
    wallet,
    orders: Array.isArray(payload.orders) ? payload.orders : [],
    updated_at: Date.now(),
  };
  const row = upsertWallet(state, wallet);
  row.open_orders = state.orders[wallet].orders.length;
}

function applyTradeEvent(state, event) {
  const wallet = String(event.wallet || event.entity_key || "");
  if (!wallet) return;
  const payload = event.payload || {};
  const current = state.trades[wallet] || {
    wallet,
    trades: 0,
    volume_usd: 0,
    realized_pnl_usd: 0,
    last_trade_at: 0,
  };
  current.trades += asNumber(payload.trade_count, 1);
  current.volume_usd += asNumber(payload.volume_usd, 0);
  current.realized_pnl_usd += asNumber(payload.realized_pnl_usd, 0);
  current.last_trade_at = Math.max(current.last_trade_at || 0, asNumber(event.event_time, 0));
  current.updated_at = Date.now();
  state.trades[wallet] = current;
  const row = upsertWallet(state, wallet);
  row.trades = current.trades;
  row.volume_usd = current.volume_usd;
  row.realized_pnl_usd = current.realized_pnl_usd;
  row.state_version = Math.max(row.state_version || 0, asNumber(event.sequence, 0));
}

function applyHeadCheckEvent(state, event) {
  const wallet = String(event.wallet || event.entity_key || "");
  if (!wallet) return;
  const payload = event.payload || {};
  const row = upsertWallet(state, wallet);
  const incomingLastTradeAt = asNumber(payload.last_trade_at, 0);
  const incomingFirstTradeAt = asNumber(payload.first_trade_at, 0);
  row.last_head_check_at = asNumber(payload.last_head_check_at || event.event_time, row.last_head_check_at);
  row.verified_through_at = Math.max(
    row.verified_through_at || 0,
    asNumber(payload.verified_through_at || payload.last_head_check_at || event.event_time, 0)
  );
  row.review_stage = payload.review_stage || row.review_stage || "live_transition";
  row.last_trade_at = Math.max(row.last_trade_at || 0, incomingLastTradeAt);
  if (incomingFirstTradeAt > 0 && (!row.first_trade_at || incomingFirstTradeAt < row.first_trade_at)) {
    row.first_trade_at = incomingFirstTradeAt;
  }
  if (asNumber(payload.volume_usd, 0) > 0) {
    row.volume_usd = Math.max(row.volume_usd || 0, asNumber(payload.volume_usd, 0));
  }
  state.live[wallet] = {
    wallet,
    last_opened_at: row.last_opened_at,
    last_activity_at: row.last_activity_at,
    last_head_check_at: row.last_head_check_at,
    verified_through_at: row.verified_through_at,
    open_positions: row.open_positions,
    open_positions_updated_at: row.open_positions_updated_at,
    review_stage: row.review_stage,
    is_live: row.review_stage === "live",
    updated_at: Date.now(),
  };
}

function applyLiveSnapshotEvent(state, event) {
  const wallet = String(event.wallet || event.entity_key || "");
  if (!wallet) return;
  const payload = event.payload || {};
  const row = upsertWallet(state, wallet);
  const incomingCount = Math.max(0, asNumber(payload.open_positions, 0));
  const incomingUpdatedAt = Math.max(
    0,
    asNumber(payload.open_positions_updated_at, 0),
    asNumber(payload.live_scanned_at, 0),
    asNumber(event.event_time, 0)
  );
  const currentUpdatedAt = Math.max(
    0,
    asNumber(row.open_positions_updated_at, 0),
    asNumber((state.live[wallet] || {}).open_positions_updated_at, 0),
    asNumber((state.positions[wallet] || {}).updated_at, 0)
  );
  if (incomingUpdatedAt < currentUpdatedAt) return;
  row.open_positions = incomingCount;
  row.open_positions_updated_at = incomingUpdatedAt;
  state.live[wallet] = {
    ...(state.live[wallet] || {}),
    wallet,
    last_opened_at: row.last_opened_at || 0,
    open_positions: incomingCount,
    open_positions_updated_at: incomingUpdatedAt,
    updated_at: Date.now(),
  };
  state.positions[wallet] = {
    ...(state.positions[wallet] || { wallet, positions: [] }),
    wallet,
    open_positions_count: incomingCount,
    updated_at: incomingUpdatedAt,
  };
}

function applyEvent(state, event) {
  const key = String(event.entity_key || event.wallet || event.market || "");
  const currentSequence = Number((state.meta.lastSequenceByEntity || {})[key] || 0);
  const incomingSequence = Number(event.sequence || 0);
  if (key && incomingSequence > 0 && incomingSequence < currentSequence) {
    return state;
  }

  switch (String(event.channel || "")) {
    case "market.price":
    case "market.book":
    case "market.trade":
      applyMarketEvent(state, event);
      break;
    case "bootstrap.wallet_snapshot":
      applyWalletSnapshot(state, event);
      break;
    case "wallet.positions_snapshot":
    case "wallet.position_update":
      applyPositionEvent(state, event);
      break;
    case "wallet.orders_snapshot":
    case "wallet.order_update":
      applyOrderEvent(state, event);
      break;
    case "wallet.trade_update":
      applyTradeEvent(state, event);
      break;
    case "wallet.head_check":
      applyHeadCheckEvent(state, event);
      break;
    case "wallet.live_snapshot":
      applyLiveSnapshotEvent(state, event);
      break;
    default:
      break;
  }

  if (key && incomingSequence > 0) {
    state.meta.lastSequenceByEntity[key] = incomingSequence;
  }
  state.meta.generatedAt = Date.now();
  state.meta.eventCount = asNumber(state.meta.eventCount, 0) + 1;
  return state;
}

module.exports = { applyEvent };
