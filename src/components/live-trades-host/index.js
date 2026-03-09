function deriveLi(payload) {
  if (payload && payload.li !== undefined && payload.li !== null) {
    return payload.li;
  }

  const data = payload && payload.data;

  if (Array.isArray(data) && data.length) {
    const last = data[data.length - 1];
    if (last && last.li !== undefined && last.li !== null) return last.li;
  }

  if (data && typeof data === "object" && data.li !== undefined && data.li !== null) {
    return data.li;
  }

  return null;
}

function createLiveTradesHostComponent({ wsClient, pipeline, account, logger = console }) {
  const detailSymbolMaxRaw = Number(process.env.PACIFICA_LIVE_DETAIL_SYMBOL_MAX || 6);
  const detailSymbolMax =
    Number.isFinite(detailSymbolMaxRaw) && detailSymbolMaxRaw > 0
      ? Math.max(1, Math.floor(detailSymbolMaxRaw))
      : 6;
  const tradeSymbolMaxRaw = Number(process.env.PACIFICA_LIVE_TRADES_SYMBOL_MAX || 0);
  const tradeSymbolMax =
    Number.isFinite(tradeSymbolMaxRaw) && tradeSymbolMaxRaw > 0
      ? Math.max(1, Math.floor(tradeSymbolMaxRaw))
      : Infinity;

  const state = {
    account: account || null,
    focusSymbols: [],
    started: false,
    subscribedSet: new Set(),
    lastIngestAtByKey: new Map(),
    throttleMsByChannel: {
      prices: 1000,
      bbo: 400,
      book: 1000,
      trades: 300,
      candle: 800,
      mark_price_candle: 800,
    },
  };

  function subscriptionKey(params) {
    return JSON.stringify(params);
  }

  function subscribe(params) {
    const key = subscriptionKey(params);
    if (state.subscribedSet.has(key)) return;
    state.subscribedSet.add(key);
    wsClient.subscribe(params);
  }

  function unsubscribe(params) {
    const key = subscriptionKey(params);
    if (!state.subscribedSet.has(key)) return;
    state.subscribedSet.delete(key);
    wsClient.unsubscribe(params);
  }

  function resetSubscriptions() {
    const current = Array.from(state.subscribedSet.values()).map((raw) => {
      try {
        return JSON.parse(raw);
      } catch (_error) {
        return null;
      }
    });

    current.forEach((params) => {
      if (params) wsClient.unsubscribe(params);
    });

    state.subscribedSet.clear();
  }

  function subscribeMarketChannels() {
    subscribe({ source: "prices" });

    const tradeSymbols = Number.isFinite(tradeSymbolMax)
      ? state.focusSymbols.slice(0, tradeSymbolMax)
      : state.focusSymbols;
    const detailSymbols = state.focusSymbols.slice(0, detailSymbolMax);

    tradeSymbols.forEach((symbol) => {
      subscribe({ source: "trades", symbol });
    });

    detailSymbols.forEach((symbol) => {
      subscribe({ source: "bbo", symbol });
      subscribe({ source: "candle", symbol, interval: "1m" });
      subscribe({ source: "mark_price_candle", symbol, interval: "1m" });
      subscribe({ source: "book", symbol, agg_level: 1 });
    });
  }

  function subscribeAccountChannels() {
    if (!state.account) return;
    subscribe({ source: "account_info", account: state.account });
    subscribe({ source: "account_positions", account: state.account });
    subscribe({ source: "account_trades", account: state.account });
    subscribe({ source: "account_order_updates", account: state.account });
    subscribe({ source: "account_margin", account: state.account });
    subscribe({ source: "account_leverage", account: state.account });
    // Intentionally no `account_orders` subscription (deprecated/removed).
  }

  function syncSubscriptions() {
    if (wsClient.getState().status !== "open") return;
    resetSubscriptions();
    subscribeMarketChannels();
    subscribeAccountChannels();
  }

  function getPayloadSymbol(payload) {
    if (payload && payload.data && !Array.isArray(payload.data)) {
      return payload.data.s || payload.data.symbol || null;
    }
    if (payload && Array.isArray(payload.data) && payload.data.length) {
      return payload.data[0].s || payload.data[0].symbol || null;
    }
    return null;
  }

  function shouldIngest(payload) {
    const channel = payload && payload.channel ? String(payload.channel) : "";
    const throttleMs = Number(state.throttleMsByChannel[channel] || 0);
    if (!channel || throttleMs <= 0) return true;

    const symbol = getPayloadSymbol(payload);
    const key = symbol ? `${channel}:${String(symbol).toUpperCase()}` : channel;
    const now = Date.now();
    const last = Number(state.lastIngestAtByKey.get(key) || 0);
    if (now - last < throttleMs) return false;
    state.lastIngestAtByKey.set(key, now);
    return true;
  }

  function ingestWsEvent(type, payload, dedupeHint = null) {
    const li = deriveLi(payload);
    const symbolFromData = getPayloadSymbol(payload);

    const channelKey = symbolFromData
      ? `${payload.channel}:${String(symbolFromData).toUpperCase()}`
      : payload.channel;

    const event = {
      source: "ws",
      type,
      channel: payload.channel,
      channelKey,
      li,
      data: payload.data,
    };

    pipeline.recordEvent(event, {
      dedupeKey:
        dedupeHint ||
        (li !== null && li !== undefined ? `ws:${channelKey}:${li}` : null),
    });
  }

  function onMessage(payload) {
    if (!payload || typeof payload !== "object") return;
    if (!payload.channel || payload.data === undefined || payload.data === null) return;
    if (!shouldIngest(payload)) return;

    switch (payload.channel) {
      case "prices":
        if (!state.focusSymbols.length && Array.isArray(payload.data) && payload.data.length) {
          const symbols = payload.data
            .map((row) => String((row && (row.symbol || row.s)) || "").toUpperCase())
            .filter(Boolean);
          if (symbols.length) {
            state.focusSymbols = Array.from(new Set(symbols));
            if (wsClient.getState().status === "open") {
              syncSubscriptions();
            }
          }
        }
        ingestWsEvent("ws.prices", payload);
        break;
      case "account_info":
        ingestWsEvent("ws.account_info", payload);
        break;
      case "account_positions":
        ingestWsEvent("ws.account_positions", payload);
        break;
      case "account_trades":
        ingestWsEvent("ws.account_trades", payload);
        break;
      case "account_order_updates":
        ingestWsEvent("ws.account_order_updates", payload);
        break;
      case "account_margin":
        ingestWsEvent("ws.account_margin", payload);
        break;
      case "account_leverage":
        ingestWsEvent("ws.account_leverage", payload);
        break;
      case "bbo":
        ingestWsEvent("ws.bbo", payload);
        break;
      case "trades":
        ingestWsEvent("ws.trades_public", payload);
        break;
      case "candle":
        ingestWsEvent("ws.candle", payload);
        break;
      case "mark_price_candle":
        ingestWsEvent("ws.mark_price_candle", payload);
        break;
      case "book":
        ingestWsEvent("ws.book", payload);
        break;
      default:
        break;
    }
  }

  function onStatus(status) {
    pipeline.updateWsStatus(status);
    if (status === "open") {
      syncSubscriptions();
    }
  }

  function onError(error) {
    logger.warn(`[live-host] ws handler error: ${error.message}`);
  }

  function start() {
    if (state.started) return;
    state.started = true;
    wsClient.setHandlers({ onMessage, onStatus, onError });
    wsClient.start();
  }

  function stop() {
    state.started = false;
    resetSubscriptions();
    wsClient.stop();
  }

  function setAccount(nextAccount) {
    state.account = nextAccount || null;
    if (state.started) syncSubscriptions();
  }

  function setFocusSymbols(symbols = []) {
    state.focusSymbols = Array.from(
      new Set(
        (Array.isArray(symbols) ? symbols : [])
          .map((symbol) => String(symbol || "").toUpperCase())
          .filter(Boolean)
      )
    );

    if (state.started) syncSubscriptions();
  }

  function getState() {
    return {
      account: state.account,
      focusSymbols: state.focusSymbols,
      subscriptions: state.subscribedSet.size,
    };
  }

  return {
    start,
    stop,
    setAccount,
    setFocusSymbols,
    getState,
  };
}

module.exports = {
  createLiveTradesHostComponent,
};
