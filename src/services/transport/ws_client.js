const WebSocket = global.WebSocket || require("ws");

function createWsClient({
  url,
  logger = console,
  reconnectMs = 2000,
  pingIntervalMs = 25000,
}) {
  const state = {
    ws: null,
    status: "idle",
    shouldRun: false,
    reconnectTimer: null,
    pingTimer: null,
    subscriptions: new Map(),
    onMessage: null,
    onStatus: null,
    onError: null,
  };

  function emitStatus(nextStatus) {
    state.status = nextStatus;
    if (typeof state.onStatus === "function") {
      state.onStatus(nextStatus);
    }
  }

  function clearTimers() {
    if (state.reconnectTimer) {
      clearTimeout(state.reconnectTimer);
      state.reconnectTimer = null;
    }
    if (state.pingTimer) {
      clearInterval(state.pingTimer);
      state.pingTimer = null;
    }
  }

  function sendRaw(payload) {
    if (!state.ws || state.status !== "open") return false;
    state.ws.send(JSON.stringify(payload));
    return true;
  }

  function subscribe(params) {
    const key = JSON.stringify(params);
    state.subscriptions.set(key, params);
    sendRaw({ method: "subscribe", params });
  }

  function unsubscribe(params) {
    const key = JSON.stringify(params);
    state.subscriptions.delete(key);
    sendRaw({ method: "unsubscribe", params });
  }

  function clearSubscriptions() {
    const paramsList = Array.from(state.subscriptions.values());
    state.subscriptions.clear();
    paramsList.forEach((params) => {
      sendRaw({ method: "unsubscribe", params });
    });
  }

  function resubscribeAll() {
    state.subscriptions.forEach((params) => {
      sendRaw({ method: "subscribe", params });
    });
  }

  function scheduleReconnect() {
    if (!state.shouldRun || state.reconnectTimer) return;
    state.reconnectTimer = setTimeout(() => {
      state.reconnectTimer = null;
      connect();
    }, reconnectMs);
  }

  function handleMessage(raw) {
    try {
      const text = typeof raw === "string" ? raw : raw.toString();
      const payload = JSON.parse(text);
      if (payload.channel === "pong") return;
      if (typeof state.onMessage === "function") {
        state.onMessage(payload);
      }
    } catch (error) {
      if (typeof state.onError === "function") state.onError(error);
    }
  }

  function connect() {
    if (!state.shouldRun) return;
    clearTimers();

    emitStatus("connecting");
    const ws = new WebSocket(url);
    state.ws = ws;

    ws.on("open", () => {
      emitStatus("open");
      resubscribeAll();
      state.pingTimer = setInterval(() => {
        sendRaw({ method: "ping" });
      }, pingIntervalMs);
    });

    ws.on("message", handleMessage);

    ws.on("error", (error) => {
      logger.warn(`[ws-client] error: ${error.message}`);
      if (typeof state.onError === "function") state.onError(error);
      emitStatus("error");
    });

    ws.on("close", () => {
      emitStatus("closed");
      state.ws = null;
      clearTimers();
      scheduleReconnect();
    });
  }

  function start() {
    if (state.shouldRun) return;
    state.shouldRun = true;
    connect();
  }

  function stop() {
    state.shouldRun = false;
    clearTimers();
    if (state.ws) {
      try {
        state.ws.close();
      } catch (_error) {
        // ignore
      }
    }
    state.ws = null;
    emitStatus("stopped");
  }

  function setHandlers({ onMessage, onStatus, onError }) {
    state.onMessage = onMessage || null;
    state.onStatus = onStatus || null;
    state.onError = onError || null;
  }

  function getState() {
    return {
      status: state.status,
      subscriptions: state.subscriptions.size,
      url,
    };
  }

  return {
    getState,
    sendRaw,
    setHandlers,
    start,
    stop,
    clearSubscriptions,
    subscribe,
    unsubscribe,
  };
}

module.exports = {
  createWsClient,
};
