let NodeWebSocket = null;
try {
  const wsModule = require("ws");
  NodeWebSocket = wsModule && wsModule.WebSocket ? wsModule.WebSocket : wsModule;
} catch (_error) {
  NodeWebSocket = null;
}

let HttpsProxyAgent = null;
try {
  const proxyModule = require("https-proxy-agent");
  HttpsProxyAgent =
    proxyModule && proxyModule.HttpsProxyAgent
      ? proxyModule.HttpsProxyAgent
      : proxyModule;
} catch (_error) {
  HttpsProxyAgent = null;
}

const WebSocketImpl = NodeWebSocket || global.WebSocket || null;

function bindWsEvent(ws, eventName, handler) {
  if (!ws || typeof handler !== "function") return;
  if (typeof ws.on === "function") {
    ws.on(eventName, handler);
    return;
  }
  if (typeof ws.addEventListener === "function") {
    ws.addEventListener(eventName, (event) => {
      if (eventName === "message") {
        handler(event && Object.prototype.hasOwnProperty.call(event, "data") ? event.data : event);
        return;
      }
      handler(event);
    });
    return;
  }
  throw new Error(`WebSocket transport does not support event binding for ${eventName}`);
}

function createWsClient({
  url,
  logger = console,
  reconnectMs = 2000,
  pingIntervalMs = 25000,
  connectTimeoutMs = 10000,
  proxyUrl = "",
}) {
  const proxyText = String(proxyUrl || "").trim();
  const normalizedProxyUrl = proxyText
    ? /^[a-z]+:\/\//i.test(proxyText)
      ? proxyText
      : `http://${proxyText}`
    : "";
  const proxyAgent =
    normalizedProxyUrl && HttpsProxyAgent ? new HttpsProxyAgent(normalizedProxyUrl) : null;
  const state = {
    ws: null,
    status: "idle",
    shouldRun: false,
    manualClose: false,
    reconnectTimer: null,
    pingTimer: null,
    connectTimer: null,
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
    if (state.connectTimer) {
      clearTimeout(state.connectTimer);
      state.connectTimer = null;
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

    state.manualClose = false;
    emitStatus("connecting");
    if (!WebSocketImpl) {
      throw new Error("No WebSocket implementation available");
    }
    const wsOptions = proxyAgent ? { agent: proxyAgent } : undefined;
    const ws = wsOptions ? new WebSocketImpl(url, wsOptions) : new WebSocketImpl(url);
    state.ws = ws;
    state.connectTimer = setTimeout(() => {
      state.connectTimer = null;
      if (!state.ws || state.ws !== ws) return;
      if (state.status === "open" || state.manualClose) return;
      try {
        if (typeof ws.terminate === "function") {
          ws.terminate();
        } else if (typeof ws.close === "function") {
          ws.close();
        }
      } catch (_error) {
        // ignore
      }
      if (typeof state.onError === "function") {
        state.onError(new Error(`ws_connect_timeout:${connectTimeoutMs}`));
      }
    }, Math.max(1000, Number(connectTimeoutMs || 10000)));
    if (state.connectTimer && typeof state.connectTimer.unref === "function") {
      state.connectTimer.unref();
    }

    bindWsEvent(ws, "open", () => {
      if (state.connectTimer) {
        clearTimeout(state.connectTimer);
        state.connectTimer = null;
      }
      emitStatus("open");
      resubscribeAll();
      state.pingTimer = setInterval(() => {
        sendRaw({ method: "ping" });
      }, pingIntervalMs);
    });

    bindWsEvent(ws, "message", handleMessage);

    bindWsEvent(ws, "error", (error) => {
      if (state.connectTimer) {
        clearTimeout(state.connectTimer);
        state.connectTimer = null;
      }
      if (!state.manualClose) {
        logger.warn(`[ws-client] error: ${error.message}`);
        if (typeof state.onError === "function") state.onError(error);
      }
      emitStatus("error");
    });

    bindWsEvent(ws, "close", () => {
      if (state.connectTimer) {
        clearTimeout(state.connectTimer);
        state.connectTimer = null;
      }
      emitStatus("closed");
      state.ws = null;
      clearTimers();
      if (!state.manualClose) {
        scheduleReconnect();
      }
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
        state.manualClose = true;
        const readyState = Number(state.ws.readyState);
        if (
          readyState === Number(state.ws.OPEN) ||
          readyState === Number(state.ws.CLOSING)
        ) {
          state.ws.close();
        } else if (typeof state.ws.terminate === "function") {
          state.ws.terminate();
        }
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
      proxyUrl: normalizedProxyUrl || null,
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
