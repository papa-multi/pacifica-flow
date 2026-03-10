"use strict";

if (typeof window !== "undefined") {
  window.__PF_APP_SCRIPT_EXECUTED = true;
  window.__PF_APP_BOOTED = false;
}
const state = {
  view: "exchange",
  timeframe: "all",
  analyticsTab: "volume",
  exchange: null,
  tokenAnalytics: null,
  tokenSymbol: "",
  volumeSeries: null,
  volumeRankPage: 1,
  volumeRankPageSize: 10,
  volumeFilter: "",
  volumeSort: "volume",
  volumeRankFilteredTotal: 0,
  wallets: null,
  walletProfile: null,
  walletSearch: "",
  walletPage: 1,
  walletPageSize: 20,
  selectedWallet: null
};
const tickerState = {
  signature: "",
  layout: ""
};
const refreshState = {
  exchange: null,
  wallets: null,
  walletProfile: null
};
const pollTimers = new Map();
let tickerResizeRaf = 0;
let chartResizeRaf = 0;
let activeSeries = "monthly";
let activeChartType = "bars";
let chartHasUserControl = false;
const ENABLE_CHART_BRUSH = true;
const chartState = {
  window: 0,
  offset: 0
};
const navigatorState = {
  mode: null,
  startX: 0,
  startOffset: 0,
  startWindow: 0
};
let chartDragging = false;
let chartDragStartX = 0;
let chartDragStartOffset = 0;
let chartResizeObserver = null;
let chartViewportResizeRaf = 0;
const chartDateFormatters = {
  daily: new Intl.DateTimeFormat("en-GB", {
    day: "2-digit",
    month: "short",
    year: "numeric",
    timeZone: "UTC"
  }),
  weekly: new Intl.DateTimeFormat("en-GB", {
    day: "2-digit",
    month: "short",
    year: "numeric",
    timeZone: "UTC"
  }),
  monthly: new Intl.DateTimeFormat("en-GB", {
    month: "short",
    year: "numeric",
    timeZone: "UTC"
  })
};
const TOKEN_DEFAULT_METRIC = "oi";
const TOKEN_METRIC_ROUTE_TO_TAB = {
  oi: "oi",
  "open-interest": "oi",
  open_interest: "oi",
  funding: "funding",
  liquidations: "liquidations",
  netflow: "netflow",
  "wallet-activity": "wallet_activity",
  wallet_activity: "wallet_activity",
  volume: "volume"
};
const TOKEN_TAB_TO_ROUTE_METRIC = {
  oi: "oi",
  funding: "funding",
  liquidations: "liquidations",
  netflow: "netflow",
  wallet_activity: "wallet-activity",
  volume: "volume"
};
function toNum(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}
function fmt(value, digits = 2) {
  const num = toNum(value, NaN);
  if (!Number.isFinite(num)) return "-";
  return num.toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits
  });
}
function fmtSigned(value, digits = 2) {
  const num = toNum(value, NaN);
  if (!Number.isFinite(num)) return "-";
  return `${num > 0 ? "+" : ""}${fmt(num, digits)}`;
}
function fmtSignedPct(value, digits = 1) {
  const num = toNum(value, NaN);
  if (!Number.isFinite(num)) return "-";
  return `${num > 0 ? "+" : ""}${fmt(num, digits)}%`;
}
function fmtCompact(value) {
  const num = toNum(value, 0);
  const abs = Math.abs(num);
  if (abs >= 1e12) return `${fmt(num / 1e12, 2)}T`;
  if (abs >= 1e9) return `${fmt(num / 1e9, 2)}B`;
  if (abs >= 1e6) return `${fmt(num / 1e6, 2)}M`;
  if (abs >= 1e3) return `${fmt(num / 1e3, 2)}K`;
  return fmt(num, 2);
}
function fmtMs(value) {
  const num = toNum(value, NaN);
  if (!Number.isFinite(num) || num < 0) return "-";
  if (num < 1000) return `${Math.round(num)}ms`;
  return `${(num / 1000).toFixed(2)}s`;
}
function fmtDurationMs(value) {
  const num = toNum(value, NaN);
  if (!Number.isFinite(num) || num < 0) return "-";
  if (num < 1000) return `${Math.round(num)}ms`;
  if (num < 60000) return `${Math.round(num / 1000)}s`;
  if (num < 3600000) return `${Math.round(num / 60000)}m`;
  if (num < 86400000) return `${(num / 3600000).toFixed(1)}h`;
  return `${(num / 86400000).toFixed(1)}d`;
}
function fmtTime(ts) {
  const num = Number(ts);
  if (!Number.isFinite(num) || num <= 0) return "-";
  return new Date(num).toLocaleString();
}
function fmtAgo(ts) {
  const num = Number(ts);
  if (!Number.isFinite(num) || num <= 0) return "-";
  const diff = Math.max(0, Date.now() - num);
  if (diff < 1000) return "just now";
  if (diff < 60000) return `${Math.round(diff / 1000)}s ago`;
  if (diff < 3600000) return `${Math.round(diff / 60000)}m ago`;
  if (diff < 86400000) return `${Math.round(diff / 3600000)}h ago`;
  return `${Math.round(diff / 86400000)}d ago`;
}
function getKpiComparisonDisplay(entry = {}) {
  const trend = String((entry === null || entry === void 0 ? void 0 : entry.trend) || "flat").toLowerCase();
  const deltaPct = toNum(entry === null || entry === void 0 ? void 0 : entry.deltaPct, NaN);
  const comparisonLabel = String((entry === null || entry === void 0 ? void 0 : entry.comparisonLabel) || "snapshot");
  if (Number.isFinite(deltaPct)) {
    const badgeClass = trend === "up" ? "up" : trend === "down" ? "down" : "flat";
    const icon = trend === "up" ? "▲" : trend === "down" ? "▼" : "●";
    return {
      badgeClass,
      badgeText: `${icon} ${fmtSignedPct(deltaPct, 1)}`,
      labelText: comparisonLabel
    };
  }
  if (trend === "up" || trend === "down") {
    const icon = trend === "up" ? "▲" : "▼";
    return {
      badgeClass: trend,
      badgeText: `${icon} ${comparisonLabel}`,
      labelText: "comparison pending"
    };
  }
  return {
    badgeClass: "flat",
    badgeText: "● steady",
    labelText: comparisonLabel
  };
}
function syncTimeframeButtons() {
  const activeTimeframe = String(state.timeframe || "all");
  document.querySelectorAll("[data-timeframe]").forEach(btn => {
    const isActive = String(btn.dataset.timeframe || "all") === activeTimeframe;
    btn.classList.toggle("active", isActive);
  });
}
function escapeHtml(value) {
  return String(value || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}
function sanitizeTickerLabel(value) {
  return String(value || "").trim().toUpperCase();
}
function formatTickerPrice(value) {
  const num = toNum(value, NaN);
  if (!Number.isFinite(num)) return "-";
  const abs = Math.abs(num);
  let maxDigits = 2;
  if (abs < 1) maxDigits = 4;
  if (abs < 0.1) maxDigits = 5;
  if (abs < 0.01) maxDigits = 6;
  return `$${num.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: maxDigits
  })}`;
}
function formatTickerPercent(value) {
  const num = toNum(value, NaN);
  if (!Number.isFinite(num)) return "N/A";
  return `${num > 0 ? "+" : ""}${num.toFixed(2)}%`;
}
function normalizeTokenSymbol(value) {
  return String(value || "").trim().toUpperCase().replace(/[^A-Z0-9/_-]/g, "");
}
function normalizeAnalyticsTab(value) {
  const raw = String(value || TOKEN_DEFAULT_METRIC).trim().toLowerCase().replace(/\s+/g, "_").replace(/-/g, "_");
  if (raw === "open_interest") return "oi";
  return TOKEN_METRIC_ROUTE_TO_TAB[raw] || TOKEN_DEFAULT_METRIC;
}
function analyticsTabToRouteMetric(tab) {
  const normalized = normalizeAnalyticsTab(tab);
  return TOKEN_TAB_TO_ROUTE_METRIC[normalized] || TOKEN_TAB_TO_ROUTE_METRIC[TOKEN_DEFAULT_METRIC];
}
function analyticsRouteMetricToTab(metric) {
  if (!metric) return TOKEN_DEFAULT_METRIC;
  const raw = String(metric).trim().toLowerCase();
  return normalizeAnalyticsTab(TOKEN_METRIC_ROUTE_TO_TAB[raw] || raw);
}
function getTokenRoutePath(symbol = state.tokenSymbol, metric = state.analyticsTab) {
  const normalizedSymbol = normalizeTokenSymbol(symbol);
  if (!normalizedSymbol) return "/";
  const normalizedMetric = normalizeAnalyticsTab(metric);
  const metricSlug = analyticsTabToRouteMetric(normalizedMetric);
  const base = `/exchange/${encodeURIComponent(normalizedSymbol)}`;
  return normalizedMetric === TOKEN_DEFAULT_METRIC ? base : `${base}/${metricSlug}`;
}
function parseTokenRoute(pathname = "/") {
  const match = String(pathname || "").match(/^\/exchange\/([^/]+)(?:\/([^/]+))?\/?$/i);
  if (!match) return null;
  const symbol = normalizeTokenSymbol(decodeURIComponent(match[1] || ""));
  const metric = analyticsRouteMetricToTab(match[2] || TOKEN_DEFAULT_METRIC);
  if (!symbol) return null;
  return {
    symbol,
    metric
  };
}
function buildTickerItemMarkup(item = {}) {
  const symbol = sanitizeTickerLabel(item.displaySymbol || item.symbol || "UNKNOWN");
  const quote = sanitizeTickerLabel(item.quoteAsset || "USD");
  const price = formatTickerPrice(item.price);
  const change = Number(item.changePct24h);
  const changeClass = Number.isFinite(change) ? change > 0 ? "up" : change < 0 ? "down" : "flat" : "unknown";
  const changeLabel = formatTickerPercent(change);
  return `
    <article class="markets-ticker-item">
      <div class="markets-ticker-symbols">
        <span class="markets-ticker-symbol">${escapeHtml(symbol)}</span>
        <span class="markets-ticker-quote">${escapeHtml(quote)}</span>
      </div>
      <strong class="markets-ticker-price">${price}</strong>
      <span class="markets-ticker-change ${changeClass}">${changeLabel} 24h</span>
    </article>
  `;
}
function isCompactTickerMode() {
  if (typeof window === "undefined" || typeof window.matchMedia !== "function") return false;
  return window.matchMedia("(max-width: 980px)").matches;
}
function mapExchangePricesToTickerMarkets(payload = {}) {
  const rows = payload && payload.source && Array.isArray(payload.source.prices) && payload.source.prices.length ? payload.source.prices : [];
  return rows.map(row => {
    const symbol = sanitizeTickerLabel(row && row.symbol ? row.symbol : "");
    if (!symbol) return null;
    const price = toNum(row && row.mark !== undefined ? row.mark : row && row.price !== undefined ? row.price : NaN, NaN);
    const prev = toNum(row && row.yesterday_price !== undefined ? row.yesterday_price : row && row.yesterdayPrice !== undefined ? row.yesterdayPrice : NaN, NaN);
    let changePct24h = NaN;
    if (Number.isFinite(price) && Number.isFinite(prev) && prev !== 0) {
      changePct24h = (price - prev) / prev * 100;
    } else {
      changePct24h = toNum(row && row.change_24h_pct !== undefined ? row.change_24h_pct : row && row.change24hPct !== undefined ? row.change24hPct : NaN, NaN);
    }
    return {
      symbol,
      displaySymbol: symbol,
      quoteAsset: "USD",
      price,
      changePct24h,
      volume24h: toNum(row && row.volume_24h !== undefined ? row.volume_24h : row && row.volume24h !== undefined ? row.volume24h : 0, 0)
    };
  }).filter(row => row && row.symbol && Number.isFinite(row.price)).sort((a, b) => toNum(b.volume24h, 0) - toNum(a.volume24h, 0)).slice(0, 80);
}
function renderMarketsTicker(payload = {}) {
  const ticker = el("marketsTicker");
  const track = el("marketsTickerTrack");
  if (!ticker || !track) return;
  const markets = mapExchangePricesToTickerMarkets(payload);
  if (!markets.length) {
    ticker.hidden = true;
    track.innerHTML = "";
    tickerState.signature = "";
    tickerState.layout = "";
    return;
  }
  const compactMode = isCompactTickerMode();
  const layout = compactMode ? "compact" : "loop";
  const signature = markets.map(item => {
    const price = Number(item.price);
    const change = Number(item.changePct24h);
    return `${item.symbol}:${Number.isFinite(price) ? price.toFixed(8) : "na"}:${Number.isFinite(change) ? change.toFixed(6) : "na"}`;
  }).join("|");
  ticker.dataset.layout = layout;
  if (signature !== tickerState.signature || layout !== tickerState.layout) {
    const markup = markets.map(item => buildTickerItemMarkup(item)).join("");
    track.innerHTML = compactMode ? markup : `${markup}${markup}`;
    if (compactMode) {
      track.style.removeProperty("--ticker-duration");
    } else {
      const durationSeconds = Math.min(220, Math.max(58, markets.length * 2.2));
      track.style.setProperty("--ticker-duration", `${durationSeconds}s`);
    }
    tickerState.signature = signature;
    tickerState.layout = layout;
  }
  ticker.hidden = false;
}
function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
}
function runSingleFlight(key, task) {
  if (refreshState[key]) return refreshState[key];
  const promise = (async () => {
    try {
      return await task();
    } finally {
      refreshState[key] = null;
    }
  })();
  refreshState[key] = promise;
  return promise;
}
function startPollingLoop(key, task, intervalMs) {
  const delay = Math.max(1000, Number(intervalMs || 0));
  if (pollTimers.has(key)) {
    window.clearTimeout(pollTimers.get(key));
    pollTimers.delete(key);
  }
  const tick = async () => {
    try {
      if (typeof document === "undefined" || document.visibilityState === "visible") {
        await task();
      }
    } catch (_error) {
      // Keep poll loops alive; runtime banner is only for hard boot/runtime failures.
    } finally {
      pollTimers.set(key, window.setTimeout(tick, delay));
    }
  };
  pollTimers.set(key, window.setTimeout(tick, delay));
}
function toFiniteNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}
function formatChartCurrency(value) {
  if (!Number.isFinite(value)) return "N/A";
  const sign = value < 0 ? "-" : "";
  const abs = Math.abs(value);
  let suffix = "";
  let scaled = abs;
  if (abs >= 1e12) {
    scaled = abs / 1e12;
    suffix = "t";
  } else if (abs >= 1e9) {
    scaled = abs / 1e9;
    suffix = "b";
  } else if (abs >= 1e6) {
    scaled = abs / 1e6;
    suffix = "m";
  } else if (abs >= 1e3) {
    scaled = abs / 1e3;
    suffix = "k";
  }
  const digits = scaled >= 100 ? 0 : scaled >= 10 ? 1 : 2;
  return `${sign}$${scaled.toFixed(digits)}${suffix}`;
}
function formatMetricAxis(value) {
  const unit = getAnalyticsPresentation().unit;
  if (unit === "pct") return `${toFiniteNumber(value, 0).toFixed(3)}%`;
  if (unit === "count") return fmt(value, 0);
  return formatChartCurrency(value);
}
function formatMetricTooltip(value) {
  const unit = getAnalyticsPresentation().unit;
  if (unit === "pct") return `${toFiniteNumber(value, 0).toFixed(4)}%`;
  if (unit === "count") return fmt(value, 0);
  return formatChartCurrency(value);
}
function getSeriesValue(item) {
  if (!item) return 0;
  if (Number.isFinite(item.value)) return item.value;
  if (Number.isFinite(item.volume)) return item.volume;
  return 0;
}
function getSnapshotLimitForSeries() {
  if (activeSeries === "daily") return 20;
  if (activeSeries === "weekly") return 32;
  return 48;
}
function getAnalyticsPresentation(tab = state.analyticsTab) {
  const normalizedTab = normalizeAnalyticsTab(tab);
  const map = {
    volume: {
      title: "Trading Volume",
      note: "Token-scoped notional volume trend.",
      unit: "usd",
      chartMode: "bar",
      legend: [{
        key: "volume",
        label: "Volume",
        tone: "volume"
      }]
    },
    oi: {
      title: "Open Interest",
      note: "Open interest trend for the selected token.",
      unit: "usd",
      chartMode: "line_area",
      legend: [{
        key: "oi",
        label: "Open Interest",
        tone: "oi"
      }]
    },
    funding: {
      title: "Funding",
      note: "Funding trend with explicit zero baseline.",
      unit: "pct",
      chartMode: "line_zero",
      legend: [{
        key: "positive",
        label: "Positive Funding",
        tone: "positive"
      }, {
        key: "negative",
        label: "Negative Funding",
        tone: "negative"
      }]
    },
    liquidations: {
      title: "Liquidations",
      note: "Long vs short liquidation pressure by time bucket.",
      unit: "usd",
      chartMode: "stacked_split",
      legend: [{
        key: "long",
        label: "Long Pressure",
        tone: "negative"
      }, {
        key: "short",
        label: "Short Pressure",
        tone: "positive"
      }]
    },
    netflow: {
      title: "Netflow",
      note: "Signed flow split around a zero baseline.",
      unit: "usd",
      chartMode: "diverging",
      legend: [{
        key: "inflow",
        label: "Positive Netflow",
        tone: "positive"
      }, {
        key: "outflow",
        label: "Negative Netflow",
        tone: "negative"
      }]
    },
    wallet_activity: {
      title: "Wallet Activity",
      note: "Event activity lane for token-related flow.",
      unit: "count",
      chartMode: "event_lane",
      legend: [{
        key: "events",
        label: "Wallet Events",
        tone: "event"
      }]
    }
  };
  return map[normalizedTab] || map[TOKEN_DEFAULT_METRIC];
}
function buildSymbolSnapshotSeries(mapper, options = {}) {
  var _state$exchange, _state$exchange$sourc;
  const prices = Array.isArray((_state$exchange = state.exchange) === null || _state$exchange === void 0 ? void 0 : (_state$exchange$sourc = _state$exchange.source) === null || _state$exchange$sourc === void 0 ? void 0 : _state$exchange$sourc.prices) ? state.exchange.source.prices : [];
  const useAbsSort = Boolean(options.absSort);
  const limit = Math.max(1, Number(options.limit || getSnapshotLimitForSeries()));
  return prices.map(row => {
    const symbol = String((row === null || row === void 0 ? void 0 : row.symbol) || "").trim();
    if (!symbol) return null;
    const value = toFiniteNumber(mapper(row), Number.NaN);
    if (!Number.isFinite(value)) return null;
    return {
      label: symbol,
      value
    };
  }).filter(Boolean).sort((a, b) => {
    const av = useAbsSort ? Math.abs(a.value) : a.value;
    const bv = useAbsSort ? Math.abs(b.value) : b.value;
    return bv - av;
  }).slice(0, limit);
}
function buildChartData(series) {
  if (!Array.isArray(series) || !series.length) return [];
  return series.map(item => ({
    ...item,
    value: toFiniteNumber(getSeriesValue(item), 0)
  }));
}
function buildCumulativeSeries(series) {
  if (!Array.isArray(series) || !series.length) return [];
  let running = 0;
  return series.map(item => {
    const stepValue = toFiniteNumber(getSeriesValue(item), 0);
    running += stepValue;
    return {
      ...item,
      value: running,
      stepValue
    };
  });
}
function buildSmoothLinePath(points) {
  if (!Array.isArray(points) || !points.length) return "";
  return points.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x.toFixed(3)} ${point.y.toFixed(3)}`).join(" ");
}
function formatChartLabel(item, mode = "axis") {
  if (!item) return "";
  const hasRange = Number.isFinite(item.bucketStart) && Number.isFinite(item.bucketEnd) && item.bucketEnd > item.bucketStart;
  if (hasRange && mode === "tooltip") {
    const formatter = chartDateFormatters[activeSeries] || chartDateFormatters.daily;
    const start = formatter.format(new Date(item.bucketStart));
    const end = formatter.format(new Date(item.bucketEnd));
    return activeSeries === "daily" ? `${start} - ${end} UTC` : `${start} - ${end}`;
  }
  if (Number.isFinite(item.timestamp)) {
    const formatter = chartDateFormatters[activeSeries] || chartDateFormatters.daily;
    const label = formatter.format(new Date(item.timestamp));
    return mode === "tooltip" && activeSeries === "daily" ? `${label} UTC` : label;
  }
  return item.label || "";
}
function shouldShowAxisLabel(index, total) {
  if (total <= 12) return true;
  const step = Math.max(1, Math.ceil(total / 8));
  return index % step === 0 || index === total - 1;
}
function aggregateBarSeriesByWidth(series, chartWidth) {
  if (!Array.isArray(series) || !series.length) {
    return {
      items: [],
      binned: false,
      binSize: 1
    };
  }
  const width = Number.isFinite(chartWidth) && chartWidth > 0 ? chartWidth : 720;
  const maxRenderableBars = Math.max(12, Math.floor(width / 4));
  if (series.length <= maxRenderableBars) {
    return {
      items: series,
      binned: false,
      binSize: 1
    };
  }
  const binSize = Math.max(2, Math.ceil(series.length / maxRenderableBars));
  const binned = [];
  for (let index = 0; index < series.length; index += binSize) {
    const chunk = series.slice(index, Math.min(index + binSize, series.length));
    if (!chunk.length) continue;
    const first = chunk[0];
    const last = chunk[chunk.length - 1];
    const sumValue = chunk.reduce((total, item) => total + toFiniteNumber(item === null || item === void 0 ? void 0 : item.value, 0), 0);
    const firstTs = Number.isFinite(first === null || first === void 0 ? void 0 : first.timestamp) ? first.timestamp : null;
    const lastTs = Number.isFinite(last === null || last === void 0 ? void 0 : last.timestamp) ? last.timestamp : firstTs;
    const midpointTs = Number.isFinite(firstTs) && Number.isFinite(lastTs) ? Math.round((firstTs + lastTs) / 2) : Number.isFinite(lastTs) ? lastTs : Number.isFinite(firstTs) ? firstTs : null;
    binned.push({
      ...last,
      value: sumValue,
      timestamp: midpointTs,
      bucketStart: firstTs,
      bucketEnd: lastTs,
      bucketCount: chunk.length,
      binned: true
    });
  }
  return {
    items: binned,
    binned: true,
    binSize
  };
}
function getScaleConfig(data, options = {}) {
  const baseline = options.baseline === "auto" ? "auto" : "zero";
  const values = (Array.isArray(data) ? data : []).map(item => toFiniteNumber(item === null || item === void 0 ? void 0 : item.value, Number.NaN)).filter(value => Number.isFinite(value));
  if (!values.length) {
    return {
      min: 0,
      max: 1,
      span: 1,
      normalize: () => 0,
      axisAtRatio: ratio => clamp(toFiniteNumber(ratio, 0), 0, 1)
    };
  }
  let minRaw = Math.min(...values);
  const maxRaw = Math.max(...values);
  if (baseline === "zero" || maxRaw <= 0) {
    minRaw = Math.min(0, minRaw);
  }
  const spanRaw = Math.max(maxRaw - minRaw, 0);
  const headroomPct = activeSeries === "daily" ? 0.12 : 0.08;
  const headroom = spanRaw > 0 ? spanRaw * headroomPct : Math.max(Math.abs(maxRaw) * headroomPct, 1);
  let domainMax = maxRaw + headroom;
  let domainMin = minRaw;
  if (!Number.isFinite(domainMax) || domainMax <= domainMin) {
    domainMax = domainMin + Math.max(Math.abs(domainMin) * 0.05, 1);
  }
  const span = domainMax - domainMin;
  return {
    min: domainMin,
    max: domainMax,
    span,
    normalize: value => clamp((toFiniteNumber(value, domainMin) - domainMin) / span, 0, 1),
    axisAtRatio: ratio => domainMin + span * clamp(toFiniteNumber(ratio, 0), 0, 1)
  };
}
function renderCumulativeLine(layer, data, scaleConfig, pointsContainer, options = {}) {
  if (!layer || !Array.isArray(data) || !data.length) return [];
  const maxDots = 48;
  const bounds = layer.getBoundingClientRect();
  const plotWidth = Math.max(1, Math.round(layer.clientWidth || bounds.width || 0));
  const plotHeight = Math.max(1, Math.round(layer.clientHeight || bounds.height || 0));
  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("viewBox", `0 0 ${plotWidth} ${plotHeight}`);
  svg.setAttribute("preserveAspectRatio", "xMinYMin meet");
  svg.setAttribute("width", String(plotWidth));
  svg.setAttribute("height", String(plotHeight));
  svg.classList.add("chart-line-svg");
  const points = data.map((item, index) => {
    const xPct = data.length === 1 ? 50 : index / (data.length - 1) * 100;
    const x = xPct / 100 * plotWidth;
    const ratio = scaleConfig !== null && scaleConfig !== void 0 && scaleConfig.normalize ? scaleConfig.normalize(item.value) : 0;
    const yPct = 100 - ratio * 100;
    const y = yPct / 100 * plotHeight;
    return {
      x,
      y,
      xPct,
      yPct,
      item
    };
  });
  const linePathData = buildSmoothLinePath(points);
  const firstPoint = points[0];
  const lastPoint = points[points.length - 1];
  const areaPathData = `${linePathData} L ${lastPoint.x.toFixed(3)} ${plotHeight.toFixed(3)} L ${firstPoint.x.toFixed(3)} ${plotHeight.toFixed(3)} Z`;
  if (options.showArea !== false) {
    const areaPath = document.createElementNS("http://www.w3.org/2000/svg", "path");
    areaPath.setAttribute("d", areaPathData);
    areaPath.classList.add("chart-area-minimal");
    svg.appendChild(areaPath);
  }
  const linePath = document.createElementNS("http://www.w3.org/2000/svg", "path");
  linePath.setAttribute("d", linePathData);
  linePath.classList.add("chart-line-minimal");
  if (options.lineClass) linePath.classList.add(options.lineClass);
  svg.appendChild(linePath);
  layer.appendChild(svg);
  const dotStep = Math.max(1, Math.ceil(points.length / maxDots));
  const target = pointsContainer || layer;
  points.forEach((point, index) => {
    const isEdge = index === 0 || index === points.length - 1;
    if (!isEdge && index % dotStep !== 0) return;
    const dot = document.createElement("div");
    dot.className = "chart-point";
    dot.style.left = `${point.xPct}%`;
    dot.style.top = `${point.yPct}%`;
    target.appendChild(dot);
  });
  return points;
}
function attachLineHoverOverlay(container, points) {
  if (!container || !Array.isArray(points) || !points.length) return;
  const overlay = document.createElement("div");
  overlay.className = "chart-line-hover-layer";
  const crosshair = document.createElement("div");
  crosshair.className = "chart-crosshair";
  const tooltip = document.createElement("div");
  tooltip.className = "chart-hover-tooltip";
  overlay.appendChild(crosshair);
  overlay.appendChild(tooltip);
  container.appendChild(overlay);
  const hide = () => overlay.classList.remove("visible");
  const update = clientX => {
    if (chartDragging) {
      hide();
      return;
    }
    const rect = container.getBoundingClientRect();
    if (!rect || rect.width <= 0) return;
    const ratio = clamp((clientX - rect.left) / rect.width, 0, 1);
    const index = Math.round(ratio * Math.max(points.length - 1, 0));
    const point = points[index];
    if (!point || !point.item) return;
    const label = formatChartLabel(point.item, "tooltip") || point.item.label || "";
    tooltip.textContent = `${label} · ${formatMetricTooltip(point.item.value)}`;
    crosshair.style.left = `${point.xPct}%`;
    tooltip.style.left = `${clamp(point.xPct, 8, 92)}%`;
    overlay.classList.add("visible");
  };
  container.addEventListener("pointermove", event => update(event.clientX));
  container.addEventListener("pointerleave", hide);
  container.addEventListener("pointerdown", hide);
}
function buildDateBandTicks(data, trackWidth) {
  if (!Array.isArray(data) || !data.length) return [];
  const total = data.length;
  if (total === 1) return [0];
  const minSpacingPx = activeSeries === "monthly" ? 92 : activeSeries === "weekly" ? 98 : activeChartType === "line" ? 112 : 104;
  const maxTicksByWidth = Number.isFinite(trackWidth) && trackWidth > 0 ? Math.max(2, Math.floor(trackWidth / minSpacingPx)) : 6;
  const target = Math.max(2, Math.min(total, maxTicksByWidth));
  const step = Math.max(1, Math.ceil((total - 1) / Math.max(target - 1, 1)));
  const ticks = [0];
  for (let index = step; index < total - 1; index += step) ticks.push(index);
  if (ticks[ticks.length - 1] !== total - 1) ticks.push(total - 1);
  return ticks;
}
function formatBandLabel(timestamp) {
  if (!Number.isFinite(timestamp)) return "";
  const formatter = chartDateFormatters[activeSeries] || chartDateFormatters.monthly;
  return formatter.format(new Date(timestamp));
}
function renderDateBand(data) {
  var _el, _el$dataset;
  const chartDateBand = el("chartDateBand");
  if (!chartDateBand) return;
  const track = chartDateBand.querySelector(".chart-date-track") || chartDateBand;
  track.innerHTML = "";
  if (!Array.isArray(data) || !data.length) return;
  const total = data.length;
  const trackWidth = track.clientWidth || chartDateBand.clientWidth || 0;
  const ticks = buildDateBandTicks(data, trackWidth);
  const chartMode = String(((_el = el("volumeChart")) === null || _el === void 0 ? void 0 : (_el$dataset = _el.dataset) === null || _el$dataset === void 0 ? void 0 : _el$dataset.chartType) || "");
  const isLineMode = chartMode.startsWith("line");
  const minDistancePx = activeSeries === "monthly" ? 70 : isLineMode ? 92 : 84;
  let lastRight = -Infinity;
  let lastLabel = "";
  ticks.forEach(index => {
    const item = data[index];
    if (!item) return;
    const label = formatBandLabel(item.timestamp) || formatChartLabel(item, "axis");
    if (!label) return;
    const isFirst = index === 0;
    const isLast = index === total - 1;
    if (!isFirst && !isLast && label === lastLabel) return;
    lastLabel = label;
    const pct = total === 1 ? 0.5 : index / (total - 1);
    const px = pct * trackWidth;
    const estimatedWidth = clamp(label.length * 6.4, 36, 92);
    const leftEdge = px - estimatedWidth / 2;
    const rightEdge = px + estimatedWidth / 2;
    if (!isFirst && !isLast && leftEdge < lastRight + minDistancePx * 0.18) return;
    if (isLast && lastRight !== -Infinity && leftEdge < lastRight + minDistancePx * 0.08) return;
    const node = document.createElement("div");
    node.className = "chart-date-marker";
    if (isFirst) node.classList.add("first");
    if (isLast) node.classList.add("last");
    node.style.left = `${pct * 100}%`;
    node.textContent = label;
    track.appendChild(node);
    lastRight = rightEdge;
  });
}
function renderVolumeChart(container, series, presentation = null) {
  if (!container) return;
  container.innerHTML = "";
  container.dataset.series = activeSeries;
  const requestedMode = (presentation === null || presentation === void 0 ? void 0 : presentation.chartMode) || "bar";
  const chartMode = state.analyticsTab === "volume" && activeChartType === "line" ? "line" : requestedMode;
  container.dataset.chartType = chartMode;
  const useLine = chartMode === "line" || chartMode === "line_area" || chartMode === "line_zero";
  const useStacked = chartMode === "stacked_split";
  const useDiverging = chartMode === "diverging";
  const useEventLane = chartMode === "event_lane";
  if (!Array.isArray(series) || !series.length) {
    container.innerHTML = "<div class='muted'>No data yet.</div>";
    renderDateBand([]);
    return;
  }
  const chartWidth = Math.max(320, (container.clientWidth || 720) - 62);
  const shouldAggregate = !(useLine || useStacked || useDiverging || useEventLane);
  const barAggregation = shouldAggregate ? aggregateBarSeriesByWidth(series, chartWidth) : {
    items: series,
    binned: false,
    binSize: 1
  };
  const data = barAggregation.items;
  const baseline = useLine && chartMode !== "line_zero" ? "auto" : "zero";
  const scaleConfig = getScaleConfig(data, {
    baseline
  });
  const chartHeight = container.clientHeight || 280;
  const maxBarHeight = Math.max(chartHeight - 108, 124);
  const zeroRatio = scaleConfig !== null && scaleConfig !== void 0 && scaleConfig.normalize ? scaleConfig.normalize(0) : 0;
  const maxValue = data.reduce((max, item) => Math.max(max, Math.abs(toFiniteNumber(item === null || item === void 0 ? void 0 : item.value, 0))), 0);
  const axis = document.createElement("div");
  axis.className = "chart-axis";
  const bars = document.createElement("div");
  bars.className = "chart-bars";
  if (useEventLane) bars.classList.add("event-lane");
  if (useStacked) bars.classList.add("stacked-mode");
  if (useDiverging) bars.classList.add("diverging-mode");
  if (useLine && chartMode === "line_zero") bars.classList.add("line-zero-mode");
  bars.style.gridTemplateColumns = `repeat(${data.length}, minmax(0, 1fr))`;
  const slotWidth = chartWidth / Math.max(data.length, 1);
  const denseSeries = data.length >= 120 || barAggregation.binned;
  const dynamicGap = clamp(Math.round(slotWidth * (denseSeries ? 0.2 : 0.34)), 1, denseSeries ? 10 : 18);
  const dynamicBarWidth = clamp(Math.round(slotWidth * (denseSeries ? 0.76 : 0.56)), 3, denseSeries ? 12 : 16);
  bars.style.gap = `${dynamicGap}px`;
  bars.style.setProperty("--bar-width", `${dynamicBarWidth}px`);
  if (useLine) bars.classList.add("line-only");
  const ticks = 4;
  for (let i = 0; i <= ticks; i += 1) {
    const ratio = i / ticks;
    const axisValue = scaleConfig.axisAtRatio ? scaleConfig.axisAtRatio(ratio) : ratio;
    const label = document.createElement("div");
    label.className = "axis-label";
    label.style.bottom = `${ratio * 100}%`;
    label.textContent = formatMetricAxis(axisValue);
    axis.appendChild(label);
    const line = document.createElement("div");
    line.className = "chart-grid-line";
    line.style.bottom = `${ratio * 100}%`;
    bars.appendChild(line);
  }
  container.appendChild(axis);
  container.appendChild(bars);
  if (useLine && chartMode === "line_zero" || useDiverging) {
    const zeroLine = document.createElement("div");
    zeroLine.className = "chart-zero-line";
    zeroLine.style.bottom = `${zeroRatio * 100}%`;
    bars.appendChild(zeroLine);
  }
  let hoverPoints = [];
  if (useLine) {
    const lineLayer = document.createElement("div");
    lineLayer.className = "chart-line-layer";
    if (chartMode === "line_zero") lineLayer.classList.add("line-with-zero");
    bars.appendChild(lineLayer);
    hoverPoints = renderCumulativeLine(lineLayer, data, scaleConfig, bars, {
      showArea: chartMode !== "line_zero",
      lineClass: chartMode === "line_zero" ? "chart-line-zero" : chartMode === "line_area" ? "chart-line-oi" : ""
    });
  } else {
    hoverPoints = data.map((item, index) => ({
      xPct: data.length === 1 ? 50 : index / (data.length - 1) * 100,
      item
    }));
  }
  data.forEach((item, index) => {
    if (useLine) return;
    const valueNum = toFiniteNumber(item.value, 0);
    const wrap = document.createElement("div");
    wrap.className = "volume-bar";
    const bar = document.createElement("div");
    bar.className = "bar";
    const labelText = formatChartLabel(item, "tooltip") || (item === null || item === void 0 ? void 0 : item.label) || "-";
    if (useEventLane) {
      wrap.classList.add("event-lane-cell");
      const eventDot = document.createElement("div");
      eventDot.className = "wallet-event-dot";
      const maxDot = Math.max(1, maxValue);
      const intensity = clamp(Math.abs(valueNum) / maxDot, 0, 1);
      const sizePx = clamp(6 + intensity * 10, 6, 16);
      const opacity = clamp(0.42 + intensity * 0.48, 0.42, 0.92);
      eventDot.style.setProperty("--event-size", `${sizePx}px`);
      eventDot.style.setProperty("--event-opacity", `${opacity}`);
      const tooltip = document.createElement("div");
      tooltip.className = "bar-tooltip";
      tooltip.textContent = `${labelText} · ${fmt(valueNum, 0)} trades`;
      eventDot.appendChild(tooltip);
      bar.classList.add("event-lane-bar");
      bar.appendChild(eventDot);
      wrap.appendChild(bar);
    } else if (useStacked) {
      wrap.classList.add("volume-bar-stacked");
      const ratio = maxValue > 0 ? Math.abs(valueNum) / maxValue : 0;
      const height = valueNum > 0 ? Math.max(Math.round(ratio * maxBarHeight), 2) : 0;
      bar.classList.add("stacked");
      bar.style.height = `${height}px`;
      const longValue = Math.max(0, toFiniteNumber(item === null || item === void 0 ? void 0 : item.longValue, 0));
      const shortValue = Math.max(0, toFiniteNumber(item === null || item === void 0 ? void 0 : item.shortValue, 0));
      const splitTotal = Math.max(longValue + shortValue, 1);
      const longHeight = Math.round(height * longValue / splitTotal);
      const shortHeight = Math.max(0, height - longHeight);
      const segShort = document.createElement("span");
      segShort.className = "stack-seg short";
      segShort.style.height = `${shortHeight}px`;
      bar.appendChild(segShort);
      const segLong = document.createElement("span");
      segLong.className = "stack-seg long";
      segLong.style.height = `${longHeight}px`;
      bar.appendChild(segLong);
      const tooltip = document.createElement("div");
      tooltip.className = "bar-tooltip";
      tooltip.textContent = `${labelText} · long ${formatMetricTooltip(longValue)} · short ${formatMetricTooltip(shortValue)}`;
      bar.appendChild(tooltip);
      wrap.appendChild(bar);
    } else if (useDiverging) {
      wrap.classList.add("volume-bar-diverging");
      bar.classList.add("diverging");
      const ratio = scaleConfig.normalize ? scaleConfig.normalize(valueNum) : 0;
      const deltaRatio = Math.abs(ratio - zeroRatio);
      const height = Math.max(Math.round(deltaRatio * maxBarHeight), valueNum === 0 ? 0 : 2);
      const zeroBottomPx = Math.round(zeroRatio * maxBarHeight);
      const segment = document.createElement("span");
      segment.className = `diverge-seg ${valueNum >= 0 ? "positive" : "negative"}`;
      segment.style.height = `${height}px`;
      segment.style.bottom = valueNum >= 0 ? `${zeroBottomPx}px` : `${Math.max(zeroBottomPx - height, 0)}px`;
      bar.appendChild(segment);
      const tooltip = document.createElement("div");
      tooltip.className = "bar-tooltip";
      tooltip.textContent = `${labelText} · ${formatMetricTooltip(valueNum)}`;
      bar.appendChild(tooltip);
      wrap.appendChild(bar);
    } else {
      const ratio = scaleConfig.normalize ? scaleConfig.normalize(valueNum) : 0;
      const height = valueNum > 0 ? Math.max(Math.round(ratio * maxBarHeight), 2) : 0;
      bar.style.height = `${height}px`;
      const tooltip = document.createElement("div");
      tooltip.className = "bar-tooltip";
      tooltip.textContent = `${labelText} · ${formatMetricTooltip(valueNum)}`;
      bar.appendChild(tooltip);
      wrap.appendChild(bar);
    }
    const label = document.createElement("div");
    label.className = `bar-label${shouldShowAxisLabel(index, data.length) ? "" : " dim"}`;
    label.textContent = shouldShowAxisLabel(index, data.length) && !useEventLane ? formatChartLabel(item, "axis") : "";
    wrap.appendChild(label);
    bars.appendChild(wrap);
  });
  attachLineHoverOverlay(bars, hoverPoints);
  renderDateBand(data);
}
function getVolumeSeriesForWindow() {
  const metricSet = state.volumeSeries && state.volumeSeries.volume ? state.volumeSeries.volume : null;
  if (!metricSet) return [];
  return Array.isArray(metricSet[activeSeries]) ? metricSet[activeSeries] : [];
}
function getActiveSeries() {
  const tab = normalizeAnalyticsTab(state.analyticsTab || TOKEN_DEFAULT_METRIC);
  if (state.view === "token") {
    return getTokenActiveSeries(tab);
  }
  if (tab === "volume") return getVolumeSeriesForWindow();
  const limit = getSnapshotLimitForSeries();
  if (tab === "oi") {
    return buildSymbolSnapshotSeries(row => toFiniteNumber(row === null || row === void 0 ? void 0 : row.open_interest, 0) * toFiniteNumber(row === null || row === void 0 ? void 0 : row.mark, 0), {
      limit
    });
  }
  if (tab === "funding") {
    return buildSymbolSnapshotSeries(row => toFiniteNumber(row === null || row === void 0 ? void 0 : row.funding, 0) * 100, {
      absSort: true,
      limit
    });
  }
  if (tab === "liquidations") {
    var _state$exchange2, _state$exchange2$sour;
    const prices = Array.isArray((_state$exchange2 = state.exchange) === null || _state$exchange2 === void 0 ? void 0 : (_state$exchange2$sour = _state$exchange2.source) === null || _state$exchange2$sour === void 0 ? void 0 : _state$exchange2$sour.prices) ? state.exchange.source.prices : [];
    return prices.map(row => {
      const symbol = String((row === null || row === void 0 ? void 0 : row.symbol) || "").trim();
      if (!symbol) return null;
      const funding = toFiniteNumber(row === null || row === void 0 ? void 0 : row.funding, 0);
      const openInterest = toFiniteNumber(row === null || row === void 0 ? void 0 : row.open_interest, 0);
      const mark = toFiniteNumber(row === null || row === void 0 ? void 0 : row.mark, 0);
      const pressure = Math.abs(funding) * openInterest * mark;
      if (!Number.isFinite(pressure)) return null;
      const longValue = funding >= 0 ? pressure : 0;
      const shortValue = funding < 0 ? pressure : 0;
      return {
        label: symbol,
        value: pressure,
        longValue,
        shortValue,
        funding
      };
    }).filter(Boolean).sort((a, b) => Math.abs(toFiniteNumber(b.value, 0)) - Math.abs(toFiniteNumber(a.value, 0))).slice(0, limit);
  }
  if (tab === "netflow") {
    return buildSymbolSnapshotSeries(row => {
      const mark = toFiniteNumber(row === null || row === void 0 ? void 0 : row.mark, 0);
      const y = toFiniteNumber(row === null || row === void 0 ? void 0 : row.yesterday_price, mark);
      const oi = toFiniteNumber(row === null || row === void 0 ? void 0 : row.open_interest, 0);
      return (mark - y) * oi;
    }, {
      absSort: true,
      limit
    });
  }
  if (tab === "wallet_activity") {
    var _state$wallets;
    const rows = Array.isArray((_state$wallets = state.wallets) === null || _state$wallets === void 0 ? void 0 : _state$wallets.rows) ? state.wallets.rows : [];
    const now = Date.now();
    const horizonMs = activeSeries === "daily" ? 24 * 60 * 60 * 1000 : activeSeries === "weekly" ? 7 * 24 * 60 * 60 * 1000 : 30 * 24 * 60 * 60 * 1000;
    const mapped = rows.map(row => {
      const wallet = String((row === null || row === void 0 ? void 0 : row.wallet) || "");
      const shortWallet = wallet ? `${wallet.slice(0, 4)}…${wallet.slice(-4)}` : "unknown";
      const rawLastTrade = (row === null || row === void 0 ? void 0 : row.lastTrade) !== undefined ? row.lastTrade : row === null || row === void 0 ? void 0 : row.last_trade;
      let lastTradeTs = toFiniteNumber(rawLastTrade, NaN);
      if (!Number.isFinite(lastTradeTs) && typeof rawLastTrade === "string") {
        const parsed = Date.parse(rawLastTrade);
        if (Number.isFinite(parsed)) lastTradeTs = parsed;
      }
      return {
        label: shortWallet,
        value: toFiniteNumber(row === null || row === void 0 ? void 0 : row.trades, 0),
        timestamp: Number.isFinite(lastTradeTs) && lastTradeTs > 0 ? lastTradeTs : null
      };
    }).filter(row => row.timestamp !== null).sort((a, b) => toFiniteNumber(a.timestamp, 0) - toFiniteNumber(b.timestamp, 0));
    const windowed = mapped.filter(row => {
      const ts = toFiniteNumber(row.timestamp, 0);
      return ts >= now - horizonMs && ts <= now + 60000;
    });
    const selected = windowed.length >= 6 ? windowed : mapped;
    return selected.slice(-Math.max(10, limit));
  }
  return getVolumeSeriesForWindow();
}
function syncChartWindow(series) {
  const total = Array.isArray(series) ? series.length : 0;
  if (!total) {
    chartState.window = 0;
    chartState.offset = 0;
    return;
  }
  const minWindow = Math.min(6, total);
  const maxWindow = total;
  if (!Number.isFinite(chartState.window) || chartState.window < minWindow) chartState.window = minWindow;
  if (chartState.window > maxWindow) chartState.window = maxWindow;
  if (!chartHasUserControl) {
    chartState.window = maxWindow;
    chartState.offset = 0;
    return;
  }
  const maxOffset = Math.max(total - chartState.window, 0);
  chartState.offset = clamp(chartState.offset, 0, maxOffset);
}
function getVisibleSeries(series) {
  if (!Array.isArray(series) || !series.length) return [];
  const start = Math.max(0, Math.min(chartState.offset, series.length - 1));
  const end = Math.min(start + chartState.window, series.length);
  return series.slice(start, end);
}
function getChartBounds() {
  const volumeChart = el("volumeChart");
  if (!volumeChart) return null;
  return volumeChart.getBoundingClientRect();
}
function getNavigatorBounds() {
  const chartNavigator = el("chartNavigator");
  if (!chartNavigator) return null;
  return chartNavigator.getBoundingClientRect();
}
function renderNavigatorArea(series) {
  if (!ENABLE_CHART_BRUSH) return;
  const navigatorBars = el("navigatorBars");
  if (!navigatorBars) return;
  navigatorBars.innerHTML = "";
  if (!Array.isArray(series) || !series.length) return;
  const values = series.map(item => Math.abs(getSeriesValue(item)));
  const max = Math.max(...values, 1);
  const bounds = navigatorBars.getBoundingClientRect();
  const plotWidth = Math.max(1, Math.round(navigatorBars.clientWidth || bounds.width || 0));
  const plotHeight = Math.max(1, Math.round(navigatorBars.clientHeight || bounds.height || 0));
  const topInset = Math.max(1, Math.round(plotHeight * 0.1));
  const bottomInset = Math.max(topInset + 1, Math.round(plotHeight * 0.9));
  const drawable = bottomInset - topInset;
  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("viewBox", `0 0 ${plotWidth} ${plotHeight}`);
  svg.setAttribute("preserveAspectRatio", "xMinYMin meet");
  svg.setAttribute("width", String(plotWidth));
  svg.setAttribute("height", String(plotHeight));
  svg.classList.add("navigator-area-svg");
  const points = values.map((valueNum, index) => {
    const x = values.length === 1 ? plotWidth / 2 : index / (values.length - 1) * plotWidth;
    const ratio = max ? valueNum / max : 0;
    const y = bottomInset - ratio * drawable;
    return {
      x,
      y
    };
  });
  const linePathData = buildSmoothLinePath(points);
  const firstPoint = points[0];
  const lastPoint = points[points.length - 1];
  const areaPathData = `${linePathData} L ${lastPoint.x.toFixed(3)} ${plotHeight.toFixed(3)} L ${firstPoint.x.toFixed(3)} ${plotHeight.toFixed(3)} Z`;
  const areaPath = document.createElementNS("http://www.w3.org/2000/svg", "path");
  areaPath.setAttribute("d", areaPathData);
  areaPath.classList.add("navigator-area-fill");
  svg.appendChild(areaPath);
  const linePath = document.createElementNS("http://www.w3.org/2000/svg", "path");
  linePath.setAttribute("d", linePathData);
  linePath.classList.add("navigator-area-line");
  svg.appendChild(linePath);
  navigatorBars.appendChild(svg);
}
function updateNavigatorWindow(series) {
  if (!ENABLE_CHART_BRUSH) return;
  const chartNavigator = el("chartNavigator");
  const navigatorWindow = el("navigatorWindow");
  const navigatorMaskLeft = el("navigatorMaskLeft");
  const navigatorMaskRight = el("navigatorMaskRight");
  if (!chartNavigator || !navigatorWindow || !navigatorMaskLeft || !navigatorMaskRight) return;
  const total = Array.isArray(series) ? series.length : 0;
  if (!total) {
    navigatorWindow.style.width = "0";
    navigatorMaskLeft.style.width = "0";
    navigatorMaskRight.style.width = "0";
    return;
  }
  const start = chartState.offset;
  const end = Math.min(start + chartState.window, total);
  const leftPct = start / total * 100;
  const widthPct = (end - start) / total * 100;
  navigatorWindow.style.left = `${leftPct}%`;
  navigatorWindow.style.width = `${widthPct}%`;
  navigatorMaskLeft.style.width = `${leftPct}%`;
  navigatorMaskRight.style.width = `${100 - leftPct - widthPct}%`;
}
function renderNavigator(series) {
  const chartNavigator = el("chartNavigator");
  if (!chartNavigator) return;
  if (!ENABLE_CHART_BRUSH) {
    chartNavigator.hidden = true;
    return;
  }
  chartNavigator.hidden = false;
  chartNavigator.classList.toggle("minimal", activeSeries === "daily");
  renderNavigatorArea(series);
  updateNavigatorWindow(series);
}
function handleChartResize() {
  if (chartResizeRaf) window.cancelAnimationFrame(chartResizeRaf);
  chartResizeRaf = window.requestAnimationFrame(() => {
    chartResizeRaf = 0;
    renderActiveSeries();
  });
}
function queueChartViewportResize() {
  if (chartViewportResizeRaf) window.cancelAnimationFrame(chartViewportResizeRaf);
  chartViewportResizeRaf = window.requestAnimationFrame(() => {
    chartViewportResizeRaf = 0;
    handleChartResize();
  });
}
function initChartResizeObserver() {
  const volumeChart = el("volumeChart");
  const chartDateBand = el("chartDateBand");
  const chartNavigator = el("chartNavigator");
  if (!volumeChart || typeof ResizeObserver !== "function") return;
  if (chartResizeObserver) {
    chartResizeObserver.disconnect();
  }
  const targets = [volumeChart, volumeChart.parentElement, chartDateBand, chartNavigator].filter(Boolean);
  const seen = new Set();
  chartResizeObserver = new ResizeObserver(() => {
    handleChartResize();
  });
  targets.forEach(target => {
    if (seen.has(target)) return;
    seen.add(target);
    chartResizeObserver.observe(target);
  });
}
function handleChartWheel(event) {
  const volumeChart = el("volumeChart");
  if (!volumeChart || !state.volumeSeries) return;
  const series = getActiveSeries();
  if (!series.length) return;
  event.preventDefault();
  chartHasUserControl = true;
  const chartNavigator = el("chartNavigator");
  const rect = chartNavigator && event.currentTarget === chartNavigator ? getNavigatorBounds() : getChartBounds();
  if (!rect || rect.width === 0) return;
  const ratio = clamp((event.clientX - rect.left) / rect.width, 0, 1);
  const maxOffset = Math.max(series.length - chartState.window, 0);
  if (event.shiftKey || Math.abs(event.deltaX) > Math.abs(event.deltaY)) {
    const panDelta = event.deltaX !== 0 ? event.deltaX : event.deltaY;
    const pixelsPerBar = rect.width / Math.max(chartState.window, 1);
    const barsShift = Math.round(panDelta / pixelsPerBar);
    chartState.offset = clamp(chartState.offset + barsShift, 0, maxOffset);
    renderActiveSeries();
    return;
  }
  const focusIndex = chartState.offset + Math.round(ratio * Math.max(chartState.window - 1, 0));
  const delta = Math.sign(event.deltaY);
  const step = event.ctrlKey ? 1 : Math.max(1, Math.round(chartState.window * 0.08));
  const minWindow = Math.min(6, series.length);
  const maxWindow = Math.max(series.length, minWindow);
  let nextWindow = chartState.window + (delta > 0 ? step : -step);
  nextWindow = clamp(nextWindow, minWindow, maxWindow);
  if (nextWindow === chartState.window) return;
  const nextMaxOffset = Math.max(series.length - nextWindow, 0);
  const nextOffset = clamp(focusIndex - Math.round(ratio * Math.max(nextWindow - 1, 0)), 0, nextMaxOffset);
  chartState.window = nextWindow;
  chartState.offset = nextOffset;
  renderActiveSeries();
}
function handleChartPointerDown(event) {
  const volumeChart = el("volumeChart");
  if (!volumeChart || !state.volumeSeries) return;
  const series = getActiveSeries();
  if (!series.length) return;
  if (event.button !== undefined && event.button !== 0) return;
  chartDragging = true;
  chartHasUserControl = true;
  chartDragStartX = event.clientX;
  chartDragStartOffset = chartState.offset;
  volumeChart.classList.add("dragging");
  document.body.classList.add("chart-dragging");
  if (volumeChart.setPointerCapture && event.pointerId !== undefined) {
    volumeChart.setPointerCapture(event.pointerId);
  }
}
function handleChartPointerMove(event) {
  const volumeChart = el("volumeChart");
  if (!chartDragging || !volumeChart || !state.volumeSeries) return;
  const series = getActiveSeries();
  if (!series.length) return;
  const rect = getChartBounds();
  if (!rect || rect.width === 0) return;
  const pixelsPerBar = rect.width / Math.max(chartState.window, 1);
  const deltaX = event.clientX - chartDragStartX;
  const barsShift = Math.round(deltaX / pixelsPerBar);
  const maxOffset = Math.max(series.length - chartState.window, 0);
  chartState.offset = clamp(chartDragStartOffset - barsShift, 0, maxOffset);
  renderActiveSeries();
}
function handleChartPointerUp(event) {
  const volumeChart = el("volumeChart");
  if (!chartDragging) return;
  chartDragging = false;
  if (volumeChart) volumeChart.classList.remove("dragging");
  document.body.classList.remove("chart-dragging");
  if (volumeChart && volumeChart.releasePointerCapture && event.pointerId !== undefined) {
    volumeChart.releasePointerCapture(event.pointerId);
  }
}
function handleNavigatorPointerDown(event) {
  var _handle$dataset, _handle$dataset2;
  const chartNavigator = el("chartNavigator");
  if (!chartNavigator || !state.volumeSeries) return;
  const series = getActiveSeries();
  if (!series.length) return;
  if (event.button !== undefined && event.button !== 0) return;
  const rect = getNavigatorBounds();
  if (!rect || rect.width === 0) return;
  chartHasUserControl = true;
  const pointerTarget = event.target instanceof Element ? event.target : null;
  const handle = pointerTarget ? pointerTarget.closest(".navigator-handle") : null;
  if ((handle === null || handle === void 0 ? void 0 : (_handle$dataset = handle.dataset) === null || _handle$dataset === void 0 ? void 0 : _handle$dataset.handle) === "left") {
    navigatorState.mode = "resize-left";
  } else if ((handle === null || handle === void 0 ? void 0 : (_handle$dataset2 = handle.dataset) === null || _handle$dataset2 === void 0 ? void 0 : _handle$dataset2.handle) === "right") {
    navigatorState.mode = "resize-right";
  } else {
    navigatorState.mode = "pan";
  }
  const total = series.length;
  const clickRatio = clamp((event.clientX - rect.left) / rect.width, 0, 1);
  const clickIndex = Math.round(clickRatio * (total - 1));
  const windowStart = chartState.offset;
  const windowEnd = windowStart + Math.max(chartState.window - 1, 0);
  if (!handle && (clickIndex < windowStart || clickIndex > windowEnd)) {
    const maxOffset = Math.max(total - chartState.window, 0);
    chartState.offset = clamp(clickIndex - Math.floor(chartState.window / 2), 0, maxOffset);
    renderActiveSeries();
  }
  navigatorState.startX = event.clientX;
  navigatorState.startOffset = chartState.offset;
  navigatorState.startWindow = chartState.window;
  chartNavigator.classList.add("dragging");
  document.body.classList.add("chart-dragging");
  if (chartNavigator.setPointerCapture && event.pointerId !== undefined) {
    chartNavigator.setPointerCapture(event.pointerId);
  }
}
function handleNavigatorPointerMove(event) {
  const chartNavigator = el("chartNavigator");
  if (!navigatorState.mode || !chartNavigator || !state.volumeSeries) return;
  const series = getActiveSeries();
  if (!series.length) return;
  const rect = getNavigatorBounds();
  if (!rect || rect.width === 0) return;
  event.preventDefault();
  const total = series.length;
  const minWindow = Math.min(6, total);
  const deltaX = event.clientX - navigatorState.startX;
  const barsShift = Math.round(deltaX / rect.width * total);
  if (navigatorState.mode === "pan") {
    const maxOffset = Math.max(total - chartState.window, 0);
    chartState.offset = clamp(navigatorState.startOffset + barsShift, 0, maxOffset);
  } else if (navigatorState.mode === "resize-left") {
    const rightEdge = navigatorState.startOffset + navigatorState.startWindow;
    const nextOffset = clamp(navigatorState.startOffset + barsShift, 0, rightEdge - minWindow);
    const nextWindow = clamp(rightEdge - nextOffset, minWindow, total);
    chartState.offset = nextOffset;
    chartState.window = nextWindow;
  } else if (navigatorState.mode === "resize-right") {
    const maxWindow = Math.max(total - navigatorState.startOffset, minWindow);
    const nextWindow = clamp(navigatorState.startWindow + barsShift, minWindow, maxWindow);
    chartState.window = nextWindow;
    chartState.offset = clamp(navigatorState.startOffset, 0, Math.max(total - nextWindow, 0));
  }
  renderActiveSeries();
}
function handleNavigatorPointerUp(event) {
  const chartNavigator = el("chartNavigator");
  if (!navigatorState.mode) return;
  navigatorState.mode = null;
  if (chartNavigator) chartNavigator.classList.remove("dragging");
  document.body.classList.remove("chart-dragging");
  if (chartNavigator && chartNavigator.releasePointerCapture && event.pointerId !== undefined) {
    chartNavigator.releasePointerCapture(event.pointerId);
  }
}
function syncCumulativeControl() {
  const isVolumeTab = state.analyticsTab === "volume";
  const cumulativeActive = isVolumeTab && activeChartType === "line";
  const buttons = [["seriesDaily", "daily"], ["seriesWeekly", "weekly"], ["seriesMonthly", "monthly"]];
  const toggleGroup = document.querySelector(".chart-toggle-group");
  if (toggleGroup) {
    toggleGroup.style.display = "inline-flex";
  }
  buttons.forEach(([id, key]) => {
    const btn = el(id);
    if (!btn) return;
    const isActive = !cumulativeActive && key === activeSeries;
    btn.classList.toggle("active", isActive);
    btn.setAttribute("aria-pressed", isActive ? "true" : "false");
    btn.disabled = false;
  });
  const cumulativeBtn = el("chartCumulative");
  if (cumulativeBtn) {
    cumulativeBtn.hidden = !isVolumeTab;
    cumulativeBtn.classList.toggle("active", isVolumeTab && cumulativeActive);
    cumulativeBtn.setAttribute("aria-pressed", isVolumeTab && cumulativeActive ? "true" : "false");
    cumulativeBtn.disabled = !isVolumeTab;
  }
}
function renderAnalyticsLegend(presentation = null) {
  const host = el("analytics-legend");
  if (!host) return;
  const legendItems = Array.isArray(presentation === null || presentation === void 0 ? void 0 : presentation.legend) ? presentation.legend : [];
  if (!legendItems.length) {
    host.innerHTML = "";
    host.hidden = true;
    return;
  }
  host.hidden = false;
  host.innerHTML = legendItems.map(item => `<span class="legend-item ${escapeHtml((item === null || item === void 0 ? void 0 : item.tone) || "default")}"><i></i>${escapeHtml((item === null || item === void 0 ? void 0 : item.label) || "-")}</span>`).join("");
}
function renderActiveSeries() {
  const container = el("volumeChart");
  if (!container) return;
  const allSeries = getActiveSeries();
  const presentation = getAnalyticsPresentation();
  setText("chartTitle", presentation.title);
  setText("analytics-note", presentation.note);
  renderAnalyticsLegend(presentation);
  syncChartWindow(allSeries);
  const prepared = buildChartData(allSeries);
  const visiblePrepared = getVisibleSeries(prepared);
  const useCumulative = state.analyticsTab === "volume" && activeChartType === "line";
  const chartData = useCumulative ? buildCumulativeSeries(visiblePrepared) : visiblePrepared;
  renderVolumeChart(container, chartData, presentation);
  renderNavigator(allSeries);
  syncCumulativeControl();
}
function updateSeriesToggle(seriesKey) {
  if (!["daily", "weekly", "monthly"].includes(seriesKey)) return;
  activeSeries = seriesKey;
  activeChartType = "bars";
  chartHasUserControl = false;
  renderActiveSeries();
}
function updateChartType(type) {
  if (type !== "line") return;
  activeSeries = "daily";
  activeChartType = "line";
  chartHasUserControl = false;
  renderActiveSeries();
}
function el(id) {
  return document.getElementById(id);
}
function setText(id, value) {
  const node = el(id);
  if (node) node.textContent = value;
}
function getIndexerBreakdown(indexer = null) {
  var _ref, _lifecycle$discovered, _ref2, _lifecycle$pendingBac, _ref3, _lifecycle$backfillin, _ref4, _lifecycle$fullyIndex, _lifecycle$liveTracki, _ref5, _lifecycle$backfillCo, _indexer$failedWallet, _ref6, _lifecycle$failedBack, _ref7, _indexer$knownWallets;
  const lifecycle = (indexer === null || indexer === void 0 ? void 0 : indexer.lifecycle) || {};
  const discovered = Number((_ref = (_lifecycle$discovered = lifecycle === null || lifecycle === void 0 ? void 0 : lifecycle.discovered) !== null && _lifecycle$discovered !== void 0 ? _lifecycle$discovered : indexer === null || indexer === void 0 ? void 0 : indexer.knownWallets) !== null && _ref !== void 0 ? _ref : 0);
  const pendingBackfill = Number((_ref2 = (_lifecycle$pendingBac = lifecycle === null || lifecycle === void 0 ? void 0 : lifecycle.pendingBackfill) !== null && _lifecycle$pendingBac !== void 0 ? _lifecycle$pendingBac : indexer === null || indexer === void 0 ? void 0 : indexer.pendingWallets) !== null && _ref2 !== void 0 ? _ref2 : 0);
  const backfilling = Number((_ref3 = (_lifecycle$backfillin = lifecycle === null || lifecycle === void 0 ? void 0 : lifecycle.backfilling) !== null && _lifecycle$backfillin !== void 0 ? _lifecycle$backfillin : indexer === null || indexer === void 0 ? void 0 : indexer.partiallyIndexedWallets) !== null && _ref3 !== void 0 ? _ref3 : 0);
  const fullyIndexed = Number((_ref4 = (_lifecycle$fullyIndex = lifecycle === null || lifecycle === void 0 ? void 0 : lifecycle.fullyIndexed) !== null && _lifecycle$fullyIndex !== void 0 ? _lifecycle$fullyIndex : indexer === null || indexer === void 0 ? void 0 : indexer.indexedCompleteWallets) !== null && _ref4 !== void 0 ? _ref4 : 0);
  const liveTracking = Number((_lifecycle$liveTracki = lifecycle === null || lifecycle === void 0 ? void 0 : lifecycle.liveTracking) !== null && _lifecycle$liveTracki !== void 0 ? _lifecycle$liveTracki : 0);
  const indexed = Number((_ref5 = (_lifecycle$backfillCo = lifecycle === null || lifecycle === void 0 ? void 0 : lifecycle.backfillComplete) !== null && _lifecycle$backfillCo !== void 0 ? _lifecycle$backfillCo : indexer === null || indexer === void 0 ? void 0 : indexer.indexedCompleteWallets) !== null && _ref5 !== void 0 ? _ref5 : 0);
  const partial = backfilling;
  const pending = pendingBackfill;
  const failed = Number((_indexer$failedWallet = indexer === null || indexer === void 0 ? void 0 : indexer.failedWallets) !== null && _indexer$failedWallet !== void 0 ? _indexer$failedWallet : 0);
  const failedBackfill = Number((_ref6 = (_lifecycle$failedBack = lifecycle === null || lifecycle === void 0 ? void 0 : lifecycle.failedBackfill) !== null && _lifecycle$failedBack !== void 0 ? _lifecycle$failedBack : indexer === null || indexer === void 0 ? void 0 : indexer.failedBackfillWallets) !== null && _ref6 !== void 0 ? _ref6 : failed);
  const knownFallback = indexed + partial + pending + failed;
  const known = Number((_ref7 = (_indexer$knownWallets = indexer === null || indexer === void 0 ? void 0 : indexer.knownWallets) !== null && _indexer$knownWallets !== void 0 ? _indexer$knownWallets : discovered) !== null && _ref7 !== void 0 ? _ref7 : knownFallback);
  const completePct = known > 0 ? indexed / known * 100 : 0;
  return {
    indexed,
    partial,
    pending,
    failed,
    known,
    discovered,
    pendingBackfill,
    backfilling,
    fullyIndexed,
    liveTracking,
    failedBackfill,
    completePct,
    backlog: pendingBackfill + backfilling + failedBackfill
  };
}
async function fetchJson(url, options = {}) {
  var _options$timeoutMs;
  const timeoutMs = Number((_options$timeoutMs = options.timeoutMs) !== null && _options$timeoutMs !== void 0 ? _options$timeoutMs : 10000);
  const controller = typeof AbortController !== "undefined" ? new AbortController() : null;
  const timer = controller && Number.isFinite(timeoutMs) && timeoutMs > 0 ? window.setTimeout(() => controller.abort(), timeoutMs) : null;
  let res;
  try {
    res = await fetch(url, {
      cache: "no-store",
      signal: controller ? controller.signal : undefined
    });
  } catch (error) {
    if (timer) window.clearTimeout(timer);
    if (error && error.name === "AbortError") {
      throw new Error(`${url} -> timeout`);
    }
    throw error;
  }
  if (timer) window.clearTimeout(timer);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.error || `${url} -> ${res.status}`);
  }
  return data;
}
async function postJson(url, payload = {}, options = {}) {
  var _options$timeoutMs2;
  const timeoutMs = Number((_options$timeoutMs2 = options.timeoutMs) !== null && _options$timeoutMs2 !== void 0 ? _options$timeoutMs2 : 15000);
  const controller = typeof AbortController !== "undefined" ? new AbortController() : null;
  const timer = controller && Number.isFinite(timeoutMs) && timeoutMs > 0 ? window.setTimeout(() => controller.abort(), timeoutMs) : null;
  let res;
  try {
    res = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(payload),
      cache: "no-store",
      signal: controller ? controller.signal : undefined
    });
  } catch (error) {
    if (timer) window.clearTimeout(timer);
    if (error && error.name === "AbortError") {
      throw new Error(`${url} -> timeout`);
    }
    throw error;
  }
  if (timer) window.clearTimeout(timer);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.error || `${url} -> ${res.status}`);
  }
  return data;
}
function renderStatusBars() {
  var _ex$sync, _ref8, _indexer$liveGroupSiz, _indexer$liveStaleWal, _indexer$successfulSc, _indexer$failedScans, _indexer$liveAverageA, _indexer$priorityQueu, _indexer$liveQueueSiz, _indexer$restClients$, _indexer$restClients, _indexer$liveQueueSiz2, _indexer$priorityQueu2, _indexer$backlogMode$, _indexer$backlogMode, _indexer$backlogMode2;
  const ex = state.exchange || {};
  const indexer = (ex === null || ex === void 0 ? void 0 : ex.indexer) || null;
  const progress = getIndexerBreakdown(indexer);
  const wsStatus = (ex === null || ex === void 0 ? void 0 : (_ex$sync = ex.sync) === null || _ex$sync === void 0 ? void 0 : _ex$sync.wsStatus) || "unknown";
  const generatedAt = Number((ex === null || ex === void 0 ? void 0 : ex.generatedAt) || 0);
  const liveGroup = Number((_ref8 = (_indexer$liveGroupSiz = indexer === null || indexer === void 0 ? void 0 : indexer.liveGroupSize) !== null && _indexer$liveGroupSiz !== void 0 ? _indexer$liveGroupSiz : progress.liveTracking) !== null && _ref8 !== void 0 ? _ref8 : 0);
  const staleWallets = Number((_indexer$liveStaleWal = indexer === null || indexer === void 0 ? void 0 : indexer.liveStaleWallets) !== null && _indexer$liveStaleWal !== void 0 ? _indexer$liveStaleWal : 0);
  const staleRatio = liveGroup > 0 ? staleWallets / liveGroup : 0;
  const successfulScans = Number((_indexer$successfulSc = indexer === null || indexer === void 0 ? void 0 : indexer.successfulScans) !== null && _indexer$successfulSc !== void 0 ? _indexer$successfulSc : 0);
  const failedScans = Number((_indexer$failedScans = indexer === null || indexer === void 0 ? void 0 : indexer.failedScans) !== null && _indexer$failedScans !== void 0 ? _indexer$failedScans : 0);
  const scanAttempts = successfulScans + failedScans;
  const refreshSuccessPct = scanAttempts > 0 ? successfulScans / scanAttempts * 100 : 0;
  const avgAgeMs = Number((_indexer$liveAverageA = indexer === null || indexer === void 0 ? void 0 : indexer.liveAverageAgeMs) !== null && _indexer$liveAverageA !== void 0 ? _indexer$liveAverageA : 0);
  const queuePressure = Number((_indexer$priorityQueu = indexer === null || indexer === void 0 ? void 0 : indexer.priorityQueueSize) !== null && _indexer$priorityQueu !== void 0 ? _indexer$priorityQueu : 0) + Number((_indexer$liveQueueSiz = indexer === null || indexer === void 0 ? void 0 : indexer.liveQueueSize) !== null && _indexer$liveQueueSiz !== void 0 ? _indexer$liveQueueSiz : 0);
  const topError = Array.isArray(indexer === null || indexer === void 0 ? void 0 : indexer.topErrorReasons) && indexer.topErrorReasons.length ? indexer.topErrorReasons[0] : null;
  const activeEgress = Number((_indexer$restClients$ = indexer === null || indexer === void 0 ? void 0 : (_indexer$restClients = indexer.restClients) === null || _indexer$restClients === void 0 ? void 0 : _indexer$restClients.count) !== null && _indexer$restClients$ !== void 0 ? _indexer$restClients$ : 0);
  const alertCount = Number(staleRatio > 0.35) + Number(refreshSuccessPct < 70) + Number(queuePressure > 2000) + Number(progress.failedBackfill > 0) + Number(Boolean(indexer === null || indexer === void 0 ? void 0 : indexer.lastError));
  setText("exchange-live-label", wsStatus === "open" ? "LIVE" : "SYNCING");
  setText("status-refresh-age", `Data age: ${fmtAgo(generatedAt)}`);
  setText("status-latency", `Latency: ${fmtDurationMs(avgAgeMs)}`);
  setText("status-alerts", `Alerts: ${fmt(alertCount, 0)}`);
  setText("ops-freshness-state", staleRatio > 0.45 ? "degraded" : staleRatio > 0.2 ? "warming" : "healthy");
  setText("ops-health-state", refreshSuccessPct >= 85 ? "stable" : refreshSuccessPct >= 70 ? "warning" : "critical");
  setText("ops-queue-pressure", fmt(queuePressure, 0));
  setText("ops-active-egress", fmt(activeEgress, 0));
  setText("ops-top-error", topError ? `${topError.reason} (${fmt(topError.count, 0)})` : "none");
  const liveQueue = Number((_indexer$liveQueueSiz2 = indexer === null || indexer === void 0 ? void 0 : indexer.liveQueueSize) !== null && _indexer$liveQueueSiz2 !== void 0 ? _indexer$liveQueueSiz2 : 0);
  const backfillQueue = Number((_indexer$priorityQueu2 = indexer === null || indexer === void 0 ? void 0 : indexer.priorityQueueSize) !== null && _indexer$priorityQueu2 !== void 0 ? _indexer$priorityQueu2 : 0);
  const liveQueueTarget = Math.max(liveGroup || 1, 1500);
  const backfillQueueTarget = Math.max(Number((_indexer$backlogMode$ = indexer === null || indexer === void 0 ? void 0 : (_indexer$backlogMode = indexer.backlogMode) === null || _indexer$backlogMode === void 0 ? void 0 : _indexer$backlogMode.thresholdWallets) !== null && _indexer$backlogMode$ !== void 0 ? _indexer$backlogMode$ : 2000), 2000);
  const liveFill = clamp(liveQueue / liveQueueTarget * 100, 0, 100);
  const backfillFill = clamp(backfillQueue / backfillQueueTarget * 100, 0, 100);
  const liveQueueFillNode = el("ops-live-queue-fill");
  if (liveQueueFillNode) liveQueueFillNode.style.width = `${liveFill}%`;
  const backfillQueueFillNode = el("ops-backfill-queue-fill");
  if (backfillQueueFillNode) backfillQueueFillNode.style.width = `${backfillFill}%`;
  setText("ops-live-queue-label", `${fmt(liveQueue, 0)} / ${fmt(liveQueueTarget, 0)}`);
  setText("ops-backfill-queue-label", `${fmt(backfillQueue, 0)} / ${fmt(backfillQueueTarget, 0)}`);
  setText("ops-queue-mode", indexer !== null && indexer !== void 0 && (_indexer$backlogMode2 = indexer.backlogMode) !== null && _indexer$backlogMode2 !== void 0 && _indexer$backlogMode2.active ? "backlog mode" : "normal");
  const anomalies = [];
  if (staleRatio > 0.35) anomalies.push(`Stale ratio elevated (${fmt(staleRatio * 100, 1)}%).`);
  if (refreshSuccessPct < 75) anomalies.push(`Refresh success low (${fmt(refreshSuccessPct, 1)}%).`);
  if (backfillQueue > backfillQueueTarget * 0.6) anomalies.push(`Backfill queue pressure high (${fmt(backfillQueue, 0)}).`);
  if ((topError === null || topError === void 0 ? void 0 : topError.count) > 0) anomalies.push(`Top error: ${topError.reason} (${fmt(topError.count, 0)}).`);
  const anomalyList = el("ops-anomaly-list");
  if (anomalyList) {
    anomalyList.innerHTML = anomalies.length ? anomalies.slice(0, 5).map(item => `<li><span class="anomaly-time">now</span><span class="anomaly-msg">${escapeHtml(item)}</span></li>`).join("") : "<li><span class='anomaly-time'>now</span><span class='anomaly-msg'>No active anomalies.</span></li>";
  }
  setText("ops-anomaly-count", fmt(anomalies.length, 0));
}
function renderIndexerProgressPanel() {
  const ex = state.exchange || {};
  const indexer = ex.indexer || null;
  const progress = getIndexerBreakdown(indexer);
  const kpiWrap = el("indexer-progress-kpis");
  if (kpiWrap) {
    kpiWrap.innerHTML = [["Discovered", fmt(progress.discovered, 0)], ["Pending Backfill", fmt(progress.pendingBackfill, 0)], ["Backfilling", fmt(progress.backfilling, 0)], ["Backfill Complete", fmt(progress.indexed, 0)], ["Live Tracking", fmt(progress.liveTracking, 0)], ["Failed Backfill", fmt(progress.failedBackfill, 0)]].map(([title, value]) => `<article class="kpi-card"><div class="kpi-title">${title}</div><div class="kpi-value">${value}</div></article>`).join("");
  }
  const bar = el("indexer-progress-bar");
  if (bar) bar.style.width = `${Math.max(0, Math.min(100, progress.completePct))}%`;
  setText("indexer-backlog-label", `Backlog: ${fmt(progress.backlog, 0)}`);
  const status = indexer || {};
  setText("indexer-progress-meta", `Backfill ${fmt(progress.completePct, 2)}% | backfill queue ${fmt(status.priorityQueueSize || 0, 0)} | live queue ${fmt(status.liveQueueSize || 0, 0)} | avg wait ${fmt(Number(status.averagePendingWaitMs || 0) / 60000, 1)}m | scans ok ${fmt(status.successfulScans || 0, 0)} / failed ${fmt(status.failedScans || 0, 0)}`);
}
function buildVolumeSymbolCandidates(symbol) {
  const raw = String(symbol || "").trim().toUpperCase();
  if (!raw) return [];
  const compact = raw.replace(/[^A-Z0-9]/g, "");
  const noPerp = compact.replace(/PERP$/, "");
  const noUsd = noPerp.replace(/USDT$/, "").replace(/USD$/, "");
  return Array.from(new Set([raw, compact, noPerp, noUsd].filter(Boolean)));
}
function buildPriceSymbolLookup(rows = []) {
  const lookup = new Map();
  (Array.isArray(rows) ? rows : []).forEach(row => {
    const symbol = String((row === null || row === void 0 ? void 0 : row.symbol) || "").trim().toUpperCase();
    if (!symbol) return;
    buildVolumeSymbolCandidates(symbol).forEach(candidate => {
      if (!lookup.has(candidate)) lookup.set(candidate, row);
    });
  });
  return lookup;
}
function resolvePriceRowForSymbol(symbol, lookup) {
  if (!lookup || typeof lookup.get !== "function") return null;
  const candidates = buildVolumeSymbolCandidates(symbol);
  for (const candidate of candidates) {
    if (lookup.has(candidate)) return lookup.get(candidate);
  }
  return null;
}
function buildPerpPairLabel(symbol) {
  const raw = String(symbol || "").trim().toUpperCase();
  if (!raw) return "-";
  if (raw.includes("/")) return raw;
  if (raw.endsWith("-PERP") || raw.endsWith("_PERP")) {
    return `${raw.replace(/[-_]PERP$/, "")}/PERP`;
  }
  if (raw.endsWith("PERP")) return `${raw.replace(/PERP$/, "")}/PERP`;
  return `${raw}/PERP`;
}
function getTidalMovement(deltaPct) {
  if (deltaPct > 0.35) return "up";
  if (deltaPct < -0.35) return "down";
  return "flat";
}
function getTidalTier(rank) {
  if (rank <= 3) return {
    key: "surface-crest",
    label: "Surface Crest"
  };
  if (rank <= 10) return {
    key: "mid-current",
    label: "Mid Current"
  };
  return {
    key: "deep-flow",
    label: "Deep Flow"
  };
}
function startOfUtcDay(ts) {
  const d = new Date(ts);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
}
function startOfUtcWeek(ts) {
  const d = new Date(startOfUtcDay(ts));
  const weekday = (d.getUTCDay() + 6) % 7;
  d.setUTCDate(d.getUTCDate() - weekday);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
}
function startOfUtcMonth(ts) {
  const d = new Date(ts);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1);
}
function bucketStartForSeries(ts, seriesKey = activeSeries) {
  if (!Number.isFinite(ts)) return null;
  if (seriesKey === "daily") return startOfUtcDay(ts);
  if (seriesKey === "weekly") return startOfUtcWeek(ts);
  return startOfUtcMonth(ts);
}
function getSeriesLookbackMs(seriesKey = activeSeries) {
  const day = 24 * 60 * 60 * 1000;
  if (seriesKey === "daily") return 180 * day;
  if (seriesKey === "weekly") return 370 * day;
  return 3 * 365 * day;
}
function getExchangePriceRows() {
  var _state$exchange3, _state$exchange3$sour;
  return Array.isArray((_state$exchange3 = state.exchange) === null || _state$exchange3 === void 0 ? void 0 : (_state$exchange3$sour = _state$exchange3.source) === null || _state$exchange3$sour === void 0 ? void 0 : _state$exchange3$sour.prices) ? state.exchange.source.prices : [];
}
function getDashboardPriceRows() {
  return [];
}
function resolveCanonicalTokenSymbol(rawSymbol) {
  const normalized = normalizeTokenSymbol(rawSymbol);
  if (!normalized) return "";
  const exchangeLookup = buildPriceSymbolLookup(getExchangePriceRows());
  const exchangeRow = resolvePriceRowForSymbol(normalized, exchangeLookup);
  if (exchangeRow !== null && exchangeRow !== void 0 && exchangeRow.symbol) return normalizeTokenSymbol(exchangeRow.symbol);
  return normalized;
}
function isKnownTokenSymbol(rawSymbol) {
  const normalized = normalizeTokenSymbol(rawSymbol);
  if (!normalized) return false;
  const exchangeLookup = buildPriceSymbolLookup(getExchangePriceRows());
  if (resolvePriceRowForSymbol(normalized, exchangeLookup)) return true;
  return false;
}
function getDefaultTokenSymbol() {
  var _state$exchange4;
  const ranked = Array.isArray((_state$exchange4 = state.exchange) === null || _state$exchange4 === void 0 ? void 0 : _state$exchange4.volumeRank) ? state.exchange.volumeRank : [];
  if (ranked.length) {
    var _ranked$;
    const candidate = resolveCanonicalTokenSymbol((_ranked$ = ranked[0]) === null || _ranked$ === void 0 ? void 0 : _ranked$.symbol);
    if (candidate) return candidate;
  }
  const exchangePrices = getExchangePriceRows();
  if (exchangePrices.length) {
    var _exchangePrices$;
    const candidate = resolveCanonicalTokenSymbol((_exchangePrices$ = exchangePrices[0]) === null || _exchangePrices$ === void 0 ? void 0 : _exchangePrices$.symbol);
    if (candidate) return candidate;
  }
  return "";
}
function ensureTokenSymbol() {
  const current = resolveCanonicalTokenSymbol(state.tokenSymbol);
  const hasUniverse = getExchangePriceRows().length > 0;
  if (current && (!hasUniverse || isKnownTokenSymbol(current))) {
    state.tokenSymbol = current;
    return current;
  }
  const fallback = getDefaultTokenSymbol();
  if (fallback) {
    state.tokenSymbol = fallback;
    return fallback;
  }
  state.tokenSymbol = "";
  return "";
}
function getTokenAnalyticsSnapshot(rawSymbol = state.tokenSymbol) {
  const analytics = state.tokenAnalytics;
  if (!analytics || typeof analytics !== "object") return null;
  const requested = resolveCanonicalTokenSymbol(rawSymbol);
  const snapshotSymbol = resolveCanonicalTokenSymbol(analytics.symbol || "");
  if (!requested || !snapshotSymbol || requested !== snapshotSymbol) return null;
  const timeframe = String(analytics.timeframe || "").toLowerCase();
  const currentTf = String(state.timeframe || "").toLowerCase();
  if (timeframe && currentTf && timeframe !== currentTf) return null;
  return analytics;
}
function getTokenDataContext(rawSymbol = state.tokenSymbol) {
  var _state$dashboard;
  const symbol = resolveCanonicalTokenSymbol(rawSymbol);
  const dashboardMarket = ((_state$dashboard = state.dashboard) === null || _state$dashboard === void 0 ? void 0 : _state$dashboard.market) || {};
  const exchangePrices = getExchangePriceRows();
  const dashboardPrices = getDashboardPriceRows();
  const mergedPrices = [...exchangePrices, ...dashboardPrices];
  const priceLookup = buildPriceSymbolLookup(mergedPrices);
  const priceRow = resolvePriceRowForSymbol(symbol, priceLookup);
  const markCandles = (dashboardMarket.markCandlesBySymbol && dashboardMarket.markCandlesBySymbol[symbol] && Array.isArray(dashboardMarket.markCandlesBySymbol[symbol].rows) ? dashboardMarket.markCandlesBySymbol[symbol].rows : []) || [];
  const candles = (dashboardMarket.candlesBySymbol && dashboardMarket.candlesBySymbol[symbol] && Array.isArray(dashboardMarket.candlesBySymbol[symbol].rows) ? dashboardMarket.candlesBySymbol[symbol].rows : []) || [];
  const fundingRows = (dashboardMarket.fundingBySymbol && dashboardMarket.fundingBySymbol[symbol] && Array.isArray(dashboardMarket.fundingBySymbol[symbol].rows) ? dashboardMarket.fundingBySymbol[symbol].rows : []) || [];
  const publicTrades = (dashboardMarket.publicTradesBySymbol && Array.isArray(dashboardMarket.publicTradesBySymbol[symbol]) ? dashboardMarket.publicTradesBySymbol[symbol] : []) || [];
  return {
    symbol,
    priceRow: priceRow || null,
    candles: candles.slice(),
    markCandles: markCandles.slice(),
    fundingRows: fundingRows.slice(),
    publicTrades: publicTrades.slice()
  };
}
function sortByTimestampAsc(rows = [], tsKey = "timestamp") {
  return [...rows].sort((a, b) => toNum(a === null || a === void 0 ? void 0 : a[tsKey], 0) - toNum(b === null || b === void 0 ? void 0 : b[tsKey], 0));
}
function aggregateBucketed(rows = [], seriesKey = activeSeries, options = {}) {
  const lookbackMs = Number(options.lookbackMs || getSeriesLookbackMs(seriesKey));
  const now = Date.now();
  const map = new Map();
  rows.forEach(row => {
    const ts = toNum(row === null || row === void 0 ? void 0 : row.timestamp, NaN);
    if (!Number.isFinite(ts)) return;
    if (lookbackMs > 0 && ts < now - lookbackMs) return;
    const bucketStart = bucketStartForSeries(ts, seriesKey);
    if (!Number.isFinite(bucketStart)) return;
    let bucket = map.get(bucketStart);
    if (!bucket) {
      bucket = {
        timestamp: bucketStart,
        value: 0,
        count: 0,
        longValue: 0,
        shortValue: 0
      };
      map.set(bucketStart, bucket);
    }
    bucket.value += toNum(row === null || row === void 0 ? void 0 : row.value, 0);
    bucket.longValue += toNum(row === null || row === void 0 ? void 0 : row.longValue, 0);
    bucket.shortValue += toNum(row === null || row === void 0 ? void 0 : row.shortValue, 0);
    bucket.count += 1;
  });
  return Array.from(map.values()).sort((a, b) => a.timestamp - b.timestamp).map(bucket => {
    const next = {
      ...bucket
    };
    if (options.average && bucket.count > 0) {
      next.value = bucket.value / bucket.count;
    }
    return next;
  });
}
function buildTokenVolumeSeries(tokenContext, seriesKey = activeSeries) {
  const candles = tokenContext.markCandles.length ? tokenContext.markCandles : tokenContext.candles;
  if (!candles.length) return [];
  const points = sortByTimestampAsc(candles, "t").map(row => {
    const close = toNum(row === null || row === void 0 ? void 0 : row.c, 0);
    const volume = toNum(row === null || row === void 0 ? void 0 : row.v, 0);
    return {
      timestamp: toNum(row === null || row === void 0 ? void 0 : row.t, NaN),
      value: close * volume
    };
  });
  return aggregateBucketed(points, seriesKey, {
    average: false
  });
}
function buildTokenOiSeries(tokenContext, seriesKey = activeSeries) {
  var _tokenContext$priceRo, _tokenContext$priceRo2, _candles;
  const candles = tokenContext.markCandles.length ? tokenContext.markCandles : tokenContext.candles;
  const markNow = toNum((_tokenContext$priceRo = tokenContext.priceRow) === null || _tokenContext$priceRo === void 0 ? void 0 : _tokenContext$priceRo.mark, NaN);
  const openInterest = toNum((_tokenContext$priceRo2 = tokenContext.priceRow) === null || _tokenContext$priceRo2 === void 0 ? void 0 : _tokenContext$priceRo2.open_interest, NaN);
  const oiUsdNow = Number.isFinite(markNow) && Number.isFinite(openInterest) ? markNow * openInterest : NaN;
  if (!candles.length || !Number.isFinite(oiUsdNow) || oiUsdNow <= 0) {
    if (Number.isFinite(oiUsdNow) && oiUsdNow > 0) {
      return [{
        timestamp: startOfUtcDay(Date.now()),
        value: oiUsdNow
      }];
    }
    return [];
  }
  const latestClose = toNum((_candles = candles[candles.length - 1]) === null || _candles === void 0 ? void 0 : _candles.c, markNow || 1);
  const points = sortByTimestampAsc(candles, "t").map(row => {
    const close = toNum(row === null || row === void 0 ? void 0 : row.c, latestClose);
    const scale = latestClose > 0 ? close / latestClose : 1;
    return {
      timestamp: toNum(row === null || row === void 0 ? void 0 : row.t, NaN),
      value: Math.max(0, oiUsdNow * scale)
    };
  });
  return aggregateBucketed(points, seriesKey, {
    average: true
  });
}
function buildTokenFundingSeries(tokenContext, seriesKey = activeSeries) {
  const points = sortByTimestampAsc(tokenContext.fundingRows, "createdAt").map(row => ({
    timestamp: toNum(row === null || row === void 0 ? void 0 : row.createdAt, NaN),
    value: toNum(row === null || row === void 0 ? void 0 : row.fundingRate, 0) * 100
  }));
  if (!points.length && tokenContext.priceRow) {
    var _tokenContext$priceRo3;
    const fallback = toNum((_tokenContext$priceRo3 = tokenContext.priceRow) === null || _tokenContext$priceRo3 === void 0 ? void 0 : _tokenContext$priceRo3.funding, NaN);
    if (Number.isFinite(fallback)) {
      return [{
        timestamp: startOfUtcDay(Date.now()),
        value: fallback * 100
      }];
    }
  }
  return aggregateBucketed(points, seriesKey, {
    average: true
  });
}
function normalizeTokenTradeSide(side) {
  return String(side || "").trim().toLowerCase().replace(/[\s-]+/g, "_");
}
function isLiquidationCauseToken(cause) {
  const normalized = String(cause || "").toLowerCase();
  return normalized.includes("liq") || normalized.includes("liquid");
}
function getTokenNetflowSign(side) {
  const normalized = normalizeTokenTradeSide(side);
  if (normalized === "buy" || normalized === "b" || normalized === "open_long" || normalized === "close_short") {
    return 1;
  }
  if (normalized === "sell" || normalized === "s" || normalized === "open_short" || normalized === "close_long") {
    return -1;
  }
  return 0;
}
function getTokenLiquidationBucket(side) {
  const normalized = normalizeTokenTradeSide(side);
  if (normalized === "sell" || normalized === "s" || normalized === "close_long" || normalized === "open_short") {
    return "long";
  }
  if (normalized === "buy" || normalized === "b" || normalized === "close_short" || normalized === "open_long") {
    return "short";
  }
  return "unknown";
}
function buildTokenLiquidationSeries(tokenContext, seriesKey = activeSeries) {
  const points = sortByTimestampAsc(tokenContext.publicTrades, "timestamp").filter(row => isLiquidationCauseToken(row === null || row === void 0 ? void 0 : row.cause)).map(row => {
    const ts = toNum(row === null || row === void 0 ? void 0 : row.timestamp, NaN);
    const notional = Math.abs(toNum(row === null || row === void 0 ? void 0 : row.price, 0) * toNum(row === null || row === void 0 ? void 0 : row.amount, 0));
    const bucket = getTokenLiquidationBucket(row === null || row === void 0 ? void 0 : row.side);
    const longValue = bucket === "long" ? notional : 0;
    const shortValue = bucket === "short" ? notional : 0;
    const inferredLong = bucket === "unknown" ? getTokenNetflowSign(row === null || row === void 0 ? void 0 : row.side) < 0 ? notional : 0 : longValue;
    const inferredShort = bucket === "unknown" ? getTokenNetflowSign(row === null || row === void 0 ? void 0 : row.side) >= 0 ? notional : 0 : shortValue;
    return {
      timestamp: ts,
      value: inferredLong + inferredShort,
      longValue: inferredLong,
      shortValue: inferredShort
    };
  });
  return aggregateBucketed(points, seriesKey, {
    average: false
  });
}
function buildTokenNetflowSeries(tokenContext, seriesKey = activeSeries) {
  const points = sortByTimestampAsc(tokenContext.publicTrades, "timestamp").map(row => {
    const notional = Math.abs(toNum(row === null || row === void 0 ? void 0 : row.price, 0) * toNum(row === null || row === void 0 ? void 0 : row.amount, 0));
    const signed = getTokenNetflowSign(row === null || row === void 0 ? void 0 : row.side) * notional;
    return {
      timestamp: toNum(row === null || row === void 0 ? void 0 : row.timestamp, NaN),
      value: signed
    };
  });
  return aggregateBucketed(points, seriesKey, {
    average: false
  });
}
function buildTokenWalletActivitySeries(tokenContext, seriesKey = activeSeries) {
  const points = sortByTimestampAsc(tokenContext.publicTrades, "timestamp").map(row => ({
    timestamp: toNum(row === null || row === void 0 ? void 0 : row.timestamp, NaN),
    value: 1
  }));
  return aggregateBucketed(points, seriesKey, {
    average: false
  });
}
function buildSeriesFromTokenAnalytics(tokenAnalytics, metric, seriesKey = activeSeries) {
  if (!tokenAnalytics || !tokenAnalytics.series || typeof tokenAnalytics.series !== "object") return [];
  const rows = Array.isArray(tokenAnalytics.series[metric]) ? tokenAnalytics.series[metric] : [];
  if (!rows.length) return [];
  const points = rows.map(row => ({
    timestamp: toNum(row === null || row === void 0 ? void 0 : row.timestamp, NaN),
    value: toNum(row === null || row === void 0 ? void 0 : row.value, 0),
    longValue: toNum(row === null || row === void 0 ? void 0 : row.longValue, 0),
    shortValue: toNum(row === null || row === void 0 ? void 0 : row.shortValue, 0)
  })).filter(row => Number.isFinite(row.timestamp));
  if (!points.length) return [];
  const averageMetrics = new Set(["oi", "funding"]);
  return aggregateBucketed(points, seriesKey, {
    average: averageMetrics.has(metric)
  });
}
function getTokenActiveSeries(tab = state.analyticsTab) {
  const symbol = ensureTokenSymbol();
  if (!symbol) return [];
  const tokenAnalytics = getTokenAnalyticsSnapshot(symbol);
  const tokenContext = getTokenDataContext(symbol);
  const metric = normalizeAnalyticsTab(tab);
  if (tokenAnalytics) {
    const fromGateway = buildSeriesFromTokenAnalytics(tokenAnalytics, metric, activeSeries);
    if (fromGateway.length) return fromGateway;
  }
  if (metric === "volume") return buildTokenVolumeSeries(tokenContext, activeSeries);
  if (metric === "oi") return buildTokenOiSeries(tokenContext, activeSeries);
  if (metric === "funding") return buildTokenFundingSeries(tokenContext, activeSeries);
  if (metric === "liquidations") return buildTokenLiquidationSeries(tokenContext, activeSeries);
  if (metric === "netflow") return buildTokenNetflowSeries(tokenContext, activeSeries);
  if (metric === "wallet_activity") return buildTokenWalletActivitySeries(tokenContext, activeSeries);
  return [];
}
function signedUsdCompact(value) {
  const num = toNum(value, 0);
  const sign = num >= 0 ? "+" : "-";
  return `${sign}$${fmtCompact(Math.abs(num))}`;
}
function percentageDelta(fromValue, toValue) {
  const from = toNum(fromValue, NaN);
  const to = toNum(toValue, NaN);
  if (!Number.isFinite(from) || !Number.isFinite(to) || from === 0) return NaN;
  return (to - from) / Math.abs(from) * 100;
}
function detectTrendLabel(delta, tolerance = 0.00001) {
  const value = toNum(delta, 0);
  if (value > tolerance) return "uptrend";
  if (value < -tolerance) return "downtrend";
  return "flat";
}
function classifyFundingRegime(fundingPct) {
  const value = toNum(fundingPct, 0);
  if (value >= 0.01) return "long crowded";
  if (value <= -0.01) return "short crowded";
  return "neutral";
}
function sumSeriesValue(rows = [], key = "value") {
  return (Array.isArray(rows) ? rows : []).reduce((sum, row) => sum + toNum(row === null || row === void 0 ? void 0 : row[key], 0), 0);
}
function getTokenMetricNarrative(metric, symbol) {
  const map = {
    oi: `Market structure and positioning depth for ${symbol}.`,
    funding: `Directional crowding signal for ${symbol} with explicit baseline context.`,
    liquidations: `Stress map showing liquidation pressure and spike windows for ${symbol}.`,
    netflow: `Directional capital pressure for ${symbol} via signed flow across intervals.`,
    wallet_activity: `Behavioral intelligence stream of token-linked activity for ${symbol}.`,
    volume: `Per-token notional turnover and dominance rhythm for ${symbol}.`
  };
  return map[metric] || `Per-token analytics intelligence for ${symbol}.`;
}
function formatTokenAnalyticsSource(tokenAnalytics, metric) {
  var _tokenAnalytics$meta;
  const source = (tokenAnalytics === null || tokenAnalytics === void 0 ? void 0 : (_tokenAnalytics$meta = tokenAnalytics.meta) === null || _tokenAnalytics$meta === void 0 ? void 0 : _tokenAnalytics$meta.source) || {};
  const warnings = Array.isArray(source.warnings) ? source.warnings : [];
  const warningCount = warnings.length;
  const withWarnings = label => warningCount > 0 ? `${label} · ${warningCount} warning${warningCount > 1 ? "s" : ""}` : label;
  if (metric === "funding") return withWarnings(source.funding || "dashboard_fallback");
  if (metric === "liquidations" || metric === "netflow" || metric === "wallet_activity" || metric === "volume") {
    return withWarnings(source.trades || "dashboard_fallback");
  }
  if (metric === "oi") return withWarnings(source.price || "dashboard_fallback");
  return withWarnings(source.price || source.trades || source.funding || "dashboard_fallback");
}
function getMetricModeLabel(metric, tokenAnalytics = null) {
  var _tokenAnalytics$meta2;
  const mode = (tokenAnalytics === null || tokenAnalytics === void 0 ? void 0 : (_tokenAnalytics$meta2 = tokenAnalytics.meta) === null || _tokenAnalytics$meta2 === void 0 ? void 0 : _tokenAnalytics$meta2.mode) || {};
  if (metric === "oi") return mode.oi || "snapshot_only";
  if (metric === "funding") return mode.funding || "funding_history";
  if (metric === "liquidations") return mode.liquidations || "cause_filtered";
  if (metric === "netflow") return mode.netflow || "side_mapped";
  if (metric === "wallet_activity") return "event_activity";
  return "series";
}
function buildMetricTableRows(metric, series = [], tokenAnalytics = null) {
  const recent = (Array.isArray(series) ? series : []).slice(-12);
  if (metric === "oi") {
    return recent.map((row, index) => {
      const prev = index > 0 ? recent[index - 1] : null;
      const delta = prev ? toNum(row.value, 0) - toNum(prev.value, 0) : 0;
      return [formatChartLabel(row, "axis") || fmtTime(row.timestamp), `$${fmtCompact(toNum(row.value, 0))}`, signedUsdCompact(delta)];
    });
  }
  if (metric === "funding") {
    return recent.map(row => {
      const rate = toNum(row.value, 0);
      return [formatChartLabel(row, "axis") || fmtTime(row.timestamp), `${fmtSigned(rate, 4)}%`, classifyFundingRegime(rate)];
    });
  }
  if (metric === "liquidations") {
    return recent.map(row => {
      const total = toNum(row.value, 0);
      const longValue = toNum(row.longValue, 0);
      const shortValue = toNum(row.shortValue, 0);
      const bias = longValue > shortValue ? "long stress" : shortValue > longValue ? "short stress" : "balanced";
      return [formatChartLabel(row, "axis") || fmtTime(row.timestamp), `$${fmtCompact(total)}`, `$${fmtCompact(longValue)}`, `$${fmtCompact(shortValue)}`, bias];
    });
  }
  if (metric === "netflow") {
    let running = 0;
    return recent.map(row => {
      const value = toNum(row.value, 0);
      running += value;
      return [formatChartLabel(row, "axis") || fmtTime(row.timestamp), signedUsdCompact(value), signedUsdCompact(running), value >= 0 ? "inflow" : "outflow"];
    });
  }
  if (metric === "wallet_activity") {
    const events = Array.isArray(tokenAnalytics === null || tokenAnalytics === void 0 ? void 0 : tokenAnalytics.events) ? tokenAnalytics.events.slice(0, 12) : [];
    if (events.length) {
      return events.map(event => [fmtTime(event.timestamp), String(event.side || "-").replace(/_/g, " "), String(event.cause || "normal").replace(/_/g, " "), `$${fmtCompact(toNum(event.notionalUsd, 0))}`]);
    }
    return recent.map(row => [formatChartLabel(row, "axis") || fmtTime(row.timestamp), fmt(toNum(row.value, 0), 0), toNum(row.value, 0) >= 8 ? "burst" : "normal", fmtAgo(row.timestamp)]);
  }
  return recent.map(row => [formatChartLabel(row, "axis") || fmtTime(row.timestamp), `$${fmtCompact(toNum(row.value, 0))}`]);
}
function buildTokenInsightModel(symbol, metric, series = [], tokenAnalytics = null) {
  const rows = buildMetricTableRows(metric, series, tokenAnalytics);
  const modeLabel = getMetricModeLabel(metric, tokenAnalytics);
  const meta = `${rows.length} intervals · ${modeLabel}`;
  if (metric === "oi") {
    return {
      title: "OI Structure Breakdown",
      meta,
      columns: ["Interval", "OI", "Delta"],
      rows
    };
  }
  if (metric === "funding") {
    return {
      title: "Funding Regime Breakdown",
      meta,
      columns: ["Interval", "Funding", "Regime"],
      rows
    };
  }
  if (metric === "liquidations") {
    return {
      title: "Liquidation Stress Breakdown",
      meta,
      columns: ["Interval", "Total", "Long", "Short", "Bias"],
      rows
    };
  }
  if (metric === "netflow") {
    return {
      title: "Directional Flow Breakdown",
      meta,
      columns: ["Interval", "Netflow", "Cumulative", "Direction"],
      rows
    };
  }
  if (metric === "wallet_activity") {
    return {
      title: "Wallet Intelligence Feed",
      meta: Array.isArray(tokenAnalytics === null || tokenAnalytics === void 0 ? void 0 : tokenAnalytics.events) && tokenAnalytics.events.length ? `${tokenAnalytics.events.length} recent events · ${modeLabel}` : meta,
      columns: ["Time", "Side", "Cause", "Notional"],
      rows
    };
  }
  return {
    title: "Volume Dominance Breakdown",
    meta,
    columns: ["Interval", "Volume"],
    rows
  };
}
function buildMetricHighlightChips(metric, series = [], tokenAnalytics = null) {
  const chips = [];
  const values = (Array.isArray(series) ? series : []).map(row => toNum(row.value, 0));
  const last = values.length ? values[values.length - 1] : 0;
  const first = values.length ? values[0] : 0;
  const delta = last - first;
  if (metric === "oi") {
    const high = values.length ? Math.max(...values) : 0;
    const low = values.length ? Math.min(...values) : 0;
    const uniqueSamples = new Set(values.map(value => Number(value).toFixed(6))).size;
    chips.push(`trend ${detectTrendLabel(delta)}`);
    chips.push(`range $${fmtCompact(low)} -> $${fmtCompact(high)}`);
    chips.push(Math.abs(percentageDelta(first, last)) >= 5 ? "expansion regime" : "stable structure");
    if (uniqueSamples <= 1) chips.push("snapshot-only OI (history unavailable)");
    return chips;
  }
  if (metric === "funding") {
    chips.push(classifyFundingRegime(last));
    const positives = values.filter(v => v > 0).length;
    const negatives = values.filter(v => v < 0).length;
    chips.push(`positive ${positives} / negative ${negatives}`);
    chips.push(Math.abs(last) >= 0.01 ? "crowded direction" : "balanced carry");
    return chips;
  }
  if (metric === "liquidations") {
    const longTotal = sumSeriesValue(series, "longValue");
    const shortTotal = sumSeriesValue(series, "shortValue");
    const total = longTotal + shortTotal;
    chips.push(`total $${fmtCompact(total)}`);
    chips.push(longTotal > shortTotal ? "long liquidation bias" : shortTotal > longTotal ? "short liquidation bias" : "balanced pressure");
    const peak = values.length ? Math.max(...values) : 0;
    chips.push(`peak spike $${fmtCompact(peak)}`);
    return chips;
  }
  if (metric === "netflow") {
    const cumulative = values.reduce((sum, value) => sum + value, 0);
    chips.push(cumulative >= 0 ? "net inflow regime" : "net outflow regime");
    chips.push(`cumulative ${signedUsdCompact(cumulative)}`);
    chips.push(Math.abs(delta) >= Math.max(1, Math.abs(first) * 0.25) ? "directional swing" : "range flow");
    return chips;
  }
  if (metric === "wallet_activity") {
    var _state$exchange5, _state$exchange5$inde, _state$exchange6, _state$exchange6$inde, _tokenAnalytics$meta3, _tokenAnalytics$meta4;
    const maxBucket = values.length ? Math.max(...values) : 0;
    const totalEvents = values.reduce((sum, value) => sum + value, 0);
    const freshTracked = toNum((_state$exchange5 = state.exchange) === null || _state$exchange5 === void 0 ? void 0 : (_state$exchange5$inde = _state$exchange5.indexer) === null || _state$exchange5$inde === void 0 ? void 0 : _state$exchange5$inde.liveGroupSize, 0);
    const staleTracked = toNum((_state$exchange6 = state.exchange) === null || _state$exchange6 === void 0 ? void 0 : (_state$exchange6$inde = _state$exchange6.indexer) === null || _state$exchange6$inde === void 0 ? void 0 : _state$exchange6$inde.liveStaleWallets, 0);
    chips.push(`events ${fmt(totalEvents, 0)} / max burst ${fmt(maxBucket, 0)}`);
    chips.push(`tracked wallets ${fmt(freshTracked, 0)} live`);
    chips.push(`stale wallets ${fmt(staleTracked, 0)}`);
    const warnings = Array.isArray(tokenAnalytics === null || tokenAnalytics === void 0 ? void 0 : (_tokenAnalytics$meta3 = tokenAnalytics.meta) === null || _tokenAnalytics$meta3 === void 0 ? void 0 : (_tokenAnalytics$meta4 = _tokenAnalytics$meta3.source) === null || _tokenAnalytics$meta4 === void 0 ? void 0 : _tokenAnalytics$meta4.warnings) ? tokenAnalytics.meta.source.warnings : [];
    if (warnings.some(warning => String(warning).includes("truncated"))) {
      chips.push("history truncated by page cap");
    }
    return chips;
  }
  chips.push(`latest $${fmtCompact(last)}`);
  chips.push(`change ${signedUsdCompact(delta)}`);
  chips.push(`samples ${values.length}`);
  return chips;
}
function buildTokenKpiRows(symbol, metric = state.analyticsTab) {
  var _tokenContext$priceRo4, _tokenContext$priceRo5, _tokenContext$priceRo6, _tokenContext$priceRo7, _tokenAnalytics$fresh;
  const normalizedMetric = normalizeAnalyticsTab(metric);
  const tokenAnalytics = getTokenAnalyticsSnapshot(symbol);
  const series = getTokenActiveSeries(normalizedMetric);
  const values = series.map(row => toNum(row.value, 0));
  const lastValue = values.length ? values[values.length - 1] : 0;
  const firstValue = values.length ? values[0] : 0;
  const valueDelta = lastValue - firstValue;
  const tokenContext = getTokenDataContext(symbol);
  const mark = toNum((_tokenContext$priceRo4 = tokenContext.priceRow) === null || _tokenContext$priceRo4 === void 0 ? void 0 : _tokenContext$priceRo4.mark, 0);
  const openInterest = toNum((_tokenContext$priceRo5 = tokenContext.priceRow) === null || _tokenContext$priceRo5 === void 0 ? void 0 : _tokenContext$priceRo5.open_interest, 0);
  const oiUsd = mark * openInterest;
  const volume24h = toNum((_tokenContext$priceRo6 = tokenContext.priceRow) === null || _tokenContext$priceRo6 === void 0 ? void 0 : _tokenContext$priceRo6.volume_24h, 0);
  const fundingPct = toNum((_tokenContext$priceRo7 = tokenContext.priceRow) === null || _tokenContext$priceRo7 === void 0 ? void 0 : _tokenContext$priceRo7.funding, 0) * 100;
  const k = (tokenAnalytics === null || tokenAnalytics === void 0 ? void 0 : tokenAnalytics.kpis) || {};
  const currentOi = toNum(k.currentOiUsd, oiUsd);
  const currentVolume24h = toNum(k.volume24hUsd, volume24h);
  const currentFundingPct = toNum(k.fundingPctCurrent, fundingPct);
  const lastTsCandidates = [k.lastTradeAt, k.lastFundingAt, tokenAnalytics === null || tokenAnalytics === void 0 ? void 0 : (_tokenAnalytics$fresh = tokenAnalytics.freshness) === null || _tokenAnalytics$fresh === void 0 ? void 0 : _tokenAnalytics$fresh.updatedAt, tokenAnalytics === null || tokenAnalytics === void 0 ? void 0 : tokenAnalytics.generatedAt, ...series.map(row => row.timestamp)].map(value => Number(value)).filter(value => Number.isFinite(value) && value > 0);
  const lastTs = lastTsCandidates.length ? Math.max(...lastTsCandidates) : Date.now();
  let rows = [];
  if (normalizedMetric === "oi") {
    const high = values.length ? Math.max(...values) : currentOi;
    const low = values.length ? Math.min(...values) : currentOi;
    const deltaPct = percentageDelta(firstValue || currentOi, lastValue || currentOi);
    rows = [{
      key: "Current OI",
      value: `$${fmtCompact(currentOi)}`
    }, {
      key: "OI Change",
      value: Number.isFinite(deltaPct) ? `${fmtSigned(deltaPct, 2)}%` : signedUsdCompact(valueDelta)
    }, {
      key: "Local High",
      value: `$${fmtCompact(high)}`
    }, {
      key: "Local Low",
      value: `$${fmtCompact(low)}`
    }, {
      key: "Trend",
      value: detectTrendLabel(valueDelta)
    }];
  } else if (normalizedMetric === "funding") {
    const avg = values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : currentFundingPct;
    const maxPos = values.filter(value => value > 0);
    const maxNeg = values.filter(value => value < 0);
    const cumulative = values.reduce((sum, value) => sum + value, 0);
    rows = [{
      key: "Current Funding",
      value: `${fmtSigned(currentFundingPct, 4)}%`
    }, {
      key: "Average Funding",
      value: `${fmtSigned(avg, 4)}%`
    }, {
      key: "Highest Positive",
      value: `${fmtSigned(maxPos.length ? Math.max(...maxPos) : 0, 4)}%`
    }, {
      key: "Lowest Negative",
      value: `${fmtSigned(maxNeg.length ? Math.min(...maxNeg) : 0, 4)}%`
    }, {
      key: "Cumulative Funding",
      value: `${fmtSigned(cumulative, 4)}%`
    }];
  } else if (normalizedMetric === "liquidations") {
    const longTotal = sumSeriesValue(series, "longValue");
    const shortTotal = sumSeriesValue(series, "shortValue");
    const total = longTotal + shortTotal;
    const peak = values.length ? Math.max(...values) : 0;
    const imbalance = total > 0 ? (longTotal - shortTotal) / total * 100 : 0;
    rows = [{
      key: "Total Liquidations",
      value: `$${fmtCompact(total)}`
    }, {
      key: "Long Liquidations",
      value: `$${fmtCompact(longTotal)}`
    }, {
      key: "Short Liquidations",
      value: `$${fmtCompact(shortTotal)}`
    }, {
      key: "Biggest Spike",
      value: `$${fmtCompact(peak)}`
    }, {
      key: "Imbalance",
      value: `${fmtSigned(imbalance, 2)}%`
    }];
  } else if (normalizedMetric === "netflow") {
    const cumulative = values.reduce((sum, value) => sum + value, 0);
    const strongestIn = values.length ? Math.max(...values) : 0;
    const strongestOut = values.length ? Math.min(...values) : 0;
    rows = [{
      key: "Current Netflow",
      value: signedUsdCompact(lastValue)
    }, {
      key: "Cumulative Netflow",
      value: signedUsdCompact(cumulative)
    }, {
      key: "Strongest Inflow",
      value: signedUsdCompact(strongestIn)
    }, {
      key: "Strongest Outflow",
      value: signedUsdCompact(strongestOut)
    }, {
      key: "Direction",
      value: cumulative >= 0 ? "inflow" : "outflow"
    }];
  } else if (normalizedMetric === "wallet_activity") {
    var _state$exchange7, _state$exchange7$inde, _state$exchange8, _state$exchange8$inde;
    const totalEvents = values.reduce((sum, value) => sum + value, 0);
    const activeBuckets = values.filter(value => value > 0).length;
    const maxBurst = values.length ? Math.max(...values) : 0;
    const liveTracked = toNum((_state$exchange7 = state.exchange) === null || _state$exchange7 === void 0 ? void 0 : (_state$exchange7$inde = _state$exchange7.indexer) === null || _state$exchange7$inde === void 0 ? void 0 : _state$exchange7$inde.liveGroupSize, 0);
    const staleTracked = toNum((_state$exchange8 = state.exchange) === null || _state$exchange8 === void 0 ? void 0 : (_state$exchange8$inde = _state$exchange8.indexer) === null || _state$exchange8$inde === void 0 ? void 0 : _state$exchange8$inde.liveStaleWallets, 0);
    rows = [{
      key: "Active Intervals",
      value: fmt(activeBuckets, 0)
    }, {
      key: "Activity Events",
      value: fmt(totalEvents, 0)
    }, {
      key: "Peak Burst",
      value: fmt(maxBurst, 0)
    }, {
      key: "Live Tracked Wallets",
      value: fmt(liveTracked, 0)
    }, {
      key: "Stale Tracked Wallets",
      value: fmt(staleTracked, 0)
    }];
  } else {
    const high = values.length ? Math.max(...values) : currentVolume24h;
    const low = values.length ? Math.min(...values) : currentVolume24h;
    rows = [{
      key: "24h Volume",
      value: `$${fmtCompact(currentVolume24h)}`
    }, {
      key: "Volume Delta",
      value: signedUsdCompact(valueDelta)
    }, {
      key: "Local High",
      value: `$${fmtCompact(high)}`
    }, {
      key: "Local Low",
      value: `$${fmtCompact(low)}`
    }, {
      key: "Samples",
      value: fmt(values.length, 0)
    }];
  }
  return {
    rows,
    lastTs,
    source: formatTokenAnalyticsSource(tokenAnalytics, normalizedMetric),
    subtitle: getTokenMetricNarrative(normalizedMetric, symbol),
    insight: buildTokenInsightModel(symbol, normalizedMetric, series, tokenAnalytics),
    chips: buildMetricHighlightChips(normalizedMetric, series, tokenAnalytics)
  };
}
function renderTokenInsights(kpiModel) {
  const titleNode = el("token-insight-title");
  const metaNode = el("token-insight-meta");
  const chipsNode = el("token-highlight-chips");
  const headNode = el("token-breakdown-head");
  const bodyNode = el("token-breakdown-body");
  const insight = (kpiModel === null || kpiModel === void 0 ? void 0 : kpiModel.insight) || {};
  if (titleNode) titleNode.textContent = insight.title || "Metric Insights";
  if (metaNode) metaNode.textContent = insight.meta || "-";
  if (chipsNode) {
    const chips = Array.isArray(kpiModel === null || kpiModel === void 0 ? void 0 : kpiModel.chips) ? kpiModel.chips : [];
    chipsNode.innerHTML = chips.length ? chips.map(chip => `<span class="token-chip">${escapeHtml(chip)}</span>`).join("") : "<span class='muted'>No highlights yet.</span>";
  }
  if (headNode) {
    const columns = Array.isArray(insight.columns) ? insight.columns : [];
    headNode.innerHTML = columns.length ? `<tr>${columns.map(column => `<th>${escapeHtml(column)}</th>`).join("")}</tr>` : "";
  }
  if (bodyNode) {
    const rows = Array.isArray(insight.rows) ? insight.rows : [];
    const columnCount = Math.max(1, Array.isArray(insight.columns) ? insight.columns.length : 1);
    bodyNode.innerHTML = rows.length ? rows.map(row => `<tr>${row.map(cell => `<td>${escapeHtml(cell)}</td>`).join("")}</tr>`).join("") : `<tr><td colspan='${columnCount}' class='muted'>No interval data available for this metric yet.</td></tr>`;
  }
}
function syncAnalyticsTabs() {
  const current = normalizeAnalyticsTab(state.analyticsTab);
  document.querySelectorAll("#analytics-tabs .analytics-tab").forEach(node => {
    node.classList.toggle("active", normalizeAnalyticsTab(node.dataset.analyticsTab) === current);
  });
}
function renderTokenView() {
  var _state$exchange9, _state$exchange9$sync, _state$exchange10, _state$dashboard2;
  const symbol = ensureTokenSymbol();
  const normalizedMetric = normalizeAnalyticsTab(state.analyticsTab);
  state.analyticsTab = normalizedMetric;
  syncAnalyticsTabs();
  if (!symbol) {
    setText("token-page-title", "Token Analytics");
    setText("token-page-subtitle", "No token available from current Pacifica payload.");
    setText("token-symbol-pill", "Symbol -");
    setText("token-exchange-pill", "Pacifica");
    setText("token-timeframe-pill", `TF ${String(state.timeframe || "all").toUpperCase()}`);
    setText("token-live-pill", "SYNCING");
    setText("token-freshness-pill", "updated -");
    setText("token-source-pill", "source -");
    const host = el("token-kpis");
    if (host) host.innerHTML = "<div class='muted'>No token analytics data available yet.</div>";
    renderTokenInsights(null);
    return;
  }
  const metricName = getAnalyticsPresentation(normalizedMetric).title;
  const kpi = buildTokenKpiRows(symbol, normalizedMetric);
  setText("token-page-title", `${symbol} Analytics`);
  setText("token-page-subtitle", `${kpi.subtitle || `${metricName} intelligence for ${symbol} on Pacifica.`}`);
  setText("token-symbol-pill", `Symbol ${symbol}`);
  setText("token-exchange-pill", "Pacifica");
  setText("token-timeframe-pill", `TF ${String(state.timeframe || "all").toUpperCase()}`);
  setText("token-live-pill", ((_state$exchange9 = state.exchange) === null || _state$exchange9 === void 0 ? void 0 : (_state$exchange9$sync = _state$exchange9.sync) === null || _state$exchange9$sync === void 0 ? void 0 : _state$exchange9$sync.wsStatus) === "open" ? "LIVE" : "SYNCING");
  setText("token-freshness-pill", `updated ${fmtAgo(kpi.lastTs || ((_state$exchange10 = state.exchange) === null || _state$exchange10 === void 0 ? void 0 : _state$exchange10.generatedAt) || ((_state$dashboard2 = state.dashboard) === null || _state$dashboard2 === void 0 ? void 0 : _state$dashboard2.generatedAt) || Date.now())}`);
  setText("token-source-pill", `source ${kpi.source || "-"}`);
  const host = el("token-kpis");
  if (host) {
    host.innerHTML = kpi.rows.map(row => `<article class="kpi-card">
            <div class="kpi-title">${escapeHtml(row.key)}</div>
            <div class="kpi-value">${escapeHtml(row.value)}</div>
            ${row.meta ? `<div class="kpi-meta">${escapeHtml(row.meta)}</div>` : ""}
          </article>`).join("");
  }
  renderTokenInsights(kpi);
}
function renderExchangeTokenPreview(rows = []) {
  const host = el("exchange-token-preview-body");
  if (!host) return;
  const items = (Array.isArray(rows) ? rows : []).slice(0, 8);
  if (!items.length) {
    host.innerHTML = "<div class='muted'>No symbols available.</div>";
    return;
  }
  host.innerHTML = items.map((row, index) => {
    const symbol = escapeHtml((row === null || row === void 0 ? void 0 : row.symbol) || "-");
    const volume = `$${fmtCompact(toNum(row === null || row === void 0 ? void 0 : row.volume, 0))}`;
    const share = `${fmt(toNum(row === null || row === void 0 ? void 0 : row.sharePct, 0), 2)}% share`;
    const delta = fmtSigned(toNum(row === null || row === void 0 ? void 0 : row.deltaPct, 0), 2);
    return `<button class="token-preview-card" type="button" data-token-symbol="${symbol}">
        <span class="token-preview-rank">#${String(index + 1).padStart(2, "0")}</span>
        <strong>${symbol}</strong>
        <span class="token-preview-volume">${volume}</span>
        <span class="token-preview-meta">${share} · ${delta}%</span>
      </button>`;
  }).join("");
}
function syncRouteFromState(options = {}) {
  if (typeof window === "undefined" || !window.history) return;
  const replace = Boolean(options.replace);
  let nextPath = "/";
  if (state.view === "wallets") {
    nextPath = "/wallets";
  }
  if (window.location.pathname === nextPath) return;
  if (replace) {
    window.history.replaceState({
      view: state.view
    }, "", nextPath);
  } else {
    window.history.pushState({
      view: state.view
    }, "", nextPath);
  }
}
function applyRouteFromLocation(options = {}) {
  const replace = Boolean(options.replace);
  const pathname = typeof window !== "undefined" ? window.location.pathname : "/";
  if (pathname === "/wallets") {
    applyView("wallets", {
      skipRoute: true
    });
    if (replace) syncRouteFromState({
      replace: true
    });
    return;
  }
  applyView("exchange", {
    skipRoute: true
  });
  if (replace && pathname !== "/") {
    syncRouteFromState({
      replace: true
    });
  }
}
function openTokenAnalytics(symbol, metric = state.analyticsTab, options = {}) {
  const nextSymbol = resolveCanonicalTokenSymbol(symbol) || ensureTokenSymbol();
  if (!nextSymbol) return false;
  state.volumeFilter = String(nextSymbol).toUpperCase();
  state.volumeRankPage = 1;
  const filterInput = el("volume-filter");
  if (filterInput) filterInput.value = state.volumeFilter;
  applyView("exchange", {
    skipRoute: true
  });
  renderExchange();
  if (!options.skipRoute) syncRouteFromState({
    replace: Boolean(options.replaceRoute)
  });
  return true;
}
function buildTidalSparklineHeights(row, maxVolume, maxAbsDelta) {
  const bars = 10;
  const symbol = String((row === null || row === void 0 ? void 0 : row.symbol) || "");
  const seed = symbol.split("").reduce((acc, ch) => (acc * 31 + ch.charCodeAt(0)) % 9973, 17);
  const phase = seed % 23 / 7;
  const volRatio = maxVolume > 0 ? clamp(toNum(row === null || row === void 0 ? void 0 : row.volume, 0) / maxVolume, 0, 1) : 0;
  const deltaRatio = maxAbsDelta > 0 ? clamp(Math.abs(toNum(row === null || row === void 0 ? void 0 : row.deltaPct, 0)) / maxAbsDelta, 0, 1) : 0;
  const movement = (row === null || row === void 0 ? void 0 : row.movement) || "flat";
  return Array.from({
    length: bars
  }, (_, index) => {
    const wave = 0.58 + Math.sin(index * 0.88 + phase) * 0.22 + Math.cos(index * 0.51 + phase * 1.7) * 0.08;
    const trend = movement === "up" ? index * 1.55 : movement === "down" ? (bars - index - 1) * 1.45 : index % 2 === 0 ? 1.2 : -0.9;
    const jitter = (seed + index * 19) % 9 - 4;
    const baseline = 20 + volRatio * 44;
    const height = baseline * wave + trend + deltaRatio * 16 + jitter;
    return clamp(height, 16, 94);
  });
}
function buildTidalReasonChips(row, isMomentumLeader) {
  const chips = [];
  if (toNum(row === null || row === void 0 ? void 0 : row.sharePct, 0) >= 12 || toNum(row === null || row === void 0 ? void 0 : row.volumeRank, 99) <= 3) chips.push("dominant volume");
  if (toNum(row === null || row === void 0 ? void 0 : row.deltaPct, 0) >= 1.5) chips.push("rising fast");
  if (isMomentumLeader) chips.push("momentum leader");
  if (!chips.length) {
    chips.push((row === null || row === void 0 ? void 0 : row.movement) === "down" ? "mean reversion" : "steady flow");
  }
  return chips.slice(0, 3);
}
function renderExchange() {
  var _indexer$successfulSc2, _indexer$failedScans2, _payload$source, _payload$source2, _payload$source3, _payload$source4, _payload$source5;
  const payload = state.exchange || {};
  const kpis = payload.kpis || {};
  const comparisons = payload.kpiComparisons && payload.kpiComparisons.metrics ? payload.kpiComparisons.metrics : {};
  const indexer = payload.indexer || {};
  const progress = getIndexerBreakdown(indexer);
  const successfulScans = Number((_indexer$successfulSc2 = indexer === null || indexer === void 0 ? void 0 : indexer.successfulScans) !== null && _indexer$successfulSc2 !== void 0 ? _indexer$successfulSc2 : 0);
  const failedScans = Number((_indexer$failedScans2 = indexer === null || indexer === void 0 ? void 0 : indexer.failedScans) !== null && _indexer$failedScans2 !== void 0 ? _indexer$failedScans2 : 0);
  const scanAttempts = successfulScans + failedScans;
  const kpiRows = [{
    key: "Total Accounts",
    value: fmt(kpis.totalAccounts || 0, 0),
    comparison: getKpiComparisonDisplay(comparisons.totalAccounts)
  }, {
    key: "Total Trades",
    value: fmt(kpis.totalTrades || 0, 0),
    comparison: getKpiComparisonDisplay(comparisons.totalTrades)
  }, {
    key: "Total Volume",
    value: `$${fmtCompact(kpis.totalVolumeUsd)}`,
    comparison: getKpiComparisonDisplay(comparisons.totalVolumeUsd)
  }, {
    key: "Total Fees",
    value: `$${fmt(kpis.totalFeesUsd, 2)}`,
    comparison: getKpiComparisonDisplay(comparisons.totalFeesUsd)
  }, {
    key: "Open Interest",
    value: `$${fmtCompact(kpis.openInterestAtEnd)}`,
    comparison: getKpiComparisonDisplay(comparisons.openInterestAtEnd)
  }];
  const kpiContainer = el("exchange-kpis");
  if (kpiContainer) {
    kpiContainer.innerHTML = kpiRows.map(row => {
      var _row$comparison, _row$comparison2, _row$comparison3;
      return `<article class="kpi-card">
            <div class="kpi-title">${escapeHtml(row.key)}</div>
            <div class="kpi-value">${row.value}</div>
            <div class="kpi-delta-row">
              <span class="kpi-delta-badge ${escapeHtml(((_row$comparison = row.comparison) === null || _row$comparison === void 0 ? void 0 : _row$comparison.badgeClass) || "flat")}">${escapeHtml(((_row$comparison2 = row.comparison) === null || _row$comparison2 === void 0 ? void 0 : _row$comparison2.badgeText) || "● steady")}</span>
              <span class="kpi-delta-label">${escapeHtml(((_row$comparison3 = row.comparison) === null || _row$comparison3 === void 0 ? void 0 : _row$comparison3.labelText) || "snapshot")}</span>
            </div>
            ${row.meta ? `<div class="kpi-meta">${escapeHtml(row.meta)}</div>` : ""}
          </article>`;
    }).join("");
  }
  syncTimeframeButtons();
  renderMarketsTicker(payload);
  const volumeMethod = (payload === null || payload === void 0 ? void 0 : (_payload$source = payload.source) === null || _payload$source === void 0 ? void 0 : _payload$source.totalVolumeMethod) || "n/a";
  const rankSource = (payload === null || payload === void 0 ? void 0 : (_payload$source2 = payload.source) === null || _payload$source2 === void 0 ? void 0 : _payload$source2.volumeRankSource) || "n/a";
  setText("kpi-context-label", `Source: ${volumeMethod} · rank: ${rankSource} · updated ${fmtAgo(payload.generatedAt)}`);
  const body = el("volume-rank-body");
  const allRows = Array.isArray(payload.volumeRank) ? payload.volumeRank : [];
  const priceRows = Array.isArray(payload === null || payload === void 0 ? void 0 : (_payload$source3 = payload.source) === null || _payload$source3 === void 0 ? void 0 : _payload$source3.prices) ? payload.source.prices : [];
  const priceLookup = buildPriceSymbolLookup(priceRows);
  const totalVolumeUniverse = allRows.reduce((sum, row) => {
    const volume = toNum((row === null || row === void 0 ? void 0 : row.volume_24h_usd) !== undefined ? row.volume_24h_usd : (row === null || row === void 0 ? void 0 : row.volume_usd) !== undefined ? row.volume_usd : row === null || row === void 0 ? void 0 : row.volumeUsd, 0);
    return sum + volume;
  }, 0);
  const normalizedRows = allRows.map((row, index) => {
    const symbol = String((row === null || row === void 0 ? void 0 : row.symbol) || (row === null || row === void 0 ? void 0 : row.market) || "").trim().toUpperCase();
    const volume = toNum((row === null || row === void 0 ? void 0 : row.volume_24h_usd) !== undefined ? row.volume_24h_usd : (row === null || row === void 0 ? void 0 : row.volume_usd) !== undefined ? row.volume_usd : row === null || row === void 0 ? void 0 : row.volumeUsd, 0);
    const volumeRank = Math.max(1, Number((row === null || row === void 0 ? void 0 : row.rank) || index + 1));
    const priceRow = resolvePriceRowForSymbol(symbol, priceLookup);
    const mark = toNum(priceRow === null || priceRow === void 0 ? void 0 : priceRow.mark, NaN);
    const yesterday = toNum((priceRow === null || priceRow === void 0 ? void 0 : priceRow.yesterday_price) !== undefined ? priceRow === null || priceRow === void 0 ? void 0 : priceRow.yesterday_price : priceRow === null || priceRow === void 0 ? void 0 : priceRow.yesterdayPrice, NaN);
    const deltaFromPrice = Number.isFinite(mark) && Number.isFinite(yesterday) && yesterday !== 0 ? (mark - yesterday) / yesterday * 100 : NaN;
    const deltaPct = Number.isFinite(deltaFromPrice) ? deltaFromPrice : toNum((row === null || row === void 0 ? void 0 : row.change_24h_pct) !== undefined ? row.change_24h_pct : row === null || row === void 0 ? void 0 : row.change24hPct, 0);
    let openInterestUsd = toNum(row === null || row === void 0 ? void 0 : row.open_interest_usd, NaN);
    if (!Number.isFinite(openInterestUsd) && priceRow) {
      const oi = toNum(priceRow === null || priceRow === void 0 ? void 0 : priceRow.open_interest, 0);
      openInterestUsd = Number.isFinite(mark) ? oi * mark : 0;
    }
    const sharePct = totalVolumeUniverse > 0 ? volume / totalVolumeUniverse * 100 : 0;
    const movement = getTidalMovement(deltaPct);
    return {
      symbol,
      symbolKey: buildVolumeSymbolCandidates(symbol).join("|"),
      pairLabel: buildPerpPairLabel(symbol),
      volume,
      sharePct,
      deltaPct,
      movement,
      volumeRank,
      openInterestUsd: Number.isFinite(openInterestUsd) ? openInterestUsd : 0,
      momentumScore: 0,
      sparkline: [],
      reasonChips: []
    };
  });
  const maxVolume = normalizedRows.reduce((max, row) => Math.max(max, toNum(row === null || row === void 0 ? void 0 : row.volume, 0)), 0);
  const maxAbsDelta = normalizedRows.reduce((max, row) => Math.max(max, Math.abs(toNum(row === null || row === void 0 ? void 0 : row.deltaPct, 0))), 0);
  normalizedRows.forEach(row => {
    row.momentumScore = toNum(row.deltaPct, 0) * Math.sqrt(Math.max(toNum(row.sharePct, 0), 0) + 1) + Math.log10(Math.max(toNum(row.volume, 0), 1)) * 0.6;
    row.sparkline = buildTidalSparklineHeights(row, maxVolume, maxAbsDelta);
  });
  const momentumLeaderCount = Math.min(5, normalizedRows.length);
  const momentumLeaderKeys = new Set(normalizedRows.slice().sort((a, b) => toNum(b.momentumScore, 0) - toNum(a.momentumScore, 0)).slice(0, momentumLeaderCount).map(row => row.symbolKey));
  normalizedRows.forEach(row => {
    row.reasonChips = buildTidalReasonChips(row, momentumLeaderKeys.has(row.symbolKey));
  });
  const filterNeedle = String(state.volumeFilter || "").trim().toUpperCase();
  let rankedRows = normalizedRows.filter(row => {
    if (!filterNeedle) return true;
    return String((row === null || row === void 0 ? void 0 : row.symbol) || "").toUpperCase().includes(filterNeedle) || String((row === null || row === void 0 ? void 0 : row.pairLabel) || "").toUpperCase().includes(filterNeedle);
  });
  if (state.volumeSort === "share") {
    rankedRows = rankedRows.sort((a, b) => {
      const av = toNum(a === null || a === void 0 ? void 0 : a.sharePct, 0);
      const bv = toNum(b === null || b === void 0 ? void 0 : b.sharePct, 0);
      if (bv === av) return toNum(b === null || b === void 0 ? void 0 : b.volume, 0) - toNum(a === null || a === void 0 ? void 0 : a.volume, 0);
      return bv - av;
    });
  } else if (state.volumeSort === "momentum") {
    rankedRows = rankedRows.sort((a, b) => {
      const av = toNum(a === null || a === void 0 ? void 0 : a.momentumScore, 0);
      const bv = toNum(b === null || b === void 0 ? void 0 : b.momentumScore, 0);
      if (bv === av) return toNum(b === null || b === void 0 ? void 0 : b.volume, 0) - toNum(a === null || a === void 0 ? void 0 : a.volume, 0);
      return bv - av;
    });
  } else {
    rankedRows = rankedRows.sort((a, b) => {
      const av = toNum(a === null || a === void 0 ? void 0 : a.volume, 0);
      const bv = toNum(b === null || b === void 0 ? void 0 : b.volume, 0);
      return bv - av;
    });
  }
  const totalRows = rankedRows.length;
  state.volumeRankFilteredTotal = totalRows;
  const pageSize = Math.max(1, Number(state.volumeRankPageSize || 10));
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize));
  state.volumeRankPage = Math.max(1, Math.min(totalPages, Number(state.volumeRankPage || 1)));
  const pageStart = (state.volumeRankPage - 1) * pageSize;
  const pageRows = rankedRows.slice(pageStart, pageStart + pageSize);
  const rankSourceLabel = String((payload === null || payload === void 0 ? void 0 : (_payload$source4 = payload.source) === null || _payload$source4 === void 0 ? void 0 : _payload$source4.volumeRankSource) || "n/a").replace(/^\/api\//, "").split(":")[0];
  const rankWindowLabel = String((payload === null || payload === void 0 ? void 0 : (_payload$source5 = payload.source) === null || _payload$source5 === void 0 ? void 0 : _payload$source5.volumeRankWindowUsed) || state.timeframe || "24h").toUpperCase();
  setText("tidal-last-updated", `updated ${fmtAgo(payload.generatedAt)}`);
  setText("tidal-source-tag", `source ${rankWindowLabel} · ${rankSourceLabel || "n/a"}`);
  document.querySelectorAll("#volume-sort-toggle .sort-mode-btn").forEach(btn => {
    btn.classList.toggle("active", btn.dataset.sortMode === state.volumeSort);
  });
  if (body) {
    body.innerHTML = pageRows.length ? pageRows.map((row, index) => {
      const rank = pageStart + index + 1;
      const tier = getTidalTier(rank);
      const volume = toNum(row === null || row === void 0 ? void 0 : row.volume, 0);
      const share = toNum(row === null || row === void 0 ? void 0 : row.sharePct, 0);
      const delta = toNum(row === null || row === void 0 ? void 0 : row.deltaPct, 0);
      const dominance = maxVolume > 0 ? clamp(volume / maxVolume * 100, 7, 100) : 0;
      const deltaClass = delta >= 0 ? "up" : "down";
      const movementIcon = (row === null || row === void 0 ? void 0 : row.movement) === "up" ? "▲" : (row === null || row === void 0 ? void 0 : row.movement) === "down" ? "▼" : "•";
      const reasonMarkup = (Array.isArray(row === null || row === void 0 ? void 0 : row.reasonChips) ? row.reasonChips : []).map(chip => `<span class="tidal-reason-chip">${escapeHtml(chip)}</span>`).join("");
      const sparklineMarkup = (Array.isArray(row === null || row === void 0 ? void 0 : row.sparkline) ? row.sparkline : []).map(point => `<span style="--spark-h:${toNum(point, 20).toFixed(2)}%"></span>`).join("");
      return `<article class="tidal-row tier-${tier.key} movement-${(row === null || row === void 0 ? void 0 : row.movement) || "flat"}${rank <= 3 ? " is-prestige" : ""}" style="--tidal-dominance:${dominance.toFixed(2)}%">
              <div class="tidal-row-fill"></div>
              <div class="tidal-row-grid">
                <div class="tidal-rank-slot">
                  <span class="tidal-rank-pill">#${String(rank).padStart(2, "0")}</span>
                  <span class="tidal-tier-chip">${tier.label}</span>
                </div>
                <div class="tidal-symbol-slot">
                  <div class="tidal-symbol-line">
                    <strong>${escapeHtml((row === null || row === void 0 ? void 0 : row.symbol) || "-")}</strong>
                    <span class="tidal-pair-label">${escapeHtml((row === null || row === void 0 ? void 0 : row.pairLabel) || "-")}</span>
                  </div>
                  <div class="tidal-detail-line">
                    <span class="tidal-movement-chip ${(row === null || row === void 0 ? void 0 : row.movement) || "flat"}">${movementIcon} ${fmtSigned(delta, 2)}%</span>
                    <span class="tidal-sparkline" aria-hidden="true">${sparklineMarkup}</span>
                    <span class="tidal-reason-strip">${reasonMarkup}</span>
                  </div>
                </div>
                <div class="tidal-metrics-slot">
                  <strong class="tidal-volume-value">$${fmtCompact(volume)}</strong>
                  <span class="tidal-share-value">${fmt(share, 2)}% share</span>
                  <span class="tidal-delta-value ${deltaClass}">${fmtSigned(delta, 2)}%</span>
                </div>
              </div>
            </article>`;
    }).join("") : "<div class='tidal-empty muted'>No symbols match the current filter.</div>";
  }
  setText("volume-total-label", `${fmt(totalRows, 0)} symbols`);
  setText("volume-page-label", `Page ${state.volumeRankPage} / ${totalPages}`);
  const volumePrev = el("volume-prev-btn");
  const volumeNext = el("volume-next-btn");
  if (volumePrev) volumePrev.disabled = state.volumeRankPage <= 1;
  if (volumeNext) volumeNext.disabled = state.volumeRankPage >= totalPages;
}
async function runTerminalSearch() {
  var _state$exchange11, _rankRows$find;
  const input = el("terminal-search-input");
  const raw = input ? String(input.value || "").trim() : "";
  if (!raw) return;
  const looksLikeWallet = raw.length >= 32;
  if (looksLikeWallet) {
    applyView("wallets");
    state.walletSearch = raw;
    state.walletPage = 1;
    const walletSearchInput = el("wallet-search");
    if (walletSearchInput) walletSearchInput.value = raw;
    await refreshWallets();
    return;
  }
  const normalized = raw.toUpperCase();
  const rankRows = Array.isArray((_state$exchange11 = state.exchange) === null || _state$exchange11 === void 0 ? void 0 : _state$exchange11.volumeRank) ? state.exchange.volumeRank : [];
  const matchedSymbol = (_rankRows$find = rankRows.find(row => String((row === null || row === void 0 ? void 0 : row.symbol) || "").toUpperCase() === normalized)) === null || _rankRows$find === void 0 ? void 0 : _rankRows$find.symbol;
  const resolved = matchedSymbol || resolveCanonicalTokenSymbol(normalized);
  if (resolved) {
    applyView("exchange");
    state.volumeFilter = String(resolved).toUpperCase();
    state.volumeRankPage = 1;
    const filterInput = el("volume-filter");
    if (filterInput) filterInput.value = state.volumeFilter;
    renderExchange();
  }
}
function renderWallets() {
  const payload = state.wallets || {};
  const rows = Array.isArray(payload.rows) ? payload.rows : [];
  setText("wallet-total-label", `${payload.total || 0} wallets`);
  setText("wallet-page-label", `Page ${payload.page || 1} / ${payload.pages || 1}`);
  const body = el("wallet-table-body");
  if (body) {
    body.innerHTML = rows.map(row => {
      var _row$lifecycle;
      const lifecycleStage = (row === null || row === void 0 ? void 0 : (_row$lifecycle = row.lifecycle) === null || _row$lifecycle === void 0 ? void 0 : _row$lifecycle.lifecycleStage) || "unknown";
      const stageLabel = String(lifecycleStage).replace(/_/g, " ");
      return `<tr>
          <td>${row.rank}</td>
          <td class="wallet-cell" title="${row.wallet}">${row.wallet}</td>
          <td>${fmt(row.trades, 0)}</td>
          <td>$${fmtCompact(row.volumeUsd)}</td>
          <td>${fmt(row.totalWins, 0)}</td>
          <td>${fmt(row.totalLosses, 0)}</td>
          <td class="${toNum(row.pnlUsd, 0) >= 0 ? "good" : "bad"}">${fmtSigned(row.pnlUsd, 2)}</td>
          <td>${fmt(row.winRate, 2)}%</td>
          <td>${fmtTime(row.firstTrade)}</td>
          <td>${fmtTime(row.lastTrade)}</td>
          <td><span class="muted-pill">${stageLabel}</span></td>
          <td><button class="btn-ghost inspect-btn" data-wallet="${row.wallet}">Inspect</button></td>
        </tr>`;
    }).join("");
  }
  const prevBtn = el("wallet-prev-btn");
  const nextBtn = el("wallet-next-btn");
  if (prevBtn) prevBtn.disabled = (payload.page || 1) <= 1;
  if (nextBtn) nextBtn.disabled = (payload.page || 1) >= (payload.pages || 1);
}
function renderWalletProfile() {
  const profile = state.walletProfile;
  const kpiWrap = el("wallet-profile-kpis");
  const symbolsBody = el("wallet-profile-symbols");
  if (!profile || !profile.found || !profile.summary) {
    setText("wallet-profile-id", "Select a wallet");
    if (kpiWrap) kpiWrap.innerHTML = "<div class='muted'>No wallet selected.</div>";
    if (symbolsBody) symbolsBody.innerHTML = "";
    return;
  }
  setText("wallet-profile-id", profile.wallet);
  const s = profile.summary;
  if (kpiWrap) {
    var _profile$lifecycle;
    const lifecycleStage = (profile === null || profile === void 0 ? void 0 : (_profile$lifecycle = profile.lifecycle) === null || _profile$lifecycle === void 0 ? void 0 : _profile$lifecycle.lifecycleStage) || "unknown";
    kpiWrap.innerHTML = [["Rank", `#${s.rank || "-"}`], ["Trades", fmt(s.trades, 0)], ["Volume", `$${fmtCompact(s.volumeUsd)}`], ["PnL", fmtSigned(s.pnlUsd, 2)], ["Win Rate", `${fmt(s.winRate, 2)}%`], ["First Trade", fmtTime(s.firstTrade)], ["Last Trade", fmtTime(s.lastTrade)], ["Fees", `$${fmtCompact(s.feesUsd)}`], ["Stage", String(lifecycleStage).replace(/_/g, " ")]].map(([label, value]) => `<div class="profile-kpi"><div>${label}</div><strong>${value}</strong></div>`).join("");
  }
  if (symbolsBody) {
    const rows = Array.isArray(profile.symbolBreakdown) ? profile.symbolBreakdown : [];
    symbolsBody.innerHTML = rows.map(row => `<tr>
          <td>${row.symbol}</td>
          <td>$${row.volumeCompact}</td>
        </tr>`).join("");
  }
}
function applyView(nextView, options = {}) {
  var _el2, _el3;
  const skipRoute = Boolean(options.skipRoute);
  const replaceRoute = Boolean(options.replaceRoute);
  state.view = nextView;
  document.querySelectorAll(".tab-btn").forEach(btn => {
    btn.classList.toggle("active", btn.dataset.view === nextView);
  });
  (_el2 = el("exchange-view")) === null || _el2 === void 0 ? void 0 : _el2.classList.toggle("active", nextView === "exchange");
  (_el3 = el("wallets-view")) === null || _el3 === void 0 ? void 0 : _el3.classList.toggle("active", nextView === "wallets");
  if (!skipRoute) {
    syncRouteFromState({
      replace: replaceRoute
    });
  }
}
async function refreshExchange() {
  return runSingleFlight("exchange", async () => {
    var _data$source;
    const data = await fetchJson(`/api/exchange/overview?timeframe=${encodeURIComponent(state.timeframe)}`);
    if (!Array.isArray(data === null || data === void 0 ? void 0 : (_data$source = data.source) === null || _data$source === void 0 ? void 0 : _data$source.prices) || !data.source.prices.length) {
      try {
        const pricesPayload = await fetchJson("/api/prices");
        if (Array.isArray(pricesPayload === null || pricesPayload === void 0 ? void 0 : pricesPayload.prices) && pricesPayload.prices.length) {
          data.source = {
            ...(data.source || {}),
            prices: pricesPayload.prices
          };
        }
      } catch (_error) {
        // Keep overview payload as-is when fallback prices endpoint is unavailable.
      }
    }
    state.exchange = data;
    renderStatusBars();
    renderExchange();
    renderIndexerProgressPanel();
    return data;
  });
}
async function refreshDashboard() {
  const data = await fetchJson("/api/dashboard");
  state.dashboard = data;
}
async function refreshTokenAnalytics(options = {}) {
  const force = Boolean(options.force);
  const symbol = ensureTokenSymbol();
  if (!symbol) {
    state.tokenAnalytics = null;
    return null;
  }
  const existing = getTokenAnalyticsSnapshot(symbol);
  const existingAgeMs = Date.now() - Number((existing === null || existing === void 0 ? void 0 : existing.generatedAt) || 0);
  if (!force && existing && existingAgeMs >= 0 && existingAgeMs < 10000) {
    return existing;
  }
  const params = new URLSearchParams({
    symbol,
    timeframe: String(state.timeframe || "all")
  });
  if (force) params.set("force", "1");
  const payload = await fetchJson(`/api/token-analytics?${params.toString()}`);
  if (payload && payload.ok !== false) {
    state.tokenAnalytics = payload;
    return payload;
  }
  return null;
}
async function refreshVolumeSeries() {
  return runSingleFlight("volumeSeries", async () => {
    const data = await fetchJson("/api/volume-series");
    if (data && data.volume && data.fees) {
      state.volumeSeries = data;
    } else if (data && data.daily) {
      state.volumeSeries = {
        volume: data,
        fees: {
          daily: [],
          weekly: [],
          monthly: [],
          yearly: []
        }
      };
    } else {
      state.volumeSeries = data;
    }
    renderActiveSeries();
    return state.volumeSeries;
  });
}
async function refreshWallets() {
  return runSingleFlight("wallets", async () => {
    const params = new URLSearchParams({
      timeframe: state.timeframe,
      q: state.walletSearch,
      page: String(state.walletPage),
      pageSize: String(state.walletPageSize)
    });
    const data = await fetchJson(`/api/wallets?${params.toString()}`);
    state.wallets = data;
    renderWallets();
    return data;
  });
}
async function inspectWallet(wallet) {
  if (!wallet) return;
  state.selectedWallet = wallet;
  return runSingleFlight("walletProfile", async () => {
    const params = new URLSearchParams({
      wallet,
      timeframe: state.timeframe
    });
    state.walletProfile = await fetchJson(`/api/wallets/profile?${params.toString()}`);
    renderWalletProfile();
    return state.walletProfile;
  });
}
async function refreshAll() {
  await Promise.allSettled([refreshExchange(), refreshVolumeSeries(), refreshWallets()]);
  if (state.selectedWallet) {
    await inspectWallet(state.selectedWallet).catch(() => null);
  }
}
async function setTrackedWallet() {
  const input = el("account-input");
  const wallet = input ? input.value.trim() : "";
  await postJson("/api/config/account", {
    account: wallet
  });
  state.walletPage = 1;
  await refreshAll();
}
async function loadInitialWallet() {
  const cfg = await fetchJson("/api/config/account");
  if (el("account-input")) {
    el("account-input").value = cfg.account || "";
  }
}
function bindEvents() {
  var _el4, _el5, _el6, _el7, _el8, _el9, _el10, _el11, _el12, _el13, _el14, _el15, _el16, _el17, _el18, _el19, _el20, _el21;
  document.querySelectorAll(".tab-btn").forEach(btn => {
    btn.addEventListener("click", () => applyView(btn.dataset.view));
  });
  document.querySelectorAll("[data-timeframe]").forEach(btn => {
    btn.addEventListener("click", async () => {
      state.timeframe = btn.dataset.timeframe || "all";
      state.analyticsTab = "volume";
      state.volumeRankPage = 1;
      syncTimeframeButtons();
      state.walletPage = 1;
      await refreshAll();
    });
  });
  (_el4 = el("terminal-search-btn")) === null || _el4 === void 0 ? void 0 : _el4.addEventListener("click", async () => {
    await runTerminalSearch();
  });
  (_el5 = el("terminal-search-input")) === null || _el5 === void 0 ? void 0 : _el5.addEventListener("keydown", async event => {
    if (event.key !== "Enter") return;
    event.preventDefault();
    await runTerminalSearch();
  });
  (_el6 = el("apply-account-btn")) === null || _el6 === void 0 ? void 0 : _el6.addEventListener("click", async () => {
    await setTrackedWallet();
  });
  (_el7 = el("refresh-all-btn")) === null || _el7 === void 0 ? void 0 : _el7.addEventListener("click", async () => {
    await postJson("/api/snapshot/refresh", {});
    await postJson("/api/indexer/discover", {}).catch(() => null);
    await postJson("/api/indexer/scan", {}).catch(() => null);
    await refreshAll();
  });
  (_el8 = el("wallet-refresh-btn")) === null || _el8 === void 0 ? void 0 : _el8.addEventListener("click", async () => {
    await refreshWallets();
    if (state.selectedWallet) await inspectWallet(state.selectedWallet);
  });
  (_el9 = el("wallet-reindex-btn")) === null || _el9 === void 0 ? void 0 : _el9.addEventListener("click", async () => {
    const confirmed = window.confirm("Reindex from zero for all discovered wallets?\n\nThis keeps the discovered wallet registry, but clears per-wallet indexing progress/history.");
    if (!confirmed) return;
    try {
      await postJson("/api/indexer/reset", {
        preserveKnownWallets: true,
        resetWalletStore: true,
        clearHistoryFiles: true
      });
      await postJson("/api/indexer/discover", {}).catch(() => null);
      await postJson("/api/indexer/scan", {}).catch(() => null);
      state.walletPage = 1;
      state.selectedWallet = null;
      state.walletProfile = null;
      await refreshAll();
    } catch (error) {
      window.alert(`Reindex reset failed: ${error.message}`);
    }
  });
  (_el10 = el("wallet-search")) === null || _el10 === void 0 ? void 0 : _el10.addEventListener("input", async event => {
    state.walletSearch = event.target.value || "";
    state.walletPage = 1;
    await refreshWallets();
  });
  (_el11 = el("wallet-page-size")) === null || _el11 === void 0 ? void 0 : _el11.addEventListener("change", async event => {
    state.walletPageSize = Number(event.target.value || 20);
    state.walletPage = 1;
    await refreshWallets();
  });
  (_el12 = el("wallet-prev-btn")) === null || _el12 === void 0 ? void 0 : _el12.addEventListener("click", async () => {
    state.walletPage = Math.max(1, state.walletPage - 1);
    await refreshWallets();
  });
  (_el13 = el("wallet-next-btn")) === null || _el13 === void 0 ? void 0 : _el13.addEventListener("click", async () => {
    const pages = state.wallets && state.wallets.pages ? state.wallets.pages : 1;
    state.walletPage = Math.min(pages, state.walletPage + 1);
    await refreshWallets();
  });
  (_el14 = el("wallet-table-body")) === null || _el14 === void 0 ? void 0 : _el14.addEventListener("click", async event => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    const btn = target.closest(".inspect-btn");
    if (!btn) return;
    const wallet = btn.getAttribute("data-wallet");
    await inspectWallet(wallet);
  });
  (_el15 = el("volume-filter")) === null || _el15 === void 0 ? void 0 : _el15.addEventListener("input", event => {
    state.volumeFilter = String(event.target.value || "");
    state.volumeRankPage = 1;
    renderExchange();
  });
  document.querySelectorAll("#volume-sort-toggle .sort-mode-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      const nextSort = String(btn.dataset.sortMode || "volume");
      if (!nextSort || state.volumeSort === nextSort) return;
      state.volumeSort = nextSort;
      state.volumeRankPage = 1;
      renderExchange();
    });
  });
  (_el16 = el("volume-prev-btn")) === null || _el16 === void 0 ? void 0 : _el16.addEventListener("click", () => {
    state.volumeRankPage = Math.max(1, state.volumeRankPage - 1);
    renderExchange();
  });
  (_el17 = el("volume-next-btn")) === null || _el17 === void 0 ? void 0 : _el17.addEventListener("click", () => {
    var _state$exchange12;
    const totalRows = Math.max(0, Number(state.volumeRankFilteredTotal || (Array.isArray((_state$exchange12 = state.exchange) === null || _state$exchange12 === void 0 ? void 0 : _state$exchange12.volumeRank) ? state.exchange.volumeRank.length : 0)));
    const totalPages = Math.max(1, Math.ceil(totalRows / Math.max(1, state.volumeRankPageSize)));
    state.volumeRankPage = Math.min(totalPages, state.volumeRankPage + 1);
    renderExchange();
  });
  (_el18 = el("seriesDaily")) === null || _el18 === void 0 ? void 0 : _el18.addEventListener("click", () => updateSeriesToggle("daily"));
  (_el19 = el("seriesWeekly")) === null || _el19 === void 0 ? void 0 : _el19.addEventListener("click", () => updateSeriesToggle("weekly"));
  (_el20 = el("seriesMonthly")) === null || _el20 === void 0 ? void 0 : _el20.addEventListener("click", () => updateSeriesToggle("monthly"));
  (_el21 = el("chartCumulative")) === null || _el21 === void 0 ? void 0 : _el21.addEventListener("click", () => updateChartType("line"));
  const volumeChart = el("volumeChart");
  if (volumeChart) {
    volumeChart.addEventListener("wheel", handleChartWheel, {
      passive: false
    });
    volumeChart.addEventListener("pointerdown", handleChartPointerDown);
    window.addEventListener("pointermove", handleChartPointerMove);
    window.addEventListener("pointerup", handleChartPointerUp);
    window.addEventListener("pointercancel", handleChartPointerUp);
  }
  const chartNavigator = el("chartNavigator");
  if (chartNavigator && ENABLE_CHART_BRUSH) {
    chartNavigator.addEventListener("wheel", handleChartWheel, {
      passive: false
    });
    chartNavigator.addEventListener("pointerdown", handleNavigatorPointerDown);
    window.addEventListener("pointermove", handleNavigatorPointerMove);
    window.addEventListener("pointerup", handleNavigatorPointerUp);
    window.addEventListener("pointercancel", handleNavigatorPointerUp);
  } else if (chartNavigator) {
    chartNavigator.hidden = true;
  }
  window.addEventListener("resize", () => {
    if (tickerResizeRaf) window.cancelAnimationFrame(tickerResizeRaf);
    tickerResizeRaf = window.requestAnimationFrame(() => {
      tickerResizeRaf = 0;
      if (state.exchange) renderMarketsTicker(state.exchange);
    });
    handleChartResize();
  }, {
    passive: true
  });
  window.addEventListener("orientationchange", () => {
    handleChartResize();
    setTimeout(handleChartResize, 160);
  }, {
    passive: true
  });
  window.addEventListener("pageshow", handleChartResize, {
    passive: true
  });
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
      handleChartResize();
      refreshExchange().catch(() => null);
      refreshVolumeSeries().catch(() => null);
      if (state.view === "wallets" || state.selectedWallet) {
        refreshWallets().catch(() => null);
      }
      if (state.selectedWallet) {
        inspectWallet(state.selectedWallet).catch(() => null);
      }
    }
  });
  if (window.visualViewport) {
    window.visualViewport.addEventListener("resize", queueChartViewportResize, {
      passive: true
    });
    window.visualViewport.addEventListener("scroll", queueChartViewportResize, {
      passive: true
    });
  }
  window.addEventListener("popstate", () => {
    applyRouteFromLocation({
      replace: false
    });
  });
  initChartResizeObserver();
}
function initBackgroundMotion() {
  if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
  const root = document.documentElement;
  let targetX = 0;
  let targetY = 0;
  let currentX = 0;
  let currentY = 0;
  let rafId = 0;
  const tick = () => {
    rafId = 0;
    currentX += (targetX - currentX) * 0.08;
    currentY += (targetY - currentY) * 0.08;
    root.style.setProperty("--pf-mx", currentX.toFixed(4));
    root.style.setProperty("--pf-my", currentY.toFixed(4));
    if (Math.abs(targetX - currentX) > 0.0008 || Math.abs(targetY - currentY) > 0.0008) {
      rafId = window.requestAnimationFrame(tick);
    }
  };
  const schedule = () => {
    if (!rafId) rafId = window.requestAnimationFrame(tick);
  };
  window.addEventListener("pointermove", event => {
    const w = window.innerWidth || 1;
    const h = window.innerHeight || 1;
    targetX = (event.clientX / w * 2 - 1) * 0.85;
    targetY = (event.clientY / h * 2 - 1) * 0.85;
    schedule();
  }, {
    passive: true
  });
  window.addEventListener("pointerleave", () => {
    targetX = 0;
    targetY = 0;
    schedule();
  }, {
    passive: true
  });
}
async function init() {
  if (typeof window !== "undefined") {
    window.__PF_APP_BOOT_STEP = "init_start";
    window.__PF_APP_BOOTED = true;
  }
  if (typeof window !== "undefined") {
    window.__PF_APP_BOOT_STEP = "bind_events";
  }
  bindEvents();
  if (typeof window !== "undefined") {
    window.__PF_APP_BOOT_STEP = "background_motion";
  }
  initBackgroundMotion();
  if (typeof window !== "undefined") {
    window.__PF_APP_BOOT_STEP = "apply_route";
  }
  applyRouteFromLocation({
    replace: true
  });
  if (typeof window !== "undefined") {
    window.__PF_APP_BOOT_STEP = "load_initial_wallet";
  }
  await loadInitialWallet().catch(() => null);
  if (typeof window !== "undefined") {
    window.__PF_APP_BOOT_STEP = "refresh_exchange";
  }
  await refreshExchange().catch(error => {
    console.warn("initial exchange refresh failed", error);
  });
  refreshVolumeSeries().catch(() => null);
  refreshWallets().catch(() => null);
  if (state.selectedWallet) {
    inspectWallet(state.selectedWallet).catch(() => null);
  }
  startPollingLoop("exchange", () => refreshExchange(), 8000);
  startPollingLoop("volumeSeries", () => refreshVolumeSeries(), 30000);
  startPollingLoop("wallets", () => refreshWallets(), 12000);
  startPollingLoop("walletProfile", () => {
    if (!state.selectedWallet) return Promise.resolve(null);
    return inspectWallet(state.selectedWallet);
  }, 15000);
  if (typeof window !== "undefined") {
    window.__PF_APP_BOOT_STEP = "steady_state";
  }
}
init().catch(error => {
  console.error(error);
  if (typeof window !== "undefined" && typeof window.__PF_APP_SHOW_RUNTIME_ERROR === "function") {
    window.__PF_APP_SHOW_RUNTIME_ERROR(error && error.message ? error.message : "unknown");
  }
  setText("status-sync", `Error: ${error.message}`);
});
