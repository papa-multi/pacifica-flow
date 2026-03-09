const state = {
  view: "exchange",
  timeframe: "all",
  exchange: null,
  volumeSeries: null,
  volumeRankPage: 1,
  volumeRankPageSize: 10,
  wallets: null,
  walletProfile: null,
  walletSearch: "",
  walletPage: 1,
  walletPageSize: 20,
  selectedWallet: null,
};
const tickerState = {
  signature: "",
  layout: "",
};
let tickerResizeRaf = 0;
let chartResizeRaf = 0;
let activeSeries = "monthly";
let activeChartType = "bars";
let chartHasUserControl = false;
const ENABLE_CHART_BRUSH = true;
const chartState = {
  window: 0,
  offset: 0,
};
const navigatorState = {
  mode: null,
  startX: 0,
  startOffset: 0,
  startWindow: 0,
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
    timeZone: "UTC",
  }),
  weekly: new Intl.DateTimeFormat("en-GB", {
    day: "2-digit",
    month: "short",
    year: "numeric",
    timeZone: "UTC",
  }),
  monthly: new Intl.DateTimeFormat("en-GB", {
    month: "short",
    year: "numeric",
    timeZone: "UTC",
  }),
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
    maximumFractionDigits: digits,
  });
}

function fmtSigned(value, digits = 2) {
  const num = toNum(value, NaN);
  if (!Number.isFinite(num)) return "-";
  return `${num > 0 ? "+" : ""}${fmt(num, digits)}`;
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

function fmtTime(ts) {
  const num = Number(ts);
  if (!Number.isFinite(num) || num <= 0) return "-";
  return new Date(num).toLocaleString();
}

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
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
    maximumFractionDigits: maxDigits,
  })}`;
}

function formatTickerPercent(value) {
  const num = toNum(value, NaN);
  if (!Number.isFinite(num)) return "N/A";
  return `${num > 0 ? "+" : ""}${num.toFixed(2)}%`;
}

function buildTickerItemMarkup(item = {}) {
  const symbol = sanitizeTickerLabel(item.displaySymbol || item.symbol || "UNKNOWN");
  const quote = sanitizeTickerLabel(item.quoteAsset || "USD");
  const price = formatTickerPrice(item.price);
  const change = Number(item.changePct24h);
  const changeClass = Number.isFinite(change)
    ? change > 0
      ? "up"
      : change < 0
        ? "down"
        : "flat"
    : "unknown";
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
  const rows =
    payload &&
    payload.source &&
    Array.isArray(payload.source.prices) &&
    payload.source.prices.length
      ? payload.source.prices
      : [];

  return rows
    .map((row) => {
      const symbol = sanitizeTickerLabel(row && row.symbol ? row.symbol : "");
      if (!symbol) return null;

      const price = toNum(
        row && row.mark !== undefined
          ? row.mark
          : row && row.price !== undefined
            ? row.price
            : NaN,
        NaN
      );
      const prev = toNum(
        row && row.yesterday_price !== undefined
          ? row.yesterday_price
          : row && row.yesterdayPrice !== undefined
            ? row.yesterdayPrice
            : NaN,
        NaN
      );
      let changePct24h = NaN;
      if (Number.isFinite(price) && Number.isFinite(prev) && prev !== 0) {
        changePct24h = ((price - prev) / prev) * 100;
      } else {
        changePct24h = toNum(
          row && row.change_24h_pct !== undefined
            ? row.change_24h_pct
            : row && row.change24hPct !== undefined
              ? row.change24hPct
              : NaN,
          NaN
        );
      }

      return {
        symbol,
        displaySymbol: symbol,
        quoteAsset: "USD",
        price,
        changePct24h,
        volume24h: toNum(
          row && row.volume_24h !== undefined
            ? row.volume_24h
            : row && row.volume24h !== undefined
              ? row.volume24h
              : 0,
          0
        ),
      };
    })
    .filter((row) => row && row.symbol && Number.isFinite(row.price))
    .sort((a, b) => toNum(b.volume24h, 0) - toNum(a.volume24h, 0))
    .slice(0, 80);
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
  const signature = markets
    .map((item) => {
      const price = Number(item.price);
      const change = Number(item.changePct24h);
      return `${item.symbol}:${Number.isFinite(price) ? price.toFixed(8) : "na"}:${
        Number.isFinite(change) ? change.toFixed(6) : "na"
      }`;
    })
    .join("|");

  ticker.dataset.layout = layout;
  if (signature !== tickerState.signature || layout !== tickerState.layout) {
    const markup = markets.map((item) => buildTickerItemMarkup(item)).join("");
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
  return formatChartCurrency(value);
}

function formatMetricTooltip(value) {
  return formatChartCurrency(value);
}

function getSeriesValue(item) {
  if (!item) return 0;
  if (Number.isFinite(item.value)) return item.value;
  if (Number.isFinite(item.volume)) return item.volume;
  return 0;
}

function buildChartData(series) {
  if (!Array.isArray(series) || !series.length) return [];
  return series.map((item) => ({
    ...item,
    value: toFiniteNumber(getSeriesValue(item), 0),
  }));
}

function buildCumulativeSeries(series) {
  if (!Array.isArray(series) || !series.length) return [];
  let running = 0;
  return series.map((item) => {
    const stepValue = toFiniteNumber(getSeriesValue(item), 0);
    running += stepValue;
    return { ...item, value: running, stepValue };
  });
}

function buildSmoothLinePath(points) {
  if (!Array.isArray(points) || !points.length) return "";
  return points
    .map((point, index) => `${index === 0 ? "M" : "L"} ${point.x.toFixed(3)} ${point.y.toFixed(3)}`)
    .join(" ");
}

function formatChartLabel(item, mode = "axis") {
  if (!item) return "";
  const hasRange =
    Number.isFinite(item.bucketStart) &&
    Number.isFinite(item.bucketEnd) &&
    item.bucketEnd > item.bucketStart;
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
    return { items: [], binned: false, binSize: 1 };
  }
  const width = Number.isFinite(chartWidth) && chartWidth > 0 ? chartWidth : 720;
  const maxRenderableBars = Math.max(12, Math.floor(width / 4));
  if (series.length <= maxRenderableBars) {
    return { items: series, binned: false, binSize: 1 };
  }
  const binSize = Math.max(2, Math.ceil(series.length / maxRenderableBars));
  const binned = [];
  for (let index = 0; index < series.length; index += binSize) {
    const chunk = series.slice(index, Math.min(index + binSize, series.length));
    if (!chunk.length) continue;
    const first = chunk[0];
    const last = chunk[chunk.length - 1];
    const sumValue = chunk.reduce((total, item) => total + toFiniteNumber(item?.value, 0), 0);
    const firstTs = Number.isFinite(first?.timestamp) ? first.timestamp : null;
    const lastTs = Number.isFinite(last?.timestamp) ? last.timestamp : firstTs;
    const midpointTs =
      Number.isFinite(firstTs) && Number.isFinite(lastTs)
        ? Math.round((firstTs + lastTs) / 2)
        : Number.isFinite(lastTs)
        ? lastTs
        : Number.isFinite(firstTs)
        ? firstTs
        : null;
    binned.push({
      ...last,
      value: sumValue,
      timestamp: midpointTs,
      bucketStart: firstTs,
      bucketEnd: lastTs,
      bucketCount: chunk.length,
      binned: true,
    });
  }
  return { items: binned, binned: true, binSize };
}

function getScaleConfig(data, options = {}) {
  const baseline = options.baseline === "auto" ? "auto" : "zero";
  const values = (Array.isArray(data) ? data : [])
    .map((item) => toFiniteNumber(item?.value, Number.NaN))
    .filter((value) => Number.isFinite(value));
  if (!values.length) {
    return {
      min: 0,
      max: 1,
      span: 1,
      normalize: () => 0,
      axisAtRatio: (ratio) => clamp(toFiniteNumber(ratio, 0), 0, 1),
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
    normalize: (value) => clamp((toFiniteNumber(value, domainMin) - domainMin) / span, 0, 1),
    axisAtRatio: (ratio) => domainMin + span * clamp(toFiniteNumber(ratio, 0), 0, 1),
  };
}

function renderCumulativeLine(layer, data, scaleConfig, pointsContainer) {
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
    const xPct = data.length === 1 ? 50 : (index / (data.length - 1)) * 100;
    const x = (xPct / 100) * plotWidth;
    const ratio = scaleConfig?.normalize ? scaleConfig.normalize(item.value) : 0;
    const yPct = 100 - ratio * 100;
    const y = (yPct / 100) * plotHeight;
    return { x, y, xPct, yPct, item };
  });

  const linePathData = buildSmoothLinePath(points);
  const firstPoint = points[0];
  const lastPoint = points[points.length - 1];
  const areaPathData = `${linePathData} L ${lastPoint.x.toFixed(3)} ${plotHeight.toFixed(3)} L ${firstPoint.x.toFixed(3)} ${plotHeight.toFixed(3)} Z`;

  const areaPath = document.createElementNS("http://www.w3.org/2000/svg", "path");
  areaPath.setAttribute("d", areaPathData);
  areaPath.classList.add("chart-area-minimal");
  svg.appendChild(areaPath);

  const linePath = document.createElementNS("http://www.w3.org/2000/svg", "path");
  linePath.setAttribute("d", linePathData);
  linePath.classList.add("chart-line-minimal");
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
  const update = (clientX) => {
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

  container.addEventListener("pointermove", (event) => update(event.clientX));
  container.addEventListener("pointerleave", hide);
  container.addEventListener("pointerdown", hide);
}

function buildDateBandTicks(data, trackWidth) {
  if (!Array.isArray(data) || !data.length) return [];
  const total = data.length;
  if (total === 1) return [0];
  const minSpacingPx =
    activeSeries === "monthly" ? 92 : activeSeries === "weekly" ? 98 : activeChartType === "line" ? 112 : 104;
  const maxTicksByWidth =
    Number.isFinite(trackWidth) && trackWidth > 0 ? Math.max(2, Math.floor(trackWidth / minSpacingPx)) : 6;
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
  const chartDateBand = el("chartDateBand");
  if (!chartDateBand) return;
  const track = chartDateBand.querySelector(".chart-date-track") || chartDateBand;
  track.innerHTML = "";
  if (!Array.isArray(data) || !data.length) return;
  const total = data.length;
  const trackWidth = track.clientWidth || chartDateBand.clientWidth || 0;
  const ticks = buildDateBandTicks(data, trackWidth);
  const minDistancePx = activeSeries === "monthly" ? 70 : activeChartType === "line" ? 92 : 84;
  let lastRight = -Infinity;
  let lastLabel = "";

  ticks.forEach((index) => {
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

function renderVolumeChart(container, series) {
  if (!container) return;
  container.innerHTML = "";
  container.dataset.series = activeSeries;
  container.dataset.chartType = activeChartType;
  const useLine = activeChartType === "line";

  if (!Array.isArray(series) || !series.length) {
    container.innerHTML = "<div class='muted'>No data yet.</div>";
    renderDateBand([]);
    return;
  }

  const chartWidth = Math.max(320, (container.clientWidth || 720) - 62);
  const barAggregation = useLine
    ? { items: series, binned: false, binSize: 1 }
    : aggregateBarSeriesByWidth(series, chartWidth);
  const data = barAggregation.items;
  const scaleConfig = getScaleConfig(data, {
    baseline: useLine ? "auto" : "zero",
  });
  const chartHeight = container.clientHeight || 280;
  const maxBarHeight = Math.max(chartHeight - 108, 124);

  const axis = document.createElement("div");
  axis.className = "chart-axis";
  const bars = document.createElement("div");
  bars.className = "chart-bars";
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

  let hoverPoints = [];
  if (useLine) {
    const lineLayer = document.createElement("div");
    lineLayer.className = "chart-line-layer";
    bars.appendChild(lineLayer);
    hoverPoints = renderCumulativeLine(lineLayer, data, scaleConfig, bars);
  } else {
    hoverPoints = data.map((item, index) => ({
      xPct: data.length === 1 ? 50 : (index / (data.length - 1)) * 100,
      item,
    }));
  }

  data.forEach((item, index) => {
    if (useLine) return;
    const valueNum = toFiniteNumber(item.value, 0);
    const wrap = document.createElement("div");
    wrap.className = "volume-bar";
    const bar = document.createElement("div");
    bar.className = "bar";
    const ratio = scaleConfig.normalize ? scaleConfig.normalize(valueNum) : 0;
    const height = valueNum > 0 ? Math.max(Math.round(ratio * maxBarHeight), 2) : 0;
    bar.style.height = `${height}px`;
    const labelText = formatChartLabel(item, "tooltip");
    const tooltip = document.createElement("div");
    tooltip.className = "bar-tooltip";
    tooltip.textContent = `${labelText} · ${formatMetricTooltip(valueNum)}`;
    bar.appendChild(tooltip);
    wrap.appendChild(bar);

    const label = document.createElement("div");
    label.className = `bar-label${shouldShowAxisLabel(index, data.length) ? "" : " dim"}`;
    label.textContent = shouldShowAxisLabel(index, data.length) ? formatChartLabel(item, "axis") : "";
    wrap.appendChild(label);
    bars.appendChild(wrap);
  });

  attachLineHoverOverlay(bars, hoverPoints);
  renderDateBand(data);
}

function getActiveSeries() {
  const metricSet = state.volumeSeries && state.volumeSeries.volume ? state.volumeSeries.volume : null;
  if (!metricSet) return [];
  return Array.isArray(metricSet[activeSeries]) ? metricSet[activeSeries] : [];
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

  const values = series.map((item) => Math.max(0, getSeriesValue(item)));
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
    const x = values.length === 1 ? plotWidth / 2 : (index / (values.length - 1)) * plotWidth;
    const ratio = max ? valueNum / max : 0;
    const y = bottomInset - ratio * drawable;
    return { x, y };
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
  const leftPct = (start / total) * 100;
  const widthPct = ((end - start) / total) * 100;
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
  targets.forEach((target) => {
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
  const rect =
    chartNavigator && event.currentTarget === chartNavigator
      ? getNavigatorBounds()
      : getChartBounds();
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
  const focusIndex =
    chartState.offset + Math.round(ratio * Math.max(chartState.window - 1, 0));
  const delta = Math.sign(event.deltaY);
  const step = event.ctrlKey ? 1 : Math.max(1, Math.round(chartState.window * 0.08));
  const minWindow = Math.min(6, series.length);
  const maxWindow = Math.max(series.length, minWindow);
  let nextWindow = chartState.window + (delta > 0 ? step : -step);
  nextWindow = clamp(nextWindow, minWindow, maxWindow);
  if (nextWindow === chartState.window) return;

  const nextMaxOffset = Math.max(series.length - nextWindow, 0);
  const nextOffset = clamp(
    focusIndex - Math.round(ratio * Math.max(nextWindow - 1, 0)),
    0,
    nextMaxOffset
  );
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
  if (handle?.dataset?.handle === "left") {
    navigatorState.mode = "resize-left";
  } else if (handle?.dataset?.handle === "right") {
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
    chartState.offset = clamp(
      clickIndex - Math.floor(chartState.window / 2),
      0,
      maxOffset
    );
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
  const barsShift = Math.round((deltaX / rect.width) * total);

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
  const cumulativeActive = activeChartType === "line";
  const buttons = [
    ["seriesDaily", "daily"],
    ["seriesWeekly", "weekly"],
    ["seriesMonthly", "monthly"],
  ];
  buttons.forEach(([id, key]) => {
    const btn = el(id);
    if (!btn) return;
    const isActive = !cumulativeActive && key === activeSeries;
    btn.classList.toggle("active", isActive);
    btn.setAttribute("aria-pressed", isActive ? "true" : "false");
  });
  const cumulativeBtn = el("chartCumulative");
  if (cumulativeBtn) {
    cumulativeBtn.hidden = false;
    cumulativeBtn.classList.toggle("active", cumulativeActive);
    cumulativeBtn.setAttribute("aria-pressed", cumulativeActive ? "true" : "false");
  }
}

function renderActiveSeries() {
  const container = el("volumeChart");
  if (!container) return;
  const allSeries = getActiveSeries();
  syncChartWindow(allSeries);
  const prepared = buildChartData(allSeries);
  const visiblePrepared = getVisibleSeries(prepared);
  const chartData = activeChartType === "line" ? buildCumulativeSeries(visiblePrepared) : visiblePrepared;
  renderVolumeChart(container, chartData);
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
  const lifecycle = indexer?.lifecycle || {};
  const discovered = Number(lifecycle?.discovered ?? indexer?.knownWallets ?? 0);
  const pendingBackfill = Number(lifecycle?.pendingBackfill ?? indexer?.pendingWallets ?? 0);
  const backfilling = Number(lifecycle?.backfilling ?? indexer?.partiallyIndexedWallets ?? 0);
  const fullyIndexed = Number(lifecycle?.fullyIndexed ?? indexer?.indexedCompleteWallets ?? 0);
  const liveTracking = Number(lifecycle?.liveTracking ?? 0);
  const indexed = Number(lifecycle?.backfillComplete ?? indexer?.indexedCompleteWallets ?? 0);
  const partial = backfilling;
  const pending = pendingBackfill;
  const failed = Number(indexer?.failedWallets ?? 0);
  const failedBackfill = Number(lifecycle?.failedBackfill ?? indexer?.failedBackfillWallets ?? failed);
  const knownFallback = indexed + partial + pending + failed;
  const known = Number(indexer?.knownWallets ?? discovered ?? knownFallback);
  const completePct = known > 0 ? (indexed / known) * 100 : 0;
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
    backlog: pendingBackfill + backfilling + failedBackfill,
  };
}

async function fetchJson(url) {
  const res = await fetch(url);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.error || `${url} -> ${res.status}`);
  }
  return data;
}

async function postJson(url, payload = {}) {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.error || `${url} -> ${res.status}`);
  }
  return data;
}

function renderStatusBars() {
  const ex = state.exchange;
  const indexer = ex?.indexer || null;
  const progress = getIndexerBreakdown(indexer);
  const indexed = progress.indexed || Number(indexer?.indexedWallets ?? ex?.source?.walletsIndexed ?? 0);
  const known = progress.known || Number(indexer?.knownWallets ?? indexed);
  setText("status-sync", `Sync: ${fmtTime(ex?.sync?.lastBootstrapAt)}`);
  setText("status-live", `WS: ${ex?.sync?.wsStatus || "-"}`);
  setText(
    "status-wallet",
    `Wallets: ${fmt(progress.discovered, 0)} discovered | ${fmt(progress.liveTracking, 0)} live`
  );
  setText(
    "status-progress",
    `Backfill: ${fmt(progress.completePct, 2)}% (pending ${progress.pendingBackfill}, backfilling ${progress.backfilling}, failed ${progress.failedBackfill})`
  );
  setText("status-updated", `Updated: ${fmtTime(ex?.generatedAt)}`);

  setText("exchange-live-label", ex?.sync?.wsStatus === "open" ? "LIVE" : "SYNCING");
}

function renderIndexerProgressPanel() {
  const ex = state.exchange || {};
  const indexer = ex.indexer || null;
  const progress = getIndexerBreakdown(indexer);

  const kpiWrap = el("indexer-progress-kpis");
  if (kpiWrap) {
    kpiWrap.innerHTML = [
      ["Discovered", fmt(progress.discovered, 0)],
      ["Pending Backfill", fmt(progress.pendingBackfill, 0)],
      ["Backfilling", fmt(progress.backfilling, 0)],
      ["Backfill Complete", fmt(progress.indexed, 0)],
      ["Live Tracking", fmt(progress.liveTracking, 0)],
      ["Failed Backfill", fmt(progress.failedBackfill, 0)],
    ]
      .map(
        ([title, value]) =>
          `<article class="kpi-card"><div class="kpi-title">${title}</div><div class="kpi-value">${value}</div></article>`
      )
      .join("");
  }

  const bar = el("indexer-progress-bar");
  if (bar) bar.style.width = `${Math.max(0, Math.min(100, progress.completePct))}%`;

  setText("indexer-backlog-label", `Backlog: ${fmt(progress.backlog, 0)}`);

  const status = indexer || {};
  setText(
    "indexer-progress-meta",
    `Backfill ${fmt(progress.completePct, 2)}% | backfill queue ${fmt(status.priorityQueueSize || 0, 0)} | live queue ${fmt(
      status.liveQueueSize || 0,
      0
    )} | avg wait ${fmt(Number(status.averagePendingWaitMs || 0) / 60000, 1)}m | scans ok ${fmt(
      status.successfulScans || 0,
      0
    )} / failed ${fmt(status.failedScans || 0, 0)}`
  );
}

function renderExchange() {
  const payload = state.exchange || {};
  const kpis = payload.kpis || {};

  const kpiRows = [
    { key: "Total Accounts", value: fmt(kpis.totalAccounts || 0, 0) },
    { key: "Total Trades", value: fmt(kpis.totalTrades || 0, 0) },
    { key: "Total Volume", value: `$${fmtCompact(kpis.totalVolumeUsd)}` },
    { key: "Total Fees", value: `$${fmt(kpis.totalFeesUsd, 2)}` },
  ];

  const kpiContainer = el("exchange-kpis");
  if (kpiContainer) {
    kpiContainer.innerHTML = kpiRows
      .map(
        (row) => `<article class="kpi-card">
            <div class="kpi-title">${row.key}</div>
            <div class="kpi-value">${row.value}</div>
            ${row.meta ? `<div class="kpi-meta">${row.meta}</div>` : ""}
          </article>`
      )
      .join("");
  }

  renderMarketsTicker(payload);

  const body = el("volume-rank-body");
  const allRows = Array.isArray(payload.volumeRank) ? payload.volumeRank : [];
  const totalRows = allRows.length;
  const pageSize = Math.max(1, Number(state.volumeRankPageSize || 10));
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize));
  state.volumeRankPage = Math.max(1, Math.min(totalPages, Number(state.volumeRankPage || 1)));
  const pageStart = (state.volumeRankPage - 1) * pageSize;
  const pageRows = allRows.slice(pageStart, pageStart + pageSize);

  if (body) {
    body.innerHTML = pageRows
      .map(
        (row) => `<tr>
          <td><span class="rank-pill">#${row.rank}</span></td>
          <td><strong>${row.symbol}</strong></td>
          <td>$${fmtCompact(row.volume_24h_usd !== undefined ? row.volume_24h_usd : row.volumeUsd)}</td>
        </tr>`
      )
      .join("");
  }

  setText("volume-total-label", `${fmt(totalRows, 0)} symbols`);
  setText("volume-page-label", `Page ${state.volumeRankPage} / ${totalPages}`);
  const volumePrev = el("volume-prev-btn");
  const volumeNext = el("volume-next-btn");
  if (volumePrev) volumePrev.disabled = state.volumeRankPage <= 1;
  if (volumeNext) volumeNext.disabled = state.volumeRankPage >= totalPages;
}

function renderWallets() {
  const payload = state.wallets || {};
  const rows = Array.isArray(payload.rows) ? payload.rows : [];

  setText("wallet-total-label", `${payload.total || 0} wallets`);
  setText("wallet-page-label", `Page ${payload.page || 1} / ${payload.pages || 1}`);

  const body = el("wallet-table-body");
  if (body) {
    body.innerHTML = rows
      .map((row) => {
        const lifecycleStage = row?.lifecycle?.lifecycleStage || "unknown";
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
      })
      .join("");
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
    const lifecycleStage = profile?.lifecycle?.lifecycleStage || "unknown";
    kpiWrap.innerHTML = [
      ["Rank", `#${s.rank || "-"}`],
      ["Trades", fmt(s.trades, 0)],
      ["Volume", `$${fmtCompact(s.volumeUsd)}`],
      ["PnL", fmtSigned(s.pnlUsd, 2)],
      ["Win Rate", `${fmt(s.winRate, 2)}%`],
      ["First Trade", fmtTime(s.firstTrade)],
      ["Last Trade", fmtTime(s.lastTrade)],
      ["Fees", `$${fmtCompact(s.feesUsd)}`],
      ["Stage", String(lifecycleStage).replace(/_/g, " ")],
    ]
      .map(
        ([label, value]) => `<div class="profile-kpi"><div>${label}</div><strong>${value}</strong></div>`
      )
      .join("");
  }

  if (symbolsBody) {
    const rows = Array.isArray(profile.symbolBreakdown) ? profile.symbolBreakdown : [];
    symbolsBody.innerHTML = rows
      .map(
        (row) => `<tr>
          <td>${row.symbol}</td>
          <td>$${row.volumeCompact}</td>
        </tr>`
      )
      .join("");
  }
}

function applyView(nextView) {
  state.view = nextView;

  document.querySelectorAll(".tab-btn").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.view === nextView);
  });

  el("exchange-view")?.classList.toggle("active", nextView === "exchange");
  el("wallets-view")?.classList.toggle("active", nextView === "wallets");
}

async function refreshExchange() {
  const data = await fetchJson(`/api/exchange/overview?timeframe=${encodeURIComponent(state.timeframe)}`);
  if (!Array.isArray(data?.source?.prices) || !data.source.prices.length) {
    try {
      const pricesPayload = await fetchJson("/api/prices");
      if (Array.isArray(pricesPayload?.prices) && pricesPayload.prices.length) {
        data.source = {
          ...(data.source || {}),
          prices: pricesPayload.prices,
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
}

async function refreshVolumeSeries() {
  const data = await fetchJson("/api/volume-series");
  if (data && data.volume && data.fees) {
    state.volumeSeries = data;
  } else if (data && data.daily) {
    state.volumeSeries = {
      volume: data,
      fees: { daily: [], weekly: [], monthly: [], yearly: [] },
    };
  } else {
    state.volumeSeries = data;
  }
  renderActiveSeries();
}

async function refreshWallets() {
  const params = new URLSearchParams({
    timeframe: state.timeframe,
    q: state.walletSearch,
    page: String(state.walletPage),
    pageSize: String(state.walletPageSize),
  });
  const data = await fetchJson(`/api/wallets?${params.toString()}`);
  state.wallets = data;
  renderWallets();
}

async function inspectWallet(wallet) {
  if (!wallet) return;
  state.selectedWallet = wallet;
  const params = new URLSearchParams({
    wallet,
    timeframe: state.timeframe,
  });
  state.walletProfile = await fetchJson(`/api/wallets/profile?${params.toString()}`);
  renderWalletProfile();
}

async function refreshAll() {
  await Promise.allSettled([refreshExchange(), refreshWallets(), refreshVolumeSeries()]);
  if (state.selectedWallet) {
    await inspectWallet(state.selectedWallet).catch(() => null);
  }
}

async function setTrackedWallet() {
  const input = el("account-input");
  const wallet = input ? input.value.trim() : "";
  await postJson("/api/config/account", { account: wallet });
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
  document.querySelectorAll(".tab-btn").forEach((btn) => {
    btn.addEventListener("click", () => applyView(btn.dataset.view));
  });

  document.querySelectorAll(".chip-btn").forEach((btn) => {
    btn.addEventListener("click", async () => {
      state.timeframe = btn.dataset.timeframe || "all";
      state.volumeRankPage = 1;
      document.querySelectorAll(".chip-btn").forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      state.walletPage = 1;
      await refreshAll();
    });
  });

  el("apply-account-btn")?.addEventListener("click", async () => {
    await setTrackedWallet();
  });

  el("refresh-all-btn")?.addEventListener("click", async () => {
    await postJson("/api/snapshot/refresh", {});
    await postJson("/api/indexer/discover", {}).catch(() => null);
    await postJson("/api/indexer/scan", {}).catch(() => null);
    await refreshAll();
  });

  el("wallet-refresh-btn")?.addEventListener("click", async () => {
    await refreshWallets();
    if (state.selectedWallet) await inspectWallet(state.selectedWallet);
  });

  el("wallet-reindex-btn")?.addEventListener("click", async () => {
    const confirmed = window.confirm(
      "Reindex from zero for all discovered wallets?\n\nThis keeps the discovered wallet registry, but clears per-wallet indexing progress/history."
    );
    if (!confirmed) return;

    try {
      await postJson("/api/indexer/reset", {
        preserveKnownWallets: true,
        resetWalletStore: true,
        clearHistoryFiles: true,
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

  el("wallet-search")?.addEventListener("input", async (event) => {
    state.walletSearch = event.target.value || "";
    state.walletPage = 1;
    await refreshWallets();
  });

  el("wallet-page-size")?.addEventListener("change", async (event) => {
    state.walletPageSize = Number(event.target.value || 20);
    state.walletPage = 1;
    await refreshWallets();
  });

  el("wallet-prev-btn")?.addEventListener("click", async () => {
    state.walletPage = Math.max(1, state.walletPage - 1);
    await refreshWallets();
  });

  el("wallet-next-btn")?.addEventListener("click", async () => {
    const pages = state.wallets && state.wallets.pages ? state.wallets.pages : 1;
    state.walletPage = Math.min(pages, state.walletPage + 1);
    await refreshWallets();
  });

  el("wallet-table-body")?.addEventListener("click", async (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    const btn = target.closest(".inspect-btn");
    if (!btn) return;
    const wallet = btn.getAttribute("data-wallet");
    await inspectWallet(wallet);
  });

  el("volume-prev-btn")?.addEventListener("click", () => {
    state.volumeRankPage = Math.max(1, state.volumeRankPage - 1);
    renderExchange();
  });

  el("volume-next-btn")?.addEventListener("click", () => {
    const totalRows = Array.isArray(state.exchange?.volumeRank) ? state.exchange.volumeRank.length : 0;
    const totalPages = Math.max(1, Math.ceil(totalRows / Math.max(1, state.volumeRankPageSize)));
    state.volumeRankPage = Math.min(totalPages, state.volumeRankPage + 1);
    renderExchange();
  });

  el("seriesDaily")?.addEventListener("click", () => updateSeriesToggle("daily"));
  el("seriesWeekly")?.addEventListener("click", () => updateSeriesToggle("weekly"));
  el("seriesMonthly")?.addEventListener("click", () => updateSeriesToggle("monthly"));
  el("chartCumulative")?.addEventListener("click", () => updateChartType("line"));

  const volumeChart = el("volumeChart");
  if (volumeChart) {
    volumeChart.addEventListener("wheel", handleChartWheel, { passive: false });
    volumeChart.addEventListener("pointerdown", handleChartPointerDown);
    window.addEventListener("pointermove", handleChartPointerMove);
    window.addEventListener("pointerup", handleChartPointerUp);
    window.addEventListener("pointercancel", handleChartPointerUp);
  }

  const chartNavigator = el("chartNavigator");
  if (chartNavigator && ENABLE_CHART_BRUSH) {
    chartNavigator.addEventListener("wheel", handleChartWheel, { passive: false });
    chartNavigator.addEventListener("pointerdown", handleNavigatorPointerDown);
    window.addEventListener("pointermove", handleNavigatorPointerMove);
    window.addEventListener("pointerup", handleNavigatorPointerUp);
    window.addEventListener("pointercancel", handleNavigatorPointerUp);
  } else if (chartNavigator) {
    chartNavigator.hidden = true;
  }

  window.addEventListener(
    "resize",
    () => {
      if (tickerResizeRaf) window.cancelAnimationFrame(tickerResizeRaf);
      tickerResizeRaf = window.requestAnimationFrame(() => {
        tickerResizeRaf = 0;
        if (state.exchange) renderMarketsTicker(state.exchange);
      });
      handleChartResize();
    },
    { passive: true }
  );

  window.addEventListener(
    "orientationchange",
    () => {
      handleChartResize();
      setTimeout(handleChartResize, 160);
    },
    { passive: true }
  );
  window.addEventListener("pageshow", handleChartResize, { passive: true });
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
      handleChartResize();
    }
  });
  if (window.visualViewport) {
    window.visualViewport.addEventListener("resize", queueChartViewportResize, { passive: true });
    window.visualViewport.addEventListener("scroll", queueChartViewportResize, { passive: true });
  }
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

  window.addEventListener(
    "pointermove",
    (event) => {
      const w = window.innerWidth || 1;
      const h = window.innerHeight || 1;
      targetX = ((event.clientX / w) * 2 - 1) * 0.85;
      targetY = ((event.clientY / h) * 2 - 1) * 0.85;
      schedule();
    },
    { passive: true }
  );

  window.addEventListener(
    "pointerleave",
    () => {
      targetX = 0;
      targetY = 0;
      schedule();
    },
    { passive: true }
  );
}

async function init() {
  bindEvents();
  initBackgroundMotion();
  await loadInitialWallet();
  await postJson("/api/indexer/discover", {}).catch(() => null);
  await refreshAll();
  setInterval(() => {
    refreshExchange().catch(() => null);
  }, 8000);
  setInterval(() => {
    refreshWallets().catch(() => null);
  }, 12000);
  setInterval(() => {
    refreshVolumeSeries().catch(() => null);
  }, 30000);
  setInterval(() => {
    if (!state.selectedWallet) return;
    inspectWallet(state.selectedWallet).catch(() => null);
  }, 15000);
}

init().catch((error) => {
  console.error(error);
  setText("status-sync", `Error: ${error.message}`);
});
