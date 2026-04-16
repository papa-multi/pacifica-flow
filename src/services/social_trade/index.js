const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const sharp = require("sharp");
const zlib = require("zlib");

const { ensureDir, normalizeAddress, readJson } = require("../pipeline/utils");

const DAY_MS = 24 * 60 * 60 * 1000;
const DEFAULT_TIMEFRAME = "all";
const SHARE_BASE_URL = "https://pacificaflow.xyz/social-cart";

const ELIGIBLE_MIN_TRADES = 5;
const ELIGIBLE_MIN_VOLUME_USD = 10_000;
const CARD_RENDER_VERSION = 9;
const BASE_CARD_WIDTH = 3000;
const BASE_CARD_HEIGHT = 1500;
const OUTPUT_CARD_WIDTH = 2400;
const OUTPUT_CARD_HEIGHT = 1200;

const ARCHETYPE_PRIORITY = [
  "druckenmiller",
  "buffett",
  "simons",
  "dalio",
  "livermore",
  "soros",
  "ackman",
  "gill",
];

const ARCHETYPES = [
  {
    key: "livermore",
    title: "The Livermore",
    imageFile: "The Livermore (3).png",
    score(percentiles) {
      return (
        0.45 * percentiles.pnl7dPct +
        0.25 * percentiles.volumePct +
        0.15 * percentiles.tradesPct +
        0.15 * percentiles.activityPct
      );
    },
  },
  {
    key: "buffett",
    title: "The Buffett",
    imageFile: "The Buffett (2).png",
    score(percentiles) {
      return (
        0.45 * percentiles.winRatePct +
        0.25 * (100 - percentiles.tradesPct) +
        0.2 * percentiles.pnl30dPct +
        0.1 * percentiles.consistencyPct
      );
    },
  },
  {
    key: "soros",
    title: "The Soros",
    imageFile: "The Soros.png",
    score(percentiles) {
      return (
        0.45 * percentiles.pnl30dPct +
        0.25 * percentiles.avgTradeSizePct +
        0.2 * percentiles.volumePct +
        0.1 * percentiles.concentrationPct
      );
    },
  },
  {
    key: "druckenmiller",
    title: "The Druckenmiller",
    imageFile: "The Druckenmiller.png",
    score(percentiles) {
      return (
        0.3 * percentiles.pnl30dPct +
        0.25 * percentiles.winRatePct +
        0.2 * percentiles.volumePct +
        0.15 * percentiles.tradesPct +
        0.1 * percentiles.pnl7dPct
      );
    },
  },
  {
    key: "simons",
    title: "The Simons",
    imageFile: "The Simons.png",
    score(percentiles) {
      return (
        0.4 * percentiles.tradesPct +
        0.3 * percentiles.winRatePct +
        0.2 * percentiles.activityPct +
        0.1 * percentiles.pnl30dPct
      );
    },
  },
  {
    key: "dalio",
    title: "The Dalio",
    imageFile: "The Dalio.png",
    score(percentiles) {
      return (
        0.35 * percentiles.consistencyPct +
        0.3 * percentiles.winRatePct +
        0.2 * percentiles.diversificationPct +
        0.15 * percentiles.pnl30dPct
      );
    },
  },
  {
    key: "ackman",
    title: "The Ackman",
    imageFile: "The Ackman.png",
    score(percentiles) {
      return (
        0.4 * percentiles.volumePct +
        0.3 * percentiles.avgTradeSizePct +
        0.2 * percentiles.pnl30dPct +
        0.1 * (100 - percentiles.tradesPct)
      );
    },
  },
  {
    key: "gill",
    title: "The Gill",
    imageFile: "The Gill.png",
    score(percentiles) {
      return (
        0.45 * percentiles.pnl7dPct +
        0.2 * percentiles.tradesPct +
        0.2 * percentiles.volumePct +
        0.15 * percentiles.concentrationPct
      );
    },
  },
];

const SHARE_TEXT_BY_ARCHETYPE = {
  livermore:
    "You’re The Livermore.\n\nYou move on timing, conviction, and momentum.\nWhen the setup is right, you don’t hesitate.\n\nTrade your edge on Pacifica DEX:\nhttps://app.pacifica.fi/",
  buffett:
    "You’re The Buffett.\n\nYou stay patient, choose carefully, and ignore the noise.\nYou wait for the few trades that really matter.\n\nTrade with discipline on Pacifica DEX:\nhttps://app.pacifica.fi/",
  soros:
    "You’re The Soros.\n\nYou see the big narrative before the crowd does.\nAnd when the opportunity turns asymmetric, you press hard.\n\nTrade the move on Pacifica DEX:\nhttps://app.pacifica.fi/",
  druckenmiller:
    "You’re The Druckenmiller.\n\nYou adapt fast, stay flexible, and act with conviction.\nWhen trend, timing, and macro line up, you go for it.\n\nTrade with conviction on Pacifica DEX:\nhttps://app.pacifica.fi/",
  simons:
    "You’re The Simons.\n\nYou trust data over emotion.\nYou spot patterns early and let logic guide every move.\n\nTrade with logic on Pacifica DEX:\nhttps://app.pacifica.fi/",
  dalio:
    "You’re The Dalio.\n\nYou manage risk first and never trade on impulse.\nBalance, structure, and consistency define your style.\n\nTrade with balance on Pacifica DEX:\nhttps://app.pacifica.fi/",
  ackman:
    "You’re The Ackman.\n\nYou back big ideas and size up when conviction is high.\nYou’re not afraid to stand apart from the crowd.\n\nTrade bold on Pacifica DEX:\nhttps://app.pacifica.fi/",
  gill:
    "You’re The Gill.\n\nYou thrive on conviction, chaos, and crowd energy.\nYou spot explosive upside before everyone else catches on.\n\nTrade the narrative on Pacifica DEX:\nhttps://app.pacifica.fi/",
};

function toNum(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function clamp(value, min = 0, max = 100) {
  const num = Number(value);
  if (!Number.isFinite(num)) return min;
  return Math.min(max, Math.max(min, num));
}

function round2(value) {
  return Number(toNum(value, 0).toFixed(2));
}

function normalizeTimeframe(value) {
  const raw = String(value || DEFAULT_TIMEFRAME).trim().toLowerCase();
  if (raw === "7d" || raw === "7") return "7d";
  if (raw === "30d" || raw === "30") return "30d";
  if (raw === "all") return "all";
  return DEFAULT_TIMEFRAME;
}

function bucketKeyForTimeframe(timeframe = DEFAULT_TIMEFRAME) {
  const normalized = normalizeTimeframe(timeframe);
  if (normalized === "7d") return "d7";
  if (normalized === "30d") return "d30";
  return "all";
}

function shortWallet(wallet = "") {
  const text = String(wallet || "").trim();
  if (text.length <= 12) return text;
  return `${text.slice(0, 6)}...${text.slice(-4)}`;
}

function formatMoneyCompact(value) {
  const num = toNum(value, 0);
  const abs = Math.abs(num);
  const sign = num < 0 ? "-" : "";
  if (abs >= 1e12) return `${sign}$${(abs / 1e12).toFixed(2)}T`;
  if (abs >= 1e9) return `${sign}$${(abs / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${sign}$${(abs / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `${sign}$${(abs / 1e3).toFixed(2)}K`;
  return `${sign}$${abs.toFixed(2)}`;
}

function formatCountCompact(value) {
  const num = Math.round(toNum(value, 0));
  const abs = Math.abs(num);
  const sign = num < 0 ? "-" : "";
  if (abs >= 1e12) return `${sign}${(abs / 1e12).toFixed(2)}T`;
  if (abs >= 1e9) return `${sign}${(abs / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${sign}${(abs / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `${sign}${(abs / 1e3).toFixed(2)}K`;
  return `${num}`;
}

function formatPercent(value) {
  const num = toNum(value, 0);
  return `${num.toFixed(1)}%`;
}

function formatDateLabel(value) {
  const ts = Number(value || 0);
  if (!Number.isFinite(ts) || ts <= 0) return "-";
  return new Date(ts).toISOString().slice(0, 10);
}

function pickBucket(row = {}, timeframe = DEFAULT_TIMEFRAME) {
  const safeRow = row && typeof row === "object" ? row : {};
  const key = bucketKeyForTimeframe(timeframe);
  const bucket = safeRow[key];
  return bucket && typeof bucket === "object" ? bucket : {};
}

function bucketMetric(row = {}, timeframe = DEFAULT_TIMEFRAME) {
  const bucket = pickBucket(row, timeframe);
  const volumeUsd = toNum(bucket.volumeUsd, timeframe === "all" ? row.volumeUsd : 0);
  const trades = Math.round(toNum(bucket.trades, timeframe === "all" ? row.trades : 0));
  const pnlUsd = round2(toNum(bucket.pnlUsd, timeframe === "all" ? row.pnlUsd : 0));
  const winRatePct = round2(
    toNum(
      bucket.winRatePct !== undefined ? bucket.winRatePct : timeframe === "all" ? row.winRate : 0,
      0
    )
  );
  const firstTrade = Number(bucket.firstTrade || (timeframe === "all" ? row.firstTrade : 0)) || null;
  const lastTrade = Number(bucket.lastTrade || (timeframe === "all" ? row.lastTrade : 0)) || null;
  const symbolVolumes =
    bucket.symbolVolumes && typeof bucket.symbolVolumes === "object" ? bucket.symbolVolumes : {};
  return {
    trades,
    volumeUsd: round2(volumeUsd),
    pnlUsd,
    winRatePct,
    firstTrade,
    lastTrade,
    symbolVolumes,
  };
}

function computeTopSharePct(symbolVolumes = {}, totalVolumeUsd = 0) {
  const total = Math.max(0, toNum(totalVolumeUsd, 0));
  if (total <= 0) return 0;
  const top = Math.max(
    0,
    ...Object.values(symbolVolumes || {}).map((value) => toNum(value, 0))
  );
  return round2((top / total) * 100);
}

function estimateActiveDays(bucket, timeframe = DEFAULT_TIMEFRAME) {
  const trades = Math.max(0, Math.round(toNum(bucket && bucket.trades, 0)));
  if (trades <= 0) return 0;
  const maxDays = timeframe === "7d" ? 7 : timeframe === "30d" ? 30 : 365;
  const firstTrade = Number(bucket && bucket.firstTrade ? bucket.firstTrade : 0) || 0;
  const lastTrade = Number(bucket && bucket.lastTrade ? bucket.lastTrade : 0) || 0;
  const spanDays =
    firstTrade > 0 && lastTrade >= firstTrade
      ? clamp(Math.floor((lastTrade - firstTrade) / DAY_MS) + 1, 1, maxDays)
      : maxDays;
  const densityDays = Math.max(1, Math.round(Math.sqrt(trades) * (timeframe === "7d" ? 1.2 : 1.7)));
  return clamp(Math.min(spanDays, densityDays), 1, maxDays);
}

function upperBound(sorted = [], value) {
  let low = 0;
  let high = sorted.length;
  while (low < high) {
    const mid = (low + high) >> 1;
    if (sorted[mid] <= value) low = mid + 1;
    else high = mid;
  }
  return low;
}

function percentileFromSorted(sorted = [], value) {
  const safe = Array.isArray(sorted) ? sorted : [];
  if (!safe.length) return 0;
  if (safe.length === 1) return 100;
  const idx = upperBound(safe, toNum(value, 0));
  return round2(((Math.max(0, idx - 1)) / (safe.length - 1)) * 100);
}

function isEligibleMetric(metric = {}) {
  return Number(metric.trades || 0) >= ELIGIBLE_MIN_TRADES || Number(metric.volumeUsd || 0) >= ELIGIBLE_MIN_VOLUME_USD;
}

function computeWindowHistoryMetrics(historyPath, days) {
  if (!historyPath || !fs.existsSync(historyPath)) return null;
  const history = readJson(historyPath, null);
  const trades = Array.isArray(history && history.trades) ? history.trades : null;
  if (!trades || !trades.length) return null;
  const cutoff = Date.now() - Math.max(1, Number(days || 30)) * DAY_MS;
  const ascending =
    trades.length < 2 ||
    Number(trades[0] && trades[0].timestamp || 0) <=
      Number(trades[trades.length - 1] && trades[trades.length - 1].timestamp || 0);
  const dailyPnl = new Map();
  const symbolVolumes = new Map();
  let activeTrades = 0;
  let lastActiveAt = 0;
  let firstTradeAt = 0;
  for (let index = trades.length - 1; index >= 0; index -= 1) {
    const trade = trades[index] || {};
    const timestamp = Number(trade.timestamp || 0);
    if (!Number.isFinite(timestamp) || timestamp <= 0) continue;
    if (timestamp < cutoff) {
      if (ascending) break;
      continue;
    }
    activeTrades += 1;
    if (!lastActiveAt || timestamp > lastActiveAt) lastActiveAt = timestamp;
    if (!firstTradeAt || timestamp < firstTradeAt) firstTradeAt = timestamp;
    const pnl = toNum(trade.pnl, 0);
    const amount = Math.abs(toNum(trade.amount, 0));
    const price = Math.abs(toNum(trade.price, 0));
    const notional = amount * price;
    const symbol = String(trade.symbol || "").trim().toUpperCase();
    const day = new Date(timestamp).toISOString().slice(0, 10);
    dailyPnl.set(day, toNum(dailyPnl.get(day), 0) + pnl);
    if (symbol) {
      symbolVolumes.set(symbol, toNum(symbolVolumes.get(symbol), 0) + notional);
    }
  }
  const activeDays = dailyPnl.size;
  const positiveDays = Array.from(dailyPnl.values()).filter((value) => toNum(value, 0) > 0).length;
  const totalVolume = Array.from(symbolVolumes.values()).reduce((sum, value) => sum + toNum(value, 0), 0);
  const topSharePct =
    totalVolume > 0
      ? round2(
          (Math.max(0, ...Array.from(symbolVolumes.values()).map((value) => toNum(value, 0))) /
            totalVolume) *
            100
        )
      : 0;
  return {
    activeTrades,
    activeDays,
    positiveDays,
    uniqueSymbols: symbolVolumes.size,
    topSharePct,
    lastActiveAt: lastActiveAt || null,
    firstTradeAt: firstTradeAt || null,
  };
}

function pickTieBreakWinner(scores = {}) {
  const entries = Object.entries(scores).sort((left, right) => toNum(right[1], 0) - toNum(left[1], 0));
  if (!entries.length) return null;
  const top = entries[0];
  const runnerUp = entries[1];
  if (!runnerUp || Math.abs(toNum(top[1], 0) - toNum(runnerUp[1], 0)) > 3) {
    return top[0];
  }
  const topBand = entries.filter((entry) => Math.abs(toNum(top[1], 0) - toNum(entry[1], 0)) <= 3);
  for (const key of ARCHETYPE_PRIORITY) {
    if (topBand.some((entry) => entry[0] === key)) {
      return key;
    }
  }
  return top[0];
}

function xmlEscape(value) {
  return String(value === null || value === undefined ? "" : value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function buildCardOverlaySvg(
  profile,
  { width = OUTPUT_CARD_WIDTH, height = OUTPUT_CARD_HEIGHT } = {}
) {
  const metrics = profile && profile.metrics ? profile.metrics : {};
  const scaleX = width / BASE_CARD_WIDTH;
  const scaleY = height / BASE_CARD_HEIGHT;
  const sx = (value) => round2(Number(value || 0) * scaleX);
  const sy = (value) => round2(Number(value || 0) * scaleY);
  const metricRows = [
    {
      x: sx(1098),
      y: sy(290),
      maxWidth: sx(610),
      fontSize: sy(82),
      minFontSize: sy(50),
      value: formatMoneyCompact(metrics.totalVolume),
    },
    {
      x: sx(2058),
      y: sy(290),
      maxWidth: sx(500),
      fontSize: sy(82),
      minFontSize: sy(50),
      value: formatCountCompact(metrics.totalTrades),
    },
    {
      x: sx(1098),
      y: sy(524),
      maxWidth: sx(610),
      fontSize: sy(84),
      minFontSize: sy(52),
      value: formatMoneyCompact(metrics.walletPnl),
    },
    {
      x: sx(2058),
      y: sy(524),
      maxWidth: sx(500),
      fontSize: sy(84),
      minFontSize: sy(52),
      value: formatMoneyCompact(metrics.realizedPnl),
    },
    {
      x: sx(1098),
      y: sy(758),
      maxWidth: sx(520),
      fontSize: sy(66),
      minFontSize: sy(42),
      value: formatDateLabel(metrics.firstTradeAt),
    },
    {
      x: sx(2058),
      y: sy(758),
      maxWidth: sx(430),
      fontSize: sy(74),
      minFontSize: sy(46),
      value: formatPercent(metrics.winRate),
    },
    {
      x: sx(1098),
      y: sy(993),
      maxWidth: sx(440),
      fontSize: sy(74),
      minFontSize: sy(46),
      value: metrics.volumeRank ? `#${formatCountCompact(metrics.volumeRank)}` : "-",
    },
    {
      x: sx(2058),
      y: sy(993),
      maxWidth: sx(440),
      fontSize: sy(74),
      minFontSize: sy(46),
      value: metrics.pnlRank ? `#${formatCountCompact(metrics.pnlRank)}` : "-",
    },
  ];
  const walletLine = `${shortWallet(profile.wallet)} • ${String(profile.timeframe || DEFAULT_TIMEFRAME).toUpperCase()}`;
  const archetypeLine = String(profile.archetypeTitle || "").toUpperCase();
  const scoreValue = round2(profile.archetypeScore).toFixed(1);
  const buildMetricText = (item) => {
    const value = String(item && item.value !== undefined && item.value !== null ? item.value : "-");
    const text = xmlEscape(value);
    const length = value.length;
    let fontSize = Number(item.fontSize || sy(72));
    if (length >= 16) fontSize = Math.max(Number(item.minFontSize || sy(42)), fontSize - sy(22));
    else if (length >= 13) fontSize = Math.max(Number(item.minFontSize || sy(42)), fontSize - sy(16));
    else if (length >= 11) fontSize = Math.max(Number(item.minFontSize || sy(42)), fontSize - sy(10));
    else if (length >= 9) fontSize = Math.max(Number(item.minFontSize || sy(42)), fontSize - sy(5));
    const fit = length >= 10 && Number(item.maxWidth || 0) > 0;
    return `<text x="${item.x}" y="${item.y}" text-anchor="start" dominant-baseline="alphabetic" class="metric" font-size="${fontSize}"${
      fit ? ` textLength="${item.maxWidth}" lengthAdjust="spacingAndGlyphs"` : ""
    }>${text}</text>`;
  };
  return `<?xml version="1.0" encoding="UTF-8"?>
<svg width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" xmlns="http://www.w3.org/2000/svg">
  <style>
    .metric {
      fill: #e6dac9;
      font-family: "Aptos", "Segoe UI Semibold", "Segoe UI", Arial, sans-serif;
      font-weight: 760;
      letter-spacing: -0.35px;
      paint-order: stroke;
      stroke: rgba(8, 16, 23, 0.12);
      stroke-width: ${sy(2.6)};
    }
    .footer-shell {
      fill: rgba(11, 19, 26, 0.18);
      stroke: rgba(235, 239, 241, 0.07);
      stroke-width: 0.9;
    }
    .footer-divider {
      stroke: rgba(232, 238, 241, 0.08);
      stroke-width: 0.9;
    }
    .footer-kicker {
      fill: rgba(190, 209, 220, 0.72);
      font-family: "Segoe UI", Arial, sans-serif;
      font-size: ${sy(11)}px;
      font-weight: 600;
      letter-spacing: 1.4px;
    }
    .footer-title {
      fill: rgba(246, 244, 239, 0.98);
      font-family: "Segoe UI Semibold", "Segoe UI", Arial, sans-serif;
      font-size: ${sy(22)}px;
      font-weight: 700;
      letter-spacing: 0.2px;
      paint-order: stroke;
      stroke: rgba(8, 16, 23, 0.14);
      stroke-width: ${sy(1.4)};
    }
    .footer-wallet {
      fill: rgba(214, 225, 231, 0.88);
      font-family: "Segoe UI", Arial, sans-serif;
      font-size: ${sy(14)}px;
      font-weight: 580;
      letter-spacing: 0.14px;
      paint-order: stroke;
      stroke: rgba(8, 16, 23, 0.12);
      stroke-width: ${sy(1.2)};
    }
    .footer-note {
      fill: rgba(177, 197, 206, 0.72);
      font-family: "Segoe UI", Arial, sans-serif;
      font-size: ${sy(10)}px;
      font-weight: 500;
      letter-spacing: 0.7px;
    }
    .footer-score-label {
      fill: rgba(193, 212, 221, 0.7);
      font-family: "Segoe UI", Arial, sans-serif;
      font-size: ${sy(10)}px;
      font-weight: 600;
      letter-spacing: 1.2px;
    }
    .footer-score-value {
      fill: rgba(248, 244, 237, 0.99);
      font-family: "Segoe UI Semibold", "Segoe UI", Arial, sans-serif;
      font-size: ${sy(40)}px;
      font-weight: 700;
      letter-spacing: -0.8px;
      paint-order: stroke;
      stroke: rgba(8, 16, 23, 0.12);
      stroke-width: ${sy(1.2)};
    }
  </style>
  <g>
    ${metricRows
      .map((item) => buildMetricText(item))
      .join("\n    ")}
    <rect x="${sx(94)}" y="${sy(1352)}" width="${sx(520)}" height="${sy(74)}" rx="${sy(18)}" class="footer-shell" />
    <text x="${sx(122)}" y="${sy(1380)}" class="footer-kicker">PACIFICAFLOW</text>
    <text x="${sx(122)}" y="${sy(1408)}" class="footer-title">${xmlEscape(archetypeLine)}</text>

    <rect x="${sx(2388)}" y="${sy(1348)}" width="${sx(500)}" height="${sy(78)}" rx="${sy(18)}" class="footer-shell" />
    <line x1="${sx(2686)}" y1="${sy(1362)}" x2="${sx(2686)}" y2="${sy(1412)}" class="footer-divider" />
    <text x="${sx(2414)}" y="${sy(1380)}" class="footer-wallet">${xmlEscape(walletLine)}</text>
    <text x="${sx(2414)}" y="${sy(1407)}" class="footer-note">identity</text>
    <text x="${sx(2858)}" y="${sy(1378)}" text-anchor="end" class="footer-score-label">SCORE</text>
    <text x="${sx(2858)}" y="${sy(1410)}" text-anchor="end" class="footer-score-value">${xmlEscape(scoreValue)}</text>
  </g>
</svg>`;
}

function buildProfileShareLink(wallet, timeframe) {
  const params = new URLSearchParams({
    wallet: String(wallet || "").trim(),
    timeframe: normalizeTimeframe(timeframe),
  });
  return `${SHARE_BASE_URL}?${params.toString()}`;
}

function readJsonMaybeCompressed(filePath, fallback = null) {
  try {
    if (!filePath || !fs.existsSync(filePath)) return fallback;
    const buffer = fs.readFileSync(filePath);
    const text = String(filePath).endsWith(".gz")
      ? zlib.gunzipSync(buffer).toString("utf8")
      : buffer.toString("utf8");
    return JSON.parse(text);
  } catch (_error) {
    return fallback;
  }
}

function loadRowsFromShardedManifest(rawManifest = null, manifestPath = "") {
  const manifest = rawManifest && typeof rawManifest === "object" ? rawManifest : null;
  const shards = Array.isArray(manifest && manifest.shards) ? manifest.shards : [];
  if (!shards.length) return [];
  const baseDir = path.dirname(String(manifestPath || ""));
  const rows = [];
  for (const shard of shards) {
    const shardFile = String(
      (shard && (shard.file || shard.path || shard.filename)) || ""
    ).trim();
    if (!shardFile) continue;
    const shardPath = path.isAbsolute(shardFile)
      ? shardFile
      : path.resolve(baseDir, shardFile);
    const shardPayload = readJsonMaybeCompressed(shardPath, null);
    const shardRows = Array.isArray(shardPayload && shardPayload.rows)
      ? shardPayload.rows
      : [];
    if (shardRows.length) {
      rows.push(...shardRows);
    }
  }
  return rows;
}

function createSocialTradeService({
  datasetPath,
  cardCacheDir,
  imageDir,
}) {
  const datasetCache = {
    mtimeMs: 0,
    value: null,
  };
  const universeCache = new Map();
  const profileInflight = new Map();
  const imageInflight = new Map();

  function loadDataset() {
    const safePath = path.resolve(String(datasetPath || ""));
    const stats = fs.existsSync(safePath) ? fs.statSync(safePath) : null;
    const mtimeMs = stats ? Number(stats.mtimeMs || 0) : 0;
    if (datasetCache.value && datasetCache.mtimeMs === mtimeMs) {
      return datasetCache.value;
    }
    const raw = readJson(safePath, null);
    let rows = Array.isArray(raw && raw.rows) ? raw.rows : [];
    if (!rows.length && raw && typeof raw === "object" && Array.isArray(raw.shards)) {
      rows = loadRowsFromShardedManifest(raw, safePath);
    }
    const rowsByWallet = new Map();
    rows.forEach((row) => {
      const wallet = normalizeAddress(String((row && row.wallet) || "").trim());
      if (wallet) rowsByWallet.set(wallet, row);
    });
    const dataset = {
      path: safePath,
      mtimeMs,
      generatedAt: Number((raw && raw.generatedAt) || Date.now()),
      rows,
      rowsByWallet,
    };
    datasetCache.mtimeMs = mtimeMs;
    datasetCache.value = dataset;
    universeCache.clear();
    return dataset;
  }

  function buildUniverse(timeframe = DEFAULT_TIMEFRAME) {
    const dataset = loadDataset();
    const normalizedTimeframe = normalizeTimeframe(timeframe);
    const cacheKey = `${dataset.mtimeMs}:${normalizedTimeframe}`;
    if (universeCache.has(cacheKey)) {
      return universeCache.get(cacheKey);
    }
    const eligibleRows = [];
    const volumeRankRows = [];
    const pnlRankRows = [];
    for (const row of dataset.rows) {
      const bucket = bucketMetric(row, normalizedTimeframe);
      if (!isEligibleMetric(bucket)) continue;
      const uniqueSymbols = Object.keys(bucket.symbolVolumes || {}).length;
      const topSharePct = computeTopSharePct(bucket.symbolVolumes, bucket.volumeUsd);
      const avgTradeSize = bucket.volumeUsd / Math.max(1, bucket.trades);
      const activityValue = estimateActiveDays(bucket, normalizedTimeframe);
      const consistencyValue =
        clamp(0.6 * bucket.winRatePct + 0.4 * ((activityValue / Math.max(1, normalizedTimeframe === "7d" ? 7 : normalizedTimeframe === "30d" ? 30 : 365)) * 100), 0, 100);
      const normalizedWallet = normalizeAddress(String((row && row.wallet) || "").trim());
      const summary = {
        wallet: normalizedWallet,
        volumeUsd: bucket.volumeUsd,
        trades: bucket.trades,
        winRatePct: bucket.winRatePct,
        pnlUsd: bucket.pnlUsd,
        avgTradeSize,
        uniqueSymbols,
        topSharePct,
        diversificationValue: uniqueSymbols,
        activityValue,
        consistencyValue,
      };
      eligibleRows.push(summary);
      volumeRankRows.push(summary);
      pnlRankRows.push(summary);
    }
    volumeRankRows.sort((left, right) => {
      const diff = toNum(right.volumeUsd, 0) - toNum(left.volumeUsd, 0);
      if (diff !== 0) return diff;
      return String(left.wallet || "").localeCompare(String(right.wallet || ""));
    });
    pnlRankRows.sort((left, right) => {
      const diff = toNum(right.pnlUsd, 0) - toNum(left.pnlUsd, 0);
      if (diff !== 0) return diff;
      return String(left.wallet || "").localeCompare(String(right.wallet || ""));
    });
    const volumeRankByWallet = new Map();
    const pnlRankByWallet = new Map();
    volumeRankRows.forEach((row, index) => {
      if (row.wallet) volumeRankByWallet.set(row.wallet, index + 1);
    });
    pnlRankRows.forEach((row, index) => {
      if (row.wallet) pnlRankByWallet.set(row.wallet, index + 1);
    });
    const numericArrays = {
      volumePct: eligibleRows.map((row) => toNum(row.volumeUsd, 0)).sort((a, b) => a - b),
      tradesPct: eligibleRows.map((row) => toNum(row.trades, 0)).sort((a, b) => a - b),
      winRatePct: eligibleRows.map((row) => toNum(row.winRatePct, 0)).sort((a, b) => a - b),
      pnlPct: eligibleRows.map((row) => toNum(row.pnlUsd, 0)).sort((a, b) => a - b),
      avgTradeSizePct: eligibleRows.map((row) => toNum(row.avgTradeSize, 0)).sort((a, b) => a - b),
      concentrationPct: eligibleRows.map((row) => toNum(row.topSharePct, 0)).sort((a, b) => a - b),
      diversificationPct: eligibleRows.map((row) => toNum(row.diversificationValue, 0)).sort((a, b) => a - b),
      activityPct: eligibleRows.map((row) => toNum(row.activityValue, 0)).sort((a, b) => a - b),
      consistencyPct: eligibleRows.map((row) => toNum(row.consistencyValue, 0)).sort((a, b) => a - b),
    };
    const universe = {
      timeframe: normalizedTimeframe,
      generatedAt: Date.now(),
      eligibleRows,
      volumeRankByWallet,
      pnlRankByWallet,
      numericArrays,
    };
    universeCache.set(cacheKey, universe);
    return universe;
  }

  async function getProfile(wallet, timeframe = DEFAULT_TIMEFRAME) {
    const normalizedWallet = normalizeAddress(String(wallet || "").trim());
    const normalizedTimeframe = normalizeTimeframe(timeframe);
    if (!normalizedWallet) {
      const error = new Error("wallet_required");
      error.statusCode = 400;
      throw error;
    }
    const inflightKey = `${normalizedWallet}:${normalizedTimeframe}`;
    if (profileInflight.has(inflightKey)) {
      return profileInflight.get(inflightKey);
    }
    const inflight = Promise.resolve().then(() => {
      const dataset = loadDataset();
      const row = dataset.rowsByWallet.get(normalizedWallet);
      if (!row) {
        const error = new Error("wallet_not_found");
        error.statusCode = 404;
        throw error;
      }
      const row7 = bucketMetric(row, "7d");
      const row30 = bucketMetric(row, "30d");
      const rowAll = bucketMetric(row, "all");
      const historyPath = row && row.sources && row.sources.historyPath ? String(row.sources.historyPath) : null;
      const history30 = computeWindowHistoryMetrics(historyPath, 30);
      const primary30 = {
        totalVolume: row30.volumeUsd,
        totalTrades: row30.trades,
        winRate: row30.winRatePct,
        realizedPnl: row30.pnlUsd,
        avgTradeSize: round2(row30.volumeUsd / Math.max(1, row30.trades)),
        activeDays: history30 && Number.isFinite(Number(history30.activeDays))
          ? Number(history30.activeDays)
          : estimateActiveDays(row30, "30d"),
        positiveDays: history30 && Number.isFinite(Number(history30.positiveDays))
          ? Number(history30.positiveDays)
          : Math.round(
              estimateActiveDays(row30, "30d") * (clamp(row30.winRatePct, 0, 100) / 100)
            ),
        uniqueSymbols:
          history30 && Number.isFinite(Number(history30.uniqueSymbols))
            ? Number(history30.uniqueSymbols)
            : Object.keys(row30.symbolVolumes || {}).length,
        topSymbolShare:
          history30 && Number.isFinite(Number(history30.topSharePct))
            ? Number(history30.topSharePct)
            : computeTopSharePct(row30.symbolVolumes, row30.volumeUsd),
      };
      const universe30 = buildUniverse("30d");
      const universe7 = buildUniverse("7d");
      const selectedUniverse = buildUniverse(normalizedTimeframe);
      const activityPct = percentileFromSorted(
        universe30.numericArrays.activityPct,
        primary30.activeDays
      );
      const consistencyValueExact =
        primary30.activeDays > 0
          ? clamp((primary30.positiveDays / Math.max(1, primary30.activeDays)) * 100, 0, 100)
          : clamp(0.6 * row30.winRatePct + 0.4 * activityPct, 0, 100);
      const percentiles = {
        volumePct: percentileFromSorted(universe30.numericArrays.volumePct, primary30.totalVolume),
        tradesPct: percentileFromSorted(universe30.numericArrays.tradesPct, primary30.totalTrades),
        winRatePct: percentileFromSorted(universe30.numericArrays.winRatePct, primary30.winRate),
        pnl30dPct: percentileFromSorted(universe30.numericArrays.pnlPct, primary30.realizedPnl),
        pnl7dPct: percentileFromSorted(universe7.numericArrays.pnlPct, row7.pnlUsd),
        avgTradeSizePct: percentileFromSorted(
          universe30.numericArrays.avgTradeSizePct,
          primary30.avgTradeSize
        ),
        activityPct,
        concentrationPct: percentileFromSorted(
          universe30.numericArrays.concentrationPct,
          primary30.topSymbolShare
        ),
        diversificationPct: percentileFromSorted(
          universe30.numericArrays.diversificationPct,
          primary30.uniqueSymbols
        ),
        consistencyPct: percentileFromSorted(
          universe30.numericArrays.consistencyPct,
          consistencyValueExact
        ),
      };
      const scores = {};
      ARCHETYPES.forEach((archetype) => {
        scores[archetype.key] = round2(archetype.score(percentiles));
      });
      const winningKey = pickTieBreakWinner(scores);
      const winningArchetype = ARCHETYPES.find((item) => item.key === winningKey) || ARCHETYPES[0];
      const selectedBucket = bucketMetric(row, normalizedTimeframe);
      const totalPnlCurrent =
        row.totalPnlUsd !== undefined
          ? round2(row.totalPnlUsd)
          : round2(toNum(rowAll.pnlUsd, 0) + toNum(row.unrealizedPnlUsd, 0));
      const metrics = {
        totalVolume: selectedBucket.volumeUsd,
        totalTrades: selectedBucket.trades,
        walletPnl: totalPnlCurrent,
        realizedPnl: selectedBucket.pnlUsd,
        openPositions: Math.max(0, Math.round(toNum(row.openPositions, 0))),
        firstTradeAt: Number(row.firstTrade || rowAll.firstTrade || selectedBucket.firstTrade || 0) || null,
        winRate: selectedBucket.winRatePct,
        volumeRank: selectedUniverse.volumeRankByWallet.get(normalizedWallet) || null,
        pnlRank: selectedUniverse.pnlRankByWallet.get(normalizedWallet) || null,
        activeDays30d: primary30.activeDays,
        avgTradeSize: primary30.avgTradeSize,
        lastActiveAt:
          Number(
            row.lastActivity ||
              (history30 && history30.lastActiveAt) ||
              selectedBucket.lastTrade ||
              row.lastTrade ||
              row.updatedAt ||
              0
          ) || null,
        uniqueSymbols30d: primary30.uniqueSymbols,
        topSymbolShare30d: primary30.topSymbolShare,
        totalVolume30d: primary30.totalVolume,
        totalTrades30d: primary30.totalTrades,
        winRate30d: primary30.winRate,
        realizedPnl30d: primary30.realizedPnl,
        realizedPnl7d: row7.pnlUsd,
        walletPnlCurrent: totalPnlCurrent,
        volumeRank30d: universe30.volumeRankByWallet.get(normalizedWallet) || null,
        pnlRank30d: universe30.pnlRankByWallet.get(normalizedWallet) || null,
      };
      const shareText = SHARE_TEXT_BY_ARCHETYPE[winningArchetype.key] || "";
      const shareLink = buildProfileShareLink(normalizedWallet, normalizedTimeframe);
      const cardVersion = crypto
        .createHash("sha1")
        .update(
          JSON.stringify({
            cardRenderVersion: CARD_RENDER_VERSION,
            datasetGeneratedAt: dataset.generatedAt,
            wallet: normalizedWallet,
            timeframe: normalizedTimeframe,
            archetype: winningArchetype.key,
            metrics,
          })
        )
        .digest("hex")
        .slice(0, 16);
      return {
        generatedAt: Date.now(),
        wallet: normalizedWallet,
        timeframe: normalizedTimeframe,
        archetypeKey: winningArchetype.key,
        archetypeTitle: winningArchetype.title,
        archetypeScore: scores[winningArchetype.key],
        scores,
        allArchetypeScores: scores,
        metrics,
        percentiles,
        shareText,
        shareLink,
        eligible: isEligibleMetric(selectedBucket) || isEligibleMetric(row30) || isEligibleMetric(rowAll),
        cardImageUrl: `/api/social-trade/card-image?wallet=${encodeURIComponent(
          normalizedWallet
        )}&timeframe=${encodeURIComponent(normalizedTimeframe)}&v=${encodeURIComponent(cardVersion)}`,
      };
    }).finally(() => {
      profileInflight.delete(inflightKey);
    });
    profileInflight.set(inflightKey, inflight);
    return inflight;
  }

  async function getCardImage(wallet, timeframe = DEFAULT_TIMEFRAME) {
    const profile = await getProfile(wallet, timeframe);
    ensureDir(cardCacheDir);
    const cacheKey = crypto
      .createHash("sha1")
      .update(
        JSON.stringify({
          cardRenderVersion: CARD_RENDER_VERSION,
          wallet: profile.wallet,
          timeframe: profile.timeframe,
          archetypeKey: profile.archetypeKey,
          archetypeScore: profile.archetypeScore,
          metrics: profile.metrics,
        })
      )
      .digest("hex");
    const cachePath = path.join(cardCacheDir, `${cacheKey}.png`);
    if (fs.existsSync(cachePath)) {
      return {
        buffer: fs.readFileSync(cachePath),
        profile,
        cachePath,
      };
    }
    if (imageInflight.has(cacheKey)) {
      return imageInflight.get(cacheKey);
    }
    const inflight = Promise.resolve()
      .then(async () => {
        const archetype = ARCHETYPES.find((item) => item.key === profile.archetypeKey) || ARCHETYPES[0];
        const basePath = path.join(imageDir, archetype.imageFile);
        if (!fs.existsSync(basePath)) {
          const error = new Error("card_image_base_missing");
          error.statusCode = 500;
          throw error;
        }
        const overlaySvg = buildCardOverlaySvg(profile, {
          width: OUTPUT_CARD_WIDTH,
          height: OUTPUT_CARD_HEIGHT,
        });
        const buffer = await sharp(basePath)
          .resize(OUTPUT_CARD_WIDTH, OUTPUT_CARD_HEIGHT, { fit: "fill", kernel: sharp.kernel.lanczos3 })
          .composite([{ input: Buffer.from(overlaySvg, "utf8"), top: 0, left: 0 }])
          .png({
            compressionLevel: 9,
            adaptiveFiltering: true,
            effort: 10,
          })
          .toBuffer();
        fs.writeFileSync(cachePath, buffer);
        return {
          buffer,
          profile,
          cachePath,
        };
      })
      .finally(() => {
        imageInflight.delete(cacheKey);
      });
    imageInflight.set(cacheKey, inflight);
    return inflight;
  }

  return {
    getProfile,
    getCardImage,
  };
}

module.exports = {
  ARCHETYPES,
  SHARE_TEXT_BY_ARCHETYPE,
  createSocialTradeService,
  normalizeTimeframe,
  shortWallet,
};
