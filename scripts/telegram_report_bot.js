"use strict";

const fs = require("fs");
const path = require("path");

const ROOT = path.resolve(__dirname, "..");
const DEFAULT_API_BASE = process.env.PACIFICA_TELEGRAM_REPORT_API_BASE || "http://127.0.0.1:3200";
const BOT_ENABLED = /^(1|true|yes|on)$/i.test(
  String(process.env.PACIFICA_TELEGRAM_REPORT_BOT_ENABLED || "").trim()
);
const BOT_TOKEN = String(process.env.PACIFICA_TELEGRAM_REPORT_BOT_TOKEN || "").trim();
const POLL_TIMEOUT_SEC = Math.max(
  1,
  Math.min(60, Number(process.env.PACIFICA_TELEGRAM_REPORT_POLL_TIMEOUT_SEC || 50) || 50)
);
const STATE_FILE = path.resolve(
  ROOT,
  String(process.env.PACIFICA_TELEGRAM_REPORT_STATE_FILE || "./data/runtime/telegram_report_bot_state.json")
);
const DRY_RUN = /^(1|true|yes|on)$/i.test(
  String(process.env.PACIFICA_TELEGRAM_REPORT_DRY_RUN || "").trim()
);
const ALLOWED_CHAT_IDS = new Set(
  String(process.env.PACIFICA_TELEGRAM_REPORT_ALLOWED_CHAT_IDS || "")
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean)
);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function ensureStateDir() {
  fs.mkdirSync(path.dirname(STATE_FILE), { recursive: true });
}

function readState() {
  try {
    return JSON.parse(fs.readFileSync(STATE_FILE, "utf8"));
  } catch (_error) {
    return {};
  }
}

function writeState(nextState) {
  ensureStateDir();
  fs.writeFileSync(STATE_FILE, JSON.stringify(nextState, null, 2));
}

function fmtInt(value) {
  const num = Number(value || 0);
  return Number.isFinite(num) ? Math.round(num).toLocaleString("en-GB") : "0";
}

function fmtCompactUsd(value) {
  const num = Number(value || 0);
  if (!Number.isFinite(num)) return "$0";
  const abs = Math.abs(num);
  if (abs >= 1e9) return `$${(num / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `$${(num / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `$${(num / 1e3).toFixed(2)}K`;
  return `$${num.toFixed(2)}`;
}

function fmtPct(value) {
  const num = Number(value || 0);
  if (!Number.isFinite(num)) return "0%";
  return `${num.toFixed(1)}%`;
}

function fmtUtc(timestamp) {
  const value = Number(timestamp || 0);
  if (!Number.isFinite(value) || value <= 0) return "-";
  return new Date(value).toISOString().replace("T", " ").replace(".000Z", " UTC");
}

async function fetchJson(url, timeoutMs = 15000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      method: "GET",
      headers: { Accept: "application/json", "User-Agent": "pacifica-telegram-report-bot/1.0" },
      signal: controller.signal,
    });
    if (!response.ok) {
      throw new Error(`${url} failed ${response.status}`);
    }
    return await response.json();
  } finally {
    clearTimeout(timer);
  }
}

function buildKeyboard() {
  return {
    keyboard: [[{ text: "Start" }, { text: "Report" }]],
    resize_keyboard: true,
    is_persistent: true,
  };
}

function isAllowedChat(chatId) {
  if (!ALLOWED_CHAT_IDS.size) return true;
  return ALLOWED_CHAT_IDS.has(String(chatId));
}

async function telegramApi(method, payload = null, timeoutMs = 60000) {
  if (!BOT_TOKEN) throw new Error("missing_bot_token");
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/${method}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: payload ? JSON.stringify(payload) : "{}",
      signal: controller.signal,
    });
    const body = await response.json().catch(() => ({}));
    if (!response.ok || !body.ok) {
      throw new Error(body.description || `${method} failed ${response.status}`);
    }
    return body.result;
  } finally {
    clearTimeout(timer);
  }
}

async function sendTelegramMessage(chatId, text) {
  return telegramApi("sendMessage", {
    chat_id: chatId,
    text,
    reply_markup: buildKeyboard(),
    disable_web_page_preview: true,
  });
}

async function buildStatusReport() {
  const now = Date.now();
  const londonDate = new Intl.DateTimeFormat("en-GB", {
    dateStyle: "full",
    timeStyle: "medium",
    timeZone: "Europe/London",
  }).format(new Date(now));
  const [syncResult, liveResult, exchangeResult] = await Promise.allSettled([
    fetchJson(`${DEFAULT_API_BASE}/api/wallets/sync-health`, 20000),
    fetchJson(`${DEFAULT_API_BASE}/api/live-trades/wallet-first/metrics`, 20000),
    fetchJson(`${DEFAULT_API_BASE}/api/exchange/overview?timeframe=24h`, 20000),
  ]);
  const lines = [`Pacifica Status Report`, `Report Date: ${londonDate}`, `Report Date (UTC): ${fmtUtc(now)}`];

  if (syncResult.status === "fulfilled") {
    const sync = syncResult.value || {};
    const tradeSummary = sync.tradeVisibility && sync.tradeVisibility.summary ? sync.tradeVisibility.summary : {};
    const reviewPipeline = sync.reviewPipeline || {};
    const trackerPool = sync.trackerPool || {};
    const groups = sync.groups || {};
    const stageCounts = reviewPipeline.stageCounts || {};
    const activeClaims = reviewPipeline.activeClaims || {};
    const recentUpdates = reviewPipeline.recentUpdates || {};
    const trackerProgress = trackerPool.progress || {};
    const bottlenecks = trackerPool.bottlenecks || {};
    lines.push("");
    lines.push("Wallet Explorer");
    lines.push(`Status: ${String(sync.overallStatus || "unknown")}`);
    lines.push(`Visible wallets: ${fmtInt(sync.visibleWallets)}`);
    lines.push(`Zero-verified wallets: ${fmtInt(sync.verifiedZeroTradeWallets)}`);
    lines.push(`Trade-summary wallets: ${fmtInt(tradeSummary.walletsInTradeSummary)}`);
    lines.push(`Latest trade seen: ${fmtUtc(tradeSummary.latestTradeAt)}`);

    lines.push("");
    lines.push("Tracker History Review");
    lines.push(`Pipeline: v${fmtInt(reviewPipeline.version || 0)} • ${String(sync.overallStatus || "unknown")}`);
    lines.push(
      `Stages: live ${fmtInt(stageCounts.live)} • zero_verified ${fmtInt(stageCounts.zero_verified)} • history ${fmtInt(stageCounts.history)} • gap ${fmtInt(stageCounts.gap)}`
    );
    lines.push(`Active claims: live ${fmtInt(activeClaims.live)}`);
    lines.push(
      `Recent updates: 1m ${fmtInt(recentUpdates["1m"])} • 5m ${fmtInt(recentUpdates["5m"])} • 15m ${fmtInt(recentUpdates["15m"])} • 60m ${fmtInt(recentUpdates["60m"])}`
    );
    lines.push(`Baseline through: ${reviewPipeline.baselineThroughDate || "-"}`);
    lines.push(
      `Tracker pool: ${String(trackerPool.mode || "unknown")} • known ${fmtInt(trackerProgress.knownWallets)} • tracked ${fmtInt(trackerProgress.trackedPool)} • zero-history ${fmtInt(trackerProgress.zeroHistory)}`
    );
    lines.push(
      `Bottlenecks: stalled ${fmtInt(bottlenecks.stalledShards)} • proof pending ${fmtInt(bottlenecks.headProofPending)} • mismatches ${fmtInt(bottlenecks.headProofMismatch)} • errors ${fmtInt(bottlenecks.headProofError)}`
    );

    if (groups && typeof groups === "object") {
      const groupSummary = [
        ["history", groups.historyCompletion],
        ["catch-up", groups.catchUp],
        ["zero-history", groups.zeroHistory],
        ["tracked-pool", groups.trackedPool],
        ["untracked", groups.untracked],
      ]
        .filter(([, group]) => group && typeof group === "object")
        .map(([label, group]) => `${label} ${fmtInt(group.count)} (${fmtPct(group.progressPct)})`)
        .join(" • ");
      if (groupSummary) {
        lines.push(`Groups: ${groupSummary}`);
      }
    }
  } else {
    lines.push("");
    lines.push(`Wallet Explorer: unavailable (${syncResult.reason && syncResult.reason.message ? syncResult.reason.message : "request_failed"})`);
  }

  if (liveResult.status === "fulfilled") {
    const live = liveResult.value || {};
    const agg = live.aggregate || {};
    lines.push("");
    lines.push("Live Trade");
    lines.push(
      `Coverage: ${fmtInt(agg.trackedWallets)} tracked / ${fmtInt(agg.subscribedWallets)} subscribed / ${fmtInt(agg.openWebsocketConnections)} open ws`
    );
    lines.push(`Events/min: ${fmtInt(agg.eventsPerMinute)}`);
    lines.push(`New opens/min: ${fmtInt(agg.newOpenEventsPerMinute)}`);
    lines.push(`Delayed opens >5s: ${fmtInt(agg.delayedOpenEventsOver5s1m)}`);
  } else {
    lines.push("");
    lines.push(`Live Trade: unavailable (${liveResult.reason && liveResult.reason.message ? liveResult.reason.message : "request_failed"})`);
  }

  if (exchangeResult.status === "fulfilled") {
    const overview = exchangeResult.value || {};
    const kpis = overview.kpis || {};
    lines.push("");
    lines.push("Exchange 24H");
    lines.push(`Active wallets: ${fmtInt(kpis.activeWallets)}`);
    lines.push(`Trades: ${fmtInt(kpis.totalTrades)}`);
    lines.push(`Volume: ${fmtCompactUsd(kpis.totalVolumeUsd)}`);
    lines.push(`Fees: ${fmtCompactUsd(kpis.totalFeesUsd)}`);
    lines.push(`Open interest: ${fmtCompactUsd(kpis.openInterestAtEnd)}`);
  } else {
    lines.push("");
    lines.push(`Exchange 24H: unavailable (${exchangeResult.reason && exchangeResult.reason.message ? exchangeResult.reason.message : "request_failed"})`);
  }

  return lines.join("\n");
}

async function handleMessage(message) {
  const chatId = message && message.chat ? message.chat.id : null;
  const text = String((message && message.text) || "").trim();
  if (!chatId || !isAllowedChat(chatId)) {
    return;
  }
  const normalized = text.toLowerCase();
  if (normalized === "/start" || normalized === "start") {
    await sendTelegramMessage(
      chatId,
      "Pacifica report bot is ready.\n\nTap Report any time to receive the current status summary."
    );
    return;
  }
  if (normalized === "/report" || normalized === "report") {
    const report = await buildStatusReport();
    await sendTelegramMessage(chatId, report);
    return;
  }
  await sendTelegramMessage(chatId, "Use Start or Report.");
}

async function pollLoop() {
  const state = readState();
  let offset = Number(state.offset || 0) || 0;
  for (;;) {
    try {
      const updates = await telegramApi(
        "getUpdates",
        {
          offset,
          timeout: POLL_TIMEOUT_SEC,
          allowed_updates: ["message"],
        },
        (POLL_TIMEOUT_SEC + 15) * 1000
      );
      if (!Array.isArray(updates) || !updates.length) {
        continue;
      }
      for (const update of updates) {
        offset = Math.max(offset, Number(update.update_id || 0) + 1);
        writeState({ offset, updatedAt: Date.now() });
        if (update && update.message) {
          await handleMessage(update.message);
        }
      }
    } catch (error) {
      console.error(`[telegram-report-bot] ${error && error.message ? error.message : error}`);
      await sleep(3000);
    }
  }
}

async function main() {
  if (DRY_RUN) {
    const report = await buildStatusReport();
    process.stdout.write(`${report}\n`);
    return;
  }
  if (!BOT_ENABLED) {
    console.log("[telegram-report-bot] disabled via PACIFICA_TELEGRAM_REPORT_BOT_ENABLED");
    return;
  }
  if (!BOT_TOKEN) {
    console.log("[telegram-report-bot] token missing; set PACIFICA_TELEGRAM_REPORT_BOT_TOKEN");
    return;
  }
  ensureStateDir();
  console.log("[telegram-report-bot] started");
  await pollLoop();
}

main().catch((error) => {
  console.error(error && error.stack ? error.stack : error);
  process.exitCode = 1;
});
