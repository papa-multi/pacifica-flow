"use strict";

const fs = require("fs");
const path = require("path");
const { loadProjectConfig } = require("../src/services/telegram_copilot/project");
const {
  ensureChatState,
  ensureWatchedWallets,
  ensureProjectProjectState,
  findWatchedWallet,
  readState,
  removeWatchedWallet,
  upsertWatchedWallet,
  writeState,
} = require("../src/services/telegram_copilot/state");
const {
  answerCallback,
  buildInlineMenu,
  sendMessage,
  setMyCommands,
  telegramApi,
} = require("../src/services/telegram_copilot/telegram");

const ROOT = path.resolve(__dirname, "..");
const BOT_ENABLED = /^(1|true|yes|on)$/i.test(String(process.env.PACIFICA_TELEGRAM_COPILOT_ENABLED || "").trim());
const BOT_TOKEN = String(process.env.PACIFICA_TELEGRAM_COPILOT_BOT_TOKEN || "").trim();
const PROJECT_KEY = String(process.env.PACIFICA_TELEGRAM_COPILOT_PROJECT_KEY || "pacifica-flow").trim() || "pacifica-flow";
const CONFIG_DIR = String(process.env.PACIFICA_TELEGRAM_COPILOT_CONFIG_DIR || "./config/telegram-copilot").trim();
const STATE_FILE = path.resolve(ROOT, String(process.env.PACIFICA_TELEGRAM_COPILOT_STATE_FILE || "./data/runtime/telegram_copilot_state.json"));
const API_BASE = String(process.env.PACIFICA_TELEGRAM_COPILOT_API_BASE || "http://127.0.0.1:3200").trim();
const DRY_RUN = /^(1|true|yes|on)$/i.test(String(process.env.PACIFICA_TELEGRAM_COPILOT_DRY_RUN || "").trim());
const POLL_TIMEOUT_SEC = Math.max(1, Math.min(60, Number(process.env.PACIFICA_TELEGRAM_COPILOT_POLL_TIMEOUT_SEC || 50) || 50));
const ALLOWED_CHAT_IDS = new Set(
  String(process.env.PACIFICA_TELEGRAM_COPILOT_ALLOWED_CHAT_IDS || "")
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean)
);
const WATCH_POLL_MS = Math.max(15000, Number(process.env.PACIFICA_TELEGRAM_COPILOT_WATCH_POLL_MS || 30000) || 30000);
const DEFAULT_LIST_PAGE_SIZE = 10;
const DEFAULT_LIST_FETCH_PAGE_SIZE = 20;
const DEFAULT_LIST_FETCH_PAGES = 3;
const DEFAULT_LIST_TARGET_SIZE = 50;
const DEFAULT_LIST_CACHE_TTL_MS = Math.max(
  5 * 60 * 1000,
  Number(process.env.PACIFICA_TELEGRAM_COPILOT_WALLET_LIST_CACHE_TTL_MS || 10 * 60 * 1000) || 10 * 60 * 1000
);
const DEFAULT_WALLET_SOURCE_PATH = path.join(ROOT, "data/ui/.build/wallet_dataset_compact_source.json");
let watchScanInFlight = false;
let watchScanTimer = null;
let shutdownRequested = false;
const walletListFetchInFlight = new Map();
const walletCatalogSourceCache = {
  mtimeMs: 0,
  rows: null,
  error: null,
};

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function ensureStateDir() {
  fs.mkdirSync(path.dirname(STATE_FILE), { recursive: true });
}

function isAllowedChat(chatId) {
  if (!ALLOWED_CHAT_IDS.size) return true;
  return ALLOWED_CHAT_IDS.has(String(chatId));
}

function cleanText(value) {
  return String(value == null ? "" : value).trim();
}

function shortText(value, limit) {
  const text = cleanText(value);
  if (text.length <= limit) return text;
  return `${text.slice(0, Math.max(0, limit - 1)).trim()}…`;
}

function formatWalletId(value) {
  const wallet = cleanText(value);
  if (!wallet) return "-";
  if (wallet.length <= 16) return wallet;
  return `${wallet.slice(0, 8)}…${wallet.slice(-6)}`;
}

function formatTimestamp(value) {
  const ts = Number(value || 0);
  if (!(ts > 0)) return "-";
  return new Date(ts).toISOString().replace("T", " ").replace(".000Z", " UTC");
}

function getBotDisplayName(project) {
  const name = project && typeof project === "object" ? String(project.botName || project.projectName || "").trim() : "";
  return name || "Pacifica profit pulse";
}

function getProjectState(state, projectKey) {
  return ensureProjectProjectState(state, projectKey);
}

function getDefaultWalletCatalog(projectState) {
  const safe = projectState && typeof projectState === "object" ? projectState : {};
  const catalog = safe.walletCatalog && typeof safe.walletCatalog === "object" ? safe.walletCatalog : null;
  if (!catalog) return null;
  return {
    generatedAt: Number(catalog.generatedAt || 0),
    source: String(catalog.source || "api_wallets"),
    lists: catalog.lists && typeof catalog.lists === "object" ? catalog.lists : {},
  };
}

function setDefaultWalletCatalog(projectState, catalog) {
  if (!projectState || typeof projectState !== "object") return null;
  const safeCatalog = catalog && typeof catalog === "object" ? catalog : null;
  if (!safeCatalog) return null;
  projectState.walletCatalog = {
    generatedAt: Number(safeCatalog.generatedAt || Date.now()),
    source: String(safeCatalog.source || "api_wallets"),
    lists:
      safeCatalog.lists && typeof safeCatalog.lists === "object"
        ? safeCatalog.lists
        : { trades: [], pnl: [], combined: [] },
  };
  projectState.updatedAt = Date.now();
  return projectState.walletCatalog;
}

function normalizeWalletCatalogRow(row, rank, listKey) {
  const safe = row && typeof row === "object" ? row : {};
  const wallet = cleanText(safe.wallet);
  if (!wallet) return null;
  return {
    wallet,
    label: cleanText(safe.walletLabel || safe.username || formatWalletId(wallet)),
    rank: Number(rank || safe.rank || 0),
    listKey: String(listKey || ""),
    trades: toNumber(safe.trades, 0),
    pnlUsd: toNumber(safe.pnlUsd, 0),
    volumeUsd: toNumber(safe.volumeUsd, 0),
    openPositions: toNumber(safe.openPositions, 0),
    lastTradeAt: toNumber(safe.lastTrade || safe.lastActivityAt || safe.lastTradeAt || safe.updatedAt, 0),
    updatedAt: toNumber(safe.updatedAt || safe.lastUpdatedAt || safe.lastActivityAt || safe.lastTrade || 0, 0),
    walletRecordId: cleanText(safe.walletRecordId),
    sourceRow: safe,
  };
}

function normalizeCatalogList(rows, listKey) {
  const list = Array.isArray(rows) ? rows : [];
  return list
    .map((row, index) => normalizeWalletCatalogRow(row, index + 1, listKey))
    .filter(Boolean)
    .slice(0, DEFAULT_LIST_TARGET_SIZE);
}

function dedupeCatalogList(rows) {
  const map = new Map();
  for (const row of Array.isArray(rows) ? rows : []) {
    if (!row || !row.wallet) continue;
    if (!map.has(row.wallet)) {
      map.set(row.wallet, row);
    }
  }
  return Array.from(map.values());
}

async function fetchWalletCatalogRows(sortKey) {
  const rows = [];
  for (let page = 1; page <= DEFAULT_LIST_FETCH_PAGES; page += 1) {
    let pageRows = [];
    for (let attempt = 0; attempt < 3; attempt += 1) {
      try {
        const query = new URLSearchParams();
        query.set("page", String(page));
        query.set("page_size", String(DEFAULT_LIST_FETCH_PAGE_SIZE));
        query.set("sort", sortKey);
        query.set("dir", "desc");
        const payload = await fetchJson(`${API_BASE}/api/wallets?${query.toString()}`, 20000);
        pageRows = Array.isArray(payload && payload.rows)
          ? payload.rows
          : Array.isArray(payload && payload.wallets)
          ? payload.wallets
          : [];
        break;
      } catch (_error) {
        if (attempt < 2) {
          await sleep(250);
          continue;
        }
        pageRows = [];
      }
    }
    if (!pageRows.length) break;
    rows.push(...pageRows);
    if (pageRows.length < DEFAULT_LIST_FETCH_PAGE_SIZE) break;
  }
  return rows;
}

function loadWalletCatalogSourceRows() {
  try {
    const stat = fs.existsSync(DEFAULT_WALLET_SOURCE_PATH) ? fs.statSync(DEFAULT_WALLET_SOURCE_PATH) : null;
    const mtimeMs = stat ? Number(stat.mtimeMs || 0) : 0;
    if (walletCatalogSourceCache.rows && walletCatalogSourceCache.mtimeMs === mtimeMs) {
      return walletCatalogSourceCache.rows;
    }
    if (!stat || !stat.isFile()) {
      walletCatalogSourceCache.rows = null;
      walletCatalogSourceCache.mtimeMs = 0;
      return null;
    }
    const payload = JSON.parse(fs.readFileSync(DEFAULT_WALLET_SOURCE_PATH, "utf8"));
    const rows = Array.isArray(payload && payload.rows) ? payload.rows : [];
    walletCatalogSourceCache.rows = rows;
    walletCatalogSourceCache.mtimeMs = mtimeMs;
    walletCatalogSourceCache.error = null;
    return rows;
  } catch (error) {
    walletCatalogSourceCache.error = String(error && error.message ? error.message : error);
    return walletCatalogSourceCache.rows;
  }
}

async function loadDefaultWalletCatalog(state, projectKey, force = false) {
  const projectState = getProjectState(state, projectKey);
  const cached = getDefaultWalletCatalog(projectState);
  const now = Date.now();
  if (!force && cached && cached.generatedAt && now - cached.generatedAt < DEFAULT_LIST_CACHE_TTL_MS) {
    return cached;
  }
  if (walletListFetchInFlight.has(projectKey)) {
    return walletListFetchInFlight.get(projectKey);
  }
  const inFlight = (async () => {
    let sourceRows = loadWalletCatalogSourceRows();
    let sourceSource = "local_wallet_dataset";
    if (!Array.isArray(sourceRows) || !sourceRows.length) {
      const [tradesResult, pnlResult] = await Promise.allSettled([
        fetchWalletCatalogRows("trades"),
        fetchWalletCatalogRows("pnlUsd"),
      ]);
      const tradesRows = tradesResult.status === "fulfilled" ? tradesResult.value : [];
      const pnlRows = pnlResult.status === "fulfilled" ? pnlResult.value : [];
      sourceRows = dedupeCatalogList([...tradesRows, ...pnlRows]);
      sourceSource = "api_wallets";
    }
    const rows = Array.isArray(sourceRows) ? sourceRows : [];
    const tradesRows = rows
      .slice()
      .sort((left, right) => {
        const diff = toNumber(right && right.trades, 0) - toNumber(left && left.trades, 0);
        if (diff) return diff;
        const pnlDiff = toNumber(right && right.pnlUsd, 0) - toNumber(left && left.pnlUsd, 0);
        if (pnlDiff) return pnlDiff;
        return String(left && left.wallet ? left.wallet : "").localeCompare(String(right && right.wallet ? right.wallet : ""));
      });
    const pnlRows = rows
      .filter((row) => toNumber(row && row.pnlUsd, 0) > 0)
      .sort((left, right) => {
        const diff = toNumber(right && right.pnlUsd, 0) - toNumber(left && left.pnlUsd, 0);
        if (diff) return diff;
        const tradeDiff = toNumber(right && right.trades, 0) - toNumber(left && left.trades, 0);
        if (tradeDiff) return tradeDiff;
        return String(left && left.wallet ? left.wallet : "").localeCompare(String(right && right.wallet ? right.wallet : ""));
      });
    const tradesList = normalizeCatalogList(tradesRows, "trades");
    const pnlList = normalizeCatalogList(pnlRows, "pnl");
    const combined = dedupeCatalogList([...tradesList, ...pnlList]);
    const catalog = {
      generatedAt: now,
      source: sourceSource,
      lists: {
        trades: tradesList,
        pnl: pnlList,
        combined,
      },
    };
    setDefaultWalletCatalog(projectState, catalog);
    state.projects[projectKey] = projectState;
    writeState(STATE_FILE, state);
    return catalog;
  })();
  try {
    walletListFetchInFlight.set(projectKey, inFlight);
    return await inFlight;
  } finally {
    walletListFetchInFlight.delete(projectKey);
  }
}

function getCatalogListTitle(listKey) {
  if (listKey === "pnl") return "Top 50 Wallets by PnL";
  return "Top 50 Wallets by Number of Trades";
}

function getCatalogListEmptyText(listKey) {
  if (listKey === "pnl") return "No positive-PnL wallets were returned yet.";
  return "No trade-ranked wallets were returned yet.";
}

function buildWalletCatalogHomeMenu() {
  return buildInlineMenu([
    [
      { label: "🔥 Top 50 by Trades", data: "wallet_list_home:trades" },
      { label: "💰 Top 50 by PnL", data: "wallet_list_home:pnl" },
    ],
    [
      { label: "⬅️ Back to Main Menu", data: "menu:main_menu" },
    ],
  ]);
}

function buildWalletCatalogPageMenu(listKey, page, totalPages, entries, chatState) {
  const rows = [];
  for (let index = 0; index < entries.length; index += 2) {
    const first = entries[index];
    const second = entries[index + 1];
    const buttons = [];
    if (first) {
      buttons.push({
        label: `${findWatchedWallet(chatState, first.wallet) ? "🟢" : "⚪"} ${shortText(first.label || first.wallet, 14)}`,
        data: `wallet_pick:${listKey}:${page}:${index}`,
      });
    }
    if (second) {
      buttons.push({
        label: `${findWatchedWallet(chatState, second.wallet) ? "🟢" : "⚪"} ${shortText(second.label || second.wallet, 14)}`,
        data: `wallet_pick:${listKey}:${page}:${index + 1}`,
      });
    }
    if (buttons.length) rows.push(buttons);
  }
  rows.push([
    { label: "⬅️ Prev", data: `wallet_list_page:${listKey}:${Math.max(1, page - 1)}` },
    { label: `🔄 Refresh`, data: `wallet_list_page:${listKey}:${page}` },
    { label: "Next ➡️", data: `wallet_list_page:${listKey}:${Math.min(totalPages, page + 1)}` },
  ]);
  rows.push([
    { label: listKey === "pnl" ? "🔥 Top 50 by Trades" : "💰 Top 50 by PnL", data: `wallet_list_page:${listKey === "pnl" ? "trades" : "pnl"}:1` },
  ]);
  rows.push([
    { label: "⬅️ Back to Main Menu", data: "menu:main_menu" },
  ]);
  return buildInlineMenu(rows);
}

function formatCatalogWalletLine(entry, index) {
  const walletLabel = entry.label || formatWalletId(entry.wallet);
  const stats = [];
  if (Number.isFinite(Number(entry.trades))) stats.push(`Trades ${formatCountCompact(entry.trades)}`);
  if (Number.isFinite(Number(entry.pnlUsd))) stats.push(`PnL ${formatCompactMoney(entry.pnlUsd)}`);
  if (Number.isFinite(Number(entry.volumeUsd))) stats.push(`Volume ${formatCompactMoney(entry.volumeUsd)}`);
  return [
    `${index + 1}. ${walletLabel}`,
    `   ${formatWalletId(entry.wallet)}${stats.length ? ` • ${stats.join(" • ")}` : ""}`,
  ].join("\n");
}

async function sendWalletCatalogHome(botToken, chatId, project, state, chatState) {
  try {
    await loadDefaultWalletCatalog(state, project.projectKey);
    await sendMessage(
      botToken,
      chatId,
      [
        "Wallet lists",
        "",
        "Choose from the 100 default wallets used by this bot.",
        "Tap a wallet to add it to tracking automatically.",
        "",
        "Need a different wallet? Use /watch <wallet>.",
      ].join("\n"),
      { replyMarkup: buildWalletCatalogHomeMenu() }
    );
  } catch (error) {
    await sendMessage(
      botToken,
      chatId,
      [
        "Wallet lists",
        "",
        "I could not load the default wallet lists just now.",
        String(error && error.message ? error.message : error),
      ].join("\n"),
      { replyMarkup: buildWalletMenu() }
    );
  }
}

async function sendWalletCatalogPage(botToken, chatId, project, state, chatState, listKey = "trades", page = 1) {
  try {
    const catalog = await loadDefaultWalletCatalog(state, project.projectKey);
    const lists = catalog && catalog.lists && typeof catalog.lists === "object" ? catalog.lists : {};
    const key = String(listKey || "trades").toLowerCase() === "pnl" ? "pnl" : "trades";
    const list = Array.isArray(lists[key]) ? lists[key] : [];
    const pageSize = DEFAULT_LIST_PAGE_SIZE;
    const totalPages = Math.max(1, Math.ceil(list.length / pageSize));
    const safePage = Math.max(1, Math.min(totalPages, Number(page || 1) || 1));
    const start = (safePage - 1) * pageSize;
    const pageRows = list.slice(start, start + pageSize);
    const lines = pageRows.length
      ? pageRows.map((entry, index) => formatCatalogWalletLine(entry, start + index))
      : [getCatalogListEmptyText(key)];
    await sendMessage(
      botToken,
      chatId,
      [
        getCatalogListTitle(key),
        "",
        `Page ${safePage} / ${totalPages}`,
        "Tap a wallet to add it to tracking automatically.",
        "",
        ...lines,
      ].join("\n"),
      { replyMarkup: buildWalletCatalogPageMenu(key, safePage, totalPages, pageRows, chatState) }
    );
  } catch (error) {
    await sendMessage(
      botToken,
      chatId,
      [
        getCatalogListTitle(String(listKey || "trades").toLowerCase() === "pnl" ? "pnl" : "trades"),
        "",
        "I could not load the selected wallet list just now.",
        String(error && error.message ? error.message : error),
      ].join("\n"),
      { replyMarkup: buildWalletCatalogHomeMenu() }
    );
  }
}

async function handleWalletCatalogSelection(botToken, chatId, project, state, chatState, listKey, page, index) {
  try {
    const catalog = await loadDefaultWalletCatalog(state, project.projectKey);
    const lists = catalog && catalog.lists && typeof catalog.lists === "object" ? catalog.lists : {};
    const key = String(listKey || "trades").toLowerCase() === "pnl" ? "pnl" : "trades";
    const list = Array.isArray(lists[key]) ? lists[key] : [];
    const safeIndex = Math.max(0, Number(index || 0) || 0);
    const entry = list[safeIndex] || null;
    if (!entry) {
      await sendMessage(
        botToken,
        chatId,
        "That wallet is no longer available in the current default list. Refresh the list and try again.",
        { replyMarkup: buildWalletCatalogHomeMenu() }
      );
      return;
    }
    const watch = findWatchedWallet(chatState, entry.wallet);
    if (watch) {
      await sendWalletStatus(botToken, chatId, project, chatState, entry.wallet);
      return;
    }
    await addWalletWatch(botToken, chatId, project, state, chatState, `${entry.wallet} ${entry.label || ""}`.trim());
  } catch (error) {
    await sendMessage(
      botToken,
      chatId,
      [
        "Wallet lists",
        "",
        "I could not add that wallet from the default list.",
        String(error && error.message ? error.message : error),
      ].join("\n"),
      { replyMarkup: buildWalletCatalogHomeMenu() }
    );
  }
}

async function fetchJson(url, timeoutMs = 15000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      method: "GET",
      headers: {
        Accept: "application/json",
        "User-Agent": "pacifica-telegram-copilot/1.0",
      },
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

function buildWalletMenu() {
  return buildInlineMenu([
    [
      { label: "➕ Add wallet", data: "menu:add_wallet" },
      { label: "🔢 Wallet status", data: "menu:wallet_status" },
    ],
    [
      { label: "🚨 Alerts", data: "menu:alerts" },
      { label: "📂 Wallets", data: "menu:watch_list" },
    ],
    [
      { label: "ℹ️ Help", data: "menu:help" },
    ],
  ]);
}

function buildWalletPickerMenu(watches, emptyLabel = "No watched wallets yet.") {
  const rows = [];
  const list = Array.isArray(watches) ? watches : [];
  if (!list.length) {
    rows.push([{ label: "➕ Add wallet", data: "menu:add_wallet" }]);
  } else {
    for (const watch of list.slice(0, 8)) {
      const label = shortText(getWatchedWalletDisplayLabel(watch) || watch.wallet, 18);
      rows.push([
        { label: `📍 ${label}`, data: `wallet_status:${watch.wallet}` },
      ]);
    }
  }
  rows.push([
    { label: "📂 Wallets", data: "menu:watch_list" },
    { label: "⬅️ Back to Main Menu", data: "menu:main_menu" },
  ]);
  return buildInlineMenu(rows);
}

function buildWalletDetailMenu(wallet) {
  return buildInlineMenu([
    [
      { label: "🏷️ Label wallet", data: `wallet_label:${wallet}` },
      { label: "➖ Remove wallet", data: `wallet_remove:${wallet}` },
    ],
    [
      { label: "🚨 Alerts", data: `wallet_alerts:${wallet}` },
      { label: "⏹️ Stop alerts", data: `wallet_alert_stop:${wallet}` },
    ],
    [
      { label: "📂 Wallets", data: "menu:watch_list" },
      { label: "⬅️ Back to Main Menu", data: "menu:main_menu" },
    ],
  ]);
}

function buildAlertSetupMenu(wallet) {
  return buildInlineMenu([
    [
      { label: "✅ Yes, alerts on", data: `alert_setup_yes:${wallet}` },
      { label: "➖ No alerts", data: `alert_setup_no:${wallet}` },
    ],
    [
      { label: "⬅️ Back to Main Menu", data: "menu:main_menu" },
    ],
  ]);
}

function buildAlertTypeMenu(wallet, selected = []) {
  const set = new Set((Array.isArray(selected) ? selected : []).map((value) => String(value || "").trim().toLowerCase()).filter(Boolean));
  const labelFor = (key, label) => `${set.has(key) ? "☑️" : "⬜"} ${label}`;
  return buildInlineMenu([
    [
      { label: labelFor("opened", "Open Position"), data: `alert_type:${wallet}:opened` },
      { label: labelFor("closed", "Close Position"), data: `alert_type:${wallet}:closed` },
    ],
    [
      { label: labelFor("increased", "Increase Size"), data: `alert_type:${wallet}:increased` },
      { label: labelFor("reduced", "Reduce Size"), data: `alert_type:${wallet}:reduced` },
    ],
    [
      { label: "✅ Save alerts", data: `alert_save:${wallet}` },
      { label: "⏹️ Stop alerts", data: `wallet_alert_stop:${wallet}` },
    ],
    [
      { label: "⬅️ Back to Main Menu", data: "menu:main_menu" },
    ],
  ]);
}

function buildAlertsMenu(watches) {
  const list = Array.isArray(watches) ? watches : [];
  if (!list.length) {
    return buildInlineMenu([
      [{ label: "➕ Add wallet", data: "menu:add_wallet" }],
      [{ label: "⬅️ Back to Main Menu", data: "menu:main_menu" }],
    ]);
  }
  const rows = list.slice(0, 8).map((watch) => {
    const active = watch.alertPreferences && watch.alertPreferences.enabled;
    const label = getWatchedWalletDisplayLabel(watch) || watch.wallet;
    return [
      { label: `${active ? "🟢" : "⚪"} ${shortText(label, 18)}`, data: `wallet_alerts:${watch.wallet}` },
    ];
  });
  rows.push([{ label: "⬅️ Back to Main Menu", data: "menu:main_menu" }]);
  return buildInlineMenu(rows);
}

function resolveModeFromText(text) {
  const normalized = cleanText(text).toLowerCase();
  if (normalized === "add wallet" || normalized === "track wallet" || normalized === "add") return "wallet_catalog_home";
  if (normalized === "remove wallet" || normalized === "unwatch wallet" || normalized === "remove") return "unwatch_wallet";
  if (normalized === "wallet status" || normalized === "wallet" || normalized === "status") return "wallet_status";
  if (normalized === "watched wallets" || normalized === "watch list" || normalized === "wallets") return "watch_list";
  if (normalized === "alerts") return "alerts";
  if (normalized === "label wallet" || normalized === "label") return "label_wallet";
  if (normalized === "lists" || normalized === "wallet lists") return "wallet_catalog_home";
  if (normalized === "help") return "help";
  if (normalized === "/watch" || normalized === "/add") return "watch_wallet";
  if (normalized === "/unwatch" || normalized === "/remove") return "unwatch_wallet";
  if (normalized === "/wallet" || normalized === "/status") return "wallet_status";
  if (normalized === "/watches" || normalized === "/wallets") return "watch_list";
  if (normalized === "/alerts") return "alerts";
  if (normalized === "/label") return "label_wallet";
  if (normalized === "/lists") return "wallet_catalog_home";
  if (normalized === "/help") return "help";
  return "general";
}

function toNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function formatUsd(value) {
  const num = toNumber(value, 0);
  const sign = num >= 0 ? "+" : "";
  return `${sign}${num.toFixed(2)}`;
}

function formatMoney(value) {
  const num = toNumber(value, 0);
  return `${num < 0 ? "-" : ""}$${Math.abs(num).toLocaleString("en-US", { maximumFractionDigits: 2, minimumFractionDigits: 2 })}`;
}

function formatCompactMoney(value) {
  const num = Math.abs(toNumber(value, 0));
  const sign = toNumber(value, 0) < 0 ? "-" : "";
  if (num >= 1_000_000) return `${sign}$${(num / 1_000_000).toFixed(2)}M`;
  if (num >= 1_000) return `${sign}$${(num / 1_000).toFixed(2)}K`;
  return `${sign}$${num.toFixed(2)}`;
}

function formatPositionHeader(index, row) {
  const symbol = String(row && row.symbol ? row.symbol : "unknown").toUpperCase();
  const side = String(row && row.side ? row.side : row && row.rawSide ? row.rawSide : "unknown").toUpperCase();
  return `${index + 1}. ${symbol} • ${side}`;
}

function formatPositionCard(index, row) {
  const size = Number(row && row.size != null ? row.size : row && row.currentSize != null ? row.currentSize : 0);
  const pnl = Number(row && row.pnl != null ? row.pnl : 0);
  return [
    "━━━━━━━━━━━━━━━━━━━━",
    formatPositionHeader(index, row),
    `Size: ${formatAmount(size)} • PnL: ${formatUsd(pnl)}`,
    `Entry: ${row && row.entry != null ? formatMoney(row.entry) : "-"} • Mark: ${row && row.mark != null ? formatMoney(row.mark) : "-"}`,
    `Opened: ${row && row.openedAt ? formatTimestamp(row.openedAt) : "-"}`,
    `Updated: ${row && row.updatedAt ? formatTimestamp(row.updatedAt) : "-"}`,
  ].join("\n");
}

function formatAlertCard(event, watch, index = 0) {
  const row = event.row || {};
  const previousSize = event.previous && Number.isFinite(Number(event.previous.size)) ? Number(event.previous.size) : null;
  const delta = Number.isFinite(Number(event.delta)) ? Number(event.delta) : null;
  const currentSizeValue = Number(
    event.type === "closed"
      ? 0
      : Number.isFinite(Number(row.size))
      ? Number(row.size)
      : Number(previousSize || 0) + Number(delta || 0)
  );
  const title =
    event.type === "opened"
      ? "🟢 Position Opened"
      : event.type === "increased"
      ? "🟢 Position Increased"
      : event.type === "reduced"
      ? "📉 Position Reduced"
      : event.type === "closed"
      ? "🔴 Position Closed"
      : "Position Update";
  const changeValue =
    delta !== null
      ? delta
      : event.type === "closed"
      ? -Number(previousSize || row.size || 0)
      : Number(row.size || 0);
  const activityAt = Number(event.eventTs || row.updatedAt || row.openedAt || Date.now());
  return [
    "━━━━━━━━━━━━━━━━━━━━",
    index ? `#${index} ${title}` : title,
    `Wallet: ${formatWalletId(watch.wallet)}${watch.label ? ` (${watch.label})` : ""}`,
    `Symbol: ${String(row.symbol || "unknown").toUpperCase()}`,
    `Side: ${String(row.side || "unknown").toUpperCase()}`,
    `Change: ${formatCompactMoney(changeValue)}`,
    `New Size: ${event.type === "closed" ? "$0.00" : formatCompactMoney(currentSizeValue)}`,
    `Entry: ${row.entry != null ? formatMoney(row.entry) : "-"} • Mark: ${row.mark != null ? formatMoney(row.mark) : "-"}`,
    `Unrealized: ${Number.isFinite(Number(row.pnl)) ? formatCompactMoney(row.pnl) : "-"}`,
    `Last activity: ${formatRelativeSeconds(Date.now() - activityAt)} • ${formatTimestamp(activityAt)}`,
  ].join("\n");
}

function formatAmount(value) {
  const num = toNumber(value, 0);
  return num.toFixed(4);
}

function getPerformanceRange(performance) {
  const ranges = performance && performance.metricsByRange && typeof performance.metricsByRange === "object" ? performance.metricsByRange : {};
  return ranges.all || ranges["30d"] || ranges["7d"] || ranges["1d"] || null;
}

function getTotalVolumeUsd(performance) {
  const range = getPerformanceRange(performance);
  if (!range) return 0;
  return toNumber(range.tradingVolumeUsd || range.volumeUsd || range.volume || 0, 0);
}

function formatPositionLine(row, index) {
  const symbol = String(row.symbol || "UNKNOWN").toUpperCase();
  const side = String(row.side || "unknown").toLowerCase();
  const size = formatAmount(row.size);
  const pnl = formatUsd(row.pnl);
  const entry = toNumber(row.entry, 0);
  const mark = toNumber(row.mark, 0);
  const openedAt = row.openedAt ? new Date(row.openedAt).toISOString().replace("T", " ").replace(".000Z", " UTC") : "-";
  const bits = [
    `${index + 1}. ${symbol} ${side}`,
    `size ${size}`,
    `PnL ${pnl}`,
    `entry ${entry ? entry.toFixed(2) : "-"}`,
    `mark ${mark ? mark.toFixed(2) : "-"}`,
    `opened ${openedAt}`,
  ];
  return bits.join(" • ");
}

function formatRelativeSeconds(msValue) {
  const ms = Math.max(0, Number(msValue || 0));
  const seconds = Math.max(0, Math.round(ms / 1000));
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function formatOpenPositionsBlock(positions) {
  const rows = Array.isArray(positions) ? positions : [];
  if (!rows.length) return "No open positions right now.";
  return rows.slice(0, 12).map((row, index) => formatPositionLine(row, index)).join("\n");
}

function sumOpenPositionPnl(positions) {
  return (Array.isArray(positions) ? positions : []).reduce((total, row) => total + toNumber(row && row.pnl, 0), 0);
}

function normalizeEventTimestamp(row) {
  return (
    Number(
      row &&
        (row.lastStateChangeAt ||
          row.updatedAt ||
          row.updated_at ||
          row.observedAt ||
          row.timestamp ||
          row.createdAt ||
          row.created_at ||
          row.openedAt ||
          row.opened_at ||
          0)
    ) || 0
  );
}

function getPositionComparableTimestamp(row) {
  return Math.max(
    Number((row && row.updatedAt) || 0) || 0,
    Number((row && row.openedAt) || 0) || 0,
    Number((row && row.lastStateChangeAt) || 0) || 0
  );
}

function extractAlertEventsFromPayload(payload, sinceTs) {
  const rows = Array.isArray(payload && payload.positions) ? payload.positions : [];
  return rows
    .map((row) => {
      const eventType = String(row && (row.eventType || row.sideEvent || row.cause || "")).trim().toLowerCase();
      const eventTs = toNumber(
        row &&
          (row.lastStateChangeAt ||
            row.updatedAt ||
            row.updated_at ||
            row.observedAt ||
            row.timestamp ||
            row.createdAt ||
            row.created_at ||
            row.openedAt ||
            row.opened_at),
        0
      );
      const oldSize = toNumber(row && (row.oldSize || row.previousSize || row.startSize), NaN);
      const newSize = toNumber(
        row &&
          (row.newSize ||
            row.currentSize ||
            row.size ||
            row.amount ||
            row.positionSize ||
            row.eventSize),
        NaN
      );
      const sizeDelta =
        Number.isFinite(oldSize) && Number.isFinite(newSize) ? Number((newSize - oldSize).toFixed(8)) : null;
      let type = null;
      if (eventType.includes("position_opened")) {
        type = "opened";
      } else if (
        eventType.includes("position_increased") ||
        eventType.includes("increase") ||
        eventType.includes("position_bought")
      ) {
        type = "increased";
      } else if (
        eventType.includes("position_reduced") ||
        eventType.includes("position_closed_partial") ||
        eventType.includes("partial")
      ) {
        type = "reduced";
      } else if (eventType.includes("position_closed")) {
        type = "closed";
      } else if (eventType.includes("position_size_changed")) {
        if (Number.isFinite(sizeDelta)) {
          if (Math.abs(newSize) <= 1e-10) type = "closed";
          else if (sizeDelta > 0) type = "increased";
          else if (sizeDelta < 0) type = "reduced";
          else type = "changed";
        } else {
          type = "changed";
        }
      }
      return {
        row,
        eventType,
        type,
        eventTs,
        oldSize: Number.isFinite(oldSize) ? oldSize : null,
        newSize: Number.isFinite(newSize) ? newSize : null,
        sizeDelta,
      };
    })
    .filter((item) => item.eventTs > sinceTs)
    .filter((item) => Boolean(item.type));
}

function getWatchedWalletDisplayLabel(watch) {
  if (!watch) return "";
  const label = String(watch.label || "").trim();
  if (label) return label;
  return watch.wallet ? shortText(watch.wallet, 18) : "";
}

function watchPendingWallet(chatState) {
  const value = String(chatState && chatState.pendingWallet ? chatState.pendingWallet : "").trim();
  return value || "";
}

function getAlertTypesLabel(types) {
  const list = Array.isArray(types) ? types : [];
  if (!list.length) return "none";
  const map = {
    opened: "Open",
    closed: "Close",
    increased: "Increase",
    reduced: "Reduce",
  };
  return list
    .map((value) => String(value || "").trim().toLowerCase())
    .filter(Boolean)
    .map((value) => map[value] || value)
    .join(", ");
}

function getAlertPref(watch) {
  const prefs = watch && watch.alertPreferences && typeof watch.alertPreferences === "object" ? watch.alertPreferences : {};
  const enabled = Boolean(prefs.enabled);
  const types = Array.isArray(prefs.types) ? prefs.types.map((value) => String(value || "").trim().toLowerCase()).filter(Boolean) : [];
  return {
    enabled,
    types,
  };
}

function setAlertPref(watch, next = {}) {
  if (!watch || typeof watch !== "object") return null;
  const prefs = getAlertPref(watch);
  if (Object.prototype.hasOwnProperty.call(next, "enabled")) {
    prefs.enabled = Boolean(next.enabled);
  }
  if (Object.prototype.hasOwnProperty.call(next, "types")) {
    prefs.types = Array.from(
      new Set(
        (Array.isArray(next.types) ? next.types : [])
          .map((value) => String(value || "").trim().toLowerCase())
          .filter(Boolean)
      )
    );
  }
  watch.alertPreferences = prefs;
  watch.updatedAt = Date.now();
  return watch;
}

function shouldNotifyAlertEvent(watch, eventType) {
  const prefs = getAlertPref(watch);
  if (!prefs.enabled) return false;
  const normalized = String(eventType || "").trim().toLowerCase();
  if (!normalized) return false;
  if (!prefs.types.length) return false;
  return prefs.types.includes(normalized);
}

function normalizeWalletKey(row) {
  const safeRow = row && typeof row === "object" ? row : {};
  const symbol = String(safeRow.symbol || safeRow.asset || safeRow.token || "").trim().toUpperCase();
  const sideSource = String(
    safeRow.rawSide ||
      safeRow.side ||
      safeRow.direction ||
      safeRow.positionSide ||
      safeRow.type ||
      ""
  )
    .trim()
    .toLowerCase();
  const side = sideSource.includes("short")
    ? "short"
    : sideSource.includes("sell")
    ? "short"
    : sideSource.includes("long")
    ? "long"
    : sideSource.includes("open_short")
    ? "short"
    : sideSource.includes("open_long")
    ? "long"
    : sideSource.includes("bid")
    ? "long"
    : sideSource.includes("ask")
    ? "short"
    : sideSource || "unknown";
  const marginMode = String(
    safeRow.marginMode || safeRow.margin_mode || safeRow.marginmode || safeRow.margin || ""
  )
    .trim()
    .toLowerCase();
  const isolated =
    safeRow.isolated !== undefined ? Boolean(safeRow.isolated) : marginMode.includes("isolated");
  return `${symbol || "?"}|${side}|${isolated ? "isolated" : "cross"}`;
}

function toOpenSize(row) {
  const safeRow = row && typeof row === "object" ? row : {};
  const candidates = [
    safeRow.currentSize,
    safeRow.size,
    safeRow.amount,
    safeRow.positionSize,
    safeRow.newSize,
  ];
  for (const value of candidates) {
    const num = Number(value);
    if (Number.isFinite(num)) return Math.abs(num);
  }
  const usd = Number(safeRow.positionUsd || safeRow.eventUsd || 0);
  if (Number.isFinite(usd) && usd > 0) return usd;
  return 0;
}

function extractOpenPositionsFromSnapshot(payload) {
  const rows = Array.isArray(payload && payload.positions)
    ? payload.positions
    : Array.isArray(payload && payload.walletPositions)
    ? payload.walletPositions
    : [];
  return rows
    .map((row) => {
      const size = toOpenSize(row);
      if (!(size > 0)) return null;
      const wallet = String((row && row.wallet) || "").trim();
      const symbol = String((row && row.symbol) || "").trim().toUpperCase();
      const side = normalizeWalletKey(row).split("|")[1] || "unknown";
      const entry = Number(
        (row &&
          (row.entry ||
            row.entryPrice ||
            row.entry_price ||
            row.price ||
            row.averageEntryPrice ||
            0)) || 0
      ) || 0;
      const mark = Number(
        (row &&
          (row.mark ||
            row.markPrice ||
            row.mark_price ||
            row.priceMark ||
            row.lastPrice ||
            0)) || 0
      ) || 0;
      return {
        wallet,
        symbol,
        side,
        key: normalizeWalletKey(row),
        size,
        entry,
        mark,
        pnl:
          Number((row && (row.pnl || row.unrealizedPnlUsd || row.pnlUsd || row.positionPnlUsd || 0)) || 0) || 0,
        openedAt:
          Number(
            (row &&
              (row.openedAt ||
                row.opened_at ||
                row.createdAt ||
                row.created_at ||
                row.timestamp ||
                row.lastOpenedAt ||
                0)) || 0
          ) || 0,
        updatedAt:
          Number(
            (row &&
              (row.updatedAt ||
                row.updated_at ||
                row.lastStateChangeAt ||
                row.observedAt ||
                row.timestamp ||
                row.lastOpenedAt ||
                0)) || 0
          ) || 0,
        sourceRow: row,
      };
    })
    .filter(Boolean);
}

function mergeOpenPositionRows(...groups) {
  const map = new Map();
  for (const group of groups) {
    const rows = Array.isArray(group) ? group : [];
    for (const row of rows) {
      if (!row || !row.key) continue;
      const existing = map.get(row.key);
      if (!existing) {
        map.set(row.key, row);
        continue;
      }
      const currentScore = Math.max(
        Number(row.updatedAt || 0) || 0,
        Number(row.openedAt || 0) || 0
      );
      const existingScore = Math.max(
        Number(existing.updatedAt || 0) || 0,
        Number(existing.openedAt || 0) || 0
      );
      if (currentScore >= existingScore) {
        map.set(row.key, row);
      }
    }
  }
  return Array.from(map.values()).sort((left, right) => {
    const rightScore = Math.max(Number(right.updatedAt || 0) || 0, Number(right.openedAt || 0) || 0);
    const leftScore = Math.max(Number(left.updatedAt || 0) || 0, Number(left.openedAt || 0) || 0);
    return rightScore - leftScore || String(right.symbol || "").localeCompare(String(left.symbol || ""));
  });
}

function computeTradeWindowStats({ trades = [], positions = [], overview = null, performance = null }) {
  const rows = Array.isArray(trades) ? trades : [];
  const tradeTimes = rows
    .map((row) => Number((row && (row.timestamp || row.createdAt || row.created_at || row.updatedAt || row.observedAt)) || 0) || 0)
    .filter((ts) => ts > 0);
  const positionTimes = Array.isArray(positions)
    ? positions
        .flatMap((row) => [Number((row && row.openedAt) || 0) || 0, Number((row && row.updatedAt) || 0) || 0])
        .filter((ts) => ts > 0)
    : [];
  const overviewLastOpenedAt = Number(overview && overview.summary && overview.summary.liveLastOpenedAt) || 0;
  const overviewLastUpdatedAt = Number(overview && overview.summary && overview.summary.liveLastUpdatedAt) || 0;
  const performanceLastUpdatedAt = Number((performance && performance.account && performance.account.updatedAt) || 0) || 0;
  const performanceFirstSeenAt =
    Number(
      (performance &&
        performance.metricsByRange &&
        performance.metricsByRange.all &&
        performance.metricsByRange.all.firstSeenAt) ||
        0
    ) || 0;
  const performanceLastSeenAt =
    Number(
      (performance &&
        performance.metricsByRange &&
        performance.metricsByRange.all &&
        performance.metricsByRange.all.lastSeenAt) ||
        0
    ) || 0;
  const firstTradeAt = tradeTimes.length
    ? Math.min(...tradeTimes)
    : positionTimes.length
    ? Math.min(...positionTimes)
    : performanceFirstSeenAt || 0;
  const lastTradeAt = tradeTimes.length
    ? Math.max(...tradeTimes)
    : Math.max(...positionTimes, overviewLastOpenedAt, overviewLastUpdatedAt, performanceLastUpdatedAt, performanceLastSeenAt);
  const recentTradeVolumeUsd = rows.reduce((sum, row) => sum + toNumber(row && row.volumeUsd, 0), 0);
  return {
    firstTradeAt: firstTradeAt || 0,
    lastTradeAt: lastTradeAt || 0,
    recentTradeCount: rows.length,
    recentTradeVolumeUsd: Number(recentTradeVolumeUsd.toFixed(2)),
  };
}

function formatWalletPositions(positions) {
  const rows = Array.isArray(positions) ? positions : [];
  if (!rows.length) return "No open positions detected.";
  return rows
    .slice(0, 12)
    .map((row) => {
      const pnl = Number(row.pnl || 0);
      const pnlText = Number.isFinite(pnl) ? `${pnl >= 0 ? "+" : ""}${pnl.toFixed(2)}` : "-";
      return `• ${row.symbol} ${row.side} size ${row.size} • pnl ${pnlText}`;
    })
    .join("\n");
}

async function fetchWalletSnapshot(wallet) {
  const normalized = String(wallet || "").trim();
  if (!normalized) throw new Error("wallet_required");
  const [overviewResult, performanceResult, tradesResult] = await Promise.allSettled([
    fetchJson(
      `${API_BASE}/api/live-trades/wallet-first?wallet=${encodeURIComponent(normalized)}&position_limit=200&events_limit=25`,
      20000
    ),
    fetchJson(`${API_BASE}/api/wallet-performance/wallet/${encodeURIComponent(normalized)}?force=1`, 20000),
    fetchJson(`${API_BASE}/api/trades/history?wallet=${encodeURIComponent(normalized)}&limit=200`, 15000),
  ]);
  const overview = overviewResult.status === "fulfilled" ? overviewResult.value || {} : null;
  const performance = performanceResult.status === "fulfilled" ? performanceResult.value || {} : null;
  const trades = tradesResult.status === "fulfilled" ? tradesResult.value || {} : null;
  const overviewPositions = extractOpenPositionsFromSnapshot(overview);
  const performancePositions = extractOpenPositionsFromSnapshot(performance);
  const positions = mergeOpenPositionRows(overviewPositions, performancePositions);
  const totalPnl = sumOpenPositionPnl(positions);
  const totalVolumeUsd = getTotalVolumeUsd(performance);
  const tradeWindow = computeTradeWindowStats({
    trades: Array.isArray(trades && trades.rows) ? trades.rows : [],
    positions,
    overview,
    performance,
  });
  return {
    wallet: normalized,
    overview,
    performance,
    trades,
    positions,
    openCount:
      Number(
        (overview && overview.summary && overview.summary.openPositionsTotal) ||
          (performance && performance.account && performance.account.openPositions) ||
          positions.length ||
          0
      ) || 0,
    equity:
      Number(
        (performance && performance.account && performance.account.accountEquityUsd) ||
          (overview && overview.summary && overview.summary.accountEquityUsd) ||
          0
      ) || 0,
    totalPnl,
    totalVolumeUsd,
    firstTradeAt: tradeWindow.firstTradeAt,
    lastTradeAt: tradeWindow.lastTradeAt,
    recentTradeCount: tradeWindow.recentTradeCount,
    recentTradeVolumeUsd: tradeWindow.recentTradeVolumeUsd,
    marginUsed:
      Number((performance && performance.account && performance.account.totalMarginUsedUsd) || 0) || 0,
    availableToSpend:
      Number((performance && performance.account && performance.account.availableToSpendUsd) || 0) || 0,
    return7d:
      Number(
        (performance &&
          performance.metricsByRange &&
          performance.metricsByRange["7d"] &&
          performance.metricsByRange["7d"].returnPct) ||
          0
      ) || 0,
    updatedAt:
      Number(
        (performance && performance.account && performance.account.updatedAt) ||
          (overview && overview.generatedAt) ||
          Date.now()
      ) || Date.now(),
  };
}

async function fetchWalletEventFeed(wallet) {
  const normalized = String(wallet || "").trim();
  if (!normalized) throw new Error("wallet_required");
  return fetchJson(
    `${API_BASE}/api/live-trades/wallet-first/positions?wallet=${encodeURIComponent(normalized)}&feed_mode=events&events_limit=50`,
    20000
  );
}

function renderWalletStatus(snapshot, watch) {
  const wallet = snapshot.wallet;
  const label = watch && watch.label ? watch.label : wallet;
  const positions = Array.isArray(snapshot.positions) ? snapshot.positions : [];
  const alertPref = getAlertPref(watch);
  const alertState = alertPref.enabled ? `On (${getAlertTypesLabel(alertPref.types)})` : "Off";
  const topPositions = positions.length
    ? [
        "",
        "Open positions",
        ...positions.slice(0, 6).map((row, index) => formatPositionCard(index, row)),
      ]
    : ["", "Open positions", "No open positions right now."];
  return [
    `📂 Wallet status`,
    "",
    `Wallet: ${label}`,
    `ID: ${formatWalletId(wallet)}`,
    `Alerts: ${alertState}`,
    `Open positions: ${snapshot.openCount}`,
    `Total PnL: ${formatUsd(snapshot.totalPnl)}`,
    `Total volume: $${snapshot.totalVolumeUsd.toFixed(2)}`,
    `First trade: ${formatTimestamp(snapshot.firstTradeAt)}`,
    `Last trade: ${formatTimestamp(snapshot.lastTradeAt)}`,
    `Recent trades: ${Number(snapshot.recentTradeCount || 0)} • $${Number(snapshot.recentTradeVolumeUsd || 0).toFixed(2)}`,
    `7d return: ${snapshot.return7d.toFixed(2)}%`,
    `Updated: ${new Date(snapshot.updatedAt).toISOString().replace("T", " ").replace(".000Z", " UTC")}`,
    ...topPositions,
  ].join("\n");
}

function diffPositions(previous, current) {
  const prevMap = new Map(Array.isArray(previous) ? previous.map((row) => [row.key, row]) : []);
  const nextMap = new Map(Array.isArray(current) ? current.map((row) => [row.key, row]) : []);
  const events = [];
  for (const [key, nextRow] of nextMap.entries()) {
    const prevRow = prevMap.get(key) || null;
    if (!prevRow) {
      events.push({
        type: "opened",
        key,
        row: nextRow,
        previous: null,
        eventTs: Math.max(getPositionComparableTimestamp(nextRow), Date.now()),
      });
      continue;
    }
    const prevSize = Number(prevRow.size || 0);
    const nextSize = Number(nextRow.size || 0);
    if (Number.isFinite(prevSize) && Number.isFinite(nextSize) && Math.abs(nextSize - prevSize) > 1e-10) {
      events.push({
        type: nextSize > prevSize ? "increased" : "reduced",
        key,
        row: nextRow,
        previous: prevRow,
        delta: Number((nextSize - prevSize).toFixed(8)),
        eventTs: Math.max(getPositionComparableTimestamp(nextRow), getPositionComparableTimestamp(prevRow), Date.now()),
      });
    }
  }
  for (const [key, prevRow] of prevMap.entries()) {
    if (!nextMap.has(key)) {
      events.push({
        type: "closed",
        key,
        row: prevRow,
        previous: prevRow,
        delta: Number((0 - Number(prevRow.size || 0)).toFixed(8)),
        eventTs: Math.max(getPositionComparableTimestamp(prevRow), Date.now()),
      });
    }
  }
  return events;
}

function renderPositionEvent(event, watch, index = 0) {
  return formatAlertCard(event, watch, index);
}

async function sendWalletStatus(botToken, chatId, project, chatState, walletInput) {
  const wallet = String(walletInput || "").trim();
  const watches = ensureWatchedWallets(chatState);
  if (!wallet) {
    if (!watches.length) {
      await sendMessage(
        botToken,
        chatId,
        [
          "No wallets are being tracked yet.",
          "",
          "Add a wallet first, then you can open Wallet Status from the saved list.",
        ].join("\n"),
        { replyMarkup: buildWalletMenu() }
      );
      return;
    }
    await sendMessage(
      botToken,
      chatId,
      [
        "Select a tracked wallet to view its status.",
        "",
        "You do not need to type the wallet address again.",
      ].join("\n"),
      { replyMarkup: buildWalletPickerMenu(watches) }
    );
    return;
  }
  try {
    const snapshot = await fetchWalletSnapshot(wallet);
    const watch = findWatchedWallet(chatState, wallet);
    await sendMessage(botToken, chatId, renderWalletStatus(snapshot, watch), { replyMarkup: buildWalletDetailMenu(wallet) });
  } catch (error) {
    await sendMessage(
      botToken,
      chatId,
      `Failed to load wallet status for ${wallet}.\n\n${String(error && error.message ? error.message : error)}`,
      { replyMarkup: buildWalletMenu() }
    );
  }
}

async function addWalletWatch(botToken, chatId, project, state, chatState, walletText) {
  const raw = cleanText(walletText);
  if (!raw) {
    chatState.mode = "watch_wallet";
    chatState.lastPrompt = "Send the wallet address you want to watch.";
    writeState(STATE_FILE, state);
    await sendMessage(botToken, chatId, chatState.lastPrompt, { replyMarkup: buildWalletMenu() });
    return;
  }
  const [wallet, ...labelParts] = raw.split(/\s+/);
  const label = labelParts.join(" ").trim();
  const watch = upsertWatchedWallet(chatState, wallet, label);
  if (!watch) {
    await sendMessage(botToken, chatId, "I could not save that wallet. Send a valid wallet address.", { replyMarkup: buildWalletMenu() });
    return;
  }
  try {
    const snapshot = await fetchWalletSnapshot(wallet);
    watch.baselineSetAt = Date.now();
    watch.baselineKeys = snapshot.positions.map((row) => row.key);
    watch.lastSeenKeys = snapshot.positions.map((row) => row.key);
    watch.lastSnapshotAt = Date.now();
    watch.lastCheckAt = Date.now();
    watch.lastOpenCount = snapshot.positions.length;
    watch.firstTradeAt = snapshot.firstTradeAt ? Number(snapshot.firstTradeAt || 0) : watch.firstTradeAt || 0;
    watch.lastTradeAt = snapshot.lastTradeAt ? Number(snapshot.lastTradeAt || 0) : watch.lastTradeAt || 0;
    watch.recentTradeCount = snapshot.recentTradeCount ? Number(snapshot.recentTradeCount || 0) : watch.recentTradeCount || 0;
    watch.recentTradeVolumeUsd =
      snapshot.recentTradeVolumeUsd ? Number(snapshot.recentTradeVolumeUsd || 0) : watch.recentTradeVolumeUsd || 0;
    watch.lastEventAt = watch.baselineSetAt;
    setAlertPref(watch, { enabled: false, types: [] });
    watch.lastError = "";
    watch.updatedAt = Date.now();
    state.chats[String(chatId)] = chatState;
    writeState(STATE_FILE, state);
    await sendMessage(
      botToken,
      chatId,
      [
        `Watching wallet: ${wallet}`,
        label ? `Label: ${label}` : "",
        "",
        `Baseline set with ${snapshot.positions.length} open positions.`,
        "Only new changes after now will trigger alerts.",
        "",
        "Do you want alerts for this wallet?",
      ]
        .filter(Boolean)
        .join("\n"),
      { replyMarkup: buildAlertSetupMenu(wallet) }
    );
  } catch (error) {
    watch.lastError = String(error && error.message ? error.message : error);
    watch.updatedAt = Date.now();
    state.chats[String(chatId)] = chatState;
    writeState(STATE_FILE, state);
    await sendMessage(
      botToken,
      chatId,
      `Wallet saved, but I could not fetch the initial snapshot.\n${watch.lastError}`,
      { replyMarkup: buildWalletMenu() }
    );
  }
}

async function removeWalletWatchCommand(botToken, chatId, project, state, chatState, walletText) {
  const wallet = cleanText(walletText);
  if (!wallet) {
    chatState.mode = "unwatch_wallet";
    chatState.lastPrompt = "Send the wallet address you want to stop watching.";
    writeState(STATE_FILE, state);
    await sendMessage(botToken, chatId, chatState.lastPrompt, { replyMarkup: buildWalletMenu() });
    return;
  }
  const removed = removeWatchedWallet(chatState, wallet);
  if (removed && String(chatState.pendingWallet || "") === wallet) {
    chatState.pendingWallet = "";
  }
  state.chats[String(chatId)] = chatState;
  writeState(STATE_FILE, state);
  await sendMessage(
    botToken,
    chatId,
    removed ? `Stopped watching ${wallet}.` : `That wallet was not in your watch list.`,
    { replyMarkup: buildWalletMenu() }
  );
}

async function sendWatchedWallets(botToken, chatId, chatState) {
  const watches = ensureWatchedWallets(chatState);
  if (!watches.length) {
    await sendMessage(
      botToken,
      chatId,
      [
        "No wallets are being tracked yet.",
        "",
        "Add one wallet to start tracking open positions and alerts.",
      ].join("\n"),
      { replyMarkup: buildWalletMenu() }
    );
    return;
  }
  const lines = watches.map((watch, index) => {
    const displayLabel = getWatchedWalletDisplayLabel(watch) || watch.wallet;
    const alertPref = getAlertPref(watch);
    const alertState = alertPref.enabled ? `alerts ${getAlertTypesLabel(alertPref.types)}` : "alerts off";
    const lastSeen = watch.lastCheckAt ? new Date(watch.lastCheckAt).toISOString().replace("T", " ").replace(".000Z", " UTC") : "-";
    const lastTrade = watch.lastTradeAt ? formatTimestamp(watch.lastTradeAt) : "-";
    return [
      `${index + 1}. ${displayLabel}`,
      `   ${formatWalletId(watch.wallet)} • open ${watch.lastOpenCount || 0} • ${alertState}`,
      `   last trade ${lastTrade} • last check ${lastSeen}`,
    ].join("\n");
  });
  await sendMessage(
    botToken,
    chatId,
    [
      "Watched wallets",
      "",
      "Select a wallet to view status, alerts, or edit its label.",
      "",
      ...lines,
    ].join("\n"),
    { replyMarkup: buildWalletPickerMenu(watches) }
  );
}

async function scanWatchedWallets(botToken, project, state) {
  if (watchScanInFlight) return;
  watchScanInFlight = true;
  try {
    const chats = Object.values(state.chats || {});
    for (const chatState of chats) {
      if (!chatState || typeof chatState !== "object") continue;
      const chatId = chatState.chatId;
      if (!chatId || !isAllowedChat(chatId)) continue;
      const watches = ensureWatchedWallets(chatState);
      if (!watches.length) continue;
      for (const watch of watches) {
        if (!watch || watch.muted) continue;
        try {
          const [snapshotResult, eventFeedResult] = await Promise.allSettled([
            fetchWalletSnapshot(watch.wallet),
            fetchWalletEventFeed(watch.wallet),
          ]);
          const snapshot = snapshotResult.status === "fulfilled" ? snapshotResult.value : null;
          const eventFeed = eventFeedResult.status === "fulfilled" ? eventFeedResult.value : null;
          const current = snapshot && Array.isArray(snapshot.positions) ? snapshot.positions : Array.isArray(watch.lastSeenPositions) ? watch.lastSeenPositions : [];
          const previous = Array.isArray(watch.lastSeenPositions) ? watch.lastSeenPositions : [];
          const currentKeys = current.map((row) => row.key);
          if (!watch.baselineSetAt || !previous.length) {
            watch.baselineSetAt = watch.baselineSetAt || Date.now();
            watch.baselineKeys = currentKeys;
            watch.lastSeenPositions = current;
            watch.lastSeenKeys = currentKeys;
            watch.lastSnapshotAt = Date.now();
            watch.lastCheckAt = Date.now();
            watch.lastOpenCount = current.length;
            if (snapshot) {
              watch.firstTradeAt = snapshot.firstTradeAt ? Number(snapshot.firstTradeAt || 0) : watch.firstTradeAt || 0;
              watch.lastTradeAt = snapshot.lastTradeAt ? Number(snapshot.lastTradeAt || 0) : watch.lastTradeAt || 0;
              watch.recentTradeCount = snapshot.recentTradeCount ? Number(snapshot.recentTradeCount || 0) : watch.recentTradeCount || 0;
              watch.recentTradeVolumeUsd =
                snapshot.recentTradeVolumeUsd ? Number(snapshot.recentTradeVolumeUsd || 0) : watch.recentTradeVolumeUsd || 0;
            }
            watch.lastError = "";
            continue;
          }
          const feedEvents = eventFeed
            ? extractAlertEventsFromPayload(eventFeed, Number(watch.lastEventAt || watch.baselineSetAt || 0))
            : [];
          const diffEvents = diffPositions(previous, current);
          const mergedEvents = [];
          const seenKeys = new Set();
          for (const event of [...feedEvents, ...diffEvents]) {
            const type = String(event && event.type ? event.type : "").trim();
            const key = `${type}|${String(event && event.key ? event.key : normalizeWalletKey(event && event.row ? event.row : {}))}`;
            if (!type || seenKeys.has(key)) continue;
            seenKeys.add(key);
            mergedEvents.push(event);
          }
          mergedEvents.sort((a, b) => Number(a.eventTs || 0) - Number(b.eventTs || 0));
          const notifyEvents = mergedEvents.filter((item) => shouldNotifyAlertEvent(watch, item.type || item.eventType));
          if (notifyEvents.length) {
            const activeMessages = notifyEvents
              .slice(0, 10)
              .map((item, index) =>
                renderPositionEvent(
                  {
                    type: item.type || (item.eventType === "position_opened" ? "opened" : "closed"),
                    delta: item.sizeDelta || item.delta || null,
                    previous: item.previous || null,
                    row: item.row,
                  },
                  watch,
                  index + 1
                )
              );
            const watchLabel = watch.label ? `${watch.label} (${watch.wallet})` : watch.wallet;
            await sendMessage(
              BOT_TOKEN,
              chatId,
              [`Wallet update: ${watchLabel}`, "", ...activeMessages].join("\n"),
              { replyMarkup: buildWalletMenu() }
            );
            const maxEventTs = notifyEvents.reduce((max, item) => Math.max(max, Number(item.eventTs || 0)), Number(watch.lastEventAt || 0));
            watch.lastEventAt = maxEventTs;
            chatState.lastAlertAt = Date.now();
          } else if (mergedEvents.length) {
            const maxEventTs = mergedEvents.reduce((max, item) => Math.max(max, Number(item.eventTs || 0)), Number(watch.lastEventAt || 0));
            watch.lastEventAt = maxEventTs;
          }
          if (snapshot) {
            watch.firstTradeAt = snapshot.firstTradeAt ? Number(snapshot.firstTradeAt || 0) : watch.firstTradeAt || 0;
            watch.lastTradeAt = snapshot.lastTradeAt ? Number(snapshot.lastTradeAt || 0) : watch.lastTradeAt || 0;
            watch.recentTradeCount = snapshot.recentTradeCount ? Number(snapshot.recentTradeCount || 0) : watch.recentTradeCount || 0;
            watch.recentTradeVolumeUsd =
              snapshot.recentTradeVolumeUsd ? Number(snapshot.recentTradeVolumeUsd || 0) : watch.recentTradeVolumeUsd || 0;
          }
          watch.lastError = "";
          watch.lastSeenPositions = current;
          watch.lastSeenKeys = currentKeys;
          watch.lastSnapshotAt = Date.now();
          watch.lastCheckAt = Date.now();
          watch.lastOpenCount = current.length;
          watch.updatedAt = Date.now();
        } catch (error) {
          watch.lastError = String(error && error.message ? error.message : error);
          watch.lastCheckAt = Date.now();
          watch.updatedAt = Date.now();
        }
      }
      state.chats[String(chatId)] = chatState;
    }
    writeState(STATE_FILE, state);
  } finally {
    watchScanInFlight = false;
  }
}

async function sendMainMenu(botToken, chatId, project, chatState) {
  const botName = getBotDisplayName(project);
  const watchCount = Array.isArray(chatState && chatState.watchedWallets) ? chatState.watchedWallets.length : 0;
  const summary = [
      `📌 ${botName}`,
      "",
      "Track wallets, inspect wallet status, and receive live position alerts.",
      "",
      watchCount > 0
      ? `Tracked wallets: ${watchCount}. Tap Wallet Status to choose one.`
      : "No wallets are being tracked yet. Add one to browse the default wallet lists.",
  ].join("\n");
  await sendMessage(botToken, chatId, summary, {
    replyMarkup: buildWalletMenu(),
  });
}

async function sendHelp(botToken, chatId, project) {
  const botName = getBotDisplayName(project);
  const text = [
    `ℹ️ ${botName} help`,
    "",
    "What this bot does:",
    "• Browse the default wallet lists and add a wallet",
    "• Remove a wallet",
    "• Set or edit a wallet label",
    "• Pick a tracked wallet from the Wallet Status menu",
    "• Show open positions, PnL, and volume",
    "• Configure alerts for each wallet",
    "• Alert on new opens, closes, increases, and reductions",
    "",
    "How to use it:",
    "1. Tap Add wallet to browse the default wallet lists.",
    "2. Pick a wallet to auto-add it to tracking.",
    "3. Use Wallet status to choose one of your saved wallets.",
  ].join("\n");
  await sendMessage(botToken, chatId, text, { replyMarkup: buildWalletMenu() });
}

async function handleCommand(botToken, chatId, project, state, chatState, command, argsText) {
  const content = cleanText(argsText);
  switch (command) {
    case "/start":
    case "/menu":
      await sendMainMenu(botToken, chatId, project, chatState);
      return true;
    case "/help":
      await sendHelp(botToken, chatId, project);
      return true;
    case "/watch":
    case "/add":
      if (!content) {
        await sendWalletCatalogHome(botToken, chatId, project, state, chatState);
      } else {
        await addWalletWatch(botToken, chatId, project, state, chatState, content);
      }
      return true;
    case "/unwatch":
    case "/remove":
      await removeWalletWatchCommand(botToken, chatId, project, state, chatState, content);
      return true;
    case "/watches":
    case "/wallets":
      await sendWatchedWallets(botToken, chatId, chatState);
      return true;
    case "/alerts":
      {
        const watches = ensureWatchedWallets(chatState);
        if (!watches.length) {
          await sendMessage(
            botToken,
            chatId,
            [
              "No wallets are being tracked yet.",
              "",
              "Add a wallet first to configure alerts.",
            ].join("\n"),
            { replyMarkup: buildWalletMenu() }
          );
        } else {
          await sendMessage(
            botToken,
            chatId,
            [
              "Alerts",
              "",
              "Choose a tracked wallet to edit its alert preferences.",
            ].join("\n"),
            { replyMarkup: buildAlertsMenu(watches) }
          );
        }
      }
      return true;
    case "/lists":
      await sendWalletCatalogHome(botToken, chatId, project, state, chatState);
      return true;
    case "/wallet":
    case "/status":
      await sendWalletStatus(botToken, chatId, project, chatState, content);
      return true;
    case "/label":
      if (!content) {
        chatState.mode = "label_wallet";
        chatState.lastPrompt = "Send the wallet address and the new label.";
        state.chats[String(chatId)] = chatState;
        writeState(STATE_FILE, state);
        await sendMessage(botToken, chatId, chatState.lastPrompt, { replyMarkup: buildWalletMenu() });
        return true;
      }
      {
        const [wallet, ...labelParts] = content.split(/\s+/);
        const label = labelParts.join(" ").trim();
        const watch = findWatchedWallet(chatState, wallet);
        if (!watch) {
          await sendMessage(botToken, chatId, "That wallet is not being watched yet. Add it first.", { replyMarkup: buildWalletMenu() });
          return true;
        }
        watch.label = label || watch.label || "";
        watch.updatedAt = Date.now();
        state.chats[String(chatId)] = chatState;
        writeState(STATE_FILE, state);
        await sendMessage(botToken, chatId, `Label updated for ${wallet}.`, { replyMarkup: buildWalletMenu() });
        return true;
      }
    default:
      return false;
  }
}

function mapCallbackToIntent(data) {
  const normalized = String(data || "").trim();
  if (normalized.startsWith("wallet_list_home:")) {
    return { type: "wallet_catalog_home", listKey: normalized.slice("wallet_list_home:".length).trim() };
  }
  if (normalized.startsWith("wallet_list_page:")) {
    const parts = normalized.split(":");
    return {
      type: "wallet_catalog_page",
      listKey: parts[1] || "trades",
      page: Number(parts[2] || 1) || 1,
    };
  }
  if (normalized.startsWith("wallet_pick:")) {
    const parts = normalized.split(":");
    return {
      type: "wallet_catalog_pick",
      listKey: parts[1] || "trades",
      page: Number(parts[2] || 1) || 1,
      index: Number(parts[3] || 0) || 0,
    };
  }
  if (normalized.startsWith("wallet_status:")) {
    return { type: "wallet_status_wallet", wallet: normalized.slice("wallet_status:".length).trim() };
  }
  if (normalized.startsWith("wallet_label:")) {
    return { type: "wallet_label_wallet", wallet: normalized.slice("wallet_label:".length).trim() };
  }
  if (normalized.startsWith("wallet_remove:")) {
    return { type: "wallet_remove_wallet", wallet: normalized.slice("wallet_remove:".length).trim() };
  }
  if (normalized.startsWith("wallet_alerts:")) {
    return { type: "wallet_alerts_wallet", wallet: normalized.slice("wallet_alerts:".length).trim() };
  }
  if (normalized.startsWith("wallet_alert_stop:")) {
    return { type: "wallet_alert_stop", wallet: normalized.slice("wallet_alert_stop:".length).trim() };
  }
  if (normalized.startsWith("alert_setup_yes:")) {
    return { type: "alert_setup_yes", wallet: normalized.slice("alert_setup_yes:".length).trim() };
  }
  if (normalized.startsWith("alert_setup_no:")) {
    return { type: "alert_setup_no", wallet: normalized.slice("alert_setup_no:".length).trim() };
  }
  if (normalized.startsWith("alert_type:")) {
    const parts = normalized.split(":");
    return {
      type: "alert_type_toggle",
      wallet: parts[1] || "",
      alertType: parts.slice(2).join(":").trim(),
    };
  }
  if (normalized.startsWith("alert_save:")) {
    return { type: "alert_save", wallet: normalized.slice("alert_save:".length).trim() };
  }
  switch (normalized) {
    case "menu:main_menu":
    case "menu:home":
      return { type: "main_menu" };
    case "menu:add_wallet":
      return { type: "wallet_catalog_home" };
    case "menu:watch_list":
      return { type: "watch_list" };
    case "menu:wallet_status":
      return { type: "wallet_status" };
    case "menu:label_wallet":
      return { type: "label_wallet" };
    case "menu:remove_wallet":
      return { type: "unwatch_wallet" };
    case "menu:help":
      return { type: "help" };
    case "menu:alerts":
      return { type: "alerts" };
    case "menu:wallet_lists":
      return { type: "wallet_catalog_home" };
    default:
      return { type: "unknown" };
  }
}

async function handleCallback(botToken, project, state, callbackQuery) {
  const callbackId = callbackQuery && callbackQuery.id;
  const data = callbackQuery && callbackQuery.data ? String(callbackQuery.data) : "";
  const message = callbackQuery && callbackQuery.message ? callbackQuery.message : null;
  const chat = message && message.chat ? message.chat : null;
  const chatId = chat ? chat.id : null;
  const chatState = ensureChatState(state, chatId, project.projectKey);
  const mapped = mapCallbackToIntent(data);
  if (callbackId) {
    await answerCallback(botToken, callbackId, "Working…").catch(() => {});
  }
  if (!chatId) return;
  if (mapped.type === "main_menu") {
    await sendMainMenu(botToken, chatId, project, chatState);
  } else if (mapped.type === "wallet_catalog_home") {
    if (mapped.listKey) {
      await sendWalletCatalogPage(botToken, chatId, project, state, chatState, mapped.listKey, 1);
    } else {
      await sendWalletCatalogHome(botToken, chatId, project, state, chatState);
    }
  } else if (mapped.type === "wallet_catalog_page") {
    await sendWalletCatalogPage(botToken, chatId, project, state, chatState, mapped.listKey, mapped.page);
  } else if (mapped.type === "wallet_catalog_pick") {
    await handleWalletCatalogSelection(botToken, chatId, project, state, chatState, mapped.listKey, mapped.page, mapped.index);
  } else if (mapped.type === "watch_list") {
    await sendWatchedWallets(botToken, chatId, chatState);
  } else if (mapped.type === "wallet_status") {
    await sendWalletStatus(botToken, chatId, project, chatState, "");
  } else if (mapped.type === "wallet_status_wallet") {
    await sendWalletStatus(botToken, chatId, project, chatState, mapped.wallet);
  } else if (mapped.type === "alerts") {
    const watches = ensureWatchedWallets(chatState);
    if (!watches.length) {
      await sendMessage(
        botToken,
        chatId,
        [
          "No wallets are being tracked yet.",
          "",
          "Add a wallet first to configure alerts.",
        ].join("\n"),
        { replyMarkup: buildWalletMenu() }
      );
    } else {
      await sendMessage(
        botToken,
        chatId,
        [
          "Alerts",
          "",
          "Choose a tracked wallet to edit its alert preferences.",
        ].join("\n"),
        { replyMarkup: buildAlertsMenu(watches) }
      );
    }
  } else if (mapped.type === "wallet_alerts_wallet") {
    const watch = findWatchedWallet(chatState, mapped.wallet);
    if (!watch) {
      await sendMessage(botToken, chatId, "That wallet is not being watched yet. Add it first.", { replyMarkup: buildWalletMenu() });
    } else {
      const prefs = getAlertPref(watch);
      const walletLabel = getWatchedWalletDisplayLabel(watch);
      await sendMessage(
        botToken,
        chatId,
        [
          `🚨 Alerts for ${walletLabel}`,
          "",
          prefs.enabled ? `Status: On (${getAlertTypesLabel(prefs.types)})` : "Status: Off",
          "",
          "Tap the buttons below to edit this wallet's alert preferences.",
        ].join("\n"),
        { replyMarkup: buildAlertTypeMenu(watch.wallet, prefs.types) }
      );
    }
  } else if (mapped.type === "wallet_alert_stop") {
    const watch = findWatchedWallet(chatState, mapped.wallet);
    if (!watch) {
      await sendMessage(botToken, chatId, "That wallet is not being watched yet. Add it first.", { replyMarkup: buildWalletMenu() });
    } else {
      setAlertPref(watch, { enabled: false, types: [] });
      watch.updatedAt = Date.now();
      state.chats[String(chatId)] = chatState;
      writeState(STATE_FILE, state);
      await sendMessage(
        botToken,
        chatId,
        [
          `⏹️ Alerts stopped for ${getWatchedWalletDisplayLabel(watch)}`,
          "",
          "You can turn them back on later from the Alerts menu.",
        ].join("\n"),
        { replyMarkup: buildWalletDetailMenu(watch.wallet) }
      );
    }
  } else if (mapped.type === "alert_setup_yes") {
    const watch = findWatchedWallet(chatState, mapped.wallet);
    if (!watch) {
      await sendMessage(botToken, chatId, "That wallet is not being watched yet. Add it first.", { replyMarkup: buildWalletMenu() });
    } else {
      watch.pendingAlertSetup = true;
      watch.alertPreferences = watch.alertPreferences || { enabled: false, types: [] };
      watch.alertPreferences.enabled = true;
      watch.alertPreferences.types = ["opened", "closed", "increased", "reduced"];
      watch.updatedAt = Date.now();
      state.chats[String(chatId)] = chatState;
      writeState(STATE_FILE, state);
      await sendMessage(
        botToken,
        chatId,
        [
          `✅ Alerts enabled for ${getWatchedWalletDisplayLabel(watch)}`,
          "",
          "Which alerts do you want to receive?",
        ].join("\n"),
        { replyMarkup: buildAlertTypeMenu(watch.wallet, watch.alertPreferences.types) }
      );
    }
  } else if (mapped.type === "alert_setup_no") {
    const watch = findWatchedWallet(chatState, mapped.wallet);
    if (!watch) {
      await sendMessage(botToken, chatId, "That wallet is not being watched yet. Add it first.", { replyMarkup: buildWalletMenu() });
    } else {
      setAlertPref(watch, { enabled: false, types: [] });
      watch.updatedAt = Date.now();
      state.chats[String(chatId)] = chatState;
      writeState(STATE_FILE, state);
      await sendMessage(
        botToken,
        chatId,
        `Alerts remain off for ${getWatchedWalletDisplayLabel(watch)}.`,
        { replyMarkup: buildWalletDetailMenu(watch.wallet) }
      );
    }
  } else if (mapped.type === "alert_type_toggle") {
    const watch = findWatchedWallet(chatState, mapped.wallet);
    if (!watch) {
      await sendMessage(botToken, chatId, "That wallet is not being watched yet. Add it first.", { replyMarkup: buildWalletMenu() });
    } else {
      const current = getAlertPref(watch);
      const key = String(mapped.alertType || "").trim().toLowerCase();
      const nextTypes = new Set(current.types);
      if (nextTypes.has(key)) nextTypes.delete(key);
      else nextTypes.add(key);
      setAlertPref(watch, { enabled: nextTypes.size > 0, types: Array.from(nextTypes) });
      state.chats[String(chatId)] = chatState;
      writeState(STATE_FILE, state);
      await sendMessage(
        botToken,
        chatId,
        [
          `🚨 Alerts for ${getWatchedWalletDisplayLabel(watch)}`,
          "",
          watch.alertPreferences && watch.alertPreferences.enabled
            ? `Status: On (${getAlertTypesLabel(watch.alertPreferences.types)})`
            : "Status: Off",
          "",
          "Tap the buttons below to edit this wallet's alert preferences.",
        ].join("\n"),
        { replyMarkup: buildAlertTypeMenu(watch.wallet, watch.alertPreferences ? watch.alertPreferences.types : []) }
      );
    }
  } else if (mapped.type === "alert_save") {
    const watch = findWatchedWallet(chatState, mapped.wallet);
    if (!watch) {
      await sendMessage(botToken, chatId, "That wallet is not being watched yet. Add it first.", { replyMarkup: buildWalletMenu() });
    } else {
      state.chats[String(chatId)] = chatState;
      writeState(STATE_FILE, state);
      await sendMessage(
        botToken,
        chatId,
        [
          `Saved alerts for ${getWatchedWalletDisplayLabel(watch)}`,
          "",
          watch.alertPreferences && watch.alertPreferences.enabled
            ? `Active alerts: ${getAlertTypesLabel(watch.alertPreferences.types)}`
            : "Alerts are off for this wallet.",
        ].join("\n"),
        { replyMarkup: buildWalletDetailMenu(watch.wallet) }
      );
    }
  } else if (mapped.type === "help") {
    await sendHelp(botToken, chatId, project);
  } else if (mapped.type === "watch_wallet") {
    chatState.mode = "watch_wallet";
    chatState.lastPrompt = "Send the wallet address you want to watch. You can optionally add a label after the wallet.";
    state.chats[String(chatId)] = chatState;
    writeState(STATE_FILE, state);
    await sendMessage(botToken, chatId, chatState.lastPrompt, { replyMarkup: buildWalletMenu() });
  } else if (mapped.type === "unwatch_wallet") {
    chatState.mode = "unwatch_wallet";
    chatState.lastPrompt = "Send the wallet address you want to remove.";
    state.chats[String(chatId)] = chatState;
    writeState(STATE_FILE, state);
    await sendMessage(botToken, chatId, chatState.lastPrompt, { replyMarkup: buildWalletMenu() });
  } else if (mapped.type === "label_wallet") {
    chatState.mode = "label_wallet";
    chatState.lastPrompt = "Send the wallet address and the new label.";
    state.chats[String(chatId)] = chatState;
    writeState(STATE_FILE, state);
    await sendMessage(botToken, chatId, chatState.lastPrompt, { replyMarkup: buildWalletMenu() });
  } else if (mapped.type === "wallet_label_wallet") {
    chatState.mode = "label_wallet";
    chatState.pendingWallet = mapped.wallet;
    chatState.lastPrompt = "Send the new label for this wallet.";
    state.chats[String(chatId)] = chatState;
    writeState(STATE_FILE, state);
    await sendMessage(botToken, chatId, `Send the label for ${mapped.wallet}.`, { replyMarkup: buildWalletMenu() });
  } else if (mapped.type === "wallet_remove_wallet") {
    const removed = removeWatchedWallet(chatState, mapped.wallet);
    state.chats[String(chatId)] = chatState;
    writeState(STATE_FILE, state);
    await sendMessage(
      botToken,
      chatId,
      removed ? `Stopped watching ${mapped.wallet}.` : `That wallet was not in your watch list.`,
      { replyMarkup: buildWalletMenu() }
    );
  } else if (mapped.type !== "unknown") {
    await sendHelp(botToken, chatId, project);
  }
}

async function handleMessage(botToken, project, state, message) {
  const chat = message && message.chat ? message.chat : null;
  const chatId = chat ? chat.id : null;
  const text = cleanText(message && message.text);
  if (!chatId || !isAllowedChat(chatId)) return;
  const chatState = ensureChatState(state, chatId, project.projectKey);
  if (!text) return;
  const lower = text.toLowerCase();
  if (lower.startsWith("/")) {
    const command = lower.split(/\s+/)[0];
    const argsText = text.slice(command.length).trim();
    const handled = await handleCommand(botToken, chatId, project, state, chatState, command, argsText);
    if (handled) {
      writeState(STATE_FILE, state);
      return;
    }
  }
  const currentMode = String(chatState.mode || "idle");
  const effectiveMode = currentMode !== "idle" ? currentMode : resolveModeFromText(text);
  if (effectiveMode && effectiveMode !== "general") {
    if (effectiveMode === "help") {
      await sendHelp(botToken, chatId, project);
      return;
    }
    if (effectiveMode === "wallet_catalog_home") {
      await sendWalletCatalogHome(botToken, chatId, project, state, chatState);
      return;
    }
    if (effectiveMode === "wallet_catalog_page") {
      await sendWalletCatalogPage(botToken, chatId, project, state, chatState, "trades", 1);
      return;
    }
    if (effectiveMode === "watch_wallet") {
      chatState.mode = "watch_wallet";
      await addWalletWatch(botToken, chatId, project, state, chatState, text);
      return;
    }
    if (effectiveMode === "unwatch_wallet") {
      chatState.mode = "unwatch_wallet";
      await removeWalletWatchCommand(botToken, chatId, project, state, chatState, text);
      return;
    }
    if (effectiveMode === "watch_list") {
      await sendWatchedWallets(botToken, chatId, chatState);
      return;
    }
    if (effectiveMode === "alerts") {
      await handleCallback(botToken, project, state, {
        id: null,
        data: "menu:alerts",
        message: { chat: { id: chatId } },
      });
      return;
    }
    if (effectiveMode === "wallet_status") {
      chatState.mode = "wallet_status";
      await sendWalletStatus(botToken, chatId, project, chatState, text);
      return;
    }
    if (effectiveMode === "label_wallet") {
      chatState.mode = "label_wallet";
      const input = cleanText(text);
      const [wallet, ...labelParts] = input.split(/\s+/);
      const label = labelParts.join(" ").trim();
      const targetWallet = watchPendingWallet(chatState) || wallet;
      if (!targetWallet || !label) {
        chatState.lastPrompt = targetWallet
          ? "Send the new label."
          : "Send the wallet address and the new label.";
        state.chats[String(chatId)] = chatState;
        writeState(STATE_FILE, state);
        await sendMessage(botToken, chatId, chatState.lastPrompt, { replyMarkup: buildWalletMenu() });
        return;
      }
      const watch = findWatchedWallet(chatState, targetWallet);
      if (!watch) {
        await sendMessage(botToken, chatId, "That wallet is not being watched yet. Add it first.", { replyMarkup: buildWalletMenu() });
        return;
      }
      watch.label = label;
      watch.updatedAt = Date.now();
      chatState.pendingWallet = "";
      state.chats[String(chatId)] = chatState;
      writeState(STATE_FILE, state);
      await sendMessage(botToken, chatId, `Label updated for ${targetWallet}.`, { replyMarkup: buildWalletMenu() });
      return;
    }
  }
  await sendHelp(botToken, chatId, project);
}

async function pollLoop(botToken, project, state) {
  let offset = Number(state.offset || 0) || 0;
  for (;;) {
    if (shutdownRequested) return;
    try {
      const updates = await telegramApi(
        botToken,
        "getUpdates",
        {
          offset,
          timeout: POLL_TIMEOUT_SEC,
          allowed_updates: ["message", "callback_query"],
        },
        (POLL_TIMEOUT_SEC + 15) * 1000
      );
      if (!Array.isArray(updates) || !updates.length) continue;
      for (const update of updates) {
        offset = Math.max(offset, Number(update.update_id || 0) + 1);
        state.offset = offset;
        state.updatedAt = Date.now();
        writeState(STATE_FILE, state);
        if (update && update.message) {
          await handleMessage(botToken, project, state, update.message);
        } else if (update && update.callback_query) {
          await handleCallback(botToken, project, state, update.callback_query);
        }
      }
    } catch (error) {
      console.error(`[telegram-copilot] ${error && error.stack ? error.stack : error}`);
      await sleep(3000);
    }
  }
}

async function dryRunOnce(project, state) {
  const chatState = ensureChatState(state, 0, project.projectKey);
  console.log(`📌 ${getBotDisplayName(project)}`);
  console.log("");
  console.log(`Watched wallets: ${Array.isArray(chatState.watchedWallets) ? chatState.watchedWallets.length : 0}`);
}

async function main() {
  const project = loadProjectConfig(ROOT, CONFIG_DIR, PROJECT_KEY);
  ensureStateDir();
  const state = readState(STATE_FILE, project.projectKey);
  if (DRY_RUN) {
    await dryRunOnce(project, state);
    return;
  }
  if (!BOT_ENABLED) {
    console.log("[telegram-copilot] disabled via PACIFICA_TELEGRAM_COPILOT_ENABLED");
    return;
  }
  if (!BOT_TOKEN) {
    console.log("[telegram-copilot] token missing; set PACIFICA_TELEGRAM_COPILOT_BOT_TOKEN");
    return;
  }
  await setMyCommands(BOT_TOKEN, [
    { command: "start", description: "Open the main menu" },
    { command: "watch", description: "Open wallet lists or track a wallet" },
    { command: "lists", description: "Open the default wallet lists" },
    { command: "wallet", description: "Show wallet status" },
    { command: "alerts", description: "Manage wallet alerts" },
    { command: "label", description: "Set a wallet label" },
    { command: "unwatch", description: "Stop tracking a wallet" },
    { command: "watches", description: "List watched wallets" },
    { command: "help", description: "Explain the bot" },
  ]).catch((error) => {
    console.warn(`[telegram-copilot] setMyCommands failed: ${error && error.message ? error.message : error}`);
  });
  console.log(`[telegram-copilot] started project=${project.projectKey} mode=wallet-tracker`);
  watchScanTimer = setInterval(() => {
    scanWatchedWallets(BOT_TOKEN, project, state).catch((error) => {
      console.error(`[telegram-copilot] watch scan failed: ${error && error.stack ? error.stack : error}`);
    });
  }, WATCH_POLL_MS);
  if (watchScanTimer && typeof watchScanTimer.unref === "function") {
    watchScanTimer.unref();
  }
  const stop = () => {
    shutdownRequested = true;
    if (watchScanTimer) {
      clearInterval(watchScanTimer);
      watchScanTimer = null;
    }
  };
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
  await pollLoop(BOT_TOKEN, project, state);
}

main().catch((error) => {
  console.error(error && error.stack ? error.stack : error);
  process.exitCode = 1;
});
