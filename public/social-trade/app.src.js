const state = {
  wallet: "",
  timeframe: "all",
  loading: false,
  profile: null,
  error: "",
};
const SESSION_CACHE_KEYS = {
  profile: "pf.socialCart.profile.v1",
  exchange: "pf.exchange.v2",
  exchangeSeries: "pf.exchangeSeries.v3",
  wallets: "pf.wallets.v2",
  liveTradeSummary: "pf.liveTrade.summary.v1",
};

function el(id) {
  return document.getElementById(id);
}

function readSessionJson(key) {
  if (typeof window === "undefined" || !window.sessionStorage || !key) return null;
  try {
    const raw = window.sessionStorage.getItem(key);
    if (!raw) return null;
    return JSON.parse(raw);
  } catch (_error) {
    return null;
  }
}

function writeSessionJson(key, value) {
  if (typeof window === "undefined" || !window.sessionStorage || !key) return;
  try {
    if (value === null || value === undefined) {
      window.sessionStorage.removeItem(key);
      return;
    }
    window.sessionStorage.setItem(key, JSON.stringify(value));
  } catch (_error) {
    // ignore storage failures
  }
}

function scheduleIdleTask(task, timeout = 2000) {
  if (typeof window === "undefined" || typeof task !== "function") return;
  if (typeof window.requestIdleCallback === "function") {
    window.requestIdleCallback(() => task(), { timeout });
    return;
  }
  window.setTimeout(task, Math.max(0, Math.min(timeout, 1200)));
}

function afterWindowLoad(task, delayMs = 0) {
  if (typeof window === "undefined" || typeof task !== "function") return;
  const run = () => window.setTimeout(task, Math.max(0, Number(delayMs || 0)));
  if (document.readyState === "complete") {
    run();
    return;
  }
  window.addEventListener("load", run, { once: true });
}

function isConstrainedNetwork() {
  if (typeof navigator === "undefined") return false;
  const connection = navigator.connection || navigator.mozConnection || navigator.webkitConnection;
  if (!connection) return false;
  if (connection.saveData) return true;
  const effectiveType = String(connection.effectiveType || "").toLowerCase();
  return effectiveType === "slow-2g" || effectiveType === "2g";
}

function toNum(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function escapeHtml(value) {
  return String(value === null || value === undefined ? "" : value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function shortWallet(wallet = "") {
  const value = String(wallet || "").trim();
  if (value.length <= 12) return value;
  return `${value.slice(0, 6)}...${value.slice(-4)}`;
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
  return `${toNum(value, 0).toFixed(1)}%`;
}

function formatDate(value) {
  const ts = Number(value || 0);
  if (!Number.isFinite(ts) || ts <= 0) return "-";
  return new Date(ts).toISOString().slice(0, 10);
}

function buildShareLink(profile = state.profile) {
  if (profile && profile.shareLink) return String(profile.shareLink);
  const params = new URLSearchParams();
  if (state.wallet) params.set("wallet", state.wallet);
  params.set("timeframe", state.timeframe || "all");
  return `${window.location.origin}/social-cart?${params.toString()}`;
}

function buildTweetText(profile = state.profile) {
  if (!profile) return "";
  return `${profile.shareText}\n\nWallet: ${shortWallet(profile.wallet)}`;
}

async function fetchJson(url, options = {}) {
  const timeoutMs = Number(options.timeoutMs ?? 15000);
  const retries = Math.max(0, Math.min(2, Number(options.retries ?? 0)));
  let lastError = null;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const controller = typeof AbortController === "function" ? new AbortController() : null;
    const timeoutId =
      controller && timeoutMs > 0
        ? window.setTimeout(() => {
            try {
              controller.abort(
                new DOMException("Social Cart request timed out.", "AbortError")
              );
            } catch (_error) {
              try {
                controller.abort();
              } catch (_error2) {}
            }
          }, timeoutMs)
        : null;
    let response;
    try {
      response = await fetch(url, {
        headers: {
          Accept: "application/json",
        },
        signal: controller ? controller.signal : undefined,
      });
    } catch (error) {
      lastError = error;
      const aborted =
        error &&
        (error.name === "AbortError" ||
          String(error.message || "").toLowerCase().includes("aborted"));
      if (timeoutId) window.clearTimeout(timeoutId);
      if (aborted && attempt < retries) continue;
      if (aborted) {
        throw new Error("Social Cart request timed out. Please try again.");
      }
      throw error;
    } finally {
      if (timeoutId) window.clearTimeout(timeoutId);
    }
  let payload = null;
    try {
      payload = await response.json();
    } catch (_error) {
      payload = null;
    }
    if (!response.ok) {
      const message =
        payload && payload.error ? String(payload.error) : `Request failed (${response.status})`;
      throw new Error(message);
    }
    return payload;
  }
  throw lastError || new Error("social_cart_request_failed");
}

async function copyToClipboard(text) {
  const value = String(text || "");
  if (!value) return false;
  if (navigator.clipboard && typeof navigator.clipboard.writeText === "function") {
    await navigator.clipboard.writeText(value);
    return true;
  }
  const fallback = document.createElement("textarea");
  fallback.value = value;
  fallback.setAttribute("readonly", "readonly");
  fallback.style.position = "fixed";
  fallback.style.opacity = "0";
  document.body.appendChild(fallback);
  fallback.select();
  document.execCommand("copy");
  document.body.removeChild(fallback);
  return true;
}

function setStatus(text, isError = false) {
  const node = el("st-status-text");
  if (!node) return;
  node.textContent = text;
  node.classList.toggle("st-muted", !isError);
  node.style.color = isError ? "rgba(255, 188, 188, 0.96)" : "";
}

function syncTimeframeButtons() {
  document.querySelectorAll("[data-timeframe]").forEach((button) => {
    const active = String(button.getAttribute("data-timeframe") || "") === state.timeframe;
    button.classList.toggle("active", active);
    button.setAttribute("aria-pressed", active ? "true" : "false");
  });
}

function renderMetrics(profile) {
  const grid = el("st-metrics-grid");
  if (!grid) return;
  if (!profile || !profile.metrics) {
    grid.innerHTML = "";
    return;
  }
  const metrics = profile.metrics;
  const items = [
    {
      label: "Total Volume",
      value: formatMoneyCompact(metrics.totalVolume),
      note: `${String(profile.timeframe || "30d").toUpperCase()} window`,
      className: "st-metric-card",
    },
    {
      label: "Total Trades",
      value: formatCountCompact(metrics.totalTrades),
      note: `${String(profile.timeframe || "30d").toUpperCase()} window`,
      className: "st-metric-card",
    },
    {
      label: "Wallet PnL",
      value: formatMoneyCompact(metrics.walletPnl),
      note: "Realized + unrealized",
      className: "st-metric-card is-hero",
    },
    {
      label: "Realized PnL",
      value: formatMoneyCompact(metrics.realizedPnl),
      note: `${String(profile.timeframe || "30d").toUpperCase()} realized`,
      className: "st-metric-card is-hero-alt",
    },
    {
      label: "First Trade",
      value: formatDate(metrics.firstTradeAt),
      note: "All-time reference",
      className: "st-metric-card",
    },
    {
      label: "Win Rate",
      value: formatPercent(metrics.winRate),
      note: `${String(profile.timeframe || "30d").toUpperCase()} window`,
      className: "st-metric-card",
    },
    {
      label: "Volume Rank",
      value: metrics.volumeRank ? `#${formatCountCompact(metrics.volumeRank)}` : "-",
      note: "Eligible wallet universe",
      className: "st-metric-card",
    },
    {
      label: "PnL Rank",
      value: metrics.pnlRank ? `#${formatCountCompact(metrics.pnlRank)}` : "-",
      note: "Eligible wallet universe",
      className: "st-metric-card",
    },
    {
      label: "Open Positions",
      value: formatCountCompact(metrics.openPositions || 0),
      note: "Current live overlay",
      className: "st-metric-card",
    },
    {
      label: "Last Active",
      value: formatDate(metrics.lastActiveAt),
      note: "Latest observed activity",
      className: "st-metric-card",
    },
  ];
  grid.innerHTML = items
    .map(
      (item) => `
        <article class="${escapeHtml(item.className || "st-metric-card")}">
          <span>${escapeHtml(item.label)}</span>
          <strong>${escapeHtml(item.value)}</strong>
          <small>${escapeHtml(item.note)}</small>
        </article>
      `
    )
    .join("");
}

function renderScores(profile) {
  const container = el("st-score-list");
  if (!container) return;
  if (!profile || !profile.scores) {
    container.innerHTML = "";
    return;
  }
  const rows = Object.entries(profile.scores)
    .map(([key, score]) => ({
      key,
      label:
        key === "livermore"
          ? "The Livermore"
          : key === "buffett"
          ? "The Buffett"
          : key === "soros"
          ? "The Soros"
          : key === "druckenmiller"
          ? "The Druckenmiller"
          : key === "simons"
          ? "The Simons"
          : key === "dalio"
          ? "The Dalio"
          : key === "ackman"
          ? "The Ackman"
          : "The Gill",
      score: toNum(score, 0),
    }))
    .sort((left, right) => right.score - left.score);
  container.innerHTML = rows
    .map(
      (row) => `
        <div class="st-score-row${row.key === profile.archetypeKey ? " is-active" : ""}">
          <div class="st-score-row-head">
            <strong>${escapeHtml(row.label)}</strong>
            <span>${escapeHtml(row.score.toFixed(1))}</span>
          </div>
          <div class="st-score-bar"><span style="width:${Math.max(0, Math.min(100, row.score))}%"></span></div>
        </div>
      `
    )
    .join("");
}

function renderCard(profile) {
  const img = el("st-card-image");
  const placeholder = el("st-card-placeholder");
  if (!img || !placeholder) return;
  if (!profile || !profile.cardImageUrl) {
    img.removeAttribute("src");
    img.style.display = "none";
    placeholder.style.display = "grid";
    return;
  }
  img.src = `${profile.cardImageUrl}${profile.cardImageUrl.includes("?") ? "&" : "?"}cb=${Date.now()}`;
  img.style.display = "block";
  placeholder.style.display = "none";
}

function render() {
  syncTimeframeButtons();
  const profile = state.profile;
  const hasProfile = Boolean(profile);
  const walletValue = el("st-wallet-value");
  const title = el("st-archetype-title");
  const score = el("st-archetype-score");
  const shareText = el("st-share-text");
  const input = el("st-wallet-input");
  if (input && document.activeElement !== input) input.value = state.wallet;
  if (walletValue) walletValue.textContent = hasProfile ? profile.wallet : "-";
  if (title) title.textContent = hasProfile ? profile.archetypeTitle : "No card yet";
  if (score) score.textContent = hasProfile ? toNum(profile.archetypeScore, 0).toFixed(1) : "-";
  if (shareText) {
    shareText.textContent = hasProfile
      ? profile.shareText
      : "Search a wallet to generate share copy.";
  }
  renderMetrics(profile);
  renderScores(profile);
  renderCard(profile);
  ["st-share-x-btn", "st-download-btn", "st-copy-text-btn", "st-copy-link-btn"].forEach((id) => {
    const node = el(id);
    if (node) node.disabled = !hasProfile || state.loading;
  });
  const generateBtn = el("st-generate-btn");
  if (generateBtn) {
    generateBtn.disabled = state.loading;
    generateBtn.textContent = state.loading ? "Generating..." : "Generate Card";
  }
}

async function loadProfile() {
  const wallet = String(state.wallet || "").trim();
  if (!wallet) {
    setStatus("Enter a wallet address to generate a social card.", true);
    return;
  }
  state.loading = true;
  state.error = "";
  render();
  setStatus("Loading trader archetype...");
  try {
    const params = new URLSearchParams({
      wallet,
      timeframe: state.timeframe,
    });
    const profile = await fetchJson(`/api/social-trade/profile?${params.toString()}`, {
      timeoutMs: 60000,
      retries: 1,
    });
    state.profile = profile;
    writeSessionJson(SESSION_CACHE_KEYS.profile, profile);
    const nextUrl = `/social-cart?wallet=${encodeURIComponent(wallet)}&timeframe=${encodeURIComponent(
      state.timeframe
    )}`;
    window.history.replaceState({}, "", nextUrl);
    setStatus(`${profile.archetypeTitle} ready.`);
  } catch (error) {
    state.profile = null;
    state.error = error && error.message ? error.message : "Failed to generate social card.";
    setStatus(state.error, true);
  } finally {
    state.loading = false;
    render();
  }
}

async function onShareX() {
  if (!state.profile) return;
  const intent = new URL("https://twitter.com/intent/tweet");
  intent.searchParams.set("text", buildTweetText(state.profile));
  intent.searchParams.set("url", buildShareLink(state.profile));
  window.open(intent.toString(), "_blank", "noopener,noreferrer");
}

async function onDownloadCard() {
  if (!state.profile || !state.profile.cardImageUrl) return;
  const link = document.createElement("a");
  link.href = state.profile.cardImageUrl;
  link.download = `${String(state.profile.archetypeKey || "social-cart")}-${shortWallet(
    state.profile.wallet
  )}.png`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}

async function onCopyText() {
  if (!state.profile) return;
  await copyToClipboard(state.profile.shareText || "");
  setStatus("Share text copied.");
}

async function onCopyLink() {
  if (!state.profile) return;
  await copyToClipboard(buildShareLink(state.profile));
  setStatus("Share link copied.");
}

function bindEvents() {
  const input = el("st-wallet-input");
  if (input) {
    input.addEventListener("input", (event) => {
      state.wallet = String(event.target.value || "").trim();
    });
    input.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        loadProfile();
      }
    });
  }
  el("st-generate-btn")?.addEventListener("click", () => loadProfile());
  el("st-share-x-btn")?.addEventListener("click", onShareX);
  el("st-download-btn")?.addEventListener("click", onDownloadCard);
  el("st-copy-text-btn")?.addEventListener("click", onCopyText);
  el("st-copy-link-btn")?.addEventListener("click", onCopyLink);
  document.querySelectorAll("[data-timeframe]").forEach((button) => {
    button.addEventListener("click", () => {
      const next = String(button.getAttribute("data-timeframe") || "30d");
      if (next === state.timeframe) return;
      state.timeframe = next;
      render();
      if (state.wallet) loadProfile();
    });
  });
}

function bootFromUrl() {
  const params = new URLSearchParams(window.location.search || "");
  const wallet = String(params.get("wallet") || "").trim();
  const timeframe = String(params.get("timeframe") || "all").trim().toLowerCase();
  state.wallet = wallet;
  state.timeframe = timeframe === "all" ? "all" : "all";
}

function hydrateProfileFromSession() {
  if (!state.wallet || state.profile) return;
  const cached = readSessionJson(SESSION_CACHE_KEYS.profile);
  if (!cached || typeof cached !== "object") return;
  if (String(cached.wallet || "").trim() !== state.wallet) return;
  state.profile = cached;
}

async function prewarmSessionPayload(key, url, timeoutMs = 5000) {
  if (readSessionJson(key)) return;
  try {
    const payload = await fetchJson(url, { timeoutMs });
    if (payload && typeof payload === "object") {
      writeSessionJson(key, payload);
    }
  } catch (_error) {
    // ignore warmup failures
  }
}

function start() {
  bootFromUrl();
  hydrateProfileFromSession();
  bindEvents();
  render();
  afterWindowLoad(() => {
    if (isConstrainedNetwork() || document.visibilityState === "hidden") return;
    scheduleIdleTask(() => {
      prewarmSessionPayload(SESSION_CACHE_KEYS.exchange, "/api/exchange/overview?timeframe=all", 5000).catch(
        () => null
      );
      prewarmSessionPayload(
        SESSION_CACHE_KEYS.wallets,
        "/api/wallets?timeframe=all&page=1&pageSize=20&sort=volumeUsdRaw&dir=desc",
        8000
      ).catch(() => null);
      prewarmSessionPayload(SESSION_CACHE_KEYS.liveTradeSummary, "/api/live-trades/wallet-first", 5000).catch(
        () => null
      );
    }, 4000);
  }, 12000);
  if (state.wallet) loadProfile();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", start, { once: true });
} else {
  start();
}
