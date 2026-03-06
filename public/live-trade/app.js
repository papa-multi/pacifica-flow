const state = {
  tab: "public",
  search: "",
  payload: null,
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

function fmtTime(ts) {
  const num = Number(ts);
  if (!Number.isFinite(num) || num <= 0) return "-";
  return new Date(num).toLocaleString();
}

function el(id) {
  return document.getElementById(id);
}

function setText(id, value) {
  const node = el(id);
  if (node) node.textContent = value;
}

async function fetchJson(url) {
  const res = await fetch(url);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.error || `${url} -> ${res.status}`);
  }
  return data;
}

function matchSearch(values = []) {
  const q = String(state.search || "").trim().toLowerCase();
  if (!q) return true;
  return values.some((value) => String(value || "").toLowerCase().includes(q));
}

function renderStatus() {
  const payload = state.payload || {};
  setText("live-ws", `WS: ${payload.summary ? payload.summary.wsStatus : "-"}`);
  setText("live-sync", `Sync: ${fmtTime(payload.sync ? payload.sync.lastBootstrapAt : null)}`);
  setText("live-updated", `Updated: ${fmtTime(payload.generatedAt)}`);
}

function renderKpis() {
  const payload = state.payload || {};
  const summary = payload.summary || {};
  const rows = [
    ["Market Trades", fmt(summary.publicTrades || 0, 0)],
    ["Account Trades", fmt(summary.accountTrades || 0, 0)],
    ["Open Positions", fmt(summary.openPositions || 0, 0)],
    ["Indexed Wallets", fmt(summary.indexedWallets || 0, 0)],
  ];

  const container = el("live-kpis");
  if (!container) return;
  container.innerHTML = rows
    .map(
      ([title, value]) => `<article class="kpi-card">
        <div class="kpi-title">${title}</div>
        <div class="kpi-value">${value}</div>
      </article>`
    )
    .join("");
}

function renderPublicTrades() {
  const payload = state.payload || {};
  const rows = Array.isArray(payload.publicTrades) ? payload.publicTrades : [];
  const filtered = rows.filter((row) =>
    matchSearch([row.symbol, row.side, row.cause, row.price, row.amount, row.li])
  );
  setText("public-count", `${filtered.length} rows`);

  const body = el("public-trades-body");
  if (!body) return;
  body.innerHTML = filtered
    .slice(0, 500)
    .map(
      (row) => `<tr>
      <td>${fmtTime(row.timestamp)}</td>
      <td><strong>${row.symbol || "-"}</strong></td>
      <td>${row.side || "-"}</td>
      <td>${fmt(row.price, 6)}</td>
      <td>${fmt(row.amount, 8)}</td>
      <td>${row.cause || "-"}</td>
      <td>${row.li || "-"}</td>
    </tr>`
    )
    .join("");
}

function renderAccountTrades() {
  const payload = state.payload || {};
  const rows = Array.isArray(payload.accountTrades) ? payload.accountTrades : [];
  const filtered = rows.filter((row) =>
    matchSearch([row.symbol, row.side, row.cause, row.price, row.amount, row.pnl, row.li])
  );
  setText("account-count", `${filtered.length} rows`);

  const body = el("account-trades-body");
  if (!body) return;
  body.innerHTML = filtered
    .slice(0, 500)
    .map(
      (row) => `<tr>
      <td>${fmtTime(row.timestamp)}</td>
      <td><strong>${row.symbol || "-"}</strong></td>
      <td>${row.side || "-"}</td>
      <td>${fmt(row.price, 6)}</td>
      <td>${fmt(row.amount, 8)}</td>
      <td class="${toNum(row.pnl, 0) >= 0 ? "good" : "bad"}">${fmtSigned(row.pnl, 2)}</td>
      <td>${fmt(row.fee, 6)}</td>
      <td>${row.li || "-"}</td>
    </tr>`
    )
    .join("");
}

function renderPositions() {
  const payload = state.payload || {};
  const rows = Array.isArray(payload.positions) ? payload.positions : [];
  const filtered = rows.filter((row) =>
    matchSearch([row.symbol, row.side, row.amountAbs, row.entryPrice, row.markPrice, row.riskTag])
  );
  setText("positions-count", `${filtered.length} rows`);

  const body = el("positions-body");
  if (!body) return;
  body.innerHTML = filtered
    .slice(0, 300)
    .map(
      (row) => `<tr>
      <td><strong>${row.symbol || "-"}</strong></td>
      <td>${row.side || "-"}</td>
      <td>${fmt(row.amountAbs, 8)}</td>
      <td>${fmt(row.entryPrice, 6)}</td>
      <td>${fmt(row.markPrice, 6)}</td>
      <td>$${fmtCompact(row.notionalUsd)}</td>
      <td class="${toNum(row.unrealizedPnlUsd, 0) >= 0 ? "good" : "bad"}">${fmtSigned(
        row.unrealizedPnlUsd,
        2
      )}</td>
      <td>${row.riskTag || "-"}</td>
    </tr>`
    )
    .join("");
}

function pickWalletBucket(row = {}) {
  return row.all || row.d24 || row.d30 || {};
}

function renderWallets() {
  const payload = state.payload || {};
  const rows = Array.isArray(payload.walletPerformance) ? payload.walletPerformance : [];
  const normalized = rows
    .map((row) => {
      const bucket = pickWalletBucket(row);
      return {
        wallet: row.wallet,
        trades: toNum(bucket.trades, 0),
        volumeUsd: toNum(bucket.volumeUsd, 0),
        pnlUsd: toNum(bucket.pnlUsd, 0),
        winRate: toNum(bucket.winRatePct, 0),
        firstTrade: bucket.firstTrade || null,
        lastTrade: bucket.lastTrade || null,
      };
    })
    .filter((row) =>
      matchSearch([row.wallet, row.trades, row.volumeUsd, row.pnlUsd, row.winRate, row.lastTrade])
    )
    .sort((a, b) => b.volumeUsd - a.volumeUsd)
    .slice(0, 400);

  setText("wallets-count", `${normalized.length} rows`);

  const body = el("wallets-body");
  if (!body) return;
  body.innerHTML = normalized
    .map(
      (row) => `<tr>
      <td class="wallet-cell" title="${row.wallet}">${row.wallet}</td>
      <td>${fmt(row.trades, 0)}</td>
      <td>$${fmtCompact(row.volumeUsd)}</td>
      <td class="${row.pnlUsd >= 0 ? "good" : "bad"}">${fmtSigned(row.pnlUsd, 2)}</td>
      <td>${fmt(row.winRate, 2)}%</td>
      <td>${fmtTime(row.firstTrade)}</td>
      <td>${fmtTime(row.lastTrade)}</td>
    </tr>`
    )
    .join("");
}

function renderAll() {
  renderStatus();
  renderKpis();
  renderPublicTrades();
  renderAccountTrades();
  renderPositions();
  renderWallets();
}

function applyTab(tab) {
  state.tab = tab;
  document.querySelectorAll("#live-tabs .chip-btn").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.tab === tab);
  });
  document.querySelectorAll(".live-section").forEach((section) => {
    section.classList.toggle("active", section.id === `tab-${tab}`);
  });
}

async function refreshData() {
  state.payload = await fetchJson("/api/live-trades");
  renderAll();
}

function wireEvents() {
  document.querySelectorAll("#live-tabs .chip-btn").forEach((btn) => {
    btn.addEventListener("click", () => applyTab(btn.dataset.tab));
  });

  el("live-refresh-btn")?.addEventListener("click", async () => {
    await refreshData();
  });

  el("live-search")?.addEventListener("input", (event) => {
    state.search = String(event.target.value || "");
    renderAll();
  });
}

async function boot() {
  wireEvents();
  applyTab("public");

  try {
    await refreshData();
  } catch (error) {
    console.error(error);
  }

  setInterval(async () => {
    try {
      await refreshData();
    } catch (_error) {
      // keep page alive on transient failures
    }
  }, 2500);
}

boot();

