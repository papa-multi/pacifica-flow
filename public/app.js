const state = {
  view: "exchange",
  timeframe: "all",
  exchange: null,
  volumeRankPage: 1,
  volumeRankPageSize: 10,
  wallets: null,
  walletProfile: null,
  walletSearch: "",
  walletPage: 1,
  walletPageSize: 20,
  selectedWallet: null,
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
  const source = payload.source || {};
  const idx = payload.indexer || {};

  const volumeMetaParts = [];
  if (Number.isFinite(Number(source.defillamaFetchDurationMs))) {
    volumeMetaParts.push(
      `load ${fmtMs(source.defillamaFetchDurationMs)} (${source.defillamaCacheHit ? "cache" : "live"})`
    );
  }
  if (source.defillamaFetchedAt) {
    volumeMetaParts.push(`updated ${fmtTime(source.defillamaFetchedAt)}`);
  }
  if (source.defillamaCheckedAt) {
    volumeMetaParts.push(`checked ${fmtTime(source.defillamaCheckedAt)}`);
  }
  if (source.defillamaStale) {
    volumeMetaParts.push("stale");
  }
  if (source.defillamaLastError) {
    volumeMetaParts.push(`err: ${String(source.defillamaLastError).slice(0, 48)}`);
  }
  if (source.defillamaSource) {
    volumeMetaParts.push(`src ${source.defillamaSource}`);
  }
  if (Number.isFinite(Number(source.defillamaDailyVolumeFromPrices24h))) {
    volumeMetaParts.push(`24h ref $${fmtCompact(Number(source.defillamaDailyVolumeFromPrices24h))}`);
  }
  const backfill = source.defillamaBackfillProgress || {};
  const backfillStart = backfill.start_date || source.defillamaTrackingStartDate || null;
  const backfillCurrent =
    backfill.current_processed_day || source.defillamaTrackedThroughDate || null;
  const backfillProcessed =
    backfill.days_processed !== null && backfill.days_processed !== undefined
      ? Number(backfill.days_processed)
      : source.defillamaProcessedDays !== null && source.defillamaProcessedDays !== undefined
      ? Number(source.defillamaProcessedDays)
      : null;
  const backfillRemaining =
    backfill.days_remaining !== null && backfill.days_remaining !== undefined
      ? Number(backfill.days_remaining)
      : source.defillamaRemainingDaysToToday !== null &&
        source.defillamaRemainingDaysToToday !== undefined
      ? Number(source.defillamaRemainingDaysToToday)
      : null;
  if (backfillStart) {
    volumeMetaParts.push(`start_date: ${backfillStart}`);
  }
  if (backfillCurrent) {
    volumeMetaParts.push(`current_processed_day: ${backfillCurrent}`);
  }
  if (backfillProcessed !== null) {
    volumeMetaParts.push(`days_processed: ${backfillProcessed}`);
  }
  if (backfillRemaining !== null) {
    const todayLabel = source.defillamaTodayUtcDate ? ` (today ${source.defillamaTodayUtcDate})` : "";
    volumeMetaParts.push(`days_remaining: ${backfillRemaining}${todayLabel}`);
  }
  if (
    backfillProcessed !== null &&
    source.defillamaTotalDaysToToday !== null &&
    source.defillamaTotalDaysToToday !== undefined
  ) {
    volumeMetaParts.push(`progress ${backfillProcessed}/${Number(source.defillamaTotalDaysToToday)}d`);
  }
  if (source.defillamaBackfillComplete === true) {
    volumeMetaParts.push("backfill complete");
  }
  if (source.defillamaVolumeMethod) {
    volumeMetaParts.push(`method ${source.defillamaVolumeMethod}`);
  }

  const feeMetaParts = ["wallet-indexed"];
  const lifecycle = idx.lifecycle || {};
  const indexedWallets = Number(
    lifecycle.backfillComplete !== undefined
      ? lifecycle.backfillComplete
      : idx.indexedCompleteWallets !== undefined
      ? idx.indexedCompleteWallets
      : source.walletsIndexed || 0
  );
  const knownWallets = Number(idx.knownWallets || 0);
  if (knownWallets > 0) {
    const pct = (indexedWallets / knownWallets) * 100;
    feeMetaParts.push(`coverage ${fmt(indexedWallets, 0)}/${fmt(knownWallets, 0)} (${fmt(pct, 2)}%)`);
  } else if (indexedWallets > 0) {
    feeMetaParts.push(`indexed ${fmt(indexedWallets, 0)} wallets`);
  }
  if (Number.isFinite(Number(kpis.totalTradingFeesUsd))) {
    feeMetaParts.push(`trading $${fmtCompact(Number(kpis.totalTradingFeesUsd))}`);
  }
  if (Number.isFinite(Number(kpis.totalLiquidityPoolFeesUsd))) {
    const lpValue = Number(kpis.totalLiquidityPoolFeesUsd);
    if (lpValue > 0) {
      feeMetaParts.push(`lp $${fmtCompact(lpValue)}`);
    } else if (source.liquidityPoolFeesIncluded === false) {
      feeMetaParts.push("lp unavailable");
    }
  } else if (source.liquidityPoolFeesIncluded === false) {
    feeMetaParts.push("lp unavailable");
  }

  const kpiRows = [
    { key: "Total Revenue", value: `$${fmtCompact(kpis.totalRevenueUsd)}` },
    { key: "Total Accounts", value: fmt(kpis.totalAccounts || 0, 0) },
    { key: "Total Trades", value: fmt(kpis.totalTrades || 0, 0) },
    {
      key: "Total Volume",
      value: `$${fmtCompact(kpis.totalVolumeUsd)}`,
      meta: volumeMetaParts.join(" • "),
    },
    {
      key: "Total Fees",
      value: `$${fmt(kpis.totalFeesUsd, 2)}`,
      meta: feeMetaParts.join(" • "),
    },
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

  const volumeSpotValue = `$${fmtCompact(kpis.totalVolumeUsd)}`;
  setText("volume-hero-value", volumeSpotValue);
  setText(
    "volume-hero-sub",
    backfillCurrent
      ? `Tracked through ${backfillCurrent} UTC`
      : "Historical backfill in progress"
  );

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
  state.exchange = data;
  renderStatusBars();
  renderExchange();
  renderIndexerProgressPanel();
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
  await Promise.allSettled([refreshExchange(), refreshWallets()]);
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
    if (!state.selectedWallet) return;
    inspectWallet(state.selectedWallet).catch(() => null);
  }, 15000);
}

init().catch((error) => {
  console.error(error);
  setText("status-sync", `Error: ${error.message}`);
});
