#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const { performance } = require("perf_hooks");

const args = process.argv.slice(2);
function readArg(name, fallback = null) {
  const prefix = `--${name}=`;
  const arg = args.find((value) => value.startsWith(prefix));
  if (!arg) return fallback;
  const value = arg.slice(prefix.length).trim();
  return value ? value : fallback;
}

const rootDir = path.resolve(readArg("root", process.cwd()));
const sourcePath = path.resolve(
  readArg("source", path.join(rootDir, "data", "ui", ".build", "wallet_dataset_compact_source.json"))
);
const outputRoot = path.resolve(
  readArg("out", path.join(rootDir, "data", "wallet_explorer_v3"))
);
const manifestPath = path.join(outputRoot, "manifest.json");
const shardCount = Number(readArg("shards", process.env.PACIFICA_WALLET_EXPLORER_V3_SHARDS || 32));

if (Number.isFinite(shardCount) && shardCount > 0) {
  process.env.PACIFICA_WALLET_EXPLORER_V3_SHARDS = String(Math.trunc(shardCount));
}

const {
  writeWalletExplorerV3Snapshot,
  loadWalletExplorerV3Snapshot,
  MANIFEST_PATH,
} = require(path.join(rootDir, "src", "services", "read_model", "wallet_storage_v3"));
const {
  buildLeaderboardIndex,
  loadPacificaPublicWalletDetailCache,
  loadPacificaPublicWalletSourcesSnapshot,
} = require(path.join(rootDir, "src", "services", "read_model", "pacifica_public_wallet_sources"));
const legacyIsolatedHistoryDir = path.join(rootDir, "data", "wallet_explorer_v2", "isolated");
const legacySummaryCache = new Map();

function fileSizeBytes(filePath) {
  try {
    return fs.statSync(filePath).size || 0;
  } catch {
    return 0;
  }
}

function dirSizeBytes(dirPath) {
  let total = 0;
  const stack = [dirPath];
  while (stack.length) {
    const current = stack.pop();
    let entries = [];
    try {
      entries = fs.readdirSync(current, { withFileTypes: true });
    } catch {
      continue;
    }
    for (const entry of entries) {
      const fullPath = path.join(current, entry.name);
      if (entry.isDirectory()) {
        stack.push(fullPath);
      } else if (entry.isFile() || entry.isSymbolicLink()) {
        total += fileSizeBytes(fullPath);
      }
    }
  }
  return total;
}

function formatBytes(bytes) {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let value = Number(bytes) || 0;
  let unitIndex = 0;
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }
  return `${value.toFixed(value >= 10 || unitIndex === 0 ? 0 : 2)} ${units[unitIndex]}`;
}

function readSource() {
  const raw = fs.readFileSync(sourcePath, "utf8");
  const payload = JSON.parse(raw);
  const rows = Array.isArray(payload.rows) ? payload.rows : [];
  return {
    payload,
    rows,
  };
}

function loadLegacyHistorySummaries(wallet) {
  const normalized = String(wallet || "").trim();
  if (!normalized) return null;
  if (legacySummaryCache.has(normalized)) {
    return legacySummaryCache.get(normalized);
  }
  const filePath = path.join(legacyIsolatedHistoryDir, normalized, "wallet_history.json");
  if (!fs.existsSync(filePath)) {
    legacySummaryCache.set(normalized, null);
    return null;
  }
  try {
    const payload = JSON.parse(fs.readFileSync(filePath, "utf8"));
    const trades = Array.isArray(payload && payload.trades) ? payload.trades : [];
    if (!trades.length) {
      legacySummaryCache.set(normalized, null);
      return null;
    }
    let feesPaidUsd = 0;
    let liquidityPoolFeesUsd = 0;
    let netFeesUsd = 0;
    let volumeUsd = 0;
    let pnlUsd = 0;
    let firstTrade = null;
    let lastTrade = null;
    let firstOpenedAt = null;
    let lastOpenedAt = null;
    for (const trade of trades) {
      if (!trade || typeof trade !== "object") continue;
      const ts = Number(trade.timestamp || trade.created_at || trade.createdAt || 0);
      if (Number.isFinite(ts) && ts > 0) {
        firstTrade = firstTrade === null ? ts : Math.min(firstTrade, ts);
        lastTrade = lastTrade === null ? ts : Math.max(lastTrade, ts);
      }
      const side = String(trade.side || "").trim().toLowerCase();
      if (side.startsWith("open_") && Number.isFinite(ts) && ts > 0) {
        firstOpenedAt = firstOpenedAt === null ? ts : Math.min(firstOpenedAt, ts);
        lastOpenedAt = lastOpenedAt === null ? ts : Math.max(lastOpenedAt, ts);
      }
      const amount = Math.abs(Number(trade.amount || 0));
      const price = Number(trade.price || 0);
      if (Number.isFinite(amount) && Number.isFinite(price)) {
        volumeUsd += amount * price;
      }
      const fee = Number(trade.fee || 0);
      const lpFee = Math.max(
        0,
        Number(
          trade.liquidityPoolFee ??
            trade.liquidity_pool_fee ??
            trade.lp_fee ??
            trade.lpFee ??
            trade.supply_side_fee ??
            trade.supplySideFee ??
            0
        )
      );
      if (fee > 0) feesPaidUsd += fee;
      liquidityPoolFeesUsd += lpFee;
      netFeesUsd += fee + lpFee;
      pnlUsd += Number(trade.pnl || 0);
    }
    const summaries = {
      wallet: normalized,
      trades: trades.length,
      volumeUsd: Number(volumeUsd.toFixed(2)),
      feesPaidUsd: Number(feesPaidUsd.toFixed(2)),
      feesUsd: Number(feesPaidUsd.toFixed(2)),
      liquidityPoolFeesUsd: Number(liquidityPoolFeesUsd.toFixed(2)),
      netFeesUsd: Number(netFeesUsd.toFixed(2)),
      pnlUsd: Number(pnlUsd.toFixed(2)),
      firstTrade: firstTrade || null,
      lastTrade: lastTrade || null,
      firstOpenedAt,
      lastOpenedAt,
      source: "isolated_wallet_history",
      updatedAt: Number(payload && payload.updatedAt) || null,
    };
    legacySummaryCache.set(normalized, summaries);
    return summaries;
  } catch {
    legacySummaryCache.set(normalized, null);
    return null;
  }
}

function loadLegacyFeeSummary(wallet) {
  const normalized = String(wallet || "").trim();
  if (!normalized) return null;
  return loadLegacyHistorySummaries(normalized);
}

function loadLegacyOpenedSummary(wallet) {
  const normalized = String(wallet || "").trim();
  if (!normalized) return null;
  const summaries = loadLegacyHistorySummaries(normalized);
  if (!summaries || !summaries.lastOpenedAt) return null;
  return {
    wallet: normalized,
    firstOpenedAt: summaries.firstOpenedAt,
    lastOpenedAt: summaries.lastOpenedAt,
    source: summaries.source,
    updatedAt: summaries.updatedAt,
  };
}

function applyPortfolioMetricOverlay(row = {}, detailEntry = null) {
  const safeRow = row && typeof row === "object" ? { ...row } : {};
  const detail = detailEntry && typeof detailEntry === "object" ? detailEntry : null;
  if (!detail) return safeRow;
  const volumeAllTime = Number(detail.volumeAllTime || 0);
  const pnlAllTime = Number(detail.pnlAllTime || 0);
  const drawdownPct = Number(detail.drawdownPct || 0);
  const returnPct = Number(detail.returnPct || 0);
  const accountEquityUsd = Number(detail.accountEquityUsd || 0);
  const openPositions = Math.max(0, Number(detail.openPositions || 0));
  if (Number.isFinite(volumeAllTime)) {
    safeRow.volumeUsdRaw = volumeAllTime;
    safeRow.volumeUsd = volumeAllTime;
  }
  if (Number.isFinite(pnlAllTime)) {
    safeRow.pnlUsd = pnlAllTime;
    safeRow.totalPnlUsd = pnlAllTime;
    safeRow.unrealizedPnlUsd = 0;
  }
  if (Number.isFinite(drawdownPct)) safeRow.drawdownPct = drawdownPct;
  if (Number.isFinite(returnPct)) safeRow.returnPct = returnPct;
  if (Number.isFinite(accountEquityUsd)) safeRow.accountEquityUsd = accountEquityUsd;
  safeRow.openPositions = openPositions;
  const baseAll = safeRow.all && typeof safeRow.all === "object" ? { ...safeRow.all } : {};
  safeRow.all = {
    ...baseAll,
    volumeUsd: Number.isFinite(volumeAllTime) ? volumeAllTime : Number(baseAll.volumeUsd || 0),
    pnlUsd: Number.isFinite(pnlAllTime) ? pnlAllTime : Number(baseAll.pnlUsd || 0),
    drawdownPct: Number.isFinite(drawdownPct) ? drawdownPct : Number(baseAll.drawdownPct || 0),
    returnPct: Number.isFinite(returnPct) ? returnPct : Number(baseAll.returnPct || 0),
    accountEquityUsd: Number.isFinite(accountEquityUsd)
      ? accountEquityUsd
      : Number(baseAll.accountEquityUsd || 0),
    openPositions,
  };
  const baseStatus = safeRow.status && typeof safeRow.status === "object" ? { ...safeRow.status } : {};
  safeRow.status = {
    ...baseStatus,
    metricSources: {
      ...(baseStatus.metricSources && typeof baseStatus.metricSources === "object"
        ? baseStatus.metricSources
        : {}),
      volumeUsd: "portfolio_volume",
      pnlUsd: "portfolio_timeline",
      drawdownPct: "portfolio_timeline",
      returnPct: "portfolio_timeline",
      accountEquityUsd: "portfolio_account",
      openPositions: "portfolio_account",
    },
    portfolioMetrics: {
      source: String(detail.source || "portfolio_timeline_volume_account"),
      fetchedAt: Number(detail.fetchedAt || 0) || null,
      firstSeenAt: Number(detail.firstSeenAt || 0) || null,
      lastSeenAt: Number(detail.lastSeenAt || 0) || null,
      accountUpdatedAt: Number(detail.accountUpdatedAt || 0) || null,
    },
  };
  return safeRow;
}

function enrichRowsWithLegacyFees(rows) {
  const enriched = [];
  let enrichedCount = 0;
  for (const row of rows) {
    const next = row && typeof row === "object" ? { ...row } : {};
    const wallet = String(next.wallet || "").trim();
    const openedSummary = wallet ? loadLegacyOpenedSummary(wallet) : null;
    const hasFees = Number(next.feesPaidUsd || next.feesUsd || 0) > 0;
    if (!hasFees && wallet) {
      const summary = loadLegacyFeeSummary(wallet);
      if (summary) {
        next.feesPaidUsd = summary.feesPaidUsd;
        next.feesUsd = summary.feesUsd;
        next.netFeesUsd = summary.netFeesUsd;
        next.feeRebatesUsd = summary.liquidityPoolFeesUsd;
        next.revenueUsd = Number(next.revenueUsd || 0) || Number(next.pnlUsd || 0) + Number(next.unrealizedPnlUsd || 0);
        if (!Number(next.firstTrade || 0) && summary.firstTrade) next.firstTrade = summary.firstTrade;
        if (!Number(next.lastTrade || 0) && summary.lastTrade) next.lastTrade = summary.lastTrade;
        enrichedCount += 1;
      }
    }
    if (openedSummary && openedSummary.lastOpenedAt) {
      next.lastOpenedAt = openedSummary.lastOpenedAt;
      next.lastActivity = openedSummary.lastOpenedAt;
      next.lastActiveAt = openedSummary.lastOpenedAt;
      next.lastActivityAt = openedSummary.lastOpenedAt;
    }
    enriched.push(next);
  }
  return { rows: enriched, enrichedCount };
}

function applyPublicEndpointCoverage(rows) {
  const snapshot = loadPacificaPublicWalletSourcesSnapshot();
  const detailCache = loadPacificaPublicWalletDetailCache();
  const detailByWallet =
    detailCache && detailCache.wallets && typeof detailCache.wallets === "object"
      ? detailCache.wallets
      : {};
  const leaderboardIndex = buildLeaderboardIndex(snapshot);
  const byWallet = new Map();
  for (const row of Array.isArray(rows) ? rows : []) {
    const wallet = String(row && row.wallet ? row.wallet : "").trim();
    if (!wallet) continue;
    byWallet.set(wallet, { ...(row || {}) });
  }
  let endpointCoveredWallets = 0;
  let endpointSeededWallets = 0;
  leaderboardIndex.forEach((entry, wallet) => {
    const detailEntry = detailByWallet[wallet] || null;
    const existing = byWallet.get(wallet);
    if (existing) {
      endpointCoveredWallets += 1;
      byWallet.set(wallet, {
        ...existing,
        firstTrade:
          Number(existing.firstTrade || 0) ||
          Number(detailEntry && detailEntry.firstSeenAt ? detailEntry.firstSeenAt : 0) ||
          null,
        lastTrade:
          Number(existing.lastTrade || 0) ||
          Number(detailEntry && detailEntry.lastSeenAt ? detailEntry.lastSeenAt : 0) ||
          null,
        status: {
          ...(existing.status && typeof existing.status === "object" ? existing.status : {}),
          coverageModel: "pipeline_plus_public_endpoint",
          sourceCoverage: {
            leaderboard: true,
            portfolioDetail: true,
          },
          publicEndpoints: {
            username: String(entry.username || "").trim() || null,
            rankAllTime: Number(entry.rankAllTime || 0) || null,
            pnlAllTime: Number(entry.pnlAllTime || 0),
            volumeAllTime: Number(entry.volumeAllTime || 0),
            equityCurrent: Number(entry.equityCurrent || 0),
            oiCurrent: Number(entry.oiCurrent || 0),
            fetchedAt: snapshot && snapshot.generatedAt ? snapshot.generatedAt : null,
            firstSeenAt: Number(detailEntry && detailEntry.firstSeenAt ? detailEntry.firstSeenAt : 0) || null,
            lastSeenAt: Number(detailEntry && detailEntry.lastSeenAt ? detailEntry.lastSeenAt : 0) || null,
          },
        },
      });
      return;
    }
    endpointCoveredWallets += 1;
    endpointSeededWallets += 1;
    const username = String(entry.username || "").trim() || null;
    byWallet.set(wallet, applyPortfolioMetricOverlay({
      wallet,
      walletRecordId: null,
      createdAt: null,
      updatedAt: snapshot && snapshot.generatedAt ? snapshot.generatedAt : Date.now(),
      trades: 0,
      volumeUsdRaw: Number(entry.volumeAllTime || 0),
      volumeUsd: Number(entry.volumeAllTime || 0),
      pnlUsd: Number(entry.pnlAllTime || 0),
      unrealizedPnlUsd: 0,
      totalPnlUsd: Number(entry.pnlAllTime || 0),
      totalWins: 0,
      totalLosses: 0,
      winRate: 0,
      openPositions: 0,
      firstTrade: Number(detailEntry && detailEntry.firstSeenAt ? detailEntry.firstSeenAt : 0) || null,
      lastTrade: Number(detailEntry && detailEntry.lastSeenAt ? detailEntry.lastSeenAt : 0) || null,
      lastOpenedAt: null,
      lastActivity: null,
      exposureUsd: Math.max(0, Number(entry.oiCurrent || 0)),
      feesPaidUsd: 0,
      feesUsd: 0,
      netFeesUsd: 0,
      feeRebatesUsd: 0,
      revenueUsd: 0,
      symbolBreakdown: [],
      d24: null,
      d7: null,
      d30: null,
      all: null,
      completeness: {
        historyVerified: false,
        metricsVerified: false,
        leaderboardEligible: false,
        endpointSeeded: true,
        endpointCoverage: ["leaderboard", "portfolio_detail"],
        leaderboardBlockedReasons: ["endpoint_seeded_missing_canonical_history"],
      },
      searchText: `${wallet} ${username || ""}`.trim().toLowerCase(),
      symbols: [],
      lifecycleStage: "endpoint_seeded",
      status: {
        coverageModel: "public_endpoint_seed",
        sourceCoverage: {
          leaderboard: true,
          portfolioDetail: true,
        },
        publicEndpoints: {
          username,
          rankAllTime: Number(entry.rankAllTime || 0) || null,
          pnlAllTime: Number(entry.pnlAllTime || 0),
          volumeAllTime: Number(entry.volumeAllTime || 0),
          equityCurrent: Number(entry.equityCurrent || 0),
          oiCurrent: Number(entry.oiCurrent || 0),
          fetchedAt: snapshot && snapshot.generatedAt ? snapshot.generatedAt : null,
          firstSeenAt: Number(detailEntry && detailEntry.firstSeenAt ? detailEntry.firstSeenAt : 0) || null,
          lastSeenAt: Number(detailEntry && detailEntry.lastSeenAt ? detailEntry.lastSeenAt : 0) || null,
        },
      },
    }, detailEntry));
  });
  return {
    rows: Array.from(byWallet.values()).map((row) =>
      applyPortfolioMetricOverlay(
        row,
        detailByWallet[String((row && row.wallet) || "").trim()] || null
      )
    ),
    endpointCoveredWallets,
    endpointSeededWallets,
  };
}

function verifySnapshot(expectedRows) {
  const snapshot = loadWalletExplorerV3Snapshot();
  if (!snapshot || !Array.isArray(snapshot.preparedRows)) {
    throw new Error("v3 snapshot did not load after conversion");
  }
  if (snapshot.preparedRows.length !== expectedRows.length) {
    throw new Error(`row count mismatch: expected ${expectedRows.length}, got ${snapshot.preparedRows.length}`);
  }
  const expectedByWallet = new Map(expectedRows.map((row) => [String(row.wallet || "").trim(), row]));
  for (const row of snapshot.preparedRows) {
    const wallet = String(row.wallet || "").trim();
    const expected = expectedByWallet.get(wallet);
    if (!expected) {
      throw new Error(`missing wallet in v3 snapshot: ${wallet}`);
    }
    const expectedVolume = Number(expected.volumeUsdRaw ?? expected.volumeUsd ?? 0);
    const actualVolume = Number(row.volumeUsdRaw ?? row.volumeUsd ?? 0);
    if (Number.isFinite(expectedVolume) && Number.isFinite(actualVolume) && Math.abs(expectedVolume - actualVolume) > 0.0001) {
      throw new Error(`volume mismatch for ${wallet}`);
    }
    const expectedTrades = Number(expected.trades || 0);
    const actualTrades = Number(row.trades || 0);
    if (expectedTrades !== actualTrades) {
      throw new Error(`trade count mismatch for ${wallet}`);
    }
  }
}

function main() {
  const startedAt = performance.now();
  const source = readSource();
  const sourceSize = fileSizeBytes(sourcePath);
  const existingSize = dirSizeBytes(outputRoot);
  const enriched = enrichRowsWithLegacyFees(source.rows);
  const withPublicCoverage = applyPublicEndpointCoverage(enriched.rows);
  const manifest = writeWalletExplorerV3Snapshot({
    sourcePath,
    rows: withPublicCoverage.rows,
    counts: source.payload.counts || null,
    availableSymbols: source.payload.availableSymbols || null,
    manifestPath,
  });
  const verifyFullSnapshot =
    String(process.env.PACIFICA_WALLET_EXPLORER_V3_VERIFY_FULL || "")
      .trim()
      .toLowerCase() === "true";
  if (verifyFullSnapshot) {
    verifySnapshot(withPublicCoverage.rows);
  }
  const finalSize = dirSizeBytes(outputRoot);
  const elapsedMs = Math.round(performance.now() - startedAt);
  const shardFiles = (manifest.shards || []).filter((shard) => Number(shard.rowCount || 0) > 0).length;
  const report = {
    sourcePath,
    manifestPath,
    rows: source.rows.length,
    shardCount: manifest.shardCount,
    populatedShards: shardFiles,
    sourceSizeBytes: sourceSize,
    existingSizeBytes: existingSize,
    finalSizeBytes: finalSize,
    reducedBytes: Math.max(0, sourceSize - finalSize),
    enrichedFromLegacyFeeHistory: enriched.enrichedCount,
    endpointCoveredWallets: withPublicCoverage.endpointCoveredWallets,
    endpointSeededWallets: withPublicCoverage.endpointSeededWallets,
    elapsedMs,
    elapsedSeconds: Number((elapsedMs / 1000).toFixed(2)),
    manifestGeneratedAt: manifest.generatedAt,
    manifestPathUsed: manifestPath === MANIFEST_PATH ? manifestPath : MANIFEST_PATH,
  };
  console.log(JSON.stringify(report, null, 2));
}

try {
  main();
} catch (error) {
  console.error(error && error.stack ? error.stack : String(error));
  process.exitCode = 1;
}
