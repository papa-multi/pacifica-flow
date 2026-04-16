#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

const ROOT = path.resolve(__dirname, "..");
const MANIFEST_PATH = path.join(ROOT, "data", "wallet_explorer_v3", "manifest.json");

const {
  loadWalletExplorerV3Snapshot,
  writeWalletExplorerV3Snapshot,
} = require(path.join(ROOT, "src", "services", "read_model", "wallet_storage_v3"));
const {
  buildLeaderboardIndex,
  fetchPacificaPublicWalletSources,
  loadPacificaPublicWalletSourcesSnapshot,
  writePacificaPublicWalletSourcesSnapshot,
} = require(path.join(ROOT, "src", "services", "read_model", "pacifica_public_wallet_sources"));
const { readJson, writeJsonAtomic } = require(path.join(ROOT, "src", "services", "pipeline", "utils"));

function toNum(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function normalizeWallet(value) {
  return String(value || "").trim();
}

function applyPublicCoverage(rows, snapshot) {
  const leaderboardIndex = buildLeaderboardIndex(snapshot);
  const byWallet = new Map();
  for (const row of Array.isArray(rows) ? rows : []) {
    const wallet = normalizeWallet(row && row.wallet);
    if (!wallet) continue;
    byWallet.set(wallet, { ...(row || {}) });
  }
  let endpointCoveredWallets = 0;
  let endpointSeededWallets = 0;
  leaderboardIndex.forEach((entry, wallet) => {
    const existing = byWallet.get(wallet);
    const publicStatus = {
      coverageModel: existing ? "pipeline_plus_public_endpoint" : "public_endpoint_seed",
      sourceCoverage: {
        leaderboard: true,
        portfolioDetail: true,
      },
      publicEndpoints: {
        username: String(entry.username || "").trim() || null,
        rankAllTime: toNum(entry.rankAllTime, 0) || null,
        pnlAllTime: toNum(entry.pnlAllTime, 0),
        volumeAllTime: toNum(entry.volumeAllTime, 0),
        equityCurrent: toNum(entry.equityCurrent, 0),
        oiCurrent: toNum(entry.oiCurrent, 0),
        fetchedAt: snapshot && snapshot.generatedAt ? snapshot.generatedAt : null,
      },
    };
    endpointCoveredWallets += 1;
    if (existing) {
      byWallet.set(wallet, {
        ...existing,
        status: {
          ...(existing.status && typeof existing.status === "object" ? existing.status : {}),
          ...publicStatus,
        },
      });
      return;
    }
    endpointSeededWallets += 1;
    byWallet.set(wallet, {
      wallet,
      walletRecordId: null,
      createdAt: null,
      updatedAt: snapshot && snapshot.generatedAt ? snapshot.generatedAt : Date.now(),
      trades: 0,
      volumeUsdRaw: toNum(entry.volumeAllTime, 0),
      volumeUsd: toNum(entry.volumeAllTime, 0),
      pnlUsd: toNum(entry.pnlAllTime, 0),
      unrealizedPnlUsd: 0,
      totalPnlUsd: toNum(entry.pnlAllTime, 0),
      totalWins: 0,
      totalLosses: 0,
      winRate: 0,
      openPositions: 0,
      firstTrade: null,
      lastTrade: null,
      lastOpenedAt: null,
      lastActivity: null,
      exposureUsd: Math.max(0, toNum(entry.oiCurrent, 0)),
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
      searchText: `${wallet} ${String(entry.username || "").trim()}`.trim().toLowerCase(),
      symbols: [],
      lifecycleStage: "endpoint_seeded",
      status: publicStatus,
    });
  });
  return {
    rows: Array.from(byWallet.values()),
    endpointCoveredWallets,
    endpointSeededWallets,
    leaderboardWallets: leaderboardIndex.size,
  };
}

async function main() {
  let publicSnapshot = loadPacificaPublicWalletSourcesSnapshot();
  publicSnapshot = await fetchPacificaPublicWalletSources().catch(() => publicSnapshot);
  if (!publicSnapshot) {
    throw new Error("missing public endpoint snapshot");
  }
  writePacificaPublicWalletSourcesSnapshot(publicSnapshot);

  const snapshot = loadWalletExplorerV3Snapshot();
  if (!snapshot || !Array.isArray(snapshot.rows)) {
    throw new Error(`missing wallet_explorer_v3 snapshot at ${MANIFEST_PATH}`);
  }

  const applied = applyPublicCoverage(snapshot.rows, publicSnapshot);
  const manifest = snapshot.manifest && typeof snapshot.manifest === "object" ? snapshot.manifest : {};
  const writtenManifest = writeWalletExplorerV3Snapshot({
    sourcePath: "wallet_explorer_v3_public_endpoint_coverage",
    rows: applied.rows,
    counts: manifest.counts || snapshot.counts || null,
    availableSymbols: manifest.availableSymbols || snapshot.availableSymbols || null,
    manifestPath: MANIFEST_PATH,
  });

  const manifestPayload = readJson(MANIFEST_PATH, {}) || {};
  manifestPayload.generatedAt = writtenManifest.generatedAt || Date.now();
  manifestPayload.lastActivitySource = "isolated_wallet_open_history";
  manifestPayload.historicalSource = "wallet_explorer_v2_isolated_records_and_history+public_leaderboard_seed";
  manifestPayload.liveOverlaySource = "nextgen_wallet_state";
  manifestPayload.refreshMode = "incremental_canonical";
  manifestPayload.publicEndpointCoverage = {
    leaderboardWallets: applied.leaderboardWallets,
    endpointCoveredWallets: applied.endpointCoveredWallets,
    endpointSeededWallets: applied.endpointSeededWallets,
    pipelineOnlyWallets: Math.max(0, applied.rows.length - applied.endpointCoveredWallets),
    portfolioMode:
      publicSnapshot && publicSnapshot.portfolio && publicSnapshot.portfolio.mode
        ? publicSnapshot.portfolio.mode
        : "account_scoped_detail",
  };
  writeJsonAtomic(MANIFEST_PATH, manifestPayload);

  console.log(
    JSON.stringify(
      {
        done: true,
        walletCount: applied.rows.length,
        endpointCoveredWallets: applied.endpointCoveredWallets,
        endpointSeededWallets: applied.endpointSeededWallets,
        manifestPath: MANIFEST_PATH,
      },
      null,
      2
    )
  );
}

main().catch((error) => {
  console.error(error && error.stack ? error.stack : String(error));
  process.exitCode = 1;
});
