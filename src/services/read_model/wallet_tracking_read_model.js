const fs = require("fs");
const path = require("path");

const { readJson } = require("../pipeline/utils");
const { createWalletSnapshotQuery } = require("./wallet_snapshot_query");
const {
  MANIFEST_PATH: WALLET_EXPLORER_V3_MANIFEST_PATH,
  loadWalletExplorerV3Snapshot,
} = require("./wallet_storage_v3");

function buildDefaultWalletSearchParams() {
  const params = new URLSearchParams();
  params.set("timeframe", "all");
  params.set("page", "1");
  params.set("pageSize", "20");
  params.set("sort", "rankingVolumeUsd");
  params.set("dir", "desc");
  return params;
}

function isBlankQueryValue(value) {
  return String(value || "").trim() === "";
}

function isDefaultWalletSearchParams(searchParams) {
  if (!searchParams || typeof searchParams.keys !== "function") return false;
  const allowedKeys = new Set([
    "page",
    "pageSize",
    "timeframe",
    "sort",
    "dir",
    "q",
    "symbol",
    "symbols",
    "status",
  ]);
  for (const key of searchParams.keys()) {
    const normalizedKey = String(key || "").trim();
    if (!allowedKeys.has(normalizedKey)) return false;
    if (
      (normalizedKey === "q" ||
        normalizedKey === "symbol" ||
        normalizedKey === "symbols" ||
        normalizedKey === "status") &&
      !isBlankQueryValue(searchParams.get(key))
    ) {
      return false;
    }
  }
  const page = String(searchParams.get("page") || "1").trim();
  const pageSize = String(searchParams.get("pageSize") || "20").trim();
  const timeframe = String(searchParams.get("timeframe") || "all").trim().toLowerCase();
  const sort = String(searchParams.get("sort") || "rankingVolumeUsd").trim();
  const dir = String(searchParams.get("dir") || "desc").trim().toLowerCase();
  return (
    page === "1" &&
    pageSize === "20" &&
    timeframe === "all" &&
    (sort === "rankingVolumeUsd" || sort === "volume" || sort === "volumeUsd") &&
    dir === "desc"
  );
}

function cloneJson(value) {
  if (value === null || value === undefined) return value;
  return JSON.parse(JSON.stringify(value));
}

function manifestGeneratedAt(manifest = null) {
  return Number(
    (manifest &&
      manifest.dataset &&
      manifest.dataset.generatedAt) ||
      (manifest && manifest.generatedAt) ||
      0
  ) || 0;
}

function buildFallbackStatusFromManifest(manifest = null) {
  if (!manifest || typeof manifest !== "object") return null;
  const generatedAt = manifestGeneratedAt(manifest) || Date.now();
  const dataset = manifest.dataset && typeof manifest.dataset === "object" ? manifest.dataset : {};
  const counts = dataset.counts && typeof dataset.counts === "object" ? dataset.counts : {};
  const exchangeOverview =
    dataset.exchangeOverview && typeof dataset.exchangeOverview === "object"
      ? dataset.exchangeOverview
      : {};
  const totalWallets = Number(counts.totalWallets || 0);
  const historyVerified = Number(counts.tradeHistoryCompleteWallets || 0);
  return {
    generatedAt,
    status: {
      generatedAt,
      sourceCount: 1,
      activeSourceCount: 1,
      summary: {
        walletExplorerProgress: {
          discovered: totalWallets,
          validated: Number(counts.validatedWallets || 0),
          backfilled: Number(counts.backfilledWallets || 0),
          currentlyTracked: Number(counts.trackedWallets || totalWallets),
          trackedLite: Number(counts.trackedLiteWallets || 0),
          trackedFull: Number(counts.trackedFullWallets || 0),
          metricsComplete: Number(counts.metricsCompleteWallets || 0),
          leaderboardEligible: Number(counts.leaderboardEligibleWallets || 0),
          tradePagesFetched: Number(counts.tradePagesFetched || 0),
          fundingPagesFetched: Number(counts.fundingPagesFetched || 0),
          tradePagesPending: Number(counts.tradePagesPending || 0),
          fundingPagesPending: Number(counts.fundingPagesPending || 0),
          tradePagesFailed: Number(counts.tradePagesFailed || 0),
          fundingPagesFailed: Number(counts.fundingPagesFailed || 0),
          completionRatio: totalWallets > 0 ? historyVerified / totalWallets : 0,
          mode: "snapshot_manifest_fallback",
          updatedAt: generatedAt,
        },
        exchangeOverview,
      },
      services: [
        { id: "ui", name: "UI/API", status: "ready" },
        { id: "walletReadModel", name: "Wallet Read Model", status: "ready" },
      ],
      startup: {
        phase: "ready",
        readyAt: generatedAt,
        steps: {},
      },
    },
  };
}

function mergeLiveDiscoverySummary(statusSnapshot = null, discoverySummary = null) {
  if (!statusSnapshot || typeof statusSnapshot !== "object" || !discoverySummary || typeof discoverySummary !== "object") {
    return statusSnapshot;
  }
  const next = cloneJson(statusSnapshot);
  const mergedDiscovery = cloneJson(discoverySummary);
  const now = Date.now();
  const lastScanAt = Number(mergedDiscovery.lastScanAt || 0) || 0;
  const staleAfterMs = Number(mergedDiscovery.staleAfterMs || 0) || 0;
  if (lastScanAt > 0) {
    mergedDiscovery.status =
      staleAfterMs > 0 && now - lastScanAt > staleAfterMs
        ? "stale"
        : String(mergedDiscovery.mode || "").trim().toLowerCase() === "live"
          ? "live"
          : "catch_up";
  } else {
    mergedDiscovery.status = "idle";
  }
  if (!next.status || typeof next.status !== "object") {
    next.status = {};
  }
  if (!next.status.summary || typeof next.status.summary !== "object") {
    next.status.summary = {};
  }
  const summary = next.status.summary;
  const walletProgress =
    summary.walletExplorerProgress && typeof summary.walletExplorerProgress === "object"
      ? summary.walletExplorerProgress
      : {};
  summary.discoveryStatus = mergedDiscovery;
  summary.walletExplorerProgress = {
    ...walletProgress,
    discovered: Math.max(
      Number(walletProgress.discovered || 0),
      Number(mergedDiscovery.walletCount || 0)
    ),
    validated: Math.max(
      Number(walletProgress.validated || 0),
      Number(mergedDiscovery.confirmedWallets || 0)
    ),
    discoveryStatus: mergedDiscovery,
  };
  summary.dataFreshness = {
    ...(summary.dataFreshness && typeof summary.dataFreshness === "object"
      ? summary.dataFreshness
      : {}),
    lastSuccessfulWalletDiscoveryAt:
      Number(mergedDiscovery.lastScanAt || 0) || Number(mergedDiscovery.updatedAt || 0) || null,
  };
  return next;
}

function attachStatusToWalletPayload(payload, statusSnapshot = null) {
  if (!payload || typeof payload !== "object") return payload;
  const next = cloneJson(payload);
  const statusValue =
    statusSnapshot &&
    statusSnapshot.status &&
    typeof statusSnapshot.status === "object"
      ? statusSnapshot.status
      : null;
  if (statusValue) {
    next.runtime = {
      generatedAt: Number(statusSnapshot.generatedAt || 0) || null,
      sourceCount: Number(statusValue.sourceCount || 0),
      activeSourceCount: Number(statusValue.activeSourceCount || 0),
    };
    next.progress =
      statusValue.summary && statusValue.summary.walletExplorerProgress
        ? statusValue.summary.walletExplorerProgress
        : next.progress || {};
    next.system =
      statusValue.summary
        ? {
            mode: statusValue.summary.systemMode || null,
            ...(statusValue.summary.systemStatus &&
            typeof statusValue.summary.systemStatus === "object"
              ? statusValue.summary.systemStatus
              : {}),
          }
        : next.system || null;
    next.freshness =
      statusValue.summary && statusValue.summary.dataFreshness
        ? statusValue.summary.dataFreshness
        : next.freshness || null;
    next.discovery =
      statusValue.summary && statusValue.summary.discoveryStatus
        ? statusValue.summary.discoveryStatus
        : next.discovery || null;
  }
  next.warmup = {
    persistedPreview: true,
    liveEnrichmentPending: false,
    ...(next.warmup && typeof next.warmup === "object" ? next.warmup : {}),
  };
  return next;
}

function mergeLiveWalletSummary(statusSnapshot = null, datasetSummary = null) {
  if (
    !statusSnapshot ||
    typeof statusSnapshot !== "object" ||
    !datasetSummary ||
    typeof datasetSummary !== "object" ||
    !datasetSummary.counts
  ) {
    return statusSnapshot;
  }
  const next = cloneJson(statusSnapshot);
  if (!next.status || typeof next.status !== "object") {
    return next;
  }
  if (!next.status.summary || typeof next.status.summary !== "object") {
    next.status.summary = {};
  }
  const summary = next.status.summary;
  const counts = datasetSummary.counts || {};
  const generatedAt = Number(datasetSummary.generatedAt || next.generatedAt || Date.now()) || Date.now();
  const currentRawVolumeUsd = Number(counts.rawVolumeUsd || 0);
  const currentVerifiedVolumeUsd = Number(counts.verifiedVolumeUsd || 0);

  const walletExplorerProgress =
    summary.walletExplorerProgress && typeof summary.walletExplorerProgress === "object"
      ? summary.walletExplorerProgress
      : {};
  const existingVolumeProgress =
    walletExplorerProgress.walletVolumeProgress &&
    typeof walletExplorerProgress.walletVolumeProgress === "object"
      ? walletExplorerProgress.walletVolumeProgress
      : {};
  const existingWindowDeltaRaw = Number(existingVolumeProgress.windowDeltaRawVolumeUsd || 0);
  const existingWindowDeltaVerified = Number(
    existingVolumeProgress.windowDeltaVerifiedVolumeUsd || 0
  );
  const existingWindowDeltaWallets = Number(
    existingVolumeProgress.windowDeltaCompletedWallets || 0
  );
  const totalWallets = Number(counts.totalWallets || walletExplorerProgress.discovered || 0);
  const trackedFull = Number(counts.trackedFullWallets || 0);
  const trackedLite = Math.max(0, totalWallets - trackedFull);
  summary.walletExplorerProgress = {
    ...walletExplorerProgress,
    discovered: totalWallets,
    validated: Number(counts.validatedWallets || trackedFull),
    backfilled: Number(counts.backfilledWallets || trackedFull),
    currentlyTracked: Number(counts.trackedWallets || totalWallets),
    trackedLite,
    trackedFull,
    metricsComplete: Number(counts.metricsCompleteWallets || 0),
    leaderboardEligible: Number(counts.leaderboardEligibleWallets || 0),
    tradePagesFetched: Number(counts.tradePagesFetched || 0),
    fundingPagesFetched: Number(counts.fundingPagesFetched || 0),
    tradePagesPending: Number(counts.tradePagesPending || 0),
    fundingPagesPending: Number(counts.fundingPagesPending || 0),
    tradePagesFailed: Number(counts.tradePagesFailed || 0),
    fundingPagesFailed: Number(counts.fundingPagesFailed || 0),
    completionRatio: totalWallets > 0 ? trackedFull / totalWallets : 0,
    updatedAt: generatedAt,
    walletVolumeProgress: {
      ...existingVolumeProgress,
      generatedAt,
      currentRawVolumeUsd,
      currentVerifiedVolumeUsd,
      currentCompletedWallets: trackedFull,
      remainingWallets: Math.max(0, totalWallets - trackedFull),
      walletCoveragePct:
        summary.exchangeOverview && Number(summary.exchangeOverview.totalHistoricalVolumeUsd || 0) > 0
          ? Number(
              (
                (currentRawVolumeUsd /
                  Number(summary.exchangeOverview.totalHistoricalVolumeUsd || 1)) *
                100
              ).toFixed(4)
            )
          : existingVolumeProgress.walletCoveragePct || 0,
      verifiedCoveragePct:
        summary.exchangeOverview && Number(summary.exchangeOverview.totalHistoricalVolumeUsd || 0) > 0
          ? Number(
              (
                (currentVerifiedVolumeUsd /
                  Number(summary.exchangeOverview.totalHistoricalVolumeUsd || 1)) *
                100
              ).toFixed(4)
            )
          : existingVolumeProgress.verifiedCoveragePct || 0,
      rawVolumePerHour: Math.max(0, Number(existingVolumeProgress.rawVolumePerHour || 0)),
      verifiedVolumePerHour: Math.max(
        0,
        Number(existingVolumeProgress.verifiedVolumePerHour || 0)
      ),
      completedWalletsPerHour: Math.max(
        0,
        Number(existingVolumeProgress.completedWalletsPerHour || 0)
      ),
      windowRawVolumeGrowthUsd: Math.max(
        0,
        Number(
          existingVolumeProgress.windowRawVolumeGrowthUsd !== undefined
            ? existingVolumeProgress.windowRawVolumeGrowthUsd
            : existingWindowDeltaRaw
        ) || 0
      ),
      windowVerifiedVolumeGrowthUsd: Math.max(
        0,
        Number(
          existingVolumeProgress.windowVerifiedVolumeGrowthUsd !== undefined
            ? existingVolumeProgress.windowVerifiedVolumeGrowthUsd
            : existingWindowDeltaVerified
        ) || 0
      ),
      windowCompletedWalletGrowth: Math.max(
        0,
        Number(
          existingVolumeProgress.windowCompletedWalletGrowth !== undefined
            ? existingVolumeProgress.windowCompletedWalletGrowth
            : existingWindowDeltaWallets
        ) || 0
      ),
      windowRawVolumeCorrectionUsd: Math.min(
        0,
        Number(
          existingVolumeProgress.windowRawVolumeCorrectionUsd !== undefined
            ? existingVolumeProgress.windowRawVolumeCorrectionUsd
            : existingWindowDeltaRaw
        ) || 0
      ),
      windowVerifiedVolumeCorrectionUsd: Math.min(
        0,
        Number(
          existingVolumeProgress.windowVerifiedVolumeCorrectionUsd !== undefined
            ? existingVolumeProgress.windowVerifiedVolumeCorrectionUsd
            : existingWindowDeltaVerified
        ) || 0
      ),
    },
  };

  if (summary.exchangeOverview && typeof summary.exchangeOverview === "object") {
    const marketTotal = Number(summary.exchangeOverview.totalHistoricalVolumeUsd || 0);
    summary.exchangeOverview = {
      ...summary.exchangeOverview,
      walletSummedVolumeUsd: currentRawVolumeUsd,
      walletVerifiedVolumeUsd: currentVerifiedVolumeUsd,
      volumeGapUsd: Number((marketTotal - currentRawVolumeUsd).toFixed(2)),
      volumeRatio: marketTotal > 0 ? Number((currentRawVolumeUsd / marketTotal).toFixed(6)) : 0,
    };
  }

  if (summary.system && typeof summary.system === "object") {
    summary.system.liveWalletOverlay = datasetSummary.liveOverlay || null;
  }
  next.generatedAt = generatedAt;
  next.status.generatedAt = generatedAt;
  return next;
}

function createWalletTrackingReadModel({ dataRoot }) {
  const uiDir = path.join(dataRoot, "ui");
  const paths = {
    manifest: path.join(uiDir, "wallet_tracking_manifest.json"),
    statusSummary: path.join(uiDir, "system_status_summary.json"),
    defaultWalletPage: path.join(uiDir, "wallets_page1_default.json"),
    walletExplorerV3Manifest: WALLET_EXPLORER_V3_MANIFEST_PATH,
    discoverySummary: path.join(dataRoot, "indexer", "wallet_discovery_summary.json"),
  };
  const walletSnapshotQuery = createWalletSnapshotQuery({
    datasetPath: paths.walletExplorerV3Manifest,
    isolatedDir: path.join(dataRoot, "wallet_explorer_v2", "isolated"),
    heavyLaneStatePath: path.join(dataRoot, "wallet_explorer_v2", "heavy_lane_state.json"),
    livePositionsDir: path.join(dataRoot, "live_positions"),
  });
  const manifestCache = {
    mtimeMs: 0,
    value: null,
  };
  const discoverySummaryCache = {
    mtimeMs: 0,
    value: null,
  };

  function loadManifest(force = false) {
    let mtimeMs = 0;
    try {
      mtimeMs = Number(fs.statSync(paths.manifest).mtimeMs || 0) || 0;
    } catch (_error) {
      mtimeMs = 0;
    }
    if (!force && manifestCache.value && manifestCache.mtimeMs >= mtimeMs) {
      return manifestCache.value;
    }
    const payload = readJson(paths.manifest, null);
    manifestCache.value = payload && typeof payload === "object" ? payload : null;
    manifestCache.mtimeMs = mtimeMs;
    return manifestCache.value;
  }

  function getStatusSummary(force = false) {
    let discoveryMtimeMs = 0;
    try {
      discoveryMtimeMs = Number(fs.statSync(paths.discoverySummary).mtimeMs || 0) || 0;
    } catch (_error) {
      discoveryMtimeMs = 0;
    }
    let discoverySummary = discoverySummaryCache.value;
    if (force || !discoverySummaryCache.value || discoverySummaryCache.mtimeMs < discoveryMtimeMs) {
      discoverySummary = readJson(paths.discoverySummary, null);
      discoverySummaryCache.value =
        discoverySummary && typeof discoverySummary === "object" ? discoverySummary : null;
      discoverySummaryCache.mtimeMs = discoveryMtimeMs;
    }

    const snapshot = readJson(paths.statusSummary, null);
    const datasetSummary = walletSnapshotQuery.getDatasetSummary();
    if (snapshot && snapshot.status && typeof snapshot.status === "object") {
      return mergeLiveDiscoverySummary(
        mergeLiveWalletSummary(snapshot, datasetSummary),
        discoverySummary
      );
    }
    return mergeLiveDiscoverySummary(
      mergeLiveWalletSummary(buildFallbackStatusFromManifest(loadManifest(force)), datasetSummary),
      discoverySummary
    );
  }

  function isDefaultPageSnapshotCoherent(snapshot = null, manifest = null) {
    if (!snapshot || !snapshot.payload || typeof snapshot.payload !== "object") return false;
    const expected = manifestGeneratedAt(manifest);
    if (!expected) return true;
    const actual = Number(snapshot.sourceGeneratedAt || snapshot.generatedAt || 0) || 0;
    return actual === expected;
  }

  function getDefaultWalletPage(statusSnapshot = null) {
    const status = statusSnapshot || getStatusSummary(false);
    const manifest = loadManifest(false);
    const snapshot = readJson(paths.defaultWalletPage, null);
    if (
      isDefaultPageSnapshotCoherent(snapshot, manifest) &&
      !walletSnapshotQuery.hasLiveOverlay() &&
      !walletSnapshotQuery.hasLivePositionsOverlay()
    ) {
      return attachStatusToWalletPayload(snapshot.payload, status);
    }
    const payload = walletSnapshotQuery.queryWallets(buildDefaultWalletSearchParams(), status);
    return payload ? attachStatusToWalletPayload(payload, status) : null;
  }

  function queryWallets(searchParams = null, statusSnapshot = null) {
    const status = statusSnapshot || getStatusSummary(false);
    if (isDefaultWalletSearchParams(searchParams)) {
      return getDefaultWalletPage(status);
    }
    return walletSnapshotQuery.queryWallets(searchParams, status);
  }

  function preload() {
    loadManifest(false);
    getStatusSummary(false);
    walletSnapshotQuery.loadDataset(false);
    try {
      loadWalletExplorerV3Snapshot();
    } catch (_error) {
      // Best effort only. The v3 snapshot warms again through the component path.
    }
  }

  return {
    paths,
    loadManifest,
    getStatusSummary,
    getDefaultWalletPage,
    queryWallets,
    getWalletProfile(wallet) {
      return walletSnapshotQuery.getWalletProfile(wallet);
    },
    preload,
    isDefaultWalletSearchParams,
  };
}

module.exports = {
  createWalletTrackingReadModel,
};
