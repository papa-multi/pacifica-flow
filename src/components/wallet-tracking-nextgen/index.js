"use strict";

const NEXTGEN_WALLET_API_SHADOW_ENABLED = /^(1|true|yes|on)$/i.test(
  String(process.env.PACIFICA_NEXTGEN_WALLET_API_SHADOW_ENABLED || "").trim()
);

function createWalletTrackingNextgenComponent({
  sendJson,
  runtime,
}) {
  function buildSyncHealthPreview(syncHealth = null) {
    if (!syncHealth || typeof syncHealth !== "object") return null;
    const reviewPipeline =
      syncHealth.reviewPipeline && typeof syncHealth.reviewPipeline === "object"
        ? syncHealth.reviewPipeline
        : null;
    const strictLive =
      syncHealth.nextgen &&
      syncHealth.nextgen.strictLive &&
      typeof syncHealth.nextgen.strictLive === "object"
        ? syncHealth.nextgen.strictLive
        : null;
    const counts =
      syncHealth.counts && typeof syncHealth.counts === "object" ? syncHealth.counts : null;
    return {
      generatedAt: Number(syncHealth.generatedAt || 0) || null,
      visibleWallets: Math.max(0, Number(syncHealth.visibleWallets || 0) || 0),
      hiddenZeroTradeWallets: Math.max(
        0,
        Number(syncHealth.hiddenZeroTradeWallets || 0) || 0
      ),
      verifiedZeroTradeWallets: Math.max(
        0,
        Number(syncHealth.verifiedZeroTradeWallets || 0) || 0
      ),
      counts: counts
        ? {
            totalWallets: Math.max(0, Number(counts.totalWallets || 0) || 0),
            openPositions: Math.max(0, Number(counts.openPositions || 0) || 0),
          }
        : null,
      reviewPipeline: reviewPipeline
        ? {
            generatedAt: Number(reviewPipeline.generatedAt || 0) || null,
            totalWallets: Math.max(0, Number(reviewPipeline.totalWallets || 0) || 0),
            verifiedZeroTradeWallets: Math.max(
              0,
              Number(reviewPipeline.verifiedZeroTradeWallets || 0) || 0
            ),
            visibleWallets: Math.max(0, Number(reviewPipeline.visibleWallets || 0) || 0),
            hiddenZeroTradeWallets: Math.max(
              0,
              Number(reviewPipeline.hiddenZeroTradeWallets || 0) || 0
            ),
            stageCounts:
              reviewPipeline.stageCounts && typeof reviewPipeline.stageCounts === "object"
                ? reviewPipeline.stageCounts
                : {},
            activeClaims:
              reviewPipeline.activeClaims && typeof reviewPipeline.activeClaims === "object"
                ? reviewPipeline.activeClaims
                : {},
            recentUpdates:
              reviewPipeline.recentUpdates && typeof reviewPipeline.recentUpdates === "object"
                ? reviewPipeline.recentUpdates
                : {},
            baselineThroughDate:
              String(reviewPipeline.baselineThroughDate || "").trim() || null,
          }
        : null,
      nextgen: strictLive
        ? {
            strictLive: {
              sourceMode: String(strictLive.sourceMode || "").trim() || null,
              generatedAt: Number(strictLive.generatedAt || 0) || null,
              snapshotGeneratedAt: Number(strictLive.snapshotGeneratedAt || 0) || null,
              totalWallets: Math.max(0, Number(strictLive.totalWallets || 0) || 0),
              openWallets: Math.max(0, Number(strictLive.openWallets || 0) || 0),
              healthyWallets: Math.max(0, Number(strictLive.healthyWallets || 0) || 0),
              degradedWallets: Math.max(0, Number(strictLive.degradedWallets || 0) || 0),
              staleWallets: Math.max(0, Number(strictLive.staleWallets || 0) || 0),
              missingWallets: Math.max(0, Number(strictLive.missingWallets || 0) || 0),
            },
          }
        : null,
    };
  }

  function parsePage(url) {
    return Math.max(1, Number(url.searchParams.get("page") || 1));
  }

  function parsePageSize(url) {
    return 20;
  }

  function parseDirection(url) {
    return String(
      url.searchParams.get("order") ||
      url.searchParams.get("dir") ||
      "desc"
    ).trim();
  }

  return {
    async handleRequest(req, res, url) {
      if (!runtime) return false;

      if (NEXTGEN_WALLET_API_SHADOW_ENABLED && url.pathname === "/api/wallets" && req.method === "GET") {
        const q = String(url.searchParams.get("q") || "").trim();
        const page = parsePage(url);
        const pageSize = parsePageSize(url);
        const sort = String(url.searchParams.get("sort") || "lastTrade").trim();
        const order = parseDirection(url);
        const result = runtime.queryWallets({ q, page, pageSize, sort, order });
        sendJson(res, 200, {
          generatedAt: Date.now(),
          query: q,
          page: result.page,
          pageSize: result.pageSize,
          total: result.total,
          pages: Math.max(1, Math.ceil(result.total / result.pageSize)),
          wallets: result.rows,
          rows: result.rows,
          sorting: { sort, order },
          timeframe: "all",
          counts: {
            totalWallets: result.totalWallets,
            hiddenZeroTradeWallets: result.hiddenZeroTradeWallets,
          },
          syncHealth: buildSyncHealthPreview(runtime.getSyncHealth()),
        });
        return true;
      }

      if (NEXTGEN_WALLET_API_SHADOW_ENABLED && url.pathname === "/api/wallets/profile" && req.method === "GET") {
        const wallet = String(url.searchParams.get("wallet") || "").trim();
        const payload = runtime.getProfile(wallet);
        sendJson(res, 200, {
          generatedAt: Date.now(),
          found: Boolean(payload.found),
          wallet,
          summary: payload.summary,
          reviewStage: payload.reviewStage,
          verifiedThroughAt: payload.verifiedThroughAt,
          lastHeadCheckAt: payload.lastHeadCheckAt,
          lastActivity: payload.lastActivity,
          lastOpenedAt: payload.lastOpenedAt,
          lastOpenedPosition: payload.lastOpenedPosition,
          lastPositionActivityAt: payload.lastPositionActivityAt,
          lastPositionActivityType: payload.lastPositionActivityType,
          lastPositionActivityConfidence: payload.summary ? payload.summary.lastPositionActivityConfidence : null,
          liveSourceMode: payload.summary ? payload.summary.liveSourceMode : null,
          wsSubscriptionStatus: payload.summary ? payload.summary.wsSubscriptionStatus : null,
          activeWsSubscription: payload.summary ? payload.summary.activeWsSubscription : null,
          lastEventReceivedAt: payload.summary ? payload.summary.lastEventReceivedAt : null,
          lastLocalStateUpdateAt: payload.summary ? payload.summary.lastLocalStateUpdateAt : null,
          lastPositionSnapshotAt: payload.summary ? payload.summary.lastPositionSnapshotAt : null,
          liveHeartbeatHealthy: payload.summary ? payload.summary.liveHeartbeatHealthy : null,
          liveFreshnessStatus: payload.summary ? payload.summary.liveFreshnessStatus : null,
          liveFreshnessAgeMs: payload.summary ? payload.summary.liveFreshnessAgeMs : null,
          snapshotConsistency: payload.summary ? payload.summary.snapshotConsistency : null,
          hasReliablePositionSet: payload.summary ? payload.summary.hasReliablePositionSet : null,
          accountHintReason: payload.summary ? payload.summary.accountHintReason : null,
          accountHintMissCount: payload.summary ? payload.summary.accountHintMissCount : null,
          materializedPositions: payload.summary ? payload.summary.materializedPositions : null,
          isTrulyLive: payload.summary ? payload.summary.isTrulyLive : null,
          symbolBreakdown: payload.symbolBreakdown,
          openPositions: payload.openPositions,
          positions: payload.positions,
          orders: payload.orders,
          trades: payload.trades,
          recentTrades: payload.recentTrades,
          tradeStore: payload.tradeStore,
          live: payload.live,
        });
        return true;
      }

      if (NEXTGEN_WALLET_API_SHADOW_ENABLED && url.pathname === "/api/wallets/sync-health" && req.method === "GET") {
        sendJson(res, 200, runtime.getSyncHealth());
        return true;
      }

      if (url.pathname === "/api/nextgen/wallets" && req.method === "GET") {
        const q = String(url.searchParams.get("q") || "").trim();
        sendJson(res, 200, runtime.queryWallets({ q, page: 1, pageSize: 20 }));
        return true;
      }

      return false;
    },
  };
}

module.exports = {
  createWalletTrackingNextgenComponent,
};
