function createGeneralDataComponent({
  sendJson,
  pipeline,
  restClient,
  clockSync,
  rateGuard,
  readJsonBody,
  onAccountChanged,
  refreshSnapshots,
  liveHost,
  walletIndexer,
  onchainDiscovery,
  getEgressUsage,
  getGlobalKpiState,
  refreshGlobalKpi,
}) {
  function computeDefiLlamaV2FromPrices(rows = []) {
    return (Array.isArray(rows) ? rows : []).reduce(
      (acc, item) => {
        const volume24h = Number(item && item.volume_24h !== undefined ? item.volume_24h : 0);
        const openInterest = Number(
          item && item.open_interest !== undefined ? item.open_interest : 0
        );
        const mark = Number(item && item.mark !== undefined ? item.mark : 0);

        acc.dailyVolume += Number.isFinite(volume24h) ? volume24h : 0;
        if (Number.isFinite(openInterest) && Number.isFinite(mark)) {
          acc.openInterestAtEnd += openInterest * mark;
        }
        return acc;
      },
      {
        dailyVolume: 0,
        openInterestAtEnd: 0,
      }
    );
  }

  async function handleRequest(req, res, url) {
    if (url.pathname === "/api/health") {
      sendJson(res, 200, {
        ok: true,
        now: Date.now(),
        account: pipeline.getAccount(),
        wsStatus: pipeline.getDashboardPayload().environment.wsStatus,
        clock: clockSync.getState(),
        rateLimit: rateGuard.getState(),
        liveHost: liveHost && typeof liveHost.getState === "function" ? liveHost.getState() : null,
        indexer:
          walletIndexer && typeof walletIndexer.getStatus === "function"
            ? walletIndexer.getStatus()
            : null,
        egress: typeof getEgressUsage === "function" ? getEgressUsage() : null,
      });
      return true;
    }

    if (url.pathname === "/api/progress/overview" && req.method === "GET") {
      const indexerStatus =
        walletIndexer && typeof walletIndexer.getStatus === "function"
          ? walletIndexer.getStatus()
          : null;
      const onchainStatus =
        onchainDiscovery && typeof onchainDiscovery.getStatus === "function"
          ? onchainDiscovery.getStatus()
          : null;
      const rate = rateGuard && typeof rateGuard.getState === "function" ? rateGuard.getState() : null;
      const egress = typeof getEgressUsage === "function" ? getEgressUsage() : null;

      const known = Number(indexerStatus && indexerStatus.knownWallets ? indexerStatus.knownWallets : 0);
      const lifecycle = indexerStatus && indexerStatus.lifecycle ? indexerStatus.lifecycle : {};
      const indexed = Number(
        lifecycle && lifecycle.backfillComplete !== undefined
          ? lifecycle.backfillComplete
          : indexerStatus && indexerStatus.indexedCompleteWallets
          ? indexerStatus.indexedCompleteWallets
          : 0
      );
      const partial = Number(
        lifecycle && lifecycle.backfilling !== undefined
          ? lifecycle.backfilling
          : indexerStatus && indexerStatus.partiallyIndexedWallets
          ? indexerStatus.partiallyIndexedWallets
          : 0
      );
      const pending = Number(
        lifecycle && lifecycle.pendingBackfill !== undefined
          ? lifecycle.pendingBackfill
          : indexerStatus && indexerStatus.pendingWallets
          ? indexerStatus.pendingWallets
          : 0
      );
      const failed = Number(indexerStatus && indexerStatus.failedWallets ? indexerStatus.failedWallets : 0);
      const completionPct = known > 0 ? (indexed / known) * 100 : 0;

      sendJson(res, 200, {
        ok: true,
        generatedAt: Date.now(),
        wallets: {
          known,
          indexedBackfillComplete: indexed,
          partialBackfilling: partial,
          pendingBackfill: pending,
          failed,
          failedBackfill: Number(lifecycle.failedBackfill || indexerStatus?.failedBackfillWallets || 0),
          backlog:
            Number(indexerStatus && indexerStatus.walletBacklog ? indexerStatus.walletBacklog : partial + pending + failed),
          completionPct: Number(completionPct.toFixed(4)),
          lifecycle: {
            discovered: Number(lifecycle.discovered || 0),
            pendingBackfill: pending,
            backfilling: partial,
            fullyIndexed: Number(lifecycle.fullyIndexed || 0),
            liveTracking: Number(lifecycle.liveTracking || 0),
            backfillComplete: indexed,
          },
        },
        indexer: indexerStatus,
        onchain: onchainStatus,
        pacificaApi: rate,
        egress,
        globalKpi: typeof getGlobalKpiState === "function" ? getGlobalKpiState() : null,
      });
      return true;
    }

    if (url.pathname === "/api/kpi/global" && req.method === "GET") {
      sendJson(res, 200, {
        ok: true,
        generatedAt: Date.now(),
        data: typeof getGlobalKpiState === "function" ? getGlobalKpiState() : null,
      });
      return true;
    }

    if (url.pathname === "/api/kpi/global/refresh" && req.method === "POST") {
      if (typeof refreshGlobalKpi !== "function") {
        sendJson(res, 501, {
          ok: false,
          error: "global KPI worker disabled",
        });
        return true;
      }
      try {
        const data = await refreshGlobalKpi();
        sendJson(res, 200, {
          ok: true,
          generatedAt: Date.now(),
          data,
        });
      } catch (error) {
        sendJson(res, 500, {
          ok: false,
          error: error.message,
        });
      }
      return true;
    }

    if (url.pathname === "/api/indexer/discover" && req.method === "POST") {
      if (!walletIndexer || typeof walletIndexer.discoverWallets !== "function") {
        sendJson(res, 501, {
          ok: false,
          error: "wallet indexer not enabled",
        });
        return true;
      }

      try {
        const result = await walletIndexer.discoverWallets();
        sendJson(res, 200, {
          ok: true,
          ...result,
          status: walletIndexer.getStatus(),
        });
      } catch (error) {
        sendJson(res, 500, {
          ok: false,
          error: error.message,
        });
      }
      return true;
    }

    if (url.pathname === "/api/indexer/onchain/status" && req.method === "GET") {
      if (!onchainDiscovery || typeof onchainDiscovery.getStatus !== "function") {
        sendJson(res, 200, {
          ok: true,
          enabled: false,
          status: null,
        });
        return true;
      }

      sendJson(res, 200, {
        ok: true,
        enabled: true,
        status: onchainDiscovery.getStatus(),
      });
      return true;
    }

    if (url.pathname === "/api/indexer/onchain/step" && req.method === "POST") {
      if (!onchainDiscovery || typeof onchainDiscovery.discoverStep !== "function") {
        sendJson(res, 501, {
          ok: false,
          error: "onchain discovery not enabled",
        });
        return true;
      }

      try {
        const body = await readJsonBody(req);
        const pages = body && body.pages !== undefined ? Number(body.pages) : undefined;
        const validateLimit =
          body && body.validateLimit !== undefined ? Number(body.validateLimit) : undefined;
        const result = await onchainDiscovery.discoverStep({
          pages,
          validateLimit,
        });
        sendJson(res, 200, result);
      } catch (error) {
        sendJson(res, 500, {
          ok: false,
          error: error.message,
        });
      }
      return true;
    }

    if (url.pathname === "/api/indexer/onchain/mode" && req.method === "POST") {
      if (!onchainDiscovery || typeof onchainDiscovery.setMode !== "function") {
        sendJson(res, 501, {
          ok: false,
          error: "onchain discovery not enabled",
        });
        return true;
      }

      try {
        const body = await readJsonBody(req);
        onchainDiscovery.setMode(body && body.mode ? body.mode : "backfill");
        sendJson(res, 200, {
          ok: true,
          status: onchainDiscovery.getStatus(),
        });
      } catch (error) {
        sendJson(res, 400, {
          ok: false,
          error: error.message,
        });
      }
      return true;
    }

    if (url.pathname === "/api/indexer/onchain/window" && req.method === "POST") {
      if (!onchainDiscovery || typeof onchainDiscovery.setWindow !== "function") {
        sendJson(res, 501, {
          ok: false,
          error: "onchain discovery not enabled",
        });
        return true;
      }

      try {
        const body = await readJsonBody(req);
        onchainDiscovery.setWindow({
          startTimeMs: body.startTimeMs,
          endTimeMs: body.endTimeMs,
        });
        sendJson(res, 200, {
          ok: true,
          status: onchainDiscovery.getStatus(),
        });
      } catch (error) {
        sendJson(res, 400, {
          ok: false,
          error: error.message,
        });
      }
      return true;
    }

    if (url.pathname === "/api/indexer/onchain/reset" && req.method === "POST") {
      if (!onchainDiscovery || typeof onchainDiscovery.resetBackfill !== "function") {
        sendJson(res, 501, {
          ok: false,
          error: "onchain discovery not enabled",
        });
        return true;
      }

      try {
        const body = await readJsonBody(req);
        onchainDiscovery.resetBackfill({
          resetWallets: Boolean(body.resetWallets),
        });
        sendJson(res, 200, {
          ok: true,
          status: onchainDiscovery.getStatus(),
        });
      } catch (error) {
        sendJson(res, 400, {
          ok: false,
          error: error.message,
        });
      }
      return true;
    }

    if (url.pathname === "/api/indexer/onchain/wallets" && req.method === "GET") {
      if (!onchainDiscovery || typeof onchainDiscovery.listWallets !== "function") {
        sendJson(res, 200, {
          ok: true,
          enabled: false,
          rows: [],
          total: 0,
        });
        return true;
      }

      const confidenceMin = Number(url.searchParams.get("confidenceMin") || 0);
      const onlyConfirmed = String(url.searchParams.get("confirmed") || "").toLowerCase() === "1";
      const page = Math.max(1, Number(url.searchParams.get("page") || 1));
      const pageSize = Math.max(1, Math.min(200, Number(url.searchParams.get("pageSize") || 50)));

      const rows = onchainDiscovery.listWallets({
        confidenceMin,
        onlyConfirmed,
      });
      const total = rows.length;
      const start = (page - 1) * pageSize;
      const paged = rows.slice(start, start + pageSize);

      sendJson(res, 200, {
        ok: true,
        total,
        page,
        pageSize,
        rows: paged,
      });
      return true;
    }

    if (url.pathname === "/api/indexer/onchain/deposit_wallets" && req.method === "GET") {
      if (!onchainDiscovery || typeof onchainDiscovery.listDepositWalletAddresses !== "function") {
        sendJson(res, 200, {
          ok: true,
          enabled: false,
          rows: [],
          total: 0,
        });
        return true;
      }

      const page = Math.max(1, Number(url.searchParams.get("page") || 1));
      const pageSize = Math.max(1, Math.min(1000, Number(url.searchParams.get("pageSize") || 200)));

      const rows = onchainDiscovery.listDepositWalletAddresses();
      const total = rows.length;
      const start = (page - 1) * pageSize;
      const paged = rows.slice(start, start + pageSize);

      sendJson(res, 200, {
        ok: true,
        total,
        page,
        pageSize,
        rows: paged,
      });
      return true;
    }

    if (url.pathname === "/api/indexer/onchain/deposit_wallet_evidence" && req.method === "GET") {
      if (!onchainDiscovery || typeof onchainDiscovery.getWalletDepositEvidence !== "function") {
        sendJson(res, 501, {
          ok: false,
          error: "onchain discovery evidence API not enabled",
        });
        return true;
      }

      const wallet = String(url.searchParams.get("wallet") || "").trim();
      const page = Math.max(1, Number(url.searchParams.get("page") || 1));
      const pageSize = Math.max(1, Math.min(500, Number(url.searchParams.get("pageSize") || 100)));
      if (!wallet) {
        sendJson(res, 400, {
          ok: false,
          error: "missing wallet query param",
        });
        return true;
      }

      const payload = onchainDiscovery.getWalletDepositEvidence(wallet, { page, pageSize });
      sendJson(res, 200, {
        ok: true,
        ...payload,
      });
      return true;
    }

    if (url.pathname === "/api/indexer/scan" && req.method === "POST") {
      if (!walletIndexer || typeof walletIndexer.scanCycle !== "function") {
        sendJson(res, 501, {
          ok: false,
          error: "wallet indexer not enabled",
        });
        return true;
      }

      try {
        const result = await walletIndexer.scanCycle();
        sendJson(res, 200, {
          ok: true,
          result,
          status: walletIndexer.getStatus(),
        });
      } catch (error) {
        sendJson(res, 500, {
          ok: false,
          error: error.message,
        });
      }
      return true;
    }

    if (url.pathname === "/api/indexer/reset" && req.method === "POST") {
      if (!walletIndexer || typeof walletIndexer.resetIndexingState !== "function") {
        sendJson(res, 501, {
          ok: false,
          error: "wallet indexer reset API not enabled",
        });
        return true;
      }

      try {
        const body = await readJsonBody(req);
        const result = walletIndexer.resetIndexingState({
          preserveKnownWallets:
            body && Object.prototype.hasOwnProperty.call(body, "preserveKnownWallets")
              ? Boolean(body.preserveKnownWallets)
              : true,
          resetWalletStore:
            body && Object.prototype.hasOwnProperty.call(body, "resetWalletStore")
              ? Boolean(body.resetWalletStore)
              : true,
          clearHistoryFiles:
            body && Object.prototype.hasOwnProperty.call(body, "clearHistoryFiles")
              ? Boolean(body.clearHistoryFiles)
              : true,
        });
        sendJson(res, 200, {
          ok: true,
          result,
          status: walletIndexer.getStatus(),
        });
      } catch (error) {
        sendJson(res, 409, {
          ok: false,
          error: error.message,
        });
      }
      return true;
    }

    if (url.pathname === "/api/indexer/diagnostics" && req.method === "GET") {
      if (!walletIndexer || typeof walletIndexer.getDiagnostics !== "function") {
        sendJson(res, 501, {
          ok: false,
          error: "wallet indexer diagnostics not enabled",
        });
        return true;
      }

      const page = Math.max(1, Number(url.searchParams.get("page") || 1));
      const pageSize = Math.max(
        1,
        Math.min(500, Number(url.searchParams.get("pageSize") || 100))
      );
      const status = String(url.searchParams.get("status") || "").trim();
      const lifecycle = String(url.searchParams.get("lifecycle") || "").trim();
      const q = String(url.searchParams.get("q") || "").trim();

      sendJson(res, 200, {
        ok: true,
        ...walletIndexer.getDiagnostics({
          page,
          pageSize,
          status: status || undefined,
          lifecycle: lifecycle || undefined,
          q: q || undefined,
        }),
      });
      return true;
    }

    if (url.pathname === "/api/indexer/wallets" && req.method === "POST") {
      if (!walletIndexer || typeof walletIndexer.addWallets !== "function") {
        sendJson(res, 501, {
          ok: false,
          error: "wallet indexer not enabled",
        });
        return true;
      }

      try {
        const body = await readJsonBody(req);
        const wallets = Array.isArray(body.wallets)
          ? body.wallets
          : body.wallet
          ? [body.wallet]
          : [];
        const added = walletIndexer.addWallets(wallets, "api");
        sendJson(res, 200, {
          ok: true,
          added,
          status: walletIndexer.getStatus(),
        });
      } catch (error) {
        sendJson(res, 400, {
          ok: false,
          error: error.message,
        });
      }
      return true;
    }

    if (url.pathname === "/api/dashboard" || url.pathname === "/api/dashboard/full") {
      sendJson(res, 200, pipeline.getDashboardPayload());
      return true;
    }

    if (url.pathname === "/api/prices") {
      sendJson(res, 200, {
        generatedAt: Date.now(),
        prices: pipeline.getDashboardPayload().market.prices,
      });
      return true;
    }

    if (url.pathname === "/api/config/account" && req.method === "GET") {
      sendJson(res, 200, {
        account: pipeline.getAccount(),
      });
      return true;
    }

    if (url.pathname === "/api/config/account" && req.method === "POST") {
      try {
        const body = await readJsonBody(req);
        const next = body && body.account ? String(body.account).trim() : "";

        await onAccountChanged(next || null);
        sendJson(res, 200, {
          ok: true,
          account: pipeline.getAccount(),
        });
      } catch (error) {
        sendJson(res, 400, {
          ok: false,
          error: error.message,
        });
      }
      return true;
    }

    if (url.pathname === "/api/snapshot/refresh" && req.method === "POST") {
      try {
        const result = await refreshSnapshots();
        sendJson(res, 200, {
          ok: true,
          ...result,
        });
      } catch (error) {
        sendJson(res, 500, {
          ok: false,
          error: error.message,
        });
      }
      return true;
    }

    if (url.pathname === "/api/clock/sync" && req.method === "POST") {
      try {
        await restClient.get("/info/prices", { cost: 1 });
        sendJson(res, 200, {
          ok: true,
          clock: clockSync.getState(),
        });
      } catch (error) {
        sendJson(res, 500, {
          ok: false,
          error: error.message,
        });
      }
      return true;
    }

    if (
      (url.pathname === "/api/defillama/v2" ||
        url.pathname === "/api/metrics/defillama/v2" ||
        url.pathname === "/api/metrics/defillama/volume24h") &&
      req.method === "GET"
    ) {
      try {
        if (typeof getGlobalKpiState === "function") {
          const shared = getGlobalKpiState();
          if (
            shared &&
            typeof shared === "object" &&
            Number(shared.fetchedAt || 0) > 0 &&
            Number.isFinite(Number(shared.dailyVolume))
          ) {
            const meta =
              shared.volumeMeta && typeof shared.volumeMeta === "object" ? shared.volumeMeta : {};
            sendJson(res, 200, {
              dailyVolume: Number(shared.dailyVolume || 0),
              openInterestAtEnd: Number(shared.openInterestAtEnd || 0),
              totalHistoricalVolume:
                shared.totalHistoricalVolume !== null &&
                shared.totalHistoricalVolume !== undefined &&
                Number.isFinite(Number(shared.totalHistoricalVolume))
                  ? Number(shared.totalHistoricalVolume)
                  : null,
              volumeMethod: shared.volumeMethod || "prices_rolling_24h",
              volumeSource: shared.volumeSource || "/api/v1/info/prices:sum(volume_24h)",
              fetchedAt: Number(shared.fetchedAt || 0),
              trackingStartDate: meta.trackingStartDate || null,
              lastProcessedDate: meta.lastProcessedDate || null,
              remainingDaysToToday:
                meta.remainingDaysToToday !== null && meta.remainingDaysToToday !== undefined
                  ? Number(meta.remainingDaysToToday)
                  : null,
              processedDays:
                meta.processedDays !== null && meta.processedDays !== undefined
                  ? Number(meta.processedDays)
                  : null,
              totalDaysToToday:
                meta.totalDaysToToday !== null && meta.totalDaysToToday !== undefined
                  ? Number(meta.totalDaysToToday)
                  : null,
              backfillComplete:
                meta.backfillComplete !== null && meta.backfillComplete !== undefined
                  ? Boolean(meta.backfillComplete)
                  : null,
              backfillProgress: {
                start_date: meta.trackingStartDate || null,
                current_processed_day: meta.lastProcessedDate || null,
                days_processed:
                  meta.processedDays !== null && meta.processedDays !== undefined
                    ? Number(meta.processedDays)
                    : null,
                days_remaining:
                  meta.remainingDaysToToday !== null && meta.remainingDaysToToday !== undefined
                    ? Number(meta.remainingDaysToToday)
                    : null,
              },
            });
            return true;
          }
        }

        const resPrices = await restClient.get("/info/prices", { cost: 1 });
        const rows = extractPayloadData(resPrices, []);
        const out = computeDefiLlamaV2FromPrices(rows);
        sendJson(res, 200, {
          dailyVolume: out.dailyVolume,
          openInterestAtEnd: out.openInterestAtEnd,
          totalHistoricalVolume: null,
          volumeMethod: "prices_rolling_24h",
          volumeSource: "/api/v1/info/prices:sum(volume_24h)",
          trackingStartDate: null,
          lastProcessedDate: null,
          remainingDaysToToday: null,
          processedDays: null,
          totalDaysToToday: null,
          backfillComplete: null,
          backfillProgress: {
            start_date: null,
            current_processed_day: null,
            days_processed: null,
            days_remaining: null,
          },
        });
      } catch (error) {
        sendJson(res, 500, {
          ok: false,
          error: error.message,
        });
      }
      return true;
    }

    return false;
  }

  return {
    handleRequest,
  };
}

module.exports = {
  createGeneralDataComponent,
};
