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
  const DAY_MS = 24 * 60 * 60 * 1000;

  function parseUtcDateMs(dateLike) {
    const iso = String(dateLike || "").trim().slice(0, 10);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(iso)) return null;
    const ms = Date.parse(`${iso}T00:00:00.000Z`);
    return Number.isFinite(ms) ? ms : null;
  }

  function getDayStartUtc(timestamp) {
    const value = Number(timestamp);
    if (!Number.isFinite(value)) return null;
    const date = new Date(value);
    return Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate());
  }

  function getWeekStartUtc(timestamp) {
    const dayStart = getDayStartUtc(timestamp);
    if (!Number.isFinite(dayStart)) return null;
    const date = new Date(dayStart);
    const day = (date.getUTCDay() + 6) % 7; // Monday as week start
    return dayStart - day * DAY_MS;
  }

  function getMonthKeyUtc(timestamp) {
    const value = Number(timestamp);
    if (!Number.isFinite(value)) return null;
    const date = new Date(value);
    const year = date.getUTCFullYear();
    const month = String(date.getUTCMonth() + 1).padStart(2, "0");
    return `${year}-${month}`;
  }

  function getYearKeyUtc(timestamp) {
    const value = Number(timestamp);
    if (!Number.isFinite(value)) return null;
    return String(new Date(value).getUTCFullYear());
  }

  function monthKeyToTimestamp(key) {
    const parts = String(key || "").split("-");
    if (parts.length !== 2) return null;
    const year = Number(parts[0]);
    const month = Number(parts[1]);
    if (!Number.isFinite(year) || !Number.isFinite(month)) return null;
    return Date.UTC(year, month - 1, 1);
  }

  function aggregateFromDayMap(dayMap, keyFn) {
    const result = new Map();
    if (!dayMap || typeof dayMap.forEach !== "function") return result;
    dayMap.forEach((value, key) => {
      const dayKey = Number(key);
      const numValue = Number(value);
      if (!Number.isFinite(dayKey) || !Number.isFinite(numValue)) return;
      const bucketKey = keyFn(dayKey);
      if (bucketKey === null || bucketKey === undefined) return;
      result.set(bucketKey, (result.get(bucketKey) || 0) + numValue);
    });
    return result;
  }

  function normalizeDailyMap(dayMap, { startDay = null, coverageDay = null } = {}) {
    const normalized = new Map();
    if (!dayMap || typeof dayMap.forEach !== "function") return normalized;
    let maxDay = null;
    dayMap.forEach((value, key) => {
      const dayKey = getDayStartUtc(Number(key));
      const numValue = Number(value);
      if (!Number.isFinite(dayKey) || !Number.isFinite(numValue)) return;
      if (Number.isFinite(startDay) && dayKey < startDay) return;
      if (Number.isFinite(coverageDay) && dayKey > coverageDay) return;
      normalized.set(dayKey, (normalized.get(dayKey) || 0) + numValue);
      if (!Number.isFinite(maxDay) || dayKey > maxDay) maxDay = dayKey;
    });

    const fromDay = Number.isFinite(startDay) ? startDay : null;
    const toDay = Number.isFinite(coverageDay)
      ? coverageDay
      : Number.isFinite(maxDay)
      ? maxDay
      : null;
    if (Number.isFinite(fromDay) && Number.isFinite(toDay) && toDay >= fromDay) {
      for (let day = fromDay; day <= toDay; day += DAY_MS) {
        if (!normalized.has(day)) normalized.set(day, 0);
      }
    }

    return new Map([...normalized.entries()].sort((a, b) => a[0] - b[0]));
  }

  function buildMetricSeries(
    { dailyMap, weeklyMap, monthlyMap, yearlyMap },
    coverageTimestamp,
    monthNames
  ) {
    const hasCoverage = Number.isFinite(coverageTimestamp);
    const daily = [...(dailyMap || new Map()).entries()]
      .map(([key, value]) => ({ key: Number(key), value: Number(value) }))
      .filter((item) => Number.isFinite(item.key) && Number.isFinite(item.value))
      .filter((item) => (!hasCoverage ? true : item.key <= coverageTimestamp))
      .sort((a, b) => a.key - b.key);

    const weekly = [...(weeklyMap || new Map()).entries()]
      .map(([key, value]) => ({ key: Number(key), value: Number(value) }))
      .filter((item) => Number.isFinite(item.key) && Number.isFinite(item.value))
      .filter((item) => (!hasCoverage ? true : item.key <= coverageTimestamp))
      .sort((a, b) => a.key - b.key);

    const monthly = [...(monthlyMap || new Map()).entries()]
      .map(([key, value]) => ({
        key: String(key),
        value: Number(value),
        timestamp: monthKeyToTimestamp(key),
      }))
      .filter((item) => Number.isFinite(item.timestamp) && Number.isFinite(item.value))
      .filter((item) => (!hasCoverage ? true : item.timestamp <= coverageTimestamp))
      .sort((a, b) => (a.key === b.key ? 0 : a.key < b.key ? -1 : 1));

    const yearly = [...(yearlyMap || new Map()).entries()]
      .map(([key, value]) => ({ key: String(key), value: Number(value) }))
      .filter((item) => Number.isFinite(Number(item.key)) && Number.isFinite(item.value))
      .sort((a, b) => Number(a.key) - Number(b.key));

    return {
      daily: daily.map((item) => {
        const date = new Date(item.key);
        return {
          label: `${monthNames[date.getUTCMonth()]} ${String(date.getUTCDate()).padStart(2, "0")}`,
          value: item.value,
          timestamp: item.key,
        };
      }),
      weekly: weekly.map((item) => {
        const date = new Date(item.key);
        return {
          label: `${monthNames[date.getUTCMonth()]} ${String(date.getUTCDate()).padStart(2, "0")}`,
          value: item.value,
          timestamp: item.key,
        };
      }),
      monthly: monthly.map((item) => ({
        label: `${monthNames[Number(item.key.split("-")[1]) - 1]} ${item.key.split("-")[0]}`,
        value: item.value,
        timestamp: item.timestamp,
      })),
      yearly: yearly.map((item) => ({
        label: item.key,
        value: item.value,
        timestamp: Date.UTC(Number(item.key), 0, 1),
      })),
    };
  }

  function buildVolumeSeriesFromGlobalKpi(globalState) {
    const history =
      globalState && globalState.history && typeof globalState.history === "object"
        ? globalState.history
        : {};
    const meta =
      globalState && globalState.volumeMeta && typeof globalState.volumeMeta === "object"
        ? globalState.volumeMeta
        : {};
    const dailyByDate =
      history && history.dailyByDate && typeof history.dailyByDate === "object"
        ? history.dailyByDate
        : {};

    const startDay =
      parseUtcDateMs(meta.trackingStartDate) ||
      parseUtcDateMs(history.startDate) ||
      parseUtcDateMs("2025-09-09");
    const coverageDay =
      parseUtcDateMs(meta.lastProcessedDate) ||
      parseUtcDateMs(history.lastProcessedDate) ||
      getDayStartUtc(Date.now());

    const rawDailyMap = new Map();
    Object.entries(dailyByDate).forEach(([day, row]) => {
      const dayMs = parseUtcDateMs(day);
      if (!Number.isFinite(dayMs)) return;
      const value =
        row && typeof row === "object" && row.dailyVolume !== undefined ? row.dailyVolume : row;
      const volume = Number(value);
      if (!Number.isFinite(volume)) return;
      rawDailyMap.set(dayMs, volume);
    });

    const volumeDaily = normalizeDailyMap(rawDailyMap, {
      startDay,
      coverageDay,
    });
    const volumeWeekly = aggregateFromDayMap(volumeDaily, getWeekStartUtc);
    const volumeMonthly = aggregateFromDayMap(volumeDaily, getMonthKeyUtc);
    const volumeYearly = aggregateFromDayMap(volumeDaily, getYearKeyUtc);

    const monthNames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    const volumeSeries = buildMetricSeries(
      {
        dailyMap: volumeDaily,
        weeklyMap: volumeWeekly,
        monthlyMap: volumeMonthly,
        yearlyMap: volumeYearly,
      },
      coverageDay,
      monthNames
    );

    return {
      volume: volumeSeries,
      fees: { daily: [], weekly: [], monthly: [], yearly: [] },
      revenue: { daily: [], weekly: [], monthly: [], yearly: [] },
      meta: {
        source: "global_kpi_history",
        startDate: Number.isFinite(startDay) ? new Date(startDay).toISOString().slice(0, 10) : null,
        trackedThrough: Number.isFinite(coverageDay)
          ? new Date(coverageDay).toISOString().slice(0, 10)
          : null,
        points: volumeSeries.daily.length,
      },
    };
  }

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

    if (url.pathname === "/api/volume-series" && req.method === "GET") {
      const shared = typeof getGlobalKpiState === "function" ? getGlobalKpiState() : null;
      sendJson(res, 200, buildVolumeSeriesFromGlobalKpi(shared || {}));
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
        const shared = typeof getGlobalKpiState === "function" ? getGlobalKpiState() : null;
        const meta =
          shared && shared.volumeMeta && typeof shared.volumeMeta === "object"
            ? shared.volumeMeta
            : {};

        const resPrices = await restClient.get("/info/prices", { cost: 1 });
        const payload = resPrices && resPrices.payload ? resPrices.payload : {};
        const rows = Array.isArray(payload.data) ? payload.data : [];
        const out = computeDefiLlamaV2FromPrices(rows);
        sendJson(res, 200, {
          dailyVolume: out.dailyVolume,
          openInterestAtEnd: out.openInterestAtEnd,
          totalHistoricalVolume:
            shared &&
            shared.totalHistoricalVolume !== null &&
            shared.totalHistoricalVolume !== undefined &&
            Number.isFinite(Number(shared.totalHistoricalVolume))
              ? Number(shared.totalHistoricalVolume)
              : null,
          volumeMethod:
            shared && shared.volumeMethod ? String(shared.volumeMethod) : "prices_rolling_24h",
          volumeSource: "/api/v1/info/prices:sum(volume_24h)",
          fetchedAt:
            shared && Number.isFinite(Number(shared.fetchedAt))
              ? Number(shared.fetchedAt)
              : Date.now(),
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
