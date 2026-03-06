const { RetryableError } = require("../transport/retry_policy");

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function shouldRetryStatus(status) {
  return [408, 409, 425, 429, 500, 502, 503, 504].includes(Number(status));
}

function shouldRetryRpcError(error) {
  if (!error || typeof error !== "object") return false;
  const code = Number(error.code);
  return [
    -32004, // Block not available
    -32005, // Node behind / rate limit
    -32007, // Slot skipped / unavailable
    -32009,
    -32014,
    -32603, // Internal error
  ].includes(code);
}

function createTokenBucket(options = {}) {
  const state = {
    capacity: Math.max(0.1, Number(options.capacity || 1)),
    refillPerSec: Math.max(0.01, Number(options.refillPerSec || 1)),
    tokens: Math.max(0.1, Number(options.capacity || 1)),
    lastRefillTs: Date.now(),
  };

  function refill() {
    const now = Date.now();
    const elapsedMs = Math.max(0, now - state.lastRefillTs);
    if (!elapsedMs) return;

    const refill = (elapsedMs / 1000) * state.refillPerSec;
    state.tokens = Math.min(state.capacity, state.tokens + refill);
    state.lastRefillTs = now;
  }

  async function consume(cost = 1) {
    const target = Math.max(0.1, Number(cost || 1));

    while (true) {
      refill();
      if (state.tokens >= target) {
        state.tokens -= target;
        return;
      }

      const deficit = Math.max(0, target - state.tokens);
      const waitMs = Math.max(25, Math.ceil((deficit / state.refillPerSec) * 1000));
      await sleep(Math.min(waitMs, 2000));
    }
  }

  function getState() {
    refill();
    return {
      capacity: state.capacity,
      refillPerSec: state.refillPerSec,
      tokens: Number(state.tokens.toFixed(3)),
    };
  }

  return {
    consume,
    getState,
  };
}

function createSolanaRpcClient({
  rpcUrl,
  retryPolicy,
  rateLimitGuard,
  defaultHeaders = {},
  defaultTimeoutMs = 20000,
  logger = console,
  adaptive = {},
}) {
  const fetchFn = global.fetch;
  if (typeof fetchFn !== "function") {
    throw new Error("Global fetch is required (Node 18+).");
  }

  if (!rpcUrl) {
    throw new Error("SOLANA RPC URL is required");
  }

  const cfg = {
    methodProfiles: {
      getSignaturesForAddress: {
        bucketCapacity: Number(adaptive.sigBucketCapacity || 1),
        bucketRefillPerSec: Number(adaptive.sigRefillPerSec || 0.35),
        minConcurrency: 1,
        maxConcurrency: Math.max(1, Number(adaptive.sigMaxConcurrency || 1)),
        initialConcurrency: Math.max(1, Number(adaptive.sigInitialConcurrency || 1)),
        minDelayMs: Math.max(50, Number(adaptive.sigMinDelayMs || 700)),
        maxDelayMs: Math.max(1000, Number(adaptive.sigMaxDelayMs || 30000)),
      },
      getTransaction: {
        bucketCapacity: Number(adaptive.txBucketCapacity || 1),
        bucketRefillPerSec: Number(adaptive.txRefillPerSec || 0.5),
        minConcurrency: 1,
        maxConcurrency: Math.max(1, Number(adaptive.txMaxConcurrency || 2)),
        initialConcurrency: Math.max(1, Number(adaptive.txInitialConcurrency || 1)),
        minDelayMs: Math.max(50, Number(adaptive.txMinDelayMs || 500)),
        maxDelayMs: Math.max(1000, Number(adaptive.txMaxDelayMs || 30000)),
      },
      default: {
        bucketCapacity: Number(adaptive.defaultBucketCapacity || 1),
        bucketRefillPerSec: Number(adaptive.defaultRefillPerSec || 0.3),
        minConcurrency: 1,
        maxConcurrency: 1,
        initialConcurrency: 1,
        minDelayMs: Math.max(50, Number(adaptive.defaultMinDelayMs || 800)),
        maxDelayMs: Math.max(1000, Number(adaptive.defaultMaxDelayMs || 30000)),
      },
    },
    globalBucketCapacity: Math.max(0.1, Number(adaptive.globalBucketCapacity || 1.5)),
    globalRefillPerSec: Math.max(0.01, Number(adaptive.globalRefillPerSec || 0.8)),
    base429DelayMs: Math.max(500, Number(adaptive.base429DelayMs || 2500)),
    max429DelayMs: Math.max(5000, Number(adaptive.max429DelayMs || 120000)),
    rampQuietMs: Math.max(15000, Number(adaptive.rampQuietMs || 180000)),
    rampStepMs: Math.max(5000, Number(adaptive.rampStepMs || 60000)),
    cacheMaxEntries: Math.max(1000, Number(adaptive.cacheMaxEntries || 25000)),
    signaturesCacheTtlMs: Math.max(0, Number(adaptive.signaturesCacheTtlMs || 10000)),
    transactionCacheTtlMs: Math.max(0, Number(adaptive.transactionCacheTtlMs || 6 * 60 * 60 * 1000)),
    nullTxCacheTtlMs: Math.max(0, Number(adaptive.nullTxCacheTtlMs || 30000)),
  };

  const globalBucket = createTokenBucket({
    capacity: cfg.globalBucketCapacity,
    refillPerSec: cfg.globalRefillPerSec,
  });

  let requestId = 1;

  const stats = {
    startedAt: Date.now(),
    totalRequests: 0,
    totalSuccesses: 0,
    totalErrors: 0,
    total429: 0,
    totalRetries: 0,
    recentRequests: [],
    recent429: [],
    cacheHits: 0,
    cacheMisses: 0,
  };

  const methodStates = new Map();
  const cache = new Map();

  function cleanupTimestamps(list, maxAgeMs = 60000) {
    const cutoff = Date.now() - maxAgeMs;
    while (list.length && list[0] < cutoff) {
      list.shift();
    }
  }

  function getMethodProfile(name) {
    return cfg.methodProfiles[name] || cfg.methodProfiles.default;
  }

  function getMethodState(name) {
    if (methodStates.has(name)) return methodStates.get(name);

    const profile = getMethodProfile(name);
    const state = {
      method: name,
      profile,
      bucket: createTokenBucket({
        capacity: profile.bucketCapacity,
        refillPerSec: profile.bucketRefillPerSec,
      }),
      minConcurrency: Math.max(1, Number(profile.minConcurrency || 1)),
      maxConcurrency: Math.max(1, Number(profile.maxConcurrency || 1)),
      currentConcurrency: Math.max(1, Number(profile.initialConcurrency || 1)),
      inFlight: 0,
      delayMs: Number(profile.minDelayMs || 250),
      minDelayMs: Number(profile.minDelayMs || 250),
      maxDelayMs: Number(profile.maxDelayMs || 30000),
      backoffUntil: 0,
      nextAllowedAt: 0,
      consecutive429: 0,
      last429At: 0,
      lastBackoffDelayMs: 0,
      lastSuccessAt: 0,
      lastRampAt: 0,
      totalRequests: 0,
      totalSuccesses: 0,
      totalErrors: 0,
      total429: 0,
      totalRetries: 0,
      recentRequests: [],
      recent429: [],
    };

    state.currentConcurrency = clamp(
      state.currentConcurrency,
      state.minConcurrency,
      state.maxConcurrency
    );

    methodStates.set(name, state);
    return state;
  }

  function cacheKey(method, params) {
    return `${method}:${JSON.stringify(params)}`;
  }

  function pruneCache() {
    if (cache.size <= cfg.cacheMaxEntries) return;

    const removeCount = cache.size - cfg.cacheMaxEntries;
    let removed = 0;
    for (const key of cache.keys()) {
      cache.delete(key);
      removed += 1;
      if (removed >= removeCount) break;
    }
  }

  function getCached(key) {
    if (!cache.has(key)) {
      stats.cacheMisses += 1;
      return { hit: false, value: null };
    }

    const entry = cache.get(key);
    if (!entry || Date.now() >= entry.expiresAt) {
      cache.delete(key);
      stats.cacheMisses += 1;
      return { hit: false, value: null };
    }

    stats.cacheHits += 1;
    entry.lastAccessAt = Date.now();
    return {
      hit: true,
      value: entry.value,
    };
  }

  function setCached(key, value, ttlMs) {
    if (!Number.isFinite(ttlMs) || ttlMs <= 0) return;

    cache.set(key, {
      value,
      expiresAt: Date.now() + ttlMs,
      createdAt: Date.now(),
      lastAccessAt: Date.now(),
    });
    pruneCache();
  }

  function recordAttempt(methodState) {
    const now = Date.now();
    stats.totalRequests += 1;
    methodState.totalRequests += 1;
    stats.recentRequests.push(now);
    methodState.recentRequests.push(now);
  }

  function recordSuccess(methodState) {
    const now = Date.now();
    stats.totalSuccesses += 1;
    methodState.totalSuccesses += 1;
    methodState.lastSuccessAt = now;

    if (methodState.consecutive429 > 0) {
      methodState.consecutive429 = Math.max(0, methodState.consecutive429 - 1);
    }

    if (methodState.delayMs > methodState.minDelayMs) {
      methodState.delayMs = Math.max(
        methodState.minDelayMs,
        Math.floor(methodState.delayMs * 0.92)
      );
    }

    const last429At = Number(methodState.last429At || 0);
    if (
      methodState.currentConcurrency < methodState.maxConcurrency &&
      now - last429At > cfg.rampQuietMs &&
      now - Number(methodState.lastRampAt || 0) > cfg.rampStepMs
    ) {
      methodState.currentConcurrency += 1;
      methodState.currentConcurrency = clamp(
        methodState.currentConcurrency,
        methodState.minConcurrency,
        methodState.maxConcurrency
      );
      methodState.lastRampAt = now;
    }
  }

  function recordError(methodState) {
    stats.totalErrors += 1;
    methodState.totalErrors += 1;
  }

  function on429(methodState, attempt = 1) {
    const now = Date.now();

    stats.total429 += 1;
    methodState.total429 += 1;
    stats.recent429.push(now);
    methodState.recent429.push(now);

    methodState.last429At = now;
    methodState.consecutive429 = Math.min(12, methodState.consecutive429 + 1);

    const prevConcurrency = methodState.currentConcurrency;
    methodState.currentConcurrency = Math.max(
      methodState.minConcurrency,
      Math.floor(methodState.currentConcurrency / 2) || 1
    );

    methodState.delayMs = Math.min(
      methodState.maxDelayMs,
      Math.max(methodState.minDelayMs * 2, Math.ceil(methodState.delayMs * 1.8))
    );

    const exp = Math.min(12, methodState.consecutive429 + Math.max(0, attempt - 1));
    const rawBackoff = Math.min(cfg.max429DelayMs, cfg.base429DelayMs * 2 ** (exp - 1));
    const jitter = Math.floor(Math.random() * Math.max(350, rawBackoff * 0.35));
    const wait = rawBackoff + jitter;

    methodState.lastBackoffDelayMs = wait;
    methodState.backoffUntil = Math.max(methodState.backoffUntil, now + wait);

    if (prevConcurrency !== methodState.currentConcurrency) {
      logger.warn(
        `[solana-rpc] 429 method=${methodState.method} concurrency ${prevConcurrency}->${methodState.currentConcurrency} delay_ms=${methodState.delayMs} backoff_ms=${wait}`
      );
    }

    return wait;
  }

  async function acquirePermit(method, cost = 1) {
    const methodState = getMethodState(method);

    await globalBucket.consume(cost);
    await methodState.bucket.consume(cost);

    while (true) {
      const now = Date.now();
      let waitMs = 0;

      if (now < methodState.backoffUntil) {
        waitMs = Math.max(waitMs, methodState.backoffUntil - now);
      }

      if (now < methodState.nextAllowedAt) {
        waitMs = Math.max(waitMs, methodState.nextAllowedAt - now);
      }

      if (methodState.inFlight >= methodState.currentConcurrency) {
        waitMs = Math.max(waitMs, 35);
      }

      if (waitMs <= 0) break;
      await sleep(Math.min(waitMs, 2000));
    }

    methodState.inFlight += 1;
    methodState.nextAllowedAt = Math.max(methodState.nextAllowedAt, Date.now()) + methodState.delayMs;

    const release = () => {
      methodState.inFlight = Math.max(0, methodState.inFlight - 1);
    };

    return {
      methodState,
      release,
    };
  }

  async function rpcCall(method, params = [], options = {}) {
    const timeoutMs = Math.max(1000, Number(options.timeoutMs || defaultTimeoutMs));
    const cost = Math.max(0.1, Number(options.cost || 1));

    const ttl = Number.isFinite(Number(options.cacheTtlMs)) ? Number(options.cacheTtlMs) : 0;
    const key = options.disableCache ? null : cacheKey(method, params);

    if (key && ttl > 0) {
      const cached = getCached(key);
      if (cached.hit) return cached.value;
    }

    return retryPolicy.execute(
      async (attempt) => {
        const permit = await acquirePermit(method, cost);
        const methodState = permit.methodState;
        const release = permit.release;

        recordAttempt(methodState);

        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), timeoutMs);

        try {
          const body = {
            jsonrpc: "2.0",
            id: requestId += 1,
            method,
            params,
          };

          const res = await fetchFn(rpcUrl, {
            method: "POST",
            headers: {
              Accept: "application/json",
              "Content-Type": "application/json",
              ...(defaultHeaders || {}),
            },
            body: JSON.stringify(body),
            signal: controller.signal,
          });

          clearTimeout(timer);
          rateLimitGuard.observeHeaders(res.headers);

          const text = await res.text();
          let payload = null;
          let parseError = null;

          if (text) {
            try {
              payload = JSON.parse(text);
            } catch (error) {
              parseError = error;
              payload = text;
            }
          }

          if (!res.ok) {
            const bodyHint =
              typeof payload === "string"
                ? payload.slice(0, 180)
                : payload && typeof payload === "object" && payload.error
                ? payload.error.message || JSON.stringify(payload.error)
                : "";

            const message = `RPC ${method} failed with HTTP ${res.status}${
              bodyHint ? ` body=${bodyHint}` : ""
            }`;

            recordError(methodState);

            if (Number(res.status) === 429) {
              const backoff = on429(methodState, attempt);
              throw new RetryableError(message, {
                retryDelayMs: backoff,
                is429: true,
              });
            }

            if (shouldRetryStatus(res.status)) {
              throw new RetryableError(message);
            }

            const error = new Error(message);
            error.status = res.status;
            error.payload = payload;
            throw error;
          }

          if (parseError) {
            recordError(methodState);
            throw new Error(
              `RPC ${method} returned non-JSON success response: ${String(payload).slice(0, 180)}`
            );
          }

          if (payload && payload.error) {
            const rpcError = payload.error;
            const message = `RPC ${method} error code=${rpcError.code} message=${rpcError.message || "unknown"}`;

            recordError(methodState);

            if (Number(rpcError.code) === -32005) {
              const backoff = on429(methodState, attempt);
              throw new RetryableError(message, {
                retryDelayMs: backoff,
                is429: true,
              });
            }

            if (shouldRetryRpcError(rpcError)) {
              throw new RetryableError(message);
            }

            const error = new Error(message);
            error.code = rpcError.code;
            error.data = rpcError.data;
            throw error;
          }

          recordSuccess(methodState);
          cleanupTimestamps(stats.recentRequests);
          cleanupTimestamps(methodState.recentRequests);
          cleanupTimestamps(stats.recent429);
          cleanupTimestamps(methodState.recent429);

          const result = payload ? payload.result : null;

          if (key && ttl > 0) {
            setCached(key, result, ttl);
          }

          return result;
        } catch (error) {
          clearTimeout(timer);

          if (error.name === "AbortError") {
            recordError(methodState);
            throw new RetryableError(`RPC ${method} timeout`);
          }

          if (error instanceof RetryableError) {
            throw error;
          }

          if (error.status && shouldRetryStatus(error.status)) {
            throw new RetryableError(error.message);
          }

          throw error;
        } finally {
          release();
        }
      },
      {
        onRetry: ({ attempt, delay, error }) => {
          const methodState = getMethodState(method);
          stats.totalRetries += 1;
          methodState.totalRetries += 1;
          logger.warn(
            `[solana-rpc] retry method=${method} attempt=${attempt} delay_ms=${delay} reason=${error.message}`
          );
        },
      }
    );
  }

  async function getSignaturesForAddress(address, options = {}) {
    const config = {
      commitment: options.commitment || "confirmed",
      limit: Math.max(1, Math.min(1000, Number(options.limit || 1000))),
    };

    if (options.before) config.before = String(options.before);
    if (options.until) config.until = String(options.until);
    if (Number.isFinite(Number(options.minContextSlot))) {
      config.minContextSlot = Number(options.minContextSlot);
    }

    const result = await rpcCall("getSignaturesForAddress", [String(address), config], {
      cost: Number(options.cost || 1),
      timeoutMs: options.timeoutMs,
      cacheTtlMs:
        options.cacheTtlMs !== undefined ? Number(options.cacheTtlMs) : cfg.signaturesCacheTtlMs,
      disableCache: Boolean(options.disableCache),
    });

    return Array.isArray(result) ? result : [];
  }

  async function getTransaction(signature, options = {}) {
    const config = {
      encoding: options.encoding || "jsonParsed",
      commitment: options.commitment || "confirmed",
      maxSupportedTransactionVersion:
        options.maxSupportedTransactionVersion !== undefined
          ? options.maxSupportedTransactionVersion
          : 0,
    };

    const result = await rpcCall("getTransaction", [String(signature), config], {
      cost: Number(options.cost || 1),
      timeoutMs: options.timeoutMs,
      cacheTtlMs:
        options.cacheTtlMs !== undefined
          ? Number(options.cacheTtlMs)
          : cfg.transactionCacheTtlMs,
      disableCache: Boolean(options.disableCache),
    });

    if (!result) {
      const key = cacheKey("getTransaction", [String(signature), config]);
      setCached(key, result, cfg.nullTxCacheTtlMs);
    }

    return result;
  }

  async function getAccountInfo(address, options = {}) {
    const config = {
      encoding: options.encoding || "jsonParsed",
      commitment: options.commitment || "confirmed",
    };

    return rpcCall("getAccountInfo", [String(address), config], {
      cost: Number(options.cost || 1),
      timeoutMs: options.timeoutMs,
      cacheTtlMs: options.cacheTtlMs !== undefined ? Number(options.cacheTtlMs) : 60000,
      disableCache: Boolean(options.disableCache),
    });
  }

  async function getProgramAccounts(programId, options = {}) {
    const cfgReq = {
      commitment: options.commitment || "confirmed",
      encoding: options.encoding || "jsonParsed",
      filters: Array.isArray(options.filters) ? options.filters : undefined,
      withContext: Boolean(options.withContext),
    };

    if (cfgReq.filters === undefined) {
      delete cfgReq.filters;
    }

    return rpcCall("getProgramAccounts", [String(programId), cfgReq], {
      cost: Number(options.cost || 3),
      timeoutMs: options.timeoutMs,
      cacheTtlMs: options.cacheTtlMs !== undefined ? Number(options.cacheTtlMs) : 60000,
      disableCache: Boolean(options.disableCache),
    });
  }

  function getStats() {
    const now = Date.now();
    cleanupTimestamps(stats.recentRequests);
    cleanupTimestamps(stats.recent429);

    const methods = {};
    methodStates.forEach((state, method) => {
      cleanupTimestamps(state.recentRequests);
      cleanupTimestamps(state.recent429);
      methods[method] = {
        inFlight: state.inFlight,
        concurrency: state.currentConcurrency,
        concurrencyMin: state.minConcurrency,
        concurrencyMax: state.maxConcurrency,
        delayMs: state.delayMs,
        backoffRemainingMs: Math.max(0, Number(state.backoffUntil || 0) - now),
        lastBackoffDelayMs: state.lastBackoffDelayMs,
        totalRequests: state.totalRequests,
        totalSuccesses: state.totalSuccesses,
        totalErrors: state.totalErrors,
        totalRetries: state.totalRetries,
        total429: state.total429,
        rps1m: Number((state.recentRequests.length / 60).toFixed(3)),
        rate429PerMin: state.recent429.length,
        bucket: state.bucket.getState(),
      };
    });

    return {
      uptimeMs: now - stats.startedAt,
      totalRequests: stats.totalRequests,
      totalSuccesses: stats.totalSuccesses,
      totalErrors: stats.totalErrors,
      totalRetries: stats.totalRetries,
      total429: stats.total429,
      rps1m: Number((stats.recentRequests.length / 60).toFixed(3)),
      rate429PerMin: stats.recent429.length,
      cache: {
        size: cache.size,
        maxEntries: cfg.cacheMaxEntries,
        hits: stats.cacheHits,
        misses: stats.cacheMisses,
      },
      globalBucket: globalBucket.getState(),
      methods,
    };
  }

  return {
    rpcCall,
    getAccountInfo,
    getProgramAccounts,
    getSignaturesForAddress,
    getTransaction,
    getStats,
  };
}

module.exports = {
  createSolanaRpcClient,
};
