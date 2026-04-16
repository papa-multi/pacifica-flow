const {
  assignShardByKey,
  normalizeShardItems,
} = require("./sharding");

function createShardedSolanaRpcClient({ clients = [], logger = console } = {}) {
  const entries = normalizeShardItems(clients).filter(
    (entry) => entry && entry.client && typeof entry.client.getTransaction === "function"
  );
  if (!entries.length) {
    throw new Error("createShardedSolanaRpcClient requires at least one Solana RPC client.");
  }

  function getEntryStats(entry) {
    if (!entry || !entry.client || typeof entry.client.getStats !== "function") return null;
    try {
      return entry.client.getStats();
    } catch (_error) {
      return null;
    }
  }

  function computeEntryScore(entry, method, preferredId) {
    const stats = getEntryStats(entry);
    const methodStats =
      stats && stats.methods && Object.prototype.hasOwnProperty.call(stats.methods, method)
        ? stats.methods[method]
        : null;
    const methodConcurrency = Math.max(
      1,
      Number(
        (methodStats && (methodStats.concurrency || methodStats.concurrencyMax)) || 1
      )
    );
    const methodInFlight = Number((methodStats && methodStats.inFlight) || 0);
    const methodUtilization = Math.min(1, methodInFlight / methodConcurrency);
    const methodBackoffMs = Number((methodStats && methodStats.backoffRemainingMs) || 0);
    const method429 = Number((methodStats && methodStats.rate429PerMin) || 0);
    const methodRps = Number((methodStats && methodStats.rps1m) || 0);
    const global429 = Number((stats && stats.rate429PerMin) || 0);
    const globalRps = Number((stats && stats.rps1m) || 0);
    const preferredPenalty =
      entry.id === preferredId
        ? 0
        : method === "getSignaturesForAddress"
        ? 0.75
        : method === "getTransaction"
        ? 2.5
        : 1.5;
    return (
      preferredPenalty +
      methodBackoffMs / 1000 +
      methodInFlight * 2 +
      methodUtilization * 6 +
      method429 * 20 +
      global429 * 10 +
      methodRps * 1.2 +
      globalRps * 0.75
    );
  }

  function pickClient(partitionKey, excludeIds = null, method = "getTransaction") {
    const pool =
      excludeIds && excludeIds.size
        ? entries.filter((entry) => !excludeIds.has(entry.id))
        : entries;
    if (!pool.length) return null;
    const assigned = assignShardByKey(String(partitionKey || "default"), pool);
    const preferredId = assigned && assigned.item ? assigned.item.id : pool[0].id;
    return pool
      .slice()
      .sort((a, b) => {
        const delta = computeEntryScore(a, method, preferredId) - computeEntryScore(b, method, preferredId);
        if (delta !== 0) return delta;
        if (a.id === preferredId) return -1;
        if (b.id === preferredId) return 1;
        return String(a.id).localeCompare(String(b.id));
      })[0];
  }

  async function call(method, args = [], options = {}) {
    const partitionKey = String(options.partitionKey || method || "default");
    const excludeIds = new Set();
    let attempts = 0;
    const maxAttempts = Math.max(
      1,
      Math.min(entries.length, Number(options.maxAttempts || entries.length))
    );
    let lastError = null;

    while (attempts < maxAttempts) {
      attempts += 1;
      const entry = pickClient(`${method}:${partitionKey}`, excludeIds, method);
      if (!entry) break;
      excludeIds.add(entry.id);
      try {
        return await entry.client[method](...args);
      } catch (error) {
        lastError = error;
        logger.warn(
          `[solana-rpc-pool] method=${method} shard=${entry.id} attempt=${attempts}/${maxAttempts} reason=${
            error && error.message ? error.message : "unknown_error"
          }`
        );
      }
    }

    throw lastError || new Error(`RPC ${method} failed: no available client`);
  }

  function getSignaturesForAddress(address, options = {}) {
    return call("getSignaturesForAddress", [address, options], {
      partitionKey: options.partitionKey || `signatures:${address}`,
      maxAttempts: options.maxAttempts,
    });
  }

  function getTransaction(signature, options = {}) {
    return call("getTransaction", [signature, options], {
      partitionKey: options.partitionKey || `transaction:${signature}`,
      maxAttempts: options.maxAttempts,
    });
  }

  function getAccountInfo(address, options = {}) {
    return call("getAccountInfo", [address, options], {
      partitionKey: options.partitionKey || `account:${address}`,
      maxAttempts: options.maxAttempts,
    });
  }

  function getProgramAccounts(programId, options = {}) {
    return call("getProgramAccounts", [programId, options], {
      partitionKey: options.partitionKey || `program:${programId}`,
      maxAttempts: options.maxAttempts,
    });
  }

  function getStats() {
    return {
      mode: "sharded_pool",
      shardCount: entries.length,
      shards: entries.map((entry, idx) => ({
        id: entry.id,
        index: idx,
        stats:
          entry.client && typeof entry.client.getStats === "function"
            ? entry.client.getStats()
            : null,
      })),
    };
  }

  return {
    getSignaturesForAddress,
    getTransaction,
    getAccountInfo,
    getProgramAccounts,
    getStats,
  };
}

module.exports = {
  createShardedSolanaRpcClient,
};
