function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function parseRateLimitHeader(raw) {
  if (!raw || typeof raw !== "string") return null;
  const matchR = raw.match(/r=(\d+)/);
  const matchT = raw.match(/t=(\d+)/);
  if (!matchR || !matchT) return null;
  return {
    remainingTenths: Number(matchR[1]),
    retryAfterSec: Number(matchT[1]),
  };
}

function parseRateLimitPolicyHeader(raw) {
  if (!raw || typeof raw !== "string") return null;
  const matchQ = raw.match(/q=(\d+)/);
  const matchW = raw.match(/w=(\d+)/);
  if (!matchQ || !matchW) return null;
  return {
    quotaTenths: Number(matchQ[1]),
    windowSec: Number(matchW[1]),
  };
}

function parseRetryAfterHeader(raw) {
  if (raw === null || raw === undefined) return null;
  const text = String(raw).trim();
  if (!text) return null;

  const secs = Number(text);
  if (Number.isFinite(secs) && secs >= 0) return Math.floor(secs * 1000);

  const dateMs = Date.parse(text);
  if (Number.isFinite(dateMs)) {
    return Math.max(0, dateMs - Date.now());
  }

  return null;
}

function createRateLimitGuard(options = {}) {
  const configuredCapacity = Math.max(0.1, Number(options.capacity || 300));
  const configuredRefillWindowMs = Math.max(
    5000,
    Number(options.refillWindowMs || 60000)
  );
  const state = {
    configuredCapacity,
    configuredRefillWindowMs,
    safetyMarginRatio: clamp(
      Number(options.safetyMarginRatio || 0.9),
      0.5,
      0.99
    ),
    observedCapacity: null,
    observedRefillWindowMs: null,
    tokens: configuredCapacity,
    lastRefillTs: Date.now(),
    serverHint: null,
    pauseUntilTs: 0,
    recentConsumes: [],
    totalConsumes: 0,
  };

  function getEffectiveCapacity() {
    const base =
      Number.isFinite(Number(state.observedCapacity)) && Number(state.observedCapacity) > 0
        ? Number(state.observedCapacity)
        : state.configuredCapacity;
    return Math.max(0.1, Number(base.toFixed(3)));
  }

  function getEffectiveRefillWindowMs() {
    const base =
      Number.isFinite(Number(state.observedRefillWindowMs)) &&
      Number(state.observedRefillWindowMs) >= 1000
        ? Number(state.observedRefillWindowMs)
        : state.configuredRefillWindowMs;
    return Math.max(1000, Math.round(base));
  }

  function trimRecentConsumes(maxAgeMs = 60000) {
    const cutoff = Date.now() - maxAgeMs;
    while (state.recentConsumes.length && state.recentConsumes[0].ts < cutoff) {
      state.recentConsumes.shift();
    }
  }

  function refill() {
    const now = Date.now();
    const elapsed = Math.max(0, now - state.lastRefillTs);
    const capacity = getEffectiveCapacity();
    const refillRate = capacity / getEffectiveRefillWindowMs();
    const refillAmount = elapsed * refillRate;
    if (refillAmount > 0) {
      state.tokens = Math.min(capacity, state.tokens + refillAmount);
      state.lastRefillTs = now;
    }
  }

  async function consume(cost = 1) {
    const target = Math.max(0.1, Number(cost || 1));

    while (true) {
      refill();
      trimRecentConsumes();

      const now = Date.now();
      if (state.pauseUntilTs > now) {
        await sleep(Math.min(2000, Math.max(25, state.pauseUntilTs - now)));
        continue;
      }

      if (state.tokens >= target) {
        state.tokens -= target;
        state.totalConsumes += target;
        state.recentConsumes.push({
          ts: now,
          cost: target,
        });
        return;
      }

      const refillRate = getEffectiveCapacity() / getEffectiveRefillWindowMs();
      const deficit = Math.max(0, target - state.tokens);
      const waitMs = Math.max(25, Math.ceil(deficit / refillRate));
      await sleep(Math.min(waitMs, 2000));
    }
  }

  function observeHeaders(headers) {
    if (!headers || typeof headers.get !== "function") return;
    const hint = parseRateLimitHeader(headers.get("ratelimit"));
    const policy = parseRateLimitPolicyHeader(headers.get("ratelimit-policy"));
    const retryAfterMs = parseRetryAfterHeader(headers.get("retry-after"));
    if (policy) {
      refill();
      const rawQuotaCredits = policy.quotaTenths / 10;
      state.observedCapacity = Math.max(
        0.1,
        Number((rawQuotaCredits * state.safetyMarginRatio).toFixed(3))
      );
      state.observedRefillWindowMs = Math.max(1000, policy.windowSec * 1000);
      state.tokens = Math.min(state.tokens, getEffectiveCapacity());
    }
    if (hint) {
      const remainingCredits = hint.remainingTenths / 10;
      const effectiveRemaining = Math.max(
        0,
        Number((remainingCredits * state.safetyMarginRatio).toFixed(3))
      );
      state.serverHint = {
        remainingCredits,
        effectiveRemainingCredits: effectiveRemaining,
        retryAfterSec: hint.retryAfterSec,
        observedAt: Date.now(),
        policy:
          policy && Number.isFinite(policy.quotaTenths) && Number.isFinite(policy.windowSec)
            ? {
                quotaCredits: policy.quotaTenths / 10,
                effectiveQuotaCredits: Math.max(
                  0.1,
                  Number(((policy.quotaTenths / 10) * state.safetyMarginRatio).toFixed(3))
                ),
                windowSec: policy.windowSec,
              }
            : null,
      };
      state.tokens = Math.min(state.tokens, effectiveRemaining);

      if (hint.retryAfterSec > 0 && hint.remainingTenths <= 0) {
        state.pauseUntilTs = Math.max(state.pauseUntilTs, Date.now() + hint.retryAfterSec * 1000);
      }
    }

    if (Number.isFinite(retryAfterMs) && retryAfterMs > 0) {
      state.pauseUntilTs = Math.max(state.pauseUntilTs, Date.now() + retryAfterMs);
    }
  }

  function getState() {
    refill();
    trimRecentConsumes();

    const now = Date.now();
    const used1m = state.recentConsumes.reduce((sum, row) => sum + Number(row.cost || 0), 0);
    const effectiveCapacity = getEffectiveCapacity();
    const effectiveRefillWindowMs = getEffectiveRefillWindowMs();
    const rpmCap = Number((effectiveCapacity * (60000 / effectiveRefillWindowMs)).toFixed(3));
    const configuredRpmCap = Number(
      (
        state.configuredCapacity *
        (60000 / Math.max(1000, state.configuredRefillWindowMs))
      ).toFixed(3)
    );
    const observedRpmCap =
      Number.isFinite(Number(state.observedCapacity)) && Number(state.observedCapacity) > 0
        ? Number((state.observedCapacity * (60000 / effectiveRefillWindowMs)).toFixed(3))
        : null;

    return {
      tokens: Number(state.tokens.toFixed(2)),
      capacity: effectiveCapacity,
      configuredCapacity: state.configuredCapacity,
      observedCapacity: state.observedCapacity,
      refillWindowMs: effectiveRefillWindowMs,
      configuredRefillWindowMs: state.configuredRefillWindowMs,
      observedRefillWindowMs: state.observedRefillWindowMs,
      rpmCap,
      configuredRpmCap,
      observedRpmCap,
      used1m: Number(used1m.toFixed(3)),
      pauseRemainingMs: Math.max(0, Number(state.pauseUntilTs || 0) - now),
      totalConsumes: Number(state.totalConsumes.toFixed(3)),
      safetyMarginRatio: state.safetyMarginRatio,
      serverHint: state.serverHint,
    };
  }

  return {
    consume,
    getState,
    observeHeaders,
  };
}

module.exports = {
  createRateLimitGuard,
};
