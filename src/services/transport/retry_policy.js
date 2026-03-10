class RetryableError extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = "RetryableError";
    this.details = details;
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function createRetryPolicy(options = {}) {
  const maxAttempts = Math.max(1, Number(options.maxAttempts || 3));
  const baseDelayMs = Math.max(25, Number(options.baseDelayMs || 200));
  const maxDelayMs = Math.max(baseDelayMs, Number(options.maxDelayMs || 2000));
  const jitterRatio = Math.max(0, Number(options.jitterRatio || 0));

  async function execute(fn, context = {}) {
    const effectiveMaxAttempts = Math.max(
      1,
      Number(context && context.maxAttempts ? context.maxAttempts : maxAttempts)
    );
    let attempt = 0;
    let lastError = null;

    while (attempt < effectiveMaxAttempts) {
      attempt += 1;
      try {
        return await fn(attempt);
      } catch (error) {
        lastError = error;
        const retryable = error instanceof RetryableError || Boolean(error.retryable);
        if (!retryable || attempt >= effectiveMaxAttempts) throw error;

        const customDelay = Number(
          error instanceof RetryableError && error.details
            ? error.details.retryDelayMs
            : NaN
        );
        let delay = Number.isFinite(customDelay)
          ? Math.max(0, customDelay)
          : Math.min(maxDelayMs, baseDelayMs * 2 ** (attempt - 1));

        if (jitterRatio > 0) {
          const jitter = delay * jitterRatio * Math.random();
          delay += jitter;
        }
        delay = Math.floor(delay);

        if (typeof context.onRetry === "function") {
          context.onRetry({ attempt, delay, error });
        }
        await sleep(delay);
      }
    }

    throw lastError;
  }

  return {
    execute,
  };
}

module.exports = {
  RetryableError,
  createRetryPolicy,
};
