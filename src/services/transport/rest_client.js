const { execFile } = require("child_process");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { RetryableError } = require("./retry_policy");

let UndiciProxyAgent = null;
try {
  ({ ProxyAgent: UndiciProxyAgent } = require("undici"));
} catch (_error) {
  UndiciProxyAgent = null;
}

const proxyDispatcherCache = new Map();

function buildUrl(baseUrl, pathname, query = null) {
  const normalizedPath = String(pathname || "").replace(/^\/+/, "");
  const url = new URL(
    normalizedPath,
    baseUrl.endsWith("/") ? baseUrl : `${baseUrl}/`
  );
  if (query && typeof query === "object") {
    Object.entries(query).forEach(([key, value]) => {
      if (value === undefined || value === null || value === "") return;
      url.searchParams.set(key, String(value));
    });
  }
  return url;
}

function shouldRetryStatus(status) {
  return [408, 409, 425, 429, 500, 502, 503, 504].includes(status);
}

function parseRetryDelayMsFromHeaders(headers) {
  if (!headers || typeof headers.get !== "function") return null;

  const retryAfterRaw = headers.get("retry-after");
  if (retryAfterRaw) {
    const retryAfterSec = Number(String(retryAfterRaw).trim());
    if (Number.isFinite(retryAfterSec) && retryAfterSec >= 0) {
      return Math.floor(retryAfterSec * 1000);
    }
  }

  const rateLimitRaw = headers.get("ratelimit");
  if (rateLimitRaw && typeof rateLimitRaw === "string") {
    const matchT = rateLimitRaw.match(/t=(\d+)/);
    if (matchT) {
      const retrySec = Number(matchT[1]);
      if (Number.isFinite(retrySec) && retrySec >= 0) {
        return Math.floor(retrySec * 1000);
      }
    }
  }

  return null;
}

function buildHeaderAccessor(raw = "") {
  const source = String(raw || "");
  const blocks = [];
  let currentBlock = [];

  source.split(/\r?\n/).forEach((line) => {
    if (/^HTTP\/\d/i.test(line)) {
      if (currentBlock.length) blocks.push(currentBlock);
      currentBlock = [line];
      return;
    }
    if (!currentBlock.length) return;
    currentBlock.push(line);
  });
  if (currentBlock.length) blocks.push(currentBlock);

  const selected = blocks.length ? blocks[blocks.length - 1] : [];
  const map = new Map();
  selected.slice(1).forEach((line) => {
    const idx = line.indexOf(":");
    if (idx <= 0) return;
    const key = line.slice(0, idx).trim().toLowerCase();
    const value = line.slice(idx + 1).trim();
    if (!key) return;
    if (map.has(key)) {
      map.set(key, `${map.get(key)}, ${value}`);
    } else {
      map.set(key, value);
    }
  });

  return {
    get(name) {
      return map.get(String(name || "").trim().toLowerCase()) || null;
    },
  };
}

function runCurlRequest({
  method,
  url,
  headers,
  body,
  timeoutMs,
  proxyUrl,
}) {
  const timeoutSec = Math.max(1, Math.ceil(timeoutMs / 1000));
  const marker = `__PF_META__${Date.now()}_${Math.random().toString(16).slice(2, 8)}`;
  const headerPath = path.join(
    os.tmpdir(),
    `pf-curl-${process.pid}-${Date.now()}-${Math.random().toString(16).slice(2)}.headers`
  );
  const args = [
    "--silent",
    "--show-error",
    "--location",
    "--max-time",
    String(timeoutSec),
    "--request",
    method,
    "--dump-header",
    headerPath,
  ];

  if (proxyUrl) {
    args.push("--proxy", String(proxyUrl));
  }

  Object.entries(headers || {}).forEach(([key, value]) => {
    if (value === undefined || value === null) return;
    args.push("--header", `${key}: ${value}`);
  });

  if (body !== undefined && body !== null) {
    args.push("--data-raw", String(body));
  }

  args.push("--write-out", `\\n${marker}:%{http_code}`, String(url));

  return new Promise((resolve, reject) => {
    execFile(
      "curl",
      args,
      {
        maxBuffer: 8 * 1024 * 1024,
      },
      (error, stdout, stderr) => {
        const rawHeaders = (() => {
          try {
            if (!fs.existsSync(headerPath)) return "";
            return fs.readFileSync(headerPath, "utf8");
          } catch (_error) {
            return "";
          } finally {
            try {
              if (fs.existsSync(headerPath)) fs.unlinkSync(headerPath);
            } catch (_error) {
              // Best effort cleanup only.
            }
          }
        })();
        if (error) {
          const message = String(stderr || error.message || "curl_error")
            .trim()
            .slice(0, 300);
          const wrapped = new Error(`curl_request_failed: ${message}`);
          wrapped.cause = error;
          reject(wrapped);
          return;
        }

        const output = String(stdout || "");
        const markerToken = `\n${marker}:`;
        const markerPos = output.lastIndexOf(markerToken);
        if (markerPos < 0) {
          reject(new Error("curl_response_missing_status_marker"));
          return;
        }

        const bodyText = output.slice(0, markerPos);
        const metaLine = output.slice(markerPos + 1).trim();
        const statusCode = Number(String(metaLine).split(":")[1] || 0);

        resolve({
          statusCode: Number.isFinite(statusCode) ? statusCode : 0,
          bodyText,
          headers: buildHeaderAccessor(rawHeaders),
        });
      }
    );
  });
}

function getFetchDispatcher(proxyUrl) {
  const raw = String(proxyUrl || "").trim();
  const normalized = raw && /^[a-z]+:\/\//i.test(raw) ? raw : raw ? `http://${raw}` : "";
  if (!normalized || !UndiciProxyAgent) return null;
  if (!proxyDispatcherCache.has(normalized)) {
    proxyDispatcherCache.set(
      normalized,
      new UndiciProxyAgent({
        uri: normalized,
        connect: {
          timeout: 10_000,
        },
      })
    );
  }
  return proxyDispatcherCache.get(normalized) || null;
}

function createRestClient({
  baseUrl,
  retryPolicy,
  rateLimitGuard,
  clockSync,
  defaultHeaders = {},
  defaultTimeoutMs = 15000,
  logger = console,
  transport = "fetch",
  proxyUrl = "",
  clientId = "default",
}) {
  const fetchFn = global.fetch;
  const requestedTransport =
    String(transport || "fetch").toLowerCase() === "curl" ? "curl" : "fetch";
  const normalizedTransport =
    requestedTransport === "fetch" && proxyUrl && !UndiciProxyAgent ? "curl" : requestedTransport;
  if (normalizedTransport === "fetch" && typeof fetchFn !== "function") {
    throw new Error("Global fetch is required (Node 18+).");
  }
  const fetchDispatcher =
    normalizedTransport === "fetch" ? getFetchDispatcher(proxyUrl) : null;

  async function request(method, pathname, options = {}) {
    const cost = Number(options.cost || 1);
    await rateLimitGuard.consume(cost);

    const timeoutMs = Math.max(1000, Number(options.timeoutMs || defaultTimeoutMs));
    const headers = {
      Accept: "application/json",
      ...(defaultHeaders || {}),
      ...(options.headers || {}),
    };

    const url = buildUrl(baseUrl, pathname, options.query);

    return retryPolicy.execute(
      async () => {
        try {
          let status = 0;
          let responseHeaders = null;
          let text = "";

          if (normalizedTransport === "curl") {
            const body = options.body ? JSON.stringify(options.body) : null;
            const curlRes = await runCurlRequest({
              method,
              url: url.toString(),
              headers,
              body,
              timeoutMs,
              proxyUrl,
            });
            status = Number(curlRes.statusCode || 0);
            text = String(curlRes.bodyText || "");
            responseHeaders = curlRes.headers || null;
          } else {
            const controller = new AbortController();
            const timer = setTimeout(() => controller.abort(), timeoutMs);
            try {
              const res = await fetchFn(url, {
                method,
                headers,
                signal: controller.signal,
                body: options.body ? JSON.stringify(options.body) : undefined,
                dispatcher: fetchDispatcher || undefined,
              });
              status = Number(res.status || 0);
              responseHeaders = res.headers;
              text = await res.text();
            } finally {
              clearTimeout(timer);
            }
          }

          if (responseHeaders && typeof responseHeaders.get === "function") {
            clockSync.observeServerDate(responseHeaders.get("date"));
            rateLimitGuard.observeHeaders(responseHeaders);
          }

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

          const isOk = status >= 200 && status < 300;
          if (!Number.isFinite(status) || status <= 0) {
            throw new RetryableError(
              `REST ${method} ${url.pathname} failed with invalid status ${status}`
            );
          }
          if (!isOk) {
            const bodyHint =
              typeof payload === "string"
                ? ` body=${payload.slice(0, 180)}`
                : payload && typeof payload === "object" && payload.error
                ? ` error=${payload.error}`
                : "";
            const error = new Error(
              `REST ${method} ${url.pathname} failed with ${status}${bodyHint}`
            );
            error.status = status;
            error.payload = payload;
            if (shouldRetryStatus(status)) {
              const retryDelayMs = status === 429 && responseHeaders
                ? parseRetryDelayMsFromHeaders(responseHeaders)
                : null;
              throw new RetryableError(error.message, {
                cause: error,
                retryDelayMs: Number.isFinite(retryDelayMs) ? retryDelayMs : undefined,
              });
            }
            throw error;
          }

          if (parseError) {
            const syntaxError = new Error(
              `Non-JSON success response for ${method} ${url.pathname}: ${String(payload).slice(0, 180)}`
            );
            syntaxError.status = status;
            syntaxError.payload = payload;
            throw syntaxError;
          }

          return {
            status,
            headers: responseHeaders,
            payload,
          };
        } catch (error) {
          if (error.name === "AbortError") {
            throw new RetryableError(`REST timeout for ${method} ${url.pathname}`);
          }
          if (error instanceof RetryableError) throw error;
          if (error.status && shouldRetryStatus(error.status)) {
            throw new RetryableError(error.message, { cause: error });
          }
          throw error;
        }
      },
      {
        maxAttempts: Math.max(
          1,
          Number(options.retryMaxAttempts || options.maxAttempts || NaN) || undefined
        ),
        onRetry: ({ attempt, delay, error }) => {
          logger.warn(
            `[rest-client:${clientId}] retry attempt=${attempt} delay_ms=${delay} reason=${error.message}`
          );
        },
      }
    );
  }

  async function get(pathname, options = {}) {
    return request("GET", pathname, options);
  }

  async function post(pathname, options = {}) {
    return request("POST", pathname, {
      ...options,
      headers: {
        "Content-Type": "application/json",
        ...(options.headers || {}),
      },
    });
  }

  return {
    get,
    post,
    request,
    meta: {
      transport: normalizedTransport,
      proxyUrl: proxyUrl || null,
      clientId,
    },
  };
}

module.exports = {
  createRestClient,
};
