#!/usr/bin/env node
/* eslint-disable no-console */
const fs = require("fs");
const path = require("path");
const { execFile } = require("child_process");

function parseArgs(argv) {
  const out = {
    file: "",
    url: "https://api.pacifica.fi/api/v1/info/prices",
    timeoutMs: 12000,
    concurrency: 6,
    outPath: "",
    workingOutPath: "",
  };

  for (let i = 2; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--file" && argv[i + 1]) out.file = argv[++i];
    else if (arg === "--url" && argv[i + 1]) out.url = argv[++i];
    else if (arg === "--timeout-ms" && argv[i + 1]) out.timeoutMs = Number(argv[++i]);
    else if (arg === "--concurrency" && argv[i + 1]) out.concurrency = Number(argv[++i]);
    else if (arg === "--out" && argv[i + 1]) out.outPath = argv[++i];
    else if (arg === "--working-out" && argv[i + 1]) out.workingOutPath = argv[++i];
  }

  if (!out.file) {
    throw new Error("Missing required --file <path> argument");
  }
  if (!Number.isFinite(out.timeoutMs) || out.timeoutMs < 1000) out.timeoutMs = 12000;
  if (!Number.isFinite(out.concurrency) || out.concurrency < 1) out.concurrency = 6;
  out.concurrency = Math.min(32, Math.max(1, Math.floor(out.concurrency)));
  return out;
}

function ensureDirFor(filePath) {
  const dir = path.dirname(filePath);
  fs.mkdirSync(dir, { recursive: true });
}

function parseProxyFile(raw) {
  const text = String(raw || "").trim();
  if (!text) return [];

  // Format 1: JSON array
  try {
    const parsed = JSON.parse(text);
    if (Array.isArray(parsed)) {
      return parsed
        .map((row) => {
          if (typeof row === "string") return row.trim();
          if (row && typeof row === "object" && row.proxy) return String(row.proxy).trim();
          return "";
        })
        .filter(Boolean);
    }

    // Format 2: JSON object wrapper (e.g. prior test output)
    if (parsed && typeof parsed === "object") {
      const candidateLists = [];
      if (Array.isArray(parsed.results)) candidateLists.push(parsed.results);
      if (Array.isArray(parsed.proxies)) candidateLists.push(parsed.proxies);
      if (Array.isArray(parsed.rows)) candidateLists.push(parsed.rows);
      if (Array.isArray(parsed.items)) candidateLists.push(parsed.items);

      if (candidateLists.length) {
        return candidateLists
          .flatMap((list) => list)
          .map((row) => {
            if (typeof row === "string") return row.trim();
            if (row && typeof row === "object" && row.proxy) return String(row.proxy).trim();
            return "";
          })
          .filter(Boolean);
      }
    }
  } catch (_error) {
    // continue
  }

  // Format 2: NDJSON or simple one-proxy-per-line
  return text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      if (line.startsWith("#")) return "";
      try {
        const parsed = JSON.parse(line);
        if (typeof parsed === "string") return parsed.trim();
        if (parsed && typeof parsed === "object" && parsed.proxy) return String(parsed.proxy).trim();
      } catch (_error) {
        // plain text line
      }
      return line;
    })
    .filter(Boolean);
}

function uniq(list) {
  return Array.from(new Set(Array.isArray(list) ? list : []));
}

function runCurlProxyTest({ proxy, url, timeoutMs }) {
  const timeoutSec = Math.max(1, Math.ceil(timeoutMs / 1000));
  const marker = "__PF_PROXY_META__";
  const args = [
    "--silent",
    "--show-error",
    "--location",
    "--max-time",
    String(timeoutSec),
    "--proxy",
    proxy,
    "--write-out",
    `\\n${marker}:%{http_code}:%{time_total}`,
    url,
  ];

  return new Promise((resolve) => {
    execFile("curl", args, { maxBuffer: 4 * 1024 * 1024 }, (error, stdout, stderr) => {
      const output = String(stdout || "");
      const markerPos = output.lastIndexOf(`\n${marker}:`);
      const body = markerPos >= 0 ? output.slice(0, markerPos) : output;
      const meta = markerPos >= 0 ? output.slice(markerPos + 1).trim() : "";

      let statusCode = 0;
      let elapsedSec = NaN;
      if (meta.startsWith(`${marker}:`)) {
        const parts = meta.split(":");
        statusCode = Number(parts[1] || 0);
        elapsedSec = Number(parts[2] || NaN);
      }

      const base = {
        proxy,
        statusCode: Number.isFinite(statusCode) ? statusCode : 0,
        latencyMs: Number.isFinite(elapsedSec) ? Math.round(elapsedSec * 1000) : null,
        testedAt: new Date().toISOString(),
      };

      if (error) {
        resolve({
          ...base,
          ok: false,
          error: String(stderr || error.message || "curl_error").trim().slice(0, 300),
        });
        return;
      }

      if (statusCode !== 200) {
        resolve({
          ...base,
          ok: false,
          error: `http_${statusCode || "unknown"}`,
        });
        return;
      }

      try {
        const parsed = JSON.parse(body);
        const looksValid =
          (parsed && typeof parsed === "object" && parsed.success === true) ||
          (parsed && Array.isArray(parsed.data)) ||
          Array.isArray(parsed);

        if (!looksValid) {
          resolve({
            ...base,
            ok: false,
            error: "invalid_json_payload_shape",
          });
          return;
        }

        resolve({
          ...base,
          ok: true,
          sampleSize:
            parsed && Array.isArray(parsed.data)
              ? parsed.data.length
              : Array.isArray(parsed)
              ? parsed.length
              : null,
        });
      } catch (_error) {
        resolve({
          ...base,
          ok: false,
          error: "non_json_response",
        });
      }
    });
  });
}

async function runWithConcurrency(items, limit, fn) {
  const results = new Array(items.length);
  let idx = 0;

  async function worker() {
    while (true) {
      const current = idx;
      idx += 1;
      if (current >= items.length) return;
      results[current] = await fn(items[current], current);
    }
  }

  const workers = [];
  for (let i = 0; i < Math.min(limit, items.length); i += 1) {
    workers.push(worker());
  }
  await Promise.all(workers);
  return results;
}

async function main() {
  const opts = parseArgs(process.argv);
  const inputPath = path.resolve(opts.file);
  const outPath = path.resolve(
    opts.outPath || path.join(process.cwd(), "data", "indexer", "proxy_test_results.json")
  );
  const workingOutPath = path.resolve(
    opts.workingOutPath ||
      path.join(process.cwd(), "data", "indexer", "working_proxies.txt")
  );

  if (!fs.existsSync(inputPath)) {
    throw new Error(`Proxy file not found: ${inputPath}`);
  }

  const raw = fs.readFileSync(inputPath, "utf8");
  const proxies = uniq(parseProxyFile(raw));
  if (!proxies.length) {
    throw new Error(`No proxy entries found in: ${inputPath}`);
  }

  console.log(
    `[proxy-test] start count=${proxies.length} concurrency=${opts.concurrency} timeout_ms=${opts.timeoutMs} url=${opts.url}`
  );

  let tested = 0;
  const startedAt = Date.now();
  const results = await runWithConcurrency(proxies, opts.concurrency, async (proxy) => {
    const result = await runCurlProxyTest({
      proxy,
      url: opts.url,
      timeoutMs: opts.timeoutMs,
    });
    tested += 1;
    const okFail = result.ok ? "ok" : "fail";
    console.log(
      `[proxy-test] ${okFail} ${tested}/${proxies.length} proxy=${proxy} status=${result.statusCode} latency_ms=${
        result.latencyMs === null ? "n/a" : result.latencyMs
      } error=${result.error || "none"}`
    );
    return result;
  });

  const working = results.filter((row) => row && row.ok).map((row) => row.proxy);
  const failed = results.filter((row) => row && !row.ok);
  const finishedAt = Date.now();

  const summary = {
    type: "pacifica_proxy_test",
    ok: true,
    url: opts.url,
    total: results.length,
    working: working.length,
    failed: failed.length,
    durationMs: finishedAt - startedAt,
    startedAt: new Date(startedAt).toISOString(),
    finishedAt: new Date(finishedAt).toISOString(),
  };

  ensureDirFor(outPath);
  ensureDirFor(workingOutPath);
  fs.writeFileSync(
    outPath,
    JSON.stringify(
      {
        ...summary,
        results,
      },
      null,
      2
    ),
    "utf8"
  );
  fs.writeFileSync(workingOutPath, `${working.join("\n")}${working.length ? "\n" : ""}`, "utf8");

  console.log(JSON.stringify({ ...summary, outPath, workingOutPath }, null, 2));
}

main().catch((error) => {
  console.error(
    JSON.stringify(
      {
        type: "pacifica_proxy_test",
        ok: false,
        error: error.message,
      },
      null,
      2
    )
  );
  process.exit(1);
});
