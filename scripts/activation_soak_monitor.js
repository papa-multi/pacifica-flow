#!/usr/bin/env node

const fs = require("fs");
const path = require("path");
const http = require("http");
const https = require("https");
const { execFileSync } = require("child_process");

const ROOT_DIR = path.resolve(__dirname, "..");
const OUTPUT_DIR = process.env.PACIFICA_MONITOR_OUTPUT_DIR
  ? path.resolve(process.env.PACIFICA_MONITOR_OUTPUT_DIR)
  : path.join(ROOT_DIR, "data", "runtime", "activation-monitor");
const STATUS_URL =
  String(process.env.PACIFICA_MONITOR_STATUS_URL || "http://127.0.0.1:3200/api/system/status").trim();
const INTERVAL_MS = Math.max(
  5000,
  Number(process.env.PACIFICA_MONITOR_INTERVAL_MS || 30000)
);
const SAMPLES_PATH = path.join(OUTPUT_DIR, "samples.ndjson");
const SUMMARY_PATH = path.join(OUTPUT_DIR, "summary.json");
const SERVICE_UNITS = [
  "pacifica-wallet-indexer.service",
  "pacifica-onchain-discovery.service",
  "pacifica-ui-api.service",
];

function ensureDir(dirPath) {
  if (!dirPath) return;
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
}

function writeJsonAtomic(filePath, value) {
  ensureDir(path.dirname(filePath));
  const tmpPath = `${filePath}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmpPath, JSON.stringify(value, null, 2));
  fs.renameSync(tmpPath, filePath);
}

function appendLine(filePath, line) {
  ensureDir(path.dirname(filePath));
  fs.appendFileSync(filePath, `${line}\n`);
}

function toNum(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function avg(values = []) {
  const rows = values.filter((value) => Number.isFinite(Number(value)));
  if (!rows.length) return 0;
  return rows.reduce((sum, value) => sum + Number(value), 0) / rows.length;
}

function percentile(values = [], p = 0.95) {
  const rows = values
    .map((value) => Number(value))
    .filter((value) => Number.isFinite(value))
    .sort((a, b) => a - b);
  if (!rows.length) return 0;
  const index = Math.min(rows.length - 1, Math.max(0, Math.floor((rows.length - 1) * p)));
  return rows[index];
}

function fetchJson(urlText) {
  return new Promise((resolve, reject) => {
    const url = new URL(urlText);
    const mod = url.protocol === "https:" ? https : http;
    const req = mod.get(url, (res) => {
      let data = "";
      res.setEncoding("utf8");
      res.on("data", (chunk) => {
        data += chunk;
      });
      res.on("end", () => {
        if (Number(res.statusCode || 0) >= 400) {
          reject(new Error(`status_${res.statusCode}`));
          return;
        }
        try {
          resolve(JSON.parse(data));
        } catch (error) {
          reject(error);
        }
      });
    });
    req.on("error", reject);
    req.setTimeout(10000, () => {
      req.destroy(new Error("timeout"));
    });
  });
}

function parseSystemctlShow(raw = "") {
  const out = {};
  String(raw || "")
    .split(/\r?\n/)
    .filter(Boolean)
    .forEach((line) => {
      const idx = line.indexOf("=");
      if (idx <= 0) return;
      out[line.slice(0, idx)] = line.slice(idx + 1);
    });
  return out;
}

function listLivePositionUnits() {
  try {
    const raw = execFileSync(
      "systemctl",
      ["list-units", "--type=service", "--all", "pacifica-live-positions@*.service", "--no-legend", "--plain"],
      { encoding: "utf8" }
    );
    return String(raw || "")
      .split(/\r?\n/)
      .map((line) => line.trim().split(/\s+/)[0])
      .filter(Boolean);
  } catch (_error) {
    return [];
  }
}

function readServiceStats(unit) {
  try {
    const raw = execFileSync(
      "systemctl",
      ["show", unit, "--property=MainPID,MemoryCurrent,ActiveState,SubState"],
      { encoding: "utf8" }
    );
    const parsed = parseSystemctlShow(raw);
    return {
      unit,
      mainPid: toNum(parsed.MainPID, 0),
      memoryBytes: toNum(parsed.MemoryCurrent, 0),
      activeState: parsed.ActiveState || "unknown",
      subState: parsed.SubState || "unknown",
    };
  } catch (error) {
    return {
      unit,
      mainPid: 0,
      memoryBytes: 0,
      activeState: "error",
      subState: error.message,
    };
  }
}

function summarizeShardRows(rows = []) {
  const assigned = rows.map((row) => toNum(row && row.assignedWallets, 0));
  const queued = rows.map((row) => toNum(row && row.cycleQueuedWallets, 0));
  return {
    shardCount: rows.length,
    assignedAvg: Number(avg(assigned).toFixed(2)),
    assignedMax: assigned.length ? Math.max(...assigned) : 0,
    assignedP95: Number(percentile(assigned, 0.95).toFixed(2)),
    cycleQueuedAvg: Number(avg(queued).toFixed(2)),
    cycleQueuedMax: queued.length ? Math.max(...queued) : 0,
    cycleQueuedP95: Number(percentile(queued, 0.95).toFixed(2)),
  };
}

function buildWarnings(sample, prevSample = null) {
  const warnings = [];
  if (toNum(sample.rest.failures429, 0) > 0 || toNum(sample.helius.rate429PerMin, 0) > 0) {
    warnings.push("rate_limit_pressure");
  }
  if (toNum(sample.rest.pauseRemainingMsMax, 0) > 0) {
    warnings.push("rest_pause_active");
  }
  if (
    prevSample &&
    toNum(sample.queue.pendingBacklog, 0) > toNum(prevSample.queue.pendingBacklog, 0) &&
    toNum(sample.rest.headroomPct, 0) >= 50
  ) {
    warnings.push("queue_growing_with_headroom");
  }
  if (
    toNum(sample.shards.assignedMax, 0) >
    Math.max(1, toNum(sample.shards.assignedAvg, 0)) * 3
  ) {
    warnings.push("shard_assignment_skew");
  }
  if (
    toNum(sample.memory.totalMb, 0) >= 2500
  ) {
    warnings.push("memory_high");
  }
  return warnings;
}

function buildSample(statusPayload, prevSample, serviceStats) {
  const root = statusPayload && statusPayload.status ? statusPayload.status : {};
  const summary = root.summary || {};
  const workers = root.workers || {};
  const indexer = workers.indexer || {};
  const onchain = workers.onchain || {};
  const live = root.live || {};
  const pool = indexer.restClients && indexer.restClients.pool ? indexer.restClients.pool : {};
  const shardRows =
    indexer.restClients && Array.isArray(indexer.restClients.shards)
      ? indexer.restClients.shards
      : [];
  const shardSummary = summarizeShardRows(shardRows);
  const memoryRows = Array.isArray(serviceStats) ? serviceStats : [];
  const totalMemoryBytes = memoryRows.reduce(
    (sum, row) => sum + toNum(row && row.memoryBytes, 0),
    0
  );
  const sample = {
    at: Date.now(),
    mode: summary.systemMode || (summary.systemStatus && summary.systemStatus.mode) || "unknown",
    queue: {
      knownWallets: toNum(indexer.knownWallets, 0),
      discovered: toNum(indexer.lifecycle && indexer.lifecycle.discovered, 0),
      validated: toNum(onchain.confirmedWallets, 0),
      backfillComplete: toNum(indexer.lifecycle && indexer.lifecycle.backfillComplete, 0),
      liveTracking: toNum(indexer.lifecycle && indexer.lifecycle.liveTracking, 0),
      liveTrackingPartial: toNum(indexer.lifecycle && indexer.lifecycle.liveTrackingPartial, 0),
      pendingBacklog: toNum(indexer.walletBacklog, 0),
      priorityQueue: toNum(indexer.priorityQueueSize, 0),
      pendingWallets: toNum(indexer.pendingWallets, 0),
      partialWallets: toNum(indexer.partiallyIndexedWallets, 0),
      trackedWallets: toNum(live.walletsKnownGlobal || 0, 0),
      avgPendingWaitMs: toNum(indexer.averagePendingWaitMs, 0),
      avgQueueWaitMs: toNum(indexer.averageQueueWaitMs, 0),
    },
    rest: {
      rpmUsed: toNum(pool.rpmUsed, 0),
      rpmCap: toNum(pool.rpmCap, 0),
      headroomPct:
        toNum(pool.rpmCap, 0) > 0
          ? Number(
              (
                (1 - toNum(pool.rpmUsed, 0) / Math.max(1, toNum(pool.rpmCap, 0))) *
                100
              ).toFixed(2)
            )
          : null,
      activeClients: toNum(pool.active, 0),
      coolingClients: toNum(pool.cooling, 0),
      inFlight: toNum(pool.inFlight, 0),
      failures429: toNum(pool.failures429, 0),
      pauseRemainingMsMax: toNum(pool.pauseRemainingMsMax, 0),
    },
    shards: shardSummary,
    onchain: {
      progressPct: Number(
        onchain.progress && onchain.progress.pct !== undefined
          ? onchain.progress.pct
          : 0
      ),
      walletCount: toNum(onchain.walletCount, 0),
      pendingTransactions: toNum(onchain.pendingTransactions, 0),
      etaMs: toNum(onchain.etaMs, 0),
    },
    helius: {
      shardCount:
        onchain.rpc && Array.isArray(onchain.rpc.shards) ? onchain.rpc.shards.length : 0,
      rate429PerMin:
        onchain.rpc && Array.isArray(onchain.rpc.shards)
          ? onchain.rpc.shards.reduce(
              (sum, row) => sum + toNum(row && row.stats && row.stats.rate429PerMin, 0),
              0
            )
          : 0,
    },
    retries: {
      failedScans: toNum(indexer.failedScans, 0),
      topErrorReasons: Array.isArray(indexer.topErrorReasons)
        ? indexer.topErrorReasons.slice(0, 5)
        : [],
    },
    memory: {
      totalMb: Number((totalMemoryBytes / (1024 * 1024)).toFixed(2)),
      byUnit: memoryRows.map((row) => ({
        unit: row.unit,
        activeState: row.activeState,
        subState: row.subState,
        mainPid: row.mainPid,
        memoryMb: Number((toNum(row.memoryBytes, 0) / (1024 * 1024)).toFixed(2)),
      })),
    },
  };
  sample.delta = prevSample
    ? {
        discovered: sample.queue.discovered - toNum(prevSample.queue && prevSample.queue.discovered, 0),
        backfillComplete:
          sample.queue.backfillComplete -
          toNum(prevSample.queue && prevSample.queue.backfillComplete, 0),
        liveTracking:
          sample.queue.liveTracking - toNum(prevSample.queue && prevSample.queue.liveTracking, 0),
        pendingBacklog:
          sample.queue.pendingBacklog -
          toNum(prevSample.queue && prevSample.queue.pendingBacklog, 0),
      }
    : {
        discovered: 0,
        backfillComplete: 0,
        liveTracking: 0,
        pendingBacklog: 0,
      };
  sample.warnings = buildWarnings(sample, prevSample);
  return sample;
}

function updateSummary(summary, sample) {
  const next = summary && typeof summary === "object" ? { ...summary } : {};
  next.startedAt = next.startedAt || sample.at;
  next.updatedAt = sample.at;
  next.samples = toNum(next.samples, 0) + 1;
  next.maxPendingBacklog = Math.max(
    toNum(next.maxPendingBacklog, 0),
    toNum(sample.queue.pendingBacklog, 0)
  );
  next.minPendingBacklog =
    next.minPendingBacklog === undefined
      ? toNum(sample.queue.pendingBacklog, 0)
      : Math.min(toNum(next.minPendingBacklog, 0), toNum(sample.queue.pendingBacklog, 0));
  next.maxLiveTracking = Math.max(
    toNum(next.maxLiveTracking, 0),
    toNum(sample.queue.liveTracking, 0)
  );
  next.maxLiveTrackingPartial = Math.max(
    toNum(next.maxLiveTrackingPartial, 0),
    toNum(sample.queue.liveTrackingPartial, 0)
  );
  next.maxDiscovered = Math.max(
    toNum(next.maxDiscovered, 0),
    toNum(sample.queue.discovered, 0)
  );
  next.maxBackfillComplete = Math.max(
    toNum(next.maxBackfillComplete, 0),
    toNum(sample.queue.backfillComplete, 0)
  );
  next.maxRestRpmUsed = Math.max(
    toNum(next.maxRestRpmUsed, 0),
    toNum(sample.rest.rpmUsed, 0)
  );
  next.maxMemoryMb = Math.max(
    toNum(next.maxMemoryMb, 0),
    toNum(sample.memory.totalMb, 0)
  );
  next.maxHelius429PerMin = Math.max(
    toNum(next.maxHelius429PerMin, 0),
    toNum(sample.helius.rate429PerMin, 0)
  );
  next.maxRest429 = Math.max(
    toNum(next.maxRest429, 0),
    toNum(sample.rest.failures429, 0)
  );
  next.warningCounts = next.warningCounts && typeof next.warningCounts === "object" ? next.warningCounts : {};
  (Array.isArray(sample.warnings) ? sample.warnings : []).forEach((warning) => {
    next.warningCounts[warning] = toNum(next.warningCounts[warning], 0) + 1;
  });
  next.lastSample = sample;
  return next;
}

function collectServiceStats() {
  const units = Array.from(new Set([...SERVICE_UNITS, ...listLivePositionUnits()]));
  return units.map((unit) => readServiceStats(unit));
}

async function main() {
  ensureDir(OUTPUT_DIR);
  let prevSample = null;
  let summary = fs.existsSync(SUMMARY_PATH)
    ? JSON.parse(fs.readFileSync(SUMMARY_PATH, "utf8"))
    : {};

  for (;;) {
    const startedAt = Date.now();
    try {
      const [statusPayload, serviceStats] = await Promise.all([
        fetchJson(STATUS_URL),
        Promise.resolve(collectServiceStats()),
      ]);
      const sample = buildSample(statusPayload, prevSample, serviceStats);
      appendLine(SAMPLES_PATH, JSON.stringify(sample));
      summary = updateSummary(summary, sample);
      writeJsonAtomic(SUMMARY_PATH, summary);
      prevSample = sample;
      const warnings = sample.warnings.length ? ` warnings=${sample.warnings.join(",")}` : "";
      console.log(
        `[activation-monitor] discovered=${sample.queue.discovered} backfill_complete=${sample.queue.backfillComplete} live=${sample.queue.liveTracking} live_partial=${sample.queue.liveTrackingPartial} backlog=${sample.queue.pendingBacklog} queue=${sample.queue.priorityQueue} rest=${sample.rest.rpmUsed}/${sample.rest.rpmCap} headroom=${sample.rest.headroomPct}% mem_mb=${sample.memory.totalMb} onchain_pct=${sample.onchain.progressPct}${warnings}`
      );
    } catch (error) {
      const sample = {
        at: Date.now(),
        error: error && error.message ? error.message : String(error || "unknown_error"),
      };
      appendLine(SAMPLES_PATH, JSON.stringify(sample));
      summary = updateSummary(summary, {
        at: sample.at,
        queue: {},
        rest: {},
        helius: {},
        memory: {},
        warnings: ["monitor_error"],
      });
      summary.lastError = sample;
      writeJsonAtomic(SUMMARY_PATH, summary);
      console.warn(`[activation-monitor] error=${sample.error}`);
    }
    const elapsed = Date.now() - startedAt;
    const sleepMs = Math.max(1000, INTERVAL_MS - elapsed);
    await new Promise((resolve) => setTimeout(resolve, sleepMs));
  }
}

main().catch((error) => {
  console.error(error && error.stack ? error.stack : error);
  process.exit(1);
});
