#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const { buildWalletRecordFromHistory } = require("../src/services/analytics/wallet_stats");
const { writeJsonAtomic, ensureDir } = require("../src/services/pipeline/utils");

function parseArgs(argv = []) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const arg = String(argv[i] || "");
    if (!arg.startsWith("--")) continue;
    const key = arg.slice(2);
    const next = argv[i + 1];
    if (next && !String(next).startsWith("--")) {
      out[key] = next;
      i += 1;
    } else {
      out[key] = "true";
    }
  }
  return out;
}

function readJsonSafe(filePath, fallback = null) {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch (_error) {
    return fallback;
  }
}

function toBool(value, fallback = false) {
  if (value === undefined || value === null) return fallback;
  const raw = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(raw)) return true;
  if (["0", "false", "no", "n", "off"].includes(raw)) return false;
  return fallback;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const dataDir = path.resolve(args["data-dir"] || path.join(process.cwd(), "data", "indexer"));
  const historyDir = path.resolve(args["history-dir"] || path.join(dataDir, "wallet_history"));
  const walletsPath = path.resolve(args["wallets-path"] || path.join(dataDir, "wallets.json"));
  const includeExisting = toBool(args["include-existing"], true);
  const dryRun = toBool(args["dry-run"], false);

  if (!fs.existsSync(historyDir)) {
    throw new Error(`wallet_history directory not found: ${historyDir}`);
  }

  const files = fs
    .readdirSync(historyDir)
    .filter((name) => name.endsWith(".json"))
    .sort((a, b) => a.localeCompare(b));

  const existing = includeExisting ? readJsonSafe(walletsPath, {}) : {};
  const existingWallets =
    existing && existing.wallets && typeof existing.wallets === "object" ? existing.wallets : {};

  const wallets = {};
  let scanned = 0;
  let rebuilt = 0;
  let skipped = 0;
  let failed = 0;

  for (const fileName of files) {
    scanned += 1;
    const filePath = path.join(historyDir, fileName);
    const payload = readJsonSafe(filePath, null);
    if (!payload || typeof payload !== "object") {
      failed += 1;
      continue;
    }

    const wallet = String(payload.wallet || path.basename(fileName, ".json")).trim();
    if (!wallet) {
      failed += 1;
      continue;
    }

    const tradesHistory = Array.isArray(payload.trades) ? payload.trades : [];
    const fundingHistory = Array.isArray(payload.funding) ? payload.funding : [];

    if (!tradesHistory.length && !fundingHistory.length) {
      if (existingWallets[wallet]) {
        wallets[wallet] = existingWallets[wallet];
      }
      skipped += 1;
      continue;
    }

    const record = buildWalletRecordFromHistory({
      wallet,
      tradesHistory,
      fundingHistory,
      computedAt: Date.now(),
    });
    wallets[wallet] = record;
    rebuilt += 1;
  }

  if (includeExisting) {
    for (const [wallet, record] of Object.entries(existingWallets)) {
      if (!wallets[wallet]) wallets[wallet] = record;
    }
  }

  const output = {
    wallets,
    updatedAt: Date.now(),
    stats: {
      scannedFiles: scanned,
      rebuiltWallets: rebuilt,
      skippedWallets: skipped,
      failedFiles: failed,
      totalWallets: Object.keys(wallets).length,
    },
  };

  if (!dryRun) {
    ensureDir(path.dirname(walletsPath));
    writeJsonAtomic(walletsPath, output);
  }

  console.log(
    JSON.stringify(
      {
        type: "rebuild_wallet_stats_from_history",
        ok: true,
        dryRun,
        historyDir,
        walletsPath,
        ...output.stats,
      },
      null,
      2
    )
  );
}

main().catch((error) => {
  console.error(
    JSON.stringify(
      {
        type: "rebuild_wallet_stats_from_history",
        ok: false,
        error: error && error.message ? error.message : String(error),
      },
      null,
      2
    )
  );
  process.exitCode = 1;
});

