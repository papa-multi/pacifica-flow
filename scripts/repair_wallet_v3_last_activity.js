"use strict";

const fs = require("fs");
const path = require("path");
const zlib = require("zlib");

const ROOT = path.resolve(__dirname, "..");
const V3_DIR = path.join(ROOT, "data", "wallet_explorer_v3");
const SHARDS_DIR = path.join(V3_DIR, "shards");
const MANIFEST_PATH = path.join(V3_DIR, "manifest.json");
const ISOLATED_DIR = path.join(ROOT, "data", "wallet_explorer_v2", "isolated");

const historyCache = new Map();

function loadLastOpenedAt(wallet) {
  const normalizedWallet = String(wallet || "").trim();
  if (!normalizedWallet) return null;
  if (historyCache.has(normalizedWallet)) return historyCache.get(normalizedWallet);
  const filePath = path.join(ISOLATED_DIR, normalizedWallet, "wallet_history.json");
  let value = null;
  try {
    const payload = JSON.parse(fs.readFileSync(filePath, "utf8"));
    const trades = Array.isArray(payload && payload.trades) ? payload.trades : [];
    let lastOpenedAt = 0;
    for (const trade of trades) {
      if (!trade || typeof trade !== "object") continue;
      const side = String(trade.side || "").trim().toLowerCase();
      if (!side.startsWith("open_")) continue;
      const ts = Number(trade.timestamp || trade.created_at || trade.createdAt || 0) || 0;
      if (ts > lastOpenedAt) lastOpenedAt = ts;
    }
    value = lastOpenedAt > 0 ? lastOpenedAt : null;
  } catch {
    value = null;
  }
  historyCache.set(normalizedWallet, value);
  return value;
}

function main() {
  const manifest = JSON.parse(fs.readFileSync(MANIFEST_PATH, "utf8"));
  const shardFiles = fs
    .readdirSync(SHARDS_DIR)
    .filter((name) => name.endsWith(".json.gz"))
    .sort();

  let changedRows = 0;
  let changedShards = 0;
  const repairedAt = Date.now();

  shardFiles.forEach((name, index) => {
    const filePath = path.join(SHARDS_DIR, name);
    const payload = JSON.parse(zlib.gunzipSync(fs.readFileSync(filePath)).toString("utf8"));
    const rows = Array.isArray(payload && payload.rows) ? payload.rows : [];
    let shardChanged = false;

    rows.forEach((row) => {
      if (!row || typeof row !== "object") return;
      const lastOpenedAt = loadLastOpenedAt(row.wallet);
      const currentCanonical =
        Math.max(
          Number(row.lastOpenedAt || 0),
          Number(row.lastActivity || 0),
          Number(row.lastActivityAt || 0),
          Number(row.lastActiveAt || 0)
        ) || null;
      const nextValue = lastOpenedAt || null;
      if (currentCanonical === nextValue) return;
      row.lastOpenedAt = nextValue;
      row.lastActivity = nextValue;
      row.lastActivityAt = nextValue;
      row.lastActiveAt = nextValue;
      changedRows += 1;
      shardChanged = true;
    });

    if (shardChanged) {
      payload.generatedAt = repairedAt;
      fs.writeFileSync(filePath, zlib.gzipSync(Buffer.from(JSON.stringify(payload))));
      changedShards += 1;
    }

    if ((index + 1) % 16 === 0) {
      console.log(JSON.stringify({ progress: `${index + 1}/${shardFiles.length}`, changedRows, changedShards }));
    }
  });

  manifest.generatedAt = repairedAt;
  manifest.repairedAt = repairedAt;
  manifest.lastActivitySource = "isolated_wallet_open_history";
  fs.writeFileSync(MANIFEST_PATH, JSON.stringify(manifest, null, 2));

  console.log(JSON.stringify({ done: true, shardCount: shardFiles.length, changedRows, changedShards }));
}

main();
