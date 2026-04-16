#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

const ROOT = path.resolve(__dirname, "..");
const {
  DETAIL_CACHE_PATH,
  loadPacificaPublicWalletDetailCache,
  writePacificaPublicWalletDetailCache,
} = require(path.join(ROOT, "src", "services", "read_model", "pacifica_public_wallet_sources"));

function readJson(filePath) {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return null;
  }
}

function normalizeWallet(value) {
  return String(value || "").trim();
}

function metricFieldScore(entry = {}) {
  const fields = [
    "drawdownPct",
    "returnPct",
    "accountEquityUsd",
    "volumeAllTime",
    "pnlAllTime",
    "openPositions",
  ];
  return fields.reduce((score, field) => {
    return score + (entry[field] !== undefined && entry[field] !== null ? 1 : 0);
  }, 0);
}

function fetchedAtValue(entry = {}) {
  return Number(entry.fetchedAt || entry.accountUpdatedAt || 0) || 0;
}

function chooseBetterEntry(currentEntry = null, candidateEntry = null) {
  if (!candidateEntry || typeof candidateEntry !== "object") return currentEntry;
  if (!currentEntry || typeof currentEntry !== "object") return candidateEntry;
  const currentScore = metricFieldScore(currentEntry);
  const candidateScore = metricFieldScore(candidateEntry);
  if (candidateScore !== currentScore) return candidateScore > currentScore ? candidateEntry : currentEntry;
  const currentFetchedAt = fetchedAtValue(currentEntry);
  const candidateFetchedAt = fetchedAtValue(candidateEntry);
  if (candidateFetchedAt !== currentFetchedAt) {
    return candidateFetchedAt > currentFetchedAt ? candidateEntry : currentEntry;
  }
  return currentEntry;
}

function main() {
  const sourcePaths = process.argv.slice(2).filter(Boolean);
  if (!sourcePaths.length) {
    console.error(JSON.stringify({ done: false, error: "no_source_paths" }));
    process.exit(1);
  }
  const existing =
    loadPacificaPublicWalletDetailCache() || {
      source: "pacifica_public_endpoints",
      generatedAt: 0,
      wallets: {},
    };
  const mergedWallets =
    existing.wallets && typeof existing.wallets === "object" ? { ...existing.wallets } : {};
  let mergedCount = 0;
  for (const sourcePath of sourcePaths) {
    const payload = readJson(sourcePath);
    const wallets =
      payload && payload.wallets && typeof payload.wallets === "object" ? payload.wallets : {};
    for (const [wallet, entry] of Object.entries(wallets)) {
      const normalizedWallet = normalizeWallet(wallet);
      if (!normalizedWallet || !entry || typeof entry !== "object") continue;
      const currentEntry = mergedWallets[normalizedWallet] || null;
      const nextEntry = chooseBetterEntry(currentEntry, entry);
      if (nextEntry !== currentEntry) {
        mergedWallets[normalizedWallet] = nextEntry;
        mergedCount += 1;
      }
    }
  }
  const nextPayload = {
    source: "pacifica_public_endpoints",
    generatedAt: Date.now(),
    walletCount: Object.keys(mergedWallets).length,
    wallets: mergedWallets,
  };
  writePacificaPublicWalletDetailCache(nextPayload);
  console.log(
    JSON.stringify({
      done: true,
      targetPath: DETAIL_CACHE_PATH,
      sourcePaths,
      mergedCount,
      walletCount: nextPayload.walletCount,
    })
  );
}

main();
