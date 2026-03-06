#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const readline = require("readline");
const {
  extractDepositWalletsFromTransaction,
  normalizeAddress,
} = require("../src/services/indexer/wallet_extractor");

function parseArgs(argv = []) {
  const args = {};
  for (let i = 0; i < argv.length; i += 1) {
    const token = String(argv[i] || "");
    if (!token.startsWith("--")) continue;
    const key = token.slice(2);
    const next = argv[i + 1];
    if (!next || String(next).startsWith("--")) {
      args[key] = true;
    } else {
      args[key] = next;
      i += 1;
    }
  }
  return args;
}

function toList(value, fallback = []) {
  if (Array.isArray(value)) return value;
  if (!value || typeof value !== "string") return fallback;
  return String(value)
    .split(",")
    .map((v) => normalizeAddress(v))
    .filter(Boolean);
}

function evidenceKey(row = {}) {
  const signature = String(row.signature || "");
  const scope = String(row.scope || "");
  const destination = String(row.destination || row.vault || "");
  const wallet = String(row.wallet || "");
  const selectedBy = String(row.selectedBy || "");
  return [signature, scope, destination, wallet, selectedBy].join("|");
}

function safeReadJson(filePath) {
  if (!fs.existsSync(filePath)) return null;
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function normalizeEvidenceRow({
  wallet,
  signature,
  slot,
  blockTimeMs,
  programId,
  deposit,
}) {
  return {
    wallet,
    signature: signature || null,
    slot: Number.isFinite(Number(slot)) ? Number(slot) : null,
    blockTimeMs: Number.isFinite(Number(blockTimeMs)) ? Number(blockTimeMs) : null,
    programId: programId || null,
    vault: deposit && deposit.destination ? String(deposit.destination) : null,
    destination: deposit && deposit.destination ? String(deposit.destination) : null,
    source: deposit && deposit.source ? String(deposit.source) : null,
    mint: deposit && deposit.mint ? String(deposit.mint) : null,
    authority: deposit && deposit.authority ? String(deposit.authority) : null,
    sourceOwner: deposit && deposit.sourceOwner ? String(deposit.sourceOwner) : null,
    selectedBy: deposit && deposit.selectedBy ? String(deposit.selectedBy) : null,
    scope: deposit && deposit.scope ? String(deposit.scope) : null,
    instructionType: deposit && deposit.instructionType ? String(deposit.instructionType) : null,
    instructionKind: deposit && deposit.instructionKind ? String(deposit.instructionKind) : null,
    instructionIndex:
      deposit && Number.isFinite(Number(deposit.instructionIndex))
        ? Number(deposit.instructionIndex)
        : null,
    parentInstructionIndex:
      deposit && Number.isFinite(Number(deposit.parentInstructionIndex))
        ? Number(deposit.parentInstructionIndex)
        : null,
    innerInstructionIndex:
      deposit && Number.isFinite(Number(deposit.innerInstructionIndex))
        ? Number(deposit.innerInstructionIndex)
        : null,
    observedAt: Date.now(),
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const rootDir = path.resolve(__dirname, "..");
  const dataDir = path.resolve(
    String(args.dataDir || process.env.PACIFICA_DATA_DIR || path.join(rootDir, "data", "indexer"))
  );
  const walletDiscoveryPath = path.resolve(
    String(args.walletDiscovery || path.join(dataDir, "wallet_discovery.json"))
  );
  const rawTransactionsPath = path.resolve(
    String(args.rawTransactions || path.join(dataDir, "raw_transactions.ndjson"))
  );
  const outPath = path.resolve(String(args.out || walletDiscoveryPath));

  const programIds = toList(
    args.programs || process.env.PACIFICA_PROGRAM_IDS || "PCFA5iYgmqK6MqPhWNKg7Yv7auX7VZ4Cx7T1eJyrAMH"
  );
  const vaults = toList(
    args.vaults || process.env.PACIFICA_DEPOSIT_VAULTS || "72R843XwZxqWhsJceARQQTTbYtWy6Zw9et2YV4FpRHTa"
  );

  if (!programIds.length) throw new Error("No Pacifica program IDs configured");
  if (!vaults.length) throw new Error("No Pacifica deposit vaults configured");

  const walletDiscovery = safeReadJson(walletDiscoveryPath);
  if (!walletDiscovery || !walletDiscovery.wallets || typeof walletDiscovery.wallets !== "object") {
    throw new Error(`Invalid wallet_discovery file: ${walletDiscoveryPath}`);
  }
  if (!fs.existsSync(rawTransactionsPath)) {
    throw new Error(`raw_transactions.ndjson not found: ${rawTransactionsPath}`);
  }

  const wallets = walletDiscovery.wallets;
  const signatureTargets = new Map(); // signature -> Set<wallet>
  const candidates = [];

  Object.entries(wallets).forEach(([wallet, row]) => {
    const normalized = normalizeAddress(wallet);
    if (!normalized || !row || typeof row !== "object") return;

    const hasEvidence =
      Array.isArray(row.recentDepositEvidence) && row.recentDepositEvidence.length > 0;
    if (hasEvidence) return;

    const signatures = Array.isArray(row.recentEvidenceSignatures)
      ? row.recentEvidenceSignatures
      : [];
    if (!signatures.length) return;

    candidates.push(normalized);
    signatures.forEach((sig) => {
      const signature = sig ? String(sig) : null;
      if (!signature) return;
      if (!signatureTargets.has(signature)) {
        signatureTargets.set(signature, new Set());
      }
      signatureTargets.get(signature).add(normalized);
    });
  });

  const stats = {
    candidateWallets: candidates.length,
    targetSignatures: signatureTargets.size,
    scannedRawTxRows: 0,
    matchedRawTxRows: 0,
    evidenceAdded: 0,
    walletsEnriched: 0,
  };

  if (!signatureTargets.size) {
    process.stdout.write(
      `${JSON.stringify(
        {
          type: "enrich_deposit_evidence",
          ok: true,
          message: "nothing_to_enrich",
          stats,
        },
        null,
        2
      )}\n`
    );
    return;
  }

  const enrichedWallets = new Set();
  const stream = fs.createReadStream(rawTransactionsPath, "utf8");
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  for await (const line of rl) {
    if (!line || !line.trim()) continue;
    stats.scannedRawTxRows += 1;

    let row;
    try {
      row = JSON.parse(line);
    } catch (_err) {
      continue;
    }

    const signature = row && row.signature ? String(row.signature) : null;
    if (!signature || !signatureTargets.has(signature)) continue;
    stats.matchedRawTxRows += 1;

    const targetWallets = signatureTargets.get(signature);
    if (!targetWallets || !targetWallets.size) continue;

    const extracted = extractDepositWalletsFromTransaction(row.tx, {
      programIds,
      vaults,
      exclude: [],
    });
    const deposits = Array.isArray(extracted && extracted.deposits) ? extracted.deposits : [];
    if (!deposits.length) continue;

    const depositsByWallet = new Map();
    deposits.forEach((deposit) => {
      const wallet = normalizeAddress(deposit && deposit.wallet);
      if (!wallet) return;
      if (!depositsByWallet.has(wallet)) depositsByWallet.set(wallet, []);
      depositsByWallet.get(wallet).push(deposit);
    });

    for (const wallet of targetWallets.values()) {
      const rowWallet = wallets[wallet];
      if (!rowWallet) continue;

      const walletDeposits = depositsByWallet.get(wallet) || [];
      if (!walletDeposits.length) continue;

      const current = Array.isArray(rowWallet.recentDepositEvidence)
        ? rowWallet.recentDepositEvidence.slice()
        : [];
      const dedupe = new Set(current.map((ev) => evidenceKey(ev)));

      let addedForWallet = 0;
      walletDeposits.forEach((deposit) => {
        const evidence = normalizeEvidenceRow({
          wallet,
          signature,
          slot: row.slot,
          blockTimeMs: row.blockTimeMs,
          programId: row.programId || null,
          deposit,
        });
        const key = evidenceKey(evidence);
        if (dedupe.has(key)) return;
        dedupe.add(key);
        current.push(evidence);
        addedForWallet += 1;
      });

      if (!addedForWallet) continue;

      if (current.length > 256) {
        current.splice(0, current.length - 256);
      }

      rowWallet.recentDepositEvidence = current;
      rowWallet.depositEvidenceCount = Number(rowWallet.depositEvidenceCount || 0) + addedForWallet;
      rowWallet.updatedAt = Date.now();
      stats.evidenceAdded += addedForWallet;
      enrichedWallets.add(wallet);
    }
  }

  rl.close();
  stream.close();

  stats.walletsEnriched = enrichedWallets.size;

  walletDiscovery.updatedAt = Date.now();
  fs.writeFileSync(outPath, `${JSON.stringify(walletDiscovery, null, 2)}\n`, "utf8");

  process.stdout.write(
    `${JSON.stringify(
      {
        type: "enrich_deposit_evidence",
        ok: true,
        outPath,
        stats,
      },
      null,
      2
    )}\n`
  );
}

main().catch((error) => {
  process.stderr.write(`[enrich_deposit_evidence] ${error.message}\n`);
  process.exit(1);
});

