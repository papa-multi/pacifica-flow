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

function createRng(seed) {
  let state = Number(seed) >>> 0;
  if (!state) state = 0x6d2b79f5;
  return () => {
    state = (state * 1664525 + 1013904223) >>> 0;
    return state / 0x100000000;
  };
}

function sampleWithoutReplacement(list, count, seed) {
  const out = [];
  const work = list.slice();
  const rng = createRng(seed);
  const target = Math.min(work.length, Math.max(0, Number(count || 0)));
  for (let i = 0; i < target; i += 1) {
    const idx = Math.floor(rng() * work.length);
    out.push(work[idx]);
    work.splice(idx, 1);
  }
  return out;
}

function evidenceKey(row = {}) {
  const signature = String(row.signature || "");
  const scope = String(row.scope || "");
  const destination = String(row.destination || row.vault || "");
  const wallet = String(row.wallet || "");
  const selectedBy = String(row.selectedBy || "");
  return [signature, scope, destination, wallet, selectedBy].join("|");
}

function inferSelectedByFromSourceCounts(sourceCounts = {}) {
  if (!sourceCounts || typeof sourceCounts !== "object") return null;
  if (Number(sourceCounts.deposit_authority || 0) > 0) return "authority";
  if (Number(sourceCounts.deposit_source_owner || 0) > 0) return "source_owner";
  if (Number(sourceCounts.deposit_fee_payer || 0) > 0) return "fee_payer_fallback";
  return null;
}

function loadWalletEvidence(walletDiscoveryPath) {
  if (!fs.existsSync(walletDiscoveryPath)) {
    throw new Error(`wallet_discovery not found: ${walletDiscoveryPath}`);
  }

  const raw = JSON.parse(fs.readFileSync(walletDiscoveryPath, "utf8"));
  const wallets = raw && raw.wallets && typeof raw.wallets === "object" ? raw.wallets : {};

  const dedupe = new Map();
  Object.entries(wallets).forEach(([wallet, row]) => {
    const normalizedWallet = normalizeAddress(wallet);
    if (!normalizedWallet) return;
    const evidenceRows = Array.isArray(row && row.recentDepositEvidence)
      ? row.recentDepositEvidence
      : [];
    evidenceRows.forEach((ev) => {
      const normalized = {
        wallet: normalizeAddress(ev && ev.wallet ? ev.wallet : normalizedWallet) || normalizedWallet,
        signature: ev && ev.signature ? String(ev.signature) : null,
        slot: Number.isFinite(Number(ev && ev.slot)) ? Number(ev.slot) : null,
        blockTimeMs: Number.isFinite(Number(ev && ev.blockTimeMs)) ? Number(ev.blockTimeMs) : null,
        programId: ev && ev.programId ? String(ev.programId) : null,
        destination:
          normalizeAddress(ev && ev.destination ? ev.destination : ev && ev.vault ? ev.vault : null) ||
          null,
        source: normalizeAddress(ev && ev.source) || null,
        mint: normalizeAddress(ev && ev.mint) || null,
        authority: normalizeAddress(ev && ev.authority) || null,
        sourceOwner: normalizeAddress(ev && ev.sourceOwner) || null,
        selectedBy: ev && ev.selectedBy ? String(ev.selectedBy) : null,
        scope: ev && ev.scope ? String(ev.scope) : null,
        instructionType: ev && ev.instructionType ? String(ev.instructionType) : null,
        instructionKind: ev && ev.instructionKind ? String(ev.instructionKind) : null,
        instructionIndex:
          Number.isFinite(Number(ev && ev.instructionIndex)) ? Number(ev.instructionIndex) : null,
        parentInstructionIndex:
          Number.isFinite(Number(ev && ev.parentInstructionIndex))
            ? Number(ev.parentInstructionIndex)
            : null,
        innerInstructionIndex:
          Number.isFinite(Number(ev && ev.innerInstructionIndex))
            ? Number(ev.innerInstructionIndex)
            : null,
        observedAt: Number.isFinite(Number(ev && ev.observedAt)) ? Number(ev.observedAt) : null,
      };

      if (!normalized.signature || !normalized.destination) return;
      dedupe.set(evidenceKey(normalized), normalized);
    });

    if (!evidenceRows.length) {
      const fallbackSignatures = Array.isArray(row && row.recentEvidenceSignatures)
        ? row.recentEvidenceSignatures
        : [];
      const inferredSelectedBy = inferSelectedByFromSourceCounts(row && row.sourceCounts);
      fallbackSignatures.forEach((signature) => {
        const normalizedSig = signature ? String(signature) : null;
        if (!normalizedSig) return;
        const legacy = {
          wallet: normalizedWallet,
          signature: normalizedSig,
          slot: null,
          blockTimeMs: null,
          programId: null,
          destination: null,
          source: null,
          mint: null,
          authority: null,
          sourceOwner: null,
          selectedBy: inferredSelectedBy,
          scope: null,
          instructionType: null,
          instructionKind: null,
          instructionIndex: null,
          parentInstructionIndex: null,
          innerInstructionIndex: null,
          observedAt: null,
          legacySignatureEvidence: true,
        };
        dedupe.set(evidenceKey(legacy), legacy);
      });
    }
  });

  return Array.from(dedupe.values());
}

async function loadRawTransactionsBySignature(rawTransactionsPath, signatures = new Set()) {
  if (!signatures.size) return new Map();
  if (!fs.existsSync(rawTransactionsPath)) {
    throw new Error(`raw_transactions.ndjson not found: ${rawTransactionsPath}`);
  }

  const results = new Map();
  const stream = fs.createReadStream(rawTransactionsPath, "utf8");
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  for await (const line of rl) {
    if (!line || !line.trim()) continue;
    let row;
    try {
      row = JSON.parse(line);
    } catch (_err) {
      continue;
    }
    const signature = row && row.signature ? String(row.signature) : null;
    if (!signature || !signatures.has(signature)) continue;
    if (!results.has(signature)) results.set(signature, row);
    if (results.size >= signatures.size) break;
  }

  rl.close();
  stream.close();
  return results;
}

function matchDepositFromExtraction(sample, extracted = []) {
  const wallet = normalizeAddress(sample.wallet);
  const destination = normalizeAddress(sample.destination);
  const candidates = (Array.isArray(extracted) ? extracted : []).filter((row) => {
    if (normalizeAddress(row.wallet) !== wallet) return false;
    if (destination && normalizeAddress(row.destination) !== destination) return false;
    return true;
  });

  if (!candidates.length) return null;

  const strict = candidates.find((row) => {
    const sameScope = String(row.scope || "") === String(sample.scope || "");
    const sameIx =
      Number(row.instructionIndex) === Number(sample.instructionIndex) &&
      Number(row.parentInstructionIndex) === Number(sample.parentInstructionIndex) &&
      Number(row.innerInstructionIndex) === Number(sample.innerInstructionIndex);
    return sameScope || sameIx;
  });

  if (strict) return strict;
  return candidates[0];
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
  const sampleSize = Math.max(1, Number(args.sample || 25));
  const seed = Number.isFinite(Number(args.seed)) ? Number(args.seed) : Date.now();
  const outputPath = args.out ? path.resolve(String(args.out)) : null;

  const programIds = toList(
    args.programs || process.env.PACIFICA_PROGRAM_IDS || "PCFA5iYgmqK6MqPhWNKg7Yv7auX7VZ4Cx7T1eJyrAMH"
  );
  const vaults = toList(
    args.vaults || process.env.PACIFICA_DEPOSIT_VAULTS || "72R843XwZxqWhsJceARQQTTbYtWy6Zw9et2YV4FpRHTa"
  );

  if (!programIds.length) {
    throw new Error("No Pacifica program IDs configured for audit");
  }
  if (!vaults.length) {
    throw new Error("No Pacifica vaults configured for audit");
  }

  const allEvidence = loadWalletEvidence(walletDiscoveryPath);
  if (!allEvidence.length) {
    throw new Error("No deposit evidence found in wallet_discovery.json");
  }

  const samples = sampleWithoutReplacement(allEvidence, sampleSize, seed);
  const signatureSet = new Set(samples.map((row) => String(row.signature)));
  const txMap = await loadRawTransactionsBySignature(rawTransactionsPath, signatureSet);

  const failures = [];
  let passed = 0;
  const selectedByCounts = {
    authority: 0,
    source_owner: 0,
    fee_payer_fallback: 0,
    other: 0,
  };

  for (const sample of samples) {
    const selectedBy = String(sample.selectedBy || "");
    if (Object.prototype.hasOwnProperty.call(selectedByCounts, selectedBy)) {
      selectedByCounts[selectedBy] += 1;
    } else {
      selectedByCounts.other += 1;
    }

    const txRow = txMap.get(String(sample.signature));
    if (!txRow || !txRow.tx) {
      failures.push({
        reason: "missing_raw_transaction",
        sample,
      });
      continue;
    }

    const extracted = extractDepositWalletsFromTransaction(txRow.tx, {
      programIds,
      vaults,
      exclude: [],
    });
    const matched = matchDepositFromExtraction(sample, extracted && extracted.deposits);
    if (!matched) {
      failures.push({
        reason: "extraction_mismatch",
        sample,
        extractedCount: Array.isArray(extracted && extracted.deposits)
          ? extracted.deposits.length
          : 0,
      });
      continue;
    }

    if (sample.selectedBy && String(matched.selectedBy || "") !== String(sample.selectedBy || "")) {
      failures.push({
        reason: "selected_by_mismatch",
        sample,
        matched,
      });
      continue;
    }

    passed += 1;
  }

  const summary = {
    type: "deposit_wallet_audit",
    generatedAt: Date.now(),
    seed,
    sampleSizeRequested: sampleSize,
    sampleSizeActual: samples.length,
    evidenceTotal: allEvidence.length,
    programIds,
    vaults,
    passed,
    failed: failures.length,
    passRatePct: samples.length ? Number(((passed / samples.length) * 100).toFixed(2)) : 0,
    selectedByCounts,
    failures: failures.slice(0, 100),
  };

  if (outputPath) {
    fs.writeFileSync(outputPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
  }

  process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
  process.exitCode = failures.length ? 2 : 0;
}

main().catch((error) => {
  process.stderr.write(`[audit_deposit_wallets] ${error.message}\n`);
  process.exit(1);
});
